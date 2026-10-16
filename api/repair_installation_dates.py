"""
One-off repair (2026-10-16): stations whose daily history starts on a
placeholder installation_date.

Onboarding stored installation_date = "tonight minus ONBOARD_BACKFILL_DAYS" and
started the backfill there; the nightly's correction only moved the date to the
oldest reading, i.e. to that same clipped start. Compared with Solis's first-
generation date (fisGenerateTime on every userStationList row): 194 of 696
stations began later than Solis, 7,110 station-days missing, 13 stations by
more than 90 days (Cabrales 375). Solis still serves those months (Manalo's
April 2026 = 477 kWh), so the history is recoverable.

For every active station with a Solis id whose installation_date is LATER than
Solis's first-generation day:
  * installation_date := Solis's day (never moved later — a date an engineer
    set earlier on purpose stays), and
  * when the station's oldest stored day is after that day, one DAILY backfill
    job for [Solis's day, oldest stored day − 1] (stationMonth per month, the
    ordinary backfill worker drains it; a station with a daily job already
    active is skipped, not failed).

Reads: the Solis roster (8 calls) and one DB query. Writes: through db.audited,
source 'fleet_script', one request id for the batch.

    python -m api.repair_installation_dates --dry-run
    python -m api.repair_installation_dates
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()

from api import db  # noqa: E402
from api.solis_client import SolisCloudClient, first_generation_date  # noqa: E402

log = logging.getLogger("repair_installation_dates")
PHT = timezone(timedelta(hours=8))

STATIONS_SQL = """
select s.id as system_id, s.solis_station_id, s.system_name, s.installation_date,
       min((r."timestamp" at time zone 'Asia/Manila')::date) as first_day,
       exists (select 1 from public.backfill_jobs j
                where j.system_id = s.id and j.granularity = 'daily'
                  and j.status in ('queued', 'running')) as busy
  from public.solar_systems s
  left join public.energy_readings r on r.system_id = s.id
 where s.status = 'active' and s.solis_station_id is not null
 group by 1, 2, 3, 4
 order by s.system_name
"""


async def fetch_roster(solis: SolisCloudClient) -> Dict[str, dict]:
    roster: Dict[str, dict] = {}
    page = 1
    while True:
        resp = await solis.list_stations(page_no=page, page_size=100)
        pg = resp.get("page") if isinstance(resp, dict) else None
        recs = ((pg or {}).get("records") or (resp.get("records") if isinstance(resp, dict) else None) or [])
        for rec in recs:
            roster[str(rec.get("id"))] = rec
        pages = int((pg or {}).get("pages") or 1)
        if page >= pages or not recs:
            break
        page += 1
    return roster


async def plan() -> Dict:
    for k in ("SOLIS_CLOUD_KEY_ID", "SOLIS_CLOUD_KEY_SECRET"):
        if not os.getenv(k):
            raise SystemExit(f"missing env var: {k}")
    solis = SolisCloudClient(os.environ["SOLIS_CLOUD_KEY_ID"], os.environ["SOLIS_CLOUD_KEY_SECRET"])
    try:
        roster = await fetch_roster(solis)
    finally:
        await solis.aclose()
    with db.connect(autocommit=True) as conn:
        rows = conn.execute(STATIONS_SQL).fetchall()

    today = datetime.now(PHT).date()
    yesterday = today - timedelta(days=1)
    moves: List[dict] = []
    jobs: List[dict] = []
    not_in_roster = no_solis_date = busy = 0
    for r in rows:
        rec = roster.get(str(r["solis_station_id"]))
        if rec is None:
            not_in_roster += 1
            continue
        solis_first = first_generation_date(rec)
        if not solis_first or solis_first >= today:
            no_solis_date += 1
            continue
        current = r["installation_date"]
        if current is not None and current <= solis_first:
            continue  # already at or before Solis's day: leave it
        moves.append({"system_id": r["system_id"], "name": r["system_name"], "station_id": r["solis_station_id"],
                      "from": current, "to": solis_first})
        first_day = r["first_day"]
        job_from, job_to = solis_first, (first_day - timedelta(days=1)) if first_day else yesterday
        if job_to < job_from:
            continue  # history already reaches back to Solis's day
        if r["busy"]:
            busy += 1
            continue
        jobs.append({"system_id": r["system_id"], "station_id": r["solis_station_id"], "name": r["system_name"],
                     "date_from": job_from, "date_to": job_to, "days": (job_to - job_from).days + 1,
                     "months": (job_to.year - job_from.year) * 12 + job_to.month - job_from.month + 1})
    return {"stations": len(rows), "not_in_roster": not_in_roster, "no_solis_date": no_solis_date,
            "moves": moves, "jobs": jobs, "busy": busy}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Move placeholder installation dates to Solis's first generation and queue the missing history.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    p = asyncio.run(plan())
    moves, jobs = p["moves"], p["jobs"]
    print(f"active stations with a Solis id: {p['stations']}  (not in roster: {p['not_in_roster']}, no Solis first-generation date: {p['no_solis_date']})")
    print(f"installation_date to move earlier: {len(moves)}")
    print(f"daily history jobs to queue: {len(jobs)}  (skipped, daily job already active: {p['busy']})")
    print(f"station-days to fetch: {sum(j['days'] for j in jobs)}  stationMonth calls: {sum(j['months'] for j in jobs)}")
    if jobs:
        print("\nlargest 15:")
        for j in sorted(jobs, key=lambda j: -j["days"])[:15]:
            print(f"  {str(j['name'])[:40]:40} {j['date_from']} → {j['date_to']}  {j['days']:>4} day(s), {j['months']:>2} call(s)")
    if args.dry_run or not moves:
        print("\ndry run — nothing written" if args.dry_run else "\nnothing to do")
        return

    request_id = str(uuid.uuid4())
    reason = ("installation date moved to Solis's first-generation day (fisGenerateTime); onboarding had stored "
              "'onboarding night minus 60 days' and started the history there — repair 2026-10-16")
    with db.audited(None, None, reason, source="fleet_script", request_id=request_id) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "update public.solar_systems set installation_date = %s where id = %s and installation_date is distinct from %s",
                [(m["to"], m["system_id"], m["to"]) for m in moves])
            cur.executemany(
                """insert into public.backfill_jobs
                     (system_id, solis_station_id, granularity, date_from, date_to,
                      requested_by, requested_reason, request_id)
                   values (%s, %s, 'daily', %s, %s, null, %s, %s)
                   on conflict do nothing""",
                [(j["system_id"], j["station_id"], j["date_from"], j["date_to"],
                  "history before the first stored day: " + reason, request_id) for j in jobs])
    print(f"\nmoved {len(moves)} installation date(s) and queued {len(jobs)} daily job(s) under request {request_id}")


if __name__ == "__main__":
    main()
