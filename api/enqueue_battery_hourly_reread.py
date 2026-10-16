"""
Queue a targeted hourly re-read for the battery stations whose
energy_readings_hourly rows were written before migration 24 (2026-10-09) and
therefore hold 0 for battery charge and discharge.

Why: the customer portal's one-day view splits each hour's consumption into
grid / solar / battery from the hourly table. With battery at 0, the energy the
battery supplied is shown as "Solar" (De Zosa, 2026-09-01: 48 % "from solar"
where Solis says 29 % solar + 21 % battery). The initial 90-day hourly fill
(2026-10-03) and the 30-day fleet re-read (2026-10-09, days 09-09..10-08) left
2026-07-05..2026-09-08 at 0 on ~519 stations, plus a short tail to 10-11.

What this queues: ONE hourly backfill job per station, covering only the days
that need it — days where the station's DAILY row shows battery activity but
the hourly rows for that day sum to 0 battery (or are missing). The ordinary
backfill worker drains the jobs (one stationDay call per station-day, ~2.3 s
each measured 2026-10-03/09). Stations with an hourly job already queued or
running are skipped, not failed. Nothing here talks to Solis.

A station's need-days are grouped into runs (gaps of up to MAX_GAP_DAYS are
bridged, a few extra calls being cheaper than a second job); the LONGEST run is
queued now. backfill_jobs allows one active hourly job per station, so a
station with a second run gets it on the next invocation — re-run this script
once the batch has drained and it queues whatever is still at 0.

    python -m api.enqueue_battery_hourly_reread --dry-run   # counts and ranges only
    python -m api.enqueue_battery_hourly_reread             # queue
    python -m api.enqueue_battery_hourly_reread --cutoff 2026-10-12 --start 2026-07-05

Audit: requested_by NULL, source 'fleet_script', one request id for the batch
(the Backfills tab shows it as one run).
"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple

from dotenv import load_dotenv

load_dotenv()

from api import db  # noqa: E402

log = logging.getLogger("enqueue_battery_hourly_reread")
PHT = timezone(timedelta(hours=8))

# First day of the initial 90-day hourly fill (2026-10-03): nothing older exists
# in energy_readings_hourly, so nothing older can be "at 0".
HOURLY_HISTORY_START = date(2026, 7, 5)
# Exclusive. Rows rolled up live after the 2026-10-09 deploy carry battery; the
# last zero-battery day seen on 2026-10-16 was 2026-10-11.
DEFAULT_CUTOFF = date(2026, 10, 12)
MAX_GAP_DAYS = 3
SECONDS_PER_STATION_DAY = 2.3  # measured: 90-day jobs 204 s, 30-day jobs 70 s

# Days a station needs: its daily row shows battery activity, but its hourly
# rows for the day sum to 0 battery (or there are none). Manila dates; the
# hour_start bounds are sargable on the partitioned hourly table.
NEED_SQL = """
with bounds as (
  select (%(start)s::timestamp at time zone 'Asia/Manila')  as t0,
         (%(cutoff)s::timestamp at time zone 'Asia/Manila') as t1),
bat_days as (
  select r.system_id, (r."timestamp" at time zone 'Asia/Manila')::date as d
    from public.energy_readings r, bounds
   where r."timestamp" >= bounds.t0 and r."timestamp" < bounds.t1
     and (coalesce(r.battery_charge_kwh, 0) > 0 or coalesce(r.battery_discharge_kwh, 0) > 0)),
hours as (
  select h.system_id, (h.hour_start at time zone 'Asia/Manila')::date as d,
         sum(h.battery_charge_kwh + h.battery_discharge_kwh) as hbat
    from public.energy_readings_hourly h, bounds
   where h.hour_start >= bounds.t0 and h.hour_start < bounds.t1
   group by 1, 2),
need as (
  select b.system_id, b.d
    from bat_days b left join hours h using (system_id, d)
   where h.d is null or h.hbat = 0)
select s.id as system_id, s.solis_station_id, s.system_name,
       array_agg(n.d order by n.d) as days,
       exists (select 1 from public.backfill_jobs j
                where j.system_id = s.id and j.granularity = 'hourly'
                  and j.status in ('queued', 'running')) as busy
  from need n
  join public.solar_systems s on s.id = n.system_id
 where s.status = 'active' and s.solis_station_id is not null
 group by 1, 2, 3
 order by s.system_name
"""


def runs(days: List[date], max_gap: int) -> List[Tuple[date, date, int]]:
    """Consecutive-ish runs of dates: (first, last, days_in_run), bridging gaps of up to max_gap days."""
    out: List[Tuple[date, date, int]] = []
    if not days:
        return out
    start = prev = days[0]
    n = 1
    for d in days[1:]:
        if (d - prev).days > max_gap + 1:
            out.append((start, prev, n))
            start, n = d, 0
        prev = d
        n += 1
    out.append((start, prev, n))
    return out


def plan(start: date, cutoff: date) -> Dict:
    yesterday = datetime.now(PHT).date() - timedelta(days=1)
    with db.connect(autocommit=True) as conn:
        rows = conn.execute(NEED_SQL, {"start": start.isoformat(), "cutoff": cutoff.isoformat()}).fetchall()
    jobs, busy, leftovers, need_days = [], 0, 0, 0
    for r in rows:
        days = [d for d in r["days"] if d <= yesterday]
        if not days:
            continue
        need_days += len(days)
        if r["busy"]:
            busy += 1
            continue
        rs = runs(days, MAX_GAP_DAYS)
        first, last, _ = max(rs, key=lambda x: x[2])
        if len(rs) > 1:
            leftovers += 1
        jobs.append({"system_id": r["system_id"], "solis_station_id": r["solis_station_id"],
                     "system_name": r["system_name"], "date_from": first, "date_to": last,
                     "calls": (last - first).days + 1, "need": len(days), "runs": len(rs)})
    return {"stations_needing": len(rows), "need_station_days": need_days, "busy": busy,
            "jobs": jobs, "stations_with_leftover_runs": leftovers,
            "calls": sum(j["calls"] for j in jobs)}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Queue hourly re-reads for battery stations whose hourly rows hold 0 battery.")
    ap.add_argument("--start", type=date.fromisoformat, default=HOURLY_HISTORY_START, help="first day considered (Manila)")
    ap.add_argument("--cutoff", type=date.fromisoformat, default=DEFAULT_CUTOFF, help="exclusive last day considered (Manila)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    p = plan(args.start, args.cutoff)
    jobs = p["jobs"]
    hours = p["calls"] * SECONDS_PER_STATION_DAY / 3600
    print(f"window {args.start}..{args.cutoff} (exclusive)")
    print(f"stations needing a re-read: {p['stations_needing']}  (station-days at 0 battery: {p['need_station_days']})")
    print(f"jobs to queue: {len(jobs)}  skipped (hourly job already active): {p['busy']}")
    print(f"Solis stationDay calls: {p['calls']}  ≈ {hours:.1f} worker-hours at {SECONDS_PER_STATION_DAY} s each")
    print(f"stations with a second run left for a later pass: {p['stations_with_leftover_runs']}")
    if jobs:
        froms = sorted(j["date_from"] for j in jobs)
        tos = sorted(j["date_to"] for j in jobs)
        print(f"date_from: {froms[0]} .. {froms[-1]}   date_to: {tos[0]} .. {tos[-1]}")
        print("\nlargest 10:")
        for j in sorted(jobs, key=lambda j: -j["calls"])[:10]:
            print(f"  {str(j['system_name'])[:40]:40} {j['date_from']} → {j['date_to']}  {j['calls']:>3} day(s), {j['need']:>3} needed, {j['runs']} run(s)")
    if args.dry_run or not jobs:
        print("\ndry run — nothing queued" if args.dry_run else "\nnothing to queue")
        return

    request_id = str(uuid.uuid4())
    reason = ("battery re-read: hourly rows written before migration 24 (2026-10-09) hold 0 battery; "
              "re-read the stationDay curves for the station's zero-battery days")
    with db.audited(None, None, reason, source="fleet_script", request_id=request_id) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """insert into public.backfill_jobs
                     (system_id, solis_station_id, granularity, date_from, date_to,
                      requested_by, requested_reason, request_id)
                   values (%s, %s, 'hourly', %s, %s, null, %s, %s)
                   on conflict do nothing""",
                [(j["system_id"], j["solis_station_id"], j["date_from"], j["date_to"], reason, request_id)
                 for j in jobs])
    print(f"\nqueued {len(jobs)} hourly job(s) under request {request_id}")


if __name__ == "__main__":
    main()
