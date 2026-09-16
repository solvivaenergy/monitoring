"""
Fill the gaps in energy_readings — days missing INSIDE a station's own history —
without touching any row that already exists.

This is deliberately NOT a rebuild. Measured 2026-09-16: 619 stations hold
115,732 day-rows and are 96.2% complete within their own spans; the 3.8% missing
is 4,413 days in 305 station-months. A full refetch would be ~115k station-days
against an API that fails ~24% of calls — the 2026-09-02 rebuild wrote 103,185
rows against a table holding 114,858, i.e. one pass LOSES ~10% of history. So:
fetch only the station-months that have a hole, insert only the missing days,
and never update a row that is already there (ON CONFLICT DO NOTHING).

Because each fetched month also covers days we already hold, the same call
gives a free reconciliation sample: existing production_kwh is compared with
what Solis reports today and mismatches are written to a CSV. That report is
the answer to "can we trust the data" — a rebuild is not.

Usage:
    python -m api.backfill_gaps                        # dry-run: fetch, compare, write nothing
    python -m api.backfill_gaps --apply                # insert the missing days
    python -m api.backfill_gaps --station 1298491919450893691   # one station
    python -m api.backfill_gaps --report-dir C:\\path   # where the two CSVs go (default: cwd)

Gaps are computed straight in Postgres over SUPABASE_DB_URL (psycopg): a set
difference over 116k rows is trivial in SQL and absurd over PostgREST. Writes go
through PostgREST like every other writer, so the same triggers and keys apply.
Ops script — psycopg is not in the worker image, run it from a workstation.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import psycopg
from dotenv import load_dotenv
from supabase import create_client

from api.backfill_history import PHT, SUPABASE_BATCH_SIZE, parse_month_day
from api.solis_client import SolisCloudClient, SolisCloudError

load_dotenv()
log = logging.getLogger("backfill_gaps")

# Same pacing as backfill_history: Solis returns 429 above ~2 concurrent calls.
SOLIS_CONCURRENCY = 2
SOLIS_DELAY = 0.5

# A day counts as a mismatch when Supabase and Solis disagree by more than this.
# Solis rounds daily energy to 0.01 kWh and revises the last day or two of a
# month after the fact, so exact equality is the wrong bar.
MISMATCH_ABS_KWH = 0.05
MISMATCH_REL = 0.01

# Dates are compared in Asia/Manila: Solis's dateStr is station-local, and the
# canonical row timestamp (04:00Z) is noon Manila. 405 legacy rows carry
# insert-time wall-clock timestamps; taking the Manila date keeps a row written
# at 23:50 Manila on the day it belongs to, where the UTC date would not.
GAP_SQL = """
with d as (
  select system_id, ("timestamp" at time zone 'Asia/Manila')::date as day
    from public.energy_readings group by 1, 2),
spans as (
  select system_id, min(day) as first_day, max(day) as last_day from d group by 1),
missing as (
  select s.system_id, g::date as day
    from spans s
    cross join lateral generate_series(s.first_day, s.last_day, interval '1 day') g
   where not exists (select 1 from d where d.system_id = s.system_id and d.day = g::date))
select m.system_id::text, ss.user_id::text, ss.solis_station_id, ss.capacity_kwp::float,
       ss.system_name, to_char(m.day, 'YYYY-MM') as month,
       array_agg(m.day::text order by m.day) as days
  from missing m
  join public.solar_systems ss on ss.id = m.system_id
 where ss.solis_station_id is not null
   and (%(station)s::text is null or ss.solis_station_id = %(station)s)
 group by 1, 2, 3, 4, 5, 6
 order by 3, 6
"""

EXISTING_SQL = """
select system_id::text,
       (("timestamp" at time zone 'Asia/Manila')::date)::text as day,
       production_kwh::float
  from public.energy_readings
 where system_id = any(%(ids)s::uuid[])
"""


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Insert the missing days of energy_readings; never touch existing rows.")
    p.add_argument("--apply", action="store_true", help="Write to Supabase. Default is dry-run.")
    p.add_argument("--station", default=None, help="Restrict to one Solis station id.")
    p.add_argument("--report-dir", default=".", help="Directory for the two report CSVs.")
    return p.parse_args()


def load_gaps(conn, station: Optional[str]) -> List[dict]:
    rows = conn.execute(GAP_SQL, {"station": station}).fetchall()
    keys = ["system_id", "user_id", "station_id", "capacity_kwp", "system_name", "month", "days"]
    return [dict(zip(keys, r)) for r in rows]


def load_existing(conn, system_ids: List[str]) -> Dict[Tuple[str, str], float]:
    if not system_ids:
        return {}
    return {(sid, day): kwh for sid, day, kwh in conn.execute(EXISTING_SQL, {"ids": system_ids})}


async def fetch_month(solis: SolisCloudClient, sem: asyncio.Semaphore, station_id: str, month: str):
    async with sem:
        await asyncio.sleep(SOLIS_DELAY)
        try:
            return await solis.station_month(station_id, month)
        except SolisCloudError as exc:
            log.warning("Solis error for station %s month %s: %s", station_id, month, exc)
            return None
        except Exception as exc:  # network etc. — leave the gap, report it
            log.warning("Fetch failed for station %s month %s: %s", station_id, month, exc)
            return None


def noon_manila(date_str: str) -> str:
    y, m, d = (int(x) for x in date_str.split("-"))
    return datetime(y, m, d, 12, 0, 0, tzinfo=PHT).isoformat()


def insert_missing(sb, rows: List[dict]) -> None:
    """Insert with ON CONFLICT DO NOTHING, in batches, with retry.

    ignore_duplicates=True is the whole point of this script: a row that exists
    is left exactly as it is, whatever Solis says today. The arbiter must match
    the unique index from migration 05, (system_id, "timestamp").
    """
    for start in range(0, len(rows), SUPABASE_BATCH_SIZE):
        batch = rows[start:start + SUPABASE_BATCH_SIZE]
        for attempt in range(5):
            try:
                sb.table("energy_readings").upsert(
                    batch, on_conflict="system_id,timestamp", ignore_duplicates=True
                ).execute()
                break
            except Exception as exc:
                if attempt == 4:
                    log.error("Batch insert failed after 5 attempts: %s", str(exc)[:200])
                    raise
                wait = 2 ** (attempt + 1)
                log.warning("Batch insert failed (attempt %d/5), retrying in %ds: %s", attempt + 1, wait, str(exc)[:120])
                time.sleep(wait)


async def main() -> None:
    args = parse_args()
    dry_run = not args.apply
    stamp = datetime.now(PHT).strftime("%Y-%m-%d")
    os.makedirs(args.report_dir, exist_ok=True)

    conn = psycopg.connect(get_env("SUPABASE_DB_URL"), autocommit=True)
    gaps = load_gaps(conn, args.station)
    if not gaps:
        print("No gaps found. Nothing to do.")
        return
    system_ids = sorted({g["system_id"] for g in gaps})
    existing = load_existing(conn, system_ids)

    total_missing = sum(len(g["days"]) for g in gaps)
    print("=" * 72)
    print(f"{'DRY RUN' if dry_run else 'LIVE RUN'} — {len(gaps)} station-month(s) across "
          f"{len({g['station_id'] for g in gaps})} station(s), {total_missing} missing day(s)")
    print("=" * 72)

    solis = SolisCloudClient(key_id=get_env("SOLIS_CLOUD_KEY_ID"), key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"))
    sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    t0 = time.time()
    months = await asyncio.gather(*(fetch_month(solis, sem, g["station_id"], g["month"]) for g in gaps))
    log.info("Fetched %d station-month(s) from Solis in %.0fs", len(gaps), time.time() - t0)

    inserts: Dict[Tuple[str, str], dict] = {}
    mismatches: List[dict] = []
    summary: List[dict] = []
    fetch_errors = 0
    compared = 0

    for gap, month_data in zip(gaps, months):
        missing = set(gap["days"])
        row_summary = {
            "station_id": gap["station_id"], "system_name": gap["system_name"], "month": gap["month"],
            "missing_days": len(missing), "filled_from_solis": 0, "no_solis_data": 0,
            "existing_compared": 0, "existing_mismatched": 0, "fetch_error": "",
        }
        if not isinstance(month_data, list):
            fetch_errors += 1
            row_summary["fetch_error"] = "no data / error"
            row_summary["no_solis_data"] = len(missing)
            summary.append(row_summary)
            continue

        by_day: Dict[str, dict] = {}
        for day in month_data:
            parsed = parse_month_day(day, float(gap["capacity_kwp"] or 0))
            if parsed is not None:
                by_day[parsed["date_str"]] = parsed  # Solis can repeat a date; keep the last

        for date_str, parsed in by_day.items():
            if date_str in missing:
                fields = dict(parsed)
                fields.pop("date_str")
                inserts[(gap["system_id"], date_str)] = {
                    "user_id": gap["user_id"],
                    "system_id": gap["system_id"],
                    "timestamp": noon_manila(date_str),
                    **fields,
                }
                row_summary["filled_from_solis"] += 1
            elif (gap["system_id"], date_str) in existing:
                ours = existing[(gap["system_id"], date_str)]
                theirs = parsed["production_kwh"]
                compared += 1
                row_summary["existing_compared"] += 1
                if abs(ours - theirs) > max(MISMATCH_ABS_KWH, MISMATCH_REL * max(abs(ours), abs(theirs))):
                    row_summary["existing_mismatched"] += 1
                    mismatches.append({
                        "station_id": gap["station_id"], "system_name": gap["system_name"], "day": date_str,
                        "supabase_kwh": round(ours, 4), "solis_kwh": round(theirs, 4),
                        "diff_kwh": round(theirs - ours, 4),
                    })
        row_summary["no_solis_data"] = len(missing - set(by_day))
        summary.append(row_summary)

    rows = list(inserts.values())
    no_data = sum(s["no_solis_data"] for s in summary)

    summary_path = os.path.join(args.report_dir, f"backfill_gaps_summary_{stamp}.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0].keys()))
        w.writeheader()
        w.writerows(summary)
    mismatch_path = os.path.join(args.report_dir, f"backfill_gaps_mismatches_{stamp}.csv")
    with open(mismatch_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["station_id", "system_name", "day", "supabase_kwh", "solis_kwh", "diff_kwh"])
        w.writeheader()
        w.writerows(sorted(mismatches, key=lambda m: -abs(m["diff_kwh"])))

    print()
    print(f"  Missing days:                      {total_missing}")
    print(f"  Fillable from Solis:               {len(rows)}")
    print(f"  Solis has no data for:             {no_data}   (genuinely dark days — stay empty)")
    print(f"  Station-months that failed to fetch: {fetch_errors}")
    print(f"  Existing days compared with Solis: {compared}")
    print(f"  ...of which mismatched:            {len(mismatches)}"
          f"  ({100.0 * len(mismatches) / compared:.2f}%)" if compared else "")
    print(f"  Reports: {summary_path}")
    print(f"           {mismatch_path}")

    if dry_run:
        print("\n  Dry run — nothing written. Pass --apply to insert the fillable days.")
        return

    if rows:
        sb = create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))
        t1 = time.time()
        insert_missing(sb, rows)
        log.info("Inserted up to %d row(s) in %.0fs", len(rows), time.time() - t1)

    remaining = load_gaps(conn, args.station)
    remaining_days = sum(len(g["days"]) for g in remaining)
    print(f"\n  Remaining missing days after apply: {remaining_days}  (was {total_missing};"
          f" {no_data} are days Solis itself has no data for)")


if __name__ == "__main__":
    asyncio.run(main())
