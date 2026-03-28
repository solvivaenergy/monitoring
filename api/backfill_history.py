"""
Backfill historical Solis data into Supabase.

For each user with a solis_station_id, fetches daily energy data from Solis
and writes one energy_reading per day.

Smart mode (default): uses each station's installation_date from
solar_systems to determine how far back to fetch — no wasted API calls.

Optimized: fetches days concurrently (semaphore-bounded) and batch-inserts.

Usage:
    cd monitoring
    python -m api.backfill_history          # dry-run, smart per-station dates
    python -m api.backfill_history --apply  # actually write to Supabase
    python -m api.backfill_history --days 180 --apply  # override: fixed 180 days for all
"""

import asyncio
import os
import sys
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from api.solis_client import SolisCloudClient, SolisCloudError
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill")

PHT = timezone(timedelta(hours=8))

# Concurrency for Solis API — 8 of 10 req/sec limit
SOLIS_CONCURRENCY = 8
# Batch size for Supabase inserts
SUPABASE_BATCH_SIZE = 500
# How many users to process in parallel
USER_CONCURRENCY = 3


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def aggregate_day(day_data: list) -> Optional[dict]:
    """Aggregate 5-min interval list into daily totals. Returns None if no production."""
    if not day_data or not isinstance(day_data, list):
        return None

    production_kwh = sum(float(p.get("power") or 0) for p in day_data) * (5 / 60) / 1000
    if production_kwh <= 0:
        return None

    consumption_kwh = sum(float(p.get("consumeEnergy") or 0) for p in day_data) * (5 / 60) / 1000
    grid_export_kwh = sum(max(float(p.get("psum") or 0), 0) for p in day_data) * (5 / 60) / 1000
    grid_import_kwh = sum(abs(min(float(p.get("psum") or 0), 0)) for p in day_data) * (5 / 60) / 1000

    soc_vals = [float(p.get("batteryCapacitySoc") or 0) for p in day_data if p.get("batteryCapacitySoc") is not None]
    avg_battery = round(sum(soc_vals) / len(soc_vals), 1) if soc_vals else None

    return {
        "production_kwh": round(production_kwh, 4),
        "consumption_kwh": round(consumption_kwh, 4),
        "grid_export_kwh": round(grid_export_kwh, 4),
        "grid_import_kwh": round(grid_import_kwh, 4),
        "battery_level": avg_battery,
        "battery_status": None if avg_battery is None else ("charging" if avg_battery > 50 else "discharging"),
    }


async def fetch_one_day(solis, sem, station_id, date_str):
    """Fetch one day's data with concurrency control."""
    async with sem:
        try:
            data = await solis.station_day(station_id, date_str)
            return date_str, data
        except SolisCloudError as e:
            if "no data" in str(e).lower():
                return date_str, None
            log.debug("Solis error for station %s date %s: %s", station_id, date_str, e)
            return date_str, None
        except Exception as e:
            log.debug("Error for station %s date %s: %s", station_id, date_str, e)
            return date_str, None


def sb_batch_insert(sb, rows):
    """Insert rows into Supabase with retry, in batches."""
    for batch_start in range(0, len(rows), SUPABASE_BATCH_SIZE):
        batch = rows[batch_start:batch_start + SUPABASE_BATCH_SIZE]
        for attempt in range(5):
            try:
                sb.table("energy_readings").insert(batch).execute()
                break
            except Exception as e:
                if attempt < 4:
                    wait = 2 ** (attempt + 1)
                    log.warning("Supabase batch insert failed (attempt %d/5), retrying in %ds: %s", attempt + 1, wait, str(e)[:120])
                    time.sleep(wait)
                else:
                    log.error("Supabase batch insert failed after 5 attempts: %s", str(e)[:200])
                    raise


async def main():
    dry_run = "--apply" not in sys.argv

    # Parse --days N (overrides smart mode with a fixed window)
    fixed_days = None
    if "--days" in sys.argv:
        idx = sys.argv.index("--days")
        if idx + 1 < len(sys.argv):
            fixed_days = int(sys.argv[idx + 1])

    mode_label = f"fixed {fixed_days} days" if fixed_days else "smart (per-station install date)"

    if dry_run:
        print("=" * 70)
        print(f"DRY RUN — mode: {mode_label}. Pass --apply to write.")
        print("=" * 70)
    else:
        print("=" * 70)
        print(f"LIVE RUN — mode: {mode_label}")
        print(f"  Solis concurrency: {SOLIS_CONCURRENCY} | User concurrency: {USER_CONCURRENCY} | Supabase batch: {SUPABASE_BATCH_SIZE}")
        print("=" * 70)

    sb = create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))
    solis = SolisCloudClient(
        key_id=get_env("SOLIS_CLOUD_KEY_ID"),
        key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"),
    )
    solis_sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    user_sem = asyncio.Semaphore(USER_CONCURRENCY)

    # ── Pre-load all data upfront (2 queries instead of 532) ──────────
    log.info("Pre-loading user profiles, solar systems, and existing readings...")

    resp = (
        sb.table("user_profiles")
        .select("id, full_name, solis_station_id")
        .not_.is_("solis_station_id", "null")
        .execute()
    )
    users = resp.data or []

    # Pre-load ALL solar_systems (id + user_id + installation_date)
    ss_resp = sb.table("solar_systems").select("id, user_id, installation_date, status").execute()
    system_ids = {}      # user_id → system UUID
    install_dates = {}   # user_id → install date string
    for row in (ss_resp.data or []):
        if row.get("status") == "active":
            system_ids[row["user_id"]] = row["id"]
        if row.get("installation_date"):
            install_dates[row["user_id"]] = row["installation_date"]

    # Pre-load ALL existing reading dates (paginated)
    all_existing = {}  # user_id → set of date strings
    offset = 0
    while True:
        batch = (
            sb.table("energy_readings")
            .select("user_id, timestamp")
            .range(offset, offset + 999)
            .execute()
        ).data
        if not batch:
            break
        for r in batch:
            uid = r["user_id"]
            if uid not in all_existing:
                all_existing[uid] = set()
            all_existing[uid].add(r["timestamp"][:10])
        if len(batch) < 1000:
            break
        offset += 1000

    log.info(
        "Pre-loaded: %d users, %d solar systems, %d existing readings.",
        len(users), len(system_ids), sum(len(v) for v in all_existing.values()),
    )

    today = datetime.now(PHT).date()
    counters = {"written": 0, "skipped": 0, "errors": 0, "api_calls": 0, "done": 0}
    start_time = time.time()
    total_users = len(users)

    async def process_user(user, idx):
        """Process one user's backfill, bounded by user_sem."""
        async with user_sem:
            uid = user["id"]
            station_id = user["solis_station_id"]
            name = user.get("full_name", "?")

            # Determine how many days to backfill
            if fixed_days:
                user_days = fixed_days
            else:
                install_str = install_dates.get(uid)
                if install_str:
                    install_dt = datetime.fromisoformat(install_str).date()
                    user_days = (today - install_dt).days
                else:
                    user_days = 90

            if user_days <= 0:
                counters["done"] += 1
                return

            # System ID from pre-loaded data
            system_id = system_ids.get(uid)
            if not system_id:
                try:
                    detail = await solis.station_detail(station_id)
                    capacity = float(detail.get("capacity") or 0)
                    station_name = detail.get("stationName") or str(station_id)
                except Exception as e:
                    log.error("[%d/%d] %s: station detail failed: %s", idx, total_users, name, e)
                    counters["errors"] += 1
                    counters["done"] += 1
                    return

                if not dry_run:
                    ins = sb.table("solar_systems").insert({
                        "user_id": uid,
                        "system_name": station_name,
                        "capacity_kwp": capacity,
                        "installation_date": today.isoformat(),
                        "address": "—",
                        "status": "active",
                    }).execute()
                    system_id = ins.data[0]["id"]
                else:
                    system_id = "DRY-RUN"

            # Dates needed from pre-loaded existing readings
            existing = all_existing.get(uid, set())
            dates_needed = []
            for d in range(user_days, 0, -1):
                target_date = today - timedelta(days=d)
                if target_date.isoformat() not in existing:
                    dates_needed.append(target_date)

            user_skipped = user_days - len(dates_needed)

            if not dates_needed:
                if user_skipped > 0:
                    log.info("[%d/%d] %s: fully backfilled, %d days skipped", idx, total_users, name, user_skipped)
                counters["skipped"] += user_skipped
                counters["done"] += 1
                return

            # Fetch all needed days concurrently (shared Solis semaphore)
            tasks = [
                fetch_one_day(solis, solis_sem, station_id, d.isoformat())
                for d in dates_needed
            ]
            results = await asyncio.gather(*tasks)
            counters["api_calls"] += len(tasks)

            # Aggregate and build insert rows
            rows = []
            for target_date, (date_str, day_data) in zip(dates_needed, results):
                agg = aggregate_day(day_data)
                if agg is None:
                    continue
                reading_ts = datetime(
                    target_date.year, target_date.month, target_date.day,
                    12, 0, 0, tzinfo=PHT,
                )
                rows.append({
                    "user_id": uid,
                    "system_id": system_id,
                    "timestamp": reading_ts.isoformat(),
                    **agg,
                })

            if dry_run:
                for row in rows[:3]:
                    log.info(
                        "[%d/%d] %s | %s | prod=%.2f cons=%.2f (dry-run)",
                        idx, total_users, name, row["timestamp"][:10],
                        row["production_kwh"], row["consumption_kwh"],
                    )
                if len(rows) > 3:
                    log.info("[%d/%d] %s | ... and %d more days", idx, total_users, name, len(rows) - 3)
            else:
                if rows:
                    try:
                        sb_batch_insert(sb, rows)
                    except Exception:
                        counters["errors"] += len(rows)
                        rows = []

            user_written = len(rows)
            counters["written"] += user_written
            counters["skipped"] += user_skipped
            counters["done"] += 1

            elapsed = time.time() - start_time
            rate = counters["written"] / elapsed * 60 if elapsed > 0 and counters["written"] > 0 else 0

            log.info(
                "[%d/%d] %s: %d written, %d skipped, range=%dd | elapsed=%.0fs, rate=%.0f/min, done=%d/%d",
                idx, total_users, name, user_written, user_skipped, user_days,
                elapsed, rate, counters["done"], total_users,
            )

    # ── Launch all users concurrently (bounded by user_sem) ───────────
    user_tasks = [
        process_user(user, i)
        for i, user in enumerate(users, 1)
    ]
    await asyncio.gather(*user_tasks)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 70}")
    print("BACKFILL SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Mode:                {mode_label}")
    print(f"  Users processed:     {total_users}")
    print(f"  Solis API calls:     {counters['api_calls']}")
    print(f"  Readings written:    {counters['written']}")
    print(f"  Readings skipped:    {counters['skipped']} (already existed)")
    print(f"  Errors:              {counters['errors']}")
    print(f"  Elapsed:             {elapsed:.0f}s ({elapsed/60:.1f} min)")
    eff_rate = counters['api_calls'] / elapsed if elapsed > 0 else 0
    print(f"  Effective rate:      {eff_rate:.1f} Solis calls/sec")
    if dry_run:
        print(f"\n  Pass --apply to write these readings to Supabase.")


if __name__ == "__main__":
    asyncio.run(main())
