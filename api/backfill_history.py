"""
Backfill historical Solis data into Supabase.

For each user with a solis_station_id, fetches monthly summaries from
stationMonth (pre-computed daily data from Solis) and writes one
energy_reading per day.

Smart mode (default): uses each station's installation_date from
solar_systems to determine how far back to fetch.

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

# Concurrency for Solis API — keep low to avoid 429s
SOLIS_CONCURRENCY = 2
# Batch size for Supabase upserts
SUPABASE_BATCH_SIZE = 500
# How many users to process in parallel
USER_CONCURRENCY = 1
# Delay between Solis API calls (seconds)
SOLIS_DELAY = 0.5


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def parse_month_day(day: dict) -> Optional[dict]:
    """Convert a stationMonth day record into an energy_reading row fragment.
    Returns None if dateStr is missing."""
    date_str = day.get("dateStr")
    if not date_str:
        return None

    production_kwh = float(day.get("energy") or 0)
    # Solis UI "Daily Grid Load" = homeGridEnergy; fall back to consumeEnergy.
    consumption_kwh = float(
        day.get("homeGridEnergy")
        if day.get("homeGridEnergy") is not None
        else (day.get("consumeEnergy") or 0)
    )
    grid_import_kwh = float(day.get("gridPurchasedEnergy") or 0)
    grid_export_kwh = float(day.get("gridSellEnergy") or 0)
    daily_earning = float(day.get("money") or 0)

    return {
        "date_str": date_str,
        "production_kwh": round(production_kwh, 4),
        "consumption_kwh": round(consumption_kwh, 4),
        "grid_export_kwh": round(grid_export_kwh, 4),
        "grid_import_kwh": round(grid_import_kwh, 4),
        "daily_earning": round(daily_earning, 2),
        "battery_level": None,
        "battery_status": None,
    }


async def fetch_one_month(solis, sem, station_id, month_str):
    """Fetch one month's data with concurrency control and rate-limit delay."""
    async with sem:
        await asyncio.sleep(SOLIS_DELAY)
        try:
            data = await solis.station_month(station_id, month_str)
            return month_str, data
        except SolisCloudError as e:
            log.debug("Solis error for station %s month %s: %s", station_id, month_str, e)
            return month_str, None
        except Exception as e:
            log.debug("Error for station %s month %s: %s", station_id, month_str, e)
            return month_str, None


def sb_batch_upsert(sb, rows):
    """Upsert rows into Supabase with retry, in batches.
    Uses user_id+timestamp as the conflict key so existing rows get updated."""
    for batch_start in range(0, len(rows), SUPABASE_BATCH_SIZE):
        batch = rows[batch_start:batch_start + SUPABASE_BATCH_SIZE]
        for attempt in range(5):
            try:
                sb.table("energy_readings").upsert(
                    batch, on_conflict="user_id,timestamp"
                ).execute()
                break
            except Exception as e:
                if attempt < 4:
                    wait = 2 ** (attempt + 1)
                    log.warning("Supabase batch upsert failed (attempt %d/5), retrying in %ds: %s", attempt + 1, wait, str(e)[:120])
                    time.sleep(wait)
                else:
                    log.error("Supabase batch upsert failed after 5 attempts: %s", str(e)[:200])
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

    log.info(
        "Pre-loaded: %d users, %d solar systems.",
        len(users), len(system_ids),
    )

    today = datetime.now(PHT).date()
    counters = {"written": 0, "errors": 0, "api_calls": 0, "done": 0}
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

            # Determine start date for backfill
            start_date = today - timedelta(days=user_days)

            # Determine which months to fetch
            months_needed = set()
            d = start_date
            while d < today:
                months_needed.add(d.strftime("%Y-%m"))
                if d.month == 12:
                    d = d.replace(year=d.year + 1, month=1, day=1)
                else:
                    d = d.replace(month=d.month + 1, day=1)

            # Fetch all needed months concurrently (shared Solis semaphore)
            tasks = [
                fetch_one_month(solis, solis_sem, station_id, m)
                for m in sorted(months_needed)
            ]
            results = await asyncio.gather(*tasks)
            counters["api_calls"] += len(tasks)

            # Parse and build upsert rows
            rows = []
            for month_str, month_data in results:
                if not month_data or not isinstance(month_data, list):
                    continue
                for day in month_data:
                    parsed = parse_month_day(day)
                    if parsed is None:
                        continue
                    ds = parsed.pop("date_str")
                    # Only include days within the backfill window
                    if ds < start_date.isoformat() or ds >= today.isoformat():
                        continue
                    parts = ds.split("-")
                    noon = datetime(
                        int(parts[0]), int(parts[1]), int(parts[2]),
                        12, 0, 0, tzinfo=PHT,
                    )
                    rows.append({
                        "user_id": uid,
                        "system_id": system_id,
                        "timestamp": noon.isoformat(),
                        **parsed,
                    })

            # Dedup rows on (user_id, timestamp): Solis occasionally returns the
            # same date under two adjacent months. Upsert rejects the whole batch
            # if a duplicate is present, so keep the last occurrence.
            if rows:
                seen: dict[tuple, dict] = {}
                for r in rows:
                    seen[(r["user_id"], r["timestamp"])] = r
                rows = list(seen.values())

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
                        sb_batch_upsert(sb, rows)
                    except Exception:
                        counters["errors"] += len(rows)
                        rows = []

            user_written = len(rows)
            counters["written"] += user_written
            counters["done"] += 1

            elapsed = time.time() - start_time
            rate = counters["written"] / elapsed * 60 if elapsed > 0 and counters["written"] > 0 else 0

            log.info(
                "[%d/%d] %s: %d upserted, range=%dd (%d months) | elapsed=%.0fs, rate=%.0f/min, done=%d/%d",
                idx, total_users, name, user_written, user_days, len(months_needed),
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
    print(f"  Readings upserted:   {counters['written']}")
    print(f"  Errors:              {counters['errors']}")
    print(f"  Elapsed:             {elapsed:.0f}s ({elapsed/60:.1f} min)")
    eff_rate = counters['api_calls'] / elapsed if elapsed > 0 else 0
    print(f"  Effective rate:      {eff_rate:.1f} Solis calls/sec")
    if dry_run:
        print(f"\n  Pass --apply to write these readings to Supabase.")


if __name__ == "__main__":
    asyncio.run(main())
