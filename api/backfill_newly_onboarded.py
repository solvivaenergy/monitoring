"""
Backfill historical DAILY readings for newly onboarded users from a station-id CSV.

This script is a targeted variant of backfill_history.py. It only processes users
whose user_profiles.solis_station_id appears in the provided CSV.

Default behavior:
- Dry-run (no writes)
- Only users that currently have zero rows in energy_readings
- Smart range per user based on solar_systems.installation_date (fallback: 180 days)

Usage:
    python -m api.backfill_newly_onboarded
    python -m api.backfill_newly_onboarded --apply
    python -m api.backfill_newly_onboarded --csv "Supabase Snippet Untitled query.csv" --apply
    python -m api.backfill_newly_onboarded --days 365 --apply
    python -m api.backfill_newly_onboarded --include-users-with-existing-daily --apply
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv
from supabase import Client, create_client

from api.backfill_history import parse_month_day, sb_batch_upsert
from api.solis_client import SolisCloudClient, SolisCloudError


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill_new")

PHT = timezone(timedelta(hours=8))
SOLIS_CONCURRENCY = 2
SOLIS_DELAY = 0.5
USER_CONCURRENCY = 1
DEFAULT_CSV = "Supabase Snippet Untitled query.csv"


def get_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Missing env var: {key}")
    return value


def load_environment() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    solviva_env = os.path.normpath(
        os.path.join(here, "..", "..", "Odoo Solviva", "solviva_service", ".env")
    )
    if os.path.exists(solviva_env):
        load_dotenv(solviva_env)
    load_dotenv(os.path.join(here, "..", ".env"))
    load_dotenv()


def build_supabase() -> Client:
    return create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))


def build_solis() -> SolisCloudClient:
    return SolisCloudClient(get_env("SOLIS_CLOUD_KEY_ID"), get_env("SOLIS_CLOUD_KEY_SECRET"))


def load_station_ids(csv_path: str) -> Set[str]:
    station_ids: Set[str] = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = str(row.get("station_id") or "").strip()
            if sid:
                station_ids.add(sid)
    return station_ids


def month_iter(start_date, end_date):
    d = start_date.replace(day=1)
    end_m = end_date.replace(day=1)
    while d <= end_m:
        yield d.strftime("%Y-%m")
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)


async def fetch_station_month(
    solis: SolisCloudClient,
    sem: asyncio.Semaphore,
    station_id: str,
    month_str: str,
) -> Optional[list]:
    async with sem:
        await asyncio.sleep(SOLIS_DELAY)
        try:
            data = await solis.station_month(station_id, month_str)
            return data if isinstance(data, list) else None
        except Exception as exc:
            log.debug("station_month failed station=%s month=%s: %s", station_id, month_str, exc)
            return None


async def backfill_station_ids(
    sb: Client,
    solis: SolisCloudClient,
    station_ids: Set[str],
    days: int,
    apply: bool = False,
    skip_users_with_readings: bool = True,
) -> Dict[str, int]:
    """Backfill up to `days` of daily history for the given station IDs.

    Targets only user_profiles mapped to those stations; by default skips users
    that already have any energy_readings. Reuses the same parse/consumption
    logic as backfill_history. Idempotent (upsert by user_id + timestamp).
    """
    counters: Dict[str, int] = {"users": 0, "api_calls": 0, "rows": 0, "errors": 0}
    if not station_ids:
        return counters

    users = (
        sb.table("user_profiles")
        .select("id,full_name,solis_station_id")
        .not_.is_("solis_station_id", "null")
        .execute()
        .data
        or []
    )
    target_users = [u for u in users if str(u.get("solis_station_id") or "") in station_ids]
    if not target_users:
        return counters

    # Keyed on solis_station_id, NOT user_id. `{str(r["user_id"]): r}` is
    # last-wins: a customer with two stations collapsed to whichever row
    # PostgREST returned last, so this backfill wrote one station's history
    # against the other's system_id. Station id is unique per row (migration
    # 04's UNIQUE(user_id, solis_station_id)), so this mapping is exact.
    systems = (
        sb.table("solar_systems")
        .select("id,user_id,solis_station_id,installation_date,status")
        .not_.is_("solis_station_id", "null")
        .eq("status", "active")
        .execute()
        .data
        or []
    )
    sys_by_station: Dict[str, Dict] = {
        str(r["solis_station_id"]).strip(): r for r in systems
    }

    if skip_users_with_readings:
        with_readings: Set[str] = set()
        offset = 0
        page_size = 1000
        while True:
            page = (
                sb.table("energy_readings")
                .select("user_id")
                .range(offset, offset + page_size - 1)
                .execute()
                .data
                or []
            )
            for row in page:
                uid = str(row.get("user_id") or "")
                if uid:
                    with_readings.add(uid)
            if len(page) < page_size:
                break
            offset += page_size
        target_users = [u for u in target_users if str(u["id"]) not in with_readings]

    if not target_users:
        return counters

    counters["users"] = len(target_users)
    solis_sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    today = datetime.now(PHT).date()
    start_date = today - timedelta(days=days)

    for user in target_users:
        uid = str(user["id"])
        station_id = str(user["solis_station_id"])
        name = user.get("full_name") or uid
        system = sys_by_station.get(station_id)
        system_id = system.get("id") if system else None

        if not system_id:
            try:
                detail = await solis.station_detail(station_id)
                station_name = detail.get("stationName") or station_id
                capacity = float(detail.get("capacity") or 0)
            except Exception as exc:
                log.error("backfill: station detail failed for %s: %s", name, exc)
                counters["errors"] += 1
                continue
            if not apply:
                system_id = "DRY-RUN"
            else:
                ins = (
                    sb.table("solar_systems")
                    .insert(
                        {
                            "user_id": uid,
                            "system_name": station_name,
                            "capacity_kwp": capacity,
                            "installation_date": start_date.isoformat(),
                            "address": "—",
                            "status": "active",
                        }
                    )
                    .execute()
                )
                system_id = ins.data[0]["id"]

        months = list(month_iter(start_date, today - timedelta(days=1)))
        month_results = await asyncio.gather(
            *[fetch_station_month(solis, solis_sem, station_id, m) for m in months]
        )
        counters["api_calls"] += len(months)

        rows: List[Dict] = []
        for month_data in month_results:
            if not month_data:
                continue
            for day in month_data:
                parsed = parse_month_day(day)
                if not parsed:
                    continue
                date_str = parsed.pop("date_str")
                if date_str < start_date.isoformat() or date_str >= today.isoformat():
                    continue
                y, m, d = [int(x) for x in date_str.split("-")]
                ts = datetime(y, m, d, 12, 0, 0, tzinfo=PHT).isoformat()
                rows.append({"user_id": uid, "system_id": system_id, "timestamp": ts, **parsed})

        dedup: Dict = {}
        for row in rows:
            dedup[(row["user_id"], row["timestamp"])] = row
        rows = list(dedup.values())

        if rows and apply:
            try:
                sb_batch_upsert(sb, rows)
            except Exception as exc:
                log.error("backfill: upsert failed for %s: %s", name, exc)
                counters["errors"] += len(rows)
                continue

        counters["rows"] += len(rows)
        log.info("backfill: %s: %d row(s) over %d day(s)", name, len(rows), days)

    return counters


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill newly onboarded users from station CSV")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="CSV with station_id column")
    parser.add_argument("--apply", action="store_true", help="Write to Supabase")
    parser.add_argument("--days", type=int, default=None, help="Fixed backfill days for all users")
    parser.add_argument(
        "--include-users-with-existing-daily",
        action="store_true",
        help="Include users even if they already have energy_readings rows",
    )
    args = parser.parse_args()

    load_environment()

    dry_run = not args.apply
    if dry_run:
        print("=" * 72)
        print("DRY RUN -- no writes")
        print("=" * 72)
    else:
        print("=" * 72)
        print("LIVE RUN -- writing backfill rows")
        print("=" * 72)

    sb = build_supabase()
    solis = build_solis()

    station_ids = load_station_ids(args.csv)
    if not station_ids:
        print("No station IDs found in CSV.")
        return

    users_resp = (
        sb.table("user_profiles")
        .select("id,full_name,solis_station_id")
        .not_.is_("solis_station_id", "null")
        .execute()
    )
    users = users_resp.data or []

    target_users = [u for u in users if str(u.get("solis_station_id") or "") in station_ids]
    if not target_users:
        print("No user_profiles mapped to CSV station IDs.")
        return

    # Keyed on solis_station_id, NOT user_id. `{str(r["user_id"]): r}` is
    # last-wins: a customer with two stations collapsed to whichever row
    # PostgREST returned last, so this backfill wrote one station's history
    # against the other's system_id. Station id is unique per row (migration
    # 04's UNIQUE(user_id, solis_station_id)), so this mapping is exact.
    systems = (
        sb.table("solar_systems")
        .select("id,user_id,solis_station_id,installation_date,status")
        .not_.is_("solis_station_id", "null")
        .eq("status", "active")
        .execute()
        .data
        or []
    )
    sys_by_station: Dict[str, Dict] = {
        str(r["solis_station_id"]).strip(): r for r in systems
    }

    existing_daily_users: Set[str] = set()
    page_size = 1000
    offset = 0
    while True:
        page = (
            sb.table("energy_readings")
            .select("user_id")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
            or []
        )
        for row in page:
            uid = str(row.get("user_id") or "")
            if uid:
                existing_daily_users.add(uid)
        if len(page) < page_size:
            break
        offset += page_size

    if not args.include_users_with_existing_daily:
        target_users = [u for u in target_users if str(u["id"]) not in existing_daily_users]

    if not target_users:
        print("No target users after filtering (all already have daily readings).")
        return

    solis_sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    user_sem = asyncio.Semaphore(USER_CONCURRENCY)

    today = datetime.now(PHT).date()
    counters = {
        "users": len(target_users),
        "api_calls": 0,
        "rows": 0,
        "errors": 0,
    }
    started = time.time()

    async def process_user(idx: int, user: Dict) -> None:
        async with user_sem:
            uid = str(user["id"])
            station_id = str(user["solis_station_id"])
            name = user.get("full_name") or uid

            system = sys_by_station.get(station_id)
            system_id = system.get("id") if system else None
            install_date = None
            if system and system.get("installation_date"):
                try:
                    install_date = datetime.fromisoformat(system["installation_date"]).date()
                except Exception:
                    install_date = None

            if args.days:
                start_date = today - timedelta(days=args.days)
            else:
                start_date = install_date or (today - timedelta(days=180))

            if start_date >= today:
                return

            if not system_id:
                try:
                    detail = await solis.station_detail(station_id)
                    station_name = detail.get("stationName") or station_id
                    capacity = float(detail.get("capacity") or 0)
                except Exception as exc:
                    log.error("[%d/%d] %s: failed to fetch station detail: %s", idx, counters["users"], name, exc)
                    counters["errors"] += 1
                    return

                if dry_run:
                    system_id = "DRY-RUN"
                else:
                    ins = (
                        sb.table("solar_systems")
                        .insert(
                            {
                                "user_id": uid,
                                "system_name": station_name,
                                "capacity_kwp": capacity,
                                "installation_date": start_date.isoformat(),
                                "address": "—",
                                "status": "active",
                            }
                        )
                        .execute()
                    )
                    system_id = ins.data[0]["id"]

            months = list(month_iter(start_date, today - timedelta(days=1)))
            month_tasks = [fetch_station_month(solis, solis_sem, station_id, m) for m in months]
            month_results = await asyncio.gather(*month_tasks)
            counters["api_calls"] += len(months)

            rows: List[Dict] = []
            for month_data in month_results:
                if not month_data:
                    continue
                for day in month_data:
                    parsed = parse_month_day(day)
                    if not parsed:
                        continue
                    date_str = parsed.pop("date_str")
                    if date_str < start_date.isoformat() or date_str >= today.isoformat():
                        continue
                    y, m, d = [int(x) for x in date_str.split("-")]
                    ts = datetime(y, m, d, 12, 0, 0, tzinfo=PHT).isoformat()
                    rows.append(
                        {
                            "user_id": uid,
                            "system_id": system_id,
                            "timestamp": ts,
                            **parsed,
                        }
                    )

            dedup = {}
            for row in rows:
                dedup[(row["user_id"], row["timestamp"])] = row
            rows = list(dedup.values())

            if rows and not dry_run:
                try:
                    sb_batch_upsert(sb, rows)
                except Exception as exc:
                    log.error("[%d/%d] %s: upsert failed: %s", idx, counters["users"], name, exc)
                    counters["errors"] += len(rows)
                    return

            counters["rows"] += len(rows)
            log.info(
                "[%d/%d] %s: %d row(s) | range=%s..%s | %d month(s)",
                idx,
                counters["users"],
                name,
                len(rows),
                start_date.isoformat(),
                (today - timedelta(days=1)).isoformat(),
                len(months),
            )

    await asyncio.gather(*[process_user(i, u) for i, u in enumerate(target_users, 1)])

    elapsed = time.time() - started
    print("\n" + "=" * 72)
    print("TARGETED BACKFILL SUMMARY")
    print("=" * 72)
    print(f"Target users processed:    {counters['users']}")
    print(f"Solis API calls:           {counters['api_calls']}")
    print(f"Rows prepared/upserted:    {counters['rows']}")
    print(f"Errors:                    {counters['errors']}")
    print(f"Elapsed:                   {elapsed:.1f}s")
    if dry_run:
        print("\nPass --apply to write these rows.")


if __name__ == "__main__":
    asyncio.run(main())
