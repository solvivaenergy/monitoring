"""
Solis -> Supabase 5-minute interval sync.

Fetches today's stationDay curve for every mapped Solis station and stores each
5-minute interval in the energy_readings_five_minutes table.

Designed for Render cron:
    python -m api.sync_five_minutes_to_supabase

Optional flags:
    --dry-run   Parse and log counts without writing to Supabase
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import Client, create_client

from api.solis_client import SolisCloudClient, SolisCloudError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("solis_five_minute_sync")
logging.getLogger("httpx").setLevel(logging.WARNING)

PHT = timezone(timedelta(hours=8))
SUPABASE_BATCH_SIZE = 500
SUPABASE_PAGE_SIZE = 1000
SOLIS_CONCURRENCY = 8


def get_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def build_supabase() -> Client:
    return create_client(
        get_env("SUPABASE_URL"),
        get_env("SUPABASE_SERVICE_KEY"),
    )


def build_solis() -> SolisCloudClient:
    return SolisCloudClient(
        key_id=get_env("SOLIS_CLOUD_KEY_ID"),
        key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"),
    )


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_timestamp(point: dict) -> Optional[datetime]:
    ts_ms = point.get("time") or point.get("dataTimestamp")
    if ts_ms is None:
        return None

    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=PHT)
    except (TypeError, ValueError, OSError):
        return None


def _normalize_timestamp_key(timestamp_str: str) -> int:
    return int(
        datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).timestamp()
    )


def _battery_status(point: dict) -> Optional[str]:
    battery_power = _to_float(point.get("batteryPower"))
    if battery_power > 0:
        return "charging"
    if battery_power < 0:
        return "discharging"
    if point.get("batteryCapacitySoc") is not None:
        return "idle"
    return None


def _build_row(user_id: str, system_id: str, point: dict) -> Optional[Tuple[int, dict]]:
    ts = _parse_timestamp(point)
    if ts is None:
        return None

    production_w = _to_float(point.get("power"))
    consumption_w = _to_float(point.get("consumeEnergy"))
    grid_w = _to_float(point.get("psum"))
    battery_level_raw = point.get("batteryCapacitySoc")
    battery_level = (
        round(_to_float(battery_level_raw), 1)
        if battery_level_raw is not None and battery_level_raw != ""
        else None
    )

    row = {
        "user_id": user_id,
        "system_id": system_id,
        "timestamp": ts.isoformat(),
        "production_kwh": round(production_w * (5 / 60) / 1000, 6),
        "consumption_kwh": round(consumption_w * (5 / 60) / 1000, 6),
        "battery_level": battery_level,
        "battery_status": _battery_status(point),
        "grid_import_kwh": round(abs(min(grid_w, 0.0)) * (5 / 60) / 1000, 6),
        "grid_export_kwh": round(max(grid_w, 0.0) * (5 / 60) / 1000, 6),
        "daily_earning": 0,
    }
    return int(ts.timestamp()), row


def _chunked(rows: List[dict], chunk_size: int) -> List[List[dict]]:
    return [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]


def _load_existing_rows(
    sb: Client,
    day_start: str,
    day_end: str,
) -> Dict[str, Dict[int, str]]:
    existing_by_user: Dict[str, Dict[int, str]] = {}
    offset = 0

    while True:
        batch = (
            sb.table("energy_readings_five_minutes")
            .select("id, user_id, timestamp")
            .gte("timestamp", day_start)
            .lt("timestamp", day_end)
            .range(offset, offset + SUPABASE_PAGE_SIZE - 1)
            .execute()
        ).data or []

        if not batch:
            break

        for row in batch:
            user_rows = existing_by_user.setdefault(row["user_id"], {})
            user_rows[_normalize_timestamp_key(row["timestamp"])] = row["id"]

        if len(batch) < SUPABASE_PAGE_SIZE:
            break

        offset += SUPABASE_PAGE_SIZE

    return existing_by_user


def _purge_old_rows(sb: Client, day_start: str) -> Optional[int]:
    resp = (
        sb.table("energy_readings_five_minutes")
        .delete(count="exact")
        .lt("timestamp", day_start)
        .execute()
    )
    return resp.count


async def _ensure_active_system(
    sb: Client,
    solis: SolisCloudClient,
    system_ids: Dict[str, str],
    user_id: str,
    station_id: str,
) -> str:
    existing = system_ids.get(user_id)
    if existing:
        return existing

    station = await solis.station_detail(station_id)
    insert_resp = (
        sb.table("solar_systems")
        .insert({
            "user_id": user_id,
            "system_name": station.get("stationName") or station_id,
            "capacity_kwp": _to_float(station.get("capacity")),
            "installation_date": datetime.now(PHT).date().isoformat(),
            "address": "-",
            "status": "active",
        })
        .execute()
    )
    system_id = insert_resp.data[0]["id"]
    system_ids[user_id] = system_id
    log.info("Created active solar_systems row for user %s", user_id)
    return system_id


async def _fetch_station_day(
    solis: SolisCloudClient,
    sem: asyncio.Semaphore,
    user: dict,
    today_str: str,
) -> Tuple[dict, Optional[list], Optional[Exception]]:
    async with sem:
        try:
            data = await solis.station_day(user["solis_station_id"], today_str)
            return user, data if isinstance(data, list) else None, None
        except Exception as exc:
            return user, None, exc


async def sync_once(dry_run: bool = False) -> int:
    sb = build_supabase()
    solis = build_solis()

    users_resp = (
        sb.table("user_profiles")
        .select("id, full_name, solis_station_id")
        .not_.is_("solis_station_id", "null")
        .execute()
    )
    users = users_resp.data or []

    if not users:
        log.info("No users with solis_station_id mapped. Nothing to sync.")
        return 0

    systems_resp = (
        sb.table("solar_systems")
        .select("id, user_id, status")
        .eq("status", "active")
        .execute()
    )
    system_ids = {row["user_id"]: row["id"] for row in (systems_resp.data or [])}

    today = datetime.now(PHT).date()
    day_start = f"{today.isoformat()}T00:00:00+08:00"
    day_end = f"{(today + timedelta(days=1)).isoformat()}T00:00:00+08:00"
    today_str = today.isoformat()
    deleted_count = _purge_old_rows(sb, day_start)
    log.info("Purged %s old 5-minute row(s) before syncing %s.", deleted_count or 0, today_str)
    existing_by_user = _load_existing_rows(sb, day_start, day_end)

    prepared_users: List[dict] = []

    for user in users:
        user_id = user["id"]
        station_id = user["solis_station_id"]

        try:
            system_id = await _ensure_active_system(sb, solis, system_ids, user_id, station_id)
            prepared_users.append({**user, "system_id": system_id})

        except SolisCloudError as exc:
            log.error("Solis API error for %s (station %s): %s", user_id, station_id, exc)
        except Exception as exc:
            log.error("Sync failed for %s (station %s): %s", user_id, station_id, exc)

    fetch_sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    fetch_results = await asyncio.gather(*[
        _fetch_station_day(solis, fetch_sem, user, today_str)
        for user in prepared_users
    ])

    total_written = 0
    all_inserts: List[dict] = []
    all_updates: List[Tuple[str, dict]] = []

    for user, day_data, error in fetch_results:
        user_id = user["id"]
        station_id = user["solis_station_id"]
        system_id = user["system_id"]
        name = user.get("full_name") or user_id

        if error:
            if isinstance(error, SolisCloudError):
                log.error("Solis API error for %s (station %s): %s", name, station_id, error)
            else:
                log.error("Sync failed for %s (station %s): %s", name, station_id, error)
            continue

        if not day_data:
            log.info("%s: no stationDay data for %s", name, today_str)
            continue

        parsed_rows: List[Tuple[int, dict]] = []
        for point in day_data:
            built = _build_row(user_id, system_id, point)
            if built is not None:
                parsed_rows.append(built)

        if not parsed_rows:
            log.info("%s: Solis returned no parseable 5-minute points", name)
            continue

        existing_by_ts = existing_by_user.get(user_id, {})
        latest_ts = max(ts_key for ts_key, _ in parsed_rows)
        inserts: List[dict] = []
        updates: List[Tuple[str, dict]] = []

        for ts_key, row in parsed_rows:
            existing_id = existing_by_ts.get(ts_key)
            if existing_id:
                if ts_key == latest_ts:
                    updates.append((existing_id, row))
                continue
            inserts.append(row)

        total_written += len(inserts) + len(updates)
        all_inserts.extend(inserts)
        all_updates.extend(updates)

        suffix = " [dry-run]" if dry_run else ""
        log.info(
            "%s: %d interval(s) parsed, %d insert(s), %d latest update(s)%s",
            name,
            len(parsed_rows),
            len(inserts),
            len(updates),
            suffix,
        )

    if dry_run:
        return total_written

    for batch in _chunked(all_inserts, SUPABASE_BATCH_SIZE):
        sb.table("energy_readings_five_minutes").insert(batch).execute()

    for row_id, row in all_updates:
        sb.table("energy_readings_five_minutes").update(row).eq("id", row_id).execute()

    return total_written


async def main() -> None:
    dry_run = "--dry-run" in os.sys.argv
    written = await sync_once(dry_run=dry_run)
    suffix = " [dry-run]" if dry_run else ""
    log.info("Done. %d row(s) processed%s.", written, suffix)


if __name__ == "__main__":
    asyncio.run(main())
