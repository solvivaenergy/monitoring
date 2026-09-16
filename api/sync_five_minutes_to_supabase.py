"""
Solis -> Supabase interval sync.

Runs every 15 minutes and fetches today's stationDay curve for every mapped
Solis station, storing each 5-minute interval in the energy_readings_five_minutes
table.

Designed for Render cron:
    python -m api.sync_five_minutes_to_supabase

Optional flags:
    --dry-run   Parse and log counts without writing to Supabase
"""

import asyncio
import logging
import os
import time
from decimal import Decimal, InvalidOperation
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

# Purge sizing. PostgREST connects as `authenticator`, whose statement_timeout is
# 8s, so every DELETE has to fit inside that. Rows go one hour-window at a
# time: an hour of ~574 stations at 5-minute resolution is ~7,000 rows, which
# deletes in well under a second with the "timestamp" index from migration 01.
# 168 windows is a week of backlog per run; anything older waits for the next
# run 15 minutes later.
#
# Windows by timestamp, NOT lists of ids. PostgREST filters travel in the URL,
# and measured on 2026-09-16: 500 UUIDs works, 1,000 gets HTTP 400 from the
# gateway, 5,000 is refused by the HTTP client before it is even sent. The first
# version of this fix batched 5,000 ids and failed on its first request.
PURGE_WINDOW = timedelta(hours=1)
PURGE_MAX_WINDOWS = 24 * 7


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


def _is_retryable_supabase_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "522" in message:
        return True

    status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    return status_code == 522


def _execute_with_retry(action_name: str, factory):
    last_exc: Exception | None = None

    for attempt in range(3):
        try:
            return factory().execute()
        except Exception as exc:
            last_exc = exc
            if not _is_retryable_supabase_error(exc) or attempt == 2:
                raise
            delay_seconds = 0.5 * (attempt + 1)
            log.warning(
                "%s failed on attempt %d; retrying in %.1fs: %s",
                action_name,
                attempt + 1,
                delay_seconds,
                exc,
            )
            time.sleep(delay_seconds)

    raise RuntimeError(f"{action_name} failed after retries") from last_exc


def _extract_lifetime_earning(station_all: object) -> float:
    if isinstance(station_all, list) and station_all:
        total = 0.0
        for payload in station_all:
            total += _to_float(payload.get("money"))
        return round(total, 2)
    elif isinstance(station_all, dict):
        return round(_to_float(station_all.get("money")), 2)
    else:
        return 0.0


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


def _build_row(
    user_id: str,
    system_id: str,
    point: dict,
    lifetime_earning: Optional[float] = None,
) -> Optional[Tuple[int, dict]]:
    ts = _parse_timestamp(point)
    if ts is None:
        return None

    production_w = _to_float(point.get("power"))
    # Household consumption in watts.
    # Use grid-side household load + backup/bypass load when available.
    # Keep familyLoadPower as fallback for older payload shapes.
    family_load_w = _to_float(point.get("familyLoadPower"))
    backup_load_w = _to_float(point.get("bypassLoadPower"))
    consumption_w = family_load_w + backup_load_w
    if consumption_w <= 0:
        consumption_w = family_load_w
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
    if lifetime_earning is not None:
        row["lifetime_earning"] = lifetime_earning
    return int(ts.timestamp()), row


def _chunked(rows: List[dict], chunk_size: int) -> List[List[dict]]:
    return [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]


def _load_existing_rows(
    sb: Client,
    day_start: str,
    day_end: str,
) -> Dict[str, Dict[int, str]]:
    # Keyed on system_id, NOT user_id.
    #
    # This index decides whether a Solis data point is already stored. Keyed on
    # user_id, a customer's second station found the FIRST station's row at the
    # same timestamp, concluded the point was already present and `continue`d —
    # so the second station's readings were silently discarded all day, except
    # its latest timestamp, which overwrote the first station's. The failure was
    # invisible: no error, and the log line still reported intervals parsed.
    existing_by_system: Dict[str, Dict[int, str]] = {}
    offset = 0

    while True:
        start = offset
        batch = _execute_with_retry(
            "load existing five-minute rows",
            lambda s=start: (
                sb.table("energy_readings_five_minutes")
                .select("id, system_id, timestamp")
                .gte("timestamp", day_start)
                .lt("timestamp", day_end)
                .range(s, s + SUPABASE_PAGE_SIZE - 1)
            ),
        ).data or []

        if not batch:
            break

        for row in batch:
            system_rows = existing_by_system.setdefault(row["system_id"], {})
            system_rows[_normalize_timestamp_key(row["timestamp"])] = row["id"]

        if len(batch) < SUPABASE_PAGE_SIZE:
            break

        offset += SUPABASE_PAGE_SIZE

    return existing_by_system


def _purge_old_rows(sb: Client, day_start: str) -> Optional[int]:
    """Drop rows older than the current Asia/Manila day, in bounded batches.

    This used to be one unbounded DELETE. It deadlocked the whole feed on
    2026-09-15: at Manila midnight the cutoff advances a full day, so every row
    in this rolling table — 160,788 of them, 79 MB — becomes eligible at once,
    and PostgREST runs as `authenticator`, whose statement_timeout is 8s. The
    delete could not finish, raised 57014, and because the purge runs before any
    write and its exception aborted the run, NOTHING was ever written. The
    backlog therefore never shrank and every subsequent run failed the same way.
    The feed was dead for ~19.5 hours (roughly 78 missed runs) until it was
    cleared by hand.

    Two independent guards, because either alone would have left the outage
    possible:

    1. Bounded windows. Each DELETE covers one PURGE_WINDOW of "timestamp"
       (an hour, ~7,000 rows) so its work is proportional to the window, never
       to the size of the backlog, and we walk windows from the oldest row up
       to the cutoff. The filter is a timestamp range, not a list of ids —
       PostgREST filters travel in the URL and a few hundred UUIDs is already
       the limit (see PURGE_WINDOW above).

    2. Never fatal. A purge failure returns None instead of propagating. Purging
       is housekeeping — the table is a rolling one-day cache, and carrying a
       stale day costs disk. Writing today's curve is the actual job. Letting
       housekeeping abort the job is what turned a slow query into an outage.

    Returns the number of rows deleted, or None if the purge could not complete
    (the caller only logs it).
    """
    deleted = 0
    windows = 0
    try:
        oldest = _execute_with_retry(
            "find oldest five-minute row",
            lambda: sb.table("energy_readings_five_minutes")
            .select("timestamp")
            .lt("timestamp", day_start)
            .order("timestamp")
            .limit(1),
        ).data or []
        if not oldest:
            return 0

        cutoff = datetime.fromisoformat(day_start)
        window_start = datetime.fromisoformat(
            str(oldest[0]["timestamp"]).replace("Z", "+00:00")
        ).replace(minute=0, second=0, microsecond=0)

        while window_start < cutoff and windows < PURGE_MAX_WINDOWS:
            window_end = min(window_start + PURGE_WINDOW, cutoff)
            resp = _execute_with_retry(
                "purge old five-minute rows",
                lambda ws=window_start, we=window_end: sb.table("energy_readings_five_minutes")
                .delete(count="exact")
                .gte("timestamp", ws.isoformat())
                .lt("timestamp", we.isoformat()),
            )
            deleted += resp.count or 0
            window_start = window_end
            windows += 1

        if window_start < cutoff:
            log.warning(
                "Purge stopped after %d hour-window(s) with rows still older than %s; "
                "the rest will go on the next run.",
                windows,
                day_start,
            )
        return deleted
    except Exception as exc:
        log.warning(
            "Purge failed after deleting %d row(s) in %d window(s); continuing with the sync: %s",
            deleted,
            windows,
            exc,
        )
        return None


def _has_lifetime_earning_column(sb: Client) -> bool:
    try:
        _execute_with_retry(
            "check lifetime earning column",
            lambda: sb.table("energy_readings_five_minutes").select("id, lifetime_earning").limit(1),
        )
        return True
    except Exception:
        return False


async def _ensure_active_system(
    sb: Client,
    solis: SolisCloudClient,
    system_ids: Dict[str, str],
    user_id: str,
    station_id: str,
) -> str:
    raise NotImplementedError(
        "Removed. This created a solar_systems row when its user_id lookup "
        "missed, with address '-'. Together with the daily sync's '—' insert "
        "it produced the 104 duplicate pairs of 2026-08-10 and 2026-08-20: "
        "both crons ran 'find an active row for this user, else INSERT', they "
        "overlap at 18:00 UTC, and there was no unique constraint to stop them. "
        "A sync job must never create a station. Onboarding does that, and "
        "migration 04's UNIQUE(user_id, solis_station_id) now enforces it."
    )


async def _fetch_station_day(
    solis: SolisCloudClient,
    sem: asyncio.Semaphore,
    user: dict,
    today_str: str,
) -> Tuple[dict, Optional[list], Optional[float], Optional[Exception]]:
    async with sem:
        try:
            station_id = user["solis_station_id"]
            day_data, station_all = await asyncio.gather(
                solis.station_day(station_id, today_str),
                solis.station_all(station_id),
            )
            lifetime_earning = _extract_lifetime_earning(station_all)
            return user, day_data if isinstance(day_data, list) else None, lifetime_earning, None
        except Exception as exc:
            return user, None, None, exc


async def sync_once(dry_run: bool = False) -> int:
    sb = build_supabase()
    solis = build_solis()

    # The unit of work is a STATION, not a user. This was two queries joined by
    # a dict keyed on user_id:
    #     system_ids = {row["user_id"]: row["id"] for row in ...}
    # which is last-wins — a customer with two systems silently collapsed to
    # whichever row PostgREST returned last, and BOTH stations' 5-minute points
    # were then written against it. solar_systems.solis_station_id (migration
    # 04) makes the station the primary record, so the join is unnecessary.
    #
    # Paginated: an unbounded PostgREST select silently truncates at 1000 rows.
    stations: List[dict] = []
    page_size = 1000
    offset = 0
    while True:
        start = offset
        page = _execute_with_retry(
            "load active stations",
            lambda s=start: (
                sb.table("solar_systems")
                .select("id, user_id, solis_station_id, system_name")
                .not_.is_("solis_station_id", "null")
                .eq("status", "active")
                .order("id")
                .range(s, s + page_size - 1)
            ),
        ).data or []
        stations.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    if not stations:
        log.info("No active stations with a solis_station_id. Nothing to sync.")
        return 0

    # Shape each station like the old `user` dict so downstream code (which
    # reads user["id"], user["solis_station_id"], user["full_name"]) is
    # unchanged, but carry system_id explicitly rather than deriving it.
    users = [
        {
            "id": s["user_id"],
            "full_name": s.get("system_name") or s["solis_station_id"],
            "solis_station_id": s["solis_station_id"],
            "system_id": s["id"],
        }
        for s in stations
    ]

    today = datetime.now(PHT).date()
    day_start = f"{today.isoformat()}T00:00:00+08:00"
    day_end = f"{(today + timedelta(days=1)).isoformat()}T00:00:00+08:00"
    today_str = today.isoformat()
    # --dry-run used to purge anyway: the flag is documented as "without writing
    # to Supabase", but the purge ran unconditionally, so a dry run deleted a
    # day of five-minute data. Deleting is writing.
    if dry_run:
        stale = _execute_with_retry(
            "count stale five-minute rows",
            lambda: sb.table("energy_readings_five_minutes")
            .select("id", count="exact")
            .lt("timestamp", day_start)
            .limit(1),
        )
        log.info("Would purge %s old 5-minute row(s) [dry-run].", stale.count or 0)
    else:
        deleted_count = _purge_old_rows(sb, day_start)
        log.info("Purged %s old 5-minute row(s) before syncing %s.", deleted_count or 0, today_str)
    existing_by_system = _load_existing_rows(sb, day_start, day_end)
    has_lifetime_earning = _has_lifetime_earning_column(sb)
    if not has_lifetime_earning:
        log.warning(
            "energy_readings_five_minutes has no lifetime_earning column yet; "
            "5-minute sync will skip writing lifetime earnings until the column is added."
        )

    prepared_users: List[dict] = []

    # Every station already carries its system_id, so there is nothing to
    # resolve and nothing to create. _ensure_active_system used to INSERT a
    # solar_systems row here when its user_id lookup missed — the '-' half of
    # the 104 duplicate pairs, racing the daily cron's '—' insert at 18:00 UTC.
    # Station creation belongs to onboarding; this cron only reads.
    prepared_users = users

    fetch_sem = asyncio.Semaphore(SOLIS_CONCURRENCY)
    fetch_results = await asyncio.gather(*[
        _fetch_station_day(solis, fetch_sem, user, today_str)
        for user in prepared_users
    ])

    total_written = 0
    all_inserts: List[dict] = []
    all_updates: List[Tuple[str, dict]] = []

    for user, day_data, lifetime_earning, error in fetch_results:
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

        parsed_by_ts: Dict[int, dict] = {}
        for point in day_data:
            built = _build_row(
                user_id,
                system_id,
                point,
                lifetime_earning=lifetime_earning if has_lifetime_earning else None,
            )
            if built is not None:
                ts_key, row = built
                # Solis can occasionally repeat a timestamp; keep the latest copy.
                parsed_by_ts[ts_key] = row

        parsed_rows = sorted(parsed_by_ts.items())

        if not parsed_rows:
            log.info("%s: Solis returned no parseable 5-minute points", name)
            continue

        # Look up THIS STATION's already-stored points, not the customer's.
        existing_by_ts = existing_by_system.get(system_id, {})
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
            existing_by_ts[ts_key] = "__pending_insert__"

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

    # One upsert pass, with retry. Until 2026-09-16 the inserts were upserted
    # TWICE — a retry-less loop here and the retrying loop below, identical
    # batches — and the "latest point" refresh was a separate UPDATE per
    # station, ~520 sequential round-trips. On a full table (~125k rows) that
    # write phase took ~11 minutes of a run that has a 15-minute slot. When a
    # run overruns, Render skips the next one, and the feed measured on
    # 2026-09-16 was landing every ~40 minutes instead of every 15.
    #
    # The refreshed latest rows go through the same upsert: ON CONFLICT
    # (system_id, "timestamp") updates them in place, which is exactly what
    # update(row).eq("id", ...) did, in 1/500th of the requests.
    all_inserts.extend(row for _, row in all_updates)
    for batch in _chunked(all_inserts, SUPABASE_BATCH_SIZE):
        _execute_with_retry(
            f"upsert batch of {len(batch)} five-minute rows",
            lambda batch=batch: sb.table("energy_readings_five_minutes").upsert(batch, on_conflict="system_id,timestamp"),
        )

    return total_written


async def main() -> None:
    dry_run = "--dry-run" in os.sys.argv
    written = await sync_once(dry_run=dry_run)
    suffix = " [dry-run]" if dry_run else ""
    log.info("Done. %d row(s) processed%s.", written, suffix)


if __name__ == "__main__":
    asyncio.run(main())
