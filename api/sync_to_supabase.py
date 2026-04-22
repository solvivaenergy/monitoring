"""
Solis → Supabase Daily Sync

Runs once daily (10:00 AM PHT via Render cron) for all users that have a
solis_station_id mapped in user_profiles.  Uses the Solis stationMonth API
to get pre-computed daily summaries (production, consumption, grid, earning)
and upserts them into Supabase energy_readings.

Real-time data (Today chart, current power) comes from /app/live which
hits Solis on-demand — no need for frequent syncing.

Usage:
    # One-shot sync (used by cron)
    python -m api.sync_to_supabase

    # Continuous (legacy, not recommended)
    python -m api.sync_to_supabase --loop

Environment variables:
    SOLIS_CLOUD_KEY_ID       – Solis Cloud API key ID
    SOLIS_CLOUD_KEY_SECRET   – Solis Cloud API secret
    SUPABASE_URL             – Supabase project URL
    SUPABASE_SERVICE_KEY     – Supabase service-role key (bypasses RLS)
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from api.solis_client import SolisCloudClient, SolisCloudError

# supabase-py (sync client)
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("solis_sync")

SYNC_INTERVAL_SECONDS = 900  # 15 minutes


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


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


async def _fetch_battery_capacity_kwh(
    solis: SolisCloudClient, station_id: str
) -> Optional[float]:
    """Best-effort lookup of the battery pack's rated capacity in kWh.

    Tries the first inverter on the station and inspects several Solis fields
    that may carry capacity info. Returns None if the station has no battery
    or the value can't be determined.
    """
    try:
        inv_list = await solis.list_inverters(station_id, page_size=1)
    except Exception as e:
        log.debug("list_inverters failed for station %s: %s", station_id, e)
        return None

    records = None
    if isinstance(inv_list, dict):
        page = inv_list.get("page")
        if isinstance(page, dict):
            records = page.get("records")
        if not records:
            records = inv_list.get("records")
    if not records:
        return None

    inverter_id = records[0].get("id") or records[0].get("sn")
    if not inverter_id:
        return None

    try:
        detail = await solis.inverter_detail(str(inverter_id))
    except Exception as e:
        log.debug("inverter_detail failed for %s: %s", inverter_id, e)
        return None

    if not isinstance(detail, dict):
        return None

    # 1) Direct kWh fields some Solis firmwares expose
    for key in ("batteryCapacityKwh", "batteryCapacityEnergy", "batteryTotalCapacity"):
        val = detail.get(key)
        if val not in (None, "", 0, "0"):
            try:
                return round(float(val), 2)
            except (TypeError, ValueError):
                pass

    # 2) Derive from Ah * V (most common)
    ah = detail.get("storageBatteryCapacity") or detail.get("batteryCapacity")
    v = detail.get("storageBatteryVoltage") or detail.get("batteryVoltage")
    try:
        if ah and v:
            kwh = float(ah) * float(v) / 1000.0
            if kwh > 0:
                return round(kwh, 2)
    except (TypeError, ValueError):
        pass

    return None


async def sync_once(solis: SolisCloudClient, sb: Client) -> int:
    """Run one sync cycle. Returns number of readings written."""

    # 1. Get all users with a mapped Solis station
    resp = (
        sb.table("user_profiles")
        .select("id, full_name, solis_station_id")
        .not_.is_("solis_station_id", "null")
        .execute()
    )
    users = resp.data or []

    if not users:
        log.info("No users with solis_station_id mapped — nothing to sync.")
        return 0

    log.info("Found %d user(s) to sync.", len(users))
    written = 0

    now = datetime.now(timezone(timedelta(hours=8)))  # PHT
    current_month = now.strftime("%Y-%m")

    for user in users:
        user_id: str = user["id"]
        station_id: str = user["solis_station_id"]
        name: str = user.get("full_name", "?")

        try:
            # 2a. Fetch station detail for system metadata
            station = await solis.station_detail(station_id)
            capacity_kwp = float(station.get("capacity") or 0)
            station_name = station.get("stationName") or station.get("sno") or station_id

            # 2a-bis. Best-effort battery capacity from inverterDetail (kWh)
            battery_capacity_kwh = await _fetch_battery_capacity_kwh(solis, station_id)

            # 2b. Fetch current month's daily summaries (1 API call, all days)
            month_data = await solis.station_month(station_id, current_month)

            if not month_data or not isinstance(month_data, list):
                log.warning("No stationMonth data for %s — skipping.", name)
                continue

            # 3. Ensure solar_systems row exists
            sys_resp = (
                sb.table("solar_systems")
                .select("id")
                .eq("user_id", user_id)
                .eq("status", "active")
                .limit(1)
                .execute()
            )
            # Helper: find the oldest reading timestamp for this user
            def _oldest_reading_date() -> str:
                oldest = (
                    sb.table("energy_readings")
                    .select("timestamp")
                    .eq("user_id", user_id)
                    .order("timestamp", desc=False)
                    .limit(1)
                    .execute()
                )
                if oldest.data:
                    return oldest.data[0]["timestamp"][:10]
                return now.strftime("%Y-%m-%d")

            if sys_resp.data:
                system_id = sys_resp.data[0]["id"]
                update_payload = {
                    "system_name": station_name,
                    "capacity_kwp": capacity_kwp,
                }
                if battery_capacity_kwh is not None:
                    update_payload["battery_capacity_kwh"] = battery_capacity_kwh
                # Backfill installation_date if it was set to a recent placeholder
                sys_row = (
                    sb.table("solar_systems")
                    .select("installation_date")
                    .eq("id", system_id)
                    .single()
                    .execute()
                )
                current_install = sys_row.data.get("installation_date") if sys_row.data else None
                if current_install and current_install >= (now - timedelta(days=30)).strftime("%Y-%m-%d"):
                    update_payload["installation_date"] = _oldest_reading_date()
                sb.table("solar_systems").update(update_payload).eq("id", system_id).execute()
            else:
                install_date = _oldest_reading_date()
                insert_payload = {
                    "user_id": user_id,
                    "system_name": station_name,
                    "capacity_kwp": capacity_kwp,
                    "installation_date": install_date,
                    "address": "—",
                    "status": "active",
                }
                if battery_capacity_kwh is not None:
                    insert_payload["battery_capacity_kwh"] = battery_capacity_kwh
                insert_resp = (
                    sb.table("solar_systems")
                    .insert(insert_payload)
                    .execute()
                )
                system_id = insert_resp.data[0]["id"]
                log.info("Created solar_systems row for %s (system_id=%s)", name, system_id)

            # 4. Upsert each day's reading from stationMonth data
            user_written = 0
            for day in month_data:
                date_str = day.get("dateStr")  # "2026-04-08"
                if not date_str:
                    continue

                production_kwh = float(day.get("energy") or 0)
                # Solis UI "Daily Grid Load" = homeGridEnergy (non-backup grid-side loads).
                # Fall back to consumeEnergy (total home load incl. backup + battery
                # round-trip) only if homeGridEnergy is missing.
                consumption_kwh = float(
                    day.get("homeGridEnergy")
                    if day.get("homeGridEnergy") is not None
                    else (day.get("consumeEnergy") or 0)
                )
                grid_import_kwh = float(day.get("gridPurchasedEnergy") or 0)
                grid_export_kwh = float(day.get("gridSellEnergy") or 0)
                daily_earning = float(day.get("money") or 0)

                # Build noon timestamp for this date
                parts = date_str.split("-")
                noon = datetime(
                    int(parts[0]), int(parts[1]), int(parts[2]),
                    12, 0, 0, tzinfo=timezone(timedelta(hours=8)),
                )

                reading = {
                    "user_id": user_id,
                    "system_id": system_id,
                    "timestamp": noon.isoformat(),
                    "production_kwh": round(production_kwh, 4),
                    "consumption_kwh": round(consumption_kwh, 4),
                    "battery_level": None,
                    "battery_status": None,
                    "grid_import_kwh": round(grid_import_kwh, 4),
                    "grid_export_kwh": round(grid_export_kwh, 4),
                    "daily_earning": round(daily_earning, 2),
                }

                # Check if this day's reading already exists
                existing = (
                    sb.table("energy_readings")
                    .select("id")
                    .eq("user_id", user_id)
                    .gte("timestamp", f"{date_str}T00:00:00+08:00")
                    .lt("timestamp", f"{date_str}T23:59:59+08:00")
                    .limit(1)
                    .execute()
                )
                if existing.data:
                    sb.table("energy_readings").update(reading).eq("id", existing.data[0]["id"]).execute()
                else:
                    sb.table("energy_readings").insert(reading).execute()

                user_written += 1

            written += user_written
            log.info(
                "Synced %s | %d days | month=%s",
                name, user_written, current_month,
            )

        except SolisCloudError as e:
            log.error("Solis API error for %s (station %s): %s", name, station_id, e)
        except Exception as e:
            log.error("Unexpected error for %s (station %s): %s", name, station_id, e)

    return written


async def run_loop(solis: SolisCloudClient, sb: Client) -> None:
    """Continuously sync every 5 minutes."""
    log.info("Starting continuous sync (interval=%ds)…", SYNC_INTERVAL_SECONDS)
    while True:
        try:
            count = await sync_once(solis, sb)
            log.info("Cycle complete — %d reading(s) written. Sleeping %ds…", count, SYNC_INTERVAL_SECONDS)
        except Exception as e:
            log.error("Sync cycle failed: %s — retrying next cycle.", e)
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


async def main() -> None:
    sb = build_supabase()
    solis = build_solis()

    if "--loop" in sys.argv:
        await run_loop(solis, sb)
    else:
        count = await sync_once(solis, sb)
        log.info("Done — %d reading(s) written.", count)

        # Sync referral codes from Odoo → user_profiles
        try:
            from api.sync_referral_codes import sync_referral_codes
            log.info("Starting referral code sync from Odoo…")
            sync_referral_codes(sb)
        except Exception as e:
            log.error("Referral code sync failed: %s", e)


if __name__ == "__main__":
    asyncio.run(main())
