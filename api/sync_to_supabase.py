"""
Solis → Supabase Daily Sync

Runs once daily (11:30 PM PHT via Render cron) for all users that have a
solis_station_id mapped in user_profiles, then writes one energy_reading
per user per day to Supabase.

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
    today_str = now.date().isoformat()
    # Noon timestamp for today's rolling reading
    today_noon = datetime(
        now.year, now.month, now.day, 12, 0, 0,
        tzinfo=timezone(timedelta(hours=8)),
    )

    for user in users:
        user_id: str = user["id"]
        station_id: str = user["solis_station_id"]
        name: str = user.get("full_name", "?")

        try:
            # 2a. Fetch station detail for system metadata
            station = await solis.station_detail(station_id)
            capacity_kwp = float(station.get("capacity") or 0)
            station_name = station.get("stationName") or station.get("sno") or station_id

            # 2b. Fetch today's 5-min interval data for rich metrics
            day_data = await solis.station_day(station_id, today_str)

            if not day_data or not isinstance(day_data, list):
                log.warning("No station_day data for %s — skipping.", name)
                continue

            # Aggregate 5-min intervals into daily running totals (values are watts)
            production_kwh = round(
                sum(float(p.get("power") or 0) for p in day_data) * (5 / 60) / 1000, 4
            )
            consumption_kwh = round(
                sum(float(p.get("consumeEnergy") or 0) for p in day_data) * (5 / 60) / 1000, 4
            )
            grid_export_kwh = round(
                sum(max(float(p.get("psum") or 0), 0) for p in day_data) * (5 / 60) / 1000, 4
            )
            grid_import_kwh = round(
                sum(abs(min(float(p.get("psum") or 0), 0)) for p in day_data) * (5 / 60) / 1000, 4
            )

            # Latest battery SOC
            soc_vals = [
                float(p.get("batteryCapacitySoc") or 0)
                for p in day_data
                if p.get("batteryCapacitySoc") is not None
            ]
            battery_level = round(soc_vals[-1], 1) if soc_vals else None
            if battery_level is None:
                battery_status = None
            elif battery_level <= 0:
                battery_status = None
            else:
                # Check latest battery power: positive = charging, negative = discharging
                last_batt_power = float(day_data[-1].get("batteryPower") or 0)
                battery_status = "charging" if last_batt_power > 0 else "discharging" if last_batt_power < 0 else "idle"

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
                insert_resp = (
                    sb.table("solar_systems")
                    .insert({
                        "user_id": user_id,
                        "system_name": station_name,
                        "capacity_kwp": capacity_kwp,
                        "installation_date": install_date,
                        "address": "—",
                        "status": "active",
                    })
                    .execute()
                )
                system_id = insert_resp.data[0]["id"]
                log.info("Created solar_systems row for %s (system_id=%s)", name, system_id)

            # 4. Upsert today's energy_reading (one row per user per day)
            reading = {
                "user_id": user_id,
                "system_id": system_id,
                "timestamp": today_noon.isoformat(),
                "production_kwh": production_kwh,
                "consumption_kwh": consumption_kwh,
                "battery_level": battery_level,
                "battery_status": battery_status,
                "grid_import_kwh": grid_import_kwh,
                "grid_export_kwh": grid_export_kwh,
            }

            # Check if today's reading already exists for this user
            existing = (
                sb.table("energy_readings")
                .select("id")
                .eq("user_id", user_id)
                .gte("timestamp", f"{today_str}T00:00:00+08:00")
                .lt("timestamp", f"{today_str}T23:59:59+08:00")
                .limit(1)
                .execute()
            )
            if existing.data:
                sb.table("energy_readings").update(reading).eq("id", existing.data[0]["id"]).execute()
            else:
                sb.table("energy_readings").insert(reading).execute()

            written += 1
            log.info(
                "Synced %s | prod=%.2f cons=%.2f grid_in=%.2f grid_out=%.2f batt=%s kWh",
                name, production_kwh, consumption_kwh, grid_import_kwh, grid_export_kwh,
                f"{battery_level}%" if battery_level else "n/a",
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


if __name__ == "__main__":
    asyncio.run(main())
