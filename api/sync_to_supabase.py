"""
Solis → Supabase Sync Service

Polls Solis Cloud every 15 minutes for all users that have a
solis_station_id mapped in user_profiles, then writes energy_readings
to Supabase so the mobile app can read them.

Usage:
    # One-shot sync
    python -m api.sync_to_supabase

    # Continuous (every 15 min)
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

    for user in users:
        user_id: str = user["id"]
        station_id: str = user["solis_station_id"]
        name: str = user.get("full_name", "?")

        try:
            # 2. Fetch station detail from Solis
            station = await solis.station_detail(station_id)

            power_kw = float(station.get("power") or 0)
            day_energy = float(station.get("dayEnergy") or 0)
            capacity_kwp = float(station.get("capacity") or 0)
            station_name = station.get("stationName") or station.get("sno") or station_id
            state = station.get("state")  # 1=Online, 3=Alarm

            # Convert real-time power (kW) → energy over 15 min interval
            production_kwh = round(power_kw * (15 / 60), 4)

            now = datetime.now(timezone(timedelta(hours=8)))  # PHT

            # 3. Ensure solar_systems row exists
            sys_resp = (
                sb.table("solar_systems")
                .select("id")
                .eq("user_id", user_id)
                .eq("status", "active")
                .limit(1)
                .execute()
            )
            if sys_resp.data:
                system_id = sys_resp.data[0]["id"]
                # Update capacity / name from Solis if changed
                sb.table("solar_systems").update({
                    "system_name": station_name,
                    "capacity_kwp": capacity_kwp,
                }).eq("id", system_id).execute()
            else:
                # Create new solar_systems row
                insert_resp = (
                    sb.table("solar_systems")
                    .insert({
                        "user_id": user_id,
                        "system_name": station_name,
                        "capacity_kwp": capacity_kwp,
                        "installation_date": now.strftime("%Y-%m-%d"),
                        "address": "—",
                        "status": "active",
                    })
                    .execute()
                )
                system_id = insert_resp.data[0]["id"]
                log.info("Created solar_systems row for %s (system_id=%s)", name, system_id)

            # 4. Write energy_reading
            sb.table("energy_readings").insert({
                "user_id": user_id,
                "system_id": system_id,
                "timestamp": now.isoformat(),
                "production_kwh": production_kwh,
                "consumption_kwh": 0,        # Solis doesn't provide consumption
                "battery_level": None,        # Not available without battery inverter
                "battery_status": None,
                "grid_import_kwh": 0,
                "grid_export_kwh": 0,
            }).execute()

            written += 1
            log.info(
                "Synced %s | station=%s | power=%.2f kW | dayEnergy=%.2f kWh | prod_15min=%.4f kWh",
                name, station_id, power_kw, day_energy, production_kwh,
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
