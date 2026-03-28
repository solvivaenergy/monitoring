"""
Mobile-app-facing routes for the hybrid architecture.

These endpoints are called by the Solviva mobile app to get real-time
data from Solis that isn't in Supabase (current power, live battery state).

Historical data (charts, weekly/monthly trends) stays in Supabase.
Real-time data (current wattage, today's running production) comes from here.
"""

import os
from datetime import date, datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Header
from supabase import create_client

from .solis_client import SolisCloudClient, SolisCloudError

PHT = timezone(timedelta(hours=8))

router = APIRouter(prefix="/app", tags=["Mobile App"])


def _energy_kwh(detail: dict, field: str) -> float:
    """Convert a Solis energy value to kWh.

    Solis auto-formats large values into MWh/GWh — the raw numeric field
    changes magnitude and the companion ``{field}Str`` field holds the unit.
    """
    val = float(detail.get(field) or 0)
    unit = (detail.get(f"{field}Str") or "kWh").strip()
    if unit == "MWh":
        val *= 1000
    elif unit == "GWh":
        val *= 1_000_000
    return val


def _get_solis() -> SolisCloudClient:
    key_id = os.getenv("SOLIS_CLOUD_KEY_ID", "")
    key_secret = os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
    if not key_id or not key_secret:
        raise HTTPException(status_code=500, detail="Solis credentials not configured")
    return SolisCloudClient(key_id, key_secret)


def _get_supabase():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    return create_client(url, key)


async def _resolve_station_id(user_id: str) -> str:
    """Look up the Solis station ID for a Supabase user."""
    sb = _get_supabase()
    resp = (
        sb.table("user_profiles")
        .select("solis_station_id")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    if not resp.data or not resp.data[0].get("solis_station_id"):
        raise HTTPException(status_code=404, detail="No Solis station mapped for this user")
    return resp.data[0]["solis_station_id"]


async def _authenticate(authorization: str = Header(...)) -> str:
    """Validate the Supabase JWT and return the user ID."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[7:]
    sb = _get_supabase()
    try:
        user_resp = sb.auth.get_user(token)
        if not user_resp or not user_resp.user:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_resp.user.id
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.get("/live")
async def get_live_data(authorization: str = Header(...)):
    """
    Get real-time data for the authenticated user's station.

    Returns current power output, today's running totals, and live battery state.
    The mobile app calls this on Home screen load and pull-to-refresh.
    Historical data (week/month charts) continues to come from Supabase directly.
    """
    user_id = await _authenticate(authorization)
    station_id = await _resolve_station_id(user_id)
    solis = _get_solis()

    try:
        # Fetch station detail (current power, today's energy, all-time stats)
        detail = await solis.station_detail(station_id)
    except SolisCloudError as e:
        raise HTTPException(status_code=502, detail=f"Solis API error: {e.message}")

    # Fetch today's 5-min data for battery state and intraday curve
    today_str = datetime.now(PHT).strftime("%Y-%m-%d")
    try:
        day_data = await solis.station_day(station_id, today_str)
    except SolisCloudError:
        day_data = None

    # Extract battery from latest interval
    battery_level = None
    battery_status = None
    if day_data and isinstance(day_data, list) and len(day_data) > 0:
        latest = day_data[-1]
        soc = latest.get("batteryCapacitySoc")
        if soc is not None:
            battery_level = round(float(soc), 1)
            batt_power = float(latest.get("batteryPower") or 0)
            battery_status = "charging" if batt_power > 0 else "discharging" if batt_power < 0 else "idle"

    # Compute today's running totals from 5-min intervals
    production_kwh = 0.0
    consumption_kwh = 0.0
    grid_import_kwh = 0.0
    grid_export_kwh = 0.0
    today_hourly = []
    today_readings = []
    if day_data and isinstance(day_data, list):
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

        # Build 2-hour buckets for the Today chart (5 AM to current hour)
        # Labels use end-time: bucket 5-7 AM → labeled "7", bucket 7-9 AM → "9", etc.
        now_pht = datetime.now(PHT)
        current_hour = now_pht.hour
        buckets: dict[int, dict] = {}

        for p in day_data:
            # Determine PHT hour from timestamp fields
            # Solis uses 'time' (ms epoch) as primary, 'dataTimestamp' as alternate
            ts_ms = p.get("time") or p.get("dataTimestamp")
            ts_pht = None
            hour = None
            if ts_ms:
                ts_pht = datetime.fromtimestamp(int(ts_ms) / 1000, tz=PHT)
                hour = ts_pht.hour

            if hour is None:
                continue

            # Build 5-min readings list for Today's Readings section
            power_kw = round(float(p.get("power") or 0) / 1000, 3)
            consume_kw = round(float(p.get("consumeEnergy") or 0) / 1000, 3)
            soc_val = p.get("batteryCapacitySoc")
            reading_battery = round(float(soc_val), 1) if soc_val is not None else None

            if ts_pht and (power_kw > 0 or consume_kw > 0):
                today_readings.append({
                    "timestamp": ts_pht.isoformat(),
                    "production_kw": power_kw,
                    "consumption_kw": consume_kw,
                    "battery_level": reading_battery,
                })

            # 2-hour bucket starting at 5 AM, capped at slot 21 (9-11 PM)
            if hour < 5 or hour > current_hour:
                continue
            slot = min(5 + ((hour - 5) // 2) * 2, 21)

            if slot not in buckets:
                buckets[slot] = {"prod": 0.0, "cons": 0.0, "count": 0}
            buckets[slot]["prod"] += float(p.get("power") or 0) * (5 / 60) / 1000
            buckets[slot]["cons"] += float(p.get("consumeEnergy") or 0) * (5 / 60) / 1000
            buckets[slot]["count"] += 1

        today_hourly = [
            {
                "hour": slot + 2,  # end-time label: 5→7, 7→9, 9→11, etc.
                "production_kwh": round(v["prod"], 4),
                "consumption_kwh": round(v["cons"], 4),
            }
            for slot, v in sorted(buckets.items())
            if v["count"] > 0
        ]

    return {
        # Real-time power (watts)
        "current_power_w": float(detail.get("power") or 0),
        # Today's running totals (kWh) — computed from 5-min intervals
        "today_production_kwh": production_kwh,
        "today_consumption_kwh": consumption_kwh,
        "today_grid_import_kwh": grid_import_kwh,
        "today_grid_export_kwh": grid_export_kwh,
        # Battery
        "battery_level": battery_level,
        "battery_status": battery_status,
        # Station metadata from Solis
        "capacity_kwp": float(detail.get("capacity") or 0),
        "station_name": detail.get("stationName") or "",
        # All-time totals from Solis (unit-aware: Solis auto-formats to MWh/GWh)
        "alltime_production_kwh": _energy_kwh(detail, "allEnergy"),
        "month_production_kwh": _energy_kwh(detail, "monthEnergy"),
        # 2-hour buckets for Today chart (from 5-min Solis intervals)
        "today_hourly": today_hourly,
        # 5-min interval readings for Today's Readings list
        "today_readings": today_readings,
    }
