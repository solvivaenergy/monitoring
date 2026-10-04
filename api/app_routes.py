"""
Mobile-app-facing routes for the hybrid architecture.

These endpoints are called by the Solviva mobile app to get real-time
data from Solis that isn't in Supabase (current power, live battery state).

Historical data (charts, weekly/monthly trends) stays in Supabase.
Real-time data (current wattage, today's running production) comes from here.
"""

import asyncio
import os
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Query
from supabase import create_client

from .solis_client import SolisCloudClient, SolisCloudError

PHT = timezone(timedelta(hours=8))

router = APIRouter(prefix="/app", tags=["Mobile App"])
log = logging.getLogger(__name__)


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


def _is_retryable_supabase_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "522" in message:
        return True

    status_code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    return status_code == 522


def _lookup_station_for_user(sb, user_id: str) -> str | None:
    """The station this login may look at: its own primary station, else the
    first station it was GRANTED view access to (public.system_access, migration
    16 — e.g. the staff member an SME owner assigns to watch the portal). One
    station per login until the portal's station selector ships; the oldest
    grant wins so the answer is stable."""
    resp = (
        sb.table("user_profiles")
        .select("solis_station_id")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    if resp.data and resp.data[0].get("solis_station_id"):
        return resp.data[0]["solis_station_id"]

    grant = (
        sb.table("system_access")
        .select("system_id")
        .eq("user_id", user_id)
        .order("granted_at")
        .limit(1)
        .execute()
    )
    if grant.data:
        system = (
            sb.table("solar_systems")
            .select("solis_station_id")
            .eq("id", grant.data[0]["system_id"])
            .limit(1)
            .execute()
        )
        if system.data and system.data[0].get("solis_station_id"):
            return system.data[0]["solis_station_id"]
    return None


async def _query_user_profile_station_id(user_id: str) -> str:
    sb = _get_supabase()
    last_exc: Exception | None = None

    for attempt in range(3):
        try:
            station_id = _lookup_station_for_user(sb, user_id)
            if not station_id:
                raise HTTPException(status_code=404, detail="No Solis station mapped for this user")
            return station_id
        except HTTPException:
            raise
        except Exception as exc:
            last_exc = exc
            if not _is_retryable_supabase_error(exc) or attempt == 2:
                break
            log.warning("Supabase user_profiles lookup failed for %s on attempt %d: %s", user_id, attempt + 1, exc)
            await asyncio.sleep(0.5 * (attempt + 1))

    raise HTTPException(status_code=503, detail="Supabase user profile lookup temporarily unavailable") from last_exc


def _station_id_from_auth_metadata(user: dict) -> str:
    metadata = user.get("user_metadata") or {}
    station_id = metadata.get("solis_station_id")
    return str(station_id).strip() if station_id else ""


async def _resolve_station_id(user: dict) -> str:
    """Look up the Solis station ID for a Supabase user."""
    station_id = _station_id_from_auth_metadata(user)
    if station_id:
        return station_id
    return await _query_user_profile_station_id(str(user["id"]))


async def _authenticate(authorization: str = Header(...)) -> dict:
    """Validate the Supabase JWT and return the Supabase user payload."""
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[7:]
    sb = _get_supabase()
    try:
        user_resp = sb.auth.get_user(token)
        if not user_resp or not user_resp.user:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = user_resp.user
        return {
            "id": user.id,
            "user_metadata": getattr(user, "user_metadata", {}) or {},
        }
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


HOURLY_FIELDS = ("production_kwh", "consumption_kwh", "grid_import_kwh", "grid_export_kwh")


def _touch_activity(station_id: str) -> None:
    """Record that a portal login looked at this station (migration 23's
    station_activity). The worker polls Solis every 5 minutes only for stations
    viewed in the last hour, and every 15 minutes for the rest. Fire-and-forget:
    a failure here never affects the response."""
    try:
        _get_supabase().rpc("touch_station_activity", {"p_station_id": station_id}).execute()
    except Exception as exc:
        log.debug("touch_station_activity failed: %s", exc)


def _touch_activity_later(station_id: str) -> None:
    try:
        asyncio.get_running_loop().create_task(asyncio.to_thread(_touch_activity, station_id))
    except RuntimeError:
        pass


def _empty_hour(day_start: datetime, hour: int) -> dict:
    return {"hour": hour, "hour_start": (day_start + timedelta(hours=hour)).isoformat(),
            "production_kwh": None, "consumption_kwh": None, "grid_import_kwh": None, "grid_export_kwh": None,
            "peak_power_kw": None, "battery_level_end": None, "points": 0, "partial": False}


@router.get("/hourly")
async def get_hourly(day: Optional[str] = Query(None, alias="date", pattern=r"^\d{4}-\d{2}-\d{2}$"),
                     authorization: str = Header(...)):
    """Hourly energy for one Manila day (default today): 24 buckets.

    Today comes LIVE from the five-minute table (the current hour grows every
    five minutes, `partial: true`); earlier days from energy_readings_hourly,
    which the five-minute sync rolls up as hours close and the backfill worker
    fills from Solis for history (migration 20). An hour with no data has null
    values and points = 0; `points` < 12 means the inverter reported only part
    of the hour.
    """
    user = await _authenticate(authorization)
    station_id = await _resolve_station_id(user)
    _touch_activity_later(station_id)
    sb = _get_supabase()
    sys_rows = (sb.table("solar_systems").select("id").eq("solis_station_id", station_id)
                .limit(1).execute().data or [])
    if not sys_rows:
        raise HTTPException(status_code=404, detail="No station row for this login")
    system_id = sys_rows[0]["id"]

    today = datetime.now(PHT).date()
    try:
        the_day = date.fromisoformat(day) if day else today
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    if the_day > today:
        raise HTTPException(status_code=400, detail="date is in the future")
    day_start = datetime.combine(the_day, datetime.min.time(), tzinfo=PHT)
    day_end = day_start + timedelta(days=1)
    hours = {h: _empty_hour(day_start, h) for h in range(24)}

    if the_day == today:
        rows = (sb.table("energy_readings_five_minutes")
                .select("timestamp,production_kwh,consumption_kwh,grid_import_kwh,grid_export_kwh,battery_level")
                .eq("system_id", system_id).gte("timestamp", day_start.isoformat()).lt("timestamp", day_end.isoformat())
                .order("timestamp").limit(2000).execute().data or [])
        for r in rows:
            ts = datetime.fromisoformat(str(r["timestamp"]).replace("Z", "+00:00")).astimezone(PHT)
            h = hours[ts.hour]
            if h["points"] == 0:
                for k in HOURLY_FIELDS:
                    h[k] = 0.0
                h["peak_power_kw"] = 0.0
            for k in HOURLY_FIELDS:
                h[k] = round(h[k] + float(r.get(k) or 0), 3)
            h["peak_power_kw"] = round(max(h["peak_power_kw"], float(r.get("production_kwh") or 0) * 12), 3)
            if r.get("battery_level") is not None:
                h["battery_level_end"] = float(r["battery_level"])
            h["points"] = min(12, h["points"] + 1)
        current_hour = datetime.now(PHT).hour
        for h in hours.values():
            h["partial"] = h["hour"] == current_hour or (0 < h["points"] < 12)
        source = "live"
    else:
        rows = (sb.table("energy_readings_hourly")
                .select("hour_start,production_kwh,consumption_kwh,grid_import_kwh,grid_export_kwh,"
                        "peak_power_kw,battery_level_end,points")
                .eq("system_id", system_id).gte("hour_start", day_start.isoformat()).lt("hour_start", day_end.isoformat())
                .order("hour_start").execute().data or [])
        for r in rows:
            ts = datetime.fromisoformat(str(r["hour_start"]).replace("Z", "+00:00")).astimezone(PHT)
            h = hours[ts.hour]
            for k in HOURLY_FIELDS + ("peak_power_kw", "battery_level_end"):
                h[k] = float(r[k]) if r.get(k) is not None else None
            h["points"] = int(r.get("points") or 0)
            h["partial"] = 0 < h["points"] < 12
        source = "stored"

    return {
        "date": the_day.isoformat(), "source": source, "station_id": station_id,
        "totals": {k: round(sum(h[k] or 0 for h in hours.values()), 3) for k in HOURLY_FIELDS},
        "hours": [hours[h] for h in range(24)],
    }


@router.get("/live")
async def get_live_data(authorization: str = Header(...)):
    """
    Get real-time data for the authenticated user's station.

    Returns current power output, today's running totals, and live battery state.
    The mobile app calls this on Home screen load and pull-to-refresh.
    Historical data (week/month charts) continues to come from Supabase directly.
    """
    user = await _authenticate(authorization)
    user_id = str(user["id"])
    station_id = await _resolve_station_id(user)
    _touch_activity_later(station_id)
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
            sum(
                (float(p.get("familyLoadPower") or 0) + float(p.get("bypassLoadPower") or 0))
                for p in day_data
            )
            * (5 / 60)
            / 1000,
            4,
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
            consume_kw = round(
                (float(p.get("familyLoadPower") or 0) + float(p.get("bypassLoadPower") or 0))
                / 1000,
                3,
            )
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
            buckets[slot]["cons"] += (
                (float(p.get("familyLoadPower") or 0) + float(p.get("bypassLoadPower") or 0))
                * (5 / 60)
                / 1000
            )
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
