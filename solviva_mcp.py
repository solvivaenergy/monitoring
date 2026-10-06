"""
Solviva Solis Cloud + cross-system MCP server — read-only.

Solis Cloud has no MCP server of its own, so this one exposes the fleet through
the monitoring repo's signed client (api/solis_client.py: HMAC auth, rate gate,
retries, the code-'1' guard):

    solis_search_stations         find a plant by name, owner email, id or serial
    solis_fleet_health            every plant offline or in alarm right now
    solis_station_detail          one plant: state, power, energy totals, address, inverters
    solis_station_day             one plant's five-minute curve for a date
    solis_station_month / _year   per-day / per-month energy, grid and battery figures
    solis_inverters               the inverters of a plant
    solis_inverter_detail         live readings of one inverter (strings, AC, battery)
    solis_inverter_day            one inverter's five-minute curve for a date
    solis_alarms                  alarm history of a plant
    solis_collectors              data loggers of a plant

plus the checks that need Odoo, Supabase and Solis together, which no
single-system connector can do:

    client_360                    one client across all three systems
    stations_missing_recent_data  onboarded in Supabase, but no recent readings
    unmapped_leads                Odoo lead has a station id that never became a user
    reconcile_month               Solis stationMonth vs Supabase energy_readings

Odoo records themselves are served by the MCP module inside production Odoo
(the "Odoo SH Production" connector) and the monitoring database by its own
Supabase connector, so the generic Odoo read tools that used to live here were
removed on 2026-10-06.

Imports from api/ are limited to the Solis client and the onboarding helpers.
The small numeric helpers are defined here on purpose: importing private names
from the sync modules is what broke every deploy between 2026-09-25 and
2026-10-06 (ImportError on start; Render kept serving the 2026-09-21 image).

Run locally (stdio, for Claude Code):
    python solviva_mcp.py

Run as a remote server (Render, behind the claude.ai connector):
    python solviva_mcp.py --http --port 8081

This server never writes. Every tool is a read, and there is no code path that
inserts, updates or deletes in Odoo, Supabase or Solis.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import ipaddress
import json
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from api.onboard_from_station_csv import (
    build_solis,
    build_supabase,
    get_auth_users_by_email,
    get_existing_station_ids,
    load_environment,
)
from api.onboard_from_odoo import (
    DEFAULT_FIELD_NAME,
    fetch_leads_with_station_id,
)
from api.solis_client import SolisCloudError

load_environment()

PHT = timezone(timedelta(hours=8))

# Tolerances carried over from api/audit_jul_aug_all.py.
TOL_PROD_KWH = 0.5
TOL_CONS_KWH = 1.0


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _daily_consumption_kwh(day: Dict[str, Any]) -> float:
    """Consumption for one stationMonth day record.

    The same rule as api/backfill_history.parse_month_day, which is what the
    nightly sync writes into energy_readings: reconcile_month must apply the
    identical rule or every day would read as drift. A local copy on purpose
    (see the module docstring): if the sync rule changes, that surfaces as
    drift in reconcile_month, which someone sees, rather than as an
    ImportError on start, which nobody did for eleven days.
    """
    total_grid_load = _to_float(day.get("homeGridEnergy"))
    backup_load = _to_float(day.get("backUpEnergy")) + _to_float(day.get("backup2Energy"))
    consumption = total_grid_load + backup_load
    if consumption <= 0:
        consumption = _to_float(day.get("homeLoadEnergy"))
    if consumption <= 0:
        consumption = _to_float(day.get("consumeEnergy"))
    return consumption


def _pht_iso(epoch_ms: Any) -> Optional[str]:
    """Solis epoch-millisecond timestamp -> ISO string in Manila time."""
    ms = _to_float(epoch_ms)
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=PHT).isoformat()


_SOLIS_CODE_1_NOTE = (
    "Solis answered code '1', which it uses for BOTH an unknown station id and a "
    "transient outage; the two responses are byte-identical. Do not conclude the "
    "station is gone: the roster (solis_search_stations) is the authoritative check."
)


def _solis_error(exc: Exception, **context: Any) -> Dict[str, Any]:
    """Shape a Solis failure as a tool result instead of a raised exception."""
    out: Dict[str, Any] = {"error": "solis_error", **context}
    if isinstance(exc, SolisCloudError):
        out["code"] = exc.code
        out["detail"] = exc.message
        if str(exc.code) == "1":
            out["note"] = _SOLIS_CODE_1_NOTE
    else:
        out["detail"] = str(exc)[:300]
    return out


def _records(payload: Any) -> List[Dict[str, Any]]:
    """Solis list endpoints answer either a bare list or {page: {records: [...]}}."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        page = payload.get("page") or {}
        return page.get("records") or payload.get("records") or []
    return []


def _pick(record: Dict[str, Any], keys: tuple, time_keys: tuple = ()) -> Dict[str, Any]:
    """Only the keys present and meaningful ("--" is Solis for n/a), with
    epoch-ms fields rendered in Manila time."""
    out: Dict[str, Any] = {}
    for key in keys:
        value = record.get(key)
        if value in (None, "", "--"):
            continue
        out[key] = _pht_iso(value) if key in time_keys else value
    return out

# Solis station `state` (and its mirror `alarmState`), confirmed against the live
# fleet: 1 => generating normally, 2 => offline with no power and stale data,
# 3 => producing but raising an alarm (alarmLevel 1).
SOLIS_STATE = {1: "normal", 2: "offline", 3: "alarm"}

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)

mcp = MCPServer(
    name="solviva",
    instructions=(
        "Read-only Solis Cloud data for Solviva's ~700 solar plants: roster search, "
        "fleet health, and per plant the detail, five-minute day curve, monthly and "
        "yearly energy, inverters, alarms and data loggers. Also the cross-system "
        "checks that join Odoo, Supabase and Solis (client_360, unmapped_leads, "
        "stations_missing_recent_data, reconcile_month). Station ids come from "
        "solis_search_stations; times are Manila (UTC+8). For Odoo records use the "
        "Odoo SH Production connector; for the monitoring database its Supabase connector."
    ),
    version="0.2.0",
)


def _page_all(query_builder, page_size: int = 1000) -> List[Dict]:
    """Page through a PostgREST query that may exceed the row cap."""
    rows: List[Dict] = []
    offset = 0
    while True:
        page = query_builder().range(offset, offset + page_size - 1).execute().data or []
        rows.extend(page)
        if len(page) < page_size:
            return rows
        offset += page_size


def _mapped_users(sb) -> List[Dict]:
    """user_profiles rows that have a Solis station mapped."""
    return _page_all(
        lambda: sb.table("user_profiles")
        .select("id, full_name, phone, solis_station_id")
        .not_.is_("solis_station_id", "null")
    )


# Paging the whole fleet costs ~7 Solis calls and about 40 seconds, which is far
# too slow to repeat on every tool call once a team is using this. Fleet state
# changes on the order of minutes, so a short TTL is plenty. A race just costs a
# duplicate fetch, so this deliberately avoids a lock (which would bind to one
# event loop and break the background warmer).
_STATION_CACHE_TTL_SECONDS = 300
_station_cache: Optional[tuple] = None


async def _fetch_all_solis_stations(solis, force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Page through userStationList, cached for _STATION_CACHE_TTL_SECONDS."""
    global _station_cache
    if not force_refresh and _station_cache:
        cached_at, cached = _station_cache
        if time.monotonic() - cached_at < _STATION_CACHE_TTL_SECONDS:
            return cached

    records: List[Dict[str, Any]] = []
    page_no = 1
    while True:
        payload = await solis.list_stations(page_no=page_no, page_size=100)
        page = payload.get("page") or {}
        chunk = page.get("records") or []
        records.extend(chunk)
        if len(chunk) < 100:
            break
        page_no += 1

    _station_cache = (time.monotonic(), records)
    return records


def _station_summary(rec: Dict[str, Any], onboarded: Set[str]) -> Dict[str, Any]:
    state = int(_to_float(rec.get("state"), 0))
    ts = rec.get("dataTimestamp")
    last_seen = None
    days_stale = None
    if ts:
        last = datetime.fromtimestamp(float(ts) / 1000, tz=PHT)
        last_seen = last.isoformat()
        days_stale = (datetime.now(PHT) - last).days
    station_id = str(rec.get("id") or "")
    return {
        "station_id": station_id,
        "name": rec.get("stationName"),
        "serial": rec.get("sno"),
        "owner_email": rec.get("userEmail"),
        "state": SOLIS_STATE.get(state, f"unknown({state})"),
        "capacity_kwp": _to_float(rec.get("capacity")),
        "current_power": _to_float(rec.get("power")),
        "today_kwh": _to_float(rec.get("dayEnergy")),
        "inverters_online": f"{rec.get('inverterOnlineCount')}/{rec.get('inverterCount')}",
        "last_seen": last_seen,
        "days_since_data": days_stale,
        "onboarded_in_supabase": station_id in onboarded,
    }


@mcp.tool(
    description=(
        "Search the Solis Cloud fleet by station name, owner email, station id or "
        "data-logger serial. Use this to find a client's station when you only have a "
        "name or email. Also reports whether each match is onboarded in Supabase."
    ),
    annotations=READ_ONLY,
)
async def solis_search_stations(query: str, limit: int = 20) -> Dict[str, Any]:
    needle = query.strip().lower()
    if not needle:
        return {"error": "query must not be empty"}

    sb = build_supabase()
    onboarded = get_existing_station_ids(sb)
    records = await _fetch_all_solis_stations(build_solis())

    matches = [
        rec
        for rec in records
        if any(
            needle in str(rec.get(field) or "").lower()
            for field in ("stationName", "userEmail", "id", "sno")
        )
    ]
    return {
        "query": query,
        "fleet_size": len(records),
        "match_count": len(matches),
        "truncated": len(matches) > limit,
        "matches": [_station_summary(r, onboarded) for r in matches[:limit]],
    }


@mcp.tool(
    description=(
        "Fleet-wide Solis health: every station currently offline or raising an alarm, "
        "cross-referenced against Supabase onboarding. Offline means no power and stale "
        "data; alarm means the station is still producing but reporting a fault. Use "
        "this to answer 'which sites are down right now' — Supabase cannot answer it, "
        "since a down station simply stops producing rows."
    ),
    annotations=READ_ONLY,
)
async def solis_fleet_health() -> Dict[str, Any]:
    sb = build_supabase()
    onboarded = get_existing_station_ids(sb)
    records = await _fetch_all_solis_stations(build_solis())

    by_state: Dict[str, int] = {}
    unhealthy: List[Dict[str, Any]] = []
    for rec in records:
        summary = _station_summary(rec, onboarded)
        by_state[summary["state"]] = by_state.get(summary["state"], 0) + 1
        if summary["state"] in ("offline", "alarm"):
            summary["alarm_message"] = rec.get("alarmMsg") if rec.get("alarmMsg") != "--" else None
            unhealthy.append(summary)

    # Longest outage first; alarms (still producing) after offline stations.
    unhealthy.sort(key=lambda r: (r["state"] != "offline", -(r["days_since_data"] or 0)))
    return {
        "fleet_size": len(records),
        "by_state": by_state,
        "unhealthy_count": len(unhealthy),
        "not_onboarded_count": sum(1 for r in unhealthy if not r["onboarded_in_supabase"]),
        "unhealthy": unhealthy,
    }


# --------------------------------------------------------------------------
# Single-plant Solis reads. Each is one signed call through api/solis_client.py
# (rate-gated, retried) and never touches the fleet roster, so they answer in
# a second or two. Output keeps Solis's own field names, plus the *Str unit
# fields where Solis sends them, so nothing is silently re-interpreted; the
# only transformations are epoch-ms -> Manila time and a state label.
# --------------------------------------------------------------------------

# Inverters and data loggers share the station numbering (1 online, 2 offline,
# 3 alarm) but "normal" reads oddly for a device, hence the second label set.
DEVICE_STATE = {1: "online", 2: "offline", 3: "alarm"}

_STATION_DETAIL_KEYS = (
    "id", "stationName", "sno", "userEmail", "state", "alarmState", "alarmLevel", "alarmMsg",
    "capacity", "capacityStr", "power", "powerStr", "dayEnergy", "dayEnergyStr",
    "monthEnergy", "monthEnergyStr", "yearEnergy", "yearEnergyStr", "allEnergy", "allEnergyStr",
    "dayIncome", "allIncome", "money", "inverterCount", "inverterOnlineCount",
    "batteryTotalChargeEnergy", "batteryTotalDischargeEnergy", "gridSwitch", "type",
    "addr", "cityStr", "regionStr", "countryStr", "timeZone", "installer", "installerEmail",
    "dataTimestamp", "createDate", "updateDate", "fisPowerTime", "fisGenerateTime",
)
_STATION_TIME_KEYS = ("dataTimestamp", "createDate", "updateDate", "fisPowerTime", "fisGenerateTime")

_INVERTER_LIST_KEYS = (
    "id", "sn", "name", "productModel", "state", "currentState", "pac", "pacStr",
    "etoday", "etodayStr", "etotal", "etotalStr", "inverterTemperature", "collectorSn",
    "collectorName", "version", "dataTimestamp", "fisGenerateTime",
)
_DEVICE_TIME_KEYS = ("dataTimestamp", "fisGenerateTime", "createDate", "updateDate")
_INVERTER_DETAIL_KEYS = _INVERTER_LIST_KEYS + (
    "stationId", "stationName", "eToday", "eTotal", "eMonth", "eYear",
    "uPv1", "iPv1", "pow1", "uPv2", "iPv2", "pow2", "uPv3", "iPv3", "pow3", "uPv4", "iPv4", "pow4",
    "uAc1", "iAc1", "uAc2", "iAc2", "uAc3", "iAc3", "fac", "facStr",
    "psum", "psumStr", "familyLoadPower", "familyLoadPowerStr", "bypassLoadPower",
    "gridPurchasedTodayEnergy", "gridSellTodayEnergy", "homeLoadTodayEnergy",
    "batteryPower", "batteryPowerStr", "batteryCapacitySoc", "batteryVoltage", "batteryCurrent",
    "batteryTodayChargeEnergy", "batteryTodayDischargeEnergy", "batteryType", "batteryHealthSoh",
    "warningInfoData", "model", "timeZone",
)
_ALARM_KEYS = (
    "id", "alarmCode", "alarmMsg", "alarmLevel", "state", "alarmBeginTime", "alarmEndTime",
    "alarmDeviceSn", "inverterSn", "deviceType", "stationName", "advice",
)
_ALARM_TIME_KEYS = ("alarmBeginTime", "alarmEndTime")
_COLLECTOR_KEYS = (
    "id", "sn", "name", "model", "state", "version", "rssiLevel", "signal", "inverterCount",
    "stationName", "dataTimestamp", "createDate", "updateDate",
)
# stationMonth days and stationYear months: Solis names kept; dateStr is
# "YYYY-MM-DD" for days and "YYYY-MM" for months (the epoch `date` is dropped).
_ENERGY_ROW_KEYS = (
    "dateStr", "energy", "energyStr", "gridPurchasedEnergy", "gridSellEnergy",
    "homeGridEnergy", "backUpEnergy", "backup2Energy", "homeLoadEnergy", "consumeEnergy",
    "batteryChargeEnergy", "batteryDischargeEnergy", "fullHour", "money",
)
# stationDay / inverterDay: five-minute samples. Every power here is in WATTS
# even though Solis's powerStr/pacStr on these points says "kW": verified
# 2026-10-06 on a 5.04 kWp plant (curve peak 3268 with dayEnergy 7.3 kWh; the
# same inverter's detail reports familyLoadPower 0.806 kW where the curve says
# 806). api/app_routes.py divides by 1000 for the same reason. The misleading
# unit strings are deliberately not passed through.
_CURVE_KEYS = (
    "timeStr", "power", "pac", "familyLoadPower", "bypassLoadPower",
    "consumePower", "gridPurchasedPower", "gridSellPower", "psum",
    "batteryPower", "batteryCapacitySoc", "energy",
)
_CURVE_UNIT_NOTE = (
    "power, pac, familyLoadPower, bypassLoadPower and psum are in watts (Solis labels "
    "these samples 'kW' but sends W); psum positive = exporting, negative = importing."
)


def _label_state(record: Dict[str, Any], labels: Dict[int, str]) -> None:
    if "state" in record:
        record["state_label"] = labels.get(int(_to_float(record.get("state"))), "unknown")


def _point_power_w(point: Dict[str, Any]) -> float:
    """Watts of a curve sample: stations report `power`, inverters `pac`."""
    if point.get("power") not in (None, "", "--"):
        return _to_float(point.get("power"))
    return _to_float(point.get("pac"))


def _summarise_curve(points: List[Dict[str, Any]], max_points: int) -> Dict[str, Any]:
    """Span, peak, battery at the last sample and an evenly thinned sample.

    estimated_production_kwh integrates watts over five-minute steps the way
    the mobile API does for its Today card (api/app_routes.py); Solis's own
    daily figure (solis_station_month) is the authoritative total and can
    differ by a few percent.
    """
    if not points:
        return {"point_count": 0, "points": []}
    stamped = sorted(
        ((_to_float(p.get("time") or p.get("dataTimestamp")), p) for p in points),
        key=lambda t: t[0],
    )
    values = [_point_power_w(p) for _, p in stamped]
    peak_i = max(range(len(values)), key=values.__getitem__)
    estimate = round(sum(values) * (5 / 60) / 1000, 3)

    sample: List[Dict[str, Any]] = []
    if max_points > 0:
        step = max(1, len(stamped) // max_points)
        for i, (ts, p) in enumerate(stamped):
            if i % step == 0 or i == len(stamped) - 1:
                row = {"time_pht": _pht_iso(ts)}
                row.update(_pick(p, _CURVE_KEYS))
                sample.append(row)

    latest = stamped[-1][1]
    return {
        "point_count": len(stamped),
        "first_point": _pht_iso(stamped[0][0]),
        "last_point": _pht_iso(stamped[-1][0]),
        "peak_power_w": values[peak_i],
        "peak_at": _pht_iso(stamped[peak_i][0]),
        "estimated_production_kwh": estimate,
        "latest_battery_soc": latest.get("batteryCapacitySoc"),
        "latest_battery_power_w": latest.get("batteryPower"),
        "units": _CURVE_UNIT_NOTE,
        "points_returned": len(sample),
        "points": sample,
    }


def _valid_date(text: Optional[str], fmt: str, label: str) -> tuple:
    """(normalised value, error dict or None)."""
    value = (text or "").strip()
    try:
        datetime.strptime(value, fmt)
    except ValueError:
        return value, {"error": f"{label} must be {fmt.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD')}, got {text!r}"}
    return value, None


@mcp.tool(
    description=(
        "Everything Solis knows about one plant: state (normal / offline / alarm), "
        "current power, today / month / year / lifetime energy with their units, "
        "inverter counts, address, installer, first-generation date and the last "
        "time data arrived (Manila time). station_id is the Solis station id; find "
        "it with solis_search_stations. include_raw=true returns every field Solis sends."
    ),
    annotations=READ_ONLY,
)
async def solis_station_detail(station_id: str, include_raw: bool = False) -> Dict[str, Any]:
    station_id = station_id.strip()
    if not station_id:
        return {"error": "station_id must not be empty"}
    try:
        data = await build_solis().station_detail(station_id)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id)
    if not data:
        return {"station_id": station_id, "error": "empty_response", "note": _SOLIS_CODE_1_NOTE}
    out = _pick(data, _STATION_DETAIL_KEYS, _STATION_TIME_KEYS)
    _label_state(out, SOLIS_STATE)
    out["station_id"] = station_id
    out["onboarded_in_supabase"] = station_id in get_existing_station_ids(build_supabase())
    out["field_count"] = len(data)
    if include_raw:
        out["raw"] = data
    return out


@mcp.tool(
    description=(
        "One plant's five-minute curve for a date (YYYY-MM-DD, Manila; default today): "
        "first and last sample, peak power and when, battery SOC at the last sample, "
        "an estimated production integrated from the curve, and up to max_points "
        "evenly spaced samples with power, household load, grid power (psum: positive "
        "= exporting, negative = importing) and battery. Use it to see whether a plant "
        "produced today and when it stopped. max_points=0 returns the summary only."
    ),
    annotations=READ_ONLY,
)
async def solis_station_day(
    station_id: str, date: Optional[str] = None, max_points: int = 48
) -> Dict[str, Any]:
    station_id = station_id.strip()
    day, bad = _valid_date(date or datetime.now(PHT).strftime("%Y-%m-%d"), "%Y-%m-%d", "date")
    if bad:
        return bad
    try:
        data = await build_solis().station_day(station_id, day)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id, date=day)
    points = _records(data)
    out: Dict[str, Any] = {"station_id": station_id, "date": day}
    out.update(_summarise_curve(points, max_points))
    if not points:
        out["note"] = (
            "No samples: the plant sent nothing that day (inverter dark or logger "
            "offline), or the id is unknown. Check solis_search_stations."
        )
    return out


@mcp.tool(
    description=(
        "Per-day energy for one plant and month (YYYY-MM): production (energy), grid "
        "import (gridPurchasedEnergy), grid export (gridSellEnergy), the load split "
        "(homeGridEnergy / backUpEnergy / homeLoadEnergy), battery charge and "
        "discharge, full-load hours and Solis's income figure, plus month totals. "
        "consumption_kwh per day is computed with the same rule the nightly sync "
        "uses. These are Solis's authoritative daily totals; the curve from "
        "solis_station_day is an estimate."
    ),
    annotations=READ_ONLY,
)
async def solis_station_month(station_id: str, month: str) -> Dict[str, Any]:
    station_id = station_id.strip()
    month, bad = _valid_date(month, "%Y-%m", "month")
    if bad:
        return bad
    try:
        data = await build_solis().station_month(station_id, month)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id, month=month)
    rows: List[Dict[str, Any]] = []
    for day in _records(data):
        row = _pick(day, _ENERGY_ROW_KEYS)
        row["consumption_kwh"] = round(_daily_consumption_kwh(day), 4)
        rows.append(row)
    rows.sort(key=lambda r: str(r.get("dateStr") or r.get("date") or ""))
    out: Dict[str, Any] = {
        "station_id": station_id,
        "month": month,
        "days": len(rows),
        "total_production_kwh": round(sum(_to_float(r.get("energy")) for r in rows), 3),
        "total_consumption_kwh": round(sum(r["consumption_kwh"] for r in rows), 3),
        "total_grid_import_kwh": round(sum(_to_float(r.get("gridPurchasedEnergy")) for r in rows), 3),
        "total_grid_export_kwh": round(sum(_to_float(r.get("gridSellEnergy")) for r in rows), 3),
        "rows": rows,
    }
    if not rows:
        out["note"] = "No days returned: no data that month, or an unknown id (check solis_search_stations)."
    return out


@mcp.tool(
    description=(
        "Per-month energy for one plant and year (YYYY): production, grid import and "
        "export, battery and income per month, with year totals. For a day-by-day "
        "view use solis_station_month."
    ),
    annotations=READ_ONLY,
)
async def solis_station_year(station_id: str, year: str) -> Dict[str, Any]:
    station_id = station_id.strip()
    year, bad = _valid_date(year, "%Y", "year")
    if bad:
        return bad
    try:
        data = await build_solis().station_year(station_id, year)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id, year=year)
    rows = [_pick(m, _ENERGY_ROW_KEYS) for m in _records(data)]
    rows.sort(key=lambda r: str(r.get("dateStr") or r.get("date") or ""))
    out: Dict[str, Any] = {
        "station_id": station_id,
        "year": year,
        "months": len(rows),
        "total_production_kwh": round(sum(_to_float(r.get("energy")) for r in rows), 3),
        "total_grid_import_kwh": round(sum(_to_float(r.get("gridPurchasedEnergy")) for r in rows), 3),
        "total_grid_export_kwh": round(sum(_to_float(r.get("gridSellEnergy")) for r in rows), 3),
        "rows": rows,
    }
    if not rows:
        out["note"] = "No months returned: no data that year, or an unknown id (check solis_search_stations)."
    return out


@mcp.tool(
    description=(
        "The inverters of one plant: Solis inverter id (what solis_inverter_detail and "
        "solis_inverter_day take), serial, model, state, current AC power, today's and "
        "lifetime energy, temperature, firmware, the data logger it reports through "
        "and its last data time."
    ),
    annotations=READ_ONLY,
)
async def solis_inverters(station_id: str) -> Dict[str, Any]:
    station_id = station_id.strip()
    try:
        data = await build_solis().list_inverters(station_id, page_no=1, page_size=100)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id)
    rows: List[Dict[str, Any]] = []
    for rec in _records(data):
        row = _pick(rec, _INVERTER_LIST_KEYS, _DEVICE_TIME_KEYS)
        _label_state(row, DEVICE_STATE)
        rows.append(row)
    out: Dict[str, Any] = {"station_id": station_id, "inverter_count": len(rows), "inverters": rows}
    if not rows:
        out["note"] = "No inverters returned: the plant has none registered, or the id is unknown (check solis_search_stations)."
    return out


@mcp.tool(
    description=(
        "Live readings of one inverter by Solis inverter id (from solis_inverters): "
        "state, AC power and frequency, per-string DC voltage / current / power "
        "(uPv1, iPv1, pow1 ...), grid power (psum: positive = exporting), household "
        "load, battery SOC / power / voltage / health, today's grid, load and battery "
        "energies, temperature and firmware. include_raw=true returns every field "
        "(typically 200+)."
    ),
    annotations=READ_ONLY,
)
async def solis_inverter_detail(inverter_id: str, include_raw: bool = False) -> Dict[str, Any]:
    inverter_id = inverter_id.strip()
    if not inverter_id:
        return {"error": "inverter_id must not be empty"}
    try:
        data = await build_solis().inverter_detail(inverter_id)
    except Exception as exc:
        return _solis_error(exc, inverter_id=inverter_id)
    if not data:
        return {"inverter_id": inverter_id, "error": "empty_response", "note": _SOLIS_CODE_1_NOTE}
    out = _pick(data, _INVERTER_DETAIL_KEYS, _DEVICE_TIME_KEYS)
    _label_state(out, DEVICE_STATE)
    out["inverter_id"] = inverter_id
    out["field_count"] = len(data)
    if include_raw:
        out["raw"] = data
    return out


@mcp.tool(
    description=(
        "One inverter's five-minute curve for a date (YYYY-MM-DD, Manila; default "
        "today), with the same summary and sampling as solis_station_day. Use it on "
        "multi-inverter plants to see which unit stopped."
    ),
    annotations=READ_ONLY,
)
async def solis_inverter_day(
    inverter_id: str, date: Optional[str] = None, max_points: int = 48
) -> Dict[str, Any]:
    inverter_id = inverter_id.strip()
    day, bad = _valid_date(date or datetime.now(PHT).strftime("%Y-%m-%d"), "%Y-%m-%d", "date")
    if bad:
        return bad
    try:
        data = await build_solis().inverter_day(inverter_id, day)
    except Exception as exc:
        return _solis_error(exc, inverter_id=inverter_id, date=day)
    points = _records(data)
    out: Dict[str, Any] = {"inverter_id": inverter_id, "date": day}
    out.update(_summarise_curve(points, max_points))
    if not points:
        out["note"] = "No samples: the inverter sent nothing that day, or the id is unknown (check solis_inverters)."
    return out


@mcp.tool(
    description=(
        "Alarm history of one plant: code, message, level, whether it is still active "
        "(state), begin and end time in Manila time, the device serial and Solis's "
        "advice. begin and end are 'YYYY-MM-DD HH:MM:SS' (Manila); omit both for "
        "Solis's default recent window. Returns up to limit (max 100) in Solis's order."
    ),
    annotations=READ_ONLY,
)
async def solis_alarms(
    station_id: str, begin: Optional[str] = None, end: Optional[str] = None, limit: int = 50
) -> Dict[str, Any]:
    station_id = station_id.strip()
    size = max(1, min(int(limit), 100))
    try:
        data = await build_solis().alarm_list(
            station_id, page_no=1, page_size=size, begin_time=begin or None, end_time=end or None
        )
    except Exception as exc:
        return _solis_error(exc, station_id=station_id, begin=begin, end=end)
    rows = [_pick(rec, _ALARM_KEYS, _ALARM_TIME_KEYS) for rec in _records(data)]
    total = (data.get("page") or {}).get("total") if isinstance(data, dict) else None
    return {
        "station_id": station_id,
        "window": {"begin": begin, "end": end},
        "returned": len(rows),
        "total_in_window": total,
        "alarms": rows,
    }


@mcp.tool(
    description=(
        "Data loggers (the Wi-Fi / LAN sticks) of one plant: serial, model, firmware, "
        "state, signal and last data time. When a plant is offline in Solis but the "
        "inverter itself has power, the logger listed here is usually the culprit."
    ),
    annotations=READ_ONLY,
)
async def solis_collectors(station_id: str) -> Dict[str, Any]:
    station_id = station_id.strip()
    try:
        data = await build_solis().list_collectors(station_id, page_no=1, page_size=100)
    except Exception as exc:
        return _solis_error(exc, station_id=station_id)
    rows: List[Dict[str, Any]] = []
    for rec in _records(data):
        row = _pick(rec, _COLLECTOR_KEYS, _DEVICE_TIME_KEYS)
        _label_state(row, DEVICE_STATE)
        rows.append(row)
    out: Dict[str, Any] = {"station_id": station_id, "collector_count": len(rows), "collectors": rows}
    if not rows:
        out["note"] = "No data loggers returned: none registered, or the id is unknown (check solis_search_stations)."
    return out


@mcp.tool(
    description=(
        "Full picture of one client across all three systems at once: the Odoo CRM lead "
        "(contact details, station id field), the Supabase user profile and reading "
        "history, and live Solis station state. Accepts a station id, a client name, or "
        "an email. Use this to answer 'what is going on with this client' without "
        "querying three systems separately."
    ),
    annotations=READ_ONLY,
)
async def client_360(query: str) -> Dict[str, Any]:
    needle = query.strip().lower()
    if not needle:
        return {"error": "query must not be empty"}

    sb = build_supabase()
    stations = await _fetch_all_solis_stations(build_solis())

    # user_profiles has no email column — the account email lives in auth.users,
    # so it takes an admin lookup. Invert the email->id map once per call (~0.8s
    # for the whole tenant) rather than querying per matched station.
    email_by_user_id = {uid: email for email, uid in get_auth_users_by_email(sb).items()}

    matched = [
        rec
        for rec in stations
        if any(
            needle in str(rec.get(field) or "").lower()
            for field in ("stationName", "userEmail", "id", "sno")
        )
    ]
    if not matched:
        return {"query": query, "found": False, "note": "No Solis station matched that name, email or id."}

    # One Odoo fetch, indexed by station id, rather than a call per match.
    leads_by_station: Dict[str, Dict[str, Any]] = {}
    for lead in fetch_leads_with_station_id():
        sid = str(lead.get(DEFAULT_FIELD_NAME) or "").strip()
        if sid:
            leads_by_station.setdefault(sid, lead)

    results: List[Dict[str, Any]] = []
    for rec in matched[:10]:
        station_id = str(rec.get("id") or "")
        profile = (
            sb.table("user_profiles")
            .select("id, full_name, phone, address")
            .eq("solis_station_id", station_id)
            .limit(1)
            .execute()
        ).data
        supabase_block: Dict[str, Any] = {"onboarded": bool(profile)}
        if profile:
            uid = profile[0]["id"]
            recent = (
                sb.table("energy_readings")
                .select("timestamp, production_kwh, consumption_kwh")
                .eq("user_id", uid)
                .order("timestamp", desc=True)
                .limit(7)
                .execute()
            ).data or []
            supabase_block.update(
                {
                    "user_id": uid,
                    "full_name": profile[0].get("full_name"),
                    "email": email_by_user_id.get(uid),
                    "phone": profile[0].get("phone"),
                    "address": profile[0].get("address"),
                    "last_7_readings": [
                        {
                            "date": str(r["timestamp"])[:10],
                            "production_kwh": _to_float(r.get("production_kwh")),
                            "consumption_kwh": _to_float(r.get("consumption_kwh")),
                        }
                        for r in recent
                    ],
                    "all_recent_production_zero": bool(recent)
                    and all(_to_float(r.get("production_kwh")) == 0 for r in recent),
                }
            )

        lead = leads_by_station.get(station_id)
        odoo_block = (
            {
                "lead_found": True,
                "lead_id": lead.get("id"),
                "lead_name": lead.get("name"),
                "contact_name": lead.get("contact_name") or lead.get("partner_name"),
                "email": lead.get("email_from") or None,
                "phone": lead.get("phone") or None,
            }
            if lead
            else {"lead_found": False, "note": "No Odoo lead carries this station id."}
        )

        solis_block = _station_summary(rec, set())
        solis_block.pop("onboarded_in_supabase", None)

        results.append(
            {
                "station_id": station_id,
                "solis": solis_block,
                "supabase": supabase_block,
                "odoo": odoo_block,
                "diagnosis": _diagnose_client(solis_block, supabase_block, odoo_block),
            }
        )

    return {"query": query, "found": True, "match_count": len(matched), "clients": results}


def _diagnose_client(solis: Dict, supabase: Dict, odoo: Dict) -> str:
    """The one line worth reading, combining what each system says."""
    if solis["state"] == "offline" and supabase.get("all_recent_production_zero"):
        return (
            f"System offline in Solis for {solis['days_since_data']} days, but the sync is "
            "still writing zero-production rows daily — so dashboards and freshness checks "
            "look healthy. Needs a site visit."
        )
    if solis["state"] == "offline":
        return f"System offline in Solis for {solis['days_since_data']} days."
    if solis["state"] == "alarm":
        return "Station is producing but raising a Solis alarm."
    if not supabase.get("onboarded"):
        return "Producing normally in Solis but not onboarded in Supabase — client sees no dashboard."
    if not odoo.get("lead_found"):
        return "Healthy, but no Odoo lead carries this station id — CRM is out of sync."
    return "Healthy across all three systems."


def _next_month_iso(month: str) -> str:
    year, mon = (int(p) for p in month.split("-"))
    nxt = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    return f"{nxt.isoformat()}T00:00:00+08:00"


@mcp.tool(
    description=(
        "Stations that are onboarded in Supabase but have no energy_readings row in "
        "the last N days. This is the daily-sync health check: a station here is "
        "usually a Solis mapping problem or a failed sync, and the client's dashboard "
        "is stale. Returns each station with its last reading date and days of silence."
    ),
    annotations=READ_ONLY,
)
def stations_missing_recent_data(days: int = 7) -> Dict[str, Any]:
    sb = build_supabase()
    users = _mapped_users(sb)
    if not users:
        return {"checked": 0, "stale": [], "note": "No user_profiles have solis_station_id set."}

    now = datetime.now(PHT)
    cutoff_iso = (now - timedelta(days=days)).isoformat()

    # One paged scan for everyone WITH recent data, rather than a query per user.
    fresh: Set[str] = {
        str(r["user_id"])
        for r in _page_all(
            lambda: sb.table("energy_readings").select("user_id").gte("timestamp", cutoff_iso)
        )
        if r.get("user_id")
    }

    stale: List[Dict[str, Any]] = []
    for user in users:
        uid = str(user["id"])
        if uid in fresh:
            continue
        # Only the stale ones need a per-user lookup, so this stays cheap.
        last = (
            sb.table("energy_readings")
            .select("timestamp")
            .eq("user_id", uid)
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        ).data
        last_ts = last[0]["timestamp"] if last else None
        days_silent: Optional[int] = None
        if last_ts:
            parsed = datetime.fromisoformat(str(last_ts).replace("Z", "+00:00"))
            days_silent = (now - parsed.astimezone(PHT)).days
        stale.append(
            {
                "user_id": uid,
                "name": user.get("full_name"),
                "phone": user.get("phone"),
                "station_id": user.get("solis_station_id"),
                "last_reading": last_ts,
                "days_since_last_reading": days_silent,
                "ever_synced": bool(last_ts),
            }
        )

    # Never-synced first, then longest silence.
    stale.sort(key=lambda r: (r["ever_synced"], -(r["days_since_last_reading"] or 0)))
    return {
        "checked": len(users),
        "threshold_days": days,
        "stale_count": len(stale),
        "never_synced_count": sum(1 for s in stale if not s["ever_synced"]),
        "stale": stale,
    }


@mcp.tool(
    description=(
        "Odoo crm.lead records whose Solis station-id field is populated but whose "
        "station never became a Supabase user. These are clients the auto-onboarder "
        "skipped — usually missing an email on the lead. Returns each lead with the "
        "likely reason it could not be onboarded."
    ),
    annotations=READ_ONLY,
)
def unmapped_leads(field_name: str = DEFAULT_FIELD_NAME) -> Dict[str, Any]:
    sb = build_supabase()
    known: Set[str] = get_existing_station_ids(sb)
    auth_emails = get_auth_users_by_email(sb)
    leads = fetch_leads_with_station_id(field_name)

    unmapped: List[Dict[str, Any]] = []
    for lead in leads:
        station_id = str(lead.get(field_name) or "").strip()
        if not station_id or station_id in known:
            continue
        email = str(lead.get("email_from") or "").strip()
        reason, detail = _diagnose_email(email, auth_emails)
        unmapped.append(
            {
                "odoo_lead_id": lead.get("id"),
                "lead_name": lead.get("name"),
                "contact_name": lead.get("contact_name") or lead.get("partner_name"),
                "email": email or None,
                "phone": lead.get("phone") or None,
                "station_id": station_id,
                "reason": reason,
                "detail": detail,
            }
        )

    by_reason: Dict[str, int] = {}
    for row in unmapped:
        by_reason[row["reason"]] = by_reason.get(row["reason"], 0) + 1

    return {
        "odoo_leads_with_station_id": len(leads),
        "already_onboarded": len(known),
        "unmapped_count": len(unmapped),
        "by_reason": by_reason,
        "unmapped": unmapped,
    }


def _diagnose_email(email: str, auth_emails: Dict[str, str]) -> tuple[str, str]:
    """Why a lead with a station id never became a Supabase user.

    Ordered most-specific first. The multi-address and second-property cases are
    the two that actually show up in the Odoo data: sales puts two contacts in
    email_from, or a repeat client's second array reuses an email that already
    owns an auth user.
    """
    if not email:
        return (
            "no_email",
            "No email on the lead; the Solis station userEmail fallback must also be empty.",
        )
    if email.count("@") > 1:
        return (
            "multiple_emails_in_field",
            "email_from holds more than one address. Split it so the lead carries a "
            "single address, then re-run onboarding.",
        )
    if "@" not in email:
        return ("invalid_email", f"{email!r} is not an email address — looks like a placeholder.")

    owner = auth_emails.get(email.lower())
    if owner:
        return (
            "email_already_has_auth_user",
            f"A Supabase auth user ({owner}) already owns this email — typically a "
            "repeat client's second array. One auth user cannot hold two stations.",
        )
    return (
        "unknown",
        "Email looks valid and unused; onboarding may simply not have run since this "
        "lead was last updated.",
    )


@mcp.tool(
    description=(
        "Reconcile one month of Solis stationMonth data against the energy_readings "
        "rows stored in Supabase, day by day. Surfaces days where stored production or "
        "consumption drifted from Solis, and days missing from Supabase entirely. "
        "month is 'YYYY-MM'. Omit station_id to check every mapped station."
    ),
    annotations=READ_ONLY,
)
async def reconcile_month(month: str, station_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        return {"error": f"month must be 'YYYY-MM', got {month!r}"}

    sb = build_supabase()
    users = _mapped_users(sb)
    if station_id:
        users = [u for u in users if str(u.get("solis_station_id")) == str(station_id)]
        if not users:
            return {"error": f"No mapped user found for station_id {station_id!r}"}

    solis = build_solis()
    stations: List[Dict[str, Any]] = []

    for user in users:
        uid = str(user["id"])
        sid = str(user["solis_station_id"])
        try:
            month_data = await solis.station_month(sid, month)
        except Exception as exc:  # Solis rate-limits and occasionally 500s; report, don't abort.
            stations.append({"station_id": sid, "name": user.get("full_name"), "error": str(exc)})
            continue

        solis_days: Dict[str, Dict[str, float]] = {}
        for day in month_data or []:
            date_str = day.get("dateStr")
            if date_str:
                solis_days[date_str] = {
                    "production_kwh": round(_to_float(day.get("energy")), 4),
                    "consumption_kwh": round(_daily_consumption_kwh(day), 4),
                }

        stored = (
            sb.table("energy_readings")
            .select("timestamp, production_kwh, consumption_kwh")
            .eq("user_id", uid)
            .gte("timestamp", f"{month}-01T00:00:00+08:00")
            .lt("timestamp", _next_month_iso(month))
            .execute()
        ).data or []
        stored_days = {
            str(r["timestamp"])[:10]: {
                "production_kwh": _to_float(r.get("production_kwh")),
                "consumption_kwh": _to_float(r.get("consumption_kwh")),
            }
            for r in stored
        }

        mismatches: List[Dict[str, Any]] = []
        for date_str, solis_vals in sorted(solis_days.items()):
            stored_vals = stored_days.get(date_str)
            if stored_vals is None:
                mismatches.append({"date": date_str, "issue": "missing_in_supabase", "solis": solis_vals})
                continue
            prod_delta = round(stored_vals["production_kwh"] - solis_vals["production_kwh"], 4)
            cons_delta = round(stored_vals["consumption_kwh"] - solis_vals["consumption_kwh"], 4)
            if abs(prod_delta) > TOL_PROD_KWH or abs(cons_delta) > TOL_CONS_KWH:
                mismatches.append(
                    {
                        "date": date_str,
                        "issue": "value_drift",
                        "solis": solis_vals,
                        "supabase": stored_vals,
                        "production_delta": prod_delta,
                        "consumption_delta": cons_delta,
                    }
                )

        stations.append(
            {
                "station_id": sid,
                "name": user.get("full_name"),
                "solis_days": len(solis_days),
                "supabase_days": len(stored_days),
                "mismatch_count": len(mismatches),
                "mismatches": mismatches,
                "days_in_supabase_not_in_solis": sorted(set(stored_days) - set(solis_days)),
            }
        )

    return {
        "month": month,
        "stations_checked": len(stations),
        "stations_with_mismatches": sum(1 for s in stations if s.get("mismatch_count")),
        "tolerances_kwh": {"production": TOL_PROD_KWH, "consumption": TOL_CONS_KWH},
        "stations": stations,
    }


# --------------------------------------------------------------------------
# Remote serving (Claude Teams). Local stdio needs none of this — the MCP spec
# says stdio servers take credentials from the environment instead.
# --------------------------------------------------------------------------

# Anthropic's published egress range. Every request Claude makes to a connector
# comes from here, so anything else is not Claude.
ANTHROPIC_EGRESS = ipaddress.ip_network("160.79.104.0/21")


class BearerAuthMiddleware:
    """Shared service-account token + Anthropic IP allowlist.

    Pure ASGI rather than BaseHTTPMiddleware: streamable HTTP holds long-lived
    SSE responses open, and BaseHTTPMiddleware buffers them.
    """

    def __init__(self, app, token: str, restrict_ips: bool = True) -> None:
        self.app = app
        self.expected = f"Bearer {token}"
        self.restrict_ips = restrict_ips

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Render's health check must not need the token.
        if scope["path"] == "/healthz":
            await _send_json(send, 200, {"status": "ok"})
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}

        if self.restrict_ips and not _from_anthropic(headers, scope):
            # Log the chain: getting the proxy-hop depth wrong here silently
            # rejects every legitimate request, and the header is the only way
            # to see what the platform actually forwarded.
            print(
                f"[auth] rejected {scope['path']} — resolved client "
                f"{_client_ip(headers, scope)} not in {ANTHROPIC_EGRESS}; "
                f"x-forwarded-for={headers.get('x-forwarded-for', '(none)')!r}",
                flush=True,
            )
            await _send_json(send, 403, {"error": "forbidden"})
            return

        # Constant-time compare so the token can't be recovered by timing.
        #
        # Deliberately no WWW-Authenticate header: that would tell Claude this is
        # an OAuth resource server, sending it looking for discovery documents we
        # do not serve, and the connector would fail with "Couldn't reach the MCP
        # server". This server uses a fixed shared credential, which Claude sends
        # as a configured request header rather than discovering.
        supplied = headers.get("authorization", "")
        if not hmac.compare_digest(supplied, self.expected):
            # Distinguish "the client sent no credential" from "the credential is
            # wrong" — the two have completely different fixes, and from outside
            # both look like an identical 401. Never log the token itself.
            if not supplied:
                detail = "no Authorization header sent"
            elif not supplied.startswith("Bearer "):
                detail = f"Authorization header missing 'Bearer ' prefix (starts {supplied[:8]!r})"
            else:
                detail = (
                    f"token mismatch: got {len(supplied) - 7} chars, "
                    f"expected {len(self.expected) - 7}"
                )
            print(f"[auth] 401 on {scope['path']} — {detail}", flush=True)
            await _send_json(send, 401, {"error": "invalid_token"})
            return

        await self.app(scope, receive, send)


def _client_ip(headers: Dict[str, str], scope) -> Optional[ipaddress._BaseAddress]:
    """The caller's real public IP, as seen through Render's proxy chain.

    Render fronts every service with Cloudflare, which sets CF-Connecting-IP to
    the true client address and overwrites whatever the client sent — so that
    header is both authoritative and unforgeable here, and it is checked first.

    The X-Forwarded-For fallback exists for running behind some other proxy. It
    walks from the right and takes the first PUBLIC address, because leading
    entries are client-supplied and trailing entries are internal hops. Note this
    fallback is exactly what fails on Cloudflare: its edge IPs (172.64.0.0/13 and
    friends) are public, so the walk stops at Cloudflare instead of the caller.
    """
    for header in ("cf-connecting-ip", "true-client-ip"):
        raw = headers.get(header, "").strip()
        if raw:
            try:
                return ipaddress.ip_address(raw)
            except ValueError:
                pass

    chain = [p.strip() for p in headers.get("x-forwarded-for", "").split(",") if p.strip()]
    if not chain:
        chain = [(scope.get("client") or ("",))[0]]

    for raw in reversed(chain):
        try:
            addr = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            continue
        return addr
    return None


def _from_anthropic(headers: Dict[str, str], scope) -> bool:
    addr = _client_ip(headers, scope)
    return addr is not None and addr in ANTHROPIC_EGRESS


async def _send_json(send, status: int, body: Dict[str, Any], extra_headers=None) -> None:
    payload = json.dumps(body).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(payload)).encode())]
    headers.extend(extra_headers or [])
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": payload})


def main() -> None:
    parser = argparse.ArgumentParser(description="Solviva cross-system MCP server (read-only).")
    parser.add_argument("--http", action="store_true", help="Serve streamable HTTP instead of stdio.")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8081")))
    parser.add_argument(
        "--allow-any-ip",
        action="store_true",
        help="Skip the Anthropic IP allowlist (local testing only).",
    )
    args = parser.parse_args()

    if not args.http:
        mcp.run(transport="stdio")
        return

    token = os.getenv("MCP_SERVICE_TOKEN", "")
    if not token:
        raise SystemExit("MCP_SERVICE_TOKEN must be set when serving over HTTP.")

    import uvicorn
    from mcp.server.transport_security import TransportSecuritySettings

    # DNS-rebinding protection matches the Host header exactly; a trailing ":*"
    # allows any port. Without a configured public host we are running locally,
    # where the check has nothing meaningful to protect.
    public_host = os.getenv("MCP_PUBLIC_HOST", "")
    if public_host:
        security = TransportSecuritySettings(
            allowed_hosts=[public_host, f"{public_host}:*"],
            allowed_origins=[f"https://{public_host}"],
        )
    else:
        security = TransportSecuritySettings(enable_dns_rebinding_protection=False)

    app = mcp.streamable_http_app(
        stateless_http=True,  # No session affinity needed, so a restart drops nothing.
        transport_security=security,
    )
    # Escape hatch: the IP allowlist is defence-in-depth behind the token, so it
    # must never be the thing that blocks a working deploy. Set
    # MCP_RESTRICT_IPS=false if the platform's proxy chain defeats it.
    restrict = not args.allow_any_ip and os.getenv("MCP_RESTRICT_IPS", "true").lower() != "false"
    if not restrict:
        print("[auth] IP allowlist DISABLED — bearer token is the only control.", flush=True)
    app = BearerAuthMiddleware(app, token=token, restrict_ips=restrict)

    _start_cache_warmer()
    uvicorn.run(app, host="0.0.0.0", port=args.port)


def _start_cache_warmer() -> None:
    """Keep the Solis fleet cache warm in the background.

    Without this the first request after a deploy pays the full ~40s fleet page,
    which is long enough to look broken in a chat. Runs on its own thread so
    /healthz answers immediately and Render sees a live service.
    """

    async def _refresh_forever() -> None:
        while True:
            try:
                await _fetch_all_solis_stations(build_solis(), force_refresh=True)
            except Exception as exc:  # Never let a Solis blip kill the warmer.
                print(f"[cache-warmer] refresh failed: {exc}", flush=True)
            await asyncio.sleep(_STATION_CACHE_TTL_SECONDS // 2)

    threading.Thread(
        target=lambda: asyncio.run(_refresh_forever()), daemon=True, name="solis-cache-warmer"
    ).start()


if __name__ == "__main__":
    main()
