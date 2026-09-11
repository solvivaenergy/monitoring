"""
Solviva cross-system MCP server — read-only.

Answers questions that need Odoo, Supabase and Solis together, which neither
the Supabase nor the Odoo connector can do on its own:

    stations_missing_recent_data  onboarded in Supabase, but no recent readings
    unmapped_leads                Odoo lead has a station id that never became a user
    reconcile_month               Solis stationMonth vs Supabase energy_readings

Reuses the proven primitives in api/ rather than reimplementing them.

Run locally (stdio, for Claude Code):
    python solviva_mcp.py

Run as a remote server (for a hosted deployment later):
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
from api.onboard_from_odoo import DEFAULT_FIELD_NAME, fetch_leads_with_station_id
from api.sync_to_supabase import _daily_consumption_kwh, _to_float

load_environment()

PHT = timezone(timedelta(hours=8))

# Tolerances carried over from api/audit_jul_aug_all.py.
TOL_PROD_KWH = 0.5
TOL_CONS_KWH = 1.0

# Solis station `state` (and its mirror `alarmState`), confirmed against the live
# fleet: 1 => generating normally, 2 => offline with no power and stale data,
# 3 => producing but raising an alarm (alarmLevel 1).
SOLIS_STATE = {1: "normal", 2: "offline", 3: "alarm"}

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)

mcp = MCPServer(
    name="solviva",
    instructions=(
        "Read-only cross-system queries over Solviva's Odoo CRM, Supabase and Solis "
        "Cloud. Use these when a question spans two systems. For single-system "
        "queries prefer the dedicated Supabase or Odoo connectors."
    ),
    version="0.1.0",
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
        if not hmac.compare_digest(headers.get("authorization", ""), self.expected):
            await _send_json(send, 401, {"error": "invalid_token"})
            return

        await self.app(scope, receive, send)


def _client_ip(headers: Dict[str, str], scope) -> Optional[ipaddress._BaseAddress]:
    """The caller's real public IP, as seen through Render's proxy chain.

    X-Forwarded-For is client-supplied first, then appended to by each proxy, so
    neither end of the list is reliable on its own: the first entries are forged
    by whoever wants, and the last entries are Render's own internal hops. Walk
    from the right and take the first PUBLIC address — everything to its right is
    infrastructure, and anything a client forges sits to its left, behind the
    real address Render's edge recorded.
    """
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
