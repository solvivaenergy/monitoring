"""
Back-office ID validation routes.

Three validators, used by the engineering back office before an identity edit is
persisted and before any backfill is triggered:

    POST /admin/validate/solis-station   does this Solis station id exist, and who owns it
    POST /admin/validate/odoo-lead       does this crm.lead id exist, and does its station id agree
    POST /admin/validate/odoo-customer   does this res.partner id exist, and what leads hang off it

plus bulk variants and one combined grid endpoint (`/admin/validate/rows`) that
validates every column of every row in a single round trip.

THE INVARIANT THIS MODULE EXISTS TO ENFORCE
-------------------------------------------
Every validator returns one of exactly three verdicts:

    "valid"        the id was checked and it exists
    "not_found"    the id was checked and it does not exist
    "unavailable"  the id could NOT be checked

`not_found` is reachable from exactly two places in this file: a `None` return
from `station_detail` (the documented Solis behaviour for a nonexistent id), and
an empty `search_read` result from Odoo. Every exception path — timeout, 502,
SolisCloudError, XML-RPC fault, anything — produces `unavailable`. Never
`not_found`.

This is not defensive tidiness. Measured against the live Solis API on
2026-09-14: a valid station id took 28.0 s and needed one internal retry; a
nonexistent id took 48.3 s and two; and a third call exhausted all three retries
and raised `httpx.ReadTimeout`. If a timeout were reported as "does not exist",
an engineer would blank a correct mapping and the next backfill would overwrite
that customer's history in place (energy_readings has no soft delete and the
project has no PITR).

Note that an exhausted `SolisCloudClient._request` re-raises the last **httpx**
exception, not a `SolisCloudError`. `api/app_routes.py:146` catches only
`SolisCloudError`, which is why a flaky Solis currently surfaces there as an
unhandled 500 rather than the intended 502. Handlers here catch `Exception`.

WHY THE FLEET ROSTER, NOT stationDetail
---------------------------------------
`userStationList` returns the full record for 100 stations in one call — measured
12.9 s for page 1 of 7, covering all 648 stations — and each record carries every
field this endpoint reports (stationName, userEmail, capacity, installer,
countryStr/regionStr/cityStr, fisPowerTimeStr, inverterCount, state). One
stationDetail call costs 28-48 s for one station. So the roster is both cheaper
and richer. It is cached for 5 minutes (the cadence used by solviva_mcp.py) and
serves ~every validation as a memory hit. `station_detail` is used only as a
tie-breaker for an id the roster does not know about, which is the one case the
roster cannot answer: a station created in the last few minutes.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import threading
import time
import xmlrpc.client
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import APIRouter, Header, HTTPException, Response
from pydantic import BaseModel, Field
from supabase import create_client

from .onboard_from_odoo import DEFAULT_FIELD_NAME, _build_odoo_config, _connect_odoo
from .solis_client import SolisCloudClient, SolisCloudError

router = APIRouter(prefix="/admin/validate", tags=["Back Office — Validation"])
log = logging.getLogger(__name__)

# Verdicts. Only these three ever appear in a `status` field.
VALID = "valid"
NOT_FOUND = "not_found"
UNAVAILABLE = "unavailable"

# Real Solis station ids are 19 digits; the fleet is 100% consistent on this.
# `user_profiles` currently holds '12' and Odoo holds '36' — both fat-fingers
# that this pattern rejects without spending a 30 s Solis call on them.
STATION_ID_RE = re.compile(r"^[0-9]{15,20}$")

# Solis station `state`, confirmed against the live fleet (same mapping as
# solviva_mcp.py): 1 generating, 2 offline, 3 producing with an alarm raised.
SOLIS_STATE = {1: "normal", 2: "offline", 3: "alarm"}

# --- Timeout budgets (seconds) ------------------------------------------------
# SolisCloudClient retries 3x with 2/4/8 s backoff on a 30 s httpx timeout, so
# its own worst case before raising is ~104 s. That is longer than any browser,
# Render's proxy, or an engineer will wait, so every Solis call here is wrapped
# in an explicit wait_for. Blowing the budget is `unavailable`, by definition.
SOLIS_DEEP_BUDGET = 35.0        # one stationDetail tie-breaker
SOLIS_ROSTER_BUDGET = 120.0     # a full 7-page fleet walk
ODOO_BUDGET = 20.0              # measured: 0.12 s for 300 leads. 20 s is a fuse.
SUPABASE_BUDGET = 15.0

# --- Cache TTLs (seconds) -----------------------------------------------------
ROSTER_TTL = 300.0              # matches solviva_mcp._STATION_CACHE_TTL_SECONDS
ROSTER_STALE_MAX = 3600.0       # serve stale beyond TTL rather than fail
VALID_TTL = 300.0
NOT_FOUND_TTL = 60.0            # short: a station created 2 min ago must appear
ODOO_TTL = 30.0
# `unavailable` is never cached, and neither is the Supabase assignment check —
# that check is the TOCTOU guard for requirement 5 and must always be live.

MAX_BULK_IDS = 1000
MAX_DEEP_CHECKS_PER_BULK = 25   # stations absent from the roster, checked live


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

def _get_supabase():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase not configured")
    return create_client(url, key)


def _get_solis() -> SolisCloudClient:
    key_id = os.getenv("SOLIS_CLOUD_KEY_ID", "")
    key_secret = os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
    if not key_id or not key_secret:
        raise HTTPException(status_code=500, detail="Solis credentials not configured")
    client = SolisCloudClient(key_id, key_secret)
    # Instance-level override of the class defaults (2 x 30 s + 1.5 + 2.25 s of
    # backoff ~= 64 s worst case instead of 104 s). A human is waiting on this
    # call, unlike the nightly sync these defaults were tuned for.
    client._MAX_RETRIES = 2
    client._BACKOFF_BASE = 1.5
    return client


# xmlrpc.client.ServerProxy reuses one HTTP connection and is not thread-safe,
# and every call here runs in a worker thread. One process-wide lock is ample:
# a bulk validation of all 615 rows is two execute_kw calls totalling ~0.3 s.
_odoo_session: Optional[Tuple[Any, int, xmlrpc.client.ServerProxy]] = None
_odoo_lock = threading.Lock()


def _odoo_call(model: str, method: str, args: List, kwargs: Optional[Dict] = None) -> Any:
    """Blocking Odoo XML-RPC call. Authenticates once, re-authenticates on drop.

    Same connection pattern as api/onboard_from_odoo.py and solviva_mcp.py:
    ODOO_SH_* credentials, falling back to the ODOO_* set used by the worker.
    """
    global _odoo_session
    with _odoo_lock:
        for attempt in (1, 2):
            try:
                if _odoo_session is None:
                    cfg = _build_odoo_config()
                    uid, models = _connect_odoo(cfg)
                    _odoo_session = (cfg, uid, models)
                cfg, uid, models = _odoo_session
                return models.execute_kw(
                    cfg.db, uid, cfg.auth, model, method, args, kwargs or {}
                )
            except Exception as exc:
                # A dropped Odoo.sh session looks like a transport error, not an
                # auth error. Throw the session away and re-authenticate once.
                _odoo_session = None
                if attempt == 2:
                    raise
                log.warning("Odoo %s.%s failed (%s); re-authenticating", model, method, exc)


async def _odoo_search_read(
    model: str, domain: List, fields: List[str], limit: int = 0
) -> List[Dict]:
    """search_read that ALSO sees archived records.

    `active_test: False` is mandatory, not a nicety. Four crm.lead records
    carrying a Solis station id are archived, including lead 275667 — one of the
    three leads whose station id is claimed by a second lead. Verified live:
    searching for lead 275667 or 18585 without this context returns `[]`, so a
    validator without it reports "this lead does not exist" for a lead that
    plainly does. `read` is not an alternative: it ignores active_test but
    returns a phantom `{'id': 0, 'name': False}` record for id 0.
    """
    opts: Dict[str, Any] = {"fields": fields, "context": {"active_test": False}}
    if limit:
        opts["limit"] = limit
    return await asyncio.wait_for(
        asyncio.to_thread(_odoo_call, model, "search_read", [domain], opts),
        timeout=ODOO_BUDGET,
    )


# ---------------------------------------------------------------------------
# Auth — staff only
# ---------------------------------------------------------------------------

async def _authenticate_staff(authorization: str) -> Dict[str, Any]:
    """Validate a Supabase JWT and require a staff_users row.

    Deliberately NOT the `/solis/*` model, which has no auth at all: these
    endpoints are a station-existence and customer-identity oracle over the whole
    fleet, and `/solis/*` being public is a finding to fix, not a pattern to copy.

    Depends on the `staff_users` table + `is_staff()` from the security audit.
    Until that table exists, STAFF_BOOTSTRAP_EMAILS (a comma-separated env var)
    is honoured so the back office can be brought up; remove it once staff rows
    are seeded, and never let it be the permanent mechanism.
    """
    if not authorization or not authorization.startswith("Bearer "):
        # 401, not FastAPI's default 422 for a missing Header(...) — a client
        # cannot tell a malformed request from an expired session otherwise.
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    token = authorization[7:]
    sb = _get_supabase()

    try:
        user_resp = await asyncio.wait_for(
            asyncio.to_thread(sb.auth.get_user, token), timeout=SUPABASE_BUDGET
        )
    except asyncio.TimeoutError:
        # An auth backend that is merely slow is 503, not 401. Reporting it as
        # 401 (the bug at app_routes.py:125) makes an outage look like a
        # logout and sends the whole team re-entering passwords.
        raise HTTPException(status_code=503, detail="Auth backend unavailable")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    if not user_resp or not user_resp.user:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = user_resp.user
    email = (getattr(user, "email", "") or "").lower()

    bootstrap = {
        e.strip().lower()
        for e in os.getenv("STAFF_BOOTSTRAP_EMAILS", "").split(",")
        if e.strip()
    }
    if email and email in bootstrap:
        return {"id": str(user.id), "email": email, "role": "admin"}

    try:
        rows = (
            sb.table("staff_users")
            .select("user_id, role, active, revoked_at")
            .eq("user_id", str(user.id))
            .limit(1)
            .execute()
        ).data or []
    except Exception as exc:
        log.warning("staff_users lookup failed: %s", exc)
        raise HTTPException(status_code=503, detail="Staff directory unavailable")

    if not rows or not rows[0].get("active") or rows[0].get("revoked_at"):
        raise HTTPException(status_code=403, detail="Not authorised for the back office")

    return {"id": str(user.id), "email": email, "role": rows[0].get("role") or "readonly"}


# ---------------------------------------------------------------------------
# Rate limiting — per staff user, in-process
# ---------------------------------------------------------------------------
# solviva-api runs as a single Render instance, so an in-process limiter is the
# whole limiter. If it is ever scaled out, move this to a Postgres counter; do
# not silently accept N x the limit.

class _RateLimiter:
    def __init__(self, capacity: int, refill_per_second: float):
        self.capacity = float(capacity)
        self.rate = refill_per_second
        self._buckets: Dict[str, Tuple[float, float]] = {}
        self._lock = threading.Lock()

    def take(self, key: str, cost: float = 1.0) -> Optional[float]:
        """Return None if allowed, else the seconds to wait."""
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - last) * self.rate)
            if tokens >= cost:
                self._buckets[key] = (tokens - cost, now)
                return None
            self._buckets[key] = (tokens, now)
            return round((cost - tokens) / self.rate, 1)


# 60-burst then 1/s sustained: an engineer tabbing through a grid never trips it;
# a stuck retry loop does.
_single_limiter = _RateLimiter(capacity=60, refill_per_second=1.0)
# Bulk is far cheaper per id but pins the roster and two Odoo calls: 6 burst,
# 1 per 10 s sustained.
_bulk_limiter = _RateLimiter(capacity=6, refill_per_second=0.1)


def _enforce(limiter: _RateLimiter, staff: Dict, response: Response, cost: float = 1.0) -> None:
    wait = limiter.take(staff["id"], cost)
    if wait is not None:
        # Headers must go on the exception: the injected Response object is
        # discarded when the handler raises.
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded; retry in {wait}s",
            headers={"Retry-After": str(int(wait) + 1)},
        )


# ---------------------------------------------------------------------------
# TTL cache
# ---------------------------------------------------------------------------

_cache: Dict[str, Tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        hit = _cache.get(key)
    if not hit:
        return None
    expires_at, value = hit
    if time.monotonic() >= expires_at:
        with _cache_lock:
            _cache.pop(key, None)
        return None
    return value


def _cache_put(key: str, value: Any, ttl: float) -> None:
    if ttl <= 0:
        return
    with _cache_lock:
        _cache[key] = (time.monotonic() + ttl, value)


def _cache_invalidate(prefix: str) -> None:
    """Called by the write path after a mapping edit, so the grid never shows a
    verdict that predates the change the engineer just made."""
    with _cache_lock:
        for key in [k for k in _cache if k.startswith(prefix)]:
            _cache.pop(key, None)


# ---------------------------------------------------------------------------
# Solis fleet roster
# ---------------------------------------------------------------------------

_roster: Optional[Tuple[float, Dict[str, Dict[str, Any]]]] = None   # (fetched_at, by_id)
_roster_refresh_lock = asyncio.Lock()


async def _fetch_roster() -> Dict[str, Dict[str, Any]]:
    """Page userStationList into {station_id: record}. ~7 calls, ~90 s."""
    solis = _get_solis()
    records: Dict[str, Dict[str, Any]] = {}
    page_no = 1
    while True:
        payload = await solis.list_stations(page_no=page_no, page_size=100)
        page = (payload or {}).get("page") or {}
        chunk = page.get("records") or []
        for rec in chunk:
            sid = str(rec.get("id") or "").strip()
            if sid:
                records[sid] = rec
        if len(chunk) < 100:
            break
        page_no += 1
        if page_no > 50:   # hard stop; the fleet is 648 stations over 7 pages
            break
    return records


async def _get_roster(force: bool = False) -> Tuple[Optional[Dict[str, Dict[str, Any]]], float, bool]:
    """Return (roster_by_id, age_seconds, is_stale).

    Stale-while-revalidate. A fleet walk takes ~90 s and hits 502s often enough
    that the audit saw one on a normal 7-page pass, so an expired roster is
    served stale rather than making a human wait for a refresh that may fail.
    Only a completely cold cache blocks, and it blocks under a hard budget.
    """
    global _roster
    snapshot = _roster
    now = time.monotonic()

    if snapshot and not force:
        fetched_at, data = snapshot
        age = now - fetched_at
        if age < ROSTER_TTL:
            return data, age, False

    async with _roster_refresh_lock:
        # Single-flight: another request may have refreshed while we queued.
        snapshot = _roster
        if snapshot and not force and (time.monotonic() - snapshot[0]) < ROSTER_TTL:
            return snapshot[1], time.monotonic() - snapshot[0], False
        try:
            data = await asyncio.wait_for(_fetch_roster(), timeout=SOLIS_ROSTER_BUDGET)
            _roster = (time.monotonic(), data)
            return data, 0.0, False
        except Exception as exc:
            log.warning("Solis fleet roster refresh failed: %s", exc)
            if snapshot and (time.monotonic() - snapshot[0]) < ROSTER_STALE_MAX:
                return snapshot[1], time.monotonic() - snapshot[0], True
            return None, 0.0, True


def _station_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    """The fields the back office grid displays. Field names verified live
    against both userStationList and stationDetail on 2026-09-14."""
    state = rec.get("state")
    try:
        state_int = int(float(state)) if state is not None else None
    except (TypeError, ValueError):
        state_int = None
    return {
        "station_id": str(rec.get("id") or ""),
        # Solis exposes NO owner-name field. `stationName` is both the plant name
        # and the only person-ish name Solis has; the back office's "customer
        # name (solis)" and "solis plant name" are one column, not two.
        "station_name": rec.get("stationName") or None,
        "user_email": rec.get("userEmail") or None,
        "solis_user_id": str(rec.get("userId")) if rec.get("userId") else None,
        "capacity_kwp": float(rec.get("capacity") or 0) or None,
        "capacity_unit": rec.get("capacityStr") or "kWp",
        # Constant "Solviva Energy OPC" across all 648 stations — it is us, not
        # the customer. Returned so an unexpected value is visible.
        "installer": rec.get("installer") or None,
        "serial": rec.get("sno") or None,
        "created_at": rec.get("createDateStr") or None,
        "first_power_at": rec.get("fisPowerTimeStr") or None,
        "inverter_count": rec.get("inverterCount"),
        "inverters_online": rec.get("inverterOnlineCount"),
        "country": rec.get("countryStr") or None,
        "region": rec.get("regionStr") or None,
        "city": rec.get("cityStr") or None,
        "state": SOLIS_STATE.get(state_int, f"unknown({state})" if state_int is None else f"unknown({state_int})"),
        "state_code": state_int,
    }


# ---------------------------------------------------------------------------
# Supabase side-checks: is this station already taken, and does it have history
# ---------------------------------------------------------------------------

def _assignment_rows(sb, station_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """{station_id: [{source, user_id, full_name, system_id}]} for ids already mapped.

    Reads both homes for the mapping, because they coexist during the
    multi-station migration: `user_profiles.solis_station_id` (the scalar that
    exists today) and `solar_systems.solis_station_id` (added by the migration,
    which is where it belongs once one user can own N stations). A missing
    column is tolerated so this ships before or after the DDL.
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    if not station_ids:
        return out

    try:
        rows = (
            sb.table("user_profiles")
            .select("id, full_name, solis_station_id")
            .in_("solis_station_id", station_ids)
            .execute()
        ).data or []
        for r in rows:
            out.setdefault(str(r["solis_station_id"]), []).append(
                {
                    "source": "user_profiles",
                    "user_id": r["id"],
                    "full_name": r.get("full_name"),
                    "system_id": None,
                }
            )
    except Exception as exc:
        log.warning("user_profiles assignment check failed: %s", exc)
        raise

    try:
        rows = (
            sb.table("solar_systems")
            .select("id, user_id, system_name, solis_station_id")
            .in_("solis_station_id", station_ids)
            .execute()
        ).data or []
        for r in rows:
            out.setdefault(str(r["solis_station_id"]), []).append(
                {
                    "source": "solar_systems",
                    "user_id": r["user_id"],
                    "full_name": r.get("system_name"),
                    "system_id": r["id"],
                }
            )
    except Exception as exc:
        # Expected until the migration adds the column (PGRST204 / 42703).
        log.debug("solar_systems.solis_station_id not queryable yet: %s", exc)

    return out


def _reading_counts(sb, owners: Iterable[Tuple[str, Optional[str]]]) -> Dict[str, Dict[str, Any]]:
    """{user_id: {count, first, last}} — the blast radius of a remap.

    Today energy_readings carries no station id, so history can only be counted
    per user (per system once solar_systems.solis_station_id lands). This is the
    number the confirm dialog must show before an edit: "this repoints N daily
    readings spanning X to Y", per the guardrail in the security audit.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for user_id, system_id in owners:
        if not user_id or user_id in out:
            continue
        try:
            q = sb.table("energy_readings").select("timestamp", count="exact")
            q = q.eq("system_id", system_id) if system_id else q.eq("user_id", user_id)
            newest = q.order("timestamp", desc=True).limit(1).execute()
            count = newest.count or 0
            first = None
            if count:
                q2 = sb.table("energy_readings").select("timestamp")
                q2 = q2.eq("system_id", system_id) if system_id else q2.eq("user_id", user_id)
                oldest = q2.order("timestamp", desc=False).limit(1).execute()
                first = (oldest.data or [{}])[0].get("timestamp")
            out[user_id] = {
                "count": count,
                "first_reading": first,
                "last_reading": (newest.data or [{}])[0].get("timestamp") if count else None,
            }
        except Exception as exc:
            log.warning("reading count failed for %s: %s", user_id, exc)
            out[user_id] = {"count": None, "first_reading": None, "last_reading": None}
    return out


async def _supabase_station_context(station_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Assignment + history for a batch of station ids. Never cached."""
    sb = _get_supabase()
    try:
        assignments = await asyncio.wait_for(
            asyncio.to_thread(_assignment_rows, sb, station_ids), timeout=SUPABASE_BUDGET
        )
    except Exception as exc:
        log.warning("assignment check unavailable: %s", exc)
        return {sid: {"assignment_checked": False} for sid in station_ids}

    owners = [
        (a["user_id"], a.get("system_id"))
        for rows in assignments.values()
        for a in rows
    ]
    try:
        counts = await asyncio.wait_for(
            asyncio.to_thread(_reading_counts, sb, owners), timeout=SUPABASE_BUDGET
        )
    except Exception as exc:
        log.warning("reading counts unavailable: %s", exc)
        counts = {}

    context: Dict[str, Dict[str, Any]] = {}
    for sid in station_ids:
        rows = assignments.get(sid, [])
        history = next(
            (counts.get(r["user_id"]) for r in rows if counts.get(r["user_id"])), None
        )
        context[sid] = {
            "assignment_checked": True,
            "already_assigned": bool(rows),
            "assigned_to": rows or None,
            "has_readings": bool(history and history.get("count")),
            "reading_count": (history or {}).get("count"),
            "first_reading": (history or {}).get("first_reading"),
            "last_reading": (history or {}).get("last_reading"),
        }
    return context


# ---------------------------------------------------------------------------
# 1. Solis station validator
# ---------------------------------------------------------------------------

class StationRequest(BaseModel):
    station_id: str = Field(..., description="Solis station id, normally 19 digits")
    for_user_id: Optional[str] = Field(
        None,
        description=(
            "The Supabase user the id is about to be assigned to. When given, an "
            "existing assignment to THIS user is reported as already_assigned "
            "without the conflict flag."
        ),
    )
    refresh: bool = Field(False, description="Bypass the verdict cache and the roster TTL")


class StationBulkRequest(BaseModel):
    station_ids: List[str] = Field(..., max_length=MAX_BULK_IDS)
    deep_check_missing: bool = Field(
        True, description=f"Live stationDetail for up to {MAX_DEEP_CHECKS_PER_BULK} ids absent from the roster"
    )


async def _solis_deep_check(station_id: str) -> Tuple[str, Optional[Dict], Optional[str]]:
    """(verdict, record, reason). The ONLY place `not_found` can come from Solis.

    `station_detail` returns None for a nonexistent id instead of raising, and
    raises SolisCloudError('1', 'Communication error...') for an empty string —
    both verified live. `None` is the sole not_found signal. Every exception,
    including SolisCloudError, is `unavailable`: code '1' is literally worded as
    a communication error and must never be read as "does not exist".
    """
    solis = _get_solis()
    try:
        detail = await asyncio.wait_for(
            solis.station_detail(station_id), timeout=SOLIS_DEEP_BUDGET
        )
    except SolisCloudError as exc:
        log.warning("Solis rejected stationDetail(%s): [%s] %s", station_id, exc.code, exc.message)
        return UNAVAILABLE, None, f"solis_error_{exc.code}"
    except asyncio.TimeoutError:
        log.warning("Solis stationDetail(%s) exceeded the %.0fs budget", station_id, SOLIS_DEEP_BUDGET)
        return UNAVAILABLE, None, "timeout"
    except Exception as exc:
        # SolisCloudClient re-raises the last httpx error once retries are spent
        # (httpx.ReadTimeout / HTTPStatusError), so this is the common flaky path,
        # not an edge case. Observed live during this design.
        log.warning("Solis stationDetail(%s) failed: %s: %s", station_id, type(exc).__name__, exc)
        return UNAVAILABLE, None, "transport_error"

    if detail is None:
        return NOT_FOUND, None, "solis_returned_null"
    return VALID, detail, None


async def _validate_station(
    station_id: str, for_user_id: Optional[str], refresh: bool
) -> Dict[str, Any]:
    raw = (station_id or "").strip()

    # Format first, before any network call. An empty id must never reach Solis:
    # station_detail("") raises SolisCloudError('1'), which is indistinguishable
    # from a transient failure and would be reported as `unavailable` forever.
    if not raw:
        return {
            "station_id": raw,
            "status": NOT_FOUND,
            "reason": "empty",
            "checked_solis": False,
            "message": "No station id supplied.",
        }
    if not STATION_ID_RE.match(raw):
        # '12' (in user_profiles today) and '36' (in Odoo) land here. Rejecting
        # on shape is safe — every one of the 648 live stations is 19 digits —
        # and avoids the 30 s+ Solis round trip a malformed id provokes.
        return {
            "station_id": raw,
            "status": NOT_FOUND,
            "reason": "malformed",
            "checked_solis": False,
            "message": "Not a Solis station id — expected 15-20 digits.",
        }

    cache_key = f"solis:{raw}"
    cached = None if refresh else _cache_get(cache_key)
    if cached:
        result = dict(cached)
        result["cached"] = True
    else:
        roster, age, stale = await _get_roster(force=refresh)
        result: Dict[str, Any]
        if roster is not None and raw in roster:
            result = {
                "station_id": raw,
                "status": VALID,
                "reason": None,
                "checked_solis": True,
                "source": "fleet_roster",
                "roster_age_seconds": round(age, 1),
                "station": _station_payload(roster[raw]),
                "in_solviva_account": True,
            }
            _cache_put(cache_key, result, VALID_TTL)
        else:
            # Absent from the roster. Could be genuinely nonexistent, could be
            # minutes old, could be a roster we failed to refresh. Only a live
            # stationDetail can tell those apart.
            verdict, detail, reason = await _solis_deep_check(raw)
            if verdict == VALID:
                result = {
                    "station_id": raw,
                    "status": VALID,
                    "reason": None,
                    "checked_solis": True,
                    "source": "station_detail",
                    "station": _station_payload(detail),
                    # Real, but not in the cached fleet list: either brand new,
                    # or on another Solis account. Worth surfacing loudly.
                    "in_solviva_account": False,
                    "warning": "Station exists but is not in the cached Solviva fleet roster.",
                }
                _cache_put(cache_key, result, VALID_TTL)
            elif verdict == NOT_FOUND and roster is None:
                # No roster AND a failed deep check is not evidence of absence.
                result = {
                    "station_id": raw,
                    "status": UNAVAILABLE,
                    "reason": "no_roster_and_deep_check_inconclusive",
                    "checked_solis": True,
                    "message": "Could not verify against Solis. Do not treat as invalid.",
                    "retry_after_seconds": 30,
                }
            elif verdict == NOT_FOUND:
                result = {
                    "station_id": raw,
                    "status": NOT_FOUND,
                    "reason": reason,
                    "checked_solis": True,
                    "source": "station_detail",
                    "message": "Solis has no station with this id.",
                }
                _cache_put(cache_key, result, NOT_FOUND_TTL)
            else:
                result = {
                    "station_id": raw,
                    "status": UNAVAILABLE,
                    "reason": reason,
                    "checked_solis": True,
                    "message": "Solis could not be reached. This is NOT a verdict that the id is invalid.",
                    "retry_after_seconds": 30,
                }
                if stale:
                    result["note"] = "Fleet roster is stale and a live check also failed."
        result["cached"] = False

    # The uniqueness check the requirements did not ask for. Always live: the
    # whole point is to catch an id another engineer assigned a minute ago, and
    # nothing in the schema enforces it (there is no UNIQUE on
    # user_profiles.solis_station_id today — 0 duplicates by luck, not by
    # constraint).
    context = await _supabase_station_context([raw])
    ctx = context.get(raw, {})
    result.update(ctx)
    if ctx.get("already_assigned") and for_user_id:
        owners = {str(a["user_id"]) for a in (ctx.get("assigned_to") or [])}
        result["assignment_conflict"] = owners != {str(for_user_id)}
    else:
        result["assignment_conflict"] = bool(ctx.get("already_assigned"))
    return result


@router.post("/solis-station")
async def validate_solis_station(
    body: StationRequest, response: Response, authorization: str = Header(None)
):
    staff = await _authenticate_staff(authorization)
    _enforce(_single_limiter, staff, response)
    result = await _validate_station(body.station_id, body.for_user_id, body.refresh)
    if result["status"] == UNAVAILABLE:
        response.headers["Retry-After"] = str(result.get("retry_after_seconds", 30))
    response.headers["Cache-Control"] = "no-store"
    return result


@router.post("/solis-stations")
async def validate_solis_stations_bulk(
    body: StationBulkRequest, response: Response, authorization: str = Header(None)
):
    """Bulk: the whole grid in one call.

    All 615 rows cost one roster read (memory, or ~90 s once every 5 minutes)
    plus one PostgREST `in.()` query. Only ids missing from the roster cost a
    live Solis call, and those are capped so one bad paste cannot spend 25
    minutes of Solis budget.
    """
    staff = await _authenticate_staff(authorization)
    _enforce(_bulk_limiter, staff, response)

    ids = [str(s or "").strip() for s in body.station_ids]
    if len(ids) > MAX_BULK_IDS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_BULK_IDS} ids per request")

    roster, age, stale = await _get_roster()
    context = await _supabase_station_context([i for i in ids if i])

    results: List[Dict[str, Any]] = []
    deep_queue: List[int] = []

    for raw in ids:
        if not raw:
            results.append({"station_id": raw, "status": NOT_FOUND, "reason": "empty", "checked_solis": False})
        elif not STATION_ID_RE.match(raw):
            results.append({"station_id": raw, "status": NOT_FOUND, "reason": "malformed", "checked_solis": False})
        elif roster is not None and raw in roster:
            results.append({
                "station_id": raw, "status": VALID, "reason": None, "checked_solis": True,
                "source": "fleet_roster", "station": _station_payload(roster[raw]),
                "in_solviva_account": True,
            })
        elif roster is None:
            results.append({
                "station_id": raw, "status": UNAVAILABLE, "reason": "roster_unavailable",
                "checked_solis": False, "retry_after_seconds": 60,
            })
        else:
            results.append({
                "station_id": raw, "status": UNAVAILABLE, "reason": "absent_from_roster_pending_deep_check",
                "checked_solis": False, "retry_after_seconds": 30,
            })
            deep_queue.append(len(results) - 1)

    if body.deep_check_missing and deep_queue:
        budget = deep_queue[:MAX_DEEP_CHECKS_PER_BULK]
        # Bounded concurrency: SolisCloudClient already paces itself at ~10 req/s,
        # and a stationDetail costs 28-48 s, so 3 in flight is the useful ceiling.
        sem = asyncio.Semaphore(3)

        async def _one(idx: int) -> None:
            async with sem:
                sid = results[idx]["station_id"]
                verdict, detail, reason = await _solis_deep_check(sid)
                if verdict == VALID:
                    results[idx] = {
                        "station_id": sid, "status": VALID, "reason": None, "checked_solis": True,
                        "source": "station_detail", "station": _station_payload(detail),
                        "in_solviva_account": False,
                        "warning": "Exists in Solis but not in the Solviva fleet roster.",
                    }
                elif verdict == NOT_FOUND:
                    results[idx] = {
                        "station_id": sid, "status": NOT_FOUND, "reason": reason,
                        "checked_solis": True, "source": "station_detail",
                    }
                else:
                    results[idx] = {
                        "station_id": sid, "status": UNAVAILABLE, "reason": reason,
                        "checked_solis": True, "retry_after_seconds": 30,
                    }

        await asyncio.gather(*(_one(i) for i in budget))

    for row in results:
        ctx = context.get(row["station_id"])
        if ctx:
            row.update(ctx)
            row["assignment_conflict"] = bool(ctx.get("already_assigned"))

    counts = {VALID: 0, NOT_FOUND: 0, UNAVAILABLE: 0}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1

    response.headers["Cache-Control"] = "no-store"
    return {
        "requested": len(ids),
        "summary": counts,
        "roster_age_seconds": round(age, 1),
        "roster_stale": stale,
        "deep_checked": len(deep_queue[:MAX_DEEP_CHECKS_PER_BULK]) if body.deep_check_missing else 0,
        "deep_check_deferred": max(0, len(deep_queue) - MAX_DEEP_CHECKS_PER_BULK) if body.deep_check_missing else len(deep_queue),
        "results": results,
    }


# ---------------------------------------------------------------------------
# 2. Odoo lead validator (crm.lead)
# ---------------------------------------------------------------------------

LEAD_FIELDS = [
    "id", "name", "contact_name", "partner_name", "email_from", "phone",
    "partner_id", "stage_id", "type", "active", "create_date", "write_date",
    DEFAULT_FIELD_NAME,
]


class LeadRequest(BaseModel):
    lead_id: int
    expected_station_id: Optional[str] = Field(
        None, description="The station id being typed; compared against what Odoo holds"
    )


class LeadBulkRequest(BaseModel):
    lead_ids: List[int] = Field(..., max_length=MAX_BULK_IDS)
    expected_station_ids: Optional[Dict[str, str]] = Field(
        None, description="{lead_id: station_id} to compare against Odoo, keyed by string id"
    )


def _lead_payload(lead: Dict[str, Any], expected_station_id: Optional[str]) -> Dict[str, Any]:
    partner = lead.get("partner_id") or None
    stage = lead.get("stage_id") or None
    odoo_station = lead.get(DEFAULT_FIELD_NAME)
    odoo_station = str(odoo_station).strip() if odoo_station and odoo_station is not False else None
    expected = (expected_station_id or "").strip() or None

    return {
        "lead_id": lead["id"],
        "name": lead.get("name") or None,
        "contact_name": (lead.get("contact_name") or None) if lead.get("contact_name") is not False else None,
        "partner_name": (lead.get("partner_name") or None) if lead.get("partner_name") is not False else None,
        "email_from": (lead.get("email_from") or None) if lead.get("email_from") is not False else None,
        "phone": (lead.get("phone") or None) if lead.get("phone") is not False else None,
        "partner_id": partner[0] if partner else None,
        "partner_display_name": partner[1] if partner else None,
        "stage_id": stage[0] if stage else None,
        "stage_name": stage[1] if stage else None,
        "type": lead.get("type"),
        # An archived lead is a real record and must not read as "not found",
        # but it is almost never the lead an engineer means to link. Lead 275667
        # is archived AND shares its station id with active lead 131643.
        "archived": not lead.get("active", True),
        "created_at": lead.get("create_date"),
        "updated_at": lead.get("write_date"),
        "odoo_station_id": odoo_station,
        "expected_station_id": expected,
        "station_id_matches": (odoo_station == expected) if (expected and odoo_station) else None,
        "station_id_state": (
            "match" if expected and odoo_station and odoo_station == expected
            else "mismatch" if expected and odoo_station
            else "missing_in_odoo" if expected and not odoo_station
            else "not_compared"
        ),
    }


async def _leads_by_station(station_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Other leads claiming the same station id.

    Three station ids are each on two leads (1298491919450502433,
    1298491919450429471, 1298491919449962500), so "the lead id for this station"
    is sometimes a choice the operator has to make. Show it rather than pick.
    """
    if not station_ids:
        return {}
    rows = await _odoo_search_read(
        "crm.lead", [[DEFAULT_FIELD_NAME, "in", station_ids]],
        ["id", "name", "stage_id", "active", DEFAULT_FIELD_NAME],
    )
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        sid = str(r.get(DEFAULT_FIELD_NAME) or "").strip()
        if sid:
            out.setdefault(sid, []).append({
                "lead_id": r["id"],
                "name": r.get("name"),
                "stage_name": (r.get("stage_id") or [None, None])[1],
                "archived": not r.get("active", True),
            })
    return out


@router.post("/odoo-lead")
async def validate_odoo_lead(
    body: LeadRequest, response: Response, authorization: str = Header(None)
):
    staff = await _authenticate_staff(authorization)
    _enforce(_single_limiter, staff, response)
    response.headers["Cache-Control"] = "no-store"

    if body.lead_id <= 0:
        # Odoo's `read` returns a phantom {'id': 0, 'name': False} for id 0,
        # which is why this module uses search_read everywhere and rejects
        # non-positive ids up front.
        return {"lead_id": body.lead_id, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False}

    cache_key = f"odoo_lead:{body.lead_id}:{(body.expected_station_id or '').strip()}"
    cached = _cache_get(cache_key)
    if cached:
        return {**cached, "cached": True}

    try:
        rows = await _odoo_search_read("crm.lead", [["id", "=", body.lead_id]], LEAD_FIELDS, limit=1)
    except asyncio.TimeoutError:
        response.headers["Retry-After"] = "15"
        return {"lead_id": body.lead_id, "status": UNAVAILABLE, "reason": "timeout",
                "checked_odoo": True, "retry_after_seconds": 15,
                "message": "Odoo did not respond. This is NOT a verdict that the lead is missing."}
    except Exception as exc:
        log.warning("Odoo lead lookup failed for %s: %s", body.lead_id, exc)
        response.headers["Retry-After"] = "15"
        return {"lead_id": body.lead_id, "status": UNAVAILABLE, "reason": "odoo_error",
                "checked_odoo": True, "retry_after_seconds": 15, "detail": str(exc)[:200]}

    if not rows:
        result = {"lead_id": body.lead_id, "status": NOT_FOUND, "reason": "no_such_lead",
                  "checked_odoo": True, "message": "No crm.lead with this id (archived records included)."}
        _cache_put(cache_key, result, NOT_FOUND_TTL)
        return result

    payload = _lead_payload(rows[0], body.expected_station_id)
    sid = payload["odoo_station_id"]
    try:
        siblings = await _leads_by_station([sid]) if sid else {}
    except Exception:
        siblings = {}
    others = [l for l in siblings.get(sid or "", []) if l["lead_id"] != body.lead_id]

    result = {"status": VALID, "reason": None, "checked_odoo": True, "lead": payload,
              "other_leads_with_same_station_id": others or None}
    _cache_put(cache_key, result, ODOO_TTL)
    return {**result, "cached": False}


@router.post("/odoo-leads")
async def validate_odoo_leads_bulk(
    body: LeadBulkRequest, response: Response, authorization: str = Header(None)
):
    """615 leads in one XML-RPC call. Measured: 300 leads in 0.12 s."""
    staff = await _authenticate_staff(authorization)
    _enforce(_bulk_limiter, staff, response)
    response.headers["Cache-Control"] = "no-store"

    ids = [int(i) for i in body.lead_ids if int(i) > 0]
    if len(body.lead_ids) > MAX_BULK_IDS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_BULK_IDS} ids per request")

    expected = {str(k): v for k, v in (body.expected_station_ids or {}).items()}

    try:
        rows = await _odoo_search_read("crm.lead", [["id", "in", ids]], LEAD_FIELDS)
    except Exception as exc:
        log.warning("Bulk Odoo lead lookup failed: %s", exc)
        response.headers["Retry-After"] = "15"
        # Whole-batch failure is whole-batch `unavailable`. Never per-row
        # not_found: one dead XML-RPC call is not 615 missing leads.
        return {
            "requested": len(body.lead_ids),
            "summary": {VALID: 0, NOT_FOUND: 0, UNAVAILABLE: len(body.lead_ids)},
            "results": [{"lead_id": i, "status": UNAVAILABLE, "reason": "odoo_error", "checked_odoo": True}
                        for i in body.lead_ids],
        }

    found = {r["id"]: r for r in rows}
    station_ids = list({
        str(r.get(DEFAULT_FIELD_NAME)).strip()
        for r in rows
        if r.get(DEFAULT_FIELD_NAME) and r.get(DEFAULT_FIELD_NAME) is not False
    })
    try:
        siblings = await _leads_by_station(station_ids)
    except Exception:
        siblings = {}

    results: List[Dict[str, Any]] = []
    for lead_id in body.lead_ids:
        if int(lead_id) <= 0:
            results.append({"lead_id": lead_id, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False})
            continue
        row = found.get(int(lead_id))
        if not row:
            results.append({"lead_id": lead_id, "status": NOT_FOUND, "reason": "no_such_lead", "checked_odoo": True})
            continue
        payload = _lead_payload(row, expected.get(str(lead_id)))
        sid = payload["odoo_station_id"]
        others = [l for l in siblings.get(sid or "", []) if l["lead_id"] != int(lead_id)]
        results.append({"status": VALID, "reason": None, "checked_odoo": True, "lead": payload,
                        "other_leads_with_same_station_id": others or None})

    counts = {VALID: 0, NOT_FOUND: 0, UNAVAILABLE: 0}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"requested": len(body.lead_ids), "summary": counts, "results": results}


# ---------------------------------------------------------------------------
# 3. Odoo customer validator (res.partner)
# ---------------------------------------------------------------------------

PARTNER_FIELDS = [
    "id", "name", "email", "phone", "mobile", "street", "city",
    "is_company", "parent_id", "customer_rank", "active", "create_date",
]


class PartnerRequest(BaseModel):
    partner_id: int


class PartnerBulkRequest(BaseModel):
    partner_ids: List[int] = Field(..., max_length=MAX_BULK_IDS)


def _partner_payload(p: Dict[str, Any], leads: List[Dict[str, Any]]) -> Dict[str, Any]:
    parent = p.get("parent_id") or None
    station_ids = sorted({
        str(l.get(DEFAULT_FIELD_NAME)).strip()
        for l in leads
        if l.get(DEFAULT_FIELD_NAME) and l.get(DEFAULT_FIELD_NAME) is not False
    })
    return {
        "partner_id": p["id"],
        "name": p.get("name") or None,
        "email": (p.get("email") or None) if p.get("email") is not False else None,
        "phone": (p.get("phone") or None) if p.get("phone") is not False else None,
        "mobile": (p.get("mobile") or None) if p.get("mobile") is not False else None,
        "city": (p.get("city") or None) if p.get("city") is not False else None,
        "is_company": bool(p.get("is_company")),
        "parent_id": parent[0] if parent else None,
        "parent_name": parent[1] if parent else None,
        "archived": not p.get("active", True),
        "created_at": p.get("create_date"),
        "lead_count": len(leads),
        # res.partner.id is the identity that survives multi-station: 8 partners
        # already carry 2-3 station-bearing leads, so this list is the evidence
        # that a customer needs N stations, not one.
        "leads": [
            {
                "lead_id": l["id"],
                "name": l.get("name"),
                "stage_name": (l.get("stage_id") or [None, None])[1],
                "email_from": (l.get("email_from") or None) if l.get("email_from") is not False else None,
                "archived": not l.get("active", True),
                "station_id": (str(l.get(DEFAULT_FIELD_NAME)).strip()
                               if l.get(DEFAULT_FIELD_NAME) and l.get(DEFAULT_FIELD_NAME) is not False else None),
            }
            for l in sorted(leads, key=lambda x: x["id"], reverse=True)
        ],
        "station_ids": station_ids,
        "multi_station": len(station_ids) > 1,
    }


@router.post("/odoo-customer")
async def validate_odoo_customer(
    body: PartnerRequest, response: Response, authorization: str = Header(None)
):
    staff = await _authenticate_staff(authorization)
    _enforce(_single_limiter, staff, response)
    response.headers["Cache-Control"] = "no-store"

    if body.partner_id <= 0:
        return {"partner_id": body.partner_id, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False}

    cache_key = f"odoo_partner:{body.partner_id}"
    cached = _cache_get(cache_key)
    if cached:
        return {**cached, "cached": True}

    try:
        rows = await _odoo_search_read("res.partner", [["id", "=", body.partner_id]], PARTNER_FIELDS, limit=1)
        if not rows:
            result = {"partner_id": body.partner_id, "status": NOT_FOUND, "reason": "no_such_partner",
                      "checked_odoo": True, "message": "No res.partner with this id (archived included)."}
            _cache_put(cache_key, result, NOT_FOUND_TTL)
            return result
        leads = await _odoo_search_read(
            "crm.lead", [["partner_id", "=", body.partner_id]],
            ["id", "name", "stage_id", "email_from", "active", DEFAULT_FIELD_NAME],
        )
    except asyncio.TimeoutError:
        response.headers["Retry-After"] = "15"
        return {"partner_id": body.partner_id, "status": UNAVAILABLE, "reason": "timeout",
                "checked_odoo": True, "retry_after_seconds": 15}
    except Exception as exc:
        log.warning("Odoo partner lookup failed for %s: %s", body.partner_id, exc)
        response.headers["Retry-After"] = "15"
        return {"partner_id": body.partner_id, "status": UNAVAILABLE, "reason": "odoo_error",
                "checked_odoo": True, "retry_after_seconds": 15, "detail": str(exc)[:200]}

    result = {"status": VALID, "reason": None, "checked_odoo": True,
              "customer": _partner_payload(rows[0], leads)}
    _cache_put(cache_key, result, ODOO_TTL)
    return {**result, "cached": False}


@router.post("/odoo-customers")
async def validate_odoo_customers_bulk(
    body: PartnerBulkRequest, response: Response, authorization: str = Header(None)
):
    staff = await _authenticate_staff(authorization)
    _enforce(_bulk_limiter, staff, response)
    response.headers["Cache-Control"] = "no-store"

    ids = [int(i) for i in body.partner_ids if int(i) > 0]
    if len(body.partner_ids) > MAX_BULK_IDS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_BULK_IDS} ids per request")

    try:
        partners = await _odoo_search_read("res.partner", [["id", "in", ids]], PARTNER_FIELDS)
        leads = await _odoo_search_read(
            "crm.lead", [["partner_id", "in", ids]],
            ["id", "name", "stage_id", "email_from", "active", "partner_id", DEFAULT_FIELD_NAME],
        )
    except Exception as exc:
        log.warning("Bulk Odoo partner lookup failed: %s", exc)
        response.headers["Retry-After"] = "15"
        return {
            "requested": len(body.partner_ids),
            "summary": {VALID: 0, NOT_FOUND: 0, UNAVAILABLE: len(body.partner_ids)},
            "results": [{"partner_id": i, "status": UNAVAILABLE, "reason": "odoo_error", "checked_odoo": True}
                        for i in body.partner_ids],
        }

    leads_by_partner: Dict[int, List[Dict]] = {}
    for l in leads:
        pid = (l.get("partner_id") or [None])[0]
        if pid:
            leads_by_partner.setdefault(pid, []).append(l)

    found = {p["id"]: p for p in partners}
    results = []
    for pid in body.partner_ids:
        if int(pid) <= 0:
            results.append({"partner_id": pid, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False})
        elif int(pid) not in found:
            results.append({"partner_id": pid, "status": NOT_FOUND, "reason": "no_such_partner", "checked_odoo": True})
        else:
            results.append({"status": VALID, "reason": None, "checked_odoo": True,
                            "customer": _partner_payload(found[int(pid)], leads_by_partner.get(int(pid), []))})

    counts = {VALID: 0, NOT_FOUND: 0, UNAVAILABLE: 0}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"requested": len(body.partner_ids), "summary": counts, "results": results}


# ---------------------------------------------------------------------------
# 4. The grid endpoint — every column of every row, one round trip
# ---------------------------------------------------------------------------

class GridRow(BaseModel):
    row_id: str = Field(..., description="Opaque client key, echoed back")
    user_id: Optional[str] = None
    station_id: Optional[str] = None
    odoo_lead_id: Optional[int] = None
    odoo_partner_id: Optional[int] = None


class GridRequest(BaseModel):
    rows: List[GridRow] = Field(..., max_length=MAX_BULK_IDS)
    deep_check_missing: bool = False


@router.post("/rows")
async def validate_rows(
    body: GridRequest, response: Response, authorization: str = Header(None)
):
    """Validate the whole back-office grid at once.

    Cost for all 615 rows: one roster read (memory hit, or ~90 s once per 5 min),
    one PostgREST `in.()` query, and three XML-RPC calls. Measured components:
    300 leads 0.12 s, 298 partners 0.14 s, PostgREST in-filter 1.4 s. Total well
    under 3 s warm — versus 615 x 28-48 s (4.8-8 hours) for one-at-a-time.

    Also reports cross-system agreement per row, which is the thing an engineer
    actually needs: Odoo and Supabase disagree on the station id far more often
    than either id is simply absent.
    """
    staff = await _authenticate_staff(authorization)
    _enforce(_bulk_limiter, staff, response, cost=2.0)
    response.headers["Cache-Control"] = "no-store"

    if len(body.rows) > MAX_BULK_IDS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_BULK_IDS} rows per request")

    station_ids = [r.station_id.strip() for r in body.rows if r.station_id and r.station_id.strip()]
    lead_ids = [int(r.odoo_lead_id) for r in body.rows if r.odoo_lead_id and int(r.odoo_lead_id) > 0]
    partner_ids = [int(r.odoo_partner_id) for r in body.rows if r.odoo_partner_id and int(r.odoo_partner_id) > 0]

    roster, roster_age, roster_stale = await _get_roster()
    context = await _supabase_station_context(sorted(set(station_ids)))

    leads_by_id: Dict[int, Dict] = {}
    lead_error = None
    if lead_ids:
        try:
            for r in await _odoo_search_read("crm.lead", [["id", "in", sorted(set(lead_ids))]], LEAD_FIELDS):
                leads_by_id[r["id"]] = r
        except Exception as exc:
            lead_error = str(exc)[:200]
            log.warning("grid lead lookup failed: %s", exc)

    partners_by_id: Dict[int, Dict] = {}
    partner_leads: Dict[int, List[Dict]] = {}
    partner_error = None
    if partner_ids:
        try:
            for p in await _odoo_search_read("res.partner", [["id", "in", sorted(set(partner_ids))]], PARTNER_FIELDS):
                partners_by_id[p["id"]] = p
            for l in await _odoo_search_read(
                "crm.lead", [["partner_id", "in", sorted(set(partner_ids))]],
                ["id", "name", "stage_id", "email_from", "active", "partner_id", DEFAULT_FIELD_NAME],
            ):
                pid = (l.get("partner_id") or [None])[0]
                if pid:
                    partner_leads.setdefault(pid, []).append(l)
        except Exception as exc:
            partner_error = str(exc)[:200]
            log.warning("grid partner lookup failed: %s", exc)

    out: List[Dict[str, Any]] = []
    for row in body.rows:
        entry: Dict[str, Any] = {"row_id": row.row_id}

        sid = (row.station_id or "").strip()
        if not sid:
            entry["solis_station"] = {"status": NOT_FOUND, "reason": "empty", "checked_solis": False}
        elif not STATION_ID_RE.match(sid):
            entry["solis_station"] = {"station_id": sid, "status": NOT_FOUND, "reason": "malformed", "checked_solis": False}
        elif roster is None:
            entry["solis_station"] = {"station_id": sid, "status": UNAVAILABLE, "reason": "roster_unavailable", "checked_solis": False}
        elif sid in roster:
            entry["solis_station"] = {"station_id": sid, "status": VALID, "reason": None, "checked_solis": True,
                                      "source": "fleet_roster", "station": _station_payload(roster[sid]),
                                      "in_solviva_account": True}
        else:
            entry["solis_station"] = {"station_id": sid, "status": UNAVAILABLE,
                                      "reason": "absent_from_roster_pending_deep_check", "checked_solis": False,
                                      "hint": "POST /admin/validate/solis-station for a live check"}
        if sid and context.get(sid):
            entry["solis_station"].update(context[sid])
            owners = {str(a["user_id"]) for a in (context[sid].get("assigned_to") or [])}
            entry["solis_station"]["assignment_conflict"] = bool(owners) and owners != {str(row.user_id or "")}

        if row.odoo_lead_id and int(row.odoo_lead_id) > 0:
            if lead_error:
                entry["odoo_lead"] = {"lead_id": row.odoo_lead_id, "status": UNAVAILABLE, "reason": "odoo_error", "checked_odoo": True}
            elif int(row.odoo_lead_id) in leads_by_id:
                entry["odoo_lead"] = {"status": VALID, "reason": None, "checked_odoo": True,
                                      "lead": _lead_payload(leads_by_id[int(row.odoo_lead_id)], sid or None)}
            else:
                entry["odoo_lead"] = {"lead_id": row.odoo_lead_id, "status": NOT_FOUND, "reason": "no_such_lead", "checked_odoo": True}
        elif row.odoo_lead_id is not None:
            entry["odoo_lead"] = {"lead_id": row.odoo_lead_id, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False}

        if row.odoo_partner_id and int(row.odoo_partner_id) > 0:
            pid = int(row.odoo_partner_id)
            if partner_error:
                entry["odoo_customer"] = {"partner_id": pid, "status": UNAVAILABLE, "reason": "odoo_error", "checked_odoo": True}
            elif pid in partners_by_id:
                entry["odoo_customer"] = {"status": VALID, "reason": None, "checked_odoo": True,
                                          "customer": _partner_payload(partners_by_id[pid], partner_leads.get(pid, []))}
            else:
                entry["odoo_customer"] = {"partner_id": pid, "status": NOT_FOUND, "reason": "no_such_partner", "checked_odoo": True}
        elif row.odoo_partner_id is not None:
            entry["odoo_customer"] = {"partner_id": row.odoo_partner_id, "status": NOT_FOUND, "reason": "invalid_id", "checked_odoo": False}

        # Cross-system agreement. 157 of 606 profiles disagree with the Solis
        # plant name and 68 of 608 emails disagree with Odoo — mostly legitimate
        # (spouse/company inbox), so these are flags for a human, never auto-fixes.
        flags: List[str] = []
        lead = (entry.get("odoo_lead") or {}).get("lead") or {}
        if lead.get("station_id_state") == "mismatch":
            flags.append("odoo_station_id_differs_from_supabase")
        if lead.get("station_id_state") == "missing_in_odoo":
            flags.append("odoo_lead_has_no_station_id")
        if lead.get("archived"):
            flags.append("odoo_lead_archived")
        if (entry.get("odoo_customer") or {}).get("customer", {}).get("multi_station"):
            flags.append("customer_owns_multiple_stations_in_odoo")
        if entry["solis_station"].get("assignment_conflict"):
            flags.append("station_id_assigned_to_another_user")
        station = entry["solis_station"].get("station") or {}
        if station.get("state") == "offline":
            flags.append("solis_station_offline")
        entry["flags"] = flags
        entry["ok"] = all(
            (entry.get(k) or {}).get("status", VALID) == VALID
            for k in ("solis_station", "odoo_lead", "odoo_customer")
        ) and not flags

        out.append(entry)

    return {
        "requested": len(body.rows),
        "roster_age_seconds": round(roster_age, 1),
        "roster_stale": roster_stale,
        "odoo_lead_lookup_failed": bool(lead_error),
        "odoo_partner_lookup_failed": bool(partner_error),
        "summary": {
            "ok": sum(1 for r in out if r["ok"]),
            "with_flags": sum(1 for r in out if r["flags"]),
            "unavailable": sum(
                1 for r in out
                if any((r.get(k) or {}).get("status") == UNAVAILABLE
                       for k in ("solis_station", "odoo_lead", "odoo_customer"))
            ),
        },
        "rows": out,
    }


@router.post("/cache/invalidate")
async def invalidate_cache(response: Response, authorization: str = Header(None)):
    """Called by the write path after any identity edit.

    Without this, an engineer fixes a station id and the grid keeps showing the
    verdict cached from before the fix for up to five minutes — which reads as
    "my edit did not save" and provokes a second, worse edit.
    """
    staff = await _authenticate_staff(authorization)
    _enforce(_single_limiter, staff, response)
    for prefix in ("solis:", "odoo_lead:", "odoo_partner:"):
        _cache_invalidate(prefix)
    return {"invalidated": True}
