"""
Monitoring Admin — the engineering team's view of customer identity across
Supabase, Solis Cloud and Odoo, and the only sanctioned way to change it.

    GET  /                                 the single-page UI (main.py mounts serve_index;
                                           /monitoring-admin and /backoffice redirect there)
    GET  /monitoring-admin/api/config            Supabase URL + anon key for the UI's login
    GET  /monitoring-admin/api/me                who am I, what role
    GET  /monitoring-admin/api/rows              the grid: one row per station
    GET  /monitoring-admin/api/rows/{system_id}  one row + its audit trail + jobs
    PATCH /monitoring-admin/api/systems/{id}     edit a station (engineer+)
    POST /monitoring-admin/api/systems/{id}/mapping-verified
                                           tick/untick "manually verified" on a mapping (engineer+)
    PATCH /monitoring-admin/api/profiles/{id}    edit a customer profile (engineer+)
    POST /monitoring-admin/api/systems           attach another station to a customer (engineer+)
    POST /monitoring-admin/api/backfill          enqueue a backfill (engineer+)
    GET  /monitoring-admin/api/jobs              backfill queue
    POST /monitoring-admin/api/jobs/{id}/cancel  cancel a queued job (engineer+)
    GET  /monitoring-admin/api/audit             who changed what
    GET  /monitoring-admin/api/unresolved        the work queue: gaps the nightly pipeline cannot close
    GET/POST/PATCH /monitoring-admin/api/staff   staff directory (admin)

Rules this file enforces, because nothing else will:
  * Odoo is read-only. There is no endpoint that writes to Odoo and there must
    never be one — sales owns it. The odoo_* columns edited here are OUR cached
    copy of what Odoo says, kept so the grid can show disagreement.
  * Every write goes through db.audited(), so the 08 audit trigger records the
    staff member, the reason they typed, and a request id tying a multi-field
    edit together. A write without a reason is rejected.
  * Backfills are enqueued, never run inline. A worker drains backfill_jobs.
  * Authentication is the Supabase JWT + a staff_users row, shared with
    /admin/validate/* via _authenticate_staff. Roles: readonly < engineer < admin.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import psycopg
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field

from . import db
from .backfill_history import PHT, parse_month_day
from .solis_client import SolisCloudClient
from .onboard_from_odoo import DEFAULT_FIELD_NAME
from .validation_routes import _authenticate_staff, _get_roster, _odoo_search_read

log = logging.getLogger(__name__)
router = APIRouter(prefix="/monitoring-admin", tags=["Monitoring Admin"])

INDEX_HTML = Path(__file__).parent / "monitoring_admin" / "index.html"
STATION_ID_RE = re.compile(r"^\d{15,20}$")
ROLE_RANK = {"readonly": 0, "engineer": 1, "admin": 2}

# Every column on the two tables is editable from the drawer for now (user's
# decision 2026-09-21: "make them all available for edit"). Edits change OUR
# database only — nothing here writes to Odoo (sales' system of record) or to
# Solis (no API for it). The odoo_* / solis_* columns and system_name,
# capacity_kwp, battery_capacity_kwh are nightly copies, so an edit to one of
# them is overwritten by the next sync unless the source is fixed too; the
# drawer says so next to each block.
SYSTEM_FIELDS = {
    "solis_station_id", "system_name", "capacity_kwp", "installation_date", "status",
    "address", "battery_capacity_kwh", "is_primary",
    "odoo_lead_id", "odoo_lead_email", "odoo_lead_name", "odoo_stage",
    "solis_plant_name", "solis_user_email",
}
PROFILE_FIELDS = {
    "full_name", "phone", "address", "odoo_partner_id", "odoo_email",
    "odoo_customer_name", "electricity_provider_id",
}

# Fields the engineering team declares read-only here regardless of role,
# because they must be changed in the system that owns them. The team will
# submit the list; add entries as  field -> where to fix it  and both the API
# (refused with that message) and the drawer (shown, not typed into; the list
# is sent with /api/me) follow. Empty until the list arrives.
LOCKED_FIELDS: Dict[str, str] = {}


def _refuse_unknown(fields: Dict[str, Any], allowed: set, hint: str = "") -> None:
    locked = sorted(k for k in fields if k in LOCKED_FIELDS)
    if locked:
        raise HTTPException(400, f"Not editable here: {locked[0]} — {LOCKED_FIELDS[locked[0]]}")
    unknown = set(fields) - allowed
    if unknown:
        raise HTTPException(400, f"Not editable here: {sorted(unknown)}{hint}")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _require(staff: Dict[str, Any], min_role: str) -> None:
    if ROLE_RANK.get(staff.get("role", "readonly"), 0) < ROLE_RANK[min_role]:
        raise HTTPException(status_code=403, detail=f"Requires role '{min_role}' or higher")


def _j(v: Any) -> Any:
    """Make psycopg row values JSON-serialisable."""
    if isinstance(v, dict):
        return {k: _j(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_j(x) for x in v]
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, uuid.UUID):
        return str(v)
    return v


def _db_error(exc: Exception) -> HTTPException:
    """Translate constraint failures into a message an engineer can act on."""
    if isinstance(exc, psycopg.errors.UniqueViolation):
        msg = str(exc).splitlines()[0]
        if "solis_station_id" in msg or "user_station" in msg:
            return HTTPException(409, "That Solis station id is already assigned to another system.")
        if "backfill_jobs_one_active" in msg:
            return HTTPException(409, "A backfill for this station is already queued or running.")
        if "is_primary" in msg or "primary" in msg:
            return HTTPException(409, "This customer already has a primary station; unset it first.")
        return HTTPException(409, f"Conflicts with an existing row: {msg[:160]}")
    if isinstance(exc, (psycopg.errors.CheckViolation, psycopg.errors.ForeignKeyViolation,
                        psycopg.errors.InvalidTextRepresentation, psycopg.errors.DataError,
                        psycopg.errors.NotNullViolation)):
        return HTTPException(400, str(exc).splitlines()[0][:200])
    log.exception("monitoring admin database error")
    return HTTPException(500, "Database error")


def _reason(reason: Optional[str]) -> str:
    r = (reason or "").strip()
    if len(r) < 3:
        raise HTTPException(400, "A reason is required (what changed and why).")
    return r[:500]


_NAME_STOP = {
    "residence", "residential", "house", "home", "plant", "system", "solar", "meter", "grocery", "store",
    "inc", "corp", "co", "mr", "mrs", "ms", "dr", "engr", "and", "the", "of", "de", "dela", "del",
    "san", "sta", "sto", "jr", "sr", "ii", "iii", "2nd", "3rd",
}


def _name_tokens(s: Optional[str]) -> set:
    """Words that identify a person in a name-ish string; used to compare the
    customer's profile name, the Solis plant name and the Odoo name."""
    return {t for t in re.split(r"[^a-z0-9]+", (s or "").lower()) if len(t) > 2 and t not in _NAME_STOP}


def _supabase_env() -> Dict[str, str]:
    url = os.getenv("SUPABASE_URL", "").rstrip("/")
    anon = os.getenv("SUPABASE_ANON_KEY", "")
    service = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not service:
        raise HTTPException(500, "Supabase is not configured on the server")
    return {"url": url, "anon": anon, "service": service}


# ---------------------------------------------------------------------------
# UI + identity
# ---------------------------------------------------------------------------

def serve_index() -> FileResponse:
    """The single-page UI. main.py mounts this at the host root ("/")."""
    if not INDEX_HTML.exists():
        raise HTTPException(404, "Monitoring Admin UI not built")
    return FileResponse(INDEX_HTML, media_type="text/html", headers={"Cache-Control": "no-store"})


@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def monitoring_admin_index():
    # The page moved to the host root on 2026-09-18; this prefix keeps only the
    # API. Old links (and the 308 from /backoffice) still land on the page.
    return RedirectResponse("/", status_code=308)


@router.get("/api/config")
async def config():
    """What the browser needs to start a Supabase session. The anon key is public
    by design (it ships in the mobile app); RLS and staff_users do the gating."""
    env = _supabase_env()
    if not env["anon"]:
        raise HTTPException(500, "SUPABASE_ANON_KEY is not configured on the server")
    return {"supabase_url": env["url"], "supabase_anon_key": env["anon"]}


@router.get("/api/me")
async def me(authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    return {**staff, "locked_fields": LOCKED_FIELDS}


# ---------------------------------------------------------------------------
# the grid
# ---------------------------------------------------------------------------

# Per-station reading stats come from ONE grouped scan of each reading table
# (~116k and ~165k rows, both indexed on system_id), not a lateral count per
# row: the lateral version took 7.2 s for 627 rows; this takes well under 1 s.
ROWS_SQL = """
with daily as (
  select system_id, max("timestamp") as last_daily, count(*) as daily_rows
    from public.energy_readings group by system_id),
fm as (
  select system_id, max("timestamp") as last_5m
    from public.energy_readings_five_minutes group by system_id),
active_job as (
  select distinct on (system_id) system_id, id, status, granularity
    from public.backfill_jobs where status in ('queued','running')
    order by system_id, queued_at desc)
select s.id as system_id, s.user_id, s.system_name, s.capacity_kwp, s.installation_date,
       s.status, s.is_primary, s.address, s.battery_capacity_kwh,
       s.solis_station_id, s.solis_plant_name, s.solis_user_email, s.solis_validated_at, s.solis_validation,
       s.odoo_lead_id, s.odoo_lead_email, s.odoo_lead_name, s.odoo_stage,
       s.odoo_synced_at, s.solis_synced_at, p.odoo_synced_at as profile_odoo_synced_at,
       (select count(*) from public.system_access a where a.system_id = s.id) as viewer_count,
       s.last_backfill_at, s.last_backfill_status, s.created_at,
       s.mapping_verified_at, s.mapping_verified_by, s.mapping_verified_note,
       v.email as mapping_verified_by_email,
       p.full_name, p.phone, p.address as profile_address,
       p.odoo_partner_id, p.odoo_email, p.odoo_customer_name,
       u.email as login_email, u.last_sign_in_at, u.banned_until,
       r.last_daily, r.daily_rows, f.last_5m,
       j.id as active_job_id, j.status as active_job_status, j.granularity as active_job_granularity
  from public.solar_systems s
  left join public.user_profiles p on p.id = s.user_id
  left join auth.users u on u.id = s.user_id
  left join daily r on r.system_id = s.id
  left join fm f on f.system_id = s.id
  left join active_job j on j.system_id = s.id
  left join public.staff_users v on v.user_id = s.mapping_verified_by
"""


# The grid is the one expensive call (two grouped scans of the reading tables
# plus the auth.users join, ~2 s from Oregon to Mumbai). Cache it briefly;
# every mutation endpoint drops the cache so an edit shows on the next load,
# and ?fresh=1 bypasses it (the UI's Reload button).
ROWS_CACHE_TTL = 30.0
_rows_cache: Dict[str, Any] = {"at": 0.0, "data": None}
_rows_lock = threading.Lock()


def _invalidate_rows() -> None:
    _rows_cache["at"] = 0.0


@router.get("/api/rows")
async def rows(fresh: int = 0, authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    now = time.monotonic()
    cached = _rows_cache["data"]
    if not fresh and cached is not None and now - _rows_cache["at"] < ROWS_CACHE_TTL:
        return {"rows": cached, "count": len(cached), "cached": True}
    try:
        with db.connect(autocommit=True) as conn:
            data = _j(conn.execute(ROWS_SQL + " order by coalesce(p.full_name, s.system_name)").fetchall())
    except RuntimeError as exc:
        raise HTTPException(503, str(exc))
    with _rows_lock:
        _rows_cache["at"], _rows_cache["data"] = now, data
    return {"rows": data, "count": len(data), "cached": False}


@router.get("/api/rows/{system_id}")
async def row_detail(system_id: uuid.UUID, authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    with db.connect(autocommit=True) as conn:
        row = conn.execute(ROWS_SQL + " where s.id = %s", (system_id,)).fetchone()
        if not row:
            raise HTTPException(404, "No such system")
        siblings = conn.execute(
            "select id, system_name, solis_station_id, is_primary, status from public.solar_systems "
            "where user_id = %s and id <> %s order by created_at", (row["user_id"], system_id)).fetchall()
        audit = conn.execute(
            """select occurred_at, actor_email, actor_kind, table_name, row_pk, operation,
                      field_name, old_value, new_value, reason, source
                 from public.audit_log
                where (table_name = 'solar_systems' and row_pk = %s)
                   or (table_name = 'user_profiles' and row_pk = %s)
                   or (table_name = 'backfill_jobs' and row_pk in
                        (select id::text from public.backfill_jobs where system_id = %s))
                order by occurred_at desc limit 100""",
            (str(system_id), str(row["user_id"]), system_id)).fetchall()
        jobs = conn.execute(
            "select * from public.backfill_jobs where system_id = %s order by queued_at desc limit 20",
            (system_id,)).fetchall()
        # Why (if at all) this record sits under Unresolved → Possible wrong
        # station: the same heuristic the tab uses, for this one row, so the
        # drawer can spell the reasons out next to the plant fields.
        suspect_row = conn.execute(SUSPECT_SQL + " and s.id = %s", (system_id,)).fetchone()
    wrong = _wrong_station_signals(suspect_row) if suspect_row else None
    return {"row": _j(row), "other_systems": _j(siblings), "audit": _j(audit), "jobs": _j(jobs),
            "wrong_station": wrong}


# ---------------------------------------------------------------------------
# edits
# ---------------------------------------------------------------------------

class BackfillSpec(BaseModel):
    granularity: str = Field("daily", pattern="^(daily|five_minutes)$")
    days: int = Field(60, ge=1, le=730)


class EditRequest(BaseModel):
    fields: Dict[str, Any]
    reason: str
    backfill: Optional[BackfillSpec] = None


def _enqueue(conn: psycopg.Connection, system_id: uuid.UUID, station_id: Optional[str],
             spec: BackfillSpec, staff: Dict[str, Any], reason: str, request_id: str) -> Dict[str, Any]:
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
    job = conn.execute(
        """insert into public.backfill_jobs
             (system_id, solis_station_id, granularity, date_from, date_to,
              requested_by, requested_reason, request_id)
           values (%s, %s, %s, %s, %s, %s, %s, %s)
           returning id, status, granularity, date_from, date_to""",
        (system_id, station_id, spec.granularity, today - dt.timedelta(days=spec.days),
         today - dt.timedelta(days=1), staff["id"], reason, request_id)).fetchone()
    return _j(job)


def _enqueue_range(conn: psycopg.Connection, system_id: uuid.UUID, station_id: Optional[str], granularity: str,
                   date_from: dt.date, date_to: dt.date, staff: Dict[str, Any], reason: str, request_id: str) -> Dict[str, Any]:
    job = conn.execute(
        """insert into public.backfill_jobs
             (system_id, solis_station_id, granularity, date_from, date_to,
              requested_by, requested_reason, request_id)
           values (%s, %s, %s, %s, %s, %s, %s, %s)
           returning id, status, granularity, date_from, date_to""",
        (system_id, station_id, granularity, date_from, date_to, staff["id"], reason, request_id)).fetchone()
    return _j(job)


@router.patch("/api/systems/{system_id}")
async def edit_system(system_id: uuid.UUID, body: EditRequest, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)

    _refuse_unknown(body.fields, SYSTEM_FIELDS)
    fields = dict(body.fields)
    if not fields and not body.backfill:
        raise HTTPException(400, "Nothing to change")

    sid = fields.get("solis_station_id")
    if sid is not None:
        sid = str(sid).strip() or None
        if sid and not STATION_ID_RE.match(sid):
            raise HTTPException(400, "Solis station id must be 15–20 digits")
        fields["solis_station_id"] = sid
    for k in ("odoo_lead_id", "capacity_kwp", "battery_capacity_kwh", "installation_date"):
        if k in fields and fields[k] in ("", None):
            fields[k] = None

    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            if fields.get("is_primary") is True:
                # One primary per customer; demote the others in the same transaction.
                conn.execute(
                    "update public.solar_systems set is_primary = false "
                    "where user_id = (select user_id from public.solar_systems where id = %s) and id <> %s",
                    (system_id, system_id))
            if fields:
                sets = ", ".join(f'"{k}" = %s' for k in fields)
                updated = conn.execute(
                    f"update public.solar_systems set {sets}, updated_at = now() where id = %s returning id",
                    (*fields.values(), system_id)).fetchone()
                if not updated:
                    raise HTTPException(404, "No such system")
            job = None
            if body.backfill:
                station_now = conn.execute(
                    "select solis_station_id from public.solar_systems where id = %s", (system_id,)).fetchone()
                job = _enqueue(conn, system_id, station_now and station_now["solis_station_id"],
                               body.backfill, staff, reason, request_id)
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, "changed": sorted(fields), "backfill_job": job}


class MappingVerifiedRequest(BaseModel):
    verified: bool
    reason: str


@router.post("/api/systems/{system_id}/mapping-verified")
async def set_mapping_verified(system_id: uuid.UUID, body: MappingVerifiedRequest,
                               authorization: str = Header(None)):
    """Tick or untick "manually verified" on a station's customer mapping.

    The "possible wrong station" scan is a name heuristic: it cannot clear a
    plant named after a business, a church or a relative. An engineer who has
    checked one says so here. The row then leaves the scan, and WHO decided,
    WHEN and WHY are stamped from the staff session — never taken from the
    browser — and land in the audit trail through the 08 trigger (file 15 added
    the two columns to its list). Changing the station id later clears the tick
    (trg_clear_mapping_verified): the verdict was about that mapping only.

    Its own endpoint rather than a PATCH field so the by/at columns can never
    be set to arbitrary values from the client.
    """
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            if body.verified:
                row = conn.execute(
                    """update public.solar_systems
                          set mapping_verified_at = now(), mapping_verified_by = %s,
                              mapping_verified_note = %s, updated_at = now()
                        where id = %s
                    returning id, solis_station_id, mapping_verified_at, mapping_verified_note""",
                    (staff["id"], reason, system_id)).fetchone()
            else:
                row = conn.execute(
                    """update public.solar_systems
                          set mapping_verified_at = null, mapping_verified_by = null,
                              mapping_verified_note = null, updated_at = now()
                        where id = %s
                    returning id, solis_station_id, mapping_verified_at, mapping_verified_note""",
                    (system_id,)).fetchone()
            if not row:
                raise HTTPException(404, "No such system")
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, **_j(dict(row)),
            "mapping_verified_by_email": staff["email"] if body.verified else None}


@router.patch("/api/profiles/{user_id}")
async def edit_profile(user_id: uuid.UUID, body: EditRequest, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    _refuse_unknown(body.fields, PROFILE_FIELDS, " (the station id lives on the system)")
    fields = dict(body.fields)
    if not fields:
        raise HTTPException(400, "Nothing to change")
    for k in ("odoo_partner_id", "electricity_provider_id"):
        if k in fields and fields[k] in ("", None):
            fields[k] = None
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            sets = ", ".join(f'"{k}" = %s' for k in fields)
            updated = conn.execute(
                f"update public.user_profiles set {sets}, updated_at = now() where id = %s returning id",
                (*fields.values(), user_id)).fetchone()
            if not updated:
                raise HTTPException(404, "No such profile")
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, "changed": sorted(fields)}


class AddStationRequest(BaseModel):
    user_id: uuid.UUID
    solis_station_id: str
    system_name: Optional[str] = None
    capacity_kwp: Optional[float] = None
    installation_date: Optional[dt.date] = None
    reason: str
    backfill: Optional[BackfillSpec] = BackfillSpec()


@router.post("/api/systems")
async def add_station(body: AddStationRequest, authorization: str = Header(None)):
    """Attach a further Solis station to an existing customer — the multi-station
    case the nightly onboarding deliberately refuses (skipped_would_repoint)."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    sid = body.solis_station_id.strip()
    if not STATION_ID_RE.match(sid):
        raise HTTPException(400, "Solis station id must be 15–20 digits")

    name, capacity = body.system_name, body.capacity_kwp
    if not name or capacity is None:
        key_id, key_secret = os.getenv("SOLIS_CLOUD_KEY_ID", ""), os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
        detail = None
        if key_id and key_secret:
            try:
                detail = await SolisCloudClient(key_id, key_secret).station_detail(sid)
            except Exception as exc:
                log.warning("stationDetail for %s failed: %s", sid, exc)
        if not detail:
            raise HTTPException(
                400, "Solis returned no detail for that station; supply system_name and capacity_kwp explicitly.")
        name = name or detail.get("stationName") or sid
        capacity = capacity if capacity is not None else float(detail.get("capacity") or 0)

    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            owner = conn.execute("select id from public.user_profiles where id = %s", (body.user_id,)).fetchone()
            if not owner:
                raise HTTPException(404, "No such customer profile")
            row = conn.execute(
                """insert into public.solar_systems
                     (user_id, system_name, capacity_kwp, installation_date, address, status,
                      solis_station_id, solis_plant_name, is_primary)
                   values (%s, %s, %s, %s, '—', 'active', %s, %s, false)
                   returning id""",
                (body.user_id, name, capacity,
                 body.installation_date or (dt.date.today() - dt.timedelta(days=body.backfill.days if body.backfill else 60)),
                 sid, name)).fetchone()
            job = _enqueue(conn, row["id"], sid, body.backfill, staff, reason, request_id) if body.backfill else None
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "system_id": str(row["id"]), "request_id": request_id, "backfill_job": job}


# ---------------------------------------------------------------------------
# wrong-station repair: check first, then remap with quarantine
# ---------------------------------------------------------------------------

MISMATCH_ABS_KWH, MISMATCH_REL = 0.05, 0.01


class ReconcileRequest(BaseModel):
    months: int = Field(3, ge=1, le=12)


@router.post("/api/systems/{system_id}/reconcile")
async def reconcile_system(system_id: uuid.UUID, body: ReconcileRequest, authorization: str = Header(None)):
    """Compare this system's stored daily production with what Solis reports for
    its station over the last N months. A right mapping agrees ~99% of days; a
    wrong one agrees on almost none. Read-only, a few Solis calls."""
    await _authenticate_staff(authorization)
    today = dt.datetime.now(PHT).date()
    months, d = [], today.replace(day=1)
    for _ in range(body.months):
        months.append(d.strftime("%Y-%m"))
        d = d.replace(year=d.year - 1, month=12) if d.month == 1 else d.replace(month=d.month - 1)
    y, m = (int(x) for x in months[-1].split("-"))
    since = dt.datetime(y, m, 1, tzinfo=PHT)

    with db.connect(autocommit=True) as conn:
        sys_row = conn.execute(
            "select id, solis_station_id, capacity_kwp from public.solar_systems where id = %s", (system_id,)).fetchone()
        if not sys_row:
            raise HTTPException(404, "No such system")
        if not sys_row["solis_station_id"]:
            raise HTTPException(400, "This system has no Solis station id")
        ours = {r["day"].isoformat(): float(r["kwh"] or 0) for r in conn.execute(
            """select ("timestamp" at time zone 'Asia/Manila')::date as day, production_kwh as kwh
                 from public.energy_readings where system_id = %s and "timestamp" >= %s""",
            (system_id, since)).fetchall()}

    key_id, key_secret = os.getenv("SOLIS_CLOUD_KEY_ID", ""), os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
    if not key_id or not key_secret:
        raise HTTPException(500, "Solis credentials not configured")
    solis = SolisCloudClient(key_id, key_secret)
    theirs: Dict[str, float] = {}
    errors: List[str] = []
    for mon in months:
        try:
            data = await solis.station_month(sys_row["solis_station_id"], mon)
        except Exception as exc:
            errors.append(f"{mon}: {str(exc)[:80]}")
            continue
        for day in (data if isinstance(data, list) else []):
            p = parse_month_day(day, float(sys_row["capacity_kwp"] or 0))
            if p and p["date_str"] < today.isoformat():
                theirs[p["date_str"]] = p["production_kwh"]

    compared = sorted(set(ours) & set(theirs))
    mism = []
    for day_s in compared:
        a, b = ours[day_s], theirs[day_s]
        if abs(a - b) > max(MISMATCH_ABS_KWH, MISMATCH_REL * max(abs(a), abs(b))):
            mism.append({"day": day_s, "ours_kwh": round(a, 2), "solis_kwh": round(b, 2), "diff_kwh": round(b - a, 2)})
    pct = round(100.0 * (len(compared) - len(mism)) / len(compared), 1) if compared else None
    # This compares OUR rows with Solis for the station id WE HOLD. It proves
    # data integrity for that id (stale rows, a past remap never backfilled),
    # not identity: a customer wrongly mapped to another plant still agrees
    # 99% here, because the rows are that plant's data faithfully copied.
    # Identity is judged from the plant name and Solis email vs the customer.
    if not compared:
        verdict = "nothing to compare — no overlapping days"
    elif pct >= 97:
        verdict = "stored data matches Solis for this station id"
    elif pct >= 80:
        verdict = "mostly matches — look at the mismatched days (Solis revises recent days)"
    else:
        verdict = "stored data does NOT match this station's Solis history — stale rows, or a remap that was never backfilled; queue a backfill"
    return _j({
        "station_id": sys_row["solis_station_id"], "months": months, "days_compared": len(compared),
        "mismatched": len(mism), "agreement_pct": pct, "verdict": verdict,
        "only_in_ours": sorted(set(ours) - set(theirs))[:30], "only_in_solis": sorted(set(theirs) - set(ours))[:30],
        "mismatches": sorted(mism, key=lambda x: -abs(x["diff_kwh"]))[:40], "solis_errors": errors,
    })


class RemapRequest(BaseModel):
    new_station_id: str
    reason: str = ""
    dry_run: bool = True
    backfill_from: Optional[dt.date] = None


@router.post("/api/systems/{system_id}/remap")
async def remap_station(system_id: uuid.UUID, body: RemapRequest, authorization: str = Header(None)):
    """Move a system to the correct Solis station — the fix for a wrong station id.

    In ONE transaction: every reading captured under the old id is moved to
    energy_readings_quarantine (migration 13 — recoverable, never deleted), the
    system's id/name/capacity/email/first-power date are refreshed from the Solis
    roster, any live backfill jobs are cancelled, and two new jobs are queued: a
    daily backfill of the correct station from its first-power date (or
    backfill_from) and a five-minute refresh of today. dry_run=True (default)
    returns exactly what would happen and writes nothing.
    """
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    new_sid = body.new_station_id.strip()
    # DETACH mode (new_station_id empty): the customer was given someone else's
    # station and their own plant is not in our Solis account at all — found to
    # be the common case on 2026-09-17 (no plant among 654 carried any of the
    # suspects' names). Quarantine the wrong data, clear the id, mark the system
    # 'pending', queue nothing. Sales/engineering attach the right plant later.
    detach = new_sid == ""
    rec: Dict[str, Any] = {}
    stale = False
    if not detach:
        if not STATION_ID_RE.match(new_sid):
            raise HTTPException(400, "Solis station id must be 15–20 digits (or empty to detach)")
        roster, _age, stale = await _get_roster()
        if roster is None:
            raise HTTPException(503, "Solis roster unavailable — try again in a minute")
        rec = roster.get(new_sid) or {}
        if not rec:
            raise HTTPException(400, f"Station {new_sid} is not in our Solis account (checked the live roster)")

    with db.connect(autocommit=True) as conn:
        sys_row = conn.execute(
            "select id, user_id, solis_station_id, system_name, is_primary from public.solar_systems where id = %s",
            (system_id,)).fetchone()
        if not sys_row:
            raise HTTPException(404, "No such system")
        if not detach and sys_row["solis_station_id"] == new_sid:
            raise HTTPException(400, "That is already this system's station id")
        if detach and not sys_row["solis_station_id"]:
            raise HTTPException(400, "This system has no station id to detach")
        holder = None if detach else conn.execute(
            """select s.id, s.system_name, p.full_name from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                where s.solis_station_id = %s and s.id <> %s""", (new_sid, system_id)).fetchone()
        if holder:
            raise HTTPException(409, f"Station {new_sid} is already assigned to "
                                     f"{holder['full_name'] or holder['system_name']} — correct that system first")
        stats = conn.execute(
            """select count(*) as rows, min("timestamp") as first_ts, max("timestamp") as last_ts,
                      coalesce(round(sum(production_kwh)::numeric, 1), 0) as kwh
                 from public.energy_readings where system_id = %s""", (system_id,)).fetchone()
        fm_rows = conn.execute(
            "select count(*) as n from public.energy_readings_five_minutes where system_id = %s", (system_id,)).fetchone()["n"]
        live_jobs = conn.execute(
            "select id from public.backfill_jobs where system_id = %s and status in ('queued','running')",
            (system_id,)).fetchall()

    first_power = None
    for k in ("fisPowerTimeStr", "createDateStr"):
        if rec.get(k):
            try:
                first_power = dt.date.fromisoformat(str(rec[k])[:10])
                break
            except ValueError:
                pass
    today = dt.datetime.now(PHT).date()
    backfill_from = None if detach else (body.backfill_from or first_power or (today - dt.timedelta(days=365)))
    preview = _j({
        "system_id": system_id, "detach": detach,
        "old_station_id": sys_row["solis_station_id"], "new_station_id": None if detach else new_sid,
        "new_plant_name": rec.get("stationName"), "new_capacity_kwp": float(rec.get("capacity") or 0) or None,
        "new_solis_email": rec.get("userEmail"), "first_power_date": first_power,
        "rows_to_quarantine": stats["rows"], "quarantine_range": [stats["first_ts"], stats["last_ts"]],
        "kwh_to_quarantine": stats["kwh"], "five_minute_rows_to_drop": fm_rows,
        "backfill_from": backfill_from, "backfill_to": None if detach else today - dt.timedelta(days=1),
        "live_jobs_to_cancel": len(live_jobs), "roster_stale": stale,
    })
    if body.dry_run:
        return {"dry_run": True, **preview}

    reason = _reason(body.reason)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            conn.execute(
                """insert into public.energy_readings_quarantine
                   select r.*, now(), %s, %s, %s, %s, %s
                     from public.energy_readings r where r.system_id = %s""",
                (staff["id"], request_id, reason, sys_row["solis_station_id"], None if detach else new_sid, system_id))
            conn.execute("delete from public.energy_readings where system_id = %s", (system_id,))
            conn.execute("delete from public.energy_readings_five_minutes where system_id = %s", (system_id,))
            if live_jobs:
                conn.execute(
                    "update public.backfill_jobs set status = 'cancelled', finished_at = now(), "
                    "error = 'superseded by remap' where system_id = %s and status in ('queued','running')",
                    (system_id,))
            if detach:
                # No correct plant known: clear the id, park the system as
                # 'pending' so the syncs skip it and the grid shows it as
                # unresolved, keep the old plant name for the audit trail.
                conn.execute(
                    """update public.solar_systems
                          set solis_station_id = null, status = 'pending',
                              solis_validated_at = now(), solis_validation = 'absent',
                              last_backfill_status = null, last_backfill_at = null, updated_at = now()
                        where id = %s""", (system_id,))
                job = job5 = None
            else:
                conn.execute(
                    """update public.solar_systems
                          set solis_station_id = %s, solis_plant_name = %s, solis_user_email = %s,
                              capacity_kwp = coalesce(%s, capacity_kwp),
                              installation_date = coalesce(%s, installation_date),
                              status = case when status = 'pending' then 'active' else status end,
                              solis_validated_at = now(), solis_validation = 'verified',
                              last_backfill_status = null, last_backfill_at = null, updated_at = now()
                        where id = %s""",
                    (new_sid, rec.get("stationName") or sys_row["system_name"], rec.get("userEmail"),
                     float(rec.get("capacity") or 0) or None, first_power, system_id))
                job = _enqueue_range(conn, system_id, new_sid, "daily", backfill_from, today - dt.timedelta(days=1),
                                     staff, reason, request_id)
                job5 = _enqueue_range(conn, system_id, new_sid, "five_minutes", today, today, staff, reason, request_id)
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"dry_run": False, "request_id": request_id, "backfill_job": job, "five_minute_job": job5, **preview}


# ---------------------------------------------------------------------------
# retire an account: a duplicate login, or test data, or another plant's data
# ---------------------------------------------------------------------------

class RetireRequest(BaseModel):
    reason: str = ""
    # The live account of the same customer, when this one is a duplicate. With
    # a twin, the account may only be retired if the twin already holds every
    # day this one holds — otherwise it is a MERGE, not a retirement.
    twin_user_id: Optional[uuid.UUID] = None
    # False: ban the login (reversible — the person keeps their other login).
    # True: delete it (test data, or a login that never had a plant of its own).
    delete_login: bool = False
    dry_run: bool = True


@router.post("/api/customers/{user_id}/retire")
async def retire_customer(user_id: uuid.UUID, body: RetireRequest, authorization: str = Header(None)):
    """Remove a customer account that should not exist: a second login for a
    customer who already has a working one, seed/test data, or a login that
    was given another plant's station. Readings go to quarantine (recoverable),
    the system and profile rows are deleted, the login is banned or deleted.
    Refuses staff accounts, accounts that still own a station (remap or detach
    first), and — when a twin is named — accounts holding days the twin lacks."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    with db.connect(autocommit=True) as conn:
        login = conn.execute("select email, last_sign_in_at from auth.users where id = %s", (user_id,)).fetchone()
        if not login:
            raise HTTPException(404, "No such login")
        if conn.execute("select 1 from public.staff_users where user_id = %s", (user_id,)).fetchone():
            raise HTTPException(400, "That is a staff account — manage it in the Staff tab")
        if str(user_id) == staff["id"]:
            raise HTTPException(400, "You cannot retire yourself")
        profile = conn.execute("select full_name from public.user_profiles where id = %s", (user_id,)).fetchone()
        systems = conn.execute(
            "select id, system_name, solis_station_id from public.solar_systems where user_id = %s", (user_id,)).fetchall()
        twin = None
        if body.twin_user_id:
            twin = conn.execute(
                """select u.email, p.full_name, s.solis_station_id
                     from auth.users u left join public.user_profiles p on p.id = u.id
                     left join public.solar_systems s on s.user_id = u.id and s.is_primary
                    where u.id = %s""", (body.twin_user_id,)).fetchone()
            if not twin:
                raise HTTPException(404, "No such twin login")
        still_owned = [s for s in systems if s["solis_station_id"]
                       and not (twin and s["solis_station_id"] == twin["solis_station_id"])]
        if still_owned:
            raise HTTPException(400, f"This account still owns station {still_owned[0]['solis_station_id']} — "
                                     "remap or detach it first, or name the twin that holds the same station")
        stats = conn.execute(
            """select count(*) as rows, min("timestamp") as first_ts, max("timestamp") as last_ts,
                      coalesce(round(sum(production_kwh)::numeric, 1), 0) as kwh
                 from public.energy_readings where user_id = %s""", (user_id,)).fetchone()
        fm_rows = conn.execute(
            "select count(*) as n from public.energy_readings_five_minutes where user_id = %s", (user_id,)).fetchone()["n"]
        only_here = None
        if twin:
            only_here = conn.execute(
                """select count(*) from (
                     select distinct ("timestamp" at time zone 'Asia/Manila')::date from public.energy_readings where user_id = %s
                     except
                     select distinct ("timestamp" at time zone 'Asia/Manila')::date from public.energy_readings where user_id = %s) q""",
                (user_id, body.twin_user_id)).fetchone()["count"]
            if only_here:
                raise HTTPException(400, f"This account holds {only_here} day(s) the twin does not — that is a merge, not a retirement")
        live_jobs = conn.execute(
            "select count(*) as n from public.backfill_jobs where system_id in "
            "(select id from public.solar_systems where user_id = %s) and status in ('queued','running')", (user_id,)).fetchone()["n"]
        # cleaned_data is a legacy derived copy of the readings (nothing has
        # written to it since 2026-04-22) and references solar_systems with
        # ON DELETE RESTRICT; a retired system's rows there go with it.
        cleaned_rows = conn.execute(
            "select count(*) as n from public.cleaned_data where system_id in "
            "(select id from public.solar_systems where user_id = %s)", (user_id,)).fetchone()["n"]
        # Customer-side tables keyed on the login (FKs to auth.users, NO ACTION).
        # They block deleting the login; for a banned login they simply stay.
        related = {t: conn.execute(f"select count(*) as n from public.{t} where {c} = %s", (user_id,)).fetchone()["n"]
                   for t, c in (("energy_tips", "user_id"), ("billing_records", "user_id"), ("support_tickets", "user_id"),
                                ("ticket_messages", "user_id"), ("referrals", "referrer_user_id"))}
        related = {t: n for t, n in related.items() if n}

    preview = _j({
        "user_id": user_id, "email": login["email"], "full_name": profile["full_name"] if profile else None,
        "last_sign_in_at": login["last_sign_in_at"], "systems": [dict(s) for s in systems],
        "twin": ({"email": twin["email"], "full_name": twin["full_name"], "station": twin["solis_station_id"]} if twin else None),
        "days_only_here": only_here, "rows_to_quarantine": stats["rows"],
        "quarantine_range": [stats["first_ts"], stats["last_ts"]], "kwh_to_quarantine": stats["kwh"],
        "five_minute_rows_to_drop": fm_rows, "cleaned_data_rows_to_drop": cleaned_rows,
        "live_jobs_to_cancel": live_jobs, "related_rows": related,
        "related_rows_action": ("delete" if body.delete_login else "keep") if related else None,
        "login_action": "delete" if body.delete_login else "ban",
    })
    if body.dry_run:
        return {"dry_run": True, **preview}

    reason = _reason(body.reason)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            conn.execute(
                """insert into public.energy_readings_quarantine
                   select r.*, now(), %s, %s, %s,
                          (select solis_station_id from public.solar_systems s where s.id = r.system_id), %s
                     from public.energy_readings r where r.user_id = %s""",
                (staff["id"], request_id, reason, twin["solis_station_id"] if twin else None, user_id))
            conn.execute("delete from public.energy_readings where user_id = %s", (user_id,))
            conn.execute("delete from public.energy_readings_five_minutes where user_id = %s", (user_id,))
            conn.execute(
                "update public.backfill_jobs set status = 'cancelled', finished_at = now(), error = 'account retired' "
                "where system_id in (select id from public.solar_systems where user_id = %s) and status in ('queued','running')",
                (user_id,))
            conn.execute(
                "delete from public.cleaned_data where system_id in (select id from public.solar_systems where user_id = %s)",
                (user_id,))
            conn.execute("delete from public.solar_systems where user_id = %s", (user_id,))
            conn.execute("delete from public.user_profiles where id = %s", (user_id,))
            if body.delete_login and related:
                # Test data / a login with no plant of its own: the customer-side
                # rows would block deleting the login. Children before parents.
                conn.execute("delete from public.ticket_messages where user_id = %s or ticket_id in "
                             "(select id from public.support_tickets where user_id = %s)", (user_id, user_id))
                conn.execute("delete from public.support_tickets where user_id = %s", (user_id,))
                conn.execute("delete from public.billing_records where user_id = %s", (user_id,))
                conn.execute("delete from public.referrals where referrer_user_id = %s", (user_id,))
                conn.execute("delete from public.energy_tips where user_id = %s", (user_id,))
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)

    env = _supabase_env()
    hdr = {"apikey": env["service"], "Authorization": f"Bearer {env['service']}"}
    async with httpx.AsyncClient(timeout=30) as client:
        if body.delete_login:
            r = await client.delete(env["url"] + f"/auth/v1/admin/users/{user_id}", headers=hdr)
        else:
            r = await client.put(env["url"] + f"/auth/v1/admin/users/{user_id}", headers=hdr, json={"ban_duration": "876000h"})
    login_done = r.status_code in (200, 204)
    if not login_done:
        log.warning("retire: data removed but login %s failed for %s: %s %s", preview["login_action"], login["email"], r.status_code, r.text[:160])
    _invalidate_rows()
    return {"dry_run": False, "request_id": request_id, "login_done": login_done, **preview}


# ---------------------------------------------------------------------------
# merge: two logins, one customer, each holding its own plant
# ---------------------------------------------------------------------------

class MergeRequest(BaseModel):
    survivor_user_id: uuid.UUID
    reason: str = ""
    dry_run: bool = True


@router.post("/api/customers/{user_id}/merge-into")
async def merge_customer(user_id: uuid.UUID, body: MergeRequest, authorization: str = Header(None)):
    """Fold this login (the loser) into the survivor: its stations move to the
    survivor as secondary systems, their readings and the customer-side rows
    follow, the loser's profile is deleted and its login banned. Nothing is
    quarantined or refetched — the readings simply change owner. Use Retire
    instead when the loser holds a COPY of a plant the survivor already has."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    survivor = body.survivor_user_id
    if survivor == user_id:
        raise HTTPException(400, "Survivor and loser are the same login")
    with db.connect(autocommit=True) as conn:
        acct = {}
        for label, uid_ in (("loser", user_id), ("survivor", survivor)):
            u = conn.execute("select email, last_sign_in_at from auth.users where id = %s", (uid_,)).fetchone()
            if not u:
                raise HTTPException(404, f"No such {label} login")
            if conn.execute("select 1 from public.staff_users where user_id = %s", (uid_,)).fetchone():
                raise HTTPException(400, f"The {label} is a staff account")
            p = conn.execute("select full_name, phone, address from public.user_profiles where id = %s", (uid_,)).fetchone()
            systems = conn.execute(
                """select s.id, s.solis_station_id, coalesce(s.solis_plant_name, s.system_name) as plant, s.is_primary,
                          (select count(*) from public.energy_readings r where r.system_id = s.id) as readings
                     from public.solar_systems s where s.user_id = %s order by s.created_at""", (uid_,)).fetchall()
            acct[label] = {"email": u["email"], "last_sign_in_at": u["last_sign_in_at"],
                           "full_name": p["full_name"] if p else None, "systems": [dict(s) for s in systems]}
        if not conn.execute("select 1 from public.user_profiles where id = %s", (survivor,)).fetchone():
            raise HTTPException(400, "The survivor has no profile — it cannot receive systems")
        shared = {s["solis_station_id"] for s in acct["loser"]["systems"]} & {s["solis_station_id"] for s in acct["survivor"]["systems"]} - {None}
        if shared:
            raise HTTPException(400, f"Both logins hold station {sorted(shared)[0]} — that is a duplicate; use Retire on the copy")
        related = {t: conn.execute(f"select count(*) as n from public.{t} where {col} = %s", (user_id,)).fetchone()["n"]
                   for t, col in (("energy_tips", "user_id"), ("billing_records", "user_id"), ("support_tickets", "user_id"),
                                  ("ticket_messages", "user_id"), ("referrals", "referrer_user_id"))}
        related = {t: n for t, n in related.items() if n}
        readings = conn.execute("select count(*) as n from public.energy_readings where user_id = %s", (user_id,)).fetchone()["n"]

    preview = _j({"loser": acct["loser"], "survivor": acct["survivor"], "readings_to_move": readings,
                  "related_rows_to_move": related, "loser_login_action": "ban"})
    if body.dry_run:
        return {"dry_run": True, **preview}

    reason = _reason(body.reason)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            conn.execute("update public.solar_systems set user_id = %s, is_primary = false, updated_at = now() where user_id = %s",
                         (survivor, user_id))
            for tbl in ("energy_readings", "energy_readings_five_minutes", "cleaned_data", "energy_readings_quarantine",
                        "energy_tips", "billing_records", "support_tickets", "ticket_messages"):
                conn.execute(f"update public.{tbl} set user_id = %s where user_id = %s", (survivor, user_id))
            conn.execute("update public.referrals set referrer_user_id = %s where referrer_user_id = %s", (survivor, user_id))
            conn.execute("delete from public.user_profiles where id = %s", (user_id,))
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    env = _supabase_env()
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(env["url"] + f"/auth/v1/admin/users/{user_id}",
                             headers={"apikey": env["service"], "Authorization": f"Bearer {env['service']}"},
                             json={"ban_duration": "876000h"})
    _invalidate_rows()
    return {"dry_run": False, "request_id": request_id, "login_banned": r.status_code == 200, **preview}


# ---------------------------------------------------------------------------
# backfill queue
# ---------------------------------------------------------------------------

class BackfillRequest(BaseModel):
    system_id: uuid.UUID
    granularity: str = Field("daily", pattern="^(daily|five_minutes)$")
    date_from: dt.date
    date_to: dt.date
    reason: str


@router.post("/api/backfill")
async def enqueue_backfill(body: BackfillRequest, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    if body.date_to < body.date_from:
        raise HTTPException(400, "date_to is before date_from")
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            sys_row = conn.execute(
                "select solis_station_id from public.solar_systems where id = %s", (body.system_id,)).fetchone()
            if not sys_row:
                raise HTTPException(404, "No such system")
            if not sys_row["solis_station_id"]:
                raise HTTPException(400, "This system has no Solis station id; set it first.")
            job = conn.execute(
                """insert into public.backfill_jobs
                     (system_id, solis_station_id, granularity, date_from, date_to,
                      requested_by, requested_reason, request_id)
                   values (%s, %s, %s, %s, %s, %s, %s, %s) returning *""",
                (body.system_id, sys_row["solis_station_id"], body.granularity, body.date_from, body.date_to,
                 staff["id"], reason, request_id)).fetchone()
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "job": _j(job)}


@router.get("/api/jobs")
async def jobs(status: Optional[str] = None, limit: int = 200, authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    with db.connect(autocommit=True) as conn:
        if status:
            data = conn.execute(
                """select j.*, s.system_name, p.full_name from public.backfill_jobs j
                     join public.solar_systems s on s.id = j.system_id
                     left join public.user_profiles p on p.id = s.user_id
                    where j.status = %s order by j.queued_at desc limit %s""", (status, min(limit, 1000))).fetchall()
        else:
            data = conn.execute(
                """select j.*, s.system_name, p.full_name from public.backfill_jobs j
                     join public.solar_systems s on s.id = j.system_id
                     left join public.user_profiles p on p.id = s.user_id
                    order by j.queued_at desc limit %s""", (min(limit, 1000),)).fetchall()
    return {"jobs": _j(data)}


@router.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: uuid.UUID, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    try:
        with db.audited(staff["id"], staff["email"], "cancelled from Monitoring Admin") as conn:
            row = conn.execute(
                "update public.backfill_jobs set status = 'cancelled', finished_at = now() "
                "where id = %s and status = 'queued' returning id", (job_id,)).fetchone()
    except psycopg.Error as exc:
        raise _db_error(exc)
    if not row:
        raise HTTPException(409, "Only queued jobs can be cancelled")
    return {"ok": True}


# ---------------------------------------------------------------------------
# audit + work queue
# ---------------------------------------------------------------------------

@router.get("/api/audit")
async def audit(table: Optional[str] = None, row_pk: Optional[str] = None,
                actor: Optional[str] = None, limit: int = 200, authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    clauses, params = [], []
    if table:
        clauses.append("table_name = %s"); params.append(table)
    if row_pk:
        clauses.append("row_pk = %s"); params.append(row_pk)
    if actor:
        clauses.append("actor_email ilike %s"); params.append(f"%{actor}%")
    where = ("where " + " and ".join(clauses)) if clauses else ""
    with db.connect(autocommit=True) as conn:
        data = conn.execute(
            f"select * from public.audit_log {where} order by occurred_at desc limit %s",
            (*params, min(limit, 2000))).fetchall()
    return {"audit": _j(data)}


# One row per mapped station with the three names and three emails the
# "possible wrong station" heuristic compares. Shared by the Unresolved tab
# (all rows) and the record drawer (one row), so both spell out the same reasons.
SUSPECT_SQL = """
select s.id as system_id, p.full_name, s.solis_plant_name,
       coalesce(p.odoo_customer_name, s.odoo_lead_name) as odoo_name,
       u.email as login_email, s.solis_user_email,
       coalesce(p.odoo_email, s.odoo_lead_email) as odoo_email,
       s.solis_station_id, s.odoo_lead_id, s.mapping_verified_at,
       (select count(*) from public.energy_readings r where r.system_id = s.id) as readings
  from public.solar_systems s
  left join public.user_profiles p on p.id = s.user_id
  left join auth.users u on u.id = s.user_id
 where s.solis_station_id is not null and s.solis_plant_name is not null
"""


def _wrong_station_signals(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The 'possible wrong station' heuristic for ONE row.

    None when the Solis plant name shares a word with the customer's name (not
    flagged). Otherwise the list of signals an engineer reads to decide, and a
    score = how many of them point at a wrong station id rather than at a wrong
    profile name. Two very different causes look alike here, so every signal is
    reported and the engineer decides — "Check data vs Solis" and the emails
    usually settle it:
      * the Odoo lead carries another plant's station id  → remap + tell sales
      * the customer's PROFILE name is wrong (nickname, email as name)
        while the mapping is right                          → edit the profile
      * the mapping is right and the plant is simply named after a business,
        a church, a relative or a lot number (28 of 30 on 2026-09-19) — the
        heuristic can never clear these; the engineer ticks "manually
        verified" (mapping_verified_at) and the row leaves the scan.
    """
    nt, pt, ot = _name_tokens(r["full_name"]), _name_tokens(r["solis_plant_name"]), _name_tokens(r["odoo_name"])
    if not (nt and pt) or (nt & pt):
        return None
    signals = ["Solis plant name shares no word with the customer's name"]
    score = 1
    if ot and not (nt & ot):
        signals.append("Odoo name also differs from the customer's name"); score += 1
    le, se = (r["login_email"] or "").lower(), (r["solis_user_email"] or "").lower()
    if se and le and se != le:
        signals.append("Solis plant email differs from the login email"); score += 1
    elif se and le and se == le:
        signals.append("Solis plant email MATCHES the login email → mapping is probably right; the profile name is the odd one")
    if pt and ot and (pt & ot):
        signals.append("plant name matches the ODOO name → the profile name is probably what's wrong")
    # Household: the plant carries a relative's name, and that name is in
    # the customer's own login address (genpastrana_delacruz@ ↔ "Helen Pastrana").
    if le and any(w in le.split("@")[0] for w in pt if len(w) > 3):
        signals.append("the login email contains a word of the plant name → same household, mapping is probably right")
    # Organisation: a church, shop or company plant named after the entity,
    # with the customer as its contact person.
    if re.search(r"\b(ministr|church|grocery|store|shop|inc|corp|co\b|school|clinic|hoa|homes|depot|station|resort|farm)",
                 (r["solis_plant_name"] or "").lower()):
        signals.append("plant name looks like an organisation → the customer is probably its contact person")
    return {"score": score, "signals": signals}


@router.get("/api/unresolved")
async def unresolved(authorization: str = Header(None)):
    """Everything the nightly pipeline cannot fix by itself. Odoo-side items are
    reported for sales to act on; nothing here writes to Odoo."""
    await _authenticate_staff(authorization)
    with db.connect(autocommit=True) as conn:
        no_system = conn.execute(
            """select p.id as user_id, p.full_name, p.solis_station_id, u.email, p.created_at
                 from public.user_profiles p left join auth.users u on u.id = p.id
                where not exists (select 1 from public.solar_systems s where s.user_id = p.id)
                  -- a view-only login (system_access) owns nothing by design
                  and not exists (select 1 from public.system_access a where a.user_id = p.id)
                order by p.created_at desc""").fetchall()
        no_station = conn.execute(
            """select s.id as system_id, s.system_name, s.capacity_kwp, p.full_name, u.email, s.created_at,
                      (select max("timestamp") from public.energy_readings r where r.system_id = s.id) as last_daily
                 from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                 left join auth.users u on u.id = s.user_id
                where s.solis_station_id is null order by s.created_at""").fetchall()
        dark = conn.execute(
            """select s.id as system_id, s.system_name, s.solis_station_id, p.full_name, s.status,
                      r.last_daily, (current_date - r.last_daily::date) as days_dark
                 from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                 left join lateral (select max("timestamp") as last_daily
                                      from public.energy_readings r where r.system_id = s.id) r on true
                where s.status = 'active' and s.solis_station_id is not null
                  and (r.last_daily is null or r.last_daily < now() - interval '3 days')
                order by r.last_daily nulls first""").fetchall()
        run = conn.execute(
            "select ran_at, candidates, created, failed, duplicate_email, missing_email, duplicate_station, report "
            "from public.onboarding_runs order by ran_at desc limit 1").fetchone()
        suspects_raw = conn.execute(SUSPECT_SQL + " and s.mapping_verified_at is null").fetchall()
        # The register of mappings an engineer has ticked "manually verified".
        # Returned separately so the page can show them collapsed and a tick
        # made in error can be undone from the same place.
        verified_raw = conn.execute(
            """select s.id as system_id, p.full_name, s.solis_plant_name, s.solis_station_id,
                      coalesce(p.odoo_customer_name, s.odoo_lead_name) as odoo_name,
                      u.email as login_email, s.solis_user_email,
                      s.mapping_verified_at, s.mapping_verified_note,
                      v.email as mapping_verified_by_email
                 from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                 left join auth.users u on u.id = s.user_id
                 left join public.staff_users v on v.user_id = s.mapping_verified_by
                where s.mapping_verified_at is not null
                order by s.mapping_verified_at desc""").fetchall()
    onboarding: Dict[str, Any] = {}
    if run:
        report = run["report"] if isinstance(run["report"], dict) else json.loads(run["report"] or "{}")
        results = report.get("results") or []
        onboarding = {
            "ran_at": run["ran_at"], "candidates": run["candidates"], "created": run["created"],
            "failed": run["failed"],
            "failures": [{k: r.get(k) for k in ("full_name", "email", "station_id", "error")}
                         for r in results if r.get("status") == "failed"],
            "second_station_candidates": [
                {k: r.get(k) for k in ("full_name", "email", "station_id", "existing_station_id")}
                for r in results if r.get("status") == "skipped_would_repoint"],
        }
    # Possible wrong station: the Solis plant name shares no word with the
    # customer's name. Two very different causes look alike here, so every
    # signal is reported and the engineer decides — "Check data vs Solis" and
    # the emails usually settle it:
    #   * the Odoo lead carries another plant's station id  → remap + tell sales
    #   * the customer's PROFILE name is wrong (nickname, email as name)
    #     while the mapping is right                          → edit the profile
    #   * the mapping is right and the plant is simply named after a business,
    #     a church, a relative or a lot number (28 of 30 on 2026-09-19) — the
    #     heuristic can never clear these; the engineer ticks "manually
    #     verified" (mapping_verified_at) and the row leaves this scan.
    suspects = []
    for r in suspects_raw:
        verdict = _wrong_station_signals(r)
        if verdict:
            suspects.append({**dict(r), **verdict, "suggested_stations": []})
    suspects.sort(key=lambda x: (-x["score"], -(x["readings"] or 0)))

    # For each verified mapping, say whether the name heuristic would still
    # flag it today — a tick on a row whose names now agree is just history.
    verified = []
    for r in verified_raw:
        nt, pt = _name_tokens(r["full_name"]), _name_tokens(r["solis_plant_name"])
        verified.append({**dict(r), "still_flagged_by_names": bool(nt and pt and not (nt & pt))})

    # Suggested correct plants come from a separate endpoint
    # (/api/suspects/suggestions): they need the Solis roster, whose cold walk
    # takes ~90 s and fails outright when Solis is having a bad hour. This page
    # must never wait on that.

    return _j({
        "profiles_without_system": no_system,
        "systems_without_station": no_station,
        "dark_systems": dark,
        "last_onboarding": onboarding,
        "suspect_mappings": suspects,
        "verified_mappings": verified,
    })


@router.get("/api/suspects/suggestions")
async def suspect_suggestions(authorization: str = Header(None)):
    """For every 'possible wrong station' suspect, the unassigned plants in our
    Solis roster that are named after the customer — the likely correct id,
    offered for the engineer to validate, never applied.

    Separate from /api/unresolved on purpose: this needs the fleet roster, whose
    cold walk takes ~90 s and fails when Solis is unwell (502s), and the page
    must render without it. The UI fills the column asynchronously and offers
    Retry on 503."""
    await _authenticate_staff(authorization)
    roster, _age, stale = await _get_roster()
    if roster is None:
        raise HTTPException(503, "Solis roster unavailable right now — retry in a minute")
    with db.connect(autocommit=True) as conn:
        rows = conn.execute(
            """select s.id as system_id, p.full_name, s.solis_plant_name,
                      coalesce(p.odoo_customer_name, s.odoo_lead_name) as odoo_name, u.email as login_email
                 from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                 left join auth.users u on u.id = s.user_id
                where s.solis_station_id is not null and s.solis_plant_name is not null""").fetchall()
        assigned = {str(x["solis_station_id"]) for x in conn.execute(
            "select solis_station_id from public.solar_systems where solis_station_id is not null").fetchall()}
    unassigned = [(str(sid), rec) for sid, rec in roster.items() if str(sid) not in assigned]
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        nt, pt0 = _name_tokens(r["full_name"]), _name_tokens(r["solis_plant_name"])
        if not (nt and pt0) or (nt & pt0):
            continue  # same suspect rule as /api/unresolved
        nt |= _name_tokens(r["odoo_name"])
        hits = []
        for sid, rec in unassigned:
            pt = _name_tokens(rec.get("stationName"))
            if len(nt & pt) >= (2 if len(pt) > 1 else 1):
                em = (rec.get("userEmail") or "").lower()
                hits.append({
                    "station_id": sid, "plant_name": rec.get("stationName"),
                    "capacity_kwp": float(rec.get("capacity") or 0) or None, "solis_email": rec.get("userEmail"),
                    "email_matches_login": bool(em and r["login_email"] and em == r["login_email"].lower()),
                })
        if hits:
            out[str(r["system_id"])] = sorted(hits, key=lambda h: (not h["email_matches_login"], h["plant_name"] or ""))
    return {"suggestions": out, "unassigned_in_roster": len(unassigned), "roster_stale": stale}


# ---------------------------------------------------------------------------
# on-demand refresh of our Odoo / Solis copy for ONE record
# ---------------------------------------------------------------------------
#
# The nightly jobs (sync_identity_mirror, sync_to_supabase) refresh every row
# once a day. These two endpoints do the same for one record, now, so a fix
# made in Odoo or in Solis Cloud shows here in seconds. They READ the source
# and WRITE our copy — never the other way round — and the 08 trigger attributes
# every changed field to the engineer who pressed the button.

def _clean_odoo(v: Any) -> Optional[str]:
    if v in (None, False, ""):
        return None
    s = str(v).strip()
    return s or None


def _diff(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """{field: {old, new}} for the fields whose value actually changes."""
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in new.items():
        o = old.get(k)
        if o in (None, "") and v in (None, ""):
            continue
        if _j(o) != _j(v):
            out[k] = {"old": _j(o), "new": _j(v)}
    return out


@router.post("/api/systems/{system_id}/refresh-odoo")
async def refresh_from_odoo(system_id: uuid.UUID, authorization: str = Header(None)):
    """Re-read the Odoo lead that carries this station id (and its contact) and
    update our odoo_* copy — the nightly mirror's logic for one row. Newest lead
    wins when two leads carry the id, as in the mirror; the response says so."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    with db.connect(autocommit=True) as conn:
        s = conn.execute(
            """select s.id, s.user_id, s.is_primary, s.solis_station_id,
                      s.odoo_lead_id, s.odoo_lead_email, s.odoo_lead_name, s.odoo_stage,
                      p.odoo_partner_id, p.odoo_email, p.odoo_customer_name
                 from public.solar_systems s left join public.user_profiles p on p.id = s.user_id
                where s.id = %s""", (system_id,)).fetchone()
    if not s:
        raise HTTPException(404, "No such system")
    sid = s["solis_station_id"]
    if not sid:
        raise HTTPException(400, "This system has no Solis station id; the Odoo lead is found through the station id on the lead")
    try:
        leads = await _odoo_search_read(
            "crm.lead", [[DEFAULT_FIELD_NAME, "=", sid]],
            ["id", "name", "partner_name", "contact_name", "email_from", "stage_id", "partner_id"])
    except Exception as exc:
        raise HTTPException(502, f"Odoo did not answer: {str(exc)[:160]}")
    if not leads:
        return {"ok": True, "found": False, "changed": {},
                "message": f"No lead in Odoo carries station id {sid}. Our copy was left as it was; "
                           f"if the id was removed from the lead on purpose, sales should say which lead is right."}
    lead = max(leads, key=lambda l: l["id"])
    partner = None
    if lead.get("partner_id"):
        try:
            found = await _odoo_search_read("res.partner", [["id", "=", lead["partner_id"][0]]], ["id", "name", "email"])
        except Exception as exc:
            raise HTTPException(502, f"Odoo did not answer for the contact: {str(exc)[:160]}")
        partner = found[0] if found else None

    new_sys = {
        "odoo_lead_id": lead["id"],
        "odoo_lead_email": _clean_odoo(lead.get("email_from")),
        "odoo_lead_name": _clean_odoo(lead.get("partner_name")) or _clean_odoo(lead.get("contact_name")) or _clean_odoo(lead.get("name")),
        "odoo_stage": _clean_odoo(lead["stage_id"][1]) if lead.get("stage_id") else None,
    }
    new_prof = None
    if s["is_primary"] and s["user_id"]:
        new_prof = {
            "odoo_partner_id": partner["id"] if partner else None,
            "odoo_email": (_clean_odoo(partner.get("email")) if partner else None) or new_sys["odoo_lead_email"],
            "odoo_customer_name": (_clean_odoo(partner.get("name")) if partner else None) or new_sys["odoo_lead_name"],
        }
    changed = _diff(dict(s), new_sys)
    if new_prof:
        changed.update(_diff(dict(s), new_prof))

    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], "Refresh from Odoo now", request_id=request_id) as conn:
            conn.execute(
                """update public.solar_systems
                      set odoo_lead_id = %s, odoo_lead_email = %s, odoo_lead_name = %s, odoo_stage = %s,
                          odoo_synced_at = now(), updated_at = now()
                    where id = %s""",
                (new_sys["odoo_lead_id"], new_sys["odoo_lead_email"], new_sys["odoo_lead_name"], new_sys["odoo_stage"], system_id))
            if new_prof:
                conn.execute(
                    """update public.user_profiles
                          set odoo_partner_id = %s, odoo_email = %s, odoo_customer_name = %s,
                              odoo_synced_at = now(), updated_at = now()
                        where id = %s""",
                    (new_prof["odoo_partner_id"], new_prof["odoo_email"], new_prof["odoo_customer_name"], s["user_id"]))
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "found": True, "request_id": request_id, "changed": changed,
            "values": {**new_sys, **(new_prof or {})}, "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "collision": len(leads) > 1, "lead_ids": sorted(l["id"] for l in leads),
            "profile_updated": bool(new_prof)}


@router.post("/api/systems/{system_id}/refresh-solis")
async def refresh_from_solis(system_id: uuid.UUID, authorization: str = Header(None)):
    """Re-read the plant from Solis Cloud (stationDetail, plus the inverter's
    battery capacity) and update our solis_* / system_name / capacity copy —
    what the nightly daily sync writes, for one row."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    with db.connect(autocommit=True) as conn:
        s = conn.execute(
            """select id, solis_station_id, solis_plant_name, solis_user_email, system_name,
                      capacity_kwp, battery_capacity_kwh
                 from public.solar_systems where id = %s""", (system_id,)).fetchone()
    if not s:
        raise HTTPException(404, "No such system")
    sid = s["solis_station_id"]
    if not sid:
        raise HTTPException(400, "This system has no Solis station id to read")
    key_id, key_secret = os.getenv("SOLIS_CLOUD_KEY_ID", ""), os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
    if not key_id or not key_secret:
        raise HTTPException(500, "Solis credentials are not configured on the server")
    solis = SolisCloudClient(key_id, key_secret)
    try:
        detail = await solis.station_detail(sid)
    except Exception as exc:
        raise HTTPException(502, f"Solis did not answer: {str(exc)[:160]}")
    if not detail:
        raise HTTPException(502, "Solis returned no detail for this station id — that is either a communication error "
                                 "or a station that does not exist, and Solis cannot tell them apart. Nothing was changed; "
                                 "Validate Solis station checks the fleet roster instead.")
    from .sync_to_supabase import _fetch_battery_capacity_kwh  # best-effort, same as the nightly sync
    battery = await _fetch_battery_capacity_kwh(solis, sid)

    name = _clean_odoo(detail.get("stationName"))
    capacity = float(detail.get("capacity") or 0) or None
    new = {
        "solis_plant_name": name or s["solis_plant_name"],
        "system_name": name or s["system_name"],              # the daily sync keeps these two equal
        "solis_user_email": _clean_odoo(detail.get("userEmail")) or s["solis_user_email"],  # never blank a non-null (04a)
        "capacity_kwp": capacity if capacity is not None else s["capacity_kwp"],
        "battery_capacity_kwh": battery if battery is not None else s["battery_capacity_kwh"],
    }
    changed = _diff(dict(s), new)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], "Refresh from Solis now", request_id=request_id) as conn:
            conn.execute(
                """update public.solar_systems
                      set solis_plant_name = %s, system_name = %s, solis_user_email = %s,
                          capacity_kwp = %s, battery_capacity_kwh = %s,
                          solis_synced_at = now(), updated_at = now()
                    where id = %s""",
                (new["solis_plant_name"], new["system_name"], new["solis_user_email"],
                 new["capacity_kwp"], new["battery_capacity_kwh"], system_id))
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, "changed": changed, "values": _j(new),
            "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "solis": {"stationName": detail.get("stationName"), "capacity": detail.get("capacity"),
                      "userEmail": detail.get("userEmail"), "state": detail.get("state")}}


# ---------------------------------------------------------------------------
# view-only access grants: several logins, one station (migration 16)
# ---------------------------------------------------------------------------
#
# An SME owner hands the portal to a staff member with their own email. The
# station keeps ONE owner (solar_systems.user_id — syncs, backfills and the Odoo
# mapping follow it); a row in public.system_access lets a second login READ
# the station and its readings through the same RLS helper the owner uses.
# Grants are made here with a reason, audited by the 08 trigger, and revoked by
# deleting the row. A grantee never gets the owner's phone or address.

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class GrantAccessRequest(BaseModel):
    email: str
    full_name: Optional[str] = None
    reason: str


class RevokeAccessRequest(BaseModel):
    reason: str
    # Ban the login when the revoke leaves it with nothing to look at (no station
    # of its own, no other grant, not staff). Off by default: the person may be
    # granted another station a minute later.
    disable_login: bool = False


async def _create_login_with_temp_password(email: str, full_name: Optional[str]) -> tuple[str, str]:
    """Create a NEW auth user with a temporary password, returned once. Unlike
    _create_or_reset_login this never touches an existing login: the caller has
    already checked auth.users, and resetting a customer's password because an
    engineer typed their address into the grant box would be a nasty surprise."""
    import secrets
    env = _supabase_env()
    hdr = {"apikey": env["service"], "Authorization": f"Bearer {env['service']}"}
    temp = secrets.token_urlsafe(12)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(env["url"] + "/auth/v1/admin/users", headers=hdr,
                              json={"email": email, "password": temp, "email_confirm": True,
                                    "user_metadata": {"full_name": full_name or ""}})
    if r.status_code in (200, 201):
        return r.json()["id"], temp
    if r.status_code == 422:
        raise HTTPException(409, f"A login for {email} appeared while granting — retry")
    raise HTTPException(502, f"Could not create a login for {email}: {r.text[:160]}")


@router.get("/api/systems/{system_id}/access")
async def list_access(system_id: uuid.UUID, authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    with db.connect(autocommit=True) as conn:
        owner = conn.execute(
            """select s.user_id, u.email, p.full_name, u.last_sign_in_at
                 from public.solar_systems s
                 left join auth.users u on u.id = s.user_id
                 left join public.user_profiles p on p.id = s.user_id
                where s.id = %s""", (system_id,)).fetchone()
        if not owner:
            raise HTTPException(404, "No such system")
        viewers = conn.execute(
            """select a.id as grant_id, a.user_id, u.email, p.full_name, u.last_sign_in_at,
                      (u.banned_until > now()) as banned,
                      a.granted_at, a.reason, g.email as granted_by_email,
                      exists (select 1 from public.solar_systems o where o.user_id = a.user_id) as owns_a_station
                 from public.system_access a
                 join auth.users u on u.id = a.user_id
                 left join public.user_profiles p on p.id = a.user_id
                 left join public.staff_users g on g.user_id = a.granted_by
                where a.system_id = %s
                order by a.granted_at""", (system_id,)).fetchall()
    return {"owner": _j(dict(owner)), "viewers": _j(viewers)}


@router.post("/api/systems/{system_id}/access")
async def grant_access(system_id: uuid.UUID, body: GrantAccessRequest, authorization: str = Header(None)):
    """Let `email` view this station. Creates the login if none exists (temporary
    password returned ONCE, never stored) and a minimal profile row so the
    portal has a name to show. Never resets an existing login's password."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    email = (body.email or "").strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(400, "Enter a valid email address")

    with db.connect(autocommit=True) as conn:
        system = conn.execute(
            """select s.id, s.user_id, s.system_name, s.solis_station_id, u.email as owner_email
                 from public.solar_systems s left join auth.users u on u.id = s.user_id
                where s.id = %s""", (system_id,)).fetchone()
        if not system:
            raise HTTPException(404, "No such system")
        if (system["owner_email"] or "").lower() == email:
            raise HTTPException(409, f"{email} already owns this station")
        login = conn.execute(
            "select id, (banned_until > now()) as banned from auth.users where lower(email) = %s", (email,)).fetchone()
        if login and conn.execute(
                "select 1 from public.system_access where system_id = %s and user_id = %s",
                (system_id, login["id"])).fetchone():
            raise HTTPException(409, f"{email} already has view access to this station")
        if login and login["banned"]:
            raise HTTPException(409, f"The login {email} is banned (a retired duplicate?) — pick another address or restore it first")

    created, temp, user_id = False, None, (str(login["id"]) if login else None)
    if not user_id:
        user_id, temp = await _create_login_with_temp_password(email, body.full_name)
        created = True

    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            # A profile row (full_name is NOT NULL) so the portal can greet the
            # person. No station on it: the grant is the only link, on purpose.
            conn.execute(
                """insert into public.user_profiles (id, full_name)
                   values (%s, %s) on conflict (id) do nothing""",
                (user_id, (body.full_name or "").strip() or email))
            conn.execute(
                """insert into public.system_access (system_id, user_id, role, granted_by, reason)
                   values (%s, %s, 'viewer', %s, %s)""",
                (system_id, user_id, staff["id"], reason))
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, "system_id": str(system_id),
            "station_id": system["solis_station_id"], "user_id": user_id, "email": email,
            "login_created": created, "temporary_password": temp}


@router.post("/api/systems/{system_id}/access/{user_id}/revoke")
async def revoke_access(system_id: uuid.UUID, user_id: uuid.UUID, body: RevokeAccessRequest,
                        authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    request_id = str(uuid.uuid4())
    try:
        with db.audited(staff["id"], staff["email"], reason, request_id=request_id) as conn:
            gone = conn.execute(
                "delete from public.system_access where system_id = %s and user_id = %s returning id",
                (system_id, user_id)).fetchone()
            if not gone:
                raise HTTPException(404, "That login has no grant on this station")
            still = conn.execute(
                """select exists (select 1 from public.solar_systems where user_id = %s) as owns,
                          exists (select 1 from public.system_access where user_id = %s) as other_grants,
                          exists (select 1 from public.staff_users where user_id = %s) as is_staff,
                          (select email from auth.users where id = %s) as email""",
                (user_id, user_id, user_id, user_id)).fetchone()
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise _db_error(exc)

    stranded = not (still["owns"] or still["other_grants"] or still["is_staff"])
    disabled = False
    if body.disable_login and stranded:
        env = _supabase_env()
        hdr = {"apikey": env["service"], "Authorization": f"Bearer {env['service']}"}
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.put(env["url"] + f"/auth/v1/admin/users/{user_id}", headers=hdr,
                                 json={"ban_duration": "876000h"})
        disabled = r.status_code == 200
        if not disabled:
            log.warning("revoke: grant removed but ban of %s failed: %s %s", still["email"], r.status_code, r.text[:160])
    _invalidate_rows()
    return {"ok": True, "request_id": request_id, "email": still["email"],
            "login_still_has_access": not stranded, "login_disabled": disabled}


# ---------------------------------------------------------------------------
# staff directory (admin)
# ---------------------------------------------------------------------------

class StaffCreate(BaseModel):
    email: str
    role: str = Field("readonly", pattern="^(readonly|engineer|admin)$")
    full_name: Optional[str] = None
    # "invite": Supabase emails a link (lands on the project's Site URL — set it
    # to the Monitoring Admin page or the link points at localhost:3000).
    # "password": create the login now with a temporary password, returned ONCE
    # in the response and never stored by us; the person changes it after the
    # first sign-in. Fits the development phase, where the team signs in as
    # several users; no email round trip.
    mode: str = Field("invite", pattern="^(invite|password)$")


async def _create_or_reset_login(email: str, full_name: Optional[str]) -> tuple[str, str]:
    """Create the auth user with a fresh temporary password, or set one on an
    existing user. Returns (user_id, temporary_password)."""
    import secrets
    env = _supabase_env()
    hdr = {"apikey": env["service"], "Authorization": f"Bearer {env['service']}"}
    temp = secrets.token_urlsafe(12)
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(env["url"] + "/auth/v1/admin/users", headers=hdr,
                              json={"email": email, "password": temp, "email_confirm": True,
                                    "user_metadata": {"full_name": full_name or ""}})
        if r.status_code in (200, 201):
            return r.json()["id"], temp
        # already registered → find and reset
        page = 1
        while page < 20:
            lst = await client.get(env["url"] + "/auth/v1/admin/users", headers=hdr,
                                   params={"page": page, "per_page": 1000})
            users = lst.json().get("users", []) if lst.status_code == 200 else []
            for u in users:
                if (u.get("email") or "").lower() == email:
                    upd = await client.put(env["url"] + f"/auth/v1/admin/users/{u['id']}", headers=hdr,
                                           json={"password": temp, "email_confirm": True, "ban_duration": "none"})
                    if upd.status_code != 200:
                        raise HTTPException(502, f"Could not set a password for {email}: {upd.text[:160]}")
                    return u["id"], temp
            if len(users) < 1000:
                break
            page += 1
    raise HTTPException(502, f"Could not create a login for {email}: {r.text[:160]}")


class StaffPatch(BaseModel):
    role: Optional[str] = Field(None, pattern="^(readonly|engineer|admin)$")
    active: Optional[bool] = None


@router.get("/api/staff")
async def staff_list(authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "admin")
    with db.connect(autocommit=True) as conn:
        data = conn.execute(
            """select s.*, u.last_sign_in_at, (u.raw_app_meta_data->'providers') as providers
                 from public.staff_users s left join auth.users u on u.id = s.user_id
                order by s.created_at""").fetchall()
    return {"staff": _j(data)}


async def _find_or_invite_login(email: str, full_name: Optional[str], redirect_to: str) -> str:
    """Return the auth.users id for `email`, inviting (email sent by Supabase) if new."""
    env = _supabase_env()
    hdr = {"apikey": env["service"], "Authorization": f"Bearer {env['service']}"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(env["url"] + "/auth/v1/invite", headers=hdr,
                              params={"redirect_to": redirect_to},
                              json={"email": email, "data": {"full_name": full_name or ""}})
        if r.status_code in (200, 201):
            return r.json()["id"]
        # already registered → find the id
        page = 1
        while page < 20:
            lst = await client.get(env["url"] + "/auth/v1/admin/users", headers=hdr,
                                   params={"page": page, "per_page": 1000})
            users = lst.json().get("users", []) if lst.status_code == 200 else []
            for u in users:
                if (u.get("email") or "").lower() == email:
                    return u["id"]
            if len(users) < 1000:
                break
            page += 1
    raise HTTPException(502, f"Could not create or find a login for {email}: {r.text[:160]}")


@router.post("/api/staff")
async def staff_create(body: StaffCreate, request: Request, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "admin")
    email = body.email.strip().lower()
    if "@" not in email:
        raise HTTPException(400, "Invalid email")
    temporary_password = None
    if body.mode == "password":
        user_id, temporary_password = await _create_or_reset_login(email, body.full_name)
    else:
        redirect_to = str(request.base_url)   # the page is served at the host root
        user_id = await _find_or_invite_login(email, body.full_name, redirect_to)
    try:
        with db.audited(staff["id"], staff["email"], f"staff added by {staff['email']} ({body.mode})") as conn:
            conn.execute(
                """insert into public.staff_users (user_id, email, role, active, created_by)
                   values (%s, %s, %s, true, %s)
                   on conflict (user_id) do update
                     set role = excluded.role, active = true, revoked_at = null, updated_at = now()""",
                (user_id, email, body.role, staff["id"]))
    except psycopg.Error as exc:
        raise _db_error(exc)
    return {"ok": True, "user_id": user_id, "email": email, "role": body.role, "mode": body.mode,
            "temporary_password": temporary_password}


@router.delete("/api/staff/{user_id}")
async def staff_delete(user_id: uuid.UUID, delete_login: bool = True, authorization: str = Header(None)):
    """Remove a staff member entirely — the staff_users row and, by default, the
    Supabase login — so a fresh invite can be sent. Refuses to delete yourself,
    and refuses any login that owns customer data (a profile or a station):
    those are customers, and customer identity is never deleted from here."""
    staff = await _authenticate_staff(authorization)
    _require(staff, "admin")
    if str(user_id) == staff["id"]:
        raise HTTPException(400, "You cannot delete yourself")
    with db.connect(autocommit=True) as conn:
        row = conn.execute("select email from public.staff_users where user_id = %s", (user_id,)).fetchone()
        if not row:
            raise HTTPException(404, "No such staff user")
        owns = conn.execute(
            """select (select count(*) from public.user_profiles where id = %s)
                    + (select count(*) from public.solar_systems where user_id = %s) as n""",
            (user_id, user_id)).fetchone()["n"]
    if owns and delete_login:
        raise HTTPException(400, f"{row['email']} owns customer data (profile/stations); remove the staff role only, not the login")
    try:
        with db.audited(staff["id"], staff["email"], f"staff removed by {staff['email']}"
                        + (" (login deleted)" if delete_login else "")) as conn:
            conn.execute("delete from public.staff_users where user_id = %s", (user_id,))
    except psycopg.Error as exc:
        raise _db_error(exc)
    login_deleted = False
    if delete_login:
        env = _supabase_env()
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.delete(env["url"] + f"/auth/v1/admin/users/{user_id}",
                                    headers={"apikey": env["service"], "Authorization": f"Bearer {env['service']}"})
        login_deleted = r.status_code in (200, 204)
        if not login_deleted:
            log.warning("staff row removed but login delete failed for %s: %s %s", row["email"], r.status_code, r.text[:160])
    return {"ok": True, "email": row["email"], "login_deleted": login_deleted}


@router.patch("/api/staff/{user_id}")
async def staff_patch(user_id: uuid.UUID, body: StaffPatch, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "admin")
    if str(user_id) == staff["id"] and body.active is False:
        raise HTTPException(400, "You cannot deactivate yourself")
    sets, params = [], []
    if body.role:
        sets.append("role = %s"); params.append(body.role)
    if body.active is not None:
        sets.append("active = %s"); params.append(body.active)
        sets.append("revoked_at = %s"); params.append(None if body.active else dt.datetime.now(dt.timezone.utc))
    if not sets:
        raise HTTPException(400, "Nothing to change")
    try:
        with db.audited(staff["id"], staff["email"], f"staff changed by {staff['email']}") as conn:
            row = conn.execute(
                f"update public.staff_users set {', '.join(sets)} where user_id = %s returning user_id",
                (*params, user_id)).fetchone()
    except psycopg.Error as exc:
        raise _db_error(exc)
    if not row:
        raise HTTPException(404, "No such staff user")
    return {"ok": True}
