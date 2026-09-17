"""
Back office — the engineering team's view of customer identity across
Supabase, Solis Cloud and Odoo, and the only sanctioned way to change it.

    GET  /backoffice                       the single-page UI
    GET  /backoffice/api/config            Supabase URL + anon key for the UI's login
    GET  /backoffice/api/me                who am I, what role
    GET  /backoffice/api/rows              the grid: one row per station
    GET  /backoffice/api/rows/{system_id}  one row + its audit trail + jobs
    PATCH /backoffice/api/systems/{id}     edit a station (engineer+)
    PATCH /backoffice/api/profiles/{id}    edit a customer profile (engineer+)
    POST /backoffice/api/systems           attach another station to a customer (engineer+)
    POST /backoffice/api/backfill          enqueue a backfill (engineer+)
    GET  /backoffice/api/jobs              backfill queue
    POST /backoffice/api/jobs/{id}/cancel  cancel a queued job (engineer+)
    GET  /backoffice/api/audit             who changed what
    GET  /backoffice/api/unresolved        the work queue: gaps the nightly pipeline cannot close
    GET/POST/PATCH /backoffice/api/staff   staff directory (admin)

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
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import psycopg
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import db
from .solis_client import SolisCloudClient
from .validation_routes import _authenticate_staff

log = logging.getLogger(__name__)
router = APIRouter(prefix="/backoffice", tags=["Back Office"])

INDEX_HTML = Path(__file__).parent / "backoffice" / "index.html"
STATION_ID_RE = re.compile(r"^\d{15,20}$")
ROLE_RANK = {"readonly": 0, "engineer": 1, "admin": 2}

SYSTEM_FIELDS = {
    "solis_station_id", "system_name", "capacity_kwp", "installation_date", "status",
    "address", "battery_capacity_kwh", "is_primary",
    "odoo_lead_id", "odoo_lead_email", "odoo_lead_name", "odoo_stage", "solis_plant_name",
}
PROFILE_FIELDS = {
    "full_name", "phone", "address", "odoo_partner_id", "odoo_email",
    "odoo_customer_name", "electricity_provider_id",
}


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
    log.exception("back office database error")
    return HTTPException(500, "Database error")


def _reason(reason: Optional[str]) -> str:
    r = (reason or "").strip()
    if len(r) < 3:
        raise HTTPException(400, "A reason is required (what changed and why).")
    return r[:500]


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

@router.get("", include_in_schema=False)
@router.get("/", include_in_schema=False)
async def backoffice_index():
    if not INDEX_HTML.exists():
        raise HTTPException(404, "Back office UI not built")
    return FileResponse(INDEX_HTML, media_type="text/html", headers={"Cache-Control": "no-store"})


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
    return await _authenticate_staff(authorization)


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
       s.status, s.is_primary, s.address,
       s.solis_station_id, s.solis_plant_name, s.solis_user_email, s.solis_validated_at, s.solis_validation,
       s.odoo_lead_id, s.odoo_lead_email, s.odoo_lead_name, s.odoo_stage,
       s.last_backfill_at, s.last_backfill_status, s.created_at,
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
"""


@router.get("/api/rows")
async def rows(authorization: str = Header(None)):
    await _authenticate_staff(authorization)
    try:
        with db.connect(autocommit=True) as conn:
            data = conn.execute(ROWS_SQL + " order by coalesce(p.full_name, s.system_name)").fetchall()
    except RuntimeError as exc:
        raise HTTPException(503, str(exc))
    return {"rows": _j(data), "count": len(data)}


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
    return {"row": _j(row), "other_systems": _j(siblings), "audit": _j(audit), "jobs": _j(jobs)}


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


@router.patch("/api/systems/{system_id}")
async def edit_system(system_id: uuid.UUID, body: EditRequest, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)

    fields = {k: v for k, v in body.fields.items() if k in SYSTEM_FIELDS}
    unknown = set(body.fields) - SYSTEM_FIELDS
    if unknown:
        raise HTTPException(400, f"Not editable here: {sorted(unknown)}")
    if not fields and not body.backfill:
        raise HTTPException(400, "Nothing to change")

    sid = fields.get("solis_station_id")
    if sid is not None:
        sid = str(sid).strip() or None
        if sid and not STATION_ID_RE.match(sid):
            raise HTTPException(400, "Solis station id must be 15–20 digits")
        fields["solis_station_id"] = sid
    for k in ("odoo_lead_id",):
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
    return {"ok": True, "request_id": request_id, "changed": sorted(fields), "backfill_job": job}


@router.patch("/api/profiles/{user_id}")
async def edit_profile(user_id: uuid.UUID, body: EditRequest, authorization: str = Header(None)):
    staff = await _authenticate_staff(authorization)
    _require(staff, "engineer")
    reason = _reason(body.reason)
    fields = {k: v for k, v in body.fields.items() if k in PROFILE_FIELDS}
    unknown = set(body.fields) - PROFILE_FIELDS
    if unknown:
        raise HTTPException(400, f"Not editable here: {sorted(unknown)} (the station id lives on the system)")
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
    return {"ok": True, "system_id": str(row["id"]), "request_id": request_id, "backfill_job": job}


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
        with db.audited(staff["id"], staff["email"], "cancelled from back office") as conn:
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
    return _j({
        "profiles_without_system": no_system,
        "systems_without_station": no_station,
        "dark_systems": dark,
        "last_onboarding": onboarding,
    })


# ---------------------------------------------------------------------------
# staff directory (admin)
# ---------------------------------------------------------------------------

class StaffCreate(BaseModel):
    email: str
    role: str = Field("readonly", pattern="^(readonly|engineer|admin)$")
    full_name: Optional[str] = None


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
    redirect_to = str(request.base_url).rstrip("/") + "/backoffice"
    user_id = await _find_or_invite_login(email, body.full_name, redirect_to)
    try:
        with db.audited(staff["id"], staff["email"], f"staff added by {staff['email']}") as conn:
            conn.execute(
                """insert into public.staff_users (user_id, email, role, active, created_by)
                   values (%s, %s, %s, true, %s)
                   on conflict (user_id) do update
                     set role = excluded.role, active = true, revoked_at = null, updated_at = now()""",
                (user_id, email, body.role, staff["id"]))
    except psycopg.Error as exc:
        raise _db_error(exc)
    return {"ok": True, "user_id": user_id, "email": email, "role": body.role}


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
