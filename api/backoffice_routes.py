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
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import psycopg
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import db
from .backfill_history import PHT, parse_month_day
from .solis_client import SolisCloudClient
from .validation_routes import _authenticate_staff, _get_roster

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
    _invalidate_rows()
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
        suspects_raw = conn.execute(
            """select s.id as system_id, p.full_name, s.solis_plant_name,
                      coalesce(p.odoo_customer_name, s.odoo_lead_name) as odoo_name,
                      u.email as login_email, s.solis_user_email,
                      coalesce(p.odoo_email, s.odoo_lead_email) as odoo_email,
                      s.solis_station_id, s.odoo_lead_id,
                      (select count(*) from public.energy_readings r where r.system_id = s.id) as readings
                 from public.solar_systems s
                 left join public.user_profiles p on p.id = s.user_id
                 left join auth.users u on u.id = s.user_id
                where s.solis_station_id is not null and s.solis_plant_name is not null""").fetchall()
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
    suspects = []
    for r in suspects_raw:
        nt, pt, ot = _name_tokens(r["full_name"]), _name_tokens(r["solis_plant_name"]), _name_tokens(r["odoo_name"])
        if not (nt and pt) or (nt & pt):
            continue
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
        suspects.append({**dict(r), "score": score, "signals": signals, "suggested_stations": []})
    suspects.sort(key=lambda x: (-x["score"], -(x["readings"] or 0)))

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
# staff directory (admin)
# ---------------------------------------------------------------------------

class StaffCreate(BaseModel):
    email: str
    role: str = Field("readonly", pattern="^(readonly|engineer|admin)$")
    full_name: Optional[str] = None
    # "invite": Supabase emails a link (lands on the project's Site URL — set it
    # to the back office or the link points at localhost:3000).
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
        redirect_to = str(request.base_url).rstrip("/") + "/backoffice"
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
