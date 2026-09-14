"""
Auto-onboard Supabase users from Odoo.sh CRM leads that have a Solis station ID.

Any Odoo lead whose Solis-station-id field is populated becomes a Supabase
user (auth user + user_profiles.solis_station_id) so the daily and 5-minute
syncs start extracting its energy readings.

Trigger rule:
    Field presence only — every crm.lead with a non-empty station id field is a
    candidate, regardless of pipeline stage.

Email resolution:
    1. Odoo lead email_from
    2. Fallback: Solis station userEmail (from list_stations / station_detail)
    Leads with no email from either source are skipped and reported.

This module is reused as a pre-step by api.sync_to_supabase (runs with apply=True
before the daily sync) and can also be run standalone:

    python -m api.onboard_from_odoo            # dry-run (default)
    python -m api.onboard_from_odoo --apply    # create users / upsert profiles
    python -m api.onboard_from_odoo --field x_studio_crm_design_solis_station_id
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import smtplib
import xmlrpc.client
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Dict, List, Optional, Set, Tuple

import httpx
from supabase import Client

# Reuse the proven Supabase/Solis onboarding primitives.
from api.onboard_from_station_csv import (
    Candidate,
    build_solis,
    build_supabase,
    fetch_station_map,
    get_existing_station_ids,
    load_environment,
    run_onboarding,
)
from api.solis_client import SolisCloudClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("onboard_odoo")
logging.getLogger("httpx").setLevel(logging.WARNING)

DEFAULT_FIELD_NAME = "x_studio_crm_design_solis_station_id"
DEFAULT_REPORT_PATH = "onboarding_from_odoo_auto_report.json"


@dataclass
class OdooConfig:
    url: str
    db: str
    user: str
    auth: str


def _false_like(value) -> bool:
    return str(value if value is not None else "").strip().lower() in ("", "false", "0")


def _build_odoo_config() -> OdooConfig:
    """Prefer Odoo.sh (ODOO_SH_*) creds, fall back to the ODOO_* set used by the worker."""
    url = os.getenv("ODOO_SH_URL") or os.getenv("ODOO_URL") or ""
    db = os.getenv("ODOO_SH_DB") or os.getenv("ODOO_DB") or ""
    user = os.getenv("ODOO_SH_USER") or os.getenv("ODOO_USER") or ""
    auth = (
        os.getenv("ODOO_SH_API_KEY")
        or os.getenv("ODOO_API_KEY")
        or os.getenv("ODOO_PASSWORD")
        or ""
    )
    return OdooConfig(url=url.rstrip("/"), db=db, user=user, auth=auth)


def _connect_odoo(config: OdooConfig) -> Tuple[int, xmlrpc.client.ServerProxy]:
    if not all([config.url, config.db, config.user, config.auth]):
        raise RuntimeError(
            "Missing Odoo connection env vars "
            "(ODOO_SH_URL/DB/USER/API_KEY or ODOO_URL/DB/USER/API_KEY)."
        )
    common = xmlrpc.client.ServerProxy(f"{config.url}/xmlrpc/2/common")
    uid = common.authenticate(config.db, config.user, config.auth, {})
    if not uid:
        raise RuntimeError("Odoo authentication failed.")
    models = xmlrpc.client.ServerProxy(f"{config.url}/xmlrpc/2/object")
    log.info("Connected to Odoo at %s", config.url)
    return uid, models


def fetch_leads_with_station_id(field_name: str = DEFAULT_FIELD_NAME) -> List[Dict]:
    """Return crm.lead records whose Solis station-id field is populated."""
    config = _build_odoo_config()
    uid, models = _connect_odoo(config)

    lead_ids = models.execute_kw(
        config.db,
        uid,
        config.auth,
        "crm.lead",
        "search",
        [[[field_name, "!=", False]]],
    )
    log.info("Odoo leads with %s populated: %d", field_name, len(lead_ids))
    if not lead_ids:
        return []

    fields = [
        "id",
        "name",
        "partner_name",
        "contact_name",
        "email_from",
        "phone",
        field_name,
    ]

    leads: List[Dict] = []
    batch = 200
    for i in range(0, len(lead_ids), batch):
        chunk = lead_ids[i : i + batch]
        leads.extend(
            models.execute_kw(
                config.db, uid, config.auth, "crm.lead", "read", [chunk], {"fields": fields}
            )
        )
    return leads


def _lead_display_name(lead: Dict, fallback: str) -> str:
    for key in ("contact_name", "partner_name", "name"):
        value = lead.get(key)
        if value and not _false_like(value):
            return str(value).strip()
    return fallback


def _clean(value) -> str:
    return "" if _false_like(value) else str(value).strip()


def build_candidates(
    leads: List[Dict],
    existing_station_ids: Set[str],
    station_map: Dict[str, Dict[str, str]],
    field_name: str,
) -> Tuple[List[Candidate], Dict[str, List[Dict[str, str]]]]:
    skipped: Dict[str, List[Dict[str, str]]] = {
        "already_mapped": [],
        "missing_email": [],
        "duplicate_station": [],
        "duplicate_email": [],
    }

    candidates: List[Candidate] = []
    seen_stations: Set[str] = set()
    seen_emails: Set[str] = set()

    for lead in leads:
        sid = _clean(lead.get(field_name))
        if not sid:
            continue

        info = {"odoo_lead_id": lead.get("id"), "station_id": sid, "lead_name": _lead_display_name(lead, sid)}

        if sid in existing_station_ids:
            skipped["already_mapped"].append(info)
            continue

        if sid in seen_stations:
            skipped["duplicate_station"].append(info)
            continue
        seen_stations.add(sid)

        solis_row = station_map.get(sid, {})
        email = (_clean(lead.get("email_from")) or str(solis_row.get("email") or "")).strip().lower()
        if not email:
            skipped["missing_email"].append(info)
            continue

        if email in seen_emails:
            skipped["duplicate_email"].append({**info, "email": email})
            continue
        seen_emails.add(email)

        display_name = _lead_display_name(lead, str(solis_row.get("station_name") or sid))
        phone = _clean(lead.get("phone")) or str(solis_row.get("phone") or "").strip()

        candidates.append(
            Candidate(
                station_id=sid,
                csv_plant_name=str(solis_row.get("station_name") or ""),
                station_name=display_name,
                email=email,
                phone=phone,
            )
        )

    return candidates, skipped


async def onboard(
    sb: Client,
    solis: SolisCloudClient,
    apply: bool,
    field_name: str = DEFAULT_FIELD_NAME,
) -> Dict:
    """Core flow: read Odoo leads, resolve email/station data, create/upsert users."""
    leads = fetch_leads_with_station_id(field_name)
    existing_station_ids = get_existing_station_ids(sb)

    candidate_station_ids: Set[str] = {
        _clean(lead.get(field_name))
        for lead in leads
        if _clean(lead.get(field_name)) and _clean(lead.get(field_name)) not in existing_station_ids
    }

    station_map: Dict[str, Dict[str, str]] = {}
    if candidate_station_ids:
        station_map = await fetch_station_map(solis, candidate_station_ids)

    candidates, skipped = build_candidates(leads, existing_station_ids, station_map, field_name)

    counts: Dict[str, int] = {
        "created_auth_users": 0,
        "used_existing_auth_users": 0,
        "upserted_profiles": 0,
        "skipped_would_repoint": 0,
        "failed": 0,
    }
    results: List[Dict[str, str]] = []
    if candidates:
        counts, results = run_onboarding(sb, candidates, apply)

    # Backfill recent daily history for freshly onboarded stations (apply only).
    # Scoped to a short, env-configurable window (default 2 months) so the daily
    # cron stays fast. Idempotent (upsert-by-date) and only touches users with
    # zero existing readings.
    backfill_days = int(os.getenv("ONBOARD_BACKFILL_DAYS", "60"))
    backfill: Dict[str, int] = {"users": 0, "api_calls": 0, "rows": 0, "errors": 0}
    if apply and candidates and backfill_days > 0:
        try:
            from api.backfill_newly_onboarded import backfill_station_ids

            backfill = await backfill_station_ids(
                sb,
                solis,
                {c.station_id for c in candidates},
                days=backfill_days,
                apply=True,
            )
            log.info(
                "Onboarding backfill: %d user(s), %d row(s) over %d day(s).",
                backfill["users"],
                backfill["rows"],
                backfill_days,
            )
        except Exception as exc:  # never let backfill block onboarding
            log.error("Onboarding backfill failed: %s", exc)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "apply_mode": apply,
        "field_name": field_name,
        "odoo_leads_with_station_id": len(leads),
        "already_mapped_in_supabase": len(existing_station_ids),
        "candidate_count": len(candidates),
        "counts": counts,
        "skipped": skipped,
        "results": results,
        "backfill_days": backfill_days,
        "backfill": backfill,
    }

    finalize_run(sb, report)
    return report


def exception_count(report: Dict) -> int:
    """Actionable exceptions (already-mapped is normal and excluded)."""
    skipped = report.get("skipped", {})
    return (
        len(skipped.get("missing_email", []))
        + len(skipped.get("duplicate_email", []))
        + len(skipped.get("duplicate_station", []))
        + report.get("counts", {}).get("failed", 0)
        # A skipped repoint means a customer owns a station we cannot yet model:
        # actionable, and the multi-station merge worklist. Counting it here
        # routes it to the existing ALERT_WEBHOOK_URL / ALERT_EMAIL_TO path
        # rather than inventing a second notification channel.
        + report.get("counts", {}).get("skipped_would_repoint", 0)
    )


def record_run(sb: Client, report: Dict) -> None:
    """Persist one summary row to the onboarding_runs table (durable across cron runs)."""
    skipped = report.get("skipped", {})
    counts = report.get("counts", {})
    row = {
        "apply_mode": report.get("apply_mode", False),
        "candidates": report.get("candidate_count", 0),
        "created": counts.get("created_auth_users", 0),
        "failed": counts.get("failed", 0),
        "duplicate_email": len(skipped.get("duplicate_email", [])),
        "missing_email": len(skipped.get("missing_email", [])),
        "duplicate_station": len(skipped.get("duplicate_station", [])),
        "report": report,
    }
    try:
        sb.table("onboarding_runs").insert(row).execute()
        log.info("Recorded onboarding run to onboarding_runs.")
    except Exception as exc:
        log.error("Failed to record onboarding run: %s", exc)


def send_alert(report: Dict) -> None:
    """Notify via webhook and/or email, but only when there are actionable exceptions."""
    if exception_count(report) <= 0:
        return
    text = _alert_text(report)
    _send_webhook(text)
    _send_email(report, text)


def _alert_text(report: Dict) -> str:
    skipped = report.get("skipped", {})
    counts = report.get("counts", {})
    date = str(report.get("generated_at_utc", ""))[:10]
    return (
        f"Odoo→Supabase onboarding {date}: "
        f"{counts.get('upserted_profiles', 0)} onboarded, "
        f"{len(skipped.get('duplicate_email', []))} duplicate email, "
        f"{len(skipped.get('missing_email', []))} missing email, "
        f"{len(skipped.get('duplicate_station', []))} duplicate station, "
        f"{counts.get('failed', 0)} failed. See the onboarding_runs table."
    )


def _send_webhook(text: str) -> None:
    """Post to ALERT_WEBHOOK_URL (Slack/Discord compatible)."""
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    try:
        httpx.post(webhook, json={"text": f":warning: {text}", "content": f":warning: {text}"}, timeout=10.0)
    except Exception as exc:
        log.error("Failed to send onboarding webhook alert: %s", exc)


def _send_email(report: Dict, text: str) -> None:
    """Email the alert via SMTP. Requires SMTP_HOST + ALERT_EMAIL_TO (comma-separated)."""
    recipients = os.getenv("ALERT_EMAIL_TO")
    host = os.getenv("SMTP_HOST")
    if not recipients or not host:
        return
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("ALERT_EMAIL_FROM") or user or recipients.split(",")[0].strip()
    date = str(report.get("generated_at_utc", ""))[:10]

    msg = EmailMessage()
    msg["Subject"] = f"[Solviva] Onboarding exceptions — {date}"
    msg["From"] = sender
    msg["To"] = recipients
    msg.set_content(
        f"{text}\n\nFull details are in the onboarding_runs table (latest row).\n"
        f"apply_mode={report.get('apply_mode')}  candidates={report.get('candidate_count', 0)}"
    )
    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.starttls()
            if user and password:
                server.login(user, password)
            server.send_message(msg)
        log.info("Sent onboarding email alert to %s", recipients)
    except Exception as exc:
        log.error("Failed to send onboarding email alert: %s", exc)


def finalize_run(sb: Client, report: Dict) -> None:
    """Durable record + conditional alert, run after every onboarding pass."""
    record_run(sb, report)
    send_alert(report)


async def auto_onboard_from_odoo(
    sb: Client,
    solis: SolisCloudClient,
    field_name: str = DEFAULT_FIELD_NAME,
) -> Dict:
    """Convenience entry point for the daily sync pre-step (always applies)."""
    report = await onboard(sb, solis, apply=True, field_name=field_name)
    counts = report["counts"]
    log.info(
        "Odoo auto-onboarding: %d candidate(s) — created %d auth user(s), "
        "upserted %d profile(s), %d failed.",
        report["candidate_count"],
        counts["created_auth_users"],
        counts["upserted_profiles"],
        counts["failed"],
    )
    return report


def print_summary(report: Dict) -> None:
    skipped = report["skipped"]
    counts = report["counts"]
    print("=" * 72)
    print("ODOO → SUPABASE AUTO-ONBOARDING")
    print("=" * 72)
    print(f"Odoo leads with station id: {report['odoo_leads_with_station_id']}")
    print(f"Already mapped in Supabase: {report['already_mapped_in_supabase']}")
    print(f"Candidates to onboard:      {report['candidate_count']}")
    print(f"Skipped already mapped:     {len(skipped['already_mapped'])}")
    print(f"Skipped missing email:      {len(skipped['missing_email'])}")
    print(f"Skipped duplicate station:  {len(skipped['duplicate_station'])}")
    print(f"Skipped duplicate email:    {len(skipped['duplicate_email'])}")
    print("-" * 72)
    print("APPLY RESULTS" if report["apply_mode"] else "DRY-RUN RESULTS")
    print(f"Auth users to create:       {counts['created_auth_users']}")
    print(f"Use existing auth users:    {counts['used_existing_auth_users']}")
    print(f"Profiles to upsert:         {counts['upserted_profiles']}")
    print(f"Failed:                     {counts['failed']}")
    print("=" * 72)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Auto-onboard Supabase users from Odoo leads.")
    parser.add_argument("--apply", action="store_true", help="Create users / upsert profiles (default: dry-run).")
    parser.add_argument("--field", default=DEFAULT_FIELD_NAME, help="Odoo Solis station-id field name.")
    parser.add_argument("--report", default=DEFAULT_REPORT_PATH, help="Path to write the JSON report.")
    args = parser.parse_args()

    load_environment()
    sb = build_supabase()
    solis = build_solis()

    if not args.apply:
        print("DRY RUN — no users will be created. Pass --apply to execute.")

    report = await onboard(sb, solis, apply=args.apply, field_name=args.field)
    print_summary(report)

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"Report written to {os.path.abspath(args.report)}")


if __name__ == "__main__":
    asyncio.run(main())
