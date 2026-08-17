"""
Onboard Supabase users from a station-id CSV using Solis as source data.

Input CSV format (required columns):
    station_id, excel_plant_name

The script:
1) Reads station IDs from CSV.
2) Checks existing Supabase mappings in user_profiles.solis_station_id.
3) Resolves station metadata from Solis (stationName, userEmail, phone if present).
4) Creates missing auth users and upserts user_profiles with solis_station_id.
    New auth users also get solis_station_id in user_metadata so the mobile app
    can resolve station mappings without a live user_profiles lookup.

Usage:
    python -m api.onboard_from_station_csv
    python -m api.onboard_from_station_csv --apply
    python -m api.onboard_from_station_csv --csv "Supabase Snippet Untitled query.csv"
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from supabase import Client, create_client

from api.solis_client import SolisCloudClient


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("onboard_station_csv")
logging.getLogger("httpx").setLevel(logging.WARNING)

# Password for auto-created Supabase auth users. Sourced from env so it is not
# committed to the repo; required only when actually creating users (apply mode).
DEFAULT_PASSWORD_ENV = "DEFAULT_USER_PASSWORD"
DEFAULT_CSV_PATH = "Supabase Snippet Untitled query.csv"

POSSIBLE_EMAIL_FIELDS = ["userEmail", "email", "mail", "ownerEmail", "stationOwnerEmail"]
POSSIBLE_PHONE_FIELDS = [
    "phone",
    "mobile",
    "userPhone",
    "phoneNumber",
    "linkPhone",
    "contactPhone",
    "ownerPhone",
]


@dataclass
class CsvRow:
    station_id: str
    excel_plant_name: str


@dataclass
class Candidate:
    station_id: str
    csv_plant_name: str
    station_name: str
    email: str
    phone: str


def get_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def load_environment() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    solviva_env = os.path.normpath(
        os.path.join(here, "..", "..", "Odoo Solviva", "solviva_service", ".env")
    )
    if os.path.exists(solviva_env):
        load_dotenv(solviva_env)
    load_dotenv(os.path.join(here, "..", ".env"))
    load_dotenv()


def build_supabase() -> Client:
    return create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))


def build_solis() -> SolisCloudClient:
    return SolisCloudClient(
        key_id=get_env("SOLIS_CLOUD_KEY_ID"),
        key_secret=get_env("SOLIS_CLOUD_KEY_SECRET"),
    )


def read_csv_rows(csv_path: str) -> List[CsvRow]:
    rows: List[CsvRow] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            station_id = str(raw.get("station_id") or "").strip()
            if not station_id:
                continue
            rows.append(
                CsvRow(
                    station_id=station_id,
                    excel_plant_name=str(raw.get("excel_plant_name") or "").strip(),
                )
            )
    return rows


def get_existing_station_ids(sb: Client) -> Set[str]:
    existing: Set[str] = set()
    page_size = 1000
    offset = 0
    while True:
        page = (
            sb.table("user_profiles")
            .select("solis_station_id")
            .not_.is_("solis_station_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        ).data or []
        for row in page:
            sid = str(row.get("solis_station_id") or "").strip()
            if sid:
                existing.add(sid)
        if len(page) < page_size:
            break
        offset += page_size
    return existing


def first_non_empty(payload: Dict, keys: List[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() != "false":
            return text
    return ""


async def fetch_station_map(
    solis: SolisCloudClient, station_ids: Set[str]
) -> Dict[str, Dict[str, str]]:
    """
    Build map station_id -> {station_name, email, phone}.

    Uses list_stations pagination first, then station_detail fallback for IDs not found
    in the paged station list.
    """
    resolved: Dict[str, Dict[str, str]] = {}

    page_no = 1
    page_size = 100
    while True:
        payload = await solis.list_stations(page_no=page_no, page_size=page_size)
        page = payload.get("page") if isinstance(payload, dict) else {}
        records = []
        if isinstance(page, dict):
            records = page.get("records") or []
        if not records and isinstance(payload, dict):
            records = payload.get("records") or []

        for rec in records:
            sid = str(rec.get("id") or "").strip()
            if sid in station_ids:
                resolved[sid] = {
                    "station_name": str(rec.get("stationName") or rec.get("name") or sid),
                    "email": first_non_empty(rec, POSSIBLE_EMAIL_FIELDS).lower(),
                    "phone": first_non_empty(rec, POSSIBLE_PHONE_FIELDS),
                }

        total = page.get("total") if isinstance(page, dict) else None
        if not records:
            break
        if total is not None and page_no * page_size >= int(total):
            break
        page_no += 1

    missing_ids = [sid for sid in station_ids if sid not in resolved]
    if missing_ids:
        log.info("Station list fallback: calling station_detail for %d station(s)", len(missing_ids))

    for sid in missing_ids:
        try:
            detail = await solis.station_detail(sid)
            if not isinstance(detail, dict):
                log.warning("station_detail returned empty payload for %s", sid)
                continue
            resolved[sid] = {
                "station_name": str(detail.get("stationName") or detail.get("name") or sid),
                "email": first_non_empty(detail, POSSIBLE_EMAIL_FIELDS).lower(),
                "phone": first_non_empty(detail, POSSIBLE_PHONE_FIELDS),
            }
        except Exception as exc:
            log.warning("Failed station_detail for %s: %s", sid, exc)

    return resolved


def build_candidates(
    rows: List[CsvRow],
    existing_station_ids: Set[str],
    station_map: Dict[str, Dict[str, str]],
) -> Tuple[List[Candidate], Dict[str, List[Dict[str, str]]]]:
    skipped = {
        "already_mapped": [],
        "not_found_in_solis": [],
        "missing_email": [],
        "duplicate_email": [],
    }

    candidates: List[Candidate] = []
    seen_emails: Set[str] = set()

    for row in rows:
        sid = row.station_id
        if sid in existing_station_ids:
            skipped["already_mapped"].append(
                {"station_id": sid, "excel_plant_name": row.excel_plant_name}
            )
            continue

        solis_row = station_map.get(sid)
        if not solis_row:
            skipped["not_found_in_solis"].append(
                {"station_id": sid, "excel_plant_name": row.excel_plant_name}
            )
            continue

        email = str(solis_row.get("email") or "").strip().lower()
        if not email:
            skipped["missing_email"].append(
                {
                    "station_id": sid,
                    "excel_plant_name": row.excel_plant_name,
                    "station_name": solis_row.get("station_name") or "",
                }
            )
            continue

        if email in seen_emails:
            skipped["duplicate_email"].append(
                {
                    "station_id": sid,
                    "excel_plant_name": row.excel_plant_name,
                    "station_name": solis_row.get("station_name") or "",
                    "email": email,
                }
            )
            continue
        seen_emails.add(email)

        station_name = str(solis_row.get("station_name") or "").strip() or row.excel_plant_name or sid
        candidates.append(
            Candidate(
                station_id=sid,
                csv_plant_name=row.excel_plant_name,
                station_name=station_name,
                email=email,
                phone=str(solis_row.get("phone") or "").strip(),
            )
        )

    return candidates, skipped


def get_auth_users_by_email(sb: Client) -> Dict[str, str]:
    email_to_user_id: Dict[str, str] = {}
    page = 1
    per_page = 1000
    while True:
        users = sb.auth.admin.list_users(page=page, per_page=per_page)
        if not users:
            break
        for user in users:
            email = str(getattr(user, "email", "") or "").strip().lower()
            user_id = str(getattr(user, "id", "") or "").strip()
            if email and user_id:
                email_to_user_id[email] = user_id
        if len(users) < per_page:
            break
        page += 1
    return email_to_user_id


def find_auth_user_id_by_email(sb: Client, email: str) -> Optional[str]:
    target = email.strip().lower()
    if not target:
        return None
    page = 1
    per_page = 1000
    while True:
        users = sb.auth.admin.list_users(page=page, per_page=per_page)
        if not users:
            break
        for user in users:
            em = str(getattr(user, "email", "") or "").strip().lower()
            if em == target:
                user_id = str(getattr(user, "id", "") or "").strip()
                if user_id:
                    return user_id
        if len(users) < per_page:
            break
        page += 1
    return None


def run_onboarding(
    sb: Client,
    candidates: List[Candidate],
    apply: bool,
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    counts = {
        "created_auth_users": 0,
        "used_existing_auth_users": 0,
        "upserted_profiles": 0,
        "failed": 0,
    }
    results: List[Dict[str, str]] = []

    default_password = os.getenv(DEFAULT_PASSWORD_ENV)
    if apply and not default_password:
        raise RuntimeError(
            f"{DEFAULT_PASSWORD_ENV} env var is required to create Supabase auth users."
        )

    email_to_user_id = get_auth_users_by_email(sb)

    for c in candidates:
        try:
            user_id = email_to_user_id.get(c.email)
            action = "existing_auth"

            if not user_id:
                action = "create_auth"
                created_auth = False
                if apply:
                    try:
                        created = sb.auth.admin.create_user(
                            {
                                "email": c.email,
                                "password": default_password,
                                "email_confirm": True,
                                "user_metadata": {
                                    "full_name": c.station_name,
                                    "solis_station_id": c.station_id,
                                },
                            }
                        )
                        user_id = str(created.user.id)
                        email_to_user_id[c.email] = user_id
                        created_auth = True
                    except Exception as create_exc:
                        msg = str(create_exc).lower()
                        if "already" in msg and "registered" in msg:
                            existing_id = find_auth_user_id_by_email(sb, c.email)
                            if existing_id:
                                user_id = existing_id
                                email_to_user_id[c.email] = user_id
                                action = "existing_auth_after_create_conflict"
                            else:
                                raise
                        else:
                            raise
                else:
                    created_auth = True

                if created_auth:
                    counts["created_auth_users"] += 1
                else:
                    counts["used_existing_auth_users"] += 1
            else:
                counts["used_existing_auth_users"] += 1

            profile_payload = {
                "id": user_id or "<dry-run-user-id>",
                "full_name": c.station_name,
                "phone": c.phone or None,
                "solis_station_id": c.station_id,
            }

            if apply and user_id:
                sb.table("user_profiles").upsert(profile_payload).execute()

            counts["upserted_profiles"] += 1
            results.append(
                {
                    "station_id": c.station_id,
                    "email": c.email,
                    "full_name": c.station_name,
                    "phone": c.phone,
                    "status": "ok",
                    "auth_action": action,
                }
            )

        except Exception as exc:
            counts["failed"] += 1
            results.append(
                {
                    "station_id": c.station_id,
                    "email": c.email,
                    "full_name": c.station_name,
                    "phone": c.phone,
                    "status": "failed",
                    "error": str(exc),
                }
            )

    return counts, results


def print_summary(
    csv_rows: List[CsvRow],
    existing_station_ids: Set[str],
    candidates: List[Candidate],
    skipped: Dict[str, List[Dict[str, str]]],
    counts: Optional[Dict[str, int]] = None,
    apply: bool = False,
) -> None:
    print("=" * 72)
    print("STATION CSV ONBOARDING")
    print("=" * 72)
    print(f"CSV rows loaded:          {len(csv_rows)}")
    print(f"Mapped stations in DB:    {len(existing_station_ids)}")
    print(f"Candidates to onboard:    {len(candidates)}")
    print(f"Skipped already mapped:   {len(skipped['already_mapped'])}")
    print(f"Skipped not in Solis:     {len(skipped['not_found_in_solis'])}")
    print(f"Skipped missing email:    {len(skipped['missing_email'])}")
    print(f"Skipped duplicate email:  {len(skipped['duplicate_email'])}")

    if counts is not None:
        print("-" * 72)
        print("APPLY RESULTS" if apply else "DRY-RUN RESULTS")
        print(f"Auth users to create:     {counts['created_auth_users']}")
        print(f"Use existing auth users:  {counts['used_existing_auth_users']}")
        print(f"Profiles to upsert:       {counts['upserted_profiles']}")
        print(f"Failed:                   {counts['failed']}")
    print("=" * 72)


def save_report(
    report_path: str,
    csv_path: str,
    apply: bool,
    csv_rows: List[CsvRow],
    candidates: List[Candidate],
    skipped: Dict[str, List[Dict[str, str]]],
    counts: Dict[str, int],
    results: List[Dict[str, str]],
) -> None:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "apply_mode": apply,
        "csv_path": os.path.abspath(csv_path),
        "csv_rows": len(csv_rows),
        "candidate_count": len(candidates),
        "counts": counts,
        "skipped": skipped,
        "results": results,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


async def async_main(args: argparse.Namespace) -> None:
    load_environment()
    csv_path = args.csv
    apply_mode = args.apply

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    csv_rows = read_csv_rows(csv_path)
    if not csv_rows:
        print("CSV is empty or has no valid station_id rows.")
        return

    sb = build_supabase()
    solis = build_solis()

    existing_station_ids = get_existing_station_ids(sb)
    station_ids = {row.station_id for row in csv_rows}
    station_map = await fetch_station_map(solis, station_ids)
    candidates, skipped = build_candidates(csv_rows, existing_station_ids, station_map)

    if apply_mode:
        log.info("LIVE RUN -- creating/updating Supabase records")
    else:
        log.info("DRY RUN -- no Supabase writes")

    counts, results = run_onboarding(sb, candidates, apply=apply_mode)
    print_summary(
        csv_rows=csv_rows,
        existing_station_ids=existing_station_ids,
        candidates=candidates,
        skipped=skipped,
        counts=counts,
        apply=apply_mode,
    )

    if args.show:
        print("Sample candidates:")
        for c in candidates[: args.show]:
            print(
                f"  station={c.station_id} email={c.email} name={c.station_name} phone={c.phone or '-'}"
            )

    report_path = args.report
    save_report(
        report_path=report_path,
        csv_path=csv_path,
        apply=apply_mode,
        csv_rows=csv_rows,
        candidates=candidates,
        skipped=skipped,
        counts=counts,
        results=results,
    )
    print(f"Report saved to: {os.path.abspath(report_path)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Onboard Supabase users from station_id CSV using Solis station data"
    )
    parser.add_argument(
        "--csv",
        default=DEFAULT_CSV_PATH,
        help="Path to input CSV file (default: Supabase Snippet Untitled query.csv)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (create auth users + upsert user_profiles). Default is dry-run.",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=10,
        help="Number of candidate rows to print (default: 10)",
    )
    parser.add_argument(
        "--report",
        default="onboarding_from_station_csv_report.json",
        help="Output JSON report path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
