r"""
Provision Supabase users from the Solis-Odoo mapping CSV file.

Reads the CSV (solis_odoo_mapping_2026-05-29_all.csv), identifies unmapped users
(already_mapped = FALSE), fetches their details from Odoo, and creates Supabase
auth users + user_profiles.

Usage:
    cd C:\Users\roald\Documents\GitHub\monitoring
    python -m api.provision_from_csv          # dry-run (default)
    python -m api.provision_from_csv --apply  # actually create users
"""

import asyncio
import csv
import os
import sys
import logging
from typing import Dict, List

from dotenv import load_dotenv

_here = os.path.dirname(os.path.abspath(__file__))
_solviva_env = os.path.normpath(os.path.join(
    _here, "..", "..", "Odoo Solviva", "solviva_service", ".env",
))
if os.path.exists(_solviva_env):
    load_dotenv(_solviva_env)
load_dotenv(os.path.join(_here, "..", ".env"))

from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("provision_csv")

# Was a hardcoded literal until 2026-09-16 — the same leaked password as
# api/provision_users.py. This file is untracked, so it never reached GitHub,
# but it would have leaked the moment anyone committed it. Env var only, no
# fallback. See api/rotate_leaked_passwords.py.
DEFAULT_PASSWORD_ENV = "DEFAULT_USER_PASSWORD"
CSV_PATH = os.path.join(_here, "..", "solis_odoo_mapping_2026-05-29_all.csv")


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def build_supabase() -> Client:
    return create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))


def load_unmapped_from_csv() -> List[Dict]:
    """Load unmapped rows from CSV (already_mapped = FALSE)."""
    unmapped = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("already_mapped", "").upper() == "FALSE":
                unmapped.append(row)
    
    log.info("Loaded %d unmapped rows from CSV", len(unmapped))
    return unmapped


def extract_data_from_csv(unmapped: List[Dict]) -> tuple:
    """Extract email and details directly from CSV rows.
    
    Returns: (data_dict, csv_by_lead_dict)
    """
    results = {}
    csv_by_lead = {}
    for row in unmapped:
        try:
            lead_id = int(row.get("odoo_lead_id", "0") or "0")
            if lead_id > 0:
                results[lead_id] = {
                    "email_from": row.get("odoo_email", "").strip(),
                    "partner_name": row.get("odoo_lead_name", ""),
                    "contact_name": row.get("odoo_lead_name", ""),
                    "phone": "",
                    "x_studio_complete_address": row.get("solis_address", ""),
                }
                csv_by_lead[lead_id] = row
        except ValueError:
            pass
    
    log.info("Extracted data from CSV for %d lead records", len(results))
    return results, csv_by_lead


def main():
    dry_run = "--apply" not in sys.argv

    if dry_run:
        print("=" * 70)
        print("DRY RUN — no users will be created. Pass --apply to execute.")
        print("=" * 70)
    else:
        print("=" * 70)
        print("LIVE RUN — creating users in Supabase!")
        print("=" * 70)

    # 1. Load unmapped rows from CSV
    unmapped = load_unmapped_from_csv()

    # 2. Extract data from CSV rows
    log.info("Extracting data from CSV for %d unmapped rows...", len(unmapped))
    odoo_data, csv_by_lead = extract_data_from_csv(unmapped)
    
    if not odoo_data:
        print("No unmapped users to provision.")
        return

    # 3. Build provisioning list
    to_provision = []
    skipped_no_email = []
    skipped_duplicate = []
    seen_emails = set()

    for lead_id in odoo_data.keys():
        csv_row = csv_by_lead.get(lead_id, {})
        odoo_lead = odoo_data.get(lead_id, {})
        email = (odoo_lead.get("email_from") or "").strip().lower()

        if not email or email == "false":
            skipped_no_email.append((lead_id, csv_row))
            continue

        if email in seen_emails:
            skipped_duplicate.append((lead_id, csv_row, email))
            continue
        seen_emails.add(email)

        # Determine display name
        partner = odoo_lead.get("partner_name") or ""
        contact = odoo_lead.get("contact_name") or ""
        display_name = contact if contact and contact != "False" else partner
        if not display_name or display_name == "False":
            display_name = csv_row.get("solis_plant_name", "Solviva User")

        phone = odoo_lead.get("phone") or ""
        if phone == "False":
            phone = ""
        address = odoo_lead.get("x_studio_complete_address") or csv_row.get("solis_address", "")
        if address == "False":
            address = ""

        to_provision.append({
            "email": email,
            "full_name": display_name,
            "phone": phone,
            "address": address,
            "solis_station_id": csv_row.get("solis_station_id"),
            "station_name": csv_row.get("solis_plant_name"),
            "lead_id": lead_id,
        })

    # 4. Print summary
    print(f"\n{'=' * 70}")
    print("PROVISIONING SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Unmapped rows in CSV:     {len(unmapped)}")
    print(f"  Extracted from CSV:       {len(odoo_data)}")
    print(f"  With valid email:         {len(to_provision)}")
    print(f"  Skipped (no email):       {len(skipped_no_email)}")
    print(f"  Skipped (duplicate email): {len(skipped_duplicate)}")
    print()

    if skipped_no_email:
        print(f"Leads without email ({len(skipped_no_email)}):")
        for lead_id, row in skipped_no_email:
            print(f"  - Lead #{lead_id} \"{row.get('odoo_lead_name', '')}\" → Station \"{row.get('solis_plant_name', '')}\"")
        print()

    if skipped_duplicate:
        print(f"Duplicate emails skipped ({len(skipped_duplicate)}):")
        for lead_id, row, email in skipped_duplicate:
            print(f"  - Lead #{lead_id} \"{row.get('odoo_lead_name', '')}\" email={email}")
        print()

    print(f"Users to create ({len(to_provision)}):")
    for u in to_provision:
        print(f"  {u['email']:40s} → {u['full_name']:30s} station={u['station_name']}")
    print()

    if dry_run:
        print("Pass --apply to create these users.")
        return

    # 5. Create users in Supabase
    default_password = os.getenv(DEFAULT_PASSWORD_ENV)
    if not default_password:
        raise RuntimeError(
            f"{DEFAULT_PASSWORD_ENV} env var is required to create Supabase auth users."
        )
    sb = build_supabase()
    created = 0
    failed = 0
    already_exists = 0

    for u in to_provision:
        try:
            # Create auth user via admin API
            res = sb.auth.admin.create_user({
                "email": u["email"],
                "password": default_password,
                "email_confirm": True,  # mark email as confirmed
                "user_metadata": {"full_name": u["full_name"]},
            })

            user_id = res.user.id
            log.info("Created auth user: %s (id=%s)", u["email"], user_id)

            # Create user_profile
            sb.table("user_profiles").upsert({
                "id": user_id,
                "full_name": u["full_name"],
                "phone": u["phone"] or None,
                "address": u["address"] or None,
                "solis_station_id": str(u["solis_station_id"]),
            }).execute()

            log.info("Created profile for %s with solis_station_id=%s", u["email"], u["solis_station_id"])
            created += 1

        except Exception as e:
            err_msg = str(e)
            if "already been registered" in err_msg or "already exists" in err_msg:
                log.warning("User already exists: %s — updating profile only", u["email"])
                # Try to find existing user and update their profile
                try:
                    # Use a filter query instead of listing all users
                    existing = sb.table("user_profiles").select("id").eq("solis_station_id", str(u["solis_station_id"])).execute()
                    if existing.data:
                        user_id = existing.data[0]["id"]
                        sb.table("user_profiles").upsert({
                            "id": user_id,
                            "full_name": u["full_name"],
                            "phone": u["phone"] or None,
                            "address": u["address"] or None,
                            "solis_station_id": str(u["solis_station_id"]),
                        }).execute()
                        log.info("Updated existing profile for %s (solis_station=%s)", u["email"], u["solis_station_id"])
                    else:
                        log.warning("Could not find existing user with solis_station_id=%s", u["solis_station_id"])
                except Exception as e2:
                    log.error("Failed to update existing user %s: %s", u["email"], e2)
                already_exists += 1
            else:
                log.error("Failed to create %s: %s", u["email"], e)
                failed += 1

    print(f"\n{'=' * 70}")
    print("RESULTS")
    print(f"{'=' * 70}")
    print(f"  Created:        {created}")
    print(f"  Already existed: {already_exists}")
    print(f"  Failed:          {failed}")
    print(f"{'=' * 70}\n")

    report = {
        "total_to_provision": len(to_provision),
        "created": created,
        "already_exists": already_exists,
        "failed": failed,
    }
    
    report_path = os.path.join(_here, "..", "provisioning_from_csv_report.json")
    import json
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    log.info("Report saved to %s", report_path)


if __name__ == "__main__":
    main()
