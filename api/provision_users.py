r"""
Provision Supabase users from Odoo-Solis exact matches.

Reads the match report, fetches email addresses from Odoo for exact/high
matches, then creates Supabase auth users + user_profiles with the
solis_station_id already mapped.

Usage:
    cd C:\Users\roald\Documents\GitHub\monitoring
    python -m api.provision_users          # dry-run (default)
    python -m api.provision_users --apply  # actually create users
"""

import asyncio
import json
import os
import sys
import xmlrpc.client
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
log = logging.getLogger("provision")

DEFAULT_PASSWORD = "solviva$upremacy2026"
REPORT_PATH = os.path.join(_here, "..", "odoo_solis_match_report.json")


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def build_supabase() -> Client:
    return create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))


def load_report() -> Dict:
    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_emails_from_odoo(lead_ids: List[int]) -> Dict[int, Dict]:
    """Fetch email_from and partner_name for specific lead IDs from Odoo."""
    url = get_env("ODOO_URL")
    db = get_env("ODOO_DB")
    user = get_env("ODOO_USER")
    auth = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD")

    common = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/common")
    uid = common.authenticate(db, user, auth, {})
    if not uid:
        raise RuntimeError("Odoo auth failed")

    models = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/object")

    results = {}
    batch = 200
    for i in range(0, len(lead_ids), batch):
        chunk = lead_ids[i:i + batch]
        recs = models.execute_kw(
            db, uid, auth,
            "crm.lead", "read",
            [chunk],
            {"fields": ["id", "email_from", "partner_name", "contact_name", "phone",
                         "x_studio_complete_address"]},
        )
        for r in recs:
            results[r["id"]] = r
        log.info("Fetched emails: %d/%d", len(results), len(lead_ids))

    return results


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

    # 1. Load match report
    report = load_report()
    exact_matches = report.get("exact_matches", [])
    log.info("Loaded %d exact/high-confidence matches from report.", len(exact_matches))

    # 2. Get lead IDs and fetch emails from Odoo
    lead_ids = [m["lead_id"] for m in exact_matches]
    log.info("Fetching email addresses from Odoo for %d leads...", len(lead_ids))
    odoo_data = fetch_emails_from_odoo(lead_ids)

    # 3. Build provisioning list
    to_provision = []
    skipped_no_email = []
    skipped_duplicate = []
    seen_emails = set()

    for match in exact_matches:
        lead_id = match["lead_id"]
        odoo_lead = odoo_data.get(lead_id, {})
        email = (odoo_lead.get("email_from") or "").strip().lower()

        if not email or email == "false":
            skipped_no_email.append(match)
            continue

        if email in seen_emails:
            skipped_duplicate.append({**match, "email": email})
            continue
        seen_emails.add(email)

        # Determine display name
        partner = odoo_lead.get("partner_name") or ""
        contact = odoo_lead.get("contact_name") or ""
        display_name = contact if contact and contact != "False" else partner
        if not display_name or display_name == "False":
            display_name = match.get("station_name", "Solviva User")

        phone = odoo_lead.get("phone") or ""
        if phone == "False":
            phone = ""
        address = odoo_lead.get("x_studio_complete_address") or ""
        if address == "False":
            address = ""

        to_provision.append({
            "email": email,
            "full_name": display_name,
            "phone": phone,
            "address": address,
            "solis_station_id": match["station_id"],
            "station_name": match["station_name"],
            "lead_id": lead_id,
        })

    # 4. Print summary
    print(f"\n{'=' * 70}")
    print("PROVISIONING SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Exact matches loaded:     {len(exact_matches)}")
    print(f"  With valid email:         {len(to_provision)}")
    print(f"  Skipped (no email):       {len(skipped_no_email)}")
    print(f"  Skipped (duplicate email): {len(skipped_duplicate)}")
    print()

    if skipped_no_email:
        print(f"Leads without email ({len(skipped_no_email)}):")
        for m in skipped_no_email:
            print(f"  - Lead #{m['lead_id']} \"{m.get('partner_name', '')}\" → Station \"{m['station_name']}\"")
        print()

    if skipped_duplicate:
        print(f"Duplicate emails skipped ({len(skipped_duplicate)}):")
        for m in skipped_duplicate:
            print(f"  - Lead #{m['lead_id']} \"{m.get('partner_name', '')}\" email={m['email']}")
        print()

    print(f"Users to create ({len(to_provision)}):")
    for u in to_provision:
        print(f"  {u['email']:40s} → {u['full_name']:30s} station={u['station_name']}")
    print()

    if dry_run:
        print("Pass --apply to create these users.")
        return

    # 5. Create users in Supabase
    sb = build_supabase()
    created = 0
    failed = 0
    already_exists = 0

    for u in to_provision:
        try:
            # Create auth user via admin API
            res = sb.auth.admin.create_user({
                "email": u["email"],
                "password": DEFAULT_PASSWORD,
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
                    existing = sb.auth.admin.list_users()
                    existing_user = None
                    for eu in existing:
                        if hasattr(eu, 'email') and eu.email == u["email"]:
                            existing_user = eu
                            break
                    if existing_user:
                        sb.table("user_profiles").upsert({
                            "id": existing_user.id,
                            "full_name": u["full_name"],
                            "phone": u["phone"] or None,
                            "address": u["address"] or None,
                            "solis_station_id": str(u["solis_station_id"]),
                        }).execute()
                        log.info("Updated existing profile for %s", u["email"])
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
    print(f"  Total processed: {created + already_exists + failed}")

    # Save provisioning report
    report_out = {
        "created": created,
        "already_existed": already_exists,
        "failed": failed,
        "users": to_provision,
        "skipped_no_email": [{"lead_id": m["lead_id"], "partner_name": m.get("partner_name", "")} for m in skipped_no_email],
        "skipped_duplicate": skipped_duplicate,
    }
    out_path = os.path.join(_here, "..", "provisioning_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report_out, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nReport saved to: {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()
