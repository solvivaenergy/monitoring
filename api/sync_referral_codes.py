r"""
Sync referral codes from Odoo CRM → Supabase user_profiles.

Each Solviva client has a permanent referral code in Odoo (field:
x_studio_customer_referral_code) that they share to refer new clients.
This script fetches those codes and stores them in user_profiles.referral_code.

Matching strategy:
  1. Primary — email: Odoo lead email_from → Supabase auth user email → user_id
  2. Fallback — name:  Odoo contact/partner name → user_profiles.full_name

Usage:
    # Standalone run
    python -m api.sync_referral_codes

    # Also called from sync_to_supabase.py during the daily cron
"""

import logging
import os
import sys
import xmlrpc.client
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

_here = os.path.dirname(os.path.abspath(__file__))
_solviva_env = os.path.normpath(
    os.path.join(_here, "..", "..", "Odoo Solviva", "solviva_service", ".env")
)
if os.path.exists(_solviva_env):
    load_dotenv(_solviva_env)
load_dotenv(os.path.join(_here, "..", ".env"))

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("referral_sync")


def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def build_supabase() -> Client:
    return create_client(get_env("SUPABASE_URL"), get_env("SUPABASE_SERVICE_KEY"))


# ── Odoo ─────────────────────────────────────────────────────────


def _odoo_connect() -> Tuple[str, int, str, xmlrpc.client.ServerProxy]:
    """Authenticate to Odoo and return (db, uid, auth, models_proxy)."""
    url = get_env("ODOO_URL")
    db = get_env("ODOO_DB")
    user = get_env("ODOO_USER")
    auth = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD")
    if not auth:
        raise RuntimeError("Missing ODOO_API_KEY or ODOO_PASSWORD")

    common = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/common")
    uid = common.authenticate(db, user, auth, {})
    if not uid:
        raise RuntimeError("Odoo authentication failed")

    models = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/object")
    return db, uid, auth, models


def fetch_odoo_referral_codes(
    db: str, uid: int, auth: str, models: xmlrpc.client.ServerProxy
) -> List[Dict]:
    """Fetch all CRM leads that have a customer referral code."""
    lead_ids = models.execute_kw(
        db, uid, auth,
        "crm.lead", "search",
        [[["x_studio_customer_referral_code", "!=", False]]],
    )
    log.info("Found %d Odoo leads with referral codes.", len(lead_ids))

    fields = [
        "id", "email_from", "contact_name", "partner_name",
        "x_studio_customer_referral_code",
    ]

    leads: List[Dict] = []
    batch = 200
    for i in range(0, len(lead_ids), batch):
        chunk = lead_ids[i : i + batch]
        recs = models.execute_kw(
            db, uid, auth,
            "crm.lead", "read",
            [chunk],
            {"fields": fields},
        )
        leads.extend(recs)

    return leads


# ── Matching ─────────────────────────────────────────────────────


def _normalize(name: str) -> str:
    return " ".join(name.lower().split())


def sync_referral_codes(sb: Optional[Client] = None) -> int:
    """
    Pull referral codes from Odoo and update user_profiles in Supabase.
    Returns the number of profiles updated.
    """
    # 1. Connect to Odoo
    db, uid, auth, models = _odoo_connect()

    # 2. Fetch leads with referral codes
    leads = fetch_odoo_referral_codes(db, uid, auth, models)
    if not leads:
        log.info("No leads with referral codes — nothing to sync.")
        return 0

    # 3. Connect to Supabase
    if sb is None:
        sb = build_supabase()

    # 4. Fetch all user_profiles
    profiles_resp = sb.table("user_profiles").select("id, full_name, referral_code").execute()
    profiles = profiles_resp.data or []
    log.info("Loaded %d user_profiles from Supabase.", len(profiles))

    # 5. Fetch auth users to build email → user_id map
    auth_users = sb.auth.admin.list_users()
    email_to_uid: Dict[str, str] = {}
    for u in auth_users:
        if u.email:
            email_to_uid[u.email.strip().lower()] = u.id
    log.info("Loaded %d auth users for email matching.", len(email_to_uid))

    # 6. Build name → profile map (normalized full_name → profile)
    name_to_profile: Dict[str, Dict] = {}
    id_to_profile: Dict[str, Dict] = {}
    for p in profiles:
        id_to_profile[p["id"]] = p
        fn = p.get("full_name")
        if fn:
            name_to_profile[_normalize(fn)] = p

    # 7. Match and update
    updated = 0
    skipped_no_match = 0
    skipped_already_set = 0

    for lead in leads:
        code = (lead.get("x_studio_customer_referral_code") or "").strip()
        if not code:
            continue

        email = (lead.get("email_from") or "").strip().lower()
        contact = lead.get("contact_name") or ""
        partner = lead.get("partner_name") or ""
        if contact == "False":
            contact = ""
        if partner == "False":
            partner = ""

        # Try email match first
        profile: Optional[Dict] = None
        if email and email in email_to_uid:
            uid_match = email_to_uid[email]
            profile = id_to_profile.get(uid_match)

        # Fallback: name match
        if profile is None:
            for name_candidate in (contact, partner):
                if name_candidate:
                    norm = _normalize(name_candidate)
                    if norm in name_to_profile:
                        profile = name_to_profile[norm]
                        break

        if profile is None:
            skipped_no_match += 1
            continue

        # Skip if already set to the same value
        if profile.get("referral_code") == code:
            skipped_already_set += 1
            continue

        # Update
        sb.table("user_profiles").update({"referral_code": code}).eq("id", profile["id"]).execute()
        updated += 1
        display = contact or partner or email or f"lead#{lead['id']}"
        log.info("Updated referral_code for %s → %s", display, code)

    log.info(
        "Referral sync complete: %d updated, %d already set, %d no match.",
        updated, skipped_already_set, skipped_no_match,
    )
    return updated


def main() -> None:
    sync_referral_codes()


if __name__ == "__main__":
    main()
