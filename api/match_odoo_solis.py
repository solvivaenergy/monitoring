r"""
Match Odoo CRM leads (Installed and beyond) with Solis Cloud stations.

Pulls leads from Odoo in stages: Installed, Project Closed, Permitting
(including Zoho equivalents) and all stations from Solis, then matches
by name (exact and partial/fuzzy) to help map solis_station_id to users.

Usage:
    cd C:\Users\roald\Documents\GitHub\monitoring
    python -m api.match_odoo_solis
"""

import asyncio
import os
import sys
import json
import re
import xmlrpc.client
from datetime import datetime
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

# Load env from solviva_service/.env which has both Odoo + Solis creds
_here = os.path.dirname(__file__)
_solviva_env = os.path.normpath(os.path.join(
    _here, "..", "..", "Odoo Solviva", "solviva_service", ".env",
))
if os.path.exists(_solviva_env):
    load_dotenv(_solviva_env)
# Also load local .env (for Solis creds if stored here)
load_dotenv(os.path.join(_here, "..", ".env"))

from api.solis_client import SolisCloudClient


# ── helpers ──────────────────────────────────────────────────────

def normalize(name: str) -> str:
    """Lowercase, strip non-alphanum, collapse whitespace."""
    s = re.sub(r"[^\w\s]", " ", name.lower())
    return " ".join(s.split())


def token_set(name: str) -> set:
    return set(normalize(name).split())


def partial_ratio(a: str, b: str) -> float:
    """Simple partial match score: fraction of tokens in common."""
    ta, tb = token_set(a), token_set(b)
    if not ta or not tb:
        return 0.0
    common = ta & tb
    return len(common) / min(len(ta), len(tb))


# ── Odoo ─────────────────────────────────────────────────────────

def fetch_odoo_installed_leads() -> List[Dict]:
    url = os.getenv("ODOO_URL")
    db = os.getenv("ODOO_DB")
    user = os.getenv("ODOO_USER")
    api_key = os.getenv("ODOO_API_KEY")
    password = os.getenv("ODOO_PASSWORD")
    auth = api_key or password

    if not all([url, db, user, auth]):
        raise RuntimeError("Missing ODOO_URL / ODOO_DB / ODOO_USER / ODOO_API_KEY in env")

    common = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/common")
    uid = common.authenticate(db, user, auth, {})
    if not uid:
        raise RuntimeError("Odoo auth failed")

    models = xmlrpc.client.ServerProxy(f"{url.rstrip('/')}/xmlrpc/2/object")

    # Search for leads in "Installed" stage and stages after it:
    #   10 Installed, 11 Project Closed, 12 Permitting (+ Zoho equivalents)
    stage_ids = []
    for keyword in ["Installed", "Project Closed", "Permitting"]:
        ids = models.execute_kw(
            db, uid, auth,
            "crm.stage", "search",
            [[["name", "ilike", keyword]]],
        )
        stage_ids.extend(ids)
    # Deduplicate and exclude deprecated stages
    stage_ids = list(set(stage_ids))
    if stage_ids:
        stages_info = models.execute_kw(
            db, uid, auth,
            "crm.stage", "read",
            [stage_ids],
            {"fields": ["id", "name"]},
        )
        stage_ids = [s["id"] for s in stages_info if "DEPRECATED" not in s["name"]]

    if not stage_ids:
        print("WARNING: No matching stages found, fetching Won stages…")
        stage_ids = models.execute_kw(
            db, uid, auth,
            "crm.stage", "search",
            [[["is_won", "=", True]]],
        )

    if not stage_ids:
        raise RuntimeError("No Installed/Won stages found in Odoo")

    # Read stage names for reporting
    stages = models.execute_kw(
        db, uid, auth,
        "crm.stage", "read",
        [stage_ids],
        {"fields": ["name"]},
    )
    stage_names = {s["id"]: s["name"] for s in stages}
    print(f"Using stages: {stage_names}")

    # Fetch leads in those stages
    fields = [
        "id", "name", "partner_name", "contact_name",
        "email_from", "phone", "city", "stage_id",
        "x_studio_complete_address",
        "x_studio_city_municipality",
        "x_studio_province",
    ]

    lead_ids = models.execute_kw(
        db, uid, auth,
        "crm.lead", "search",
        [[["stage_id", "in", stage_ids]]],
    )
    print(f"Found {len(lead_ids)} leads (Installed + post-Installed stages)")

    leads = []
    batch = 200
    for i in range(0, len(lead_ids), batch):
        chunk = lead_ids[i:i + batch]
        recs = models.execute_kw(
            db, uid, auth,
            "crm.lead", "read",
            [chunk],
            {"fields": fields},
        )
        leads.extend(recs)
        print(f"  Fetched {len(leads)}/{len(lead_ids)}")

    return leads


# ── Solis ────────────────────────────────────────────────────────

async def fetch_all_solis_stations() -> List[Dict]:
    key_id = os.getenv("SOLIS_CLOUD_KEY_ID")
    key_secret = os.getenv("SOLIS_CLOUD_KEY_SECRET")
    if not key_id or not key_secret:
        raise RuntimeError("Missing SOLIS_CLOUD_KEY_ID / SOLIS_CLOUD_KEY_SECRET in env")

    client = SolisCloudClient(key_id=key_id, key_secret=key_secret)

    stations: List[Dict] = []
    page = 1
    while True:
        data = await client.list_stations(page_no=page, page_size=100)
        page_records = data.get("page", {}).get("records", [])
        if not page_records:
            break
        stations.extend(page_records)
        total = data.get("page", {}).get("total", 0)
        print(f"  Solis stations: fetched {len(stations)}/{total}")
        if len(stations) >= total:
            break
        page += 1

    return stations


# ── Matching ─────────────────────────────────────────────────────

def match_leads_to_stations(
    leads: List[Dict], stations: List[Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Returns:
        exact_matches   – lead matched to station with high confidence
        partial_matches – lead has plausible partial match
        unmatched_leads – no station found
    """
    exact = []
    partial = []
    unmatched = []

    for lead in leads:
        # Build candidate name strings from the lead
        lead_names = set()
        for key in ("partner_name", "contact_name", "name"):
            v = lead.get(key)
            if v and isinstance(v, str) and v.strip():
                lead_names.add(v.strip())

        # Also try city
        lead_city = (lead.get("city") or lead.get("x_studio_city_municipality") or "").strip()

        best_score = 0.0
        best_station = None
        best_lead_name = ""

        for station in stations:
            sname = station.get("stationName") or station.get("sno") or ""
            if not sname:
                continue

            for lname in lead_names:
                # Exact (case-insensitive)
                if normalize(lname) == normalize(sname):
                    best_score = 1.0
                    best_station = station
                    best_lead_name = lname
                    break

                # Check if lead name is contained in station name or vice versa
                nl, ns = normalize(lname), normalize(sname)
                if nl in ns or ns in nl:
                    score = 0.85
                else:
                    score = partial_ratio(lname, sname)

                # Boost if city appears in station name
                if lead_city and normalize(lead_city) in normalize(sname):
                    score = min(1.0, score + 0.15)

                if score > best_score:
                    best_score = score
                    best_station = station
                    best_lead_name = lname

            if best_score >= 1.0:
                break

        entry = {
            "lead_id": lead["id"],
            "lead_name": lead.get("name", ""),
            "partner_name": lead.get("partner_name", ""),
            "contact_name": lead.get("contact_name", ""),
            "email": lead.get("email_from", ""),
            "phone": lead.get("phone", ""),
            "city": lead_city,
            "stage": lead.get("stage_id", [None, ""])[1] if isinstance(lead.get("stage_id"), (list, tuple)) else "",
        }

        if best_score >= 0.8:
            entry["match_type"] = "exact" if best_score >= 1.0 else "high"
            entry["match_score"] = round(best_score, 2)
            entry["station_id"] = best_station["id"]
            entry["station_name"] = best_station.get("stationName", "")
            entry["station_sn"] = best_station.get("sno", "")
            entry["station_capacity_kw"] = best_station.get("capacity", "")
            entry["station_status"] = {1: "Online", 3: "Alarm"}.get(best_station.get("state"), "Offline")
            entry["matched_on"] = best_lead_name
            exact.append(entry)
        elif best_score >= 0.3:
            entry["match_type"] = "partial"
            entry["match_score"] = round(best_score, 2)
            entry["station_id"] = best_station["id"]
            entry["station_name"] = best_station.get("stationName", "")
            entry["station_sn"] = best_station.get("sno", "")
            entry["station_capacity_kw"] = best_station.get("capacity", "")
            entry["matched_on"] = best_lead_name
            partial.append(entry)
        else:
            entry["match_score"] = round(best_score, 2)
            unmatched.append(entry)

    return exact, partial, unmatched


# ── Main ─────────────────────────────────────────────────────────

async def main():
    print("=" * 70)
    print("ODOO ↔ SOLIS STATION MATCHING")
    print("=" * 70)

    print("\n[1/2] Fetching installed leads from Odoo…")
    leads = fetch_odoo_installed_leads()

    print(f"\n[2/2] Fetching all stations from Solis Cloud…")
    stations = await fetch_all_solis_stations()

    print(f"\n{'=' * 70}")
    print(f"DATA SUMMARY")
    print(f"  Odoo installed leads: {len(leads)}")
    print(f"  Solis stations:       {len(stations)}")
    print(f"{'=' * 70}")

    exact, partial, unmatched = match_leads_to_stations(leads, stations)

    # ── Print results ──
    print(f"\n{'━' * 70}")
    print(f"EXACT / HIGH-CONFIDENCE MATCHES ({len(exact)})")
    print(f"{'━' * 70}")
    for m in sorted(exact, key=lambda x: -x["match_score"]):
        print(f"  ✅ [{m['match_score']}] Lead #{m['lead_id']} \"{m['partner_name']}\"")
        print(f"     → Station \"{m['station_name']}\" (ID: {m['station_id']}, SN: {m['station_sn']}, {m.get('station_capacity_kw','')} kWp, {m.get('station_status','')})")
        print(f"     Matched on: \"{m['matched_on']}\"")
        print()

    print(f"\n{'━' * 70}")
    print(f"PARTIAL MATCHES ({len(partial)})  — review manually")
    print(f"{'━' * 70}")
    for m in sorted(partial, key=lambda x: -x["match_score"]):
        print(f"  🔶 [{m['match_score']}] Lead #{m['lead_id']} \"{m['partner_name']}\"")
        print(f"     → Station \"{m['station_name']}\" (ID: {m['station_id']}, SN: {m['station_sn']})")
        print(f"     Matched on: \"{m['matched_on']}\"")
        print()

    print(f"\n{'━' * 70}")
    print(f"UNMATCHED LEADS ({len(unmatched)})  — no Solis station found")
    print(f"{'━' * 70}")
    for m in unmatched:
        names = f"partner=\"{m['partner_name']}\"" if m['partner_name'] else ""
        if m['contact_name']:
            names += f" contact=\"{m['contact_name']}\""
        print(f"  ❌ Lead #{m['lead_id']} {names}  city={m['city']}")

    # ── Unmatched stations ──
    matched_station_ids = set(
        m["station_id"] for m in exact + partial
    )
    unmatched_stations = [s for s in stations if s["id"] not in matched_station_ids]
    print(f"\n{'━' * 70}")
    print(f"UNMATCHED SOLIS STATIONS ({len(unmatched_stations)})  — no Odoo lead found")
    print(f"{'━' * 70}")
    for s in unmatched_stations:
        status = {1: "Online", 3: "Alarm"}.get(s.get("state"), "Offline")
        print(f"  📡 \"{s.get('stationName','')}\" (ID: {s['id']}, SN: {s.get('sno','')}, {s.get('capacity','')} kWp, {status})")

    # ── Summary analytics ──
    total_leads = len(leads)
    total_stations = len(stations)
    print(f"\n{'=' * 70}")
    print("ANALYTICS")
    print(f"{'=' * 70}")
    print(f"  Total Odoo installed leads:     {total_leads}")
    print(f"  Total Solis stations:            {total_stations}")
    print(f"  Exact/high matches:              {len(exact)}  ({len(exact)/max(total_leads,1)*100:.0f}% of leads)")
    print(f"  Partial matches:                 {len(partial)}  ({len(partial)/max(total_leads,1)*100:.0f}% of leads)")
    print(f"  Unmatched leads:                 {len(unmatched)}  ({len(unmatched)/max(total_leads,1)*100:.0f}% of leads)")
    print(f"  Unmatched Solis stations:        {len(unmatched_stations)}  ({len(unmatched_stations)/max(total_stations,1)*100:.0f}% of stations)")
    print(f"  Match coverage:                  {(len(exact)+len(partial))/max(total_leads,1)*100:.0f}% of leads have a possible station")
    print(f"{'=' * 70}")

    # Save JSON report
    report = {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "odoo_installed_leads": total_leads,
            "solis_stations": total_stations,
            "exact_matches": len(exact),
            "partial_matches": len(partial),
            "unmatched_leads": len(unmatched),
            "unmatched_stations": len(unmatched_stations),
        },
        "exact_matches": exact,
        "partial_matches": partial,
        "unmatched_leads": unmatched,
        "unmatched_stations": [
            {
                "station_id": s["id"],
                "station_name": s.get("stationName", ""),
                "station_sn": s.get("sno", ""),
                "capacity_kw": s.get("capacity", ""),
                "status": {1: "Online", 3: "Alarm"}.get(s.get("state"), "Offline"),
            }
            for s in unmatched_stations
        ],
    }

    out_path = os.path.join(_here, "..", "odoo_solis_match_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nFull report saved to: {os.path.abspath(out_path)}")


if __name__ == "__main__":
    asyncio.run(main())
