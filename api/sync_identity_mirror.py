"""
Mirror customer-identity fields from Odoo and Solis into OUR cached columns.

    solar_systems.odoo_lead_id / odoo_lead_email / odoo_lead_name / odoo_stage
    solar_systems.solis_plant_name / solis_user_email
    user_profiles.odoo_partner_id / odoo_email / odoo_customer_name

Migration 04a created these columns so Monitoring Admin could show, side by
side, what each system believes about a customer — and where they disagree.
Nothing populated them: on 2026-09-17 all four odoo_* columns were empty on
every one of 627 rows. This job fills them and keeps them current.

READ-ONLY toward Odoo and Solis. Odoo is the sales team's source of truth and is
never written from this project (see api/monitoring_admin_routes.py); this copies
Odoo's values into our database so they can be displayed and compared. Values
are mirrored as they are — a malformed email in Odoo is shown malformed here,
because that is exactly what an engineer needs to see.

Keyed by Solis station id: an Odoo lead is matched to a solar_systems row by
the lead's "CRM Design Solis Station ID" field; partner fields go onto the
profile of the station's owner (via the primary system). Writes go through
db.audited() with source='sync_identity_mirror', so the 08 audit trigger
records every changed value as actor_kind='job'. Only rows whose values differ
are touched.

    python -m api.sync_identity_mirror            # dry-run: counts only
    python -m api.sync_identity_mirror --apply    # write

Runs nightly at the end of api.sync_to_supabase; safe to run any time.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from api import db  # noqa: E402
from api.onboard_from_odoo import DEFAULT_FIELD_NAME, _build_odoo_config, _connect_odoo  # noqa: E402

log = logging.getLogger("identity_mirror")

LEAD_FIELDS = ["id", "name", "partner_name", "contact_name", "email_from", "stage_id",
               "partner_id", "write_date", DEFAULT_FIELD_NAME]
PARTNER_FIELDS = ["id", "name", "email"]


def _clean(v: Any) -> Optional[str]:
    if v in (None, False, ""):
        return None
    s = str(v).strip()
    return s or None


def fetch_odoo() -> Dict[str, Dict[str, Any]]:
    """{station_id: lead-with-partner} for every lead carrying a station id.
    If two leads carry the same id (a data error sales must fix), the newest
    lead wins and the collision is logged."""
    config = _build_odoo_config()
    uid, models = _connect_odoo(config)
    kw = dict(fields=LEAD_FIELDS)
    ids = models.execute_kw(config.db, uid, config.auth, "crm.lead", "search",
                            [[[DEFAULT_FIELD_NAME, "!=", False]]])
    leads: List[Dict[str, Any]] = []
    for i in range(0, len(ids), 200):
        leads.extend(models.execute_kw(config.db, uid, config.auth, "crm.lead", "read", [ids[i:i + 200]], kw))

    partner_ids = sorted({l["partner_id"][0] for l in leads if l.get("partner_id")})
    partners: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(partner_ids), 200):
        for p in models.execute_kw(config.db, uid, config.auth, "res.partner", "read",
                                   [partner_ids[i:i + 200]], dict(fields=PARTNER_FIELDS)):
            partners[p["id"]] = p

    by_station: Dict[str, Dict[str, Any]] = {}
    for lead in sorted(leads, key=lambda l: l["id"]):
        sid = _clean(lead.get(DEFAULT_FIELD_NAME))
        if not sid:
            continue
        if sid in by_station:
            log.warning("Odoo: station %s is on leads %s and %s — keeping the newer",
                        sid, by_station[sid]["lead_id"], lead["id"])
        partner = partners.get(lead["partner_id"][0]) if lead.get("partner_id") else None
        by_station[sid] = {
            "lead_id": lead["id"],
            "lead_email": _clean(lead.get("email_from")),
            "lead_name": _clean(lead.get("partner_name")) or _clean(lead.get("contact_name")) or _clean(lead.get("name")),
            "stage": _clean(lead["stage_id"][1]) if lead.get("stage_id") else None,
            "partner_id": partner["id"] if partner else None,
            "partner_email": _clean(partner.get("email")) if partner else None,
            "partner_name": _clean(partner.get("name")) if partner else None,
        }
    log.info("Odoo: %d leads with a station id -> %d distinct stations, %d partners",
             len(leads), len(by_station), len(partners))
    return by_station


async def fetch_solis() -> Dict[str, Dict[str, Any]]:
    """{station_id: {plant_name, user_email}} from the account roster."""
    from api.validation_routes import _fetch_roster  # walks userStationList, ~7 calls
    roster = await _fetch_roster()
    out = {str(sid): {"plant_name": _clean(rec.get("stationName")), "user_email": _clean(rec.get("userEmail"))}
           for sid, rec in roster.items()}
    log.info("Solis: %d stations in roster, %d with a userEmail",
             len(out), sum(1 for v in out.values() if v["user_email"]))
    return out


async def run(apply: bool) -> Dict[str, int]:
    odoo = fetch_odoo()
    solis = await fetch_solis()
    now = datetime.now(timezone.utc)

    with db.connect(autocommit=True) as conn:
        systems = conn.execute(
            """select id, user_id, solis_station_id, is_primary,
                      odoo_lead_id, odoo_lead_email, odoo_lead_name, odoo_stage,
                      solis_plant_name, solis_user_email
                 from public.solar_systems where solis_station_id is not null""").fetchall()
        profiles = {p["id"]: p for p in conn.execute(
            "select id, odoo_partner_id, odoo_email, odoo_customer_name from public.user_profiles").fetchall()}

    sys_updates: List[tuple] = []
    prof_updates: Dict[Any, tuple] = {}
    # Rows whose values were confirmed tonight, changed or not. Monitoring Admin
    # shows odoo_synced_at / solis_synced_at as "last synced", so the stamp must
    # mean "the copy was checked against the source", not "a value changed" —
    # the first version only stamped changed rows, and every unchanged row
    # kept showing the date of the first fill.
    odoo_ok_systems: List[Any] = []
    solis_ok_systems: List[Any] = []
    odoo_ok_profiles: List[Any] = []
    counts = {"systems_seen": len(systems), "systems_changed": 0, "profiles_changed": 0,
              "stations_in_odoo": 0, "stations_in_solis": 0}

    for s in systems:
        sid = str(s["solis_station_id"])
        o, z = odoo.get(sid), solis.get(sid)
        counts["stations_in_odoo"] += bool(o)
        counts["stations_in_solis"] += bool(z)
        if o:
            odoo_ok_systems.append(s["id"])
        if z:
            solis_ok_systems.append(s["id"])
        new = {
            "odoo_lead_id": o["lead_id"] if o else s["odoo_lead_id"],
            "odoo_lead_email": o["lead_email"] if o else s["odoo_lead_email"],
            "odoo_lead_name": o["lead_name"] if o else s["odoo_lead_name"],
            "odoo_stage": o["stage"] if o else s["odoo_stage"],
            "solis_plant_name": (z["plant_name"] if z else None) or s["solis_plant_name"],
            "solis_user_email": z["user_email"] if z else s["solis_user_email"],
        }
        if any(new[k] != s[k] for k in new):
            sys_updates.append((new["odoo_lead_id"], new["odoo_lead_email"], new["odoo_lead_name"], new["odoo_stage"],
                                now if o else None, new["solis_plant_name"], new["solis_user_email"],
                                now if z else None, s["id"]))
        if o and s["is_primary"] and s["user_id"] in profiles:
            p = profiles[s["user_id"]]
            odoo_ok_profiles.append(p["id"])
            pn = {"odoo_partner_id": o["partner_id"],
                  "odoo_email": o["partner_email"] or o["lead_email"],
                  "odoo_customer_name": o["partner_name"] or o["lead_name"]}
            if any(pn[k] != p[k] for k in pn):
                prof_updates[p["id"]] = (pn["odoo_partner_id"], pn["odoo_email"], pn["odoo_customer_name"], now, p["id"])

    counts["systems_changed"], counts["profiles_changed"] = len(sys_updates), len(prof_updates)
    log.info("mirror: %s", counts)
    if not apply:
        log.info("dry-run — nothing written. Pass --apply to write.")
        return counts

    with db.audited(None, None, "nightly identity mirror from Odoo + Solis", source="sync_identity_mirror") as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """update public.solar_systems
                      set odoo_lead_id = %s, odoo_lead_email = %s, odoo_lead_name = %s, odoo_stage = %s,
                          odoo_synced_at = coalesce(%s, odoo_synced_at),
                          solis_plant_name = %s, solis_user_email = %s,
                          solis_synced_at = coalesce(%s, solis_synced_at),
                          updated_at = now()
                    where id = %s""", sys_updates)
            cur.executemany(
                """update public.user_profiles
                      set odoo_partner_id = %s, odoo_email = %s, odoo_customer_name = %s,
                          odoo_synced_at = %s, updated_at = now()
                    where id = %s""", list(prof_updates.values()))
            # Freshness stamp for the unchanged rows. The *_synced_at columns are
            # not in the 08 audit trigger's list and updated_at is left alone, so
            # this writes no audit rows and does not make the rows look edited.
            cur.execute("update public.solar_systems set odoo_synced_at = %s where id = any(%s)",
                        (now, odoo_ok_systems))
            cur.execute("update public.solar_systems set solis_synced_at = %s where id = any(%s)",
                        (now, solis_ok_systems))
            cur.execute("update public.user_profiles set odoo_synced_at = %s where id = any(%s)",
                        (now, odoo_ok_profiles))
    log.info("mirror: wrote %d system row(s), %d profile row(s); stamped %d/%d systems (odoo/solis), %d profiles",
             len(sys_updates), len(prof_updates), len(odoo_ok_systems), len(solis_ok_systems), len(odoo_ok_profiles))
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="Mirror Odoo/Solis identity fields into our cached columns (read-only toward Odoo/Solis).")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    counts = asyncio.run(run(apply=args.apply))
    print(counts)


if __name__ == "__main__":
    sys.exit(main())
