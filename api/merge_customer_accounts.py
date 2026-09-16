"""
Merge split customer accounts so one customer holds several Solis stations.

Some customers own more than one installation. Because `user_profiles` carried a
single `solis_station_id`, the only way to represent that was a second login, so
the fleet accumulated two kinds of breakage:

  SPLIT_ACCOUNTS  two auth users for one human, one station each. The portal
                  shows each login half the story, and billing/referrals, which
                  are keyed on user_id, are split across both.
  ADD_STATION     one auth user, and the second station never onboarded at all
                  because onboard_from_odoo skips any lead whose email it has
                  already seen (`duplicate_email: 1` on 12 consecutive runs).

This script fixes the first kind. It moves every row belonging to the loser
account onto the survivor, leaving the loser with nothing, then optionally bans
the loser's login. The second kind needs no merge — just a solar_systems row —
and is handled by --add-station.

NOTHING IS AUTO-DISCOVERED. Merging customer accounts is destructive and
customer-visible, and the signals disagree: grouping by Odoo partner_id misses
customers with duplicate res.partner records, while grouping by email wrongly
fuses different people who share one (megaportcustomsbrokerage@yahoo.com covers
Arlene Sollano AND Merian Misula; camperandcabin@gmail.com covers two different
Canonizados; renewableph@gmail.com is an installer account spanning three
unrelated customers). So this takes an explicit plan file that a human has read.

Generate a candidate plan to review:
    python -m api.merge_customer_accounts --suggest > merge_plan.json

Then, after editing it:
    python -m api.merge_customer_accounts --plan merge_plan.json            # dry-run
    python -m api.merge_customer_accounts --plan merge_plan.json --apply

*** ORDERING: run this AFTER sql/2026-09-14_06_drop_old_conflict_key.sql. ***
energy_readings has UNIQUE(user_id, "timestamp") until file 06 drops it. Both
accounts in a merge almost always have a reading at the same 04:00Z daily
timestamp, so re-pointing user_id would violate that key and abort. The script
detects the collision and refuses rather than letting Postgres fail halfway.

Plan format:
    {
      "merges": [
        {"survivor": "<uuid>", "losers": ["<uuid>"], "note": "William Dionisio"}
      ],
      "add_stations": [
        {"user_id": "<uuid>", "solis_station_id": "1298...", "note": "Chavez 2nd"}
      ]
    }
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

# Tables carrying a user_id that must follow the customer to the survivor.
# Order matters only for readability; none of these reference each other.
#
# `referrals` is listed separately because its column is referrer_user_id, and
# `solar_systems` is moved first so that a partial failure leaves readings
# pointing at a station that is already on the survivor rather than the reverse.
USER_TABLES = [
    ("solar_systems", "user_id"),
    ("energy_readings", "user_id"),
    ("energy_readings_five_minutes", "user_id"),
    ("cleaned_data", "user_id"),
    ("billing_records", "user_id"),
    ("support_tickets", "user_id"),
    ("ticket_messages", "user_id"),
    ("energy_tips", "user_id"),
    ("referrals", "referrer_user_id"),
]


def _env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


class Supa:
    def __init__(self) -> None:
        self.url = _env("SUPABASE_URL").rstrip("/")
        key = _env("SUPABASE_SERVICE_KEY")
        self.h = {"apikey": key, "Authorization": f"Bearer {key}"}

    def get(self, table: str, query: str = "", limit: int = 100000) -> List[dict]:
        rows: List[dict] = []
        offset = 0
        page = 1000
        while True:
            url = f"{self.url}/rest/v1/{table}?{query}"
            r = httpx.get(
                url,
                headers={**self.h, "Range": f"{offset}-{offset + page - 1}"},
                timeout=120,
            )
            r.raise_for_status()
            batch = r.json()
            rows.extend(batch)
            if len(batch) < page or len(rows) >= limit:
                break
            offset += page
        return rows

    def count(self, table: str, query: str) -> int:
        r = httpx.get(
            f"{self.url}/rest/v1/{table}?{query}&select=*",
            headers={**self.h, "Prefer": "count=exact", "Range": "0-0"},
            timeout=120,
        )
        r.raise_for_status()
        cr = r.headers.get("content-range", "*/0")
        return int(cr.split("/")[-1]) if "/" in cr else 0

    def patch(self, table: str, query: str, payload: dict) -> int:
        r = httpx.patch(
            f"{self.url}/rest/v1/{table}?{query}",
            headers={**self.h, "Content-Type": "application/json",
                     "Prefer": "return=representation"},
            json=payload,
            timeout=300,
        )
        r.raise_for_status()
        return len(r.json())

    def auth_users(self) -> List[dict]:
        users: List[dict] = []
        page = 1
        while page <= 20:
            r = httpx.get(
                f"{self.url}/auth/v1/admin/users?page={page}&per_page=200",
                headers=self.h, timeout=120,
            )
            r.raise_for_status()
            batch = r.json().get("users", [])
            if not batch:
                break
            users.extend(batch)
            page += 1
        return users

    def ban_user(self, user_id: str) -> None:
        # A banned user keeps their row and their history stays attributable,
        # but the login stops working. Deleting the auth user would cascade in
        # ways we do not control and destroys the audit trail of the merge.
        r = httpx.put(
            f"{self.url}/auth/v1/admin/users/{user_id}",
            headers={**self.h, "Content-Type": "application/json"},
            json={"ban_duration": "876000h"},  # ~100 years
            timeout=60,
        )
        r.raise_for_status()


def collision_check(sb: Supa, survivor: str, loser: str) -> Dict[str, int]:
    """Count (user_id, timestamp) collisions that would break the OLD unique key.

    Returns {} when the merge is safe under either key. A non-empty result means
    sql/2026-09-14_06_drop_old_conflict_key.sql has not been applied yet, or the
    new (system_id, timestamp) index is missing.
    """
    out: Dict[str, int] = {}
    for table in ("energy_readings", "energy_readings_five_minutes"):
        s_ts = {r["timestamp"] for r in sb.get(table, f"select=timestamp&user_id=eq.{survivor}")}
        l_ts = {r["timestamp"] for r in sb.get(table, f"select=timestamp&user_id=eq.{loser}")}
        overlap = s_ts & l_ts
        if overlap:
            out[table] = len(overlap)
    return out


def do_merge(sb: Supa, survivor: str, losers: List[str], note: str,
             apply: bool, ban: bool) -> dict:
    result = {"survivor": survivor, "losers": losers, "note": note, "moved": {}, "blocked": None}

    for loser in losers:
        if loser == survivor:
            result["blocked"] = "survivor listed as its own loser"
            return result

        collisions = collision_check(sb, survivor, loser)
        if collisions:
            result["blocked"] = (
                f"{collisions} overlapping (user_id, timestamp) row(s). "
                "Apply sql/2026-09-14_06_drop_old_conflict_key.sql first — the old "
                "UNIQUE(user_id, timestamp) would reject this merge partway through."
            )
            return result

    for loser in losers:
        for table, col in USER_TABLES:
            n = sb.count(table, f"{col}=eq.{loser}")
            if not n:
                continue
            key = f"{table}.{col}"
            result["moved"][key] = result["moved"].get(key, 0) + n
            if apply:
                sb.patch(table, f"{col}=eq.{loser}", {col: survivor})

        if apply and ban:
            sb.ban_user(loser)
            result["moved"]["auth.banned"] = result["moved"].get("auth.banned", 0) + 1

    return result


def do_add_station(sb: Supa, user_id: str, station_id: str, note: str, apply: bool) -> dict:
    """Attach an un-onboarded station to an existing customer.

    Relies on solar_systems.solis_station_id (migration 04) and its
    UNIQUE(user_id, solis_station_id), which makes this idempotent.
    """
    existing = sb.get("solar_systems", f"select=id,user_id&solis_station_id=eq.{station_id}")
    if existing:
        return {"user_id": user_id, "solis_station_id": station_id, "note": note,
                "action": "already_exists", "system_id": existing[0]["id"]}

    if not apply:
        return {"user_id": user_id, "solis_station_id": station_id, "note": note,
                "action": "would_create"}

    r = httpx.post(
        f"{sb.url}/rest/v1/solar_systems",
        headers={**sb.h, "Content-Type": "application/json", "Prefer": "return=representation"},
        json={
            "user_id": user_id,
            "solis_station_id": station_id,
            "system_name": note or station_id,
            "status": "active",
            # capacity/installation_date are left for the nightly sync to fill
            # from Solis; inventing values here would look like real data.
        },
        timeout=60,
    )
    r.raise_for_status()
    return {"user_id": user_id, "solis_station_id": station_id, "note": note,
            "action": "created", "system_id": r.json()[0]["id"]}


def suggest(sb: Supa) -> dict:
    """Emit a CANDIDATE plan for a human to review. Never run this blind.

    Groups by Solis owner email, which is the closest thing to ground truth for
    "one account owns these plants" — but it is not authoritative, so known
    installer/shared mailboxes are excluded and every group is emitted with the
    evidence needed to judge it.
    """
    # Mailboxes that provably cover MORE THAN ONE customer. Grouping on these
    # would merge unrelated people's data together — the one irreversible
    # mistake this script can make.
    SHARED = {
        "renewableph@gmail.com",        # installer; 3 unrelated customers
        "camperandcabin@gmail.com",     # two different Canonizados
        "megaportcustomsbrokerage@yahoo.com",  # Arlene Sollano + Merian Misula
    }

    profiles = sb.get("user_profiles", "select=id,full_name,solis_station_id")
    by_station = {
        str(p["solis_station_id"]).strip(): p
        for p in profiles if p.get("solis_station_id")
    }
    emails = {u["id"]: (u.get("email") or "").lower() for u in sb.auth_users()}

    # solar_systems.solis_station_id only exists once migration 04 is applied.
    # --suggest must still work before that, so the merge half of the plan can
    # be reviewed while the migration is still being scheduled; only the
    # add_stations half needs the column.
    try:
        systems = sb.get("solar_systems", "select=id,user_id,solis_station_id")
        station_to_system = {
            str(s["solis_station_id"]).strip(): s
            for s in systems if s.get("solis_station_id")
        }
        have_station_column = True
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 400:
            raise
        station_to_system = {}
        have_station_column = False
        print(
            "NOTE: solar_systems.solis_station_id does not exist yet, so "
            "add_stations cannot be computed. Apply "
            "sql/2026-09-14_04_station_identity.sql, then re-run --suggest. "
            "The merges below are unaffected.",
            file=sys.stderr,
        )

    merges: List[dict] = []
    add_stations: List[dict] = []
    seen_groups = set()

    # Reuse the reconciliation CSV if present — it already carries the Solis
    # owner email per station, so this needs no Solis calls.
    recon_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "station_master_reconciliation_2026-09-14.csv",
    )
    if not os.path.exists(recon_path):
        raise SystemExit(
            f"Expected {recon_path}. Regenerate the reconciliation first — "
            "--suggest reads the Solis owner email from it."
        )

    import csv as _csv
    groups: Dict[str, List[dict]] = defaultdict(list)
    with open(recon_path, encoding="utf-8-sig") as fh:
        for row in _csv.DictReader(fh):
            se = (row.get("solis_email") or "").strip().lower()
            if se and se not in SHARED:
                groups[se].append(row)

    for email, rows in sorted(groups.items()):
        stations = sorted({r["solis_station_id"] for r in rows})
        if len(stations) < 2:
            continue
        key = tuple(stations)
        if key in seen_groups:
            continue
        seen_groups.add(key)

        owners = {}
        for sid in stations:
            p = by_station.get(sid)
            if p:
                owners[sid] = p["id"]

        distinct = sorted(set(owners.values()))
        note = rows[0].get("solis_plant_name") or email

        if len(distinct) > 1:
            # Survivor = the account holding the most readings, so the smaller
            # history is the one that moves.
            counts = {u: sb.count("energy_readings", f"user_id=eq.{u}") for u in distinct}
            survivor = max(counts, key=lambda u: counts[u])
            merges.append({
                "survivor": survivor,
                "losers": [u for u in distinct if u != survivor],
                "note": note,
                "_evidence": {
                    "solis_owner_email": email,
                    "stations": stations,
                    "reading_counts": counts,
                    "logins": {u: emails.get(u, "?") for u in distinct},
                },
            })
        elif len(distinct) == 1 and have_station_column:
            owner = distinct[0]
            for sid in stations:
                if sid not in station_to_system:
                    add_stations.append({
                        "user_id": owner,
                        "solis_station_id": sid,
                        "note": note,
                        "_evidence": {"solis_owner_email": email,
                                      "login": emails.get(owner, "?")},
                    })

    return {"merges": merges, "add_stations": add_stations}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suggest", action="store_true",
                    help="Print a candidate plan as JSON for review. Writes nothing.")
    ap.add_argument("--plan", help="Path to a reviewed plan file.")
    ap.add_argument("--apply", action="store_true",
                    help="Execute the plan. Without this the run is a dry-run.")
    ap.add_argument("--no-ban", action="store_true",
                    help="Leave the loser logins active after merging.")
    args = ap.parse_args()

    sb = Supa()

    if args.suggest:
        print(json.dumps(suggest(sb), indent=2))
        return

    if not args.plan:
        ap.error("one of --suggest or --plan is required")

    with open(args.plan, encoding="utf-8") as fh:
        plan = json.load(fh)

    mode = "LIVE" if args.apply else "DRY-RUN"
    print("=" * 72)
    print(f"Customer account merge — {mode}")
    print("=" * 72)

    blocked = 0
    for entry in plan.get("merges", []):
        res = do_merge(sb, entry["survivor"], entry.get("losers", []),
                       entry.get("note", ""), args.apply, not args.no_ban)
        label = res["note"] or res["survivor"]
        if res["blocked"]:
            blocked += 1
            print(f"  BLOCKED  {label}: {res['blocked']}")
        else:
            moved = ", ".join(f"{k}={v}" for k, v in sorted(res["moved"].items())) or "nothing"
            print(f"  {'merged ' if args.apply else 'would  '} {label}: {moved}")

    for entry in plan.get("add_stations", []):
        res = do_add_station(sb, entry["user_id"], entry["solis_station_id"],
                             entry.get("note", ""), args.apply)
        print(f"  {res['action']:16} {res['note']}  station={res['solis_station_id']}")

    print("-" * 72)
    if blocked:
        print(f"{blocked} merge(s) BLOCKED. Apply sql/2026-09-14_06_drop_old_conflict_key.sql, "
              "then re-run.")
    if not args.apply:
        print("Dry-run only — nothing was written. Re-run with --apply.")


if __name__ == "__main__":
    main()
