"""
Rotate the customer passwords burned by the public repo.

`api/provision_users.py:37` hardcoded a DEFAULT_PASSWORD literal (not repeated
here — it is in git history and does not need another copy). That line has been
on origin/main of github.com/solvivaenergy/monitoring since commit ed55c3c
(2026-03-26), and the repo is public. Every account created by
provision_users.py / provision_from_csv.py was given that password.

An account that has never signed in has never changed its password. As measured
2026-09-14: 620 auth users, 565 with last_sign_in_at = null. Those 565 logins
are openable by anyone who reads the repo.

This script sets a fresh random password on each affected account, which makes
the leaked string useless, and optionally emails the customer a recovery link so
they can choose their own.

    python -m api.rotate_leaked_passwords                      # dry-run
    python -m api.rotate_leaked_passwords --apply              # rotate only
    python -m api.rotate_leaked_passwords --apply --send-reset # rotate + email

Scope: by default only accounts that have NEVER signed in, because those are the
ones provably still holding the leaked password. --all-users rotates everybody,
which is the safer reading of "assume compromised" but will lock out the 55
customers who have set their own password until they use the reset link. Do not
pass --all-users without --send-reset.

The constant has since been removed from provision_users.py and
provision_from_csv.py (2026-09-16); both now require DEFAULT_USER_PASSWORD in the
environment and fail loudly without it. Rotating does not un-leak the string: it
remains in git history forever and must never be used again.
"""

import argparse
import csv
import os
import secrets
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

import httpx
from dotenv import load_dotenv

load_dotenv()

ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789!@#$%^&*-_"


def _env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def new_password(length: int = 24) -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(length))


def list_users(url: str, headers: Dict[str, str]) -> List[dict]:
    users: List[dict] = []
    page = 1
    while page <= 50:
        r = httpx.get(
            f"{url}/auth/v1/admin/users?page={page}&per_page=200",
            headers=headers, timeout=120,
        )
        r.raise_for_status()
        batch = r.json().get("users", [])
        if not batch:
            break
        users.extend(batch)
        page += 1
    return users


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--apply", action="store_true",
                    help="Actually rotate. Without it this is a dry-run.")
    ap.add_argument("--send-reset", action="store_true",
                    help="Also email each customer a password-recovery link.")
    ap.add_argument("--all-users", action="store_true",
                    help="Rotate every account, not just never-signed-in ones.")
    ap.add_argument("--exclude", default="",
                    help="Comma-separated emails to skip (e.g. your own staff logins).")
    args = ap.parse_args()

    if args.all_users and not args.send_reset:
        ap.error(
            "--all-users without --send-reset would lock out the customers who "
            "already set their own password, with no way back in. Add --send-reset."
        )

    url = _env("SUPABASE_URL").rstrip("/")
    key = _env("SUPABASE_SERVICE_KEY")
    headers = {"apikey": key, "Authorization": f"Bearer {key}",
               "Content-Type": "application/json"}

    excluded = {e.strip().lower() for e in args.exclude.split(",") if e.strip()}

    users = list_users(url, headers)
    never = [u for u in users if not u.get("last_sign_in_at")]
    targets = users if args.all_users else never
    targets = [u for u in targets if (u.get("email") or "").lower() not in excluded]

    print("=" * 72)
    print(f"Credential rotation — {'LIVE' if args.apply else 'DRY-RUN'}")
    print("=" * 72)
    print(f"  total auth users        : {len(users)}")
    print(f"  never signed in         : {len(never)}")
    print(f"  excluded by --exclude   : {len(excluded)}")
    print(f"  TO ROTATE               : {len(targets)}")
    print(f"  send recovery email     : {args.send_reset}")
    print()

    if not args.apply:
        for u in targets[:10]:
            print(f"    would rotate {u.get('email')}")
        if len(targets) > 10:
            print(f"    ... and {len(targets) - 10} more")
        print()
        print("Dry-run only — nothing was changed. Re-run with --apply.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    report_path = f"password_rotation_{stamp}.csv"
    rotated = failed = mailed = 0

    with open(report_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        # The new passwords are deliberately NOT written to this file. Storing
        # 565 live credentials in a CSV in the repo directory would recreate the
        # exact problem being fixed. Customers regain access via the recovery
        # email; anyone without one needs an admin-issued link.
        w.writerow(["email", "user_id", "rotated", "reset_email_sent", "error"])

        for i, u in enumerate(targets, 1):
            uid, email = u["id"], (u.get("email") or "")
            err = ""
            ok = sent = False
            try:
                r = httpx.put(
                    f"{url}/auth/v1/admin/users/{uid}",
                    headers=headers,
                    json={"password": new_password()},
                    timeout=60,
                )
                r.raise_for_status()
                ok = True
                rotated += 1
            except Exception as exc:
                err = str(exc)[:200]
                failed += 1

            if ok and args.send_reset and email:
                try:
                    r = httpx.post(
                        f"{url}/auth/v1/recover",
                        headers={"apikey": key, "Content-Type": "application/json"},
                        json={"email": email},
                        timeout=60,
                    )
                    r.raise_for_status()
                    sent = True
                    mailed += 1
                except Exception as exc:
                    err = (err + " | reset: " + str(exc)[:120]).strip(" |")

            w.writerow([email, uid, ok, sent, err])

            if i % 25 == 0:
                print(f"  {i}/{len(targets)} processed...")
            # GoTrue rate-limits the recovery endpoint far more tightly than the
            # admin API. Without this, bulk sends start returning 429 and the
            # customers behind them silently never get their email.
            if args.send_reset:
                time.sleep(1.0)

    print()
    print(f"  rotated  : {rotated}")
    print(f"  failed   : {failed}")
    print(f"  emailed  : {mailed}")
    print(f"  report   : {report_path}")
    print()
    print("Next: delete DEFAULT_PASSWORD from api/provision_users.py and")
    print("api/provision_from_csv.py, and read it from DEFAULT_USER_PASSWORD")
    print("as api/onboard_from_station_csv.py:45 already does.")


if __name__ == "__main__":
    main()
