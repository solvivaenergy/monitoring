# 2026-09-14 migration series — what is applied to production

Supabase project `kzsocvzhbgtfyksrjmvk`. Update this file whenever you run one.

These were applied over a direct Postgres connection (`SUPABASE_DB_URL`, psycopg
with `autocommit=True`), **not** the Supabase SQL editor. The editor wraps a
submission in a transaction, which makes `CREATE INDEX CONCURRENTLY` fail with
`25001` and silently drops `create temp table ... on commit drop` between
statements (`42P01`). Every `CONCURRENTLY` build in this series is on a table
that is written continuously; do not work around 25001 by deleting the keyword.

| file | status | applied |
|---|---|---|
| 00 preflight (read-only) | run | 2026-09-14 |
| 01 missing indexes | **applied** | 2026-09-15 |
| 02 repair orphans | **applied** | 2026-09-14 |
| 03 dedup solar_systems | **applied** — 719 → 615 rows | 2026-09-15 |
| 04a identity columns + seed | **applied** | 2026-09-15 |
| 04b identity indexes (9) | **applied**, all concurrent, none invalid | 2026-09-15 |
| 04c primary flag + triggers | **applied** — 4 triggers live | 2026-09-15 |
| 05 new conflict key | **applied** | 2026-09-16 |
| — code deploy — | `main` = `b05a30b`, live on Render | 2026-09-16 |
| 06 drop old conflict key | **applied** — old `(user_id,timestamp)` keys gone, `user_id` indexes rebuilt | 2026-09-16 |
| 07 foreign keys | **applied — as corrected**; file rewritten to match (see its header) | 2026-09-16 |
| 08 staff and audit | **applied** — `staff_users`, `audit_log`, 3 audit triggers | 2026-09-16 |
| 09 backfill jobs | **applied** — `backfill_jobs` + one-live-job guard | 2026-09-16 |
| 10 station-scoped RLS | **applied** — old policies recorded in `rls_policies_ASBUILT_2026-09-16.sql`; verified as a real customer, a stranger, and anon | 2026-09-16 |
| 11 view security | **applied** — six views: anon/authenticated revoked, `security_invoker=on`; `system_metrics` created. As-built DDL in `monthly_energy_sync_views_ASBUILT.sql` | 2026-09-16 |
| 12 legacy timestamp quarantine (optional) | not run — see below | |
| **13 readings quarantine** (`2026-09-17_13_readings_quarantine.sql`) | **applied** — `energy_readings_quarantine`, used by the back office's Remap to hold readings captured under a wrong station id (recoverable, never deleted) | 2026-09-17 |
| **14 audit_log append-only trigger** (`2026-09-17_14_audit_log_append_only_trigger.sql`) | **applied** — replaces 08's rewrite rules, which had made **every auth user undeletable** (the FK's ON DELETE SET NULL was rewritten to nothing). File 08 updated to match. | 2026-09-17 |

## Verified state after 11 (2026-09-16 ~15:40 UTC)

```
energy_readings                116,121 rows   0 duplicate keys, 0 orphans, 0 owner mismatches
energy_readings_five_minutes   ~156k rows     rolling one Manila day; cron writes every 15 min
solar_systems                      625 rows   625 customers, 619 station ids all distinct
user_profiles                      627        auth.users 630 (2 internal logins banned 2026-09-16)
anon → monthly_energy_sync*        401        service key → still reads (n8n MARKETING workflow unaffected)
```

## Gap backfill (api/backfill_gaps.py), 2026-09-16

4,413 days missing inside stations' own histories. Solis has data for **25** of
them (inserted, 5 stations, all Aug-2026); the other **4,388 are days Solis
itself has no data for** — genuine downtime, not sync loss. 3,545 existing days
were compared with Solis at the same time: 21 mismatches (0.59%), all explained
— 12 are today's zero placeholder rows (filled by tonight's 18:00 UTC sync), 7
are yesterday revised by Solis by ≤0.6 kWh, 2 are legacy wall-clock rows holding
a partial-morning value. Full reports kept outside the repo (customer names).

## On file 12

The two legacy mismatches above are why 12 exists: the 405 rows with insert-time
timestamps are partial-day snapshots, not day totals. 12 quarantines them; the
gap filler would then re-insert the correct noon rows from Solis. Run it when
the back office can show what changed — it is a data edit, not a schema one.

## Not yet done

- `api/merge_customer_accounts.py --apply` (6 merges) — **needs a decision**:
  merged customers see only one system until the portal has a station selector.
- The back office itself; the backfill worker that drains `backfill_jobs`;
  staff accounts in `staff_users` (none exist — the table is empty).
- Render: `SOLIS_API_TOKEN` still unset (`/solis/*` returns 503 to everyone).
- GitHub: Settings → Pages → Source: None.
- Password rotation for the 565 never-signed-in accounts.
