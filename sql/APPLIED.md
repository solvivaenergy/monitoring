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
| **13 readings quarantine** (`2026-09-17_13_readings_quarantine.sql`) | **applied** — `energy_readings_quarantine`, used by Monitoring Admin's Remap to hold readings captured under a wrong station id (recoverable, never deleted) | 2026-09-17 |
| **14 audit_log append-only trigger** (`2026-09-17_14_audit_log_append_only_trigger.sql`) | **applied** — replaces 08's rewrite rules, which had made **every auth user undeletable** (the FK's ON DELETE SET NULL was rewritten to nothing). File 08 updated to match. | 2026-09-17 |
| **15 mapping "manually verified"** (`2026-09-19_15_mapping_verified.sql`) | **applied** — `solar_systems.mapping_verified_at / _by / _note`; the 08 audit trigger now lists `mapping_verified_at` + `_note` (file 08 updated to match); `trg_clear_mapping_verified` clears the tick whenever `solis_station_id` changes. Proven in a rolled-back transaction: tick audited to the staff email, remap clears and audits the clearing, same-value id write keeps it. 0 verified at apply time. | 2026-09-19 |
| **17 write amplification** (`2026-09-25_17_write_amplification.sql`) | **applied** — after Supabase's "Disk IO Budget" mail (2026-09-23; project is on NANO compute and swapping ~400 MB around the clock — the compute upgrade is the real fix). Dropped 5 unused/duplicate indexes (five-minute table 6 → 3, daily 6 → 4), `fillfactor` 85 / 90, new view `public.five_minute_watermarks` (invoker rights, anon/authenticated revoked, service role only) so the five-minute sync learns what it holds in one request. Verified: reloptions set, 3 + 4 indexes, view = 596 stations / 74,014 rows today. Rollback DDL in the file header. | 2026-09-25 |
| **18 station electricity provider** (`2026-10-02_18_station_electricity_provider.sql`) | **applied** — `solar_systems.electricity_provider_id` (bigint, FK → `electricity_providers` ON DELETE SET NULL, validated), seeded from the owner's profile where NULL, added to the 08 audit trigger (file 08 updated), and `monthly_energy_sync_base` repointed from `up.` to `ss.electricity_provider_id` with `security_invoker=on` re-asserted (CREATE OR REPLACE VIEW replaces reloptions — the clause is mandatory). **Found on apply:** John Velasquez had already created the column and filled 666 rows from Odoo at 16:19–16:21 Manila (unaudited: the column was not in the trigger list yet); the file is idempotent and layered on top (seed touched 2 rows). The live view already used `CURRENT_DATE - 1 mon` (ASBUILT said 2 mons; file updated). Backfill = first `api.sync_identity_mirror --skip-solis --apply` run (17:10 Manila): 666/680 stations resolve by Odoo code, 0 unknown codes, 11 leads blank, 3 stations on no lead; 61 legacy profile copies filled, 0 disagreements. Admin Gap bucket 71 → 11 (all 11 = lead has no provider; sales). | 2026-10-02 |
| **23 worker bookkeeping** (`2026-10-04_23_worker_runs.sql`) | **applied** — `public.sync_runs` (one row per worker task run: task, started/finished, status, stats jsonb, error, host; staff-read via `is_staff()`, inquiry_ro read; trimmed to 90 days by pg_cron `sync_runs_retention` weekly) and `public.station_activity` (system_id, last_viewed_at, views; staff-read) + `touch_station_activity(p_station_id)` (SECURITY DEFINER, service_role only) called by `/app/live` and `/app/hourly`. Feeds Monitoring Admin → **Health** and the worker's cadence-by-demand (`api/worker.py`). Touch function exercised once and the row removed. | 2026-10-04 |
| **21 five-minute cache partitioned** (`2026-10-04_21_five_minute_partitions.sql`) | **applied** 10:08Z — `energy_readings_five_minutes` rebuilt as a range-partitioned table (one partition per UTC day, `_pYYYY_MM_DD`, fillfactor 85 per partition, default partition), copied and swapped in one 10-second transaction (133,531 rows, counts matched). PK is now `(id, "timestamp")`; `(system_id, "timestamp")` unique, FKs, check, the 04c owner trigger and the `er5m_read_own_station` policy carried over; anon/authenticated revoked on every partition. `five_minute_watermarks` re-created (it had followed the old table through the rename). **Retention is now `maintain_five_minute_partitions()`** (creates today..+2, drops partitions older than 2 UTC days) run hourly by **pg_cron** (`five_minute_partitions`, `7 * * * *`) — the Python purge DELETE is gone. Old table kept as `energy_readings_five_minutes_old` for a day; **dropped 2026-10-05 ~12:00Z** (46 MB; nothing referenced it, 6 live partitions confirmed). | 2026-10-04 |
| **20 hourly readings** (`2026-10-03_20_hourly_readings.sql`) | **applied** — `public.energy_readings_hourly` (one row per station per Manila hour: production/consumption/grid kWh, peak kW, end SoC, `points` = 5-min samples, `source` rollup/backfill; RLS = owner-or-grantee via `owns_system()` + staff; anon nothing; service role writes) and `rollup_hourly(from, to)` (SECURITY DEFINER, service_role only; aggregates the five-minute cache; never thins a fuller hour, never rewrites an identical row). `backfill_jobs.granularity` now allows `'hourly'`. Decided with Alden ("option A"): today is served LIVE from the five-minute table by `/app/hourly`; the five-minute cron rolls up closed hours (two just-closed hours each run, yesterday before the midnight purge, whole day in the last runs); history = worker granularity `hourly` (stationDay per station-day), 90 days fleet-wide at launch. First apply failed on `max(uuid)` (fixed, re-run); first roll-up 00:00–14:00 Manila wrote 8,752 rows for 630 stations, second run 0, hour sums = five-minute sums exactly. | 2026-10-03 |
| **19 drop the stale status check** (`2026-10-02_19_drop_old_status_check.sql`) | **applied** — `solar_systems` had TWO status checks: the original `solar_systems_status_check` (active/inactive/maintenance) and 04a's `solar_systems_status_chk` (active/inactive/decommissioned/pending, NOT VALID). Both were enforced, so `decommissioned` and `pending` — the Status select's options and the detach flow's target — failed with a check violation. Dropped the old one, validated 04a's (all 680 rows were `active`). Found while decommissioning "Eduardo Rolle System 2" (station …134060: Solis B0014 "plant does not exist", 0 readings; the live plant is System 2.1 on another login) — first `decommissioned` row, audited to alden with source `monitoring_admin_chat`. | 2026-10-02 |
| **16 view-only access grants** (`2026-09-21_16_system_access.sql`) | **applied** — `public.system_access` (one row per station × viewer login; RLS on, anon revoked, authenticated read-own + staff, no write policy; audit + updated_at triggers); `owns_system()` now returns true for owner OR grantee, so file 10's reading policies did not change; new `ss_read_granted` policy on `solar_systems`. Proven in a rolled-back transaction: a stranger saw 0 → 919 daily + 177 five-minute rows and the station row after a grant, never the owner's profile; an unrelated customer stayed at 0; the INSERT was audited. 0 grants at apply time. Used by Monitoring Admin's "Who can view this station" and by `/app/*` station resolution (owned, else oldest grant). | 2026-09-21 |
| **22 hourly readings partitioned** (`2026-10-05_22_hourly_partitions.sql`) | **applied** — found applied when 24 was prepared on 2026-10-09 (this ledger still said "NOT applied"): `energy_readings_hourly` is range-partitioned by UTC month, `_p2026_07` … `_p2026_12` + default, 1,306,529 rows / 682 stations, pg_cron `hourly_partitions`. The pre-swap copy `energy_readings_hourly_old` (1,245,602 rows, 309 MB, nothing depending on it, live table a superset) was still present on 2026-10-09 and dropped that day. | 2026-10-05 (approx.) |
| **24 battery energy** (`2026-10-09_24_battery_energy.sql`) | **applied** 2026-10-09 ~02:10Z — `battery_charge_kwh` / `battery_discharge_kwh` on `energy_readings_five_minutes` (numeric, default 0) and `energy_readings_hourly` (numeric(10,3) not null default 0): the five-minute slice of Solis's `batteryPower` (+ = charging, − = discharging). `rollup_hourly()` replaced with the two sums and a wider "identical row" tuple. Verified: columns on all 6 + 7 partitions, function grants unchanged (postgres, service_role), smoke roll-up of a closed hour wrote 0 rows, a real `--limit 2` sync run upserted through PostgREST with the new keys. Why: the portal's "Battery" share of consumption was 0 for today and the 1D view — only the daily table carried battery energy. Code: `_build_row` writes it (with a column guard so the feed survives a missing column), the worker's `_hourly_rows` and `/app/hourly` + `/app/live` sum/expose it. Hourly rows written before this hold 0 until re-read (fleet hourly backfill). | 2026-10-09 |

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
Monitoring Admin can show what changed — it is a data edit, not a schema one.

## Not yet done (updated 2026-10-09)

- File 22 is applied (see the table; the 2026-10-04 note here was stale) and both `_old` copies are dropped.
- File 12 (above).
- Everything else from the original list is done: the 6 merges were applied
  through Monitoring Admin's Merge action on 2026-09-18 (not the script);
  Monitoring Admin (`/monitoring-admin`, formerly `/backoffice`), the backfill
  worker and `staff_users` are live; `SOLIS_API_TOKEN` is set; GitHub Pages was
  already off. `audit_log.source` says `'backoffice'` for rows written before
  the 2026-09-18 rename and `'monitoring_admin'` after.
- Password rotation for the 565 never-signed-in accounts.
