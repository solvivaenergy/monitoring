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
| 05 new conflict key | **applied** — both keys coexist | 2026-09-16 |
| 06 drop old conflict key | not run — **requires the code deploy first** | |
| 07 foreign keys | not run | |
| 08 staff and audit | not run | |
| 09 backfill jobs | not run | |
| 10 station-scoped RLS | not run | |
| 11 view security | not run — **needs `monthly_energy_sync_views_ASBUILT.sql`, which does not exist yet; the DDL lives only in the DB (file 00 query dumps it)** | |
| 12 legacy timestamp quarantine (optional) | not run | |

## The order is load-bearing

```
04 → 05 → DEPLOY CODE → 06 → merge_customer_accounts --apply → 07…11
```

The deployed syncs upsert with `on_conflict="system_id,timestamp"`, which needs
05's index. 05 leaves **both** the old `(user_id, timestamp)` and the new
`(system_id, timestamp)` keys in place, so old and new code both work and the
deploy has no outage — that is the whole reason 06 is a separate file. Running
06 before the deploy breaks every upsert with `42P10`.

`api/merge_customer_accounts.py` refuses to run before 06: it detects real
`(user_id, timestamp)` collisions, and 4 of the 6 planned merges would otherwise
fail halfway through moving rows.

## State after 05 (verified 2026-09-16)

```
energy_readings                116,096 rows   0 duplicate (system_id, timestamp)
energy_readings_five_minutes   160,788 rows   0 duplicate (system_id, timestamp)
orphaned readings                    0        invalid indexes: none
solar_systems                      625 rows   619 with a station id, 619 distinct
```
