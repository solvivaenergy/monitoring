-- ============================================================================
-- FILE 04 IS SPLIT INTO THREE PARTS: 04a -> 04b -> 04c. Run them in order.
--
-- Why: the Supabase SQL editor runs a submission inside a transaction, and
-- CREATE INDEX CONCURRENTLY cannot run inside one (ERROR 25001). 04b holds
-- every CONCURRENTLY statement and must be run ONE STATEMENT AT A TIME; 04a
-- and 04c contain none and paste whole.
-- ============================================================================


-- ****************************************************************************
-- RUN EACH NUMBERED STATEMENT BELOW ON ITS OWN.
-- Clear the editor between each. Do NOT paste this whole file.
--
-- If one fails it leaves an INVALID index that silently does nothing. Check:
--     select c.relname from pg_class c join pg_index i on i.indexrelid=c.oid
--      where not i.indisvalid;
-- then:  drop index concurrently <name>;   and retry that statement.
--
-- If your editor refuses CONCURRENTLY even for a lone statement, delete the
-- word CONCURRENTLY. solar_systems is 615 rows, so the exclusive lock is
-- milliseconds. Do not do that on the two reading tables.
-- ****************************************************************************


-- ===== STATEMENT 1 of 9 — RUN ALONE =====
create unique index concurrently if not exists solar_systems_solis_station_id_uk
  on public.solar_systems (solis_station_id) where solis_station_id is not null;

-- one lead describes at most one station

-- ===== STATEMENT 2 of 9 — RUN ALONE =====
create unique index concurrently if not exists solar_systems_odoo_lead_id_uk
  on public.solar_systems (odoo_lead_id) where odoo_lead_id is not null;

-- closes the two-cron race that produced the 104 duplicates

-- ===== STATEMENT 3 of 9 — RUN ALONE =====
create unique index concurrently if not exists solar_systems_user_station_uk
  on public.solar_systems (user_id, solis_station_id) where solis_station_id is not null;

-- exactly one primary station per user = the portal picker's default

-- ===== STATEMENT 4 of 9 — RUN ALONE =====
create unique index concurrently if not exists solar_systems_one_primary_uk
  on public.solar_systems (user_id) where is_primary;

---------------------------------------------------------------------------
-- PART 6 — back-office lookup indexes (also CONCURRENTLY; run each alone)
---------------------------------------------------------------------------

-- ===== STATEMENT 5 of 9 — RUN ALONE =====
create index concurrently if not exists user_profiles_odoo_partner_id_idx
  on public.user_profiles (odoo_partner_id) where odoo_partner_id is not null;

-- ===== STATEMENT 6 of 9 — RUN ALONE =====
create extension if not exists pg_trgm;

-- the search box is a full scan per keystroke today

-- ===== STATEMENT 7 of 9 — RUN ALONE =====
create index concurrently if not exists user_profiles_full_name_trgm
  on public.user_profiles using gin (full_name gin_trgm_ops);

-- ===== STATEMENT 8 of 9 — RUN ALONE =====
create index concurrently if not exists solar_systems_plant_name_trgm
  on public.solar_systems using gin (solis_plant_name gin_trgm_ops);

-- ===== STATEMENT 9 of 9 — RUN ALONE =====
create index concurrently if not exists solar_systems_station_id_trgm
  on public.solar_systems using gin (solis_station_id gin_trgm_ops);

-- ---------------------------------------------------------------------------
-- All statements done? Verify none is invalid, then run 04c.
--     select c.relname from pg_class c join pg_index i on i.indexrelid=c.oid
--      where not i.indisvalid;     -- expect no rows
-- ---------------------------------------------------------------------------
