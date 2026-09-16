-- 2026-09-14 step 04b (NON-CONCURRENT variant) — run this INSTEAD of
-- 2026-09-14_04b_station_identity_indexes.sql if your SQL editor reports
--     ERROR: 25001: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
-- even for a single statement. The Supabase editor wraps every submission in a
-- transaction, so CONCURRENTLY can be unusable there.
--
-- SAFE HERE, and only here. Without CONCURRENTLY each CREATE INDEX takes an
-- ACCESS EXCLUSIVE lock that blocks writes to the table for its duration. Every
-- index below is on solar_systems (615 rows) or user_profiles (615 rows), so
-- that is single-digit milliseconds.
--
-- *** DO NOT apply this trick to file 05. *** Those two indexes are on
-- energy_readings (114k rows) and energy_readings_five_minutes (129k rows),
-- which the 15-minute cron writes to continuously. Use CONCURRENTLY there via
-- psql or the Supabase connection string, not the web editor.
--
-- Paste this WHOLE file and run it once. Order: after 04a, before 04c.


-- (1 of 9)
create unique index if not exists solar_systems_solis_station_id_uk
  on public.solar_systems (solis_station_id) where solis_station_id is not null;

-- (2 of 9)
create unique index if not exists solar_systems_odoo_lead_id_uk
  on public.solar_systems (odoo_lead_id) where odoo_lead_id is not null;

-- (3 of 9)
create unique index if not exists solar_systems_user_station_uk
  on public.solar_systems (user_id, solis_station_id) where solis_station_id is not null;

-- (4 of 9)
create unique index if not exists solar_systems_one_primary_uk
  on public.solar_systems (user_id) where is_primary;

-- (5 of 9)
create index if not exists user_profiles_odoo_partner_id_idx
  on public.user_profiles (odoo_partner_id) where odoo_partner_id is not null;

-- (6 of 9)
create extension if not exists pg_trgm;

-- (7 of 9)
create index if not exists user_profiles_full_name_trgm
  on public.user_profiles using gin (full_name gin_trgm_ops);

-- (8 of 9)
create index if not exists solar_systems_plant_name_trgm
  on public.solar_systems using gin (solis_plant_name gin_trgm_ops);

-- (9 of 9)
create index if not exists solar_systems_station_id_trgm
  on public.solar_systems using gin (solis_station_id gin_trgm_ops);

-- Verify all 8 indexes plus the extension landed.
select indexname from pg_indexes
 where schemaname = 'public'
   and indexname in ('solar_systems_solis_station_id_uk','solar_systems_odoo_lead_id_uk',
                     'solar_systems_user_station_uk','solar_systems_one_primary_uk',
                     'user_profiles_odoo_partner_id_idx','user_profiles_full_name_trgm',
                     'solar_systems_plant_name_trgm','solar_systems_station_id_trgm')
 order by indexname;
-- Expect 8 rows. Then run 04c.
