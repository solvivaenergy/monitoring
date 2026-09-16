-- 2026-09-14 step 00 — PREFLIGHT DISCOVERY (read-only, changes nothing).
-- Run this once in the Supabase SQL editor, FIRST, and keep the output.
--
-- Purpose: this project exposes no RPC functions, so pg_catalog is unreachable
-- from PostgREST. Everything below was inferred by probing. Run it to confirm,
-- and to capture the three things the later files need but cannot introspect:
--   (a) the real name of the UNIQUE(user_id, timestamp) constraint (file 06),
--   (b) the ON DELETE rule on energy_readings.system_id (file 03 is correct
--       under either rule, but you should know which it is),
--   (c) the DDL of the six monthly_energy_sync* views (file 11).
--
-- Order: 00 -> 01 -> 02 -> 03 -> 04 -> 05 -> [DEPLOY CODE] -> 06 -> 07
--        -> 08 -> 09 -> 10 -> 11.  File 12 is optional and can run any time
--        after 05.

-- (a) constraints and indexes on the four tables we touch
select t.relname          as table_name,
       c.conname          as constraint_name,
       c.contype          as type,      -- p=pk u=unique f=fk c=check
       pg_get_constraintdef(c.oid) as definition
  from pg_constraint c
  join pg_class     t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
 where n.nspname = 'public'
   and t.relname in ('energy_readings','energy_readings_five_minutes',
                     'solar_systems','user_profiles','cleaned_data')
 order by t.relname, c.contype, c.conname;

select tablename, indexname, indexdef
  from pg_indexes
 where schemaname = 'public'
   and tablename in ('energy_readings','energy_readings_five_minutes',
                     'solar_systems','user_profiles','cleaned_data')
 order by tablename, indexname;

-- (b) the ON DELETE rule on the reading -> station FKs.
--     confdeltype: a=NO ACTION r=RESTRICT c=CASCADE n=SET NULL d=SET DEFAULT
select t.relname, c.conname, c.confdeltype, pg_get_constraintdef(c.oid)
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
 where c.contype = 'f'
   and t.relname in ('energy_readings','energy_readings_five_minutes');

-- (c) view DDL — file 11 needs this, and the DDL exists ONLY in the database.
--     Save the output into sql/monthly_energy_sync_views_ASBUILT.sql.
select c.relname, pg_get_viewdef(c.oid, true) as definition
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = 'public' and c.relkind = 'v'
   and c.relname like 'monthly_energy_sync%'
 order by c.relname;

-- (d) existing RLS policies — file 10 replaces the SELECT policies on four
--     tables with canonical station-scoped ones. Record what is there now.
select tablename, policyname, cmd, permissive, roles, qual, with_check
  from pg_policies
 where schemaname = 'public'
 order by tablename, cmd, policyname;

-- (e) view ownership / security_invoker (confirms the anon leak in file 11)
select c.relname, c.relkind, c.relrowsecurity,
       (select rolname from pg_roles where oid = c.relowner) as owner,
       c.reloptions
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = 'public' and c.relkind in ('r','v')
 order by c.relkind, c.relname;

-- (f) any INVALID index left behind by a failed CREATE INDEX CONCURRENTLY.
--     Re-run this after every file that uses CONCURRENTLY.
--     Any row here: drop index concurrently <name>;  then retry that statement.
select c.relname as invalid_index
  from pg_class c join pg_index i on i.indexrelid = c.oid
 where not i.indisvalid;
