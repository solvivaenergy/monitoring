-- 2026-09-14 step 11 — close the anon PII leak on the six monthly_energy_sync
-- views, and apply the one migration that was written and never run.
-- Run this once in the Supabase SQL editor. Order: after 10, but the REVOKE
-- block is urgent and independent — run it TODAY, ahead of everything else if
-- you like. Re-runnable. No heavy locks.
--
-- *** SEV-1. THIS IS A LIVE DATA LEAK, NOT A MIGRATION CONCERN. ***
--
-- Measured with the PUBLIC anon key and no login at all:
--   monthly_energy_sync                 464 rows
--   monthly_energy_sync_base            531 rows   <-- the largest
--   monthly_energy_sync_offline          55
--   monthly_energy_sync_critical_output   8
--   monthly_energy_sync_admin_gaps        2
--   monthly_energy_sync_new_installs      2
-- Every one exposes: user_id, customer_name, system_id, solis_station_id,
-- odoo_month_name, provider_name, electricity_rate, total_production,
-- total_consumption, total_flh, bucket_category.
-- That is 531 of 615 customers (86%) — real name, auth UUID, system UUID, Solis
-- station id, monthly kWh and their electricity tariff — readable by anyone
-- holding the anon key, which ships in the mobile app bundle and is public by
-- design. All 11 base tables are correctly locked; only these six views leak.
--
-- CAUSE: a Postgres view runs with its OWNER's rights. A view owned by postgres
-- over an RLS-protected table bypasses that RLS entirely, and anon/authenticated
-- hold SELECT on the view. The standard Supabase footgun.
--
-- These are operational/back-office views — bucket categories, offline lists,
-- admin gaps. Nothing in the customer app reads them, so revoking is safe. The
-- back office reads them through the service role.

do $$
declare v text;
begin
  foreach v in array array[
    'monthly_energy_sync',
    'monthly_energy_sync_base',
    'monthly_energy_sync_offline',
    'monthly_energy_sync_critical_output',
    'monthly_energy_sync_admin_gaps',
    'monthly_energy_sync_new_installs'
  ]
  loop
    if exists (select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace
                where n.nspname = 'public' and c.relname = v and c.relkind = 'v') then
      execute format('revoke all on public.%I from anon, authenticated', v);
      -- Belt and braces: invoker rights mean RLS applies even if a GRANT is
      -- re-added later. PG15+ / all current Supabase projects.
      begin
        execute format('alter view public.%I set (security_invoker = on)', v);
      exception when others then
        raise warning 'security_invoker not supported for % : %', v, sqlerrm;
      end;
      raise notice 'locked down view %', v;
    end if;
  end loop;
end $$;

-- Verify with the ANON key afterwards; all six must return content-range */0.

-- ------------------------------------------------------------------
-- NOT DROPPING THE VIEWS — correcting the data-integrity audit.
-- That audit concluded "the migration must DROP VIEW all six, alter the tables,
-- then CREATE VIEW all six", because Postgres refuses ALTER/DROP on a column a
-- GROUP BY view depends on. That is true, but CONDITIONAL on altering or
-- dropping user_profiles.solis_station_id — which this migration deliberately
-- does NOT do (file 04 keeps it as a trigger-maintained compat mirror, because
-- app_routes.py:74, sync_five_minutes:301, backfill_history:204,
-- solviva_mcp:95,448 and the mobile app all still read it). Nothing in files
-- 01-10 drops a column or changes a type any view depends on, so no view breaks.
--
-- The views ARE still semantically wrong under multi-station: they GROUP BY
-- user_id and source solis_station_id by joining user_profiles, so a customer
-- with two real stations gets their production summed into one row carrying an
-- arbitrary system_id and their PRIMARY station's id. Today that is invisible
-- because no user has two real stations in Supabase yet.
--
-- Fixing them requires their DDL, which exists ONLY in this database and is not
-- in the repo or anywhere in version control. Run query (c) in file 00, save the
-- output to sql/monthly_energy_sync_views_ASBUILT.sql, COMMIT THAT FILE, and
-- only then rewrite them to:
--     group by s.id  (the station)          -- not by user_id
--     select s.solis_station_id             -- from solar_systems, not user_profiles
--     join user_profiles only for customer_name
-- Keeping the LEFT JOIN from systems/readings to user_profiles is required:
-- the 2 "Bucket 4: Admin Gap" rows exist precisely because it is a LEFT JOIN,
-- and file 02 has now given those two users profiles, so expect that bucket to
-- drop to 0 on the next MBR run. That is the intended outcome, not a regression.
-- ------------------------------------------------------------------

-- ------------------------------------------------------------------
-- sql/add_system_metrics_table.sql was written but never pasted into the SQL
-- editor: GET /rest/v1/system_metrics returns 404 PGRST205 even with the service
-- key. api/sync_client_counts.py:67 upserts into it and fails on every run. That
-- file is also still UNTRACKED in git. Inlined here so the applied state is one
-- ordered set of files.
--
-- Note separately that sync_client_counts.py:36-47 counts USERS and labels them
-- "clients_with_solis_station". Once one user owns N stations that metric is
-- neither a client count nor a station count — repoint it at
-- solar_systems where solis_station_id is not null.
-- ------------------------------------------------------------------
create table if not exists public.system_metrics (
  metric_name  text primary key,
  metric_value bigint not null,
  metadata     jsonb not null default '{}'::jsonb,
  updated_at   timestamptz not null default now()
);

create index if not exists system_metrics_updated_at_idx
  on public.system_metrics (updated_at desc);

alter table public.system_metrics enable row level security;

do $$
begin
  if not exists (select 1 from pg_policies
                  where schemaname = 'public' and tablename = 'system_metrics'
                    and policyname = 'service role full access') then
    create policy "service role full access"
      on public.system_metrics for all
      using (auth.role() = 'service_role')
      with check (auth.role() = 'service_role');
  end if;
end $$;
