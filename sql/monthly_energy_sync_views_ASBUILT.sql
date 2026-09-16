-- AS-BUILT definitions of the six monthly_energy_sync* views, dumped from
-- production (kzsocvzhbgtfyksrjmvk) on 2026-09-16 with pg_get_viewdef().
-- These existed only in the database; no migration file created them.
-- Migration 11 recreates them with security_invoker and revokes anon.
-- Grants at dump time are recorded per view so 11 can be checked against them.

-- ===== public.monthly_energy_sync   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync as
 SELECT user_id,
    customer_name,
    system_id,
    solis_station_id,
    odoo_month_name,
    provider_name,
    electricity_rate,
    total_production,
    total_consumption,
    total_flh,
    bucket_category
   FROM monthly_energy_sync_base
  WHERE bucket_category = 'Bucket 1: Success'::text;

-- ===== public.monthly_energy_sync_admin_gaps   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync_admin_gaps as
 SELECT user_id,
    customer_name,
    system_id,
    solis_station_id,
    odoo_month_name,
    provider_name,
    electricity_rate,
    total_production,
    total_consumption,
    total_flh,
    bucket_category
   FROM monthly_energy_sync_base
  WHERE bucket_category = 'Bucket 4: Admin Gap'::text;

-- ===== public.monthly_energy_sync_base   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync_base as
 WITH target_month AS (
         SELECT date_trunc('month'::text, CURRENT_DATE::timestamp with time zone - '2 mons'::interval) AS month_start
        )
 SELECT e.user_id,
    up.full_name AS customer_name,
    e.system_id,
    up.solis_station_id,
    to_char(tm.month_start, 'FMMonth YYYY'::text) AS odoo_month_name,
    ep.name AS provider_name,
    er.rate AS electricity_rate,
    sum(e.production_kwh) AS total_production,
    sum(e.consumption_kwh) AS total_consumption,
    sum(COALESCE(e.full_load_hours, 0::numeric)) AS total_flh,
        CASE
            WHEN min(ss.installation_date) >= tm.month_start THEN 'Bucket 5: New Install'::text
            WHEN ep.name IS NULL OR er.rate IS NULL THEN 'Bucket 4: Admin Gap'::text
            WHEN (EXTRACT(day FROM tm.month_start + '1 mon'::interval - '1 day'::interval) - count(e.id)::numeric + sum(
            CASE
                WHEN COALESCE(e.production_kwh, 0::numeric) <= 0::numeric THEN 1
                ELSE 0
            END)::numeric) >= 10::numeric THEN 'Bucket 3: Offline'::text
            WHEN (sum(e.production_kwh) / NULLIF(sum(e.consumption_kwh), 0::numeric) * 100::numeric) <= 20::numeric THEN 'Bucket 2: Critical Output'::text
            ELSE 'Bucket 1: Success'::text
        END AS bucket_category
   FROM energy_readings e
     CROSS JOIN target_month tm
     JOIN solar_systems ss ON e.system_id = ss.id
     LEFT JOIN user_profiles up ON e.user_id = up.id
     LEFT JOIN electricity_providers ep ON up.electricity_provider_id = ep.id
     LEFT JOIN LATERAL ( SELECT rates.rate
           FROM electricity_rates rates
          WHERE rates.provider_id = ep.id AND rates.effective_date <= (tm.month_start + '1 mon'::interval - '1 day'::interval)
          ORDER BY rates.effective_date DESC
         LIMIT 1) er ON true
  WHERE e."timestamp" >= tm.month_start AND e."timestamp" < (tm.month_start + '1 mon'::interval)
  GROUP BY e.user_id, up.full_name, e.system_id, up.solis_station_id, tm.month_start, ep.name, er.rate;

-- ===== public.monthly_energy_sync_critical_output   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync_critical_output as
 SELECT user_id,
    customer_name,
    system_id,
    solis_station_id,
    odoo_month_name,
    provider_name,
    electricity_rate,
    total_production,
    total_consumption,
    total_flh,
    bucket_category
   FROM monthly_energy_sync_base
  WHERE bucket_category = 'Bucket 2: Critical Output'::text;

-- ===== public.monthly_energy_sync_new_installs   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync_new_installs as
 SELECT user_id,
    customer_name,
    system_id,
    solis_station_id,
    odoo_month_name,
    provider_name,
    electricity_rate,
    total_production,
    total_consumption,
    total_flh,
    bucket_category
   FROM monthly_energy_sync_base
  WHERE bucket_category = 'Bucket 5: New Install'::text;

-- ===== public.monthly_energy_sync_offline   owner=postgres   reloptions=None
--   grant: anon -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: authenticated -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: postgres -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
--   grant: service_role -> DELETE,INSERT,REFERENCES,SELECT,TRIGGER,TRUNCATE,UPDATE
create or replace view public.monthly_energy_sync_offline as
 SELECT user_id,
    customer_name,
    system_id,
    solis_station_id,
    odoo_month_name,
    provider_name,
    electricity_rate,
    total_production,
    total_consumption,
    total_flh,
    bucket_category
   FROM monthly_energy_sync_base
  WHERE bucket_category = 'Bucket 3: Offline'::text;
