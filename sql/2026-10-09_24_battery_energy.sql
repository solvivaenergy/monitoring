-- 2026-10-09  24  Battery energy on the five-minute and hourly tables
--
-- WHY: the portal's consumption chart splits every bar into grid / solar /
-- battery. Past days take battery from energy_readings.battery_discharge_kwh
-- (Solis's settled day total, written by the nightly sync). Today and the 1D
-- view are built from the five-minute cache and energy_readings_hourly, and
-- neither held battery ENERGY: the five-minute sync read Solis's batteryPower
-- only to label the row charging / discharging / idle and dropped the
-- magnitude. So today's bar always said "Battery 0.0 kWh" while every earlier
-- day showed a value (reported by Alden 2026-10-09).
--
-- Design
--   * Two columns on each table, battery_charge_kwh / battery_discharge_kwh:
--     the five-minute slice of batteryPower (watts; POSITIVE = charging,
--     NEGATIVE = discharging — verified 2026-10-09 on three plants: SoC rises
--     while it is positive at noon and falls while it is negative at night;
--     batteryPowerFu / batteryPowerZheng are only its two halves). Integrating
--     the curve reproduced Solis's own batteryDischargeEnergy within 1 kWh on
--     two of the three plants and came out lower on one (4.5 vs 8 kWh), so a
--     day built from these columns can differ from the daily table's figure —
--     the same relation production already has between the live estimate and
--     the settled day.
--   * api/sync_five_minutes_to_supabase._build_row writes the five-minute
--     values; the worker's hourly backfill (_hourly_rows) and rollup_hourly()
--     below sum them into the hour.
--   * Both tables are partitioned (files 21 and 22): ADD COLUMN on the parent
--     reaches every partition and new partitions inherit it. A constant
--     default is metadata only on this Postgres — no rewrite, a lock held for
--     milliseconds. lock_timeout keeps the ALTER from queueing behind a long
--     read and then blocking the five-minute writer behind itself.
--   * rollup_hourly()'s "identical row" tuple gains the two columns, so hours
--     rolled up with zeros before the code deploy are refreshed on the next
--     run while their five-minute rows are still in the two-day cache.
--   * Deploy order: this file, THEN the API/worker (every upsert batch names
--     the columns, so against the old table every batch fails), THEN the
--     portal (it selects the columns straight from Supabase).
--   * History: energy_readings_hourly rows written before this hold 0; the
--     fleet hourly backfill (api.enqueue_fleet_backfill --hourly --days N)
--     re-reads them from Solis.
--
-- Apply over the direct connection (autocommit). Re-runnable.

set lock_timeout = '5s';

alter table public.energy_readings_five_minutes
  add column if not exists battery_charge_kwh    numeric default 0,
  add column if not exists battery_discharge_kwh numeric default 0;

comment on column public.energy_readings_five_minutes.battery_charge_kwh is
  'kWh into the battery in this five-minute slice: max(batteryPower, 0) W × 5/60 / 1000. 0 when the plant has no battery.';
comment on column public.energy_readings_five_minutes.battery_discharge_kwh is
  'kWh out of the battery in this five-minute slice: max(-batteryPower, 0) W × 5/60 / 1000. The portal''s "Battery" share of consumption.';

alter table public.energy_readings_hourly
  add column if not exists battery_charge_kwh    numeric(10,3) not null default 0,
  add column if not exists battery_discharge_kwh numeric(10,3) not null default 0;

comment on column public.energy_readings_hourly.battery_charge_kwh is
  'Sum of the hour''s five-minute battery_charge_kwh (rollup_hourly or the worker''s hourly backfill). 0 for rows written before file 24 until re-read.';
comment on column public.energy_readings_hourly.battery_discharge_kwh is
  'Sum of the hour''s five-minute battery_discharge_kwh. 0 for rows written before file 24 until re-read.';

-- The roll-up from file 20, with the two sums. Supersedes file 20's definition.
create or replace function public.rollup_hourly(p_from timestamptz, p_to timestamptz)
returns integer
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  n integer;
begin
  insert into public.energy_readings_hourly
    (system_id, user_id, hour_start, production_kwh, consumption_kwh,
     grid_import_kwh, grid_export_kwh, battery_charge_kwh, battery_discharge_kwh,
     peak_power_kw, battery_level_end, points, source)
  select f.system_id,
         (array_agg(f.user_id) filter (where f.user_id is not null))[1],   -- no max(uuid) in Postgres
         (date_trunc('hour', f."timestamp" at time zone 'Asia/Manila') at time zone 'Asia/Manila') as hour_start,
         round(sum(f.production_kwh)::numeric, 3),
         round(sum(f.consumption_kwh)::numeric, 3),
         round(sum(f.grid_import_kwh)::numeric, 3),
         round(sum(f.grid_export_kwh)::numeric, 3),
         round(coalesce(sum(f.battery_charge_kwh), 0)::numeric, 3),
         round(coalesce(sum(f.battery_discharge_kwh), 0)::numeric, 3),
         round((max(f.production_kwh) * 12)::numeric, 3),              -- the biggest 5-min slice, as kW
         (array_agg(f.battery_level order by f."timestamp" desc) filter (where f.battery_level is not null))[1],
         least(count(*), 12)::smallint,
         'rollup'
    from public.energy_readings_five_minutes f
   where f."timestamp" >= p_from and f."timestamp" < p_to
     and f.system_id is not null
   group by f.system_id, 3
  on conflict (system_id, hour_start) do update
     set production_kwh        = excluded.production_kwh,
         consumption_kwh       = excluded.consumption_kwh,
         grid_import_kwh       = excluded.grid_import_kwh,
         grid_export_kwh       = excluded.grid_export_kwh,
         battery_charge_kwh    = excluded.battery_charge_kwh,
         battery_discharge_kwh = excluded.battery_discharge_kwh,
         peak_power_kw         = excluded.peak_power_kw,
         battery_level_end     = excluded.battery_level_end,
         points                = excluded.points,
         source                = 'rollup',
         user_id               = coalesce(excluded.user_id, public.energy_readings_hourly.user_id)
   where excluded.points >= public.energy_readings_hourly.points
     and (public.energy_readings_hourly.production_kwh, public.energy_readings_hourly.consumption_kwh,
          public.energy_readings_hourly.grid_import_kwh, public.energy_readings_hourly.grid_export_kwh,
          public.energy_readings_hourly.battery_charge_kwh, public.energy_readings_hourly.battery_discharge_kwh,
          public.energy_readings_hourly.points, public.energy_readings_hourly.battery_level_end)
         is distinct from
         (excluded.production_kwh, excluded.consumption_kwh,
          excluded.grid_import_kwh, excluded.grid_export_kwh,
          excluded.battery_charge_kwh, excluded.battery_discharge_kwh,
          excluded.points, excluded.battery_level_end);
  get diagnostics n = row_count;
  return n;
end $$;

comment on function public.rollup_hourly(timestamptz, timestamptz) is
  'Aggregate energy_readings_five_minutes into energy_readings_hourly for [p_from, p_to), battery energy included '
  '(file 24). Idempotent: never replaces a fuller hour with a thinner one, never rewrites an identical row. Returns rows written.';

-- Grants are table-level (authenticated/staff read, service_role writes,
-- inquiry_ro reads), so the new columns need nothing; the function keeps its
-- service_role-only execute from file 20.

select pg_notify('pgrst', 'reload schema');

-- Verify
select table_name, column_name, data_type, column_default
  from information_schema.columns
 where table_schema = 'public'
   and table_name in ('energy_readings_five_minutes', 'energy_readings_hourly')
   and column_name in ('battery_charge_kwh', 'battery_discharge_kwh')
 order by 1, 2;
select count(*) as partitions_with_columns
  from pg_inherits i join pg_attribute a on a.attrelid = i.inhrelid
 where i.inhparent in ('public.energy_readings_five_minutes'::regclass, 'public.energy_readings_hourly'::regclass)
   and a.attname = 'battery_discharge_kwh' and not a.attisdropped;

-- Rollback, if ever needed:
--   alter table public.energy_readings_five_minutes drop column battery_charge_kwh, drop column battery_discharge_kwh;
--   alter table public.energy_readings_hourly       drop column battery_charge_kwh, drop column battery_discharge_kwh;
--   re-create rollup_hourly() from file 20, and roll the API/worker back first.
