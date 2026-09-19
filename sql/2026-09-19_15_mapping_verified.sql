-- 2026-09-19 step 15 — "manually verified" on a station's customer mapping.
-- Run over the direct connection (autocommit). Re-runnable. No heavy locks:
-- nullable columns (catalog-only) and two trigger swaps on a ~630-row table.
--
-- WHY: Monitoring Admin's "Possible wrong station" list is a heuristic — the
-- Solis plant name shares no word with the customer's name. On 2026-09-19 it
-- held 30 stations; in 28 of them the Supabase and Odoo names already agreed
-- and the plant was simply named after a business, a church, a relative or a
-- lot number. Those are correct mappings the heuristic can never clear by
-- itself, so an engineer who has checked one needs a way to say so that
--   (a) removes the row from the scan,
--   (b) records WHO decided, WHEN and WHY, and
--   (c) is undone automatically if the station id later changes — the verdict
--       was about THAT (customer, station) pair, not the customer.
--
-- Who/when are stamped by the API from the staff session, never supplied by
-- the browser. The verifier's EMAIL is deliberately not stored on the row:
-- solar_systems is readable by the customer who owns it (file 10) and a staff
-- address does not belong there. Monitoring Admin resolves it through
-- staff_users at read time, and audit_log.actor_email keeps it permanently.

alter table public.solar_systems
  add column if not exists mapping_verified_at   timestamptz,
  add column if not exists mapping_verified_by   uuid references auth.users(id) on delete set null,
  add column if not exists mapping_verified_note text;

comment on column public.solar_systems.mapping_verified_at is
  'Set when an engineer confirmed in Monitoring Admin that this Solis station belongs to this customer. '
  'Hides the row from the "possible wrong station" scan. Cleared by trg_clear_mapping_verified when solis_station_id changes.';

-- (a) + (b) the audit trail: the tick, the untick and the note become field
-- changes attributed by the 08 GUCs (actor, reason, request id). The actor IS
-- the verifier, so mapping_verified_by itself need not be listed — listing it
-- would only add a uuid row that repeats actor_email.
drop trigger if exists trg_audit_solar_systems on public.solar_systems;
create trigger trg_audit_solar_systems
  after insert or update or delete on public.solar_systems
  for each row execute function public.fn_audit_row_changes(
    'user_id','system_name','capacity_kwp','installation_date',
    'battery_capacity_kwh','status','address','is_primary',
    'solis_station_id','solis_plant_name','solis_user_email',
    'odoo_lead_id','odoo_lead_email',
    'mapping_verified_at','mapping_verified_note');

-- (c) Remap, detach and a hand edit of the id all invalidate the verdict.
-- Clear it in the same statement, so the audit trail shows the clearing under
-- the same reason and request id as the change that caused it. The second
-- condition lets one statement set a new id AND a new verification on purpose.
create or replace function public.tg_clear_mapping_verified()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
begin
  if new.solis_station_id is distinct from old.solis_station_id
     and new.mapping_verified_at is not distinct from old.mapping_verified_at then
    new.mapping_verified_at   := null;
    new.mapping_verified_by   := null;
    new.mapping_verified_note := null;
  end if;
  return new;
end $$;

drop trigger if exists trg_clear_mapping_verified on public.solar_systems;
create trigger trg_clear_mapping_verified
  before update of solis_station_id on public.solar_systems
  for each row execute function public.tg_clear_mapping_verified();

-- Verify
select count(*) filter (where mapping_verified_at is not null) as verified,
       count(*) as systems
  from public.solar_systems;
select tgname
  from pg_trigger
 where tgrelid = 'public.solar_systems'::regclass and not tgisinternal
 order by 1;
