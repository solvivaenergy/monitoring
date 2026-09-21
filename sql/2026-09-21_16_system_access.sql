-- 2026-09-21 step 16 — view-only access grants: several logins, one station.
-- Run over the direct connection (autocommit). Re-runnable. No heavy locks:
-- one new table, one function body replaced, one SELECT policy added.
--
-- WHY: an SME customer's owner will not watch the portal personally; they hand
-- it to a staff member who has their own email. Today a Solis station belongs
-- to exactly one login (solar_systems_solis_station_id_uk, file 04b) and every
-- customer read policy follows that owner (file 10), so "two logins on one
-- station" was only ever an accident — the Retire action exists to remove it.
-- This file adds a second, explicit relationship: a GRANT that lets a login
-- VIEW a station it does not own. Ownership, the Odoo mapping, the syncs and
-- the backfills are untouched; only the read boundary widens.
--
-- Design points
--   * One row per (station, viewer). Granted from Monitoring Admin with a
--     reason; the 08 audit trigger records the grant, the revoke (a DELETE),
--     who did it and why.
--   * owns_system() — the SECURITY DEFINER helper every reading policy calls —
--     now also returns true for a grantee. The reading policies themselves do
--     not change, so the as-built policy record still matches. The name stays
--     for the same reason; the function comment says what it means now.
--   * solar_systems gets one extra SELECT policy through the same helper, so
--     the portal's "which stations can I see" query lists granted stations.
--   * A grantee reads the station row and its readings. NOT the owner's
--     profile (phone, address): up_read_own is unchanged.
--   * Nothing here lets a grantee write anything; file 10's rule stands.
--   * Deleting the station or the login removes the grant (ON DELETE CASCADE),
--     so Retire, Merge and staff Delete need no new code.

create table if not exists public.system_access (
  id          uuid primary key default gen_random_uuid(),
  system_id   uuid not null references public.solar_systems(id) on delete cascade,
  user_id     uuid not null references auth.users(id) on delete cascade,
  role        text not null default 'viewer' check (role in ('viewer')),
  granted_by  uuid references auth.users(id) on delete set null,
  granted_at  timestamptz not null default now(),
  reason      text,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  constraint system_access_system_user_uk unique (system_id, user_id)
);
create index if not exists system_access_user_idx on public.system_access (user_id);

comment on table public.system_access is
  'View-only access of a login to a station it does not own (e.g. the staff member an SME owner '
  'assigns to watch the portal). Granted from Monitoring Admin with a reason; revoking deletes the '
  'row. The owner stays solar_systems.user_id; syncs, backfills and the Odoo mapping follow the owner.';

drop trigger if exists trg_system_access_updated_at on public.system_access;
create trigger trg_system_access_updated_at
  before update on public.system_access
  for each row execute function public.tg_set_updated_at();

-- Audit: INSERT/DELETE log the whole row (grant / revoke); the listed columns
-- cover the only UPDATE that makes sense (a corrected reason).
drop trigger if exists trg_audit_system_access on public.system_access;
create trigger trg_audit_system_access
  after insert or update or delete on public.system_access
  for each row execute function public.fn_audit_row_changes('system_id','user_id','role','reason');

-- Locked down like the other customer tables: RLS on, no write policy for
-- anyone but service_role (which bypasses RLS), and the default table grants
-- that Supabase hands anon/authenticated are revoked for writes outright.
alter table public.system_access enable row level security;
revoke all on public.system_access from anon;
revoke insert, update, delete on public.system_access from authenticated;
grant select on public.system_access to authenticated;

do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='system_access' and policyname='sa_read_own') then
    create policy sa_read_own on public.system_access
      for select to authenticated using (user_id = auth.uid());
  end if;
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='system_access' and policyname='sa_read_staff') then
    create policy sa_read_staff on public.system_access
      for select to authenticated using (public.is_staff());
  end if;
end $$;

-- The one behavioural change for existing policies: owner OR grantee.
create or replace function public.owns_system(p_system_id uuid)
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select exists (
           select 1 from public.solar_systems s
            where s.id = p_system_id and s.user_id = auth.uid())
      or exists (
           select 1 from public.system_access a
            where a.system_id = p_system_id and a.user_id = auth.uid());
$$;
comment on function public.owns_system(uuid) is
  'True when the caller OWNS the station (solar_systems.user_id) OR holds a view grant on it '
  '(system_access, file 16). The name predates the grants and was kept so file 10''s policies '
  'did not have to be recreated.';

-- Grantees must also see the station row itself (name, capacity, station id),
-- or the portal cannot list what they may look at. Through the helper, not an
-- inline subquery: a subquery inside a policy is subject to the other table's
-- RLS (file 10's footgun).
do $$
begin
  if not exists (select 1 from pg_policies where schemaname='public'
                  and tablename='solar_systems' and policyname='ss_read_granted') then
    create policy ss_read_granted on public.solar_systems
      for select to authenticated using (public.owns_system(id));
  end if;
end $$;

-- Verify
select tablename, policyname, cmd, qual
  from pg_policies
 where schemaname = 'public' and tablename in ('system_access','solar_systems')
 order by 1, 2;
select count(*) as grants from public.system_access;
select tgname from pg_trigger
 where tgrelid = 'public.system_access'::regclass and not tgisinternal order by 1;

-- Rollback, if ever needed (grants are lost; nothing else changes):
--   drop policy if exists ss_read_granted on public.solar_systems;
--   drop table if exists public.system_access;
--   then re-run the owns_system definition from file 10.
