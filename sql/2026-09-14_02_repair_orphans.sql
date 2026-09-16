-- 2026-09-14 step 02 — repair referential orphans so file 07 can add FKs.
-- Run this once in the Supabase SQL editor. Order: after 01, before 03.
--
-- Verified live 2026-09-14:
--   solar_systems.user_id not in user_profiles     : 2 rows, 2 users
--   energy_readings.user_id not in user_profiles   : 637 rows, the SAME 2 users
--   energy_readings_five_minutes orphan user_id    : 0
--   billing_records/support_tickets/referrals      : 0 orphans (FK-ready)
--   cleaned_data.user_id orphan                    : 470 rows  <-- NO FK possible
--
-- Both orphan users EXIST in auth.users and are real customers with years of
-- history. They surface in production today as monthly_energy_sync_admin_gaps
-- "Bucket 4: Admin Gap" with NULL customer_name. The fix is to recreate their
-- user_profiles rows, never to delete their readings.
--   ca785875-196e-4b3d-b1e9-bb5c1b272325  chancesrei@gmail.com   380 readings
--   33c83383-4c87-411a-8564-83796f12d1ce  arnela@rocketmail.com  257 readings
-- full_name values below are taken verbatim from their auth.users user_metadata.
--
-- No heavy locks in this file.

insert into public.user_profiles (id, full_name, created_at, updated_at)
values
  ('ca785875-196e-4b3d-b1e9-bb5c1b272325', 'Rei Ignacio NM upgraade', now(), now()),
  ('33c83383-4c87-411a-8564-83796f12d1ce', 'J Arnel Agustin',         now(), now())
on conflict (id) do nothing;

-- Verify: all three must return 0 before you proceed to file 07.
select 'solar_systems' as src, count(*) as orphans
  from public.solar_systems s
  left join public.user_profiles p on p.id = s.user_id
 where p.id is null
union all
select 'energy_readings', count(*)
  from public.energy_readings r
  left join public.user_profiles p on p.id = r.user_id
 where p.id is null
union all
select 'energy_readings_five_minutes', count(*)
  from public.energy_readings_five_minutes r
  left join public.user_profiles p on p.id = r.user_id
 where p.id is null;
