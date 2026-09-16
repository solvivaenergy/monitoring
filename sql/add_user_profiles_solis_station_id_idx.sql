-- Speeds up Supabase lookups that filter user_profiles by mapped Solis stations.
-- Run this once in the Supabase SQL editor.

create index if not exists user_profiles_solis_station_id_idx on public.user_profiles (solis_station_id)
where
    solis_station_id is not null;