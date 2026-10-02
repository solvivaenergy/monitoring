-- 2026-10-02  19  Drop the pre-04a status check that still blocked 'decommissioned' and 'pending'
--
-- solar_systems carried TWO check constraints on status: the original
-- solar_systems_status_check (active | inactive | maintenance) and 04a's
-- solar_systems_status_chk (active | inactive | decommissioned | pending,
-- added NOT VALID). Postgres enforces both, so the only values that passed
-- were 'active' and 'inactive': the Status select's 'decommissioned' and
-- 'pending' options, and the detach flow's status := 'pending', failed with a
-- check violation. Found while decommissioning "Eduardo Rolle System 2"
-- (station 1298491919450134060, no longer in Solis) on 2026-10-02. All 680
-- rows are 'active', so validating 04a's constraint is a no-op scan.
--
-- Apply over a direct connection with autocommit (see sql/APPLIED.md).

alter table public.solar_systems drop constraint if exists solar_systems_status_check;
alter table public.solar_systems validate constraint solar_systems_status_chk;
