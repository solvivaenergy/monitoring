ALTER TABLE public.energy_readings_five_minutes
ADD COLUMN IF NOT EXISTS lifetime_earning NUMERIC DEFAULT 0;
