ALTER TABLE public.energy_readings
ADD COLUMN IF NOT EXISTS full_load_hours NUMERIC DEFAULT 0;