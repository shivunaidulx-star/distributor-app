-- =============================================================
-- MIGRATION: v171 cash handovers RLS & Permissions
-- Purpose: Ensures the cash_handovers table is accessible to users
-- Run this in Supabase SQL Editor
-- Safe to re-run
-- =============================================================

-- 1. Grant basic usage privileges to standard roles
GRANT SELECT, INSERT, UPDATE, DELETE ON public.cash_handovers TO anon, authenticated, service_role;

-- 2. Enable Row Level Security (RLS)
ALTER TABLE public.cash_handovers ENABLE ROW LEVEL SECURITY;

-- 3. Create generic policies so Authenticated users can view and edit
-- Drop existing policies if they exist (to make this safe to re-run)
DROP POLICY IF EXISTS "Enable read access for all users" ON public.cash_handovers;
DROP POLICY IF EXISTS "Enable insert for authenticated users" ON public.cash_handovers;
DROP POLICY IF EXISTS "Enable update for authenticated users" ON public.cash_handovers;
DROP POLICY IF EXISTS "Enable delete for authenticated users" ON public.cash_handovers;

-- Create open policies for authenticated users
CREATE POLICY "Enable read access for all users"
ON public.cash_handovers FOR SELECT
USING (true);

CREATE POLICY "Enable insert for authenticated users"
ON public.cash_handovers FOR INSERT
WITH CHECK (true);

CREATE POLICY "Enable update for authenticated users"
ON public.cash_handovers FOR UPDATE
USING (true);

CREATE POLICY "Enable delete for authenticated users"
ON public.cash_handovers FOR DELETE
USING (true);

-- 4. Reload PostgREST schema cache to apply permission changes immediately
NOTIFY pgrst, 'reload schema';
