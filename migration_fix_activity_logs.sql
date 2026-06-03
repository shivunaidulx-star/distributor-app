-- =============================================================
-- MIGRATION: Fix activity_logs table - add missing columns
-- Run this in Supabase SQL Editor
-- Safe to re-run (uses IF NOT EXISTS)
--
-- Fixes: "column 'user_id' of relation 'activity_logs' does not exist"
-- Root cause: CREATE TABLE IF NOT EXISTS skipped creation when
-- the table already existed with a different schema.
-- =============================================================

BEGIN;

-- Add all columns that might be missing from an older version of the table
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS user_name TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS action TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS table_name TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS record_id TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS old_data JSONB;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS new_data JSONB;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE public.activity_logs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

-- Re-create the log_app_activity function to make sure it matches
CREATE OR REPLACE FUNCTION public.log_app_activity(
    p_user_id TEXT,
    p_user_name TEXT,
    p_action TEXT,
    p_table_name TEXT,
    p_record_id TEXT,
    p_old_data JSONB DEFAULT NULL,
    p_new_data JSONB DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    INSERT INTO public.activity_logs (
        id, user_id, user_name, action, table_name, record_id, old_data, new_data, notes
    )
    VALUES (
        'log_' || extract(epoch FROM clock_timestamp())::bigint || '_' || substr(md5(random()::text), 1, 8),
        p_user_id, p_user_name, p_action, p_table_name, p_record_id, p_old_data, p_new_data, p_notes
    );
END;
$$;

-- Ensure permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.activity_logs TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.log_app_activity(TEXT, TEXT, TEXT, TEXT, TEXT, JSONB, JSONB, TEXT) TO anon, authenticated;

-- Ensure RLS policy exists
ALTER TABLE public.activity_logs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS app_custom_auth_all ON public.activity_logs;
CREATE POLICY app_custom_auth_all ON public.activity_logs FOR ALL TO anon, authenticated USING (true) WITH CHECK (true);

NOTIFY pgrst, 'reload schema';

COMMIT;
