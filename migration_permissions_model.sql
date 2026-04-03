-- =========================================================
-- USER PERMISSIONS MODEL MIGRATION
-- Run this in Supabase SQL Editor before deploying the new
-- Users & Roles permission model.
-- =========================================================

ALTER TABLE public.users ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS roles JSONB DEFAULT '[]'::jsonb;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS monthly_target NUMERIC DEFAULT 0;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS extra_perms JSONB DEFAULT '[]'::jsonb;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS can_edit BOOLEAN DEFAULT false;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS allow_perms JSONB DEFAULT '[]'::jsonb;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS deny_perms JSONB DEFAULT '[]'::jsonb;

UPDATE public.users
SET user_id = COALESCE(NULLIF(user_id, ''), LOWER(REGEXP_REPLACE(name, '\s+', '', 'g')))
WHERE COALESCE(user_id, '') = '';

UPDATE public.users
SET roles = to_jsonb(ARRAY[COALESCE(NULLIF(role, ''), 'Salesman')])
WHERE roles IS NULL
   OR jsonb_typeof(roles) <> 'array'
   OR jsonb_array_length(roles) = 0;

UPDATE public.users
SET extra_perms = '[]'::jsonb
WHERE extra_perms IS NULL OR jsonb_typeof(extra_perms) <> 'array';

UPDATE public.users
SET allow_perms = '[]'::jsonb
WHERE allow_perms IS NULL OR jsonb_typeof(allow_perms) <> 'array';

UPDATE public.users
SET deny_perms = '[]'::jsonb
WHERE deny_perms IS NULL OR jsonb_typeof(deny_perms) <> 'array';

CREATE INDEX IF NOT EXISTS idx_users_user_id_lower
ON public.users ((LOWER(user_id)));
