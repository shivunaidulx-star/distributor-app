-- =============================================================
-- MIGRATION: v175 live access repair after Supabase security fixes
-- Run this in Supabase SQL Editor
-- Safe to re-run
--
-- Context:
-- This app currently uses its own in-app PIN login with the public
-- Supabase anon key. If Supabase Auth-only grants/RLS were applied,
-- the app can read some tables but HR/payroll and write flows may be
-- hidden or blocked.
--
-- This repair keeps RLS enabled, but restores anon/authenticated access
-- for the app tables until the app is migrated to real Supabase Auth.
-- =============================================================

BEGIN;

GRANT USAGE ON SCHEMA public TO anon, authenticated;

DO $$
DECLARE
    v_table TEXT;
    v_policy CONSTANT TEXT := 'app_custom_auth_all';
    v_tables CONSTANT TEXT[] := ARRAY[
        'users',
        'parties',
        'inventory',
        'sales_orders',
        'purchase_orders',
        'invoices',
        'payments',
        'expenses',
        'stock_ledger',
        'party_ledger',
        'categories',
        'uom',
        'brands',
        'packers',
        'delivery_persons',
        'delivery',
        'settings',
        'staff',
        'attendance',
        'salary_records',
        'salary_advances',
        'customer_registrations',
        'customer_otps',
        'activity_logs',
        'no_series',
        'cash_handovers',
        'gps_logs'
    ];
BEGIN
    FOREACH v_table IN ARRAY v_tables
    LOOP
        IF to_regclass(format('public.%I', v_table)) IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.%I TO anon, authenticated',
            v_table
        );

        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', v_table);

        EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I', v_policy, v_table);
        EXECUTE format(
            'CREATE POLICY %I ON public.%I FOR ALL TO anon, authenticated USING (true) WITH CHECK (true)',
            v_policy,
            v_table
        );
    END LOOP;
END $$;

-- Sequence grants are harmless if no sequences exist, and useful for
-- any future id/number helper that uses serial/identity columns.
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO anon, authenticated;

-- Restore RPC execute grants required by the current app.
-- v174 intentionally revoked anon from admin helpers, but this app is not
-- using Supabase Auth yet, so that change blocks live admin/update flows.
DO $$
BEGIN
    IF to_regprocedure('public.set_session_role(text, text)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.set_session_role(TEXT, TEXT) TO anon, authenticated';
    END IF;

    IF to_regprocedure('public.admin_delete_record(text, text, text)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.admin_delete_record(TEXT, TEXT, TEXT) TO anon, authenticated';
    END IF;

    IF to_regprocedure('public.admin_update_record(text, text, jsonb, text)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.admin_update_record(TEXT, TEXT, JSONB, TEXT) TO anon, authenticated';
    END IF;

    IF to_regprocedure('public.admin_reset_app_data(text, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.admin_reset_app_data(TEXT, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN) TO anon, authenticated';
    END IF;

    IF to_regprocedure('public.create_cash_handover(jsonb)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.create_cash_handover(JSONB) TO anon, authenticated';
    END IF;

    IF to_regprocedure('public.get_next_no_fy(text)') IS NOT NULL THEN
        EXECUTE 'GRANT EXECUTE ON FUNCTION public.get_next_no_fy(TEXT) TO anon, authenticated';
    END IF;
END $$;

NOTIFY pgrst, 'reload schema';

COMMIT;
