-- =============================================================
-- MIGRATION: Fix Admin Reset / Admin RPC helpers
-- Run this in Supabase SQL Editor
-- Safe to re-run
-- =============================================================

-- 1. Session role helper
CREATE OR REPLACE FUNCTION public.set_session_role(user_role TEXT, user_id TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    PERFORM set_config('app.user_role', COALESCE(user_role, ''), true);
    PERFORM set_config('app.user_id', COALESCE(user_id, ''), true);
END;
$$;


-- 2. Admin delete helper
-- Fixes uuid/text mismatch by comparing id::text
CREATE OR REPLACE FUNCTION public.admin_delete_record(
    p_table TEXT,
    p_id TEXT,
    p_user_role TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_allowed_tables CONSTANT TEXT[] := ARRAY[
        'users','parties','inventory','sales_orders','purchase_orders',
        'invoices','payments','expenses','stock_ledger','party_ledger',
        'categories','uom','brands','packers','delivery_persons','delivery',
        'settings','staff','attendance','salary_records','salary_advances',
        'customer_registrations','customer_otps','activity_logs','no_series'
    ];
BEGIN
    IF COALESCE(p_user_role, '') NOT IN ('Admin', 'Manager') THEN
        RAISE EXCEPTION 'Permission denied: role "%" cannot delete records', p_user_role;
    END IF;

    IF NOT (p_table = ANY (v_allowed_tables)) THEN
        RAISE EXCEPTION 'Table "%" is not allowed for admin_delete_record', p_table;
    END IF;

    PERFORM set_config('app.user_role', p_user_role, true);

    EXECUTE format('DELETE FROM %I WHERE id::text = $1', p_table)
    USING p_id;
END;
$$;


-- 3. Admin update helper
-- Fixes uuid/text mismatch in WHERE clause
CREATE OR REPLACE FUNCTION public.admin_update_record(
    p_table TEXT,
    p_id TEXT,
    p_data JSONB,
    p_user_role TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_allowed_tables CONSTANT TEXT[] := ARRAY[
        'users','parties','inventory','sales_orders','purchase_orders',
        'invoices','payments','expenses','stock_ledger','party_ledger',
        'categories','uom','brands','packers','delivery_persons','delivery',
        'settings','staff','attendance','salary_records','salary_advances',
        'customer_registrations','customer_otps','activity_logs','no_series'
    ];
    col_key   TEXT;
    col_val   JSONB;
    set_parts TEXT[];
    sql_str   TEXT;
BEGIN
    IF COALESCE(p_user_role, '') NOT IN ('Admin', 'Manager') THEN
        RAISE EXCEPTION 'Permission denied: role "%" cannot update records', p_user_role;
    END IF;

    IF NOT (p_table = ANY (v_allowed_tables)) THEN
        RAISE EXCEPTION 'Table "%" is not allowed for admin_update_record', p_table;
    END IF;

    PERFORM set_config('app.user_role', p_user_role, true);

    FOR col_key IN SELECT jsonb_object_keys(COALESCE(p_data, '{}'::jsonb))
    LOOP
        col_val := p_data -> col_key;

        IF col_val IS NULL OR col_val = 'null'::jsonb THEN
            set_parts := array_append(set_parts, format('%I = NULL', col_key));
        ELSIF jsonb_typeof(col_val) IN ('object', 'array') THEN
            set_parts := array_append(set_parts, format('%I = %L::jsonb', col_key, col_val::text));
        ELSIF jsonb_typeof(col_val) = 'number' THEN
            set_parts := array_append(set_parts, format('%I = %s', col_key, col_val #>> '{}'));
        ELSIF jsonb_typeof(col_val) = 'boolean' THEN
            set_parts := array_append(set_parts, format('%I = %s', col_key, col_val #>> '{}'));
        ELSE
            set_parts := array_append(set_parts, format('%I = %L', col_key, col_val #>> '{}'));
        END IF;
    END LOOP;

    IF array_length(set_parts, 1) IS NULL OR array_length(set_parts, 1) = 0 THEN
        RETURN;
    END IF;

    sql_str := format(
        'UPDATE %I SET %s WHERE id::text = %L',
        p_table,
        array_to_string(set_parts, ', '),
        p_id
    );
    EXECUTE sql_str;
END;
$$;


-- 4. Admin full reset helper
-- Entries-only is the default behavior when p_reset_all = false.
CREATE OR REPLACE FUNCTION public.admin_reset_app_data(
    p_user_role TEXT,
    p_reset_all BOOLEAN DEFAULT false,
    p_delete_parties BOOLEAN DEFAULT false,
    p_delete_inventory BOOLEAN DEFAULT false,
    p_delete_categories BOOLEAN DEFAULT false,
    p_delete_uom BOOLEAN DEFAULT false,
    p_delete_delivery_persons BOOLEAN DEFAULT false,
    p_delete_packers BOOLEAN DEFAULT false,
    p_delete_users BOOLEAN DEFAULT false
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_count INTEGER := 0;
    v_summary JSONB := '{}'::jsonb;
BEGIN
    IF COALESCE(p_user_role, '') <> 'Admin' THEN
        RAISE EXCEPTION 'Permission denied: role "%" cannot reset data', p_user_role;
    END IF;

    PERFORM set_config('app.user_role', p_user_role, true);

    -- Delete child / dependent rows first
    DELETE FROM delivery;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('delivery_deleted', v_count);

    DELETE FROM payments;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('payments_deleted', v_count);

    DELETE FROM expenses;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('expenses_deleted', v_count);

    DELETE FROM invoices;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('invoices_deleted', v_count);

    DELETE FROM purchase_orders;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('purchase_orders_deleted', v_count);

    DELETE FROM sales_orders;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('sales_orders_deleted', v_count);

    DELETE FROM stock_ledger;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('stock_ledger_deleted', v_count);

    DELETE FROM party_ledger;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('party_ledger_deleted', v_count);

    UPDATE parties
    SET balance = 0;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('parties_balances_zeroed', v_count);

    UPDATE inventory
    SET stock = 0,
        reserved_qty = 0,
        batches = '[]'::jsonb;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('inventory_stock_zeroed', v_count);

    UPDATE no_series
    SET last_no = 0
    WHERE UPPER(COALESCE(code, '')) IN (
        'INVOICE',
        'SALES_ORDER',
        'PURCHASE_ORDER',
        'PURCHASE_INVOICE',
        'VYAPAR_INVOICE',
        'PAYMENT_IN',
        'PAYMENT_OUT',
        'EXPENSE'
    );
    GET DIAGNOSTICS v_count = ROW_COUNT;
    v_summary := v_summary || jsonb_build_object('document_series_reset', v_count);

    INSERT INTO settings(key, value)
    VALUES ('pay_settings', jsonb_build_object('currentNo', '1'))
    ON CONFLICT (key) DO UPDATE
    SET value = jsonb_set(
        CASE WHEN jsonb_typeof(settings.value) = 'object' THEN settings.value ELSE '{}'::jsonb END,
        '{currentNo}',
        to_jsonb('1'::text),
        true
    );

    INSERT INTO settings(key, value)
    VALUES ('vyapar_settings', jsonb_build_object('currentNo', '1'))
    ON CONFLICT (key) DO UPDATE
    SET value = jsonb_set(
        CASE WHEN jsonb_typeof(settings.value) = 'object' THEN settings.value ELSE '{}'::jsonb END,
        '{currentNo}',
        to_jsonb('1'::text),
        true
    );

    IF p_reset_all THEN
        IF p_delete_delivery_persons THEN DELETE FROM delivery_persons; END IF;
        IF p_delete_packers THEN DELETE FROM packers; END IF;
        IF p_delete_categories THEN DELETE FROM categories; END IF;
        IF p_delete_uom THEN DELETE FROM uom; END IF;
        IF p_delete_inventory THEN DELETE FROM inventory; END IF;
        IF p_delete_parties THEN
            DELETE FROM parties;
            INSERT INTO settings(key, value)
            VALUES ('db_number_series', jsonb_build_object('cust_start', 1, 'supp_start', 1))
            ON CONFLICT (key) DO UPDATE
            SET value = jsonb_set(
                jsonb_set(
                    CASE WHEN jsonb_typeof(settings.value) = 'object' THEN settings.value ELSE '{}'::jsonb END,
                    '{cust_start}', to_jsonb(1), true
                ),
                '{supp_start}', to_jsonb(1), true
            );
        END IF;
        IF p_delete_users THEN DELETE FROM users; END IF;
    END IF;

    RETURN v_summary || jsonb_build_object('status', 'ok');
END;
$$;


-- 5. Grants
GRANT EXECUTE ON FUNCTION public.set_session_role(TEXT, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_delete_record(TEXT, TEXT, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_update_record(TEXT, TEXT, JSONB, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_reset_app_data(
    TEXT, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN
) TO anon, authenticated;


-- 6. Refresh PostgREST schema cache
NOTIFY pgrst, 'reload schema';


-- 7. Optional verification
-- SELECT public.admin_reset_app_data(
--   p_user_role => 'Admin',
--   p_reset_all => false,
--   p_delete_parties => false,
--   p_delete_inventory => false,
--   p_delete_categories => false,
--   p_delete_uom => false,
--   p_delete_delivery_persons => false,
--   p_delete_packers => false,
--   p_delete_users => false
-- );
