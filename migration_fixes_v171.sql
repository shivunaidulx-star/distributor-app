-- =============================================================
-- MIGRATION: v171 fixes
-- 1. Add cash_handovers to admin_update_record allowed tables
-- 2. Re-create create_cash_handover with correct column names
-- Run this in Supabase SQL Editor
-- Safe to re-run
-- =============================================================

-- Fix admin_update_record to allow cash_handovers table
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
        'customer_registrations','customer_otps','activity_logs','no_series',
        'cash_handovers'
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

-- Re-create create_cash_handover with correct column name (expected_amount, not expected_cash)
CREATE OR REPLACE FUNCTION public.create_cash_handover(p_data JSONB)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    inserted_row JSONB;
BEGIN
    INSERT INTO public.cash_handovers (
        id,
        collection_date,
        handover_date,
        salesman_name,
        salesman_id,
        expected_amount,
        declared_amount,
        counted_amount,
        variance_amount,
        denomination_counts,
        admin_denomination_counts,
        payment_row_ids,
        payment_refs,
        status,
        note,
        admin_note,
        submitted_at,
        submitted_by,
        received_by,
        received_at,
        created_by
    )
    VALUES (
        COALESCE(NULLIF(p_data->>'id', ''), md5(random()::text || clock_timestamp()::text)),
        NULLIF(p_data->>'collection_date', '')::DATE,
        NULLIF(p_data->>'handover_date', '')::DATE,
        COALESCE(NULLIF(p_data->>'salesman_name', ''), 'Unknown'),
        NULLIF(p_data->>'salesman_id', ''),
        COALESCE(NULLIF(p_data->>'expected_amount', ''), '0')::NUMERIC,
        COALESCE(NULLIF(p_data->>'declared_amount', ''), '0')::NUMERIC,
        COALESCE(NULLIF(p_data->>'counted_amount', ''), '0')::NUMERIC,
        COALESCE(NULLIF(p_data->>'variance_amount', ''), '0')::NUMERIC,
        COALESCE(p_data->'denomination_counts', '{}'::JSONB),
        COALESCE(p_data->'admin_denomination_counts', '{}'::JSONB),
        COALESCE(p_data->'payment_row_ids', '[]'::JSONB),
        COALESCE(p_data->'payment_refs', '[]'::JSONB),
        COALESCE(NULLIF(p_data->>'status', ''), 'submitted'),
        NULLIF(p_data->>'note', ''),
        NULLIF(p_data->>'admin_note', ''),
        COALESCE(NULLIF(p_data->>'submitted_at', '')::TIMESTAMPTZ, NOW()),
        NULLIF(p_data->>'submitted_by', ''),
        NULLIF(p_data->>'received_by', ''),
        NULLIF(p_data->>'received_at', '')::TIMESTAMPTZ,
        NULLIF(p_data->>'created_by', '')
    )
    RETURNING to_jsonb(cash_handovers.*) INTO inserted_row;

    RETURN inserted_row;
END;
$$;

-- Re-grant permissions
GRANT EXECUTE ON FUNCTION public.admin_update_record(TEXT, TEXT, JSONB, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.create_cash_handover(JSONB) TO anon, authenticated, service_role;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
