-- =============================================================
-- MIGRATION: Fix RLS blocking delete/update on payments & expenses
-- Run in Supabase SQL Editor — safe to re-run
--
-- ROOT CAUSE: set_session_role uses set_config(..., true) which
-- is transaction-local. Each REST API call is a separate transaction,
-- so the role is lost before the actual delete/update runs.
--
-- FIX: Create SECURITY DEFINER functions that set the role AND
-- perform the operation in the SAME transaction.
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. admin_delete_record — delete a row by id from any RLS table
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION admin_delete_record(
    p_table TEXT,
    p_id    TEXT,
    p_user_role TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    -- Only Admin / Manager may use this function
    IF p_user_role NOT IN ('Admin', 'Manager') THEN
        RAISE EXCEPTION 'Permission denied: role "%" cannot delete records', p_user_role;
    END IF;

    PERFORM set_config('app.user_role', p_user_role, true);

    EXECUTE format('DELETE FROM %I WHERE id = $1', p_table) USING p_id;
END;
$$;


-- ─────────────────────────────────────────────────────────────
-- 2. admin_update_record — update a row by id from any RLS table
--    Handles both text and JSONB column values correctly.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION admin_update_record(
    p_table     TEXT,
    p_id        TEXT,
    p_data      JSONB,
    p_user_role TEXT
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    col_key   TEXT;
    col_val   JSONB;
    set_parts TEXT[];
    sql_str   TEXT;
BEGIN
    IF p_user_role NOT IN ('Admin', 'Manager') THEN
        RAISE EXCEPTION 'Permission denied: role "%" cannot update records', p_user_role;
    END IF;

    PERFORM set_config('app.user_role', p_user_role, true);

    -- Build SET clause — correctly handle different JSON types
    FOR col_key IN SELECT jsonb_object_keys(p_data)
    LOOP
        col_val := p_data -> col_key;

        IF col_val IS NULL OR col_val = 'null'::jsonb THEN
            set_parts := array_append(set_parts, format('%I = NULL', col_key));
        ELSIF jsonb_typeof(col_val) = 'object' OR jsonb_typeof(col_val) = 'array' THEN
            -- JSONB columns: cast the value as jsonb
            set_parts := array_append(set_parts, format('%I = %L::jsonb', col_key, col_val::text));
        ELSIF jsonb_typeof(col_val) = 'number' THEN
            set_parts := array_append(set_parts, format('%I = %s', col_key, col_val #>> '{}'));
        ELSIF jsonb_typeof(col_val) = 'boolean' THEN
            set_parts := array_append(set_parts, format('%I = %s', col_key, col_val #>> '{}'));
        ELSE
            -- string values
            set_parts := array_append(set_parts, format('%I = %L', col_key, col_val #>> '{}'));
        END IF;
    END LOOP;

    IF array_length(set_parts, 1) IS NULL OR array_length(set_parts, 1) = 0 THEN
        RETURN;
    END IF;

    sql_str := format('UPDATE %I SET %s WHERE id = %L',
                      p_table,
                      array_to_string(set_parts, ', '),
                      p_id);
    EXECUTE sql_str;
END;
$$;


-- ─────────────────────────────────────────────────────────────
-- 3. VERIFY (uncomment to check)
-- ─────────────────────────────────────────────────────────────

-- SELECT proname, prosecdef FROM pg_proc
-- WHERE proname IN ('admin_delete_record', 'admin_update_record');
