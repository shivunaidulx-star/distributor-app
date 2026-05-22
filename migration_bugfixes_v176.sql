-- =============================================================
-- MIGRATION: v176 bug fixes for attendance, payroll, cheques
-- Run this in Supabase SQL Editor
-- Safe to re-run
--
-- Adds:
-- - attendance upsert RPC + unique staff/date guard
-- - atomic payroll pay/reset RPCs
-- - cheque status RPC with Closed support
-- - lightweight activity/audit log table
-- =============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS public.activity_logs (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    user_id TEXT,
    user_name TEXT,
    action TEXT,
    table_name TEXT,
    record_id TEXT,
    old_data JSONB,
    new_data JSONB,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS public.attendance (
    id TEXT PRIMARY KEY,
    staff_id TEXT,
    staff_name TEXT,
    date DATE,
    status TEXT,
    marked_by TEXT
);

CREATE TABLE IF NOT EXISTS public.salary_advances (
    id TEXT PRIMARY KEY,
    staff_id TEXT,
    staff_name TEXT,
    date DATE,
    amount NUMERIC DEFAULT 0,
    deducted NUMERIC DEFAULT 0,
    month TEXT,
    notes TEXT,
    paid_by TEXT
);

CREATE TABLE IF NOT EXISTS public.salary_records (
    id TEXT PRIMARY KEY,
    staff_id TEXT,
    staff_name TEXT,
    month TEXT,
    monthly_salary NUMERIC DEFAULT 0,
    working_days NUMERIC DEFAULT 0,
    days_present NUMERIC DEFAULT 0,
    earned_salary NUMERIC DEFAULT 0,
    advances NUMERIC DEFAULT 0,
    net_payable NUMERIC DEFAULT 0,
    status TEXT DEFAULT 'paid',
    paid_date DATE,
    paid_by TEXT
);

ALTER TABLE public.attendance ADD COLUMN IF NOT EXISTS marked_by TEXT;
ALTER TABLE public.attendance ADD COLUMN IF NOT EXISTS staff_name TEXT;

ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_no TEXT;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_bank TEXT;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_status TEXT DEFAULT 'Pending';
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_deposit_date DATE;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_closed_date DATE;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_status_note TEXT;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_status_by TEXT;
ALTER TABLE public.payments ADD COLUMN IF NOT EXISTS cheque_status_at TIMESTAMPTZ;

ALTER TABLE public.salary_advances ADD COLUMN IF NOT EXISTS deducted NUMERIC DEFAULT 0;
ALTER TABLE public.salary_advances ALTER COLUMN deducted SET DEFAULT 0;

ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS staff_id TEXT;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS staff_name TEXT;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS month TEXT;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS monthly_salary NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS working_days NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS days_present NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS earned_salary NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS advances NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS net_payable NUMERIC DEFAULT 0;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'paid';
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS paid_date DATE;
ALTER TABLE public.salary_records ADD COLUMN IF NOT EXISTS paid_by TEXT;

-- Clean duplicates before adding unique guards. Keep the highest text id as the latest row.
DELETE FROM public.attendance a
USING (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY staff_id, date
               ORDER BY COALESCE(id, '') DESC
           ) AS rn
    FROM public.attendance
    WHERE staff_id IS NOT NULL AND date IS NOT NULL
) d
WHERE a.ctid = d.ctid
  AND d.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_attendance_staff_date_unique
ON public.attendance(staff_id, date);

DELETE FROM public.salary_records s
USING (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY staff_id, month
               ORDER BY COALESCE(id, '') DESC
           ) AS rn
    FROM public.salary_records
    WHERE staff_id IS NOT NULL AND month IS NOT NULL
) d
WHERE s.ctid = d.ctid
  AND d.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_salary_records_staff_month_unique
ON public.salary_records(staff_id, month);

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

CREATE OR REPLACE FUNCTION public.upsert_attendance_entry(
    p_staff_id TEXT,
    p_staff_name TEXT,
    p_date DATE,
    p_status TEXT,
    p_marked_by TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_row public.attendance%ROWTYPE;
BEGIN
    IF COALESCE(p_staff_id, '') = '' THEN
        RAISE EXCEPTION 'staff_id is required';
    END IF;
    IF p_date IS NULL THEN
        RAISE EXCEPTION 'date is required';
    END IF;
    IF COALESCE(p_status, '') NOT IN ('Present', 'Absent', 'Half Day', 'Paid Leave', 'Holiday') THEN
        RAISE EXCEPTION 'Invalid attendance status: %', p_status;
    END IF;

    INSERT INTO public.attendance (id, staff_id, staff_name, date, status, marked_by)
    VALUES (
        'att_' || extract(epoch FROM clock_timestamp())::bigint || '_' || substr(md5(random()::text), 1, 8),
        p_staff_id,
        p_staff_name,
        p_date,
        p_status,
        p_marked_by
    )
    ON CONFLICT (staff_id, date)
    DO UPDATE SET
        staff_name = EXCLUDED.staff_name,
        status = EXCLUDED.status,
        marked_by = EXCLUDED.marked_by
    RETURNING * INTO v_row;

    PERFORM public.log_app_activity(NULL, p_marked_by, 'attendance_upsert', 'attendance', v_row.id, NULL, to_jsonb(v_row), NULL);

    RETURN to_jsonb(v_row);
END;
$$;

CREATE OR REPLACE FUNCTION public.update_cheque_status(
    p_payment_id TEXT,
    p_status TEXT,
    p_status_by TEXT DEFAULT NULL,
    p_note TEXT DEFAULT NULL,
    p_status_date DATE DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_old JSONB;
    v_row public.payments%ROWTYPE;
    v_status TEXT := INITCAP(TRIM(COALESCE(p_status, '')));
    v_date DATE := COALESCE(p_status_date, CURRENT_DATE);
BEGIN
    IF v_status NOT IN ('Pending', 'Deposited', 'Cleared', 'Closed', 'Bounced') THEN
        RAISE EXCEPTION 'Invalid cheque status: %', p_status;
    END IF;

    SELECT to_jsonb(p.*) INTO v_old
    FROM public.payments p
    WHERE p.id::text = p_payment_id
    FOR UPDATE;

    IF v_old IS NULL THEN
        RAISE EXCEPTION 'Payment not found';
    END IF;

    UPDATE public.payments
    SET cheque_status = v_status,
        cheque_deposit_date = CASE
            WHEN v_status IN ('Deposited', 'Cleared', 'Closed') THEN COALESCE(cheque_deposit_date, v_date)
            WHEN v_status = 'Pending' THEN NULL
            ELSE cheque_deposit_date
        END,
        cheque_closed_date = CASE
            WHEN v_status IN ('Cleared', 'Closed', 'Bounced') THEN v_date
            WHEN v_status IN ('Pending', 'Deposited') THEN NULL
            ELSE cheque_closed_date
        END,
        cheque_status_note = NULLIF(p_note, ''),
        cheque_status_by = NULLIF(p_status_by, ''),
        cheque_status_at = NOW()
    WHERE id::text = p_payment_id
    RETURNING * INTO v_row;

    PERFORM public.log_app_activity(NULL, p_status_by, 'cheque_status_update', 'payments', v_row.id, v_old, to_jsonb(v_row), p_note);

    RETURN to_jsonb(v_row);
END;
$$;

CREATE OR REPLACE FUNCTION public.mark_salary_paid_atomic(
    p_staff_id TEXT,
    p_staff_name TEXT,
    p_month TEXT,
    p_monthly_salary NUMERIC,
    p_working_days NUMERIC,
    p_days_present NUMERIC,
    p_earned_salary NUMERIC,
    p_requested_deduction NUMERIC,
    p_paid_by TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_existing public.salary_records%ROWTYPE;
    v_adv RECORD;
    v_pending NUMERIC := 0;
    v_remaining NUMERIC := 0;
    v_deduct_now NUMERIC := 0;
    v_final_deduction NUMERIC := 0;
    v_net NUMERIC := 0;
    v_salary public.salary_records%ROWTYPE;
BEGIN
    IF COALESCE(p_staff_id, '') = '' THEN
        RAISE EXCEPTION 'staff_id is required';
    END IF;
    IF COALESCE(p_month, '') = '' THEN
        RAISE EXCEPTION 'month is required';
    END IF;

    SELECT * INTO v_existing
    FROM public.salary_records
    WHERE staff_id = p_staff_id AND month = p_month
    FOR UPDATE;

    IF FOUND THEN
        IF v_existing.status = 'paid' THEN
            RAISE EXCEPTION 'Salary already marked paid for this month';
        END IF;
        DELETE FROM public.salary_records WHERE id = v_existing.id;
    END IF;

    SELECT COALESCE(SUM(GREATEST(0, COALESCE(amount, 0) - COALESCE(deducted, 0))), 0)
    INTO v_pending
    FROM public.salary_advances
    WHERE staff_id = p_staff_id;

    v_final_deduction := LEAST(
        GREATEST(0, COALESCE(p_requested_deduction, 0)),
        GREATEST(0, v_pending),
        GREATEST(0, COALESCE(p_earned_salary, 0))
    );
    v_remaining := v_final_deduction;
    v_net := GREATEST(0, COALESCE(p_earned_salary, 0) - v_final_deduction);

    INSERT INTO public.salary_records (
        id, staff_id, staff_name, month, monthly_salary, working_days,
        days_present, earned_salary, advances, net_payable,
        status, paid_date, paid_by
    )
    VALUES (
        'sal_' || extract(epoch FROM clock_timestamp())::bigint || '_' || substr(md5(random()::text), 1, 8),
        p_staff_id, p_staff_name, p_month, COALESCE(p_monthly_salary, 0), COALESCE(p_working_days, 0),
        COALESCE(p_days_present, 0), COALESCE(p_earned_salary, 0), v_final_deduction, v_net,
        'paid', CURRENT_DATE, p_paid_by
    )
    RETURNING * INTO v_salary;

    FOR v_adv IN
        SELECT id, COALESCE(amount, 0) AS amount, COALESCE(deducted, 0) AS deducted
        FROM public.salary_advances
        WHERE staff_id = p_staff_id
          AND (COALESCE(amount, 0) - COALESCE(deducted, 0)) > 0.01
        ORDER BY date ASC, id ASC
        FOR UPDATE
    LOOP
        EXIT WHEN v_remaining <= 0;
        v_deduct_now := LEAST(v_adv.amount - v_adv.deducted, v_remaining);
        UPDATE public.salary_advances
        SET deducted = ROUND((v_adv.deducted + v_deduct_now)::numeric, 2)
        WHERE id = v_adv.id;
        v_remaining := ROUND((v_remaining - v_deduct_now)::numeric, 2);
    END LOOP;

    PERFORM public.log_app_activity(NULL, p_paid_by, 'salary_paid', 'salary_records', v_salary.id, NULL, to_jsonb(v_salary), NULL);

    RETURN to_jsonb(v_salary);
END;
$$;

CREATE OR REPLACE FUNCTION public.reset_salary_paid_atomic(
    p_staff_id TEXT,
    p_month TEXT,
    p_reset_by TEXT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_salary public.salary_records%ROWTYPE;
    v_adv RECORD;
    v_remaining NUMERIC := 0;
    v_reverse_now NUMERIC := 0;
    v_old JSONB;
BEGIN
    SELECT * INTO v_salary
    FROM public.salary_records
    WHERE staff_id = p_staff_id AND month = p_month
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Salary record not found';
    END IF;

    v_old := to_jsonb(v_salary);
    v_remaining := GREATEST(0, COALESCE(v_salary.advances, 0));

    FOR v_adv IN
        SELECT id, COALESCE(deducted, 0) AS deducted
        FROM public.salary_advances
        WHERE staff_id = p_staff_id
          AND COALESCE(deducted, 0) > 0
        ORDER BY date DESC, id DESC
        FOR UPDATE
    LOOP
        EXIT WHEN v_remaining <= 0;
        v_reverse_now := LEAST(v_adv.deducted, v_remaining);
        UPDATE public.salary_advances
        SET deducted = ROUND((v_adv.deducted - v_reverse_now)::numeric, 2)
        WHERE id = v_adv.id;
        v_remaining := ROUND((v_remaining - v_reverse_now)::numeric, 2);
    END LOOP;

    DELETE FROM public.salary_records WHERE id = v_salary.id;

    PERFORM public.log_app_activity(NULL, p_reset_by, 'salary_reset', 'salary_records', v_salary.id, v_old, NULL, NULL);

    RETURN v_old || jsonb_build_object('reset', true);
END;
$$;

DO $$
DECLARE
    v_table TEXT;
    v_tables CONSTANT TEXT[] := ARRAY[
        'activity_logs',
        'attendance',
        'salary_advances',
        'salary_records',
        'payments'
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
        EXECUTE format('DROP POLICY IF EXISTS app_custom_auth_all ON public.%I', v_table);
        EXECUTE format(
            'CREATE POLICY app_custom_auth_all ON public.%I FOR ALL TO anon, authenticated USING (true) WITH CHECK (true)',
            v_table
        );
    END LOOP;
END $$;

GRANT EXECUTE ON FUNCTION public.log_app_activity(TEXT, TEXT, TEXT, TEXT, TEXT, JSONB, JSONB, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.upsert_attendance_entry(TEXT, TEXT, DATE, TEXT, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.update_cheque_status(TEXT, TEXT, TEXT, TEXT, DATE) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.mark_salary_paid_atomic(TEXT, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.reset_salary_paid_atomic(TEXT, TEXT, TEXT) TO anon, authenticated;

NOTIFY pgrst, 'reload schema';

COMMIT;
