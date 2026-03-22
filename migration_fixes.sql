-- =============================================================
-- MIGRATION: Fix delete/edit permissions + missing columns
-- Run in Supabase SQL Editor — safe to re-run
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- STEP 1 — ADD MISSING COLUMNS
-- ─────────────────────────────────────────────────────────────

-- payments: status column (all payments are 'posted' when saved)
ALTER TABLE payments ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'posted';

-- expenses: status + party link + doc reference
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS status      TEXT DEFAULT 'manual';
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS party_id    TEXT;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS party_name  TEXT;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS doc_no      TEXT;


-- ─────────────────────────────────────────────────────────────
-- STEP 2 — BACKFILL STATUS ON EXISTING ROWS
-- ─────────────────────────────────────────────────────────────

-- Every payment entered via the app is posted
UPDATE payments
SET    status = 'posted'
WHERE  status IS NULL OR status = '';

-- Discount expenses auto-created by the app are posted
UPDATE expenses
SET    status = 'posted'
WHERE  category IN ('Payment Discount', 'Sales Discount')
  AND (status IS NULL OR status = '');

-- All remaining manual expenses
UPDATE expenses
SET    status = 'manual'
WHERE  status IS NULL OR status = '';


-- ─────────────────────────────────────────────────────────────
-- STEP 3 — set_session_role RPC
-- Stores the logged-in user's role in a session config variable
-- so RLS policies can read it via current_setting()
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION set_session_role(user_role TEXT, user_id TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    PERFORM set_config('app.user_role', user_role, true);
    PERFORM set_config('app.user_id',   user_id,   true);
END;
$$;


-- ─────────────────────────────────────────────────────────────
-- STEP 4 — RLS POLICIES
-- Enable RLS on the three tables that need it, then create
-- role-aware policies using the session variable set above.
-- ─────────────────────────────────────────────────────────────

-- Helper macro: is the caller Admin or Manager?
-- current_setting('app.user_role', true) returns NULL if not set (safe fallback)

-- ── payments ────────────────────────────────────────────────

ALTER TABLE payments ENABLE ROW LEVEL SECURITY;

-- All authenticated users can SELECT payments
DROP POLICY IF EXISTS "payments_select" ON payments;
CREATE POLICY "payments_select" ON payments
    FOR SELECT USING (true);

-- All authenticated users can INSERT payments
DROP POLICY IF EXISTS "payments_insert" ON payments;
CREATE POLICY "payments_insert" ON payments
    FOR INSERT WITH CHECK (true);

-- Only Admin / Manager can UPDATE payments
DROP POLICY IF EXISTS "payments_update" ON payments;
CREATE POLICY "payments_update" ON payments
    FOR UPDATE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    )
    WITH CHECK (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );

-- Only Admin / Manager can DELETE payments
DROP POLICY IF EXISTS "payments_delete" ON payments;
CREATE POLICY "payments_delete" ON payments
    FOR DELETE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );


-- ── expenses ────────────────────────────────────────────────

ALTER TABLE expenses ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "expenses_select" ON expenses;
CREATE POLICY "expenses_select" ON expenses
    FOR SELECT USING (true);

DROP POLICY IF EXISTS "expenses_insert" ON expenses;
CREATE POLICY "expenses_insert" ON expenses
    FOR INSERT WITH CHECK (true);

DROP POLICY IF EXISTS "expenses_update" ON expenses;
CREATE POLICY "expenses_update" ON expenses
    FOR UPDATE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    )
    WITH CHECK (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );

DROP POLICY IF EXISTS "expenses_delete" ON expenses;
CREATE POLICY "expenses_delete" ON expenses
    FOR DELETE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );


-- ── invoices ────────────────────────────────────────────────
-- Admin/Manager can edit posted invoices (like Vyapar)

ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "invoices_select" ON invoices;
CREATE POLICY "invoices_select" ON invoices
    FOR SELECT USING (true);

DROP POLICY IF EXISTS "invoices_insert" ON invoices;
CREATE POLICY "invoices_insert" ON invoices
    FOR INSERT WITH CHECK (true);

DROP POLICY IF EXISTS "invoices_update" ON invoices;
CREATE POLICY "invoices_update" ON invoices
    FOR UPDATE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    )
    WITH CHECK (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );

DROP POLICY IF EXISTS "invoices_delete" ON invoices;
CREATE POLICY "invoices_delete" ON invoices
    FOR DELETE
    USING (
        current_setting('app.user_role', true) IN ('Admin', 'Manager')
    );


-- ─────────────────────────────────────────────────────────────
-- STEP 5 — VERIFY (uncomment and run to confirm)
-- ─────────────────────────────────────────────────────────────

-- Column check
-- SELECT column_name, data_type, column_default
-- FROM   information_schema.columns
-- WHERE  table_name IN ('payments','expenses')
--   AND  column_name IN ('status','party_id','party_name','doc_no')
-- ORDER  BY table_name, column_name;

-- Data check
-- SELECT status, COUNT(*) FROM payments GROUP BY status;
-- SELECT status, COUNT(*) FROM expenses GROUP BY status;

-- Policy check
-- SELECT tablename, policyname, cmd, qual
-- FROM   pg_policies
-- WHERE  tablename IN ('payments','expenses','invoices')
-- ORDER  BY tablename, cmd;
