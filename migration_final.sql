-- ============================================================
-- Migration: Delivery & Party Feature Extensions
-- Run in Supabase SQL Editor
-- ============================================================

-- 1. Delivery table: Enhancement for re-dispatches and returns
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS re_dispatch_of TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS returned_at DATE;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS cancel_reason TEXT;

-- 2. Parties table: group/category and area/route for filtering
ALTER TABLE parties ADD COLUMN IF NOT EXISTS "group" TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS area TEXT;

-- 3. Invoices RLS: Allow status updates to 'cancelled'
-- This ensures the Delivery module can cancel invoices even if the user isn't an admin.
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies 
        WHERE tablename = 'invoices' AND policyname = 'Allow cancel for authenticated users'
    ) THEN
        CREATE POLICY "Allow cancel for authenticated users"
        ON invoices
        FOR UPDATE
        TO authenticated
        USING (true)
        WITH CHECK (status = 'cancelled' OR (auth.jwt() ->> 'role' IN ('Admin', 'Manager')));
    END IF;
END $$;

-- 4. Verify columns were added
SELECT table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name IN ('delivery', 'parties')
  AND column_name IN ('re_dispatch_of', 'returned_at', 'cancel_reason', 'group', 'area')
ORDER BY table_name, column_name;
