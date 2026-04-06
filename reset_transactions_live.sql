-- =============================================================
-- RESET ALL TRANSACTIONS — KEEP ALL MASTERS
-- Run in Supabase SQL Editor
-- Date: 2026-04-06
-- Purpose: Clear all test data before Go-Live
-- =============================================================
--
-- MASTERS KEPT (not touched):
--   users, parties, inventory, categories, uom,
--   packers, delivery_persons, staff, settings, no_series
--
-- TRANSACTIONS DELETED:
--   sales_orders, purchase_orders, invoices, payments,
--   expenses, delivery, stock_ledger, party_ledger,
--   gps_logs, customer_otps, customer_registrations
--
-- BALANCES RESET:
--   parties.balance -> 0
--   inventory.stock -> 0, reserved_qty -> 0, batches -> []
--   no_series.last_no -> 0
-- =============================================================

BEGIN;

-- -------------------------------------------------------------
-- 0. LIVE-SCHEMA SAFETY
-- Fixes the common error:
-- column "reserved_qty" of relation "inventory" does not exist
-- -------------------------------------------------------------

ALTER TABLE IF EXISTS inventory
    ADD COLUMN IF NOT EXISTS reserved_qty NUMERIC DEFAULT 0;

ALTER TABLE IF EXISTS inventory
    ADD COLUMN IF NOT EXISTS batches JSONB DEFAULT '[]'::jsonb;


-- -------------------------------------------------------------
-- 1. DELETE ALL TRANSACTION TABLES
-- -------------------------------------------------------------

DELETE FROM payments;
DELETE FROM expenses;
DELETE FROM delivery;
DELETE FROM invoices;
DELETE FROM purchase_orders;
DELETE FROM sales_orders;
DELETE FROM stock_ledger;
DELETE FROM party_ledger;
DELETE FROM gps_logs;
DELETE FROM customer_otps;
DELETE FROM customer_registrations;


-- -------------------------------------------------------------
-- 2. RESET BALANCES / STOCK ON MASTER TABLES
-- -------------------------------------------------------------

UPDATE parties
SET balance = 0;

UPDATE inventory
SET stock = 0,
    reserved_qty = 0,
    batches = '[]'::jsonb;


-- -------------------------------------------------------------
-- 3. RESET DOCUMENT NUMBER SERIES
-- -------------------------------------------------------------

UPDATE no_series
SET last_no = 0;

INSERT INTO settings(key, value)
VALUES ('pay_settings', '{"currentNo":"1"}'::jsonb)
ON CONFLICT (key) DO UPDATE
SET value = jsonb_set(
    CASE
        WHEN jsonb_typeof(settings.value) = 'object' THEN settings.value
        ELSE '{}'::jsonb
    END,
    '{currentNo}',
    '"1"'::jsonb,
    true
);

INSERT INTO settings(key, value)
VALUES ('vyapar_settings', '{"currentNo":"1"}'::jsonb)
ON CONFLICT (key) DO UPDATE
SET value = jsonb_set(
    CASE
        WHEN jsonb_typeof(settings.value) = 'object' THEN settings.value
        ELSE '{}'::jsonb
    END,
    '{currentNo}',
    '"1"'::jsonb,
    true
);

COMMIT;


-- -------------------------------------------------------------
-- 4. VERIFY
-- -------------------------------------------------------------

SELECT
    (SELECT COUNT(*) FROM sales_orders) AS sales_orders,
    (SELECT COUNT(*) FROM purchase_orders) AS purchase_orders,
    (SELECT COUNT(*) FROM invoices) AS invoices,
    (SELECT COUNT(*) FROM payments) AS payments,
    (SELECT COUNT(*) FROM expenses) AS expenses,
    (SELECT COUNT(*) FROM delivery) AS delivery,
    (SELECT COUNT(*) FROM stock_ledger) AS stock_ledger,
    (SELECT COUNT(*) FROM party_ledger) AS party_ledger,
    (SELECT COUNT(*) FROM gps_logs) AS gps_logs;

-- Expected: all zeros

SELECT
    (SELECT COUNT(*) FROM users) AS users,
    (SELECT COUNT(*) FROM parties) AS parties,
    (SELECT COUNT(*) FROM inventory) AS inventory,
    (SELECT COUNT(*) FROM categories) AS categories,
    (SELECT COUNT(*) FROM staff) AS staff,
    (SELECT COUNT(*) FROM packers) AS packers,
    (SELECT COUNT(*) FROM delivery_persons) AS delivery_persons;

-- Expected: masters remain intact
