-- =========================================================
-- DISTRIBUTOR APP — FULL SCHEMA SQL
-- Run this in Supabase SQL Editor
-- Safe to re-run: uses IF NOT EXISTS everywhere
-- =========================================================

-- ─────────────────────────────────────────────
-- 1. USERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    user_id TEXT,
    role TEXT DEFAULT 'Salesman',
    roles JSONB DEFAULT '[]',
    pin TEXT,
    monthly_target NUMERIC DEFAULT 0,
    extra_perms JSONB DEFAULT '[]',
    can_edit BOOLEAN DEFAULT false,
    allow_perms JSONB DEFAULT '[]',
    deny_perms JSONB DEFAULT '[]',
    dashboard_prefs JSONB DEFAULT '{}'
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS roles JSONB DEFAULT '[]';
ALTER TABLE users ADD COLUMN IF NOT EXISTS monthly_target NUMERIC DEFAULT 0;
ALTER TABLE users ADD COLUMN IF NOT EXISTS extra_perms JSONB DEFAULT '[]';
ALTER TABLE users ADD COLUMN IF NOT EXISTS can_edit BOOLEAN DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS allow_perms JSONB DEFAULT '[]';
ALTER TABLE users ADD COLUMN IF NOT EXISTS deny_perms JSONB DEFAULT '[]';
ALTER TABLE users ADD COLUMN IF NOT EXISTS dashboard_prefs JSONB DEFAULT '{}';

-- ─────────────────────────────────────────────
-- 2. PARTIES (Customers & Suppliers)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS parties (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT DEFAULT 'Customer',
    phone TEXT,
    gstin TEXT,
    address TEXT,
    city TEXT,
    lat NUMERIC,
    lng NUMERIC,
    balance NUMERIC DEFAULT 0,
    credit_limit NUMERIC DEFAULT 0,
    blocked BOOLEAN DEFAULT false,
    party_code TEXT,
    "group" TEXT,
    area TEXT
);
ALTER TABLE parties ADD COLUMN IF NOT EXISTS gstin TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS city TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS lat NUMERIC;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS lng NUMERIC;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS balance NUMERIC DEFAULT 0;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS credit_limit NUMERIC DEFAULT 0;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT false;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS post_code TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS payment_terms TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS party_code TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS "group" TEXT;
ALTER TABLE parties ADD COLUMN IF NOT EXISTS area TEXT;

-- ─────────────────────────────────────────────
-- 3. INVENTORY
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    item_code TEXT,
    category TEXT,
    sub_category TEXT,
    hsn TEXT,
    unit TEXT DEFAULT 'Pcs',
    sec_uom TEXT,
    sec_uom_ratio NUMERIC DEFAULT 0,
    purchase_price NUMERIC DEFAULT 0,
    sale_price NUMERIC DEFAULT 0,
    mrp NUMERIC DEFAULT 0,
    stock NUMERIC DEFAULT 0,
    reserved_qty NUMERIC DEFAULT 0,
    low_stock_alert NUMERIC DEFAULT 5,
    warehouse TEXT DEFAULT 'Main Warehouse',
    price_tiers JSONB DEFAULT '[]',
    batches JSONB DEFAULT '[]',
    photo TEXT,
    gst_rate NUMERIC DEFAULT 0
);
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS item_code TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS sub_category TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS hsn TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS sec_uom TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS sec_uom_ratio NUMERIC DEFAULT 0;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS mrp NUMERIC DEFAULT 0;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS reserved_qty NUMERIC DEFAULT 0;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS low_stock_alert NUMERIC DEFAULT 5;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS warehouse TEXT DEFAULT 'Main Warehouse';
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS price_tiers JSONB DEFAULT '[]';
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS batches JSONB DEFAULT '[]';
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS photo TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS image_url TEXT;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS gst_rate NUMERIC DEFAULT 0;
ALTER TABLE inventory ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT true;

-- ─────────────────────────────────────────────
-- 4. SALES ORDERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales_orders (
    id TEXT PRIMARY KEY,
    order_no TEXT,
    date DATE,
    expected_delivery_date DATE,
    priority TEXT DEFAULT 'Normal',
    is_urgent BOOLEAN DEFAULT false,
    party_id TEXT,
    party_name TEXT,
    items JSONB DEFAULT '[]',
    total NUMERIC DEFAULT 0,
    notes TEXT,
    status TEXT DEFAULT 'pending',
    created_by TEXT,
    approved_at TIMESTAMPTZ,
    packed BOOLEAN DEFAULT false,
    packed_by TEXT,
    packed_at TIMESTAMPTZ,
    packing_start_time TIMESTAMPTZ,
    packing_duration_mins INTEGER DEFAULT 0,
    assigned_packer TEXT,
    packed_items JSONB DEFAULT '[]',
    packed_total NUMERIC DEFAULT 0,
    box_count INTEGER DEFAULT 0,
    crate_count INTEGER DEFAULT 0,
    package_numbers JSONB DEFAULT '[]',
    cannot_complete BOOLEAN DEFAULT false,
    cannot_complete_reason TEXT,
    cannot_complete_notes TEXT,
    cannot_complete_by TEXT,
    cannot_complete_lines JSONB DEFAULT '[]',
    invoice_no TEXT,
    invoice_cancelled BOOLEAN DEFAULT false
);
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS expected_delivery_date DATE;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS priority TEXT DEFAULT 'Normal';
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS is_urgent BOOLEAN DEFAULT false;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packed BOOLEAN DEFAULT false;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packed_by TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packed_at TIMESTAMPTZ;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packing_start_time TIMESTAMPTZ;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packing_duration_mins INTEGER DEFAULT 0;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS assigned_packer TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packed_items JSONB DEFAULT '[]';
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS packed_total NUMERIC DEFAULT 0;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS box_count INTEGER DEFAULT 0;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS crate_count INTEGER DEFAULT 0;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS package_numbers JSONB DEFAULT '[]';
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cannot_complete BOOLEAN DEFAULT false;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cannot_complete_reason TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cannot_complete_notes TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cannot_complete_by TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS cannot_complete_lines JSONB DEFAULT '[]';
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS invoice_no TEXT;
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS invoice_cancelled BOOLEAN DEFAULT false;

-- ─────────────────────────────────────────────
-- 5. PURCHASE ORDERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS purchase_orders (
    id TEXT PRIMARY KEY,
    po_no TEXT,
    date DATE,
    party_id TEXT,
    party_name TEXT,
    items JSONB DEFAULT '[]',
    total NUMERIC DEFAULT 0,
    status TEXT DEFAULT 'pending',
    notes TEXT,
    created_by TEXT
);
ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS created_by TEXT;

-- ─────────────────────────────────────────────
-- 6. INVOICES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS invoices (
    id TEXT PRIMARY KEY,
    invoice_no TEXT,
    vyapar_invoice_no TEXT,
    date DATE,
    type TEXT DEFAULT 'sale',
    party_id TEXT,
    party_name TEXT,
    items JSONB DEFAULT '[]',
    subtotal NUMERIC DEFAULT 0,
    gst NUMERIC DEFAULT 0,
    total NUMERIC DEFAULT 0,
    status TEXT DEFAULT 'active',
    from_order TEXT,
    assigned_to TEXT,
    handover_date DATE,
    cancelled_at DATE,
    packed_items JSONB DEFAULT '[]',
    packed_total NUMERIC DEFAULT 0,
    created_by TEXT
);
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS vyapar_invoice_no TEXT;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS subtotal NUMERIC DEFAULT 0;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS gst NUMERIC DEFAULT 0;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS from_order TEXT;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS assigned_to TEXT;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS handover_date DATE;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS cancelled_at DATE;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS packed_items JSONB DEFAULT '[]';
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS packed_total NUMERIC DEFAULT 0;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS due_date DATE;

-- ─────────────────────────────────────────────
-- 7. PAYMENTS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payments (
    id TEXT PRIMARY KEY,
    pay_no TEXT,
    date DATE,
    type TEXT DEFAULT 'in',
    party_id TEXT,
    party_name TEXT,
    amount NUMERIC DEFAULT 0,
    mode TEXT DEFAULT 'Cash',
    invoice_no TEXT,
    note TEXT,
    collected_by TEXT,
    cheque_no TEXT,
    cheque_bank TEXT,
    cheque_status TEXT DEFAULT 'Pending',
    cheque_deposit_date DATE,
    allocations JSONB DEFAULT '{}',
    upi_ref TEXT,
    attachment_url TEXT,
    attachment_name TEXT,
    verification_status TEXT,
    verified_by TEXT,
    verified_at TIMESTAMPTZ,
    verification_note TEXT,
    created_by TEXT
);
ALTER TABLE payments ADD COLUMN IF NOT EXISTS pay_no TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS note TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS collected_by TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS cheque_no TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS cheque_bank TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS cheque_status TEXT DEFAULT 'Pending';
ALTER TABLE payments ADD COLUMN IF NOT EXISTS cheque_deposit_date DATE;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS allocations JSONB DEFAULT '{}';
ALTER TABLE payments ADD COLUMN IF NOT EXISTS discount NUMERIC DEFAULT 0;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS total_reduction NUMERIC DEFAULT 0;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS upi_ref TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS attachment_url TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS attachment_name TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS verification_status TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS verified_by TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS verification_note TEXT;

-- â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
-- 7B. CASH HANDOVERS
-- â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
CREATE TABLE IF NOT EXISTS cash_handovers (
    id TEXT PRIMARY KEY,
    collection_date DATE NOT NULL,
    handover_date DATE NOT NULL,
    salesman_name TEXT NOT NULL,
    salesman_id TEXT,
    expected_amount NUMERIC DEFAULT 0,
    declared_amount NUMERIC DEFAULT 0,
    counted_amount NUMERIC DEFAULT 0,
    variance_amount NUMERIC DEFAULT 0,
    denomination_counts JSONB DEFAULT '{}',
    admin_denomination_counts JSONB DEFAULT '{}',
    payment_row_ids JSONB DEFAULT '[]',
    payment_refs JSONB DEFAULT '[]',
    status TEXT DEFAULT 'submitted',
    note TEXT,
    admin_note TEXT,
    submitted_at TIMESTAMPTZ DEFAULT NOW(),
    submitted_by TEXT,
    received_by TEXT,
    received_at TIMESTAMPTZ,
    created_by TEXT
);

-- ─────────────────────────────────────────────
-- 8. EXPENSES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS expenses (
    id TEXT PRIMARY KEY,
    date DATE,
    category TEXT,
    note TEXT,
    amount NUMERIC DEFAULT 0,
    created_by TEXT
);
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS note TEXT;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS description TEXT;

-- ─────────────────────────────────────────────
-- 9. STOCK LEDGER
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_ledger (
    id TEXT PRIMARY KEY,
    date DATE,
    item_id TEXT,
    item_name TEXT,
    entry_type TEXT,
    qty NUMERIC DEFAULT 0,
    running_stock NUMERIC DEFAULT 0,
    document_no TEXT,
    reason TEXT,
    mrp NUMERIC,
    created_by TEXT
);
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS mrp NUMERIC;
ALTER TABLE stock_ledger ADD COLUMN IF NOT EXISTS running_stock NUMERIC DEFAULT 0;

-- ─────────────────────────────────────────────
-- 10. PARTY LEDGER
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS party_ledger (
    id TEXT PRIMARY KEY,
    date DATE,
    party_id TEXT,
    party_name TEXT,
    type TEXT,
    amount NUMERIC DEFAULT 0,
    balance NUMERIC DEFAULT 0,
    doc_no TEXT,
    notes TEXT,
    created_by TEXT
);
ALTER TABLE party_ledger ADD COLUMN IF NOT EXISTS balance NUMERIC DEFAULT 0;
ALTER TABLE party_ledger ADD COLUMN IF NOT EXISTS notes TEXT;

-- ─────────────────────────────────────────────
-- 11. CATEGORIES
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS categories (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sub_categories JSONB DEFAULT '[]'
);
ALTER TABLE categories ADD COLUMN IF NOT EXISTS sub_categories JSONB DEFAULT '[]';

-- ─────────────────────────────────────────────
-- 12. UOM (Units of Measure)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS uom (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- ─────────────────────────────────────────────
-- 13. PACKERS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS packers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- ─────────────────────────────────────────────
-- 14. DELIVERY PERSONS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS delivery_persons (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

-- ─────────────────────────────────────────────
-- 15. DELIVERY
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS delivery (
    id TEXT PRIMARY KEY,
    order_id TEXT,
    order_no TEXT,
    invoice_no TEXT,
    invoice_date DATE,
    party_id TEXT,
    party_name TEXT,
    delivery_person TEXT,
    package_numbers JSONB DEFAULT '[]',
    items JSONB DEFAULT '[]',
    total NUMERIC DEFAULT 0,
    status TEXT DEFAULT 'Dispatched',
    dispatched_at DATE,
    delivered_at DATE,
    returned_at DATE,
    reason TEXT,
    cancel_reason TEXT,
    re_dispatch_of TEXT,
    notes TEXT,
    created_by TEXT
);
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS order_id TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS invoice_date DATE;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS package_numbers JSONB DEFAULT '[]';
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS items JSONB DEFAULT '[]';
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS total NUMERIC DEFAULT 0;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS dispatched_at DATE;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS delivered_at DATE;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS cancel_reason TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS delivery_location TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS delivery_lat NUMERIC;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS delivery_lng NUMERIC;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS re_dispatch_of TEXT;
ALTER TABLE delivery ADD COLUMN IF NOT EXISTS returned_at DATE;

-- ─────────────────────────────────────────────
-- 16. SETTINGS (company config, app preferences)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value JSONB DEFAULT '{}'
);

-- =========================================================
-- INDEXES for performance (optional but recommended)
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_invoices_party ON invoices(party_id);
CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(date);
CREATE INDEX IF NOT EXISTS idx_invoices_type ON invoices(type);
CREATE INDEX IF NOT EXISTS idx_payments_party ON payments(party_id);
CREATE INDEX IF NOT EXISTS idx_payments_date ON payments(date);
CREATE INDEX IF NOT EXISTS idx_cash_handovers_collection_date ON cash_handovers(collection_date);
CREATE INDEX IF NOT EXISTS idx_cash_handovers_handover_date ON cash_handovers(handover_date);
CREATE INDEX IF NOT EXISTS idx_cash_handovers_salesman_name ON cash_handovers(salesman_name);
CREATE INDEX IF NOT EXISTS idx_cash_handovers_status ON cash_handovers(status);

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

GRANT EXECUTE ON FUNCTION public.create_cash_handover(JSONB) TO anon, authenticated, service_role;
CREATE INDEX IF NOT EXISTS idx_stock_ledger_item ON stock_ledger(item_id);
CREATE INDEX IF NOT EXISTS idx_party_ledger_party ON party_ledger(party_id);
CREATE INDEX IF NOT EXISTS idx_sales_orders_status ON sales_orders(status);
CREATE INDEX IF NOT EXISTS idx_delivery_status ON delivery(status);

-- =========================================================
-- DONE — All 15 tables ready, safe to re-run anytime
-- =========================================================

-- ─────────────────────────────────────────────
-- 17. GPS LOGS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gps_logs (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    user_name TEXT,
    action TEXT,
    reference_id TEXT,
    lat NUMERIC,
    lng NUMERIC,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gps_logs_time ON gps_logs(timestamp);
