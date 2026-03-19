/* ============================================
   DistroManager — Core Application (Refactored)
   ============================================ */

// --- Supabase Config ---
const SUPABASE_URL = 'https://pfukfcnxvrkefcmevcxq.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBmdWtmY254dnJrZWZjbWV2Y3hxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0NTk2MjksImV4cCI6MjA4OTAzNTYyOX0.tPCMJ431g5iHb9qkRSzMWlV0dL_iVPNXPnQjJ0DwZPw';
const supabaseClient = supabase.createClient(SUPABASE_URL, SUPABASE_KEY);

// --- Database Layer (Modified for Supabase) ---
const DB = {
    // ✅ Centralised Cache for Sync Access
    cache: {},

    // localStorage helper
    ls: {
        get(key) { try { return JSON.parse(localStorage.getItem(key)) || []; } catch(e) { return []; } },
        getObj(key) { try { return JSON.parse(localStorage.getItem(key)) || {}; } catch(e) { return {}; } },
        set(key, data) { localStorage.setItem(key, JSON.stringify(data)); }
    },

    async refresh() {
        // Core tables needed for Login & Dashboard basic structure
        const coreTables = ['users', 'categories', 'uom']; 
        const tables = ['parties', 'inventory', 'sales_orders', 'invoices', 'payments', 'expenses', 'party_ledger', 'stock_ledger', 'packers', 'delivery_persons', 'delivery'];
        
        // 1. Load settings and core tables FIRST - block the boot sequence for these
        try {
            const [settings, ...coreResults] = await Promise.all([
                this.loadSettings(),
                ...coreTables.map(t => supabaseClient.from(t).select('*'))
            ]);
            coreResults.forEach((res, i) => {
                if (!res.error) this.cache[coreTables[i]] = this._toCamel(res.data || []);
            });
        } catch(e) { console.error('Core Refresh Error:', e); }

        // 2. Start loading secondary tables in the background - DO NOT await here!
        // This allows the app to show the Login/Dashboard immediately.
        tables.forEach(t => {
            supabaseClient.from(t).select('*').then(res => {
                if (res.error) console.error(`Bg Refresh Error ${t}:`, res.error);
                else {
                    const camel = this._toCamel(res.data || []);
                    this.cache[t] = camel;
                    this.cache[`db_${t}`] = camel;
                    // Trigger a UI refresh if we are on a page that needs this data
                    const isCatalog = currentPage === 'catalog';
                    if (currentPage === t || (t === 'sales_orders' && currentPage === 'salesorders') || (isCatalog && (t === 'sales_orders' || t === 'inventory' || t === 'parties'))) {
                        console.log(`Bg loaded ${t}, refreshing UI...`);
                        clearTimeout(this._bgNavTimer);
                        this._bgNavTimer = setTimeout(() => navigateTo(currentPage), 300);
                    }
                    // Run data repair only after both invoices and sales_orders are cached
                    if (t === 'invoices' || t === 'sales_orders') {
                        if (this.cache['invoices'] && this.cache['sales_orders']) {
                            repairCancelledInvoiceOrders();
                        }
                    }
                }
            });
        });

        // Legacy map for what we have so far
        this.cache['db_users'] = this.cache['users'] || [];
        this.cache['db_categories'] = this.cache['categories'] || [];
        this.cache['db_uom'] = this.cache['uom'] || [];
    },

    // Refresh only specific tables — much faster than full refresh after saves
    async refreshTables(tableList) {
        const results = await Promise.all(tableList.map(t => supabaseClient.from(t).select('*')));
        results.forEach((res, i) => {
            if (res.error) { console.error(`Error refreshing ${tableList[i]}:`, res.error); return; }
            const t = tableList[i];
            const camel = this._toCamel(res.data || []);
            this.cache[t] = camel;
            this.cache[`db_${t}`] = camel;
            // Legacy camelCase keys for renamed tables
            if (t === 'sales_orders') this.cache['db_salesorders'] = camel;
            if (t === 'purchase_orders') this.cache['db_purchaseorders'] = camel;
        });
    },

    get(key) { 
        if (this.cache[key]) return this.cache[key];
        return this.ls.get(key); 
    },
    getObj(key) { 
        if (this.cache[key]) return this.cache[key];
        return this.ls.getObj(key); 
    },
    set(key, data) { return this.ls.set(key, data); },

    // New Async Supabase methods with Auto Mapping
    _toSnake(obj) {
        if (!obj || typeof obj !== 'object') return obj;
        if (Array.isArray(obj)) return obj.map(v => this._toSnake(v));
        const res = {};
        for (const key in obj) {
            const snake = key.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
            res[snake] = obj[key];
        }
        return res;
    },
    _toCamel(obj) {
        if (!obj || typeof obj !== 'object') return obj;
        if (Array.isArray(obj)) return obj.map(v => this._toCamel(v));
        const res = {};
        for (const key in obj) {
            const camel = key.replace(/([-_][a-z])/ig, ($1) => $1.toUpperCase().replace('-', '').replace('_', ''));
            res[camel] = obj[key];
        }
        return res;
    },

    // Convert empty strings to null for date/numeric columns so Postgres doesn't choke
    _clean(obj) {
        if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return obj;
        const res = {};
        for (const key in obj) {
            const v = obj[key];
            res[key] = (v === '' && (key.includes('date') || key.includes('at') || key.includes('_at'))) ? null : v;
        }
        return res;
    },

    async getAll(table) {
        // Handle legacy table name calls
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { data, error } = await supabaseClient.from(actualTable).select('*');
        if (error) { console.error(`Error fetching ${actualTable}:`, error); return []; }
        const camelData = this._toCamel(data) || [];
        this.cache[actualTable] = camelData;
        this.cache[`db_${table}`] = camelData; // Sync legacy key
        return camelData;
    },

    async insert(table, row) {
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { data, error } = await supabaseClient.from(actualTable).insert(this._clean(this._toSnake(row))).select();
        if (error) { console.error(`Error inserting into ${actualTable}:`, error.message, '|', error.details, '| sent:', JSON.stringify(this._toSnake(row))); throw error; }
        await this.refreshTables([actualTable]);
        return this._toCamel(data[0]);
    },

    async update(table, id, row) {
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { data, error } = await supabaseClient.from(actualTable).update(this._clean(this._toSnake(row))).eq('id', id).select();
        if (error) { console.error(`Error updating ${actualTable}:`, error.message, '|', error.details, '| sent:', JSON.stringify(this._toSnake(row))); throw error; }
        await this.refreshTables([actualTable]);
        return this._toCamel(data[0]);
    },

    async delete(table, id) {
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { error } = await supabaseClient.from(actualTable).delete().eq('id', id);
        if (error) { console.error(`Error deleting from ${actualTable}:`, error); throw error; }
        await this.refreshTables([actualTable]);
    },

    // ── Raw operations — NO auto-refresh (batch multiple then call DB.refresh() once) ──
    async rawUpdate(table, id, row) {
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { error } = await supabaseClient.from(actualTable).update(this._clean(this._toSnake(row))).eq('id', id);
        if (error) { console.error(`rawUpdate ${actualTable}:`, error.message); throw error; }
    },
    async rawInsert(table, row) {
        const actualTable = table.replace('salesorders', 'sales_orders').replace('purchaseorders', 'purchase_orders');
        const { data, error } = await supabaseClient.from(actualTable).insert(this._clean(this._toSnake(row))).select();
        if (error) { console.error(`rawInsert ${actualTable}:`, error.message); throw error; }
        return this._toCamel(data[0]);
    },

    // ── Settings: persisted in Supabase `settings` table AND localStorage ──
    async saveSettings(key, data) {
        this.ls.set(key, data); // always update local immediately
        try {
            const { error } = await supabaseClient.from('settings').upsert({ key, value: data }, { onConflict: 'key' });
            if (error) {
                console.warn('saveSettings cloud error:', error.message);
                // Show warning if it looks like table is missing or permission denied
                if (error.code === '42P01' || error.message?.includes('does not exist')) {
                    showToast('⚠️ Cloud sync failed: settings table missing. Run schema.sql in Supabase.', 'error');
                }
            }
        } catch(e) { console.warn('saveSettings error:', e); }
    },
    async loadSettings() {
        try {
            const { data, error } = await supabaseClient.from('settings').select('*');
            if (error) { console.warn('loadSettings cloud error:', error.message); return; }
            if (data && data.length > 0) {
                data.forEach(row => { this.ls.set(row.key, row.value); });
            }
        } catch(e) { console.warn('loadSettings error:', e); }
    },

    id() { 
        if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }
};

// ── Column Personalization Manager ──
const ColumnManager = {
    PAGES: {
        inventory: [
            { key: 'name',          label: 'Item Name',   required: true  },
            { key: 'abc',           label: 'ABC',         visible: true   },
            { key: 'warehouse',     label: 'Warehouse',   visible: false  },
            { key: 'hsn',           label: 'HSN',         visible: false  },
            { key: 'unit',          label: 'Unit',        visible: true   },
            { key: 'purchasePrice', label: 'Purchase ₹',  visible: true   },
            { key: 'salePrice',     label: 'Sale ₹',      visible: true   },
            { key: 'mrp',           label: 'MRP',         visible: false  },
            { key: 'stock',         label: 'Stock',       visible: true   },
            { key: 'reserved',      label: 'Reserved',    visible: true   },
            { key: 'avail',         label: 'Avail',       visible: true   },
            { key: 'value',         label: 'Value',       visible: true   },
            { key: 'actions',       label: 'Actions',     required: true  },
        ],
        parties: [
            { key: 'name',         label: 'Name',          required: true },
            { key: 'partyCode',    label: 'Party Code',    visible: true  },
            { key: 'type',         label: 'Type',          visible: true  },
            { key: 'phone',        label: 'Phone',         visible: true  },
            { key: 'city',         label: 'City',          visible: true  },
            { key: 'postCode',     label: 'Post Code',     visible: true  },
            { key: 'paymentTerms', label: 'Payment Terms', visible: true  },
            { key: 'gstin',        label: 'GSTIN',         visible: false },
            { key: 'balance',      label: 'Balance',       visible: true  },
            { key: 'actions',      label: 'Actions',       required: true },
            { key: 'address',      label: 'Address',       visible: false },
        ],
        salesorders: [
            { key: 'date',     label: 'Date',        visible: true  },
            { key: 'orderNo',  label: 'Order #',     required: true },
            { key: 'party',    label: 'Party',       visible: true  },
            { key: 'delivery', label: 'Delivery By', visible: true  },
            { key: 'items',    label: 'Items',       visible: false },
            { key: 'total',    label: 'Total',       visible: true  },
            { key: 'by',       label: 'Created By',  visible: false },
            { key: 'status',   label: 'Status',      visible: true  },
            { key: 'actions',  label: 'Actions',     required: true },
        ],
        purchaseorders: [
            { key: 'date',    label: 'Date',     visible: true  },
            { key: 'poNo',    label: 'PO #',     required: true },
            { key: 'party',   label: 'Supplier', visible: true  },
            { key: 'items',   label: 'Items',    visible: false },
            { key: 'total',   label: 'Total',    visible: true  },
            { key: 'status',  label: 'Status',   visible: true  },
            { key: 'actions', label: 'Actions',  required: true },
        ],
        invoices: [
            { key: 'date',      label: 'Date',       visible: true  },
            { key: 'invoiceNo', label: 'Invoice #',  required: true },
            { key: 'party',     label: 'Party',      visible: true  },
            { key: 'type',      label: 'Type',       visible: true  },
            { key: 'status',    label: 'Status',     visible: true  },
            { key: 'items',     label: 'Items',      visible: false },
            { key: 'total',     label: 'Total',      visible: true  },
            { key: 'actions',   label: 'Actions',    required: true },
        ],
        payments: [
            { key: 'date',        label: 'Date',         visible: true  },
            { key: 'receiptNo',   label: 'Receipt #',    visible: true  },
            { key: 'party',       label: 'Party',        required: true },
            { key: 'type',        label: 'Type',         visible: true  },
            { key: 'invoiceNo',   label: 'Invoice',      visible: true  },
            { key: 'mode',        label: 'Mode',         visible: true  },
            { key: 'collectedBy', label: 'Collected By', visible: false },
            { key: 'amount',      label: 'Amount',       visible: true  },
            { key: 'actions',     label: 'Actions',      required: true },
        ],
        expenses: [
            { key: 'date',      label: 'Date',       visible: true  },
            { key: 'category',  label: 'Category',   visible: true  },
            { key: 'party',     label: 'Party',      visible: true  },
            { key: 'docNo',     label: 'Doc No',     visible: true  },
            { key: 'amount',    label: 'Amount',     visible: true  },
            { key: 'addedBy',   label: 'Added By',   visible: false },
            { key: 'actions',   label: 'Actions',    required: true },
        ],
        packing: [
            { key: 'orderNo',      label: 'Order #',     required: true },
            { key: 'date',         label: 'Date',        visible: true  },
            { key: 'party',        label: 'Party',       visible: true  },
            { key: 'items',        label: 'Items',       visible: false },
            { key: 'total',        label: 'Total',       visible: true  },
            { key: 'assignedTo',   label: 'Assigned To', visible: true  },
            { key: 'actions',      label: 'Actions',     required: true },
        ],
        delivery: [
            { key: 'orderNo',      label: 'Order #',    required: true },
            { key: 'invoiceNo',    label: 'Invoice',    visible: true  },
            { key: 'invoiceDate',  label: 'Inv Date',   visible: false },
            { key: 'party',        label: 'Party',      visible: true  },
            { key: 'location',     label: 'Location',   visible: true  },
            { key: 'phone',        label: 'Phone',      visible: true  },
            { key: 'person',       label: 'Person',     visible: true  },
            { key: 'packages',     label: 'Packages',   visible: false },
            { key: 'status',       label: 'Status',     visible: true  },
            { key: 'reason',       label: 'Reason',     visible: false },
            { key: 'actions',      label: 'Actions',    required: true },
        ],
    },
    get(page) {
        try {
            const saved = JSON.parse(localStorage.getItem('colcfg_' + page));
            if (saved && saved.length) return saved;
        } catch(e) {}
        return this.PAGES[page].map(c => ({ ...c, visible: c.required || c.visible !== false }));
    },
    save(page, cols) {
        localStorage.setItem('colcfg_' + page, JSON.stringify(cols));
    },
    reset(page) {
        localStorage.removeItem('colcfg_' + page);
    }
};

function openColumnPersonalizer(page, rerenderFn) {
    const cols = ColumnManager.get(page);
    const listId = 'col-personalize-list';

    const rows = cols.map((c, i) => `
        <div class="col-cfg-row" data-idx="${i}" style="display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border)">
            <span style="cursor:grab;color:var(--text-muted);font-size:1.1rem;user-select:none">⠿</span>
            <span style="flex:1;font-size:0.9rem;font-weight:500">${c.label}</span>
            <button class="btn btn-outline btn-sm" title="Move Up"    onclick="colMoveUp(${i},'${page}','${rerenderFn}')"    ${i===0?'disabled':''}>↑</button>
            <button class="btn btn-outline btn-sm" title="Move Down"  onclick="colMoveDown(${i},'${page}','${rerenderFn}')"  ${i===cols.length-1?'disabled':''}>↓</button>
            ${c.required
                ? `<span class="badge badge-outline" style="opacity:0.5;min-width:56px;text-align:center">Always</span>`
                : `<button class="btn btn-sm ${c.visible?'btn-primary':'btn-outline'}" onclick="colToggle(${i},'${page}','${rerenderFn}')" style="min-width:56px">${c.visible?'Visible':'Hidden'}</button>`
            }
        </div>`).join('');

    openModal('⚙️ Personalise Columns', `
        <p style="font-size:0.83rem;color:var(--text-muted);margin-bottom:14px">Show/hide columns and reorder them. Changes are saved to this device.</p>
        <div id="${listId}">${rows}</div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="colReset('${page}','${rerenderFn}')">↺ Reset Defaults</button>
            <button class="btn btn-outline" onclick="closeModal()">Close</button>
        </div>`);
}

function _colSave(page, rerenderFn) {
    // Read current state from DOM back into ColumnManager, then re-render
    const rows = document.querySelectorAll('#col-personalize-list .col-cfg-row');
    const cols = ColumnManager.get(page);
    // Rebuild ordered list from DOM order
    const ordered = Array.from(rows).map(row => {
        const idx = parseInt(row.dataset.idx);
        return cols[idx];
    });
    ColumnManager.save(page, ordered);
    closeModal();
    if (window[rerenderFn]) window[rerenderFn]();
}

function colToggle(idx, page, rerenderFn) {
    const cols = ColumnManager.get(page);
    cols[idx].visible = !cols[idx].visible;
    ColumnManager.save(page, cols);
    openColumnPersonalizer(page, rerenderFn);
}
function colMoveUp(idx, page, rerenderFn) {
    const cols = ColumnManager.get(page);
    if (idx > 0) { [cols[idx-1], cols[idx]] = [cols[idx], cols[idx-1]]; }
    ColumnManager.save(page, cols);
    openColumnPersonalizer(page, rerenderFn);
}
function colMoveDown(idx, page, rerenderFn) {
    const cols = ColumnManager.get(page);
    if (idx < cols.length-1) { [cols[idx], cols[idx+1]] = [cols[idx+1], cols[idx]]; }
    ColumnManager.save(page, cols);
    openColumnPersonalizer(page, rerenderFn);
}
function colReset(page, rerenderFn) {
    ColumnManager.reset(page);
    openColumnPersonalizer(page, rerenderFn);
}

// --- Real-time Notifications ---

// --- XSS-safe HTML escaping ---
function escapeHtml(str) {
    if (!str) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// --- Global save guard (prevents double-submit on mobile) ---
let _isSaving = false;
window._saveAndNew = false; // set true by "Save & New" buttons
function beginSave(btnSelector) {
    if (_isSaving) return false;
    _isSaving = true;
    document.querySelectorAll('.modal-footer .btn-primary, .modal-actions .btn-primary').forEach(b => {
        b.disabled = true; b.style.opacity = '0.65';
    });
    return true;
}
function endSave() {
    _isSaving = false;
    document.querySelectorAll('.modal-footer .btn-primary, .modal-actions .btn-primary').forEach(b => {
        b.disabled = false; b.style.opacity = '';
    });
}

// --- Collision-proof sequential number generator ---
const _nextNumLocks = {};
async function nextNumber(prefix) {
    // Prevent concurrent calls for same prefix
    if (_nextNumLocks[prefix]) {
        await _nextNumLocks[prefix];
    }
    let resolveLock;
    _nextNumLocks[prefix] = new Promise(r => { resolveLock = r; });

    try {
        const table = prefix === 'SO-' ? 'sales_orders' : 'invoices';
        const numField = prefix === 'SO-' ? 'order_no' : 'invoice_no';
        const { data } = await supabaseClient.from(table).select(numField);
        const allNums = (data || []).map(o => {
            const m = (o[numField] || '').match(/(\d+)$/);
            return m ? parseInt(m[1]) : 0;
        });
        const counters = DB.ls.getObj('db_counters');
        const maxExisting = allNums.length ? Math.max(...allNums) : 0;
        const current = Math.max(counters[prefix] || 0, maxExisting) + 1;
        counters[prefix] = current;
        DB.saveSettings('db_counters', counters); // saves to LS immediately + Supabase async
        return prefix + current.toString().padStart(4, '0');
    } finally {
        resolveLock();
        delete _nextNumLocks[prefix];
    }
}

// --- Toast Notification System ---
function showToast(message, type = 'success', duration = 3000) {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        container.style.cssText = 'position:fixed;top:20px;right:20px;z-index:10000;display:flex;flex-direction:column;gap:10px;pointer-events:none;';
        document.body.appendChild(container);
    }
    const icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
    const colors = { success: '#8b5cf6', error: '#f43f5e', warning: '#f59e0b', info: '#6366f1' };
    const borders = { success: '#a78bfa', error: '#fb7185', warning: '#fbbf24', info: '#818cf8' };
    const toast = document.createElement('div');
    toast.style.cssText = `pointer-events:auto;padding:14px 20px;border-radius:14px;color:#fff;font-size:0.9rem;font-family:Inter,sans-serif;font-weight:500;box-shadow:0 8px 32px rgba(0,0,0,0.4);display:flex;align-items:center;gap:12px;min-width:280px;max-width:420px;opacity:0;transform:translateX(40px);transition:all 0.35s cubic-bezier(0.16,1,0.3,1);background:${colors[type] || colors.info};border-left:4px solid ${borders[type] || borders.info};backdrop-filter:blur(16px);`;
    toast.innerHTML = `<span style="font-size:1.2rem;flex-shrink:0">${icons[type] || icons.info}</span><span style="flex:1">${escapeHtml(message)}</span>`;
    container.appendChild(toast);
    requestAnimationFrame(() => { toast.style.opacity = '1'; toast.style.transform = 'translateX(0)'; });
    setTimeout(() => {
        toast.style.opacity = '0'; toast.style.transform = 'translateX(40px)';
        setTimeout(() => toast.remove(), 350);
    }, duration);
}

// --- Init Storage Keys (no hardcoded data) ---
function initDefaults() {
    ['db_users', 'db_parties', 'db_inventory', 'db_invoices', 'db_payments', 'db_expenses', 'db_packing', 'db_delivery', 'db_salesorders', 'db_delivery_persons', 'db_packers', 'db_stock_ledger', 'db_party_ledger', 'db_uom', 'db_brands'].forEach(k => {
        if (!localStorage.getItem(k)) DB.set(k, []);
    });
    if (!localStorage.getItem('db_company')) DB.set('db_company', {});
    if (!localStorage.getItem('db_counters')) DB.set('db_counters', {});
}
initDefaults();

// --- Stock Ledger Helper (Consolidated) ---
const ADJUSTMENT_REASONS = ['Physical Count', 'Damaged Goods', 'Stock Correction', 'Expired Items', 'Sample/Gift', 'Warehouse Transfer', 'Production Use', 'Other'];
async function addLedgerEntry(itemId, itemName, entryType, qty, documentNo, reason) {
    const items = await DB.getAll('inventory');
    const item = items.find(x => x.id === itemId);
    await DB.insert('stock_ledger', {
        date: today(), itemId, itemName,
        entryType, qty, runningStock: item ? item.stock : 0,
        documentNo: (documentNo && typeof documentNo === 'object') ? JSON.stringify(documentNo) : (documentNo || ''),
        reason: reason || '',
        createdBy: currentUser ? currentUser.name : 'System'
    });
}

// --- Party Ledger Helper ---
async function addPartyLedgerEntry(partyId, partyName, type, amount, docNo, notes) {
    const parties = await DB.getAll('parties');
    const party = parties.find(x => x.id === partyId);
    await DB.insert('party_ledger', {
        date: today(), partyId, partyName,
        type, amount, balance: party ? party.balance : 0,
        docNo: (docNo && typeof docNo === 'object') ? JSON.stringify(docNo) : (docNo || ''),
        notes: notes || '',
        createdBy: currentUser ? currentUser.name : 'System'
    });
}

// --- Real-time Notifications ---
async function initRealtime() {
    try {
        supabaseClient
            .channel('app-changes')
            .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'sales_orders' }, payload => {
                showToast(`New Order Received: ${payload.new.order_no || payload.new.id}`, 'info');
                if (currentPage === 'salesorders') renderSalesOrders();
                if (currentPage === 'dashboard') renderDashboard();
            })
            .on('postgres_changes', { event: 'UPDATE', schema: 'public', table: 'sales_orders' }, payload => {
                const oldStatus = payload.old ? payload.old.status : null;
                if (oldStatus !== payload.new.status) {
                    showToast(`Order ${payload.new.order_no || payload.new.id} is now ${payload.new.status.toUpperCase()}`, 'info');
                    if (['salesorders', 'packing', 'delivery'].includes(currentPage)) navigateTo(currentPage);
                }
            })
            .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'invoices' }, payload => {
                showToast(`New Invoice Generated: ${payload.new.invoice_no}`, 'success');
                if (currentPage === 'invoices') renderInvoices();
            })
            .subscribe();
    } catch (err) {
        console.warn('Real-time subscription failed:', err);
    }
}

// --- Data Repair: fix sales orders orphaned by old cancel-invoice bug ---
async function repairCancelledInvoiceOrders() {
    try {
        const invoices = DB.cache['invoices'] || [];
        const orders = DB.cache['sales_orders'] || [];
        if (!invoices.length || !orders.length) return;

        // Use Map for O(1) lookup instead of find() O(N) inside loop
        const orderMap = new Map();
        orders.forEach(o => orderMap.set(o.orderNo, o));

        const repairs = [];
        invoices.forEach(function (inv) {
            if (inv.status === 'cancelled' && inv.fromOrder) {
                const order = orderMap.get(inv.fromOrder);
                if (order && order.packed && order.invoiceNo === inv.invoiceNo) {
                    repairs.push(DB.rawUpdate('sales_orders', order.id, {
                        packed: false, packedBy: null, packedAt: null, invoiceNo: null,
                        packedItems: null, packedTotal: null, invoiceCancelled: true
                    }));
                }
            }
        });
        if (repairs.length) { await Promise.all(repairs); await DB.refreshTables(['sales_orders']); }
    } catch (e) { console.warn('repairCancelledInvoiceOrders:', e.message); }
}

// --- Role Permissions ---
const ROLE_PAGES = {
    Admin: ['dashboard', 'parties', 'partyledger', 'inventorysetup', 'categories', 'uom', 'inventory', 'catalog', 'salesorders', 'purchaseorders', 'invoices', 'payments', 'expenses', 'packing', 'delivery', 'reports', 'packers', 'deliverypersons', 'users', 'setup', 'staffmaster', 'attendance', 'hrpayroll'],
    Manager: ['dashboard', 'parties', 'partyledger', 'inventorysetup', 'categories', 'uom', 'inventory', 'catalog', 'salesorders', 'purchaseorders', 'invoices', 'payments', 'expenses', 'packing', 'delivery', 'reports', 'packers', 'deliverypersons'],
    Salesman: ['dashboard', 'parties', 'inventory', 'catalog', 'salesorders', 'payments'],
    Delivery: ['dashboard', 'delivery'],
    Packing: ['dashboard', 'packing']
};

function getUserPages(user) {
    const roles = Array.isArray(user.roles) && user.roles.length ? user.roles : [user.role];
    const basePages = [...new Set(roles.flatMap(r => ROLE_PAGES[r] || []))];
    const extra = Array.isArray(user.extra_perms) ? user.extra_perms : [];
    return [...new Set([...basePages, ...extra])];
}

// --- State ---
let currentUser = null;
let currentPage = 'dashboard';
let currentLedgerPartyId = null;

// --- DOM Refs ---
const $ = id => document.getElementById(id);
const loginScreen = $('login-screen');
const setupWizard = $('setup-wizard');
const appEl = $('app');
const pageContent = $('page-content');
const pageTitle = $('page-title');
const sidebar = $('sidebar');

// =============================================
//  SETUP WIZARD (First Launch)
// =============================================
async function checkFirstLaunch() {
    const users = await DB.getAll('users');
    if (users.length === 0) {
        showSetupWizard();
    } else {
        await initRealtime();
        await showLoginScreen();
    }
}

function showSetupWizard() {
    setupWizard.classList.remove('hidden');
    loginScreen.classList.add('hidden');
    appEl.classList.add('hidden');
    renderSetupStep1();
}

function renderSetupStep1() {
    $('setup-step').innerHTML = `
        <h3 style="margin-bottom:16px;font-size:1rem">Step 1: Company Information</h3>
        <div class="form-group"><label>Company Name *</label><input id="sw-company" placeholder="Your Business Name"></div>
        <div class="form-row"><div class="form-group"><label>Phone</label><input id="sw-phone" placeholder="Phone Number"></div>
        <div class="form-group"><label>GSTIN</label><input id="sw-gstin" placeholder="GST Number"></div></div>
        <div class="form-group"><label>Address</label><input id="sw-address" placeholder="Business Address"></div>
        <div class="form-group"><label>City</label><input id="sw-city" placeholder="City"></div>
        <button class="btn btn-primary btn-block" onclick="saveSetupStep1()">Next →</button>`;
}

async function saveSetupStep1() {
    const name = $('sw-company').value.trim();
    if (!name) return alert('Company name is required');

    // We'll store company info in a special table or just as a single row in a 'metadata' table
    // For now, let's keep basic company info in localStorage or a 'setup' table.
    // Let's assume a 'db_company' table exists in the future, but for now we'll push on.
    const coData = {
        name,
        phone: $('sw-phone').value.trim(),
        gstin: $('sw-gstin').value.trim(),
        address: $('sw-address').value.trim(),
        city: $('sw-city').value.trim()
    };
    await DB.saveSettings('db_company', coData);
    renderSetupStep2();
}

function renderSetupStep2() {
    $('setup-step').innerHTML = `
        <h3 style="margin-bottom:16px;font-size:1rem">Step 2: Create Admin User</h3>
        <div class="form-group"><label>Admin Name *</label><input id="sw-admin-name" placeholder="Your Full Name"></div>
        <div class="form-group"><label>User ID * <span style="font-size:0.78rem;color:var(--text-muted)">(used to login — e.g. admin, ram01)</span></label><input id="sw-admin-userid" placeholder="e.g. admin" style="text-transform:lowercase" oninput="this.value=this.value.toLowerCase().replace(/\\s/g,'')"></div>
        <div class="form-group"><label>PIN * <span style="font-size:0.78rem;color:var(--text-muted)">(4 to 6 digits)</span></label><input type="password" id="sw-admin-pin" maxlength="6" placeholder="e.g. 1234 or 123456" inputmode="numeric"></div>
        <button class="btn btn-primary btn-block" onclick="completeSetup()">Complete Setup ✓</button>`;
}

async function completeSetup() {
    const name = $('sw-admin-name').value.trim();
    const userId = ($('sw-admin-userid') ? $('sw-admin-userid').value.trim() : '').toLowerCase().replace(/\s/g,'') || 'admin';
    const pin = $('sw-admin-pin').value.trim();
    if (!name) return alert('Name is required');
    if (!pin || pin.length < 4 || pin.length > 6 || !/^\d+$/.test(pin)) return alert('PIN must be 4 to 6 digits (numbers only)');

    await DB.insert('users', { name, userId, role: 'Admin', roles: ['Admin'], pin });
    setupWizard.classList.add('hidden');
    await showLoginScreen();
}

// --- Session Persistence ---
function dmSaveSession(user) {
    const session = {
        user: user,
        expiry: Date.now() + (4 * 60 * 60 * 1000) // 4 hours
    };
    localStorage.setItem('dm_session', JSON.stringify(session));
}

function dmRestoreSession() {
    try {
        const saved = localStorage.getItem('dm_session');
        if (!saved) return false;
        const session = JSON.parse(saved);
        if (!session || !session.user || !session.expiry) return false;
        if (Date.now() > session.expiry) {
            localStorage.removeItem('dm_session');
            return false;
        }
        doLoginSuccess(session.user, true); // true = silent/restore
        return true;
    } catch(e) {
        localStorage.removeItem('dm_session');
        return false;
    }
}

// =============================================
//  AUTH
// =============================================
async function showLoginScreen() {
    loginScreen.classList.remove('hidden');
    setupWizard.classList.add('hidden');
    appEl.classList.add('hidden');

    const co = DB.ls.getObj('db_company'); // Keeping company info local for now as it's static
    if (co.name) $('login-company-name').textContent = co.name;

    const logoEl = document.querySelector('#login-screen .logo-icon');
    if (logoEl) {
        if (co.logo) {
            logoEl.innerHTML = `<img src="${co.logo}" style="width:100%;height:100%;object-fit:cover;border-radius:12px">`;
        } else {
            logoEl.textContent = co.name ? co.name.charAt(0).toUpperCase() : 'D';
        }
    }
}

async function populateLoginUsers() { /* replaced by userId text input */ }

async function login() {
    const inputId = ($('login-userid') || {value:''}).value.trim();
    const pin = $('login-pin').value.trim();
    if (!inputId) return alert('Enter your User ID');
    if (!pin) return alert('Enter your PIN');
    const users = await DB.getAll('users');
    const user = users.find(u => (u.userId && u.userId.toLowerCase() === inputId.toLowerCase()) || u.name.toLowerCase() === inputId.toLowerCase());

    if (!user || user.pin !== pin) return alert('Invalid User ID or PIN');

    dmSaveSession(user);
    doLoginSuccess(user);
}

async function doLoginSuccess(user, isRestore = false) {
    currentUser = user;
    loginScreen.classList.add('hidden');
    appEl.classList.remove('hidden');
    $('sidebar-username').textContent = user.name;
    const displayRoles = Array.isArray(user.roles) && user.roles.length ? user.roles.join(' | ') : (user.role || '');
    $('sidebar-role').textContent = displayRoles;
    $('sidebar-avatar').textContent = user.name.charAt(0).toUpperCase();

    const co = DB.ls.getObj('db_company');
    $('sidebar-brand').textContent = co.name || 'DistroManager';
    const sidebarLogo = document.querySelector('#sidebar .logo-icon-sm');
    if (sidebarLogo) {
        if (co.logo) {
            sidebarLogo.innerHTML = `<img src="${co.logo}" style="width:100%;height:100%;object-fit:cover;border-radius:6px">`;
            sidebarLogo.style.background = 'transparent';
        } else {
            sidebarLogo.textContent = (co.name || 'D').charAt(0).toUpperCase();
            sidebarLogo.style.background = 'linear-gradient(135deg, var(--primary), var(--secondary))';
        }
    }
    $('current-date').textContent = new Date().toLocaleDateString('en-IN', { weekday: 'short', day: '2-digit', month: 'short', year: 'numeric' });
    buildSidebar();
    showBottomNav();
    await navigateTo('dashboard');
}

function openChangePinModal() {
    openModal('Change PIN', `
        <div class="form-group">
            <label>Current PIN *</label>
            <div style="position:relative">
                <input type="password" id="cp-old-pin" class="form-control" maxlength="6" placeholder="Enter current PIN" inputmode="numeric" style="padding-right:40px">
                <button type="button" onclick="const p=$('cp-old-pin');p.type=p.type==='password'?'text':'password'" style="position:absolute;right:10px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;color:var(--text-muted)">👁</button>
            </div>
        </div>
        <div class="form-group">
            <label>New PIN * <span style="font-size:0.78rem;color:var(--text-muted)">(4 to 6 digits)</span></label>
            <div style="position:relative">
                <input type="password" id="cp-new-pin" class="form-control" maxlength="6" placeholder="Enter new PIN" inputmode="numeric" style="padding-right:40px">
                <button type="button" onclick="const p=$('cp-new-pin');p.type=p.type==='password'?'text':'password'" style="position:absolute;right:10px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;color:var(--text-muted)">👁</button>
            </div>
        </div>
        <div class="form-group">
            <label>Confirm New PIN *</label>
            <div style="position:relative">
                <input type="password" id="cp-confirm-pin" class="form-control" maxlength="6" placeholder="Re-enter new PIN" inputmode="numeric" style="padding-right:40px">
                <button type="button" onclick="const p=$('cp-confirm-pin');p.type=p.type==='password'?'text':'password'" style="position:absolute;right:10px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;color:var(--text-muted)">👁</button>
            </div>
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveChangedPin()">Update PIN</button>
        </div>`);
    setTimeout(() => $('cp-old-pin') && $('cp-old-pin').focus(), 100);
}

async function saveChangedPin() {
    const oldPin = $('cp-old-pin').value.trim();
    const newPin = $('cp-new-pin').value.trim();
    const confirmPin = $('cp-confirm-pin').value.trim();
    if (!oldPin) return alert('Enter your current PIN');
    if (oldPin !== currentUser.pin) return alert('Current PIN is incorrect');
    if (!newPin || !/^\d{4,6}$/.test(newPin)) return alert('New PIN must be 4 to 6 digits (numbers only)');
    if (newPin !== confirmPin) return alert('New PINs do not match');
    if (newPin === oldPin) return alert('New PIN must be different from current PIN');
    try {
        await DB.update('users', currentUser.id, { pin: newPin });
        currentUser.pin = newPin;
        closeModal();
        showToast('PIN updated successfully!', 'success');
    } catch(e) { alert('Error updating PIN: ' + e.message); }
}

function logout() {
    currentUser = null;
    localStorage.removeItem('dm_session');
    
    // Clear login fields
    if ($('login-pin')) $('login-pin').value = '';
    if ($('login-userid')) $('login-userid').value = '';
    
    // Hide app elements
    const bn = $('bottom-nav');
    if (bn) bn.classList.add('hidden');
    const fab = $('app-fab');
    if (fab) fab.classList.add('hidden');
    
    // Close overlays, sidebars and modals
    closeMoreSheet();
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.remove('open');
    closeModal();
    
    showLoginScreen();
}

// --- Sidebar ---
function buildSidebar() {
    const pages = getUserPages(currentUser);
    document.querySelectorAll('.sidebar-nav .nav-item').forEach(el => {
        if (el.id === 'btn-logout') {
            el.style.display = 'flex'; // Always show Logout
            return;
        }
        el.style.display = pages.includes(el.dataset.page) ? 'flex' : 'none';
    });
    const divider = document.querySelector('.nav-divider');
    if (divider) {
        const hasAny = ['packers', 'deliverypersons', 'users', 'setup'].some(p => pages.includes(p));
        divider.style.display = hasAny ? 'block' : 'none';
    }
}

// --- Event Listeners ---
// ── Prevent browser swipe-back / forward navigation ──

// 1. Push a dummy state so there's always a history entry to absorb the back gesture
history.pushState(null, '', window.location.href);
window.addEventListener('popstate', function () {
    history.pushState(null, '', window.location.href);
});

// 2. Block horizontal trackpad/wheel scroll that Chrome uses to trigger back/forward
window.addEventListener('wheel', function (e) {
    // If horizontal delta dominates, prevent it from reaching the browser chrome
    if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        e.preventDefault();
    }
}, { passive: false });

// 3. Block touch swipes starting from the left 20px edge (Chrome's drag-back zone)
window.addEventListener('touchstart', function (e) {
    if (e.touches[0].clientX < 20) {
        e.preventDefault();
    }
}, { passive: false });

// 4. Block mouse back/forward buttons (button 3 = back, button 4 = forward)
window.addEventListener('mousedown', function (e) {
    if (e.button === 3 || e.button === 4) {
        e.preventDefault();
    }
});

document.addEventListener('DOMContentLoaded', async () => {
    const loadingEl = $('app-loading');
    
    // Safety timeout: if app doesn't boot in 10s, try to proceed anyway
    const bootTimeout = setTimeout(() => {
        console.warn('Boot timeout reached. Forcing UI reveal.');
        if (loadingEl) loadingEl.classList.add('hidden');
    }, 10000);

    try {
        // Check for saved customer portal session first
        if (cpRestoreSession()) {
            clearTimeout(bootTimeout);
            if (loadingEl) loadingEl.classList.add('hidden');
            return;
        }

        await DB.refresh(); // Populate cache (core only) immediately

        // Check for main app session
        if (dmRestoreSession()) {
            clearTimeout(bootTimeout);
            if (loadingEl) loadingEl.classList.add('hidden');
        } else {
            await checkFirstLaunch();
        }
        
        $('btn-login').addEventListener('click', login);
        $('login-pin').addEventListener('keypress', e => { if (e.key === 'Enter') login(); });
        
        // Link Logout button
        const logoutBtn = $('btn-logout');
        if (logoutBtn) logoutBtn.addEventListener('click', (e) => { e.preventDefault(); logout(); });
        $('sidebar-close').addEventListener('click', () => sidebar.classList.remove('open'));
        $('sidebar-toggle').addEventListener('click', () => sidebar.classList.toggle('open'));
        $('modal-close').addEventListener('click', closeModal);
        $('modal-overlay').addEventListener('click', e => { if (e.target === $('modal-overlay')) closeModal(); });
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', async e => { e.preventDefault(); await navigateTo(item.dataset.page); sidebar.classList.remove('open'); });
        });
        // Auto-select "0" in number inputs so typing replaces it instead of appending
        document.addEventListener('focus', function(e) {
            if (e.target.tagName === 'INPUT' && e.target.type === 'number' && e.target.value === '0') {
                e.target.select();
            }
        }, true);

        // Success: hide loading
        clearTimeout(bootTimeout);
        if (loadingEl) loadingEl.classList.add('hidden');
        
    } catch (err) {
        clearTimeout(bootTimeout);
        console.error('Boot Error:', err);
        const errorUI = $('boot-error-ui');
        if (errorUI) errorUI.classList.remove('hidden');
        // Keep loading spinner visible but show the error UI
        const spinner = document.querySelector('#app-loading .spinner');
        if (spinner) spinner.style.display = 'none';
        const loadingText = document.querySelector('#app-loading p');
        if (loadingText) loadingText.textContent = 'Boot Interrupted';
    }
});

// --- Modal ---
function openModal(title, html, footer, isFullScreen = false) {
    $('modal-title').textContent = title;
    $('modal-body').innerHTML = html;
    const footerEl = $('modal-footer');
    if (footerEl) {
        if (footer) { footerEl.innerHTML = footer; footerEl.classList.remove('hidden'); }
        else { footerEl.innerHTML = ''; footerEl.classList.add('hidden'); }
    }
    const modalWrap = document.querySelector('.modal');
    if (isFullScreen) modalWrap.classList.add('full-screen-modal');
    else modalWrap.classList.remove('full-screen-modal');
    
    $('modal-overlay').classList.remove('hidden');
    // Prevent background page scroll while modal is open
    document.body.style.overflow = 'hidden';
    // Hide FAB so it doesn't overlap modal buttons
    const fab = $('app-fab');
    if (fab) fab.classList.add('hidden');
    // Scroll modal to top
    const body = $('modal-body');
    if (body) { body.scrollTop = 0; requestAnimationFrame(() => { body.scrollTop = 0; }); }
    // Auto-fix accessibility: link labels to inputs and ensure all inputs have id
    let autoIdx = 0;
    body.querySelectorAll('.form-group, .form-row').forEach(group => {
        group.querySelectorAll('input, select, textarea').forEach(inp => {
            if (!inp.id && !inp.name) inp.id = 'f-auto-' + (++autoIdx);
        });
        const label = group.querySelector('label');
        const inp = group.querySelector('input, select, textarea');
        if (label && inp && inp.id && !label.getAttribute('for')) label.setAttribute('for', inp.id);
    });
}
function closeModal() {
    $('modal-overlay').classList.add('hidden');
    document.body.style.overflow = ''; // Restore page scroll
    document.querySelectorAll('.search-dropdown-list').forEach(d => d.remove());
    const footerEl = $('modal-footer');
    if (footerEl) { footerEl.innerHTML = ''; footerEl.classList.add('hidden'); }
    endSave(); // Always reset save guard when modal closes
    // Restore FAB visibility for current page
    updateFab(currentPage);
}

// --- Custom Searchable Dropdown (replaces broken datalist inside modals) ---
function initSearchDropdown(inputId, items, onSelect) {
    const inp = $(inputId);
    if (!inp) return;

    // BUG-003 fix: clean up old event listeners to prevent ghost duplicate handlers
    if (inp._ddAbortCtrl) { inp._ddAbortCtrl.abort(); }
    const ac = new AbortController();
    const sig = { signal: ac.signal };
    inp._ddAbortCtrl = ac;

    // Wrap input in a relative container if not already wrapped
    let wrapper = inp.closest('.search-dropdown-wrapper');
    if (!wrapper) {
        wrapper = document.createElement('div');
        wrapper.className = 'search-dropdown-wrapper';
        inp.parentNode.insertBefore(wrapper, inp);
        wrapper.appendChild(inp);
    }

    // Create dropdown container
    let dd = document.getElementById(inputId + '-dropdown');
    if (dd) dd.remove();
    dd = document.createElement('div');
    dd.id = inputId + '-dropdown';
    dd.className = 'search-dropdown-list';
    wrapper.appendChild(dd);

    let highlightIdx = -1;

    function renderItems(query) {
        const q = (query || '').toLowerCase();
        const filtered = items.filter(it => {
            if (!q) return true;
            return (it.label || '').toLowerCase().includes(q) ||
                (it.code || '').toLowerCase().includes(q) ||
                (it.searchText || '').toLowerCase().includes(q);
        });
        highlightIdx = -1;
        if (!filtered.length) {
            dd.innerHTML = '<div class="search-dropdown-empty">No items found</div>';
        } else {
            dd.innerHTML = filtered.map((it, idx) =>
                `<div class="search-dropdown-item" data-idx="${idx}" data-value="${it.value}">
                    <span>${it.label}${it.code ? ' <span class="item-code">[' + it.code + ']</span>' : ''}</span>
                    <span class="item-stock">${it.stockText || ''}</span>
                </div>`
            ).join('');
        }
        dd._filtered = filtered;
    }

    function openDD() {
        renderItems(inp.value);
        dd.classList.add('open');
    }

    function closeDD() {
        dd.classList.remove('open');
        highlightIdx = -1;
    }

    function selectItem(item) {
        inp.value = item.label;
        inp.dataset.selectedId = item.id || '';
        inp.dataset.selectedValue = item.value || '';
        closeDD();
        if (onSelect) onSelect(item);
    }

    // BUG-005 fix: scroll input into view when focused (avoids keyboard hiding it)
    inp.addEventListener('focus', () => {
        setTimeout(() => { inp.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }, 100);
        openDD();
    }, sig);
    inp.addEventListener('input', () => { openDD(); }, sig);
    inp.addEventListener('keydown', (e) => {
        const filtered = dd._filtered || [];
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            highlightIdx = Math.min(highlightIdx + 1, filtered.length - 1);
            updateHighlight();
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            highlightIdx = Math.max(highlightIdx - 1, 0);
            updateHighlight();
        } else if (e.key === 'Enter') {
            e.preventDefault();
            if (highlightIdx >= 0 && filtered[highlightIdx]) {
                selectItem(filtered[highlightIdx]);
            } else if (filtered.length === 1) {
                selectItem(filtered[0]);
            }
        } else if (e.key === 'Escape') {
            closeDD();
        }
    }, sig);

    function updateHighlight() {
        dd.querySelectorAll('.search-dropdown-item').forEach((el, i) => {
            el.classList.toggle('highlighted', i === highlightIdx);
            if (i === highlightIdx) el.scrollIntoView({ block: 'nearest' });
        });
    }

    dd.addEventListener('mousedown', (e) => {
        e.preventDefault(); // prevent blur
        const el = e.target.closest('.search-dropdown-item');
        if (el) {
            const idx = parseInt(el.dataset.idx);
            const filtered = dd._filtered || [];
            if (filtered[idx]) selectItem(filtered[idx]);
        }
    });

    inp.addEventListener('blur', () => { setTimeout(closeDD, 200); }, sig);

    return { openDD, closeDD, renderItems };
}

// Helper to build item list for search dropdown
function buildItemSearchList(inventoryItems) {
    // Filter out deactivated items from all lookups
    const activeItems = inventoryItems.filter(i => i.active !== false);
    return activeItems.map(i => {
        const avail = getAvailableStock(i).available;
        return {
            id: i.id,
            label: i.name,
            value: i.name,
            code: i.itemCode || '',
            category: i.category || '',
            subCategory: i.subCategory || '',
            stockText: 'Avail: ' + avail + ' ' + (i.unit || 'Pcs'),
            searchText: (i.name + ' ' + (i.itemCode || '')),
            salePrice: i.salePrice,
            purchasePrice: i.purchasePrice,
            unit: i.unit || 'Pcs',
            secUom: i.secUom || '',
            secUomRatio: i.secUomRatio || 0,
            priceTiers: i.priceTiers || [],
            _raw: i
        };
    });
}


// Helper to ensure we have user coordinates for proximity sorting
async function ensureGeolocation() {
    if (window._userCoords) return window._userCoords;
    if (!navigator.geolocation) return null;
    return new Promise((resolve) => {
        navigator.geolocation.getCurrentPosition(
            (pos) => {
                window._userCoords = { lat: pos.coords.latitude, lng: pos.coords.longitude };
                resolve(window._userCoords);
            },
            () => resolve(null),
            { timeout: 6000, maximumAge: 300000, enableHighAccuracy: false }
        );
    });
}

window.forceHardRefresh = async function() {
    if ('serviceWorker' in navigator) {
        try {
            const regs = await navigator.serviceWorker.getRegistrations();
            for (let r of regs) await r.unregister();
        } catch(e) { console.error('SW unregister failed', e); }
    }
    window.location.reload(true);
};

function buildPartySearchList(parties) {
    // Filter out blocked/deactivated parties from lookups
    let pts = parties.filter(p => p.active !== false && !p.blocked);
    
    // Sort by proximity if coordinates are available
    if (window._userCoords) {
        pts.sort((a, b) => {
            const da = haversine(window._userCoords.lat, window._userCoords.lng, a.lat, a.lng);
            const db = haversine(window._userCoords.lat, window._userCoords.lng, b.lat, b.lng);
            return da - db;
        });
    }

    return pts.map(p => ({
        id: p.id,
        label: p.name,
        value: p.name,
        code: '',
        stockText: p.phone || '',
        searchText: (p.name + ' ' + (p.phone || ''))
    }));
}


// --- Navigation ---
async function navigateTo(page) {
    if (!currentUser) return showLoginScreen();
    // Clear balance filter when navigating away from parties
    if (page !== 'parties') window._partyBalanceFilter = null;
    currentPage = page;
    document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n.dataset.page === page));
    // Sync bottom nav active state (only for tabs that exist in bottom nav)
    document.querySelectorAll('.bn-item[data-page]').forEach(n => n.classList.toggle('active', n.dataset.page === page));
    // BUG-012 fix: always close sidebar on navigate (important for mobile)
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.remove('open');
    // Close more sheet if open
    closeMoreSheet();
    // Update FAB for this page
    updateFab(page);

    const titles = { dashboard: 'Dashboard', parties: 'Parties', partyledger: 'Party Ledger', inventorysetup: 'Inventory Setup', categories: 'Categories Master', uom: 'UOM Master', inventory: 'Inventory', catalog: 'Item Catalog', salesorders: 'Sales Orders', purchaseorders: 'Purchase Orders', invoices: 'Invoices', payments: 'Payments', expenses: 'Expenses', packing: 'Packing', delivery: 'Delivery', reports: 'Reports', packers: 'Packers Master', deliverypersons: 'Delivery Persons', users: 'Users & Roles', setup: 'Company Setup', customerrequests: 'Customer Requests', staffmaster: 'Staff Master', attendance: 'Attendance', hrpayroll: 'HR & Payroll' };
    pageTitle.textContent = titles[page] || page;

    // Show a small loader in the content area
    pageContent.innerHTML = '<div style="display:flex;justify-content:center;align-items:center;height:200px"><div class="loader"></div></div>';

    const renderers = { dashboard: renderDashboard, parties: renderParties, partyledger: renderPartyLedgerLayout, inventorysetup: renderInventorySetup, categories: renderCategories, uom: renderUOM, inventory: renderInventory, catalog: renderCatalog, salesorders: renderSalesOrders, purchaseorders: renderPurchaseOrders, invoices: renderInvoices, payments: renderPayments, expenses: renderExpenses, packing: renderPacking, delivery: renderDelivery, reports: renderReports, packers: renderPackers, deliverypersons: renderDeliveryPersons, users: renderUsers, setup: renderCompanySetup, customerrequests: renderCustomerRequests, staffmaster: renderStaffMaster, attendance: renderAttendance, hrpayroll: renderHRPayroll };

    if (renderers[page]) {
        await renderers[page]();
    }
    // Auto-sync Catalog in background
    if (page === 'catalog') {
        syncCatalogData(true); // silent=true
    }
    // Update user location on specific pages
    if (['catalog', 'salesorders', 'payments', 'parties'].includes(page)) {
        updateUserLocation();
    }
}

// Global User Location
window._userCoords = null;
async function updateUserLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(pos => {
            window._userCoords = { lat: pos.coords.latitude, lng: pos.coords.longitude };
        }, err => console.warn('Geolocation failed', err));
    }
}

// Global Haversine Utility
window.haversine = function(lat1, lon1, lat2, lon2) {
    if (!lat1 || !lon1 || !lat2 || !lon2) return Infinity;
    const R = 6371; // km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
};

// =============================================
//  BOTTOM NAV — More Sheet & FAB
// =============================================
const MORE_ITEMS = [
    { page: 'payments',        icon: '💳', label: 'Payments' },
    { page: 'parties',         icon: '👥', label: 'Parties' },
    { page: 'inventory',       icon: '📦', label: 'Inventory' },
    { page: 'packing',         icon: '📋', label: 'Packing' },
    { page: 'delivery',        icon: '🚚', label: 'Delivery' },
    { page: 'reports',         icon: '📈', label: 'Reports' },
    { page: 'expenses',        icon: '💸', label: 'Expenses' },
    { page: 'purchaseorders',  icon: '🛒', label: 'Purchase' },
    { page: 'catalog',         icon: '🛍️', label: 'Catalog' },
    { page: 'packers',         icon: '🧑‍🏭', label: 'Packers' },
    { page: 'deliverypersons', icon: '🧑‍✈️', label: 'Del.Persons' },
    { page: 'users',           icon: '🔐', label: 'Users' },
    { page: 'setup',           icon: '⚙️', label: 'Setup' },
    { page: 'staffmaster',     icon: '👤', label: 'Staff' },
    { page: 'attendance',      icon: '📅', label: 'Attendance' },
    { page: 'hrpayroll',       icon: '💵', label: 'Payroll' },
    { fn: 'forceHardRefresh()',icon: '🔄', label: 'Hard Refresh' }
];

const BOTTOM_NAV_TABS = {
    Admin:    [{ page:'dashboard', icon:'📊', label:'Home' }, { page:'catalog',      icon:'🛍️', label:'Catalog'  }, { page:'salesorders', icon:'📝', label:'Orders'   }, { page:'invoices', icon:'🧾', label:'Invoices' }, { fn:'openPaymentModal()', icon:'💰', label:'Record' }],
    Manager:  [{ page:'dashboard', icon:'📊', label:'Home' }, { page:'catalog',      icon:'🛍️', label:'Catalog'  }, { page:'salesorders', icon:'📝', label:'Orders'   }, { page:'invoices', icon:'🧾', label:'Invoices' }, { fn:'openPaymentModal()', icon:'💰', label:'Record' }],
    Salesman: [{ page:'dashboard', icon:'📊', label:'Home' }, { page:'catalog',      icon:'🛍️', label:'Catalog'  }, { page:'salesorders', icon:'📝', label:'Orders'   }, { page:'parties',  icon:'👥', label:'Parties'  }, { fn:'openPaymentModal()', icon:'💰', label:'Record' }],
    Packing:  [{ page:'dashboard', icon:'📊', label:'Home' }, { page:'packing',      icon:'📋', label:'Packing'  }],
    Delivery: [{ page:'dashboard', icon:'📊', label:'Home' }, { page:'delivery',     icon:'🚚', label:'Delivery' }],
};

// ── All available quick actions ──
const ALL_QUICK_ACTIONS = [
    { key:'new-sale',       icon:'🧾', label:'New Sale',     fn:"openInvoiceModal('sale')" },
    { key:'payment-in',     icon:'💰', label:'Record Payment', fn:"openPaymentModal()" },
    { key:'catalog',        icon:'🛍️', label:'Catalog',      fn:"navigateTo('catalog')" },
    { key:'salesorders',    icon:'📝', label:'Orders',       fn:"navigateTo('salesorders')" },
    { key:'parties',        icon:'👥', label:'Parties',      fn:"navigateTo('parties')" },
    { key:'inventory',      icon:'📦', label:'Inventory',    fn:"navigateTo('inventory')" },
    { key:'invoices',       icon:'🧾', label:'Invoices',     fn:"navigateTo('invoices')" },
    { key:'payments',       icon:'💳', label:'Payments',     fn:"navigateTo('payments')" },
    { key:'delivery',       icon:'🚚', label:'Delivery',     fn:"navigateTo('delivery')" },
    { key:'expenses',       icon:'💸', label:'Expenses',     fn:"navigateTo('expenses')" },
    { key:'reports',        icon:'📈', label:'Reports',      fn:"navigateTo('reports')" },
    { key:'packing',        icon:'📋', label:'Packing',      fn:"navigateTo('packing')" },
    { key:'purchaseorders', icon:'🛒', label:'Purchase',     fn:"navigateTo('purchaseorders')" },
    { key:'new-party',      icon:'➕', label:'New Party',    fn:"openPartyModal()" },
    { key:'update-party-gps',icon:'📍',label:'Update GPS',   fn:"openPartyGpsModal()" },
];
const DEFAULT_QUICK_ACTIONS = {
    Admin:    ['new-sale','payment-in','catalog','salesorders','parties','inventory','delivery','reports','update-party-gps'],
    Manager:  ['new-sale','payment-in','catalog','salesorders','parties','payments','update-party-gps'],
    Salesman: ['catalog','payment-in','salesorders','parties'],
    Packing:  ['packing','salesorders'],
    Delivery: ['delivery','salesorders'],
};
function getQuickActionKeys(role) {
    const saved = DB.ls.getObj('qa_prefs_' + role);
    return Array.isArray(saved) && saved.length ? saved : (DEFAULT_QUICK_ACTIONS[role] || DEFAULT_QUICK_ACTIONS['Admin']);
}
function saveQuickActionPrefs(role, keys) {
    localStorage.setItem('qa_prefs_' + role, JSON.stringify(keys));
    renderDashboard();
    closeModal();
    showToast('Quick actions saved!', 'success');
}
function openEditQuickActions() {
    const role = currentUser?.role || 'Admin';
    const current = getQuickActionKeys(role);
    const rows = ALL_QUICK_ACTIONS.map(a =>
        `<label style="display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--border);cursor:pointer">
            <input type="checkbox" id="qa-chk-${a.key}" ${current.includes(a.key)?'checked':''} style="width:18px;height:18px;accent-color:var(--primary)">
            <span style="font-size:1.1rem">${a.icon}</span>
            <span style="font-weight:600">${a.label}</span>
        </label>`).join('');
    openModal(`✏️ Edit Quick Actions (${role})`,
        `<p style="font-size:0.85rem;color:var(--text-muted);margin-bottom:12px">Select which shortcuts appear on your dashboard.</p>${rows}`,
        `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
         <button class="btn btn-primary" onclick="
            const keys=[];
            document.querySelectorAll('[id^=qa-chk-]:checked').forEach(el=>keys.push(el.id.replace('qa-chk-','')));
            if(!keys.length){showToast('Select at least one action','error');return;}
            saveQuickActionPrefs('${role}',keys)
         ">Save</button>`);
}
function showBottomNav() {
    const bn = $('bottom-nav');
    if (!bn) return;
    const role = currentUser?.role || 'Admin';
    const tabs = BOTTOM_NAV_TABS[role] || BOTTOM_NAV_TABS['Admin'];
    // Rebuild tabs using DOM API to avoid encoding/parsing issues with innerHTML
    bn.innerHTML = '';
    tabs.forEach(function(t) {
        const a = document.createElement('a');
        a.className = 'bn-item' + (t.page && currentPage === t.page ? ' active' : '');
        if (t.fn) a.setAttribute('data-fn', t.fn);
        else a.setAttribute('data-page', t.page);
        a.href = '#';
        const iconSpan = document.createElement('span');
        iconSpan.className = 'bn-icon';
        iconSpan.textContent = t.icon;
        const labelSpan = document.createElement('span');
        labelSpan.className = 'bn-label';
        labelSpan.textContent = t.label;
        a.appendChild(iconSpan);
        a.appendChild(labelSpan);
        bn.appendChild(a);
    });
    // Add "More" button
    const moreBtn = document.createElement('a');
    moreBtn.className = 'bn-item';
    moreBtn.id = 'bn-more-btn';
    moreBtn.href = '#';
    const moreIcon = document.createElement('span');
    moreIcon.className = 'bn-icon';
    moreIcon.textContent = '\u2630';
    const moreLabel = document.createElement('span');
    moreLabel.className = 'bn-label';
    moreLabel.textContent = 'More';
    moreBtn.appendChild(moreIcon);
    moreBtn.appendChild(moreLabel);
    bn.appendChild(moreBtn);
    bn.classList.remove('hidden');

    // Use event delegation on the container to guarantee click handling
    // This handles the case where individual element events are intercepted
    bn.onclick = function(e) {
        e.preventDefault();
        e.stopPropagation();
        const item = e.target.closest('.bn-item');
        if (!item) return;
        if (item.id === 'bn-more-btn') {
            toggleMoreSheet();
        } else if (item.dataset.fn) {
            // eslint-disable-next-line no-new-func
            (new Function(item.dataset.fn))();
        } else if (item.dataset.page) {
            navigateTo(item.dataset.page);
        }
    };

    buildMoreSheet();
}

function buildMoreSheet() {
    const grid = $('more-sheet-grid');
    if (!grid) return;
    const role = currentUser?.role || 'Admin';
    const allowed = getUserPages(currentUser || { role: 'Admin', roles: [] });
    const mainTabPages = (BOTTOM_NAV_TABS[role] || []).map(t => t.page);
    // Only show items in More sheet that are allowed AND not already a main tab
    const moreItems = MORE_ITEMS.filter(it => {
        if (it.fn) return true;
        return allowed.includes(it.page) && !mainTabPages.includes(it.page);
    });
    grid.innerHTML = moreItems.map(it => `<button class="more-sheet-item" onclick="${it.fn || `navigateTo('${it.page}')`}">
            <span class="more-sheet-icon">${it.icon}</span>
            <span class="more-sheet-label">${it.label}</span>
        </button>`).join('') + `<button class="more-sheet-item" onclick="logout()" style="border-top:1px solid var(--border)">
            <span class="more-sheet-icon">🚪</span>
            <span class="more-sheet-label" style="color:var(--danger)">Logout</span>
        </button>`;
    // Hide "More" btn if nothing to show
    const moreBtn = $('bn-more-btn');
    if (moreBtn) moreBtn.style.display = moreItems.length ? '' : 'none';
}

function toggleMoreSheet() {
    const sheet = $('more-sheet'), overlay = $('more-sheet-overlay');
    if (!sheet) return;
    if (sheet.classList.contains('hidden')) {
        sheet.classList.remove('hidden');
        overlay.classList.remove('hidden');
        // Mark More tab active when sheet is open
        document.querySelectorAll('.bn-item').forEach(n => n.classList.remove('active'));
        $('bn-more-btn').classList.add('active');
    } else {
        closeMoreSheet();
    }
}

function closeMoreSheet() {
    const sheet = $('more-sheet'), overlay = $('more-sheet-overlay');
    if (sheet) sheet.classList.add('hidden');
    if (overlay) overlay.classList.add('hidden');
    // Restore bottom nav active state
    document.querySelectorAll('.bn-item[data-page]').forEach(n => n.classList.toggle('active', n.dataset.page === currentPage));
    if ($('bn-more-btn')) $('bn-more-btn').classList.remove('active');
}

// FAB — shows primary action button per page (mobile only)
const FAB_MAP = {
    salesorders:    () => openSalesOrderModal(),
    invoices:       () => openInvoiceModal('sale'),
    payments:       () => openPaymentModal(),
    parties:        () => openPartyModal(),
    inventory:      () => openItemModal(),
    expenses:       () => openExpenseModal(),
    purchaseorders: () => openPurchaseOrderModal(),
};

function updateFab(page) {
    const fab = $('app-fab');
    if (!fab) return;
    if (window.innerWidth > 768) { fab.classList.add('hidden'); return; }
    if (FAB_MAP[page] && canEdit()) {
        fab.classList.remove('hidden');
    } else {
        fab.classList.add('hidden');
    }
}

function fabAction() {
    const fn = FAB_MAP[currentPage];
    if (fn) fn();
}

// --- Helpers ---
function compressImage(file, { maxWidth = 1024, maxHeight = 1024, quality = 0.75 } = {}) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.readAsDataURL(file);
        reader.onload = e => {
            const img = new Image();
            img.src = e.target.result;
            img.onload = () => {
                const canvas = document.createElement('canvas');
                let width = img.width;
                let height = img.height;

                if (width > height) {
                    if (width > maxWidth) {
                        height *= maxWidth / width;
                        width = maxWidth;
                    }
                } else {
                    if (height > maxHeight) {
                        width *= maxHeight / height;
                        height = maxHeight;
                    }
                }

                canvas.width = width;
                canvas.height = height;
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0, width, height);
                resolve(canvas.toDataURL('image/jpeg', quality));
            };
            img.onerror = reject;
        };
        reader.onerror = reject;
    });
}

function currency(n) { return '₹' + Number(n || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
function fmtDate(d) { return d ? new Date(d).toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }) : '-'; }
function today() { return new Date().toISOString().split('T')[0]; }
function isSalesman() { return currentUser && currentUser.role === 'Salesman'; }
function isPacker()   { return currentUser && currentUser.role === 'Packing'; }
function canEdit()    { return currentUser && (currentUser.role === 'Admin' || currentUser.role === 'Manager'); }

// =============================================
//  DASHBOARD
// =============================================
window.dashFilterFrom = window.dashFilterFrom || '';
window.dashFilterTo = window.dashFilterTo || today();

function applyDashboardFilter() {
    window.dashFilterFrom = $('dash-f-from').value;
    window.dashFilterTo = $('dash-f-to').value;
    renderDashboard();
}

async function renderCustReqWidget() {
    const { data: regs } = await supabaseClient.from('customer_registrations').select('*').order('submitted_at', { ascending: false }).limit(20);
    const pending = (regs||[]).filter(r => r.status === 'pending');
    const recent  = (regs||[]).slice(0, 5);
    return `
    <div class="card" style="margin-top:12px">
        <div class="card-header" style="display:flex;align-items:center;justify-content:space-between">
            <h3 style="margin:0">🧑‍💼 Customer Portal Requests</h3>
            <div style="display:flex;align-items:center;gap:10px">
                ${pending.length ? `<span class="badge badge-danger" style="font-size:0.82rem">${pending.length} Pending</span>` : '<span style="font-size:0.78rem;color:var(--text-muted)">No pending</span>'}
                <button class="btn btn-outline btn-sm" onclick="navigateTo('customerrequests')">View All</button>
            </div>
        </div>
        <div class="card-body" style="padding:6px 10px">
        ${!regs || !regs.length ? '<p style="color:var(--text-muted);font-size:0.85rem;padding:8px">No registration requests yet.</p>' : `
        <div class="table-wrapper"><table class="data-table">
            <thead><tr><th>Business</th><th>Phone</th><th>City</th><th>Date</th><th>Status</th><th></th></tr></thead>
            <tbody>
            ${recent.map(r => `<tr>
                <td style="font-weight:600">${r.business_name||''}</td>
                <td>${r.phone||''}</td>
                <td style="color:var(--text-muted)">${r.city||'-'}</td>
                <td style="font-size:0.8rem;color:var(--text-muted)">${new Date(r.submitted_at||Date.now()).toLocaleDateString('en-IN')}</td>
                <td><span class="badge ${r.status==='pending'?'badge-warning':r.status==='approved'?'badge-success':'badge-danger'}">${r.status}</span></td>
                <td>${r.status==='pending' ? `<button class="btn btn-primary btn-sm" onclick="navigateTo('customerrequests')">Review</button>` : ''}</td>
            </tr>`).join('')}
            </tbody>
        </table></div>`}
        </div>
    </div>`;
}

function renderPartyNavWidget(parties, limit = 6) {
    const located = parties.filter(p => p.lat && p.lng);
    if (!located.length) return '';
    return `
        <div class="card" style="margin-top:12px">
            <div class="card-header" style="display:flex;align-items:center;justify-content:space-between">
                <h3 style="margin:0">🗺️ Navigate to Party</h3>
                <span style="font-size:0.78rem;color:var(--text-muted)">${located.length} with location</span>
            </div>
            <div class="card-body" style="padding:6px 10px">
                <input placeholder="Search party..." oninput="filterNavWidget(this.value)" style="width:100%;margin-bottom:8px;padding:6px 10px;border:1px solid var(--border);border-radius:6px;font-size:0.85rem;background:var(--surface)">
                <div id="nav-widget-list">
                    ${located.slice(0, limit).map(p => `
                    <div class="nav-party-row" data-name="${escapeHtml(p.name.toLowerCase())}">
                        <div style="flex:1;min-width:0">
                            <div style="font-weight:600;font-size:0.88rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(p.name)}</div>
                            <div style="font-size:0.75rem;color:var(--text-muted)">${escapeHtml(p.city || p.address || '')}</div>
                        </div>
                        <a href="https://www.google.com/maps?q=${p.lat},${p.lng}" target="_blank" class="btn btn-outline btn-sm" style="flex-shrink:0;padding:4px 10px;font-size:0.8rem;border-color:#3b82f6;color:#3b82f6">🗺️ Go</a>
                    </div>`).join('')}
                    ${located.length > limit ? `<div style="text-align:center;padding:6px;font-size:0.8rem;color:var(--text-muted)">+ ${located.length - limit} more — go to <span style="color:var(--accent);cursor:pointer" onclick="navigateTo('parties')">Parties</span></div>` : ''}
                </div>
            </div>
        </div>`;
}
function filterNavWidget(q) {
    const rows = document.querySelectorAll('#nav-widget-list .nav-party-row');
    const s = q.toLowerCase();
    rows.forEach(r => r.style.display = (!s || r.dataset.name.includes(s)) ? '' : 'none');
}
async function renderDashboard() {
    const role = currentUser.role;
    // Batch fetch data from Supabase
    const [invoices, payments, expenses, inventory, salesOrders, dels, parties] = await Promise.all([
        DB.getAll('invoices'),
        DB.getAll('payments'),
        DB.getAll('expenses'),
        DB.getAll('inventory'),
        DB.getAll('sales_orders'),
        DB.getAll('delivery'),
        DB.getAll('parties')
    ]);

    // ── SALESMAN DASHBOARD ──
    if (role === 'Salesman') {
        const mySO = salesOrders.filter(o => o.createdBy === currentUser.name);
        pageContent.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card amber"><div class="stat-icon">⏳</div><div class="stat-value">${mySO.filter(o => o.status === 'pending').length}</div><div class="stat-label">My Pending</div></div>
                <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${mySO.filter(o => o.status === 'approved').length}</div><div class="stat-label">My Approved</div></div>
                <div class="stat-card red"><div class="stat-icon">❌</div><div class="stat-value">${mySO.filter(o => o.status === 'rejected').length}</div><div class="stat-label">My Rejected</div></div>
                <div class="stat-card blue"><div class="stat-icon">📦</div><div class="stat-value">${mySO.length}</div><div class="stat-label">Total Orders</div></div>
            </div>
            <div class="section-toolbar" style="margin-top:8px"><h3>Quick Actions</h3></div>
            <div class="quick-actions">
                <button class="quick-action-btn" onclick="navigateTo('salesorders')"><span class="qa-icon">📝</span><span class="qa-label">New Order</span></button>
                <button class="quick-action-btn" onclick="navigateTo('parties')"><span class="qa-icon">👥</span><span class="qa-label">Parties</span></button>
                <button class="quick-action-btn" onclick="navigateTo('inventory')"><span class="qa-icon">📦</span><span class="qa-label">Inventory</span></button>
                <button class="quick-action-btn" onclick="openPartyGpsModal()"><span class="qa-icon">📍</span><span class="qa-label">Update GPS</span></button>
            </div>
            <div class="card"><div class="card-header"><h3>My Recent Orders</h3></div><div class="card-body">
                <div class="table-wrapper">
                    <table class="data-table"><thead><tr><th>Date</th><th>Order #</th><th>Party</th><th>Status</th><th>Total</th></tr></thead>
                    <tbody>${mySO.slice(-5).reverse().map(o => {
                        const stMap = { pending: 'badge-warning', approved: 'badge-success', rejected: 'badge-danger' };
                        const stText = o.status || 'pending';
                        return `<tr><td>${fmtDate(o.date)}</td><td>${o.orderNo}</td><td>${o.partyName}</td><td><span class="badge ${stMap[stText]||'badge-warning'}" style="text-transform:capitalize">${stText}</span></td><td class="amount-green">${currency(o.total)}</td></tr>`;
                    }).join('') || '<tr><td colspan="5"><div class="empty-state"><span class="empty-icon">📝</span><p>No orders yet</p><p class="empty-subtitle">Create your first sales order to get started</p></div></td></tr>'}</tbody></table>
                </div>
            </div></div>
            ${renderPartyNavWidget(parties)}`; return;
    }

    // ── DELIVERY DASHBOARD ──
    if (role === 'Delivery') {
        const myDels = dels.filter(d => d.deliveryPerson === currentUser.name);
        const dispatched = myDels.filter(d => d.status === 'Dispatched');
        const delivered = myDels.filter(d => d.status === 'Delivered');
        const undelivered = myDels.filter(d => d.status === 'Undelivered');
        pageContent.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card blue"><div class="stat-icon">🚚</div><div class="stat-value">${dispatched.length}</div><div class="stat-label">In Transit</div></div>
                <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${delivered.length}</div><div class="stat-label">Delivered</div></div>
                <div class="stat-card red"><div class="stat-icon">↩️</div><div class="stat-value">${undelivered.length}</div><div class="stat-label">Undelivered</div></div>
                <div class="stat-card amber"><div class="stat-icon">📦</div><div class="stat-value">${myDels.length}</div><div class="stat-label">Total Assigned</div></div>
            </div>
            <div class="section-toolbar" style="margin-top:8px"><h3>Quick Actions</h3></div>
            <div class="quick-actions">
                <button class="quick-action-btn" onclick="navigateTo('delivery')"><span class="qa-icon">🚚</span><span class="qa-label">My Deliveries</span></button>
                <button class="quick-action-btn" onclick="navigateTo('deliverypersons')"><span class="qa-icon">🧑‍✈️</span><span class="qa-label">Del. Persons</span></button>
                <button class="quick-action-btn" onclick="openPartyGpsModal()"><span class="qa-icon">📍</span><span class="qa-label">Update GPS</span></button>
            </div>
            <div class="card"><div class="card-header"><h3>My Active Dispatches</h3></div><div class="card-body">
                <div class="table-wrapper">
                    <table class="data-table"><thead><tr><th>Order #</th><th>Party</th><th>Invoice</th><th>Status</th></tr></thead>
                    <tbody>${dispatched.slice(-5).reverse().map(d => `<tr><td style="font-weight:600">${d.orderNo}</td><td>${d.partyName}</td><td><span class="badge badge-info">${d.invoiceNo || '-'}</span></td><td><span class="badge badge-info">${d.status}</span></td></tr>`).join('') || '<tr><td colspan="4"><div class="empty-state"><span class="empty-icon">🚚</span><p>No active dispatches</p><p class="empty-subtitle">All deliveries are complete</p></div></td></tr>'}</tbody></table>
                </div>
            </div></div>
            ${renderPartyNavWidget(parties)}`; return;
    }

    // ── PACKING DASHBOARD ──
    if (role === 'Packing') {
        const allApproved = salesOrders.filter(o => o.status === 'approved' && !o.packed && !o.cannotComplete);
        // Packing queue = unassigned OR assigned to me
        const myQueue = allApproved.filter(o => !o.assignedPacker || o.assignedPacker === currentUser.name);
        // Unassigned only for the table (available to self-assign)
        const unassigned = allApproved.filter(o => !o.assignedPacker);
        const myAssigned = allApproved.filter(o => o.assignedPacker === currentUser.name);
        const packed = salesOrders.filter(o => o.packed && o.packedBy === currentUser.name);
        pageContent.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card amber"><div class="stat-icon">📋</div><div class="stat-value">${myQueue.length}</div><div class="stat-label">Packing Queue</div></div>
                <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${packed.length}</div><div class="stat-label">Packed by Me</div></div>
            </div>
            <div class="section-toolbar" style="margin-top:8px"><h3>Quick Actions</h3></div>
            <div class="quick-actions">
                <button class="quick-action-btn" onclick="navigateTo('packing')"><span class="qa-icon">📋</span><span class="qa-label">Packing Queue</span></button>
            </div>
            ${myAssigned.length ? `<div class="card" style="margin-bottom:12px"><div class="card-header"><h3>Assigned to Me</h3></div><div class="card-body">
                <div class="table-wrapper">
                    <table class="data-table"><thead><tr><th>Order #</th><th>Party</th><th>Items</th><th>Total</th></tr></thead>
                    <tbody>${myAssigned.slice(0, 5).map(o => `<tr><td style="font-weight:600">${o.orderNo}</td><td>${o.partyName}</td><td>${o.items.length}</td><td class="amount-green">${currency(o.total)}</td></tr>`).join('')}</tbody></table>
                </div>
            </div></div>` : ''}
            ${unassigned.length ? `<div class="card"><div class="card-header"><h3>Unassigned Orders</h3></div><div class="card-body">
                <div class="table-wrapper">
                    <table class="data-table"><thead><tr><th>Order #</th><th>Party</th><th>Items</th><th>Total</th></tr></thead>
                    <tbody>${unassigned.slice(0, 5).map(o => `<tr><td style="font-weight:600">${o.orderNo}</td><td>${o.partyName}</td><td>${o.items.length}</td><td class="amount-green">${currency(o.total)}</td></tr>`).join('')}</tbody></table>
                </div>
            </div></div>` : (!myAssigned.length ? '<div class="card"><div class="card-body"><div class="empty-state"><span class="empty-icon">✅</span><p>All caught up!</p><p class="empty-subtitle">No orders waiting to be packed.</p></div></div></div>' : '')}
            ${renderPartyNavWidget(parties)}`; return;
    }

    // ── ADMIN / MANAGER DASHBOARD ──
    const pendingSO   = salesOrders.filter(o => o.status === 'pending').length;
    const hasCancelledInvoice = (o) => o.invoiceCancelled || invoices.some(i => i.fromOrder === o.orderNo && i.status === 'cancelled');
    const approvedUnpacked = salesOrders.filter(o => o.status === 'approved' && !o.packed && !hasCancelledInvoice(o) && !o.cannotComplete).length;
    const undeliveredCount = dels.filter(d => d.status === 'Undelivered' || d.status === 'Returned').length;
    const lowStock = inventory.filter(i => i.stock <= (i.lowStockAlert || 5)).length;
    updateNavBadges(inventory);
    const pendingCheques = payments.filter(p => p.mode === 'Cheque' && (!p.chequeStatus || p.chequeStatus === 'Pending')).length;

    // Receivable / Payable from party ledger balances
    // balance < 0 (Cr) = customer owes us = Receivable
    // balance > 0 (Dr) = we owe them (advance/overpaid) = Payable
    const recParties = parties.filter(p => (p.balance || 0) < 0);
    const payParties = parties.filter(p => (p.balance || 0) > 0);
    const totalReceivable = recParties.reduce((s, p) => s + Math.abs(p.balance), 0);
    const totalPayable    = payParties.reduce((s, p) => s + p.balance, 0);
    const drParties = recParties;
    const crParties = payParties;

    // Store for chart re-render
    window._dashInvoicesAll = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
    window._dashPeriod = window._dashPeriod || 'month';

    // Quick KPI (this month)
    const thisMonthStart = today().substring(0, 8) + '01';
    const tmInvs = window._dashInvoicesAll.filter(i => i.date >= thisMonthStart);
    const tmSales = tmInvs.reduce((s, i) => s + i.total, 0);
    const tmPayIn = payments.filter(p => p.type === 'in' && p.date >= thisMonthStart).reduce((s, p) => s + p.amount, 0);
    const tmExp   = expenses.filter(e => e.date >= thisMonthStart).reduce((s, e) => s + e.amount, 0);

    // Slow moving (for bottom section)
    const ninetyDaysAgo = new Date(); ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
    const ninetyDaysStr = ninetyDaysAgo.toISOString().split('T')[0];
    const itemSalesMap = {}; const itemLastSoldMap = {};
    invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled').forEach(inv => {
        inv.items.forEach(li => {
            if (inv.date >= ninetyDaysStr) itemSalesMap[li.itemId] = (itemSalesMap[li.itemId] || 0) + (li.packedQty !== undefined ? li.packedQty : li.qty);
            if (!itemLastSoldMap[li.itemId] || inv.date > itemLastSoldMap[li.itemId]) itemLastSoldMap[li.itemId] = inv.date;
        });
    });
    const nonMovingItems = inventory.filter(i => !itemSalesMap[i.id] && i.stock > 0);
    const slowMovingItems = inventory.filter(i => itemSalesMap[i.id] && itemSalesMap[i.id] <= 5 && i.stock > 0);

    const tileHover = 'onmouseover="this.style.transform=\'translateY(-2px)\';this.style.boxShadow=\'0 6px 20px rgba(0,0,0,0.2)\'" onmouseout="this.style.transform=\'none\';this.style.boxShadow=\'none\'"';

    pageContent.innerHTML = `
    <!-- Alert strips -->
    ${pendingSO ? `<div class="dash-alert dash-alert-amber" onclick="navigateTo('salesorders')" style="cursor:pointer">📝 <strong>${pendingSO} Pending Orders</strong> awaiting approval &nbsp;<span style="color:var(--accent)">→ Review</span></div>` : ''}
    ${approvedUnpacked ? `<div class="dash-alert dash-alert-blue" onclick="navigateTo('packing')" style="cursor:pointer">📋 <strong>${approvedUnpacked} Orders</strong> ready for packing &nbsp;<span style="color:var(--accent)">→ Pack Now</span></div>` : ''}
    ${undeliveredCount ? `<div class="dash-alert dash-alert-red" onclick="navigateTo('delivery')" style="cursor:pointer">↩️ <strong>${undeliveredCount} Undelivered / Returned</strong> need attention &nbsp;<span style="color:var(--accent)">→ Handle</span></div>` : ''}

    <!-- Receivable / Payable -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
        <div class="dash-kpi-card dash-kpi-green" onclick="window._partyBalanceFilter='receivable';navigateTo('parties')" style="cursor:pointer">
            <div class="dash-kpi-label">Receivable</div>
            <div class="dash-kpi-amount dash-count" data-val="${totalReceivable}">${currency(totalReceivable)}</div>
            <div class="dash-kpi-badge dash-kpi-badge-green">${drParties.length} parties</div>
        </div>
        <div class="dash-kpi-card dash-kpi-red" onclick="window._partyBalanceFilter='payable';navigateTo('parties')" style="cursor:pointer">
            <div class="dash-kpi-label">Payable</div>
            <div class="dash-kpi-amount dash-count" data-val="${totalPayable}">${currency(totalPayable)}</div>
            <div class="dash-kpi-badge dash-kpi-badge-red">${crParties.length} parties</div>
        </div>
    </div>

    <!-- Sales Chart -->
    <div class="dash-fin-card" style="margin-bottom:14px">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px;flex-wrap:wrap;gap:8px">
            <div>
                <div style="font-size:0.82rem;color:var(--text-muted)">Total Sale</div>
                <div style="font-size:1.5rem;font-weight:700;color:var(--text-primary)" id="dash-chart-total">${currency(tmSales)}</div>
                <div id="dash-chart-compare" style="font-size:0.8rem;margin-top:2px"></div>
            </div>
            <div style="position:relative">
                <button id="dash-period-btn" onclick="toggleDashPeriodMenu()" class="btn btn-outline btn-sm" style="min-width:120px;display:flex;justify-content:space-between;align-items:center;gap:8px">
                    <span id="dash-period-label">This Month</span> <span>▾</span>
                </button>
                <div id="dash-period-menu" style="display:none;position:absolute;right:0;top:36px;background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius-md);box-shadow:0 8px 24px rgba(0,0,0,0.15);z-index:100;min-width:140px;overflow:hidden">
                    ${[['week','This Week'],['lastmonth','Last Month'],['month','This Month'],['quarter','This Quarter'],['halfyear','Half Year'],['year','This Year']].map(([v,l])=>`<div class="dash-period-opt" data-val="${v}" onclick="selectDashPeriod('${v}','${l}')" style="padding:9px 16px;cursor:pointer;font-size:0.88rem;color:var(--text-primary)">${l}</div>`).join('')}
                </div>
            </div>
        </div>
        <div id="dash-chart-wrap" style="margin-top:10px;overflow:hidden"></div>
    </div>

    <!-- This Month Quick Stats -->
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px">
        <div class="dash-pulse-tile" onclick="navigateTo('payments')" style="--tile-color:#10b981;animation-delay:0.05s">
            <div class="dash-pulse-icon">💰</div>
            <div class="dash-pulse-val dash-count" data-val="${tmPayIn}" style="color:#10b981">${currency(tmPayIn)}</div>
            <div class="dash-pulse-lbl">Collected</div>
        </div>
        <div class="dash-pulse-tile" onclick="navigateTo('expenses')" style="--tile-color:#ef4444;animation-delay:0.1s">
            <div class="dash-pulse-icon">💸</div>
            <div class="dash-pulse-val dash-count" data-val="${tmExp}" style="color:#ef4444">${currency(tmExp)}</div>
            <div class="dash-pulse-lbl">Expenses</div>
        </div>
        <div class="dash-pulse-tile${lowStock ? ' dash-pulse-alert' : ''}" onclick="navigateTo('inventory')" style="--tile-color:${lowStock?'#ef4444':'var(--text-primary)'};animation-delay:0.15s">
            <div class="dash-pulse-icon">📦</div>
            <div class="dash-pulse-val" style="color:${lowStock?'#ef4444':'var(--text-primary)'}">${lowStock}</div>
            <div class="dash-pulse-lbl">Low Stock</div>
        </div>
        <div class="dash-pulse-tile${pendingCheques ? ' dash-pulse-alert' : ''}" onclick="navigateTo('reports');setTimeout(()=>showReport('chequeregister'),200)" style="--tile-color:${pendingCheques?'#f59e0b':'var(--text-primary)'};animation-delay:0.2s">
            <div class="dash-pulse-icon">🏦</div>
            <div class="dash-pulse-val" style="color:${pendingCheques?'#f59e0b':'var(--text-primary)'}">${pendingCheques}</div>
            <div class="dash-pulse-lbl">Cheques</div>
        </div>
    </div>

    <!-- Most Used Reports -->
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <span style="font-weight:600;font-size:0.95rem">Most Used Reports</span>
        <a href="#" onclick="navigateTo('reports');return false" style="font-size:0.83rem;color:var(--accent);text-decoration:none">View All →</a>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:18px">
        ${[
            ['sales','Sale Report'],
            ['payments','All Transactions'],
            ['invoice-pnl','Daybook Report'],
            ['outstanding','Party Statement']
        ].map(([r,l])=>`<div class="dash-report-chip" onclick="navigateTo('reports');setTimeout(()=>showReport('${r}'),200)" style="cursor:pointer" ${tileHover}><span>${l}</span><span style="color:var(--accent)">›</span></div>`).join('')}
    </div>

    <!-- Quick Actions -->
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <span style="font-weight:700;font-size:0.9rem;color:var(--text-secondary);letter-spacing:0.04em;text-transform:uppercase">Quick Actions</span>
        <button class="btn-icon" onclick="openEditQuickActions()" title="Edit Quick Actions" style="font-size:1rem;padding:4px 8px">✏️</button>
    </div>
    <div class="quick-actions" style="margin-bottom:18px">
        ${getQuickActionKeys(currentUser?.role||'Admin').map(key=>{
            const a = ALL_QUICK_ACTIONS.find(x=>x.key===key);
            return a ? `<button class="quick-action-btn" onclick="${a.fn}"><span class="qa-icon">${a.icon}</span><span class="qa-label">${a.label}</span></button>` : '';
        }).join('')}
    </div>

    <!-- Slow / Non-Moving Items -->
    ${(nonMovingItems.length || slowMovingItems.length) ? `
    <div class="card" style="margin-bottom:14px"><div class="card-header" style="display:flex;justify-content:space-between;align-items:center"><h3>📉 Slow / Non-Moving Items</h3><span style="font-size:0.78rem;color:var(--text-muted)">Last 90 days</span></div><div class="card-body">
        ${nonMovingItems.length ? `<div style="margin-bottom:14px"><div style="font-weight:600;color:var(--danger);font-size:0.88rem;margin-bottom:8px">🚫 Non-Moving (${nonMovingItems.length}) — Zero sales in 90 days</div>
        <table class="data-table"><thead><tr><th>Item</th><th>Stock</th><th>Last Sold</th></tr></thead><tbody>${nonMovingItems.slice(0,8).map(i=>`<tr><td style="font-weight:600">${i.name}</td><td><span class="badge badge-danger">${i.stock}</span></td><td style="color:var(--text-muted);font-size:0.82rem">${itemLastSoldMap[i.id]?fmtDate(itemLastSoldMap[i.id]):'Never'}</td></tr>`).join('')}</tbody></table></div>` : ''}
        ${slowMovingItems.length ? `<div><div style="font-weight:600;color:var(--warning);font-size:0.88rem;margin-bottom:8px">🐢 Slow Moving (${slowMovingItems.length}) — ≤5 units in 90 days</div>
        <table class="data-table"><thead><tr><th>Item</th><th>Stock</th><th>Sold (90d)</th></tr></thead><tbody>${slowMovingItems.slice(0,8).map(i=>`<tr><td style="font-weight:600">${i.name}</td><td><span class="badge badge-info">${i.stock}</span></td><td style="font-weight:600;color:var(--warning)">${itemSalesMap[i.id]||0}</td></tr>`).join('')}</tbody></table></div>` : ''}
    </div></div>` : ''}

    <!-- Recent Invoices -->
    <div class="card"><div class="card-header"><h3>Recent Invoices</h3></div><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table"><thead><tr><th>Date</th><th>Invoice #</th><th>Party</th><th>Type</th><th>Amount</th></tr></thead>
            <tbody>${invoices.slice(-5).reverse().map(i=>`<tr><td>${fmtDate(i.date)}</td><td style="font-weight:600">${i.invoiceNo}</td><td>${i.partyName}</td><td><span class="badge ${i.type==='sale'?'badge-success':'badge-info'}">${i.type}</span></td><td class="${i.type==='sale'?'amount-green':'amount-red'}">${currency(i.total)}</td></tr>`).join('')||'<tr><td colspan="5"><div class="empty-state"><p>No invoices yet</p></div></td></tr>'}</tbody></table>
        </div>
    </div></div>
    ${await renderCustReqWidget()}
    `;

    // Render chart after DOM is ready
    renderDashChart();
    // Animate counters
    requestAnimationFrame(() => {
        document.querySelectorAll('.dash-count[data-val]').forEach(el => {
            const target = parseFloat(el.dataset.val) || 0;
            if (target === 0) return;
            const isAmount = el.classList.contains('dash-kpi-amount') || el.classList.contains('dash-pulse-val');
            const dur = 700, start = Date.now();
            const tick = () => {
                const p = Math.min((Date.now() - start) / dur, 1);
                const eased = 1 - Math.pow(1 - p, 3);
                const cur = target * eased;
                el.textContent = isAmount ? currency(cur) : Math.round(cur);
                if (p < 1) requestAnimationFrame(tick);
            };
            tick();
        });
    });
}

// ── ADMIN DASHBOARD CHART HELPERS ──
function getDashPeriodDates(period) {
    const now = new Date();
    const pad = n => String(n).padStart(2,'0');
    const fmt = d => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
    const todayStr = fmt(now);
    let from, to = todayStr;
    if (period === 'week') {
        const d = new Date(now); d.setDate(d.getDate() - 6); from = fmt(d);
    } else if (period === 'lastmonth') {
        from = fmt(new Date(now.getFullYear(), now.getMonth()-1, 1));
        to   = fmt(new Date(now.getFullYear(), now.getMonth(), 0));
    } else if (period === 'quarter') {
        const d = new Date(now); d.setMonth(d.getMonth()-2); d.setDate(1); from = fmt(d);
    } else if (period === 'halfyear') {
        const d = new Date(now); d.setMonth(d.getMonth()-5); d.setDate(1); from = fmt(d);
    } else if (period === 'year') {
        from = `${now.getFullYear()}-01-01`;
    } else { // month
        from = `${now.getFullYear()}-${pad(now.getMonth()+1)}-01`;
    }
    return { from, to };
}

function buildDashChartData(invoices, from, to) {
    // Build day-by-day totals between from and to
    const map = {};
    invoices.filter(i => i.date >= from && i.date <= to).forEach(i => { map[i.date] = (map[i.date]||0) + i.total; });
    const pts = [];
    const cur = new Date(from + 'T00:00:00');
    const end = new Date(to   + 'T00:00:00');
    while (cur <= end) {
        const d = `${cur.getFullYear()}-${String(cur.getMonth()+1).padStart(2,'0')}-${String(cur.getDate()).padStart(2,'0')}`;
        pts.push({ d, v: map[d]||0, label: `${cur.getDate()} ${['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][cur.getMonth()]}` });
        cur.setDate(cur.getDate()+1);
    }
    return pts;
}

function makeSalesSvg(pts) {
    if (!pts.length) return '<div style="text-align:center;padding:40px;color:var(--text-muted);font-size:0.9rem">No sales data for this period</div>';
    const W=700, H=200, pl=52, pr=10, pt=14, pb=36;
    const w=W-pl-pr, h=H-pt-pb;
    const maxV = Math.max(...pts.map(p=>p.v), 1);
    // nice ceiling
    const mag = Math.pow(10, Math.floor(Math.log10(maxV)));
    const niceMax = Math.ceil(maxV/mag)*mag;
    const xStep = pts.length > 1 ? w/(pts.length-1) : w;
    const xy = pts.map((p,i) => [pl+i*xStep, pt+h - (p.v/niceMax)*h]);
    const fmtY = v => v>=1e5?(v/1e5).toFixed(0)+'L':v>=1e3?(v/1e3).toFixed(0)+'k':v;
    // gridlines
    const grids = [0.25,0.5,0.75,1].map(f=>{
        const y=pt+h-f*h;
        return `<line x1="${pl}" y1="${y}" x2="${W-pr}" y2="${y}" stroke="currentColor" opacity="0.1"/>
                <text x="${pl-5}" y="${y+4}" text-anchor="end" font-size="11" fill="currentColor" opacity="0.45">${fmtY(niceMax*f)}</text>`;
    }).join('');
    // x labels — show ~6 evenly
    const step = Math.max(1,Math.ceil(pts.length/6));
    const xlbls = pts.filter((_,i)=>i%step===0||i===pts.length-1).map(p=>{
        const i=pts.indexOf(p);
        return `<text x="${pl+i*xStep}" y="${H-8}" text-anchor="middle" font-size="11" fill="currentColor" opacity="0.45">${p.label}</text>`;
    }).join('');
    // smooth path using cubic bezier
    let d='', ad='';
    xy.forEach(([x,y],i)=>{
        if(i===0){d+=`M${x},${y}`;ad+=`M${x},${y}`;}
        else{
            const [px,py]=xy[i-1];
            const cpx=(px+x)/2;
            d+=` C${cpx},${py} ${cpx},${y} ${x},${y}`;
            ad+=` C${cpx},${py} ${cpx},${y} ${x},${y}`;
        }
    });
    ad+=` L${xy[xy.length-1][0]},${pt+h} L${pl},${pt+h} Z`;
    return `<svg viewBox="0 0 ${W} ${H}" width="100%" preserveAspectRatio="none" style="display:block;max-height:200px">
        <defs><linearGradient id="sg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#3b82f6" stop-opacity="0.25"/><stop offset="100%" stop-color="#3b82f6" stop-opacity="0.01"/></linearGradient></defs>
        ${grids}
        <path d="${ad}" fill="url(#sg)"/>
        <path d="${d}" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
        ${xlbls}
    </svg>`;
}

function renderDashChart() {
    const wrap  = $('dash-chart-wrap'); if (!wrap) return;
    const period = window._dashPeriod || 'month';
    const { from, to } = getDashPeriodDates(period);
    const allInv = window._dashInvoicesAll || [];
    const pts = buildDashChartData(allInv, from, to);
    const total = pts.reduce((s,p)=>s+p.v,0);

    // Comparison vs previous equal-length period
    const fromD = new Date(from+'T00:00:00'), toD = new Date(to+'T00:00:00');
    const days  = Math.round((toD-fromD)/(864e5))+1;
    const prevTo   = new Date(fromD); prevTo.setDate(prevTo.getDate()-1);
    const prevFrom = new Date(prevTo); prevFrom.setDate(prevFrom.getDate()-days+1);
    const pf = prevFrom.toISOString().split('T')[0], pt2 = prevTo.toISOString().split('T')[0];
    const prevTotal = allInv.filter(i=>i.date>=pf&&i.date<=pt2).reduce((s,i)=>s+i.total,0);
    let compareHtml = '';
    if (prevTotal > 0) {
        const pct = Math.abs(Math.round((total-prevTotal)/prevTotal*100));
        const up = total >= prevTotal;
        compareHtml = `<span style="color:${up?'#10b981':'#ef4444'};font-size:0.82rem;font-weight:600">${up?'▲':'▼'} ${pct}% ${up?'more':'less'} than previous period</span>`;
    }
    const totalEl   = $('dash-chart-total');
    const compareEl = $('dash-chart-compare');
    if (totalEl)   totalEl.textContent   = currency(total);
    if (compareEl) compareEl.innerHTML   = compareHtml;
    wrap.innerHTML = makeSalesSvg(pts);
}

function toggleDashPeriodMenu() {
    const m = $('dash-period-menu');
    if (m) m.style.display = m.style.display === 'none' ? 'block' : 'none';
    // Close on outside click
    setTimeout(() => {
        const close = (e) => { if (m && !m.contains(e.target) && e.target.id !== 'dash-period-btn') { m.style.display='none'; document.removeEventListener('click',close); } };
        document.addEventListener('click', close);
    }, 10);
}

function selectDashPeriod(val, label) {
    window._dashPeriod = val;
    const btn = $('dash-period-btn'); if (btn) btn.querySelector('#dash-period-label').textContent = label;
    const m = $('dash-period-menu'); if (m) { m.style.display='none'; m.querySelectorAll('.dash-period-opt').forEach(o=>{ o.style.background = o.dataset.val===val?'var(--primary-light, rgba(59,130,246,0.1))':''; o.style.fontWeight = o.dataset.val===val?'600':''; }); }
    renderDashChart();
}

function openDashboardSettings() {
    const prefs = currentUser.dashboardPrefs || {};
    openModal('⚙️ Customize Dashboard', `
        <p style="margin-bottom:15px;color:var(--text-secondary);font-size:0.9rem">Toggle sections on or off to personalize your dashboard layout.</p>
        <div style="background:var(--bg-card);padding:20px;border-radius:var(--radius-md);border:1px solid var(--border)">
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;margin-bottom:14px;font-size:0.95rem">
                <input type="checkbox" id="pref-filters" style="width:18px;height:18px" ${!prefs.hideFilters ? 'checked' : ''}> Show Date Filters
            </label>
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;margin-bottom:14px;font-size:0.95rem">
                <input type="checkbox" id="pref-top" style="width:18px;height:18px" ${!prefs.hideTopKPIs ? 'checked' : ''}> Show Top Financial KPIs
            </label>
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;margin-bottom:14px;font-size:0.95rem">
                <input type="checkbox" id="pref-sec" style="width:18px;height:18px" ${!prefs.hideSecondaryKPIs ? 'checked' : ''}> Show Secondary KPIs & Stock
            </label>
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;margin-bottom:14px;font-size:0.95rem">
                <input type="checkbox" id="pref-actions" style="width:18px;height:18px" ${!prefs.hideQuickActions ? 'checked' : ''}> Show Quick Actions
            </label>
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;margin-bottom:14px;font-size:0.95rem">
                <input type="checkbox" id="pref-slow" style="width:18px;height:18px" ${!prefs.hideSlowItems ? 'checked' : ''}> Show Slow-Moving Items
            </label>
            <label style="display:flex;align-items:center;gap:12px;cursor:pointer;font-size:0.95rem">
                <input type="checkbox" id="pref-recent" style="width:18px;height:18px" ${!prefs.hideRecentInvoices ? 'checked' : ''}> Show Recent Invoices
            </label>
        </div>
        <div class="modal-actions" style="margin-top:20px">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveDashboardSettings()">Save Settings</button>
        </div>
    `);
}

function saveDashboardSettings() {
    const prefs = {
        hideFilters: !$('pref-filters').checked,
        hideTopKPIs: !$('pref-top').checked,
        hideSecondaryKPIs: !$('pref-sec').checked,
        hideQuickActions: !$('pref-actions').checked,
        hideSlowItems: !$('pref-slow').checked,
        hideRecentInvoices: !$('pref-recent').checked
    };
    currentUser.dashboardPrefs = prefs;

    // persist dashboardPrefs in Supabase
    if (currentUser && currentUser.id) {
        DB.update('users', currentUser.id, { dashboardPrefs: prefs }).catch(e => console.warn('dashboardPrefs save:', e.message));
    }

    closeModal();
    renderDashboard();
    showToast('Dashboard layout saved successfully.', 'success');
}

// =============================================
//  PARTIES
// =============================================
// =============================================
//  PARTIES
// =============================================
let _partyTab = 'all';

async function renderParties() {
    window._bulkParties = new Set();
    const parties = await DB.getAll('parties');
    const customers = parties.filter(p => p.type === 'Customer');
    const suppliers = parties.filter(p => p.type === 'Supplier');
    let shown = _partyTab === 'customer' ? customers : _partyTab === 'supplier' ? suppliers : parties;

    // Apply balance filter if coming from dashboard receivable/payable card
    const balFilter = window._partyBalanceFilter;
    if (balFilter === 'receivable') shown = shown.filter(p => (p.balance || 0) < 0);
    else if (balFilter === 'payable') shown = shown.filter(p => (p.balance || 0) > 0);

    const balFilterBadge = balFilter ? `
        <div style="display:flex;align-items:center;gap:8px;padding:8px 14px;background:${balFilter==='receivable'?'#d1fae5':'#fee2e2'};border-radius:8px;margin-bottom:10px;font-size:0.85rem;font-weight:600;color:${balFilter==='receivable'?'#065f46':'#991b1b'}">
            ${balFilter==='receivable'?'🟢 Showing: Parties with Receivable Balance':'🔴 Showing: Parties with Payable Balance'}
            <button onclick="window._partyBalanceFilter=null;renderParties()" style="margin-left:auto;background:none;border:none;cursor:pointer;font-size:1rem;padding:0;color:inherit">✕ Clear</button>
        </div>` : '';

    pageContent.innerHTML = `
        ${balFilterBadge}
        <div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">
            <button class="catalog-pill ${_partyTab==='all'?'active':''}" onclick="_partyTab='all';window._partyBalanceFilter=null;renderParties()">👥 All Parties (${parties.length})</button>
            <button class="catalog-pill ${_partyTab==='customer'?'active':''}" onclick="_partyTab='customer';window._partyBalanceFilter=null;renderParties()">🛍️ Customers (${customers.length})</button>
            <button class="catalog-pill ${_partyTab==='supplier'?'active':''}" onclick="_partyTab='supplier';window._partyBalanceFilter=null;renderParties()">🏭 Suppliers (${suppliers.length})</button>
        </div>
        <div class="section-toolbar">
            <input class="search-box" id="party-search" placeholder="Search parties..." oninput="filterPartyTable()">
            <div class="filter-group">
                <button class="btn btn-outline" onclick="openColumnPersonalizer('parties','renderParties')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button>
                ${!isSalesman() ? `<button class="btn btn-outline" onclick="downloadPartyTemplate()">📋 Party Template</button>
                <button class="btn btn-outline" onclick="exportPartiesExcel()">📤 Export Parties</button>
                <button class="btn btn-outline" onclick="importPartyExcel()">📥 Import Parties</button>
                <button class="btn btn-outline" style="border-color:#f59e0b;color:#f59e0b" onclick="downloadOpeningBalTemplate()">📋 Opening Bal Template</button>
                <button class="btn btn-outline" style="border-color:#f59e0b;color:#f59e0b" onclick="importOpeningBalExcel()">📥 Import Opening Bal</button>
                <button class="btn btn-primary" onclick="openPartyModal()">+ Add Party</button>` : ''}
            </div>
        </div>
        <div id="bulk-bar-par" style="display:none;align-items:center;gap:8px;background:var(--accent);color:#fff;padding:8px 12px;border-radius:8px;margin-bottom:8px;flex-wrap:wrap">
            <span id="bulk-cnt-par" style="font-weight:700;flex:1">0 selected</span>
            <button class="btn" onclick="bulkActivateParties()" style="background:#fff;color:#10b981;padding:4px 10px;font-size:0.82rem;font-weight:600">✅ Active</button>
            <button class="btn" onclick="bulkBlockParties()" style="background:#fff;color:#ef4444;padding:4px 10px;font-size:0.82rem;font-weight:600">🚫 Block</button>
            <button class="btn" onclick="bulkDeleteParties()" style="background:#ef4444;color:#fff;padding:4px 10px;font-size:0.82rem;font-weight:600">🗑️ Delete</button>
            <button class="btn" onclick="clearBulkParties()" style="background:rgba(255,255,255,0.2);color:#fff;padding:4px 10px;font-size:0.82rem">✕ Clear</button>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table" style="min-width:920px;width:100%"><thead><tr><th style="width:36px;text-align:center"><input type="checkbox" id="bulk-all-par" onchange="toggleSelectAllParties(this)" style="width:16px;height:16px;cursor:pointer"></th>${ColumnManager.get('parties').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="party-tbody">${renderPartyRows(shown)}</tbody></table>
            </div>
        </div></div>
        <input type="file" id="party-file-input" accept=".csv,.txt,.xlsx,.xls" style="display:none" onchange="processPartyImport(event)">`;
}
function renderPartyRows(parties) {
    if (!parties.length) return '<tr><td colspan="8"><div class="empty-state"><span class="empty-icon">👥</span><p>No parties found</p></div></td></tr>';
    const cols = ColumnManager.get('parties').filter(c => c.visible);
    return parties.map(p => {
        const cellMap = {
            name:      `<td style="color:var(--text-primary);font-weight:600">${escapeHtml(p.name)}${p.blocked ? ' <span class="badge badge-danger" style="font-size:0.7rem;padding:2px 5px">🔒 Blocked</span>' : ''}${p.active === false ? ' <span class="badge badge-danger" style="font-size:0.7rem;padding:2px 5px">Inactive</span>' : ''}</td>`,
            partyCode: `<td style="font-family:monospace;font-size:0.82rem;color:var(--accent)">${p.partyCode||'-'}</td>`,
            type:    `<td><span class="badge ${p.type === 'Customer' ? 'badge-success' : 'badge-info'}">${p.type}</span></td>`,
            phone:   `<td>${p.phone || '-'}</td>`,
            gstin:   `<td style="font-size:0.82rem">${p.gstin || '-'}</td>`,
            balance: `<td class="${(p.balance||0) < 0 ? 'amount-green' : 'amount-red'}">${currency(Math.abs(p.balance || 0))} ${(p.balance||0) < 0 ? '(Cr)' : '(Dr)'}</td>`,
            actions: `<td><div class="action-btns">
                <button class="btn-icon" onclick="openDedicatedPartyLedger('${p.id}')" title="View Ledger">📜</button>
                ${p.phone ? `<a href="tel:${p.phone}" class="btn-icon" title="Call party" style="text-decoration:none">📞</a>` : ''}
                ${p.lat && p.lng ? `<button class="btn-icon" onclick="openPartyMap('${p.lat}','${p.lng}','${escapeHtml(p.name)}')" title="Navigate to party">🗺️</button>` : ''}
                ${!isPacker() && !(p.lat && p.lng) ? `<button class="btn-icon" onclick="updatePartyLocation('${p.id}')" title="Update Location" style="color:#3b82f6">📍</button>` : ''}
                ${canEdit() ? `<button class="btn-icon" onclick="openPartyModal('${p.id}')">✏️</button><button class="btn-icon" onclick="deleteParty('${p.id}')">🗑️</button>` : ''}
            </div></td>`,
            city:         `<td style="font-size:0.85rem">${escapeHtml(p.city || '-')}</td>`,
            postCode:     `<td style="font-size:0.85rem">${escapeHtml(p.postCode || '-')}</td>`,
            paymentTerms: `<td style="font-size:0.82rem">${p.paymentTerms ? `<span class="badge badge-info" style="font-size:0.72rem">${escapeHtml(p.paymentTerms)}</span>` : '<span style="color:var(--text-muted)">-</span>'}</td>`,
            address:      `<td style="font-size:0.82rem;color:var(--text-muted);max-width:220px;white-space:normal">${escapeHtml(p.address || '-')}</td>`,
        };
        return `<tr data-type="${p.type}"><td style="width:36px;text-align:center"><input type="checkbox" class="bulk-chk-party" data-id="${p.id}" onchange="toggleBulkParty('${p.id}',this)" style="width:16px;height:16px;cursor:pointer" ${window._bulkParties && window._bulkParties.has(p.id) ? 'checked' : ''}></td>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
async function filterPartyTable() {
    const search = ($('party-search')||{}).value.toLowerCase();
    let parties = await DB.getAll('parties');
    if (_partyTab === 'customer') parties = parties.filter(p => p.type === 'Customer');
    if (_partyTab === 'supplier') parties = parties.filter(p => p.type === 'Supplier');
    if (search) parties = parties.filter(p =>
        p.name.toLowerCase().includes(search) ||
        (p.phone || '').includes(search) ||
        (p.city || '').toLowerCase().includes(search) ||
        (p.postCode || '').includes(search) ||
        (p.address || '').toLowerCase().includes(search)
    );
    $('party-tbody').innerHTML = renderPartyRows(parties);
}
async function onPartyTypeChange(sel) {
    if (sel.dataset.isNew === 'true') {
        const type = sel.value;
        const code = await nextPartyCode(type);
        const el = document.getElementById('f-party-code');
        if (el) el.value = code;
    }
}

async function openPartyModal(id) {
    const parties = await DB.getAll('parties');
    const p = id ? parties.find(x => x.id === id) : null;
    let defaultCode = '';
    if (!p) {
        defaultCode = await nextPartyCode('Customer');
    }
    openModal(p ? 'Edit Party' : 'Add Party', `
        <div class="form-group"><label>Name *</label><input id="f-party-name" value="${p ? p.name : ''}"></div>
        <div class="form-group"><label>Party Code <span style="font-size:0.78rem;color:var(--text-muted)">(auto-generated from number series)</span></label>
            <input id="f-party-code" class="form-control" value="${p ? p.partyCode || '' : defaultCode}" placeholder="e.g. CUST-00001" style="text-transform:uppercase;font-family:monospace" oninput="this.value=this.value.toUpperCase().replace(/[^A-Z0-9-]/g,'')">
        </div>
        <div class="form-row"><div class="form-group"><label>Type</label><select id="f-party-type" data-is-new="${!p}" onchange="onPartyTypeChange(this)"><option ${p && p.type === 'Customer' ? 'selected' : ''}>Customer</option><option ${p && p.type === 'Supplier' ? 'selected' : ''}>Supplier</option></select></div>
        <div class="form-group"><label>Phone</label><input id="f-party-phone" value="${p ? p.phone || '' : ''}"></div></div>
        <div class="form-row"><div class="form-group"><label>City</label><input id="f-party-city" value="${p ? p.city || '' : ''}"></div>
        <div class="form-group"><label>Post Code</label><input id="f-party-postcode" value="${p ? p.postCode || '' : ''}" placeholder="PIN / ZIP"></div></div>
        <div class="form-row"><div class="form-group"><label>GSTIN</label><input id="f-party-gstin" value="${p ? p.gstin || '' : ''}"></div>
        <div class="form-group"><label>Payment Terms</label><select id="f-party-terms">
            <option value="">-- None / Default --</option>
            ${getPaymentTermsList().map(t => `<option value="${escapeHtml(t.name)}" ${p && p.paymentTerms === t.name ? 'selected' : ''}>${escapeHtml(t.name)} (${t.days}d)</option>`).join('')}
        </select></div></div>
        <div class="form-group"><label>Address</label><input id="f-party-addr" value="${p ? p.address || '' : ''}" placeholder="Street, Area..."></div>
        <div class="form-group">
            <label>📍 GPS Location <small style="color:var(--text-muted)">${p && p.lat ? `Saved: ${(+p.lat).toFixed(5)}, ${(+p.lng).toFixed(5)}` : 'Not set'}</small></label>
            <div style="display:flex;gap:8px;align-items:center;margin-bottom:6px">
                <input id="f-party-lat" type="number" step="any" placeholder="Latitude" value="${p && p.lat ? p.lat : ''}" style="flex:1">
                <input id="f-party-lng" type="number" step="any" placeholder="Longitude" value="${p && p.lng ? p.lng : ''}" style="flex:1">
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap">
                <button class="btn btn-primary btn-sm" type="button" onclick="capturePartyLiveGPS()" id="btn-live-gps" style="flex:1">📍 Use My Live Location</button>
                <button class="btn btn-outline btn-sm" type="button" onclick="capturePartyGPS()" id="btn-addr-gps" style="flex:1">🔍 Search from Address</button>
            </div>
            <small style="color:var(--text-muted);display:block;margin-top:4px">💡 Go to customer location and tap "Live Location" for best accuracy</small>
            ${p && p.lat && p.lng ? `<a href="https://www.google.com/maps?q=${p.lat},${p.lng}" target="_blank" style="font-size:0.8rem;color:var(--primary);display:inline-block;margin-top:4px">🗺️ View on Google Maps</a>` : ''}
        </div>
        ${p && p.type === 'Customer' && canEdit() ? `
        <div style="background:rgba(239,68,68,0.06);border:1px solid rgba(239,68,68,0.25);border-radius:8px;padding:10px;margin-bottom:12px">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:12px">
                <div>
                    <strong style="font-size:0.88rem">🔒 Block Customer</strong>
                    <div style="font-size:0.78rem;color:var(--text-muted)">Blocked customers cannot place new Sales Orders or Invoices</div>
                </div>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;margin:0;flex-shrink:0">
                    <input type="checkbox" id="f-party-blocked" ${p && p.blocked ? 'checked' : ''} style="width:18px;height:18px">
                    <span style="font-size:0.85rem;font-weight:600">${p && p.blocked ? '🔒 Blocked' : '✅ Active'}</span>
                </label>
            </div>
        </div>` : ''}
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveParty('')">＋ Save & New</button>` : ''}
        <button class="btn btn-primary" onclick="saveParty('${id || ''}')">Save Party</button>`);
}
async function saveParty(id) {
    const name = $('f-party-name').value.trim();
    if (!name) return alert('Name is required');
    const latVal = $('f-party-lat') ? $('f-party-lat').value.trim() : '';
    const lngVal = $('f-party-lng') ? $('f-party-lng').value.trim() : '';
    const partyCode = $('f-party-code') ? $('f-party-code').value.trim().toUpperCase() : '';
    const data = {
        name,
        partyCode: partyCode || null,
        type: $('f-party-type').value,
        phone: $('f-party-phone').value.trim(),
        city: $('f-party-city').value.trim(),
        gstin: $('f-party-gstin') ? $('f-party-gstin').value.trim() : '',
        address: $('f-party-addr') ? $('f-party-addr').value.trim() : '',
        postCode: $('f-party-postcode') ? $('f-party-postcode').value.trim() : '',
        paymentTerms: $('f-party-terms') ? $('f-party-terms').value : '',
        lat: latVal ? parseFloat(latVal) : null,
        lng: lngVal ? parseFloat(lngVal) : null,
        blocked: $('f-party-blocked') ? $('f-party-blocked').checked : false
    };

    try {
        if (id) {
            await DB.update('parties', id, data);
        } else {
            await DB.insert('parties', { ...data, balance: 0 });
        }
        
        closeModal();
        await renderParties();
        showToast('Party saved successfully', 'success');
        if (window._saveAndNew) {
            window._saveAndNew = false;
            openPartyModal();
        }
    } catch (err) {
        window._saveAndNew = false;
        alert('Error saving party: ' + (err.message || err.details || JSON.stringify(err)));
    }
}

// ── GPS Quick Update Feature ──
async function openPartyGpsModal() {
    const parties = await DB.getAll('parties');
    const noGpsParties = parties.filter(p => !p.lat || !p.lng);
    
    openModal('📍 Update Party GPS', `
        <div style="margin-bottom:12px">
            <input type="text" id="f-gps-search" placeholder="🔍 Search missing GPS parties..." 
                   style="width:100%;padding:10px;border-radius:var(--radius-md);border:1px solid var(--border);"
                   onkeyup="filterGpsPartyList(this.value)">
        </div>
        <div id="gps-party-list" style="max-height:60vh;overflow-y:auto;">
            ${renderGpsPartyList(noGpsParties)}
        </div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Close</button>`);
    
    // Store original list for filtering
    window._gpsPartiesData = noGpsParties;
}

function renderGpsPartyList(parties) {
    if (!parties.length) return `<div class="empty-state"><span class="empty-icon">🎉</span><p>All Caught Up</p><p class="empty-subtitle">All parties have GPS locations saved!</p></div>`;
    
    return parties.map(p => `
        <div class="card" id="gps-row-${p.id}" style="margin-bottom:10px;padding:12px;display:flex;justify-content:space-between;align-items:center;gap:10px;background:#f8fafc;border:1px solid var(--border);">
            <div style="flex:1;min-width:0;">
                <div style="font-weight:600;font-size:0.95rem;color:var(--text-primary);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${p.name}</div>
                <div style="font-size:0.8rem;color:var(--text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${p.phone || 'No phone'} • ${p.address || p.city || 'No address'}</div>
            </div>
            <button class="btn btn-primary btn-sm" onclick="updatePartyLiveLocation('${p.id}')" style="white-space:nowrap;display:flex;align-items:center;gap:4px">
                📍 Update
            </button>
        </div>
    `).join('');
}

function filterGpsPartyList(q) {
    const s = q.toLowerCase();
    const filtered = (window._gpsPartiesData || []).filter(p => p.name.toLowerCase().includes(s) || (p.phone && p.phone.includes(s)));
    $('gps-party-list').innerHTML = renderGpsPartyList(filtered);
}

async function updatePartyLiveLocation(partyId) {
    if (!navigator.geolocation) return alert('Geolocation is not supported by this browser.');
    
    const row = document.getElementById('gps-row-' + partyId);
    const btn = row.querySelector('button');
    const origHtml = btn.innerHTML;
    btn.innerHTML = '🕒 ...';
    btn.disabled = true;

    navigator.geolocation.getCurrentPosition(async (pos) => {
        try {
            const lat = pos.coords.latitude;
            const lng = pos.coords.longitude;
            await DB.update('parties', partyId, { lat, lng });
            
            showToast('GPS Updated Successfully!', 'success');
            
            // Remove from list
            row.style.opacity = '0';
            setTimeout(() => {
                row.remove();
                if (window._gpsPartiesData) {
                    window._gpsPartiesData = window._gpsPartiesData.filter(p => p.id !== partyId);
                    if (window._gpsPartiesData.length === 0) {
                        $('gps-party-list').innerHTML = renderGpsPartyList([]);
                    }
                }
            }, 300);
            
        } catch (e) {
            alert('Error updating GPS: ' + e.message);
            btn.innerHTML = origHtml;
            btn.disabled = false;
        }
    }, (err) => {
        alert('GPS Failed: ' + err.message + '\nPlease enable location permissions.');
        btn.innerHTML = origHtml;
        btn.disabled = false;
    }, { enableHighAccuracy: true });
}
function capturePartyLiveGPS() {
    if (!navigator.geolocation) return alert('Geolocation is not supported by this browser/device.');
    const btn = $('btn-live-gps');
    if (btn) { btn.textContent = '⏳ Getting location...'; btn.disabled = true; }
    navigator.geolocation.getCurrentPosition(
        pos => {
            $('f-party-lat').value = pos.coords.latitude.toFixed(6);
            $('f-party-lng').value = pos.coords.longitude.toFixed(6);
            if (btn) { btn.textContent = '✅ Location Captured'; btn.disabled = false; }
            showToast(`Live GPS captured: ${pos.coords.latitude.toFixed(5)}, ${pos.coords.longitude.toFixed(5)} (±${Math.round(pos.coords.accuracy)}m)`, 'success');
        },
        err => {
            if (btn) { btn.textContent = '📍 Use My Live Location'; btn.disabled = false; }
            const msg = err.code === 1 ? '⚠️ Location permission denied — enter coordinates manually or use Search from Address.' :
                        err.code === 2 ? '⚠️ GPS unavailable. Make sure location is on.' :
                        '⚠️ Location timed out. Try again.';
            showToast(msg, 'warning');
        },
        { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 }
    );
}
async function updatePartyLocation(id) {
    if (!navigator.geolocation) return alert('Geolocation not supported by this browser/device.');
    // Find party name for the confirmation prompt
    const parties = await DB.getAll('parties');
    const p = parties.find(x => x.id === id);
    if (!p) return;
    showToast('Getting your live location...', 'info');
    navigator.geolocation.getCurrentPosition(
        async pos => {
            const lat = pos.coords.latitude.toFixed(6);
            const lng = pos.coords.longitude.toFixed(6);
            const acc = Math.round(pos.coords.accuracy);
            if (!confirm(`📍 Update location for:\n${p.name}\n\nCoordinates: ${lat}, ${lng}\nAccuracy: ±${acc}m\n\nConfirm?`)) return;
            await DB.update('parties', id, { ...p, lat, lng });
            showToast(`Location saved for ${p.name} (±${acc}m accuracy)`, 'success');
            renderParties();
        },
        err => {
            const msg = err.code === 1 ? 'Location permission denied — allow location access in browser settings.' :
                        err.code === 2 ? 'GPS unavailable. Turn on location services.' :
                        'Location timed out. Try again.';
            alert('⚠️ ' + msg);
        },
        { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 }
    );
}
async function capturePartyGPS() {
    const addr = $('f-party-addr') ? $('f-party-addr').value.trim() : '';
    const city = $('f-party-city') ? $('f-party-city').value.trim() : '';
    const name = $('f-party-name') ? $('f-party-name').value.trim() : '';
    const query = [addr, city, name, 'India'].filter(Boolean).join(', ');

    if (!addr && !city) return alert('Enter the party\'s Address or City first.');

    const btn = $('btn-addr-gps');
    if (btn) { btn.textContent = '⏳ Searching...'; btn.disabled = true; }

    try {
        const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=5&countrycodes=in&addressdetails=1`;
        const res = await fetch(url, { headers: { 'Accept-Language': 'en' } });
        const data = await res.json();

        if (btn) { btn.textContent = '🔍 Search from Address'; btn.disabled = false; }

        if (!data.length) return alert(`No location found for "${[addr, city].filter(Boolean).join(', ')}". Try a shorter or more general address.`);

        if (data.length === 1) {
            $('f-party-lat').value = parseFloat(data[0].lat).toFixed(6);
            $('f-party-lng').value = parseFloat(data[0].lon).toFixed(6);
            showToast(`Found: ${data[0].display_name.split(',').slice(0, 3).join(',')}`, 'success');
        } else {
            // Show picker if multiple results
            const opts = data.map(r => `<div onclick="selectGPSResult(${r.lat},${r.lon})" style="padding:10px;border:1px solid var(--border);border-radius:8px;margin-bottom:6px;cursor:pointer" onmouseover="this.style.background='rgba(0,212,170,0.1)'" onmouseout="this.style.background=''">
                <div style="font-size:0.85rem;font-weight:600">${r.display_name.split(',').slice(0, 3).join(',')}</div>
                <div style="font-size:0.75rem;color:var(--text-muted)">${parseFloat(r.lat).toFixed(5)}, ${parseFloat(r.lon).toFixed(5)}</div>
            </div>`).join('');
            openModal('Select Location', `<p style="margin-bottom:12px;font-size:0.85rem;color:var(--text-muted)">Multiple results found. Tap the correct one:</p>${opts}<div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button></div>`);
        }
    } catch (e) {
        if (btn) { btn.textContent = '🔍 Search from Address'; btn.disabled = false; }
        alert('Search failed: ' + e.message);
    }
}
function selectGPSResult(lat, lon) {
    $('f-party-lat').value = parseFloat(lat).toFixed(6);
    $('f-party-lng').value = parseFloat(lon).toFixed(6);
    closeModal();
    showToast(`Location set: ${parseFloat(lat).toFixed(5)}, ${parseFloat(lon).toFixed(5)}`, 'success');
}

function openPartyMap(lat, lng, name) {
    const url = `https://www.google.com/maps?q=${lat},${lng}&z=16&label=${encodeURIComponent(name)}`;
    window.open(url, '_blank');
}

async function deleteParty(id) {
    // BUG-021 fix: fetch fresh data from Supabase to avoid stale cache false negatives
    const sid = String(id);
    const [orders, invoices, payments] = await Promise.all([
        DB.getAll('sales_orders'),
        DB.getAll('invoices'),
        DB.getAll('payments')
    ]);

    const hasOrders   = orders.some(x => String(x.partyId) === sid);
    const hasInvoices = invoices.some(x => String(x.partyId) === sid);
    const hasPayments = payments.some(x => String(x.partyId) === sid);

    if (hasOrders || hasInvoices || hasPayments) {
        return alert('Cannot delete — this party has linked orders, invoices, or payments.');
    }

    if (!confirm('Delete this party? This cannot be undone.')) return;

    try {
        await DB.delete('parties', id);
        await renderParties();
        showToast('Party deleted successfully', 'success');
    } catch (err) {
        alert('Error deleting party: ' + (err.message || err.details || JSON.stringify(err)));
    }
}

// --- Party Excel Import ---
function downloadPartyTemplate() {
    let csv = 'Party Code *,Name *,Type (Customer/Supplier) *,Phone,GSTIN,Address,City,Post Code,Location Lat,Location Lng,Opening Balance,Credit Limit,Payment Terms (COD/Net7/Net15/Net30/Net60),Blocked (true/false)\n';
    csv += 'ACME,Acme Corp,Customer,9988776655,27AADCA2230M1Z2,123 Main St,Mumbai,400001,19.0760,72.8777,1000,50000,Net30,false\n';
    csv += 'GLOB,Global Supplies,Supplier,9876543210,,45 Park Ave,Delhi,110001,,0,0,COD,false\n';
    csv += 'RAJE,Raj Traders,Customer,9876500001,29ABCDE1234F1Z5,MG Road,Bangalore,560001,12.9716,77.5946,500,20000,Net15,false\n';
    downloadCSV(csv, 'party_import_template.csv');
    showToast('Party template downloaded!', 'success');
}

function downloadOpeningBalTemplate() {
    let csv = 'Party Code *,Party Name *,Party Type (Customer/Supplier),Invoice No *,Invoice Date (YYYY-MM-DD) *,Invoice Amount *,Due Date (YYYY-MM-DD),Notes\n';
    csv += 'ACME,Acme Corp,Customer,INV-001,2024-01-15,15000,2024-02-14,Outstanding from Jan\n';
    csv += 'ACME,Acme Corp,Customer,INV-002,2024-02-10,8500,2024-03-11,\n';
    csv += 'RAJE,Raj Enterprises,Customer,INV-101,2024-03-01,22000,2024-03-31,\n';
    csv += 'GLOB,Global Supplies,Supplier,PINV-55,2024-02-20,45000,2024-03-20,Purchase outstanding\n';
    downloadCSV(csv, 'opening_balance_import_template.csv');
    showToast('Opening balance template downloaded!', 'success');
}

function importOpeningBalExcel() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.csv';
    input.onchange = processOpeningBalImport;
    input.click();
}

async function processOpeningBalImport(event) {
    const file = event.target.files[0];
    if (!file) return;
    const text = await file.text();
    const lines = text.split(/\r?\n/).filter(l => l.trim());
    if (lines.length < 2) return alert('File is empty or has no data rows');

    const parties = DB.get('db_parties') || [];
    const errors = [];
    const preview = [];

    for (let i = 1; i < lines.length; i++) {
        const cols = parseCSVLine(lines[i]);
        const [partyCode, partyName, partyType, invoiceNo, invoiceDate, amountStr, dueDate, notes] = cols;
        if (!partyCode || !partyName || !invoiceNo || !invoiceDate || !amountStr) {
            errors.push(`Row ${i+1}: Missing required fields (Party Code, Party Name, Invoice No, Invoice Date, Amount)`);
            continue;
        }
        const amount = parseFloat(amountStr);
        if (isNaN(amount) || amount <= 0) { errors.push(`Row ${i+1}: Invalid amount "${amountStr}"`); continue; }
        // Find existing party by partyCode first, then by name
        const existing = parties.find(p => p.partyCode && p.partyCode.toUpperCase() === partyCode.trim().toUpperCase())
                      || parties.find(p => p.name.toLowerCase() === partyName.trim().toLowerCase());
        preview.push({
            partyCode: partyCode.trim().toUpperCase(),
            partyName: partyName.trim(),
            partyType: (partyType||'Customer').trim() || 'Customer',
            invoiceNo: invoiceNo.trim(),
            invoiceDate: invoiceDate.trim(),
            amount,
            dueDate: dueDate ? dueDate.trim() : '',
            notes: notes ? notes.trim() : '',
            existingParty: existing || null,
            action: existing ? 'update' : 'create'
        });
    }

    if (errors.length) {
        const proceed = confirm(`${errors.length} error(s) found:\n${errors.slice(0,5).join('\n')}\n\nProceed with valid rows?`);
        if (!proceed) return;
    }
    if (!preview.length) return alert('No valid rows to import');

    // Show preview modal
    openModal('Opening Balance Import Preview', `
        <p style="font-size:0.85rem;color:var(--text-muted);margin-bottom:12px">${preview.length} invoice rows ready to import. Parties marked <span style="color:#22c55e;font-weight:600">CREATE</span> will be created, <span style="color:#3b82f6;font-weight:600">UPDATE</span> will have balance added.</p>
        <div class="table-wrapper" style="max-height:300px;overflow-y:auto">
        <table class="data-table" style="font-size:0.8rem">
            <thead><tr><th>Party Code</th><th>Party Name</th><th>Invoice No</th><th>Date</th><th>Amount</th><th>Action</th></tr></thead>
            <tbody>${preview.map(r => `<tr>
                <td style="font-family:monospace;font-weight:600">${r.partyCode}</td>
                <td>${r.partyName}</td>
                <td style="font-family:monospace">${r.invoiceNo}</td>
                <td>${r.invoiceDate}</td>
                <td style="font-weight:600;color:var(--accent)">${currency(r.amount)}</td>
                <td><span class="badge ${r.action==='create'?'badge-success':'badge-info'}">${r.action.toUpperCase()}</span></td>
            </tr>`).join('')}</tbody>
        </table></div>
        <input type="hidden" id="ob-rows-json" value='${JSON.stringify(preview).replace(/'/g,"&apos;")}'>`,
        `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
         <button class="btn btn-primary" onclick="confirmOpeningBalImport()">✅ Confirm Import</button>`);
}

async function confirmOpeningBalImport() {
    const el = document.getElementById('ob-rows-json');
    if (!el) return;
    const rows = JSON.parse(el.value.replace(/&apos;/g,"'"));
    const parties = DB.get('db_parties') || [];
    let created = 0, updated = 0, invoices = 0;
    const partyMap = {}; // partyCode → party id

    // Group by partyCode
    const byParty = {};
    for (const r of rows) {
        if (!byParty[r.partyCode]) byParty[r.partyCode] = { info: r, invoices: [] };
        byParty[r.partyCode].invoices.push(r);
    }

    for (const [code, group] of Object.entries(byParty)) {
        const { info } = group;
        const totalAmount = group.invoices.reduce((s, r) => s + r.amount, 0);
        let partyId;

        if (info.existingParty) {
            // Update existing — add to balance
            partyId = info.existingParty.id;
            const newBal = (info.existingParty.balance || 0) + totalAmount;
            await DB.rawUpdate('parties', partyId, { balance: newBal, partyCode: code });
            updated++;
        } else {
            // Create new party
            partyId = 'P' + Date.now() + Math.random().toString(36).slice(2,5);
            await DB.rawInsert('parties', {
                id: partyId,
                name: info.partyName,
                partyCode: code,
                type: info.partyType,
                balance: totalAmount,
                credit_limit: 0,
                blocked: false
            });
            created++;
        }
        partyMap[code] = partyId;

        // Create party_ledger entries per invoice
        for (const r of group.invoices) {
            const runBal = r.amount; // individual entry amount
            await DB.rawInsert('party_ledger', {
                id: 'PL' + Date.now() + Math.random().toString(36).slice(2,6),
                date: r.invoiceDate,
                partyId,
                partyName: info.partyName,
                type: 'opening_balance',
                amount: r.amount,
                balance: runBal,
                docNo: r.invoiceNo,
                notes: r.notes || 'Opening balance import',
                createdBy: currentUser ? currentUser.name : 'Import'
            });
            invoices++;
        }
    }

    await DB.refreshTables(['parties', 'party_ledger']);
    closeModal();
    showToast(`Import done: ${created} parties created, ${updated} updated, ${invoices} invoice entries added`, 'success');
    await renderParties();
}

function importPartyExcel() {
    const input = $('party-file-input');
    if (input) { input.value = ''; input.click(); }
}

let pendingPartyImports = [];
function processPartyImport(event) {
    const file = event.target.files[0];
    if (!file) return;

    function parseText(text) {
        const lines = text.split(/\r?\n/).filter(l => l.trim());
        if (lines.length < 2) return alert('File is empty or has no data rows');
        const errors = [];
        pendingPartyImports = [];
        const parties = DB.get('db_parties');
        for (let i = 1; i < lines.length; i++) {
            const cols = parseCSVLine(lines[i]);
            if (cols.length < 2) { errors.push(`Row ${i + 1}: Not enough columns`); continue; }
            // Expected Order (matching template):
            // 0:PartyCode, 1:Name, 2:Type, 3:Phone, 4:GSTIN, 5:Address, 6:City, 7:PostCode, 8:Lat, 9:Lng, 10:OpeningBal, 11:CreditLimit, 12:PayTerms, 13:Blocked
            const [partyCode, name, typeStr, phone, gstin, address, city, postCode, lat, lng, balStr] = cols.map(c => (c || '').trim());
            if (!name) { errors.push(`Row ${i + 1}: Name is empty`); continue; }
            const existingParty = parties.find(p => p.name.toLowerCase() === name.toLowerCase());
            if (pendingPartyImports.some(p => p.name.toLowerCase() === name.toLowerCase())) {
                errors.push(`Row ${i + 1}: Duplicate party "${name}" in file.`); continue;
            }
            const partyType = typeStr.toLowerCase().includes('supp') ? 'Supplier' : 'Customer';
            const balance = +(balStr || 0);
            const entry = {
                name, partyCode: partyCode || null, type: partyType,
                phone: phone || '', city: city || '', gstin: gstin || '', address: address || '',
                postCode: postCode || '',
                lat: lat ? +lat : null,
                lng: lng ? +lng : null,
                balance: existingParty ? existingParty.balance : balance,
                isUpdate: !!existingParty
            };
            if (existingParty) entry.id = existingParty.id;
            pendingPartyImports.push(entry);
        }
        showPartyImportPreview(errors);
        event.target.value = '';
    }

    if (file.name.match(/\.xlsx?$/i) && typeof XLSX !== 'undefined') {
        const reader = new FileReader();
        reader.onload = e => {
            const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
            parseText(XLSX.utils.sheet_to_csv(wb.Sheets[wb.SheetNames[0]]));
        };
        reader.readAsArrayBuffer(file);
    } else {
        const reader = new FileReader();
        reader.onload = e => parseText(e.target.result);
        reader.readAsText(file);
    }
}

function showPartyImportPreview(errors) {
    let html = '';
    if (errors && errors.length) {
        html += `<div style="margin-bottom:14px;padding:12px;background:var(--danger-soft);border:1px solid rgba(239,68,68,0.3);border-radius:8px;font-size:0.85rem">
            <strong style="color:var(--danger)">⚠️ ${errors.length} Errors (Rows skipped)</strong>
            <ul style="margin-top:6px;padding-left:14px;color:var(--danger);max-height:80px;overflow-y:auto">
                ${errors.map(err => `<li>${err}</li>`).join('')}
            </ul>
        </div>`;
    }

    const newCount = pendingPartyImports.filter(p => !p.isUpdate).length;
    const updCount = pendingPartyImports.filter(p => p.isUpdate).length;
    html += `<div style="margin-bottom:10px;font-weight:600">✅ ${pendingPartyImports.length} Valid Parties <span style="font-size:0.8rem;color:var(--text-muted)">(${newCount} New, ${updCount} Update)</span></div>`;

    if (pendingPartyImports.length) {
        html += `<div style="max-height:350px;overflow-y:auto;border:1px solid var(--border);border-radius:var(--radius-sm)">
            <table class="data-table" style="font-size:0.85rem"><thead><tr><th>Code</th><th>Name</th><th>Type</th><th>Phone</th><th>City</th><th>GSTIN</th><th>Address</th><th>Status</th><th></th></tr></thead>
            <tbody>${pendingPartyImports.map((p, idx) => `<tr id="pi-row-${idx}">
                <td><input value="${p.partyCode||''}" onchange="pendingPartyImports[${idx}].partyCode=this.value.toUpperCase()" style="width:70px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.82rem;font-family:monospace"></td>
                <td><input value="${p.name}" onchange="pendingPartyImports[${idx}].name=this.value" style="width:100%;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.85rem"></td>
                <td><select onchange="pendingPartyImports[${idx}].type=this.value" style="background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px;color:var(--text-primary);font-size:0.85rem">
                    <option value="Customer" ${p.type === 'Customer' ? 'selected' : ''}>Customer</option>
                    <option value="Supplier" ${p.type === 'Supplier' ? 'selected' : ''}>Supplier</option>
                </select></td>
                <td><input value="${p.phone}" onchange="pendingPartyImports[${idx}].phone=this.value" style="width:80px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.85rem"></td>
                <td><input value="${p.city}" onchange="pendingPartyImports[${idx}].city=this.value" style="width:70px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.85rem"></td>
                <td><input value="${p.gstin}" onchange="pendingPartyImports[${idx}].gstin=this.value" style="width:100px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.85rem"></td>
                <td><input value="${p.address}" onchange="pendingPartyImports[${idx}].address=this.value" style="width:100px;background:var(--bg-input);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text-primary);font-size:0.85rem"></td>
                <td><span class="badge ${p.isUpdate ? 'badge-warning' : 'badge-success'}">${p.isUpdate ? 'Update' : 'New'}</span></td>
                <td><button class="btn-icon" onclick="pendingPartyImports.splice(${idx},1);showPartyImportPreview()" title="Remove">🗑️</button></td>
            </tr>`).join('')}</tbody></table>
        </div>`;
    }

    html += `<div class="modal-actions">
        <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="commitPartyImport()" ${pendingPartyImports.length === 0 ? 'disabled' : ''}>💾 Confirm & Import ${pendingPartyImports.length} Parties</button>
    </div>`;

    openModal('Import Parties Preview', html);
}


async function commitPartyImport() {
    let added = 0, updated = 0;

    try {
        for (const p of pendingPartyImports) {
            const { isUpdate, id, ...data } = p;
            if (isUpdate) {
                await DB.update('parties', id, data);
                updated++;
            } else {
                await DB.insert('parties', data); // no id — Supabase auto-generates UUID
                added++;
            }
        }

        pendingPartyImports = [];
        closeModal();
        await renderParties();
        showToast(`Import complete! ${added} added, ${updated} updated.`, 'success');
    } catch (err) {
        alert('Error during party import: ' + err.message);
    }
}


// =============================================
//  CATEGORIES MASTER
// =============================================
async function renderCategories() {
    const cats = await DB.getAll('categories');
    const container = $('inv-setup-content') || pageContent;
    container.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">🏷️ Categories</h3>
            <div class="filter-group">
                <button class="btn btn-outline" onclick="triggerCategorizeExcelImport()">📥 Import</button>
                <input type="file" id="f-cat-import" accept=".xlsx, .xls" style="display:none" onchange="importCategoriesExcel(event)">
                <button class="btn btn-primary" onclick="openCategoryModal()">+ Add Category</button>
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Category Name</th><th>Sub-Categories</th><th>Actions</th></tr></thead>
                <tbody>${cats.length ? cats.map(c => `<tr>
                    <td style="font-weight:600">${c.name}</td>
                    <td>${(c.subCategories || []).join(', ') || '-'}</td>
                    <td><div class="action-btns"><button class="btn-icon" onclick="openCategoryModal('${c.id}')" title="Edit">✏️</button><button class="btn-icon" onclick="deleteCategory('${c.id}')" title="Delete">🗑️</button></div></td>
                </tr>`).join('') : '<tr><td colspan="3"><div class="empty-state"><span class="empty-icon">🏷️</span><p>No categories defined</p><p class="empty-subtitle">Add your first category above</p></div></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}

async function openCategoryModal(id) {
    const cats = await DB.getAll('categories');
    const c = id ? cats.find(x => x.id === id) : null;
    openModal(c ? 'Edit Category' : 'Add Category', `
        <div class="form-group"><label>Category Name *</label><input id="f-cat-name" value="${c ? c.name : ''}"></div>
        <div class="form-group"><label>Sub-Categories (comma separated)</label><input id="f-cat-subs" value="${c ? (c.subCategories || []).join(', ') : ''}" placeholder="e.g. Mobile, Laptop, Tablet"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveCategory('')">＋ Save & New</button>` : ''}<button class="btn btn-primary" onclick="saveCategory('${id || ''}')">Save Category</button></div>
    `);
}

async function saveCategory(id) {
    const name = $('f-cat-name').value.trim();
    if (!name) return alert('Category Name is required');
    const subsStr = $('f-cat-subs').value.trim();
    const subCategories = subsStr ? subsStr.split(',').map(s => s.trim()).filter(s => s) : [];

    try {
        if (id) {
            await DB.update('categories', id, { name, subCategories });
        } else {
            const cats = await DB.getAll('categories');
            if (cats.some(c => c.name.toLowerCase() === name.toLowerCase())) return alert('Category already exists');
            await DB.insert('categories', { name, subCategories });
        }
        closeModal();
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderCategories();
        showToast('Category saved!', 'success');
        if (window._saveAndNew) { window._saveAndNew = false; openCategoryModal(); }
    } catch (err) {
        window._saveAndNew = false;
        alert('Error saving category: ' + err.message);
    }
}

async function deleteCategory(id) {
    if (!confirm('Delete this category? Items using it will retain the text value but lose the reference.')) return;
    try {
        await DB.delete('categories', id);
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderCategories();
        showToast('Category deleted!', 'warning');
    } catch (err) {
        alert('Error deleting category: ' + err.message);
    }
}

function triggerCategorizeExcelImport() {
    $('f-cat-import').click();
}

async function importCategoriesExcel(e) {
    const file = e.target.files[0];
    if (!file) return;
    try {
        const data = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const workbook = XLSX.read(e.target.result, {type: 'binary'});
                const firstSheet = workbook.SheetNames[0];
                const excelRows = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[firstSheet]);
                resolve(excelRows);
            };
            reader.onerror = reject;
            reader.readAsBinaryString(file);
        });

        if (!data || data.length === 0) return alert('No data found in the Excel file.');
        
        const existingCats = await DB.getAll('categories');
        let added = 0;
        let updated = 0;

        for (const row of data) {
            const name = (row['Category Name'] || row['CategoryName'] || row['Name'] || '').toString().trim();
            if (!name) continue;

            const subsStr = (row['Sub-Categories'] || row['SubCategories'] || row['Sub Categories'] || '').toString().trim();
            const subCategories = subsStr ? subsStr.split(',').map(s => s.trim()).filter(s => s) : [];

            const existing = existingCats.find(c => c.name.toLowerCase() === name.toLowerCase());
            if (existing) {
                // Merge subcategories (prevent duplicates)
                const mergedSubs = [...new Set([...(existing.subCategories || []), ...subCategories])];
                await DB.update('categories', existing.id, { subCategories: mergedSubs });
                updated++;
            } else {
                await DB.insert('categories', { name, subCategories });
                added++;
            }
        }

        e.target.value = ''; // Reset input
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderCategories();
        showToast(`Import complete! ${added} added, ${updated} updated.`, 'success');

    } catch (err) {
        alert('Error parsing Excel: ' + err.message);
    }
}

// --- ABC Analysis Helper ---
function getABCAnalysis(items) {
    if (!items.length) return {};
    const sorted = [...items].sort((a, b) => (b.stock * (b.purchasePrice || 0)) - (a.stock * (a.purchasePrice || 0)));
    const totalValue = sorted.reduce((s, i) => s + (i.stock * (i.purchasePrice || 0)), 0);
    if (totalValue === 0) return {};
    let cumulativeValue = 0;
    const analysis = {};
    sorted.forEach(i => {
        cumulativeValue += (i.stock * (i.purchasePrice || 0));
        const percent = (cumulativeValue / totalValue) * 100;
        if (percent <= 70) analysis[i.id] = 'A';
        else if (percent <= 90) analysis[i.id] = 'B';
        else analysis[i.id] = 'C';
    });
    return analysis;
}

// =============================================
//  STOCK COMPUTATION UTILITIES
// =============================================
// =============================================
//  STOCK COMPUTATION UTILITIES
// =============================================
function getAvailableStock(item) {
    const orders = DB.get('db_salesorders');   // sync
    let reserved = 0;
    (orders || []).forEach(o => {
        if ((o.status === 'pending' || o.status === 'approved') && !o.packed) {
            (o.items || []).forEach(li => {
                if (li.itemId === item.id) reserved += Number(li.qty);
            });
        }
    });
    return { stock: item.stock, reserved, available: (item.stock || 0) - reserved };
}

// =============================================
//  INVENTORY (BC-Style with Ledger & Adjustments)
// =============================================
function updateNavBadges(inventory) {
    const lowCount = (inventory||[]).filter(i => (i.stock||0) <= (i.lowStockAlert||5)).length;
    const badge = document.getElementById('nav-badge-inventory');
    if (badge) { badge.textContent = lowCount; badge.style.display = lowCount > 0 ? '' : 'none'; }
}

async function renderInventory() {
    window._bulkItems = new Set();
    // Fetch all required data in parallel
    const [items, salesOrders] = await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('salesorders')
    ]);

    updateNavBadges(items);
    const totalItems = items.length;
    const totalValue = items.reduce((s, i) => s + (i.stock * i.purchasePrice), 0);
    const lowStock = items.filter(i => i.stock <= (i.lowStockAlert || 5)).length;
    const totalStock = items.reduce((s, i) => s + i.stock, 0);
    // Pre-calculate reserved stock to avoid O(N^2) in the loop if we can
    const reservedMap = {};
    salesOrders.forEach(o => {
        if ((o.status === 'pending' || o.status === 'approved') && !o.packed) {
            o.items.forEach(li => {
                reservedMap[li.itemId] = (reservedMap[li.itemId] || 0) + Number(li.qty);
            });
        }
    });

    // Expose reservedMap globally so filterInvTable can use it during search
    window._inventoryReservedMap = reservedMap;

    pageContent.innerHTML = `
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px">
            <div class="dash-pulse-tile" style="--tile-color:#3b82f6;animation-delay:0.04s">
                <div class="dash-pulse-icon">📦</div>
                <div class="dash-pulse-val dash-count" data-val="${totalItems}" style="color:#3b82f6">${totalItems}</div>
                <div class="dash-pulse-lbl">Total Items</div>
            </div>
            <div class="dash-pulse-tile" style="--tile-color:#10b981;animation-delay:0.08s">
                <div class="dash-pulse-icon">📊</div>
                <div class="dash-pulse-val dash-count" data-val="${totalStock}" style="color:#10b981">${totalStock}</div>
                <div class="dash-pulse-lbl">Stock Qty</div>
            </div>
            <div class="dash-pulse-tile" style="--tile-color:#f59e0b;animation-delay:0.12s">
                <div class="dash-pulse-icon">💰</div>
                <div class="dash-pulse-val" style="color:#f59e0b;font-size:0.7rem">${currency(totalValue)}</div>
                <div class="dash-pulse-lbl">Stock Value</div>
            </div>
            <div class="dash-pulse-tile${lowStock ? ' dash-pulse-alert' : ''}" onclick="document.getElementById('inv-search')&&(document.getElementById('inv-cat-filter').value='__low__',filterInvTable())" style="--tile-color:${lowStock?'#ef4444':'#10b981'};animation-delay:0.16s;cursor:${lowStock?'pointer':'default'}">
                <div class="dash-pulse-icon">⚠️</div>
                <div class="dash-pulse-val" style="color:${lowStock?'#ef4444':'#10b981'}">${lowStock}</div>
                <div class="dash-pulse-lbl">Low Stock</div>
            </div>
        </div>
        <div class="section-toolbar">
            <input class="search-box" id="inv-search" placeholder="Search items..." oninput="filterInvTable()">
            <div class="filter-group" style="flex-wrap:wrap">
                <button class="btn btn-outline" onclick="openColumnPersonalizer('inventory','renderInventory')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button>
                ${canEdit() ? `<button class="btn btn-primary" onclick="openItemModal()">+ Add Item</button>
                <button class="btn btn-outline" onclick="openStockAdjustmentModal()">🔧 Stock Adjustment</button>
                <button class="btn btn-outline" onclick="exportInventoryExcel()">📤 Export Excel</button>
                <button class="btn btn-outline" style="border-color:var(--primary);color:var(--primary)" onclick="downloadItemTemplate()">📋 Item Template</button>
                <button class="btn btn-outline" style="border-color:var(--primary);color:var(--primary)" onclick="importItemExcel()">📥 Import Items</button>
                <button class="btn btn-outline" onclick="downloadStockTemplate()">📋 Stock Template</button>
                <button class="btn btn-outline" onclick="importStockExcel()">📥 Import Stock</button>` : ''}
            </div>
        </div>
        <div id="bulk-bar-inv" style="display:none;align-items:center;gap:8px;background:var(--accent);color:#fff;padding:8px 12px;border-radius:8px;margin-bottom:8px;flex-wrap:wrap">
            <span id="bulk-cnt-inv" style="font-weight:700;flex:1">0 selected</span>
            <button class="btn" onclick="bulkActivateItems()" style="background:#fff;color:#10b981;padding:4px 10px;font-size:0.82rem;font-weight:600">✅ Activate</button>
            <button class="btn" onclick="bulkDeactivateItems()" style="background:#fff;color:#f59e0b;padding:4px 10px;font-size:0.82rem;font-weight:600">⏸ Deactivate</button>
            <button class="btn" onclick="bulkDeleteItems()" style="background:#ef4444;color:#fff;padding:4px 10px;font-size:0.82rem;font-weight:600">🗑️ Delete</button>
            <button class="btn" onclick="clearBulkItems()" style="background:rgba(255,255,255,0.2);color:#fff;padding:4px 10px;font-size:0.82rem">✕ Clear</button>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table" id="inv-table" style="min-width:900px"><thead><tr><th style="width:36px;text-align:center"><input type="checkbox" id="bulk-all-inv" onchange="toggleSelectAllItems(this)" style="width:16px;height:16px;cursor:pointer"></th>${ColumnManager.get('inventory').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="inv-tbody">${renderInvRows(items, reservedMap, getABCAnalysis(items))}</tbody></table>
            </div>
        </div></div>
        <input type="file" id="stock-file-input" accept=".csv,.txt,.xlsx,.xls" style="display:none" onchange="processStockImport(event)">
        <input type="file" id="item-file-input" accept=".csv,.txt,.xlsx,.xls" style="display:none" onchange="processItemImport(event)">`;
    requestAnimationFrame(() => {
        document.querySelectorAll('.dash-count[data-val]').forEach(el => {
            const target = parseFloat(el.dataset.val) || 0;
            if (!target) return;
            const dur = 600, start = Date.now();
            const tick = () => {
                const p = Math.min((Date.now()-start)/dur,1), e=1-Math.pow(1-p,3);
                el.textContent = Math.round(target*e);
                if(p<1) requestAnimationFrame(tick);
            }; tick();
        });
    });
}

function renderInvRows(items, reservedMap = {}, abcMap = {}) {
    if (!items.length) return '<tr><td colspan="13"><div class="empty-state"><div class="empty-icon">📦</div><p>No items yet</p></div></td></tr>';
    const cols = ColumnManager.get('inventory').filter(c => c.visible);
    return items.map(i => {
        const reserved = reservedMap[i.id] || 0;
        const available = i.stock - reserved;
        const abc = abcMap[i.id] || 'C';
        const abcClass = abc === 'A' ? 'badge-primary' : abc === 'B' ? 'badge-info' : 'badge-outline';
        const cellMap = {
            name:          `<td><div style="display:flex;align-items:center;gap:8px">${(i.imageUrl || i.photo) ? `<img src="${i.imageUrl || i.photo}" style="width:32px;height:32px;border-radius:6px;object-fit:cover;flex-shrink:0">` : ''}<div><div style="color:var(--text-primary);font-weight:600">${i.name}${i.active === false ? ' <span class="badge badge-danger" style="font-size:0.7rem;padding:2px 5px">Inactive</span>' : ''}</div>${i.itemCode ? `<div style="font-size:0.75rem;color:var(--text-muted)">Code: ${i.itemCode}</div>` : ''}</div></div></td>`,
            abc:           `<td><span class="badge ${abcClass}" style="width:24px;text-align:center">${abc}</span></td>`,
            warehouse:     `<td style="font-size:0.85rem;color:var(--text-muted)">${i.warehouse || 'Main Warehouse'}</td>`,
            hsn:           `<td>${i.hsn || '-'}</td>`,
            unit:          `<td>${i.unit || 'Pcs'}${i.secUom ? `<br><span style="font-size:0.75rem;color:var(--text-muted)">1 ${i.unit} = ${i.secUomRatio || 0} ${i.secUom}</span>` : ''}</td>`,
            purchasePrice: `<td>${currency(i.purchasePrice)}${(() => { const nb = getLastActiveBatch(i); return nb && nb.purchasePrice !== i.purchasePrice ? `<br><span style="font-size:0.7rem;color:var(--text-muted)">Latest batch</span>` : ''; })()}</td>`,
            salePrice:     `<td>${currency(i.salePrice)}${(() => { const fb = getFifoBatch(i); return fb && (fb.qty||0) > 0 ? `<br><span style="font-size:0.7rem;color:var(--accent)">MRP ₹${fb.mrp}</span>` : ''; })()}</td>`,
            mrp:           `<td>${(() => { const fb = getFifoBatch(i); return fb ? currency(fb.mrp) : (i.mrp ? currency(i.mrp) : '-'); })()}${i.batches && i.batches.filter(b=>b.isActive!==false).length > 1 ? `<br><span style="font-size:0.7rem;color:var(--text-muted)">${i.batches.filter(b=>b.isActive!==false).length} batches</span>` : ''}</td>`,
            stock:         `<td>${i.stock}</td>`,
            reserved:      `<td>${reserved > 0 ? `<span style="color:var(--danger);font-weight:600">${reserved}</span>` : '0'}</td>`,
            avail:         `<td><span class="badge ${available <= (i.lowStockAlert || 5) ? 'badge-danger' : 'badge-success'}">${available}</span></td>`,
            value:         `<td>${currency(i.stock * i.purchasePrice)}</td>`,
            actions:       `<td><div class="action-btns">${canEdit() ? `<button class="btn-icon" onclick="openStockAdjustmentModal('${i.id}')" title="Adjust Stock">🔧</button>` : ''}<button class="btn-icon" onclick="viewItemLedger('${i.id}')" title="View Ledger">📜</button>${canEdit() ? `<button class="btn-icon" onclick="openItemModal('${i.id}')" title="Edit">✏️</button><button class="btn-icon" onclick="deleteItem('${i.id}')" title="Delete">🗑️</button>` : ''}</div></td>`,
        };
        return `<tr><td style="width:36px;text-align:center"><input type="checkbox" class="bulk-chk-item" data-id="${i.id}" onchange="toggleBulkItem('${i.id}',this)" style="width:16px;height:16px;cursor:pointer" ${window._bulkItems && window._bulkItems.has(i.id) ? 'checked' : ''}></td>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
function filterInvTable() {
    const s = $('inv-search').value.toLowerCase();
    let items = DB.get('db_inventory');
    if (s) items = items.filter(i => i.name.toLowerCase().includes(s) || (i.hsn || '').toLowerCase().includes(s));
    $('inv-tbody').innerHTML = renderInvRows(items, window._inventoryReservedMap || {}, getABCAnalysis(items));
}
let currentItemTiers = [];
let currentItemBatches = [];

// ── Batch / MRP Helpers ──
function getActiveBatches(item) {
    return (item.batches || []).filter(b => b.isActive !== false).sort((a,b) => (a.receivedDate||'') < (b.receivedDate||'') ? -1 : 1);
}
function getLastBatch(item) {
    const bs = item.batches || [];
    return bs.length ? bs[bs.length - 1] : null;
}
function getLastActiveBatch(item) {
    const active = getActiveBatches(item);
    return active.length ? active[active.length - 1] : null;
}
// FIFO: oldest active batch that still has stock > 0 (for sales pricing)
function getFifoBatch(item) {
    const active = getActiveBatches(item);
    return active.find(b => (b.qty || 0) > 0) || active[0] || null;
}
// Sync item prices using FIFO logic: salePrice/mrp from oldest with stock, purchasePrice from newest
function syncItemPricesFromBatches(batches) {
    const active = batches.filter(b => b.isActive !== false).sort((a,b) => (a.receivedDate||'') < (b.receivedDate||'') ? -1 : 1);
    const fifo   = active.find(b => (b.qty||0) > 0) || active[0];
    const newest = active[active.length - 1];
    const update = {};
    if (fifo)   { update.mrp = fifo.mrp; update.salePrice = fifo.salePrice; }
    if (newest) { update.purchasePrice = newest.purchasePrice; }
    return update;
}
// FIFO deduction: reduces batch qtys oldest-first, returns {updatedBatches, priceSync}
function deductBatchQtyFifo(item, qtyToDeduct) {
    if (!item.batches || !item.batches.length) return { updatedBatches: null, priceSync: {} };
    const batches = JSON.parse(JSON.stringify(item.batches));
    const active  = batches.filter(b => b.isActive !== false).sort((a,b) => (a.receivedDate||'') < (b.receivedDate||'') ? -1 : 1);
    let remaining = qtyToDeduct;
    for (const b of active) {
        if (remaining <= 0) break;
        const deduct = Math.min(remaining, b.qty || 0);
        b.qty = (b.qty || 0) - deduct;
        remaining -= deduct;
    }
    return { updatedBatches: batches, priceSync: syncItemPricesFromBatches(batches) };
}

function openItemModal(id) {
    window._editItemId = id || '';
    window._itemPhotoFile = null;
    const i = id ? DB.get('db_inventory').find(x => x.id === id) : null;
    currentItemTiers = i && i.priceTiers ? JSON.parse(JSON.stringify(i.priceTiers)) : [];
    currentItemBatches = i && i.batches ? JSON.parse(JSON.stringify(i.batches)) : [];

    const cats = DB.get('db_categories') || [];
    const uomList = DB.get('db_uom') || [];
    const uomOpts = uomList.length ? uomList.map(u => `<option value="${u.name}">`).join('') : '<option value="Pcs"><option value="Kg"><option value="Ltr"><option value="Box"><option value="Pack"><option value="Bag">';
    const taxCfg  = DB.ls.getObj('db_tax_settings') || {};
    const gstSlabs = Array.isArray(taxCfg.gstSlabs) && taxCfg.gstSlabs.length ? taxCfg.gstSlabs : [0,5,12,18,28];
    const selCatOpts = cats.map(c => `<option value="${c.name}" ${i && i.category === c.name ? 'selected' : ''}>${c.name}</option>`).join('');

    // Pre-determine sub-categories for selected or first category
    let subOpts = '';
    if (i && i.category) {
        const catObj = cats.find(c => c.name === i.category);
        if (catObj && catObj.subCategories) subOpts = catObj.subCategories.map(s => `<option value="${s}" ${i.subCategory === s ? 'selected' : ''}>${s}</option>`).join('');
    }

    openModal(i ? 'Edit Item' : 'Add Item', `
        <div style="margin-bottom:14px;display:flex;align-items:center;gap:14px">
            <div id="item-photo-preview" style="width:70px;height:70px;border-radius:10px;border:2px dashed var(--border);display:flex;align-items:center;justify-content:center;overflow:hidden;cursor:pointer;flex-shrink:0;background:var(--bg-body)" onclick="document.getElementById('f-item-photo').click()">
                ${i && (i.imageUrl || i.photo) ? `<img src="${i.imageUrl || i.photo}" style="width:100%;height:100%;object-fit:cover">` : '<span style="font-size:1.5rem">📷</span>'}
            </div>
            <div style="flex:1">
                <div style="font-size:0.82rem;color:var(--text-muted);margin-bottom:4px">Item Photo (optional)</div>
                <input type="file" id="f-item-photo" accept="image/*" style="display:none" onchange="previewItemPhoto(event)">
                <button class="btn btn-outline btn-sm" onclick="document.getElementById('f-item-photo').click()" style="font-size:0.78rem">📷 Upload Photo</button>
                ${i && (i.imageUrl || i.photo) ? ' <button class="btn btn-outline btn-sm" onclick="removeItemPhoto()" style="font-size:0.78rem">✕ Remove</button>' : ''}
            </div>
            <input type="hidden" id="f-item-existing-url" value="${i && i.imageUrl ? i.imageUrl : (i && i.photo ? i.photo : '')}">
        </div>
        <div class="form-row">
            <div class="form-group"><label>Item Code</label><input id="f-item-code" value="${i ? i.itemCode || '' : ''}" placeholder="SKU/Barcode"></div>
            <div class="form-group"><label>Item Name *</label><input id="f-item-name" value="${i ? i.name : ''}"></div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>Category *</label><select id="f-item-cat" onchange="onCatChangeItemModal()"><option value="">Select Category</option>${selCatOpts}</select></div>
            <div class="form-group"><label>Sub-Category *</label><select id="f-item-subcat"><option value="">Select Sub-Category</option>${subOpts}</select></div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>HSN Code</label><input id="f-item-hsn" value="${i ? i.hsn || '' : ''}" placeholder="e.g. 10063020"></div>
            <div class="form-group">
                <label>GST Rate %</label>
                <select id="f-item-gstrate">
                    ${gstSlabs.map(r=>`<option value="${r}" ${(i ? +(i.gstRate||0) : 0)===r?'selected':''}>${r}%</option>`).join('')}
                </select>
            </div>
            <div class="form-group"><label>Primary Unit</label>
                <input id="f-item-unit" list="uom-options" value="${i ? i.unit || 'Pcs' : 'Pcs'}" placeholder="e.g. Pcs, Box, Kg...">
                <datalist id="uom-options">
                    ${uomOpts}
                </datalist>
            </div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>Secondary UOM (Opt)</label><input id="f-item-secuom" list="uom-options" value="${i ? i.secUom || '' : ''}" placeholder="e.g. Box"></div>
            <div class="form-group"><label>Conversion Ratio</label><input type="number" id="f-item-secratio" value="${i ? i.secUomRatio || '' : ''}" placeholder="1 Pri = ? Sec"></div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>Purchase Price</label><input type="number" id="f-item-pp" value="${i ? i.purchasePrice : 0}"></div>
            <div class="form-group"><label>Standard Sale Price</label><input type="number" id="f-item-sp" value="${i ? i.salePrice : 0}"></div>
            <div class="form-group"><label>MRP</label><input type="number" id="f-item-mrp" value="${i ? i.mrp || '' : ''}" placeholder="Max Retail Price"></div>
        </div>
        
        <div style="background:var(--bg-body);padding:10px;border-radius:6px;border:1px solid var(--border);margin-bottom:12px;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span style="margin:0;font-weight:600;font-size:0.85rem">Volume Pricing (Opt)</span>
                <button class="btn btn-outline btn-sm" onclick="addPriceTier()" style="padding:4px 8px;font-size:0.75rem">+ Add Tier</button>
            </div>
            <div id="price-tiers-container"></div>
        </div>

        <div class="form-row">
            <div class="form-group"><label>Opening Stock</label><input type="number" id="f-item-stock" value="${i ? i.stock : 0}"></div>
            <div class="form-group"><label>Warehouse</label>
                <select id="f-item-warehouse">
                    <option value="Main Warehouse" ${i && i.warehouse === 'Main Warehouse' ? 'selected' : ''}>Main Warehouse</option>
                    <option value="Store" ${i && i.warehouse === 'Store' ? 'selected' : ''}>Store</option>
                    <option value="Van Stock" ${i && i.warehouse === 'Van Stock' ? 'selected' : ''}>Van Stock</option>
                </select>
            </div>
            <div class="form-group"><label>Low Stock Alert</label><input type="number" id="f-item-low" value="${i ? i.lowStockAlert || 5 : 5}"></div>
        </div>
        <div style="background:var(--bg-body);padding:10px;border-radius:6px;border:1px solid var(--border);margin-bottom:12px;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span style="font-weight:600;font-size:0.85rem">📦 MRP / Batch Stock</span>
                <button class="btn btn-outline btn-sm" onclick="openAddBatchForm()" style="padding:4px 8px;font-size:0.75rem">+ Add MRP Batch</button>
            </div>
            <div id="item-batches-container"></div>
        </div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveItem('')">＋ Save & New</button>` : ''}
        <button class="btn btn-primary" onclick="saveItem('${id || ''}')">Save Item</button></div>`);

    renderPriceTiers();
    renderItemBatches();
}

function previewItemPhoto(event) {
    const file = event.target.files[0];
    if (!file) return;
    window._itemPhotoFile = file;
    const preview = $('item-photo-preview');
    if (preview) preview.innerHTML = `<img src="${URL.createObjectURL(file)}" style="width:100%;height:100%;object-fit:cover">`;
    // Clear existing URL since a new file will replace it
    const existing = $('f-item-existing-url');
    if (existing) existing.value = '';
}
function removeItemPhoto() {
    window._itemPhotoFile = null;
    const preview = $('item-photo-preview');
    if (preview) preview.innerHTML = '<span style="font-size:1.5rem">📷</span>';
    const existing = $('f-item-existing-url');
    if (existing) existing.value = '__remove__';
}

function onCatChangeItemModal() {
    const catName = $('f-item-cat').value;
    const subSel = $('f-item-subcat');
    if (!subSel) return;
    subSel.innerHTML = '<option value="">Select Sub-Category</option>';
    if (catName) {
        const catObj = DB.get('db_categories').find(c => c.name === catName);
        if (catObj && catObj.subCategories) {
            subSel.innerHTML += catObj.subCategories.map(s => `<option value="${s}">${s}</option>`).join('');
        }
    }
}

function addPriceTier() {
    currentItemTiers.push({ minQty: 10, price: 0 });
    renderPriceTiers();
}
function removePriceTier(idx) {
    currentItemTiers.splice(idx, 1);
    renderPriceTiers();
}
function updatePriceTier(idx, field, value) {
    currentItemTiers[idx][field] = +value;
}
function renderPriceTiers() {
    const el = $('price-tiers-container');
    if (!el) return;
    if (!currentItemTiers.length) {
        el.innerHTML = '<div style="font-size:0.8rem;color:var(--text-muted)">No volume pricing added. Standard Sale Price will be used.</div>';
        return;
    }
    el.innerHTML = currentItemTiers.map((t, i) => `
        <div style="display:flex;gap:10px;align-items:center;margin-bottom:6px;">
            <div><span style="font-size:0.8rem">Min Qty:</span> <input type="number" style="width:70px;padding:4px;font-size:0.9rem;border-radius:4px;border:1px solid var(--border)" value="${t.minQty}" onchange="updatePriceTier(${i}, 'minQty', this.value)"></div>
            <div><span style="font-size:0.8rem">Price ₹:</span> <input type="number" style="width:90px;padding:4px;font-size:0.9rem;border-radius:4px;border:1px solid var(--border)" value="${t.price}" onchange="updatePriceTier(${i}, 'price', this.value)"></div>
            <button class="btn-icon" style="color:var(--danger);margin-top:14px" onclick="removePriceTier(${i})">✕</button>
        </div>
    `).join('');
}

function renderItemBatches() {
    const el = $('item-batches-container');
    if (!el) return;
    if (!currentItemBatches.length) {
        el.innerHTML = '<div style="font-size:0.8rem;color:var(--text-muted)">No MRP batches. Prices above are item defaults.</div>';
        return;
    }
    el.innerHTML = `<table style="width:100%;font-size:0.82rem;border-collapse:collapse">
        <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="padding:4px 6px;text-align:left">MRP</th>
            <th style="padding:4px 6px;text-align:left">Purchase ₹</th>
            <th style="padding:4px 6px;text-align:left">Sale ₹</th>
            <th style="padding:4px 6px;text-align:center">Avail Qty</th>
            <th style="padding:4px 6px;text-align:left">Date</th>
            <th style="padding:4px 6px;text-align:center">Status</th>
            <th style="padding:4px 6px;text-align:center">Actions</th>
        </tr></thead>
        <tbody>${currentItemBatches.map((b,idx) => `
            <tr style="border-bottom:1px solid var(--border);opacity:${b.isActive===false?0.5:1}">
                <td style="padding:5px 6px;font-weight:600">₹${b.mrp}</td>
                <td style="padding:5px 6px">₹${b.purchasePrice}</td>
                <td style="padding:5px 6px">₹${b.salePrice}</td>
                <td style="padding:5px 6px;text-align:center">${b.qty||0}</td>
                <td style="padding:5px 6px;color:var(--text-muted)">${b.receivedDate||'-'}</td>
                <td style="padding:5px 6px;text-align:center">
                    <span class="badge ${b.isActive===false?'badge-danger':'badge-success'}">${b.isActive===false?'Inactive':'Active'}</span>
                </td>
                <td style="padding:5px 6px;text-align:center">
                    <button class="btn btn-outline btn-sm" onclick="toggleItemBatchActive(${idx})" style="padding:2px 6px;font-size:0.75rem">${b.isActive===false?'Activate':'Deactivate'}</button>
                    <button class="btn-icon" onclick="deleteItemBatch(${idx})" style="color:var(--danger);margin-left:4px">✕</button>
                </td>
            </tr>`).join('')}
        </tbody></table>`;
}

function toggleItemBatchActive(idx) {
    currentItemBatches[idx].isActive = currentItemBatches[idx].isActive === false ? true : false;
    // Resync item prices after changing active status
    const sync = syncItemPricesFromBatches(currentItemBatches);
    if (sync.mrp)           { const el = $('f-item-mrp');  if (el) el.value = sync.mrp; }
    if (sync.salePrice)     { const el = $('f-item-sp');   if (el) el.value = sync.salePrice; }
    if (sync.purchasePrice) { const el = $('f-item-pp');   if (el) el.value = sync.purchasePrice; }
    renderItemBatches();
}

function deleteItemBatch(idx) {
    if (!confirm('Remove this batch?')) return;
    currentItemBatches.splice(idx, 1);
    // Resync item prices after deleting a batch
    const sync = syncItemPricesFromBatches(currentItemBatches);
    if (sync.mrp)           { const el = $('f-item-mrp');  if (el) el.value = sync.mrp; }
    if (sync.salePrice)     { const el = $('f-item-sp');   if (el) el.value = sync.salePrice; }
    if (sync.purchasePrice) { const el = $('f-item-pp');   if (el) el.value = sync.purchasePrice; }
    renderItemBatches();
}

function openAddBatchForm() {
    const today = new Date().toISOString().substring(0,10);
    openModal('Add MRP Batch', `
        <div style="background:rgba(249,115,22,0.08);border:1px solid rgba(249,115,22,0.3);border-radius:8px;padding:10px;margin-bottom:14px;font-size:0.83rem">
            ℹ️ When a new MRP is received, add it as a new batch. Purchase Price and Sale Price are mandatory.
        </div>
        <div class="form-row">
            <div class="form-group"><label>MRP ₹ *</label><input type="number" id="f-batch-mrp" placeholder="Max Retail Price" oninput="onBatchMrpChange()"></div>
            <div class="form-group"><label>Date Received</label><input type="date" id="f-batch-date" value="${today}"></div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>Purchase Price ₹ *</label><input type="number" id="f-batch-pp" placeholder="Your cost price" step="0.01"></div>
            <div class="form-group"><label>Sale Price ₹ *</label><input type="number" id="f-batch-sp" placeholder="Price to customer" step="0.01"></div>
        </div>
        <div class="form-group"><label>Opening Qty (this batch)</label><input type="number" id="f-batch-qty" value="0" min="0"></div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal();openItemModal(window._editItemId||'')">Cancel</button>
            <button class="btn btn-primary" onclick="saveItemBatch()">Add Batch</button>
        </div>`);
}

function onBatchMrpChange() {
    // Optional: could auto-suggest prices based on margin
}

function saveItemBatch() {
    const mrp = +($('f-batch-mrp')||{}).value;
    const pp  = +($('f-batch-pp')||{}).value;
    const sp  = +($('f-batch-sp')||{}).value;
    const qty = +($('f-batch-qty')||{}).value||0;
    const date = ($('f-batch-date')||{}).value||'';
    if (!mrp) return alert('MRP is required');
    if (!pp)  return alert('Purchase Price is mandatory for a new MRP batch');
    if (!sp)  return alert('Sale Price is mandatory for a new MRP batch');
    currentItemBatches.push({ id: 'b_' + Date.now().toString(36), mrp, purchasePrice: pp, salePrice: sp, qty, receivedDate: date, isActive: true });
    // Sync main form fields to new batch
    if ($('f-item-mrp')) $('f-item-mrp').value = mrp;
    if ($('f-item-pp')) $('f-item-pp').value = pp;
    if ($('f-item-sp')) $('f-item-sp').value = sp;
    closeModal();
    // Re-open item modal to show batches (use stored id)
    openItemModal(window._editItemId || '');
}

async function saveItem(id) {
    const name = $('f-item-name').value.trim();
    if (!name) return alert('Item name is required');
    const category = $('f-item-cat').value;
    if (!category) return alert('Category is required');
    const subCategory = $('f-item-subcat').value;
    if (!subCategory) return alert('Sub-Category is required');

    const newStock = +$('f-item-stock').value;

    // Sort price tiers by minQty descending
    currentItemTiers.sort((a, b) => b.minQty - a.minQty);

    const data = {
        name,
        category,
        subCategory,
        itemCode: $('f-item-code').value.trim(),
        secUom: $('f-item-secuom').value.trim(),
        secUomRatio: +$('f-item-secratio').value || 0,
        hsn: $('f-item-hsn').value.trim(),
        gstRate: +($('f-item-gstrate') ? $('f-item-gstrate').value : 0),
        unit: $('f-item-unit').value,
        purchasePrice: +$('f-item-pp').value,
        salePrice: +$('f-item-sp').value,
        mrp: +$('f-item-mrp').value || 0,
        stock: newStock,
        lowStockAlert: +$('f-item-low').value,
        warehouse: $('f-item-warehouse').value || 'Main Warehouse',
        priceTiers: currentItemTiers,
        batches: currentItemBatches,
    };
    // Handle image upload to Supabase Storage
    const existingUrlEl = $('f-item-existing-url');
    const existingUrl = existingUrlEl ? existingUrlEl.value : '';
    if (window._itemPhotoFile) {
        try {
            const ext = window._itemPhotoFile.name.split('.').pop() || 'jpg';
            const fileName = `item_${Date.now()}_${Math.random().toString(36).substr(2,6)}.${ext}`;
            const { data: upData, error: upErr } = await supabaseClient.storage
                .from('item-images')
                .upload(fileName, window._itemPhotoFile, { upsert: true });
            if (upErr) throw upErr;
            const { data: urlData } = supabaseClient.storage.from('item-images').getPublicUrl(fileName);
            data.imageUrl = urlData.publicUrl;
            window._itemPhotoFile = null;
        } catch (uploadErr) {
            console.error('Image upload failed:', uploadErr);
            alert('Image upload failed: ' + uploadErr.message + '\nItem will be saved without photo.');
        }
    } else if (existingUrl === '__remove__') {
        data.imageUrl = null;
    } else if (existingUrl) {
        data.imageUrl = existingUrl;
    }
    // Sync item prices using FIFO: salePrice/MRP from oldest active batch with stock, purchasePrice from newest
    if (currentItemBatches.length) {
        const priceSync = syncItemPricesFromBatches(currentItemBatches);
        Object.assign(data, priceSync);
    }

    try {
        if (id) {
            const items = await DB.getAll('inventory');
            const item = items.find(x => x.id === id);
            if (item) {
                const oldStock = item.stock;
                await DB.update('inventory', id, data);
                if (newStock !== oldStock) {
                    const diff = newStock - oldStock;
                    await addLedgerEntry(id, name, diff > 0 ? 'Positive Adj' : 'Negative Adj', diff, 'EDIT-' + id.substr(0, 6).toUpperCase(), 'Manual edit');
                }
            }
        } else {
            const inserted = await DB.insert('inventory', data);
            if (newStock > 0) {
                await addLedgerEntry(inserted.id, name, 'Opening', newStock, 'OPEN-' + inserted.id.substr(0, 6).toUpperCase(), 'Opening stock');
            }
        }
        closeModal();
        await renderInventory();
        showToast('Item saved successfully', 'success');
        if (window._saveAndNew) { window._saveAndNew = false; openItemModal(); }
    } catch (err) {
        window._saveAndNew = false;
        alert('Error saving item: ' + err.message);
    }
}
// ─── Bulk Management ──────────────────────────────────────────────────────────
window._bulkItems   = new Set();
window._bulkParties = new Set();

function toggleBulkItem(id, chk) {
    if (chk.checked) window._bulkItems.add(id); else window._bulkItems.delete(id);
    updateBulkItemBar();
}
function toggleSelectAllItems(chk) {
    document.querySelectorAll('.bulk-chk-item').forEach(el => {
        el.checked = chk.checked;
        if (chk.checked) window._bulkItems.add(el.dataset.id);
        else window._bulkItems.delete(el.dataset.id);
    });
    updateBulkItemBar();
}
function updateBulkItemBar() {
    const bar = document.getElementById('bulk-bar-inv');
    const cnt = document.getElementById('bulk-cnt-inv');
    if (!bar) return;
    const n = window._bulkItems.size;
    bar.style.display = n > 0 ? 'flex' : 'none';
    if (cnt) cnt.textContent = n + ' selected';
}
function clearBulkItems() {
    window._bulkItems.clear();
    document.querySelectorAll('.bulk-chk-item').forEach(el => el.checked = false);
    const a = document.getElementById('bulk-all-inv'); if (a) a.checked = false;
    updateBulkItemBar();
}
async function bulkActivateItems() {
    if (!window._bulkItems.size) return;
    const inv = await DB.getAll('inventory');
    for (const id of window._bulkItems) {
        const item = inv.find(x => x.id === id);
        if (item) await DB.update('inventory', id, { ...item, active: true });
    }
    showToast(window._bulkItems.size + ' items activated', 'success');
    window._bulkItems.clear();
    renderInventory();
}
async function bulkDeactivateItems() {
    if (!window._bulkItems.size) return;
    const inv = await DB.getAll('inventory');
    for (const id of window._bulkItems) {
        const item = inv.find(x => x.id === id);
        if (item) await DB.update('inventory', id, { ...item, active: false });
    }
    showToast(window._bulkItems.size + ' items deactivated', 'success');
    window._bulkItems.clear();
    renderInventory();
}
async function bulkDeleteItems() {
    if (!window._bulkItems.size) return;
    const [invoices, orders, stockLedger] = await Promise.all([
        DB.getAll('invoices'), DB.getAll('salesorders'), DB.getAll('stock_ledger')
    ]);
    const blocked = [], toDelete = [];
    const matchItem = (li, id) => String(li.itemId || li.item_id || '') === String(id);
    for (const id of window._bulkItems) {
        const hasInv = invoices.some(x => (x.items||[]).some(li => matchItem(li, id)));
        const hasOrd = orders.some(x => (x.items||[]).some(li => matchItem(li, id)));
        const hasLed = stockLedger.some(x => String(x.itemId || x.item_id || '') === String(id));
        if (hasInv || hasOrd || hasLed) blocked.push(id); else toDelete.push(id);
    }
    if (blocked.length) showToast(blocked.length + ' item(s) skipped — have transactions. Use Deactivate instead.', 'error');
    if (!toDelete.length) return;
    if (!confirm('Delete ' + toDelete.length + ' item(s)? Cannot be undone.')) return;
    for (const id of toDelete) await DB.delete('inventory', id);
    showToast(toDelete.length + ' items deleted', 'success');
    window._bulkItems.clear();
    renderInventory();
}

function toggleBulkParty(id, chk) {
    if (chk.checked) window._bulkParties.add(id); else window._bulkParties.delete(id);
    updateBulkPartyBar();
}
function toggleSelectAllParties(chk) {
    document.querySelectorAll('.bulk-chk-party').forEach(el => {
        el.checked = chk.checked;
        if (chk.checked) window._bulkParties.add(el.dataset.id);
        else window._bulkParties.delete(el.dataset.id);
    });
    updateBulkPartyBar();
}
function updateBulkPartyBar() {
    const bar = document.getElementById('bulk-bar-par');
    const cnt = document.getElementById('bulk-cnt-par');
    if (!bar) return;
    const n = window._bulkParties.size;
    bar.style.display = n > 0 ? 'flex' : 'none';
    if (cnt) cnt.textContent = n + ' selected';
}
function clearBulkParties() {
    window._bulkParties.clear();
    document.querySelectorAll('.bulk-chk-party').forEach(el => el.checked = false);
    const a = document.getElementById('bulk-all-par'); if (a) a.checked = false;
    updateBulkPartyBar();
}
async function bulkActivateParties() {
    if (!window._bulkParties.size) return;
    const parties = await DB.getAll('parties');
    for (const id of window._bulkParties) {
        const p = parties.find(x => x.id === id);
        if (p) await DB.update('parties', id, { ...p, active: true, blocked: false });
    }
    showToast(window._bulkParties.size + ' parties activated', 'success');
    window._bulkParties.clear();
    renderParties();
}
async function bulkBlockParties() {
    if (!window._bulkParties.size) return;
    if (!confirm('Block ' + window._bulkParties.size + ' party(ies)? They will be restricted from new transactions.')) return;
    const parties = await DB.getAll('parties');
    for (const id of window._bulkParties) {
        const p = parties.find(x => x.id === id);
        if (p) await DB.update('parties', id, { ...p, active: false, blocked: true });
    }
    showToast(window._bulkParties.size + ' parties blocked 🚫', 'warning');
    window._bulkParties.clear();
    renderParties();
}
async function bulkDeleteParties() {
    if (!window._bulkParties.size) return;
    const [orders, invoices, payments] = await Promise.all([
        DB.getAll('salesorders'), DB.getAll('invoices'), DB.getAll('payments')
    ]);
    const blocked = [], toDelete = [];
    for (const id of window._bulkParties) {
        const sid = String(id);
        const hasTx = orders.some(x => String(x.partyId) === sid) ||
                      invoices.some(x => String(x.partyId) === sid) ||
                      payments.some(x => String(x.partyId) === sid);
        if (hasTx) blocked.push(id); else toDelete.push(id);
    }
    if (blocked.length) showToast(blocked.length + ' party(ies) skipped — have transactions. Use Block instead.', 'error');
    if (!toDelete.length) return;
    if (!confirm('Delete ' + toDelete.length + ' party(ies)? Cannot be undone.')) return;
    for (const id of toDelete) await DB.delete('parties', id);
    showToast(toDelete.length + ' parties deleted', 'success');
    window._bulkParties.clear();
    renderParties();
}

async function deleteItem(id) {
    const [invoices, orders, stockLedger] = await Promise.all([
        DB.getAll('invoices'), DB.getAll('salesorders'), DB.getAll('stock_ledger')
    ]);
    const sid = String(id);
    // JSONB items array uses snake_case item_id; top-level stock_ledger uses camelCase itemId
    const matchItem = li => String(li.itemId || li.item_id || '') === sid;
    const hasInv = invoices.some(x => (x.items||[]).some(matchItem));
    const hasOrd = orders.some(x => (x.items||[]).some(matchItem));
    const hasLed = stockLedger.some(x => String(x.itemId || x.item_id || '') === sid);
    if (hasInv || hasOrd || hasLed) return alert('Cannot delete — this item has transactions. Use Deactivate instead.');
    if (!confirm('Delete item? This cannot be undone.')) return;
    try {
        await DB.delete('inventory', id);
        await renderInventory();
        showToast('Item deleted', 'success');
    } catch (err) {
        alert('Error deleting item: ' + err.message);
    }
}

// --- Stock Adjustment Modal (BC-style Item Journal) ---
async function openStockAdjustmentModal(itemId) {
    const inv = await DB.getAll('inventory');
    const item = itemId ? inv.find(x => x.id === itemId) : null;
    const adjNo = 'ADJ-' + Date.now().toString(36).toUpperCase().substr(-6);
    openModal('Stock Adjustment Journal', `
        <div style="margin-bottom:14px;padding:10px;background:rgba(0,212,170,0.08);border:1px solid rgba(0,212,170,0.2);border-radius:8px;font-size:0.85rem">
            <strong>📋 Document:</strong> ${adjNo} | <strong>Date:</strong> ${today()}
        </div>
        <div class="form-group"><label for="f-adj-item-input">Item *</label>
            <input id="f-adj-item-input" placeholder="Type item name or code..." autocomplete="off" value="${item ? item.name + (item.itemCode ? ' [' + item.itemCode + ']' : '') : ''}">
            <input type="hidden" id="f-adj-item" value="${item ? item.id : ''}">
        </div>
        <div id="adj-current-stock" style="margin-bottom:12px;font-size:0.9rem;color:var(--text-secondary)">
            ${item ? `<strong>Current Stock:</strong> <span class="badge badge-info">${item.stock} ${item.unit || 'Pcs'}</span>` : ''}
        </div>
        <div class="form-row">
            <div class="form-group"><label for="f-adj-mrp">MRP ₹ <small style="color:var(--text-muted)">(from last transaction — edit if new MRP)</small></label>
                <input type="number" id="f-adj-mrp" value="${item ? ((getLastActiveBatch(item)||getLastBatch(item)||{}).mrp||item.mrp||'') : ''}" placeholder="MRP for this lot" step="0.01" oninput="onAdjMrpChange()">
            </div>
        </div>
        <div id="adj-new-mrp-section" style="display:none;background:rgba(249,115,22,0.08);border:1px solid rgba(249,115,22,0.3);border-radius:8px;padding:12px;margin-bottom:12px">
            <div style="font-size:0.83rem;font-weight:600;color:var(--warning);margin-bottom:10px">🆕 New MRP detected — Purchase Price &amp; Sale Price are mandatory</div>
            <div class="form-row">
                <div class="form-group"><label for="f-adj-pp">Purchase Price ₹ *</label><input type="number" id="f-adj-pp" placeholder="Your cost" step="0.01"></div>
                <div class="form-group"><label for="f-adj-sp">Sale Price ₹ *</label><input type="number" id="f-adj-sp" placeholder="Customer price" step="0.01"></div>
            </div>
        </div>
        <div class="form-row">
            <div class="form-group"><label for="f-adj-type">Adjustment Type *</label>
                <select id="f-adj-type">
                    <option value="Positive Adj">➕ Increase (Positive)</option>
                    <option value="Negative Adj">➖ Decrease (Negative)</option>
                </select>
            </div>
            <div class="form-group"><label for="f-adj-qty">Quantity *</label><input type="number" id="f-adj-qty" min="1" value="1"></div>
        </div>
        <div class="form-group"><label for="f-adj-date">Date</label><input type="date" id="f-adj-date" value="${today()}"></div>
        <div class="form-group"><label for="f-adj-reason">Reason *</label>
            <select id="f-adj-reason">
                ${ADJUSTMENT_REASONS.map(r => `<option>${r}</option>`).join('')}
            </select>
        </div>
        <div class="form-group"><label for="f-adj-notes">Notes</label><input id="f-adj-notes" placeholder="Additional details..."></div>
        <input type="hidden" id="f-adj-docno" value="${adjNo}">
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="saveStockAdjustment()">✅ Post Adjustment</button></div>`);

    initSearchDropdown('f-adj-item-input', buildItemSearchList(inv), (selectedItem) => {
        $('f-adj-item').value = selectedItem.id || '';
        onAdjItemChange();
    });
    if (item) onAdjItemChange();
}
function onAdjItemChange() {
    const itemId = ($('f-adj-item')||{}).value;
    const el = $('adj-current-stock');
    if (!itemId) { if (el) el.innerHTML = ''; return; }
    const item = (DB.get('db_inventory') || []).find(x => x.id === itemId);
    if (!item) return;
    if (el) el.innerHTML = `<strong>Current Stock:</strong> <span class="badge badge-info">${item.stock} ${item.unit || 'Pcs'}</span>`;
    const lb = getLastActiveBatch(item) || getLastBatch(item);
    const mrpEl = $('f-adj-mrp');
    if (mrpEl && !mrpEl.value) mrpEl.value = lb ? lb.mrp : (item.mrp || '');
    onAdjMrpChange();
}
function onAdjMrpChange() {
    const itemId = ($('f-adj-item')||{}).value;
    const mrp = +($('f-adj-mrp')||{}).value;
    const section = $('adj-new-mrp-section');
    if (!section || !itemId || !mrp) { if (section) section.style.display = 'none'; return; }
    const item = (DB.get('db_inventory') || []).find(x => x.id === itemId);
    const batches = (item && item.batches) || [];
    // Treat as existing if: batch found OR item has no batches yet and MRP matches item default
    const exists = batches.some(b => +b.mrp === mrp) || (!batches.length && +item.mrp === mrp);
    section.style.display = exists ? 'none' : 'block';
}
async function saveStockAdjustment() {
    const itemId = $('f-adj-item').value;
    if (!itemId) return alert('Select an item');
    const qty = +$('f-adj-qty').value;
    if (!qty || qty <= 0) return alert('Enter a valid quantity');
    const type = $('f-adj-type').value;
    const adjDate = $('f-adj-date').value;
    const reason = $('f-adj-reason').value + ($('f-adj-notes').value.trim() ? ' — ' + $('f-adj-notes').value.trim() : '');
    const docNo = $('f-adj-docno').value;
    const mrp = +($('f-adj-mrp')||{}).value || 0;

    try {
        const items = await DB.getAll('inventory');
        const item = items.find(x => x.id === itemId);
        if (!item) return alert('Item not found');

        const actualQty = type === 'Positive Adj' ? qty : -qty;
        if (item.stock + actualQty < 0) return alert('Cannot reduce below zero. Current stock: ' + item.stock);

        const newStock = item.stock + actualQty;
        const updateData = { stock: newStock };

        // --- Batch / MRP handling ---
        if (mrp) {
            const batches = item.batches ? JSON.parse(JSON.stringify(item.batches)) : [];
            const existingBatch = batches.find(b => +b.mrp === mrp);

            if (existingBatch) {
                // MRP already exists — just update qty on that batch
                existingBatch.qty = (existingBatch.qty || 0) + actualQty;
                if (existingBatch.qty < 0) existingBatch.qty = 0;
            } else if (!batches.length && +item.mrp === mrp) {
                // No batches yet and MRP matches item default — silently create first batch from existing item prices
                batches.push({ id: 'b_' + Date.now().toString(36), mrp, purchasePrice: item.purchasePrice || 0, salePrice: item.salePrice || 0, qty: Math.max(0, newStock), receivedDate: adjDate, isActive: true });
            } else {
                // Genuinely new MRP — purchase price and sale price are mandatory
                const pp = +($('f-adj-pp')||{}).value;
                const sp = +($('f-adj-sp')||{}).value;
                if (!pp) return alert('Purchase Price is mandatory for a new MRP batch');
                if (!sp) return alert('Sale Price is mandatory for a new MRP batch');
                batches.push({
                    id: 'b_' + Date.now().toString(36),
                    mrp, purchasePrice: pp, salePrice: sp,
                    qty: type === 'Positive Adj' ? qty : 0,
                    receivedDate: adjDate, isActive: true
                });
                // Sync item prices via FIFO after adding new batch
                Object.assign(updateData, syncItemPricesFromBatches(batches));
            }
            updateData.batches = batches;
        }

        await DB.update('inventory', itemId, updateData);
        await addLedgerEntry(item.id, item.name, type, actualQty, docNo, reason + (mrp ? ` | MRP ₹${mrp}` : ''));

        closeModal();
        await renderInventory();
        showToast(`Stock adjusted! ${item.name}: ${actualQty > 0 ? '+' : ''}${actualQty} → New stock: ${newStock}${mrp ? ` | MRP ₹${mrp}` : ''}`, 'success');
    } catch (err) {
        alert('Error adjusting stock: ' + err.message);
    }
}

// --- Item Ledger View (BC-style Item Ledger Entries) ---
async function viewItemLedger(itemId) {
    const [items, ledger] = await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('stock_ledger')
    ]);
    const item = items.find(x => x.id === itemId);
    if (!item) return;
    const itemLedger = ledger.filter(e => e.itemId === itemId);
    const rows = itemLedger.slice().reverse();

    function extractMrp(e) {
        if (e.mrp) return '₹' + e.mrp;
        const m = (e.reason || '').match(/MRP\s*₹?\s*(\d+\.?\d*)/i);
        return m ? '₹' + m[1] : '-';
    }
    function cleanReason(reason) {
        return (reason || '').replace(/\s*\|\s*MRP\s*₹?\s*\d+\.?\d*/i, '').trim() || '-';
    }

    openModal(`📜 ${item.name} — Ledger`, `
        <div class="stats-grid-sm" style="margin-bottom:14px">
            <div class="stat-card blue"><div class="stat-icon">📦</div><div class="stat-value">${item.stock}</div><div class="stat-label">Current Stock</div></div>
            <div class="stat-card green"><div class="stat-icon">📋</div><div class="stat-value">${itemLedger.length}</div><div class="stat-label">Total Entries</div></div>
            ${item.mrp ? `<div class="stat-card amber"><div class="stat-icon">🏷️</div><div class="stat-value">₹${item.mrp}</div><div class="stat-label">Current MRP</div></div>` : ''}
            ${item.batches && item.batches.length ? `<div class="stat-card"><div class="stat-icon">🗂️</div><div class="stat-value">${item.batches.filter(b=>b.isActive!==false).length}</div><div class="stat-label">Active Batches</div></div>` : ''}
        </div>
        ${rows.length ? `<div style="overflow-x:auto">
        <table class="data-table" style="min-width:680px"><thead><tr>
            <th>Date</th><th>Type</th><th>Doc #</th><th>MRP</th><th>Qty</th><th>Balance</th><th>Reason</th><th>By</th>
        </tr></thead>
        <tbody>${rows.map(e => `<tr>
            <td style="white-space:nowrap">${fmtDate(e.date)}</td>
            <td><span class="badge ${e.qty > 0 ? 'badge-success' : 'badge-danger'}" style="white-space:nowrap">${e.entryType}</span></td>
            <td style="font-size:0.78rem;color:var(--text-muted);white-space:nowrap">${e.documentNo || '-'}</td>
            <td style="font-weight:600;color:var(--accent);white-space:nowrap">${extractMrp(e)}</td>
            <td style="font-weight:700;color:${e.qty > 0 ? 'var(--success)' : 'var(--danger)'};white-space:nowrap">${e.qty > 0 ? '+' : ''}${e.qty}</td>
            <td style="font-weight:600;white-space:nowrap">${e.runningStock}</td>
            <td style="font-size:0.82rem;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(cleanReason(e.reason))}">${cleanReason(e.reason)}</td>
            <td style="font-size:0.78rem;white-space:nowrap">${e.createdBy || '-'}</td>
        </tr>`).join('')}</tbody></table></div>`
        : '<div class="empty-state" style="padding:30px"><div class="empty-icon">📜</div><p>No ledger entries yet.</p></div>'}`,
        `<button class="btn btn-outline" onclick="closeModal()">Close</button>
        ${canEdit() ? `<button class="btn btn-primary" onclick="closeModal();openStockAdjustmentModal('${itemId}')">🔧 Adjust Stock</button>` : ''}`);
}

// --- CSV Download Helper ---
async function downloadCSV(csvContent, fileName) {
    if (window.showSaveFilePicker) {
        try {
            const handle = await window.showSaveFilePicker({
                suggestedName: fileName,
                types: [{ description: 'CSV File', accept: { 'text/csv': ['.csv'] } }],
            });
            const writable = await handle.createWritable();
            await writable.write(csvContent);
            await writable.close();
            return;
        } catch (err) {
            if (err.name !== 'AbortError') console.error(err);
        }
    }

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    if (window.navigator && window.navigator.msSaveOrOpenBlob) {
        window.navigator.msSaveOrOpenBlob(blob, fileName);
    } else {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = fileName;
        a.style.display = 'none';
        document.body.appendChild(a);
        a.click();

        // Delay cleanup so the browser has time to start the download
        setTimeout(() => {
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }, 100);
    }
}

// --- Excel Export (CSV) ---
async function exportInventoryExcel() {
    const items = await DB.getAll('inventory');
    if (!items.length) return alert('No items to export');
    let csv = 'Item Name,HSN,Unit,Purchase Price,Sale Price,Current Stock,Stock Value,Low Stock Alert\n';
    items.forEach(i => {
        csv += `"${i.name}","${i.hsn || ''}","${i.unit || 'Pcs'}",${i.purchasePrice},${i.salePrice},${i.stock},${(i.stock * i.purchasePrice).toFixed(2)},${i.lowStockAlert || 5}\n`;
    });
    downloadCSV(csv, 'inventory_' + today() + '.csv');
}

async function exportPartiesExcel() {
    const parties = await DB.getAll('parties');
    if (!parties.length) return alert('No parties to export');
    let csv = 'Party Code,Name,Type,Phone,GSTIN,Address,City,Post Code,Location Lat,Location Lng,Balance,Credit Limit,Payment Terms,Blocked\n';
    parties.forEach(p => {
        csv += `"${p.partyCode || ''}","${p.name}","${p.type}","${p.phone || ''}","${p.gstin || ''}","${p.address || ''}","${p.city || ''}","${p.postCode || ''}",${p.lat || ''},${p.lng || ''},${p.balance || 0},${p.creditLimit || 0},"${p.paymentTerms || ''}",${p.blocked || false}\n`;
    });
    downloadCSV(csv, 'parties_' + today() + '.csv');
}

// --- Excel Template Download ---
function downloadStockTemplate() {
    const dt = today();
    let csv = 'Item Name *,Date (YYYY-MM-DD) *,Type (Increase/Decrease) *,Quantity *,Reason,MRP\n';
    csv += `Premium Soap,${dt},Increase,10,Physical Count,40.00\n`;
    csv += `Rice 5Kg,${dt},Decrease,3,Damaged Goods,220.00\n`;
    csv += `Parle-G Biscuit,${dt},Increase,50,New Stock Received,7.00\n`;
    downloadCSV(csv, 'stock_adjustment_template.csv');
    showToast('Stock adjustment template downloaded!', 'success');
}

// --- Excel Import (CSV) ---
function importStockExcel() {
    const input = $('stock-file-input');
    if (input) { input.value = ''; input.click(); }
}
let pendingStockImports = [];
function processStockImport(event) {
    const file = event.target.files[0];
    if (!file) return;

    function parseText(text) {
        const lines = text.split(/\r?\n/).filter(l => l.trim());
        if (lines.length < 2) return alert('File is empty or has no data rows');
        const items = DB.get('db_inventory');
        const errors = [];
        pendingStockImports = [];
        for (let i = 1; i < lines.length; i++) {
            const cols = parseCSVLine(lines[i]);
            if (cols.length < 4) { errors.push(`Row ${i + 1}: Not enough columns`); continue; }
            const [itemName, dateStr, typeStr, qtyStr, reason] = [cols[0].trim(), cols[1].trim(), cols[2].trim(), cols[3].trim(), (cols[4] || '').trim()];
            const item = items.find(x => x.name.toLowerCase() === itemName.toLowerCase());
            if (!item) { errors.push(`Row ${i + 1}: Item "${itemName}" not found`); continue; }
            const qty = parseInt(qtyStr, 10);
            if (isNaN(qty) || qty <= 0) { errors.push(`Row ${i + 1}: Invalid qty "${qtyStr}"`); continue; }
            const isIncrease = typeStr.toLowerCase().startsWith('increase') || typeStr.toLowerCase() === 'positive' || typeStr === '+';
            const actualQty = isIncrease ? qty : -qty;
            if (item.stock + actualQty < 0) { errors.push(`Row ${i + 1}: "${itemName}" stock would go below zero`); continue; }
            const adjDate = dateStr && /^\d{4}-\d{2}-\d{2}$/.test(dateStr) ? dateStr : today();
            pendingStockImports.push({ item, actualQty, adjDate, entryType: isIncrease ? 'Positive Adj' : 'Negative Adj', reason: (reason || 'Excel Import') + ' (imported)' });
        }
        let html = '';
        if (errors.length) html += `<div style="margin-bottom:14px;padding:12px;background:var(--danger-soft);border:1px solid rgba(239,68,68,0.3);border-radius:8px;font-size:0.85rem"><strong style="color:var(--danger)">⚠️ ${errors.length} Errors Found (Rows skipped)</strong><ul style="margin-top:6px;padding-left:14px;color:var(--danger);max-height:100px;overflow-y:auto">${errors.map(err => `<li>${err}</li>`).join('')}</ul></div>`;
        html += `<div style="margin-bottom:10px;font-weight:600">✅ ${pendingStockImports.length} Valid Adjustments Preview</div>`;
        if (pendingStockImports.length) html += `<div style="max-height:300px;overflow-y:auto;border:1px solid var(--border);border-radius:var(--radius-sm)"><table class="data-table"><thead><tr><th>Date</th><th>Item</th><th>Type</th><th>Qty</th><th>New Stock</th></tr></thead><tbody>${pendingStockImports.map(p => `<tr><td>${fmtDate(p.adjDate)}</td><td>${p.item.name}</td><td><span class="badge ${p.actualQty > 0 ? 'badge-success' : 'badge-danger'}">${p.entryType}</span></td><td style="font-weight:700;color:${p.actualQty > 0 ? 'var(--success)' : 'var(--danger)'}">${p.actualQty > 0 ? '+' : ''}${p.actualQty}</td><td>${p.item.stock + p.actualQty}</td></tr>`).join('')}</tbody></table></div>`;
        html += `<div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="commitStockImport()" ${pendingStockImports.length === 0 ? 'disabled' : ''}>💾 Confirm & Apply Adjustments</button></div>`;
        openModal('Import Excel Preview', html);
        event.target.value = '';
    }

    if (file.name.match(/\.xlsx?$/i) && typeof XLSX !== 'undefined') {
        const reader = new FileReader();
        reader.onload = e => { const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' }); parseText(XLSX.utils.sheet_to_csv(wb.Sheets[wb.SheetNames[0]])); };
        reader.readAsArrayBuffer(file);
    } else {
        const reader = new FileReader();
        reader.onload = e => parseText(e.target.result);
        reader.readAsText(file);
    }
}

async function commitStockImport() {
    let count = 0;

    try {
        const inventory = await DB.getAll('inventory');
        for (const p of pendingStockImports) {
            const item = inventory.find(x => x.id === p.item.id);
            if (item) {
                const newStock = item.stock + p.actualQty;
                await DB.update('inventory', item.id, { stock: newStock });
                
                // Update local item object for next iteration if same item exists in import
                item.stock = newStock;

                // Add ledger entry
                await addLedgerEntry(item.id, item.name, p.entryType, p.actualQty, 'IMPORT-' + Date.now().toString(36).toUpperCase().substr(-6), p.reason);
                count++;
            }
        }

        pendingStockImports = [];
        closeModal();
        await renderInventory();
        showToast(`Import complete! ${count} adjustments applied.`, 'success');
    } catch (err) {
        alert('Error during stock import: ' + err.message);
    }
}
function parseCSVLine(line) {
    const result = []; let current = ''; let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"') { if (i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; } else { inQuotes = false; } }
            else { current += ch; }
        } else {
            if (ch === '"') { inQuotes = true; }
            else if (ch === ',') { result.push(current); current = ''; }
            else { current += ch; }
        }
    }
    result.push(current);
    return result;
}

// --- Excel Item Master Import ---
function downloadItemTemplate() {
    let csv = 'Item Code,Item Name *,Category *,Sub-Category,HSN,Primary Unit *,Secondary UOM,Conversion Ratio,Purchase Price *,Sale Price *,MRP,Opening Stock,Low Stock Alert,Warehouse,Price Tier 1 Qty,Price Tier 1 Price,Price Tier 2 Qty,Price Tier 2 Price\n';
    csv += 'SKU-001,Premium Soap,FMCG,Personal Care,3401,Pcs,Box,12,25.00,35.00,40.00,100,20,Main Warehouse,50,33.00,100,30.00\n';
    csv += 'SKU-002,Rice 5Kg,Grocery,Staples,1006,Bag,,,160.00,200.00,220.00,50,10,Main Warehouse,,,\n';
    csv += 'SKU-003,Parle-G Biscuit,Biscuits,Glucose,,Pcs,Box,24,5.00,6.00,7.00,200,50,Main Warehouse,100,5.50,,\n';
    downloadCSV(csv, 'item_master_template.csv');
    showToast('Item template downloaded!', 'success');
}

function importItemExcel() {
    const input = $('item-file-input');
    if (input) { input.value = ''; input.click(); }
}

let pendingItemImports = [];
function processItemImport(event) {
    const file = event.target.files[0];
    if (!file) return;

    function parseText(text) {
        const lines = text.split(/\r?\n/).filter(l => l.trim());
        if (lines.length < 2) return alert('File is empty or has no data rows');
        const errors = [];
        pendingItemImports = [];
        const items = DB.get('db_inventory');
        const categories = DB.get('db_categories');
        for (let i = 1; i < lines.length; i++) {
            const cols = parseCSVLine(lines[i]);
            if (cols.length < 3) { errors.push(`Row ${i + 1}: Not enough columns`); continue; }
            const [code, name, cat, subcat, hsn, priUnit, secUOM, convStr, ppStr, spStr, mrpStr, stockStr, lowStr, warehouse] = cols.map(c => (c || '').trim());
            const existingItem = items.find(it => it.name.toLowerCase() === name.toLowerCase());
            if (pendingItemImports.some(it => it.name.toLowerCase() === name.toLowerCase())) { errors.push(`Row ${i + 1}: Duplicate item "${name}" in file.`); continue; }
            if (!cat) { errors.push(`Row ${i + 1}: Category required for "${name}"`); continue; }
            if (!subcat) { errors.push(`Row ${i + 1}: Sub-category required for "${name}"`); continue; }
            let catObj = categories.find(c => c.name.toLowerCase() === cat.toLowerCase());
            let createdCat = !catObj || !catObj.subCategories.find(s => s.toLowerCase() === subcat.toLowerCase());
            const purchasePrice = parseFloat(ppStr) || 0;
            const salePrice = parseFloat(spStr) || 0;
            const stock = parseInt(stockStr, 10) || 0;
            const lowAlert = parseInt(lowStr, 10) || 5;
            const ratio = parseFloat(convStr) || 0;
            const entry = {
                itemCode: code || (existingItem ? existingItem.itemCode : ''),
                name, category: cat, subCategory: subcat,
                hsn: hsn || (existingItem ? existingItem.hsn : ''),
                unit: priUnit || (existingItem ? existingItem.unit : 'Pcs'),
                secUom: secUOM || (existingItem ? existingItem.secUom : ''),
                secUomRatio: ratio > 0 ? ratio : (existingItem ? existingItem.secUomRatio : 0),
                purchasePrice: purchasePrice > 0 ? purchasePrice : (existingItem ? existingItem.purchasePrice : 0),
                salePrice: salePrice > 0 ? salePrice : (existingItem ? existingItem.salePrice : 0),
                mrp: mrpStr ? parseFloat(mrpStr) : (existingItem ? existingItem.mrp : 0),
                stock: existingItem ? existingItem.stock : stock,
                lowStockAlert: lowAlert > 0 ? lowAlert : (existingItem ? existingItem.lowStockAlert : 5),
                warehouse: warehouse || (existingItem ? existingItem.warehouse : 'Main Warehouse'),
                _catNeedsCreation: createdCat, isUpdate: !!existingItem
            };
            if (existingItem) entry.id = existingItem.id;
            pendingItemImports.push(entry);
        }
        let html = '';
        if (errors.length) html += `<div style="margin-bottom:14px;padding:12px;background:var(--danger-soft);border:1px solid rgba(239,68,68,0.3);border-radius:8px;font-size:0.85rem"><strong style="color:var(--danger)">⚠️ ${errors.length} Errors (Rows skipped)</strong><ul style="margin-top:6px;padding-left:14px;color:var(--danger);max-height:100px;overflow-y:auto">${errors.map(e => `<li>${e}</li>`).join('')}</ul></div>`;
        const newCount = pendingItemImports.filter(p => !p.isUpdate).length;
        const updCount = pendingItemImports.filter(p => p.isUpdate).length;
        html += `<div style="margin-bottom:10px;font-weight:600">✅ ${pendingItemImports.length} Valid Items <span style="font-size:0.8rem;color:var(--text-muted)">(${newCount} New, ${updCount} Update)</span></div>`;
        const newCats = pendingItemImports.filter(p => p._catNeedsCreation).length;
        if (newCats > 0) html += `<div style="margin-bottom:10px;font-size:0.85rem;color:var(--warning)">Note: ${newCats} items have missing Category/Sub-Category — will be created automatically.</div>`;
        if (pendingItemImports.length) html += `<div style="max-height:300px;overflow-y:auto;border:1px solid var(--border);border-radius:var(--radius-sm)"><table class="data-table"><thead><tr><th>Name</th><th>Cat / Sub</th><th>Sale ₹</th><th>Action</th></tr></thead><tbody>${pendingItemImports.map(p => `<tr><td>${p.name}</td><td>${p.category} > ${p.subCategory}</td><td>${currency(p.salePrice)}</td><td><span class="badge ${p.isUpdate ? 'badge-warning' : 'badge-success'}">${p.isUpdate ? 'Update' : 'New'}</span></td></tr>`).join('')}</tbody></table></div>`;
        html += `<div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="commitItemImport()" ${pendingItemImports.length === 0 ? 'disabled' : ''}>💾 Confirm & Import Items</button></div>`;
        openModal('Import Items Preview', html);
        event.target.value = '';
    }

    if (file.name.match(/\.xlsx?$/i) && typeof XLSX !== 'undefined') {
        const reader = new FileReader();
        reader.onload = e => { const wb = XLSX.read(new Uint8Array(e.target.result), { type: 'array' }); parseText(XLSX.utils.sheet_to_csv(wb.Sheets[wb.SheetNames[0]])); };
        reader.readAsArrayBuffer(file);
    } else {
        const reader = new FileReader();
        reader.onload = e => parseText(e.target.result);
        reader.readAsText(file);
    }
}

async function commitItemImport() {
    let added = 0, updated = 0;

    try {
        const [categories, inventory] = await Promise.all([
            DB.getAll('categories'),
            DB.getAll('inventory')
        ]);

        for (const p of pendingItemImports) {
            // Ensure category and subcategory exist
            let catObj = categories.find(c => c.name.toLowerCase() === p.category.toLowerCase());
            if (!catObj) {
                catObj = await DB.insert('categories', { name: p.category, subCategories: [p.subCategory] });
                categories.push(catObj);
            } else if (!catObj.subCategories.find(s => s.toLowerCase() === p.subCategory.toLowerCase())) {
                catObj.subCategories.push(p.subCategory);
                await DB.update('categories', catObj.id, { subCategories: catObj.subCategories });
            }

            const isUpdate = p.isUpdate;
            const itemId = p.id;
            delete p._catNeedsCreation;
            delete p.isUpdate;
            delete p.id; 

            if (isUpdate) {
                const existing = inventory.find(it => it.id === itemId);
                const dataToUpdate = { ...p };
                if (existing) {
                    dataToUpdate.stock = existing.stock;
                    dataToUpdate.priceTiers = existing.priceTiers || [];
                }
                await DB.update('inventory', itemId, dataToUpdate);
                updated++;
            } else {
                const inserted = await DB.insert('inventory', { ...p, priceTiers: [] });
                added++;
                if (p.stock > 0) {
                    // Update the inserted object's stock state for the ledger entry
                    inserted.stock = p.stock; 
                    await addLedgerEntry(inserted.id, p.name, 'Opening', p.stock, 'OPEN-IMP', 'Bulk Excel Import');
                }
            }
        }

        pendingItemImports = [];
        closeModal();
        await renderInventory();
        showToast(`Import complete! ${added} added, ${updated} updated.`, 'success');
    } catch (err) {
        alert('Error during item import: ' + err.message);
    }
}

// =============================================
//  SALES ORDERS (Approval — no invoice on approve)
// =============================================
let soItems = [];
async function renderSalesOrders() {
    const orders = await DB.getAll('salesorders');
    const soStatusRank = o => {
        if (o.status === 'pending')   return 0;
        if (o.status === 'approved' && !o.packed && !o.invoiceNo) return 1;
        if (o.packed && !o.invoiceNo) return 2;
        if (o.invoiceNo)              return 3;
        if (o.status === 'rejected')  return 4;
        if (o.status === 'cancelled') return 5;
        return 6;
    };
    orders.sort((a, b) => soStatusRank(a) - soStatusRank(b) || (b.date || '').localeCompare(a.date || ''));
    const isApprover = currentUser.role === 'Admin' || currentUser.role === 'Manager';
    const p = orders.filter(o => o.status === 'pending'), a = orders.filter(o => o.status === 'approved'), r = orders.filter(o => o.status === 'rejected');
    pageContent.innerHTML = `
        <div class="stats-grid" style="margin-bottom:18px">
            <div class="stat-card amber"><div class="stat-icon">⏳</div><div class="stat-value">${p.length}</div><div class="stat-label">Pending</div></div>
            <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${a.length}</div><div class="stat-label">Approved</div></div>
            <div class="stat-card red"><div class="stat-icon">❌</div><div class="stat-value">${r.length}</div><div class="stat-label">Rejected</div></div>
        </div>
        <div class="section-toolbar">
            <div class="filter-group"><select id="so-status-filter" onchange="filterSOTable()"><option value="">All</option><option value="pending">Pending</option><option value="approved">Approved</option><option value="rejected">Rejected</option></select>
            <select id="so-priority-filter" onchange="filterSOTable()"><option value="">All Priority</option><option value="Urgent">Urgent</option><option value="Normal">Normal</option></select>
            <select id="so-sort" onchange="filterSOTable()"><option value="date-desc">Date ↓</option><option value="date-asc">Date ↑</option><option value="delivery-asc">Delivery ↑</option><option value="delivery-desc">Delivery ↓</option></select>
            <input class="search-box" id="so-search" placeholder="Search..." oninput="filterSOTable()" style="width:200px">
            <button class="btn btn-outline" onclick="openColumnPersonalizer('salesorders','renderSalesOrders')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button></div>
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                ${isApprover && p.length > 0 ? `<button class="btn btn-success" id="btn-bulk-approve" onclick="bulkApproveOrders()" style="display:none">✅ Approve Selected (<span id="bulk-approve-count">0</span>)</button>` : ''}
                <button class="btn btn-primary" onclick="openSalesOrderModal()">+ New Sales Order</button>
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>${isApprover ? `<th style="width:36px"><input type="checkbox" id="so-select-all" title="Select all pending" onchange="soToggleAll(this.checked)"></th>` : '<th style="width:36px"></th>'}${ColumnManager.get('salesorders').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="so-tbody">${renderSORows(orders, isApprover)}</tbody></table>
            </div>
        </div></div>`;
}
function getOrderDisplayStatusSync(o) {
    if (o.status !== 'approved') return { text: o.status, class: o.status === 'rejected' ? 'badge-danger' : 'badge-warning' };
    if (!o.packed && !o.invoiceNo) return { text: 'approved', class: 'badge-success' };
    const dels = (DB.cache['delivery'] || []).filter(d => d.orderNo === o.orderNo);
    let deliveryStatus = null;
    if (dels.length > 0) {
        const activeDel = dels.find(d => d.status !== 'Cancelled') || dels[dels.length - 1];
        deliveryStatus = activeDel.status;
    }
    if (o.invoiceNo) {
        const inv = (DB.cache['invoices'] || []).find(i => i.invoiceNo === o.invoiceNo);
        if (inv && inv.status === 'cancelled') return { text: 'returned/cancelled', class: 'badge-danger' };
    }
    if (deliveryStatus === 'Delivered') return { text: 'Delivered', class: 'badge-success' };
    if (deliveryStatus === 'Dispatched') return { text: 'Dispatched', class: 'badge-info' };
    if (deliveryStatus === 'Undelivered') return { text: 'Undelivered', class: 'badge-warning' };
    if (deliveryStatus === 'Returned') return { text: 'Returned', class: 'badge-danger' };
    if (o.invoiceNo) return { text: 'Invoiced', class: 'badge-success' };
    if (o.packed) return { text: 'Packed', class: 'badge-success' };
    return { text: 'approved', class: 'badge-success' };
}
async function getOrderDisplayStatus(o) {
    if (o.status !== 'approved') return { text: o.status, class: o.status === 'rejected' ? 'badge-danger' : 'badge-warning' };

    // It's approved. Let's trace it downstream.
    if (!o.packed && !o.invoiceNo) return { text: 'approved', class: 'badge-success' };

    // Check delivery records
    let deliveryStatus = null;
    if (o.invoiceNo) {
        const dels = (await DB.getAll('delivery')).filter(d => d.orderNo === o.orderNo);
        if (dels.length > 0) {
            const activeDel = dels.find(d => d.status !== 'Cancelled') || dels[dels.length - 1];
            deliveryStatus = activeDel.status;
        }
    }

    // Check if the associated invoice was cancelled completely
    if (o.invoiceNo) {
        const invoices = await DB.getAll('invoices');
        const inv = invoices.find(i => i.invoiceNo === o.invoiceNo);
        if (inv && inv.status === 'cancelled') {
            return { text: 'returned/cancelled', class: 'badge-danger' };
        }
    }

    if (deliveryStatus === 'Delivered') return { text: 'Delivered', class: 'badge-success' };
    if (deliveryStatus === 'Dispatched') return { text: 'Dispatched', class: 'badge-info' };
    if (deliveryStatus === 'Undelivered') return { text: 'Undelivered', class: 'badge-warning' };
    if (deliveryStatus === 'Returned') return { text: 'Returned', class: 'badge-danger' };

    if (o.invoiceNo) return { text: 'Invoiced', class: 'badge-success' };
    if (o.packed) return { text: 'Packed', class: 'badge-success' };

    return { text: 'approved', class: 'badge-success' };
}

function renderSORows(orders, isApprover) {
    if (!orders.length) return '<tr><td colspan="10" class="empty-state"><p>No orders found</p></td></tr>';
    const cols = ColumnManager.get('salesorders').filter(c => c.visible);
    return orders.map(o => {
        const disp = getOrderDisplayStatusSync(o);
        const isUrgent = o.isUrgent;
        const delDate = o.expectedDeliveryDate ? `<span style="font-size:0.8rem;color:${new Date(o.expectedDeliveryDate) < new Date() && o.status !== 'delivered' ? 'var(--danger)' : 'var(--text-muted)'}">${fmtDate(o.expectedDeliveryDate)}</span>` : '-';
        const chkTd = isApprover ? `<td><input type="checkbox" class="so-select-chk" data-id="${o.id}" onchange="soUpdateBulkBtn()" style="width:16px;height:16px"></td>` : '<td></td>';
        const cellMap = {
            date:     `<td>${fmtDate(o.date)}</td>`,
            orderNo:  `<td style="font-weight:600">${o.orderNo}${isUrgent ? ' <span class="badge badge-danger" style="font-size:0.6rem">🔥</span>' : ''}</td>`,
            party:    `<td>${escapeHtml(o.partyName)}</td>`,
            delivery: `<td>${delDate}</td>`,
            items:    `<td>${o.items.length}</td>`,
            total:    `<td class="amount-green">${currency(o.total)}</td>`,
            by:       `<td style="font-size:0.82rem">${o.createdBy || '-'}</td>`,
            status:   `<td><span class="badge ${disp.class}" style="text-transform:capitalize">${disp.text}</span></td>`,
            actions:  `<td><div class="action-btns">
                <button class="btn-icon" onclick="viewSalesOrder('${o.id}')">👁️</button>
                <button class="btn-icon" onclick="duplicateSalesOrder('${o.id}')" title="Duplicate">📋</button>
                ${o.status === 'pending' && isApprover ? `<button class="btn-icon" style="color:var(--success)" onclick="approveSalesOrder('${o.id}')">✅</button><button class="btn-icon" style="color:var(--danger)" onclick="rejectSalesOrder('${o.id}')">❌</button>` : ''}
                ${o.status === 'pending' && canEdit() ? `<button class="btn-icon" onclick="deleteSalesOrder('${o.id}')">🗑️</button>` : ''}
            </div></td>`,
        };
        return `<tr${isUrgent ? ' style="background:rgba(239,68,68,0.06);border-left:3px solid var(--danger)"' : ''}>${chkTd}${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
function soToggleAll(checked) {
    document.querySelectorAll('.so-select-chk').forEach(c => { c.checked = checked; });
    soUpdateBulkBtn();
}
function soUpdateBulkBtn() {
    const checked = document.querySelectorAll('.so-select-chk:checked');
    const btn = $('btn-bulk-approve');
    const cnt = $('bulk-approve-count');
    if (btn) { btn.style.display = checked.length > 0 ? '' : 'none'; }
    if (cnt) cnt.textContent = checked.length;
    // Sync select-all checkbox state
    const all = document.querySelectorAll('.so-select-chk');
    const selAll = $('so-select-all');
    if (selAll && all.length > 0) selAll.checked = all.length === checked.length;
}
async function bulkApproveOrders() {
    const checked = [...document.querySelectorAll('.so-select-chk:checked')];
    if (!checked.length) return;
    if (!confirm(`Approve ${checked.length} selected order(s)?`)) return;
    const ids = checked.map(c => c.dataset.id);
    await Promise.all(ids.map(id => DB.update('salesorders', id, { status: 'approved' })));
    showToast(`${ids.length} order(s) approved!`, 'success');
    renderSalesOrders();
}
async function filterSOTable() {
    const s = $('so-search').value.toLowerCase(), st = $('so-status-filter').value;
    const pf = $('so-priority-filter') ? $('so-priority-filter').value : '';
    const sort = $('so-sort') ? $('so-sort').value : 'date-desc';
    let orders = await DB.getAll('salesorders');
    if (s) orders = orders.filter(o => o.orderNo.toLowerCase().includes(s) || o.partyName.toLowerCase().includes(s));
    if (st) orders = orders.filter(o => o.status === st);
    if (pf) orders = orders.filter(o => (o.priority || 'Normal') === pf);
    // Sorting
    const statusRank = o => {
        if (o.status === 'pending')   return 0;
        if (o.status === 'approved' && !o.packed && !o.invoiceNo) return 1;
        if (o.packed && !o.invoiceNo) return 2;
        if (o.invoiceNo)              return 3;
        if (o.status === 'rejected')  return 4;
        if (o.status === 'cancelled') return 5;
        return 6;
    };
    orders.sort((a, b) => {
        const rankDiff = statusRank(a) - statusRank(b);
        if (rankDiff !== 0) return rankDiff;
        if (sort === 'date-asc') return (a.date || '').localeCompare(b.date || '');
        if (sort === 'date-desc') return (b.date || '').localeCompare(a.date || '');
        if (sort === 'delivery-asc') return (a.expectedDeliveryDate || '9999').localeCompare(b.expectedDeliveryDate || '9999');
        if (sort === 'delivery-desc') return (b.expectedDeliveryDate || '').localeCompare(a.expectedDeliveryDate || '');
        return (b.date || '').localeCompare(a.date || '');
    });
    const isApprover = currentUser.role === 'Admin' || currentUser.role === 'Manager';
    $('so-tbody').innerHTML = renderSORows(orders, isApprover);
    soUpdateBulkBtn();
}
async function openSalesOrderModal() {
    soItems = [];
    // Trigger geolocation and wait for it to ensure proximity sorting works
    await ensureGeolocation();
    
    const [parties, inv, categories] = await Promise.all([
        DB.getAll('parties'),
        DB.getAll('inventory'),
        DB.getAll('categories')
    ]);
    const customers = parties.filter(p => p.type === 'Customer');
    const orderNo = await nextNumber('SO-');

    openModal('Create Sales Order', `
        <div class="form-row"><div class="form-group"><label>Order #</label><input id="f-so-no" value="${orderNo}" readonly></div><div class="form-group"><label>Date</label><input type="date" id="f-so-date" value="${today()}"></div></div>
        <div class="form-row"><div class="form-group"><label>Expected Delivery</label><input type="date" id="f-so-delivery" value=""></div><div class="form-group"><label>Priority</label><select id="f-so-priority"><option value="Normal">Normal</option><option value="Urgent">🔥 Urgent</option></select></div></div>
        <div class="form-group"><label>Customer * <small style="color:var(--text-muted)">(new name = auto-created)</small></label>
            <input id="f-so-party" placeholder="Type customer name or mobile...">
        </div>
        
        <hr style="border-color:var(--border);margin:16px 0"><h4 style="margin-bottom:10px;font-size:0.9rem">Items</h4>
        
        <button class="btn btn-outline btn-block" onclick="openSoItemSubModal()" style="margin-bottom:16px;border-style:dashed;color:var(--primary);border-color:var(--primary);height:44px;font-weight:600">＋ Add Item(s)</button>
        
        <div class="table-wrapper"><div id="so-lines-list"></div></div>
        
        <div style="display:flex; justify-content:flex-end; gap:15px; align-items:flex-end; margin-top:10px; flex-wrap:wrap">
            <div class="form-group" style="width:100px; margin-bottom:0">
                <label style="font-size:0.7rem">Discount %</label>
                <input type="number" id="f-so-disc-pct" value="0" min="0" max="100" step="0.01" oninput="updateSoTotal()">
            </div>
            <div class="form-group" style="width:100px; margin-bottom:0">
                <label style="font-size:0.7rem">Discount ₹</label>
                <input type="number" id="f-so-disc-amt" value="0" min="0" step="0.01" oninput="updateSoTotal()">
            </div>
            <div style="text-align:right; font-size:1.15rem; font-weight:800; color:var(--accent)" id="so-total-display">Total: ₹0.00</div>
        </div>
        
        <div id="so-item-sub-modal" class="sub-modal">
            <div class="sub-modal-header">
                <h3>Add Item to Order</h3>
                <button class="btn-icon" onclick="closeSoItemSubModal()">✕</button>
            </div>
            <div class="sub-modal-body">
                <div class="form-row" style="margin-bottom:8px">
                    <div class="form-group">
                        <label>Category Filter</label>
                        <select id="f-so-cat-filter" onchange="onSOCatFilterChange()">
                            <option value="">All Categories</option>
                            ${categories.map(c => `<option value="${c.name}">${c.name}</option>`).join('')}
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Sub-Category Filter</label>
                        <select id="f-so-subcat-filter" onchange="onSOSubcatFilterChange()">
                            <option value="">All Sub-Categories</option>
                        </select>
                    </div>
                </div>
                <div class="inv-item-entry" style="background:var(--bg-input);padding:10px;border-radius:8px;margin-bottom:12px;border:1px solid var(--border)">
                    <div class="form-group" style="margin-bottom:10px">
                        <label style="font-size:0.8rem">Search & Select Item</label>
                        <input id="f-so-item-input" placeholder="Type item name or code..." style="background:#fff">
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">Qty</label><input type="number" id="f-so-qty" value="1" min="1" style="background:#fff"></div>
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">UOM</label><select id="f-so-uom" onchange="onSOUomChange()" style="background:#fff"><option value="">--</option></select></div>
                    </div>
                    <div style="display:grid;grid-template-columns:1fr auto;gap:10px;align-items:end">
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">Price ₹</label><input type="number" id="f-so-price" value="" min="0" step="0.01" placeholder="Listed" style="background:#fff"></div>
                        <button class="btn btn-primary" onclick="addSOLine()" style="height:38px;padding:0 20px">Add</button>
                    </div>
                </div>
                <button class="btn btn-outline btn-block" onclick="closeSoItemSubModal()" style="margin-top:10px">Done Adding</button>
            </div>
        </div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveSalesOrder()">＋ Save & New</button><button class="btn btn-primary" onclick="saveSalesOrder()">✅ Submit Order</button>`, true);

    // Init custom searchable dropdowns
    initSearchDropdown('f-so-party', buildPartySearchList(customers));

    _soItemDropdown = initSearchDropdown('f-so-item-input', buildItemSearchList(inv), function (item) {
        $('f-so-price').value = item.salePrice || '';
        var uomSel = $('f-so-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + item.unit + '">' + item.unit + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}

// Sub-Category filter handler
function onSOSubcatFilterChange() {
    var cat = $('f-so-cat-filter').value;
    var sc = $('f-so-subcat-filter').value;

    var inv = DB.get('db_inventory') || [];
    if (cat) inv = inv.filter(function (i) { return (i.category || '') === cat; });
    if (sc) inv = inv.filter(function (i) { return (i.subCategory || '') === sc; });

    $('f-so-item-input').value = '';
    $('f-so-price').value = '';
    _soItemDropdown = initSearchDropdown('f-so-item-input', buildItemSearchList(inv), function (item) {
        $('f-so-price').value = item.salePrice || '';
        var uomSel = $('f-so-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + item.unit + '">' + item.unit + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}
var _soItemDropdown = null;

// Category filter handler for SO modal
function onSOCatFilterChange() {
    var cat = $('f-so-cat-filter').value;
    var subCatSelect = $('f-so-subcat-filter');
    subCatSelect.innerHTML = '<option value="">All Sub-Categories</option>';
    if (cat) {
        var catObj = (DB.get('db_categories') || []).find(function (c) { return c.name === cat; });
        if (catObj && catObj.subCategories) {
            catObj.subCategories.forEach(function (sub) {
                subCatSelect.innerHTML += '<option value="' + sub + '">' + sub + '</option>';
            });
        }
    }
    var inv = DB.get('db_inventory') || [];
    if (cat) inv = inv.filter(function (i) { return (i.category || '') === cat; });
    var sc = $('f-so-subcat-filter').value;
    if (sc) inv = inv.filter(function (i) { return (i.subCategory || '') === sc; });
    $('f-so-item-input').value = '';
    $('f-so-price').value = '';
    _soItemDropdown = initSearchDropdown('f-so-item-input', buildItemSearchList(inv), function (item) {
        $('f-so-price').value = item.salePrice || '';
        var uomSel = $('f-so-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + item.unit + '">' + item.unit + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}


function onSOCatChange(catFilterId, subcatFilterId, itemSelectId, priceInputId) {
    const catName = $(catFilterId).value;
    const subCatSelect = $(subcatFilterId);
    subCatSelect.innerHTML = '<option value="">All Sub-Categories</option>';

    if (catName) {
        const cat = (DB.get('db_categories') || []).find(c => c.name === catName);
        if (cat && cat.subCategories) {
            cat.subCategories.forEach(sub => {
                subCatSelect.innerHTML += `<option value="${sub}">${sub}</option>`;
            });
        }
    }
    filterSOItems(catFilterId, subcatFilterId, itemSelectId, priceInputId);
}

function filterSOItems(catFilterId, subcatFilterId, itemSelectInputId, priceInputId) {
    const cat = $(catFilterId).value;
    const subcat = $(subcatFilterId).value;
    const itemSelectInput = $(itemSelectInputId);

    if (!itemSelectInput) return;
    const dataListId = itemSelectInput.getAttribute('list');
    const dataList = $(dataListId);
    if (!dataList) return;

    // reset selection if filtering changes
    itemSelectInput.value = "";
    if ($(priceInputId)) $(priceInputId).value = "";

    const inv = DB.get('db_inventory') || [];
    const filteredInv = inv.filter(i => {
        if (cat && (i.category || '') !== cat) return false;
        if (subcat && (i.subCategory || '') !== subcat) return false;
        return true;
    });

    dataList.innerHTML = filteredInv.map(i => {
        const avail = getAvailableStock(i).available;
        return `<option value="${i.name} [Avail: ${avail} ${i.unit || 'Pcs'}]" data-id="${i.id}" data-cat="${i.category || ''}" data-subcat="${i.subCategory || ''}">${i.itemCode ? 'Code: ' + i.itemCode : ''}</option>`;
    }).join('');
}

function onSOItemChange() {
    const sel = $('f-so-item-input');
    if (!sel || !sel.value) { $('f-so-price').value = ''; return; }

    // Resolve item
    const match = sel.value.match(/^(.*) \[Avail:/);
    let itemName = match ? match[1].trim() : sel.value.trim();
    const inv = DB.get('db_inventory');
    const item = inv.find(i => i.name.toLowerCase() === itemName.toLowerCase() || (i.itemCode || '').toLowerCase() === itemName.toLowerCase());
    if (!item) { $('f-so-price').value = ''; return; }

    $('f-so-price').value = item.salePrice || '';
    // Populate UOM dropdown
    const uomSel = $('f-so-uom');
    if (uomSel) {
        const priUnit = item.unit || 'Pcs';
        const secUom = item.secUom || '';
        uomSel.innerHTML = `<option value="${priUnit}">${priUnit}</option>`;
        if (secUom) uomSel.innerHTML += `<option value="${secUom}">${secUom}</option>`;
    }
}
function onSOUomChange() {
    const sel = $('f-so-item-input'); if (!sel || !sel.value) return;

    const match = sel.value.match(/^(.*) \[Avail:/);
    let itemName = match ? match[1].trim() : sel.value.trim();
    const item = DB.get('db_inventory').find(i => i.name.toLowerCase() === itemName.toLowerCase() || (i.itemCode || '').toLowerCase() === itemName.toLowerCase());
    if (!item) return;

    const primaryUnit = item.unit || 'Pcs';
    const secUom = item.secUom || '';
    const secRatio = +(item.secUomRatio) || 0;
    const selectedUom = $('f-so-uom').value;

    let listedPrice = +item.salePrice || 0;
    if (selectedUom !== primaryUnit && secUom && selectedUom === secUom && secRatio > 0) {
        listedPrice = listedPrice / secRatio;
    }
    $('f-so-price').value = listedPrice > 0 ? listedPrice.toFixed(2) : '';
}
function addSOLine() {
    const sel = $('f-so-item-input'); if (!sel || !sel.value) return;

    const match = sel.value.match(/^(.*) \[Avail:/);
    let itemName = match ? match[1].trim() : sel.value.trim();
    const itemObj = DB.get('db_inventory').find(i => i.name.toLowerCase() === itemName.toLowerCase() || (i.itemCode || '').toLowerCase() === itemName.toLowerCase());
    if (!itemObj) return alert("Invalid item");

    const qty = +$('f-so-qty').value || 1;
    const itemId = itemObj.id;
    const primaryUnit = itemObj.unit || 'Pcs';
    const secUom = itemObj.secUom || '';
    const secRatio = +(itemObj.secUomRatio) || 0;
    const uomSel = $('f-so-uom');
    const selectedUom = uomSel ? uomSel.value : primaryUnit;
    const unit = selectedUom || primaryUnit;
    const avail = getAvailableStock(itemObj).available;

    // Convert qty to primary unit for stock check
    let primaryQty = qty;
    if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
        primaryQty = qty / secRatio;
    }

    let listedPrice = +(itemObj.salePrice || 0);

    // Calculate total quantity for this item in the order (in primary units)
    const existingPrimaryQty = soItems.filter(li => li.itemId === itemId).reduce((s, li) => s + (li.primaryQty || li.qty), 0);
    const totalPrimaryQty = existingPrimaryQty + primaryQty;

    // Check if enough available stock (in primary units)
    const co = DB.getObj('db_company') || {};
    if (totalPrimaryQty > avail && !co.allowNegativeStock) {
        alert(`Cannot add ${qty} ${unit}. Only ${avail} ${primaryUnit} available in stock after existing reservations.`);
        return;
    }

    // Check volume pricing based on total primary quantity
    let baseListedPrice = +(itemObj.salePrice || 0);
    const item = itemObj;
    if (item && item.priceTiers && item.priceTiers.length) {
        for (const t of item.priceTiers) {
            if (totalPrimaryQty >= t.minQty) {
                baseListedPrice = t.price;
                break;
            }
        }
    }

    // Adjust listed price for alternate UOM
    let unitListedPrice = baseListedPrice;
    let unitPurchasePrice = +(itemObj.purchasePrice || 0);
    if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
        unitListedPrice = baseListedPrice / secRatio;
        unitPurchasePrice = unitPurchasePrice / secRatio;
    }

    // Use custom price if entered, otherwise use unit listed price
    const customPrice = $('f-so-price').value;
    const price = customPrice !== '' ? +customPrice : unitListedPrice;

    // Add the new line with listedPrice for comparison
    const roundedPrice = +price.toFixed(2);
    soItems.push({ 
        itemId, name: itemObj.name, qty, price: roundedPrice, 
        listedPrice: +unitListedPrice.toFixed(2), 
        purchasePrice: +unitPurchasePrice.toFixed(2),
        discountAmt: 0, discountPct: 0,
        amount: +(qty * roundedPrice).toFixed(2), unit, primaryQty 
    });

    // Retroactively update existing lines for the same item if the price tier changed
    soItems.forEach(li => {
        if (li.itemId === itemId) {
            let lineUnitListedPrice = baseListedPrice; // this is the base baseListedPrice
            if (li.unit !== primaryUnit && secUom && li.unit === secUom && secRatio > 0) {
                lineUnitListedPrice = baseListedPrice / secRatio;
            }

            // If the price was NOT manually overridden, update it to new volume tier
            if (Math.abs(li.price - li.listedPrice) < 0.001) {
                li.price = +(lineUnitListedPrice.toFixed(2));
                li.amount = +(li.qty * li.price).toFixed(2);
            }
            li.listedPrice = +(lineUnitListedPrice.toFixed(2));
        }
    });

    showToast('Item added to order', 'success');
    $('f-so-price').value = '';
    $('f-so-qty').value = '1';
    $('f-so-item-input').value = '';
    const uomSel2 = $('f-so-uom');
    if (uomSel2) uomSel2.innerHTML = '<option value="">--</option>';

    renderSOLines();
    if (window._soItemDropdown) window._soItemDropdown.clear();
    $('f-so-item-input').focus();
}
function removeSOLine(i) { soItems.splice(i, 1); renderSOLines(); }
function updateSOLine(idx, field, value) {
    const li = soItems[idx]; if (!li) return;

    if (field === 'qty') {
        const newQty = Math.max(1, +value || 1);
        const item = DB.get('db_inventory').find(x => x.id === li.itemId);
        if (item) {
            const avail = getAvailableStock(item).available + li.qty;
            if (newQty > avail) {
                alert(`Cannot update to ${newQty} ${li.unit || 'Pcs'}. Only ${avail} available.`);
                return;
            }
        }
        li.qty = newQty;
        if (li.discountPct > 0) li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'price') { 
        li.price = Math.max(0, +value || 0); 
        if (li.discountPct > 0) li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'discountPct') {
        li.discountPct = Math.max(0, +value || 0);
        li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'discountAmt') {
        li.discountAmt = Math.max(0, +value || 0);
        const lineVal = li.qty * li.price;
        li.discountPct = lineVal > 0 ? +( (li.discountAmt / lineVal) * 100 ).toFixed(2) : 0;
    }

    li.amount = +( (li.qty * li.price) - (li.discountAmt || 0) ).toFixed(2);
    
    // Price Alert Logic
    const unitPrice = li.qty > 0 ? li.amount / li.qty : 0;
    li._priceAlert = (unitPrice < (li.purchasePrice || 0) - 0.01);

    renderSOLines();
}

window.updateSoTotal = function() {
    const subtotal = soItems.reduce((s, l) => s + l.amount, 0);
    const discPct = +($('f-so-disc-pct')?.value || 0);
    const discAmt = +($('f-so-disc-amt')?.value || 0);
    
    let finalTotal = subtotal;
    let totalDiscount = 0;
    if (discPct > 0) totalDiscount += (subtotal * discPct / 100);
    if (discAmt > 0) totalDiscount += discAmt;
    finalTotal -= totalDiscount;
    
    const el = $('so-total-display');
    if (el) {
        el.innerHTML = `
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;font-size:0.82rem;text-align:right;width:100%">
            <span style="color:var(--text-muted)">Subtotal: <b>${currency(subtotal)}</b></span>
            ${totalDiscount ? `<span style="color:var(--danger)">Discount: <b>-${currency(totalDiscount)}</b></span>` : ''}
            <span style="color:var(--text-muted);border-top:1px dashed var(--border);padding-top:3px;margin-top:2px;width:100%"></span>
            <span style="font-size:1.15rem;font-weight:800;color:var(--accent)">Total: ${currency(Math.max(0, finalTotal))}</span>
        </div>`;
    }
}

function renderSOLines() {
    const el = $('so-lines-list'); if (!el) return;
    
    const header = `<div style="display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:2px solid var(--border);font-size:0.7rem;font-weight:700;color:var(--text-secondary);text-transform:uppercase;margin-bottom:4px;min-width:600px">
        <span style="width:20px;text-align:center">#</span>
        <span style="flex:1">Item</span>
        <span style="width:45px;text-align:center">Qty</span>
        <span style="width:25px;text-align:center">UOM</span>
        <span style="width:65px;text-align:right">Price</span>
        <span style="width:40px;text-align:center">Dis%</span>
        <span style="width:50px;text-align:center">Dis₹</span>
        <span style="width:75px;text-align:right">Amount</span>
        <span style="width:24px"></span>
    </div>`;

    el.innerHTML = header + soItems.map((li, i) => {
        const edited = li.listedPrice !== undefined && Math.abs(li.price - li.listedPrice) > 0.01;
        const alertStyle = li._priceAlert ? 'background:rgba(239, 68, 68, 0.05); border-left:3px solid var(--danger); padding-left:5px' : (edited ? 'background:rgba(245,158,11,0.05); border-left:3px solid var(--warning); padding-left:5px' : '');
        
        return `<div style="display:flex;align-items:center;gap:6px;padding:6px 0;border-bottom:1px solid var(--border);${alertStyle};min-width:600px">
            <span style="width:20px;text-align:center;font-size:0.75rem;color:var(--text-muted)">${i + 1}</span>
            <div style="flex:1;min-width:0">
                <div style="font-size:0.8rem;font-weight:600;word-break:break-word">${li.name}</div>
                ${li._priceAlert ? `<div style="font-size:0.6rem;color:var(--danger);font-weight:700">⚠️ < ${currency(li.purchasePrice)}</div>` : ''}
            </div>
            <input type="number" value="${li.qty}" min="1" style="width:45px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateSOLine(${i},'qty',this.value)">
            <span style="font-size:0.7rem;color:var(--text-muted);width:25px;text-align:center">${li.unit || 'Pcs'}</span>
            <div style="width:65px;text-align:right">
                ${edited ? `<div style="font-size:0.55rem;text-decoration:line-through;color:var(--text-muted)">${currency(li.listedPrice)}</div>` : ''}
                <input type="number" value="${(+li.price).toFixed(2)}" min="0" step="0.01" style="width:65px;padding:4px 2px;border-radius:4px;border:1px solid ${edited ? 'var(--warning)' : 'var(--border)'};text-align:right;font-size:0.75rem;${edited?'color:var(--warning);font-weight:600':''}" onchange="updateSOLine(${i},'price',this.value)">
            </div>
            <input type="number" value="${li.discountPct||0}" min="0" max="100" step="0.01" style="width:40px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateSOLine(${i},'discountPct',this.value)">
            <input type="number" value="${li.discountAmt||0}" min="0" step="0.01" style="width:50px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateSOLine(${i},'discountAmt',this.value)">
            <span style="width:75px;text-align:right;font-weight:700;font-size:0.8rem;color:${li._priceAlert?'var(--danger)':'inherit'}">${currency(li.amount)}</span>
            <button class="btn-icon" onclick="removeSOLine(${i})" style="flex-shrink:0;color:var(--danger);width:24px">✕</button>
        </div>`;
    }).join('');

    updateSoTotal();
}
function openSoItemSubModal() { const el = $('so-item-sub-modal'); if (el) el.classList.add('active'); }
function closeSoItemSubModal() { const el = $('so-item-sub-modal'); if (el) el.classList.remove('active'); }
function openInvItemSubModal() { const el = $('inv-item-sub-modal'); if (el) el.classList.add('active'); }
function closeInvItemSubModal() { const el = $('inv-item-sub-modal'); if (el) el.classList.remove('active'); }

async function saveSalesOrder() {
    if (!beginSave()) return;
    const pe = $('f-so-party'); if (!pe.value) { endSave(); return alert('Select customer'); } if (!soItems.length) { endSave(); return alert('Add items'); }

    const parties = await DB.getAll('parties');
    // dataset values are always strings; Supabase IDs may be integers — use loose match
    const storedId = pe.dataset.selectedId || '';
    let matched = storedId
        ? parties.find(x => String(x.id) === storedId)
        : parties.find(x => x.name.toLowerCase() === pe.value.trim().toLowerCase());

    if (!matched) {
        const typedName = pe.value.trim();
        if (!confirm(`Customer "${typedName}" not found in your party list.\n\nClick OK to create them as a new Customer, or Cancel to go back and select from the dropdown.`)) return;
        try {
            matched = await DB.insert('parties', { name: typedName, type: 'Customer', balance: 0 });
            showToast(`Customer "${typedName}" created!`, 'success');
        } catch (err) {
            return alert('Could not create customer: ' + (err.message || JSON.stringify(err)));
        }
    }
    let partyId = matched.id;
    let partyName = matched.name;

    const editId = $('f-so-edit-id') ? $('f-so-edit-id').value : '';
    const discPct = +($('f-so-disc-pct')?.value || 0);
    const discAmt = +($('f-so-disc-amt')?.value || 0);
    const subtotal = soItems.reduce((s, l) => s + l.amount, 0);
    let finalTotal = subtotal;
    if (discPct > 0) finalTotal -= (subtotal * discPct / 100);
    if (discAmt > 0) finalTotal -= discAmt;
    finalTotal = Math.max(0, finalTotal);

    const data = {
        date: $('f-so-date').value,
        expectedDeliveryDate: ($('f-so-delivery') && $('f-so-delivery').value) ? $('f-so-delivery').value : null,
        priority: $('f-so-priority') ? $('f-so-priority').value : 'Normal',
        partyId: partyId,
        partyName: partyName,
        items: [...soItems],
        total: finalTotal,
        discountPct: discPct,
        discountAmt: discAmt,
        notes: $('f-so-notes').value.trim()
    };

    // Blocked customer check
    if (matched.blocked) return alert(`❌ "${matched.name}" is blocked. Cannot create a Sales Order for a blocked customer. Contact admin to unblock.`);

    // Credit Limit Check
    const party = parties.find(p => p.id === partyId);
    if (party && party.type === 'Customer' && party.creditLimit > 0) {
        const currentBalance = party.balance || 0;
        const netOrderTotal = data.total;
        if ((currentBalance + netOrderTotal) > party.creditLimit) {
            if (!confirm(`Warning: This order will exceed the customer's credit limit of ${currency(party.creditLimit)}. Current Balance: ${currency(currentBalance)}. Total with Order: ${currency(currentBalance + netOrderTotal)}. Proceed anyway?`)) {
                return;
            }
        }
    }

    try {
        if (editId) {
            await DB.update('salesorders', editId, data);
            showToast(`Order updated!`, 'success');
        } else {
            const order = {
                ...data,
                orderNo: $('f-so-no').value,
                status: 'pending',
                createdBy: currentUser.name,
                packed: false
            };
            await DB.insert('salesorders', order);

            showToast(`Order submitted!`, 'success');
        }
        const andNew = window._saveAndNew; window._saveAndNew = false;
        closeModal();
        if (window._catalogOrderMode) {
            window._catalogOrderMode = false;
            catalogCart = [];
            // IMPORTANT: Wait for full sync so catalog shows correct reserved/avail qty
            try {
                await Promise.all([DB.getAll('sales_orders'), DB.getAll('inventory')]);
            } catch(e) { console.warn('Catalog post-save sync error:', e); }
            await renderCatalog();
        } else {
            await renderSalesOrders();
        }
        if (andNew && !editId) openSalesOrderModal();
    } catch (err) {
        window._saveAndNew = false;
        alert('Error saving order: ' + err.message);
    }
}
async function viewSalesOrder(id) {
    const orders = await DB.getAll('salesorders');
    const o = orders.find(x => x.id === id); if (!o) return;
    const isA = currentUser.role === 'Admin' || currentUser.role === 'Manager';
    const disp = await getOrderDisplayStatus(o);

    const parties = DB.cache['parties'] || [];
    const soParty = parties.find(x => String(x.id) === String(o.partyId));
    const soMapBtn = soParty && soParty.lat && soParty.lng
        ? `<button class="btn btn-outline btn-sm" onclick="openPartyMap('${soParty.lat}','${soParty.lng}','${escapeHtml(soParty.name)}')" style="margin-left:8px;font-size:0.75rem;padding:2px 8px">🗺️ Navigate</button>`
        : '';

    openModal(`Order ${o.orderNo}`, `
        <div style="margin-bottom:14px"><strong>Date:</strong> ${fmtDate(o.date)} | <strong>Customer:</strong> ${o.partyName}${soMapBtn} | <strong>Status:</strong> <span class="badge ${disp.class}" style="text-transform: capitalize">${disp.text}</span>${o.priority === 'Urgent' ? ' <span class="badge badge-danger">🔥 URGENT</span>' : ''}</div>
        <div style="margin-bottom:10px;font-size:0.85rem;color:var(--text-secondary)"><strong>By:</strong> ${o.createdBy}${o.expectedDeliveryDate ? ` | <strong>Expected Delivery:</strong> ${fmtDate(o.expectedDeliveryDate)}` : ''} ${o.approvedBy ? ` | <strong>${o.status === 'approved' ? 'Approved' : 'Rejected'} by:</strong> ${o.approvedBy}` : ''} ${o.rejectReason ? `<br><strong>Reason:</strong> ${o.rejectReason}` : ''}</div>
        
        ${o.invoiceNo ? `<div style="margin-bottom:10px;font-size:0.85rem;background:var(--bg-card);padding:8px;border:1px solid var(--border);border-radius:4px;color:var(--text-primary)">
            <strong>Linked Invoice:</strong> ${o.invoiceNo} ${o.packedBy ? `| <strong>Packed By:</strong> ${o.packedBy}` : ''}
        </div>` : ''}

        <table class="data-table"><thead><tr><th>SL</th><th>Item</th><th>Qty</th><th>Listed</th><th>Rate</th><th>Dis%</th><th>Dis₹</th><th>Amount</th></tr></thead>
        <tbody>${o.items.map((l, idx) => {
        const edited = l.listedPrice !== undefined && l.price !== l.listedPrice;
        return `<tr${edited ? ' style="background:rgba(245,158,11,0.06)"' : ''}><td>${idx + 1}</td><td>${l.name}</td><td>${l.qty} <span style="font-size:0.75rem;color:var(--text-muted)">${l.unit || 'Pcs'}</span>${l.packedQty !== undefined && l.packedQty !== l.qty ? ` <span style="color:var(--danger);font-size:0.8rem">(Packed: ${l.packedQty})</span>` : ''}</td>
            <td style="font-size:0.82rem;color:var(--text-muted)">${l.listedPrice !== undefined ? currency(l.listedPrice) : '-'}</td>
            <td>${edited ? `<span style="color:var(--warning);font-weight:600">${currency(l.price)}</span>` : currency(l.price)}</td>
            <td style="font-size:0.8rem">${l.discountPct || 0}%</td>
            <td style="font-size:0.8rem">${currency(l.discountAmt || 0)}</td>
            <td>${currency(l.amount)}</td></tr>`;
    }).join('')}
        <tr style="font-weight:700"><td colspan="7" style="text-align:right;color:var(--accent)">Total</td><td style="color:var(--accent)">${currency(o.total)}</td></tr></tbody></table>
        ${o.notes ? `<div style="margin-top:12px;padding:10px;background:var(--bg-input);border-radius:var(--radius-sm);font-size:0.85rem"><strong>Notes:</strong> ${o.notes}</div>` : ''}
        ${o.status === 'pending' && isA ? `<div class="modal-actions">
            <button class="btn btn-danger" onclick="rejectSalesOrder('${o.id}')">❌ Reject</button>
            <button class="btn btn-outline" onclick="editSalesOrder('${o.id}')">✏️ Edit</button>
            <button class="btn btn-primary" onclick="approveSalesOrder('${o.id}')">✅ Approve</button></div>` : ''}`);
}
async function editSalesOrder(id) {
    const orders = await DB.getAll('salesorders');
    const orig = orders.find(o => o.id === id); if (!orig) return;
    const inventory = await DB.getAll('inventory');

    soItems = orig.items.map(li => {
        const item = inventory.find(x => x.id === li.itemId);
        const latestListed = item ? item.salePrice : li.listedPrice || li.price;
        const discountAmt = li.discountAmt || 0;
        const discountPct = li.discountPct || 0;
        return { 
            itemId: li.itemId, 
            name: li.name, 
            qty: li.qty, 
            price: li.price, 
            listedPrice: latestListed, 
            discountAmt, 
            discountPct, 
            amount: +( (li.qty * li.price) - discountAmt ).toFixed(2), 
            unit: li.unit || (item ? item.unit : 'Pcs'), 
            purchasePrice: li.purchasePrice || (item ? item.purchasePrice : 0)
        };
    });

    const parties = await DB.getAll('parties');
    const customers = parties.filter(p => p.type === 'Customer');

    openModal(`Edit Order ${orig.orderNo}`, `
        <input type="hidden" id="f-so-edit-id" value="${orig.id}">
        <div class="form-row"><div class="form-group"><label>Order #</label><input id="f-so-no" value="${orig.orderNo}" readonly style="opacity:0.6"></div><div class="form-group"><label>Date</label><input type="date" id="f-so-date" value="${orig.date}"></div></div>
        <div class="form-row"><div class="form-group"><label>Expected Delivery</label><input type="date" id="f-so-delivery" value="${orig.expectedDeliveryDate || ''}"></div><div class="form-group"><label>Priority</label><select id="f-so-priority"><option value="Normal" ${(orig.priority || 'Normal') === 'Normal' ? 'selected' : ''}>Normal</option><option value="Urgent" ${orig.priority === 'Urgent' ? 'selected' : ''}>🔥 Urgent</option></select></div></div>
        <div class="form-group"><label>Customer *</label>
            <input id="f-so-party" value="${orig.partyName}" data-selected-id="${orig.partyId}" placeholder="Type customer name or mobile...">
        </div>
        <hr style="border-color:var(--border);margin:16px 0"><h4 style="margin-bottom:10px;font-size:0.9rem">Items</h4>
        <div class="form-row-3" style="margin-bottom:8px"><div class="form-group"><label>Item</label>
            <input id="f-so-item-input" placeholder="Type item name or code...">
        </div>
        <div class="form-group"><label>Qty</label><input type="number" id="f-so-qty" value="1" min="1"></div>
        <div class="form-group"><label>UOM</label><select id="f-so-uom" onchange="onSOUomChange()"><option value="">--</option></select></div>
        <div class="form-group"><label>Price ₹</label><input type="number" id="f-so-price" value="" min="0" step="0.01" placeholder="Listed"></div>
        <div class="form-group"><label>&nbsp;</label><button class="btn btn-primary btn-block" onclick="addSOLine()">Add</button></div></div>
        <div id="so-lines-list"></div>
        <div style="text-align:right;font-size:1.1rem;font-weight:700;color:var(--accent)" id="so-total-display">Total: ₹0.00</div>
        <div class="form-group" style="margin-top:12px"><label>Notes</label><input id="f-so-notes" value="${orig.notes ? escapeHtml(orig.notes) : ''}"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="saveSalesOrder()">Save Changes</button></div>`);
    initSearchDropdown('f-so-party', buildPartySearchList(customers));
    renderSOLines();
}
async function approveSalesOrder(id) {
    try {
        await DB.update('salesorders', id, {
            status: 'approved',
            approvedBy: currentUser.name,
            approvedAt: new Date().toISOString()
        });
        closeModal();
        await renderSalesOrders();
        showToast(`Order approved! It will appear in Packing.`, 'success');
    } catch (err) { alert(err.message); }
}
async function rejectSalesOrder(id) {
    const reason = prompt('Reason (optional):'); if (reason === null) return;
    try {
        await DB.update('salesorders', id, {
            status: 'rejected',
            approvedBy: currentUser.name,
            rejectReason: reason || 'No reason'
        });
        closeModal();
        await renderSalesOrders();
        showToast(`Order rejected.`, 'warning');
    } catch (err) { alert(err.message); }
}
async function deleteSalesOrder(id) {
    if (!confirm('Delete?')) return;
    try {
        await DB.delete('salesorders', id);
        await renderSalesOrders();
        showToast(`Order deleted.`, 'success');
    } catch (err) { alert(err.message); }
}
async function duplicateSalesOrder(id) {
    const [orders, inventory, categories, parties] = await Promise.all([
        DB.getAll('salesorders'),
        DB.getAll('inventory'),
        DB.getAll('categories'),
        DB.getAll('parties')
    ]);
    const orig = orders.find(o => o.id === id); if (!orig) return;

    soItems = orig.items.map(li => {
        const item = inventory.find(x => x.id === li.itemId);
        const latestPrice = item ? item.salePrice : li.price;
        const discountAmt = li.discountAmt || 0;
        const discountPct = li.discountPct || 0;
        return { 
            itemId: li.itemId, 
            name: li.name, 
            qty: li.qty, 
            price: latestPrice, 
            listedPrice: latestPrice, 
            discountAmt, 
            discountPct, 
            amount: +( (li.qty * latestPrice) - discountAmt ).toFixed(2), 
            unit: li.unit || (item ? item.unit : 'Pcs'),
            purchasePrice: li.purchasePrice || (item ? item.purchasePrice : 0)
        };
    });

    const customers = parties.filter(p => p.type === 'Customer');
    const orderNo = await nextNumber('SO-');

    openModal('Duplicate Sales Order', `
        <div class="form-row"><div class="form-group"><label>Order #</label><input id="f-so-no" value="${orderNo}"></div><div class="form-group"><label>Date</label><input type="date" id="f-so-date" value="${today()}"></div></div>
        <div class="form-row"><div class="form-group"><label>Expected Delivery</label><input type="date" id="f-so-delivery" value="${orig.expectedDeliveryDate || ''}"></div><div class="form-group"><label>Priority</label><select id="f-so-priority"><option value="Normal" ${(orig.priority || 'Normal') === 'Normal' ? 'selected' : ''}>Normal</option><option value="Urgent" ${orig.priority === 'Urgent' ? 'selected' : ''}>🔥 Urgent</option></select></div></div>
        <div class="form-group"><label>Customer *</label>
            <input id="f-so-party" value="${orig.partyName}" data-selected-id="${orig.partyId}" placeholder="Type customer name or mobile...">
        </div>
        
        <hr style="border-color:var(--border);margin:16px 0"><h4 style="margin-bottom:10px;font-size:0.9rem">Items <span style="font-size:0.75rem;color:var(--text-muted)">(copied from ${orig.orderNo}, prices updated)</span></h4>
        
        <div class="form-row" style="margin-bottom:8px">
            <div class="form-group">
                <label>Category Filter</label>
                <select id="f-so-cat-filter" onchange="onSOCatFilterChange()">
                    <option value="">All Categories</option>
                    ${categories.map(c => `<option value="${c.name}">${c.name}</option>`).join('')}
                </select>
            </div>
            <div class="form-group">
                <label>Sub-Category Filter</label>
                <select id="f-so-subcat-filter" onchange="onSOSubcatFilterChange()">
                    <option value="">All Sub-Categories</option>
                </select>
            </div>
        </div>

        <div class="form-row-3" style="margin-bottom:8px">
            <div class="form-group">
                <label>Item</label>
                <input id="f-so-item-input" placeholder="Type item name or code...">
            </div>
            <div class="form-group"><label>Qty</label><input type="number" id="f-so-qty" value="1" min="1"></div>
            <div class="form-group"><label>UOM</label><select id="f-so-uom"><option value="">--</option></select></div>
            <div class="form-group"><label>Price ₹</label><input type="number" id="f-so-price" value="" min="0" step="0.01" placeholder="Listed"></div>
            <div class="form-group"><label>&nbsp;</label><button class="btn btn-primary btn-block" onclick="addSOLine()">Add</button></div>
        </div>
        
        <div id="so-lines-list"></div>
        <div style="text-align:right;font-size:1.1rem;font-weight:700;color:var(--accent)" id="so-total-display">Total: ₹0.00</div>
        
        <div class="form-group" style="margin-top:12px"><label>Notes</label><input id="f-so-notes" value="${orig.notes ? escapeHtml(orig.notes) : ''}"></div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveSalesOrder()">＋ Save & New</button><button class="btn btn-primary" onclick="saveSalesOrder()">✅ Submit Order</button>`);
    initSearchDropdown('f-so-party', buildPartySearchList(customers));
    renderSOLines();
}

// =============================================
//  PURCHASE ORDERS (New Module)
// =============================================
let poItems = [];
async function renderPurchaseOrders() {
    const orders = await DB.getAll('purchaseorders');
    const p = orders.filter(o => o.status === 'pending'), r = orders.filter(o => o.status === 'received'), c = orders.filter(o => o.status === 'cancelled');
    pageContent.innerHTML = `
        <div class="stats-grid-sm" style="margin-bottom:14px">
            <div class="stat-card amber"><div class="stat-icon">⏳</div><div class="stat-value">${p.length}</div><div class="stat-label">Pending PO</div></div>
            <div class="stat-card green"><div class="stat-icon">📥</div><div class="stat-value">${r.length}</div><div class="stat-label">Received</div></div>
            <div class="stat-card red"><div class="stat-icon">❌</div><div class="stat-value">${c.length}</div><div class="stat-label">Cancelled</div></div>
        </div>
        <div style="display:flex;gap:8px;margin-bottom:14px">
            <button class="catalog-pill active" onclick="renderPurchaseOrders()" style="font-size:0.85rem">🛒 Purchase Orders</button>
            <button class="catalog-pill" onclick="renderPurchaseInvoices()" style="font-size:0.85rem">🧾 Purchase Invoices</button>
        </div>
        <div class="section-toolbar">
            <div class="filter-group">
                <button class="btn btn-outline" onclick="openColumnPersonalizer('purchaseorders','renderPurchaseOrders')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button>
                <select id="po-status-filter" onchange="filterPOTable()"><option value="">All Status</option><option value="pending">Pending</option><option value="received">Received</option><option value="cancelled">Cancelled</option></select>
                <input class="search-box" id="po-search" placeholder="Search PO or Supplier..." oninput="filterPOTable()" style="width:200px">
            </div>
            <button class="btn btn-primary" onclick="openPurchaseOrderModal()">+ New Purchase Order</button>
        </div>
        <div class="card"><div class="card-body" style="overflow-x:auto">
            <table class="data-table" style="min-width:700px"><thead><tr>${ColumnManager.get('purchaseorders').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
            <tbody id="po-tbody">${await renderPORows(orders)}</tbody></table>
        </div></div>`;
}

async function renderPORows(orders) {
    if (!orders.length) return '<tr><td colspan="7"><div class="empty-state"><p>No purchase orders found</p></div></td></tr>';
    const cols = ColumnManager.get('purchaseorders').filter(c => c.visible);
    return orders.slice().reverse().map(o => {
        const cellMap = {
            date:    `<td>${fmtDate(o.date)}</td>`,
            poNo:    `<td style="font-weight:600">${o.poNo}</td>`,
            party:   `<td>${escapeHtml(o.partyName)}</td>`,
            items:   `<td>${o.items.length}</td>`,
            total:   `<td class="amount-green">${currency(o.total)}</td>`,
            status:  `<td><span class="badge ${o.status === 'received' ? 'badge-success' : o.status === 'cancelled' ? 'badge-danger' : 'badge-warning'}">${o.status}</span></td>`,
            actions: `<td><div class="action-btns">
                <button class="btn-icon" onclick="viewPurchaseOrder('${o.id}')">👁️</button>
                ${o.status === 'pending' ? `<button class="btn-icon" style="color:var(--success)" onclick="receivePO('${o.id}')" title="Receive Goods">📥</button>` : ''}
                ${o.status === 'pending' ? `<button class="btn-icon" onclick="deletePO('${o.id}')">🗑️</button>` : ''}
            </div></td>`,
        };
        return `<tr>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}

async function filterPOTable() {
    const s = $('po-search').value.toLowerCase(), st = $('po-status-filter').value;
    let orders = await DB.getAll('purchaseorders');
    if (s) orders = orders.filter(o => o.poNo.toLowerCase().includes(s) || o.partyName.toLowerCase().includes(s));
    if (st) orders = orders.filter(o => o.status === st);
    $('po-tbody').innerHTML = await renderPORows(orders);
}

async function renderPurchaseInvoices() {
    const [allInvoices, parties, inventory] = await Promise.all([
        DB.getAll('invoices'), DB.getAll('parties'), DB.getAll('inventory')
    ]);
    const pinvs = allInvoices.filter(i => i.type === 'purchase').slice().reverse();
    const suppliers = parties.filter(p => p.type === 'Supplier');
    const totalAmt = pinvs.reduce((s, i) => s + i.total, 0);
    const activeCount = pinvs.filter(i => i.status !== 'cancelled').length;

    pageContent.innerHTML = `
        <div class="stats-grid-sm" style="margin-bottom:14px">
            <div class="stat-card blue"><div class="stat-icon">🧾</div><div class="stat-value">${pinvs.length}</div><div class="stat-label">Total Invoices</div></div>
            <div class="stat-card red"><div class="stat-icon">💸</div><div class="stat-value">${currency(totalAmt)}</div><div class="stat-label">Total Purchase</div></div>
            <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${activeCount}</div><div class="stat-label">Active</div></div>
        </div>
        <div style="display:flex;gap:8px;margin-bottom:14px">
            <button class="catalog-pill" onclick="renderPurchaseOrders()" style="font-size:0.85rem">🛒 Purchase Orders</button>
            <button class="catalog-pill active" onclick="renderPurchaseInvoices()" style="font-size:0.85rem">🧾 Purchase Invoices</button>
        </div>
        <div class="section-toolbar">
            <div class="filter-group">
                <input class="search-box" id="pinv-search" placeholder="Search invoice or supplier..." oninput="filterPInvTable()" style="width:220px">
                <select id="pinv-supplier" onchange="filterPInvTable()"><option value="">All Suppliers</option>${suppliers.map(s=>`<option value="${s.name}">${s.name}</option>`).join('')}</select>
                <input type="date" id="pinv-from" onchange="filterPInvTable()" placeholder="From">
                <input type="date" id="pinv-to" onchange="filterPInvTable()" placeholder="To">
            </div>
            ${canEdit() ? `<button class="btn btn-primary" onclick="openDirectPurchaseInvoiceModal()">+ Direct Purchase Invoice</button>` : ''}
        </div>
        <div class="card"><div class="card-body" style="overflow-x:auto">
            <table class="data-table" id="pinv-table" style="min-width:700px">
                <thead><tr><th>Date</th><th>Invoice #</th><th>Supplier</th><th>From PO</th><th>Items</th><th style="text-align:right">Total</th><th>Status</th><th>Actions</th></tr></thead>
                <tbody id="pinv-tbody">${renderPInvRows(pinvs)}</tbody>
            </table>
        </div></div>`;
    window._pinvsAll = pinvs;
    window._pinvsInv = inventory;
}

function renderPInvRows(invs) {
    if (!invs.length) return '<tr><td colspan="8"><div class="empty-state"><p>No purchase invoices found</p></div></td></tr>';
    return invs.map(i => `<tr>
        <td>${fmtDate(i.date)}</td>
        <td style="font-weight:600">${i.invoiceNo}</td>
        <td>${escapeHtml(i.partyName||'')}</td>
        <td style="font-size:0.82rem;color:var(--text-muted)">${i.fromOrder||'-'}</td>
        <td>${(i.items||[]).length}</td>
        <td class="amount-red" style="text-align:right">${currency(i.total)}</td>
        <td><span class="badge ${i.status==='cancelled'?'badge-danger':'badge-success'}">${i.status||'active'}</span></td>
        <td><div class="action-btns">
            <button class="btn-icon" onclick="viewPurchaseInvoice('${i.id}')">👁️</button>
            ${i.status !== 'cancelled' && canEdit() ? `<button class="btn-icon" style="color:var(--danger)" onclick="cancelPurchaseInvoice('${i.id}')">✕</button>` : ''}
        </div></td>
    </tr>`).join('');
}

function filterPInvTable() {
    const s = ($('pinv-search')||{}).value.toLowerCase();
    const sup = ($('pinv-supplier')||{}).value;
    const from = ($('pinv-from')||{}).value;
    const to = ($('pinv-to')||{}).value;
    let invs = (window._pinvsAll||[]).slice();
    if (s) invs = invs.filter(i => (i.invoiceNo||'').toLowerCase().includes(s) || (i.partyName||'').toLowerCase().includes(s));
    if (sup) invs = invs.filter(i => i.partyName === sup);
    if (from) invs = invs.filter(i => i.date >= from);
    if (to)   invs = invs.filter(i => i.date <= to);
    $('pinv-tbody').innerHTML = renderPInvRows(invs);
}

async function viewPurchaseInvoice(id) {
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => String(i.id) === String(id)); if (!inv) return;
    openModal(`Purchase Invoice ${inv.invoiceNo}`, `
        <div style="margin-bottom:14px;display:flex;gap:16px;flex-wrap:wrap;font-size:0.9rem">
            <div><strong>Date:</strong> ${fmtDate(inv.date)}</div>
            <div><strong>Supplier:</strong> ${inv.partyName}</div>
            <div><strong>From PO:</strong> ${inv.fromOrder||'-'}</div>
            <div><strong>Status:</strong> <span class="badge ${inv.status==='cancelled'?'badge-danger':'badge-success'}">${inv.status||'active'}</span></div>
        </div>
        <div class="card-body" style="overflow-x:auto">
        <table class="data-table" style="min-width:500px">
            <thead><tr><th>Item</th><th>Qty</th><th>Rate</th><th style="text-align:right">Amount</th></tr></thead>
            <tbody>${(inv.items||[]).map(l=>`<tr><td>${escapeHtml(l.name||'')}</td><td>${l.qty}</td><td>${currency(l.price||0)}</td><td style="text-align:right">${currency(l.amount||0)}</td></tr>`).join('')}
            <tr style="font-weight:700"><td colspan="3" style="text-align:right">Total</td><td style="text-align:right">${currency(inv.total)}</td></tr></tbody>
        </table></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button></div>
    `);
}

async function cancelPurchaseInvoice(id) {
    if (!confirm('Cancel this purchase invoice? Stock added will NOT be reversed automatically.')) return;
    try {
        await DB.update('invoices', id, { status: 'cancelled' });
        showToast('Purchase invoice cancelled', 'success');
        await renderPurchaseInvoices();
    } catch (e) { alert(e.message); }
}

async function openDirectPurchaseInvoiceModal() {
    const [parties, inv] = await Promise.all([DB.getAll('parties'), DB.getAll('inventory')]);
    const suppliers = parties.filter(p => p.type === 'Supplier');
    const invNo = await nextNumber('PINV-');
    poItems = [];
    openModal('Direct Purchase Invoice', `
        <div class="form-row">
            <div class="form-group"><label>Invoice #</label><input id="f-di-no" value="${invNo}" readonly></div>
            <div class="form-group"><label>Date</label><input type="date" id="f-di-date" value="${today()}"></div>
        </div>
        <div class="form-group"><label>Supplier *</label>
            <input id="f-di-party" placeholder="Type to search supplier..." autocomplete="off">
            <input id="f-di-party-id" type="hidden">
        </div>
        <hr style="margin:16px 0;border-color:var(--border)">
        <h4 style="font-size:0.9rem;margin-bottom:10px">Add Items</h4>
        <div class="form-row" style="gap:8px">
            <div class="form-group" style="flex:2"><label>Item</label><input id="f-di-item-input" placeholder="Type item name..."></div>
            <div class="form-group"><label>MRP ₹</label><input type="number" id="f-di-mrp" step="0.01" placeholder="MRP"></div>
            <div class="form-group"><label>Qty</label><input type="number" id="f-di-qty" value="1" min="1"></div>
            <div class="form-group"><label>Rate ₹</label><input type="number" id="f-di-price" step="0.01"></div>
            <div class="form-group" style="flex:0"><label>&nbsp;</label><button class="btn btn-primary" onclick="addDIPOLine()">Add</button></div>
        </div>
        <div id="di-lines-list" style="margin-top:10px"></div>
        <div style="text-align:right;font-size:1.1rem;font-weight:700;color:var(--accent);margin-top:15px" id="di-total-display">Total: ₹0.00</div>
        <div class="form-group" style="margin-top:12px"><label>Notes</label><textarea id="f-di-notes" rows="2" placeholder="Optional notes..." style="width:100%;resize:vertical;background:var(--bg-input);border:1px solid var(--border);border-radius:var(--radius-sm);padding:8px;color:var(--text-primary)"></textarea></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="saveDirectPurchaseInvoice()">Save Invoice</button></div>
    `);
    initSearchDropdown('f-di-party', buildPartySearchList(suppliers), (p) => {
        if ($('f-di-party-id')) $('f-di-party-id').value = p.id || '';
    });
    initSearchDropdown('f-di-item-input', buildItemSearchList(inv), (item) => {
        const lb = getLastActiveBatch(item) || getLastBatch(item);
        if ($('f-di-mrp')) $('f-di-mrp').value = lb ? lb.mrp : (item.mrp || '');
        $('f-di-price').value = lb ? lb.purchasePrice : (item.purchasePrice || '');
        $('f-di-qty').focus();
    });
}

function addDIPOLine() {
    const inp = $('f-di-item-input');
    const itemId = inp.dataset.selectedId;
    if (!itemId) return alert('Please select an item from the list');
    const qty = +$('f-di-qty').value;
    const price = +$('f-di-price').value;
    const mrp = +($('f-di-mrp')||{}).value || 0;
    poItems.push({ itemId, name: inp.value, qty, price, mrp, amount: qty * price, unit: 'Pcs' });
    renderDILines();
    inp.value = ''; inp.dataset.selectedId = ''; $('f-di-qty').value = 1; $('f-di-price').value = ''; if ($('f-di-mrp')) $('f-di-mrp').value = ''; inp.focus();
}

function renderDILines() {
    const el = $('di-lines-list'); if (!el) return;
    el.innerHTML = poItems.map((li, i) => `
        <div style="display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);font-size:0.88rem;align-items:center">
            <span style="flex:1">${li.name}</span>
            <span style="width:60px;text-align:center">${li.qty}</span>
            <span style="width:90px;text-align:right">@ ${currency(li.price)}</span>
            <span style="width:90px;text-align:right;font-weight:600;color:var(--accent)">${currency(li.amount)}</span>
            <button class="btn-icon" onclick="poItems.splice(${i},1);renderDILines()" style="color:var(--danger)">✕</button>
        </div>`).join('');
    if ($('di-total-display')) $('di-total-display').textContent = `Total: ${currency(poItems.reduce((s, l) => s + l.amount, 0))}`;
}

async function saveDirectPurchaseInvoice() {
    const partyId = ($('f-di-party-id')||{}).value||'';
    if (!partyId) return alert('Please select a supplier');
    if (!poItems.length) return alert('Add at least one item');
    try {
        const parties = await DB.getAll('parties');
        const party = parties.find(p => String(p.id) === String(partyId));
        if (!party) return alert('Supplier not found. Please re-select.');
        const invNo = $('f-di-no').value;
        const invDate = $('f-di-date').value;
        const notes = ($('f-di-notes')||{}).value||'';
        const total = poItems.reduce((s, l) => s + l.amount, 0);
        const inv = {
            invoiceNo: invNo, date: invDate, type: 'purchase',
            partyId, partyName: party.name, items: [...poItems],
            subtotal: total, gst: 0, total, status: 'active',
            notes, createdBy: currentUser.name
        };
        await DB.insert('invoices', inv);
        // Update inventory stock + batch qty + FIFO price sync
        const inventory = await DB.getAll('inventory');
        for (const li of poItems) {
            const item = inventory.find(i => String(i.id) === String(li.itemId));
            if (item) {
                const itemUpdate = { stock: item.stock + li.qty };
                // If line has MRP, add qty to matching batch and re-sync prices
                if (li.mrp && item.batches && item.batches.length) {
                    const batches = JSON.parse(JSON.stringify(item.batches));
                    const b = batches.find(x => +x.mrp === +li.mrp);
                    if (b) { b.qty = (b.qty || 0) + li.qty; }
                    itemUpdate.batches = batches;
                    Object.assign(itemUpdate, syncItemPricesFromBatches(batches));
                    // purchasePrice always from newest (this received batch)
                    itemUpdate.purchasePrice = li.price || item.purchasePrice;
                }
                await DB.update('inventory', item.id, itemUpdate);
                await addLedgerEntry(item.id, item.name, 'Purchase', li.qty, invNo, `Direct purchase invoice${li.mrp ? ` | MRP ₹${li.mrp}` : ''}`);
            }
        }
        // Update supplier ledger
        await addPartyLedgerEntry(partyId, party.name, 'Purchase Invoice', total, invNo, `Direct purchase: ${invNo}`);
        closeModal();
        poItems = [];
        await renderPurchaseInvoices();
        showToast(`Purchase Invoice ${invNo} saved! Stock updated.`, 'success');
    } catch (e) { alert('Error: ' + e.message); }
}

async function openPurchaseOrderModal() {
    poItems = [];
    const [parties, inv] = await Promise.all([DB.getAll('parties'), DB.getAll('inventory')]);
    const suppliers = parties.filter(p => p.type === 'Supplier');
    const poNo = await nextNumber('PO-');

    openModal('Create Purchase Order', `
        <div class="form-row"><div class="form-group"><label>PO #</label><input id="f-po-no" value="${poNo}" readonly></div><div class="form-group"><label>Date</label><input type="date" id="f-po-date" value="${today()}"></div></div>
        <div class="form-group"><label>Supplier *</label>
            <input id="f-po-party" placeholder="Type to search supplier..." autocomplete="off">
            <input id="f-po-party-id" type="hidden">
        </div>
        <hr style="margin:16px 0; border-color:var(--border)">
        <h4 style="font-size:0.9rem;margin-bottom:10px">Add Items</h4>
        <div class="form-row" style="gap:8px">
            <div class="form-group" style="flex:2">
                <label>Item</label>
                <input id="f-po-item-input" placeholder="Type item name or code...">
                <div id="po-item-hint" style="display:none;font-size:0.75rem;color:var(--accent);margin-top:3px"></div>
            </div>
            <div class="form-group"><label>MRP ₹</label><input type="number" id="f-po-mrp" step="0.01" placeholder="MRP"></div>
            <div class="form-group"><label>Qty</label><input type="number" id="f-po-qty" value="1" min="1"></div>
            <div class="form-group"><label>Rate ₹</label><input type="number" id="f-po-price" step="0.01"></div>
            <div class="form-group" style="flex:0"><label>&nbsp;</label><button class="btn btn-primary" onclick="addPOLine()">Add</button></div>
        </div>
        <div id="po-lines-list" style="margin-top:10px"></div>
        <div style="text-align:right;font-size:1.1rem;font-weight:700;color:var(--accent);margin-top:15px" id="po-total-display">Total: ₹0.00</div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;savePurchaseOrder()">＋ Save & New</button><button class="btn btn-primary" onclick="savePurchaseOrder()">Save Purchase Order</button>`);

    initSearchDropdown('f-po-party', buildPartySearchList(suppliers), (party) => {
        if ($('f-po-party-id')) $('f-po-party-id').value = party.id || '';
    });
    initSearchDropdown('f-po-item-input', buildItemSearchList(inv), (item) => {
        const lb = getLastActiveBatch(item) || getLastBatch(item);
        if ($('f-po-mrp')) $('f-po-mrp').value = lb ? lb.mrp : (item.mrp || '');
        $('f-po-price').value = lb ? lb.purchasePrice : (item.purchasePrice || '');
        // Show hint: current stock + last batch info
        const hintEl = $('po-item-hint');
        if (hintEl) {
            const batchInfo = lb ? ` | Last MRP ₹${lb.mrp}, PP ₹${lb.purchasePrice}` : '';
            hintEl.textContent = `Current stock: ${item.stock || 0} ${item.unit || ''}${batchInfo}`;
            hintEl.style.display = '';
        }
        $('f-po-qty').focus();
    });
}

function addPOLine() {
    const inp = $('f-po-item-input');
    const itemId = inp.dataset.selectedId;
    if (!itemId) return alert('Please select an item from the list');
    const qty = +$('f-po-qty').value;
    const price = +$('f-po-price').value;
    poItems.push({ itemId, name: inp.value, qty, price, amount: qty * price, unit: 'Pcs' });
    renderPOLines();
    inp.value = ''; inp.dataset.selectedId = ''; $('f-po-qty').value = 1; $('f-po-price').value = '';
    inp.focus();
}

function renderPOLines() {
    const el = $('po-lines-list');
    el.innerHTML = poItems.map((li, i) => `
        <div style="display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--border);font-size:0.88rem;align-items:center">
            <span style="flex:1">${li.name}</span>
            <span style="width:70px;text-align:center">${li.qty} ${li.unit || ''}</span>
            <span style="width:100px;text-align:right">@ ${currency(li.price)}</span>
            <span style="width:100px;text-align:right;font-weight:600;color:var(--accent)">${currency(li.amount)}</span>
            <button class="btn-icon" onclick="poItems.splice(${i},1);renderPOLines()" style="color:var(--danger)">✕</button>
        </div>`).join('');
    $('po-total-display').textContent = `Total: ${currency(poItems.reduce((s, l) => s + l.amount, 0))}`;
}

async function savePurchaseOrder() {
    if (!beginSave()) return;
    const partyId = ($('f-po-party-id') || {}).value || '';
    if (!partyId) { endSave(); return alert('Please select a supplier from the dropdown'); }
    if (!poItems.length) { endSave(); return alert('Add items'); }
    const parties = await DB.getAll('parties');
    const party = parties.find(p => String(p.id) === String(partyId));
    if (!party) return alert('Supplier not found. Please re-select from the dropdown.');

    const po = {
        poNo: $('f-po-no').value,
        date: $('f-po-date').value,
        partyId,
        partyName: party.name,
        items: [...poItems],
        total: poItems.reduce((s, l) => s + l.amount, 0),
        status: 'pending',
        createdBy: currentUser.name
    };
    await DB.insert('purchaseorders', po);
    const andNew = window._saveAndNew; window._saveAndNew = false;
    closeModal();
    await renderPurchaseOrders();
    showToast('Purchase Order created!', 'success');
    if (andNew) openPurchaseOrderModal();
}

async function viewPurchaseOrder(id) {
    const orders = await DB.getAll('purchaseorders');
    const o = orders.find(x => x.id === id); if (!o) return;
    openModal(`PO ${o.poNo}`, `
        <div style="margin-bottom:14px"><strong>Date:</strong> ${fmtDate(o.date)} | <strong>Supplier:</strong> ${o.partyName} | <strong>Status:</strong> <span class="badge ${o.status === 'received' ? 'badge-success' : 'badge-warning'}">${o.status.toUpperCase()}</span></div>
        <table class="data-table"><thead><tr><th>Item</th><th>Qty</th><th>Rate</th><th>Amount</th></tr></thead>
        <tbody>${o.items.map(l => `<tr><td>${l.name}</td><td>${l.qty}</td><td>${currency(l.price)}</td><td>${currency(l.amount)}</td></tr>`).join('')}
        <tr style="font-weight:700"><td colspan="3" style="text-align:right">Total</td><td>${currency(o.total)}</td></tr></tbody></table>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button>
        ${o.status === 'pending' ? `<button class="btn btn-primary" onclick="receivePO('${o.id}')">📥 Receive Goods</button>` : ''}</div>
    `);
}

async function receivePO(id) {
    if (!confirm('Received all goods? This will update inventory and create a purchase invoice.')) return;
    const orders = await DB.getAll('purchaseorders');
    const o = orders.find(x => x.id === id); if (!o) return;

    try {
        const invNo = await nextNumber('PINV-');
        const inv = {
            id: DB.id(), invoiceNo: invNo, date: today(), type: 'purchase',
            partyId: o.partyId, partyName: o.partyName, items: [...o.items],
            subtotal: o.total, gst: 0, total: o.total, status: 'active', fromOrder: o.poNo,
            createdBy: currentUser.name
        };
        await DB.insert('invoices', inv);

        const inventory = await DB.getAll('inventory');
        for (const li of o.items) {
            const item = inventory.find(i => i.id === li.itemId);
            if (item) {
                const newStock = item.stock + li.qty;
                const itemUpdate = { stock: newStock };
                // Update batch qty if item has batches and line has MRP
                if (item.batches && item.batches.length && li.mrp) {
                    const batches = JSON.parse(JSON.stringify(item.batches));
                    const b = batches.find(x => +x.mrp === +li.mrp);
                    if (b) {
                        b.qty = (b.qty || 0) + li.qty;
                    } else {
                        batches.push({ id: 'b_' + Date.now().toString(36), mrp: li.mrp, purchasePrice: li.price || item.purchasePrice, salePrice: li.salePrice || item.salePrice, qty: li.qty, receivedDate: today(), isActive: true });
                    }
                    itemUpdate.batches = batches;
                    Object.assign(itemUpdate, syncItemPricesFromBatches(batches));
                }
                await DB.update('inventory', item.id, itemUpdate);
                await addLedgerEntry(item.id, item.name, 'Purchase', li.qty, invNo, `Received from ${o.poNo}`);
            }
        }

        const parties = await DB.getAll('parties');
        const party = parties.find(p => p.id === o.partyId);
        if (party) {
            const newBalance = (party.balance || 0) - o.total;
            await DB.update('parties', party.id, { balance: newBalance });
            await addPartyLedgerEntry(party.id, party.name, 'Purchase Invoice', -o.total, invNo, `PO Goods Receipt ${o.poNo}`);
        }

        await DB.update('purchaseorders', id, { status: 'received' });
        closeModal();
        await renderPurchaseOrders();
        showToast(`Goods received! Inventory updated. Invoice: ${invNo}`, 'success');
    } catch (err) { alert(err.message); }
}

async function deletePO(id) {
    if (!confirm('Cancel this PO?')) return;
    try {
        await DB.update('purchaseorders', id, { status: 'cancelled' });
        await renderPurchaseOrders();
        showToast('PO Cancelled', 'warning');
    } catch (err) { alert(err.message); }
}

// =============================================
//  INVOICES (Sale & Purchase — purchase adds stock)
// =============================================
let invoiceItems = [];
async function renderInvoices() {
    const invoices = await DB.getAll('invoices');
    // Salesman only sees invoices assigned to them
    const visibleInvoices = currentUser.role === 'Salesman'
        ? invoices.filter(i => i.assignedTo === currentUser.name)
        : invoices;
    const validInvoices = visibleInvoices.filter(i => i.status !== 'cancelled');
    const sales = validInvoices.filter(i => i.type === 'sale'), purchases = validInvoices.filter(i => i.type === 'purchase');
    pageContent.innerHTML = `
        <div class="stats-grid" style="margin-bottom:18px">
            <div class="stat-card green"><div class="stat-icon">💹</div><div class="stat-value">${currency(sales.reduce((s, i) => s + i.total, 0))}</div><div class="stat-label">Total Sales</div></div>
            <div class="stat-card blue"><div class="stat-icon">🛒</div><div class="stat-value">${currency(purchases.reduce((s, i) => s + i.total, 0))}</div><div class="stat-label">Total Purchases</div></div>
        </div>
        <div class="section-toolbar">
            <div class="filter-group"><select id="inv-type-filter" onchange="filterInvTable2()"><option value="">All</option><option value="sale">Sale</option><option value="purchase">Purchase</option></select>
            <input class="search-box" id="inv-search2" placeholder="Search..." oninput="filterInvTable2()" style="width:200px">
            <button class="btn btn-outline" onclick="openColumnPersonalizer('invoices','renderInvoices')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button></div>
            <div class="filter-group">
                <button class="btn btn-primary" onclick="openInvoiceModal('sale')">+ Sale Invoice</button>
                <button class="btn btn-primary" style="background:var(--info)" onclick="openInvoiceModal('purchase')">+ Purchase / Stock In</button>
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>${ColumnManager.get('invoices').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="invoice-tbody">${renderInvoiceRows(visibleInvoices)}</tbody></table>
            </div>
        </div></div>`;
}
async function getInvoicePaidAmount(invNo) {
    const payments = await DB.getAll('payments');
    return payments.reduce((sum, p) => {
        // Multi-invoice allocation link
        if (p.allocations && p.allocations[invNo]) {
            // If the payment has a discount, we should proportionally attribute it or 
            // handle it if the allocation was meant to be the "total reduction".
            // In our system, the allocation value *is* the total reduction (Amt + Disc) for that invoice.
            return sum + (+p.allocations[invNo]);
        }
        // Legacy or direct link (single invoice)
        if (p.invoiceNo === invNo) {
            return sum + (p.amount || 0) + (p.discount || 0);
        }
        return sum;
    }, 0);
}

function renderInvoiceRows(invs) {
    if (!invs.length) return '<tr><td colspan="8" class="empty-state"><p>No invoices found</p></td></tr>';
    const canPay = canEdit() || currentUser.role === 'Salesman';
    const cols = ColumnManager.get('invoices').filter(c => c.visible);
    return invs.map(i => {
        const cellMap = {
            date:      `<td>${fmtDate(i.date)}</td>`,
            invoiceNo: `<td style="font-weight:600;text-decoration:${i.status === 'cancelled' ? 'line-through' : 'none'}">${i.invoiceNo}${i.vyaparInvoiceNo ? `<br><span style="font-size:0.7rem;font-weight:500;color:var(--primary)">V: ${escapeHtml(i.vyaparInvoiceNo)}</span>` : ''}${i.assignedTo ? `<br><span style="font-size:0.68rem;color:var(--info);font-weight:600">👤 ${escapeHtml(i.assignedTo)}${i.handoverDate ? ' · ' + fmtDate(i.handoverDate) : ''}</span>` : ''}</td>`,
            party:     `<td>${escapeHtml(i.partyName)}</td>`,
            type:      `<td><span class="badge ${i.type === 'sale' ? 'badge-success' : 'badge-info'}">${i.type}</span></td>`,
            status:    `<td>${i.status === 'cancelled' ? '<span class="badge badge-danger">Cancelled</span>' : '<span class="badge badge-success">Active</span>'}</td>`,
            items:     `<td>${(i.items||[]).length}</td>`,
            total:     `<td class="${i.type === 'sale' ? 'amount-green' : 'amount-red'}">${currency(i.total)}</td>`,
            actions:   `<td><div class="action-btns">
                <button class="btn-icon" onclick="viewInvoice('${i.id}')">👁️</button>
                ${canEdit() && i.type === 'sale' && i.status !== 'cancelled' ? `<button class="btn-icon" style="color:var(--info)" onclick="openAssignInvoiceModal('${i.id}')" title="Assign to Salesman">👤</button>` : ''}
                ${canPay ? `<button class="btn-icon" style="color:var(--success)" onclick="openReceivePaymentForInvoice('${i.id}')" title="Record Payment">💰</button>` : ''}
                ${canEdit() && i.status !== 'cancelled' ? `<button class="btn-icon" style="color:var(--danger)" onclick="cancelInvoiceDirectly('${i.id}')" title="Cancel Invoice">❌</button>` : ''}
                ${canEdit() ? `<button class="btn-icon" onclick="deleteInvoice('${i.id}')">🗑️</button>` : ''}
            </div></td>`,
        };
        return `<tr data-type="${i.type}">${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
async function filterInvTable2() {
    const s = $('inv-search2').value.toLowerCase(), t = $('inv-type-filter').value;
    let invs = await DB.getAll('invoices');
    if (s) invs = invs.filter(i => i.invoiceNo.toLowerCase().includes(s) || i.partyName.toLowerCase().includes(s));
    if (t) invs = invs.filter(i => i.type === t);
    $('invoice-tbody').innerHTML = renderInvoiceRows(invs);
}
// ============================================
//  VYAPAR INVOICE NO HELPERS
// ============================================
function getVyaparPrefix() { return DB.ls.getObj('vyapar_settings').prefix || ''; }
function getVyaparCurrentNo() { return parseInt(DB.ls.getObj('vyapar_settings').currentNo || '1'); }
function saveVyaparSettings(prefix, currentNo) { DB.saveSettings('vyapar_settings', { prefix, currentNo }); }

function buildVyaparInvoiceNo() {
    const prefix = getVyaparPrefix();
    const n = getVyaparCurrentNo();
    return prefix + n;
}

function incrementVyaparNo() {
    const s = DB.ls.getObj('vyapar_settings');
    const n = parseInt(s.currentNo || '1') + 1;
    DB.saveSettings('vyapar_settings', { ...s, currentNo: String(n) });
}

// ============================================
//  PAYMENT REF NO HELPERS
// ============================================
function getPayPrefix() { return DB.ls.getObj('pay_settings').prefix || 'PAY-'; }
function getPayCurrentNo() { return parseInt(DB.ls.getObj('pay_settings').currentNo || '1'); }
function savePaySettings(prefix, currentNo) { DB.saveSettings('pay_settings', { prefix, currentNo }); }
function buildPayRefNo() {
    const prefix = getPayPrefix();
    const n = getPayCurrentNo();
    return prefix + String(n).padStart(4, '0');
}
function incrementPayNo() {
    const s = DB.ls.getObj('pay_settings');
    const n = parseInt(s.currentNo || '1') + 1;
    DB.saveSettings('pay_settings', { ...s, currentNo: String(n) });
}

function openVyaparInvoiceNoModal() {
    const prefix = getVyaparPrefix();
    const currentNo = getVyaparCurrentNo();
    const full = prefix + currentNo;
    openModal('Change Invoice No.', `
        <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:12px">Invoice Prefix</p>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px">
            <button class="vyapar-preset-btn" onclick="applyVyaparPreset('')"${!prefix ? ' data-active="1"' : ''}>None</button>
            ${prefix ? `<button class="vyapar-preset-btn" onclick="applyVyaparPreset('${escapeHtml(full)}')">${escapeHtml(full)}</button>` : ''}
            ${prefix ? `<button class="vyapar-preset-btn" data-active="1" onclick="applyVyaparPreset('${escapeHtml(prefix + currentNo)}')">${escapeHtml(prefix)}</button>` : ''}
            <button class="vyapar-preset-btn vyapar-add-prefix" onclick="document.getElementById('f-vy-prefix').focus()">+ Add Prefix</button>
        </div>
        <div class="form-group">
            <label>Prefix (e.g. PT-NS-)</label>
            <input id="f-vy-prefix" value="${escapeHtml(prefix)}" placeholder="e.g. PT-NS-" oninput="updateVyaparPreview()">
        </div>
        <div class="form-group">
            <label>Invoice No. *</label>
            <input id="f-vy-no" value="${currentNo}" type="number" min="1" oninput="updateVyaparPreview()" style="font-size:1.1rem;font-weight:700">
        </div>
        <div style="background:rgba(249,115,22,0.06);border:1px solid rgba(249,115,22,0.2);border-radius:10px;padding:12px 16px;margin-bottom:16px;display:flex;align-items:center;gap:10px">
            <span style="color:var(--text-muted);font-size:0.82rem">Preview:</span>
            <strong id="vy-preview-lbl" style="font-size:1.1rem;color:var(--primary)">${escapeHtml(full || String(currentNo))}</strong>
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveVyaparInvoiceNoFromModal()" style="min-width:120px;font-size:1rem;font-weight:700">SAVE</button>
        </div>
    `);
}

function applyVyaparPreset(val) {
    // Extract number from value
    const prefix = getVyaparPrefix();
    if (!val) {
        if ($('f-vy-prefix')) $('f-vy-prefix').value = '';
        updateVyaparPreview();
        return;
    }
    if ($('f-vy-prefix')) $('f-vy-prefix').value = prefix;
    updateVyaparPreview();
}

function updateVyaparPreview() {
    const prefix = ($('f-vy-prefix') || {}).value || '';
    const no = ($('f-vy-no') || {}).value || '1';
    const lbl = $('vy-preview-lbl');
    if (lbl) lbl.textContent = prefix + no;
}

function saveVyaparInvoiceNoFromModal() {
    const prefix = ($('f-vy-prefix') || {}).value || '';
    const no = parseInt(($('f-vy-no') || {}).value || '1');
    if (!no || isNaN(no) || no < 1) return alert('Enter a valid invoice number');
    saveVyaparSettings(prefix, String(no));
    // Update the field in invoice modal if open
    const fld = $('f-vyapar-inv-no');
    if (fld) fld.value = prefix + no;
    closeModal();
    showToast('Vyapar Invoice No. updated: ' + prefix + no, 'success');
}

function updateInvDueDate(party) {
    const dueDateEl = $('f-inv-due-date');
    if (!dueDateEl) return;
    const termName = party ? (party.paymentTerms || '') : '';
    if (!termName) { dueDateEl.value = ''; dueDateEl.placeholder = 'No terms set'; return; }
    const terms = getPaymentTermsList();
    const term = terms.find(t => t.name === termName);
    if (!term) { dueDateEl.value = ''; return; }
    const invDateEl = $('f-inv-date');
    const base = invDateEl ? invDateEl.value : today();
    const d = new Date(base);
    d.setDate(d.getDate() + (term.days || 0));
    dueDateEl.value = d.toISOString().split('T')[0];
    dueDateEl.style.color = 'var(--accent)';
    dueDateEl.title = `${termName} — ${term.days} days from invoice date`;
}
async function openInvoiceModal(type = 'sale') {
    invoiceItems = [];
    await ensureGeolocation();
    const ptype = type === 'sale' ? 'Customer' : 'Supplier';
    const [parties, inv, categories] = await Promise.all([
        DB.getAll('parties'),
        DB.getAll('inventory'),
        DB.getAll('categories')
    ]);
    const filteredParties = parties.filter(p => p.type === ptype);
    const invNo = await nextNumber(type === 'sale' ? 'INV-' : 'PUR-');

    const vyaparNo = buildVyaparInvoiceNo();
    openModal(type === 'sale' ? 'Create Sale Invoice' : 'Create Purchase / Stock In', `
        <div class="form-row"><div class="form-group"><label>Invoice #</label><input id="f-inv-no" value="${invNo}"></div><div class="form-group"><label>Date</label><input type="date" id="f-inv-date" value="${today()}" onchange="updateInvDueDate()"></div><div class="form-group"><label>Due Date <span style="font-size:0.7rem;color:var(--text-muted)">auto from terms</span></label><input type="date" id="f-inv-due-date" placeholder="Select party..."></div></div>
        <input type="hidden" id="f-inv-type" value="${type}">
        ${type === 'sale' ? `
        <div class="form-group">
            <label>Vyapar Invoice No. <span style="color:var(--error,#ef4444)">*</span> <span style="font-size:0.72rem;color:var(--text-muted)">(auto-increments on save)</span></label>
            <div class="vyapar-inv-row">
                <input id="f-vyapar-inv-no" value="${escapeHtml(vyaparNo)}" placeholder="e.g. PT-NS-1">
                <button class="vyapar-gear-btn" onclick="openVyaparInvoiceNoModal()" title="Change prefix / number">⚙️</button>
            </div>
        </div>` : ''}
        <div class="form-group"><label>${ptype} *</label>
            <input id="f-inv-party" placeholder="Type name or mobile...">
        </div>
        
        <hr style="border-color:var(--border);margin:16px 0"><h4 style="margin-bottom:10px;font-size:0.9rem">Items</h4>
        
        <button class="btn btn-outline btn-block" onclick="openInvItemSubModal()" style="margin-bottom:16px;border-style:dashed;color:var(--primary);border-color:var(--primary);height:44px;font-weight:600">＋ Add Item(s)</button>
        
        <div class="table-wrapper"><div id="inv-lines-list"></div></div>
        
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:end;margin-top:12px;background:var(--bg-card);padding:10px;border-radius:6px;border:1px dashed var(--border)">
            <div class="form-group" style="min-width:70px;margin-bottom:0;flex:1">
                <label style="font-size:0.75rem">GST %</label>
                <input type="number" id="f-inv-gst" value="0" min="0" max="100" step="0.1" onchange="updateInvoiceTotal()">
            </div>
            <div class="form-group" style="min-width:70px;margin-bottom:0;flex:1">
                <label style="font-size:0.75rem">Disc %</label>
                <input type="number" id="f-inv-disc-pct" value="0" min="0" max="100" step="0.1" onchange="updateInvoiceTotal()">
            </div>
            <div class="form-group" style="min-width:70px;margin-bottom:0;flex:1">
                <label style="font-size:0.75rem">Disc ₹</label>
                <input type="number" id="f-inv-disc-amt" value="0" min="0" step="0.01" placeholder="0.00" onchange="updateInvoiceTotal()">
            </div>
        </div>
        
        <div style="display:flex;gap:10px;align-items:end;margin-top:10px;justify-content:space-between">
            <div class="form-group" style="width:140px;margin-bottom:0">
                <label style="font-size:0.75rem">Round Off ₹</label>
                <div style="display:flex;gap:4px">
                    <input type="number" id="f-inv-roundoff" value="0" step="0.01" placeholder="0.00" oninput="updateInvoiceTotal()">
                    <button class="btn btn-outline btn-sm" onclick="autoRoundOff()" title="Auto" style="padding:0 8px">⟳</button>
                </div>
            </div>
        </div>
        
        <div id="inv-total-display" style="text-align:right;font-size:1rem;color:var(--text-secondary);font-weight:600;margin-top:4px">Total: ₹0.00</div>
        
        <div id="inv-advance-section" style="margin-top:10px"></div>
        <div id="inv-item-sub-modal" class="sub-modal">
            <div class="sub-modal-header">
                <h3>Add Item</h3>
                <button class="btn-icon" onclick="closeInvItemSubModal()">✕</button>
            </div>
            <div class="sub-modal-body">
                <div class="form-row" style="margin-bottom:8px">
                    <div class="form-group">
                        <label>Category Filter</label>
                        <select id="f-inv-cat-filter" onchange="onInvCatFilterChange()">
                            <option value="">All Categories</option>
                            ${categories.map(c => `<option value="${c.name}">${c.name}</option>`).join('')}
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Sub-Category Filter</label>
                        <select id="f-inv-subcat-filter" onchange="onInvSubcatFilterChange()">
                            <option value="">All Sub-Categories</option>
                        </select>
                    </div>
                </div>
                <div class="inv-item-entry" style="background:var(--bg-input);padding:10px;border-radius:8px;margin-bottom:12px;border:1px solid var(--border)">
                    <div class="form-group" style="margin-bottom:10px">
                        <label style="font-size:0.8rem">Search & Select Item</label>
                        <input id="f-inv-item-input" placeholder="Type item name or code..." style="background:#fff">
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">Qty</label><input type="number" id="f-inv-qty" value="1" min="1" style="background:#fff"></div>
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">UOM</label><select id="f-inv-uom" onchange="onInvUomChange()" style="background:#fff"><option value="">--</option></select></div>
                    </div>
                    <div style="display:grid;grid-template-columns:1fr auto;gap:10px;align-items:end">
                        <div class="form-group" style="margin-bottom:0"><label style="font-size:0.75rem">Price ₹</label><input type="number" id="f-inv-price" value="" min="0" step="0.01" placeholder="Listed" style="background:#fff"></div>
                        <button class="btn btn-primary" onclick="addInvoiceLine()" style="height:38px;padding:0 20px">Add</button>
                    </div>
                </div>
                <button class="btn btn-outline btn-block" onclick="closeInvItemSubModal()" style="margin-top:10px">Done Adding</button>
            </div>
        </div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveInvoice()">＋ Save & New</button><button class="btn btn-primary" onclick="saveInvoice()">💾 Save Invoice</button>`, true);

    // Init custom searchable dropdowns
    initSearchDropdown('f-inv-party', buildPartySearchList(filteredParties), function (party) {
        const t = $('f-inv-type') ? $('f-inv-type').value : 'sale';
        if (t === 'sale') loadAvailableAdvances(party.id);
        updateInvDueDate(party);
    });

    _invItemDropdown = initSearchDropdown('f-inv-item-input', buildItemSearchList(inv), function (item) {
        const type = $('f-inv-type') ? $('f-inv-type').value : 'sale';
        $('f-inv-price').value = type === 'sale' ? (item.salePrice || '') : (item.purchasePrice || '');
        var uomSel = $('f-inv-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + (item.unit || 'Pcs') + '">' + (item.unit || 'Pcs') + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}

async function loadAvailableAdvances(partyId) {
    const invSec = $('inv-advance-section');
    if (!invSec) return;

    // Find payments in with unallocated amounts
    const payments = (await DB.getAll('payments')).filter(p => p.type === 'in' && p.partyId === partyId);
    let advances = [];
    payments.forEach(p => {
        let allocated = 0;
        if (p.allocations) {
            Object.values(p.allocations).forEach(v => allocated += v);
        } else if (p.invoiceNo && p.invoiceNo !== 'Advance' && p.invoiceNo !== 'Multi') {
            allocated = p.amount; // legacy direct tie
        }
        const avail = p.amount - allocated;
        if (avail > 0.01) {
            advances.push({ id: p.id, date: p.date, avail });
        }
    });

    if (advances.length === 0) {
        invSec.innerHTML = '';
        return;
    }

    let html = `<div style="background:var(--bg-card);padding:10px;border:1px solid var(--border);border-radius:6px;margin-bottom:10px">
        <label style="color:var(--accent);font-weight:600;margin-bottom:8px;display:block">Adjust from Advance</label>
        <table class="data-table" style="font-size:0.85rem;margin:0">
            <thead style="background:var(--bg-input)"><tr><th>Date</th><th>Available ₹</th><th>Apply ₹</th></tr></thead>
            <tbody>`;

    advances.forEach(a => {
        html += `<tr>
            <td>${fmtDate(a.date)}</td>
            <td style="color:var(--success)">${currency(a.avail)}</td>
            <td><input type="number" step="0.01" max="${a.avail.toFixed(2)}" class="form-control inv-apply-adv-input" data-pay="${a.id}" placeholder="0" style="padding:4px;width:100px" oninput="calcAppliedAdvance()"></td>
        </tr>`;
    });

    html += `</tbody></table>
        <div style="text-align:right;margin-top:8px;font-size:0.85rem;font-weight:600">Total Applied: <span id="lbl-inv-total-adv" style="color:var(--success)">₹0.00</span></div>
    </div>`;

    invSec.innerHTML = html;
}

window.calcAppliedAdvance = function () {
    let tot = 0;
    document.querySelectorAll('.inv-apply-adv-input').forEach(inp => tot += (+inp.value || 0));
    const lbl = document.getElementById('lbl-inv-total-adv');
    if (lbl) lbl.innerText = '₹' + tot.toFixed(2);
};

var _invItemDropdown = null;

// Sub-Category filter handler
async function onInvSubcatFilterChange() {
    var cat = $('f-inv-cat-filter').value;
    var sc = $('f-inv-subcat-filter').value;

    var inv = (await DB.getAll('inventory')) || [];
    if (cat) inv = inv.filter(function (i) { return (i.category || '') === cat; });
    if (sc) inv = inv.filter(function (i) { return (i.subCategory || '') === sc; });

    $('f-inv-item-input').value = '';
    $('f-inv-price').value = '';
    _invItemDropdown = initSearchDropdown('f-inv-item-input', buildItemSearchList(inv), function (item) {
        const type = $('f-inv-type') ? $('f-inv-type').value : 'sale';
        $('f-inv-price').value = type === 'sale' ? (item.salePrice || '') : (item.purchasePrice || '');
        var uomSel = $('f-inv-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + (item.unit || 'Pcs') + '">' + (item.unit || 'Pcs') + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}

// Category filter handler for Invoice modal
async function onInvCatFilterChange() {
    var catName = $('f-inv-cat-filter').value;
    var subCatSelect = $('f-inv-subcat-filter');
    subCatSelect.innerHTML = '<option value="">All Sub-Categories</option>';
    if (catName) {
        const categories = await DB.getAll('categories');
        var catObj = categories.find(function (c) { return c.name === catName; });
        if (catObj && catObj.subCategories) {
            catObj.subCategories.forEach(function (sub) {
                subCatSelect.innerHTML += '<option value="' + sub + '">' + sub + '</option>';
            });
        }
    }
    var inv = (await DB.getAll('inventory')) || [];
    if (catName) inv = inv.filter(function (i) { return (i.category || '') === catName; });
    var sc = $('f-inv-subcat-filter').value;
    if (sc) inv = inv.filter(function (i) { return (i.subCategory || '') === sc; });
    $('f-inv-item-input').value = '';
    $('f-inv-price').value = '';
    _invItemDropdown = initSearchDropdown('f-inv-item-input', buildItemSearchList(inv), function (item) {
        const type = $('f-inv-type') ? $('f-inv-type').value : 'sale';
        $('f-inv-price').value = type === 'sale' ? (item.salePrice || '') : (item.purchasePrice || '');
        var uomSel = $('f-inv-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + (item.unit || 'Pcs') + '">' + (item.unit || 'Pcs') + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });
}

async function onInvUomChange() {
    const sel = $('f-inv-item-input'); if (!sel || !sel.value) return;

    const match = sel.value.match(/^(.*) \[Avail:/);
    let itemName = match ? match[1].trim() : sel.value.trim();
    const inventory = await DB.getAll('inventory');
    const item = inventory.find(i => i.name.toLowerCase() === itemName.toLowerCase() || (i.itemCode || '').toLowerCase() === itemName.toLowerCase());
    if (!item) return;

    const type = $('f-inv-type') ? $('f-inv-type').value : 'sale';
    const primaryUnit = item.unit || 'Pcs';
    const secUom = item.secUom || '';
    const secRatio = +(item.secUomRatio) || 0;
    const selectedUom = $('f-inv-uom').value;

    let listedPrice = type === 'sale' ? +(item.salePrice) : +(item.purchasePrice);
    if (!listedPrice) listedPrice = 0;

    if (selectedUom !== primaryUnit && secUom && selectedUom === secUom && secRatio > 0) {
        listedPrice = listedPrice / secRatio;
    }
    $('f-inv-price').value = listedPrice > 0 ? listedPrice.toFixed(2) : '';
}

async function addInvoiceLine() {
    const sel = $('f-inv-item-input'); if (!sel || !sel.value) return;

    const match = sel.value.match(/^(.*) \[Avail:/);
    let itemName = match ? match[1].trim() : sel.value.trim();
    const inventory = await DB.getAll('inventory');
    const itemObj = inventory.find(i => i.name.toLowerCase() === itemName.toLowerCase() || (i.itemCode || '').toLowerCase() === itemName.toLowerCase());
    if (!itemObj) return alert("Invalid item");

    const type = $('f-inv-type').value;
    const qty = +$('f-inv-qty').value || 1;
    const itemId = itemObj.id;
    const primaryUnit = itemObj.unit || 'Pcs';
    const secUom = itemObj.secUom || '';
    const secRatio = +(itemObj.secUomRatio) || 0;
    const uomSel = $('f-inv-uom');
    const selectedUom = uomSel ? uomSel.value : primaryUnit;
    const unit = selectedUom || primaryUnit;
    let listedPrice = 0;

    // Convert qty to primary unit for stock check
    let primaryQty = qty;
    if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
        primaryQty = qty / secRatio;
    }

    if (type === 'sale') {
        const avail = (await getAvailableStock(itemObj)).available;
        const existingPrimaryQty = invoiceItems.filter(li => li.itemId === itemId).reduce((s, li) => s + (li.primaryQty || li.qty), 0);
        const totalPrimaryQty = existingPrimaryQty + primaryQty;

        const co = DB.getObj('db_company') || {};
        if (totalPrimaryQty > avail && !co.allowNegativeStock) {
            alert(`Cannot add ${qty} ${unit}. Only ${avail} ${primaryUnit} available in stock after existing reservations.`);
            return;
        }

        let baseListedPrice = +(itemObj.salePrice || 0);
        const item = itemObj;
        if (item) {
            baseListedPrice = item.salePrice;
            if (item.priceTiers && item.priceTiers.length) {
                for (const t of item.priceTiers) {
                    if (totalPrimaryQty >= t.minQty) { baseListedPrice = t.price; break; }
                }
            }
        }
        listedPrice = baseListedPrice;
    } else {
        listedPrice = +(itemObj.purchasePrice || 0);
    }

    // Adjust listed price for alternate UOM
    let unitListedPrice = listedPrice;
    let unitPurchasePrice = +(itemObj.purchasePrice || 0);
    if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
        unitListedPrice = listedPrice / secRatio;
        unitPurchasePrice = unitPurchasePrice / secRatio;
    }

    // Use custom price if entered, otherwise use unit listed price
    const customPrice = $('f-inv-price').value;
    const price = customPrice !== '' ? +customPrice : unitListedPrice;

    const itemGstRate = +(itemObj.gstRate || 0);
    const lineAmount  = qty * price;
    const lineBase    = itemGstRate > 0 ? +(lineAmount / (1 + itemGstRate / 100)).toFixed(2) : lineAmount;
    const lineTax     = +(lineAmount - lineBase).toFixed(2);
    invoiceItems.push({ 
        itemId, name: itemObj.name, qty, price, 
        listedPrice: +unitListedPrice.toFixed(2), 
        purchasePrice: +unitPurchasePrice.toFixed(2),
        discountAmt: 0, discountPct: 0,
        amount: lineAmount, unit, primaryQty, gstRate: itemGstRate, 
        baseAmount: lineBase, taxAmount: lineTax 
    });

    // Sync GST% field from item rates — if all items share one rate, show it; else leave as-is
    const activeRates = [...new Set(invoiceItems.map(li => li.gstRate || 0).filter(r => r > 0))];
    const gstFld = $('f-inv-gst');
    if (gstFld) {
        if (activeRates.length === 1) gstFld.value = activeRates[0];
        else if (activeRates.length === 0) gstFld.value = 0;
    }

    // Retroactively update listed prices for same item (volume tier changes)
    if (type === 'sale') {
        invoiceItems.forEach(li => {
            if (li.itemId === itemId) {
                let lineUnitListedPrice = listedPrice; // this is the base listedPrice
                if (li.unit !== primaryUnit && secUom && li.unit === secUom && secRatio > 0) {
                    lineUnitListedPrice = listedPrice / secRatio;
                }

                // If the price was NOT manually overridden, update it to new volume tier
                if (Math.abs(li.price - li.listedPrice) < 0.001) {
                    li.price = +(lineUnitListedPrice.toFixed(2));
                    li.amount = li.qty * li.price;
                }
                li.listedPrice = +(lineUnitListedPrice.toFixed(2));
            }
        });
    }

    showToast('Item added to invoice', 'success');
    $('f-inv-price').value = '';
    $('f-inv-qty').value = '1';
    $('f-inv-item-input').value = '';
    const uomSel2 = $('f-inv-uom');
    if (uomSel2) uomSel2.innerHTML = '<option value="">--</option>';

    renderInvoiceLines();
    if (window._invItemDropdown) window._invItemDropdown.clear();
    $('f-inv-item-input').focus();
}
function removeInvoiceLine(idx) {
    invoiceItems.splice(idx, 1);
    const activeRates = [...new Set(invoiceItems.map(li => li.gstRate || 0).filter(r => r > 0))];
    const gstFld = $('f-inv-gst');
    if (gstFld) {
        if (activeRates.length === 1) gstFld.value = activeRates[0];
        else if (activeRates.length === 0) gstFld.value = 0;
    }
    renderInvoiceLines();
}
function updateInvoiceLine(idx, field, value) {
    const li = invoiceItems[idx]; if (!li) return;
    const type = $('f-inv-type') ? $('f-inv-type').value : 'sale';

    if (field === 'qty') {
        const newQty = Math.max(1, +value || 1);
        const item = DB.cache['inventory'].find(x => x.id === li.itemId);
        if (item && type === 'sale') {
            const avail = getAvailableStock(item).available + li.qty;
            if (newQty > avail) {
                alert(`Cannot update to ${newQty} ${li.unit || 'Pcs'}. Only ${avail} available.`);
                return;
            }
        }
        li.qty = newQty;
        if (li.discountPct > 0) li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'price') { 
        li.price = Math.max(0, +value || 0); 
        if (li.discountPct > 0) li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'discountPct') {
        li.discountPct = Math.max(0, +value || 0);
        li.discountAmt = +( (li.qty * li.price) * (li.discountPct / 100) ).toFixed(2);
    }
    if (field === 'discountAmt') {
        li.discountAmt = Math.max(0, +value || 0);
        const lineVal = li.qty * li.price;
        li.discountPct = lineVal > 0 ? +( (li.discountAmt / lineVal) * 100 ).toFixed(2) : 0;
    }

    li.amount = +( (li.qty * li.price) - (li.discountAmt || 0) ).toFixed(2);
    
    // Recalculate GST for the line
    const gstRate = +(li.gstRate || 0);
    li.baseAmount = gstRate > 0 ? +(li.amount / (1 + gstRate / 100)).toFixed(2) : li.amount;
    li.taxAmount = +(li.amount - li.baseAmount).toFixed(2);

    // Price Alert Logic
    const unitPrice = li.qty > 0 ? li.amount / li.qty : 0;
    li._priceAlert = (type === 'sale' && unitPrice < (li.purchasePrice || 0) - 0.01);

    renderInvoiceLines();
}
function renderInvoiceLines() {
    const el = $('inv-lines-list'); if (!el) return;
    const invType = ($('f-inv-type')||{}).value;

    const header = `<div style="display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:2px solid var(--border);font-size:0.7rem;font-weight:700;color:var(--text-secondary);text-transform:uppercase;margin-bottom:4px;min-width:600px">
        <span style="width:20px;text-align:center">#</span>
        <span style="flex:1">Item</span>
        <span style="width:45px;text-align:center">Qty</span>
        <span style="width:25px;text-align:center">UOM</span>
        <span style="width:65px;text-align:right">Price</span>
        <span style="width:40px;text-align:center">Dis%</span>
        <span style="width:50px;text-align:center">Dis₹</span>
        <span style="width:75px;text-align:right">Amount</span>
        <span style="width:24px"></span>
    </div>`;

    el.innerHTML = header + invoiceItems.map((li, i) => {
        const edited   = li.listedPrice !== undefined && Math.abs(li.price - li.listedPrice) > 0.01;
        const gstLabel = li.gstRate ? `<span style="font-size:0.7rem;color:var(--text-muted)">Base ${currency(li.baseAmount||li.amount)} + GST ${li.gstRate}%: ${currency(li.taxAmount||0)}</span>` : '';
        const alertStyle = li._priceAlert ? 'background:rgba(239, 68, 68, 0.05); border-left:3px solid var(--danger); padding-left:5px' : (edited ? 'background:rgba(245,158,11,0.05); border-left:3px solid var(--warning); padding-left:5px' : '');

        return `<div style="padding:6px 0;border-bottom:1px solid var(--border);${alertStyle}">
            <div style="display:flex;align-items:center;gap:6px;min-width:600px">
                <span style="width:20px;text-align:center;font-size:0.75rem;color:var(--text-muted)">${i+1}</span>
                <div style="flex:1;min-width:0">
                    <div style="font-size:0.8rem;font-weight:600;word-break:break-word">${li.name}</div>
                    ${li._priceAlert ? `<div style="font-size:0.6rem;color:var(--danger);font-weight:700">⚠️ < ${currency(li.purchasePrice)}</div>` : ''}
                </div>
                <input type="number" value="${li.qty}" min="0.001" step="any" style="width:45px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateInvoiceLine(${i},'qty',this.value)">
                <span style="font-size:0.7rem;color:var(--text-muted);width:25px;text-align:center">${li.unit||'Pcs'}</span>
                <div style="width:65px;text-align:right">
                    ${edited ? `<div style="font-size:0.55rem;text-decoration:line-through;color:var(--text-muted)">${currency(li.listedPrice)}</div>` : ''}
                    <input type="number" value="${(+li.price).toFixed(2)}" min="0" step="0.01" style="width:65px;padding:4px 2px;border-radius:4px;border:1px solid ${edited ? 'var(--warning)' : 'var(--border)'};text-align:right;font-size:0.75rem;${edited?'color:var(--warning);font-weight:600':''}" onchange="updateInvoiceLine(${i},'price',this.value)">
                </div>
                <input type="number" value="${li.discountPct||0}" min="0" max="100" step="0.01" placeholder="%" title="Discount %" style="width:40px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateInvoiceLine(${i},'discountPct',this.value)">
                <input type="number" value="${li.discountAmt||0}" min="0" step="0.01" placeholder="₹" title="Discount ₹" style="width:50px;padding:4px 2px;border-radius:4px;border:1px solid var(--border);text-align:center;font-size:0.75rem" onchange="updateInvoiceLine(${i},'discountAmt',this.value)">
                <span style="width:75px;text-align:right;font-weight:700;font-size:0.8rem;color:${li._priceAlert?'var(--danger)':'inherit'}">${currency(li.amount)}</span>
                <button class="btn-icon" onclick="removeInvoiceLine(${i})" style="flex-shrink:0;color:var(--danger);width:24px">✕</button>
            </div>
            ${gstLabel ? `<div style="padding-left:32px;margin-top:2px">${gstLabel}</div>` : ''}
        </div>`;
    }).join('');
    // Auto round-off for sale invoices on every line change
    const invTypeEl = $('f-inv-type');
    if (invTypeEl && invTypeEl.value === 'sale') {
        const sub  = invoiceItems.reduce((s, li) => s + li.amount, 0);
        const roEl = $('f-inv-roundoff');
        if (roEl) roEl.value = +(Math.round(sub) - sub).toFixed(2);
    }
    updateInvoiceTotal();
}
function updateInvoiceTotal() {
    const sub      = invoiceItems.reduce((s, li) => s + li.amount, 0);
    const gst      = +(($('f-inv-gst')      || {}).value || 0);
    const roundoff = +(($('f-inv-roundoff') || {}).value || 0);
    const discPct  = +(($('f-inv-disc-pct') || {}).value || 0);
    const discAmt  = +(($('f-inv-disc-amt') || {}).value || 0);
    
    let totalDiscount = 0;
    if (discPct > 0) totalDiscount += (sub * discPct / 100);
    if (discAmt > 0) totalDiscount += discAmt;

    // Prices are GST-inclusive; total = subtotal + roundoff - global discounts
    let total = sub + roundoff - totalDiscount;
    total = Math.max(0, total);

    const el = $('inv-total-display'); if (!el) return;

    // Build GST breakdown from per-item rates first, then fall back to global rate
    const hasItemGst = invoiceItems.some(li => li.gstRate > 0);
    let taxableAmt, totalTax, rateLines;
    if (hasItemGst) {
        taxableAmt = invoiceItems.reduce((s, li) => s + (li.baseAmount || li.amount), 0);
        totalTax   = invoiceItems.reduce((s, li) => s + (li.taxAmount  || 0), 0);
        const rateMap = {};
        invoiceItems.forEach(li => {
            if (li.gstRate > 0) rateMap[li.gstRate] = (rateMap[li.gstRate] || 0) + (li.taxAmount || 0);
        });
        rateLines = Object.entries(rateMap).sort((a,b)=>+a[0]-+b[0])
            .map(([r, amt]) => `<span style="color:var(--text-muted)">GST ${r}% <i>(Incl.)</i>: <b>${currency(amt)}</b></span>`).join('');
    } else if (gst > 0) {
        // Global GST% — price is inclusive, so back-calculate
        taxableAmt = +(sub / (1 + gst / 100)).toFixed(2);
        totalTax   = +(sub - taxableAmt).toFixed(2);
        rateLines  = `<span style="color:var(--text-muted)">GST ${gst}% <i>(Incl.)</i>: <b>${currency(totalTax)}</b></span>`;
    }

    el.innerHTML = `
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;font-size:0.82rem;width:100%;text-align:right">
            ${taxableAmt !== undefined ? `<span style="color:var(--text-muted)">Taxable Amount: <b>${currency(taxableAmt)}</b></span>` : ''}
            ${rateLines || ''}
            ${totalTax  ? `<span style="color:var(--text-muted)">Total GST <i>(Included)</i>: <b>${currency(totalTax)}</b></span>` : ''}
            <span style="color:var(--text-muted);${(taxableAmt !== undefined)?'border-top:1px dashed var(--border);padding-top:3px;margin-top:2px;width:100%':''}">Subtotal: <b>${currency(sub)}</b></span>
            ${totalDiscount ? `<span style="color:var(--danger)">Discount: <b>-${currency(totalDiscount)}</b></span>` : ''}
            ${roundoff ? `<span style="color:var(--text-muted)">Round Off: <b style="color:${roundoff<0?'#ef4444':'#10b981'}">${roundoff>0?'+':''}${currency(roundoff)}</b></span>` : ''}
            <span style="color:var(--text-muted);border-top:1px dashed var(--border);padding-top:3px;margin-top:2px;width:100%"></span>
            <span style="font-size:1.15rem;font-weight:800;color:var(--accent)">Total: ${currency(total)}</span>
        </div>`;
}
function autoRoundOff() {
    // Prices are GST-inclusive; round off on subtotal directly
    const sub  = invoiceItems.reduce((s, li) => s + li.amount, 0);
    const diff = +(Math.round(sub) - sub).toFixed(2);
    const el   = $('f-inv-roundoff'); if (el) el.value = diff;
    updateInvoiceTotal();
}
async function saveInvoice() {
    if (!beginSave()) return;
    const pe = $('f-inv-party'); if (!pe.value) { endSave(); return alert('Select a party'); } if (!invoiceItems.length) { endSave(); return alert('Add items'); }

    const parties = DB.get('db_parties');
    let partyId = '';
    let partyName = pe.value;
    const match = pe.value.match(/^(.*) \[(.*)\]$/);
    if (match) {
        partyName = match[1].trim();
        const p = parties.find(x => x.name === partyName && (x.phone || '') === match[2].trim());
        if (p) partyId = p.id;
    } else {
        const p = parties.find(x => x.name.toLowerCase() === pe.value.trim().toLowerCase());
        if (p) partyId = p.id;
    }

    if (!partyId) return alert('Invalid party selected. Please select from the dropdown.');

    const invParty = parties.find(p => String(p.id) === String(partyId));
    if (invParty && invParty.blocked && ($('f-inv-type')||{}).value === 'sale') {
        return alert(`❌ "${invParty.name}" is blocked. Cannot create a sale invoice for a blocked customer. Contact admin to unblock.`);
    }

    const invType = ($('f-inv-type')||{}).value;
    if (invType === 'sale' && invParty && invParty.creditLimit > 0) {
        const sub2 = invoiceItems.reduce((s, li) => s + li.amount, 0);
        const gst2 = +($('f-inv-gst').value || 0);
        const ro2  = +(($('f-inv-roundoff')||{}).value || 0);
        const discPct = +(($('f-inv-disc-pct') || {}).value || 0);
        const discAmt = +(($('f-inv-disc-amt') || {}).value || 0);
        
        // Prices are GST-inclusive
        let invoiceTotal = sub2 + ro2;
        if (discPct > 0) invoiceTotal -= (sub2 * discPct / 100);
        if (discAmt > 0) invoiceTotal -= discAmt;
        invoiceTotal = Math.max(0, invoiceTotal);

        if (((invParty.balance || 0) + invoiceTotal) > invParty.creditLimit) {
            if (!confirm(`⚠️ Credit limit exceeded!\nCredit Limit: ${currency(invParty.creditLimit)}\nCurrent Balance: ${currency(invParty.balance || 0)}\nThis Invoice: ${currency(invoiceTotal)}\n\nProceed anyway?`)) { endSave(); return; }
        }
    }

    const invNo = $('f-inv-no').value.trim();
    const invoices = DB.get('db_invoices');
    if (invoices.find(i => i.invoiceNo === invNo)) return alert('Invoice number ' + invNo + ' already exists!');

    const sub  = invoiceItems.reduce((s, li) => s + li.amount, 0);
    const gst  = +($('f-inv-gst').value || 0);
    const type = $('f-inv-type').value;
    let roundoff = +(($('f-inv-roundoff')||{}).value || 0);
    const discPctGlobal = +(($('f-inv-disc-pct') || {}).value || 0);
    const discAmtGlobal = +(($('f-inv-disc-amt') || {}).value || 0);

    if (type === 'sale' && roundoff === 0) {
        let tempTotal = sub;
        if (discPctGlobal > 0) tempTotal -= (sub * discPctGlobal / 100);
        if (discAmtGlobal > 0) tempTotal -= discAmtGlobal;
        tempTotal = Math.max(0, tempTotal);
        roundoff = +(Math.round(tempTotal) - tempTotal).toFixed(2);
        
        const roEl = $('f-inv-roundoff'); if (roEl) roEl.value = roundoff;
        updateInvoiceTotal();
    }
    
    let total = sub + roundoff;
    if (discPctGlobal > 0) total -= (sub * discPctGlobal / 100);
    if (discAmtGlobal > 0) total -= discAmtGlobal;
    total = Math.max(0, total);
    const vyaparInvNo = type === 'sale' ? ($('f-vyapar-inv-no') ? $('f-vyapar-inv-no').value.trim() : '') : '';
    if (type === 'sale' && !vyaparInvNo) return alert('Vyapar Invoice No. is mandatory for sale invoices.');

    // Read fromOrder before modal closes
    const fromOrderId = ($('f-inv-from-order') || {}).value || '';

    try {
        const inventory = DB.get('db_inventory');
        const co2 = DB.getObj('db_company');

        // Stock availability checks
        if (type === 'sale') {
            const co2 = DB.getObj('db_company');
            if (co2 && !co2.allowNegativeStock) {
                const shortItems = [];
                await Promise.all(invoiceItems.map(async li => {
                    const item = inventory.find(x => x.id === li.itemId);
                    if (item) {
                        const avail = (await getAvailableStock(item)).available;
                        if (avail < li.qty) shortItems.push(`${li.name} (need ${li.qty}, available ${avail})`);
                    }
                }));

                if (shortItems.length) { 
                    endSave(); 
                    return alert('Insufficient stock:\n' + shortItems.join('\n')); 
                }
                for (const li of invoiceItems) {
                    const item = inventory.find(x => x.id === li.itemId);
                    if (item && (item.stock || 0) < li.qty) { 
                        endSave(); 
                        return alert(`Insufficient stock for "${li.name}".\nAvailable: ${item.stock || 0}, Required: ${li.qty}`); 
                    }
                }
            }
        }

        // Build all DB operations
        const ops = [];

        for (const li of invoiceItems) {
            const item = inventory.find(x => x.id === li.itemId);
            if (!item) continue;
            const qtyChange = type === 'sale' ? -li.qty : li.qty;
            const newStock = (item.stock || 0) + qtyChange;
            const itemUpdate = { stock: newStock };
            if (type === 'sale' && item.batches && item.batches.length) {
                const { updatedBatches, priceSync } = deductBatchQtyFifo(item, li.qty);
                if (updatedBatches) { itemUpdate.batches = updatedBatches; Object.assign(itemUpdate, priceSync); }
            }
            ops.push(DB.rawUpdate('inventory', item.id, itemUpdate));
            ops.push(DB.rawInsert('stock_ledger', {
                date: today(), itemId: item.id, itemName: item.name,
                entryType: type === 'sale' ? 'Sale' : 'Purchase', qty: qtyChange,
                runningStock: newStock, documentNo: invNo,
                reason: type === 'sale' ? 'Sale Invoice' : 'Purchase Invoice',
                createdBy: currentUser.name
            }));
        }

        // Expense Entry for Discount (only on posted invoice, not SO)
        const globalDiscountAmt = discAmtGlobal + (discPctGlobal > 0 ? (sub * discPctGlobal / 100) : 0);
        const totalDiscount = invoiceItems.reduce((s, li) => s + (li.discountAmt || 0), 0) + globalDiscountAmt;
        if (totalDiscount > 0 && type === 'sale') {
            ops.push(DB.rawInsert('expenses', {
                id: DB.id(),
                date: $('f-inv-date').value,
                category: 'Sales Discount',
                amount: +totalDiscount.toFixed(2),
                partyId: partyId,
                partyName: partyName,
                docNo: invNo,
                description: `Discount on ${invNo}`
            }));
        }

        // Party balance + ledger
        const party = parties.find(p => p.id === partyId);
        if (party) {
            const balChange = type === 'sale' ? total : -total;
            const newBal = (party.balance || 0) + balChange;
            ops.push(DB.rawUpdate('parties', party.id, { balance: newBal }));
            ops.push(DB.rawInsert('party_ledger', {
                date: today(), partyId: party.id, partyName: party.name,
                type: type === 'sale' ? 'Sale Invoice' : 'Purchase Invoice',
                amount: balChange, balance: newBal, docNo: invNo,
                notes: type === 'sale' ? 'Sale' : 'Purchase', createdBy: currentUser.name
            }));
        }

        // Resolve fromOrder number
        let fromOrderNo = '';
        if (fromOrderId) {
            const allOrders2 = DB.get('db_salesorders');
            const fo = allOrders2.find(x => x.id === fromOrderId);
            if (fo) fromOrderNo = fo.orderNo;
        }

        // Invoice record
        const dueDateVal = $('f-inv-due-date') ? $('f-inv-due-date').value : '';
        const invData = {
            invoiceNo: invNo, date: $('f-inv-date').value, dueDate: dueDateVal || null,
            type, partyId, partyName, items: [...invoiceItems],
            subtotal: sub, gst, roundOff: roundoff, total,
            discountPct: discPctGlobal,
            discountAmt: discAmtGlobal,
            status: fromOrderId ? 'from-packing' : 'created',
            createdBy: currentUser.name,
            ...(vyaparInvNo ? { vyaparInvoiceNo: vyaparInvNo } : {}),
            ...(fromOrderNo ? { fromOrder: fromOrderNo } : {})
        };
        ops.push(DB.rawInsert('invoices', invData));
        if (fromOrderId) ops.push(DB.rawUpdate('salesorders', fromOrderId, { invoiceNo: invNo }));

        // Advance payment allocations
        const advInputs = [...document.querySelectorAll('.inv-apply-adv-input')].filter(inp => +inp.value > 0);
        if (advInputs.length) {
            const payments = DB.get('db_payments');
            for (const inp of advInputs) {
                const pay = payments.find(p => p.id === inp.dataset.pay);
                if (pay) {
                    const allocs = { ...(pay.allocations || {}), [invNo]: (pay.allocations?.[invNo] || 0) + +inp.value };
                    ops.push(DB.rawUpdate('payments', pay.id, { allocations: allocs }));
                }
            }
        }

        // Execute all in parallel
        await Promise.all(ops);
        await DB.refreshTables(['invoices', 'inventory', 'parties', 'payments', 'sales_orders']);

        if (type === 'sale' && vyaparInvNo) incrementVyaparNo();

        const andNew = window._saveAndNew; window._saveAndNew = false;
        const savedType = invType;
        closeModal();
        if (fromOrderId) {
            await renderPacking();
        } else {
            await renderInvoices();
        }
        showToast(`Invoice ${invNo} saved!`, 'success');
        if (andNew && !fromOrderId) openInvoiceModal(savedType);
    } catch (err) {
        window._saveAndNew = false;
        endSave();
        alert('Error saving invoice: ' + err.message);
    }
}
async function openAssignInvoiceModal(invId) {
    const [invoices, users] = await Promise.all([DB.getAll('invoices'), DB.getAll('users')]);
    const inv = invoices.find(i => i.id === invId); if (!inv) return;
    const salesmen = users.filter(u => ['Salesman','Manager','Admin'].includes(u.role));
    openModal(`Assign Invoice ${inv.invoiceNo}`, `
        <div class="form-group">
            <label>Assigned To (Salesman / Collector)</label>
            <select id="f-assign-user">
                <option value="">-- Select --</option>
                ${salesmen.map(u => `<option value="${u.name}" ${inv.assignedTo === u.name ? 'selected' : ''}>${u.name} (${u.role})</option>`).join('')}
            </select>
        </div>
        <div class="form-group">
            <label>Handover Date</label>
            <input type="date" id="f-assign-date" value="${inv.handoverDate || today()}">
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveAssignInvoice('${invId}')">Assign</button>
        </div>
    `);
}

async function saveAssignInvoice(invId) {
    const assignedTo = $('f-assign-user').value;
    const handoverDate = $('f-assign-date').value;
    if (!assignedTo) return alert('Select a salesman');
    try {
        await DB.update('invoices', invId, { assignedTo, handoverDate });
        closeModal();
        await renderInvoices();
        showToast(`Invoice assigned to ${assignedTo}`, 'success');
    } catch(e) { alert('Error: ' + e.message); }
}

async function viewInvoice(id) {
    const [invoices, co, payments] = await Promise.all([
        DB.getAll('invoices'),
        DB.getObj('db_company'),
        DB.getAll('payments')
    ]);
    const i = invoices.find(x => x.id === id); if (!i) return;

    const paid = await getInvoicePaidAmount(i.invoiceNo);
    const relatedPayments = payments.filter(p => p.invoiceNo === i.invoiceNo || (p.allocations && p.allocations[i.invoiceNo]));

    openModal(`Invoice ${i.invoiceNo}`, `
        <div id="invoice-print-area">
        ${co.logo ? `<div style="text-align:center;margin-bottom:12px"><img src="${co.logo}" style="max-height:60px;max-width:200px;object-fit:contain" alt="Logo"></div>` : ''}
        ${co.name ? `<div style="text-align:center;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid var(--border)"><h3>${escapeHtml(co.name)}</h3><div style="font-size:0.8rem;color:var(--text-muted)">${escapeHtml(co.address || '')} ${co.city ? ', ' + escapeHtml(co.city) : ''} ${co.gstin ? ' | GSTIN: ' + escapeHtml(co.gstin) : ''}</div></div>` : ''}
        <div style="margin-bottom:14px"><strong>Date:</strong> ${fmtDate(i.date)} | <strong>${i.type === 'sale' ? 'Customer' : 'Supplier'}:</strong> ${escapeHtml(i.partyName)} | <span class="badge ${i.type === 'sale' ? 'badge-success' : 'badge-info'}">${i.type}</span> ${i.status === 'cancelled' ? '<span class="badge badge-danger">Cancelled</span>' : ''}${i.vyaparInvoiceNo ? ` | <strong style="color:var(--primary)">Vyapar No:</strong> <span style="font-weight:700;color:var(--primary)">${escapeHtml(i.vyaparInvoiceNo)}</span>` : ''}</div>
        <table class="data-table"><thead><tr><th>SL</th><th>Item</th><th>Qty</th><th>Rate</th><th>Amount</th></tr></thead>
        <tbody>${i.items.map((l, idx) => `<tr><td>${idx + 1}</td><td>${escapeHtml(l.name)}</td><td>${l.packedQty !== undefined ? l.packedQty : l.qty} <span style="font-size:0.75rem;color:var(--text-muted)">${l.unit || 'Pcs'}</span></td><td>${currency(l.price)}</td><td>${currency(l.amount)}</td></tr>`).join('')}
        ${i.gst ? `<tr><td colspan="4" style="text-align:right;font-size:0.85rem">Subtotal</td><td>${currency(i.subtotal)}</td></tr><tr><td colspan="4" style="text-align:right;font-size:0.85rem">GST (${i.gst}%)</td><td>${currency(i.subtotal * i.gst / 100)}</td></tr>` : ''}
        <tr style="font-weight:700"><td colspan="4" style="text-align:right;color:var(--accent)">Total</td><td style="color:var(--accent)">${currency(i.total)}</td></tr></table>
        
        ${relatedPayments.length ? `<div style="margin-top:20px;padding-top:15px;border-top:1px dashed var(--border)">
                <h4 style="margin-bottom:10px;font-size:0.95rem;color:var(--text-primary)">💰 Payment History</h4>
                <table class="data-table" style="font-size:0.85rem">
                    <thead style="background:var(--bg-input)"><tr><th>Date</th><th>Voucher #</th><th>Mode</th><th>Note</th><th>Amount</th></tr></thead>
                    <tbody>${relatedPayments.map(p => `<tr>
                        <td><a href="#" onclick="viewPaymentDetails('${p.id}');return false" style="color:var(--accent);text-decoration:underline">${fmtDate(p.date)}</a></td>
                        <td style="font-weight:600;color:var(--accent)">${p.payNo || '-'}</td>
                        <td>${p.mode}</td><td>${p.note || '-'}</td>
                        <td class="amount-green">${currency(p.invoiceNo === i.invoiceNo ? p.amount : (p.allocations && p.allocations[i.invoiceNo]))}</td>
                    </tr>`).join('')}
                    <tr style="font-weight:700;background:var(--bg-body)"><td colspan="4" style="text-align:right">Total Paid</td><td class="amount-green">${currency(paid)}</td></tr>
                    <tr style="font-weight:700;background:var(--bg-body)"><td colspan="4" style="text-align:right;color:var(--danger)">Balance Due</td><td style="color:var(--danger)">${currency(i.total - paid)}</td></tr>
                    </tbody>
                </table>
            </div>` : ''}
        
        ${(() => {
            const due = i.total - paid;
            if (due <= 0 || i.status === 'cancelled') return '';

            // Check for available advances
            let availAdvance = 0;
            const partyAdvances = payments.filter(p => p.partyId === i.partyId && (p.invoiceNo === 'Advance' || p.invoiceNo === 'Multi' || (!p.invoiceNo && p.type === 'in')));
            partyAdvances.forEach(a => {
                const used = a.allocations ? Object.values(a.allocations).reduce((s, val) => s + (+val), 0) : 0;
                const rem = a.amount - used;
                if (rem > 0) availAdvance += rem;
            });

            const advanceHtml = availAdvance > 0 ? `<button class="btn btn-outline btn-sm" onclick="allocateAdvanceToInvoice('${i.id}')" style="margin-top:6px;font-size:0.8rem">🔗 Adjust Advance Amount (${currency(availAdvance)} available)</button>` : '';

            return `<div style="margin-top:15px;padding:12px;background:var(--bg-body);border-radius:6px;border:1px solid var(--border)">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div><strong>Remaining Balance Due:</strong> <span style="font-size:1.1rem;font-weight:700;color:var(--danger)">${currency(due)}</span>
                    ${i.allocatedTo ? `<div style="font-size:0.8rem;color:var(--text-muted);margin-top:4px">Assigned to: <b style="color:var(--warning)">${escapeHtml(i.allocatedTo)}</b></div>` : ''}
                    </div>
                    <div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end">
                        <button class="btn btn-outline btn-sm" onclick="openAssignCollectorModal('${i.id}')" style="font-size:0.8rem">👤 Assign Collector</button>
                        ${advanceHtml}
                    </div>
                </div>
            </div>`;
        })()}
        </div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button><button class="btn btn-primary" onclick="printInvoice()">🖨️ Print</button></div>`);
}

async function openAssignCollectorModal(invId) {
    const users = await DB.getAll('users');
    const collectors = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
    
    openModal('Assign Collector', `
        <div class="form-group" style="margin-bottom:20px">
            <label>Select Staff / Salesman</label>
            <select id="f-assign-collector">
                <option value="">-- Remove Assignment --</option>
                ${collectors.map(u => `<option value="${u.name}">${u.name} (${u.role})</option>`).join('')}
            </select>
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="executeAssignCollector('${invId}')">Save Assignment</button>
        </div>
    `);
}

async function executeAssignCollector(invId) {
    const collector = $('f-assign-collector').value;
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === invId);
    if (!inv) return;
    
    const historyEntry = {
        date: new Date().toISOString(),
        assignedTo: collector || 'Unassigned',
        assignedBy: currentUser ? currentUser.name : 'System'
    };
    
    const allocationHistory = Array.isArray(inv.allocationHistory) ? inv.allocationHistory : [];
    allocationHistory.push(historyEntry);
    
    await DB.update('invoices', invId, { allocatedTo: collector || null, allocationHistory });
    showToast(collector ? 'Invoice assigned to ' + collector : 'Assignment removed', 'success');
    closeModal();
    if (currentPage === 'invoices') renderInvoices();
}

function cancelInvoiceDirectly(id) {
    const invoices = DB.get('db_invoices');
    const inv = invoices.find(i => i.id === id);
    if (!inv || inv.status === 'cancelled') return;
    openModal('Cancel Invoice', `
        <div style="margin-bottom:14px;padding:12px;background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:8px;font-size:0.9rem">
            <strong>Invoice:</strong> ${inv.invoiceNo}<br>
            <strong>Party:</strong> ${inv.partyName} | <strong>Amount:</strong> ${currency(inv.total)}
        </div>
        <p style="margin-bottom:16px;font-size:0.9rem">Cancel this invoice? Stock will be restored and party balance adjusted.</p>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Keep Invoice</button>
        <button class="btn btn-danger" onclick="executeCancelInvoice('${id}')">❌ Cancel Invoice</button></div>`);
}
async function executeCancelInvoice(id) {
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === id);
    if (!inv) { closeModal(); return; }
    if (inv.status === 'cancelled') { closeModal(); alert('Invoice is already cancelled.'); return; }

    try {
        // Restore stock
        const inventory = await DB.getAll('inventory');
        for (const li of (inv.items || [])) {
            const item = inventory.find(x => x.id === li.itemId);
            if (item) {
                // BUG-018 fix: use packedQty if available (partial packing), else fall back to qty
                const effectiveQty = li.packedQty !== undefined ? li.packedQty : li.qty;
                const qtyChange = inv.type === 'sale' ? effectiveQty : -effectiveQty;
                const newStock = (item.stock || 0) + qtyChange;
                const itemUpdate = { stock: newStock };
                // Restore batch quantities for sale cancellation (reverse FIFO)
                if (inv.type === 'sale' && item.batches && item.batches.length) {
                    const batches = JSON.parse(JSON.stringify(item.batches));
                    // Add back to the newest active batch (or last batch)
                    const active = batches.filter(b => b.isActive !== false).sort((a, b) => (a.receivedDate||'') < (b.receivedDate||'') ? -1 : 1);
                    const target = active[active.length - 1] || batches[batches.length - 1];
                    if (target) target.qty = (target.qty || 0) + effectiveQty;
                    itemUpdate.batches = batches;
                    Object.assign(itemUpdate, syncItemPricesFromBatches(batches));
                }
                await DB.update('inventory', item.id, itemUpdate);
                await addLedgerEntry(item.id, item.name, inv.type === 'sale' ? 'Sale Return' : 'Purchase Return', qtyChange, inv.invoiceNo, 'Invoice Cancelled');
            }
        }

        // Reverse party balance
        const parties = await DB.getAll('parties');
        const party = parties.find(p => p.id === inv.partyId);
        if (party) {
            const balChange = inv.type === 'sale' ? -inv.total : inv.total;
            const newBal = (party.balance || 0) + balChange;
            await DB.update('parties', party.id, { balance: newBal });
            await addPartyLedgerEntry(party.id, party.name, inv.type === 'sale' ? 'Sale Cancel' : 'Purchase Cancel', balChange, inv.invoiceNo, 'Invoice Cancelled');
        }

        // Mark invoice as cancelled
        await DB.update('invoices', inv.id, { status: 'cancelled', cancelledAt: today() });

        // Reset associated sales order
        if (inv.fromOrder) {
            const orders = await DB.getAll('salesorders');
            const order = orders.find(o => o.orderNo === inv.fromOrder);
            if (order) {
                await DB.update('salesorders', order.id, {
                    packed: false, packedBy: null, packedAt: null,
                    invoiceNo: null, packedItems: null, packedTotal: null,
                    invoiceCancelled: true
                });
            }
        }

        // Cancel delivery records
        const dels = await DB.getAll('delivery');
        for (const d of dels) {
            if (d.invoiceNo === inv.invoiceNo && d.status !== 'Delivered' && d.status !== 'Cancelled') {
                await DB.update('delivery', d.id, { status: 'Cancelled', cancelReason: 'Invoice cancelled' });
            }
        }

        closeModal();
        await renderInvoices();
        showToast('Invoice ' + inv.invoiceNo + ' cancelled!', 'warning');
    } catch (err) {
        alert('Error cancelling invoice: ' + err.message);
    }
}
async function deleteInvoice(id) {
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === id);
    if (!inv) return;
    if (!confirm('Delete this invoice permanently? Effects will be reversed.')) return;

    try {
        if (inv.status !== 'cancelled') {
            await executeCancelInvoice(id);
        }
        await DB.delete('invoices', id);
        await renderInvoices();
        showToast('Invoice deleted!', 'warning');
    } catch (err) {
        alert('Error deleting invoice: ' + err.message);
    }
}

// --- Print Invoice ---
function printInvoice() {
    const printArea = document.getElementById('invoice-print-area');
    if (!printArea) return;
    const printWindow = window.open('', '_blank', 'width=800,height=600');
    // BUG-022 fix: force white background and dark text so dark-mode elements print correctly
    printWindow.document.write(`<html><head><title>Print Invoice</title>
        <style>*{background:#fff!important;color:#000!important;border-color:#ccc!important;box-shadow:none!important}body{font-family:Inter,Arial,sans-serif;padding:30px}table{width:100%;border-collapse:collapse;margin-top:16px}th,td{border:1px solid #ddd!important;padding:8px 12px;text-align:left;font-size:0.9rem}th{background:#f0f0f0!important;font-weight:600}tr:last-child td{font-weight:700}.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.8rem;background:#eee!important}h3{margin:0 0 4px 0}.amount-green{color:#16a34a!important}.amount-red{color:#dc2626!important}@media print{body{padding:20px}}</style>
    </head><body>${printArea.innerHTML}</body></html>`);
    printWindow.document.close();
    printWindow.focus();
    setTimeout(() => { printWindow.print(); printWindow.close(); }, 400);
}

// =============================================
//  PAYMENTS
// =============================================
async function renderPayments() {
    const payments = await DB.getAll('payments');
    const isSalesman = currentUser.role === 'Salesman';
    // Salesman sees only their own payments
    const visiblePayments = isSalesman
        ? payments.filter(p => p.collectedBy === currentUser.name || p.createdBy === currentUser.name)
        : payments;
    const totalIn = visiblePayments.filter(p => p.type === 'in').reduce((s, p) => s + p.amount, 0);
    const totalOut = visiblePayments.filter(p => p.type === 'out').reduce((s, p) => s + p.amount, 0);

    // Mode-wise breakup for Payment In
    const modeBreakup = {};
    visiblePayments.filter(p => p.type === 'in').forEach(p => {
        const m = p.mode || 'Cash';
        modeBreakup[m] = (modeBreakup[m] || 0) + p.amount;
    });
    const modeChips = Object.entries(modeBreakup).map(([m, a]) =>
        `<div class="pay-mode-chip"><span>${m}</span><strong>${currency(a)}</strong></div>`).join('');

    const today1 = today();
    const monthStart = today1.substring(0, 8) + '01';

    pageContent.innerHTML = `
        <div class="stats-grid" style="margin-bottom:14px" id="pay-stat-tiles">
            <div class="stat-card green"><div class="stat-icon">📥</div><div class="stat-value" id="pay-stat-in">${currency(totalIn)}</div><div class="stat-label">Payment In</div></div>
            ${!isSalesman ? `<div class="stat-card red"><div class="stat-icon">📤</div><div class="stat-value" id="pay-stat-out">${currency(totalOut)}</div><div class="stat-label">Payment Out</div></div>` : ''}
        </div>
        <div id="pay-mode-bar-wrap" style="margin-bottom:14px">${Object.keys(modeBreakup).length > 0 ? `
        <div class="pay-summary-bar">
            <div class="pay-summary-total">
                <span class="pay-sum-label">Mode Breakup (In)</span>
                <span class="pay-sum-value" id="pay-mode-total">${currency(totalIn)}</span>
            </div>
            <div class="pay-summary-modes" id="pay-mode-chips">${modeChips}</div>
        </div>` : ''}</div>
        <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:12px">
            <!-- Row 1: Date range (flex so inputs shrink properly on Android) -->
            <div style="display:flex;gap:8px">
                <div style="flex:1;min-width:0"><label style="font-size:0.7rem;color:var(--text-muted);display:block;margin-bottom:2px">From</label>
                    <input type="date" id="pay-f-from" value="${monthStart}" onchange="filterPayTable()" style="width:100%;min-width:0;box-sizing:border-box"></div>
                <div style="flex:1;min-width:0"><label style="font-size:0.7rem;color:var(--text-muted);display:block;margin-bottom:2px">To</label>
                    <input type="date" id="pay-f-to" value="${today1}" onchange="filterPayTable()" style="width:100%;min-width:0;box-sizing:border-box"></div>
            </div>
            <!-- Row 2: Search + Columns -->
            <div style="display:flex;gap:8px;align-items:center">
                <input class="search-box" id="pay-search" placeholder="Search..." oninput="filterPayTable()" style="flex:1;min-width:0;margin:0">
                <button class="btn btn-outline" onclick="openColumnPersonalizer('payments','renderPayments')" style="border-color:var(--accent);color:var(--accent);white-space:nowrap;flex-shrink:0">⚙️ Columns</button>
            </div>
            <!-- Row 3: Type + Mode filters -->
            <div style="display:flex;gap:8px">
                <select id="pay-type-filter" onchange="filterPayTable()" style="flex:1;min-width:0"><option value="">All Types</option><option value="in">Payment In</option><option value="out">Payment Out</option></select>
                <select id="pay-mode-filter" onchange="filterPayTable()" style="flex:1;min-width:0"><option value="">All Modes</option><option>Cash</option><option>UPI</option><option>Cheque</option><option>Bank Transfer</option></select>
            </div>
            ${!isSalesman ? `<!-- Row 4: Collector + Record button -->
            <div style="display:flex;gap:8px;align-items:center">
                <select id="pay-collector-filter" onchange="filterPayTable()" style="flex:1;min-width:0"><option value="">All Collectors</option>${[...new Set(visiblePayments.map(p=>p.collectedBy||p.createdBy).filter(Boolean))].map(n=>`<option>${n}</option>`).join('')}</select>
                <button class="btn btn-primary" onclick="openPaymentModal()" style="white-space:nowrap;flex-shrink:0">+ Record</button>
            </div>` : `<button class="btn btn-primary" onclick="openPaymentModal()" style="width:100%">+ Record Payment</button>`}
        </div>
        <div class="card" style="margin-top:12px"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>${ColumnManager.get('payments').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="pay-tbody">${renderPayRows(visiblePayments)}</tbody></table>
            </div>
        </div></div>`;
}
function buildPayInvoiceCell(p) {
    // Multi-invoice allocation — show each invoice + amount
    if (p.allocations && Object.keys(p.allocations).length > 0) {
        const chips = Object.entries(p.allocations).map(([inv, amt]) =>
            `<a href="#" onclick="viewInvoiceByNo('${inv}');return false"
                style="display:inline-flex;align-items:center;gap:4px;background:rgba(249,115,22,0.1);
                border:1px solid rgba(249,115,22,0.3);border-radius:6px;padding:2px 7px;
                font-size:0.72rem;font-weight:600;color:var(--primary);text-decoration:none;
                margin:2px;white-space:nowrap" title="View Invoice">
                ${escapeHtml(inv)} <span style="color:var(--text-muted)">₹${(+amt).toFixed(0)}</span>
            </a>`
        ).join('');
        const unallocated = p.amount - Object.values(p.allocations).reduce((s, v) => s + (+v), 0);
        const unallocBadge = unallocated > 0.01
            ? `<span style="display:inline-block;font-size:0.7rem;color:var(--success);margin-top:2px">+₹${unallocated.toFixed(0)} unallocated</span>`
            : '';
        return `<div style="line-height:1.8">${chips}${unallocBadge}</div>`;
    }
    // Single invoice
    if (p.invoiceNo && p.invoiceNo !== 'Advance') {
        return `<a href="#" onclick="viewInvoiceByNo('${p.invoiceNo}');return false" class="badge badge-info" style="cursor:pointer;color:white;text-decoration:none">${escapeHtml(p.invoiceNo)}</a>`;
    }
    // Advance / unlinked
    if (p.invoiceNo === 'Advance') {
        return `<span class="badge badge-warning">Advance</span>`;
    }
    return canEdit() && p.type === 'in'
        ? `<button class="btn-icon" style="font-size:0.7rem;color:var(--accent)" onclick="openLinkInvoiceModal('${p.id}')" title="Link Invoice">🔗 Link</button>`
        : '<span style="color:var(--text-muted)">—</span>';
}

function renderPayRows(pays) {
    if (!pays.length) return '<tr><td colspan="8" class="empty-state"><p>No payments found</p></td></tr>';
    const cols = ColumnManager.get('payments').filter(c => c.visible);
    return pays.map(p => {
        const editBtn = canEdit() ? `<button class="btn-icon" onclick="openEditPaymentModal('${p.id}')" title="Edit">✏️</button>` : '';
        const cellMap = {
            date:        `<td>${fmtDate(p.date)}</td>`,
            receiptNo:   `<td style="font-weight:600;color:var(--accent)">${p.payNo || (p.id ? p.id.substring(0,8) : '-')}</td>`,
            party:       `<td style="font-weight:600">${escapeHtml(p.partyName)}</td>`,
            type:        `<td><span class="badge ${p.type === 'in' ? 'badge-success' : 'badge-danger'}">${p.type === 'in' ? 'Payment In' : 'Payment Out'}</span></td>`,
            invoiceNo:   `<td>${buildPayInvoiceCell(p)}</td>`,
            mode:        `<td>${p.mode || 'Cash'}${p.mode === 'Cheque' && p.chequeNo ? `<br><span style="font-size:0.75rem;color:var(--text-muted)">#${p.chequeNo} | ${p.chequeBank || ''}</span><br><span class="badge ${p.chequeStatus === 'Cleared' ? 'badge-success' : p.chequeStatus === 'Deposited' ? 'badge-warning' : 'badge-danger'}" style="font-size:0.65rem">${p.chequeStatus || 'Pending'}</span>` : ''}</td>`,
            collectedBy: `<td style="font-size:0.82rem;color:var(--text-secondary)">${escapeHtml(p.collectedBy || p.createdBy || '-')}</td>`,
            amount:      `<td class="${p.type === 'in' ? 'amount-green' : 'amount-red'}">${currency(p.amount)}</td>`,
            actions:     `<td><div class="action-btns">${editBtn}${canEdit() ? `<button class="btn-icon" onclick="deletePayment('${p.id}')" title="Delete Payment">🗑️</button>` : '—'}</div></td>`,
        };
        return `<tr>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
async function filterPayTable() {
    const s = ($('pay-search') || {}).value?.toLowerCase() || '';
    const t = ($('pay-type-filter') || {}).value || '';
    const modeF = ($('pay-mode-filter') || {}).value || '';
    const collF = ($('pay-collector-filter') || {}).value || '';
    const from = ($('pay-f-from') || {}).value || '';
    const to = ($('pay-f-to') || {}).value || '';
    let pays = await DB.getAll('payments');
    if (currentUser && currentUser.role === 'Salesman') {
        pays = pays.filter(p => (p.collectedBy === currentUser.name || p.createdBy === currentUser.name) && p.type !== 'out');
    }
    if (from) pays = pays.filter(p => p.date >= from);
    if (to) pays = pays.filter(p => p.date <= to);
    if (s) pays = pays.filter(p => (p.partyName||'').toLowerCase().includes(s) || (p.note||'').toLowerCase().includes(s) || (p.invoiceNo||'').toLowerCase().includes(s));
    if (t) pays = pays.filter(p => p.type === t);
    if (modeF) pays = pays.filter(p => (p.mode||'Cash') === modeF);
    if (collF) pays = pays.filter(p => (p.collectedBy || p.createdBy) === collF);
    $('pay-tbody').innerHTML = renderPayRows(pays);

    // Update stat tiles
    const filtIn = pays.filter(p => p.type === 'in').reduce((s, p) => s + p.amount, 0);
    const filtOut = pays.filter(p => p.type === 'out').reduce((s, p) => s + p.amount, 0);
    if ($('pay-stat-in')) $('pay-stat-in').textContent = currency(filtIn);
    if ($('pay-stat-out')) $('pay-stat-out').textContent = currency(filtOut);

    // Update mode breakup bar
    const mb = {};
    pays.filter(p => p.type === 'in').forEach(p => { const m = p.mode || 'Cash'; mb[m] = (mb[m] || 0) + p.amount; });
    const chips = Object.entries(mb).map(([m, a]) => `<div class="pay-mode-chip"><span>${m}</span><strong>${currency(a)}</strong></div>`).join('');
    const wrap = $('pay-mode-bar-wrap');
    if (wrap) {
        wrap.innerHTML = Object.keys(mb).length > 0 ? `
        <div class="pay-summary-bar">
            <div class="pay-summary-total">
                <span class="pay-sum-label">Mode Breakup (In)</span>
                <span class="pay-sum-value" id="pay-mode-total">${currency(filtIn)}</span>
            </div>
            <div class="pay-summary-modes" id="pay-mode-chips">${chips}</div>
        </div>` : '';
    }
}

function viewInvoiceByNo(invoiceNo) {
    const inv = DB.get('db_invoices').find(i => i.invoiceNo === invoiceNo);
    if (inv) viewInvoice(inv.id);
}

async function viewPaymentDetails(id) {
    const payments = await DB.getAll('payments');
    const p = payments.find(x => x.id === id);
    if (!p) return;
    openModal('Payment Receipt', `
        <div style="background:var(--bg-card);padding:20px;border-radius:var(--radius-md);border:1px solid var(--border)">
            <div style="display:flex;justify-content:space-between;margin-bottom:15px;border-bottom:1px solid var(--border);padding-bottom:10px">
                <div>
                    <h3 style="margin:0;font-size:1.1rem">${p.partyName}</h3>
                    <div style="font-size:0.85rem;color:var(--text-muted)">Date: ${fmtDate(p.date)}</div>
                </div>
                <div style="text-align:right">
                    <span class="badge ${p.type === 'in' ? 'badge-success' : 'badge-danger'}">${p.type === 'in' ? 'Payment In' : 'Payment Out'}</span>
                </div>
            </div>
            <table style="width:100%;font-size:0.9rem;border-collapse:collapse;margin-bottom:15px">
                <tr style="border-bottom:1px dashed var(--border)"><td style="padding:8px 0;color:var(--text-secondary)">Payment Mode</td><td style="padding:8px 0;text-align:right;font-weight:600">${p.mode || 'Cash'}</td></tr>
                <tr style="border-bottom:1px dashed var(--border)"><td style="padding:8px 0;color:var(--text-secondary)">Amount</td><td style="padding:8px 0;text-align:right;font-weight:700;font-size:1.1rem;color:${p.type === 'in' ? 'var(--success)' : 'var(--danger)'}">${currency(p.amount)}</td></tr>
                <tr style="border-bottom:1px dashed var(--border)"><td style="padding:8px 0;color:var(--text-secondary);vertical-align:top">Invoice(s)</td><td style="padding:8px 0;text-align:right">
                    ${p.allocations && Object.keys(p.allocations).length > 0
                        ? `<table style="width:100%;font-size:0.82rem;border-collapse:collapse">
                            ${Object.entries(p.allocations).map(([inv, amt]) => `
                            <tr>
                                <td style="padding:2px 0"><a href="#" onclick="viewInvoiceByNo('${inv}');return false" style="color:var(--primary);font-weight:600">${escapeHtml(inv)}</a></td>
                                <td style="text-align:right;font-weight:700;color:var(--success)">₹${(+amt).toFixed(2)}</td>
                            </tr>`).join('')}
                            ${(() => { const used = Object.values(p.allocations).reduce((s,v)=>s+(+v),0); const rem = p.amount - used; return rem > 0.01 ? `<tr><td style="padding:2px 0;color:var(--text-muted)">Unallocated</td><td style="text-align:right;color:var(--accent);font-weight:700">₹${rem.toFixed(2)}</td></tr>` : ''; })()}
                          </table>`
                        : (p.invoiceNo && p.invoiceNo !== 'Advance'
                            ? `<a href="#" onclick="viewInvoiceByNo('${p.invoiceNo}');return false" style="color:var(--accent);text-decoration:underline">${escapeHtml(p.invoiceNo)}</a>`
                            : `<span style="color:var(--text-muted)">${p.invoiceNo === 'Advance' ? 'Advance' : 'Unlinked'}</span>`)}
                </td></tr>
                <tr><td style="padding:8px 0;color:var(--text-secondary)">Note</td><td style="padding:8px 0;text-align:right">${p.note || '-'}</td></tr>
            </table>
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Close</button>
            <button class="btn btn-primary" onclick="printPaymentReceipt()">🖨️ Print Receipt</button>
        </div>
    `);
}
function printPaymentReceipt() {
    const area = document.querySelector('#modal-body');
    if (!area) return;
    const co = DB.getObj('db_company') || {};
    const header = co.name ? `<div style="text-align:center;margin-bottom:12px"><h2 style="margin:0">${escapeHtml(co.name)}</h2>${co.address?`<div style="font-size:0.85rem">${escapeHtml(co.address)}${co.city?', '+escapeHtml(co.city):''}</div>`:''}${co.phone?`<div style="font-size:0.85rem">Ph: ${escapeHtml(co.phone)}</div>`:''}</div><hr>` : '';
    const w = window.open('', '_blank');
    w.document.write(`<!DOCTYPE html><html><head><title>Payment Receipt</title><style>body{font-family:sans-serif;padding:20px;max-width:400px;margin:0 auto}table{width:100%;border-collapse:collapse}td{padding:8px 4px;border-bottom:1px dashed #eee}hr{border:none;border-top:1px solid #ccc}@media print{button{display:none}}</style></head><body>${header}${area.innerHTML}<button onclick="window.print()" style="margin-top:16px;padding:8px 20px;background:#f97316;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:1rem">🖨️ Print</button></body></html>`);
    w.document.close();
}

function _initPayPartyDropdown(parties, filterType) {
    let filtered = filterType ? parties.filter(p => p.type === filterType) : parties;

    // Search object list
    const customPartySearchList = (pts) => pts.map(p => ({
        id: p.id,
        label: p.name + (p.blocked ? ' (Blocked)' : ''),
        value: p.name,
        code: '',
        stockText: p.phone || '',
        searchText: (p.name + ' ' + (p.phone || ''))
    }));

    const sortAndInit = (sortedParties) => {
        initSearchDropdown('f-pay-party', customPartySearchList(sortedParties), (party) => {
            if ($('f-pay-party-id')) $('f-pay-party-id').value = party.id || '';
            onPayPartyChange();
        });
    };

    if (window._userCoords) {
        const withDist = filtered.map(p => {
            const lat = parseFloat(p.lat || 0);
            const lng = parseFloat(p.lng || 0);
            const dist = (lat && lng) ? haversine(window._userCoords.lat, window._userCoords.lng, lat, lng) : 99999;
            return { ...p, _dist: dist };
        });
        withDist.sort((a, b) => a._dist - b._dist);
        sortAndInit(withDist);
    } else {
        const sorted = [...filtered].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
        sortAndInit(sorted);
    }
}
async function openPaymentModal(prefillPartyId) {
    await ensureGeolocation();
    // Hide FAB — full-page form has its own footer buttons
    const fab = $('app-fab');
    if (fab) fab.classList.add('hidden');

    // Render as full page (Vyapar-style) instead of a modal bottom sheet
    const [parties, users] = await Promise.all([DB.getAll('parties'), DB.getAll('users')]);
    const collectors = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
    const co = DB.ls.getObj('db_company') || {};
    const isSalesmanRole = currentUser.role === 'Salesman';

    pageContent.innerHTML = `
        <!-- Sticky top bar -->
        <div class="pay-page-header">
            <button class="btn-icon pay-back-btn" onclick="renderPayments()">←</button>
            <div style="flex:1">
                <div style="font-size:1rem;font-weight:700;color:var(--text-primary)">Record Payment</div>
                <div style="font-size:0.75rem;color:var(--text-muted)" id="pay-co-name">${escapeHtml(co.name || '')}</div>
            </div>
            <div style="display:flex;gap:6px">
                <button class="btn btn-outline btn-sm" style="padding:6px 10px" onclick="onPayTypeChange()" id="pay-type-toggle-btn" title="Switch type">
                    <span id="pay-type-label">💰 Payment In</span>
                </button>
            </div>
        </div>

        <!-- Hidden type/party-type state -->
        <input type="hidden" id="f-pay-type" value="in">
        <input type="hidden" id="f-pay-party-type" value="Customer">
        <input type="hidden" id="f-pay-party-id" value="">

        <!-- Party section -->
        <div class="pay-section">
            <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">PARTY</div>
            <div id="pay-party-balance-row" style="display:none;margin-bottom:10px;padding:10px 14px;background:rgba(16,185,129,0.07);border:1px solid rgba(16,185,129,0.2);border-radius:10px;justify-content:space-between;align-items:center">
                <span style="font-size:0.82rem;color:var(--text-muted)">Party Balance</span>
                <span style="font-size:1rem;font-weight:700" id="pay-party-balance-val">₹0.00</span>
            </div>
            <div class="form-group" style="margin-bottom:0">
                <input id="f-pay-party" placeholder="Search customer name..." autocomplete="off" style="font-size:1rem;font-weight:500">
            </div>
        </div>

        <!-- Amount & Mode section -->
        <div class="pay-section">
            <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">AMOUNT & MODE</div>
            <!-- Payment Modes / Split Payment -->
            <div id="pay-modes-container" style="margin-bottom:12px">
                <div class="pay-mode-row" style="display:grid;grid-template-columns:1fr 120px 40px;gap:8px;margin-bottom:8px;align-items:end">
                    <div class="form-group" style="margin-bottom:0">
                        <label style="font-size:0.75rem">Mode</label>
                        <select class="f-pay-row-mode" onchange="onPayModeChange()"><option>Cash</option><option>UPI</option><option>Bank Transfer</option><option>Cheque</option></select>
                    </div>
                    <div class="form-group" style="margin-bottom:0">
                        <label style="font-size:0.75rem">Amount ₹</label>
                        <input type="number" class="f-pay-row-amount" placeholder="0.00" oninput="onPayAmountChange()">
                    </div>
                    <div style="height:38px"></div> <!-- Spacer for delete button alignment -->
                </div>
            </div>
            <button class="btn btn-outline btn-sm" onclick="addPaymentModeRow()" style="margin-bottom:14px;width:100%;border-style:dashed">+ Add Another Mode (Split Payment)</button>

            <div style="margin-bottom:14px">
                <div style="font-size:0.78rem;color:var(--text-muted);margin-bottom:6px">Total Discount ₹</div>
                <input type="number" id="f-pay-discount" min="0" placeholder="0.00"
                    style="font-size:1.2rem;font-weight:600;color:var(--text-secondary);border:none;border-bottom:1px solid var(--border);background:transparent;width:100%;padding:4px 0;outline:none"
                    oninput="onPayAmountChange()">
            </div>
            <div style="margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-top:1px dashed var(--border)">
                <span style="font-size:0.9rem;font-weight:700;color:var(--text-muted)">Total Received:</span>
                <span style="font-size:1.2rem;font-weight:800;color:var(--primary)" id="pay-total-received-display">₹0.00</span>
            </div>
            <div style="margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;padding-top:4px">
                <span style="font-size:0.9rem;font-weight:700;color:var(--text-muted)">Total Balance Reduction:</span>
                <span style="font-size:1.2rem;font-weight:800;color:var(--success)" id="pay-total-display">₹0.00</span>
            </div>
            <div id="pay-qr-box" style="text-align:center;margin:10px 0;display:none;"></div>
            <div id="pay-cheque-fields" style="display:none;">
                <div class="form-row"><div class="form-group"><label>Cheque No *</label><input id="f-pay-cheque-no" placeholder="e.g. 123456"></div>
                <div class="form-group"><label>Bank Name *</label><input id="f-pay-cheque-bank" placeholder="e.g. SBI"></div></div>
                <div class="form-group"><label>Cheque Date *</label><input type="date" id="f-pay-cheque-date" value="${today()}"></div>
            </div>
        </div>

        <!-- Invoice allocation section -->
        <div id="pay-invoice-section" class="pay-section" style="display:none"></div>

        <!-- Details section -->
        <div class="pay-section">
            <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">DETAILS</div>
            <div class="form-row" style="margin-bottom:12px">
                <div class="form-group"><label>Date</label><input type="date" id="f-pay-date" value="${today()}"></div>
                <div class="form-group"><label>Collected By</label>
                    <select id="f-pay-collected-by">
                        <option value="${currentUser.name}">${currentUser.name} (Me)</option>
                        ${collectors.filter(u=>u.name!==currentUser.name).map(u=>`<option value="${u.name}">${u.name} (${u.role})</option>`).join('')}
                    </select>
                </div>
            </div>
            <div class="form-group" style="margin-bottom:0">
                <label>Note / Reference</label>
                <input id="f-pay-note" placeholder="e.g. Cash collected at shop">
            </div>
        </div>

        <!-- Bottom spacer so sticky footer doesn't cover last input -->
        <div style="height:80px"></div>

        <!-- Sticky save footer -->
        <div class="pay-page-footer">
            ${!isSalesmanRole ? `<button class="btn btn-outline" style="flex:1;min-height:48px" onclick="renderPayments()">Cancel</button>` : ''}
            <button class="btn btn-outline btn-save-new" id="btn-save-new" onclick="window._saveAndNew=true;savePayment()">＋ Save & New</button>
            <button class="btn btn-primary" id="btn-save-payment" style="flex:2;min-height:48px;font-size:1rem;font-weight:700" onclick="savePayment()">💾 Save Payment</button>
        </div>
    `;

    // Scroll to top of page so user sees the form from the beginning
    window.scrollTo({ top: 0, behavior: 'instant' });
    if (pageContent) pageContent.scrollTop = 0;

    // Init party search dropdown (sorted by GPS proximity)
    _initPayPartyDropdown(parties, 'Customer');

    // Pre-fill party if provided (e.g. from invoice "Receive Payment" button)
    if (prefillPartyId) {
        const party = parties.find(p => p.id === prefillPartyId);
        if (party) {
            const inp = $('f-pay-party');
            if (inp) { inp.value = party.name; inp.dataset.selectedId = party.id; }
            $('f-pay-party-id').value = party.id;
            onPayPartyChange();
        }
    }

    updatePaymentQR();
}

// Payment Mode Row Helpers
window.addPaymentModeRow = function() {
    const container = $('pay-modes-container');
    const div = document.createElement('div');
    div.className = 'pay-mode-row';
    div.style = 'display:grid;grid-template-columns:1fr 120px 40px;gap:8px;margin-bottom:8px;align-items:end';
    div.innerHTML = `
        <div class="form-group" style="margin-bottom:0">
            <select class="f-pay-row-mode" onchange="onPayModeChange()"><option>Cash</option><option>UPI</option><option>Bank Transfer</option><option>Cheque</option></select>
        </div>
        <div class="form-group" style="margin-bottom:0">
            <input type="number" class="f-pay-row-amount" placeholder="0.00" oninput="onPayAmountChange()">
        </div>
        <button class="btn-icon" onclick="this.parentNode.remove();onPayAmountChange()" style="height:38px;color:var(--danger)">🗑️</button>
    `;
    container.appendChild(div);
};

window.onPayAmountChange = function() {
    let totalReceived = 0;
    document.querySelectorAll('.f-pay-row-amount').forEach(inp => totalReceived += (+inp.value || 0));
    const disc = +($('f-pay-discount')?.value) || 0;
    const totalReduction = totalReceived + disc;

    if ($('pay-total-received-display')) $('pay-total-received-display').textContent = currency(totalReceived);
    if ($('pay-total-display')) $('pay-total-display').textContent = currency(totalReduction);

    autoAllocPayment();
    updatePaymentQR();

    const partyId = ($('f-pay-party-id') || {}).value || '';
    if (partyId) updatePaymentInvoicesAllocation(totalReduction);
};

// Mode chip selector (legacy support, but we now use dropdowns in rows)
window.selectPayMode = function(mode) {
    // For single mode logic, we'll just update the first row's mode
    const firstSelect = document.querySelector('.f-pay-row-mode');
    if (firstSelect) {
        firstSelect.value = mode;
        onPayModeChange();
    }
};
function onPayTypeChange() {
    const inp = $('f-pay-type');
    const newType = inp.value === 'in' ? 'out' : 'in';
    inp.value = newType;
    // Update toggle button label
    const lbl = $('pay-type-label');
    if (lbl) lbl.textContent = newType === 'in' ? '💰 Payment In' : '💸 Payment Out';
    // Auto-set party type: Payment In → Customer, Payment Out → Supplier
    const ptInp = $('f-pay-party-type');
    if (ptInp) ptInp.value = newType === 'in' ? 'Customer' : 'Supplier';
    // Clear party selection and reload dropdown
    if ($('f-pay-party')) $('f-pay-party').value = '';
    if ($('f-pay-party-id')) $('f-pay-party-id').value = '';
    onPayPartyTypeChange();
}

async function onPayPartyTypeChange() {
    const parties = await DB.getAll('parties');
    const ptype = ($('f-pay-party-type') || {}).value || '';
    // Clear current selection
    if ($('f-pay-party')) $('f-pay-party').value = '';
    if ($('f-pay-party-id')) $('f-pay-party-id').value = '';
    _initPayPartyDropdown(parties, ptype);
    onPayPartyChange();
}
async function onPayPartyChange() {
    const partyId = ($('f-pay-party-id') || {}).value || '';
    const type = $('f-pay-type').value;
    const invSec = $('pay-invoice-section');
    if (!invSec) return;

    // Show / hide party balance
    const balRow = $('pay-party-balance-row');
    const balVal = $('pay-party-balance-val');
    if (balRow) balRow.style.display = 'none';

    if (!partyId) { invSec.innerHTML = ''; invSec.style.display = 'none'; return; }

    // Fetch fresh party balance
    const parties = await DB.getAll('parties');
    const party = parties.find(p => p.id === partyId);
    if (balRow && balVal && party) {
        const bal = party.balance || 0;
        balRow.style.display = 'flex';
        balVal.textContent = currency(bal);
        balVal.style.color = bal > 0 ? 'var(--danger)' : 'var(--success)';
    }

    invSec.style.display = 'block';

    const invType = type === 'in' ? 'sale' : 'purchase';
    const invoices = (await DB.getAll('invoices'))
        .filter(i => i.type === invType && i.partyId === partyId && i.status !== 'cancelled')
        .sort((a, b) => new Date(a.date) - new Date(b.date));

    const pending = [];
    for (const i of invoices) {
        const paid = await getInvoicePaidAmount(i.invoiceNo);
        const remaining = +(i.total - paid).toFixed(2);
        if (remaining <= 0.01) continue;
        pending.push({ invoiceNo: i.invoiceNo, date: i.date, total: i.total, remaining });
    }

    if (!pending.length) {
        invSec.innerHTML = `
            <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">INVOICES</div>
            <div style="font-size:0.85rem;color:var(--text-muted)">No pending invoices. Payment will be saved as Advance.</div>`;
        return;
    }

    const cards = pending.map(inv => `
        <div class="pay-alloc-card" id="pac-${inv.invoiceNo.replace(/[^a-z0-9]/gi,'_')}">
            <input type="checkbox" class="pay-alloc-chk" data-inv="${inv.invoiceNo}" data-max="${inv.remaining}"
                checked style="width:20px;height:20px;accent-color:var(--primary);flex-shrink:0;cursor:pointer;margin-top:2px"
                onchange="togglePayAllocInv()">
            <div style="flex:1;min-width:0">
                <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                    <span style="font-weight:700;font-size:0.92rem;color:var(--text-primary)">${inv.invoiceNo}</span>
                    <span style="font-size:0.8rem;color:var(--text-muted)">${fmtDate(inv.date)}</span>
                </div>
                <div style="display:flex;justify-content:space-between;align-items:flex-end">
                    <div>
                        <div style="font-size:0.7rem;color:var(--text-muted);margin-bottom:2px">Due Amount</div>
                        <div style="font-weight:700;color:var(--danger);font-size:0.95rem">${currency(inv.remaining)}</div>
                    </div>
                    <div style="text-align:right">
                        <div style="font-size:0.7rem;color:var(--text-muted);margin-bottom:2px">Allocate ₹</div>
                        <input type="number" step="0.01" min="0" max="${inv.remaining}"
                            class="pay-alloc-input" data-inv="${inv.invoiceNo}"
                            style="width:110px;text-align:right;font-weight:700;font-size:0.95rem;border:none;border-bottom:2px solid var(--primary);background:transparent;padding:2px 4px;outline:none;color:var(--text-primary)"
                            placeholder="0.00" oninput="updatePayAllocSummary()">
                    </div>
                </div>
            </div>
        </div>`).join('');

    invSec.innerHTML = `
        <div style="font-size:0.72rem;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">INVOICES</div>
        <div style="font-size:0.82rem;color:var(--text-muted);margin-bottom:10px">
            Allocate Payment to Invoices <span style="color:var(--accent)">(Optional)</span> — uncheck to skip an invoice
        </div>
        <div id="pay-alloc-cards">${cards}</div>
        <div style="display:flex;justify-content:space-between;align-items:center;padding:10px 4px;margin-top:6px;border-top:1px solid var(--border)">
            <span style="font-size:0.85rem;color:var(--text-muted)">Total Allocated: <strong id="lbl-total-alloc" style="color:var(--success)">₹0.00</strong></span>
            <span id="lbl-unused-alloc" style="font-size:0.82rem;font-weight:600"></span>
        </div>`;

    // Auto-allocate based on current payment amount
    autoAllocPayment();
}

// onPayAmountChange is defined above with multi-mode support

// FIFO allocation across checked invoices only
window.autoAllocPayment = function () {
    let amt = 0;
    document.querySelectorAll('.f-pay-row-amount').forEach(inp => amt += (+inp.value || 0));
    const disc = +($('f-pay-discount')?.value) || 0;
    let remaining = amt + disc;

    // Zero out unchecked invoices first
    document.querySelectorAll('.pay-alloc-chk:not(:checked)').forEach(chk => {
        const inp = document.querySelector(`.pay-alloc-input[data-inv="${chk.dataset.inv}"]`);
        if (inp) inp.value = '';
    });

    // FIFO distribute across checked invoices (DOM order = oldest first)
    document.querySelectorAll('.pay-alloc-chk:checked').forEach(chk => {
        const inp = document.querySelector(`.pay-alloc-input[data-inv="${chk.dataset.inv}"]`);
        if (!inp) return;
        const max = +chk.dataset.max;
        if (remaining >= max) {
            inp.value = max.toFixed(2);
            remaining -= max;
        } else if (remaining > 0) {
            inp.value = remaining.toFixed(2);
            remaining = 0;
        } else {
            inp.value = '';
        }
    });

    updatePayAllocSummary();
};

window.togglePayAllocInv = function () {
    autoAllocPayment();
};

window.updatePayAllocSummary = function () {
    let allocated = 0;
    document.querySelectorAll('.pay-alloc-input').forEach(inp => allocated += (+inp.value || 0));
    let amt = 0;
    document.querySelectorAll('.f-pay-row-amount').forEach(inp => amt += (+inp.value || 0));
    if (!amt) amt = +($('f-pay-amount')?.value) || 0; // fallback for modal context
    const disc = +($('f-pay-discount')?.value) || 0;
    const totalReduction = amt + disc;
    const unused = +(totalReduction - allocated).toFixed(2);

    const lblAlloc  = document.getElementById('lbl-total-alloc');
    const lblUnused = document.getElementById('lbl-unused-alloc');
    if (lblAlloc)  lblAlloc.textContent  = currency(allocated);
    if (lblUnused) {
        lblUnused.textContent  = unused > 0.01 ? `Unused: ${currency(unused)}` : allocated > 0 ? '✅ Fully allocated' : '';
        lblUnused.style.color  = unused > 0.01 ? 'var(--warning)' : 'var(--success)';
    }
    calcTotalAllocation();
};

window.updatePaymentQR = function () {
    const box = $('pay-qr-box');
    if (!box) return;
    // With multi-mode rows, UPI QR shows if any row has UPI selected
    const modeSelects = [...document.querySelectorAll('.f-pay-row-mode')];
    const upiRow = modeSelects.find(s => s.value === 'UPI');
    const mode = upiRow ? 'UPI' : (modeSelects[0]?.value || '');
    // Sum the UPI row's amount specifically, or total if single mode
    let amount = 0;
    if (upiRow) {
        const row = upiRow.closest('.pay-mode-row');
        const amtInp = row ? row.querySelector('.f-pay-row-amount') : null;
        amount = amtInp ? (+amtInp.value || 0) : 0;
    } else {
        document.querySelectorAll('.f-pay-row-amount').forEach(inp => amount += (+inp.value || 0));
    }
    // Always read directly from localStorage to get the freshest UPI setting
    const co = DB.ls.getObj('db_company') || {};

    // Collect allocated invoice numbers for the note
    let invNotes = [];
    document.querySelectorAll('.pay-alloc-input').forEach(inp => {
        if (+inp.value > 0) invNotes.push(inp.dataset.inv);
    });
    const tn = invNotes.length ? invNotes.join(',') : '';
    const partyName = ($('f-pay-party') || {}).value?.trim() || '';
    const userName = currentUser?.name || '';

    if (mode === 'UPI' && amount > 0 && co.upi) {
        box.style.display = 'block';
        let upiString = `upi://pay?pa=${co.upi}&pn=${encodeURIComponent(co.name || 'Company')}&am=${amount}&cu=INR`;
        const noteParts = [];
        if (partyName) noteParts.push(partyName + (userName ? ' - ' + userName : ''));
        if (tn) noteParts.push('Inv:' + tn);
        if (noteParts.length) upiString += `&tn=${encodeURIComponent(noteParts.join(' | '))}`;
        const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(upiString)}`;
        const noteDisplay = [];
        if (tn) noteDisplay.push('<span style="color:var(--accent)">Invoice: ' + tn + '</span>');
        if (partyName) noteDisplay.push('<span style="color:var(--text-primary);font-weight:600">' + partyName + (userName ? ' - ' + userName : '') + '</span>');
        box.innerHTML = `<img src="${qrUrl}" alt="Scan to pay via UPI" style="display:block;margin:0 auto;border:1px solid var(--border);border-radius:8px;padding:8px;background:#fff;max-width:180px;width:180px;">
        <div style="font-size:0.8rem;color:var(--text-secondary);margin-top:4px;text-align:center;">Scan with any UPI App${noteDisplay.length ? '<br>' + noteDisplay.join('<br>') : ''}</div>`;
    } else {
        box.style.display = 'none';
        box.innerHTML = '';
        if (mode === 'UPI' && !co.upi) {
            box.style.display = 'block';
            box.innerHTML = `<div style="font-size:0.85rem;color:var(--warning);background:var(--warning-soft);padding:8px;border-radius:4px;">Configure UPI ID in Company Setup to generate QR codes.</div>`;
        }
    }
}

window.onPayModeChange = function () {
    updatePaymentQR();
    const modes = [...document.querySelectorAll('.f-pay-row-mode')].map(s => s.value);
    const chequeFields = $('pay-cheque-fields');
    if (chequeFields) {
        chequeFields.style.display = modes.includes('Cheque') ? 'block' : 'none';
    }
}

window.calcTotalAllocation = function () {
    let tot = 0;
    document.querySelectorAll('.pay-alloc-input').forEach(inp => tot += (+inp.value || 0));
    const lbl = document.getElementById('lbl-total-alloc');
    if (lbl) lbl.innerText = '₹' + tot.toFixed(2);
};
async function savePayment() {
    if (!beginSave()) return;
    
    // UI lock
    const saveBtn = document.getElementById('btn-save-payment');
    const saveNewBtn = document.getElementById('btn-save-new');
    if (saveBtn) { saveBtn.disabled = true; saveBtn.innerHTML = '🕒 Saving...'; }
    if (saveNewBtn) { saveNewBtn.disabled = true; saveNewBtn.innerHTML = '🕒 ...'; }
    
    const payPartyId = ($('f-pay-party-id') || {}).value || '';
    const payPartyName = ($('f-pay-party') || {}).value?.trim() || '';
    if (!payPartyId) { endSave(); return alert('Please select a party from the dropdown'); }

    // Collect all payment modes and amounts
    const payRows = [];
    document.querySelectorAll('.pay-mode-row').forEach(row => {
        const m = row.querySelector('.f-pay-row-mode').value;
        const a = +(row.querySelector('.f-pay-row-amount').value || 0);
        if (a > 0) payRows.push({ mode: m, amount: a });
    });

    if (payRows.length === 0) { endSave(); return alert('Enter at least one payment amount'); }

    const disc = +($('f-pay-discount')?.value) || 0;
    const totalReceived = payRows.reduce((sum, r) => sum + r.amount, 0);
    const totalReduction = totalReceived + disc;

    let allocations = {};
    let totalAlloc = 0;
    document.querySelectorAll('.pay-alloc-input').forEach(inp => {
        const val = +inp.value;
        if (val > 0) {
            allocations[inp.dataset.inv] = val;
            totalAlloc += val;
        }
    });

    if (totalAlloc > totalReduction + 0.01) { endSave(); return alert('Allocation exceeds total amount (Received + Discount).'); }

    const payType = $('f-pay-type').value;
    const invNo = Object.keys(allocations).length === 1 ? Object.keys(allocations)[0] : (Object.keys(allocations).length > 1 ? 'Multi' : '');

    try {
        const payRefNo = buildPayRefNo();
        const commonData = {
            payNo: payRefNo,
            date: $('f-pay-date').value,
            type: payType,
            partyId: payPartyId,
            partyName: payPartyName,
            note: $('f-pay-note').value.trim(),
            invoiceNo: invNo,
            allocations: Object.keys(allocations).length > 0 ? allocations : null,
            createdBy: currentUser.name,
            collectedBy: ($('f-pay-collected-by') ? $('f-pay-collected-by').value : null) || currentUser.name
        };

        // If multiple modes, we save multiple records
        // Discount is added only to the FIRST record to avoid duplication in accounting
        for (let i = 0; i < payRows.length; i++) {
            const row = payRows[i];
            const rowDisc = i === 0 ? disc : 0;
            const payData = {
                ...commonData,
                id: DB.id(),
                amount: row.amount,
                discount: rowDisc,
                totalReduction: row.amount + rowDisc,
                mode: row.mode
            };
            await DB.insert('payments', payData);
            
            // Record each mode in ledger
            await addPartyLedgerEntry(payPartyId, payPartyName, payType === 'in' ? 'Payment In' : 'Payment Out', row.amount * (payType === 'in' ? -1 : 1), payRefNo, row.mode, invNo);
        }
        incrementPayNo();

        // Automatic Expense Entry for Discount
        if (disc > 0 && payType === 'in') {
            await DB.insert('expenses', {
                date: $('f-pay-date').value,
                category: 'Payment Discount',
                amount: disc,
                partyId: payPartyId,
                partyName: payPartyName,
                docNo: payRefNo,
                description: `Payment Discount for ${payPartyName} (${payRefNo})`
            });
        }

        // Update party balance
        const parties = await DB.getAll('parties');
        const party = parties.find(p => p.id === payPartyId);
        if (party) {
            const balChange = payType === 'in' ? -totalReduction : totalReduction;
            const newBal = (party.balance || 0) + balChange;
            await DB.update('parties', party.id, { balance: newBal });
        }

        const andNew = window._saveAndNew; window._saveAndNew = false;
        await renderPayments();
        showToast('Payment saved!', 'success');
        if (andNew) openPaymentModal();
    } catch (err) {
        window._saveAndNew = false;
        alert('Error: ' + err.message);
    } finally {
        if (saveBtn) { saveBtn.disabled = false; saveBtn.innerHTML = '💾 Save Payment'; }
        if (saveNewBtn) { saveNewBtn.disabled = false; saveNewBtn.innerHTML = '＋ Save & New'; }
        endSave();
    }
}
async function openLinkInvoiceModal(payId) {
    const payments = await DB.getAll('payments');
    const pay = payments.find(p => p.id === payId); if (!pay) return;
    const invType = pay.type === 'in' ? 'sale' : 'purchase';
    const invoices = (await DB.getAll('invoices')).filter(i => i.type === invType && i.partyId === pay.partyId && i.status !== 'cancelled');

    let options = '';
    for (const i of invoices) {
        const paid = await getInvoicePaidAmount(i.invoiceNo);
        const adjPaid = pay.invoiceNo === i.invoiceNo ? paid - pay.amount : paid;
        const remaining = i.total - adjPaid;
        if (remaining > 0.01) options += `<option value="${i.invoiceNo}">${i.invoiceNo} — Due: ${currency(remaining)}</option>`;
    }

    openModal('Link Payment to Invoice', `
        <div style="margin-bottom:14px"><strong>Party:</strong> ${pay.partyName} | <strong>Amount:</strong> ${currency(pay.amount)}</div>
        <div class="form-group"><label>Select Invoice</label>
            <select id="f-link-invoice"><option value="">Select</option>${options}</select>
        </div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="linkPaymentToInvoice('${payId}')">🔗 Link</button></div>`);
}

async function openReceivePaymentForInvoice(invoiceId) {
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === invoiceId);
    if (!inv) return;
    const paid = await getInvoicePaidAmount(inv.invoiceNo);
    const due = inv.total - paid;
    if (due <= 0) return alert('Invoice is fully paid.');

    const isSale = inv.type === 'sale';

    openModal(isSale ? 'Receive Payment' : 'Make Payment', `
        <div style="font-size:0.9rem;margin-bottom:15px;padding:12px;background:var(--bg-body);border-radius:10px;border:1px solid var(--border)">
            <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="color:var(--text-muted)">${isSale ? 'Customer' : 'Supplier'}</span>
                <span style="font-weight:600">${escapeHtml(inv.partyName)}</span>
            </div>
            <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                <span style="color:var(--text-muted)">Invoice #</span>
                <span class="badge badge-info">${inv.invoiceNo}</span>
            </div>
            <div style="display:flex;justify-content:space-between;border-top:1px dashed var(--border);margin-top:8px;padding-top:8px">
                <span style="font-weight:700">Due Amount</span>
                <span style="font-weight:800;color:var(--danger)">${currency(due)}</span>
            </div>
        </div>
        
        <div class="form-row">
            <div class="form-group"><label>Amount Received ₹ *</label><input type="number" step="0.01" id="f-pay-amount" value="${due.toFixed(2)}" oninput="onDirectPayAmountChange()"></div>
            <div class="form-group"><label>Discount ₹</label><input type="number" step="0.01" id="f-pay-discount" value="0.00" oninput="onDirectPayAmountChange()"></div>
        </div>

        <div style="margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;padding:10px;background:rgba(16,185,129,0.05);border-radius:8px">
            <span style="font-size:0.9rem;font-weight:700;color:var(--text-muted)">Total Reduction:</span>
            <span style="font-size:1.1rem;font-weight:800;color:var(--success)" id="direct-pay-total-display">${currency(due)}</span>
        </div>

        <div class="form-group"><label>Date *</label><input type="date" id="f-pay-date" value="${today()}"></div>

        <div class="form-row">
            <div class="form-group"><label>Mode</label><select id="f-pay-mode"><option>Cash</option><option>UPI</option><option>Bank Transfer</option><option>Cheque</option></select></div>
            <div class="form-group"><label>Note</label><input id="f-pay-note" placeholder="Optional note"></div>
        </div>
        <input type="hidden" id="f-pay-inv-id" value="${inv.id}">
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveDirectPayment()">Save Payment</button>
        </div>
    `);

    window.onDirectPayAmountChange = function() {
        const amt = +($('f-pay-amount').value) || 0;
        const disc = +($('f-pay-discount').value) || 0;
        $('direct-pay-total-display').textContent = currency(amt + disc);
    };
}

async function saveDirectPayment() {
    const invId = $('f-pay-inv-id').value;
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === invId);
    if (!inv) return alert('Invoice not found');
    
    const amt = +$('f-pay-amount').value;
    const disc = +($('f-pay-discount')?.value) || 0;
    if (!amt || amt <= 0) return alert('Enter a valid amount');
    const totalReduction = amt + disc;

    const isSale = inv.type === 'sale';
    const payType = isSale ? 'in' : 'out';

    const payRefNo = buildPayRefNo();
    const payData = {
        payNo: payRefNo,
        date: $('f-pay-date').value,
        type: payType,
        partyId: inv.partyId,
        partyName: inv.partyName,
        amount: amt,
        discount: disc,
        totalReduction: totalReduction,
        mode: $('f-pay-mode').value,
        note: $('f-pay-note').value.trim(),
        invoiceNo: inv.invoiceNo,
        createdBy: currentUser.name,
        collectedBy: currentUser.name
    };

    try {
        await DB.insert('payments', payData);
        incrementPayNo();

        // Automatic Expense Entry for Discount
        if (disc > 0 && payType === 'in') {
            await DB.insert('expenses', {
                date: $('f-pay-date').value,
                category: 'Payment Discount',
                amount: disc,
                partyId: payPartyId || inv.partyId || '',
                partyName: inv.partyName || '',
                docNo: payRefNo,
                description: `Payment Discount for ${inv.partyName} (${payRefNo})`
            });
        }

        const parties = await DB.getAll('parties');
        const party = parties.find(p => p.id === inv.partyId);
        if (party) {
            const balChange = payType === 'in' ? -totalReduction : totalReduction;
            const newBal = (party.balance || 0) + balChange;
            await DB.update('parties', party.id, { balance: newBal });
            await addPartyLedgerEntry(party.id, party.name, payType === 'in' ? 'Payment In' : 'Payment Out', balChange, payRefNo, payData.note || 'Payment Received');
        }

        closeModal();
        if ($('invoice-tbody')) {
            await filterInvTable2();
        } else {
            await renderPayments();
        }
        showToast('Payment recorded successfully', 'success');
    } catch (err) {
        alert('Error: ' + err.message);
    }
}

async function allocateAdvanceToInvoice(invId) {
    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.id === invId);
    if (!inv) return;
    const paid = await getInvoicePaidAmount(inv.invoiceNo);
    const due = inv.total - paid;
    if (due <= 0) return alert('Invoice is fully paid.');

    const payments = await DB.getAll('payments');
    const payType = inv.type === 'sale' ? 'in' : 'out';
    const advances = payments.filter(p => {
        if (p.partyId !== inv.partyId || p.type !== payType || p.status === 'cancelled') return false;
        // Calculate remaining balance for this payment
        const used = p.allocations
            ? Object.values(p.allocations).reduce((s, v) => s + (+v), 0)
            : (p.invoiceNo && p.invoiceNo !== 'Advance' && p.invoiceNo !== 'Multi' ? p.amount : 0);
        return (p.amount - used) > 0.01;
    });

    let optionsHtml = '';
    for (const a of advances) {
        const used = a.allocations ? Object.values(a.allocations).reduce((s, val) => s + (+val), 0) : 0;
        const rem = a.amount - used;
        if (rem > 0) {
            optionsHtml += `
            <div style="display:flex;justify-content:space-between;align-items:center;padding:10px;border:1px solid var(--border);border-radius:6px;margin-bottom:8px;">
                <div>
                    <strong>Date:</strong> ${fmtDate(a.date)}<br>
                    <strong>Available:</strong> <span style="color:var(--success);font-weight:700">${currency(rem)}</span> (of ${currency(a.amount)})
                </div>
                <div>
                    <input type="number" id="alloc-amt-${a.id}" max="${Math.min(rem, due)}" value="${Math.min(rem, due).toFixed(2)}" class="form-control" style="width:100px;display:inline-block;padding:4px" placeholder="Amount">
                    <button class="btn btn-primary btn-sm" onclick="saveAdvanceAllocation('${a.id}', '${inv.invoiceNo}', ${rem})">Apply</button>
                </div>
            </div>
            `;
        }
    }

    if (!optionsHtml) return alert('No advance payments available.');

    openModal('Apply Advance Payment', `
        <div style="margin-bottom:15px;padding:10px;background:var(--bg-body);border-radius:6px;border:1px solid var(--border)">
            <strong>Invoice:</strong> <span class="badge badge-info">${inv.invoiceNo}</span> | <strong>Due:</strong> <span style="color:var(--danger);font-weight:700">${currency(due)}</span>
        </div>
        <div style="max-height:300px;overflow-y:auto;padding-right:5px">
            ${optionsHtml}
        </div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button></div>
    `);
}

async function saveAdvanceAllocation(payId, invoiceNo, maxAvail) {
    const amt = +document.getElementById('alloc-amt-' + payId).value;
    if (!amt || amt <= 0) return alert('Enter amount to apply');
    if (amt > maxAvail + 0.01) return alert('Amount exceeds available advance');

    const invoices = await DB.getAll('invoices');
    const inv = invoices.find(i => i.invoiceNo === invoiceNo);
    if (!inv) return alert('Invoice not found');
    const paid = await getInvoicePaidAmount(invoiceNo);
    const due = inv.total - paid;
    if (amt > due + 0.01) return alert('Amount exceeds invoice due amount');

    try {
        const payments = await DB.getAll('payments');
        const pay = payments.find(p => p.id === payId);
        if (!pay) return;

        const allocs = pay.allocations || {};
        allocs[invoiceNo] = (allocs[invoiceNo] || 0) + amt;

        await DB.update('payments', pay.id, { allocations: allocs });
        showToast('Advance applied successfully', 'success');
        await viewInvoice(inv.id);
    } catch (err) {
        alert('Error: ' + err.message);
    }
}

async function linkPaymentToInvoice(payId) {
    const invNo = $('f-link-invoice').value; if (!invNo) return alert('Select an invoice');
    try {
        await DB.update('payments', payId, { invoiceNo: invNo });
        closeModal();
        await renderPayments();
        showToast('Payment linked!', 'success');
    } catch (err) {
        alert('Error: ' + err.message);
    }
}
async function deletePayment(id) {
    if (!confirm('Delete payment? Effects will be reversed.')) return;
    try {
        const payments = await DB.getAll('payments');
        const pay = payments.find(p => p.id === id);
        if (pay) {
            const parties = await DB.getAll('parties');
            const party = parties.find(p => p.id === pay.partyId);
            if (party) {
                const balChange = pay.type === 'in' ? pay.amount : -pay.amount;
                const newBal = (party.balance || 0) + balChange;
                await DB.update('parties', party.id, { balance: newBal });
                await addPartyLedgerEntry(party.id, party.name, 'Payment Deleted', balChange, pay.invoiceNo || 'Advance', 'Payment Deleted');
            }
        }
        await DB.delete('payments', id);
        await renderPayments();
        showToast('Payment deleted!', 'warning');
    } catch (err) {
        alert('Error: ' + err.message);
    }
}

async function openEditPaymentModal(id) {
    const payments = await DB.getAll('payments');
    const pay = payments.find(p => p.id === id);
    if (!pay) return alert('Payment not found');

    const invType = pay.type === 'in' ? 'sale' : 'purchase';
    const invoices = (await DB.getAll('invoices')).filter(i => i.type === invType && i.partyId === pay.partyId && i.status !== 'cancelled');

    let rows = '';
    for (const i of invoices) {
        // allocation already attributed to this instance — we attribute the FULL reduction (Amt+Disc)
        let alreadyAllocatedHere = (pay.allocations && pay.allocations[i.invoiceNo]) ? pay.allocations[i.invoiceNo] : 0;
        let totalPaidBefore = (await getInvoicePaidAmount(i.invoiceNo)) - alreadyAllocatedHere;
        let remaining = i.total - totalPaidBefore;

        if (remaining <= 0.01 && alreadyAllocatedHere <= 0.01) continue;
        rows += `
            <tr>
                <td>${i.invoiceNo}</td>
                <td style="color:var(--danger)">${currency(remaining)}</td>
                <td><input type="number" step="0.01" max="${remaining.toFixed(2)}" class="form-control pay-alloc-input" data-inv="${i.invoiceNo}" value="${alreadyAllocatedHere > 0 ? alreadyAllocatedHere : ''}" placeholder="0.00" style="padding:4px;width:100px;text-align:right" oninput="updateEditAllocTotal()"></td>
            </tr>
        `;
    }

    let invSecHtml = rows ? `
        <div style="margin-top:15px;margin-bottom:15px">
            <label style="font-size:0.8rem;font-weight:700;color:var(--text-muted)">ALLOCATION BY INVOICE</label>
            <div style="max-height:200px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;margin-top:5px">
                <table class="data-table" style="margin:0;font-size:0.85rem">
                    <thead style="background:var(--bg-input)"><tr><th>Invoice #</th><th>Due</th><th>Allocate ₹</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <div style="text-align:right;margin-top:8px;font-size:0.85rem;font-weight:700">Allocated: <span id="lbl-edit-alloc-total" style="color:var(--success)">₹0.00</span></div>
        </div>
    ` : `<div style="margin:10px 0;font-size:0.85rem;color:var(--text-muted)">No pending invoices for this party.</div>`;

    openModal('Edit Payment', `
        <div class="form-row">
            <div class="form-group"><label>Date</label><input type="date" id="f-pay-date" value="${pay.date}"></div>
            <div class="form-group"><label>Type</label>
                <select id="f-pay-type" disabled><option value="in" ${pay.type === 'in' ? 'selected' : ''}>Payment In</option><option value="out" ${pay.type === 'out' ? 'selected' : ''}>Payment Out</option></select>
            </div>
        </div>
        <div class="form-group"><label style="font-size:0.85rem;font-weight:700">${pay.type === 'in' ? 'Customer' : 'Supplier'}</label>
            <input value="${escapeHtml(pay.partyName)}" disabled style="background:var(--bg-input);font-weight:600">
        </div>
        
        <div class="form-row">
            <div class="form-group"><label>Amount ₹</label><input type="number" id="f-pay-amount" min="0" value="${pay.amount}" oninput="onEditPayAmountChange()"></div>
            <div class="form-group"><label>Discount ₹</label><input type="number" id="f-pay-discount" min="0" value="${pay.discount || 0}" oninput="onEditPayAmountChange()"></div>
        </div>
        
        <div style="margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;padding:10px;background:rgba(59,130,246,0.05);border-radius:8px">
            <span style="font-size:0.9rem;font-weight:700;color:var(--text-muted)">Balance Reduction:</span>
            <span style="font-size:1.1rem;font-weight:800;color:var(--primary)" id="edit-pay-total-display">${currency((pay.amount||0) + (pay.discount||0))}</span>
        </div>

        <div id="pay-edit-invoice-section">${invSecHtml}</div>

        <div class="form-row">
            <div class="form-group"><label>Mode</label>
                <select id="f-pay-mode"><option ${pay.mode === 'Cash' ? 'selected' : ''}>Cash</option><option ${pay.mode === 'UPI' ? 'selected' : ''}>UPI</option><option ${pay.mode === 'Bank Transfer' ? 'selected' : ''}>Bank Transfer</option><option ${pay.mode === 'Cheque' ? 'selected' : ''}>Cheque</option></select>
            </div>
            <div class="form-group"><label>Collected By</label><input id="f-pay-collected-by" value="${pay.collectedBy || ''}"></div>
        </div>
        <div class="form-group"><label>Note</label><input id="f-pay-note" placeholder="Optional note" value="${escapeHtml(pay.note || '')}"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="saveEditedPayment('${pay.id}')">Save Changes</button></div>
    `);

    window.onEditPayAmountChange = function() {
        const amt = +($('f-pay-amount').value) || 0;
        const disc = +($('f-pay-discount').value) || 0;
        $('edit-pay-total-display').textContent = currency(amt + disc);
    };

    window.updateEditAllocTotal = function() {
        let tot = 0;
        document.querySelectorAll('.pay-alloc-input').forEach(inp => tot += (+inp.value || 0));
        const lbl = $('lbl-edit-alloc-total');
        if (lbl) lbl.textContent = currency(tot);
    };

    setTimeout(() => window.updateEditAllocTotal(), 100);
}

window.saveEditedPayment = async function (id) {
    const amt = +$('f-pay-amount').value;
    const disc = +($('f-pay-discount')?.value) || 0;
    if (!amt || amt <= 0) return alert('Enter valid amount');
    const totalReduction = amt + disc;

    let allocations = {};
    let totalAlloc = 0;
    document.querySelectorAll('.pay-alloc-input').forEach(inp => {
        const val = +inp.value;
        if (val > 0) { allocations[inp.dataset.inv] = val; totalAlloc += val; }
    });
    if (totalAlloc > totalReduction + 0.01) return alert('Allocation exceeds total reduction (Amount + Discount).');

    const invNo = Object.keys(allocations).length === 1 ? Object.keys(allocations)[0] : (Object.keys(allocations).length > 1 ? 'Multi' : '');

    try {
        const payments = await DB.getAll('payments');
        const oldPay = payments.find(p => p.id === id);
        if (!oldPay) return alert('Payment not found');

        const oldAmount = oldPay.amount || 0;
        const oldDiscount = oldPay.discount || 0;
        const oldTotalReduction = oldPay.totalReduction || (oldAmount + oldDiscount);

        // Update party balance
        const parties = await DB.getAll('parties');
        const party = parties.find(p => p.id === oldPay.partyId);
        if (party) {
            // Revert old reduction: if it was "in", we add it back to balance.
            const revBalChange = oldPay.type === 'in' ? oldTotalReduction : -oldTotalReduction;
            const tempBal = (party.balance || 0) + revBalChange;
            
            // Apply new reduction
            const newBalChange = oldPay.type === 'in' ? -totalReduction : totalReduction;
            const finalBal = tempBal + newBalChange;
            
            await DB.update('parties', party.id, { balance: finalBal });
            await addPartyLedgerEntry(party.id, party.name, oldPay.type === 'in' ? 'Payment Edited' : 'Payment Out Edited', newBalChange, id, `Mode: ${$('f-pay-mode').value}`);
        }

        // Manage Expense Entry for Discount
        if (oldPay.type === 'in') {
            const expenses = await DB.getAll('expenses');
            const payRefNo = oldPay.payNo || oldPay.id.substring(0,8);
            const discExp = expenses.find(e => e.category === 'Payment Discount' && e.description && e.description.includes(`(${payRefNo})`));

            if (disc > 0) {
                const expData = {
                    date: $('f-pay-date').value,
                    category: 'Payment Discount',
                    amount: disc,
                    description: `Payment Discount for ${oldPay.partyName} (${payRefNo})`
                };
                if (discExp) {
                    await DB.update('expenses', discExp.id, expData);
                } else {
                    await DB.insert('expenses', expData);
                }
            } else if (discExp) {
                // Discount removed
                await DB.delete('expenses', discExp.id);
            }
        }

        // Update payment record
        await DB.update('payments', id, {
            date: $('f-pay-date').value,
            amount: amt,
            discount: disc,
            totalReduction: totalReduction,
            mode: $('f-pay-mode').value,
            note: $('f-pay-note').value.trim(),
            collectedBy: $('f-pay-collected-by')?.value || oldPay.collectedBy,
            invoiceNo: invNo,
            allocations: Object.keys(allocations).length > 0 ? allocations : null
        });

        closeModal();
        await renderPayments();
        showToast('Payment updated successfully!', 'success');
    } catch (err) {
        alert('Error: ' + err.message);
    }
};

// =============================================
//  EXPENSES
// =============================================
function renderExpRows(expenses) {
    if (!expenses.length) return '<tr><td colspan="8" class="empty-state"><p>No expenses found</p></td></tr>';
    const cols = ColumnManager.get('expenses').filter(c => c.visible);
    const isAdm = currentUser && (currentUser.role === 'Admin' || (currentUser.roles || []).includes('Admin'));
    const parties = DB.get('db_parties') || [];
    return expenses.map(e => {
        const party = e.partyId ? parties.find(p => p.id === e.partyId) : null;
        const partyCode = party ? (party.partyCode || '') : '';
        const partyDisplay = e.partyName
            ? `<div style="font-weight:600;font-size:0.85rem">${escapeHtml(e.partyName)}</div>${partyCode ? `<div style="font-size:0.75rem;color:var(--text-muted)">${escapeHtml(partyCode)}</div>` : ''}`
            : `<span style="color:var(--text-muted)">-</span>`;
        // Doc link — navigate to invoice or payment
        let docLink = '-';
        if (e.docNo) {
            const isPayment = e.docNo.startsWith('PAY-');
            docLink = `<button class="btn-link" style="font-weight:600;color:var(--accent);background:none;border:none;cursor:pointer;padding:0;font-size:0.85rem" onclick="viewExpenseDoc('${escapeHtml(e.docNo)}','${isPayment?'payment':'invoice'}')">${escapeHtml(e.docNo)}</button>`;
        }
        const cellMap = {
            date:     `<td style="white-space:nowrap">${fmtDate(e.date)}</td>`,
            category: `<td><span class="badge ${e.category==='Sales Discount'?'badge-warning':e.category==='Payment Discount'?'badge-info':'badge-success'}" style="font-size:0.75rem">${escapeHtml(e.category || '')}</span></td>`,
            party:    `<td>${partyDisplay}</td>`,
            docNo:    `<td>${docLink}</td>`,
            amount:   `<td class="amount-red" style="font-weight:700">${currency(e.amount)}</td>`,
            addedBy:  `<td style="font-size:0.82rem;color:var(--text-muted)">${escapeHtml(e.createdBy || '-')}</td>`,
            actions:  `<td><div class="action-btns">
                ${isAdm && e.docNo ? `<button class="btn-icon" title="View Document" onclick="viewExpenseDoc('${escapeHtml(e.docNo)}','${e.docNo.startsWith('PAY-')?'payment':'invoice'}')">👁️</button>` : ''}
                ${!e.docNo && canEdit() ? `<button class="btn-icon" onclick="openEditExpenseModal('${e.id}')">✏️</button>` : ''}
                ${canEdit() ? `<button class="btn-icon" onclick="deleteExpense('${e.id}')">🗑️</button>` : ''}
            </div></td>`,
        };
        return `<tr>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}

async function viewExpenseDoc(docNo, type) {
    if (type === 'payment') {
        // Navigate to payments first
        navigateTo('payments');
        setTimeout(async () => {
            const payments = DB.get('db_payments') || await DB.getAll('payments');
            const p = payments.find(x => x.payNo === docNo || x.id === docNo);
            if (p) { viewPaymentDetails(p.id); }
            else { showToast('Payment record not found: ' + docNo, 'error'); }
        }, 150);
    } else {
        // Navigate to invoices first
        navigateTo('invoices');
        setTimeout(async () => {
            const invoices = DB.get('db_invoices') || await DB.getAll('invoices');
            const inv = invoices.find(x => x.invoiceNo === docNo);
            if (inv) { viewInvoice(inv.id); }
            else { showToast('Invoice not found: ' + docNo, 'error'); }
        }, 150);
    }
}

function openEditExpenseModal(id) {
    const expenses = DB.get('db_expenses') || [];
    const e = expenses.find(x => x.id === id);
    if (!e) return;
    openModal('Edit Expense', `
        <div class="form-row">
            <div class="form-group"><label>Date *</label><input type="date" id="f-exp-date" value="${e.date || today()}"></div>
            <div class="form-group"><label>Category</label><input id="f-exp-cat" value="${escapeHtml(e.category || '')}" placeholder="e.g. Transport, Office"></div>
        </div>
        <div class="form-group"><label>Amount *</label><input type="number" id="f-exp-amt" min="0" step="0.01" value="${e.amount || 0}"></div>
        <div class="form-group"><label>Description</label><input id="f-exp-desc" value="${escapeHtml(e.description || e.note || '')}" placeholder="Details..."></div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-primary" onclick="saveEditedExpense('${id}')">💾 Save</button>`);
}

async function saveEditedExpense(id) {
    const amt = +$('f-exp-amt').value; if (!amt) return alert('Enter amount');
    try {
        await DB.update('expenses', id, {
            date: $('f-exp-date').value,
            category: $('f-exp-cat').value,
            amount: amt,
            description: $('f-exp-desc').value.trim()
        });
        closeModal();
        await renderExpenses();
        showToast('Expense updated', 'success');
    } catch(err) { alert('Error: ' + err.message); }
}

async function renderExpenses() {
    const expenses = await DB.getAll('expenses');
    window._allExpenses = expenses;
    const salDisc   = expenses.filter(e => e.category === 'Sales Discount').reduce((s, e) => s + (e.amount || 0), 0);
    const payDisc   = expenses.filter(e => e.category === 'Payment Discount').reduce((s, e) => s + (e.amount || 0), 0);
    const total     = expenses.reduce((s, e) => s + (e.amount || 0), 0);
    // Build category options
    const cats = [...new Set(expenses.map(e => e.category).filter(Boolean))].sort();
    pageContent.innerHTML = `
        <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:14px">
            <div class="stat-card amber" style="flex:1;min-width:140px;padding:12px 16px"><div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:4px">Sales Discount</div><div style="font-size:1.3rem;font-weight:800;color:#f59e0b">${currency(salDisc)}</div></div>
            <div class="stat-card blue" style="flex:1;min-width:140px;padding:12px 16px"><div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:4px">Payment Discount</div><div style="font-size:1.3rem;font-weight:800;color:#3b82f6">${currency(payDisc)}</div></div>
            <div class="stat-card red" style="flex:1;min-width:140px;padding:12px 16px"><div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;color:var(--text-muted);margin-bottom:4px">Total Expenses</div><div style="font-size:1.3rem;font-weight:800;color:#ef4444">${currency(total)}</div></div>
        </div>
        <div class="card" style="margin-bottom:10px"><div class="card-body" style="padding:10px 14px">
            <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:flex-end">
                <div style="display:flex;flex-direction:column;gap:3px"><label style="font-size:0.72rem;font-weight:600;color:var(--text-muted)">FROM</label><input type="date" id="exp-f-from" class="form-control" style="width:130px;padding:5px 8px;font-size:0.82rem" onchange="filterExpTable()"></div>
                <div style="display:flex;flex-direction:column;gap:3px"><label style="font-size:0.72rem;font-weight:600;color:var(--text-muted)">TO</label><input type="date" id="exp-f-to" class="form-control" style="width:130px;padding:5px 8px;font-size:0.82rem" onchange="filterExpTable()"></div>
                <div style="display:flex;flex-direction:column;gap:3px"><label style="font-size:0.72rem;font-weight:600;color:var(--text-muted)">CATEGORY</label><select id="exp-f-cat" class="form-control" style="width:160px;padding:5px 8px;font-size:0.82rem" onchange="filterExpTable()"><option value="">All Categories</option>${cats.map(c=>`<option>${escapeHtml(c)}</option>`).join('')}</select></div>
                <div style="display:flex;flex-direction:column;gap:3px"><label style="font-size:0.72rem;font-weight:600;color:var(--text-muted)">PARTY</label><input type="text" id="exp-f-party" class="form-control" placeholder="Party name..." style="width:150px;padding:5px 8px;font-size:0.82rem" oninput="filterExpTable()"></div>
                <div style="display:flex;flex-direction:column;gap:3px"><label style="font-size:0.72rem;font-weight:600;color:var(--text-muted)">DOC NO</label><input type="text" id="exp-f-doc" class="form-control" placeholder="INV-0001..." style="width:130px;padding:5px 8px;font-size:0.82rem" oninput="filterExpTable()"></div>
                <button class="btn btn-outline btn-sm" onclick="clearExpFilters()" style="align-self:flex-end">✕ Clear</button>
                <div style="margin-left:auto;align-self:flex-end;display:flex;gap:8px">
                    <button class="btn btn-outline" onclick="openColumnPersonalizer('expenses','renderExpenses')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button>
                    <button class="btn btn-primary" onclick="openExpenseModal()">+ Add Expense</button>
                </div>
            </div>
        </div></div>
        <div class="card"><div class="card-body" style="padding:0">
            <div class="table-wrapper"><table class="data-table">
                <thead><tr>${ColumnManager.get('expenses').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="exp-tbody">${renderExpRows(expenses)}</tbody>
            </table></div>
        </div></div>`;
}

function clearExpFilters() {
    ['exp-f-from','exp-f-to','exp-f-party','exp-f-doc'].forEach(id => { const el = $(id); if (el) el.value = ''; });
    const cat = $('exp-f-cat'); if (cat) cat.value = '';
    filterExpTable();
}

async function filterExpTable() {
    let exps = window._allExpenses || await DB.getAll('expenses');
    const from  = ($('exp-f-from') || {}).value || '';
    const to    = ($('exp-f-to') || {}).value || '';
    const cat   = (($('exp-f-cat') || {}).value || '').toLowerCase();
    const party = (($('exp-f-party') || {}).value || '').toLowerCase();
    const doc   = (($('exp-f-doc') || {}).value || '').toLowerCase();
    if (from)  exps = exps.filter(e => (e.date || '') >= from);
    if (to)    exps = exps.filter(e => (e.date || '') <= to);
    if (cat)   exps = exps.filter(e => (e.category || '').toLowerCase().includes(cat));
    if (party) exps = exps.filter(e => (e.partyName || '').toLowerCase().includes(party));
    if (doc)   exps = exps.filter(e => (e.docNo || '').toLowerCase().includes(doc));
    const tbody = $('exp-tbody');
    if (tbody) tbody.innerHTML = renderExpRows(exps);
}

function openExpenseModal() {
    openModal('Add Expense', `
        <div class="form-row">
            <div class="form-group"><label>Date *</label><input type="date" id="f-exp-date" value="${today()}"></div>
            <div class="form-group"><label>Category</label><input id="f-exp-cat" placeholder="e.g. Transport, Office"></div>
        </div>
        <div class="form-group"><label>Amount *</label><input type="number" id="f-exp-amt" min="0" step="0.01" placeholder="0.00"></div>
        <div class="form-group"><label>Description</label><input id="f-exp-desc" placeholder="Details..."></div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveExpense()">＋ Save & New</button><button class="btn btn-primary" onclick="saveExpense()">💾 Save Expense</button>`);
}

async function saveExpense() {
    if (!beginSave()) return;
    const amt = +$('f-exp-amt').value; if (!amt) { endSave(); return alert('Enter amount'); }
    const expData = {
        date: $('f-exp-date').value,
        category: $('f-exp-cat').value,
        amount: amt,
        description: $('f-exp-desc').value.trim()
    };
    try {
        await DB.insert('expenses', expData);
        const andNew = window._saveAndNew; window._saveAndNew = false;
        closeModal();
        await renderExpenses();
        showToast('Expense saved', 'success');
        if (andNew) openExpenseModal();
    } catch (err) { window._saveAndNew = false; alert('Error: ' + err.message); }
}

async function deleteExpense(id) {
    if (!confirm('Delete?')) return;
    try {
        await DB.delete('expenses', id);
        await renderExpenses();
    } catch (err) { alert('Error: ' + err.message); }
}

// =============================================
//  PACKING (Shows approved orders → Pack → Admin/Manager generates invoice)
// =============================================
function renderPacking() {
    const orders = DB.cache['sales_orders'] || DB.cache['db_salesorders'] || [];
    const invoices = DB.cache['invoices'] || DB.cache['db_invoices'] || [];
    const hasCancelledInvoice = (o) => {
        if (o.invoiceCancelled) return true;
        return invoices.some(i => i.fromOrder === o.orderNo && i.status === 'cancelled');
    };
    const isAdmin = currentUser.role === 'Admin' || currentUser.role === 'Manager';

    // Cannot complete orders (flagged, not yet re-approved)
    const cannotCompleteOrders = orders.filter(o => o.cannotComplete && !o.packed && o.status === 'approved');

    // Filter ready to pack — exclude cannot-complete flagged ones
    let readyToPackRows = orders.filter(o => o.status === 'approved' && !o.packed && !hasCancelledInvoice(o) && !o.cannotComplete);
    if (!isAdmin) {
        readyToPackRows = readyToPackRows.filter(o => !o.assignedPacker || o.assignedPacker === currentUser.name);
    }

    const allPackedNoInvoice = orders.filter(o => o.packed && !o.invoiceNo);
    // Non-admin sees only their own packed orders awaiting invoice
    const packedNoInvoice = isAdmin ? allPackedNoInvoice : allPackedNoInvoice.filter(o => o.packedBy === currentUser.name);
    const packedWithInvoice = orders.filter(o => o.packed && o.invoiceNo);

    // Date filter for Packed History (default: current month)
    const todayD = today();
    const monthStartD = todayD.substring(0, 8) + '01';
    const savedFrom = window._packHistFrom || monthStartD;
    const savedTo = window._packHistTo || todayD;
    const filteredHistory = packedWithInvoice.filter(o => {
        const d = (o.packedAt || o.date || '').substring(0, 10);
        return (!savedFrom || d >= savedFrom) && (!savedTo || d <= savedTo);
    });

    pageContent.innerHTML = `
        <div class="stats-grid" style="margin-bottom:18px">
            <div class="stat-card amber"><div class="stat-icon">📋</div><div class="stat-value">${readyToPackRows.length}</div><div class="stat-label">Ready to Pack</div></div>
            <div class="stat-card blue"><div class="stat-icon">📦</div><div class="stat-value">${packedNoInvoice.length}</div><div class="stat-label">Awaiting Invoice</div></div>
            <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${packedWithInvoice.length}</div><div class="stat-label">Packed History</div></div>
            <div class="stat-card red"><div class="stat-icon">❌</div><div class="stat-value">${cannotCompleteOrders.length}</div><div class="stat-label">Cannot Complete</div></div>
        </div>
        <h3 style="margin-bottom:14px;font-size:1rem">🔶 Orders Ready for Packing</h3>
        <div class="section-toolbar" style="margin-bottom:8px"><div class="filter-group"><button class="btn btn-outline" onclick="openColumnPersonalizer('packing','renderPacking')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button></div></div>
        <div class="card" style="margin-bottom:24px"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>${ColumnManager.get('packing').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody>${readyToPackRows.length ? readyToPackRows.map(o => {
                    const packCols = ColumnManager.get('packing').filter(c => c.visible);
                    const cellMap = {
                        orderNo:    `<td style="font-weight:600">${o.orderNo}</td>`,
                        date:       `<td>${fmtDate(o.date)}</td>`,
                        party:      `<td>${escapeHtml(o.partyName)}</td>`,
                        items:      `<td>${o.items.length}</td>`,
                        total:      `<td class="amount-green">${currency(o.total)}</td>`,
                        assignedTo: `<td>${o.assignedPacker ? `<span class="badge badge-info">${o.assignedPacker}</span>` : '<span class="badge badge-warning">Unassigned</span>'}</td>`,
                        actions:    `<td><div class="action-btns">
                            ${!o.assignedPacker && isAdmin ? `<button class="btn btn-outline btn-sm" onclick="openAssignPackerModal('${o.id}')">👤 Assign</button>` : ''}
                            ${!o.assignedPacker && !isAdmin ? `<button class="btn btn-outline btn-sm" onclick="selfAssign('${o.id}')">✋ Self Assign</button>` : ''}
                            ${o.assignedPacker === currentUser.name || isAdmin ? `<button class="btn btn-primary btn-sm" onclick="startPacking('${o.id}')">▶️ Start Packing</button>` : ''}
                        </div></td>`,
                    };
                    return `<tr>${packCols.map(c => cellMap[c.key] || '').join('')}</tr>`;
                }).join('') : '<tr><td colspan="7" class="empty-state"><p>No orders waiting</p></td></tr>'}</tbody></table>
            </div>
        </div></div>
        ${cannotCompleteOrders.length ? `<h3 style="margin-bottom:14px;font-size:1rem">❌ Cannot Complete — Needs Admin Review</h3>
        <div class="card" style="margin-bottom:24px;border:1px solid rgba(239,68,68,0.3)"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Order #</th><th>Party</th><th>Flagged By</th><th>Reason</th><th>Notes</th><th>Line Status</th>${isAdmin ? '<th>Actions</th>' : ''}</tr></thead>
                <tbody>${cannotCompleteOrders.map(o => {
            const lines = o.cannotCompleteLines || o.items.map(li => ({ ...li, pickedQty: 0, picked: false }));
            const linesSummary = lines.map((li, i) => `<div style="font-size:0.78rem;padding:2px 0">${i + 1}. ${li.name}: <strong style="color:${li.picked ? 'var(--success)' : 'var(--danger)'}">${li.pickedQty}/${li.qty}</strong> ${li.picked ? '✅' : '🔴'}</div>`).join('');
            return `<tr>
                        <td style="font-weight:600">${o.orderNo}</td>
                        <td>${o.partyName}</td>
                        <td><span class="badge badge-info">${o.cannotCompleteBy || '-'}</span></td>
                        <td><span class="badge badge-danger">${o.cannotCompleteReason || '-'}</span></td>
                        <td style="font-size:0.82rem;color:var(--text-secondary)">${o.cannotCompleteNotes || '-'}</td>
                        <td style="max-width:200px">${linesSummary}</td>
                        ${isAdmin ? `<td><div class="action-btns">
                            <button class="btn btn-outline btn-sm" onclick="clearCannotComplete('${o.id}')">🔄 Retry</button>
                            <button class="btn btn-danger btn-sm" onclick="cancelOrderFromPacking('${o.id}')">❌ Cancel Order</button>
                        </div></td>` : ''}
                    </tr>`;
        }).join('')}
                </tbody></table>
            </div>
        </div></div>` : ''}
        ${packedNoInvoice.length ? `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
            <h3 style="font-size:1rem;margin:0">📦 Packed — Awaiting Invoice</h3>
            ${isAdmin ? `<button class="btn btn-primary btn-sm" onclick="bulkGenerateInvoicesFromPacked()">🧾 Bulk Generate Invoices</button>` : ''}
        </div>
        <div class="card" style="margin-bottom:24px"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>
                    ${isAdmin ? '<th style="width:40px"><input type="checkbox" id="chk-all-packed" onchange="toggleAllPacked(this)"></th>' : ''}
                    <th>Order #</th><th>Party</th><th>Packer</th><th>Time</th><th>Packages</th><th>Total</th><th>Actions</th>
                </tr></thead>
                <tbody>${packedNoInvoice.map(o => `<tr>
                    ${isAdmin ? `<td><input type="checkbox" class="chk-packed-order" value="${o.id}"></td>` : ''}
                    <td style="font-weight:600">${o.orderNo}</td><td>${o.partyName}</td><td>${o.packedBy || '-'}</td>
                    <td style="font-size:0.8rem">${o.packingDurationMins !== undefined ? o.packingDurationMins + ' min' : '-'}</td>
                    <td style="font-size:0.8rem">${o.boxCount ? o.boxCount + ' Boxes<br><span style="color:var(--text-muted);font-size:0.75rem">' + (o.packageNumbers || []).join() + '</span>' : '-'}</td>
                    <td class="amount-green">${currency(o.packedTotal || o.total)}</td>
                    <td>${isAdmin ? `<button class="btn btn-outline btn-sm" onclick="generateInvoiceFromPacked('${o.id}')">🧾 Generate</button>` : '<span class="badge badge-warning">Awaiting Admin</span>'}</td>
                </tr>`).join('')}</tbody></table>
            </div>
        </div></div>` : ''}
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:14px">
            <h3 style="font-size:1rem;margin:0">📋 Packed History</h3>
            <div class="filter-group">
                <input type="date" id="pack-hist-from" value="${savedFrom}" onchange="window._packHistFrom=this.value;renderPacking()" style="width:140px">
                <input type="date" id="pack-hist-to" value="${savedTo}" onchange="window._packHistTo=this.value;renderPacking()" style="width:140px">
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Order #</th><th>Party</th><th>Packer</th><th>Time</th><th>Packages</th><th>Invoice</th><th>Total</th></tr></thead>
                <tbody>${filteredHistory.length ? filteredHistory.map(o => `<tr>
                    <td style="font-weight:600">${o.orderNo}</td><td>${o.partyName}</td><td>${o.packedBy || '-'}</td>
                    <td style="font-size:0.8rem">${o.packingDurationMins !== undefined ? o.packingDurationMins + ' min' : '-'}</td>
                    <td style="font-size:0.8rem">${(o.packageNumbers||[]).length ? (o.packageNumbers||[]).length + ' pkg<br><span style="color:var(--text-muted);font-size:0.75rem">' + (o.packageNumbers||[]).join(', ') + '</span>' : '-'}</td>
                    <td><span class="badge badge-success">${o.invoiceNo || '-'}</span></td><td class="amount-green">${currency(o.total)}</td>
                </tr>`).join('') : '<tr><td colspan="7" class="empty-state"><p>No packed history for selected range</p></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}

async function clearCannotComplete(orderId) {
    try {
        await DB.update('salesorders', orderId, {
            cannotComplete: false, cannotCompleteReason: null, cannotCompleteNotes: null,
            cannotCompleteBy: null, cannotCompleteAt: null, cannotCompleteLines: null,
            assignedPacker: null
        });
        renderPacking();
        showToast('Order moved back to packing queue', 'info');
    } catch(err) { alert('Error: ' + err.message); }
}

async function cancelOrderFromPacking(orderId) {
    const orders = DB.cache['sales_orders'] || [];
    const o = orders.find(x => x.id === orderId);
    if (!confirm(`Cancel order ${o ? o.orderNo : orderId}? This cannot be undone.`)) return;
    try {
        await DB.update('salesorders', orderId, {
            status: 'rejected', rejectReason: 'Cancelled from Packing queue',
            packed: false, cannotComplete: false
        });
        renderPacking();
        showToast('Order cancelled', 'error');
    } catch(err) { alert('Error: ' + err.message); }
}

async function selfAssign(orderId) {
    try {
        await DB.update('salesorders', orderId, { assignedPacker: currentUser.name });
        renderPacking();
        showToast('Order assigned to you', 'success');
    } catch(err) { alert('Error: ' + err.message); }
}

function openAssignPackerModal(orderId) {
    const orders = DB.cache['sales_orders'] || DB.cache['db_salesorders'] || [];
    const o = orders.find(x => x.id === orderId);
    if (!o) return;

    // Show all users — Admin can assign to anyone
    const allUsers = DB.cache['users'] || DB.cache['db_users'] || [];
    const packingUsers = allUsers.map(u => u.name).filter(Boolean);

    openModal(`Assign Packer for Order ${o.orderNo}`, `
        <div style="margin-bottom:14px"><strong>Customer:</strong> ${o.partyName} | <strong>Order Total:</strong> ${currency(o.total)}</div>
        <div class="form-group"><label>Select Packer (Uses from Users list) *</label>
            <select id="f-assign-packer"><option value="">Select Packer</option>${packingUsers.map(p => `<option value="${p}">${p}</option>`).join('')}</select>
        </div>
        ${!packingUsers.length ? '<div style="font-size:0.8rem;color:var(--warning);margin-bottom:10px">⚠️ No packing users found.</div>' : ''}
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="executeAssignPacker('${orderId}')">✅ Assign</button></div>`);
}

async function executeAssignPacker(orderId) {
    const packer = $('f-assign-packer').value;
    if (!packer) return alert('Select a packer');
    try {
        await DB.update('salesorders', orderId, { assignedPacker: packer });
        closeModal();
        renderPacking();
        showToast(`Order assigned to ${packer}`, 'success');
    } catch (err) {
        alert('Error assigning packer: ' + (err.message || JSON.stringify(err)));
    }
}

function startPacking(orderId) {
    console.log('Starting packing for order:', orderId);
    openPackModal(orderId);
}

// BUG-017 fix: track when packing started per order (so duration is accurate)
window._packingStartTimes = window._packingStartTimes || {};

function openPackModal(orderId) {
    const orders = DB.get('db_salesorders');
    const o = orders.find(x => x.id === orderId); 
    if (!o) {
        console.error('Order not found for packing:', orderId, 'Orders in cache:', orders.length);
        showToast('Order not found. Please refresh and try again.', 'error', 5000);
        return;
    }
    // Record packing start time the first time this order is opened for packing
    if (!window._packingStartTimes[orderId]) {
        window._packingStartTimes[orderId] = new Date().toISOString();
    }
    const inv = DB.get('db_inventory');

    // Check if assigned
    const assignedName = o.assignedPacker || currentUser.name;

    openModal(`Pack Order ${o.orderNo}`, `
        <div style="margin-bottom:14px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px">
            <div><strong>Customer:</strong> ${o.partyName} | <strong>Order Total:</strong> ${currency(o.total)}</div>
            <div><strong>Assigned To:</strong> <span class="badge badge-info">${assignedName}</span></div>
        </div>
        <h4 style="margin-bottom:10px;font-size:0.9rem">Items — Adjust Picked Qty & UOM</h4>
        <div style="border:1px solid var(--border);border-radius:var(--radius-sm);overflow-x:auto;margin-bottom:14px">
            <table class="data-table" style="margin:0;min-width:640px"><thead><tr><th style="width:32px">SL</th><th style="width:64px">Photo</th><th>Item</th><th>Order</th><th>Stock</th><th>Pack Qty</th><th>MRP Batch</th><th>UOM</th><th>Dis%</th><th>Dis₹</th><th>Picked</th><th>St.</th></tr></thead>
            <tbody>${o.items.map((li, idx) => {
        const item = inv.find(x => x.id === li.itemId);
        let displayStock = 0;
        let uomOptions = '';
        if (item) {
            displayStock = item.stock;
            const uomList = [{ name: item.unit || 'Pcs', factor: 1, price: li.price || item.salePrice || 0 }, ...(item.uoms || [])];
            const currentUom = li.selectedUom || li.uom || item.unit || 'Pcs';
            uomOptions = uomList.map(u =>
                `<option value="${u.name}" data-factor="${u.factor}" data-price="${u.price}" ${u.name === currentUom ? 'selected' : ''}>${u.name}</option>`
            ).join('');
        }
        // MRP batch selection
        const activeBatches = item ? getActiveBatches(item) : [];
        let mrpSelectorHtml = '';
        if (activeBatches.length > 1) {
            mrpSelectorHtml = `<select id="pack-mrp-${idx}" onchange="packMrpSelected(${idx})" style="padding:4px;border-radius:4px;border:2px solid var(--warning);font-size:0.82rem;color:var(--warning);font-weight:600">
                <option value="">⚠️ Confirm MRP</option>
                ${activeBatches.map(b => `<option value="${b.salePrice}" data-mrp="${b.mrp}">MRP ₹${b.mrp} → Sale ₹${b.salePrice} (Qty:${b.qty})</option>`).join('')}
            </select>`;
        } else if (activeBatches.length === 1) {
            mrpSelectorHtml = `<span style="font-size:0.8rem;color:var(--success)">MRP ₹${activeBatches[0].mrp}</span>
                <input type="hidden" id="pack-mrp-${idx}" value="${activeBatches[0].salePrice}" data-confirmed="1">`;
        } else {
            mrpSelectorHtml = `<span style="font-size:0.78rem;color:var(--text-muted)">-</span>`;
        }

        // Photo cell
        const photoCell = item && item.photo
            ? `<td style="text-align:center;padding:4px">
                <img src="${item.photo}" id="pack-photo-${idx}"
                    style="width:52px;height:52px;object-fit:cover;border-radius:6px;cursor:pointer;border:2px solid var(--border)"
                    onclick="packViewPhoto('${li.itemId}','${escapeHtml(li.name)}','${o.id}')" title="Click to enlarge">
               </td>`
            : `<td style="text-align:center;padding:4px">
                <div id="pack-photo-${idx}"
                    style="width:52px;height:52px;background:var(--bg-input);border:2px dashed var(--border);border-radius:6px;display:flex;flex-direction:column;align-items:center;justify-content:center;cursor:pointer;gap:2px;margin:auto"
                    onclick="packAddItemPhoto('${li.itemId}',${idx})"
                    title="No photo — tap to add (optional)">
                    <span style="font-size:1.1rem">📷</span>
                    <span style="font-size:0.58rem;color:var(--text-muted);line-height:1.2">Add<br>Photo</span>
                </div>
               </td>`;

        return `<tr id="pack-row-${idx}">
                    <td>${idx + 1}</td>
                    ${photoCell}
                    <td class="wrap-text" style="font-size:0.88rem;font-weight:600;min-width:140px">${escapeHtml(li.name)}</td>
                    <td style="font-weight:600;text-align:center">${li.qty}</td>
                    <td style="text-align:center"><span id="pack-stock-badge-${idx}" class="badge ${displayStock < li.qty ? 'badge-danger' : 'badge-success'}">${displayStock}</span></td>
                    <td><input type="number" id="pack-qty-${idx}" value="${li.qty}" min="0" oninput="packLineChanged(${idx}, '${li.itemId}', ${o.items.length})" onchange="packLineChanged(${idx}, '${li.itemId}', ${o.items.length})" style="width:65px;padding:4px;border-radius:4px;border:1px solid var(--border);text-align:center"></td>
                    <td>${mrpSelectorHtml}</td>
                    <td><select id="pack-uom-${idx}" onchange="packLineChanged(${idx}, '${li.itemId}', ${o.items.length})" style="padding:4px;border-radius:4px;border:1px solid var(--border)">${uomOptions}</select></td>
                    <td style="font-size:0.75rem;text-align:center">${li.discountPct || 0}%</td>
                    <td style="font-size:0.75rem;text-align:center">${currency(li.discountAmt || 0)}</td>
                    <td style="text-align:center"><input type="checkbox" id="pack-picked-${idx}" onchange="checkAllPicked(${o.items.length})" style="width:18px;height:18px;cursor:pointer"></td>
                    <td style="text-align:center"><span id="pack-status-${idx}" title="Not Picked">🔴</span></td>
                    <input type="hidden" id="pack-price-${idx}" value="${li.price}">
                    <input type="hidden" id="pack-discount-pct-${idx}" value="${li.discountPct || 0}">
                    <input type="hidden" id="pack-discount-amt-${idx}" value="${li.discountAmt || 0}">
                </tr>`;
    }).join('')}</tbody></table>
        </div>
        <input type="hidden" id="f-pack-packer" value="${assignedName}">
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px">
            <button class="btn btn-outline btn-sm" onclick="markAllPicked(${o.items.length})">☑️ Mark All as Picked</button>
            <button class="btn btn-outline btn-sm" onclick="showBarcodeScanner('${orderId}')">📷 Scan Barcode</button>
        </div>`,
        `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
         <button class="btn btn-danger" onclick="cannotCompletePacking('${orderId}')">❌ Cannot Complete</button>
         <button class="btn btn-primary" id="btn-complete-packing" disabled onclick="completePacking('${orderId}')">✅ Complete Packing</button>`);

    setTimeout(() => {
        o.items.forEach((li, idx) => {
            if ($(`pack-uom-${idx}`)) packLineChanged(idx, li.itemId, o.items.length);
        });
        checkAllPicked(o.items.length);
    }, 100);
}

// --- Packing Photo Helpers ---
function packViewPhoto(itemId, itemName, orderId) {
    // Open a fullscreen-style view of the product photo
    const inv = DB.get('db_inventory');
    const item = inv.find(x => x.id === itemId);
    if (!item || !item.photo) return;
    openModal(itemName, `
        <div style="text-align:center;padding:8px">
            <img src="${item.photo}" style="max-width:100%;max-height:70vh;border-radius:10px;object-fit:contain">
        </div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="${orderId ? `openPackModal('${orderId}')` : 'closeModal()'}">← Back to Order</button></div>`);
}

function packAddItemPhoto(itemId, rowIdx) {
    // Create a temporary file input and trigger it
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/*';
    input.capture = 'environment'; // prefer rear camera on mobile
    input.style.display = 'none';
    document.body.appendChild(input);
        input.onchange = async function () {
            const file = input.files[0];
            document.body.removeChild(input);
            if (!file) return;

            showToast('Processing photo...', 'info');
            try {
                const dataUrl = await compressImage(file, { maxWidth: 1024, quality: 0.75 });
                await DB.update('inventory', itemId, { photo: dataUrl });
                // Replace placeholder cell in packing table in-place
                const cell = document.getElementById('pack-photo-' + rowIdx);
                if (cell) {
                    const inv2 = DB.get('db_inventory');
                    const item = inv2.find(x => x.id === itemId);
                    const name = item ? item.name : '';
                    cell.outerHTML = `<img src="${dataUrl}" id="pack-photo-${rowIdx}"
                        style="width:52px;height:52px;object-fit:cover;border-radius:6px;cursor:pointer;border:2px solid var(--success)"
                        onclick="packViewPhoto('${itemId}','${escapeHtml(name)}','')" title="Click to enlarge">`;
                }
                showToast('Photo saved!', 'success');
            } catch (err) {
                console.error('Image compression/save error:', err);
                showToast('Failed to save photo: ' + err.message, 'error');
            }
        };
    input.oncancel = () => { document.body.removeChild(input); };
    input.click();
}

// --- Barcode Verification ---
function showBarcodeScanner(orderId) {
    const readerHtml = `
        <div style="text-align:center;padding:20px">
            <div style="width:100%;height:240px;background:rgba(0,0,0,0.05);border-radius:12px;display:flex;flex-direction:column;align-items:center;justify-content:center;border:2px dashed var(--border);margin-bottom:20px;position:relative;overflow:hidden">
                <span style="font-size:3rem;margin-bottom:10px">📷</span>
                <p style="color:var(--text-secondary);font-weight:500">Camera view would appear here</p>
                <div style="width:100%;height:3px;background:var(--accent);position:absolute;top:0;left:0;box-shadow:0 0 15px var(--accent);animation:scan-line 3s infinite linear;z-index:2"></div>
            </div>
            <p style="font-size:0.9rem;margin-bottom:16px;color:var(--text-muted)">Point your camera at the item barcode or SKU to verify the pack quantity.</p>
            <div class="form-group"><input id="f-barcode-manual" placeholder="Or enter manual SKU/Barcode..." style="text-align:center;letter-spacing:1px;font-weight:600"></div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="openPackModal('${orderId}')">Cancel</button>
                <button class="btn btn-primary" onclick="verifyBarcode($('f-barcode-manual').value, '${orderId}')">Verify & Add</button>
            </div>
        </div>
        <style>
            @keyframes scan-line {
                0% { top: 0; opacity: 0; }
                15% { opacity: 1; }
                85% { opacity: 1; }
                100% { top: 100%; opacity: 0; }
            }
        </style>`;
    openModal('Barcode / SKU Verification', readerHtml);
    setTimeout(() => $('f-barcode-manual').focus(), 100);
}

async function verifyBarcode(code, orderId) {
    if (!code) return;
    const inv = await DB.getAll('inventory');
    const codeClean = code.trim().toLowerCase();
    const item = inv.find(i => (i.itemCode || '').toLowerCase() === codeClean || i.name.toLowerCase().includes(codeClean));
    
    if (item) {
        showToast(`Verified: ${item.name}`, 'success');
        // Re-open packing modal (ideally would scroll to the item or highlight it)
        openPackModal(orderId);
    } else {
        showToast('Item not found or invalid barcode', 'error');
    }
}

function packLineChanged(idx, itemId, totalItemsCount) {
    const inv = DB.get('db_inventory');
    const item = inv.find(x => x.id === itemId);
    if (!item) return;

    const uomSel = $(`pack-uom-${idx}`);
    if (!uomSel) return;

    const opt = uomSel.options[uomSel.selectedIndex];
    const factor = parseFloat(opt.dataset.factor || 1);
    const price = parseFloat(opt.dataset.price || item.price || 0);

    // Update hidden price so we use the correct one for completion
    const priceInput = $(`pack-price-${idx}`);
    if (priceInput) priceInput.value = price;

    const qtyInput = $(`pack-qty-${idx}`);
    const currentQty = qtyInput ? parseFloat(qtyInput.value || 0) : 0;

    // Trigger the picked checkbox validation using the known array length
    checkAllPicked(totalItemsCount);

    // Calculate stock in relation to chosen UOM
    const stockInThisUom = Math.floor(item.stock / factor);

    // Update stock badge
    const badge = $(`pack-stock-badge-${idx}`);
    if (badge) {
        badge.textContent = stockInThisUom;
        badge.className = `badge ${stockInThisUom < currentQty ? 'badge-danger' : 'badge-success'}`;
    }
}

function packMrpSelected(idx) {
    const sel = $(`pack-mrp-${idx}`);
    if (!sel || !sel.value) return;
    const priceInput = $(`pack-price-${idx}`);
    if (priceInput) priceInput.value = sel.value;
    // Visual feedback
    sel.style.borderColor = 'var(--success)';
    sel.style.color = 'var(--success)';
    checkAllPicked(document.querySelectorAll('[id^="pack-qty-"]').length);
}

function checkAllPicked(rowCount) {
    let allValid = true;
    let anyToBePacked = false;
    for (let idx = 0; idx < rowCount; idx++) {
        const qtyCtrl = $(`pack-qty-${idx}`);
        const pickedCtrl = $(`pack-picked-${idx}`);
        const statusBadge = $(`pack-status-${idx}`);
        if (!qtyCtrl || !pickedCtrl) continue;

        const qty = parseFloat(qtyCtrl.value || 0);
        if (qty > 0) {
            anyToBePacked = true;
            if (pickedCtrl.checked) {
                if (statusBadge) { statusBadge.textContent = '🟢'; statusBadge.title = 'Picked ✅'; }
            } else {
                allValid = false;
                if (statusBadge) { statusBadge.textContent = '🟡'; statusBadge.title = 'Qty set — tick Picked checkbox to confirm'; }
            }
        } else {
            pickedCtrl.checked = false;
            if (statusBadge) { statusBadge.textContent = '🔴'; statusBadge.title = 'Not Picked / Short qty'; }
        }
    }
    const btn = $('btn-complete-packing');
    if (btn) {
        btn.disabled = !(allValid && anyToBePacked);
        btn.title = !anyToBePacked ? 'No items have qty > 0' : !allValid ? 'Tick the Picked checkbox for every item to enable' : '';
    }
}

function markAllPicked(rowCount) {
    for (let idx = 0; idx < rowCount; idx++) {
        const qtyCtrl = $(`pack-qty-${idx}`);
        const pickedCtrl = $(`pack-picked-${idx}`);
        if (!qtyCtrl || !pickedCtrl) continue;
        const qty = parseFloat(qtyCtrl.value || 0);
        if (qty > 0) pickedCtrl.checked = true;
    }
    checkAllPicked(rowCount);
    showToast('All items marked as picked', 'success', 2000);
}

function cannotCompletePacking(orderId) {
    const orders = DB.get('db_salesorders');
    const o = orders.find(x => x.id === orderId);
    if (!o) return;
    // Collect line-wise picked state before opening reason dialog
    const lineStatus = o.items.map((li, idx) => {
        const qtyCtrl = $(`pack-qty-${idx}`);
        const pickedCtrl = $(`pack-picked-${idx}`);
        const pickedQty = qtyCtrl ? parseFloat(qtyCtrl.value || 0) : li.qty;
        const picked = pickedCtrl ? pickedCtrl.checked : false;
        return { ...li, pickedQty, picked };
    });
    const reasonHtml = `
        <p style="margin-bottom:12px;color:var(--text-secondary);font-size:0.9rem">Please provide a reason why this order cannot be completed, and confirm the current picking status per line.</p>
        <div style="border:1px solid var(--border);border-radius:6px;overflow:hidden;margin-bottom:14px">
            <table class="data-table" style="margin:0">
                <thead><tr><th>SL</th><th>Item</th><th>Ordered</th><th>Picked Qty</th><th>Picked?</th></tr></thead>
                <tbody>${lineStatus.map((li, idx) => `<tr>
                    <td>${idx + 1}</td>
                    <td>${li.name}</td>
                    <td>${li.qty} ${li.uom || ''}</td>
                    <td><strong style="color:${li.pickedQty < li.qty ? 'var(--danger)' : 'var(--success)'}">${li.pickedQty}</strong></td>
                    <td>${li.picked ? '<span class="badge badge-success">✅ Yes</span>' : '<span class="badge badge-danger">❌ No</span>'}</td>
                </tr>`).join('')}
                </tbody>
            </table>
        </div>
        <div class="form-group"><label>Reason for Incomplete Packing *</label>
            <select id="f-cannot-reason">
                <option value="">Select Reason</option>
                <option>Out of Stock</option>
                <option>Item Damaged</option>
                <option>Partial Stock Available</option>
                <option>Wrong Items Received</option>
                <option>Other</option>
            </select>
        </div>
        <div class="form-group"><label>Additional Notes</label>
            <input id="f-cannot-notes" placeholder="Additional details...">
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="startPacking('${orderId}')">← Back</button>
            <button class="btn btn-danger" onclick="confirmCannotComplete('${orderId}', ${JSON.stringify(lineStatus).replace(/"/g, '&quot;')})">❌ Mark as Cannot Complete</button>
        </div>`;
    openModal(`Cannot Complete — Order ${o.orderNo}`, reasonHtml);
}

async function confirmCannotComplete(orderId, lineStatus) {
    const reason = $('f-cannot-reason').value;
    if (!reason) return alert('Please select a reason');
    const notes = $('f-cannot-notes') ? $('f-cannot-notes').value.trim() : '';
    try {
        await DB.update('salesorders', orderId, {
            cannotComplete: true, cannotCompleteReason: reason, cannotCompleteNotes: notes,
            cannotCompleteBy: currentUser.name, cannotCompleteAt: new Date().toISOString(),
            cannotCompleteLines: lineStatus
        });
        closeModal(); renderPacking();
        showToast(`Order flagged as Cannot Complete — ${reason}`, 'warning', 4000);
    } catch(err) { alert('Error: ' + err.message); }
}

function completePacking(orderId) {
    const packer = $('f-pack-packer').value;
    const orders = DB.get('db_salesorders');
    const o = orders.find(x => x.id === orderId); if (!o) return;

    const packedItems = o.items.map((li, idx) => {
        const qtyInput = $('pack-qty-' + idx);
        const uomSelInput = $('pack-uom-' + idx);
        const priceInput = $('pack-price-' + idx);
        const dPctInput = $(`pack-discount-pct-${idx}`);
        const dAmtInput = $(`pack-discount-amt-${idx}`);

        const packedQty = Math.max(0, qtyInput ? +qtyInput.value : li.qty);
        const selectedUom = uomSelInput ? uomSelInput.value : (li.uom || 'Pcs');
        const price = priceInput ? parseFloat(priceInput.value) : li.price;
        const discountPct = dPctInput ? parseFloat(dPctInput.value) || 0 : (li.discountPct || 0);
        const discountAmt = dAmtInput ? parseFloat(dAmtInput.value) || 0 : (li.discountAmt || 0);
        
        const factor = uomSelInput && uomSelInput.options.length ? parseFloat(uomSelInput.options[uomSelInput.selectedIndex].dataset.factor) : 1;
        // Total amount for the line = (Qty * Price) - Discount
        const amount = +( (packedQty * price) - discountAmt ).toFixed(2);

        return { ...li, packedQty, selectedUom, uom: selectedUom, price, discountPct, discountAmt, amount, factor };
    }).filter(li => li.packedQty > 0);

    if (!packedItems.length) return alert('At least one item must have a packed quantity > 0');
    // Check MRP confirmation for multi-batch items
    let mrpPending = false;
    o.items.forEach((_li, idx) => {
        const mrpSel = $(`pack-mrp-${idx}`);
        if (mrpSel && mrpSel.tagName === 'SELECT' && !mrpSel.value) mrpPending = true;
    });
    if (mrpPending) return alert('Please confirm the MRP batch for all items before completing packing.');

    const packingEndTime = new Date();
    // BUG-017 fix: use tracked start time (from when modal was first opened)
    const startTimeStr = window._packingStartTimes[orderId] || o.packingStartTime;
    const packingStartTime = startTimeStr ? new Date(startTimeStr) : packingEndTime;
    let durationMins = Math.round((packingEndTime - packingStartTime) / 60000);
    if (durationMins < 1) durationMins = 1;

    openModal(`Package Details \u2014 ${o.orderNo}`, `
        <div style="margin-bottom:14px;color:var(--text-secondary);font-size:0.9rem">
            Please confirm package details before completing. Delivery relies on accurate package tracking.
        </div>
        <div class="stats-grid-sm">
            <div class="stat-card blue" style="padding:10px"><div class="stat-label">Pack Duration</div><div class="stat-value" style="font-size:1.2rem">${durationMins} Min</div></div>
            <div class="stat-card green" style="padding:10px"><div class="stat-label">Order Total</div><div class="stat-value" style="font-size:1.2rem">${currency(packedItems.reduce((s, li) => s + li.amount, 0))}</div></div>
        </div>
        
        <div class="form-row">
            <div class="form-group"><label>\ud83d\udce6 Total Boxes</label><input type="number" id="f-pkg-totalBoxes" value="1" min="0" onchange="onPkgCountChange()"></div>
            <div class="form-group"><label>\ud83d\udccb Total Crates</label><input type="number" id="f-pkg-totalCrates" value="0" min="0" onchange="onPkgCountChange()"></div>
        </div>
        
        <div id="pkg-box-section">
            <div class="form-group">
                <label style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                    <span>\ud83d\udce6 Box Numbers</span>
                    <button type="button" class="btn btn-outline btn-sm" onclick="addPkgInputRow('box')" style="padding:4px 8px;font-size:0.8rem">+ Add Box</button>
                </label>
                <div id="pkg-box-container">
                    <div class="pkg-input-row" style="display:flex;gap:8px;margin-bottom:8px">
                        <input type="text" class="f-box-number-input" value="" placeholder="Box number (required)" required style="flex:1">
                        <button type="button" class="btn-icon" onclick="removePkgInputRow(this,'box')" style="color:var(--danger)">\ud83d\uddd1\ufe0f</button>
                    </div>
                </div>
            </div>
        </div>
        
        <div id="pkg-crate-section" style="display:none">
            <div class="form-group">
                <label style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                    <span>\ud83d\udccb Crate Numbers</span>
                    <button type="button" class="btn btn-outline btn-sm" onclick="addPkgInputRow('crate')" style="padding:4px 8px;font-size:0.8rem">+ Add Crate</button>
                </label>
                <div id="pkg-crate-container"></div>
            </div>
        </div>
        
        <small style="color:var(--text-muted);display:block;margin-top:4px">Each input represents one physical box or crate. Numbers must be unique for today across all users.</small>

        <input type="hidden" id="f-pkg-orderId" value="${orderId}">
        <input type="hidden" id="f-pkg-packer" value="${packer}">
        <input type="hidden" id="f-pkg-endTime" value="${packingEndTime.toISOString()}">
        <input type="hidden" id="f-pkg-duration" value="${durationMins}">
        <input type="hidden" id="f-pkg-items" value="${encodeURIComponent(JSON.stringify(packedItems))}">

        <div class="modal-actions" style="margin-top:20px">
            <button class="btn btn-outline" onclick="openPackModal('${orderId}')">\u2190 Back to Items</button>
            <button class="btn btn-primary" onclick="finalizePacking()">\u2705 Save & Complete</button>
        </div>`);
}

function onPkgCountChange() {
    const totalBoxes = Math.max(0, +($('f-pkg-totalBoxes').value) || 0);
    const totalCrates = Math.max(0, +($('f-pkg-totalCrates').value) || 0);
    const boxSection = $('pkg-box-section'); if (boxSection) boxSection.style.display = totalBoxes > 0 ? '' : 'none';
    const crateSection = $('pkg-crate-section'); if (crateSection) crateSection.style.display = totalCrates > 0 ? '' : 'none';
    syncPkgInputRows('box', totalBoxes);
    syncPkgInputRows('crate', totalCrates);
}

function syncPkgInputRows(type, count) {
    const containerId = type === 'box' ? 'pkg-box-container' : 'pkg-crate-container';
    const container = $(containerId); if (!container) return;
    const className = type === 'box' ? 'f-box-number-input' : 'f-crate-number-input';
    const existing = container.querySelectorAll('.pkg-input-row');
    const currentCount = existing.length;
    if (count > currentCount) {
        for (let i = 0; i < count - currentCount; i++) {
            const rowNum = currentCount + i + 1;
            const div = document.createElement('div');
            div.className = 'pkg-input-row';
            div.style.cssText = 'display:flex;gap:8px;margin-bottom:8px;align-items:center';
            div.innerHTML = `<span style="min-width:24px;color:var(--text-muted);font-size:0.85rem">${rowNum}.</span>
                <input type="text" id="${className}-${rowNum}" name="${className}-${rowNum}" class="${className}" value="" placeholder="${type === 'box' ? 'Box' : 'Crate'} number (required)" required style="flex:1">
                <button type="button" class="btn-icon" onclick="removePkgInputRow(this,'${type}')" style="color:var(--danger)">🗑️</button>`;
            container.appendChild(div);
        }
    } else if (count < currentCount) {
        for (let i = currentCount - 1; i >= count; i--) existing[i].remove();
    }
}

function addPkgInputRow(type) {
    const countId = type === 'box' ? 'f-pkg-totalBoxes' : 'f-pkg-totalCrates';
    const el = $(countId); if (el) el.value = (+el.value || 0) + 1;
    syncPkgInputRows(type, +el.value);
}

function removePkgInputRow(btn, type) {
    const containerId = type === 'box' ? 'pkg-box-container' : 'pkg-crate-container';
    const container = $(containerId); if (!container) return;
    btn.parentElement.remove();
    const countId = type === 'box' ? 'f-pkg-totalBoxes' : 'f-pkg-totalCrates';
    const el = $(countId); if (el) el.value = Math.max(0, (+el.value || 1) - 1);
}

async function finalizePacking() {
    const orderId = $('f-pkg-orderId').value;
    const packer = $('f-pkg-packer').value;
    const endTime = $('f-pkg-endTime').value;
    const packedItems = JSON.parse(decodeURIComponent($('f-pkg-items').value));
    const totalBoxes = +($('f-pkg-totalBoxes').value) || 0;
    const totalCrates = +($('f-pkg-totalCrates').value) || 0;

    if (totalBoxes + totalCrates < 1) return alert('Enter at least 1 box or crate');

    const boxInputs = Array.from(document.querySelectorAll('.f-box-number-input'));
    const crateInputs = Array.from(document.querySelectorAll('.f-crate-number-input'));

    // Mandatory: if boxes > 1 or crates > 1, ALL numbers must be filled
    if (totalBoxes > 0) {
        const emptyBox = boxInputs.find(inp => !inp.value.trim());
        if (emptyBox) return alert('Box number is mandatory for each box. Please fill all box numbers.');
    }
    if (totalCrates > 0) {
        const emptyCrate = crateInputs.find(inp => !inp.value.trim());
        if (emptyCrate) return alert('Crate number is mandatory for each crate. Please fill all crate numbers.');
    }

    const boxNumbers = boxInputs.map(inp => inp.value.trim()).filter(s => s);
    const crateNumbers = crateInputs.map(inp => inp.value.trim()).filter(s => s);
    const allPkgNumbers = [...boxNumbers, ...crateNumbers];

    // Check for duplicates within form
    const seen = new Set();
    for (const p of allPkgNumbers) { if (seen.has(p)) return alert(`Duplicate package number "${p}" in this form!`); seen.add(p); }

    // Cross-user same-day duplicate validation: compare against all orders packed today
    const todayDate = new Date().toISOString().split('T')[0]; // "YYYY-MM-DD"
    const orders = DB.cache['sales_orders'] || DB.cache['db_salesorders'] || [];
    let duplicateError = '';
    for (const so of orders) {
        if (so.id === orderId || !so.packedAt) continue;
        const packedDate = so.packedAt.substring(0, 10);
        if (packedDate !== todayDate) continue;
        const usedNums = [...(so.boxNumbers || []), ...(so.crateNumbers || []), ...(so.packageNumbers || [])];
        for (const num of allPkgNumbers) {
            if (usedNums.includes(num)) { duplicateError = `Package number "${num}" already used in order ${so.orderNo} today!`; break; }
        }
        if (duplicateError) break;
    }
    if (duplicateError) return alert(duplicateError);

    const o = orders.find(x => x.id === orderId); if (!o) return alert('Order not found');
    const packedTotal = packedItems.reduce((s, li) => s + li.amount, 0);

    try {
        const durationMins = +($('f-pkg-duration').value) || 1;
        await DB.update('salesorders', orderId, {
            packed: true,
            packedBy: packer,
            packedAt: endTime || new Date().toISOString(),
            packingDurationMins: durationMins,
            packedItems,
            packedTotal,
            packageNumbers: allPkgNumbers,
            boxCount: totalBoxes,
            crateCount: totalCrates
        });
        closeModal();
        renderPacking();
        showToast(`Order ${o.orderNo} packed — ${totalBoxes} box(es), ${totalCrates} crate(s)!`, 'success');
    } catch (err) {
        alert('Error saving packing: ' + (err.message || JSON.stringify(err)));
    }
}



async function generateInvoiceFromPacked(orderId) {
    try {
        const orders = await DB.getAll('salesorders');
        const o = orders.find(x => x.id === orderId);
        if (!o) return alert('Order not found');
        if (!o.packed) return alert('Order is not packed yet');
        if (o.invoiceNo) return alert('Invoice already generated: ' + o.invoiceNo);

        const packedItems = o.packedItems && o.packedItems.length ? o.packedItems : o.items;
        const [parties, inv] = await Promise.all([DB.getAll('parties'), DB.getAll('inventory')]);
        const invNo = await nextNumber('INV-');
        const vyaparNo = buildVyaparInvoiceNo();

        // Pre-fill global invoiceItems from packed order lines
        invoiceItems = packedItems.map(li => {
            const qty = li.packedQty !== undefined ? li.packedQty : li.qty;
            const price = li.price || li.salePrice || 0;
            const discountAmt = li.discountAmt || 0;
            const discountPct = li.discountPct || 0;
            return {
                itemId: li.itemId,
                name: li.name,
                qty,
                price,
                listedPrice: price,
                discountAmt,
                discountPct,
                amount: +( (qty * price) - discountAmt ).toFixed(2),
                unit: li.uom || li.unit || 'Pcs',
                primaryQty: qty
            };
        });

        const filteredParties = parties.filter(p => p.type === 'Customer');
        const party = parties.find(p => String(p.id) === String(o.partyId));
        const partyValue = party ? party.name : (o.partyName || '');

        openModal('Sale Invoice — Verify & Post', `
            <div style="background:rgba(249,115,22,0.1);border:1px solid var(--warning);border-radius:6px;padding:8px 12px;margin-bottom:14px;font-size:0.85rem">
                📦 Pre-filled from packing <strong>${escapeHtml(o.orderNo)}</strong> — verify details then click <strong>Save Invoice</strong>.
            </div>
            <div class="form-row">
                <div class="form-group"><label>Invoice #</label><input id="f-inv-no" value="${escapeHtml(invNo)}"></div>
                <div class="form-group"><label>Date</label><input type="date" id="f-inv-date" value="${today()}"></div>
            </div>
            <input type="hidden" id="f-inv-type" value="sale">
            <input type="hidden" id="f-inv-from-order" value="${orderId}">
            <div class="form-group">
                <label>Vyapar Invoice No. <span style="color:var(--error,#ef4444)">*</span></label>
                <div class="vyapar-inv-row">
                    <input id="f-vyapar-inv-no" value="${escapeHtml(vyaparNo)}" placeholder="e.g. PT-NS-1">
                    <button class="vyapar-gear-btn" onclick="openVyaparInvoiceNoModal()" title="Change prefix / number">⚙️</button>
                </div>
            </div>
            <div class="form-group"><label>Customer *</label>
                <input id="f-inv-party" value="${escapeHtml(partyValue)}" placeholder="Type name or mobile...">
            </div>
            <hr style="border-color:var(--border);margin:14px 0">
            <h4 style="margin-bottom:10px;font-size:0.9rem">Items</h4>
            <div class="form-row-3" style="margin-bottom:8px">
                <div class="form-group"><label>Item</label><input id="f-inv-item-input" placeholder="Type item name or code..."></div>
                <div class="form-group"><label>Qty</label><input type="number" id="f-inv-qty" value="1" min="1"></div>
                <div class="form-group"><label>UOM</label><select id="f-inv-uom" onchange="onInvUomChange()"><option value="">--</option></select></div>
                <div class="form-group"><label>Price ₹</label><input type="number" id="f-inv-price" value="" min="0" step="0.01" placeholder="Listed"></div>
                <div class="form-group"><label>&nbsp;</label><button class="btn btn-primary btn-block" onclick="addInvoiceLine()">Add</button></div>
            </div>
            <div id="inv-lines-list"></div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:8px">
                <div class="form-group" style="min-width:100px">
                    <label>GST %</label>
                    <input type="number" id="f-inv-gst" value="0" min="0" step="0.1" onchange="updateInvoiceTotal()">
                </div>
                <div class="form-group" style="min-width:100px">
                    <label>Round Off ₹</label>
                    <input type="number" id="f-inv-roundoff" value="0" step="0.01" placeholder="0.00" oninput="updateInvoiceTotal()">
                </div>
                <div class="form-group" style="align-self:flex-end">
                    <button class="btn btn-outline btn-sm" onclick="autoRoundOff()">⟳ Auto</button>
                </div>
            </div>
            <div id="inv-total-display" style="text-align:right;font-size:1rem;color:var(--text-secondary);font-weight:600;margin-top:4px">Total: ₹0.00</div>
            
            <div style="display:flex; justify-content:flex-end; gap:12px; align-items:flex-end; margin-top:8px; flex-wrap:wrap">
                <div class="form-group" style="width:90px; margin-bottom:0">
                    <label style="font-size:0.65rem">Disc %</label>
                    <input type="number" id="f-inv-disc-pct" value="0" min="0" max="100" step="0.01" oninput="updateInvoiceTotal()">
                </div>
                <div class="form-group" style="width:90px; margin-bottom:0">
                    <label style="font-size:0.65rem">Disc ₹</label>
                    <input type="number" id="f-inv-disc-amt" value="0" min="0" step="0.01" oninput="updateInvoiceTotal()">
                </div>
            </div>
            
            <div id="inv-advance-section" style="margin-top:10px"></div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
                <button class="btn btn-primary" onclick="saveInvoice()">Save Invoice</button>
            </div>
        `);

        initSearchDropdown('f-inv-party', buildPartySearchList(filteredParties), function(p) {
            loadAvailableAdvances(p.id);
        });
        // Restore pre-filled value after dropdown init
        const partyEl = $('f-inv-party');
        if (partyEl && partyValue) partyEl.value = partyValue;

        _invItemDropdown = initSearchDropdown('f-inv-item-input', buildItemSearchList(inv), function(item) {
            $('f-inv-price').value = item.salePrice || '';
            const uomSel = $('f-inv-uom');
            if (uomSel) {
                uomSel.innerHTML = '<option value="' + (item.unit || 'Pcs') + '">' + (item.unit || 'Pcs') + '</option>';
                if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
            }
        });

        renderInvoiceLines();
        if (party) loadAvailableAdvances(party.id);
    } catch (err) {
        alert('Error: ' + err.message);
    }
}

function toggleAllPacked(chk) {
    const rowChks = document.querySelectorAll('.chk-packed-order');
    rowChks.forEach(c => c.checked = chk.checked);
}

async function bulkGenerateInvoicesFromPacked() {
    const selectedBoxes = [...document.querySelectorAll('.chk-packed-order:checked')];
    if (!selectedBoxes.length) return alert('Select at least one packed order to generate invoices.');

    try {
        const [orders, parties, inventory] = await Promise.all([
            DB.getAll('salesorders'), DB.getAll('parties'), DB.getAll('inventory')
        ]);

        let startInvNumber = parseInt((await nextNumber('INV-')).split('-')[1]);
        let startVyaparNumber = parseInt((buildVyaparInvoiceNo()).split('-').pop()) || 1;
        const autoPrefix = buildVyaparInvoiceNo().substring(0, buildVyaparInvoiceNo().lastIndexOf('-') + 1) || 'PT-NS-';

        // Build preview list — skip already invoiced / no stock
        const rows = [];
        let skipCount = 0;
        for (const box of selectedBoxes) {
            const orderId = box.value;
            const o = orders.find(x => x.id === orderId);
            if (!o || !o.packed || o.invoiceNo) { skipCount++; continue; }

            const packedItems = o.packedItems && o.packedItems.length ? o.packedItems : o.items;
            let hasStock = true;
            for (const li of packedItems) {
                const item = inventory.find(x => x.id === li.itemId);
                const qty = li.packedQty !== undefined ? li.packedQty : li.qty;
                if (!item || ((item.stock || 0) < qty && !DB.getObj('db_company').allowNegativeStock)) { hasStock = false; break; }
            }
            if (!hasStock && !DB.getObj('db_company').allowNegativeStock) { skipCount++; continue; }

            const invNo = 'INV-' + String(startInvNumber).padStart(4, '0');
            const vyaparNo = autoPrefix + startVyaparNumber;
            const sub = packedItems.reduce((s, li) => { const q = li.packedQty !== undefined ? li.packedQty : li.qty; return s + q * (li.price || li.salePrice || 0); }, 0);
            rows.push({ orderId, orderNo: o.orderNo, partyId: o.partyId, partyName: o.partyName, invNo, vyaparNo, sub });
            startInvNumber++;
            startVyaparNumber++;
        }

        if (!rows.length) return alert('No eligible orders found (check stock or already invoiced).');

        const skipNote = skipCount ? `<p style="color:#ef4444;font-size:0.8rem;margin-bottom:10px">⚠️ ${skipCount} order(s) skipped (already invoiced or insufficient stock).</p>` : '';

        openModal('Verify Bulk Invoices', `
        ${skipNote}
        <p style="font-size:0.83rem;color:var(--text-muted);margin-bottom:12px">
            Review below. Edit <strong>Vyapar No</strong> if needed, then click <strong>Confirm & Generate</strong>.
        </p>
        <div style="overflow-x:auto;max-height:55vh;overflow-y:auto">
        <table class="data-table" style="font-size:0.82rem;min-width:480px">
            <thead><tr>
                <th>#</th><th>Order</th><th>Party</th>
                <th>Invoice No</th><th>Vyapar No</th><th style="text-align:right">Amount</th>
            </tr></thead>
            <tbody>
            ${rows.map((r, i) => `<tr>
                <td style="color:var(--text-muted)">${i+1}</td>
                <td><strong>${r.orderNo}</strong></td>
                <td style="font-size:0.78rem">${r.partyName}</td>
                <td><span style="font-weight:700;color:var(--accent)">${r.invNo}</span></td>
                <td><input id="bv-vyapar-${i}" class="form-control" value="${r.vyaparNo}" style="padding:4px 8px;font-size:0.82rem;min-width:110px"></td>
                <td style="text-align:right;font-weight:600">${currency(r.sub)}</td>
            </tr>`).join('')}
            </tbody>
        </table>
        </div>
        <input type="hidden" id="bv-rows-json" value="${encodeURIComponent(JSON.stringify(rows))}">
        <div class="modal-actions" style="margin-top:16px">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="confirmBulkInvoices()">✅ Confirm & Generate (${rows.length})</button>
        </div>`);

    } catch (err) {
        alert('Bulk Error: ' + err.message);
    }
}

async function confirmBulkInvoices() {
    const rowsEl = document.getElementById('bv-rows-json');
    if (!rowsEl) return;
    const rows = JSON.parse(decodeURIComponent(rowsEl.value));

    // Read edited Vyapar numbers from inputs
    rows.forEach((r, i) => {
        const inp = document.getElementById('bv-vyapar-' + i);
        if (inp) r.vyaparNo = inp.value.trim() || r.vyaparNo;
    });

    closeModal();
    showToast('Generating invoices…', 'info');

    try {
        const [orders, parties, inventory] = await Promise.all([
            DB.getAll('salesorders'), DB.getAll('parties'), DB.getAll('inventory')
        ]);

        let successCount = 0;
        for (const r of rows) {
            const o = orders.find(x => x.id === r.orderId);
            if (!o) continue;

            const packedItems = o.packedItems && o.packedItems.length ? o.packedItems : o.items;
            const party = parties.find(p => String(p.id) === String(o.partyId));
            const invoiceItems = packedItems.map(li => {
                const qty = li.packedQty !== undefined ? li.packedQty : li.qty;
                const price = li.price || li.salePrice || 0;
                const discountAmt = li.discountAmt || 0;
                const discountPct = li.discountPct || 0;
                return { 
                    itemId: li.itemId, 
                    name: li.name, 
                    qty, 
                    price, 
                    listedPrice: price, 
                    discountAmt,
                    discountPct,
                    amount: +( (qty * price) - discountAmt ).toFixed(2), 
                    unit: li.uom || li.unit || 'Pcs', 
                    primaryQty: qty 
                };
            });

            const sub = invoiceItems.reduce((s, li) => s + li.amount, 0);
            const roundoff = +(Math.round(sub) - sub).toFixed(2);
            const total = sub + roundoff;

            const ops = [];
            for (const li of invoiceItems) {
                const item = inventory.find(x => x.id === li.itemId);
                if (!item) continue;
                const newStock = (item.stock || 0) - li.qty;
                const itemUpdate = { stock: newStock };
                if (item.batches && item.batches.length) {
                    const { updatedBatches, priceSync } = deductBatchQtyFifo(item, li.qty);
                    if (updatedBatches) { itemUpdate.batches = updatedBatches; Object.assign(itemUpdate, priceSync); }
                }
                ops.push(DB.rawUpdate('inventory', item.id, itemUpdate));
                ops.push(DB.rawInsert('stock_ledger', { date: today(), itemId: item.id, itemName: item.name, entryType: 'Sale', qty: -li.qty, runningStock: newStock, documentNo: r.invNo, reason: 'Sale Invoice', createdBy: currentUser.name }));
            }

            if (party) {
                const newBal = (party.balance || 0) + total;
                ops.push(DB.rawUpdate('parties', party.id, { balance: newBal }));
                ops.push(DB.rawInsert('party_ledger', { date: today(), partyId: party.id, partyName: party.name, type: 'Sale Invoice', amount: total, balance: newBal, docNo: r.invNo, notes: 'Sale', createdBy: currentUser.name }));
                // Update local cache so next order in loop sees updated balance
                party.balance = newBal;
            }

            const invData = { invoiceNo: r.invNo, date: today(), dueDate: null, type: 'sale', partyId: o.partyId, partyName: o.partyName, items: invoiceItems, subtotal: sub, gst: 0, roundOff: roundoff, total, status: 'from-packing', createdBy: currentUser.name, vyaparInvoiceNo: r.vyaparNo, fromOrder: o.orderNo };
            ops.push(DB.rawInsert('invoices', invData));
            ops.push(DB.rawUpdate('salesorders', o.id, { invoiceNo: r.invNo }));

            await Promise.all(ops);
            incrementVyaparNo();
            successCount++;
        }

        await DB.refreshTables(['invoices', 'inventory', 'parties', 'sales_orders']);
        renderPacking();
        showToast(`✅ ${successCount} invoice(s) generated successfully!`, 'success');
    } catch (err) {
        alert('Bulk Error: ' + err.message);
    }
}

// =============================================
//  DELIVERY (with undelivered returns, re-dispatch, cancel)
// =============================================
const UNDELIVERED_REASONS = ['Customer Not Available', 'Wrong Address', 'Customer Refused', 'Damaged Goods', 'Payment Issue', 'Area Not Accessible', 'Other'];

async function renderDelivery() {
    let [dels, allOrders, allInvoices, allParties] = await Promise.all([
        DB.getAll('delivery'),
        DB.getAll('salesorders'),
        DB.getAll('invoices'),
        DB.getAll('parties')
    ]);

    // User-wise filter: Delivery role only sees their own assignments
    if (currentUser && currentUser.role === 'Delivery') {
        dels = dels.filter(d => d.deliveryPerson === currentUser.name);
    }

    const dispatched = dels.filter(d => d.status === 'Dispatched');
    const delivered = dels.filter(d => d.status === 'Delivered');
    const undelivered = dels.filter(d => d.status === 'Undelivered');
    const returned = dels.filter(d => d.status === 'Returned');
    const cancelled = dels.filter(d => d.status === 'Cancelled');

    // Get packed orders that haven't been dispatched yet
    const readyFromOrders = allOrders.filter(o => o.packed && o.invoiceNo && !dels.some(d => d.orderNo === o.orderNo && d.status !== 'Cancelled'));
    // Get direct sale invoices (not from orders) that haven't been dispatched
    const directInvoices = allInvoices.filter(i => i.type === 'sale' && i.status !== 'cancelled' && !i.fromOrder && !dels.some(d => d.invoiceNo === i.invoiceNo && d.status !== 'Cancelled'));

    const readyToDispatch = [
        ...readyFromOrders.map(o => ({ source: 'order', id: o.id, orderNo: o.orderNo, invoiceNo: o.invoiceNo, partyName: o.partyName, partyId: o.partyId, total: o.total, items: o.packedItems || o.items })),
        ...directInvoices.map(i => ({ source: 'invoice', id: i.id, orderNo: i.invoiceNo, invoiceNo: i.invoiceNo, partyName: i.partyName, partyId: i.partyId, total: i.total, items: i.items }))
    ];

    pageContent.innerHTML = `
        <div class="stats-grid" style="margin-bottom:18px">
            ${canEdit() ? `<div class="stat-card amber"><div class="stat-icon">📦</div><div class="stat-value">${readyToDispatch.length}</div><div class="stat-label">Ready to Dispatch</div></div>` : ''}
            <div class="stat-card blue"><div class="stat-icon">🚚</div><div class="stat-value">${dispatched.length}</div><div class="stat-label">In Transit</div></div>
            <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${delivered.length}</div><div class="stat-label">Delivered</div></div>
            <div class="stat-card red"><div class="stat-icon">↩️</div><div class="stat-value">${undelivered.length + returned.length}</div><div class="stat-label">Undelivered / Returned</div></div>
        </div>
        ${(readyToDispatch.length && canEdit()) ? `
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <h3 style="font-size:1rem;margin:0">📦 Ready to Dispatch (${readyToDispatch.length})</h3>
            <button class="btn btn-primary btn-sm" onclick="openBulkDispatchModal()">🚚 Bulk Dispatch</button>
        </div>
        <div class="card" style="margin-bottom:24px"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>
                    <th style="width:36px"><input type="checkbox" id="chk-disp-all" onchange="document.querySelectorAll('.chk-disp-row').forEach(c=>c.checked=this.checked)"></th>
                    <th>Order #</th><th>Invoice</th><th>Party</th><th>Total</th><th>Action</th>
                </tr></thead>
                <tbody>${readyToDispatch.map(o => `<tr>
                    <td><input type="checkbox" class="chk-disp-row" value="${o.id}" data-source="${o.source}" data-orderno="${o.orderNo}" data-party="${escapeHtml(o.partyName)}"></td>
                    <td style="font-weight:600">${o.orderNo}</td>
                    <td><span class="badge badge-success">${o.invoiceNo || '-'}</span></td>
                    <td>${o.partyName}</td>
                    <td class="amount-green">${currency(o.total)}</td>
                    <td><button class="btn btn-primary btn-sm" onclick="openDispatchModalUnified('${o.id}','${o.source}')">🚚 Dispatch</button></td>
                </tr>`).join('')}</tbody></table>
            </div>
        </div></div>` : ''}
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
            <h3 style="font-size:1rem;margin:0">🚚 All Deliveries</h3>
            <button class="btn btn-outline btn-sm" onclick="printDeliveryRouteSheet()">🖨️ Print Route Sheet</button>
        </div>
        <div class="section-toolbar" style="margin-bottom:12px">
            <div class="filter-group"><button class="btn btn-outline" onclick="openColumnPersonalizer('delivery','renderDelivery')" style="border-color:var(--accent);color:var(--accent)">⚙️ Columns</button><select id="del-status-filter" onchange="filterDelTable()"><option value="">All Statuses</option><option value="Dispatched">In Transit</option><option value="Delivered">Delivered</option><option value="Undelivered">Undelivered</option><option value="Returned">Returned</option><option value="Cancelled">Cancelled</option></select></div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr>${ColumnManager.get('delivery').filter(c=>c.visible).map(c=>`<th>${c.label}</th>`).join('')}</tr></thead>
                <tbody id="del-tbody">${renderDelRows(dels, allParties)}</tbody></table>
            </div>
        </div></div>`;
}
function renderDelRows(dels, parties) {
    if (!dels.length) return '<tr><td colspan="9"><div class="empty-state"><p>No deliveries found</p></div></td></tr>';
    const cols = ColumnManager.get('delivery').filter(c => c.visible);
    return dels.map(d => {
        const party = (parties||[]).find(p => String(p.id) === String(d.partyId));
        const statusBadge = d.status === 'Delivered' ? 'badge-success' : d.status === 'Returned' ? 'badge-danger' : 'badge-warning';
        const pkgNums = d.packageNumbers || [];
        const pkgDisplay = pkgNums.slice(0,3).map(n=>`<span class="badge badge-outline" style="font-size:0.68rem">${n}</span>`).join(' ') + (pkgNums.length>3?` +${pkgNums.length-3}`:'');
        const gpsBtn = party && party.lat && party.lng ? `<button class="btn-icon" onclick="openPartyMap('${party.lat}','${party.lng}','${escapeHtml(d.partyName)}')" title="Navigate" style="font-size:0.8rem">🗺️</button>` : '';
        const actions = `<div class="action-btns">
            <button class="btn-icon" onclick="viewDeliveryDetail('${d.id}')">👁️</button>
            ${d.status !== 'Delivered' ? `<button class="btn btn-primary btn-sm" onclick="markDelivered('${d.id}')">✅ Delivered</button>` : ''}
            ${d.status !== 'Returned' && d.status !== 'Delivered' ? `<button class="btn btn-outline btn-sm" onclick="openUndeliveredModal('${d.id}')">↩ Return</button>` : ''}
        </div>`;
        const partyPhone = party ? (party.phone || '') : '';
        // Location cell: show address/city from party, with delivery confirmation note if delivered
        const partyAddr = party ? (party.address || party.city || '') : '';
        const locationCell = (() => {
            const addrLine = partyAddr ? `<div style="font-size:0.8rem;color:var(--text-muted);max-width:140px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${escapeHtml(partyAddr)}</div>` : '';
            const gpsLine = party && party.lat && party.lng
                ? `<a href="https://www.google.com/maps?q=${party.lat},${party.lng}&z=16" target="_blank" style="font-size:0.72rem;color:var(--accent);text-decoration:none;white-space:nowrap">🗺️ Navigate</a>`
                : '';
            const delLocLine = d.deliveryLocation
                ? `<div style="font-size:0.72rem;color:var(--success);margin-top:2px;max-width:140px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${escapeHtml(d.deliveryLocation)}">✅ ${escapeHtml(d.deliveryLocation)}</div>`
                : (d.deliveryLat ? `<div style="font-size:0.72rem;color:var(--success)"><a href="https://www.google.com/maps?q=${d.deliveryLat},${d.deliveryLng}&z=16" target="_blank" style="color:var(--success);text-decoration:none">✅ GPS Confirmed</a></div>` : '');
            return `<td>${addrLine}${gpsLine}${delLocLine}</td>`;
        })();

        // Person cell: admin/manager can change delivery person before delivery
        const canChangePerson = currentUser && (currentUser.role === 'Admin' || currentUser.role === 'Manager') && d.status !== 'Delivered' && d.status !== 'Cancelled';
        const personCell = `<td>
            <div style="font-weight:600;font-size:0.85rem">${d.deliveryPerson || '-'}</div>
            ${canChangePerson ? `<button class="btn-link" style="font-size:0.72rem;color:var(--accent);background:none;border:none;cursor:pointer;padding:0;margin-top:2px" onclick="openChangeDeliveryPerson('${d.id}')">✏️ Change</button>` : ''}
        </td>`;

        const cellMap = {
            orderNo:     `<td style="font-weight:600">${d.orderNo}</td>`,
            invoiceNo:   `<td><span class="badge badge-success" style="font-size:0.72rem">${d.invoiceNo || '-'}</span></td>`,
            invoiceDate: `<td style="font-size:0.8rem">${d.invoiceDate || d.dispatchedAt || '-'}</td>`,
            party:       `<td>${escapeHtml(d.partyName)}</td>`,
            location:    locationCell,
            phone:       `<td style="white-space:nowrap">${partyPhone
                ? `<a href="tel:${partyPhone}" style="display:inline-flex;align-items:center;gap:5px;color:var(--success);font-weight:600;text-decoration:none;font-size:0.88rem">
                       <span style="font-size:1rem">📞</span>${partyPhone}
                   </a>`
                : '<span style="color:var(--text-muted);font-size:0.82rem">-</span>'}</td>`,
            person:      personCell,
            packages:    `<td style="max-width:180px">${pkgDisplay}<div style="font-size:0.7rem;color:var(--text-muted);margin-top:2px">${pkgNums.length} pkg(s)</div></td>`,
            status:      `<td><span class="badge ${statusBadge}">${d.status}</span></td>`,
            reason:      `<td style="font-size:0.82rem;color:var(--text-muted)">${escapeHtml(d.undeliveredReason || '-')}</td>`,
            actions:     `<td>${actions}</td>`,
        };
        return `<tr>${cols.map(c => cellMap[c.key] || '').join('')}</tr>`;
    }).join('');
}
async function filterDelTable() {
    const st = $('del-status-filter').value;
    let dels = await DB.getAll('delivery');
    if (currentUser && currentUser.role === 'Delivery') {
        dels = dels.filter(d => d.deliveryPerson === currentUser.name);
    }
    if (st) dels = dels.filter(d => d.status === st);
    $('del-tbody').innerHTML = renderDelRows(dels, DB.cache['parties'] || []);
}

async function openChangeDeliveryPerson(id) {
    if (!currentUser || (currentUser.role !== 'Admin' && currentUser.role !== 'Manager')) {
        return alert('Only Admin or Manager can change the delivery person.');
    }
    const dels = DB.cache['delivery'] || [];
    const d = dels.find(x => x.id === id);
    if (!d) return;
    const dp = await DB.getAll('delivery_persons');
    openModal('✏️ Change Delivery Person', `
        <div style="margin-bottom:14px;padding:12px;background:var(--bg-secondary);border-radius:8px;font-size:0.85rem">
            <strong>${d.orderNo}</strong> — ${escapeHtml(d.partyName)}<br>
            <span style="color:var(--text-muted)">Current: <strong>${d.deliveryPerson || '-'}</strong></span>
        </div>
        <div class="form-group"><label>New Delivery Person *</label>
            <select id="f-chg-person">
                <option value="">-- Select --</option>
                ${dp.map(p => `<option value="${escapeHtml(p.name)}" ${p.name === d.deliveryPerson ? 'selected' : ''}>${escapeHtml(p.name)}${p.phone ? ' (' + p.phone + ')' : ''}</option>`).join('')}
            </select>
        </div>
        ${!dp.length ? '<div style="font-size:0.8rem;color:var(--warning);margin-bottom:10px">⚠️ No delivery persons. <a href="#" onclick="closeModal();navigateTo(\'deliverypersons\')" style="color:var(--accent)">Add Now</a></div>' : ''}
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="saveChangeDeliveryPerson('${id}')">Save</button>
        </div>`);
}

async function saveChangeDeliveryPerson(id) {
    const person = ($('f-chg-person') || {}).value;
    if (!person) return alert('Select a delivery person.');
    await DB.update('delivery', id, { deliveryPerson: person });
    closeModal();
    await renderDelivery();
    showToast('Delivery person updated!', 'success');
}

async function viewDeliveryDetail(id) {
    const [dels, allParties, inventory] = await Promise.all([
        DB.getAll('delivery'),
        DB.getAll('parties'),
        DB.getAll('inventory')
    ]);
    const d = dels.find(x => x.id === id);
    if (!d) return alert('Delivery record not found.');
    const party = allParties.find(p => p.id === d.partyId);

    const statusColor = d.status === 'Delivered' ? 'var(--success)' : d.status === 'Returned' ? 'var(--danger)' : 'var(--warning)';

    // Items table with photos
    const items = d.items || [];
    const itemsHtml = items.length ? `
        <div style="margin-bottom:16px">
            <div style="font-weight:600;font-size:0.82rem;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.04em">📦 Items</div>
            <div style="overflow-x:auto;border-radius:8px;border:1px solid var(--border)">
                <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
                    <thead><tr style="background:var(--bg-secondary)">
                        <th style="padding:8px 10px;text-align:left;font-weight:600;font-size:0.72rem;color:var(--text-muted)">Photo</th>
                        <th style="padding:8px 10px;text-align:left;font-weight:600;font-size:0.72rem;color:var(--text-muted)">Item</th>
                        <th style="padding:8px 10px;text-align:right;font-weight:600;font-size:0.72rem;color:var(--text-muted)">Qty</th>
                        <th style="padding:8px 10px;text-align:right;font-weight:600;font-size:0.72rem;color:var(--text-muted)">Rate</th>
                        <th style="padding:8px 10px;text-align:right;font-weight:600;font-size:0.72rem;color:var(--text-muted)">Amount</th>
                    </tr></thead>
                    <tbody>
                        ${items.map(li => {
                            const invItem = inventory.find(x => x.id === li.itemId || x.name === li.name);
                            const photoHtml = invItem && invItem.photo
                                ? `<img src="${invItem.photo}" style="width:44px;height:44px;object-fit:cover;border-radius:6px;border:1px solid var(--border)">`
                                : `<div style="width:44px;height:44px;border-radius:6px;border:1px dashed var(--border);display:flex;align-items:center;justify-content:center;font-size:1.1rem">📦</div>`;
                            const amt = (li.qty || 0) * (li.price || li.salePrice || 0);
                            return `<tr style="border-top:1px solid var(--border)">
                                <td style="padding:8px 10px">${photoHtml}</td>
                                <td style="padding:8px 10px">
                                    <div style="font-weight:600">${escapeHtml(li.name || '')}</div>
                                    ${li.itemCode ? `<div style="font-size:0.7rem;color:var(--text-muted)">${li.itemCode}</div>` : ''}
                                </td>
                                <td style="padding:8px 10px;text-align:right;font-weight:600">${li.qty || 0} ${li.unit || ''}</td>
                                <td style="padding:8px 10px;text-align:right;color:var(--text-muted)">${currency(li.price || li.salePrice || 0)}</td>
                                <td style="padding:8px 10px;text-align:right;font-weight:700;color:var(--success)">${currency(amt)}</td>
                            </tr>`;
                        }).join('')}
                        <tr style="border-top:2px solid var(--border);background:var(--bg-secondary)">
                            <td colspan="4" style="padding:8px 10px;font-weight:700;text-align:right">Total</td>
                            <td style="padding:8px 10px;text-align:right;font-weight:700;color:var(--success)">${currency(d.total || 0)}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>` : '<div style="color:var(--text-muted);font-size:0.85rem;margin-bottom:14px">No item details available.</div>';

    // Location section
    const locationHtml = `
        <div style="margin-bottom:16px">
            <div style="font-weight:600;font-size:0.82rem;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.04em">📍 Location</div>
            ${party && party.lat && party.lng
                ? `<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;background:rgba(52,168,83,0.07);border:1px solid rgba(52,168,83,0.2);border-radius:8px">
                       <div style="flex:1">
                           <div style="font-size:0.85rem;font-weight:600">${escapeHtml(party.address || party.city || d.partyName)}</div>
                           ${party.city ? `<div style="font-size:0.75rem;color:var(--text-muted)">${escapeHtml(party.city)}${party.postCode ? ' — ' + escapeHtml(party.postCode) : ''}</div>` : ''}
                           <div style="font-size:0.7rem;color:var(--text-muted);margin-top:2px">GPS: ${(+party.lat).toFixed(5)}, ${(+party.lng).toFixed(5)}</div>
                       </div>
                       <a href="https://www.google.com/maps?q=${party.lat},${party.lng}&z=16" target="_blank" style="background:#34a853;color:#fff;padding:6px 12px;border-radius:8px;font-size:0.8rem;text-decoration:none;white-space:nowrap">🗺️ Navigate</a>
                   </div>`
                : `<div style="padding:10px 12px;background:rgba(255,193,7,0.07);border:1px solid rgba(255,193,7,0.2);border-radius:8px;font-size:0.82rem;color:var(--text-muted)">
                       ⚠️ No GPS saved for this customer.
                       ${party && (party.address || party.city) ? `<br><span>${escapeHtml(party.address || '')} ${escapeHtml(party.city || '')}</span>` : ''}
                   </div>`
            }
            ${d.deliveryLocation || d.deliveryLat ? `
                <div style="margin-top:8px;padding:10px 12px;background:rgba(99,102,241,0.06);border:1px solid rgba(99,102,241,0.18);border-radius:8px">
                    <div style="font-size:0.72rem;color:var(--text-muted);margin-bottom:4px;font-weight:600">✅ Delivery Confirmation Location</div>
                    ${d.deliveryLocation ? `<div style="font-size:0.85rem;font-weight:600">${escapeHtml(d.deliveryLocation)}</div>` : ''}
                    ${d.deliveryLat ? `<div style="font-size:0.72rem;color:var(--text-muted)">GPS: ${(+d.deliveryLat).toFixed(5)}, ${(+d.deliveryLng).toFixed(5)}</div>
                    <a href="https://www.google.com/maps?q=${d.deliveryLat},${d.deliveryLng}&z=16" target="_blank" style="color:var(--accent);font-size:0.78rem;text-decoration:none">View on map ↗</a>` : ''}
                </div>` : ''}
        </div>`;

    const pkgs = d.packageNumbers || [];
    const pkgHtml = pkgs.length ? `<div style="margin-bottom:12px">
        <span style="font-size:0.78rem;color:var(--text-muted)">Packages: </span>
        ${pkgs.map(n => `<span class="badge badge-outline" style="font-size:0.7rem">${n}</span>`).join(' ')}
    </div>` : '';

    const actionHtml = d.status === 'Dispatched'
        ? `<button class="btn btn-primary" onclick="markDelivered('${d.id}')">✅ Mark Delivered</button>
           <button class="btn btn-outline" onclick="openUndeliveredModal('${d.id}')">↩ Undelivered</button>`
        : d.status === 'Undelivered'
        ? `<button class="btn btn-primary btn-sm" onclick="closeModal();reDispatchOrder('${d.id}')">🚚 Re-Dispatch</button>`
        : '';

    openModal(`Delivery — ${d.orderNo}`, `
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 16px;margin-bottom:16px;padding:12px 14px;background:var(--bg-secondary);border-radius:8px;font-size:0.85rem">
            <div><span style="color:var(--text-muted);font-size:0.75rem">Invoice</span><br><strong>${d.invoiceNo || '-'}</strong></div>
            <div><span style="color:var(--text-muted);font-size:0.75rem">Party</span><br><strong>${escapeHtml(d.partyName)}</strong></div>
            <div><span style="color:var(--text-muted);font-size:0.75rem">Delivery Person</span><br><strong>${d.deliveryPerson || '-'}</strong></div>
            <div><span style="color:var(--text-muted);font-size:0.75rem">Status</span><br><strong style="color:${statusColor}">${d.status}</strong></div>
            <div><span style="color:var(--text-muted);font-size:0.75rem">Dispatched</span><br><strong>${d.dispatchedAt || '-'}</strong></div>
            ${d.deliveredAt ? `<div><span style="color:var(--text-muted);font-size:0.75rem">Delivered</span><br><strong style="color:var(--success)">${d.deliveredAt}</strong></div>` : ''}
            ${d.undeliveredReason ? `<div style="grid-column:span 2"><span style="color:var(--text-muted);font-size:0.75rem">Return Reason</span><br><strong style="color:var(--danger)">${escapeHtml(d.undeliveredReason)}</strong></div>` : ''}
        </div>
        ${pkgHtml}
        ${itemsHtml}
        ${locationHtml}
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Close</button>
            ${actionHtml}
        </div>`);
}

function markDelivered(id) {
    const dels = DB.cache['delivery'] || [];
    const d = dels.find(x => x.id === id);
    const summaryHtml = d ? `<div style="margin-bottom:14px;padding:12px;background:var(--bg-secondary);border-radius:8px;font-size:0.85rem">
        <strong>${d.orderNo}</strong> — ${escapeHtml(d.partyName)}<br>
        <span style="color:var(--text-muted)">Invoice: ${d.invoiceNo || '-'} | ${currency(d.total || 0)}</span>
    </div>` : '';
    openModal('✅ Confirm Delivery', `
        ${summaryHtml}
        <div style="font-weight:600;font-size:0.82rem;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.04em">📍 Delivery Location <span style="font-weight:400;text-transform:none">(Optional)</span></div>
        <div class="form-group">
            <label>Location / Handover Notes</label>
            <input type="text" id="f-del-location" placeholder="e.g. Left at gate, Handed to owner...">
        </div>
        <div style="display:flex;gap:8px;margin-bottom:4px">
            <div class="form-group" style="flex:1;margin-bottom:0"><label>Latitude</label>
                <input type="number" step="any" id="f-del-lat" placeholder="Auto-fill via GPS">
            </div>
            <div class="form-group" style="flex:1;margin-bottom:0"><label>Longitude</label>
                <input type="number" step="any" id="f-del-lng" placeholder="Auto-fill via GPS">
            </div>
        </div>
        <button class="btn btn-outline btn-sm" onclick="captureDeliveryGps()" style="margin-bottom:14px">📍 Use My Live Location</button>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="confirmMarkDelivered('${id}')">✅ Mark as Delivered</button>
        </div>`);
}

function captureDeliveryGps() {
    if (!navigator.geolocation) return alert('Geolocation not supported.');
    const btn = document.querySelector('[onclick="captureDeliveryGps()"]');
    if (btn) { btn.textContent = '⏳ Getting...'; btn.disabled = true; }
    navigator.geolocation.getCurrentPosition(
        pos => {
            const latEl = $('f-del-lat'), lngEl = $('f-del-lng');
            if (latEl) latEl.value = pos.coords.latitude.toFixed(6);
            if (lngEl) lngEl.value = pos.coords.longitude.toFixed(6);
            if (btn) { btn.textContent = '✅ Location Captured'; btn.disabled = false; }
            showToast('Delivery location captured!', 'success');
        },
        err => {
            if (btn) { btn.textContent = '📍 Use My Live Location'; btn.disabled = false; }
            alert('Could not get location: ' + err.message);
        },
        { enableHighAccuracy: true, timeout: 10000 }
    );
}

async function confirmMarkDelivered(id) {
    const location = ($('f-del-location') || {}).value.trim();
    const lat = parseFloat(($('f-del-lat') || {}).value || '');
    const lng = parseFloat(($('f-del-lng') || {}).value || '');
    const update = { status: 'Delivered', deliveredAt: today() };
    if (location) update.deliveryLocation = location;
    if (!isNaN(lat) && !isNaN(lng)) { update.deliveryLat = lat; update.deliveryLng = lng; }
    await DB.update('delivery', id, update);
    closeModal();
    await renderDelivery();
    showToast('Delivery confirmed!', 'success');
}
async function openQuickGpsUpdate(partyId, returnId, returnSource) {
    if (!partyId) return alert('No party linked to this record.');
    const parties = await DB.getAll('parties');
    const party = parties.find(p => p.id === partyId);
    if (!party) return alert('Party not found.');

    // Any user can update GPS — but non-editors see ONLY lat/lng (no other fields)
    const isEditor = canEdit();
    openModal(`📍 Update GPS — ${escapeHtml(party.name)}`, `
        <div style="background:rgba(99,102,241,0.06);border:1px solid rgba(99,102,241,0.18);border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:0.82rem;color:var(--text-muted)">
            Only GPS coordinates will be updated. All other party details remain unchanged.
        </div>
        <div class="form-group">
            <label style="font-weight:600">${escapeHtml(party.name)}</label>
            <div style="font-size:0.82rem;color:var(--text-muted)">${escapeHtml(party.address || '')} ${escapeHtml(party.city || '')} ${escapeHtml(party.postCode || '')}</div>
            ${party.phone ? `<div style="margin-top:4px"><a href="tel:${party.phone}" style="color:var(--success);font-size:0.85rem">📞 ${party.phone}</a></div>` : ''}
        </div>
        <div style="display:flex;gap:8px;margin-bottom:10px">
            <div class="form-group" style="flex:1"><label>Latitude</label>
                <input type="number" step="any" id="qgps-lat" value="${party.lat || ''}" placeholder="e.g. 12.97194">
            </div>
            <div class="form-group" style="flex:1"><label>Longitude</label>
                <input type="number" step="any" id="qgps-lng" value="${party.lng || ''}" placeholder="e.g. 77.59369">
            </div>
        </div>
        ${party.lat && party.lng ? `<div style="margin-bottom:10px;font-size:0.8rem;color:var(--success)">📍 Current: ${(+party.lat).toFixed(5)}, ${(+party.lng).toFixed(5)}</div>` : ''}
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px">
            <button class="btn btn-primary btn-sm" onclick="qgpsLiveLocation()">📍 Use My Live Location</button>
            ${isEditor ? `<button class="btn btn-outline btn-sm" onclick="closeModal();openPartyModal('${partyId}')">✏️ Full Edit</button>` : ''}
        </div>
        <small style="color:var(--text-muted)">💡 Go to the customer location and tap "Use My Live Location" for best accuracy.</small>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="${returnId ? `openDispatchModalUnified('${returnId}','${returnSource}')` : 'closeModal()'}">← Back</button>
            <button class="btn btn-primary" onclick="saveQuickGps('${partyId}','${returnId}','${returnSource}')">Save GPS</button>
        </div>`);
}

function qgpsLiveLocation() {
    if (!navigator.geolocation) return alert('Geolocation not supported.');
    const btn = document.querySelector('[onclick="qgpsLiveLocation()"]');
    if (btn) { btn.textContent = '⏳ Getting...'; btn.disabled = true; }
    navigator.geolocation.getCurrentPosition(
        pos => {
            const latEl = $('qgps-lat'), lngEl = $('qgps-lng');
            if (latEl) latEl.value = pos.coords.latitude.toFixed(6);
            if (lngEl) lngEl.value = pos.coords.longitude.toFixed(6);
            if (btn) { btn.textContent = '✅ Location Set'; btn.disabled = false; }
            showToast('Location captured!', 'success');
        },
        err => {
            if (btn) { btn.textContent = '📍 Use My Live Location'; btn.disabled = false; }
            alert('Could not get location: ' + err.message);
        },
        { enableHighAccuracy: true, timeout: 10000 }
    );
}

async function saveQuickGps(partyId, returnId, returnSource) {
    const lat = parseFloat(($('qgps-lat') || {}).value || '');
    const lng = parseFloat(($('qgps-lng') || {}).value || '');
    if (isNaN(lat) || isNaN(lng)) return alert('Enter valid latitude and longitude.');
    try {
        await DB.update('parties', partyId, { lat, lng });
        showToast('GPS saved!', 'success');
        if (returnId) {
            openDispatchModalUnified(returnId, returnSource);
        } else {
            closeModal();
        }
    } catch(err) {
        alert('Error saving GPS: ' + (err.message || err));
    }
}

async function openDispatchModal(orderId) {
    const orders = await DB.getAll('salesorders');
    const o = orders.find(x => x.id === orderId); if (!o) return;
    const dp = await DB.getAll('delivery_persons');
    openModal(`Dispatch ${o.orderNo}`, `
        <div style="margin-bottom:14px"><strong>Customer:</strong> ${o.partyName} | <strong>Invoice:</strong> ${o.invoiceNo || '-'}</div>
        <div class="form-group"><label>Delivery Person *</label>
            <select id="f-del-person"><option value="">Select</option>${dp.map(p => `<option value="${p.name}">${p.name} (${p.phone || ''})</option>`).join('')}</select>
        </div>
        ${!dp.length ? '<div style="font-size:0.8rem;color:var(--warning);margin-bottom:10px">⚠️ No delivery persons. <a href="#" onclick="closeModal();navigateTo(\'deliverypersons\')" style="color:var(--accent)">Add Now</a></div>' : ''}
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="dispatchOrder('${orderId}')">🚚 Dispatch</button></div>`);
}
function calcDistanceKm(lat1, lng1, lat2, lng2) {
    const R = 6371, toRad = x => x * Math.PI / 180;
    const dLat = toRad(lat2 - lat1), dLng = toRad(lng2 - lng1);
    const a = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function printDeliveryRouteSheet() {
    const [dels, allParties] = await Promise.all([DB.getAll('delivery'), DB.getAll('parties')]);
    const dispatched = dels.filter(d => d.status === 'Dispatched');
    const co = DB.getObj('db_company') || {};
    const warehouseLat = parseFloat(co.warehouseLat || 0);
    const warehouseLng = parseFloat(co.warehouseLng || 0);
    const hasWarehouseGps = warehouseLat !== 0 || warehouseLng !== 0;

    // Group by delivery person
    const byPerson = {};
    dispatched.forEach(d => {
        const name = d.deliveryPerson || 'Unassigned';
        if (!byPerson[name]) byPerson[name] = [];
        const party = allParties.find(p => String(p.id) === String(d.partyId));
        const hasGps = party && party.lat && party.lng;
        const distKm = (hasWarehouseGps && hasGps)
            ? calcDistanceKm(warehouseLat, warehouseLng, +party.lat, +party.lng)
            : null;
        byPerson[name].push({ ...d, _party: party, _hasGps: hasGps, _distKm: distKm });
    });

    const sections = Object.entries(byPerson).sort(([a],[b])=>a.localeCompare(b)).map(([person, dlist]) => {
        // Separate stops into those with GPS and those without
        let unrouted = dlist.filter(d => d._hasGps);
        const withoutGps = dlist.filter(d => !d._hasGps);
        
        const withGps = [];
        let currentLat = warehouseLat;
        let currentLng = warehouseLng;

        // Nearest Neighbor algorithm
        while (unrouted.length > 0) {
            let nearestIdx = -1;
            let minDistance = Infinity;

            for (let i = 0; i < unrouted.length; i++) {
                const stop = unrouted[i];
                // If warehouse GPS is missing but stops have GPS, we can't do the first leg properly.
                // We'll just calculate relative distances assuming the first stop is origin.
                if (!hasWarehouseGps && withGps.length === 0) {
                    nearestIdx = i; // Just pick the first one if we have no starting point
                    minDistance = 0;
                    break;
                }

                const dist = calcDistanceKm(currentLat, currentLng, +stop._party.lat, +stop._party.lng);
                if (dist < minDistance) {
                    minDistance = dist;
                    nearestIdx = i;
                }
            }

            const nearestStop = unrouted[nearestIdx];
            nearestStop._legDistance = minDistance; // Store distance from previous point
            withGps.push(nearestStop);
            unrouted.splice(nearestIdx, 1);

            currentLat = +nearestStop._party.lat;
            currentLng = +nearestStop._party.lng;
        }

        const sorted = [...withGps, ...withoutGps];

        let seq = 0;
        const rows = sorted.map(d => {
            seq++;
            const p = d._party;
            const gpsInfo = d._hasGps
                ? `<span style="color:#34a853;font-size:0.78rem">📍 ${p.address||p.city||''} ${d._legDistance !== undefined && d._legDistance !== null ? `(Leg: ${d._legDistance.toFixed(1)} km)` : ''}</span>`
                : `<span style="color:#f59e0b;font-size:0.78rem">⚠️ No GPS — Manual seq: <input type="number" value="${seq}" style="width:40px;border:1px solid #ddd;border-radius:3px;padding:1px 3px;text-align:center" onchange="this.closest('tr').querySelector('.seq-num').textContent=this.value"></span>`;
            const phone = p ? (p.phone || '-') : '-';
            return `<tr>
                <td style="text-align:center;font-weight:700;font-size:1rem" class="seq-num">${seq}</td>
                <td><strong>${d.orderNo||d.invoiceNo||'-'}</strong><br><span style="font-size:0.75rem;color:#666">${d.invoiceNo||''}</span></td>
                <td><strong>${d.partyName}</strong><br>${gpsInfo}</td>
                <td>${phone !== '-' ? `<a href="tel:${phone}" style="color:#1a73e8">${phone}</a>` : '-'}</td>
                <td>${(d.packageNumbers||[]).join(', ')||'-'}</td>
                <td style="text-align:right">₹${(d.total||0).toFixed(2)}</td>
                <td style="width:90px"></td>
            </tr>`;
        }).join('');

        const total = dlist.reduce((s,d)=>s+(d.total||0),0);
        const gpsCount = withGps.length, noGpsCount = withoutGps.length;
        const totalDistance = withGps.reduce((s,d)=>s+(d._legDistance||0),0);
        const gpsNote = hasWarehouseGps
            ? `<span style="font-size:0.78rem;color:#34a853">📍 ${gpsCount} GPS stops (Route: ${totalDistance.toFixed(1)} km)</span>${noGpsCount ? ` &nbsp; <span style="font-size:0.78rem;color:#f59e0b">⚠️ ${noGpsCount} without GPS</span>` : ''}`
            : `<span style="font-size:0.78rem;color:#f59e0b">⚠️ Warehouse GPS missing — Route distances unoptimised</span>`;

        return `<div style="margin-bottom:32px;page-break-inside:avoid">
        <div style="border-bottom:2px solid #333;padding-bottom:6px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:4px">
            <h3 style="margin:0">🧑‍✈️ ${person} &nbsp;<span style="font-size:0.85rem;color:#666;font-weight:400">Date: ${new Date().toLocaleDateString('en-IN')}</span></h3>
            <div>${gpsNote}</div>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:0.88rem">
        <thead><tr style="background:#f3f4f6">
            <th style="border:1px solid #ddd;padding:6px;width:32px">#</th>
            <th style="border:1px solid #ddd;padding:6px">Order</th>
            <th style="border:1px solid #ddd;padding:6px">Party &amp; Location</th>
            <th style="border:1px solid #ddd;padding:6px">Phone</th>
            <th style="border:1px solid #ddd;padding:6px">Packages</th>
            <th style="border:1px solid #ddd;padding:6px;text-align:right">Amount</th>
            <th style="border:1px solid #ddd;padding:6px">Signature</th>
        </tr></thead>
        <tbody>${rows.replace(/<td(?! style)/g,'<td style="border:1px solid #ddd;padding:5px"')}</tbody>
        <tfoot><tr>
            <td colspan="5" style="border:1px solid #ddd;padding:6px;text-align:right;font-weight:700">Total (${dlist.length} stops)</td>
            <td style="border:1px solid #ddd;padding:6px;text-align:right;font-weight:700">₹${total.toFixed(2)}</td>
            <td style="border:1px solid #ddd"></td>
        </tr></tfoot>
        </table></div>`;
    }).join('');

    const header = `<div style="text-align:center;margin-bottom:16px">
        <h2 style="margin:0">${co.name || 'Delivery Route Sheet'}</h2>
        <div style="font-size:0.85rem;color:#666">Route Sheet &nbsp;|&nbsp; Printed: ${new Date().toLocaleString('en-IN')}${hasWarehouseGps ? ' &nbsp;|&nbsp; 📍 GPS-optimised route' : ''}</div>
    </div><hr style="margin-bottom:16px">`;

    const w = window.open('', '_blank');
    w.document.write(`<!DOCTYPE html><html><head><title>Route Sheet</title>
    <style>body{font-family:sans-serif;padding:20px;max-width:960px;margin:0 auto}@media print{button{display:none}.no-print{display:none}}</style>
    </head><body>${header}${sections || '<p>No dispatched deliveries</p>'}
    <button onclick="window.print()" style="margin-top:16px;padding:8px 20px;background:#f97316;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:1rem">🖨️ Print Route Sheet</button>
    </body></html>`);
    w.document.close();
}

async function openDispatchModalUnified(id, source) {
    const [dp, parties] = await Promise.all([DB.getAll('delivery_persons'), DB.getAll('parties')]);
    let label, partyName, partyId, invoiceNo;
    if (source === 'order') {
        const orders = await DB.getAll('salesorders');
        const o = orders.find(x => x.id === id); if (!o) return;
        label = o.orderNo; partyName = o.partyName; partyId = o.partyId; invoiceNo = o.invoiceNo || '-';
    } else {
        const invoices = await DB.getAll('invoices');
        const i = invoices.find(x => x.id === id); if (!i) return;
        label = i.invoiceNo; partyName = i.partyName; partyId = i.partyId; invoiceNo = i.invoiceNo;
    }
    const party = parties.find(p => p.id === partyId);
    const gpsHtml = (party && party.lat && party.lng)
        ? `<div style="margin-bottom:14px;padding:10px 14px;background:rgba(52,168,83,0.08);border:1px solid rgba(52,168,83,0.3);border-radius:8px;display:flex;align-items:center;gap:12px">
               <span style="font-size:1.4rem">📍</span>
               <div style="flex:1">
                   <div style="font-size:0.8rem;color:var(--text-muted);margin-bottom:2px">Customer GPS Location</div>
                   <div style="font-size:0.85rem;font-weight:600">${escapeHtml(party.address || party.city || partyName)}</div>
                   <div style="font-size:0.75rem;color:var(--text-muted)">${(+party.lat).toFixed(5)}, ${(+party.lng).toFixed(5)}</div>
               </div>
               <a href="https://www.google.com/maps?q=${party.lat},${party.lng}&z=16" target="_blank" style="background:#34a853;color:#fff;padding:7px 14px;border-radius:8px;font-size:0.85rem;text-decoration:none;white-space:nowrap">🗺️ Navigate</a>
           </div>`
        : `<div style="margin-bottom:14px;padding:10px 14px;background:rgba(255,193,7,0.08);border:1px solid rgba(255,193,7,0.3);border-radius:8px;display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
               <div style="font-size:0.82rem;color:var(--text-muted)">⚠️ No GPS saved for <strong>${escapeHtml(partyName)}</strong>. Route sheet won't include this stop.</div>
               <button class="btn btn-outline btn-sm" style="border-color:var(--warning);color:var(--warning);white-space:nowrap" onclick="openQuickGpsUpdate('${partyId || ''}','${id}','${source}')">📍 Add GPS Now</button>
           </div>`;
    openModal(`Dispatch ${label}`, `
        <div style="margin-bottom:14px"><strong>Customer:</strong> ${escapeHtml(partyName)} | <strong>Invoice:</strong> ${invoiceNo}</div>
        ${gpsHtml}
        <div class="form-group"><label>Delivery Person *</label>
            <select id="f-del-person"><option value="">Select</option>${dp.map(p => `<option value="${p.name}">${p.name} (${p.phone || ''})</option>`).join('')}</select>
        </div>
        ${!dp.length ? '<div style="font-size:0.8rem;color:var(--warning);margin-bottom:10px">⚠️ No delivery persons. <a href="#" onclick="closeModal();navigateTo(\'deliverypersons\')" style="color:var(--accent)">Add Now</a></div>' : ''}
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="dispatchOrderUnified('${id}','${source}')">🚚 Dispatch</button></div>`);
}
async function dispatchOrderUnified(id, source) {
    const person = $('f-del-person').value; if (!person) return alert('Select delivery person');
    let orderNo, partyName, partyId, invoiceNo, total, items, packageNumbers = [], invoiceDate = '';

    if (source === 'order') {
        const orders = await DB.getAll('salesorders');
        const o = orders.find(x => x.id === id); if (!o) return;
        orderNo = o.orderNo; partyName = o.partyName; partyId = o.partyId;
        invoiceNo = o.invoiceNo || ''; total = o.total; items = o.packedItems || o.items;
        packageNumbers = o.packageNumbers || [];
    } else {
        const invoices = await DB.getAll('invoices');
        const i = invoices.find(x => x.id === id); if (!i) return;
        orderNo = i.invoiceNo; partyName = i.partyName; partyId = i.partyId;
        invoiceNo = i.invoiceNo; total = i.total; items = i.items;
    }

    if (invoiceNo) {
        const invoices = await DB.getAll('invoices');
        const inv = invoices.find(i => i.invoiceNo === invoiceNo);
        if (inv) invoiceDate = inv.date || '';
    }

    const delData = {
        orderId: id, orderNo, partyName, partyId, invoiceNo,
        invoiceDate, packageNumbers, deliveryPerson: person,
        status: 'Dispatched', dispatchedAt: today(), total, items
    };

    await DB.insert('delivery', delData);
    closeModal();
    await renderDelivery();
    showToast(`${orderNo} dispatched with ${person}!`, 'success');
}

async function dispatchOrder(orderId) {
    const person = $('f-del-person').value; if (!person) return alert('Select delivery person');
    const orders = await DB.getAll('salesorders');
    const o = orders.find(x => x.id === orderId); if (!o) return;

    const delData = {
        orderId: o.id, orderNo: o.orderNo, partyName: o.partyName,
        partyId: o.partyId, invoiceNo: o.invoiceNo || '',
        deliveryPerson: person, status: 'Dispatched',
        dispatchedAt: today(), total: o.total, items: o.items
    };

    await DB.insert('delivery', delData);
    closeModal();
    await renderDelivery();
    showToast(`${o.orderNo} dispatched!`, 'success');
}

async function updateDeliveryStatus(id, status) {
    const update = { status };
    if (status === 'Delivered') update.deliveredAt = today();
    await DB.update('delivery', id, update);
    await renderDelivery();
}

function openUndeliveredModal(id) {
    openModal('Mark as Undelivered', `
        <div class="form-group"><label>Reason *</label>
            <select id="f-undel-reason">${UNDELIVERED_REASONS.map(r => `<option>${r}</option>`).join('')}</select>
        </div>
        <div class="form-group"><label>Additional Notes</label><input id="f-undel-notes" placeholder="Optional details..."></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-danger" onclick="markUndelivered('${id}')">↩️ Mark Undelivered</button></div>`);
}

async function markUndelivered(id) {
    const reason = $('f-undel-reason').value + ($('f-undel-notes').value.trim() ? ' — ' + $('f-undel-notes').value.trim() : '');
    await DB.update('delivery', id, {
        status: 'Undelivered',
        undeliveredReason: reason,
        undeliveredAt: today()
    });
    closeModal();
    await renderDelivery();
}
function confirmReturn(id) {
    const dels = DB.cache['delivery'] || [];
    const d = dels.find(x => x.id === id);
    if (!d) return;
    openModal('Confirm Return', `
        <div style="margin-bottom:14px;padding:12px;background:rgba(255,193,7,0.1);border:1px solid rgba(255,193,7,0.3);border-radius:8px;font-size:0.9rem">
            <strong>Order:</strong> ${d.orderNo} | <strong>Party:</strong> ${d.partyName}
        </div>
        <p style="margin-bottom:16px;font-size:0.9rem">Confirm that goods have been returned to the warehouse?</p>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="executeConfirmReturn('${id}')">📦 Confirm Return</button></div>`);
}
async function executeConfirmReturn(id) {
    try {
        await DB.update('delivery', id, {
            status: 'Returned',
            returnedAt: today()
        });
        closeModal();
        await renderDelivery();
        showToast('Return confirmed!', 'success');
    } catch (e) {
        alert('Error: ' + e.message);
        closeModal();
    }
}
async function openBulkDispatchModal() {
    const selected = [...document.querySelectorAll('.chk-disp-row:checked')];
    if (!selected.length) return alert('Select at least one order to dispatch.');
    const dp = await DB.getAll('delivery_persons');
    const rows = selected.map(c => ({
        id: c.value, source: c.dataset.source,
        orderNo: c.dataset.orderno, party: c.dataset.party
    }));
    openModal('Bulk Dispatch', `
    <p style="font-size:0.85rem;color:var(--text-muted);margin-bottom:14px">
        Dispatching <strong>${rows.length}</strong> order(s) to the same delivery person.
    </p>
    <div style="max-height:200px;overflow-y:auto;margin-bottom:16px;border:1px solid var(--border);border-radius:8px;padding:8px">
        ${rows.map((r,i) => `<div style="display:flex;justify-content:space-between;font-size:0.85rem;padding:5px 4px;${i?'border-top:1px solid var(--border)':''}">
            <span style="font-weight:600">${r.orderNo}</span>
            <span style="color:var(--text-muted)">${r.party}</span>
        </div>`).join('')}
    </div>
    <div class="form-group">
        <label>Delivery Person *</label>
        <select id="f-bulk-del-person" class="form-control">
            <option value="">— Select Person —</option>
            ${dp.map(p => `<option value="${p.name}">${p.name}${p.phone ? ' ('+p.phone+')' : ''}</option>`).join('')}
        </select>
        ${!dp.length ? '<p style="font-size:0.78rem;color:var(--warning);margin-top:6px">⚠️ No delivery persons found. <a href="#" onclick="closeModal();navigateTo(\'deliverypersons\')" style="color:var(--accent)">Add Now</a></p>' : ''}
    </div>
    <input type="hidden" id="bulk-disp-rows" value="${encodeURIComponent(JSON.stringify(rows))}">
    <div class="modal-actions">
        <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="confirmBulkDispatch()">🚚 Dispatch All (${rows.length})</button>
    </div>`);
}

async function confirmBulkDispatch() {
    const person = $('f-bulk-del-person').value;
    if (!person) { alert('Select a delivery person'); return; }
    const rows = JSON.parse(decodeURIComponent($('bulk-disp-rows').value));
    closeModal();
    showToast('Dispatching…', 'info');
    try {
        const [orders, invoices] = await Promise.all([DB.getAll('salesorders'), DB.getAll('invoices')]);
        const ops = rows.map(r => {
            let delData;
            if (r.source === 'order') {
                const o = orders.find(x => x.id === r.id);
                if (!o) return null;
                delData = { orderId: o.id, orderNo: o.orderNo, partyName: o.partyName, partyId: o.partyId, invoiceNo: o.invoiceNo || '', deliveryPerson: person, status: 'Dispatched', dispatchedAt: today(), total: o.total, items: o.items };
            } else {
                const inv = invoices.find(x => x.id === r.id);
                if (!inv) return null;
                delData = { orderId: inv.id, orderNo: inv.invoiceNo, partyName: inv.partyName, partyId: inv.partyId, invoiceNo: inv.invoiceNo, deliveryPerson: person, status: 'Dispatched', dispatchedAt: today(), total: inv.total, items: inv.items };
            }
            return DB.rawInsert('delivery', delData);
        }).filter(Boolean);
        await Promise.all(ops);
        await DB.refreshTables(['delivery']);
        await renderDelivery();
        showToast(`✅ ${ops.length} order(s) dispatched to ${person}!`, 'success');
    } catch (err) {
        alert('Bulk Dispatch Error: ' + err.message);
    }
}

async function reDispatchOrder(id) {
    const dels = await DB.getAll('delivery');
    const d = dels.find(x => x.id === id);
    if (!d) { alert('Delivery record not found'); return; }
    const dp = await DB.getAll('delivery_persons');
    openModal('Re-Dispatch ' + d.orderNo, `
        <div style="margin-bottom:14px;padding:10px;background:rgba(255,193,7,0.1);border:1px solid rgba(255,193,7,0.3);border-radius:8px;font-size:0.85rem"><strong>Return Reason:</strong> ${d.undeliveredReason || 'N/A'}</div>
        <div class="form-group"><label>Delivery Person *</label>
            <select id="f-redel-person"><option value="">Select</option>${dp.map(p => `<option value="${p.name}">${p.name} (${p.phone || ''})</option>`).join('')}</select>
        </div>
        ${!dp.length ? '<div style="font-size:0.8rem;color:var(--warning);margin-bottom:10px">⚠️ No delivery persons. <a href="#" onclick="closeModal();navigateTo(\'deliverypersons\')" style="color:var(--accent)">Add Now</a></div>' : ''}
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="executeReDispatch('${id}')">🚚 Re-Dispatch</button></div>`);
}

async function executeReDispatch(id) {
    const person = $('f-redel-person').value;
    if (!person) { $('f-redel-person').focus(); return; }
    try {
        const dels = await DB.getAll('delivery');
        const d = dels.find(x => x.id === id);
        if (!d) { closeModal(); return; }

        // Mark old delivery as cancelled
        await DB.update('delivery', d.id, {
            status: 'Cancelled',
            cancelReason: 'Re-dispatched'
        });

        // Create new dispatch entry
        const newDel = {
            orderId: d.orderId, orderNo: d.orderNo, partyName: d.partyName,
            partyId: d.partyId, invoiceNo: d.invoiceNo, deliveryPerson: person,
            status: 'Dispatched', dispatchedAt: today(), total: d.total,
            items: d.items, reDispatchOf: d.id
        };
        await DB.insert('delivery', newDel);

        closeModal();
        await renderDelivery();
        showToast(`Re-dispatched ${d.orderNo} with ${person}!`, 'success');
    } catch (e) {
        alert('Error: ' + e.message);
        closeModal();
    }
}
async function cancelDeliveryInvoice(id) {
    const dels = await DB.getAll('delivery');
    const d = dels.find(x => x.id === id);
    if (!d) return;
    openModal('Cancel Invoice', `
        <div style="margin-bottom:14px;padding:12px;background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.3);border-radius:8px;font-size:0.9rem">
            <strong>Order:</strong> ${d.orderNo} | <strong>Invoice:</strong> ${d.invoiceNo || 'N/A'}<br>
            <strong>Party:</strong> ${d.partyName}
        </div>
        <p style="margin-bottom:16px;font-size:0.9rem">Cancel this invoice? Stock will be restored and party balance adjusted.</p>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Keep Invoice</button>
        <button class="btn btn-danger" onclick="executeCancelDeliveryInvoice('${id}')">❌ Cancel Invoice</button></div>`);
}
async function executeCancelDeliveryInvoice(id) {
    const dels = await DB.getAll('delivery');
    const d = dels.find(x => x.id === id);
    if (!d) { closeModal(); alert('Record not found.'); return; }

    try {
        // Mark delivery as cancelled
        await DB.update('delivery', d.id, {
            status: 'Cancelled',
            cancelReason: 'Invoice cancelled'
        });

        // Use core cancellation logic if invoice exists
        if (d.invoiceNo) {
            const invoices = await DB.getAll('invoices');
            const inv = invoices.find(i => i.invoiceNo === d.invoiceNo);
            if (inv) {
                await executeCancelInvoice(inv.id);
            } else {
                alert('Invoice ' + d.invoiceNo + ' not found.');
            }
        }

        closeModal();
        await renderDelivery();
        showToast('Invoice and delivery cancelled.', 'warning');
    } catch (err) {
        alert('Error: ' + err.message);
        closeModal();
    }
}

// =============================================
//  REPORTS
// =============================================
function exportTableToExcel(tableId, filename) {
    const table = document.getElementById(tableId);
    if (!table) return alert('No data to export');
    let csv = [];
    const rows = table.querySelectorAll('tr');
    rows.forEach(row => {
        const cols = row.querySelectorAll('th, td');
        const rowData = [];
        cols.forEach(col => {
            // Skip action columns with buttons
            if (col.querySelector('button')) { rowData.push(''); return; }
            let text = col.innerText.replace(/[\n\r]+/g, ' ').replace(/,/g, ' ');
            rowData.push('"' + text + '"');
        });
        csv.push(rowData.join(','));
    });
    const csvContent = csv.join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = (filename || 'Report') + '_' + today() + '.csv';
    link.click();
    showToast('Report exported!', 'success');
}
function renderReports() {
    pageContent.innerHTML = `
        <div class="report-grid">
            <div class="report-card" onclick="showReport('payment-report')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(16,185,129,0.12),rgba(249,115,22,0.08))"><div class="report-icon">💰</div></div><div class="report-text"><h4>Payment Report</h4><p>Pay In / Pay Out with filters</p></div></div>
            <div class="report-card" onclick="showReport('sales')"><div class="report-icon-wrap"><div class="report-icon">💹</div></div><div class="report-text"><h4>Sales Report</h4><p>Sales invoices summary</p></div></div>
            <div class="report-card" onclick="showReport('purchases')"><div class="report-icon-wrap"><div class="report-icon">🛒</div></div><div class="report-text"><h4>Purchase Report</h4><p>Purchase invoices summary</p></div></div>
            <div class="report-card" onclick="showReport('usersales')"><div class="report-icon-wrap"><div class="report-icon">👤</div></div><div class="report-text"><h4>User Sales</h4><p>Detailed salesman performance</p></div></div>
            <div class="report-card" onclick="showReport('userpayments')"><div class="report-icon-wrap"><div class="report-icon">💸</div></div><div class="report-text"><h4>User Collections</h4><p>Detailed salesman collections</p></div></div>
            <div class="report-card" onclick="showReport('pnl')"><div class="report-icon-wrap"><div class="report-icon">📊</div></div><div class="report-text"><h4>Profit & Loss</h4><p>Revenue vs expenses</p></div></div>
            <div class="report-card" onclick="showReport('invoice-pnl')"><div class="report-icon-wrap"><div class="report-icon">🧾</div></div><div class="report-text"><h4>Invoice P&L</h4><p>Profit per invoice</p></div></div>
            <div class="report-card" onclick="showReport('stock')"><div class="report-icon-wrap"><div class="report-icon">📦</div></div><div class="report-text"><h4>Stock Summary</h4><p>Current inventory levels</p></div></div>
            <div class="report-card" onclick="showReport('outstanding')"><div class="report-icon-wrap"><div class="report-icon">💰</div></div><div class="report-text"><h4>Outstanding</h4><p>Party balances</p></div></div>
            <div class="report-card" onclick="showReport('expenses')"><div class="report-icon-wrap"><div class="report-icon">💸</div></div><div class="report-text"><h4>Expense Summary</h4><p>Category-wise breakdown</p></div></div>
            <div class="report-card" onclick="showReport('chequeregister')"><div class="report-icon-wrap"><div class="report-icon">📝</div></div><div class="report-text"><h4>Cheque Register</h4><p>Track cheque deposits & clearance</p></div></div>
            <div class="report-card" onclick="showReport('salesman')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(124,58,237,0.12),rgba(99,102,241,0.08))"><div class="report-icon">🏆</div></div><div class="report-text"><h4>Salesman Performance</h4><p>Invoices + collections by salesman</p></div></div>
            <div class="report-card" onclick="showReport('user-outstanding')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(239,68,68,0.12),rgba(249,115,22,0.08))"><div class="report-icon">👤💰</div></div><div class="report-text"><h4>Outstanding by User</h4><p>Pending bills grouped by salesman</p></div></div>
            <div class="report-card" onclick="showReport('collection-allocations')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(59,130,246,0.12),rgba(37,99,235,0.08))"><div class="report-icon">👤💳</div></div><div class="report-text"><h4>Collection Allocations</h4><p>Track assigned invoices & payments</p></div></div>
            <div class="report-card" onclick="showReport('daybook')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(20,184,166,0.12),rgba(6,182,212,0.08))"><div class="report-icon">📒</div></div><div class="report-text"><h4>Day Book</h4><p>Date-wise transaction summary</p></div></div>
        </div>

        <div class="section-toolbar" style="margin-top:28px">
            <h3>📒 Vyapar Import Reports</h3>
        </div>
        <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">Line-wise detailed reports for re-entry into Vyapar accounting software.</p>
        <div class="report-grid">
            <div class="report-card" onclick="showReport('vyapar-sales')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(22,163,74,0.12),rgba(16,185,129,0.08))"><div class="report-icon">🧾</div></div><div class="report-text"><h4>Vyapar Sales Import</h4><p>Sales invoices — line-wise for Vyapar entry</p></div></div>
            <div class="report-card" onclick="showReport('vyapar-payments')"><div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(37,99,235,0.12),rgba(99,102,241,0.08))"><div class="report-icon">💳</div></div><div class="report-text"><h4>Vyapar Payment In Import</h4><p>Payment receipts — line-wise for Vyapar entry</p></div></div>
            <div class="report-card" onclick="showReport('payment-trend')">
                <div class="report-icon-wrap" style="background:linear-gradient(135deg,rgba(124,45,18,0.12),rgba(249,115,22,0.08))">
                    <div class="report-icon">📊</div>
                </div>
                <div class="report-text">
                    <h4>Customer Payment Trend</h4>
                    <p>Customer × Date pivot — collection summary</p>
                </div>
            </div>
        </div>
`;
}
async function showReport(type) {
    // Each report opens as a full page — replace page content entirely
    pageContent.innerHTML = `
        <div class="section-toolbar" style="margin-bottom:16px;flex-wrap:wrap;gap:8px">
            <button class="btn btn-outline" onclick="renderReports()">← Back</button>
        </div>
        <div id="report-detail"></div>`;
    const el = $('report-detail'); if (!el) return;

    // Fetch common data needed by multiple reports
    const [inventory, invoices, payments, expenses, users, categories, parties] = await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('invoices'),
        DB.getAll('payments'),
        DB.getAll('expenses'),
        DB.getAll('users'),
        DB.getAll('categories'),
        DB.getAll('parties')
    ]);

    if (type === 'sales') {
        window._rSalesAll = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
        const monthStart = today().substring(0, 8) + '01';
        const salesUsers = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-s-from" value="${monthStart}" onchange="renderSalesRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-s-to" value="${today()}" onchange="renderSalesRpt()"></div>
                <div class="form-group"><label>Party</label><input id="r-s-party" placeholder="All parties..." oninput="renderSalesRpt()" style="width:160px"></div>
                <div class="form-group"><label>Salesman</label><select id="r-s-user" onchange="renderSalesRpt()"><option value="">All</option>${salesUsers.map(u=>`<option>${u.name}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-sales','SalesReport_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-s-out"></div>`;
        renderSalesRpt();
    }
    
    if (type === 'collection-allocations') {
        window._rAllocAll = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled' && i.allocatedTo);
        const collectors = [...new Set(window._rAllocAll.map(i => i.allocatedTo))].filter(Boolean).sort();
        
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>Filter Collector</label><select id="r-ca-user" onchange="renderCollectionAllocationsRpt()"><option value="">All</option>${collectors.map(c=>`<option>${c}</option>`).join('')}</select></div>
                <div class="form-group"><label>Status</label><select id="r-ca-status" onchange="renderCollectionAllocationsRpt()"><option value="">All Assigned</option><option value="pending">Pending Balance</option><option value="paid">Fully Paid</option></select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-allocations','CollectionAllocations_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-ca-out"></div>`;
        
        window.renderCollectionAllocationsRpt = async function() {
            const userFlt = $('r-ca-user').value;
            const statusFlt = $('r-ca-status').value;
            
            let html = '<div class="table-wrapper"><table class="data-table" id="tbl-allocations"><thead><tr><th>Collector</th><th>Invoice No</th><th>Date</th><th>Customer</th><th>Total Amt</th><th>Paid</th><th>Balance</th><th>Assigned On</th></tr></thead><tbody>';
            let grandTotal = 0, grandPaid = 0, grandBal = 0;
            
            for (const inv of window._rAllocAll) {
                if (userFlt && inv.allocatedTo !== userFlt) continue;
                
                const paid = await getInvoicePaidAmount(inv.invoiceNo);
                const bal = inv.total - paid;
                
                if (statusFlt === 'pending' && bal <= 0) continue;
                if (statusFlt === 'paid' && bal > 0) continue;
                
                grandTotal += inv.total;
                grandPaid += paid;
                grandBal += bal;
                
                let assignDate = '-';
                if (inv.allocationHistory && inv.allocationHistory.length) {
                    const last = inv.allocationHistory[inv.allocationHistory.length - 1];
                    assignDate = fmtDate(last.date.substring(0,10));
                }
                
                html += `<tr>
                    <td><span class="badge badge-info">${escapeHtml(inv.allocatedTo)}</span></td>
                    <td><a href="#" onclick="viewInvoice('${inv.id}')" style="color:var(--primary);text-decoration:underline;font-weight:600">${inv.invoiceNo}</a></td>
                    <td>${fmtDate(inv.date)}</td>
                    <td>${escapeHtml(inv.partyName)}</td>
                    <td>${currency(inv.total)}</td>
                    <td class="amount-green">${currency(paid)}</td>
                    <td style="color:${bal>0?'var(--danger)':'inherit'};font-weight:600">${currency(bal)}</td>
                    <td style="font-size:0.8rem;color:var(--text-muted)">${assignDate}</td>
                </tr>`;
            }
            
            html += `<tr style="font-weight:700;background:var(--bg-card)">
                <td colspan="4" style="text-align:right">Total</td>
                <td>${currency(grandTotal)}</td>
                <td class="amount-green">${currency(grandPaid)}</td>
                <td style="color:var(--danger)">${currency(grandBal)}</td>
                <td></td>
            </tr></tbody></table></div>`;
            $('r-ca-out').innerHTML = html;
        };
        
        await window.renderCollectionAllocationsRpt();
    }

    if (type === 'purchases') {
        window._rPurchAll = invoices.filter(i => i.type === 'purchase' && i.status !== 'cancelled');
        const monthStart = today().substring(0, 8) + '01';
        const poUsers = users.filter(u => ['Admin','Manager'].includes(u.role));
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-p-from" value="${monthStart}" onchange="renderPurchaseRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-p-to" value="${today()}" onchange="renderPurchaseRpt()"></div>
                <div class="form-group"><label>Supplier</label><input id="r-p-party" placeholder="All suppliers..." oninput="renderPurchaseRpt()" style="width:160px"></div>
                <div class="form-group"><label>Created By</label><select id="r-p-user" onchange="renderPurchaseRpt()"><option value="">All</option>${poUsers.map(u=>`<option>${u.name}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-purchases','PurchaseReport_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-p-out"></div>`;
        renderPurchaseRpt();
    }
    if (type === 'pnl') {
        window._rPnlInv = invoices.filter(i => i.status !== 'cancelled');
        window._rPnlExp = expenses;
        window._rPnlInvt = inventory;
        const monthStart = today().substring(0, 8) + '01';
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-pnl-from" value="${monthStart}" onchange="renderPnlRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-pnl-to" value="${today()}" onchange="renderPnlRpt()"></div>
            </div>
        </div></div>
        <div id="r-pnl-out"></div>`;
        renderPnlRpt();
    }
    if (type === 'invoice-pnl') {
        window._rInvPnlAll = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
        window._rInvPnlInvt = inventory;
        const monthStart = today().substring(0, 8) + '01';
        const salesUsers = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-ip-from" value="${monthStart}" onchange="renderInvPnlRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-ip-to" value="${today()}" onchange="renderInvPnlRpt()"></div>
                <div class="form-group"><label>Party</label><input id="r-ip-party" placeholder="All parties..." oninput="renderInvPnlRpt()" style="width:160px"></div>
                <div class="form-group"><label>Salesman</label><select id="r-ip-user" onchange="renderInvPnlRpt()"><option value="">All</option>${salesUsers.map(u=>`<option>${u.name}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-invpnl','InvoicePnL_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-ip-out"></div>`;
        renderInvPnlRpt();
    }
    if (type === 'stock') {
        window._rStockAll = inventory;
        const catList = [...new Set(inventory.map(i => i.category).filter(Boolean))].sort();
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>Category</label><select id="r-st-cat" onchange="renderStockRpt()"><option value="">All Categories</option>${catList.map(c=>`<option>${c}</option>`).join('')}</select></div>
                <div class="form-group"><label>Stock Status</label><select id="r-st-status" onchange="renderStockRpt()"><option value="">All</option><option value="low">Low / Out of Stock</option><option value="out">Out of Stock Only</option><option value="ok">In Stock</option></select></div>
                <div class="form-group"><label>Search</label><input id="r-st-search" placeholder="Item name..." oninput="renderStockRpt()" style="width:160px"></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-stock','StockSummary_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-st-out"></div>`;
        renderStockRpt();
    }
    if (type === 'outstanding') {
        window._rOutAll = parties;
        window._rOutInv = invoices.filter(i => i.status !== 'cancelled');
        window._rOutPay = payments;
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>Party Type</label><select id="r-out-type" onchange="renderOutstandingRpt()"><option value="">All</option><option>Customer</option><option>Supplier</option></select></div>
                <div class="form-group"><label>Search</label><input id="r-out-search" placeholder="Party name..." oninput="renderOutstandingRpt()" style="width:180px"></div>
                <div class="form-group"><label>Balance</label><select id="r-out-bal" onchange="renderOutstandingRpt()"><option value="">All</option><option value="dr">Receivable (Customer owes us)</option><option value="cr">Payable (We owe them)</option></select></div>
                <div class="form-group"><label>Age</label><select id="r-out-age" onchange="renderOutstandingRpt()"><option value="">All</option><option value="0-30">0–30 days</option><option value="31-60">31–60 days</option><option value="61-90">61–90 days</option><option value="90+">90+ days</option></select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-outstanding','Outstanding_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-out-out"></div>`;
        renderOutstandingRpt();
    }
    if (type === 'expenses') {
        window._rExpAll = expenses;
        const monthStart = today().substring(0, 8) + '01';
        const expCats = [...new Set(expenses.map(e => e.category || 'General'))].sort();
        const expUsers = [...new Set(expenses.map(e => e.createdBy).filter(Boolean))].sort();
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-exp-from" value="${monthStart}" onchange="renderExpenseRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-exp-to" value="${today()}" onchange="renderExpenseRpt()"></div>
                <div class="form-group"><label>Category</label><select id="r-exp-cat" onchange="renderExpenseRpt()"><option value="">All Categories</option>${expCats.map(c=>`<option>${c}</option>`).join('')}</select></div>
                <div class="form-group"><label>Added By</label><select id="r-exp-user" onchange="renderExpenseRpt()"><option value="">All</option>${expUsers.map(u=>`<option>${u}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-expenses','ExpenseSummary_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-exp-out"></div>`;
        renderExpenseRpt();
    }

    if (type === 'usersales') {
        renderUserSalesReportUI(el, users, categories);
    }
    if (type === 'userpayments') {
        renderUserPaymentReportUI(el, users, payments);
    }
    if (type === 'chequeregister') {
        window._rChqAll = payments.filter(p => p.mode === 'Cheque');
        const monthStart = today().substring(0, 8) + '01';
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-chq-from" value="${monthStart}" onchange="renderChequeRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-chq-to" value="${today()}" onchange="renderChequeRpt()"></div>
                <div class="form-group"><label>Party</label><input id="r-chq-party" placeholder="All parties..." oninput="renderChequeRpt()" style="width:160px"></div>
                <div class="form-group"><label>Status</label><select id="r-chq-status" onchange="renderChequeRpt()"><option value="">All</option><option>Pending</option><option>Deposited</option><option>Cleared</option></select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('cheque-reg-table','ChequeRegister_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-chq-out"></div>`;
        renderChequeRpt();
    }

    // ── VYAPAR SALES IMPORT REPORT ──
    if (type === 'vyapar-sales') {
        window._vySalesAll = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
        const monthStart = today().substring(0, 8) + '01';
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="vy-s-from" value="${monthStart}" onchange="renderVyaparSalesTable()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="vy-s-to" value="${today()}" onchange="renderVyaparSalesTable()"></div>
                <div class="form-group"><label>Party</label><input id="vy-s-party" placeholder="All parties..." oninput="renderVyaparSalesTable()" style="width:180px"></div>
                <div class="form-group"><label>Salesman</label><select id="vy-s-user" onchange="renderVyaparSalesTable()"><option value="">All</option>${users.filter(u=>['Admin','Manager','Salesman'].includes(u.role)).map(u=>`<option>${u.name}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-vy-sales','VyaparSales_${today()}')">📥 Export Excel</button></div>
            </div>
        </div></div>
        <div class="card"><div class="card-body" id="vy-sales-wrap">
            <table class="data-table" id="tbl-vy-sales" style="font-size:0.82rem">
                <thead><tr><th>Date</th><th>Vyapar Invoice No</th><th>Party</th><th>Item</th><th>Qty</th><th>Unit</th><th>Rate</th><th>Amount</th><th>GST%</th><th>Total</th></tr></thead>
                <tbody id="vy-sales-tbody"></tbody>
            </table>
        </div></div>`;
        renderVyaparSalesTable();
    }

    // ── VYAPAR PAYMENT IN IMPORT REPORT ──
    if (type === 'vyapar-payments') {
        window._vyPayAll = payments.filter(p => p.type === 'in');
        const monthStart = today().substring(0, 8) + '01';
        const collectors = [...new Set(window._vyPayAll.map(p => p.collectedBy || p.createdBy).filter(Boolean))].sort();
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="vy-p-from" value="${monthStart}" onchange="renderVyaparPayTable()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="vy-p-to" value="${today()}" onchange="renderVyaparPayTable()"></div>
                <div class="form-group"><label>Party</label><input id="vy-p-party" placeholder="All parties..." oninput="renderVyaparPayTable()" style="width:180px"></div>
                <div class="form-group"><label>Mode</label><select id="vy-p-mode" onchange="renderVyaparPayTable()"><option value="">All Modes</option><option>Cash</option><option>UPI</option><option>Cheque</option><option>Bank Transfer</option></select></div>
                <div class="form-group"><label>Collected By</label><select id="vy-p-collector" onchange="renderVyaparPayTable()"><option value="">All</option>${collectors.map(n=>`<option>${n}</option>`).join('')}</select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-vy-pay','VyaparPayments_${today()}')">📥 Export Excel</button></div>
            </div>
        </div></div>
        <div class="card"><div class="card-body">
            <table class="data-table" id="tbl-vy-pay" style="font-size:0.82rem">
                <thead><tr><th>Date</th><th>Receipt No</th><th>Party</th><th>Invoice No</th><th>Mode</th><th>Ref</th><th>Collected By</th><th style="text-align:right">Amount</th></tr></thead>
                <tbody id="vy-pay-tbody"></tbody>
            </table>
        </div></div>`;
        renderVyaparPayTable();
    }
    if (type === 'salesman') {
        window._rSlsInv = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
        window._rSlsPay = payments.filter(p => p.type === 'in');
        const salesUsers = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
        const monthStart = today().substring(0,8)+'01';
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-sl-from" value="${monthStart}" onchange="renderSalesmanRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-sl-to" value="${today()}" onchange="renderSalesmanRpt()"></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-salesman','SalesmanPerformance_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-sl-out"></div>`;
        renderSalesmanRpt();
    }

    if (type === 'daybook') {
        window._rDbInv = invoices.filter(i => i.status !== 'cancelled');
        window._rDbPay = payments;
        window._rDbExp = expenses;
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>From Date</label><input type="date" id="r-db-from" value="${today()}" onchange="renderDayBook()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-db-to" value="${today()}" onchange="renderDayBook()"></div>
                <div class="form-group"><label>Type</label><select id="r-db-type" onchange="renderDayBook()"><option value="">All</option><option>Sale Invoice</option><option>Purchase Invoice</option><option>Payment In</option><option>Payment Out</option><option>Expense</option></select></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-daybook','DayBook_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-db-out"></div>`;
        renderDayBook();
    }

    if (type === 'user-outstanding') {
        window._rUOutInv = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled');
        window._rUOutPay = payments;
        window._rUOutUsers = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0;flex-wrap:wrap;gap:10px">
                <div class="form-group"><label>Salesman</label><select id="r-uo-user" onchange="renderUserOutstandingRpt()"><option value="">All Salesmen</option>${users.filter(u=>['Admin','Manager','Salesman'].includes(u.role)).map(u=>`<option>${escapeHtml(u.name)}</option>`).join('')}</select></div>
                <div class="form-group"><label>Age</label><select id="r-uo-age" onchange="renderUserOutstandingRpt()"><option value="">All</option><option value="0-30">0–30 days</option><option value="31-60">31–60 days</option><option value="61-90">61–90 days</option><option value="90+">90+ days</option></select></div>
                <div class="form-group"><label>Search Party</label><input id="r-uo-party" placeholder="Party name..." oninput="renderUserOutstandingRpt()" style="width:160px"></div>
                <div class="form-group" style="align-self:flex-end"><button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-user-outstanding','OutstandingByUser_${today()}')">📥 Export</button></div>
            </div>
        </div></div>
        <div id="r-uo-out"></div>`;
        renderUserOutstandingRpt();
    }

    if (type === 'payment-trend') {
        const allPayments = await DB.getAll('payments');
        const users = await DB.getAll('users');
        const salesmen = users.filter(u => u.role === 'Salesman' || u.role === 'Manager' || u.role === 'Admin');

        pageContent.innerHTML = `
        <div class="section-toolbar" style="flex-wrap:wrap;gap:8px;margin-bottom:16px">
            <button class="btn btn-outline" onclick="renderReports()">← Back</button>
            <h3 style="flex:1;min-width:200px">📊 Customer Payment Trend</h3>
            <button class="btn btn-primary" onclick="exportTableToExcel('tbl-pay-trend','CustomerPaymentTrend')">📥 Export Excel</button>
        </div>
        <div class="card"><div class="card-body padded" style="padding-bottom:12px">
            <div class="form-row" style="margin-bottom:0">
                <div class="form-group"><label>From Date</label><input type="date" id="f-trend-from" value="${new Date(new Date().setDate(1)).toISOString().split('T')[0]}" onchange="renderPayTrend()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="f-trend-to" value="${today()}" onchange="renderPayTrend()"></div>
                <div class="form-group"><label>Customer</label><input id="f-trend-cust" placeholder="All customers..." oninput="renderPayTrend()"></div>
                <div class="form-group"><label>Collected By</label>
                    <select id="f-trend-user" onchange="renderPayTrend()">
                        <option value="">All</option>
                        ${salesmen.map(u => `<option value="${u.name}">${u.name}</option>`).join('')}
                    </select>
                </div>
            </div>
        </div></div>
        <div id="pay-trend-output" style="margin-top:16px;overflow-x:auto"></div>`;

        // Store payments globally for filter re-renders
        window['_trendPayments'] = allPayments.filter(p => p.type === 'in');
        renderPayTrend();
        return;
    }

    if (type === 'payment-report') {
        const monthStart = today().substring(0, 8) + '01';
        const collectors = users.filter(u => ['Admin','Manager','Salesman'].includes(u.role));
        window._rPayAll = payments;
        el.innerHTML = `
        <div class="card" style="margin-bottom:14px"><div class="card-body padded" style="padding-bottom:12px">
            <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end">
                <div class="form-group"><label>From Date</label><input type="date" id="r-pay-from" value="${monthStart}" onchange="renderPaymentRpt()"></div>
                <div class="form-group"><label>To Date</label><input type="date" id="r-pay-to" value="${today()}" onchange="renderPaymentRpt()"></div>
                <div class="form-group"><label>Type</label>
                    <select id="r-pay-type" onchange="renderPaymentRpt()">
                        <option value="">Pay In + Pay Out</option>
                        <option value="in">Pay In Only</option>
                        <option value="out">Pay Out Only</option>
                    </select>
                </div>
                <div class="form-group"><label>Mode</label>
                    <select id="r-pay-mode" onchange="renderPaymentRpt()">
                        <option value="">All Modes</option>
                        <option>Cash</option><option>UPI</option>
                        <option>Bank Transfer</option><option>Cheque</option>
                    </select>
                </div>
                <div class="form-group"><label>Party</label>
                    <input id="r-pay-party" placeholder="All parties..." oninput="renderPaymentRpt()" style="width:150px">
                </div>
                <div class="form-group"><label>Collected By</label>
                    <select id="r-pay-user" onchange="renderPaymentRpt()">
                        <option value="">All</option>
                        ${collectors.map(u=>`<option>${escapeHtml(u.name)}</option>`).join('')}
                    </select>
                </div>
                <div class="form-group" style="align-self:flex-end">
                    <button class="btn btn-primary btn-sm" onclick="exportTableToExcel('tbl-pay-rpt','PaymentReport_${today()}')">📥 Export</button>
                </div>
            </div>
        </div></div>
        <!-- Summary chips -->
        <div id="r-pay-summary" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px"></div>
        <div id="r-pay-out"></div>`;
        renderPaymentRpt();
        return;
    }
}

// ── REPORT RENDER HELPERS ──
function renderPaymentRpt() {
    const from   = ($('r-pay-from')||{}).value||'';
    const to     = ($('r-pay-to')||{}).value||'';
    const type   = ($('r-pay-type')||{}).value||'';
    const mode   = ($('r-pay-mode')||{}).value||'';
    const party  = (($('r-pay-party')||{}).value||'').toLowerCase();
    const user   = ($('r-pay-user')||{}).value||'';
    const out    = $('r-pay-out');     if (!out) return;
    const sumEl  = $('r-pay-summary');

    let rows = (window._rPayAll||[]).slice();
    if (from)  rows = rows.filter(p => p.date >= from);
    if (to)    rows = rows.filter(p => p.date <= to);
    if (type)  rows = rows.filter(p => p.type === type);
    if (mode)  rows = rows.filter(p => (p.mode||'') === mode);
    if (party) rows = rows.filter(p => (p.partyName||'').toLowerCase().includes(party));
    if (user)  rows = rows.filter(p => (p.collectedBy||p.createdBy||'') === user);
    rows.sort((a,b) => (b.date||'').localeCompare(a.date||''));

    const totalIn  = rows.filter(p=>p.type==='in').reduce((s,p)=>s+p.amount,0);
    const totalOut = rows.filter(p=>p.type==='out').reduce((s,p)=>s+p.amount,0);
    const net      = totalIn - totalOut;

    if (sumEl) sumEl.innerHTML = `
        <div class="dash-kpi-card dash-kpi-green" style="flex:1;min-width:130px;padding:10px 14px">
            <div class="dash-kpi-label">Pay In</div>
            <div class="dash-kpi-amount">${currency(totalIn)}</div>
            <div class="dash-kpi-badge dash-kpi-badge-green">${rows.filter(p=>p.type==='in').length} entries</div>
        </div>
        <div class="dash-kpi-card dash-kpi-red" style="flex:1;min-width:130px;padding:10px 14px">
            <div class="dash-kpi-label">Pay Out</div>
            <div class="dash-kpi-amount">${currency(totalOut)}</div>
            <div class="dash-kpi-badge dash-kpi-badge-red">${rows.filter(p=>p.type==='out').length} entries</div>
        </div>
        <div class="dash-kpi-card" style="flex:1;min-width:130px;padding:10px 14px;border-color:${net>=0?'rgba(16,185,129,0.3)':'rgba(239,68,68,0.3)'}">
            <div class="dash-kpi-label">Net</div>
            <div class="dash-kpi-amount" style="color:${net>=0?'#10b981':'#ef4444'}">${currency(Math.abs(net))}</div>
            <div class="dash-kpi-badge" style="background:${net>=0?'rgba(16,185,129,0.1)':'rgba(239,68,68,0.1)'};color:${net>=0?'#10b981':'#ef4444'}">${net>=0?'Surplus':'Deficit'}</div>
        </div>`;

    if (!rows.length) { out.innerHTML = '<div class="empty-state"><p>No payments found for selected filters</p></div>'; return; }

    out.innerHTML = `<div class="card"><div class="card-body">
        <div class="table-wrapper">
        <table class="data-table" id="tbl-pay-rpt" style="min-width:700px">
            <thead><tr>
                <th>Date</th><th>Receipt #</th><th>Party</th>
                <th>Type</th><th>Mode</th><th>Amount</th>
                <th>Collected By</th><th>Note</th>
            </tr></thead>
            <tbody>
            ${rows.map(p=>`<tr>
                <td>${fmtDate(p.date)}</td>
                <td style="font-weight:600;font-size:0.82rem">${escapeHtml(p.receiptNo||'-')}</td>
                <td>${escapeHtml(p.partyName||'-')}</td>
                <td><span class="badge ${p.type==='in'?'badge-success':'badge-danger'}">${p.type==='in'?'Pay In':'Pay Out'}</span></td>
                <td><span class="badge badge-info">${escapeHtml(p.mode||'-')}</span></td>
                <td class="${p.type==='in'?'amount-green':'amount-red'}" style="font-weight:700">${currency(p.amount)}</td>
                <td style="font-size:0.82rem">${escapeHtml(p.collectedBy||p.createdBy||'-')}</td>
                <td style="font-size:0.8rem;color:var(--text-muted)">${escapeHtml(p.notes||p.note||'-')}</td>
            </tr>`).join('')}
            </tbody>
        </table></div>
    </div></div>`;
}

function renderSalesRpt() {
    const from  = ($('r-s-from')||{}).value||'';
    const to    = ($('r-s-to')||{}).value||'';
    const party = (($('r-s-party')||{}).value||'').toLowerCase();
    const user  = ($('r-s-user')||{}).value||'';
    const out   = $('r-s-out'); if (!out) return;
    let inv = (window._rSalesAll||[]).slice();
    if (from)  inv = inv.filter(i => i.date >= from);
    if (to)    inv = inv.filter(i => i.date <= to);
    if (party) inv = inv.filter(i => (i.partyName||'').toLowerCase().includes(party));
    if (user)  inv = inv.filter(i => i.createdBy === user);
    const total = inv.reduce((s,i) => s+i.total, 0);
    const count = inv.length;
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card green"><div class="stat-icon">🧾</div><div class="stat-value">${count}</div><div class="stat-label">Invoices</div></div>
        <div class="stat-card blue"><div class="stat-icon">💹</div><div class="stat-value">${currency(total)}</div><div class="stat-label">Total Sales</div></div>
        <div class="stat-card amber"><div class="stat-icon">📊</div><div class="stat-value">${count ? currency(total/count) : '—'}</div><div class="stat-label">Avg Invoice</div></div>
    </div>
    <div class="card"><div class="card-body"><table class="data-table" id="tbl-sales">
        <thead><tr><th>Date</th><th>Invoice</th><th>Party</th><th>Salesman</th><th style="text-align:right">Amount</th></tr></thead>
        <tbody>${inv.map(i=>`<tr><td>${fmtDate(i.date)}</td><td style="font-weight:600">${i.invoiceNo}</td><td>${escapeHtml(i.partyName)}</td><td>${i.createdBy||'-'}</td><td class="amount-green" style="text-align:right">${currency(i.total)}</td></tr>`).join('')||'<tr><td colspan="5" class="empty-state"><p>No sales found</p></td></tr>'}
        <tr style="font-weight:700;background:rgba(0,212,170,0.1)"><td colspan="4" style="text-align:right">Total (${count} invoices)</td><td class="amount-green" style="text-align:right">${currency(total)}</td></tr>
        </tbody></table></div></div>`;
}

function renderPurchaseRpt() {
    const from  = ($('r-p-from')||{}).value||'';
    const to    = ($('r-p-to')||{}).value||'';
    const party = (($('r-p-party')||{}).value||'').toLowerCase();
    const user  = ($('r-p-user')||{}).value||'';
    const out   = $('r-p-out'); if (!out) return;
    let inv = (window._rPurchAll||[]).slice();
    if (from)  inv = inv.filter(i => i.date >= from);
    if (to)    inv = inv.filter(i => i.date <= to);
    if (party) inv = inv.filter(i => (i.partyName||'').toLowerCase().includes(party));
    if (user)  inv = inv.filter(i => i.createdBy === user);
    const total = inv.reduce((s,i) => s+i.total, 0);
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card blue"><div class="stat-icon">🛒</div><div class="stat-value">${inv.length}</div><div class="stat-label">Invoices</div></div>
        <div class="stat-card red"><div class="stat-icon">💸</div><div class="stat-value">${currency(total)}</div><div class="stat-label">Total Purchases</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-purchases">
            <thead><tr><th>Date</th><th>Invoice</th><th>Supplier</th><th>Created By</th><th style="text-align:right">Amount</th></tr></thead>
            <tbody>${inv.map(i=>`<tr><td>${fmtDate(i.date)}</td><td style="font-weight:600">${i.invoiceNo}</td><td>${escapeHtml(i.partyName)}</td><td>${i.createdBy||'-'}</td><td class="amount-red" style="text-align:right">${currency(i.total)}</td></tr>`).join('')||'<tr><td colspan="5" class="empty-state"><p>No purchases found</p></td></tr>'}
            <tr style="font-weight:700;background:rgba(0,180,216,0.1)"><td colspan="4" style="text-align:right">Total</td><td class="amount-red" style="text-align:right">${currency(total)}</td></tr>
            </tbody></table>
        </div>
    </div></div>`;
}

function renderPnlRpt() {
    const from = ($('r-pnl-from')||{}).value||'';
    const to   = ($('r-pnl-to')||{}).value||'';
    const out  = $('r-pnl-out'); if (!out) return;
    const invt = window._rPnlInvt||[];
    let saleInvs = (window._rPnlInv||[]).filter(i => i.type==='sale');
    let expList  = (window._rPnlExp||[]).slice();
    if (from) { saleInvs = saleInvs.filter(i => i.date >= from); expList = expList.filter(e => e.date >= from); }
    if (to)   { saleInvs = saleInvs.filter(i => i.date <= to);   expList = expList.filter(e => e.date <= to);   }
    const s = saleInvs.reduce((a,i) => a+i.total, 0);
    let estCost = 0;
    saleInvs.forEach(inv => { inv.items.forEach(li => { const item = invt.find(x => x.id === li.itemId); estCost += (li.packedQty !== undefined ? li.packedQty : li.qty) * (item ? (item.purchasePrice||0) : 0); }); });
    const e = expList.reduce((a,x) => a+x.amount, 0);
    const gross = s - estCost;
    const net = gross - e;
    const margin = s > 0 ? ((gross/s)*100).toFixed(1) : '0.0';
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card green"><div class="stat-icon">💹</div><div class="stat-value">${currency(s)}</div><div class="stat-label">Sales Revenue</div></div>
        <div class="stat-card red"><div class="stat-icon">📦</div><div class="stat-value">${currency(estCost)}</div><div class="stat-label">Cost of Goods</div></div>
        <div class="stat-card blue"><div class="stat-icon">📊</div><div class="stat-value">${currency(gross)}</div><div class="stat-label">Gross Profit (${margin}%)</div></div>
        <div class="stat-card red"><div class="stat-icon">💸</div><div class="stat-value">${currency(e)}</div><div class="stat-label">Expenses</div></div>
    </div>
    <div class="card"><div class="card-body padded">
        <div style="font-size:1.05rem;margin-bottom:10px;display:flex;justify-content:space-between"><span>Sales Revenue</span><span class="amount-green">${currency(s)}</span></div>
        <div style="font-size:1.05rem;margin-bottom:10px;display:flex;justify-content:space-between"><span>Cost of Goods Sold <span style="font-size:0.78rem;color:var(--text-muted)">(purchase price)</span></span><span class="amount-red">− ${currency(estCost)}</span></div>
        <div style="font-size:1.05rem;margin-bottom:10px;display:flex;justify-content:space-between;font-weight:600"><span>Gross Margin</span><span class="${gross>=0?'amount-green':'amount-red'}">${currency(gross)} (${margin}%)</span></div>
        <div style="font-size:1.05rem;margin-bottom:10px;display:flex;justify-content:space-between"><span>Expenses</span><span class="amount-red">− ${currency(e)}</span></div>
        <hr style="border-color:var(--border);margin:14px 0">
        <div style="font-size:1.25rem;font-weight:700;display:flex;justify-content:space-between"><span>Net Profit</span><span class="${net>=0?'amount-green':'amount-red'}">${currency(net)}</span></div>
    </div></div>`;
}

function renderInvPnlRpt() {
    const from  = ($('r-ip-from')||{}).value||'';
    const to    = ($('r-ip-to')||{}).value||'';
    const party = (($('r-ip-party')||{}).value||'').toLowerCase();
    const user  = ($('r-ip-user')||{}).value||'';
    const out   = $('r-ip-out'); if (!out) return;
    const invt  = window._rInvPnlInvt||[];
    let invs = (window._rInvPnlAll||[]).slice();
    if (from)  invs = invs.filter(i => i.date >= from);
    if (to)    invs = invs.filter(i => i.date <= to);
    if (party) invs = invs.filter(i => (i.partyName||'').toLowerCase().includes(party));
    if (user)  invs = invs.filter(i => i.createdBy === user);
    let totalRev = 0, totalCost = 0;
    const rows = invs.map(inv => {
        const revenue = inv.total;
        let cost = 0;
        inv.items.forEach(li => { const item = invt.find(x => x.id === li.itemId); cost += (li.packedQty !== undefined ? li.packedQty : li.qty) * (item ? (item.purchasePrice||0) : 0); });
        const profit = revenue - cost;
        const margin = revenue > 0 ? ((profit/revenue)*100).toFixed(1) : '0.0';
        totalRev += revenue; totalCost += cost;
        return `<tr><td>${fmtDate(inv.date)}</td><td style="font-weight:600">${inv.invoiceNo}</td><td>${escapeHtml(inv.partyName)}</td><td>${inv.createdBy||'-'}</td><td class="amount-green" style="text-align:right">${currency(revenue)}</td><td class="amount-red" style="text-align:right">${currency(cost)}</td><td class="${profit>=0?'amount-green':'amount-red'}" style="text-align:right;font-weight:700">${currency(profit)}</td><td style="font-weight:600;text-align:right;color:${+margin>=0?'var(--success)':'var(--danger)'}">${margin}%</td></tr>`;
    }).join('');
    const totalProfit = totalRev - totalCost;
    const totalMargin = totalRev > 0 ? ((totalProfit/totalRev)*100).toFixed(1) : '0.0';
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card green"><div class="stat-icon">💹</div><div class="stat-value">${currency(totalRev)}</div><div class="stat-label">Revenue</div></div>
        <div class="stat-card red"><div class="stat-icon">📦</div><div class="stat-value">${currency(totalCost)}</div><div class="stat-label">Cost</div></div>
        <div class="stat-card blue"><div class="stat-icon">📊</div><div class="stat-value">${currency(totalProfit)}</div><div class="stat-label">Profit (${totalMargin}%)</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-invpnl">
            <thead><tr><th>Date</th><th>Invoice</th><th>Party</th><th>Salesman</th><th style="text-align:right">Revenue</th><th style="text-align:right">Cost</th><th style="text-align:right">Profit</th><th style="text-align:right">Margin</th></tr></thead>
            <tbody>${rows||'<tr><td colspan="8" class="empty-state"><p>No invoices found</p></td></tr>'}
            <tr style="font-weight:700;background:rgba(0,212,170,0.1)"><td colspan="4" style="text-align:right">Total</td><td class="amount-green" style="text-align:right">${currency(totalRev)}</td><td class="amount-red" style="text-align:right">${currency(totalCost)}</td><td class="${totalProfit>=0?'amount-green':'amount-red'}" style="text-align:right">${currency(totalProfit)}</td><td style="text-align:right;font-weight:600">${totalMargin}%</td></tr>
            </tbody></table>
        </div>
    </div></div>`;
}

function renderStockRpt() {
    const cat    = ($('r-st-cat')||{}).value||'';
    const status = ($('r-st-status')||{}).value||'';
    const search = (($('r-st-search')||{}).value||'').toLowerCase();
    const out    = $('r-st-out'); if (!out) return;
    let items = (window._rStockAll||[]).slice();
    if (cat)    items = items.filter(i => i.category === cat);
    if (search) items = items.filter(i => (i.name||'').toLowerCase().includes(search));
    if (status === 'low')  items = items.filter(i => i.stock <= (i.lowStockAlert||5));
    if (status === 'out')  items = items.filter(i => i.stock <= 0);
    if (status === 'ok')   items = items.filter(i => i.stock > (i.lowStockAlert||5));
    const totalVal = items.reduce((s,i) => s + i.stock*(i.purchasePrice||0), 0);
    const lowCount = items.filter(i => i.stock <= (i.lowStockAlert||5) && i.stock > 0).length;
    const outCount = items.filter(i => i.stock <= 0).length;
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card blue"><div class="stat-icon">📦</div><div class="stat-value">${items.length}</div><div class="stat-label">Items</div></div>
        <div class="stat-card green"><div class="stat-icon">💰</div><div class="stat-value">${currency(totalVal)}</div><div class="stat-label">Stock Value</div></div>
        <div class="stat-card amber"><div class="stat-icon">⚠️</div><div class="stat-value">${lowCount}</div><div class="stat-label">Low Stock</div></div>
        <div class="stat-card red"><div class="stat-icon">🚫</div><div class="stat-value">${outCount}</div><div class="stat-label">Out of Stock</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-stock">
            <thead><tr><th>Item</th><th>Category</th><th>Unit</th><th style="text-align:right">Stock</th><th style="text-align:right">Purchase Price</th><th style="text-align:right">Stock Value</th></tr></thead>
            <tbody>${items.map(i=>{const low=i.stock<=(i.lowStockAlert||5);return`<tr><td style="font-weight:600">${escapeHtml(i.name)}</td><td>${i.category||'-'}</td><td>${i.unit||'Pcs'}</td><td style="text-align:right"><span class="badge ${i.stock<=0?'badge-danger':low?'badge-warning':'badge-success'}">${i.stock}</span></td><td style="text-align:right">${currency(i.purchasePrice||0)}</td><td style="text-align:right">${currency(i.stock*(i.purchasePrice||0))}</td></tr>`;}).join('')||'<tr><td colspan="6" class="empty-state"><p>No items found</p></td></tr>'}
            <tr style="font-weight:700"><td colspan="5" style="text-align:right">Total Value</td><td style="text-align:right">${currency(totalVal)}</td></tr>
            </tbody></table>
        </div>
    </div></div>`;
}

function renderOutstandingRpt() {
    const ptype  = ($('r-out-type')||{}).value||'';
    const search = (($('r-out-search')||{}).value||'').toLowerCase();
    const bal    = ($('r-out-bal')||{}).value||'';
    const age    = ($('r-out-age')||{}).value||'';
    const out    = $('r-out-out'); if (!out) return;
    const invAll = window._rOutInv || [];
    const payAll = window._rOutPay || [];
    const todayMs = new Date().setHours(0,0,0,0);

    // Build paid-per-invoice map
    const paidMap = {};
    payAll.forEach(p => {
        if (p.allocations) { Object.entries(p.allocations).forEach(([inv,amt]) => { paidMap[inv] = (paidMap[inv]||0) + (+amt||0); }); }
        else if (p.invoiceNo && p.invoiceNo !== 'Advance' && p.invoiceNo !== 'Multi') { paidMap[p.invoiceNo] = (paidMap[p.invoiceNo]||0) + (p.amount||0); }
    });

    let pts = (window._rOutAll||[]).filter(p => p.balance);
    if (ptype)  pts = pts.filter(p => p.type === ptype);
    if (search) pts = pts.filter(p => (p.name||'').toLowerCase().includes(search));
    if (bal === 'dr') pts = pts.filter(p => p.balance < 0);
    if (bal === 'cr') pts = pts.filter(p => p.balance > 0);

    const rows = [];
    pts.forEach(p => {
        const partyInvs = invAll.filter(i => String(i.partyId) === String(p.id) && i.type === 'sale');
        const pending = partyInvs.filter(i => (i.total - (paidMap[i.invoiceNo]||0)) > 0.01).sort((a,b)=>(a.dueDate||a.date).localeCompare(b.dueDate||b.date));
        const lastPay = payAll.filter(py => String(py.partyId) === String(p.id)).sort((a,b)=>b.date.localeCompare(a.date))[0];
        const oldest = pending[0];
        let daysDue = null, ageBucket = '';
        if (oldest) {
            const ageRef = oldest.dueDate || oldest.date;
            daysDue = Math.floor((todayMs - new Date(ageRef).setHours(0,0,0,0)) / 86400000);
            ageBucket = daysDue <= 30 ? '0-30' : daysDue <= 60 ? '31-60' : daysDue <= 90 ? '61-90' : '90+';
        }
        if (age && ageBucket !== age) return;
        const ageColor = daysDue === null ? 'var(--text-muted)' : daysDue <= 30 ? '#22c55e' : daysDue <= 60 ? '#f59e0b' : '#ef4444';
        const dirColor = p.balance < 0 ? 'var(--success)' : 'var(--danger)';
        rows.push(`<tr style="cursor:pointer;font-weight:500" onclick="currentLedgerPartyId='${p.id}';navigateTo('partyledger')">
            <td><span style="font-weight:700">${escapeHtml(p.name)}</span><br><span style="font-size:0.75rem;color:var(--text-muted)">${p.phone||''}</span></td>
            <td><span class="badge ${p.type==='Customer'?'badge-success':'badge-info'}">${p.type}</span></td>
            <td style="font-size:0.82rem">${pending.length}</td>
            <td style="font-size:0.82rem;color:var(--text-muted)">${lastPay ? fmtDate(lastPay.date) : '<span style="color:var(--danger)">Never</span>'}</td>
            <td style="text-align:right"><span style="font-weight:700;color:${ageColor}">${daysDue !== null ? daysDue+'d' : '-'}</span></td>
            <td style="text-align:right;font-weight:700;font-size:1rem;color:${dirColor}">${currency(Math.abs(p.balance))}</td>
            <td><span class="badge ${p.balance<0?'badge-success':'badge-danger'}">${p.balance<0?'Receivable':'Payable'}</span></td>
        </tr>`);
        pending.forEach(i => {
            const paid = paidMap[i.invoiceNo]||0;
            const due = i.total - paid;
            const ageRef = i.dueDate || i.date;
            const d = Math.floor((todayMs - new Date(ageRef).setHours(0,0,0,0)) / 86400000);
            const ac = d <= 0 ? '#22c55e' : d <= 30 ? '#f59e0b' : '#ef4444';
            const dueLabel = i.dueDate ? `Due ${fmtDate(i.dueDate)}` : fmtDate(i.date);
            const overLabel = d <= 0 ? `${Math.abs(d)}d left` : `${d}d overdue`;
            rows.push(`<tr style="background:var(--bg-body)">
                <td style="padding:3px 8px;font-size:0.78rem;color:var(--text-muted);padding-left:24px">↳ <a href="#" onclick="viewInvoiceByNo('${i.invoiceNo}');return false" style="color:var(--accent)">${i.invoiceNo}</a></td>
                <td style="padding:3px 8px;font-size:0.78rem;color:var(--text-muted)">${fmtDate(i.date)}</td>
                <td style="padding:3px 8px;font-size:0.78rem;color:var(--text-muted)">Total: ₹${i.total.toFixed(2)}</td>
                <td style="padding:3px 8px;font-size:0.78rem;color:var(--text-muted)">${dueLabel}</td>
                <td style="padding:3px 8px;text-align:right"><span style="color:${ac};font-weight:600;font-size:0.8rem">${overLabel}</span></td>
                <td style="padding:3px 8px;text-align:right;font-weight:700;color:#ef4444;font-size:0.85rem">₹${due.toFixed(2)}</td>
                <td></td>
            </tr>`);
        });
    });

    const filtered = pts.filter(p => {
        if (!age) return true;
        const invs = invAll.filter(i => String(i.partyId)===String(p.id)&&i.type==='sale');
        const pend = invs.filter(i=>(i.total-(paidMap[i.invoiceNo]||0))>0.01).sort((a,b)=>a.date.localeCompare(b.date));
        if (!pend[0]) return false;
        const d = Math.floor((todayMs - new Date(pend[0].date).setHours(0,0,0,0))/86400000);
        return (d<=30?'0-30':d<=60?'31-60':d<=90?'61-90':'90+') === age;
    });
    const totalRec = filtered.filter(p=>p.balance<0).reduce((s,p)=>s+Math.abs(p.balance),0);
    const totalPay = filtered.filter(p=>p.balance>0).reduce((s,p)=>s+p.balance,0);

    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card green"><div class="stat-icon">💰</div><div class="stat-value">${currency(totalRec)}</div><div class="stat-label">Receivable</div></div>
        <div class="stat-card red"><div class="stat-icon">💸</div><div class="stat-value">${currency(totalPay)}</div><div class="stat-label">Payable</div></div>
        <div class="stat-card blue"><div class="stat-icon">👥</div><div class="stat-value">${rows.filter(r=>!r.includes('padding-left:24px')).length}</div><div class="stat-label">Parties</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-outstanding">
            <thead><tr><th>Party</th><th>Type</th><th>Pending Inv.</th><th>Last Payment</th><th style="text-align:right">Oldest Age</th><th style="text-align:right">Balance</th><th>Status</th></tr></thead>
            <tbody>${rows.join('')||'<tr><td colspan="7" class="empty-state"><p>No outstanding balance found</p></td></tr>'}
            </tbody></table>
        </div>
    </div></div>`;
}

function renderUserOutstandingRpt() {
    const filterUser  = ($('r-uo-user')||{}).value||'';
    const filterAge   = ($('r-uo-age')||{}).value||'';
    const filterParty = (($('r-uo-party')||{}).value||'').toLowerCase();
    const out = $('r-uo-out'); if (!out) return;

    const invAll = window._rUOutInv || [];
    const payAll = window._rUOutPay || [];
    const todayMs = new Date().setHours(0,0,0,0);

    // Build paid-per-invoice map from allocations + direct links
    const paidMap = {};
    payAll.forEach(p => {
        if (p.allocations) {
            Object.entries(p.allocations).forEach(([inv, amt]) => { paidMap[inv] = (paidMap[inv]||0) + (+amt||0); });
        } else if (p.invoiceNo && p.invoiceNo !== 'Advance' && p.invoiceNo !== 'Multi' && p.invoiceNo !== '') {
            paidMap[p.invoiceNo] = (paidMap[p.invoiceNo]||0) + (p.amount||0);
        }
    });

    // Find all pending invoices
    const pendingInvs = invAll.filter(i => {
        const due = (i.total||0) - (paidMap[i.invoiceNo]||0);
        return due > 0.01;
    });

    // Group by salesman (createdBy)
    const byUser = {};
    pendingInvs.forEach(i => {
        const user = i.createdBy || '(Unassigned)';
        if (!byUser[user]) byUser[user] = [];
        byUser[user].push(i);
    });

    // Filter by salesman
    let userKeys = Object.keys(byUser).sort();
    if (filterUser) userKeys = userKeys.filter(u => u === filterUser);

    const rows = [];
    let grandTotal = 0, grandInvCount = 0;

    userKeys.forEach(userName => {
        let invs = byUser[userName];

        // Filter by party name
        if (filterParty) invs = invs.filter(i => (i.partyName||'').toLowerCase().includes(filterParty));
        if (!invs.length) return;

        // Filter by age (use dueDate if present, else invoice date)
        if (filterAge) {
            invs = invs.filter(i => {
                const ageRef = i.dueDate || i.date;
                const d = Math.floor((todayMs - new Date(ageRef).setHours(0,0,0,0)) / 86400000);
                const b = d <= 30 ? '0-30' : d <= 60 ? '31-60' : d <= 90 ? '61-90' : '90+';
                return b === filterAge;
            });
        }
        if (!invs.length) return;

        // Sort oldest due date first
        invs.sort((a,b) => (a.dueDate||a.date).localeCompare(b.dueDate||b.date));

        const userTotal = invs.reduce((s,i) => s + ((i.total||0) - (paidMap[i.invoiceNo]||0)), 0);
        const oldest = invs[0];
        const oldestAgeRef = oldest.dueDate || oldest.date;
        const oldestDays = Math.floor((todayMs - new Date(oldestAgeRef).setHours(0,0,0,0)) / 86400000);
        const ageColor = oldestDays <= 30 ? '#22c55e' : oldestDays <= 60 ? '#f59e0b' : '#ef4444';

        grandTotal += userTotal;
        grandInvCount += invs.length;

        // Salesman summary row
        rows.push(`<tr style="background:var(--bg-card);font-weight:600">
            <td colspan="2" style="padding:10px 12px;font-size:0.95rem">
                <span style="font-size:1rem">👤</span> ${escapeHtml(userName)}
            </td>
            <td style="padding:10px 12px;text-align:center">
                <span class="badge badge-info">${invs.length} invoices</span>
            </td>
            <td style="padding:10px 12px;text-align:right;color:${ageColor}">Oldest: ${oldestDays}d</td>
            <td></td>
            <td style="padding:10px 12px;text-align:right;font-size:1.05rem;color:#ef4444">${currency(userTotal)}</td>
        </tr>`);

        // Invoice detail rows
        invs.forEach(i => {
            const paid = paidMap[i.invoiceNo]||0;
            const due = (i.total||0) - paid;
            const ageRef = i.dueDate || i.date;
            const d = Math.floor((todayMs - new Date(ageRef).setHours(0,0,0,0)) / 86400000);
            const ac = d <= 0 ? '#22c55e' : d <= 30 ? '#f59e0b' : '#ef4444';
            const overLabel = d <= 0 ? `${Math.abs(d)}d left` : `${d}d over`;
            rows.push(`<tr style="background:var(--bg-body)">
                <td style="padding:4px 12px 4px 28px;font-size:0.8rem;color:var(--accent);white-space:nowrap">
                    ↳ <a href="#" onclick="viewInvoiceByNo('${escapeHtml(i.invoiceNo)}');return false" style="color:var(--accent);font-weight:600">${escapeHtml(i.invoiceNo)}</a>
                </td>
                <td style="padding:4px 8px;font-size:0.8rem;color:var(--text-muted)">${escapeHtml(i.partyName||'-')}</td>
                <td style="padding:4px 8px;font-size:0.8rem;color:var(--text-muted);text-align:center">${fmtDate(i.date)}${i.dueDate ? `<br><span style="font-size:0.72rem;color:var(--accent)">Due: ${fmtDate(i.dueDate)}</span>` : ''}</td>
                <td style="padding:4px 8px;font-size:0.8rem;color:var(--text-muted);text-align:right"><span style="color:${ac};font-weight:600">${overLabel}</span></td>
                <td style="padding:4px 8px;font-size:0.8rem;text-align:right;color:var(--text-muted)">Paid: ₹${paid.toFixed(2)}</td>
                <td style="padding:4px 8px;font-size:0.85rem;font-weight:700;color:#ef4444;text-align:right">₹${due.toFixed(2)}</td>
            </tr>`);
        });
    });

    if (!rows.length) {
        out.innerHTML = `<div class="empty-state"><p>No outstanding invoices found</p></div>`;
        return;
    }

    out.innerHTML = `
    <div class="stats-grid-sm" style="margin-bottom:14px">
        <div class="stat-card red"><div class="stat-icon">💰</div><div class="stat-value">${currency(grandTotal)}</div><div class="stat-label">Total Outstanding</div></div>
        <div class="stat-card blue"><div class="stat-icon">🧾</div><div class="stat-value">${grandInvCount}</div><div class="stat-label">Pending Invoices</div></div>
        <div class="stat-card"><div class="stat-icon">👤</div><div class="stat-value">${userKeys.filter(u => {
            let invs = byUser[u]||[];
            if (filterParty) invs = invs.filter(i=>(i.partyName||'').toLowerCase().includes(filterParty));
            if (filterAge) invs = invs.filter(i=>{ const d=Math.floor((todayMs-new Date(i.date).setHours(0,0,0,0))/86400000); return (d<=30?'0-30':d<=60?'31-60':d<=90?'61-90':'90+')===filterAge; });
            return invs.length > 0;
        }).length}</div><div class="stat-label">Salesmen</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-user-outstanding">
                <thead><tr>
                    <th>Invoice #</th><th>Party</th><th style="text-align:center">Date</th>
                    <th style="text-align:right">Age</th><th style="text-align:right">Paid</th>
                    <th style="text-align:right">Due</th>
                </tr></thead>
                <tbody>${rows.join('')}</tbody>
            </table>
        </div>
    </div></div>`;
}

function renderExpenseRpt() {
    const from = ($('r-exp-from')||{}).value||'';
    const to   = ($('r-exp-to')||{}).value||'';
    const cat  = ($('r-exp-cat')||{}).value||'';
    const user = ($('r-exp-user')||{}).value||'';
    const out  = $('r-exp-out'); if (!out) return;
    let exps = (window._rExpAll||[]).slice();
    if (from) exps = exps.filter(e => e.date >= from);
    if (to)   exps = exps.filter(e => e.date <= to);
    if (cat)  exps = exps.filter(e => (e.category||'General') === cat);
    if (user) exps = exps.filter(e => e.createdBy === user);
    const total = exps.reduce((s,e) => s+e.amount, 0);
    const catMap = {};
    exps.forEach(e => { const c = e.category||'General'; catMap[c] = (catMap[c]||0) + e.amount; });
    const catRows = Object.entries(catMap).sort((a,b)=>b[1]-a[1]).map(([c,a])=>`<tr><td style="font-weight:600">${c}</td><td class="amount-red" style="text-align:right">${currency(a)}</td><td style="text-align:right;color:var(--text-muted)">${total>0?((a/total)*100).toFixed(1):0}%</td></tr>`).join('');
    const detailRows = exps.map(e=>`<tr><td>${fmtDate(e.date)}</td><td>${e.category||'General'}</td><td>${escapeHtml(e.note||'-')}</td><td>${e.createdBy||'-'}</td><td class="amount-red" style="text-align:right">${currency(e.amount)}</td></tr>`).join('');
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card red"><div class="stat-icon">💸</div><div class="stat-value">${currency(total)}</div><div class="stat-label">Total Expenses</div></div>
        <div class="stat-card blue"><div class="stat-icon">📋</div><div class="stat-value">${exps.length}</div><div class="stat-label">Entries</div></div>
        <div class="stat-card amber"><div class="stat-icon">🗂️</div><div class="stat-value">${Object.keys(catMap).length}</div><div class="stat-label">Categories</div></div>
    </div>
    <div class="card" style="margin-bottom:14px"><div class="card-header"><h4 style="font-size:0.9rem">Category Breakup</h4></div><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-expenses">
            <thead><tr><th>Category</th><th style="text-align:right">Amount</th><th style="text-align:right">%</th></tr></thead>
            <tbody>${catRows||'<tr><td colspan="3" class="empty-state"><p>No expenses</p></td></tr>'}
            <tr style="font-weight:700"><td style="text-align:right">Total</td><td class="amount-red" style="text-align:right">${currency(total)}</td><td></td></tr>
            </tbody></table>
        </div>
    </div></div>
    <div class="card"><div class="card-header"><h4 style="font-size:0.9rem">Expense Details</h4></div><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table">
            <thead><tr><th>Date</th><th>Category</th><th>Note</th><th>Added By</th><th style="text-align:right">Amount</th></tr></thead>
            <tbody>${detailRows||'<tr><td colspan="5" class="empty-state"><p>No expenses</p></td></tr>'}</tbody></table>
        </div>
    </div></div>`;
}

function renderChequeRpt() {
    const from   = ($('r-chq-from')||{}).value||'';
    const to     = ($('r-chq-to')||{}).value||'';
    const party  = (($('r-chq-party')||{}).value||'').toLowerCase();
    const status = ($('r-chq-status')||{}).value||'';
    const out    = $('r-chq-out'); if (!out) return;
    let cheques = (window._rChqAll||[]).slice();
    if (from)   cheques = cheques.filter(c => c.date >= from);
    if (to)     cheques = cheques.filter(c => c.date <= to);
    if (party)  cheques = cheques.filter(c => (c.partyName||'').toLowerCase().includes(party));
    if (status) cheques = cheques.filter(c => (c.chequeStatus||'Pending') === status);
    const pending   = cheques.filter(c => !c.chequeStatus || c.chequeStatus === 'Pending').length;
    const deposited = cheques.filter(c => c.chequeStatus === 'Deposited').length;
    const cleared   = cheques.filter(c => c.chequeStatus === 'Cleared').length;
    const totalAmt  = cheques.reduce((s,c) => s+c.amount, 0);
    const rows = cheques.map(c => {
        const statusBadge = c.chequeStatus==='Cleared'?'badge-success':c.chequeStatus==='Deposited'?'badge-warning':'badge-danger';
        let actionBtns = '';
        if (!c.chequeStatus || c.chequeStatus==='Pending') actionBtns = `<button class="btn btn-outline btn-sm" onclick="updateChequeStatus('${c.id}','Deposited')">Mark Deposited</button>`;
        else if (c.chequeStatus==='Deposited') actionBtns = `<button class="btn btn-primary btn-sm" onclick="updateChequeStatus('${c.id}','Cleared')">Mark Cleared</button>`;
        else actionBtns = '<span style="color:var(--success);font-weight:600">✔ Done</span>';
        return `<tr><td style="font-size:0.8rem;color:var(--text-muted)">${c.id.substring(0,8)}</td><td>${fmtDate(c.date)}</td><td style="font-weight:600">${c.chequeNo||'-'}</td><td>${escapeHtml(c.partyName)}</td><td>${c.chequeBank||'-'}</td><td>${c.chequeDepositDate?fmtDate(c.chequeDepositDate):'-'}</td><td class="${c.type==='in'?'amount-green':'amount-red'}" style="text-align:right">${currency(c.amount)}</td><td><span class="badge ${statusBadge}">${c.chequeStatus||'Pending'}</span></td><td>${actionBtns}</td></tr>`;
    }).join('');
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card amber"><div class="stat-icon">⏳</div><div class="stat-value">${pending}</div><div class="stat-label">Pending</div></div>
        <div class="stat-card blue"><div class="stat-icon">🏦</div><div class="stat-value">${deposited}</div><div class="stat-label">Deposited</div></div>
        <div class="stat-card green"><div class="stat-icon">✅</div><div class="stat-value">${cleared}</div><div class="stat-label">Cleared</div></div>
        <div class="stat-card blue"><div class="stat-icon">💰</div><div class="stat-value">${currency(totalAmt)}</div><div class="stat-label">Total Amount</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="cheque-reg-table">
            <thead><tr><th>Voucher #</th><th>Pay Date</th><th>Cheque #</th><th>Party</th><th>Bank</th><th>Deposit Date</th><th style="text-align:right">Amount</th><th>Status</th><th>Action</th></tr></thead>
            <tbody>${rows||'<tr><td colspan="9"><div class="empty-state"><p>No cheques found</p></div></td></tr>'}</tbody>
            </table>
        </div>
    </div></div>`;
}

async function updateChequeStatus(payId, newStatus) {
    try {
        await DB.update('payments', payId, { chequeStatus: newStatus, chequeDepositDate: newStatus === 'Deposited' ? today() : undefined });
        showToast(`Cheque status updated to ${newStatus}`, 'success');
        await showReport('chequeregister');
    } catch (err) {
        alert('Error updating cheque status: ' + err.message);
    }
}

function renderSalesmanRpt() {
    const from = ($('r-sl-from')||{}).value||'';
    const to   = ($('r-sl-to')||{}).value||'';
    const out  = $('r-sl-out'); if (!out) return;
    let invs = (window._rSlsInv||[]).slice();
    let pays = (window._rSlsPay||[]).slice();
    if (from) { invs = invs.filter(i => i.date >= from); pays = pays.filter(p => p.date >= from); }
    if (to)   { invs = invs.filter(i => i.date <= to);   pays = pays.filter(p => p.date <= to);   }
    // Group by salesman
    const slsMap = {};
    invs.forEach(i => {
        const name = i.createdBy || 'Unassigned';
        if (!slsMap[name]) slsMap[name] = { invoices: 0, sales: 0, collections: 0, payCount: 0 };
        slsMap[name].invoices++;
        slsMap[name].sales += i.total;
    });
    pays.forEach(p => {
        const name = p.collectedBy || p.createdBy || 'Unassigned';
        if (!slsMap[name]) slsMap[name] = { invoices: 0, sales: 0, collections: 0, payCount: 0 };
        slsMap[name].collections += p.amount;
        slsMap[name].payCount++;
    });
    const rows = Object.entries(slsMap).sort((a,b)=>b[1].sales-a[1].sales).map(([name, d]) => {
        const eff = d.sales > 0 ? ((d.collections/d.sales)*100).toFixed(1) : '0.0';
        return `<tr>
            <td style="font-weight:700">${escapeHtml(name)}</td>
            <td style="text-align:right">${d.invoices}</td>
            <td class="amount-green" style="text-align:right">${currency(d.sales)}</td>
            <td style="text-align:right">${d.payCount}</td>
            <td class="amount-green" style="text-align:right">${currency(d.collections)}</td>
            <td style="text-align:right;font-weight:600;color:${+eff>=80?'var(--success)':+eff>=50?'#f59e0b':'#ef4444'}">${eff}%</td>
        </tr>`;
    });
    const totSales = Object.values(slsMap).reduce((s,d)=>s+d.sales,0);
    const totColl  = Object.values(slsMap).reduce((s,d)=>s+d.collections,0);
    out.innerHTML = `
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-salesman">
                <thead><tr><th>Salesman</th><th style="text-align:right">Invoices</th><th style="text-align:right">Sales ₹</th><th style="text-align:right">Receipts</th><th style="text-align:right">Collections ₹</th><th style="text-align:right">Collection %</th></tr></thead>
                <tbody>${rows.join('')||'<tr><td colspan="6" class="empty-state"><p>No data found</p></td></tr>'}
                <tr style="font-weight:700;background:rgba(0,212,170,0.1)"><td>Total</td><td></td><td class="amount-green" style="text-align:right">${currency(totSales)}</td><td></td><td class="amount-green" style="text-align:right">${currency(totColl)}</td><td></td></tr>
                </tbody>
            </table>
        </div>
    </div></div>`;
}

function renderDayBook() {
    const from  = ($('r-db-from')||{}).value||today();
    const to    = ($('r-db-to')||{}).value||today();
    const ftype = ($('r-db-type')||{}).value||'';
    const out   = $('r-db-out'); if (!out) return;

    // Build unified transaction list
    const txns = [];
    (window._rDbInv||[]).filter(i => i.date >= from && i.date <= to).forEach(i => {
        const label = i.type === 'sale' ? 'Sale Invoice' : 'Purchase Invoice';
        if (ftype && ftype !== label) return;
        txns.push({ date: i.date, type: label, ref: i.invoiceNo, party: i.partyName, dr: i.type === 'sale' ? i.total : 0, cr: i.type === 'purchase' ? i.total : 0, by: i.createdBy||'' });
    });
    (window._rDbPay||[]).filter(p => p.date >= from && p.date <= to).forEach(p => {
        const label = p.type === 'in' ? 'Payment In' : 'Payment Out';
        if (ftype && ftype !== label) return;
        txns.push({ date: p.date, type: label, ref: p.payNo||p.id.substring(0,8), party: p.partyName, dr: p.type === 'in' ? p.amount : 0, cr: p.type === 'out' ? p.amount : 0, by: p.collectedBy||p.createdBy||'' });
    });
    (window._rDbExp||[]).filter(e => e.date >= from && e.date <= to).forEach(e => {
        if (ftype && ftype !== 'Expense') return;
        txns.push({ date: e.date, type: 'Expense', ref: e.category||'General', party: e.note||'-', dr: 0, cr: e.amount, by: e.createdBy||'' });
    });
    txns.sort((a,b)=>a.date.localeCompare(b.date));

    const totalDr = txns.reduce((s,t)=>s+t.dr,0);
    const totalCr = txns.reduce((s,t)=>s+t.cr,0);
    const typeColor = { 'Sale Invoice': 'var(--success)', 'Purchase Invoice': 'var(--info)', 'Payment In': '#22c55e', 'Payment Out': '#ef4444', 'Expense': '#f59e0b' };
    const rows = txns.map(t => `<tr>
        <td>${fmtDate(t.date)}</td>
        <td><span style="font-size:0.8rem;font-weight:600;color:${typeColor[t.type]||'var(--text-primary)'}">${t.type}</span></td>
        <td style="font-weight:600">${escapeHtml(t.ref)}</td>
        <td>${escapeHtml(t.party)}</td>
        <td class="amount-green" style="text-align:right">${t.dr > 0 ? currency(t.dr) : '-'}</td>
        <td class="amount-red" style="text-align:right">${t.cr > 0 ? currency(t.cr) : '-'}</td>
        <td style="font-size:0.8rem;color:var(--text-muted)">${t.by}</td>
    </tr>`).join('');
    out.innerHTML = `
    <div class="stats-grid-sm">
        <div class="stat-card green"><div class="stat-icon">📥</div><div class="stat-value">${currency(totalDr)}</div><div class="stat-label">Total Inflow (Sales+Receipts)</div></div>
        <div class="stat-card red"><div class="stat-icon">📤</div><div class="stat-value">${currency(totalCr)}</div><div class="stat-label">Total Outflow (Purchases+Exp)</div></div>
        <div class="stat-card blue"><div class="stat-icon">📋</div><div class="stat-value">${txns.length}</div><div class="stat-label">Transactions</div></div>
    </div>
    <div class="card"><div class="card-body">
        <div class="table-wrapper">
            <table class="data-table" id="tbl-daybook">
                <thead><tr><th>Date</th><th>Type</th><th>Ref #</th><th>Party / Note</th><th style="text-align:right">Inflow ₹</th><th style="text-align:right">Outflow ₹</th><th>By</th></tr></thead>
                <tbody>${rows||'<tr><td colspan="7" class="empty-state"><p>No transactions found</p></td></tr>'}
                <tr style="font-weight:700;background:rgba(0,212,170,0.08)"><td colspan="4" style="text-align:right">Totals</td><td class="amount-green" style="text-align:right">${currency(totalDr)}</td><td class="amount-red" style="text-align:right">${currency(totalCr)}</td><td></td></tr>
                </tbody>
            </table>
        </div>
    </div></div>`;
}

function renderUserSalesReportUI(el, users, categories) {
    // Only admins/managers can filter by all users, explicitly.
    // If it's a salesman, they should only see themselves initially.
    const isSalesAdmin = canEdit();
    const userOptions = isSalesAdmin ? `<option value="">All Users</option>` + users.map(u => `<option value="${u.name}">${u.name}</option>`).join('') : `<option value="${currentUser.name}">${currentUser.name}</option>`;

    el.innerHTML = `
        <div class="card" style="margin-bottom:15px">
            <div class="card-body" style="padding:15px">
                <div class="filter-group" style="display:flex;gap:15px;align-items:flex-end;flex-wrap:wrap">
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">From Date</label><input type="date" id="rep-us-from" class="search-box" style="width:140px" value="${today()}"></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">To Date</label><input type="date" id="rep-us-to" class="search-box" style="width:140px" value="${today()}"></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">User</label><select id="rep-us-user" class="search-box" style="width:160px">${userOptions}</select></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">Category</label><select id="rep-us-cat" class="search-box" style="width:160px"><option value="">All Categories</option>${categories.map(c => `<option value="${c.name}">${c.name}</option>`).join('')}</select></div>
                    <div><button class="btn btn-primary" onclick="generateUserSalesReport()">Generate</button></div>
                </div>
            </div>
        </div>
        <div id="rep-us-output"></div>
    `;
    generateUserSalesReport(); // Initial load
}

async function generateUserSalesReport() {
    const from = $('rep-us-from').value;
    const to = $('rep-us-to').value;
    const user = $('rep-us-user').value;
    const catSearch = $('rep-us-cat').value;

    const [orders, invoices, payments, inventory] = await Promise.all([
        DB.getAll('salesorders'),
        DB.getAll('invoices'),
        DB.getAll('payments'),
        DB.getAll('inventory')
    ]);

    const filteredOrders = orders.filter(o => o.status !== 'cancelled' && (from ? o.date >= from : true) && (to ? o.date <= to : true) && (user ? o.createdBy === user : true));
    const filteredInvoices = invoices.filter(i => i.type === 'sale' && i.status !== 'cancelled' && (from ? i.date >= from : true) && (to ? i.date <= to : true) && (user ? i.createdBy === user : true));
    const filteredPayments = payments.filter(p => p.type === 'in' && (from ? p.date >= from : true) && (to ? p.date <= to : true) && (user ? p.createdBy === user : true));

    // 1. Totals
    const totalOrderValue = filteredOrders.reduce((sum, o) => sum + o.total, 0);
    const totalInvoiceValue = filteredInvoices.reduce((sum, i) => sum + i.total, 0);
    const totalPayments = filteredPayments.reduce((sum, p) => sum + p.amount, 0);
    const outstanding = totalInvoiceValue - totalPayments;

    // 2. Category & Item calculations
    const catTotals = {};
    const itemMap = {};

    filteredOrders.forEach(o => {
        o.items.forEach(li => {
            const item = inventory.find(x => x.id === li.itemId);
            const cName = item ? item.category : 'Uncategorized';
            if (catSearch && cName !== catSearch) return;

            catTotals[cName] = (catTotals[cName] || 0) + (li.qty * li.price);
            if (!itemMap[li.itemId]) itemMap[li.itemId] = { name: (item ? item.name : li.itemId), ordQty: 0, ordVal: 0, invQty: 0, invVal: 0 };
            itemMap[li.itemId].ordQty += li.qty;
            itemMap[li.itemId].ordVal += (li.qty * li.price);
        });
    });

    filteredInvoices.forEach(inv => {
        inv.items.forEach(li => {
            const item = inventory.find(x => x.id === li.itemId);
            const cName = item ? item.category : 'Uncategorized';
            if (catSearch && cName !== catSearch) return;

            const qty = li.packedQty !== undefined ? li.packedQty : li.qty;
            if (!itemMap[li.itemId]) itemMap[li.itemId] = { name: (item ? item.name : li.itemId), ordQty: 0, ordVal: 0, invQty: 0, invVal: 0 };
            itemMap[li.itemId].invQty += qty;
            itemMap[li.itemId].invVal += (qty * li.price);
        });
    });

    const itemsArr = Object.values(itemMap).filter(x => x.ordVal > 0 || x.invVal > 0);

    $('rep-us-output').innerHTML = `
        <div class="stats-grid" style="margin-bottom:15px">
            <div class="stat-card blue"><div class="stat-icon">🛒</div><div class="stat-value">${currency(totalOrderValue)}</div><div class="stat-label">Total Ordered</div></div>
            <div class="stat-card green"><div class="stat-icon">🧾</div><div class="stat-value">${currency(totalInvoiceValue)}</div><div class="stat-label">Total Invoiced</div></div>
            <div class="stat-card amber"><div class="stat-icon">💰</div><div class="stat-value">${currency(totalPayments)}</div><div class="stat-label">Total Collected</div></div>
            <div class="stat-card red"><div class="stat-icon">⏳</div><div class="stat-value">${currency(outstanding)}</div><div class="stat-label">Outstanding (Inv - Pay)</div></div>
        </div>
        <div class="card">
            <div class="card-header"><h3 style="font-size:0.95rem">Item-wise Details ${catSearch ? `(Category: ${catSearch})` : ''}</h3></div>
            <div class="card-body">
                <div class="table-wrapper">
                    <table class="data-table" style="font-size:0.85rem">
                        <thead style="background:var(--bg-input)">
                            <tr><th>Item</th><th style="text-align:right">Ord Qty</th><th style="text-align:right">Ord Value</th><th style="text-align:right;border-left:2px solid var(--border)">Inv Qty</th><th style="text-align:right">Inv Value</th></tr>
                        </thead>
                        <tbody>${itemsArr.map(i => `
                            <tr>
                                <td style="font-weight:600">${i.name}</td>
                                <td style="text-align:right">${i.ordQty}</td>
                                <td class="amount-blue" style="text-align:right">${currency(i.ordVal)}</td>
                                <td style="text-align:right;border-left:2px solid var(--border)">${i.invQty}</td>
                                <td class="amount-green" style="text-align:right">${currency(i.invVal)}</td>
                            </tr>
                        `).join('') || '<tr><td colspan="5" class="empty-state">No item data found for this range</td></tr>'}</tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
}


function renderUserPaymentReportUI(el, users, payments) {
    const isSalesAdmin = canEdit();
    const userOptions = isSalesAdmin ? `<option value="">All Users</option>` + users.map(u => `<option value="${u.name}">${u.name}</option>`).join('') : `<option value="${currentUser.name}">${currentUser.name}</option>`;

    // Get unique modes used across all payments
    const modes = [...new Set(payments.map(p => p.mode || 'Cash'))];

    el.innerHTML = `
        <div class="card" style="margin-bottom:15px">
            <div class="card-body" style="padding:15px">
                <div class="filter-group" style="display:flex;gap:15px;align-items:flex-end;flex-wrap:wrap">
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">From Date</label><input type="date" id="rep-up-from" class="search-box" style="width:140px" value="${today()}"></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">To Date</label><input type="date" id="rep-up-to" class="search-box" style="width:140px" value="${today()}"></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">User</label><select id="rep-up-user" class="search-box" style="width:160px">${userOptions}</select></div>
                    <div><label style="font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:4px">Mode</label><select id="rep-up-mode" class="search-box" style="width:160px"><option value="">All Modes</option>${modes.map(m => `<option value="${m}">${m}</option>`).join('')}</select></div>
                    <div><button class="btn btn-primary" onclick="generateUserPaymentReport()">Generate</button></div>
                </div>
            </div>
        </div>
        <div id="rep-up-output"></div>
    `;
    generateUserPaymentReport(); // Initial load
}

async function generateUserPaymentReport() {
    const from = $('rep-up-from').value;
    const to = $('rep-up-to').value;
    const user = $('rep-up-user').value;
    const mode = $('rep-up-mode').value;

    let payments = (await DB.getAll('payments')).filter(p => p.type === 'in' && (from ? p.date >= from : true) && (to ? p.date <= to : true) && (user ? p.createdBy === user : true));

    if (mode) {
        payments = payments.filter(p => (p.mode || 'Cash') === mode);
    }

    const totalCollected = payments.reduce((sum, p) => sum + p.amount, 0);
    const modewise = {};
    payments.forEach(p => {
        const m = p.mode || 'Cash';
        modewise[m] = (modewise[m] || 0) + p.amount;
    });

    $('rep-up-output').innerHTML = `
        <div class="pay-summary-bar">
            <div class="pay-summary-total">
                <span class="pay-sum-label">Total Collection</span>
                <span class="pay-sum-value">${currency(totalCollected)}</span>
            </div>
            <div class="pay-summary-modes">
                ${Object.entries(modewise).map(([m, a]) => `
                <div class="pay-mode-chip">
                    <span>${m}</span>
                    <strong>${currency(a)}</strong>
                </div>`).join('')}
            </div>
        </div>

        <div class="card">
            <div class="card-header" style="display:flex;justify-content:space-between;align-items:center">
                <h3 style="font-size:0.95rem">Receipt Breakup</h3>
                <button class="btn btn-outline btn-sm" onclick="exportTableToExcel('tbl-pay-report','PaymentReport_${new Date().toISOString().split('T')[0]}')">📥 Export Excel</button>
            </div>
            <div class="card-body">
                <table class="data-table" id="tbl-pay-report" style="font-size:0.85rem">
                    <thead>
                        <tr><th>Date</th><th>Mode</th><th>Reference / Invoice</th><th>Party</th><th>Collected By</th><th style="text-align:right">Amount</th></tr>
                    </thead>
                    <tbody>${payments.map(p => `
                        <tr>
                            <td>${fmtDate(p.date)}</td>
                            <td><span class="badge badge-info">${p.mode || 'Cash'}</span></td>
                            <td>${p.invoiceNo ? `<a href="#" onclick="viewInvoiceByNo('${p.invoiceNo}');return false" style="color:var(--primary);text-decoration:underline">${p.invoiceNo}</a>` : p.note || '-'}</td>
                            <td style="font-weight:600">${escapeHtml(p.partyName)}</td>
                            <td>${escapeHtml(p.createdBy || 'System')}</td>
                            <td class="amount-green" style="text-align:right;font-weight:700">${currency(p.amount)}</td>
                        </tr>
                    `).join('') || '<tr><td colspan="6"><div class="empty-state"><span class="empty-icon">💰</span><p>No payments collected in this range</p></div></td></tr>'}
                    ${payments.length ? `<tr style="font-weight:800;background:rgba(249,115,22,0.04);border-top:2px solid var(--border)"><td colspan="5" style="text-align:right;font-size:0.85rem;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">Grand Total</td><td class="amount-green" style="text-align:right;font-size:1rem">${currency(totalCollected)}</td></tr>` : ''}
                    </tbody>
                </table>
            </div>
        </div>
    `;
}

// =============================================
//  PACKERS MASTER TABLE
// =============================================
function renderPackers() {
    const packers = DB.cache['packers'] || [];
    pageContent.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">Manage Packers</h3>
            <button class="btn btn-primary" onclick="openPackerModal()">+ Add Packer</button>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Name</th><th>Phone</th><th>Actions</th></tr></thead>
                <tbody>${packers.length ? packers.map(p => `<tr>
                    <td style="color:var(--text-primary);font-weight:600">${p.name}</td><td>${p.phone || '-'}</td>
                    <td><div class="action-btns"><button class="btn-icon" onclick="openPackerModal('${p.id}')">✏️</button><button class="btn-icon" onclick="deletePacker('${p.id}')">🗑️</button></div></td>
                </tr>`).join('') : '<tr><td colspan="3"><div class="empty-state"><div class="empty-icon">🧑‍🏭</div><p>No packers added yet</p></div></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}
function openPackerModal(id) {
    const p = id ? (DB.cache['packers'] || []).find(x => x.id === id) : null;
    openModal(p ? 'Edit Packer' : 'Add Packer', `
        <div class="form-group"><label>Name *</label><input id="f-packer-name" value="${p ? p.name : ''}"></div>
        <div class="form-group"><label>Phone</label><input id="f-packer-phone" value="${p ? p.phone || '' : ''}"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;savePacker('')">＋ Save & New</button>` : ''}
        <button class="btn btn-primary" onclick="savePacker('${id || ''}')">Save Packer</button></div>`);
}
async function savePacker(id) {
    const name = $('f-packer-name').value.trim(); if (!name) return alert('Name required');
    try {
        if (id) { await DB.update('packers', id, { name, phone: $('f-packer-phone').value.trim() }); }
        else { await DB.insert('packers', { name, phone: $('f-packer-phone').value.trim() }); }
        closeModal(); renderPackers();
        if (window._saveAndNew) { window._saveAndNew = false; openPackerModal(); }
    } catch (e) { window._saveAndNew = false; alert('Error saving packer: ' + e.message); }
}
async function deletePacker(id) {
    if (!confirm('Delete packer?')) return;
    try { await DB.delete('packers', id); renderPackers(); }
    catch (e) { alert('Error deleting packer: ' + e.message); }
}

// =============================================
//  DELIVERY PERSONS MASTER TABLE
// =============================================
function renderDeliveryPersons() {
    const persons = DB.cache['delivery_persons'] || [];
    pageContent.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">Manage Delivery Persons</h3>
            <button class="btn btn-primary" onclick="openDelPersonModal()">+ Add Delivery Person</button>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Name</th><th>Phone</th><th>Vehicle</th><th>Actions</th></tr></thead>
                <tbody>${persons.length ? persons.map(p => `<tr>
                    <td style="color:var(--text-primary);font-weight:600">${p.name}</td><td>${p.phone || '-'}</td><td>${p.vehicle || '-'}</td>
                    <td><div class="action-btns"><button class="btn-icon" onclick="openDelPersonModal('${p.id}')">✏️</button><button class="btn-icon" onclick="deleteDelPerson('${p.id}')">🗑️</button></div></td>
                </tr>`).join('') : '<tr><td colspan="4"><div class="empty-state"><div class="empty-icon">🧑‍✈️</div><p>No delivery persons added yet</p></div></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}
function openDelPersonModal(id) {
    const p = id ? (DB.cache['delivery_persons'] || []).find(x => x.id === id) : null;
    openModal(p ? 'Edit Delivery Person' : 'Add Delivery Person', `
        <div class="form-group"><label>Name *</label><input id="f-dp-name" value="${p ? p.name : ''}"></div>
        <div class="form-row"><div class="form-group"><label>Phone</label><input id="f-dp-phone" value="${p ? p.phone || '' : ''}"></div>
        <div class="form-group"><label>Vehicle</label><input id="f-dp-vehicle" value="${p ? p.vehicle || '' : ''}" placeholder="e.g. Tempo, Bike"></div></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveDelPerson('')">＋ Save & New</button>` : ''}
        <button class="btn btn-primary" onclick="saveDelPerson('${id || ''}')">Save</button></div>`);
}
async function saveDelPerson(id) {
    const name = $('f-dp-name').value.trim(); if (!name) return alert('Name required');
    const data = { name, phone: $('f-dp-phone').value.trim(), vehicle: $('f-dp-vehicle').value.trim() };
    try {
        if (id) { await DB.update('delivery_persons', id, data); }
        else { await DB.insert('delivery_persons', data); }
        closeModal(); renderDeliveryPersons();
        if (window._saveAndNew) { window._saveAndNew = false; openDelPersonModal(); }
    } catch (e) { window._saveAndNew = false; alert('Error saving delivery person: ' + e.message); }
}
async function deleteDelPerson(id) {
    if (!confirm('Delete?')) return;
    try { await DB.delete('delivery_persons', id); renderDeliveryPersons(); }
    catch (e) { alert('Error deleting: ' + e.message); }
}

// =============================================
//  USERS & ROLES
// =============================================
function renderUsers() {
    const users = DB.cache['users'] || [];
    const roleBadgeClass = { Admin:'badge-danger', Manager:'badge-info', Salesman:'badge-success', Delivery:'badge-info', Packing:'badge-warning' };
    pageContent.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">Users & Access</h3>
            <button class="btn btn-primary" onclick="openUserModal()">+ Add User</button>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table">
                    <thead><tr><th>Name</th><th>User ID</th><th>Roles</th><th>PIN</th><th>Actions</th></tr></thead>
                    <tbody>${users.map(u => {
                        const roles = Array.isArray(u.roles) && u.roles.length ? u.roles : [u.role];
                        return `<tr>
                            <td style="font-weight:600">${escapeHtml(u.name)}</td>
                            <td style="font-family:monospace;color:var(--accent);font-weight:600">${escapeHtml(u.userId || u.name)}</td>
                            <td>${roles.map(r => `<span class="badge ${roleBadgeClass[r]||'badge-info'}" style="margin-right:4px">${r}</span>`).join('')}</td>
                            <td style="color:var(--text-muted);letter-spacing:3px">${'•'.repeat((u.pin||'').length)}</td>
                            <td><div class="action-btns">
                                <button class="btn-icon" onclick="openUserModal('${u.id}')">✏️</button>
                                ${users.length > 1 ? `<button class="btn-icon" onclick="deleteUser('${u.id}')">🗑️</button>` : ''}
                            </div></td>
                        </tr>`;
                    }).join('')}</tbody>
                </table>
            </div>
        </div></div>
        <div class="card" style="margin-top:12px"><div class="card-header"><h3 style="margin:0;font-size:0.9rem">Role Permissions Reference</h3></div>
        <div class="card-body"><div class="table-wrapper">
        <table class="data-table" style="font-size:0.82rem">
            <thead><tr><th>Role</th><th>Access</th></tr></thead>
            <tbody>
                ${Object.entries(ROLE_PAGES).map(([role, pages]) => `<tr>
                    <td><span class="badge ${roleBadgeClass[role]||'badge-info'}">${role}</span></td>
                    <td style="color:var(--text-muted)">${pages.map(p => p.charAt(0).toUpperCase()+p.slice(1)).join(', ')}</td>
                </tr>`).join('')}
            </tbody>
        </table></div></div></div>`;
}
function openUserModal(id) {
    const u = id ? (DB.cache['users'] || []).find(x => x.id === id) : null;
    const userRoles = u ? (Array.isArray(u.roles) && u.roles.length ? u.roles : [u.role]) : [];
    const allRoles = ['Admin','Manager','Salesman','Delivery','Packing'];
    const roleBadgeClass = { Admin:'badge-danger', Manager:'badge-info', Salesman:'badge-success', Delivery:'badge-info', Packing:'badge-warning' };
    openModal(u ? 'Edit User' : 'Add User', `
        <div class="form-group">
            <label>Full Name *</label>
            <input id="f-user-name" class="form-control" value="${u ? escapeHtml(u.name) : ''}" placeholder="Employee name">
        </div>
        <div class="form-group">
            <label>User ID * <span style="font-size:0.78rem;color:var(--text-muted)">(for login — no spaces, e.g. ram01)</span></label>
            <input id="f-user-userid" class="form-control" value="${u ? escapeHtml(u.userId || '') : ''}" placeholder="e.g. ram01" style="text-transform:lowercase;font-family:monospace" oninput="this.value=this.value.toLowerCase().replace(/\\s/g,'')">
        </div>
        <div class="form-group">
            <label>Roles * <span style="font-size:0.78rem;color:var(--text-muted)">(select one or more)</span></label>
            <div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:8px">
                ${allRoles.map(r => `
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:6px 12px;border:2px solid var(--border);border-radius:20px;font-size:0.85rem;transition:all 0.15s;${userRoles.includes(r)?'border-color:var(--primary);background:var(--primary-light,#eff6ff);font-weight:600':''}">
                    <input type="checkbox" class="chk-user-role" value="${r}" ${userRoles.includes(r)?'checked':''} onchange="this.parentElement.style.borderColor=this.checked?'var(--primary)':'var(--border)';this.parentElement.style.background=this.checked?'var(--primary-light,#eff6ff)':'';this.parentElement.style.fontWeight=this.checked?'600':'400'">
                    <span class="badge ${roleBadgeClass[r]}">${r}</span>
                </label>`).join('')}
            </div>
        </div>
        <div class="form-group">
            <label>PIN * <span style="font-size:0.78rem;color:var(--text-muted)">(4 to 6 digits)</span></label>
            <div style="position:relative">
                <input type="password" id="f-user-pin" class="form-control" maxlength="6" value="${u ? u.pin : ''}" placeholder="Enter 4–6 digit PIN" inputmode="numeric" style="padding-right:40px">
                <button type="button" onclick="const p=$('f-user-pin');p.type=p.type==='password'?'text':'password'" style="position:absolute;right:10px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1rem">👁</button>
            </div>
        </div>
        <div class="form-group" id="extra-perms-section">
            <label>Extra Permissions <span style="font-size:0.78rem;color:var(--text-muted)">(beyond role defaults)</span></label>
            <div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:8px">
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:6px 12px;border:2px solid var(--border);border-radius:20px;font-size:0.85rem;${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('partyledger'))?'border-color:var(--primary);background:#eff6ff;font-weight:600':''}">
                    <input type="checkbox" id="perm-partyledger" value="partyledger" ${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('partyledger'))?'checked':''} onchange="this.parentElement.style.borderColor=this.checked?'var(--primary)':'var(--border)';this.parentElement.style.background=this.checked?'#eff6ff':'';this.parentElement.style.fontWeight=this.checked?'600':'400'">
                    📒 View Party Ledger
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:6px 12px;border:2px solid var(--border);border-radius:20px;font-size:0.85rem;${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('reports'))?'border-color:var(--primary);background:#eff6ff;font-weight:600':''}">
                    <input type="checkbox" id="perm-reports" value="reports" ${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('reports'))?'checked':''} onchange="this.parentElement.style.borderColor=this.checked?'var(--primary)':'var(--border)';this.parentElement.style.background=this.checked?'#eff6ff':'';this.parentElement.style.fontWeight=this.checked?'600':'400'">
                    📊 View Reports
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:6px 12px;border:2px solid var(--border);border-radius:20px;font-size:0.85rem;${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('invoices'))?'border-color:var(--primary);background:#eff6ff;font-weight:600':''}">
                    <input type="checkbox" id="perm-invoices" value="invoices" ${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('invoices'))?'checked':''} onchange="this.parentElement.style.borderColor=this.checked?'var(--primary)':'var(--border)';this.parentElement.style.background=this.checked?'#eff6ff':'';this.parentElement.style.fontWeight=this.checked?'600':'400'">
                    🧾 View Invoices
                </label>
                <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:6px 12px;border:2px solid var(--border);border-radius:20px;font-size:0.85rem;${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('expenses'))?'border-color:var(--primary);background:#eff6ff;font-weight:600':''}">
                    <input type="checkbox" id="perm-expenses" value="expenses" ${(u && Array.isArray(u.extra_perms) && u.extra_perms.includes('expenses'))?'checked':''} onchange="this.parentElement.style.borderColor=this.checked?'var(--primary)':'var(--border)';this.parentElement.style.background=this.checked?'#eff6ff':'';this.parentElement.style.fontWeight=this.checked?'600':'400'">
                    💸 View Expenses
                </label>
            </div>
        </div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveUser('')">＋ Save & New</button>` : ''}
            <button class="btn btn-primary" onclick="saveUser('${id || ''}')">Save User</button>
        </div>`);
}
async function saveUser(id) {
    const name = $('f-user-name').value.trim();
    const userId = $('f-user-userid').value.trim().toLowerCase().replace(/\s/g,'');
    const pin = $('f-user-pin').value.trim();
    const selectedRoles = [...document.querySelectorAll('.chk-user-role:checked')].map(c => c.value);
    const extraPerms = ['partyledger','reports','invoices','expenses'].filter(p => {
        const el = document.getElementById('perm-' + p);
        return el && el.checked;
    });

    if (!name) return alert('Name is required');
    if (!userId) return alert('User ID is required');
    if (!selectedRoles.length) return alert('Select at least one role');
    if (!pin || !/^\d{4,6}$/.test(pin)) return alert('PIN must be 4 to 6 digits (numbers only)');

    // Check userId uniqueness
    const allUsers = DB.cache['users'] || [];
    const conflict = allUsers.find(u => u.userId && u.userId.toLowerCase() === userId && u.id !== id);
    if (conflict) return alert(`User ID "${userId}" is already taken by ${conflict.name}`);

    const primaryRole = selectedRoles[0];
    const data = { name, userId, role: primaryRole, roles: selectedRoles, pin, extra_perms: extraPerms };

    try {
        if (id) {
            await DB.update('users', id, data);
        } else {
            await DB.insert('users', data);
            // Auto-create Packer or Delivery Person record
            if (selectedRoles.includes('Packing')) {
                const packers = await DB.getAll('packers');
                if (!packers.some(p => p.name === name)) await DB.insert('packers', { name });
            }
            if (selectedRoles.includes('Delivery')) {
                const dps = await DB.getAll('delivery_persons');
                if (!dps.some(p => p.name === name)) await DB.insert('delivery_persons', { name });
            }
        }
        closeModal(); renderUsers();
        showToast('User saved!', 'success');
        if (window._saveAndNew) { window._saveAndNew = false; openUserModal(); }
    } catch (e) { window._saveAndNew = false; alert('Error saving user: ' + e.message); }
}
async function deleteUser(id) {
    if (!confirm('Delete user?')) return;
    if (currentUser && currentUser.id === id) return alert('Cannot delete yourself');
    try { await DB.delete('users', id); renderUsers(); }
    catch (e) { alert('Error deleting user: ' + e.message); }
}

// =============================================
//  COMPANY SETUP
// =============================================
function renderVyaparSalesTable() {
    const from = ($('vy-s-from') || {}).value || '';
    const to   = ($('vy-s-to')   || {}).value || '';
    const party = (($('vy-s-party') || {}).value || '').toLowerCase();
    const user  = ($('vy-s-user')  || {}).value || '';
    const tbody = $('vy-sales-tbody'); if (!tbody) return;
    let invs = (window._vySalesAll || []).slice();
    if (from) invs = invs.filter(i => i.date >= from);
    if (to)   invs = invs.filter(i => i.date <= to);
    if (party) invs = invs.filter(i => (i.partyName || '').toLowerCase().includes(party));
    if (user)  invs = invs.filter(i => i.createdBy === user);
    const rows = [];
    invs.forEach(inv => {
        inv.items.forEach((li, idx) => {
            rows.push(`<tr>
                <td style="white-space:nowrap">${fmtDate(inv.date)}</td>
                <td style="font-weight:700;white-space:nowrap">${escapeHtml(inv.vyaparInvoiceNo || inv.invoiceNo)}</td>
                <td>${escapeHtml(inv.partyName)}</td>
                <td>${escapeHtml(li.name)}</td>
                <td style="text-align:right">${li.packedQty !== undefined ? li.packedQty : li.qty}</td>
                <td>${escapeHtml(li.unit || 'Pcs')}</td>
                <td style="text-align:right">${currency(li.price)}</td>
                <td style="text-align:right;font-weight:600" class="amount-green">${currency(li.amount)}</td>
                <td style="text-align:right">${inv.gst || 0}%</td>
                ${idx === 0 ? `<td rowspan="${inv.items.length}" style="text-align:right;font-weight:800;vertical-align:middle" class="amount-green">${currency(inv.total)}</td>` : ''}
            </tr>`);
        });
    });
    tbody.innerHTML = rows.join('') || '<tr><td colspan="10"><div class="empty-state"><span class="empty-icon">🧾</span><p>No invoices for selected filters</p></div></td></tr>';
}

function renderVyaparPayTable() {
    const from      = ($('vy-p-from')       || {}).value || '';
    const to        = ($('vy-p-to')         || {}).value || '';
    const party     = (($('vy-p-party')     || {}).value || '').toLowerCase();
    const mode      = ($('vy-p-mode')       || {}).value || '';
    const collector = ($('vy-p-collector')  || {}).value || '';
    const tbody = $('vy-pay-tbody'); if (!tbody) return;
    let pays = (window._vyPayAll || []).slice();
    if (from)      pays = pays.filter(p => p.date >= from);
    if (to)        pays = pays.filter(p => p.date <= to);
    if (party)     pays = pays.filter(p => (p.partyName || '').toLowerCase().includes(party));
    if (mode)      pays = pays.filter(p => (p.mode || 'Cash') === mode);
    if (collector) pays = pays.filter(p => (p.collectedBy || p.createdBy) === collector);
    const total = pays.reduce((s, p) => s + p.amount, 0);
    tbody.innerHTML = pays.map((p, i) => `<tr>
        <td style="white-space:nowrap">${fmtDate(p.date)}</td>
        <td style="font-weight:700">RCP-${String(i+1).padStart(4,'0')}</td>
        <td>${escapeHtml(p.partyName)}</td>
        <td>${p.invoiceNo ? `<span style="color:var(--primary);font-weight:600">${escapeHtml(p.invoiceNo)}</span>` : '-'}</td>
        <td><span class="badge badge-info">${p.mode || 'Cash'}</span></td>
        <td style="color:var(--text-muted);font-size:0.8rem">${escapeHtml(p.chequeNo || p.upiRef || p.note || '-')}</td>
        <td>${escapeHtml(p.collectedBy || p.createdBy || 'System')}</td>
        <td class="amount-green" style="text-align:right;font-weight:700">${currency(p.amount)}</td>
    </tr>`).join('') +
    (pays.length ? `<tr style="font-weight:800;border-top:2px solid var(--border)"><td colspan="7" style="text-align:right;text-transform:uppercase;font-size:0.82rem;color:var(--text-muted)">Total (${pays.length} records)</td><td class="amount-green" style="text-align:right;font-size:1rem">${currency(total)}</td></tr>` : '<tr><td colspan="8"><div class="empty-state"><span class="empty-icon">💳</span><p>No payments for selected filters</p></div></td></tr>');
}

function renderPayTrend() {
    const from = ($('f-trend-from') || {}).value || '';
    const to = ($('f-trend-to') || {}).value || '';
    const custFilter = (($('f-trend-cust') || {}).value || '').toLowerCase();
    const userFilter = ($('f-trend-user') || {}).value || '';

    let pays = (window['_trendPayments'] || []);
    if (from) pays = pays.filter(p => p.date >= from);
    if (to) pays = pays.filter(p => p.date <= to);
    if (custFilter) pays = pays.filter(p => (p.partyName || '').toLowerCase().includes(custFilter));
    if (userFilter) pays = pays.filter(p => p.createdBy === userFilter);

    if (!pays.length) {
        $('pay-trend-output').innerHTML = '<div class="empty-state"><span class="empty-icon">📊</span><p>No payments in selected range</p></div>';
        return;
    }

    // Build pivot: unique dates (columns) and customers (rows)
    const dates = [...new Set(pays.map(p => p.date))].sort();
    const customers = [...new Set(pays.map(p => p.partyName))].sort();

    // Build map: customer -> date -> amount
    const pivot = {};
    customers.forEach(c => { pivot[c] = {}; dates.forEach(d => pivot[c][d] = 0); });
    pays.forEach(p => { if (pivot[p.partyName]) pivot[p.partyName][p.date] = (pivot[p.partyName][p.date] || 0) + p.amount; });

    const rowTotals = {};
    customers.forEach(c => { rowTotals[c] = dates.reduce((s, d) => s + (pivot[c][d] || 0), 0); });
    const colTotals = {};
    dates.forEach(d => { colTotals[d] = customers.reduce((s, c) => s + (pivot[c][d] || 0), 0); });
    const grandTotal = Object.values(rowTotals).reduce((s, v) => s + v, 0);

    const fmtAmt = v => v > 0 ? `<span style="color:var(--success);font-weight:600">${v.toLocaleString('en-IN')}</span>` : `<span style="color:var(--text-muted)">-</span>`;

    $('pay-trend-output').innerHTML = `
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table" id="tbl-pay-trend" style="min-width:${Math.max(600, 180 + dates.length * 100)}px">
                    <thead><tr>
                        <th style="position:sticky;left:0;background:var(--bg-input);z-index:2;min-width:160px">Customer</th>
                        ${dates.map(d => `<th style="text-align:right;min-width:90px">${fmtDate(d)}</th>`).join('')}
                        <th style="text-align:right;background:rgba(249,115,22,0.08);color:var(--primary);min-width:100px">Total</th>
                    </tr></thead>
                    <tbody>
                        ${customers.map(c => `<tr>
                            <td style="position:sticky;left:0;background:var(--bg-card);font-weight:600;z-index:1">${escapeHtml(c)}</td>
                            ${dates.map(d => `<td style="text-align:right">${fmtAmt(pivot[c][d])}</td>`).join('')}
                            <td style="text-align:right;font-weight:700;color:var(--primary);background:rgba(249,115,22,0.05)">${currency(rowTotals[c])}</td>
                        </tr>`).join('')}
                        <tr style="font-weight:800;background:rgba(249,115,22,0.08);border-top:2px solid var(--primary)">
                            <td style="position:sticky;left:0;background:rgba(249,115,22,0.08);z-index:1;color:var(--primary)">Grand Total</td>
                            ${dates.map(d => `<td style="text-align:right;color:var(--primary)">${colTotals[d] > 0 ? colTotals[d].toLocaleString('en-IN') : '-'}</td>`).join('')}
                            <td style="text-align:right;color:var(--primary);font-size:1.05rem">${currency(grandTotal)}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div></div>`;
}
async function saveVyaparSetup() {
    const prefix = ($('f-vy-setup-prefix') || {}).value || '';
    const no = parseInt(($('f-vy-setup-no') || {}).value || '1');
    if (!no || isNaN(no) || no < 1) return alert('Enter a valid invoice number');
    const s = { prefix, currentNo: String(no) };
    await DB.saveSettings('vyapar_settings', s);
    showToast('Vyapar settings saved! Next: ' + prefix + no, 'success');
    renderCompanySetup();
}
async function savePaySetup() {
    const prefix = ($('f-pay-setup-prefix') || {}).value || 'PAY-';
    const no = parseInt(($('f-pay-setup-no') || {}).value || '1');
    if (!no || isNaN(no) || no < 1) return alert('Enter a valid payment number');
    const s = { prefix, currentNo: String(no) };
    await DB.saveSettings('pay_settings', s);
    showToast('Payment settings saved! Next: ' + prefix + String(no).padStart(4, '0'), 'success');
    renderCompanySetup();
}
function getPaymentTermsList() {
    const co = DB.getObj('db_company') || {};
    if (co.paymentTermsList && co.paymentTermsList.length) return co.paymentTermsList;
    return [
        { name: 'Due on Receipt', days: 0 },
        { name: 'Net 7',  days: 7  },
        { name: 'Net 15', days: 15 },
        { name: 'Net 30', days: 30 },
        { name: 'Net 45', days: 45 },
        { name: 'Net 60', days: 60 }
    ];
}
function updateNsPreview(key) {
    const prefix = (document.getElementById('ns-'+key+'-prefix')||{}).value || '';
    const pad    = parseInt((document.getElementById('ns-'+key+'-pad')||{}).value) || 5;
    const start  = parseInt((document.getElementById('ns-'+key+'-start')||{}).value) || 1;
    const el = document.getElementById('ns-'+key+'-preview');
    if (el) el.textContent = prefix + String(start).padStart(pad,'0');
}

async function saveNumberSeries() {
    const keys = ['inv','pur','so','po','cust','supp'];
    const ns = DB.ls.getObj('db_number_series') || {};
    for (const k of keys) {
        const prefix = (document.getElementById('ns-'+k+'-prefix')||{}).value || '';
        const pad    = parseInt((document.getElementById('ns-'+k+'-pad')||{}).value) || 5;
        const start  = parseInt((document.getElementById('ns-'+k+'-start')||{}).value) || 1;
        ns[k+'_prefix'] = prefix;
        ns[k+'_pad']    = pad;
        ns[k+'_start']  = start;
    }
    await DB.saveSettings('db_number_series', ns);
    showToast('Number series saved!', 'success');
}

function getNsSetting(key, field, fallback) {
    const ns = DB.ls.getObj('db_number_series') || {};
    return ns[key+'_'+field] !== undefined ? ns[key+'_'+field] : fallback;
}

async function nextPartyCode(type) {
    // type: 'Customer' or 'Supplier'
    const key     = type === 'Supplier' ? 'supp' : 'cust';
    const prefix  = getNsSetting(key, 'prefix', type === 'Supplier' ? 'SUP-' : 'CUST-');
    const pad     = getNsSetting(key, 'pad', 5);
    // Find max existing code number for this prefix
    const parties = DB.get('db_parties') || [];
    const nums = parties
        .filter(p => p.partyCode && p.partyCode.startsWith(prefix))
        .map(p => { const m = p.partyCode.match(/(\d+)$/); return m ? parseInt(m[1]) : 0; });
    const maxExisting = nums.length ? Math.max(...nums) : 0;
    const configStart = getNsSetting(key, 'start', 1);
    const next = Math.max(maxExisting + 1, configStart);
    // Update start for next call
    const ns = DB.ls.getObj('db_number_series') || {};
    ns[key+'_start'] = next + 1;
    await DB.saveSettings('db_number_series', ns);
    return prefix + String(next).padStart(pad, '0');
}

async function autoAssignPartyCodes() {
    const parties = await DB.getAll('parties');
    const without = parties.filter(p => !p.partyCode);
    if (!without.length) return showToast('All parties already have a code!', 'success');
    const confirmed = confirm(`${without.length} parties have no code.\n\nCustomers → CUST-00001 format\nSuppliers → SUP-00001 format\n\nProceed?`);
    if (!confirmed) return;

    const key_cust  = 'cust', key_supp = 'supp';
    const custPrefix = getNsSetting(key_cust, 'prefix', 'CUST-');
    const custPad    = getNsSetting(key_cust, 'pad', 5);
    const suppPrefix = getNsSetting(key_supp, 'prefix', 'SUP-');
    const suppPad    = getNsSetting(key_supp, 'pad', 5);

    // Find current max numbers
    const allParties = DB.get('db_parties') || [];
    const maxCust = allParties.filter(p => p.partyCode && p.partyCode.startsWith(custPrefix))
        .reduce((m, p) => { const n = p.partyCode.match(/(\d+)$/); return n ? Math.max(m, parseInt(n[1])) : m; }, 0);
    const maxSupp = allParties.filter(p => p.partyCode && p.partyCode.startsWith(suppPrefix))
        .reduce((m, p) => { const n = p.partyCode.match(/(\d+)$/); return n ? Math.max(m, parseInt(n[1])) : m; }, 0);

    let custCounter = Math.max(maxCust + 1, getNsSetting(key_cust, 'start', 1));
    let suppCounter = Math.max(maxSupp + 1, getNsSetting(key_supp, 'start', 1));

    try {
        // Update one by one (sequential) to catch first error clearly
        for (const p of without) {
            let code;
            if ((p.type || 'Customer').toLowerCase() === 'supplier') {
                code = suppPrefix + String(suppCounter).padStart(suppPad, '0');
                suppCounter++;
            } else {
                code = custPrefix + String(custCounter).padStart(custPad, '0');
                custCounter++;
            }
            await supabaseClient.from('parties').update({ party_code: code }).eq('id', p.id);
        }

        // Save updated counters
        const ns = DB.ls.getObj('db_number_series') || {};
        ns[key_cust+'_start'] = custCounter;
        ns[key_supp+'_start'] = suppCounter;
        await DB.saveSettings('db_number_series', ns);

        await DB.refreshTables(['parties']);
        showToast(`Done! ${without.length} parties assigned codes.`, 'success');
        await renderCompanySetup();
    } catch(e) {
        alert('Error assigning party codes: ' + e.message + '\n\nMake sure you have run the SQL:\nALTER TABLE parties ADD COLUMN IF NOT EXISTS party_code TEXT;');
    }
}

async function renderCompanySetup() {
    // Always reload from Supabase so UPI / settings are fresh on any device
    await DB.loadSettings();
    const co = DB.ls.getObj('db_company') || {};
    pageContent.innerHTML = `
        <div class="card"><div class="card-body padded">
            <h3 style="margin-bottom:20px;font-size:1.1rem">Company Information</h3>
            <div class="form-group">
                <label>Company Logo</label>
                <div style="display:flex;align-items:center;gap:16px;margin-bottom:8px">
                    ${co.logo ? `<img src="${co.logo}" style="max-height:60px;max-width:180px;object-fit:contain;border:1px solid var(--border);border-radius:8px;padding:4px" alt="Logo">` : '<div style="width:80px;height:50px;background:var(--bg-input);border:1px dashed var(--border);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:0.8rem;color:var(--text-muted)">No Logo</div>'}
                    <div><button class="btn btn-outline btn-sm" onclick="document.getElementById('logo-upload').click()">📷 Upload Logo</button>
                    ${co.logo ? ' <button class="btn btn-outline btn-sm" onclick="removeCompanyLogo()">✕ Remove</button>' : ''}</div>
                </div>
                <input type="file" id="logo-upload" accept="image/*" style="display:none" onchange="handleLogoUpload(event)">
            </div>
            <div class="form-group"><label>Company Name *</label><input id="f-co-name" value="${co.name || ''}"></div>
            <div class="form-row"><div class="form-group"><label>Phone</label><input id="f-co-phone" value="${co.phone || ''}"></div>
            <div class="form-group"><label>GSTIN</label><input id="f-co-gstin" value="${co.gstin || ''}"></div></div>
            <div class="form-group"><label>Address</label><input id="f-co-address" value="${co.address || ''}"></div>
            <div class="form-row"><div class="form-group"><label>City</label><input id="f-co-city" value="${co.city || ''}"></div>
            <div class="form-group"><label>UPI ID (Optional)</label><input id="f-co-upi" value="${co.upi || ''}" placeholder="e.g. 9876543210@upi"></div></div>
            <div class="form-group">
                <label>🏭 Warehouse / Office GPS <small style="color:var(--text-muted)">(used for route sheet distance sorting)</small></label>
                <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                    <input type="number" step="any" id="f-co-wlat" value="${co.warehouseLat || ''}" placeholder="Latitude" style="flex:1;min-width:120px">
                    <input type="number" step="any" id="f-co-wlng" value="${co.warehouseLng || ''}" placeholder="Longitude" style="flex:1;min-width:120px">
                    <button class="btn btn-outline btn-sm" type="button" onclick="captureWarehouseGps()">📍 Live Location</button>
                </div>
                ${co.warehouseLat ? `<small style="color:var(--success)">✅ Set: ${(+co.warehouseLat).toFixed(5)}, ${(+co.warehouseLng).toFixed(5)}</small>` : `<small style="color:var(--text-muted)">Not set — route sheet will not sort by distance until configured.</small>`}
            </div>
            <button class="btn btn-primary" onclick="saveCompanySetup()">Save Changes</button>
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
    <h3 style="margin-bottom:16px;font-size:1rem">📒 Vyapar Invoice Settings</h3>
    <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">Set the prefix and current number for Vyapar Invoice No. This auto-increments on each sale invoice save.</p>
    <div class="form-row">
        <div class="form-group">
            <label>Invoice Prefix <span style="font-size:0.75rem;color:var(--text-muted)">(e.g. PT-NS-)</span></label>
            <input id="f-vy-setup-prefix" value="${escapeHtml(getVyaparPrefix())}" placeholder="e.g. PT-NS-" oninput="const p=this.value,n=$('f-vy-setup-no').value,lbl=$('vy-setup-preview');if(lbl)lbl.textContent=p+n">
        </div>
        <div class="form-group">
            <label>Current Invoice No.</label>
            <input id="f-vy-setup-no" type="number" min="1" value="${getVyaparCurrentNo()}" style="font-weight:700" oninput="const p=$('f-vy-setup-prefix').value,n=this.value,lbl=$('vy-setup-preview');if(lbl)lbl.textContent=p+n">
        </div>
    </div>
    <div style="background:rgba(249,115,22,0.06);border:1px solid rgba(249,115,22,0.2);border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:0.9rem">
        Next invoice will be: <strong id="vy-setup-preview" style="color:var(--primary)">${escapeHtml(buildVyaparInvoiceNo())}</strong>
    </div>
    <button class="btn btn-primary" onclick="saveVyaparSetup()">Save Vyapar Settings</button>
</div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
    <h3 style="margin-bottom:6px;font-size:1rem">🔢 Number Series Setup</h3>
    <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:16px">Configure prefixes and starting numbers for all auto-generated codes.</p>
    ${(()=>{
        const ns = DB.ls.getObj('db_number_series') || {};
        const rows = [
            { key:'inv',   label:'Sale Invoice',    defPrefix:'INV-',   defStart:1 },
            { key:'pur',   label:'Purchase Invoice', defPrefix:'PUR-',   defStart:1 },
            { key:'so',    label:'Sales Order',      defPrefix:'SO-',    defStart:1 },
            { key:'po',    label:'Purchase Order',   defPrefix:'PO-',    defStart:1 },
            { key:'cust',  label:'Customer Code',    defPrefix:'CUST-',  defStart:1 },
            { key:'supp',  label:'Supplier Code',    defPrefix:'SUP-',   defStart:1 },
        ];
        return `<div class="table-wrapper"><table class="data-table">
            <thead><tr><th>Series</th><th>Prefix</th><th>Padding (digits)</th><th>Next No.</th><th>Preview</th></tr></thead>
            <tbody>${rows.map(r => {
                const prefix = ns[r.key+'_prefix'] !== undefined ? ns[r.key+'_prefix'] : r.defPrefix;
                const start  = ns[r.key+'_start']  !== undefined ? ns[r.key+'_start']  : r.defStart;
                const pad    = ns[r.key+'_pad']    !== undefined ? ns[r.key+'_pad']    : 5;
                const preview = prefix + String(start).padStart(pad,'0');
                return `<tr>
                    <td style="font-weight:600;font-size:0.88rem">${r.label}</td>
                    <td><input id="ns-${r.key}-prefix" value="${escapeHtml(prefix)}" style="width:90px;font-family:monospace" oninput="updateNsPreview('${r.key}')"></td>
                    <td><input id="ns-${r.key}-pad" type="number" min="1" max="8" value="${pad}" style="width:60px;text-align:center" oninput="updateNsPreview('${r.key}')"></td>
                    <td><input id="ns-${r.key}-start" type="number" min="1" value="${start}" style="width:80px;font-weight:700" oninput="updateNsPreview('${r.key}')"></td>
                    <td><span id="ns-${r.key}-preview" style="font-family:monospace;color:var(--accent);font-weight:700">${escapeHtml(preview)}</span></td>
                </tr>`;
            }).join('')}</tbody>
        </table></div>
        <button class="btn btn-primary" style="margin-top:14px" onclick="saveNumberSeries()">Save Number Series</button>`;
    })()}
</div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
    <h3 style="margin-bottom:16px;font-size:1rem">💳 Payment Reference No. Settings</h3>
    <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">Set the prefix and starting number for Payment receipts. Auto-increments on each payment saved.</p>
    <div class="form-row">
        <div class="form-group">
            <label>Payment Prefix <span style="font-size:0.75rem;color:var(--text-muted)">(e.g. PAY-, RCP-)</span></label>
            <input id="f-pay-setup-prefix" value="${escapeHtml(getPayPrefix())}" placeholder="e.g. PAY-" oninput="const p=this.value,n=$('f-pay-setup-no').value,lbl=$('pay-setup-preview');if(lbl)lbl.textContent=p+String(n).padStart(4,'0')">
        </div>
        <div class="form-group">
            <label>Current Number</label>
            <input id="f-pay-setup-no" type="number" min="1" value="${getPayCurrentNo()}" style="font-weight:700" oninput="const p=$('f-pay-setup-prefix').value,n=this.value,lbl=$('pay-setup-preview');if(lbl)lbl.textContent=p+String(n).padStart(4,'0')">
        </div>
    </div>
    <div style="background:rgba(99,102,241,0.06);border:1px solid rgba(99,102,241,0.2);border-radius:8px;padding:10px 14px;margin-bottom:14px;font-size:0.9rem">
        Next payment will be: <strong id="pay-setup-preview" style="color:var(--accent)">${escapeHtml(buildPayRefNo())}</strong>
    </div>
    <button class="btn btn-primary" onclick="savePaySetup()">Save Payment Settings</button>
</div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:6px;font-size:1rem">⏱️ Payment Terms Master</h3>
            <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">Define payment terms used on party accounts. These drive due date and aging calculations automatically.</p>
            <div class="table-wrapper">
                <table class="data-table" style="margin-bottom:12px">
                    <thead><tr><th>Term Name</th><th style="width:110px">Days</th><th style="width:50px"></th></tr></thead>
                    <tbody id="pt-list-body">
                        ${getPaymentTermsList().map((t,i) => `<tr>
                            <td><input id="pt-name-${i}" value="${escapeHtml(t.name)}" style="width:100%;border:none;background:transparent;font-size:0.9rem" placeholder="e.g. Net 30"></td>
                            <td><input id="pt-days-${i}" type="number" value="${t.days}" min="0" style="width:80px;border:none;background:transparent;font-weight:600;text-align:center"></td>
                            <td><button class="btn-icon" style="color:var(--danger)" onclick="deletePaymentTerm(${i})" title="Delete">🗑️</button></td>
                        </tr>`).join('')}
                    </tbody>
                </table>
            </div>
            <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px">
                <button class="btn btn-outline btn-sm" onclick="addPaymentTermRow()">+ Add Term</button>
                <button class="btn btn-primary btn-sm" onclick="savePaymentTerms()">Save Terms</button>
            </div>
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:16px;font-size:1rem">📦 Inventory Settings</h3>
            <div style="display:flex;align-items:flex-start;gap:14px;padding:14px;background:rgba(99,102,241,0.05);border:1px solid rgba(99,102,241,0.18);border-radius:10px">
                <input type="checkbox" id="f-allow-neg-stock" ${co.allowNegativeStock ? 'checked' : ''} style="margin-top:3px;width:18px;height:18px;cursor:pointer">
                <div>
                    <div style="font-weight:600;font-size:0.95rem">Allow Negative Stock</div>
                    <div style="font-size:0.82rem;color:var(--text-muted);margin-top:2px">When disabled, sale invoices will be blocked if stock is insufficient. Currently: <strong>${co.allowNegativeStock ? '<span style="color:var(--warning)">Allowed</span>' : '<span style="color:var(--success)">Blocked (safe)</span>'}</strong></div>
                </div>
            </div>
            <button class="btn btn-primary" style="margin-top:14px" onclick="saveInventorySettings()">Save Inventory Settings</button>
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:10px;font-size:1rem">📲 Fast2SMS — OTP Settings</h3>
            <p style="font-size:0.82rem;color:var(--text-muted);margin-bottom:14px">Automatically sends OTP to customers on the Customer Portal. Get API key from fast2sms.com → Dev → API.</p>
            <div class="form-group">
                <label>Fast2SMS API Key</label>
                <div style="display:flex;gap:8px">
                    <input type="password" id="f-f2s-key" value="${co.fast2smsKey || ''}" placeholder="Paste your Fast2SMS API key here" style="flex:1">
                    <button class="btn btn-outline btn-sm" type="button" onclick="const el=document.getElementById('f-f2s-key');el.type=el.type==='password'?'text':'password'">👁</button>
                </div>
            </div>
            <button class="btn btn-primary" onclick="saveF2SKey()">Save SMS Key</button>
            ${co.fast2smsKey ? ' <span style="margin-left:12px;font-size:0.82rem;color:var(--success)">✅ OTP will be sent via SMS automatically</span>' : ' <span style="margin-left:12px;font-size:0.82rem;color:var(--text-muted)">No key set — OTP shown on screen only</span>'}
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:10px;font-size:1rem">🔧 Admin Tools</h3>
            <p style="font-size:0.85rem;color:var(--text-muted);margin-bottom:14px">Maintenance tools for data integrity and bulk operations.</p>
            <div style="display:flex;gap:10px;flex-wrap:wrap">
                <button class="btn btn-outline" onclick="healStockLedger()">🩺 Heal Stock Ledger</button>
                <button class="btn btn-outline" style="border-color:#f59e0b;color:#f59e0b" onclick="autoAssignPartyCodes()">🔄 Auto-assign Party Codes</button>
            </div>
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:10px;font-size:1rem">💾 Data Backup & Restore</h3>
            <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:14px">Export all app data as JSON backup, or restore from a previous backup.</p>
            <div style="display:flex;gap:10px;flex-wrap:wrap">
                <button class="btn btn-primary" onclick="exportDataBackup()">📤 Export Backup</button>
                <button class="btn btn-outline" onclick="importDataBackup()">📥 Import Backup</button>
            </div>
            <input type="file" id="backup-file-input" accept=".json" style="display:none" onchange="processBackupImport(event)">
        </div></div>
        <div class="card" style="margin-top:20px"><div class="card-body padded">
            <h3 style="margin-bottom:10px;font-size:1rem;color:var(--danger)">⚠️ Danger Zone</h3>
            <p style="font-size:0.85rem;color:var(--text-secondary);margin-bottom:14px">Selectively delete entries or master data. Cannot be undone.</p>
            <button class="btn btn-danger" onclick="openSmartReset()">🗑️ Reset Data</button>
        </div></div>`;
}
async function saveCompanySetup() {
    const name = $('f-co-name').value.trim(); if (!name) return alert('Company name required');
    const co = DB.getObj('db_company');
    const wlat = $('f-co-wlat') ? $('f-co-wlat').value.trim() : '';
    const wlng = $('f-co-wlng') ? $('f-co-wlng').value.trim() : '';
    const newCo = { ...co, name, phone: $('f-co-phone').value.trim(), gstin: $('f-co-gstin').value.trim(), address: $('f-co-address').value.trim(), city: $('f-co-city').value.trim(), upi: $('f-co-upi').value.trim(), warehouseLat: wlat ? parseFloat(wlat) : null, warehouseLng: wlng ? parseFloat(wlng) : null };
    await DB.saveSettings('db_company', newCo);
    $('sidebar-brand').textContent = name;
    showToast('Company info saved!', 'success');
}
function captureWarehouseGps() {
    if (!navigator.geolocation) return alert('Geolocation not supported.');
    navigator.geolocation.getCurrentPosition(pos => {
        const latEl = $('f-co-wlat'), lngEl = $('f-co-wlng');
        if (latEl) latEl.value = pos.coords.latitude.toFixed(6);
        if (lngEl) lngEl.value = pos.coords.longitude.toFixed(6);
        showToast('Warehouse location captured! Click Save Changes.', 'success');
    }, err => alert('Could not get location: ' + err.message), { enableHighAccuracy: true });
}
async function saveF2SKey() {
    const key = ($('f-f2s-key') || {}).value || '';
    if (!key.trim()) { showToast('Please enter your Fast2SMS API key', 'error'); return; }
    const co = DB.ls.getObj('db_company') || {};
    co.fast2smsKey = key.trim();
    await DB.saveSettings('db_company', co);
    showToast('Fast2SMS key saved! OTP will now be sent via SMS.', 'success');
    renderCompanySetup();
}

async function saveInventorySettings() {
    const co = DB.getObj('db_company');
    co.allowNegativeStock = !!($('f-allow-neg-stock') && $('f-allow-neg-stock').checked);
    await DB.saveSettings('db_company', co);
    renderCompanySetup();
    showToast('Inventory settings saved', 'success');
}
async function savePaymentTerms() {
    const rows = document.querySelectorAll('#pt-list-body tr');
    const list = [];
    rows.forEach((_, i) => {
        const name = ($('pt-name-' + i) || {}).value.trim();
        const days = parseInt(($('pt-days-' + i) || {}).value || '0');
        if (name) list.push({ name, days: isNaN(days) ? 0 : days });
    });
    const co = DB.getObj('db_company') || {};
    await DB.saveSettings('db_company', { ...co, paymentTermsList: list });
    showToast('Payment terms saved!', 'success');
    renderCompanySetup();
}
function addPaymentTermRow() {
    const tbody = document.getElementById('pt-list-body');
    if (!tbody) return;
    const i = tbody.rows.length;
    const tr = document.createElement('tr');
    tr.innerHTML = `<td><input id="pt-name-${i}" style="width:100%;border:none;background:transparent;font-size:0.9rem" placeholder="e.g. Net 30"></td>
        <td><input id="pt-days-${i}" type="number" min="0" value="0" style="width:80px;border:none;background:transparent;font-weight:600;text-align:center"></td>
        <td><button class="btn-icon" style="color:var(--danger)" onclick="this.closest('tr').remove()" title="Delete">🗑️</button></td>`;
    tbody.appendChild(tr);
    const inp = document.getElementById('pt-name-' + i);
    if (inp) inp.focus();
}
async function deletePaymentTerm(idx) {
    const terms = getPaymentTermsList().filter((_,i) => i !== idx);
    const co = DB.getObj('db_company') || {};
    await DB.saveSettings('db_company', { ...co, paymentTermsList: terms });
    renderCompanySetup();
}
async function healStockLedger() {
    if (!confirm('This will scan all items and create correction ledger entries for any discrepancies. Continue?')) return;
    const [items, ledger] = await Promise.all([DB.getAll('inventory'), DB.getAll('stock_ledger')]);
    let fixed = 0;
    const report = [];
    for (const item of items) {
        const entries = ledger.filter(e => e.itemId === item.id);
        const ledgerSum = entries.reduce((s, e) => s + (e.qty || 0), 0);
        const diff = (item.stock || 0) - ledgerSum;
        if (Math.abs(diff) > 0.001) {
            await addLedgerEntry(item.id, item.name, diff > 0 ? 'Positive Adj' : 'Negative Adj', diff,
                'HEAL-' + item.id.substr(0, 6).toUpperCase(), 'Stock ledger reconciliation');
            report.push(`${item.name}: ledger total=${ledgerSum}, actual stock=${item.stock || 0}, correction=${diff > 0 ? '+' : ''}${diff}`);
            fixed++;
        }
    }
    if (!fixed) {
        showToast('✅ All items balanced — no discrepancies found!', 'success');
    } else {
        alert(`Healed ${fixed} item(s):\n\n${report.join('\n')}`);
        showToast(`Healed ${fixed} ledger discrepancy(s)`, 'success');
    }
}
function handleLogoUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    showToast('Processing logo...', 'info');
    compressImage(file, { maxWidth: 512, quality: 0.8 }).then(async dataUrl => {
        const co = DB.getObj('db_company');
        co.logo = dataUrl;
        await DB.saveSettings('db_company', co);
        renderCompanySetup();
        showToast('Logo uploaded!', 'success');
    }).catch(err => {
        console.error('Logo upload error:', err);
        showToast('Failed to upload logo: ' + err.message, 'error');
    });
}
async function removeCompanyLogo() {
    const co = DB.getObj('db_company');
    delete co.logo;
    await DB.saveSettings('db_company', co);
    renderCompanySetup();
    showToast('Logo removed.', 'info');
}
function openSmartReset() {
    window._resetOption = 'entries';
    openModal('🗑️ Reset Data', `
        <p style="color:var(--text-secondary);font-size:0.88rem;margin-bottom:14px">Choose what to delete. This <strong>cannot be undone</strong>.</p>
        <div id="reset-opt-entries" class="reset-option-card active" onclick="selectResetOption('entries')">
            <div style="font-weight:700">📋 Entries Only</div>
            <div style="font-size:0.8rem;color:var(--text-muted);margin-top:4px">Orders, Invoices, Payments, Expenses, Packing, Delivery, Ledger entries</div>
            <div style="font-size:0.78rem;color:var(--success);margin-top:4px">✅ Masters (Parties, Items, etc.) kept</div>
        </div>
        <div id="reset-opt-all" class="reset-option-card" onclick="selectResetOption('all')" style="margin-top:10px">
            <div style="font-weight:700">💣 Entries + Masters</div>
            <div style="font-size:0.8rem;color:var(--text-muted);margin-top:4px">Transactions AND selected master data</div>
        </div>
        <div id="reset-masters-section" style="display:none;margin-top:12px;padding:12px;background:var(--bg-input);border-radius:var(--radius-md)">
            <div style="font-size:0.8rem;font-weight:700;margin-bottom:10px;color:var(--text-secondary);letter-spacing:0.05em">SELECT MASTERS TO DELETE:</div>
            <label class="reset-master-check"><input type="checkbox" id="rm-parties" checked> Parties / Customers / Suppliers</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-inventory" checked> Items / Inventory</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-categories"> Categories</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-uom"> Units of Measure (UOM)</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-brands"> Brands</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-delpersons"> Delivery Persons</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-packers"> Packers</label>
            <label class="reset-master-check"><input type="checkbox" id="rm-users"> Users</label>
        </div>
        <div style="margin-top:16px">
            <label style="font-size:0.85rem;color:var(--text-secondary)">Type <strong>RESET</strong> to confirm:</label>
            <input id="reset-confirm-input" type="text" class="form-input" placeholder="RESET" style="margin-top:6px;font-weight:700;letter-spacing:2px;text-transform:uppercase">
        </div>`,
        `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
         <button class="btn btn-danger" onclick="executeSmartReset()">🗑️ Reset Now</button>`);
}

function selectResetOption(opt) {
    window._resetOption = opt;
    document.getElementById('reset-opt-entries').classList.toggle('active', opt === 'entries');
    document.getElementById('reset-opt-all').classList.toggle('active', opt === 'all');
    document.getElementById('reset-masters-section').style.display = opt === 'all' ? 'block' : 'none';
}

async function executeSmartReset() {
    const confirmVal = ($('reset-confirm-input') || {}).value || '';
    if (confirmVal.trim().toUpperCase() !== 'RESET') { showToast('Type RESET to confirm', 'error'); return; }

    const entryTables = ['sales_orders', 'invoices', 'payments', 'expenses', 'stock_ledger', 'party_ledger', 'delivery'];
    const entryLsKeys = ['db_salesorders', 'db_invoices', 'db_payments', 'db_expenses', 'db_packing', 'db_delivery', 'db_stock_ledger', 'db_party_ledger', 'db_counters'];
    const masterMap = [
        { id: 'rm-parties',    supabase: 'parties',          ls: 'db_parties' },
        { id: 'rm-inventory',  supabase: 'inventory',        ls: 'db_inventory' },
        { id: 'rm-categories', supabase: 'categories',       ls: 'db_categories' },
        { id: 'rm-uom',        supabase: 'uom',              ls: 'db_uom' },
        { id: 'rm-brands',     supabase: null,               ls: 'db_brands' },
        { id: 'rm-delpersons', supabase: 'delivery_persons', ls: 'db_delivery_persons' },
        { id: 'rm-packers',    supabase: 'packers',          ls: 'db_packers' },
        { id: 'rm-users',      supabase: 'users',            ls: 'db_users' },
    ];

    const btn = document.querySelector('#modal-overlay .btn-danger');
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Resetting...'; }

    try {
        for (const t of entryTables) {
            await supabaseClient.from(t).delete().not('id', 'is', null);
        }
        // Zero out all party balances so receivable/payable resets
        await supabaseClient.from('parties').update({ balance: 0 }).not('id', 'is', null);
        entryLsKeys.forEach(k => localStorage.removeItem(k));
        localStorage.removeItem('db_parties'); // force re-fetch so balance shows 0

        if (window._resetOption === 'all') {
            for (const m of masterMap) {
                const el = document.getElementById(m.id);
                if (el && el.checked) {
                    if (m.supabase) await supabaseClient.from(m.supabase).delete().not('id', 'is', null);
                    localStorage.removeItem(m.ls);
                }
            }
        }

        closeModal();
        showToast('Reset complete! Reloading...', 'success');
        setTimeout(() => location.reload(), 1200);
    } catch(e) {
        showToast('Reset failed: ' + e.message, 'error');
        if (btn) { btn.disabled = false; btn.textContent = '🗑️ Reset Now'; }
    }
}

// --- Data Backup & Restore ---
function exportDataBackup() {
    const keys = ['db_users', 'db_parties', 'db_inventory', 'db_invoices', 'db_payments', 'db_expenses', 'db_packing', 'db_delivery', 'db_salesorders', 'db_delivery_persons', 'db_packers', 'db_stock_ledger', 'db_party_ledger', 'db_company', 'db_counters', 'db_categories', 'db_uom', 'db_brands', 'db_tax_settings'];
    const backup = {};
    keys.forEach(k => { backup[k] = localStorage.getItem(k); });
    backup._meta = { exportedAt: new Date().toISOString(), version: '1.0' };
    const blob = new Blob([JSON.stringify(backup, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'distromanager_backup_' + today() + '.json';
    document.body.appendChild(a); a.click();
    setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 100);
    showToast('Backup exported successfully!', 'success');
}
function importDataBackup() {
    const input = $('backup-file-input');
    if (input) { input.value = ''; input.click(); }
}
function processBackupImport(event) {
    const file = event.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function (e) {
        try {
            const backup = JSON.parse(e.target.result);
            if (!backup.db_users && !backup.db_company) {
                alert('Invalid backup file. Missing required data.'); return;
            }
            if (!confirm('This will REPLACE all current data with the backup. Continue?')) return;
            Object.keys(backup).forEach(k => {
                if (k !== '_meta' && backup[k] !== null) {
                    localStorage.setItem(k, backup[k]);
                }
            });
            showToast('Backup restored! Reloading...', 'success');
            setTimeout(() => location.reload(), 1500);
        } catch (err) {
            alert('Error reading backup file: ' + err.message);
        }
    };
    reader.readAsText(file);
}

function openDedicatedPartyLedger(partyId) {
    currentLedgerPartyId = partyId;
    navigateTo('partyledger');
}

async function renderPartyLedgerLayout() {
    if (!currentLedgerPartyId) {
        navigateTo('parties');
        return;
    }
    const partyId = currentLedgerPartyId;
    // BUG-015 fix: always fetch fresh data so running balance is up-to-date after edits
    await Promise.all([DB.getAll('party_ledger'), DB.getAll('parties')]);
    const party = DB.get('db_parties').find(p => p.id === partyId);
    if (!party) {
        navigateTo('parties');
        return;
    }

    const ledger = (DB.cache['party_ledger'] || DB.get('db_party_ledger') || []).filter(l => String(l.partyId) === String(partyId)).sort((a, b) => new Date(a.date) - new Date(b.date) || String(a.id).localeCompare(String(b.id)));

    // Compute running balance fresh from sorted entries (ignore stored balance field)
    let _rb = 0;
    ledger.forEach(e => { _rb += (e.amount || 0); e._runningBalance = _rb; });

    pageContent.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem;display:flex;align-items:center;gap:10px">
                <button class="btn-icon" onclick="navigateTo('parties')" title="Back to Parties">⬅️</button>
                📜 Ledger: ${party.name} <span class="badge ${party.type === 'Customer' ? 'badge-success' : 'badge-info'}" style="font-size:0.7rem">${party.type}</span>
            </h3>
            <div class="filter-group">
                <button class="btn btn-outline btn-sm" onclick="exportPartyLedger('${partyId}')">📥 Export Excel</button>
            </div>
        </div>
        <div class="card" style="margin-bottom:15px">
            <div class="card-body" style="display:flex;justify-content:space-between;align-items:center">
                <div>
                    <span style="font-size:0.9rem;color:var(--text-muted)">${party.phone || 'No phone'} | ${party.city || 'No city'}</span>
                </div>
                <div style="text-align:right">
                    <div style="font-size:0.8rem;color:var(--text-muted)">Current Balance</div>
                    <div style="font-size:1.4rem;font-weight:700;color:${party.balance >= 0 ? 'var(--success)' : 'var(--danger)'}">${currency(Math.abs(party.balance || 0))} ${party.balance < 0 ? '(Cr)' : '(Dr)'}</div>
                </div>
            </div>
        </div>
        <div class="card" style="margin-bottom:15px">
            <div class="card-body" style="display:flex;gap:10px;flex-wrap:wrap;padding:12px">
                <input class="search-box" id="ledg-filter-search" placeholder="Search Ref# or Reason..." oninput="filterPartyLedger()" style="flex:1">
                <select id="ledg-filter-type" class="search-box" style="width:auto" onchange="filterPartyLedger()">
                    <option value="">All Types</option>
                    <option value="Sale Invoice">Sale Invoice</option>
                    <option value="Purchase Invoice">Purchase</option>
                    <option value="Payment In">Payment In</option>
                    <option value="Payment Out">Payment Out</option>
                    <option value="Sale Cancel">Cancel</option>
                </select>
                <input type="date" id="ledg-filter-from" class="search-box" style="width:auto" onchange="filterPartyLedger()" title="From Date">
                <input type="date" id="ledg-filter-to" class="search-box" style="width:auto" onchange="filterPartyLedger()" title="To Date">
                <button class="btn btn-outline" onclick="document.getElementById('ledg-filter-search').value='';document.getElementById('ledg-filter-type').value='';document.getElementById('ledg-filter-from').value='';document.getElementById('ledg-filter-to').value='';filterPartyLedger()">Clear</button>
            </div>
        </div>
        
        <div class="card">
            <div class="card-body" style="padding:0">
                ${ledger.length ? `
                <div class="table-wrapper" style="max-height:600px;overflow-y:auto">
                    <table class="data-table">
                        <thead style="position:sticky;top:0;background:var(--bg-body);z-index:10;box-shadow:0 1px 2px rgba(0,0,0,0.05)">
                            <tr><th>Date</th><th>Type</th><th>Ref #</th><th>Doc Bal</th><th>Dr/Cr</th><th>Balance</th><th>Reason</th><th>By</th>${canEdit() ? '<th>Actions</th>' : ''}</tr>
                        </thead>
                        <tbody id="party-ledger-tbody">
                            ${ledger.slice().reverse().map(e => {
        const entryType = e.type || e.entryType || '-';
        const docNo     = e.docNo || e.documentNo || '';
        const bal       = e._runningBalance !== undefined ? e._runningBalance : '';
        const reason    = e.notes || e.reason || '';
        let docBalCell = '<td style="font-size:0.82rem;color:var(--text-muted)">-</td>';
        if (entryType.includes('Invoice') && docNo) {
            const allInvs = DB.cache['invoices'] || DB.get('db_invoices') || [];
            const inv = allInvs.find(i => i.invoiceNo === docNo);
            if (inv) {
                const allPays = DB.cache['payments'] || DB.get('db_payments') || [];
                let paid = 0;
                allPays.forEach(p => {
                    if (p.invoiceNo === inv.invoiceNo) paid += (p.amount || 0);
                    if (p.allocations && p.allocations[inv.invoiceNo]) paid += p.allocations[inv.invoiceNo];
                });
                const due = inv.total - paid;
                const dueColor = due <= 0.01 ? 'var(--success)' : 'var(--danger)';
                docBalCell = `<td style="font-size:0.85rem;font-weight:600;color:${dueColor};white-space:nowrap">${due <= 0.01 ? '✅ Settled' : '₹' + due.toFixed(2)}</td>`;
            }
        } else if ((entryType === 'Payment In' || entryType === 'Payment Out') && docNo) {
            const allPays = DB.cache['payments'] || DB.get('db_payments') || [];
            // Match by payNo (new records) or by id (old UUID records)
            const pay = allPays.find(p => p.payNo === docNo) || allPays.find(p => p.id === docNo);
            if (pay) {
                let allocated = 0;
                if (pay.allocations) {
                    // Advance with explicit per-invoice allocations
                    Object.values(pay.allocations).forEach(v => allocated += (+v || 0));
                } else if (pay.invoiceNo && pay.invoiceNo !== 'Advance' && pay.invoiceNo !== 'Multi' && pay.invoiceNo !== '') {
                    // Direct payment linked to one invoice — only consume up to that invoice's total
                    const allInvs2 = DB.cache['invoices'] || DB.get('db_invoices') || [];
                    const linkedInv = allInvs2.find(i => i.invoiceNo === pay.invoiceNo);
                    allocated = linkedInv ? Math.min(pay.amount || 0, linkedInv.total || 0) : (pay.amount || 0);
                }
                const unallocated = (pay.amount || 0) - allocated;
                if (unallocated <= 0.01) {
                    docBalCell = `<td style="font-size:0.85rem;font-weight:600;color:var(--success);white-space:nowrap">✅ Fully Applied</td>`;
                } else {
                    docBalCell = `<td style="font-size:0.85rem;font-weight:600;color:var(--warning);white-space:nowrap">₹${unallocated.toFixed(2)} Advance</td>`;
                }
            }
        }
        return `
                            <tr class="ledger-row ${e.amount >= 0 ? 'ledger-row-positive' : 'ledger-row-negative'}" data-date="${e.date}" data-type="${entryType}">
                                <td style="white-space:nowrap">${fmtDate(e.date)}</td>
                                <td>${entryType}</td>
                                <td style="font-weight:600" class="ledg-ref">${docNo || '-'}</td>
                                ${docBalCell}
                                <td style="font-weight:700;color:${e.amount > 0 ? 'var(--success)' : (e.amount < 0 ? 'var(--danger)' : 'inherit')}">${e.amount > 0 ? '+' : ''}${currency(Math.abs(e.amount))}</td>
                                <td>${bal !== '' ? currency(bal) : '-'}</td>
                                <td class="ledg-reason" style="font-size:0.85rem;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${reason}">${reason || '-'}</td>
                                <td style="font-size:0.8rem">${e.createdBy || '-'}</td>
                                <td>
                                    <div class="action-btns" style="flex-wrap:nowrap">
                                        ${entryType.includes('Invoice') && docNo ? `<button class="btn-icon" onclick="showPaymentHistory('${partyId}', '${docNo}')" title="Payment History">💳</button>` : ''}
                                        ${canEdit() ? `<button class="btn-icon" onclick="editPartyLedgerEntry('${partyId}','${e.id}')" title="Edit Entry">✏️</button><button class="btn-icon" onclick="deletePartyLedgerEntry('${partyId}','${e.id}')" title="Delete Entry" style="color:var(--danger)">🗑️</button>` : ''}
                                    </div>
                                </td>
                            </tr>`;
    }).join('')}
                        </tbody>
                    </table>
                </div>` : '<div class="empty-state" style="padding:40px"><div class="empty-icon">📜</div><p>No transactions recorded yet.</p></div>'}
            </div>
        </div>
    `;
}

// --- Payment History Modal ---
async function showPaymentHistory(partyId, invoiceNo) {
    const payments = await DB.getAll('payments');
    let partyPayments = payments.filter(p => String(p.partyId) === String(partyId));

    if (invoiceNo) {
        partyPayments = partyPayments.filter(p => {
            if (p.invoiceNo === invoiceNo) return true;
            if (p.allocations && p.allocations[invoiceNo]) return true;
            return false;
        });
    }

    partyPayments.sort((a, b) => new Date(a.date || a.created_at) - new Date(b.date || b.created_at));

    const modalTitle = invoiceNo ? `Payment History for ${invoiceNo}` : 'Payment History';

    if (!partyPayments.length) {
        openModal(modalTitle, '<div class="empty-state"><span class="empty-icon">💳</span><p>No payment records found for this invoice.</p></div>');
        return;
    }

    // Build the rows: each payment with its date, ref, type, and linked amount
    let totalLinked = 0;
    const rows = partyPayments.map(p => {
        const dt = p.date ? new Date(p.date).toLocaleDateString('en-IN', {day:'2-digit', month:'2-digit', year:'numeric'}) : '-';
        const refNo = p.payNo || p.id || '-';
        const txType = p.type === 'out' ? 'Payment-Out' : 'Payment-In';
        
        let linkedAmt = 0;
        let linkedInfo = '';
        if (p.allocations && invoiceNo && p.allocations[invoiceNo]) {
            linkedAmt = Number(p.allocations[invoiceNo]);
            linkedInfo = invoiceNo;
        } else if (p.invoiceNo === invoiceNo) {
            linkedAmt = Number(p.amount || 0);
            linkedInfo = invoiceNo;
        } else {
            linkedAmt = Number(p.amount || 0); // fallback if no invoice specified
            if (p.invoiceNo && p.invoiceNo !== 'Advance' && p.invoiceNo !== 'Multi') {
                linkedInfo = p.invoiceNo;
            } else if (p.allocations) {
                linkedInfo = Object.keys(p.allocations).join(', ');
            } else {
                linkedInfo = 'Advance';
            }
        }
        
        totalLinked += linkedAmt;

        return `<tr>
            <td style="font-size:0.85rem">${dt}</td>
            <td style="font-size:0.85rem;font-weight:600">${refNo}</td>
            <td><span class="badge ${p.type === 'out' ? 'badge-warning' : 'badge-success'}" style="font-size:0.75rem">${txType}</span></td>
            <td style="font-size:0.85rem;color:var(--text-muted)">${linkedInfo}</td>
            <td style="text-align:right;font-weight:600;font-size:0.9rem">${currency(linkedAmt)}</td>
        </tr>`;
    }).join('');

    openModal(modalTitle, `
        <div class="table-wrapper" style="max-height:500px;overflow-y:auto">
            <table class="data-table">
                <thead style="position:sticky;top:0;background:var(--bg-body);z-index:5">
                    <tr><th>Transaction Date</th><th>Ref No</th><th>Transaction Type</th><th>Linked Invoice</th><th style="text-align:right">Linked Amount</th></tr>
                </thead>
                <tbody>${rows}</tbody>
                <tfoot>
                    <tr style="border-top:2px solid var(--border);font-weight:700">
                        <td colspan="4" style="text-align:right;font-size:0.9rem">Total Received for Invoice:</td>
                        <td style="text-align:right;font-size:1.05rem;color:var(--primary)">${currency(totalLinked)}</td>
                    </tr>
                </tfoot>
            </table>
        </div>
    `);
}

function exportPartyLedger(partyId) {
    if (typeof XLSX === 'undefined') return alert('Excel library not loaded. Please check your connection.');
    const party = (DB.cache['parties'] || DB.get('db_parties') || []).find(p => String(p.id) === String(partyId));
    const ledger = (DB.cache['party_ledger'] || DB.get('db_party_ledger') || [])
        .filter(l => String(l.partyId) === String(partyId))
        .sort((a, b) => new Date(a.date) - new Date(b.date) || String(a.id).localeCompare(String(b.id)));
    let rb = 0;
    const rows = [['Date', 'Type', 'Ref #', 'Dr/Cr', 'Running Balance', 'Reason', 'By']];
    ledger.forEach(e => {
        rb += (e.amount || 0);
        rows.push([
            e.date || '',
            e.type || e.entryType || '',
            e.docNo || e.documentNo || '',
            e.amount || 0,
            rb,
            e.notes || e.reason || '',
            e.createdBy || ''
        ]);
    });
    rows.push([]);
    rows.push(['', '', '', 'Closing Balance', rb, '', '']);
    const ws = XLSX.utils.aoa_to_sheet(rows);
    ws['!cols'] = [{ wch: 14 }, { wch: 18 }, { wch: 22 }, { wch: 14 }, { wch: 18 }, { wch: 30 }, { wch: 12 }];
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Party Ledger');
    const name = party ? party.name.replace(/[^a-z0-9]/gi, '_') : 'Ledger';
    XLSX.writeFile(wb, `Ledger_${name}_${today()}.xlsx`);
}

function editPartyLedgerEntry(partyId, ledgerId) {
    const ledger = DB.get('db_party_ledger');
    const entry = ledger.find(x => x.id === ledgerId);
    if (!entry) return;
    openModal('Edit Ledger Entry', `
        <div style="font-size:0.85rem;color:var(--warning);margin-bottom:15px">
            ⚠️ <strong>Warning:</strong> Editing system-generated entries (like Invoice Postings) here will NOT update the original invoice document. This only adjusts the ledger line and running balance.
        </div>
        <div class="form-row">
            <div class="form-group"><label>Date *</label><input type="date" id="f-ledg-date" value="${entry.date || today()}"></div>
            <div class="form-group"><label>Ref / Doc #</label><input id="f-ledg-doc" value="${entry.docNo || entry.documentNo || ''}"></div>
        </div>
        <div class="form-row">
            <div class="form-group"><label>Amount ₹ * <span style="font-weight:normal;color:var(--text-muted)">(Positive = Cr/Received, Negative = Dr/Paid)</span></label>
                <input type="number" step="0.01" id="f-ledg-amount" value="${entry.amount}">
            </div>
        </div>
        <div class="form-group"><label>Reason</label><input id="f-ledg-reason" value="${entry.notes || entry.reason || ''}"></div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" onclick="savePartyLedgerEntry('${partyId}', '${ledgerId}')">Save Changes</button>
        </div>
    `);
}

async function savePartyLedgerEntry(partyId, ledgerId) {
    const date = $('f-ledg-date').value;
    const docNo = $('f-ledg-doc').value.trim();
    const amount = +$('f-ledg-amount').value;
    const reason = $('f-ledg-reason').value.trim();

    if (!date) return alert('Date is required');
    if (isNaN(amount) || amount === 0) return alert('Valid non-zero amount is required');

    try {
        // BUG-015 fix: save to Supabase so running balance is fresh on re-render
        await DB.update('party_ledger', ledgerId, { date, docNo: docNo, amount, notes: reason });
        await recalculatePartyLedger(partyId);
        closeModal();
        renderPartyLedgerLayout(); // Refresh UI (async, fetches fresh Supabase data)
        showToast('Ledger entry updated.', 'success');
    } catch(err) {
        alert('Error saving: ' + (err.message || err));
    }
}

function filterPartyLedger() {
    const search = $('ledg-filter-search').value.toLowerCase();
    const type = $('ledg-filter-type').value;
    const fromDate = $('ledg-filter-from').value;
    const toDate = $('ledg-filter-to').value;

    document.querySelectorAll('#party-ledger-tbody .ledger-row').forEach(row => {
        const rowDate = row.getAttribute('data-date');
        const rowType = row.getAttribute('data-type');
        const ref = (row.querySelector('.ledg-ref').textContent || '').toLowerCase();
        const reason = (row.querySelector('.ledg-reason').textContent || '').toLowerCase();

        let match = true;
        if (search && !ref.includes(search) && !reason.includes(search)) match = false;
        if (type && rowType !== type && !rowType.includes(type)) match = false;
        if (fromDate && rowDate < fromDate) match = false;
        if (toDate && rowDate > toDate) match = false;

        row.style.display = match ? '' : 'none';
    });
}

function deletePartyLedgerEntry(partyId, ledgerId) {
    openModal('Confirm Delete', `
        <p style="margin-bottom:20px;color:var(--text-secondary)">Are you sure you want to permanently delete this ledger entry? Running balances will be auto-recalculated.</p>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
            <button class="btn btn-primary" style="background:var(--danger)" onclick="confirmDeleteLedgerEntry('${partyId}', '${ledgerId}')">Yes, Delete</button>
        </div>
    `);
}

function confirmDeleteLedgerEntry(partyId, ledgerId) {
    let ledger = DB.get('db_party_ledger');
    ledger = ledger.filter(x => String(x.id) !== String(ledgerId));
    DB.set('db_party_ledger', ledger);
    recalculatePartyLedger(partyId);
    closeModal();
    renderPartyLedgerLayout(); // Refresh UI
    showToast('Ledger entry deleted successfully.', 'success');
}

function recalculatePartyLedger(partyId) {
    const parties = DB.get('db_parties');
    const partyIdx = parties.findIndex(p => p.id === partyId);
    if (partyIdx === -1) return;

    let ledger = DB.get('db_party_ledger');

    // Extract this party's ledger lines and sort strictly chronologically ascending
    const partyLines = ledger.filter(l => String(l.partyId) === String(partyId)).sort((a, b) => new Date(a.date) - new Date(b.date) || String(a.id).localeCompare(String(b.id))); // Fallback to id to maintain stable sort

    let runningBalance = 0;
    const updatedIds = [];

    // Recalculate
    partyLines.forEach(line => {
        runningBalance += line.amount;
        line.runningBalance = runningBalance;
        updatedIds.push(line.id);
    });

    // Merge back into main ledger DB using IDs
    partyLines.forEach(line => {
        const globalIdx = ledger.findIndex(x => x.id === line.id);
        if (globalIdx !== -1) ledger[globalIdx] = line;
    });

    DB.set('db_party_ledger', ledger);

    // Update party's total current balance
    parties[partyIdx].balance = runningBalance;
    DB.set('db_parties', parties);
}

// =============================================
//  UOM MASTER TABLE
// =============================================
async function renderUOM() {
    const uoms = await DB.getAll('uom');
    const container = $('inv-setup-content') || pageContent;
    container.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">📏 Unit of Measurement Master</h3>
            <div class="filter-group">
                <button class="btn btn-outline" onclick="triggerUomExcelImport()">📥 Import</button>
                <input type="file" id="f-uom-import" accept=".xlsx, .xls" style="display:none" onchange="importUomExcel(event)">
                <button class="btn btn-primary" onclick="openUOMModal()">+ Add UOM</button>
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Unit Name</th><th>Short Code</th><th>Description</th><th>Actions</th></tr></thead>
                <tbody>${uoms.length ? uoms.map(u => `<tr>
                    <td style="color:var(--text-primary);font-weight:600">${u.name}</td>
                    <td><span class="badge badge-info">${u.code || u.name}</span></td>
                    <td style="font-size:0.85rem;color:var(--text-muted)">${u.description || '-'}</td>
                    <td><div class="action-btns"><button class="btn-icon" onclick="openUOMModal('${u.id}')">✏️</button><button class="btn-icon" onclick="deleteUOM('${u.id}')">🗑️</button></div></td>
                </tr>`).join('') : '<tr><td colspan="4"><div class="empty-state"><div class="empty-icon">📏</div><p>No UOM entries yet. Add units like Pcs, Kg, Box, Ltr, Pack etc.</p></div></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}

async function openUOMModal(id) {
    const uoms = await DB.getAll('uom');
    const u = id ? uoms.find(x => x.id === id) : null;
    openModal(u ? 'Edit UOM' : 'Add UOM', `
        <div class="form-group"><label>Unit Name *</label><input id="f-uom-name" value="${u ? u.name : ''}" placeholder="e.g. Kilogram"></div>
        <div class="form-group"><label>Short Code</label><input id="f-uom-code" value="${u ? u.code || '' : ''}" placeholder="e.g. Kg"></div>
        <div class="form-group"><label>Description</label><input id="f-uom-desc" value="${u ? u.description || '' : ''}" placeholder="Optional description"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        ${!id ? `<button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveUOM('')">＋ Save & New</button>` : ''}
        <button class="btn btn-primary" onclick="saveUOM('${id || ''}')">Save UOM</button></div>`);
}

async function saveUOM(id) {
    const name = $('f-uom-name').value.trim(); if (!name) return alert('Unit name is required');
    const data = { name, code: $('f-uom-code').value.trim() || name, description: $('f-uom-desc').value.trim() };
    
    try {
        if (id) {
            await DB.update('uom', id, data);
        } else {
            await DB.insert('uom', data);
        }
        closeModal();
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderUOM();
        showToast('UOM saved!', 'success');
        if (window._saveAndNew) { window._saveAndNew = false; openUOMModal(); }
    } catch (err) {
        window._saveAndNew = false;
        alert('Error saving UOM: ' + err.message);
    }
}

async function deleteUOM(id) {
    if (!confirm('Delete this UOM?')) return;
    try {
        await DB.delete('uom', id);
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderUOM();
        showToast('UOM deleted!', 'warning');
    } catch (err) {
        alert('Error deleting UOM: ' + err.message);
    }
}

function triggerUomExcelImport() {
    $('f-uom-import').click();
}

async function importUomExcel(e) {
    const file = e.target.files[0];
    if (!file) return;
    try {
        const data = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const workbook = XLSX.read(e.target.result, {type: 'binary'});
                const firstSheet = workbook.SheetNames[0];
                const excelRows = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[firstSheet]);
                resolve(excelRows);
            };
            reader.onerror = reject;
            reader.readAsBinaryString(file);
        });

        if (!data || data.length === 0) return alert('No data found in the Excel file.');
        
        const existingUoms = await DB.getAll('uom');
        let added = 0;
        let updated = 0;

        for (const row of data) {
            const name = (row['Unit Name'] || row['UnitName'] || row['Name'] || '').toString().trim();
            if (!name) continue;

            const code = (row['Short Code'] || row['ShortCode'] || row['Code'] || name).toString().trim();
            const description = (row['Description'] || row['Desc'] || '').toString().trim();

            const existing = existingUoms.find(u => u.name.toLowerCase() === name.toLowerCase());
            if (existing) {
                await DB.update('uom', existing.id, { code, description });
                updated++;
            } else {
                await DB.insert('uom', { name, code, description });
                added++;
            }
        }

        e.target.value = ''; // Reset input
        if ($('inv-setup-content')) await renderInventorySetup(); else await renderUOM();
        showToast(`Import complete! ${added} added, ${updated} updated.`, 'success');

    } catch (err) {
        alert('Error parsing Excel: ' + err.message);
    }
}

// =============================================
//  ITEM CATALOG (Visual product grid for Sales Orders)
// =============================================
let catalogCart = [];
let _catalogAutoSyncInterval = null;

async function renderCatalog() {
    // Setup background interval if not already running
    if (!_catalogAutoSyncInterval) {
        _catalogAutoSyncInterval = setInterval(() => {
            if (currentPage === 'catalog') {
                console.log('Catalog: Auto-syncing stock...');
                syncCatalogData(true); // silent
            }
        }, 60 * 1000);
    }
    const [allItems, categories] = await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('categories')
    ]);
    // Only show active items in catalog
    const items = allItems.filter(i => i.active !== false);
    const catNames = [...new Set(items.map(i => i.category).filter(Boolean))];

    pageContent.innerHTML = `
        <div style="margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;gap:10px">
            <h2 style="font-size:1.2rem;margin:0">📦 Item Catalog</h2>
            <button class="btn btn-outline btn-sm" onclick="syncCatalogData()">🔄 Sync Data</button>
        </div>
        <div style="margin-bottom:16px;display:flex;gap:10px">
            <input class="search-box" id="catalog-search" placeholder="🔍 Search products..." oninput="filterCatalog()" style="flex:1;font-size:1rem;padding:12px 16px;border-radius:12px">
            <select id="catalog-sort" class="search-box" style="width:auto;border-radius:12px" onchange="filterCatalog()">
                <option value="">Sort: Default</option>
                <option value="name-asc">Name: A to Z</option>
                <option value="name-desc">Name: Z to A</option>
                <option value="price-asc">Price: Low to High</option>
                <option value="price-desc">Price: High to Low</option>
            </select>
        </div>
        <div id="catalog-pills" style="display:flex;gap:8px;overflow-x:auto;padding-bottom:8px;margin-bottom:8px;-webkit-overflow-scrolling:touch">
            <button class="catalog-pill active" data-cat="" onclick="filterCatalogByCat('')">All</button>
            ${catNames.map(c => `<button class="catalog-pill" data-cat="${c}" onclick="filterCatalogByCat('${c}')">${c}</button>`).join('')}
        </div>
        <div id="catalog-subcat-pills" style="display:flex;gap:6px;overflow-x:auto;padding-bottom:8px;margin-bottom:8px;-webkit-overflow-scrolling:touch"></div>
        <div id="catalog-movement-pills" style="display:flex;gap:8px;margin-bottom:14px">
            <button class="catalog-pill active" data-movement="" onclick="filterCatalogByMovement('')" style="font-size:0.75rem;padding:5px 12px">📦 All Items</button>
            <button class="catalog-pill" data-movement="slow" onclick="filterCatalogByMovement('slow')" style="font-size:0.75rem;padding:5px 12px;border-color:var(--warning)">🐢 Slow Moving</button>
            <button class="catalog-pill" data-movement="non" onclick="filterCatalogByMovement('non')" style="font-size:0.75rem;padding:5px 12px;border-color:var(--danger)">⛔ Non-Moving (10d)</button>
        </div>
        <div id="catalog-grid" class="catalog-grid">
            ${await renderCatalogCards(items)}
        </div>
        ${catalogCart.length ? renderCatalogCartBar() : ''}`;
}

async function syncCatalogData(silent = false) {
    if (!silent) showToast('Syncing latest stock...', 'info');
    await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('salesorders'),
        DB.getAll('parties')
    ]);
    if (currentPage === 'catalog') renderCatalog();
    if (!silent) showToast('Stock sync complete!', 'success');
}

async function renderCatalogCards(items) {
    if (!items.length) return '<div class="empty-state" style="padding:40px"><div class="empty-icon">📦</div><p>No products found</p></div>';
    
    // Process all cards in parallel
    const cards = await Promise.all(items.map(async i => {
        const stockData = await getAvailableStock(i);
        const cartEntries = catalogCart.filter(c => c.itemId === i.id);
        const isLow = i.stock <= (i.lowStockAlert || 5);
        return `<div class="catalog-card">
            <div class="catalog-card-img" onclick="viewCatalogItem('${i.id}')">
                ${(i.imageUrl || i.photo) ? `<img src="${i.imageUrl || i.photo}" alt="${i.name}">` : `<div class="catalog-card-placeholder">${i.name.charAt(0).toUpperCase()}</div>`}
            </div>
            <div class="catalog-card-body">
                <div class="catalog-card-name">${i.name}</div>
                <div class="catalog-card-meta">
                    ${i.category ? `<span class="badge badge-info" style="font-size:0.65rem">${i.category}</span>` : ''}
                    ${i.subCategory ? `<span style="font-size:0.7rem;color:var(--text-muted)"> ${i.subCategory}</span>` : ''}
                </div>
                <div style="display:flex;gap:4px;flex-wrap:wrap;margin:4px 0">
                    <span class="catalog-uom-badge">${i.unit || 'Pcs'}</span>
                    ${i.secUom ? `<span class="catalog-uom-badge">${i.secUom}${i.secUomRatio ? ' (' + i.secUomRatio + ')' : ''}</span>` : ''}
                </div>
                <div class="catalog-card-price">₹${i.salePrice} <span style="font-size:0.75rem;color:var(--text-muted)">/ ${i.unit || 'Pcs'}</span></div>
                ${i.mrp ? `<div style="font-size:0.72rem;color:var(--text-muted)">MRP: <span style="text-decoration:none">₹${i.mrp}</span></div>` : ''}
                <div style="background:var(--bg-body);padding:8px;border-radius:8px;margin-top:8px;border:1px solid var(--border)">
                    <div style="display:flex;justify-content:space-between;font-size:0.7rem;color:var(--text-secondary);margin-bottom:2px">
                        <span>Stock:</span>
                        <span>${stockData.stock} ${i.unit || 'Pcs'}</span>
                    </div>
                    <div onclick="showReservedDetails('${i.id}')" style="display:flex;justify-content:space-between;font-size:0.7rem;color:var(--warning);margin-bottom:4px;cursor:pointer">
                        <span style="display:flex;align-items:center;gap:2px">ℹ️ Reserved:</span>
                        <span style="font-weight:600">${stockData.reserved} ${i.unit || 'Pcs'}</span>
                    </div>
                    <div style="display:flex;justify-content:space-between;font-size:0.85rem;color:${isLow ? 'var(--danger)' : 'var(--success)'};font-weight:700;border-top:1px dashed var(--border);padding-top:4px">
                        <span>Available:</span>
                        <span>${stockData.available} ${i.unit || 'Pcs'}</span>
                    </div>
                </div>
            </div>
            <div class="catalog-card-action">
                ${cartEntries.length ? `<div style="width:100%">
                    ${cartEntries.map(ce => `<div style="display:flex;align-items:center;justify-content:center;gap:6px;${cartEntries.length > 1 ? 'margin-bottom:4px' : ''}">
                        <button class="catalog-qty-btn" onclick="updateCartQty('${i.id}',-1,'${ce.unit}')">−</button>
                        <span style="font-weight:700;min-width:24px;text-align:center">${ce.qty}</span>
                        <button class="catalog-qty-btn" onclick="updateCartQty('${i.id}',1,'${ce.unit}')">+</button>
                        <span style="font-size:0.72rem;color:var(--text-muted);min-width:28px">${ce.unit}</span>
                    </div>`).join('')}
                    ${i.secUom ? `<button class="catalog-add-btn" onclick="addToCatalogCart('${i.id}')" style="margin-top:4px;padding:4px 12px;font-size:0.75rem">+ More</button>` : ''}
                </div>` : `<button class="catalog-add-btn" onclick="addToCatalogCart('${i.id}')">+ Add</button>`}
            </div>
        </div>`;
    }));
    return cards.join('');
}

function renderCatalogCartBar() {
    const totalItems = catalogCart.reduce((s, c) => s + c.qty, 0);
    const totalAmt = catalogCart.reduce((s, c) => s + c.qty * c.price, 0);
    return `<div id="catalog-cart-bar" class="catalog-cart-bar">
        <div onclick="openCatalogCart()" style="cursor:pointer;display:flex;align-items:center;gap:10px;flex:1">
            <span style="font-size:1.3rem">🛒</span>
            <span><strong>${totalItems} item${totalItems > 1 ? 's' : ''}</strong> • ${currency(totalAmt)}</span>
        </div>
        <button class="btn btn-primary btn-sm" onclick="createOrderFromCatalog()" style="border-radius:8px;font-weight:600">Create Order →</button>
    </div>`;
}

async function filterCatalog() {
    const s = ($('catalog-search') ? $('catalog-search').value : '').toLowerCase();
    const activeCatPill = document.querySelector('#catalog-pills .catalog-pill.active');
    const cat = activeCatPill ? activeCatPill.dataset.cat || '' : '';
    const activeSubPill = document.querySelector('#catalog-subcat-pills .catalog-pill.active');
    const subCat = activeSubPill ? activeSubPill.dataset.subcat || '' : '';
    const activeMovPill = document.querySelector('#catalog-movement-pills .catalog-pill.active');
    const movement = activeMovPill ? activeMovPill.dataset.movement || '' : '';
    const allItems = await DB.getAll('inventory');
    let items = allItems.filter(i => i.active !== false); // Hide deactivated
    if (cat) items = items.filter(i => i.category === cat);
    if (subCat) items = items.filter(i => (i.subCategory || '') === subCat);
    if (s) items = items.filter(i => i.name.toLowerCase().includes(s) || (i.itemCode || '').toLowerCase().includes(s) || (i.category || '').toLowerCase().includes(s) || (i.subCategory || '').toLowerCase().includes(s));
    if (movement) {
        const movementMap = await getItemMovementMap();
        items = items.filter(i => movementMap[i.id] === movement);
    }

    const sortBy = $('catalog-sort') ? $('catalog-sort').value : '';
    if (sortBy === 'name-asc') items.sort((a, b) => a.name.localeCompare(b.name));
    else if (sortBy === 'name-desc') items.sort((a, b) => b.name.localeCompare(a.name));
    else if (sortBy === 'price-asc') items.sort((a, b) => a.salePrice - b.salePrice);
    else if (sortBy === 'price-desc') items.sort((a, b) => b.salePrice - a.salePrice);

    const grid = $('catalog-grid');
    if (grid) grid.innerHTML = await renderCatalogCards(items);
}

async function getItemMovementMap() {
    const [inventory, invoices] = await Promise.all([
        DB.getAll('inventory'),
        DB.getAll('invoices')
    ]);
    const now = new Date();
    const d10 = new Date(now); d10.setDate(d10.getDate() - 10);
    const d90 = new Date(now); d90.setDate(d90.getDate() - 90);
    const d10Str = d10.toISOString().split('T')[0];
    const d90Str = d90.toISOString().split('T')[0];

    // Build sales quantity map per item for last 10d and 90d
    const sales10 = {}, sales90 = {};
    invoices.filter(inv => inv.type === 'sale' && inv.status !== 'cancelled').forEach(inv => {
        const invDate = inv.date || '';
        (inv.items || []).forEach(li => {
            const qty = li.packedQty || li.qty || 0;
            if (invDate >= d90Str) { sales90[li.itemId] = (sales90[li.itemId] || 0) + qty; }
            if (invDate >= d10Str) { sales10[li.itemId] = (sales10[li.itemId] || 0) + qty; }
        });
    });

    const map = {};
    inventory.forEach(i => {
        if (i.stock <= 0) { map[i.id] = 'ok'; return; }
        if (!sales10[i.id]) { map[i.id] = 'non'; return; } // Non-moving: no sales in 10 days
        if ((sales90[i.id] || 0) <= 5) { map[i.id] = 'slow'; return; } // Slow: <=5 units in 90 days
        map[i.id] = 'ok';
    });
    return map;
}

function filterCatalogByMovement(movement) {
    document.querySelectorAll('#catalog-movement-pills .catalog-pill').forEach(p => {
        p.classList.toggle('active', (p.dataset.movement || '') === movement);
    });
    filterCatalog();
}

// Lightweight refresh: only update grid + cart bar (preserves search, filters, scroll)
async function refreshCatalogGrid() {
    await filterCatalog();
    // Update or add/remove cart bar
    const existingBar = document.getElementById('catalog-cart-bar');
    if (catalogCart.length) {
        const barHtml = renderCatalogCartBar();
        if (existingBar) {
            existingBar.outerHTML = barHtml;
        } else {
            pageContent.insertAdjacentHTML('beforeend', barHtml);
        }
    } else if (existingBar) {
        existingBar.remove();
    }
}

function filterCatalogByCat(cat) {
    // Update category pill active state
    document.querySelectorAll('#catalog-pills .catalog-pill').forEach(p => {
        p.classList.toggle('active', (p.dataset.cat || '') === cat);
    });

    // Build sub-category pills for selected category
    const subPillsContainer = $('catalog-subcat-pills');
    if (subPillsContainer) {
        if (cat) {
            const catObj = (DB.get('db_categories') || []).find(c => c.name === cat);
            const subCats = (catObj && catObj.subCategories) ? catObj.subCategories : [];
            if (subCats.length) {
                subPillsContainer.innerHTML = `<button class="catalog-pill active" data-subcat="" onclick="filterCatalogBySubcat('All')" style="font-size:0.75rem;padding:5px 12px">All ${cat}</button>` +
                    subCats.map(sc => `<button class="catalog-pill" data-subcat="${sc}" onclick="filterCatalogBySubcat('${sc}')" style="font-size:0.75rem;padding:5px 12px">${sc}</button>`).join('');
            } else {
                subPillsContainer.innerHTML = '';
            }
        } else {
            subPillsContainer.innerHTML = '';
        }
    }
    filterCatalog();
}

function filterCatalogBySubcat(subcat) {
    document.querySelectorAll('#catalog-subcat-pills .catalog-pill').forEach(p => {
        const psc = p.dataset.subcat || '';
        p.classList.toggle('active', subcat === 'All' ? psc === '' : psc === subcat);
    });
    filterCatalog();
}

async function addToCatalogCart(itemId, uom) {
    const items = await DB.getAll('inventory');
    const item = items.find(x => x.id === itemId);
    if (!item) return;

    // If item has secondary UOM and no UOM specified, show picker
    if (!uom && item.secUom) {
        const priUnit = item.unit || 'Pcs';
        const secUom = item.secUom;
        const secRatio = +(item.secUomRatio) || 0;
        const secPrice = secRatio > 0 ? (item.salePrice / secRatio).toFixed(2) : item.salePrice;
        openModal('Select Unit', `
            <div style="text-align:center;margin-bottom:16px">
                ${item.photo ? `<img src="${item.photo}" style="max-height:80px;border-radius:10px;object-fit:cover">` : `<div style="width:60px;height:60px;background:linear-gradient(135deg,var(--primary),var(--accent));border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.5rem;color:#fff;margin:0 auto">${item.name.charAt(0)}</div>`}
                <div style="font-weight:700;margin-top:8px">${item.name}</div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
                <button class="btn btn-outline" style="padding:16px;border-radius:12px;display:flex;flex-direction:column;align-items:center;gap:6px" onclick="closeModal();addToCatalogCart('${itemId}','${priUnit}')">
                    <span style="font-size:1.3rem">📦</span>
                    <span style="font-weight:700">${priUnit}</span>
                    <span style="font-size:0.85rem;color:var(--accent)">₹${item.salePrice}</span>
                </button>
                <button class="btn btn-outline" style="padding:16px;border-radius:12px;display:flex;flex-direction:column;align-items:center;gap:6px" onclick="closeModal();addToCatalogCart('${itemId}','${secUom}')">
                    <span style="font-size:1.3rem">📋</span>
                    <span style="font-weight:700">${secUom}</span>
                    <span style="font-size:0.85rem;color:var(--accent)">₹${secPrice}${secRatio > 0 ? ` (1 ${priUnit} = ${secRatio} ${secUom})` : ''}</span>
                </button>
            </div>
            <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button></div>`);
        return;
    }

    const selectedUom = uom || item.unit || 'Pcs';
    const priUnit = item.unit || 'Pcs';
    const secRatio = +(item.secUomRatio) || 0;
    let price = item.salePrice;
    if (selectedUom !== priUnit && item.secUom && selectedUom === item.secUom && secRatio > 0) {
        price = item.salePrice / secRatio;
    }

    // Check if same item with same UOM already in cart
    const exists = catalogCart.find(c => c.itemId === itemId && c.unit === selectedUom);
    if (exists) { exists.qty++; exists.amount = exists.qty * exists.price; }
    else catalogCart.push({ itemId: item.id, name: item.name, qty: 1, price: +price.toFixed(2), unit: selectedUom, amount: +price.toFixed(2) });
    await refreshCatalogGrid();
}

function updateCartQty(itemId, delta, unit) {
    const ci = catalogCart.find(c => c.itemId === itemId && (!unit || c.unit === unit));
    if (!ci) return;
    ci.qty += delta;
    ci.amount = ci.qty * ci.price;
    if (ci.qty <= 0) catalogCart = catalogCart.filter(c => !(c.itemId === itemId && c.unit === ci.unit));
    refreshCatalogGrid();
}

async function showReservedDetails(itemId) {
    const items = await DB.get('db_inventory') || [];
    const item = items.find(x => x.id === itemId);
    if (!item) return;

    const orders = await DB.getAll('salesorders');
    const reservedOrders = orders.filter(o => {
        if ((o.status === 'pending' || o.status === 'approved') && !o.packed) {
            return (o.items || []).some(li => li.itemId === itemId);
        }
        return false;
    });

    if (!reservedOrders.length) return alert('No active reservations found for this item.');

    const rows = reservedOrders.map(o => {
        const li = o.items.find(x => x.itemId === itemId);
        return `<tr style="font-size:0.85rem">
            <td style="font-weight:600;width:90px;padding-right:10px">${o.orderNo}</td>
            <td style="min-width:140px;padding-right:10px">${o.partyName}</td>
            <td style="font-weight:700;color:var(--primary);width:60px;text-align:center">${li ? li.qty : 0} ${li ? li.unit : ''}</td>
            <td style="width:90px;text-align:center">${fmtDate(o.date)}</td>
            <td style="width:80px;text-align:right"><span class="badge ${o.status === 'approved' ? 'badge-success' : 'badge-warning'}" style="white-space:nowrap;display:inline-block">${o.status}</span></td>
        </tr>`;
    }).join('');

    openModal(`Reservations for: ${item.name}`, `
        <div style="margin-bottom:12px;font-size:0.9rem;color:var(--text-secondary)">Total Reserved: <strong>${getAvailableStock(item).reserved} ${item.unit || 'Pcs'}</strong></div>
        <div class="table-wrapper" style="overflow-x:auto">
            <table class="data-table" style="min-width:500px">
                <thead><tr>
                    <th style="text-align:left;width:90px">Order #</th>
                    <th style="text-align:left">Customer</th>
                    <th style="text-align:center;width:60px">Qty</th>
                    <th style="text-align:center;width:90px">Date</th>
                    <th style="text-align:right;width:80px">Status</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
        <div class="modal-actions"><button class="btn btn-primary" onclick="closeModal()">Done</button></div>
    `);
}

async function viewCatalogItem(itemId) {
    const items = await DB.getAll('inventory');
    const i = items.find(x => x.id === itemId);
    if (!i) return;
    const stockData = await getAvailableStock(i);
    openModal(i.name, `
        <div style="text-align:center;margin-bottom:16px">
            ${i.photo ? `<img src="${i.photo}" style="max-height:150px;max-width:100%;border-radius:12px;object-fit:cover">` : `<div style="width:100px;height:100px;background:linear-gradient(135deg,var(--primary),var(--accent));border-radius:16px;display:flex;align-items:center;justify-content:center;font-size:2.5rem;color:#fff;margin:0 auto">${i.name.charAt(0)}</div>`}
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">
            <div style="padding:10px;background:var(--bg-body);border-radius:8px"><div style="font-size:0.75rem;color:var(--text-muted)">Category</div><div style="font-weight:600">${i.category || '-'}${i.subCategory ? ' / ' + i.subCategory : ''}</div></div>
            <div style="padding:10px;background:var(--bg-body);border-radius:8px"><div style="font-size:0.75rem;color:var(--text-muted)">Unit</div><div style="font-weight:600">${i.unit || 'Pcs'}${i.secUom ? ' / ' + i.secUom : ''}</div></div>
            <div style="padding:10px;background:var(--bg-body);border-radius:8px"><div style="font-size:0.75rem;color:var(--text-muted)">Sale Price</div><div style="font-weight:700;color:var(--accent)">₹${i.salePrice}</div></div>
            <div style="padding:10px;background:var(--bg-body);border-radius:8px"><div style="font-size:0.75rem;color:var(--text-muted)">Stock</div><div style="font-weight:600;color:${stockData.available <= (i.lowStockAlert || 5) ? 'var(--danger)' : 'var(--success)'}">${stockData.available} ${i.unit || 'Pcs'}</div></div>
        </div>
        ${i.itemCode ? `<div style="font-size:0.85rem;color:var(--text-muted);margin-bottom:8px">Code: <strong>${i.itemCode}</strong></div>` : ''}
        ${i.hsn ? `<div style="font-size:0.85rem;color:var(--text-muted);margin-bottom:8px">HSN: <strong>${i.hsn}</strong></div>` : ''}
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Close</button>
        <button class="btn btn-primary" onclick="closeModal();addToCatalogCart('${i.id}')">+ Add to Cart</button></div>`);
}

async function openCatalogCart() {
    if (!catalogCart.length) return;
    const total = catalogCart.reduce((s, c) => s + c.qty * c.price, 0);
    openModal('🛒 Cart', `
        <div style="max-height:350px;overflow-y:auto">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Item</th><th>Qty</th><th>Unit</th><th>Price</th><th>Amount</th><th></th></tr></thead>
                <tbody>${catalogCart.map((c, idx) => `<tr>
                    <td style="font-weight:600">${c.name}</td>
                    <td><div style="display:flex;align-items:center;gap:4px">
                        <button class="catalog-qty-btn" onclick="updateCartQtyInModal(${idx},-1)">−</button>
                        <span style="font-weight:700">${c.qty}</span>
                        <button class="catalog-qty-btn" onclick="updateCartQtyInModal(${idx},1)">+</button>
                    </div></td>
                    <td><span class="catalog-uom-badge">${c.unit}</span></td>
                    <td>₹${c.price}</td>
                    <td class="amount-green" style="font-weight:600">${currency(c.qty * c.price)}</td>
                    <td><button class="btn-icon" onclick="removeFromCartByIdx(${idx})" style="color:var(--danger)">🗑️</button></td>
                </tr>`).join('')}</tbody></table>
            </div>
        </div>
        <div style="text-align:right;font-size:1.2rem;font-weight:700;color:var(--accent);margin-top:12px">Total: ${currency(total)}</div>
        <div class="modal-actions">
            <button class="btn btn-outline" onclick="catalogCart=[];closeModal();renderCatalog()">Clear Cart</button>
            <button class="btn btn-outline" onclick="closeModal()">Continue Shopping</button>
            <button class="btn btn-primary" onclick="closeModal();createOrderFromCatalog()">Create Order →</button>
        </div>`);
}

async function updateCartQtyInModal(idx, delta) {
    if (catalogCart[idx]) {
        catalogCart[idx].qty += delta;
        catalogCart[idx].amount = catalogCart[idx].qty * catalogCart[idx].price;
        if (catalogCart[idx].qty <= 0) catalogCart.splice(idx, 1);
    }
    if (catalogCart.length) await openCatalogCart();
    else { closeModal(); await renderCatalog(); }
}

async function removeFromCartByIdx(idx) {
    catalogCart.splice(idx, 1);
    if (catalogCart.length) await openCatalogCart();
    else { closeModal(); await renderCatalog(); }
}

async function createOrderFromCatalog() {
    if (!catalogCart.length) return alert('Cart is empty');
    const inv = DB.get('db_inventory');

    // Build soItems using the same logic as addSOLine
    soItems = [];
    catalogCart.forEach(c => {
        const itemObj = inv.find(x => x.id === c.itemId);
        if (!itemObj) return;

        const primaryUnit = itemObj.unit || 'Pcs';
        const secUom = itemObj.secUom || '';
        const secRatio = +(itemObj.secUomRatio) || 0;
        const qty = c.qty;
        const unit = c.unit || primaryUnit;

        // Calculate primaryQty for stock check
        let primaryQty = qty;
        if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
            primaryQty = qty / secRatio;
        }

        // Volume pricing check (based on primary qty)
        let baseListedPrice = +(itemObj.salePrice || 0);
        if (itemObj.priceTiers && itemObj.priceTiers.length) {
            for (const t of itemObj.priceTiers) {
                if (primaryQty >= t.minQty) {
                    baseListedPrice = t.price;
                    break;
                }
            }
        }

        // Adjust price for secondary UOM
        let unitPrice = baseListedPrice;
        if (unit !== primaryUnit && secUom && unit === secUom && secRatio > 0) {
            unitPrice = baseListedPrice / secRatio;
        }

        soItems.push({
            itemId: itemObj.id,
            name: itemObj.name,
            qty: qty,
            price: +unitPrice.toFixed(2),
            listedPrice: +unitPrice.toFixed(2),
            discountAmt: 0,
            discountPct: 0,
            amount: +(qty * unitPrice).toFixed(2),
            unit: unit,
            primaryQty: primaryQty
        });
    });

    // Flag so saveSalesOrder knows to clear cart + go back to catalog
    window._catalogOrderMode = true;

    // Open the standard SO modal
    const allParties = await DB.getAll('parties');
    const customers = allParties.filter(p => p.type === 'Customer');
    const categories = await DB.getAll('categories');
    const orderNo = await nextNumber('SO-');

    openModal('Create Sales Order (from Catalog)', `
        <div class="form-row"><div class="form-group"><label>Order #</label><input id="f-so-no" value="${orderNo}" readonly></div><div class="form-group"><label>Date</label><input type="date" id="f-so-date" value="${today()}"></div></div>
        <div class="form-row"><div class="form-group"><label>Expected Delivery</label><input type="date" id="f-so-delivery" value=""></div><div class="form-group"><label>Priority</label><select id="f-so-priority"><option value="Normal">Normal</option><option value="Urgent">🔥 Urgent</option></select></div></div>
        <div class="form-group"><label>Customer * <small style="color:var(--text-muted)">(new name = auto-created)</small></label>
            <input id="f-so-party" placeholder="Type customer name or mobile...">
        </div>
        
        <hr style="border-color:var(--border);margin:16px 0"><h4 style="margin-bottom:10px;font-size:0.9rem">Items</h4>
        
        <div class="form-row" style="margin-bottom:8px">
            <div class="form-group">
                <label>Category Filter</label>
                <select id="f-so-cat-filter" onchange="onSOCatFilterChange()">
                    <option value="">All Categories</option>
                    ${categories.map(c => `<option value="${c.name}">${c.name}</option>`).join('')}
                </select>
            </div>
            <div class="form-group">
                <label>Sub-Category Filter</label>
                <select id="f-so-subcat-filter" onchange="onSOSubcatFilterChange()">
                    <option value="">All Sub-Categories</option>
                </select>
            </div>
        </div>

        <div class="form-row-3" style="margin-bottom:8px">
            <div class="form-group">
                <label>Item</label>
                <input id="f-so-item-input" placeholder="Type item name or code...">
            </div>
            <div class="form-group"><label>Qty</label><input type="number" id="f-so-qty" value="1" min="1"></div>
            <div class="form-group"><label>UOM</label><select id="f-so-uom" onchange="onSOUomChange()"><option value="">--</option></select></div>
            <div class="form-group"><label>Price ₹</label><input type="number" id="f-so-price" value="" min="0" step="0.01" placeholder="Listed"></div>
            <div class="form-group"><label>&nbsp;</label><button class="btn btn-primary btn-block" onclick="addSOLine()">Add</button></div>
        </div>
        
        <div class="table-wrapper"><div id="so-lines-list"></div></div>
        <div style="text-align:right;font-size:1.1rem;font-weight:700;color:var(--accent)" id="so-total-display">Total: ${currency(soItems.reduce((s, li) => s + li.amount, 0))}</div>
        
        <div class="form-group" style="margin-top:12px"><label>Notes</label><input id="f-so-notes" placeholder="Instructions..."></div>
    `, `<button class="btn btn-outline" onclick="closeModal()">Cancel</button><button class="btn btn-outline btn-save-new" onclick="window._saveAndNew=true;saveSalesOrder()">＋ Save & New</button><button class="btn btn-primary" onclick="saveSalesOrder()">✅ Submit Order</button>`);

    // Initialize party search and item list search
    initSearchDropdown('f-so-party', buildPartySearchList(customers));
    _soItemDropdown = initSearchDropdown('f-so-item-input', buildItemSearchList(inv), function (item) {
        $('f-so-price').value = item.salePrice || '';
        var uomSel = $('f-so-uom');
        if (uomSel) {
            uomSel.innerHTML = '<option value="' + item.unit + '">' + item.unit + '</option>';
            if (item.secUom) uomSel.innerHTML += '<option value="' + item.secUom + '">' + item.secUom + '</option>';
        }
    });

    // Render pre-filled lines from cart
    renderSOLines();
}

// =============================================
//  INVENTORY SETUP (Tabbed: Categories, UOM, Brands, Tax)
// =============================================
let invSetupTab = 'categories';

async function renderInventorySetup() {
    pageContent.innerHTML = `
        <div style="display:flex;gap:8px;margin-bottom:20px;overflow-x:auto;padding-bottom:4px">
            <button class="catalog-pill ${invSetupTab === 'categories' ? 'active' : ''}" onclick="invSetupTab='categories';renderInventorySetup()">🏷️ Categories</button>
            <button class="catalog-pill ${invSetupTab === 'uom' ? 'active' : ''}" onclick="invSetupTab='uom';renderInventorySetup()">📏 UOM</button>
            <button class="catalog-pill ${invSetupTab === 'brands' ? 'active' : ''}" onclick="invSetupTab='brands';renderInventorySetup()">🏭 Brands</button>
            <button class="catalog-pill ${invSetupTab === 'tax' ? 'active' : ''}" onclick="invSetupTab='tax';renderInventorySetup()">💹 Tax / GST</button>
        </div>
        <div id="inv-setup-content"></div>`;

    if (invSetupTab === 'categories') await renderCategories();
    if (invSetupTab === 'uom') await renderUOM();
    if (invSetupTab === 'brands') await renderBrands();
    if (invSetupTab === 'tax') await renderTaxSetup();
}

// --- Brands Master ---
async function renderBrands() {
    const brands = await DB.getAll('brands');
    const el = $('inv-setup-content') || pageContent;
    el.innerHTML = `
        <div class="section-toolbar">
            <h3 style="font-size:1rem">🏭 Brand Master</h3>
            <div class="filter-group">
                <button class="btn btn-outline" onclick="triggerBrandExcelImport()">📥 Import</button>
                <input type="file" id="f-brand-import" accept=".xlsx, .xls" style="display:none" onchange="importBrandsExcel(event)">
                <button class="btn btn-primary" onclick="openBrandModal()">+ Add Brand</button>
            </div>
        </div>
        <div class="card"><div class="card-body">
            <div class="table-wrapper">
                <table class="data-table"><thead><tr><th>Brand Name</th><th>Description</th><th>Actions</th></tr></thead>
                <tbody>${brands.length ? brands.map(b => `<tr>
                    <td style="color:var(--text-primary);font-weight:600">${b.name}</td>
                    <td style="font-size:0.85rem;color:var(--text-muted)">${b.description || '-'}</td>
                    <td><div class="action-btns"><button class="btn-icon" onclick="openBrandModal('${b.id}')">✏️</button><button class="btn-icon" onclick="deleteBrand('${b.id}')">🗑️</button></div></td>
                </tr>`).join('') : '<tr><td colspan="3"><div class="empty-state"><div class="empty-icon">🏭</div><p>No brands yet. Add brands to categorize products by manufacturer.</p></div></td></tr>'}</tbody></table>
            </div>
        </div></div>`;
}

async function openBrandModal(id) {
    const brands = await DB.getAll('brands');
    const b = id ? brands.find(x => x.id === id) : null;
    openModal(b ? 'Edit Brand' : 'Add Brand', `
        <div class="form-group"><label>Brand Name *</label><input id="f-brand-name" value="${b ? b.name : ''}" placeholder="e.g. Coca-Cola, Nestlé"></div>
        <div class="form-group"><label>Description</label><input id="f-brand-desc" value="${b ? b.description || '' : ''}" placeholder="Optional"></div>
        <div class="modal-actions"><button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="saveBrand('${id || ''}')">Save</button></div>`);
}

async function saveBrand(id) {
    const name = $('f-brand-name').value.trim(); if (!name) return alert('Brand name required');
    const description = $('f-brand-desc').value.trim();
    
    try {
        if (id) {
            await DB.update('brands', id, { name, description });
        } else {
            await DB.insert('brands', { name, description });
        }
        closeModal();
        await renderInventorySetup();
        showToast('Brand saved!', 'success');
    } catch (err) {
        alert('Error saving brand: ' + err.message);
    }
}

async function deleteBrand(id) {
    if (!confirm('Delete brand?')) return;
    try {
        await DB.delete('brands', id);
        await renderInventorySetup();
        showToast('Brand deleted!', 'warning');
    } catch (err) {
        alert('Error deleting brand: ' + err.message);
    }
}

function triggerBrandExcelImport() {
    $('f-brand-import').click();
}

async function importBrandsExcel(e) {
    const file = e.target.files[0];
    if (!file) return;
    try {
        const data = await new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const workbook = XLSX.read(e.target.result, {type: 'binary'});
                const firstSheet = workbook.SheetNames[0];
                const excelRows = XLSX.utils.sheet_to_row_object_array(workbook.Sheets[firstSheet]);
                resolve(excelRows);
            };
            reader.onerror = reject;
            reader.readAsBinaryString(file);
        });

        if (!data || data.length === 0) return alert('No data found in the Excel file.');
        
        const existingBrands = await DB.getAll('brands');
        let added = 0;
        let updated = 0;

        for (const row of data) {
            const name = (row['Brand Name'] || row['BrandName'] || row['Name'] || '').toString().trim();
            if (!name) continue;

            const description = (row['Description'] || row['Desc'] || row['Manufacturer'] || '').toString().trim();

            const existing = existingBrands.find(b => b.name.toLowerCase() === name.toLowerCase());
            if (existing) {
                await DB.update('brands', existing.id, { description });
                updated++;
            } else {
                await DB.insert('brands', { name, description });
                added++;
            }
        }

        e.target.value = ''; // Reset input
        await renderInventorySetup();
        showToast(`Import complete! ${added} added, ${updated} updated.`, 'success');

    } catch (err) {
        alert('Error parsing Excel: ' + err.message);
    }
}

// --- Tax / GST Setup ---
async function renderTaxSetup() {
    const tax = DB.ls.getObj('db_tax_settings') || { gstEnabled: false, defaultGST: 18, gstSlabs: [0, 5, 12, 18, 28] };
    const el = $('inv-setup-content') || pageContent;
    el.innerHTML = `
        <div class="card"><div class="card-body padded">
            <h3 style="font-size:1rem;margin-bottom:16px">💹 Tax / GST Configuration</h3>
            <div class="form-group">
                <label style="display:flex;align-items:center;gap:10px;cursor:pointer">
                    <input type="checkbox" id="f-tax-gst-enabled" ${tax.gstEnabled ? 'checked' : ''} style="width:18px;height:18px;accent-color:var(--accent)">
                    <span style="font-size:0.95rem;text-transform:none;letter-spacing:0">Enable GST on invoices</span>
                </label>
            </div>
            <div class="form-row">
                <div class="form-group"><label>Default GST Rate (%)</label><input type="number" id="f-tax-default-gst" value="${tax.defaultGST || 18}" min="0" max="100" step="0.5"></div>
                <div class="form-group"><label>Available GST Slabs</label><input id="f-tax-slabs" value="${(tax.gstSlabs || [0, 5, 12, 18, 28]).join(', ')}" placeholder="0, 5, 12, 18, 28"></div>
            </div>
            <div class="form-group"><label>GSTIN (Company)</label><input id="f-tax-gstin" value="${tax.gstin || ''}" placeholder="e.g. 29ABCDE1234F1Z5"></div>
            <div class="modal-actions" style="margin-top:12px">
                <button class="btn btn-primary" onclick="saveTaxSetup()">Save Tax Settings</button>
            </div>
        </div></div>`;
}

async function saveTaxSetup() {
    const data = {
        gstEnabled: $('f-tax-gst-enabled').checked,
        defaultGST: +$('f-tax-default-gst').value || 18,
        gstSlabs: $('f-tax-slabs').value.split(',').map(s => +s.trim()).filter(n => !isNaN(n)),
        gstin: $('f-tax-gstin').value.trim()
    };
    try {
        await DB.saveSettings('db_tax_settings', data);
        showToast('Tax settings saved!', 'success');
    } catch (err) {
        alert('Error saving tax settings: ' + err.message);
    }
}

// ============================================================
// CUSTOMER PORTAL
// ============================================================

let cpSession = null; // { phone, partyId, partyName, allowedCategories, paymentTerms, creditLimit, balance }
let cpCart = {}; // { itemId: qty }

function cpRestoreSession() {
    try {
        const saved = localStorage.getItem('cp_session');
        if (!saved) return false;
        cpSession = JSON.parse(saved);
        // Validate session has required fields — clear stale/invalid sessions
        if (!cpSession || !cpSession.phone || !cpSession.partyId) {
            cpSession = null;
            localStorage.removeItem('cp_session');
            return false;
        }
        // Show portal immediately
        const root = document.getElementById('cp-root');
        if (!root) { cpSession = null; localStorage.removeItem('cp_session'); return false; }
        root.style.display = 'block';
        document.getElementById('login-screen') && (document.getElementById('login-screen').classList.add('hidden'));
        document.getElementById('app') && (document.getElementById('app').classList.add('hidden'));
        DB.refresh().then(() => cpRenderHome()).catch(() => { cpLogout(); location.reload(); });
        return true;
    } catch(e) { localStorage.removeItem('cp_session'); return false; }
}

function showCustomerPortal() {
    const root = document.getElementById('cp-root');
    root.style.display = 'block';
    document.getElementById('login-screen').classList.add('hidden');
    cpRenderAuth();
}

function cpExitPortal() {
    const root = document.getElementById('cp-root');
    root.style.display = 'none';
    root.innerHTML = '';
    document.getElementById('login-screen').classList.remove('hidden');
}

function cpLogout() {
    cpSession = null;
    cpCart = {};
    localStorage.removeItem('cp_session');
    cpExitPortal();
}

function cpSaveSession() {
    localStorage.setItem('cp_session', JSON.stringify(cpSession));
}

function cpShell(content, showBack, backFn) {
    window._cpBackFn = backFn || null;
    const company = DB.ls.getObj('db_company').name || 'DistroManager';
    const root = document.getElementById('cp-root');
    root.innerHTML = `
    <div class="cp-shell">
        <div class="cp-header">
            ${showBack ? `<button class="cp-hbtn" onclick="window._cpBackFn && window._cpBackFn()">&#8592;</button>` : `<div style="width:36px"></div>`}
            <span class="cp-title">${company}</span>
            ${cpSession ? `<button class="cp-hbtn" onclick="cpLogout()" title="Logout">&#x23FB;</button>` : `<button class="cp-hbtn" onclick="cpExitPortal()">&#x2715;</button>`}
        </div>
        <div class="cp-body">${content}</div>
    </div>`;
}

// ---- AUTH ----
function cpRenderAuth() {
    cpShell(`
    <div class="cp-card" style="margin-top:32px">
        <h2 style="margin:0 0 4px;font-size:1.3rem">Customer Login</h2>
        <p style="color:var(--text-muted);font-size:0.85rem;margin:0 0 20px">Enter your phone number to continue</p>
        <div class="form-group">
            <label>Phone Number</label>
            <input id="cp-phone" type="tel" class="form-control" placeholder="10-digit mobile number" maxlength="15">
        </div>
        <button class="btn btn-primary btn-block" onclick="cpInitAuth(false)">Send OTP</button>
        <div style="margin-top:16px;padding-top:16px;border-top:1px solid var(--border);text-align:center">
            <p style="font-size:0.8rem;color:var(--text-muted);margin-bottom:8px">New customer?</p>
            <button class="btn btn-outline btn-block" onclick="cpRenderRegister()">Register Your Business</button>
        </div>
    </div>`, false, null);
}

async function cpInitAuth(forRegister, regData) {
    const phone = (document.getElementById('cp-phone') ? document.getElementById('cp-phone').value : (regData && regData.phone) || '').trim();
    if (!phone || phone.length < 10) { alert('Please enter a valid phone number'); return; }

    if (!forRegister) {
        // Check for duplicate: already logged in elsewhere or phone not registered
        await DB.refreshTables(['parties']);
        const parties = DB.get('db_parties') || [];
        const approved = parties.find(p => p.portalPhone === phone && p.portalEnabled);
        if (!approved) {
            // Check if pending registration exists
            const { data: regs } = await supabaseClient.from('customer_registrations')
                .select('status').eq('phone', phone).order('submitted_at', { ascending: false }).limit(1);
            const reg = regs && regs[0];
            if (reg && reg.status === 'pending') {
                alert('Your registration is pending admin approval.\nYou will be notified once approved.');
                return;
            }
            if (reg && reg.status === 'rejected') {
                alert('Your registration was rejected.\nPlease contact the distributor for help.');
                return;
            }
            alert('This mobile number is not registered.\nPlease register first.');
            cpRenderRegister();
            return;
        }
    }

    await cpSendOTP(phone, forRegister, regData);
}

async function cpSendOTP(phone, forRegister, regData) {
    const otp = String(Math.floor(100000 + Math.random() * 900000));
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();

    // Save OTP to Supabase
    const { error } = await supabaseClient.from('customer_otps').upsert(
        { phone, otp, expires_at: expiresAt, purpose: forRegister ? 'register' : 'login' },
        { onConflict: 'phone' }
    );
    if (error) {
        showToast('OTP Error: ' + error.message, 'error', 8000);
        console.error('cpSendOTP error:', error);
        return;
    }

    // Try Fast2SMS if key is configured
    const co = DB.ls.getObj('db_company') || {};
    const f2sKey = co.fast2smsKey || '';
    let smsSent = false;
    if (f2sKey) {
        try {
            const url = `https://www.fast2sms.com/dev/bulkV2?authorization=${encodeURIComponent(f2sKey)}&variables_values=${otp}&route=otp&numbers=${encodeURIComponent(phone)}`;
            const res = await fetch(url);
            const json = await res.json();
            if (json.return === true) {
                smsSent = true;
                showToast(`OTP sent to ${phone} via SMS`, 'success');
            } else {
                console.warn('Fast2SMS error:', json);
                showToast('SMS failed: ' + (json.message || 'Unknown error') + '. OTP shown on screen.', 'error', 6000);
            }
        } catch(e) {
            console.warn('Fast2SMS fetch failed:', e);
            showToast('SMS could not be sent. OTP shown on screen.', 'error', 5000);
        }
    }

    console.log(`OTP for ${phone}: ${otp}`);
    cpRenderOTP(phone, otp, forRegister, regData, smsSent);
}

function cpRenderOTP(phone, otpHint, forRegister, regData, smsSent) {
    const regDataJson = JSON.stringify(regData || null).replace(/"/g, '&quot;');
    const regDataStr  = regData ? JSON.stringify(regData).replace(/"/g, "'") : 'null';
    const otpBox = smsSent
        ? `<div style="background:rgba(34,197,94,0.1);border:2px solid rgba(34,197,94,0.4);border-radius:12px;padding:14px;text-align:center;margin-bottom:18px">
               <div style="font-size:0.8rem;color:#22c55e;font-weight:600;margin-bottom:4px">✅ OTP sent to ${phone} via SMS</div>
               <div style="font-size:0.75rem;color:var(--text-muted)">Ask customer to check their messages</div>
           </div>`
        : `<div style="background:rgba(99,102,241,0.1);border:2px dashed rgba(99,102,241,0.4);border-radius:12px;padding:14px;text-align:center;margin-bottom:18px">
               <div style="font-size:0.75rem;color:var(--text-muted);margin-bottom:6px">📲 Share this OTP with customer on WhatsApp</div>
               <div style="font-size:2rem;font-weight:800;letter-spacing:8px;color:var(--accent)">${otpHint}</div>
           </div>`;
    cpShell(`
    <div class="cp-card" style="margin-top:32px">
        <h2 style="margin:0 0 4px;font-size:1.3rem">Enter OTP</h2>
        <p style="color:var(--text-muted);font-size:0.85rem;margin:0 0 4px">OTP for <strong>${phone}</strong></p>
        <p style="color:var(--text-muted);font-size:0.78rem;margin:0 0 14px">Valid for 10 minutes</p>
        ${otpBox}
        <div class="cp-otp-row" id="cp-otp-row">
            ${[0,1,2,3,4,5].map(i => `<input class="cp-otp-box" maxlength="1" type="text" inputmode="numeric" pattern="[0-9]" id="cp-otp-${i}" oninput="cpOTPInput(this,${i})" onkeydown="cpOTPKey(this,${i},event)">`).join('')}
        </div>
        <input type="hidden" id="cp-otp-phone" value="${phone}">
        <input type="hidden" id="cp-otp-for-reg" value="${forRegister ? '1' : '0'}">
        <button class="btn btn-primary btn-block" style="margin-top:20px" onclick="cpVerifyOTP(${regDataJson})">Verify OTP</button>
        <button class="btn btn-outline btn-block" style="margin-top:8px" onclick="cpSendOTP('${phone}',${forRegister},${regDataStr})">Resend OTP</button>
    </div>`, true, cpRenderAuth);
    setTimeout(() => { const el = document.getElementById('cp-otp-0'); if(el) el.focus(); }, 100);
}

function cpOTPInput(el, idx) {
    el.value = el.value.replace(/[^0-9]/g,'');
    if (el.value && idx < 5) {
        const next = document.getElementById('cp-otp-' + (idx+1));
        if (next) next.focus();
    }
}

function cpOTPKey(el, idx, e) {
    if (e.key === 'Backspace' && !el.value && idx > 0) {
        const prev = document.getElementById('cp-otp-' + (idx-1));
        if (prev) { prev.focus(); prev.value = ''; }
    }
}

async function cpVerifyOTP(regData) {
    const phone = document.getElementById('cp-otp-phone').value;
    const forReg = document.getElementById('cp-otp-for-reg').value === '1';
    const entered = [0,1,2,3,4,5].map(i => { const el = document.getElementById('cp-otp-'+i); return el ? el.value : ''; }).join('');
    if (entered.length < 6) { alert('Please enter all 6 digits'); return; }
    const { data, error } = await supabaseClient.from('customer_otps').select('*').eq('phone', phone).single();
    if (error || !data) { alert('OTP not found. Please request again.'); return; }
    if (data.otp !== entered) { alert('Incorrect OTP. Please try again.'); return; }
    if (new Date(data.expires_at) < new Date()) { alert('OTP expired. Please request a new one.'); return; }
    // Delete used OTP
    await supabaseClient.from('customer_otps').delete().eq('phone', phone);
    if (forReg && regData) {
        await cpSaveReg(phone, regData);
    } else {
        await cpDoLogin(phone);
    }
}

async function cpDoLogin(phone) {
    await DB.refreshTables(['parties']);
    const parties = DB.get('db_parties') || [];
    const party = parties.find(p => p.portalPhone === phone && p.portalEnabled);
    if (party) {
        cpSession = {
            phone, partyId: party.id, partyName: party.name,
            allowedCategories: party.allowedCategories || [],
            paymentTerms: party.paymentTerms || 'COD',
            creditLimit: party.creditLimit || 0,
            balance: party.balance || 0
        };
        cpSaveSession();
        cpRenderHome();
        return;
    }
    // Check pending registration
    const { data: regs } = await supabaseClient.from('customer_registrations').select('*').eq('phone', phone).order('submitted_at', { ascending: false }).limit(1);
    const reg = regs && regs[0];
    if (reg && reg.status === 'pending') { cpRenderPending(reg); return; }
    if (reg && reg.status === 'rejected') { cpRenderRejected(reg); return; }
    // No account
    cpShell(`
    <div class="cp-card" style="margin-top:32px;text-align:center">
        <div style="font-size:3rem;margin-bottom:12px">🔍</div>
        <h3>No Account Found</h3>
        <p style="color:var(--text-muted);font-size:0.88rem">No approved account linked to <strong>${phone}</strong>.</p>
        <button class="btn btn-primary btn-block" style="margin-top:16px" onclick="cpRenderRegister()">Register Now</button>
        <button class="btn btn-outline btn-block" style="margin-top:8px" onclick="cpRenderAuth()">Try Another Number</button>
    </div>`, false, null);
}

// ---- REGISTRATION ----
function cpRenderRegister() {
    cpShell(`
    <div class="cp-card" style="margin-top:16px">
        <h2 style="margin:0 0 4px;font-size:1.2rem">Business Registration</h2>
        <p style="color:var(--text-muted);font-size:0.82rem;margin:0 0 16px">Fill in your details to request access</p>
        <div class="form-group"><label>Phone Number *</label><input id="cp-reg-phone" type="tel" class="form-control" placeholder="10-digit mobile" maxlength="15"></div>
        <div class="form-group"><label>Business Name *</label><input id="cp-reg-biz" class="form-control" placeholder="Your shop / company name"></div>
        <div class="form-group"><label>Contact Person *</label><input id="cp-reg-contact" class="form-control" placeholder="Owner / manager name"></div>
        <div class="form-group"><label>Address</label><input id="cp-reg-addr" class="form-control" placeholder="Full address"></div>
        <div class="form-group"><label>City</label><input id="cp-reg-city" class="form-control" placeholder="City"></div>
        <div class="form-group"><label>GSTIN (optional)</label><input id="cp-reg-gstin" class="form-control" placeholder="GST number"></div>
        <div class="form-group">
            <label>Location (GPS)</label>
            <div style="display:flex;gap:8px;align-items:center">
                <input id="cp-reg-lat" class="form-control" placeholder="Latitude" readonly style="flex:1">
                <input id="cp-reg-lng" class="form-control" placeholder="Longitude" readonly style="flex:1">
                <button class="btn btn-outline" style="white-space:nowrap" onclick="cpCaptureGeo()">📍 Get</button>
            </div>
        </div>
        <button class="btn btn-primary btn-block" style="margin-top:8px" onclick="cpRegSubmit()">Submit Registration</button>
    </div>`, true, cpRenderAuth);
}

function cpCaptureGeo() {
    if (!navigator.geolocation) { alert('GPS not supported'); return; }
    navigator.geolocation.getCurrentPosition(pos => {
        document.getElementById('cp-reg-lat').value = pos.coords.latitude.toFixed(6);
        document.getElementById('cp-reg-lng').value = pos.coords.longitude.toFixed(6);
        showToast('Location captured!', 'success');
    }, () => alert('Could not get location. Please allow GPS access.'));
}

async function cpRegSubmit() {
    const phone = (document.getElementById('cp-reg-phone').value || '').trim();
    const biz = (document.getElementById('cp-reg-biz').value || '').trim();
    const contact = (document.getElementById('cp-reg-contact').value || '').trim();
    if (!phone || phone.length < 10) { alert('Valid phone required'); return; }
    if (!biz) { alert('Business name required'); return; }
    if (!contact) { alert('Contact person required'); return; }

    // Check if phone already has an approved portal account
    await DB.refreshTables(['parties']);
    const parties = DB.get('db_parties') || [];
    const existing = parties.find(p => p.portalPhone === phone && p.portalEnabled);
    if (existing) {
        alert('This mobile number is already registered and approved.\nPlease use Login instead.');
        cpRenderAuth();
        return;
    }

    // Check for pending / rejected registration
    const { data: prevRegs } = await supabaseClient.from('customer_registrations')
        .select('status').eq('phone', phone).order('submitted_at', { ascending: false }).limit(1);
    const prev = prevRegs && prevRegs[0];
    if (prev && prev.status === 'pending') {
        alert('A registration request for this number is already pending.\nPlease wait for admin approval.');
        return;
    }

    const regData = {
        phone, businessName: biz, contactName: contact,
        address: document.getElementById('cp-reg-addr').value.trim(),
        city: document.getElementById('cp-reg-city').value.trim(),
        gstin: document.getElementById('cp-reg-gstin').value.trim(),
        lat: document.getElementById('cp-reg-lat').value,
        lng: document.getElementById('cp-reg-lng').value
    };
    await cpSendOTP(phone, true, regData);
}

async function cpSaveReg(phone, d) {
    const id = 'reg_' + Date.now();
    const payload = {
        id, phone, business_name: d.businessName, contact_name: d.contactName,
        address: d.address, city: d.city, gstin: d.gstin,
        lat: d.lat, lng: d.lng, status: 'pending'
    };
    const { error } = await supabaseClient.from('customer_registrations').insert(payload);
    if (error) { alert('Error submitting: ' + error.message); return; }
    showToast('Registration submitted! Admin will review soon.', 'success');
    cpRenderPending({ business_name: d.businessName, contact_name: d.contactName });
}

function cpRenderPending(reg) {
    cpShell(`
    <div class="cp-card" style="margin-top:48px;text-align:center">
        <div style="font-size:3.5rem;margin-bottom:16px">⏳</div>
        <h3 style="margin:0 0 8px">Registration Pending</h3>
        <p style="color:var(--text-muted);font-size:0.88rem;margin:0 0 4px"><strong>${reg.business_name || ''}</strong></p>
        <p style="color:var(--text-muted);font-size:0.85rem">Your registration is under review. The admin will approve soon and notify you.</p>
        <button class="btn btn-outline btn-block" style="margin-top:24px" onclick="cpLogout()">Back to Login</button>
    </div>`, false, null);
}

function cpRenderRejected(reg) {
    cpShell(`
    <div class="cp-card" style="margin-top:48px;text-align:center">
        <div style="font-size:3.5rem;margin-bottom:16px">❌</div>
        <h3 style="margin:0 0 8px">Registration Rejected</h3>
        ${reg.rejection_reason ? `<p style="color:#ef4444;font-size:0.88rem">Reason: ${reg.rejection_reason}</p>` : ''}
        <p style="color:var(--text-muted);font-size:0.85rem">Please contact the admin for more information.</p>
        <button class="btn btn-outline btn-block" style="margin-top:24px" onclick="cpLogout()">Back to Login</button>
    </div>`, false, null);
}

// ---- HOME ----
async function cpRenderHome() {
    if (!cpSession) { cpRenderAuth(); return; }
    await DB.refreshTables(['sales_orders', 'inventory', 'categories']);
    const orders = (DB.get('db_salesorders') || []).filter(o => o.partyId === cpSession.partyId).sort((a,b) => (b.date||'').localeCompare(a.date||'')).slice(0, 20);
    const bal = cpSession.balance || 0;
    const balLabel = bal < 0 ? `You are owed ₹${Math.abs(bal).toLocaleString('en-IN')}` : bal > 0 ? `You owe ₹${bal.toLocaleString('en-IN')}` : 'No outstanding balance';
    const balColor = bal < 0 ? '#22c55e' : bal > 0 ? '#ef4444' : '#6b7280';
    cpShell(`
    <div class="cp-balance-card" style="background:linear-gradient(135deg,var(--accent),#f59e0b);color:#fff;border-radius:16px;padding:20px;margin-bottom:20px">
        <div style="font-size:0.8rem;opacity:0.85;margin-bottom:4px">Welcome</div>
        <div style="font-size:1.2rem;font-weight:700;margin-bottom:12px">${cpSession.partyName}</div>
        <div style="font-size:0.78rem;opacity:0.8;margin-bottom:2px">Account Balance</div>
        <div style="font-size:1.5rem;font-weight:800">${balLabel}</div>
    </div>
    <div style="display:flex;gap:12px;margin-bottom:20px">
        <button class="btn btn-primary" style="flex:1;font-size:0.9rem" onclick="cpRenderCatalog(null,'')">🛍️ Place Order</button>
        <button class="btn btn-outline" style="flex:1;font-size:0.9rem" onclick="cpRenderCart()">🛒 Cart${Object.keys(cpCart).length ? ' ('+Object.values(cpCart).reduce((a,b)=>a+b,0)+')' : ''}</button>
    </div>
    <h4 style="margin:0 0 12px;font-size:0.95rem;color:var(--text-muted)">Recent Orders</h4>
    ${orders.length ? orders.map(o => `
    <div class="cp-order-card" onclick="cpViewOrder('${o.id}')">
        <div style="display:flex;justify-content:space-between;align-items:center">
            <div>
                <div style="font-weight:600;font-size:0.92rem">${o.orderNo || o.id}</div>
                <div style="font-size:0.78rem;color:var(--text-muted)">${o.date || ''} &bull; ${(o.items||[]).length} items</div>
            </div>
            <div style="text-align:right">
                <div style="font-weight:700">₹${(o.total||0).toLocaleString('en-IN')}</div>
                <span class="status-badge status-${o.status||'pending'}">${o.status||'pending'}</span>
            </div>
        </div>
    </div>`).join('') : '<p style="text-align:center;color:var(--text-muted);padding:24px 0">No orders yet</p>'}
    `, false, null);
}

function cpViewOrder(orderId) {
    const orders = DB.get('db_salesorders') || [];
    const o = orders.find(x => x.id === orderId);
    if (!o) return;
    const items = o.items || [];
    cpShell(`
    <div class="cp-card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <div>
                <div style="font-weight:700;font-size:1rem">${o.orderNo || o.id}</div>
                <div style="font-size:0.8rem;color:var(--text-muted)">${o.date || ''}</div>
            </div>
            <span class="status-badge status-${o.status||'pending'}">${o.status||'pending'}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:0.85rem">
            <thead><tr style="border-bottom:1px solid var(--border)">
                <th style="text-align:left;padding:6px 4px">Item</th>
                <th style="text-align:center;padding:6px 4px">Qty</th>
                <th style="text-align:right;padding:6px 4px">Price</th>
                <th style="text-align:right;padding:6px 4px">Total</th>
            </tr></thead>
            <tbody>${items.map(li => `
            <tr style="border-bottom:1px solid var(--border)">
                <td style="padding:6px 4px">${li.itemName||li.name||''}</td>
                <td style="text-align:center;padding:6px 4px">${li.qty||0} ${li.uom||''}</td>
                <td style="text-align:right;padding:6px 4px">₹${(+(li.price||0)).toFixed(2)}</td>
                <td style="text-align:right;padding:6px 4px">₹${(+(li.amount||0)).toFixed(2)}</td>
            </tr>`).join('')}
            </tbody>
            <tfoot><tr>
                <td colspan="3" style="text-align:right;padding:8px 4px;font-weight:600">Total</td>
                <td style="text-align:right;padding:8px 4px;font-weight:700">₹${(o.total||0).toLocaleString('en-IN')}</td>
            </tr></tfoot>
        </table>
        ${o.notes ? `<div style="margin-top:12px;padding:10px;background:var(--bg-card);border-radius:8px;font-size:0.85rem;color:var(--text-muted)">${o.notes}</div>` : ''}
    </div>`, true, cpRenderHome);
}

// ---- CATALOG ----
function cpRenderCatalog(filterCat, search) {
    const inventory = DB.get('db_inventory') || [];
    const categories = DB.get('db_categories') || [];
    const allowed = cpSession.allowedCategories || [];
    // Filter to allowed categories (empty = all allowed)
    let items = inventory.filter(i => i.isActive !== false);
    if (allowed.length > 0) items = items.filter(i => allowed.includes(i.category));
    // Category filter
    const cats = [...new Set(items.map(i => i.category).filter(Boolean))];
    if (filterCat) items = items.filter(i => i.category === filterCat);
    if (search) items = items.filter(i => (i.name||'').toLowerCase().includes(search.toLowerCase()));
    cpShell(`
    <div>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px">
            <input id="cp-search" class="form-control" placeholder="Search items..." value="${search||''}" oninput="cpRenderCatalog('${filterCat||''}',this.value)" style="flex:1">
            <button class="btn btn-outline" onclick="cpRenderCart()">🛒 ${Object.values(cpCart).reduce((a,b)=>a+b,0)||''}</button>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px">
            <button class="cp-cat-pill ${!filterCat?'active':''}" onclick="cpRenderCatalog(null,'${search||''}')">All</button>
            ${cats.map(c => `<button class="cp-cat-pill ${filterCat===c?'active':''}" onclick="cpRenderCatalog('${c}','${search||''}')">${c}</button>`).join('')}
        </div>
        ${items.length ? cpItemListHTML(items) : '<p style="text-align:center;color:var(--text-muted);padding:24px 0">No items found</p>'}
    </div>`, true, cpRenderHome);
}

function cpItemListHTML(items) {
    return items.map(i => {
        const qty = cpCart[i.id] || 0;
        const price = i.salePrice || i.mrp || 0;
        return `<div class="cp-item-row">
            ${(i.imageUrl||i.photo) ? `<img src="${i.imageUrl||i.photo}" class="cp-item-img">` : `<div class="cp-item-img" style="background:var(--bg-page);display:flex;align-items:center;justify-content:center;font-size:1.4rem">📦</div>`}
            <div style="flex:1;min-width:0">
                <div style="font-weight:600;font-size:0.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${i.name}</div>
                <div style="font-size:0.78rem;color:var(--text-muted)">${i.category||''} ${i.uom ? '| '+i.uom : ''}</div>
                <div style="font-weight:700;color:var(--accent);font-size:0.95rem">₹${price.toFixed ? price.toFixed(2) : price}</div>
            </div>
            <div style="display:flex;align-items:center;gap:6px;flex-shrink:0">
                ${qty > 0 ? `
                <button class="cp-qty-btn" onclick="cpCartUpdate('${i.id}',-1)">-</button>
                <span style="min-width:24px;text-align:center;font-weight:600">${qty}</span>
                <button class="cp-qty-btn" onclick="cpCartUpdate('${i.id}',1)">+</button>
                ` : `<button class="btn btn-primary" style="font-size:0.8rem;padding:6px 14px" onclick="cpCartAdd('${i.id}')">Add</button>`}
            </div>
        </div>`;
    }).join('');
}

function cpCartAdd(itemId) {
    cpCart[itemId] = 1;
    // Re-render the specific item row by refreshing the whole catalog
    const search = document.getElementById('cp-search') ? document.getElementById('cp-search').value : '';
    const active = document.querySelector('.cp-cat-pill.active');
    const cat = active && active.textContent !== 'All' ? active.textContent : null;
    cpRenderCatalog(cat, search);
}

function cpCartUpdate(itemId, delta) {
    cpCart[itemId] = Math.max(0, (cpCart[itemId] || 0) + delta);
    if (cpCart[itemId] === 0) delete cpCart[itemId];
    const search = document.getElementById('cp-search') ? document.getElementById('cp-search').value : '';
    const active = document.querySelector('.cp-cat-pill.active');
    const cat = active && active.textContent !== 'All' ? active.textContent : null;
    cpRenderCatalog(cat, search);
}

function cpRenderCart() {
    const inventory = DB.get('db_inventory') || [];
    const cartItems = Object.entries(cpCart).map(([id, qty]) => {
        const item = inventory.find(i => i.id === id);
        if (!item) return null;
        const price = item.salePrice || item.mrp || 0;
        return { ...item, qty, price, amount: qty * price };
    }).filter(Boolean);
    const total = cartItems.reduce((s, i) => s + i.amount, 0);
    cpShell(`
    <h3 style="margin:0 0 16px">Your Cart</h3>
    ${cartItems.length ? `
    ${cartItems.map(i => `
    <div class="cp-item-row">
        <div style="flex:1">
            <div style="font-weight:600">${i.name}</div>
            <div style="font-size:0.8rem;color:var(--text-muted)">₹${i.price.toFixed(2)} each</div>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
            <button class="cp-qty-btn" onclick="cpCartUpdate('${i.id}',-1)">-</button>
            <span style="min-width:24px;text-align:center;font-weight:600">${i.qty}</span>
            <button class="cp-qty-btn" onclick="cpCartUpdate('${i.id}',1)">+</button>
            <span style="min-width:70px;text-align:right;font-weight:700">₹${i.amount.toFixed(2)}</span>
        </div>
    </div>`).join('')}
    <div style="border-top:2px solid var(--border);margin-top:12px;padding-top:12px;display:flex;justify-content:space-between;align-items:center">
        <span style="font-size:1rem;font-weight:600">Total</span>
        <span style="font-size:1.2rem;font-weight:800;color:var(--accent)">₹${total.toLocaleString('en-IN', {minimumFractionDigits:2})}</span>
    </div>
    <div class="form-group" style="margin-top:16px"><label>Notes / Instructions</label><textarea id="cp-order-notes" class="form-control" rows="2" placeholder="Any special instructions..."></textarea></div>
    <button class="btn btn-primary btn-block" style="margin-top:8px;font-size:1rem" onclick="cpPlaceOrder()">Place Order</button>
    ` : `<p style="text-align:center;color:var(--text-muted);padding:40px 0">Your cart is empty</p>`}
    <button class="btn btn-outline btn-block" style="margin-top:8px" onclick="cpRenderCatalog(null,'')">Continue Shopping</button>
    `, true, cpRenderHome);
}

async function cpPlaceOrder() {
    if (!Object.keys(cpCart).length) { alert('Cart is empty'); return; }
    const inventory = DB.get('db_inventory') || [];
    const items = Object.entries(cpCart).map(([id, qty]) => {
        const item = inventory.find(i => i.id === id);
        if (!item) return null;
        const price = +(item.salePrice || item.mrp || 0).toFixed ? +(item.salePrice || item.mrp || 0).toFixed(2) : (item.salePrice || item.mrp || 0);
        return { itemId: id, itemName: item.name, qty, uom: item.uom||'', price, amount: +(qty * price).toFixed(2) };
    }).filter(Boolean);
    const total = items.reduce((s, i) => s + i.amount, 0);
    const now = new Date();
    const orderId = 'cp-' + now.getTime();
    const orderNo = 'CPO-' + Math.random().toString(36).substr(2,8).toUpperCase();
    const notes = document.getElementById('cp-order-notes') ? document.getElementById('cp-order-notes').value.trim() : '';
    const payload = {
        id: orderId, order_no: orderNo,
        date: now.toISOString().split('T')[0],
        party_id: cpSession.partyId, party_name: cpSession.partyName,
        items, total: +total.toFixed(2),
        status: 'pending', notes,
        payment_terms: cpSession.paymentTerms,
        created_by: 'Customer Portal'
    };
    const { error } = await supabaseClient.from('sales_orders').insert(payload);
    if (error) { alert('Error placing order: ' + error.message); return; }
    cpCart = {};
    showToast('Order placed successfully!', 'success');
    await DB.refreshTables(['sales_orders']);
    cpRenderHome();
}

// ---- ADMIN: CUSTOMER REQUESTS ----
async function renderCustomerRequests() {
    const { data: regs, error } = await supabaseClient.from('customer_registrations').select('*').order('submitted_at', { ascending: false });
    if (error) { document.getElementById('page-content').innerHTML = '<p>Error loading requests: ' + error.message + '</p>'; return; }
    const pending = (regs||[]).filter(r => r.status === 'pending');
    const others = (regs||[]).filter(r => r.status !== 'pending');
    document.getElementById('page-content').innerHTML = `
    <div style="padding:16px">
        <h3 style="margin:0 0 16px">Pending Requests (${pending.length})</h3>
        ${pending.length ? pending.map(r => cpRegCard(r)).join('') : '<p style="color:var(--text-muted)">No pending requests</p>'}
        ${others.length ? `<h3 style="margin:20px 0 12px">Processed (${others.length})</h3>${others.map(r => cpRegCard(r)).join('')}` : ''}
    </div>`;
}

function cpRegCard(r) {
    return `<div class="cp-card" style="margin-bottom:12px">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
            <div>
                <div style="font-weight:700">${r.business_name||''}</div>
                <div style="font-size:0.82rem;color:var(--text-muted)">${r.contact_name||''} &bull; ${r.phone||''}</div>
                ${r.city ? `<div style="font-size:0.8rem;color:var(--text-muted)">${r.city}</div>` : ''}
                ${r.gstin ? `<div style="font-size:0.78rem;color:var(--text-muted)">GSTIN: ${r.gstin}</div>` : ''}
                ${r.lat && r.lng ? `<div style="font-size:0.78rem"><a href="https://maps.google.com/?q=${r.lat},${r.lng}" target="_blank" style="color:var(--accent)">📍 View on Map</a></div>` : ''}
                <div style="font-size:0.75rem;color:var(--text-muted);margin-top:4px">${new Date(r.submitted_at||Date.now()).toLocaleDateString('en-IN')}</div>
            </div>
            <div>
                <span class="status-badge status-${r.status||'pending'}">${r.status||'pending'}</span>
                ${r.rejection_reason ? `<div style="font-size:0.78rem;color:#ef4444;margin-top:4px">Reason: ${r.rejection_reason}</div>` : ''}
            </div>
        </div>
        ${r.status === 'pending' ? `
        <div style="display:flex;gap:8px;margin-top:12px">
            <button class="btn btn-primary" style="flex:1" onclick="openApproveCustModal('${r.id}')">Approve</button>
            <button class="btn" style="flex:1;background:#ef4444;color:#fff" onclick="rejectCustReg('${r.id}')">Reject</button>
        </div>` : ''}
    </div>`;
}

async function openApproveCustModal(regId) {
    const { data: regs } = await supabaseClient.from('customer_registrations').select('*').eq('id', regId).single();
    if (!regs) return;
    const parties = DB.get('db_parties') || [];
    const cats = DB.get('db_categories') || [];
    const matching = parties.filter(p => p.name && regs.business_name && p.name.toLowerCase().includes(regs.business_name.toLowerCase().split(' ')[0]));
    openModal('Approve Customer', `
    <div>
        <p style="font-size:0.88rem;margin-bottom:16px"><strong>${regs.business_name}</strong> — ${regs.phone}</p>
        <div class="form-group">
            <label>Link to Existing Party (optional)</label>
            <select id="ap-party" class="form-control">
                <option value="">— Create New Party —</option>
                ${parties.filter(p => p.type === 'customer' || !p.type).map(p => `<option value="${p.id}" ${matching.find(m=>m.id===p.id)?'selected':''}>${p.name}</option>`).join('')}
            </select>
        </div>
        <div class="form-group">
            <label>Payment Terms</label>
            <select id="ap-terms" class="form-control">
                <option value="COD">Cash on Delivery (COD)</option>
                <option value="Net7">Net 7 Days</option>
                <option value="Net15">Net 15 Days</option>
                <option value="Net30">Net 30 Days</option>
                <option value="Net60">Net 60 Days</option>
            </select>
        </div>
        <div class="form-group">
            <label>Credit Limit (₹)</label>
            <input id="ap-credit" type="number" class="form-control" value="0" min="0">
        </div>
        <div class="form-group">
            <label>Allowed Categories (leave empty for all)</label>
            <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:6px" id="ap-cats">
                ${cats.map(c => `<label style="display:flex;align-items:center;gap:4px;font-size:0.85rem"><input type="checkbox" value="${c.name||c}"> ${c.name||c}</label>`).join('')}
            </div>
        </div>
    </div>`,
    `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
     <button class="btn btn-primary" onclick="confirmApproveCust('${regId}', document.getElementById('ap-party').value)">Confirm Approve</button>`);
}

async function confirmApproveCust(regId, existingPartyId) {
    const { data: reg } = await supabaseClient.from('customer_registrations').select('*').eq('id', regId).single();
    if (!reg) return;
    const paymentTerms = document.getElementById('ap-terms').value;
    const creditLimit = +(document.getElementById('ap-credit').value) || 0;
    const checkedCats = [...document.querySelectorAll('#ap-cats input:checked')].map(el => el.value);
    let partyId = existingPartyId;
    if (!partyId) {
        // Create new party
        const newParty = {
            id: crypto.randomUUID ? crypto.randomUUID() : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            }),
            name: reg.business_name, type: 'customer',
            phone: reg.phone, address: reg.address || '', city: reg.city || '',
            gstin: reg.gstin || '', balance: 0,
            portal_phone: reg.phone, portal_enabled: true,
            allowed_categories: checkedCats, payment_terms: paymentTerms, credit_limit: creditLimit
        };
        const { error: pe } = await supabaseClient.from('parties').insert(newParty);
        if (pe) { alert('Error creating party: ' + pe.message); return; }
        partyId = newParty.id;
    } else {
        // Update existing party
        const { error: pe } = await supabaseClient.from('parties').update({
            portal_phone: reg.phone, portal_enabled: true,
            allowed_categories: checkedCats, payment_terms: paymentTerms, credit_limit: creditLimit
        }).eq('id', partyId);
        if (pe) { alert('Error updating party: ' + pe.message); return; }
    }
    // Update registration status
    await supabaseClient.from('customer_registrations').update({ status: 'approved', party_id: partyId }).eq('id', regId);
    closeModal();
    showToast('Customer approved!', 'success');
    await DB.refreshTables(['parties']);
    renderCustomerRequests();
}

async function rejectCustReg(regId) {
    const reason = prompt('Reason for rejection (optional):') || '';
    await supabaseClient.from('customer_registrations').update({ status: 'rejected', rejection_reason: reason }).eq('id', regId);
    showToast('Registration rejected.', 'info');
    renderCustomerRequests();
}

// ============================================================
// HR MODULE: STAFF MASTER, ATTENDANCE, PAYROLL
// ============================================================

// ---- STAFF MASTER ----
async function renderStaffMaster() {
    const { data: staff, error } = await supabaseClient.from('staff').select('*').order('name');
    if (error) { pageContent.innerHTML = `<p style="color:red">Error: ${error.message}</p>`; return; }
    const isAdmin = currentUser && currentUser.role === 'Admin';
    pageContent.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <div>
            <div style="font-size:0.85rem;color:var(--text-muted)">${staff.length} staff member(s)</div>
        </div>
        ${isAdmin ? `<button class="btn btn-primary" onclick="openStaffModal()">+ Add Staff</button>` : ''}
    </div>
    <div class="card"><div class="card-body">
    <div class="table-wrapper"><table class="data-table">
        <thead><tr><th>Name</th><th>Role</th><th>Phone</th><th>Monthly Salary</th><th>Join Date</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>
        ${staff.length ? staff.map(s => `<tr style="${s.status==='inactive'?'opacity:0.55':''}">
            <td style="font-weight:600">${escapeHtml(s.name)}</td>
            <td><span class="badge badge-info" style="font-size:0.72rem">${s.role||'Staff'}</span></td>
            <td>${s.phone ? `<a href="tel:${s.phone}" style="color:var(--success)">${s.phone}</a>` : '-'}</td>
            <td style="font-weight:600">${currency(s.monthly_salary||0)}</td>
            <td>${s.join_date ? fmtDate(s.join_date) : '-'}</td>
            <td><span class="badge ${s.status==='active'?'badge-success':'badge-danger'}">${s.status||'active'}</span></td>
            <td><div class="action-btns">
                ${isAdmin ? `<button class="btn-icon" onclick="openStaffModal('${s.id}')">✏️</button>
                <button class="btn-icon" onclick="toggleStaffStatus('${s.id}','${s.status||'active'}')" title="${s.status==='inactive'?'Activate':'Deactivate'}">${s.status==='inactive'?'✅':'🚫'}</button>
                <button class="btn-icon" onclick="openStaffAdvance('${s.id}','${escapeHtml(s.name)}')" title="Give Advance">💵</button>` : ''}
            </div></td>
        </tr>`).join('') : '<tr><td colspan="7"><div class="empty-state"><p>No staff added yet</p></div></td></tr>'}
        </tbody>
    </table></div>
    </div></div>`;
}

async function openStaffModal(id) {
    let s = null;
    if (id) {
        const { data } = await supabaseClient.from('staff').select('*').eq('id', id).single();
        s = data;
    }
    const ROLES = ['Admin', 'Manager', 'Salesman', 'Packer', 'Delivery', 'Driver', 'Helper', 'Accountant', 'Staff'];
    openModal(s ? 'Edit Staff' : 'Add Staff', `
    <div class="form-row">
        <div class="form-group"><label>Name *</label><input id="f-st-name" class="form-control" value="${s ? escapeHtml(s.name) : ''}"></div>
        <div class="form-group"><label>Role</label>
            <select id="f-st-role" class="form-control">${ROLES.map(r=>`<option ${s&&s.role===r?'selected':''}>${r}</option>`).join('')}</select>
        </div>
    </div>
    <div class="form-row">
        <div class="form-group"><label>Phone</label><input id="f-st-phone" class="form-control" value="${s ? (s.phone||'') : ''}" type="tel"></div>
        <div class="form-group"><label>Monthly Salary (₹)</label><input id="f-st-salary" class="form-control" type="number" min="0" value="${s ? (s.monthly_salary||0) : ''}"></div>
    </div>
    <div class="form-row">
        <div class="form-group"><label>Join Date</label><input id="f-st-join" class="form-control" type="date" value="${s ? (s.join_date||'') : today()}"></div>
    </div>
    <div class="modal-actions">
        <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="saveStaff('${id||''}')">Save</button>
    </div>`);
}

async function saveStaff(id) {
    const name = ($('f-st-name').value||'').trim();
    if (!name) return alert('Name is required');
    const data = {
        name, role: $('f-st-role').value,
        phone: ($('f-st-phone').value||'').trim(),
        monthly_salary: +$('f-st-salary').value || 0,
        join_date: $('f-st-join').value || null,
        status: 'active'
    };
    if (id) {
        const { error } = await supabaseClient.from('staff').update(data).eq('id', id);
        if (error) return alert('Error: ' + error.message);
    } else {
        data.id = 'st_' + Date.now();
        const { error } = await supabaseClient.from('staff').insert(data);
        if (error) return alert('Error: ' + error.message);
    }
    closeModal(); showToast('Staff saved!', 'success'); renderStaffMaster();
}

async function toggleStaffStatus(id, currentStatus) {
    const newStatus = currentStatus === 'inactive' ? 'active' : 'inactive';
    await supabaseClient.from('staff').update({ status: newStatus }).eq('id', id);
    showToast(`Staff ${newStatus === 'active' ? 'activated' : 'deactivated'}!`, 'success');
    renderStaffMaster();
}

// ---- ATTENDANCE ----
async function renderAttendance() {
    const selDate  = window._attDate  || today();
    const selFrom  = window._attFrom  || today();
    const selTo    = window._attTo    || today();
    const rangeMode = window._attRangeMode || false;
    const selMonth = selDate.substring(0, 7);

    const { data: staff } = await supabaseClient.from('staff').select('*').eq('status', 'active').order('name');

    // Single-day data
    const { data: attRecs } = await supabaseClient.from('attendance').select('*').eq('date', selDate);
    const attMap = {}; (attRecs||[]).forEach(r => attMap[r.staff_id] = r);

    // Monthly summary
    const { data: monthRecs } = await supabaseClient.from('attendance').select('*').gte('date', selMonth+'-01').lte('date', selMonth+'-31');
    const monthMap = {};
    (monthRecs||[]).forEach(r => {
        if (!monthMap[r.staff_id]) monthMap[r.staff_id] = { P:0, A:0, HD:0, PL:0, H:0 };
        const k = r.status === 'Present' ? 'P' : r.status === 'Absent' ? 'A' : r.status === 'Half Day' ? 'HD' : r.status === 'Paid Leave' ? 'PL' : 'H';
        monthMap[r.staff_id][k] = (monthMap[r.staff_id][k]||0) + 1;
    });

    const STATUS_BTNS = [
        { s: 'Present',    label: 'P',    color: '#22c55e', bg: 'rgba(34,197,94,0.15)' },
        { s: 'Absent',     label: 'A',    color: '#ef4444', bg: 'rgba(239,68,68,0.15)' },
        { s: 'Half Day',   label: '½',    color: '#f59e0b', bg: 'rgba(245,158,11,0.15)' },
        { s: 'Paid Leave', label: 'PL',   color: '#6366f1', bg: 'rgba(99,102,241,0.15)' },
        { s: 'Holiday',    label: 'H',    color: '#64748b', bg: 'rgba(100,116,139,0.15)' },
    ];

    // ── Header toolbar ──
    const singleToolbar = `
        <input type="date" value="${selDate}" onchange="window._attDate=this.value;renderAttendance()" class="form-control" style="width:160px">
        <button class="btn btn-outline btn-sm" onclick="window._attDate='${today()}';renderAttendance()">Today</button>
        <button class="btn btn-outline btn-sm" onclick="markAllAttendance('Present','${selDate}')">✅ All Present</button>
        <button class="btn btn-outline btn-sm" onclick="markAllAttendance('Holiday','${selDate}')">🏖️ Holiday</button>`;

    const rangeToolbar = `
        <label style="font-size:0.82rem;color:var(--text-muted);margin:0">From</label>
        <input type="date" id="att-from" value="${selFrom}" class="form-control" style="width:150px">
        <label style="font-size:0.82rem;color:var(--text-muted);margin:0">To</label>
        <input type="date" id="att-to" value="${selTo}" class="form-control" style="width:150px">
        <button class="btn btn-primary btn-sm" onclick="openRangeAttModal()">Mark Range…</button>`;

    pageContent.innerHTML = `
    <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-bottom:16px">
        <div style="display:flex;align-items:center;gap:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:3px">
            <button class="btn btn-sm ${!rangeMode?'btn-primary':'btn-outline'}" style="border-radius:6px" onclick="window._attRangeMode=false;renderAttendance()">Single Day</button>
            <button class="btn btn-sm ${rangeMode?'btn-primary':'btn-outline'}" style="border-radius:6px" onclick="window._attRangeMode=true;renderAttendance()">Date Range</button>
        </div>
        ${rangeMode ? rangeToolbar : singleToolbar}
    </div>

    ${!(staff&&staff.length) ? '<div class="empty-state"><p>No active staff. <a href="#" onclick="navigateTo(\'staffmaster\')" style="color:var(--accent)">Add staff first</a></p></div>' : `

    ${rangeMode ? `
    <div class="card" style="margin-bottom:20px;background:rgba(99,102,241,0.06);border:1px solid rgba(99,102,241,0.2)">
        <div class="card-body" style="padding:14px 18px">
            <p style="margin:0;font-size:0.88rem;color:var(--text-muted)">
                <b>Date Range Mode:</b> Select From / To dates above, then click <b>Mark Range…</b> to choose a status and apply it to all staff (or individual staff) across every day in the range. Existing records are overwritten.
            </p>
        </div>
    </div>` : ''}

    <div class="card" style="margin-bottom:20px"><div class="card-body">
    <div class="table-wrapper"><table class="data-table">
        <thead><tr><th>Staff</th><th>${rangeMode ? 'Bulk Range Action' : 'Mark Attendance'}</th><th style="text-align:center">This Month (${selMonth})</th></tr></thead>
        <tbody>
        ${staff.map(s => {
            const cur = attMap[s.id];
            const mo = monthMap[s.id] || { P:0, A:0, HD:0, PL:0, H:0 };
            const monthSummary = `<span style="font-size:0.8rem;color:var(--text-muted)">P:<b style="color:#22c55e">${mo.P}</b> A:<b style="color:#ef4444">${mo.A}</b> ½:<b style="color:#f59e0b">${mo.HD}</b> PL:<b style="color:#6366f1">${mo.PL}</b></span>`;

            let actionCell;
            if (rangeMode) {
                actionCell = `<div style="display:flex;flex-wrap:wrap;gap:6px">
                    ${STATUS_BTNS.map(b => `<button onclick="markStaffRange('${s.id}','${escapeHtml(s.name)}','${b.s}')" style="padding:5px 10px;border-radius:20px;font-size:0.78rem;font-weight:700;cursor:pointer;border:2px solid ${b.color};color:${b.color};background:${b.bg};transition:all 0.15s">${b.label}</button>`).join('')}
                </div>`;
            } else {
                actionCell = `<div style="display:flex;flex-wrap:wrap;gap:6px">
                    ${STATUS_BTNS.map(b => {
                        const active = cur && cur.status === b.s;
                        return `<button onclick="markAttendance('${s.id}','${escapeHtml(s.name)}','${selDate}','${b.s}')" style="padding:5px 10px;border-radius:20px;font-size:0.78rem;font-weight:700;cursor:pointer;border:2px solid ${b.color};color:${active?'#fff':b.color};background:${active?b.color:b.bg};transition:all 0.15s">${b.label}</button>`;
                    }).join('')}
                </div>`;
            }

            return `<tr>
                <td><div style="font-weight:600">${escapeHtml(s.name)}</div><div style="font-size:0.75rem;color:var(--text-muted)">${s.role||'Staff'}</div></td>
                <td>${actionCell}</td>
                <td style="text-align:center">${monthSummary}</td>
            </tr>`;
        }).join('')}
        </tbody>
    </table></div>
    </div></div>

    <h4 style="margin-bottom:12px;font-size:0.95rem">Monthly Attendance — ${selMonth}</h4>
    <div class="card"><div class="card-body" style="overflow-x:auto">
        ${renderMonthCalendar(staff, monthRecs||[], selMonth)}
    </div></div>`}`;
}

// ── Range mode helpers ──
function _getRangeDates() {
    const fromEl = document.getElementById('att-from');
    const toEl   = document.getElementById('att-to');
    const from = fromEl ? fromEl.value : (window._attFrom || today());
    const to   = toEl   ? toEl.value   : (window._attTo   || today());
    if (from > to) { showToast('From date must be before To date', 'error'); return null; }
    window._attFrom = from; window._attTo = to;
    const dates = [];
    let cur = new Date(from);
    const end = new Date(to);
    while (cur <= end) {
        dates.push(cur.toISOString().split('T')[0]);
        cur.setDate(cur.getDate() + 1);
    }
    return dates;
}

function openRangeAttModal() {
    const dates = _getRangeDates();
    if (!dates) return;
    openModal('Mark Attendance — Date Range',
        `<p style="margin:0 0 14px;color:var(--text-muted);font-size:0.88rem">
            Applying to <b>${dates.length} day(s)</b>: ${dates[0]} → ${dates[dates.length-1]}
        </p>
        <div class="form-group">
            <label>Status to apply to ALL staff</label>
            <select id="rng-status" class="form-control">
                <option value="Present">Present</option>
                <option value="Absent">Absent</option>
                <option value="Half Day">Half Day</option>
                <option value="Paid Leave">Paid Leave</option>
                <option value="Holiday">Holiday</option>
            </select>
        </div>
        <p style="font-size:0.8rem;color:var(--text-muted);margin:8px 0 0">Use individual staff row buttons to apply different statuses per staff.</p>`,
        `<button class="btn btn-outline" onclick="closeModal()">Cancel</button>
         <button class="btn btn-primary" onclick="confirmRangeAttendance()">Apply to All Staff</button>`
    );
}

async function confirmRangeAttendance() {
    const dates = _getRangeDates();
    if (!dates) return;
    const status = $('rng-status').value;
    closeModal();
    await _applyRangeAttendance(null, null, dates, status);
    showToast(`Marked ${status} for all staff across ${dates.length} day(s)`, 'success');
    renderAttendance();
}

async function markStaffRange(staffId, staffName, status) {
    const dates = _getRangeDates();
    if (!dates) return;
    if (!confirm(`Mark ${staffName} as "${status}" for ${dates.length} day(s)?\n${dates[0]} → ${dates[dates.length-1]}`)) return;
    await _applyRangeAttendance(staffId, staffName, dates, status);
    showToast(`${staffName} marked as ${status} for ${dates.length} day(s)`, 'success');
    renderAttendance();
}

async function _applyRangeAttendance(staffId, staffName, dates, status) {
    // Load staff list if applying to all
    let targets = [];
    if (staffId) {
        targets = [{ id: staffId, name: staffName }];
    } else {
        const { data: allStaff } = await supabaseClient.from('staff').select('id,name').eq('status', 'active');
        targets = allStaff || [];
    }

    // Load existing records for the range to detect upsert vs insert
    const from = dates[0], to = dates[dates.length-1];
    const staffIds = targets.map(s => s.id);
    const { data: existing } = await supabaseClient.from('attendance').select('id,staff_id,date')
        .in('staff_id', staffIds).gte('date', from).lte('date', to);
    const exMap = {};
    (existing||[]).forEach(r => { exMap[r.staff_id + '_' + r.date] = r.id; });

    const ops = [];
    for (const s of targets) {
        for (const date of dates) {
            const key = s.id + '_' + date;
            if (exMap[key]) {
                ops.push(supabaseClient.from('attendance').update({ status, marked_by: currentUser.name }).eq('id', exMap[key]));
            } else {
                ops.push(supabaseClient.from('attendance').insert({ id: 'att_'+Date.now()+'_'+s.id.slice(-4)+Math.random().toString(36).slice(2,5), staff_id: s.id, staff_name: s.name, date, status, marked_by: currentUser.name }));
            }
        }
    }
    await Promise.all(ops);
}

function renderMonthCalendar(staff, records, month) {
    const [y, m] = month.split('-').map(Number);
    const daysInMonth = new Date(y, m, 0).getDate();
    const days = Array.from({length: daysInMonth}, (_, i) => i+1);
    // Map: staffId -> { day -> status }
    const map = {};
    records.forEach(r => {
        const day = parseInt(r.date.split('-')[2]);
        if (!map[r.staff_id]) map[r.staff_id] = {};
        map[r.staff_id][day] = r.status;
    });
    const STATUS_COLOR = { Present:'#22c55e', Absent:'#ef4444', 'Half Day':'#f59e0b', 'Paid Leave':'#6366f1', Holiday:'#94a3b8' };
    const STATUS_ABBR  = { Present:'P', Absent:'A', 'Half Day':'½', 'Paid Leave':'PL', Holiday:'H' };
    if (!staff.length) return '<p style="color:var(--text-muted)">No staff</p>';
    return `<table style="border-collapse:collapse;font-size:0.72rem;min-width:600px">
    <thead><tr>
        <th style="padding:4px 8px;text-align:left;border-bottom:2px solid var(--border)">Staff</th>
        ${days.map(d => {
            const dow = new Date(y, m-1, d).getDay();
            return `<th style="padding:4px 4px;text-align:center;border-bottom:2px solid var(--border);${dow===0?'color:#ef4444':''}">${d}<br><span style="font-size:0.65rem;font-weight:400">${['Su','Mo','Tu','We','Th','Fr','Sa'][dow]}</span></th>`;
        }).join('')}
        <th style="padding:4px 8px;border-bottom:2px solid var(--border)">Summary</th>
    </tr></thead>
    <tbody>${staff.map(s => {
        const sm = map[s.id] || {};
        let p=0,a=0,hd=0,pl=0;
        days.forEach(d => { const st=sm[d]; if(st==='Present')p++; else if(st==='Absent')a++; else if(st==='Half Day')hd++; else if(st==='Paid Leave')pl++; });
        const eff = p + hd*0.5 + pl;
        return `<tr>
            <td style="padding:4px 8px;font-weight:600;white-space:nowrap;border-bottom:1px solid var(--border)">${escapeHtml(s.name)}</td>
            ${days.map(d => {
                const st = sm[d];
                const col = STATUS_COLOR[st] || 'transparent';
                const dow = new Date(y, m-1, d).getDay();
                return `<td style="text-align:center;padding:2px;border-bottom:1px solid var(--border)">
                    <div style="width:22px;height:22px;border-radius:50%;background:${st?col:'rgba(0,0,0,0.04)'};display:flex;align-items:center;justify-content:center;margin:auto;cursor:pointer;font-size:0.65rem;font-weight:700;color:${st?'#fff':'var(--text-muted)'}" title="${st||'No record'}">${st?STATUS_ABBR[st]:(dow===0?'S':'')}</div>
                </td>`;
            }).join('')}
            <td style="padding:4px 8px;border-bottom:1px solid var(--border);white-space:nowrap;font-size:0.75rem">
                <b style="color:#22c55e">${p}P</b> <b style="color:#ef4444">${a}A</b> <b style="color:#f59e0b">${hd}½</b>
                <br><span style="color:var(--accent);font-weight:700">Eff: ${eff.toFixed(1)}</span>
            </td>
        </tr>`;
    }).join('')}</tbody></table>`;
}

async function markAttendance(staffId, staffName, date, status) {
    const { data: existing } = await supabaseClient.from('attendance').select('id').eq('staff_id', staffId).eq('date', date).single();
    if (existing) {
        await supabaseClient.from('attendance').update({ status, marked_by: currentUser.name }).eq('id', existing.id);
    } else {
        await supabaseClient.from('attendance').insert({ id: 'att_'+Date.now()+'_'+staffId.slice(-4), staff_id: staffId, staff_name: staffName, date, status, marked_by: currentUser.name });
    }
    renderAttendance();
}

async function markAllAttendance(status, date) {
    const { data: staff } = await supabaseClient.from('staff').select('id,name').eq('status', 'active');
    if (!staff || !staff.length) return;
    await Promise.all(staff.map(async s => {
        const { data: ex } = await supabaseClient.from('attendance').select('id').eq('staff_id', s.id).eq('date', date).single();
        if (ex) return supabaseClient.from('attendance').update({ status, marked_by: currentUser.name }).eq('id', ex.id);
        return supabaseClient.from('attendance').insert({ id: 'att_'+Date.now()+'_'+s.id.slice(-4)+Math.random().toString(36).slice(2,5), staff_id: s.id, staff_name: s.name, date, status, marked_by: currentUser.name });
    }));
    showToast(`All marked as ${status}!`, 'success');
    renderAttendance();
}

// ---- PAYROLL ----
async function renderHRPayroll() {
    const selMonth = window._payMonth || today().substring(0, 7);
    const [y, m] = selMonth.split('-').map(Number);
    const daysInMonth = new Date(y, m, 0).getDate();
    const workingDays = Array.from({length: daysInMonth}, (_, i) => i+1).filter(d => new Date(y, m-1, d).getDay() !== 0).length;

    const [staffRes, attRes, advRes, salRes] = await Promise.all([
        supabaseClient.from('staff').select('*').eq('status', 'active').order('name'),
        supabaseClient.from('attendance').select('*').gte('date', selMonth+'-01').lte('date', selMonth+'-31'),
        supabaseClient.from('salary_advances').select('*').order('date', { ascending: true }), // ALL advances (all months)
        supabaseClient.from('salary_records').select('*').eq('month', selMonth)
    ]);

    const staff = staffRes.data || [];
    const attRecs = attRes.data || [];
    const allAdvRecs = advRes.data || [];
    const salRecs = salRes.data || [];

    // Build per-staff summary
    const rows = staff.map(s => {
        const myAtt = attRecs.filter(r => r.staff_id === s.id);
        const p  = myAtt.filter(r => r.status === 'Present').length;
        const hd = myAtt.filter(r => r.status === 'Half Day').length;
        const pl = myAtt.filter(r => r.status === 'Paid Leave').length;
        const daysEff = p + hd * 0.5 + pl;
        const salary = s.monthly_salary || 0;
        const earned = workingDays > 0 ? +((salary / workingDays) * daysEff).toFixed(2) : 0;

        // Pending advance balance across ALL months (FIFO simulation)
        const myAllAdvs = allAdvRecs.filter(r => r.staff_id === s.id);
        const advPending = +myAllAdvs.reduce((t, a) => t + Math.max(0, (a.amount||0) - (a.deducted||0)), 0).toFixed(2);

        // Simulate how much will be deducted this month (FIFO)
        let toDeduct = 0, rem = earned;
        for (const adv of myAllAdvs) {
            if (rem <= 0) break;
            const bal = Math.max(0, (adv.amount||0) - (adv.deducted||0));
            const d = Math.min(bal, rem);
            toDeduct += d; rem -= d;
        }
        toDeduct = +toDeduct.toFixed(2);
        const net = +Math.max(0, earned - toDeduct).toFixed(2);
        const salRec = salRecs.find(r => r.staff_id === s.id);
        const paid = salRec && salRec.status === 'paid';

        let displayEarned = earned;
        let displayDeducted = toDeduct;
        let displayNet = net;

        if (paid) {
            displayEarned = salRec.earned_salary || earned;
            displayDeducted = salRec.advances || 0;
            displayNet = salRec.net_payable || (displayEarned - displayDeducted);
        }

        return { s, p, hd, pl, daysEff, earned: displayEarned, advPending, toDeduct: displayDeducted, net: displayNet, salRec, paid };
    });

    const totalEarned = rows.reduce((t, r) => t + r.earned, 0);
    const totalNet = rows.reduce((t, r) => t + r.net, 0);
    const totalAdvPending = rows.reduce((t, r) => t + r.advPending, 0);

    pageContent.innerHTML = `
    <div style="display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:16px">
        <input type="month" value="${selMonth}" onchange="window._payMonth=this.value;renderHRPayroll()" class="form-control" style="width:160px">
        <span style="font-size:0.82rem;color:var(--text-muted)">Working Days (excl. Sun): <strong>${workingDays}</strong></span>
        <button class="btn btn-outline btn-sm" onclick="navigateTo('attendance')">📅 Mark Attendance</button>
        <button class="btn btn-outline btn-sm" onclick="openStaffAdvance()">💵 Give Advance</button>
    </div>

    <div class="stats-grid" style="margin-bottom:16px">
        <div class="stat-card blue"><div class="stat-icon">👤</div><div class="stat-value">${staff.length}</div><div class="stat-label">Active Staff</div></div>
        <div class="stat-card green"><div class="stat-icon">💰</div><div class="stat-value">${currency(totalEarned)}</div><div class="stat-label">Total Earned</div></div>
        <div class="stat-card amber"><div class="stat-icon">💵</div><div class="stat-value">${currency(totalAdvPending)}</div><div class="stat-label">Advance Balance</div></div>
        <div class="stat-card red"><div class="stat-icon">🏦</div><div class="stat-value">${currency(totalNet)}</div><div class="stat-label">Net Payable</div></div>
    </div>

    <div class="card" style="margin-bottom:20px"><div class="card-body">
    <div style="font-weight:700;margin-bottom:12px">Salary Sheet — ${selMonth}</div>
    <div class="table-wrapper"><table class="data-table">
        <thead><tr><th>Staff</th><th>Salary/Mo</th><th style="text-align:center">P</th><th style="text-align:center">½</th><th style="text-align:center">Eff.Days</th><th style="text-align:right">Earned</th><th style="text-align:right">Adv Balance</th><th style="text-align:right">Deducting</th><th style="text-align:right">Net Payable</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>
        ${rows.length ? rows.map(r => `<tr style="${r.paid?'background:rgba(34,197,94,0.05)':''}">
            <td style="font-weight:600">${escapeHtml(r.s.name)}<div style="font-size:0.72rem;color:var(--text-muted)">${r.s.role||'Staff'}</div></td>
            <td>${currency(r.s.monthly_salary||0)}</td>
            <td style="text-align:center;color:#22c55e;font-weight:600">${r.p}</td>
            <td style="text-align:center;color:#f59e0b;font-weight:600">${r.hd}</td>
            <td style="text-align:center;font-weight:700">${r.daysEff.toFixed(1)}</td>
            <td style="text-align:right;font-weight:600">${currency(r.earned)}</td>
            <td style="text-align:right;color:${r.advPending>0?'#ef4444':'var(--text-muted)'}">
                ${r.advPending > 0 ? currency(r.advPending) : '-'}
            </td>
            <td style="text-align:right">
                ${!r.paid && r.advPending > 0
                    ? `<input type="number" id="deduct-${r.s.id}" value="${r.toDeduct}" min="0" max="${r.advPending}" step="1" class="form-control" style="width:90px;text-align:right;padding:4px 6px;font-size:0.85rem;color:#ef4444;font-weight:600" oninput="updateNetPayable('${r.s.id}',${r.earned})">`
                    : (r.toDeduct > 0 ? `<span style="color:#ef4444;font-weight:600">- ${currency(r.toDeduct)}</span>` : '<span style="color:var(--text-muted)">-</span>')}
            </td>
            <td style="text-align:right;font-weight:800;color:var(--accent);font-size:1rem"><span id="net-${r.s.id}">${currency(r.net)}</span></td>
            <td><span class="badge ${r.paid?'badge-success':'badge-warning'}">${r.paid?'Paid':'Pending'}</span></td>
            <td><div class="action-btns" style="gap:4px">
                ${!r.paid
                    ? `<button class="btn btn-primary btn-sm" onclick="markSalaryPaid('${r.s.id}','${escapeHtml(r.s.name)}','${selMonth}',${r.s.monthly_salary||0},${workingDays},${r.daysEff},${r.earned})">Mark Paid</button>`
                    : `<button class="btn btn-outline btn-sm" onclick="viewPaySlip('${r.s.id}','${selMonth}')">📄 Pay Slip</button>
                       <button class="btn btn-warning btn-sm" onclick="resetSalaryPaid('${r.s.id}','${escapeHtml(r.s.name)}','${selMonth}')">↩ Recalc</button>`
                }
                <button class="btn btn-outline btn-sm" onclick="openStaffAdvance('${r.s.id}','${escapeHtml(r.s.name)}')">+Advance</button>
            </div></td>
        </tr>`).join('') : '<tr><td colspan="11"><div class="empty-state"><p>No active staff</p></div></td></tr>'}
        </tbody>
    </table></div>
    </div></div>

    <h4 style="margin-bottom:12px;font-size:0.95rem">All Advance Records</h4>
    <div class="card"><div class="card-body">
    <div class="table-wrapper"><table class="data-table">
        <thead><tr><th>Staff</th><th>Date</th><th style="text-align:right">Given</th><th style="text-align:right">Deducted</th><th style="text-align:right">Balance</th><th>Notes</th><th>Action</th></tr></thead>
        <tbody>
        ${allAdvRecs.length ? allAdvRecs.map(a => {
            const bal = Math.max(0, (a.amount||0) - (a.deducted||0));
            const statusColor = bal === 0 ? '#22c55e' : bal < (a.amount||0) ? '#f59e0b' : '#ef4444';
            return `<tr>
                <td style="font-weight:600">${escapeHtml(a.staff_name||'')}</td>
                <td>${fmtDate(a.date)}<div style="font-size:0.72rem;color:var(--text-muted)">${a.month||''}</div></td>
                <td style="text-align:right;font-weight:700;color:#ef4444">${currency(a.amount||0)}</td>
                <td style="text-align:right;color:#22c55e">${(a.deducted||0)>0?currency(a.deducted):'-'}</td>
                <td style="text-align:right;font-weight:700;color:${statusColor}">${bal>0?currency(bal):'Cleared'}</td>
                <td style="color:var(--text-muted);font-size:0.85rem">${escapeHtml(a.notes||'-')}</td>
                <td>${bal>0?`<button class="btn-icon" onclick="deleteAdvance('${a.id}')" title="Delete">🗑️</button>`:''}</td>
            </tr>`;
        }).join('') : '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:20px">No advance records</td></tr>'}
        </tbody>
    </table></div>
    </div></div>`;
}

async function openStaffAdvance(staffId, staffName) {
    const { data: staff } = await supabaseClient.from('staff').select('id,name').eq('status', 'active').order('name');
    const selMonth = window._payMonth || today().substring(0, 7);
    openModal('Give Salary Advance', `
    <div class="form-group">
        <label>Staff Member *</label>
        <select id="f-adv-staff" class="form-control">
            <option value="">— Select —</option>
            ${(staff||[]).map(s => `<option value="${s.id}" data-name="${escapeHtml(s.name)}" ${staffId&&staffId===s.id?'selected':''}>${escapeHtml(s.name)}</option>`).join('')}
        </select>
    </div>
    <div class="form-row">
        <div class="form-group"><label>Date</label><input id="f-adv-date" type="date" class="form-control" value="${today()}"></div>
        <div class="form-group"><label>Amount (₹) *</label><input id="f-adv-amount" type="number" min="1" class="form-control" placeholder="0"></div>
    </div>
    <div class="form-group"><label>For Month</label><input id="f-adv-month" type="month" class="form-control" value="${selMonth}"></div>
    <div class="form-group"><label>Notes</label><input id="f-adv-notes" class="form-control" placeholder="Reason / notes..."></div>
    <div class="modal-actions">
        <button class="btn btn-outline" onclick="closeModal()">Cancel</button>
        <button class="btn btn-primary" onclick="saveAdvance()">Save Advance</button>
    </div>`);
}

async function saveAdvance() {
    const sel = $('f-adv-staff');
    const staffId = sel.value;
    const opt = sel.options[sel.selectedIndex];
    const staffName = opt ? opt.getAttribute('data-name') : '';
    if (!staffId) return alert('Select a staff member');
    const amount = +($('f-adv-amount').value || 0);
    if (!amount || amount <= 0) return alert('Enter a valid amount');
    const rec = {
        id: 'adv_' + Date.now(),
        staff_id: staffId, staff_name: staffName,
        date: $('f-adv-date').value,
        amount, month: $('f-adv-month').value,
        notes: ($('f-adv-notes').value||'').trim(),
        paid_by: currentUser.name
    };
    const { error } = await supabaseClient.from('salary_advances').insert(rec);
    if (error) return alert('Error: ' + error.message);
    closeModal(); showToast('Advance recorded!', 'success');
    renderHRPayroll();
}

async function deleteAdvance(id) {
    if (!confirm('Delete this advance record?')) return;
    await supabaseClient.from('salary_advances').delete().eq('id', id);
    showToast('Advance deleted', 'info');
    renderHRPayroll();
}

function updateNetPayable(staffId, earned) {
    const deductEl = document.getElementById('deduct-' + staffId);
    const netEl = document.getElementById('net-' + staffId);
    if (!deductEl || !netEl) return;
    const deduct = Math.min(Math.max(0, +deductEl.value || 0), earned);
    netEl.textContent = currency(Math.max(0, earned - deduct));
}

async function markSalaryPaid(staffId, staffName, month, monthlySalary, workingDays, daysEff, earned) {
    try {
        // Fetch ALL advances for this staff ordered oldest first (FIFO)
        const { data: allAdvs, error: advErr } = await supabaseClient
            .from('salary_advances').select('*')
            .eq('staff_id', staffId)
            .order('date', { ascending: true });
        
        if (advErr) throw advErr;

        const pendingAdvs = (allAdvs || []).filter(a => Math.max(0, (a.amount||0) - (a.deducted||0)) > 0);
        const totalPending = pendingAdvs.reduce((t, a) => t + Math.max(0, (a.amount||0) - (a.deducted||0)), 0);

        // Read admin-edited deduction amount (if input exists on page)
        const deductInput = document.getElementById('deduct-' + staffId);
        const requestedDeduction = deductInput
            ? Math.min(Math.max(0, +deductInput.value || 0), Math.min(earned, totalPending))
            : Math.min(earned, totalPending);

        // FIFO: deduct from oldest advance first, capped at requestedDeduction
        let remaining = requestedDeduction;
        let totalDeducted = 0;
        const advUpdates = []; // { id, newDeducted, deductNow, date, notes, total, oldDeducted }

        for (const adv of pendingAdvs) {
            if (remaining <= 0) break;
            const bal = Math.max(0, (adv.amount||0) - (adv.deducted||0));
            const deductNow = +Math.min(bal, remaining).toFixed(2);
            remaining = +(remaining - deductNow).toFixed(2);
            totalDeducted = +(totalDeducted + deductNow).toFixed(2);
            advUpdates.push({ id: adv.id, newDeducted: +((adv.deducted||0) + deductNow).toFixed(2), deductNow, date: adv.date, notes: adv.notes, total: adv.amount, oldDeducted: adv.deducted||0 });
        }

        const net = +Math.max(0, earned - totalDeducted).toFixed(2);
        const carryForward = +(totalPending - totalDeducted).toFixed(2);

        // Build confirmation message
        let confLines = [`Staff: ${staffName}`, `Earned: ${currency(earned)}`];
        if (totalDeducted > 0) {
            confLines.push(`Advance deducted: - ${currency(totalDeducted)}`);
            if (carryForward > 0) confLines.push(`Carry-forward balance: ${currency(carryForward)}`);
        }
        confLines.push(`Net Payable: ${currency(net)}`);
        if (!confirm(confLines.join('\n'))) return;

        // Update each advance's deducted amount
        const ops = advUpdates.map(u =>
            supabaseClient.from('salary_advances').update({ deducted: u.newDeducted }).eq('id', u.id)
        );

        // Upsert salary record
        const { data: existing } = await supabaseClient.from('salary_records').select('id').eq('staff_id', staffId).eq('month', month).single();
        const rec = { staff_id: staffId, staff_name: staffName, month, monthly_salary: monthlySalary, working_days: workingDays, days_present: daysEff, earned_salary: earned, advances: totalDeducted, net_payable: net, status: 'paid', paid_date: today(), paid_by: currentUser.name };
        if (existing) {
            ops.push(supabaseClient.from('salary_records').update(rec).eq('id', existing.id));
        } else {
            rec.id = 'sal_' + Date.now() + '_' + staffId.slice(-4);
            ops.push(supabaseClient.from('salary_records').insert(rec));
        }

        const results = await Promise.all(ops);
        const firstError = results.find(r => r.error);
        if (firstError) throw firstError.error;

        showToast(`Salary paid for ${staffName}! Net: ${currency(net)}${carryForward > 0 ? ' | Adv carry-forward: ' + currency(carryForward) : ''}`, 'success');
        renderHRPayroll();
    } catch (err) {
        console.error('Error marking salary as paid:', err);
        alert('Error: ' + err.message);
    }
}

async function resetSalaryPaid(staffId, staffName, month) {
    if (!confirm(`Reset salary for ${staffName} (${month}) back to Pending?\n\nThis will:\n• Mark salary as Unpaid\n• Reverse advance deductions for this month\n• Allow you to re-enter deduction amount and recalculate`)) return;

    // Load salary record to know how much was deducted
    const { data: sr } = await supabaseClient
        .from('salary_records').select('*')
        .eq('staff_id', staffId).eq('month', month).single();
    if (!sr) return alert('Salary record not found');

    // Load all advances for this staff oldest-first
    const { data: allAdvs } = await supabaseClient
        .from('salary_advances').select('*')
        .eq('staff_id', staffId)
        .order('date', { ascending: true });

    // Reverse FIFO: subtract the deducted amount back from advances (newest first among those that were deducted)
    let toReverse = +(sr.advances || 0);
    const ops = [];
    const deductedAdvs = [...(allAdvs || [])].filter(a => (a.deducted || 0) > 0).reverse(); // newest first for reversal
    for (const adv of deductedAdvs) {
        if (toReverse <= 0) break;
        const reverseNow = +Math.min(adv.deducted || 0, toReverse).toFixed(2);
        toReverse = +(toReverse - reverseNow).toFixed(2);
        const newDeducted = +Math.max(0, (adv.deducted || 0) - reverseNow).toFixed(2);
        ops.push(supabaseClient.from('salary_advances').update({ deducted: newDeducted }).eq('id', adv.id));
    }

    // Mark salary record as unpaid (keep record for audit, just reset status)
    ops.push(supabaseClient.from('salary_records').update({
        status: 'unpaid', paid_date: null, paid_by: null, advances: 0, net_payable: sr.earned_salary
    }).eq('id', sr.id));

    await Promise.all(ops);
    showToast(`Salary reset to Pending for ${staffName}. Advance deductions reversed.`, 'success');
    renderHRPayroll();
}

async function viewPaySlip(staffId, month) {
    const [{ data: s }, { data: sr }, { data: allAdvs }] = await Promise.all([
        supabaseClient.from('staff').select('*').eq('id', staffId).single(),
        supabaseClient.from('salary_records').select('*').eq('staff_id', staffId).eq('month', month).single(),
        supabaseClient.from('salary_advances').select('*').eq('staff_id', staffId).order('date', { ascending: true })
    ]);
    if (!sr) return alert('Salary record not found');
    const co = DB.getObj('db_company');

    // Build advance deduction rows for pay slip
    // Show advances that had deductions applied (deducted > 0) up to this month
    const advRows = (allAdvs || []).filter(a => (a.deducted||0) > 0).map(a => {
        const bal = Math.max(0, (a.amount||0) - (a.deducted||0));
        return `<tr style="color:#ef4444">
            <td style="padding:8px;border:1px solid var(--border)">
                Advance (${fmtDate(a.date)})${a.notes?' — '+a.notes:''}
                <div style="font-size:0.72rem;color:var(--text-muted)">Total: ${currency(a.amount)} | Deducted so far: ${currency(a.deducted)}${bal>0?' | Remaining: '+currency(bal):' | Cleared'}</div>
            </td>
            <td style="padding:8px;text-align:right;border:1px solid var(--border)">- ${currency(sr.advances)}</td>
        </tr>`;
    }).slice(0, 1); // show summary row (net advances in salary record is the total for this month)

    // Simpler: just show the total deducted this month from salary record
    const advDeductRow = sr.advances > 0 ? `<tr style="color:#ef4444">
        <td style="padding:8px;border:1px solid var(--border)">Advance Adjustment${(allAdvs||[]).filter(a=>(a.deducted||0)>0).length > 0 ? ' (oldest first)' : ''}</td>
        <td style="padding:8px;text-align:right;border:1px solid var(--border)">- ${currency(sr.advances)}</td>
    </tr>` : '';

    openModal('Pay Slip', `
    <div id="payslip-print">
        <div style="text-align:center;border-bottom:2px solid var(--border);padding-bottom:12px;margin-bottom:16px">
            <div style="font-size:1.2rem;font-weight:800">${escapeHtml(co.name||'Company')}</div>
            <div style="font-size:0.8rem;color:var(--text-muted)">${co.address||''}</div>
            <div style="margin-top:8px;font-size:1rem;font-weight:700;color:var(--accent)">SALARY SLIP — ${month}</div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.85rem;margin-bottom:16px">
            <div><span style="color:var(--text-muted)">Employee:</span> <strong>${escapeHtml(s.name)}</strong></div>
            <div><span style="color:var(--text-muted)">Role:</span> ${s.role||'Staff'}</div>
            <div><span style="color:var(--text-muted)">Month:</span> ${month}</div>
            <div><span style="color:var(--text-muted)">Paid Date:</span> ${fmtDate(sr.paid_date)}</div>
            <div><span style="color:var(--text-muted)">Phone:</span> ${s.phone||'-'}</div>
            <div><span style="color:var(--text-muted)">Paid By:</span> ${sr.paid_by||'-'}</div>
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:0.85rem;margin-bottom:12px">
            <tr style="background:var(--bg-page)"><th style="padding:8px;text-align:left;border:1px solid var(--border)">Description</th><th style="padding:8px;text-align:right;border:1px solid var(--border)">Amount</th></tr>
            <tr><td style="padding:8px;border:1px solid var(--border)">Monthly Salary</td><td style="padding:8px;text-align:right;border:1px solid var(--border)">${currency(sr.monthly_salary)}</td></tr>
            <tr><td style="padding:8px;border:1px solid var(--border)">Working Days (excl. Sun)</td><td style="padding:8px;text-align:right;border:1px solid var(--border)">${sr.working_days}</td></tr>
            <tr><td style="padding:8px;border:1px solid var(--border)">Days Present (effective)</td><td style="padding:8px;text-align:right;border:1px solid var(--border)">${(+sr.days_present).toFixed(1)}</td></tr>
            <tr style="background:rgba(34,197,94,0.07)"><td style="padding:8px;border:1px solid var(--border);font-weight:600">Earned Salary</td><td style="padding:8px;text-align:right;border:1px solid var(--border);font-weight:600">${currency(sr.earned_salary)}</td></tr>
            ${advDeductRow}
            <tr style="background:var(--accent);color:#fff"><td style="padding:10px;border:1px solid var(--border);font-weight:700;font-size:1rem">NET PAYABLE</td><td style="padding:10px;text-align:right;border:1px solid var(--border);font-weight:800;font-size:1.1rem">${currency(sr.net_payable)}</td></tr>
        </table>
        <div style="display:flex;justify-content:space-between;font-size:0.75rem;color:var(--text-muted);margin-top:16px;padding-top:12px;border-top:1px dashed var(--border)">
            <span>Employee Signature: ___________________</span>
            <span>Authorized: ___________________</span>
        </div>
    </div>
    <div class="modal-actions" style="margin-top:12px">
        <button class="btn btn-outline" onclick="closeModal()">Close</button>
        <button class="btn btn-primary" onclick="window.print()">🖨️ Print</button>
    </div>`);
}



