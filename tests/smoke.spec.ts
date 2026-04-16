import { test, expect } from '@playwright/test';

const userId = process.env.E2E_USER;
const pin = process.env.E2E_PIN;

test.skip(!userId || !pin, 'E2E_USER and E2E_PIN must be set to run smoke tests.');

async function login(page) {
  await page.goto('/');
  await page.waitForSelector('#login-userid');
  await page.fill('#login-userid', userId || '');
  await page.fill('#login-pin', pin || '');
  await page.click('#btn-login');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 30000 });
}

test('login / inventory / order to payment / invoices', async ({ page }) => {
  await login(page);

  const seed = await page.evaluate(async () => {
    const suffix = Date.now().toString();
    const partyName = `E2E Party ${suffix}`;
    const itemName = `E2E Item ${suffix}`;

    let categories = await DB.getAll('categories');
    if (!categories.length) {
      const cat = await DB.insert('categories', { name: 'General', subCategories: [] });
      categories = [cat];
    }

    let uoms = await DB.getAll('uom');
    if (!uoms.length) {
      const u = await DB.insert('uom', { name: 'Pcs' });
      uoms = [u];
    }

    const party = await DB.insert('parties', { name: partyName, type: 'Customer', balance: 0 });
    const item = await DB.insert('inventory', {
      name: itemName,
      category: categories[0].name,
      subCategory: '',
      itemCode: `E2E-${suffix.slice(-6)}`,
      unit: uoms[0].name || 'Pcs',
      purchasePrice: 10,
      salePrice: 15,
      mrp: 20,
      stock: 25,
      lowStockAlert: 2,
      warehouse: 'Main Warehouse',
      priceTiers: [],
      batches: []
    });

    const qty = 1;
    const price = +(item.salePrice || 15);
    const amount = +(qty * price).toFixed(2);
    const gstRate = +(item.gstRate || 0);
    const baseAmount = gstRate > 0 ? +(amount / (1 + gstRate / 100)).toFixed(2) : amount;
    const taxAmount = +(amount - baseAmount).toFixed(2);

    const line = {
      itemId: item.id,
      name: item.name,
      qty,
      price,
      listedPrice: price,
      purchasePrice: +(item.purchasePrice || 0),
      discountAmt: 0,
      discountPct: 0,
      amount,
      unit: item.unit || 'Pcs',
      primaryQty: qty,
      gstRate,
      baseAmount,
      taxAmount,
      hsn: item.hsn || ''
    };

    const orderNo = await nextNumber('SO-');
    const orderRow = await DB.insert('salesorders', {
      date: today(),
      expectedDeliveryDate: null,
      priority: 'Normal',
      partyId: party.id,
      partyName: party.name,
      items: [line],
      total: amount,
      discountPct: 0,
      discountAmt: 0,
      notes: 'E2E smoke order',
      orderNo,
      status: 'approved',
      createdBy: currentUser?.name || 'System',
      packed: false
    });

    const invoiceNo = await nextNumber('INV-');
    const vyaparInvoiceNo = await nextNumber('PT-26-27-');
    const invoiceRow = await DB.insert('invoices', {
      invoiceNo,
      vyaparInvoiceNo,
      date: today(),
      type: 'sale',
      partyId: party.id,
      partyName: party.name,
      items: [line],
      subtotal: amount,
      gst: 0,
      discountAmt: 0,
      roundOff: 0,
      total: amount,
      status: 'posted',
      createdBy: currentUser?.name || 'System',
      fromOrder: orderNo
    });

    const payNo = await nextNumber('PAY-IN');
    const paymentRow = await DB.insert('payments', {
      payNo,
      date: today(),
      type: 'in',
      partyId: party.id,
      partyName: party.name,
      amount,
      discount: 0,
      totalReduction: amount,
      mode: 'Cash',
      note: 'E2E smoke payment',
      invoiceNo,
      allocations: { [invoiceNo]: amount },
      collectedBy: currentUser?.name || 'System',
      createdBy: currentUser?.userId || 'System',
      status: 'posted'
    });

    return {
      partyId: party.id,
      itemId: item.id,
      orderId: orderRow?.id || null,
      invoiceId: invoiceRow?.id || null,
      paymentId: paymentRow?.id || null,
      partyName,
      itemName,
      orderNo,
      invoiceNo,
      payNo
    };
  });

  await page.locator('.nav-item[data-page="inventory"]').click();
  await page.waitForSelector('#inv-search');
  await page.fill('#inv-search', seed.itemName);
  await expect(page.getByText(seed.itemName)).toBeVisible();

  await page.locator('.nav-item[data-page="salesorders"]').click();
  await page.waitForSelector('#so-search');
  await page.fill('#so-search', seed.orderNo);
  await expect(page.getByText(seed.orderNo)).toBeVisible();

  await page.locator('.nav-item[data-page="invoices"]').click();
  await page.waitForSelector('#inv-search2');
  await page.fill('#inv-search2', seed.invoiceNo);
  await expect(page.getByText(seed.invoiceNo)).toBeVisible();

  await page.locator('.nav-item[data-page="payments"]').click();
  await page.waitForSelector('#pay-search');
  await page.fill('#pay-search', seed.payNo);
  await expect(page.getByText(seed.payNo)).toBeVisible();

  await page.evaluate(async (ids) => {
    const safeDelete = async (table, id) => {
      if (!id) return;
      try { await DB.delete(table, id); } catch (err) { console.warn('Cleanup failed', table, id, err?.message || err); }
    };
    await safeDelete('payments', ids.paymentId);
    await safeDelete('invoices', ids.invoiceId);
    await safeDelete('salesorders', ids.orderId);
    await safeDelete('inventory', ids.itemId);
    await safeDelete('parties', ids.partyId);
  }, {
    partyId: seed.partyId,
    itemId: seed.itemId,
    orderId: seed.orderId,
    invoiceId: seed.invoiceId,
    paymentId: seed.paymentId
  });
});
