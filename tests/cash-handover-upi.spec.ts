import { test, expect } from '@playwright/test';

const userId = process.env.E2E_USER;
const pin = process.env.E2E_PIN;

test.skip(!userId || !pin, 'E2E_USER and E2E_PIN must be set to run cash handover smoke tests.');

async function login(page) {
  await page.goto('/');
  await page.waitForSelector('#login-userid');
  await page.fill('#login-userid', userId || '');
  await page.fill('#login-pin', pin || '');
  await page.click('#btn-login');
  await page.waitForSelector('#app:not(.hidden)', { timeout: 30000 });
}

test('upi confirmation and cash handover flow', async ({ page }) => {
  await login(page);

  let seed: {
    partyId: string | null;
    upiPaymentId: string | null;
    cashPaymentId: string | null;
    upiPayNo: string;
    cashPayNo: string;
  } | null = null;

  try {
    seed = await page.evaluate(async () => {
      const suffix = Date.now().toString();
      const party = await DB.insert('parties', {
        name: `E2E Cash Party ${suffix}`,
        type: 'Customer',
        balance: 0
      });
      const upiPayNo = `E2E-UPI-${suffix.slice(-6)}`;
      const cashPayNo = `E2E-CASH-${suffix.slice(-6)}`;
      const proofDataUrl = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4////fwAJ+wP9KobjigAAAABJRU5ErkJggg==';

      const upiPayment = await DB.insert('payments', {
        payNo: upiPayNo,
        date: today(),
        type: 'in',
        partyId: party.id,
        partyName: party.name,
        amount: 123,
        discount: 0,
        totalReduction: 123,
        mode: 'UPI',
        note: 'E2E UPI payment',
        invoiceNo: 'Advance',
        allocations: {},
        collectedBy: currentUser?.name || 'System',
        createdBy: currentUser?.userId || 'System',
        upiRef: `UTR${suffix.slice(-8)}`,
        attachmentUrl: proofDataUrl,
        attachmentName: 'proof.png',
        verificationStatus: 'pending',
        status: 'posted'
      });

      const cashPayment = await DB.insert('payments', {
        payNo: cashPayNo,
        date: getYesterdayDate(),
        type: 'in',
        partyId: party.id,
        partyName: party.name,
        amount: 500,
        discount: 0,
        totalReduction: 500,
        mode: 'Cash',
        note: 'E2E cash handover payment',
        invoiceNo: 'Advance',
        allocations: {},
        collectedBy: currentUser?.name || 'System',
        createdBy: currentUser?.userId || 'System',
        status: 'posted'
      });

      return {
        partyId: party?.id || null,
        upiPaymentId: upiPayment?.id || null,
        cashPaymentId: cashPayment?.id || null,
        upiPayNo,
        cashPayNo
      };
    });

    await page.locator('.nav-item[data-page="payments"]').click();
    await page.waitForSelector('#pay-search');

    await page.evaluate((paymentId) => {
      viewPaymentDetails(paymentId);
    }, seed.upiPaymentId);
    await expect(page.getByText('Payment Receipt')).toBeVisible();
    await expect(page.locator('#modal-body').getByText('UPI Pending')).toBeVisible();
    await page.getByRole('button', { name: /Confirm UPI/i }).click();
    await expect(page.locator('#modal-body').getByText('UPI Confirmed')).toBeVisible();
    await page.getByRole('button', { name: /^Close$/ }).click();

    await page.getByRole('button', { name: /Cash Handover/i }).first().click();
    await expect(page.getByText('Cash Handovers')).toBeVisible();
    await page.getByRole('button', { name: /New Handover/i }).click();
    await expect(page.getByText('Submit Cash Handover')).toBeVisible();
    await expect(page.getByText(seed.cashPayNo)).toBeVisible();
    await page.locator('[data-denom-prefix="cash-handover-submit"][data-denom-value="500"]').fill('1');
    await page.getByRole('button', { name: /Submit Handover/i }).click();

    const submittedRow = page.locator('#tbl-cash-handovers-body tr').filter({ hasText: seed.cashPayNo }).first();
    await expect(submittedRow).toBeVisible();
    await expect(submittedRow).toContainText('Submitted');
    await submittedRow.locator('button[title="Confirm Received"]').click();

    await expect(page.getByText('Confirm Cash Received')).toBeVisible();
    await page.locator('[data-denom-prefix="cash-handover-admin"][data-denom-value="500"]').fill('1');
    await page.getByRole('button', { name: /Confirm Received/i }).click();

    const receivedRow = page.locator('#tbl-cash-handovers-body tr').filter({ hasText: seed.cashPayNo }).first();
    await expect(receivedRow).toBeVisible();
    await expect(receivedRow).toContainText('Received');
  } finally {
    if (seed) {
      await page.evaluate(async (ids) => {
        const handovers = await DB.getAll('cash_handovers');
        const related = (handovers || []).filter(row => Array.isArray(row.paymentRefs) && row.paymentRefs.includes(ids.cashPayNo));
        for (const row of related) {
          try { await DB.delete('cash_handovers', row.id); } catch (err) { console.warn('Cleanup failed cash_handovers', row.id, err?.message || err); }
        }
        const safeDelete = async (table, id) => {
          if (!id) return;
          try { await DB.delete(table, id); } catch (err) { console.warn('Cleanup failed', table, id, err?.message || err); }
        };
        await safeDelete('payments', ids.upiPaymentId);
        await safeDelete('payments', ids.cashPaymentId);
        await safeDelete('parties', ids.partyId);
      }, seed);
    }
  }
});
