# Go-Live QA Signoff

Date: `2026-04-06`

Environment: `localhost:8081` UAT

Login used: `shivu`

Observed app banner during runtime retest: `v151`

Note: this sheet reflects the current localhost working tree. The payment-grouping fix and item-save unlock fix are local `app.js` changes verified in runtime during this session.

| Test | Expected | Actual | Status |
| --- | --- | --- | --- |
| Cold boot to login | App loads login screen without crash | Login screen rendered, no schema errors seen in runtime report | PASS |
| Login to dashboard | Admin can sign in and land on dashboard | `shivu / 889282` login succeeded, current page `dashboard` | PASS |
| Core page navigation smoke | Main pages open with no JS/network failures | `18/18` pages opened; `0` console errors, `0` JS exceptions, `0` network failures from `_qa_runtime_report.json` | PASS |
| Add new item | Item saves and appears in inventory | `QA AUTO ITEM 20260405171156` saved as id `2d10f38c-ebf6-43bb-a10a-b8fc772d063a` | PASS |
| Save lock after item save | After item save, next transaction should work without refresh | Runtime retest created `QA FLOW ITEM 1775446217149`; `_isSaving=false` after save | PASS |
| Create sales order | Sales order saves once and appears in list | `SO-0006/26-27` created earlier; runtime retest also created `SO-0007/26-27` with `_isSaving=false` after save | PASS |
| Approve sales order | Approved order updates status and audit fields | `SO-0006/26-27` status `approved`, `approvedBy=shivu` | PASS |
| Create direct sale invoice | Direct invoice posts once with correct totals | `INV-0013/26-27` / `PT-0001/26-27` created, total `24`, status `posted` | PASS |
| Receive payment against invoice | Payment should increase paid amount and reduce outstanding | Earlier failure reproduced with `PR-0001/26-27`; after local fix, live page retest shows `INV-0013/26-27 paid=24` and payment groups no longer collide on reused pay numbers | PASS |
| Packing save | Packed order saves package details and history | `SO-0007/26-27` approved, packed by `shivu`, `boxCount=1`, package `QA-BOX-1775446476670`, `packedItems=1` | PASS |
| Dispatch from delivery | Dispatch should save and update invoice delivery status | `INV-0013/26-27` dispatched to `BALU`, delivery row `a8086ca3-3e79-4186-a2da-3c2233b07d9b` created successfully | PASS |
| Delivered / undelivered flow | Delivery outcome should persist with reason/status | Same delivery row moved to `Delivered` on `2026-04-06`, location `QA delivered`, invoice delivery-status map also resolved to `Delivered` | PASS |
| Entries Only reset | Transactions clear while masters remain | Reproduced failure from UI reset: `salesOrders`, `delivery`, `stockLedger`, `partyLedger` cleared, but `invoices=6` and `payments=10` remained. Root cause isolated to broken DB RPC `admin_reset_app_data()` returning `DELETE requires a WHERE clause`. App now fails loudly instead of false success, but DB SQL fix is still required. | FAIL |
| Purchase / stock intake | Purchase-side stock flow should save and reflect in stock | Not executed in this QA pass | PENDING |
| Staff / attendance / payroll write flow | Add/update payroll records without save error | Pages open successfully, but write flow not executed in this QA pass | PENDING |
| Customer portal OTP / registration | Registration and OTP should work end to end | Not executed in this QA pass | PENDING |

## Summary

- Passed now: boot, login, page navigation smoke, item create, post-item save unlock, sales order create, sales order approve, packing save, direct invoice create, direct invoice payment allocation refresh, dispatch, delivered status propagation.
- Pending manual/live-risk flows: purchase intake, payroll write path, customer portal OTP, optional undelivered/returned exception handling.
- Blocking DB fix still required: apply [migration_admin_rpc_reset_fix.sql](c:/Users/Admin/.gemini/antigravity/scratch/distributor-app/migration_admin_rpc_reset_fix.sql) before retesting reset.

## Signoff

Tested by: `Codex + shivu UAT session`

Ready status: `Conditional / Blocked on reset SQL`

Condition: apply the reset-RPC SQL fix, retest `Entries Only` reset, then complete the remaining pending business flows above.
