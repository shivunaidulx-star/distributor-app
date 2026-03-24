// Packing Mobile Cards Generators
function renderReadyToPackCards(orders, isAdmin) {
    if (!orders.length) return '<div style="text-align:center;padding:30px;color:var(--text-muted)">No orders waiting</div>';
    return orders.map(o => `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05)">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
                <div style="font-weight:700;font-size:1rem;color:var(--text-primary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(o.partyName)}</div>
                <div style="font-size:0.75rem;color:var(--text-muted)">#${o.orderNo}</div>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span class="amount-green" style="font-size:1.1rem;font-weight:800">${currency(o.total)}</span>
                <span style="font-size:0.75rem;color:var(--text-muted)">Items: ${o.items.length}</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;padding-top:10px;border-top:1px solid var(--border)">
                <div>${o.assignedPacker ? `<span class="badge badge-info" style="font-size:0.65rem">${o.assignedPacker}</span>` : '<span class="badge badge-warning" style="font-size:0.65rem">Unassigned</span>'}</div>
                <div style="display:flex;gap:6px">
                    ${!o.assignedPacker && isAdmin ? `<button class="btn btn-outline btn-sm" style="padding:4px 10px;font-size:0.75rem" onclick="openAssignPackerModal('${o.id}')"> Assign</button>` : ''}
                    ${!o.assignedPacker && !isAdmin ? `<button class="btn btn-outline btn-sm" style="padding:4px 10px;font-size:0.75rem" onclick="selfAssign('${o.id}')"> Self Assign</button>` : ''}
                    ${o.assignedPacker === currentUser.name || isAdmin ? `<button class="btn btn-primary btn-sm" style="padding:4px 10px;font-size:0.75rem" onclick="startPacking('${o.id}')"> Start</button>` : ''}
                </div>
            </div>
        </div>
    `).join('');
}

function renderCannotCompleteCards(orders, isAdmin) {
    if (!orders.length) return '';
    return orders.map(o => `
        <div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05)">
            <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                <div style="font-weight:700;color:#991b1b">${escapeHtml(o.partyName)} (#${o.orderNo})</div>
                <span class="badge badge-danger" style="font-size:0.65rem">${o.cannotCompleteReason || 'Flagged'}</span>
            </div>
            <div style="font-size:0.8rem;color:#7f1d1d;margin-bottom:8px">${o.cannotCompleteNotes || '-'}</div>
            <div style="font-size:0.75rem;color:#991b1b;margin-bottom:8px">By: <strong>${o.cannotCompleteBy || '-'}</strong></div>
            ${isAdmin ? `<div style="display:flex;gap:6px;padding-top:8px;border-top:1px solid #fca5a5">
                <button class="btn btn-outline btn-sm" style="flex:1;border-color:#b91c1c;color:#b91c1c" onclick="clearCannotComplete('${o.id}')">Retry</button>
                <button class="btn btn-danger btn-sm" style="flex:1" onclick="cancelOrderFromPacking('${o.id}')">Cancel Order</button>
            </div>` : ''}
        </div>
    `).join('');
}

function renderPackedNoInvoiceCards(orders, isAdmin) {
    if (!orders.length) return '';
    return orders.map(o => `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-left:3px solid #3b82f6;border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05)">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
                <div style="font-weight:700;font-size:0.95rem;color:var(--text-primary)">${escapeHtml(o.partyName)}</div>
                <div style="font-size:0.75rem;color:var(--text-muted)">#${o.orderNo}</div>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <span class="amount-green" style="font-size:1.05rem;font-weight:800">${currency(o.packedTotal || o.total)}</span>
                <span style="font-size:0.75rem;color:#3b82f6;font-weight:600">${o.boxCount ? o.boxCount + ' Boxes' : 'Packed'}</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;padding-top:10px;border-top:1px solid var(--border)">
                <div style="font-size:0.75rem;color:var(--text-muted)">By: ${o.packedBy || '-'} ${o.packingDurationMins !== undefined ? '(' + o.packingDurationMins + 'm)' : ''}</div>
                <div>${isAdmin ? `<button class="btn btn-outline btn-sm" style="padding:4px 10px;font-size:0.75rem" onclick="generateInvoiceFromPacked('${o.id}')"> Generate Invoice</button>` : '<span class="badge badge-warning" style="font-size:0.65rem">Awaiting Admin</span>'}</div>
            </div>
        </div>
    `).join('');
}

function renderPackedHistoryCards(orders) {
    if (!orders.length) return '<div style="text-align:center;padding:20px;color:var(--text-muted)">No history</div>';
    return orders.map(o => {
        const pkgs = o.packageNumbers || [];
        const bc = o.boxCount || 0;
        const cc = o.crateCount || 0;
        const boxes = o.boxNumbers || (bc > 0 ? pkgs.slice(0, bc) : []);
        const crates = o.crateNumbers || (cc > 0 ? pkgs.slice(bc, bc + cc) : []);
        const boxDisplay = boxes.length ? boxes.map(n => `<span class="badge badge-outline" style="font-size:0.65rem;margin:1px;padding:2px 4px">${escapeHtml(n)}</span>`).join(' ') : '-';
        const crateDisplay = crates.length ? crates.map(n => `<span class="badge badge-outline" style="font-size:0.65rem;margin:1px;padding:2px 4px;border-color:var(--warning);color:var(--warning)">${escapeHtml(n)}</span>`).join(' ') : '-';
        return `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05)">
            <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                <div style="font-weight:700;font-size:0.95rem;color:var(--text-primary)">${escapeHtml(o.partyName)}</div>
                <div style="font-size:0.75rem;color:var(--text-muted)">Inv: ${o.invoiceNo || '-'}</div>
            </div>
            <div style="display:flex;justify-content:space-between;margin-bottom:8px">
                <span class="amount-green" style="font-size:1.05rem;font-weight:800">${currency(o.total)}</span>
                <span style="font-size:0.75rem;color:var(--text-muted)">#${o.orderNo}</span>
            </div>
            <div style="display:flex;justify-content:space-between;padding-top:8px;border-top:1px solid var(--border)">
                <div style="font-size:0.75rem;color:var(--text-muted)">By: ${o.packedBy || '-'}</div>
                <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
                    ${boxes.length ? `<div style="font-size:0.75rem">📦 ${boxDisplay}</div>` : ''}
                    ${crates.length ? `<div style="font-size:0.75rem">📋 ${crateDisplay}</div>` : ''}
                </div>
            </div>
        </div>`;
    }).join('');
}
