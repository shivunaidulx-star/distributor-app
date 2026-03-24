// Delivery Mobile Cards Generators
function renderReadyToDispatchCards(orders) {
    if (!orders.length) return '';
    return orders.map(o => `
        <div style="background:var(--bg-card);border:1px solid var(--border);border-left:3px solid #f59e0b;border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05)">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
                <div style="display:flex;align-items:center;gap:8px;flex:1;min-width:0;padding-right:8px">
                    <input type="checkbox" class="chk-disp-row" value="${o.id}" data-source="${o.source}" data-orderno="${o.orderNo}" data-party="${escapeHtml(o.partyName)}" style="width:18px;height:18px">
                    <div style="font-weight:700;font-size:1rem;color:var(--text-primary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(o.partyName)}</div>
                </div>
                <div style="font-size:0.75rem;color:var(--text-muted)">#${o.orderNo}</div>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;padding-left:26px">
                <span class="amount-green" style="font-size:1.05rem;font-weight:800">${currency(o.total)}</span>
                <span class="badge badge-success" style="font-size:0.65rem">Inv: ${o.invoiceNo || '-'}</span>
            </div>
            <div style="display:flex;justify-content:flex-end;padding-top:10px;border-top:1px solid var(--border)">
                <button class="btn btn-primary btn-sm" style="padding:4px 10px;font-size:0.75rem" onclick="openDispatchModalUnified('${o.id}','${o.source}')"> Dispatch</button>
            </div>
        </div>
    `).join('');
}

function renderDelCards(dels, allParties) {
    if (!dels.length) return '<div style="text-align:center;padding:30px;color:var(--text-muted)">No deliveries found</div>';
    return dels.map(d => {
        const bdgClass = d.status === 'Delivered' ? 'badge-success' : (d.status === 'Dispatched' ? 'badge-info' : (d.status === 'Cancelled' ? 'badge-danger' : 'badge-warning'));
        const isSalesman = currentUser && currentUser.role === 'Salesman';
        return `
        <div data-del-id="${d.id}" style="background:var(--bg-card);border:1px solid var(--border);border-radius:12px;padding:12px 14px;margin-bottom:10px;box-shadow:0 1px 4px rgba(0,0,0,0.05);position:relative" onclick="viewDelivery('${d.id}')">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px">
                <div style="font-weight:700;font-size:0.95rem;color:var(--text-primary);padding-right:8px">${escapeHtml(d.partyName)}</div>
                <span class="badge ${bdgClass}" style="font-size:0.65rem;padding:2px 6px">${d.status}</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                <div style="font-size:0.75rem;color:var(--text-muted);display:flex;flex-direction:column;gap:2px">
                    <span>Date: ${fmtDate(d.date)}</span>
                    <span style="color:#3b82f6;font-weight:600">By: ${d.deliveryPerson || '-'}</span>
                </div>
                <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px">
                    <span style="font-size:0.75rem;color:var(--text-muted)">Order: #${d.orderNo}</span>
                    <span style="font-size:0.75rem;color:var(--success)">Inv: ${d.invoiceNo || '-'}</span>
                </div>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;padding-top:10px;border-top:1px solid var(--border)">
                <div style="display:flex;gap:4px">
                    <button class="btn btn-outline btn-sm" style="padding:4px 8px;font-size:0.75rem;border-radius:6px" onclick="event.stopPropagation();printDeliverySlip('${d.id}')">🖨️ Print</button>
                    ${d.status === 'Dispatched' && !isSalesman ? `<button class="btn btn-primary btn-sm" style="padding:4px 8px;font-size:0.75rem;border-radius:6px" onclick="event.stopPropagation();updateDelStatus('${d.id}')">Update</button>` : ''}
                </div>
                ${canEdit() && d.status !== 'Delivered' && d.status !== 'Cancelled' ? `<button class="btn btn-outline btn-sm" style="padding:4px 8px;font-size:0.75rem;border-radius:6px;border-color:var(--danger);color:var(--danger)" onclick="event.stopPropagation();cancelDelivery('${d.id}')">Cancel</button>` : ''}
            </div>
        </div>`;
    }).join('');
}
