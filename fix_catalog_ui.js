const fs = require('fs');

function findReplace(content, searchStr, replaceStr, label) {
    if (content.includes(searchStr)) {
        content = content.replace(searchStr, replaceStr);
        console.log('Applied:', label);
        return content;
    }
    const searchNorm = searchStr.replace(/\r\n/g, '\n');
    const contentNorm = content.replace(/\r\n/g, '\n');
    if (contentNorm.includes(searchNorm)) {
        const replaceNorm = replaceStr.replace(/\r\n/g, '\n');
        const result = contentNorm.replace(searchNorm, replaceNorm);
        content = result.replace(/\n/g, '\r\n');
        console.log('Applied (normalized):', label);
        return content;
    }
    console.log('ERROR: not found -', label);
    // process.exit(1);
    return content;
}

let appJs = fs.readFileSync('app.js', 'utf8');

// 1. ADD updateCatalogStickyFilter function to app.js
const updateFunc = `
function updateCatalogStickyFilter() {
    const stickyFilter = document.getElementById('catalog-sticky-filter');
    if (!stickyFilter) return;
    
    const movPillsEl = document.getElementById('catalog-movement-pills');
    const catPillsEl = document.getElementById('catalog-pills');
    const subPillsEl = document.getElementById('catalog-subcat-pills');
    
    stickyFilter.innerHTML = '';
    
    if (catPillsEl && catPillsEl.innerHTML.trim() !== '') {
        const catClone = catPillsEl.cloneNode(true);
        catClone.removeAttribute('id');
        catClone.style.cssText = 'display:flex;gap:6px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px;width:100%;';
        catClone.querySelectorAll('.catalog-pill').forEach(pill => {
            const cat = pill.dataset.cat || '';
            pill.onclick = () => filterCatalogByCat(cat);
        });
        stickyFilter.appendChild(catClone);
    }
    
    if (subPillsEl && subPillsEl.innerHTML.trim() !== '') {
        const subClone = subPillsEl.cloneNode(true);
        subClone.removeAttribute('id');
        subClone.style.cssText = 'display:flex;gap:6px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px;width:100%;';
        subClone.querySelectorAll('.catalog-pill').forEach(pill => {
            const subcat = pill.dataset.subcat || '';
            pill.onclick = () => filterCatalogBySubcat(subcat);
        });
        stickyFilter.appendChild(subClone);
    }
    
    if (movPillsEl && movPillsEl.innerHTML.trim() !== '') {
        const movClone = movPillsEl.cloneNode(true);
        movClone.removeAttribute('id');
        movClone.style.cssText = 'display:flex;gap:6px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px;width:100%;';
        movClone.querySelectorAll('.catalog-pill').forEach(pill => {
            const movement = pill.dataset.movement || '';
            pill.onclick = () => filterCatalogByMovement(movement);
        });
        stickyFilter.appendChild(movClone);
    }
}
`;

// Insert the new function before bindCatalogTopButton
appJs = findReplace(appJs, `function bindCatalogTopButton() {`, updateFunc + `\nfunction bindCatalogTopButton() {`, 'Insert updateCatalogStickyFilter');

// 2. Modify renderCatalog so it calls updateCatalogStickyFilter instead of cloning manually
const oldRenderClone = `    // Clone category and movement pills into sticky filter
    const movPillsEl = document.getElementById('catalog-movement-pills');
    const catPillsEl = document.getElementById('catalog-pills');
    if (stickyFilter && (movPillsEl || catPillsEl)) {
        stickyFilter.innerHTML = '';
        if (catPillsEl) {
            const catClone = catPillsEl.cloneNode(true);
            catClone.removeAttribute('id');
            catClone.style.cssText = 'display:flex;gap:6px;flex-shrink:0;';
            catClone.querySelectorAll('.catalog-pill').forEach(pill => {
                const cat = pill.dataset.cat || '';
                pill.onclick = () => filterCatalogByCat(cat);
            });
            stickyFilter.appendChild(catClone);
        }
        if (movPillsEl) {
            const movClone = movPillsEl.cloneNode(true);
            movClone.removeAttribute('id');
            movClone.style.cssText = 'display:flex;gap:6px;flex-shrink:0;';
            movClone.querySelectorAll('.catalog-pill').forEach(pill => {
                const movement = pill.dataset.movement || '';
                pill.onclick = () => filterCatalogByMovement(movement);
            });
            stickyFilter.appendChild(movClone);
        }
    }`;

appJs = findReplace(appJs, oldRenderClone, `    updateCatalogStickyFilter();`, 'Replace renderCatalog cloning with function call');


// 3. Remove the pill syncing logic from bindCatalogStickyFilter onScroll and rely on filterCatalog calling it
const oldOnScrollSync = `            // Keep sticky filter pills in sync with actual filter state
            if (stickyFilter.classList.contains('visible')) {
                const activeCat = document.querySelector('#catalog-pills .catalog-pill.active');
                const activeMov = document.querySelector('#catalog-movement-pills .catalog-pill.active');
                if (activeCat) {
                    const catVal = activeCat.dataset.cat || '';
                    stickyFilter.querySelectorAll('[data-cat]').forEach(p => {
                        p.classList.toggle('active', (p.dataset.cat || '') === catVal);
                    });
                }
                if (activeMov) {
                    const movVal = activeMov.dataset.movement || '';
                    stickyFilter.querySelectorAll('[data-movement]').forEach(p => {
                        p.classList.toggle('active', (p.dataset.movement || '') === movVal);
                    });
                }
            }`;

appJs = findReplace(appJs, oldOnScrollSync, `            // Sticky filter sync is now handled by updateCatalogStickyFilter on filter change`, 'Remove manual onScroll sync');

// 4. Update filterCatalog to call updateCatalogStickyFilter
const oldFilterEnd = `    const grid = $('catalog-grid');
    if (grid) grid.innerHTML = await renderCatalogCards(items);
    updateCatalogTopButtonVisibility();
}`;
const newFilterEnd = `    const grid = $('catalog-grid');
    if (grid) grid.innerHTML = await renderCatalogCards(items);
    updateCatalogTopButtonVisibility();
    updateCatalogStickyFilter();
}`;
appJs = findReplace(appJs, oldFilterEnd, newFilterEnd, 'Add updateCatalogStickyFilter to filterCatalog');

fs.writeFileSync('app.js', appJs, 'utf8');


// 5. Fix CSS style.css
let styleCss = fs.readFileSync('style.css', 'utf8');

const oldStickyCss = `.catalog-sticky-filter {
    position: fixed;
    top: -60px;
    left: 0;
    right: 0;
    z-index: 850;
    background: rgba(255,255,255,0.96);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    border-bottom: 1px solid rgba(249,115,22,0.15);
    box-shadow: 0 4px 20px rgba(0,0,0,0.08);
    padding: 8px 16px;
    transition: top 0.3s cubic-bezier(0.16,1,0.3,1);
    display: flex;
    gap: 8px;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}`;

const newStickyCss = `.catalog-sticky-filter {
    position: fixed;
    top: -120px; /* Hide further up since it may be taller */
    left: 0;
    right: 0;
    z-index: 850;
    background: rgba(255,255,255,0.96);
    backdrop-filter: blur(14px);
    -webkit-backdrop-filter: blur(14px);
    border-bottom: 1px solid rgba(249,115,22,0.15);
    box-shadow: 0 4px 20px rgba(0,0,0,0.08);
    padding: 8px 16px;
    transition: top 0.3s cubic-bezier(0.16,1,0.3,1);
    display: flex;
    flex-direction: column;
    gap: 8px;
}`;
styleCss = findReplace(styleCss, oldStickyCss, newStickyCss, 'Fix .catalog-sticky-filter CSS');

// 6. Fix .catalog-add-btn width
const oldAddBtnCss = `.catalog-add-btn { padding: 6px 14px; font-size: 0.78rem; }`;
const newAddBtnCss = `.catalog-add-btn { padding: 6px 14px; font-size: 0.78rem; width: 100%; }`;
styleCss = findReplace(styleCss, oldAddBtnCss, newAddBtnCss, 'Fix .catalog-add-btn width');

fs.writeFileSync('style.css', styleCss, 'utf8');

console.log('Script completed');
