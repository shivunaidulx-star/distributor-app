const fs = require('fs');
let c = fs.readFileSync('app.js', 'utf8');

// Fix 1: Replace scrollCatalogToTop to scroll ALL possible containers
const old1 = "function scrollCatalogToTop() {\r\n    const root = getCatalogScrollRoot();\r\n    try {\r\n        root.scrollTo({ top: 0, behavior: 'smooth' });\r\n    } catch (e) {\r\n        root.scrollTop = 0;\r\n    }\r\n    updateCatalogTopButtonVisibility();\r\n}";
const new1 = `function scrollCatalogToTop() {
    // Scroll ALL possible scrollable containers to top
    try {
        const mc = document.querySelector('.main-content');
        if (mc) mc.scrollTo({ top: 0, behavior: 'smooth' });
    } catch (e) {
        const mc = document.querySelector('.main-content');
        if (mc) mc.scrollTop = 0;
    }
    try {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    } catch (e) {
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
    }
    // Also scroll page-content if it's independently scrollable
    const pc = document.getElementById('page-content');
    if (pc) { try { pc.scrollTo({ top: 0, behavior: 'smooth' }); } catch(e) { pc.scrollTop = 0; } }
    setTimeout(() => updateCatalogTopButtonVisibility(), 400);
}`;
if (!c.includes(old1)) { console.log('ERROR: Fix 1 not found'); process.exit(1); }
c = c.replace(old1, new1);
console.log('Fix 1 applied: scrollCatalogToTop');

// Fix 2: Replace bindCatalogStickyFilter to listen on ALL scroll containers + use requestAnimationFrame
const old2 = "let _catalogStickyBound = false;\r\nlet _catalogLastScrollY = 0;\r\n\r\nfunction bindCatalogStickyFilter() {\r\n    if (_catalogStickyBound) return;\r\n    const root = getCatalogScrollRoot();\r\n    if (!root) return;\r\n\r\n    const getScrollY = () => {\r\n        if (root === document.scrollingElement || root === document.documentElement) {\r\n            return window.scrollY || document.documentElement.scrollTop || 0;\r\n        }\r\n        return root.scrollTop || 0;\r\n    };\r\n\r\n    const onScroll = () => {\r\n        if (currentPage !== 'catalog') return;\r\n        const stickyFilter = document.getElementById('catalog-sticky-filter');\r\n        if (!stickyFilter) return;\r\n\r\n        const y = getScrollY();\r\n        const isScrollingUp = y < _catalogLastScrollY && y > 200;\r\n        stickyFilter.classList.toggle('visible', isScrollingUp);\r\n        _catalogLastScrollY = y;\r\n\r\n        // Keep sticky filter pills in sync with actual filter state\r\n        if (isScrollingUp) {\r\n            const activeCat = document.querySelector('#catalog-pills .catalog-pill.active');\r\n            const activeMov = document.querySelector('#catalog-movement-pills .catalog-pill.active');\r\n            if (activeCat) {\r\n                const catVal = activeCat.dataset.cat || '';\r\n                stickyFilter.querySelectorAll('[data-cat]').forEach(p => {\r\n                    p.classList.toggle('active', (p.dataset.cat || '') === catVal);\r\n                });\r\n            }\r\n            if (activeMov) {\r\n                const movVal = activeMov.dataset.movement || '';\r\n                stickyFilter.querySelectorAll('[data-movement]').forEach(p => {\r\n                    p.classList.toggle('active', (p.dataset.movement || '') === movVal);\r\n                });\r\n            }\r\n        }\r\n    };\r\n\r\n    if (root === document.scrollingElement || root === document.documentElement) {\r\n        window.addEventListener('scroll', onScroll, { passive: true });\r\n    } else {\r\n        root.addEventListener('scroll', onScroll, { passive: true });\r\n    }\r\n    _catalogStickyBound = true;\r\n}";

const new2 = `let _catalogStickyBound = false;
let _catalogLastScrollY = 0;

function bindCatalogStickyFilter() {
    if (_catalogStickyBound) return;

    const getScrollY = () => {
        const mc = document.querySelector('.main-content');
        if (mc && mc.scrollTop > 0) return mc.scrollTop;
        return window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    };

    let _rafPending = false;
    const onScroll = () => {
        if (currentPage !== 'catalog') return;
        if (_rafPending) return;
        _rafPending = true;
        requestAnimationFrame(() => {
            _rafPending = false;
            const stickyFilter = document.getElementById('catalog-sticky-filter');
            if (!stickyFilter) return;

            const y = getScrollY();
            const delta = _catalogLastScrollY - y;
            const isScrollingUp = delta > 5 && y > 200;
            const isScrollingDown = delta < -5;
            
            if (isScrollingUp) {
                stickyFilter.classList.add('visible');
            } else if (isScrollingDown || y <= 100) {
                stickyFilter.classList.remove('visible');
            }
            _catalogLastScrollY = y;

            // Keep sticky filter pills in sync with actual filter state
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
            }
        });
    };

    // Listen on ALL possible scroll containers
    const mc = document.querySelector('.main-content');
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    _catalogStickyBound = true;
}`;
if (!c.includes(old2)) { console.log('ERROR: Fix 2 not found'); process.exit(1); }
c = c.replace(old2, new2);
console.log('Fix 2 applied: bindCatalogStickyFilter');

// Fix 3: Also fix bindCatalogTopButton to listen on all containers
const old3 = "function bindCatalogTopButton() {\r\n    if (_catalogTopButtonBound) return;\r\n    const root = getCatalogScrollRoot();\r\n    if (!root) return;\r\n\r\n    const onScroll = () => {\r\n        if (currentPage !== 'catalog') return;\r\n        updateCatalogTopButtonVisibility();\r\n    };\r\n\r\n    if (root === document.scrollingElement || root === document.documentElement) {\r\n        window.addEventListener('scroll', onScroll, { passive: true });\r\n    } else {\r\n        root.addEventListener('scroll', onScroll, { passive: true });\r\n    }\r\n    window.addEventListener('resize', onScroll, { passive: true });\r\n    _catalogTopButtonBound = true;\r\n}";

const new3 = `function bindCatalogTopButton() {
    if (_catalogTopButtonBound) return;

    const onScroll = () => {
        if (currentPage !== 'catalog') return;
        updateCatalogTopButtonVisibility();
    };

    // Listen on ALL possible scroll containers
    const mc = document.querySelector('.main-content');
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', onScroll, { passive: true });
    _catalogTopButtonBound = true;
}`;
if (!c.includes(old3)) { console.log('ERROR: Fix 3 not found'); process.exit(1); }
c = c.replace(old3, new3);
console.log('Fix 3 applied: bindCatalogTopButton');

// Fix 4: Also fix updateCatalogTopButtonVisibility to check all scroll containers
const old4 = "function updateCatalogTopButtonVisibility() {\r\n    const btn = $('catalog-top-btn');\r\n    if (!btn) return;\r\n    const root = getCatalogScrollRoot();\r\n    const y = (root === document.scrollingElement || root === document.documentElement)\r\n        ? (window.scrollY || document.documentElement.scrollTop || 0)\r\n        : (root.scrollTop || 0);\r\n    btn.classList.toggle('show', y > 600);\r\n    btn.classList.toggle('with-cart', !!document.getElementById('catalog-cart-bar'));\r\n}";

const new4 = `function updateCatalogTopButtonVisibility() {
    const btn = $('catalog-top-btn');
    if (!btn) return;
    const mc = document.querySelector('.main-content');
    const y = Math.max(
        mc ? mc.scrollTop : 0,
        window.scrollY || 0,
        document.documentElement.scrollTop || 0
    );
    btn.classList.toggle('show', y > 600);
    btn.classList.toggle('with-cart', !!document.getElementById('catalog-cart-bar'));
}`;
if (!c.includes(old4)) { console.log('ERROR: Fix 4 not found'); process.exit(1); }
c = c.replace(old4, new4);
console.log('Fix 4 applied: updateCatalogTopButtonVisibility');

fs.writeFileSync('app.js', c, 'utf8');
console.log('All fixes written to disk!');
