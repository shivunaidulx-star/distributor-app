const fs = require('fs');
let c = fs.readFileSync('app.js', 'utf8');

// Helper: normalize line endings for matching
function findReplace(content, searchStr, replaceStr, label) {
    // Try exact match first
    if (content.includes(searchStr)) {
        content = content.replace(searchStr, replaceStr);
        console.log('Applied:', label);
        return content;
    }
    // Try with \r\n normalization
    const searchNorm = searchStr.replace(/\r\n/g, '\n');
    const contentNorm = content.replace(/\r\n/g, '\n');
    if (contentNorm.includes(searchNorm)) {
        // Replace in normalized content then restore \r\n
        const replaceNorm = replaceStr.replace(/\r\n/g, '\n');
        const result = contentNorm.replace(searchNorm, replaceNorm);
        // Restore \r\n everywhere
        content = result.replace(/\n/g, '\r\n');
        console.log('Applied (normalized):', label);
        return content;
    }
    console.log('ERROR: not found -', label);
    process.exit(1);
}

// 1. Fix getCatalogScrollRoot
c = findReplace(c,
    `function getCatalogScrollRoot() {
    return document.querySelector('.main-content') || document.scrollingElement || document.documentElement;
}`,
    `function getCatalogScrollRoot() {
    // On mobile the page-content div is the actual scrolling container
    const pc = document.getElementById('page-content');
    if (pc && pc.scrollHeight > pc.clientHeight + 10) return pc;
    const mc = document.querySelector('.main-content');
    if (mc && mc.scrollHeight > mc.clientHeight + 10) return mc;
    return document.scrollingElement || document.documentElement;
}`,
    'getCatalogScrollRoot');

// 2. Fix scrollCatalogToTop - scroll page-content too
c = findReplace(c,
    `function scrollCatalogToTop() {
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
}`,
    `function scrollCatalogToTop() {
    // Scroll ALL possible scrollable containers to top
    // page-content is the real scroll container on mobile
    const pc = document.getElementById('page-content');
    if (pc) { try { pc.scrollTo({ top: 0, behavior: 'smooth' }); } catch(e) { pc.scrollTop = 0; } }
    const mc = document.querySelector('.main-content');
    if (mc) { try { mc.scrollTo({ top: 0, behavior: 'smooth' }); } catch(e) { mc.scrollTop = 0; } }
    try { window.scrollTo({ top: 0, behavior: 'smooth' }); } catch(e) { document.documentElement.scrollTop = 0; }
    setTimeout(() => updateCatalogTopButtonVisibility(), 400);
}`,
    'scrollCatalogToTop');

// 3. Fix updateCatalogTopButtonVisibility
c = findReplace(c,
    `function updateCatalogTopButtonVisibility() {
    const btn = $('catalog-top-btn');
    if (!btn) return;
    const mc = document.querySelector('.main-content');
    const y = Math.max(
        mc ? mc.scrollTop : 0,
        window.scrollY || 0,
        document.documentElement.scrollTop || 0
    );`,
    `function updateCatalogTopButtonVisibility() {
    const btn = $('catalog-top-btn');
    if (!btn) return;
    const pc = document.getElementById('page-content');
    const mc = document.querySelector('.main-content');
    const y = Math.max(
        pc ? pc.scrollTop : 0,
        mc ? mc.scrollTop : 0,
        window.scrollY || 0,
        document.documentElement.scrollTop || 0
    );`,
    'updateCatalogTopButtonVisibility');

// 4. Fix bindCatalogTopButton listeners
c = findReplace(c,
    `    // Listen on ALL possible scroll containers
    const mc = document.querySelector('.main-content');
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', onScroll, { passive: true });
    _catalogTopButtonBound = true;
}`,
    `    // Listen on ALL possible scroll containers (page-content is the real one on mobile)
    const pc = document.getElementById('page-content');
    const mc = document.querySelector('.main-content');
    if (pc) pc.addEventListener('scroll', onScroll, { passive: true });
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', onScroll, { passive: true });
    _catalogTopButtonBound = true;
}`,
    'bindCatalogTopButton listeners');

// 5. Fix getScrollY in bindCatalogStickyFilter
c = findReplace(c,
    `    const getScrollY = () => {
        const mc = document.querySelector('.main-content');
        if (mc && mc.scrollTop > 0) return mc.scrollTop;
        return window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    };`,
    `    const getScrollY = () => {
        const pc = document.getElementById('page-content');
        if (pc && pc.scrollTop > 0) return pc.scrollTop;
        const mc = document.querySelector('.main-content');
        if (mc && mc.scrollTop > 0) return mc.scrollTop;
        return window.scrollY || document.documentElement.scrollTop || document.body.scrollTop || 0;
    };`,
    'getScrollY in bindCatalogStickyFilter');

// 6. Fix bindCatalogStickyFilter listeners
c = findReplace(c,
    `    // Listen on ALL possible scroll containers
    const mc = document.querySelector('.main-content');
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    _catalogStickyBound = true;
}`,
    `    // Listen on ALL possible scroll containers (page-content is the real one on mobile)
    const pc = document.getElementById('page-content');
    const mc = document.querySelector('.main-content');
    if (pc) pc.addEventListener('scroll', onScroll, { passive: true });
    if (mc) mc.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('scroll', onScroll, { passive: true });
    _catalogStickyBound = true;
}`,
    'bindCatalogStickyFilter listeners');

fs.writeFileSync('app.js', c, 'utf8');
console.log('\nAll 6 fixes written to disk successfully!');
