const CACHE = 'distromanager-v13';
const STATIC = ['/', '/index.html', '/style.css', '/app.js', '/manifest.json'];

self.addEventListener('install', e => {
    e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)));
    self.skipWaiting(); // Activate new SW immediately
});

self.addEventListener('activate', e => {
    // Delete old caches
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
        )
    );
    self.clients.claim(); // Take control of all pages immediately
});

self.addEventListener('fetch', e => {
    // Always network-first for HTML/JS/CSS so updates are instant
    if (e.request.url.includes('supabase.co') ||
        e.request.url.includes('fonts.') ||
        e.request.url.includes('cdn.')) return;

    e.respondWith(
        fetch(e.request)
            .then(res => {
                // Update cache with fresh response
                const clone = res.clone();
                caches.open(CACHE).then(c => c.put(e.request, clone));
                return res;
            })
            .catch(() => caches.match(e.request)) // Offline fallback
    );
});
