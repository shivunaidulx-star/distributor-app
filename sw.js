const CACHE = 'distromanager-v16';
const STATIC = ['./', './index.html', './style.css', './app.js', './manifest.json'];

// Helper to ignore query parameters (like ?v=67) for cache matching
function cleanURL(url) {
    const u = new URL(url);
    u.search = '';
    return u.toString();
}

self.addEventListener('install', e => {
    e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)));
    self.skipWaiting();
});

self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

self.addEventListener('fetch', e => {
    const url = e.request.url;
    // Skip external APIs and CDNs
    if (url.includes('supabase.co') || url.includes('fonts.') || url.includes('cdn.')) return;

    // Stale-While-Revalidate Strategy
    e.respondWith(
        caches.open(CACHE).then(cache => {
            const cleanedReq = cleanURL(url);
            return cache.match(cleanedReq).then(cachedRes => {
                const fetchPromise = fetch(e.request).then(networkRes => {
                    if (networkRes && networkRes.status === 200) {
                        cache.put(cleanedReq, networkRes.clone());
                    }
                    return networkRes;
                }).catch(() => cachedRes); // Fallback to cache if network fails entirely

                return cachedRes || fetchPromise;
            });
        })
    );
});
