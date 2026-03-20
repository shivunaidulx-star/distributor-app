const CACHE = 'distromanager-v34';
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

    // Network-First Strategy
    // This ensures that if the user is online, they ALWAYS get the latest code.
    // If they are offline, we fall back to the cache.
    e.respondWith(
        fetch(e.request)
            .then(networkRes => {
                const resClone = networkRes.clone();
                caches.open(CACHE).then(cache => {
                    cache.put(cleanURL(url), resClone);
                });
                return networkRes;
            })
            .catch(() => {
                return caches.match(cleanURL(url));
            })
    );
});
