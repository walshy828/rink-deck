// Cache the app shell so it opens with no signal (audio lives in IndexedDB, not here).
// Cache-first: a launch never waits on the network, so one bar of signal opens as fast as airplane mode.
// Each launch refreshes the cache in the background, so a new deploy shows up on the launch after.
const V = 'rinkdeck-v34';
const SHELL = ['./', 'index.html', 'manifest.webmanifest', 'icon-192.png', 'icon-512.png', 'icon-maskable-512.png'];
// iOS Safari refuses to open a page whose service-worker response is marked "redirected" (e.g. one that went
// through a proxy login hop), so a cold offline launch would fail. Re-wrap such a response as a plain one.
const clean = r => r && r.redirected ? r.blob().then(b => new Response(b, { status: r.status, statusText: r.statusText, headers: r.headers })) : r;
self.addEventListener('install', e => {
  // Install fails (and the old worker keeps serving) unless every file is a real 200 straight from this server.
  e.waitUntil(caches.open(V).then(c => Promise.all(SHELL.map(u => fetch(new Request(u, { cache: 'reload' })).then(r => {
    if (!r.ok || r.redirected) throw new Error('bad shell response ' + u);
    return c.put(u, r);
  })))).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== V).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const req = e.request, url = new URL(req.url);
  if (req.method !== 'GET' || url.origin !== location.origin) return;
  if (url.pathname.includes('/api/')) return;   // sync traffic goes straight to the network
  const nav = req.mode === 'navigate', key = nav ? 'index.html' : req;   // every page load is the one-page app
  // Only a real 200 from this server replaces the cache: never a proxy error page (502 while the server is down) or a login redirect.
  const net = fetch(req).then(r => {
    if (r.ok && r.type === 'basic' && !r.redirected) { const copy = r.clone(); caches.open(V).then(c => c.put(key, copy)); }
    return r;
  });
  e.waitUntil(net.then(() => {}, () => {}));
  e.respondWith(caches.match(key, { ignoreSearch: nav }).then(hit => hit || net.catch(() => nav ? caches.match('index.html') : Response.error())).then(r => nav ? clean(r) : r));
});
