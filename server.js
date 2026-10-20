// Rink Deck server: static app + tiny sync API. No dependencies.
// Audio: /data/audio/<id> (+ <id>.type).  Library: /data/state.json  {rev, state}
'use strict';
const http = require('http');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { pipeline } = require('stream/promises');
const { execFile } = require('child_process');
const crypto = require('crypto');

const PORT = +process.env.PORT || 80;
const DATA = process.env.DATA_DIR || '/data';
const PUB = __dirname;
const AUDIO = path.join(DATA, 'audio');
const STATE = path.join(DATA, 'state.json');
const MAX_UPLOAD = 200 * 1024 * 1024;
const GC_GRACE_MS = 10 * 60 * 1000;      // never delete a file younger than this (upload may precede state push)
const ID_RE = /^[a-z0-9]{4,32}$/;
const TMP = path.join(DATA, 'tmp');
const YTDLP = process.env.YTDLP || 'yt-dlp';
const MAX_DURATION = +process.env.MAX_DURATION || 1200;   // seconds; refuses hour-long mixes
const MUSIC = process.env.MUSIC_DIR || '/music';            // read-only mount of the shared music library
const AUDIO_TYPES = { mp3: 'audio/mpeg', m4a: 'audio/mp4', aac: 'audio/aac', flac: 'audio/flac', ogg: 'audio/ogg', oga: 'audio/ogg', opus: 'audio/ogg', wav: 'audio/wav' };
const YT_HOST = /^(www\.|m\.|music\.)?(youtube\.com|youtu\.be)$/i;

const STATIC = {
  '/': 'index.html', '/index.html': 'index.html', '/sw.js': 'sw.js',
  '/manifest.webmanifest': 'manifest.webmanifest',
  '/icon-192.png': 'icon-192.png', '/icon-512.png': 'icon-512.png', '/icon-maskable-512.png': 'icon-maskable-512.png',
};
const MIME = { html: 'text/html; charset=utf-8', js: 'text/javascript', webmanifest: 'application/manifest+json', png: 'image/png' };

fs.mkdirSync(AUDIO, { recursive: true });
fs.rmSync(TMP, { recursive: true, force: true });
fs.mkdirSync(TMP, { recursive: true });
let lib = { rev: 0, state: null };
try { lib = JSON.parse(fs.readFileSync(STATE, 'utf8')); } catch (e) {}

const send = (res, code, body, headers = {}) => {
  const isObj = body !== null && typeof body === 'object';
  res.writeHead(code, { 'Content-Type': isObj ? 'application/json' : 'text/plain', 'Cache-Control': 'no-store', ...headers });
  res.end(isObj ? JSON.stringify(body) : body);
};
const readBody = (req, limit) => new Promise((resolve, reject) => {
  const chunks = []; let n = 0;
  req.on('data', c => { n += c.length; if (n > limit) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
  req.on('end', () => resolve(Buffer.concat(chunks)));
  req.on('error', reject);
});

async function writeAtomic(file, data) {
  const tmp = file + '.' + process.pid + '.tmp';
  await fsp.writeFile(tmp, data);
  await fsp.rename(tmp, file);
}

function referencedIds(state) {
  const ids = new Set();
  for (const b of (state && state.banks) || []) for (const p of b.pads || []) if (p.fileId) ids.add(p.fileId);
  return ids;
}
async function gc(state) {
  const keep = referencedIds(state), now = Date.now();
  for (const name of await fsp.readdir(AUDIO)) {
    const id = name.replace(/\.type$/, '');
    if (!ID_RE.test(id) || keep.has(id)) continue;
    try {
      const st = await fsp.stat(path.join(AUDIO, name));
      if (now - st.mtimeMs > GC_GRACE_MS) await fsp.unlink(path.join(AUDIO, name));
    } catch (e) {}
  }
}

async function listFiles() {
  const out = [];
  for (const name of await fsp.readdir(AUDIO)) {
    if (!ID_RE.test(name)) continue;       // skips .type and .tmp files
    try { out.push({ id: name, size: (await fsp.stat(path.join(AUDIO, name))).size }); } catch (e) {}
  }
  return out;
}

async function serveFile(req, res, id) {
  const file = path.join(AUDIO, id);
  let st; try { st = await fsp.stat(file); } catch (e) { return send(res, 404, 'not found'); }
  let type = 'application/octet-stream';
  try { type = (await fsp.readFile(file + '.type', 'utf8')) || type; } catch (e) {}
  const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': 'no-store' };
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  let start = 0, end = st.size - 1, code = 200;
  if (m && (m[1] || m[2])) {
    if (m[1]) { start = +m[1]; if (m[2]) end = Math.min(+m[2], end); } else { start = Math.max(0, st.size - +m[2]); }
    if (start > end) return send(res, 416, 'bad range', { 'Content-Range': 'bytes */' + st.size });
    code = 206; h['Content-Range'] = `bytes ${start}-${end}/${st.size}`;
  }
  h['Content-Length'] = end - start + 1;
  res.writeHead(code, h);
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(file, { start, end }).on('error', () => res.destroy()).pipe(res);
}

/* ---------- YouTube import (yt-dlp + ffmpeg) ---------- */
const lastLine = t => String(t || '').trim().split('\n').filter(Boolean).pop() || '';
const ytdlp = (args, ms) => new Promise((resolve, reject) => {
  execFile(YTDLP, ['--no-warnings', '--js-runtimes', 'node', ...args], { timeout: ms, maxBuffer: 50e6 }, (err, out, errout) => {
    if (err) return reject(new Error(err.code === 'ENOENT' ? 'yt-dlp is not installed on the server' : lastLine(errout) || err.message));
    resolve(out);
  });
});

// A YouTube link becomes a watch URL for that exact video; anything else is a search term.
function ytTarget(q) {
  q = String(q || '').trim().slice(0, 200);
  if (!q) throw new Error('Nothing to look up');
  if (!/^https?:\/\//i.test(q)) return { target: 'ytsearch1:' + q + ' audio' };
  let u; try { u = new URL(q); } catch (e) { throw new Error('Not a valid link'); }
  if (!YT_HOST.test(u.hostname)) throw new Error('Only YouTube links are supported');
  const m = u.hostname.toLowerCase().endsWith('youtu.be') ? /^\/([\w-]{11})/.exec(u.pathname)
    : (u.searchParams.get('v') || '').match(/^([\w-]{11})$/) || /^\/(?:shorts|embed|live)\/([\w-]{11})/.exec(u.pathname);
  if (!m) throw new Error('That link is not a single video');
  return { target: 'https://www.youtube.com/watch?v=' + m[1] };
}

let ytVersion = null;
const jobs = new Map();
let slots = 2; const slotQ = [];
const acquire = () => slots > 0 ? (slots--, Promise.resolve()) : new Promise(r => slotQ.push(r));
const release = () => { const n = slotQ.shift(); if (n) n(); else slots++; };

async function runDownload(job, vid) {
  await acquire();
  job.status = 'running';
  const dir = path.join(TMP, crypto.randomBytes(6).toString('hex'));
  try {
    await fsp.mkdir(dir, { recursive: true });
    const args = ['--no-playlist', '-f', 'bestaudio/best', '-x', '--audio-format', 'mp3', '--audio-quality', '192K',
      '--match-filter', 'duration<=?' + MAX_DURATION, '-o', path.join(dir, '%(id)s.%(ext)s'),
      '--', 'https://www.youtube.com/watch?v=' + vid];
    try { await ytdlp(args, 15 * 60 * 1000); }
    catch (e) { await new Promise(r => setTimeout(r, 2000)); await ytdlp(args, 15 * 60 * 1000); }   // YouTube throws the odd transient 403
    const f = (await fsp.readdir(dir)).find(n => n.endsWith('.mp3'));
    if (!f) throw new Error('Skipped: longer than ' + Math.round(MAX_DURATION / 60) + ' minutes');
    const id = crypto.randomBytes(5).toString('hex');
    await fsp.rename(path.join(dir, f), path.join(AUDIO, id));
    await fsp.writeFile(path.join(AUDIO, id + '.type'), 'audio/mpeg');
    job.status = 'done'; job.id = id;
  } catch (e) { job.status = 'error'; job.error = e.message; }
  finally { release(); fsp.rm(dir, { recursive: true, force: true }).catch(() => {}); }
}

async function handleYt(req, res, p) {
  const json = async () => { try { return JSON.parse((await readBody(req, 1e5)).toString()); } catch (e) { return null; } };
  if (p === '/api/yt/status' && req.method === 'GET') {
    try { ytVersion = ytVersion || (await ytdlp(['--version'], 15000)).trim(); return send(res, 200, { ok: true, version: ytVersion }); }
    catch (e) { return send(res, 200, { ok: false, error: e.message }); }
  }
  if (p === '/api/yt/resolve' && req.method === 'POST') {
    const b = await json(); if (!b) return send(res, 400, 'bad json');
    try {
      const out = await ytdlp(['--no-playlist', '--flat-playlist', '--skip-download', '--dump-single-json', ytTarget(b.q).target], 60000);
      const j = JSON.parse(out), e = j.entries ? j.entries[0] : j;
      if (!e || !/^[\w-]{11}$/.test(e.id || '')) return send(res, 200, { found: false });
      return send(res, 200, { found: true, vid: e.id, title: e.title || '', duration: e.duration || null });
    } catch (e) { return send(res, 200, { found: false, error: e.message }); }
  }
  if (p === '/api/yt/download' && req.method === 'POST') {
    const b = await json(); if (!b || !/^[\w-]{11}$/.test(b.vid || '')) return send(res, 400, 'bad video id');
    const id = crypto.randomBytes(6).toString('hex'), job = { status: 'queued' };
    jobs.set(id, job); setTimeout(() => jobs.delete(id), 3600e3).unref();
    runDownload(job, b.vid);
    return send(res, 200, { job: id });
  }
  const m = /^\/api\/yt\/job\/([a-f0-9]+)$/.exec(p);
  if (m && req.method === 'GET') { const j = jobs.get(m[1]); return j ? send(res, 200, j) : send(res, 404, 'unknown job'); }
  if (p === '/api/yt/update' && req.method === 'POST') {
    try {
      const out = await ytdlp(['-U'], 120000); ytVersion = null;
      return send(res, 200, { ok: true, message: lastLine(out) });
    } catch (e) { return send(res, 200, { ok: false, message: e.message }); }
  }
  send(res, 404, 'not found');
}

/* ---------- shared music library (read-only mount) ---------- */
// Layout is usually <Artist>/<Album>/<NN - Song>.mp3, so artist/album/title come from the path for searching;
// real tags (ffprobe) are read only for the songs that actually get imported.
let musicIdx = [], musicState = { scanning: false, at: null, error: null };
let musicByPath = new Map(), playlistIdx = [];       // lowercased path -> index entry; found .m3u/.m3u8 files
const plCache = new Map();                           // playlist path -> {mtime, at, res}
const extOf = n => path.extname(n).slice(1).toLowerCase();
const tidyFile = n => path.basename(n, path.extname(n)).replace(/_/g, ' ').replace(/^\s*\d{1,3}\s*[-._)]\s*|^\s*\d{1,3}\s+(?=\D)/, '').trim();

async function scanMusic() {
  if (musicState.scanning) return;
  musicState = { scanning: true, at: musicState.at, error: null };
  const out = [], pls = [], stack = [''];
  try {
    await fsp.access(MUSIC);
    while (stack.length) {
      const rel = stack.pop();
      for (const d of await fsp.readdir(path.join(MUSIC, rel), { withFileTypes: true })) {
        if (d.name.startsWith('.')) continue;                    // dotfiles, macOS ._ shadows, .AppleDouble
        const r = rel ? rel + '/' + d.name : d.name;
        if (d.isDirectory()) stack.push(r);
        else if (d.isFile() && /^m3u8?$/.test(extOf(d.name))) pls.push({ p: r, name: path.basename(d.name, path.extname(d.name)) });
        else if (d.isFile() && AUDIO_TYPES[extOf(d.name)]) {
          const parts = r.split('/'), title = tidyFile(d.name) || d.name;
          const artist = parts.length > 1 ? parts[0] : '', album = parts.length > 2 ? parts[parts.length - 2] : '';
          out.push({ p: r, title, artist, album, hay: (title + ' ' + parts.join(' ')).toLowerCase() });
        }
      }
    }
    musicIdx = out; playlistIdx = pls;
    musicByPath = new Map(out.map(e => [e.p.toLowerCase(), e]));
    plCache.clear();
    musicState = { scanning: false, at: Date.now(), error: null };
  } catch (e) {
    musicState = { scanning: false, at: musicState.at, error: e.code === 'ENOENT' ? 'Music folder is not mounted at ' + MUSIC : e.message };
  }
}

function searchMusic(q) {
  const terms = String(q || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (!terms.length) return [];
  const whole = terms.join(' '), hits = [];
  for (const e of musicIdx) {
    if (!terms.every(t => e.hay.includes(t))) continue;
    const tl = e.title.toLowerCase();
    hits.push({ e, score: tl === whole ? 0 : tl.startsWith(whole) ? 1 : tl.includes(whole) ? 2 : e.artist.toLowerCase().includes(whole) ? 3 : 4 });
  }
  hits.sort((a, b) => a.score - b.score || a.e.artist.localeCompare(b.e.artist) || a.e.p.localeCompare(b.e.p));
  return hits.slice(0, 60).map(h => ({ path: h.e.p, title: h.e.title, artist: h.e.artist, album: h.e.album }));
}

/* ---------- playlists (.m3u / .m3u8 files inside the library) ---------- */
// Entries are matched to indexed songs by path: first relative to the playlist's own folder, then by dropping leading
// path segments, so absolute paths written on another machine (/volume1/music/Artist/Album/x.mp3) still resolve.
function resolveEntry(dirRel, raw) {
  let f = raw.replace(/^file:\/\/(localhost)?/i, '').replace(/\\/g, '/');
  try { f = decodeURIComponent(f); } catch (e) {}
  const segs = f.split('/').filter(Boolean);
  const rel = f.startsWith('/') || /^[a-z]:/i.test(f) ? null : path.posix.normalize(dirRel ? dirRel + '/' + f : f);
  if (rel && !rel.startsWith('..')) { const e = musicByPath.get(rel.toLowerCase()); if (e) return e; }
  for (let i = 0; i < segs.length; i++) {
    const e = musicByPath.get(segs.slice(i).join('/').toLowerCase());
    if (e) return e;
  }
  return null;
}
function parseM3U(text, dirRel) {
  const seen = new Set(), tracks = []; let total = 0, missing = 0;
  for (let line of text.replace(/^﻿/, '').split(/\r?\n/)) {
    line = line.trim();
    if (!line || line.startsWith('#')) continue;
    total++;
    const e = resolveEntry(dirRel, line);
    if (!e) missing++;
    else if (!seen.has(e.p)) { seen.add(e.p); tracks.push(e.p); }
  }
  return { total, missing, tracks };
}
async function loadPlaylist(pl) {
  const st = await fsp.stat(path.join(MUSIC, pl.p)), c = plCache.get(pl.p);
  if (c && c.mtime === st.mtimeMs) return c.res;
  if (st.size > 5e6) throw new Error('too large');
  const text = await fsp.readFile(path.join(MUSIC, pl.p), 'utf8');
  const res = { path: pl.p, name: pl.name, folder: path.posix.dirname(pl.p) === '.' ? '' : path.posix.dirname(pl.p), ...parseM3U(text, path.posix.dirname(pl.p) === '.' ? '' : path.posix.dirname(pl.p)) };
  plCache.set(pl.p, { mtime: st.mtimeMs, res });
  return res;
}
async function searchPlaylists(q) {
  const terms = String(q || '').toLowerCase().split(/\s+/).filter(Boolean);
  const hits = playlistIdx.filter(p => terms.every(t => p.p.toLowerCase().includes(t)));
  hits.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
  const out = [];
  for (const pl of hits.slice(0, 100)) { try { out.push(await loadPlaylist(pl)); } catch (e) {} }
  return out;
}

const probeTags = file => new Promise(resolve => {
  execFile('ffprobe', ['-v', 'quiet', '-print_format', 'json', '-show_entries', 'format_tags', file], { timeout: 15000 }, (err, out) => {
    if (err) return resolve({});
    try {
      const raw = JSON.parse(out).format.tags || {}, t = {};
      for (const k of Object.keys(raw)) t[k.toLowerCase()] = String(raw[k]).trim();
      resolve({ title: t.title, artist: t.artist || t.album_artist, album: t.album });
    } catch (e) { resolve({}); }
  });
});

async function handleMusic(req, res, p, url) {
  if (p === '/api/music/status' && req.method === 'GET') {
    if (!musicState.at && !musicState.scanning && !musicState.error) scanMusic();
    return send(res, 200, { ok: !musicState.error, error: musicState.error, count: musicIdx.length, scanning: musicState.scanning, at: musicState.at });
  }
  if (p === '/api/music/rescan' && req.method === 'POST') {
    await scanMusic();
    return send(res, 200, { ok: !musicState.error, error: musicState.error, count: musicIdx.length });
  }
  if (p === '/api/music/search' && req.method === 'GET') return send(res, 200, searchMusic(url.searchParams.get('q')));
  if (p === '/api/music/playlists' && req.method === 'GET') return send(res, 200, await searchPlaylists(url.searchParams.get('q')));
  if (p === '/api/music/playlist/parse' && req.method === 'POST') {      // a playlist file picked on the device
    let b; try { b = JSON.parse((await readBody(req, 5e6)).toString()); } catch (e) { return send(res, 400, 'bad json'); }
    const name = String((b && b.name) || 'Playlist').replace(/\.m3u8?$/i, '').slice(0, 80);
    return send(res, 200, { path: '', name, folder: '', ...parseM3U(String((b && b.text) || ''), '') });
  }
  if (p === '/api/music/import' && req.method === 'POST') {
    let b; try { b = JSON.parse((await readBody(req, 1e5)).toString()); } catch (e) { return send(res, 400, 'bad json'); }
    const rel = String((b && b.path) || '');
    // Only paths we indexed; also resolve symlinks so nothing outside the mount can be read.
    if (!musicIdx.some(e => e.p === rel)) return send(res, 404, 'not in library');
    let src; try { src = await fsp.realpath(path.join(MUSIC, rel)); } catch (e) { return send(res, 404, 'file missing'); }
    if (!src.startsWith((await fsp.realpath(MUSIC)) + path.sep)) return send(res, 403, 'outside library');
    const id = crypto.randomBytes(5).toString('hex'), dst = path.join(AUDIO, id), tmp = dst + '.' + process.pid + '.tmp';
    try {
      await fsp.copyFile(src, tmp);
      await fsp.rename(tmp, dst);
      await fsp.writeFile(dst + '.type', AUDIO_TYPES[extOf(src)] || 'application/octet-stream');
    } catch (e) { fsp.unlink(tmp).catch(() => {}); return send(res, 500, 'copy failed'); }
    const tags = await probeTags(src), entry = musicIdx.find(e => e.p === rel);
    return send(res, 200, { id, title: tags.title || entry.title, artist: tags.artist || entry.artist, album: tags.album || entry.album });
  }
  send(res, 404, 'not found');
}

async function handleApi(req, res, url) {
  const p = url.pathname;
  if (p.startsWith('/api/yt/')) return handleYt(req, res, p);
  if (p.startsWith('/api/music/')) return handleMusic(req, res, p, url);
  if (p === '/api/state') {
    if (req.method === 'GET') return send(res, 200, lib);
    if (req.method === 'PUT') {
      let body; try { body = JSON.parse((await readBody(req, 5e6)).toString()); } catch (e) { return send(res, 400, 'bad json'); }
      if (!body || !Array.isArray(body.banks)) return send(res, 400, 'bad state');
      // optimistic concurrency: the client says which revision it started from; if someone else pushed since, hand back the current state to merge
      if (typeof body.baseRev === 'number' && lib.state && body.baseRev !== lib.rev) return send(res, 409, lib);
      delete body.baseRev;
      lib = { rev: lib.rev + 1, state: body };
      await writeAtomic(STATE, JSON.stringify(lib));
      gc(body).catch(() => {});
      return send(res, 200, { rev: lib.rev });
    }
  }
  if (p === '/api/files' && req.method === 'GET') return send(res, 200, await listFiles());
  const m = /^\/api\/files\/([^/]+)$/.exec(p);
  if (m) {
    const id = m[1];
    if (!ID_RE.test(id)) return send(res, 400, 'bad id');
    if (req.method === 'GET' || req.method === 'HEAD') return serveFile(req, res, id);
    if (req.method === 'PUT') {
      if (+req.headers['content-length'] > MAX_UPLOAD) return send(res, 413, 'too large');
      const file = path.join(AUDIO, id), tmp = file + '.' + process.pid + '.tmp';
      try {
        await pipeline(req, fs.createWriteStream(tmp));
        await fsp.rename(tmp, file);
        await fsp.writeFile(file + '.type', (req.headers['content-type'] || 'application/octet-stream').slice(0, 100));
      } catch (e) { fsp.unlink(tmp).catch(() => {}); return send(res, 500, 'upload failed'); }
      return send(res, 200, { id });
    }
    if (req.method === 'DELETE') {
      await fsp.unlink(path.join(AUDIO, id)).catch(() => {});
      await fsp.unlink(path.join(AUDIO, id + '.type')).catch(() => {});
      return send(res, 200, { id });
    }
  }
  send(res, 404, 'not found');
}

http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    if (url.pathname.startsWith('/api/')) return await handleApi(req, res, url);
    const name = STATIC[url.pathname];
    if (!name || (req.method !== 'GET' && req.method !== 'HEAD')) return send(res, 404, 'not found');
    const buf = await fsp.readFile(path.join(PUB, name));
    res.writeHead(200, {
      'Content-Type': MIME[name.split('.').pop()] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    res.end(req.method === 'HEAD' ? undefined : buf);
  } catch (e) {
    if (!res.headersSent) send(res, 500, 'server error'); else res.destroy();
  }
}).listen(PORT, () => { scanMusic(); console.log('Rink Deck on :' + PORT + ', data in ' + DATA); });
