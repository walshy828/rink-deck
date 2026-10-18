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

async function handleApi(req, res, url) {
  const p = url.pathname;
  if (p.startsWith('/api/yt/')) return handleYt(req, res, p);
  if (p === '/api/state') {
    if (req.method === 'GET') return send(res, 200, lib);
    if (req.method === 'PUT') {
      let body; try { body = JSON.parse((await readBody(req, 5e6)).toString()); } catch (e) { return send(res, 400, 'bad json'); }
      if (!body || !Array.isArray(body.banks)) return send(res, 400, 'bad state');
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
}).listen(PORT, () => console.log('Rink Deck on :' + PORT + ', data in ' + DATA));
