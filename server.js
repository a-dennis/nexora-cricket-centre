'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const engine = require('./engine');

const PORT = Number(process.env.PORT || 3000);
const PUB = __dirname;
const MAX_MATCHES = 300;
const CREATE_LIMIT = 15; // per IP per hour

// ---------- storage ----------
function makeStore() {
  if (process.env.DATABASE_URL) {
    const { Pool } = require('pg');
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false }, max: 4 });
    const ready = pool.query(`CREATE TABLE IF NOT EXISTS cricket_matches (
      id TEXT PRIMARY KEY, config JSONB NOT NULL, events JSONB NOT NULL DEFAULT '[]', key_hash TEXT NOT NULL,
      rev INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
    const row = (r) => r && { id: r.id, config: r.config, events: r.events, keyHash: r.key_hash, rev: r.rev, updated: r.updated_at };
    return {
      kind: 'durable',
      async get(id) { await ready; const r = await pool.query('SELECT * FROM cricket_matches WHERE id=$1', [id]); return row(r.rows[0]); },
      async count() { await ready; const r = await pool.query('SELECT count(*)::int AS n FROM cricket_matches'); return r.rows[0].n; },
      async create(m) { await ready; await pool.query('INSERT INTO cricket_matches(id,config,events,key_hash) VALUES($1,$2,$3,$4)', [m.id, JSON.stringify(m.config), '[]', m.keyHash]); },
      async update(id, events, expectRev) {
        await ready;
        const r = await pool.query('UPDATE cricket_matches SET events=$1, rev=rev+1, updated_at=now() WHERE id=$2 AND rev=$3 RETURNING rev', [JSON.stringify(events), id, expectRev]);
        return r.rowCount === 1;
      },
    };
  }
  // Temporary fallback (file in /tmp): survives restarts of the process, not redeploys.
  const file = process.env.STORE_FILE || path.join(require('os').tmpdir(), 'cricket-matches.json');
  let db = {};
  try { db = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { db = {}; }
  const save = () => { try { fs.writeFileSync(file + '.tmp', JSON.stringify(db)); fs.renameSync(file + '.tmp', file); } catch (e) { console.error('save failed', e.message); } };
  return {
    kind: 'temporary',
    async get(id) { const m = db[id]; return m ? JSON.parse(JSON.stringify(m)) : null; },
    async count() { return Object.keys(db).length; },
    async create(m) { db[m.id] = { id: m.id, config: m.config, events: [], keyHash: m.keyHash, rev: 0, updated: new Date().toISOString() }; save(); },
    async update(id, events, expectRev) { const m = db[id]; if (!m || m.rev !== expectRev) return false; m.events = events; m.rev++; m.updated = new Date().toISOString(); save(); return true; },
  };
}
const store = makeStore();

// ---------- helpers ----------
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const rnd = (n) => crypto.randomBytes(n).toString('base64url').replace(/[-_]/g, 'x').slice(0, n);
const hits = new Map();
function limited(ip) {
  const now = Date.now(); const arr = (hits.get(ip) || []).filter((t) => now - t < 3600e3);
  if (arr.length >= CREATE_LIMIT) { hits.set(ip, arr); return true; }
  arr.push(now); hits.set(ip, arr); return false;
}
function send(res, code, obj, headers) {
  const body = JSON.stringify(obj);
  res.writeHead(code, Object.assign({ 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }, headers || {}));
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = ''; req.on('data', (c) => { b += c; if (b.length > 20000) { reject(Object.assign(new Error('Too large'), { user: true })); req.destroy(); } });
    req.on('end', () => { try { resolve(b ? JSON.parse(b) : {}); } catch (e) { reject(Object.assign(new Error('Bad JSON'), { user: true })); } });
    req.on('error', reject);
  });
}
function publicView(m) {
  return { id: m.id, rev: m.rev, updated: m.updated, config: m.config, state: engine.view(m.config, m.events), storage: store.kind };
}
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
const STATIC_OK = new Set(['index.html']);
function serveStatic(res, rel) {
  if (!STATIC_OK.has(rel)) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Not found'); }
  const f = path.join(PUB, rel);
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Not found'); }
    res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
    res.end(d);
  });
}

async function handle(req, res) {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  if (p === '/healthz') return send(res, 200, { ok: true, storage: store.kind });
  if (p === '/api/info') return send(res, 200, { storage: store.kind });
  let mt;
  if (req.method === 'POST' && p === '/api/matches') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (limited(ip)) return send(res, 429, { error: 'Too many matches created from this connection. Try again later.' });
    if ((await store.count()) >= MAX_MATCHES) return send(res, 507, { error: 'Storage limit reached for the free trial.' });
    const config = engine.normalizeConfig(await readBody(req));
    const id = rnd(8).toLowerCase(), key = rnd(14);
    await store.create({ id, config, keyHash: sha(key) });
    return send(res, 201, { id, scorerKey: key, viewerPath: '/m/' + id, scorerPath: '/s/' + id, storage: store.kind });
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})$/)) && req.method === 'GET') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (url.searchParams.get('rev') === String(m.rev)) return send(res, 200, { unchanged: true, rev: m.rev });
    return send(res, 200, publicView(m));
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/(event|undo)$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    const key = req.headers['x-scorer-key'] || '';
    const a = Buffer.from(sha(key)), b = Buffer.from(m.keyHash);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return send(res, 403, { error: 'This link cannot score. Use the scorer link.' });
    const body = await readBody(req);
    if (body.rev !== undefined && body.rev !== m.rev) return send(res, 409, { error: 'Match changed on another device. Refreshed.', match: publicView(m) });
    let events = m.events.slice();
    if (mt[2] === 'undo') {
      if (!events.length) return send(res, 400, { error: 'Nothing to undo' });
      events.pop();
    } else {
      const ev = body.event;
      const st = engine.replay(m.config, events);
      const er = engine.validate(m.config, st, ev);
      if (er) return send(res, 400, { error: er });
      const clean = ev.t === 'sel' ? { t: 'sel', role: ev.role, player: ev.player }
        : ev.t === 'ball' ? Object.assign({ t: 'ball', r: ev.r, x: ev.x || '' }, ev.w ? { w: { k: ev.w.k, who: ev.w.who, f: engine.cleanName(ev.w.f, 24) } } : {})
        : { t: 'end' };
      events.push(clean);
      if (events.length > 800) return send(res, 400, { error: 'Too many events for one match' });
    }
    const ok = await store.update(m.id, events, m.rev);
    if (!ok) { const m2 = await store.get(m.id); return send(res, 409, { error: 'Match changed on another device. Refreshed.', match: publicView(m2) }); }
    const m3 = await store.get(m.id);
    return send(res, 200, publicView(m3));
  }
  if (p.startsWith('/api/')) return send(res, 404, { error: 'Unknown API' });
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  if (p === '/' || /^\/(m|s)\/[a-z0-9]+\/?$/.test(p)) return serveStatic(res, 'index.html');
  return serveStatic(res, p.slice(1));
}

http.createServer((req, res) => {
  handle(req, res).catch((e) => {
    if (e && e.user) return send(res, 400, { error: e.message });
    console.error(e); send(res, 500, { error: 'Server error' });
  });
}).listen(PORT, () => console.log('Cricket centre on :' + PORT + ' storage=' + store.kind));
