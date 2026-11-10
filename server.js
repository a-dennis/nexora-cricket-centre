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
function makePgStore() {
    const { Pool } = require('pg');
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false }, max: 4 });
    const ready = pool.query(`CREATE TABLE IF NOT EXISTS cricket_matches (
      id TEXT PRIMARY KEY, config JSONB NOT NULL, events JSONB NOT NULL DEFAULT '[]', key_hash TEXT NOT NULL,
      rev INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
    const tready = pool.query(`CREATE TABLE IF NOT EXISTS cricket_tournaments (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, key_hash TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`).then(() => pool.query("ALTER TABLE cricket_tournaments ADD COLUMN IF NOT EXISTS teams JSONB NOT NULL DEFAULT '[]', ADD COLUMN IF NOT EXISTS reg_key_hash TEXT, ADD COLUMN IF NOT EXISTS listed BOOLEAN NOT NULL DEFAULT false"));
    const sready = pool.query('CREATE TABLE IF NOT EXISTS cricket_settings (k TEXT PRIMARY KEY, v TEXT)');
    const kready = pool.query('CREATE TABLE IF NOT EXISTS cricket_match_keys (id TEXT PRIMARY KEY, key_hash TEXT NOT NULL)');
    const fready = pool.query('CREATE TABLE IF NOT EXISTS cricket_fb_manifest (id TEXT PRIMARY KEY, kind TEXT NOT NULL, since TIMESTAMPTZ NOT NULL DEFAULT now())');
    const nready = pool.query('CREATE TABLE IF NOT EXISTS cricket_replica_snapshots (seq BIGSERIAL PRIMARY KEY, entity_id TEXT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT now(), payload JSONB NOT NULL)');
    const row = (r) => r && { id: r.id, config: r.config, events: r.events, keyHash: r.key_hash, rev: r.rev, updated: r.updated_at };
    return {
      kind: 'durable',
      async matchKeySet(id, keyHash) { await kready; await pool.query('INSERT INTO cricket_match_keys(id,key_hash) VALUES($1,$2) ON CONFLICT (id) DO UPDATE SET key_hash=$2', [id, keyHash]); },
      async matchKeyGet(id) { await kready; const r = await pool.query('SELECT key_hash FROM cricket_match_keys WHERE id=$1', [id]); if (r.rows[0]) return r.rows[0].key_hash; await ready; const r2 = await pool.query('SELECT key_hash FROM cricket_matches WHERE id=$1', [id]); return r2.rows[0] ? r2.rows[0].key_hash : null; },
      async matchKeyDel(id) { await kready; await pool.query('DELETE FROM cricket_match_keys WHERE id=$1', [id]); },
      async matchIds() { await ready; const r = await pool.query('SELECT id FROM cricket_matches'); return r.rows.map((x) => x.id); },
      async deleteMatchRow(id) { await ready; await pool.query('DELETE FROM cricket_matches WHERE id=$1', [id]); },
      async deleteAllMatchRows() { await ready; await pool.query('DELETE FROM cricket_matches'); },
      async matchKeysGet(ids) { await kready; if (!ids || !ids.length) return {}; const r = await pool.query('SELECT id, key_hash FROM cricket_match_keys WHERE id = ANY($1)', [ids]); const out = {}; for (const x of r.rows) out[x.id] = x.key_hash; const missing = ids.filter((id) => !out[id]); if (missing.length) { await ready; const r2 = await pool.query('SELECT id, key_hash FROM cricket_matches WHERE id = ANY($1)', [missing]); for (const x of r2.rows) out[x.id] = x.key_hash; } return out; },
      async manifestSet(id, kind) { await fready; await pool.query('INSERT INTO cricket_fb_manifest(id,kind) VALUES($1,$2) ON CONFLICT (id) DO UPDATE SET kind=$2', [id, kind]); },
      async manifestGet(id) { await fready; const r = await pool.query('SELECT kind FROM cricket_fb_manifest WHERE id=$1', [id]); return r.rows[0] ? r.rows[0].kind : null; },
      async manifestDel(id) { await fready; await pool.query('DELETE FROM cricket_fb_manifest WHERE id=$1', [id]); },
      async manifestIds() { await fready; const r = await pool.query('SELECT id, kind FROM cricket_fb_manifest'); const out = {}; for (const x of r.rows) out[x.id] = x.kind; return out; },
      async snapshotSave(entityId, payload) { await nready; await pool.query('INSERT INTO cricket_replica_snapshots(entity_id,payload) VALUES($1,$2)', [entityId, JSON.stringify(payload)]); },
      async exportMatchUpdate(id, config, events, rev, updated, expectRev) { await ready; const r = await pool.query('UPDATE cricket_matches SET config=$1, events=$2, rev=$3, updated_at=$4 WHERE id=$5 AND rev=$6', [JSON.stringify(config), JSON.stringify(events), rev, updated, id, expectRev]); return r.rowCount === 1; },
      async replaceAllMatches(expected, fresh) {
        await ready; const c = await pool.connect();
        try {
          await c.query('BEGIN'); await c.query('LOCK TABLE cricket_matches IN EXCLUSIVE MODE');
          const rows = await c.query('SELECT * FROM cricket_matches ORDER BY id FOR UPDATE');
          const actual = rows.rows.map(row);
          if (matchSnapshot(actual) !== matchSnapshot(expected)) { await c.query('ROLLBACK'); return false; }
          await c.query('DELETE FROM cricket_matches');
          await c.query('INSERT INTO cricket_matches(id,config,events,key_hash) VALUES($1,$2,$3,$4)', [fresh.id, JSON.stringify(fresh.config), '[]', fresh.keyHash]);
          await c.query('COMMIT'); return true;
        } catch(e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
      },
      async deleteBackedUpMatch(expected) {
        await ready; const c=await pool.connect();
        try {
          await c.query('BEGIN');
          const found=await c.query('SELECT * FROM cricket_matches WHERE id=$1 FOR UPDATE',[expected.id]);
          if(found.rows.length!==1 || matchSnapshot([row(found.rows[0])])!==matchSnapshot([expected])) { await c.query('ROLLBACK'); return false; }
          const removed=await c.query('DELETE FROM cricket_matches WHERE id=$1 AND rev=$2',[expected.id,expected.rev]);
          if(removed.rowCount!==1) { await c.query('ROLLBACK'); return false; }
          await c.query('COMMIT'); return true;
        } catch(e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
      },
      async get(id) { await ready; const r = await pool.query('SELECT * FROM cricket_matches WHERE id=$1', [id]); return row(r.rows[0]); },
      async count() { await ready; const r = await pool.query('SELECT count(*)::int AS n FROM cricket_matches'); return r.rows[0].n; },
      async create(m) { await ready; await pool.query('INSERT INTO cricket_matches(id,config,events,key_hash) VALUES($1,$2,$3,$4)', [m.id, JSON.stringify(m.config), '[]', m.keyHash]); },
      async tcreate(t) { await tready; await pool.query('INSERT INTO cricket_tournaments(id,name,key_hash) VALUES($1,$2,$3)', [t.id, t.name, t.keyHash]); },
      async tget(id) { await tready; const r = await pool.query('SELECT * FROM cricket_tournaments WHERE id=$1', [id]); const x = r.rows[0]; return x && { id: x.id, name: x.name, keyHash: x.key_hash, teams: Array.isArray(x.teams) ? x.teams : [], regKeyHash: x.reg_key_hash || null, listed: !!x.listed }; },
      async tsetTeams(id, teams) { await tready; await pool.query('UPDATE cricket_tournaments SET teams=$2 WHERE id=$1', [id, JSON.stringify(teams)]); },
      async tmutateTeams(id, fn) { await tready; const t = await this.tget(id); if (!t) return false; const teams = fn(t.teams || []); if (!teams) return true; await pool.query('UPDATE cricket_tournaments SET teams=$2 WHERE id=$1', [id, JSON.stringify(teams)]); return true; },
      async reconcileFromFirebase(dry) { return { dryRun: !!dry, note: 'postgres mode: nothing to reconcile', lifted: [], insertedMatches: [], repairedMatches: [], repairedTournaments: [], syncedFlags: [], conflicts: [], errors: [] }; },
      async tsetReg(id, regKeyHash) { await tready; await pool.query('UPDATE cricket_tournaments SET reg_key_hash=$2 WHERE id=$1', [id, regKeyHash]); },
      async getSetting(k) { await sready; const r = await pool.query('SELECT v FROM cricket_settings WHERE k=$1', [k]); return r.rows[0] ? r.rows[0].v : null; },
      async setSetting(k, v) { await sready; if (v === null) { await pool.query('DELETE FROM cricket_settings WHERE k=$1', [k]); return; } await pool.query('INSERT INTO cricket_settings(k,v) VALUES($1,$2) ON CONFLICT (k) DO UPDATE SET v=$2', [k, v]); },
      async clearSettings(prefix, keepKey) { await sready; if (keepKey) await pool.query('DELETE FROM cricket_settings WHERE k LIKE $1 AND k <> $2', [prefix + '%', keepKey]); else await pool.query('DELETE FROM cricket_settings WHERE k LIKE $1', [prefix + '%']); },
      async listSettings(prefix) { await sready; const r = await pool.query('SELECT k, v FROM cricket_settings WHERE k LIKE $1', [prefix + '%']); return r.rows; },
      async tsetKey(id, keyHash) { await tready; const r = await pool.query('UPDATE cricket_tournaments SET key_hash=$2 WHERE id=$1', [id, keyHash]); return r.rowCount === 1; },
      async tsetListed(id, listed) { await tready; const r = await pool.query('UPDATE cricket_tournaments SET listed=$2 WHERE id=$1', [id, !!listed]); return r.rowCount === 1; },
      async listedTournaments() { await tready; const r = await pool.query('SELECT id,name FROM cricket_tournaments WHERE listed ORDER BY created_at'); return r.rows.map((x) => ({ id: x.id, name: x.name })); },
      async trename(id, name) { await tready; const r = await pool.query('UPDATE cricket_tournaments SET name=$2 WHERE id=$1', [id, name]); return r.rowCount === 1; },
      async tcount() { await tready; const r = await pool.query('SELECT count(*)::int AS n FROM cricket_tournaments'); return r.rows[0].n; },
      async byTournament(tid) { await ready; const r = await pool.query("SELECT * FROM cricket_matches WHERE config->>'tid'=$1 ORDER BY created_at LIMIT 300", [tid]); return r.rows.map(row); },
      async setConfig(id, config, bump) { await ready; await pool.query(bump ? 'UPDATE cricket_matches SET config=$1, rev=rev+1, updated_at=now() WHERE id=$2' : 'UPDATE cricket_matches SET config=$1 WHERE id=$2', [JSON.stringify(config), id]); },
      async allMatches() { await ready; const r = await pool.query('SELECT * FROM cricket_matches ORDER BY created_at'); return r.rows.map((x) => Object.assign(row(x), { created: x.created_at })); },
      async allTournaments() { await tready; const r = await pool.query('SELECT * FROM cricket_tournaments ORDER BY created_at'); return r.rows.map((x) => ({ id: x.id, name: x.name, keyHash: x.key_hash, teams: Array.isArray(x.teams) ? x.teams : [], regKeyHash: x.reg_key_hash || null, listed: !!x.listed, created: x.created_at })); },
      async importMatch(m) { await ready; const r = await pool.query('INSERT INTO cricket_matches(id,config,events,key_hash,rev,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING', [m.id, JSON.stringify(m.config), JSON.stringify(m.events), m.keyHash, m.rev, m.created || m.updated, m.updated]); return r.rowCount === 1; },
      async importTournament(t) { await tready; const r = await pool.query('INSERT INTO cricket_tournaments(id,name,key_hash,created_at,teams,reg_key_hash,listed) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING', [t.id, t.name, t.keyHash, t.created || new Date().toISOString(), JSON.stringify(Array.isArray(t.teams) ? t.teams : []), t.regKeyHash || null, t.listed === true]); return r.rowCount === 1; },
      async update(id, events, expectRev) {
        await ready;
        const r = await pool.query('UPDATE cricket_matches SET events=$1, rev=rev+1, updated_at=now() WHERE id=$2 AND rev=$3 RETURNING rev', [JSON.stringify(events), id, expectRev]);
        return r.rowCount === 1;
      },
    };
}
// Temporary fallback (file in /tmp): survives restarts of the process, not redeploys.
function makeFileStore() {
  const file = process.env.STORE_FILE || path.join(require('os').tmpdir(), 'cricket-matches.json');
  let db = {};
  try { db = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { db = {}; }
  const tfile = file + '.t.json';
  let tdb = {};
  try { tdb = JSON.parse(fs.readFileSync(tfile, 'utf8')); } catch (e) { tdb = {}; }
  const tsave = () => { try { fs.writeFileSync(tfile + '.tmp', JSON.stringify(tdb)); fs.renameSync(tfile + '.tmp', tfile); } catch (e) { console.error('save failed', e.message); } };
  const sfile = file + '.s.json';
  let sdb = {};
  try { sdb = JSON.parse(fs.readFileSync(sfile, 'utf8')); } catch (e) { sdb = {}; }
  const ssave = () => { try { fs.writeFileSync(sfile + '.tmp', JSON.stringify(sdb)); fs.renameSync(sfile + '.tmp', sfile); } catch (e) { console.error('save failed', e.message); } };
  const save = () => { try { fs.writeFileSync(file + '.tmp', JSON.stringify(db)); fs.renameSync(file + '.tmp', file); } catch (e) { console.error('save failed', e.message); } };
  return {
    kind: 'temporary',
    async replaceAllMatches(expected, fresh) {
      if (matchSnapshot(Object.values(db)) !== matchSnapshot(expected)) return false;
      db = { [fresh.id]: { id:fresh.id,config:fresh.config,events:[],keyHash:fresh.keyHash,rev:0,updated:new Date().toISOString() } }; save(); return true;
    },
    async deleteBackedUpMatch(expected) {
      if(!db[expected.id] || matchSnapshot([db[expected.id]])!==matchSnapshot([expected]))return false;
      const next=Object.assign({},db);delete next[expected.id];
      fs.writeFileSync(file+'.tmp',JSON.stringify(next));fs.renameSync(file+'.tmp',file);db=next;return true;
    },
    async get(id) { const m = db[id]; return m ? JSON.parse(JSON.stringify(m)) : null; },
    async count() { return Object.keys(db).length; },
    async create(m) { db[m.id] = { id: m.id, config: m.config, events: [], keyHash: m.keyHash, rev: 0, updated: new Date().toISOString() }; save(); },
    async tcreate(t) { tdb[t.id] = { id: t.id, name: t.name, keyHash: t.keyHash }; tsave(); },
    async tget(id) { const t = tdb[id]; return t ? Object.assign({}, t) : null; },
    async trename(id, name) { if (!tdb[id]) return false; tdb[id].name = name; tsave(); return true; },
    async tsetTeams(id, teams) { if (tdb[id]) { tdb[id].teams = teams; tsave(); } },
    async tmutateTeams(id, fn) { const t = tdb[id]; if (!t) return false; const teams = fn(t.teams || []); if (!teams) return true; t.teams = teams; tsave(); return true; },
    async manifestSet(id, kind) { return false; },
    async reconcileFromFirebase(dry) { return { dryRun: !!dry, note: 'temporary store: nothing to reconcile', lifted: [], insertedMatches: [], repairedMatches: [], repairedTournaments: [], syncedFlags: [], conflicts: [], errors: [] }; },
    async tsetReg(id, regKeyHash) { if (tdb[id]) { tdb[id].regKeyHash = regKeyHash; tsave(); } },
    async getSetting(k) { return sdb[k] !== undefined ? sdb[k] : null; },
    async setSetting(k, v) { if (v === null) { delete sdb[k]; } else { sdb[k] = v; } ssave(); },
    async clearSettings(prefix, keepKey) { for (const k of Object.keys(sdb)) { if (k.startsWith(prefix) && k !== keepKey) delete sdb[k]; } ssave(); },
    async listSettings(prefix) { return Object.keys(sdb).filter((k) => k.startsWith(prefix)).map((k) => ({ k, v: sdb[k] })); },
    async tsetKey(id, keyHash) { if (!tdb[id]) return false; tdb[id].keyHash = keyHash; tsave(); return true; },
    async tsetListed(id, listed) { if (!tdb[id]) return false; tdb[id].listed = !!listed; tsave(); return true; },
    async listedTournaments() { return Object.values(tdb).filter((t) => t.listed).map((t) => ({ id: t.id, name: t.name })); },
    async tcount() { return Object.keys(tdb).length; },
    async byTournament(tid) { return Object.values(db).filter((m) => m.config && m.config.tid === tid).map((m) => JSON.parse(JSON.stringify(m))); },
    async setConfig(id, config, bump) { if (db[id]) { db[id].config = config; if (bump) { db[id].rev = (db[id].rev || 0) + 1; db[id].updated = new Date().toISOString(); } save(); } },
    async allMatches() { return Object.values(db).map((m) => JSON.parse(JSON.stringify(m))); },
    async allTournaments() { return Object.values(tdb).map((t) => Object.assign({}, t)); },
    async importMatch(m) { if (db[m.id]) return false; db[m.id] = { id: m.id, config: m.config, events: m.events, keyHash: m.keyHash, rev: m.rev, updated: m.updated }; save(); return true; },
    async importTournament(t) { if (tdb[t.id]) return false; tdb[t.id] = { id: t.id, name: t.name, keyHash: t.keyHash, teams: Array.isArray(t.teams) ? t.teams : [], regKeyHash: t.regKeyHash || null, listed: t.listed === true }; tsave(); return true; },
    async update(id, events, expectRev) { const m = db[id]; if (!m || m.rev !== expectRev) return false; m.events = events; m.rev++; m.updated = new Date().toISOString(); save(); return true; },
  };
}
const tidOf = (config) => (config && typeof config.tid === 'string' && config.tid) || '';
const matchPath = (tid, id) => (tid ? 'tournaments/' + tid + '/matches/' + id : 'practice/' + id);
const matchParent = (tid) => (tid ? 'tournaments/' + tid + '/matches' : 'practice');
  // The public match record carries ONLY the approved allowlist. Private fields
  // (videoId/ninjaView camera links, tid, demo, registrationId, anything else present
  // or future) never leave Postgres. Events are already field-whitelisted at ingestion.
const publicMatchConfig = (c) => ({
  title: c.title, overs: c.overs,
  teams: (c.teams || []).map((t) => ({ name: t.name, players: (t.players || []).map((p) => (typeof p === 'string' ? p : (p && p.name) || '')) })),
  batFirst: c.batFirst, wideRun: c.wideRun, nbRun: c.nbRun, maxOversPerBowler: c.maxOversPerBowler,
  date: c.date,
  });
const PRIVATE_CONFIG_KEYS = ['videoId', 'ninjaView', 'tid', 'demo'];
  // Serve-time merge: public state from the canonical Firebase record, private-only
  // fields (camera links etc.) from the Postgres replica/legacy row.
const mergedConfig = (fbCfg, pgCfg) => {
  const out = Object.assign({}, fbCfg);
  for (const k of PRIVATE_CONFIG_KEYS) if (pgCfg && pgCfg[k] !== undefined) out[k] = pgCfg[k];
  if (pgCfg && Array.isArray(pgCfg.teams) && Array.isArray(out.teams)) {
    out.teams = out.teams.map((t, i) => {
    const pt = pgCfg.teams[i];
    return pt && pt.registrationId ? Object.assign({}, t, { registrationId: pt.registrationId }) : t;
    });
  }
  return out;
  };
const fbMatchRec = (config, events, rev, created, updated) => ({ config: publicMatchConfig(config), events, rev, created: created || iso(), updated: updated || iso() });
  // Firebase records carry PUBLIC fields only - never employee IDs or departments,
  // for ANY tournament including whxrz4tw. The PII merge join key is (team id, player
  // name lowercase): safe because no player-rename route exists; any future roster-edit
  // feature must thread empId through the mutation or this join breaks silently.
const publicTeam = (t) => ({
  id: t.id, name: t.name, submittedAt: t.submittedAt,
  players: (t.players || []).map((p) => ({ name: p.name, role: p.role || '' })),
  });

function makeStore() {
  const pg = process.env.DATABASE_URL ? makePgStore() : null;
  if (pg && process.env.FIREBASE_USER_EMAIL && process.env.FIREBASE_USER_PASSWORD) return makeFirebaseStore(pg);
  return pg || makeFileStore();
}
// Firebase-primary store: public app data (tournament records, approved rosters,
// matches and scores) lives in RTDB under nexoraCentre/ and nowhere else.
// Private auth fields (key hashes, sessions, organiser password), pending teams
// and employee details stay in Postgres. Postgres-only rows are legacy data
// served by private link and are never pushed to the public path.
function makeFirebaseStore(pg) {
  const fbd = require('./firebase-db');
  const iso = () => new Date().toISOString();
  // Nested layout (approved): every match lives under its tournament folder, or under
  // practice/ when it belongs to no tournament:
  //   nexoraCentre/tournaments/<tid>/info    {name, listed, created, registration}
  //   nexoraCentre/tournaments/<tid>/teams   {<teamId>: publicTeam}
  //   nexoraCentre/tournaments/<tid>/matches {<mid>: public match record}
  //   nexoraCentre/practice/<mid>            public match record
  async function fbTournament(id) { const t = await fbd.get('tournaments/' + id + '/info'); return t && typeof t === 'object' ? t : null; }
  async function fbTournamentTeams(id) { const t = await fbd.get('tournaments/' + id + '/teams'); return t && typeof t === 'object' ? Object.values(t) : []; }
  // Match id -> tournament membership lives in Postgres (replica/legacy row config.tid);
  // the nested Firebase path is derived from it, never from a stored public tid field.
  async function fbMatchPath(id, legacyRow) {
    const pgRow = legacyRow !== undefined ? legacyRow : await pg.get(id);
    if (pgRow) return { path: matchPath(tidOf(pgRow.config), id), pgRow };
    // No Postgres row (replica gap or rebuild): membership is recovered from the
    // canonical nested layout itself - practice node plus each tournament's matches
    // node, bounded by the tournament cap. Ambiguity is loud, never guessed.
    const homes = [];
    const prac = await fbd.getShallow('practice');
    if (prac && typeof prac === 'object' && prac[id]) homes.push('');
    const tours = await fbd.getShallow('tournaments');
    const tids = tours && typeof tours === 'object' ? Object.keys(tours) : [];
    if (tids.length > MAX_TOURNAMENTS + 40) { const err = new Error('Tournament count exceeded safe capacity - reconcile required'); err.status = 500; throw err; }
    for (const tid of tids) {
      const ms = await fbd.getShallow('tournaments/' + tid + '/matches');
      if (ms && typeof ms === 'object' && ms[id]) homes.push(tid);
    }
    if (homes.length > 1) { const err = new Error('Match ' + id + ' found under multiple parents - reconcile required'); err.status = 500; throw err; }
    return { path: matchPath(homes[0] || '', id), pgRow };
  }
  async function fbMatchIds() {
    return (await fbMatchLocations()).map((x) => x.id);
  }
  // Canonical delete/enumerate paths come from the nested layout itself, never from
  // the Postgres replica: under a Postgres outage/wipe the replica cannot name the
  // parent, and a guessed parent would silently miss the real record.
  async function fbMatchLocations() {
    const out = [];
    const prac = await fbd.getShallow('practice');
    if (prac && typeof prac === 'object') for (const id of Object.keys(prac)) out.push({ id, path: 'practice/' + id });
    const tours = await fbd.getShallow('tournaments');
    for (const tid of tours && typeof tours === 'object' ? Object.keys(tours) : []) {
      const ms = await fbd.getShallow('tournaments/' + tid + '/matches');
      if (ms && typeof ms === 'object') for (const id of Object.keys(ms)) out.push({ id, path: 'tournaments/' + tid + '/matches/' + id });
    }
    return out;
  }
  const canonicalMissing = (kind, id) => { const e = new Error('Canonical Firebase record missing for ' + kind + ' ' + id); e.status = 500; e.canonical = true; return e; };
  async function matchKeyHash(id, legacyRow) {
    let keyHash = await pg.matchKeyGet(id);
    if (!keyHash) { const legacy = legacyRow !== undefined ? legacyRow : await pg.get(id); keyHash = (legacy && legacy.keyHash) || ''; }
    return keyHash;
  }
  async function tgetMerged(id) {
    const kind0 = await pg.manifestGet(id);
    if (kind0 === 'tournament-degraded') return pg.tget(id); // Postgres-canonical until reconcile lifts: no Firebase call (works during an outage)
    const fb = await fbTournament(id); // throws (loud 500) on Firebase errors; null = absent
    if (!fb) {
      if (kind0 === 'tournament') throw canonicalMissing('tournament', id); // was Firebase-canonical: never resurrect the stale replica
      return pg.tget(id); // true legacy link-private
    }
    const legacy = await pg.tget(id);
    const blobTeams = legacy && Array.isArray(legacy.teams) ? legacy.teams : [];
    const blobById = {};
    for (const t of blobTeams) if (t && t.id) blobById[t.id] = t;
    const approved = (await fbTournamentTeams(id)).map((t) => {
      const blob = blobById[t.id] || {};
      const byName = {};
      for (const p of (blob.players || [])) if (p && p.name) byName[String(p.name).toLowerCase()] = p;
      return {
        id: t.id, name: t.name, submittedAt: t.submittedAt || blob.submittedAt, status: 'approved',
        players: (Array.isArray(t.players) ? t.players : []).map((p) => {
          const priv = byName[String(p.name || '').toLowerCase()] || {};
          return { name: p.name, empId: priv.empId || '', dept: priv.dept || '', role: p.role || '' };
        }),
      };
    });
    const approvedIds = new Set(approved.map((t) => t.id));
    // Dedupe by TEAM ID: a team approved in Firebase never reappears as pending,
    // even if the Postgres remainder lagged (or a same-named team exists).
    const pending = blobTeams.filter((t) => t && t.status !== 'approved' && !approvedIds.has(t.id));
    return {
      id,
      name: fb.name,
      keyHash: (legacy && legacy.keyHash) || '',
      teams: approved.concat(pending),
      regKeyHash: legacy ? legacy.regKeyHash || null : null,
      listed: !!fb.listed,
      created: fb.created || (legacy && legacy.created),
    };
  }
  async function allMatchesMerged() {
    const fbRecs = [];
    const prac = await fbd.get('practice');
    if (prac && typeof prac === 'object') for (const id of Object.keys(prac)) fbRecs.push([id, prac[id]]);
    const tours = await fbd.getShallow('tournaments');
    for (const tid of tours && typeof tours === 'object' ? Object.keys(tours) : []) {
      const ms = await fbd.get('tournaments/' + tid + '/matches');
      if (ms && typeof ms === 'object') for (const id of Object.keys(ms)) fbRecs.push([id, ms[id]]);
    }
    const fbIds = fbRecs.map(([id]) => id);
    const out = [];
    for (const [id, r] of fbRecs) {
      if (!r || typeof r !== 'object') continue;
      const pgRow = await pg.get(id);
      out.push({ id, config: mergedConfig(r.config, pgRow && pgRow.config), events: Array.isArray(r.events) ? r.events : [], keyHash: await matchKeyHash(id), rev: r.rev || 0, updated: r.updated, created: r.created });
    }
    for (const m of await pg.allMatches()) if (!fbIds.includes(m.id)) out.push(m);
    return out;
  }
  return {
    kind: 'durable',
    getSetting: (k) => pg.getSetting(k),
    setSetting: (k, v) => pg.setSetting(k, v),
    clearSettings: (prefix, keepKey) => pg.clearSettings(prefix, keepKey),
    listSettings: (prefix) => pg.listSettings(prefix),
    // Organiser keys are secrets: they live in Postgres only, never in Firebase.
    tsetKey: (id, keyHash) => pg.tsetKey(id, keyHash),
    async get(id) {
      const kind0 = await pg.manifestGet(id);
      if (kind0 === 'match-degraded') return pg.get(id); // Postgres-canonical until reconcile lifts: no Firebase call (works during an outage)
      const { path, pgRow } = await fbMatchPath(id);
      const fb = await fbd.get(path);
      if (fb && typeof fb === 'object') return { id, config: mergedConfig(fb.config, pgRow && pgRow.config), events: Array.isArray(fb.events) ? fb.events : [], keyHash: await matchKeyHash(id, pgRow), rev: fb.rev || 0, updated: fb.updated, created: fb.created };
      if (kind0 === 'match') throw canonicalMissing('match', id); // was Firebase-canonical: never resurrect the stale replica
      return pgRow; // true legacy
    },
    async count() {
      // Storage-limit guard only (not user-facing canonical data): if Firebase is
      // unreachable, fall back to the Postgres count so a degraded create can proceed
      // (same documented pattern as tcount below).
      let fb = [];
      try { fb = await fbMatchIds(); }
      catch (e) { return (await pg.matchIds()).length; }
      const legacy = (await pg.matchIds()).filter((x) => !fb.includes(x));
      return fb.length + legacy.length;
    },
    async create(m) {
      // Private key + degraded marker first, then the canonical Firebase record. If the
      // Firebase write fails the match is created degraded: a Postgres replica row keeps
      // it working link-privately and reconcile lifts it into Firebase later.
      await pg.matchKeySet(m.id, m.keyHash);
      await pg.manifestSet(m.id, 'match-degraded');
      try {
        await fbd.put(matchPath(tidOf(m.config), m.id), fbMatchRec(m.config, [], 0, iso(), iso()));
      } catch (e) {
        await pg.importMatch({ id: m.id, config: m.config, events: [], keyHash: m.keyHash, rev: 0, created: iso(), updated: iso() });
        return false; // degraded: durable in Postgres, Firebase sync pending (reconcile lifts)
      }
      await pg.manifestSet(m.id, 'match');
      try { await pg.importMatch({ id: m.id, config: m.config, events: [], keyHash: m.keyHash, rev: 0, created: iso(), updated: iso() }); }
      catch (e) { console.error('replica insert failed for match ' + m.id + ' (reconcile repairs)', e); }
    },
    async update(id, events, expectRev) {
      const kind0 = await pg.manifestGet(id);
      if (kind0 === 'match-degraded') return pg.update(id, events, expectRev); // Postgres-canonical until lifted
      const { path, pgRow } = await fbMatchPath(id);
      const g = await fbd.get(path, true);
      if (!g.data || typeof g.data !== 'object') {
        if (kind0 === 'match') throw canonicalMissing('match', id);
        return pg.update(id, events, expectRev);
      }
      if ((g.data.rev || 0) !== expectRev) return false;
      const next = Object.assign({}, g.data, { events, rev: expectRev + 1, updated: iso() });
      try { await fbd.put(path, next, g.etag || undefined); }
      catch (e) { if (e.status === 412) return false; throw e; }
      // Canonical commit succeeded. Replica update is best-effort; reconcile repairs drift.
      // The replica keeps the FULL config (private fields) merged over the public one.
      try { await pg.exportMatchUpdate(id, mergedConfig(next.config, pgRow && pgRow.config), events, expectRev + 1, next.updated, expectRev); }
      catch (e) { console.error('replica update failed for match ' + id + ' (reconcile repairs)', e); }
      return true;
    },
    async setConfig(id, config, bump) {
      const kind0 = await pg.manifestGet(id);
      if (kind0 === 'match-degraded') return pg.setConfig(id, config, bump); // Postgres-canonical until lifted
      const { path } = await fbMatchPath(id);
      const g = await fbd.get(path, true);
      if (!g.data || typeof g.data !== 'object') {
        if (kind0 === 'match') throw canonicalMissing('match', id);
        return pg.setConfig(id, config, bump);
      }
      // Firebase stores ONLY the allowlisted public projection; the full config
      // (private fields) lives in the Postgres replica and merges back at serve time.
      const pub = publicMatchConfig(config);
      const attempt = (cur, etag) => {
        const next = Object.assign({}, cur, { config: pub });
        if (bump) { next.rev = (cur.rev || 0) + 1; next.updated = iso(); }
        return fbd.put(path, next, etag || undefined);
      };
      try { await attempt(g.data, g.etag); }
      catch (e) {
        if (e.status !== 412) throw e;
        const g2 = await fbd.get(path, true);
        if (!g2.data || typeof g2.data !== 'object') throw e;
        await attempt(g2.data, g2.etag); // a second conflict surfaces as an error
      }
      // Canonical commit succeeded; replica is best-effort, reconcile repairs drift.
      try { const cur = await fbd.get(path); if (cur) await pg.exportMatchUpdate(id, config, cur.events || [], cur.rev || 0, cur.updated || iso(), (cur.rev || 0) - (bump ? 1 : 0)); } catch (e) { console.error('replica setConfig failed for ' + id + ' (reconcile repairs)', e); }
    },
    // Bounded read WITHOUT RTDB indexes: the frozen rules carry no .indexOn and the real
    // REST API REJECTS unindexed orderBy/equalTo queries with 400 (verified against the
    // RTDB emulator with the approved rules). So candidate ids come from Postgres
    // (indexed SQL on config->>'tid', covering replica + legacy), canonical membership
    // from ONE shallow key read, and canonical records are fetched by id - bounded by the
    // tournament's own size. Canonical-missing is loud (never serve the stale replica).
    async byTournament(tid) {
      const legacyRows = await pg.byTournament(tid);
      let shallow;
      try { shallow = await fbd.getShallow('tournaments/' + tid + '/matches'); }
      catch (e) { if ((await pg.manifestGet(tid)) === 'tournament-degraded') return legacyRows; throw e; } // degraded tournament serves Postgres-only during an outage
      const fbIds = shallow && typeof shallow === 'object' ? Object.keys(shallow) : [];
      const fbSet = new Set(fbIds);
      const out = [];
      const seen = new Set();
      const present = [];
      for (const m of legacyRows) {
        seen.add(m.id);
        if (!fbSet.has(m.id)) { out.push(m); continue; } // legacy or degraded: Postgres copy
        present.push(m);
      }
      // The nested node is tournament-scoped by construction; membership in
      // tournaments/<tid>/matches IS the canonical tid assignment (no public tid field).
      for (let i = 0; i < present.length; i += 8) {
        const got = await Promise.all(present.slice(i, i + 8).map((m) => fbd.get('tournaments/' + tid + '/matches/' + m.id).then((g) => ({ m, g }))));
        for (const { m, g } of got) {
          if (!g || typeof g !== 'object' || !g.config) throw canonicalMissing('match', m.id);
          out.push({ id: m.id, config: mergedConfig(g.config, m.config), events: Array.isArray(g.events) ? g.events : [], keyHash: m.keyHash, rev: g.rev || 0, updated: g.updated, created: g.created });
        }
      }
      // Canonical matches whose Postgres replica is missing (post-loss window; reconcile
      // repairs): bounded backfill so the tournament view stays complete meanwhile.
      const orphans = fbIds.filter((id) => !seen.has(id));
      for (let i = 0; i < orphans.length; i += 8) {
        const got = await Promise.all(orphans.slice(i, i + 8).map((id) => fbd.get('tournaments/' + tid + '/matches/' + id).then((g) => ({ id, g }))));
        for (const { id, g } of got) {
          if (g && typeof g === 'object' && g.config) {
            const keyHash = await pg.matchKeyGet(id);
            const pgRow = await pg.get(id);
            out.push({ id, config: mergedConfig(g.config, pgRow && pgRow.config), events: Array.isArray(g.events) ? g.events : [], keyHash: keyHash || '', rev: g.rev || 0, updated: g.updated, created: g.created });
            if (out.length > MAX_PER_TOURNAMENT + 40) { const err = new Error('Tournament match backfill exceeded safe capacity - reconcile required'); err.status = 500; throw err; }
          }
        }
      }
      return out;
    },
    async allMatches() { return allMatchesMerged(); },
    async importMatch(m) {
      // Restore ALWAYS rebuilds the local manager key + replica row (INSERT-only, never
      // clobbers) so the original scorer link keeps working; the canonical Firebase
      // record itself is only inserted when absent (never overwritten).
      await pg.matchKeySet(m.id, m.keyHash);
      const localAdded = await pg.importMatch(m);
      const mpath = matchPath(tidOf(m.config), m.id);
      const existing = await fbd.get(mpath);
      if (!existing) await fbd.put(mpath, fbMatchRec(m.config, m.events || [], m.rev || 0, m.created, m.updated));
      await pg.manifestSet(m.id, 'match');
      return localAdded || !existing;
    },
    async deleteBackedUpMatch(expected) {
      const g = await fbd.get(matchPath(tidOf(expected.config), expected.id));
      if (!g || typeof g !== 'object') return pg.deleteBackedUpMatch(expected);
      const legacy = await pg.get(expected.id);
      const keyHash = await matchKeyHash(expected.id, legacy);
      const cur = { id: expected.id, config: g.config, events: Array.isArray(g.events) ? g.events : [], keyHash, rev: g.rev || 0 };
      if (matchSnapshot([cur]) !== matchSnapshot([expected])) return false;
      await pg.snapshotSave('match:' + expected.id, cur); // version preserved before the owner-authorised delete
      await fbd.del(matchPath(tidOf(expected.config), expected.id));
      await pg.matchKeyDel(expected.id);
      await pg.manifestDel(expected.id);
      if (legacy) await pg.deleteMatchRow(expected.id); // drop the stale migrated/replica copy
      return true;
    },
    async replaceAllMatches(expected, fresh) {
      const all = await allMatchesMerged();
      if (matchSnapshot(all) !== matchSnapshot(expected)) return false;
      await pg.snapshotSave('matches:all', all); // version preserved before the owner-authorised replace
      for (const loc of await fbMatchLocations()) { await fbd.del(loc.path); await pg.matchKeyDel(loc.id); await pg.manifestDel(loc.id); }
      await pg.deleteAllMatchRows();
      await fbd.put(matchPath(tidOf(fresh.config), fresh.id), fbMatchRec(fresh.config, [], 0, iso(), iso()));
      await pg.matchKeySet(fresh.id, fresh.keyHash);
      await pg.manifestSet(fresh.id, 'match');
      return true;
    },
    async tcreate(t) {
      // Postgres first (secrets + durable row + degraded marker): if the canonical
      // Firebase write fails, the tournament works link-privately and reconcile lifts it.
      await pg.tcreate(t);
      await pg.manifestSet(t.id, 'tournament-degraded');
      try {
        await fbd.put('tournaments/' + t.id + '/info', { name: t.name, listed: false, created: iso(), registration: false });
        await pg.manifestSet(t.id, 'tournament');
        return true;
      } catch (e) { console.error('Firebase tournament create degraded for ' + t.id + ' (reconcile lifts)', e); return false; }
    },
    async tget(id) { return tgetMerged(id); },
    // Compare-and-swap TRANSFORM for roster intents: re-read with ETag, apply the
    // caller's mutation to the merged state, conditional commit, bounded retry on 412.
    // Both intents survive concurrent edits (captain submit vs organiser approve).
    // fn(teams) returns the next full roster, or null for a no-op/conflict signalled by the route.
    async tmutateTeams(id, fn) {
      for (let attempt = 0; attempt < 3; attempt++) {
        const kind0 = await pg.manifestGet(id);
        // Canonical existence is the info node; roster CAS targets the teams node only,
        // so concurrent match writes under the same tournament never conflict with it.
        const info = kind0 === 'tournament-degraded' ? null : await fbTournament(id);
        if (!info) {
          if (kind0 === 'tournament') throw canonicalMissing('tournament', id);
          const legacy = await pg.tget(id);
          if (!legacy) return false;
          const teams = fn(legacy.teams || []);
          if (!teams) return true;
          await pg.tsetTeams(id, teams);
          return true;
        }
        const cur = await fbd.get('tournaments/' + id + '/teams', true);
        const merged = await tgetMerged(id);
        if (!merged) return false;
        const teams = fn(merged.teams || []);
        if (!teams) return true;
        const approved = {};
        for (const t of teams) if (t && t.status === 'approved') approved[t.id] = publicTeam(t);
        // Conditional PUT of the re-read teams node: the real REST API honors If-Match
        // on PUT (a PATCH commit cannot prove the base ETag, so it would silently lose
        // concurrent intents). The node carries only zero-PII public projections.
        try {
          await fbd.put('tournaments/' + id + '/teams', approved, cur.etag || undefined);
        } catch (e) { if (e.status === 412) continue; throw e; }
        // Canonical commit done. Replica remainder (FULL roster incl. pending + PII) in Postgres.
        await pg.tsetTeams(id, teams); // failure -> 500; dedupe-on-read + reconcile converge
        return true;
      }
      return false; // CAS retries exhausted -> route maps this to a 409
    },
    async tsetReg(id, regKeyHash) {
      // Registration key hash is a Postgres secret and leads; the Firebase flag is its projection.
      await pg.tsetReg(id, regKeyHash);
      const fb = await fbTournament(id);
      if (fb) await fbd.patch('tournaments/' + id + '/info', { registration: !!regKeyHash });
    },
    async tsetListed(id, listed) {
      // Listed is public canonical in Firebase; the Postgres row is the replica.
      const fb = await fbTournament(id);
      if (fb) await fbd.patch('tournaments/' + id + '/info', { listed: !!listed });
      return pg.tsetListed(id, listed);
    },
    async trename(id, name) {
      const fb = await fbTournament(id);
      if (fb) { await fbd.patch('tournaments/' + id + '/info', { name }); await pg.trename(id, name); return true; }
      return pg.trename(id, name);
    },
    async tcount() {
      // Storage-limit guard only (not user-facing canonical data): if Firebase is
      // unreachable, fall back to the Postgres count so a degraded create can proceed.
      let fbIds = [];
      try {
        const all = await fbd.getShallow('tournaments');
        fbIds = all && typeof all === 'object' ? Object.keys(all) : [];
      } catch (e) { return (await pg.allTournaments()).length; }
      const legacy = await pg.allTournaments();
      return fbIds.length + legacy.filter((t) => !fbIds.includes(t.id)).length;
    },
    // Bounded read WITHOUT RTDB indexes (see byTournament): Postgres supplies the listed
    // candidates, one shallow read establishes canonical membership, canonical records
    // are fetched by id and their Firebase `listed` flag is the truth. FB-only ids
    // (replica gap; reconcile repairs) get a bounded backfill.
    async listedTournaments() {
      const legacy = await pg.listedTournaments();
      const shallow = await fbd.getShallow('tournaments');
      const fbIds = shallow && typeof shallow === 'object' ? Object.keys(shallow) : [];
      const fbSet = new Set(fbIds);
      const out = [];
      const seen = new Set();
      const man = await pg.manifestIds(); // one read for the whole list
      const present = [];
      for (const t of legacy) {
        seen.add(t.id);
        const k = man[t.id];
        if (k === 'tournament-degraded') { out.push(t); continue; } // Postgres-canonical until lifted (same routing as tget)
        if (!fbSet.has(t.id)) {
          if (k === 'tournament') throw canonicalMissing('tournament', t.id); // canonical marker but record gone: loud, never resurrect the stale replica
          out.push(t); continue; // true legacy (no marker): Postgres copy
        }
        present.push(t);
      }
      // Candidates in both stores: fetch canonical records by id, 8 at a time, order preserved.
      for (let i = 0; i < present.length; i += 8) {
        const got = await Promise.all(present.slice(i, i + 8).map((t) => fbd.get('tournaments/' + t.id).then((fb) => ({ t, fb }))));
        for (const { t, fb } of got) {
          if (!fb) throw canonicalMissing('tournament', t.id);
          if (fb.listed) out.push({ id: t.id, name: fb.name });
        }
      }
      const orphans = fbIds.filter((id) => !seen.has(id)).slice(0, 100);
      for (const id of orphans) {
        const fb = await fbd.get('tournaments/' + id);
        if (fb && fb.listed) out.push({ id, name: fb.name });
      }
      return out;
    },
    // Backup-only path (owner-triggered): whole-subtree reads are acceptable here.
    async allTournaments() {
      const all = await fbd.get('tournaments');
      const fbIds = all && typeof all === 'object' ? Object.keys(all) : [];
      const out = [];
      for (const id of fbIds) { const t = await tgetMerged(id); if (t) out.push(t); }
      for (const t of await pg.allTournaments()) if (!fbIds.includes(t.id)) out.push(t);
      return out;
    },
    manifestSet: (id, kind) => pg.manifestSet(id, kind),
    // Reconcile Postgres replicas/secrets to the canonical Firebase state. Dry-run unless
    // apply=false. Versioned: every repair write snapshots the previous replica first
    // (cricket_replica_snapshots, append-only). Firebase is never modified by reconcile
    // except lifting degraded creates (INSERT-only, their intended canonical home).
    async reconcileFromFirebase(dry) {
      const out = { dryRun: dry, lifted: [], insertedMatches: [], repairedMatches: [], repairedTournaments: [], syncedFlags: [], conflicts: [], errors: [] };
      // 1. Degraded creates (Postgres-only after a Firebase outage) -> lift into Firebase.
      const man = await pg.manifestIds();
      for (const id of Object.keys(man)) {
        try {
          if (man[id] === 'tournament-degraded') {
            const fb = await fbTournament(id);
            if (fb) {
              // Committed-but-response-lost: the Firebase record landed, only the manifest
              // heal failed. The Postgres copy stayed canonical for the whole degraded
              // window, so it can hold NEWER roster/meta updates than Firebase. Heal the
              // marker only when both copies agree; on divergence hold and report - a
              // blind heal would let the converge step overwrite the window updates.
              // (Snapshots make that recoverable; recoverable is not permission.)
              const legacy = await pg.tget(id);
              if (!legacy) { out.conflicts.push({ id, reason: 'degraded tournament missing from Postgres' }); continue; }
              const pub = (t) => ({ id: t.id, name: t.name, submittedAt: t.submittedAt, players: (t.players || []).map((p) => ({ name: p.name, role: p.role || '' })) });
              const sortById = (a) => a.slice().sort((x, y) => String(x.id).localeCompare(String(y.id)));
              // Pending teams live in Postgres only by design (the merge re-adds them), so they are not divergence.
              const pgApproved = sortById(cleanStoredTeams(legacy.teams).filter((x) => x.status === 'approved').map(pub));
              const fbApproved = sortById((await fbTournamentTeams(id)).map(pub));
              const diverged = legacy.name !== fb.name || !!legacy.listed !== !!fb.listed || !!legacy.regKeyHash !== !!fb.registration
                || JSON.stringify(stableValue(pgApproved)) !== JSON.stringify(stableValue(fbApproved));
              if (diverged) { out.conflicts.push({ id, reason: 'degraded tournament: Postgres and Firebase copies diverge - held, no heal, window updates preserved; owner review needed' }); continue; }
              if (!dry) await pg.manifestSet(id, 'tournament');
              continue;
            }
            const legacy = await pg.tget(id);
            if (!legacy) { out.conflicts.push({ id, reason: 'degraded tournament missing from Postgres' }); continue; }
            const approved = cleanStoredTeams(legacy.teams).filter((x) => x.status === 'approved').map((t) => publicTeam(t));
            out.lifted.push({ tournament: id });
            if (!dry) {
              const teamMap = {};
              for (const t of approved) teamMap[t.id] = t;
              // teams node first: an info-only partial lift still reads as absent (fbTournament
              // returns null) so the next reconcile retries cleanly; manifest heals last.
              await fbd.put('tournaments/' + id + '/teams', teamMap);
              await fbd.put('tournaments/' + id + '/info', { name: legacy.name, listed: !!legacy.listed, created: legacy.created || iso(), registration: !!legacy.regKeyHash });
              await pg.manifestSet(id, 'tournament');
            }
          }
          if (man[id] === 'match-degraded') {
            const legacy = await pg.get(id);
            if (!legacy) { out.conflicts.push({ id, reason: 'degraded match missing from Postgres' }); continue; }
            const mpath = matchPath(tidOf(legacy.config), id);
            const fb = await fbd.get(mpath);
            if (fb && typeof fb === 'object' && fb.config) {
              // Same committed-but-response-lost guard as tournaments: heal only when the
              // Postgres copy (canonical during the window) and Firebase agree, so window
              // scoring is never silently overwritten by the converge step. Firebase holds
              // only the public projection, so compare projections, not the full config.
              const sameConfig = JSON.stringify(stableValue(publicMatchConfig(legacy.config))) === JSON.stringify(stableValue(fb.config));
              const sameEvents = JSON.stringify(stableValue(legacy.events || [])) === JSON.stringify(stableValue(Array.isArray(fb.events) ? fb.events : []));
              if (!sameConfig || !sameEvents) { out.conflicts.push({ id, reason: 'degraded match: Postgres and Firebase copies diverge - held, no heal, window scoring preserved; owner review needed' }); continue; }
              if (!dry) await pg.manifestSet(id, 'match');
              continue;
            }
            out.lifted.push({ match: id });
            if (!dry) { await fbd.put(mpath, fbMatchRec(legacy.config, legacy.events || [], legacy.rev || 0, legacy.created, legacy.updated)); await pg.manifestSet(id, 'match'); }
          }
        } catch (e) { out.errors.push({ id, error: String((e && e.message) || e).slice(0, 200) }); }
      }
      // 2. Canonical tournaments: converge the Postgres replica row to Firebase truth.
      const tshallow = await fbd.getShallow('tournaments');
      const fbTids = tshallow && typeof tshallow === 'object' ? Object.keys(tshallow) : [];
      for (const id of fbTids) {
        try {
          const kindH = await pg.manifestGet(id);
          if (kindH === 'tournament-degraded') continue; // still Postgres-canonical (held divergent or awaiting lift): never converge over it
          if (kindH !== 'tournament') { out.syncedFlags.push({ tournament: id, manifestHealed: true }); if (!dry) await pg.manifestSet(id, 'tournament'); }
          const merged = await tgetMerged(id);
          if (!merged) continue;
          const legacy = await pg.tget(id);
          if (!legacy) { out.conflicts.push({ tournament: id, reason: 'canonical Firebase tournament has no Postgres row - owner action needed (key cannot be fabricated)' }); continue; }
          const blobSame = JSON.stringify(stableValue(cleanStoredTeams(legacy.teams))) === JSON.stringify(stableValue(cleanStoredTeams(merged.teams)));
          const metaSame = legacy.name === merged.name && !!legacy.listed === !!merged.listed;
          if (!blobSame || !metaSame) {
            out.repairedTournaments.push({ tournament: id, teams: !blobSame, meta: !metaSame });
            if (!dry) {
              await pg.snapshotSave('tournament:' + id, legacy);
              await pg.tsetTeams(id, merged.teams);
              if (legacy.name !== merged.name) await pg.trename(id, merged.name);
              if (!!legacy.listed !== !!merged.listed) await pg.tsetListed(id, merged.listed);
            }
          }
          // registration flag: Postgres secret leads, Firebase flag follows
          const fb = await fbTournament(id);
          if (fb && !!fb.registration !== !!merged.regKeyHash) {
            out.syncedFlags.push({ tournament: id, registration: !!merged.regKeyHash });
            if (!dry) await fbd.patch('tournaments/' + id + '/info', { registration: !!merged.regKeyHash });
          }
        } catch (e) { out.errors.push({ tournament: id, error: String((e && e.message) || e).slice(0, 200) }); }
      }
      // 3. Canonical matches: insert missing replicas, set diverged replicas to canonical.
      // Nested layout: ids come from the practice + per-tournament nodes; the Postgres
      // replica row resolves each id to its nested path (membership lives in Postgres).
      const fbMids = await fbMatchIds();
      for (const id of fbMids) {
        try {
          const kindH = await pg.manifestGet(id);
          if (kindH === 'match-degraded') continue; // still Postgres-canonical (held divergent or awaiting lift): never converge over it
          if (kindH !== 'match') { out.syncedFlags.push({ match: id, manifestHealed: true }); if (!dry) await pg.manifestSet(id, 'match'); }
          const legacy = await pg.get(id);
          const fb = await fbd.get(matchPath(tidOf(legacy && legacy.config), id));
          if (!fb || typeof fb !== 'object' || !fb.config) continue;
          const fbEvents = Array.isArray(fb.events) ? fb.events : [];
          if (!legacy) {
            const keyHash = await pg.matchKeyGet(id);
            if (!keyHash) { out.conflicts.push({ match: id, reason: 'no manager key in Postgres - not exported' }); continue; }
            out.insertedMatches.push({ match: id, events: fbEvents.length });
            if (!dry) await pg.importMatch({ id, config: fb.config, events: fbEvents, keyHash, rev: fb.rev || 0, created: fb.created, updated: fb.updated });
            continue;
          }
          // jsonb round-trips reorder object keys; compare normalized values, not raw strings.
          // Firebase holds only the public projection: compare projections, and repair the
          // replica with canonical public fields merged over its preserved private fields.
          const sameEvents = JSON.stringify(stableValue(legacy.events || [])) === JSON.stringify(stableValue(fbEvents));
          const sameConfig = JSON.stringify(stableValue(publicMatchConfig(legacy.config))) === JSON.stringify(stableValue(fb.config));
          if (!sameEvents || !sameConfig || (legacy.rev || 0) !== (fb.rev || 0)) {
            out.repairedMatches.push({ match: id, events: !sameEvents, config: !sameConfig, rev: (legacy.rev || 0) !== (fb.rev || 0) });
            if (!dry) {
              await pg.snapshotSave('match:' + id, legacy);
              const okUp = await pg.exportMatchUpdate(id, mergedConfig(fb.config, legacy.config), fbEvents, fb.rev || 0, fb.updated || iso(), legacy.rev || 0);
              if (!okUp) out.conflicts.push({ match: id, reason: 'replica changed concurrently - rerun reconcile' });
            }
          }
        } catch (e) { out.errors.push({ match: id, error: String((e && e.message) || e).slice(0, 200) }); }
      }
      return out;
    },
    async importTournament(t) {
      // Restore ALWAYS rebuilds the local row (key hash, secrets, roster blob); the
      // canonical Firebase record is only inserted when absent (never overwritten).
      const localAdded = await pg.importTournament(t);
      const existing = await fbTournament(t.id);
      if (!existing) {
        const approved = cleanStoredTeams(t.teams).filter((x) => x.status === 'approved').map((x) => publicTeam(x));
        const teamMap = {};
        for (const tm of approved) teamMap[tm.id] = tm;
        await fbd.put('tournaments/' + t.id + '/teams', teamMap);
        await fbd.put('tournaments/' + t.id + '/info', { name: t.name, listed: !!t.listed, created: t.created || iso(), registration: !!t.regKeyHash });
      }
      await pg.manifestSet(t.id, 'tournament');
      return localAdded || !existing;
    },
  };
}
function stableValue(x) { if (Array.isArray(x)) return x.map(stableValue); if(x && typeof x==='object') return Object.keys(x).sort().reduce((a,k)=>(a[k]=stableValue(x[k]),a),{}); return x; }
function matchSnapshot(ms) { return JSON.stringify(stableValue(ms.map(m=>({id:m.id,config:m.config,events:m.events,keyHash:m.keyHash,rev:m.rev})).sort((a,b)=>a.id.localeCompare(b.id)))); }
const store = makeStore();
// In-app self-healing: on boot (Firebase mode), converge replicas/manifest and lift any
// degraded creates in the background. Logged, never blocks serving; dry-run never used here.
if (process.env.FIREBASE_USER_EMAIL) {
  setImmediate(() => store.reconcileFromFirebase(false)
    .then((r) => console.log('boot reconcile: lifted=' + r.lifted.length + ' repaired=' + (r.repairedMatches.length + r.repairedTournaments.length) + ' healed=' + r.syncedFlags.length + ' conflicts=' + r.conflicts.length + ' errors=' + r.errors.length))
    .catch((e) => console.error('boot reconcile failed (serving continues; next boot retries)', e && e.message)));
}

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
function readBody(req, max) {
  return new Promise((resolve, reject) => {
    let b = ''; req.on('data', (c) => { b += c; if (b.length > (max || 20000)) { reject(Object.assign(new Error('Too large'), { user: true })); req.destroy(); } });
    req.on('end', () => { try { resolve(b ? JSON.parse(b) : {}); } catch (e) { reject(Object.assign(new Error('Bad JSON'), { user: true })); } });
    req.on('error', reject);
  });
}
function publicView(m) {
  const config = Object.assign({}, m.config); delete config.tid;
  return { id: m.id, rev: m.rev, updated: m.updated, config, inTournament: !!(m.config && m.config.tid), state: engine.view(m.config, m.events), storage: store.kind };
}
const keyEq = (given, hash) => { const a = Buffer.from(sha(given || '')), b = Buffer.from(String(hash)); return a.length === b.length && crypto.timingSafeEqual(a, b); };
async function orgSessionRecord(token){
  if(typeof token!=='string'||token.length<16||token.length>64)return null;
  let rec=null;try{rec=JSON.parse((await store.getSetting('orgsess_'+sha(token)))||'null')}catch(e){}
  if(rec&&rec.exp>Date.now()&&keyEq(token,rec.h))return rec;
  try{rec=JSON.parse((await store.getSetting('orgsess'))||'null')}catch(e){rec=null}
  return rec&&rec.exp>Date.now()&&keyEq(token,rec.h)?rec:null;
}
async function validOrgSession(token){return !!(await orgSessionRecord(token));}
// A saved tournament needs both organiser sign-in and its existing private key.
// Keep only the key fingerprint, so replacing an old key invalidates saved access.
async function savedOrgTournament(req, t) {
  return !!(t && await validOrgSession(req.headers['x-org-session']) &&
    (await store.getSetting('orgtour_'+t.id)) === t.keyHash);
}
// Organiser mutations need BOTH a signed-in session and the tournament's organiser
// link on the request - a link alone (or a login alone) mutates nothing.
async function orgAuthed(req, t) {
  return !!(t && (await validOrgSession(req.headers['x-org-session'])) && keyEq(req.headers['x-org-key'], t.keyHash));
}
// --- Organiser password policy ---
// v:2 marks a password set under the 10-character minimum. Anything older is
// "legacy": the scrypt hash cannot reveal its length or strength, so every
// pre-policy password is treated as unverified and must be replaced (he types it
// himself) before organiser console actions. Sign-in and the change itself keep
// working with the current password or an existing session - never a hard lockout.
async function orgpassRec() { let ph = null; try { ph = JSON.parse((await store.getSetting('orgpass')) || 'null'); } catch (e) { ph = null; } return ph && ph.salt && ph.hash ? ph : null; }
async function orgpassLegacy() { const ph = await orgpassRec(); return !!(ph && ph.v !== 2); }
// Central gate for every organiser console action: a legacy (pre-policy) password
// blocks console actions until he sets a new one. One responder, so every route
// returns the same reset signal; auth failures keep their own 403 below. Sign-in,
// the password change itself and logout stay open - never a hard lockout.
async function orgResetBlocked(res) {
  if (!(await orgpassLegacy())) return false;
  send(res, 409, { error: 'Set a new password (10+ characters) first.', code: 'reset_required' });
  return true;
}
// Security audit trail (orgaudit_YYYY-MM-DD settings rows): times + event names + IP
// only - never passwords, tokens, links or key material.
// Audit calls are fire-and-forget at call sites; serialize the read-modify-write
// through a queue so concurrent events never overwrite each other.
let auditQ = Promise.resolve();
function auditOrg(event, detail) {
  auditQ = auditQ.then(() => auditOrgNow(event, detail));
  return auditQ;
}
async function auditOrgNow(event, detail) {
  try {
    const k = 'orgaudit_' + new Date().toISOString().slice(0, 10);
    const cur = (await store.getSetting(k)) || '';
    if (cur.length > 60000) return;
    await store.setSetting(k, (cur ? cur + '\n' : '') + JSON.stringify(Object.assign({ at: new Date().toISOString(), event }, detail || {})));
  } catch (e) { console.error('audit write failed', e && e.message); }
}
// Wrong-sign-in protection: 5 consecutive failures from one connection block sign-in
// for 5 minutes. Persisted in the settings store so a restart keeps the block; a
// successful sign-in clears the counter. The generic 10/hour badAuth backstop stays.
const LOGIN_MAX_FAILS = 5, LOGIN_FAIL_WINDOW = 10 * 60e3, LOGIN_BLOCK_MS = 5 * 60e3;
const loginLockMem = new Map(); // fallback if the settings store itself errors
async function loginLockGet(ip) { try { const r = await store.getSetting('loginlock_' + ip); return r ? JSON.parse(r) : null; } catch (e) { return loginLockMem.get(ip) || null; } }
async function loginLockSet(ip, v) { try { await store.setSetting('loginlock_' + ip, v ? JSON.stringify(v) : null); } catch (e) { if (v) loginLockMem.set(ip, v); else loginLockMem.delete(ip); } }
async function loginBlocked(ip) { const r = await loginLockGet(ip); return !!(r && r.until && r.until > Date.now()); }
async function loginNoteFail(ip) {
  const now = Date.now();
  const r = (await loginLockGet(ip)) || { fails: [], until: 0 };
  if (r.until && r.until > now) return; // already blocked; the 429 path reports it
  const fails = (r.fails || []).filter((t) => now - t < LOGIN_FAIL_WINDOW);
  fails.push(now);
  if (fails.length >= LOGIN_MAX_FAILS) { await loginLockSet(ip, { fails: [], until: now + LOGIN_BLOCK_MS }); auditOrg('login_blocked', { ip }); }
  else await loginLockSet(ip, { fails, until: 0 });
}
async function loginNoteOk(ip) { await loginLockSet(ip, null); }
// Typed public names land in world-readable Firebase records. Reject obvious
// employee-ID / email / phone shapes. Heuristic only - not a personal-data detector.
function publicNameProblem(s) {
  const v = String(s || '');
  if (/@/.test(v)) return 'looks like an email address';
  if (/(?:\+?\d[\d\s().-]{6,}\d)/.test(v)) return 'looks like a phone number';
  if (/^\d{4,}$/.test(v.trim())) return 'looks like an ID number';
  if (/\b(emp|employee|id)\s*[:#-]?\s*[a-z]*\d{3,}\b/i.test(v)) return 'looks like an ID number';
  return null;
}
const PUBLIC_NAME_MSG = 'Visible to everyone. Do not enter phone numbers or private details.';

// Scoring access, two ways in and no others:
// 1. A ground device the organiser approved for THIS match (scorer link key + its
//    approved device grant). The raw scorer link alone writes nothing - no events,
//    no undo, no video, no backup. Grants are match-scoped and expire.
// 2. The organiser's own signed-in session, scoped to proven ownership: a
//    tournament match needs the carried orgtour fingerprint, a practice match is
//    the organiser's own. No special-case bypass for any match.
async function organiserOwnsMatch(req, m) {
  if (!(await validOrgSession(req.headers['x-org-session']))) return false;
  const tid = m.config && m.config.tid;
  // Practice: only a match the organiser's session provably created. Tournament: the
  // carried orgtour ownership fingerprint. A bare session alone grants nothing.
  if (!tid) return (await store.getSetting('orgown_' + m.id)) === '1';
  const t = await store.tget(tid);
  return !!(t && (await store.getSetting('orgtour_' + tid)) === t.keyHash);
}
const SCORER_DEVICE_TTL_MS = 30 * 24 * 3600e3;
function scorerDevKey(matchId, token) { return 'scorerdev_' + matchId + '_' + sha(token); }
async function scorerDeviceGrant(matchId, token) {
  if (!/^[A-Za-z0-9]{16,80}$/.test(String(token || ''))) return null;
  let g = null; try { g = JSON.parse((await store.getSetting(scorerDevKey(matchId, token))) || 'null'); } catch (e) { g = null; }
  if (!g || g.matchId !== matchId) return null;
  if (g.exp && Date.parse(g.exp) < Date.now()) return null;
  return g.status === 'approved' ? 'approved' : (g.status === 'pending' ? 'pending' : (g.status === 'dismissed' ? 'dismissed' : null));
}
async function canScore(req, m) {
  if (keyEq(req.headers['x-scorer-key'], m.keyHash)) {
    const st = await scorerDeviceGrant(m.id, req.headers['x-scorer-device']);
    if (st === 'approved') return true;
  }
  return organiserOwnsMatch(req, m);
}

// Organiser-session scoring (ball-by-ball, video via his signed-in session) is an
// organiser action: a legacy (pre-policy) password blocks it until reset, exactly
// like the console routes. An APPROVED scorer device is the ground person's
// independent grant and is never blocked by the organiser password state. Auth
// failure keeps its own 403; only a valid organiser session gets the reset signal.
async function scoreResetBlocked(req, res, m) {
  if (keyEq(req.headers['x-scorer-key'], m.keyHash) &&
      (await scorerDeviceGrant(m.id, req.headers['x-scorer-device'])) === 'approved') return false;
  if (!(await organiserOwnsMatch(req, m))) return false;
  return orgResetBlocked(res);
}

// Match days are plain YYYY-MM-DD strings chosen by the user, never converted between timezones.
function cleanDate(d) {
  if (typeof d === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(d)) {
    const t = new Date(d + 'T00:00:00Z');
    if (!isNaN(t) && t.toISOString().slice(0, 10) === d && d >= '2020-01-01' && d <= '2100-12-31') return d;
  }
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
}
// Extracts a YouTube video id from the common link shapes, or accepts a bare id. Returns '' for empty input, null when unrecognised.
function ytId(u) {
  const s = String(u == null ? '' : u).trim();
  if (!s) return '';
  if (/^[A-Za-z0-9_-]{11}$/.test(s)) return s;
  const m = s.match(/(?:youtube\.com\/(?:watch\?[^#]*v=|live\/|embed\/|shorts\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/);
  return m ? m[1] : null;
}
// Team registration: same limits as match teams, so an approved team always fits a match.
function cleanTeamReg(body) {
  const name = engine.cleanName(body && body.name, 24);
  const raw = Array.isArray(body && body.players) ? body.players : [];
  const players = [];
  for (const p of raw) {
    const pn = engine.cleanName(p && p.name, 24);
    const pe = engine.cleanName(p && p.empId, 24);
    const pd = engine.cleanName(p && p.dept, 24);
    if (!pn && !pe && !pd) continue;
    if (!pn) return { error: 'Every player row needs a name' };
    if (!pe) return { error: 'Every player needs an employee ID' };
    if (players.some((x) => x.name.toLowerCase() === pn.toLowerCase())) continue;
    const pr = p && (p.role === 'c' || p.role === 'vc') ? p.role : '';
    players.push({ name: pn, empId: pe, dept: pd, role: pr });
  }
  let seenC = false, seenV = false;
  for (const pl of players) {
    if (pl.role === 'c') { if (seenC) pl.role = ''; else seenC = true; }
    else if (pl.role === 'vc') { if (seenV) pl.role = ''; else seenV = true; }
  }
  if (!name) return { error: 'Enter a team name' };
  if (publicNameProblem(name) || players.some((pl) => publicNameProblem(pl.name))) return { error: PUBLIC_NAME_MSG };
  if (players.length < MIN_TEAM_PLAYERS) return { error: 'Add at least ' + MIN_TEAM_PLAYERS + ' players with their employee IDs' };
  if (players.length > 15) return { error: 'A team can have at most 15 players' };
  return { name, players };
}
// Restore/read validation for stored tournament teams; drops anything malformed.
function cleanStoredTeams(list) {
  if (!Array.isArray(list)) return [];
  const out = [];
  for (const t of list) {
    if (!t || typeof t.id !== 'string' || !/^[a-z0-9]{6,12}$/.test(t.id)) continue;
    const name = engine.cleanName(t.name, 24);
    const players = Array.isArray(t.players) ? t.players.map((p) => {
      if (typeof p === 'string') { const sn = engine.cleanName(p, 24); return sn ? { name: sn, empId: '', dept: '' } : null; }
      const pn = engine.cleanName(p && p.name, 24);
      return pn ? { name: pn, empId: engine.cleanName(p && p.empId, 24), dept: engine.cleanName(p && p.dept, 24), role: p && (p.role === 'c' || p.role === 'vc') ? p.role : '' } : null;
    }).filter(Boolean).slice(0, 15) : [];
    if (!name || players.length < 2) continue;
    out.push({ id: t.id, name, players, status: t.status === 'approved' ? 'approved' : 'pending', submittedAt: typeof t.submittedAt === 'string' ? t.submittedAt.slice(0, 40) : new Date().toISOString() });
    if (out.length >= MAX_TEAMS_PER_TOURNAMENT) break;
  }
  return out;
}
// Public employee fields are authorised only for this tournament's approved teams.
const PUBLIC_DETAIL_TOURNAMENT = 'whxrz4tw';
function registeredTeamRefs(m, t) {
  if (!t || t.id !== PUBLIC_DETAIL_TOURNAMENT || m.config.tid !== t.id) return [null, null];
  const approved = cleanStoredTeams(t.teams).filter(x => x.status === 'approved');
  return m.config.teams.map(team => {
    const same = approved.filter(x => x.name === team.name && JSON.stringify(x.players.map(p => p.name)) === JSON.stringify(team.players));
    const found = team.registrationId ? same.find(x => x.id === team.registrationId) : (same.length === 1 ? same[0] : null);
    return found ? {id: found.id, tournamentId: t.id} : null;
  });
}
function summary(m, tournament) {
  const st = engine.view(m.config, m.events);
  return { id: m.id, title: m.config.title, date: m.config.date || null, teams: m.config.teams.map((t) => t.name), teamRefs: registeredTeamRefs(m, tournament),
    innings: st.innings.filter((i) => i.balls > 0 || i.summary).map((i) => ({ team: i.batName, runs: i.runs, wkts: i.wkts, overs: i.overs })),
    result: st.result ? st.result.text : null, status: m.events.length === 0 ? 'new' : (st.result ? 'done' : 'live') };
}
const bad = new Map();
function badAuth(ip) { const now = Date.now(); return (bad.get(ip) || []).filter((t) => now - t < 3600e3).length >= 10; }
function noteBad(ip) { const now = Date.now(); const a = (bad.get(ip) || []).filter((t) => now - t < 3600e3); a.push(now); bad.set(ip, a); }
const MAX_TOURNAMENTS = 100, MAX_PER_TOURNAMENT = 200, MAX_TEAMS_PER_TOURNAMENT = 40;
const MIN_TEAM_PLAYERS = 2; // minimum players per registered team (organiser policy; raise to 15 if every slot is mandatory)
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
const STATIC_OK = new Set(['index.html','camera.html','invite.html']);
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

  // ---------- organiser sign-in for the main-screen organiser area ----------
  if (p === '/api/auth/status' && req.method === 'GET') {
    return send(res, 200, { setup: !!(await store.getSetting('orgpass')) });
  }
  if (p === '/api/auth/tournaments' && (req.method === 'GET' || req.method === 'POST')) {
    if (!(await validOrgSession(req.headers['x-org-session']))) return send(res,403,{error:'Sign in as organiser first.'});
    if (await orgResetBlocked(res)) return;
    if (req.method === 'POST') {
      const ip=(req.headers['x-forwarded-for']||req.socket.remoteAddress||'').split(',')[0].trim();
      if(badAuth(ip))return send(res,429,{error:'Too many failed attempts. Try again later.'});
      const body=await readBody(req,2000),tid=String(body&&body.tid||'');
      const t=/^[a-z0-9]{6,12}$/.test(tid)?await store.tget(tid):null;
      if (!t || !keyEq(body&&body.orgKey,t.keyHash)) {noteBad(ip);return send(res,403,{error:'Open this tournament on the device holding its private organiser link.'});}
      await store.setSetting('orgtour_'+t.id,t.keyHash);
      return send(res,200,{ok:true});
    }
    // All tournaments are listed so the console can re-establish organiser links after
    // a rotation; 'saved' marks the ones whose current link fingerprint this account holds.
    const tournaments=[];
    for(const t of await store.allTournaments()) {
      tournaments.push({id:t.id,name:t.name,saved:(await store.getSetting('orgtour_'+t.id))===t.keyHash});
    }
    return send(res,200,{tournaments});
  }
  if (p === '/api/auth/setup' && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    // Bootstrap only: an organiser link can set the FIRST password, never replace an
    // existing one. Replacing requires a signed-in session (change-password below).
    if (await orgpassRec()) return send(res, 409, { error: 'Password is already set. Change it from the organiser area after signing in.' });
    const body = await readBody(req, 2e3);
    const tid = String((body && body.tid) || '');
    const t = /^[a-z0-9]{6,12}$/.test(tid) ? await store.tget(tid) : null;
    if (!t || !keyEq(body && body.orgKey, t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'A valid organiser link on this device is needed to set the password' }); }
    const pw = String((body && body.password) || '');
    if (pw.length < 10 || pw.length > 72) return send(res, 400, { error: 'Password needs 10 to 72 characters' });
    const salt = rnd(16);
    const hash = crypto.scryptSync(pw, salt, 32).toString('hex');
    await store.setSetting('orgpass', JSON.stringify({ salt, hash, v: 2 }));
    auditOrg('password_bootstrapped', { ip });
    return send(res, 200, { ok: true });
  }
  if (p === '/api/auth/change-password' && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    if (!(await orgpassRec())) return send(res, 409, { error: 'Sign-in is not set up yet' });
    const token = req.headers['x-org-session'];
    if (!(await validOrgSession(token))) return send(res, 403, { error: 'Sign in as organiser first.' });
    const body = await readBody(req, 2e3);
    const pw = String((body && body.password) || '');
    if (pw.length < 10 || pw.length > 72) return send(res, 400, { error: 'Password needs 10 to 72 characters' });
    const salt = rnd(16);
    const hash = crypto.scryptSync(pw, salt, 32).toString('hex');
    await store.setSetting('orgpass', JSON.stringify({ salt, hash, v: 2 }));
    // His rule: every OTHER device signs out; this device keeps its session. If the
    // caller's valid session is the legacy shared record, re-mint it in the current
    // per-token format first so the caller is not signed out with it.
    const keep = 'orgsess_' + sha(token);
    let shared = null; try { shared = JSON.parse((await store.getSetting('orgsess')) || 'null'); } catch (e) { shared = null; }
    if (shared && shared.exp > Date.now() && keyEq(token, shared.h)) await store.setSetting(keep, JSON.stringify({ h: sha(token), exp: shared.exp }));
    await store.clearSettings('orgsess_', keep);
    await store.setSetting('orgsess', null);
    auditOrg('password_changed', { ip });
    return send(res, 200, { ok: true });
  }
  if (p === '/api/auth/login' && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (await loginBlocked(ip)) return send(res, 429, { error: 'Too many wrong tries. Wait 5 minutes and try again.' });
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const body = await readBody(req, 2e3);
    const ph = await orgpassRec();
    if (!ph) return send(res, 409, { error: 'Sign-in is not set up yet' });
    const user = String((body && body.user) || '').trim().toLowerCase();
    const pw = String((body && body.password) || '');
    const ok = !!(user === 'denny' && pw.length > 0 && pw.length <= 72 &&
      crypto.timingSafeEqual(Buffer.from(crypto.scryptSync(pw, ph.salt, 32).toString('hex')), Buffer.from(String(ph.hash))));
    if (!ok) { noteBad(ip); await loginNoteFail(ip); auditOrg('login_failed', { ip }); return send(res, 403, { error: 'Wrong username or password' }); }
    await loginNoteOk(ip);
    const token = rnd(24);
    await store.setSetting('orgsess_'+sha(token),JSON.stringify({h:sha(token),exp:Date.now()+30*864e5}));
    auditOrg('login_ok', { ip });
    return send(res, 200, { ok: true, token, resetRequired: ph.v !== 2 });
  }
  if (p === '/api/auth/check' && req.method === 'POST') {
    const body = await readBody(req, 2e3);
    const ok=await validOrgSession(body&&body.token);
    return send(res, 200, { ok, resetRequired: ok ? await orgpassLegacy() : false });
  }
  if (p === '/api/auth/logout' && req.method === 'POST') {
    const body=await readBody(req,2e3),token=body&&body.token;
    if(!(await validOrgSession(token)))return send(res,403,{error:'Valid session required'});
    await store.setSetting('orgsess_'+sha(token),null);
    let legacy=null;try{legacy=JSON.parse((await store.getSetting('orgsess'))||'null')}catch(e){}
    if(legacy&&keyEq(token,legacy.h))await store.setSetting('orgsess',null);
    return send(res, 200, { ok: true });
  }
  let mt;
  if (p === '/api/admin/delete-match' || p === '/api/backup' || p === '/api/restore' || p === '/api/admin/tournament-name' || p === '/api/admin/tournament-listed' || p === '/api/admin/replace-matches' || p === '/api/admin/firebase-migrate' || p === '/api/admin/firebase-reconcile' || p === '/api/admin/security-state' || p === '/api/admin/rotate-organiser-keys') {
    // Owner-only. Disabled unless BACKUP_TOKEN is set. Backups contain match data and hashed keys, never raw keys.
    const tok = process.env.BACKUP_TOKEN;
    if (!tok || tok.length < 24) return send(res, 404, { error: 'Unknown API' });
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    if (!keyEq(req.headers['x-backup-token'], sha(tok))) { noteBad(ip); return send(res, 403, { error: 'Not allowed' }); }
    if (p === '/api/admin/security-state' && req.method === 'GET') {
      // Safe inspection for the security review: counts and expiry times only -
      // never tokens, hashes, passwords or link material.
      const ph = await orgpassRec();
      const sess = await store.listSettings('orgsess');
      const sessions = [];
      for (const row of sess) {
        let rec = null; try { rec = JSON.parse(row.v); } catch (e) { rec = null; }
        sessions.push({ kind: row.k === 'orgsess' ? 'legacy-shared' : 'token', expires: rec && rec.exp ? new Date(rec.exp).toISOString() : null });
      }
      const locks = (await store.listSettings('loginlock_')).map((row) => { let r = null; try { r = JSON.parse(row.v); } catch (e) { r = null; } return { ip: row.k.slice('loginlock_'.length), blockedUntil: r && r.until ? new Date(r.until).toISOString() : null, recentFails: r && r.fails ? r.fails.length : 0 }; });
      return send(res, 200, { passwordSet: !!ph, passwordVersion: ph && ph.v === 2 ? 2 : ph ? 1 : null, sessions, loginLocks: locks });
    }
    if (p === '/api/admin/rotate-organiser-keys' && req.method === 'POST') {
      // His explicit request: cancel every existing tournament organiser link at
      // cutover. New random keyHashes are stored and the raw keys DISCARDED - every
      // old link dies immediately and no valid link exists until he creates a new one
      // himself from the signed-in organiser console (organiser-link route below).
      // Touches organiser keys only: scorer, registration and viewer links unchanged,
      // no tournament/match/team data modified.
      const ts = await store.allTournaments();
      let rotated = 0;
      for (const t of ts) {
        const newHash = sha(rnd(14)); // raw key discarded: every old link dies, no valid link remains
        if (await store.tsetKey(t.id, newHash)) {
          // Same operation: an existing saved fingerprint moves to the new hash, so the
          // signed-in owner's saved access survives rotation; no new device is authorised.
          if ((await store.getSetting('orgtour_' + t.id)) === t.keyHash) await store.setSetting('orgtour_' + t.id, newHash);
          rotated++;
        }
      }
      auditOrg('organiser_keys_invalidated', { count: rotated });
      return send(res, 200, { ok: true, rotated });
    }
    if (p === '/api/backup' && req.method === 'GET') {
      const [ms, ts] = [await store.allMatches(), await store.allTournaments()];
      return send(res, 200, { format: 'cricket-backup', version: 1, exportedAt: new Date().toISOString(), storage: store.kind,
        counts: { matches: ms.length, tournaments: ts.length }, tournaments: ts, matches: ms });
    }
    if (p === '/api/admin/firebase-migrate' && req.method === 'POST') {
      // One-time INSERT-only copy of the public tournament(s) and their matches into
      // nexoraCentre/. Never overwrites an existing Firebase record, never deletes or
      // rewrites any Postgres data (the Postgres originals stay exactly as they were -
      // they become the recovery replica). Default is a dry run; {apply:true} executes.
      // Unlisted tournaments (e.g. hovxdntl) are skipped unless {includeUnlisted:true}
      // is sent, which only happens after the owner confirms the audience for that data.
      // Firebase records NEVER carry employee IDs/departments, for any tournament.
      const body = await readBody(req, 2000);
      const dry = !body || body.apply !== true;
      const includeUnlisted = !!(body && body.includeUnlisted === true);
      const fbd = require('./firebase-db');
      const out = { dryRun: dry, tournaments: [], matches: [], skipped: [], errors: [] };
      for (const tid of ['whxrz4tw', 'hovxdntl']) {
        try {
          const t = await store.tget(tid);
          if (!t) { out.skipped.push({ tournament: tid, reason: 'missing' }); continue; }
          if (!t.listed && !includeUnlisted) { out.skipped.push({ tournament: tid, reason: 'unlisted - owner audience approval required' }); continue; }
          const existing = await fbd.get('tournaments/' + tid + '/info');
          if (existing) { out.skipped.push({ tournament: tid, reason: 'already in Firebase' }); }
          else {
            const approved = cleanStoredTeams(t.teams).filter((x) => x.status === 'approved');
            const teamMap = {};
            for (const tm of approved) teamMap[tm.id] = { id: tm.id, name: tm.name, submittedAt: tm.submittedAt,
              players: tm.players.map((p) => ({ name: p.name, role: p.role || '' })) };
            out.tournaments.push({ id: tid, name: t.name, approvedTeams: approved.length });
            if (!dry) {
              await fbd.put('tournaments/' + tid + '/teams', teamMap);
              await fbd.put('tournaments/' + tid + '/info', { name: t.name, listed: !!t.listed, created: t.created || new Date().toISOString(), registration: !!t.regKeyHash });
              await store.manifestSet(tid, 'tournament');
            }
          }
          for (const m of await store.byTournament(tid)) {
            const mpath = 'tournaments/' + tid + '/matches/' + m.id;
            const have = await fbd.get(mpath);
            if (have) { out.skipped.push({ match: m.id, reason: 'already in Firebase' }); continue; }
            out.matches.push({ id: m.id, tournament: tid, events: (m.events || []).length });
            if (!dry) { await fbd.put(mpath, fbMatchRec(m.config, m.events || [], m.rev || 0, m.created || m.updated, m.updated)); await store.manifestSet(m.id, 'match'); }
          }
        } catch (e) { out.errors.push({ tournament: tid, error: String((e && e.message) || e).slice(0, 200) }); }
      }
      return send(res, 200, out);
    }
    if (p === '/api/admin/firebase-reconcile' && req.method === 'POST') {
      // Rollback/convergence machinery: converge the Postgres recovery replica to the
      // canonical Firebase state (and lift degraded creates into Firebase). Dry run by
      // default; {apply:true} executes. Every repair write snapshots the previous replica
      // into cricket_replica_snapshots first (append-only). Never deletes anything.
      const body = await readBody(req, 2000);
      return send(res, 200, await store.reconcileFromFirebase(!body || body.apply !== true));
    }
    if (p === '/api/admin/delete-match' && req.method === 'POST') {
      const body=await readBody(req,5e6),m=body&&body.expected;
      if(!body || body.confirm!=='delete-only-backed-up-match' || !m || !/^[a-z0-9]{6,12}$/.test(String(m.id||'')) || !m.config || !Array.isArray(m.events) || !Number.isInteger(m.rev) || !/^[a-f0-9]{64}$/.test(String(m.keyHash||'')))return send(res,400,{error:'Need one exact backed-up match'});
      if(m.id==='49asxecn')return send(res,403,{error:'The ceremonial match is protected from this operation'});
      if(!(await store.deleteBackedUpMatch(m)))return send(res,409,{error:'Match missing or changed since backup. Nothing deleted.'});
      return send(res,200,{ok:true,deletedId:m.id,removed:1});
    }
    if (p === '/api/admin/replace-matches' && req.method === 'POST') {
      const body = await readBody(req, 5e6);
      if (!body || body.confirm !== 'replace-backed-up-matches' || !Array.isArray(body.expected) || body.expected.length > MAX_MATCHES) return send(res,400,{error:'Need exact backed-up match snapshot'});
      const source = body.expected.find(m=>m.id===body.ceremonySource);
      const ninja = engine.ninjaViewUrl(body.ninjaView);
      if (!source || !source.config || !ninja) return send(res,400,{error:'Need backed-up ceremony and receiving-only viewing URL'});
      const config = Object.assign({},source.config,{ninjaView:ninja}); delete config.videoId;
      try { engine.replay(config,[]); } catch(e) { return send(res,400,{error:'Invalid ceremony setup'}); }
      const id=rnd(8).toLowerCase(),key=rnd(14);
      if (!(await store.replaceAllMatches(body.expected,{id,config,keyHash:sha(key)}))) return send(res,409,{error:'Matches changed since backup. No matches removed. Back up again.'});
      return send(res,200,{ok:true,removed:body.expected.length,id,scorerKey:key,viewerPath:'/m/'+id,scorerPath:'/s/'+id});
    }
    if (p === '/api/admin/tournament-name' && req.method === 'POST') {
      const body = await readBody(req, 2e3);
      const nm = body && engine.cleanName(body.name, 40);
      if (!body || !/^[a-z0-9]{6,12}$/.test(String(body.id || '')) || !nm) return send(res, 400, { error: 'Need tournament id and a name up to 40 characters' });
      return (await store.trename(body.id, nm)) ? send(res, 200, { ok: true, id: body.id, name: nm }) : send(res, 404, { error: 'Tournament not found' });
    }
    if (p === '/api/admin/tournament-listed' && req.method === 'POST') {
      const body = await readBody(req, 2e3);
      if (!body || !/^[a-z0-9]{6,12}$/.test(String(body.id || ''))) return send(res, 400, { error: 'Need tournament id' });
      return (await store.tsetListed(body.id, !!body.listed)) ? send(res, 200, { ok: true, id: body.id, listed: !!body.listed }) : send(res, 404, { error: 'Tournament not found' });
    }
    if (p === '/api/restore' && req.method === 'POST') {
      const body = await readBody(req, 5e6);
      if (!body || body.format !== 'cricket-backup' || body.version !== 1 || !Array.isArray(body.matches) || !Array.isArray(body.tournaments)) return send(res, 400, { error: 'Not a valid backup file' });
      const idOk = (x) => typeof x === 'string' && /^[a-z0-9]{6,12}$/.test(x), hashOk = (x) => typeof x === 'string' && /^[a-f0-9]{64}$/.test(x);
      const out = { tournaments: { added: 0, skipped: 0, rejected: 0 }, matches: { added: 0, skipped: 0, rejected: 0 } };
      for (const t of body.tournaments) {
        if (!t || !idOk(t.id) || !hashOk(t.keyHash) || !engine.cleanName(t.name, 40)) { out.tournaments.rejected++; continue; }
        if (t.regKeyHash !== undefined && t.regKeyHash !== null && !hashOk(t.regKeyHash)) { out.tournaments.rejected++; continue; }
        (await store.importTournament({ id: t.id, name: engine.cleanName(t.name, 40), keyHash: t.keyHash, created: t.created, teams: cleanStoredTeams(t.teams), regKeyHash: t.regKeyHash || null, listed: t.listed === true })) ? out.tournaments.added++ : out.tournaments.skipped++;
      }
      for (const m of body.matches) {
        let ok = m && idOk(m.id) && hashOk(m.keyHash) && Array.isArray(m.events) && m.events.length <= 800 && m.config && typeof m.config === 'object';
        let cfg = null;
        if (ok) { try { cfg = Object.assign(engine.normalizeConfig(m.config), { date: cleanDate(m.config.date) }); if (idOk(m.config.tid)) cfg.tid = m.config.tid; engine.replay(cfg, m.events); } catch (e) { ok = false; } }
        if (!ok) { out.matches.rejected++; continue; }
        (await store.importMatch({ id: m.id, config: cfg, events: m.events, keyHash: m.keyHash, rev: Number.isInteger(m.rev) ? m.rev : m.events.length, updated: m.updated || new Date().toISOString(), created: m.created })) ? out.matches.added++ : out.matches.skipped++;
      }
      return send(res, 200, out);
    }
    return send(res, 405, { error: 'Method not allowed' });
  }
  if (req.method === 'POST' && p === '/api/tournaments') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (limited(ip)) return send(res, 429, { error: 'Too many requests from this connection. Try again later.' });
    // Bootstrap: before the FIRST password exists, tournament creation is open so the
    // first organiser link can exist for the setup ceremony. Once any password is set
    // (always the case in production), creation needs a signed-in, post-reset session.
    if (await orgpassRec()) {
      if (!(await validOrgSession(req.headers['x-org-session']))) return send(res, 403, { error: 'Sign in as organiser first.' });
      if (await orgResetBlocked(res)) return;
    }
    if ((await store.tcount()) >= MAX_TOURNAMENTS) return send(res, 507, { error: 'Storage limit reached.' });
    const body = await readBody(req);
    const name = engine.cleanName(body.name, 40);
    if (!name) return send(res, 400, { error: 'Enter a tournament name' });
    if (publicNameProblem(name)) return send(res, 400, { error: PUBLIC_NAME_MSG });
    const id = rnd(8).toLowerCase(), key = rnd(14);
    const committed = await store.tcreate({ id, name, keyHash: sha(key) });
    await store.setSetting('orgtour_' + id, sha(key));
    const out = { id, name, orgKey: key, calendarPath: '/t/' + id };
    if (committed === false) { out.degraded = true; out.notice = 'Saved on the server. Firebase sync is pending.'; }
    return send(res, 201, out);
  }
  // A ground device claims scoring for one match with its scorer link. The organiser
  // approves the pending claim remotely from the console; the raw link alone writes nothing.
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/scorer-claim$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip) || limited(ip)) return send(res, 429, { error: 'Too many attempts. Try again later.' });
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!keyEq(req.headers['x-scorer-key'], m.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Use this match\'s scorer link.' }); }
    const body = await readBody(req, 2000);
    const label = String((body && body.label) || '').replace(/[^A-Za-z0-9 .\-]/g, '').trim().slice(0, 40);
    const token = rnd(24);
    const now = Date.now();
    await store.setSetting(scorerDevKey(m.id, token), JSON.stringify({
      matchId: m.id, status: 'pending', label,
      created: new Date(now).toISOString(), exp: new Date(now + SCORER_DEVICE_TTL_MS).toISOString(),
    }));
    auditOrg('scorer_device_claimed', { match: m.id, ip });
    return send(res, 201, { ok: true, device: token, status: 'pending' });
  }
  // The claiming device polls its own status while it waits for approval.
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/scorer-claim$/)) && req.method === 'GET') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!keyEq(req.headers['x-scorer-key'], m.keyHash)) return send(res, 403, { error: 'Use this match\'s scorer link.' });
    return send(res, 200, { status: (await scorerDeviceGrant(m.id, req.headers['x-scorer-device'])) || 'none' });
  }
  // Console: list pending/approved ground devices across matches. Tokens never leave
  // the server - claims are identified by match, label, time and a short suffix.
  if (p === '/api/scorer-devices' && req.method === 'GET') {
    if (!(await validOrgSession(req.headers['x-org-session']))) return send(res, 401, { error: 'Sign in as organiser.' });
    if (await orgpassLegacy()) return send(res, 403, { error: 'Set a new organiser password first.', resetRequired: true });
    const rows = await store.listSettings('scorerdev_');
    const out = [];
    for (const r of rows) {
      let g = null; try { g = JSON.parse(r.v); } catch (e) { continue; }
      if (!g || !g.matchId) continue;
      if (g.status === 'dismissed') continue;
      const expired = g.exp && Date.parse(g.exp) < Date.now();
      const dm = await store.get(g.matchId);
      out.push({ matchId: g.matchId, title: (dm && dm.config && dm.config.title) || '', claim: r.k.slice(-8), status: expired ? 'expired' : g.status, label: g.label || '', created: g.created, exp: g.exp });
    }
    out.sort((a, b) => String(a.created).localeCompare(String(b.created)));
    return send(res, 200, { devices: out });
  }
  // Console: approve or revoke one pending/approved claim. Ownership-scoped: the
  // organiser session must hold proven ownership of the claim's match.
  if (p === '/api/scorer-devices' && req.method === 'POST') {
    const body = await readBody(req, 2000);
    const matchId = body && typeof body.matchId === 'string' ? body.matchId : '';
    const claim = body && typeof body.claim === 'string' ? body.claim : '';
    const action = body && body.action;
    if (!/^[a-z0-9]{6,12}$/.test(matchId) || !/^[a-f0-9]{8}$/.test(claim) || (action !== 'approve' && action !== 'revoke' && action !== 'dismiss')) {
      return send(res, 400, { error: 'matchId, claim and action (approve|revoke|dismiss) required' });
    }
    const m = await store.get(matchId);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!(await organiserOwnsMatch(req, m))) return send(res, 403, { error: 'Sign in as organiser on a device holding this tournament\'s saved access.' });
    if (await orgResetBlocked(res)) return;
    const rows = await store.listSettings('scorerdev_' + matchId + '_');
    const row = rows.find((r) => r.k.slice(-8) === claim);
    if (!row) return send(res, 404, { error: 'Claim not found' });
    let g = null; try { g = JSON.parse(row.v); } catch (e) { g = null; }
    if (!g) return send(res, 404, { error: 'Claim not found' });
    if (action === 'dismiss' && g.status !== 'pending') return send(res, 409, { error: 'Only a claim still waiting for approval can be dismissed.' });
    g.status = action === 'approve' ? 'approved' : (action === 'dismiss' ? 'dismissed' : 'revoked');
    await store.setSetting(row.k, JSON.stringify(g));
    auditOrg(action === 'approve' ? 'scorer_device_approved' : (action === 'dismiss' ? 'scorer_device_dismissed' : 'scorer_device_revoked'), { match: matchId, claim });
    return send(res, 200, { ok: true, status: g.status });
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/backup$/)) && req.method === 'GET') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!(await canScore(req,m))) { noteBad(ip); return send(res, 403, { error: 'Only an authorized scorer can download the match backup.' }); }
    return send(res, 200, { format: 'cricket-backup', version: 1, exportedAt: new Date().toISOString(), storage: store.kind, counts: { matches: 1, tournaments: 0 }, tournaments: [], matches: [{ id: m.id, config: m.config, events: m.events, keyHash: m.keyHash, rev: m.rev, updated: m.updated }] });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/backup$/)) && req.method === 'GET') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!(await orgAuthed(req, t))) { noteBad(ip); return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' }); }
    if (await orgResetBlocked(res)) return;
    const ms = await store.byTournament(t.id);
    return send(res, 200, { format: 'cricket-backup', version: 1, exportedAt: new Date().toISOString(), storage: store.kind, counts: { matches: ms.length, tournaments: 1 }, tournaments: [{ id: t.id, name: t.name, keyHash: t.keyHash, teams: cleanStoredTeams(t.teams), regKeyHash: t.regKeyHash || null }], matches: ms.map((m) => ({ id: m.id, config: m.config, events: m.events, keyHash: m.keyHash, rev: m.rev, updated: m.updated })) });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/team-details$/)) && req.method === 'GET') {
    if (mt[1] !== PUBLIC_DETAIL_TOURNAMENT) return send(res, 404, {error:'Team details are not public for this tournament.'});
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, {error:'Tournament not found'});
    return send(res, 200, {id:t.id,name:t.name,teams:cleanStoredTeams(t.teams).filter(x=>x.status==='approved').map(x=>({id:x.id,name:x.name,players:x.players.map(p=>({name:p.name,empId:p.empId||'',dept:p.dept||''}))}))});
  }
  if (p === '/api/tournaments/listed' && req.method === 'GET') {
    return send(res, 200, { tournaments: await store.listedTournaments() });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})$/)) && req.method === 'GET') {
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    const ms = await store.byTournament(t.id);
    const teams = cleanStoredTeams(t.teams);
    const resp = { id: t.id, name: t.name, listed: !!t.listed, matches: ms.map(m => summary(m, t)),
      teams: teams.filter((x) => x.status === 'approved').map((x) => ({ id: x.id, name: x.name, players: x.players.map((p) => p.name + (p.role === 'c' ? ' (C)' : p.role === 'vc' ? ' (VC)' : '')) })) };
    const orgKey = req.headers['x-org-key'];
    if (orgKey) {
      const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
      if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
      if (!keyEq(orgKey, t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Not allowed' }); }
      resp.registration = t.regKeyHash ? 'open' : 'closed';
      resp.pendingTeams = teams.filter((x) => x.status === 'pending').map((x) => ({ id: x.id, name: x.name, players: x.players }));
      resp.approvedTeams = teams.filter((x) => x.status === 'approved').map((x) => ({ id: x.id, name: x.name, players: x.players }));
    }
    return send(res, 200, resp);
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/reglink$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!(await orgAuthed(req, t))) { noteBad(ip); return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' }); }
    if (await orgResetBlocked(res)) return;
    const body = await readBody(req, 2e3);
    const rk = String(body && body.regKey || '');
    if (!rk) { await store.tsetReg(t.id, null); return send(res, 200, { ok: true, registration: 'closed' }); }
    if (!/^[A-Za-z0-9]{10,24}$/.test(rk)) return send(res, 400, { error: 'Bad registration key' });
    await store.tsetReg(t.id, sha(rk));
    return send(res, 200, { ok: true, registration: 'open' });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/organiser-link$/)) && req.method === 'POST') {
    // His private retrieval path after rotation: a signed-in, post-reset session creates
    // a FRESH organiser link for one tournament. The old link for that tournament dies
    // here. The raw key is returned once, to this signed-in device only - it is never
    // logged, never stored readable, and never sent anywhere else. Saved access
    // (orgtour_ fingerprint) moves to the new key in the same operation.
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    if (!(await validOrgSession(req.headers['x-org-session']))) return send(res, 403, { error: 'Sign in as organiser first.' });
    if (await orgResetBlocked(res)) return;
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    const key = rnd(14);
    const h = sha(key);
    if (!(await store.tsetKey(t.id, h))) return send(res, 500, { error: 'Could not create the link. Try again.' });
    await store.setSetting('orgtour_' + t.id, h);
    auditOrg('organiser_link_created', { ip, tournament: t.id });
    return send(res, 200, { ok: true, id: t.id, orgKey: key, organiserPath: '/t/' + t.id + '?key=' + key });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/listed$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!(await orgAuthed(req, t))) { noteBad(ip); return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' }); }
    if (await orgResetBlocked(res)) return;
    const body = await readBody(req, 2e3);
    await store.tsetListed(t.id, !!(body && body.listed));
    return send(res, 200, { ok: true, listed: !!(body && body.listed) });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/teams$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (limited(ip)) return send(res, 429, { error: 'Too many requests from this connection. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    if (!t.regKeyHash || !keyEq(req.headers['x-reg-key'], t.regKeyHash)) { noteBad(ip); return send(res, 403, { error: 'This registration link is not active. Ask the organiser for the current link.' }); }
    const body = await readBody(req);
    const c = cleanTeamReg(body);
    if (c.error) return send(res, 400, { error: c.error });
    let full = false, dupe = false;
    const okm = await store.tmutateTeams(t.id, (teams) => {
      if (teams.length >= MAX_TEAMS_PER_TOURNAMENT) { full = true; return null; }
      if (teams.some((x) => x.name.toLowerCase() === c.name.toLowerCase())) { dupe = true; return null; }
      return teams.concat([{ id: rnd(8).toLowerCase(), name: c.name, players: c.players, status: 'pending', submittedAt: new Date().toISOString() }]);
    });
    if (full) return send(res, 507, { error: 'This tournament has reached its team limit.' });
    if (dupe) return send(res, 409, { error: 'A team with this name is already registered.' });
    if (!okm) return send(res, 409, { error: 'Tournament changed on another device. Try again.' });
    return send(res, 201, { ok: true });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/teams\/([a-z0-9]{6,12})$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!(await orgAuthed(req, t))) { noteBad(ip); return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' }); }
    if (await orgResetBlocked(res)) return;
    const body = await readBody(req, 2e3);
    let notFound = false, badAction = false, badPlayer = false;
    const okm = await store.tmutateTeams(t.id, (teams) => {
      const ix = teams.findIndex((x) => x.id === mt[2]);
      if (ix === -1) { notFound = true; return null; }
      const next = teams.map((x) => Object.assign({}, x, { players: (x.players || []).map((pl) => Object.assign({}, pl)) }));
      if (body && body.action === 'approve') next[ix].status = 'approved';
      else if (body && body.action === 'remove') next.splice(ix, 1);
      else if (body && body.action === 'role') {
        const pix = typeof body.player === 'number' && Number.isInteger(body.player) ? body.player : -1;
        const role = body.role === 'c' || body.role === 'vc' ? body.role : '';
        if (pix < 0 || pix >= next[ix].players.length) { badPlayer = true; return null; }
        if (role) next[ix].players.forEach((pl, i2) => { if (i2 !== pix && pl.role === role) pl.role = ''; });
        next[ix].players[pix].role = role;
      }
      else { badAction = true; return null; }
      return next;
    });
    if (notFound) return send(res, 404, { error: 'Team not found' });
    if (badPlayer) return send(res, 400, { error: 'Unknown player' });
    if (badAction) return send(res, 400, { error: 'Unknown action' });
    if (!okm) return send(res, 409, { error: 'Tournament changed on another device. Try again.' });
    return send(res, 200, { ok: true });
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/attach$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!keyEq(req.headers['x-scorer-key'], m.keyHash)) return send(res, 403, { error: 'Use this match\'s scorer link to add it to a tournament.' });
    const body = await readBody(req);
    const t = await store.tget(String(body.tournament || ''));
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!(await orgAuthed(req, t))) return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' });
    if (await orgResetBlocked(res)) return;
    if (m.config.tid && m.config.tid !== t.id) return send(res, 409, { error: 'This match already belongs to a tournament.' });
    if (!m.config.tid && (await store.byTournament(t.id)).length >= MAX_PER_TOURNAMENT) return send(res, 507, { error: 'Tournament is full.' });
    const config = Object.assign({}, m.config, { tid: t.id, date: cleanDate(body.date || m.config.date) });
    await store.setConfig(m.id, config);
    return send(res, 200, { ok: true });
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/video$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!(await canScore(req,m))) return send(res, 403, { error: 'Use this match\'s scorer link or verified organiser access to set the live video.' });
    if (await scoreResetBlocked(req, res, m)) return;
    const body = await readBody(req);
    const source = body.source || 'youtube';
    if (!['youtube','ninja'].includes(source)) return send(res, 400, {error:'Choose YouTube or VDO.Ninja.'});
    const raw = String(body.video || '').trim();
    const vid = source === 'youtube' ? ytId(raw) : '';
    const ninja = source === 'ninja' && raw ? engine.ninjaViewUrl(raw) : '';
    if (source === 'youtube' && vid === null) return send(res, 400, {error:'That does not look like a YouTube link.'});
    if (source === 'ninja' && raw && !ninja) return send(res, 400, {error:'Paste the HTTPS vdo.ninja VIEW link, not a camera, room or director link.'});
    const config = Object.assign({}, m.config);
    delete config.videoId; delete config.ninjaView;
    if (vid) config.videoId = vid;
    if (ninja) config.ninjaView = ninja;
    await store.setConfig(m.id, config, true);
    const m2 = await store.get(m.id);
    return send(res, 200, {ok:true,videoId:vid||null,ninjaView:ninja||null,rev:m2?m2.rev:undefined});
  }
  if (req.method === 'POST' && p === '/api/matches') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (limited(ip)) return send(res, 429, { error: 'Too many matches created from this connection. Try again later.' });
    if ((await store.count()) >= MAX_MATCHES) return send(res, 507, { error: 'Storage limit reached for the free trial.' });
    const body = await readBody(req);
    const config = engine.normalizeConfig(body);
    // Names typed here land in world-readable Firebase records (PUBLIC_NAME_MSG).
    const nameFields = [config.title].concat(config.teams.map((t) => t.name), ...config.teams.map((t) => t.players));
    if (nameFields.some((s) => publicNameProblem(s))) return send(res, 400, { error: PUBLIC_NAME_MSG });
    config.date = cleanDate(body.date);
    if (body.tournament) {
      // Tournament-linked create needs the canonical roster to authorise and de-dupe.
      // During a Firebase outage it must FAIL LOUD with a clear message (never fall back
      // to a stale roster for auth) - but not with the generic 'Server error'. A
      // canonical-missing signal (data loss) still propagates as a loud 500.
      let t;
      try { t = await store.tget(String(body.tournament)); }
      catch (e) { if (e && e.canonical) throw e; return send(res, 503, { error: 'The live store is unreachable right now, so tournament matches cannot be created. Try again shortly.' }); }
      if (!t) return send(res, 404, { error: 'Tournament not found' });
      if (!(await orgAuthed(req, t)) && !(await savedOrgTournament(req,t))) return send(res, 403, { error: 'Sign in as organiser on the device holding this tournament organiser link.' });
      if (await orgResetBlocked(res)) return;
      let curCount;
      try { curCount = (await store.byTournament(t.id)).length; }
      catch (e) { if (e && e.canonical) throw e; return send(res, 503, { error: 'The live store is unreachable right now, so tournament matches cannot be created. Try again shortly.' }); }
      if (curCount >= MAX_PER_TOURNAMENT) return send(res, 507, { error: 'Tournament is full.' });
      config.tid = t.id;
      const approved = cleanStoredTeams(t.teams).filter(x=>x.status==='approved');
      config.teams.forEach((team,i)=>{const rid=body.teams[i]&&body.teams[i].registrationId;const found=approved.find(x=>x.id===rid&&x.name===team.name&&JSON.stringify(x.players.map(p=>p.name))===JSON.stringify(team.players));if(found)team.registrationId=found.id;});
    }
    const id = rnd(8).toLowerCase(), key = rnd(14);
    const committed = await store.create({ id, config, keyHash: sha(key) });
    // Durable creator ownership: a match created by the organiser's signed-in session
    // carries a persisted proof so that session alone later scores/approves it; matches
    // created without that session (open practice create) need an approved scorer device.
    if (await validOrgSession(req.headers['x-org-session'])) await store.setSetting('orgown_' + id, '1');
    const out = { id, scorerKey: key, viewerPath: '/m/' + id, scorerPath: '/s/' + id, storage: store.kind };
    if (committed === false) { out.degraded = true; out.notice = 'Saved on the server. Firebase sync is pending.'; }
    return send(res, 201, out);
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/scoring-access$/)) && req.method === 'GET') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if(badAuth(ip))return send(res,429,{error:'Too many failed attempts. Try again later.'});
    const m=await store.get(mt[1]);if(!m)return send(res,404,{error:'Match not found'});
    if(!(await canScore(req,m))){noteBad(ip);return send(res,403,{error:'Sign in as organiser on the device holding this tournament organiser link.'})}
    return send(res,200,{ok:true});
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})$/)) && req.method === 'GET') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (url.searchParams.get('rev') === String(m.rev)) return send(res, 200, { unchanged: true, rev: m.rev });
    const view = publicView(m); view.teamRefs = registeredTeamRefs(m, m.config.tid ? await store.tget(m.config.tid) : null); view.canScore = await canScore(req, m); return send(res, 200, view);
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/(event|undo)$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!(await canScore(req,m))) return send(res, 403, { error: 'This device cannot score. Use the scorer link or verified organiser access.' });
    if (await scoreResetBlocked(req, res, m)) return;
    const body = await readBody(req);
    if (body.rev !== undefined && body.rev !== m.rev) return send(res, 409, { error: 'Match changed on another device. Refreshed.', match: publicView(m) });
    let events = m.events.slice();
    if (mt[2] === 'undo') {
      if (!events.length) return send(res, 400, { error: 'Nothing to undo' });
      events.pop();
    } else {
      const ev = body.event;
      const st = engine.replay(m.config, events);
      if(ev&&ev.t==='completedSummary'&&events.length)return send(res,400,{error:'Completed summary requires a match with no recorded events. Existing events were preserved.'});
      const er = engine.validate(m.config, st, ev);
      if (er) return send(res, 400, { error: er });
      const clean = ev.t==='summary'?{t:'summary',runs:ev.runs,wkts:ev.wkts,balls:ev.balls}:ev.t === 'completedSummary' ? {t:'completedSummary',innings:ev.innings.map(x=>({runs:x.runs,wkts:x.wkts,balls:x.balls})),winner:ev.winner} : ev.t === 'sel' ? { t: 'sel', role: ev.role, player: ev.player }
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
  if(p==='/invite/49asxecn')return serveStatic(res,'invite.html');
  if(p==='/camera')return serveStatic(res,'camera.html');
  if (p === '/' || p === '/launch' || /^\/(m|s|t|r)\/[a-z0-9]+\/?$/.test(p)) return serveStatic(res, 'index.html');
  return serveStatic(res, p.slice(1));
}

http.createServer((req, res) => {
  handle(req, res).catch((e) => {
    if (e && e.user) return send(res, 400, { error: e.message });
    console.error(e); send(res, 500, { error: 'Server error' });
  });
}).listen(PORT, () => console.log('Cricket centre on :' + PORT + ' storage=' + store.kind));
