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
    const pready = pool.query("CREATE TABLE IF NOT EXISTS cricket_team_pii (tid TEXT NOT NULL, team_id TEXT NOT NULL, players JSONB NOT NULL DEFAULT '{}', PRIMARY KEY (tid, team_id))");
    const row = (r) => r && { id: r.id, config: r.config, events: r.events, keyHash: r.key_hash, rev: r.rev, updated: r.updated_at };
    return {
      kind: 'durable',
      async matchKeySet(id, keyHash) { await kready; await pool.query('INSERT INTO cricket_match_keys(id,key_hash) VALUES($1,$2) ON CONFLICT (id) DO UPDATE SET key_hash=$2', [id, keyHash]); },
      async matchKeyGet(id) { await kready; const r = await pool.query('SELECT key_hash FROM cricket_match_keys WHERE id=$1', [id]); return r.rows[0] ? r.rows[0].key_hash : null; },
      async matchKeyDel(id) { await kready; await pool.query('DELETE FROM cricket_match_keys WHERE id=$1', [id]); },
      async matchIds() { await ready; const r = await pool.query('SELECT id FROM cricket_matches'); return r.rows.map((x) => x.id); },
      async deleteMatchRow(id) { await ready; await pool.query('DELETE FROM cricket_matches WHERE id=$1', [id]); },
      async deleteAllMatchRows() { await ready; await pool.query('DELETE FROM cricket_matches'); },
      async teamPiiSet(tid, teamId, players) { await pready; await pool.query('INSERT INTO cricket_team_pii(tid,team_id,players) VALUES($1,$2,$3) ON CONFLICT (tid,team_id) DO UPDATE SET players=$3', [tid, teamId, JSON.stringify(players)]); },
      async teamPiiGet(tid) { await pready; const r = await pool.query('SELECT team_id, players FROM cricket_team_pii WHERE tid=$1', [tid]); const out = {}; for (const x of r.rows) out[x.team_id] = x.players || {}; return out; },
      async teamPiiPrune(tid, keepTeamIds) { await pready; await pool.query('DELETE FROM cricket_team_pii WHERE tid=$1 AND NOT (team_id = ANY($2))', [tid, keepTeamIds && keepTeamIds.length ? keepTeamIds : ['__none__']]); },
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
      async tsetReg(id, regKeyHash) { await tready; await pool.query('UPDATE cricket_tournaments SET reg_key_hash=$2 WHERE id=$1', [id, regKeyHash]); },
      async getSetting(k) { await sready; const r = await pool.query('SELECT v FROM cricket_settings WHERE k=$1', [k]); return r.rows[0] ? r.rows[0].v : null; },
      async setSetting(k, v) { await sready; if (v === null) { await pool.query('DELETE FROM cricket_settings WHERE k=$1', [k]); return; } await pool.query('INSERT INTO cricket_settings(k,v) VALUES($1,$2) ON CONFLICT (k) DO UPDATE SET v=$2', [k, v]); },
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
    async tsetReg(id, regKeyHash) { if (tdb[id]) { tdb[id].regKeyHash = regKeyHash; tsave(); } },
    async getSetting(k) { return sdb[k] !== undefined ? sdb[k] : null; },
    async setSetting(k, v) { if (v === null) { delete sdb[k]; } else { sdb[k] = v; } ssave(); },
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
  const fbMatchRec = (config, events, rev, created, updated) => ({ config, events, rev, created: created || iso(), updated: updated || iso() });
  const piiMapOf = (t) => { const m = {}; for (const p of (t.players || [])) { if (p && p.name) m[String(p.name).toLowerCase()] = { empId: p.empId || '', dept: p.dept || '' }; } return m; };
  const publicTeam = (t, includePii) => ({
    id: t.id, name: t.name, submittedAt: t.submittedAt,
    players: (t.players || []).map((p) => includePii
      ? { name: p.name, empId: p.empId || '', dept: p.dept || '', role: p.role || '' }
      : { name: p.name, role: p.role || '' }),
  });
  async function fbTournament(id) { const t = await fbd.get('tournaments/' + id); return t && typeof t === 'object' ? t : null; }
  async function fbMatchIds() { const m = await fbd.get('matches'); return m && typeof m === 'object' ? Object.keys(m) : []; }
  async function matchKeyHash(id, legacyRow) {
    let keyHash = await pg.matchKeyGet(id);
    if (!keyHash) { const legacy = legacyRow !== undefined ? legacyRow : await pg.get(id); keyHash = (legacy && legacy.keyHash) || ''; }
    return keyHash;
  }
  async function tgetMerged(id) {
    const fb = await fbTournament(id);
    const legacy = await pg.tget(id);
    if (!fb && !legacy) return null;
    if (!fb) return legacy;
    const includePii = id === PUBLIC_DETAIL_TOURNAMENT;
    const pii = includePii ? {} : await pg.teamPiiGet(id);
    const pending = legacy && Array.isArray(legacy.teams) ? legacy.teams.filter((t) => t.status !== 'approved') : [];
    const approved = (Array.isArray(fb.teams) ? fb.teams : []).map((t) => ({
      id: t.id, name: t.name, submittedAt: t.submittedAt, status: 'approved',
      players: (Array.isArray(t.players) ? t.players : []).map((p) => {
        const priv = (pii[t.id] || {})[String(p.name || '').toLowerCase()] || {};
        return { name: p.name, empId: includePii ? (p.empId || '') : (priv.empId || ''), dept: includePii ? (p.dept || '') : (priv.dept || ''), role: p.role || '' };
      }),
    }));
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
    const fbAll = await fbd.get('matches');
    const fbIds = fbAll && typeof fbAll === 'object' ? Object.keys(fbAll) : [];
    const out = [];
    for (const id of fbIds) {
      const r = fbAll[id];
      out.push({ id, config: r.config, events: Array.isArray(r.events) ? r.events : [], keyHash: await matchKeyHash(id), rev: r.rev || 0, updated: r.updated, created: r.created });
    }
    for (const m of await pg.allMatches()) if (!fbIds.includes(m.id)) out.push(m);
    return out;
  }
  return {
    kind: 'durable',
    getSetting: (k) => pg.getSetting(k),
    setSetting: (k, v) => pg.setSetting(k, v),
    async get(id) {
      const fb = await fbd.get('matches/' + id);
      if (fb && typeof fb === 'object') return { id, config: fb.config, events: Array.isArray(fb.events) ? fb.events : [], keyHash: await matchKeyHash(id), rev: fb.rev || 0, updated: fb.updated, created: fb.created };
      return pg.get(id);
    },
    async count() { const fb = await fbMatchIds(); const legacy = (await pg.matchIds()).filter((x) => !fb.includes(x)); return fb.length + legacy.length; },
    async create(m) {
      // Private key first: a failed Firebase write then strands only an invisible key row
      // (no public record without its manager key; retry with the same id recovers).
      await pg.matchKeySet(m.id, m.keyHash);
      await fbd.put('matches/' + m.id, fbMatchRec(m.config, [], 0, iso(), iso()));
    },
    async update(id, events, expectRev) {
      const g = await fbd.get('matches/' + id, true);
      if (!g.data || typeof g.data !== 'object') return pg.update(id, events, expectRev);
      if ((g.data.rev || 0) !== expectRev) return false;
      const next = Object.assign({}, g.data, { events, rev: expectRev + 1, updated: iso() });
      try { await fbd.put('matches/' + id, next, g.etag || undefined); return true; }
      catch (e) { if (e.status === 412) return false; throw e; }
    },
    async setConfig(id, config, bump) {
      const g = await fbd.get('matches/' + id, true);
      if (!g.data || typeof g.data !== 'object') return pg.setConfig(id, config, bump);
      const attempt = (cur, etag) => {
        const next = Object.assign({}, cur, { config });
        if (bump) { next.rev = (cur.rev || 0) + 1; next.updated = iso(); }
        return fbd.put('matches/' + id, next, etag || undefined);
      };
      try { await attempt(g.data, g.etag); }
      catch (e) {
        if (e.status !== 412) throw e;
        const g2 = await fbd.get('matches/' + id, true);
        if (!g2.data || typeof g2.data !== 'object') throw e;
        await attempt(g2.data, g2.etag); // a second conflict surfaces as an error
      }
    },
    async byTournament(tid) {
      const fbAll = await fbd.get('matches');
      const fbIds = fbAll && typeof fbAll === 'object' ? Object.keys(fbAll) : [];
      const out = [];
      for (const id of fbIds) {
        const r = fbAll[id];
        if (r && r.config && r.config.tid === tid) out.push({ id, config: r.config, events: Array.isArray(r.events) ? r.events : [], keyHash: await matchKeyHash(id), rev: r.rev || 0, updated: r.updated, created: r.created });
      }
      for (const m of await pg.byTournament(tid)) if (!fbIds.includes(m.id)) out.push(m);
      return out;
    },
    async allMatches() { return allMatchesMerged(); },
    async importMatch(m) {
      const existing = await fbd.get('matches/' + m.id);
      if (existing) return false;
      return pg.importMatch(m); // restored legacy data stays private-link only
    },
    async deleteBackedUpMatch(expected) {
      const g = await fbd.get('matches/' + expected.id);
      if (!g || typeof g !== 'object') return pg.deleteBackedUpMatch(expected);
      const legacy = await pg.get(expected.id);
      const keyHash = await matchKeyHash(expected.id, legacy);
      const cur = { id: expected.id, config: g.config, events: Array.isArray(g.events) ? g.events : [], keyHash, rev: g.rev || 0 };
      if (matchSnapshot([cur]) !== matchSnapshot([expected])) return false;
      await fbd.del('matches/' + expected.id);
      await pg.matchKeyDel(expected.id);
      if (legacy) await pg.deleteMatchRow(expected.id); // drop the stale migrated copy
      return true;
    },
    async replaceAllMatches(expected, fresh) {
      const all = await allMatchesMerged();
      if (matchSnapshot(all) !== matchSnapshot(expected)) return false;
      for (const id of await fbMatchIds()) { await fbd.del('matches/' + id); await pg.matchKeyDel(id); }
      await pg.deleteAllMatchRows();
      await fbd.put('matches/' + fresh.id, fbMatchRec(fresh.config, [], 0, iso(), iso()));
      await pg.matchKeySet(fresh.id, fresh.keyHash);
      return true;
    },
    async tcreate(t) {
      // Postgres first (holds keyHash and the private remainder): a failed Firebase write
      // leaves a working link-private tournament instead of a public record with no key.
      await pg.tcreate(t);
      await fbd.put('tournaments/' + t.id, { name: t.name, listed: false, created: iso(), registration: false, teams: [] });
    },
    async tget(id) { return tgetMerged(id); },
    // Migration-only: after the INSERT-only Firebase tournament record is created,
    // move the Postgres remainder into place (pending teams stay in the teams blob,
    // employee details of approved teams go to the private side table) WITHOUT any
    // further Firebase write, so migration never overwrites a Firebase record.
    async privateSplitAfterMigrate(id, fullTeams) {
      const fb = await fbTournament(id);
      if (!fb) throw new Error('Firebase tournament record missing for ' + id);
      const includePii = id === PUBLIC_DETAIL_TOURNAMENT;
      const approved = (fullTeams || []).filter((t) => t.status === 'approved');
      const pending = (fullTeams || []).filter((t) => t.status !== 'approved');
      await pg.tsetTeams(id, pending);
      if (!includePii) {
        for (const t of approved) await pg.teamPiiSet(id, t.id, piiMapOf(t));
        await pg.teamPiiPrune(id, approved.map((t) => t.id));
      }
    },
    async tsetTeams(id, teams) {
      const fb = await fbTournament(id);
      if (!fb) return pg.tsetTeams(id, teams); // legacy link-private tournament: never enters the public path
      const includePii = id === PUBLIC_DETAIL_TOURNAMENT;
      const approved = (teams || []).filter((t) => t.status === 'approved');
      const pending = (teams || []).filter((t) => t.status !== 'approved');
      await fbd.put('tournaments/' + id, Object.assign({}, fb, { teams: approved.map((t) => publicTeam(t, includePii)) }));
      await pg.tsetTeams(id, pending);
      if (!includePii) {
        for (const t of approved) await pg.teamPiiSet(id, t.id, piiMapOf(t));
        await pg.teamPiiPrune(id, approved.map((t) => t.id));
      }
    },
    async tsetReg(id, regKeyHash) {
      const fb = await fbTournament(id);
      if (fb) await fbd.patch('tournaments/' + id, { registration: !!regKeyHash });
      await pg.tsetReg(id, regKeyHash);
    },
    async tsetListed(id, listed) {
      const fb = await fbTournament(id);
      if (fb) await fbd.patch('tournaments/' + id, { listed: !!listed });
      return pg.tsetListed(id, listed);
    },
    async trename(id, name) {
      const fb = await fbTournament(id);
      if (fb) { await fbd.patch('tournaments/' + id, { name }); await pg.trename(id, name); return true; }
      return pg.trename(id, name);
    },
    async tcount() {
      const all = await fbd.get('tournaments');
      const fbIds = all && typeof all === 'object' ? Object.keys(all) : [];
      const legacy = await pg.allTournaments();
      return fbIds.length + legacy.filter((t) => !fbIds.includes(t.id)).length;
    },
    async listedTournaments() {
      const all = await fbd.get('tournaments');
      const out = [];
      const fbIds = [];
      if (all && typeof all === 'object') for (const id of Object.keys(all)) { fbIds.push(id); if (all[id] && all[id].listed) out.push({ id, name: all[id].name }); }
      for (const t of await pg.listedTournaments()) if (!fbIds.includes(t.id)) out.push(t);
      return out;
    },
    async allTournaments() {
      const all = await fbd.get('tournaments');
      const fbIds = all && typeof all === 'object' ? Object.keys(all) : [];
      const out = [];
      for (const id of fbIds) { const t = await tgetMerged(id); if (t) out.push(t); }
      for (const t of await pg.allTournaments()) if (!fbIds.includes(t.id)) out.push(t);
      return out;
    },
    async importTournament(t) {
      const existing = await fbTournament(t.id);
      if (existing) return false;
      return pg.importTournament(t); // restored legacy data stays private-link only
    },
  };
}
function stableValue(x) { if (Array.isArray(x)) return x.map(stableValue); if(x && typeof x==='object') return Object.keys(x).sort().reduce((a,k)=>(a[k]=stableValue(x[k]),a),{}); return x; }
function matchSnapshot(ms) { return JSON.stringify(stableValue(ms.map(m=>({id:m.id,config:m.config,events:m.events,keyHash:m.keyHash,rev:m.rev})).sort((a,b)=>a.id.localeCompare(b.id)))); }
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

// Recovery is limited to the fresh replacement ceremony. It never reveals or rotates its scorer key.
async function canScore(req,m) {
  if (keyEq(req.headers['x-scorer-key'],m.keyHash)) return true;
  if(m.id!=='49asxecn'||!m.config||m.config.tid!=='whxrz4tw') return false;
  if(!(await validOrgSession(req.headers['x-org-session'])))return false;
  const t=await store.tget(m.config.tid);
  return !!(t&&keyEq(req.headers['x-org-key'],t.keyHash));
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
    if (req.method === 'POST') {
      const ip=(req.headers['x-forwarded-for']||req.socket.remoteAddress||'').split(',')[0].trim();
      if(badAuth(ip))return send(res,429,{error:'Too many failed attempts. Try again later.'});
      const body=await readBody(req,2000),tid=String(body&&body.tid||'');
      const t=/^[a-z0-9]{6,12}$/.test(tid)?await store.tget(tid):null;
      if (!t || !keyEq(body&&body.orgKey,t.keyHash)) {noteBad(ip);return send(res,403,{error:'Open this tournament on the device holding its private organiser link.'});}
      await store.setSetting('orgtour_'+t.id,t.keyHash);
      return send(res,200,{ok:true});
    }
    const tournaments=[];
    for(const t of await store.allTournaments()) {
      if((await store.getSetting('orgtour_'+t.id))===t.keyHash) tournaments.push({id:t.id,name:t.name});
    }
    return send(res,200,{tournaments});
  }
  if (p === '/api/auth/setup' && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const body = await readBody(req, 2e3);
    const tid = String((body && body.tid) || '');
    const t = /^[a-z0-9]{6,12}$/.test(tid) ? await store.tget(tid) : null;
    if (!t || !keyEq(body && body.orgKey, t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'A valid organiser link on this device is needed to set the password' }); }
    const pw = String((body && body.password) || '');
    if (pw.length < 4 || pw.length > 72) return send(res, 400, { error: 'Password needs 4 to 72 characters' });
    const salt = rnd(16);
    const hash = crypto.scryptSync(pw, salt, 32).toString('hex');
    await store.setSetting('orgpass', JSON.stringify({ salt, hash }));
    return send(res, 200, { ok: true });
  }
  if (p === '/api/auth/login' && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const body = await readBody(req, 2e3);
    const rec = await store.getSetting('orgpass');
    if (!rec) return send(res, 409, { error: 'Sign-in is not set up yet' });
    let ph = null; try { ph = JSON.parse(rec); } catch (e) { ph = null; }
    const user = String((body && body.user) || '').trim().toLowerCase();
    const pw = String((body && body.password) || '');
    const ok = !!(ph && user === 'denny' && pw.length > 0 && pw.length <= 72 &&
      crypto.timingSafeEqual(Buffer.from(crypto.scryptSync(pw, ph.salt, 32).toString('hex')), Buffer.from(String(ph.hash))));
    if (!ok) { noteBad(ip); return send(res, 403, { error: 'Wrong username or password' }); }
    const token = rnd(24);
    await store.setSetting('orgsess_'+sha(token),JSON.stringify({h:sha(token),exp:Date.now()+30*864e5}));
    return send(res, 200, { ok: true, token });
  }
  if (p === '/api/auth/check' && req.method === 'POST') {
    const body = await readBody(req, 2e3);
    const ok=await validOrgSession(body&&body.token);
    return send(res, 200, { ok });
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
  if (p === '/api/admin/delete-match' || p === '/api/backup' || p === '/api/restore' || p === '/api/admin/tournament-name' || p === '/api/admin/tournament-listed' || p === '/api/admin/replace-matches' || p === '/api/admin/firebase-migrate') {
    // Owner-only. Disabled unless BACKUP_TOKEN is set. Backups contain match data and hashed keys, never raw keys.
    const tok = process.env.BACKUP_TOKEN;
    if (!tok || tok.length < 24) return send(res, 404, { error: 'Unknown API' });
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    if (!keyEq(req.headers['x-backup-token'], sha(tok))) { noteBad(ip); return send(res, 403, { error: 'Not allowed' }); }
    if (p === '/api/backup' && req.method === 'GET') {
      const [ms, ts] = [await store.allMatches(), await store.allTournaments()];
      return send(res, 200, { format: 'cricket-backup', version: 1, exportedAt: new Date().toISOString(), storage: store.kind,
        counts: { matches: ms.length, tournaments: ts.length }, tournaments: ts, matches: ms });
    }
    if (p === '/api/admin/firebase-migrate' && req.method === 'POST') {
      // One-time INSERT-only copy of the two public tournaments and their matches into
      // nexoraCentre/. Never overwrites an existing Firebase record, never deletes anything.
      // Default is a dry run that only reports what would be copied; {apply:true} executes.
      // {seedBindings:true} also saves organiser access for these tournaments - only ever
      // sent after the owner explicitly confirms the binding for that tournament.
      const body = await readBody(req, 2000);
      const dry = !body || body.apply !== true;
      const fbd = require('./firebase-db');
      const out = { dryRun: dry, tournaments: [], matches: [], skipped: [], errors: [] };
      for (const tid of ['whxrz4tw', 'hovxdntl']) {
        try {
          const t = await store.tget(tid);
          if (!t) { out.skipped.push({ tournament: tid, reason: 'missing' }); continue; }
          const existing = await fbd.get('tournaments/' + tid);
          if (existing) { out.skipped.push({ tournament: tid, reason: 'already in Firebase' }); }
          else {
            const includePii = tid === PUBLIC_DETAIL_TOURNAMENT;
            const approved = cleanStoredTeams(t.teams).filter((x) => x.status === 'approved');
            const rec = { name: t.name, listed: !!t.listed, created: t.created || new Date().toISOString(), registration: !!t.regKeyHash,
              teams: approved.map((tm) => ({ id: tm.id, name: tm.name, submittedAt: tm.submittedAt,
                players: tm.players.map((p) => includePii ? { name: p.name, empId: p.empId || '', dept: p.dept || '', role: p.role || '' } : { name: p.name, role: p.role || '' }) })) };
            out.tournaments.push({ id: tid, name: t.name, approvedTeams: approved.length });
            if (!dry) {
              await fbd.put('tournaments/' + tid, rec); // INSERT-only: 'existing' was checked absent above
              // Postgres remainder only (pending teams + private employee details). No second
              // Firebase write: the record above already carries the full approved projection.
              await store.privateSplitAfterMigrate(tid, cleanStoredTeams(t.teams));
              if (body.seedBindings === true) await store.setSetting('orgtour_' + tid, t.keyHash);
            }
          }
          for (const m of await store.byTournament(tid)) {
            const have = await fbd.get('matches/' + m.id);
            if (have) { out.skipped.push({ match: m.id, reason: 'already in Firebase' }); continue; }
            out.matches.push({ id: m.id, tournament: tid, events: (m.events || []).length });
            if (!dry) await fbd.put('matches/' + m.id, { config: m.config, events: m.events || [], rev: m.rev || 0, created: m.created || m.updated || new Date().toISOString(), updated: m.updated || new Date().toISOString() });
          }
        } catch (e) { out.errors.push({ tournament: tid, error: String((e && e.message) || e).slice(0, 200) }); }
      }
      return send(res, 200, out);
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
    if ((await store.tcount()) >= MAX_TOURNAMENTS) return send(res, 507, { error: 'Storage limit reached.' });
    const body = await readBody(req);
    const name = engine.cleanName(body.name, 40);
    if (!name) return send(res, 400, { error: 'Enter a tournament name' });
    const id = rnd(8).toLowerCase(), key = rnd(14);
    await store.tcreate({ id, name, keyHash: sha(key) });
    return send(res, 201, { id, name, orgKey: key, calendarPath: '/t/' + id });
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
    if (!keyEq(req.headers['x-org-key'], t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Only the organiser link can download the tournament backup.' }); }
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
    if (!keyEq(req.headers['x-org-key'], t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Only the tournament organiser can manage team registration.' }); }
    const body = await readBody(req, 2e3);
    const rk = String(body && body.regKey || '');
    if (!rk) { await store.tsetReg(t.id, null); return send(res, 200, { ok: true, registration: 'closed' }); }
    if (!/^[A-Za-z0-9]{10,24}$/.test(rk)) return send(res, 400, { error: 'Bad registration key' });
    await store.tsetReg(t.id, sha(rk));
    return send(res, 200, { ok: true, registration: 'open' });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/listed$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!keyEq(req.headers['x-org-key'], t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Only the tournament organiser can change home listing.' }); }
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
    const teams = cleanStoredTeams(t.teams);
    if (teams.length >= MAX_TEAMS_PER_TOURNAMENT) return send(res, 507, { error: 'This tournament has reached its team limit.' });
    if (teams.some((x) => x.name.toLowerCase() === c.name.toLowerCase())) return send(res, 409, { error: 'A team with this name is already registered.' });
    teams.push({ id: rnd(8).toLowerCase(), name: c.name, players: c.players, status: 'pending', submittedAt: new Date().toISOString() });
    await store.tsetTeams(t.id, teams);
    return send(res, 201, { ok: true });
  }
  if ((mt = p.match(/^\/api\/tournaments\/([a-z0-9]{6,12})\/teams\/([a-z0-9]{6,12})$/)) && req.method === 'POST') {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    if (badAuth(ip)) return send(res, 429, { error: 'Too many failed attempts. Try again later.' });
    const t = await store.tget(mt[1]);
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!keyEq(req.headers['x-org-key'], t.keyHash)) { noteBad(ip); return send(res, 403, { error: 'Only the tournament organiser can review teams.' }); }
    const body = await readBody(req, 2e3);
    const teams = cleanStoredTeams(t.teams);
    const ix = teams.findIndex((x) => x.id === mt[2]);
    if (ix === -1) return send(res, 404, { error: 'Team not found' });
    if (body && body.action === 'approve') teams[ix].status = 'approved';
    else if (body && body.action === 'remove') teams.splice(ix, 1);
    else if (body && body.action === 'role') {
      const pix = typeof body.player === 'number' && Number.isInteger(body.player) ? body.player : -1;
      const role = body.role === 'c' || body.role === 'vc' ? body.role : '';
      if (pix < 0 || pix >= teams[ix].players.length) return send(res, 400, { error: 'Unknown player' });
      if (role) teams[ix].players.forEach((pl, i2) => { if (i2 !== pix && pl.role === role) pl.role = ''; });
      teams[ix].players[pix].role = role;
    }
    else return send(res, 400, { error: 'Unknown action' });
    await store.tsetTeams(t.id, teams);
    return send(res, 200, { ok: true });
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/attach$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!keyEq(req.headers['x-scorer-key'], m.keyHash)) return send(res, 403, { error: 'Use this match\'s scorer link to add it to a tournament.' });
    const body = await readBody(req);
    const t = await store.tget(String(body.tournament || ''));
    if (!t) return send(res, 404, { error: 'Tournament not found' });
    if (!keyEq(req.headers['x-org-key'], t.keyHash)) return send(res, 403, { error: 'Only the tournament organiser can add matches.' });
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
    config.date = cleanDate(body.date);
    if (body.tournament) {
      const t = await store.tget(String(body.tournament));
      if (!t) return send(res, 404, { error: 'Tournament not found' });
      if (!keyEq(req.headers['x-org-key'], t.keyHash) && !(await savedOrgTournament(req,t))) return send(res, 403, { error: 'Only the tournament organiser can add matches.' });
      if ((await store.byTournament(t.id)).length >= MAX_PER_TOURNAMENT) return send(res, 507, { error: 'Tournament is full.' });
      config.tid = t.id;
      const approved = cleanStoredTeams(t.teams).filter(x=>x.status==='approved');
      config.teams.forEach((team,i)=>{const rid=body.teams[i]&&body.teams[i].registrationId;const found=approved.find(x=>x.id===rid&&x.name===team.name&&JSON.stringify(x.players.map(p=>p.name))===JSON.stringify(team.players));if(found)team.registrationId=found.id;});
    }
    const id = rnd(8).toLowerCase(), key = rnd(14);
    await store.create({ id, config, keyHash: sha(key) });
    return send(res, 201, { id, scorerKey: key, viewerPath: '/m/' + id, scorerPath: '/s/' + id, storage: store.kind });
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
    const view = publicView(m); view.teamRefs = registeredTeamRefs(m, m.config.tid ? await store.tget(m.config.tid) : null); return send(res, 200, view);
  }
  if ((mt = p.match(/^\/api\/matches\/([a-z0-9]{6,12})\/(event|undo)$/)) && req.method === 'POST') {
    const m = await store.get(mt[1]);
    if (!m) return send(res, 404, { error: 'Match not found' });
    if (!(await canScore(req,m))) return send(res, 403, { error: 'This device cannot score. Use the scorer link or verified organiser access.' });
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
