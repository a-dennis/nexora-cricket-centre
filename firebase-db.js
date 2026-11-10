'use strict';
// Minimal Firebase Realtime Database REST client with email/password auth.
// No dependencies; Node 18+ fetch. Used only by server.js when
// FIREBASE_USER_EMAIL and FIREBASE_USER_PASSWORD are set (Render environment).
// Emulator overrides for local tests: FB_DB_URL, FB_AUTH_BASE, FB_TOKEN_BASE.

const CONFIG = {
  apiKey: 'AIzaSyBAKELv9icd9r1nWOE6T0zct1wIJHJVy-A',
  databaseURL: 'https://cricket-b12ab-default-rtdb.asia-southeast1.firebasedatabase.app',
};
const ROOT = 'nexoraCentre'; // the only path this server ever touches

function dbUrl() { return (process.env.FB_DB_URL || CONFIG.databaseURL).replace(/\/+$/, ''); }
function authBase() { return process.env.FB_AUTH_BASE || 'https://identitytoolkit.googleapis.com/v1'; }
function tokenBase() { return process.env.FB_TOKEN_BASE || 'https://securetoken.googleapis.com/v1'; }

let idToken = null, refreshToken = null, tokenExp = 0;

async function postJson(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  const text = await r.text();
  let data = null; try { data = text ? JSON.parse(text) : null; } catch (e) { /* keep null */ }
  if (!r.ok) {
    const msg = (data && data.error && data.error.message) || text.slice(0, 200);
    const err = new Error('Firebase auth ' + r.status + ': ' + msg);
    err.status = r.status;
    throw err;
  }
  return data;
}

async function signIn() {
  const email = process.env.FIREBASE_USER_EMAIL;
  const password = process.env.FIREBASE_USER_PASSWORD;
  if (!email || !password) throw new Error('Firebase env missing');
  const r = await postJson(authBase() + '/accounts:signInWithPassword?key=' + CONFIG.apiKey,
    { email, password, returnSecureToken: true });
  idToken = r.idToken;
  refreshToken = r.refreshToken || null;
  tokenExp = Date.now() + Math.max(60, Number(r.expiresIn || 3600) - 120) * 1000;
}

async function refresh() {
  if (!refreshToken) return signIn();
  try {
    const r = await postJson(tokenBase() + '/token?key=' + CONFIG.apiKey,
      { grant_type: 'refresh_token', refresh_token: refreshToken });
    idToken = r.id_token;
    refreshToken = r.refresh_token || refreshToken;
    tokenExp = Date.now() + Math.max(60, Number(r.expires_in || 3600) - 120) * 1000;
  } catch (e) {
    await signIn();
  }
}

async function getToken() {
  if (!idToken || Date.now() > tokenExp) await refresh();
  return idToken;
}

// One authenticated request against a path under nexoraCentre/.
// Returns { status, data, etag }. Throws on transport errors only.
async function raw(method, path, body, opts) {
  const t = await getToken();
  let url = dbUrl() + '/' + ROOT + '/' + path + '.json?auth=' + encodeURIComponent(t);
  if (process.env.FB_NS) url += '&ns=' + encodeURIComponent(process.env.FB_NS); // emulator namespace (never set in production)
  const headers = {};
  if (body !== undefined) headers['content-type'] = 'application/json';
  if (opts && opts.ifMatch) headers['If-Match'] = opts.ifMatch;
  if (opts && opts.wantEtag) headers['X-Firebase-ETag'] = 'true';
  const r = await fetch(url, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(15000),
  });
  const text = await r.text();
  let data = null; try { data = text ? JSON.parse(text) : null; } catch (e) { /* keep raw */ }
  return { status: r.status, data, etag: r.headers.get('etag'), text };
}

async function request(method, path, body, opts) {
  const r = await raw(method, path, body, opts);
  if (r.status === 401 || r.status === 403) {
    // token may have expired mid-flight; refresh once and retry
    idToken = null;
    const r2 = await raw(method, path, body, opts);
    if (r2.status === 401 || r2.status === 403) {
      const err = new Error('Firebase denied ' + method + ' ' + path + ' (' + r2.status + '): ' + r2.text.slice(0, 200));
      err.status = r2.status;
      throw err;
    }
    return finish(r2, method, path);
  }
  return finish(r, method, path);
}

function finish(r, method, path) {
  if (r.status === 412) { const err = new Error('Firebase conflict on ' + path); err.status = 412; throw err; }
  if (r.status >= 400) {
    const err = new Error('Firebase ' + method + ' ' + path + ' -> ' + r.status + ': ' + r.text.slice(0, 200));
    err.status = r.status;
    throw err;
  }
  return { data: r.data, etag: r.etag };
}

module.exports = {
  ROOT,
  // GET path under nexoraCentre/; null when absent. wantEtag returns {data, etag}.
  async get(path, wantEtag) {
    const r = await request('GET', path, undefined, { wantEtag: !!wantEtag });
    return wantEtag ? r : r.data;
  },
  // PUT (create/replace) with optional If-Match ETag for optimistic concurrency.
  async put(path, data, ifMatchEtag) {
    return request('PUT', path, data, ifMatchEtag ? { ifMatch: ifMatchEtag } : undefined);
  },
  // PATCH (shallow merge of the given children).
  async patch(path, data) { return request('PATCH', path, data); },
  // DELETE a subtree. Used only by the app's existing admin delete flows.
  async del(path) { return request('DELETE', path); },
  // Health probe: true when auth + read succeed.
  async probe() {
    try { await request('GET', 'meta', undefined, undefined); return true; } catch (e) { return false; }
  },
};
