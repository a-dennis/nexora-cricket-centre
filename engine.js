'use strict';
// Ball-by-ball cricket engine. The event log is the source of truth; state is replayed from it.
const KINDS_ANY = ['bowled', 'caught', 'lbw', 'stumped', 'runout', 'hitwicket', 'other'];

function cleanName(s, max) { return String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, max || 30); }

// Accept only a VDO.Ninja receiving link, never capture/director/room controls.
function ninjaViewUrl(raw) {
  try {
    const u = new URL(String(raw || '').trim());
    if (u.protocol !== 'https:' || u.hostname !== 'vdo.ninja' || u.port || u.username || u.password || u.pathname !== '/' || u.hash) return '';
    const id = u.searchParams.get('view');
    if (!id || !/^[A-Za-z0-9_-]{3,128}$/.test(id) || u.searchParams.getAll('view').length !== 1) return '';
    if (['push','room','director'].some(k=>u.searchParams.has(k))) return '';
    const safe = new URL('https://vdo.ninja/');safe.searchParams.set('view',id);
    const pw=u.searchParams.get('password');if(pw){if(pw.length>128)return '';safe.searchParams.set('password',pw);}
    return safe.href;
  } catch (_) { return ''; }
}
function normalizeConfig(c) {
  const err = (m) => { const e = new Error(m); e.user = true; throw e; };
  if (!c || typeof c !== 'object') err('Missing match setup');
  const overs = Number(c.overs);
  if (!Number.isInteger(overs) || overs < 1 || overs > 10) err('Overs must be a whole number from 1 to 10');
  if (!Array.isArray(c.teams) || c.teams.length !== 2) err('Two teams needed');
  const teams = c.teams.map((t, i) => {
    const name = cleanName(t && t.name, 24) || ('Team ' + (i + 1));
    let players = Array.isArray(t && t.players) ? t.players.map((p) => cleanName(p, 24)).filter(Boolean) : [];
    if (players.length < 2) err(name + ' needs at least 2 players');
    if (players.length > 15) err(name + ' can have at most 15 players');
    return { name, players };
  });
  if (teams[0].name.toLowerCase() === teams[1].name.toLowerCase()) err('Team names must be different');
  const wideRun = c.wideRun === undefined ? 1 : Number(c.wideRun);
  const nbRun = c.nbRun === undefined ? 1 : Number(c.nbRun);
  if (![0, 1, 2].includes(wideRun) || ![0, 1, 2].includes(nbRun)) err('Wide / no-ball penalty must be 0, 1 or 2');
  const maxOversPerBowler = c.maxOversPerBowler ? Number(c.maxOversPerBowler) : 0;
  if (!Number.isInteger(maxOversPerBowler) || maxOversPerBowler < 0 || maxOversPerBowler > 10) err('Bad max overs per bowler');
  const out = {
    title: cleanName(c.title, 50) || 'Match',
    overs, teams,
    batFirst: c.batFirst === 1 || c.batFirst === '1' ? 1 : 0,
    wideRun, nbRun, maxOversPerBowler,
    demo: c.demo !== false,
  };
  // Optional YouTube video id for the viewer page. Invalid ids are dropped silently so a restore never fails over a link.
  if (typeof c.videoId === 'string' && /^[A-Za-z0-9_-]{11}$/.test(c.videoId)) out.videoId = c.videoId;
  const ninja = ninjaViewUrl(c.ninjaView); if (ninja) { out.ninjaView = ninja; delete out.videoId; }
  return out;
}

function mkInnings(cfg, i) {
  const bat = i === 0 ? cfg.batFirst : 1 - cfg.batFirst;
  const bowl = 1 - bat;
  return {
    bat, bowl, runs: 0, wkts: 0, balls: 0, extras: { wd: 0, nb: 0, b: 0, lb: 0 },
    batters: cfg.teams[bat].players.map(() => ({ runs: 0, balls: 0, fours: 0, sixes: 0, in: false, out: null })),
    order: [], bowlers: {}, fow: [], overs: [], cur: [], curRuns: 0, curLegal: 0, curTotal: 0,
    striker: null, non: null, pendingSlot: null, bowler: null, lastBowler: null, done: false, target: null, endedBy: null, log: [],
  };
}
function swap(inn) {
  const t = inn.striker; inn.striker = inn.non; inn.non = t;
  if (inn.pendingSlot) inn.pendingSlot = inn.pendingSlot === 's' ? 'n' : 's';
}
function bstat(inn, p) { return inn.bowlers[p] || (inn.bowlers[p] = { balls: 0, runs: 0, wkts: 0, wd: 0, nb: 0, maidens: 0 }); }

function needsOf(st) {
  if (st.result) return [];
  const inn = st.innings[st.cur];
  if (inn.done) return [];
  const n = [];
  if (inn.striker === null && inn.pendingSlot !== 's') n.push('striker');
  if (inn.non === null && inn.pendingSlot !== 'n') n.push('non');
  if (inn.pendingSlot) n.push('newbat');
  if (inn.bowler === null) n.push('bowler');
  return n;
}

function finishInnings(cfg, st, why) {
  const inn = st.innings[st.cur];
  if (inn.done) return;
  inn.done = true; inn.endedBy = why;
  if (st.cur === 0) {
    st.cur = 1; const n = mkInnings(cfg, 1); n.target = inn.runs + 1; st.innings.push(n);
  } else {
    const a = st.innings[0], b = st.innings[1];
    const n2 = cfg.teams[b.bat].players.length;
    if (b.runs >= b.target) st.result = { winner: b.bat, text: cfg.teams[b.bat].name + ' won by ' + (n2 - 1 - b.wkts) + ' wicket' + ((n2 - 1 - b.wkts) === 1 ? '' : 's') };
    else if (b.runs === a.runs) st.result = { winner: null, text: 'Match tied' };
    else st.result = { winner: a.bat, text: cfg.teams[a.bat].name + ' won by ' + (a.runs - b.runs) + ' run' + ((a.runs - b.runs) === 1 ? '' : 's') };
  }
}

function checkEnd(cfg, st) {
  const inn = st.innings[st.cur];
  if (inn.done) return;
  const players = cfg.teams[inn.bat].players.length;
  if (st.cur === 1 && inn.runs >= inn.target) return finishInnings(cfg, st, 'target');
  if (inn.wkts >= players - 1) return finishInnings(cfg, st, 'allout');
  if (inn.balls >= cfg.overs * 6) return finishInnings(cfg, st, 'overs');
}

function validate(cfg, st, ev) {
  if (!ev || typeof ev !== 'object') return 'Bad event';
  if (st.result) return 'Match is finished';
  const inn = st.innings[st.cur];
  const needs = needsOf(st);
  if (ev.t === 'end') return null;
  if (ev.t === 'sel') {
    if (!needs.includes(ev.role)) return 'Not needed right now';
    const p = ev.player;
    if (!Number.isInteger(p)) return 'Pick a player';
    if (ev.role === 'bowler') {
      const bt = cfg.teams[inn.bowl].players;
      if (p < 0 || p >= bt.length) return 'Unknown player';
      if (bt.length > 1 && p === inn.lastBowler) return 'Same bowler cannot bowl two overs in a row';
      if (cfg.maxOversPerBowler && (inn.bowlers[p] ? inn.bowlers[p].balls : 0) >= cfg.maxOversPerBowler * 6) return 'This bowler has used the maximum overs';
      return null;
    }
    const bt = cfg.teams[inn.bat].players;
    if (p < 0 || p >= bt.length) return 'Unknown player';
    if (inn.batters[p].in) return 'That player has already batted';
    return null;
  }
  if (ev.t === 'ball') {
    if (needs.length) return 'Finish selecting players first';
    const x = ev.x || '';
    if (!['', 'wd', 'nb', 'b', 'lb'].includes(x)) return 'Bad extra type';
    if (!Number.isInteger(ev.r) || ev.r < 0 || ev.r > 7) return 'Runs must be 0 to 7';
    if (ev.w) {
      const k = ev.w.k, who = ev.w.who;
      if (!KINDS_ANY.includes(k)) return 'Bad dismissal';
      if (who !== 's' && who !== 'n') return 'Who is out?';
      if (x === 'wd' && !['stumped', 'runout'].includes(k)) return 'Only stumped or run out on a wide';
      if ((x === 'nb' || x === 'b' || x === 'lb') && k !== 'runout') return 'Only run out is possible here';
      if (k !== 'runout' && who !== 's') return 'Only the striker can be out that way';
    }
    return null;
  }
  return 'Unknown event';
}

function applyBall(cfg, st, ev) {
  const inn = st.innings[st.cur];
  const x = ev.x || '', r = ev.r;
  const legal = x !== 'wd' && x !== 'nb';
  const bat = (x === '' || x === 'nb') ? r : 0;
  let extra = 0;
  if (x === 'wd') { extra = cfg.wideRun + r; inn.extras.wd += extra; }
  else if (x === 'nb') { extra = cfg.nbRun; inn.extras.nb += extra; }
  else if (x === 'b') { extra = r; inn.extras.b += r; }
  else if (x === 'lb') { extra = r; inn.extras.lb += r; }
  const total = bat + extra;
  inn.runs += total; inn.curTotal += total;
  const sp = inn.striker;
  const logRec = { ov: inn.overs.length, lb: inn.curLegal + (legal ? 1 : 0), bowler: inn.bowler, striker: sp, non: inn.non, r, x, bat, extra, total };
  if (x !== 'wd') inn.batters[sp].balls += 1;
  inn.batters[sp].runs += bat;
  if (bat === 4) inn.batters[sp].fours += 1;
  if (bat === 6) inn.batters[sp].sixes += 1;
  const bw = bstat(inn, inn.bowler);
  const charged = bat + (x === 'wd' ? extra : x === 'nb' ? extra : 0);
  bw.runs += charged; inn.curRuns += charged;
  if (x === 'wd') bw.wd += extra; if (x === 'nb') bw.nb += 1;
  if (legal) { bw.balls += 1; inn.balls += 1; inn.curLegal += 1; }
  // token
  let tok;
  if (x === 'wd') tok = (total === 1 ? '' : total) + 'Wd';
  else if (x === 'nb') tok = 'Nb' + (bat ? '+' + bat : '');
  else if (x === 'b') tok = r + 'B';
  else if (x === 'lb') tok = r + 'Lb';
  else tok = String(r);
  // strike rotation
  if (r % 2 === 1) swap(inn);
  // wicket
  if (ev.w) {
    const outP = ev.w.who === 'n' ? (r % 2 === 1 ? inn.striker : inn.non) : (r % 2 === 1 ? inn.non : inn.striker);
    const slot = inn.striker === outP ? 's' : 'n';
    const bname = cfg.teams[inn.bowl].players[inn.bowler];
    const k = ev.w.k, f = cleanName(ev.w.f, 24);
    const text = { bowled: 'b ' + bname, lbw: 'lbw b ' + bname, caught: 'c ' + (f || '?') + ' b ' + bname, stumped: 'st ' + (f || '?') + ' b ' + bname, hitwicket: 'hit wicket b ' + bname, runout: 'run out' + (f ? ' (' + f + ')' : ''), other: 'out' }[k];
    inn.batters[outP].out = { k, text, bowler: ['runout', 'other'].includes(k) ? null : inn.bowler };
    if (!['runout', 'other'].includes(k)) bw.wkts += 1;
    inn.wkts += 1;
    inn.fow.push({ runs: inn.runs, wkts: inn.wkts, player: outP, over: Math.floor(inn.balls / 6) + '.' + (inn.balls % 6) });
    if (slot === 's') inn.striker = null; else inn.non = null;
    inn.pendingSlot = slot;
    tok = (r ? r : '') + 'W';
    logRec.w = { k, who: outP, f: f || null, text };
  }
  inn.cur.push(tok);
  logRec.runs = inn.runs; logRec.wkts = inn.wkts; logRec.tok = tok;
  inn.log.push(logRec);
  if (legal && inn.curLegal === 6) {
    if (inn.curRuns === 0) bw.maidens += 1;
    inn.overs.push({ bowler: inn.bowler, balls: inn.cur.slice(), runs: inn.curTotal });
    inn.cur = []; inn.curRuns = 0; inn.curLegal = 0; inn.curTotal = 0;
    swap(inn);
    inn.lastBowler = inn.bowler; inn.bowler = null;
  }
  checkEnd(cfg, st);
}

function replay(cfg, events) {
  const st = { innings: [mkInnings(cfg, 0)], cur: 0, result: null };
  for (const ev of events) {
    const inn = st.innings[st.cur];
    if (ev.t === 'sel') {
      const p = ev.player;
      if (ev.role === 'bowler') inn.bowler = p;
      else {
        inn.batters[p].in = true; inn.order.push(p);
        if (ev.role === 'striker') inn.striker = p;
        else if (ev.role === 'non') inn.non = p;
        else { if (inn.pendingSlot === 's') inn.striker = p; else inn.non = p; inn.pendingSlot = null; }
      }
    } else if (ev.t === 'ball') applyBall(cfg, st, ev);
    else if (ev.t === 'end') {
      // flush a partial over into history so scorecards stay complete
      if (inn.cur.length) inn.overs.push({ bowler: inn.bowler, balls: inn.cur.slice(), partial: true });
      finishInnings(cfg, st, 'declared');
    }
  }
  return st;
}

function oversText(b) { return Math.floor(b / 6) + (b % 6 ? '.' + (b % 6) : ''); }

function view(cfg, events) {
  const st = replay(cfg, events);
  const needs = needsOf(st);
  const mapInn = (inn, idx) => {
    const bp = cfg.teams[inn.bat].players, wp = cfg.teams[inn.bowl].players;
    const maxBalls = cfg.overs * 6;
    return {
      idx, bat: inn.bat, bowl: inn.bowl, batName: cfg.teams[inn.bat].name, bowlName: cfg.teams[inn.bowl].name,
      runs: inn.runs, wkts: inn.wkts, balls: inn.balls, overs: oversText(inn.balls), done: inn.done, endedBy: inn.endedBy,
      target: inn.target, extras: inn.extras, extrasTotal: inn.extras.wd + inn.extras.nb + inn.extras.b + inn.extras.lb,
      rr: inn.balls ? +(inn.runs * 6 / inn.balls).toFixed(2) : 0,
      striker: inn.striker, non: inn.non, bowler: inn.bowler,
      batting: inn.order.map((p) => ({ p, name: bp[p], ...inn.batters[p], out: inn.batters[p].out ? inn.batters[p].out.text : null, notOut: !inn.batters[p].out, onStrike: p === inn.striker, sr: inn.batters[p].balls ? Math.round(inn.batters[p].runs * 100 / inn.batters[p].balls) : 0 })),
      yetToBat: bp.map((n, p) => ({ p, name: n })).filter((o) => !inn.batters[o.p].in),
      bowling: Object.keys(inn.bowlers).map(Number).map((p) => { const b = inn.bowlers[p]; return { p, name: wp[p], overs: oversText(b.balls), maidens: b.maidens, runs: b.runs, wkts: b.wkts, wd: b.wd, nb: b.nb, econ: b.balls ? +(b.runs * 6 / b.balls).toFixed(1) : 0, current: p === inn.bowler }; }),
      fow: inn.fow.map((f) => ({ ...f, name: bp[f.player] })),
      ballLog: inn.log.map((l) => ({ ov: l.ov, lb: l.lb, bowler: wp[l.bowler], striker: l.striker == null ? null : bp[l.striker], non: l.non == null ? null : bp[l.non], x: l.x, r: l.r, bat: l.bat, extra: l.extra, total: l.total, tok: l.tok, out: l.w ? { k: l.w.k, who: bp[l.w.who], f: l.w.f, text: l.w.text } : null, runs: l.runs, wkts: l.wkts })),
      overHistory: inn.overs.map((o, i) => ({ n: i + 1, bowler: wp[o.bowler], balls: o.balls, runs: o.runs, partial: !!o.partial })),
      thisOver: inn.cur.slice(), curOverNo: inn.overs.length + 1,
      ballsLeft: maxBalls - inn.balls,
      need: inn.target ? Math.max(0, inn.target - inn.runs) : null,
      rrr: inn.target && maxBalls - inn.balls > 0 ? +(Math.max(0, inn.target - inn.runs) * 6 / (maxBalls - inn.balls)).toFixed(2) : null,
      projected: !inn.target && inn.balls ? Math.round(inn.runs * maxBalls / inn.balls) : null,
      lastBowler: inn.lastBowler,
      players: { bat: bp, bowl: wp },
    };
  };
  return {
    cur: st.cur, result: st.result, needs, innings: st.innings.map(mapInn),
    lastBall: events.length,
  };
}

module.exports = { ninjaViewUrl, normalizeConfig, replay, view, validate, needsOf, cleanName };
