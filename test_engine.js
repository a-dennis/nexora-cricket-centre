const E=require('./engine.js');const assert=require('assert');
const cfg=E.normalizeConfig({overs:2,teams:[{name:'A',players:['a1','a2','a3']},{name:'B',players:['b1','b2','b3']}]});
let ev=[];
function push(e){const st=E.replay(cfg,ev);const er=E.validate(cfg,st,e);if(er)throw new Error(er+' '+JSON.stringify(e));ev.push(e);return E.view(cfg,ev);}
push({t:'sel',role:'striker',player:0});push({t:'sel',role:'non',player:1});push({t:'sel',role:'bowler',player:0});
let v=push({t:'ball',r:1,x:''}); // a2 on strike
assert.equal(v.innings[0].striker,1);
v=push({t:'ball',r:0,x:'wd'}); assert.equal(v.innings[0].runs,2); assert.equal(v.innings[0].balls,1);
v=push({t:'ball',r:4,x:''}); v=push({t:'ball',r:0,x:'nb'}); assert.equal(v.innings[0].runs,2+4+1);
v=push({t:'ball',r:0,x:'',w:{k:'bowled',who:'s'}}); assert.deepEqual(v.needs,['newbat']);
assert.throws(()=>push({t:'ball',r:1,x:''}));
v=push({t:'sel',role:'newbat',player:2}); assert.equal(v.innings[0].striker,2);
v=push({t:'ball',r:6,x:''}); v=push({t:'ball',r:2,x:'b'}); v=push({t:'ball',r:0,x:''}); assert.deepEqual(v.needs,['bowler']);
assert.equal(v.innings[0].balls,6);
assert.throws(()=>push({t:'sel',role:'bowler',player:0}));
v=push({t:'sel',role:'bowler',player:1});
// over2: wicket ends innings after a3 out? players 3 -> allout at 2 wkts
v=push({t:'ball',r:0,x:'',w:{k:'caught',who:'s',f:'b3'}});
console.log(JSON.stringify(v.innings[0].batting.map(b=>[b.name,b.runs,b.balls,b.out])), v.innings[0].runs, v.innings[0].wkts, v.cur, v.innings[0].done);
assert.equal(v.cur,1); assert.equal(v.innings[1].target, v.innings[0].runs+1);
// chase
push({t:'sel',role:'striker',player:0});push({t:'sel',role:'non',player:1});push({t:'sel',role:'bowler',player:2});
v=E.view(cfg,ev);
for(let i=0;i<30;i++){ if(v.needs.includes('bowler')) v=push({t:'sel',role:'bowler',player:v.innings[1].lastBowler===2?0:2}); v=push({t:'ball',r:2,x:'lb'}); if(v.result)break;}
console.log(v.result, v.innings[1].runs, v.innings[1].balls);
assert(v.result);
console.log('tests ok');
