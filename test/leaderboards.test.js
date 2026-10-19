const test = require("node:test"), assert = require("node:assert/strict");
const Database = require("better-sqlite3");
const { createLeaderboards } = require("../leaderboards");
const { createFusionVolume } = require("../fusion-volume");
const HOUR=3600000, start=1800000000000;
function fixture(t) {
  const db=new Database(":memory:");t.after(()=>db.close());
  db.exec(`CREATE TABLE trades(id INTEGER PRIMARY KEY,mint TEXT,created_at INTEGER,block_time INTEGER,quote_asset TEXT,quote_amount TEXT,price_usd REAL,price_quote REAL,side TEXT);
    CREATE TABLE token_stats(mint TEXT,name TEXT,symbol TEXT,image TEXT,price_usd REAL,updated_at INTEGER);
    CREATE TABLE candles_1m(mint TEXT,bucket_ts INTEGER,open_usd REAL,close_usd REAL);
    CREATE TABLE top_token_state(id INTEGER PRIMARY KEY,mint TEXT,round_start INTEGER,round_end INTEGER,round_volume_usd REAL,round_volume_quote REAL,trades_count INTEGER,buys_count INTEGER,sells_count INTEGER,last_trade_at INTEGER,updated_at INTEGER);`);
  let now=start+HOUR+120000;t.mock.method(Date,"now",()=>now);
  const fusionDb=new Database(":memory:");t.after(()=>fusionDb.close());
  fusionDb.exec(`CREATE TABLE ll_tokens(mint TEXT,data TEXT);
    CREATE TABLE ll_volume_pools(mint TEXT,data TEXT);
    CREATE TABLE ll_volume_trades(signature TEXT,pool_id TEXT,instruction TEXT,mint TEXT,at INTEGER,side TEXT,volume_usd REAL,quote_ui REAL,price_usd REAL);`);
  fusionDb.prepare("INSERT INTO ll_tokens VALUES(?,?)").run("fusion",JSON.stringify({mint:"fusion",name:"Fusion Token",symbol:"FUS",image:"https://example.com/token.png",poolId:"curve",status:"bonding"}));
  const coverage = (through=start+2*HOUR) => fusionDb.prepare("INSERT INTO ll_volume_pools VALUES(?,?)").run("fusion",JSON.stringify({coverageStart:start-HOUR,coveredThrough:through,market:{poolId:"curve",venue:"launchlab"}}));
  const trade=(mint,at,usd,signature=String(at))=>fusionDb.prepare("INSERT INTO ll_volume_trades VALUES(?,?,?,?,?,?,?,?,?)").run(signature,"curve","0:0",mint,at,"buy",usd,usd===null?1:usd/100,usd===null?null:usd/2);
  const legacy=(mint,at,quoteRaw,asset="SOL",priceUsd=100,priceQuote=1)=>db.prepare("INSERT INTO trades(mint,created_at,block_time,quote_asset,quote_amount,price_usd,price_quote,side) VALUES(?,?,?,?,?,?,?,?)")
    .run(mint,Math.floor(now/1000),Math.floor(at/1000),asset,String(quoteRaw),priceUsd,priceQuote,"buy");
  const fusion=createFusionVolume(null,fusionDb), board=createLeaderboards({db,getPrice:()=>({price:999}),fusion});
  return {db,fusionDb,coverage,trade,legacy,board,fusion,time:v=>{now=v;}};
}
test("one leaderboard includes both sources; previous completed hour wins, current hour stays live",t=>{
  const f=fixture(t);f.coverage();f.legacy("legacy",start+1000,1000000000);
  f.trade("fusion",start+2000,200);f.legacy("legacy",start+HOUR,3000000000);f.trade("fusion",start+HOUR+1000,50);
  const snap=f.board.getLeaderboardsSnapshot();assert.equal(snap.top_token.mint,"fusion");assert.equal(snap.top_token.name,"Fusion Token");
  assert.equal(snap.top_token.hour_volume_usd,200);assert.equal(snap.top_token.round_start,start);
  assert.deepEqual(snap.top10.map(r=>[r.mint,r.hour_volume_usd]),[["legacy",300],["fusion",50]]);
  assert.equal(snap.top_token_source,"completed_round");
});
test("late Fusion catch-up and missing prices hold the existing winner; quiet hours preserve it",t=>{
  const f=fixture(t);f.legacy("legacy",start+1000,1000000000);f.trade("fusion",start+2000,200);
  f.db.prepare("INSERT INTO top_token_state VALUES(1,?,?,?,?,?,?,?,?,?,?)").run("old",start-HOUR,start,500,5,1,1,0,start-1,0);
  assert.equal(f.board.getLeaderboardsSnapshot().top_token.mint,"old");
  f.coverage();f.trade("fusion",start+3000,null);
  assert.equal(f.board.getLeaderboardsSnapshot().fusion_volume.completed_round_pending,true);
  assert.equal(f.board.getLeaderboardsSnapshot().top_token.mint,"old");
  f.fusionDb.prepare("UPDATE ll_volume_trades SET volume_usd=50 WHERE volume_usd IS NULL").run();
  assert.equal(f.board.getLeaderboardsSnapshot().top_token.mint,"fusion");
  f.time(start+2*HOUR+120000);assert.equal(f.board.getLeaderboardsSnapshot().top_token.mint,"fusion");
});
test("known Fusion mints are not double-counted and exact hour boundaries separate rounds",t=>{
  const f=fixture(t);f.coverage();f.legacy("fusion",start+HOUR,9000000000);f.trade("fusion",start+HOUR,100);
  f.trade("fusion",start+HOUR-1,75);const snap=f.board.getLeaderboardsSnapshot();
  assert.equal(snap.top10[0].hour_volume_usd,100);assert.equal(snap.top10[0].trades_count,1);assert.equal(snap.top_token.hour_volume_usd,75);
});
test("large rounds are not truncated at 20000 trades and stored prices are not repriced today",t=>{
  const f=fixture(t);f.coverage();f.db.transaction(()=>{for(let i=0;i<20001;i++)f.legacy("legacy",start+HOUR+1000,1000000,"USDC",1,1);})();
  const snap=f.board.getLeaderboardsSnapshot();assert.equal(snap.top10[0].hour_volume_usd,20001);assert.equal(snap.top10[0].trades_count,20001);
});
test("Fusion candles contain only recorded priced swaps and survive graduation",t=>{
  const f=fixture(t);f.trade("fusion",start+1000,100);f.trade("fusion",start+2000,200);f.trade("fusion",start+3000,null);
  f.trade("fusion",start+901000,300);const bars=f.fusion.candles({mint:"fusion",interval:"15m",limit:32});
  assert.equal(bars.length,2);assert.equal(bars[0].bucket_ts,start/1000);assert.equal(bars[0].open_usd,50);assert.equal(bars[0].close_usd,100);
  assert.equal(bars[0].volume_usd,300);assert.equal(f.fusion.candles({mint:"legacy"}),null);
  assert.throws(()=>f.fusion.candles({mint:"fusion",interval:"bad"}),/Invalid interval/);
});
test("unconfigured Fusion leaves v1 available; configured but unavailable Fusion blocks settlement",t=>{
  const f=fixture(t);f.legacy("legacy",start+1000,1000000000);
  const legacy=createLeaderboards({db:f.db,getPrice:()=>({price:100}),fusion:createFusionVolume("")});
  assert.equal(legacy.getLeaderboardsSnapshot().top_token.mint,"legacy");
  const unavailable=createFusionVolume("/nonexistent/fusion.sqlite");assert.equal(unavailable.snapshot(start,start+HOUR).ready,false);
  assert.equal(unavailable.candles({mint:"legacy"}),null);
});
