const Database = require("better-sqlite3");

// Both the HTTP snapshot and Redis publisher read the same durable ledger.
// No RPC/provider work occurs in a page request. The indexer cannot write to it.
function createFusionVolume(filename = process.env.MOONZ_FUSION_DB_PATH, injectedDb = null) {
  let connection = injectedDb, retryAt = 0;
  const enabled = !!filename || !!injectedDb;
  function db() {
    if (!enabled) return null;
    if (connection) return connection;
    if (Date.now() < retryAt) throw new Error("Fusion volume is reconnecting");
    try {
      connection = new Database(filename, { readonly: true, fileMustExist: true, timeout: 1000 });
      connection.pragma("query_only = ON");
      return connection;
    } catch { retryAt = Date.now() + 10000; throw new Error("Fusion volume database unavailable"); }
  }
  function snapshot(start, end) {
    if (!enabled) return { enabled: false, ready: true, rows: [], mints: new Set() };
    try {
      const source = db();
      return source.transaction(() => {
        const tokens = source.prepare("SELECT mint,data FROM ll_tokens").all();
        const pools = source.prepare("SELECT mint,data FROM ll_volume_pools").all().map(r => ({ mint:r.mint,...JSON.parse(r.data) }));
        const mints = new Set(tokens.map(t => t.mint));
        const unpriced = source.prepare("SELECT COUNT(*) AS n FROM ll_volume_trades WHERE at>=? AND at<? AND volume_usd IS NULL").get(start,end).n;
        const ready = !unpriced && tokens.every(t => {
          const token = JSON.parse(t.data), tracked = pools.filter(p => p.mint === t.mint);
          return tracked.some(p => p.market.poolId === token.poolId) &&
            (token.status !== "graduated" || tracked.some(p => p.market.venue === "cpmm" && (!token.cpmmPoolId || p.market.poolId === token.cpmmPoolId))) &&
            tracked.every(p => p.coverageStart <= start && p.coveredThrough >= end);
        });
        const rows = source.prepare(`SELECT mint,at AS block_time,volume_usd,quote_ui AS quote_amount_ui,side
          FROM ll_volume_trades WHERE at>=? AND at<? AND volume_usd IS NOT NULL ORDER BY at,signature,instruction`).all(start,end)
          .filter(r => mints.has(r.mint)).map(r => ({ ...r,source:"fusion" }));
        return { enabled:true,ready,rows,mints,unpriced_trades:unpriced };
      })();
    } catch { return { enabled:true,ready:false,rows:[],mints:new Set(),unavailable:true }; }
  }
  function metadata(mint) {
    if (!enabled) return null;
    try {
      const source = db(), row = source.prepare("SELECT data FROM ll_tokens WHERE mint=?").get(mint);
      if (!row) return null;
      const token = JSON.parse(row.data);
      const first = source.prepare("SELECT price_usd FROM ll_volume_trades WHERE mint=? AND price_usd>0 ORDER BY at,signature,instruction LIMIT 1").get(mint);
      const last = source.prepare("SELECT price_usd FROM ll_volume_trades WHERE mint=? AND price_usd>0 ORDER BY at DESC,signature DESC,instruction DESC LIMIT 1").get(mint);
      return { ...token, source:"fusion",price_usd:last?.price_usd || null,launch_price_usd:first?.price_usd || null,
        price_change_since_launch_percent: first?.price_usd && last?.price_usd ? (last.price_usd / first.price_usd - 1) * 100 : null };
    } catch { return null; }
  }
  function candles({ mint,interval="1m",limit=500,since=null }) {
    if (!enabled) return null;
    let source;
    try {
      source = db();
      if (!source.prepare("SELECT 1 FROM sqlite_master WHERE name='ll_volume_trades'").get() ||
          !source.prepare("SELECT 1 FROM ll_tokens WHERE mint=?").get(mint)) return null;
    } catch { return null; }
    const seconds = {"1m":60,"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400}[interval];
    if (!seconds) throw new Error("Invalid interval");
    const capped = Math.min(5000,Math.max(1,Math.floor(Number(limit)||500)));
    return source.prepare(`WITH ticks AS (
      SELECT *,CAST(at / ? AS INTEGER)*? AS bucket_ts,
        ROW_NUMBER() OVER (PARTITION BY CAST(at / ? AS INTEGER) ORDER BY at,signature,instruction) AS first,
        ROW_NUMBER() OVER (PARTITION BY CAST(at / ? AS INTEGER) ORDER BY at DESC,signature DESC,instruction DESC) AS last
      FROM ll_volume_trades WHERE mint=? AND price_usd>0 AND (? IS NULL OR at>=?)
    ) SELECT bucket_ts,MAX(CASE WHEN first=1 THEN price_usd END) AS open_usd,MAX(price_usd) AS high_usd,
      MIN(price_usd) AS low_usd,MAX(CASE WHEN last=1 THEN price_usd END) AS close_usd,SUM(volume_usd) AS volume_usd,COUNT(*) AS trades
      FROM ticks GROUP BY bucket_ts ORDER BY bucket_ts DESC LIMIT ?`)
      .all(seconds*1000,seconds,seconds*1000,seconds*1000,mint,since,since === null ? null : since*1000,capped).reverse();
  }
  return { snapshot,metadata,candles,enabled };
}
module.exports = { createFusionVolume };
