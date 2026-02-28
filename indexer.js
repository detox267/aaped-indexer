// indexer.js
require("dotenv").config();

const fs = require("fs");
const WebSocket = require("ws");
const anchor = require("@coral-xyz/anchor");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const {
  db,
  hasSeenTx,
  markTxSeen,
  upsertLaunch,
  insertTrade,
  setPrice,
  getPrice,
  now,
} = require("./db");

const fetch = globalThis.fetch || require("node-fetch");
const PROGRAM_ID = process.env.PROGRAM_ID;
const IDL_PATH = process.env.IDL_PATH;

if (!PROGRAM_ID) throw new Error("Missing PROGRAM_ID");
if (!IDL_PATH) throw new Error("Missing IDL_PATH");
if (!fs.existsSync(IDL_PATH)) throw new Error(`IDL_PATH not found: ${IDL_PATH}`);

const idl = JSON.parse(fs.readFileSync(IDL_PATH, "utf8"));
const coder = new anchor.BorshCoder(idl);

// --------------------
// constants (your setup)
// --------------------
const SOL_DECIMALS = 1e9;
const TOKEN_DECIMALS = 1e6; // ✅ you said 6 decimals
const TOTAL_SUPPLY_TOKENS = 1_000_000_000; // ✅ 1,000,000,000.000000
const TOTAL_SUPPLY_BASE = TOTAL_SUPPLY_TOKENS * TOKEN_DECIMALS;
const V_SOL = 75.8;           // virtual SOL
const V_TOK = 526_200_000;    // virtual tokens
const ENABLE_GLOBAL_EVENTS = process.env.ENABLE_GLOBAL_EVENTS === "true";

const TF_SECONDS = {
  "1m": 60,
  "5m": 300,
  "15m": 900,
  "30m": 1800,
  "1h": 3600,
  "4h": 14400,
  "1d": 86400,
};

function normalizeTf(tf) {
  const key = String(tf || "1m").toLowerCase();
  return TF_SECONDS[key] ? key : null;
}
// --------------------
// Web UI socket server
// --------------------
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });

const WS_PORT = Number(process.env.WS_PORT || 3010);

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/simulate-buy", (req, res) => {
  const mint = req.query.mint;
  const sol = Number(req.query.sol);

  if (!mint || !sol) {
    return res.status(400).json({ error: "mint and sol required" });
  }

  const result = simulateBuy(mint, sol);

  res.json(result);
});

const lastAggEmit = new Map(); // key = `${mint}:${tf}` -> tf_bucket_ts

app.get("/candles", (req, res) => {
  const mint = req.query.mint;
  const tf = normalizeTf(req.query.interval || "1m");
  const limit = Math.min(5000, Math.max(1, Number(req.query.limit || 500)));
  const since = req.query.since ? Number(req.query.since) : null; // unix seconds

  if (!mint) return res.status(400).json({ error: "mint required" });
  if (!tf) return res.status(400).json({ error: "invalid interval. use 1m,5m,15m,30m,1h,4h,1d" });

  // 1m = direct read
  if (tf === "1m") {
    const rows = db.prepare(`
      SELECT
        bucket_ts,
        open_sol,
        high_sol,
        low_sol,
        close_sol,
        volume_sol,
        volume_tokens,
        trades_count,
        buys_count,
        sells_count
      FROM candles_1m
      WHERE mint = ?
        AND (? IS NULL OR bucket_ts >= ?)
      ORDER BY bucket_ts DESC
      LIMIT ?
    `).all(mint, since, since, limit);

    return res.json(rows.reverse());
  }

  const step = TF_SECONDS[tf];

  // Derived aggregation from 1m using window functions
  const rows = db.prepare(`
    WITH base AS (
      SELECT
        mint,
        bucket_ts,
        (bucket_ts / ?) * ? AS tf_bucket,
        open_sol, high_sol, low_sol, close_sol,
        volume_sol, volume_tokens,
        trades_count, buys_count, sells_count
      FROM candles_1m
      WHERE mint = ?
        AND (? IS NULL OR bucket_ts >= ?)
    ),
    ranked AS (
      SELECT
        tf_bucket,
        open_sol,
        close_sol,
        high_sol,
        low_sol,
        volume_sol,
        volume_tokens,
        trades_count,
        buys_count,
        sells_count,
        ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts ASC)  AS rn_open,
        ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts DESC) AS rn_close
      FROM base
    ),
    agg AS (
      SELECT
        tf_bucket AS bucket_ts,
        MAX(CASE WHEN rn_open = 1 THEN open_sol END)  AS open_sol,
        MAX(high_sol) AS high_sol,
        MIN(low_sol)  AS low_sol,
        MAX(CASE WHEN rn_close = 1 THEN close_sol END) AS close_sol,
        SUM(volume_sol)    AS volume_sol,
        SUM(volume_tokens) AS volume_tokens,
        SUM(trades_count)  AS trades_count,
        SUM(buys_count)    AS buys_count,
        SUM(sells_count)   AS sells_count
      FROM ranked
      GROUP BY tf_bucket
    )
    SELECT *
    FROM agg
    ORDER BY bucket_ts DESC
    LIMIT ?
  `).all(step, step, mint, since, since, limit);

  res.json(rows.reverse());
});

io.on("connection", (socket) => {
  // join by mint + channel
  // { mint, channel: "events"|"trades"|"candles1m"|"stats" } OR { room: "global:prices" }
  socket.on("join", (msg = {}) => {
    if (msg.room && typeof msg.room === "string") {
      socket.join(msg.room);
      return;
    }
    const mint = typeof msg.mint === "string" ? msg.mint : null;
    const channel = typeof msg.channel === "string" ? msg.channel : "events";
    if (!mint) return;

    if (channel === "events") socket.join(`mint:${mint}:events`);
    if (channel === "trades") socket.join(`mint:${mint}:trades`);
    if (channel === "candles1m") socket.join(`mint:${mint}:candles:1m`);
    if (channel.startsWith("candles:")) {
      const tf = normalizeTf(channel.split(":")[1]);
      if (tf) socket.join(`mint:${mint}:candles:${tf}`);
    }
    if (channel === "stats") socket.join(`mint:${mint}:stats`);
    if (channel === "ticks") socket.join(`mint:${mint}:ticks`);
  });

  socket.on("leave", (msg = {}) => {
  if (msg.room && typeof msg.room === "string") {
    socket.leave(msg.room);
    return;
  }

  const mint = typeof msg.mint === "string" ? msg.mint : null;
  const channel = typeof msg.channel === "string" ? msg.channel : "events";
  if (!mint) return;

  if (channel === "events") socket.leave(`mint:${mint}:events`);
  if (channel === "trades") socket.leave(`mint:${mint}:trades`);
  if (channel === "candles1m") socket.leave(`mint:${mint}:candles:1m`);

  if (channel.startsWith("candles:")) {
    const tf = normalizeTf(channel.split(":")[1]);
    if (tf) socket.leave(`mint:${mint}:candles:${tf}`);
  }

  if (channel === "stats") socket.leave(`mint:${mint}:stats`);
  if (channel === "ticks") socket.leave(`mint:${mint}:ticks`);
});
  
socket.on("simulateBuy", (msg = {}) => {
    const mint = msg.mint;
    const sol = Number(msg.sol);

    if (!mint || !sol) return;

    const result = simulateBuy(mint, sol);

    socket.emit("simulation", result);
  });
  // optional: allow global streams
  socket.on("joinGlobalPrices", () => socket.join("global:prices"));
  socket.on("joinGlobalEvents", () => socket.join("global:events"));
});

server.listen(WS_PORT, () => {
  console.log(`Indexer socket server on :${WS_PORT}`);
});

const candleEmitThrottle = new Map(); // mint -> lastEmitMs

function emitCandleThrottled(mint, candle) {
  const nowMs = Date.now();
  const last = candleEmitThrottle.get(mint) || 0;
  if (nowMs - last < 1000) return; // max 1/sec per mint
  candleEmitThrottle.set(mint, nowMs);

  io.to(`mint:${mint}:candles:1m`).emit("candle", { tf: "1m", ...candle });
}

const TICK_FLUSH_MS = Number(process.env.TICK_FLUSH_MS || 250);

// latest tick per mint (overwritten frequently)
const pendingTicks = new Map(); // mint -> tick

function queueTick(mint, tick) {
  if (!mint) return;
  pendingTicks.set(mint, tick);
}

setInterval(() => {
  if (pendingTicks.size === 0) return;

  // flush each mint once per interval
  for (const [mint, tick] of pendingTicks.entries()) {
    io.to(`mint:${mint}:ticks`).emit("tick", tick);
  }
  pendingTicks.clear();
}, TICK_FLUSH_MS);

console.log(`Tick batching enabled: flush every ${TICK_FLUSH_MS}ms`);
// --------------------
// SOL price poller (Dexscreener)
// --------------------
const SOL_PAIR =
  process.env.DEX_SOL_PAIR ||
  "58oqchx4ywmvkdwllzzbi4chocc2fqcuwbkwmihlyqo2";

const DEX_URL = `https://api.dexscreener.com/latest/dex/pairs/solana/${SOL_PAIR}`;

async function pollSolPriceOnce() {
  const res = await fetch(DEX_URL);
  if (!res.ok) throw new Error(`Dexscreener HTTP ${res.status}`);

  const json = await res.json();
  const pair = json?.pairs?.[0];
  const priceUsd = pair?.priceUsd ? Number(pair.priceUsd) : null;

  if (!priceUsd || !Number.isFinite(priceUsd)) {
    throw new Error("No priceUsd in Dexscreener response");
  }

  setPrice("SOL_USD", priceUsd);

  const payload = { key: "SOL_USD", price: priceUsd, updated_at: now() };
  io.emit("price", payload);
  io.to("global:prices").emit("price", payload);
}

function startSolPricePoller() {
  const intervalMs = Number(process.env.SOL_PRICE_INTERVAL_MS || 15_000);

  const loop = async () => {
    try {
      await pollSolPriceOnce();
    } catch (e) {
      console.error("SOL price poll error:", e.message);
    }
  };

  loop();
  setInterval(loop, intervalMs);

  console.log(`SOL price poller started (${intervalMs}ms) pair ${SOL_PAIR}`);
}
startSolPricePoller();

// --------------------
// candle + stats writers
// --------------------
function minuteBucket(tsSec) {
  return Math.floor(tsSec / 60) * 60;
}

function toNumStr(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (typeof v?.toString === "function") return v.toString();
  return null;
}

function safeJson(v) {
  return JSON.stringify(v, (_, val) => {
    if (val && typeof val === "object") {
      if (typeof val.toBase58 === "function") return val.toBase58();
      if (val.constructor && val.constructor.name === "BN") return val.toString();
    }
    return val;
  });
}

function asStrMaybePk(v) {
  if (!v) return null;
  if (typeof v === "string") return v;
  if (typeof v.toBase58 === "function") return v.toBase58();
  if (typeof v.toString === "function") return v.toString();
  return null;
}

function computeTradePriceSol({ side, solLamportsStr, tokenBaseStr }) {
  const solLamports = Number(solLamportsStr || 0);
  const tokenBase = Number(tokenBaseStr || 0);
  if (!solLamports || !tokenBase) return null;

  const sol = solLamports / SOL_DECIMALS;
  const tokens = tokenBase / TOKEN_DECIMALS;
  if (tokens <= 0) return null;

  return sol / tokens; // SOL per 1 token
}

function emitTick(mint, ts, side, priceSol) {
  if (!mint || !priceSol) return;

  const solUsd = getPrice("SOL_USD")?.price || null;
  const priceUsd = solUsd ? priceSol * solUsd : null;

  queueTick(mint, {
    mint,
    ts,
    side,
    price_sol: priceSol,
    price_usd: priceUsd,
  });
}

function upsertCandle1m(mint, tsSec, priceSol, volSol, volTokens, side) {
  const bucket = minuteBucket(tsSec);
  const existing = db
    .prepare(`SELECT * FROM candles_1m WHERE mint=? AND bucket_ts=?`)
    .get(mint, bucket);

  if (!existing) {
    db.prepare(`
      INSERT INTO candles_1m (
        mint, bucket_ts,
        open_sol, high_sol, low_sol, close_sol,
        volume_sol, volume_tokens,
        trades_count, buys_count, sells_count,
        updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      mint,
      bucket,
      priceSol,
      priceSol,
      priceSol,
      priceSol,
      volSol || 0,
      volTokens || 0,
      1,
      side === "BUY" || side === "DEVBUY" ? 1 : 0,
      side === "SELL" ? 1 : 0,
      now()
    );
    return db.prepare(`SELECT * FROM candles_1m WHERE mint=? AND bucket_ts=?`).get(mint, bucket);
  }

  const high = Math.max(existing.high_sol ?? priceSol, priceSol);
  const low = Math.min(existing.low_sol ?? priceSol, priceSol);

  db.prepare(`
    UPDATE candles_1m
    SET
      high_sol = ?,
      low_sol = ?,
      close_sol = ?,
      volume_sol = volume_sol + ?,
      volume_tokens = volume_tokens + ?,
      trades_count = trades_count + 1,
      buys_count = buys_count + ?,
      sells_count = sells_count + ?,
      updated_at = ?
    WHERE mint=? AND bucket_ts=?
  `).run(
    high,
    low,
    priceSol,
    volSol || 0,
    volTokens || 0,
    side === "BUY" || side === "DEVBUY" ? 1 : 0,
    side === "SELL" ? 1 : 0,
    now(),
    mint,
    bucket
  );

  return db.prepare(`SELECT * FROM candles_1m WHERE mint=? AND bucket_ts=?`).get(mint, bucket);
}

// --------------------
// live aggregated candle emission (derived TFs)
// --------------------
const LIVE_TFS = ["5m", "15m", "30m", "1h", "4h", "1d"];

function getAggCandleForBucket(mint, tf, tsSec) {
  const step = TF_SECONDS[tf];
  const tfBucket = Math.floor(tsSec / step) * step;

  const row = db.prepare(`
    WITH base AS (
      SELECT
        bucket_ts,
        (bucket_ts / ?) * ? AS tf_bucket,
        open_sol, high_sol, low_sol, close_sol,
        volume_sol, volume_tokens,
        trades_count, buys_count, sells_count
      FROM candles_1m
      WHERE mint = ?
        AND bucket_ts >= ?
        AND bucket_ts <  ?
    ),
    ranked AS (
      SELECT
        tf_bucket,
        open_sol,
        close_sol,
        high_sol,
        low_sol,
        volume_sol,
        volume_tokens,
        trades_count, buys_count, sells_count,
        ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts ASC)  AS rn_open,
        ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts DESC) AS rn_close
      FROM base
    )
    SELECT
      tf_bucket AS bucket_ts,
      MAX(CASE WHEN rn_open = 1 THEN open_sol END)  AS open_sol,
      MAX(high_sol) AS high_sol,
      MIN(low_sol)  AS low_sol,
      MAX(CASE WHEN rn_close = 1 THEN close_sol END) AS close_sol,
      SUM(volume_sol)    AS volume_sol,
      SUM(volume_tokens) AS volume_tokens,
      SUM(trades_count)  AS trades_count,
      SUM(buys_count)    AS buys_count,
      SUM(sells_count)   AS sells_count
    FROM ranked
    WHERE tf_bucket = ?
    GROUP BY tf_bucket
  `).get(step, step, mint, tfBucket, tfBucket + step, tfBucket);

  return row || null;
}

function emitLiveAggregates(mint, tsSec) {
  for (const tf of LIVE_TFS) {
    const step = TF_SECONDS[tf];
    const tfBucket = Math.floor(tsSec / step) * step;

    const key = `${mint}:${tf}`;
    const prev = lastAggEmit.get(key);

    // only recompute/emit once per tf-bucket (reduces spam)
    if (prev === tfBucket) continue;
    lastAggEmit.set(key, tfBucket);

    const agg = getAggCandleForBucket(mint, tf, tsSec);
    if (!agg) continue;
    io.to(`mint:${mint}:candles:${tf}`).emit("candle", { tf, ...agg });
  }
}

function safeObj(v) {
  return JSON.parse(JSON.stringify(v, (_, val) => {
    if (val && typeof val === "object") {
      if (typeof val.toBase58 === "function") return val.toBase58();
      if (val.constructor && val.constructor.name === "BN") return val.toString();
    }
    return val;
  }));
}

function upsertTokenStats(mint, patch) {
  const existing = db.prepare(`SELECT mint FROM token_stats WHERE mint=?`).get(mint);
  const fields = Object.keys(patch);
  if (!fields.length) return;

  if (!existing) {
    const cols = ["mint", ...fields, "updated_at"];
    const vals = [mint, ...fields.map((k) => patch[k]), now()];
    const q = `INSERT INTO token_stats (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`;
    db.prepare(q).run(...vals);
  } else {
    const sets = fields.map((k) => `${k}=?`).join(", ");
    const vals = [...fields.map((k) => patch[k]), now(), mint];
    db.prepare(`UPDATE token_stats SET ${sets}, updated_at=? WHERE mint=?`).run(...vals);
  }

  return db.prepare(`SELECT * FROM token_stats WHERE mint=?`).get(mint);
}

// --------------------
// Helius WS connection (logsSubscribe)
// --------------------
const HELIUS_WSS =
  process.env.HELIUS_WSS ||
  `wss://devnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY || ""}`;

if (!HELIUS_WSS.startsWith("wss://")) throw new Error("HELIUS_WSS must be a wss:// url");

let ws;
let pingInterval = null;

function cleanup() {
  try {
    if (pingInterval) clearInterval(pingInterval);
  } catch {}
  pingInterval = null;
}

function simulateBuy(mint, solInput) {
  const stats = db.prepare(`
    SELECT tokens_sold_total
    FROM token_stats
    WHERE mint=?
  `).get(mint);

  const tokensSold = stats?.tokens_sold_total
    ? Number(stats.tokens_sold_total) / TOKEN_DECIMALS
    : 0;

  const virtualSol = V_SOL + tokensSold;
  const virtualTok = Math.max(1, V_TOK - tokensSold);

  const k = virtualSol * virtualTok;

  const newSol = virtualSol + solInput;
  const newTok = k / newSol;

  const tokensOut = virtualTok - newTok;

  const priceBefore = virtualSol / virtualTok;
  const priceAfter = newSol / newTok;

  const solUsd = getPrice("SOL_USD")?.price || null;

  const priceUsd = solUsd ? priceAfter * solUsd : null;

  const marketcapUsd = priceUsd
    ? priceUsd * TOTAL_SUPPLY_TOKENS
    : null;

  const marketcapSol = priceAfter * TOTAL_SUPPLY_TOKENS;

  return {
    mint,
    solInput,
    tokensOut,
    priceBefore,
    priceAfter,
    priceUsd,
    marketcapUsd,
    marketcapSol
  };
}

function connect() {
  ws = new WebSocket(HELIUS_WSS);

  ws.on("open", () => {
    console.log("Connected to Helius WS");

    pingInterval = setInterval(() => {
      try {
        ws.ping();
      } catch {}
    }, 60_000);

    ws.send(JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [{ mentions: [PROGRAM_ID] }, { commitment: "confirmed" }],
    }));
  });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString("utf8")); } catch { return; }

    if (msg.id === 1 && msg.result) {
      console.log("logsSubscribe subId:", msg.result);
      return;
    }

    if (msg.method !== "logsNotification") return;

    const ctxSlot = msg.params?.result?.context?.slot ?? null;
    const value = msg.params?.result?.value;
    if (!value) return;

    const sig = value.signature;
    const logs = value.logs || [];
    if (!sig) return;
    if (value.err) return;

    if (hasSeenTx(sig)) return;
    markTxSeen(sig, ctxSlot);

    for (const line of logs) {
      const prefix = "Program data: ";
      if (!line.startsWith(prefix)) continue;

      let buf;
      try { buf = Buffer.from(line.slice(prefix.length).trim(), "base64"); }
      catch { continue; }

      let decoded;
      try { decoded = coder.events.decode(buf); }
      catch { continue; }

      if (!decoded) continue;

      const eventName = decoded.name;
      const payload = decoded.data;

      const mint = asStrMaybePk(payload.mint);
      const user = asStrMaybePk(payload.user);

      // --------------------
      // launches upserts
      // --------------------
      if (eventName === "LaunchInitialized") {
        upsertLaunch(mint, {
          launch_state: asStrMaybePk(payload.launch_state),
          creator: asStrMaybePk(payload.creator),
          platform: asStrMaybePk(payload.platform),
          core_authority: asStrMaybePk(payload.core_authority),
          total_supply: toNumStr(payload.total_supply),
          sale_supply: toNumStr(payload.sale_supply),
          lp_supply: toNumStr(payload.lp_supply),
        });
      }

      if (eventName === "MetadataInitialized") {
        upsertLaunch(mint, {
          name: payload.name ?? null,
          symbol: payload.symbol ?? null,
          metadata_uri: payload.uri ?? null,
        });
      }

      // --------------------
      // trades + candles + stats
      // --------------------
      const ts = Number(payload.ts ?? now());
      const solUsd = getPrice("SOL_USD")?.price || null;

      if (eventName === "CurveActivated") {
        // DEVBUY: use sol_in_gross + tokens_out
        const solLamports = toNumStr(payload.sol_in_gross);
        const tokBase = toNumStr(payload.tokens_out);
        const priceSol = computeTradePriceSol({ side: "DEVBUY", solLamportsStr: solLamports, tokenBaseStr: tokBase });

        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user: asStrMaybePk(payload.dev),
          side: "DEVBUY",
          phase_u8: null,
          sol_in_gross: solLamports,
          tokens_out: tokBase,
          ts_i64: toNumStr(payload.ts),
        });

        if (priceSol) {
          const volSol = (Number(solLamports) / SOL_DECIMALS);
          const volTok = (Number(tokBase) / TOKEN_DECIMALS);

          const candle = upsertCandle1m(mint, ts, priceSol, volSol, volTok, "DEVBUY");
          emitCandleThrottled(mint, candle);

          emitTick(mint, ts, "DEVBUY", priceSol); // ✅ add this
          emitLiveAggregates(mint, ts);
        }
      }

      if (eventName === "BuyExecuted") {
        const solLamports = toNumStr(payload.sol_in_gross);
        const tokBase = toNumStr(payload.tokens_out);
        const priceSol = computeTradePriceSol({ side: "BUY", solLamportsStr: solLamports, tokenBaseStr: tokBase });

        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user,
          side: "BUY",
          phase_u8: payload.phase ?? null,
          sol_in_gross: solLamports,
          sol_eff_used: toNumStr(payload.sol_eff_used),
          tokens_out: tokBase,
          creator_fee: toNumStr(payload.creator_fee),
          platform_fee: toNumStr(payload.platform_fee),
          lp_fee: toNumStr(payload.lp_fee),
          tokens_sold_total: toNumStr(payload.tokens_sold_total),
          sol_collected_total: toNumStr(payload.sol_collected_total),
          ts_i64: toNumStr(payload.ts),
        });

        if (priceSol) {
          const volSol = (Number(solLamports) / SOL_DECIMALS);
          const volTok = (Number(tokBase) / TOKEN_DECIMALS);

          const candle = upsertCandle1m(mint, ts, priceSol, volSol, volTok, "BUY");

          const priceUsd = solUsd ? priceSol * solUsd : null;
          const marketcapUsd = priceUsd ? priceUsd * TOTAL_SUPPLY_TOKENS : null;
          const marketcapSol = priceSol * TOTAL_SUPPLY_TOKENS;

          // progress uses sale_supply from launches (since migration = sold out)
          const launch = db.prepare(`SELECT sale_supply FROM launches WHERE mint=?`).get(mint);
          const saleSupply = launch?.sale_supply ? Number(launch.sale_supply) : null;
          const soldTotal = payload.tokens_sold_total ? Number(toNumStr(payload.tokens_sold_total)) : null;

          const progress = (saleSupply && soldTotal !== null)
            ? Math.max(0, Math.min(1, soldTotal / saleSupply))
            : null;

          const stats = upsertTokenStats(mint, {
            last_price_sol: priceSol,
            last_price_usd: priceUsd,
            marketcap_usd: marketcapUsd,
            marketcap_sol: marketcapSol,
            tokens_sold_total: toNumStr(payload.tokens_sold_total),
            sale_supply: launch?.sale_supply ?? null,
            progress,
            last_trade_ts: ts,
          });

          emitCandleThrottled(mint, candle);
          emitTick(mint, ts, "BUY", priceSol);
          emitLiveAggregates(mint, ts);
          io.to(`mint:${mint}:stats`).emit("stats", stats);
          io.to(`mint:${mint}:trades`).emit("trade", { sig, slot: ctxSlot, mint, user, side: "BUY", priceSol, ts });
        }
      }

      if (eventName === "SellExecuted") {
        const solLamports = toNumStr(payload.sol_net);
        const tokBase = toNumStr(payload.tokens_in);
        const priceSol = computeTradePriceSol({ side: "SELL", solLamportsStr: solLamports, tokenBaseStr: tokBase });

        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user,
          side: "SELL",
          phase_u8: payload.phase ?? null,
          sol_net: solLamports,
          sol_gross: toNumStr(payload.sol_gross),
          tokens_in: tokBase,
          creator_fee: toNumStr(payload.creator_fee),
          platform_fee: toNumStr(payload.platform_fee),
          lp_fee: toNumStr(payload.lp_fee),
          tokens_sold_total: toNumStr(payload.tokens_sold_total),
          sol_collected_total: toNumStr(payload.sol_collected_total),
          ts_i64: toNumStr(payload.ts),
        });

        if (priceSol) {
          const volSol = (Number(solLamports) / SOL_DECIMALS);
          const volTok = (Number(tokBase) / TOKEN_DECIMALS);

          const candle = upsertCandle1m(mint, ts, priceSol, volSol, volTok, "SELL");

          const priceUsd = solUsd ? priceSol * solUsd : null;
          const marketcapUsd = priceUsd ? priceUsd * TOTAL_SUPPLY_TOKENS : null;
          const marketcapSol = priceSol * TOTAL_SUPPLY_TOKENS;

          const launch = db.prepare(`SELECT sale_supply FROM launches WHERE mint=?`).get(mint);
          const saleSupply = launch?.sale_supply ? Number(launch.sale_supply) : null;
          const soldTotal = payload.tokens_sold_total ? Number(toNumStr(payload.tokens_sold_total)) : null;

          const progress = (saleSupply && soldTotal !== null)
            ? Math.max(0, Math.min(1, soldTotal / saleSupply))
            : null;

          const stats = upsertTokenStats(mint, {
            last_price_sol: priceSol,
            last_price_usd: priceUsd,
            marketcap_usd: marketcapUsd,
            marketcap_sol: marketcapSol,
            tokens_sold_total: toNumStr(payload.tokens_sold_total),
            sale_supply: launch?.sale_supply ?? null,
            progress,
            last_trade_ts: ts,
          });

          emitCandleThrottled(mint, candle);
          emitTick(mint, ts, "SELL", priceSol);
          emitLiveAggregates(mint, ts);
          io.to(`mint:${mint}:stats`).emit("stats", stats);
          io.to(`mint:${mint}:trades`).emit("trade", { sig, slot: ctxSlot, mint, user, side: "SELL", priceSol, ts });
        }
      }

      const rawEvent = {
        sig,
        slot: ctxSlot,
        eventName,
        mint,
        user,
        payload: safeObj(payload),
      };

      if (ENABLE_GLOBAL_EVENTS) {
        io.to("global:events").emit("event", rawEvent);
      }
      if (mint) {
        io.to(`mint:${mint}:events`).emit("event", rawEvent);
      }
    } // <-- closes: for (const line of logs)
  });  // <-- closes: ws.on("message")

  ws.on("close", () => {
    console.log("Helius WS closed. Reconnecting...");
    cleanup();
    setTimeout(connect, 1500);
  });

  ws.on("error", (e) => {
    console.error("Helius WS error:", e.message);
  });
}

connect();
