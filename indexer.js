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

// --------------------
// Web UI socket server
// --------------------
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });

const WS_PORT = Number(process.env.WS_PORT || 3010);

app.get("/health", (req, res) => res.json({ ok: true }));

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
    if (channel === "stats") socket.join(`mint:${mint}:stats`);
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
    if (channel === "stats") socket.leave(`mint:${mint}:stats`);
  });

  // optional: allow global streams
  socket.on("joinGlobalPrices", () => socket.join("global:prices"));
  socket.on("joinGlobalEvents", () => socket.join("global:events"));
});

server.listen(WS_PORT, () => {
  console.log(`Indexer socket server on :${WS_PORT}`);
});

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
          io.to(`mint:${mint}:candles:1m`).emit("candle1m", candle);
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

          io.to(`mint:${mint}:candles:1m`).emit("candle1m", candle);
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

          io.to(`mint:${mint}:candles:1m`).emit("candle1m", candle);
          io.to(`mint:${mint}:stats`).emit("stats", stats);
          io.to(`mint:${mint}:trades`).emit("trade", { sig, slot: ctxSlot, mint, user, side: "SELL", priceSol, ts });
        }
      }

      // --------------------
      // broadcast raw events
      // --------------------
      const rawEvent = { sig, slot: ctxSlot, eventName, mint, user, payload: JSON.parse(safeJson(payload)) };
      io.emit("event", rawEvent);
      io.to("global:events").emit("event", rawEvent);
      if (mint) io.to(`mint:${mint}:events`).emit("event", rawEvent);
    }
  });

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
