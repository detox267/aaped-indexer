// indexer.js
require("dotenv").config();

const fs = require("fs");
const WebSocket = require("ws");
const anchor = require("@coral-xyz/anchor");

// your local db helpers (must export: db, hasSeenTx, markTxSeen, upsertLaunch, insertTrade)
const { db, hasSeenTx, markTxSeen, upsertLaunch, insertTrade } = require("./db");

// Node 18+ has global fetch. If you're on Node 16, install node-fetch and uncomment below.
// const fetch = require("node-fetch");

const PROGRAM_ID = process.env.PROGRAM_ID;
const IDL_PATH = process.env.IDL_PATH;

if (!PROGRAM_ID) throw new Error("Missing PROGRAM_ID");
if (!IDL_PATH) throw new Error("Missing IDL_PATH");
if (!fs.existsSync(IDL_PATH)) throw new Error(`IDL_PATH not found: ${IDL_PATH}`);

const idl = JSON.parse(fs.readFileSync(IDL_PATH, "utf8"));
const coder = new anchor.BorshCoder(idl);

// ------------------------------------------------------------
// Rooms / Topics (DOWNSTREAM EMIT CONTRACT)
// ------------------------------------------------------------
// Global rooms:
const ROOMS = {
  GLOBAL_EVENTS: "global:events",
  GLOBAL_TRADES: "global:trades",
  GLOBAL_LAUNCH: "global:launch",
  GLOBAL_MIGRATION: "global:migration",
  GLOBAL_PRICES: "global:prices",
};

// Mint namespace root: `mint:<mint>`
// Mint-scoped topic rooms:
// - `mint:<mint>:events`
// - `mint:<mint>:trades`
// - `mint:<mint>:launch`
// - `mint:<mint>:migration`
// - `mint:<mint>:prices`
function mintRoom(mint, topic) {
  return `mint:${mint}:${topic}`;
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function safeJson(v) {
  return JSON.stringify(v, (_, val) => {
    if (val && typeof val === "object") {
      if (typeof val.toBase58 === "function") return val.toBase58(); // PublicKey
      if (val.constructor && val.constructor.name === "BN") return val.toString(); // BN
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

// ------------------------------------------------------------
// DB bootstrap (extra tables for indexer)
// ------------------------------------------------------------
function ensureIndexerTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS prices (
      key TEXT PRIMARY KEY,
      price REAL,
      updated_at INTEGER NOT NULL
    );
  `);
}

function setPrice(key, price) {
  db.prepare(
    `INSERT INTO prices (key, price, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(key) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at`
  ).run(key, price, nowSec());
}

function getPrice(key) {
  return db.prepare(`SELECT key, price, updated_at FROM prices WHERE key=?`).get(key);
}

// ------------------------------------------------------------
// SOL price poller (Dexscreener)
// ------------------------------------------------------------
// Dexscreener pair endpoint: /latest/dex/pairs/solana/<pairAddress>
const SOL_PAIR =
  process.env.DEX_SOL_PAIR ||
  "58oqchx4ywmvkdwllzzbi4chocc2fqcuwbkwmihlyqo2"; // common SOL/USDC Raydium pair
const DEX_URL = `https://api.dexscreener.com/latest/dex/pairs/solana/${SOL_PAIR}`;

async function pollSolPriceOnce() {
  const res = await fetch(DEX_URL, { method: "GET" });
  if (!res.ok) throw new Error(`Dexscreener HTTP ${res.status}`);
  const json = await res.json();

  const pair = json?.pairs?.[0];
  const priceUsd = pair?.priceUsd ? Number(pair.priceUsd) : null;
  if (!priceUsd || !Number.isFinite(priceUsd)) throw new Error("No priceUsd in Dexscreener response");

  setPrice("SOL_USD", priceUsd);

  return { key: "SOL_USD", price: priceUsd, updated_at: nowSec() };
}

function startSolPricePoller({ io }) {
  const intervalMs = Number(process.env.SOL_PRICE_INTERVAL_MS || 15_000);

  const loop = async () => {
    try {
      const p = await pollSolPriceOnce();

      // global price stream
      io.to(ROOMS.GLOBAL_PRICES).emit("price", p);
      io.emit("price", p); // also broadcast to anyone not in rooms (optional)

      // NOTE: per-mint price rooms are for later (token USD prices), not SOL.
    } catch (e) {
      console.error("SOL price poll error:", e.message);
    }
  };

  loop();
  setInterval(loop, intervalMs);

  console.log(`SOL price poller started (${intervalMs}ms) using pair ${SOL_PAIR}`);
}

// ------------------------------------------------------------
// Helius WS connection (UPSTREAM)
// ------------------------------------------------------------
const HELIUS_WSS =
  process.env.HELIUS_WSS ||
  `wss://devnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY || ""}`;

if (!HELIUS_WSS.startsWith("wss://")) throw new Error("HELIUS_WSS must be a wss:// url");

let ws;
let pingInterval = null;

function cleanupUpstream() {
  try {
    if (pingInterval) clearInterval(pingInterval);
  } catch {}
  pingInterval = null;
}

function connectUpstream({ io }) {
  ws = new WebSocket(HELIUS_WSS);

  ws.on("open", () => {
    console.log("Connected to Helius WS");

    // keepalive ping (some providers drop idle sockets)
    pingInterval = setInterval(() => {
      try {
        ws.ping();
      } catch {}
    }, 60_000);

    const req = {
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [{ mentions: [PROGRAM_ID] }, { commitment: "confirmed" }],
    };

    ws.send(JSON.stringify(req));
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString("utf8"));
    } catch {
      return;
    }

    // subscription ack
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
    const err = value.err;

    if (!sig) return;
    if (err) return; // ignore failed txs for now

    // de-dupe tx processing
    if (hasSeenTx(sig)) return;
    markTxSeen(sig, ctxSlot);

    // scan for Anchor event payloads
    for (const line of logs) {
      const prefix = "Program data: ";
      if (!line.startsWith(prefix)) continue;

      const b64 = line.slice(prefix.length).trim();

      let buf;
      try {
        buf = Buffer.from(b64, "base64");
      } catch {
        continue;
      }

      let decoded;
      try {
        decoded = coder.events.decode(buf);
      } catch {
        continue;
      }
      if (!decoded) continue;

      const eventName = decoded.name;
      const payload = decoded.data;

      const mint = asStrMaybePk(payload.mint);
      const user = asStrMaybePk(payload.user);

      // -----------------------------
      // Upserts / Normalization
      // -----------------------------
      if (eventName === "LaunchInitialized") {
        upsertLaunch(mint, {
          launch_state: asStrMaybePk(payload.launch_state),
          creator: asStrMaybePk(payload.creator),
          platform: asStrMaybePk(payload.platform),
          core_authority: asStrMaybePk(payload.core_authority),
          total_supply: payload.total_supply?.toString?.() ?? String(payload.total_supply ?? ""),
          sale_supply: payload.sale_supply?.toString?.() ?? String(payload.sale_supply ?? ""),
          lp_supply: payload.lp_supply?.toString?.() ?? String(payload.lp_supply ?? ""),
          state_u8: payload.state_u8 ?? payload.phase ?? null,
          name: null,
          symbol: null,
          metadata_uri: null,
          // optional fields you have in db schema:
          description: null,
          image: null,
          pinata_cid: null,
        });

        // Launch topic emits
        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_LAUNCH).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(`mint:${mint}`).emit("event", evt); // general mint namespace
          io.to(mintRoom(mint, "launch")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
        }
        continue;
      }

      if (eventName === "MetadataInitialized") {
        upsertLaunch(mint, {
          name: payload.name ?? null,
          symbol: payload.symbol ?? null,
          metadata_uri: payload.uri ?? null,
        });

        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_LAUNCH).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(`mint:${mint}`).emit("event", evt);
          io.to(mintRoom(mint, "launch")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
        }
        continue;
      }

      // Trades normalization (your volume/game mode table)
      if (eventName === "CurveActivated") {
        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user: asStrMaybePk(payload.dev),
          side: "DEVBUY",
          phase_u8: null,
          sol_in_gross: payload.sol_in_gross?.toString?.() ?? String(payload.sol_in_gross ?? ""),
          sol_eff_used: null,
          sol_gross: null,
          sol_net: null,
          tokens_out: payload.tokens_out?.toString?.() ?? String(payload.tokens_out ?? ""),
          tokens_in: null,
          creator_fee: null,
          platform_fee: null,
          lp_fee: null,
          tokens_sold_total: null,
          sol_collected_total: null,
          ts_i64: payload.ts?.toString?.() ?? null,
        });

        const evt = { sig, slot: ctxSlot, eventName, mint, user: asStrMaybePk(payload.dev), payload };
        io.to(ROOMS.GLOBAL_TRADES).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(mintRoom(mint, "trades")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
          io.to(`mint:${mint}`).emit("event", evt);
        }
        continue;
      }

      if (eventName === "BuyExecuted") {
        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user,
          side: "BUY",
          phase_u8: payload.phase ?? null,
          sol_in_gross: payload.sol_in_gross?.toString?.() ?? null,
          sol_eff_used: payload.sol_eff_used?.toString?.() ?? null,
          sol_gross: null,
          sol_net: null,
          tokens_out: payload.tokens_out?.toString?.() ?? null,
          tokens_in: null,
          creator_fee: payload.creator_fee?.toString?.() ?? null,
          platform_fee: payload.platform_fee?.toString?.() ?? null,
          lp_fee: payload.lp_fee?.toString?.() ?? null,
          tokens_sold_total: payload.tokens_sold_total?.toString?.() ?? null,
          sol_collected_total: payload.sol_collected_total?.toString?.() ?? null,
          ts_i64: payload.ts?.toString?.() ?? null,
        });

        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_TRADES).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(mintRoom(mint, "trades")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
          io.to(`mint:${mint}`).emit("event", evt);
        }

        // Optional: include SOL price snapshot on trade ticks
        const p = getPrice("SOL_USD");
        if (p?.price) {
          io.to(ROOMS.GLOBAL_PRICES).emit("price", p);
          if (mint) io.to(mintRoom(mint, "prices")).emit("price", p);
        }
        continue;
      }

      if (eventName === "SellExecuted") {
        insertTrade({
          sig,
          slot: ctxSlot,
          block_time: null,
          mint,
          user,
          side: "SELL",
          phase_u8: payload.phase ?? null,
          sol_in_gross: null,
          sol_eff_used: null,
          sol_gross: payload.sol_gross?.toString?.() ?? null,
          sol_net: payload.sol_net?.toString?.() ?? null,
          tokens_out: null,
          tokens_in: payload.tokens_in?.toString?.() ?? null,
          creator_fee: payload.creator_fee?.toString?.() ?? null,
          platform_fee: payload.platform_fee?.toString?.() ?? null,
          lp_fee: payload.lp_fee?.toString?.() ?? null,
          tokens_sold_total: payload.tokens_sold_total?.toString?.() ?? null,
          sol_collected_total: payload.sol_collected_total?.toString?.() ?? null,
          ts_i64: payload.ts?.toString?.() ?? null,
        });

        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_TRADES).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(mintRoom(mint, "trades")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
          io.to(`mint:${mint}`).emit("event", evt);
        }

        const p = getPrice("SOL_USD");
        if (p?.price) {
          io.to(ROOMS.GLOBAL_PRICES).emit("price", p);
          if (mint) io.to(mintRoom(mint, "prices")).emit("price", p);
        }
        continue;
      }

      // Migration topic emits (if your program emits these)
      if (eventName === "MigrationPending" || eventName === "MigratedToCore") {
        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_MIGRATION).emit("event", evt);
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(mintRoom(mint, "migration")).emit("event", evt);
          io.to(mintRoom(mint, "events")).emit("event", evt);
          io.to(`mint:${mint}`).emit("event", evt);
        }
        continue;
      }

      // Fallback: broadcast all events to general channels
      {
        const evt = { sig, slot: ctxSlot, eventName, mint, user, payload };
        io.to(ROOMS.GLOBAL_EVENTS).emit("event", evt);
        if (mint) {
          io.to(mintRoom(mint, "events")).emit("event", evt);
          io.to(`mint:${mint}`).emit("event", evt);
        }
      }
    }
  });

  ws.on("close", () => {
    console.log("Helius WS closed. Reconnecting...");
    cleanupUpstream();
    setTimeout(() => connectUpstream({ io }), 1500);
  });

  ws.on("error", (e) => {
    console.error("Helius WS error:", e.message);
  });
}

// ------------------------------------------------------------
// Public entrypoint
// ------------------------------------------------------------
async function startIndexer({ io }) {
  if (!io) throw new Error("startIndexer requires { io } from server.js");

  ensureIndexerTables();

  // start downstream pollers (SOL price)
  startSolPricePoller({ io });

  // connect upstream (Helius program logs)
  connectUpstream({ io });

  console.log("Indexer started");
  console.log("Rooms:", ROOMS);
}

module.exports = { startIndexer };
