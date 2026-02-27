require("dotenv").config();

const fs = require("fs");
const WebSocket = require("ws");
const anchor = require("@coral-xyz/anchor");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

// your local db helpers (must export: db, hasSeenTx, markTxSeen, upsertLaunch, insertTrade)
const { db, hasSeenTx, markTxSeen, upsertLaunch, insertTrade } = require("./db");

// fetch compat (Node 18+ has global fetch; older needs node-fetch)
let fetchFn = global.fetch;
if (!fetchFn) {
  fetchFn = (...args) => import("node-fetch").then(({ default: f }) => f(...args));
}

const PROGRAM_ID = process.env.PROGRAM_ID;
const IDL_PATH = process.env.IDL_PATH;

if (!PROGRAM_ID) throw new Error("Missing PROGRAM_ID");
if (!IDL_PATH) throw new Error("Missing IDL_PATH");
if (!fs.existsSync(IDL_PATH)) throw new Error(`IDL_PATH not found: ${IDL_PATH}`);

const idl = JSON.parse(fs.readFileSync(IDL_PATH, "utf8"));
const coder = new anchor.BorshCoder(idl);

// --------------------
// Web UI socket server
// --------------------
const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

const WS_PORT = Number(process.env.WS_PORT || 3010);

// --------------------
// Rooms / subrooms
// --------------------
const VALID_TOPICS = new Set(["trades", "launch", "migration", "meta", "prices", "events"]);

function mintRoom(mint, topic) {
  return `mint:${mint}:${topic}`;
}
function globalRoom(topic) {
  return `global:${topic}`;
}

io.on("connection", (socket) => {
  // New join format:
  // socket.emit("join", { mint: "<mint>" (optional), topic: "trades|launch|migration|meta|prices|events" })
  socket.on("join", ({ mint, topic }) => {
    try {
      if (!topic || !VALID_TOPICS.has(topic)) return;

      // global room if no mint passed
      if (!mint) return socket.join(globalRoom(topic));

      if (typeof mint === "string") socket.join(mintRoom(mint, topic));
    } catch {}
  });

  socket.on("leave", ({ mint, topic }) => {
    try {
      if (!topic || !VALID_TOPICS.has(topic)) return;

      if (!mint) return socket.leave(globalRoom(topic));

      if (typeof mint === "string") socket.leave(mintRoom(mint, topic));
    } catch {}
  });

  // backwards compatibility for your old UI:
  // socket.emit("join", { mint: "<mint>" }) -> joins mint:<mint>:events
  socket.on("joinLegacyMint", ({ mint }) => {
    if (mint && typeof mint === "string") socket.join(mintRoom(mint, "events"));
  });
});

server.listen(WS_PORT, () => {
  console.log(`Indexer socket server on :${WS_PORT}`);
});

// quick health
app.get("/health", (req, res) => res.json({ ok: true }));

// --------------------
// DB bootstrap (extra tables for indexer)
// --------------------
db.exec(`
CREATE TABLE IF NOT EXISTS prices (
  key TEXT PRIMARY KEY,
  price REAL,
  updated_at INTEGER NOT NULL
);
`);

// --------------------
// SOL price poller (Dexscreener)
// --------------------
// Raydium SOL/USDC pair used by Dexscreener UI commonly:
// https://dexscreener.com/solana/58oqchx4ywmvkdwllzzbi4chocc2fqcuwbkwmihlyqo2
const SOL_PAIR =
  process.env.DEX_SOL_PAIR || "58oqchx4ywmvkdwllzzbi4chocc2fqcuwbkwmihlyqo2";
const DEX_URL = `https://api.dexscreener.com/latest/dex/pairs/solana/${SOL_PAIR}`;

function nowSec() {
  return Math.floor(Date.now() / 1000);
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

async function pollSolPriceOnce() {
  const res = await fetchFn(DEX_URL, { method: "GET" });
  if (!res.ok) throw new Error(`Dexscreener HTTP ${res.status}`);
  const json = await res.json();

  const pair = json?.pairs?.[0];
  const priceUsd = pair?.priceUsd ? Number(pair.priceUsd) : null;
  if (!priceUsd || !Number.isFinite(priceUsd)) {
    throw new Error("No priceUsd in Dexscreener response");
  }

  setPrice("SOL_USD", priceUsd);

  // broadcast to UI (global prices)
  const payload = { key: "SOL_USD", price: priceUsd, updated_at: nowSec() };
  io.to(globalRoom("prices")).emit("price", payload);

  // optional: also emit a raw global event feed if you want
  io.to(globalRoom("events")).emit("price", payload);
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

  console.log(`SOL price poller started (${intervalMs}ms) using pair ${SOL_PAIR}`);
}

startSolPricePoller();

// --------------------
// Anchor event helpers
// --------------------
function safeJson(v) {
  return JSON.stringify(v, (_, val) => {
    if (val && typeof val === "object") {
      // PublicKey
      if (typeof val.toBase58 === "function") return val.toBase58();
      // BN
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

function topicForEvent(eventName) {
  // routes events into your subrooms
  if (eventName === "BuyExecuted" || eventName === "SellExecuted" || eventName === "CurveActivated")
    return "trades";

  if (eventName === "LaunchInitialized") return "launch";
  if (eventName === "MetadataInitialized") return "meta";

  if (eventName === "MigrationPending" || eventName === "MigratedToCore") return "migration";

  // everything else still goes to events
  return "events";
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

    // keepalive ping
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
    if (err) return;

    // de-dupe tx processing
    if (hasSeenTx(sig)) return;
    markTxSeen(sig, ctxSlot);

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

      // -----------------------------------------
      // 1) launches table upserts
      // -----------------------------------------
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
        });
      }

      if (eventName === "MetadataInitialized") {
        upsertLaunch(mint, {
          name: payload.name ?? null,
          symbol: payload.symbol ?? null,
          metadata_uri: payload.uri ?? null,
        });
      }

      // -----------------------------------------
      // 2) trades inserts
      // -----------------------------------------
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
      }

      // -----------------------------------------
      // 3) broadcast to rooms
      // -----------------------------------------
      const topic = topicForEvent(eventName);

      const packet = { sig, slot: ctxSlot, eventName, mint, user, payload };

      // global topic room
      io.to(globalRoom(topic)).emit("event", packet);

      // always also push into global events feed
      io.to(globalRoom("events")).emit("event", packet);

      // mint topic room + mint events room
      if (mint) {
        io.to(mintRoom(mint, topic)).emit("event", packet);
        io.to(mintRoom(mint, "events")).emit("event", packet);

        // backwards compatibility: if any old clients still join `mint:<mint>` (no topic)
        // you can optionally support this by ALSO emitting to that legacy name:
        io.to(`mint:${mint}`).emit("event", packet);
      }

      // when trades happen, also push latest SOL price snapshot into those same scopes
      if (topic === "trades") {
        const p = getPrice("SOL_USD");
        if (p?.price) {
          io.to(globalRoom("prices")).emit("price", p);
          if (mint) io.to(mintRoom(mint, "prices")).emit("price", p);
        }
      }
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
