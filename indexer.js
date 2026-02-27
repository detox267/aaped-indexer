// indexer.js
require("dotenv").config();
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");
const anchor = require("@coral-xyz/anchor");
const Database = require("better-sqlite3");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const bs58 = require("bs58");

const PROGRAM_ID = process.env.PROGRAM_ID;
const IDL_PATH = process.env.IDL_PATH;

if (!PROGRAM_ID) throw new Error("Missing PROGRAM_ID");
if (!IDL_PATH) throw new Error("Missing IDL_PATH");

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

io.on("connection", (socket) => {
  // client can join rooms like: mint:<mintAddress>
  socket.on("join", ({ mint }) => {
    if (mint && typeof mint === "string") socket.join(`mint:${mint}`);
  });
  socket.on("leave", ({ mint }) => {
    if (mint && typeof mint === "string") socket.leave(`mint:${mint}`);
  });
});

server.listen(WS_PORT, () => {
  console.log(`Indexer socket server on :${WS_PORT}`);
});

// --------------------
// Indexer DB
// --------------------
const INDEX_DB = process.env.INDEX_DB || path.join(__dirname, "indexer.db");
const db = new Database(INDEX_DB);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

// Minimal tables — expand later for candles/volume/leaderboards.
db.exec(`
CREATE TABLE IF NOT EXISTS program_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sig TEXT NOT NULL,
  slot INTEGER,
  block_time INTEGER,
  event_name TEXT NOT NULL,
  mint TEXT,
  user TEXT,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS program_events_sig_idx ON program_events(sig);
CREATE INDEX IF NOT EXISTS program_events_mint_idx ON program_events(mint);
CREATE INDEX IF NOT EXISTS program_events_event_idx ON program_events(event_name);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sig TEXT NOT NULL,
  slot INTEGER,
  block_time INTEGER,
  mint TEXT NOT NULL,
  side TEXT NOT NULL, -- BUY / SELL
  user TEXT,
  sol_in_gross INTEGER,
  sol_out_net INTEGER,
  tokens INTEGER,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS trades_mint_idx ON trades(mint);
CREATE INDEX IF NOT EXISTS trades_side_idx ON trades(side);
`);

const insertEvent = db.prepare(`
  INSERT INTO program_events (sig, slot, block_time, event_name, mint, user, payload_json)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertTrade = db.prepare(`
  INSERT INTO trades (sig, slot, block_time, mint, side, user, sol_in_gross, sol_out_net, tokens)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

// --------------------
// Helius WS connection
// --------------------
const HELIUS_WSS =
  process.env.HELIUS_WSS ||
  `wss://devnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY || ""}`;

if (!HELIUS_WSS.includes("wss://")) {
  throw new Error("HELIUS_WSS must be a wss:// url");
}

let ws;
let subId = null;
let pingInterval = null;

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function connect() {
  ws = new WebSocket(HELIUS_WSS);

  ws.on("open", () => {
    console.log("Connected to Helius WS");

    // Keepalive ping every minute (Helius recommends pings to avoid inactivity drop)
    pingInterval = setInterval(() => {
      try {
        ws.ping();
      } catch {}
    }, 60_000);

    // logsSubscribe mentions: [PROGRAM_ID]
    const req = {
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [
        { mentions: [PROGRAM_ID] },
        { commitment: "confirmed" },
      ],
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

    // subscription response
    if (msg.id === 1 && msg.result) {
      subId = msg.result;
      console.log("logsSubscribe subId:", subId);
      return;
    }

    // notifications
    if (msg.method === "logsNotification") {
      const value = msg.params?.result?.value;
      if (!value) return;

      const sig = value.signature;
      const slot = msg.params?.result?.context?.slot ?? null;
      const logs = value.logs || [];
      const err = value.err;

      // You may want to ignore failed txs, or store them separately:
      if (err) return;

      // Parse Anchor events from log lines
      // Anchor emits: "Program data: <base64>" for event payloads.
      // We'll scan logs for "Program data: " and decode.
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

        // Decode using IDL event layout
        let decoded;
        try {
          decoded = coder.events.decode(buf);
        } catch {
          continue;
        }
        if (!decoded) continue;

        const eventName = decoded.name;
        const payload = decoded.data;

        const mint = payload.mint ? payload.mint.toBase58?.() : payload.mint?.toString?.();
        const user = payload.user ? payload.user.toBase58?.() : payload.user?.toString?.();

        // Persist raw event
        insertEvent.run(
          sig,
          slot,
          null, // fill block_time later if you want (getBlockTime or getTransaction)
          eventName,
          mint || null,
          user || null,
          JSON.stringify(payload, (_, v) => {
            // BN / PublicKey to string
            if (v && v.toBase58) return v.toBase58();
            if (v && v.toString && typeof v === "object" && v.constructor?.name === "BN") return v.toString();
            return v;
          })
        );

        // Normalize trade rows for your “volume game mode”
        if (eventName === "BuyExecuted") {
          insertTrade.run(
            sig,
            slot,
            null,
            mint,
            "BUY",
            user,
            Number(payload.sol_in_gross || 0),
            null,
            Number(payload.tokens_out || 0)
          );
        }

        if (eventName === "SellExecuted") {
          insertTrade.run(
            sig,
            slot,
            null,
            mint,
            "SELL",
            user,
            null,
            Number(payload.sol_net || 0),
            Number(payload.tokens_in || 0)
          );
        }

        // Broadcast to website clients
        io.emit("event", { sig, slot, eventName, mint, user, payload });

        if (mint) {
          io.to(`mint:${mint}`).emit("event", { sig, slot, eventName, mint, user, payload });
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

function cleanup() {
  try {
    if (pingInterval) clearInterval(pingInterval);
  } catch {}
  pingInterval = null;
  subId = null;
}

connect();
