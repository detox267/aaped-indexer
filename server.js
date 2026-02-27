// server.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { startIndexer } = require("./indexer");

const app = express();
app.use(express.json({ limit: "10mb" }));

app.get("/health", (req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clients = new Map(); // ws -> { mints:Set<string> }

function broadcast(msg) {
  const mint = msg?.data?.mint?.toBase58 ? msg.data.mint.toBase58() : (msg?.data?.mint || null);

  for (const [ws, state] of clients.entries()) {
    if (ws.readyState !== WebSocket.OPEN) continue;

    // if client joined rooms, filter; otherwise global
    if (state.mints.size > 0) {
      if (!mint || !state.mints.has(mint)) continue;
    }

    ws.send(JSON.stringify(msg));
  }
}

wss.on("connection", (ws) => {
  clients.set(ws, { mints: new Set() });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "join" && msg.mint) {
        clients.get(ws).mints.add(String(msg.mint));
      }
      if (msg.type === "leave" && msg.mint) {
        clients.get(ws).mints.delete(String(msg.mint));
      }
      if (msg.type === "joinAll") {
        clients.get(ws).mints.clear();
      }
    } catch {}
  });

  ws.on("close", () => clients.delete(ws));
});

(async () => {
  // start indexer loop
  await startIndexer({ broadcast });

  const PORT = Number(process.env.PORT || 3000);
  server.listen(PORT, () => {
    console.log(`Indexer WS/API running on port ${PORT}`);
  });
})();
