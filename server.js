require("dotenv").config();

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const {
  db,
  getToken,
  listTokens,
  getTrades,
  getCandles,
  getPrice,
} = require("./db");
const { startIndexer, refreshMintState, simulateBuy } = require("./indexer");

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "10mb" }));

const allowedOrigins = (process.env.CORS_ORIGINS || "https://moonz-frontend.vercel.app,https://aaped.fun,https://www.aaped.fun,http://localhost:5173,http://localhost:3000")
  .split(",")
  .map((x) => x.trim())
  .filter(Boolean);

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.get("/health", (req, res) => {
  const sol = getPrice("SOL_USD");
  res.json({ ok: true, service: "moonz-indexer", solPrice: sol?.price || null });
});

app.get("/prices/sol", (req, res) => {
  res.json(getPrice("SOL_USD") || { key: "SOL_USD", price: null, updated_at: null });
});

app.get("/tokens", (req, res) => {
  try {
    const rows = listTokens({
      limit: req.query.limit,
      offset: req.query.offset,
      phase: req.query.phase || null,
    });
    res.json(rows);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/tokens/:mint", async (req, res) => {
  try {
    let token = getToken(req.params.mint);

    if (req.query.refresh === "true" || !token) {
      await refreshMintState(req.params.mint).catch(() => null);
      token = getToken(req.params.mint);
    }

    if (!token) return res.status(404).json({ error: "Token not found" });
    res.json(token);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/token/:mint", async (req, res) => {
  try {
    let token = getToken(req.params.mint);

    if (req.query.refresh === "true" || !token) {
      await refreshMintState(req.params.mint).catch(() => null);
      token = getToken(req.params.mint);
    }

    if (!token) return res.status(404).json({ error: "Token not found" });
    res.json(token);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/tokens/:mint/trades", (req, res) => {
  try {
    res.json(getTrades({ mint: req.params.mint, limit: req.query.limit, offset: req.query.offset }));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/token/:mint/trades", (req, res) => {
  try {
    res.json(getTrades({ mint: req.params.mint, limit: req.query.limit, offset: req.query.offset }));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/trades", (req, res) => {
  try {
    res.json(getTrades({ limit: req.query.limit, offset: req.query.offset }));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/live-trades", (req, res) => {
  try {
    res.json(getTrades({ limit: req.query.limit || 50, offset: req.query.offset }));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/tokens/:mint/candles", (req, res) => {
  try {
    const rows = getCandles({
      mint: req.params.mint,
      interval: String(req.query.interval || "1m"),
      limit: req.query.limit || 500,
      since: req.query.since ? Number(req.query.since) : null,
    });
    res.json(rows);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/candles", (req, res) => {
  try {
    if (!req.query.mint) throw new Error("mint required");
    const rows = getCandles({
      mint: req.query.mint,
      interval: String(req.query.interval || "1m"),
      limit: req.query.limit || 500,
      since: req.query.since ? Number(req.query.since) : null,
    });
    res.json(rows);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/simulate-buy", async (req, res) => {
  try {
    if (!req.query.mint) throw new Error("mint required");
    const amount = Number(req.query.amount || req.query.sol || req.query.usdc || 0);
    if (!amount || !Number.isFinite(amount) || amount <= 0) throw new Error("amount required");
    res.json(await simulateBuy(req.query.mint, amount));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.post("/admin/refresh/:mint", async (req, res) => {
  try {
    const result = await refreshMintState(req.params.mint, io);
    if (!result) return res.status(404).json({ error: "Token state not found" });
    res.json(result.stats || result);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/debug/db-counts", (req, res) => {
  const tables = ["launches", "token_stats", "trades", "events", "candles_1m", "tx_seen"];
  const out = {};
  for (const table of tables) {
    out[table] = db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n;
  }
  res.json(out);
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: allowedOrigins.length ? allowedOrigins : "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
});

io.on("connection", (socket) => {
  socket.on("join", (msg = {}) => {
    if (msg.room && typeof msg.room === "string") {
      socket.join(msg.room);
      socket.emit("joined", { room: msg.room });
      return;
    }

    const mint = typeof msg.mint === "string" ? msg.mint : null;
    const channel = typeof msg.channel === "string" ? msg.channel : null;
    if (!mint) return;

    if (!channel) {
      socket.join(`mint:${mint}`);
      socket.emit("joined", { room: `mint:${mint}` });
      return;
    }

    const room = `mint:${mint}:${channel}`;
    socket.join(room);
    socket.emit("joined", { room });
  });

  socket.on("leave", (msg = {}) => {
    if (msg.room && typeof msg.room === "string") {
      socket.leave(msg.room);
      socket.emit("left", { room: msg.room });
      return;
    }

    const mint = typeof msg.mint === "string" ? msg.mint : null;
    const channel = typeof msg.channel === "string" ? msg.channel : null;
    if (!mint) return;

    const room = channel ? `mint:${mint}:${channel}` : `mint:${mint}`;
    socket.leave(room);
    socket.emit("left", { room });
  });

  socket.on("joinMint", ({ mint } = {}) => {
    if (!mint) return;
    socket.join(`mint:${mint}`);
    socket.emit("joined", { room: `mint:${mint}` });
  });

  socket.on("leaveMint", ({ mint } = {}) => {
    if (!mint) return;
    socket.leave(`mint:${mint}`);
    socket.emit("left", { room: `mint:${mint}` });
  });

  socket.on("joinGlobals", () => {
    const rooms = ["global:trades", "global:events", "global:prices"];
    for (const room of rooms) socket.join(room);
    socket.emit("joinedGlobals", { rooms });
  });

  socket.on("joinRoom", ({ room } = {}) => {
    if (!room) return;
    socket.join(room);
    socket.emit("joined", { room });
  });
});

app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ error: "Internal server error" });
});

(async () => {
  await startIndexer({ io });

  const PORT = Number(process.env.PORT || process.env.WS_PORT || 3010);
  server.listen(PORT, () => {
    console.log(`Moonz indexer HTTP+Socket server listening on :${PORT}`);
  });
})().catch((err) => {
  console.error("Fatal indexer server error:", err);
  process.exit(1);
});
