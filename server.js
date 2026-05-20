require("dotenv").config();

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const fetch = globalThis.fetch || require("node-fetch");

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
  getHolderSummary,
  getTopHolders,
} = require("./db");

const { startIndexer, refreshMintState, simulateBuy } = require("./indexer");

const MEDIA_CACHE_DIR =
  process.env.MEDIA_CACHE_DIR || "/root/aaped-indexer/media-cache";

const IPFS_GATEWAY =
  process.env.IPFS_GATEWAY || "https://gateway.pinata.cloud/ipfs";

fs.mkdirSync(MEDIA_CACHE_DIR, { recursive: true });

function ipfsToHttp(uri) {
  if (!uri) return null;

  const value = String(uri);

  if (value.startsWith("ipfs://")) {
    const cidPath = value.replace("ipfs://", "").replace(/^\/+/, "");
    return `${IPFS_GATEWAY.replace(/\/+$/, "")}/${cidPath}`;
  }

  return value;
}

function safeImageExt(contentType = "") {
  if (contentType.includes("png")) return ".png";
  if (contentType.includes("webp")) return ".webp";
  if (contentType.includes("gif")) return ".gif";
  if (contentType.includes("svg")) return ".svg";
  return ".jpg";
}

function mediaCacheKey(input) {
  return crypto.createHash("sha256").update(String(input)).digest("hex");
}

const app = express();

app.disable("x-powered-by");
app.use(express.json({ limit: "10mb" }));

const allowedOrigins = (
  process.env.CORS_ORIGINS ||
  "https://moonz.fun,https://www.moonz.fun,https://moonz-frontend.vercel.app,http://localhost:5173,http://localhost:3000"
)
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

app.get("/media/token/:mint", async (req, res) => {
  try {
    const mint = req.params.mint;

    const row = db
      .prepare(`
        SELECT 
          ts.image AS stats_image,
          ts.metadata_uri AS stats_metadata_uri,
          l.image AS launch_image,
          l.metadata_uri AS launch_metadata_uri
        FROM token_stats ts
        LEFT JOIN launches l ON l.mint = ts.mint
        WHERE ts.mint = ?
      `)
      .get(mint);

    if (!row) {
      return res.status(404).json({ error: "Token not found" });
    }

    let imageUri = row.launch_image || row.stats_image || null;

    if (!imageUri) {
      const metadataUri =
        row.launch_metadata_uri || row.stats_metadata_uri || null;

      const metadataUrl = ipfsToHttp(metadataUri);

      if (metadataUrl) {
        const metadataRes = await fetch(metadataUrl);

        if (metadataRes.ok) {
          const metadata = await metadataRes.json().catch(() => null);
          imageUri = metadata?.image || null;
        }
      }
    }

    if (!imageUri) {
      return res.status(404).json({ error: "Token image not found" });
    }

    const imageUrl = ipfsToHttp(imageUri);

    if (!imageUrl) {
      return res.status(404).json({ error: "Invalid token image" });
    }

    const key = mediaCacheKey(imageUrl);

    const cachedFile = fs
      .readdirSync(MEDIA_CACHE_DIR)
      .find((file) => file.startsWith(`${key}.`));

    if (cachedFile) {
      const cachedPath = path.join(MEDIA_CACHE_DIR, cachedFile);

      res.setHeader("Cache-Control", "public, max-age=31536000, immutable");

      return res.sendFile(cachedPath);
    }

    const imageRes = await fetch(imageUrl);

    if (!imageRes.ok) {
      return res.status(502).json({
        error: `Image source failed: ${imageRes.status}`,
      });
    }

    const contentType = imageRes.headers.get("content-type") || "image/jpeg";
    const ext = safeImageExt(contentType);
    const filePath = path.join(MEDIA_CACHE_DIR, `${key}${ext}`);

    const arrayBuffer = await imageRes.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    fs.writeFileSync(filePath, buffer);

    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "public, max-age=31536000, immutable");

    return res.send(buffer);
  } catch (err) {
    console.error("media token image error:", err);
    return res.status(500).json({ error: "Failed to load token image" });
  }
});

app.get("/health", (req, res) => {
  const sol = getPrice("SOL_USD");

  res.json({
    ok: true,
    service: "moonz-indexer",
    solPrice: sol?.price || null,
  });
});

app.get("/prices/sol", (req, res) => {
  res.json(
    getPrice("SOL_USD") || {
      key: "SOL_USD",
      price: null,
      updated_at: null,
    }
  );
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
      await refreshMintState(req.params.mint).catch((err) => {
        console.error("refreshMintState failed:", err?.message || err);
      });

      token = getToken(req.params.mint);
    }

    if (!token) {
      return res.status(404).json({ error: "Token not found" });
    }

    res.json(token);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/token/:mint", async (req, res) => {
  try {
    let token = getToken(req.params.mint);

    if (req.query.refresh === "true" || !token) {
      await refreshMintState(req.params.mint).catch((err) => {
        console.error("refreshMintState failed:", err?.message || err);
      });

      token = getToken(req.params.mint);
    }

    if (!token) {
      return res.status(404).json({ error: "Token not found" });
    }

    res.json(token);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/tokens/:mint/holders", (req, res) => {
  try {
    res.json(getHolderSummary(req.params.mint));
  } catch (err) {
    res.status(400).json({
      error: err.message || "Failed to load holders",
    });
  }
});

app.get("/token/:mint/holders", (req, res) => {
  try {
    res.json(getHolderSummary(req.params.mint));
  } catch (err) {
    res.status(400).json({
      error: err.message || "Failed to load holders",
    });
  }
});

app.get("/tokens/:mint/holders/top", (req, res) => {
  try {
    res.json({
      mint: req.params.mint,
      holders: getTopHolders({
        mint: req.params.mint,
        limit: req.query.limit,
        offset: req.query.offset,
      }),
    });
  } catch (err) {
    res.status(400).json({
      error: err.message || "Failed to load top holders",
    });
  }
});

app.get("/token/:mint/holders/top", (req, res) => {
  try {
    res.json({
      mint: req.params.mint,
      holders: getTopHolders({
        mint: req.params.mint,
        limit: req.query.limit,
        offset: req.query.offset,
      }),
    });
  } catch (err) {
    res.status(400).json({
      error: err.message || "Failed to load top holders",
    });
  }
});

function safeLimit(value, fallback = 50, max = 200) {
  const n = Number(value || fallback);

  if (!Number.isFinite(n) || n <= 0) return fallback;

  return Math.min(max, Math.floor(n));
}

function safeOffset(value) {
  const n = Number(value || 0);

  if (!Number.isFinite(n) || n < 0) return 0;

  return Math.floor(n);
}

function getTradesWithTokenMeta({ mint = null, limit = 50, offset = 0 } = {}) {
  const safeMint = mint || null;
  const safeTradeLimit = safeLimit(limit, 50, 200);
  const safeTradeOffset = safeOffset(offset);

  if (safeMint) {
    return db
      .prepare(`
        SELECT
          t.*,

          COALESCE(ts.symbol, l.symbol) AS symbol,
          COALESCE(ts.name, l.name) AS name,
          COALESCE(ts.image, l.image) AS image,
          COALESCE(ts.metadata_uri, l.metadata_uri) AS metadata_uri,

          COALESCE(ts.quote_asset, t.quote_asset, 'SOL') AS quote_asset

        FROM trades t
        LEFT JOIN token_stats ts ON ts.mint = t.mint
        LEFT JOIN launches l ON l.mint = t.mint
        WHERE t.mint = ?
        ORDER BY t.created_at DESC
        LIMIT ? OFFSET ?
      `)
      .all(safeMint, safeTradeLimit, safeTradeOffset);
  }

  return db
    .prepare(`
      SELECT
        t.*,

        COALESCE(ts.symbol, l.symbol) AS symbol,
        COALESCE(ts.name, l.name) AS name,
        COALESCE(ts.image, l.image) AS image,
        COALESCE(ts.metadata_uri, l.metadata_uri) AS metadata_uri,

        COALESCE(ts.quote_asset, t.quote_asset, 'SOL') AS quote_asset

      FROM trades t
      LEFT JOIN token_stats ts ON ts.mint = t.mint
      LEFT JOIN launches l ON l.mint = t.mint
      ORDER BY t.created_at DESC
      LIMIT ? OFFSET ?
    `)
    .all(safeTradeLimit, safeTradeOffset);
}

app.get("/tokens/:mint/trades", (req, res) => {
  try {
    res.json(
      getTradesWithTokenMeta({
        mint: req.params.mint,
        limit: req.query.limit,
        offset: req.query.offset,
      })
    );
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/token/:mint/trades", (req, res) => {
  try {
    res.json(
      getTradesWithTokenMeta({
        mint: req.params.mint,
        limit: req.query.limit,
        offset: req.query.offset,
      })
    );
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/trades", (req, res) => {
  try {
    res.json(
      getTradesWithTokenMeta({
        limit: req.query.limit,
        offset: req.query.offset,
      })
    );
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/live-trades", (req, res) => {
  try {
    res.json(
      getTradesWithTokenMeta({
        limit: req.query.limit || 50,
        offset: req.query.offset,
      })
    );
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
    if (!req.query.mint) {
      throw new Error("mint required");
    }

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
    if (!req.query.mint) {
      throw new Error("mint required");
    }

    const amount = Number(req.query.amount || req.query.sol || req.query.usdc || 0);

    if (!amount || !Number.isFinite(amount) || amount <= 0) {
      throw new Error("amount required");
    }

    res.json(await simulateBuy(req.query.mint, amount));
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.post("/admin/refresh/:mint", async (req, res) => {
  try {
    const result = await refreshMintState(req.params.mint, io);

    if (!result) {
      return res.status(404).json({ error: "Token state not found" });
    }

    res.json(result.stats || result);
  } catch (err) {
    res.status(400).json({ error: err.message || String(err) });
  }
});

app.get("/debug/db-counts", (req, res) => {
  const tables = [
  "launches",
  "token_stats",
  "token_holders",
  "trades",
  "events",
  "candles_1m",
  "tx_seen",
  ];

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

    for (const room of rooms) {
      socket.join(room);
    }

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
