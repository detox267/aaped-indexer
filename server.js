require("dotenv").config();

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const fetch = globalThis.fetch || require("node-fetch");
const sharp = require("sharp");

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
  getCreatorProfile,
  normalizeUsername,
  getUserProfile,
  getUserProfileByUsername,
  isUsernameAvailable,
  upsertUserProfile,
  followUser,
  unfollowUser,
  isFollowing,
  listFollowers,
  listFollowing,
} = require("./db");

const { startIndexer, refreshMintState, simulateBuy } = require("./indexer");

const MEDIA_CACHE_DIR =
  process.env.MEDIA_CACHE_DIR || "/root/aaped-indexer/media-cache";

const AVATAR_CACHE_DIR =
  process.env.AVATAR_CACHE_DIR || path.join(MEDIA_CACHE_DIR, "profile-avatars");

const IPFS_GATEWAY =
  process.env.IPFS_GATEWAY || "https://gateway.pinata.cloud/ipfs";

fs.mkdirSync(MEDIA_CACHE_DIR, { recursive: true });
fs.mkdirSync(AVATAR_CACHE_DIR, { recursive: true });

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

const MEDIA_VARIANTS = {
  thumb: {
    size: Number(process.env.MEDIA_THUMB_SIZE || 96),
    quality: Number(process.env.MEDIA_THUMB_QUALITY || 76),
  },
  card: {
    size: Number(process.env.MEDIA_CARD_SIZE || 160),
    quality: Number(process.env.MEDIA_CARD_QUALITY || 78),
  },
  default: {
    size: Number(process.env.MEDIA_DEFAULT_SIZE || 256),
    quality: Number(process.env.MEDIA_DEFAULT_QUALITY || 80),
  },
};

function mediaVariant(value) {
  const key = String(value || "default").toLowerCase();

  if (key === "thumb") return "thumb";
  if (key === "card") return "card";

  return "default";
}

async function makeTokenImageWebp(buffer, variant = "default") {
  const cfg = MEDIA_VARIANTS[variant] || MEDIA_VARIANTS.default;

  return sharp(buffer, {
    animated: false,
    limitInputPixels: 16_000_000,
  })
    .rotate()
    .resize(cfg.size, cfg.size, {
      fit: "cover",
      position: "center",
      withoutEnlargement: false,
    })
    .webp({
      quality: cfg.quality,
      effort: 4,
    })
    .toBuffer();
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

app.get(["/media/token/:mint", "/media/token/:mint/:variant"], async (req, res) => {
  try {
    const mint = req.params.mint;
    const variant = mediaVariant(req.params.variant);

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

    const key = mediaCacheKey(`${variant}:${imageUrl}`);
    const cachedFile = `${key}.webp`;
    const cachedPath = path.join(MEDIA_CACHE_DIR, cachedFile);

    res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    res.setHeader("Content-Type", "image/webp");
    res.setHeader("X-Content-Type-Options", "nosniff");

    if (fs.existsSync(cachedPath)) {
      return res.sendFile(cachedPath);
    }

    const imageRes = await fetch(imageUrl);

    if (!imageRes.ok) {
      return res.status(502).json({
        error: `Image source failed: ${imageRes.status}`,
      });
    }

    const arrayBuffer = await imageRes.arrayBuffer();
    const inputBuffer = Buffer.from(arrayBuffer);

    let outputBuffer;

    try {
      outputBuffer = await makeTokenImageWebp(inputBuffer, variant);
    } catch (err) {
      console.error(`image compression failed for ${mint}:`, err?.message || err);

      return res.status(415).json({
        error: "Token image could not be processed",
      });
    }

    fs.writeFileSync(cachedPath, outputBuffer);

    return res.send(outputBuffer);
  } catch (err) {
    console.error("media token image error:", err);
    return res.status(500).json({ error: "Failed to load token image" });
  }
});



function cleanWallet(value) {
  return String(value || "").trim();
}

function publicAvatarUrl(req, wallet) {
  const base = `${req.protocol}://${req.get("host")}`;
  return `${base}/media/avatar/${encodeURIComponent(wallet)}`;
}

function parseDataUrlImage(value) {
  const raw = String(value || "");

  const match = raw.match(/^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$/);

  if (match) {
    return {
      mime: match[1],
      buffer: Buffer.from(match[2], "base64"),
    };
  }

  return {
    mime: "image/jpeg",
    buffer: Buffer.from(raw, "base64"),
  };
}

app.get("/media/avatar/:wallet", async (req, res) => {
  try {
    const wallet = cleanWallet(req.params.wallet);
    const file = path.join(AVATAR_CACHE_DIR, `${wallet}.webp`);

    if (!wallet || !fs.existsSync(file)) {
      return res.status(404).json({ error: "Avatar not found" });
    }

    res.setHeader("Cache-Control", "public, max-age=300");
    res.setHeader("Content-Type", "image/webp");

    return res.sendFile(file);
  } catch (err) {
    console.error("avatar media error:", err);
    return res.status(500).json({ error: "Failed to load avatar" });
  }
});

app.get("/profile/:wallet", (req, res) => {
  try {
    const wallet = cleanWallet(req.params.wallet);
    if (!wallet) return res.status(400).json({ error: "Wallet is required" });

    const profile = getUserProfile(wallet);

    return res.json({
      ok: true,
      profile: profile || {
        wallet,
        username: null,
        display_username: null,
        display_name: null,
        bio: null,
        avatar_url: null,
        follower_count: 0,
        following_count: 0,
      },
    });
  } catch (err) {
    console.error("profile get error:", err);
    return res.status(500).json({ error: err.message || "Failed to load profile" });
  }
});

app.get("/u/:username", (req, res) => {
  try {
    const username = normalizeUsername(req.params.username);
    const profile = getUserProfileByUsername(username);

    if (!profile) {
      return res.status(404).json({
        ok: false,
        error: "Username not found",
      });
    }

    return res.json({
      ok: true,
      profile,
    });
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || "Invalid username",
    });
  }
});

app.get("/username/check/:username", (req, res) => {
  try {
    const wallet = cleanWallet(req.query.wallet);
    const result = isUsernameAvailable(req.params.username, wallet);

    return res.json({
      ok: true,
      ...result,
    });
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || "Invalid username",
    });
  }
});

app.post("/profile", (req, res) => {
  try {
    const wallet = cleanWallet(req.body.wallet);
    if (!wallet) return res.status(400).json({ ok: false, error: "Wallet is required" });

    const profile = upsertUserProfile({
      wallet,
      username: req.body.username,
      display_name: req.body.display_name,
      bio: req.body.bio,
    });

    return res.json({
      ok: true,
      profile,
    });
  } catch (err) {
    console.error("profile save error:", err);
    return res.status(400).json({
      ok: false,
      error: err.message || "Failed to save profile",
    });
  }
});

app.post("/profile/avatar", async (req, res) => {
  try {
    const wallet = cleanWallet(req.body.wallet);
    const image = req.body.image || req.body.image_base64 || req.body.data_url;

    if (!wallet) return res.status(400).json({ ok: false, error: "Wallet is required" });
    if (!image) return res.status(400).json({ ok: false, error: "Image is required" });

    const parsed = parseDataUrlImage(image);

    if (!parsed.mime.startsWith("image/")) {
      return res.status(400).json({ ok: false, error: "Avatar must be an image" });
    }

    if (parsed.buffer.length > 3 * 1024 * 1024) {
      return res.status(400).json({ ok: false, error: "Avatar max size is 3MB" });
    }

    const output = await sharp(parsed.buffer)
      .rotate()
      .resize(256, 256, {
        fit: "cover",
        withoutEnlargement: false,
      })
      .webp({ quality: 82 })
      .toBuffer();

    const file = path.join(AVATAR_CACHE_DIR, `${wallet}.webp`);
    fs.writeFileSync(file, output);

    const avatarUrl = publicAvatarUrl(req, wallet);

    const profile = upsertUserProfile({
      wallet,
      avatar_url: avatarUrl,
    });

    return res.json({
      ok: true,
      avatar_url: avatarUrl,
      profile,
    });
  } catch (err) {
    console.error("avatar save error:", err);
    return res.status(400).json({
      ok: false,
      error: err.message || "Failed to save avatar",
    });
  }
});

app.post("/follow", (req, res) => {
  try {
    const follower = cleanWallet(req.body.follower_wallet || req.body.follower);
    const following = cleanWallet(req.body.following_wallet || req.body.following);

    const result = followUser(follower, following);

    return res.json(result);
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || "Failed to follow user",
    });
  }
});

app.post("/unfollow", (req, res) => {
  try {
    const follower = cleanWallet(req.body.follower_wallet || req.body.follower);
    const following = cleanWallet(req.body.following_wallet || req.body.following);

    const result = unfollowUser(follower, following);

    return res.json(result);
  } catch (err) {
    return res.status(400).json({
      ok: false,
      error: err.message || "Failed to unfollow user",
    });
  }
});

app.get("/profile/:wallet/followers", (req, res) => {
  try {
    return res.json({
      ok: true,
      followers: listFollowers(req.params.wallet, req.query.limit || 50),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to load followers" });
  }
});

app.get("/profile/:wallet/following", (req, res) => {
  try {
    return res.json({
      ok: true,
      following: listFollowing(req.params.wallet, req.query.limit || 50),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to load following" });
  }
});

app.get("/profile/:wallet/is-following/:target", (req, res) => {
  try {
    return res.json({
      ok: true,
      follower_wallet: req.params.wallet,
      following_wallet: req.params.target,
      following: isFollowing(req.params.wallet, req.params.target),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to check follow state" });
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

app.get("/creator/:address", (req, res) => {
  try {
    res.json(getCreatorProfile(req.params.address));
  } catch (err) {
    console.error("creator profile error:", err);
    res.status(400).json({
      error: err.message || "Failed to load creator profile",
    });
  }
});

app.get("/creators/:address", (req, res) => {
  try {
    res.json(getCreatorProfile(req.params.address));
  } catch (err) {
    console.error("creator profile error:", err);
    res.status(400).json({
      error: err.message || "Failed to load creator profile",
    });
  }
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


// -----------------------------------------------------------------------------
// King of the Moonz: current-hour top traded tokens
// -----------------------------------------------------------------------------
function kingParseTime(row = {}) {
  const raw =
    row.created_at ??
    row.createdAt ??
    row.timestamp ??
    row.ts ??
    row.time ??
    row.block_time ??
    row.blockTime ??
    row.inserted_at ??
    row.insertedAt;

  if (raw === undefined || raw === null || raw === "") return 0;

  if (typeof raw === "number") {
    return raw > 10_000_000_000 ? raw : raw * 1000;
  }

  const s = String(raw).trim();

  if (/^\d+$/.test(s)) {
    const n = Number(s);
    return n > 10_000_000_000 ? n : n * 1000;
  }

  const parsed = new Date(s).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function kingNum(row = {}, keys = []) {
  for (const key of keys) {
    const v = row[key];
    if (v === undefined || v === null || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}


function kingQuoteAmountUi(row = {}) {
  const quoteAsset = String(
    row.quote_asset ||
    row.quoteAsset ||
    row.quote ||
    "SOL"
  ).toUpperCase();

  const ui = kingNum(row, [
    "quote_amount_ui",
    "quoteAmountUi",
    "sol_amount_ui",
    "solAmountUi",
    "quote_in_ui",
    "quoteInUi",
    "quote_out_ui",
    "quoteOutUi",
  ]);

  if (ui > 0) return Math.abs(ui);

  const raw = Math.abs(kingNum(row, [
    "quote_amount",
    "quoteAmount",
    "sol_amount",
    "solAmount",
    "quote_in",
    "quoteIn",
    "quote_out",
    "quoteOut",
  ]));

  if (raw <= 0) return 0;

  if (quoteAsset === "USDC") {
    return raw / 1_000_000;
  }

  return raw / 1_000_000_000;
}

function getKingTokenMeta(mint) {
  if (!mint) return {};

  try {
    return db.prepare(`
      SELECT *
      FROM token_stats
      WHERE mint = ?
      ORDER BY updated_at DESC
      LIMIT 1
    `).get(mint) || {};
  } catch (_err) {
    return {};
  }
}

function getKingOfMoonzSnapshot() {
  const now = Date.now();
  const hourStart = new Date(now);
  hourStart.setMinutes(0, 0, 0);

  const hourStartMs = hourStart.getTime();
  const hourEndMs = hourStartMs + 3600000;

  let rows = [];

  try {
    rows = db.prepare(`
      SELECT *
      FROM trades
      ORDER BY rowid DESC
      LIMIT 20000
    `).all();
  } catch (err) {
    console.error("[king] trades read failed:", err?.message || err);
    return {
      hour_start: hourStartMs,
      hour_end: hourEndMs,
      top10: [],
      top3: [],
      king: null,
    };
  }

  const byMint = new Map();

  for (const row of rows) {
    const mint = String(row.mint || row.token_mint || row.tokenMint || "").trim();
    if (!mint) continue;

    const ts = kingParseTime(row);
    if (!ts || ts < hourStartMs || ts >= hourEndMs) continue;

    let volumeUsd = kingNum(row, [
      "volume_usd",
      "volumeUsd",
      "amount_usd",
      "amountUsd",
      "value_usd",
      "valueUsd",
      "trade_usd",
      "tradeUsd",
      "usd_value",
      "usdValue",
    ]);

    const quoteUi = kingQuoteAmountUi(row);

    // Fallback if trade rows do not store USD volume.
    if (volumeUsd <= 0) {
      const quoteAsset = String(
        row.quote_asset ||
        row.quoteAsset ||
        row.quote ||
        "SOL"
      ).toUpperCase();

      if (quoteAsset === "USDC") {
        volumeUsd = quoteUi;
      } else {
        const solUsd =
          typeof getPrice === "function"
            ? Number(getPrice("SOL_USD")?.price || 0)
            : 0;

        volumeUsd = solUsd > 0 ? quoteUi * solUsd : quoteUi;
      }
    }

    const side = String(row.side || row.trade_side || row.type || "").toLowerCase();

    const prev = byMint.get(mint) || {
      mint,
      hour_volume_usd: 0,
      hour_volume_quote: 0,
      trades_count: 0,
      buys_count: 0,
      sells_count: 0,
      last_trade_at: 0,
    };

    prev.hour_volume_usd += Number(volumeUsd || 0);
    prev.hour_volume_quote += Number(quoteUi || 0);
    prev.trades_count += 1;

    if (side.includes("buy")) prev.buys_count += 1;
    if (side.includes("sell")) prev.sells_count += 1;

    prev.last_trade_at = Math.max(prev.last_trade_at || 0, ts);
    byMint.set(mint, prev);
  }

  const top10 = [...byMint.values()]
    .sort((a, b) => {
      if (b.hour_volume_usd !== a.hour_volume_usd) {
        return b.hour_volume_usd - a.hour_volume_usd;
      }

      if (b.trades_count !== a.trades_count) {
        return b.trades_count - a.trades_count;
      }

      return b.last_trade_at - a.last_trade_at;
    })
    .slice(0, 10)
    .map((item, index) => {
      const meta = getKingTokenMeta(item.mint);

      return {
        rank: index + 1,
        mint: item.mint,
        name: meta.name || meta.launch_name || meta.token_name || "Unknown Token",
        symbol: meta.symbol || meta.launch_symbol || meta.token_symbol || "",
        image: meta.image || meta.launch_image || meta.token_image || "",
        price_usd: Number(meta.price_usd || meta.priceUsd || 0),
        marketcap_usd: Number(meta.marketcap_usd || meta.marketCapUsd || 0),
        hour_volume_usd: Number(item.hour_volume_usd || 0),
        hour_volume_quote: Number(item.hour_volume_quote || 0),
        trades_count: Number(item.trades_count || 0),
        buys_count: Number(item.buys_count || 0),
        sells_count: Number(item.sells_count || 0),
        last_trade_at: item.last_trade_at || 0,
      };
    });

  return {
    hour_start: hourStartMs,
    hour_end: hourEndMs,
    top10,
    top3: top10.slice(0, 3),
    king: top10[0] || null,
  };
}

app.get("/api/king-of-moonz", (_req, res) => {
  try {
    res.json({
      ok: true,
      ...getKingOfMoonzSnapshot(),
    });
  } catch (err) {
    console.error("[king] api failed:", err?.message || err);
    res.status(500).json({
      ok: false,
      error: "Failed to load King of the Moonz",
    });
  }
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
