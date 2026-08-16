const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
require("dotenv").config();

let createClient = null;
try {
  ({ createClient } = require("redis"));
} catch {
  createClient = null;
}

const LIVE_EVENT_CHANNEL = process.env.LIVE_EVENT_CHANNEL || "moonz:events";
const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

let notificationRedisClient = null;
let notificationRedisConnecting = null;

async function getNotificationRedisClient() {
  if (!createClient) return null;

  if (notificationRedisClient?.isOpen) {
    return notificationRedisClient;
  }

  if (notificationRedisConnecting) {
    return notificationRedisConnecting;
  }

  notificationRedisClient = createClient({ url: REDIS_URL });
  notificationRedisClient.on("error", (err) => {
    console.warn("[notifications-redis] publish error:", err?.message || err);
  });

  notificationRedisConnecting = notificationRedisClient
    .connect()
    .then(() => notificationRedisClient)
    .catch((err) => {
      console.warn("[notifications-redis] connect failed:", err?.message || err);
      notificationRedisClient = null;
      return null;
    })
    .finally(() => {
      notificationRedisConnecting = null;
    });

  return notificationRedisConnecting;
}

function publishNotificationCreated(notification) {
  if (!notification?.recipient_wallet) return;

  const event = {
    type: "notification.created",
    recipient_wallet: notification.recipient_wallet,
    notification,
  };

  getNotificationRedisClient()
    .then((client) => {
      if (!client) return null;
      return client.publish(LIVE_EVENT_CHANNEL, JSON.stringify(event));
    })
    .catch((err) => {
      console.warn("[notifications-redis] publish failed:", err?.message || err);
    });
}


const TOKENS_DB = process.env.TOKENS_DB || path.join(__dirname, "tokens.db");
fs.mkdirSync(path.dirname(TOKENS_DB), { recursive: true });

const db = new Database(TOKENS_DB);

db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("foreign_keys = ON");
db.pragma("cache_size = -200000");
db.pragma("temp_store = MEMORY");
db.pragma("busy_timeout = 5000");



// User profile/social tables.
db.exec(`
CREATE TABLE IF NOT EXISTS user_profiles (
  wallet TEXT PRIMARY KEY,
  username TEXT,
  username_lc TEXT,
  display_name TEXT,
  bio TEXT,
  avatar_url TEXT,
  avatar_updated_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS user_profiles_username_lc_unique
ON user_profiles(username_lc)
WHERE username_lc IS NOT NULL AND username_lc != '';

CREATE INDEX IF NOT EXISTS user_profiles_updated_idx
ON user_profiles(updated_at);

CREATE TABLE IF NOT EXISTS user_follows (
  follower_wallet TEXT NOT NULL,
  following_wallet TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (follower_wallet, following_wallet),
  CHECK (follower_wallet != following_wallet)
);

CREATE INDEX IF NOT EXISTS user_follows_follower_idx
ON user_follows(follower_wallet, created_at DESC);

CREATE INDEX IF NOT EXISTS user_follows_following_idx
ON user_follows(following_wallet, created_at DESC);
`);


function addColumnIfMissing(table, column, ddl) {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all();

  if (!rows.some((row) => row.name === column)) {
    db.prepare(ddl).run();
  }
}

function runFollowSecurityMigration() {
  const tables = db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name = 'user_follows'
  `).all();

  if (!tables.length) {
    return;
  }

  addColumnIfMissing(
    "user_follows",
    "verified_at",
    "ALTER TABLE user_follows ADD COLUMN verified_at INTEGER"
  );

  addColumnIfMissing(
    "user_follows",
    "verified_reason",
    "ALTER TABLE user_follows ADD COLUMN verified_reason TEXT"
  );

  addColumnIfMissing(
    "user_follows",
    "follower_sol_lamports",
    "ALTER TABLE user_follows ADD COLUMN follower_sol_lamports TEXT"
  );

  addColumnIfMissing(
    "user_follows",
    "follower_profile_created_at",
    "ALTER TABLE user_follows ADD COLUMN follower_profile_created_at INTEGER"
  );
}

runFollowSecurityMigration();










// User notification tables.
db.exec(`
CREATE TABLE IF NOT EXISTS user_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recipient_wallet TEXT NOT NULL,
  actor_wallet TEXT,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT,
  mint TEXT,
  data_json TEXT,
  unique_key TEXT,
  read_at INTEGER,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS user_notifications_unique_key_idx
ON user_notifications(recipient_wallet, unique_key)
WHERE unique_key IS NOT NULL AND unique_key != '';

CREATE INDEX IF NOT EXISTS user_notifications_recipient_idx
ON user_notifications(recipient_wallet, read_at, created_at DESC);

CREATE INDEX IF NOT EXISTS user_notifications_type_idx
ON user_notifications(type, created_at DESC);

CREATE INDEX IF NOT EXISTS user_notifications_mint_idx
ON user_notifications(mint);
`);



// One-time launch notification marker.
db.exec(`
CREATE TABLE IF NOT EXISTS creator_launch_notifications_sent (
  mint TEXT PRIMARY KEY,
  creator TEXT NOT NULL,
  followers_count INTEGER DEFAULT 0,
  inserted_count INTEGER DEFAULT 0,
  sent_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS creator_launch_notifications_creator_idx
ON creator_launch_notifications_sent(creator, sent_at DESC);
`);

function now() {
  return Math.floor(Date.now() / 1000);
}

db.exec(`
CREATE TABLE IF NOT EXISTS tx_seen (
  sig TEXT PRIMARY KEY,
  slot INTEGER,
  first_seen_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
  key TEXT PRIMARY KEY,
  price REAL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS launches (
  mint TEXT PRIMARY KEY,
  launch_state TEXT,
  launch_escrow TEXT,
  escrow_sol_vault TEXT,
  sale_vault TEXT,
  lp_vault TEXT,
  treasury_wsol_vault TEXT,
  treasury_usdc_vault TEXT,
  metadata TEXT,

  creator TEXT,
  platform TEXT,
  core_authority TEXT,

  name TEXT,
  symbol TEXT,
  description TEXT,
  image TEXT,
  metadata_uri TEXT,
  pinata_cid TEXT,
  extensions_json TEXT,
  website TEXT,
  twitter TEXT,
  telegram TEXT,

  total_supply TEXT,
  sale_supply TEXT,
  lp_supply TEXT,
  decimals INTEGER DEFAULT 6,

  state_u8 INTEGER,
  phase TEXT,
  quote_asset_u8 INTEGER,
  quote_asset TEXT,
  pending_quote_asset_u8 INTEGER,
  pending_quote_asset TEXT,

  tokens_sold TEXT,
  sol_collected TEXT,
  amm_initial_sol TEXT,
  amm_initial_tok TEXT,
  migrated_at INTEGER,
  launch_ts INTEGER,
  last_trade_ts INTEGER,
  last_pool_switch_ts INTEGER,
  switch_started_at INTEGER,
  dev_buy_done INTEGER DEFAULT 0,
  escrow_settled INTEGER DEFAULT 0,

  sale_vault_amount TEXT,
  lp_vault_amount TEXT,
  treasury_wsol_amount TEXT,
  treasury_usdc_amount TEXT,

  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS launches_phase_idx ON launches(phase);
CREATE INDEX IF NOT EXISTS launches_creator_idx ON launches(creator);
CREATE INDEX IF NOT EXISTS launches_updated_idx ON launches(updated_at);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sig TEXT NOT NULL,
  slot INTEGER,
  block_time INTEGER,
  log_index INTEGER DEFAULT 0,

  mint TEXT NOT NULL,
  user TEXT,
  side TEXT NOT NULL,
  phase TEXT,
  phase_u8 INTEGER,
  quote_asset TEXT,
  quote_asset_u8 INTEGER,

  input_amount TEXT,
  input_mint TEXT,
  output_amount TEXT,
  output_mint TEXT,

  quote_amount TEXT,
  token_amount TEXT,
  price_quote REAL,
  price_sol REAL,
  price_usd REAL,

  creator_fee TEXT,
  platform_fee TEXT,
  lp_fee TEXT,
  tokens_sold_total TEXT,
  sol_collected_total TEXT,

  raw_event_name TEXT,
  raw_event_json TEXT,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS trades_sig_log_idx ON trades(sig, log_index, raw_event_name);
CREATE INDEX IF NOT EXISTS trades_mint_id_idx ON trades(mint, id);
CREATE INDEX IF NOT EXISTS trades_mint_created_idx ON trades(mint, created_at);
CREATE INDEX IF NOT EXISTS trades_user_idx ON trades(user);
CREATE INDEX IF NOT EXISTS trades_side_idx ON trades(side);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sig TEXT NOT NULL,
  slot INTEGER,
  log_index INTEGER DEFAULT 0,
  mint TEXT,
  user TEXT,
  event_name TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS events_sig_log_idx ON events(sig, log_index, event_name);
CREATE INDEX IF NOT EXISTS events_mint_idx ON events(mint);

CREATE TABLE IF NOT EXISTS token_stats (
  mint TEXT PRIMARY KEY,
  name TEXT,
  symbol TEXT,
  image TEXT,
  metadata_uri TEXT,

  phase TEXT,
  phase_u8 INTEGER,
  quote_asset TEXT,
  quote_asset_u8 INTEGER,

  price_quote REAL,
  price_sol REAL,
  price_usd REAL,
  marketcap_quote REAL,
  marketcap_sol REAL,
  marketcap_usd REAL,

  total_supply TEXT,
  sale_supply TEXT,
  lp_supply TEXT,
  tokens_sold TEXT,
  tokens_remaining TEXT,
  bonding_progress REAL,

  sol_collected TEXT,
  sol_collected_lamports TEXT,
  liquidity_sol REAL DEFAULT 0,
  liquidity_usd REAL DEFAULT 0,

  sale_vault TEXT,
  lp_vault TEXT,
  treasury_wsol_vault TEXT,
  treasury_usdc_vault TEXT,
  sale_vault_amount TEXT,
  lp_vault_amount TEXT,
  treasury_wsol_amount TEXT,
  treasury_usdc_amount TEXT,

  holders_count INTEGER DEFAULT 0,

  volume_24h_quote REAL DEFAULT 0,
  volume_24h_sol REAL DEFAULT 0,
  volume_24h_usd REAL DEFAULT 0,
  trades_24h INTEGER DEFAULT 0,
  last_trade_ts INTEGER,
  updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS token_stats_marketcap_idx ON token_stats(marketcap_usd);
CREATE INDEX IF NOT EXISTS token_stats_phase_idx ON token_stats(phase);
CREATE INDEX IF NOT EXISTS token_stats_updated_idx ON token_stats(updated_at);

CREATE TABLE IF NOT EXISTS token_holders (
  mint TEXT NOT NULL,
  owner TEXT NOT NULL,
  token_account TEXT NOT NULL,
  amount TEXT NOT NULL DEFAULT '0',
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (mint, token_account)
);

CREATE INDEX IF NOT EXISTS token_holders_mint_owner_idx ON token_holders(mint, owner);
CREATE INDEX IF NOT EXISTS token_holders_mint_amount_idx ON token_holders(mint, amount);
CREATE INDEX IF NOT EXISTS token_holders_updated_idx ON token_holders(updated_at);

CREATE TABLE IF NOT EXISTS candles_1m (
  mint TEXT NOT NULL,
  bucket_ts INTEGER NOT NULL,
  open_sol REAL,
  high_sol REAL,
  low_sol REAL,
  close_sol REAL,
  open_usd REAL,
  high_usd REAL,
  low_usd REAL,
  close_usd REAL,
  volume_quote REAL NOT NULL DEFAULT 0,
  volume_sol REAL NOT NULL DEFAULT 0,
  volume_usd REAL NOT NULL DEFAULT 0,
  volume_tokens REAL NOT NULL DEFAULT 0,
  trades_count INTEGER NOT NULL DEFAULT 0,
  buys_count INTEGER NOT NULL DEFAULT 0,
  sells_count INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (mint, bucket_ts)
);

CREATE INDEX IF NOT EXISTS candles_1m_mint_ts_idx ON candles_1m(mint, bucket_ts);
`);

function columnExists(table, column) {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all();
  return rows.some((r) => r.name === column);
}

function ensureColumn(table, column, definition) {
  if (!columnExists(table, column)) {
    db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

ensureColumn("token_stats", "sol_collected", "TEXT");
ensureColumn("token_stats", "sol_collected_lamports", "TEXT");
ensureColumn("token_stats", "liquidity_sol", "REAL DEFAULT 0");
ensureColumn("token_stats", "liquidity_usd", "REAL DEFAULT 0");
ensureColumn("token_stats", "holders_count", "INTEGER DEFAULT 0");
ensureColumn("token_stats", "price_change_24h_percent", "REAL DEFAULT 0");
ensureColumn("token_stats", "price_change_24h_usd", "REAL DEFAULT 0");
ensureColumn("token_stats", "price_24h_ago_usd", "REAL");
ensureColumn("token_stats", "price_24h_ago_sol", "REAL");

ensureColumn("launches", "extensions_json", "TEXT");
ensureColumn("launches", "website", "TEXT");
ensureColumn("launches", "twitter", "TEXT");
ensureColumn("launches", "telegram", "TEXT");

function hasSeenTx(sig) {
  return !!db.prepare(`SELECT sig FROM tx_seen WHERE sig = ?`).get(sig);
}

function markTxSeen(sig, slot) {
  db.prepare(`
    INSERT OR IGNORE INTO tx_seen (sig, slot, first_seen_at)
    VALUES (?, ?, ?)
  `).run(sig, slot ?? null, now());
}

function setPrice(key, price) {
  db.prepare(`
    INSERT INTO prices (key, price, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(key)
    DO UPDATE SET price = excluded.price, updated_at = excluded.updated_at
  `).run(key, price, now());
}

function getPrice(key) {
  return db.prepare(`SELECT * FROM prices WHERE key = ?`).get(key);
}

function upsert(table, keyColumn, keyValue, patch) {
  const fields = Object.keys(patch).filter((k) => patch[k] !== undefined);

  if (!fields.length) {
    return db.prepare(`SELECT * FROM ${table} WHERE ${keyColumn} = ?`).get(keyValue);
  }

  const existing = db
    .prepare(`SELECT ${keyColumn} FROM ${table} WHERE ${keyColumn} = ?`)
    .get(keyValue);

  if (!existing) {
    const cols = [keyColumn, ...fields, "created_at", "updated_at"];
    const vals = [keyValue, ...fields.map((k) => patch[k]), now(), now()];
    const q = `INSERT INTO ${table} (${cols.join(", ")}) VALUES (${cols
      .map(() => "?")
      .join(", ")})`;

    db.prepare(q).run(...vals);
  } else {
    const sets = fields.map((k) => `${k} = ?`).join(", ");
    const vals = [...fields.map((k) => patch[k]), now(), keyValue];

    db.prepare(`
      UPDATE ${table}
      SET ${sets}, updated_at = ?
      WHERE ${keyColumn} = ?
    `).run(...vals);
  }

  return db.prepare(`SELECT * FROM ${table} WHERE ${keyColumn} = ?`).get(keyValue);
}

function upsertLaunch(mint, patch) {
  return upsert("launches", "mint", mint, patch);
}

function upsertTokenStats(mint, patch) {
  const fields = Object.keys(patch).filter((k) => patch[k] !== undefined);
  if (!fields.length) return getToken(mint);

  const existing = db.prepare(`SELECT mint FROM token_stats WHERE mint = ?`).get(mint);

  if (!existing) {
    const cols = ["mint", ...fields, "updated_at"];
    const vals = [mint, ...fields.map((k) => patch[k]), now()];

    db.prepare(`
      INSERT INTO token_stats (${cols.join(", ")})
      VALUES (${cols.map(() => "?").join(", ")})
    `).run(...vals);
  } else {
    const sets = fields.map((k) => `${k} = ?`).join(", ");

    db.prepare(`
      UPDATE token_stats
      SET ${sets}, updated_at = ?
      WHERE mint = ?
    `).run(
      ...fields.map((k) => patch[k]),
      now(),
      mint
    );
  }

  return getToken(mint);
}

function insertEvent(row) {
  db.prepare(`
    INSERT OR IGNORE INTO events (
      sig, slot, log_index, mint, user, event_name, payload_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    row.sig,
    row.slot ?? null,
    row.log_index ?? 0,
    row.mint ?? null,
    row.user ?? null,
    row.event_name,
    row.payload_json,
    row.created_at ?? now()
  );
}

function insertTrade(row) {
  const result = db.prepare(`
    INSERT OR IGNORE INTO trades (
      sig, slot, block_time, log_index,
      mint, user, side, phase, phase_u8, quote_asset, quote_asset_u8,
      input_amount, input_mint, output_amount, output_mint,
      quote_amount, token_amount, price_quote, price_sol, price_usd,
      creator_fee, platform_fee, lp_fee,
      tokens_sold_total, sol_collected_total,
      raw_event_name, raw_event_json, created_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `).run(
    row.sig,
    row.slot ?? null,
    row.block_time ?? null,
    row.log_index ?? 0,
    row.mint,
    row.user ?? null,
    row.side,
    row.phase ?? null,
    row.phase_u8 ?? null,
    row.quote_asset ?? null,
    row.quote_asset_u8 ?? null,
    row.input_amount ?? null,
    row.input_mint ?? null,
    row.output_amount ?? null,
    row.output_mint ?? null,
    row.quote_amount ?? null,
    row.token_amount ?? null,
    row.price_quote ?? null,
    row.price_sol ?? null,
    row.price_usd ?? null,
    row.creator_fee ?? null,
    row.platform_fee ?? null,
    row.lp_fee ?? null,
    row.tokens_sold_total ?? null,
    row.sol_collected_total ?? null,
    row.raw_event_name ?? null,
    row.raw_event_json ?? null,
    row.created_at ?? now()
  );

  return result.changes > 0;
}

function getHolderCount(mint) {
  const row = db.prepare(`
    SELECT COUNT(*) AS holders
    FROM (
      SELECT owner, SUM(CAST(amount AS INTEGER)) AS total_amount
      FROM token_holders
      WHERE mint = ?
      GROUP BY owner
      HAVING total_amount > 0
    )
  `).get(mint);

  return Number(row?.holders || 0);
}

function updateTokenHolderCount(mint) {
  const holders = getHolderCount(mint);

  db.prepare(`
    UPDATE token_stats
    SET holders_count = ?, updated_at = ?
    WHERE mint = ?
  `).run(holders, now(), mint);

  return holders;
}

function upsertHolderBalance({ mint, owner, token_account, amount, updated_at }) {
  if (!mint || !owner) return null;

  const tokenAccount = token_account || owner;
  const cleanAmount = String(amount ?? "0");
  const ts = updated_at || now();

  db.prepare(`
    INSERT INTO token_holders (
      mint,
      owner,
      token_account,
      amount,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(mint, token_account)
    DO UPDATE SET
      owner = excluded.owner,
      amount = excluded.amount,
      updated_at = excluded.updated_at
  `).run(mint, owner, tokenAccount, cleanAmount, ts);

  const holders = getHolderCount(mint);

  db.prepare(`
    UPDATE token_stats
    SET holders_count = ?, updated_at = ?
    WHERE mint = ?
  `).run(holders, ts, mint);

  return {
    mint,
    owner,
    token_account: tokenAccount,
    amount: cleanAmount,
    holders,
    holders_count: holders,
    updated_at: ts,
  };
}

function getHolderSummary(mint) {
  const token = getToken(mint);
  const holders = getHolderCount(mint);

  return {
    mint,
    holders,
    holders_count: holders,
    updated_at: token?.updated_at || now(),
  };
}

function getTopHolders({ mint, limit = 50, offset = 0 } = {}) {
  const cappedLimit = Math.min(200, Math.max(1, Number(limit || 50)));
  const safeOffset = Math.max(0, Number(offset || 0));

  return db.prepare(`
    SELECT
      owner,
      SUM(CAST(amount AS INTEGER)) AS amount_base,
      MAX(updated_at) AS updated_at
    FROM token_holders
    WHERE mint = ?
    GROUP BY owner
    HAVING amount_base > 0
    ORDER BY amount_base DESC
    LIMIT ? OFFSET ?
  `).all(mint, cappedLimit, safeOffset);
}

function minuteBucket(ts) {
  return Math.floor(Number(ts || now()) / 60) * 60;
}

function sanePositiveNumber(value) {
  const n = Number(value);

  return Number.isFinite(n) && n > 0 ? n : null;
}

function upsertCandle1m({
  mint,
  ts,
  priceSol,
  priceUsd,
  openPriceSol = null,
  openPriceUsd = null,
  volumeQuote,
  volumeSol,
  volumeUsd,
  volumeTokens,
  side,
}) {
  const cleanPriceSol = sanePositiveNumber(priceSol);
  const cleanPriceUsd = priceUsd === null || priceUsd === undefined
    ? null
    : sanePositiveNumber(priceUsd);

  if (!mint || !cleanPriceSol) return null;

  const explicitOpenSol = sanePositiveNumber(openPriceSol);
  const explicitOpenUsd = sanePositiveNumber(openPriceUsd);

  const bucket = minuteBucket(ts);
  const existing = db
    .prepare(`SELECT * FROM candles_1m WHERE mint = ? AND bucket_ts = ?`)
    .get(mint, bucket);

  const previous = !existing
    ? db.prepare(`
        SELECT close_sol, close_usd
        FROM candles_1m
        WHERE mint = ?
          AND bucket_ts < ?
          AND close_sol IS NOT NULL
          AND close_sol > 0
        ORDER BY bucket_ts DESC
        LIMIT 1
      `).get(mint, bucket)
    : null;

  const cleanOpenSol =
    explicitOpenSol ||
    sanePositiveNumber(previous?.close_sol) ||
    cleanPriceSol;

  const cleanOpenUsd =
    explicitOpenUsd ||
    sanePositiveNumber(previous?.close_usd) ||
    cleanPriceUsd;

  const isBuy = side === "BUY" || side === "DEVBUY" || side === "AMM_BUY";
  const isSell = side === "SELL" || side === "AMM_SELL";

  if (!existing) {
    db.prepare(`
      INSERT INTO candles_1m (
        mint, bucket_ts,
        open_sol, high_sol, low_sol, close_sol,
        open_usd, high_usd, low_usd, close_usd,
        volume_quote, volume_sol, volume_usd, volume_tokens,
        trades_count, buys_count, sells_count, updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      mint,
      bucket,
      cleanOpenSol,
      Math.max(cleanOpenSol, cleanPriceSol),
      Math.min(cleanOpenSol, cleanPriceSol),
      cleanPriceSol,
      cleanOpenUsd,
      cleanOpenUsd == null || cleanPriceUsd == null
        ? cleanOpenUsd ?? cleanPriceUsd
        : Math.max(cleanOpenUsd, cleanPriceUsd),
      cleanOpenUsd == null || cleanPriceUsd == null
        ? cleanOpenUsd ?? cleanPriceUsd
        : Math.min(cleanOpenUsd, cleanPriceUsd),
      cleanPriceUsd,
      volumeQuote || 0,
      volumeSol || 0,
      volumeUsd || 0,
      volumeTokens || 0,
      1,
      isBuy ? 1 : 0,
      isSell ? 1 : 0,
      now()
    );
  } else {
    const existingHighSol = sanePositiveNumber(existing.high_sol) || cleanPriceSol;
    const existingLowSol = sanePositiveNumber(existing.low_sol) || cleanPriceSol;
    const existingHighUsd = sanePositiveNumber(existing.high_usd);
    const existingLowUsd = sanePositiveNumber(existing.low_usd);

    const highSol = Math.max(existingHighSol, cleanPriceSol);
    const lowSol = Math.min(existingLowSol, cleanPriceSol);

    const highUsd =
      cleanPriceUsd == null
        ? existingHighUsd
        : Math.max(existingHighUsd || cleanPriceUsd, cleanPriceUsd);

    const lowUsd =
      cleanPriceUsd == null
        ? existingLowUsd
        : Math.min(existingLowUsd || cleanPriceUsd, cleanPriceUsd);

    db.prepare(`
      UPDATE candles_1m
      SET high_sol = ?, low_sol = ?, close_sol = ?,
          high_usd = ?, low_usd = ?, close_usd = ?,
          volume_quote = volume_quote + ?,
          volume_sol = volume_sol + ?,
          volume_usd = volume_usd + ?,
          volume_tokens = volume_tokens + ?,
          trades_count = trades_count + 1,
          buys_count = buys_count + ?,
          sells_count = sells_count + ?,
          updated_at = ?
      WHERE mint = ? AND bucket_ts = ?
    `).run(
      highSol,
      lowSol,
      cleanPriceSol,
      highUsd,
      lowUsd,
      cleanPriceUsd ?? sanePositiveNumber(existing.close_usd),
      volumeQuote || 0,
      volumeSol || 0,
      volumeUsd || 0,
      volumeTokens || 0,
      isBuy ? 1 : 0,
      isSell ? 1 : 0,
      now(),
      mint,
      bucket
    );
  }

  return db
    .prepare(`SELECT * FROM candles_1m WHERE mint = ? AND bucket_ts = ?`)
    .get(mint, bucket);
}

function get24hPriceChange(mint) {
  const since = now() - 86400;

  const token = getToken(mint);

  const latest = db.prepare(`
    SELECT close_usd, close_sol, bucket_ts
    FROM candles_1m
    WHERE mint = ?
      AND (
        close_usd IS NOT NULL
        OR close_sol IS NOT NULL
      )
    ORDER BY bucket_ts DESC
    LIMIT 1
  `).get(mint);

  const currentUsd =
    Number(token?.price_usd || 0) > 0
      ? Number(token.price_usd)
      : Number(latest?.close_usd || 0);

  const currentSol =
    Number(token?.price_sol || 0) > 0
      ? Number(token.price_sol)
      : Number(latest?.close_sol || 0);

  // Preferred baseline: a real candle from 24h ago or older.
  let old = db.prepare(`
    SELECT close_usd, close_sol, bucket_ts
    FROM candles_1m
    WHERE mint = ?
      AND bucket_ts <= ?
      AND (
        close_usd IS NOT NULL
        OR close_sol IS NOT NULL
      )
    ORDER BY bucket_ts DESC
    LIMIT 1
  `).get(mint, since);

  // New-token fallback: compare against the first candle open.
  // This makes the token card show movement since launch until a real 24h
  // baseline exists.
  if (!old) {
    old = db.prepare(`
      SELECT
        COALESCE(open_usd, close_usd) AS close_usd,
        COALESCE(open_sol, close_sol) AS close_sol,
        bucket_ts
      FROM candles_1m
      WHERE mint = ?
        AND (
          open_usd IS NOT NULL
          OR close_usd IS NOT NULL
          OR open_sol IS NOT NULL
          OR close_sol IS NOT NULL
        )
      ORDER BY bucket_ts ASC
      LIMIT 1
    `).get(mint);
  }

  let oldUsd = Number(old?.close_usd || 0);
  let oldSol = Number(old?.close_sol || 0);

  // Final fallback: if candles are missing/stale but token_stats already has
  // a stored baseline, use it instead of returning fake 0.00%.
  if ((!oldUsd || oldUsd <= 0) && Number(token?.price_24h_ago_usd || 0) > 0) {
    oldUsd = Number(token.price_24h_ago_usd);
  }

  if ((!oldSol || oldSol <= 0) && Number(token?.price_24h_ago_sol || 0) > 0) {
    oldSol = Number(token.price_24h_ago_sol);
  }

  const useUsd = currentUsd > 0 && oldUsd > 0;
  const current = useUsd ? currentUsd : currentSol;
  const oldPrice = useUsd ? oldUsd : oldSol;

  if (
    !Number.isFinite(current) ||
    !Number.isFinite(oldPrice) ||
    current <= 0 ||
    oldPrice <= 0
  ) {
    return {
      price_change_24h_percent: 0,
      price_change_24h_usd: 0,
      price_24h_ago_usd: oldUsd || null,
      price_24h_ago_sol: oldSol || null,
    };
  }

  const change = current - oldPrice;
  const percent = (change / oldPrice) * 100;

  return {
    price_change_24h_percent: Number.isFinite(percent) ? percent : 0,
    price_change_24h_usd:
      currentUsd > 0 && oldUsd > 0 && Number.isFinite(currentUsd - oldUsd)
        ? currentUsd - oldUsd
        : 0,
    price_24h_ago_usd: oldUsd || null,
    price_24h_ago_sol: oldSol || null,
  };
}


function refresh24hVolume(mint) {
  const since = now() - 86400;

  const row = db.prepare(`
    SELECT
      COALESCE(SUM(volume_quote), 0) AS volume_quote,
      COALESCE(SUM(volume_sol), 0) AS volume_sol,
      COALESCE(SUM(volume_usd), 0) AS stored_volume_usd,
      COALESCE(SUM(trades_count), 0) AS trades_count
    FROM candles_1m
    WHERE mint = ? AND bucket_ts >= ?
  `).get(mint, since);

  const volumeSol = Number(row.volume_sol || 0);
  const solUsd = Number(getPrice("SOL_USD")?.price || 0);

  // Recalculate USD volume from live SOL/USD.
  // Older candle rows may have volume_usd stored with SOL fixed at $100.
  const liveVolumeUsd =
    Number.isFinite(volumeSol) && volumeSol > 0 && Number.isFinite(solUsd) && solUsd > 0
      ? volumeSol * solUsd
      : Number(row.stored_volume_usd || 0);

  const priceChange = get24hPriceChange(mint);

  return upsertTokenStats(mint, {
    volume_24h_quote: row.volume_quote || 0,
    volume_24h_sol: volumeSol || 0,
    volume_24h_usd: liveVolumeUsd || 0,
    trades_24h: row.trades_count || 0,

    price_change_24h_percent: priceChange.price_change_24h_percent,
    price_change_24h_usd: priceChange.price_change_24h_usd,
    price_24h_ago_usd: priceChange.price_24h_ago_usd,
    price_24h_ago_sol: priceChange.price_24h_ago_sol,
  });
}

function getToken(mint) {
  return db.prepare(`
    SELECT
      ts.*,

      l.creator,
      l.launch_state,
      l.launch_escrow,
      l.metadata,
      l.description,

      l.name AS launch_name,
      l.symbol AS launch_symbol,
      l.image AS launch_image,
      l.metadata_uri AS launch_metadata_uri,
      l.pinata_cid AS launch_pinata_cid,
      l.extensions_json,
      l.website,
      l.twitter,
      l.telegram,

      l.launch_ts AS launch_ts,
      COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) AS created_at

    FROM token_stats ts
    LEFT JOIN launches l ON l.mint = ts.mint
    WHERE ts.mint = ?
  `).get(mint);
}

function listTokens({ limit = 50, offset = 0, phase = null } = {}) {
  const cappedLimit = Math.min(200, Math.max(1, Number(limit || 50)));
  const safeOffset = Math.max(0, Number(offset || 0));

  if (phase) {
    return db.prepare(`
      SELECT
        ts.*,

        l.creator,
        l.description,

        l.name AS launch_name,
        l.symbol AS launch_symbol,
        l.image AS launch_image,
        l.metadata_uri AS launch_metadata_uri,
        l.pinata_cid AS launch_pinata_cid,
        l.extensions_json,
        l.website,
        l.twitter,
        l.telegram,

        l.launch_ts AS launch_ts,
        COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) AS created_at

      FROM token_stats ts
      LEFT JOIN launches l ON l.mint = ts.mint
      WHERE ts.phase = ?
      ORDER BY COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) DESC
      LIMIT ? OFFSET ?
    `).all(phase, cappedLimit, safeOffset);
  }

  return db.prepare(`
    SELECT
      ts.*,

      l.creator,
      l.description,

      l.name AS launch_name,
      l.symbol AS launch_symbol,
      l.image AS launch_image,
      l.metadata_uri AS launch_metadata_uri,
      l.pinata_cid AS launch_pinata_cid,
      l.extensions_json,
      l.website,
      l.twitter,
      l.telegram,

      l.launch_ts AS launch_ts,
      COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) AS created_at

    FROM token_stats ts
    LEFT JOIN launches l ON l.mint = ts.mint
    ORDER BY COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) DESC
    LIMIT ? OFFSET ?
  `).all(cappedLimit, safeOffset);
}

function getTrades({ mint = null, limit = 50, offset = 0 } = {}) {
  const cappedLimit = Math.min(200, Math.max(1, Number(limit || 50)));
  const safeOffset = Math.max(0, Number(offset || 0));

  if (mint) {
    return db.prepare(`
      SELECT * FROM trades
      WHERE mint = ?
      ORDER BY id DESC
      LIMIT ? OFFSET ?
    `).all(mint, cappedLimit, safeOffset);
  }

  return db.prepare(`
    SELECT * FROM trades
    ORDER BY id DESC
    LIMIT ? OFFSET ?
  `).all(cappedLimit, safeOffset);
}

function getCandles({ mint, interval = "1m", limit = 500, since = null }) {
  const cappedLimit = Math.min(5000, Math.max(1, Number(limit || 500)));

  if (interval !== "1m") {
    const seconds = {
      "5m": 300,
      "15m": 900,
      "30m": 1800,
      "1h": 3600,
      "4h": 14400,
      "1d": 86400,
    }[interval];

    if (!seconds) throw new Error("Invalid interval");

    return db.prepare(`
      WITH base AS (
        SELECT *, (bucket_ts / ?) * ? AS tf_bucket
        FROM candles_1m
        WHERE mint = ?
          AND (? IS NULL OR bucket_ts >= ?)
          AND open_sol IS NOT NULL
          AND high_sol IS NOT NULL
          AND low_sol IS NOT NULL
          AND close_sol IS NOT NULL
          AND open_sol > 0
          AND high_sol > 0
          AND low_sol > 0
          AND close_sol > 0
      ), ranked AS (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts ASC) AS rn_open,
          ROW_NUMBER() OVER (PARTITION BY tf_bucket ORDER BY bucket_ts DESC) AS rn_close
        FROM base
      ), agg AS (
        SELECT
          tf_bucket AS bucket_ts,
          MAX(CASE WHEN rn_open = 1 THEN open_sol END) AS open_sol,
          MAX(high_sol) AS high_sol,
          MIN(low_sol) AS low_sol,
          MAX(CASE WHEN rn_close = 1 THEN close_sol END) AS close_sol,
          MAX(CASE WHEN rn_open = 1 THEN open_usd END) AS open_usd,
          MAX(high_usd) AS high_usd,
          MIN(low_usd) AS low_usd,
          MAX(CASE WHEN rn_close = 1 THEN close_usd END) AS close_usd,
          SUM(volume_quote) AS volume_quote,
          SUM(volume_sol) AS volume_sol,
          SUM(volume_usd) AS volume_usd,
          SUM(volume_tokens) AS volume_tokens,
          SUM(trades_count) AS trades_count,
          SUM(buys_count) AS buys_count,
          SUM(sells_count) AS sells_count
        FROM ranked
        GROUP BY tf_bucket
      )
      SELECT * FROM agg
      ORDER BY bucket_ts DESC
      LIMIT ?
    `).all(seconds, seconds, mint, since, since, cappedLimit).reverse();
  }

  return db.prepare(`
    SELECT * FROM candles_1m
    WHERE mint = ?
      AND (? IS NULL OR bucket_ts >= ?)
      AND open_sol IS NOT NULL
      AND high_sol IS NOT NULL
      AND low_sol IS NOT NULL
      AND close_sol IS NOT NULL
      AND open_sol > 0
      AND high_sol > 0
      AND low_sol > 0
      AND close_sol > 0
    ORDER BY bucket_ts DESC
    LIMIT ?
  `).all(mint, since, since, cappedLimit).reverse();
}


function isSaneMoonzPriceUsd(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 && n < 10;
}

function isSaneMoonzPriceSol(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) && n > 0 && n < 1;
}

function quoteBaseToUiForAsset(base, quoteAsset) {
  const n = Number(base || 0);

  if (!Number.isFinite(n)) return 0;

  if (quoteAsset === "USDC") {
    return n / 1_000_000;
  }

  return n / 1_000_000_000;
}

function tradeSolUsd(row) {
  const priceSol = Number(row.price_sol || 0);
  const priceUsd = Number(row.price_usd || 0);

  if (priceSol > 0 && priceUsd > 0) {
    return priceUsd / priceSol;
  }

  const sol = getPrice("SOL_USD");
  return Number(sol?.price || 0);
}

function quoteBaseToUsdForTrade(base, quoteAsset, row) {
  const amountUi = quoteBaseToUiForAsset(base, quoteAsset);

  if (quoteAsset === "USDC") {
    return amountUi;
  }

  const solUsd = tradeSolUsd(row);
  return solUsd > 0 ? amountUi * solUsd : 0;
}

function getCreatorTokens(address) {
  if (!address) return [];

  return db.prepare(`
    SELECT
      ts.*,

      l.creator,
      l.description,

      l.name AS launch_name,
      l.symbol AS launch_symbol,
      l.image AS launch_image,
      l.metadata_uri AS launch_metadata_uri,
      l.pinata_cid AS launch_pinata_cid,
      l.extensions_json,
      l.website,
      l.twitter,
      l.telegram,

      l.launch_ts AS launch_ts,
      COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) AS created_at

    FROM launches l
    LEFT JOIN token_stats ts ON ts.mint = l.mint
    WHERE l.creator = ?
    ORDER BY COALESCE(ts.last_trade_ts, l.launch_ts, ts.updated_at, l.updated_at) DESC
  `).all(address);
}

function getCreatorTrades(address) {
  if (!address) return [];

  return db.prepare(`
    SELECT
      t.*,
      l.creator
    FROM trades t
    INNER JOIN launches l ON l.mint = t.mint
    WHERE l.creator = ?
    ORDER BY t.created_at DESC
  `).all(address);
}



function normalizeUsername(username) {
  const clean = String(username || "")
    .trim()
    .replace(/^@+/, "")
    .toLowerCase();

  if (!clean) return "";

  if (!/^[a-z0-9_]{3,20}$/.test(clean)) {
    throw new Error("Username must be 3-20 characters using letters, numbers, or underscore only");
  }

  return clean;
}

function publicUserProfile(row) {
  if (!row) return null;

  const wallet = String(row.wallet || "");

  const rawFollowerCount = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_follows
    WHERE following_wallet = ?
  `).get(wallet)?.count || 0;

  const followerCount = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_follows
    WHERE following_wallet = ?
      AND verified_at IS NOT NULL
  `).get(wallet)?.count || 0;

  const followingCount = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_follows
    WHERE follower_wallet = ?
  `).get(wallet)?.count || 0;

  return {
    wallet,
    username: row.username || null,
    display_username: row.username ? `@${row.username}` : null,
    username_lc: row.username_lc || null,
    display_name: row.display_name || null,
    bio: row.bio || null,
    avatar_url: row.avatar_url || null,
    avatar_updated_at: row.avatar_updated_at || null,
    follower_count: Number(followerCount || 0),
    raw_follower_count: Number(rawFollowerCount || 0),
    following_count: Number(followingCount || 0),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
}

function getUserProfile(wallet) {
  if (!wallet) return null;

  const row = db.prepare(`
    SELECT *
    FROM user_profiles
    WHERE wallet = ?
  `).get(String(wallet));

  return publicUserProfile(row);
}

function getUserProfileByUsername(username) {
  const usernameLc = normalizeUsername(username);

  const row = db.prepare(`
    SELECT *
    FROM user_profiles
    WHERE username_lc = ?
  `).get(usernameLc);

  return publicUserProfile(row);
}

function isUsernameAvailable(username, wallet = "") {
  const usernameLc = normalizeUsername(username);

  const row = db.prepare(`
    SELECT wallet
    FROM user_profiles
    WHERE username_lc = ?
  `).get(usernameLc);

  return {
    username: usernameLc,
    available: !row || String(row.wallet) === String(wallet || ""),
    owner_wallet: row?.wallet || null,
  };
}

function upsertUserProfile({ wallet, username, display_name, bio, avatar_url }) {
  if (!wallet) throw new Error("Wallet is required");

  const cleanWallet = String(wallet).trim();
  if (!cleanWallet) throw new Error("Wallet is required");

  let usernameLc = null;
  let usernameClean = null;

  if (username !== undefined && username !== null && String(username).trim() !== "") {
    usernameLc = normalizeUsername(username);
    usernameClean = usernameLc;
  }

  const current = db.prepare(`
    SELECT *
    FROM user_profiles
    WHERE wallet = ?
  `).get(cleanWallet);

  const nowTs = now();

  const next = {
    wallet: cleanWallet,
    username: usernameClean !== null ? usernameClean : current?.username || null,
    username_lc: usernameLc !== null ? usernameLc : current?.username_lc || null,
    display_name:
      display_name !== undefined
        ? String(display_name || "").trim().slice(0, 40) || null
        : current?.display_name || null,
    bio:
      bio !== undefined
        ? String(bio || "").trim().slice(0, 160) || null
        : current?.bio || null,
    avatar_url:
      avatar_url !== undefined
        ? avatar_url || null
        : current?.avatar_url || null,
    avatar_updated_at:
      avatar_url !== undefined
        ? nowTs
        : current?.avatar_updated_at || null,
    updated_at: nowTs,
  };

  db.prepare(`
    INSERT INTO user_profiles (
      wallet,
      username,
      username_lc,
      display_name,
      bio,
      avatar_url,
      avatar_updated_at,
      created_at,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(wallet) DO UPDATE SET
      username = excluded.username,
      username_lc = excluded.username_lc,
      display_name = excluded.display_name,
      bio = excluded.bio,
      avatar_url = excluded.avatar_url,
      avatar_updated_at = excluded.avatar_updated_at,
      updated_at = excluded.updated_at
  `).run(
    next.wallet,
    next.username,
    next.username_lc,
    next.display_name,
    next.bio,
    next.avatar_url,
    next.avatar_updated_at,
    current?.created_at || nowTs,
    next.updated_at
  );

  return getUserProfile(cleanWallet);
}

function followUser(followerWallet, followingWallet, options = {}) {
  const follower = String(followerWallet || "").trim();
  const following = String(followingWallet || "").trim();

  if (!follower || !following) throw new Error("Follower and following wallets are required");
  if (follower === following) throw new Error("You cannot follow yourself");

  const followerProfile = db.prepare(`
    SELECT *
    FROM user_profiles
    WHERE wallet = ?
  `).get(follower);

  if (!followerProfile) {
    throw new Error("Create a profile before following creators");
  }

  const nowTs = now();

  const verifiedAt = options.verified ? nowTs : null;
  const verifiedReason = options.verifiedReason || null;
  const followerSolLamports =
    options.followerSolLamports !== undefined && options.followerSolLamports !== null
      ? String(options.followerSolLamports)
      : null;

  db.prepare(`
    INSERT INTO user_follows (
      follower_wallet,
      following_wallet,
      created_at,
      verified_at,
      verified_reason,
      follower_sol_lamports,
      follower_profile_created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(follower_wallet, following_wallet) DO UPDATE SET
      verified_at = COALESCE(excluded.verified_at, user_follows.verified_at),
      verified_reason = COALESCE(excluded.verified_reason, user_follows.verified_reason),
      follower_sol_lamports = COALESCE(excluded.follower_sol_lamports, user_follows.follower_sol_lamports),
      follower_profile_created_at = COALESCE(excluded.follower_profile_created_at, user_follows.follower_profile_created_at)
  `).run(
    follower,
    following,
    nowTs,
    verifiedAt,
    verifiedReason,
    followerSolLamports,
    followerProfile.created_at || nowTs
  );

  return {
    ok: true,
    follower_wallet: follower,
    following_wallet: following,
    following: true,
    verified: Boolean(verifiedAt),
  };
}

function unfollowUser(followerWallet, followingWallet) {
  const follower = String(followerWallet || "").trim();
  const following = String(followingWallet || "").trim();

  if (!follower || !following) throw new Error("Follower and following wallets are required");

  db.prepare(`
    DELETE FROM user_follows
    WHERE follower_wallet = ?
      AND following_wallet = ?
  `).run(follower, following);

  return {
    ok: true,
    follower_wallet: follower,
    following_wallet: following,
    following: false,
  };
}

function isFollowing(followerWallet, followingWallet) {
  const row = db.prepare(`
    SELECT 1 AS ok
    FROM user_follows
    WHERE follower_wallet = ?
      AND following_wallet = ?
  `).get(String(followerWallet || ""), String(followingWallet || ""));

  return Boolean(row);
}

function listFollowers(wallet, limit = 50) {
  const rows = db.prepare(`
    SELECT
      f.follower_wallet AS wallet,
      f.created_at AS followed_at,
      p.username,
      p.username_lc,
      p.display_name,
      p.bio,
      p.avatar_url,
      p.avatar_updated_at,
      p.created_at,
      p.updated_at
    FROM user_follows f
    LEFT JOIN user_profiles p ON p.wallet = f.follower_wallet
    WHERE f.following_wallet = ?
    ORDER BY f.created_at DESC
    LIMIT ?
  `).all(String(wallet || ""), Math.min(Math.max(Number(limit) || 50, 1), 100));

  return rows.map(publicUserProfile).filter(Boolean);
}

function listFollowing(wallet, limit = 50) {
  const rows = db.prepare(`
    SELECT
      f.following_wallet AS wallet,
      f.created_at AS followed_at,
      p.username,
      p.username_lc,
      p.display_name,
      p.bio,
      p.avatar_url,
      p.avatar_updated_at,
      p.created_at,
      p.updated_at
    FROM user_follows f
    LEFT JOIN user_profiles p ON p.wallet = f.following_wallet
    WHERE f.follower_wallet = ?
    ORDER BY f.created_at DESC
    LIMIT ?
  `).all(String(wallet || ""), Math.min(Math.max(Number(limit) || 50, 1), 100));

  return rows.map(publicUserProfile).filter(Boolean);
}



function publicNotification(row) {
  if (!row) return null;

  let data = null;

  try {
    data = row.data_json ? JSON.parse(row.data_json) : null;
  } catch {
    data = null;
  }

  return {
    id: row.id,
    recipient_wallet: row.recipient_wallet,
    actor_wallet: row.actor_wallet || null,
    type: row.type,
    title: row.title,
    body: row.body || null,
    mint: row.mint || null,
    data,
    read_at: row.read_at || null,
    created_at: row.created_at || null,
  };
}


db.exec(`
CREATE TABLE IF NOT EXISTS token_king_notifications_sent (
  mint TEXT NOT NULL,
  hour_start INTEGER NOT NULL,
  creator TEXT,
  sent_at INTEGER NOT NULL,
  PRIMARY KEY (mint, hour_start)
);

CREATE TABLE IF NOT EXISTS token_migration_notifications_sent (
  mint TEXT PRIMARY KEY,
  creator TEXT,
  sent_at INTEGER NOT NULL
);
`);

function createUserNotification({
  recipient_wallet,
  actor_wallet = null,
  type,
  title,
  body = null,
  mint = null,
  data = null,
  unique_key = null,
}) {
  const recipient = String(recipient_wallet || "").trim();

  if (!recipient) throw new Error("Notification recipient is required");
  if (!type) throw new Error("Notification type is required");
  if (!title) throw new Error("Notification title is required");

  const nowTs = now();
  const dataJson = data ? JSON.stringify(data) : null;

  const result = db.prepare(`
    INSERT OR IGNORE INTO user_notifications (
      recipient_wallet,
      actor_wallet,
      type,
      title,
      body,
      mint,
      data_json,
      unique_key,
      created_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    recipient,
    actor_wallet || null,
    String(type),
    String(title),
    body || null,
    mint || null,
    dataJson,
    unique_key || null,
    nowTs
  );

  if (!result.changes) {
    return true;
  }

  const row = db.prepare(`
    SELECT *
    FROM user_notifications
    WHERE id = ?
  `).get(result.lastInsertRowid);

  const notification = publicNotification(row);

  publishNotificationCreated(notification);

  return notification || true;
}

function createNotificationsForFollowers({
  actor_wallet,
  type,
  title,
  body = null,
  mint = null,
  data = null,
  unique_key_prefix = null,
}) {
  const actor = String(actor_wallet || "").trim();
  if (!actor) return { inserted: 0, followers: 0 };

  const followers = db.prepare(`
    SELECT follower_wallet
    FROM user_follows
    WHERE following_wallet = ?
      AND verified_at IS NOT NULL
  `).all(actor);

  let inserted = 0;

  const tx = db.transaction(() => {
    for (const row of followers) {
      const recipient = String(row.follower_wallet || "").trim();
      if (!recipient || recipient === actor) continue;

      const uniqueKey = unique_key_prefix
        ? `${unique_key_prefix}:${recipient}`
        : null;

      const before = db.prepare(`
        SELECT COUNT(*) AS count
        FROM user_notifications
        WHERE recipient_wallet = ?
          AND unique_key = ?
      `).get(recipient, uniqueKey)?.count || 0;

      createUserNotification({
        recipient_wallet: recipient,
        actor_wallet: actor,
        type,
        title,
        body,
        mint,
        data,
        unique_key: uniqueKey,
      });

      const after = db.prepare(`
        SELECT COUNT(*) AS count
        FROM user_notifications
        WHERE recipient_wallet = ?
          AND unique_key = ?
      `).get(recipient, uniqueKey)?.count || 0;

      if (after > before) inserted += 1;
    }
  });

  tx();

  return {
    inserted,
    followers: followers.length,
  };
}

function listUserNotifications(wallet, limit = 50) {
  const clean = String(wallet || "").trim();
  const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 100);

  if (!clean) return [];

  const rows = db.prepare(`
    SELECT *
    FROM user_notifications
    WHERE recipient_wallet = ?
    ORDER BY created_at DESC, id DESC
    LIMIT ?
  `).all(clean, safeLimit);

  return rows.map(publicNotification).filter(Boolean);
}

function getUnreadNotificationCount(wallet) {
  const clean = String(wallet || "").trim();

  if (!clean) return 0;

  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_notifications
    WHERE recipient_wallet = ?
      AND read_at IS NULL
  `).get(clean);

  return Number(row?.count || 0);
}

function markNotificationsRead(wallet, ids = []) {
  const clean = String(wallet || "").trim();
  const safeIds = Array.isArray(ids)
    ? ids.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)
    : [];

  if (!clean || !safeIds.length) return 0;

  const placeholders = safeIds.map(() => "?").join(",");

  const result = db.prepare(`
    DELETE FROM user_notifications
    WHERE recipient_wallet = ?
      AND id IN (${placeholders})
  `).run(clean, ...safeIds);

  return result.changes || 0;
}

function markAllNotificationsRead(wallet) {
  const clean = String(wallet || "").trim();

  if (!clean) return 0;

  const result = db.prepare(`
    DELETE FROM user_notifications
    WHERE recipient_wallet = ?
  `).run(clean);

  return result.changes || 0;
}

function notifyFollowersOfCreatorLaunch({
  creator,
  mint,
  name,
  symbol,
  image = null,
}) {
  const actor = String(creator || "").trim();
  const tokenMint = String(mint || "").trim();

  if (!actor || !tokenMint) {
    return { inserted: 0, followers: 0 };
  }

  const cleanSymbol = String(symbol || "").trim();
  const cleanName = String(name || "").trim();

  return createNotificationsForFollowers({
    actor_wallet: actor,
    type: "creator_token_launch",
    title: cleanSymbol
      ? `New Moonz launch: $${cleanSymbol}`
      : "New Moonz token launched",
    body: cleanName
      ? `${cleanName} was launched by a creator you follow.`
      : "A creator you follow launched a new token.",
    mint: tokenMint,
    data: {
      creator: actor,
      mint: tokenMint,
      name: cleanName || null,
      symbol: cleanSymbol || null,
      image: image || null,
      token_url: `/token/${tokenMint}`,
      creator_url: `/creator/${actor}`,
    },
    unique_key_prefix: `creator_token_launch:${tokenMint}`,
  });
}



function notifyFollowersOnceOfCreatorLaunch({
  creator,
  mint,
  name,
  symbol,
  image = null,
}) {
  const actor = String(creator || "").trim();
  const tokenMint = String(mint || "").trim();

  if (!actor || !tokenMint) {
    return {
      skipped: true,
      reason: "missing_creator_or_mint",
      followers: 0,
      inserted: 0,
    };
  }

  const existing = db.prepare(`
    SELECT mint
    FROM creator_launch_notifications_sent
    WHERE mint = ?
  `).get(tokenMint);

  if (existing) {
    return {
      skipped: true,
      reason: "already_sent",
      followers: 0,
      inserted: 0,
    };
  }

  const result = notifyFollowersOfCreatorLaunch({
    creator: actor,
    mint: tokenMint,
    name,
    symbol,
    image,
  });

  db.prepare(`
    INSERT OR IGNORE INTO creator_launch_notifications_sent (
      mint,
      creator,
      followers_count,
      inserted_count,
      sent_at
    )
    VALUES (?, ?, ?, ?, ?)
  `).run(
    tokenMint,
    actor,
    Number(result.followers || 0),
    Number(result.inserted || 0),
    now()
  );

  return {
    skipped: false,
    reason: "sent",
    followers: Number(result.followers || 0),
    inserted: Number(result.inserted || 0),
  };
}


function getTokenNotificationMeta(mint) {
  const cleanMint = String(mint || "").trim();

  if (!cleanMint) return null;

  return db.prepare(`
    SELECT
      l.mint,
      l.creator,
      COALESCE(ts.name, l.name) AS name,
      COALESCE(ts.symbol, l.symbol) AS symbol,
      COALESCE(ts.image, l.image) AS image,
      COALESCE(ts.phase, l.phase) AS phase,
      COALESCE(NULLIF(l.launch_ts, 0), l.created_at, ts.updated_at) AS created_at
    FROM launches l
    LEFT JOIN token_stats ts ON ts.mint = l.mint
    WHERE l.mint = ?
  `).get(cleanMint);
}

function notifyWalletFollowed({ follower, following }) {
  const followerWallet = String(follower || "").trim();
  const followingWallet = String(following || "").trim();

  if (!followerWallet || !followingWallet || followerWallet === followingWallet) {
    return { inserted: 0, skipped: true };
  }

  const followerProfile = getUserProfile(followerWallet);
  const followerName =
    followerProfile?.display_username ||
    followerProfile?.username ||
    `${followerWallet.slice(0, 4)}...${followerWallet.slice(-4)}`;

  const uniqueKey = `creator_followed_you:${followingWallet}:${followerWallet}`;

  const before = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_notifications
    WHERE recipient_wallet = ?
      AND unique_key = ?
  `).get(followingWallet, uniqueKey)?.count || 0;

  createUserNotification({
    recipient_wallet: followingWallet,
    actor_wallet: followerWallet,
    type: "creator_followed_you",
    title: `${followerName} followed you`,
    body: "A Moonz user followed your creator profile.",
    mint: null,
    data: {
      follower_wallet: followerWallet,
      follower_username: followerProfile?.username || null,
      creator_url: `/creator/${followerWallet}`,
    },
    unique_key: uniqueKey,
  });

  const after = db.prepare(`
    SELECT COUNT(*) AS count
    FROM user_notifications
    WHERE recipient_wallet = ?
      AND unique_key = ?
  `).get(followingWallet, uniqueKey)?.count || 0;

  return { inserted: after > before ? 1 : 0, skipped: after <= before };
}

function notifyTokenHitKingOnce({ mint, hour_start = null }) {
  const token = getTokenNotificationMeta(mint);

  if (!token?.mint || !token?.creator) {
    return { inserted: 0, skipped: true, reason: "missing_token_or_creator" };
  }

  const hourStart = Number(hour_start || Math.floor(Date.now() / 3600000) * 3600000);

  const marker = db.prepare(`
    INSERT OR IGNORE INTO token_king_notifications_sent (
      mint,
      hour_start,
      creator,
      sent_at
    )
    VALUES (?, ?, ?, ?)
  `).run(token.mint, hourStart, token.creator, now());

  if (!marker.changes) {
    return { inserted: 0, skipped: true, reason: "already_sent" };
  }

  const symbol = String(token.symbol || "").trim();
  const name = String(token.name || "").trim();

  createUserNotification({
    recipient_wallet: token.creator,
    actor_wallet: token.creator,
    type: "token_hit_king",
    title: symbol ? `$${symbol} became King of the Moonz` : "Your token became King of the Moonz",
    body: name ? `${name} is leading hourly Moonz volume.` : "Your token is leading hourly Moonz volume.",
    mint: token.mint,
    data: {
      mint: token.mint,
      name: name || null,
      symbol: symbol || null,
      image: token.image || null,
      token_url: `/token/${token.mint}`,
      creator_url: `/creator/${token.creator}`,
    },
    unique_key: `token_hit_king:${token.mint}:${hourStart}`,
  });

  return { inserted: 1, skipped: false };
}

function notifyTokenMigratedAmmOnce({ mint }) {
  const token = getTokenNotificationMeta(mint);

  if (!token?.mint || !token?.creator) {
    return { inserted: 0, skipped: true, reason: "missing_token_or_creator" };
  }

  const phase = String(token.phase || "").toLowerCase();

  if (!["amm_live", "migrated"].includes(phase)) {
    return { inserted: 0, skipped: true, reason: "not_amm_live" };
  }

  const marker = db.prepare(`
    INSERT OR IGNORE INTO token_migration_notifications_sent (
      mint,
      creator,
      sent_at
    )
    VALUES (?, ?, ?)
  `).run(token.mint, token.creator, now());

  if (!marker.changes) {
    return { inserted: 0, skipped: true, reason: "already_sent" };
  }

  const symbol = String(token.symbol || "").trim();
  const name = String(token.name || "").trim();

  createUserNotification({
    recipient_wallet: token.creator,
    actor_wallet: token.creator,
    type: "token_migrated_amm",
    title: symbol ? `$${symbol} is now AMM live` : "Your token is now AMM live",
    body: name ? `${name} migrated from bonding to AMM live.` : "Your token migrated from bonding to AMM live.",
    mint: token.mint,
    data: {
      mint: token.mint,
      name: name || null,
      symbol: symbol || null,
      image: token.image || null,
      token_url: `/token/${token.mint}`,
      creator_url: `/creator/${token.creator}`,
    },
    unique_key: `token_migrated_amm:${token.mint}`,
  });

  return { inserted: 1, skipped: false };
}

function notifyCreatorFeeClaimableOnce({
  creator,
  mint,
  amountUsd = 0,
  bucket = "default",
}) {
  const cleanCreator = String(creator || "").trim();
  const cleanMint = String(mint || "").trim();

  if (!cleanCreator || !cleanMint) {
    return { inserted: 0, skipped: true, reason: "missing_creator_or_mint" };
  }

  const token = getTokenNotificationMeta(cleanMint) || {};
  const cleanBucket = String(bucket || "default").trim();

  const marker = db.prepare(`
    INSERT OR IGNORE INTO creator_fee_notifications_sent (
      creator,
      mint,
      bucket,
      sent_at
    )
    VALUES (?, ?, ?, ?)
  `).run(cleanCreator, cleanMint, cleanBucket, now());

  if (!marker.changes) {
    return { inserted: 0, skipped: true, reason: "already_sent" };
  }

  const symbol = String(token.symbol || "").trim();

  createUserNotification({
    recipient_wallet: cleanCreator,
    actor_wallet: cleanCreator,
    type: "creator_fee_claimable",
    title: symbol ? `Creator fees available for $${symbol}` : "Creator fees available",
    body:
      Number(amountUsd || 0) > 0
        ? `You have creator fees available. Estimated value: $${Number(amountUsd).toFixed(2)}.`
        : "You have creator fees available to review.",
    mint: cleanMint,
    data: {
      mint: cleanMint,
      symbol: symbol || null,
      amount_usd: Number(amountUsd || 0),
      token_url: `/token/${cleanMint}`,
      creator_url: `/creator/${cleanCreator}`,
    },
    unique_key: `creator_fee_claimable:${cleanCreator}:${cleanMint}:${cleanBucket}`,
  });

  return { inserted: 1, skipped: false };
}

function seedExistingNotificationMarkers() {
  db.prepare(`
    INSERT OR IGNORE INTO creator_launch_notifications_sent (
      mint,
      creator,
      followers_count,
      inserted_count,
      sent_at
    )
    SELECT
      mint,
      COALESCE(creator, ''),
      0,
      0,
      strftime('%s','now')
    FROM launches
    WHERE mint IS NOT NULL
      AND mint != ''
      AND creator IS NOT NULL
      AND creator != ''
  `).run();

  db.prepare(`
    INSERT OR IGNORE INTO token_migration_notifications_sent (
      mint,
      creator,
      sent_at
    )
    SELECT
      mint,
      COALESCE(creator, ''),
      strftime('%s','now')
    FROM launches
    WHERE mint IS NOT NULL
      AND mint != ''
      AND creator IS NOT NULL
      AND creator != ''
      AND LOWER(COALESCE(phase, '')) IN ('amm_live', 'migrated')
  `).run();
}

seedExistingNotificationMarkers();


function getCreatorProfile(address) {
  const tokensCreated = getCreatorTokens(address);
  const trades = getCreatorTrades(address);

  const tsNow = now();
  const todayStart = tsNow - 86400;
  const weekStart = tsNow - 86400 * 7;

  let totalVolumeUsd = 0;
  let totalVolume24hUsd = 0;

  let totalCreatorFeesUsd = 0;
  let creatorFeesTodayUsd = 0;
  let creatorFeesWeekUsd = 0;

  let totalCreatorFeesSol = 0;
  let creatorFeesTodaySol = 0;
  let creatorFeesWeekSol = 0;

  for (const trade of trades) {
    const quoteAsset = trade.quote_asset || "SOL";
    const createdAt = Number(trade.created_at || 0);

    const volumeUsd = quoteBaseToUsdForTrade(
      trade.quote_amount,
      quoteAsset,
      trade
    );

    const creatorFeeUsd = quoteBaseToUsdForTrade(
      trade.creator_fee,
      quoteAsset,
      trade
    );

    const creatorFeeSol =
      quoteAsset === "SOL"
        ? quoteBaseToUiForAsset(trade.creator_fee, "SOL")
        : 0;

    totalVolumeUsd += volumeUsd;
    totalCreatorFeesUsd += creatorFeeUsd;
    totalCreatorFeesSol += creatorFeeSol;

    if (createdAt >= todayStart) {
      totalVolume24hUsd += volumeUsd;
      creatorFeesTodayUsd += creatorFeeUsd;
      creatorFeesTodaySol += creatorFeeSol;
    }

    if (createdAt >= weekStart) {
      creatorFeesWeekUsd += creatorFeeUsd;
      creatorFeesWeekSol += creatorFeeSol;
    }
  }

  const activeTokens = tokensCreated.filter((token) => {
    const phase = String(token.phase || "").toLowerCase();

    return [
      "bonding",
      "pending_dev_buy",
      "migration_pending",
      "amm_live",
      "migrated",
      "switching",
    ].includes(phase);
  }).length;

  const liveTokens = tokensCreated.filter((token) => {
    const phase = String(token.phase || "").toLowerCase();

    return ["amm_live", "migrated", "switching"].includes(phase);
  }).length;

  const totalMarketCapUsd = tokensCreated.reduce((acc, token) => {
    return acc + Number(token.marketcap_usd || token.marketCapUsd || 0);
  }, 0);

  const bestTokenRow =
    [...tokensCreated].sort((a, b) => {
      return Number(b.marketcap_usd || 0) - Number(a.marketcap_usd || 0);
    })[0] || null;

  const bestToken = bestTokenRow
    ? {
        mint: bestTokenRow.mint,
        name: bestTokenRow.name || bestTokenRow.launch_name || null,
        symbol: bestTokenRow.symbol || bestTokenRow.launch_symbol || null,
        image: bestTokenRow.image || bestTokenRow.launch_image || null,
        phase: bestTokenRow.phase || null,
        marketcap_usd: Number(bestTokenRow.marketcap_usd || 0),
        volume_24h_usd: Number(bestTokenRow.volume_24h_usd || 0),
        holders_count: Number(bestTokenRow.holders_count || 0),
      }
    : null;

  return {
    address,

    tokensCreated,
    tokenCount: tokensCreated.length,
    activeTokens,
    liveTokens,

    totalMarketCapUsd,
    totalVolumeUsd,
    totalVolume24hUsd,

    totalCreatorFeesUsd,
    creatorFeesTodayUsd,
    creatorFeesWeekUsd,

    totalCreatorFeesSol,
    creatorFeesTodaySol,
    creatorFeesWeekSol,

    totalFeesEarnedUsd: totalCreatorFeesUsd,

    bestToken,
    updated_at: tsNow,
  };
}

module.exports = {
  db,
  now,
  hasSeenTx,
  markTxSeen,
  setPrice,
  getPrice,
  upsertLaunch,
  upsertTokenStats,
  insertEvent,
  insertTrade,
  upsertHolderBalance,
  getHolderCount,
  getHolderSummary,
  getTopHolders,
  upsertCandle1m,
  refresh24hVolume,
  getToken,
  listTokens,
  getTrades,
  getCandles,
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
  createUserNotification,
  createNotificationsForFollowers,
  notifyTokenMigratedAmmOnce,
  notifyTokenHitKingOnce,
  notifyWalletFollowed,
  listUserNotifications,
  getUnreadNotificationCount,
  markNotificationsRead,
  markAllNotificationsRead,
  notifyFollowersOfCreatorLaunch,
  notifyFollowersOnceOfCreatorLaunch,
};
