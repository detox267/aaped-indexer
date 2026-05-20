const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");
require("dotenv").config();

const TOKENS_DB = process.env.TOKENS_DB || path.join(__dirname, "tokens.db");
fs.mkdirSync(path.dirname(TOKENS_DB), { recursive: true });

const db = new Database(TOKENS_DB);

db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("foreign_keys = ON");
db.pragma("cache_size = -200000");
db.pragma("temp_store = MEMORY");
db.pragma("busy_timeout = 5000");

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

ensureColumn("token_stats", "holders_count", "INTEGER DEFAULT 0");

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
  db.prepare(`
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

function upsertCandle1m({
  mint,
  ts,
  priceSol,
  priceUsd,
  volumeQuote,
  volumeSol,
  volumeUsd,
  volumeTokens,
  side,
}) {
  if (!mint || !Number.isFinite(priceSol) || priceSol <= 0) return null;

  const bucket = minuteBucket(ts);
  const existing = db
    .prepare(`SELECT * FROM candles_1m WHERE mint = ? AND bucket_ts = ?`)
    .get(mint, bucket);

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
      priceSol,
      priceSol,
      priceSol,
      priceSol,
      priceUsd ?? null,
      priceUsd ?? null,
      priceUsd ?? null,
      priceUsd ?? null,
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
    const highSol = Math.max(existing.high_sol ?? priceSol, priceSol);
    const lowSol = Math.min(existing.low_sol ?? priceSol, priceSol);
    const highUsd =
      priceUsd == null
        ? existing.high_usd
        : Math.max(existing.high_usd ?? priceUsd, priceUsd);

    const lowUsd =
      priceUsd == null
        ? existing.low_usd
        : Math.min(existing.low_usd ?? priceUsd, priceUsd);

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
      priceSol,
      highUsd,
      lowUsd,
      priceUsd ?? existing.close_usd,
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

function refresh24hVolume(mint) {
  const since = now() - 86400;

  const row = db.prepare(`
    SELECT
      COALESCE(SUM(volume_quote), 0) AS volume_quote,
      COALESCE(SUM(volume_sol), 0) AS volume_sol,
      COALESCE(SUM(volume_usd), 0) AS volume_usd,
      COALESCE(SUM(trades_count), 0) AS trades_count
    FROM candles_1m
    WHERE mint = ? AND bucket_ts >= ?
  `).get(mint, since);

  return upsertTokenStats(mint, {
    volume_24h_quote: row.volume_quote || 0,
    volume_24h_sol: row.volume_sol || 0,
    volume_24h_usd: row.volume_usd || 0,
    trades_24h: row.trades_count || 0,
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

      l.launch_ts AS launch_ts,
      l.launch_ts AS created_at

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

        l.launch_ts AS launch_ts,
        l.launch_ts AS created_at

      FROM token_stats ts
      LEFT JOIN launches l ON l.mint = ts.mint
      WHERE ts.phase = ?
      ORDER BY COALESCE(l.launch_ts, ts.updated_at) DESC
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

      l.launch_ts AS launch_ts,
      l.launch_ts AS created_at

    FROM token_stats ts
    LEFT JOIN launches l ON l.mint = ts.mint
    ORDER BY COALESCE(l.launch_ts, ts.updated_at) DESC
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
        WHERE mint = ? AND (? IS NULL OR bucket_ts >= ?)
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
    WHERE mint = ? AND (? IS NULL OR bucket_ts >= ?)
    ORDER BY bucket_ts DESC
    LIMIT ?
  `).all(mint, since, since, cappedLimit).reverse();
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

      l.launch_ts AS launch_ts,
      l.launch_ts AS created_at

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

    const volumeUsd =
      Number(trade.price_usd || 0) > 0 && Number(trade.token_amount || 0) > 0
        ? quoteBaseToUsdForTrade(trade.quote_amount, quoteAsset, trade)
        : quoteBaseToUsdForTrade(trade.quote_amount, quoteAsset, trade);

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

    if (Number(trade.created_at || 0) >= todayStart) {
      totalVolume24hUsd += volumeUsd;
      creatorFeesTodayUsd += creatorFeeUsd;
      creatorFeesTodaySol += creatorFeeSol;
    }

    if (Number(trade.created_at || 0) >= weekStart) {
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

  const bestToken =
    [...tokensCreated].sort((a, b) => {
      return Number(b.marketcap_usd || 0) - Number(a.marketcap_usd || 0);
    })[0] || null;

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
    totalVolumeUsd,

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
};
