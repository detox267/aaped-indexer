// db.js
const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

// Use TOKENS_DB env var (you already do this pattern)
const TOKENS_DB = process.env.TOKENS_DB || path.join(__dirname, "tokens.db");
fs.mkdirSync(path.dirname(TOKENS_DB), { recursive: true });

const db = new Database(TOKENS_DB);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

// -----------------------------
// schema
// -----------------------------
db.exec(`
CREATE TABLE IF NOT EXISTS launches (
  mint TEXT PRIMARY KEY,
  launch_state TEXT,
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

  state_u8 INTEGER,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sig TEXT NOT NULL,
  slot INTEGER,
  block_time INTEGER,

  mint TEXT NOT NULL,
  user TEXT NOT NULL,
  side TEXT NOT NULL,              -- BUY | SELL | DEVBUY
  phase_u8 INTEGER,

  sol_in_gross TEXT,               -- u64 stored as string
  sol_eff_used TEXT,               -- u64 stored as string
  sol_gross TEXT,                  -- sell gross u64 stored as string
  sol_net TEXT,                    -- sell net u64 stored as string

  tokens_out TEXT,                 -- u64 stored as string
  tokens_in TEXT,                  -- u64 stored as string

  creator_fee TEXT,
  platform_fee TEXT,
  lp_fee TEXT,

  tokens_sold_total TEXT,          -- u64 stored as string
  sol_collected_total TEXT,        -- u128 stored as string

  ts_i64 TEXT                      -- event timestamp from program (string)
);

CREATE UNIQUE INDEX IF NOT EXISTS trades_sig_mint_side_idx
ON trades(sig, mint, side);

CREATE TABLE IF NOT EXISTS tx_seen (
  sig TEXT PRIMARY KEY,
  slot INTEGER,
  first_seen_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS trades_mint_idx ON trades(mint);
CREATE INDEX IF NOT EXISTS trades_user_idx ON trades(user);

-- -----------------------------
-- SOL price tables (latest + history)
-- -----------------------------
CREATE TABLE IF NOT EXISTS sol_price (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  price_usd REAL NOT NULL,
  source TEXT NOT NULL,
  pair TEXT,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sol_price_history (
  ts INTEGER PRIMARY KEY,
  price_usd REAL NOT NULL,
  source TEXT NOT NULL,
  pair TEXT
);
`);

// -----------------------------
// helpers
// -----------------------------
function now() {
  return Math.floor(Date.now() / 1000);
}

// -----------------------------
// tx_seen
// -----------------------------
const stmtMarkTxSeen = db.prepare(`
  INSERT OR IGNORE INTO tx_seen (sig, slot, first_seen_at)
  VALUES (?, ?, ?)
`);

const stmtHasSeenTx = db.prepare(`
  SELECT sig FROM tx_seen WHERE sig = ?
`);

function markTxSeen(sig, slot) {
  if (!sig) throw new Error("markTxSeen: sig required");
  stmtMarkTxSeen.run(sig, slot ?? null, now());
}

function hasSeenTx(sig) {
  if (!sig) return false;
  const r = stmtHasSeenTx.get(sig);
  return !!r;
}

// -----------------------------
// launches (upsert patch)
// -----------------------------
const stmtLaunchExists = db.prepare(`SELECT mint FROM launches WHERE mint = ?`);

function upsertLaunch(mint, patch) {
  if (!mint) throw new Error("upsertLaunch: mint required");
  if (!patch || typeof patch !== "object") return;

  const fields = Object.keys(patch).filter((k) => patch[k] !== undefined);
  if (!fields.length) return;

  const existing = stmtLaunchExists.get(mint);

  if (!existing) {
    const cols = ["mint", ...fields, "created_at", "updated_at"];
    const placeholders = cols.map(() => "?").join(",");
    const values = [mint, ...fields.map((k) => patch[k]), now(), now()];

    const q = `INSERT INTO launches (${cols.join(",")}) VALUES (${placeholders})`;
    db.prepare(q).run(...values);
  } else {
    const sets = fields.map((k) => `${k} = ?`).join(", ");
    const values = [...fields.map((k) => patch[k]), now(), mint];

    const q = `UPDATE launches SET ${sets}, updated_at = ? WHERE mint = ?`;
    db.prepare(q).run(...values);
  }
}

// -----------------------------
// trades
// -----------------------------
const stmtInsertTrade = db.prepare(`
  INSERT OR IGNORE INTO trades (
    sig, slot, block_time,
    mint, user, side, phase_u8,
    sol_in_gross, sol_eff_used, sol_gross, sol_net,
    tokens_out, tokens_in,
    creator_fee, platform_fee, lp_fee,
    tokens_sold_total, sol_collected_total,
    ts_i64
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
`);

function insertTrade(row) {
  if (!row) throw new Error("insertTrade: row required");
  if (!row.sig) throw new Error("insertTrade: row.sig required");
  if (!row.mint) throw new Error("insertTrade: row.mint required");
  if (!row.user) throw new Error("insertTrade: row.user required");
  if (!row.side) throw new Error("insertTrade: row.side required");

  stmtInsertTrade.run(
    row.sig,
    row.slot ?? null,
    row.block_time ?? null,

    row.mint,
    row.user,
    row.side,
    row.phase_u8 ?? null,

    row.sol_in_gross ?? null,
    row.sol_eff_used ?? null,
    row.sol_gross ?? null,
    row.sol_net ?? null,

    row.tokens_out ?? null,
    row.tokens_in ?? null,

    row.creator_fee ?? null,
    row.platform_fee ?? null,
    row.lp_fee ?? null,

    row.tokens_sold_total ?? null,
    row.sol_collected_total ?? null,

    row.ts_i64 ?? null
  );
}

// -----------------------------
// SOL price
// -----------------------------
const stmtUpsertSolPrice = db.prepare(`
  INSERT INTO sol_price (id, price_usd, source, pair, updated_at)
  VALUES (1, ?, ?, ?, ?)
  ON CONFLICT(id) DO UPDATE SET
    price_usd = excluded.price_usd,
    source = excluded.source,
    pair = excluded.pair,
    updated_at = excluded.updated_at
`);

const stmtInsertSolPriceHist = db.prepare(`
  INSERT OR REPLACE INTO sol_price_history (ts, price_usd, source, pair)
  VALUES (?, ?, ?, ?)
`);

const stmtGetSolPrice = db.prepare(`SELECT * FROM sol_price WHERE id = 1`);

function setSolPrice({ priceUsd, source, pair }) {
  if (typeof priceUsd !== "number" || !isFinite(priceUsd) || priceUsd <= 0) {
    throw new Error("setSolPrice: priceUsd must be a positive number");
  }
  if (!source) throw new Error("setSolPrice: source required");

  const ts = now();
  stmtUpsertSolPrice.run(priceUsd, source, pair || null, ts);
  stmtInsertSolPriceHist.run(ts, priceUsd, source, pair || null);
}

function getSolPrice() {
  return stmtGetSolPrice.get();
}

module.exports = {
  db,

  // tx_seen
  hasSeenTx,
  markTxSeen,

  // launches/trades
  upsertLaunch,
  insertTrade,

  // sol price
  setSolPrice,
  getSolPrice,
};
