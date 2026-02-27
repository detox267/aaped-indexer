const fs = require("fs");
const path = require("path");
const Database = require("better-sqlite3");

const TOKENS_DB = process.env.TOKENS_DB || path.join(__dirname, "tokens.db");
fs.mkdirSync(path.dirname(TOKENS_DB), { recursive: true });

const db = new Database(TOKENS_DB);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

db.exec(
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
  created_at INTEGER,
  updated_at INTEGER
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
  sol_eff_used TEXT,               -- u64 string
  sol_gross TEXT,                  -- sell gross u64 string
  sol_net TEXT,                    -- sell net u64 string

  tokens_out TEXT,                 -- u64 string
  tokens_in TEXT,                  -- u64 string

  creator_fee TEXT,
  platform_fee TEXT,
  lp_fee TEXT,

  tokens_sold_total TEXT,          -- u64 string
  sol_collected_total TEXT,        -- u128 string

  ts_i64 TEXT                       -- event ts from program (string)
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
);

function now() {
  return Math.floor(Date.now() / 1000);
}

function markTxSeen(sig, slot) {
  db.prepare(
    INSERT OR IGNORE INTO tx_seen (sig, slot, first_seen_at)
    VALUES (?, ?, ?)
  ).run(sig, slot ?? null, now());
}

function hasSeenTx(sig) {
  const r = db.prepare(SELECT sig FROM tx_seen WHERE sig = ?).get(sig);
  return !!r;
}

function upsertLaunch(mint, patch) {
  const existing = db.prepare(SELECT mint FROM launches WHERE mint = ?).get(mint);

  const fields = Object.keys(patch);
  if (!fields.length) return;

  if (!existing) {
    // build insert with defaults
    const cols = ["mint", ...fields, "created_at", "updated_at"];
    const vals = [mint, ...fields.map((k) => patch[k]), now(), now()];
    const q = INSERT INTO launches (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")});
    db.prepare(q).run(...vals);
  } else {
    const sets = fields.map((k) => ${k} = ?).join(", ");
    const vals = [...fields.map((k) => patch[k]), now(), mint];
    db.prepare(UPDATE launches SET ${sets}, updated_at = ? WHERE mint = ?).run(...vals);
  }
}

function insertTrade(row) {
  db.prepare(
    INSERT OR IGNORE INTO trades (
      sig, slot, block_time,
      mint, user, side, phase_u8,
      sol_in_gross, sol_eff_used, sol_gross, sol_net,
      tokens_out, tokens_in,
      creator_fee, platform_fee, lp_fee,
      tokens_sold_total, sol_collected_total,
      ts_i64
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  ).run(
    row.sig, row.slot ?? null, row.block_time ?? null,
    row.mint, row.user, row.side, row.phase_u8 ?? null,
    row.sol_in_gross ?? null, row.sol_eff_used ?? null, row.sol_gross ?? null, row.sol_net ?? null,
    row.tokens_out ?? null, row.tokens_in ?? null,
    row.creator_fee ?? null, row.platform_fee ?? null, row.lp_fee ?? null,
    row.tokens_sold_total ?? null, row.sol_collected_total ?? null,
    row.ts_i64 ?? null
  );
}

module.exports = {
  db,
  hasSeenTx,
  markTxSeen,
  upsertLaunch,
  insertTrade,
};
