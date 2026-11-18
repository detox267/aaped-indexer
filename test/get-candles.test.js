"use strict";

const assert = require("node:assert/strict");
const { after, test } = require("node:test");

process.env.TOKENS_DB = ":memory:";
const { db, getCandles } = require("../db");
after(() => db.close());

const insert = db.prepare(`
  INSERT INTO candles_1m (
    mint, bucket_ts, open_sol, high_sol, low_sol, close_sol,
    open_usd, high_usd, low_usd, close_usd,
    volume_quote, volume_sol, volume_usd, volume_tokens,
    trades_count, buys_count, sells_count, updated_at
  ) VALUES (
    @mint, @ts, @open, @high, @low, @close, @openUsd, @highUsd, @lowUsd, @closeUsd,
    @vol, @vol * 2, @vol * 3, @vol * 4, @trades, @buys, @sells, @ts
  )
`);

function put(mint, ts, overrides = {}) {
  insert.run({
    mint, ts, open: 10, high: 12, low: 8, close: 11,
    openUsd: 100, highUsd: 120, lowUsd: 80, closeUsd: 110,
    vol: 0.5, trades: 2, buys: 1, sells: 1, ...overrides,
  });
}

test("rolls up every qualifying row in the oldest requested bucket", () => {
  const mint = "irregular";
  const start = 900_000;
  put(mint, 900); // A much older populated bucket.
  // More rows here than a 15-minute bucket could hold on a minute grid.
  for (let i = 1; i <= 20; i++) put(mint, start + i);
  put(mint, start, { open: 5, openUsd: null, high: 50, highUsd: 500 });
  put(mint, start + 27, { close: 7, closeUsd: null, low: 2, lowUsd: 20 });
  put(mint, start + 899, { open: null, high: 800, vol: 2000 });
  put("other-mint", start + 898, { high: 1000 });
  put(mint, start + 901, { low: -1 }); // A bucket with only invalid rows.
  put(mint, start + 1801, { vol: 0, trades: 0, buys: 0, sells: 0 }); // USD mark.
  put(mint, start + 1802, { high: 0 });
  put(mint, start + 2700, { close: null });

  const read = (options = {}) => getCandles({ mint, interval: "15m", limit: 2, ...options });
  const [first, mark] = read();
  assert.deepEqual(first, {
    bucket_ts: start, open_sol: 5, high_sol: 50, low_sol: 2, close_sol: 7,
    open_usd: null, high_usd: 500, low_usd: 20, close_usd: null,
    volume_quote: 11, volume_sol: 22, volume_usd: 33, volume_tokens: 44,
    trades_count: 44, buys_count: 22, sells_count: 22,
  });
  assert.deepEqual(mark, {
    bucket_ts: start + 1800, open_sol: 10, high_sol: 12, low_sol: 8, close_sol: 11,
    open_usd: 100, high_usd: 120, low_usd: 80, close_usd: 110,
    volume_quote: 0, volume_sol: 0, volume_usd: 0, volume_tokens: 0,
    trades_count: 0, buys_count: 0, sells_count: 0,
  });
  assert.deepEqual(read({ limit: 1 }), [mark]);
  assert.deepEqual(read({ since: start + 18 }), [{
    ...first, open_sol: 10, open_usd: 100, high_sol: 12, high_usd: 120,
    volume_quote: 2, volume_sol: 4, volume_usd: 6, volume_tokens: 8,
    trades_count: 8, buys_count: 4, sells_count: 4,
  }, mark]);
  assert.deepEqual(read({ since: start + 1802 }), []);
  assert.deepEqual(read({ mint: "missing" }), []);
  assert.deepEqual(read({ limit: 32 }).map(row => row.bucket_ts), [900, start, mark.bucket_ts]);
});

test("selects complete buckets for each supported rollup across the Unix epoch", () => {
  for (const [interval, seconds] of Object.entries({
    "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400,
  })) {
    for (const ts of [-seconds * 3, -seconds * 2 + 1, -seconds, -seconds + 1, 0, seconds - 1]) {
      put(interval, ts);
    }
    const rows = getCandles({ mint: interval, interval, limit: 2 });
    assert.deepEqual(rows.map(row => [row.bucket_ts, row.trades_count]), [[-seconds, 4], [0, 6]]);
  }
});

test("preserves the default, minimum and maximum result counts", () => {
  const mint = "long";
  db.transaction(() => {
    for (let i = 1; i <= 5001; i++) put(mint, i * 300);
  })();
  const read = limit => getCandles({ mint, interval: "5m", limit });
  assert.equal(read(0).length, 500);
  assert.deepEqual(read(-2).map(row => row.bucket_ts), [5001 * 300]);
  const capped = read(7000);
  assert.equal(capped.length, 5000);
  assert.equal(capped[0].bucket_ts, 600);
  assert.equal(capped.at(-1).bucket_ts, 5001 * 300);
});
