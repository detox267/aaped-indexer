const { createFusionVolume } = require("./fusion-volume");

function createLeaderboards({ db, getPrice, fusion = createFusionVolume() }) {
  db.exec("CREATE INDEX IF NOT EXISTS trades_leaderboard_time ON trades(COALESCE(block_time, created_at))");
function leaderboardsParseTime(row = {}) {
  const raw =
    row.block_time ??
    row.blockTime ??
    row.created_at ??
    row.createdAt ??
    row.timestamp ??
    row.ts ??
    row.time ??
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

function leaderboardsNum(row = {}, keys = []) {
  for (const key of keys) {
    const v = row[key];
    if (v === undefined || v === null || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}


function leaderboardsQuoteAmountUi(row = {}) {
  const quoteAsset = String(
    row.quote_asset ||
    row.quoteAsset ||
    row.quote ||
    "SOL"
  ).toUpperCase();

  const ui = leaderboardsNum(row, [
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

  const raw = Math.abs(leaderboardsNum(row, [
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

function getTopTokenMeta(mint) {
  if (!mint) return {};
  const external = fusion.metadata(mint);
  if (external) return external;

  try {
    const meta = db.prepare(`
      SELECT *
      FROM token_stats
      WHERE mint = ?
      ORDER BY updated_at DESC
      LIMIT 1
    `).get(mint) || {};

    const first = db.prepare(`
      SELECT
        COALESCE(open_usd, close_usd) AS launch_price_usd
      FROM candles_1m
      WHERE mint = ?
        AND (
          (open_usd IS NOT NULL AND open_usd > 0)
          OR
          (close_usd IS NOT NULL AND close_usd > 0)
        )
      ORDER BY bucket_ts ASC
      LIMIT 1
    `).get(mint);

    const launchPriceUsd = Number(
      first?.launch_price_usd || 0
    );

    const currentPriceUsd = Number(
      meta.price_usd ||
      meta.priceUsd ||
      0
    );

    const change =
      launchPriceUsd > 0 &&
      currentPriceUsd > 0
        ? ((currentPriceUsd / launchPriceUsd) - 1) * 100
        : null;

    return {
      ...meta,
      launch_price_usd:
        launchPriceUsd > 0
          ? launchPriceUsd
          : null,
      price_change_since_launch_percent:
        Number.isFinite(change)
          ? change
          : null,
    };
  } catch (_err) {
    return {};
  }
}

function getPersistedTopToken() {
  try {
    const state = db.prepare(`
      SELECT *
      FROM top_token_state
      WHERE id = 1
      LIMIT 1
    `).get();

    if (!state?.mint) return null;

    const meta = getTopTokenMeta(state.mint);

    return {
      rank: 1,
      mint: state.mint,
      name:
        meta.name ||
        meta.launch_name ||
        meta.token_name ||
        "Unknown Token",
      symbol:
        meta.symbol ||
        meta.launch_symbol ||
        meta.token_symbol ||
        "",
      image:
        meta.image ||
        meta.launch_image ||
        meta.token_image ||
        "",
      price_usd: Number(meta.price_usd || meta.priceUsd || 0),
      marketcap_usd: Number(
        meta.marketcap_usd ||
        meta.marketCapUsd ||
        0
      ),
      price_change_24h_percent: Number(
        meta.price_change_24h_percent ||
        meta.priceChange24hPercent ||
        0
      ),
      launch_price_usd:
        meta.launch_price_usd != null
          ? Number(meta.launch_price_usd)
          : null,
      price_change_since_launch_percent:
        meta.price_change_since_launch_percent != null
          ? Number(meta.price_change_since_launch_percent)
          : null,
      hour_volume_usd: Number(state.round_volume_usd || 0),
      hour_volume_quote: Number(state.round_volume_quote || 0),
      trades_count: Number(state.trades_count || 0),
      buys_count: Number(state.buys_count || 0),
      sells_count: Number(state.sells_count || 0),
      last_trade_at: Number(state.last_trade_at || 0),
      round_start: Number(state.round_start || 0),
      round_end: Number(state.round_end || 0),
      persisted: true,
    };
  } catch (err) {
    console.warn("[top-token] persisted read failed:", err?.message || err);
    return null;
  }
}

function persistTopToken(token, roundStart, roundEnd) {
  if (!token?.mint) return false;

  try {
    db.prepare(`
      INSERT INTO top_token_state (
        id,
        mint,
        round_start,
        round_end,
        round_volume_usd,
        round_volume_quote,
        trades_count,
        buys_count,
        sells_count,
        last_trade_at,
        updated_at
      )
      VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id)
      DO UPDATE SET
        mint = excluded.mint,
        round_start = excluded.round_start,
        round_end = excluded.round_end,
        round_volume_usd = excluded.round_volume_usd,
        round_volume_quote = excluded.round_volume_quote,
        trades_count = excluded.trades_count,
        buys_count = excluded.buys_count,
        sells_count = excluded.sells_count,
        last_trade_at = excluded.last_trade_at,
        updated_at = excluded.updated_at
    `).run(
      String(token.mint),
      Number(roundStart),
      Number(roundEnd),
      Number(token.hour_volume_usd || 0),
      Number(token.hour_volume_quote || 0),
      Number(token.trades_count || 0),
      Number(token.buys_count || 0),
      Number(token.sells_count || 0),
      Number(token.last_trade_at || 0),
      Math.floor(Date.now() / 1000)
    );

    return true;
  } catch (err) {
    console.warn("[top-token] persisted write failed:", err?.message || err);
    return false;
  }
}

function buildKingRound(rows, roundStartMs, roundEndMs, solUsd) {
  const byMint = new Map();

  for (const row of rows) {
    const mint = String(
      row.mint ||
      row.token_mint ||
      row.tokenMint ||
      ""
    ).trim();

    if (!mint) continue;

    const ts = leaderboardsParseTime(row);

    if (
      !ts ||
      ts < roundStartMs ||
      ts >= roundEndMs
    ) {
      continue;
    }

    let volumeUsd = leaderboardsNum(row, [
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

    const quoteUi = leaderboardsQuoteAmountUi(row);

    if (row.source === "fusion" && volumeUsd <= 0) continue;
    // Preserve the USD conversion observed with the legacy trade when available.
    if (volumeUsd <= 0 && row.price_usd > 0 && row.price_quote > 0)
      volumeUsd = quoteUi * row.price_usd / row.price_quote;
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
        volumeUsd = solUsd > 0 ? quoteUi * solUsd : 0;
      }

    }

    if (!(volumeUsd > 0) || !Number.isFinite(volumeUsd)) continue;

    const side = String(
      row.side ||
      row.trade_side ||
      row.type ||
      ""
    ).toLowerCase();

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

    prev.last_trade_at = Math.max(
      prev.last_trade_at || 0,
      ts
    );

    byMint.set(mint, prev);
  }

  return [...byMint.values()]
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
      const meta = getTopTokenMeta(item.mint);

      return {
        rank: index + 1,
        mint: item.mint,
        name:
          meta.name ||
          meta.launch_name ||
          meta.token_name ||
          "Unknown Token",
        symbol:
          meta.symbol ||
          meta.launch_symbol ||
          meta.token_symbol ||
          "",
        image:
          meta.image ||
          meta.launch_image ||
          meta.token_image ||
          "",
        price_usd: Number(
          meta.price_usd ||
          meta.priceUsd ||
          0
        ),
        marketcap_usd: Number(
          meta.marketcap_usd ||
          meta.marketCapUsd ||
          0
        ),
        price_change_24h_percent: Number(
          meta.price_change_24h_percent ||
          meta.priceChange24hPercent ||
          0
        ),
        launch_price_usd:
          meta.launch_price_usd != null
            ? Number(meta.launch_price_usd)
            : null,
        price_change_since_launch_percent:
          meta.price_change_since_launch_percent != null
            ? Number(meta.price_change_since_launch_percent)
            : null,
        hour_volume_usd: Number(
          item.hour_volume_usd || 0
        ),
        hour_volume_quote: Number(
          item.hour_volume_quote || 0
        ),
        trades_count: Number(item.trades_count || 0),
        buys_count: Number(item.buys_count || 0),
        sells_count: Number(item.sells_count || 0),
        last_trade_at: item.last_trade_at || 0,
      };
    });
}

function getLeaderboardsSnapshot() {
  const now = Date.now();

  const hourStart = new Date(now);
  hourStart.setMinutes(0, 0, 0);

  const hourStartMs = hourStart.getTime();
  const hourEndMs = hourStartMs + 3600000;

  const previousStartMs = hourStartMs - 3600000;
  const previousEndMs = hourStartMs;

  let rows = [];

  try {
    rows = db.prepare(`
      SELECT *
      FROM trades
      WHERE COALESCE(block_time, created_at) >= ?
        AND COALESCE(block_time, created_at) < ?
      ORDER BY COALESCE(block_time, created_at), id
    `).all(previousStartMs / 1000, hourEndMs / 1000);
  } catch (err) {
    console.error(
      "[top-token] trades read failed:",
      err?.message || err
    );

    const persisted = getPersistedTopToken();

    return {
      hour_start: hourStartMs,
      hour_end: hourEndMs,
      top10: [],
      top3: [],
      top_token: persisted,
      top_token_source: persisted
        ? "completed_round"
        : "none",
    };
  }

  const fusionCurrent = fusion.snapshot(previousStartMs, now);
  const fusionPrevious = fusion.snapshot(previousStartMs, previousEndMs);
  // A mint present in both registries has one authoritative trade source.
  rows = rows.filter(row => !fusionCurrent.mints.has(row.mint)).concat(fusionCurrent.rows);

  const solUsd =
    typeof getPrice === "function"
      ? Number(getPrice("SOL_USD")?.price || 0)
      : 0;

  // Current hour remains the live ranking.
  const top10 = buildKingRound(
    rows,
    hourStartMs,
    hourEndMs,
    solUsd
  );

  // The featured Top Token comes from the completed hour.
  const previousTop10 = buildKingRound(
    rows,
    previousStartMs,
    previousEndMs,
    solUsd
  );

  const previousWinner = previousTop10[0] || null;
  const storedBefore = getPersistedTopToken();

  // Only advance persistent state once a completed round
  // actually has a valid winner.
  if (
    previousWinner?.mint &&
    fusionPrevious.ready && now >= previousEndMs + 90000 &&
    Number(storedBefore?.round_start || 0) <= previousStartMs
  ) {
    persistTopToken(
      previousWinner,
      previousStartMs,
      previousEndMs
    );
  }

  const persisted = getPersistedTopToken();

  // Bootstrap only: if Moonz has never stored a completed
  // winner yet, show the current leader rather than a blank card.
  // It is not persisted until its round actually completes.
  const bootstrap = !persisted
    ? top10[0] || null
    : null;

  const topToken = persisted || bootstrap || null;

  return {
    hour_start: hourStartMs,
    hour_end: hourEndMs,
    fusion_volume: { enabled: fusionCurrent.enabled, unavailable: !!fusionCurrent.unavailable,
      completed_round_pending: !fusionPrevious.ready || now < previousEndMs + 90000,
      unpriced_trades: fusionCurrent.unpriced_trades || 0 },
    top10,
    top3: top10.slice(0, 3),
    top_token: topToken,
    top_token_source: persisted
      ? "completed_round"
      : bootstrap
        ? "bootstrap_current"
        : "none",
  };
}

return { getLeaderboardsSnapshot, buildKingRound, getTopTokenMeta, getPersistedTopToken };
}
module.exports = { createLeaderboards };
