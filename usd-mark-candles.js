"use strict";

/*
 * MOONZ USD MARK CANDLES
 *
 * Purpose:
 * Persist mark-to-market USD movement between token trades.
 *
 * SOL quoted tokens:
 *   token USD = token SOL x live SOL/USD
 *
 * USDC quoted tokens:
 *   token USD = token USDC quote
 *
 * Mark samples NEVER add:
 *   volume
 *   trades
 *   buys
 *   sells
 *
 * Existing trade candles remain canonical for trading activity.
 */

const SAMPLE_INTERVAL_MS = 15_000;
const MAX_SOL_USD_AGE_SECONDS = 600;

function positive(value) {
  const n = Number(value);

  return Number.isFinite(n) && n > 0
    ? n
    : 0;
}

function unixNow() {
  return Math.floor(Date.now() / 1000);
}

function minuteBucket(ts = unixNow()) {
  return Math.floor(ts / 60) * 60;
}

function isUsdcQuote(value) {
  const quote = String(value || "")
    .trim()
    .toUpperCase();

  return (
    quote === "USDC" ||
    quote.includes("USDC") ||
    quote ===
      "EPJFWDD5AUFQSSQEM2QN1XZYBAPC8G4WEGGKZWYDT1V"
  );
}

function deriveMark(row, solUsd) {
  const storedQuote =
    positive(row?.price_quote);

  const storedSol =
    positive(row?.price_sol);

  const storedUsd =
    positive(row?.price_usd);

  let priceSol = 0;
  let priceUsd = 0;

  if (isUsdcQuote(row?.quote_asset)) {
    priceUsd =
      storedQuote ||
      storedUsd;

    if (
      priceUsd > 0 &&
      solUsd > 0
    ) {
      priceSol =
        priceUsd / solUsd;
    }
  } else {
    priceSol =
      storedSol ||
      storedQuote ||
      (
        storedUsd > 0 &&
        solUsd > 0
          ? storedUsd / solUsd
          : 0
      );

    if (
      priceSol > 0 &&
      solUsd > 0
    ) {
      priceUsd =
        priceSol * solUsd;
    }
  }

  if (
    priceSol <= 0 ||
    priceUsd <= 0
  ) {
    return null;
  }

  return {
    priceSol,
    priceUsd,
  };
}

function startUsdMarkCandleWorker({
  db,
  getPrice,
}) {
  if (!db) {
    throw new Error(
      "USD mark candle worker requires db"
    );
  }

  if (typeof getPrice !== "function") {
    throw new Error(
      "USD mark candle worker requires getPrice"
    );
  }

  const tokenRows = db.prepare(`
    SELECT
      mint,
      phase,
      phase_u8,
      quote_asset,
      price_quote,
      price_sol,
      price_usd
    FROM token_stats
    WHERE
      COALESCE(phase_u8, -1) <> 4
      AND LOWER(
        COALESCE(phase, '')
      ) <> 'migrated'
      AND (
        COALESCE(price_sol, 0) > 0
        OR COALESCE(price_usd, 0) > 0
        OR COALESCE(price_quote, 0) > 0
      )
      AND EXISTS (
        SELECT 1
        FROM candles_1m c
        WHERE c.mint = token_stats.mint
        LIMIT 1
      )
  `);

  const currentCandle = db.prepare(`
    SELECT *
    FROM candles_1m
    WHERE mint = ?
      AND bucket_ts = ?
  `);

  const previousCandle = db.prepare(`
    SELECT
      bucket_ts,
      close_sol,
      close_usd
    FROM candles_1m
    WHERE mint = ?
      AND bucket_ts < ?
      AND close_sol IS NOT NULL
      AND close_sol > 0
    ORDER BY bucket_ts DESC
    LIMIT 1
  `);

  const insertCandle = db.prepare(`
    INSERT INTO candles_1m (
      mint,
      bucket_ts,

      open_sol,
      high_sol,
      low_sol,
      close_sol,

      open_usd,
      high_usd,
      low_usd,
      close_usd,

      volume_quote,
      volume_sol,
      volume_usd,
      volume_tokens,

      trades_count,
      buys_count,
      sells_count,

      updated_at
    )
    VALUES (
      ?, ?,

      ?, ?, ?, ?,

      ?, ?, ?, ?,

      0,
      0,
      0,
      0,

      0,
      0,
      0,

      ?
    )
  `);

  /*
   * IMPORTANT:
   *
   * This UPDATE intentionally contains no volume or trade
   * count columns.
   */
  const updateCandle = db.prepare(`
    UPDATE candles_1m
    SET
      open_sol = ?,
      high_sol = ?,
      low_sol = ?,
      close_sol = ?,

      open_usd = ?,
      high_usd = ?,
      low_usd = ?,
      close_usd = ?,

      updated_at = ?

    WHERE mint = ?
      AND bucket_ts = ?
  `);

  const writeMarks = db.transaction(
    (
      rows,
      bucket,
      timestamp,
      solUsd
    ) => {
      let inserted = 0;
      let updated = 0;
      let sampled = 0;

      for (const row of rows) {
        const mark =
          deriveMark(
            row,
            solUsd
          );

        if (!mark) {
          continue;
        }

        const {
          priceSol,
          priceUsd,
        } = mark;

        const existing =
          currentCandle.get(
            row.mint,
            bucket
          );

        if (existing) {
          const openSol =
            positive(
              existing.open_sol
            ) || priceSol;

          const highSol =
            Math.max(
              positive(
                existing.high_sol
              ) || openSol,
              openSol,
              priceSol
            );

          const lowSol =
            Math.min(
              positive(
                existing.low_sol
              ) || openSol,
              openSol,
              priceSol
            );

          const openUsd =
            positive(
              existing.open_usd
            ) || priceUsd;

          const highUsd =
            Math.max(
              positive(
                existing.high_usd
              ) || openUsd,
              openUsd,
              priceUsd
            );

          const lowUsd =
            Math.min(
              positive(
                existing.low_usd
              ) || openUsd,
              openUsd,
              priceUsd
            );

          updateCandle.run(
            openSol,
            highSol,
            lowSol,
            priceSol,

            openUsd,
            highUsd,
            lowUsd,
            priceUsd,

            timestamp,

            row.mint,
            bucket
          );

          updated += 1;
          sampled += 1;

          continue;
        }

        const previous =
          previousCandle.get(
            row.mint,
            bucket
          );

        /*
         * Only inherit the previous close if it is the
         * immediately preceding minute.
         *
         * This prevents an old historical gap from being
         * represented as one giant current candle when the
         * mark worker is first deployed.
         */
        const previousIsAdjacent =
          Number(
            previous?.bucket_ts
          ) === bucket - 60;

        const openSol =
          previousIsAdjacent
            ? (
                positive(
                  previous?.close_sol
                ) || priceSol
              )
            : priceSol;

        const openUsd =
          previousIsAdjacent
            ? (
                positive(
                  previous?.close_usd
                ) || priceUsd
              )
            : priceUsd;

        insertCandle.run(
          row.mint,
          bucket,

          openSol,
          Math.max(
            openSol,
            priceSol
          ),
          Math.min(
            openSol,
            priceSol
          ),
          priceSol,

          openUsd,
          Math.max(
            openUsd,
            priceUsd
          ),
          Math.min(
            openUsd,
            priceUsd
          ),
          priceUsd,

          timestamp
        );

        inserted += 1;
        sampled += 1;
      }

      return {
        sampled,
        inserted,
        updated,
      };
    }
  );

  let firstSuccess = true;

  function run() {
    try {
      const timestamp =
        unixNow();

      const solRow =
        getPrice("SOL_USD");

      const solUsd =
        positive(
          solRow?.price
        );

      const solUpdatedAt =
        Number(
          solRow?.updated_at || 0
        );

      const solAge =
        timestamp -
        solUpdatedAt;

      if (
        solUsd <= 0 ||
        solUpdatedAt <= 0 ||
        solAge >
          MAX_SOL_USD_AGE_SECONDS
      ) {
        console.warn(
          "[usd-mark-candles] skipped: stale SOL/USD",
          {
            sol_usd: solUsd,
            age_seconds: solAge,
          }
        );

        return;
      }

      const bucket =
        minuteBucket(
          timestamp
        );

      const rows =
        tokenRows.all();

      const result =
        writeMarks(
          rows,
          bucket,
          timestamp,
          solUsd
        );

      if (firstSuccess) {
        firstSuccess = false;

        console.log(
          "[usd-mark-candles] active",
          {
            interval_ms:
              SAMPLE_INTERVAL_MS,
            sol_usd:
              solUsd,
            tokens:
              rows.length,
            sampled:
              result.sampled,
            inserted:
              result.inserted,
            updated:
              result.updated,
          }
        );
      }
    } catch (err) {
      console.error(
        "[usd-mark-candles] failed:",
        err?.stack ||
        err?.message ||
        err
      );
    }
  }

  /*
   * First run shortly after indexer startup.
   * Then sample four times per minute.
   */
  const initialTimer =
    setTimeout(
      run,
      2_000
    );

  const intervalTimer =
    setInterval(
      run,
      SAMPLE_INTERVAL_MS
    );

  return () => {
    clearTimeout(
      initialTimer
    );

    clearInterval(
      intervalTimer
    );
  };
}

module.exports = {
  startUsdMarkCandleWorker,
};
