require("dotenv").config();

const { db } = require("./db");

const INDEXER_BASE =
  process.env.PUBLIC_INDEXER_BASE ||
  "https://indexer.moonz.fun";

const variants = ["thumb", "card", ""];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function warmUrl(url) {
  const res = await fetch(url, {
    method: "GET",
    headers: {
      "User-Agent": "MoonzMediaCacheWarmer/1.0",
    },
  });

  const buffer = Buffer.from(await res.arrayBuffer());

  return {
    ok: res.ok,
    status: res.status,
    bytes: buffer.length,
    contentType: res.headers.get("content-type"),
  };
}

async function main() {
  const tokens = db.prepare(`
    SELECT mint
    FROM token_stats
    WHERE mint IS NOT NULL
    ORDER BY updated_at DESC
  `).all();

  console.log(`Found ${tokens.length} tokens`);

  let done = 0;
  let failed = 0;

  for (const row of tokens) {
    const mint = row.mint;

    for (const variant of variants) {
      const path = variant
        ? `/media/token/${encodeURIComponent(mint)}/${variant}`
        : `/media/token/${encodeURIComponent(mint)}`;

      const url = `${INDEXER_BASE.replace(/\/+$/, "")}${path}`;

      try {
        const result = await warmUrl(url);

        if (!result.ok) {
          failed += 1;
          console.warn(`[FAIL] ${mint} ${variant || "default"} ${result.status}`);
        } else {
          done += 1;
          console.log(
            `[OK] ${mint} ${variant || "default"} ${Math.round(result.bytes / 1024)}KB ${result.contentType || ""}`
          );
        }
      } catch (err) {
        failed += 1;
        console.warn(`[ERR] ${mint} ${variant || "default"} ${err?.message || err}`);
      }

      await sleep(150);
    }
  }

  console.log(`Done. Warmed=${done} Failed=${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
