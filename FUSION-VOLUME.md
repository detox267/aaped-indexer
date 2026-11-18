# Shared hourly rankings

Deploy together with the Fusion volume worker in `detox267/aaped-api`.
Set `MOONZ_FUSION_DB_PATH` to that worker's absolute `LAUNCHLAB_DB` filename in
both the indexer and HTTP server environments. The connection is read-only;
it requires access to the SQLite file and WAL sidecars on the same host/volume.
Restart both services after deployment. No new gateway event is required.

`leaderboards.js` is shared by the HTTP snapshots and Redis publisher. It merges
legacy trade rows with verified Fusion trades in USD, keeps the current-hour
Top 10 and previous-completed-hour featured winner, and retains winners through
quiet periods. Existing tie breakers remain volume, trade count, then latest
trade. A mint registered in Fusion uses that ledger alone, preventing duplicate
counting if it also appears in the legacy registry.

The legacy query uses block time (created time only when unavailable), includes
the entire two-hour window rather than the latest 20,000 rows, and uses a trade's
recorded USD/quote price ratio where present. Older missing-price rows retain
the existing USDC/current-SOL conversion fallback; missing SOL pricing is never
treated as dollar parity. Fusion never receives this legacy fallback.

Hourly winner settlement waits 90 seconds and full Fusion coverage. The optional
`fusion_volume` response reports catch-up, missing prices and source availability.
A configured but unavailable Fusion database blocks choosing a new completed
winner; the previous winner and v1 pages remain available. With no path configured,
the indexer remains in legacy-only mode. It does **not** silently connect Fusion.

Both candle routes and the homepage Top Token snapshot can return Fusion OHLC
from recorded, priced swaps. Empty time buckets are not manufactured. The full
leaderboard snapshot participates in Redis change detection, so changes below
third place and changes in featured-token metadata are also published.

Run `node --test test/*.test.js`. After rollout, verify real swaps in both the
HTTP snapshot and `leaderboards.updated`, and confirm the first completely
priced, covered hour settles. First startup cannot reconstruct USD history that
predates the backend's stored reference observations. See the API's
`launchlab/VOLUME.md` for worker accounting and recovery details.
