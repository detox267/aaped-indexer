// indexer.js
const { Connection, PublicKey } = require("@solana/web3.js");
const { makeEventDecoder } = require("./anchorDecode");
const { loadIdl } = require("./idl");
const { hasSeenTx, markTxSeen, upsertLaunch, insertTrade } = require("./db");

const PROGRAM_ID = new PublicKey(process.env.PROGRAM_ID);
const RPC_URL = process.env.RPC_URL;
if (!RPC_URL) throw new Error("Missing RPC_URL");

const connection = new Connection(RPC_URL, { commitment: "confirmed" });

function bnToString(v) {
  if (v == null) return null;
  // Anchor BN objects look like { toString() }
  if (typeof v === "object" && typeof v.toString === "function") return v.toString();
  return String(v);
}

function pkToString(v) {
  if (!v) return null;
  if (typeof v === "string") return v;
  if (typeof v.toBase58 === "function") return v.toBase58();
  return String(v);
}

function buildTradeRow({ sig, slot, block_time, name, data }) {
  // normalize common
  const base = {
    sig,
    slot,
    block_time,
    mint: pkToString(data.mint),
    user: pkToString(data.user),
    phase_u8: data.phase ?? null,
    ts_i64: bnToString(data.ts),
  };

  if (name === "BuyExecuted") {
    return {
      ...base,
      side: "BUY",
      sol_in_gross: bnToString(data.sol_in_gross),
      sol_eff_used: bnToString(data.sol_eff_used),
      tokens_out: bnToString(data.tokens_out),
      creator_fee: bnToString(data.creator_fee),
      platform_fee: bnToString(data.platform_fee),
      lp_fee: bnToString(data.lp_fee),
      tokens_sold_total: bnToString(data.tokens_sold_total),
      sol_collected_total: bnToString(data.sol_collected_total),
    };
  }

  if (name === "SellExecuted") {
    return {
      ...base,
      side: "SELL",
      sol_gross: bnToString(data.sol_gross),
      sol_net: bnToString(data.sol_net),
      tokens_in: bnToString(data.tokens_in),
      creator_fee: bnToString(data.creator_fee),
      platform_fee: bnToString(data.platform_fee),
      lp_fee: bnToString(data.lp_fee),
      tokens_sold_total: bnToString(data.tokens_sold_total),
      sol_collected_total: bnToString(data.sol_collected_total),
    };
  }

  // DEV buy is both CurveActivated + BuyExecuted.
  // We store DEVBUY based on CurveActivated (clean) and still store BuyExecuted as BUY (optional).
  if (name === "CurveActivated") {
    return {
      sig,
      slot,
      block_time,
      mint: pkToString(data.mint),
      user: pkToString(data.dev),
      side: "DEVBUY",
      phase_u8: null,
      sol_in_gross: bnToString(data.sol_in_gross),
      tokens_out: bnToString(data.tokens_out),
      ts_i64: bnToString(data.ts),
    };
  }

  return null;
}

function applyLaunchUpserts(ev) {
  const { name, data } = ev;

  if (name === "LaunchInitialized") {
    upsertLaunch(pkToString(data.mint), {
      launch_state: pkToString(data.launch_state),
      creator: pkToString(data.creator),
      platform: pkToString(data.platform),
      core_authority: pkToString(data.core_authority),
      total_supply: bnToString(data.total_supply),
      sale_supply: bnToString(data.sale_supply),
      lp_supply: bnToString(data.lp_supply),
      state_u8: 0,
    });
  }

  if (name === "MetadataInitialized") {
    upsertLaunch(pkToString(data.mint), {
      name: data.name,
      symbol: data.symbol,
      metadata_uri: data.uri,
    });
  }

  if (name === "AuthoritiesFinalized") {
    upsertLaunch(pkToString(data.mint), { state_u8: 255 }); // mark live/immutable
  }

  if (name === "MigrationPending") {
    upsertLaunch(pkToString(data.mint), { state_u8: 200 });
  }

  if (name === "MigratedToCore") {
    upsertLaunch(pkToString(data.mint), { state_u8: 250 });
  }
}

async function startIndexer({ broadcast }) {
  const idl = loadIdl();
  const decodeEvents = makeEventDecoder(idl);

  // logsSubscribe via web3.js:
  const subId = connection.onLogs(
    PROGRAM_ID,
    async (logInfo) => {
      const sig = logInfo.signature;
      const slot = logInfo.slot;

      try {
        if (hasSeenTx(sig)) return;
        markTxSeen(sig, slot);

        // fetch parsed tx (we want logMessages + blockTime)
        const tx = await connection.getTransaction(sig, {
          commitment: "confirmed",
          maxSupportedTransactionVersion: 0,
        });

        const block_time = tx?.blockTime ?? null;
        const logs = tx?.meta?.logMessages || logInfo.logs || [];

        const events = decodeEvents(logs);

        for (const ev of events) {
          applyLaunchUpserts(ev);

          const row = buildTradeRow({
            sig,
            slot,
            block_time,
            name: ev.name,
            data: ev.data,
          });

          if (row) insertTrade(row);

          // broadcast raw event to website clients
          if (broadcast) {
            broadcast({
              type: "event",
              signature: sig,
              slot,
              event: ev.name,
              data: ev.data,
              blockTime: block_time,
            });
          }
        }
      } catch (e) {
        if (broadcast) {
          broadcast({
            type: "indexer_error",
            signature: sig,
            slot,
            error: e.message || String(e),
          });
        }
      }
    },
    "confirmed"
  );

  return {
    subId,
    stop: async () => {
      await connection.removeOnLogsListener(subId);
    },
  };
}

module.exports = { startIndexer };
