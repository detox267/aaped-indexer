require("dotenv").config();

const WebSocket = require("ws");
const anchor = require("@coral-xyz/anchor");
const { Connection, PublicKey } = require("@solana/web3.js");
const { loadIdl } = require("./idl");
const { makeEventDecoder } = require("./anchorDecode");
const {
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
  upsertCandle1m,
  refresh24hVolume,
  getToken,
  upsertHolderBalance,
} = require("./db");

const fetch = globalThis.fetch || require("node-fetch");

const PROGRAM_ID = process.env.PROGRAM_ID;
const RPC_URL = process.env.RPC_URL;
const HELIUS_WSS =
  process.env.HELIUS_WSS ||
  (process.env.HELIUS_API_KEY
    ? `wss://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`
    : null);

if (!PROGRAM_ID) throw new Error("Missing PROGRAM_ID");
if (!RPC_URL) throw new Error("Missing RPC_URL");
if (!HELIUS_WSS) throw new Error("Missing HELIUS_WSS or HELIUS_API_KEY");

const PROGRAM_PK = new PublicKey(PROGRAM_ID);
const connection = new Connection(RPC_URL, { commitment: "confirmed" });
const idl = loadIdl();
const decodeEventsFromLogs = makeEventDecoder(idl);

const TOKEN_DECIMALS = Number(process.env.TOKEN_DECIMALS || 6);
const TOKEN_SCALE = 10 ** TOKEN_DECIMALS;
const SOL_DECIMALS = 9;
const LAMPORTS_PER_SOL = 1_000_000_000;

const LAUNCH_API_BASE = process.env.LAUNCH_API_BASE || "http://127.0.0.1:3000";
const PUBLIC_INDEXER_BASE =
  process.env.PUBLIC_INDEXER_BASE || "https://indexer.moonz.fun";

const TOTAL_SUPPLY_TOKENS = Number(process.env.TOTAL_SUPPLY_TOKENS || 1_000_000_000);
const SALE_SUPPLY_TOKENS = Number(process.env.SALE_SUPPLY_TOKENS || 650_000_000);
const LP_SUPPLY_TOKENS = Number(process.env.LP_SUPPLY_TOKENS || 350_000_000);

const TOTAL_SUPPLY_BASE = BigInt(process.env.TOTAL_SUPPLY_BASE || String(TOTAL_SUPPLY_TOKENS * TOKEN_SCALE));
const SALE_SUPPLY_BASE = BigInt(process.env.SALE_SUPPLY_BASE || String(SALE_SUPPLY_TOKENS * TOKEN_SCALE));
const LP_SUPPLY_BASE = BigInt(process.env.LP_SUPPLY_BASE || String(LP_SUPPLY_TOKENS * TOKEN_SCALE));

const V_SOL_LAMPORTS = BigInt(process.env.V_SOL_LAMPORTS || String(117 * LAMPORTS_PER_SOL));
const V_TOK_BASE = BigInt(process.env.V_TOK_BASE || String(760_000_000 * TOKEN_SCALE));

const TRADE_FEE_BPS = Number(process.env.TRADE_FEE_BPS || 125);

const WSOL_MINT = process.env.WSOL_MINT || "So11111111111111111111111111111111111111112";
const USDC_MINT = process.env.USDC_MINT || "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const PLATFORM_WALLET = process.env.PLATFORM_WALLET || "ELZ5aiHLxnaTmbazgbmoSCVS6SyvJ7DbXTDxq682PuKt";

const ENABLE_GLOBAL_EVENTS = process.env.ENABLE_GLOBAL_EVENTS === "true";
const STARTUP_BACKFILL_SIGNATURES = Number(process.env.STARTUP_BACKFILL_SIGNATURES || 0);
const TX_FETCH_RETRIES = Number(process.env.TX_FETCH_RETRIES || 8);
const TX_FETCH_DELAY_MS = Number(process.env.TX_FETCH_DELAY_MS || 750);

const PHASE_BY_U8 = {
  0: "pending_dev_buy",
  1: "bonding",
  2: "migration_pending",
  3: "amm_live",
  4: "migrated",
  5: "switching",
};

const QUOTE_BY_U8 = {
  0: "SOL",
  1: "USDC",
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stringifySafe(value) {
  return JSON.stringify(value, (_, val) => {
    if (typeof val === "bigint") return val.toString();
    if (val && typeof val === "object") {
      if (typeof val.toBase58 === "function") return val.toBase58();
      if (val.constructor?.name === "BN") return val.toString();
    }
    return val;
  });
}

async function fetchLaunchMetadataFromApi(mint) {
  if (!mint || !LAUNCH_API_BASE) return null;

  try {
    const url = `${LAUNCH_API_BASE.replace(/\/+$/, "")}/launch/${encodeURIComponent(mint)}`;
    const res = await fetch(url);

    if (!res.ok) {
      return null;
    }

    const json = await res.json().catch(() => null);
    if (!json) return null;

    return {
      name: json.name || null,
      symbol: json.symbol || null,
      description: json.description || null,
      image: json.image || null,
      metadata_uri: json.metadata_uri || json.metadataUri || null,
      pinata_cid: json.pinata_cid || json.pinataCid || null,
      creator: json.creator || json.depositor || null,
    };
  } catch (err) {
    console.warn(`metadata fetch failed for ${mint}:`, err?.message || err);
    return null;
  }
}

function indexerMediaUrl(mint) {
  if (!mint) return null;
  return `${PUBLIC_INDEXER_BASE.replace(/\/+$/, "")}/media/token/${encodeURIComponent(mint)}`;
}

function toBase58Maybe(value) {
  if (!value) return null;
  if (typeof value === "string") return value;
  if (typeof value.toBase58 === "function") return value.toBase58();
  if (value._bn && typeof value.toString === "function") return value.toString();
  return String(value);
}

function toBigIntMaybe(value, fallback = 0n) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "bigint") return value;
  if (typeof value === "number") return BigInt(Math.trunc(value));
  if (typeof value === "string") {
    if (!value.trim()) return fallback;
    return BigInt(value);
  }
  if (typeof value.toString === "function") return BigInt(value.toString());
  return fallback;
}

function bigIntToString(value) {
  if (value === null || value === undefined) return null;
  return toBigIntMaybe(value).toString();
}

function baseToUi(base, decimals = TOKEN_DECIMALS) {
  const n = Number(toBigIntMaybe(base, 0n));
  return n / 10 ** decimals;
}

function quoteBaseToUi(base, quoteAsset) {
  if (quoteAsset === "USDC") return baseToUi(base, 6);
  return Number(toBigIntMaybe(base, 0n)) / LAMPORTS_PER_SOL;
}

function lamportsToSol(lamports) {
  return Number(toBigIntMaybe(lamports, 0n)) / LAMPORTS_PER_SOL;
}

function derivePdas(mintStr) {
  const mint = new PublicKey(mintStr);

  const [launchState] = PublicKey.findProgramAddressSync(
    [Buffer.from("launch_state"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [launchEscrow] = PublicKey.findProgramAddressSync(
    [Buffer.from("launch_escrow"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [escrowSolVault] = PublicKey.findProgramAddressSync(
    [Buffer.from("escrow_sol"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [saleVault] = PublicKey.findProgramAddressSync(
    [Buffer.from("sale_vault"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [lpVault] = PublicKey.findProgramAddressSync(
    [Buffer.from("lp_vault"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [treasuryWsolVault] = PublicKey.findProgramAddressSync(
    [Buffer.from("treasury_wsol"), mint.toBuffer()],
    PROGRAM_PK
  );

  const [treasuryUsdcVault] = PublicKey.findProgramAddressSync(
    [Buffer.from("treasury_usdc"), mint.toBuffer()],
    PROGRAM_PK
  );

  return {
    mint: mint.toBase58(),
    launchState: launchState.toBase58(),
    launchEscrow: launchEscrow.toBase58(),
    escrowSolVault: escrowSolVault.toBase58(),
    saleVault: saleVault.toBase58(),
    lpVault: lpVault.toBase58(),
    treasuryWsolVault: treasuryWsolVault.toBase58(),
    treasuryUsdcVault: treasuryUsdcVault.toBase58(),
  };
}

function hasBytes(buf, offset, len) {
  return !!buf && Number.isInteger(offset) && offset >= 0 && offset + len <= buf.length;
}

function readPubkey(buf, offset, fallback = null) {
  if (!hasBytes(buf, offset, 32)) return fallback;
  return new PublicKey(buf.subarray(offset, offset + 32)).toBase58();
}

function readU8(buf, offset, fallback = 0) {
  if (!hasBytes(buf, offset, 1)) return fallback;
  return buf.readUInt8(offset);
}

function readU16(buf, offset, fallback = 0) {
  if (!hasBytes(buf, offset, 2)) return fallback;
  return buf.readUInt16LE(offset);
}

function readU64(buf, offset, fallback = 0n) {
  if (!hasBytes(buf, offset, 8)) return fallback;
  return buf.readBigUInt64LE(offset);
}

function readI64(buf, offset, fallback = 0n) {
  if (!hasBytes(buf, offset, 8)) return fallback;
  return buf.readBigInt64LE(offset);
}

function readU128(buf, offset, fallback = 0n) {
  if (!hasBytes(buf, offset, 16)) return fallback;

  let out = 0n;
  for (let i = 15; i >= 0; i--) {
    out = (out << 8n) + BigInt(buf[offset + i]);
  }
  return out;
}

function decodeLaunchState(buf) {
  if (!buf || buf.length < 64) return null;

  let o = 8;

  const bump = readU8(buf, o); o += 1;
  const treasuryWsolBump = readU8(buf, o); o += 1;
  const treasuryUsdcBump = readU8(buf, o); o += 1;
  const escrowSolBump = readU8(buf, o); o += 1;
  const stateU8 = readU8(buf, o); o += 1;

  const mint = readPubkey(buf, o); o += 32;
  const creator = readPubkey(buf, o); o += 32;
  const platform = readPubkey(buf, o); o += 32;
  const coreAuthority = readPubkey(buf, o); o += 32;

  const saleVault = readPubkey(buf, o); o += 32;
  const lpVault = readPubkey(buf, o); o += 32;
  const treasuryWsolVault = readPubkey(buf, o); o += 32;
  const treasuryUsdcVault = readPubkey(buf, o); o += 32;
  const escrowSolVault = readPubkey(buf, o); o += 32;

  const totalSupply = readU64(buf, o); o += 8;
  const saleSupply = readU64(buf, o); o += 8;
  const lpSupply = readU64(buf, o); o += 8;

  const ammInitialSol = readU64(buf, o); o += 8;
  const ammInitialTok = readU64(buf, o); o += 8;
  const migratedAt = readI64(buf, o); o += 8;

  const ammType = readU8(buf, o); o += 1;
  const lpShareClaimBase = readU64(buf, o); o += 8;

  const quoteAssetU8 = readU8(buf, o); o += 1;
  const pendingQuoteAssetU8 = readU8(buf, o); o += 1;
  const lastPoolSwitchTs = readI64(buf, o); o += 8;
  const switchStartedAt = readI64(buf, o); o += 8;

  const feeTotalBps = readU16(buf, o); o += 2;
  const feeCreatorBps = readU16(buf, o); o += 2;
  const feePlatformBps = readU16(buf, o); o += 2;

  const tokensSold = readU64(buf, o); o += 8;
  const solCollected = readU128(buf, o); o += 16;

  const launchTs = readI64(buf, o); o += 8;
  const lastTradeTs = readI64(buf, o); o += 8;

  let metadata = null;
  if (hasBytes(buf, o, 32)) {
    metadata = readPubkey(buf, o);
    o += 32;
  }

  let devBuyDone = false;
  if (hasBytes(buf, o, 1)) {
    devBuyDone = Boolean(readU8(buf, o));
    o += 1;
  }

  let escrowSettled = false;
  if (hasBytes(buf, o, 1)) {
    escrowSettled = Boolean(readU8(buf, o));
    o += 1;
  }

  return {
    bump,
    treasuryWsolBump,
    treasuryUsdcBump,
    escrowSolBump,

    stateU8,
    phase: PHASE_BY_U8[stateU8] || "unknown",

    mint,
    creator,
    platform,
    coreAuthority,

    saleVault,
    lpVault,
    treasuryWsolVault,
    treasuryUsdcVault,
    escrowSolVault,

    totalSupply,
    saleSupply,
    lpSupply,

    ammInitialSol,
    ammInitialTok,
    migratedAt,

    ammType,
    lpShareClaimBase,

    quoteAssetU8,
    quoteAsset: QUOTE_BY_U8[quoteAssetU8] || "UNKNOWN",

    pendingQuoteAssetU8,
    pendingQuoteAsset: QUOTE_BY_U8[pendingQuoteAssetU8] || "UNKNOWN",

    lastPoolSwitchTs,
    switchStartedAt,

    feeTotalBps,
    feeCreatorBps,
    feePlatformBps,

    tokensSold,
    solCollected,

    launchTs,
    lastTradeTs,

    metadata,

    devBuyDone,
    escrowSettled,
  };
    }

function decodeTokenAccountAmount(buf) {
  if (!buf || buf.length < 72) return 0n;
  return buf.readBigUInt64LE(64);
}

function quoteMintForAsset(quoteAsset) {
  return quoteAsset === "USDC" ? USDC_MINT : WSOL_MINT;
}

function calculateStatsFromState(state, balances, solUsd) {
  const phase = state.phase;
  const quoteAsset = state.quoteAsset;

  const totalSupply = state.totalSupply || TOTAL_SUPPLY_BASE;
  const saleSupply = state.saleSupply || SALE_SUPPLY_BASE;
  const lpSupply = state.lpSupply || LP_SUPPLY_BASE;
  const tokensSold = state.tokensSold || 0n;
  const tokensRemaining = saleSupply > tokensSold ? saleSupply - tokensSold : 0n;

  let priceQuote = null;
  let priceSol = null;
  let priceUsd = null;

  if (
  phase === "bonding" ||
  phase === "pending_dev_buy" ||
  phase === "migration_pending"
) {
  // Bonding curve price must include virtual reserves.
  //
  // quote reserve = virtual SOL + real SOL collected
  // token reserve = virtual token reserve + remaining sale tokens
  //
  // price = quoteReserve / tokenReserve
  const realSolCollected = state.solCollected || 0n;
  const quoteReserve = V_SOL_LAMPORTS + realSolCollected;

  const realTokensRemaining =
    saleSupply > tokensSold ? saleSupply - tokensSold : 0n;

  const tokenReserve = V_TOK_BASE + realTokensRemaining;

  if (quoteReserve > 0n && tokenReserve > 0n) {
    priceSol = lamportsToSol(quoteReserve) / baseToUi(tokenReserve);
    priceQuote = priceSol;
    priceUsd = solUsd ? priceSol * solUsd : null;
  }
  } else if (phase === "amm_live" || phase === "switching" || phase === "migrated") {
    const tokenReserve = balances.lpVaultAmount || 0n;
    const quoteReserve = quoteAsset === "USDC" ? (balances.treasuryUsdcAmount || 0n) : (balances.treasuryWsolAmount || 0n);

    if (quoteReserve > 0n && tokenReserve > 0n) {
      priceQuote = quoteBaseToUi(quoteReserve, quoteAsset) / baseToUi(tokenReserve);
      priceSol = quoteAsset === "USDC" && solUsd ? priceQuote / solUsd : priceQuote;
      priceUsd = quoteAsset === "USDC" ? priceQuote : solUsd ? priceSol * solUsd : null;
    }
  }

  const totalSupplyUi = baseToUi(totalSupply || TOTAL_SUPPLY_BASE);

const marketcapQuote =
  priceQuote == null ? null : priceQuote * totalSupplyUi;

const marketcapSol =
  priceSol == null ? null : priceSol * totalSupplyUi;

const marketcapUsd =
  priceUsd == null ? null : priceUsd * totalSupplyUi;
  
  const bondingProgress = Number(saleSupply || 0n) > 0
    ? Math.max(0, Math.min(100, (baseToUi(tokensSold) / baseToUi(saleSupply)) * 100))
    : 0;

  return {
    priceQuote,
    priceSol,
    priceUsd,
    marketcapQuote,
    marketcapSol,
    marketcapUsd,
    totalSupply,
    saleSupply,
    lpSupply,
    tokensSold,
    tokensRemaining,
    bondingProgress,
  };
}

async function refreshMintState(mint, io = null) {
  const pdas = derivePdas(mint);

  const keys = [
    pdas.launchState,
    pdas.saleVault,
    pdas.lpVault,
    pdas.treasuryWsolVault,
    pdas.treasuryUsdcVault,
  ].map((x) => new PublicKey(x));

  const infos = await connection.getMultipleAccountsInfo(keys, "confirmed");
  const launchInfo = infos[0];
  if (!launchInfo) return null;

  const state = decodeLaunchState(launchInfo.data);
  if (!state) return null;

  const balances = {
    saleVaultAmount: decodeTokenAccountAmount(infos[1]?.data),
    lpVaultAmount: decodeTokenAccountAmount(infos[2]?.data),
    treasuryWsolAmount: decodeTokenAccountAmount(infos[3]?.data),
    treasuryUsdcAmount: decodeTokenAccountAmount(infos[4]?.data),
  };

  const solUsd = getPrice("SOL_USD")?.price || null;
  const computed = calculateStatsFromState(state, balances, solUsd);

  // Pull launch metadata from the API DB and copy it into the indexer DB.
  // This makes the indexer the read source for frontend token pages/cards.
  const launchMeta = await fetchLaunchMetadataFromApi(mint);
  const mediaUrl = indexerMediaUrl(mint);

  upsertLaunch(mint, {
    launch_state: pdas.launchState,
    launch_escrow: pdas.launchEscrow,
    name: launchMeta?.name || undefined,
    symbol: launchMeta?.symbol || undefined,
    description: launchMeta?.description || undefined,
    image: launchMeta?.image || undefined,
    metadata_uri: launchMeta?.metadata_uri || undefined,
    pinata_cid: launchMeta?.pinata_cid || undefined,
    escrow_sol_vault: state.escrowSolVault || pdas.escrowSolVault,
    sale_vault: state.saleVault || pdas.saleVault,
    lp_vault: state.lpVault || pdas.lpVault,
    treasury_wsol_vault: state.treasuryWsolVault || pdas.treasuryWsolVault,
    treasury_usdc_vault: state.treasuryUsdcVault || pdas.treasuryUsdcVault,
    metadata: state.metadata,
    creator: state.creator,
    platform: state.platform,
    core_authority: state.coreAuthority,
    total_supply: bigIntToString(state.totalSupply),
    sale_supply: bigIntToString(state.saleSupply),
    lp_supply: bigIntToString(state.lpSupply),
    decimals: TOKEN_DECIMALS,
    state_u8: state.stateU8,
    phase: state.phase,
    quote_asset_u8: state.quoteAssetU8,
    quote_asset: state.quoteAsset,
    pending_quote_asset_u8: state.pendingQuoteAssetU8,
    pending_quote_asset: state.pendingQuoteAsset,
    tokens_sold: bigIntToString(state.tokensSold),
    sol_collected: bigIntToString(state.solCollected),
    amm_initial_sol: bigIntToString(state.ammInitialSol),
    amm_initial_tok: bigIntToString(state.ammInitialTok),
    migrated_at: Number(state.migratedAt || 0n),
    launch_ts: Number(state.launchTs || 0n),
    last_trade_ts: Number(state.lastTradeTs || 0n),
    last_pool_switch_ts: Number(state.lastPoolSwitchTs || 0n),
    switch_started_at: Number(state.switchStartedAt || 0n),
    dev_buy_done: state.devBuyDone ? 1 : 0,
    escrow_settled: state.escrowSettled ? 1 : 0,
    sale_vault_amount: bigIntToString(balances.saleVaultAmount),
    lp_vault_amount: bigIntToString(balances.lpVaultAmount),
    treasury_wsol_amount: bigIntToString(balances.treasuryWsolAmount),
    treasury_usdc_amount: bigIntToString(balances.treasuryUsdcAmount),
  });

  const stats = upsertTokenStats(mint, {
    name: launchMeta?.name || undefined,
    symbol: launchMeta?.symbol || undefined,
    image: mediaUrl || undefined,
    metadata_uri: launchMeta?.metadata_uri || undefined,
    phase: state.phase,
    phase_u8: state.stateU8,
    quote_asset: state.quoteAsset,
    quote_asset_u8: state.quoteAssetU8,
    price_quote: computed.priceQuote,
    price_sol: computed.priceSol,
    price_usd: computed.priceUsd,
    marketcap_quote: computed.marketcapQuote,
    marketcap_sol: computed.marketcapSol,
    marketcap_usd: computed.marketcapUsd,
    total_supply: bigIntToString(computed.totalSupply),
    sale_supply: bigIntToString(computed.saleSupply),
    lp_supply: bigIntToString(computed.lpSupply),
    tokens_sold: bigIntToString(computed.tokensSold),
    tokens_remaining: bigIntToString(computed.tokensRemaining),
    bonding_progress: computed.bondingProgress,
    sale_vault: state.saleVault,
    lp_vault: state.lpVault,
    treasury_wsol_vault: state.treasuryWsolVault,
    treasury_usdc_vault: state.treasuryUsdcVault,
    sale_vault_amount: bigIntToString(balances.saleVaultAmount),
    lp_vault_amount: bigIntToString(balances.lpVaultAmount),
    treasury_wsol_amount: bigIntToString(balances.treasuryWsolAmount),
    treasury_usdc_amount: bigIntToString(balances.treasuryUsdcAmount),
    last_trade_ts: Number(state.lastTradeTs || 0n) || null,
  });

  if (io && stats) {
    io.to(`mint:${mint}`).emit("stats", stats);
    io.to(`mint:${mint}:stats`).emit("stats", stats);
  }

  return { state, balances, stats, pdas };
}

async function fetchParsedTransactionWithRetry(sig) {
  for (let i = 0; i < TX_FETCH_RETRIES; i++) {
    try {
      const tx = await connection.getParsedTransaction(sig, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
      if (tx) return tx;
    } catch (e) {
      if (i === TX_FETCH_RETRIES - 1) throw e;
    }
    await sleep(TX_FETCH_DELAY_MS);
  }
  return null;
}

function accountKeyAtIndex(tx, accountIndex) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  const key = keys[accountIndex];

  if (!key) return null;

  if (typeof key.pubkey?.toBase58 === "function") {
    return key.pubkey.toBase58();
  }

  if (key.pubkey) {
    return String(key.pubkey);
  }

  if (typeof key.toBase58 === "function") {
    return key.toBase58();
  }

  return String(key);
}

function tokenBalanceMap(tx, tokenBalances = []) {
  const map = new Map();

  for (const b of tokenBalances || []) {
    const key = `${b.accountIndex}:${b.mint}`;
    const tokenAccount = accountKeyAtIndex(tx, b.accountIndex);

    map.set(key, {
      accountIndex: b.accountIndex,
      tokenAccount,
      mint: b.mint,
      owner: b.owner || null,
      amount: toBigIntMaybe(b.uiTokenAmount?.amount || "0"),
      decimals: b.uiTokenAmount?.decimals ?? null,
    });
  }

  return map;
}

function getTokenDeltas(tx) {
  const pre = tokenBalanceMap(tx, tx?.meta?.preTokenBalances || []);
  const post = tokenBalanceMap(tx, tx?.meta?.postTokenBalances || []);
  const keys = new Set([...pre.keys(), ...post.keys()]);
  const deltas = [];

  for (const key of keys) {
    const before = pre.get(key);
    const after = post.get(key);
    const ref = after || before;

    const delta = (after?.amount || 0n) - (before?.amount || 0n);

    if (delta === 0n) continue;

    deltas.push({
      accountIndex: ref.accountIndex,
      tokenAccount: ref.tokenAccount,
      mint: ref.mint,
      owner: ref.owner,
      delta,
      before: before?.amount || 0n,
      after: after?.amount || 0n,
      decimals: ref.decimals,
    });
  }

  return deltas;
}

function updateHolderBalancesFromDeltas({ mint, deltas, refreshed, createdAt }) {
  if (!mint || !Array.isArray(deltas) || !deltas.length) return undefined;

  const excludedOwners = new Set(
    [
      PROGRAM_ID,
      PLATFORM_WALLET,
      refreshed?.pdas?.launchState,
      refreshed?.state?.coreAuthority,
      refreshed?.state?.platform,
    ].filter(Boolean)
  );

  const excludedTokenAccounts = new Set(
    [
      refreshed?.state?.saleVault,
      refreshed?.state?.lpVault,
      refreshed?.state?.treasuryWsolVault,
      refreshed?.state?.treasuryUsdcVault,
      refreshed?.pdas?.saleVault,
      refreshed?.pdas?.lpVault,
      refreshed?.pdas?.treasuryWsolVault,
      refreshed?.pdas?.treasuryUsdcVault,
    ].filter(Boolean)
  );

  let holders = undefined;

  for (const d of deltas) {
    if (d.mint !== mint) continue;
    if (!d.owner) continue;

    const tokenAccount = d.tokenAccount || `${d.accountIndex}:${d.mint}`;

    if (excludedOwners.has(d.owner)) continue;
    if (excludedTokenAccounts.has(tokenAccount)) continue;

    const result = upsertHolderBalance({
      mint,
      owner: d.owner,
      token_account: tokenAccount,
      amount: d.after.toString(),
      updated_at: createdAt || now(),
    });

    if (result?.holders !== undefined) {
      holders = result.holders;
    }
  }

  return holders;
}

function getSigners(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  return keys
    .filter((k) => k.signer)
    .map((k) => (typeof k.pubkey?.toBase58 === "function" ? k.pubkey.toBase58() : String(k.pubkey)));
}

function largestDelta(deltas, predicate) {
  const filtered = deltas.filter(predicate);
  if (!filtered.length) return null;
  return filtered.sort((a, b) => {
    const aa = a.delta < 0n ? -a.delta : a.delta;
    const bb = b.delta < 0n ? -b.delta : b.delta;
    return Number(bb - aa);
  })[0];
}

function eventMint(event) {
  return toBase58Maybe(event?.data?.mint);
}

function eventQuoteAsset(event) {
  const raw = event?.data?.quote_asset ?? event?.data?.quoteAsset;
  const u8 = raw === undefined || raw === null ? null : Number(raw.toString ? raw.toString() : raw);
  return { quoteAssetU8: u8, quoteAsset: QUOTE_BY_U8[u8] || "SOL" };
}

function classifyEventName(name) {
  const n = String(name || "");
  if (n === "BuyEvent" || n === "BuyExecuted") return "BUY";
  if (n === "SellEvent" || n === "SellExecuted") return "SELL";
  if (n === "AmmBuyEvent" || n === "AmmBuyExecuted") return "AMM_BUY";
  if (n === "AmmSellEvent" || n === "AmmSellExecuted") return "AMM_SELL";
  if (n === "CreatedTxn" || n === "CurveActivated" || n === "DevBuyEvent") return "DEVBUY";
  return null;
}

function isLaunchLikeEvent(name) {
  return [
    "LaunchEscrowFundedEvent",
    "LaunchEscrowRefundedEvent",
    "MigratedEvent",
    "PoolSwitchStartedEvent",
    "PoolSwitchCompletedEvent",
    "CreatedTxn",
  ].includes(String(name));
}

function quoteVolumeToSol(quoteAmountBase, quoteAsset) {
  if (quoteAsset === "USDC") {
    const solUsd = getPrice("SOL_USD")?.price || null;
    if (!solUsd) return 0;
    return quoteBaseToUi(quoteAmountBase, "USDC") / solUsd;
  }
  return quoteBaseToUi(quoteAmountBase, "SOL");
}

function priceFromAmounts({ quoteAmountBase, tokenAmountBase, quoteAsset }) {
  const tokenUi = baseToUi(tokenAmountBase, TOKEN_DECIMALS);
  if (!tokenUi) return { priceQuote: null, priceSol: null, priceUsd: null };

  const quoteUi = quoteBaseToUi(quoteAmountBase, quoteAsset);
  const priceQuote = quoteUi / tokenUi;
  const solUsd = getPrice("SOL_USD")?.price || null;
  const priceSol = quoteAsset === "USDC" ? (solUsd ? priceQuote / solUsd : null) : priceQuote;
  const priceUsd = quoteAsset === "USDC" ? priceQuote : (solUsd && priceSol ? priceSol * solUsd : null);
  return { priceQuote, priceSol, priceUsd };
}

async function handleTradeEvent({ sig, slot, tx, event, logIndex, io }) {
  const name = event.name;
  const side = classifyEventName(name);
  if (!side) return null;

  const data = event?.data || {};
  const mint = eventMint(event);
  if (!mint) return null;

  const isBuy = side === "BUY" || side === "AMM_BUY" || side === "DEVBUY";
  const isSell = side === "SELL" || side === "AMM_SELL";

  const refreshed = await refreshMintState(mint, io);
  const phase = refreshed?.stats?.phase || null;
  const phaseU8 = refreshed?.stats?.phase_u8 ?? null;

  const eventQuote = eventQuoteAsset(event);
  const quoteAsset =
    eventQuote.quoteAsset ||
    refreshed?.stats?.quote_asset ||
    "SOL";

  const quoteAssetU8 =
    eventQuote.quoteAssetU8 ??
    refreshed?.stats?.quote_asset_u8 ??
    (quoteAsset === "USDC" ? 1 : 0);

  const quoteMint = quoteMintForAsset(quoteAsset);

  const deltas = getTokenDeltas(tx);

  const createdAt = tx?.blockTime || now();

  const holdersCount = updateHolderBalancesFromDeltas({
    mint,
    deltas,
    refreshed,
    createdAt,
  });

  const signers = getSigners(tx).filter((x) => x !== PLATFORM_WALLET);
  
  const tokenPositive = largestDelta(
    deltas,
    (d) => d.mint === mint && d.delta > 0n
  );

  const tokenNegative = largestDelta(
    deltas,
    (d) => d.mint === mint && d.delta < 0n
  );

  const quotePositive = largestDelta(
    deltas,
    (d) => d.mint === quoteMint && d.delta > 0n
  );

  const quoteNegative = largestDelta(
    deltas,
    (d) => d.mint === quoteMint && d.delta < 0n
  );

  const eventUser =
    toBase58Maybe(data.user) ||
    toBase58Maybe(data.buyer) ||
    toBase58Maybe(data.seller) ||
    toBase58Maybe(data.creator) ||
    toBase58Maybe(data.dev) ||
    null;

  let user =
    eventUser ||
    (isBuy
      ? tokenPositive?.owner || quoteNegative?.owner
      : tokenNegative?.owner || quotePositive?.owner) ||
    signers[0] ||
    null;

  const eventInputAmount = toBigIntMaybe(
    data.input_amount ??
      data.inputAmount ??
      null,
    0n
  );

  const eventOutputAmount = toBigIntMaybe(
    data.output_amount ??
      data.outputAmount ??
      null,
    0n
  );

  const eventQuoteAmount = toBigIntMaybe(
    data.quote_amount ??
      data.quoteAmount ??
      null,
    0n
  );

  const eventTokenAmount = toBigIntMaybe(
    data.token_amount ??
      data.tokenAmount ??
      null,
    0n
  );

  const legacyAmount = toBigIntMaybe(
    data.amount ??
      data.devbuy ??
      0n,
    0n
  );

  let inputMint =
    toBase58Maybe(data.input_mint) ||
    toBase58Maybe(data.inputMint) ||
    null;

  let outputMint =
    toBase58Maybe(data.output_mint) ||
    toBase58Maybe(data.outputMint) ||
    null;

  let inputAmount = eventInputAmount;
  let outputAmount = eventOutputAmount;
  let quoteAmount = eventQuoteAmount;
  let tokenAmount = eventTokenAmount;

  // New universal event path.
  // These fields should come directly from the Rust event and are the source of truth.
  if (inputAmount > 0n || outputAmount > 0n || quoteAmount > 0n || tokenAmount > 0n) {
    if (!quoteAmount || quoteAmount <= 0n) {
      quoteAmount = isBuy ? inputAmount : outputAmount;
    }

    if (!tokenAmount || tokenAmount <= 0n) {
      tokenAmount = isBuy ? outputAmount : inputAmount;
    }

    if (!inputAmount || inputAmount <= 0n) {
      inputAmount = isBuy ? quoteAmount : tokenAmount;
    }

    if (!outputAmount || outputAmount <= 0n) {
      outputAmount = isBuy ? tokenAmount : quoteAmount;
    }

    if (!inputMint) {
      inputMint = isBuy ? quoteMint : mint;
    }

    if (!outputMint) {
      outputMint = isBuy ? mint : quoteMint;
    }
  } else {
    // Legacy fallback for old events.
    // Old events only had "amount", so this is less reliable.
    if (isBuy) {
      inputMint = quoteMint;
      outputMint = mint;

      inputAmount = legacyAmount || (quoteNegative ? -quoteNegative.delta : 0n);
      outputAmount = tokenPositive?.delta || 0n;

      quoteAmount = inputAmount;
      tokenAmount = outputAmount;
    }

    if (isSell) {
      inputMint = mint;
      outputMint = quoteMint;

      inputAmount = legacyAmount || (tokenNegative ? -tokenNegative.delta : 0n);
      outputAmount = quotePositive?.delta || 0n;

      tokenAmount = inputAmount;
      quoteAmount = outputAmount;
    }
  }

  // Final safety fallback from parsed token balance deltas.
  if ((!tokenAmount || tokenAmount <= 0n) && isBuy && tokenPositive?.delta) {
    tokenAmount = tokenPositive.delta;
  }

  if ((!tokenAmount || tokenAmount <= 0n) && isSell && tokenNegative?.delta) {
    tokenAmount = -tokenNegative.delta;
  }

  if ((!quoteAmount || quoteAmount <= 0n) && isBuy && quoteNegative?.delta) {
    quoteAmount = -quoteNegative.delta;
  }

  if ((!quoteAmount || quoteAmount <= 0n) && isSell && quotePositive?.delta) {
    quoteAmount = quotePositive.delta;
  }

  if ((!inputAmount || inputAmount <= 0n)) {
    inputAmount = isBuy ? quoteAmount : tokenAmount;
  }

  if ((!outputAmount || outputAmount <= 0n)) {
    outputAmount = isBuy ? tokenAmount : quoteAmount;
  }

  if (!inputMint) inputMint = isBuy ? quoteMint : mint;
  if (!outputMint) outputMint = isBuy ? mint : quoteMint;

  let price = priceFromAmounts({
    quoteAmountBase: quoteAmount,
    tokenAmountBase: tokenAmount,
    quoteAsset,
  });

  if (!price.priceSol && refreshed?.stats?.price_sol) {
    price = {
      priceQuote: refreshed.stats.price_quote,
      priceSol: refreshed.stats.price_sol,
      priceUsd: refreshed.stats.price_usd,
    };
  }

  const volumeQuote = quoteBaseToUi(quoteAmount, quoteAsset) || 0;
  const volumeSol = quoteVolumeToSol(quoteAmount, quoteAsset) || 0;
  const volumeUsd =
    price.priceUsd && baseToUi(tokenAmount)
      ? price.priceUsd * baseToUi(tokenAmount)
      : 0;

  const volumeTokens = baseToUi(tokenAmount) || 0;

  const tradeRow = {
    sig,
    slot,
    block_time: tx?.blockTime ?? null,
    log_index: logIndex,
    mint,
    user,
    side,
    phase,
    phase_u8: phaseU8,
    quote_asset: quoteAsset,
    quote_asset_u8: quoteAssetU8,

    input_amount: bigIntToString(inputAmount),
    input_mint: inputMint,
    output_amount: bigIntToString(outputAmount),
    output_mint: outputMint,

    quote_amount: bigIntToString(quoteAmount),
    token_amount: bigIntToString(tokenAmount),

    price_quote: price.priceQuote,
    price_sol: price.priceSol,
    price_usd: price.priceUsd,

    creator_fee: bigIntToString(data.creator_fee ?? data.creatorFee),
    platform_fee: bigIntToString(data.platform_fee ?? data.platformFee),
    lp_fee: bigIntToString(data.lp_fee ?? data.lpFee),

    tokens_sold_total:
      bigIntToString(data.tokens_sold_total ?? data.tokensSoldTotal) ||
      refreshed?.stats?.tokens_sold ||
      null,

    sol_collected_total:
      bigIntToString(data.quote_collected_total ?? data.quoteCollectedTotal) ||
      (refreshed?.state?.solCollected
        ? refreshed.state.solCollected.toString()
        : null),

    raw_event_name: name,
    raw_event_json: stringifySafe(data),
    created_at: createdAt,
  };

  insertTrade(tradeRow);

  const candle = upsertCandle1m({
    mint,
    ts: createdAt,
    priceSol: price.priceSol,
    priceUsd: price.priceUsd,
    volumeQuote,
    volumeSol,
    volumeUsd,
    volumeTokens,
    side,
  });

  const stats = refresh24hVolume(mint);

  const payload = {
    ...tradeRow,
    priceSol: price.priceSol,
    priceUsd: price.priceUsd,
    createdAt,
  };

  if (io) {
    io.to("global:trades").emit("trade", payload);
    io.to(`mint:${mint}`).emit("trade", payload);
    io.to(`mint:${mint}:trades`).emit("trade", payload);

    if (candle) {
      io.to(`mint:${mint}`).emit("candle", { interval: "1m", ...candle });
      io.to(`mint:${mint}:candles:1m`).emit("candle", {
        interval: "1m",
        ...candle,
      });
    }

    if (stats) {
      io.to(`mint:${mint}`).emit("stats", stats);
      io.to(`mint:${mint}:stats`).emit("stats", stats);
    }

    if (holdersCount !== undefined) {
      io.to(`mint:${mint}`).emit("holders", {
        mint,
        holders: holdersCount,
        holders_count: holdersCount,
        updated_at: createdAt,
      });

      io.to(`mint:${mint}:holders`).emit("holders", {
        mint,
        holders: holdersCount,
        holders_count: holdersCount,
        updated_at: createdAt,
      });
    }
  }

  return payload;
}

async function handleEvent({ sig, slot, tx, event, logIndex, io }) {
  const mint = eventMint(event);
  const user = toBase58Maybe(event?.data?.user ?? event?.data?.creator ?? event?.data?.dev);

  insertEvent({
    sig,
    slot,
    log_index: logIndex,
    mint,
    user,
    event_name: event.name,
    payload_json: stringifySafe(event.data),
    created_at: tx?.blockTime || now(),
  });

  if (mint) {
    if (event.name === "LaunchEscrowFundedEvent") {
      const pdas = derivePdas(mint);
      upsertLaunch(mint, {
        launch_escrow: pdas.launchEscrow,
        escrow_sol_vault: pdas.escrowSolVault,
        creator: toBase58Maybe(event.data.creator),
      });
    }

    await refreshMintState(mint, io).catch((e) => {
      if (!["LaunchEscrowFundedEvent", "LaunchEscrowRefundedEvent"].includes(event.name)) {
        console.error(`refreshMintState failed for ${mint}:`, e.message);
      }
    });
  }

  const raw = {
    sig,
    slot,
    logIndex,
    eventName: event.name,
    mint,
    user,
    payload: JSON.parse(stringifySafe(event.data)),
  };

  if (io) {
    if (ENABLE_GLOBAL_EVENTS || isLaunchLikeEvent(event.name)) {
      io.to("global:events").emit("event", raw);
    }
    if (mint) {
      io.to(`mint:${mint}`).emit("event", raw);
      io.to(`mint:${mint}:events`).emit("event", raw);
    }
  }

  const side = classifyEventName(event.name);
  if (side) {
    return handleTradeEvent({ sig, slot, tx, event, logIndex, io });
  }

  return raw;
}

async function processSignature(sig, slot, io = null, { force = false } = {}) {
  if (!force && hasSeenTx(sig)) return { skipped: true, sig };

  const tx = await fetchParsedTransactionWithRetry(sig);
  if (!tx || tx?.meta?.err) return { skipped: true, sig, reason: "missing_or_failed_tx" };

  const logs = tx.meta?.logMessages || [];
  const events = decodeEventsFromLogs(logs);

  if (!events.length) {
    if (!force) markTxSeen(sig, slot);
    return { skipped: true, sig, reason: "no_events" };
  }

  if (!force) markTxSeen(sig, slot);

  const handled = [];
  for (let i = 0; i < events.length; i++) {
    try {
      const result = await handleEvent({ sig, slot, tx, event: events[i], logIndex: i, io });
      handled.push(result);
    } catch (e) {
      console.error(`event handle failed ${sig} ${events[i]?.name}:`, e);
    }
  }

  return { sig, events: handled.length };
}

async function backfillRecent(io = null) {
  if (!STARTUP_BACKFILL_SIGNATURES) return;

  console.log(`Backfilling last ${STARTUP_BACKFILL_SIGNATURES} program signatures...`);
  const sigs = await connection.getSignaturesForAddress(PROGRAM_PK, {
    limit: Math.min(1000, STARTUP_BACKFILL_SIGNATURES),
  }, "confirmed");

  for (const row of sigs.reverse()) {
    try {
      await processSignature(row.signature, row.slot, io, { force: false });
    } catch (e) {
      console.error("backfill signature failed:", row.signature, e.message);
    }
  }
}

async function pollSolPriceOnce(io = null) {
  const fixed = process.env.SOL_USD_FIXED ? Number(process.env.SOL_USD_FIXED) : null;
  if (fixed && Number.isFinite(fixed) && fixed > 0) {
    setPrice("SOL_USD", fixed);
    if (io) io.to("global:prices").emit("price", { key: "SOL_USD", price: fixed, updated_at: now() });
    return fixed;
  }

  const pair = process.env.DEX_SOL_PAIR || "58oqchx4ywmvkdwllzzbi4chocc2fqcuwbkwmihlyqo2";
  const url = `https://api.dexscreener.com/latest/dex/pairs/solana/${pair}`;

  const res = await fetch(url);
  if (!res.ok) throw new Error(`Dexscreener HTTP ${res.status}`);

  const json = await res.json();
  const priceUsd = Number(json?.pairs?.[0]?.priceUsd);
  if (!Number.isFinite(priceUsd) || priceUsd <= 0) throw new Error("Dexscreener SOL price missing");

  setPrice("SOL_USD", priceUsd);
  const payload = { key: "SOL_USD", price: priceUsd, updated_at: now() };
  if (io) {
    io.emit("price", payload);
    io.to("global:prices").emit("price", payload);
  }

  return priceUsd;
}

function startSolPricePoller(io = null) {
  const interval = Number(process.env.SOL_PRICE_INTERVAL_MS || 15000);
  const run = async () => {
    try {
      await pollSolPriceOnce(io);
    } catch (e) {
      console.error("SOL price poll error:", e.message);
    }
  };

  run();
  return setInterval(run, interval);
}

function startWebsocket(io = null) {
  let ws = null;
  let pingTimer = null;
  let reconnectTimer = null;
  let stopped = false;

  function cleanup() {
    if (pingTimer) clearInterval(pingTimer);
    pingTimer = null;
  }

  function connect() {
    if (stopped) return;
    cleanup();

    ws = new WebSocket(HELIUS_WSS);

    ws.on("open", () => {
      console.log("Connected to program logs websocket");

      pingTimer = setInterval(() => {
        try { ws.ping(); } catch (_) {}
      }, 60000);

      ws.send(JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "logsSubscribe",
        params: [{ mentions: [PROGRAM_ID] }, { commitment: "confirmed" }],
      }));
    });

    ws.on("message", async (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString("utf8")); } catch (_) { return; }

      if (msg.id === 1 && msg.result) {
        console.log("logsSubscribe subId:", msg.result);
        return;
      }

      if (msg.method !== "logsNotification") return;

      const value = msg.params?.result?.value;
      const slot = msg.params?.result?.context?.slot ?? null;
      if (!value?.signature || value.err) return;

      try {
        await processSignature(value.signature, slot, io);
      } catch (e) {
        console.error("processSignature failed:", value.signature, e.message);
      }
    });

    ws.on("close", () => {
      if (stopped) return;
      console.log("Program websocket closed. Reconnecting...");
      cleanup();
      reconnectTimer = setTimeout(connect, 1500);
    });

    ws.on("error", (e) => {
      console.error("Program websocket error:", e.message);
    });
  }

  connect();

  return {
    stop() {
      stopped = true;
      cleanup();
      if (reconnectTimer) clearTimeout(reconnectTimer);
      try { ws?.close(); } catch (_) {}
    },
  };
}

async function simulateBuy(mint, quoteInUi) {
  const token = getToken(mint) || (await refreshMintState(mint).then((r) => r?.stats));
  if (!token) throw new Error("Token not found");

  const quoteAsset = token.quote_asset || "SOL";
  const quoteInBase = quoteAsset === "USDC"
    ? BigInt(Math.floor(Number(quoteInUi) * 1_000_000))
    : BigInt(Math.floor(Number(quoteInUi) * LAMPORTS_PER_SOL));

  const fee = quoteInBase * BigInt(TRADE_FEE_BPS) / 10000n;
  const net = quoteInBase - fee;

  let tokensOut = 0n;

  if (token.phase === "bonding" || token.phase === "pending_dev_buy") {
    const tokensRemaining = toBigIntMaybe(token.tokens_remaining, SALE_SUPPLY_BASE);
    const sold = toBigIntMaybe(token.tokens_sold, 0n);
    const solCollected = toBigIntMaybe(db.prepare(`SELECT sol_collected FROM launches WHERE mint=?`).get(mint)?.sol_collected, 0n);
    const x = V_SOL_LAMPORTS + solCollected;
    const y = V_TOK_BASE + (SALE_SUPPLY_BASE - sold || tokensRemaining);
    const k = x * y;
    const newX = x + net;
    const newY = k / newX;
    tokensOut = y > newY ? y - newY : 0n;
    if (tokensOut > tokensRemaining) tokensOut = tokensRemaining;
  } else {
    const quoteReserve = quoteAsset === "USDC"
      ? toBigIntMaybe(token.treasury_usdc_amount, 0n)
      : toBigIntMaybe(token.treasury_wsol_amount, 0n);
    const tokenReserve = toBigIntMaybe(token.lp_vault_amount, 0n);
    const lpFee = fee * 3750n / 10000n;
    const quoteToPool = net + lpFee;
    const k = quoteReserve * tokenReserve;
    const newX = quoteReserve + quoteToPool;
    const newY = newX > 0n ? k / newX : tokenReserve;
    tokensOut = tokenReserve > newY ? tokenReserve - newY : 0n;
  }

  return {
    mint,
    quoteAsset,
    quoteIn: quoteInUi,
    fee: quoteBaseToUi(fee, quoteAsset),
    tokensOut: baseToUi(tokensOut),
    token,
  };
}

async function startIndexer({ io = null } = {}) {
  console.log("Starting Moonz indexer");
  console.log("Program:", PROGRAM_ID);
  console.log("RPC:", RPC_URL);
  console.log("WS:", HELIUS_WSS.replace(/api-key=.*/, "api-key=***"));
  console.log(`Curve constants: V_SOL=${Number(V_SOL_LAMPORTS) / LAMPORTS_PER_SOL}, V_TOK=${Number(V_TOK_BASE) / TOKEN_SCALE}`);

  const priceTimer = startSolPricePoller(io);
  await backfillRecent(io);
  const websocket = startWebsocket(io);

  return {
    stop() {
      clearInterval(priceTimer);
      websocket.stop();
    },
  };
}

if (require.main === module) {
  startIndexer().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}

module.exports = {
  startIndexer,
  processSignature,
  refreshMintState,
  simulateBuy,
  derivePdas,
  decodeLaunchState,
};
