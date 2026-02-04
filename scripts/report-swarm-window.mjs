import { DatabaseSync } from "node:sqlite";
import fs from "node:fs/promises";
import { createPublicClient, http } from "viem";
import { sepolia } from "viem/chains";

const chainId = 11155111;
const startBlock = Number(process.env.START_BLOCK ?? 10183680);
const endBlock = Number(process.env.END_BLOCK ?? 10188021);
const rpcUrl = process.env.SEPOLIA_ETH_RPC_URL ?? "https://sepolia.drpc.org";
const dbPath = process.env.DB_PATH ?? "data/fhevm_stats.sqlite";
const wallet = (process.env.WALLET ?? "0xfceb8ec98844fa61bef9c7bd82b7fa4c7400d97a").toLowerCase();
const caller = (process.env.CALLER ?? "0x9fdd4b67c241779dca4d2eaf3d5946fb699f5d7a").toLowerCase();
const concurrency = Number(process.env.CONCURRENCY ?? 8);

const db = new DatabaseSync(dbPath, { readonly: true });

const incidentStmt = db.prepare(
  `SELECT DISTINCT block_number AS blockNumber, tx_hash AS txHash
   FROM fhe_events
   WHERE chain_id = ? AND block_number >= ? AND block_number <= ?`,
);
const incidentRows = incidentStmt.all(chainId, startBlock, endBlock);

const callerStmt = db.prepare(
  `SELECT DISTINCT tx_hash AS txHash
   FROM fhe_events
   WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
     AND lower(json_extract(args_json, '$.caller')) = ?`,
);
const callerRows = callerStmt.all(chainId, startBlock, endBlock, caller);

db.close();

const incidentByBlock = new Map();
for (const row of incidentRows) {
  const bn = Number(row.blockNumber);
  const txHash = String(row.txHash);
  const set = incidentByBlock.get(bn) ?? new Set();
  set.add(txHash);
  set.add(txHash.toLowerCase());
  incidentByBlock.set(bn, set);
}

const callerTxs = new Set(callerRows.map((r) => String(r.txHash).toLowerCase()));
const blocks = Array.from(incidentByBlock.keys()).sort((a, b) => a - b);

const client = createPublicClient({ chain: sepolia, transport: http(rpcUrl, { timeout: 30_000 }) });

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getBlockWithRetry(blockNumber) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      return await client.getBlock({ blockNumber, includeTransactions: true });
    } catch (err) {
      if (attempt === 4) throw err;
      await sleep(400 * 2 ** attempt);
    }
  }
  throw new Error("unreachable");
}

function bump(map, key, by = 1) {
  map.set(key, (map.get(key) ?? 0) + by);
}

let scannedBlocks = 0;
let incidentTxCount = 0;
let callerTxCount = 0;
let fromWalletCount = 0;
let toCallerCount = 0;
let fromWalletToCallerCount = 0;

const fromCounts = new Map();
const toCounts = new Map();

const walletIncidentTxHashes = [];
const walletCallerTxHashes = [];

let cursor = 0;

async function worker() {
  for (;;) {
    const i = cursor;
    cursor += 1;
    if (i >= blocks.length) return;

    const bn = blocks[i];
    const incidentSet = incidentByBlock.get(bn);
    if (!incidentSet || incidentSet.size === 0) continue;

    const block = await getBlockWithRetry(BigInt(bn));
    scannedBlocks += 1;

    for (const tx of block.transactions) {
      const hash = String(tx.hash).toLowerCase();
      if (!incidentSet.has(hash)) continue;

      const from = String(tx.from).toLowerCase();
      const to = tx.to ? String(tx.to).toLowerCase() : null;

      incidentTxCount += 1;
      bump(fromCounts, from);
      if (to) bump(toCounts, to);

      const isCaller = callerTxs.has(hash);
      if (isCaller) callerTxCount += 1;

      const isFromWallet = from === wallet;
      const isToCaller = to === caller;

      if (isFromWallet) {
        fromWalletCount += 1;
        walletIncidentTxHashes.push(String(tx.hash));
      }
      if (isToCaller) toCallerCount += 1;
      if (isFromWallet && isToCaller) {
        fromWalletToCallerCount += 1;
        walletCallerTxHashes.push(String(tx.hash));
      }
    }

    if (scannedBlocks % 300 === 0) {
      console.log(JSON.stringify({ progress: { scannedBlocks, totalBlocks: blocks.length, block: bn } }));
    }
  }
}

await Promise.all(Array.from({ length: concurrency }, () => worker()));

function topN(map, n) {
  return Array.from(map.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([k, v]) => ({ key: k, count: v }));
}

const report = {
  generatedAt: new Date().toISOString(),
  rpcUrl,
  dbPath,
  chainId,
  range: { startBlock, endBlock },
  incident: {
    blocksWithIncident: blocks.length,
    incidentTxsDbDistinct: incidentRows.length,
    callerTxsDbDistinct: callerRows.length,
    scannedBlocks,
    incidentTxsOnchainMatched: incidentTxCount,
    callerTxsOnchainMatched: callerTxCount,
  },
  attribution: {
    wallet,
    caller,
    fromWalletCount,
    toCallerCount,
    fromWalletToCallerCount,
  },
  topFrom: topN(fromCounts, 10),
  topTo: topN(toCounts, 10),
  samples: {
    walletIncidentTxHashes: walletIncidentTxHashes.slice(0, 25),
    walletCallerTxHashes: walletCallerTxHashes.slice(0, 25),
  },
};

const outPath = process.env.OUT_PATH ?? `/tmp/swarm_report_${startBlock}_${endBlock}.json`;
await fs.writeFile(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ wrote: outPath, summary: report.attribution }, null, 2));

