import { DatabaseSync } from "node:sqlite";
import fs from "node:fs/promises";
import { createPublicClient, http } from "viem";
import { sepolia } from "viem/chains";

const dbPath = process.env.DB_PATH ?? "data/fhevm_stats.sqlite";
const rpcUrl = process.env.SEPOLIA_ETH_RPC_URL ?? "https://sepolia.drpc.org";
const chainId = Number(process.env.CHAIN_ID ?? 11155111);

const callerLower = String(
  process.env.CALLER ?? "0x9fdd4b67c241779dca4d2eaf3d5946fb699f5d7a",
).toLowerCase();

const hoursBack = Number(process.env.HOURS_BACK ?? 17);
const explicitStart = process.env.START_BLOCK ? Number(process.env.START_BLOCK) : null;
const explicitEnd = process.env.END_BLOCK ? Number(process.env.END_BLOCK) : null;

const client = createPublicClient({ chain: sepolia, transport: http(rpcUrl, { timeout: 30_000 }) });
const tip = Number(await client.getBlockNumber());

const now = Math.floor(Date.now() / 1000);
const startTs = now - hoursBack * 3600;

async function blockForTs(target) {
  let lo = 0n;
  let hi = await client.getBlockNumber();
  while (lo < hi) {
    const mid = (lo + hi) / 2n;
    const b = await client.getBlock({ blockNumber: mid });
    const ts = Number(b.timestamp);
    if (ts >= target) hi = mid;
    else lo = mid + 1n;
  }
  return Number(lo);
}

const db = new DatabaseSync(dbPath, { readonly: true });

const dbMaxRow = db
  .prepare(`SELECT MAX(block_number) AS maxBlock FROM fhe_events WHERE chain_id = ?`)
  .get(chainId);
const dbMaxBlock = Number(dbMaxRow?.maxBlock ?? 0);

const startBlock = explicitStart ?? (await blockForTs(startTs));
const endBlock = Math.min(explicitEnd ?? tip, dbMaxBlock);

if (endBlock <= 0) {
  db.close();
  throw new Error(`No fhe_events found for chain_id=${chainId} in DB (${dbPath}).`);
}
if (startBlock > endBlock) {
  db.close();
  throw new Error(
    `startBlock (${startBlock}) is after endBlock (${endBlock}). Try reducing HOURS_BACK or set START_BLOCK/END_BLOCK.`,
  );
}

const attackerTxCountRow = db
  .prepare(
    `SELECT COUNT(DISTINCT tx_hash) AS n
     FROM fhe_events
     WHERE chain_id = ? AND block_number BETWEEN ? AND ?
       AND lower(json_extract(args_json,'$.caller')) = ?`,
  )
  .get(chainId, startBlock, endBlock, callerLower);

const attackerTxCount = Number(attackerTxCountRow?.n ?? 0);

const signatureRows = db
  .prepare(
    `WITH attacker_txs AS (
       SELECT DISTINCT tx_hash AS txHash
       FROM fhe_events
       WHERE chain_id = ? AND block_number BETWEEN ? AND ?
         AND lower(json_extract(args_json,'$.caller')) = ?
     )
     SELECT
       signature_hash AS signature,
       COUNT(*) AS txs,
       MIN(node_count) AS minNodes,
       MAX(node_count) AS maxNodes,
       AVG(node_count) AS avgNodes,
       MIN(edge_count) AS minEdges,
       MAX(edge_count) AS maxEdges,
       AVG(edge_count) AS avgEdges,
       MIN(depth) AS minDepth,
       MAX(depth) AS maxDepth,
       AVG(depth) AS avgDepth
     FROM dfg_txs
     WHERE chain_id = ?
       AND tx_hash IN (SELECT txHash FROM attacker_txs)
     GROUP BY signature_hash
     ORDER BY txs DESC, maxDepth DESC, maxNodes DESC`,
  )
  .all(chainId, startBlock, endBlock, callerLower, chainId);

const topN = Number(process.env.TOP_N ?? 25);
const topSignatures = signatureRows.slice(0, topN).map((row) => ({
  signature: String(row.signature),
  txs: Number(row.txs),
  nodeStats: {
    min: Number(row.minNodes),
    max: Number(row.maxNodes),
    avg: Number(row.avgNodes),
  },
  edgeStats: {
    min: Number(row.minEdges),
    max: Number(row.maxEdges),
    avg: Number(row.avgEdges),
  },
  depthStats: {
    min: Number(row.minDepth),
    max: Number(row.maxDepth),
    avg: Number(row.avgDepth),
  },
}));

const samplePerSig = Number(process.env.SAMPLES_PER_SIG ?? 10);
const sampleTxStmt = db.prepare(
  `WITH attacker_txs AS (
     SELECT DISTINCT tx_hash AS txHash
     FROM fhe_events
     WHERE chain_id = ? AND block_number BETWEEN ? AND ?
       AND lower(json_extract(args_json,'$.caller')) = ?
   )
   SELECT d.tx_hash AS txHash, d.block_number AS blockNumber, d.node_count AS nodes, d.edge_count AS edges, d.depth
   FROM dfg_txs d
   WHERE d.chain_id = ?
     AND d.signature_hash = ?
     AND d.tx_hash IN (SELECT txHash FROM attacker_txs)
   ORDER BY d.block_number, d.tx_hash
   LIMIT ?`,
);

const opMixStmt = db.prepare(
  `WITH attacker_txs AS (
     SELECT DISTINCT tx_hash AS txHash
     FROM fhe_events
     WHERE chain_id = ? AND block_number BETWEEN ? AND ?
       AND lower(json_extract(args_json,'$.caller')) = ?
   )
   SELECT n.op AS op, COUNT(*) AS nodes
   FROM dfg_nodes n
   WHERE n.chain_id = ?
     AND n.tx_hash IN (
       SELECT d.tx_hash
       FROM dfg_txs d
       WHERE d.chain_id = ?
         AND d.signature_hash = ?
         AND d.tx_hash IN (SELECT txHash FROM attacker_txs)
     )
   GROUP BY n.op
   ORDER BY nodes DESC
   LIMIT 25`,
);

for (const sig of topSignatures) {
  sig.samples = sampleTxStmt.all(
    chainId,
    startBlock,
    endBlock,
    callerLower,
    chainId,
    sig.signature,
    samplePerSig,
  );
}

const opMixForTop = Number(process.env.OP_MIX_FOR_TOP ?? 5);
for (let i = 0; i < Math.min(opMixForTop, topSignatures.length); i += 1) {
  const sig = topSignatures[i];
  sig.opMixTop25 = opMixStmt.all(
    chainId,
    startBlock,
    endBlock,
    callerLower,
    chainId,
    chainId,
    sig.signature,
  );
}

db.close();

const report = {
  generatedAt: new Date().toISOString(),
  dbPath,
  rpcUrl,
  chainId,
  range: { startBlock, endBlock, hoursBack, tip, dbMaxBlock },
  caller: callerLower,
  attackerTxCount,
  signatureCount: signatureRows.length,
  topSignatures,
};

const outPath =
  process.env.OUT_PATH ?? `/tmp/attacker_dfg_patterns_${startBlock}_${endBlock}.json`;
await fs.writeFile(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ wrote: outPath, attackerTxCount, signatureCount: signatureRows.length }, null, 2));
