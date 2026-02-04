import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

function parseNumber(value: string | null | undefined): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function hasTable(db: ReturnType<typeof initDatabase>, name: string): boolean {
  const row = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = $name")
    .get({ $name: name }) as { name: string } | undefined;
  return Boolean(row);
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainId = parseNumber(Bun.env.CHAIN_ID);
const db = initDatabase(dbPath);

const chains = chainId
  ? [chainId]
  : (
      db
        .prepare("SELECT DISTINCT chain_id AS chainId FROM dfg_txs ORDER BY chain_id")
        .all() as Array<{ chainId: number }>
    ).map((row) => row.chainId);

const statsStmt = db.prepare(
  `SELECT COUNT(*) AS total,
          AVG(node_count) AS avgNodes,
          AVG(edge_count) AS avgEdges,
          AVG(depth) AS avgDepth,
          MIN(node_count) AS minNodes,
          MAX(node_count) AS maxNodes,
          MIN(edge_count) AS minEdges,
          MAX(edge_count) AS maxEdges,
          MIN(depth) AS minDepth,
          MAX(depth) AS maxDepth,
          COUNT(DISTINCT signature_hash) AS signatureCount,
          MAX(block_number) AS maxBlock
   FROM dfg_txs
   WHERE chain_id = $chainId`,
);

const upsertStmt = db.prepare(
  `INSERT INTO dfg_stats_rollups (
     chain_id,
     total,
     avg_nodes,
     avg_edges,
     avg_depth,
     min_nodes,
     max_nodes,
     min_edges,
     max_edges,
     min_depth,
     max_depth,
     signature_count,
     event_tx_count,
     max_block
   )
   VALUES (
     $chainId,
     $total,
     $avgNodes,
     $avgEdges,
     $avgDepth,
     $minNodes,
     $maxNodes,
     $minEdges,
     $maxEdges,
     $minDepth,
     $maxDepth,
     $signatureCount,
     $eventTxCount,
     $maxBlock
   )
   ON CONFLICT(chain_id) DO UPDATE
     SET total = excluded.total,
         avg_nodes = excluded.avg_nodes,
         avg_edges = excluded.avg_edges,
         avg_depth = excluded.avg_depth,
         min_nodes = excluded.min_nodes,
         max_nodes = excluded.max_nodes,
         min_edges = excluded.min_edges,
         max_edges = excluded.max_edges,
         min_depth = excluded.min_depth,
         max_depth = excluded.max_depth,
         signature_count = excluded.signature_count,
         event_tx_count = excluded.event_tx_count,
         max_block = excluded.max_block,
         updated_at = datetime('now')`,
);

for (const rollupChainId of chains) {
  const stats = statsStmt.get({ $chainId: rollupChainId }) as {
    total: number;
    avgNodes: number | null;
    avgEdges: number | null;
    avgDepth: number | null;
    minNodes: number | null;
    maxNodes: number | null;
    minEdges: number | null;
    maxEdges: number | null;
    minDepth: number | null;
    maxDepth: number | null;
    signatureCount: number;
    maxBlock: number | null;
  };

  let eventTxCount: number | null = null;
  if (hasTable(db, "tx_counts")) {
    const txCountRow = db
      .prepare("SELECT count FROM tx_counts WHERE chain_id = $chainId")
      .get({ $chainId: rollupChainId }) as { count: number } | undefined;
    eventTxCount = txCountRow?.count ?? null;
  }

  upsertStmt.run({
    $chainId: rollupChainId,
    $total: stats.total,
    $avgNodes: stats.avgNodes,
    $avgEdges: stats.avgEdges,
    $avgDepth: stats.avgDepth,
    $minNodes: stats.minNodes,
    $maxNodes: stats.maxNodes,
    $minEdges: stats.minEdges,
    $maxEdges: stats.maxEdges,
    $minDepth: stats.minDepth,
    $maxDepth: stats.maxDepth,
    $signatureCount: stats.signatureCount,
    $eventTxCount: eventTxCount,
    $maxBlock: stats.maxBlock,
  });
}

const rows = db
  .prepare(
    `SELECT chain_id AS chainId, total, signature_count AS signatureCount, event_tx_count AS eventTxCount
     FROM dfg_stats_rollups
     ORDER BY chain_id`,
  )
  .all() as Array<{
  chainId: number;
  total: number;
  signatureCount: number;
  eventTxCount: number | null;
}>;

console.log(
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dbPath,
      rows,
    },
    null,
    2,
  ),
);

db.close();
