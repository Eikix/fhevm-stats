import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

type BlockCycleReport = {
  blockNumber: number;
  txCount: number;
  edgeCount: number;
  forwardEdgeCount: number;
  selfLoopCount: number;
  sccs: Array<{
    size: number;
    txs: string[];
    truncated: boolean;
  }>;
};

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseBool(value: string | null | undefined, fallback = false): boolean {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = value.toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
  if (normalized === "0" || normalized === "false" || normalized === "no") return false;
  return fallback;
}

function tarjan(nodes: string[], adjacency: Map<string, Set<string>>): string[][] {
  let index = 0;
  const stack: string[] = [];
  const onStack = new Set<string>();
  const indices = new Map<string, number>();
  const lowlink = new Map<string, number>();
  const sccs: string[][] = [];

  const strongConnect = (node: string) => {
    indices.set(node, index);
    lowlink.set(node, index);
    index += 1;
    stack.push(node);
    onStack.add(node);

    for (const next of adjacency.get(node) ?? []) {
      if (!indices.has(next)) {
        strongConnect(next);
        lowlink.set(node, Math.min(lowlink.get(node) ?? 0, lowlink.get(next) ?? 0));
      } else if (onStack.has(next)) {
        lowlink.set(node, Math.min(lowlink.get(node) ?? 0, indices.get(next) ?? 0));
      }
    }

    if (lowlink.get(node) !== indices.get(node)) return;

    const component: string[] = [];
    for (;;) {
      const popped = stack.pop();
      if (!popped) break;
      onStack.delete(popped);
      component.push(popped);
      if (popped === node) break;
    }
    sccs.push(component);
  };

  for (const node of nodes) {
    if (!indices.has(node)) strongConnect(node);
  }

  return sccs;
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainIdEnv = parseNumber(Bun.env.CHAIN_ID);
const startBlockEnv = parseNumber(Bun.env.START_BLOCK);
const endBlockEnv = parseNumber(Bun.env.END_BLOCK);
const lookbackBlocks = parseNumber(Bun.env.LOOKBACK_BLOCKS, 200) ?? 200;
const onlyCycles = parseBool(Bun.env.ONLY_CYCLES, true);
const failOnCycles = parseBool(Bun.env.FAIL_ON_CYCLES, false);
const maxSccTxs = parseNumber(Bun.env.MAX_SCC_TXS, 10) ?? 10;

const db = new Database(dbPath, { readonly: true });
db.exec("PRAGMA busy_timeout=5000;");

const tables = new Set(
  (
    db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{
      name: string;
    }>
  ).map((row) => row.name),
);

const requiredTables = ["dfg_txs", "dfg_nodes", "dfg_inputs", "fhe_events"];
const missingTables = requiredTables.filter((name) => !tables.has(name));
if (missingTables.length > 0) {
  console.error(
    JSON.stringify(
      {
        error: "missing_tables",
        dbPath,
        missingTables,
      },
      null,
      2,
    ),
  );
  db.close();
  process.exit(1);
}

const availableChains = (
  db.prepare("SELECT DISTINCT chain_id AS chainId FROM dfg_txs ORDER BY chain_id").all() as Array<{
    chainId: number;
  }>
).map((row) => row.chainId);

const chainIds = chainIdEnv ? [chainIdEnv] : availableChains;

const maxBlockStmt = db.prepare(
  "SELECT MAX(block_number) AS maxBlock FROM dfg_txs WHERE chain_id = $chainId",
);
const blocksStmt = db.prepare(
  `SELECT DISTINCT block_number AS blockNumber
   FROM dfg_txs
   WHERE chain_id = $chainId AND block_number >= $startBlock AND block_number <= $endBlock
   ORDER BY block_number`,
);

const txCountStmt = db.prepare(
  "SELECT COUNT(*) AS count FROM dfg_txs WHERE chain_id = $chainId AND block_number = $blockNumber",
);

const producedStmt = db.prepare(
  `SELECT n.output_handle AS handle, n.tx_hash AS txHash
   FROM dfg_nodes n
   JOIN dfg_txs t ON t.chain_id = n.chain_id AND t.tx_hash = n.tx_hash
   WHERE t.chain_id = $chainId AND t.block_number = $blockNumber
     AND n.output_handle IS NOT NULL`,
);

const consumedStmt = db.prepare(
  `SELECT i.handle AS handle, i.tx_hash AS txHash
   FROM dfg_inputs i
   JOIN dfg_txs t ON t.chain_id = i.chain_id AND t.tx_hash = i.tx_hash
   WHERE t.chain_id = $chainId AND t.block_number = $blockNumber
     AND i.kind = 'external'`,
);

const txOrderStmt = db.prepare(
  `SELECT tx_hash AS txHash, MIN(log_index) AS firstLogIndex
   FROM fhe_events
   WHERE chain_id = $chainId AND block_number = $blockNumber
   GROUP BY tx_hash`,
);

const report: Record<string, unknown> = {
  generatedAt: new Date().toISOString(),
  dbPath,
  filters: {
    chainId: chainIdEnv ?? null,
    startBlock: startBlockEnv ?? null,
    endBlock: endBlockEnv ?? null,
    lookbackBlocks,
    onlyCycles,
  },
  tables: {
    missing: missingTables,
  },
  chains: [] as Array<Record<string, unknown>>,
};

let hasAnyCycles = false;

for (const chainId of chainIds) {
  const maxBlockRow = maxBlockStmt.get({ $chainId: chainId }) as { maxBlock: number | null };
  const maxBlock = maxBlockRow.maxBlock ?? null;

  let endBlock = endBlockEnv ?? maxBlock ?? 0;
  let startBlock =
    startBlockEnv ??
    (endBlockEnv !== undefined ? endBlockEnv - lookbackBlocks + 1 : endBlock - lookbackBlocks + 1);
  if (startBlock < 0) startBlock = 0;
  if (endBlock < startBlock) endBlock = startBlock;

  const blocks = blocksStmt.all({
    $chainId: chainId,
    $startBlock: startBlock,
    $endBlock: endBlock,
  }) as Array<{ blockNumber: number }>;

  const blocksWithCycles: BlockCycleReport[] = [];
  let blocksChecked = 0;
  let blocksWithDeps = 0;

  for (const row of blocks) {
    const blockNumber = row.blockNumber;
    blocksChecked += 1;

    const txCountRow = txCountStmt.get({
      $chainId: chainId,
      $blockNumber: blockNumber,
    }) as { count: number };
    const txCount = txCountRow.count ?? 0;
    if (txCount === 0) continue;

    const producedRows = producedStmt.all({
      $chainId: chainId,
      $blockNumber: blockNumber,
    }) as Array<{ handle: string; txHash: string }>;

    const consumedRows = consumedStmt.all({
      $chainId: chainId,
      $blockNumber: blockNumber,
    }) as Array<{ handle: string; txHash: string }>;

    if (producedRows.length === 0 || consumedRows.length === 0) continue;

    const txOrderRows = txOrderStmt.all({
      $chainId: chainId,
      $blockNumber: blockNumber,
    }) as Array<{ txHash: string; firstLogIndex: number }>;
    const firstLogIndexByTx = new Map<string, number>();
    for (const tx of txOrderRows) {
      firstLogIndexByTx.set(tx.txHash, tx.firstLogIndex);
    }

    const producersByHandle = new Map<string, Set<string>>();
    for (const row of producedRows) {
      const handle = row.handle;
      if (!handle) continue;
      let set = producersByHandle.get(handle);
      if (!set) {
        set = new Set<string>();
        producersByHandle.set(handle, set);
      }
      set.add(row.txHash);
    }

    const adjacency = new Map<string, Set<string>>();
    const nodes = new Set<string>();
    let forwardEdgeCount = 0;
    let selfLoopCount = 0;

    for (const input of consumedRows) {
      const producers = producersByHandle.get(input.handle);
      if (!producers) continue;

      for (const producerTx of producers) {
        nodes.add(input.txHash);
        nodes.add(producerTx);

        let outs = adjacency.get(input.txHash);
        if (!outs) {
          outs = new Set<string>();
          adjacency.set(input.txHash, outs);
        }
        outs.add(producerTx);

        if (producerTx === input.txHash) {
          selfLoopCount += 1;
          continue;
        }

        const consumerFirst = firstLogIndexByTx.get(input.txHash);
        const producerFirst = firstLogIndexByTx.get(producerTx);
        if (
          consumerFirst !== undefined &&
          producerFirst !== undefined &&
          producerFirst > consumerFirst
        ) {
          forwardEdgeCount += 1;
        }
      }
    }

    const nodeList = Array.from(nodes);
    if (nodeList.length === 0) continue;
    blocksWithDeps += 1;

    const sccs = tarjan(nodeList, adjacency);
    const cyclicSccs = sccs.filter((component) => {
      if (component.length > 1) return true;
      const lone = component[0];
      if (!lone) return false;
      return adjacency.get(lone)?.has(lone) ?? false;
    });

    if (cyclicSccs.length === 0) {
      if (!onlyCycles) {
        blocksWithCycles.push({
          blockNumber,
          txCount,
          edgeCount: Array.from(adjacency.values()).reduce((acc, set) => acc + set.size, 0),
          forwardEdgeCount,
          selfLoopCount,
          sccs: [],
        });
      }
      continue;
    }

    hasAnyCycles = true;
    blocksWithCycles.push({
      blockNumber,
      txCount,
      edgeCount: Array.from(adjacency.values()).reduce((acc, set) => acc + set.size, 0),
      forwardEdgeCount,
      selfLoopCount,
      sccs: cyclicSccs.map((component) => ({
        size: component.length,
        txs: component.slice(0, maxSccTxs),
        truncated: component.length > maxSccTxs,
      })),
    });
  }

  (report.chains as Array<Record<string, unknown>>).push({
    chainId,
    maxBlock,
    range: { startBlock, endBlock },
    blocksChecked,
    blocksWithDeps,
    blocksWithCycles: blocksWithCycles.length,
    blocks: blocksWithCycles,
  });
}

console.log(JSON.stringify(report, null, 2));
db.close();

if (failOnCycles && hasAnyCycles) {
  process.exit(2);
}
