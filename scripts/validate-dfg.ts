import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

type DfgNodeRow = {
  node_id: number;
  output_handle: string | null;
  type_info_json: string | null;
};

type DfgEdgeRow = {
  from_node_id: number;
  to_node_id: number;
  input_handle: string;
};

type DfgInputInfo = {
  handle?: string;
};

type DfgTypeInfo = {
  inputs?: DfgInputInfo[];
};

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseJson<T>(value: string | null): T | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function setDifference<T>(a: Set<T>, b: Set<T>): Set<T> {
  const diff = new Set<T>();
  for (const value of a) {
    if (!b.has(value)) diff.add(value);
  }
  return diff;
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainIdEnv = parseNumber(Bun.env.CHAIN_ID);
const maxTx = parseNumber(Bun.env.MAX_TX);

const db = new Database(dbPath, { readonly: true });
db.exec("PRAGMA busy_timeout=5000;");

const tables = new Set(
  (
    db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{
      name: string;
    }>
  ).map((row) => row.name),
);

const dfgTables = ["dfg_txs", "dfg_nodes", "dfg_edges", "dfg_inputs"];
const missingTables = dfgTables.filter((name) => !tables.has(name));
const hasDfgTables = missingTables.length === 0;

const hasEvents = tables.has("fhe_events");
if (!hasEvents) {
  console.error("Missing fhe_events table in DB", { dbPath });
  process.exit(1);
}

const availableChains = (
  db
    .prepare("SELECT DISTINCT chain_id AS chainId FROM fhe_events ORDER BY chain_id")
    .all() as Array<{
    chainId: number;
  }>
).map((row) => row.chainId);

const requestedChains = chainIdEnv ? [chainIdEnv] : [1, 11155111];
const chainIds = chainIdEnv
  ? requestedChains
  : requestedChains.filter((chainId) => availableChains.includes(chainId));
const missingChains = requestedChains.filter((chainId) => !availableChains.includes(chainId));

const report: Record<string, unknown> = {
  generatedAt: new Date().toISOString(),
  dbPath,
  requestedChains,
  availableChains,
  missingChains,
  tables: {
    hasDfgTables,
    missing: missingTables,
  },
  chains: [] as Array<Record<string, unknown>>,
};

if (chainIds.length === 0) {
  report.error = "No requested chains found in fhe_events.";
  console.log(JSON.stringify(report, null, 2));
  db.close();
  process.exit(1);
}

const totalsStmt = db.prepare(
  `SELECT COUNT(*) AS eventCount,
          COUNT(DISTINCT tx_hash) AS txCount
   FROM fhe_events
   WHERE chain_id = $chainId`,
);

const dfgTxCountStmt = db.prepare(
  "SELECT COUNT(*) AS count FROM dfg_txs WHERE chain_id = $chainId",
);
const dfgNodeCountStmt = db.prepare(
  "SELECT COUNT(*) AS count FROM dfg_nodes WHERE chain_id = $chainId",
);
const dfgEdgeCountStmt = db.prepare(
  "SELECT COUNT(*) AS count FROM dfg_edges WHERE chain_id = $chainId",
);

const nodeMismatchCountStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_txs t
   LEFT JOIN (
     SELECT tx_hash, COUNT(*) AS nodeCount
     FROM dfg_nodes
     WHERE chain_id = $chainId
     GROUP BY tx_hash
   ) n ON n.tx_hash = t.tx_hash
   WHERE t.chain_id = $chainId AND COALESCE(n.nodeCount, 0) != t.node_count`,
);
const nodeMismatchSampleStmt = db.prepare(
  `SELECT t.tx_hash AS txHash, t.node_count AS expected, n.nodeCount AS actual
   FROM dfg_txs t
   LEFT JOIN (
     SELECT tx_hash, COUNT(*) AS nodeCount
     FROM dfg_nodes
     WHERE chain_id = $chainId
     GROUP BY tx_hash
   ) n ON n.tx_hash = t.tx_hash
   WHERE t.chain_id = $chainId AND COALESCE(n.nodeCount, 0) != t.node_count
   LIMIT 5`,
);

const edgeMismatchCountStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_txs t
   LEFT JOIN (
     SELECT tx_hash, COUNT(*) AS edgeCount
     FROM dfg_edges
     WHERE chain_id = $chainId
     GROUP BY tx_hash
   ) e ON e.tx_hash = t.tx_hash
   WHERE t.chain_id = $chainId AND COALESCE(e.edgeCount, 0) != t.edge_count`,
);
const edgeMismatchSampleStmt = db.prepare(
  `SELECT t.tx_hash AS txHash, t.edge_count AS expected, e.edgeCount AS actual
   FROM dfg_txs t
   LEFT JOIN (
     SELECT tx_hash, COUNT(*) AS edgeCount
     FROM dfg_edges
     WHERE chain_id = $chainId
     GROUP BY tx_hash
   ) e ON e.tx_hash = t.tx_hash
   WHERE t.chain_id = $chainId AND COALESCE(e.edgeCount, 0) != t.edge_count
   LIMIT 5`,
);

const edgeMissingFromStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_edges e
   LEFT JOIN dfg_nodes n
     ON n.chain_id = e.chain_id AND n.tx_hash = e.tx_hash AND n.node_id = e.from_node_id
   WHERE e.chain_id = $chainId AND n.node_id IS NULL`,
);
const edgeMissingToStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_edges e
   LEFT JOIN dfg_nodes n
     ON n.chain_id = e.chain_id AND n.tx_hash = e.tx_hash AND n.node_id = e.to_node_id
   WHERE e.chain_id = $chainId AND n.node_id IS NULL`,
);
const edgeHandleMismatchStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_edges e
   JOIN dfg_nodes n
     ON n.chain_id = e.chain_id AND n.tx_hash = e.tx_hash AND n.node_id = e.from_node_id
   WHERE e.chain_id = $chainId AND (n.output_handle IS NULL OR n.output_handle != e.input_handle)`,
);
const duplicateHandleStmt = db.prepare(
  `SELECT COUNT(*) AS count FROM (
     SELECT tx_hash, output_handle, COUNT(*) AS count
     FROM dfg_nodes
     WHERE chain_id = $chainId AND output_handle IS NOT NULL
     GROUP BY tx_hash, output_handle
     HAVING count > 1
   )`,
);
const duplicateHandleByOpStmt = db.prepare(
  `SELECT op, COUNT(*) AS count FROM (
     SELECT tx_hash, output_handle, op, COUNT(*) AS dupCount
     FROM dfg_nodes
     WHERE chain_id = $chainId AND output_handle IS NOT NULL
     GROUP BY tx_hash, output_handle, op
     HAVING dupCount > 1
   )
   GROUP BY op
   ORDER BY count DESC
   LIMIT 5`,
);
const externalProducedStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM dfg_inputs i
   JOIN dfg_nodes n
     ON n.chain_id = i.chain_id AND n.tx_hash = i.tx_hash AND n.output_handle = i.handle
   WHERE i.chain_id = $chainId`,
);
const externalProducedByOpStmt = db.prepare(
  `SELECT n.op AS op, COUNT(*) AS count
   FROM dfg_inputs i
   JOIN dfg_nodes n
     ON n.chain_id = i.chain_id AND n.tx_hash = i.tx_hash AND n.output_handle = i.handle
   WHERE i.chain_id = $chainId
   GROUP BY n.op
   ORDER BY count DESC
   LIMIT 5`,
);

const txListStmt = db.prepare(
  `SELECT tx_hash AS txHash, depth, node_count AS nodeCount, edge_count AS edgeCount
   FROM dfg_txs
   WHERE chain_id = $chainId
   ORDER BY block_number, tx_hash`,
);
const nodeStmt = db.prepare(
  `SELECT node_id, output_handle, type_info_json
   FROM dfg_nodes
   WHERE chain_id = $chainId AND tx_hash = $txHash
   ORDER BY node_id`,
);
const edgeStmt = db.prepare(
  `SELECT from_node_id, to_node_id, input_handle
   FROM dfg_edges
   WHERE chain_id = $chainId AND tx_hash = $txHash`,
);
const inputStmt = db.prepare(
  `SELECT handle FROM dfg_inputs
   WHERE chain_id = $chainId AND tx_hash = $txHash`,
);

for (const chainId of chainIds) {
  const totals = totalsStmt.get({ $chainId: chainId }) as {
    eventCount: number;
    txCount: number;
  };

  const chainReport: Record<string, unknown> = {
    chainId,
    totals,
  };

  if (!hasDfgTables) {
    chainReport.warning = "dfg_tables_missing";
    (report.chains as Array<Record<string, unknown>>).push(chainReport);
    continue;
  }

  const dfgTxCount = dfgTxCountStmt.get({ $chainId: chainId }) as { count: number };
  const dfgNodeCount = dfgNodeCountStmt.get({ $chainId: chainId }) as { count: number };
  const dfgEdgeCount = dfgEdgeCountStmt.get({ $chainId: chainId }) as { count: number };

  const coverage = totals.txCount > 0 ? Number((dfgTxCount.count / totals.txCount).toFixed(4)) : 0;

  const nodeMismatchCount = nodeMismatchCountStmt.get({ $chainId: chainId }) as { count: number };
  const nodeMismatchSample = nodeMismatchSampleStmt.all({ $chainId: chainId });
  const edgeMismatchCount = edgeMismatchCountStmt.get({ $chainId: chainId }) as { count: number };
  const edgeMismatchSample = edgeMismatchSampleStmt.all({ $chainId: chainId });

  const edgeMissingFrom = edgeMissingFromStmt.get({ $chainId: chainId }) as { count: number };
  const edgeMissingTo = edgeMissingToStmt.get({ $chainId: chainId }) as { count: number };
  const edgeHandleMismatch = edgeHandleMismatchStmt.get({ $chainId: chainId }) as {
    count: number;
  };
  const duplicateOutputHandles = duplicateHandleStmt.get({ $chainId: chainId }) as {
    count: number;
  };
  const duplicateOutputHandlesByOp = duplicateHandleByOpStmt.all({ $chainId: chainId });
  const externalInputsProduced = externalProducedStmt.get({ $chainId: chainId }) as {
    count: number;
  };
  const externalInputsProducedByOp = externalProducedByOpStmt.all({ $chainId: chainId });

  const deep = {
    txsChecked: 0,
    missingEdges: 0,
    extraEdges: 0,
    externalMissing: 0,
    externalExtra: 0,
    depthMismatch: 0,
    samples: {
      missingEdges: [] as string[],
      extraEdges: [] as string[],
      externalMissing: [] as string[],
      externalExtra: [] as string[],
      depthMismatch: [] as string[],
    },
  };

  const txRows = txListStmt.all({ $chainId: chainId }) as Array<{
    txHash: string;
    depth: number;
    nodeCount: number;
    edgeCount: number;
  }>;
  const txLimit = maxTx && maxTx > 0 ? Math.min(maxTx, txRows.length) : txRows.length;

  for (let i = 0; i < txLimit; i += 1) {
    const tx = txRows[i];
    if (!tx) continue;
    const nodes = nodeStmt.all({ $chainId: chainId, $txHash: tx.txHash }) as DfgNodeRow[];
    const edges = edgeStmt.all({ $chainId: chainId, $txHash: tx.txHash }) as DfgEdgeRow[];
    const inputs = inputStmt.all({ $chainId: chainId, $txHash: tx.txHash }) as Array<{
      handle: string;
    }>;

    const actualEdges = new Set(
      edges.map((edge) => `${edge.from_node_id}|${edge.to_node_id}|${edge.input_handle}`),
    );

    const expectedEdges = new Set<string>();
    const expectedExternal = new Set<string>();
    const producedHandles = new Map<string, number>();
    const depths = new Map<number, number>();
    let maxDepth = 0;

    for (const node of nodes) {
      const typeInfo = parseJson<DfgTypeInfo>(node.type_info_json);
      const inputsInfo = typeInfo?.inputs ?? [];
      let parentDepth = 0;
      for (const input of inputsInfo) {
        if (!input.handle) continue;
        const producer = producedHandles.get(input.handle);
        if (producer !== undefined && producer < node.node_id) {
          expectedEdges.add(`${producer}|${node.node_id}|${input.handle}`);
          parentDepth = Math.max(parentDepth, depths.get(producer) ?? 1);
        } else {
          expectedExternal.add(input.handle);
        }
      }

      const depth = parentDepth > 0 ? parentDepth + 1 : 1;
      depths.set(node.node_id, depth);
      maxDepth = Math.max(maxDepth, depth);

      if (node.output_handle) {
        producedHandles.set(node.output_handle, node.node_id);
      }
    }

    const missingEdges = setDifference(expectedEdges, actualEdges);
    const extraEdges = setDifference(actualEdges, expectedEdges);
    const recordedExternal = new Set(inputs.map((input) => input.handle));
    const missingExternal = setDifference(expectedExternal, recordedExternal);
    const extraExternal = setDifference(recordedExternal, expectedExternal);

    if (missingEdges.size > 0) {
      deep.missingEdges += missingEdges.size;
      if (deep.samples.missingEdges.length < 5) deep.samples.missingEdges.push(tx.txHash);
    }
    if (extraEdges.size > 0) {
      deep.extraEdges += extraEdges.size;
      if (deep.samples.extraEdges.length < 5) deep.samples.extraEdges.push(tx.txHash);
    }
    if (missingExternal.size > 0) {
      deep.externalMissing += missingExternal.size;
      if (deep.samples.externalMissing.length < 5) deep.samples.externalMissing.push(tx.txHash);
    }
    if (extraExternal.size > 0) {
      deep.externalExtra += extraExternal.size;
      if (deep.samples.externalExtra.length < 5) deep.samples.externalExtra.push(tx.txHash);
    }
    if (maxDepth !== tx.depth) {
      deep.depthMismatch += 1;
      if (deep.samples.depthMismatch.length < 5) deep.samples.depthMismatch.push(tx.txHash);
    }

    deep.txsChecked += 1;
  }

  chainReport.dfgTotals = {
    txs: dfgTxCount.count,
    nodes: dfgNodeCount.count,
    edges: dfgEdgeCount.count,
    coverage,
  };
  chainReport.mismatches = {
    nodeCountMismatch: { count: nodeMismatchCount.count, sample: nodeMismatchSample },
    edgeCountMismatch: { count: edgeMismatchCount.count, sample: edgeMismatchSample },
    edgeMissingFromNode: edgeMissingFrom.count,
    edgeMissingToNode: edgeMissingTo.count,
    edgeHandleMismatch: edgeHandleMismatch.count,
    duplicateOutputHandles: duplicateOutputHandles.count,
    duplicateOutputHandlesByOp,
    externalInputsProduced: externalInputsProduced.count,
    externalInputsProducedByOp,
    deep,
  };

  (report.chains as Array<Record<string, unknown>>).push(chainReport);
}

console.log(JSON.stringify(report, null, 2));
db.close();
