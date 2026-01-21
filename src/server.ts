import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const DEFAULT_PORT = 4310;

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const defaultChainId = parseNumber(Bun.env.CHAIN_ID);
const port = parseNumber(Bun.env.HTTP_PORT, DEFAULT_PORT) ?? DEFAULT_PORT;
const db = new Database(dbPath, { readonly: true });
db.exec("PRAGMA busy_timeout=5000;");

type Filters = {
  chainId?: number;
  startBlock?: number;
  endBlock?: number;
  eventName?: string;
};

type DepStats = {
  totalTxs: number;
  dependentTxs: number;
  independentTxs: number;
  avgUpstreamTxs: number;
  avgUpstreamHandles: number;
  maxUpstreamTxs: number;
  maxUpstreamHandles: number;
  parallelismRatio: number;
  maxChainDepth: number;
  chainDepthDistribution: Record<number, number>;
};

const TYPE_COLUMNS: Record<string, string> = {
  result: "result_type",
  lhs: "lhs_type",
  rhs: "rhs_type",
  input: "input_type",
  cast_to: "cast_to_type",
  rand: "rand_type",
  control: "control_type",
  if_true: "if_true_type",
  if_false: "if_false_type",
};

function hasTable(name: string): boolean {
  const row = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = $name")
    .get({ $name: name }) as { name: string } | undefined;
  return Boolean(row);
}

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function buildWhereClause(filters: Filters): {
  clause: string;
  params: Record<string, string | number>;
} {
  const clauses: string[] = [];
  const params: Record<string, string | number> = {};

  if (filters.chainId !== undefined) {
    clauses.push("chain_id = $chainId");
    params.$chainId = filters.chainId;
  }
  if (filters.startBlock !== undefined) {
    clauses.push("block_number >= $startBlock");
    params.$startBlock = filters.startBlock;
  }
  if (filters.endBlock !== undefined) {
    clauses.push("block_number <= $endBlock");
    params.$endBlock = filters.endBlock;
  }
  if (filters.eventName) {
    clauses.push("event_name = $eventName");
    params.$eventName = filters.eventName;
  }

  if (clauses.length === 0) return { clause: "", params };
  return { clause: `WHERE ${clauses.join(" AND ")}`, params };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json",
      "access-control-allow-origin": "*",
    },
  });
}

function parseFilters(url: URL): Filters {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));
  const eventName = url.searchParams.get("eventName") ?? undefined;
  return { chainId, startBlock, endBlock, eventName };
}

function handleHealth(): Response {
  return jsonResponse({ status: "ok", dbPath, defaultChainId, port });
}

function handleDbStats(): Response {
  const totalRow = db.prepare("SELECT COUNT(*) AS count FROM fhe_events").get() as {
    count: number;
  };
  const sizeRow = db
    .prepare(
      "SELECT page_count * page_size AS sizeBytes FROM pragma_page_count(), pragma_page_size()",
    )
    .get() as { sizeBytes: number };

  return jsonResponse({
    dbPath,
    events: totalRow.count,
    sizeBytes: sizeRow.sizeBytes,
  });
}

function handleIngestion(url: URL): Response {
  const { chainId } = parseFilters(url);
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }

  const eventRow = db
    .prepare(
      `SELECT MAX(block_number) AS maxBlock,
              MAX(created_at) AS lastEventAt,
              COUNT(*) AS eventCount
       FROM fhe_events
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as
    | { maxBlock: number | null; lastEventAt: string | null; eventCount: number }
    | undefined;

  const checkpointRow = db
    .prepare(
      `SELECT last_block AS lastBlock,
              updated_at AS updatedAt
       FROM checkpoints
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as { lastBlock: number; updatedAt: string } | undefined;

  return jsonResponse({
    chainId,
    events: {
      maxBlock: eventRow?.maxBlock ?? null,
      lastEventAt: eventRow?.lastEventAt ?? null,
      count: eventRow?.eventCount ?? 0,
    },
    checkpoint: {
      lastBlock: checkpointRow?.lastBlock ?? null,
      updatedAt: checkpointRow?.updatedAt ?? null,
    },
  });
}

function handleOps(url: URL): Response {
  const filters = parseFilters(url);
  const chainId = filters.chainId;
  const canUseRollup =
    chainId !== undefined && filters.startBlock === undefined && filters.endBlock === undefined;
  if (canUseRollup) {
    const bucketRow = db
      .prepare(
        "SELECT MAX(bucket_seconds) AS bucketSeconds FROM op_buckets WHERE chain_id = $chainId",
      )
      .get({ $chainId: chainId }) as { bucketSeconds: number | null };
    if (bucketRow?.bucketSeconds) {
      const params: Record<string, string | number> = {
        $chainId: chainId,
        $bucketSeconds: bucketRow.bucketSeconds,
      };
      const extra = filters.eventName ? " AND event_name = $eventName" : "";
      if (filters.eventName) params.$eventName = filters.eventName;

      const rows = db
        .prepare(
          `SELECT event_name AS eventName, SUM(count) AS count
           FROM op_buckets
           WHERE chain_id = $chainId AND bucket_seconds = $bucketSeconds${extra}
           GROUP BY event_name
           ORDER BY count DESC`,
        )
        .all(params) as Array<{ eventName: string; count: number }>;

      return jsonResponse({
        filters,
        source: "rollup",
        bucketSeconds: bucketRow.bucketSeconds,
        rows,
      });
    }
  }

  const { clause, params } = buildWhereClause(filters);
  const rows = db
    .prepare(
      `SELECT event_name AS eventName, COUNT(*) AS count
       FROM fhe_events
       ${clause}
       GROUP BY event_name
       ORDER BY count DESC`,
    )
    .all(params) as Array<{ eventName: string; count: number }>;

  return jsonResponse({ filters, source: "raw", rows });
}

function handleSummary(url: URL): Response {
  const filters = parseFilters(url);
  const { clause, params } = buildWhereClause(filters);
  const row = db
    .prepare(
      `SELECT COUNT(*) AS count,
              MIN(block_number) AS minBlock,
              MAX(block_number) AS maxBlock
       FROM fhe_events
       ${clause}`,
    )
    .get(params) as { count: number; minBlock: number | null; maxBlock: number | null };

  return jsonResponse({ filters, summary: row });
}

function handleBuckets(url: URL): Response {
  const filters = parseFilters(url);
  const bucketSize = parseNumber(url.searchParams.get("bucketSize"), 1000) ?? 1000;
  const { clause, params } = buildWhereClause(filters);
  const rows = db
    .prepare(
      `SELECT (CAST(block_number / $bucketSize AS INTEGER) * $bucketSize) AS bucketStart,
              COUNT(*) AS count
       FROM fhe_events
       ${clause}
       GROUP BY bucketStart
       ORDER BY bucketStart`,
    )
    .all({ ...params, $bucketSize: bucketSize }) as Array<{ bucketStart: number; count: number }>;

  return jsonResponse({ filters, bucketSize, rows });
}

function handleTypes(url: URL): Response {
  const filters = parseFilters(url);
  const role = url.searchParams.get("role") ?? "result";
  const column = TYPE_COLUMNS[role];
  if (!column) {
    return jsonResponse(
      {
        error: "invalid_role",
        allowedRoles: Object.keys(TYPE_COLUMNS),
      },
      400,
    );
  }

  const { clause, params } = buildWhereClause(filters);
  const extraFilter = clause
    ? `${clause} AND ${column} IS NOT NULL`
    : `WHERE ${column} IS NOT NULL`;
  const rows = db
    .prepare(
      `SELECT ${column} AS typeValue, COUNT(*) AS count
       FROM fhe_events
       ${extraFilter}
       GROUP BY ${column}
       ORDER BY count DESC`,
    )
    .all(params) as Array<{ typeValue: number; count: number }>;

  return jsonResponse({ filters, role, rows });
}

function handleOpTypes(url: URL): Response {
  const filters = parseFilters(url);
  const role = url.searchParams.get("role") ?? "result";
  const includeScalar = url.searchParams.get("includeScalar") === "1";
  const column = TYPE_COLUMNS[role];
  if (!column) {
    return jsonResponse(
      {
        error: "invalid_role",
        allowedRoles: Object.keys(TYPE_COLUMNS),
      },
      400,
    );
  }

  const { clause, params } = buildWhereClause(filters);
  const baseClause = clause ? `${clause} AND ${column} IS NOT NULL` : `WHERE ${column} IS NOT NULL`;

  const typeRows = db
    .prepare(
      `SELECT event_name AS eventName, CAST(${column} AS TEXT) AS typeValue, COUNT(*) AS count
       FROM fhe_events
       ${baseClause}
       GROUP BY event_name, ${column}
       ORDER BY event_name, count DESC`,
    )
    .all(params) as Array<{ eventName: string; typeValue: string; count: number }>;

  let rows = typeRows;
  if (includeScalar && role === "rhs") {
    const scalarClause = clause ? `${clause} AND scalar_flag = 1` : "WHERE scalar_flag = 1";
    const scalarRows = db
      .prepare(
        `SELECT event_name AS eventName, 'scalar' AS typeValue, COUNT(*) AS count
         FROM fhe_events
         ${scalarClause}
         GROUP BY event_name`,
      )
      .all(params) as Array<{ eventName: string; typeValue: string; count: number }>;
    rows = [...typeRows, ...scalarRows];
  }

  const totalRows = db
    .prepare(
      `SELECT event_name AS eventName, COUNT(*) AS count
       FROM fhe_events
       ${clause}
       GROUP BY event_name
       ORDER BY count DESC`,
    )
    .all(params) as Array<{ eventName: string; count: number }>;

  return jsonResponse({ filters, role, includeScalar, rows, totals: totalRows });
}

function parseJson(value: string | null): unknown | null {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function normalizeDepStats(value: unknown): DepStats | null {
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  const numRecord = record as Record<string, number>;
  const chainDepthDistribution = (record.chainDepthDistribution as Record<number, number>) ?? {};
  if (
    typeof numRecord.totalTxs === "number" &&
    typeof numRecord.dependentTxs === "number" &&
    typeof numRecord.sumUpstreamTxs === "number" &&
    typeof numRecord.sumUpstreamHandles === "number"
  ) {
    const dependentTxs = numRecord.dependentTxs;
    const totalTxs = numRecord.totalTxs;
    const independentTxs = Math.max(totalTxs - dependentTxs, 0);
    const avgUpstreamTxs = dependentTxs > 0 ? numRecord.sumUpstreamTxs / dependentTxs : 0;
    const avgUpstreamHandles = dependentTxs > 0 ? numRecord.sumUpstreamHandles / dependentTxs : 0;
    const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 0;
    return {
      totalTxs,
      dependentTxs,
      independentTxs,
      avgUpstreamTxs,
      avgUpstreamHandles,
      maxUpstreamTxs: numRecord.maxUpstreamTxs ?? 0,
      maxUpstreamHandles: numRecord.maxUpstreamHandles ?? 0,
      parallelismRatio,
      maxChainDepth: numRecord.maxChainDepth ?? 0,
      chainDepthDistribution,
    };
  }
  if (
    typeof numRecord.totalTxs === "number" &&
    typeof numRecord.dependentTxs === "number" &&
    typeof numRecord.avgUpstreamTxs === "number" &&
    typeof numRecord.avgUpstreamHandles === "number"
  ) {
    const totalTxs = numRecord.totalTxs;
    const independentTxs = Math.max(totalTxs - numRecord.dependentTxs, 0);
    const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 0;
    return {
      totalTxs,
      dependentTxs: numRecord.dependentTxs,
      independentTxs,
      avgUpstreamTxs: numRecord.avgUpstreamTxs,
      avgUpstreamHandles: numRecord.avgUpstreamHandles,
      maxUpstreamTxs: numRecord.maxUpstreamTxs ?? 0,
      maxUpstreamHandles: numRecord.maxUpstreamHandles ?? 0,
      parallelismRatio,
      maxChainDepth: numRecord.maxChainDepth ?? 0,
      chainDepthDistribution,
    };
  }
  return null;
}

function handleDfgTxs(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_txs")) {
    return jsonResponse({
      filters: { chainId },
      limit: 0,
      offset: 0,
      rows: [],
      total: 0,
      warning: "dfg_tables_missing",
    });
  }

  const limit = parseNumber(url.searchParams.get("limit"), 25) ?? 25;
  const offset = parseNumber(url.searchParams.get("offset"), 0) ?? 0;
  const minNodes = parseNumber(url.searchParams.get("minNodes"));
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;

  const clauses = ["chain_id = $chainId"];
  const params: Record<string, string | number> = { $chainId: chainId };

  if (minNodes !== undefined) {
    clauses.push("node_count >= $minNodes");
    params.$minNodes = minNodes;
  }
  if (signatureHash) {
    clauses.push("signature_hash = $signatureHash");
    params.$signatureHash = signatureHash;
  }

  const rows = db
    .prepare(
      `SELECT tx_hash AS txHash,
              block_number AS blockNumber,
              node_count AS nodeCount,
              edge_count AS edgeCount,
              depth,
              signature_hash AS signatureHash,
              stats_json AS statsJson
       FROM dfg_txs
       WHERE ${clauses.join(" AND ")}
       ORDER BY block_number DESC, tx_hash DESC
       LIMIT $limit OFFSET $offset`,
    )
    .all({ ...params, $limit: limit, $offset: offset }) as Array<{
    txHash: string;
    blockNumber: number;
    nodeCount: number;
    edgeCount: number;
    depth: number;
    signatureHash: string | null;
    statsJson: string | null;
  }>;

  const normalized = rows.map((row) => ({
    txHash: row.txHash,
    blockNumber: row.blockNumber,
    nodeCount: row.nodeCount,
    edgeCount: row.edgeCount,
    depth: row.depth,
    signatureHash: row.signatureHash,
    stats: parseJson(row.statsJson),
  }));

  const totalRow = db
    .prepare(
      `SELECT COUNT(*) AS count
       FROM dfg_txs
       WHERE ${clauses.join(" AND ")}`,
    )
    .get(params) as { count: number };

  return jsonResponse({
    filters: { chainId, minNodes, signatureHash },
    limit,
    offset,
    rows: normalized,
    total: totalRow.count,
  });
}

function handleDfgTx(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  const txHash = url.searchParams.get("txHash");
  if (!txHash) {
    return jsonResponse({ error: "tx_hash_required" }, 400);
  }

  const txRow = db
    .prepare(
      `SELECT tx_hash AS txHash,
              block_number AS blockNumber,
              node_count AS nodeCount,
              edge_count AS edgeCount,
              depth,
              signature_hash AS signatureHash,
              stats_json AS statsJson
       FROM dfg_txs
       WHERE chain_id = $chainId AND tx_hash = $txHash`,
    )
    .get({ $chainId: chainId, $txHash: txHash }) as
    | {
        txHash: string;
        blockNumber: number;
        nodeCount: number;
        edgeCount: number;
        depth: number;
        signatureHash: string | null;
        statsJson: string | null;
      }
    | undefined;

  if (!txRow) {
    return jsonResponse({ error: "not_found" }, 404);
  }

  const nodes = db
    .prepare(
      `SELECT node_id AS nodeId,
              op,
              output_handle AS outputHandle,
              input_count AS inputCount,
              scalar_flag AS scalarFlag,
              type_info_json AS typeInfoJson
       FROM dfg_nodes
       WHERE chain_id = $chainId AND tx_hash = $txHash
       ORDER BY node_id`,
    )
    .all({ $chainId: chainId, $txHash: txHash }) as Array<{
    nodeId: number;
    op: string;
    outputHandle: string | null;
    inputCount: number;
    scalarFlag: number | null;
    typeInfoJson: string | null;
  }>;

  const edges = db
    .prepare(
      `SELECT from_node_id AS fromNodeId,
              to_node_id AS toNodeId,
              input_handle AS inputHandle
       FROM dfg_edges
       WHERE chain_id = $chainId AND tx_hash = $txHash
       ORDER BY from_node_id, to_node_id`,
    )
    .all({ $chainId: chainId, $txHash: txHash }) as Array<{
    fromNodeId: number;
    toNodeId: number;
    inputHandle: string;
  }>;

  const inputs = db
    .prepare(
      `SELECT handle, kind
       FROM dfg_inputs
       WHERE chain_id = $chainId AND tx_hash = $txHash
       ORDER BY handle`,
    )
    .all({ $chainId: chainId, $txHash: txHash }) as Array<{ handle: string; kind: string }>;

  return jsonResponse({
    tx: {
      txHash: txRow.txHash,
      blockNumber: txRow.blockNumber,
      nodeCount: txRow.nodeCount,
      edgeCount: txRow.edgeCount,
      depth: txRow.depth,
      signatureHash: txRow.signatureHash,
      stats: parseJson(txRow.statsJson),
    },
    nodes: nodes.map((node) => ({
      nodeId: node.nodeId,
      op: node.op,
      outputHandle: node.outputHandle,
      inputCount: node.inputCount,
      scalarFlag: node.scalarFlag,
      typeInfo: parseJson(node.typeInfoJson),
    })),
    edges,
    inputs,
  });
}

function handleDfgSignatures(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_txs")) {
    return jsonResponse({
      filters: { chainId },
      limit: 0,
      offset: 0,
      rows: [],
      total: 0,
      warning: "dfg_tables_missing",
    });
  }

  const limit = parseNumber(url.searchParams.get("limit"), 10) ?? 10;
  const offset = parseNumber(url.searchParams.get("offset"), 0) ?? 0;
  const minNodes = parseNumber(url.searchParams.get("minNodes"), 1) ?? 1;
  const minEdges = parseNumber(url.searchParams.get("minEdges"), 0) ?? 0;

  const rows = db
    .prepare(
      `SELECT signature_hash AS signatureHash,
              COUNT(*) AS txCount,
              AVG(node_count) AS avgNodes,
              AVG(edge_count) AS avgEdges
       FROM dfg_txs
       WHERE chain_id = $chainId AND signature_hash IS NOT NULL
         AND node_count >= $minNodes AND edge_count >= $minEdges
       GROUP BY signature_hash
       ORDER BY txCount DESC
       LIMIT $limit OFFSET $offset`,
    )
    .all({
      $chainId: chainId,
      $limit: limit,
      $offset: offset,
      $minNodes: minNodes,
      $minEdges: minEdges,
    }) as Array<{
    signatureHash: string;
    txCount: number;
    avgNodes: number;
    avgEdges: number;
  }>;

  const totalRow = db
    .prepare(
      `SELECT COUNT(DISTINCT signature_hash) AS count
       FROM dfg_txs
       WHERE chain_id = $chainId AND signature_hash IS NOT NULL
         AND node_count >= $minNodes AND edge_count >= $minEdges`,
    )
    .get({ $chainId: chainId, $minNodes: minNodes, $minEdges: minEdges }) as { count: number };

  return jsonResponse({
    filters: { chainId, minNodes, minEdges },
    limit,
    offset,
    rows,
    total: totalRow.count,
  });
}

function handleDfgStats(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const includeDeps = url.searchParams.get("includeDeps") === "1";
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  const dfgRow = db
    .prepare(
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
              COUNT(DISTINCT signature_hash) AS signatureCount
       FROM dfg_txs
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as {
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
  };

  const txRow = db
    .prepare("SELECT COUNT(DISTINCT tx_hash) AS total FROM fhe_events WHERE chain_id = $chainId")
    .get({ $chainId: chainId }) as { total: number };

  const coverage = txRow.total > 0 ? dfgRow.total / txRow.total : 0;

  let deps: DepStats | null = null;

  if (hasTable("dfg_dep_rollups")) {
    const depsRow = db
      .prepare(
        `SELECT stats_json AS statsJson
         FROM dfg_dep_rollups
         WHERE chain_id = $chainId`,
      )
      .get({ $chainId: chainId }) as { statsJson: string } | undefined;
    deps = depsRow ? normalizeDepStats(parseJson(depsRow.statsJson)) : null;
  }

  if (!deps && includeDeps && hasTable("dfg_tx_deps")) {
    const depRow = db
      .prepare(
        `SELECT
           COUNT(*) AS totalTxs,
           SUM(CASE WHEN upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
           SUM(CASE WHEN upstream_txs > 0 THEN upstream_txs ELSE 0 END) AS sumUpstreamTxs,
           SUM(CASE WHEN upstream_txs > 0 THEN handle_links ELSE 0 END) AS sumUpstreamHandles,
           MAX(upstream_txs) AS maxUpstreamTxs,
           MAX(handle_links) AS maxUpstreamHandles
         FROM dfg_tx_deps
         WHERE chain_id = $chainId`,
      )
      .get({ $chainId: chainId }) as {
      totalTxs: number;
      dependentTxs: number;
      sumUpstreamTxs: number;
      sumUpstreamHandles: number;
      maxUpstreamTxs: number | null;
      maxUpstreamHandles: number | null;
    };
    deps = normalizeDepStats(depRow as unknown);
  }

  if (!deps && includeDeps && hasTable("dfg_nodes") && hasTable("dfg_inputs")) {
    const depRow = db
      .prepare(
        `WITH produced AS (
           SELECT n.output_handle AS handle,
                  n.tx_hash AS producer_tx,
                  t.block_number AS producer_block
           FROM dfg_nodes n
           JOIN dfg_txs t
             ON t.chain_id = n.chain_id AND t.tx_hash = n.tx_hash
           WHERE n.chain_id = $chainId AND n.output_handle IS NOT NULL
         ),
         consumed AS (
           SELECT i.handle, i.tx_hash AS consumer_tx
           FROM dfg_inputs i
           WHERE i.chain_id = $chainId AND i.kind = 'external'
         ),
         consumers AS (
           SELECT tx_hash AS consumer_tx, block_number AS consumer_block
           FROM dfg_txs
           WHERE chain_id = $chainId
         ),
         matches AS (
           SELECT c.consumer_tx AS consumer_tx, p.producer_tx AS producer_tx
           FROM consumed c
           JOIN produced p ON p.handle = c.handle
           JOIN consumers cc ON cc.consumer_tx = c.consumer_tx
           WHERE p.producer_block <= cc.consumer_block AND p.producer_tx != c.consumer_tx
         ),
         per_tx AS (
           SELECT consumer_tx,
                  COUNT(*) AS handleLinks,
                  COUNT(DISTINCT producer_tx) AS upstreamTxs
           FROM matches
           GROUP BY consumer_tx
         )
         SELECT
           (SELECT COUNT(*) FROM dfg_txs WHERE chain_id = $chainId) AS totalTxs,
           (SELECT COUNT(*) FROM per_tx) AS dependentTxs,
           (SELECT COALESCE(AVG(upstreamTxs), 0) FROM per_tx) AS avgUpstreamTxs,
           (SELECT COALESCE(AVG(handleLinks), 0) FROM per_tx) AS avgUpstreamHandles,
           (SELECT COALESCE(MAX(upstreamTxs), 0) FROM per_tx) AS maxUpstreamTxs,
           (SELECT COALESCE(MAX(handleLinks), 0) FROM per_tx) AS maxUpstreamHandles`,
      )
      .get({ $chainId: chainId }) as {
      totalTxs: number;
      dependentTxs: number;
      avgUpstreamTxs: number | null;
      avgUpstreamHandles: number | null;
      maxUpstreamTxs: number | null;
      maxUpstreamHandles: number | null;
    };

    if (depRow) {
      const totalTxs = depRow.totalTxs ?? 0;
      const dependentTxs = depRow.dependentTxs ?? 0;
      const independentTxs = Math.max(totalTxs - dependentTxs, 0);
      const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 0;
      deps = {
        totalTxs,
        dependentTxs,
        independentTxs,
        avgUpstreamTxs: depRow.avgUpstreamTxs ?? 0,
        avgUpstreamHandles: depRow.avgUpstreamHandles ?? 0,
        maxUpstreamTxs: depRow.maxUpstreamTxs ?? 0,
        maxUpstreamHandles: depRow.maxUpstreamHandles ?? 0,
        parallelismRatio,
        maxChainDepth: 0, // Unavailable in legacy fallback
        chainDepthDistribution: {}, // Unavailable in legacy fallback
      };
    }
  }

  return jsonResponse({
    chainId,
    dfg: dfgRow,
    totalTxs: txRow.total,
    coverage,
    deps,
  });
}

function handleDfgRollup(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_rollups")) {
    return jsonResponse({ error: "dfg_rollup_missing" }, 404);
  }

  const row = db
    .prepare(
      `SELECT dfg_tx_count AS dfgTxCount,
              stats_json AS statsJson,
              updated_at AS updatedAt
       FROM dfg_rollups
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as
    | { dfgTxCount: number; statsJson: string; updatedAt: string }
    | undefined;

  if (!row) {
    return jsonResponse({ error: "dfg_rollup_missing" }, 404);
  }

  return jsonResponse({
    chainId,
    dfgTxCount: row.dfgTxCount,
    updatedAt: row.updatedAt,
    stats: parseJson(row.statsJson),
  });
}

function handleDfgExport(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_dep_rollups")) {
    return jsonResponse({ error: "dfg_dep_rollups_missing" }, 404);
  }

  const depsRow = db
    .prepare(
      `SELECT stats_json AS statsJson
       FROM dfg_dep_rollups
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as { statsJson: string } | undefined;

  if (!depsRow) {
    return jsonResponse({ error: "no_dep_rollup_for_chain" }, 404);
  }

  const deps = normalizeDepStats(parseJson(depsRow.statsJson));
  if (!deps) {
    return jsonResponse({ error: "invalid_dep_rollup_data" }, 500);
  }

  return jsonResponse({
    chainId,
    summary: {
      totalTxs: deps.totalTxs,
      dependentTxs: deps.dependentTxs,
      independentTxs: deps.independentTxs,
      parallelismRatio: deps.parallelismRatio,
      maxChainDepth: deps.maxChainDepth,
      avgUpstreamTxs: deps.avgUpstreamTxs,
      avgUpstreamHandles: deps.avgUpstreamHandles,
      maxUpstreamTxs: deps.maxUpstreamTxs,
      maxUpstreamHandles: deps.maxUpstreamHandles,
    },
    distribution: {
      chainDepths: deps.chainDepthDistribution,
    },
  });
}

Bun.serve({
  port,
  fetch(req) {
    const url = new URL(req.url);
    switch (url.pathname) {
      case "/health":
        return handleHealth();
      case "/stats/ops":
        return handleOps(url);
      case "/stats/summary":
        return handleSummary(url);
      case "/stats/buckets":
        return handleBuckets(url);
      case "/stats/types":
        return handleTypes(url);
      case "/stats/op-types":
        return handleOpTypes(url);
      case "/stats/ingestion":
        return handleIngestion(url);
      case "/stats/db":
        return handleDbStats();
      case "/dfg/txs":
        return handleDfgTxs(url);
      case "/dfg/tx":
        return handleDfgTx(url);
      case "/dfg/signatures":
        return handleDfgSignatures(url);
      case "/dfg/stats":
        return handleDfgStats(url);
      case "/dfg/rollup":
        return handleDfgRollup(url);
      case "/dfg/export":
        return handleDfgExport(url);
      default:
        return jsonResponse(
          {
            error: "not_found",
            routes: [
              "/health",
              "/stats/summary",
              "/stats/ops",
              "/stats/buckets",
              "/stats/types",
              "/stats/op-types",
              "/stats/ingestion",
              "/stats/db",
              "/dfg/txs",
              "/dfg/tx",
              "/dfg/signatures",
              "/dfg/stats",
              "/dfg/rollup",
              "/dfg/export",
            ],
          },
          404,
        );
    }
  },
});

console.log(`fhevm-stats API listening on http://localhost:${port}`);
