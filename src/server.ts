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
  maxTotalDepth: number;
  chainDepthDistribution: Record<number, number>;
  totalDepthDistribution: Record<number, number>;
  depthMode?: "inter" | "total";
  horizon?: {
    startBlock: number;
    endBlock: number;
    blockCount: number;
  };
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

  const checkpointRow = db
    .prepare(
      `SELECT last_block AS lastBlock,
              updated_at AS updatedAt
       FROM checkpoints
       WHERE chain_id = $chainId`,
    )
    .get({ $chainId: chainId }) as { lastBlock: number; updatedAt: string } | undefined;

  const maxBlockRow = db
    .prepare("SELECT MAX(block_number) AS maxBlock FROM fhe_events WHERE chain_id = $chainId")
    .get({ $chainId: chainId }) as { maxBlock: number | null } | undefined;

  const maxBlock = checkpointRow?.lastBlock ?? maxBlockRow?.maxBlock ?? null;

  const lastEventAtRow =
    maxBlock === null
      ? undefined
      : (db
          .prepare(
            `SELECT created_at AS lastEventAt
             FROM fhe_events
             WHERE chain_id = $chainId
               AND block_number = $maxBlock
             ORDER BY log_index DESC
             LIMIT 1`,
          )
          .get({ $chainId: chainId, $maxBlock: maxBlock }) as { lastEventAt: string | null } | undefined);

  const eventCountRow = hasTable("op_counts")
    ? (db
        .prepare("SELECT COALESCE(SUM(count), 0) AS eventCount FROM op_counts WHERE chain_id = $chainId")
        .get({ $chainId: chainId }) as { eventCount: number } | undefined)
    : (db
        .prepare("SELECT COUNT(*) AS eventCount FROM fhe_events WHERE chain_id = $chainId")
        .get({ $chainId: chainId }) as { eventCount: number } | undefined);

  return jsonResponse({
    chainId,
    events: {
      maxBlock,
      lastEventAt: lastEventAtRow?.lastEventAt ?? null,
      count: eventCountRow?.eventCount ?? 0,
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
  const canUseCounts =
    chainId !== undefined &&
    filters.startBlock === undefined &&
    filters.endBlock === undefined &&
    hasTable("op_counts");
  if (canUseCounts) {
    const params: Record<string, string | number> = { $chainId: chainId };
    const extra = filters.eventName ? " AND event_name = $eventName" : "";
    if (filters.eventName) params.$eventName = filters.eventName;
    const rows = db
      .prepare(
        `SELECT event_name AS eventName, count AS count
         FROM op_counts
         WHERE chain_id = $chainId${extra}
         ORDER BY count DESC`,
      )
      .all(params) as Array<{ eventName: string; count: number }>;

    return jsonResponse({ filters, source: "op_counts", rows });
  }
  const canUseRollup =
    chainId !== undefined &&
    filters.startBlock === undefined &&
    filters.endBlock === undefined &&
    hasTable("op_buckets") &&
    hasTable("rollup_checkpoints");
  if (canUseRollup) {
    const maxBlockRow = db
      .prepare("SELECT MAX(block_number) AS maxBlock FROM fhe_events WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { maxBlock: number | null } | undefined;
    const checkpointRow = db
      .prepare("SELECT last_block AS lastBlock FROM rollup_checkpoints WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { lastBlock: number | null } | undefined;

    const maxBlock = maxBlockRow?.maxBlock ?? null;
    const lastBlock = checkpointRow?.lastBlock ?? null;

    // Only use rollups if they are up-to-date. Partial rollups are misleading.
    const rollupComplete = maxBlock !== null && lastBlock !== null && lastBlock >= maxBlock;
    if (!rollupComplete) {
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
  const chainId = filters.chainId;
  if (
    chainId !== undefined &&
    filters.startBlock === undefined &&
    filters.endBlock === undefined &&
    filters.eventName === undefined &&
    hasTable("op_counts") &&
    hasTable("checkpoints")
  ) {
    const countRow = db
      .prepare("SELECT COALESCE(SUM(count), 0) AS count FROM op_counts WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { count: number };
    const minRow = db
      .prepare("SELECT MIN(block_number) AS minBlock FROM fhe_events WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { minBlock: number | null };
    const maxRow = db
      .prepare("SELECT last_block AS maxBlock FROM checkpoints WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { maxBlock: number | null } | undefined;

    return jsonResponse({
      filters,
      summary: {
        count: countRow.count ?? 0,
        minBlock: minRow.minBlock ?? null,
        maxBlock: maxRow?.maxBlock ?? null,
      },
    });
  }

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
  const totalDepthDistribution = (record.totalDepthDistribution as Record<number, number>) ?? {};
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
      maxTotalDepth: numRecord.maxTotalDepth ?? 0,
      chainDepthDistribution,
      totalDepthDistribution,
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
      maxTotalDepth: numRecord.maxTotalDepth ?? 0,
      chainDepthDistribution,
      totalDepthDistribution,
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
  const caller = url.searchParams.get("caller") ?? undefined;
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));
  const hasTxCallers = hasTable("tx_callers");

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
  if (startBlock !== undefined) {
    clauses.push("block_number >= $startBlock");
    params.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    clauses.push("block_number <= $endBlock");
    params.$endBlock = endBlock;
  }
  if (caller) {
    clauses.push(
      hasTxCallers
        ? `EXISTS (
            SELECT 1
            FROM tx_callers c
            WHERE c.chain_id = dfg_txs.chain_id
              AND c.tx_hash = dfg_txs.tx_hash
              AND c.caller = $callerLower
            LIMIT 1
          )`
        : `EXISTS (
            SELECT 1
            FROM fhe_events e
            WHERE e.chain_id = dfg_txs.chain_id
              AND e.tx_hash = dfg_txs.tx_hash
              AND lower(json_extract(e.args_json, '$.caller')) = $callerLower
            LIMIT 1
          )`,
    );
    params.$callerLower = caller.toLowerCase();
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
    filters: { chainId, minNodes, signatureHash, caller, startBlock, endBlock },
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

  const lookbackBlocks = parseNumber(url.searchParams.get("lookbackBlocks"));

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

  // Compute cut edges if lookbackBlocks is specified
  const cutEdges: Array<{
    handle: string;
    producerTxHash: string;
    producerBlock: number;
    windowStart: number;
  }> = [];

  if (lookbackBlocks !== undefined && hasTable("dfg_handle_producers")) {
    const windowStart = txRow.blockNumber - lookbackBlocks + 1;
    const externalInputs = inputs.filter((input) => input.kind === "external");

    for (const input of externalInputs) {
      const producerRow = db
        .prepare(
          `SELECT tx_hash AS producerTxHash, block_number AS producerBlock
           FROM dfg_handle_producers
           WHERE chain_id = $chainId AND handle = $handle`,
        )
        .get({ $chainId: chainId, $handle: input.handle }) as
        | { producerTxHash: string; producerBlock: number }
        | undefined;

      if (producerRow && producerRow.producerBlock < windowStart) {
        cutEdges.push({
          handle: input.handle,
          producerTxHash: producerRow.producerTxHash,
          producerBlock: producerRow.producerBlock,
          windowStart,
        });
      }
    }
  }

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
    ...(lookbackBlocks !== undefined ? { cutEdges, lookbackBlocks } : {}),
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
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));
  const caller = url.searchParams.get("caller") ?? undefined;
  const hasTxCallers = hasTable("tx_callers");

  // Build WHERE clauses for optional block range filtering
  const whereClauses = [
    "chain_id = $chainId",
    "signature_hash IS NOT NULL",
    "node_count >= $minNodes",
    "edge_count >= $minEdges",
  ];
  const params: Record<string, string | number> = {
    $chainId: chainId,
    $limit: limit,
    $offset: offset,
    $minNodes: minNodes,
    $minEdges: minEdges,
  };

  if (startBlock !== undefined) {
    whereClauses.push("block_number >= $startBlock");
    params.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    whereClauses.push("block_number <= $endBlock");
    params.$endBlock = endBlock;
  }
  if (caller) {
    whereClauses.push(
      hasTxCallers
        ? `EXISTS (
            SELECT 1
            FROM tx_callers c
            WHERE c.chain_id = dfg_txs.chain_id
              AND c.tx_hash = dfg_txs.tx_hash
              AND c.caller = $callerLower
            LIMIT 1
          )`
        : `EXISTS (
            SELECT 1
            FROM fhe_events e
            WHERE e.chain_id = dfg_txs.chain_id
              AND e.tx_hash = dfg_txs.tx_hash
              AND lower(json_extract(e.args_json, '$.caller')) = $callerLower
            LIMIT 1
          )`,
    );
    params.$callerLower = caller.toLowerCase();
  }

  const whereClause = whereClauses.join(" AND ");

  const rows = db
    .prepare(
      `SELECT signature_hash AS signatureHash,
              COUNT(*) AS txCount,
              AVG(node_count) AS avgNodes,
              AVG(edge_count) AS avgEdges
       FROM dfg_txs
       WHERE ${whereClause}
       GROUP BY signature_hash
       ORDER BY txCount DESC
       LIMIT $limit OFFSET $offset`,
    )
    .all(params) as Array<{
    signatureHash: string;
    txCount: number;
    avgNodes: number;
    avgEdges: number;
  }>;

  // For total count, we don't need limit/offset
  const countParams = { ...params };
  delete countParams.$limit;
  delete countParams.$offset;

  const totalRow = db
    .prepare(
      `SELECT COUNT(DISTINCT signature_hash) AS count
       FROM dfg_txs
       WHERE ${whereClause}`,
    )
    .get(countParams) as { count: number };

  const txTotalRow = db
    .prepare(
      `SELECT COUNT(*) AS count
       FROM dfg_txs
       WHERE ${whereClause}`,
    )
    .get(countParams) as { count: number };

  return jsonResponse({
    filters: { chainId, minNodes, minEdges, startBlock, endBlock, caller },
    limit,
    offset,
    rows,
    total: totalRow.count,
    txTotal: txTotalRow.count,
  });
}

function handleDfgStats(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const includeDeps = url.searchParams.get("includeDeps") === "1";
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));
  const depthModeParam = url.searchParams.get("depthMode");
  const depthMode: "inter" | "total" = depthModeParam === "total" ? "total" : "inter";
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  const rollupRow = hasTable("dfg_stats_rollups")
    ? (db
        .prepare(
          `SELECT total,
                  avg_nodes AS avgNodes,
                  avg_edges AS avgEdges,
                  avg_depth AS avgDepth,
                  min_nodes AS minNodes,
                  max_nodes AS maxNodes,
                  min_edges AS minEdges,
                  max_edges AS maxEdges,
                  min_depth AS minDepth,
                  max_depth AS maxDepth,
                  signature_count AS signatureCount,
                  event_tx_count AS eventTxCount
           FROM dfg_stats_rollups
           WHERE chain_id = $chainId`,
        )
        .get({ $chainId: chainId }) as
        | {
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
            signatureCount: number | null;
            eventTxCount: number | null;
          }
        | undefined)
    : undefined;

  const dfgRow = rollupRow
    ? {
        total: rollupRow.total ?? 0,
        avgNodes: rollupRow.avgNodes ?? null,
        avgEdges: rollupRow.avgEdges ?? null,
        avgDepth: rollupRow.avgDepth ?? null,
        minNodes: rollupRow.minNodes ?? null,
        maxNodes: rollupRow.maxNodes ?? null,
        minEdges: rollupRow.minEdges ?? null,
        maxEdges: rollupRow.maxEdges ?? null,
        minDepth: rollupRow.minDepth ?? null,
        maxDepth: rollupRow.maxDepth ?? null,
        signatureCount: rollupRow.signatureCount ?? 0,
      }
    : (db
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
      });

  let totalTxs: number | null = rollupRow?.eventTxCount ?? null;
  if (totalTxs === null && hasTable("tx_counts")) {
    const txCountRow = db
      .prepare("SELECT count FROM tx_counts WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { count: number } | undefined;
    totalTxs = txCountRow?.count ?? null;
  }
  if (totalTxs === null && hasTable("tx_seen")) {
    const txSeenRow = db
      .prepare("SELECT COUNT(*) AS count FROM tx_seen WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { count: number } | undefined;
    totalTxs = txSeenRow?.count ?? null;
  }
  if (totalTxs === null) {
    const txRow = db
      .prepare("SELECT COUNT(DISTINCT tx_hash) AS total FROM fhe_events WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { total: number };
    totalTxs = txRow.total;
  }

  const coverage = totalTxs > 0 ? dfgRow.total / totalTxs : 0;

  let deps: DepStats | null = null;
  const useBlockRange = startBlock !== undefined && endBlock !== undefined;
  const shouldComputeDeps = includeDeps || useBlockRange || Boolean(signatureHash);

  if (shouldComputeDeps) {
    // When block range is provided (and optionally signatureHash), query dfg_tx_deps directly
    if (useBlockRange && hasTable("dfg_tx_deps")) {
      const depClauses = [
        "d.chain_id = $chainId",
        "d.block_number >= $startBlock",
        "d.block_number <= $endBlock",
      ];
      const depParams: Record<string, string | number> = {
        $chainId: chainId,
        $startBlock: startBlock,
        $endBlock: endBlock,
      };
      let depFromClause = "FROM dfg_tx_deps d";

      if (signatureHash) {
        depFromClause =
          "FROM dfg_tx_deps d JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash";
        depClauses.push("t.signature_hash = $signatureHash");
        depParams.$signatureHash = signatureHash;
      }

      const depRow = db
        .prepare(
          `SELECT
           COUNT(*) AS totalTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN d.upstream_txs ELSE 0 END) AS sumUpstreamTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN d.handle_links ELSE 0 END) AS sumUpstreamHandles,
           MAX(d.upstream_txs) AS maxUpstreamTxs,
           MAX(d.handle_links) AS maxUpstreamHandles,
           MAX(d.chain_depth) AS maxChainDepth,
           MAX(d.total_depth) AS maxTotalDepth
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}`,
        )
        .get(depParams) as {
        totalTxs: number;
        dependentTxs: number;
        sumUpstreamTxs: number;
        sumUpstreamHandles: number;
        maxUpstreamTxs: number | null;
        maxUpstreamHandles: number | null;
        maxChainDepth: number | null;
        maxTotalDepth: number | null;
      };

      // Get chain depth distribution for block range
      const chainDistRows = db
        .prepare(
          `SELECT d.chain_depth AS depth, COUNT(*) AS count
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}
         GROUP BY d.chain_depth`,
        )
        .all(depParams) as Array<{
        depth: number;
        count: number;
      }>;

      // Get total depth distribution for block range
      const totalDistRows = db
        .prepare(
          `SELECT d.total_depth AS depth, COUNT(*) AS count
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}
         GROUP BY d.total_depth`,
        )
        .all(depParams) as Array<{
        depth: number;
        count: number;
      }>;

      const chainDepthDistribution: Record<number, number> = {};
      for (const row of chainDistRows) {
        chainDepthDistribution[row.depth] = row.count;
      }

      const totalDepthDistribution: Record<number, number> = {};
      for (const row of totalDistRows) {
        totalDepthDistribution[row.depth] = row.count;
      }

      const totalTxs = depRow?.totalTxs ?? 0;
      const dependentTxs = depRow?.dependentTxs ?? 0;
      const independentTxs = Math.max(totalTxs - dependentTxs, 0);
      const avgUpstreamTxs = dependentTxs > 0 ? (depRow?.sumUpstreamTxs ?? 0) / dependentTxs : 0;
      const avgUpstreamHandles =
        dependentTxs > 0 ? (depRow?.sumUpstreamHandles ?? 0) / dependentTxs : 0;
      const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 0;

      deps = {
        totalTxs,
        dependentTxs,
        independentTxs,
        avgUpstreamTxs,
        avgUpstreamHandles,
        maxUpstreamTxs: depRow?.maxUpstreamTxs ?? 0,
        maxUpstreamHandles: depRow?.maxUpstreamHandles ?? 0,
        parallelismRatio,
        maxChainDepth: depRow?.maxChainDepth ?? 0,
        maxTotalDepth: depRow?.maxTotalDepth ?? 0,
        chainDepthDistribution,
        totalDepthDistribution,
        depthMode,
        horizon: {
          startBlock,
          endBlock,
          blockCount: endBlock - startBlock + 1,
        },
      };
    } else if (!signatureHash && hasTable("dfg_dep_rollups")) {
      // Use rollup data when no block range specified
      const depsRow = db
        .prepare(
          `SELECT stats_json AS statsJson
         FROM dfg_dep_rollups
         WHERE chain_id = $chainId`,
        )
        .get({ $chainId: chainId }) as { statsJson: string } | undefined;
      deps = depsRow ? normalizeDepStats(parseJson(depsRow.statsJson)) : null;
      if (deps) {
        deps.depthMode = depthMode;
      }
    }

    if (!deps && includeDeps && hasTable("dfg_tx_deps")) {
      const depClauses = ["d.chain_id = $chainId"];
      const depParams: Record<string, string | number> = { $chainId: chainId };
      let depFromClause = "FROM dfg_tx_deps d";

      if (signatureHash) {
        depFromClause =
          "FROM dfg_tx_deps d JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash";
        depClauses.push("t.signature_hash = $signatureHash");
        depParams.$signatureHash = signatureHash;
      }

      const depRow = db
        .prepare(
          `SELECT
           COUNT(*) AS totalTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN d.upstream_txs ELSE 0 END) AS sumUpstreamTxs,
           SUM(CASE WHEN d.upstream_txs > 0 THEN d.handle_links ELSE 0 END) AS sumUpstreamHandles,
           MAX(d.upstream_txs) AS maxUpstreamTxs,
           MAX(d.handle_links) AS maxUpstreamHandles,
           MAX(d.chain_depth) AS maxChainDepth,
           MAX(d.total_depth) AS maxTotalDepth
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}`,
        )
        .get(depParams) as {
        totalTxs: number;
        dependentTxs: number;
        sumUpstreamTxs: number;
        sumUpstreamHandles: number;
        maxUpstreamTxs: number | null;
        maxUpstreamHandles: number | null;
        maxChainDepth: number | null;
        maxTotalDepth: number | null;
      };

      // Get distributions
      const chainDistRows = db
        .prepare(
          `SELECT d.chain_depth AS depth, COUNT(*) AS count
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}
         GROUP BY d.chain_depth`,
        )
        .all(depParams) as Array<{ depth: number; count: number }>;

      const totalDistRows = db
        .prepare(
          `SELECT d.total_depth AS depth, COUNT(*) AS count
         ${depFromClause}
         WHERE ${depClauses.join(" AND ")}
         GROUP BY d.total_depth`,
        )
        .all(depParams) as Array<{ depth: number; count: number }>;

      const chainDepthDistribution: Record<number, number> = {};
      for (const row of chainDistRows) {
        chainDepthDistribution[row.depth] = row.count;
      }

      const totalDepthDistribution: Record<number, number> = {};
      for (const row of totalDistRows) {
        totalDepthDistribution[row.depth] = row.count;
      }

      const totalTxs = depRow?.totalTxs ?? 0;
      const dependentTxs = depRow?.dependentTxs ?? 0;
      const independentTxs = Math.max(totalTxs - dependentTxs, 0);
      const avgUpstreamTxs = dependentTxs > 0 ? (depRow?.sumUpstreamTxs ?? 0) / dependentTxs : 0;
      const avgUpstreamHandles =
        dependentTxs > 0 ? (depRow?.sumUpstreamHandles ?? 0) / dependentTxs : 0;
      const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 0;

      deps = {
        totalTxs,
        dependentTxs,
        independentTxs,
        avgUpstreamTxs,
        avgUpstreamHandles,
        maxUpstreamTxs: depRow?.maxUpstreamTxs ?? 0,
        maxUpstreamHandles: depRow?.maxUpstreamHandles ?? 0,
        parallelismRatio,
        maxChainDepth: depRow?.maxChainDepth ?? 0,
        maxTotalDepth: depRow?.maxTotalDepth ?? 0,
        chainDepthDistribution,
        totalDepthDistribution,
        depthMode,
      };
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
          maxTotalDepth: 0, // Unavailable in legacy fallback
          chainDepthDistribution: {}, // Unavailable in legacy fallback
          totalDepthDistribution: {}, // Unavailable in legacy fallback
          depthMode,
        };
      }
    }
  }

  // Get max block with dependency data for horizon filtering
  let maxDepsBlock: number | null = null;
  if (includeDeps && hasTable("dfg_tx_deps")) {
    const maxBlockRow = db
      .prepare("SELECT MAX(block_number) AS maxBlock FROM dfg_tx_deps WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { maxBlock: number | null } | undefined;
    maxDepsBlock = maxBlockRow?.maxBlock ?? null;
  }

  return jsonResponse({
    chainId,
    dfg: dfgRow,
    totalTxs: totalTxs ?? 0,
    coverage,
    deps,
    maxDepsBlock,
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

function handleDfgStatsHorizon(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const horizonSize = parseNumber(url.searchParams.get("horizonSize"), 10) ?? 10;
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_tx_deps")) {
    return jsonResponse({ error: "dfg_tx_deps_missing" }, 404);
  }

  const clauses = ["d.chain_id = $chainId"];
  const params: Record<string, string | number> = {
    $chainId: chainId,
    $horizonSize: horizonSize,
  };

  let fromClause = "FROM dfg_tx_deps d";
  if (signatureHash) {
    fromClause = `FROM dfg_tx_deps d
    JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash`;
    clauses.push("t.signature_hash = $signatureHash");
    params.$signatureHash = signatureHash;
  }

  // Aggregate across all non-overlapping N-block windows
  const row = db
    .prepare(
      `WITH chunks AS (
        SELECT
          (d.block_number / $horizonSize) AS chunk_id,
          MAX(d.chain_depth) AS max_chain_depth,
          MAX(d.total_depth) AS max_total_depth,
          COUNT(*) AS total_txs,
          SUM(CASE WHEN d.upstream_txs = 0 THEN 1 ELSE 0 END) AS independent_txs
        ${fromClause}
        WHERE ${clauses.join(" AND ")}
        GROUP BY chunk_id
      )
      SELECT
        COUNT(*) AS sample_count,
        AVG(max_chain_depth) AS avg_max_chain_depth,
        AVG(max_total_depth) AS avg_max_total_depth,
        MAX(max_chain_depth) AS max_max_chain_depth,
        MAX(max_total_depth) AS max_max_total_depth,
        AVG(CAST(independent_txs AS REAL) / total_txs) AS avg_parallelism,
        MIN(CAST(independent_txs AS REAL) / total_txs) AS min_parallelism
      FROM chunks
      WHERE total_txs > 0`,
    )
    .get(params) as
    | {
        sample_count: number;
        avg_max_chain_depth: number | null;
        avg_max_total_depth: number | null;
        max_max_chain_depth: number | null;
        max_max_total_depth: number | null;
        avg_parallelism: number | null;
        min_parallelism: number | null;
      }
    | undefined;

  return jsonResponse({
    chainId,
    signatureHash,
    horizon: {
      blockSize: horizonSize,
      sampleCount: row?.sample_count ?? 0,
      summary: {
        avgMaxChainDepth: row?.avg_max_chain_depth ?? 0,
        avgMaxTotalDepth: row?.avg_max_total_depth ?? 0,
        maxMaxChainDepth: row?.max_max_chain_depth ?? 0,
        maxMaxTotalDepth: row?.max_max_total_depth ?? 0,
        avgParallelismRatio: row?.avg_parallelism ?? 0,
        minParallelismRatio: row?.min_parallelism ?? 0,
      },
    },
  });
}

function handleDfgPattern(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const signatureHash = url.searchParams.get("signatureHash");
  const exampleLimit = parseNumber(url.searchParams.get("exampleLimit"), 5) ?? 5;

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!signatureHash) {
    return jsonResponse({ error: "signature_hash_required" }, 400);
  }
  if (!hasTable("dfg_tx_deps") || !hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  // Get stats for this specific pattern
  const statsRow = db
    .prepare(
      `SELECT
        COUNT(*) AS txCount,
        SUM(CASE WHEN d.upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
        AVG(d.chain_depth) AS avgChainDepth,
        AVG(d.total_depth) AS avgTotalDepth,
        MAX(d.chain_depth) AS maxChainDepth,
        MAX(d.total_depth) AS maxTotalDepth,
        AVG(d.upstream_txs) AS avgUpstreamTxs,
        MAX(d.upstream_txs) AS maxUpstreamTxs
      FROM dfg_tx_deps d
      JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash
      WHERE d.chain_id = $chainId AND t.signature_hash = $signatureHash`,
    )
    .get({ $chainId: chainId, $signatureHash: signatureHash }) as
    | {
        txCount: number;
        dependentTxs: number;
        avgChainDepth: number | null;
        avgTotalDepth: number | null;
        maxChainDepth: number | null;
        maxTotalDepth: number | null;
        avgUpstreamTxs: number | null;
        maxUpstreamTxs: number | null;
      }
    | undefined;

  if (!statsRow || statsRow.txCount === 0) {
    return jsonResponse({ error: "pattern_not_found" }, 404);
  }

  // Get example transactions (prioritize higher depth ones)
  const exampleRows = db
    .prepare(
      `SELECT d.tx_hash AS txHash, d.block_number AS blockNumber, d.chain_depth AS chainDepth, d.total_depth AS totalDepth
       FROM dfg_tx_deps d
       JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash
       WHERE d.chain_id = $chainId AND t.signature_hash = $signatureHash
       ORDER BY d.chain_depth DESC, d.block_number DESC
       LIMIT $limit`,
    )
    .all({ $chainId: chainId, $signatureHash: signatureHash, $limit: exampleLimit }) as Array<{
    txHash: string;
    blockNumber: number;
    chainDepth: number;
    totalDepth: number;
  }>;

  const txCount = statsRow.txCount ?? 0;
  const dependentTxs = statsRow.dependentTxs ?? 0;
  const parallelismRatio = txCount > 0 ? (txCount - dependentTxs) / txCount : 0;

  return jsonResponse({
    signatureHash,
    stats: {
      txCount,
      dependentTxs,
      parallelismRatio,
      avgChainDepth: statsRow.avgChainDepth ?? 0,
      avgTotalDepth: statsRow.avgTotalDepth ?? 0,
      maxChainDepth: statsRow.maxChainDepth ?? 0,
      maxTotalDepth: statsRow.maxTotalDepth ?? 0,
      avgUpstreamTxs: statsRow.avgUpstreamTxs ?? 0,
      maxUpstreamTxs: statsRow.maxUpstreamTxs ?? 0,
    },
    exampleTxs: exampleRows,
  });
}

function handleDfgStatsWindow(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const lookbackBlocks = parseNumber(url.searchParams.get("lookbackBlocks"), 50) ?? 50;
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;
  const topLimit = parseNumber(url.searchParams.get("topLimit"), 10) ?? 10;
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_inputs") || !hasTable("dfg_handle_producers") || !hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  // Get transactions with their blocks and intra-tx depth (with optional block range filtering)
  const txClauses = ["t.chain_id = $chainId"];
  const txParams: Record<string, string | number> = { $chainId: chainId };

  if (startBlock !== undefined) {
    txClauses.push("t.block_number >= $startBlock");
    txParams.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    txClauses.push("t.block_number <= $endBlock");
    txParams.$endBlock = endBlock;
  }
  if (signatureHash) {
    txClauses.push("t.signature_hash = $signatureHash");
    txParams.$signatureHash = signatureHash;
  }

  const txQuery = `
    SELECT t.tx_hash AS txHash, t.block_number AS blockNumber,
           t.depth AS intraTxDepth, d.chain_depth AS fullChainDepth
    FROM dfg_txs t
    LEFT JOIN dfg_tx_deps d ON d.chain_id = t.chain_id AND d.tx_hash = t.tx_hash
    WHERE ${txClauses.join(" AND ")}
  `;

  const txRows = db.prepare(txQuery).all(txParams) as Array<{
    txHash: string;
    blockNumber: number;
    intraTxDepth: number | null;
    fullChainDepth: number | null;
  }>;

  if (txRows.length === 0) {
    return jsonResponse({
      chainId,
      lookbackBlocks,
      signatureHash,
      startBlock,
      endBlock,
      stats: {
        totalTxs: 0,
        dependentTxs: 0,
        independentTxs: 0,
        parallelismRatio: 1,
        maxTruncatedDepth: 0,
        avgTruncatedDepth: 0,
        truncatedDepthDistribution: {},
      },
      topDepthTxs: [],
    });
  }

  // Build tx block lookup and compute block range
  const txBlockMap = new Map<string, number>();
  let minBlock = Infinity;
  let maxBlock = -Infinity;
  for (const row of txRows) {
    txBlockMap.set(row.txHash, row.blockNumber);
    minBlock = Math.min(minBlock, row.blockNumber);
    maxBlock = Math.max(maxBlock, row.blockNumber);
  }

  // Compute the earliest block we need for dependency lookback
  const earliestNeededBlock = minBlock - lookbackBlocks;

  // Get external inputs for txs in range (join with txs to filter)
  const inputRows = db
    .prepare(
      `SELECT i.tx_hash AS consumerTxHash, i.handle
       FROM dfg_inputs i
       INNER JOIN dfg_txs t ON t.chain_id = i.chain_id AND t.tx_hash = i.tx_hash
       WHERE i.chain_id = $chainId AND i.kind = 'external'
         AND t.block_number >= $minBlock AND t.block_number <= $maxBlock`,
    )
    .all({ $chainId: chainId, $minBlock: minBlock, $maxBlock: maxBlock }) as Array<{
    consumerTxHash: string;
    handle: string;
  }>;

  // Get handle producers within relevant block range (non-trivial only)
  const producerRows = db
    .prepare(
      `SELECT handle, tx_hash AS producerTxHash, block_number AS producerBlock
       FROM dfg_handle_producers
       WHERE chain_id = $chainId AND is_trivial = 0
         AND block_number >= $earliestNeededBlock AND block_number <= $maxBlock`,
    )
    .all({
      $chainId: chainId,
      $earliestNeededBlock: earliestNeededBlock,
      $maxBlock: maxBlock,
    }) as Array<{
    handle: string;
    producerTxHash: string;
    producerBlock: number;
  }>;

  // Build handle -> producer lookup
  const handleProducers = new Map<string, { txHash: string; block: number }>();
  for (const row of producerRows) {
    handleProducers.set(row.handle, { txHash: row.producerTxHash, block: row.producerBlock });
  }

  // Build consumer tx -> list of producer txs (with their blocks) within window
  const txUpstreams = new Map<string, Array<{ txHash: string; block: number }>>();
  for (const row of inputRows) {
    const consumerBlock = txBlockMap.get(row.consumerTxHash);
    if (consumerBlock === undefined) continue;

    const producer = handleProducers.get(row.handle);
    if (!producer) continue;
    if (producer.txHash === row.consumerTxHash) continue; // self-reference

    // Check window: producer must be within lookback window and not from future
    const windowStart = consumerBlock - lookbackBlocks + 1;
    if (producer.block < windowStart || producer.block > consumerBlock) continue; // truncated!

    let upstreams = txUpstreams.get(row.consumerTxHash);
    if (!upstreams) {
      upstreams = [];
      txUpstreams.set(row.consumerTxHash, upstreams);
    }
    // Add if not already present
    if (!upstreams.some((u) => u.txHash === producer.txHash)) {
      upstreams.push({ txHash: producer.txHash, block: producer.block });
    }
  }

  // Build lookups for depth computation
  const intraTxDepths = new Map<string, number>();
  const txBlockLookup = new Map<string, number>();
  for (const tx of txRows) {
    intraTxDepths.set(tx.txHash, tx.intraTxDepth ?? 0);
    txBlockLookup.set(tx.txHash, tx.blockNumber);
  }

  // Build full dependency graph (without window filtering) for chain traversal
  const fullUpstreams = new Map<string, string[]>();
  for (const row of inputRows) {
    const producer = handleProducers.get(row.handle);
    if (!producer) continue;
    if (producer.txHash === row.consumerTxHash) continue;

    let upstreams = fullUpstreams.get(row.consumerTxHash);
    if (!upstreams) {
      upstreams = [];
      fullUpstreams.set(row.consumerTxHash, upstreams);
    }
    if (!upstreams.includes(producer.txHash)) {
      upstreams.push(producer.txHash);
    }
  }

  // Compute depths PER TX with proper window truncation
  // For each tx, traverse its dependency chain and stop when hitting txs outside its window
  const interTxDepths = new Map<string, number>();
  const combinedDepths = new Map<string, number>();

  for (const tx of txRows) {
    const windowStart = tx.blockNumber - lookbackBlocks + 1;
    const windowEnd = tx.blockNumber; // dependencies can only be from past/same block
    const visited = new Set<string>();

    // Recursive function to compute combined depth within this tx's window
    const computeDepth = (txHash: string): { inter: number; combined: number } => {
      if (visited.has(txHash)) {
        return { inter: 0, combined: 0 }; // cycle protection
      }
      visited.add(txHash);

      const block = txBlockLookup.get(txHash);
      if (block === undefined || block < windowStart || block > windowEnd) {
        // Outside window - truncate here (contributes 0)
        return { inter: 0, combined: 0 };
      }

      const myIntra = intraTxDepths.get(txHash) ?? 0;
      const upstreams = fullUpstreams.get(txHash) ?? [];

      if (upstreams.length === 0) {
        return { inter: 0, combined: myIntra };
      }

      let maxUpstreamInter = 0;
      let maxUpstreamCombined = 0;
      for (const upstream of upstreams) {
        const upstreamBlock = txBlockLookup.get(upstream);
        if (
          upstreamBlock === undefined ||
          upstreamBlock < windowStart ||
          upstreamBlock > windowEnd
        ) {
          continue; // truncated (outside window or future block)
        }
        const { inter, combined } = computeDepth(upstream);
        maxUpstreamInter = Math.max(maxUpstreamInter, inter);
        maxUpstreamCombined = Math.max(maxUpstreamCombined, combined);
      }

      const hasInWindowUpstreams = upstreams.some((u) => {
        const b = txBlockLookup.get(u);
        return b !== undefined && b >= windowStart && b <= windowEnd;
      });

      return {
        inter: hasInWindowUpstreams ? maxUpstreamInter + 1 : 0,
        combined: maxUpstreamCombined + myIntra,
      };
    };

    const { inter, combined } = computeDepth(tx.txHash);
    interTxDepths.set(tx.txHash, inter);
    combinedDepths.set(tx.txHash, combined);
  }

  // Aggregate stats
  let totalTxs = 0;
  let dependentTxs = 0;
  let sumInterDepth = 0;
  let sumIntraDepth = 0;
  let maxInterDepth = 0;
  let maxCombinedDepth = 0;
  const interDepthDistribution: Record<number, number> = {};
  const combinedDepthDistribution: Record<number, { count: number; sumIntra: number }> = {};
  const depthResults: Array<{
    txHash: string;
    blockNumber: number;
    truncatedDepth: number;
    intraTxDepth: number;
    combinedDepth: number;
    fullChainDepth: number;
  }> = [];

  for (const tx of txRows) {
    const interDepth = interTxDepths.get(tx.txHash) ?? 0;
    const intraDepth = tx.intraTxDepth ?? 0;
    const combinedDepth = combinedDepths.get(tx.txHash) ?? intraDepth;

    totalTxs++;
    sumInterDepth += interDepth;
    sumIntraDepth += intraDepth;
    maxInterDepth = Math.max(maxInterDepth, interDepth);
    maxCombinedDepth = Math.max(maxCombinedDepth, combinedDepth);

    // Inter-tx only distribution (for backwards compatibility)
    interDepthDistribution[interDepth] = (interDepthDistribution[interDepth] ?? 0) + 1;

    // Combined distribution with intra breakdown
    if (!combinedDepthDistribution[combinedDepth]) {
      combinedDepthDistribution[combinedDepth] = { count: 0, sumIntra: 0 };
    }
    combinedDepthDistribution[combinedDepth].count++;
    combinedDepthDistribution[combinedDepth].sumIntra += intraDepth;

    if (interDepth > 0) {
      dependentTxs++;
    }

    depthResults.push({
      txHash: tx.txHash,
      blockNumber: tx.blockNumber,
      truncatedDepth: interDepth,
      intraTxDepth: intraDepth,
      combinedDepth,
      fullChainDepth: tx.fullChainDepth ?? 0,
    });
  }

  const independentTxs = totalTxs - dependentTxs;
  const parallelismRatio = totalTxs > 0 ? independentTxs / totalTxs : 1;
  const avgInterDepth = totalTxs > 0 ? sumInterDepth / totalTxs : 0;
  const avgIntraDepth = totalTxs > 0 ? sumIntraDepth / totalTxs : 0;

  // Convert combined distribution to simpler format with avg intra per bucket
  const combinedDistSimple: Record<number, { count: number; avgIntra: number }> = {};
  for (const [depth, data] of Object.entries(combinedDepthDistribution)) {
    combinedDistSimple[Number(depth)] = {
      count: data.count,
      avgIntra: data.count > 0 ? data.sumIntra / data.count : 0,
    };
  }

  // Get top depth txs (sorted by combined depth)
  const topDepthTxs = depthResults
    .sort((a, b) => b.combinedDepth - a.combinedDepth || b.blockNumber - a.blockNumber)
    .slice(0, topLimit);

  return jsonResponse({
    chainId,
    lookbackBlocks,
    signatureHash,
    startBlock,
    endBlock,
    blockRange: { min: minBlock, max: maxBlock },
    stats: {
      totalTxs,
      dependentTxs,
      independentTxs,
      parallelismRatio,
      maxTruncatedDepth: maxInterDepth,
      avgTruncatedDepth: avgInterDepth,
      maxCombinedDepth,
      avgCombinedDepth: avgInterDepth + avgIntraDepth,
      avgIntraDepth,
      truncatedDepthDistribution: interDepthDistribution,
      combinedDepthDistribution: combinedDistSimple,
    },
    topDepthTxs,
  });
}

function handleDfgStatsBySignature(url: URL): Response {
  const chainId = parseNumber(url.searchParams.get("chainId")) ?? defaultChainId;
  const limit = parseNumber(url.searchParams.get("limit"), 20) ?? 20;
  const startBlock = parseNumber(url.searchParams.get("startBlock"));
  const endBlock = parseNumber(url.searchParams.get("endBlock"));
  const orderBy = url.searchParams.get("orderBy") ?? "frequency";

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_tx_deps") || !hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  const clauses = ["d.chain_id = $chainId", "t.signature_hash IS NOT NULL"];
  const params: Record<string, string | number> = { $chainId: chainId, $limit: limit };

  if (startBlock !== undefined) {
    clauses.push("d.block_number >= $startBlock");
    params.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    clauses.push("d.block_number <= $endBlock");
    params.$endBlock = endBlock;
  }

  // Determine ORDER BY clause based on orderBy param
  const orderClause = orderBy === "maxDepth" ? "maxChainDepth DESC, txCount DESC" : "txCount DESC";

  const rows = db
    .prepare(
      `SELECT
        t.signature_hash AS signatureHash,
        COUNT(*) AS txCount,
        SUM(CASE WHEN d.upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
        AVG(d.chain_depth) AS avgChainDepth,
        AVG(d.total_depth) AS avgTotalDepth,
        MAX(d.chain_depth) AS maxChainDepth,
        MAX(d.total_depth) AS maxTotalDepth
      FROM dfg_tx_deps d
      JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash
      WHERE ${clauses.join(" AND ")}
      GROUP BY t.signature_hash
      ORDER BY ${orderClause}
      LIMIT $limit`,
    )
    .all(params) as Array<{
    signatureHash: string;
    txCount: number;
    dependentTxs: number;
    avgChainDepth: number | null;
    avgTotalDepth: number | null;
    maxChainDepth: number | null;
    maxTotalDepth: number | null;
  }>;

  const signatures = rows.map((row) => {
    const txCount = row.txCount ?? 0;
    const dependentTxs = row.dependentTxs ?? 0;
    const parallelismRatio = txCount > 0 ? (txCount - dependentTxs) / txCount : 0;
    return {
      signatureHash: row.signatureHash,
      txCount,
      dependentTxs,
      parallelismRatio,
      avgChainDepth: row.avgChainDepth ?? 0,
      avgTotalDepth: row.avgTotalDepth ?? 0,
      maxChainDepth: row.maxChainDepth ?? 0,
      maxTotalDepth: row.maxTotalDepth ?? 0,
    };
  });

  return jsonResponse({
    chainId,
    orderBy,
    signatures,
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
      case "/dfg/stats/horizon":
        return handleDfgStatsHorizon(url);
      case "/dfg/stats/by-signature":
        return handleDfgStatsBySignature(url);
      case "/dfg/stats/window":
        return handleDfgStatsWindow(url);
      case "/dfg/pattern":
        return handleDfgPattern(url);
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
              "/dfg/stats/horizon",
              "/dfg/stats/by-signature",
              "/dfg/stats/window",
              "/dfg/pattern",
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
