import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const DEFAULT_PORT = 4310;

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const defaultChainId = parseNumber(Bun.env.CHAIN_ID);
const port = parseNumber(Bun.env.HTTP_PORT, DEFAULT_PORT) ?? DEFAULT_PORT;
const db = new Database(dbPath, { readonly: true });

type Filters = {
  chainId?: number;
  startBlock?: number;
  endBlock?: number;
  eventName?: string;
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

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === "") return fallback;
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
    params.chainId = filters.chainId;
  }
  if (filters.startBlock !== undefined) {
    clauses.push("block_number >= $startBlock");
    params.startBlock = filters.startBlock;
  }
  if (filters.endBlock !== undefined) {
    clauses.push("block_number <= $endBlock");
    params.endBlock = filters.endBlock;
  }
  if (filters.eventName) {
    clauses.push("event_name = $eventName");
    params.eventName = filters.eventName;
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

function handleOps(url: URL): Response {
  const filters = parseFilters(url);
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

  return jsonResponse({ filters, rows });
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
    .all({ ...params, bucketSize }) as Array<{ bucketStart: number; count: number }>;

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
      default:
        return jsonResponse(
          {
            error: "not_found",
            routes: ["/health", "/stats/summary", "/stats/ops", "/stats/buckets", "/stats/types"],
          },
          404,
        );
    }
  },
});

console.log(`fhevm-stats API listening on http://localhost:${port}`);
