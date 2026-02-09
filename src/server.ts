import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { resolve, sep } from "node:path";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const DEFAULT_PORT = 4310;
const DEFAULT_UI_DIST_DIR = "ui/dist";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_RATE_LIMIT_WINDOW_MS = 60_000;
const DEFAULT_RATE_LIMIT_MAX_REQUESTS = 240;
const DEFAULT_MAX_PAGE_LIMIT = 100;
const DEFAULT_MAX_PAGE_OFFSET = 10_000;
const DEFAULT_MAX_BLOCK_SPAN = 200_000;
const DEFAULT_MAX_WINDOW_LOOKBACK = 2_000;
const DEFAULT_MAX_TOP_LIMIT = 100;
const DEFAULT_MAX_EXAMPLE_LIMIT = 100;
const DEFAULT_MAX_HORIZON_SIZE = 10_000;
const DEFAULT_MAX_BUCKET_SIZE = 100_000;
const DEFAULT_MAX_EVENT_NAME_LENGTH = 128;
const DEFAULT_MAX_MIN_NODES = 50_000;
const DEFAULT_MAX_MIN_EDGES = 50_000;
const DEFAULT_MAX_CHAIN_ID = 2_147_483_647;
const DEFAULT_MAX_BLOCK_NUMBER = 9_999_999_999;

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const defaultChainId = parseNumber(Bun.env.CHAIN_ID);
const isPlatformDeployment =
  (Bun.env.RAILWAY_ENVIRONMENT ?? "").trim().length > 0 || (Bun.env.PORT ?? "").trim().length > 0;
const port = Math.max(
  1,
  Math.trunc(parseNumber(Bun.env.HTTP_PORT ?? Bun.env.PORT, DEFAULT_PORT) ?? DEFAULT_PORT),
);
const host = (Bun.env.HTTP_HOST ?? "").trim() || (isPlatformDeployment ? "0.0.0.0" : DEFAULT_HOST);
const exposeDbStats = Bun.env.EXPOSE_DB_STATS === "1";
const maxPageOffset = Math.max(
  0,
  Math.trunc(
    parseNumber(Bun.env.MAX_PAGE_OFFSET, DEFAULT_MAX_PAGE_OFFSET) ?? DEFAULT_MAX_PAGE_OFFSET,
  ),
);
const maxPageLimit = Math.max(
  1,
  Math.trunc(parseNumber(Bun.env.MAX_PAGE_LIMIT, DEFAULT_MAX_PAGE_LIMIT) ?? DEFAULT_MAX_PAGE_LIMIT),
);
const maxBlockSpan = Math.max(
  1,
  Math.trunc(parseNumber(Bun.env.MAX_BLOCK_SPAN, DEFAULT_MAX_BLOCK_SPAN) ?? DEFAULT_MAX_BLOCK_SPAN),
);
const maxWindowLookback = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_WINDOW_LOOKBACK, DEFAULT_MAX_WINDOW_LOOKBACK) ??
      DEFAULT_MAX_WINDOW_LOOKBACK,
  ),
);
const maxTopLimit = Math.max(
  1,
  Math.trunc(parseNumber(Bun.env.MAX_TOP_LIMIT, DEFAULT_MAX_TOP_LIMIT) ?? DEFAULT_MAX_TOP_LIMIT),
);
const maxExampleLimit = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_EXAMPLE_LIMIT, DEFAULT_MAX_EXAMPLE_LIMIT) ?? DEFAULT_MAX_EXAMPLE_LIMIT,
  ),
);
const maxHorizonSize = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_HORIZON_SIZE, DEFAULT_MAX_HORIZON_SIZE) ?? DEFAULT_MAX_HORIZON_SIZE,
  ),
);
const maxBucketSize = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_BUCKET_SIZE, DEFAULT_MAX_BUCKET_SIZE) ?? DEFAULT_MAX_BUCKET_SIZE,
  ),
);
const maxEventNameLength = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_EVENT_NAME_LENGTH, DEFAULT_MAX_EVENT_NAME_LENGTH) ??
      DEFAULT_MAX_EVENT_NAME_LENGTH,
  ),
);
const maxMinNodes = Math.max(
  0,
  Math.trunc(parseNumber(Bun.env.MAX_MIN_NODES, DEFAULT_MAX_MIN_NODES) ?? DEFAULT_MAX_MIN_NODES),
);
const maxMinEdges = Math.max(
  0,
  Math.trunc(parseNumber(Bun.env.MAX_MIN_EDGES, DEFAULT_MAX_MIN_EDGES) ?? DEFAULT_MAX_MIN_EDGES),
);
const maxChainId = Math.max(
  1,
  Math.trunc(parseNumber(Bun.env.MAX_CHAIN_ID, DEFAULT_MAX_CHAIN_ID) ?? DEFAULT_MAX_CHAIN_ID),
);
const maxBlockNumber = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.MAX_BLOCK_NUMBER, DEFAULT_MAX_BLOCK_NUMBER) ?? DEFAULT_MAX_BLOCK_NUMBER,
  ),
);
const rateLimitWindowMs = Math.max(
  1_000,
  Math.trunc(
    parseNumber(Bun.env.RATE_LIMIT_WINDOW_MS, DEFAULT_RATE_LIMIT_WINDOW_MS) ??
      DEFAULT_RATE_LIMIT_WINDOW_MS,
  ),
);
const rateLimitMaxRequests = Math.max(
  1,
  Math.trunc(
    parseNumber(Bun.env.RATE_LIMIT_MAX_REQUESTS, DEFAULT_RATE_LIMIT_MAX_REQUESTS) ??
      DEFAULT_RATE_LIMIT_MAX_REQUESTS,
  ),
);
const rateLimitDisabled = Bun.env.RATE_LIMIT_DISABLED === "1";
const corsAllowedOrigins = new Set(
  (Bun.env.CORS_ALLOW_ORIGINS ?? "")
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0),
);
const cspHeader =
  Bun.env.CONTENT_SECURITY_POLICY ??
  "default-src 'self'; base-uri 'self'; frame-ancestors 'none'; img-src 'self' data:; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com data:; script-src 'self'; connect-src 'self'";

const CALLER_REGEX = /^0x[a-fA-F0-9]{40}$/;
const TX_HASH_REGEX = /^0x[a-fA-F0-9]{64}$/;
const SIGNATURE_HASH_REGEX = /^[a-fA-F0-9]{64}$/;

type RateLimitBucket = {
  windowStartMs: number;
  count: number;
};

const rateLimitBuckets = new Map<string, RateLimitBucket>();
let lastRateLimitSweepMs = Date.now();

const db = new Database(dbPath, { readonly: true });
db.exec("PRAGMA busy_timeout=5000;");

const uiDistDir = resolve(Bun.env.UI_DIST_DIR ?? DEFAULT_UI_DIST_DIR);
const uiIndexPath = resolve(uiDistDir, "index.html");
const canServeUi = existsSync(uiIndexPath);

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

function hasIndex(name: string): boolean {
  const row = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name = $name")
    .get({ $name: name }) as { name: string } | undefined;
  return Boolean(row);
}

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseInteger(value: string | null | undefined): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) return Number.NaN;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) ? parsed : Number.NaN;
}

function appendVary(existing: string | null, value: string): string {
  const entries = new Set(
    (existing ?? "")
      .split(",")
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0),
  );
  entries.add(value);
  return [...entries].join(", ");
}

function resolveCorsOrigin(req: Request, url: URL): string | null {
  const originHeader = req.headers.get("origin");
  if (!originHeader) return null;
  let origin: string;
  try {
    origin = new URL(originHeader).origin;
  } catch {
    return null;
  }

  if (origin === url.origin) return origin;
  if (corsAllowedOrigins.has(origin)) return origin;
  return null;
}

function applySecurityHeaders(headers: Headers): void {
  headers.set("x-content-type-options", "nosniff");
  headers.set("x-frame-options", "DENY");
  headers.set("referrer-policy", "no-referrer");
  headers.set("cross-origin-opener-policy", "same-origin");
  headers.set("cross-origin-resource-policy", "same-origin");
  headers.set("permissions-policy", "geolocation=(), microphone=(), camera=(), payment=(), usb=()");
  headers.set("content-security-policy", cspHeader);
}

function finalizeResponse(
  req: Request,
  url: URL,
  response: Response,
  isApiPathname: boolean,
): Response {
  const headers = new Headers(response.headers);
  applySecurityHeaders(headers);

  if (isApiPathname) {
    const corsOrigin = resolveCorsOrigin(req, url);
    if (corsOrigin) {
      headers.set("access-control-allow-origin", corsOrigin);
      if (!headers.has("access-control-allow-methods")) {
        headers.set("access-control-allow-methods", "GET, HEAD, OPTIONS");
      }
      if (!headers.has("access-control-allow-headers")) {
        headers.set("access-control-allow-headers", "content-type");
      }
      headers.set("vary", appendVary(headers.get("vary"), "Origin"));
    }
  }

  return new Response(response.body, {
    status: response.status,
    headers,
  });
}

function handlePreflight(req: Request, url: URL): Response {
  const corsOrigin = resolveCorsOrigin(req, url);
  if (!corsOrigin) {
    return jsonResponse({ error: "origin_not_allowed" }, 403);
  }

  const requestedHeaders = req.headers.get("access-control-request-headers");
  const headers = new Headers({
    "access-control-allow-origin": corsOrigin,
    "access-control-allow-methods": "GET, HEAD, OPTIONS",
    "access-control-allow-headers": requestedHeaders ?? "content-type",
    "access-control-max-age": "600",
    vary: "Origin",
  });
  return new Response(null, { status: 204, headers });
}

function resolveClientKey(req: Request): string {
  const xForwardedFor = req.headers.get("x-forwarded-for");
  if (xForwardedFor) {
    const candidate = xForwardedFor.split(",")[0]?.trim();
    if (candidate) return candidate.slice(0, 64);
  }
  const xRealIp = req.headers.get("x-real-ip");
  if (xRealIp) return xRealIp.trim().slice(0, 64);
  return "unknown";
}

function isRateLimited(
  req: Request,
  pathname: string,
): { limited: boolean; retryAfterSeconds: number } {
  if (rateLimitDisabled) return { limited: false, retryAfterSeconds: 0 };

  const now = Date.now();
  if (now - lastRateLimitSweepMs >= rateLimitWindowMs * 2) {
    for (const [key, bucket] of rateLimitBuckets) {
      if (now - bucket.windowStartMs >= rateLimitWindowMs * 2) {
        rateLimitBuckets.delete(key);
      }
    }
    lastRateLimitSweepMs = now;
  }

  const clientKey = resolveClientKey(req);
  const key = `${clientKey}:${pathname}`;
  const bucket = rateLimitBuckets.get(key);
  if (!bucket || now - bucket.windowStartMs >= rateLimitWindowMs) {
    rateLimitBuckets.set(key, { windowStartMs: now, count: 1 });
    return { limited: false, retryAfterSeconds: 0 };
  }

  bucket.count += 1;
  if (bucket.count > rateLimitMaxRequests) {
    const retryAfterSeconds = Math.max(
      1,
      Math.ceil((bucket.windowStartMs + rateLimitWindowMs - now) / 1000),
    );
    return { limited: true, retryAfterSeconds };
  }

  return { limited: false, retryAfterSeconds: 0 };
}

function validateIntParam(
  value: number | undefined,
  name: string,
  min: number,
  max: number,
): Response | null {
  if (value === undefined) return null;
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    return jsonResponse(
      {
        error: "invalid_parameter",
        parameter: name,
        min,
        max,
      },
      400,
    );
  }
  return null;
}

function validateBlockRange(startBlock?: number, endBlock?: number): Response | null {
  const startError = validateIntParam(startBlock, "startBlock", 0, maxBlockNumber);
  if (startError) return startError;
  const endError = validateIntParam(endBlock, "endBlock", 0, maxBlockNumber);
  if (endError) return endError;
  if (startBlock !== undefined && endBlock !== undefined) {
    if (startBlock > endBlock) {
      return jsonResponse({ error: "invalid_block_range" }, 400);
    }
    if (endBlock - startBlock > maxBlockSpan) {
      return jsonResponse(
        {
          error: "block_range_too_wide",
          maxBlockSpan,
        },
        400,
      );
    }
  }
  return null;
}

function validatePagination(limit: number, offset: number): Response | null {
  const limitError = validateIntParam(limit, "limit", 1, maxPageLimit);
  if (limitError) return limitError;
  const offsetError = validateIntParam(offset, "offset", 0, maxPageOffset);
  if (offsetError) return offsetError;
  return null;
}

function validateSignatureHash(signatureHash: string | undefined): Response | null {
  if (!signatureHash) return null;
  if (!SIGNATURE_HASH_REGEX.test(signatureHash)) {
    return jsonResponse({ error: "invalid_signature_hash" }, 400);
  }
  return null;
}

function validateTxHash(txHash: string): Response | null {
  if (!TX_HASH_REGEX.test(txHash)) {
    return jsonResponse({ error: "invalid_tx_hash" }, 400);
  }
  return null;
}

function validateCaller(caller: string | undefined): Response | null {
  if (!caller) return null;
  if (!CALLER_REGEX.test(caller)) {
    return jsonResponse({ error: "invalid_caller" }, 400);
  }
  return null;
}

function validateFilters(filters: Filters): Response | null {
  const chainIdError = validateIntParam(filters.chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const rangeError = validateBlockRange(filters.startBlock, filters.endBlock);
  if (rangeError) return rangeError;
  if (filters.eventName && filters.eventName.length > maxEventNameLength) {
    return jsonResponse({ error: "event_name_too_long" }, 400);
  }
  return null;
}

function isApiPath(pathname: string): boolean {
  if (pathname === "/health") return true;
  if (pathname.startsWith("/stats")) return true;
  if (pathname.startsWith("/dfg")) return true;
  return false;
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
    },
  });
}

function tryServeUi(pathname: string): Response | null {
  if (!canServeUi) return null;
  if (pathname === "/health") return null;
  if (pathname.startsWith("/stats") || pathname.startsWith("/dfg")) return null;

  const raw = pathname === "/" ? "/index.html" : pathname;
  let decoded: string;
  try {
    decoded = decodeURIComponent(raw);
  } catch {
    return null;
  }

  const candidatePath = resolve(uiDistDir, decoded.replace(/^\//, ""));
  const uiDistPrefix = uiDistDir.endsWith(sep) ? uiDistDir : `${uiDistDir}${sep}`;
  if (candidatePath !== uiDistDir && !candidatePath.startsWith(uiDistPrefix)) {
    return null;
  }

  if (existsSync(candidatePath)) {
    return new Response(Bun.file(candidatePath));
  }

  return new Response(Bun.file(uiIndexPath));
}

function parseFilters(url: URL): Filters {
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const startBlock = parseInteger(url.searchParams.get("startBlock"));
  const endBlock = parseInteger(url.searchParams.get("endBlock"));
  const rawEventName = url.searchParams.get("eventName");
  const eventName =
    rawEventName && rawEventName.trim().length > 0 ? rawEventName.trim() : undefined;
  return { chainId, startBlock, endBlock, eventName };
}

function handleHealth(): Response {
  return jsonResponse({ status: "ok" });
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
    events: totalRow.count,
    sizeBytes: sizeRow.sizeBytes,
  });
}

function handleIngestion(url: URL): Response {
  const filters = parseFilters(url);
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;

  const { chainId } = filters;
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
          .get({ $chainId: chainId, $maxBlock: maxBlock }) as
          | { lastEventAt: string | null }
          | undefined);

  const eventCountRow = hasTable("op_counts")
    ? (db
        .prepare(
          "SELECT COALESCE(SUM(count), 0) AS eventCount FROM op_counts WHERE chain_id = $chainId",
        )
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
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;
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
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;
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
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;

  const bucketSize = parseInteger(url.searchParams.get("bucketSize")) ?? 1000;
  const bucketSizeError = validateIntParam(bucketSize, "bucketSize", 1, maxBucketSize);
  if (bucketSizeError) return bucketSizeError;

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
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;

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
  const filtersError = validateFilters(filters);
  if (filtersError) return filtersError;

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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
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

  const limit = parseInteger(url.searchParams.get("limit")) ?? 25;
  const offset = parseInteger(url.searchParams.get("offset")) ?? 0;
  const minNodes = parseInteger(url.searchParams.get("minNodes"));
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;
  const callerRaw = url.searchParams.get("caller");
  const caller =
    callerRaw && callerRaw.trim().length > 0 ? callerRaw.trim().toLowerCase() : undefined;
  const startBlock = parseInteger(url.searchParams.get("startBlock"));
  const endBlock = parseInteger(url.searchParams.get("endBlock"));

  const paginationError = validatePagination(limit, offset);
  if (paginationError) return paginationError;
  const minNodesError = validateIntParam(minNodes, "minNodes", 0, maxMinNodes);
  if (minNodesError) return minNodesError;
  const signatureHashError = validateSignatureHash(signatureHash);
  if (signatureHashError) return signatureHashError;
  const callerError = validateCaller(caller);
  if (callerError) return callerError;
  const rangeError = validateBlockRange(startBlock, endBlock);
  if (rangeError) return rangeError;

  const hasTxCallers = hasTable("tx_callers");

  const clauses = ["t.chain_id = $chainId"];
  const params: Record<string, string | number> = { $chainId: chainId };

  if (minNodes !== undefined) {
    clauses.push("t.node_count >= $minNodes");
    params.$minNodes = minNodes;
  }
  if (signatureHash) {
    clauses.push("t.signature_hash = $signatureHash");
    params.$signatureHash = signatureHash;
  }
  if (startBlock !== undefined) {
    clauses.push("t.block_number >= $startBlock");
    params.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    clauses.push("t.block_number <= $endBlock");
    params.$endBlock = endBlock;
  }
  const useBlockIndex =
    (startBlock !== undefined || endBlock !== undefined) && hasIndex("dfg_txs_block");
  const txsFrom = useBlockIndex ? "dfg_txs t INDEXED BY dfg_txs_block" : "dfg_txs t";
  let fromClause = `FROM ${txsFrom}`;
  if (caller && hasTxCallers) {
    fromClause = `FROM ${txsFrom}
      JOIN tx_callers c
        ON c.chain_id = t.chain_id AND c.tx_hash = t.tx_hash AND c.caller = $callerLower`;
    params.$callerLower = caller;
  } else if (caller) {
    clauses.push(
      `EXISTS (
         SELECT 1
         FROM fhe_events e
         WHERE e.chain_id = t.chain_id
           AND e.tx_hash = t.tx_hash
           AND lower(json_extract(e.args_json, '$.caller')) = $callerLower
         LIMIT 1
       )`,
    );
    params.$callerLower = caller;
  }

  const rows = db
    .prepare(
      `SELECT t.tx_hash AS txHash,
              t.block_number AS blockNumber,
              t.node_count AS nodeCount,
              t.edge_count AS edgeCount,
              t.depth,
              t.signature_hash AS signatureHash,
              t.stats_json AS statsJson
       ${fromClause}
       WHERE ${clauses.join(" AND ")}
       ORDER BY t.block_number DESC, t.tx_hash DESC
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
       ${fromClause}
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
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
  const txHashError = validateTxHash(txHash);
  if (txHashError) return txHashError;

  const lookbackBlocks = parseInteger(url.searchParams.get("lookbackBlocks"));
  const lookbackError = validateIntParam(lookbackBlocks, "lookbackBlocks", 1, maxWindowLookback);
  if (lookbackError) return lookbackError;

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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
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

  const limit = parseInteger(url.searchParams.get("limit")) ?? 10;
  const offset = parseInteger(url.searchParams.get("offset")) ?? 0;
  const minNodes = parseInteger(url.searchParams.get("minNodes")) ?? 1;
  const minEdges = parseInteger(url.searchParams.get("minEdges")) ?? 0;
  const startBlock = parseInteger(url.searchParams.get("startBlock"));
  const endBlock = parseInteger(url.searchParams.get("endBlock"));
  const callerRaw = url.searchParams.get("caller");
  const caller =
    callerRaw && callerRaw.trim().length > 0 ? callerRaw.trim().toLowerCase() : undefined;

  const paginationError = validatePagination(limit, offset);
  if (paginationError) return paginationError;
  const minNodesError = validateIntParam(minNodes, "minNodes", 0, maxMinNodes);
  if (minNodesError) return minNodesError;
  const minEdgesError = validateIntParam(minEdges, "minEdges", 0, maxMinEdges);
  if (minEdgesError) return minEdgesError;
  const rangeError = validateBlockRange(startBlock, endBlock);
  if (rangeError) return rangeError;
  const callerError = validateCaller(caller);
  if (callerError) return callerError;

  const hasTxCallers = hasTable("tx_callers");

  // Build WHERE clauses for optional block range filtering
  const whereClauses = [
    "t.chain_id = $chainId",
    "t.signature_hash IS NOT NULL",
    "t.node_count >= $minNodes",
    "t.edge_count >= $minEdges",
  ];
  const params: Record<string, string | number> = {
    $chainId: chainId,
    $limit: limit,
    $offset: offset,
    $minNodes: minNodes,
    $minEdges: minEdges,
  };

  if (startBlock !== undefined) {
    whereClauses.push("t.block_number >= $startBlock");
    params.$startBlock = startBlock;
  }
  if (endBlock !== undefined) {
    whereClauses.push("t.block_number <= $endBlock");
    params.$endBlock = endBlock;
  }
  const useBlockIndex =
    (startBlock !== undefined || endBlock !== undefined) && hasIndex("dfg_txs_block");
  const txsFrom = useBlockIndex ? "dfg_txs t INDEXED BY dfg_txs_block" : "dfg_txs t";
  let fromClause = `FROM ${txsFrom}`;
  if (caller && hasTxCallers) {
    fromClause = `FROM ${txsFrom}
      JOIN tx_callers c
        ON c.chain_id = t.chain_id AND c.tx_hash = t.tx_hash AND c.caller = $callerLower`;
    params.$callerLower = caller;
  } else if (caller) {
    whereClauses.push(
      `EXISTS (
         SELECT 1
         FROM fhe_events e
         WHERE e.chain_id = t.chain_id
           AND e.tx_hash = t.tx_hash
           AND lower(json_extract(e.args_json, '$.caller')) = $callerLower
         LIMIT 1
       )`,
    );
    params.$callerLower = caller;
  }

  const whereClause = whereClauses.join(" AND ");

  const rows = db
    .prepare(
      `SELECT signature_hash AS signatureHash,
              COUNT(*) AS txCount,
              AVG(node_count) AS avgNodes,
              AVG(edge_count) AS avgEdges
       ${fromClause}
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
       ${fromClause}
       WHERE ${whereClause}`,
    )
    .get(countParams) as { count: number };

  const txTotalRow = db
    .prepare(
      `SELECT COUNT(*) AS count
       ${fromClause}
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const includeDeps = url.searchParams.get("includeDeps") === "1";
  const startBlock = parseInteger(url.searchParams.get("startBlock"));
  const endBlock = parseInteger(url.searchParams.get("endBlock"));
  const depthModeParam = url.searchParams.get("depthMode");
  const depthMode: "inter" | "total" = depthModeParam === "total" ? "total" : "inter";
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;
  const rangeError = validateBlockRange(startBlock, endBlock);
  if (rangeError) return rangeError;
  const signatureHashError = validateSignatureHash(signatureHash);
  if (signatureHashError) return signatureHashError;

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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const horizonSize = parseInteger(url.searchParams.get("horizonSize")) ?? 10;
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;
  const horizonSizeError = validateIntParam(horizonSize, "horizonSize", 1, maxHorizonSize);
  if (horizonSizeError) return horizonSizeError;
  const signatureHashError = validateSignatureHash(signatureHash);
  if (signatureHashError) return signatureHashError;

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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const signatureHash = url.searchParams.get("signatureHash");
  const exampleLimit = parseInteger(url.searchParams.get("exampleLimit")) ?? 5;
  const exampleLimitError = validateIntParam(exampleLimit, "exampleLimit", 1, maxExampleLimit);
  if (exampleLimitError) return exampleLimitError;

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!signatureHash) {
    return jsonResponse({ error: "signature_hash_required" }, 400);
  }
  const signatureHashError = validateSignatureHash(signatureHash);
  if (signatureHashError) return signatureHashError;
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const lookbackBlocks = parseInteger(url.searchParams.get("lookbackBlocks")) ?? 50;
  const signatureHash = url.searchParams.get("signatureHash") ?? undefined;
  const topLimit = parseInteger(url.searchParams.get("topLimit")) ?? 10;
  const startBlockParam = parseInteger(url.searchParams.get("startBlock"));
  const endBlockParam = parseInteger(url.searchParams.get("endBlock"));
  const lookbackError = validateIntParam(lookbackBlocks, "lookbackBlocks", 1, maxWindowLookback);
  if (lookbackError) return lookbackError;
  const topLimitError = validateIntParam(topLimit, "topLimit", 1, maxTopLimit);
  if (topLimitError) return topLimitError;
  const initialRangeError = validateBlockRange(startBlockParam, endBlockParam);
  if (initialRangeError) return initialRangeError;
  const signatureHashError = validateSignatureHash(signatureHash);
  if (signatureHashError) return signatureHashError;

  if (chainId === undefined) {
    return jsonResponse({ error: "chain_id_required" }, 400);
  }
  if (!hasTable("dfg_inputs") || !hasTable("dfg_handle_producers") || !hasTable("dfg_txs")) {
    return jsonResponse({ error: "dfg_tables_missing" }, 404);
  }

  // Default to a bounded range at the tip when no explicit range is provided.
  let startBlock = startBlockParam;
  let endBlock = endBlockParam;
  if (startBlock === undefined || endBlock === undefined) {
    const maxRow = db
      .prepare("SELECT MAX(block_number) AS maxBlock FROM dfg_txs WHERE chain_id = $chainId")
      .get({ $chainId: chainId }) as { maxBlock: number | null } | undefined;
    const maxBlock = maxRow?.maxBlock ?? null;
    if (maxBlock !== null) {
      const resolvedEnd = endBlock ?? maxBlock;
      const resolvedStart = startBlock ?? Math.max(0, resolvedEnd - lookbackBlocks + 1);
      startBlock = resolvedStart;
      endBlock = resolvedEnd;
    }
  }
  const resolvedRangeError = validateBlockRange(startBlock, endBlock);
  if (resolvedRangeError) return resolvedRangeError;

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
      `SELECT p.handle,
              p.tx_hash AS producerTxHash,
              t.block_number AS producerBlock
       FROM dfg_handle_producers p
       INNER JOIN dfg_txs t
         ON t.chain_id = p.chain_id
        AND t.tx_hash = p.tx_hash
       WHERE p.chain_id = $chainId
         AND p.is_trivial = 0
         AND t.block_number >= $earliestNeededBlock
         AND t.block_number <= $maxBlock`,
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
  const chainId = parseInteger(url.searchParams.get("chainId")) ?? defaultChainId;
  const chainIdError = validateIntParam(chainId, "chainId", 0, maxChainId);
  if (chainIdError) return chainIdError;
  const limit = parseInteger(url.searchParams.get("limit")) ?? 20;
  const startBlock = parseInteger(url.searchParams.get("startBlock"));
  const endBlock = parseInteger(url.searchParams.get("endBlock"));
  const orderBy = url.searchParams.get("orderBy") ?? "frequency";
  const limitError = validateIntParam(limit, "limit", 1, maxPageLimit);
  if (limitError) return limitError;
  const rangeError = validateBlockRange(startBlock, endBlock);
  if (rangeError) return rangeError;
  if (orderBy !== "frequency" && orderBy !== "maxDepth") {
    return jsonResponse({ error: "invalid_order_by" }, 400);
  }

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
  hostname: host,
  port,
  fetch(req) {
    const url = new URL(req.url);
    const apiPathname = isApiPath(url.pathname);

    if (apiPathname && req.method === "OPTIONS") {
      return finalizeResponse(req, url, handlePreflight(req, url), true);
    }

    if (apiPathname && req.method !== "GET" && req.method !== "HEAD") {
      const methodNotAllowed = jsonResponse({ error: "method_not_allowed" }, 405);
      methodNotAllowed.headers.set("allow", "GET, HEAD, OPTIONS");
      return finalizeResponse(req, url, methodNotAllowed, true);
    }

    if (apiPathname) {
      const rateLimitResult = isRateLimited(req, url.pathname);
      if (rateLimitResult.limited) {
        const limited = jsonResponse(
          {
            error: "rate_limited",
            retryAfterSeconds: rateLimitResult.retryAfterSeconds,
          },
          429,
        );
        limited.headers.set("retry-after", String(rateLimitResult.retryAfterSeconds));
        return finalizeResponse(req, url, limited, true);
      }
    }

    let response: Response;
    switch (url.pathname) {
      case "/health":
        response = handleHealth();
        break;
      case "/stats/ops":
        response = handleOps(url);
        break;
      case "/stats/summary":
        response = handleSummary(url);
        break;
      case "/stats/buckets":
        response = handleBuckets(url);
        break;
      case "/stats/types":
        response = handleTypes(url);
        break;
      case "/stats/op-types":
        response = handleOpTypes(url);
        break;
      case "/stats/ingestion":
        response = handleIngestion(url);
        break;
      case "/stats/db":
        response = exposeDbStats ? handleDbStats() : jsonResponse({ error: "not_found" }, 404);
        break;
      case "/dfg/txs":
        response = handleDfgTxs(url);
        break;
      case "/dfg/tx":
        response = handleDfgTx(url);
        break;
      case "/dfg/signatures":
        response = handleDfgSignatures(url);
        break;
      case "/dfg/stats":
        response = handleDfgStats(url);
        break;
      case "/dfg/rollup":
        response = handleDfgRollup(url);
        break;
      case "/dfg/export":
        response = handleDfgExport(url);
        break;
      case "/dfg/stats/horizon":
        response = handleDfgStatsHorizon(url);
        break;
      case "/dfg/stats/by-signature":
        response = handleDfgStatsBySignature(url);
        break;
      case "/dfg/stats/window":
        response = handleDfgStatsWindow(url);
        break;
      case "/dfg/pattern":
        response = handleDfgPattern(url);
        break;
      default: {
        if (req.method === "GET" || req.method === "HEAD") {
          const uiResponse = tryServeUi(url.pathname);
          if (uiResponse) {
            response = uiResponse;
            break;
          }
        }
        response = jsonResponse({ error: "not_found" }, 404);
      }
    }

    return finalizeResponse(req, url, response, apiPathname);
  },
});

console.log(`fhevm-stats API listening on http://${host}:${port}`);
