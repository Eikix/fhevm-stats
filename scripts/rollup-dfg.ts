import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

type Rollup = {
  opCounts: Record<string, number>;
  inputKinds: Record<string, Record<string, number>>;
  operandPairs: Record<string, Record<string, number>>;
  typeCounts: Record<string, Record<string, Record<string, number>>>;
};

type DepStats = {
  totalTxs: number;
  dependentTxs: number;
  independentTxs: number;
  avgUpstreamTxs: number;
  avgUpstreamHandles: number;
  maxUpstreamTxs: number;
  maxUpstreamHandles: number;
};

type DepRollupState = {
  totalTxs: number;
  dependentTxs: number;
  sumUpstreamTxs: number;
  sumUpstreamHandles: number;
  maxUpstreamTxs: number;
  maxUpstreamHandles: number;
};

function parseNumber(value: string | null | undefined): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseJson(value: string | null): Rollup | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as Rollup;
  } catch {
    return null;
  }
}

function parseDepRollup(value: string | null): DepRollupState | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(value) as Partial<DepRollupState & DepStats>;
    if (
      parsed.totalTxs !== undefined &&
      parsed.dependentTxs !== undefined &&
      parsed.sumUpstreamTxs !== undefined &&
      parsed.sumUpstreamHandles !== undefined
    ) {
      return {
        totalTxs: parsed.totalTxs,
        dependentTxs: parsed.dependentTxs,
        sumUpstreamTxs: parsed.sumUpstreamTxs,
        sumUpstreamHandles: parsed.sumUpstreamHandles,
        maxUpstreamTxs: parsed.maxUpstreamTxs ?? 0,
        maxUpstreamHandles: parsed.maxUpstreamHandles ?? 0,
      };
    }
    if (parsed.dependentTxs !== undefined && parsed.avgUpstreamTxs !== undefined) {
      const dependentTxs = parsed.dependentTxs ?? 0;
      const totalTxs = parsed.totalTxs ?? dependentTxs;
      const sumUpstreamTxs = dependentTxs * (parsed.avgUpstreamTxs ?? 0);
      const sumUpstreamHandles = dependentTxs * (parsed.avgUpstreamHandles ?? 0);
      return {
        totalTxs,
        dependentTxs,
        sumUpstreamTxs,
        sumUpstreamHandles,
        maxUpstreamTxs: parsed.maxUpstreamTxs ?? 0,
        maxUpstreamHandles: parsed.maxUpstreamHandles ?? 0,
      };
    }
  } catch {
    return null;
  }
  return null;
}

function addCount(target: Record<string, number>, key: string, value: number): void {
  target[key] = (target[key] ?? 0) + value;
}

function mergeNested(
  target: Record<string, Record<string, number>>,
  source: Record<string, Record<string, number>>,
): void {
  for (const [op, bucket] of Object.entries(source)) {
    if (!target[op]) target[op] = {};
    for (const [key, value] of Object.entries(bucket)) {
      target[op][key] = (target[op][key] ?? 0) + value;
    }
  }
}

function mergeTypeCounts(target: Rollup["typeCounts"], source: Rollup["typeCounts"]): void {
  for (const [op, roles] of Object.entries(source)) {
    if (!target[op]) target[op] = {};
    for (const [role, counts] of Object.entries(roles)) {
      if (!target[op][role]) target[op][role] = {};
      for (const [typeKey, value] of Object.entries(counts)) {
        target[op][role][typeKey] = (target[op][role][typeKey] ?? 0) + value;
      }
    }
  }
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainId = parseNumber(Bun.env.CHAIN_ID);
const fullRollup = Bun.env.DFG_ROLLUP_FULL === "1" || Bun.env.DFG_ROLLUP_FULL === "true";
const depsEnabled = Bun.env.DFG_DEPS_ROLLUP !== "0" && Bun.env.DFG_DEPS_ROLLUP !== "false";
const depsForce = Bun.env.DFG_DEPS_FORCE === "1" || Bun.env.DFG_DEPS_FORCE === "true";

const db = initDatabase(dbPath);

const chains = chainId
  ? [chainId]
  : (
      db
        .prepare("SELECT DISTINCT chain_id AS chainId FROM dfg_txs ORDER BY chain_id")
        .all() as Array<{ chainId: number }>
    ).map((row) => row.chainId);

const statsStmt = db.prepare(
  `SELECT block_number AS blockNumber, tx_hash AS txHash, stats_json AS statsJson
   FROM dfg_txs
   WHERE chain_id = $chainId
     AND stats_json IS NOT NULL
     AND (
       $lastBlock IS NULL
       OR block_number > $lastBlock
       OR (block_number = $lastBlock AND tx_hash > $lastTxHash)
     )
   ORDER BY block_number, tx_hash`,
);
const countStmt = db.prepare("SELECT COUNT(*) AS count FROM dfg_txs WHERE chain_id = $chainId");
const checkpointStmt = db.prepare(
  `SELECT last_block AS lastBlock, last_tx_hash AS lastTxHash
   FROM dfg_rollup_checkpoints
   WHERE chain_id = $chainId`,
);
const deleteCheckpointStmt = db.prepare(
  "DELETE FROM dfg_rollup_checkpoints WHERE chain_id = $chainId",
);
const upsertCheckpointStmt = db.prepare(
  `INSERT INTO dfg_rollup_checkpoints (chain_id, last_block, last_tx_hash)
   VALUES ($chainId, $lastBlock, $lastTxHash)
   ON CONFLICT(chain_id) DO UPDATE
     SET last_block = excluded.last_block,
         last_tx_hash = excluded.last_tx_hash,
         updated_at = datetime('now')`,
);
const existingRollupStmt = db.prepare(
  "SELECT stats_json AS statsJson FROM dfg_rollups WHERE chain_id = $chainId",
);
const existingDepRollupStmt = db.prepare(
  "SELECT dfg_tx_count AS dfgTxCount, stats_json AS statsJson FROM dfg_dep_rollups WHERE chain_id = $chainId",
);

const upsertStmt = db.prepare(
  `INSERT INTO dfg_rollups (chain_id, dfg_tx_count, stats_json)
   VALUES ($chainId, $dfgTxCount, $statsJson)
   ON CONFLICT(chain_id) DO UPDATE
     SET dfg_tx_count = excluded.dfg_tx_count,
         stats_json = excluded.stats_json,
         updated_at = datetime('now')`,
);
const upsertDepStmt = db.prepare(
  `INSERT INTO dfg_dep_rollups (chain_id, dfg_tx_count, stats_json)
   VALUES ($chainId, $dfgTxCount, $statsJson)
   ON CONFLICT(chain_id) DO UPDATE
     SET dfg_tx_count = excluded.dfg_tx_count,
         stats_json = excluded.stats_json,
         updated_at = datetime('now')`,
);
const depFullStmt = db.prepare(
  `SELECT
     COUNT(*) AS totalTxs,
     SUM(CASE WHEN upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
     SUM(CASE WHEN upstream_txs > 0 THEN upstream_txs ELSE 0 END) AS sumUpstreamTxs,
     SUM(CASE WHEN upstream_txs > 0 THEN handle_links ELSE 0 END) AS sumUpstreamHandles,
     MAX(upstream_txs) AS maxUpstreamTxs,
     MAX(handle_links) AS maxUpstreamHandles
   FROM dfg_tx_deps
   WHERE chain_id = $chainId`,
);
const depIncrementalStmt = db.prepare(
  `SELECT upstream_txs AS upstreamTxs, handle_links AS handleLinks
   FROM dfg_tx_deps
   WHERE chain_id = $chainId
     AND (
       $lastBlock IS NULL
       OR block_number > $lastBlock
       OR (block_number = $lastBlock AND tx_hash > $lastTxHash)
     )
   ORDER BY block_number, tx_hash`,
);

const results: Array<{ chainId: number; dfgTxCount: number }> = [];
let usedFullRollup = fullRollup;
const fallbackChains: number[] = [];

for (const id of chains) {
  const checkpoint = fullRollup
    ? undefined
    : (checkpointStmt.get({ $chainId: id }) as
        | { lastBlock: number | null; lastTxHash: string | null }
        | undefined);
  const checkpointMissing = !fullRollup && !checkpoint;
  const effectiveFullRollup = fullRollup || checkpointMissing;
  if (checkpointMissing) {
    console.log(`rollup: no checkpoint for chain ${id}, rebuilding full rollup`);
    usedFullRollup = true;
    fallbackChains.push(id);
  }

  const rollup: Rollup = effectiveFullRollup
    ? {
        opCounts: {},
        inputKinds: {},
        operandPairs: {},
        typeCounts: {},
      }
    : (parseJson(
        (existingRollupStmt.get({ $chainId: id }) as { statsJson: string | null } | undefined)
          ?.statsJson ?? null,
      ) ?? {
        opCounts: {},
        inputKinds: {},
        operandPairs: {},
        typeCounts: {},
      });
  const lastBlock = effectiveFullRollup ? null : (checkpoint?.lastBlock ?? null);
  const lastTxHash = effectiveFullRollup ? "" : (checkpoint?.lastTxHash ?? "");

  const rows = statsStmt.all({
    $chainId: id,
    $lastBlock: lastBlock,
    $lastTxHash: lastTxHash,
  }) as Array<{ blockNumber: number; txHash: string; statsJson: string }>;
  for (const row of rows) {
    const stats = parseJson(row.statsJson);
    if (!stats) continue;
    for (const [op, count] of Object.entries(stats.opCounts ?? {})) {
      addCount(rollup.opCounts, op, count);
    }
    mergeNested(rollup.inputKinds, stats.inputKinds ?? {});
    mergeNested(rollup.operandPairs, stats.operandPairs ?? {});
    mergeTypeCounts(rollup.typeCounts, stats.typeCounts ?? {});
  }

  const dfgTxCount = (countStmt.get({ $chainId: id }) as { count: number }).count;
  upsertStmt.run({ $chainId: id, $dfgTxCount: dfgTxCount, $statsJson: JSON.stringify(rollup) });
  results.push({ chainId: id, dfgTxCount });

  if (depsEnabled) {
    const existingDeps = existingDepRollupStmt.get({ $chainId: id }) as
      | { dfgTxCount: number; statsJson: string }
      | undefined;
    const shouldComputeDeps =
      depsForce || effectiveFullRollup || !existingDeps || existingDeps.dfgTxCount !== dfgTxCount;

    if (shouldComputeDeps) {
      const depState = parseDepRollup(existingDeps?.statsJson ?? null) ?? {
        totalTxs: 0,
        dependentTxs: 0,
        sumUpstreamTxs: 0,
        sumUpstreamHandles: 0,
        maxUpstreamTxs: 0,
        maxUpstreamHandles: 0,
      };

      if (effectiveFullRollup || depsForce || !existingDeps) {
        const depRow = depFullStmt.get({ $chainId: id }) as {
          totalTxs: number;
          dependentTxs: number;
          sumUpstreamTxs: number;
          sumUpstreamHandles: number;
          maxUpstreamTxs: number | null;
          maxUpstreamHandles: number | null;
        };
        depState.totalTxs = depRow?.totalTxs ?? dfgTxCount;
        depState.dependentTxs = depRow?.dependentTxs ?? 0;
        depState.sumUpstreamTxs = depRow?.sumUpstreamTxs ?? 0;
        depState.sumUpstreamHandles = depRow?.sumUpstreamHandles ?? 0;
        depState.maxUpstreamTxs = depRow?.maxUpstreamTxs ?? 0;
        depState.maxUpstreamHandles = depRow?.maxUpstreamHandles ?? 0;
      } else {
        const newRows = depIncrementalStmt.all({
          $chainId: id,
          $lastBlock: lastBlock,
          $lastTxHash: lastTxHash,
        }) as Array<{ upstreamTxs: number; handleLinks: number }>;
        depState.totalTxs += newRows.length;
        for (const row of newRows) {
          if (row.upstreamTxs > 0) {
            depState.dependentTxs += 1;
            depState.sumUpstreamTxs += row.upstreamTxs;
            depState.sumUpstreamHandles += row.handleLinks;
          }
          depState.maxUpstreamTxs = Math.max(depState.maxUpstreamTxs, row.upstreamTxs);
          depState.maxUpstreamHandles = Math.max(depState.maxUpstreamHandles, row.handleLinks);
        }
        if (dfgTxCount > depState.totalTxs) {
          depState.totalTxs = dfgTxCount;
        }
      }

      upsertDepStmt.run({
        $chainId: id,
        $dfgTxCount: depState.totalTxs,
        $statsJson: JSON.stringify(depState),
      });
    }
  }

  if (rows.length > 0) {
    const lastRow = rows[rows.length - 1];
    if (lastRow) {
      upsertCheckpointStmt.run({
        $chainId: id,
        $lastBlock: lastRow.blockNumber,
        $lastTxHash: lastRow.txHash,
      });
    }
  } else if (effectiveFullRollup) {
    deleteCheckpointStmt.run({ $chainId: id });
  }
}

console.log(
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dbPath,
      incremental: !usedFullRollup,
      fullRollupRequested: fullRollup,
      fallbackChains: fallbackChains.length > 0 ? fallbackChains : undefined,
      chainIds: chains,
      results,
    },
    null,
    2,
  ),
);

db.close();
