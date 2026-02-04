import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

function parseNumber(value: string | null | undefined): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainId = parseNumber(Bun.env.CHAIN_ID);
const startBlock = parseNumber(Bun.env.START_BLOCK);
const endBlock = parseNumber(Bun.env.END_BLOCK);

const db = initDatabase(dbPath);

const clauses: string[] = [
  "args_json IS NOT NULL",
  "json_extract(args_json, '$.caller') IS NOT NULL",
];
const params: Record<string, number> = {};

if (chainId !== undefined) {
  clauses.push("chain_id = $chainId");
  params.$chainId = chainId;
}
if (startBlock !== undefined) {
  clauses.push("block_number >= $startBlock");
  params.$startBlock = startBlock;
}
if (endBlock !== undefined) {
  clauses.push("block_number <= $endBlock");
  params.$endBlock = endBlock;
}

const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";

const stmt = db.prepare(
  `INSERT OR IGNORE INTO tx_callers(chain_id, tx_hash, caller)
   SELECT chain_id,
          tx_hash,
          MIN(lower(json_extract(args_json, '$.caller'))) AS caller
   FROM fhe_events
   ${whereClause}
   GROUP BY chain_id, tx_hash`,
);

db.exec("BEGIN");
try {
  stmt.run(params);
  db.exec("COMMIT");
} catch (err) {
  db.exec("ROLLBACK");
  throw err;
}

const countStmt = db.prepare(
  `SELECT COUNT(*) AS count
   FROM tx_callers
   ${chainId !== undefined ? "WHERE chain_id = $chainId" : ""}`,
);
const countRow = countStmt.get(chainId !== undefined ? { $chainId: chainId } : {}) as {
  count: number;
};

console.log(
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dbPath,
      filters: {
        chainId: chainId ?? null,
        startBlock: startBlock ?? null,
        endBlock: endBlock ?? null,
      },
      txCallersTotal: countRow.count,
    },
    null,
    2,
  ),
);

db.close();
