import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const db = new Database(dbPath, { readonly: true });

const totalRow = db.prepare("SELECT COUNT(*) AS count FROM fhe_events").get() as { count: number };

const rangeRow = db
  .prepare("SELECT MIN(block_number) AS minBlock, MAX(block_number) AS maxBlock FROM fhe_events")
  .get() as { minBlock: number | null; maxBlock: number | null };

const opCounts = db
  .prepare(
    "SELECT event_name AS eventName, COUNT(*) AS count FROM fhe_events GROUP BY event_name ORDER BY count DESC",
  )
  .all() as Array<{ eventName: string; count: number }>;

const output = {
  generatedAt: new Date().toISOString(),
  dbPath,
  totals: {
    events: totalRow.count,
    minBlock: rangeRow.minBlock,
    maxBlock: rangeRow.maxBlock,
  },
  opCounts,
};

console.log(JSON.stringify(output, null, 2));
