import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;

const db = initDatabase(dbPath);

db.exec("BEGIN");
try {
  db.exec("DELETE FROM op_counts");
  db.exec(`
    INSERT INTO op_counts (chain_id, event_name, count)
    SELECT chain_id, event_name, COUNT(*) AS count
    FROM fhe_events
    GROUP BY chain_id, event_name
  `);
  db.exec("COMMIT");
} catch (err) {
  db.exec("ROLLBACK");
  throw err;
}

const totals = db
  .prepare(
    `SELECT chain_id AS chainId, SUM(count) AS total, COUNT(*) AS eventTypes
     FROM op_counts
     GROUP BY chain_id
     ORDER BY chain_id`,
  )
  .all() as Array<{ chainId: number; total: number; eventTypes: number }>;

console.log(
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dbPath,
      totals,
    },
    null,
    2,
  ),
);

db.close();
