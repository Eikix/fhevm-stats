import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;

const db = initDatabase(dbPath);

db.exec("BEGIN");
try {
  db.exec("DELETE FROM tx_seen");
  db.exec("DELETE FROM tx_counts");
  db.exec(`
    INSERT OR IGNORE INTO tx_seen (chain_id, tx_hash)
    SELECT chain_id, tx_hash
    FROM fhe_events
    GROUP BY chain_id, tx_hash
  `);
  db.exec(`
    INSERT INTO tx_counts (chain_id, count)
    SELECT chain_id, COUNT(*) AS count
    FROM tx_seen
    GROUP BY chain_id
  `);
  db.exec("COMMIT");
} catch (err) {
  db.exec("ROLLBACK");
  throw err;
}

const totals = db
  .prepare(
    `SELECT chain_id AS chainId, count AS total
     FROM tx_counts
     ORDER BY chain_id`,
  )
  .all() as Array<{ chainId: number; total: number }>;

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
