import { initDatabase } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const db = initDatabase(dbPath);
db.close();

console.log(
  JSON.stringify(
    {
      migratedAt: new Date().toISOString(),
      dbPath,
    },
    null,
    2,
  ),
);
