import { spawnSync } from "node:child_process";
import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

const networks = [
  { name: "mainnet", chainId: 1, rpcEnv: "MAINNET_ETH_RPC_URL" },
  { name: "sepolia", chainId: 11155111, rpcEnv: "SEPOLIA_ETH_RPC_URL" },
];

const rollupBucketSeconds = Bun.env.ROLLUP_BUCKET_SECONDS ?? "1800";
const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const db = new Database(dbPath, { readonly: true });
const countStmt = db.prepare("SELECT COUNT(*) AS count FROM fhe_events WHERE chain_id = ?");

for (const network of networks) {
  const countRow = countStmt.get(network.chainId) as { count: number };
  if (!countRow.count) {
    console.log(`Skipping ${network.name}: no events in DB (${dbPath}).`);
    continue;
  }

  console.log(`Running rollup for ${network.name} (chainId=${network.chainId})`);

  const childEnv: Record<string, string> = {};
  for (const [key, value] of Object.entries(Bun.env)) {
    if (value !== undefined) childEnv[key] = value;
  }
  childEnv.NETWORK = network.name;
  childEnv.ROLLUP_BUCKET_SECONDS = rollupBucketSeconds;
  delete childEnv.CHAIN_ID;
  delete childEnv.RPC_URL;
  delete childEnv.FHEVM_EXECUTOR_ADDRESS;

  const result = spawnSync("bun", ["run", "scripts/rollup-ops.ts"], {
    stdio: "inherit",
    env: childEnv,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

db.close();
