import { runFromEnv } from "./src/app.ts";

runFromEnv().catch((err) => {
  console.error("fhevm-stats failed", err);
  process.exit(1);
});
