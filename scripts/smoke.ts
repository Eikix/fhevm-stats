import { runFromEnv } from "../src/app.ts";

function requiredEnv(value: string | undefined, name: string): string {
  if (!value) {
    throw new Error(`${name} is required for smoke tests`);
  }
  return value;
}

const baseEnv = { ...Bun.env } as Record<string, string | undefined>;
const smokeStart = requiredEnv(baseEnv.SMOKE_START_BLOCK, "SMOKE_START_BLOCK");
const smokeEnd = requiredEnv(baseEnv.SMOKE_END_BLOCK, "SMOKE_END_BLOCK");

const env = {
  ...baseEnv,
  MODE: "backfill",
  START_BLOCK: smokeStart,
  END_BLOCK: smokeEnd,
  BATCH_SIZE: baseEnv.BATCH_SIZE ?? "200",
};

runFromEnv(env).catch((err) => {
  console.error("smoke test failed", err);
  process.exit(1);
});
