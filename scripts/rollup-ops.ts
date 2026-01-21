import { createPublicClient, http } from "viem";
import { initDatabase, loadConfig } from "../src/app.ts";

type RollupRow = {
  blockNumber: number;
  eventName: string;
  count: number;
};

function parseNumber(value: string | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBucketStart(timestamp: bigint, bucketSeconds: number): number {
  const seconds = Number(timestamp);
  return Math.floor(seconds / bucketSeconds) * bucketSeconds;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const env = { ...Bun.env } as Record<string, string | undefined>;
  const config = loadConfig(env);
  const db = initDatabase(config.dbPath);
  const client = createPublicClient({ transport: http(config.rpcUrl) });

  const rpcChainId = Number(await client.getChainId());
  if (config.chainId !== undefined && config.chainId !== rpcChainId) {
    throw new Error(`RPC chainId (${rpcChainId}) does not match CHAIN_ID (${config.chainId}).`);
  }
  const chainId = config.chainId ?? rpcChainId;

  const bucketSeconds = parseNumber(env.ROLLUP_BUCKET_SECONDS, 1800) ?? 1800;
  if (bucketSeconds <= 0) {
    throw new Error("ROLLUP_BUCKET_SECONDS must be a positive number.");
  }

  const rangeRow = db
    .prepare(
      "SELECT MIN(block_number) AS minBlock, MAX(block_number) AS maxBlock FROM fhe_events WHERE chain_id = ?",
    )
    .get(chainId) as { minBlock: number | null; maxBlock: number | null };

  if (!rangeRow.maxBlock || rangeRow.minBlock === null) {
    console.log("rollup: no events found for chain", chainId);
    db.close();
    return;
  }

  const checkpointRow = db
    .prepare("SELECT last_block FROM rollup_checkpoints WHERE chain_id = ?")
    .get(chainId) as { last_block: number } | undefined;
  const checkpoint = checkpointRow?.last_block;

  const rollupStart = parseNumber(env.ROLLUP_START_BLOCK);
  const rollupEnd = parseNumber(env.ROLLUP_END_BLOCK);

  let startBlock = checkpoint !== undefined ? checkpoint + 1 : rangeRow.minBlock;
  if (rollupStart !== undefined) {
    if (checkpoint !== undefined && rollupStart <= checkpoint) {
      console.warn("rollup: ROLLUP_START_BLOCK ignored to avoid double counting", rollupStart);
    } else {
      startBlock = rollupStart;
    }
  }

  const endBlock = rollupEnd ?? rangeRow.maxBlock;
  if (startBlock > endBlock) {
    console.log("rollup: nothing to do (checkpoint up to date)");
    db.close();
    return;
  }

  const blockBatch = parseNumber(env.ROLLUP_BLOCK_BATCH, 5_000) ?? 5_000;
  const fetchDelayMs = parseNumber(env.ROLLUP_BLOCK_FETCH_DELAY_MS, 200) ?? 200;
  if (fetchDelayMs < 0) {
    throw new Error("ROLLUP_BLOCK_FETCH_DELAY_MS must be >= 0.");
  }
  const selectRows = db.prepare(`
    SELECT block_number AS blockNumber, event_name AS eventName, COUNT(*) AS count
    FROM fhe_events
    WHERE chain_id = ? AND block_number BETWEEN ? AND ?
    GROUP BY block_number, event_name
    ORDER BY block_number
  `);
  const upsertRollup = db.prepare(`
    INSERT INTO op_buckets (chain_id, bucket_start, bucket_seconds, event_name, count)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(chain_id, bucket_start, bucket_seconds, event_name)
    DO UPDATE SET count = count + excluded.count
  `);
  const upsertCheckpoint = db.prepare(`
    INSERT INTO rollup_checkpoints(chain_id, last_block)
    VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE
      SET last_block = excluded.last_block,
          updated_at = datetime('now')
  `);

  const bucketCache = new Map<number, number>();
  const writeRollups = db.transaction(
    (entries: Array<{ bucketStart: number; eventName: string; count: number }>) => {
      for (const entry of entries) {
        upsertRollup.run(chainId, entry.bucketStart, bucketSeconds, entry.eventName, entry.count);
      }
    },
  );

  console.log("rollup: starting", { chainId, startBlock, endBlock, bucketSeconds, blockBatch });

  let cursor = startBlock;
  while (cursor <= endBlock) {
    const batchEnd = Math.min(cursor + blockBatch - 1, endBlock);
    const rows = selectRows.all(chainId, cursor, batchEnd) as RollupRow[];

    const missingBlocks: number[] = [];
    for (const row of rows) {
      if (!bucketCache.has(row.blockNumber)) {
        missingBlocks.push(row.blockNumber);
      }
    }

    if (missingBlocks.length > 0) {
      const uniqueBlocks = Array.from(new Set(missingBlocks));
      for (const blockNumber of uniqueBlocks) {
        const block = await client.getBlock({ blockNumber: BigInt(blockNumber) });
        bucketCache.set(blockNumber, toBucketStart(block.timestamp, bucketSeconds));
        if (fetchDelayMs > 0) {
          await sleep(fetchDelayMs);
        }
      }
    }

    const grouped = new Map<string, { bucketStart: number; eventName: string; count: number }>();
    for (const row of rows) {
      const bucketStart = bucketCache.get(row.blockNumber);
      if (bucketStart === undefined) continue;
      const key = `${bucketStart}|${row.eventName}`;
      const existing = grouped.get(key);
      if (existing) {
        existing.count += row.count;
      } else {
        grouped.set(key, { bucketStart, eventName: row.eventName, count: row.count });
      }
    }

    if (grouped.size > 0) {
      writeRollups(Array.from(grouped.values()));
    }

    upsertCheckpoint.run(chainId, batchEnd);
    console.log("rollup: processed", { from: cursor, to: batchEnd, rows: rows.length });
    cursor = batchEnd + 1;
  }

  db.close();
  console.log("rollup: done");
}

main().catch((err) => {
  console.error("rollup: failed", err);
  process.exit(1);
});
