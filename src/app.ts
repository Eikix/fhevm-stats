import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { Database } from "bun:sqlite";
import {
  createPublicClient,
  decodeEventLog,
  getAddress,
  http,
  parseAbi,
  type AbiEvent,
} from "viem";

export type Mode = "backfill" | "stream" | "both";
export type NetworkName = "sepolia" | "devnet" | "mainnet" | "anvil" | "hardhat" | "custom";

export type Config = {
  rpcUrl: string;
  chainId?: number;
  fhevmExecutorAddress: string;
  startBlock?: number;
  endBlock?: number;
  confirmations: number;
  batchSize: number;
  catchupMaxBlocks: number;
  dbPath: string;
  mode: Mode;
  pollIntervalMs: number;
  network: NetworkName;
};

type ResolvedConfig = Omit<Config, "chainId"> & { chainId: number };

export type Env = Record<string, string | undefined>;
export type HandleMetadata = {
  type: number;
  version: number;
};

export type DerivedFields = {
  lhsType?: number | null;
  rhsType?: number | null;
  resultType?: number | null;
  controlType?: number | null;
  ifTrueType?: number | null;
  ifFalseType?: number | null;
  inputType?: number | null;
  castToType?: number | null;
  randType?: number | null;
  scalarFlag?: number | null;
  resultHandleVersion?: number | null;
};

export type TypeMismatch = {
  eventName: string;
  expectedType: number;
  actualType: number;
};

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const DEFAULT_POLL_INTERVAL_MS = 10_000;
const DEFAULT_SEPOLIA_RPC_URL = "https://ethereum-sepolia.publicnode.com";
const DEFAULT_MAINNET_RPC_URL = "https://ethereum.publicnode.com";
const DEFAULT_ANVIL_RPC_URL = "http://localhost:8545";
const DEFAULT_SEPOLIA_EXECUTOR_ADDRESS = "0x92C920834Ec8941d2C77D188936E1f7A6f49c127";
const DEFAULT_MAINNET_EXECUTOR_ADDRESS = "0xD82385dADa1ae3E969447f20A3164F6213100e75";
const TYPE_MISMATCHES_LOG_LIMIT = 50;

const FHE_EVENTS_ABI = parseAbi([
  "event FheAdd(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheSub(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheMul(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheDiv(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheRem(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheBitAnd(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheBitOr(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheBitXor(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheShl(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheShr(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheRotl(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheRotr(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheEq(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheNe(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheGe(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheGt(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheLe(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheLt(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheMin(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheMax(address indexed caller, bytes32 lhs, bytes32 rhs, bytes1 scalarByte, bytes32 result)",
  "event FheNeg(address indexed caller, bytes32 ct, bytes32 result)",
  "event FheNot(address indexed caller, bytes32 ct, bytes32 result)",
  "event VerifyInput(address indexed caller, bytes32 inputHandle, address userAddress, bytes inputProof, uint8 inputType, bytes32 result)",
  "event Cast(address indexed caller, bytes32 ct, uint8 toType, bytes32 result)",
  "event TrivialEncrypt(address indexed caller, uint256 pt, uint8 toType, bytes32 result)",
  "event FheIfThenElse(address indexed caller, bytes32 control, bytes32 ifTrue, bytes32 ifFalse, bytes32 result)",
  "event FheRand(address indexed caller, uint8 randType, bytes16 seed, bytes32 result)",
  "event FheRandBounded(address indexed caller, uint256 upperBound, uint8 randType, bytes16 seed, bytes32 result)",
]);

const EVENT_COLUMNS = [
  { name: "lhs_type", type: "INTEGER" },
  { name: "rhs_type", type: "INTEGER" },
  { name: "result_type", type: "INTEGER" },
  { name: "control_type", type: "INTEGER" },
  { name: "if_true_type", type: "INTEGER" },
  { name: "if_false_type", type: "INTEGER" },
  { name: "input_type", type: "INTEGER" },
  { name: "cast_to_type", type: "INTEGER" },
  { name: "rand_type", type: "INTEGER" },
  { name: "scalar_flag", type: "INTEGER" },
  { name: "result_handle_version", type: "INTEGER" },
];

const BINARY_OPS = new Set([
  "FheAdd",
  "FheSub",
  "FheMul",
  "FheDiv",
  "FheRem",
  "FheBitAnd",
  "FheBitOr",
  "FheBitXor",
  "FheShl",
  "FheShr",
  "FheRotl",
  "FheRotr",
  "FheEq",
  "FheNe",
  "FheGe",
  "FheGt",
  "FheLe",
  "FheLt",
  "FheMin",
  "FheMax",
]);

const UNARY_OPS = new Set(["FheNeg", "FheNot"]);
const NETWORK_DEFAULTS: Record<NetworkName, { chainId?: number; executorAddress?: string }> = {
  sepolia: {
    chainId: 11155111,
    executorAddress: DEFAULT_SEPOLIA_EXECUTOR_ADDRESS,
  },
  devnet: {
    chainId: 11155111,
  },
  mainnet: {
    chainId: 1,
    executorAddress: DEFAULT_MAINNET_EXECUTOR_ADDRESS,
  },
  anvil: {
    chainId: 31337,
  },
  hardhat: {
    chainId: 31337,
  },
  custom: {},
};

function parseNumber(value: string | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseMode(value: string | undefined): Mode {
  if (value === "backfill" || value === "stream" || value === "both") return value;
  return "both";
}

function parseNetwork(value: string | undefined): NetworkName {
  const normalized = (value ?? "custom").toLowerCase();
  switch (normalized) {
    case "sepolia":
    case "devnet":
    case "mainnet":
    case "anvil":
    case "hardhat":
      return normalized;
    default:
      return "custom";
  }
}

function parseNetworks(value: string | undefined): NetworkName[] {
  if (!value) return ["sepolia", "mainnet"];
  const raw = value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  const parsed = raw.length > 0 ? raw.map((entry) => parseNetwork(entry)) : [parseNetwork(value)];
  return Array.from(new Set(parsed));
}

function resolveRpcUrl(env: Env, network: NetworkName): string | undefined {
  if (env.RPC_URL) return env.RPC_URL;

  switch (network) {
    case "sepolia":
    case "devnet":
      return env.SEPOLIA_ETH_RPC_URL ?? DEFAULT_SEPOLIA_RPC_URL;
    case "mainnet":
      return env.MAINNET_ETH_RPC_URL ?? DEFAULT_MAINNET_RPC_URL;
    case "anvil":
    case "hardhat":
      return env.ANVIL_RPC_URL ?? DEFAULT_ANVIL_RPC_URL;
    default:
      return undefined;
  }
}

export function loadConfig(env: Env): Config {
  const network = parseNetwork(env.NETWORK);
  return loadConfigForNetwork(env, network);
}

export function loadConfigs(env: Env): Config[] {
  const networks = parseNetworks(env.NETWORK);
  if (networks.length > 1) {
    if (env.RPC_URL) {
      throw new Error("RPC_URL cannot be used with multiple networks.");
    }
    if (env.CHAIN_ID) {
      throw new Error("CHAIN_ID cannot be used with multiple networks.");
    }
    if (env.FHEVM_EXECUTOR_ADDRESS) {
      throw new Error("FHEVM_EXECUTOR_ADDRESS cannot be used with multiple networks.");
    }
  }
  return networks.map((network) => loadConfigForNetwork(env, network));
}

function loadConfigForNetwork(env: Env, network: NetworkName): Config {
  const defaults = NETWORK_DEFAULTS[network] ?? {};
  const rpcUrl = resolveRpcUrl(env, network);
  if (!rpcUrl) {
    throw new Error("RPC_URL is required (or set NETWORK and the matching *_RPC_URL var).");
  }

  const chainId = parseNumber(env.CHAIN_ID, defaults.chainId);
  const confirmations = parseNumber(env.CONFIRMATIONS, 0) ?? 0;
  const batchSize = parseNumber(env.BATCH_SIZE, 1_000) ?? 1_000;
  const catchupMaxBlocks = parseNumber(env.CATCHUP_MAX_BLOCKS, 256) ?? 256;

  const startBlock = parseNumber(env.START_BLOCK);
  const endBlock = parseNumber(env.END_BLOCK);

  const dbPath = env.DB_PATH ?? DEFAULT_DB_PATH;
  const pollIntervalMs =
    parseNumber(env.POLL_INTERVAL_MS, DEFAULT_POLL_INTERVAL_MS) ?? DEFAULT_POLL_INTERVAL_MS;

  const fhevmExecutorAddress = env.FHEVM_EXECUTOR_ADDRESS ?? defaults.executorAddress;
  if (!fhevmExecutorAddress) {
    throw new Error("FHEVM_EXECUTOR_ADDRESS is required (or set NETWORK to sepolia/mainnet).");
  }
  return {
    rpcUrl,
    chainId,
    fhevmExecutorAddress,
    startBlock,
    endBlock,
    confirmations,
    batchSize,
    catchupMaxBlocks,
    dbPath,
    mode: parseMode(env.MODE),
    pollIntervalMs,
    network,
  };
}

export function initDatabase(dbPath: string): Database {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode=WAL;");
  db.exec("PRAGMA busy_timeout=5000;");

  db.exec(`
    CREATE TABLE IF NOT EXISTS fhe_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      chain_id INTEGER NOT NULL,
      block_number INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      address TEXT NOT NULL,
      event_name TEXT NOT NULL,
      topic0 TEXT NOT NULL,
      data TEXT NOT NULL,
      args_json TEXT,
      lhs_type INTEGER,
      rhs_type INTEGER,
      result_type INTEGER,
      control_type INTEGER,
      if_true_type INTEGER,
      if_false_type INTEGER,
      input_type INTEGER,
      cast_to_type INTEGER,
      rand_type INTEGER,
      scalar_flag INTEGER,
      result_handle_version INTEGER,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS fhe_events_uniq
      ON fhe_events(chain_id, tx_hash, log_index);
    CREATE INDEX IF NOT EXISTS fhe_events_block
      ON fhe_events(chain_id, block_number);
    CREATE INDEX IF NOT EXISTS fhe_events_event
      ON fhe_events(chain_id, event_name);

    CREATE TABLE IF NOT EXISTS checkpoints (
      chain_id INTEGER PRIMARY KEY,
      last_block INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS op_buckets (
      chain_id INTEGER NOT NULL,
      bucket_start INTEGER NOT NULL,
      bucket_seconds INTEGER NOT NULL,
      event_name TEXT NOT NULL,
      count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (chain_id, bucket_start, bucket_seconds, event_name)
    );

    CREATE TABLE IF NOT EXISTS rollup_checkpoints (
      chain_id INTEGER PRIMARY KEY,
      last_block INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dfg_txs (
      chain_id INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      node_count INTEGER NOT NULL,
      edge_count INTEGER NOT NULL,
      depth INTEGER NOT NULL,
      signature_hash TEXT,
      stats_json TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (chain_id, tx_hash)
    );
    CREATE INDEX IF NOT EXISTS dfg_txs_block
      ON dfg_txs(chain_id, block_number);

    CREATE TABLE IF NOT EXISTS dfg_nodes (
      chain_id INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      node_id INTEGER NOT NULL,
      op TEXT NOT NULL,
      output_handle TEXT,
      input_count INTEGER NOT NULL,
      scalar_flag INTEGER,
      type_info_json TEXT,
      PRIMARY KEY (chain_id, tx_hash, node_id)
    );
    CREATE INDEX IF NOT EXISTS dfg_nodes_tx
      ON dfg_nodes(chain_id, tx_hash);
    CREATE INDEX IF NOT EXISTS dfg_nodes_output_handle
      ON dfg_nodes(chain_id, output_handle);

    CREATE TABLE IF NOT EXISTS dfg_edges (
      chain_id INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      from_node_id INTEGER NOT NULL,
      to_node_id INTEGER NOT NULL,
      input_handle TEXT NOT NULL,
      PRIMARY KEY (chain_id, tx_hash, from_node_id, to_node_id, input_handle)
    );
    CREATE INDEX IF NOT EXISTS dfg_edges_tx
      ON dfg_edges(chain_id, tx_hash);

    CREATE TABLE IF NOT EXISTS dfg_inputs (
      chain_id INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      handle TEXT NOT NULL,
      kind TEXT NOT NULL,
      PRIMARY KEY (chain_id, tx_hash, handle)
    );
    CREATE INDEX IF NOT EXISTS dfg_inputs_tx
      ON dfg_inputs(chain_id, tx_hash);
    CREATE INDEX IF NOT EXISTS dfg_inputs_handle
      ON dfg_inputs(chain_id, handle);

    CREATE TABLE IF NOT EXISTS dfg_handle_producers (
      chain_id INTEGER NOT NULL,
      handle TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      is_trivial INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (chain_id, handle)
    );
    CREATE INDEX IF NOT EXISTS dfg_handle_producers_tx
      ON dfg_handle_producers(chain_id, tx_hash);

    CREATE TABLE IF NOT EXISTS dfg_tx_deps (
      chain_id INTEGER NOT NULL,
      tx_hash TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      upstream_txs INTEGER NOT NULL,
      handle_links INTEGER NOT NULL,
      chain_depth INTEGER NOT NULL DEFAULT 0,
      total_depth INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (chain_id, tx_hash)
    );
    CREATE INDEX IF NOT EXISTS dfg_tx_deps_block
      ON dfg_tx_deps(chain_id, block_number, tx_hash);

    CREATE TABLE IF NOT EXISTS dfg_rollups (
      chain_id INTEGER PRIMARY KEY,
      dfg_tx_count INTEGER NOT NULL,
      stats_json TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dfg_dep_rollups (
      chain_id INTEGER PRIMARY KEY,
      dfg_tx_count INTEGER NOT NULL,
      stats_json TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dfg_rollup_checkpoints (
      chain_id INTEGER PRIMARY KEY,
      last_block INTEGER,
      last_tx_hash TEXT,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS dfg_build_checkpoints (
      chain_id INTEGER PRIMARY KEY,
      last_block INTEGER,
      last_tx_hash TEXT,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  ensureEventColumns(db);
  ensureDfgColumns(db);
  return db;
}

function ensureEventColumns(db: Database): void {
  const rows = db.prepare("PRAGMA table_info(fhe_events)").all() as Array<{
    name: string;
  }>;
  const existing = new Set(rows.map((row) => row.name));
  for (const column of EVENT_COLUMNS) {
    if (!existing.has(column.name)) {
      db.exec(`ALTER TABLE fhe_events ADD COLUMN ${column.name} ${column.type}`);
    }
  }
}

function ensureDfgColumns(db: Database): void {
  // Add is_trivial column to dfg_handle_producers if missing
  const producerCols = db.prepare("PRAGMA table_info(dfg_handle_producers)").all() as Array<{
    name: string;
  }>;
  const producerExisting = new Set(producerCols.map((row) => row.name));
  if (!producerExisting.has("is_trivial")) {
    db.exec("ALTER TABLE dfg_handle_producers ADD COLUMN is_trivial INTEGER NOT NULL DEFAULT 0");
  }

  // Add chain_depth column to dfg_tx_deps if missing
  const depsCols = db.prepare("PRAGMA table_info(dfg_tx_deps)").all() as Array<{
    name: string;
  }>;
  const depsExisting = new Set(depsCols.map((row) => row.name));
  if (!depsExisting.has("chain_depth")) {
    db.exec("ALTER TABLE dfg_tx_deps ADD COLUMN chain_depth INTEGER NOT NULL DEFAULT 0");
  }
  if (!depsExisting.has("total_depth")) {
    db.exec("ALTER TABLE dfg_tx_deps ADD COLUMN total_depth INTEGER NOT NULL DEFAULT 0");
  }
}

function createClient(config: Config) {
  if (!config.chainId) {
    return createPublicClient({
      transport: http(config.rpcUrl),
    });
  }
  return createPublicClient({
    chain: {
      id: config.chainId,
      name: "custom",
      nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
      rpcUrls: { default: { http: [config.rpcUrl] } },
    },
    transport: http(config.rpcUrl),
  });
}

function prepareStatements(db: Database) {
  const insertEvent = db.prepare(`
    INSERT OR IGNORE INTO fhe_events (
      chain_id,
      block_number,
      block_hash,
      tx_hash,
      log_index,
      address,
      event_name,
      topic0,
      data,
      args_json
      , lhs_type
      , rhs_type
      , result_type
      , control_type
      , if_true_type
      , if_false_type
      , input_type
      , cast_to_type
      , rand_type
      , scalar_flag
      , result_handle_version
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const getCheckpoint = db.prepare(`
    SELECT last_block FROM checkpoints WHERE chain_id = ?
  `);

  const upsertCheckpoint = db.prepare(`
    INSERT INTO checkpoints(chain_id, last_block)
    VALUES (?, ?)
    ON CONFLICT(chain_id) DO UPDATE
      SET last_block = excluded.last_block,
          updated_at = datetime('now')
  `);

  return { insertEvent, getCheckpoint, upsertCheckpoint };
}

function readCheckpoint(
  getCheckpoint: ReturnType<typeof prepareStatements>["getCheckpoint"],
  chainId: number,
): number | undefined {
  const row = getCheckpoint.get(chainId) as { last_block: number } | undefined;
  return row?.last_block;
}

function serializeArgs(args: Record<string, unknown> | undefined): string | null {
  if (!args) return null;
  return JSON.stringify(args, (_, value) => (typeof value === "bigint" ? value.toString() : value));
}

function parseSmallNumber(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    const parsed = value.startsWith("0x") ? Number.parseInt(value, 16) : Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function extractHandleMetadata(handle: unknown): HandleMetadata | null {
  if (typeof handle !== "string") return null;
  if (!handle.startsWith("0x") || handle.length !== 66) return null;
  const typeHex = handle.slice(2 + 30 * 2, 2 + 30 * 2 + 2);
  const versionHex = handle.slice(2 + 31 * 2, 2 + 31 * 2 + 2);
  const type = Number.parseInt(typeHex, 16);
  const version = Number.parseInt(versionHex, 16);
  if (Number.isNaN(type) || Number.isNaN(version)) return null;
  return { type, version };
}

function extractHandleType(handle: unknown): number | null {
  const meta = extractHandleMetadata(handle);
  return meta ? meta.type : null;
}

function parseScalarFlag(value: unknown): number | null {
  if (typeof value !== "string" || !value.startsWith("0x")) return null;
  return value.toLowerCase() === "0x00" ? 0 : 1;
}

export function deriveEventFields(eventName: string, args: Record<string, unknown>): DerivedFields {
  const derived: DerivedFields = {};

  const resultMeta = extractHandleMetadata(args.result);
  if (resultMeta) {
    derived.resultType = resultMeta.type;
    derived.resultHandleVersion = resultMeta.version;
  }

  if (BINARY_OPS.has(eventName)) {
    derived.lhsType = extractHandleType(args.lhs);
    derived.scalarFlag = parseScalarFlag(args.scalarByte);
    if (derived.scalarFlag === 0) {
      derived.rhsType = extractHandleType(args.rhs);
    }
    return derived;
  }

  if (UNARY_OPS.has(eventName)) {
    derived.lhsType = extractHandleType(args.ct);
    return derived;
  }

  switch (eventName) {
    case "FheIfThenElse":
      derived.controlType = extractHandleType(args.control);
      derived.ifTrueType = extractHandleType(args.ifTrue);
      derived.ifFalseType = extractHandleType(args.ifFalse);
      return derived;
    case "VerifyInput": {
      const inputType = parseSmallNumber(args.inputType);
      derived.inputType = inputType ?? extractHandleType(args.inputHandle);
      return derived;
    }
    case "Cast":
      derived.lhsType = extractHandleType(args.ct);
      derived.castToType = parseSmallNumber(args.toType);
      return derived;
    case "TrivialEncrypt":
      derived.castToType = parseSmallNumber(args.toType);
      return derived;
    case "FheRand":
    case "FheRandBounded":
      derived.randType = parseSmallNumber(args.randType);
      return derived;
    default:
      return derived;
  }
}

export function validateDerivedTypes(
  eventName: string,
  derived: DerivedFields,
): TypeMismatch | null {
  if (derived.resultType === null || derived.resultType === undefined) return null;
  let expected: number | null = null;

  switch (eventName) {
    case "VerifyInput":
      expected = derived.inputType ?? null;
      break;
    case "Cast":
    case "TrivialEncrypt":
      expected = derived.castToType ?? null;
      break;
    case "FheRand":
    case "FheRandBounded":
      expected = derived.randType ?? null;
      break;
    default:
      return null;
  }

  if (expected === null) return null;
  if (expected !== derived.resultType) {
    return {
      eventName,
      expectedType: expected,
      actualType: derived.resultType,
    };
  }
  return null;
}

const FHE_EVENTS = FHE_EVENTS_ABI.filter((item) => item.type === "event") as AbiEvent[];

function isInvalidBlockRangeError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const record = err as Record<string, unknown>;
  const message = typeof record.message === "string" ? record.message : "";
  const details = typeof record.details === "string" ? record.details : "";
  const shortMessage = typeof record.shortMessage === "string" ? record.shortMessage : "";
  return (
    message.includes("invalid block range") ||
    details.includes("invalid block range") ||
    shortMessage.includes("invalid block range")
  );
}

async function processRange(
  client: ReturnType<typeof createClient>,
  statements: ReturnType<typeof prepareStatements>,
  executorAddress: `0x${string}`,
  fromBlock: number,
  toBlock: number,
  chainId: number,
): Promise<void> {
  if (fromBlock > toBlock) return;

  let logs: Awaited<ReturnType<typeof client.getLogs>>;
  try {
    logs = await client.getLogs({
      address: executorAddress,
      fromBlock: BigInt(fromBlock),
      toBlock: BigInt(toBlock),
      events: FHE_EVENTS,
    });
  } catch (err) {
    if (fromBlock === toBlock && isInvalidBlockRangeError(err)) {
      console.warn("getLogs rejected block range; will retry next poll", {
        chainId,
        fromBlock,
        toBlock,
      });
      return;
    }
    throw err;
  }

  let mismatchCount = 0;
  for (const log of logs) {
    let eventName = "Unknown";
    let argsJson: string | null = null;
    let derived: DerivedFields = {};
    try {
      const decoded = decodeEventLog({
        abi: FHE_EVENTS_ABI,
        data: log.data,
        topics: log.topics,
      });
      eventName = decoded.eventName;
      const args = decoded.args as Record<string, unknown>;
      argsJson = serializeArgs(args);
      derived = deriveEventFields(eventName, args);
      const mismatch = validateDerivedTypes(eventName, derived);
      if (mismatch && mismatchCount < TYPE_MISMATCHES_LOG_LIMIT) {
        mismatchCount += 1;
        console.warn("type mismatch", {
          eventName: mismatch.eventName,
          expectedType: mismatch.expectedType,
          actualType: mismatch.actualType,
          blockNumber: Number(log.blockNumber),
          txHash: log.transactionHash,
          logIndex: Number(log.logIndex),
        });
      }
    } catch {
      eventName = "Unknown";
    }

    statements.insertEvent.run(
      chainId,
      Number(log.blockNumber),
      log.blockHash ?? "",
      log.transactionHash,
      Number(log.logIndex),
      getAddress(log.address),
      eventName,
      log.topics[0] ?? "",
      log.data,
      argsJson,
      derived.lhsType ?? null,
      derived.rhsType ?? null,
      derived.resultType ?? null,
      derived.controlType ?? null,
      derived.ifTrueType ?? null,
      derived.ifFalseType ?? null,
      derived.inputType ?? null,
      derived.castToType ?? null,
      derived.randType ?? null,
      derived.scalarFlag ?? null,
      derived.resultHandleVersion ?? null,
    );
  }

  statements.upsertCheckpoint.run(chainId, toBlock);
}

function resolveStartBlock(
  checkpoint: number | undefined,
  configStart: number | undefined,
  fallback: number,
  targetEnd: number,
  catchupMaxBlocks: number,
): number {
  if (checkpoint !== undefined) {
    const nextBlock = checkpoint + 1;
    if (catchupMaxBlocks > 0 && targetEnd - checkpoint > catchupMaxBlocks) {
      return Math.max(targetEnd - catchupMaxBlocks + 1, 0);
    }
    return nextBlock;
  }
  if (configStart !== undefined) return configStart;
  return fallback;
}

async function backfillOnce(
  client: ReturnType<typeof createClient>,
  statements: ReturnType<typeof prepareStatements>,
  executorAddress: `0x${string}`,
  config: ResolvedConfig,
  targetEnd: number,
): Promise<void> {
  const checkpoint = readCheckpoint(statements.getCheckpoint, config.chainId);
  const fromBlock = resolveStartBlock(
    checkpoint,
    config.startBlock,
    targetEnd,
    targetEnd,
    config.catchupMaxBlocks,
  );
  if (
    checkpoint !== undefined &&
    config.catchupMaxBlocks > 0 &&
    targetEnd - checkpoint > config.catchupMaxBlocks
  ) {
    console.warn("catchup limited", {
      chainId: config.chainId,
      checkpoint,
      targetEnd,
      catchupMaxBlocks: config.catchupMaxBlocks,
      fromBlock,
    });
  }

  let cursor = fromBlock;
  while (cursor <= targetEnd) {
    const batchEnd = Math.min(cursor + config.batchSize - 1, targetEnd);
    await processRange(client, statements, executorAddress, cursor, batchEnd, config.chainId);
    cursor = batchEnd + 1;
  }
}

export async function run(configInput: Config | Config[]): Promise<void> {
  const configs = Array.isArray(configInput) ? configInput : [configInput];
  const dbPath = configs[0]?.dbPath ?? DEFAULT_DB_PATH;
  for (const config of configs) {
    if (config.dbPath !== dbPath) {
      throw new Error("All networks must use the same DB_PATH when running together.");
    }
  }
  const db = initDatabase(dbPath);
  const statements = prepareStatements(db);

  const runtimes = await Promise.all(
    configs.map(async (config) => {
      const client = createClient(config);
      const rpcChainId = Number(await client.getChainId());
      if (config.chainId !== undefined && config.chainId !== rpcChainId) {
        throw new Error(`RPC chainId (${rpcChainId}) does not match CHAIN_ID (${config.chainId}).`);
      }
      const resolvedConfig: ResolvedConfig = {
        ...config,
        chainId: config.chainId ?? rpcChainId,
      };
      const executorAddress = getAddress(resolvedConfig.fhevmExecutorAddress);

      console.log("fhevm-stats config loaded", {
        rpcUrl: resolvedConfig.rpcUrl,
        chainId: resolvedConfig.chainId,
        network: resolvedConfig.network,
        fhevmExecutorAddress: executorAddress,
        startBlock: resolvedConfig.startBlock,
        endBlock: resolvedConfig.endBlock,
        confirmations: resolvedConfig.confirmations,
        batchSize: resolvedConfig.batchSize,
        catchupMaxBlocks: resolvedConfig.catchupMaxBlocks,
        dbPath: resolvedConfig.dbPath,
        mode: resolvedConfig.mode,
        pollIntervalMs: resolvedConfig.pollIntervalMs,
      });

      return { client, config: resolvedConfig, executorAddress };
    }),
  );

  const fetchTargetEnd = async (
    client: ReturnType<typeof createClient>,
    config: ResolvedConfig,
  ) => {
    const latest = Number(await client.getBlockNumber());
    const confirmed = latest - config.confirmations;
    return confirmed < 0 ? 0 : confirmed;
  };

  for (const runtime of runtimes) {
    if (runtime.config.mode === "backfill" || runtime.config.mode === "both") {
      const confirmedEnd = await fetchTargetEnd(runtime.client, runtime.config);
      const targetEnd =
        runtime.config.endBlock !== undefined
          ? Math.min(runtime.config.endBlock, confirmedEnd)
          : confirmedEnd;
      await backfillOnce(
        runtime.client,
        statements,
        runtime.executorAddress,
        runtime.config,
        targetEnd,
      );
    }
  }

  const shouldStream = runtimes.some(
    (runtime) => runtime.config.mode === "stream" || runtime.config.mode === "both",
  );
  if (shouldStream) {
    const pollIntervalMs = Math.min(...runtimes.map((runtime) => runtime.config.pollIntervalMs));
    // Stream by polling the latest confirmed block.
    for (;;) {
      for (const runtime of runtimes) {
        if (runtime.config.mode !== "stream" && runtime.config.mode !== "both") continue;
        const confirmedEnd = await fetchTargetEnd(runtime.client, runtime.config);
        await backfillOnce(
          runtime.client,
          statements,
          runtime.executorAddress,
          runtime.config,
          confirmedEnd,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  }

  db.close();
}

export async function runFromEnv(env: Env = Bun.env): Promise<void> {
  const configs = loadConfigs(env);
  await run(configs);
}
