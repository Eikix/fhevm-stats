import { Database } from "bun:sqlite";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";

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

type RecordedInput = {
  role: string;
  kind: string;
  handle?: string;
};

type TypeInfo = {
  inputs?: RecordedInput[];
};

type ExpectedInput = {
  role: string;
  kind: "handle" | "scalar";
  handle?: string;
};

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseBool(value: string | null | undefined, fallback = false): boolean {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = value.toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "yes") return true;
  if (normalized === "0" || normalized === "false" || normalized === "no") return false;
  return fallback;
}

function parseJson<T>(value: string | null): T | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function normalizeHandle(value: unknown): string | null {
  if (typeof value !== "string") return null;
  if (!value.startsWith("0x") || value.length !== 66) return null;
  return value;
}

function parseScalarByte(value: unknown): number | null {
  if (typeof value !== "string" || !value.startsWith("0x")) return null;
  const parsed = Number.parseInt(value.slice(2), 16);
  if (!Number.isFinite(parsed)) return null;
  return parsed === 0 ? 0 : 1;
}

function expectedInputsForEvent(op: string, args: Record<string, unknown>): ExpectedInput[] | null {
  if (BINARY_OPS.has(op)) {
    const lhs = normalizeHandle(args.lhs);
    if (!lhs) return null;

    const scalarFlag = parseScalarByte(args.scalarByte);
    if (scalarFlag === null) return null;

    if (scalarFlag === 1) {
      return [
        { role: "lhs", kind: "handle", handle: lhs },
        { role: "rhs", kind: "scalar" },
      ];
    }

    const rhs = normalizeHandle(args.rhs);
    if (!rhs) return null;
    return [
      { role: "lhs", kind: "handle", handle: lhs },
      { role: "rhs", kind: "handle", handle: rhs },
    ];
  }

  if (UNARY_OPS.has(op)) {
    const ct = normalizeHandle(args.ct);
    if (!ct) return null;
    return [{ role: "ct", kind: "handle", handle: ct }];
  }

  switch (op) {
    case "FheIfThenElse": {
      const control = normalizeHandle(args.control);
      const ifTrue = normalizeHandle(args.ifTrue);
      const ifFalse = normalizeHandle(args.ifFalse);
      if (!control || !ifTrue || !ifFalse) return null;
      return [
        { role: "control", kind: "handle", handle: control },
        { role: "ifTrue", kind: "handle", handle: ifTrue },
        { role: "ifFalse", kind: "handle", handle: ifFalse },
      ];
    }
    case "Cast": {
      const ct = normalizeHandle(args.ct);
      if (!ct) return null;
      return [
        { role: "ct", kind: "handle", handle: ct },
        { role: "toType", kind: "scalar" },
      ];
    }
    case "TrivialEncrypt":
      return [
        { role: "pt", kind: "scalar" },
        { role: "toType", kind: "scalar" },
      ];
    case "FheRand":
      return [
        { role: "seed", kind: "scalar" },
        { role: "randType", kind: "scalar" },
      ];
    case "FheRandBounded":
      return [
        { role: "seed", kind: "scalar" },
        { role: "upperBound", kind: "scalar" },
        { role: "randType", kind: "scalar" },
      ];
    default:
      return [];
  }
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainIdEnv = parseNumber(Bun.env.CHAIN_ID);
const startBlockEnv = parseNumber(Bun.env.START_BLOCK);
const endBlockEnv = parseNumber(Bun.env.END_BLOCK);
const lookbackBlocks = parseNumber(Bun.env.LOOKBACK_BLOCKS, 200) ?? 200;
const maxTxEnv = parseNumber(Bun.env.MAX_TX, 2000) ?? 2000;
const failOnMismatch = parseBool(Bun.env.FAIL_ON_MISMATCH, false);
const sampleLimit = parseNumber(Bun.env.SAMPLE_LIMIT, 20) ?? 20;

const db = new Database(dbPath, { readonly: true });
db.exec("PRAGMA busy_timeout=5000;");

const tables = new Set(
  (
    db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{
      name: string;
    }>
  ).map((row) => row.name),
);

const requiredTables = ["dfg_txs", "dfg_nodes", "fhe_events"];
const missingTables = requiredTables.filter((name) => !tables.has(name));
if (missingTables.length > 0) {
  console.error(
    JSON.stringify(
      {
        error: "missing_tables",
        dbPath,
        missingTables,
      },
      null,
      2,
    ),
  );
  db.close();
  process.exit(1);
}

const availableChains = (
  db.prepare("SELECT DISTINCT chain_id AS chainId FROM dfg_txs ORDER BY chain_id").all() as Array<{
    chainId: number;
  }>
).map((row) => row.chainId);

const requestedChains = chainIdEnv ? [chainIdEnv] : [1, 11155111];
const chainIds = chainIdEnv
  ? requestedChains
  : requestedChains.filter((chainId) => availableChains.includes(chainId));
const missingChains = requestedChains.filter((chainId) => !availableChains.includes(chainId));

const maxBlockStmt = db.prepare(
  "SELECT MAX(block_number) AS maxBlock FROM dfg_txs WHERE chain_id = $chainId",
);

const txListStmt = db.prepare(
  `SELECT tx_hash AS txHash, block_number AS blockNumber
   FROM dfg_txs
   WHERE chain_id = $chainId AND block_number >= $startBlock AND block_number <= $endBlock
   ORDER BY block_number, tx_hash
   ${maxTxEnv > 0 ? "LIMIT $limit" : ""}`,
);

const nodeStmt = db.prepare(
  `SELECT n.node_id AS nodeId,
          n.op AS op,
          n.scalar_flag AS scalarFlag,
          n.input_count AS inputCount,
          n.type_info_json AS typeInfoJson,
          e.args_json AS argsJson
   FROM dfg_nodes n
   LEFT JOIN fhe_events e
     ON e.chain_id = n.chain_id AND e.tx_hash = n.tx_hash AND e.log_index = n.node_id
   WHERE n.chain_id = $chainId AND n.tx_hash = $txHash
   ORDER BY n.node_id`,
);

const report: Record<string, unknown> = {
  generatedAt: new Date().toISOString(),
  dbPath,
  requestedChains,
  availableChains,
  missingChains,
  filters: {
    chainId: chainIdEnv ?? null,
    startBlock: startBlockEnv ?? null,
    endBlock: endBlockEnv ?? null,
    lookbackBlocks,
    maxTx: maxTxEnv,
  },
  chains: [] as Array<Record<string, unknown>>,
};

let hasMismatches = false;

for (const chainId of chainIds) {
  const maxBlockRow = maxBlockStmt.get({ $chainId: chainId }) as { maxBlock: number | null };
  const maxBlock = maxBlockRow.maxBlock ?? null;

  let endBlock = endBlockEnv ?? maxBlock ?? 0;
  let startBlock =
    startBlockEnv ??
    (endBlockEnv !== undefined ? endBlockEnv - lookbackBlocks + 1 : endBlock - lookbackBlocks + 1);
  if (startBlock < 0) startBlock = 0;
  if (endBlock < startBlock) endBlock = startBlock;

  const txRows = txListStmt.all({
    $chainId: chainId,
    $startBlock: startBlock,
    $endBlock: endBlock,
    ...(maxTxEnv > 0 ? { $limit: maxTxEnv } : {}),
  }) as Array<{ txHash: string; blockNumber: number }>;

  const counts = {
    txs: txRows.length,
    nodes: 0,
    argsMissing: 0,
    argsParseError: 0,
    typeInfoMissing: 0,
    typeInfoParseError: 0,
    invalidArgsForOp: 0,
    inputCountMismatch: 0,
    scalarFlagMismatch: 0,
    nodesWithMissingInputs: 0,
    missingInputs: 0,
    unexpectedHandleForScalar: 0,
  };

  const missingByOp: Record<string, number> = {};
  const missingByOpRole: Record<string, number> = {};

  const samples = {
    missingInputs: [] as Array<Record<string, unknown>>,
    scalarFlagMismatch: [] as Array<Record<string, unknown>>,
  };

  for (const tx of txRows) {
    const nodes = nodeStmt.all({ $chainId: chainId, $txHash: tx.txHash }) as Array<{
      nodeId: number;
      op: string;
      scalarFlag: number | null;
      inputCount: number;
      typeInfoJson: string | null;
      argsJson: string | null;
    }>;

    for (const node of nodes) {
      counts.nodes += 1;

      if (!node.argsJson) {
        counts.argsMissing += 1;
        continue;
      }

      const args = parseJson<Record<string, unknown>>(node.argsJson);
      if (!args) {
        counts.argsParseError += 1;
        continue;
      }

      if (!node.typeInfoJson) {
        counts.typeInfoMissing += 1;
        continue;
      }

      const typeInfo = parseJson<TypeInfo>(node.typeInfoJson);
      if (!typeInfo) {
        counts.typeInfoParseError += 1;
        continue;
      }

      const recordedInputs = typeInfo.inputs ?? [];
      if (node.inputCount !== recordedInputs.length) {
        counts.inputCountMismatch += 1;
      }

      const expected = expectedInputsForEvent(node.op, args);
      if (expected === null) {
        counts.invalidArgsForOp += 1;
        continue;
      }

      // scalar_flag check for binary ops (guards against "rhs dropped as scalar" issues)
      if (BINARY_OPS.has(node.op)) {
        const expectedScalar = parseScalarByte(args.scalarByte);
        if (
          expectedScalar !== null &&
          node.scalarFlag !== null &&
          node.scalarFlag !== expectedScalar
        ) {
          counts.scalarFlagMismatch += 1;
          if (samples.scalarFlagMismatch.length < sampleLimit) {
            samples.scalarFlagMismatch.push({
              txHash: tx.txHash,
              blockNumber: tx.blockNumber,
              nodeId: node.nodeId,
              op: node.op,
              expectedScalarFlag: expectedScalar,
              recordedScalarFlag: node.scalarFlag,
            });
          }
        }
      }

      const missingRoles: string[] = [];
      let unexpectedHandleForScalar = false;

      for (const input of expected) {
        if (input.kind === "handle") {
          const ok = recordedInputs.some(
            (rec) => rec.role === input.role && normalizeHandle(rec.handle) === input.handle,
          );
          if (!ok) {
            missingRoles.push(input.role);
            continue;
          }
          continue;
        }

        const ok = recordedInputs.some((rec) => rec.role === input.role && rec.kind === "scalar");
        if (!ok) {
          missingRoles.push(input.role);
        } else {
          const scalarRec = recordedInputs.find((rec) => rec.role === input.role);
          if (scalarRec?.handle) unexpectedHandleForScalar = true;
        }
      }

      if (unexpectedHandleForScalar) counts.unexpectedHandleForScalar += 1;

      if (missingRoles.length === 0) continue;

      hasMismatches = true;
      counts.nodesWithMissingInputs += 1;
      counts.missingInputs += missingRoles.length;

      missingByOp[node.op] = (missingByOp[node.op] ?? 0) + 1;
      for (const role of missingRoles) {
        const key = `${node.op}.${role}`;
        missingByOpRole[key] = (missingByOpRole[key] ?? 0) + 1;
      }

      if (samples.missingInputs.length < sampleLimit) {
        samples.missingInputs.push({
          txHash: tx.txHash,
          blockNumber: tx.blockNumber,
          nodeId: node.nodeId,
          op: node.op,
          missingRoles,
          recordedInputs,
        });
      }
    }
  }

  (report.chains as Array<Record<string, unknown>>).push({
    chainId,
    maxBlock,
    range: { startBlock, endBlock },
    scanned: counts,
    missingByOp,
    missingByOpRole,
    samples,
  });
}

console.log(JSON.stringify(report, null, 2));
db.close();

if (failOnMismatch && hasMismatches) {
  process.exit(2);
}
