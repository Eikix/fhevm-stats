import { initDatabase } from "../src/app.ts";
import { computeDfgSignature } from "../src/dfg-signature.ts";

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

type InputKind = "ciphertext" | "trivial" | "external" | "scalar";

type InputInfo = {
  role: string;
  kind: InputKind;
  handle?: string;
  type?: number | null;
};

type EventRow = {
  log_index: number;
  event_name: string;
  args_json: string | null;
  lhs_type: number | null;
  rhs_type: number | null;
  result_type: number | null;
  control_type: number | null;
  if_true_type: number | null;
  if_false_type: number | null;
  input_type: number | null;
  cast_to_type: number | null;
  rand_type: number | null;
  scalar_flag: number | null;
};

type TxRow = {
  chain_id: number;
  tx_hash: string;
  block_number: number;
};

type DfgNode = {
  nodeId: number;
  op: string;
  outputHandle: string | null;
  inputCount: number;
  scalarFlag: number | null;
  typeInfoJson: string;
};

type DfgEdge = {
  fromNodeId: number;
  toNodeId: number;
  inputHandle: string;
};

type ProducedHandle = {
  nodeId: number;
  kind: "ciphertext" | "trivial";
  depth: number;
};

type DfgStats = {
  opCounts: Record<string, number>;
  inputKinds: Record<string, Record<InputKind, number>>;
  operandPairs: Record<string, Record<string, number>>;
  typeCounts: Record<string, Record<string, Record<string, number>>>;
};

function parseNumber(value: string | null | undefined, fallback?: number): number | undefined {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseArgs(argsJson: string | null): Record<string, unknown> | null {
  if (!argsJson) return null;
  try {
    return JSON.parse(argsJson) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function normalizeHandle(value: unknown): string | null {
  if (typeof value !== "string") return null;
  if (!value.startsWith("0x") || value.length !== 66) return null;
  return value;
}

function incrementCounter<T extends string>(
  target: Record<string, Record<T, number>>,
  op: string,
  key: T,
): void {
  if (!target[op]) target[op] = {} as Record<T, number>;
  target[op][key] = (target[op][key] ?? 0) + 1;
}

function incrementTypeCounts(
  target: DfgStats["typeCounts"],
  op: string,
  role: string,
  value: number | null,
): void {
  if (value === null || value === undefined) return;
  if (!target[op]) target[op] = {};
  if (!target[op][role]) target[op][role] = {};
  const key = String(value);
  target[op][role][key] = (target[op][role][key] ?? 0) + 1;
}

function addOpCount(target: Record<string, number>, op: string): void {
  target[op] = (target[op] ?? 0) + 1;
}

function addPairCount(
  target: Record<string, Record<string, number>>,
  op: string,
  left: InputKind,
  right: InputKind,
): void {
  if (!target[op]) target[op] = {};
  const key = `${left}-${right}`;
  target[op][key] = (target[op][key] ?? 0) + 1;
}

function buildDfg(events: EventRow[]) {
  const nodes: DfgNode[] = [];
  const edges: DfgEdge[] = [];
  const edgeKey = new Set<string>();
  const producedHandles = new Map<string, ProducedHandle>();
  const externalHandles = new Set<string>();
  const nodeDepths = new Map<number, number>();
  let maxDepth = 0;

  const stats: DfgStats = {
    opCounts: {},
    inputKinds: {},
    operandPairs: {},
    typeCounts: {},
  };

  for (const event of events) {
    const op = event.event_name;
    if (op === "Unknown" || op === "VerifyInput") continue;
    const args = parseArgs(event.args_json);
    if (!args) continue;

    const outputHandle = normalizeHandle(args.result);
    const outputKind: ProducedHandle["kind"] = op === "TrivialEncrypt" ? "trivial" : "ciphertext";
    const scalarFlag = event.scalar_flag ?? null;

    const inputHandles: Array<{ role: string; handle: string; type?: number | null }> = [];
    const scalarInputs: Array<{ role: string; type?: number | null }> = [];

    if (BINARY_OPS.has(op)) {
      const lhs = normalizeHandle(args.lhs);
      if (lhs) {
        inputHandles.push({ role: "lhs", handle: lhs, type: event.lhs_type ?? null });
      }
      if (scalarFlag === 1) {
        scalarInputs.push({ role: "rhs" });
      } else {
        const rhs = normalizeHandle(args.rhs);
        if (rhs) {
          inputHandles.push({ role: "rhs", handle: rhs, type: event.rhs_type ?? null });
        }
      }
    } else if (UNARY_OPS.has(op)) {
      const ct = normalizeHandle(args.ct);
      if (ct) {
        inputHandles.push({ role: "ct", handle: ct, type: event.lhs_type ?? null });
      }
    } else {
      switch (op) {
        case "FheIfThenElse": {
          const control = normalizeHandle(args.control);
          const ifTrue = normalizeHandle(args.ifTrue);
          const ifFalse = normalizeHandle(args.ifFalse);
          if (control) {
            inputHandles.push({
              role: "control",
              handle: control,
              type: event.control_type ?? null,
            });
          }
          if (ifTrue) {
            inputHandles.push({ role: "ifTrue", handle: ifTrue, type: event.if_true_type ?? null });
          }
          if (ifFalse) {
            inputHandles.push({
              role: "ifFalse",
              handle: ifFalse,
              type: event.if_false_type ?? null,
            });
          }
          break;
        }
        case "Cast": {
          const ct = normalizeHandle(args.ct);
          if (ct) {
            inputHandles.push({ role: "ct", handle: ct, type: event.lhs_type ?? null });
          }
          scalarInputs.push({ role: "toType", type: event.cast_to_type ?? null });
          break;
        }
        case "TrivialEncrypt": {
          const toType = event.cast_to_type ?? null;
          scalarInputs.push({ role: "pt", type: toType });
          scalarInputs.push({ role: "toType", type: toType });
          break;
        }
        case "FheRandBounded": {
          scalarInputs.push({ role: "seed" });
          scalarInputs.push({ role: "upperBound" });
          scalarInputs.push({ role: "randType", type: event.rand_type ?? null });
          break;
        }
        case "FheRand":
          scalarInputs.push({ role: "seed" });
          scalarInputs.push({ role: "randType", type: event.rand_type ?? null });
          break;
        default:
          break;
      }
    }

    const inputInfos: InputInfo[] = [];
    const parentDepths: number[] = [];
    const kindByRole = new Map<string, InputKind>();

    for (const input of inputHandles) {
      const producer = producedHandles.get(input.handle);
      const kind: InputKind = producer ? producer.kind : "external";
      inputInfos.push({
        role: input.role,
        kind,
        handle: input.handle,
        type: input.type ?? null,
      });
      kindByRole.set(input.role, kind);
      if (producer) {
        const edgeId = `${producer.nodeId}-${event.log_index}-${input.handle}`;
        if (!edgeKey.has(edgeId)) {
          edges.push({
            fromNodeId: producer.nodeId,
            toNodeId: event.log_index,
            inputHandle: input.handle,
          });
          edgeKey.add(edgeId);
        }
        parentDepths.push(producer.depth);
      } else {
        externalHandles.add(input.handle);
      }
    }

    for (const input of scalarInputs) {
      inputInfos.push({ role: input.role, kind: "scalar", type: input.type ?? null });
      kindByRole.set(input.role, "scalar");
    }

    const depth = parentDepths.length > 0 ? Math.max(...parentDepths) + 1 : 1;
    nodeDepths.set(event.log_index, depth);
    maxDepth = Math.max(maxDepth, depth);

    if (outputHandle) {
      producedHandles.set(outputHandle, { nodeId: event.log_index, kind: outputKind, depth });
    }

    addOpCount(stats.opCounts, op);
    for (const input of inputInfos) {
      incrementCounter(stats.inputKinds, op, input.kind);
    }

    if (BINARY_OPS.has(op)) {
      const lhsKind = kindByRole.get("lhs");
      const rhsKind = kindByRole.get("rhs") ?? (scalarFlag === 1 ? "scalar" : undefined);
      if (lhsKind && rhsKind) {
        addPairCount(stats.operandPairs, op, lhsKind, rhsKind);
      }
    }

    incrementTypeCounts(stats.typeCounts, op, "result", event.result_type ?? null);

    if (BINARY_OPS.has(op)) {
      incrementTypeCounts(stats.typeCounts, op, "lhs", event.lhs_type ?? null);
      if (scalarFlag !== 1) {
        incrementTypeCounts(stats.typeCounts, op, "rhs", event.rhs_type ?? null);
      }
    } else if (UNARY_OPS.has(op)) {
      incrementTypeCounts(stats.typeCounts, op, "lhs", event.lhs_type ?? null);
    } else {
      switch (op) {
        case "FheIfThenElse":
          incrementTypeCounts(stats.typeCounts, op, "control", event.control_type ?? null);
          incrementTypeCounts(stats.typeCounts, op, "ifTrue", event.if_true_type ?? null);
          incrementTypeCounts(stats.typeCounts, op, "ifFalse", event.if_false_type ?? null);
          break;
        case "Cast":
          incrementTypeCounts(stats.typeCounts, op, "ct", event.lhs_type ?? null);
          incrementTypeCounts(stats.typeCounts, op, "toType", event.cast_to_type ?? null);
          break;
        case "TrivialEncrypt":
          incrementTypeCounts(stats.typeCounts, op, "pt", event.cast_to_type ?? null);
          incrementTypeCounts(stats.typeCounts, op, "toType", event.cast_to_type ?? null);
          break;
        case "FheRand":
          incrementTypeCounts(stats.typeCounts, op, "randType", event.rand_type ?? null);
          break;
        case "FheRandBounded":
          incrementTypeCounts(stats.typeCounts, op, "randType", event.rand_type ?? null);
          break;
        default:
          break;
      }
    }

    const typeInfo = {
      inputs: inputInfos,
      output: {
        handle: outputHandle,
        kind: outputKind,
        type: event.result_type ?? null,
      },
      types: {
        lhs: event.lhs_type ?? null,
        rhs: event.rhs_type ?? null,
        control: event.control_type ?? null,
        ifTrue: event.if_true_type ?? null,
        ifFalse: event.if_false_type ?? null,
        input: event.input_type ?? null,
        castTo: event.cast_to_type ?? null,
        rand: event.rand_type ?? null,
      },
    };

    nodes.push({
      nodeId: event.log_index,
      op,
      outputHandle: outputHandle ?? null,
      inputCount: inputInfos.length,
      scalarFlag,
      typeInfoJson: JSON.stringify(typeInfo),
    });
  }

  // Track which output handles are from TrivialEncrypt
  const trivialHandles = new Set<string>();
  for (const node of nodes) {
    if (node.op === "TrivialEncrypt" && node.outputHandle) {
      trivialHandles.add(node.outputHandle);
    }
  }

  return {
    nodes,
    edges,
    externalHandles,
    trivialHandles,
    stats,
    depth: maxDepth,
  };
}

function computeSignature(nodes: DfgNode[], edges: DfgEdge[]): string {
  return computeDfgSignature(
    nodes.map((node) => ({ nodeId: node.nodeId, op: node.op })),
    edges.map((edge) => ({ fromNodeId: edge.fromNodeId, toNodeId: edge.toNodeId })),
  );
}

const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const chainIdEnv = parseNumber(Bun.env.CHAIN_ID);
const startBlock = parseNumber(Bun.env.START_BLOCK);
const endBlock = parseNumber(Bun.env.END_BLOCK);
const txHash = Bun.env.TX_HASH;
const limit = parseNumber(Bun.env.LIMIT);
const fullBuild = Bun.env.DFG_BUILD_FULL === "1" || Bun.env.DFG_BUILD_FULL === "true";

const db = initDatabase(dbPath);

const chainId = chainIdEnv;

// Check for checkpoint (incremental build)
type Checkpoint = { lastBlock: number | null; lastTxHash: string | null };
const getCheckpoint = db.prepare(
  `SELECT last_block AS lastBlock, last_tx_hash AS lastTxHash
   FROM dfg_build_checkpoints WHERE chain_id = $chainId`,
);
const upsertCheckpoint = db.prepare(
  `INSERT INTO dfg_build_checkpoints (chain_id, last_block, last_tx_hash)
   VALUES ($chainId, $lastBlock, $lastTxHash)
   ON CONFLICT(chain_id) DO UPDATE
     SET last_block = excluded.last_block,
         last_tx_hash = excluded.last_tx_hash,
         updated_at = datetime('now')`,
);
const deleteCheckpoint = db.prepare(`DELETE FROM dfg_build_checkpoints WHERE chain_id = $chainId`);

// Get checkpoint for incremental builds
let checkpoint: Checkpoint | undefined;
const useIncremental = !fullBuild && !txHash && chainId !== undefined;
if (useIncremental && chainId !== undefined) {
  checkpoint = getCheckpoint.get({ $chainId: chainId }) as Checkpoint | undefined;
  if (!checkpoint) {
    console.log(`dfg:build: no checkpoint for chain ${chainId}, will create after first run`);
  }
}

const clauses: string[] = [];
const params: Record<string, string | number | null> = {};

if (chainId !== undefined) {
  clauses.push("chain_id = $chainId");
  params.$chainId = chainId;
}
if (startBlock !== undefined) {
  clauses.push("block_number >= $startBlock");
  params.$startBlock = startBlock;
}
if (endBlock !== undefined) {
  clauses.push("block_number <= $endBlock");
  params.$endBlock = endBlock;
}
if (txHash) {
  clauses.push("tx_hash = $txHash");
  params.$txHash = txHash;
}

// Apply checkpoint filter for incremental builds
if (useIncremental && checkpoint?.lastBlock !== undefined && checkpoint.lastBlock !== null) {
  clauses.push(
    `(block_number > $checkpointBlock OR (block_number = $checkpointBlock AND tx_hash > $checkpointTxHash))`,
  );
  params.$checkpointBlock = checkpoint.lastBlock;
  params.$checkpointTxHash = checkpoint.lastTxHash ?? "";
}

const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
const limitClause = limit !== undefined ? "LIMIT $limit" : "";
if (limit !== undefined) params.$limit = limit;

const txRows = db
  .prepare(
    `SELECT chain_id, tx_hash, MIN(block_number) AS block_number
     FROM fhe_events
     ${whereClause}
     GROUP BY chain_id, tx_hash
     ORDER BY block_number, tx_hash
     ${limitClause}`,
  )
  .all(params) as TxRow[];

const loadEvents = db.prepare(
  `SELECT log_index, event_name, args_json,
          lhs_type, rhs_type, result_type, control_type, if_true_type, if_false_type,
          input_type, cast_to_type, rand_type, scalar_flag
   FROM fhe_events
   WHERE chain_id = $chainId AND tx_hash = $txHash
   ORDER BY log_index`,
);

const deleteTx = db.prepare("DELETE FROM dfg_txs WHERE chain_id = $chainId AND tx_hash = $txHash");
const deleteNodes = db.prepare(
  "DELETE FROM dfg_nodes WHERE chain_id = $chainId AND tx_hash = $txHash",
);
const deleteEdges = db.prepare(
  "DELETE FROM dfg_edges WHERE chain_id = $chainId AND tx_hash = $txHash",
);
const deleteInputs = db.prepare(
  "DELETE FROM dfg_inputs WHERE chain_id = $chainId AND tx_hash = $txHash",
);
const deleteDeps = db.prepare(
  "DELETE FROM dfg_tx_deps WHERE chain_id = $chainId AND tx_hash = $txHash",
);
const deleteHandleProducers = db.prepare(
  "DELETE FROM dfg_handle_producers WHERE chain_id = $chainId AND tx_hash = $txHash",
);

const insertTx = db.prepare(
  `INSERT INTO dfg_txs (
     chain_id, tx_hash, block_number, node_count, edge_count, depth, signature_hash, stats_json
   ) VALUES (
     $chainId, $txHash, $blockNumber, $nodeCount, $edgeCount, $depth, $signatureHash, $statsJson
   )`,
);

const insertNode = db.prepare(
  `INSERT INTO dfg_nodes (
     chain_id, tx_hash, node_id, op, output_handle, input_count, scalar_flag, type_info_json
   ) VALUES (
     $chainId, $txHash, $nodeId, $op, $outputHandle, $inputCount, $scalarFlag, $typeInfoJson
   )`,
);

const insertEdge = db.prepare(
  `INSERT INTO dfg_edges (
     chain_id, tx_hash, from_node_id, to_node_id, input_handle
   ) VALUES (
     $chainId, $txHash, $fromNodeId, $toNodeId, $inputHandle
   )`,
);

const insertInput = db.prepare(
  `INSERT INTO dfg_inputs (
     chain_id, tx_hash, handle, kind
   ) VALUES (
     $chainId, $txHash, $handle, $kind
   )`,
);
const insertHandleProducer = db.prepare(
  `INSERT INTO dfg_handle_producers (
     chain_id, handle, tx_hash, block_number, is_trivial
   ) VALUES (
     $chainId, $handle, $txHash, $blockNumber, $isTrivial
   )
   ON CONFLICT(chain_id, handle) DO UPDATE
     SET tx_hash = excluded.tx_hash,
         block_number = excluded.block_number,
         is_trivial = excluded.is_trivial,
         updated_at = datetime('now')`,
);
const insertTxDeps = db.prepare(
  `INSERT INTO dfg_tx_deps (
     chain_id, tx_hash, block_number, upstream_txs, handle_links, chain_depth, total_depth
   ) VALUES (
     $chainId, $txHash, $blockNumber, $upstreamTxs, $handleLinks, $chainDepth, $totalDepth
   )
   ON CONFLICT(chain_id, tx_hash) DO UPDATE
     SET block_number = excluded.block_number,
         upstream_txs = excluded.upstream_txs,
         handle_links = excluded.handle_links,
         chain_depth = excluded.chain_depth,
         total_depth = excluded.total_depth,
         updated_at = datetime('now')`,
);
const lookupProducer = db.prepare(
  `SELECT tx_hash AS txHash, is_trivial AS isTrivial
   FROM dfg_handle_producers
   WHERE chain_id = $chainId AND handle = $handle AND block_number <= $blockNumber
   LIMIT 1`,
);
const lookupTxChainDepth = db.prepare(
  `SELECT chain_depth AS chainDepth
   FROM dfg_tx_deps
   WHERE chain_id = $chainId AND tx_hash = $txHash
   LIMIT 1`,
);
const lookupTxIntraDepth = db.prepare(
  `SELECT depth FROM dfg_txs WHERE chain_id = $chainId AND tx_hash = $txHash`,
);

let processed = 0;

for (const tx of txRows) {
  const events = loadEvents.all({ $chainId: tx.chain_id, $txHash: tx.tx_hash }) as EventRow[];
  if (events.length === 0) continue;

  const { nodes, edges, externalHandles, trivialHandles, stats, depth } = buildDfg(events);
  const nodeCount = nodes.length;
  const edgeCount = edges.length;
  const signatureHash = nodeCount > 0 ? computeSignature(nodes, edges) : null;
  const statsJson = JSON.stringify(stats);
  const outputHandles = new Set<string>();
  for (const node of nodes) {
    if (node.outputHandle) outputHandles.add(node.outputHandle);
  }
  const upstreamTxs = new Set<string>();
  const nonTrivialUpstreamTxs = new Set<string>();
  let handleLinks = 0;
  for (const handle of externalHandles) {
    const producer = lookupProducer.get({
      $chainId: tx.chain_id,
      $handle: handle,
      $blockNumber: tx.block_number,
    }) as { txHash: string; isTrivial: number } | undefined;
    if (producer && producer.txHash !== tx.tx_hash) {
      upstreamTxs.add(producer.txHash);
      handleLinks += 1;
      // Track non-trivial upstream txs for chain_depth computation
      if (producer.isTrivial !== 1) {
        nonTrivialUpstreamTxs.add(producer.txHash);
      }
    }
  }

  // Compute chain_depth: max chain_depth of non-trivial upstream txs + 1
  // If no non-trivial upstream deps, chain_depth = 0
  let chainDepth = 0;
  let maxUpstreamIntraDepth = 0;
  if (nonTrivialUpstreamTxs.size > 0) {
    let maxUpstreamChainDepth = 0;
    for (const upstreamTxHash of nonTrivialUpstreamTxs) {
      const depthRow = lookupTxChainDepth.get({
        $chainId: tx.chain_id,
        $txHash: upstreamTxHash,
      }) as { chainDepth: number } | undefined;
      if (depthRow && depthRow.chainDepth > maxUpstreamChainDepth) {
        maxUpstreamChainDepth = depthRow.chainDepth;
      }
      // Get max intra-tx depth from upstream txs
      const intraDepthRow = lookupTxIntraDepth.get({
        $chainId: tx.chain_id,
        $txHash: upstreamTxHash,
      }) as { depth: number } | undefined;
      if (intraDepthRow && intraDepthRow.depth > maxUpstreamIntraDepth) {
        maxUpstreamIntraDepth = intraDepthRow.depth;
      }
    }
    chainDepth = maxUpstreamChainDepth + 1;
  }

  // Compute total_depth: chain_depth + max upstream intra-tx depth + current tx intra-tx depth
  // This gives the full critical path including all FHE operations
  const currentTxIntraDepth = depth; // from buildDfg
  const totalDepth = chainDepth + maxUpstreamIntraDepth + currentTxIntraDepth;

  try {
    db.exec("BEGIN");
    deleteTx.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });
    deleteNodes.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });
    deleteEdges.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });
    deleteInputs.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });
    deleteDeps.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });
    deleteHandleProducers.run({ $chainId: tx.chain_id, $txHash: tx.tx_hash });

    insertTx.run({
      $chainId: tx.chain_id,
      $txHash: tx.tx_hash,
      $blockNumber: tx.block_number,
      $nodeCount: nodeCount,
      $edgeCount: edgeCount,
      $depth: depth,
      $signatureHash: signatureHash,
      $statsJson: statsJson,
    });

    for (const node of nodes) {
      insertNode.run({
        $chainId: tx.chain_id,
        $txHash: tx.tx_hash,
        $nodeId: node.nodeId,
        $op: node.op,
        $outputHandle: node.outputHandle,
        $inputCount: node.inputCount,
        $scalarFlag: node.scalarFlag,
        $typeInfoJson: node.typeInfoJson,
      });
    }

    for (const edge of edges) {
      insertEdge.run({
        $chainId: tx.chain_id,
        $txHash: tx.tx_hash,
        $fromNodeId: edge.fromNodeId,
        $toNodeId: edge.toNodeId,
        $inputHandle: edge.inputHandle,
      });
    }

    for (const handle of externalHandles) {
      insertInput.run({
        $chainId: tx.chain_id,
        $txHash: tx.tx_hash,
        $handle: handle,
        $kind: "external",
      });
    }

    insertTxDeps.run({
      $chainId: tx.chain_id,
      $txHash: tx.tx_hash,
      $blockNumber: tx.block_number,
      $upstreamTxs: upstreamTxs.size,
      $handleLinks: handleLinks,
      $chainDepth: chainDepth,
      $totalDepth: totalDepth,
    });

    for (const handle of outputHandles) {
      insertHandleProducer.run({
        $chainId: tx.chain_id,
        $handle: handle,
        $txHash: tx.tx_hash,
        $blockNumber: tx.block_number,
        $isTrivial: trivialHandles.has(handle) ? 1 : 0,
      });
    }

    db.exec("COMMIT");
    processed += 1;
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
}

// Update checkpoint after successful processing
const lastTx = txRows.length > 0 ? txRows[txRows.length - 1] : undefined;
if (useIncremental && chainId !== undefined && lastTx) {
  upsertCheckpoint.run({
    $chainId: chainId,
    $lastBlock: lastTx.block_number,
    $lastTxHash: lastTx.tx_hash,
  });
} else if (fullBuild && chainId !== undefined) {
  // Full build: reset checkpoint to last processed tx
  if (lastTx) {
    upsertCheckpoint.run({
      $chainId: chainId,
      $lastBlock: lastTx.block_number,
      $lastTxHash: lastTx.tx_hash,
    });
  } else {
    deleteCheckpoint.run({ $chainId: chainId });
  }
}

console.log(
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      dbPath,
      filters: { chainId, startBlock, endBlock, txHash, limit },
      incremental: useIncremental,
      fullBuild,
      checkpoint: checkpoint ?? null,
      txs: txRows.length,
      processed,
    },
    null,
    2,
  ),
);

db.close();
