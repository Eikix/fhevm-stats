# V2 Handoff (DFG Metrics)

## Goal
Move from event-level stats (v1) to tx-level DFG analytics (v2) using L1 logs as source of truth.
Keep the system lean and deterministic, with minimal post-processing and a UI viewer.

## Why DFG (meeting notes distilled)
- Tx-level DFG is the most valuable granularity for optimization insights.
- Host-listener’s approximate DFG is a hack (missing in-memory state) — do not use it.
- Scheduler/tfhe-worker uses real DFGs; we can reconstruct equivalent graphs from L1 logs.
- Avoid heavy pattern mining; downstream teams can analyze raw DFG dumps.
- Trivial encryption (cleartext-as-ciphertext) matters for performance analysis.

## DFG definition (tx-scoped)
- Node = one FHEVMExecutor event (FheAdd, FheMul, FheIfThenElse, etc.).
- Inputs = handles from event args (lhs/rhs/control/etc) or scalars.
- Output = result handle.
- Edge: input handle matches an output handle produced earlier in the same tx.
- Inputs without a local producer are external (created in prior txs/blocks).

## Expected outputs
1) SQLite storage for DFGs (primary artifact).
2) UI viewer for DFGs (basic search + tables; graph view optional later).
3) Optional JSON export for sharing.

### Suggested schema
- dfg_txs(tx_hash, chain_id, node_count, edge_count, depth, signature_hash, created_at)
- dfg_nodes(tx_hash, node_id, op, output_handle, scalar_flag, input_count, type_info_json)
- dfg_edges(tx_hash, from_node_id, to_node_id, input_handle)
- dfg_inputs(tx_hash, handle, kind)  // optional: external vs scalar vs trivial encrypt

## Minimal post-processing (per transcript)
- Per-op counts.
- Operand type breakdowns:
  - ciphertext vs scalar vs trivial encrypt (cleartext-as-ciphertext).
  - integer bit-widths per op (e.g., div on 64 vs 128).
  - operand pairs per op (ct-ct vs ct-scalar, etc).
- Optional: canonical DFG signature frequency (same structure/op mix).

## Implementation approach
1) Build a v2 script that:
   - Reads L1 events from existing DB (fhe_events).
   - Groups by tx_hash.
   - Builds DFG nodes/edges using handle dependencies.
   - Writes to SQLite DFG tables.
2) Add API endpoints for DFG lookup (tx_hash, chain_id).
3) UI: add a DFG page/tab showing node/edge tables + metadata.

## Guardrails
- Deterministic reconstruction; no optimizer-specific transforms.
- Use L1 logs only (no coprocessor DB dependency).
- Keep it lean; only add rollups if DFG queries are too slow.

## Current v1 status (context)
- Event ingestion (Bun + viem) into `fhe_events`.
- API + UI; correlation endpoint exists but is optional.
- Rollup table `op_buckets` for faster `stats/ops`.
- Multi-network stream (mainnet + sepolia) into same SQLite.

## Known issues / todo ideas
- Consider rollup batch atomicity (rollup writes + checkpoint in one txn).
- Correlation metrics are noisy on sparse networks; default sort now uses tx_pair_count.

