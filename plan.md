# Plan

Single source of truth for project status + next steps.

## Guiding principle
Prefer the smallest viable solution that minimizes code, moving parts, and maintenance burden.

## Phase 1: Chain listener (MVP)
- Implement a Bun + viem listener:
  - Backfill logs in batches and optionally tail new blocks.
  - Store raw events in SQLite (unique by chain_id + tx_hash + log_index).
  - Derive FHE types from handle metadata (byte 30).
- Provide a simple stats export and a minimal query API.

## Phase 2: Persistent stats DB/ETL (only if needed)
- Add *one* rollup table for bucketed op counts if raw queries are too slow.
- Incrementally update aggregates with a simple checkpoint.
- Keep retention simple; reindex only when required.
- Decision: deferred rollups for now because there are no slow queries yet.
- Implemented minimal bucketed rollup job and tables.

## Phase 3: UI dashboard
- Use the existing minimal API; only expand if UI needs it.

## Phase 4: Visualization & filtering
- Add network filter (default to mainnet, keep sepolia available).
- Add a minimal pie chart for op distribution (percent-based).
- Prefer rollup data when it materially improves UI performance.
- Implemented network filter and pie-chart distribution in UI.

## Phase 5: Accuracy & completeness
- Track ingestion lag and skipped ranges for transparency.
- Add an optional audit script to compare RPC logs vs DB for a range sample.

## Phase 6: V2 DFG-based metrics (transaction granularity)
- Use L1 logs as the source of truth; reconstruct tx-level DFG per transaction.
- Do not rely on host-listener's approximate DFG (it is a hack for missing in-memory state).
- Extend v1 app with DFG storage + UI viewer (no separate system).
- Store full tx DFGs in SQLite and surface them in the UI; optional JSON export.
- Status: implemented (builder, validation, rollups, API, UI).
- DFG reconstruction definition (tx-scoped):
  - Node = one FHEVMExecutor event.
  - Inputs = handles in event args (lhs/rhs/control/etc) or scalars.
  - Output = result handle.
  - Edge from producer to consumer when an input handle matches a prior output handle in the same tx.
  - Inputs without a local producer are external inputs (from prior txs/blocks).
- Minimal post-processing only:
  - Per-op counts.
  - Operand type breakdowns:
    - ciphertext vs scalar vs trivial encrypt (trivial encrypt = cleartext-as-ciphertext).
    - integer bit-widths per op (e.g., div on 64 vs 128).
    - operand pairs per op (ct-ct vs ct-scalar, etc).
  - Optional grouping by canonical DFG "signature" with frequency.
- Avoid heavy pattern mining in v2; allow downstream teams to analyze DFGs.
- DFGs should be deterministic (any optimizations must preserve determinism).
- Coprocessor parity (optional follow-up):
  - Drop VerifyInput nodes.
  - Model scalar inputs for Cast/TrivialEncrypt/FheRand/FheRandBounded (seed/toType/etc).
  - Canonicalize node ordering by output handle for signatures.
  - Ingest ACL events to set is_allowed and prune like coprocessor finalize().
- Tx dependency tracking (DFG-level, handle-based):
  - Per-tx deps: `dfg_tx_deps` (upstream_txs + handle_links).
  - Handle producer map: `dfg_handle_producers`.
  - Rollup deps to `dfg_dep_rollups` during `dfg:rollup` (incremental).
  - UI consumes deps from rollup (avoid heavy API queries).

## Phase 6.5: Optional correlation (if needed)
- Tx-level co-occurrence is optional and mostly for exploratory UI.
- Prefer support/tx counts over lift/PMI on sparse networks.
- Call-level correlation requires traces (defer until a trace RPC is available).
- Status: removed from v2 (not planned).

## Phase 6.6: Rollup hardening (nice-to-have)
- Make rollup batches atomic: rollup writes + checkpoint update in a single transaction.
- Add retry/backoff around block timestamp fetch to handle transient RPC failures.
- DFG rollup incremental:
  - Track last processed (block, tx_hash) in `dfg_rollup_checkpoints`.
  - Support `DFG_ROLLUP_FULL=1` to rebuild from scratch.

## Phase 7: Deployment
- API + ingestor on a small VM with persistent disk (SQLite).
- UI as static build on a CDN host.
- VM options: Oracle Always Free (best when it works), Google Cloud e2-micro (tight), Fly.io (paid volumes), Hetzner/DO (cheap paid).
- UI options: Cloudflare Pages (generous), Vercel (DX), Netlify (solid), GitHub Pages (simple).

## Phase 8: V3 Roadmap (DFG Analytics + Self-Contained “Done”)

### Track A — DFG Analytics (lightweight)
- k-hop neighborhood hashes (no subgraph isomorphism).
- short op-sequence windows along topo paths.
- rollup counts per chain; optional signature enrichment.
- UX: multi-sample picker for a signature; export PNG/SVG/JSON; optional edge labels on hover.
- performance: indexes for signature lookups + rollups, query limits/timeouts.

### Track B — Self-Contained “Done”
- One-command setup (`bun run setup`):
  - install deps (root + ui)
  - migrate DB
  - optional sample data bootstrap
  - build UI assets
  - smoke check (API + UI)
- Docker + Compose (`docker-compose.yml` for API + UI + volume).
- `.env.example` and “Operational” lifecycle docs.
- Existing quick demo: `bun run share:ngrok` / `bun run share:ngrok:live`.
  - Live mode runs stream + DFG/ops rollups on an interval; supports `--skip-rollups`.

### Track C — Stabilization
- CI: `bun run test` + `bun run check`, minimal API smoke checks.
- Demo data: small sample SQLite or scripted seed.
- Release checklist: migrate → ingest → dfg:build → dfg:rollup → dfg:validate → UI build.

### Sequencing
1) Track B (self-contained)
2) Track A (analytics)
3) Track C (stabilization)
