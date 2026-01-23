# V3 Handoff (manual compression)

## Current Session - Chain Depth & Parallelism Metrics (c8ef85e)

### Goal
Add metrics to distinguish parallel vs pipeline transaction patterns, filtering out trivial encrypts to avoid false dependencies.

### New Metrics
- **chain_depth**: Longest path through non-trivial handle dependencies (0 = independent/parallel)
- **parallelism_ratio**: `independentTxs / totalTxs`
- **chain_depth_distribution**: Histogram of chain lengths

### Schema Changes (`src/app.ts`)
- Added `is_trivial INTEGER` column to `dfg_handle_producers` (marks TrivialEncrypt outputs)
- Added `chain_depth INTEGER` column to `dfg_tx_deps` (max upstream non-trivial depth + 1)
- Added `dfg_build_checkpoints` table for incremental builds
- Added `ensureDfgColumns()` for backwards-compatible migration

### Build Script Changes (`scripts/build-dfg.ts`)
- **Now incremental by default** - uses checkpoint table to skip already-processed txs
- Tracks `trivialHandles` from TrivialEncrypt operations
- Computes `chain_depth` as max(non-trivial upstream chain_depth) + 1
- `DFG_BUILD_FULL=1` env var forces full rebuild

### Rollup Changes (`scripts/rollup-dfg.ts`)
- Extended `DepRollupState` with `maxChainDepth` and `chainDepthDistribution`
- Aggregates chain depth distribution in both full and incremental modes

### API Changes (`src/server.ts`)
- Extended `DepStats` type with `parallelismRatio`, `maxChainDepth`, `chainDepthDistribution`
- `normalizeDepStats()` computes `parallelismRatio`
- Added `/dfg/export` endpoint for tfhe-rs team

### UI Changes (`ui/src/App.tsx`)
- Added parallelism ratio card (percentage)
- Added max chain depth card
- Added chain depth distribution histogram (CSS bars)
- Reorganized dependency cards into 3+4 layout

### Commands
```bash
# Incremental build (default) - only new txs since checkpoint
bun run dfg:build

# Full rebuild - reprocess all, reset checkpoint
DFG_BUILD_FULL=1 bun run dfg:build

# Full rollup rebuild (needed once for historical chain_depth data)
DFG_ROLLUP_FULL=1 bun run dfg:rollup
```

### First-Time Setup for New Metrics
```bash
DFG_BUILD_FULL=1 bun run dfg:build
DFG_ROLLUP_FULL=1 bun run dfg:rollup
```

### Verification
```sql
SELECT is_trivial, COUNT(*) FROM dfg_handle_producers GROUP BY is_trivial;
SELECT chain_depth, COUNT(*) FROM dfg_tx_deps GROUP BY chain_depth;
SELECT * FROM dfg_build_checkpoints;
```

### API Endpoints
```bash
curl "localhost:3737/dfg/stats?chainId=9000&includeDeps=1"
curl "localhost:3737/dfg/export?chainId=9000"
```

### Status
- Committed and pushed: `c8ef85e`
- All precommit checks pass
- Need full rebuild once to populate historical `is_trivial` and `chain_depth`

---

## What changed since V2

### Commits pushed
```
9a3d1d6 Remove lint ignores and fix React hooks patterns
45dfeb1 Add incremental tx-dependency pipeline and DFG tooling
```

### New features (45dfeb1)
- Incremental tx-dependency pipeline with new tables:
  - `dfg_handle_producers` - maps handles to producing tx
  - `dfg_tx_deps` - per-tx dependency stats
  - `dfg_dep_rollups` - aggregated dependency metrics
- `dfg:build` writes per-tx dependency stats + handle→producer map
- `dfg:rollup` aggregates deps incrementally (`DFG_ROLLUP_FULL=1` for full rebuild)
- `/dfg/stats` reads from rollup tables (fast); heavy query only via `includeDeps=1`
- Graph edges show FHE type labels in UI
- Ops rollup in ngrok live loop with `--skip-rollups` flag
- Precommit hook + setup script
- Migration script for new schema

### Code quality fixes (9a3d1d6)
- Removed all lint ignore comments (`eslint-disable`, `biome-ignore`)
- No `any` types or `!` non-null assertions
- Downgraded `eslint-plugin-react-hooks` to v5.x (keeps `exhaustive-deps`, removes v7 strict rules)
- Used ref pattern for `dfgSelection` to avoid infinite loop
- Added `cacheBust` param to ingestion fetch

## Key commands
```bash
bun run migrate          # create new tables
bun run dfg:build        # build DFG + dependency data
bun run dfg:rollup       # aggregate stats incrementally
bun run rollup:ops:all   # ops aggregation
bun run share:ngrok:live # live sharing (or -- --skip-rollups)
scripts/setup-githooks.sh # enable precommit hook
bun run precommit        # full check: typecheck + lint + format + test + ui build
```

## Project structure
- **Backend**: Bun + SQLite (`src/app.ts`, `src/server.ts`)
- **Scripts**: `scripts/*.ts` for data processing
- **UI**: React + Vite in `ui/` folder
- **Config**: `biome.json` (lint/format), `ui/eslint.config.js` (React lint)

## Lint setup
- Root: Biome for TypeScript (lint + format)
- UI: ESLint with `eslint-plugin-react-hooks@5.x` for React rules
- Biome ignores `ui/dist` via `files.includes` pattern

## Precommit hook
- Location: `.githooks/pre-commit`
- Enable: `scripts/setup-githooks.sh`
- Runs: `bun run check && bun test && (cd ui && bun run lint && bun run build)`

## Database tables (key ones)
- `fhe_events` - raw L1 events
- `dfg_txs`, `dfg_nodes`, `dfg_edges`, `dfg_inputs` - DFG structure
- `dfg_handle_producers`, `dfg_tx_deps`, `dfg_dep_rollups` - dependency tracking
- `op_buckets` - ops rollup
- `checkpoints` - ingestion progress

## Notes
- Dependency stats now served from rollup; no more >10s API call
- `share:ngrok:live` pauses stream while rollups run (expected)
- Branch is up to date with `origin/main`
