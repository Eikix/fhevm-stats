# fhevm-stats

Lightweight FHEVM host-chain stats collector.

## Goals
- Listen to FHEVMExecutor events on a configurable EVM network.
- Store raw events in SQLite for long-lived, queryable data.
- Export usage stats (op counts, time buckets, simple patterns) for reporting.

## Planned stack
- Runtime: Bun
- EVM client: viem
- Storage: SQLite (Bun built-in)

## Usage
1) Copy `.env.example` to `.env` and fill in values.
2) Run a mode:
   - `bun run backfill`
   - `bun run stream`
   - `bun run both`
3) Export stats:
   - `bun run stats`
4) Smoke test (small range):
   - set `SMOKE_START_BLOCK` and `SMOKE_END_BLOCK`
   - `bun run smoke`
5) Tests and checks:
   - `bun run test`
   - `bun run check`
6) Serve API:
   - `bun run serve`
   - `cd ui && bun install` (first time, for UI deps)
   - Quick share (ngrok):
     - `bun run share:ngrok`
     - `bun run share:ngrok:live -- --interval 10m`
     - First-time ngrok auth:
       - `ngrok config add-authtoken <YOUR_TOKEN>`
7) Backfill derived types:
   - `bun run backfill:types`
8) Rollup op buckets:
   - `bun run rollup:ops`
   - `bun run rollup:ops:all` (mainnet + sepolia; uses defaults if RPC envs are unset)
9) Build tx-level DFGs:
   - `bun run dfg:build`
   - If `CHAIN_ID` is unset, the builder runs for all chains in the DB.
10) Migrate DB schema only:
   - `bun run migrate`
11) Validate DFG integrity (defaults to mainnet + sepolia):
   - `bun run dfg:validate`
   - Optional: `CHAIN_ID=1 MAX_TX=1000 bun run dfg:validate`
12) Roll up DFG stats (aggregate counts):
   - `bun run dfg:rollup`
   - Incremental by default (only new DFG txs since last checkpoint).
   - Force full rebuild: `DFG_ROLLUP_FULL=1 bun run dfg:rollup`
   - Dependency stats are precomputed during rollup; skip with `DFG_DEPS_ROLLUP=0`.

## API
- `GET /health`
- `GET /stats/summary?chainId=&startBlock=&endBlock=`
- `GET /stats/ops?chainId=&startBlock=&endBlock=&eventName=`
- `GET /stats/buckets?chainId=&startBlock=&endBlock=&bucketSize=`
- `GET /stats/types?chainId=&startBlock=&endBlock=&role=`
- `GET /stats/op-types?chainId=&startBlock=&endBlock=&eventName=&role=&includeScalar=`
- `GET /stats/ingestion?chainId=`
- `GET /stats/db`
- `GET /dfg/txs?chainId=&limit=&offset=&minNodes=`
- `GET /dfg/tx?chainId=&txHash=`
- `GET /dfg/signatures?chainId=&limit=&offset=`
- `GET /dfg/stats?chainId=`
- `GET /dfg/rollup?chainId=`

## DFG vs FHEVM scheduler DFG (what matches + what differs)
This project reconstructs transaction-level DFGs directly from L1 logs so it can run without the coprocessor. The goal is to match the scheduler's graph structure as closely as possible while staying L1-only.

### What matches (structure)
- **Nodes**: one node per TFHE computation event (same op + output handle semantics as the scheduler).
- **Edges**: an edge exists only when an input handle was produced earlier in the same tx. Scalars do not create edges.
- **Input roles**: binary ops use `lhs/rhs`, unary use `ct`, `FheIfThenElse` uses `control/ifTrue/ifFalse`.

### Known differences (intentional)
- **No ACL pruning**: the scheduler prunes nodes based on ACL `is_allowed` (requires ACL events). We do not ingest ACL, so we keep a superset of nodes.
- **Signature ordering**: scheduler sorts ops by output handle before building signatures; we use log order. This affects signature hashes, not edge structure.

### Why this approach
- L1 logs are the source of truth and are always available.
- It keeps the pipeline self-contained and avoids coprocessor dependencies.
- We can still analyze graph structure and usage patterns with minimal post-processing.

## FHE type derivation
FHE types are derived from handle metadata. The type byte is stored at index 30
(see `_typeOf` in `fhevm/host-contracts/contracts/FHEVMExecutor.sol`).
During ingestion, we log a warning if the derived result type conflicts with
explicit event fields (`VerifyInput`, `Cast`, `TrivialEncrypt`, `FheRand*`).

## Configuration
- NETWORK (sepolia | devnet | mainnet | anvil | hardhat | custom; comma-separated for multi; defaults to sepolia,mainnet)
- RPC_URL (single-network only)
- CHAIN_ID (optional; auto-detected from RPC or network defaults; single-network only)
- FHEVM_EXECUTOR_ADDRESS (optional for sepolia/mainnet)
- SEPOLIA_ETH_RPC_URL / MAINNET_ETH_RPC_URL / ANVIL_RPC_URL (optional fallbacks)
- START_BLOCK / END_BLOCK
- CONFIRMATIONS
- BATCH_SIZE
- CATCHUP_MAX_BLOCKS
- ROLLUP_BLOCK_FETCH_DELAY_MS (rollup:ops; per-block RPC delay, default 200ms)
- DB_PATH
- MODE (backfill | stream | both)

Multi-network notes:
- Use `NETWORK=sepolia,mainnet` and set `SEPOLIA_ETH_RPC_URL` + `MAINNET_ETH_RPC_URL`.
- Do not set `RPC_URL`, `CHAIN_ID`, or `FHEVM_EXECUTOR_ADDRESS` when using multiple networks.

## Status
Initial scaffolding only. See plan.md for phases.
