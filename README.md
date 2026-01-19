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
7) Backfill derived types:
   - `bun run backfill:types`
8) Rollup op buckets:
   - `bun run rollup:ops`

## API
- `GET /health`
- `GET /stats/summary?chainId=&startBlock=&endBlock=`
- `GET /stats/ops?chainId=&startBlock=&endBlock=&eventName=`
- `GET /stats/buckets?chainId=&startBlock=&endBlock=&bucketSize=`
- `GET /stats/types?chainId=&startBlock=&endBlock=&role=`
- `GET /stats/db`

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
- DB_PATH
- MODE (backfill | stream | both)

Multi-network notes:
- Use `NETWORK=sepolia,mainnet` and set `SEPOLIA_ETH_RPC_URL` + `MAINNET_ETH_RPC_URL`.
- Do not set `RPC_URL`, `CHAIN_ID`, or `FHEVM_EXECUTOR_ADDRESS` when using multiple networks.

## Status
Initial scaffolding only. See plan.md for phases.
