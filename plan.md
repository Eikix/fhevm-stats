# Plan

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
