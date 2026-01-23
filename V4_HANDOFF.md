# V4.1 Handoff: Rolling Chunks + Pattern-Based Analysis

## Current State (V4.0 Complete)

### What's Implemented

1. **Total depth metric** (`total_depth` column in `dfg_tx_deps`)
   - Formula: `chain_depth + max_upstream_intra_depth + current_tx_intra_depth`
   - Represents full critical path including all FHE operations

2. **Horizon filtering** (block range)
   - UI: Dropdown with "All time", "Latest block", "Last N blocks"
   - API: `startBlock` and `endBlock` params on `/dfg/stats`
   - Uses `maxDepsBlock` (latest block with dependency data) to avoid querying empty ranges

3. **Depth mode toggle**
   - UI: "Tx hops" vs "Total ops" buttons
   - API: `depthMode=inter|total` param
   - Histogram switches between `chainDepthDistribution` and `totalDepthDistribution`

4. **Side-by-side depth comparison**
   - Shows both `maxChainDepth` and `maxTotalDepth` simultaneously

5. **Auto-reload on filter change**
   - Changing horizon or depth mode auto-fetches new data (after initial load)

### Key Files Modified in V4.0

| File | Changes |
|------|---------|
| `src/app.ts` | Added `total_depth` column + migration |
| `scripts/build-dfg.ts` | Computes `total_depth` during DFG build |
| `scripts/rollup-dfg.ts` | Aggregates `totalDepthDistribution` |
| `src/server.ts` | Block-range params, depth mode, `maxDepsBlock` |
| `ui/src/App.tsx` | Horizon filter, depth toggle, side-by-side display |

---

## V4.1 Requirements

### Goal
Allow TFHE-rs engineers to answer:
1. "How do dependency patterns vary across time?" (rolling chunks)
2. "For a specific contract function, what are the typical dependency chains?" (pattern-based)
3. "For a specific contract function, how do its dependencies vary over time?" (pattern + chunks)

### Option A: Rolling Chunks

**Concept**: Divide blockchain history into non-overlapping chunks of N blocks each, compute stats per chunk.

**Use case**: "Show me parallelism ratio for every 100-block period"

```
Chunk 1: Blocks 0-99      → 72% parallelism, max depth 12
Chunk 2: Blocks 100-199   → 68% parallelism, max depth 15
Chunk 3: Blocks 200-299   → 81% parallelism, max depth 8
...
```

**Implementation approach**:

1. **API endpoint**: `GET /dfg/stats/chunks`
   ```
   ?chainId=1
   &chunkSize=100           # blocks per chunk
   &startBlock=24270000     # optional: start of range
   &endBlock=24283994       # optional: end of range (default: maxDepsBlock)
   &limit=50                # max chunks to return
   ```

2. **Response format**:
   ```typescript
   {
     chainId: number;
     chunkSize: number;
     chunks: Array<{
       startBlock: number;
       endBlock: number;
       totalTxs: number;
       dependentTxs: number;
       independentTxs: number;
       parallelismRatio: number;
       maxChainDepth: number;
       maxTotalDepth: number;
       avgChainDepth: number;
       avgTotalDepth: number;
     }>;
   }
   ```

3. **SQL query** (per chunk):
   ```sql
   SELECT
     COUNT(*) AS totalTxs,
     SUM(CASE WHEN upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
     MAX(chain_depth) AS maxChainDepth,
     MAX(total_depth) AS maxTotalDepth,
     AVG(chain_depth) AS avgChainDepth,
     AVG(total_depth) AS avgTotalDepth
   FROM dfg_tx_deps
   WHERE chain_id = $chainId
     AND block_number >= $chunkStart
     AND block_number < $chunkEnd
   ```

4. **UI**:
   - Table or line chart showing stats over chunks
   - X-axis: chunk index or block range
   - Y-axis: parallelism ratio, max depth, etc.

### Option B: Pattern-Based Filtering

**Concept**: Filter dependency stats by signature hash (contract function pattern).

**Use case**: "For all `transfer()` calls, what's the average dependency chain?"

**Implementation approach**:

1. **Join with `dfg_txs`** to get signature:
   ```sql
   SELECT
     t.signature_hash,
     COUNT(*) AS totalTxs,
     SUM(CASE WHEN d.upstream_txs > 0 THEN 1 ELSE 0 END) AS dependentTxs,
     MAX(d.chain_depth) AS maxChainDepth,
     MAX(d.total_depth) AS maxTotalDepth,
     AVG(d.chain_depth) AS avgChainDepth,
     AVG(d.total_depth) AS avgTotalDepth
   FROM dfg_tx_deps d
   JOIN dfg_txs t ON t.chain_id = d.chain_id AND t.tx_hash = d.tx_hash
   WHERE d.chain_id = $chainId
     AND t.signature_hash = $signatureHash  -- optional filter
   GROUP BY t.signature_hash
   ```

2. **API params** on `/dfg/stats`:
   ```
   ?signatureHash=0xabcd1234   # filter to specific pattern
   ```

3. **New endpoint** for pattern listing: `GET /dfg/stats/by-signature`
   ```typescript
   {
     chainId: number;
     signatures: Array<{
       signatureHash: string;
       txCount: number;
       parallelismRatio: number;
       avgChainDepth: number;
       avgTotalDepth: number;
       maxChainDepth: number;
       maxTotalDepth: number;
     }>;
   }
   ```

4. **UI**:
   - Dropdown to select signature pattern (populated from existing signatures)
   - Stats filtered to that pattern
   - "All patterns" option for unfiltered view

### Combined: Pattern + Chunks

**Use case**: "For `transfer()` calls, how has parallelism changed over time?"

1. **API**: Combine both params
   ```
   GET /dfg/stats/chunks?signatureHash=0xabcd&chunkSize=100
   ```

2. **UI**:
   - Select pattern from dropdown
   - View rolling chunk stats for that pattern
   - Line chart: parallelism ratio over time for selected pattern

---

## Implementation Order

### Phase 1: Rolling Chunks (Option A)
1. Add `GET /dfg/stats/chunks` endpoint in `src/server.ts`
2. Add chunk-based UI component (table or chart)
3. Add chunk size input control

### Phase 2: Pattern-Based (Option B)
1. Add `signatureHash` param to `/dfg/stats`
2. Add `GET /dfg/stats/by-signature` endpoint
3. Add signature dropdown in UI
4. Wire up pattern filter to dependency stats

### Phase 3: Combined
1. Add `signatureHash` param to `/dfg/stats/chunks`
2. UI: pattern dropdown + chunk view together

---

## Database Schema Reference

```sql
-- Current tables used
dfg_tx_deps (
  chain_id, tx_hash, block_number,
  upstream_txs, handle_links,
  chain_depth, total_depth,  -- V4 added total_depth
  updated_at
)

dfg_txs (
  chain_id, tx_hash, block_number,
  node_count, edge_count, depth,
  signature_hash,  -- Used for pattern filtering
  stats_json, updated_at
)
```

---

## UI Mockup

```
┌─────────────────────────────────────────────────────────────────┐
│ Tx dependencies                                             ⓘ  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Pattern: [All signatures ▼]  Chunk size: [100] blocks          │
│                                                                 │
│ View: ( ) Single range  (•) Rolling chunks                      │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Parallelism over time                                       │ │
│ │                                                             │ │
│ │  80% ─┬─────────────────────────────────────────────────    │ │
│ │       │    ╭─╮     ╭──╮                                     │ │
│ │  60% ─┼───╯   ╰───╯    ╰──╮    ╭─╮                          │ │
│ │       │                    ╰──╯   ╰───                      │ │
│ │  40% ─┼─────────────────────────────────────────────────    │ │
│ │       └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────      │ │
│ │       24.27M    24.28M    24.29M    Block number            │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Chunk details (click row to expand)                             │
│ ┌──────────┬──────────┬────────────┬───────────┬─────────────┐  │
│ │ Blocks   │ Txs      │ Parallel % │ Max depth │ Avg depth   │  │
│ ├──────────┼──────────┼────────────┼───────────┼─────────────┤  │
│ │ 24.27M   │ 142      │ 72%        │ 12        │ 2.3         │  │
│ │ 24.28M   │ 89       │ 68%        │ 15        │ 3.1         │  │
│ │ 24.29M   │ 203      │ 81%        │ 8         │ 1.8         │  │
│ └──────────┴──────────┴────────────┴───────────┴─────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Before Resuming

After manual compaction, run:
```bash
# Rebuild DFG data with total_depth populated
DFG_BUILD_FULL=1 bun run dfg:build

# Rebuild rollups
DFG_ROLLUP_FULL=1 bun run dfg:rollup

# Verify data
sqlite3 data/fhevm_stats.sqlite "SELECT MIN(block_number), MAX(block_number), COUNT(*) FROM dfg_tx_deps WHERE chain_id=1"
```

Then start with Phase 1: Rolling Chunks.
