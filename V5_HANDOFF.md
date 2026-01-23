# V5 Handoff: Rolling Window Dependency Visualization

## What Was Implemented

### Overview
Added rolling window dependency visualization that truncates depth calculations to a configurable lookback window (N blocks). This matches the batch processing model where only recent blocks matter for dependency analysis.

### Key Insight
- **Inter-tx depth**: Gets truncated by window (dependencies outside window are cut)
- **Intra-tx depth**: Always fully counted (same block = always in window)
- Truncation is automatic: for tx T at block B with window N, only follow upstream txs where `upstream_block >= B - N`

---

## Changes Made

### 1. New API Endpoint: `/dfg/stats/window`

**File**: `src/server.ts`

**Parameters**:
- `chainId` (required)
- `lookbackBlocks` (default: 50) - window size in blocks
- `signatureHash` (optional) - filter to specific pattern
- `topLimit` (default: 10) - number of top depth txs to return

**Response**:
```typescript
{
  chainId: number;
  lookbackBlocks: number;
  signatureHash?: string;
  stats: {
    totalTxs: number;
    dependentTxs: number;      // truncated deps > 0
    independentTxs: number;    // truncated deps = 0
    parallelismRatio: number;
    maxTruncatedDepth: number;
    avgTruncatedDepth: number;
    truncatedDepthDistribution: Record<number, number>;
  };
  topDepthTxs: Array<{
    txHash: string;
    blockNumber: number;
    truncatedDepth: number;
    fullChainDepth: number;
  }>;
}
```

**Algorithm**:
1. Load all txs with their blocks from `dfg_txs`
2. Load external inputs from `dfg_inputs` (kind = 'external')
3. Load non-trivial handle producers from `dfg_handle_producers`
4. For each tx at block B:
   - Find upstream txs where `producer_block >= B - lookbackBlocks`
   - `truncated_depth = max(upstream_truncated_depths) + 1` (or 0 if none)
5. Aggregate stats and return top depth txs

### 2. Modified `/dfg/tx` Endpoint

**File**: `src/server.ts`

**New Parameter**:
- `lookbackBlocks` (optional) - when provided, returns cut edges

**New Response Fields**:
```typescript
{
  // ...existing fields...
  cutEdges?: Array<{
    handle: string;
    producerTxHash: string;
    producerBlock: number;
    windowStart: number;  // txBlock - lookbackBlocks
  }>;
  lookbackBlocks?: number;
}
```

### 3. UI: Rolling Window Section

**File**: `ui/src/App.tsx`

Replaced the disabled `{false && ...}` section (previously at line 1612) with:

- **Window selector buttons**: [10, 20, 50, 100, 200] blocks
- **Stats cards**: Max/avg truncated depth, parallelism ratio, dependent txs
- **Depth histogram**: Visual distribution of truncated depths
- **Top depth txs table**: Clickable rows to view DFG with cut edges
- **Pattern browser**: Existing patterns list (unchanged)

**New State Variables**:
```typescript
const [windowLookback, setWindowLookback] = useState(50);
const [windowStats, setWindowStats] = useState<WindowStatsResponse | null>(null);
const [windowStatus, setWindowStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");
const [windowError, setWindowError] = useState<string | null>(null);
const [dfgLookback, setDfgLookback] = useState<number | null>(null);
```

### 4. DFG Graph Cut Edge Rendering

**File**: `ui/src/App.tsx`

When `dfgLookback` is set and `cutEdges` exist:
- Dashed red lines rendered for cut edges
- Labels show "CUT (block N)" indicating where dependency was truncated
- Badge in viewer header shows active lookback setting
- External inputs section highlights cut edges in red

---

## Files Modified

| File | Changes |
|------|---------|
| `src/server.ts` | Added `handleDfgStatsWindow()`, modified `handleDfgTx()` for cut edges, added route |
| `ui/src/App.tsx` | New types, state, `loadWindowStats()`, rolling window UI, cut edge rendering |

---

## Testing

### API Tests
```bash
# Test window stats with different lookback values
curl "localhost:4310/dfg/stats/window?chainId=9000&lookbackBlocks=10"
curl "localhost:4310/dfg/stats/window?chainId=9000&lookbackBlocks=100"
# Expect: smaller window → lower truncated depths

# Test cut edges on specific tx
curl "localhost:4310/dfg/tx?chainId=9000&txHash=0x...&lookbackBlocks=50"
```

### UI Tests
1. Load dashboard, scroll to "DFG stats" section
2. Find "Rolling window dependencies" panel
3. Select window size (10, 20, 50, 100, 200 blocks) → stats auto-load
4. Compare different window sizes → verify depths decrease with smaller windows
5. Click a tx in "Top depth transactions" → verify DFG viewer opens
6. Verify cut edges shown as dashed red lines (if any)
7. Verify graph view is always visible (no toggle needed)
8. Verify zooming in graph doesn't scroll the page

### Verification
- `topDepthTxs` includes both `truncatedDepth` and `fullChainDepth`
- Verify `truncatedDepth <= fullChainDepth` always

---

## Data Flow

```
User sets lookback=50
        ↓
GET /dfg/stats/window?lookbackBlocks=50
        ↓
Server: Load txs + external handles + producers
        ↓
Server: Compute truncated depths (bottom-up by block)
        ↓
Server: Aggregate stats + return top txs
        ↓
UI: Show stats, histogram, pattern list
        ↓
User clicks tx → GET /dfg/tx?lookbackBlocks=50
        ↓
UI: Render DFG with cut edges marked
```

---

## No Rebuild Required

This feature uses existing tables:
- `dfg_txs` - transaction metadata
- `dfg_inputs` - external inputs (kind = 'external')
- `dfg_handle_producers` - handle → producer tx mapping
- `dfg_tx_deps` - for `fullChainDepth` comparison

If you already have DFG data from `bun run dfg:build`, no reconstruction needed.

---

## Running Locally

```bash
# Terminal 1: Start server
bun run serve

# Terminal 2: Start UI
cd ui && bun run dev

# Or use the unified ngrok command
bun run share:ngrok:live
```

---

## V5.1 Updates (This Session)

### UI Improvements

1. **Graph View Wheel Scroll Fix**
   - Fixed page scrolling when zooming in graph view
   - Uses native event listener with `{ passive: false }` instead of React synthetic event
   - Added `dfgSvgRef` to reference SVG element

2. **Removed Recent DFGs Sidebar**
   - Removed the left sidebar showing recent DFGs list
   - Search bar now inline at top
   - Graph view takes full width

3. **Graph View Always On**
   - Removed the "Graph On/Off" toggle button
   - Graph is now always visible when a transaction is selected
   - Removed `setDfgGraphView` setter (state is always `true`)

4. **Auto-Compute on Window Selection**
   - Removed "Compute" button from rolling window section
   - Stats auto-load when window size (10/20/50/100/200) is clicked
   - Fixed useEffect dependencies to include `chainId` and `loadWindowStats`

5. **Histogram Always Visible**
   - Depth distribution histogram section always shows
   - Displays fallback message when no data instead of hiding

6. **Info Tooltips Added**
   - **Ops distribution**: Explains raw FHE events from fhe_events table
   - **Op types**: Explains Result/LHS/RHS role definitions
   - **Top ops**: Explains DFG graph ops vs raw events
   - **Input kinds**: Explains Ciphertext/Trivial/External/Scalar
   - **Operand pairs**: Explains LHS × RHS combinations

### Code Cleanup

Removed unused state and functions:
- `dfgError`, `dfgDeps`, `dfgDepsStatus`, `dfgDepsError`, `depthMode`
- `horizonSize`, `horizonData`, `horizonStatus`, `horizonError`
- `loadHorizon` function
- `HorizonSummary` and `HorizonResponse` types

### Pre-commit Hooks

Set up husky + lint-staged for pre-commit checks:

```bash
# .husky/pre-commit
#!/bin/sh
cd ui && bunx tsc --noEmit || exit 1
bunx lint-staged || exit 1
```

**lint-staged config** (in `ui/package.json`):
```json
"lint-staged": {
  "*.{ts,tsx}": ["prettier --write", "eslint --fix"],
  "*.{json,css,md}": ["prettier --write"]
}
```

---

## V5.2 Updates (This Session)

### Major: Combined Depth (True Critical Path)

**Previously**: Only showed inter-tx depth (hop count between transactions).

**Now**: Shows **combined depth** = accumulated intra-tx depths along the dependency chain.

```
Example: A → B → C (A depends on B, B depends on C)
Tx C: intra=5, combined=5
Tx B: intra=3, combined=5+3=8
Tx A: intra=7, combined=8+7=15

True critical path = 15 sequential FHE ops
```

**Algorithm** (`src/server.ts`):
```javascript
combined_depth(tx) = max(upstream_combined_depths) + tx.intra_depth
```

For each tx, recursively traverse its dependency chain within the window, accumulating intra-tx depths.

### Critical Bug Fixes

1. **Future Block Bug**
   - **Issue**: Data has anomalous entries where producer block > consumer block
   - **Effect**: Depths were wildly inflated (650 for 1-block window!)
   - **Fix**: Added `block > windowEnd` check to exclude future blocks
   ```javascript
   if (block < windowStart || block > windowEnd) // truncate
   ```

2. **Window Calculation Off-by-One**
   - **Issue**: `windowStart = block - lookbackBlocks` meant 1-block window included 2 blocks
   - **Fix**: `windowStart = block - lookbackBlocks + 1`
   - **Now**: 1-block = only same block, 5-block = current + 4 previous

3. **Infinite Loop in useEffect**
   - **Issue**: `windowStatus` in useCallback dependencies caused loop
   - **Fix**: Inlined fetch logic in useEffect, removed function dependency

### UI Improvements

1. **Window Sizes**: Changed from `[10,20,50,100,200]` to `[1,3,5,10,20,50,100,200]`

2. **Cut Edge Styling**: Red → Violet (less alarming, more informational)
   - Badge: "Window: N blocks" instead of "Cut edges"
   - Labels: "← blk N" instead of "CUT (blk N)"
   - Nodes with cut edges: violet border + small dot indicator

3. **Removed Patterns Section**: Was showing full-chain stats, not window-truncated stats

4. **Histogram Fixes**:
   - Fixed CSS percentage height bug (now uses pixel heights)
   - Added Y-axis labels showing actual counts
   - **Stacked bars**: Bottom (darker teal) = inter-tx, Top (lighter) = intra-tx
   - Legend added to explain colors

5. **Stats Cards**: Now shows 5 columns:
   - Max combined (inter + intra)
   - Avg combined (with breakdown)
   - Max inter-tx
   - Parallelism %
   - Dependent txs

6. **Top Depth Transactions**: Shows `combined: 42 (35+7)` format

### API Response Changes

```typescript
stats: {
  // existing...
  maxCombinedDepth: number;      // NEW
  avgCombinedDepth: number;      // NEW
  avgIntraDepth: number;         // NEW
  combinedDepthDistribution: Record<number, { count: number; avgIntra: number }>; // NEW
};
topDepthTxs: Array<{
  // existing...
  intraTxDepth: number;          // NEW
  combinedDepth: number;         // NEW
}>;
```

### Code Cleanup

Removed unused code:
- `useCallback` import (no longer used)
- Pattern-related types: `SignatureDepStats`, `BySignatureResponse`, `PatternExampleTx`, `PatternDetailResponse`
- Pattern-related state: `patternSortBy`, `patterns`, `patternsStatus`, `expandedPattern`, `patternDetail`, `patternDetailStatus`
- Pattern-related functions: `loadPatterns`, `loadPatternDetail`

### Data Anomaly Discovered

Some `dfg_handle_producers` entries have `block_number` GREATER than the consumer tx's block. This is invalid (a tx can't depend on a future tx) but exists in the data. The fix excludes these from depth calculations.

```sql
-- Example anomaly found:
SELECT consumer_block, producer_block FROM ...
-- 24276365 | 24276579  -- producer 214 blocks in FUTURE!
```

---

## Known Limitations

1. **Performance**: Window stats computed on-the-fly with recursive traversal per tx. O(n * chain_length).
2. **Data anomaly**: Future-block dependencies exist and are now excluded, but root cause unknown.
3. **Memory**: Loads all txs into memory for depth computation.

---

## Future Improvements

1. **Caching**: Pre-compute window stats for common window sizes
2. **Incremental**: Only recompute affected txs when new blocks arrive
3. **Data cleanup**: Investigate and fix future-block anomalies in `dfg_handle_producers`
4. **Performance**: Consider memoization for depth computation across txs with overlapping chains
