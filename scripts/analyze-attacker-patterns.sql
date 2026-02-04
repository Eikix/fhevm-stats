-- Analyze “attacker” entrypoint patterns by grouping tx-level DFG signatures.
-- Params:
--   $chainId
--   $startBlock
--   $endBlock
--   $callerLower (0x… lowercased)
--
-- Notes:
-- - We identify attacker txs by `fhe_events.args_json.caller == $callerLower`.
-- - This stays L1-log-only; tx.from is NOT stored in DB.
--
WITH attacker_txs AS (
  SELECT DISTINCT tx_hash AS txHash
  FROM fhe_events
  WHERE chain_id = $chainId
    AND block_number BETWEEN $startBlock AND $endBlock
    AND lower(json_extract(args_json, '$.caller')) = $callerLower
),
sig_stats AS (
  SELECT
    signature_hash AS signature,
    COUNT(*) AS txs,
    MIN(node_count) AS minNodes,
    MAX(node_count) AS maxNodes,
    AVG(node_count) AS avgNodes,
    MIN(edge_count) AS minEdges,
    MAX(edge_count) AS maxEdges,
    AVG(edge_count) AS avgEdges,
    MIN(depth) AS minDepth,
    MAX(depth) AS maxDepth,
    AVG(depth) AS avgDepth
  FROM dfg_txs
  WHERE chain_id = $chainId
    AND tx_hash IN (SELECT txHash FROM attacker_txs)
  GROUP BY signature_hash
)
SELECT *
FROM sig_stats
ORDER BY txs DESC, maxDepth DESC, maxNodes DESC;

