import { createHash } from "node:crypto";

export type SignatureNode = {
  nodeId: number;
  op: string;
};

export type SignatureEdge = {
  fromNodeId: number;
  toNodeId: number;
};

export function computeDfgSignature(nodes: SignatureNode[], edges: SignatureEdge[]): string {
  // Canonicalize node ids so the signature is invariant to absolute `log_index` values.
  // (Same function call in different txs should map to the same signature.)
  const sortedNodes = [...nodes].sort((a, b) => a.nodeId - b.nodeId);

  const idToIndex = new Map<number, number>();
  const ops = sortedNodes.map((node, index) => {
    idToIndex.set(node.nodeId, index);
    return node.op;
  });

  const canonicalEdges: Array<[number, number]> = [];
  for (const edge of edges) {
    const from = idToIndex.get(edge.fromNodeId);
    const to = idToIndex.get(edge.toNodeId);
    if (from === undefined || to === undefined) continue;
    canonicalEdges.push([from, to]);
  }
  canonicalEdges.sort((a, b) => a[0] - b[0] || a[1] - b[1]);

  const payload = {
    v: 2,
    ops,
    edges: canonicalEdges,
  };

  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}
