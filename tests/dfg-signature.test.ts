import { describe, expect, test } from "bun:test";
import { computeDfgSignature } from "../src/dfg-signature";

describe("computeDfgSignature", () => {
  test("is invariant to absolute node ids", () => {
    const sig1 = computeDfgSignature(
      [
        { nodeId: 120, op: "Cast" },
        { nodeId: 121, op: "FheAdd" },
        { nodeId: 122, op: "FheMul" },
      ],
      [
        { fromNodeId: 120, toNodeId: 121 },
        { fromNodeId: 121, toNodeId: 122 },
      ],
    );

    const sig2 = computeDfgSignature(
      [
        { nodeId: 5, op: "Cast" },
        { nodeId: 6, op: "FheAdd" },
        { nodeId: 7, op: "FheMul" },
      ],
      [
        { fromNodeId: 5, toNodeId: 6 },
        { fromNodeId: 6, toNodeId: 7 },
      ],
    );

    expect(sig1).toBe(sig2);
  });

  test("changes when edge structure changes", () => {
    const sigChain = computeDfgSignature(
      [
        { nodeId: 10, op: "Cast" },
        { nodeId: 11, op: "FheAdd" },
        { nodeId: 12, op: "FheMul" },
      ],
      [
        { fromNodeId: 10, toNodeId: 11 },
        { fromNodeId: 11, toNodeId: 12 },
      ],
    );

    const sigStar = computeDfgSignature(
      [
        { nodeId: 10, op: "Cast" },
        { nodeId: 11, op: "FheAdd" },
        { nodeId: 12, op: "FheMul" },
      ],
      [
        { fromNodeId: 10, toNodeId: 11 },
        { fromNodeId: 10, toNodeId: 12 },
      ],
    );

    expect(sigChain).not.toBe(sigStar);
  });
});
