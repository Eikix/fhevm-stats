import { describe, expect, it } from "bun:test";
import { deriveEventFields, extractHandleMetadata } from "../src/app.ts";

function makeHandle(type: number, version = 1): string {
  const body =
    "00".repeat(30) + type.toString(16).padStart(2, "0") + version.toString(16).padStart(2, "0");
  return `0x${body}`;
}

describe("extractHandleMetadata", () => {
  it("parses handle type and version from byte 30/31", () => {
    const handle = makeHandle(5, 2);
    const meta = extractHandleMetadata(handle);
    expect(meta).toEqual({ type: 5, version: 2 });
  });

  it("returns null for non-bytes32 values", () => {
    const meta = extractHandleMetadata("0x1234");
    expect(meta).toBe(null);
  });
});

describe("deriveEventFields", () => {
  it("derives types for binary ops with encrypted rhs", () => {
    const derived = deriveEventFields("FheAdd", {
      lhs: makeHandle(2),
      rhs: makeHandle(3),
      result: makeHandle(4, 7),
      scalarByte: "0x00",
    });

    expect(derived.lhsType).toBe(2);
    expect(derived.rhsType).toBe(3);
    expect(derived.resultType).toBe(4);
    expect(derived.resultHandleVersion).toBe(7);
    expect(derived.scalarFlag).toBe(0);
  });

  it("skips rhs type when rhs is scalar", () => {
    const derived = deriveEventFields("FheMul", {
      lhs: makeHandle(2),
      rhs: makeHandle(3),
      result: makeHandle(4),
      scalarByte: "0x01",
    });

    expect(derived.lhsType).toBe(2);
    expect(derived.rhsType).toBeUndefined();
    expect(derived.scalarFlag).toBe(1);
  });

  it("derives types for if-then-else", () => {
    const derived = deriveEventFields("FheIfThenElse", {
      control: makeHandle(0),
      ifTrue: makeHandle(2),
      ifFalse: makeHandle(2),
      result: makeHandle(2),
    });

    expect(derived.controlType).toBe(0);
    expect(derived.ifTrueType).toBe(2);
    expect(derived.ifFalseType).toBe(2);
    expect(derived.resultType).toBe(2);
  });
});
