import { describe, expect, it } from "bun:test";
import { validateDerivedTypes } from "../src/app.ts";

describe("validateDerivedTypes", () => {
  it("returns null when expected type is missing", () => {
    const result = validateDerivedTypes("Cast", { resultType: 3 });
    expect(result).toBe(null);
  });

  it("flags mismatches for cast-like events", () => {
    const result = validateDerivedTypes("TrivialEncrypt", {
      resultType: 2,
      castToType: 3,
    });
    expect(result).toEqual({ eventName: "TrivialEncrypt", expectedType: 3, actualType: 2 });
  });

  it("accepts matching rand types", () => {
    const result = validateDerivedTypes("FheRand", {
      resultType: 4,
      randType: 4,
    });
    expect(result).toBe(null);
  });
});
