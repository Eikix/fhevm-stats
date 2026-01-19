import { describe, expect, it } from "bun:test";
import { loadConfig } from "../src/app.ts";

const baseEnv: Record<string, string | undefined> = {
  NETWORK: "sepolia",
  SEPOLIA_ETH_RPC_URL: "https://example.invalid",
  FHEVM_EXECUTOR_ADDRESS: "0x92C920834Ec8941d2C77D188936E1f7A6f49c127",
};

describe("loadConfig", () => {
  it("uses sepolia defaults when RPC_URL is missing", () => {
    const config = loadConfig({ ...baseEnv });
    expect(config.rpcUrl).toBe("https://example.invalid");
    expect(config.chainId).toBe(11155111);
    expect(config.network).toBe("sepolia");
  });

  it("prefers RPC_URL over network defaults", () => {
    const config = loadConfig({
      ...baseEnv,
      RPC_URL: "http://localhost:1234",
      CHAIN_ID: "999",
    });
    expect(config.rpcUrl).toBe("http://localhost:1234");
    expect(config.chainId).toBe(999);
  });

  it("throws when executor address is missing", () => {
    const env: Record<string, string | undefined> = {
      NETWORK: "custom",
      RPC_URL: "http://localhost:1234",
    };
    delete env.FHEVM_EXECUTOR_ADDRESS;
    expect(() => loadConfig(env)).toThrow("FHEVM_EXECUTOR_ADDRESS is required");
  });
});
