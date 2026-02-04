import { describe, expect, it } from "bun:test";
import { initDatabase } from "../src/app.ts";

const DB_PATH = ":memory:";

describe("initDatabase", () => {
  it("creates required tables", () => {
    const db = initDatabase(DB_PATH);
    const rows = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all() as Array<{
      name: string;
    }>;
    const names = new Set(rows.map((row) => row.name));

    expect(names.has("fhe_events")).toBe(true);
    expect(names.has("checkpoints")).toBe(true);
    expect(names.has("op_buckets")).toBe(true);
    expect(names.has("op_counts")).toBe(true);
    expect(names.has("rollup_checkpoints")).toBe(true);
    expect(names.has("dfg_txs")).toBe(true);
    expect(names.has("dfg_nodes")).toBe(true);
    expect(names.has("dfg_edges")).toBe(true);
    expect(names.has("dfg_inputs")).toBe(true);

    const columns = db.prepare("PRAGMA table_info(fhe_events)").all() as Array<{ name: string }>;
    const columnNames = new Set(columns.map((column) => column.name));
    expect(columnNames.has("result_type")).toBe(true);
    expect(columnNames.has("lhs_type")).toBe(true);
    expect(columnNames.has("rhs_type")).toBe(true);
    expect(columnNames.has("scalar_flag")).toBe(true);
    expect(columnNames.has("result_handle_version")).toBe(true);

    db.close();
  });
});
