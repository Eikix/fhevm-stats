import { initDatabase, deriveEventFields } from "../src/app.ts";

const DEFAULT_DB_PATH = "data/fhevm_stats.sqlite";
const dbPath = Bun.env.DB_PATH ?? DEFAULT_DB_PATH;
const limit = parseNumber(Bun.env.BACKFILL_LIMIT);

function parseNumber(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

const db = initDatabase(dbPath);

const whereClause =
  "args_json IS NOT NULL AND (result_type IS NULL OR lhs_type IS NULL OR rhs_type IS NULL OR control_type IS NULL OR if_true_type IS NULL OR if_false_type IS NULL OR input_type IS NULL OR cast_to_type IS NULL OR rand_type IS NULL OR scalar_flag IS NULL OR result_handle_version IS NULL)";

const rows = db
  .prepare(
    `SELECT id, event_name AS eventName, args_json AS argsJson
     FROM fhe_events
     WHERE ${whereClause}
     ${limit ? "LIMIT $limit" : ""}`,
  )
  .all(limit ? { limit } : {}) as Array<{ id: number; eventName: string; argsJson: string }>;

const updateStmt = db.prepare(
  `UPDATE fhe_events SET
    lhs_type = COALESCE(lhs_type, $lhs_type),
    rhs_type = COALESCE(rhs_type, $rhs_type),
    result_type = COALESCE(result_type, $result_type),
    control_type = COALESCE(control_type, $control_type),
    if_true_type = COALESCE(if_true_type, $if_true_type),
    if_false_type = COALESCE(if_false_type, $if_false_type),
    input_type = COALESCE(input_type, $input_type),
    cast_to_type = COALESCE(cast_to_type, $cast_to_type),
    rand_type = COALESCE(rand_type, $rand_type),
    scalar_flag = COALESCE(scalar_flag, $scalar_flag),
    result_handle_version = COALESCE(result_handle_version, $result_handle_version)
   WHERE id = $id`,
);

let updated = 0;
for (const row of rows) {
  const args = JSON.parse(row.argsJson) as Record<string, unknown>;
  const derived = deriveEventFields(row.eventName, args);
  updateStmt.run({
    id: row.id,
    lhs_type: derived.lhsType ?? null,
    rhs_type: derived.rhsType ?? null,
    result_type: derived.resultType ?? null,
    control_type: derived.controlType ?? null,
    if_true_type: derived.ifTrueType ?? null,
    if_false_type: derived.ifFalseType ?? null,
    input_type: derived.inputType ?? null,
    cast_to_type: derived.castToType ?? null,
    rand_type: derived.randType ?? null,
    scalar_flag: derived.scalarFlag ?? null,
    result_handle_version: derived.resultHandleVersion ?? null,
  });
  updated += 1;
}

console.log(
  JSON.stringify(
    {
      dbPath,
      scanned: rows.length,
      updated,
      limit: limit ?? null,
    },
    null,
    2,
  ),
);
