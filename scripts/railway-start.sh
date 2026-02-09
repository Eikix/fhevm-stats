#!/usr/bin/env bash
set -euo pipefail

export HTTP_PORT="${HTTP_PORT:-${PORT:-4310}}"
export DB_PATH="${DB_PATH:-/data/fhevm_stats.sqlite}"
export CATCHUP_MAX_BLOCKS="${CATCHUP_MAX_BLOCKS:-0}"

RUN_MIGRATE="${RUN_MIGRATE:-1}"
RUN_ROLLUP_ON_START="${RUN_ROLLUP_ON_START:-1}"
ROLLUP_INTERVAL_SECONDS="${ROLLUP_INTERVAL_SECONDS:-600}"
SKIP_DFG_BUILD="${SKIP_DFG_BUILD:-0}"
STREAM_RESTART_DELAY_SECONDS="${STREAM_RESTART_DELAY_SECONDS:-5}"

run_rollups_once() {
  bun run rollup:ops:all || true
  if [ "$SKIP_DFG_BUILD" != "1" ]; then
    bun run dfg:build || true
    bun run dfg:rollup || true
    bun run backfill:dfg-stats || true
  fi
}

if [ "$RUN_MIGRATE" = "1" ]; then
  bun run migrate
fi

(
  while true; do
    set +e
    bun run stream
    exit_code=$?
    set -e
    echo "stream exited with code ${exit_code}; restarting in ${STREAM_RESTART_DELAY_SECONDS}s"
    sleep "$STREAM_RESTART_DELAY_SECONDS"
  done
) &
STREAM_PID=$!

if [ "$RUN_ROLLUP_ON_START" = "1" ]; then
  (
    run_rollups_once
  ) &
  INITIAL_ROLLUP_PID=$!
fi

if [ "$ROLLUP_INTERVAL_SECONDS" -gt 0 ]; then
  (
    sleep 60
    while true; do
      run_rollups_once
      sleep "$ROLLUP_INTERVAL_SECONDS"
    done
  ) &
  ROLLUP_PID=$!
fi

trap 'kill -TERM ${STREAM_PID:-} ${ROLLUP_PID:-} ${INITIAL_ROLLUP_PID:-} 2>/dev/null || true' INT TERM

bun run serve
