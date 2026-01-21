#!/usr/bin/env bash
set -euo pipefail
set +m

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

wait_for_url() {
  local log_file="$1"
  local name="${2:-}"
  local url=""
  for _ in {1..60}; do
    if [ -f "$log_file" ]; then
      url="$(awk -v name="$name" '
        {
          u=""; n="";
          for (i=1; i<=NF; i++) {
            if ($i ~ /^url=/) { u=substr($i, 5); gsub(/[",]/, "", u); }
            if ($i ~ /^name=/) { n=substr($i, 6); gsub(/[",]/, "", n); }
          }
          if (u != "" && (name == "" || n == name)) { print u; exit; }
        }
      ' "$log_file")"
      if [ -n "$url" ]; then
        echo "$url"
        return 0
      fi
    fi
    sleep 0.5
  done
  return 1
}

print_log_tail() {
  local log_file="$1"
  if [ -f "$log_file" ]; then
    echo "---- log tail: $log_file ----"
    tail -n 100 "$log_file"
    echo "---- end log tail ----"
  fi
}

require_cmd bun
require_cmd ngrok

LIVE=false
SKIP_ROLLUPS=false
INTERVAL="10m"
FORCE=false
DFG_INITIAL_DELAY_SECONDS=30
STREAM_RESTART_DELAY_SECONDS=5

print_usage() {
  cat <<'EOF'
Usage: share-ngrok.sh [--live] [--interval 10m] [--force] [--skip-rollups]

Options:
  --live           Start stream + periodic dfg:build/dfg:rollup loop.
  --interval       Loop interval (seconds or with s/m/h suffix).
  --force          Skip safety checks for existing processes/ports.
  --skip-rollups   Skip periodic DFG + ops rollups in live mode.
EOF
}

parse_interval() {
  local raw="$1"
  if [[ "$raw" =~ ^[0-9]+$ ]]; then
    echo "$raw"
    return 0
  fi
  if [[ "$raw" =~ ^([0-9]+)([smh])$ ]]; then
    local value="${BASH_REMATCH[1]}"
    local unit="${BASH_REMATCH[2]}"
    case "$unit" in
      s) echo "$value" ;;
      m) echo $((value * 60)) ;;
      h) echo $((value * 3600)) ;;
    esac
    return 0
  fi
  echo "Invalid interval: $raw" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --live)
      LIVE=true
      shift
      ;;
    --interval)
      if [ -z "${2:-}" ]; then
        echo "Missing value for --interval" >&2
        exit 1
      fi
      INTERVAL="$2"
      shift 2
      ;;
    --interval=*)
      INTERVAL="${1#*=}"
      shift
      ;;
    --force)
      FORCE=true
      shift
      ;;
    --skip-rollups)
      SKIP_ROLLUPS=true
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage
      exit 1
      ;;
  esac
done

check_port_free() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "Port $port is already in use. Stop the process or use --force."
      exit 1
    fi
  else
    echo "lsof not found; unable to check port $port."
  fi
}

check_process() {
  local pattern="$1"
  if command -v pgrep >/dev/null 2>&1; then
    if pgrep -f "$pattern" >/dev/null 2>&1; then
      echo "Process already running ($pattern). Stop it or use --force."
      exit 1
    fi
  else
    echo "pgrep not found; unable to check process '$pattern'."
  fi
}

kill_process_tree() {
  local pid="$1"
  if [ -z "$pid" ]; then
    return
  fi
  if command -v pgrep >/dev/null 2>&1; then
    local children
    children="$(pgrep -P "$pid" 2>/dev/null || true)"
    for child in $children; do
      kill_process_tree "$child"
    done
  fi
  kill -TERM "$pid" >/dev/null 2>&1 || true
}

kill_process_tree_int() {
  local pid="$1"
  if [ -z "$pid" ]; then
    return
  fi
  if command -v pgrep >/dev/null 2>&1; then
    local children
    children="$(pgrep -P "$pid" 2>/dev/null || true)"
    for child in $children; do
      kill_process_tree_int "$child"
    done
  fi
  kill -INT "$pid" >/dev/null 2>&1 || true
}

wait_for_exit() {
  local pid="$1"
  if [ -z "$pid" ]; then
    return
  fi
  for _ in {1..50}; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  kill -KILL "$pid" >/dev/null 2>&1 || true
}

wait_for_port_free() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    return
  fi
  for _ in {1..50}; do
    if ! lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
}

get_default_ngrok_config() {
  local output
  output="$(ngrok config check 2>/dev/null || true)"
  local line
  line="$(printf '%s\n' "$output" | head -n 1)"
  if [[ "$line" =~ at[[:space:]](.+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UI_DIR="$ROOT_DIR/ui"

if [ ! -d "$UI_DIR/node_modules" ]; then
  echo "Missing UI dependencies. Run: (cd ui && bun install)"
  exit 1
fi

LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/fhevm-stats-ngrok-XXXXXX")"
INTERVAL_SECONDS="$(parse_interval "$INTERVAL")"
STREAM_PAUSE_FILE="$LOG_DIR/stream.pause"
STREAM_PID_FILE="$LOG_DIR/stream.pid"

if [ "$FORCE" = false ]; then
  check_port_free 4310
  check_port_free 5173
  check_process "bun run stream"
  check_process "bun run src/server.ts"
  check_process "bun run dev"
  check_process "ngrok"
fi

cleanup() {
  kill_process_tree "${ROLLUP_PID:-}"
  kill_process_tree "${STREAM_PID:-}"
  kill_process_tree "${UI_PID:-}"
  kill_process_tree "${API_PID:-}"
  kill_process_tree "${NGROK_PID:-}"

  wait_for_exit "${ROLLUP_PID:-}"
  wait_for_exit "${STREAM_PID:-}"
  wait_for_exit "${UI_PID:-}"
  wait_for_exit "${API_PID:-}"
  wait_for_exit "${NGROK_PID:-}"

  wait_for_port_free 4310
  wait_for_port_free 5173
  rm -f "$STREAM_PAUSE_FILE" "$STREAM_PID_FILE"
}

trap cleanup EXIT INT TERM

run_build_rollup() {
  local lock_dir="$LOG_DIR/dfg-build.lock"
  if mkdir "$lock_dir" 2>/dev/null; then
    {
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] pausing stream"
      touch "$STREAM_PAUSE_FILE"
      if [ -f "$STREAM_PID_FILE" ]; then
        local stream_pid
        stream_pid="$(cat "$STREAM_PID_FILE" 2>/dev/null || true)"
        if [ -n "$stream_pid" ]; then
          kill_process_tree_int "$stream_pid"
          wait_for_exit "$stream_pid"
        fi
        rm -f "$STREAM_PID_FILE"
      fi
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] dfg:build"
      bun run dfg:build
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] dfg:rollup"
      bun run dfg:rollup
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] rollup:ops:all"
      bun run rollup:ops:all
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] resuming stream"
      rm -f "$STREAM_PAUSE_FILE"
    } >>"$LOG_DIR/dfg-loop.log" 2>&1 || {
      echo "dfg build/rollup failed. See $LOG_DIR/dfg-loop.log"
    }
    rmdir "$lock_dir" >/dev/null 2>&1 || true
  else
    echo "dfg build already running, skipping"
  fi
}

if [ "$LIVE" = true ]; then
  echo "Starting stream (auto-restart)..."
  (
    while true; do
      if [ -f "$STREAM_PAUSE_FILE" ]; then
        sleep 1
        continue
      fi
      bun run stream >>"$LOG_DIR/stream.log" 2>&1 &
      echo $! >"$STREAM_PID_FILE"
      wait "$!" || true
      rm -f "$STREAM_PID_FILE"
      if [ -f "$STREAM_PAUSE_FILE" ]; then
        continue
      fi
      echo "stream exited; retrying in ${STREAM_RESTART_DELAY_SECONDS}s" >>"$LOG_DIR/stream.log"
      sleep "$STREAM_RESTART_DELAY_SECONDS"
    done
  ) &
  STREAM_PID=$!

  if [ "$SKIP_ROLLUPS" = true ]; then
    echo "Skipping rollups in live mode."
  else
    echo "Starting dfg loop (interval: $INTERVAL, initial delay: ${DFG_INITIAL_DELAY_SECONDS}s)..."
    (
      sleep "$DFG_INITIAL_DELAY_SECONDS"
      while true; do
        run_build_rollup
        sleep "$INTERVAL_SECONDS"
      done
    ) &
    ROLLUP_PID=$!
  fi
fi

echo "Starting API..."
bun run serve >"$LOG_DIR/api.log" 2>&1 &
API_PID=$!

NGROK_CONFIG_TEMP="$LOG_DIR/ngrok.yml"
cat >"$NGROK_CONFIG_TEMP" <<'EOF'
version: "3"
tunnels:
  api:
    proto: http
    addr: 4310
    inspect: false
  ui:
    proto: http
    addr: 5173
    inspect: false
EOF

DEFAULT_NGROK_CONFIG="$(get_default_ngrok_config || true)"
if [ -n "$DEFAULT_NGROK_CONFIG" ]; then
  NGROK_CONFIG_ARG="$DEFAULT_NGROK_CONFIG,$NGROK_CONFIG_TEMP"
else
  if [ -z "${NGROK_AUTHTOKEN:-}" ]; then
    echo "ngrok config not found and NGROK_AUTHTOKEN not set. Run: ngrok config add-authtoken <token>"
    exit 1
  fi
  printf '\nauthtoken: "%s"\n' "$NGROK_AUTHTOKEN" >>"$NGROK_CONFIG_TEMP"
  NGROK_CONFIG_ARG="$NGROK_CONFIG_TEMP"
fi

echo "Starting ngrok (single agent, api + ui tunnels)..."
ngrok start --all --config "$NGROK_CONFIG_ARG" --log=stdout --log-format=logfmt >"$LOG_DIR/ngrok.log" 2>&1 &
NGROK_PID=$!

API_URL="$(wait_for_url "$LOG_DIR/ngrok.log" "api" || true)"
if [ -z "$API_URL" ]; then
  echo "Failed to detect API ngrok URL. Check: $LOG_DIR/ngrok.log"
  print_log_tail "$LOG_DIR/ngrok.log"
  exit 1
fi

echo "Starting UI with VITE_API_BASE=$API_URL..."
(cd "$UI_DIR" && VITE_API_BASE="$API_URL" bun run dev -- --host 0.0.0.0 >"$LOG_DIR/ui.log" 2>&1) &
UI_PID=$!

UI_URL="$(wait_for_url "$LOG_DIR/ngrok.log" "ui" || true)"
if [ -z "$UI_URL" ]; then
  echo "Failed to detect UI ngrok URL. Check: $LOG_DIR/ngrok.log"
  print_log_tail "$LOG_DIR/ngrok.log"
  exit 1
fi

echo "API tunnel: $API_URL"
echo "UI tunnel:  $UI_URL"
echo "Share the UI tunnel URL with your colleagues."
echo "Logs: $LOG_DIR"
if [ "$LIVE" = true ]; then
  echo "Live mode: stream + dfg loop every $INTERVAL"
fi
echo "Press Ctrl-C to stop."

wait
