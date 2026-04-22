#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.run/cquirrel"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"
BACKEND_HOST="${CQUIRREL_BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${CQUIRREL_BACKEND_PORT:-5051}"
BACKEND_ORIGIN="http://$BACKEND_HOST:$BACKEND_PORT"
RESOURCES_DIR="$ROOT_DIR/gui/cquirrel_flask/cquirrel_app/resources"
STREAMS_TYPE="${CQUIRREL_STREAMS_TYPE:-insert_only}"

mkdir -p "$LOG_DIR" "$PID_DIR"

resolve_requested_input_data_file() {
  if [ -n "${CQUIRREL_INPUT_DATA_FILE:-}" ]; then
    python3 - <<'PY'
from pathlib import Path
import os

print(Path(os.environ["CQUIRREL_INPUT_DATA_FILE"]).expanduser().resolve())
PY
    return 0
  fi

  case "${CQUIRREL_DATASET:-}" in
    "" )
      return 0
      ;;
    15|15pct )
      echo "$RESOURCES_DIR/input_data_15pct_${STREAMS_TYPE}.csv"
      ;;
    1|1pct )
      echo "$RESOURCES_DIR/input_data_1pct_${STREAMS_TYPE}.csv"
      ;;
    smoke )
      echo "$RESOURCES_DIR/input_data_smoke_${STREAMS_TYPE}.csv"
      ;;
    all|100|100pct )
      echo "$RESOURCES_DIR/input_data_all_${STREAMS_TYPE}.csv"
      ;;
    * )
      python3 - "$RESOURCES_DIR" "$STREAMS_TYPE" "${CQUIRREL_DATASET}" <<'PY'
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sys

resources_dir, streams_type, dataset = sys.argv[1:4]
text = dataset.strip().lower()

try:
    if text.endswith("pct"):
        scale = Decimal(text[:-3].replace("p", ".")) / Decimal("100")
    else:
        value = Decimal(text)
        scale = value / Decimal("100") if value > 1 else value
except InvalidOperation:
    raise SystemExit(f"Unsupported CQUIRREL_DATASET value: {dataset}")

if scale <= 0 or scale > 1:
    raise SystemExit(f"Unsupported CQUIRREL_DATASET value: {dataset}")

if scale == Decimal("1"):
    token = "all"
elif scale == Decimal("0.15"):
    token = "15pct"
elif scale == Decimal("0.01"):
    token = "1pct"
elif scale == Decimal("0.001"):
    token = "smoke"
else:
    percent = format((scale * Decimal("100")).normalize(), "f")
    if "." in percent:
        percent = percent.rstrip("0").rstrip(".")
    token = percent.replace(".", "p") + "pct"

print(Path(resources_dir) / f"input_data_{token}_{streams_type}.csv")
PY
      ;;
  esac
}

REQUESTED_INPUT_DATA_FILE="$(resolve_requested_input_data_file)"
if [ -n "$REQUESTED_INPUT_DATA_FILE" ] && [ ! -f "$REQUESTED_INPUT_DATA_FILE" ]; then
  echo "Requested input data file does not exist: $REQUESTED_INPUT_DATA_FILE" >&2
  exit 1
fi

if [ -n "$REQUESTED_INPUT_DATA_FILE" ]; then
  export CQUIRREL_INPUT_DATA_FILE="$REQUESTED_INPUT_DATA_FILE"
fi

wait_for_http() {
  local url="$1"
  local timeout_seconds="$2"
  local started_at
  started_at="$(date +%s)"

  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi

    if [ $(( $(date +%s) - started_at )) -ge "$timeout_seconds" ]; then
      echo "Timed out waiting for $url" >&2
      return 1
    fi

    sleep 1
  done
}

wait_for_taskmanager() {
  local timeout_seconds="$1"
  local started_at
  started_at="$(date +%s)"

  while true; do
    local overview
    overview="$(curl -fsS http://localhost:8081/overview 2>/dev/null || true)"
    if printf '%s' "$overview" | grep -Eq '"taskmanagers":[1-9]'; then
      return 0
    fi

    if [ $(( $(date +%s) - started_at )) -ge "$timeout_seconds" ]; then
      echo "Timed out waiting for Flink TaskManager registration." >&2
      return 1
    fi

    sleep 1
  done
}

start_background() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  shift 3

  echo "Starting $name ..."
  nohup "$@" </dev/null >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"
}

wait_for_process_exit() {
  local pid="$1"
  local timeout_seconds="$2"
  local started_at
  started_at="$(date +%s)"

  while kill -0 "$pid" >/dev/null 2>&1; do
    if [ $(( $(date +%s) - started_at )) -ge "$timeout_seconds" ]; then
      return 1
    fi
    sleep 1
  done

  return 0
}

stop_backend_processes() {
  local pids
  pids="$(pgrep -af "cquirrel_gui.py" | awk '{print $1}' || true)"

  if [ -z "$pids" ]; then
    return 0
  fi

  echo "Stopping backend pid(s): $pids"
  kill $pids >/dev/null 2>&1 || true

  for pid in $pids; do
    if ! wait_for_process_exit "$pid" 10; then
      echo "Backend pid $pid did not exit in time. Killing forcefully ..."
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  done
}

ensure_backend() {
  local existing_pid
  local current_input_data_file
  local settings_payload
  existing_pid="$(pgrep -af "cquirrel_gui.py" | awk 'NR==1 {print $1}' || true)"

  if [ -n "$existing_pid" ]; then
    settings_payload="$(curl -fsS "$BACKEND_ORIGIN/r/settings" 2>/dev/null || true)"
    current_input_data_file="$(printf '%s' "$settings_payload" | python3 - <<'PY'
import json
import sys

payload = sys.stdin.read().strip()
if not payload:
    print("")
else:
    print((json.loads(payload).get("input_data_file") or "").strip())
PY
)"

    if [ -z "$settings_payload" ]; then
      echo "Backend process exists but the API is not responding. Restarting backend ..."
      stop_backend_processes
    elif [ -n "$REQUESTED_INPUT_DATA_FILE" ] && [ "$current_input_data_file" != "$REQUESTED_INPUT_DATA_FILE" ]; then
      echo "Backend is running with a different input file."
      echo "  current:   ${current_input_data_file:-<unknown>}"
      echo "  requested: $REQUESTED_INPUT_DATA_FILE"
      echo "Restarting backend ..."
      stop_backend_processes
    else
      echo "Backend already running on $BACKEND_ORIGIN (pid: $existing_pid)."
      if [ -n "$current_input_data_file" ]; then
        echo "Backend input: $current_input_data_file"
      fi
      return 0
    fi
  fi

  start_background \
    "backend" \
    "$PID_DIR/backend.pid" \
    "$LOG_DIR/backend.log" \
    "$ROOT_DIR/scripts/start_cquirrel_backend.sh"

  wait_for_http "$BACKEND_ORIGIN/r/settings" 30
  echo "Backend is ready: $BACKEND_ORIGIN"
  if [ -n "$REQUESTED_INPUT_DATA_FILE" ]; then
    echo "Backend input: $REQUESTED_INPUT_DATA_FILE"
  fi
}

ensure_flink_cluster() {
  local overview
  overview="$(curl -fsS http://localhost:8081/overview 2>/dev/null || true)"

  if printf '%s' "$overview" | grep -Eq '"taskmanagers":[1-9]'; then
    echo "Flink cluster already running on http://localhost:8081."
    return 0
  fi

  if [ -n "$overview" ]; then
    echo "Flink JobManager is up but no TaskManager is registered. Restarting local Flink cluster ..."
    "$ROOT_DIR/scripts/start_local_flink.sh" stop >"$LOG_DIR/flink-stop.log" 2>&1 || true
    sleep 2
  else
    echo "Starting Flink cluster ..."
  fi

  "$ROOT_DIR/scripts/start_local_flink.sh" start-daemon >"$LOG_DIR/flink-cluster.log" 2>&1

  wait_for_http "http://localhost:8081/overview" 30
  wait_for_taskmanager 30
  echo "Flink cluster is ready: http://localhost:8081"
}

ensure_frontend() {
  local existing_pid existing_command
  existing_pid="$(lsof -tiTCP:3000 -sTCP:LISTEN | head -n 1 || true)"

  if [ -n "$existing_pid" ]; then
    existing_command="$(ps -p "$existing_pid" -o command= || true)"
    if printf '%s' "$existing_command" | grep -Eq 'cquirrel_react|react-app-rewired|react-scripts|webpack'; then
      wait_for_http "http://localhost:3000" 10
      echo "Frontend already running on http://localhost:3000 (pid: $existing_pid)."
      return 0
    fi

    echo "Port 3000 is already used by another process:" >&2
    echo "$existing_command" >&2
    return 1
  fi

  start_background \
    "frontend" \
    "$PID_DIR/frontend.pid" \
    "$LOG_DIR/frontend.log" \
    "$ROOT_DIR/scripts/start_cquirrel_frontend.sh"

  wait_for_http "http://localhost:3000" 120
  echo "Frontend is ready: http://localhost:3000"
}

ensure_flink_cluster
ensure_backend
ensure_frontend

echo
echo "Cquirrel stack is ready."
echo "Frontend: http://localhost:3000"
echo "Backend:  $BACKEND_ORIGIN"
echo "Flink:    http://localhost:8081"
echo "Logs:     $LOG_DIR"
