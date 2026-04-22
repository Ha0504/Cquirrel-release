#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.run/cquirrel"
PID_DIR="$RUNTIME_DIR/pids"

kill_pid() {
  local pid="$1"
  local label="$2"

  if [ -z "$pid" ]; then
    return 0
  fi

  if ! ps -p "$pid" >/dev/null 2>&1; then
    return 0
  fi

  echo "Stopping $label (pid: $pid) ..."
  kill "$pid" >/dev/null 2>&1 || true

  for _ in $(seq 1 15); do
    if ! ps -p "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  kill -9 "$pid" >/dev/null 2>&1 || true
}

kill_pidfile() {
  local pid_file="$1"
  local label="$2"

  if [ ! -f "$pid_file" ]; then
    return 0
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  kill_pid "$pid" "$label"
  rm -f "$pid_file"
}

kill_matching_pids() {
  local label="$1"
  shift

  local pid
  for pid in "$@"; do
    kill_pid "$pid" "$label"
  done
}

kill_pidfile "$PID_DIR/frontend.pid" "frontend"
kill_pidfile "$PID_DIR/backend.pid" "backend"
kill_pidfile "$PID_DIR/flink-taskmanager.pid" "flink-taskmanager"
kill_pidfile "$PID_DIR/flink-jobmanager.pid" "flink-jobmanager"

"$ROOT_DIR/scripts/start_local_flink.sh" stop >/dev/null 2>&1 || true

BACKEND_PIDS="$(pgrep -f "cquirrel_gui.py" || true)"
if [ -n "$BACKEND_PIDS" ]; then
  kill_matching_pids "backend" $BACKEND_PIDS
fi

FRONTEND_PID="$(lsof -tiTCP:3000 -sTCP:LISTEN | head -n 1 || true)"
if [ -n "$FRONTEND_PID" ]; then
  FRONTEND_COMMAND="$(ps -p "$FRONTEND_PID" -o command= || true)"
  if printf '%s' "$FRONTEND_COMMAND" | grep -Eq 'cquirrel_react|react-app-rewired|react-scripts|webpack'; then
    kill_pid "$FRONTEND_PID" "frontend"
  fi
fi

FLINK_JOBMANAGER_PIDS="$(jps -lv | awk '/StandaloneSessionClusterEntrypoint/ {print $1}' || true)"
if [ -n "$FLINK_JOBMANAGER_PIDS" ]; then
  kill_matching_pids "flink-jobmanager" $FLINK_JOBMANAGER_PIDS
fi

FLINK_TASKMANAGER_PIDS="$(jps -lv | awk '/TaskManagerRunner/ {print $1}' || true)"
if [ -n "$FLINK_TASKMANAGER_PIDS" ]; then
  kill_matching_pids "flink-taskmanager" $FLINK_TASKMANAGER_PIDS
fi

echo "Cquirrel stack stop sequence finished."
