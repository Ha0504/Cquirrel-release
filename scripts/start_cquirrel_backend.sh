#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/gui/cquirrel_flask"
BACKEND_HOST="${CQUIRREL_BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${CQUIRREL_BACKEND_PORT:-5051}"

find_backend_python() {
  local candidate
  for candidate in \
    "$BACKEND_DIR/.backend-venv312/bin/python" \
    "$BACKEND_DIR/.backend-venv/bin/python" \
    "$BACKEND_DIR/.venv/bin/python"
  do
    if [ -x "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  done

  return 1
}

VENV_PYTHON="$(find_backend_python || true)"
if [ -z "$VENV_PYTHON" ]; then
  echo "Backend virtualenv is missing." >&2
  echo "Expected one of:" >&2
  echo "  $BACKEND_DIR/.backend-venv312/bin/python" >&2
  echo "  $BACKEND_DIR/.backend-venv/bin/python" >&2
  echo "  $BACKEND_DIR/.venv/bin/python" >&2
  echo "Create a Python 3.12 virtualenv in $BACKEND_DIR and install the Flask dependencies first." >&2
  exit 1
fi

EXISTING_PID="$(lsof -tiTCP:"$BACKEND_PORT" -sTCP:LISTEN | head -n 1 || true)"
if [ -n "$EXISTING_PID" ]; then
  EXISTING_COMMAND="$(ps -p "$EXISTING_PID" -o command= || true)"
  if printf '%s' "$EXISTING_COMMAND" | grep -q "cquirrel_gui.py"; then
    echo "Cquirrel backend is already running on http://$BACKEND_HOST:$BACKEND_PORT (pid: $EXISTING_PID)."
    exit 0
  fi

  echo "Port $BACKEND_PORT is already in use by another process:" >&2
  echo "$EXISTING_COMMAND" >&2
  exit 1
fi

cd "$BACKEND_DIR"
export CQUIRREL_BACKEND_HOST="$BACKEND_HOST"
export CQUIRREL_BACKEND_PORT="$BACKEND_PORT"
exec "$VENV_PYTHON" cquirrel_gui.py
