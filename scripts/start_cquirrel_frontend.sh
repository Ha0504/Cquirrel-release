#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FRONTEND_DIR="$ROOT_DIR/gui/cquirrel_react"

if [ ! -f "$FRONTEND_DIR/package.json" ]; then
  echo "Frontend package.json is missing: $FRONTEND_DIR/package.json" >&2
  exit 1
fi

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
  echo "Frontend dependencies are missing: $FRONTEND_DIR/node_modules" >&2
  echo "Run 'npm install' in $FRONTEND_DIR first." >&2
  exit 1
fi

EXISTING_PID="$(lsof -tiTCP:3000 -sTCP:LISTEN | head -n 1 || true)"
if [ -n "$EXISTING_PID" ]; then
  EXISTING_COMMAND="$(ps -p "$EXISTING_PID" -o command= || true)"
  if printf '%s' "$EXISTING_COMMAND" | grep -Eq 'cquirrel_react|react-app-rewired|react-scripts|webpack'; then
    echo "Cquirrel frontend is already running on http://localhost:3000 (pid: $EXISTING_PID)."
    exit 0
  fi

  echo "Port 3000 is already in use by another process:" >&2
  echo "$EXISTING_COMMAND" >&2
  exit 1
fi

cd "$FRONTEND_DIR"
export BROWSER=none
exec npm start
