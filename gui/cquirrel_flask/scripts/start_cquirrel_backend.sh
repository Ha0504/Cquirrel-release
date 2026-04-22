#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_SCRIPT="$SCRIPT_DIR/../../../scripts/start_cquirrel_backend.sh"

if [ ! -x "$ROOT_SCRIPT" ]; then
  echo "Root backend script is missing: $ROOT_SCRIPT" >&2
  exit 1
fi

exec "$ROOT_SCRIPT"
