#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GENERATOR_DIR="$ROOT_DIR/DemoTools/DataGenerator"
OUTPUT_NAME="input_data_15pct_sliding_windows.csv"
BACKEND_RESOURCES_DIR="$ROOT_DIR/gui/cquirrel_flask/cquirrel_app/resources"
BACKEND_OUTPUT_FILE="$BACKEND_RESOURCES_DIR/$OUTPUT_NAME"

cd "$GENERATOR_DIR"

python3 - <<'PY'
import DataGenerator

DataGenerator.data_generator('fifteen_percent_sliding_windows_config.ini')
PY

mkdir -p "$BACKEND_RESOURCES_DIR"
cp "$GENERATOR_DIR/$OUTPUT_NAME" "$BACKEND_OUTPUT_FILE"

echo "15% sliding-windows input generated:"
echo "  $BACKEND_OUTPUT_FILE"
wc -l "$BACKEND_OUTPUT_FILE"
