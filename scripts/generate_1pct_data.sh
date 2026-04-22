#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GENERATOR_DIR="$ROOT_DIR/DemoTools/DataGenerator"
OUTPUT_NAME="input_data_1pct_insert_only.csv"
BACKEND_RESOURCES_DIR="$ROOT_DIR/gui/cquirrel_flask/cquirrel_app/resources"
BACKEND_OUTPUT_FILE="$BACKEND_RESOURCES_DIR/$OUTPUT_NAME"

cd "$GENERATOR_DIR"

python3 - <<'PY'
import DataGenerator

DataGenerator.data_generator('one_percent_config.ini')
PY

mkdir -p "$BACKEND_RESOURCES_DIR"
cp "$GENERATOR_DIR/$OUTPUT_NAME" "$BACKEND_OUTPUT_FILE"

echo "1% input generated:"
echo "  $BACKEND_OUTPUT_FILE"
wc -l "$BACKEND_OUTPUT_FILE"
