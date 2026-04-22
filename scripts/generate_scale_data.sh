#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 <scale-factor> [insert_only|sliding_windows]" >&2
  echo "Examples:" >&2
  echo "  $0 0.1 insert_only" >&2
  echo "  $0 0.1 sliding_windows" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GENERATOR_DIR="$ROOT_DIR/DemoTools/DataGenerator"
BACKEND_RESOURCES_DIR="$ROOT_DIR/gui/cquirrel_flask/cquirrel_app/resources"
SCALE_FACTOR="$1"
STREAMS_TYPE="${2:-insert_only}"
SLIDING_WINDOW_RATIO="${CQUIRREL_SLIDING_WINDOW_RATIO:-0.1}"
PYTHON_BIN="${PYTHON:-python3}"

if [ "$STREAMS_TYPE" != "insert_only" ] && [ "$STREAMS_TYPE" != "sliding_windows" ]; then
  echo "Unsupported streams type: $STREAMS_TYPE" >&2
  echo "Supported values: insert_only, sliding_windows" >&2
  exit 1
fi

read -r NORMALIZED_SCALE SCALE_TOKEN LINEITEM_SIZE WINDOW_SIZE < <(
  "$PYTHON_BIN" - "$SCALE_FACTOR" "$STREAMS_TYPE" "$SLIDING_WINDOW_RATIO" <<'PY'
from decimal import Decimal, InvalidOperation
import sys

scale_text, streams_type, ratio_text = sys.argv[1:4]

try:
    scale = Decimal(scale_text)
    ratio = Decimal(ratio_text)
except InvalidOperation:
    raise SystemExit("scale factor and sliding window ratio must be decimals")

if scale <= 0 or scale > 1:
    raise SystemExit("scale factor must satisfy 0 < scale <= 1")
if ratio <= 0 or ratio > 1:
    raise SystemExit("sliding window ratio must satisfy 0 < ratio <= 1")

lineitem_size = max(1, int(scale * Decimal(6000000)))
if streams_type == "insert_only":
    window_size = lineitem_size
else:
    window_size = max(1, int(lineitem_size * ratio))

def normalized(value):
    return format(value.normalize(), "f")

if scale == Decimal("1"):
    token = "all"
elif scale == Decimal("0.15"):
    token = "15pct"
elif scale == Decimal("0.01"):
    token = "1pct"
elif scale == Decimal("0.001"):
    token = "smoke"
else:
    percent = normalized(scale * Decimal(100))
    if "." in percent:
        percent = percent.rstrip("0").rstrip(".")
    token = percent.replace(".", "p") + "pct"

print(normalized(scale), token, lineitem_size, window_size)
PY
)

OUTPUT_NAME="input_data_${SCALE_TOKEN}_${STREAMS_TYPE}.csv"
CONFIG_FILE="$GENERATOR_DIR/generated_${SCALE_TOKEN}_${STREAMS_TYPE}_config.ini"
BACKEND_OUTPUT_FILE="$BACKEND_RESOURCES_DIR/$OUTPUT_NAME"

cat > "$CONFIG_FILE" <<EOF
[DEFAULT]
DataFilePath = .
WindowSize = $WINDOW_SIZE
ScaleFactor = $NORMALIZED_SCALE
isLineitem = yes
isOrders = yes
isCustomer = yes
isPartSupp = yes
isPart = yes
isSupplier = yes
isNation = yes
isRegion = yes
OutputFileName = $OUTPUT_NAME

[KAFKA_CONF]
KafkaEnable = no
BootstrapServer = localhost:9092
KafkaTopic = aju_data_generator
EOF

echo "Generating Cquirrel input data:"
echo "  scale factor: $NORMALIZED_SCALE"
echo "  streams type: $STREAMS_TYPE"
echo "  lineitem rows: $LINEITEM_SIZE"
echo "  window size: $WINDOW_SIZE"
echo "  output: $BACKEND_OUTPUT_FILE"

cd "$GENERATOR_DIR"
"$PYTHON_BIN" - "$CONFIG_FILE" <<'PY'
import sys
import DataGenerator

DataGenerator.data_generator(sys.argv[1])
PY

mkdir -p "$BACKEND_RESOURCES_DIR"
cp "$GENERATOR_DIR/$OUTPUT_NAME" "$BACKEND_OUTPUT_FILE"
rm -f "$GENERATOR_DIR/$OUTPUT_NAME"

echo "Generated:"
echo "  $BACKEND_OUTPUT_FILE"
wc -l "$BACKEND_OUTPUT_FILE"
