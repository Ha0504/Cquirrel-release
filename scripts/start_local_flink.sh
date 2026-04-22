#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_PARENT="$(cd "$ROOT_DIR/.." && pwd)"

FLINK_HOME_DEFAULT="$PROJECT_PARENT/flink-1.11.2-scala_2.12"
JAVA_HOME_DEFAULT="/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home"

is_java_home() {
  [ -n "${1:-}" ] && [ -x "$1/bin/java" ]
}

is_java8_home() {
  [ -n "${1:-}" ] || return 1
  "$1/bin/java" -version 2>&1 | grep -Eq 'version "1\.8|version "8'
}

resolve_java_home() {
  local candidate
  local java8_from_system=""

  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    java8_from_system="$(/usr/libexec/java_home -v 1.8 2>/dev/null || true)"
  fi

  for candidate in \
    "${JAVA_HOME:-}" \
    "$JAVA_HOME_DEFAULT" \
    "$java8_from_system"
  do
    if is_java_home "$candidate" && is_java8_home "$candidate"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

FLINK_HOME="${FLINK_HOME:-$FLINK_HOME_DEFAULT}"
JAVA_HOME="$(resolve_java_home || true)"
MODE="${1:-jobmanager}"

if [ ! -x "$FLINK_HOME/bin/flink" ]; then
  echo "Flink home is invalid: $FLINK_HOME" >&2
  exit 1
fi

if [ -z "$JAVA_HOME" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
  echo "A Java 8 home is required for Flink 1.11.2." >&2
  exit 1
fi

case "$MODE" in
  jobmanager)
    cd "$FLINK_HOME"
    exec env JAVA_HOME="$JAVA_HOME" bin/jobmanager.sh start-foreground
    ;;
  taskmanager)
    cd "$FLINK_HOME"
    exec env JAVA_HOME="$JAVA_HOME" bin/taskmanager.sh start-foreground
    ;;
  start-daemon)
    cd "$FLINK_HOME"
    exec env JAVA_HOME="$JAVA_HOME" bin/start-cluster.sh
    ;;
  stop)
    cd "$FLINK_HOME"
    exec env JAVA_HOME="$JAVA_HOME" bin/stop-cluster.sh
    ;;
  *)
    echo "Usage: $0 {jobmanager|taskmanager|start-daemon|stop}" >&2
    exit 1
    ;;
esac
