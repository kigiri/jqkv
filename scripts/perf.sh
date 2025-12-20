#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HYPERFINE_BIN="${HYPERFINE:-hyperfine}"
BASE_URL="${BASE_URL:-http://127.0.0.1:3000}"
SERVER_PID=""
DB_PATH="${ROOT}/target/perf-db"
REPORT_DIR="${REPORT_DIR:-$ROOT/target/perf-reports}"
RUN_TAG="${RUN_TAG:-$(date +%Y%m%d-%H%M%S)}"
REPORT_JSON="${REPORT_DIR}/perf-${RUN_TAG}.json"
REPORT_MD="${REPORT_DIR}/perf-${RUN_TAG}.md"

if ! command -v "$HYPERFINE_BIN" >/dev/null 2>&1; then
  echo "hyperfine not found in PATH"
  exit 1
fi

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$DB_PATH"
}
trap cleanup EXIT

mkdir -p "$REPORT_DIR"

STORE_DB_PATH="$DB_PATH" BASE_URL="$BASE_URL" cargo run --quiet --bin store >"$ROOT/target/perf_server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 50); do
  if curl -sS "$BASE_URL/__ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

"$HYPERFINE_BIN" \
  --warmup 1 \
  --min-runs 3 \
  --export-json "$REPORT_JSON" \
  --export-markdown "$REPORT_MD" \
  --command-name "big_writes" "BASE_URL=$BASE_URL $ROOT/scripts/perf_scenario.sh big_writes" \
  --command-name "simple_query" "BASE_URL=$BASE_URL $ROOT/scripts/perf_scenario.sh simple_query" \
  --command-name "complex_query" "BASE_URL=$BASE_URL $ROOT/scripts/perf_scenario.sh complex_query" \
  --command-name "small_writes" "BASE_URL=$BASE_URL $ROOT/scripts/perf_scenario.sh small_writes" \
  --command-name "mixed_read_write" "BASE_URL=$BASE_URL $ROOT/scripts/perf_scenario.sh mixed"
