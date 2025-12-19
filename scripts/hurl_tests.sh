#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_PID=""
export STORE_DB_PATH="${ROOT}/tests/db"

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$STORE_DB_PATH"
}
trap cleanup EXIT

cargo run --bin store >"$ROOT/target/hurl_server.log" &
SERVER_PID=$!

for _ in $(seq 1 50); do
  if hurl --test "$ROOT/tests/hurl/ready.hurl" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

hurl --test "$ROOT/tests/hurl/ready.hurl" >/dev/null
hurl --test --jobs 10 "$ROOT/tests/hurl/basic.hurl"
