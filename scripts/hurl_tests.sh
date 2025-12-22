#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TLS_DIR="/tmp/jqkv-tests/tls"
SERVER_PID=""
export STORE_DB_PATH="/tmp/jqkv-tests/db"
export STORE_TLS_CERT="$TLS_DIR/server.crt"
export STORE_TLS_KEY="$TLS_DIR/server.key"

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "/tmp/jqkv-tests"
  rm -rf "$STORE_DB_PATH"
  rm -rf "$TLS_DIR"
}
trap cleanup EXIT

mkdir -p "$TLS_DIR"
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$TLS_DIR/server.key" \
  -out "$TLS_DIR/server.crt" \
  -days 1 -subj "/CN=localhost" >/dev/null 2>&1

mkdir -p "/tmp/jqkv-tests"
head -c 1100000 /dev/zero > "/tmp/jqkv-tests/large.bin"

cargo run --bin store >"$ROOT/target/hurl_server.log" &
SERVER_PID=$!
for n in $(seq 1 5); do
  if [[ n == 5 ]]; then
    echo "Server failed to start for HTTPS tests after 5 attempts" >&2
    exit 1
  fi
  if hurl --insecure --test "$ROOT/tests/hurl/ready.hurl" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

hurl --insecure --test --jobs 10 --file-root "/" "$ROOT/tests/hurl/basic.hurl"
hurl --insecure --test "$ROOT/tests/hurl/cleanup.hurl"
