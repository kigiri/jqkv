#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
BASE_URL="${BASE_URL:-http://127.0.0.1:3000}"
RUN_ID="$(date +%s)-$RANDOM"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl not found in PATH"
  exit 1
fi

PAYLOAD_DIR="${PAYLOAD_DIR:-/tmp/store_perf}"
mkdir -p "$PAYLOAD_DIR"

LARGE_PAYLOAD="$PAYLOAD_DIR/large.json"
SMALL_PAYLOAD="$PAYLOAD_DIR/small.json"

if [[ ! -f "$LARGE_PAYLOAD" ]]; then
  data=$(head -c 100000 /dev/zero | tr '\0' 'a')
  printf '{"data":"%s"}' "$data" > "$LARGE_PAYLOAD"
fi

if [[ ! -f "$SMALL_PAYLOAD" ]]; then
  printf '{"data":"ok"}' > "$SMALL_PAYLOAD"
fi

case "$SCENARIO" in
  big_writes)
    for i in $(seq 1 50); do
      curl -sS -X POST "$BASE_URL/perf/$RUN_ID/big/$i" \
        -H 'Content-Type: application/json' \
        --data-binary "@$LARGE_PAYLOAD" >/dev/null
    done
    ;;
  simple_query)
    for i in $(seq 1 400); do
      json=$(printf '{"score": %d, "group": "g%d"}' "$i" "$((i % 10))")
      curl -sS -X POST "$BASE_URL/perf/$RUN_ID/query/$i" \
        -H 'Content-Type: application/json' \
        --data-binary "$json" >/dev/null
    done

    for _ in $(seq 1 40); do
      curl -sS -X POST "$BASE_URL/" \
        -H 'Content-Type: application/json' \
        --data-binary "{\"prefixes\":[\"perf/$RUN_ID/query/\"],\"query\":\"inputs | .score\",\"from\":0}" \
        >/dev/null
    done
    ;;
  complex_query)
    for i in $(seq 1 400); do
      json=$(printf '{"score": %d, "group": "g%d"}' "$i" "$((i % 10))")
      curl -sS -X POST "$BASE_URL/perf/$RUN_ID/query/$i" \
        -H 'Content-Type: application/json' \
        --data-binary "$json" >/dev/null
    done

    for _ in $(seq 1 40); do
      curl -sS -X POST "$BASE_URL/" \
        -H 'Content-Type: application/json' \
        --data-binary "{\"prefixes\":[\"perf/$RUN_ID/query/\"],\"query\":\"[inputs | select(.score > 900) | {k: ._key, s: .score}] | sort_by(.s) | last\",\"from\":0}" \
        >/dev/null
    done
    ;;
  small_writes)
    for i in $(seq 1 400); do
      curl -sS -X POST "$BASE_URL/perf/$RUN_ID/small/$i" \
        -H 'Content-Type: application/json' \
        --data-binary "@$SMALL_PAYLOAD" >/dev/null
    done
    ;;
  mixed)
    for i in $(seq 1 200); do
      curl -sS -X POST "$BASE_URL/perf/$RUN_ID/mix/item/$i" \
        -H 'Content-Type: application/json' \
        --data-binary "@$SMALL_PAYLOAD" >/dev/null
    done

    for i in $(seq 1 400); do
      if (( i % 10 == 0 )); then
        curl -sS -X POST "$BASE_URL/perf/$RUN_ID/mix/write/$i" \
          -H 'Content-Type: application/json' \
          --data-binary "@$SMALL_PAYLOAD" >/dev/null
      else
        key=$(( (RANDOM % 200) + 1 ))
        curl -sS "$BASE_URL/perf/$RUN_ID/mix/item/$key" >/dev/null
      fi
    done
    ;;
  *)
    echo "Usage: $0 {big_writes|small_writes|mixed}"
    exit 1
    ;;
esac
