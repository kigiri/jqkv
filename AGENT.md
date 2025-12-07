# AGENT.md

## Overview
- Rust service for storing and querying JSON/CBOR entries with time metadata.
- Storage: LMDB via `heed`.
- Query engine: JaQ (jq-like), with custom inputs stream for per-row metadata.
- HTTP: hyper + tokio; heavy work offloaded to blocking threads.
- Response: currently buffered; planned streaming with cancellation.

## Tech stack
- Rust + Tokio + Hyper (`hyper_util`)
- Heed (LMDB)
- JaQ (`jaq-core`, `jaq-json`, `jaq-std`)
- CBOR codec (module `minicbor-serde`)

## Data model
- Keyed documents; timestamp stored as microseconds since epoch.
- Current implementation:
  - meta DB: key -> timestamp (u64 BE)
  - data DB: key -> CBOR blob

## Query pipeline (current)
1. HTTP GET with optional params: `q` (jaq), `from` (seconds), `limit`.
2. In a blocking thread:
   - Open LMDB read txn.
   - Prefix-iterate keys, apply min timestamp filter.
   - For each row:
     - Fetch value, decode CBOR.
     - Merge per-row metadata into the JSON object: `_key`, `_at` (seconds).
     - Convert to `jaq_json::Val`.
   - Build JaQ inputs as a streaming iterator: `RcIter::new(row_iter)`.
   - Execute a single JaQ filter run: `filter.run((Ctx::new([], &inputs), Val::Null))`.
   - Collect outputs (buffered currently), or stream (planned).

## JaQ usage conventions
- We stream data via `inputs`; the dot `.` is `null` by default for global queries.
- Convenience: if user query does not reference `inputs`, we compile `inputs | <user_query>`.
  - Good for per-row transforms: `.user`, `{k: ._key, at: ._at}`
  - For aggregations, users should explicitly use `inputs`:
    - `[inputs] | sort_by(._at)` (array-based ops)
    - `reduce inputs as $row ({}; .[$row.user] += 1)` (streaming reduce)
  - we skip empty result / errors

## Streaming responses (planned)
- Replace buffered collection with a streaming body to support early cancellation when the client disconnects.
- Architecture:
  - Create `tokio::sync::mpsc::channel<Bytes>(N)`.
  - Spawn blocking producer:
    - Sends `[` once, then each serialized value (preceded by comma after first), then `]`.
    - Uses `blocking_send` from the blocking thread.
    - If `send` fails => receiver dropped => stop early.
  - In async handler:
    - Wrap `Receiver<Bytes>` with `ReceiverStream`, map to `Frame::data`, feed `StreamBody`.
- Benefits: backpressure, cancellation, constant memory.

## API behavior
- GET /{key} → fetch a single value (decoded CBOR → JSON)
- GET /{prefix}/?q=...&from=...&limit=... → query over a prefix/time window.
- POST /{key} → write/update entry; stores CBOR and timestamp; returns 204 or 304 if unchanged.

## Implementation notes and patterns
- Compiling JaQ:
  - `Arena::default()` lives for the duration of the run.
  - Compile once per request: `jq::compile(query, &arena)`.
- Inputs helpers:
  - `jq::inputs_from_iter(iter)` wraps `Iterator<Item=Result<Val, String>>` in `RcIter`.
  - `jq::empty_inputs()` for filters that ignore auxiliary inputs.

## Indexing ideas (optional)
- Time index:
  - If needed for fast time-range across all keys: key as `[ts_be][raw_key]`.
  - For your current prefix+time ≥ filter, value-embedded timestamp is sufficient.
- Bidirectional index (key→ts and ts→key) only if you need time-first scans; otherwise avoid extra DB.

## Future tasks
- Implement streaming HTTP body with cancellation as described.
- Migrate to single-DB layout: pack timestamp into value.
- Add slow-query logging (threshold) with row count and duration.
- Optional: detect and short-circuit common cheap queries (e.g., count).
