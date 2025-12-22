# jqkv

HTTP key/value store backed by LMDB with JaQ queries over JSON.

## Features
- Stores JSON values (encoded as CBOR in LMDB).
- Query over a key prefix or full DB using JaQ filters.
- Streaming JSON array responses.
- Metadata fields added to query inputs: `_key` (string), `_at` (unix seconds).

## API
All responses are JSON. Errors return `{"error":"...","code":"..."}`.

### GET /{key}
Fetch a single value by key.

### GET /?q=...&from=...
Run a query over all keys. `q` defaults to `.`. `from` is unix seconds (float).

### GET /{prefix}/?q=...&from=...
Run a query over keys with the given prefix. The prefix must end with `/`.

### POST /{key}
Upsert a JSON value at the key. Returns `204` on change, `304` if unchanged.

### POST /
Run a query with a JSON body:
```json
{
  "prefixes": ["logs/", "users/"],
  "query": ".field",
  "from": 0
}
```
`from` is optional (defaults to `0`).

### DELETE /{key}
Delete a key. Returns `204`.

## Query notes
- If the query does not contain `inputs`, the server runs `inputs | <query>`.
- Each input is a JSON object with `_key` and `_at` added.

## Configuration
- `STORE_PORT` (default: `3000`)
- `STORE_DB_PATH` (default: `cache`)
- `STORE_MAX_BODY_BYTES` (default: `1048576`)

The server binds to `127.0.0.1:<PORT>`.

## Build and run
```
cargo build
cargo run
```

## Docker
The server binds to `127.0.0.1`, so port publishing will not expose it outside the container.
Use host networking or change the bind address in code.

```
docker build -t jqkv .
docker run --rm --network host \
  -e STORE_DB_PATH=/data \
  -v $(pwd)/cache:/data \
  jqkv
```
