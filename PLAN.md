# Improvement Plan

## 1) HTTP robustness and safety
- Add request body size limits for `POST /{key}` and `POST /` (return 413 on overflow).
- Return clear error responses for invalid `q` or JSON/CBOR decode failures (use 4xx/5xx and JSON error payloads).
- Set `Content-Type: application/json` for JSON responses and errors.

## 2) LMDB configuration and resilience
- Make LMDB map size configurable (env var or CLI flag) with a documented default.
- Consider adding dynamic map resizing when nearing capacity, or document manual steps.

## 3) Data handling consistency
- Decide whether non-object values are allowed; if so, wrap them with `_key`/`_at` metadata or store metadata separately.
- Surface per-row decode errors optionally (debug mode) instead of silently skipping.

## 4) Observability
- Replace `println!` with structured logging (`tracing`) including method, path, status, duration.
- Add a minimal request ID for correlation in logs.

## 5) Testing and guardrails
- Add unit tests for `cbor` encode/decode and error paths.
- Add tests for query normalization and filtering behavior.
- Add a small integration test for the HTTP API (optional, behind feature flag).

## 6) Documentation and usage clarity
- Document API routes and parameters in README (examples for GET/POST/DELETE).
- Document expected query patterns (inputs vs aggregations) and edge cases.
