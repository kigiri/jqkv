# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs` hosts the HTTP server, request routing, and query execution flow.
- `src/lmdb.rs` encapsulates LMDB setup and CRUD helpers.
- `src/jq.rs` compiles JaQ filters and loads std/json modules.
- `src/cbor.rs` encodes/decodes JSON <-> CBOR.
- `cache/` is the local LMDB data directory created at runtime (do not commit contents).

## Build, Test, and Development Commands
- `cargo build` compiles the service.
- `cargo run` starts the server on `127.0.0.1:3000`.
- `cargo test` runs unit tests (none yet; add tests as you extend behavior).
- `cargo fmt` formats Rust sources using rustfmt.
- `cargo clippy` runs the linter for common Rust issues.

## Coding Style & Naming Conventions
- Follow standard Rust formatting (4-space indentation, rustfmt defaults).
- Use module-focused files under `src/` with descriptive names (`lmdb.rs`, `jq.rs`).
- Keep HTTP and query logic in `main.rs`; place reusable helpers in dedicated modules.

## Testing Guidelines
- No testing framework is currently configured beyond Rust's built-in test harness.
- Prefer unit tests alongside the module they cover (e.g., `src/cbor.rs`).
- Name tests descriptively (e.g., `decode_invalid_cbor_returns_error`).

## Commit & Pull Request Guidelines
- Commit history uses Conventional Commit style with scopes, e.g. `feat(search): ...`, `fix(cbor): ...`, `refactor(main): ...`.
- PRs should include a clear summary, testing notes (`cargo test`/manual), and any API or behavior changes.

## Configuration & Data Notes
- LMDB data lives in `cache/` and is created automatically; treat it as local state.
- The API supports GET/POST/DELETE on keys and JQ-style queries via `q` on prefix routes.
