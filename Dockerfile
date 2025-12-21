FROM rust:1.85-slim-bookworm AS build

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch

WORKDIR /app

COPY --from=build /app/target/x86_64-unknown-linux-musl/release/store /app/store

ENV STORE_DB_PATH=/data \
    STORE_PORT=3000 \
    STORE_MAX_BODY_BYTES=1048576

VOLUME ["/data"]

EXPOSE 3000

ENTRYPOINT ["/app/store"]
