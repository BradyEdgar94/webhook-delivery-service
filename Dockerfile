# syntax=docker/dockerfile:1.7
#
# Multi-stage build with cargo-chef for dependency caching.
#
# The first build compiles librdkafka (C, via cmake) and every Rust dependency,
# which is slow. cargo-chef splits that into its own layer keyed only on
# Cargo.toml/Cargo.lock, so rebuilding after a source change reuses it and
# only recompiles our own crates.

FROM rust:1.100-bookworm AS chef
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake build-essential clang pkg-config \
 && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --version 0.1.78 --locked
WORKDIR /app

# 1. Compute a "recipe" (the dependency graph) from the manifests.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# 2. Build only the dependencies (cached until Cargo.lock changes).
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo chef cook --release --recipe-path recipe.json
# 3. Build our code.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --bin hooklane --bin sink --bin loadcheck

# 4. Small runtime image: just the binaries and CA certificates (for HTTPS).
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 hooklane
COPY --from=builder /app/target/release/hooklane /usr/local/bin/hooklane
COPY --from=builder /app/target/release/sink /usr/local/bin/sink
COPY --from=builder /app/target/release/loadcheck /usr/local/bin/loadcheck
USER 10001
ENTRYPOINT ["hooklane"]
