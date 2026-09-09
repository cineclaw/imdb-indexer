# Build stage
FROM rust:bookworm AS builder

WORKDIR /src

# Copy manifests
COPY Cargo.toml Cargo.lock ./

ARG TARGETARCH
ARG VERSION=1.0.0
ENV APP_VERSION=$VERSION

# Pre-compile dependencies with dummy sources to warm cache
RUN mkdir -p src && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/main.rs
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target-${TARGETARCH},target=/src/target \
    cargo build --release || true

# Copy real application source code
COPY src ./src
RUN touch src/lib.rs src/main.rs

# Compile actual project with persistent cache mounts and export binary
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target-${TARGETARCH},target=/src/target \
    cargo build --release && \
    mkdir -p /out && \
    cp /src/target/release/imdb-indexer /out/imdb-indexer

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/imdb-indexer /app/imdb-indexer

EXPOSE 8090

ENV CONFIG_PATH=/app/config.yaml
ENV DATA_DIR=/data
ENV TMDB_CACHE_DIR=/data/posters

ENTRYPOINT ["/app/imdb-indexer"]
