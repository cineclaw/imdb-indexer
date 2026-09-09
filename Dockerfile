# Build stage
FROM rust:1.85-bookworm AS builder

WORKDIR /src

# Copy manifests and source code
COPY Cargo.toml Cargo.lock ./
COPY src ./src

ARG VERSION=1.0.0
ENV APP_VERSION=$VERSION

# Build release binary with full optimizations
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/imdb-indexer /app/imdb-indexer

EXPOSE 8090

ENV CONFIG_PATH=/app/config.yaml
ENV DATA_DIR=/data
ENV TMDB_CACHE_DIR=/data/posters

ENTRYPOINT ["/app/imdb-indexer"]
