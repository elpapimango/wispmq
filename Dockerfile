# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM rust:1-bookworm AS builder
WORKDIR /app

# Cache dependencies: build against stub sources first, then the real ones.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && : > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY . .
# Bust the cached crate build so the real sources are compiled.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --bin mqtt_server

# ---- Runtime stage ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -M -s /usr/sbin/nologin mqtt

COPY --from=builder /app/target/release/mqtt_server /usr/local/bin/mqtt_server

# Persistent state (SQLite DB) lives here.
WORKDIR /data
RUN chown 10001:10001 /data
VOLUME ["/data"]

# Container-friendly defaults (override via env / config file / flags).
ENV MQTT_LISTEN_ADDR=0.0.0.0:1883 \
    MQTT_ADMIN_ADDR=0.0.0.0:9001 \
    MQTT_DB_PATH=/data/mqtt_broker.db \
    RUST_LOG=info

# MQTT (1883), MQTT/TLS (8883), MQTT/WebSocket (8080), admin/metrics/MCP (9001).
EXPOSE 1883 8883 8080 9001

USER 10001

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9001/health || exit 1

ENTRYPOINT ["mqtt_server"]
