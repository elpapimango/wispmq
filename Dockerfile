# syntax=docker/dockerfile:1

# ---- Build stage (cross-compiles on the native build platform) ----
# Pinned to $BUILDPLATFORM so the compiler runs natively (fast) and targets the
# requested $TARGETARCH via a cross toolchain — avoids emulating the whole build.
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS builder
ARG TARGETARCH
# Extra Cargo features, empty by default so the published image stays lean.
# Build an OTLP-exporting image with: docker build --build-arg FEATURES=otel .
ARG FEATURES=
WORKDIR /app

# aarch64 (arm64) cross toolchain (incl. target libc headers) + Rust targets.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

# Cross-compilation env for aarch64 (used only when TARGETARCH=arm64).
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar

COPY . .
RUN set -eux; \
    case "$TARGETARCH" in \
    amd64) target=x86_64-unknown-linux-gnu ;; \
    arm64) target=aarch64-unknown-linux-gnu ;; \
    *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    features=""; \
    if [ -n "$FEATURES" ]; then features="--features $FEATURES"; fi; \
    cargo build --release --locked --target "$target" --bin pulsemq $features; \
    cp "target/$target/release/pulsemq" /pulsemq

# ---- Runtime stage (target architecture) ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -M -s /usr/sbin/nologin pulsemq

COPY --from=builder /pulsemq /usr/local/bin/pulsemq

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

ENTRYPOINT ["pulsemq"]
