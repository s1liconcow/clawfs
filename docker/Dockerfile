# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Build with the repository root as the Docker context:
#   docker build -f docker/Dockerfile .
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY clawfs-preload ./clawfs-preload
COPY clawfs-nfs-gateway ./clawfs-nfs-gateway
COPY fuser-mt ./fuser-mt

RUN cargo build --release --locked --no-default-features \
    --package clawfs \
    --bin clawfs_maintenance_worker \
    --bin clawfs_relay_executor \
    --bin clawfs_org_worker

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgcc-s1 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /var/lib/clawfs --shell /usr/sbin/nologin clawfs

WORKDIR /var/lib/clawfs

COPY --from=builder /app/target/release/clawfs_maintenance_worker /usr/local/bin/clawfs_maintenance_worker
COPY --from=builder /app/target/release/clawfs_relay_executor /usr/local/bin/clawfs_relay_executor
COPY --from=builder /app/target/release/clawfs_org_worker /usr/local/bin/clawfs_org_worker
COPY docker/hosted-accelerator-entrypoint.sh /usr/local/bin/hosted-accelerator-entrypoint.sh

ENV HOSTED_ACCELERATOR_BIN=clawfs_maintenance_worker

USER clawfs

ENTRYPOINT ["/bin/sh", "/usr/local/bin/hosted-accelerator-entrypoint.sh"]
CMD []
