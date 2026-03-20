# Default Dockerfile for building ClawFS CSI driver
# Build a multi-stage image: compile in Rust builder, copy to minimal runtime

# Stage 1: Build
FROM rust:1.75-bookworm as builder

WORKDIR /workspace

# Copy workspace
COPY . .

# Build CSI driver (release mode)
RUN cargo build --release -p clawfs-csi

# Stage 2: Runtime
FROM ubuntu:24.04

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    mount \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /workspace/target/release/clawfs-csi /clawfs-csi

# Set permissions
RUN chmod +x /clawfs-csi

# Default arguments
ENTRYPOINT ["/clawfs-csi"]

# Default: run as node driver
CMD ["--endpoint=unix:///csi/csi.sock", "--node-id=unknown", "--log-level=info"]
