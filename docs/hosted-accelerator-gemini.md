# Hosted Accelerator for ClawFS

This file is a shorter, legacy design summary kept for reference. The authoritative design doc is [`docs/clawfs-hosted-accelerator-codex.md`](docs/clawfs-hosted-accelerator-codex.md).

## Layout Note

The current split-prep workspace uses:

- `crates/clawfs-core` for the future OSS/public facade
- `crates/clawfs-private` for the future private/business facade

Hosted accelerator binaries still build from the top-level `clawfs` package while the repo split is in progress.

## Overview
The **ClawFS Hosted Accelerator** is a managed service designed to run alongside a volume's object storage backend. Its primary purpose is to lower latency for connected clients and offload critical maintenance operations from the client-side drivers to a centralized, cloud-native service.

## Goals
1. **Lower Latency:** Reduce metadata operation latency by centralizing state and caching frequent lookups closer to the data or the client.
2. **Offload Maintenance:** Move compaction (delta merging, segment rewriting) and garbage collection tasks from distributed clients to a dedicated, high-availability service. This simplifies client configuration (`--disable-cleanup`) and reduces client resource usage.

## Architecture

The Accelerator operates in two modes, which can be enabled incrementally:

### 1. Compaction Service (Base Mode)
This mode focuses purely on maintenance offloading. The Accelerator acts as an always-on "Super Client" dedicated to housekeeping.

- **Mechanism:**
  - The service monitors the volume's `superblock`.
  - It acquires `CleanupLease`s for `DeltaCompaction` and `SegmentCompaction` using the existing CAS mechanism.
  - It performs the compaction:
    - **Delta Compaction:** Reads delta logs, merges them into new immutable shards, and updates the superblock.
    - **Segment Compaction:** Reads small/fragmented segments, rewrites them into larger, contiguous segments, and updates the superblock.
- **Client Interaction:**
  - Clients are configured with `--disable-cleanup`.
  - Clients no longer compete for cleanup leases or spend bandwidth on maintenance.
- **Deployment:**
  - Can be deployed as a replicated service.
  - Lease acquisition ensures only one instance performs compaction at a time.

### 2. Metadata Gateway (High-Performance Mode)
To significantly lower latency, the Accelerator can act as an active metadata cache and coordinator.

- **Mechanism:**
  - **Read Path:** Clients connect to the Accelerator instead of reading metadata objects directly from object storage.
  - The Accelerator maintains a hot in-memory cache of the active inode map.
  - `getattr`, `lookup`, and `readdir` are served from RAM or SSD on cache hit.
- **Write Path:**
  - **Write-Through:** Clients send metadata mutations to the Accelerator, which sequences them and flushes to object storage in batches.
  - **Read-Repair:** Clients write directly and the Accelerator watches storage for invalidation.

## Constraints

- The Accelerator should be close to the object store to be effective as a compactor.
- It should also be close to clients if used as a metadata cache.
- The service should be able to rebuild state from object storage on startup.

## Testing Plan

1. Unit test lease acquisition and expiration logic.
2. Integration test hosted compaction against a local object-store mock.
3. Integration test gateway correctness against direct-object-store clients.
4. Failure-injection test accelerator death, restart, and fallback behavior.
