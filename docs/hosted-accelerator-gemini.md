# Hosted Accelerator for ClawFS

## Overview
The **ClawFS Hosted Accelerator** is a managed service designed to run alongside a volume's object storage backend. Its primary purpose is to lower latency for connected clients and offload critical maintenance operations from the client-side drivers to a centralized, cloud-native service.

## Goals
1.  **Lower Latency:** Reduce metadata operation latency by centralizing state and caching frequent lookups closer to the data or the client.
2.  **Offload Maintenance:** Move compaction (delta merging, segment rewriting) and garbage collection tasks from distributed clients to a dedicated, high-availability service. This simplifies client configuration (`--disable-cleanup`) and reduces client resource usage.

## Architecture

The Accelerator operates in two modes, which can be enabled incrementally:

### 1. Compaction Service (Base Mode)
This mode focuses purely on maintenance offloading. The Accelerator acts as an always-on "Super Client" dedicated to housekeeping.

-   **Mechanism:**
    -   The service monitors the volume's `superblock`.
    -   It acquires `CleanupLease`s for `DeltaCompaction` and `SegmentCompaction` using the existing CAS mechanism.
    -   It performs the compaction:
        -   **Delta Compaction:** Reads delta logs, merges them into new immutable shards, and updates the superblock.
        -   **Segment Compaction:** Reads small/fragmented segments, rewrites them into larger, contiguous segments, and updates the superblock.
-   **Client Interaction:**
    -   Clients are configured with `--disable-cleanup`.
    -   Clients no longer compete for cleanup leases or spend bandwidth on maintenance.
-   **Deployment:**
    -   Can be deployed as a replicated service (e.g., Kubernetes Deployment).
    -   Lease acquisition ensures only one instance performs compaction at a time (Leader Election via Object Store).

### 2. Metadata Gateway (High-Performance Mode)
To significantly lower latency, the Accelerator can act as an active metadata cache and coordinator.

-   **Mechanism:**
    -   **Read Path:** Clients connect to the Accelerator via gRPC instead of reading metadata objects directly from S3.
        -   The Accelerator maintains a hot in-memory cache of the active Inode Map.
        -   `GetAttr`, `Lookup`, and `ReadDir` are served from RAM/SSD, avoiding S3 round-trips.
    -   **Write Path (Options):**
        -   *Write-Through (Recommended):* Clients send metadata mutations (create, unlink, setattr) to the Accelerator. The Accelerator sequences them, applies them to its state, and flushes to S3 in batches. This provides strong consistency and conflict resolution.
        -   *Read-Repair:* Clients write to S3 directly. The Accelerator watches S3 and invalidates/updates its cache. (Higher latency for read-after-write).
-   **Protocol:**
    -   gRPC over TLS.
    -   Protobuf definitions for Metadata operations.

## Workflow

1.  **Provisioning:**
    -   Administrator provisions a ClawFS Volume.
    -   Administrator enables "Hosted Accelerator" in the control plane.
    -   The Accelerator service is spun up in the same cloud region as the bucket.

2.  **Client Configuration:**
    -   Clients are provided with an Accelerator Endpoint (e.g., `accelerator.clawfs.example.com:443`).
    -   Clients mount with:
        ```bash
        clawfs mount --bucket my-bucket --accelerator-endpoint https://accelerator.clawfs.example.com --disable-cleanup /mnt/point
        ```

3.  **Operation:**
    -   **Data Path:** Reads/Writes of file content (segments) continue to go directly to/from S3 for maximum throughput and scalability.
    -   **Metadata Path:** Metadata operations are routed to the Accelerator.
    -   **Maintenance:** The Accelerator silently optimizes the storage layout in the background.

## Constraints

-   **Network Proximity:** The Accelerator must be close to the Object Store (same region) to be effective as a Compactor. It should be close to Clients to be effective as a Metadata Cache. (Ideally, deploy in the same region as the workload).
-   **Statelessness:** The Accelerator should be designed to be largely stateless, rebuilding its state from S3 on startup. However, for performance, it may maintain a local RocksDB or LMDB cache.

## Tradeoffs

| Feature | Pros | Cons |
| :--- | :--- | :--- |
| **Compaction Service** | Simple to implement; immediate client CPU/Network savings; no protocol changes. | Does not drastically improve *read* latency for clients (only indirect benefit from cleaner metadata). |
| **Metadata Gateway** | Massive latency reduction for metadata; enables strong consistency/locking. | Adds a critical path component; Single Point of Failure (SPOF) unless HA is complex; Client fallback logic required. |
| **Cost** | | Running a continuously available service is more expensive than serverless S3 requests. |

## Testing Plan

1.  **Unit Testing:**
    -   Isolate the compaction logic from `src/launch.rs` into a library (`clawfs-lib`).
    -   Test lease acquisition and expiration logic.

2.  **Integration Testing:**
    -   **Compaction:**
        -   Start a `clawfs-accelerator` process pointing to a local `s3-mock` bucket.
        -   Run a standard client workload with `--disable-cleanup`.
        -   Verify that delta files are compacted and segments are merged by the accelerator.
    -   **Gateway:**
        -   Implement a basic gRPC client/server.
        -   Verify metadata correctness between direct-S3 client and Gateway-connected client.

3.  **Failure Handling (Chaos Testing):**
    -   **Kill Accelerator:** Kill the accelerator process during a compaction. Verify the lease expires and another instance (or a client) can pick it up.
    -   **Network Partition:** Isolate the accelerator. Verify clients fall back to direct S3 access (if implemented) or error gracefully.
