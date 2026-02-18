# Cleanup Agent Deployment

OsageFS coordinates background maintenance (delta compaction and segment
rewrites) via short leases stored in the superblock. Most clients that are
physically close to the object store can keep the default cleanup worker
enabled, but in multi-region deployments you may prefer to run the maintenance
loop next to the bucket to avoid cross-region transfer. This document describes
how to disable local cleanup and run a lightweight agent in the same region as
your bucket.

## 1. Disable cleanup on remote clients

All clients accept a `--disable-cleanup` flag (or `Config::disable_cleanup` via
the library API). When set, OsageFS will skip scheduling background maintenance
and will only service foreground FUSE requests. The leases remain available so a
separate agent can continue performing compaction.

```
osagefs --mount-path /mnt/osage \
        --store-path /data/osage \
        --disable-cleanup
```

## 2. Build the cleanup agent (WASI/WASM)

Create a lightweight Rust binary that links against `osagefs` and calls the
existing cleanup helpers (`MetadataStore::prune_deltas`,
`perform_segment_compaction`). The example below shows the essential
boilerplate:

```rust
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = load_config_from_env()?; // mount_path, store_path, state_path, etc.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let metadata = Arc::new(MetadataStore::new(&config, runtime.handle().clone()).await?);
        let superblock = Arc::new(SuperblockManager::load_or_init(metadata.clone(), config.shard_size).await?);
        let segments = Arc::new(SegmentManager::new(&config, runtime.handle().clone())?);
        run_cleanup_loop(metadata, superblock, segments, client_id_from_env()).await
    })
}
```

Compile it to WASI for portability:

```
cargo build --bin osagefs-cleanup-agent --target wasm32-wasi --release
```

Upload the resulting `osagefs-cleanup-agent.wasm` to your compute provider of
choice:

* **AWS Lambda** – create a Lambda function with `Runtime = provided.al2023`,
  enable "SnapStart" for faster cold starts, and upload the WASI binary via the
  [Lambda Runtime for WASI](https://aws.amazon.com/blogs/compute/introducing-the-wasm-lambda-runtime/).
* **Cloudflare Workers / R2** – deploy the WASM module with Workers for
  Platforms and configure it with the target S3-compatible endpoint. Workers run
  close to R2 (or S3 buckets served via R2 cache) and can loop across deltas
  without leaving the region.
* **Google Cloud Run / Cloud Functions** – wrap the WASM binary in a small OCI
  container (e.g., using `wasmtime run cleanup-agent.wasm`) and deploy it to a
  regional service next to the GCS bucket.

Whichever platform you choose, make sure the agent

1. Mounts or otherwise reaches the backing object-store path (e.g., by attaching
   the same service account credentials used by OsageFS).
2. Sets `DISABLE_CLEANUP=true` on the user-facing clients so only the agent
   holds maintenance leases.
3. Runs on a schedule (e.g., Cloudflare cron trigger, AWS EventBridge rule) or as
   a continuously running service if your provider allows it.

## 3. Scheduling & monitoring

* Set the agent’s poll cadence to a small value (e.g., every 30 seconds) so it
  quickly picks up leases. Because the work happens next to the bucket, the
  bandwidth cost is minimal.
* Emit logs/metrics whenever the agent acquires a lease or prunes segments.
  Since leases are stored in the superblock, clients can also inspect the `cleanup_leases`
  array to confirm which agent is active.

With this setup, regional clients can mount OsageFS with
`--disable-cleanup` so they never upload maintenance traffic across regions,
while a small WASI agent runs in-region to keep metadata tidy.
