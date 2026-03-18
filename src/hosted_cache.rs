//! Advisory hosted metadata cache client for direct_plus_cache mode.
//!
//! When a managed volume is configured with `direct_plus_cache`, clients can
//! query a near-bucket metadata cache service to serve hot inode lookups and
//! directory listings without a full object-store fetch.
//!
//! Design invariants:
//! - **Advisory only**: every cache result is validated against a minimum
//!   generation.  Stale entries (generation < min) are discarded and the
//!   caller falls back to the object store.
//! - **Graceful degradation**: any network error, HTTP error, or parse failure
//!   causes the function to return `None`.  The caller must not treat a cache
//!   miss as an error.
//! - **In-process LRU**: a small in-process LRU avoids redundant HTTP round-
//!   trips for recently fetched entries.  Entries age out after a short TTL
//!   so that invalidation events are reflected quickly even if the subscriber
//!   lags.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::inode::InodeRecord;

// ── Public types ──────────────────────────────────────────────────────────────

/// A metadata cache entry returned by the hosted cache service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedMetadataEntry {
    pub inode: u64,
    pub record: InodeRecord,
    /// Committed generation when this entry was last written to the cache.
    pub generation: u64,
}

/// A single child entry from a cached directory listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedDirEntry {
    pub name: String,
    pub inode: u64,
}

/// Configuration for the in-process hosted metadata cache client.
#[derive(Debug, Clone)]
pub struct MetadataCacheConfig {
    /// HTTP base URL of the hosted cache service (no trailing slash).
    pub cache_endpoint: String,
    /// Maximum number of entries to keep in the in-process LRU.
    pub max_entries: usize,
    /// Duration before an in-process LRU entry is considered stale and
    /// re-fetched from the hosted service.
    pub ttl: Duration,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            cache_endpoint: String::new(),
            max_entries: 2048,
            ttl: Duration::from_secs(5),
        }
    }
}

// ── Internal LRU entry ────────────────────────────────────────────────────────

pub(crate) struct LocalEntry {
    pub(crate) entry: CachedMetadataEntry,
    pub(crate) cached_at: Instant,
}

// ── HostedMetadataCache ───────────────────────────────────────────────────────

/// Advisory hosted metadata cache client.
///
/// Wraps an HTTP client that queries a managed cache service for inode records.
/// All results are validated against the caller-supplied minimum generation:
/// entries older than `min_generation` are discarded and the caller falls back
/// to the object store.
///
/// Any failure (network, HTTP error, generation mismatch) silently returns
/// `None` so the caller always falls back to the authoritative object store.
pub struct HostedMetadataCache {
    config: MetadataCacheConfig,
    client: reqwest::Client,
    /// In-process LRU keyed by inode number.
    pub(crate) local: RwLock<lru::LruCache<u64, LocalEntry>>,
}

impl HostedMetadataCache {
    /// Build a new cache client from the given configuration.
    pub fn new(config: MetadataCacheConfig) -> Arc<Self> {
        let capacity = NonZeroUsize::new(config.max_entries.max(1)).unwrap();
        Arc::new(Self {
            client: reqwest::Client::new(),
            local: RwLock::new(lru::LruCache::new(capacity)),
            config,
        })
    }

    /// Build from an accelerator endpoint URL.  Uses default TTL and capacity.
    pub fn from_endpoint(endpoint: impl Into<String>) -> Arc<Self> {
        Self::new(MetadataCacheConfig {
            cache_endpoint: endpoint.into(),
            ..MetadataCacheConfig::default()
        })
    }

    // ── Lookup ────────────────────────────────────────────────────────────────

    /// Try to look up an inode record from the hosted cache.
    ///
    /// Returns `None` when:
    /// - The entry is not in the hosted cache.
    /// - The cached entry's `generation` is older than `min_generation`.
    /// - The hosted service is unavailable or returns an error.
    pub async fn get_inode(&self, inode: u64, min_generation: u64) -> Option<CachedMetadataEntry> {
        // Fast path: check the in-process LRU before making an HTTP request.
        {
            let local = self.local.read();
            if let Some(e) = local.peek(&inode)
                && e.cached_at.elapsed() <= self.config.ttl
                && e.entry.generation >= min_generation
            {
                debug!(
                    inode,
                    generation = e.entry.generation,
                    "hosted_cache_local_hit"
                );
                return Some(e.entry.clone());
            }
        }

        // Slow path: fetch from the hosted cache service.
        let cached = match self.fetch_inode(inode).await {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                debug!(inode, "hosted_cache_miss");
                return None;
            }
            Err(err) => {
                warn!(inode, err = %err, "hosted_cache_fetch_error");
                return None;
            }
        };

        if cached.generation < min_generation {
            debug!(
                inode,
                cached_generation = cached.generation,
                min_generation,
                "hosted_cache_stale_discarded"
            );
            return None;
        }

        let entry = cached.clone();
        info!(inode, generation = entry.generation, "hosted_cache_hit");
        // Store in the in-process LRU so subsequent requests are served locally.
        self.local.write().put(
            inode,
            LocalEntry {
                entry: cached,
                cached_at: Instant::now(),
            },
        );
        Some(entry)
    }

    /// Try to get a cached directory listing for `parent_inode`.
    ///
    /// Returns the list of (name, child_inode) pairs, or `None` on any error
    /// or cache miss.  The caller should treat a `None` result as "not cached"
    /// and fall back to its normal directory-loading path.
    pub async fn get_readdir(&self, parent_inode: u64) -> Option<Vec<CachedDirEntry>> {
        let url = format!(
            "{}/readdir/{}",
            self.config.cache_endpoint.trim_end_matches('/'),
            parent_inode
        );
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    return None;
                }
                match resp.error_for_status().map_err(anyhow::Error::from) {
                    Err(err) => {
                        warn!(parent_inode, err = %err, "hosted_cache_readdir_error");
                        None
                    }
                    Ok(resp) => match resp.json::<Vec<CachedDirEntry>>().await {
                        Ok(entries) => {
                            debug!(
                                parent_inode,
                                count = entries.len(),
                                "hosted_cache_readdir_hit"
                            );
                            Some(entries)
                        }
                        Err(err) => {
                            warn!(parent_inode, err = %err, "hosted_cache_readdir_parse_error");
                            None
                        }
                    },
                }
            }
            Err(err) => {
                warn!(parent_inode, err = %err, "hosted_cache_readdir_fetch_error");
                None
            }
        }
    }

    // ── Invalidation ──────────────────────────────────────────────────────────

    /// Evict specific inodes from the in-process LRU.
    ///
    /// Called when `CoordinationSubscriber` receives an `InvalidationEvent`
    /// with `InvalidationScope::Inodes(...)`.
    pub fn invalidate_inodes(&self, inodes: &[u64]) {
        if inodes.is_empty() {
            return;
        }
        let mut local = self.local.write();
        for &inode in inodes {
            local.pop(&inode);
        }
        debug!(count = inodes.len(), "hosted_cache_inodes_invalidated");
    }

    /// Flush the entire in-process LRU.
    ///
    /// Called when `CoordinationSubscriber` receives a full invalidation or a
    /// gap in the event sequence (requiring a complete metadata refresh).
    pub fn invalidate_all(&self) {
        self.local.write().clear();
        debug!("hosted_cache_all_invalidated");
    }

    /// Number of entries currently held in the in-process LRU.
    pub fn local_len(&self) -> usize {
        self.local.read().len()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    async fn fetch_inode(&self, inode: u64) -> anyhow::Result<Option<CachedMetadataEntry>> {
        let url = format!(
            "{}/inode/{}",
            self.config.cache_endpoint.trim_end_matches('/'),
            inode
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(anyhow::Error::from)
            .map_err(|e| e.context("hosted cache: GET inode request failed"))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let entry = resp
            .error_for_status()
            .map_err(anyhow::Error::from)
            .map_err(|e| e.context("hosted cache: error status"))?
            .json::<CachedMetadataEntry>()
            .await
            .map_err(anyhow::Error::from)
            .map_err(|e| e.context("hosted cache: failed to decode inode entry"))?;
        Ok(Some(entry))
    }
}

// ── Segment cache ─────────────────────────────────────────────────────────────

/// Configuration for the hosted near-bucket segment cache.
#[derive(Debug, Clone)]
pub struct SegmentCacheConfig {
    /// HTTP base URL of the segment cache service (no trailing slash).
    pub cache_endpoint: String,
    /// Maximum segment range size that will be fetched from the hosted cache.
    /// Ranges above this threshold bypass the cache entirely.
    pub max_segment_bytes: u64,
    /// Whether the segment cache is enabled.
    pub enabled: bool,
}

impl Default for SegmentCacheConfig {
    fn default() -> Self {
        Self {
            cache_endpoint: String::new(),
            max_segment_bytes: 32 * 1024 * 1024, // 32 MiB
            enabled: false,
        }
    }
}

/// Advisory near-bucket hot-segment cache client.
///
/// Segments are immutable by identity (`generation`, `segment_id`).  A cached
/// entry keyed by `(generation, segment_id, offset, length)` is always current
/// — no active invalidation is required.  Cache misses and errors silently
/// return `None`; the caller falls back to the authoritative object store.
pub struct HostedSegmentCache {
    config: SegmentCacheConfig,
    client: reqwest::Client,
}

impl HostedSegmentCache {
    pub fn new(config: SegmentCacheConfig) -> Arc<Self> {
        Arc::new(Self {
            client: reqwest::Client::new(),
            config,
        })
    }

    /// Build from an accelerator endpoint URL with default limits.
    pub fn from_endpoint(endpoint: impl Into<String>) -> Arc<Self> {
        Self::new(SegmentCacheConfig {
            cache_endpoint: endpoint.into(),
            enabled: true,
            ..SegmentCacheConfig::default()
        })
    }

    /// Try to fetch a segment byte range from the hosted cache.
    ///
    /// The URL template is `GET /segment/{gen}/{seg_id}/{offset}+{length}`.
    /// Returns `None` on any miss, size limit exceeded, or network/HTTP error.
    pub async fn get_segment(
        &self,
        generation: u64,
        segment_id: u64,
        offset: u64,
        length: u64,
    ) -> Option<bytes::Bytes> {
        if !self.config.enabled {
            return None;
        }
        if length > self.config.max_segment_bytes {
            tracing::debug!(
                generation,
                segment_id,
                length,
                max = self.config.max_segment_bytes,
                "segment_cache_bypass_too_large"
            );
            return None;
        }

        let url = format!(
            "{}/segment/{}/{}/{}+{}",
            self.config.cache_endpoint.trim_end_matches('/'),
            generation,
            segment_id,
            offset,
            length,
        );

        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    tracing::debug!(generation, segment_id, offset, length, "segment_cache_miss");
                    return None;
                }
                match resp.error_for_status().map_err(anyhow::Error::from) {
                    Ok(r) => match r.bytes().await {
                        Ok(data) => {
                            tracing::info!(
                                generation,
                                segment_id,
                                offset,
                                length,
                                bytes = data.len(),
                                "segment_cache_hit"
                            );
                            Some(data)
                        }
                        Err(err) => {
                            tracing::warn!(
                                generation,
                                segment_id,
                                err = %err,
                                "segment_cache_body_error"
                            );
                            None
                        }
                    },
                    Err(err) => {
                        tracing::warn!(
                            generation,
                            segment_id,
                            err = %err,
                            "segment_cache_http_error"
                        );
                        None
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    generation,
                    segment_id,
                    err = %err,
                    "segment_cache_fetch_error"
                );
                None
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn max_segment_bytes(&self) -> u64 {
        self.config.max_segment_bytes
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{CachedMetadataEntry, HostedMetadataCache, MetadataCacheConfig};
    use crate::inode::InodeRecord;

    fn dummy_record(inode: u64) -> InodeRecord {
        InodeRecord::new_file(
            inode,
            1,
            format!("file{inode}"),
            format!("/file{inode}"),
            1000,
            1000,
        )
    }

    fn cached_entry(inode: u64, generation: u64) -> CachedMetadataEntry {
        CachedMetadataEntry {
            inode,
            record: dummy_record(inode),
            generation,
        }
    }

    #[test]
    fn invalidate_inodes_clears_local_lru() {
        let cache = HostedMetadataCache::new(MetadataCacheConfig {
            cache_endpoint: "http://localhost:19999".to_string(),
            max_entries: 16,
            ttl: Duration::from_secs(60),
        });

        // Manually populate the in-process LRU.
        {
            let mut local = cache.local.write();
            local.put(
                1,
                super::LocalEntry {
                    entry: cached_entry(1, 5),
                    cached_at: std::time::Instant::now(),
                },
            );
            local.put(
                2,
                super::LocalEntry {
                    entry: cached_entry(2, 5),
                    cached_at: std::time::Instant::now(),
                },
            );
        }
        assert_eq!(cache.local_len(), 2);

        cache.invalidate_inodes(&[1]);
        assert_eq!(cache.local_len(), 1);
        assert!(
            cache.local.read().peek(&1).is_none(),
            "inode 1 must be evicted"
        );
        assert!(cache.local.read().peek(&2).is_some(), "inode 2 must remain");
    }

    #[test]
    fn invalidate_all_clears_lru() {
        let cache = HostedMetadataCache::new(MetadataCacheConfig {
            cache_endpoint: "http://localhost:19999".to_string(),
            max_entries: 16,
            ttl: Duration::from_secs(60),
        });

        {
            let mut local = cache.local.write();
            for inode in 1..=5u64 {
                local.put(
                    inode,
                    super::LocalEntry {
                        entry: cached_entry(inode, 3),
                        cached_at: std::time::Instant::now(),
                    },
                );
            }
        }
        assert_eq!(cache.local_len(), 5);
        cache.invalidate_all();
        assert_eq!(cache.local_len(), 0);
    }

    #[test]
    fn from_endpoint_builds_with_defaults() {
        let cache = HostedMetadataCache::from_endpoint("http://cache.example.com");
        assert_eq!(cache.config.cache_endpoint, "http://cache.example.com");
        assert_eq!(cache.config.max_entries, 2048);
        assert_eq!(cache.config.ttl, Duration::from_secs(5));
    }
}
