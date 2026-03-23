macro_rules! maintenance_cleanup_policy {
    () => {
        /// Determines whether the local cleanup worker should run.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum CleanupPolicy {
            Local,
        }

        impl CleanupPolicy {
            pub const fn as_str(self) -> &'static str {
                "local"
            }

            pub const fn should_spawn_local_worker(self) -> bool {
                true
            }

            pub fn from_config(config: &crate::config::Config) -> Self {
                let _ = config;
                Self::Local
            }
        }
    };
}

macro_rules! maintenance_segment_delete_block {
    ($segments:ident, $segments_to_delete:ident) => {
        // Delete old segments only AFTER the superblock CAS succeeds.
        // If we deleted before and the CAS failed, metadata would still reference
        // the now-deleted segments, causing permanent data corruption (404 on read).
        for (old_gen, seg_id) in $segments_to_delete {
            if let Err(err) = $segments.delete_segment(old_gen, seg_id) {
                warn!(
                    target: "maintenance",
                    generation = old_gen,
                    segment_id = seg_id,
                    error = %err,
                    "failed to delete old segment after compaction; will be retried"
                );
            }
        }
    };
}

#[cfg(test)]
macro_rules! maintenance_test_imports {
    () => {};
}

#[cfg(test)]
macro_rules! maintenance_cleanup_policy_tests {
    () => {
        #[test]
        fn cleanup_policy_remains_local_with_default_config() {
            let config = base_config();
            assert_eq!(CleanupPolicy::from_config(&config), CleanupPolicy::Local);
            assert!(CleanupPolicy::from_config(&config).should_spawn_local_worker());
        }
    };
}

include!("maintenance_shared.inc.rs");
