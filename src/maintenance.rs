#[macro_use]
#[path = "maintenance_shared.inc.rs"]
mod maintenance_shared;

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

maintenance_shared_items!();
