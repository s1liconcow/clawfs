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

crate::clawfs_define_maintenance_module!();

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::CleanupPolicy;
    use crate::config::Config;

    fn base_config() -> Config {
        Config::with_paths(
            PathBuf::from("/tmp/mnt"),
            PathBuf::from("/tmp/store"),
            PathBuf::from("/tmp/cache"),
            PathBuf::from("/tmp/state"),
        )
    }

    #[test]
    fn cleanup_policy_no_accelerator_is_local() {
        let config = base_config();
        assert_eq!(CleanupPolicy::from_config(&config), CleanupPolicy::Local);
        assert!(CleanupPolicy::Local.should_spawn_local_worker());
    }

    #[test]
    fn cleanup_policy_remains_local_with_default_config() {
        let config = base_config();
        assert_eq!(CleanupPolicy::from_config(&config), CleanupPolicy::Local);
        assert!(CleanupPolicy::from_config(&config).should_spawn_local_worker());
    }
}
