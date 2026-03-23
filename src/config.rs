use std::path::{Path, PathBuf};

use clap::{ArgAction, Parser, ValueEnum};

#[macro_use]
#[path = "config_shared_macros.rs"]
mod config_shared_macros;

/// Default directory name for the FUSE mount point (relative to cwd).
pub const DEFAULT_MOUNT_DIR: &str = "clawfs-mnt";

// Keep shared mount/runtime config fields in parity with private/src/config.rs.
// If a field is intentionally public-only, document that divergence in both files.

// CLI configuration for launching ClawFS.
shared_cli_struct! {
    /// Enable FUSE writeback cache (enabled by default).
    #[arg(long, default_value_t = true)]
    pub writeback_cache: bool,
}

shared_config_struct! {}

impl Config {
    pub fn with_paths(
        mount_path: PathBuf,
        store_path: PathBuf,
        local_cache_path: PathBuf,
        state_path: PathBuf,
    ) -> Self {
        shared_config_with_paths_expr!(mount_path, store_path, local_cache_path, state_path, true,)
    }
}

shared_source_store_config_struct!();

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        let config_root = default_user_config_root();
        let state_path = cli
            .state_path
            .clone()
            .unwrap_or_else(|| default_state_path(&config_root));
        let log_file = cli
            .log_file
            .clone()
            .or_else(|| Some(PathBuf::from("clawfs.log")));
        let store_path = cli.store_path;
        let local_cache_path = cli
            .local_cache_path
            .clone()
            .unwrap_or_else(|| default_local_cache_path(&config_root));
        let source = shared_source_from_cli_expr!(cli);

        shared_config_from_cli_expr!(
            cli,
            source,
            log_file,
            store_path,
            local_cache_path,
            state_path,
        )
    }
}

shared_default_path_helpers!();
shared_object_store_provider_enum!();

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Config};

    #[test]
    fn cli_config_defaults_use_local_provider() {
        let config = Config::from(Cli::parse_from([
            "clawfs",
            "--mount-path",
            "/tmp/clawfs-mnt",
            "--store-path",
            "/tmp/clawfs-store",
        ]));

        assert_eq!(config.object_provider, super::ObjectStoreProvider::Local);
    }
}
