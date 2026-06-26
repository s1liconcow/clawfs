#[cfg(feature = "fuse")]
use std::env;

use anyhow::Result;
#[cfg(feature = "fuse")]
use clap::Parser;

#[cfg(feature = "fuse")]
use clawfs::config::{Cli, Config};
#[cfg(feature = "fuse")]
use clawfs::launch;
use clawfs::telemetry::install_panic_hook;

fn main() -> Result<()> {
    install_panic_hook();
    #[cfg(feature = "fuse")]
    {
        let args: Vec<_> = env::args_os().collect();
        let cli = Cli::parse();
        let config: Config = cli.into();
        launch::run_mount_entry(config, &args)
    }
    #[cfg(not(feature = "fuse"))]
    {
        anyhow::bail!(
            "FUSE support is not available in this build. Rebuild with the `fuse` feature enabled."
        )
    }
}
