use std::env;

use anyhow::Result;
use clap::Parser;

use clawfs::config::{Cli, Config};
#[cfg(feature = "fuse")]
use clawfs::launch;
use clawfs::telemetry::install_panic_hook;

fn main() -> Result<()> {
    install_panic_hook();
    let args: Vec<_> = env::args_os().collect();
    #[cfg(feature = "fuse")]
    {
        let cli = Cli::parse();
        let config: Config = cli.into();
        launch::run_mount_entry(config, &args, None)
    }
    #[cfg(not(feature = "fuse"))]
    {
        anyhow::bail!(
            "FUSE support is not available in this build. Rebuild with the `fuse` feature enabled."
        )
    }
}
