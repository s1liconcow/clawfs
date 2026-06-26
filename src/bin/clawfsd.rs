#[cfg(feature = "fuse")]
use std::env;

use anyhow::Result;
#[cfg(feature = "fuse")]
use clap::Parser;

#[cfg(feature = "fuse")]
use clawfs::config::{Cli, Config};

fn main() -> Result<()> {
    #[cfg(feature = "fuse")]
    {
        use clawfs::launch;
        let args: Vec<_> = env::args_os().collect();
        let cli = Cli::parse();
        let config: Config = cli.into();
        launch::run_mount_entry(config, &args)
    }
    #[cfg(not(feature = "fuse"))]
    {
        anyhow::bail!(
            "clawfsd requires FUSE support. This build was compiled without the `fuse` feature."
        )
    }
}
