use std::env;

use anyhow::Result;
use clap::Parser;

use clawfs::config::{Cli, Config};
use clawfs::launch;

fn main() -> Result<()> {
    let args: Vec<_> = env::args_os().collect();
    let cli = Cli::parse();
    let config: Config = cli.into();
    launch::run_mount_entry(config, &args, None)
}
