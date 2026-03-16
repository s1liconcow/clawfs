use std::env;

use anyhow::{Result, bail};

use clawfs::frontdoor::{self, DispatchAction};
use clawfs::launch;
use clawfs::telemetry::install_panic_hook;

fn main() -> Result<()> {
    install_panic_hook();
    let args: Vec<_> = env::args_os().collect();
    match frontdoor::dispatch(&args)? {
        DispatchAction::Handled => Ok(()),
        DispatchAction::Mount(invocation) => {
            let invocation = *invocation;
            launch::run_mount_entry(invocation.config, &args, Some(&invocation.hosted))
        }
        DispatchAction::FallThrough => {
            if let Some(hint) = frontdoor::manual_cli_hint(&args) {
                bail!("{hint}");
            }
            frontdoor::print_general_help();
            Ok(())
        }
    }
}
