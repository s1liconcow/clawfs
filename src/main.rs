use std::env;

use anyhow::{Result, bail};

use clawfs::frontdoor::{self, DispatchAction};
#[cfg(feature = "fuse")]
use clawfs::launch;
use clawfs::nfs_mount;
use clawfs::telemetry::install_panic_hook;

fn main() -> Result<()> {
    install_panic_hook();
    let args: Vec<_> = env::args_os().collect();
    match frontdoor::dispatch(&args)? {
        DispatchAction::Handled => Ok(()),
        #[cfg(feature = "fuse")]
        DispatchAction::Mount(invocation) => {
            let invocation = *invocation;
            launch::run_mount_entry(invocation.config, &args, Some(&invocation.hosted))
        }
        #[cfg(not(feature = "fuse"))]
        DispatchAction::Mount(_) => {
            bail!("FUSE support is not available in this build. Use --transport nfs instead.")
        }
        DispatchAction::NfsMount(invocation) => nfs_mount::run_nfs_mount(*invocation),
        DispatchAction::FallThrough => {
            if let Some(hint) = frontdoor::manual_cli_hint(&args) {
                bail!("{hint}");
            }
            frontdoor::print_general_help();
            Ok(())
        }
    }
}
