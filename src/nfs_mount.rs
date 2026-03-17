use std::fs;
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::frontdoor::{NfsMountInvocation, object_provider_name, resolve_gateway_binary};

const GATEWAY_READY_TIMEOUT: Duration = Duration::from_secs(15);
const GATEWAY_READY_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NfsMountState {
    pub gateway_pid: u32,
    pub port: u16,
    pub mount_path: String,
    pub volume: String,
}

pub fn run_nfs_mount(invocation: NfsMountInvocation) -> Result<()> {
    let gateway = resolve_gateway_binary()?;
    let port = pick_ephemeral_port()?;
    let listen_addr = format!("127.0.0.1:{port}");
    let mount_path_str = invocation
        .mount_path
        .to_str()
        .context("mount path is not valid UTF-8")?
        .to_string();

    let gateway_dummy_mount = if cfg!(windows) {
        std::env::temp_dir()
            .join("clawfs-nfs-mnt")
            .to_string_lossy()
            .into_owned()
    } else {
        "/tmp/clawfs-nfs-mnt".to_string()
    };

    // Build gateway command
    let mut cmd = Command::new(&gateway);
    cmd.arg("--mount-path")
        .arg(&gateway_dummy_mount)
        .arg("--store-path")
        .arg(&invocation.volume_paths.store_path)
        .arg("--local-cache-path")
        .arg(&invocation.volume_paths.cache_path)
        .arg("--state-path")
        .arg(&invocation.volume_paths.nfs_state_path)
        .arg("--listen")
        .arg(&listen_addr)
        .arg("--protocol")
        .arg("v3")
        .arg("--object-provider")
        .arg(object_provider_name(invocation.hosted.config.provider))
        .arg("--bucket")
        .arg(&invocation.hosted.config.bucket);

    if let Some(ref region) = invocation.hosted.config.region {
        cmd.arg("--region").arg(region);
    }
    if let Some(ref endpoint) = invocation.hosted.config.endpoint {
        cmd.arg("--endpoint").arg(endpoint);
    }
    if let Some(ref object_prefix) = invocation.hosted.config.object_prefix {
        cmd.arg("--object-prefix").arg(object_prefix);
    }
    if let Some(ref access_key_id) = invocation.hosted.config.access_key_id {
        cmd.env("AWS_ACCESS_KEY_ID", access_key_id);
    }
    if let Some(ref secret_access_key) = invocation.hosted.config.secret_access_key {
        cmd.env("AWS_SECRET_ACCESS_KEY", secret_access_key);
    }

    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    #[cfg(unix)]
    if !invocation.foreground {
        use std::os::unix::process::CommandExt;
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS
        cmd.creation_flags(0x00000200 | 0x00000008);
    }

    let mut child = cmd.spawn().context("failed to spawn NFS gateway")?;
    let gateway_pid = child.id();

    // Wait for gateway to be ready
    if let Err(err) = wait_for_ready(&mut child, &listen_addr) {
        // Clean up on failure
        let _ = child.kill();
        let _ = child.wait();
        return Err(err);
    }

    // Perform NFS mount
    if let Err(err) = perform_nfs_mount(port, &invocation.mount_path) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(err);
    }

    // Write state file
    let state = NfsMountState {
        gateway_pid,
        port,
        mount_path: mount_path_str.clone(),
        volume: invocation.hosted.volume_slug.clone(),
    };
    let state_json = serde_json::to_string_pretty(&state).context("serializing NFS mount state")?;
    fs::write(&invocation.volume_paths.nfs_mount_info_path, &state_json)
        .context("writing NFS mount state file")?;

    if invocation.foreground {
        println!("Mounted at {} (NFS, port {port})", mount_path_str);
        // Block on the gateway; clean up on exit
        let status = child.wait().context("waiting on NFS gateway")?;
        // Gateway exited — unmount
        let _ = run_umount_cmd(&invocation.mount_path);
        let _ = fs::remove_file(&invocation.volume_paths.nfs_mount_info_path);
        if !status.success() {
            bail!("NFS gateway exited with status {status}");
        }
    } else {
        println!(
            "Mounted at {} (NFS, port {port}, gateway pid {gateway_pid})",
            mount_path_str
        );
    }

    Ok(())
}

fn pick_ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("binding ephemeral port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn wait_for_ready(child: &mut std::process::Child, addr: &str) -> Result<()> {
    let deadline = Instant::now() + GATEWAY_READY_TIMEOUT;

    while Instant::now() < deadline {
        // Check if the child exited early
        if let Some(status) = child.try_wait()? {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                use std::io::Read;
                let _ = pipe.read_to_string(&mut stderr);
            }
            bail!(
                "NFS gateway exited before becoming ready (status {status}): {}",
                stderr.trim()
            );
        }

        if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(100)).is_ok() {
            return Ok(());
        }

        std::thread::sleep(GATEWAY_READY_POLL_INTERVAL);
    }

    bail!("NFS gateway did not become ready within {GATEWAY_READY_TIMEOUT:?}");
}

fn perform_nfs_mount(port: u16, mount_path: &std::path::Path) -> Result<()> {
    let status = if cfg!(target_os = "macos") {
        let nfs_opts = format!("port={port},mountport={port},vers=3,tcp,nolock,noacl");
        Command::new("mount_nfs")
            .arg("-o")
            .arg(&nfs_opts)
            .arg("127.0.0.1:/clawfs")
            .arg(mount_path)
            .status()
    } else if cfg!(target_os = "windows") {
        // Windows NFS client mounts via `mount` from Services for NFS.
        // The mount_path should be a drive letter like "Z:" or a UNC-style path.
        Command::new("mount")
            .arg("-o")
            .arg(format!("port={port},mtype=soft,lang=ansi"))
            .arg("\\\\127.0.0.1\\clawfs")
            .arg(mount_path)
            .status()
    } else {
        let nfs_opts = format!("port={port},mountport={port},vers=3,tcp,nolock,noacl");
        Command::new("mount")
            .arg("-t")
            .arg("nfs")
            .arg("-o")
            .arg(&nfs_opts)
            .arg("127.0.0.1:/clawfs")
            .arg(mount_path)
            .status()
    };

    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => {
            let hint = if cfg!(target_os = "windows") {
                "\nHint: Ensure Windows Services for NFS (NFS Client) is enabled. Run as Administrator or use `clawfs serve` for manual NFS setup."
            } else if s.code() == Some(32) || s.code() == Some(1) {
                "\nHint: NFS mount typically requires root. Try `sudo clawfs mount` or use `clawfs serve` for manual NFS setup."
            } else {
                ""
            };
            bail!("NFS mount failed (exit status {s}){hint}");
        }
        Err(err) => {
            let hint = if cfg!(target_os = "windows") {
                "\nHint: Ensure Windows Services for NFS (NFS Client) is enabled. Run as Administrator or use `clawfs serve` for manual NFS setup."
            } else {
                "\nHint: NFS mount typically requires root. Try `sudo clawfs mount` or use `clawfs serve` for manual NFS setup."
            };
            bail!("failed to run mount command: {err}{hint}");
        }
    }
}

pub fn run_umount_cmd(mount_path: &std::path::Path) -> Result<()> {
    let status = if cfg!(target_os = "macos") {
        Command::new("diskutil")
            .arg("unmount")
            .arg(mount_path)
            .status()
    } else {
        // `umount` works on both Linux and Windows (Services for NFS)
        Command::new("umount").arg(mount_path).status()
    };

    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => bail!("umount failed with status {s}"),
        Err(err) => bail!("failed to run umount: {err}"),
    }
}

/// Terminate a process by PID. On Unix uses SIGTERM then SIGKILL; on Windows
/// uses `taskkill`.
pub fn kill_gateway(pid: u32) {
    #[cfg(unix)]
    {
        unsafe {
            if libc::kill(pid as libc::pid_t, libc::SIGTERM) == 0 {
                for _ in 0..50 {
                    if libc::kill(pid as libc::pid_t, 0) != 0 {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if libc::kill(pid as libc::pid_t, 0) == 0 {
                    libc::kill(pid as libc::pid_t, libc::SIGKILL);
                }
            }
        }
    }

    #[cfg(windows)]
    {
        // taskkill /PID <pid> /F
        let _ = Command::new("taskkill")
            .arg("/PID")
            .arg(pid.to_string())
            .arg("/F")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
