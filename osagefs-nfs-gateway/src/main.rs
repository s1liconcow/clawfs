use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use tokio_util::sync::CancellationToken;
use zerofs_nfsserve::nfs::{
    FSF_CANSETTIME, FSF_HOMOGENEOUS, FSF_LINK, FSF_SYMLINK, fattr3, fileid3, filename3, fsinfo3,
    fsstat3, ftype3, nfspath3, nfsstat3, nfstime3, post_op_attr, sattr3, set_gid3, set_mode3,
    set_size3, set_uid3, specdata3, writeverf3, NFS3_WRITEVERFSIZE,
};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{AuthContext, DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use osagefs::config::{Config, ObjectStoreProvider};
use osagefs::fs::OsageFs;
use osagefs::inode::{FileStorage, InodeKind, InodeRecord, ROOT_INODE};
use osagefs::journal::JournalManager;
use osagefs::metadata::MetadataStore;
use osagefs::segment::SegmentManager;
use osagefs::state::ClientStateManager;
use osagefs::superblock::SuperblockManager;
use tempfile::NamedTempFile;
use tokio::process::Command;
use tokio::signal;
use tokio::task;
use tracing::info;

const ROOT_ID: fileid3 = ROOT_INODE;
const DEFAULT_V4_EXPORT: &str = "/osagefs";
const DEFAULT_GANESHA_BIN: &str = "ganesha.nfsd";
const WELCOME_FILENAME: &str = "WELCOME.txt";
const WELCOME_CONTENT: &str = "Welcome to OsageFS!\n\
\n\
OsageFS is a log-structured, object-store-backed filesystem designed for fast,\n\
shared access to large working sets with durable metadata and batched writes.\n\
\n\
Great use cases:\n\
- AI training data and model artifacts shared across multiple machines\n\
- Shared home directories for teams, labs, or ephemeral compute nodes\n\
- High-throughput team access to large binaries, build outputs, and datasets\n\
\n\
Why teams use it:\n\
- Immutable segment writes for efficient object-store IO\n\
- Batched metadata updates for lower API overhead\n\
- Local staging, caching, and journal replay for practical durability and speed\n\
\n\
Enjoy building on OsageFS.\n";

#[derive(Parser, Debug)]
struct Cli {
    /// Legacy path used for NFSv4/ganesha exports and as default state-path base.
    #[arg(long, value_name = "PATH", default_value = "/tmp/osagefs-mnt")]
    mount_path: PathBuf,

    /// Path for OsageFS object/store data.
    #[arg(long, value_name = "PATH", default_value = "/tmp/osagefs-store")]
    store_path: PathBuf,

    /// Path for local cache/journal files used by the direct OsageFS backend.
    #[arg(long, value_name = "PATH")]
    local_cache_path: Option<PathBuf>,

    /// Re-export an already mounted path (only valid with --protocol v4).
    #[arg(long, default_value_t = false)]
    use_existing_mount: bool,

    /// Deprecated in direct mode; retained for compatibility.
    #[arg(long)]
    osagefs_binary: Option<PathBuf>,

    /// Object store provider for direct OsageFS backend.
    #[arg(long, value_enum, default_value_t = ObjectStoreProvider::Local)]
    object_provider: ObjectStoreProvider,

    /// Bucket name for AWS/GCS providers.
    #[arg(long)]
    bucket: Option<String>,

    /// Region for AWS-compatible providers.
    #[arg(long)]
    region: Option<String>,

    /// Custom endpoint for AWS-compatible providers.
    #[arg(long)]
    endpoint: Option<String>,

    /// Prefix within the bucket/object store for OsageFS.
    #[arg(long, default_value = "")]
    object_prefix: String,

    /// Optional Google Cloud service account JSON file.
    #[arg(long, value_name = "PATH")]
    gcs_service_account: Option<PathBuf>,

    /// Optional state path for OsageFS client identity/allocation pools.
    #[arg(long, value_name = "PATH")]
    state_path: Option<PathBuf>,

    /// Disable close-time journal in direct mode (higher throughput, lower crash durability).
    #[arg(long, default_value_t = true)]
    disable_journal: bool,

    /// Address for the user-mode NFS server to bind to (ip:port).
    #[arg(long, default_value = "0.0.0.0:2049")]
    listen: String,

    /// Export pseudo path (used when generating the NFSv4 Ganesha config).
    #[arg(long, default_value = DEFAULT_V4_EXPORT)]
    pseudo_path: String,

    /// Run the gateway in read-only mode.
    #[arg(long, default_value_t = false)]
    read_only: bool,

    /// Which backend to run (direct user-mode v3 or ganesha-backed v4).
    #[arg(long, value_enum, default_value_t = Protocol::V3)]
    protocol: Protocol,

    /// Optional path to ganesha.nfsd when running with --protocol v4.
    #[arg(long)]
    ganesha_binary: Option<PathBuf>,

    /// Optional custom log path for ganesha.nfsd.
    #[arg(long)]
    ganesha_log: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
enum Protocol {
    V3,
    V4,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing();

    match cli.protocol {
        Protocol::V3 => run_user_mode(cli).await,
        Protocol::V4 => run_ganesha(cli).await,
    }
}

fn init_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn run_user_mode(cli: Cli) -> Result<()> {
    if cli.use_existing_mount {
        return Err(anyhow!(
            "--use-existing-mount is not supported with --protocol v3; direct mode serves OsageFS without FUSE"
        ));
    }

    let addr: SocketAddr = cli
        .listen
        .parse()
        .context("invalid listen address; expected ip:port")?;
    let backend = OsageDirectFs::new(&cli).await?;

    let listener = NFSTcpListener::bind(addr, backend.clone())
        .await
        .context("failed to bind NFS listener")?;
    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();

    info!(
        target: "gateway",
        protocol = "nfs3",
        listen = %cli.listen,
        store = %cli.store_path.display(),
        "serving direct OsageFS over user-mode NFS"
    );

    tokio::select! {
        res = listener.handle_with_shutdown(shutdown.clone()) => {
            res.context("NFS server stopped unexpectedly")?
        }
        _ = signal::ctrl_c() => {
            info!(target: "gateway", "received ctrl_c, shutting down user-mode NFS");
            shutdown_signal.cancel();
        }
    }

    backend.flush().await;
    Ok(())
}

async fn run_ganesha(cli: Cli) -> Result<()> {
    if !cli.use_existing_mount {
        return Err(anyhow!(
            "--protocol v4 requires --use-existing-mount because ganesha exports a real POSIX path"
        ));
    }

    let addr: SocketAddr = cli
        .listen
        .parse()
        .context("invalid listen address; expected ip:port")?;
    let bin = cli
        .ganesha_binary
        .unwrap_or_else(|| PathBuf::from(DEFAULT_GANESHA_BIN));
    if !bin.exists() {
        return Err(anyhow!(
            "{} not found; install nfs-ganesha or pass --ganesha-binary",
            bin.display()
        ));
    }

    let export_path = cli
        .mount_path
        .canonicalize()
        .context("export path must exist before starting ganesha")?;

    let config_file = NamedTempFile::new().context("failed to create temp ganesha config")?;
    let log_path = cli
        .ganesha_log
        .unwrap_or_else(|| export_path.join(".."))
        .join("osagefs-ganesha.log");

    let rendered = render_ganesha_config(&export_path, &cli.pseudo_path, addr.port(), cli.read_only);
    std::fs::write(config_file.path(), rendered).context("failed to write ganesha config")?;

    info!(
        target: "gateway",
        protocol = "nfs4",
        config = %config_file.path().display(),
        mount = %cli.mount_path.display(),
        listen = %cli.listen,
        "launching nfs-ganesha"
    );

    let mut child = Command::new(&bin)
        .arg("-f")
        .arg(config_file.path())
        .arg("-L")
        .arg(&log_path)
        .spawn()
        .context("failed to start ganesha.nfsd")?;

    tokio::select! {
        status = child.wait() => {
            let status = status?;
            if !status.success() {
                return Err(anyhow!("ganesha.nfsd exited with status {status}"));
            }
        }
        _ = signal::ctrl_c() => {
            info!(target: "gateway", "received ctrl_c, stopping ganesha");
            child.start_kill().ok();
        }
    }

    Ok(())
}

fn render_ganesha_config(export: &Path, pseudo: &str, port: u16, read_only: bool) -> String {
    let mut body = String::new();
    body.push_str("NFS_Core_Param {\n");
    body.push_str(&format!("    NFS_Port = {};\n", port));
    body.push_str("    NFS_Protocols = 4;\n");
    body.push_str("}\n\n");

    body.push_str("EXPORT {\n");
    body.push_str("    Export_Id = 1;\n");
    body.push_str(&format!("    Path = \"{}\";\n", export.display()));
    body.push_str(&format!("    Pseudo = \"{}\";\n", pseudo));
    body.push_str("    Access_Type = ");
    body.push_str(if read_only { "RO" } else { "RW" });
    body.push_str(";\n");
    body.push_str("    Protocols = 4;\n");
    body.push_str("    Transports = TCP;\n");
    body.push_str("    SecType = sys;\n");
    body.push_str("    Squash = None;\n");
    body.push_str("    FSAL {\n        Name = VFS;\n    }\n}\n");
    body
}

#[derive(Clone)]
struct OsageDirectFs {
    read_only: bool,
    fs: Arc<OsageFs>,
}

impl OsageDirectFs {
    async fn new(cli: &Cli) -> Result<Self> {
        let config = build_config(cli);
        if matches!(config.object_provider, ObjectStoreProvider::Local) {
            std::fs::create_dir_all(&config.store_path)?;
        }
        std::fs::create_dir_all(&config.local_cache_path)?;

        let handle = tokio::runtime::Handle::current();
        let metadata = Arc::new(
            MetadataStore::open(&config.store_path, config.shard_size, config.log_storage_io).await?,
        );
        let superblock = Arc::new(
            SuperblockManager::load_or_init(metadata.clone(), config.shard_size).await?,
        );
        ensure_root(metadata.clone(), superblock.clone(), &config).await?;

        let segments = Arc::new(SegmentManager::new(&config, handle.clone())?);
        let client_state = Arc::new(ClientStateManager::load(&config.state_path)?);
        let journal = if config.disable_journal {
            None
        } else {
            Some(Arc::new(JournalManager::new(&config.local_cache_path)?))
        };

        let fs = Arc::new(OsageFs::new(
            config,
            metadata,
            superblock,
            segments,
            journal,
            handle,
            client_state,
            None,
        ));

        let fs_for_replay = fs.clone();
        let replayed = task::spawn_blocking(move || fs_for_replay.replay_journal())
            .await
            .map_err(|join| anyhow!("replay task failed: {join}"))??;
        if replayed > 0 {
            info!(target: "gateway", replayed, "replayed journal entries before serving NFS");
        }

        Ok(Self { read_only: cli.read_only, fs })
    }

    async fn call<T, F>(&self, op: F) -> Result<T, nfsstat3>
    where
        T: Send + 'static,
        F: FnOnce(&OsageFs) -> std::result::Result<T, i32> + Send + 'static,
    {
        let fs = self.fs.clone();
        task::spawn_blocking(move || op(fs.as_ref()))
            .await
            .map_err(|_| nfsstat3::NFS3ERR_IO)?
            .map_err(map_errno)
    }

    fn ensure_writable(&self) -> Result<(), nfsstat3> {
        if self.read_only {
            Err(nfsstat3::NFS3ERR_ROFS)
        } else {
            Ok(())
        }
    }

    async fn flush(&self) {
        let _ = self.call(|fs| fs.nfs_flush()).await;
    }
}

#[async_trait]
impl NFSFileSystem for OsageDirectFs {
    fn capabilities(&self) -> VFSCapabilities {
        if self.read_only {
            VFSCapabilities::ReadOnly
        } else {
            VFSCapabilities::ReadWrite
        }
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_ID
    }

    async fn lookup(
        &self,
        _auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let name = filename_to_name(filename)?;
        let inode = self.call(move |fs| fs.nfs_lookup(dirid, &name)).await?;
        Ok(inode.inode)
    }

    async fn getattr(&self, _auth: &AuthContext, id: fileid3) -> Result<fattr3, nfsstat3> {
        let inode = self.call(move |fs| fs.nfs_getattr(id)).await?;
        Ok(inode_to_fattr(&inode))
    }

    async fn setattr(
        &self,
        _auth: &AuthContext,
        id: fileid3,
        setattr: sattr3,
    ) -> Result<fattr3, nfsstat3> {
        self.ensure_writable()?;
        let mode = match setattr.mode {
            set_mode3::mode(v) => Some(v),
            set_mode3::Void => None,
        };
        let uid = match setattr.uid {
            set_uid3::uid(v) => Some(v),
            set_uid3::Void => None,
        };
        let gid = match setattr.gid {
            set_gid3::gid(v) => Some(v),
            set_gid3::Void => None,
        };
        let size = match setattr.size {
            set_size3::size(v) => Some(v),
            set_size3::Void => None,
        };

        let inode = self
            .call(move |fs| fs.nfs_setattr(id, mode, uid, gid, size))
            .await?;
        Ok(inode_to_fattr(&inode))
    }

    async fn read(
        &self,
        _auth: &AuthContext,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let bytes = self.call(move |fs| fs.nfs_read(id, offset, count)).await?;
        let eof = bytes.len() < count as usize;
        Ok((bytes, eof))
    }

    async fn write(
        &self,
        _auth: &AuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3> {
        self.ensure_writable()?;
        let payload = data.to_vec();
        self.call(move |fs| fs.nfs_write(id, offset, &payload)).await?;
        let inode = self.call(move |fs| fs.nfs_getattr(id)).await?;
        Ok(inode_to_fattr(&inode))
    }

    async fn create(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let name = filename_to_name(filename)?;
        let uid = match attr.uid {
            set_uid3::uid(v) => v,
            set_uid3::Void => auth.uid,
        };
        let gid = match attr.gid {
            set_gid3::gid(v) => v,
            set_gid3::Void => auth.gid,
        };
        let inode = self.call(move |fs| fs.nfs_create(dirid, &name, uid, gid)).await?;
        let mode = match attr.mode {
            set_mode3::mode(v) => Some(v),
            set_mode3::Void => None,
        };
        let size = match attr.size {
            set_size3::size(v) => Some(v),
            set_size3::Void => None,
        };
        let inode = self
            .call(move |fs| fs.nfs_setattr(inode.inode, mode, Some(uid), Some(gid), size))
            .await?;
        Ok((inode.inode, inode_to_fattr(&inode)))
    }

    async fn create_exclusive(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let (id, _) = self.create(auth, dirid, filename, sattr3::default()).await?;
        Ok(id)
    }

    async fn mkdir(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        dirname: &filename3,
        attrs: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let name = filename_to_name(dirname)?;
        let uid = match attrs.uid {
            set_uid3::uid(v) => v,
            set_uid3::Void => auth.uid,
        };
        let gid = match attrs.gid {
            set_gid3::gid(v) => v,
            set_gid3::Void => auth.gid,
        };
        let inode = self
            .call(move |fs| fs.nfs_mkdir(dirid, &name, uid, gid))
            .await?;
        if let set_mode3::mode(mode) = attrs.mode {
            let inode = self
                .call(move |fs| fs.nfs_setattr(inode.inode, Some(mode), Some(uid), Some(gid), None))
                .await?;
            return Ok((inode.inode, inode_to_fattr(&inode)));
        }
        Ok((inode.inode, inode_to_fattr(&inode)))
    }

    async fn remove(
        &self,
        _auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<(), nfsstat3> {
        self.ensure_writable()?;
        let name = filename_to_name(filename)?;
        let lookup_name = name.clone();
        let child = self.call(move |fs| fs.nfs_lookup(dirid, &lookup_name)).await?;
        if child.is_dir() {
            self.call(move |fs| fs.nfs_remove_dir(dirid, &name)).await?;
        } else {
            self.call(move |fs| fs.nfs_remove_file(dirid, &name)).await?;
        }
        Ok(())
    }

    async fn rename(
        &self,
        _auth: &AuthContext,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        self.ensure_writable()?;
        let from_name = filename_to_name(from_filename)?;
        let to_name = filename_to_name(to_filename)?;
        self.call(move |fs| fs.nfs_rename(from_dirid, &from_name, to_dirid, &to_name, 0))
            .await?;
        Ok(())
    }

    async fn readdir(
        &self,
        _auth: &AuthContext,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let children = self.call(move |fs| fs.nfs_readdir(dirid)).await?;
        let mut entries_map = BTreeMap::new();
        for (id, name) in children {
            let inode = self.call(move |fs| fs.nfs_getattr(id)).await?;
            entries_map.insert(
                id,
                DirEntry {
                    fileid: id,
                    name: filename3::from(name.into_bytes()),
                    attr: inode_to_fattr(&inode),
                    cookie: id,
                },
            );
        }

        let mut entries_vec: Vec<_> = entries_map.into_iter().collect();
        entries_vec.sort_by_key(|(id, _)| *id);
        let mut entries = Vec::new();
        let mut started = start_after == 0;
        let mut has_more = false;
        for (id, entry) in entries_vec {
            if !started {
                if id == start_after {
                    started = true;
                }
                continue;
            }
            if entries.len() >= max_entries {
                has_more = true;
                break;
            }
            entries.push(entry);
        }

        Ok(ReadDirResult {
            end: !has_more,
            entries,
        })
    }

    async fn symlink(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let name = filename_to_name(linkname)?;
        let target = symlink.0.clone();
        let uid = auth.uid;
        let gid = auth.gid;
        let inode = self
            .call(move |fs| fs.nfs_symlink(dirid, &name, target, uid, gid))
            .await?;
        Ok((inode.inode, inode_to_fattr(&inode)))
    }

    async fn readlink(&self, _auth: &AuthContext, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let target = self.call(move |fs| fs.nfs_readlink(id)).await?;
        Ok(nfspath3::from(target))
    }

    async fn mknod(
        &self,
        auth: &AuthContext,
        dirid: fileid3,
        filename: &filename3,
        ftype: ftype3,
        attr: &sattr3,
        _spec: Option<&specdata3>,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        match ftype {
            // Back this with regular create for broad client compatibility.
            ftype3::NF3REG => self.create(auth, dirid, filename, attr.clone()).await,
            _ => Err(nfsstat3::NFS3ERR_NOTSUPP),
        }
    }

    async fn link(
        &self,
        _auth: &AuthContext,
        _fileid: fileid3,
        _linkdirid: fileid3,
        _linkname: &filename3,
    ) -> Result<(), nfsstat3> {
        // Hard links are not currently wired through the NFS direct adapter.
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn commit(
        &self,
        _auth: &AuthContext,
        _fileid: fileid3,
        _offset: u64,
        _count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        self.flush().await;
        Ok(self.get_write_verf())
    }

    fn get_write_verf(&self) -> writeverf3 {
        let pid = std::process::id().to_be_bytes();
        let mut out = [0u8; NFS3_WRITEVERFSIZE as usize];
        out[..pid.len()].copy_from_slice(&pid);
        out
    }

    async fn fsinfo(&self, auth: &AuthContext, id: fileid3) -> Result<fsinfo3, nfsstat3> {
        let obj_attr = match self.getattr(auth, id).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(_) => post_op_attr::Void,
        };

        Ok(fsinfo3 {
            obj_attributes: obj_attr,
            rtmax: 1024 * 1024,
            rtpref: 1024 * 1024,
            rtmult: 1024 * 1024,
            wtmax: 1024 * 1024,
            wtpref: 1024 * 1024,
            wtmult: 1024 * 1024,
            dtpref: 1024 * 1024,
            maxfilesize: 8 * (1 << 60), // 8 EiB cap for NFS client compatibility
            time_delta: nfstime3 {
                seconds: 0,
                nseconds: 1,
            },
            properties: FSF_LINK | FSF_SYMLINK | FSF_HOMOGENEOUS | FSF_CANSETTIME,
        })
    }

    async fn fsstat(&self, auth: &AuthContext, fileid: fileid3) -> Result<fsstat3, nfsstat3> {
        let obj_attr = match self.getattr(auth, fileid).await {
            Ok(v) => post_op_attr::attributes(v),
            Err(_) => post_op_attr::Void,
        };

        let total_bytes = 8 * (1 << 60); // 8 EiB cap for NFS client compatibility
        Ok(fsstat3 {
            obj_attributes: obj_attr,
            tbytes: total_bytes,
            fbytes: total_bytes,
            abytes: total_bytes,
            tfiles: u64::MAX / 2,
            ffiles: u64::MAX / 2,
            afiles: u64::MAX / 2,
            invarsec: 1,
        })
    }
}

fn build_config(cli: &Cli) -> Config {
    let config_root = default_user_config_root();
    let state_path = cli
        .state_path
        .clone()
        .unwrap_or_else(|| config_root.join("state").join("nfs_gateway_state.bin"));
    let local_cache_path = cli
        .local_cache_path
        .clone()
        .unwrap_or_else(|| config_root.join("cache"));

    Config {
        mount_path: cli.mount_path.clone(),
        store_path: cli.store_path.clone(),
        local_cache_path,
        log_storage_io: false,
        inline_threshold: 4 * 1024,
        inline_compression: true,
        inline_encryption_key: None,
        segment_compression: true,
        segment_encryption_key: None,
        shard_size: 2048,
        inode_batch: 1280,
        segment_batch: 2560,
        // Match ZeroFS-style aggressive writeback batching defaults for throughput.
        pending_bytes: 1024 * 1024 * 1024,
        home_prefix: "/home".to_string(),
        object_provider: cli.object_provider,
        bucket: cli.bucket.clone(),
        region: cli.region.clone(),
        endpoint: cli.endpoint.clone(),
        object_prefix: cli.object_prefix.clone(),
        gcs_service_account: cli.gcs_service_account.clone(),
        state_path,
        perf_log: None,
        disable_journal: cli.disable_journal,
        fsync_on_close: false,
        flush_interval_ms: 30_000,
        disable_cleanup: true,
        // Use short NFS-like metadata TTLs: good throughput without long stale windows.
        lookup_cache_ttl_ms: 1_000,
        dir_cache_ttl_ms: 1_000,
        metadata_poll_interval_ms: 1_000,
        segment_cache_bytes: 512 * 1024 * 1024,
        foreground: true,
        log_file: None,
        debug_log: false,
        imap_delta_batch: 256,
    }
}

fn default_user_config_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".osagefs")
}

async fn ensure_root(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
) -> Result<()> {
    let uid = unsafe { libc::geteuid() as u32 };
    let gid = unsafe { libc::getegid() as u32 };
    let desired_mode = 0o40777;

    if let Some(mut existing) = metadata.get_inode(ROOT_INODE).await? {
        if existing.uid != uid || existing.gid != gid || existing.mode != desired_mode {
            existing.uid = uid;
            existing.gid = gid;
            existing.mode = desired_mode;
            let snapshot = superblock.prepare_dirty_generation()?;
            let generation = snapshot.generation;
            if let Err(err) = metadata
                .persist_inode(&existing, generation, config.shard_size)
                .await
            {
                superblock.abort_generation(generation);
                return Err(err);
            }
            superblock.commit_generation(generation).await?;
        }
        return Ok(());
    }

    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;
    let mut root = InodeRecord::new_directory(
        ROOT_INODE,
        ROOT_INODE,
        String::from(""),
        String::from("/"),
        uid,
        gid,
    );
    root.mode = desired_mode;
    if let Err(err) = metadata
        .persist_inode(&root, generation, config.shard_size)
        .await
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    superblock.commit_generation(generation).await?;
    ensure_welcome_file(metadata, superblock, config, uid, gid).await?;

    Ok(())
}

async fn ensure_welcome_file(
    metadata: Arc<MetadataStore>,
    superblock: Arc<SuperblockManager>,
    config: &Config,
    uid: u32,
    gid: u32,
) -> Result<()> {
    let mut root = metadata
        .get_inode(ROOT_INODE)
        .await?
        .ok_or_else(|| anyhow!("missing root inode {}", ROOT_INODE))?;
    if root
        .children()
        .map(|children| children.contains_key(WELCOME_FILENAME))
        .unwrap_or(false)
    {
        return Ok(());
    }

    let new_inode = superblock.reserve_inodes(1).await?;
    let snapshot = superblock.prepare_dirty_generation()?;
    let generation = snapshot.generation;

    let mut welcome = InodeRecord::new_file(
        new_inode,
        ROOT_INODE,
        WELCOME_FILENAME.to_string(),
        format!("/{}", WELCOME_FILENAME),
        uid,
        gid,
    );
    let bytes = WELCOME_CONTENT.as_bytes().to_vec();
    welcome.size = bytes.len() as u64;
    welcome.storage = FileStorage::Inline(bytes);
    welcome.mode = 0o100644;

    if let Err(err) = metadata
        .persist_inode(&welcome, generation, config.shard_size)
        .await
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    if let Some(children) = root.children_mut() {
        children.insert(WELCOME_FILENAME.to_string(), new_inode);
    }
    if let Err(err) = metadata
        .persist_inode(&root, generation, config.shard_size)
        .await
    {
        superblock.abort_generation(generation);
        return Err(err);
    }
    superblock.commit_generation(generation).await?;
    Ok(())
}

fn map_errno(code: i32) -> nfsstat3 {
    match code {
        libc::ENOENT => nfsstat3::NFS3ERR_NOENT,
        libc::EPERM => nfsstat3::NFS3ERR_PERM,
        libc::EACCES => nfsstat3::NFS3ERR_ACCES,
        libc::EEXIST => nfsstat3::NFS3ERR_EXIST,
        libc::ENOTDIR => nfsstat3::NFS3ERR_NOTDIR,
        libc::EISDIR => nfsstat3::NFS3ERR_ISDIR,
        libc::EINVAL => nfsstat3::NFS3ERR_INVAL,
        libc::ENOTEMPTY => nfsstat3::NFS3ERR_NOTEMPTY,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

fn inode_to_fattr(inode: &InodeRecord) -> fattr3 {
    let nlink = if inode.is_dir() {
        2 + inode.children().map(|c| c.len() as u32).unwrap_or(0)
    } else {
        inode.link_count.max(1)
    };

    let ftype = match inode.kind {
        InodeKind::Directory { .. } => ftype3::NF3DIR,
        InodeKind::File => ftype3::NF3REG,
        InodeKind::Symlink => ftype3::NF3LNK,
        InodeKind::Tombstone => ftype3::NF3REG,
    };

    fattr3 {
        ftype,
        mode: inode.mode,
        nlink,
        uid: inode.uid,
        gid: inode.gid,
        size: inode.size,
        used: inode.size,
        rdev: Default::default(),
        fsid: 1,
        fileid: inode.inode,
        atime: to_nfstime(inode.atime.unix_timestamp(), inode.atime.nanosecond()),
        mtime: to_nfstime(inode.mtime.unix_timestamp(), inode.mtime.nanosecond()),
        ctime: to_nfstime(inode.ctime.unix_timestamp(), inode.ctime.nanosecond()),
    }
}

fn to_nfstime(secs: i64, nanos: u32) -> nfstime3 {
    if secs <= 0 {
        return nfstime3 {
            seconds: 0,
            nseconds: 0,
        };
    }
    nfstime3 {
        seconds: secs.min(u32::MAX as i64) as u32,
        nseconds: nanos,
    }
}

fn filename_to_name(name: &filename3) -> Result<String, nfsstat3> {
    let s = std::str::from_utf8(&name.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
    if s.is_empty() || s == "." || s == ".." || s.contains('/') {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    Ok(s.to_string())
}
