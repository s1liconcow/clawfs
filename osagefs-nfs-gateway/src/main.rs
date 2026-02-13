use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use dashmap::DashMap;
use nfsserve::fs_util::{metadata_to_fattr3, path_setattr};
use nfsserve::nfs::{fattr3, fileid3, filename3, nfspath3, nfsstat3, sattr3};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::signal;
use tracing::info;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

const ROOT_ID: fileid3 = 1;
const DEFAULT_V4_EXPORT: &str = "/osagefs";
const DEFAULT_GANESHA_BIN: &str = "ganesha.nfsd";

#[derive(Parser, Debug)]
struct Cli {
    /// Path where OsageFS is mounted and should be exported over NFS.
    #[arg(long, value_name = "PATH", default_value = "/tmp/osagefs-mnt")]
    mount_path: PathBuf,

    /// Address for the user-mode NFS server to bind to (ip:port).
    #[arg(long, default_value = "0.0.0.0:2049")]
    listen: String,

    /// Export pseudo path (used when generating the NFSv4 Ganesha config).
    #[arg(long, default_value = DEFAULT_V4_EXPORT)]
    pseudo_path: String,

    /// Run the gateway in read-only mode.
    #[arg(long, default_value_t = false)]
    read_only: bool,

    /// Which backend to run (user-mode v3 or ganesha-backed v4).
    #[arg(long, value_enum, default_value_t = Protocol::V3)]
    protocol: Protocol,

    /// Optional path to ganesha.nfsd when running with --protocol v4.
    #[arg(long)]
    ganesha_binary: Option<PathBuf>,

    /// Optional custom log path for ganesha.nfsd
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
    let addr: SocketAddr = cli
        .listen
        .parse()
        .context("invalid listen address; expected ip:port")?;
    let backend = OsageGatewayFs::new(&cli.mount_path, cli.read_only).await?;

    let listener = NFSTcpListener::bind(&format!("{}:{}", addr.ip(), addr.port()), backend)
        .await
        .context("failed to bind NFS listener")?;

    info!(
        target: "gateway",
        protocol = "nfs3",
        listen = %cli.listen,
        mount = %cli.mount_path.display(),
        "serving user-mode NFS"
    );
    tokio::select! {
        res = listener.handle_forever() => {
            res.context("NFS server stopped unexpectedly")?
        }
        _ = signal::ctrl_c() => {
            info!(target: "gateway", "received ctrl_c, shutting down user-mode NFS");
        }
    }
    Ok(())
}

async fn run_ganesha(cli: Cli) -> Result<()> {
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
        .context("export path must exist before starting ganesha (mount OsageFS first)")?;

    let config_file = NamedTempFile::new().context("failed to create temp ganesha config")?;
    let log_path = cli
        .ganesha_log
        .unwrap_or_else(|| export_path.join(".."))
        .join("osagefs-ganesha.log");

    let rendered =
        render_ganesha_config(&export_path, &cli.pseudo_path, addr.port(), cli.read_only);
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

struct OsageGatewayFs {
    read_only: bool,
    ids: PathIndex,
}

impl OsageGatewayFs {
    async fn new(root: &Path, read_only: bool) -> Result<Self> {
        if !root.exists() {
            return Err(anyhow!("mount path {} does not exist", root.display()));
        }
        let canonical = root
            .canonicalize()
            .with_context(|| format!("unable to canonicalize {}", root.display()))?;
        Ok(Self {
            read_only,
            ids: PathIndex::new(canonical),
        })
    }

    fn ensure_writable(&self) -> Result<(), nfsstat3> {
        if self.read_only {
            Err(nfsstat3::NFS3ERR_ROFS)
        } else {
            Ok(())
        }
    }

    fn rel_from_id(&self, id: fileid3) -> Result<PathBuf, nfsstat3> {
        self.ids.rel_path(id).ok_or(nfsstat3::NFS3ERR_STALE)
    }

    fn abs_from_id(&self, id: fileid3) -> Result<PathBuf, nfsstat3> {
        let rel = self.rel_from_id(id)?;
        Ok(self.ids.absolute(&rel))
    }

    fn child_path(&self, dirid: fileid3, name: &filename3) -> Result<PathBuf, nfsstat3> {
        let rel = self.rel_from_id(dirid)?;
        let mut child = rel.clone();
        child.push(filename_to_path(name)?);
        Ok(child)
    }

    fn touch_id(&self, rel: PathBuf) -> fileid3 {
        self.ids.touch(rel)
    }

    fn remove_id(&self, rel: &Path) {
        self.ids.remove(rel);
    }

    fn remove_tree(&self, rel: &Path) {
        self.ids.remove_tree(rel);
    }
}

#[async_trait]
impl NFSFileSystem for OsageGatewayFs {
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

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let child_rel = self.child_path(dirid, filename)?;
        let abs = self.ids.absolute(&child_rel);
        if !abs.exists() {
            return Err(nfsstat3::NFS3ERR_NOENT);
        }
        Ok(self.touch_id(child_rel))
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        let abs = self.abs_from_id(id)?;
        let meta = fs::metadata(&abs).await.map_err(map_error)?;
        Ok(metadata_to_fattr3(id, &meta))
    }

    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        self.ensure_writable()?;
        let abs = self.abs_from_id(id)?;
        path_setattr(&abs, &setattr).await?;
        self.getattr(id).await
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let abs = self.abs_from_id(id)?;
        let mut file = fs::File::open(&abs).await.map_err(map_error)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(map_error)?;
        let mut buf = vec![0u8; count as usize];
        let bytes = file.read(&mut buf).await.map_err(map_error)?;
        buf.truncate(bytes);
        let eof = bytes < count as usize;
        Ok((buf, eof))
    }

    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        self.ensure_writable()?;
        let abs = self.abs_from_id(id)?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&abs)
            .await
            .map_err(map_error)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(map_error)?;
        file.write_all(data).await.map_err(map_error)?;
        file.flush().await.map_err(map_error)?;
        self.getattr(id).await
    }

    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let rel = self.child_path(dirid, filename)?;
        let abs = self.ids.absolute(&rel);
        let mut opts = fs::OpenOptions::new();
        opts.create_new(true).write(true).read(true);
        opts.open(&abs).await.map_err(map_error)?;
        let id = self.touch_id(rel);
        path_setattr(&abs, &attr).await?;
        let attr = self.getattr(id).await?;
        Ok((id, attr))
    }

    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        let (id, _) = self.create(dirid, filename, sattr3::default()).await?;
        Ok(id)
    }

    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let rel = self.child_path(dirid, dirname)?;
        let abs = self.ids.absolute(&rel);
        fs::create_dir(&abs).await.map_err(map_error)?;
        let id = self.touch_id(rel);
        let attr = self.getattr(id).await?;
        Ok((id, attr))
    }

    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        self.ensure_writable()?;
        let rel = self.child_path(dirid, filename)?;
        let abs = self.ids.absolute(&rel);
        let meta = fs::symlink_metadata(&abs).await.map_err(map_error)?;
        if meta.is_dir() {
            fs::remove_dir(&abs).await.map_err(map_error)?;
            self.remove_tree(&rel);
        } else {
            fs::remove_file(&abs).await.map_err(map_error)?;
            self.remove_id(&rel);
        }
        Ok(())
    }

    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        self.ensure_writable()?;
        let from_rel = self.child_path(from_dirid, from_filename)?;
        let to_rel = self.child_path(to_dirid, to_filename)?;
        let from_abs = self.ids.absolute(&from_rel);
        let to_abs = self.ids.absolute(&to_rel);
        if let Some(parent) = to_abs.parent() {
            fs::create_dir_all(parent).await.map_err(map_error)?;
        }
        fs::rename(&from_abs, &to_abs).await.map_err(map_error)?;
        self.remove_tree(&from_rel);
        self.touch_id(to_rel);
        Ok(())
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let rel = self.rel_from_id(dirid)?;
        let abs = self.ids.absolute(&rel);
        let mut reader = fs::read_dir(&abs).await.map_err(map_error)?;
        let mut entries_map = BTreeMap::new();
        while let Some(entry) = reader.next_entry().await.map_err(map_error)? {
            let name = entry.file_name();
            if name == OsStr::new(".") || name == OsStr::new("..") {
                continue;
            }
            let mut child_rel = rel.clone();
            child_rel.push(&name);
            let id = self.touch_id(child_rel);
            let metadata = entry.metadata().await.map_err(map_error)?;
            let filename = filename3::from(name_to_bytes(&name));
            let attr = metadata_to_fattr3(id, &metadata);
            entries_map.insert(
                id,
                DirEntry {
                    fileid: id,
                    name: filename,
                    attr,
                },
            );
        }
        let mut entries_vec: Vec<_> = entries_map.into_iter().collect();
        entries_vec.sort_by_key(|(id, _)| *id);
        let mut entries = Vec::new();
        let mut started = start_after == 0;
        for (id, entry) in entries_vec.into_iter() {
            if !started {
                if id == start_after {
                    started = true;
                }
                continue;
            }
            if entries.len() >= max_entries {
                break;
            }
            entries.push(entry);
        }
        Ok(ReadDirResult {
            end: entries.len() < max_entries,
            entries,
        })
    }

    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        self.ensure_writable()?;
        let rel = self.child_path(dirid, linkname)?;
        let abs = self.ids.absolute(&rel);
        #[cfg(unix)]
        {
            let target = PathBuf::from(OsStr::from_bytes(&symlink.0));
            tokio::fs::symlink(&target, &abs).await.map_err(map_error)?;
        }
        #[cfg(not(unix))]
        {
            let _ = (symlink, attr);
            return Err(nfsstat3::NFS3ERR_NOTSUPP);
        }
        path_setattr(&abs, attr).await?;
        let id = self.touch_id(rel);
        let attr = self.getattr(id).await?;
        Ok((id, attr))
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let abs = self.abs_from_id(id)?;
        let target = fs::read_link(&abs).await.map_err(map_error)?;
        Ok(nfspath3::from(name_to_bytes(target.as_os_str())))
    }
}

fn map_error(err: std::io::Error) -> nfsstat3 {
    use std::io::ErrorKind::*;
    match err.kind() {
        NotFound => nfsstat3::NFS3ERR_NOENT,
        PermissionDenied => nfsstat3::NFS3ERR_ACCES,
        AlreadyExists => nfsstat3::NFS3ERR_EXIST,
        InvalidInput => nfsstat3::NFS3ERR_INVAL,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

fn filename_to_path(name: &filename3) -> Result<PathBuf, nfsstat3> {
    #[cfg(unix)]
    {
        let os = OsStr::from_bytes(&name.0);
        let candidate = PathBuf::from(os);
        for comp in candidate.components() {
            match comp {
                Component::Normal(_) => continue,
                Component::CurDir => continue,
                _ => return Err(nfsstat3::NFS3ERR_INVAL),
            }
        }
        if candidate.as_os_str().is_empty() {
            Err(nfsstat3::NFS3ERR_INVAL)
        } else {
            Ok(candidate)
        }
    }
    #[cfg(not(unix))]
    {
        let s = std::str::from_utf8(&name.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        if s.is_empty() || s.contains('/') || s.contains("..") {
            Err(nfsstat3::NFS3ERR_INVAL)
        } else {
            Ok(PathBuf::from(s))
        }
    }
}

fn name_to_bytes(name: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        name.as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        name.to_string_lossy().into_owned().into_bytes()
    }
}

struct PathIndex {
    root: PathBuf,
    by_id: DashMap<fileid3, PathBuf>,
    by_path: DashMap<PathBuf, fileid3>,
    next: AtomicU64,
}

impl PathIndex {
    fn new(root: PathBuf) -> Self {
        let by_id = DashMap::new();
        by_id.insert(ROOT_ID, PathBuf::new());
        let by_path = DashMap::new();
        by_path.insert(PathBuf::new(), ROOT_ID);
        Self {
            root,
            by_id,
            by_path,
            next: AtomicU64::new(ROOT_ID + 1),
        }
    }

    fn rel_path(&self, id: fileid3) -> Option<PathBuf> {
        self.by_id.get(&id).map(|p| p.clone())
    }

    fn absolute(&self, rel: &Path) -> PathBuf {
        let mut abs = self.root.clone();
        abs.push(rel);
        abs
    }

    fn touch(&self, rel: PathBuf) -> fileid3 {
        if let Some(entry) = self.by_path.get(&rel) {
            *entry.value()
        } else {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.by_path.insert(rel.clone(), id);
            self.by_id.insert(id, rel);
            id
        }
    }

    fn remove(&self, rel: &Path) {
        if let Some((_, id)) = self.by_path.remove(rel) {
            self.by_id.remove(&id);
        }
    }

    fn remove_tree(&self, rel: &Path) {
        let keys: Vec<PathBuf> = self
            .by_path
            .iter()
            .filter_map(|entry| {
                let path = entry.key();
                if path.starts_with(rel) {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }
}
