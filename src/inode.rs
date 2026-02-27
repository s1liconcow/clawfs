use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::segment::SegmentPointer;

pub const ROOT_INODE: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InodeKind {
    File,
    Directory {
        children: Arc<BTreeMap<String, u64>>,
    },
    Symlink,
    Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeRecord {
    pub inode: u64,
    pub parent: u64,
    pub name: String,
    pub path: String,
    pub kind: InodeKind,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: OffsetDateTime,
    pub mtime: OffsetDateTime,
    pub ctime: OffsetDateTime,
    pub link_count: u32,
    #[serde(default)]
    pub rdev: u32,
    pub storage: FileStorage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileStorage {
    Inline(Vec<u8>),
    InlineEncoded(InlinePayload),
    #[serde(rename = "Segment")]
    LegacySegment(SegmentPointer),
    Segments(Vec<SegmentExtent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InlinePayload {
    pub codec: InlinePayloadCodec,
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<[u8; 12]>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum InlinePayloadCodec {
    None,
    Lz4,
    ChaCha20Poly1305,
    Lz4ChaCha20Poly1305,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentExtent {
    pub logical_offset: u64,
    pub pointer: SegmentPointer,
}

impl InodeRecord {
    pub fn new_directory(
        inode: u64,
        parent: u64,
        name: String,
        path: String,
        uid: u32,
        gid: u32,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            inode,
            parent,
            name,
            path,
            kind: InodeKind::Directory {
                children: Arc::new(BTreeMap::new()),
            },
            size: 0,
            mode: 0o40755,
            uid,
            gid,
            atime: now,
            mtime: now,
            ctime: now,
            link_count: 1,
            rdev: 0,
            storage: FileStorage::Inline(Vec::new()),
        }
    }

    pub fn new_file(
        inode: u64,
        parent: u64,
        name: String,
        path: String,
        uid: u32,
        gid: u32,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            inode,
            parent,
            name,
            path,
            kind: InodeKind::File,
            size: 0,
            mode: 0o100664,
            uid,
            gid,
            atime: now,
            mtime: now,
            ctime: now,
            link_count: 1,
            rdev: 0,
            storage: FileStorage::Inline(Vec::new()),
        }
    }

    pub fn new_symlink(
        inode: u64,
        parent: u64,
        name: String,
        path: String,
        uid: u32,
        gid: u32,
        target: Vec<u8>,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        let size = target.len() as u64;
        Self {
            inode,
            parent,
            name,
            path,
            kind: InodeKind::Symlink,
            size,
            mode: 0o120777,
            uid,
            gid,
            atime: now,
            mtime: now,
            ctime: now,
            link_count: 1,
            rdev: 0,
            storage: FileStorage::Inline(target),
        }
    }

    pub fn tombstone(inode: u64) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            inode,
            parent: 0,
            name: String::new(),
            path: String::new(),
            kind: InodeKind::Tombstone,
            size: 0,
            mode: 0,
            uid: 0,
            gid: 0,
            atime: now,
            mtime: now,
            ctime: now,
            link_count: 0,
            rdev: 0,
            storage: FileStorage::Inline(Vec::new()),
        }
    }

    pub fn shard_index(&self, shard_size: u64) -> u64 {
        if shard_size == 0 {
            return 0;
        }
        self.inode / shard_size
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.kind, InodeKind::Directory { .. })
    }

    pub fn is_symlink(&self) -> bool {
        matches!(self.kind, InodeKind::Symlink)
    }

    pub fn children(&self) -> Option<&BTreeMap<String, u64>> {
        match &self.kind {
            InodeKind::Directory { children } => Some(children),
            _ => None,
        }
    }

    pub fn children_mut(&mut self) -> Option<&mut BTreeMap<String, u64>> {
        match &mut self.kind {
            InodeKind::Directory { children } => Some(Arc::make_mut(children)),
            _ => None,
        }
    }

    pub fn symlink_target_bytes(&self) -> Option<&[u8]> {
        if !self.is_symlink() {
            return None;
        }
        match &self.storage {
            FileStorage::Inline(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn update_times(&mut self) {
        let now = OffsetDateTime::now_utc();
        self.mtime = now;
        self.ctime = now;
    }

    pub fn file_mode(&self) -> u32 {
        self.mode
    }

    pub fn data_inline(&self) -> Option<&[u8]> {
        match &self.storage {
            FileStorage::Inline(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn segment_pointer(&self) -> Option<&SegmentPointer> {
        match &self.storage {
            FileStorage::LegacySegment(ptr) => Some(ptr),
            FileStorage::Segments(extents) => extents.first().map(|ext| &ext.pointer),
            _ => None,
        }
    }

    pub fn normalize_storage(&mut self) {
        if let FileStorage::LegacySegment(ptr) = &self.storage {
            self.storage = FileStorage::Segments(vec![SegmentExtent::new(0, ptr.clone())]);
        }
    }

    pub fn segment_extents(&self) -> Option<&[SegmentExtent]> {
        match &self.storage {
            FileStorage::Segments(extents) => Some(extents),
            _ => None,
        }
    }

    pub fn segment_extents_mut(&mut self) -> Option<&mut Vec<SegmentExtent>> {
        if matches!(self.storage, FileStorage::LegacySegment(_))
            && let FileStorage::LegacySegment(ptr) =
                std::mem::replace(&mut self.storage, FileStorage::Inline(Vec::new()))
        {
            self.storage = FileStorage::Segments(vec![SegmentExtent::new(0, ptr)]);
        }
        match &mut self.storage {
            FileStorage::Segments(extents) => Some(extents),
            _ => None,
        }
    }

    pub fn set_segment_extents(&mut self, mut extents: Vec<SegmentExtent>) {
        extents.sort_by_key(|ext| ext.logical_offset);
        self.storage = FileStorage::Segments(extents);
    }

    pub fn inc_links(&mut self) {
        self.link_count = self.link_count.saturating_add(1);
    }

    pub fn dec_links(&mut self) {
        self.link_count = self.link_count.saturating_sub(1);
    }
}

impl SegmentExtent {
    pub fn new(logical_offset: u64, pointer: SegmentPointer) -> Self {
        Self {
            logical_offset,
            pointer,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeShard {
    pub shard_id: u64,
    pub inodes: BTreeMap<u64, InodeRecord>,
}

impl InodeShard {
    pub fn new(shard_id: u64) -> Self {
        Self {
            shard_id,
            inodes: BTreeMap::new(),
        }
    }

    pub fn upsert(&mut self, record: InodeRecord) {
        if matches!(record.kind, InodeKind::Tombstone) {
            self.inodes.remove(&record.inode);
        } else {
            let mut normalized = record;
            normalized.normalize_storage();
            self.inodes.insert(normalized.inode, normalized);
        }
    }
}
