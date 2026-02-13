use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use tempfile::NamedTempFile;

pub fn write_flexbuffer<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent: PathBuf = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&parent).with_context(|| format!("creating dir {}", parent.display()))?;
    let data = serialize_flex(value)?;
    let mut tmp = NamedTempFile::new_in(&parent)
        .with_context(|| format!("creating temp file in {}", parent.display()))?;
    tmp.write_all(&data)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)
        .map(|_| ())
        .with_context(|| format!("persisting temp file to {}", path.display()))
}

pub fn serialize_flex<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    value.serialize(&mut serializer)?;
    Ok(serializer.take_buffer())
}

pub fn deserialize_flex<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let reader = flexbuffers::Reader::get_root(bytes)?;
    Ok(T::deserialize(reader)?)
}
