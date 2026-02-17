use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng, rand_core::RngCore};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::inode::{FileStorage, InlinePayload, InlinePayloadCodec};

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

#[derive(Debug, Clone)]
pub struct InlineCodecConfig {
    pub compression: bool,
    pub encryption_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EncodedBytes {
    pub codec: InlinePayloadCodec,
    pub payload: Vec<u8>,
    pub original_len: Option<u64>,
    pub nonce: Option<[u8; 12]>,
}

pub fn encode_inline_storage(data: &[u8], config: &InlineCodecConfig) -> Result<FileStorage> {
    let encoded = encode_bytes(data, config)?;
    if encoded.codec == InlinePayloadCodec::None {
        Ok(FileStorage::Inline(data.to_vec()))
    } else {
        Ok(FileStorage::InlineEncoded(InlinePayload {
            codec: encoded.codec,
            payload: encoded.payload,
            original_len: encoded.original_len,
            nonce: encoded.nonce,
        }))
    }
}

pub fn decode_inline_storage(
    storage: &FileStorage,
    encryption_key: Option<&str>,
) -> Result<Vec<u8>> {
    match storage {
        FileStorage::Inline(bytes) => Ok(bytes.clone()),
        FileStorage::InlineEncoded(payload) => decode_inline_payload(payload, encryption_key),
        FileStorage::LegacySegment(_) | FileStorage::Segments(_) => {
            anyhow::bail!("decode_inline_storage called for non-inline storage")
        }
    }
}

fn decode_inline_payload(payload: &InlinePayload, encryption_key: Option<&str>) -> Result<Vec<u8>> {
    decode_bytes(
        payload.codec,
        &payload.payload,
        payload.original_len,
        payload.nonce,
        encryption_key,
    )
}

pub fn encode_bytes(data: &[u8], config: &InlineCodecConfig) -> Result<EncodedBytes> {
    if data.is_empty() && config.encryption_key.is_none() {
        return Ok(EncodedBytes {
            codec: InlinePayloadCodec::None,
            payload: Vec::new(),
            original_len: None,
            nonce: None,
        });
    }

    let mut compressed_payload: Option<Vec<u8>> = None;
    let mut compressed = false;
    if config.compression {
        let candidate = compress_prepend_size(data);
        if candidate.len() < data.len() {
            compressed = true;
            compressed_payload = Some(candidate);
        }
    }

    let Some(passphrase) = config.encryption_key.as_deref() else {
        return Ok(if compressed {
            EncodedBytes {
                codec: InlinePayloadCodec::Lz4,
                payload: compressed_payload.expect("compressed payload should exist"),
                original_len: Some(data.len() as u64),
                nonce: None,
            }
        } else {
            EncodedBytes {
                codec: InlinePayloadCodec::None,
                payload: data.to_vec(),
                original_len: None,
                nonce: None,
            }
        });
    };

    let mut nonce = [0u8; 12];
    OsRng.fill_bytes(&mut nonce);
    let key = key_from_passphrase(passphrase);
    let cipher = ChaCha20Poly1305::new_from_slice(&key).expect("32-byte key");
    let plain = compressed_payload.as_deref().unwrap_or(data);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plain)
        .map_err(|_| anyhow::anyhow!("payload encryption failed"))?;
    let codec = if compressed {
        InlinePayloadCodec::Lz4ChaCha20Poly1305
    } else {
        InlinePayloadCodec::ChaCha20Poly1305
    };
    Ok(EncodedBytes {
        codec,
        payload: ciphertext,
        original_len: compressed.then_some(data.len() as u64),
        nonce: Some(nonce),
    })
}

pub fn decode_bytes(
    codec: InlinePayloadCodec,
    payload: &[u8],
    original_len: Option<u64>,
    nonce: Option<[u8; 12]>,
    encryption_key: Option<&str>,
) -> Result<Vec<u8>> {
    match codec {
        InlinePayloadCodec::None => Ok(payload.to_vec()),
        InlinePayloadCodec::Lz4 => {
            let decoded = decompress_size_prepended(payload).context("LZ4 decode failed")?;
            if let Some(expected) = original_len {
                anyhow::ensure!(
                    decoded.len() as u64 == expected,
                    "LZ4 decoded length mismatch expected={} actual={}",
                    expected,
                    decoded.len()
                );
            }
            Ok(decoded)
        }
        InlinePayloadCodec::ChaCha20Poly1305 => {
            let nonce = nonce.context("missing encryption nonce")?;
            let passphrase =
                encryption_key.context("payload is encrypted but no key was configured")?;
            let key = key_from_passphrase(passphrase);
            let cipher = ChaCha20Poly1305::new_from_slice(&key).expect("32-byte key");
            cipher
                .decrypt(Nonce::from_slice(&nonce), payload)
                .map_err(|_| anyhow::anyhow!("decryption failed"))
        }
        InlinePayloadCodec::Lz4ChaCha20Poly1305 => {
            let nonce = nonce.context("missing encryption nonce")?;
            let passphrase =
                encryption_key.context("payload is encrypted but no key was configured")?;
            let key = key_from_passphrase(passphrase);
            let cipher = ChaCha20Poly1305::new_from_slice(&key).expect("32-byte key");
            let compressed = cipher
                .decrypt(Nonce::from_slice(&nonce), payload)
                .map_err(|_| anyhow::anyhow!("decryption failed"))?;
            let decoded = decompress_size_prepended(&compressed).context("LZ4 decode failed")?;
            if let Some(expected) = original_len {
                anyhow::ensure!(
                    decoded.len() as u64 == expected,
                    "LZ4 decoded length mismatch expected={} actual={}",
                    expected,
                    decoded.len()
                );
            }
            Ok(decoded)
        }
    }
}

fn key_from_passphrase(passphrase: &str) -> [u8; 32] {
    let digest = Sha256::digest(passphrase.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}
