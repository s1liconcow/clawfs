use lz4_flex::compress_prepend_size;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// Codec helpers (adapted from src/codec.rs)
// ---------------------------------------------------------------------------

fn try_compress(data: &[u8]) -> Option<Vec<u8>> {
    let candidate = compress_prepend_size(data);
    if candidate.len() < data.len() {
        Some(candidate)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Tab 1: Cold Start — compression stats helper
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct CompressResult {
    pub original_bytes: usize,
    pub stored_bytes: usize,
    pub ratio: f64,
    pub codec: String,
}

/// Returns compression stats for a content blob (used by Tab 1 to show
/// how much space each cached directory would take on a ClawFS volume).
#[wasm_bindgen]
pub fn compute_storage(content: &str) -> JsValue {
    let data = content.as_bytes();
    let (stored, codec) = match try_compress(data) {
        Some(c) => (c.len(), "LZ4".to_string()),
        None => (data.len(), "Raw".to_string()),
    };
    let ratio = if data.is_empty() {
        1.0
    } else {
        stored as f64 / data.len() as f64
    };
    let result = CompressResult {
        original_bytes: data.len(),
        stored_bytes: stored,
        ratio,
        codec,
    };
    serde_wasm_bindgen::to_value(&result).unwrap_or(JsValue::NULL)
}

// ---------------------------------------------------------------------------
// Tab 2 & 3: Filesystem simulation (FsDemo)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
pub struct SimFile {
    pub path: String,
    pub original_bytes: usize,
    pub stored_bytes: usize,
    pub codec: String,
    pub mtime_ms: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Checkpoint {
    pub generation: u64,
    pub label: String,
    pub files: Vec<SimFile>,
}

#[wasm_bindgen]
pub struct FsDemo {
    files: Vec<SimFile>,
    checkpoints: Vec<Checkpoint>,
    generation: u64,
}

#[wasm_bindgen]
impl FsDemo {
    #[wasm_bindgen(constructor)]
    pub fn new() -> FsDemo {
        FsDemo {
            files: Vec::new(),
            checkpoints: Vec::new(),
            generation: 0,
        }
    }

    /// Add a file to the simulated volume. Returns stored byte count.
    pub fn add_file(&mut self, path: &str, content: &str, now_ms: f64) -> usize {
        let data = content.as_bytes();
        let (stored, codec) = match try_compress(data) {
            Some(c) => (c.len(), "LZ4".to_string()),
            None => (data.len(), "Raw".to_string()),
        };
        // Remove existing entry with same path if present
        self.files.retain(|f| f.path != path);
        self.files.push(SimFile {
            path: path.to_string(),
            original_bytes: data.len(),
            stored_bytes: stored,
            codec,
            mtime_ms: now_ms,
        });
        stored
    }

    /// Add a file with pre-known sizes (for large binary blobs we don't
    /// actually pass the content through WASM, just record sizes).
    pub fn add_file_sized(
        &mut self,
        path: &str,
        original_bytes: usize,
        compressible: bool,
        now_ms: f64,
    ) {
        let (stored, codec) = if compressible {
            (original_bytes * 55 / 100, "LZ4".to_string())
        } else {
            (original_bytes, "Raw".to_string())
        };
        self.files.retain(|f| f.path != path);
        self.files.push(SimFile {
            path: path.to_string(),
            original_bytes,
            stored_bytes: stored,
            codec,
            mtime_ms: now_ms,
        });
    }

    /// Snapshot current state and return the new generation number.
    pub fn checkpoint(&mut self, label: &str) -> u64 {
        self.generation += 1;
        self.checkpoints.push(Checkpoint {
            generation: self.generation,
            label: label.to_string(),
            files: self.files.clone(),
        });
        self.generation
    }

    /// Restore files to the state at a given generation.
    pub fn restore(&mut self, gen: u64) {
        if let Some(cp) = self.checkpoints.iter().find(|c| c.generation == gen) {
            self.files = cp.files.clone();
        }
    }

    /// Clear all files (simulate crash / fresh worker mount before restore).
    pub fn clear_files(&mut self) {
        self.files.clear();
    }

    pub fn reset(&mut self) {
        self.files.clear();
        self.checkpoints.clear();
        self.generation = 0;
    }

    /// Returns JSON array of file entries for rendering the file tree.
    pub fn get_tree_json(&self) -> String {
        serde_json::to_string(&self.files).unwrap_or_else(|_| "[]".to_string())
    }

    /// Returns JSON array of checkpoint summaries.
    pub fn get_checkpoints_json(&self) -> String {
        #[derive(Serialize)]
        struct CpSummary {
            generation: u64,
            label: String,
            file_count: usize,
            total_original: usize,
            total_stored: usize,
        }
        let summaries: Vec<CpSummary> = self
            .checkpoints
            .iter()
            .map(|cp| CpSummary {
                generation: cp.generation,
                label: cp.label.clone(),
                file_count: cp.files.len(),
                total_original: cp.files.iter().map(|f| f.original_bytes).sum(),
                total_stored: cp.files.iter().map(|f| f.stored_bytes).sum(),
            })
            .collect();
        serde_json::to_string(&summaries).unwrap_or_else(|_| "[]".to_string())
    }

    /// Returns aggregate totals as JSON.
    pub fn get_totals_json(&self) -> String {
        let total_orig: usize = self.files.iter().map(|f| f.original_bytes).sum();
        let total_stored: usize = self.files.iter().map(|f| f.stored_bytes).sum();
        let count = self.files.len();
        serde_json::json!({
            "file_count": count,
            "total_original": total_orig,
            "total_stored": total_stored,
            "ratio": if total_orig == 0 { 1.0 } else { total_stored as f64 / total_orig as f64 }
        })
        .to_string()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

// serde_wasm_bindgen isn't a dep — use serde_json + JsValue::from_str instead
// via a simple helper so we avoid pulling in the extra crate.
mod serde_wasm_bindgen {
    use wasm_bindgen::JsValue;
    pub fn to_value<T: serde::Serialize>(v: &T) -> Result<JsValue, ()> {
        let s = serde_json::to_string(v).map_err(|_| ())?;
        Ok(JsValue::from_str(&s))
    }
}
