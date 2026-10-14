//! Embedding slice tests (spec 19.6): fake-provider service behaviour and a hand-rolled HTTP/1.1 mock server.
//! Synthetic data only; the only sockets are on 127.0.0.1.

mod http;
mod service;
mod tokenizer;

use spool::config::{EmbeddingConfig, SearchConfig};
use spool::storage::index::Index;

/// 2025-10-12T12:00:00Z
pub const NOON: i64 = 1_760_270_400_000;
/// 2025-10-13T00:00:00Z
pub const NEXT_MIDNIGHT: i64 = 1_760_313_600_000;

pub fn emb_cfg() -> EmbeddingConfig {
    EmbeddingConfig {
        hosted_processing_approved: true,
        dimensions: 32,
        request_timeout_seconds: 2,
        ..Default::default()
    }
}

pub fn search_cfg() -> SearchConfig {
    SearchConfig::default()
}

pub async fn open_index() -> (tempfile::TempDir, Index) {
    let dir = tempfile::tempdir().unwrap();
    let index = Index::open_for_test(dir.path()).await.unwrap();
    (dir, index)
}

pub fn digest_of(n: u32) -> [u8; 32] {
    *blake3::hash(&n.to_le_bytes()).as_bytes()
}
