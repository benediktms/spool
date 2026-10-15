//! Harness adapter contract. `HarnessAdapter` is the public, sealed entry point; each harness
//! implements the crate-internal `jsonl::JsonlHarness` hooks and inherits the shared JSONL pipeline.

use crate::domain::*;
use std::io::{Read, Seek};
use std::sync::atomic::AtomicBool;

pub mod claude;
pub mod codex;
pub mod omp;

mod jsonl;
mod lineage;

mod sealed {
    pub trait Sealed {}
}

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeResult {
    /// Header positively identifies this format; `Some(version)` when versioned.
    Match {
        format_version: Option<String>,
    },
    NoMatch,
    /// Looks like this format but is an unsupported variant (e.g. compressed/archived).
    Unsupported {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct ParseOptions {
    pub max_record_bytes: u64,
    pub max_source_bytes: u64,
    /// Parse at most this many bytes (the finite observed prefix, spec 8.2); the adapter ends
    /// at the last complete record boundary <= this.
    pub observe_len: u64,
    pub include_subagents: bool,
    /// Directory roots a conversational blob reference may resolve into (spec 5.3). Empty = none.
    pub blob_roots: Vec<std::path::PathBuf>,
}

/// Streaming sink, called on a blocking thread. Returning `Err(SinkError)` aborts the parse
/// (cancellation, bounded channel closed). Adapters must call it in file order.
pub trait RecordSink {
    fn record(&mut self, rec: RecordMeta) -> Result<(), SinkError>;
    fn message(&mut self, msg: MessageEntry) -> Result<(), SinkError>;
    fn tool_call(&mut self, call: ToolCallMeta) -> Result<(), SinkError>;
    fn tool_result(&mut self, result: ToolResultMeta) -> Result<(), SinkError>;
    fn warning(&mut self, w: ParseWarning) -> Result<(), SinkError>;
    /// Cooperative cancellation flag; adapters check it between records.
    fn cancelled(&self) -> &AtomicBool;
}

#[derive(Debug, thiserror::Error)]
#[error("sink aborted")]
pub struct SinkError;

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("unsupported format: {0}")]
    Unsupported(String),
    #[error("record exceeds limit")]
    LimitExceeded,
    #[error("source io: {0}")]
    Io(#[from] std::io::Error),
    #[error("locator does not verify: {0}")]
    LocatorMismatch(String),
    #[error("cancelled")]
    Cancelled,
}

impl From<SinkError> for AdapterError {
    fn from(_: SinkError) -> Self {
        AdapterError::Cancelled
    }
}

/// Sealed: every harness implements the crate-internal `jsonl::JsonlHarness` hooks and receives
/// this trait from the shared pipeline, so limits, locator verification, and ordering cannot be
/// bypassed.
pub trait HarnessAdapter: Send + Sync + sealed::Sealed {
    fn kind(&self) -> HarnessKind;
    /// Part of the pipeline fingerprint.
    fn parser_version(&self) -> &'static str;
    /// `header` = first (up to 64 KiB) bytes of the file.
    fn probe(&self, header: &[u8]) -> ProbeResult;
    /// Stream records into `sink`. MUST NOT hold the whole transcript or deserialize excluded
    /// payloads into `serde_json::Value`.
    fn parse_snapshot(
        &self,
        source: &mut dyn Read,
        options: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError>;
    /// Re-read exactly one record from an already safely opened source (`source` is the
    /// validated fd), verify `locator.len` bytes hash to `expected`, and return its blocks.
    /// `Err(LocatorMismatch)` means positively changed; `Io` means unavailable.
    fn resolve_locator(
        &self,
        source: &mut dyn ReadSeek,
        locator: &SourceLocator,
        expected: &Digest,
        max_record_bytes: u64,
    ) -> Result<ResolvedRecord, AdapterError>;
}

pub fn adapter_for(kind: HarnessKind) -> &'static dyn HarnessAdapter {
    match kind {
        HarnessKind::Omp => &omp::OmpAdapter,
        HarnessKind::Codex => &codex::CodexAdapter,
        HarnessKind::Claude => &claude::ClaudeAdapter,
    }
}
