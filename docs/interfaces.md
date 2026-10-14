# Module interfaces: adapters, graph, chunker, source

Authoritative specs: `docs/spec.md`, `docs/read-session-contract.md`. Types live in `src/domain.rs`; the adapter trait is
in `src/adapters/mod.rs`. This file records the contracts between the parsers (`src/adapters/{omp,codex,claude}.rs`),
the graph, the chunker, safe source access and the ingest and reader code that consume them. Adapter behaviour per
harness is in `docs/adapter-support.md`; tests are in `tests/adapter*.rs`, `tests/graph.rs`, `tests/chunking.rs`,
`tests/source.rs`.

## Adapter contract (see `src/adapters/mod.rs`)

* `parse_snapshot(&mut dyn Read, &ParseOptions, &mut dyn RecordSink) -> ParsedSessionManifest`
  streams in file order: every physical record as `RecordMeta` (incl. excluded ones, they carry
  ancestry via `parent_key`), then for retained messages one `MessageEntry` (dedupe duplicate native
  representations), `ToolCallMeta`/`ToolResultMeta` (no payloads), `ParseWarning`.
  Emit `record()` for a record BEFORE its `message()/tool_*()`.
* `RecordMeta.digest` = `domain::digest(exact record bytes including terminator)`;
  `locator = {offset,len}` of those bytes. Incomplete trailing line is not emitted and `prefix_end`
  stops before it. Invalid complete line: `ParseWarning(invalid_record)` + still emits a `RecordMeta`
  with `kind=Unknown`.
* `resolve_locator(&mut dyn ReadSeek, locator, expected, max)` seeks, reads exactly `len` bytes,
  hashes; digest mismatch -> `LocatorMismatch` (core maps to SOURCE_CHANGED/STALE_REFERENCE), IO -> `Io`.
  Returns `ResolvedRecord{raw, blocks}` where `blocks[i].index` equals `TextBlock.index` /
  `ToolCallMeta.block_index` / `ToolResultMeta.block_index` for text, tool call (payload: original
  arguments string verbatim, else normalized JSON with `Fidelity::NormalizedJson`) and tool result.
  Never follows blob paths outside `blob_roots`. Formats with no byte-exact resolution must make
  `resolve_locator` return `Unsupported` (core maps to VIEW_NOT_SUPPORTED for raw).
* `ParsedSessionManifest.coverage = Unsupported` for unknown critical schema version
  (nothing publishes). Unknown noncritical fields ignored. Unknown blocks counted.
* Exclusion rules: spec 5.2 table; filtering at block level; tool-result-in-user-role yields zero
  retained text. `BlockOrigin::Unknown` only for ambiguous-origin plain text.

## Source safety (`src/source.rs`)

```rust
pub struct SourceRoots { /* canonicalized enabled roots */ }
impl SourceRoots {
    pub fn new(roots: &[PathBuf]) -> io::Result<Self>;
    /// Read-only, no-follow traversal. Pages are ordered by path and hold at most `max_entries`.
    /// `complete` is true only after a full error-free traversal; `next_after` resumes a capped page.
    pub fn scan(&self, cancel: &AtomicBool, max_entries: usize) -> ScanOutcome;
    pub fn scan_from(&self, cancel: &AtomicBool, max_entries: usize, after: Option<&Path>) -> ScanOutcome;
    // ScanOutcome { files, complete, truncated, next_after, errors }
    /// Open by anchored root dirfd with O_NOFOLLOW checks; regular current-user files only.
    /// Caller keeps the returned fd for reads.
    pub fn open(&self, path: &Path) -> Result<OpenedSource, SourceError>;
}
pub struct SourceFileInfo { pub path: PathBuf, pub len: u64, pub mtime_ms: i64, pub dev: u64, pub ino: u64 }
pub struct OpenedSource { pub file: File, pub info: SourceFileInfo }
```

## Graph (`src/graph.rs`)

Record parents are the physical links as written. The message graph is a projection: it skips control records and earlier
versions of the same message (a later save of a message never becomes its own ancestor). The reader walks the physical
record path, so superseded versions and tool-result envelopes stay visible to `with_tools`.

```rust
pub struct SessionGraph; // built from Vec<RecordMeta> + Vec<MessageEntry>, bounded memory
impl SessionGraph {
    pub fn build(records: &[RecordMeta], messages: &[MessageEntry]) -> (Self, Vec<ParseWarning>); // missing parent / cycle isolate
    /// Nearest conversational ancestor of each message (skipping control/tool/summary records).
    pub fn message_parent(&self, record_key: &str) -> Option<&str>;
    /// Maximal unbranched runs of messages, file order, each with parent run linkage.
    pub fn runs(&self) -> &[Run];
    /// Leaf message keys (branch tips); a branch = path root->leaf through the graph.
    pub fn leaves(&self) -> &[String];
    /// Message keys on the ancestry path root->`leaf`.
    pub fn path_to(&self, leaf: &str) -> Vec<&str>;
    pub fn is_ancestor(&self, a: &str, b: &str) -> bool;
}
pub struct Run { pub key: String, pub messages: Vec<String>, pub parent_run: Option<String> }
```

## Chunker (`src/ingest/chunk.rs`)

Pure, deterministic, no I/O. Token counting is injected (core supplies cl100k_base impl):

The planner checks cancellation between records, fragments, and bounded tokenizer calls; prefix scans are limited to 8 KiB. Those windows only bound work: all source bytes remain new-content spans, and the configured rendered-token limit is never exceeded. Cancellation returns no partial plan; `TokenLimit` means even one UTF-8 scalar cannot fit.

```rust
pub trait TokenCounter: Send + Sync {
    fn identity(&self) -> &str; // goes into fingerprint
    fn count(&self, text: &str) -> usize;
    /// Largest byte length <= text.len(), on a char boundary, whose prefix has <= max_tokens tokens.
    fn prefix_within(&self, text: &str, max_tokens: usize) -> usize;
}
pub struct ChunkConfig { pub target_new_tokens: usize, pub max_rendered_tokens: usize, pub overlap_tokens: usize, pub version: u32 }
pub struct ChunkMessage { pub key: String /* record_key */, pub role: Role, pub timestamp: Option<TimestampMs>, pub blocks: Vec<TextBlock> }
pub struct ChunkPlan {
    pub run_key: String,
    pub ordinal: u32,
    pub rendered: String,               // "USER: ...\n\nASSISTANT: ..." exact embedding input
    pub input_hash: Digest,             // digest(rendered)
    pub rendered_tokens: usize,
    pub time_anchor: Option<TimestampMs>, // last non-context block with known timestamp
    pub spans: Vec<ChunkSpan>,
    pub is_tail: bool,                  // replaceable underfilled final chunk of its run
}
pub struct ChunkSpan { pub message_key: String, pub block_index: u32, pub start: usize, pub end: usize /* utf8 byte offsets in block text */, pub is_context: bool }
pub enum ChunkError { Cancelled, TokenLimit }
pub fn plan_chunks(graph: &SessionGraph, messages: &[ChunkMessage], cfg: &ChunkConfig, tok: &dyn TokenCounter, cancel: &AtomicBool) -> Result<Vec<ChunkPlan>, ChunkError>;
```

Core derives the logical chunk id from session id, chunker version, spans, flags and `input_hash`.
Context spans never cross sibling branches; context comes from verified ancestors only.

## Core consumers (for orientation)

* Ingest worker: opens via `SourceRoots::open`, runs `parse_snapshot` on a blocking thread with a
  bounded sink, persists records/messages/tool metas into the staging snapshot, builds graph, runs
  `plan_chunks`, publishes.
* Reader: pinned manifest -> `SourceRoots::open` -> `resolve_locator` per needed record, verifies
  digest, builds items/fragments/cursors. Reader never calls the chunker.
