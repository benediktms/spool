//! Shared domain types: the contract between adapters, the chunker, storage and the reader.
//!
//! Owned by `spool-core`. The adapter/chunker worker implements against these types and
//! asks core before changing them.

use serde::{Deserialize, Serialize};

/// Unix milliseconds, UTC. `None` everywhere means "unknown", never a guess.
pub type TimestampMs = i64;

/// blake3 digest of exact bytes (raw record bytes incl. line terminator, or text bytes).
pub type Digest = [u8; 32];

pub fn digest(bytes: &[u8]) -> Digest {
    *blake3::hash(bytes).as_bytes()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessKind {
    Omp,
    Codex,
    Claude,
}

impl HarnessKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Omp => "omp",
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    Interactive,
    Subagent,
    Unknown,
}

/// Provenance of a retained conversational block (spec 5.2): `Unknown` when the format cannot
/// tell a user-authored block from an injected plain-text block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockOrigin {
    Authored,
    Unknown,
}

/// Where one physical record lives in the source. Offsets are hints; identity is `digest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocator {
    /// Byte offset of the first byte of the record in the (decompressed) source stream.
    pub offset: u64,
    /// Exact record length in bytes, including the line terminator when present.
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// Session header / title slot / metadata slot.
    Header,
    /// Carries at least one retained conversational block (see `MessageEntry`).
    Message,
    /// Carries tool invocation block(s); no conversational block.
    ToolCall,
    /// Carries tool result block(s); no conversational block.
    ToolResult,
    /// Parent-link/control record (model change, label, ...). Participates in ancestry.
    Control,
    /// Harness-generated compaction or branch summary. Never searchable.
    Summary,
    /// Unknown record type. Counted, never searchable.
    Unknown,
}

/// Every physical record, in file order, including excluded ones (they carry ancestry).
/// No payload bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMeta {
    /// Native record id if the format has one; otherwise
    /// `fallback:{parser_version}:{digest_hex}:{occurrence}` (spec 6.1). Unique per snapshot.
    pub key: String,
    pub native_id: Option<String>,
    /// Key of the parent record (native parent link), `None` for roots.
    pub parent_key: Option<String>,
    pub kind: RecordKind,
    pub timestamp: Option<TimestampMs>,
    pub turn_id: Option<String>,
    pub locator: SourceLocator,
    pub digest: Digest,
}

/// One retained conversational text block. `text` is verbatim UTF-8.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextBlock {
    /// Index of the content block inside its record (stable block identity within the record).
    pub index: u32,
    pub origin: BlockOrigin,
    pub text: String,
}

/// Attachment descriptor (payload excluded, no OCR).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentMeta {
    pub index: u32,
    pub kind: String,
    pub mime: Option<String>,
    pub bytes: Option<u64>,
}

/// A canonical conversational message envelope with at least one retained block.
/// Emitted once per native message (adapters dedupe duplicate representations).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageEntry {
    /// Key of the originating `RecordMeta` (a message's identity is its record).
    pub record_key: String,
    pub role: Role,
    /// Native phase such as `commentary` / `final_answer`; metadata only.
    pub phase: Option<String>,
    pub blocks: Vec<TextBlock>,
    pub attachments: Vec<AttachmentMeta>,
    /// Record keys of earlier representations of the same native message that this entry
    /// replaces (later authoritative saved update wins, e.g. accumulated Claude assistant
    /// updates sharing one native uuid). Their `RecordMeta` stays (ancestry, raw view); core
    /// drops their message/blocks from the staging snapshot, and `graph` must treat a
    /// superseded record as an alias of the superseding message for parent resolution.
    #[serde(default)]
    pub supersedes: Vec<String>,
}

/// A tool invocation block inside a record (arguments are NOT included).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallMeta {
    pub record_key: String,
    pub block_index: u32,
    pub native_call_id: Option<String>,
    pub tool_name: String,
    /// Turn that owns this call, only when established by native identity/ancestry/tested rule.
    pub turn_id: Option<String>,
}

/// A tool result block inside a record (payload NOT included).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolResultMeta {
    pub record_key: String,
    pub block_index: u32,
    pub native_call_id: Option<String>,
    /// `(record_key, block_index)` of the linked call, only when the format proves the link
    /// (native call id match on the selected ancestry). `None` = orphan or unlinkable.
    pub call: Option<(String, u32)>,
    pub is_error: Option<bool>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParseWarning {
    /// Stable machine code, e.g. `invalid_record`, `unknown_block`, `missing_parent`, `cycle`,
    /// `missing_blob`, `oversize_record`.
    pub code: String,
    pub record_key: Option<String>,
    pub locator: Option<SourceLocator>,
    /// Content-free detail (never transcript text).
    pub detail: String,
}

/// Coverage verdict the adapter reports for the whole parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coverage {
    Complete,
    Partial,
    /// Unknown critical schema version etc.: nothing may be published.
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ParseCounts {
    pub records: u64,
    pub messages: u64,
    pub blocks_retained: u64,
    pub unknown_blocks: u64,
    pub unknown_records: u64,
    pub malformed_records: u64,
    pub ambiguous_origin_blocks: u64,
    pub bytes_read: u64,
    pub bytes_discarded: u64,
}

/// Result of `parse_snapshot`; records/messages/tool metas were already streamed to the sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedSessionManifest {
    pub native_session_id: String,
    pub native_title: Option<String>,
    pub cwd: Option<String>,
    pub session_kind: SessionKind,
    /// Raw native parent-session value (lineage evidence). Never inferred.
    pub parent_native_session: Option<String>,
    /// `true` only when the format verifiably names the parent session.
    pub lineage_verified: bool,
    pub started_at: Option<TimestampMs>,
    pub last_activity_at: Option<TimestampMs>,
    /// End of the last complete record consumed. Trailing incomplete bytes are not included.
    pub prefix_end: u64,
    /// Durably established active leaf record key, else `None` (runtime-only leaf).
    pub active_leaf_key: Option<String>,
    pub coverage: Coverage,
    pub counts: ParseCounts,
}

/// Raw block classes visible when re-reading one record from source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    Text,
    ToolCall,
    ToolResult,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    Verbatim,
    NormalizedJson,
    Metadata,
}

/// One block of a record re-read from source. `bytes` is the full decoded block
/// (UTF-8 text verbatim; tool payload either the original string or `serde_json` normalized).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBlock {
    pub index: u32,
    pub kind: BlockKind,
    pub fidelity: Fidelity,
    pub bytes: Vec<u8>,
}

/// A record re-read from the original source with its identity verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRecord {
    /// Exact original bytes (incl. line terminator when present).
    pub raw: Vec<u8>,
    pub digest: Digest,
    pub native_id: Option<String>,
    pub blocks: Vec<ResolvedBlock>,
}

/// Native timestamp string -> unix milliseconds. Accepts RFC 3339 with `Z` or a numeric offset
/// (`+02:00`) and optional fraction, and a bare integer of epoch milliseconds. `None` = unknown
/// (never a guessed value). Adapters MUST use this for every record timestamp they can read.
pub fn parse_timestamp_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    if !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) {
        let v: i64 = s.parse().ok()?;
        // 10-digit values are epoch seconds, longer ones milliseconds.
        return Some(if s.len() <= 11 {
            v.checked_mul(1000)?
        } else {
            v
        });
    }
    if s.ends_with('Z') {
        return crate::search::parse_rfc3339_utc(s);
    }
    let n = s.len();
    if n > 6 && s.is_ascii() {
        let (head, off) = s.split_at(n - 6);
        let sign = match off.as_bytes()[0] {
            b'+' => 1i64,
            b'-' => -1,
            _ => return None,
        };
        if off.as_bytes()[3] != b':' {
            return None;
        }
        let (oh, om): (i64, i64) = (off[1..3].parse().ok()?, off[4..6].parse().ok()?);
        if oh > 23 || om > 59 {
            return None;
        }
        let base = crate::search::parse_rfc3339_utc(&format!("{head}Z"))?;
        return Some(base - sign * (oh * 3600 + om * 60) * 1000);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp_ms as p;
    #[test]
    fn timestamps() {
        assert_eq!(p("2026-10-12T09:00:01Z"), Some(1791795601000));
        assert_eq!(p("2026-10-12T11:00:01.250+02:00"), Some(1791795601250));
        assert_eq!(p("1791795601"), Some(1791795601000));
        assert_eq!(p("1791795601250"), Some(1791795601250));
        assert_eq!(p("garbage"), None);
    }
}
