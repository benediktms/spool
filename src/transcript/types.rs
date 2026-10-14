//! Wire types of `read_session` (spec 14.2): input, output and error payload. The JSON Schemas in
//! `schemas/read_session/` are generated from these types (`tests/reader_schemas.rs`).

use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};

pub const MAX_BYTES_MIN: u32 = 4096;
pub const MAX_BYTES_MAX: u32 = 262_144;
pub const MAX_BYTES_DEFAULT: u32 = 32_768;
pub const MAX_ITEMS_MAX: u32 = 200;
pub const MAX_ITEMS_DEFAULT: u32 = 50;
pub const CONTEXT_MAX: u32 = 20;
pub const CONTEXT_DEFAULT: u32 = 2;
pub const BRANCH_OPTIONS_MAX: usize = 50;
pub const WARNINGS_MAX: usize = 20;

// Required-but-nullable fields ("null means unknown", spec 14.2.6.1).
fn nullable_string(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": ["string", "null"] })
}

fn nullable_bool(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": ["boolean", "null"] })
}

fn nullable_locator(g: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "anyOf": [g.subschema_for::<Locator>(), { "type": "null" }] })
}

// ---------------------------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------------------------

/// Either an initial read (`session_id` required, optional selection/view/source_policy/limits)
/// or a cursor-only continuation (`cursor` required; only `max_bytes` and `max_items` may accompany
/// it). Unknown fields and mixed forms are rejected with `INVALID_ARGUMENT`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadSessionInput {
    /// Spool's internal session ID (not a file path or native ID). Required for an initial read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// What to read. Default `{"kind":"session"}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionInput>,
    /// Representation. Default `conversation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<View>,
    /// Default `prefer_source` for `conversation`, `require_source` for other views.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_policy: Option<SourcePolicy>,
    /// Final serialized tool-result cap in bytes (default 32768, 4096..=262144).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 4096, max = 262144))]
    pub max_bytes: Option<u32>,
    /// Distinct logical items per page (default 50, 1..=200).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 200))]
    pub max_items: Option<u32>,
    /// Opaque continuation. When present, only `max_bytes` and `max_items` may accompany it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SelectionInput {
    /// From the selected persisted branch's beginning to its frozen endpoint.
    Session {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch_ref: Option<String>,
    },
    /// The complete messages a search match covers, plus context messages.
    Match {
        match_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 0, max = 20))]
        before: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 0, max = 20))]
        after: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch_ref: Option<String>,
    },
    /// One conversational message plus context messages.
    Message {
        message_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 0, max = 20))]
        before: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 0, max = 20))]
        after: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch_ref: Option<String>,
    },
    /// Inclusive range of messages on one ancestry path.
    Range {
        from_message_ref: String,
        through_message_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch_ref: Option<String>,
    },
    /// One previously returned message, tool invocation, tool result or original record.
    /// `branch_ref` is optional and disambiguates which branch a tool call's linked results are
    /// listed from (`BRANCH_REQUIRED` when results exist on several branches below the call).
    Item {
        item_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch_ref: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum View {
    Conversation,
    WithTools,
    RawRecords,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourcePolicy {
    PreferSource,
    RequireSource,
    IndexedOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SelKind {
    Session,
    Match,
    Message,
    Range,
    Item,
}

// ---------------------------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadSessionOutput {
    pub session: SessionInfo,
    pub snapshot: SnapshotInfo,
    pub view: View,
    pub source_policy: SourcePolicy,
    pub selection: SelectionOut,
    pub items: Vec<Item>,
    pub page: PageInfo,
    pub navigation: Navigation,
    pub coverage: CoverageOut,
    pub warnings: Vec<Warning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionInfo {
    pub session_id: String,
    pub source_id: String,
    pub native_id: String,
    #[schemars(required, schema_with = "nullable_string")]
    pub native_title: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    Indexed,
    SourceRead,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub kind: SnapshotKind,
    /// RFC 3339 UTC.
    pub observed_at: String,
    /// Expiry of the lease that retains this read's manifest and all handles it issues.
    pub lease_expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContextCount {
    pub requested: u32,
    pub resolved: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SelectionOut {
    pub kind: SelKind,
    /// Resolved persisted path. Null when the selection has no messages.
    #[schemars(required, schema_with = "nullable_string")]
    pub branch_ref: Option<String>,
    /// First inclusive conversational message of the selected window.
    #[schemars(required, schema_with = "nullable_string")]
    pub from_message_ref: Option<String>,
    /// Last inclusive conversational message of the selected window.
    #[schemars(required, schema_with = "nullable_string")]
    pub through_message_ref: Option<String>,
    /// Messages the search match / message reference anchors to (range: both endpoints).
    pub anchor_message_refs: Vec<String>,
    /// Item selections only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_ref: Option<String>,
    /// Match/message selections only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<ContextCount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<ContextCount>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    VerifiedSource,
    IndexedSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Locator {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Provenance {
    pub origin: Origin,
    /// Opaque reference to the source file identity (never a path).
    pub source_ref: String,
    /// Opaque reference to the original record in this snapshot.
    pub record_ref: String,
    /// Expected digest of the original record (blake3, first 32 hex chars).
    pub record_digest: String,
    #[schemars(required, schema_with = "nullable_string")]
    pub native_record_id: Option<String>,
    /// Validated hint of the record's raw source-byte locator.
    #[schemars(required, schema_with = "nullable_locator")]
    pub locator: Option<Locator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BlockClass {
    Text,
    Attachment,
    ToolArguments,
    ToolResult,
    RawRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Utf8,
    Base64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FidelityOut {
    Verbatim,
    NormalizedJson,
    Metadata,
}

/// One content fragment of a block. `[start_byte, end_byte)` is a half-open range over the full
/// decoded block bytes (not escaped or base64 positions).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Fragment {
    pub block_ref: String,
    pub kind: BlockClass,
    pub encoding: Encoding,
    pub fidelity: FidelityOut,
    pub data: String,
    pub start_byte: u64,
    pub end_byte: u64,
    pub total_bytes: u64,
    /// The fragment ends at `total_bytes` (not: the whole block is on this page).
    pub is_last: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ItemCommon {
    pub item_ref: String,
    #[schemars(required, schema_with = "nullable_string")]
    pub timestamp: Option<String>,
    #[schemars(required, schema_with = "nullable_string")]
    pub turn_ref: Option<String>,
    pub blocks: Vec<Fragment>,
    /// This page reaches the end of the item's final block.
    pub item_end: bool,
    pub provenance: Provenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RoleOut {
    User,
    Assistant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResultState {
    ResultAvailable,
    /// No linked result exists in this snapshot (not a claim the call is running or failed).
    NoResultInSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MessageItem {
    pub message_ref: String,
    pub role: RoleOut,
    #[schemars(required, schema_with = "nullable_string")]
    pub phase: Option<String>,
    #[serde(flatten)]
    pub common: ItemCommon,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ToolCallItem {
    pub tool_ref: String,
    #[schemars(required, schema_with = "nullable_string")]
    pub native_call_id: Option<String>,
    pub tool_name: String,
    pub result_refs: Vec<String>,
    pub result_state: ResultState,
    /// Included by turn closure but outside the selected message window.
    pub outside_message_window: bool,
    #[serde(flatten)]
    pub common: ItemCommon,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ToolResultItem {
    /// Null for orphan results (no proven link to a call on the selected path).
    #[schemars(required, schema_with = "nullable_string")]
    pub tool_ref: Option<String>,
    #[schemars(required, schema_with = "nullable_string")]
    pub native_call_id: Option<String>,
    #[schemars(required, schema_with = "nullable_bool")]
    pub is_error: Option<bool>,
    pub outside_message_window: bool,
    #[serde(flatten)]
    pub common: ItemCommon,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RawRecordItem {
    pub record_kind: String,
    #[serde(flatten)]
    pub common: ItemCommon,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Item {
    Message(MessageItem),
    ToolCall(ToolCallItem),
    ToolResult(ToolResultItem),
    RawRecord(RawRecordItem),
}

impl Item {
    pub fn common_mut(&mut self) -> &mut ItemCommon {
        match self {
            Item::Message(i) => &mut i.common,
            Item::ToolCall(i) => &mut i.common,
            Item::ToolResult(i) => &mut i.common,
            Item::RawRecord(i) => &mut i.common,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    SelectionEnd,
    ByteLimit,
    ItemLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PageInfo {
    #[schemars(required, schema_with = "nullable_string")]
    pub next_cursor: Option<String>,
    pub selection_exhausted: bool,
    pub stop_reason: StopReason,
}

/// References that open NEW reads outside the selected window; not cursor substitutes.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Navigation {
    #[schemars(required, schema_with = "nullable_string")]
    pub before_message_ref: Option<String>,
    #[schemars(required, schema_with = "nullable_string")]
    pub after_message_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CoverageOut {
    pub status: CoverageStatus,
    /// Stable reason codes when `partial`.
    pub reasons: Vec<String>,
}

/// Typed, bounded, content-free notice.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Warning {
    pub code: String,
    pub message: String,
}

// ---------------------------------------------------------------------------------------------
// Error payload (`schemas/read_session/error.schema.json`)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidArgument,
    SessionNotFound,
    SessionNotIndexed,
    Forbidden,
    BranchRequired,
    InvalidRange,
    ViewNotSupported,
    SourcePolicyConflict,
    SourceUnavailable,
    SourceChanged,
    StaleReference,
    CursorExpired,
    UnsupportedFormat,
    ResourceLimit,
    StorageError,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BranchOption {
    pub branch_ref: String,
    pub leaf_message_ref: String,
    #[schemars(required, schema_with = "nullable_string")]
    pub leaf_timestamp: Option<String>,
    /// The adapter durably recorded this leaf as the active one (never chosen implicitly).
    pub is_active_leaf: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub retryable: bool,
    /// For example `re_search`, `new_read`, `select_branch`, `change_request`, `retry_later`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<String>,
    pub message: String,
    /// `BRANCH_REQUIRED` only: at most 50 options and within `max_bytes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_options: Option<Vec<BranchOption>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_options_truncated: Option<bool>,
    /// `RESOURCE_LIMIT` only: which budget was hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadSessionError {
    pub error: ErrorBody,
}

// ---------------------------------------------------------------------------------------------

/// RFC 3339 UTC (`2026-10-12T12:25:00Z`, millisecond part only when non-zero).
pub fn iso(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil-from-days (H. Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let (h, mi, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    if milli == 0 {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn iso_formats_epoch_and_leap_day() {
        assert_eq!(super::iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(super::iso(1_582_934_400_123), "2020-02-29T00:00:00.123Z");
        assert_eq!(super::iso(1_791_803_100_000), "2026-10-12T11:05:00Z");
    }
}
