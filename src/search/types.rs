//! Public input/output types of `search_sessions` (spec 14.1). Their JSON Schemas are the tool schemas.

use super::query::format_rfc3339;
use super::retrieve::{CoverageRow, SessionRow};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "schemars")]
pub enum Mode {
    /// Lexical + semantic candidates fused; sends the query text to the hosted embedding provider.
    Hybrid,
    /// FTS5 keyword retrieval; local only.
    Lexical,
    /// Query embedding + exact vector scan; sends the query text to the hosted embedding provider.
    Semantic,
    /// Exact substring over retained text; local only.
    Literal,
}

impl Mode {
    pub fn parse(s: &str) -> Option<Self> {
        serde_json::from_value(serde_json::Value::String(s.to_string())).ok()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "schemars")]
pub enum SessionKind {
    Interactive,
    Subagent,
    Unknown,
}

impl SessionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Subagent => "subagent",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "schemars")]
pub struct SearchInput {
    /// Non-empty query, at most 4,096 UTF-8 bytes and 1,024 provider tokens.
    pub query: String,
    /// Default `hybrid`.
    #[serde(default)]
    pub mode: Option<Mode>,
    /// Restrict to these configured source ids (e.g. `omp`). Empty = all.
    #[serde(default)]
    pub sources: Vec<String>,
    /// Absolute project path; matched exactly (canonical path) against the session working directory.
    #[serde(default)]
    pub project_path: Option<String>,
    /// Restrict to these session ids. Empty = all.
    #[serde(default)]
    pub session_ids: Vec<String>,
    /// Empty = `["interactive"]`.
    #[serde(default)]
    pub session_kinds: Vec<SessionKind>,
    /// Inclusive absolute RFC 3339 UTC timestamp (`Z`), applied to each match's own time anchor.
    #[serde(default)]
    pub after: Option<String>,
    /// Exclusive absolute RFC 3339 UTC timestamp (`Z`).
    #[serde(default)]
    pub before: Option<String>,
    /// With a time filter, also return matches whose timestamp is unknown.
    #[serde(default)]
    pub include_unknown_time: bool,
    /// Reorder only near-tied sessions (scores within 1%) newest first.
    #[serde(default)]
    pub prefer_recent: bool,
    /// Sessions to return (default 10, maximum 50).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Matches per session (default and maximum 3).
    #[serde(default)]
    pub matches_per_session: Option<usize>,
    /// Include per-match ranking diagnostics.
    #[serde(default)]
    pub debug_scores: bool,
    /// Literal mode only: case-sensitive substring match (default true).
    #[serde(default)]
    pub case_sensitive: Option<bool>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct Warning {
    /// Stable machine-readable code, e.g. `query_embedding_unavailable`.
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct Coverage {
    pub indexed_sessions: i64,
    pub active_chunks: i64,
    /// Active chunks that have a vector for the active embedding profile.
    pub embedded_chunks: i64,
    pub pending_sessions: i64,
    pub last_successful_discovery_at: Option<String>,
    pub scope: String,
}

impl Coverage {
    pub(super) fn from_row(r: &CoverageRow) -> Self {
        Self {
            indexed_sessions: r.indexed_sessions,
            active_chunks: r.active_chunks,
            embedded_chunks: r.embedded_chunks,
            pending_sessions: r.pending_sessions,
            last_successful_discovery_at: r.last_scan_ms.map(format_rfc3339),
            scope: "configured bootstrap, subsequent activity, and manual backfills; older or unindexed sessions are not searched".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct DebugScores {
    pub fused_score: f64,
    pub lexical_rank: Option<usize>,
    /// SQLite bm25 (lower is better).
    pub lexical_bm25: Option<f64>,
    pub semantic_rank: Option<usize>,
    pub semantic_cosine: Option<f64>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct MatchResult {
    /// Signed, view-independent reference for read_session.
    pub match_ref: String,
    pub match_ref_expires_at: String,
    /// Present only when needed to disambiguate the read path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_ref: Option<String>,
    pub matched_at: Option<String>,
    /// At most 1,200 UTF-8 bytes of indexed text.
    pub excerpt: String,
    /// Subset of `lexical`, `semantic`, `literal`.
    pub matched_by: Vec<String>,
    /// True when contextual text in the excerpt lies outside the requested time interval.
    pub context_outside_time_filter: bool,
    /// True when the excerpt is shorter than the matched text.
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debug: Option<DebugScores>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct SessionResult {
    pub session_id: String,
    pub source: String,
    pub session_kind: String,
    /// Native title metadata, never generated.
    pub title: Option<String>,
    /// `native` when `title` is set.
    pub title_origin: Option<String>,
    pub project_path: Option<String>,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub source_status: String,
    pub matches: Vec<MatchResult>,
}

impl SessionResult {
    pub(super) fn new(r: SessionRow, matches: Vec<MatchResult>) -> Self {
        Self {
            session_id: r.id,
            source: r.source_id,
            session_kind: r.kind,
            title_origin: r.title.as_ref().map(|_| "native".to_string()),
            title: r.title,
            project_path: r.cwd,
            started_at: r.created_at.map(format_rfc3339),
            last_activity_at: r.last_activity_at.map(format_rfc3339),
            source_status: r.source_status,
            matches,
        }
    }

    pub(super) fn with_matches(&self, matches: Vec<MatchResult>) -> Self {
        Self {
            matches,
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(crate = "schemars")]
pub struct SearchOutput {
    pub schema_version: u32,
    pub mode_requested: Mode,
    pub mode_used: Mode,
    pub index_epoch: i64,
    pub coverage: Coverage,
    pub results: Vec<SessionResult>,
    pub warnings: Vec<Warning>,
    /// True when matches were dropped to fit the response size limit.
    pub truncated: bool,
    /// Set when the query had no searchable word tokens; retry with `mode: "literal"`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suggest_literal: bool,
}
