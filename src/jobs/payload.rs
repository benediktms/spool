//! Job payloads (spec 11.2): small, versioned, references only. No transcript text and no file
//! paths: a task rereads the catalog and resolves the approved path itself.

use serde::{Deserialize, Serialize};

pub const PAYLOAD_VERSION: u32 = 1;
/// Apalis queue (`job_type`) and worker names. One ingestion worker, one control worker.
pub const INGEST_QUEUE: &str = "spool-ingest";
pub const CONTROL_QUEUE: &str = "spool-control";
pub const INGEST_WORKER: &str = "spool-ingest";
pub const CONTROL_WORKER: &str = "spool-control";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexSession {
    pub payload_version: u32,
    pub session_id: String,
    /// `size:mtime_ms:dev:ino` observed when the task was created; the handler rereads the catalog.
    pub observed_source_revision: String,
    pub pipeline_fingerprint: String,
    pub embedding_profile_id: String,
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiscoverySelection {
    Auto,
    Since { since_ms: i64 },
    All,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoverSessions {
    pub payload_version: u32,
    /// Empty = every enabled source.
    pub source_ids: Vec<String>,
    pub selection: DiscoverySelection,
    /// Explicit operations carry their id; periodic discovery has none (no table churn per tick).
    pub operation_id: Option<String>,
    /// Periodic discovery re-arms itself with Apalis scheduling (`run_after`) when it finishes.
    pub recurring: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceScope {
    /// Lease sweep, retired data, completed-job pruning, bounded checkpoint, grace-period purge.
    Routine,
    /// `index purge --unreferenced`: also purge unreferenced embeddings without the grace period.
    PurgeUnreferenced,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintainIndex {
    pub payload_version: u32,
    pub operation_id: Option<String>,
    pub scope: MaintenanceScope,
    pub recurring: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RebuildIndex {
    pub payload_version: u32,
    pub operation_id: String,
    /// Rebuild text/FTS (clears and recreates derived search data of already indexed sessions).
    pub lexical: bool,
    /// Re-embed chunks lacking vectors for the active profile.
    pub embeddings: bool,
}

/// Everything the control/maintenance worker runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "job", rename_all = "snake_case")]
pub enum ControlJob {
    Discover(DiscoverSessions),
    Maintain(MaintainIndex),
    Rebuild(RebuildIndex),
}

impl ControlJob {
    pub fn payload_version(&self) -> u32 {
        match self {
            ControlJob::Discover(j) => j.payload_version,
            ControlJob::Maintain(j) => j.payload_version,
            ControlJob::Rebuild(j) => j.payload_version,
        }
    }
}
