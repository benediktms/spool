//! Staging: safe source open, finite observed prefix, streaming parse on a blocking thread into
//! bounded async batched inserts (spec 5.7, 8, 10.4). Nothing here is visible to readers until
//! `publish` switches `sessions.active_snapshot_id`.

use crate::adapters::opencode::{ProjectedMessage, emit_projection};
use crate::adapters::{AdapterError, ParseOptions, RecordSink, SinkError, adapter_for};
use crate::clock::SharedClock;
use crate::config::{Config, IngestionConfig};
use crate::domain::*;
use crate::graph::SessionGraph;
use crate::ids::new_id;
use crate::source::opencode::{self, GuardedDb, OpenCodeError, ScanControl};
pub use crate::source::revision4;
use crate::source::{ObservationChange, SourceError};
use crate::sources::SourceRegistry;
use crate::storage::index::Index;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Ingest CPU permit (spec 4.1): parse and chunk planning run one at a time, process-wide.
/// The permit is acquired BEFORE `spawn_blocking`.
pub(crate) static CPU: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

/// Parse->insert channel bound: by event count AND by approximate bytes in flight.
const CHANNEL_EVENTS: usize = 256;
const CHANNEL_BYTES: usize = 8 << 20;
const BATCH_EVENTS: usize = 256;
const BATCH_BYTES: usize = 1 << 20;
const FINALIZE_BATCH: usize = 500;
const MAX_NATIVE_PROJECTION_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotKind {
    Indexed,
    SourceRead,
}

pub struct Staged {
    pub snapshot_id: String,
    /// Session generation observed when staging started; `publish` guards on it.
    pub generation: i64,
    pub manifest: ParsedSessionManifest,
    pub retained_bytes: i64,
    pub warning_count: usize,
    /// Graph over the retained messages (superseded representations removed).
    pub graph: SessionGraph,
    /// The source changed (grew) while parsing, or ends in an incomplete record: queue a follow-up.
    pub follow_up: bool,
    /// Source-specific persisted observation.
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageError {
    Unsupported(String),
    Cancelled,
    Limit(&'static str),
    SourceChanged,
    SourceMissing,
    Source(String),
    Storage(String),
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(m) => write!(f, "unsupported source format: {m}"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Limit(n) => write!(f, "limit exceeded: {n}"),
            Self::SourceChanged => f.write_str("source changed while parsing"),
            Self::SourceMissing => f.write_str("source missing"),
            Self::Source(m) => write!(f, "source: {m}"),
            Self::Storage(_) => f.write_str("storage error"),
        }
    }
}
impl std::error::Error for StageError {}

impl From<sqlx::Error> for StageError {
    fn from(e: sqlx::Error) -> Self {
        // Raw sqlx text can quote SQL values; keep only the category.
        Self::Storage(match &e {
            sqlx::Error::Database(d) => {
                format!("database error code {}", d.code().unwrap_or_default())
            }
            _ => "sqlx error".to_string(),
        })
    }
}

/// What the staging snapshot recorded about the session; `publish` copies it into `sessions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageMeta {
    pub title: Option<String>,
    pub cwd: Option<String>,
    #[serde(default)]
    pub remote: Option<String>,
    pub session_kind: SessionKind,
    pub lineage: Option<String>,
    pub lineage_verified: bool,
    pub started_at: Option<TimestampMs>,
    pub last_activity_at: Option<TimestampMs>,
    pub counts: ParseCounts,
    pub warnings: usize,
    pub chunker_version: u32,
    pub follow_up: bool,
}

pub fn kind_str(k: SessionKind) -> &'static str {
    match k {
        SessionKind::Interactive => "interactive",
        SessionKind::Subagent => "subagent",
        SessionKind::Unknown => "unknown",
    }
}

fn record_kind_str(k: RecordKind) -> &'static str {
    match k {
        RecordKind::Header => "header",
        RecordKind::Message => "message",
        RecordKind::ToolCall => "tool_call",
        RecordKind::ToolResult => "tool_result",
        RecordKind::Control => "control",
        RecordKind::Summary => "summary",
        RecordKind::Unknown => "unknown",
    }
}

// ---------------------------------------------------------------------------------------------
// Catalog access shared with the engine
// ---------------------------------------------------------------------------------------------

pub(crate) struct SessionRow {
    pub source_id: String,
    pub native_id: String,
    pub generation: i64,
    pub active_snapshot_id: Option<String>,
    pub index_state: String,
    pub index_error: Option<String>,
    pub tombstoned: bool,
    pub excluded: bool,
    pub path: Option<String>,
}

pub(crate) async fn load_session(
    index: &Index,
    session_id: &str,
) -> Result<Option<SessionRow>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT s.source_id, s.native_id, s.generation, s.active_snapshot_id, s.index_state, s.index_error, \
                s.tombstoned_at IS NOT NULL AS tomb, f.path, \
                (s.excluded = 1 OR EXISTS (SELECT 1 FROM exclusions e WHERE (e.kind='session' AND e.target_id = s.id) \
                                           OR (e.kind='source' AND e.target_id = s.source_id))) AS excl \
         FROM sessions s LEFT JOIN source_files f ON f.session_id = s.id WHERE s.id = ?",
    )
    .bind(session_id)
    .fetch_optional(&index.writer)
    .await?;
    Ok(row.map(|r| SessionRow {
        source_id: r.get("source_id"),
        native_id: r.get("native_id"),
        generation: r.get("generation"),
        active_snapshot_id: r.get("active_snapshot_id"),
        index_state: r.get("index_state"),
        index_error: r.get("index_error"),
        tombstoned: r.get::<i64, _>("tomb") != 0,
        excluded: r.get::<i64, _>("excl") != 0,
        path: r.get("path"),
    }))
}

fn map_open(e: SourceError) -> StageError {
    match e {
        SourceError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
            StageError::SourceMissing
        }
        SourceError::Io(_) => StageError::Source("source io error".into()),
        SourceError::OutsideRoot => StageError::Source("source is outside enabled roots".into()),
        SourceError::UnsafeFile => StageError::Source("source is not a safe regular file".into()),
    }
}

/// Safe open of the catalog path (never a request-supplied path) and record it in `source_files`.
pub(crate) async fn open_recorded(
    index: &Index,
    sources: &SourceRegistry,
    clock: &SharedClock,
    session_id: &str,
    source_id: &str,
    path: Option<&str>,
) -> Result<crate::source::OpenedSource, StageError> {
    let path = path.ok_or(StageError::SourceMissing)?;
    let opened = sources.open(source_id, Path::new(path)).map_err(map_open)?;
    let i = &opened.info;
    sqlx::query(
        "UPDATE source_files SET dev=?, ino=?, size=?, mtime_ms=?, exists_status='present', last_checked_at=? WHERE session_id=?",
    )
    .bind(i.dev as i64)
    .bind(i.ino as i64)
    .bind(i.len as i64)
    .bind(i.mtime_ms)
    .bind(clock.now_ms())
    .bind(session_id)
    .execute(&index.writer)
    .await?;
    Ok(opened)
}

pub async fn delete_snapshot(index: &Index, snapshot_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM snapshots WHERE id = ? AND state IN ('staging','read_only')")
        .bind(snapshot_id)
        .execute(&index.writer)
        .await?;
    Ok(())
}

/// Delete leftover staging snapshots (crashed or superseded stages). `session_id = None` sweeps all
/// sessions; `older_than_ms` protects stages that are still running.
pub async fn sweep_staging(
    index: &Index,
    session_id: Option<&str>,
    created_before_ms: i64,
) -> Result<u64, sqlx::Error> {
    let r = sqlx::query("DELETE FROM snapshots WHERE state = 'staging' AND created_at < ? AND (? IS NULL OR session_id = ?)")
        .bind(created_before_ms)
        .bind(session_id)
        .bind(session_id)
        .execute(&index.writer)
        .await?;
    Ok(r.rows_affected())
}

// ---------------------------------------------------------------------------------------------
// Parse thread -> bounded channel
// ---------------------------------------------------------------------------------------------

enum Ev {
    Record(RecordMeta, Option<String>),
    Message(MessageEntry),
    Call(ToolCallMeta),
    Result(ToolResultMeta),
    Warn(ParseWarning),
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum NativeLocator<'a> {
    #[serde(rename = "opencode_sqlite_v1")]
    OpenCodeSqlite {
        session_id: &'a str,
        message_id: &'a str,
    },
}

impl Ev {
    fn size(&self) -> usize {
        match self {
            Ev::Record(r, locator) => {
                192 + r.key.len()
                    + r.parent_key.as_ref().map_or(0, |k| k.len())
                    + locator.as_ref().map_or(0, |s| s.len())
            }
            Ev::Message(m) => {
                128 + m.blocks.iter().map(|b| b.text.len() + 48).sum::<usize>()
                    + m.supersedes.iter().map(|s| s.len()).sum::<usize>()
            }
            Ev::Call(c) => 96 + c.tool_name.len(),
            Ev::Result(_) => 96,
            Ev::Warn(w) => 96 + w.detail.len(),
        }
    }
}

struct ChanSink {
    tx: mpsc::Sender<(Ev, OwnedSemaphorePermit)>,
    bytes: Arc<Semaphore>,
    handle: tokio::runtime::Handle,
    cancel: Arc<AtomicBool>,
}

impl ChanSink {
    fn send(&mut self, ev: Ev) -> Result<(), SinkError> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(SinkError);
        }
        let n = ev.size().clamp(1, CHANNEL_BYTES) as u32;
        let permit = self
            .handle
            .block_on(self.bytes.clone().acquire_many_owned(n))
            .map_err(|_| SinkError)?;
        self.tx.blocking_send((ev, permit)).map_err(|_| SinkError)
    }
}

impl RecordSink for ChanSink {
    fn record(&mut self, rec: RecordMeta) -> Result<(), SinkError> {
        self.send(Ev::Record(rec, None))
    }
    fn opencode_record(
        &mut self,
        rec: RecordMeta,
        session_id: &str,
        message_id: &str,
    ) -> Result<(), SinkError> {
        if session_id.is_empty() || message_id.is_empty() {
            return Err(SinkError);
        }
        let locator = serde_json::to_string(&NativeLocator::OpenCodeSqlite {
            session_id,
            message_id,
        })
        .map_err(|_| SinkError)?;
        self.send(Ev::Record(rec, Some(locator)))
    }
    fn message(&mut self, msg: MessageEntry) -> Result<(), SinkError> {
        self.send(Ev::Message(msg))
    }
    fn tool_call(&mut self, call: ToolCallMeta) -> Result<(), SinkError> {
        self.send(Ev::Call(call))
    }
    fn tool_result(&mut self, result: ToolResultMeta) -> Result<(), SinkError> {
        self.send(Ev::Result(result))
    }
    fn warning(&mut self, w: ParseWarning) -> Result<(), SinkError> {
        self.send(Ev::Warn(w))
    }
    fn cancelled(&self) -> &AtomicBool {
        &self.cancel
    }
}

fn map_adapter(e: AdapterError) -> StageError {
    match e {
        AdapterError::Unsupported(m) => StageError::Unsupported(m),
        AdapterError::LimitExceeded => StageError::Limit("max_record_bytes"),
        AdapterError::Io(_) => StageError::Source("source io error".into()),
        AdapterError::LocatorMismatch(_) => StageError::SourceChanged,
        AdapterError::Cancelled => StageError::Cancelled,
    }
}

/// In-memory side of the insert loop: everything needed to build the graph afterwards.
struct Inserter<'a> {
    index: &'a Index,
    snapshot_id: &'a str,
    superseded: HashSet<String>,
    alias: HashMap<String, String>,
    records: Vec<RecordMeta>,
    msg_keys: Vec<String>,
    msg_roles: HashMap<String, Role>,
    /// Accumulated-update groups (a superseding record and everything it supersedes): the only
    /// scope in which a re-emitted native tool id is an upsert (ids repeat across turns/branches).
    group_of: HashMap<String, usize>,
    groups: Vec<Vec<String>>,
    ord: i64,
    warn_seq: i64,
}

impl Inserter<'_> {
    fn new<'a>(index: &'a Index, snapshot_id: &'a str) -> Inserter<'a> {
        Inserter {
            index,
            snapshot_id,
            superseded: HashSet::new(),
            alias: HashMap::new(),
            records: Vec::new(),
            msg_keys: Vec::new(),
            msg_roles: HashMap::new(),
            group_of: HashMap::new(),
            groups: Vec::new(),
            ord: 0,
            warn_seq: 0,
        }
    }

    async fn flush(&mut self, batch: Vec<Ev>) -> Result<(), StageError> {
        let sid = self.snapshot_id;
        let mut tx = self.index.writer.begin().await?;
        for ev in batch {
            match ev {
                Ev::Record(r, locator) => {
                    let res = sqlx::query(
                        "INSERT INTO records(snapshot_id,key,ord,native_id,parent_key,kind,ts,offset,len,digest,turn_id,locator_payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    )
                    .bind(sid)
                    .bind(&r.key)
                    .bind(self.ord)
                    .bind(&r.native_id)
                    .bind(&r.parent_key)
                    .bind(record_kind_str(r.kind))
                    .bind(r.timestamp)
                    .bind(r.locator.offset as i64)
                    .bind(r.locator.len as i64)
                    .bind(&r.digest[..])
                    .bind(&r.turn_id)
                    .bind(locator)
                    .execute(&mut *tx)
                    .await;
                    match res {
                        Ok(_) => {}
                        Err(sqlx::Error::Database(d)) if d.is_unique_violation() => {
                            return Err(StageError::Source(
                                "adapter emitted a duplicate record key".into(),
                            ));
                        }
                        Err(e) => return Err(e.into()),
                    }
                    self.ord += 1;
                    self.records.push(r);
                }
                Ev::Message(m) => {
                    if !m.supersedes.is_empty() {
                        let g = m
                            .supersedes
                            .iter()
                            .chain(std::iter::once(&m.record_key))
                            .find_map(|k| self.group_of.get(k).copied())
                            .unwrap_or_else(|| {
                                self.groups.push(Vec::new());
                                self.groups.len() - 1
                            });
                        for k in m.supersedes.iter().chain(std::iter::once(&m.record_key)) {
                            if self.group_of.insert(k.clone(), g).is_none() {
                                self.groups[g].push(k.clone());
                            }
                        }
                    }
                    for s in &m.supersedes {
                        self.superseded.insert(s.clone());
                        self.alias.insert(s.clone(), m.record_key.clone());
                        // Their records and tool rows stay (ancestry, raw view, tool provenance); only the
                        // conversational message/blocks/attachments go.
                        sqlx::query("DELETE FROM messages WHERE snapshot_id=? AND record_key=?")
                            .bind(sid)
                            .bind(s)
                            .execute(&mut *tx)
                            .await?;
                    }
                    if self.superseded.contains(&m.record_key) {
                        continue;
                    }
                    let role = match m.role {
                        Role::User => "user",
                        Role::Assistant => "assistant",
                    };
                    let seq = self.msg_keys.len() as i64; // provisional; finalized after the graph is known
                    sqlx::query(
                        "INSERT INTO messages(snapshot_id,record_key,seq,role,phase,ts,parent_message_key) \
                         SELECT ?,?,?,?,?,ts,NULL FROM records WHERE snapshot_id=? AND key=?",
                    )
                    .bind(sid)
                    .bind(&m.record_key)
                    .bind(seq)
                    .bind(role)
                    .bind(&m.phase)
                    .bind(sid)
                    .bind(&m.record_key)
                    .execute(&mut *tx)
                    .await?;
                    for b in &m.blocks {
                        sqlx::query("INSERT INTO blocks(snapshot_id,record_key,idx,origin,text,text_digest) VALUES (?,?,?,?,?,?)")
                            .bind(sid)
                            .bind(&m.record_key)
                            .bind(b.index as i64)
                            .bind(match b.origin {
                                BlockOrigin::Authored => "authored",
                                BlockOrigin::Unknown => "unknown",
                            })
                            .bind(&b.text)
                            .bind(&digest(b.text.as_bytes())[..])
                            .execute(&mut *tx)
                            .await?;
                    }
                    for a in &m.attachments {
                        sqlx::query("INSERT INTO attachments(snapshot_id,record_key,idx,kind,mime,bytes) VALUES (?,?,?,?,?,?)")
                            .bind(sid)
                            .bind(&m.record_key)
                            .bind(a.index as i64)
                            .bind(&a.kind)
                            .bind(&a.mime)
                            .bind(a.bytes.map(|b| b as i64))
                            .execute(&mut *tx)
                            .await?;
                    }
                    self.msg_roles.insert(m.record_key.clone(), m.role);
                    self.msg_keys.push(m.record_key);
                }
                Ev::Call(c) => {
                    // Tool provenance survives message supersession; a native id re-emitted by a later record of
                    // the same accumulated-update group is upserted (row points at the latest record/block).
                    // Other records, turns and branches may legitimately reuse ids; no native id: never merged.
                    if let (Some(id), Some(&g)) =
                        (&c.native_call_id, self.group_of.get(&c.record_key))
                    {
                        for member in self.groups[g].iter().filter(|k| **k != c.record_key) {
                            let old: Vec<i64> = sqlx::query_scalar("SELECT block_index FROM tool_calls WHERE snapshot_id=? AND native_call_id=? AND record_key=?").bind(sid).bind(id).bind(member).fetch_all(&mut *tx).await?;
                            for bi in old {
                                // Results that linked the replaced row follow it to the latest record/block.
                                sqlx::query("UPDATE tool_results SET call_record_key=?, call_block_index=? WHERE snapshot_id=? AND call_record_key=? AND call_block_index=?")
                                    .bind(&c.record_key)
                                    .bind(c.block_index as i64)
                                    .bind(sid)
                                    .bind(member)
                                    .bind(bi)
                                    .execute(&mut *tx)
                                    .await?;
                            }
                            sqlx::query("DELETE FROM tool_calls WHERE snapshot_id=? AND native_call_id=? AND record_key=?").bind(sid).bind(id).bind(member).execute(&mut *tx).await?;
                        }
                    }
                    sqlx::query("INSERT INTO tool_calls(snapshot_id,record_key,block_index,native_call_id,tool_name,turn_id) VALUES (?,?,?,?,?,?)")
                        .bind(sid)
                        .bind(&c.record_key)
                        .bind(c.block_index as i64)
                        .bind(&c.native_call_id)
                        .bind(&c.tool_name)
                        .bind(&c.turn_id)
                        .execute(&mut *tx)
                        .await?;
                }
                Ev::Result(r) => {
                    if let (Some(id), Some(&g)) =
                        (&r.native_call_id, self.group_of.get(&r.record_key))
                    {
                        for member in self.groups[g].iter().filter(|k| **k != r.record_key) {
                            sqlx::query("DELETE FROM tool_results WHERE snapshot_id=? AND native_call_id=? AND record_key=? AND call_record_key IS ? AND call_block_index IS ?")
                                .bind(sid)
                                .bind(id)
                                .bind(member)
                                .bind(r.call.as_ref().map(|c| c.0.clone()))
                                .bind(r.call.as_ref().map(|c| c.1 as i64))
                                .execute(&mut *tx)
                                .await?;
                        }
                    }
                    sqlx::query(
                        "INSERT INTO tool_results(snapshot_id,record_key,block_index,native_call_id,call_record_key,call_block_index,is_error,turn_id) VALUES (?,?,?,?,?,?,?,?)",
                    )
                    .bind(sid)
                    .bind(&r.record_key)
                    .bind(r.block_index as i64)
                    .bind(&r.native_call_id)
                    .bind(r.call.as_ref().map(|c| c.0.clone()))
                    .bind(r.call.as_ref().map(|c| c.1 as i64))
                    .bind(r.is_error.map(|b| b as i64))
                    .bind(&r.turn_id)
                    .execute(&mut *tx)
                    .await?;
                }
                Ev::Warn(w) => self.insert_warning(&mut tx, &w).await?,
            }
        }
        tx.commit().await?;
        Ok(())
    }

    async fn insert_warning(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        w: &ParseWarning,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO parse_warnings(snapshot_id,seq,code,record_key,offset,len,detail) VALUES (?,?,?,?,?,?,?)")
            .bind(self.snapshot_id)
            .bind(self.warn_seq)
            .bind(&w.code)
            .bind(&w.record_key)
            .bind(w.locator.map(|l| l.offset as i64))
            .bind(w.locator.map(|l| l.len as i64))
            .bind(&w.detail)
            .execute(&mut **tx)
            .await?;
        self.warn_seq += 1;
        Ok(())
    }
}

async fn consume(
    ins: &mut Inserter<'_>,
    mut rx: mpsc::Receiver<(Ev, OwnedSemaphorePermit)>,
) -> Result<(), StageError> {
    loop {
        let Some(first) = rx.recv().await else {
            return Ok(());
        };
        let mut bytes = first.0.size();
        let mut batch = vec![first];
        while batch.len() < BATCH_EVENTS && bytes < BATCH_BYTES {
            match rx.try_recv() {
                Ok(e) => {
                    bytes += e.0.size();
                    batch.push(e);
                }
                Err(_) => break,
            }
        }
        // Permits are released as soon as the batch is handed over to the transaction.
        let evs: Vec<Ev> = batch.into_iter().map(|(e, _p)| e).collect();
        ins.flush(evs).await?;
    }
}

/// Test seam: called right after the parse finished, before the source is re-checked.
pub type AfterParse = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
mod opencode_tests {
    use super::*;
    use crate::source::opencode::{self, ScanControl};
    use sqlx::{Connection, Executor, sqlite::SqliteConnectOptions};

    #[tokio::test]
    async fn opencode_projection_writes_native_locators_through_staging_sink() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.db");
        let mut writer = sqlx::SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&source)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        writer.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT,title TEXT,time_updated INTEGER); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            INSERT INTO session_message VALUES ('m1','native','user',1,'{\"text\":\"matchable text\"}');").await.unwrap();
        let index = Index::open_for_test(dir.path()).await.unwrap();
        sqlx::query(
            "INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('oc','opencode','[]',1)",
        )
        .execute(&index.writer)
        .await
        .unwrap();
        sqlx::query("INSERT INTO sessions(id,source_id,native_id,discriminator,session_kind,eligibility,index_state,discovered_at) VALUES ('s','oc','native','','interactive','active','pending',1)")
            .execute(&index.writer).await.unwrap();
        sqlx::query("INSERT INTO snapshots(id,session_id,kind,state,source_revision,fingerprint,generation,prefix_end,file_dev,file_ino,created_at) VALUES ('snap','s','indexed','staging','r','f',0,0,0,0,1)")
            .execute(&index.writer).await.unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let control = ScanControl {
            cancel: &cancel,
            deadline: None,
        };
        let mut db = opencode::open(&source).await.unwrap();
        let projection = opencode::project_session(&mut db, "native", 4096, &control)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(CHANNEL_EVENTS);
        let handle = tokio::runtime::Handle::current();
        let join = tokio::task::spawn_blocking(move || {
            let mut sink = ChanSink {
                tx,
                bytes: Arc::new(Semaphore::new(CHANNEL_BYTES)),
                handle,
                cancel,
            };
            emit_projection("native", projection, &mut sink)
        });
        let mut ins = Inserter::new(&index, "snap");
        consume(&mut ins, rx).await.unwrap();
        join.await.unwrap().unwrap();
        let (locator, text): (String, String) = sqlx::query_as(
            "SELECT r.locator_payload,b.text FROM records r JOIN blocks b ON b.snapshot_id=r.snapshot_id AND b.record_key=r.key WHERE r.snapshot_id='snap'",
        ).fetch_one(&index.reader).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&locator).unwrap(),
            serde_json::json!({
                "kind":"opencode_sqlite_v1","session_id":"native","message_id":"m1"
            })
        );
        assert_eq!(text, "matchable text");
    }
}

/// Shared inputs of one staging pass: where the session lives and how the snapshot is stamped.
#[derive(Clone, Copy)]
pub struct StageContext<'a> {
    pub index: &'a Index,
    pub sources: &'a SourceRegistry,
    pub cfg: &'a Config,
    pub clock: &'a SharedClock,
    pub fingerprint: &'a str,
}

struct SnapshotStamp<'a> {
    revision: &'a str,
    dev: Option<i64>,
    ino: Option<i64>,
}

async fn create_snapshot(
    ctx: &StageContext<'_>,
    session_id: &str,
    row: &SessionRow,
    kind: SnapshotKind,
    stamp: SnapshotStamp<'_>,
) -> Result<String, StageError> {
    let snapshot_id = new_id("snap");
    sqlx::query(
        "INSERT INTO snapshots(id,session_id,kind,state,source_revision,fingerprint,generation,prefix_end,file_dev,file_ino,created_at) \
         VALUES (?,?,?,'staging',?,?,?,0,?,?,?)",
    )
    .bind(&snapshot_id)
    .bind(session_id)
    .bind(if kind == SnapshotKind::Indexed { "indexed" } else { "source_read" })
    .bind(stamp.revision)
    .bind(ctx.fingerprint)
    .bind(row.generation)
    .bind(stamp.dev)
    .bind(stamp.ino)
    .bind(ctx.clock.now_ms())
    .execute(&ctx.index.writer)
    .await?;
    Ok(snapshot_id)
}

pub async fn stage_snapshot(
    ctx: &StageContext<'_>,
    session_id: &str,
    kind: SnapshotKind,
    cancel: Arc<AtomicBool>,
) -> Result<Staged, StageError> {
    stage_with_hook(ctx, session_id, kind, cancel, None).await
}

pub(crate) async fn stage_with_hook(
    ctx: &StageContext<'_>,
    session_id: &str,
    kind: SnapshotKind,
    cancel: Arc<AtomicBool>,
    after_parse: Option<AfterParse>,
) -> Result<Staged, StageError> {
    let StageContext {
        index,
        sources,
        cfg,
        clock,
        ..
    } = *ctx;

    let row = load_session(index, session_id)
        .await?
        .ok_or_else(|| StageError::Source("unknown session".into()))?;
    let harness = sources
        .kind(&row.source_id)
        .ok_or_else(|| StageError::Source("source is not configured".into()))?;
    if harness == HarnessKind::OpenCode {
        let path = sources
            .opencode_database_path(&row.source_id)
            .filter(|_| sources.is_enabled(&row.source_id))
            .ok_or_else(|| StageError::Source("source is not enabled".into()))?;
        let mut native = NativeObservation::open(index, &row, &path, &cancel).await?;
        let staged = native
            .stage(ctx, session_id, &row, kind, cancel.clone(), after_parse)
            .await?;
        if let Err(error) = native.verify(index, &row, &cancel).await {
            let _ = delete_snapshot(index, &staged.snapshot_id).await;
            return Err(error);
        }
        return Ok(staged);
    }
    let opened = open_recorded(
        index,
        sources,
        clock,
        session_id,
        &row.source_id,
        row.path.as_deref(),
    )
    .await?;
    let info = opened.info.clone();
    if info.len > cfg.ingestion.max_source_file_bytes {
        return Err(StageError::Limit("max_source_file_bytes"));
    }
    let snapshot_id = create_snapshot(
        ctx,
        session_id,
        &row,
        kind,
        SnapshotStamp {
            revision: &revision4(&info),
            dev: Some(info.dev as i64),
            ino: Some(info.ino as i64),
        },
    )
    .await?;

    let res = run(
        Pass {
            ctx: *ctx,
            row: &row,
            harness,
            kind,
            snapshot_id: &snapshot_id,
            cancel,
            after_parse,
        },
        opened,
    )
    .await;
    if res.is_err() {
        let _ = delete_snapshot(index, &snapshot_id).await;
    }
    res
}

pub(crate) fn map_native_error(error: OpenCodeError) -> StageError {
    match error {
        OpenCodeError::Unsupported => StageError::Unsupported("OpenCode storage schema".into()),
        OpenCodeError::Moved | OpenCodeError::Changed => StageError::SourceChanged,
        OpenCodeError::UnsafeSource | OpenCodeError::Unavailable(_) => {
            StageError::Source("OpenCode storage is unavailable".into())
        }
        OpenCodeError::Limit => StageError::Limit("max_source_file_bytes"),
        OpenCodeError::Cancelled => StageError::Cancelled,
    }
}

pub(crate) struct NativeObservation {
    pub db: GuardedDb,
    session: opencode::Session,
    pub seq: i64,
    deadline: Instant,
}

impl NativeObservation {
    pub(crate) async fn open(
        index: &Index,
        row: &SessionRow,
        path: &Path,
        cancel: &AtomicBool,
    ) -> Result<Self, StageError> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let control = ScanControl {
            cancel,
            deadline: Some(deadline),
        };
        let mut db = opencode::open_session(index, &row.source_id, path, &row.native_id, &control)
            .await
            .map_err(map_native_error)?;
        let (session, seq) =
            opencode::observe_session(index, &row.source_id, &mut db, &row.native_id, &control)
                .await
                .map_err(map_native_error)?
                .ok_or(StageError::SourceMissing)?;
        Ok(Self {
            db,
            session,
            seq,
            deadline,
        })
    }

    pub(crate) fn revision(&self) -> String {
        format!("opencode_sqlite_v1:{}", self.seq)
    }

    pub(crate) async fn verify(
        &mut self,
        index: &Index,
        row: &SessionRow,
        cancel: &AtomicBool,
    ) -> Result<(), StageError> {
        let control = ScanControl {
            cancel,
            deadline: Some(self.deadline),
        };
        let observed = opencode::observe_session(
            index,
            &row.source_id,
            &mut self.db,
            &row.native_id,
            &control,
        )
        .await
        .map_err(map_native_error)?;
        if observed
            .as_ref()
            .is_some_and(|(session, seq)| *seq == self.seq && *session == self.session)
        {
            Ok(())
        } else {
            Err(StageError::SourceChanged)
        }
    }

    pub(crate) async fn project(
        &mut self,
        index: &Index,
        limits: &IngestionConfig,
        row: &SessionRow,
        cancel: &AtomicBool,
    ) -> Result<Vec<ProjectedMessage>, StageError> {
        let limit_name = if limits.max_source_file_bytes <= MAX_NATIVE_PROJECTION_BYTES {
            "max_source_file_bytes"
        } else {
            "max_native_projection_bytes"
        };
        let max = usize::try_from(
            limits
                .max_source_file_bytes
                .min(MAX_NATIVE_PROJECTION_BYTES),
        )
        .map_err(|_| StageError::Limit(limit_name))?;
        let control = ScanControl {
            cancel,
            deadline: Some(self.deadline),
        };
        let projection =
            match opencode::project_session(&mut self.db, &row.native_id, max, &control).await {
                Ok(projection) => projection,
                Err(error) => {
                    let error = match self.db.checkpoint() {
                        Err(OpenCodeError::Moved) => OpenCodeError::Moved,
                        _ => opencode::classify_lookup_error(error),
                    };
                    opencode::persist_movement(index, &row.source_id, &error)
                        .await
                        .map_err(map_native_error)?;
                    if matches!(error, OpenCodeError::Limit) {
                        return Err(StageError::Limit(limit_name));
                    }
                    return Err(map_native_error(error));
                }
            };
        let max_record = limits.max_record_bytes;
        if projection.iter().any(|message| {
            !u64::try_from(message.source_bytes).is_ok_and(|bytes| bytes <= max_record)
        }) {
            return Err(StageError::Limit("max_record_bytes"));
        }
        Ok(projection)
    }

    pub(crate) async fn stage(
        &mut self,
        ctx: &StageContext<'_>,
        session_id: &str,
        row: &SessionRow,
        kind: SnapshotKind,
        cancel: Arc<AtomicBool>,
        after_parse: Option<AfterParse>,
    ) -> Result<Staged, StageError> {
        let revision = self.revision();
        let snapshot_id = create_snapshot(
            ctx,
            session_id,
            row,
            kind,
            SnapshotStamp {
                revision: &revision,
                dev: None,
                ino: None,
            },
        )
        .await?;
        let staged = async {
            let projection = self
                .project(ctx.index, &ctx.cfg.ingestion, row, &cancel)
                .await?;
            let manifest = native_manifest(&self.session, &projection)?;
            let mut ins = Inserter::new(ctx.index, &snapshot_id);
            let native_id = row.native_id.clone();
            stream_into(&mut ins, cancel.clone(), move |sink| {
                emit_native(&native_id, projection, sink)
            })
            .await?;
            if let Some(hook) = after_parse {
                hook();
            }
            self.verify(ctx.index, row, &cancel).await?;
            finalize(
                ins,
                FinalObservation {
                    generation: row.generation,
                    kind,
                    manifest,
                    revision,
                    pending_tail: None,
                    follow_up: false,
                    chunker_version: ctx.cfg.chunking.version,
                },
            )
            .await
        }
        .await;
        if staged.is_err() {
            let _ = delete_snapshot(ctx.index, &snapshot_id).await;
        }
        staged
    }
}

pub(crate) fn emit_native(
    native_id: &str,
    projection: Vec<ProjectedMessage>,
    sink: &mut dyn RecordSink,
) -> Result<(), StageError> {
    emit_projection(native_id, projection, sink).map_err(|_| {
        if sink.cancelled().load(Ordering::Relaxed) {
            StageError::Cancelled
        } else {
            StageError::Source("cannot stage OpenCode records".into())
        }
    })
}

fn native_manifest(
    session: &opencode::Session,
    projection: &[ProjectedMessage],
) -> Result<ParsedSessionManifest, StageError> {
    let mut counts = ParseCounts::default();
    for message in projection {
        counts.records += 1;
        counts.messages += u64::from(!message.blocks.is_empty());
        counts.blocks_retained += u64::try_from(message.blocks.len())
            .map_err(|_| StageError::Limit("max_source_file_bytes"))?;
        counts.bytes_read = u64::try_from(message.source_bytes)
            .ok()
            .and_then(|bytes| counts.bytes_read.checked_add(bytes))
            .ok_or(StageError::Limit("max_source_file_bytes"))?;
    }
    Ok(ParsedSessionManifest {
        native_session_id: session.id.clone(),
        native_title: session.title.clone(),
        cwd: Some(session.directory.clone()),
        remote: None,
        session_kind: if session.parent_id.is_some() {
            SessionKind::Subagent
        } else {
            SessionKind::Interactive
        },
        parent_native_session: session.parent_id.clone(),
        lineage_verified: session.parent_id.is_some(),
        started_at: None,
        last_activity_at: Some(session.updated_at),
        prefix_end: 0,
        active_leaf_key: projection
            .iter()
            .rfind(|message| !message.blocks.is_empty())
            .map(|message| message.id.clone()),
        coverage: Coverage::Complete,
        counts,
    })
}

/// One staged snapshot being filled: the session row, its harness and the knobs of this pass.
struct Pass<'a> {
    ctx: StageContext<'a>,
    row: &'a SessionRow,
    harness: HarnessKind,
    kind: SnapshotKind,
    snapshot_id: &'a str,
    cancel: Arc<AtomicBool>,
    after_parse: Option<AfterParse>,
}

async fn run(pass: Pass<'_>, opened: crate::source::OpenedSource) -> Result<Staged, StageError> {
    let Pass {
        ctx:
            StageContext {
                index,
                sources,
                cfg,
                ..
            },
        row,
        harness,
        kind,
        snapshot_id,
        cancel,
        after_parse,
    } = pass;
    let info = opened.info.clone();
    let opts = ParseOptions {
        max_record_bytes: cfg.ingestion.max_record_bytes,
        max_source_bytes: cfg.ingestion.max_source_file_bytes,
        observe_len: info.len,
        include_subagents: sources.include_subagents(&row.source_id),
        blob_roots: sources.blob_roots(&row.source_id),
    };
    let source = sources
        .session_source(&row.source_id)
        .ok_or_else(|| StageError::Source("source is outside enabled roots".into()))?;
    let mut ins = Inserter::new(index, snapshot_id);
    let manifest = stream_into(&mut ins, cancel, move |sink| {
        source
            .stream(adapter_for(harness), opened, &opts, sink)
            .map_err(map_adapter)
    })
    .await?;
    if manifest.coverage == Coverage::Unsupported {
        return Err(StageError::Unsupported(
            "source schema version is not supported".into(),
        ));
    }
    if harness == HarnessKind::Factory && manifest.native_session_id != row.native_id {
        return Err(StageError::Unsupported(
            "Factory transcript identity differs from the catalog".into(),
        ));
    }

    if let Some(h) = &after_parse {
        h();
    }
    // Re-check the source: identity, truncation, in-place rewrite, growth (spec 8.2).
    let again = sources
        .open(
            &row.source_id,
            Path::new(row.path.as_deref().unwrap_or_default()),
        )
        .map_err(|e| match map_open(e) {
            StageError::SourceMissing => StageError::SourceChanged,
            other => other,
        })?;
    let grew = match sources
        .verify_observation(&row.source_id, &info, &again, &ins.records)
        .ok_or_else(|| StageError::Source("source is outside enabled roots".into()))?
    {
        ObservationChange::Changed => return Err(StageError::SourceChanged),
        ObservationChange::Grew => true,
        ObservationChange::Unchanged => false,
    };
    // Bytes after `prefix_end` are pending data (an incomplete record): retried only after the NEXT source
    // change, so they never request a follow-up by themselves. Growth observed while parsing does.
    let pending_tail = (manifest.prefix_end < info.len).then(|| SourceLocator {
        offset: manifest.prefix_end,
        len: info.len - manifest.prefix_end,
    });
    let revision = format!("{}:{}", revision4(&info), manifest.prefix_end);
    finalize(
        ins,
        FinalObservation {
            generation: row.generation,
            kind,
            manifest,
            revision,
            pending_tail,
            follow_up: grew,
            chunker_version: cfg.chunking.version,
        },
    )
    .await
}

async fn stream_into<T: Send + 'static>(
    ins: &mut Inserter<'_>,
    cancel: Arc<AtomicBool>,
    stream: impl FnOnce(&mut dyn RecordSink) -> Result<T, StageError> + Send + 'static,
) -> Result<T, StageError> {
    let (tx, rx) = mpsc::channel(CHANNEL_EVENTS);
    let permit = CPU
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| StageError::Cancelled)?;
    if cancel.load(Ordering::Relaxed) {
        return Err(StageError::Cancelled);
    }
    let mut sink = ChanSink {
        tx,
        bytes: Arc::new(Semaphore::new(CHANNEL_BYTES)),
        handle: tokio::runtime::Handle::current(),
        cancel: cancel.clone(),
    };
    let join = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        stream(&mut sink)
    });
    let consumed = consume(ins, rx).await;
    let parsed = join
        .await
        .map_err(|_| StageError::Source("parse task failed".into()))?;
    consumed?;
    if cancel.load(Ordering::Relaxed) {
        return Err(StageError::Cancelled);
    }
    parsed
}

struct FinalObservation {
    generation: i64,
    kind: SnapshotKind,
    manifest: ParsedSessionManifest,
    revision: String,
    pending_tail: Option<SourceLocator>,
    follow_up: bool,
    chunker_version: u32,
}

async fn finalize(
    mut ins: Inserter<'_>,
    observation: FinalObservation,
) -> Result<Staged, StageError> {
    let FinalObservation {
        generation,
        kind,
        manifest,
        revision,
        pending_tail,
        follow_up,
        chunker_version,
    } = observation;
    let index = ins.index;
    let snapshot_id = ins.snapshot_id;
    let live: Vec<String> = ins
        .msg_keys
        .iter()
        .filter(|k| !ins.superseded.contains(*k))
        .cloned()
        .collect();
    let live_pairs: Vec<(String, Role)> =
        live.iter().map(|k| (k.clone(), ins.msg_roles[k])).collect();
    let (graph, gwarns) = build_graph(&ins.records, &live_pairs, &ins.alias);

    // seq + parent_message_key, short transactions.
    for (chunk_no, chunk) in live.chunks(FINALIZE_BATCH).enumerate() {
        let mut tx = index.writer.begin().await?;
        for (i, k) in chunk.iter().enumerate() {
            sqlx::query("UPDATE messages SET seq=?, parent_message_key=? WHERE snapshot_id=? AND record_key=?")
                .bind((chunk_no * FINALIZE_BATCH + i) as i64)
                .bind(graph.message_parent(k))
                .bind(snapshot_id)
                .bind(k)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
    }
    {
        let mut tx = index.writer.begin().await?;
        for w in &gwarns {
            ins.insert_warning(&mut tx, w).await?;
        }
        if let Some(locator) = pending_tail {
            let w = ParseWarning {
                code: "pending_trailing_bytes".into(),
                record_key: None,
                locator: Some(locator),
                detail: "incomplete trailing record is pending until the source changes".into(),
            };
            ins.insert_warning(&mut tx, &w).await?;
        }
        tx.commit().await?;
    }
    let warning_count = ins.warn_seq as usize;
    let retained_bytes: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(length(CAST(text AS BLOB))),0) + (SELECT count(*) FROM records WHERE snapshot_id=?) * 128 FROM blocks WHERE snapshot_id=?",
    )
    .bind(snapshot_id)
    .bind(snapshot_id)
    .fetch_one(&index.writer)
    .await?;

    let partial = manifest.coverage == Coverage::Partial
        || manifest.counts.malformed_records > 0
        || warning_count > 0;
    let coverage = if partial { "partial" } else { "complete" };
    let meta = StageMeta {
        title: manifest.native_title.clone(),
        cwd: manifest.cwd.clone(),
        remote: manifest.remote.clone(),
        session_kind: manifest.session_kind,
        lineage: manifest.parent_native_session.clone(),
        lineage_verified: manifest.lineage_verified,
        started_at: manifest.started_at,
        last_activity_at: manifest.last_activity_at,
        counts: manifest.counts.clone(),
        warnings: warning_count,
        chunker_version,
        follow_up,
    };
    sqlx::query(
        "UPDATE snapshots SET source_revision=?, prefix_end=?, coverage=?, active_leaf_key=?, counts_json=?, retained_bytes=?, \
         state=CASE WHEN ?=1 THEN 'read_only' ELSE 'staging' END WHERE id=?",
    )
    .bind(&revision)
    .bind(manifest.prefix_end as i64)
    .bind(coverage)
    .bind(&manifest.active_leaf_key)
    .bind(serde_json::to_string(&meta).map_err(|_| StageError::Storage("meta encoding".into()))?)
    .bind(retained_bytes)
    .bind((kind == SnapshotKind::SourceRead) as i64)
    .bind(snapshot_id)
    .execute(&index.writer)
    .await?;
    Ok(Staged {
        snapshot_id: snapshot_id.to_string(),
        generation,
        manifest,
        retained_bytes,
        warning_count,
        graph,
        follow_up,
        revision,
    })
}
/// Graph over the retained messages. Physical record parents stay raw; superseded representations
/// reach the graph as `supersedes` of the superseding message, so it resolves aliases itself and
/// skips same-message saves when projecting message parents (rewriting raw parents here would turn
/// a later save's link to an earlier save into a self-parent, a false cycle).
pub(crate) fn build_graph(
    records: &[RecordMeta],
    live: &[(String, Role)],
    alias: &std::collections::HashMap<String, String>,
) -> (SessionGraph, Vec<ParseWarning>) {
    let mut superseded: std::collections::HashMap<&str, Vec<String>> = Default::default();
    for (old, new) in alias {
        superseded.entry(new).or_default().push(old.clone());
    }
    let g_msgs: Vec<MessageEntry> = live
        .iter()
        .map(|(k, role)| MessageEntry {
            record_key: k.clone(),
            role: *role,
            phase: None,
            blocks: vec![],
            attachments: vec![],
            supersedes: superseded.remove(k.as_str()).unwrap_or_default(),
        })
        .collect();
    SessionGraph::build(records, &g_msgs)
}

#[cfg(test)]
mod graph_tests {
    use super::*;

    fn rec(key: &str, parent: Option<&str>) -> RecordMeta {
        RecordMeta {
            key: key.into(),
            native_id: Some(key.into()),
            parent_key: parent.map(str::to_owned),
            kind: RecordKind::Message,
            timestamp: None,
            turn_id: None,
            locator: SourceLocator { offset: 0, len: 1 },
            digest: [0; 32],
        }
    }
    fn live(keys: &[(&str, Role)]) -> Vec<(String, Role)> {
        keys.iter().map(|(k, r)| ((*k).into(), *r)).collect()
    }

    #[test]
    fn repeated_saves_keep_raw_links_without_false_cycle() {
        // u <- m1 <- tool <- m2 <- m3 (three saves of one message), then c and a distinct reply d.
        let records = [
            rec("u", None),
            rec("m1", Some("u")),
            rec("tool", Some("m1")),
            rec("m2", Some("tool")),
            rec("m3", Some("m2")),
            rec("c", Some("m3")),
            rec("d", Some("c")),
        ];
        let alias = HashMap::from([
            ("m1".to_string(), "m3".to_string()),
            ("m2".to_string(), "m3".to_string()),
        ]);
        let live = live(&[
            ("u", Role::User),
            ("m3", Role::Assistant),
            ("c", Role::User),
            ("d", Role::Assistant),
        ]);
        let (graph, warnings) = build_graph(&records, &live, &alias);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(graph.message_parent("m3"), Some("u"));
        assert_eq!(graph.message_parent("c"), Some("m3"));
        assert_eq!(graph.message_parent("d"), Some("c"));
        assert_eq!(graph.path_to("d"), ["u", "m3", "c", "d"]);
    }

    #[test]
    fn genuine_cycle_still_warns() {
        let records = [rec("a", Some("b")), rec("b", Some("a"))];
        let live = live(&[("a", Role::User), ("b", Role::Assistant)]);
        let (_, warnings) = build_graph(&records, &live, &HashMap::new());
        assert!(warnings.iter().any(|w| w.code == "cycle"));
    }
}
