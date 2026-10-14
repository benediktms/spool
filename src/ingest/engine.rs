//! `IndexSession` job body (spec 11.5), independent of Apalis. Idempotent: duplicate delivery or a
//! lost ack observes the already-published revision and becomes a cheap no-op.

use super::chunk::{ChunkConfig, ChunkMessage, ChunkPlan, TokenCounter, plan_chunks};
use super::embed::{EmbedStats, embed_missing};
use super::publish::{PublishOutcome, publish};
use super::stage::{
    self, AfterParse, CPU, SnapshotKind, StageError, Staged, build_graph, load_session,
    open_recorded, revision4, stage_with_hook,
};
use crate::adapters::{ParseOptions, RecordSink, SinkError, adapter_for};
use crate::clock::SharedClock;
use crate::config::Config;
use crate::domain::*;
use crate::embeddings::EmbeddingService;
use crate::ipc::AppError;
use crate::sources::SourceRegistry;
use crate::storage::index::Index;
use crate::transcript::service::{SourceReadStager, StagedRead};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Bump when extraction/filtering policy changes (part of the pipeline fingerprint, spec 9.4).
pub const EXTRACTION_POLICY_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct IndexRequest {
    pub session_id: String,
    /// `size:mtime_ms:dev:ino` observed by discovery (extra components ignored). Never trusted:
    /// the engine re-stats the catalog path and always indexes the current file.
    pub observed_source_revision: Option<String>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferReason {
    /// Rewritten/truncated/replaced while parsing; staging discarded, rediscover.
    SourceChanged,
    SourceDisabled,
    /// `max_record_bytes` | `max_source_file_bytes`; visible as `index_state='failed'`.
    LimitExceeded(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexOutcome {
    NoOp,
    Published {
        snapshot_id: String,
        generation: i64,
        chunks: usize,
        reused_chunks: usize,
        embeddings_pending: usize,
        follow_up_needed: bool,
    },
    /// Already published; missing vectors were generated now.
    Embedded {
        embedded: usize,
    },
    Superseded,
    Excluded,
    Deferred(DeferReason),
    Unsupported,
    SourceMissing,
    /// Published, but some vectors are still missing without an error.
    EmbeddingsPending {
        missing: usize,
        follow_up_needed: bool,
    },
}

impl IndexOutcome {
    pub fn follow_up_needed(&self) -> bool {
        matches!(
            self,
            Self::Published {
                follow_up_needed: true,
                ..
            } | Self::EmbeddingsPending {
                follow_up_needed: true,
                ..
            }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestErrorKind {
    Transient,
    Permanent,
    AuthBlocked,
    /// Not a failure: defer until `retry_after` / a budget change. `retryable` is false so retry layers do not hammer.
    BudgetDeferred,
    Storage,
    Cancelled,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("ingest {kind:?}: {message}")]
pub struct IngestError {
    pub retryable: bool,
    pub kind: IngestErrorKind,
    pub retry_after: Option<Duration>,
    /// Content-free.
    pub message: String,
}

impl IngestError {
    pub fn new(kind: IngestErrorKind, retryable: bool, message: impl Into<String>) -> Self {
        Self {
            retryable,
            kind,
            retry_after: None,
            message: message.into(),
        }
    }
    pub fn permanent(m: &str) -> Self {
        Self::new(IngestErrorKind::Permanent, false, m)
    }
    pub fn storage(_: sqlx::Error) -> Self {
        Self::new(IngestErrorKind::Storage, true, "index storage error")
    }
}

impl From<StageError> for IngestError {
    fn from(e: StageError) -> Self {
        match e {
            StageError::Cancelled => Self::new(IngestErrorKind::Cancelled, true, "cancelled"),
            StageError::Storage(m) => Self::new(IngestErrorKind::Storage, true, m),
            StageError::Source(m) => Self::new(IngestErrorKind::Transient, true, m),
            StageError::Unsupported(m) => Self::permanent(&m),
            StageError::Limit(n) => Self::permanent(&format!("limit exceeded: {n}")),
            StageError::SourceChanged => Self::new(
                IngestErrorKind::Transient,
                true,
                "source changed while parsing",
            ),
            StageError::SourceMissing => {
                Self::new(IngestErrorKind::Transient, true, "source missing")
            }
        }
    }
}

/// Test-only kill emulation (spec 19.4). One-shot: a flag that fires is cleared, so the re-run
/// converges. An injected stop returns `Err(Transient, "failpoint")` and leaves state as a kill would.
#[derive(Default)]
pub struct FailPoints {
    pub after_staging: AtomicBool,
    pub after_publish: AtomicBool,
    pub after_embed_commit: AtomicBool,
    /// Runs right after parsing finished, before the source is re-checked (e.g. to append to the file).
    pub after_parse: Mutex<Option<AfterParse>>,
}

impl FailPoints {
    fn fire(flag: &AtomicBool) -> Result<(), IngestError> {
        if flag.swap(false, Ordering::SeqCst) {
            Err(IngestError::new(
                IngestErrorKind::Transient,
                true,
                "failpoint",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Estimate {
    pub messages: usize,
    pub chunks: usize,
    pub tokens_total: usize,
    /// Tokens of distinct chunk inputs not yet embedded for the active profile.
    pub tokens_uncached: usize,
}

pub struct IngestEngine {
    index: Index,
    cfg: Arc<Config>,
    sources: Arc<SourceRegistry>,
    embeddings: Arc<EmbeddingService>,
    clock: SharedClock,
    tok: Arc<dyn TokenCounter>,
    pub fail: Arc<FailPoints>,
    /// Serializes `index_session` (the ingest permit is 1 anyway): a duplicate delivery runs after the
    /// first and observes its published revision instead of racing its staging rows.
    gate: tokio::sync::Mutex<()>,
}

impl IngestEngine {
    pub fn new(
        index: Index,
        cfg: Arc<Config>,
        sources: Arc<SourceRegistry>,
        embeddings: Arc<EmbeddingService>,
        clock: SharedClock,
    ) -> Self {
        Self::with_tokenizer(
            index,
            cfg,
            sources,
            embeddings,
            clock,
            crate::embeddings::tokenizer(),
        )
    }

    pub fn with_tokenizer(
        index: Index,
        cfg: Arc<Config>,
        sources: Arc<SourceRegistry>,
        embeddings: Arc<EmbeddingService>,
        clock: SharedClock,
        tok: Arc<dyn TokenCounter>,
    ) -> Self {
        Self {
            index,
            cfg,
            sources,
            embeddings,
            clock,
            tok,
            fail: Arc::new(FailPoints::default()),
            gate: tokio::sync::Mutex::new(()),
        }
    }

    pub fn embeddings(&self) -> &Arc<EmbeddingService> {
        &self.embeddings
    }

    fn chunk_cfg(&self) -> ChunkConfig {
        let c = &self.cfg.chunking;
        ChunkConfig {
            target_new_tokens: c.target_new_tokens,
            max_rendered_tokens: c.max_rendered_tokens,
            overlap_tokens: c.overlap_tokens,
            version: c.version,
        }
    }

    /// Adapter parser versions + extraction policy + chunker version/config + tokenizer identity (spec 9.4).
    pub fn pipeline_fingerprint(&self) -> String {
        let mut h = blake3::Hasher::new();
        h.update(b"spool-pipeline-v1");
        for k in [HarnessKind::Omp, HarnessKind::Codex, HarnessKind::Claude] {
            h.update(k.as_str().as_bytes());
            h.update(adapter_for(k).parser_version().as_bytes());
            h.update(&[0]);
        }
        h.update(&EXTRACTION_POLICY_VERSION.to_le_bytes());
        let c = self.chunk_cfg();
        for n in [
            c.target_new_tokens as u64,
            c.max_rendered_tokens as u64,
            c.overlap_tokens as u64,
            c.version as u64,
        ] {
            h.update(&n.to_le_bytes());
        }
        h.update(self.tok.identity().as_bytes());
        format!("pf1_{}", &crate::ids::hex(h.finalize().as_bytes())[..32])
    }

    async fn mark(
        &self,
        session_id: &str,
        state: &str,
        error: Option<String>,
        only_without_active: bool,
    ) -> Result<(), IngestError> {
        sqlx::query(
            "UPDATE sessions SET index_state=?, index_error=? WHERE id=? AND (?=0 OR active_snapshot_id IS NULL)",
        )
        .bind(state)
        .bind(error)
        .bind(session_id)
        .bind(only_without_active as i64)
        .execute(&self.index.writer)
        .await
        .map_err(IngestError::storage)?;
        Ok(())
    }

    async fn note_error(&self, session_id: &str, error: String) -> Result<(), IngestError> {
        sqlx::query("UPDATE sessions SET index_error=? WHERE id=?")
            .bind(error)
            .bind(session_id)
            .execute(&self.index.writer)
            .await
            .map_err(IngestError::storage)?;
        Ok(())
    }

    pub async fn index_session(
        &self,
        req: IndexRequest,
        cancel: Arc<AtomicBool>,
    ) -> Result<IndexOutcome, IngestError> {
        let _serial = self.gate.lock().await;
        let sid = req.session_id.as_str();
        // 1. Policy and catalog (the request never supplies a path).
        let row = load_session(&self.index, sid)
            .await
            .map_err(IngestError::storage)?
            .ok_or_else(|| IngestError::permanent("unknown session"))?;
        if row.excluded || row.tombstoned {
            sqlx::query(
                "DELETE FROM snapshots WHERE session_id=? AND kind='indexed' AND state='staging'",
            )
            .bind(sid)
            .execute(&self.index.writer)
            .await
            .map_err(IngestError::storage)?;
            return Ok(IndexOutcome::Excluded);
        }
        if !self.sources.is_enabled(&row.source_id) {
            return Ok(IndexOutcome::Deferred(DeferReason::SourceDisabled));
        }
        if cancel.load(Ordering::Relaxed) {
            return Err(StageError::Cancelled.into());
        }
        // Staging left behind by a killed run is invisible; discard it.
        sqlx::query(
            "DELETE FROM snapshots WHERE session_id=? AND kind='indexed' AND state='staging'",
        )
        .bind(sid)
        .execute(&self.index.writer)
        .await
        .map_err(IngestError::storage)?;

        // 2. Compare against the current source observation.
        let opened = match open_recorded(
            &self.index,
            &self.sources,
            &self.clock,
            sid,
            &row.source_id,
            row.path.as_deref(),
        )
        .await
        {
            Ok(o) => o,
            Err(StageError::SourceMissing) => return Ok(IndexOutcome::SourceMissing),
            Err(e) => return Err(e.into()),
        };
        let cur4 = revision4(&opened.info);
        drop(opened);
        let fp = self.pipeline_fingerprint();
        let memo_tail = format!("|{fp}|{cur4}");
        if let Some(err) = row
            .index_error
            .as_deref()
            .and_then(|e| e.strip_suffix(memo_tail.as_str()))
        {
            if err == "unsupported" {
                return Ok(IndexOutcome::Unsupported);
            }
            match err {
                "limit_exceeded:max_record_bytes" => {
                    return Ok(IndexOutcome::Deferred(DeferReason::LimitExceeded(
                        "max_record_bytes",
                    )));
                }
                "limit_exceeded:max_source_file_bytes" => {
                    return Ok(IndexOutcome::Deferred(DeferReason::LimitExceeded(
                        "max_source_file_bytes",
                    )));
                }
                _ => {}
            }
        }
        if let Some(active) = &row.active_snapshot_id {
            let snap = sqlx::query("SELECT source_revision, fingerprint FROM snapshots WHERE id=?")
                .bind(active)
                .fetch_optional(&self.index.writer)
                .await
                .map_err(IngestError::storage)?;
            if let Some(s) = snap {
                let rev: String = s.get("source_revision");
                let covered = rev
                    .strip_prefix(cur4.as_str())
                    .is_some_and(|t| t.starts_with(':'))
                    && s.get::<String, _>("fingerprint") == fp;
                if covered {
                    // Duplicate delivery / lost ack / retry after an embedding failure.
                    if row.index_state != "indexed" || row.index_error.is_some() {
                        // Published content is current; only the catalog bookkeeping (e.g. 'pending') is stale.
                        self.mark(sid, "indexed", None, false).await?;
                    }
                    let stats = self.embed(sid, &cancel).await?;
                    FailPoints::fire(&self.fail.after_embed_commit)?;
                    return Ok(
                        if stats.missing_before == 0
                            || stats.skipped
                            || (stats.embedded == 0 && stats.missing_after == 0)
                        {
                            IndexOutcome::NoOp
                        } else if stats.missing_after == 0 {
                            IndexOutcome::Embedded {
                                embedded: stats.embedded,
                            }
                        } else {
                            IndexOutcome::EmbeddingsPending {
                                missing: stats.missing_after,
                                follow_up_needed: false,
                            }
                        },
                    );
                }
            }
        }

        // 3. Stage a snapshot of the observed prefix.
        let hook = self.fail.after_parse.lock().clone();
        let staged = match stage_with_hook(
            &self.index,
            &self.sources,
            &self.cfg,
            sid,
            SnapshotKind::Indexed,
            cancel.clone(),
            &self.clock,
            &fp,
            hook,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => return self.stage_failed(e, sid, &fp, &cur4).await,
        };
        let Staged {
            snapshot_id,
            generation,
            graph,
            follow_up,
            ..
        } = staged;
        FailPoints::fire(&self.fail.after_staging)?;

        // 4. Chunk plan, then one guarded publication.
        let plan = match self
            .plan_for_snapshot(&snapshot_id, graph, cancel.clone())
            .await
        {
            Ok(p) => p,
            Err(e) => {
                let _ = stage::delete_snapshot(&self.index, &snapshot_id).await;
                return Err(e);
            }
        };
        if cancel.load(Ordering::Relaxed) {
            let _ = stage::delete_snapshot(&self.index, &snapshot_id).await;
            return Err(StageError::Cancelled.into());
        }
        let outcome = publish(
            &self.index,
            &self.clock,
            sid,
            &snapshot_id,
            generation,
            &fp,
            &plan,
        )
        .await?;
        let new_generation = match outcome {
            PublishOutcome::Superseded => return Ok(IndexOutcome::Superseded),
            PublishOutcome::Excluded => return Ok(IndexOutcome::Excluded),
            PublishOutcome::Published { generation, .. } => generation,
        };
        FailPoints::fire(&self.fail.after_publish)?;

        // 5. Vectors for whatever the cache lacks. Text is already searchable.
        let stats = self.embed(sid, &cancel).await?;
        FailPoints::fire(&self.fail.after_embed_commit)?;
        let _ = &req.observed_source_revision;
        Ok(IndexOutcome::Published {
            snapshot_id,
            generation: new_generation,
            chunks: plan.len(),
            reused_chunks: plan.len().saturating_sub(stats.missing_before),
            embeddings_pending: stats.missing_after,
            follow_up_needed: follow_up,
        })
    }

    async fn embed(&self, sid: &str, cancel: &AtomicBool) -> Result<EmbedStats, IngestError> {
        embed_missing(
            &self.index,
            &self.embeddings,
            &self.clock,
            self.cfg.embedding.enabled,
            sid,
            cancel,
        )
        .await
    }

    async fn stage_failed(
        &self,
        e: StageError,
        sid: &str,
        fp: &str,
        cur4: &str,
    ) -> Result<IndexOutcome, IngestError> {
        match e {
            StageError::Unsupported(_) => {
                self.mark(
                    sid,
                    "unsupported",
                    Some(format!("unsupported|{fp}|{cur4}")),
                    false,
                )
                .await?;
                Ok(IndexOutcome::Unsupported)
            }
            StageError::Limit(name) => {
                self.mark(
                    sid,
                    "failed",
                    Some(format!("limit_exceeded:{name}|{fp}|{cur4}")),
                    false,
                )
                .await?;
                Ok(IndexOutcome::Deferred(DeferReason::LimitExceeded(name)))
            }
            StageError::SourceChanged => {
                self.mark(sid, "deferred", Some("source_changed".into()), true)
                    .await?;
                self.note_error(sid, "source_changed".into()).await?;
                Ok(IndexOutcome::Deferred(DeferReason::SourceChanged))
            }
            StageError::SourceMissing => Ok(IndexOutcome::SourceMissing),
            other => Err(other.into()),
        }
    }

    /// Chunk planning on a blocking thread behind the ingest CPU permit.
    async fn plan_for_snapshot(
        &self,
        snapshot_id: &str,
        graph: crate::graph::SessionGraph,
        cancel: Arc<AtomicBool>,
    ) -> Result<Vec<ChunkPlan>, IngestError> {
        let rows = sqlx::query(
            "SELECT m.record_key AS k, m.role AS role, m.ts AS ts, b.idx AS idx, b.origin AS origin, b.text AS text \
             FROM messages m JOIN blocks b ON b.snapshot_id = m.snapshot_id AND b.record_key = m.record_key \
             WHERE m.snapshot_id = ? ORDER BY m.seq, b.idx",
        )
        .bind(snapshot_id)
        .fetch_all(&self.index.writer)
        .await
        .map_err(IngestError::storage)?;
        let mut msgs: Vec<ChunkMessage> = Vec::new();
        for r in rows {
            if cancel.load(Ordering::Relaxed) {
                return Err(StageError::Cancelled.into());
            }
            let key: String = r.get("k");
            if msgs.last().map(|m| m.key != key).unwrap_or(true) {
                msgs.push(ChunkMessage {
                    key: key.clone(),
                    role: if r.get::<String, _>("role") == "user" {
                        Role::User
                    } else {
                        Role::Assistant
                    },
                    timestamp: r.get("ts"),
                    blocks: Vec::new(),
                });
            }
            msgs.last_mut().expect("pushed").blocks.push(TextBlock {
                index: r.get::<i64, _>("idx") as u32,
                origin: if r.get::<String, _>("origin") == "authored" {
                    BlockOrigin::Authored
                } else {
                    BlockOrigin::Unknown
                },
                text: r.get("text"),
            });
        }
        self.plan_blocking(graph, msgs, cancel).await
    }

    async fn plan_blocking(
        &self,
        graph: crate::graph::SessionGraph,
        msgs: Vec<ChunkMessage>,
        cancel: Arc<AtomicBool>,
    ) -> Result<Vec<ChunkPlan>, IngestError> {
        let permit = CPU
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IngestError::from(StageError::Cancelled))?;
        if cancel.load(Ordering::Relaxed) {
            return Err(StageError::Cancelled.into());
        }
        let (cfg, tok) = (self.chunk_cfg(), self.tok.clone());
        tokio::task::spawn_blocking(move || {
            let _p = permit;
            plan_chunks(&graph, &msgs, &cfg, &*tok, &cancel).map_err(|error| match error {
                super::chunk::ChunkError::Cancelled => IngestError::from(StageError::Cancelled),
                super::chunk::ChunkError::TokenLimit => IngestError::permanent(
                    "source fragment exceeds the configured rendered token limit",
                ),
            })
        })
        .await
        .map_err(|_| IngestError::new(IngestErrorKind::Transient, true, "chunk task failed"))?
    }

    /// Local conversational token estimate for `--dry-run`: parses in memory, no database writes
    /// beyond the `source_files` observation, no provider call.
    pub async fn estimate(&self, session_id: &str) -> Result<Estimate, IngestError> {
        let row = load_session(&self.index, session_id)
            .await
            .map_err(IngestError::storage)?
            .ok_or_else(|| IngestError::permanent("unknown session"))?;
        let harness = self
            .sources
            .kind(&row.source_id)
            .ok_or_else(|| IngestError::permanent("source is not configured"))?;
        let opened = open_recorded(
            &self.index,
            &self.sources,
            &self.clock,
            session_id,
            &row.source_id,
            row.path.as_deref(),
        )
        .await?;
        let opts = ParseOptions {
            max_record_bytes: self.cfg.ingestion.max_record_bytes,
            max_source_bytes: self.cfg.ingestion.max_source_file_bytes,
            observe_len: opened.info.len,
            include_subagents: self.sources.include_subagents(&row.source_id),
            blob_roots: self.sources.blob_roots(&row.source_id),
        };
        let permit = CPU
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IngestError::from(StageError::Cancelled))?;
        let (file, len) = (opened.file, opened.info.len);
        let collected = tokio::task::spawn_blocking(move || {
            let _p = permit;
            let mut sink = Collect {
                cancel: AtomicBool::new(false),
                ..Default::default()
            };
            let mut r = BufReader::new(file.take(len));
            let m = adapter_for(harness).parse_snapshot(&mut r, &opts, &mut sink)?;
            Ok::<_, crate::adapters::AdapterError>((m, sink))
        })
        .await
        .map_err(|_| IngestError::new(IngestErrorKind::Transient, true, "estimate task failed"))?;
        let (manifest, sink) = collected.map_err(|e| match e {
            crate::adapters::AdapterError::Unsupported(m) => IngestError::permanent(&m),
            crate::adapters::AdapterError::LimitExceeded => {
                IngestError::permanent("limit exceeded: max_record_bytes")
            }
            _ => IngestError::new(IngestErrorKind::Transient, true, "source read failed"),
        })?;
        if manifest.coverage == Coverage::Unsupported {
            return Err(IngestError::permanent("unsupported source format"));
        }
        let live: Vec<&MessageEntry> = sink
            .messages
            .iter()
            .filter(|m| !sink.superseded.contains(&m.record_key))
            .collect();
        let pairs: Vec<(String, Role)> = live
            .iter()
            .map(|m| (m.record_key.clone(), m.role))
            .collect();
        let (graph, _) = build_graph(&sink.records, &pairs, &sink.alias);
        let ts: HashMap<&str, Option<TimestampMs>> = sink
            .records
            .iter()
            .map(|r| (r.key.as_str(), r.timestamp))
            .collect();
        let msgs: Vec<ChunkMessage> = live
            .iter()
            .map(|m| ChunkMessage {
                key: m.record_key.clone(),
                role: m.role,
                timestamp: ts.get(m.record_key.as_str()).copied().flatten(),
                blocks: m.blocks.clone(),
            })
            .collect();
        let n_msgs = msgs.len();
        let plan = self
            .plan_blocking(graph, msgs, Arc::new(AtomicBool::new(false)))
            .await?;

        let profile = self.embeddings.profile().id();
        let mut cached: HashSet<Digest> = HashSet::new();
        for part in plan.chunks(400) {
            let ph = vec!["?"; part.len()].join(",");
            let sql = format!(
                "SELECT input_hash FROM embeddings WHERE profile_id = ? AND input_hash IN ({ph})"
            );
            let mut q = sqlx::query_scalar::<_, Vec<u8>>(sqlx::AssertSqlSafe(sql)).bind(&profile);
            for c in part {
                q = q.bind(c.input_hash.to_vec());
            }
            for h in q
                .fetch_all(&self.index.reader)
                .await
                .map_err(IngestError::storage)?
            {
                if let Ok(d) = Digest::try_from(h) {
                    cached.insert(d);
                }
            }
        }
        let mut seen = HashSet::new();
        let tokens_uncached = plan
            .iter()
            .filter(|c| !cached.contains(&c.input_hash) && seen.insert(c.input_hash))
            .map(|c| c.rendered_tokens)
            .sum();
        Ok(Estimate {
            messages: n_msgs,
            chunks: plan.len(),
            tokens_total: plan.iter().map(|c| c.rendered_tokens).sum(),
            tokens_uncached,
        })
    }

    /// Adapter for the reader (`read_session` on a session without an indexed snapshot): stages a
    /// `read_only` snapshot, never chunks, never publishes.
    pub fn source_read_stager(self: &Arc<Self>) -> Arc<dyn SourceReadStager> {
        Arc::new(Stager(self.clone()))
    }
}

struct Stager(Arc<IngestEngine>);

impl SourceReadStager for Stager {
    fn stage_source_read<'a>(
        &'a self,
        session_id: &'a str,
    ) -> BoxFuture<'a, Result<StagedRead, AppError>> {
        Box::pin(async move {
            let e = &self.0;
            let fp = e.pipeline_fingerprint();
            match stage::stage_snapshot(
                &e.index,
                &e.sources,
                &e.cfg,
                session_id,
                SnapshotKind::SourceRead,
                Arc::new(AtomicBool::new(false)),
                &e.clock,
                &fp,
            )
            .await
            {
                Ok(s) => Ok(StagedRead {
                    snapshot_id: s.snapshot_id,
                }),
                Err(StageError::SourceMissing) => Err(AppError::new(
                    "SOURCE_UNAVAILABLE",
                    "the original transcript is not available",
                )),
                Err(StageError::SourceChanged) => Err({
                    let mut a = AppError::new(
                        "SOURCE_CHANGED",
                        "the transcript changed while it was being read",
                    );
                    a.retryable = true;
                    a
                }),
                Err(StageError::Unsupported(_)) => Err(AppError::new(
                    "VIEW_NOT_SUPPORTED",
                    "this transcript format is not supported",
                )),
                Err(StageError::Limit(_)) => Err(AppError::new(
                    "RESOURCE_LIMIT",
                    "the transcript exceeds a configured size limit",
                )),
                Err(StageError::Cancelled) => Err(AppError::new("CANCELLED", "cancelled")),
                Err(StageError::Source(_)) => Err(AppError::new(
                    "SOURCE_UNAVAILABLE",
                    "the original transcript is not available",
                )),
                Err(StageError::Storage(_)) => Err({
                    let mut a = AppError::new(
                        "STORAGE_ERROR",
                        "required local state cannot be accessed safely",
                    );
                    a.retryable = true;
                    a
                }),
            }
        })
    }
}

#[derive(Default)]
struct Collect {
    records: Vec<RecordMeta>,
    messages: Vec<MessageEntry>,
    superseded: HashSet<String>,
    alias: HashMap<String, String>,
    cancel: AtomicBool,
}

impl RecordSink for Collect {
    fn record(&mut self, rec: RecordMeta) -> Result<(), SinkError> {
        self.records.push(rec);
        Ok(())
    }
    fn message(&mut self, msg: MessageEntry) -> Result<(), SinkError> {
        for s in &msg.supersedes {
            self.superseded.insert(s.clone());
            self.alias.insert(s.clone(), msg.record_key.clone());
        }
        self.messages.push(msg);
        Ok(())
    }
    fn tool_call(&mut self, _: ToolCallMeta) -> Result<(), SinkError> {
        Ok(())
    }
    fn tool_result(&mut self, _: ToolResultMeta) -> Result<(), SinkError> {
        Ok(())
    }
    fn warning(&mut self, _: ParseWarning) -> Result<(), SinkError> {
        Ok(())
    }
    fn cancelled(&self) -> &AtomicBool {
        &self.cancel
    }
}

#[allow(dead_code)]
fn _path_type_check(_: &Path) {}
