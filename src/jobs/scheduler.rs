//! Durable submission, queue accounting, retry policy and reconciliation (spec 7.2, 11.3, 11.4,
//! 11.7, 11.8). This is the only place that pushes Apalis tasks.
//!
//! Reconciliation compares *desired* work (catalog: pending/deferred/stale/under-covered sessions)
//! with *published* state and with the live tasks in `jobs.sqlite`. It is the safety net for every
//! cross-database window: catalog updated but no task, task stored but catalog incomplete, index
//! committed but acknowledgement lost. Coalescing through idempotency keys is only an optimisation.

use super::backend::{self, Liveness};
use super::payload::*;
use crate::clock::SharedClock;
use crate::config::Config;
use crate::embeddings::{EmbeddingService, EmbeddingState, EmbeddingStatus};
use crate::ids;
use crate::ingest::engine::IngestEngine;
use crate::ipc::AppError;
use crate::storage::catalog;
use crate::storage::index::Index;
use apalis::prelude::{TaskBuilder, TaskSink};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Unstarted index tasks kept in the queue at once. Start order is FIFO, so keeping the backlog
/// short is what makes the 4:1 interleave (`interleave`) effective for newly active sessions.
pub const SUBMIT_WINDOW: i64 = 64;
/// Slow scans (stale revision, vector gaps) run at most this often.
pub const SLOW_SCAN_EVERY_MS: i64 = 15 * 60 * 1000;
/// An operation that never reached its Apalis task is failed after this long.
pub const UNACCEPTED_OP_MS: i64 = 5 * 60 * 1000;
const REPAIR_PAGE: i64 = 200;

pub fn backend_err(_: impl std::fmt::Display) -> AppError {
    AppError {
        retryable: true,
        ..AppError::new("STORAGE_ERROR", "the job queue cannot be accessed safely")
    }
}

// ---------------------------------------------------------------------------------------------
// retry policy (spec 11.7): 5 transient attempts, 5 s base, 15 min cap, jitter, honour Retry-After
// ---------------------------------------------------------------------------------------------

pub const RETRY_BASE: Duration = Duration::from_secs(5);
pub const RETRY_CAP: Duration = Duration::from_secs(15 * 60);

/// Delay before transient attempt number `attempt` (1-based) is retried. `jitter` in `[0,1)` scales
/// the delay by ±20%; the provider's `Retry-After` is a floor and never exceeded by jitter below it.
pub fn retry_delay(attempt: u32, retry_after: Option<Duration>, jitter: f64) -> Duration {
    backoff_delay(RETRY_BASE, RETRY_CAP, attempt, retry_after, jitter)
}

pub fn backoff_delay(
    base: Duration,
    cap: Duration,
    attempt: u32,
    retry_after: Option<Duration>,
    jitter: f64,
) -> Duration {
    let exp = base.saturating_mul(1u32 << attempt.saturating_sub(1).min(16));
    let capped = exp.min(cap);
    let jittered = capped.mul_f64(0.8 + 0.4 * jitter.clamp(0.0, 1.0)).min(cap);
    retry_after.map_or(jittered, |r| jittered.max(r))
}

/// Handler error meaning "transient, retry with backoff". Carries the provider's `Retry-After`.
#[derive(Debug)]
pub struct TransientIngest {
    pub retry_after: Option<Duration>,
}

impl std::fmt::Display for TransientIngest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("transient ingestion failure")
    }
}

impl std::error::Error for TransientIngest {}

/// Retry policy plugged into Apalis' retry layer (`WorkerBuilderExt::retry`): only
/// `TransientIngest` errors are retried, with exponential backoff + jitter, honouring `Retry-After`,
/// never while the worker shuts down. Permanent/auth/budget outcomes are handled (Ok) by the handler
/// and never reach this policy.
#[derive(Debug, Clone)]
pub struct IngestRetry {
    pub max_attempts: usize,
    pub base: Duration,
    pub cap: Duration,
}

impl IngestRetry {
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts,
            base: RETRY_BASE,
            cap: RETRY_CAP,
        }
    }
}

impl<T: Clone, Res>
    apalis::layers::retry::Policy<apalis::prelude::Task<T>, Res, apalis::prelude::BoxDynError>
    for IngestRetry
{
    type Future = futures::future::BoxFuture<'static, ()>;

    fn retry(
        &mut self,
        req: &mut apalis::prelude::Task<T>,
        result: &mut Result<Res, apalis::prelude::BoxDynError>,
    ) -> Option<Self::Future> {
        let worker = req
            .data()
            .get::<apalis_core::worker::context::WorkerContext>()?;
        if worker.is_shutting_down() {
            return None;
        }
        let t = result.as_ref().err()?.downcast_ref::<TransientIngest>()?;
        let attempt = req.attempt();
        if attempt >= self.max_attempts {
            return None;
        }
        let jitter = jitter_from(&format!("{:?}", req.task_id()), attempt as u64);
        let d = backoff_delay(self.base, self.cap, attempt as u32, t.retry_after, jitter);
        Some(Box::pin(tokio::time::sleep(d)))
    }

    fn clone_request(
        &mut self,
        req: &apalis::prelude::Task<T>,
    ) -> Option<apalis::prelude::Task<T>> {
        Some(req.clone())
    }
}

/// Cheap non-cryptographic jitter in `[0,1)` from a seed (no RNG dependency).
pub fn jitter_from(seed: &str, salt: u64) -> f64 {
    let h = blake3::hash(format!("{seed}:{salt}").as_bytes());
    let b = h.as_bytes();
    u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64 / (u32::MAX as f64 + 1.0)
}

// ---------------------------------------------------------------------------------------------
// start-order guard
// ---------------------------------------------------------------------------------------------

/// Order work so that one of every five starts goes to waiting backfill (`manual`) work when any
/// is waiting, the rest to newly active work. Falls back to whichever list is non-empty.
pub fn interleave<T>(active: Vec<T>, backfill: Vec<T>, counter: &mut u64) -> Vec<T> {
    let mut a = active.into_iter();
    let mut b = backfill.into_iter();
    let mut out = Vec::new();
    loop {
        *counter += 1;
        let next = if (*counter).is_multiple_of(5) {
            b.next().or_else(|| a.next())
        } else {
            a.next().or_else(|| b.next())
        };
        match next {
            Some(x) => out.push(x),
            None => break,
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// durable submission
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct QueueCounts {
    pub pending: i64,
    pub running: i64,
    pub retryable_failed: i64,
    pub done: i64,
    pub killed: i64,
}

impl QueueCounts {
    pub fn live(&self) -> i64 {
        self.pending + self.running + self.retryable_failed
    }
}

#[derive(Clone)]
pub struct Submitter {
    pub index: Index,
    pub pool: SqlitePool,
    pub cfg: Arc<Config>,
    pub liveness: Liveness,
    pub clock: SharedClock,
}

impl Submitter {
    /// Push an `IndexSession` task. Returns the Apalis task id only after the row is in `jobs.sqlite`.
    pub async fn push_index(
        &self,
        payload: IndexSession,
        key: &str,
        delay: Option<Duration>,
    ) -> Result<String, AppError> {
        let mut b = TaskBuilder::new(payload)
            .idempotency_key(key)
            .max_attempts(self.cfg.ingestion.max_transient_attempts as usize + 5);
        if let Some(d) = delay {
            b = b.run_after(d);
        }
        let mut st = backend::storage::<IndexSession>(&self.pool, INGEST_QUEUE, self.liveness);
        st.push_task(b.build()).await.map_err(backend_err)?;
        self.stored_id(INGEST_QUEUE, key).await
    }

    pub async fn push_control(
        &self,
        job: ControlJob,
        key: &str,
        delay: Option<Duration>,
    ) -> Result<String, AppError> {
        let mut b = TaskBuilder::new(job).idempotency_key(key).max_attempts(5);
        if let Some(d) = delay {
            b = b.run_after(d);
        }
        let mut st = backend::storage::<ControlJob>(&self.pool, CONTROL_QUEUE, self.liveness);
        st.push_task(b.build()).await.map_err(backend_err)?;
        self.stored_id(CONTROL_QUEUE, key).await
    }

    /// Confirm the row exists (a duplicate key resolves to the already stored task).
    async fn stored_id(&self, queue: &str, key: &str) -> Result<String, AppError> {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM Jobs WHERE job_type = ?1 AND idempotency_key = ?2",
        )
        .bind(queue)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend_err)?
        .ok_or_else(|| backend_err("task row not stored"))
    }

    pub async fn counts(&self, queue: &str) -> Result<QueueCounts, AppError> {
        let rows = sqlx::query("SELECT status, attempts < max_attempts AS retryable, count(*) AS n FROM Jobs WHERE job_type = ? GROUP BY 1, 2").bind(queue).fetch_all(&self.pool).await.map_err(backend_err)?;
        let mut c = QueueCounts::default();
        for r in rows {
            let n: i64 = r.get("n");
            let retryable: i64 = r.get("retryable");
            match r.get::<String, _>("status").as_str() {
                "Pending" | "Queued" => c.pending += n,
                "Running" => c.running += n,
                "Failed" if retryable != 0 => c.retryable_failed += n,
                "Done" => c.done += n,
                _ => c.killed += n,
            }
        }
        Ok(c)
    }

    pub async fn live_count(&self, queue: &str) -> Result<i64, AppError> {
        Ok(self.counts(queue).await?.live())
    }

    /// Which of `ids` are still going to run (or are running).
    pub async fn live_ids(&self, ids: &[String]) -> Result<HashSet<String>, AppError> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let v: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM Jobs WHERE id IN (SELECT value FROM json_each(?1)) AND (status IN ('Pending','Queued','Running') OR (status = 'Failed' AND attempts < max_attempts))",
        )
        .bind(serde_json::to_string(ids).unwrap_or_default())
        .fetch_all(&self.pool)
        .await
        .map_err(backend_err)?;
        Ok(v.into_iter().collect())
    }

    /// (status, attempts, max_attempts) of one task.
    pub async fn task_state(&self, id: &str) -> Result<Option<(String, i64, i64)>, AppError> {
        sqlx::query_as("SELECT status, attempts, max_attempts FROM Jobs WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend_err)
    }

    /// Queue-length gate for submissions that must respect `max_queued_sessions`.
    pub async fn has_room(&self) -> Result<bool, AppError> {
        Ok(self.live_count(INGEST_QUEUE).await? < self.cfg.ingestion.max_queued_sessions as i64)
    }
}

// ---------------------------------------------------------------------------------------------
// reconciliation
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PassReport {
    pub live: i64,
    pub room: i64,
    pub submitted: usize,
    pub repaired_tasks: usize,
    pub finished_operations: u64,
    pub provider_usable: bool,
    pub error: Option<String>,
}

pub struct Reconciler {
    index: Index,
    sub: Submitter,
    engine: Arc<IngestEngine>,
    embeddings: Arc<EmbeddingService>,
    cfg: Arc<Config>,
    clock: SharedClock,
    counter: Mutex<u64>,
    last_slow: AtomicU64,
    last_usable: std::sync::atomic::AtomicBool,
    repair_cursor: Mutex<String>,
    run: tokio::sync::Mutex<()>,
    pub notify: tokio::sync::Notify,
}

pub fn provider_usable(s: &EmbeddingStatus) -> bool {
    match &s.state {
        EmbeddingState::Ready => true,
        EmbeddingState::Degraded(c) => !c.blocked,
        _ => false,
    }
}

impl Reconciler {
    pub fn new(
        index: Index,
        sub: Submitter,
        engine: Arc<IngestEngine>,
        embeddings: Arc<EmbeddingService>,
        cfg: Arc<Config>,
        clock: SharedClock,
    ) -> Self {
        Self {
            index,
            sub,
            engine,
            embeddings,
            cfg,
            clock,
            counter: Mutex::new(0),
            last_slow: AtomicU64::new(0),
            last_usable: std::sync::atomic::AtomicBool::new(false),
            repair_cursor: Mutex::new(String::new()),
            run: tokio::sync::Mutex::new(()),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Ask for a pass soon (coalesced).
    pub fn kick(&self) {
        self.notify.notify_one();
    }

    /// Make the next pass run the slow scans (stale revision, vector gaps) regardless of their cadence.
    pub fn force_slow_scan(&self) {
        self.last_slow.store(0, Ordering::Relaxed);
    }

    pub async fn pass(&self) -> Result<PassReport, AppError> {
        let _g = self.run.lock().await;
        let now = self.clock.now_ms();
        let mut rep = PassReport {
            finished_operations: self.settle_operations(now).await?,
            repaired_tasks: self.repair_dead_tasks().await?,
            ..Default::default()
        };
        // A migration to a new embedding profile switches the moment the published corpus is covered.
        let _ = self.embeddings.activate_if_covered().await;
        let status = self.embeddings.status();
        let usable = provider_usable(&status);
        rep.provider_usable = usable;
        rep.live = self.sub.live_count(INGEST_QUEUE).await?;
        let cap = self.cfg.ingestion.max_queued_sessions as i64;
        rep.room = (SUBMIT_WINDOW.min(cap) - rep.live).max(0);
        if rep.room == 0 || storage_paused(&self.index, &self.cfg).await {
            return Ok(rep);
        }
        let slow_due = now - self.last_slow.load(Ordering::Relaxed) as i64 >= SLOW_SCAN_EVERY_MS
            || (usable && !self.last_usable.load(Ordering::Relaxed));
        if slow_due {
            self.last_slow.store(now as u64, Ordering::Relaxed);
        }
        self.last_usable.store(usable, Ordering::Relaxed);
        let (active, backfill) = (
            self.candidates(now, usable, slow_due, &status.profile_id, true, rep.room)
                .await?,
            self.candidates(now, usable, slow_due, &status.profile_id, false, rep.room)
                .await?,
        );
        let order = {
            let mut c = self.counter.lock().unwrap_or_else(|e| e.into_inner());
            interleave(active, backfill, &mut c)
        };
        let fingerprint = self.engine.pipeline_fingerprint();
        for cand in order.into_iter().take(rep.room as usize) {
            match self
                .submit_candidate(&cand, &fingerprint, &status.profile_id)
                .await
            {
                Ok(()) => rep.submitted += 1,
                Err(e) => {
                    rep.error = Some(e.code);
                    break;
                }
            }
        }
        Ok(rep)
    }

    async fn submit_candidate(
        &self,
        c: &Candidate,
        fingerprint: &str,
        profile: &str,
    ) -> Result<(), AppError> {
        let seq = catalog::next_submit_seq(&self.index, &c.id)
            .await
            .map_err(backend_err)?;
        let payload = IndexSession {
            payload_version: PAYLOAD_VERSION,
            session_id: c.id.clone(),
            observed_source_revision: c.revision.clone(),
            pipeline_fingerprint: fingerprint.to_string(),
            embedding_profile_id: profile.to_string(),
            operation_id: c.operation_id.clone(),
        };
        let task_id = self
            .sub
            .push_index(payload, &format!("idx:{}:{seq}", c.id), None)
            .await?;
        catalog::set_task(&self.index, &c.id, Some(&task_id))
            .await
            .map_err(backend_err)?;
        Ok(())
    }

    /// Candidate sessions of one start class, newest activity first.
    async fn candidates(
        &self,
        now: i64,
        usable: bool,
        slow: bool,
        profile: &str,
        priority: bool,
        limit: i64,
    ) -> Result<Vec<Candidate>, AppError> {
        let class = if priority {
            "s.eligibility IN ('bootstrap','active')"
        } else {
            "s.eligibility = 'manual'"
        };
        let slow_clause = if slow {
            " OR (?3 = 1 AND s.index_state = 'indexed' AND s.active_snapshot_id IS NOT NULL AND EXISTS (SELECT 1 FROM chunks c WHERE c.snapshot_id = s.active_snapshot_id \
              AND NOT EXISTS (SELECT 1 FROM embeddings e WHERE e.profile_id = ?4 AND e.input_hash = c.input_hash))) \
              OR (s.index_state = 'indexed' AND NOT EXISTS (SELECT 1 FROM snapshots sn WHERE sn.id = s.active_snapshot_id \
              AND sn.source_revision LIKE (f.size || ':' || f.mtime_ms || ':' || f.dev || ':' || f.ino || ':%')))"
        } else {
            ""
        };
        let sql = format!(
            "SELECT s.id, s.operation_id, f.size, f.mtime_ms, f.dev, f.ino FROM sessions s JOIN source_files f ON f.session_id = s.id JOIN sources src ON src.id = s.source_id \
             WHERE s.excluded = 0 AND s.source_status = 'present' AND f.exists_status = 'present' AND src.enabled = 1 \
             AND {indexable_kind} AND s.task_id IS NULL AND {class} AND ?4 = ?4 \
             AND ((s.index_state = 'pending' AND (s.defer_until IS NULL OR s.defer_until <= ?1)) \
                  OR (s.index_state = 'deferred' AND s.defer_until <= ?1 AND (s.active_snapshot_id IS NULL OR ?3 = 1)){slow_clause}) \
             ORDER BY s.last_activity_at DESC, s.id LIMIT ?2",
            indexable_kind = crate::storage::catalog::INDEXABLE_KIND
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(now)
            .bind(limit)
            .bind(usable as i64)
            .bind(profile)
            .fetch_all(&self.index.reader)
            .await
            .map_err(backend_err)?;
        Ok(rows
            .iter()
            .map(|r| Candidate {
                id: r.get("id"),
                operation_id: r.get("operation_id"),
                revision: catalog::revision(
                    r.get::<i64, _>("size") as u64,
                    r.get("mtime_ms"),
                    r.get::<i64, _>("dev") as u64,
                    r.get::<i64, _>("ino") as u64,
                ),
            })
            .collect())
    }

    /// Sessions whose recorded task no longer exists or finished without settling the session
    /// (lost enqueue, crash between task store and catalog update, pruned task): drop the stale
    /// reference so the next candidate query resubmits. Rotating bounded window.
    async fn repair_dead_tasks(&self) -> Result<usize, AppError> {
        let after = self
            .repair_cursor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT id, task_id FROM sessions WHERE task_id IS NOT NULL AND id > ?1 ORDER BY id LIMIT ?2")
            .bind(&after)
            .bind(REPAIR_PAGE)
            .fetch_all(&self.index.reader)
            .await
            .map_err(backend_err)?;
        *self.repair_cursor.lock().unwrap_or_else(|e| e.into_inner()) =
            if (rows.len() as i64) < REPAIR_PAGE {
                String::new()
            } else {
                rows.last().map(|r| r.0.clone()).unwrap_or_default()
            };
        let ids: Vec<String> = rows.iter().map(|r| r.1.clone()).collect();
        let live = self.sub.live_ids(&ids).await?;
        let mut n = 0;
        for (sid, tid) in rows {
            if !live.contains(&tid) {
                sqlx::query("UPDATE sessions SET task_id = NULL WHERE id = ?1 AND task_id = ?2")
                    .bind(&sid)
                    .bind(&tid)
                    .execute(&self.index.writer)
                    .await
                    .map_err(backend_err)?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Operation bookkeeping: fail operations that never reached a task, finish operations whose
    /// enumeration ended and whose sessions all settled.
    async fn settle_operations(&self, now: i64) -> Result<u64, AppError> {
        sqlx::query("UPDATE operations SET state = 'failed', error = 'not_accepted', updated_at = ?1 WHERE state = 'accepted' AND task_id IS NULL AND updated_at < ?2")
            .bind(now)
            .bind(now - UNACCEPTED_OP_MS)
            .execute(&self.index.writer)
            .await
            .map_err(backend_err)?;
        let done = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE operations SET state = 'done', updated_at = ?1 WHERE state IN ('accepted','running') AND cancel_requested = 0 \
             AND json_extract(progress_json, '$.enumeration_done') = 1 \
             AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.operation_id = operations.id AND s.excluded = 0 AND s.source_status = 'present' AND s.index_state IN ('pending','deferred') AND {})",
            crate::storage::catalog::INDEXABLE_KIND
        )))
        .bind(now)
        .execute(&self.index.writer)
        .await
        .map_err(backend_err)?
        .rows_affected();
        Ok(done)
    }

    /// Reconciler loop: a pass on every kick and at least every `tick`, until `stop` fires.
    pub async fn run_loop(
        self: Arc<Self>,
        tick: Duration,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if let Err(e) = self.pass().await {
                tracing::warn!(code = %e.code, "reconciliation pass failed");
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(tick) => {}
                _ = stop.changed() => { if *stop.borrow() { return } }
            }
            if *stop.borrow() {
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: String,
    pub operation_id: Option<String>,
    pub revision: String,
}

/// Index+WAL size at or above `storage.pause_ingestion_bytes` pauses new ingestion; existing search
/// stays up (spec 17.5).
pub async fn storage_paused(index: &Index, cfg: &Config) -> bool {
    storage_bytes(index)
        .await
        .is_some_and(|b| b >= cfg.storage.pause_ingestion_bytes)
}

/// Approximate `index.sqlite` + WAL bytes.
pub async fn storage_bytes(index: &Index) -> Option<u64> {
    let (db, wal) = file_sizes(&index.reader).await?;
    Some(db + wal)
}

pub async fn file_sizes(pool: &SqlitePool) -> Option<(u64, u64)> {
    let file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(pool)
            .await
            .ok()?;
    if file.is_empty() {
        return None;
    }
    let db = std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
    let wal = std::fs::metadata(format!("{file}-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    Some((db, wal))
}

pub fn new_op_key(prefix: &str) -> String {
    ids::new_id(prefix)
}
