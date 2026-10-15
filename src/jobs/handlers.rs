//! Task bodies. Handlers never trust payloads: they reread the catalog, call the Apalis-independent
//! `IngestEngine::index_session`, and settle the session in `index.sqlite`. They return `Ok` for every
//! outcome they handled (so Apalis acknowledges only after durable application progress) and `Err`
//! only when work was interrupted by shutdown (the task stays retryable and is never acknowledged).
//! Transient failures return `TransientIngest`; the Apalis `retry` layer (`scheduler::IngestRetry`)
//! applies the backoff on the same task. Recurring ticks use `run_after`.

use super::ops;
use super::payload::*;
use super::scheduler::*;
use crate::clock::SharedClock;
use crate::config::Config;
use crate::discovery::{DiscoveryService, Mode, Selection};
use crate::embeddings::{EmbeddingService, EmbeddingState};
use crate::ingest::engine::{
    DeferReason, IndexOutcome, IndexRequest, IngestEngine, IngestError, IngestErrorKind,
};
use crate::sources::SourceRegistry;
use crate::storage::catalog::{self, SessionRow};
use crate::storage::index::Index;
use crate::transcript::lease::LeaseManager;
use apalis_core::backend::Vacuum;
use futures::FutureExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

pub type JobError = Box<dyn std::error::Error + Send + Sync>;

/// How long a session whose retries are exhausted (or whose provider is blocked) rests before
/// reconciliation looks at it again: bounded, so a long outage is a slow trickle, never a storm.
pub const EXHAUSTED_REST: Duration = Duration::from_secs(15 * 60);
const CAPACITY_RECHECK_MS: u64 = 10_000;
const STORAGE_REST: Duration = Duration::from_secs(5 * 60);
const MAINTENANCE_EVERY: Duration = Duration::from_secs(3600);
const STAGING_MAX_AGE_MS: i64 = 3_600_000;
const EMBEDDING_GRACE_MS: i64 = 3_600_000;
const JOB_PRUNE_THRESHOLD: i64 = 500;

pub struct JobCtx {
    pub index: Index,
    pub sub: Submitter,
    pub cfg: Arc<Config>,
    pub sources: Arc<SourceRegistry>,
    pub engine: Arc<IngestEngine>,
    pub embeddings: Arc<EmbeddingService>,
    pub discovery: Arc<DiscoveryService>,
    pub leases: LeaseManager,
    pub reconciler: Arc<Reconciler>,
    pub clock: SharedClock,
    pub shutdown: AtomicBool,
    inflight: Mutex<HashMap<u64, Arc<AtomicBool>>>,
    next_id: AtomicU64,
    /// Concurrent ingestion sessions = 1 (spec 4.1), regardless of how the worker buffers tasks.
    pub ingest_gate: Semaphore,
    /// Delay before a backfill paused at `max_queued_sessions` looks for capacity again.
    capacity_recheck_ms: AtomicU64,
}

impl JobCtx {
    /// `index`, `cfg` and `clock` are the submitter's; `embeddings` is the engine's.
    pub fn new(
        sub: Submitter,
        sources: Arc<SourceRegistry>,
        engine: Arc<IngestEngine>,
        discovery: Arc<DiscoveryService>,
        leases: LeaseManager,
        reconciler: Arc<Reconciler>,
    ) -> Self {
        Self {
            index: sub.index.clone(),
            cfg: sub.cfg.clone(),
            clock: sub.clock.clone(),
            embeddings: engine.embeddings().clone(),
            sub,
            sources,
            engine,
            discovery,
            leases,
            reconciler,
            shutdown: AtomicBool::new(false),
            inflight: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            ingest_gate: Semaphore::new(1),
            capacity_recheck_ms: AtomicU64::new(CAPACITY_RECHECK_MS),
        }
    }
    pub fn set_capacity_recheck(&self, d: Duration) {
        self.capacity_recheck_ms
            .store(d.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Cooperative-cancel scope: the flag is set by shutdown, by operation cancellation, or when the
    /// task future is dropped (a killed worker), so blocking work stops instead of running on.
    pub fn scope(self: &Arc<Self>, op: Option<String>) -> CancelScope {
        let flag = Arc::new(AtomicBool::new(self.is_shutting_down()));
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, flag.clone());
        let watcher = op.map(|op| {
            let (index, f) = (self.index.clone(), flag.clone());
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if ops::cancel_requested(&index, &op).await {
                        f.store(true, Ordering::Relaxed);
                        return;
                    }
                }
            })
        });
        CancelScope {
            flag,
            ctx: self.clone(),
            id,
            watcher,
        }
    }

    /// Graceful shutdown: stop accepting work and cancel everything in flight.
    pub fn begin_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        for f in self
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            f.store(true, Ordering::SeqCst);
        }
    }
}

pub struct CancelScope {
    pub flag: Arc<AtomicBool>,
    ctx: Arc<JobCtx>,
    id: u64,
    watcher: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for CancelScope {
    fn drop(&mut self) {
        self.flag.store(true, Ordering::SeqCst);
        if let Some(w) = self.watcher.take() {
            w.abort();
        }
        self.ctx
            .inflight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

fn shutdown_err() -> JobError {
    "worker is shutting down; task stays queued".into()
}

// ---------------------------------------------------------------------------------------------
// IndexSession
// ---------------------------------------------------------------------------------------------

pub async fn index_session_job(
    ctx: Arc<JobCtx>,
    job: IndexSession,
    task_id: String,
    attempt: usize,
) -> Result<(), JobError> {
    let sid = job.session_id.clone();
    let res = AssertUnwindSafe(run_index(&ctx, &job, attempt))
        .catch_unwind()
        .await;
    let out = match res {
        Ok(r) => r,
        Err(_) => {
            // A panic is a bug, not a transient condition: park the session visibly, never loop on it.
            let _ = catalog::set_state(
                &ctx.index,
                &sid,
                "failed",
                Some("internal_error"),
                None,
                None,
            )
            .await;
            Ok(())
        }
    };
    if out.is_ok() {
        let _ = sqlx::query("UPDATE sessions SET task_id = NULL WHERE id = ?1 AND task_id = ?2")
            .bind(&sid)
            .bind(&task_id)
            .execute(&ctx.index.writer)
            .await;
        ctx.reconciler.kick();
    }
    out
}

async fn run_index(ctx: &Arc<JobCtx>, job: &IndexSession, attempt: usize) -> Result<(), JobError> {
    let idx = &ctx.index;
    let now = ctx.clock.now_ms();
    if job.payload_version != PAYLOAD_VERSION {
        let _ = catalog::set_state(
            idx,
            &job.session_id,
            "failed",
            Some("unsupported_payload_version"),
            None,
            None,
        )
        .await;
        return Ok(());
    }
    if attempt > ctx.cfg.ingestion.max_transient_attempts as usize + 3 {
        // Repeatedly orphaned (the process died while running it): stop re-running a poison task.
        let _ = catalog::set_state(
            idx,
            &job.session_id,
            "failed",
            Some("crash_loop"),
            None,
            None,
        )
        .await;
        return Ok(());
    }
    let Some(s) = catalog::session(idx, &job.session_id)
        .await
        .map_err(|e| Box::new(e) as JobError)?
    else {
        return Ok(());
    };
    if s.excluded || s.source_status != "present" || !ctx.sources.is_enabled(&s.source_id) {
        return Ok(()); // exclusion/removal is stronger than any old task: never resurrect
    }
    if ctx.is_shutting_down() {
        return Err(shutdown_err());
    }
    if let Some(op) = &job.operation_id
        && ops::cancel_requested(idx, op).await
    {
        return Ok(());
    }
    if storage_paused(idx, &ctx.cfg).await {
        let _ = catalog::app_meta_set(idx, "ingest.paused", "storage").await;
        let _ = catalog::set_state(
            idx,
            &s.id,
            "deferred",
            Some("storage_pause"),
            Some(now + STORAGE_REST.as_millis() as i64),
            None,
        )
        .await;
        return Ok(());
    }
    let _ = catalog::app_meta_set(idx, "ingest.paused", "").await;
    let permit = ctx
        .ingest_gate
        .acquire()
        .await
        .map_err(|e| Box::new(e) as JobError)?;
    let scope = ctx.scope(job.operation_id.clone());
    let req = IndexRequest {
        session_id: s.id.clone(),
        observed_source_revision: s
            .revision()
            .or_else(|| Some(job.observed_source_revision.clone())),
        fingerprint: None,
    };
    let res = ctx.engine.index_session(req, scope.flag.clone()).await;
    drop(permit);
    let cancelled_by_op = if let Some(op) = &job.operation_id {
        ops::cancel_requested(idx, op).await
    } else {
        false
    };
    drop(scope);
    settle(ctx, &s, attempt, res, cancelled_by_op).await
}

/// Map an engine result onto catalog state and, for transient failures, the next Apalis task.
async fn settle(
    ctx: &Arc<JobCtx>,
    s: &SessionRow,
    attempt: usize,
    res: Result<IndexOutcome, IngestError>,
    cancelled_by_op: bool,
) -> Result<(), JobError> {
    let idx = &ctx.index;
    let now = ctx.clock.now_ms();
    let set =
        |state: &'static str, err: Option<&str>, until: Option<i64>, attempts: Option<i64>| {
            let err = err.map(str::to_string);
            let id = s.id.clone();
            async move {
                if let Err(e) =
                    catalog::set_state(idx, &id, state, err.as_deref(), until, attempts).await
                {
                    tracing::warn!(error = %e, "could not record session state");
                }
            }
        };
    match res {
        Ok(o) => {
            let (missing, follow) = match &o {
                IndexOutcome::Published {
                    embeddings_pending,
                    follow_up_needed,
                    ..
                } => (*embeddings_pending, *follow_up_needed),
                IndexOutcome::EmbeddingsPending {
                    missing,
                    follow_up_needed,
                } => (*missing, *follow_up_needed),
                _ => (0, false),
            };
            match o {
                IndexOutcome::NoOp
                | IndexOutcome::Published { .. }
                | IndexOutcome::Embedded { .. }
                | IndexOutcome::EmbeddingsPending { .. } => {
                    if follow {
                        set("pending", None, None, Some(0)).await; // the file grew meanwhile: one more pass, not a chase
                    } else if missing > 0 {
                        let st = ctx.embeddings.status();
                        match st.state {
                            EmbeddingState::NotApproved
                            | EmbeddingState::NoCredential
                            | EmbeddingState::Disabled => settle_indexed(idx, &s.id).await,
                            EmbeddingState::BudgetExhausted { resets_at_ms } => {
                                set(
                                    "deferred",
                                    Some("budget_exhausted"),
                                    Some(resets_at_ms),
                                    Some(0),
                                )
                                .await
                            }
                            EmbeddingState::AuthBlocked => {
                                set(
                                    "deferred",
                                    Some("provider_auth"),
                                    Some(now + EXHAUSTED_REST.as_millis() as i64),
                                    Some(0),
                                )
                                .await
                            }
                            _ => transient(ctx, s, attempt, None, "embeddings_pending").await?,
                        }
                    } else {
                        settle_indexed(idx, &s.id).await;
                    }
                }
                IndexOutcome::Superseded => set("pending", None, None, None).await,
                IndexOutcome::Excluded => {}
                IndexOutcome::Deferred(DeferReason::SourceChanged) => {
                    set(
                        "pending",
                        None,
                        Some(now + ctx.cfg.discovery.debounce_seconds as i64 * 1000),
                        None,
                    )
                    .await
                }
                IndexOutcome::Deferred(DeferReason::SourceDisabled) => {
                    set("pending", None, None, None).await
                }
                IndexOutcome::Deferred(DeferReason::LimitExceeded(_))
                | IndexOutcome::Unsupported => {} // engine recorded failed/unsupported
                IndexOutcome::SourceMissing => {
                    set(
                        "deferred",
                        Some("source_missing"),
                        Some(now + ctx.cfg.discovery.interval_seconds as i64 * 1000),
                        None,
                    )
                    .await
                }
            }
            Ok(())
        }
        Err(e) => match e.kind {
            IngestErrorKind::Cancelled => {
                if ctx.is_shutting_down() {
                    Err(shutdown_err()) // never acknowledge unfinished work
                } else {
                    if !cancelled_by_op {
                        set("pending", None, None, None).await;
                    }
                    Ok(())
                }
            }
            IngestErrorKind::AuthBlocked => {
                set(
                    "deferred",
                    Some("provider_auth"),
                    Some(now + EXHAUSTED_REST.as_millis() as i64),
                    Some(0),
                )
                .await;
                Ok(())
            }
            IngestErrorKind::BudgetDeferred => {
                // Not a failure and not counted: wait for the reset (or the provider's hint).
                let until = e
                    .retry_after
                    .map(|d| now + d.as_millis() as i64)
                    .unwrap_or_else(|| match ctx.embeddings.status().state {
                        EmbeddingState::BudgetExhausted { resets_at_ms } => resets_at_ms,
                        _ => now + EXHAUSTED_REST.as_millis() as i64,
                    });
                set("deferred", Some("budget_exhausted"), Some(until), Some(0)).await;
                Ok(())
            }
            IngestErrorKind::Permanent => {
                set(
                    "failed",
                    Some(&e.message.chars().take(200).collect::<String>()),
                    None,
                    None,
                )
                .await;
                Ok(())
            }
            IngestErrorKind::Storage => {
                let _ =
                    catalog::app_meta_set(idx, "ingest.storage_error_at", &now.to_string()).await;
                transient(ctx, s, attempt, e.retry_after, "storage_error").await
            }
            IngestErrorKind::Transient => {
                transient(ctx, s, attempt, e.retry_after, "transient").await
            }
        },
    }
}

async fn settle_indexed(idx: &Index, sid: &str) {
    let _ = sqlx::query(
        "UPDATE sessions SET index_state = 'indexed', index_attempts = 0, defer_until = NULL, index_error = NULL \
         WHERE id = ? AND excluded = 0 AND index_state IN ('pending','deferred')",
    )
    .bind(sid)
    .execute(&idx.writer)
    .await;
}

/// A transient failure is handed to the Apalis retry layer (`scheduler::IngestRetry`: exponential
/// backoff with jitter, `Retry-After` as a floor) by returning `TransientIngest`. Once
/// `max_transient_attempts` is used up the session rests for `EXHAUSTED_REST` (not failed).
async fn transient(
    ctx: &Arc<JobCtx>,
    s: &SessionRow,
    attempt: usize,
    retry_after: Option<Duration>,
    why: &str,
) -> Result<(), JobError> {
    let now = ctx.clock.now_ms();
    if attempt >= ctx.cfg.ingestion.max_transient_attempts as usize {
        let _ = catalog::set_state(
            &ctx.index,
            &s.id,
            "deferred",
            Some("transient_exhausted"),
            Some(now + EXHAUSTED_REST.as_millis() as i64),
            Some(0),
        )
        .await;
        return Ok(());
    }
    let _ = catalog::set_state(
        &ctx.index,
        &s.id,
        "pending",
        Some(why),
        None,
        Some(attempt as i64),
    )
    .await;
    Err(Box::new(TransientIngest { retry_after }))
}

// ---------------------------------------------------------------------------------------------
// control worker: discovery, maintenance, rebuild
// ---------------------------------------------------------------------------------------------

pub async fn control_job(ctx: Arc<JobCtx>, job: ControlJob) -> Result<(), JobError> {
    let op = match &job {
        ControlJob::Discover(d) => d.operation_id.clone(),
        ControlJob::Maintain(m) => m.operation_id.clone(),
        ControlJob::Rebuild(r) => Some(r.operation_id.clone()),
    };
    if job.payload_version() != PAYLOAD_VERSION {
        if let Some(op) = &op {
            let _ = ops::set_state(
                &ctx.index,
                op,
                "failed",
                Some("unsupported_payload_version"),
                ctx.clock.now_ms(),
            )
            .await;
        }
        return Ok(());
    }
    let res = AssertUnwindSafe(async {
        match job.clone() {
            ControlJob::Discover(d) => discover(&ctx, d).await,
            ControlJob::Maintain(m) => maintain(&ctx, m).await,
            ControlJob::Rebuild(r) => rebuild(&ctx, r).await,
        }
    })
    .catch_unwind()
    .await;
    match res {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) if ctx.is_shutting_down() => Err(e),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "control job failed");
            if let Some(op) = &op {
                let _ = ops::set_state(
                    &ctx.index,
                    op,
                    "failed",
                    Some("job_error"),
                    ctx.clock.now_ms(),
                )
                .await;
            }
            Ok(())
        }
        Err(_) => {
            if let Some(op) = &op {
                let _ = ops::set_state(
                    &ctx.index,
                    op,
                    "failed",
                    Some("internal_error"),
                    ctx.clock.now_ms(),
                )
                .await;
            }
            Ok(())
        }
    }
}

/// Arm the next recurring discovery tick with Apalis scheduling (`run_after`). The tick that was
/// missed while the daemon was stopped is not persisted by magic: startup reconciliation covers it.
pub async fn schedule_discovery_tick(
    ctx: &JobCtx,
    delay: Duration,
) -> Result<String, crate::ipc::AppError> {
    let due = ctx.clock.now_ms() / 1000 + delay.as_secs() as i64;
    let job = ControlJob::Discover(DiscoverSessions {
        payload_version: PAYLOAD_VERSION,
        source_ids: vec![],
        selection: DiscoverySelection::Auto,
        operation_id: None,
        recurring: true,
    });
    let id = ctx
        .sub
        .push_control(
            job,
            &format!("tick:discover:{due}:{}", crate::ids::new_id("t")),
            Some(delay),
        )
        .await?;
    let _ = catalog::app_meta_set(&ctx.index, "tick.discovery.task", &id).await;
    Ok(id)
}

pub async fn schedule_maintenance_tick(
    ctx: &JobCtx,
    delay: Duration,
) -> Result<String, crate::ipc::AppError> {
    let due = ctx.clock.now_ms() / 1000 + delay.as_secs() as i64;
    let job = ControlJob::Maintain(MaintainIndex {
        payload_version: PAYLOAD_VERSION,
        operation_id: None,
        scope: MaintenanceScope::Routine,
        recurring: true,
    });
    let id = ctx
        .sub
        .push_control(
            job,
            &format!("tick:maintain:{due}:{}", crate::ids::new_id("t")),
            Some(delay),
        )
        .await?;
    let _ = catalog::app_meta_set(&ctx.index, "tick.maintenance.task", &id).await;
    Ok(id)
}

async fn discover(ctx: &Arc<JobCtx>, d: DiscoverSessions) -> Result<(), JobError> {
    let ids = if d.source_ids.is_empty() {
        ctx.sources.enabled_ids()
    } else {
        d.source_ids.clone()
    };
    match &d.selection {
        DiscoverySelection::Auto => {
            let scope = ctx.scope(None);
            for id in &ids {
                if ctx.is_shutting_down() {
                    break;
                }
                if let Err(e) = ctx.discovery.run_auto(id, scope.flag.clone()).await {
                    tracing::warn!(source = %id, error = %e, "discovery failed");
                }
            }
            drop(scope);
            ctx.reconciler.kick();
            if ctx.is_shutting_down() {
                return Err(shutdown_err());
            }
            if d.recurring {
                schedule_discovery_tick(
                    ctx,
                    Duration::from_secs(ctx.cfg.discovery.interval_seconds),
                )
                .await
                .map_err(|e| Box::new(e) as JobError)?;
            }
            Ok(())
        }
        sel => explicit_discovery(ctx, &d, &ids, sel).await,
    }
}

async fn open_sessions(ctx: &JobCtx) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM sessions s WHERE s.excluded = 0 AND s.source_status = 'present' AND s.eligibility != 'backfill' AND s.index_state IN ('pending','deferred') AND {}",
        crate::storage::catalog::INDEXABLE_KIND
    )))
    .fetch_one(&ctx.index.reader)
    .await
    .unwrap_or(0)
}

/// `index --since/--all`: paged enumeration with a durable cursor in `operations.progress_json`.
/// Pauses (and re-arms itself) while `max_queued_sessions` is reached; never loads the corpus.
async fn explicit_discovery(
    ctx: &Arc<JobCtx>,
    d: &DiscoverSessions,
    ids: &[String],
    sel: &DiscoverySelection,
) -> Result<(), JobError> {
    let idx = &ctx.index;
    let Some(op_id) = d.operation_id.clone() else {
        return Ok(());
    };
    let Some(op) = ops::get(idx, &op_id)
        .await
        .map_err(|e| Box::new(e) as JobError)?
    else {
        return Ok(());
    };
    if matches!(op.state.as_str(), "done" | "failed" | "cancelled") {
        return Ok(());
    }
    let sel = match sel {
        DiscoverySelection::Since { since_ms } => Selection::Since(*since_ms),
        _ => Selection::All,
    };
    let now = || ctx.clock.now_ms();
    ops::set_state(idx, &op_id, "running", None, now())
        .await
        .ok();
    let mut source_index = op
        .progress
        .get("source_index")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    let mut after: Option<PathBuf> = op
        .progress
        .get("after")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    let (mut queued, mut scanned) = (
        op.progress
            .get("queued")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        op.progress
            .get("scanned")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    );
    let scope = ctx.scope(Some(op_id.clone()));
    while source_index < ids.len() {
        if ops::cancel_requested(idx, &op_id).await {
            ops::set_state(idx, &op_id, "cancelled", None, now())
                .await
                .ok();
            return Ok(());
        }
        if ctx.is_shutting_down() {
            return Err(shutdown_err());
        }
        let room = ctx.cfg.ingestion.max_queued_sessions as i64 - open_sessions(ctx).await;
        if room <= 0 {
            ops::merge_progress(idx, &op_id, &json!({"phase": "waiting_capacity", "source_index": source_index, "after": after, "queued": queued, "scanned": scanned}), now()).await.ok();
            let id = ctx
                .sub
                .push_control(
                    ControlJob::Discover(d.clone()),
                    &format!("cont:{op_id}:{}", crate::ids::new_id("c")),
                    Some(Duration::from_millis(
                        ctx.capacity_recheck_ms.load(Ordering::Relaxed),
                    )),
                )
                .await
                .map_err(|e| Box::new(e) as JobError)?;
            ops::set_task(idx, &op_id, &id, now()).await.ok();
            return Ok(());
        }
        let rep = ctx
            .discovery
            .run_explicit(
                &ids[source_index],
                sel.clone(),
                Mode::Enqueue(op_id.clone()),
                after.clone(),
                room as usize,
                scope.flag.clone(),
            )
            .await
            .map_err(|e| e.to_string())?;
        queued += rep.queued as u64;
        scanned += rep.scanned as u64;
        ctx.reconciler.kick();
        if rep.next_after.is_some() {
            after = rep.next_after.map(PathBuf::from); // stopped by capacity: persist and loop to re-check room
            ops::merge_progress(idx, &op_id, &json!({"phase": "enumerating", "source_index": source_index, "after": after, "queued": queued, "scanned": scanned}), now()).await.ok();
        } else if rep.finished {
            source_index += 1;
            after = None;
            ops::merge_progress(idx, &op_id, &json!({"phase": "enumerating", "source_index": source_index, "after": null, "queued": queued, "scanned": scanned}), now()).await.ok();
        } else if scope.flag.load(Ordering::Relaxed) && !ctx.is_shutting_down() {
            ops::set_state(idx, &op_id, "cancelled", None, now())
                .await
                .ok();
            return Ok(());
        } else if ctx.is_shutting_down() {
            return Err(shutdown_err());
        } else {
            // Incomplete enumeration (unreadable directory, root unavailable): keep progress, fail visibly.
            ops::merge_progress(idx, &op_id, &json!({"phase": "failed", "source_index": source_index, "after": after, "queued": queued, "scanned": scanned}), now()).await.ok();
            ops::set_state(
                idx,
                &op_id,
                "failed",
                Some(rep.error.as_deref().unwrap_or("scan_incomplete")),
                now(),
            )
            .await
            .ok();
            return Ok(());
        }
    }
    ops::merge_progress(idx, &op_id, &json!({"phase": "enumerated", "enumeration_done": true, "after": null, "queued": queued, "scanned": scanned}), now()).await.ok();
    ctx.reconciler.kick();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// maintenance
// ---------------------------------------------------------------------------------------------

async fn maintain(ctx: &Arc<JobCtx>, m: MaintainIndex) -> Result<(), JobError> {
    let idx = &ctx.index;
    let now = ctx.clock.now_ms();
    let mut summary = json!({ "at_ms": now });
    let (leases, snaps) = ctx.leases.sweep().await.map_err(|e| e.message)?;
    summary["leases_released"] = json!(leases);
    summary["retired_snapshots_removed"] = json!(snaps);
    // Interrupted staging (a job died after staging, before publish) is invisible to search and
    // safe to discard; its session is re-enqueued by reconciliation.
    let staged = sqlx::query(
        "DELETE FROM snapshots WHERE kind = 'indexed' AND state = 'staging' AND created_at < ?",
    )
    .bind(now - STAGING_MAX_AGE_MS)
    .execute(&idx.writer)
    .await?
    .rows_affected();
    summary["stale_staging_removed"] = json!(staged);
    let grace = if m.scope == MaintenanceScope::PurgeUnreferenced {
        0
    } else {
        EMBEDDING_GRACE_MS
    };
    summary["embeddings_purged"] = json!(purge_unreferenced_embeddings(idx, now - grace).await?);
    sqlx::query(
        "DELETE FROM operations WHERE state IN ('done','failed','cancelled') AND updated_at < ?",
    )
    .bind(now - 30 * catalog::DAY_MS)
    .execute(&idx.writer)
    .await?;
    // Completed Apalis rows are pruned through the backend's own API once enough accumulate.
    let counts = ctx.sub.counts(INGEST_QUEUE).await.map_err(|e| e.message)?;
    if counts.done + counts.killed >= JOB_PRUNE_THRESHOLD
        || m.scope == MaintenanceScope::PurgeUnreferenced
    {
        let mut st =
            super::backend::storage::<IndexSession>(&ctx.sub.pool, INGEST_QUEUE, ctx.sub.liveness);
        match st.vacuum_before(Duration::from_secs(0)).await {
            Ok(n) => summary["jobs_pruned"] = json!(n),
            Err(e) => tracing::warn!(error = %e, "job pruning failed"),
        }
    }
    // Bounded, non-blocking checkpoints: PASSIVE never waits for readers.
    let _ = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .execute(&idx.writer)
        .await;
    let _ = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .execute(&ctx.sub.pool)
        .await;
    catalog::app_meta_set(idx, "maintenance.last", &summary.to_string()).await?;
    if let Some(op) = &m.operation_id {
        ops::merge_progress(
            idx,
            op,
            &json!({"enumeration_done": true, "result": summary}),
            now,
        )
        .await
        .ok();
        ops::set_state(idx, op, "done", None, now).await.ok();
    }
    if m.recurring && !ctx.is_shutting_down() {
        schedule_maintenance_tick(ctx, MAINTENANCE_EVERY)
            .await
            .map_err(|e| Box::new(e) as JobError)?;
    }
    Ok(())
}

/// Delete embeddings no chunk references (any profile; the hash is content-addressed) older than
/// `created_before_ms`. Bounded batches; returns rows removed.
pub async fn purge_unreferenced_embeddings(
    idx: &Index,
    created_before_ms: i64,
) -> Result<u64, sqlx::Error> {
    let mut total = 0;
    for _ in 0..40 {
        let n = sqlx::query(
            "DELETE FROM embeddings WHERE (profile_id, input_hash) IN (SELECT e.profile_id, e.input_hash FROM embeddings e \
             WHERE e.created_at <= ?1 AND NOT EXISTS (SELECT 1 FROM chunks c WHERE c.input_hash = e.input_hash) LIMIT 5000)",
        )
        .bind(created_before_ms)
        .execute(&idx.writer)
        .await?
        .rows_affected();
        total += n;
        if n < 5000 {
            break;
        }
    }
    Ok(total)
}

// ---------------------------------------------------------------------------------------------
// rebuild (spec 10.6): never broadens scope; only sessions that already have derived data
// ---------------------------------------------------------------------------------------------

async fn rebuild(ctx: &Arc<JobCtx>, r: RebuildIndex) -> Result<(), JobError> {
    let idx = &ctx.index;
    let Some(op) = ops::get(idx, &r.operation_id)
        .await
        .map_err(|e| Box::new(e) as JobError)?
    else {
        return Ok(());
    };
    if matches!(op.state.as_str(), "done" | "failed" | "cancelled") {
        return Ok(());
    }
    ops::set_state(idx, &r.operation_id, "running", None, ctx.clock.now_ms())
        .await
        .ok();
    let status = ctx.embeddings.status();
    let mut after = op
        .progress
        .get("after")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut touched = op
        .progress
        .get("touched")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    loop {
        if ops::cancel_requested(idx, &r.operation_id).await {
            ops::set_state(idx, &r.operation_id, "cancelled", None, ctx.clock.now_ms())
                .await
                .ok();
            return Ok(());
        }
        if ctx.is_shutting_down() {
            return Err(shutdown_err());
        }
        let page: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT s.id, s.active_snapshot_id FROM sessions s WHERE s.id > ?1 AND s.excluded = 0 AND s.source_status = 'present' AND s.active_snapshot_id IS NOT NULL ORDER BY s.id LIMIT 100",
        )
        .bind(&after)
        .fetch_all(&idx.reader)
        .await?;
        if page.is_empty() {
            break;
        }
        for (sid, snap) in page {
            after = sid.clone();
            if r.lexical {
                // Clears and recreates derived search data; generation bump fences any old task.
                catalog::remove_derived(idx, &sid, ctx.clock.now_ms()).await?;
            } else if r.embeddings {
                let gap: Option<i64> = sqlx::query_scalar(
                    "SELECT 1 FROM chunks c WHERE c.snapshot_id = ?1 AND NOT EXISTS (SELECT 1 FROM embeddings e WHERE e.profile_id = ?2 AND e.input_hash = c.input_hash) LIMIT 1",
                )
                .bind(snap.as_deref())
                .bind(&status.profile_id)
                .fetch_optional(&idx.reader)
                .await?;
                if gap.is_none() {
                    continue;
                }
            }
            touched += 1;
            if !r.lexical {
                // Text stays published and `indexed`; the vector gap is filled by reconciliation's
                // coverage scan (forced below), which only runs while the provider can serve it.
                continue;
            }
            sqlx::query("UPDATE sessions SET task_id = NULL WHERE id = ?")
                .bind(&sid)
                .execute(&idx.writer)
                .await?;
            let mut conn = idx.writer.acquire().await?;
            catalog::mark_pending(&mut conn, &sid, Some(&r.operation_id)).await?;
        }
        ops::merge_progress(
            idx,
            &r.operation_id,
            &json!({"after": after, "touched": touched, "phase": "rebuilding"}),
            ctx.clock.now_ms(),
        )
        .await
        .ok();
    }
    ops::merge_progress(idx, &r.operation_id, &json!({"after": null, "touched": touched, "phase": "enumerated", "enumeration_done": true}), ctx.clock.now_ms()).await.ok();
    ctx.reconciler.force_slow_scan();
    ctx.reconciler.kick();
    Ok(())
}
