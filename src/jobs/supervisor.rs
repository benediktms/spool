//! `JobSystem`: one ingestion worker and one control/maintenance worker, each supervised
//! independently of the request server (spec 11.8). A stopped or repeatedly failing worker shows up
//! as `ingestion_degraded` and is restarted with bounded, backed-off attempts; search keeps running.
//!
//! Registering a worker name fails with `WORKER_ALREADY_EXISTS` until the previous incarnation's
//! `last_seen` is older than the heartbeat (docs/dependency-baseline.md), which is expected after
//! any restart. The supervisor reads that lease first and sleeps out the remainder, so Apalis never
//! logs the refusal; a refusal that still happens is retried without counting as a worker failure.

use super::backend::{self, Liveness};
use super::handlers::{self, JobCtx};
use super::ops::{self, OpRow};
use super::payload::*;
use super::scheduler::*;
use crate::clock::SharedClock;
use crate::config::Config;
use crate::discovery::DiscoveryService;
use crate::ingest::engine::IngestEngine;
use crate::ipc::AppError;
use crate::sources::SourceRegistry;
use crate::storage::catalog;
use crate::storage::index::Index;
use crate::transcript::lease::LeaseManager;
use apalis::prelude::{BackendExt, Event, TaskId, WorkerBuilder, WorkerBuilderExt};
use apalis_core::error::WorkerError;
use apalis_core::worker::ext::event_listener::EventListenerExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::SqlitePool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

/// A worker that stayed up this long is considered healthy again (failure counter resets).
const STABLE_AFTER: Duration = Duration::from_secs(30);
const DEGRADED_AFTER: u32 = 3;
/// Beyond this many consecutive failures restarts slow to one attempt per `SLOW_RESTART`.
const MAX_FAST_RESTARTS: u32 = 8;
const SLOW_RESTART: Duration = Duration::from_secs(300);
const RESTART_CAP: Duration = Duration::from_secs(30);
const RECONCILE_TICK: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Starting,
    Running,
    Restarting,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStatus {
    pub name: String,
    pub state: WorkerState,
    pub restarts: u32,
    pub consecutive_failures: u32,
    /// Content-free category of the last failure.
    pub last_error: Option<String>,
    pub last_started_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerHealth {
    pub ingest: WorkerStatus,
    pub control: WorkerStatus,
    /// True while the ingestion worker is not running: search is unaffected.
    pub ingestion_degraded: bool,
}

type Status = Arc<Mutex<WorkerStatus>>;
// The worker future is not provably `Send` (rustc cannot see through apalis' higher-ranked error
// conversions), so it runs to completion on a dedicated blocking thread via `Handle::block_on`
// and is "killed" by dropping it from a `select!`.
type Make = Arc<
    dyn Fn(
            Arc<JobCtx>,
            Status,
            watch::Receiver<bool>,
        ) -> LocalBoxFuture<'static, Result<(), WorkerError>>
        + Send
        + Sync,
>;
type KillSlot = Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>;

fn new_status(name: &str) -> Status {
    Arc::new(Mutex::new(WorkerStatus {
        name: name.into(),
        state: WorkerState::Starting,
        restarts: 0,
        consecutive_failures: 0,
        last_error: None,
        last_started_ms: None,
    }))
}

fn update(s: &Status, f: impl FnOnce(&mut WorkerStatus)) {
    f(&mut s.lock().unwrap_or_else(|e| e.into_inner()));
}

/// Apalis emits `Start` before backend registration. `HeartBeat` follows a successful
/// registration (even for an idle worker), so it is the readiness signal.
fn record_worker_event(status: &Status, clock: &SharedClock, event: &Event) {
    if !matches!(event, Event::HeartBeat) {
        return;
    }
    update(status, |s| {
        if s.state != WorkerState::Running {
            s.state = WorkerState::Running;
            s.last_started_ms = Some(clock.now_ms());
        }
        if s.last_error.as_deref() == Some("register_refused") {
            s.last_error = None;
        }
    });
}

/// Seconds until the backend will accept `name` again, mirroring `register_worker.sql`
/// (refused while `now - last_seen <= heartbeat`). `None` means register now. A failed read also
/// means register now: the backend then reports the genuine error itself.
async fn registration_wait(pool: &SqlitePool, name: &str, heartbeat: Duration) -> Option<Duration> {
    let left: i64 = sqlx::query_scalar(
        "SELECT ?1 - (strftime('%s', 'now') - last_seen) + 1 FROM Workers WHERE id = ?2",
    )
    .bind(i64::try_from(heartbeat.as_secs()).ok()?)
    .bind(name)
    .fetch_optional(pool)
    .await
    .ok()??;
    u64::try_from(left)
        .ok()
        .filter(|s| *s > 0)
        .map(Duration::from_secs)
}

async fn stale_heartbeat(pool: &SqlitePool, name: &str, liveness: Liveness, status: &Status) {
    let Some(limit) = u32::try_from(liveness.missed_heartbeats)
        .ok()
        .and_then(|missed| liveness.heartbeat_interval.checked_mul(missed))
        .and_then(|duration| duration.checked_add(Duration::from_secs(1)))
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
    else {
        return std::future::pending().await;
    };
    loop {
        tokio::time::sleep(liveness.heartbeat_interval).await;
        let running =
            status.lock().unwrap_or_else(|e| e.into_inner()).state == WorkerState::Running;
        if !running {
            continue;
        }
        let read = sqlx::query_scalar::<_, i64>(
            "SELECT strftime('%s', 'now') - last_seen FROM Workers WHERE id = ?1",
        )
        .bind(name)
        .fetch_optional(pool);
        if matches!(tokio::time::timeout(liveness.heartbeat_interval, read).await, Ok(Ok(Some(age))) if age > limit)
        {
            return;
        }
    }
}

async fn watch_worker(
    mut worker: JoinHandle<Option<Result<(), WorkerError>>>,
    pool: &SqlitePool,
    name: &str,
    liveness: Liveness,
    status: &Status,
    abort_slot: &KillSlot,
) -> (
    Result<Option<Result<(), WorkerError>>, tokio::task::JoinError>,
    bool,
) {
    let result = tokio::select! {
        result = &mut worker => Some(result),
        () = stale_heartbeat(pool, name, liveness, status) => None,
    };
    if let Some(result) = result {
        return (result, false);
    }
    update(status, |s| {
        s.state = WorkerState::Restarting;
        s.last_error = Some("heartbeat_stale".into());
    });
    if let Some(kill) = abort_slot.lock().unwrap_or_else(|e| e.into_inner()).take() {
        let _ = kill.send(());
    }
    (worker.await, true)
}

async fn stop_signal(mut rx: watch::Receiver<bool>) -> Result<(), WorkerError> {
    let _ = rx.wait_for(|v| *v).await;
    Ok(())
}

fn make_ingest() -> Make {
    Arc::new(|ctx, status, stop| {
        async move {
            let storage =
                backend::storage::<IndexSession>(&ctx.sub.pool, INGEST_QUEUE, ctx.sub.liveness)
                    .poll_with_interval(ctx.sub.liveness.poll_interval);
            let c = ctx.clone();
            let handler =
                move |job: IndexSession, task_id: TaskId, attempt: apalis::prelude::Attempt| {
                    let c = c.clone();
                    async move {
                        handlers::index_session_job(c, job, task_id.to_string(), attempt.current())
                            .await
                    }
                };
            let clock = ctx.clock.clone();
            WorkerBuilder::new(INGEST_WORKER)
                .backend(storage)
                .retry(IngestRetry::new(
                    ctx.cfg.ingestion.max_transient_attempts as usize,
                ))
                .on_event(move |_, ev| record_worker_event(&status, &clock, ev))
                .build(handler)
                .run_until(stop_signal(stop))
                .await
        }
        .boxed_local()
    })
}

fn make_control() -> Make {
    Arc::new(|ctx, status, stop| {
        async move {
            let storage =
                backend::storage::<ControlJob>(&ctx.sub.pool, CONTROL_QUEUE, ctx.sub.liveness)
                    .poll_with_interval(ctx.sub.liveness.poll_interval);
            let c = ctx.clone();
            let handler = move |job: ControlJob| {
                let c = c.clone();
                async move { handlers::control_job(c, job).await }
            };
            let clock = ctx.clock.clone();
            WorkerBuilder::new(CONTROL_WORKER)
                .backend(storage)
                .on_event(move |_, ev| record_worker_event(&status, &clock, ev))
                .build(handler)
                .run_until(stop_signal(stop))
                .await
        }
        .boxed_local()
    })
}

/// Restart loop for one worker. Never exits on failure, only on the stop signal.
fn supervise(
    ctx: Arc<JobCtx>,
    status: Status,
    abort_slot: KillSlot,
    mut stop: watch::Receiver<bool>,
    make: Make,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consecutive = 0u32;
        loop {
            if *stop.borrow() {
                break;
            }
            let name = status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .name
                .clone();
            if let Some(wait) =
                registration_wait(&ctx.sub.pool, &name, ctx.sub.liveness.heartbeat_interval).await
            {
                tracing::info!(worker = %name, wait_secs = wait.as_secs(), "waiting for previous worker lease to expire");
                tokio::select! { _ = tokio::time::sleep(wait) => {}, _ = stop.changed() => {} }
                continue;
            }
            let started = Instant::now();
            let (kill_tx, kill_rx) = tokio::sync::oneshot::channel::<()>();
            *abort_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(kill_tx);
            let (m, c, s, st) = (make.clone(), ctx.clone(), status.clone(), stop.clone());
            let rt = tokio::runtime::Handle::current();
            let worker = tokio::task::spawn_blocking(move || {
                rt.block_on(async move {
                    let mut w = m(c, s, st);
                    tokio::select! { r = &mut w => Some(r), _ = kill_rx => None }
                })
            });
            let (res, heartbeat_stale) = watch_worker(
                worker,
                &ctx.sub.pool,
                &name,
                ctx.sub.liveness,
                &status,
                &abort_slot,
            )
            .await;
            *abort_slot.lock().unwrap_or_else(|e| e.into_inner()) = None;
            if *stop.borrow() {
                break;
            }
            if started.elapsed() >= STABLE_AFTER {
                consecutive = 0;
            }
            let (why, already_registered) = if heartbeat_stale {
                ("heartbeat_stale", false)
            } else {
                match &res {
                    Ok(Some(Ok(()))) => ("worker_stopped", false),
                    Ok(Some(Err(e))) => (
                        if e.to_string().contains("WORKER_ALREADY_EXISTS") {
                            "register_refused"
                        } else {
                            "worker_error"
                        },
                        e.to_string().contains("WORKER_ALREADY_EXISTS"),
                    ),
                    Ok(None) => ("worker_killed", false),
                    Err(_) => ("worker_panicked", false),
                }
            };
            if already_registered {
                update(&status, |s| {
                    s.state = WorkerState::Restarting;
                    s.last_error = Some(why.into());
                });
                tokio::select! { _ = tokio::time::sleep(Duration::from_millis(500)) => {}, _ = stop.changed() => {} }
                continue;
            }
            consecutive += 1;
            update(&status, |s| {
                s.restarts += 1;
                s.consecutive_failures = consecutive;
                s.last_error = Some(why.into());
                s.state = if consecutive >= DEGRADED_AFTER {
                    WorkerState::Degraded
                } else {
                    WorkerState::Restarting
                };
            });
            tracing::warn!(worker = %status.lock().map(|s| s.name.clone()).unwrap_or_default(), reason = why, consecutive, "worker stopped; restarting");
            let delay = if consecutive > MAX_FAST_RESTARTS {
                SLOW_RESTART
            } else {
                Duration::from_secs(1u64 << consecutive.min(5)).min(RESTART_CAP)
            };
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = stop.changed() => {} }
        }
        update(&status, |s| s.state = WorkerState::Stopped);
    })
}

/// Result of a durable submission: both ids exist only once the task row is stored.
#[derive(Debug, Clone, Serialize)]
pub struct Submission {
    pub operation_id: String,
    pub task_id: String,
    /// False when an idempotent replay returned the existing operation.
    pub created: bool,
}

pub struct JobSystem {
    ctx: Arc<JobCtx>,
    stop: watch::Sender<bool>,
    ingest: Status,
    control: Status,
    ingest_abort: KillSlot,
    control_abort: KillSlot,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl JobSystem {
    pub async fn start(
        index: Index,
        jobs_pool: SqlitePool,
        cfg: Arc<Config>,
        sources: Arc<SourceRegistry>,
        engine: Arc<IngestEngine>,
        clock: SharedClock,
        liveness: Liveness,
    ) -> anyhow::Result<Arc<Self>> {
        backend::setup(&jobs_pool).await?;
        catalog::sync_sources(&index, &sources.entries()).await?;
        let embeddings = engine.embeddings().clone();
        let sub = Submitter {
            index: index.clone(),
            pool: jobs_pool,
            cfg: cfg.clone(),
            liveness,
            clock: clock.clone(),
        };
        let discovery = Arc::new(DiscoveryService::new(
            index.clone(),
            sources.clone(),
            cfg.clone(),
            clock.clone(),
        ));
        let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), clock.clone());
        let reconciler = Arc::new(Reconciler::new(
            index.clone(),
            sub.clone(),
            engine.clone(),
            embeddings.clone(),
            cfg.clone(),
            clock.clone(),
        ));
        let ctx = Arc::new(JobCtx::new(
            sub,
            sources,
            engine,
            discovery,
            leases,
            reconciler.clone(),
        ));
        let (stop, stop_rx) = watch::channel(false);
        let me = Arc::new(Self {
            ctx: ctx.clone(),
            stop,
            ingest: new_status(INGEST_WORKER),
            control: new_status(CONTROL_WORKER),
            ingest_abort: Arc::default(),
            control_abort: Arc::default(),
            tasks: Mutex::new(Vec::new()),
        });
        // Startup reconciliation (spec 17.1 step 7): a tick that never fired while we were stopped is
        // not persisted, so discover immediately, make sure the recurring chains exist, then
        // reconcile desired work against the queue.
        me.ensure_recurring().await?;
        let mut tasks = vec![
            supervise(
                ctx.clone(),
                me.ingest.clone(),
                me.ingest_abort.clone(),
                stop_rx.clone(),
                make_ingest(),
            ),
            supervise(
                ctx.clone(),
                me.control.clone(),
                me.control_abort.clone(),
                stop_rx.clone(),
                make_control(),
            ),
            tokio::spawn(reconciler.clone().run_loop(RECONCILE_TICK, stop_rx)),
        ];
        me.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .append(&mut tasks);
        Ok(me)
    }

    /// Catch-up discovery now; recurring discovery/maintenance chains if none is live.
    async fn ensure_recurring(&self) -> anyhow::Result<()> {
        let ctx = &self.ctx;
        let now = ctx.clock.now_ms();
        let catch_up = ControlJob::Discover(DiscoverSessions {
            payload_version: PAYLOAD_VERSION,
            source_ids: vec![],
            selection: DiscoverySelection::Auto,
            operation_id: None,
            recurring: false,
        });
        ctx.sub
            .push_control(
                catch_up,
                &format!("startup:discover:{now}:{}", crate::ids::new_id("s")),
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!(e.message))?;
        for (key, discovery) in [
            ("tick.discovery.task", true),
            ("tick.maintenance.task", false),
        ] {
            let live = match catalog::app_meta_get(&ctx.index, key).await? {
                Some(id) => {
                    ctx.sub
                        .live_ids(&[id])
                        .await
                        .map_err(|e| anyhow::anyhow!(e.message))?
                        .len()
                        == 1
                }
                None => false,
            };
            if !live {
                let r = if discovery {
                    handlers::schedule_discovery_tick(
                        ctx,
                        Duration::from_secs(ctx.cfg.discovery.interval_seconds),
                    )
                    .await
                } else {
                    handlers::schedule_maintenance_tick(ctx, Duration::from_secs(60)).await
                };
                r.map_err(|e| anyhow::anyhow!(e.message))?;
            }
        }
        Ok(())
    }

    pub fn ctx(&self) -> &Arc<JobCtx> {
        &self.ctx
    }

    pub fn reconciler(&self) -> &Arc<Reconciler> {
        &self.ctx.reconciler
    }

    pub fn submitter(&self) -> &Submitter {
        &self.ctx.sub
    }

    pub fn health(&self) -> WorkerHealth {
        let ingest = self
            .ingest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let control = self
            .control
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let ingestion_degraded = matches!(
            ingest.state,
            WorkerState::Restarting | WorkerState::Degraded
        );
        WorkerHealth {
            ingest,
            control,
            ingestion_degraded,
        }
    }

    /// Graceful stop: refuse new work, cancel in-flight blocking work cooperatively, let workers
    /// drain, then abort whatever is left after `grace` (spec 17.2: 15 s target). Unfinished tasks are
    /// never acknowledged as done; they resume at the next start.
    pub async fn shutdown(&self, grace: Duration) {
        self.ctx.begin_shutdown();
        let _ = self.stop.send(true);
        self.ctx.reconciler.kick();
        let tasks: Vec<JoinHandle<()>> =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|e| e.into_inner()));
        let aborts: Vec<AbortHandle> = tasks.iter().map(|t| t.abort_handle()).collect();
        if tokio::time::timeout(grace, futures::future::join_all(tasks))
            .await
            .is_err()
        {
            for slot in [&self.ingest_abort, &self.control_abort] {
                if let Some(k) = slot.lock().unwrap_or_else(|e| e.into_inner()).take() {
                    let _ = k.send(());
                }
            }
            for a in aborts {
                a.abort();
            }
        }
    }

    /// Test hook (spec 19.4): abort the ingestion worker future as a SIGKILL would — no
    /// acknowledgement, no deregistration, in-flight work cancelled — while the request server and
    /// the supervisor stay up. The supervisor then restarts it.
    pub async fn kill_ingest_worker_for_test(&self) {
        if let Some(k) = self
            .ingest_abort
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = k.send(());
        }
    }

    // -----------------------------------------------------------------------------------------
    // durable submission
    // -----------------------------------------------------------------------------------------

    /// Create or replay an operation. A replay whose first attempt never reached its task (crash or
    /// enqueue failure) is re-armed so the retry actually submits.
    async fn begin_op(
        &self,
        kind: &str,
        params: &Value,
        key: Option<&str>,
    ) -> Result<(OpRow, bool), AppError> {
        let now = self.ctx.clock.now_ms();
        let (op, created) = ops::begin(&self.ctx.index, kind, params, key, now).await?;
        if !created && op.task_id.is_none() {
            if op.state == "failed" {
                ops::rearm(&self.ctx.index, &op.id, now).await?;
            }
            return Ok((ops::get(&self.ctx.index, &op.id).await?.unwrap_or(op), true));
        }
        Ok((op, created))
    }

    /// The enqueue failed: the request must not be acknowledged as accepted.
    async fn fail_unaccepted(&self, op: &OpRow, e: AppError) -> AppError {
        let _ = ops::set_state(
            &self.ctx.index,
            &op.id,
            "failed",
            Some("not_accepted"),
            self.ctx.clock.now_ms(),
        )
        .await;
        e
    }

    pub async fn submit_index_session(
        &self,
        session_id: &str,
        idempotency_key: Option<&str>,
    ) -> Result<Submission, AppError> {
        let ctx = &self.ctx;
        let s = catalog::session(&ctx.index, session_id)
            .await
            .map_err(ops::storage_err)?
            .ok_or_else(|| AppError::new("SESSION_NOT_FOUND", "no such session"))?;
        if s.excluded {
            return Err(AppError::new(
                "SOURCE_POLICY_CONFLICT",
                "the session is excluded; remove the exclusion first",
            ));
        }
        if !ctx.sources.is_enabled(&s.source_id) {
            return Err(AppError::new(
                "SOURCE_POLICY_CONFLICT",
                "the session's source is disabled",
            ));
        }
        if s.session_kind == "subagent" && !ctx.sources.include_subagents(&s.source_id) {
            return Err(AppError::new(
                "SOURCE_POLICY_CONFLICT",
                "subagent sessions are excluded for this source",
            ));
        }
        let (op, created) = self
            .begin_op(
                "index_session",
                &json!({ "session_id": session_id }),
                idempotency_key,
            )
            .await?;
        if !created && let Some(task_id) = op.task_id.clone() {
            return Ok(Submission {
                operation_id: op.id,
                task_id,
                created: false,
            });
        }
        match ctx.sub.has_room().await {
            Ok(true) => {}
            Ok(false) => {
                return Err(self
                    .fail_unaccepted(
                        &op,
                        AppError {
                            retryable: true,
                            recovery: Some("retry_later".into()),
                            ..AppError::new("INDEX_BUSY", "the index queue is full")
                        },
                    )
                    .await);
            }
            Err(e) => return Err(self.fail_unaccepted(&op, e).await),
        }
        let now = ctx.clock.now_ms();
        let mut conn = ctx.index.writer.acquire().await.map_err(ops::storage_err)?;
        if s.eligibility == "backfill" {
            catalog::set_eligibility(&mut conn, &s.id, "manual")
                .await
                .map_err(ops::storage_err)?;
        }
        catalog::mark_pending(&mut conn, &s.id, Some(&op.id))
            .await
            .map_err(ops::storage_err)?;
        drop(conn);
        ops::merge_progress(
            &ctx.index,
            &op.id,
            &json!({"enumeration_done": true, "session_id": session_id}),
            now,
        )
        .await?;
        let payload = IndexSession {
            payload_version: PAYLOAD_VERSION,
            session_id: s.id.clone(),
            observed_source_revision: s.revision().unwrap_or_default(),
            pipeline_fingerprint: ctx.engine.pipeline_fingerprint(),
            embedding_profile_id: ctx.embeddings.status().profile_id,
            operation_id: Some(op.id.clone()),
        };
        let task_id = match ctx
            .sub
            .push_index(payload, &format!("op:{}", op.id), None)
            .await
        {
            Ok(t) => t,
            Err(e) => return Err(self.fail_unaccepted(&op, e).await),
        };
        catalog::set_task(&ctx.index, &s.id, Some(&task_id))
            .await
            .map_err(ops::storage_err)?;
        ops::set_task(&ctx.index, &op.id, &task_id, now).await?;
        Ok(Submission {
            operation_id: op.id,
            task_id,
            created,
        })
    }

    /// `index --since/--all`: a durable discovery operation (never started by MCP, never implied).
    pub async fn submit_discovery(
        &self,
        selection: DiscoverySelection,
        source: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<Submission, AppError> {
        let ctx = &self.ctx;
        let kind = match selection {
            DiscoverySelection::Since { .. } => "index_since",
            DiscoverySelection::All => "index_all",
            DiscoverySelection::Auto => "discovery",
        };
        let source_ids = match source {
            Some(id) if !ctx.sources.is_enabled(id) => {
                return Err(AppError::new(
                    "INVALID_ARGUMENT",
                    "unknown or disabled source",
                ));
            }
            Some(id) => vec![id.to_string()],
            None => ctx.sources.enabled_ids(),
        };
        let params = json!({ "selection": selection, "sources": source_ids });
        let (op, created) = self.begin_op(kind, &params, idempotency_key).await?;
        if !created && let Some(task_id) = op.task_id.clone() {
            return Ok(Submission {
                operation_id: op.id,
                task_id,
                created: false,
            });
        }
        let job = ControlJob::Discover(DiscoverSessions {
            payload_version: PAYLOAD_VERSION,
            source_ids,
            selection,
            operation_id: Some(op.id.clone()),
            recurring: false,
        });
        self.push_control_for(&op, job, created).await
    }

    pub async fn submit_maintenance(
        &self,
        scope: MaintenanceScope,
        idempotency_key: Option<&str>,
    ) -> Result<Submission, AppError> {
        let kind = if scope == MaintenanceScope::PurgeUnreferenced {
            "index_purge"
        } else {
            "maintenance"
        };
        let (op, created) = self
            .begin_op(kind, &json!({ "scope": scope }), idempotency_key)
            .await?;
        if !created && let Some(task_id) = op.task_id.clone() {
            return Ok(Submission {
                operation_id: op.id,
                task_id,
                created: false,
            });
        }
        let job = ControlJob::Maintain(MaintainIndex {
            payload_version: PAYLOAD_VERSION,
            operation_id: Some(op.id.clone()),
            scope,
            recurring: false,
        });
        self.push_control_for(&op, job, created).await
    }

    /// Discovery of one source outside the recurring chain (e.g. right after `sources enable`).
    pub async fn submit_auto_discovery(&self, source: &str) -> Result<String, AppError> {
        let job = ControlJob::Discover(DiscoverSessions {
            payload_version: PAYLOAD_VERSION,
            source_ids: vec![source.to_string()],
            selection: DiscoverySelection::Auto,
            operation_id: None,
            recurring: false,
        });
        self.ctx
            .sub
            .push_control(
                job,
                &format!("manual-discover:{source}:{}", crate::ids::new_id("m")),
                None,
            )
            .await
    }

    pub async fn submit_rebuild(
        &self,
        lexical: bool,
        embeddings: bool,
        idempotency_key: Option<&str>,
    ) -> Result<Submission, AppError> {
        let (op, created) = self
            .begin_op(
                "index_rebuild",
                &json!({ "lexical": lexical, "embeddings": embeddings }),
                idempotency_key,
            )
            .await?;
        if !created && let Some(task_id) = op.task_id.clone() {
            return Ok(Submission {
                operation_id: op.id,
                task_id,
                created: false,
            });
        }
        let job = ControlJob::Rebuild(RebuildIndex {
            payload_version: PAYLOAD_VERSION,
            operation_id: op.id.clone(),
            lexical,
            embeddings,
        });
        self.push_control_for(&op, job, created).await
    }

    async fn push_control_for(
        &self,
        op: &OpRow,
        job: ControlJob,
        created: bool,
    ) -> Result<Submission, AppError> {
        let now = self.ctx.clock.now_ms();
        let task_id = match self
            .ctx
            .sub
            .push_control(job, &format!("op:{}", op.id), None)
            .await
        {
            Ok(t) => t,
            Err(e) => return Err(self.fail_unaccepted(op, e).await),
        };
        ops::set_task(&self.ctx.index, &op.id, &task_id, now).await?;
        Ok(Submission {
            operation_id: op.id.clone(),
            task_id,
            created,
        })
    }

    /// Cooperative cancel: stop enumerating/queueing, let the running session stop at its next
    /// check, keep everything already committed. Never touches source files or published data.
    pub async fn cancel_operation(&self, operation_id: &str) -> Result<OpRow, AppError> {
        let ctx = &self.ctx;
        let now = ctx.clock.now_ms();
        let op = ops::get(&ctx.index, operation_id)
            .await?
            .ok_or_else(|| AppError::new("OPERATION_NOT_FOUND", "no such operation"))?;
        if matches!(op.state.as_str(), "done" | "failed" | "cancelled") {
            return Ok(op);
        }
        ops::request_cancel(&ctx.index, &op.id, now).await?;
        // Work that was only requested by this operation returns to inert catalog state; sessions
        // that already have published data simply lose the operation tag and are processed normally.
        sqlx::query("UPDATE sessions SET eligibility = 'backfill' WHERE operation_id = ?1 AND index_state IN ('pending','deferred') AND active_snapshot_id IS NULL AND eligibility = 'manual'").bind(&op.id).execute(&ctx.index.writer).await.map_err(ops::storage_err)?;
        sqlx::query("UPDATE sessions SET operation_id = NULL WHERE operation_id = ?1 AND index_state IN ('pending','deferred')").bind(&op.id).execute(&ctx.index.writer).await.map_err(ops::storage_err)?;
        ops::set_state(&ctx.index, &op.id, "cancelled", None, now).await?;
        Ok(ops::get(&ctx.index, &op.id).await?.unwrap_or(op))
    }

    /// `jobs retry`: re-arm a failed/cancelled operation (resumes from its durable cursor) and reset
    /// the operation's `failed` sessions. Permanent errors are retried only on this explicit request.
    pub async fn retry_operation(&self, operation_id: &str) -> Result<Submission, AppError> {
        let ctx = &self.ctx;
        let now = ctx.clock.now_ms();
        let op = ops::get(&ctx.index, operation_id)
            .await?
            .ok_or_else(|| AppError::new("OPERATION_NOT_FOUND", "no such operation"))?;
        let reset = sqlx::query(
            "UPDATE sessions SET index_state = 'pending', index_attempts = 0, defer_until = NULL, index_error = NULL, task_id = NULL, \
             eligibility = CASE WHEN eligibility = 'backfill' THEN 'manual' ELSE eligibility END WHERE operation_id = ?1 AND index_state = 'failed' AND excluded = 0",
        )
        .bind(&op.id)
        .execute(&ctx.index.writer)
        .await
        .map_err(ops::storage_err)?
        .rows_affected();
        if !matches!(op.state.as_str(), "failed" | "cancelled") {
            if reset == 0 {
                return Err(AppError::new(
                    "INVALID_ARGUMENT",
                    "only failed or cancelled operations (or operations with failed sessions) can be retried",
                ));
            }
            ctx.reconciler.kick();
            let task_id = op.task_id.clone().unwrap_or_default();
            return Ok(Submission {
                operation_id: op.id,
                task_id,
                created: false,
            });
        }
        ops::rearm(&ctx.index, &op.id, now).await?;
        // A cancelled/failed enumeration restarts from the beginning: sessions it already reverted or
        // finished are skipped cheaply (`already_current`), none are missed.
        ops::merge_progress(&ctx.index, &op.id, &json!({"source_index": 0, "after": null, "enumeration_done": null, "queued": 0, "scanned": 0, "phase": "retrying"}), now).await?;
        let key = format!("op:{}:r{now}:{}", op.id, crate::ids::new_id("r"));
        let task_id = match op.kind.as_str() {
            "index_session" => {
                let sid = op
                    .params
                    .get("session_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let s = catalog::session(&ctx.index, &sid)
                    .await
                    .map_err(ops::storage_err)?
                    .ok_or_else(|| AppError::new("SESSION_NOT_FOUND", "no such session"))?;
                let mut conn = ctx.index.writer.acquire().await.map_err(ops::storage_err)?;
                catalog::mark_pending(&mut conn, &sid, Some(&op.id))
                    .await
                    .map_err(ops::storage_err)?;
                drop(conn);
                let payload = IndexSession {
                    payload_version: PAYLOAD_VERSION,
                    session_id: sid.clone(),
                    observed_source_revision: s.revision().unwrap_or_default(),
                    pipeline_fingerprint: ctx.engine.pipeline_fingerprint(),
                    embedding_profile_id: ctx.embeddings.status().profile_id,
                    operation_id: Some(op.id.clone()),
                };
                let t = ctx.sub.push_index(payload, &key, None).await?;
                catalog::set_task(&ctx.index, &sid, Some(&t))
                    .await
                    .map_err(ops::storage_err)?;
                t
            }
            "index_since" | "index_all" => {
                let sel: DiscoverySelection = op
                    .params
                    .get("selection")
                    .cloned()
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or(DiscoverySelection::All);
                let source_ids: Vec<String> = op
                    .params
                    .get("sources")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();
                ctx.sub
                    .push_control(
                        ControlJob::Discover(DiscoverSessions {
                            payload_version: PAYLOAD_VERSION,
                            source_ids,
                            selection: sel,
                            operation_id: Some(op.id.clone()),
                            recurring: false,
                        }),
                        &key,
                        None,
                    )
                    .await?
            }
            "index_rebuild" => {
                let lexical = op
                    .params
                    .get("lexical")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let embeddings = op
                    .params
                    .get("embeddings")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                ctx.sub
                    .push_control(
                        ControlJob::Rebuild(RebuildIndex {
                            payload_version: PAYLOAD_VERSION,
                            operation_id: op.id.clone(),
                            lexical,
                            embeddings,
                        }),
                        &key,
                        None,
                    )
                    .await?
            }
            _ => {
                let scope = if op.kind == "index_purge" {
                    MaintenanceScope::PurgeUnreferenced
                } else {
                    MaintenanceScope::Routine
                };
                ctx.sub
                    .push_control(
                        ControlJob::Maintain(MaintainIndex {
                            payload_version: PAYLOAD_VERSION,
                            operation_id: Some(op.id.clone()),
                            scope,
                            recurring: false,
                        }),
                        &key,
                        None,
                    )
                    .await?
            }
        };
        ops::set_task(&ctx.index, &op.id, &task_id, now).await?;
        Ok(Submission {
            operation_id: op.id,
            task_id,
            created: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    #[test]
    fn registration_refusal_clears_only_after_confirmed_readiness() {
        let status = new_status("worker");
        update(&status, |s| {
            s.state = WorkerState::Restarting;
            s.last_error = Some("register_refused".into());
        });
        let manual_clock = ManualClock::new(123);
        let clock: SharedClock = Arc::new(manual_clock.clone());

        record_worker_event(&status, &clock, &Event::Start);
        let premature = status.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(premature.state, WorkerState::Restarting);
        assert_eq!(premature.last_error.as_deref(), Some("register_refused"));
        assert_eq!(premature.last_started_ms, None);

        record_worker_event(&status, &clock, &Event::HeartBeat);
        let ready = status.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(ready.state, WorkerState::Running);
        assert_eq!(ready.last_error, None);
        assert_eq!(ready.last_started_ms, Some(123));

        manual_clock.advance_ms(1);
        record_worker_event(&status, &clock, &Event::HeartBeat);
        assert_eq!(
            status
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .last_started_ms,
            Some(123)
        );
    }

    #[tokio::test]
    async fn stale_pending_worker_is_cancelled_and_healthy_heartbeats_survive() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        backend::setup(&pool).await.unwrap();
        let liveness = Liveness {
            heartbeat_interval: Duration::from_millis(100),
            missed_heartbeats: 2,
            poll_interval: Duration::from_millis(100),
        };
        sqlx::query("INSERT INTO Workers (id, worker_type, storage_name, last_seen) VALUES ('w', 't', 's', strftime('%s', 'now'))")
            .execute(&pool).await.unwrap();
        let status = new_status("w");
        let (kill, killed) = tokio::sync::oneshot::channel();
        let slot: KillSlot = Arc::new(Mutex::new(Some(kill)));
        let worker = tokio::spawn(async move {
            tokio::select! {
                result = std::future::pending() => result,
                _ = killed => None,
            }
        });
        let watched = watch_worker(worker, &pool, "w", liveness, &status, &slot);
        tokio::pin!(watched);
        sqlx::query("UPDATE Workers SET last_seen = strftime('%s', 'now') - 100 WHERE id = 'w'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut watched)
                .await
                .is_err()
        );
        sqlx::query("UPDATE Workers SET last_seen = strftime('%s', 'now') WHERE id = 'w'")
            .execute(&pool)
            .await
            .unwrap();
        update(&status, |s| s.state = WorkerState::Running);
        let held = pool.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut watched)
                .await
                .is_err()
        );
        drop(held);
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut watched)
                .await
                .is_err()
        );
        sqlx::query("UPDATE Workers SET last_seen = strftime('%s', 'now') WHERE id = 'w'")
            .execute(&pool)
            .await
            .unwrap();
        let heartbeat_pool = pool.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                sqlx::query("UPDATE Workers SET last_seen = strftime('%s', 'now') WHERE id = 'w'")
                    .execute(&heartbeat_pool)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(3), &mut watched)
                .await
                .is_err()
        );
        heartbeat.abort();
        let _ = heartbeat.await;
        sqlx::query("UPDATE Workers SET last_seen = strftime('%s', 'now') - 100 WHERE id = 'w'")
            .execute(&pool)
            .await
            .unwrap();
        let (result, stale) = tokio::time::timeout(Duration::from_secs(2), watched)
            .await
            .unwrap();
        assert!(stale);
        assert!(result.unwrap().is_none());
        let status = status.lock().unwrap();
        assert_eq!(status.state, WorkerState::Restarting);
        assert_eq!(status.last_error.as_deref(), Some("heartbeat_stale"));
    }

    #[tokio::test]
    async fn supervisor_restarts_a_pending_stale_generation() {
        use crate::embeddings::{EmbeddingService, FakeProvider};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let tmp = tempfile::tempdir().unwrap();
        let index = Index::open_for_test(tmp.path()).await.unwrap();
        let pool =
            crate::storage::connect(&tmp.path().join("jobs.sqlite"), 4, Duration::from_secs(5))
                .await
                .unwrap();
        let cfg = Arc::new(Config::default());
        let clock: SharedClock = Arc::new(ManualClock::new(123));
        let sources = Arc::new(SourceRegistry::new(&cfg).unwrap());
        let embeddings = Arc::new(EmbeddingService::new(
            index.clone(),
            cfg.embedding.clone(),
            cfg.search.clone(),
            clock.clone(),
            Arc::new(FakeProvider::new(32)),
        ));
        embeddings.ensure_profile().await.unwrap();
        let engine = Arc::new(IngestEngine::new(
            index.clone(),
            cfg.clone(),
            sources.clone(),
            embeddings,
            clock.clone(),
        ));
        let liveness = Liveness {
            heartbeat_interval: Duration::from_millis(100),
            missed_heartbeats: 2,
            poll_interval: Duration::from_millis(100),
        };
        let system = JobSystem::start(index, pool.clone(), cfg, sources, engine, clock, liveness)
            .await
            .unwrap();
        let generation = Arc::new(AtomicUsize::new(0));
        let count = generation.clone();
        let make: Make = Arc::new(move |ctx, status, stop| {
            let number = count.fetch_add(1, Ordering::SeqCst);
            async move {
                sqlx::query("INSERT OR REPLACE INTO Workers (id, worker_type, storage_name, last_seen) VALUES ('probe', 't', 's', strftime('%s', 'now') - 100)")
                    .execute(&ctx.sub.pool).await.unwrap();
                update(&status, |s| s.state = WorkerState::Running);
                if number == 0 {
                    std::future::pending().await
                } else {
                    stop_signal(stop).await
                }
            }.boxed_local()
        });
        let status = new_status("probe");
        let slot = Arc::default();
        let (stop, rx) = watch::channel(false);
        let task = supervise(system.ctx.clone(), status.clone(), slot, rx, make);
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if generation.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        let current = status.lock().unwrap().clone();
        assert_eq!(current.restarts, 1);
        assert_eq!(current.last_error.as_deref(), Some("heartbeat_stale"));
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        system.shutdown(Duration::from_secs(2)).await;
    }

    #[tokio::test]
    async fn registration_waits_only_while_backend_would_refuse() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        backend::setup(&pool).await.unwrap();
        let hb = Duration::from_secs(300);
        assert_eq!(registration_wait(&pool, "w", hb).await, None);

        for (age, expected) in [(0, Some(280..=301)), (100, Some(150..=201)), (1000, None)] {
            sqlx::query(
                "INSERT OR REPLACE INTO Workers (id, worker_type, storage_name, last_seen) \
                 VALUES ('w', 't', 's', CAST(strftime('%s', 'now') AS INTEGER) - ?1)",
            )
            .bind(age)
            .execute(&pool)
            .await
            .unwrap();
            let wait = registration_wait(&pool, "w", hb).await.map(|d| d.as_secs());
            match expected {
                Some(range) => assert!(range.contains(&wait.unwrap()), "age {age}: {wait:?}"),
                None => assert_eq!(wait, None, "age {age}"),
            }
        }
    }
}
