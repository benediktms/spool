//! Control plane: the `index_status` MCP tool and the CLI administration dispatcher (spec 14.3,
//! 14.4, 16.2). Administration is reachable only through `Request::Control` over the owner-only
//! socket; the MCP surface has no way to start indexing, purge, rebuild or rotate keys.

pub mod status;

use crate::clock::SharedClock;
use crate::config::{Config, Paths};
use crate::discovery::{Mode, Selection};
use crate::ingest::engine::IngestEngine;
use crate::ipc::AppError;
use crate::jobs::ops::{self, storage_err};
use crate::jobs::payload::{DiscoverySelection, MaintenanceScope};
use crate::jobs::{JobSystem, Submission};
use crate::sources::{self, SourceRegistry};
use crate::storage::catalog;
use crate::storage::index::Index;
use crate::transcript::handle::KeyStore;
use crate::transcript::lease::LeaseManager;
use rmcp::model::{JsonObject, Tool, ToolAnnotations};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::Row;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub use status::{IndexStatusInput, IndexStatusOutput};

/// Sessions estimated exactly per dry run; the rest is extrapolated and flagged.
const ESTIMATE_SAMPLE: usize = 200;
const DEFAULT_WAIT_MS: u64 = 30_000;
const MAX_WAIT_MS: u64 = 600_000;
const WAIT_POLL: Duration = Duration::from_millis(250);

pub struct ControlDeps {
    pub index: Index,
    pub jobs: Arc<JobSystem>,
    pub cfg: Arc<Config>,
    pub sources: Arc<SourceRegistry>,
    pub engine: Arc<IngestEngine>,
    pub leases: LeaseManager,
    pub keys: KeyStore,
    pub paths: Paths,
    pub clock: SharedClock,
}

#[derive(Default)]
pub(crate) struct StatusCache {
    pub coverage: Option<(Instant, Option<crate::embeddings::VectorCoverage>)>,
    pub covered: Option<(Instant, i64)>,
}

pub struct ControlService {
    pub(crate) deps: ControlDeps,
    pub(crate) cache: Mutex<StatusCache>,
}

fn args<T: DeserializeOwned>(cmd: &str, v: Value) -> Result<T, AppError> {
    serde_json::from_value(if v.is_null() { json!({}) } else { v }).map_err(|e| {
        AppError::new(
            "INVALID_ARGUMENT",
            format!("invalid arguments for {cmd}: {e}"),
        )
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexSinceArgs {
    since_ms: i64,
    source: Option<String>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    wait: bool,
    wait_timeout_ms: Option<u64>,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexAllArgs {
    source: Option<String>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    wait: bool,
    wait_timeout_ms: Option<u64>,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexSessionArgs {
    session_id: String,
    #[serde(default)]
    wait: bool,
    wait_timeout_ms: Option<u64>,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RebuildArgs {
    #[serde(default)]
    lexical_only: bool,
    #[serde(default)]
    embeddings_only: bool,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoveArgs {
    session_id: String,
    #[serde(default)]
    exclude: bool,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PurgeArgs {
    #[serde(default)]
    unreferenced: bool,
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpArgs {
    operation_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceArgs {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionsListArgs {
    limit: Option<i64>,
    source: Option<String>,
    after: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Empty {}

impl ControlService {
    pub fn new(deps: ControlDeps) -> Self {
        Self {
            deps,
            cache: Mutex::new(StatusCache::default()),
        }
    }

    /// MCP tool definition for `index_status` (the only control tool on the MCP surface).
    pub fn tool_definition() -> Tool {
        Tool::new(
            "index_status",
            "Report Spool's index state: source coverage and bootstrap scope, queue counts, vector coverage, \
             embedding provider health and token budget, worker health, last discovery, parse warnings and database sizes. \
             Distinguishes ready, lexical_only, ingesting, provider_blocked, ingestion_degraded, unsupported_format and storage_error. \
             Read-only; returns no secrets and no transcript text.",
            Arc::new(JsonObject::new()),
        )
        .with_input_schema::<IndexStatusInput>()
        .with_output_schema::<IndexStatusOutput>()
        .annotate(ToolAnnotations::new().read_only(true).idempotent(true).open_world(false).destructive(false))
    }

    /// CLI administration dispatcher. Every command is JSON in, JSON out; mutating commands take an
    /// optional `idempotency_key` (same key and parameters = same operation; different = conflict).
    pub async fn call(&self, cmd: &str, a: Value) -> Result<Value, AppError> {
        match cmd {
            "status" | "index_status" => self.index_status(a).await,
            "index_since" => self.index_since(args(cmd, a)?).await,
            "index_all" => self.index_all(args(cmd, a)?).await,
            "index_session" => self.index_session(args(cmd, a)?).await,
            "index_rebuild" => self.index_rebuild(args(cmd, a)?).await,
            "index_remove" => self.index_remove(args(cmd, a)?).await,
            "index_purge" => self.index_purge(args(cmd, a)?).await,
            "jobs_status" => {
                let a: OpArgs = args(cmd, a)?;
                self.operation_view(&a.operation_id).await
            }
            "jobs_retry" => {
                let a: OpArgs = args(cmd, a)?;
                let s = self.deps.jobs.retry_operation(&a.operation_id).await?;
                Ok(submission_json(&s, "accepted"))
            }
            "jobs_cancel" => {
                let a: OpArgs = args(cmd, a)?;
                let op = self.deps.jobs.cancel_operation(&a.operation_id).await?;
                Ok(
                    json!({ "operation_id": op.id, "state": op.state, "cancel_requested": op.cancel_requested }),
                )
            }
            "sources_list" => {
                let _: Empty = args(cmd, a)?;
                self.sources_list().await
            }
            "sources_enable" => self.sources_set(args(cmd, a)?, true).await,
            "sources_disable" => self.sources_set(args(cmd, a)?, false).await,
            "handles_rotate_key" => {
                let _: Empty = args(cmd, a)?;
                self.rotate_key().await
            }
            "sessions_list" => self.sessions_list(args(cmd, a)?).await,
            other => Err(AppError::new(
                "INVALID_ARGUMENT",
                format!("unknown command {other}"),
            )),
        }
    }

    // -----------------------------------------------------------------------------------------
    // indexing
    // -----------------------------------------------------------------------------------------

    async fn index_since(&self, a: IndexSinceArgs) -> Result<Value, AppError> {
        if a.dry_run {
            return self
                .dry_run(
                    Selection::Since(a.since_ms),
                    a.source.as_deref(),
                    json!({ "since_ms": a.since_ms }),
                )
                .await;
        }
        let sub = self
            .deps
            .jobs
            .submit_discovery(
                DiscoverySelection::Since {
                    since_ms: a.since_ms,
                },
                a.source.as_deref(),
                a.idempotency_key.as_deref(),
            )
            .await?;
        self.maybe_wait(sub, a.wait, a.wait_timeout_ms).await
    }

    /// `--all` exists only as this explicit command; nothing else ever implies it.
    async fn index_all(&self, a: IndexAllArgs) -> Result<Value, AppError> {
        if a.dry_run {
            return self
                .dry_run(Selection::All, a.source.as_deref(), json!({}))
                .await;
        }
        let sub = self
            .deps
            .jobs
            .submit_discovery(
                DiscoverySelection::All,
                a.source.as_deref(),
                a.idempotency_key.as_deref(),
            )
            .await?;
        self.maybe_wait(sub, a.wait, a.wait_timeout_ms).await
    }

    async fn index_session(&self, a: IndexSessionArgs) -> Result<Value, AppError> {
        let sub = self
            .deps
            .jobs
            .submit_index_session(&a.session_id, a.idempotency_key.as_deref())
            .await?;
        self.maybe_wait(sub, a.wait, a.wait_timeout_ms).await
    }

    async fn index_rebuild(&self, a: RebuildArgs) -> Result<Value, AppError> {
        if a.lexical_only && a.embeddings_only {
            return Err(AppError::new(
                "INVALID_ARGUMENT",
                "--lexical-only and --embeddings-only are mutually exclusive",
            ));
        }
        let (lexical, embeddings) = (!a.embeddings_only, !a.lexical_only);
        let sub = self
            .deps
            .jobs
            .submit_rebuild(lexical, embeddings, a.idempotency_key.as_deref())
            .await?;
        Ok(submission_json(&sub, "accepted"))
    }

    async fn index_purge(&self, a: PurgeArgs) -> Result<Value, AppError> {
        if !a.unreferenced {
            return Err(AppError::new(
                "INVALID_ARGUMENT",
                "purge requires --unreferenced",
            ));
        }
        let sub = self
            .deps
            .jobs
            .submit_maintenance(
                MaintenanceScope::PurgeUnreferenced,
                a.idempotency_key.as_deref(),
            )
            .await?;
        Ok(submission_json(&sub, "accepted"))
    }

    /// Exclude a session: persist the exclusion (survives rediscovery and restart), revoke leases
    /// that pin it, delete derived data. The source file is never touched.
    async fn index_remove(&self, a: RemoveArgs) -> Result<Value, AppError> {
        if !a.exclude {
            return Err(AppError::new(
                "INVALID_ARGUMENT",
                "only `--exclude` removal is supported; the source is never modified",
            ));
        }
        let d = &self.deps;
        let now = d.clock.now_ms();
        let (op, created) = ops::begin(
            &d.index,
            "index_remove",
            &json!({ "session_id": a.session_id }),
            a.idempotency_key.as_deref(),
            now,
        )
        .await?;
        if !created && op.state == "done" {
            return Ok(
                json!({ "operation_id": op.id, "session_id": a.session_id, "excluded": true, "created": false }),
            );
        }
        if !catalog::exclude_session(&d.index, &a.session_id, "manual exclusion", now)
            .await
            .map_err(storage_err)?
        {
            ops::set_state(&d.index, &op.id, "failed", Some("session_not_found"), now).await?;
            return Err(AppError::new("SESSION_NOT_FOUND", "no such session"));
        }
        d.leases.revoke_session(&a.session_id).await?;
        catalog::remove_derived(&d.index, &a.session_id, now)
            .await
            .map_err(storage_err)?;
        ops::set_state(&d.index, &op.id, "done", None, now).await?;
        Ok(
            json!({ "operation_id": op.id, "session_id": a.session_id, "excluded": true, "derived_data_removed": true, "created": created }),
        )
    }

    /// Wait semantics: poll status until a terminal or blocked state, bounded.
    async fn maybe_wait(
        &self,
        sub: Submission,
        wait: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Value, AppError> {
        if !wait {
            return Ok(submission_json(&sub, "accepted"));
        }
        let budget = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_WAIT_MS).min(MAX_WAIT_MS));
        let started = Instant::now();
        loop {
            let v = self.operation_view(&sub.operation_id).await?;
            let state = v["state"].as_str().unwrap_or("").to_string();
            let terminal = matches!(state.as_str(), "done" | "failed" | "cancelled" | "blocked");
            if terminal || started.elapsed() >= budget {
                let mut v = v;
                v["task_id"] = json!(sub.task_id);
                v["created"] = json!(sub.created);
                v["waited_ms"] = json!(started.elapsed().as_millis() as u64);
                v["timed_out"] = json!(!terminal);
                return Ok(v);
            }
            tokio::time::sleep(WAIT_POLL).await;
        }
    }

    /// `jobs status`: operation state with session counts and the Apalis task's status.
    pub async fn operation_view(&self, id: &str) -> Result<Value, AppError> {
        let d = &self.deps;
        let op = ops::get(&d.index, id)
            .await?
            .ok_or_else(|| AppError::new("OPERATION_NOT_FOUND", "no such operation"))?;
        let rows = sqlx::query(
            "SELECT index_state, count(*) AS n FROM sessions WHERE operation_id = ? GROUP BY 1",
        )
        .bind(&op.id)
        .fetch_all(&d.index.reader)
        .await
        .map_err(storage_err)?;
        let mut sessions = serde_json::Map::new();
        let (mut open_pending, mut open_deferred) = (0i64, 0i64);
        for r in rows {
            let (k, n): (String, i64) = (r.get("index_state"), r.get("n"));
            match k.as_str() {
                "pending" => open_pending = n,
                "deferred" => open_deferred = n,
                _ => {}
            }
            sessions.insert(k, json!(n));
        }
        let task = match &op.task_id {
            Some(t) => d
                .jobs
                .submitter()
                .task_state(t)
                .await?
                .map(|(s, a, m)| json!({ "status": s, "attempts": a, "max_attempts": m })),
            None => None,
        };
        let enumerated = op
            .progress
            .get("enumeration_done")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let running_task = task.as_ref().is_some_and(|t| t["status"] == "Running");
        let state = match op.state.as_str() {
            "accepted" if running_task => "running",
            "accepted" => "queued",
            "running" if enumerated && open_pending == 0 && open_deferred > 0 => "blocked",
            other => other,
        };
        let mut progress = op.progress.clone();
        if !progress.is_object() {
            progress = json!({});
        }
        progress["sessions"] = Value::Object(sessions);
        Ok(json!({
            "operation_id": op.id, "kind": op.kind, "state": state, "task_id": op.task_id, "task": task,
            "progress": progress, "error": op.error, "cancel_requested": op.cancel_requested,
            "created_at_ms": op.created_at, "updated_at_ms": op.updated_at,
        }))
    }

    /// `--dry-run`: enumerate candidates (inert catalog entries, nothing queued, no provider call)
    /// and estimate locally. At most `ESTIMATE_SAMPLE` sessions are estimated exactly; the total is
    /// extrapolated from the sample and flagged. Also reports exclusions and policy skips.
    async fn dry_run(
        &self,
        sel: Selection,
        source: Option<&str>,
        params: Value,
    ) -> Result<Value, AppError> {
        let d = &self.deps;
        let ids = match source {
            Some(s) if !d.sources.is_enabled(s) => {
                return Err(AppError::new(
                    "INVALID_ARGUMENT",
                    "unknown or disabled source",
                ));
            }
            Some(s) => vec![s.to_string()],
            None => d.sources.enabled_ids(),
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let (mut candidates, mut excluded, mut subagent, mut skipped_old, mut incomplete) =
            (0usize, 0usize, 0usize, 0usize, false);
        let mut sample: Vec<String> = Vec::new();
        let mut per_source = Vec::new();
        for id in &ids {
            let rep = d
                .jobs
                .ctx()
                .discovery
                .run_explicit(
                    id,
                    sel.clone(),
                    Mode::CatalogOnly,
                    None,
                    usize::MAX,
                    cancel.clone(),
                )
                .await
                .map_err(|e| {
                    AppError::new("SOURCE_UNAVAILABLE", format!("enumeration failed: {e}"))
                })?;
            candidates += rep.queued;
            excluded += rep.skipped_excluded;
            subagent += rep.skipped_subagent;
            skipped_old += rep.skipped_old;
            incomplete |= !rep.finished;
            for c in rep.candidates.iter() {
                if sample.len() < ESTIMATE_SAMPLE {
                    sample.push(c.clone());
                }
            }
            per_source.push(json!({ "source": id, "candidate_sessions": rep.queued, "scanned": rep.scanned, "excluded": rep.skipped_excluded, "skipped_subagent": rep.skipped_subagent, "scan_complete": rep.finished }));
        }
        let (mut tokens_total, mut tokens_uncached, mut chunks, mut ok, mut failed) =
            (0u64, 0u64, 0u64, 0usize, 0usize);
        for sid in &sample {
            match d.engine.estimate(sid).await {
                Ok(e) => {
                    tokens_total += e.tokens_total as u64;
                    tokens_uncached += e.tokens_uncached as u64;
                    chunks += e.chunks as u64;
                    ok += 1;
                }
                Err(_) => failed += 1,
            }
        }
        let exact = candidates <= ESTIMATE_SAMPLE && failed == 0;
        let scale = if ok > 0 && candidates > ok {
            candidates as f64 / ok as f64
        } else {
            1.0
        };
        let mut out = json!({
            "dry_run": true,
            "selection": match sel { Selection::Since(_) => "since", Selection::All => "all", Selection::Auto => "auto" },
            "candidate_sessions": candidates,
            "excluded": excluded,
            "skipped_subagent": subagent,
            "skipped_older": skipped_old,
            "estimated_tokens": (tokens_uncached as f64 * scale) as u64,
            "estimated_tokens_total": (tokens_total as f64 * scale) as u64,
            "estimated_chunks": (chunks as f64 * scale) as u64,
            "sampled_sessions": ok,
            "estimate_errors": failed,
            "estimate_exact": exact,
            "enumeration_complete": !incomplete,
            "per_source": per_source,
            "provider_calls": 0,
            "note": "tokens are a local cl100k estimate of chunks not yet embedded for the active profile; no provider call was made",
        });
        if let (Value::Object(o), Value::Object(p)) = (&mut out, params) {
            o.extend(p);
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------------
    // sources, keys, sessions
    // -----------------------------------------------------------------------------------------

    async fn sources_list(&self) -> Result<Value, AppError> {
        let d = &self.deps;
        let rows = catalog::sources(&d.index).await.map_err(storage_err)?;
        let mut out = Vec::new();
        for e in d.sources.entries() {
            let row = rows.iter().find(|r| r.id == e.id);
            let sessions: i64 =
                sqlx::query_scalar("SELECT count(*) FROM sessions WHERE source_id = ?")
                    .bind(&e.id)
                    .fetch_one(&d.index.reader)
                    .await
                    .map_err(storage_err)?;
            out.push(json!({
                "id": e.id, "kind": e.kind.as_str(), "enabled": e.enabled, "include_subagents": e.include_subagents,
                "roots": e.roots.iter().map(|p| p.to_string_lossy()).collect::<Vec<_>>(),
                "bootstrap_started_at_ms": row.and_then(|r| r.bootstrap_started_at),
                "bootstrap_cutoff_ms": row.and_then(|r| r.bootstrap_cutoff),
                "discovery_watermark_ms": row.and_then(|r| r.discovery_watermark),
                "last_scan_at_ms": row.and_then(|r| r.last_scan_at),
                "last_scan_ok": row.and_then(|r| r.last_scan_ok),
                "last_scan_error": row.and_then(|r| r.last_scan_error.clone()),
                "sessions": sessions,
            }));
        }
        Ok(json!({ "sources": out }))
    }

    /// Edit `enabled` in config.toml (comments preserved), reload the registry, mirror to the
    /// `sources` table, and start discovery for a newly enabled source.
    async fn sources_set(&self, a: SourceArgs, enabled: bool) -> Result<Value, AppError> {
        let d = &self.deps;
        let Some(entry) = d.sources.entry(&a.id) else {
            return Err(AppError::new("INVALID_ARGUMENT", "unknown source id"));
        };
        let was = entry.enabled;
        if was != enabled {
            sources::set_enabled_in_file(&d.paths.config_file, &a.id, enabled).map_err(|_| {
                AppError::new(
                    "INVALID_ARGUMENT",
                    "config.toml could not be edited; fix it and retry",
                )
            })?;
            let cfg = Config::load(&d.paths).map_err(|_| {
                AppError::new("INVALID_ARGUMENT", "config.toml is invalid after the edit")
            })?;
            d.sources.reload(&cfg).map_err(|_| {
                AppError::new("STORAGE_ERROR", "source registry could not be reloaded")
            })?;
            catalog::sync_sources(&d.index, &d.sources.entries())
                .await
                .map_err(storage_err)?;
            if enabled {
                d.jobs.submit_auto_discovery(&a.id).await?;
            }
        }
        Ok(json!({ "id": a.id, "enabled": enabled, "changed": was != enabled }))
    }

    async fn rotate_key(&self) -> Result<Value, AppError> {
        let keys = self.deps.keys.clone();
        let epoch = tokio::task::spawn_blocking(move || keys.rotate())
            .await
            .map_err(|_| AppError::new("STORAGE_ERROR", "key rotation was interrupted"))?
            .map_err(|_| AppError::new("STORAGE_ERROR", "the signing key could not be rotated"))?;
        catalog::app_meta_set(&self.deps.index, "signing_key_epoch", &epoch.to_string())
            .await
            .map_err(storage_err)?;
        Ok(
            json!({ "rotated": true, "epoch": epoch, "note": "handles and cursors issued before this point are no longer valid" }),
        )
    }

    async fn sessions_list(&self, a: SessionsListArgs) -> Result<Value, AppError> {
        let limit = a.limit.unwrap_or(50);
        if !(1..=200).contains(&limit) {
            return Err(AppError::new(
                "INVALID_ARGUMENT",
                "limit must be between 1 and 200",
            ));
        }
        let rows = catalog::sessions_page(
            &self.deps.index,
            a.source.as_deref(),
            a.after.as_deref(),
            limit + 1,
        )
        .await
        .map_err(storage_err)?;
        let more = rows.len() as i64 > limit;
        let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
        Ok(
            json!({ "sessions": rows.iter().map(status::session_json).collect::<Vec<_>>(), "next_cursor": if more { rows.last().map(|r| r.id.clone()) } else { None } }),
        )
    }
}

fn submission_json(s: &Submission, state: &str) -> Value {
    json!({ "operation_id": s.operation_id, "task_id": s.task_id, "state": state, "created": s.created })
}
