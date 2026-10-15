//! `index_status` (spec 14.3): what the daemon knows about coverage, queue, workers, provider and
//! storage. Read-only. Never returns API keys, the signing key, transcript text, full queries or
//! native credential/config payloads: only counts, ids, timestamps and content-free categories.

use super::ControlService;
use crate::daemon::BASELINE_ID;
use crate::ipc::AppError;
use crate::jobs::ops::{self, storage_err};
use crate::jobs::payload::INGEST_QUEUE;
use crate::jobs::scheduler::{file_sizes, storage_paused};
use crate::storage::catalog;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::Row;
use std::time::{Duration, Instant};

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;
/// Expensive derived figures (vector coverage, covered sessions) are cached this long so a CLI
/// `--wait` polling loop cannot turn status into a repeated full scan.
const CACHE_TTL: Duration = Duration::from_secs(10);

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub struct IndexStatusInput {
    /// Restrict the source section and list that source's sessions (paged).
    pub source: Option<String>,
    /// Describe one session (state, error category, coverage). No transcript text.
    pub session_id: Option<String>,
    /// Describe one operation.
    pub operation_id: Option<String>,
    /// Page size for operational lists (1-100, default 20).
    pub limit: Option<i64>,
    /// Opaque cursor from `next_cursor`.
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct IndexStatusOutput {
    /// ready | lexical_only | ingesting | provider_blocked | ingestion_degraded | unsupported_format | storage_error
    pub status: String,
    /// Every state that currently applies (`status` is the most important one).
    pub states: Vec<String>,
    pub daemon_version: String,
    pub baseline: String,
    pub sources: Vec<Value>,
    pub embedding: Value,
    pub vectors: Value,
    pub queue: Value,
    pub workers: Value,
    pub last_discovery: Value,
    pub parse_warnings: Value,
    pub storage: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operations: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

fn limit_of(v: Option<i64>) -> Result<i64, AppError> {
    match v {
        None => Ok(DEFAULT_LIMIT),
        Some(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
        Some(_) => Err(AppError::new(
            "INVALID_ARGUMENT",
            "limit must be between 1 and 100",
        )),
    }
}

/// Bytes in free pages of the main database file. SQLite reuses them for new data. Only an explicit
/// VACUUM returns them to the filesystem, so they stay inside the storage budget.
async fn free_bytes(pool: &sqlx::SqlitePool) -> Option<u64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT f.freelist_count * p.page_size FROM pragma_freelist_count() f, pragma_page_size() p",
    )
    .fetch_one(pool)
    .await
    .ok()
    .map(|b| b as u64)
}

impl ControlService {
    pub async fn index_status(&self, args: Value) -> Result<Value, AppError> {
        let input: IndexStatusInput =
            serde_json::from_value(if args.is_null() { json!({}) } else { args }).map_err(|e| {
                AppError::new(
                    "INVALID_ARGUMENT",
                    format!("invalid index_status arguments: {e}"),
                )
            })?;
        let limit = limit_of(input.limit)?;
        let idx = &self.deps.index;
        let now = self.deps.clock.now_ms();
        let mut out = self.overview(now).await?;

        if let Some(src) = &input.source {
            out.sources.retain(|s| s["id"] == json!(src));
            if out.sources.is_empty() {
                return Err(AppError::new("INVALID_ARGUMENT", "unknown source"));
            }
            let rows = catalog::sessions_page(idx, Some(src), input.cursor.as_deref(), limit + 1)
                .await
                .map_err(storage_err)?;
            let more = rows.len() as i64 > limit;
            let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
            out.next_cursor = if more {
                rows.last().map(|r| r.id.clone())
            } else {
                None
            };
            out.sessions = Some(rows.iter().map(session_json).collect());
        }
        if let Some(sid) = &input.session_id {
            let s = catalog::session(idx, sid)
                .await
                .map_err(storage_err)?
                .ok_or_else(|| AppError::new("SESSION_NOT_FOUND", "no such session"))?;
            let warnings: i64 =
                sqlx::query_scalar("SELECT count(*) FROM parse_warnings WHERE snapshot_id = ?")
                    .bind(&s.active_snapshot_id)
                    .fetch_one(&idx.reader)
                    .await
                    .map_err(storage_err)?;
            let mut v = session_json(&s);
            v["parse_warnings"] = json!(warnings);
            out.session = Some(v);
        }
        if let Some(op) = &input.operation_id {
            out.operation = Some(self.operation_view(op).await?);
        } else if input.session_id.is_none() && input.source.is_none() {
            let rows = ops::list(idx, input.cursor.as_deref(), limit + 1).await?;
            let more = rows.len() as i64 > limit;
            let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
            out.next_cursor = if more {
                rows.last().map(|r| r.id.clone())
            } else {
                None
            };
            out.operations = Some(rows.iter().map(|o| o.brief()).collect());
        }
        serde_json::to_value(out)
            .map_err(|_| AppError::new("STORAGE_ERROR", "status could not be serialised"))
    }

    async fn overview(&self, now: i64) -> Result<IndexStatusOutput, AppError> {
        let d = &self.deps;
        let idx = &d.index;
        // sessions by state (backfill-only catalog entries are inert and reported separately)
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT s.index_state, count(*) AS n FROM sessions s WHERE s.excluded = 0 AND s.source_status = 'present' AND s.eligibility != 'backfill' AND {} GROUP BY 1", catalog::INDEXABLE_KIND))).fetch_all(&idx.reader).await.map_err(storage_err)?;
        let count = |name: &str| {
            rows.iter()
                .find(|r| r.get::<String, _>("index_state") == name)
                .map_or(0, |r| r.get::<i64, _>("n"))
        };
        let (pending, indexed, deferred, failed, unsupported) = (
            count("pending"),
            count("indexed"),
            count("deferred"),
            count("failed"),
            count("unsupported"),
        );
        let (backfill_known, excluded, missing): (i64, i64, i64) = sqlx::query_as(
            "SELECT COALESCE(SUM(eligibility = 'backfill' AND excluded = 0), 0), COALESCE(SUM(excluded), 0), COALESCE(SUM(source_status = 'missing'), 0) FROM sessions",
        )
        .fetch_one(&idx.reader)
        .await
        .map_err(storage_err)?;

        let emb = d.engine.embeddings().status();
        let usable = crate::jobs::scheduler::provider_usable(&emb);
        let cov = self.cached_coverage().await;
        let covered = self.cached_covered_sessions(&emb.profile_id).await;
        let q = d.jobs.submitter().counts(INGEST_QUEUE).await?;
        let health = d.jobs.health();

        // sources
        let rows = catalog::sources(idx).await.map_err(storage_err)?;
        let mut sources = Vec::new();
        for r in rows {
            let per: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT s.index_state, count(*) FROM sessions s WHERE s.source_id = ? AND s.eligibility != 'backfill' AND s.source_status = 'present' AND {} GROUP BY 1", catalog::INDEXABLE_KIND))).bind(&r.id).fetch_all(&idx.reader).await.map_err(storage_err)?;
            let total: i64 =
                sqlx::query_scalar("SELECT count(*) FROM sessions WHERE source_id = ?")
                    .bind(&r.id)
                    .fetch_one(&idx.reader)
                    .await
                    .map_err(storage_err)?;
            let mut by_state = Map::new();
            for (k, v) in per {
                by_state.insert(k, json!(v));
            }
            sources.push(json!({
                "id": r.id, "kind": r.kind, "enabled": r.enabled, "include_subagents": r.include_subagents,
                "bootstrap": {
                    "days": d.cfg.discovery.bootstrap_days,
                    "started_at_ms": r.bootstrap_started_at, "cutoff_ms": r.bootstrap_cutoff,
                    "complete": r.discovery_watermark.is_some(),
                },
                "discovery_watermark_ms": r.discovery_watermark,
                "last_scan": { "at_ms": r.last_scan_at, "ok": r.last_scan_ok, "error": r.last_scan_error },
                "sessions_total": total,
                "sessions_by_state": by_state,
            }));
        }

        let mut last = Map::new();
        for r in sqlx::query("SELECT key, value FROM app_meta WHERE key LIKE 'discovery.last.%'")
            .fetch_all(&idx.reader)
            .await
            .map_err(storage_err)?
        {
            let k: String = r.get("key");
            let v: String = r.get("value");
            last.insert(
                k.trim_start_matches("discovery.last.").to_string(),
                serde_json::from_str(&v).unwrap_or(Value::Null),
            );
        }

        let warns: Vec<(String, i64)> = sqlx::query_as(
            "SELECT pw.code, count(*) AS n FROM parse_warnings pw JOIN snapshots sn ON sn.id = pw.snapshot_id WHERE sn.state = 'published' GROUP BY pw.code ORDER BY n DESC, pw.code LIMIT 10",
        )
        .fetch_all(&idx.reader)
        .await
        .map_err(storage_err)?;
        let warn_total: i64 = warns.iter().map(|w| w.1).sum();

        let (idb, iwal) = file_sizes(&idx.reader).await.unwrap_or((0, 0));
        let (jdb, jwal) = file_sizes(&d.jobs.submitter().pool).await.unwrap_or((0, 0));
        let ifree = free_bytes(&idx.reader).await;
        let jfree = free_bytes(&d.jobs.submitter().pool).await;
        let paused = catalog::app_meta_get(idx, "ingest.paused")
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        let storage_err_at: i64 = catalog::app_meta_get(idx, "ingest.storage_error_at")
            .await
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        // state flags
        let bootstrapping = sources
            .iter()
            .any(|s| s["enabled"] == json!(true) && s["bootstrap"]["complete"] == json!(false));
        let blocked_state = matches!(
            emb.state,
            crate::embeddings::EmbeddingState::AuthBlocked
                | crate::embeddings::EmbeddingState::BudgetExhausted { .. }
        ) || matches!(&emb.state, crate::embeddings::EmbeddingState::Degraded(c) if c.blocked);
        let vectors_pending = cov.as_ref().map_or(0, |c| c.pending_chunks);
        let mut states = Vec::new();
        if paused == "storage"
            || now - storage_err_at < 600_000 && storage_err_at > 0
            || storage_paused(idx, &d.cfg).await
        {
            states.push("storage_error");
        }
        if health.ingestion_degraded {
            states.push("ingestion_degraded");
        }
        if blocked_state && (vectors_pending > 0 || deferred > 0) {
            states.push("provider_blocked");
        }
        if pending > 0 || q.pending > 0 || q.running > 0 || bootstrapping {
            states.push("ingesting");
        }
        if matches!(
            emb.state,
            crate::embeddings::EmbeddingState::NotApproved
                | crate::embeddings::EmbeddingState::NoCredential
                | crate::embeddings::EmbeddingState::Disabled
        ) || (!usable && vectors_pending > 0)
        {
            states.push("lexical_only");
        }
        if unsupported > 0 {
            states.push("unsupported_format");
        }
        let status = states.first().copied().unwrap_or("ready").to_string();
        if states.is_empty() {
            states.push("ready");
        }

        Ok(IndexStatusOutput {
            status,
            states: states.into_iter().map(String::from).collect(),
            daemon_version: env!("CARGO_PKG_VERSION").into(),
            baseline: BASELINE_ID.into(),
            sources,
            embedding: serde_json::to_value(&emb).unwrap_or(Value::Null),
            vectors: cov.map_or(
                json!({"profile_id": emb.profile_id, "available": false}),
                |c| serde_json::to_value(c).unwrap_or(Value::Null),
            ),
            queue: json!({
                "queued": q.pending, "running": q.running, "retrying": q.retryable_failed,
                "deferred": deferred, "permanently_failed": failed + unsupported,
                "covered": covered, "indexed": indexed, "pending_sessions": pending,
                "backfill_known": backfill_known, "excluded": excluded, "missing": missing,
                "max_queued_sessions": d.cfg.ingestion.max_queued_sessions,
            }),
            workers: serde_json::to_value(&health).unwrap_or(Value::Null),
            last_discovery: Value::Object(last),
            parse_warnings: json!({ "total": warn_total, "by_code": warns.iter().map(|(c, n)| json!({"code": c, "count": n})).collect::<Vec<_>>() }),
            storage: json!({
                "index_db_bytes": idb, "index_db_free_bytes": ifree, "index_wal_bytes": iwal,
                "jobs_db_bytes": jdb, "jobs_db_free_bytes": jfree, "jobs_wal_bytes": jwal,
                "warning_bytes": d.cfg.storage.warning_bytes, "pause_ingestion_bytes": d.cfg.storage.pause_ingestion_bytes,
                "ingestion_paused": paused == "storage",
                "approximate": true,
            }),
            operations: None,
            operation: None,
            session: None,
            sessions: None,
            next_cursor: None,
        })
    }

    async fn cached_coverage(&self) -> Option<crate::embeddings::VectorCoverage> {
        {
            let c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((at, v)) = &c.coverage {
                if at.elapsed() < CACHE_TTL {
                    return v.clone();
                }
            }
        }
        let v = self.deps.engine.embeddings().vector_coverage().await.ok();
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .coverage = Some((Instant::now(), v.clone()));
        v
    }

    /// Indexed sessions whose active chunks all have vectors for the active profile.
    async fn cached_covered_sessions(&self, profile: &str) -> i64 {
        {
            let c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((at, n)) = c.covered {
                if at.elapsed() < CACHE_TTL {
                    return n;
                }
            }
        }
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sessions s WHERE s.index_state = 'indexed' AND s.excluded = 0 AND s.active_snapshot_id IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM chunks c WHERE c.snapshot_id = s.active_snapshot_id AND NOT EXISTS \
             (SELECT 1 FROM embeddings e WHERE e.profile_id = ?1 AND e.input_hash = c.input_hash))",
        )
        .bind(profile)
        .fetch_one(&self.deps.index.reader)
        .await
        .unwrap_or(0);
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).covered = Some((Instant::now(), n));
        n
    }
}

pub fn session_json(s: &catalog::SessionRow) -> Value {
    json!({
        "session_id": s.id, "source_id": s.source_id, "kind": s.session_kind, "index_state": s.index_state,
        "index_error": s.index_error, "eligibility": s.eligibility, "excluded": s.excluded,
        "source_status": s.source_status, "coverage_status": s.coverage_status,
        "has_published_snapshot": s.active_snapshot_id.is_some(), "last_activity_ms": s.last_activity_at,
        "attempts": s.index_attempts, "defer_until_ms": s.defer_until, "operation_id": s.operation_id,
    })
}
