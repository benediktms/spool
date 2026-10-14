#![allow(dead_code)]
//! Shared environment for discovery/jobs/control tests: synthetic OMP/Claude files, a temp
//! `index.sqlite`/`jobs.sqlite`, the deterministic fake provider and a manual clock. No network.

use serde_json::json;
use spool::clock::{ManualClock, SharedClock};
use spool::config::{Config, SourceConfig};
use spool::domain::HarnessKind;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::ingest::engine::IngestEngine;
use spool::jobs::JobSystem;
use spool::jobs::backend::{self, Liveness};
use spool::jobs::scheduler::Submitter;
use spool::sources::SourceRegistry;
use spool::storage::catalog;
use spool::storage::index::Index;
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 2026-09-22; deliberately not "now": every time-dependent rule runs on the injected clock.
pub const T0: i64 = 1_790_000_000_000;
pub const DAY: i64 = 86_400_000;

pub const FAST: Liveness = Liveness {
    heartbeat_interval: Duration::from_secs(1),
    missed_heartbeats: 2,
    poll_interval: Duration::from_millis(100),
};

pub struct Env {
    pub tmp: tempfile::TempDir,
    pub root: PathBuf,
    pub dir: PathBuf,
    pub index: Index,
    pub jobs_pool: SqlitePool,
    pub cfg: Arc<Config>,
    pub clock: ManualClock,
    pub shared: SharedClock,
    pub provider: Arc<FakeProvider>,
    pub emb: Arc<EmbeddingService>,
    pub sources: Arc<SourceRegistry>,
    pub engine: Arc<IngestEngine>,
}

impl Env {
    pub async fn new() -> Self {
        Self::with(|_| {}).await
    }

    pub async fn with(f: impl FnOnce(&mut Config)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(tmp.path()).unwrap();
        let dir = root.join("omp");
        std::fs::create_dir_all(&dir).unwrap();
        let index = Index::open_for_test(&root).await.unwrap();
        let jobs_pool =
            spool::storage::connect(&root.join("jobs.sqlite"), 4, Duration::from_secs(5))
                .await
                .unwrap();
        backend::setup(&jobs_pool).await.unwrap();
        let mut cfg = Config::default();
        cfg.sources = vec![SourceConfig {
            id: "omp".into(),
            kind: HarnessKind::Omp,
            enabled: true,
            roots: vec![dir.to_string_lossy().into_owned()],
            include_subagents: false,
        }];
        cfg.discovery.interval_seconds = 3600; // tests drive discovery explicitly unless they say otherwise
        f(&mut cfg);
        let cfg = Arc::new(cfg);
        let clock = ManualClock::new(T0);
        let shared: SharedClock = Arc::new(clock.clone());
        let provider = Arc::new(FakeProvider::new(32));
        let emb = Arc::new(EmbeddingService::new(
            index.clone(),
            cfg.embedding.clone(),
            cfg.search.clone(),
            shared.clone(),
            provider.clone(),
        ));
        emb.ensure_profile().await.unwrap();
        let sources = Arc::new(SourceRegistry::new(&cfg).unwrap());
        let engine = Arc::new(IngestEngine::new(
            index.clone(),
            cfg.clone(),
            sources.clone(),
            emb.clone(),
            shared.clone(),
        ));
        catalog::sync_sources(&index, &sources.entries())
            .await
            .unwrap();
        Self {
            tmp,
            root,
            dir,
            index,
            jobs_pool,
            cfg,
            clock,
            shared,
            provider,
            emb,
            sources,
            engine,
        }
    }

    pub fn submitter(&self) -> Submitter {
        Submitter {
            index: self.index.clone(),
            pool: self.jobs_pool.clone(),
            cfg: self.cfg.clone(),
            liveness: FAST,
            clock: self.shared.clone(),
        }
    }

    pub async fn start(&self) -> Arc<JobSystem> {
        JobSystem::start(
            self.index.clone(),
            self.jobs_pool.clone(),
            self.cfg.clone(),
            self.sources.clone(),
            self.engine.clone(),
            self.shared.clone(),
            FAST,
        )
        .await
        .unwrap()
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Write a synthetic OMP v3 session with the given native id and user/assistant texts; set mtime.
    pub fn omp(&self, name: &str, native_id: &str, texts: &[&str], mtime_ms: i64) -> PathBuf {
        write_omp(&self.dir, name, native_id, texts, mtime_ms)
    }

    pub async fn session_by_native(&self, native: &str) -> Option<catalog::SessionRow> {
        let id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM sessions WHERE native_id = ? ORDER BY discriminator LIMIT 1",
        )
        .bind(native)
        .fetch_optional(&self.index.reader)
        .await
        .unwrap();
        match id {
            Some(id) => catalog::session(&self.index, &id).await.unwrap(),
            None => None,
        }
    }

    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
            .fetch_one(&self.index.reader)
            .await
            .unwrap()
    }

    pub async fn jobs_count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
            .fetch_one(&self.jobs_pool)
            .await
            .unwrap()
    }

    pub async fn state_of(&self, native: &str) -> String {
        self.session_by_native(native)
            .await
            .map(|s| s.index_state)
            .unwrap_or_else(|| "absent".into())
    }

    pub async fn wait_state(&self, native: &str, want: &str, secs: u64) {
        let end = std::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let s = self.state_of(native).await;
            if s == want {
                return;
            }
            assert!(
                std::time::Instant::now() < end,
                "session {native} stuck in state {s}, wanted {want}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

pub fn system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

pub fn set_mtime(path: &Path, ms: i64) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(system_time(ms)).unwrap();
}

pub fn omp_body(native_id: &str, texts: &[&str]) -> String {
    omp_body_at(native_id, texts, T0)
}

/// ISO-8601 UTC for unix milliseconds (civil-from-days, no date dependency).
pub fn iso(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Header says the session started long ago; messages carry `ts_ms` (the file's real activity).
pub fn omp_body_at(native_id: &str, texts: &[&str], ts_ms: i64) -> String {
    let mut s = String::new();
    s.push_str(&format!("{}\n", json!({"type":"session","version":3,"id":native_id,"timestamp":"2026-08-01T00:00:00Z","cwd":"/tmp/project","title":"Synthetic"})));
    let mut parent: Option<String> = None;
    for (i, t) in texts.iter().enumerate() {
        let id = format!("{:08x}", i + 1);
        let role = if i % 2 == 0 { "user" } else { "assistant" };
        s.push_str(&format!(
            "{}\n",
            json!({"type":"message","id":id,"parentId":parent,"timestamp":iso(ts_ms),"message":{"role":role,"content":[{"type":"text","text":t}]}})
        ));
        parent = Some(id);
    }
    s
}

pub fn write_omp(
    dir: &Path,
    name: &str,
    native_id: &str,
    texts: &[&str],
    mtime_ms: i64,
) -> PathBuf {
    let p = dir.join(name);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, omp_body_at(native_id, texts, mtime_ms)).unwrap();
    set_mtime(&p, mtime_ms);
    p
}

/// Claude saved-message file; `parent` makes it a subagent session.
pub fn write_claude(
    dir: &Path,
    name: &str,
    native_id: &str,
    parent: Option<&str>,
    text: &str,
    mtime_ms: i64,
) -> PathBuf {
    let p = dir.join(name);
    let mut rec = json!({"type":"user","uuid":format!("{native_id}-u1"),"parentUuid":null,"sessionId":native_id,"cwd":"/tmp/project","message":{"role":"user","content":[{"type":"text","text":text}]}});
    if let Some(parent) = parent {
        rec["parentSessionId"] = json!(parent);
    }
    std::fs::write(&p, format!("{rec}\n")).unwrap();
    set_mtime(&p, mtime_ms);
    p
}

pub async fn wait_until<F, Fut>(secs: u64, what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let end = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if f().await {
            return;
        }
        assert!(
            std::time::Instant::now() < end,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
