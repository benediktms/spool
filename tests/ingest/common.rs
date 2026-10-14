#![allow(dead_code)]

use serde_json::json;
use spool::clock::{ManualClock, SharedClock};
use spool::config::{Config, SourceConfig};
use spool::domain::HarnessKind;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::ingest::engine::{IndexOutcome, IndexRequest, IngestEngine, IngestError};
use spool::sources::SourceRegistry;
use spool::storage::index::Index;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub const T0: i64 = 1_790_000_000_000;

pub struct Env {
    pub tmp: tempfile::TempDir,
    pub root: PathBuf,
    pub index: Index,
    pub cfg: Arc<Config>,
    pub clock: ManualClock,
    pub shared: SharedClock,
    pub provider: Arc<FakeProvider>,
    pub emb: Arc<EmbeddingService>,
    pub sources: Arc<SourceRegistry>,
    pub engine: Arc<IngestEngine>,
    pub profile_id: String,
}

impl Env {
    pub async fn new() -> Self {
        Self::with(|_| {}).await
    }

    /// Small chunks so a handful of short messages yields several chunks.
    pub async fn small_chunks() -> Self {
        Self::with(|c| {
            c.chunking.target_new_tokens = 40;
            c.chunking.max_rendered_tokens = 90;
            c.chunking.overlap_tokens = 15;
        })
        .await
    }

    pub async fn with(f: impl FnOnce(&mut Config)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(tmp.path()).unwrap().join("src");
        let index = Index::open_for_test(tmp.path()).await.unwrap();
        let mut cfg = Config::default();
        cfg.embedding.enabled = true;
        cfg.sources = Vec::new();
        for (id, kind) in [
            ("omp", HarnessKind::Omp),
            ("codex", HarnessKind::Codex),
            ("claude", HarnessKind::Claude),
        ] {
            let dir = root.join(id);
            std::fs::create_dir_all(&dir).unwrap();
            cfg.sources.push(SourceConfig {
                id: id.into(),
                kind,
                enabled: true,
                roots: vec![dir.to_string_lossy().into_owned()],
                include_subagents: false,
            });
            sqlx::query("INSERT INTO sources(id, kind, roots_json, enabled) VALUES (?,?,?,1)")
                .bind(id)
                .bind(kind.as_str())
                .bind(json!([dir]).to_string())
                .execute(&index.writer)
                .await
                .unwrap();
        }
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
        let profile_id = emb.ensure_profile().await.unwrap();
        let sources = Arc::new(SourceRegistry::new(&cfg).unwrap());
        let engine = Arc::new(IngestEngine::new(
            index.clone(),
            cfg.clone(),
            sources.clone(),
            emb.clone(),
            shared.clone(),
        ));
        Self {
            tmp,
            root,
            index,
            cfg,
            clock,
            shared,
            provider,
            emb,
            sources,
            engine,
            profile_id,
        }
    }

    /// Write `content` as a source file and register a session for it. Returns `(session_id, path)`.
    pub async fn add(&self, source: &str, name: &str, content: &str) -> (String, PathBuf) {
        let path = self.root.join(source).join(name);
        std::fs::write(&path, content).unwrap();
        let sid = format!("ss_{name}");
        sqlx::query("INSERT INTO sessions(id, source_id, native_id, discovered_at, index_state) VALUES (?,?,?,?, 'pending')")
            .bind(&sid)
            .bind(source)
            .bind(name)
            .bind(T0)
            .execute(&self.index.writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO source_files(session_id, path) VALUES (?,?)")
            .bind(&sid)
            .bind(path.to_string_lossy().into_owned())
            .execute(&self.index.writer)
            .await
            .unwrap();
        (sid, path)
    }

    pub async fn add_omp(&self, name: &str, lines: &[String]) -> (String, PathBuf) {
        self.add("omp", name, &join(lines)).await
    }

    pub async fn index(&self, sid: &str) -> Result<IndexOutcome, IngestError> {
        self.engine
            .index_session(
                IndexRequest {
                    session_id: sid.into(),
                    observed_source_revision: None,
                    fingerprint: None,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
    }

    pub async fn n(&self, sql: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.to_owned()))
            .fetch_one(&self.index.writer)
            .await
            .unwrap()
    }

    /// Hits of `term` among ACTIVE chunks only (how readers see FTS).
    pub async fn fts(&self, term: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM chunks_fts JOIN chunks c ON c.rowid = chunks_fts.rowid JOIN sessions s ON s.active_snapshot_id = c.snapshot_id \
             WHERE chunks_fts MATCH ?",
        )
        .bind(term)
        .fetch_one(&self.index.writer)
        .await
        .unwrap()
    }

    /// Hits anywhere in the FTS table (active or not): used to prove nothing leaks.
    pub async fn fts_any(&self, term: &str) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH ?")
            .bind(term)
            .fetch_one(&self.index.writer)
            .await
            .unwrap()
    }

    pub async fn active(&self, sid: &str) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT active_snapshot_id FROM sessions WHERE id=?",
        )
        .bind(sid)
        .fetch_one(&self.index.writer)
        .await
        .unwrap()
    }

    pub async fn active_chunks(&self, sid: &str) -> Vec<(String, Vec<u8>, String, i64)> {
        sqlx::query_as(
            "SELECT c.logical_id, c.input_hash, c.body, c.is_tail FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id \
             WHERE s.id = ? ORDER BY c.run_key, c.ordinal",
        )
        .bind(sid)
        .fetch_all(&self.index.writer)
        .await
        .unwrap()
    }

    pub async fn embedded_count(&self) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM embeddings WHERE profile_id = ?")
            .bind(&self.profile_id)
            .fetch_one(&self.index.writer)
            .await
            .unwrap()
    }

    pub async fn staging_count(&self) -> i64 {
        self.n("SELECT count(*) FROM snapshots WHERE state = 'staging'")
            .await
    }

    pub fn arc_flag(v: bool) -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(v))
    }
}

pub fn join(lines: &[String]) -> String {
    lines.iter().map(|l| format!("{l}\n")).collect()
}

pub fn omp_header() -> String {
    json!({"type":"session","version":3,"id":"omp-synth","timestamp":"2026-10-12T00:00:00Z","cwd":"/tmp/project","title":"Synthetic"}).to_string()
}

pub fn omp_msg(id: &str, parent: Option<&str>, role: &str, text: &str) -> String {
    json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:01Z","message":{"role":role,"content":[{"type":"text","text":text}]}}).to_string()
}

pub fn omp_tool_result(id: &str, parent: &str, payload: &str) -> String {
    json!({"type":"toolResult","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:02Z","message":{"role":"toolResult","toolCallId":"call-x","toolName":"read","content":[{"type":"text","text":payload}]}}).to_string()
}

const WORDS: &[&str] = &[
    "river", "stone", "maple", "copper", "lantern", "meadow", "harbor", "violet", "granite",
    "ember", "willow", "falcon", "prairie", "marble",
];

/// Deterministic message text with a unique token `qwz{i}` and enough words for a few dozen tokens.
pub fn text(i: usize) -> String {
    let body: Vec<&str> = (0..9)
        .map(|j| WORDS[(i * 5 + j * 3) % WORDS.len()])
        .collect();
    format!("note {i} qwz{i} {}", body.join(" "))
}

/// Linear OMP session: header then `n` alternating user/assistant messages m000.. each parented on the previous.
pub fn linear(n: usize) -> Vec<String> {
    let mut v = vec![omp_header()];
    for i in 0..n {
        let parent = (i > 0).then(|| format!("m{:03}", i - 1));
        v.push(omp_msg(
            &format!("m{i:03}"),
            parent.as_deref(),
            if i % 2 == 0 { "user" } else { "assistant" },
            &text(i),
        ));
    }
    v
}
