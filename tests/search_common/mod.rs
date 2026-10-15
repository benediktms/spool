//! Synthetic fixtures for the search tests: rows are seeded directly into the real schema.
#![allow(dead_code)]

use serde_json::Value;
use spool::clock::{ManualClock, SharedClock};
use spool::config::Config;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::ipc::AppError;
use spool::search::SearchService;
use spool::storage::index::Index;
use spool::transcript::handle::{HandleKey, KeyStore};
use spool::transcript::lease::LeaseManager;
use std::sync::Arc;

pub const T0: i64 = 1_790_000_000_000;

pub struct Env {
    pub tmp: tempfile::TempDir,
    pub index: Index,
    pub provider: Arc<FakeProvider>,
    pub emb: Arc<EmbeddingService>,
    pub svc: SearchService,
    pub leases: LeaseManager,
    pub keys: KeyStore,
    pub clock: ManualClock,
    pub cfg: Arc<Config>,
    pub profile_id: String,
}

pub struct Chunk<'a> {
    pub body: &'a str,
    /// (record_key, block_index, start, end, is_context)
    pub spans: Vec<(&'a str, i64, i64, i64, bool)>,
    pub anchor: Option<i64>,
    pub run: &'a str,
    pub ordinal: i64,
    pub embed: bool,
}

/// One message record (and its single authored block) of a snapshot.
pub struct Message<'a> {
    pub key: &'a str,
    pub seq: i64,
    pub role: &'a str,
    pub ts: Option<i64>,
    pub parent: Option<&'a str>,
    pub text: &'a str,
}

/// Session-level settings of `Env::simple_with`; the default is an unembedded interactive `omp` session at `T0`.
pub struct Seed<'a> {
    pub source: &'a str,
    pub cwd: &'a str,
    pub kind: &'a str,
    pub last_activity: i64,
    pub embed: bool,
}

impl Default for Seed<'_> {
    fn default() -> Self {
        Self {
            source: "omp",
            cwd: "/w",
            kind: "interactive",
            last_activity: T0,
            embed: false,
        }
    }
}

impl Env {
    pub async fn new() -> Self {
        Self::with(|_| {}).await
    }

    pub async fn with(f: impl FnOnce(&mut Config)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let index = Index::open_for_test(tmp.path()).await.unwrap();
        let mut cfg = Config::default();
        cfg.embedding.enabled = true;
        cfg.embedding.dimensions = 16;
        f(&mut cfg);
        let cfg = Arc::new(cfg);
        let clock = ManualClock::new(T0);
        let shared: SharedClock = Arc::new(clock.clone());
        let provider = Arc::new(FakeProvider::new(16));
        let emb = Arc::new(EmbeddingService::new(
            index.clone(),
            cfg.embedding.clone(),
            cfg.search.clone(),
            shared.clone(),
            provider.clone(),
        ));
        let profile_id = emb.ensure_profile().await.unwrap();
        let key_path = tmp.path().join("handle.key");
        HandleKey::create(&key_path).unwrap();
        let keys = KeyStore::load(&key_path).unwrap();
        let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), shared);
        let svc = SearchService::new(
            index.clone(),
            cfg.clone(),
            emb.clone(),
            leases.clone(),
            keys.clone(),
        );
        let env = Self {
            tmp,
            index,
            provider,
            emb,
            svc,
            leases,
            keys,
            clock,
            cfg,
            profile_id,
        };
        sqlx::query("INSERT INTO sources(id, kind, roots_json, enabled, last_scan_at, last_scan_ok) VALUES ('omp','omp','[]',1,?,1), ('codex','codex','[]',1,?,1)")
            .bind(T0 - 5000)
            .bind(T0 - 1000)
            .execute(&env.index.writer)
            .await
            .unwrap();
        env
    }

    pub async fn search(&self, args: Value) -> Result<Value, AppError> {
        self.svc.call(args).await
    }

    pub async fn ok(&self, args: Value) -> Value {
        self.svc
            .call(args)
            .await
            .unwrap_or_else(|e| panic!("search failed: {e}"))
    }

    pub async fn session(&self, id: &str, source: &str, cwd: &str, kind: &str, last_activity: i64) {
        sqlx::query(
            "INSERT INTO sessions(id, source_id, native_id, session_kind, cwd, native_title, last_activity_at, created_at, index_state, discovered_at) \
             VALUES (?,?,?,?,?,?,?,?, 'indexed', 0)",
        )
        .bind(id)
        .bind(source)
        .bind(id)
        .bind(kind)
        .bind(cwd)
        .bind(format!("title {id}"))
        .bind(last_activity)
        .bind(last_activity - 1000)
        .execute(&self.index.writer)
        .await
        .unwrap();
    }

    pub async fn snapshot(&self, session: &str, snap: &str, state: &str) {
        sqlx::query(
            "INSERT INTO snapshots(id, session_id, kind, state, source_revision, fingerprint, generation, prefix_end, created_at, published_at, retained_bytes) \
             VALUES (?,?, 'indexed', ?, 'r', 'f', 1, 0, 0, 0, 100)",
        )
        .bind(snap)
        .bind(session)
        .bind(state)
        .execute(&self.index.writer)
        .await
        .unwrap();
    }

    pub async fn activate(&self, session: &str, snap: &str) {
        sqlx::query("UPDATE sessions SET active_snapshot_id = ? WHERE id = ?")
            .bind(snap)
            .bind(session)
            .execute(&self.index.writer)
            .await
            .unwrap();
        sqlx::query("UPDATE app_meta SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='index_epoch'").execute(&self.index.writer).await.unwrap();
    }

    pub async fn exclude(&self, session: &str) {
        sqlx::query("UPDATE sessions SET excluded = 1 WHERE id = ?")
            .bind(session)
            .execute(&self.index.writer)
            .await
            .unwrap();
    }

    pub async fn message(&self, snap: &str, m: Message<'_>) {
        let Message {
            key,
            seq,
            role,
            ts,
            parent,
            text,
        } = m;
        sqlx::query("INSERT INTO records(snapshot_id, key, ord, parent_key, kind, ts, offset, len, digest) VALUES (?,?,?,?, 'message', ?, 0, 10, zeroblob(32))")
            .bind(snap)
            .bind(key)
            .bind(seq)
            .bind(parent)
            .bind(ts)
            .execute(&self.index.writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO messages(snapshot_id, record_key, seq, role, ts, parent_message_key) VALUES (?,?,?,?,?,?)")
            .bind(snap)
            .bind(key)
            .bind(seq)
            .bind(role)
            .bind(ts)
            .bind(parent)
            .execute(&self.index.writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO blocks(snapshot_id, record_key, idx, origin, text, text_digest) VALUES (?,?,0,'authored',?, zeroblob(32))")
            .bind(snap)
            .bind(key)
            .bind(text)
            .execute(&self.index.writer)
            .await
            .unwrap();
    }

    /// Insert a chunk (FTS rows follow by trigger) and optionally its vector for the active profile.
    pub async fn chunk(&self, session: &str, snap: &str, logical: &str, c: Chunk<'_>) -> i64 {
        let hash = spool::domain::digest(c.body.as_bytes());
        let rowid = sqlx::query(
            "INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, time_anchor, rendered_tokens, is_tail) VALUES (?,?,?,?,?,?,?,?,10,0)",
        )
        .bind(logical)
        .bind(snap)
        .bind(session)
        .bind(c.run)
        .bind(c.ordinal)
        .bind(c.body)
        .bind(hash.to_vec())
        .bind(c.anchor)
        .execute(&self.index.writer)
        .await
        .unwrap()
        .last_insert_rowid();
        for (i, (rk, b, s, e, ctx)) in c.spans.iter().enumerate() {
            sqlx::query("INSERT INTO chunk_spans(chunk_rowid, seq, record_key, block_index, start_byte, end_byte, is_context) VALUES (?,?,?,?,?,?,?)")
                .bind(rowid)
                .bind(i as i64)
                .bind(rk)
                .bind(b)
                .bind(s)
                .bind(e)
                .bind(i64::from(*ctx))
                .execute(&self.index.writer)
                .await
                .unwrap();
        }
        if c.embed {
            self.embed(&self.profile_id.clone(), c.body, &hash).await;
        }
        rowid
    }

    pub async fn embed(&self, profile: &str, text_for_vector: &str, hash: &[u8; 32]) {
        self.embed_vec(profile, &self.provider.vector_for(text_for_vector), hash)
            .await;
    }

    pub async fn embed_vec(&self, profile: &str, v: &[f32], hash: &[u8; 32]) {
        let blob: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
        sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?,?,?,?,0) ON CONFLICT DO NOTHING")
            .bind(profile)
            .bind(hash.to_vec())
            .bind(v.len() as i64)
            .bind(blob)
            .execute(&self.index.writer)
            .await
            .unwrap();
        sqlx::query("UPDATE app_meta SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT) WHERE key='index_epoch'").execute(&self.index.writer).await.unwrap();
    }

    /// One interactive session with a linear conversation: for each `(text, ts)` one user message record
    /// (record key `m{n}`) and one chunk whose body is `USER: text` anchored on that block, embedded.
    pub async fn simple(&self, id: &str, source: &str, cwd: &str, texts: &[(&str, Option<i64>)]) {
        let seed = Seed {
            source,
            cwd,
            embed: true,
            ..Seed::default()
        };
        self.simple_with(id, &seed, texts).await
    }

    pub async fn simple_with(&self, id: &str, seed: &Seed<'_>, texts: &[(&str, Option<i64>)]) {
        let Seed {
            source,
            cwd,
            kind,
            last_activity,
            embed,
        } = *seed;
        self.session(id, source, cwd, kind, last_activity).await;
        let snap = format!("sn_{id}");
        self.snapshot(id, &snap, "published").await;
        let mut parent: Option<String> = None;
        for (n, (text, ts)) in texts.iter().enumerate() {
            let key = format!("m{n}");
            self.message(
                &snap,
                Message {
                    key: &key,
                    seq: n as i64,
                    role: "user",
                    ts: *ts,
                    parent: parent.as_deref(),
                    text,
                },
            )
            .await;
            let body = format!("USER: {text}");
            self.chunk(
                id,
                &snap,
                &format!("{id}-c{n:04}"),
                Chunk {
                    body: &body,
                    spans: vec![(&key, 0, 0, text.len() as i64, false)],
                    anchor: *ts,
                    run: "run0",
                    ordinal: n as i64,
                    embed,
                },
            )
            .await;
            parent = Some(key);
        }
        self.activate(id, &snap).await;
    }
}

pub fn ids(v: &Value) -> Vec<String> {
    v["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["session_id"].as_str().unwrap().to_string())
        .collect()
}

pub fn code(e: Result<Value, AppError>) -> String {
    e.expect_err("expected an error").code
}
