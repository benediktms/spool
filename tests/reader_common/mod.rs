//! Shared harness for the `read_session` tests: a real index + real ingest engine (stager and
//! publisher) + real OMP adapter over synthetic files. No network; the fake embedding provider only
//! exists so the ingest engine can be constructed and so tests can prove the reader never calls it.
#![allow(dead_code)]

use serde_json::{Value, json};
use spool::clock::{ManualClock, SharedClock};
use spool::config::{Config, SourceConfig};
use spool::domain::HarnessKind;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::ingest::engine::{IndexOutcome, IndexRequest, IngestEngine, IngestError};
use spool::ipc::AppError;
use spool::mcp::budget::tool_result_bytes;
use spool::sources::SourceRegistry;
use spool::storage::index::Index;
use spool::transcript::ReadService;
use spool::transcript::handle::{HandleKey, KeyStore};
use spool::transcript::lease::LeaseManager;
use spool::transcript::refs::{Anchor, MatchRef, MessageRef, SpanRef, short_digest};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};

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
    pub keys: KeyStore,
    pub leases: LeaseManager,
    pub svc: ReadService,
}

fn schema(name: &str) -> &'static jsonschema::Validator {
    static CELLS: OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, &'static jsonschema::Validator>>,
    > = OnceLock::new();
    let m = CELLS.get_or_init(Default::default);
    let mut g = m.lock().unwrap();
    g.entry(name.to_string()).or_insert_with(|| {
        let p = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("schemas/read_session")
            .join(format!("{name}.schema.json"));
        let v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap_or_else(|e| {
            panic!(
                "{}: {e} (run UPDATE_SCHEMAS=1 cargo test --test reader_schemas)",
                p.display()
            )
        }))
        .unwrap();
        Box::leak(Box::new(
            jsonschema::validator_for(&v).expect("schema compiles"),
        ))
    })
}

/// Every success response must validate against `output.schema.json`.
pub fn assert_output_valid(v: &Value) {
    let errs: Vec<String> = schema("output")
        .iter_errors(v)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    assert!(errs.is_empty(), "output schema violations: {errs:?}");
}

/// Every error payload must validate against `error.schema.json`.
pub fn assert_error_valid(e: &AppError) {
    let v = e.to_tool_json();
    let errs: Vec<String> = schema("error")
        .iter_errors(&v)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    assert!(errs.is_empty(), "error schema violations: {errs:?} for {v}");
}

impl Env {
    pub async fn new() -> Self {
        Self::with(|_| {}).await
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
        emb.ensure_profile().await.unwrap();
        let sources = Arc::new(SourceRegistry::new(&cfg).unwrap());
        let engine = Arc::new(IngestEngine::new(
            index.clone(),
            cfg.clone(),
            sources.clone(),
            emb.clone(),
            shared.clone(),
        ));
        let key_path = tmp.path().join("handle.key");
        HandleKey::create(&key_path).unwrap();
        let keys = KeyStore::load(&key_path).unwrap();
        let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), shared.clone());
        let svc = ReadService::new(
            index.clone(),
            cfg.clone(),
            leases.clone(),
            keys.clone(),
            sources.clone(),
            engine.source_read_stager(),
            shared.clone(),
        );
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
            keys,
            leases,
            svc,
        }
    }

    /// Simulate a daemon restart that keeps the signing key and the index file: every service is
    /// rebuilt from the files on disk.
    pub async fn restart(&mut self) {
        self.index = Index::open_for_test(self.tmp.path()).await.unwrap();
        self.keys = KeyStore::load(&self.tmp.path().join("handle.key")).unwrap();
        self.sources = Arc::new(SourceRegistry::new(&self.cfg).unwrap());
        self.emb = Arc::new(EmbeddingService::new(
            self.index.clone(),
            self.cfg.embedding.clone(),
            self.cfg.search.clone(),
            self.shared.clone(),
            self.provider.clone(),
        ));
        self.engine = Arc::new(IngestEngine::new(
            self.index.clone(),
            self.cfg.clone(),
            self.sources.clone(),
            self.emb.clone(),
            self.shared.clone(),
        ));
        self.leases = LeaseManager::new(
            self.index.clone(),
            self.cfg.reader.clone(),
            self.shared.clone(),
        );
        self.svc = ReadService::new(
            self.index.clone(),
            self.cfg.clone(),
            self.leases.clone(),
            self.keys.clone(),
            self.sources.clone(),
            self.engine.source_read_stager(),
            self.shared.clone(),
        );
    }

    /// Replace the read service config (e.g. enable raw records) without touching files.
    pub fn reconfigure(&mut self, f: impl FnOnce(&mut Config)) {
        let mut c = (*self.cfg).clone();
        f(&mut c);
        self.cfg = Arc::new(c);
        self.leases = LeaseManager::new(
            self.index.clone(),
            self.cfg.reader.clone(),
            self.shared.clone(),
        );
        self.svc = ReadService::new(
            self.index.clone(),
            self.cfg.clone(),
            self.leases.clone(),
            self.keys.clone(),
            self.sources.clone(),
            self.engine.source_read_stager(),
            self.shared.clone(),
        );
    }

    pub async fn add(&self, source: &str, name: &str, content: &[u8]) -> (String, PathBuf) {
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
        self.add("omp", name, join(lines).as_bytes()).await
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

    pub async fn indexed(&self, name: &str, lines: &[String]) -> (String, PathBuf) {
        let (sid, path) = self.add_omp(name, lines).await;
        self.index(&sid).await.expect("indexes");
        (sid, path)
    }

    pub async fn n(&self, sql: &'static str) -> i64 {
        sqlx::query_scalar::<_, i64>(sql)
            .fetch_one(&self.index.writer)
            .await
            .unwrap()
    }

    pub async fn read(&self, args: Value) -> Result<Value, AppError> {
        let r = self.svc.call(args).await;
        match &r {
            Ok(v) => {
                assert_output_valid(v);
                assert!(tool_result_bytes(v, false) <= 262_144 + 256);
            }
            Err(e) => assert_error_valid(e),
        }
        r
    }

    pub async fn ok(&self, args: Value) -> Value {
        self.read(args)
            .await
            .unwrap_or_else(|e| panic!("read failed: {e}"))
    }

    pub async fn err(&self, args: Value) -> AppError {
        match self.read(args).await {
            Ok(v) => panic!(
                "expected an error, got {} items",
                v["items"].as_array().map(|a| a.len()).unwrap_or(0)
            ),
            Err(e) => e,
        }
    }

    /// Follow `next_cursor` until exhaustion; every page is validated against the byte cap.
    pub async fn pages(&self, first: Value, max_bytes: usize) -> Vec<Value> {
        let max_items = first.get("max_items").cloned();
        let mut out = vec![self.ok(first).await];
        loop {
            let Some(c) = out.last().unwrap()["page"]["next_cursor"]
                .as_str()
                .map(str::to_string)
            else {
                break;
            };
            let mut args = json!({"cursor": c, "max_bytes": max_bytes});
            if let Some(m) = &max_items {
                args["max_items"] = m.clone();
            }
            let v = self.ok(args).await;
            assert!(
                tool_result_bytes(&v, false) <= max_bytes,
                "page exceeds max_bytes"
            );
            out.push(v);
            assert!(out.len() < 5000, "pagination does not terminate");
        }
        out
    }

    pub fn decode_message_ref(&self, token: &str) -> MessageRef {
        MessageRef::decode(&self.keys.current(), token).expect("message ref decodes")
    }

    pub fn decode_item(&self, token: &str) -> spool::transcript::refs::ItemRef {
        spool::transcript::refs::ItemRef::decode(&self.keys.current(), token)
            .expect("item ref decodes")
    }

    /// A search-style match reference over whole blocks (same payload shape the search service issues).
    pub fn match_ref(&self, anchor: &Anchor, spans: &[(&str, u32, &str)]) -> String {
        let mut frames = Vec::new();
        let mut refs = Vec::new();
        for (rec, block, text) in spans {
            frames.extend_from_slice(&(text.len() as u64).to_le_bytes());
            frames.extend_from_slice(text.as_bytes());
            refs.push(SpanRef {
                r: rec.to_string(),
                b: *block,
                s: 0,
                e: text.len() as u32,
            });
        }
        MatchRef {
            anchor: anchor.clone(),
            spans: refs,
            content: short_digest(&frames),
        }
        .encode(&self.keys.current())
    }

    /// Anchor of the latest lease/snapshot for a session, taken from a navigation/selection ref.
    pub fn anchor_of(&self, response: &Value) -> Anchor {
        let r = response["selection"]["from_message_ref"]
            .as_str()
            .expect("selection has a message ref");
        self.decode_message_ref(r).anchor
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

pub fn omp_msg_blocks(id: &str, parent: Option<&str>, role: &str, texts: &[&str]) -> String {
    let blocks: Vec<Value> = texts
        .iter()
        .map(|t| json!({"type":"text","text":t}))
        .collect();
    json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:01Z","message":{"role":role,"content":blocks}}).to_string()
}

pub fn omp_control(id: &str, parent: &str) -> String {
    json!({"type":"model_change","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:01Z","model":"m"}).to_string()
}

/// Linear session: header + `n` alternating user/assistant messages `m000..` (each parented on the previous).
pub fn linear(n: usize) -> Vec<String> {
    let mut v = vec![omp_header()];
    for i in 0..n {
        let parent = (i > 0).then(|| format!("m{:03}", i - 1));
        v.push(omp_msg(
            &format!("m{i:03}"),
            parent.as_deref(),
            if i % 2 == 0 { "user" } else { "assistant" },
            &format!("message number {i} qwz{i}"),
        ));
    }
    v
}

/// Base64 STANDARD decode helper for fragments.
pub fn b64(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

/// Decoded bytes of one fragment.
pub fn frag_bytes(f: &Value) -> Vec<u8> {
    let d = f["data"].as_str().unwrap();
    if f["encoding"] == "base64" {
        b64(d)
    } else {
        d.as_bytes().to_vec()
    }
}

/// Reassemble one block ordinal per (item_ref, block_ref) across pages, asserting no overlap/skip.
pub fn reassemble(pages: &[Value]) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = Vec::new();
    let mut cursor: std::collections::HashMap<String, usize> = Default::default();
    for p in pages {
        for it in p["items"].as_array().unwrap() {
            for f in it["blocks"].as_array().unwrap() {
                let key = f["block_ref"].as_str().unwrap().to_string();
                let start = f["start_byte"].as_u64().unwrap() as usize;
                let end = f["end_byte"].as_u64().unwrap() as usize;
                let total = f["total_bytes"].as_u64().unwrap() as usize;
                let bytes = frag_bytes(f);
                assert_eq!(
                    bytes.len(),
                    end - start,
                    "decoded length equals the byte range"
                );
                assert_eq!(
                    *cursor.get(&key).unwrap_or(&0),
                    start,
                    "no gap or overlap between fragments"
                );
                assert_eq!(f["is_last"].as_bool().unwrap(), end == total);
                cursor.insert(key.clone(), end);
                match out.iter_mut().find(|(k, _)| *k == key) {
                    Some((_, b)) => b.extend_from_slice(&bytes),
                    None => out.push((key, bytes)),
                }
            }
        }
    }
    out
}
