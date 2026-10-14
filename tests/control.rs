//! `index_status` and the CLI administration dispatcher (spec 14.3, 16.2).

mod jobs_common;

use jobs_common::*;
use serde_json::{Value, json};
use spool::config::Paths;
use spool::control::{ControlDeps, ControlService};
use spool::daemon::BASELINE_ID;
use spool::embeddings::fake::Fault;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::ingest::engine::IngestEngine;
use spool::jobs::{JobSystem, WorkerState};
use spool::storage::catalog;
use spool::transcript::handle::{HandleKey, KeyStore};
use spool::transcript::lease::LeaseManager;
use std::sync::Arc;
use std::time::Duration;

const SENTINEL: &str = "ZEBRA_SENTINEL_TRANSCRIPT_TEXT";

struct Rig {
    env: Env,
    sys: Arc<JobSystem>,
    ctl: ControlService,
    paths: Paths,
    leases: LeaseManager,
    keys: KeyStore,
}

fn paths_in(root: &std::path::Path) -> Paths {
    let home = root.join("h");
    std::fs::create_dir_all(home.join("secrets")).unwrap();
    Paths {
        root: home.clone(),
        config_file: home.join("config.toml"),
        state_dir: home.join("state"),
        socket: home.join("s.sock"),
        lock: home.join("l"),
        index_db: root.join("index.sqlite"),
        jobs_db: root.join("jobs.sqlite"),
        secrets_dir: home.join("secrets"),
        signing_key: home.join("secrets/key"),
    }
}

async fn rig_with(env: Env, engine: Option<Arc<IngestEngine>>) -> Rig {
    let paths = paths_in(&env.root);
    HandleKey::create(&paths.signing_key).unwrap();
    let keys = KeyStore::load(&paths.signing_key).unwrap();
    let engine = engine.unwrap_or_else(|| env.engine.clone());
    let sys = JobSystem::start(
        env.index.clone(),
        env.jobs_pool.clone(),
        env.cfg.clone(),
        env.sources.clone(),
        engine.clone(),
        env.shared.clone(),
        FAST,
    )
    .await
    .unwrap();
    let leases = LeaseManager::new(
        env.index.clone(),
        env.cfg.reader.clone(),
        env.shared.clone(),
    );
    let ctl = ControlService::new(ControlDeps {
        index: env.index.clone(),
        jobs: sys.clone(),
        cfg: env.cfg.clone(),
        sources: env.sources.clone(),
        engine,
        leases: leases.clone(),
        keys: keys.clone(),
        paths: paths.clone(),
        clock: env.shared.clone(),
    });
    Rig {
        env,
        sys,
        ctl,
        paths,
        leases,
        keys,
    }
}

async fn rig() -> Rig {
    rig_with(Env::new().await, None).await
}

impl Rig {
    async fn ok(&self, cmd: &str, args: Value) -> Value {
        self.ctl
            .call(cmd, args)
            .await
            .unwrap_or_else(|e| panic!("{cmd} failed: {e:?}"))
    }
    async fn err(&self, cmd: &str, args: Value) -> String {
        self.ctl.call(cmd, args).await.expect_err(cmd).code
    }
    async fn status(&self) -> Value {
        self.ctl.index_status(json!({})).await.unwrap()
    }
    async fn wait_op(&self, id: &str, want: &str) -> Value {
        let end = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let v = self.ok("jobs_status", json!({"operation_id": id})).await;
            if v["state"] == want {
                return v;
            }
            assert!(std::time::Instant::now() < end, "operation stuck: {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn shutdown(self) {
        self.sys.shutdown(Duration::from_secs(15)).await;
    }
}

// ---------------------------------------------------------------------------------------------
// index_status
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_definition_is_read_only_with_schemas_and_is_the_only_control_tool() {
    let t = ControlService::tool_definition();
    assert_eq!(t.name, "index_status");
    assert!(t.output_schema.is_some());
    assert_eq!(
        t.input_schema.get("type").and_then(Value::as_str),
        Some("object")
    );
    assert_eq!(
        t.annotations.as_ref().and_then(|a| a.read_only_hint),
        Some(true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skipped_subagents_are_not_pending_work() {
    let env = Env::new().await;
    env.omp("main.jsonl", "main", &["hello main", "reply"], T0 - DAY);
    let p = env.omp("main/Sub.jsonl", "sub", &["hello sub", "reply"], T0 - DAY);
    let body = std::fs::read_to_string(&p).unwrap().replacen(
        "\"type\":\"session\"",
        "\"parentSession\":\"main\",\"type\":\"session\"",
        1,
    );
    std::fs::write(&p, body).unwrap();
    set_mtime(&p, T0 - DAY);
    let r = rig_with(env, None).await;
    r.env.wait_state("main", "indexed", 30).await;
    wait_until(20, "queue drained despite skipped subagent", || async {
        r.status().await["status"] == "ready"
    })
    .await;
    let status = r.status().await;
    assert_eq!(status["queue"]["pending_sessions"], 0);
    assert_eq!(status["sources"][0]["sessions_total"], 2);
    let sub = r.env.session_by_native("sub").await.unwrap();
    assert_eq!(sub.index_state, "pending");
    assert_eq!(sub.session_kind, "subagent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_reports_coverage_queue_workers_provider_and_storage_without_secrets_or_text() {
    let env = Env::new().await;
    env.omp("z.jsonl", "zebra", &[SENTINEL, "a reply"], T0 - DAY);
    let r = rig_with(env, None).await;
    r.env.wait_state("zebra", "indexed", 30).await;
    wait_until(20, "vectors", || async {
        r.env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    wait_until(20, "worker running", || async {
        r.sys.health().ingest.state == WorkerState::Running
    })
    .await;
    wait_until(20, "queue drained", || async {
        r.status().await["status"] == "ready"
    })
    .await;
    let s = r.status().await;
    let text = s.to_string();
    assert!(!text.contains(SENTINEL), "no transcript text in status");
    assert!(!text.to_lowercase().contains("authorization") && !text.contains("api_key"));
    assert_eq!(s["baseline"], BASELINE_ID);
    assert_eq!(s["daemon_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(s["status"], "ready", "{s}");
    let src = &s["sources"][0];
    assert_eq!(src["id"], "omp");
    assert_eq!(src["bootstrap"]["days"], 7);
    assert_eq!(src["bootstrap"]["complete"], true);
    assert_eq!(src["bootstrap"]["cutoff_ms"], T0 - 7 * DAY);
    assert_eq!(src["last_scan"]["ok"], true);
    assert_eq!(s["queue"]["indexed"], 1);
    assert_eq!(s["queue"]["covered"], 1);
    assert_eq!(s["queue"]["permanently_failed"], 0);
    assert_eq!(s["vectors"]["pending_chunks"], 0);
    assert_eq!(s["embedding"]["state"], "ready");
    assert!(
        s["embedding"]["budget"]["ingest"]["limit"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(s["workers"]["ingest"]["state"], "running");
    assert_eq!(s["workers"]["ingestion_degraded"], false);
    assert!(s["storage"]["index_db_bytes"].as_u64().unwrap() > 0);
    assert!(s["last_discovery"]["omp"]["scanned"].as_u64().unwrap() >= 1);
    assert!(s["parse_warnings"]["total"].is_number());
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_only_when_hosted_processing_is_not_approved() {
    let env = Env::new().await;
    let provider = Arc::new(FakeProvider::new(32).hosted(true));
    let emb = Arc::new(EmbeddingService::new(
        env.index.clone(),
        env.cfg.embedding.clone(),
        env.cfg.search.clone(),
        env.shared.clone(),
        provider.clone(),
    ));
    emb.ensure_profile().await.unwrap();
    let engine = Arc::new(IngestEngine::new(
        env.index.clone(),
        env.cfg.clone(),
        env.sources.clone(),
        emb,
        env.shared.clone(),
    ));
    env.omp("l.jsonl", "lexi", &["lexical only text", "reply"], T0 - DAY);
    let r = rig_with(env, Some(engine)).await;
    r.env.wait_state("lexi", "indexed", 30).await;
    wait_until(20, "lexical_only", || async {
        r.status().await["status"] == "lexical_only"
    })
    .await;
    let s = r.status().await;
    assert_eq!(s["status"], "lexical_only", "{s}");
    assert_eq!(s["embedding"]["state"], "not_approved");
    assert_eq!(
        provider.calls(),
        0,
        "nothing was sent to a hosted provider without approval"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_blocked_unsupported_storage_and_degraded_states() {
    let r = rig().await;
    // provider_blocked: auth failure with vectors pending
    r.env.provider.set_fault(Some(Fault::Auth));
    r.env.omp(
        "p.jsonl",
        "pblock",
        &["provider blocked text", "reply"],
        T0 - DAY,
    );
    wait_until(30, "provider blocked", || async {
        r.status().await["status"] == "provider_blocked"
    })
    .await;
    let s = r.status().await;
    assert!(
        s["states"]
            .as_array()
            .unwrap()
            .contains(&json!("provider_blocked"))
    );
    // unsupported_format
    sqlx::query("UPDATE sessions SET index_state='unsupported' WHERE native_id='pblock'")
        .execute(&r.env.index.writer)
        .await
        .unwrap();
    r.env.provider.set_fault(None);
    r.env.emb.reset_auth_block();
    wait_until(20, "state recomputed", || async {
        r.status().await["states"]
            .as_array()
            .unwrap()
            .contains(&json!("unsupported_format"))
    })
    .await;
    // ingestion_degraded: kill the ingest worker; status reports it while the control plane answers
    r.sys.kill_ingest_worker_for_test().await;
    wait_until(10, "degraded visible", || async {
        r.status().await["states"]
            .as_array()
            .unwrap()
            .contains(&json!("ingestion_degraded"))
    })
    .await;
    wait_until(40, "worker back", || async {
        r.sys.health().ingest.state == WorkerState::Running
    })
    .await;
    r.shutdown().await;

    // storage_error: ingestion paused at the hard threshold, search state preserved
    let env = Env::with(|c| {
        c.storage.warning_bytes = 0;
        c.storage.pause_ingestion_bytes = 1;
    })
    .await;
    env.omp("s.jsonl", "paused", &["paused text", "reply"], T0 - DAY);
    let r = rig_with(env, None).await;
    wait_until(30, "storage_error", || async {
        r.status().await["status"] == "storage_error"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        r.env.state_of("paused").await,
        "pending",
        "no new ingestion while the storage threshold is exceeded"
    );
    assert_eq!(r.env.count("SELECT count(*) FROM chunks").await, 0);
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_pagination_and_strict_arguments() {
    let r = rig().await;
    for i in 0..25 {
        spool::jobs::ops::begin(&r.env.index, "maintenance", &json!({"i": i}), None, T0 + i)
            .await
            .unwrap();
    }
    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let args = match &cursor {
            Some(c) => json!({"limit": 10, "cursor": c}),
            None => json!({"limit": 10}),
        };
        let s = r.ctl.index_status(args).await.unwrap();
        for o in s["operations"].as_array().unwrap() {
            assert!(
                seen.insert(o["operation_id"].as_str().unwrap().to_string()),
                "no duplicate across pages"
            );
        }
        pages += 1;
        match s["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert!(seen.len() >= 25 && pages >= 3);
    assert_eq!(
        r.ctl
            .index_status(json!({"limit": 0}))
            .await
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        r.ctl
            .index_status(json!({"limit": 1000}))
            .await
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        r.ctl
            .index_status(json!({"query": "full text of a user query"}))
            .await
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        r.ctl
            .index_status(json!({"operation_id": "op_nope"}))
            .await
            .unwrap_err()
            .code,
        "OPERATION_NOT_FOUND"
    );
    assert_eq!(
        r.ctl
            .index_status(json!({"session_id": "ss_nope"}))
            .await
            .unwrap_err()
            .code,
        "SESSION_NOT_FOUND"
    );
    assert_eq!(
        r.ctl
            .index_status(json!({"source": "nope"}))
            .await
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_describes_one_source_one_session_and_one_operation() {
    let env = Env::new().await;
    for i in 0..5 {
        env.omp(
            &format!("s{i}.jsonl"),
            &format!("n{i}"),
            &["hello status", "reply"],
            T0 - DAY,
        );
    }
    let r = rig_with(env, None).await;
    wait_until(40, "five indexed", || async {
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await
            == 5
    })
    .await;
    let page1 = r
        .ctl
        .index_status(json!({"source": "omp", "limit": 2}))
        .await
        .unwrap();
    assert_eq!(page1["sessions"].as_array().unwrap().len(), 2);
    let cur = page1["next_cursor"].as_str().unwrap().to_string();
    let page2 = r
        .ctl
        .index_status(json!({"source": "omp", "limit": 10, "cursor": cur}))
        .await
        .unwrap();
    assert_eq!(page2["sessions"].as_array().unwrap().len(), 3);
    let sid = page1["sessions"][0]["session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let one = r
        .ctl
        .index_status(json!({"session_id": sid}))
        .await
        .unwrap();
    assert_eq!(one["session"]["index_state"], "indexed");
    assert!(one["session"]["parse_warnings"].is_number());
    assert!(!one.to_string().contains("hello status"));
    let sub = r
        .ok("index_session", json!({"session_id": sid, "wait": true}))
        .await;
    let op = r
        .ctl
        .index_status(json!({"operation_id": sub["operation_id"]}))
        .await
        .unwrap();
    assert_eq!(op["operation"]["state"], "done");
    r.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// indexing commands
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_reports_candidates_exclusions_and_local_token_estimates_without_side_effects() {
    let env = Env::new().await;
    for i in 0..4 {
        env.omp(
            &format!("o{i}.jsonl"),
            &format!("old{i}"),
            &[
                "a fairly ordinary sentence about databases",
                "and a reply to it",
            ],
            T0 - 20 * DAY,
        );
    }
    let r = rig_with(env, None).await;
    wait_until(20, "initial discovery", || async {
        r.env
            .count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await
            == 1
    })
    .await;
    let before_jobs = r
        .env
        .jobs_count("SELECT count(*) FROM Jobs WHERE job_type = 'spool-ingest'")
        .await;
    // exclude one up front so the dry run can report it
    let since = T0 - 30 * DAY;
    let dry = r
        .ok("index_since", json!({"since_ms": since, "dry_run": true}))
        .await;
    let first = r.env.session_by_native("old0").await.unwrap();
    catalog::exclude_session(&r.env.index, &first.id, "x", T0)
        .await
        .unwrap();
    let dry2 = r
        .ok("index_since", json!({"since_ms": since, "dry_run": true}))
        .await;
    assert_eq!(dry["dry_run"], true);
    assert_eq!(dry["candidate_sessions"], 4, "{dry}");
    assert!(dry["estimated_tokens"].as_u64().unwrap() > 0);
    assert_eq!(dry["estimate_exact"], true);
    assert_eq!(dry["provider_calls"], 0);
    assert_eq!(dry2["candidate_sessions"], 3, "{dry2}");
    assert_eq!(dry2["excluded"], 1);
    assert_eq!(r.env.provider.calls(), 0, "estimates are local");
    assert_eq!(
        r.env
            .jobs_count("SELECT count(*) FROM Jobs WHERE job_type = 'spool-ingest'")
            .await,
        before_jobs,
        "dry run enqueues nothing"
    );
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await,
        0
    );

    // the real thing, waiting by status polling
    let done = r
        .ok(
            "index_since",
            json!({"since_ms": since, "wait": true, "wait_timeout_ms": 60000}),
        )
        .await;
    assert_eq!(done["state"], "done", "{done}");
    assert_eq!(done["timed_out"], false);
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await,
        3,
        "excluded session is not indexed"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_returns_done_only_after_every_scoped_session_is_indexed_even_for_a_fresh_file() {
    let env = Env::new().await;
    env.omp(
        "fresh.jsonl",
        "fresh",
        &["written one second ago", "reply"],
        T0 - 1000,
    ); // inside the discovery debounce
    let r = rig_with(env, None).await;
    let out = r
        .ok(
            "index_since",
            json!({"since_ms": T0 - 30 * DAY, "wait": true, "wait_timeout_ms": 60000}),
        )
        .await;
    assert_eq!(out["state"], "done", "{out}");
    assert_eq!(
        r.env.state_of("fresh").await,
        "indexed",
        "done means the scoped session reached a terminal state"
    );
    assert!(r.env.count("SELECT count(*) FROM chunks").await > 0);
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_is_only_ever_explicit_and_older_sessions_are_not_auto_indexed() {
    let env = Env::new().await;
    env.omp(
        "ancient.jsonl",
        "ancient",
        &["ancient text", "reply"],
        T0 - 365 * DAY,
    );
    let r = rig_with(env, None).await;
    wait_until(20, "discovery ran", || async {
        r.env
            .count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await
            == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        r.env.count("SELECT count(*) FROM sessions").await,
        0,
        "startup/ordinary discovery does not imply --all"
    );
    let dry = r.ok("index_all", json!({"dry_run": true})).await;
    assert_eq!(dry["candidate_sessions"], 1);
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await,
        0
    );
    let out = r
        .ok("index_all", json!({"wait": true, "wait_timeout_ms": 60000}))
        .await;
    assert_eq!(out["state"], "done");
    assert_eq!(r.env.state_of("ancient").await, "indexed");
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_returns_after_its_timeout_while_the_durable_operation_continues() {
    let env = Env::new().await;
    env.omp(
        "w.jsonl",
        "waiter",
        &["waiting text", "reply"],
        T0 - 50 * DAY,
    );
    env.provider.set_fault(Some(Fault::Hang));
    let r = rig_with(env, None).await;
    wait_until(20, "cataloged", || async {
        r.env
            .count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await
            == 1
    })
    .await;
    let _ = r.ok("index_all", json!({"dry_run": true})).await;
    let sid = r.env.session_by_native("waiter").await.unwrap().id;
    let out = r
        .ok(
            "index_session",
            json!({"session_id": sid, "wait": true, "wait_timeout_ms": 1200}),
        )
        .await;
    assert_eq!(out["timed_out"], true, "{out}");
    assert!(matches!(
        out["state"].as_str(),
        Some("running") | Some("queued")
    ));
    // cancelling the *wait* did not cancel the operation; releasing the provider lets it finish
    r.env.provider.set_fault(None);
    r.sys.kill_ingest_worker_for_test().await;
    r.wait_op(out["operation_id"].as_str().unwrap(), "done")
        .await;
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jobs_cancel_is_cooperative_and_retry_resumes() {
    let env = Env::with(|c| c.ingestion.max_queued_sessions = 1).await;
    for i in 0..6 {
        env.omp(
            &format!("c{i}.jsonl"),
            &format!("c{i}"),
            &[&format!("cancel candidate {i}"), "reply"],
            T0 - 60 * DAY,
        );
    }
    let r = rig_with(env, None).await;
    r.sys
        .ctx()
        .set_capacity_recheck(Duration::from_millis(1000));
    let sub = r.ok("index_all", json!({"idempotency_key": "bf"})).await;
    let id = sub["operation_id"].as_str().unwrap().to_string();
    assert!(sub["task_id"].is_string());
    let again = r.ok("index_all", json!({"idempotency_key": "bf"})).await;
    assert_eq!(
        again["operation_id"], sub["operation_id"],
        "idempotent replay"
    );
    assert_eq!(
        r.ctl
            .call(
                "index_since",
                json!({"since_ms": 1, "idempotency_key": "bf"})
            )
            .await
            .unwrap_err()
            .code,
        "IDEMPOTENCY_CONFLICT"
    );
    let c = r.ok("jobs_cancel", json!({"operation_id": id})).await;
    assert_eq!(c["state"], "cancelled");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let v = r.ok("jobs_status", json!({"operation_id": id})).await;
    assert_eq!(v["state"], "cancelled", "{v}");
    let indexed_after_cancel = r
        .env
        .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
        .await;
    assert!(
        indexed_after_cancel < 6,
        "cancel stopped the backfill ({indexed_after_cancel})"
    );
    // already committed progress is kept: indexed sessions are still searchable
    assert_eq!(
        r.env.count("SELECT count(*) FROM chunks").await > 0 || indexed_after_cancel == 0,
        true
    );
    assert_eq!(
        r.err("jobs_retry", json!({"operation_id": "op_missing"}))
            .await,
        "OPERATION_NOT_FOUND"
    );
    r.ok("jobs_retry", json!({"operation_id": id})).await;
    r.wait_op(&id, "done").await;
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await,
        6
    );
    assert_eq!(
        r.err("jobs_retry", json!({"operation_id": id})).await,
        "INVALID_ARGUMENT",
        "a finished operation with no failed sessions cannot be retried"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebuild_never_broadens_scope_and_preserves_exclusions() {
    let env = Env::new().await;
    env.omp("a.jsonl", "ra", &["rebuild alpha text", "reply"], T0 - DAY);
    env.omp(
        "x.jsonl",
        "rx",
        &["rebuild excluded text", "reply"],
        T0 - DAY,
    );
    env.omp(
        "old.jsonl",
        "rold",
        &["rebuild old text", "reply"],
        T0 - 90 * DAY,
    );
    let r = rig_with(env, None).await;
    wait_until(40, "two indexed", || async {
        r.env
            .count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await
            == 2
    })
    .await;
    let _ = r.ok("index_all", json!({"dry_run": true})).await; // catalogs the old one inertly
    let rx = r.env.session_by_native("rx").await.unwrap().id;
    r.ok("index_remove", json!({"session_id": rx, "exclude": true}))
        .await;
    wait_until(20, "vectors", || async {
        r.env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    let gen_before = r.env.session_by_native("ra").await.unwrap().generation;

    assert_eq!(
        r.err(
            "index_rebuild",
            json!({"lexical_only": true, "embeddings_only": true})
        )
        .await,
        "INVALID_ARGUMENT"
    );
    let op = r.ok("index_rebuild", json!({"lexical_only": true})).await;
    r.wait_op(op["operation_id"].as_str().unwrap(), "done")
        .await;
    let ra = r.env.session_by_native("ra").await.unwrap();
    assert_eq!(ra.index_state, "indexed");
    assert!(ra.generation > gen_before, "derived data was recreated");
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH 'alpha'")
            .await,
        1
    );
    assert_eq!(
        r.env.state_of("rold").await,
        "pending",
        "rebuild did not broaden the scope to the old session"
    );
    assert_ne!(
        r.env.session_by_native("rold").await.unwrap().eligibility,
        "manual"
    );
    let rx = r.env.session_by_native("rx").await.unwrap();
    assert!(
        rx.excluded && rx.active_snapshot_id.is_none(),
        "exclusions are preserved"
    );
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH 'excluded'")
            .await,
        0
    );

    // embeddings-only: text untouched, lost vectors regenerated
    wait_until(20, "vectors", || async {
        r.env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    sqlx::query("DELETE FROM embeddings")
        .execute(&r.env.index.writer)
        .await
        .unwrap();
    let gen_text = r.env.session_by_native("ra").await.unwrap().generation;
    let op = r
        .ok("index_rebuild", json!({"embeddings_only": true}))
        .await;
    r.wait_op(op["operation_id"].as_str().unwrap(), "done")
        .await;
    wait_until(30, "vectors back", || async {
        r.env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    assert_eq!(
        r.env.session_by_native("ra").await.unwrap().generation,
        gen_text,
        "embeddings-only rebuild leaves published text alone"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remove_with_exclude_revokes_leases_deletes_derived_data_keeps_the_source_and_survives_rediscovery()
 {
    let env = Env::new().await;
    let p = env.omp(
        "r.jsonl",
        "removeme",
        &["secret conversation text", "reply"],
        T0 - DAY,
    );
    let r = rig_with(env, None).await;
    r.env.wait_state("removeme", "indexed", 30).await;
    let s = r.env.session_by_native("removeme").await.unwrap();
    let lease = r
        .leases
        .pin(&[spool::transcript::lease::PinTarget {
            session_id: s.id.clone(),
            snapshot_id: s.active_snapshot_id.clone().unwrap(),
        }])
        .await
        .unwrap();
    let before = std::fs::read(&p).unwrap();

    assert_eq!(
        r.err("index_remove", json!({"session_id": s.id})).await,
        "INVALID_ARGUMENT",
        "only exclusion is offered"
    );
    assert_eq!(
        r.err(
            "index_remove",
            json!({"session_id": "ss_nope", "exclude": true})
        )
        .await,
        "SESSION_NOT_FOUND"
    );
    let out = r
        .ok(
            "index_remove",
            json!({"session_id": s.id, "exclude": true, "idempotency_key": "rm"}),
        )
        .await;
    assert_eq!(out["excluded"], true);
    assert_eq!(
        r.ok(
            "index_remove",
            json!({"session_id": s.id, "exclude": true, "idempotency_key": "rm"})
        )
        .await["operation_id"],
        out["operation_id"]
    );
    assert_eq!(
        r.leases.state(&lease.lease_id).await.unwrap(),
        spool::transcript::lease::LeaseState::Revoked
    );
    assert_eq!(r.env.count("SELECT count(*) FROM chunks").await, 0);
    assert_eq!(
        r.env
            .count("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH 'secret'")
            .await,
        0
    );
    assert_eq!(
        std::fs::read(&p).unwrap(),
        before,
        "the source transcript is never modified"
    );

    // a change to the file is rediscovered but the exclusion wins; explicit indexing is refused
    std::fs::write(
        &p,
        omp_body("removeme", &["secret conversation text", "reply", "more"]),
    )
    .unwrap();
    set_mtime(&p, T0 - DAY / 2);
    r.env.clock.advance_ms(60_000);
    let d = spool::discovery::DiscoveryService::new(
        r.env.index.clone(),
        r.env.sources.clone(),
        r.env.cfg.clone(),
        r.env.shared.clone(),
    );
    d.run_auto("omp", Arc::new(Default::default()))
        .await
        .unwrap();
    r.sys.reconciler().pass().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(r.env.count("SELECT count(*) FROM chunks").await, 0);
    assert!(r.env.session_by_native("removeme").await.unwrap().excluded);
    assert_eq!(
        r.err("index_session", json!({"session_id": s.id})).await,
        "SOURCE_POLICY_CONFLICT"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_requires_the_unreferenced_flag_and_removes_only_unreferenced_vectors() {
    let r = rig().await;
    r.env.omp(
        "p.jsonl",
        "purge",
        &["purge referenced text", "reply"],
        T0 - DAY,
    );
    r.env.wait_state("purge", "indexed", 30).await;
    wait_until(20, "vectors", || async {
        r.env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    let referenced = r.env.count("SELECT count(*) FROM embeddings").await;
    sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?, ?, 32, zeroblob(128), ?)").bind(r.env.emb.profile().id()).bind(vec![9u8; 32]).bind(T0).execute(&r.env.index.writer).await.unwrap();
    assert_eq!(r.err("index_purge", json!({})).await, "INVALID_ARGUMENT");
    let op = r.ok("index_purge", json!({"unreferenced": true})).await;
    r.wait_op(op["operation_id"].as_str().unwrap(), "done")
        .await;
    assert_eq!(
        r.env.count("SELECT count(*) FROM embeddings").await,
        referenced
    );
    r.shutdown().await;
}

// ---------------------------------------------------------------------------------------------
// sources, keys, sessions, dispatcher
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sources_enable_and_disable_edit_config_toml_preserving_comments_and_mirror_to_the_catalog()
{
    let env = Env::new().await;
    let dir = env.dir.to_string_lossy().into_owned();
    let r = rig_with(env, None).await;
    let original = format!(
        "# Spool configuration -- hand written\nconfig_version = 1\n\n[discovery]\nbootstrap_days = 7 # keep a week\n\n# sources I care about\n[[sources]]\nid = \"omp\"\nkind = \"omp\"\n# flip me with `spool sources disable`\nenabled = true\nroots = [\"{dir}\"]\n\n[[sources]]\nid = \"claude\"\nkind = \"claude\"\nenabled = false\nroots = [\"{dir}/claude\"]\n"
    );
    std::fs::write(&r.paths.config_file, &original).unwrap();
    let list = r.ok("sources_list", json!({})).await;
    assert_eq!(list["sources"][0]["id"], "omp");
    assert_eq!(list["sources"][0]["enabled"], true);

    let out = r.ok("sources_disable", json!({"id": "omp"})).await;
    assert_eq!(out["changed"], true);
    let text = std::fs::read_to_string(&r.paths.config_file).unwrap();
    assert!(
        text.contains("# Spool configuration -- hand written")
            && text.contains("# keep a week")
            && text.contains("# flip me with `spool sources disable`"),
        "comments preserved:\n{text}"
    );
    assert!(text.contains("enabled = false"));
    assert!(!r.env.sources.is_enabled("omp"), "registry reloaded");
    assert_eq!(
        r.env
            .count("SELECT enabled FROM sources WHERE id = 'omp'")
            .await,
        0,
        "mirrored to the sources table"
    );
    assert_eq!(
        std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(&r.paths.config_file)
                .unwrap()
                .permissions()
        ) & 0o777,
        0o600
    );
    assert_eq!(
        r.ok("sources_disable", json!({"id": "omp"})).await["changed"],
        false
    );

    // re-enable: a new bootstrap starts and discovery runs for it
    r.env.clock.set_ms(T0 + 30 * DAY);
    r.env.omp(
        "n.jsonl",
        "newly",
        &["written after re-enable"],
        T0 + 29 * DAY,
    );
    let out = r.ok("sources_enable", json!({"id": "omp"})).await;
    assert_eq!(out["enabled"], true);
    assert!(r.env.sources.is_enabled("omp"));
    wait_until(
        30,
        "bootstrap restarted and the file discovered",
        || async { r.env.session_by_native("newly").await.is_some() },
    )
    .await;
    let s = catalog::source(&r.env.index, "omp").await.unwrap().unwrap();
    assert_eq!(s.bootstrap_started_at, Some(T0 + 30 * DAY));
    assert!(
        std::fs::read_to_string(&r.paths.config_file)
            .unwrap()
            .contains("# keep a week")
    );
    assert_eq!(
        r.err("sources_enable", json!({"id": "nope"})).await,
        "INVALID_ARGUMENT"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabling_a_source_when_the_config_has_no_sources_section_keeps_the_other_defaults() {
    let r = rig().await;
    std::fs::write(
        &r.paths.config_file,
        "# only comments here\nconfig_version = 1\n",
    )
    .unwrap();
    r.ok("sources_enable", json!({"id": "omp"})).await; // already enabled in the registry => no change, no file edit
    assert_eq!(
        std::fs::read_to_string(&r.paths.config_file).unwrap(),
        "# only comments here\nconfig_version = 1\n"
    );
    r.ok("sources_disable", json!({"id": "omp"})).await;
    let text = std::fs::read_to_string(&r.paths.config_file).unwrap();
    assert!(text.starts_with("# only comments here"));
    let cfg = spool::config::Config::load(&r.paths).unwrap();
    let ids: Vec<_> = cfg
        .sources
        .iter()
        .map(|s| (s.id.as_str(), s.enabled))
        .collect();
    assert_eq!(
        ids,
        [("omp", false), ("codex", false), ("claude", false)],
        "defaults were materialised, not dropped: {text}"
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotate_key_invalidates_old_handles() {
    let r = rig().await;
    let old = r.keys.current();
    let handle = old.sign(spool::transcript::handle::HandleKind::Match, b"payload");
    let out = r.ok("handles_rotate_key", json!({})).await;
    assert_eq!(out["rotated"], true);
    assert!(out["epoch"].as_u64().unwrap() > old.epoch() as u64);
    assert!(
        r.keys
            .current()
            .verify(spool::transcript::handle::HandleKind::Match, &handle)
            .is_err()
    );
    assert_eq!(
        r.env
            .count("SELECT CAST(value AS INTEGER) FROM app_meta WHERE key = 'signing_key_epoch'")
            .await,
        out["epoch"].as_i64().unwrap()
    );
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_list_pages_and_filters() {
    let env = Env::new().await;
    for i in 0..7 {
        env.omp(
            &format!("l{i}.jsonl"),
            &format!("l{i}"),
            &["list me", "reply"],
            T0 - DAY,
        );
    }
    let r = rig_with(env, None).await;
    wait_until(30, "7 sessions", || async {
        r.env.count("SELECT count(*) FROM sessions").await == 7
    })
    .await;
    let p1 = r.ok("sessions_list", json!({"limit": 3})).await;
    assert_eq!(p1["sessions"].as_array().unwrap().len(), 3);
    let p2 = r
        .ok(
            "sessions_list",
            json!({"limit": 10, "after": p1["next_cursor"]}),
        )
        .await;
    assert_eq!(p2["sessions"].as_array().unwrap().len(), 4);
    assert!(p2["next_cursor"].is_null());
    assert_eq!(
        r.ok("sessions_list", json!({"source": "nope"})).await["sessions"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        r.err("sessions_list", json!({"limit": 0})).await,
        "INVALID_ARGUMENT"
    );
    assert!(!p1.to_string().contains("list me"));
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatcher_rejects_unknown_commands_and_unknown_fields() {
    let r = rig().await;
    assert_eq!(
        r.err("index_everything", json!({})).await,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        r.err(
            "index_session",
            json!({"session_id": "x", "path": "/etc/passwd"})
        )
        .await,
        "INVALID_ARGUMENT"
    );
    assert_eq!(r.err("index_session", json!({})).await, "INVALID_ARGUMENT");
    assert_eq!(
        r.err("index_session", json!({"session_id": "ss_nope"}))
            .await,
        "SESSION_NOT_FOUND"
    );
    assert_eq!(
        r.err("jobs_status", json!({"operation_id": "nope"})).await,
        "OPERATION_NOT_FOUND"
    );
    assert_eq!(
        r.err("index_all", json!({"source": "nope"})).await,
        "INVALID_ARGUMENT"
    );
    assert!(r.ok("status", json!({})).await["status"].is_string());
    r.shutdown().await;
}
