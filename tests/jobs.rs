//! Spec 11 + 19.4 jobs tests: durable submission, recovery matrix at the boundaries that exist,
//! reconciliation, retry/blocked policy, supervision. Real engine + deterministic fake provider.

mod jobs_common;

use jobs_common::*;
use serde_json::json;
use spool::embeddings::fake::Fault;
use spool::jobs::WorkerState;
use spool::jobs::payload::*;
use spool::jobs::scheduler::{RETRY_CAP, Reconciler, interleave, jitter_from, retry_delay};
use spool::storage::catalog;
use std::sync::Arc;
use std::time::Duration;

async fn chunks(env: &Env) -> i64 {
    env.count("SELECT count(*) FROM chunks").await
}

async fn fts_hits(env: &Env, q: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH ?")
        .bind(q)
        .fetch_one(&env.index.reader)
        .await
        .unwrap()
}

async fn running_ingest_tasks(env: &Env) -> i64 {
    env.jobs_count(
        "SELECT count(*) FROM Jobs WHERE job_type = 'spool-ingest' AND status = 'Running'",
    )
    .await
}

fn reconciler(env: &Env) -> Reconciler {
    Reconciler::new(
        env.index.clone(),
        env.submitter(),
        env.engine.clone(),
        env.emb.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    )
}

// ---------------------------------------------------------------------------------------------
// payloads, policy, ordering (pure)
// ---------------------------------------------------------------------------------------------

#[test]
fn payloads_are_small_versioned_references_without_paths_or_text() {
    let j = IndexSession {
        payload_version: PAYLOAD_VERSION,
        session_id: "ss_abc".into(),
        observed_source_revision: "10:20:30:40".into(),
        pipeline_fingerprint: "fp".into(),
        embedding_profile_id: "prof".into(),
        operation_id: Some("op_1".into()),
    };
    let v = serde_json::to_value(&j).unwrap();
    let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "embedding_profile_id",
            "observed_source_revision",
            "operation_id",
            "payload_version",
            "pipeline_fingerprint",
            "session_id"
        ]
    );
    assert!(serde_json::to_string(&v).unwrap().len() < 300);
    assert_eq!(serde_json::from_value::<IndexSession>(v).unwrap(), j);
    for job in [
        ControlJob::Discover(DiscoverSessions {
            payload_version: 1,
            source_ids: vec!["omp".into()],
            selection: DiscoverySelection::Since { since_ms: 5 },
            operation_id: Some("op".into()),
            recurring: false,
        }),
        ControlJob::Maintain(MaintainIndex {
            payload_version: 1,
            operation_id: None,
            scope: MaintenanceScope::Routine,
            recurring: true,
        }),
        ControlJob::Rebuild(RebuildIndex {
            payload_version: 1,
            operation_id: "op".into(),
            lexical: true,
            embeddings: false,
        }),
    ] {
        let s = serde_json::to_string(&job).unwrap();
        assert!(!s.contains("path") && !s.contains('/'), "{s}");
        assert_eq!(serde_json::from_str::<ControlJob>(&s).unwrap(), job);
        assert_eq!(job.payload_version(), 1);
    }
}

#[test]
fn retry_delay_is_exponential_with_jitter_capped_and_honours_retry_after() {
    let d = |a, ra: Option<u64>, j| retry_delay(a, ra.map(Duration::from_secs), j);
    assert_eq!(d(1, None, 0.5), Duration::from_secs(5));
    assert_eq!(d(2, None, 0.5), Duration::from_secs(10));
    assert_eq!(d(3, None, 0.5), Duration::from_secs(20));
    assert!(
        d(1, None, 0.0) >= Duration::from_secs(4) && d(1, None, 0.999) <= Duration::from_secs(6)
    );
    assert_eq!(
        d(30, None, 1.0),
        RETRY_CAP,
        "capped at 15 minutes including jitter"
    );
    assert_eq!(
        d(1, Some(120), 0.5),
        Duration::from_secs(120),
        "Retry-After is a floor"
    );
    assert_eq!(d(3, Some(1), 0.5), Duration::from_secs(20));
    let a = jitter_from("s", 1);
    assert!((0.0..1.0).contains(&a));
    assert_eq!(a, jitter_from("s", 1), "deterministic per seed");
}

#[test]
fn one_of_every_five_starts_is_reserved_for_waiting_backfill() {
    let active: Vec<String> = (0..40).map(|i| format!("a{i}")).collect();
    let backfill: Vec<String> = (0..10).map(|i| format!("b{i}")).collect();
    let mut counter = 0;
    let order = interleave(active, backfill, &mut counter);
    assert_eq!(order.len(), 50);
    for (i, item) in order.iter().take(25).enumerate() {
        assert_eq!(
            item.starts_with('b'),
            (i + 1) % 5 == 0,
            "position {} = {item}",
            i + 1
        );
    }
    // no backfill waiting: all slots go to active work; no active work: backfill is not blocked
    let mut c = 0;
    assert!(
        interleave(vec!["a1", "a2", "a3", "a4", "a5", "a6"], vec![], &mut c)
            .iter()
            .all(|x| x.starts_with('a'))
    );
    let mut c = 0;
    assert_eq!(interleave(vec![], vec!["b1", "b2"], &mut c), ["b1", "b2"]);
    // the counter carries across passes so the 1-in-5 cadence is not reset every pass
    let mut c = 3;
    let o = interleave(vec!["a1", "a2", "a3"], vec!["b1"], &mut c);
    assert_eq!(o, ["a1", "b1", "a2", "a3"]);
}

// ---------------------------------------------------------------------------------------------
// end to end
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_reconciliation_discovers_and_indexes_and_restart_is_a_noop() {
    let env = Env::new().await;
    env.omp(
        "a.jsonl",
        "a",
        &["the quick brown fox", "jumps over the lazy dog"],
        T0 - DAY,
    );
    env.omp(
        "b.jsonl",
        "b",
        &["sqlite is a database", "agreed"],
        T0 - DAY,
    );
    let sys = env.start().await;
    env.wait_state("a", "indexed", 30).await;
    env.wait_state("b", "indexed", 30).await;
    assert!(chunks(&env).await >= 2);
    assert_eq!(fts_hits(&env, "fox").await, 1);
    assert_eq!(
        env.count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await,
        1
    );
    wait_until(20, "vectors for every chunk", || async {
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    let calls = env.provider.calls();
    let gens: Vec<i64> = sqlx::query_scalar("SELECT generation FROM sessions ORDER BY id")
        .fetch_all(&env.index.reader)
        .await
        .unwrap();
    let t = std::time::Instant::now();
    sys.shutdown(Duration::from_secs(15)).await;
    assert!(t.elapsed() < Duration::from_secs(15));

    // restart against the same storage: nothing to do, no provider calls, same published data
    let sys2 = env.start().await;
    wait_until(20, "second startup discovery", || async {
        env.count("SELECT count(*) FROM sources WHERE last_scan_at IS NOT NULL")
            .await
            == 1
            && sys2.health().ingest.state == WorkerState::Running
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        env.provider.calls(),
        calls,
        "unchanged sessions cost no provider call"
    );
    let gens2: Vec<i64> = sqlx::query_scalar("SELECT generation FROM sessions ORDER BY id")
        .fetch_all(&env.index.reader)
        .await
        .unwrap();
    assert_eq!(gens, gens2);
    sys2.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appended_sessions_are_picked_up_by_the_recurring_discovery_chain() {
    let env = Env::with(|c| c.discovery.interval_seconds = 1).await;
    env.clock.set_ms(T0);
    let sys = env.start().await;
    wait_until(20, "first discovery", || async {
        env.count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await
            == 1
    })
    .await;
    env.clock.advance_ms(60_000);
    env.omp(
        "late.jsonl",
        "late",
        &["arrived while the daemon was running"],
        T0 + 30_000,
    ); // after the watermark
    env.wait_state("late", "indexed", 30).await;
    // the chain re-arms itself through Apalis scheduling: a live tick task exists
    assert!(env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type = 'spool-control' AND idempotency_key LIKE 'tick:discover:%' AND status IN ('Pending','Queued','Running')").await >= 1);
    sys.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// durable submission (spec 11.3)
// ---------------------------------------------------------------------------------------------

async fn catalog_session(env: &Env, native: &str, name: &str) -> String {
    env.omp(name, native, &["hello catalog", "reply"], T0 - 40 * DAY);
    let d = spool::discovery::DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    );
    d.run_explicit(
        "omp",
        spool::discovery::Selection::All,
        spool::discovery::Mode::CatalogOnly,
        None,
        100,
        Arc::new(Default::default()),
    )
    .await
    .unwrap();
    env.session_by_native(native).await.unwrap().id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn submission_returns_operation_and_task_ids_only_after_the_task_row_exists() {
    let env = Env::new().await;
    let sid = catalog_session(&env, "old", "old.jsonl").await;
    let sys = env.start().await;
    let sub = sys.submit_index_session(&sid, Some("key-1")).await.unwrap();
    assert!(sub.created);
    assert_eq!(
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{}'",
            sub.task_id
        ))
        .await,
        1,
        "the task row is stored before the id is returned"
    );
    let op = spool::jobs::ops::get(&env.index, &sub.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(op.task_id.as_deref(), Some(sub.task_id.as_str()));

    // idempotency: same key + same parameters => same operation and task
    let again = sys.submit_index_session(&sid, Some("key-1")).await.unwrap();
    assert_eq!(
        (
            again.operation_id.as_str(),
            again.task_id.as_str(),
            again.created
        ),
        (sub.operation_id.as_str(), sub.task_id.as_str(), false)
    );
    // same key + different parameters => conflict
    let other = catalog_session(&env, "other", "other.jsonl").await;
    let err = sys
        .submit_index_session(&other, Some("key-1"))
        .await
        .unwrap_err();
    assert_eq!(err.code, "IDEMPOTENCY_CONFLICT");

    env.wait_state("old", "indexed", 30).await;
    wait_until(20, "operation done", || async {
        spool::jobs::ops::get(&env.index, &sub.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state
            == "done"
    })
    .await;
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enqueue_failure_is_never_acknowledged_and_a_retry_with_the_same_key_succeeds() {
    let env = Env::new().await;
    let sid = catalog_session(&env, "old", "old.jsonl").await;
    let sys = env.start().await;
    sqlx::query("ALTER TABLE Jobs RENAME TO Jobs_hidden")
        .execute(&env.jobs_pool)
        .await
        .unwrap();
    let err = sys.submit_index_session(&sid, Some("k")).await.unwrap_err();
    sqlx::query("ALTER TABLE Jobs_hidden RENAME TO Jobs")
        .execute(&env.jobs_pool)
        .await
        .unwrap();
    assert_eq!(err.code, "STORAGE_ERROR", "{err:?}");
    assert_eq!(env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type = 'spool-ingest' AND idempotency_key LIKE 'op:%'").await, 0, "nothing was durably enqueued");
    let op_state: (String, Option<String>) =
        sqlx::query_as("SELECT state, task_id FROM operations WHERE idempotency_key = 'k'")
            .fetch_one(&env.index.reader)
            .await
            .unwrap();
    assert_eq!(op_state, ("failed".to_string(), None));
    // the same idempotency key is not poisoned: the retry is accepted
    let sub = sys.submit_index_session(&sid, Some("k")).await.unwrap();
    assert_eq!(
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{}'",
            sub.task_id
        ))
        .await,
        1
    );
    env.wait_state("old", "indexed", 60).await;
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_enqueued_before_any_worker_exists_remains_available() {
    let env = Env::new().await;
    let sid = catalog_session(&env, "waiting", "w.jsonl").await;
    // enqueue with no worker running at all (kill point: after enqueue, before claim)
    let sub = env.submitter();
    let tid = sub
        .push_index(
            IndexSession {
                payload_version: 1,
                session_id: sid.clone(),
                observed_source_revision: "x".into(),
                pipeline_fingerprint: "x".into(),
                embedding_profile_id: "x".into(),
                operation_id: None,
            },
            "manual-key",
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{tid}' AND status = 'Pending'"
        ))
        .await,
        1
    );
    catalog::mark_pending(&mut env.index.writer.acquire().await.unwrap(), &sid, None)
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET eligibility = 'manual' WHERE id = ?")
        .bind(&sid)
        .execute(&env.index.writer)
        .await
        .unwrap();
    let sys = env.start().await;
    env.wait_state("waiting", "indexed", 30).await;
    assert_eq!(
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{tid}' AND status = 'Done'"
        ))
        .await,
        1
    );
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusion_after_enqueue_prevents_resurrection() {
    let env = Env::new().await;
    let sid = catalog_session(&env, "secret", "s.jsonl").await;
    let tid = env
        .submitter()
        .push_index(
            IndexSession {
                payload_version: 1,
                session_id: sid.clone(),
                observed_source_revision: "x".into(),
                pipeline_fingerprint: "x".into(),
                embedding_profile_id: "x".into(),
                operation_id: None,
            },
            "k",
            None,
        )
        .await
        .unwrap();
    catalog::exclude_session(&env.index, &sid, "operator", T0)
        .await
        .unwrap();
    let sys = env.start().await;
    wait_until(30, "task processed", || async {
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{tid}' AND status = 'Done'"
        ))
        .await
            == 1
    })
    .await;
    let s = catalog::session(&env.index, &sid).await.unwrap().unwrap();
    assert!(s.excluded);
    assert!(
        s.active_snapshot_id.is_none(),
        "the old task did not publish an excluded session"
    );
    assert_eq!(chunks(&env).await, 0);
    assert_eq!(fts_hits(&env, "catalog").await, 0);
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_task_indexes_the_current_source_never_stale_content() {
    let env = Env::new().await;
    env.omp("g.jsonl", "gen", &["original wording"], T0 - DAY);
    let d = spool::discovery::DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    );
    d.run_auto("omp", Arc::new(Default::default()))
        .await
        .unwrap();
    let sid = env.session_by_native("gen").await.unwrap().id;
    let rev = env
        .session_by_native("gen")
        .await
        .unwrap()
        .revision()
        .unwrap();
    env.submitter()
        .push_index(
            IndexSession {
                payload_version: 1,
                session_id: sid.clone(),
                observed_source_revision: rev,
                pipeline_fingerprint: "x".into(),
                embedding_profile_id: "x".into(),
                operation_id: None,
            },
            "k",
            None,
        )
        .await
        .unwrap();
    // the source is rewritten before the task is claimed
    std::fs::write(
        env.path("g.jsonl"),
        omp_body("gen", &["rewritten wording", "with more"]),
    )
    .unwrap();
    set_mtime(&env.path("g.jsonl"), T0 - DAY / 2);
    let sys = env.start().await;
    env.wait_state("gen", "indexed", 30).await;
    assert_eq!(fts_hits(&env, "rewritten").await, 1);
    assert_eq!(fts_hits(&env, "original").await, 0);
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_delivery_is_a_cheap_noop() {
    let env = Env::new().await;
    env.omp("dup.jsonl", "dup", &["alpha text", "beta text"], T0 - DAY);
    let sys = env.start().await;
    env.wait_state("dup", "indexed", 30).await;
    wait_until(20, "vectors", || async {
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    let sid = env.session_by_native("dup").await.unwrap().id;
    let (calls, n_chunks, snaps) = (
        env.provider.calls(),
        chunks(&env).await,
        env.count("SELECT count(*) FROM snapshots").await,
    );
    // the same work delivered twice more (lost ack / duplicate task)
    for k in ["dup-1", "dup-2"] {
        env.submitter()
            .push_index(
                IndexSession {
                    payload_version: 1,
                    session_id: sid.clone(),
                    observed_source_revision: "x".into(),
                    pipeline_fingerprint: "x".into(),
                    embedding_profile_id: "x".into(),
                    operation_id: None,
                },
                k,
                None,
            )
            .await
            .unwrap();
    }
    wait_until(30, "duplicates processed", || async { env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-ingest' AND status = 'Done' AND idempotency_key LIKE 'dup-%'").await == 2 }).await;
    assert_eq!(env.provider.calls(), calls);
    assert_eq!(chunks(&env).await, n_chunks, "no duplicate searchable data");
    assert_eq!(env.count("SELECT count(*) FROM snapshots").await, snaps);
    assert_eq!(env.state_of("dup").await, "indexed");
    sys.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// worker kill / supervision (spec 19.4, 11.8)
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_ingest_worker_is_restarted_and_its_job_recovered_while_search_stays_up() {
    let env = Env::new().await;
    env.omp(
        "k.jsonl",
        "killme",
        &["searchable before the kill", "second message"],
        T0 - DAY,
    );
    env.provider.set_fault(Some(Fault::Hang)); // the job blocks in the embedding step, after text publication
    let sys = env.start().await;
    wait_until(30, "text published while embeddings hang", || async {
        running_ingest_tasks(&env).await == 1 && fts_hits(&env, "searchable").await == 1
    })
    .await;
    let attempts_before: i64 = env
        .jobs_count("SELECT attempts FROM Jobs WHERE job_type='spool-ingest' AND status='Running'")
        .await;

    env.provider.set_fault(None);
    sys.kill_ingest_worker_for_test().await;

    // the request server (reads) is unaffected the whole time, and the supervisor reports the failure
    let mut saw_degraded = false;
    let end = std::time::Instant::now() + Duration::from_secs(40);
    loop {
        assert_eq!(
            fts_hits(&env, "searchable").await,
            1,
            "search stays available while the worker is down"
        );
        let h = sys.health();
        saw_degraded |= h.ingestion_degraded;
        assert!(
            !matches!(
                h.control.state,
                WorkerState::Restarting | WorkerState::Degraded
            ),
            "the control worker is supervised independently: {h:?}"
        );
        if env.state_of("killme").await == "indexed"
            && h.ingest.state == WorkerState::Running
            && env.emb.vector_coverage().await.unwrap().pending_chunks == 0
        {
            break;
        }
        assert!(std::time::Instant::now() < end, "never recovered: {h:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(saw_degraded, "ingestion_degraded was reported");
    assert!(sys.health().ingest.restarts >= 1);
    wait_until(15, "task acknowledged", || async {
        env.jobs_count(
            "SELECT count(*) FROM Jobs WHERE job_type='spool-ingest' AND status = 'Done'",
        )
        .await
            >= 1
    })
    .await;
    let (status, attempts): (String, i64) = sqlx::query_as(
        "SELECT status, attempts FROM Jobs WHERE job_type='spool-ingest' ORDER BY rowid LIMIT 1",
    )
    .fetch_one(&env.jobs_pool)
    .await
    .unwrap();
    assert_eq!(status, "Done");
    assert!(
        attempts > attempts_before,
        "the orphaned task was re-run (attempts {attempts_before} -> {attempts})"
    );
    assert_eq!(
        chunks(&env).await,
        env.count("SELECT count(DISTINCT logical_id) FROM chunks")
            .await,
        "no duplicate chunks after recovery"
    );
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_never_acknowledges_unfinished_work_and_the_next_start_resumes_it() {
    let env = Env::new().await;
    env.omp(
        "s.jsonl",
        "shutdown",
        &["text before shutdown", "reply"],
        T0 - DAY,
    );
    env.provider.set_fault(Some(Fault::Hang));
    let sys = env.start().await;
    wait_until(30, "job in flight", || async {
        running_ingest_tasks(&env).await == 1
    })
    .await;
    let t = std::time::Instant::now();
    sys.shutdown(Duration::from_secs(3)).await; // the engine does not abort an in-flight provider call: the grace period bounds it
    assert!(
        t.elapsed() < Duration::from_secs(8),
        "shutdown honoured its grace period: {:?}",
        t.elapsed()
    );
    let done = env
        .jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-ingest' AND status = 'Done'")
        .await;
    assert_eq!(done, 0, "unfinished work must not be acknowledged");
    assert_ne!(env.state_of("shutdown").await, "failed");

    env.provider.set_fault(None);
    let sys2 = env.start().await;
    env.wait_state("shutdown", "indexed", 60).await;
    wait_until(30, "vectors", || async {
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    sys2.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// retry / blocked-state policy (spec 11.7)
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_failures_retry_through_apalis_scheduling_then_rest_without_failing_permanently()
{
    let env = Env::with(|c| c.ingestion.max_transient_attempts = 2).await;
    env.omp(
        "t.jsonl",
        "trans",
        &["flaky provider text", "reply"],
        T0 - DAY,
    );
    env.provider.set_fault(Some(Fault::Server));
    let sys = env.start().await;
    // attempt 1 fails -> the Apalis retry layer backs off (in-process, ~5s) on the SAME task: no second task row is created
    wait_until(30, "first failure recorded", || async {
        env.session_by_native("trans")
            .await
            .is_some_and(|s| s.index_attempts == 1 && s.index_state == "pending")
    })
    .await;
    assert_eq!(
        env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-ingest'")
            .await,
        1,
        "no application-level resubmission"
    );
    // attempt 2 (max) fails -> rest, visible as deferred/transient_exhausted, NOT failed
    wait_until(40, "exhausted", || async {
        env.state_of("trans").await == "deferred"
    })
    .await;
    let s = env.session_by_native("trans").await.unwrap();
    assert_eq!(s.index_error.as_deref(), Some("transient_exhausted"));
    assert_eq!(s.defer_until, Some(T0 + 15 * 60 * 1000));
    let calls = env.provider.calls();
    // reconciliation passes do not retry-storm it before the rest period ends
    for _ in 0..3 {
        sys.reconciler().pass().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(env.provider.calls(), calls);
    assert!(
        chunks(&env).await >= 1,
        "text was published and stays searchable despite the provider failure"
    );
    sys.shutdown(Duration::from_secs(15)).await;
}

/// Observed retry timing of the pinned Apalis retry layer with the Spool policy (base shrunk to
/// 200 ms so the test is fast). Prints `RETRY_TIMING` lines recorded in docs/dependency-baseline.md.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apalis_retry_layer_backs_off_exponentially_with_jitter_and_honours_retry_after() {
    use apalis::prelude::{Attempt, WorkerBuilder, WorkerBuilderExt};
    use spool::jobs::backend;
    use spool::jobs::scheduler::{IngestRetry, TransientIngest};
    let env = Env::new().await;
    let mut st = backend::storage::<IndexSession>(&env.jobs_pool, "retry-probe", FAST);
    {
        use apalis::prelude::{TaskBuilder, TaskSink};
        let payload = IndexSession {
            payload_version: 1,
            session_id: "x".into(),
            observed_source_revision: "x".into(),
            pipeline_fingerprint: "x".into(),
            embedding_profile_id: "x".into(),
            operation_id: None,
        };
        st.push_task(TaskBuilder::new(payload).max_attempts(10).build())
            .await
            .unwrap();
    }
    let stamps: Arc<parking_lot::Mutex<Vec<std::time::Instant>>> = Arc::default();
    let s2 = stamps.clone();
    let handler =
        move |_job: IndexSession, attempt: Attempt, wrk: apalis::prelude::WorkerContext| {
            let s2 = s2.clone();
            async move {
                s2.lock().push(std::time::Instant::now());
                let n = attempt.current();
                if n < 5 {
                    // attempt 2 carries a Retry-After of 1s, a floor above the 400ms backoff
                    let ra = (n == 2).then(|| Duration::from_secs(1));
                    return Err::<(), apalis::prelude::BoxDynError>(Box::new(TransientIngest {
                        retry_after: ra,
                    }));
                }
                wrk.stop().ok();
                Ok(())
            }
        };
    let policy = IngestRetry {
        max_attempts: 5,
        base: Duration::from_millis(200),
        cap: Duration::from_millis(2000),
    };
    let backend = backend::storage::<IndexSession>(&env.jobs_pool, "retry-probe", FAST);
    let worker = WorkerBuilder::new("probe")
        .backend(backend)
        .retry(policy)
        .build(handler);
    tokio::time::timeout(Duration::from_secs(30), worker.run())
        .await
        .expect("probe finished")
        .unwrap();
    let v = stamps.lock().clone();
    assert_eq!(v.len(), 5, "failed 4 times then succeeded");
    let gaps: Vec<u128> = v.windows(2).map(|w| (w[1] - w[0]).as_millis()).collect();
    println!("RETRY_TIMING base=200ms cap=2000ms retry_after(attempt2)=1000ms gaps_ms={gaps:?}");
    assert!(
        (140..=400).contains(&gaps[0]),
        "attempt 1 -> 2 ~200ms +-20%: {gaps:?}"
    );
    assert!(
        gaps[1] >= 1000 && gaps[1] < 1500,
        "Retry-After is a floor: {gaps:?}"
    );
    assert!(
        (560..=900).contains(&gaps[2]),
        "attempt 3 -> 4 ~800ms with jitter... {gaps:?}"
    );
    let (status, attempts): (String, i64) =
        sqlx::query_as("SELECT status, attempts FROM Jobs WHERE job_type='retry-probe'")
            .fetch_one(&env.jobs_pool)
            .await
            .unwrap();
    println!("RETRY_TIMING final status={status} attempts={attempts}");
    assert_eq!(status, "Done");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auth_failure_pauses_hosted_embedding_work_without_a_retry_storm() {
    let env = Env::new().await;
    env.omp("a.jsonl", "auth", &["auth blocked text", "reply"], T0 - DAY);
    env.omp(
        "b.jsonl",
        "auth2",
        &["second session text", "reply"],
        T0 - DAY,
    );
    env.provider.set_fault(Some(Fault::Auth));
    let sys = env.start().await;
    wait_until(30, "provider blocked", || async {
        env.state_of("auth").await == "deferred" || env.state_of("auth2").await == "deferred"
    })
    .await;
    wait_until(30, "both settled", || async {
        env.state_of("auth").await != "pending" && env.state_of("auth2").await != "pending"
    })
    .await;
    let calls = env.provider.calls();
    assert!(
        calls <= 2,
        "after the first auth failure no further provider calls are made for other sessions: {calls}"
    );
    env.clock.advance_ms(16 * 60 * 1000);
    for _ in 0..5 {
        sys.reconciler().pass().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        env.provider.calls(),
        calls,
        "blocked state gates reconciliation: no repeated private-text calls"
    );
    assert!(
        fts_hits(&env, "auth").await >= 1,
        "lexical search still works"
    );
    // operator fixes the credential: work resumes
    env.provider.set_fault(None);
    env.emb.reset_auth_block();
    sys.reconciler().pass().await.unwrap();
    wait_until(40, "vectors after unblock", || async {
        sys.reconciler().pass().await.ok();
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
            && env.state_of("auth").await == "indexed"
    })
    .await;
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn budget_exhaustion_defers_until_reset_and_is_not_a_failure() {
    let env = Env::with(|c| c.embedding.daily_ingestion_token_budget = 5).await;
    env.omp(
        "b.jsonl",
        "budget",
        &[
            "a sentence that costs more than five tokens to embed for sure",
            "yes",
        ],
        T0 - DAY,
    );
    let sys = env.start().await;
    wait_until(30, "deferred", || async {
        env.state_of("budget").await == "deferred"
    })
    .await;
    let s = env.session_by_native("budget").await.unwrap();
    assert_eq!(s.index_error.as_deref(), Some("budget_exhausted"));
    assert_eq!(
        s.index_attempts, 0,
        "budget deferral is not counted as a failure"
    );
    let resets = env.emb.status().budget.resets_at_ms;
    assert_eq!(s.defer_until, Some(resets));
    assert!(chunks(&env).await >= 1);
    let calls = env.provider.calls();
    sys.reconciler().pass().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        env.provider.calls(),
        calls,
        "nothing is retried before the budget resets"
    );
    sys.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permanent_errors_are_not_retried_by_reconciliation() {
    let env = Env::with(|c| {
        c.ingestion.max_record_bytes = 4096;
        c.ingestion.max_source_file_bytes = 4096;
    })
    .await;
    // a source file over max_source_file_bytes => permanent limit error
    let part = "x".repeat(3000); // each record fits max_record_bytes, the file does not fit max_source_file_bytes
    env.omp("p.jsonl", "perm", &[&part, &part], T0 - DAY);
    let sys = env.start().await;
    env.wait_state("perm", "failed", 30).await;
    let tasks = env
        .jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-ingest'")
        .await;
    for _ in 0..3 {
        sys.reconciler().pass().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-ingest'")
            .await,
        tasks,
        "a permanent failure is not resubmitted on timer ticks"
    );
    sys.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// queue cap and paged submission with a durable cursor
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_backfill_is_submitted_in_pages_under_the_queue_cap_and_resumes_its_cursor() {
    let env = Env::with(|c| c.ingestion.max_queued_sessions = 3).await;
    for i in 0..9 {
        env.omp(
            &format!("old{i}.jsonl"),
            &format!("old{i}"),
            &[&format!("backfill session number {i}")],
            T0 - 90 * DAY,
        );
    }
    let sys = env.start().await;
    sys.ctx().set_capacity_recheck(Duration::from_millis(1000));
    let sub = sys
        .submit_discovery(DiscoverySelection::All, None, Some("backfill"))
        .await
        .unwrap();
    let max_open = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let saw_cursor = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (envr, opid) = (&env, sub.operation_id.clone());
    let (mo, sc) = (max_open.clone(), saw_cursor.clone());
    wait_until(120, "backfill complete", move || {
        let (mo, sc, opid) = (mo.clone(), sc.clone(), opid.clone());
        async move {
            let open = envr.count("SELECT count(*) FROM sessions WHERE excluded = 0 AND eligibility != 'backfill' AND index_state IN ('pending','deferred')").await;
            mo.fetch_max(open, std::sync::atomic::Ordering::Relaxed);
            let op = spool::jobs::ops::get(&envr.index, &opid).await.unwrap().unwrap();
            if op.progress.get("after").is_some_and(|a| a.is_string()) {
                sc.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            op.state == "done"
        }
    })
    .await;
    let (max_open, saw_cursor) = (
        max_open.load(std::sync::atomic::Ordering::Relaxed),
        saw_cursor.load(std::sync::atomic::Ordering::Relaxed),
    );
    assert!(
        max_open <= 3,
        "never more than max_queued_sessions open at once: {max_open}"
    );
    assert!(saw_cursor, "a durable cursor was persisted between pages");
    assert_eq!(
        env.count("SELECT count(*) FROM sessions WHERE index_state = 'indexed'")
            .await,
        9
    );
    assert!(env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type='spool-control' AND idempotency_key LIKE 'cont:%'").await >= 2, "pages continued through scheduled continuation tasks");
    sys.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// reconciliation (spec 11.4): no workers, passes driven by hand
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_enqueues_desired_work_that_has_no_task() {
    let env = Env::new().await;
    env.omp(
        "r.jsonl",
        "recon",
        &["catalog updated but enqueue failed"],
        T0 - DAY,
    );
    let d = spool::discovery::DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    );
    d.run_auto("omp", Arc::new(Default::default()))
        .await
        .unwrap();
    let r = reconciler(&env);
    assert_eq!(env.jobs_count("SELECT count(*) FROM Jobs").await, 0);
    let rep = r.pass().await.unwrap();
    assert_eq!(rep.submitted, 1);
    let s = env.session_by_native("recon").await.unwrap();
    let tid = s.task_id.clone().unwrap();
    assert_eq!(
        env.jobs_count(&format!("SELECT count(*) FROM Jobs WHERE id = '{tid}'"))
            .await,
        1
    );
    // a second pass does not enqueue a duplicate while the task is live
    assert_eq!(r.pass().await.unwrap().submitted, 0);
    assert_eq!(
        env.jobs_count("SELECT count(*) FROM Jobs WHERE job_type = 'spool-ingest'")
            .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_repairs_a_session_whose_recorded_task_is_gone() {
    let env = Env::new().await;
    env.omp("r.jsonl", "lost", &["task row lost"], T0 - DAY);
    let d = spool::discovery::DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    );
    d.run_auto("omp", Arc::new(Default::default()))
        .await
        .unwrap();
    let sid = env.session_by_native("lost").await.unwrap().id;
    catalog::set_task(&env.index, &sid, Some("01HZZZZZZZZZZZZZZZZZZZZZZZ"))
        .await
        .unwrap(); // enqueue "succeeded" but the row never existed
    let r = reconciler(&env);
    let rep = r.pass().await.unwrap();
    assert_eq!(rep.repaired_tasks, 1);
    assert_eq!(rep.submitted, 1);
    let s = catalog::session(&env.index, &sid).await.unwrap().unwrap();
    assert_ne!(s.task_id.as_deref(), Some("01HZZZZZZZZZZZZZZZZZZZZZZZ"));
    assert_eq!(
        env.jobs_count(&format!(
            "SELECT count(*) FROM Jobs WHERE id = '{}'",
            s.task_id.unwrap()
        ))
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_finds_incomplete_embedding_coverage_only_when_the_provider_can_serve_it() {
    let env = Env::new().await;
    env.omp(
        "c.jsonl",
        "cover",
        &["vector gap one", "vector gap two"],
        T0 - DAY,
    );
    let sys = env.start().await;
    env.wait_state("cover", "indexed", 30).await;
    wait_until(20, "vectors", || async {
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    sys.shutdown(Duration::from_secs(15)).await;
    // lose the vectors (e.g. profile migration / interrupted embedding)
    sqlx::query("DELETE FROM embeddings")
        .execute(&env.index.writer)
        .await
        .unwrap();
    assert!(env.emb.vector_coverage().await.unwrap().pending_chunks > 0);
    let r = reconciler(&env);
    // provider blocked => no work is created
    env.provider.set_fault(Some(Fault::Auth));
    let _ = env
        .emb
        .embed_documents(
            vec![([1u8; 32], "probe".into())],
            &std::sync::atomic::AtomicBool::new(false),
        )
        .await; // trips the auth block
    env.clock.advance_ms(20 * 60 * 1000);
    assert_eq!(
        r.pass().await.unwrap().submitted,
        0,
        "provider-blocked state is respected"
    );
    // provider healthy again => the gap is found and a task is enqueued
    env.provider.set_fault(None);
    env.emb.reset_auth_block();
    let r2 = reconciler(&env);
    let rep = r2.pass().await.unwrap();
    assert_eq!(rep.submitted, 1, "{rep:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_skips_excluded_disabled_and_missing_sessions() {
    let env = Env::new().await;
    for (n, f) in [("ex", "ex.jsonl"), ("mi", "mi.jsonl"), ("ok", "ok.jsonl")] {
        env.omp(f, n, &["hello"], T0 - DAY);
    }
    let d = spool::discovery::DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    );
    d.run_auto("omp", Arc::new(Default::default()))
        .await
        .unwrap();
    let ex = env.session_by_native("ex").await.unwrap().id;
    catalog::exclude_session(&env.index, &ex, "x", T0)
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET source_status='missing' WHERE native_id='mi'")
        .execute(&env.index.writer)
        .await
        .unwrap();
    let r = reconciler(&env);
    assert_eq!(
        r.pass().await.unwrap().submitted,
        1,
        "only the healthy session is enqueued"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operations_that_never_reached_a_task_are_failed_not_left_dangling() {
    let env = Env::new().await;
    let (op, _) = spool::jobs::ops::begin(
        &env.index,
        "index_session",
        &json!({"session_id": "x"}),
        Some("dangling"),
        T0,
    )
    .await
    .unwrap();
    env.clock.advance_ms(10 * 60 * 1000);
    let r = reconciler(&env);
    r.pass().await.unwrap();
    let op = spool::jobs::ops::get(&env.index, &op.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (op.state.as_str(), op.error.as_deref()),
        ("failed", Some("not_accepted"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciliation_reserves_one_of_every_five_starts_for_waiting_backfill() {
    let env = Env::new().await;
    let insert = |id: String, class: &'static str, n: i64| {
        let env = &env;
        async move {
            sqlx::query("INSERT INTO sessions(id, source_id, native_id, discovered_at, eligibility, index_state, last_activity_at) VALUES (?, 'omp', ?, ?, ?, 'pending', ?)").bind(&id).bind(&id).bind(T0).bind(class).bind(T0 - n).execute(&env.index.writer).await.unwrap();
            sqlx::query("INSERT INTO source_files(session_id, path, dev, ino, size, mtime_ms) VALUES (?, ?, 1, ?, 10, 1)").bind(&id).bind(format!("/nowhere/{id}")).bind(n).execute(&env.index.writer).await.unwrap();
        }
    };
    for i in 0..10 {
        insert(format!("a{i}"), "active", i).await;
    }
    for i in 0..4 {
        insert(format!("b{i}"), "manual", i).await;
    }
    let r = reconciler(&env);
    assert_eq!(r.pass().await.unwrap().submitted, 14);
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT CAST(job AS TEXT) FROM Jobs WHERE job_type = 'spool-ingest' ORDER BY rowid",
    )
    .fetch_all(&env.jobs_pool)
    .await
    .unwrap();
    let order: String = rows
        .iter()
        .map(|j| {
            serde_json::from_str::<serde_json::Value>(j).unwrap()["session_id"]
                .as_str()
                .unwrap()
                .chars()
                .next()
                .unwrap()
        })
        .collect();
    assert_eq!(
        order, "aaaabaaaabaabb",
        "positions 5 and 10 are reserved for backfill while active work is waiting"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tick_missed_while_stopped_is_caught_by_startup_reconciliation() {
    let env = Env::new().await; // discovery interval is an hour: no tick can fire during this test
    let sys = env.start().await;
    wait_until(20, "first discovery", || async {
        env.count("SELECT count(*) FROM sources WHERE discovery_watermark IS NOT NULL")
            .await
            == 1
    })
    .await;
    sys.shutdown(Duration::from_secs(15)).await;
    env.clock.advance_ms(60_000);
    env.omp(
        "missed.jsonl",
        "missed",
        &["written while the daemon was stopped", "reply"],
        T0 + 30_000,
    );
    let sys2 = env.start().await;
    env.wait_state("missed", "indexed", 30).await;
    sys2.shutdown(Duration::from_secs(15)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_whose_catalog_entry_is_gone_is_a_noop_and_the_control_worker_is_independent() {
    let env = Env::new().await;
    env.submitter()
        .push_index(
            IndexSession {
                payload_version: 1,
                session_id: "ss_unknown".into(),
                observed_source_revision: "x".into(),
                pipeline_fingerprint: "x".into(),
                embedding_profile_id: "x".into(),
                operation_id: None,
            },
            "k",
            None,
        )
        .await
        .unwrap();
    let sys = env.start().await;
    wait_until(30, "task done", || async {
        env.jobs_count(
            "SELECT count(*) FROM Jobs WHERE job_type='spool-ingest' AND status = 'Done'",
        )
        .await
            == 1
    })
    .await;
    assert_eq!(env.count("SELECT count(*) FROM snapshots").await, 0);
    wait_until(20, "both workers up", || async {
        let h = sys.health();
        h.ingest.state == WorkerState::Running && h.control.state == WorkerState::Running
    })
    .await;
    sys.shutdown(Duration::from_secs(15)).await;
}

// ---------------------------------------------------------------------------------------------
// maintenance
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_sweeps_leases_purges_unreferenced_vectors_and_prunes_completed_jobs() {
    let env = Env::new().await;
    env.omp(
        "m.jsonl",
        "maint",
        &["referenced vector text", "reply"],
        T0 - DAY,
    );
    let sys = env.start().await;
    env.wait_state("maint", "indexed", 30).await;
    wait_until(20, "vectors", || async {
        env.emb.vector_coverage().await.unwrap().pending_chunks == 0
    })
    .await;
    let profile = env.emb.profile().id();
    let referenced = env.count("SELECT count(*) FROM embeddings").await;
    // an unreferenced vector (old) and a fresh one (inside the grace period)
    for (h, age) in [(1u8, 2 * 3_600_000i64), (2u8, 0)] {
        sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?, ?, 32, zeroblob(128), ?)").bind(&profile).bind(vec![h; 32]).bind(T0 - age).execute(&env.index.writer).await.unwrap();
    }
    // an expired lease
    sqlx::query("INSERT INTO read_leases(id, expires_at, state, bytes_charged, created_at) VALUES ('l1', ?, 'live', 0, ?)").bind(T0 - 1000).bind(T0 - 5000).execute(&env.index.writer).await.unwrap();

    let routine = sys
        .submit_maintenance(MaintenanceScope::Routine, None)
        .await
        .unwrap();
    wait_until(30, "maintenance done", || async {
        spool::jobs::ops::get(&env.index, &routine.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state
            == "done"
    })
    .await;
    assert_eq!(
        env.count("SELECT count(*) FROM embeddings").await,
        referenced + 1,
        "old unreferenced purged, fresh one kept for the grace period, referenced kept"
    );
    assert_eq!(
        env.count("SELECT count(*) FROM read_leases WHERE id = 'l1'")
            .await,
        0,
        "expired lease swept"
    );

    let purge = sys
        .submit_maintenance(MaintenanceScope::PurgeUnreferenced, None)
        .await
        .unwrap();
    wait_until(30, "purge done", || async {
        spool::jobs::ops::get(&env.index, &purge.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state
            == "done"
    })
    .await;
    assert_eq!(
        env.count("SELECT count(*) FROM embeddings").await,
        referenced,
        "explicit purge removes every unreferenced vector"
    );
    assert_eq!(
        fts_hits(&env, "referenced").await,
        1,
        "searchable text untouched"
    );
    sys.shutdown(Duration::from_secs(15)).await;
}
