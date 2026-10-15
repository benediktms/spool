//! EmbeddingService behaviour over the deterministic FakeProvider.

use super::*;
use spool::clock::{ManualClock, SharedClock};
use spool::embeddings::fake::Fault;
use spool::embeddings::vectors::{decode_le_f32, dot};
use spool::embeddings::{
    EmbedErrorKind as K, EmbeddingService, EmbeddingState, FakeProvider, Purpose,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

const DIMS: usize = 32;

struct Harness {
    _dir: tempfile::TempDir,
    index: Index,
    clock: ManualClock,
    fake: Arc<FakeProvider>,
    svc: EmbeddingService,
}

async fn harness_with(cfg: spool::config::EmbeddingConfig) -> Harness {
    let (dir, index) = open_index().await;
    let clock = ManualClock::new(NOON);
    let fake = Arc::new(FakeProvider::new(DIMS));
    let svc = service_over(&index, &clock, &fake, cfg);
    Harness {
        _dir: dir,
        index,
        clock,
        fake,
        svc,
    }
}

fn service_over(
    index: &Index,
    clock: &ManualClock,
    fake: &Arc<FakeProvider>,
    cfg: spool::config::EmbeddingConfig,
) -> EmbeddingService {
    let shared: SharedClock = Arc::new(clock.clone());
    EmbeddingService::new(index.clone(), cfg, search_cfg(), shared, fake.clone())
}

async fn harness() -> Harness {
    harness_with(emb_cfg()).await
}

/// Stored vectors are re-normalized by the validator, so compare with a tolerance.
fn close(a: &[f32], b: &[f32]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-5)
}

fn docs(range: std::ops::Range<u32>) -> Vec<([u8; 32], String)> {
    range
        .map(|n| {
            (
                digest_of(n),
                format!("document number {n} about sqlite storage"),
            )
        })
        .collect()
}

async fn states(index: &Index) -> Vec<(String, String, i64)> {
    sqlx::query_as(
        "SELECT purpose, state, reserved_tokens FROM embedding_requests ORDER BY created_at, id",
    )
    .fetch_all(&index.reader)
    .await
    .unwrap()
}

async fn wait_for_state(index: &Index, state: &str) {
    for _ in 0..100 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM embedding_requests WHERE state = ?")
            .bind(state)
            .fetch_one(&index.reader)
            .await
            .unwrap();
        if n > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no request reached state {state}");
}

#[test]
fn concept_vectors_express_paraphrase_without_shared_words() {
    let f = FakeProvider::new(DIMS);
    let a = f.vector_for("my automobile broke");
    let b = f.vector_for("the sedan crashed");
    let c = f.vector_for("a recipe for dinner");
    assert!(
        dot(&a, &b) > 0.3,
        "paraphrases should be close: {}",
        dot(&a, &b)
    );
    assert!(
        dot(&a, &c) < 0.1,
        "unrelated texts should be far: {}",
        dot(&a, &c)
    );
    assert!((dot(&a, &a) - 1.0).abs() < 1e-5);
    assert_eq!(f.vector_for("same"), f.vector_for("same"));
}

#[tokio::test]
async fn ensure_profile_upserts_and_activates() {
    let h = harness().await;
    let id = h.svc.ensure_profile().await.unwrap();
    assert_eq!(id, h.svc.profile().id());
    assert_eq!(h.svc.ensure_profile().await.unwrap(), id);
    let active: String =
        sqlx::query_scalar("SELECT value FROM app_meta WHERE key='active_embedding_profile'")
            .fetch_one(&h.index.reader)
            .await
            .unwrap();
    assert_eq!(active, id);
    let (model, dims): (String, i64) =
        sqlx::query_as("SELECT model, dimensions FROM embedding_profiles WHERE id=?")
            .bind(&id)
            .fetch_one(&h.index.reader)
            .await
            .unwrap();
    assert_eq!((model.as_str(), dims), ("fake-concepts", DIMS as i64));
    // epoch is bumped only when the active profile actually changes
    let e = h.index.epoch().await.unwrap();
    h.svc.ensure_profile().await.unwrap();
    assert_eq!(h.index.epoch().await.unwrap(), e);

    let mut p = h.svc.profile().clone();
    let base = p.id();
    p.generation += 1;
    assert_ne!(p.id(), base);
    p.generation -= 1;
    p.endpoint_identity = "other.example".into();
    assert_ne!(p.id(), base);
    p.endpoint_identity = h.svc.profile().endpoint_identity.clone();
    p.dimensions += 1;
    assert_ne!(p.id(), base);
}

#[tokio::test]
async fn documents_are_committed_cached_and_batched() {
    let h = harness_with(spool::config::EmbeddingConfig {
        batch_max_inputs: 3,
        ..emb_cfg()
    })
    .await;
    h.svc.ensure_profile().await.unwrap();
    let epoch0 = h.index.epoch().await.unwrap();
    let cancel = AtomicBool::new(false);

    let mut inputs = docs(0..7);
    inputs.push((digest_of(100), "   ".into())); // empty input is never sent
    inputs.push((digest_of(0), "duplicate hash in the same call".into()));
    let p = h.svc.embed_documents(inputs, &cancel).await.unwrap();
    assert_eq!((p.embedded, p.cached, p.deferred, p.rejected), (7, 0, 0, 1));
    assert_eq!(h.fake.batch_sizes(), vec![3, 3, 1]);
    assert!(p.usage_tokens > 0);
    assert!(h.fake.seen_inputs().iter().all(|t| !t.trim().is_empty()));
    assert_eq!(
        h.index.epoch().await.unwrap(),
        epoch0 + 3,
        "one epoch bump per committed batch"
    );

    let (dims, blob): (i64, Vec<u8>) = sqlx::query_as(
        "SELECT dimensions, vec FROM embeddings WHERE profile_id=? AND input_hash=?",
    )
    .bind(h.svc.profile().id())
    .bind(digest_of(2).to_vec())
    .fetch_one(&h.index.reader)
    .await
    .unwrap();
    let v = decode_le_f32(&blob).unwrap();
    assert_eq!((dims as usize, v.len()), (DIMS, DIMS));
    assert!(close(&v, &h.fake.vector_for(&docs(2..3)[0].1)));

    // second call: everything is cached, nothing is sent
    let calls = h.fake.calls();
    let p = h.svc.embed_documents(docs(0..7), &cancel).await.unwrap();
    assert_eq!((p.embedded, p.cached), (0, 7));
    assert_eq!(h.fake.calls(), calls);
    // mixed: only the new input is embedded
    let p = h.svc.embed_documents(docs(5..9), &cancel).await.unwrap();
    assert_eq!((p.embedded, p.cached), (2, 2));
    assert_eq!(h.fake.calls(), calls + 1);
}

#[tokio::test]
async fn token_cap_splits_batches_and_oversized_input_is_rejected() {
    let h = harness_with(spool::config::EmbeddingConfig {
        batch_max_tokens: 1200,
        ..emb_cfg()
    })
    .await;
    let one = "word ".repeat(500); // ~500 tokens
    let huge = "word ".repeat(9000); // above the 8191-token provider per-input limit
    let inputs = vec![
        (digest_of(1), one.clone()),
        (digest_of(2), format!("{one}x")),
        (digest_of(3), format!("{one}y")),
        (digest_of(4), huge),
    ];
    let p = h
        .svc
        .embed_documents(inputs, &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!((p.embedded, p.rejected), (3, 1));
    assert_eq!(
        h.fake.batch_sizes(),
        vec![2, 1],
        "1200-token cap fits two 500-token inputs per request"
    );
}

#[tokio::test]
async fn cancel_flag_is_honored_between_batches() {
    let h = harness_with(spool::config::EmbeddingConfig {
        batch_max_inputs: 2,
        ..emb_cfg()
    })
    .await;
    let cancel = AtomicBool::new(true);
    let p = h.svc.embed_documents(docs(0..5), &cancel).await.unwrap();
    assert!(p.cancelled);
    assert_eq!((p.embedded, p.deferred), (0, 5));
    assert_eq!(h.fake.calls(), 0);
}

#[tokio::test]
async fn provider_faults_are_rejected_and_nothing_is_committed() {
    for (fault, kind, retryable) in [
        (Fault::WrongDimensions, K::UnsupportedModel, false),
        (Fault::WrongCount(-1), K::BadResponse, true),
        (Fault::WrongCount(2), K::BadResponse, true),
        (Fault::DuplicateIndex, K::BadResponse, false),
        (Fault::Nan, K::BadResponse, false),
        (Fault::ZeroNorm, K::BadResponse, false),
    ] {
        let h = harness().await;
        h.fake.fail_next(fault.clone());
        let e = h
            .svc
            .embed_documents(docs(0..3), &AtomicBool::new(false))
            .await
            .unwrap_err();
        assert_eq!((e.kind, e.retryable), (kind, retryable), "{fault:?}");
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM embeddings")
            .fetch_one(&h.index.reader)
            .await
            .unwrap();
        assert_eq!(n, 0, "{fault:?}");
        let rows = states(&h.index).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1, "ambiguous",
            "a malformed answer was still processed and counts against the budget"
        );
    }
}

#[tokio::test]
async fn reordered_indices_are_mapped_by_index() {
    let h = harness().await;
    h.fake.set_fault(Some(Fault::Reorder));
    let p = h
        .svc
        .embed_documents(docs(0..4), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!(p.embedded, 4);
    for (hash, text) in docs(0..4) {
        let blob: Vec<u8> = sqlx::query_scalar("SELECT vec FROM embeddings WHERE input_hash=?")
            .bind(hash.to_vec())
            .fetch_one(&h.index.reader)
            .await
            .unwrap();
        assert!(close(
            &decode_le_f32(&blob).unwrap(),
            &h.fake.vector_for(&text)
        ));
    }
    let q = h
        .svc
        .embed_query("hello there", Duration::from_secs(2))
        .await
        .unwrap();
    assert!((dot(&q, &q) - 1.0).abs() < 1e-5);
}

#[tokio::test]
async fn budget_defers_only_ingestion_and_resets_on_utc_day() {
    let h = harness().await;
    let text = "alpha beta gamma delta";
    let n = h.svc.count_tokens(text) as u64;
    let cfg = spool::config::EmbeddingConfig {
        batch_max_inputs: 1,
        daily_ingestion_token_budget: 2 * n + 1,
        ..emb_cfg()
    };
    let h = Harness {
        svc: service_over(&h.index, &h.clock, &h.fake, cfg.clone()),
        ..h
    };
    let inputs: Vec<_> = (0..3).map(|i| (digest_of(i), text.to_string())).collect();
    // identical texts but distinct hashes: exact-hash cache keys, so each is embedded
    let p = h
        .svc
        .embed_documents(inputs.clone(), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!((p.embedded, p.deferred, p.budget_exhausted), (2, 1, true));
    assert_eq!(p.resets_at_ms, Some(NEXT_MIDNIGHT));
    let st = h.svc.status();
    assert_eq!(st.budget.ingest.used, 2 * n);
    assert!(st.budget.ingest.limit == 2 * n + 1 && !st.budget.ingest.exhausted);
    assert_eq!(
        (
            st.budget.resets_at_ms,
            st.budget.timezone.as_str(),
            st.budget.day.as_str()
        ),
        (NEXT_MIDNIGHT, "UTC", "2025-10-12")
    );

    // the interactive allowance is independent
    h.svc
        .embed_query("still searchable", Duration::from_secs(2))
        .await
        .unwrap();

    // restart: a fresh service over the same database derives usage from the durable rows
    let svc2 = service_over(&h.index, &h.clock, &h.fake, cfg.clone());
    svc2.ensure_profile().await.unwrap();
    assert_eq!(svc2.status().budget.ingest.used, 2 * n);
    let p = svc2
        .embed_documents(inputs.clone(), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!(
        (p.embedded, p.deferred),
        (0, 1),
        "restart does not forget what was spent today"
    );

    // still before midnight: deterministic under the injected clock
    h.clock.set_ms(NEXT_MIDNIGHT - 1);
    assert_eq!(svc2.status().budget.day, "2025-10-12");
    assert_eq!(svc2.status().budget.ingest.used, 2 * n);
    h.clock.set_ms(NEXT_MIDNIGHT);
    let st = svc2.status();
    assert_eq!(
        (st.budget.day.as_str(), st.budget.ingest.used),
        ("2025-10-13", 0)
    );
    let p = svc2
        .embed_documents(inputs, &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!((p.embedded, p.deferred), (1, 0));
}

/// Spec 12.3: the budget day follows the operator-configured zone (UTC or a fixed offset), per injected clock.
#[tokio::test]
async fn budget_day_follows_the_configured_fixed_offset() {
    // 2025-10-12T18:30:00Z is local midnight at +05:30; NOON (12:00Z) is 17:30 local on the 12th.
    const LOCAL_MIDNIGHT: i64 = NOON + 6 * 3_600_000 + 30 * 60_000;
    let text = "alpha beta gamma delta";
    let h0 = harness().await;
    let n = h0.svc.count_tokens(text) as u64;
    let cfg = spool::config::EmbeddingConfig {
        budget_timezone: "+05:30".into(),
        batch_max_inputs: 1,
        daily_ingestion_token_budget: 2 * n + 1,
        ..emb_cfg()
    };
    let h = Harness {
        svc: service_over(&h0.index, &h0.clock, &h0.fake, cfg.clone()),
        ..h0
    };
    let b = h.svc.status().budget;
    assert_eq!(
        (b.timezone.as_str(), b.day.as_str(), b.resets_at_ms),
        ("+05:30", "2025-10-12", LOCAL_MIDNIGHT)
    );
    let inputs: Vec<_> = (0..3).map(|i| (digest_of(i), text.to_string())).collect();
    let p = h
        .svc
        .embed_documents(inputs.clone(), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!((p.embedded, p.deferred), (2, 1));
    assert_eq!(p.resets_at_ms, Some(LOCAL_MIDNIGHT));

    // restart in the same zone: the spent budget is derived from the durable rows
    let svc2 = service_over(&h.index, &h.clock, &h.fake, cfg.clone());
    svc2.ensure_profile().await.unwrap();
    assert_eq!(svc2.status().budget.ingest.used, 2 * n);

    // one millisecond before local midnight it is still the 12th; at local midnight the budget resets
    // (a UTC budget would not reset for another 5.5 hours)
    h.clock.set_ms(LOCAL_MIDNIGHT - 1);
    let b = svc2.status().budget;
    assert_eq!((b.day.as_str(), b.ingest.used), ("2025-10-12", 2 * n));
    h.clock.set_ms(LOCAL_MIDNIGHT);
    let b = svc2.status().budget;
    assert_eq!((b.day.as_str(), b.ingest.used), ("2025-10-13", 0));
    assert_eq!(b.resets_at_ms, LOCAL_MIDNIGHT + 86_400_000);
    let p = svc2
        .embed_documents(inputs, &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!((p.embedded, p.deferred), (1, 0));

    // Switching the same database back to UTC re-derives the day from the rows: all of today's UTC spend counts.
    let utc = service_over(&h.index, &h.clock, &h.fake, emb_cfg());
    utc.ensure_profile().await.unwrap();
    let b = utc.status().budget;
    assert_eq!((b.timezone.as_str(), b.day.as_str()), ("UTC", "2025-10-12"));
    assert!(b.ingest.used > 2 * n, "{}", b.ingest.used);

    // only UTC and fixed offsets are accepted; a named zone is rejected with the reason
    let mut c = spool::config::Config::default();
    c.embedding.budget_timezone = "-08:00".into();
    if let Err(e) = c.validate() {
        assert!(
            !e.to_string().contains("budget_timezone"),
            "a fixed offset is valid: {e}"
        );
    }
    c.embedding.budget_timezone = "Europe/Berlin".into();
    let e = c.validate().unwrap_err().to_string();
    assert!(
        e.contains("budget_timezone") && e.contains("fixed offset"),
        "{e}"
    );
}

#[tokio::test]
async fn query_budget_exhaustion_is_a_typed_error_and_leaves_ingestion_alone() {
    let cfg = spool::config::EmbeddingConfig {
        daily_query_token_budget: 3,
        ..emb_cfg()
    };
    let h = harness_with(cfg).await;
    let e = h
        .svc
        .embed_query(
            "this query has more than three tokens in it",
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::BudgetExhausted);
    assert_eq!(h.fake.calls(), 0);
    let p = h
        .svc
        .embed_documents(docs(0..2), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!(p.embedded, 2);
}

#[tokio::test]
async fn interrupted_requests_survive_a_kill_and_count_against_the_budget() {
    let h = harness().await;
    // what a killed process leaves behind: a dispatched row and a never-dispatched reservation
    for (id, state, tokens) in [
        ("er_dead1", "dispatched", 900_000i64),
        ("er_dead2", "reserved", 50_000),
    ] {
        sqlx::query("INSERT INTO embedding_requests(id, profile_id, purpose, budget_day, reserved_tokens, state, created_at) VALUES (?,?, 'ingest', '2025-10-12', ?, ?, ?)")
            .bind(id)
            .bind(h.svc.profile().id())
            .bind(tokens)
            .bind(state)
            .bind(NOON)
            .execute(&h.index.writer)
            .await
            .unwrap();
    }
    h.svc.ensure_profile().await.unwrap();
    assert_eq!(h.svc.status().budget.ingest.used, 950_000);
    // first use recovers them: dispatched -> ambiguous (still counted), reserved -> failed (not sent, released)
    let p = h
        .svc
        .embed_documents(docs(0..1), &AtomicBool::new(false))
        .await
        .unwrap();
    assert_eq!(p.embedded, 1);
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, state FROM embedding_requests WHERE id LIKE 'er_dead%' ORDER BY id",
    )
    .fetch_all(&h.index.reader)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("er_dead1".into(), "ambiguous".into()),
            ("er_dead2".into(), "failed".into())
        ]
    );
    let used = h.svc.status().budget.ingest.used;
    assert!((900_000..950_000).contains(&used), "{used}");
}

#[tokio::test]
async fn request_state_transitions_are_recorded() {
    let h = harness().await;
    h.svc
        .embed_documents(docs(0..1), &AtomicBool::new(false))
        .await
        .unwrap();
    let (state, reserved, actual, done): (String, i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT state, reserved_tokens, actual_tokens, completed_at FROM embedding_requests",
    )
    .fetch_one(&h.index.reader)
    .await
    .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(Some(reserved), actual);
    assert_eq!(done, Some(NOON));

    h.fake.fail_next(Fault::Server);
    h.clock.advance_ms(1);
    h.svc
        .embed_documents(docs(1..2), &AtomicBool::new(false))
        .await
        .unwrap_err();
    h.clock.advance_ms(1);
    h.fake.fail_next(Fault::Network);
    h.svc
        .embed_documents(docs(2..3), &AtomicBool::new(false))
        .await
        .unwrap_err();
    let rows = states(&h.index).await;
    let st: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
    assert_eq!(st, vec!["completed", "failed", "ambiguous"]);
    // failed (definitely not processed) is not counted; ambiguous is
    let tokens = rows[2].2 as u64;
    assert_eq!(h.svc.status().budget.ingest.used, rows[0].2 as u64 + tokens);
}

#[tokio::test]
async fn ingestion_and_query_have_separate_concurrency_permits() {
    // ingestion permit = 1 even when several batches are submitted at once
    let h = harness().await;
    h.fake.set_delay(Duration::from_millis(150));
    let svc = Arc::new(h.svc);
    let cancel = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for i in 0..3u32 {
        let (svc, cancel) = (svc.clone(), cancel.clone());
        tasks.push(tokio::spawn(async move {
            svc.embed_documents(docs(i * 10..i * 10 + 2), &cancel)
                .await
                .unwrap()
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(h.fake.max_in_flight(Purpose::Ingest), 1);

    // query permits = 2
    let mut tasks = Vec::new();
    for i in 0..5 {
        let svc = svc.clone();
        tasks.push(tokio::spawn(async move {
            svc.embed_query(&format!("distinct query {i}"), Duration::from_secs(5))
                .await
                .unwrap()
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(h.fake.max_in_flight(Purpose::Query), 2);

    // a slow backfill batch does not make an interactive query wait behind it
    h.fake.set_delay(Duration::from_millis(400));
    let started = Instant::now();
    let bg = {
        let svc = svc.clone();
        tokio::spawn(async move {
            svc.embed_documents(docs(500..502), &AtomicBool::new(false))
                .await
                .unwrap()
        })
    };
    let q = {
        let svc = svc.clone();
        tokio::spawn(async move {
            svc.embed_query("interactive while backfilling", Duration::from_secs(5))
                .await
                .unwrap()
        })
    };
    q.await.unwrap();
    bg.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "ran serially: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn auth_failure_pauses_hosted_work_until_reset() {
    let h = harness().await;
    h.fake.set_fault(Some(Fault::Auth));
    let e = h
        .svc
        .embed_documents(docs(0..2), &AtomicBool::new(false))
        .await
        .unwrap_err();
    assert_eq!((e.kind, e.retryable), (K::Auth, false));
    assert_eq!(h.fake.calls(), 1);
    assert_eq!(h.svc.status().state, EmbeddingState::AuthBlocked);

    // no private text is sent again, from either path, however many times they are called
    for _ in 0..5 {
        assert_eq!(
            h.svc
                .embed_documents(docs(0..2), &AtomicBool::new(false))
                .await
                .unwrap_err()
                .kind,
            K::Auth
        );
        assert_eq!(
            h.svc
                .embed_query("anything", Duration::from_secs(1))
                .await
                .unwrap_err()
                .kind,
            K::Auth
        );
    }
    assert_eq!(h.fake.calls(), 1);

    h.fake.set_fault(None);
    h.svc.reset_auth_block();
    assert_eq!(h.svc.status().state, EmbeddingState::Ready);
    assert_eq!(
        h.svc
            .embed_documents(docs(0..2), &AtomicBool::new(false))
            .await
            .unwrap()
            .embedded,
        2
    );
}

#[tokio::test]
async fn explicit_probe_can_clear_an_auth_block() {
    let h = harness().await;
    h.fake.fail_next(Fault::Auth);
    h.svc
        .embed_query("x y z", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(h.svc.status().state, EmbeddingState::AuthBlocked);
    let out = h.svc.probe().await.unwrap();
    assert_eq!(out.dimensions, DIMS);
    assert_eq!(h.svc.status().state, EmbeddingState::Ready);
    assert!(
        h.fake.seen_inputs().last().unwrap().contains("probe"),
        "probe sends only its fixed text"
    );
}

#[tokio::test]
async fn circuit_breaker_fails_fast_during_a_known_outage() {
    let h = harness().await;
    h.fake.set_fault(Some(Fault::Server));
    for i in 0..3 {
        let e = h
            .svc
            .embed_query(&format!("q{i}"), Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!((e.kind, e.retryable), (K::Server, true));
    }
    assert_eq!(h.fake.calls(), 3);
    let t = Instant::now();
    let e = h
        .svc
        .embed_query("q-next", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::Unavailable);
    assert!(e.retry_after.unwrap() <= Duration::from_secs(30));
    assert!(t.elapsed() < Duration::from_millis(100));
    assert_eq!(h.fake.calls(), 3, "open circuit sends nothing");
    assert!(matches!(h.svc.status().state, EmbeddingState::Degraded(_)));
    assert_eq!(
        h.svc
            .embed_documents(docs(0..1), &AtomicBool::new(false))
            .await
            .unwrap_err()
            .kind,
        K::Unavailable
    );

    // cooldown passes (injected clock) -> exactly one half-open probe; success closes the circuit
    h.clock.advance_ms(31_000);
    h.fake.set_fault(None);
    h.svc
        .embed_query("q-after", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(h.fake.calls(), 4);
    assert_eq!(h.svc.status().state, EmbeddingState::Ready);
}

#[tokio::test]
async fn rate_limit_opens_the_circuit_for_retry_after() {
    let h = harness().await;
    h.fake
        .fail_next(Fault::RateLimited(Duration::from_secs(10)));
    let e = h
        .svc
        .embed_query("first", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind, e.retry_after),
        (K::RateLimited, Some(Duration::from_secs(10)))
    );
    let e = h
        .svc
        .embed_query("second", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::Unavailable);
    assert_eq!(h.fake.calls(), 1);
    h.clock.advance_ms(10_001);
    h.svc
        .embed_query("third", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(h.fake.calls(), 2);
}

#[tokio::test]
async fn hung_provider_is_bounded_by_the_deadline_and_counted_ambiguous() {
    let h = harness().await;
    h.fake.set_fault(Some(Fault::Hang));
    let t = Instant::now();
    let e = h
        .svc
        .embed_query("will hang", Duration::from_millis(200))
        .await
        .unwrap_err();
    assert!(t.elapsed() < Duration::from_secs(2));
    assert_eq!((e.kind, e.ambiguous, e.retryable), (K::Timeout, true, true));
    wait_for_state(&h.index, "ambiguous").await;
    assert!(
        h.svc.status().budget.query.used > 0,
        "an outcome-unknown request counts against the budget"
    );
    // the permit was released: other queries are not wedged behind the hung one
    h.fake.set_fault(None);
    h.clock.advance_ms(1);
    h.svc.reset_auth_block();
    h.svc
        .embed_query("fine now", Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn query_cache_is_keyed_bounded_and_expires() {
    let h = harness().await;
    let a = h
        .svc
        .embed_query("cached question", Duration::from_secs(1))
        .await
        .unwrap();
    let b = h
        .svc
        .embed_query("cached question", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(a, b);
    assert_eq!(h.fake.calls_for(Purpose::Query), 1);
    h.clock.advance_ms(601_000); // default TTL is 600 s
    h.svc
        .embed_query("cached question", Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(h.fake.calls_for(Purpose::Query), 2);
    // never persisted
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM embeddings")
        .fetch_one(&h.index.reader)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        h.svc
            .embed_query("   ", Duration::from_secs(1))
            .await
            .unwrap_err()
            .kind,
        K::BadResponse
    );
}

#[tokio::test]
async fn gate_is_checked_before_any_provider_work() {
    for (cfg, kind, state) in [
        (
            spool::config::EmbeddingConfig {
                enabled: false,
                ..emb_cfg()
            },
            K::Disabled,
            EmbeddingState::Disabled,
        ),
        (
            spool::config::EmbeddingConfig {
                hosted_processing_approved: false,
                ..emb_cfg()
            },
            K::NotApproved,
            EmbeddingState::NotApproved,
        ),
    ] {
        let (dir, index) = open_index().await;
        let fake = Arc::new(FakeProvider::new(DIMS).hosted(true));
        let svc = EmbeddingService::new(
            index.clone(),
            cfg,
            search_cfg(),
            Arc::new(ManualClock::new(NOON)),
            fake.clone(),
        );
        assert_eq!(svc.status().state, state);
        assert_eq!(
            svc.embed_query("q", Duration::from_secs(1))
                .await
                .unwrap_err()
                .kind,
            kind
        );
        assert_eq!(
            svc.embed_documents(docs(0..2), &AtomicBool::new(false))
                .await
                .unwrap_err()
                .kind,
            kind
        );
        assert_eq!(svc.probe().await.unwrap_err().kind, kind);
        assert_eq!(fake.calls(), 0);
        drop(dir);
    }
}

#[tokio::test]
async fn vector_coverage_counts_active_chunks_without_vectors() {
    let h = harness().await;
    let w = &h.index.writer;
    sqlx::query("INSERT INTO sources(id, kind, roots_json) VALUES ('s','omp','[]')")
        .execute(w)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO sessions(id, source_id, native_id, discovered_at) VALUES ('ss1','s','n1',0)",
    )
    .execute(w)
    .await
    .unwrap();
    sqlx::query("INSERT INTO snapshots(id, session_id, kind, state, source_revision, fingerprint, generation, prefix_end, created_at) VALUES ('sn1','ss1','indexed','published','r','f',1,0,0)").execute(w).await.unwrap();
    sqlx::query("UPDATE sessions SET active_snapshot_id='sn1' WHERE id='ss1'")
        .execute(w)
        .await
        .unwrap();
    for n in 0..4u32 {
        sqlx::query("INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, rendered_tokens) VALUES (?, 'sn1','ss1','r',?, 'body', ?, 1)")
            .bind(format!("c{n}"))
            .bind(n as i64)
            .bind(digest_of(n).to_vec())
            .execute(w)
            .await
            .unwrap();
    }
    h.svc
        .embed_documents(docs(0..2), &AtomicBool::new(false))
        .await
        .unwrap();
    let c = h.svc.vector_coverage().await.unwrap();
    assert_eq!(
        (c.total_chunks, c.embedded_chunks, c.pending_chunks),
        (4, 2, 2)
    );
}
