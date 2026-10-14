//! Provider outage/auth/budget outcome mapping (spec 11.7): published text stays searchable.

use crate::common::*;
use spool::embeddings::fake::Fault;
use spool::ingest::engine::{IndexOutcome, IngestErrorKind};
use std::time::Duration;

async fn failing(fault: Fault) -> (Env, String, spool::ingest::engine::IngestError) {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(10)).await;
    env.provider.set_fault(Some(fault));
    let err = env.index(&sid).await.unwrap_err();
    // Text publication is independent of embeddings.
    assert!(env.active(&sid).await.is_some());
    assert!(env.fts("qwz3").await >= 1);
    assert_eq!(
        env.n("SELECT index_state = 'indexed' FROM sessions").await,
        1
    );
    assert_eq!(env.embedded_count().await, 0);
    (env, sid, err)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_outage_is_transient_and_recovers() {
    let (env, sid, err) = failing(Fault::Server).await;
    assert_eq!(
        (err.kind, err.retryable),
        (IngestErrorKind::Transient, true)
    );
    env.provider.set_fault(None);
    let out = env.index(&sid).await.unwrap();
    assert!(
        matches!(out, IndexOutcome::Embedded { embedded } if embedded > 0),
        "{out:?}"
    );
    assert_eq!(
        env.n("SELECT count(*) FROM chunks").await as usize,
        env.active_chunks(&sid).await.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limit_is_transient_with_retry_after() {
    let (_env, _sid, err) = failing(Fault::RateLimited(Duration::from_secs(7))).await;
    assert_eq!(
        (err.kind, err.retryable),
        (IngestErrorKind::Transient, true)
    );
    assert_eq!(err.retry_after, Some(Duration::from_secs(7)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_failure_is_transient() {
    let (_env, _sid, err) = failing(Fault::Network).await;
    assert_eq!(
        (err.kind, err.retryable),
        (IngestErrorKind::Transient, true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_failure_blocks_without_repeated_calls() {
    let (env, sid, err) = failing(Fault::Auth).await;
    assert_eq!(
        (err.kind, err.retryable),
        (IngestErrorKind::AuthBlocked, false)
    );
    let calls = env.provider.calls();
    let again = env.index(&sid).await.unwrap_err();
    assert_eq!(again.kind, IngestErrorKind::AuthBlocked);
    assert_eq!(
        env.provider.calls(),
        calls,
        "no rapid repeated calls while auth-blocked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_model_is_permanent() {
    let (_env, _sid, err) = failing(Fault::UnsupportedModel).await;
    assert_eq!(
        (err.kind, err.retryable),
        (IngestErrorKind::Permanent, false)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_exhaustion_defers_instead_of_failing() {
    let env = Env::with(|c| {
        c.chunking.target_new_tokens = 40;
        c.chunking.max_rendered_tokens = 90;
        c.chunking.overlap_tokens = 15;
        c.embedding.daily_ingestion_token_budget = 1;
    })
    .await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(10)).await;
    let err = env.index(&sid).await.unwrap_err();
    assert_eq!(err.kind, IngestErrorKind::BudgetDeferred);
    assert!(!err.retryable && err.retry_after.is_some(), "{err:?}");
    assert_eq!(env.provider.calls(), 0, "nothing sent beyond the budget");
    assert!(env.fts("qwz3").await >= 1);
    assert_eq!(
        env.n("SELECT index_state = 'indexed' FROM sessions").await,
        1,
        "not a permanent failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_embeddings_leave_lexical_search_working() {
    let env = Env::with(|c| c.embedding.enabled = false).await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(6)).await;
    let out = env.index(&sid).await.unwrap();
    let IndexOutcome::Published {
        chunks,
        embeddings_pending,
        ..
    } = out
    else {
        panic!("{out:?}")
    };
    assert_eq!(embeddings_pending, chunks);
    assert_eq!(env.provider.calls(), 0);
    assert!(env.fts("qwz3").await >= 1);
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp);
}
