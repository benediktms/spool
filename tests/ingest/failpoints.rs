//! Kill matrix (spec 19.4): a failpoint stops the run exactly where a process kill would; a FRESH
//! engine over the same storage must then converge without duplicates.

use crate::common::*;
use spool::ingest::engine::{IndexOutcome, IndexRequest, IngestEngine, IngestErrorKind};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn restarted(env: &Env) -> IngestEngine {
    IngestEngine::new(
        env.index.clone(),
        env.cfg.clone(),
        env.sources.clone(),
        env.emb.clone(),
        env.shared.clone(),
    )
}

async fn rerun(_env: &Env, e: &IngestEngine, sid: &str) -> IndexOutcome {
    e.index_session(
        IndexRequest {
            session_id: sid.into(),
            observed_source_revision: None,
            fingerprint: None,
        },
        Env::arc_flag(false),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_staging_before_publish() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(12)).await;
    env.engine.fail.after_staging.store(true, Ordering::SeqCst);
    let err = env.index(&sid).await.unwrap_err();
    assert_eq!(err.kind, IngestErrorKind::Transient);
    // Staging exists but is invisible; the published state (none) is unchanged.
    assert_eq!(env.staging_count().await, 1);
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(env.fts_any("qwz3").await, 0);
    assert_eq!(env.provider.calls(), 0);
    let out = rerun(&env, &restarted(&env), &sid).await;
    assert!(matches!(out, IndexOutcome::Published { .. }), "{out:?}");
    assert_eq!(env.staging_count().await, 0, "old staging discarded");
    assert_eq!(env.n("SELECT count(*) FROM snapshots").await, 1);
    assert!(env.fts("qwz3").await >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_publish_before_embeddings() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(12)).await;
    env.engine.fail.after_publish.store(true, Ordering::SeqCst);
    env.index(&sid).await.unwrap_err();
    // Text is lexically searchable; vectors are pending.
    assert!(env.active(&sid).await.is_some());
    assert!(env.fts("qwz3").await >= 1);
    assert_eq!(env.embedded_count().await, 0);
    assert_eq!(env.provider.calls(), 0);
    let chunks = env.active_chunks(&sid).await;
    let cov = env.emb.vector_coverage().await.unwrap();
    assert_eq!(
        (cov.embedded_chunks, cov.pending_chunks as usize),
        (0, chunks.len())
    );

    let out = rerun(&env, &restarted(&env), &sid).await;
    assert!(
        matches!(out, IndexOutcome::Embedded { embedded } if embedded == chunks.len()),
        "{out:?}"
    );
    assert_eq!(env.embedded_count().await as usize, chunks.len());
    assert_eq!(
        env.active_chunks(&sid).await.len(),
        chunks.len(),
        "no duplicate chunks"
    );
    assert_eq!(env.n("SELECT generation FROM sessions").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_after_embedding_commit_before_ack() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(12)).await;
    env.engine
        .fail
        .after_embed_commit
        .store(true, Ordering::SeqCst);
    env.index(&sid).await.unwrap_err();
    let chunks = env.active_chunks(&sid).await;
    let distinct: HashSet<_> = chunks.iter().map(|c| c.1.clone()).collect();
    assert_eq!(env.embedded_count().await as usize, distinct.len());
    let calls = env.provider.calls();
    assert!(calls > 0);
    // The retry observes the finished revision: cache reused, nothing duplicated, no provider call.
    assert_eq!(
        rerun(&env, &restarted(&env), &sid).await,
        IndexOutcome::NoOp
    );
    assert_eq!(env.provider.calls(), calls);
    assert_eq!(env.active_chunks(&sid).await.len(), chunks.len());
    assert_eq!(
        env.n("SELECT count(*) FROM chunks").await as usize,
        chunks.len()
    );
    assert_eq!(env.n("SELECT generation FROM sessions").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_while_parsing_leaves_published_snapshot_valid() {
    let env = Env::small_chunks().await;
    let mut lines = linear(10);
    let (sid, path) = env.add_omp("a.jsonl", &lines).await;
    env.index(&sid).await.unwrap();
    let active = env.active(&sid).await;
    lines.push(omp_msg("m010", Some("m009"), "user", &text(10)));
    std::fs::write(&path, join(&lines)).unwrap();

    let cancel = Env::arc_flag(false);
    let c2 = cancel.clone();
    *env.engine.fail.after_parse.lock() = Some(Arc::new(move || c2.store(true, Ordering::SeqCst)));
    let err = env
        .engine
        .index_session(
            IndexRequest {
                session_id: sid.clone(),
                observed_source_revision: None,
                fingerprint: None,
            },
            cancel,
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, IngestErrorKind::Cancelled);
    assert!(err.retryable);
    assert_eq!(env.staging_count().await, 0, "cancelled staging is deleted");
    assert_eq!(env.active(&sid).await, active);
    assert!(env.fts("qwz3").await >= 1);
    assert_eq!(env.fts("qwz10").await, 0, "cancelled run published nothing");
    // A fresh run without the hook picks the change up.
    *env.engine.fail.after_parse.lock() = None;
    assert!(matches!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    assert!(env.fts("qwz10").await >= 1);
}
