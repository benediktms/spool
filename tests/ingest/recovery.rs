//! Recovery behaviour around leftovers and leases (spec 10.4, 11.4, 19.4).

use crate::common::*;
use spool::ingest::engine::IndexOutcome;
use spool::ingest::stage::sweep_staging;
use spool::transcript::lease::{LeaseManager, PinTarget};
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_run_then_exclusion_never_resurrects_and_cleans_up() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(6)).await;
    env.engine.fail.after_staging.store(true, Ordering::SeqCst);
    env.index(&sid).await.unwrap_err();
    assert_eq!(env.staging_count().await, 1);
    env.n("UPDATE sessions SET excluded = 1 RETURNING 1").await;
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::Excluded);
    assert_eq!(
        env.staging_count().await,
        0,
        "orphaned staging of an excluded session is discarded"
    );
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(env.provider.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_staging_sweep_respects_age() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(6)).await;
    env.engine.fail.after_staging.store(true, Ordering::SeqCst);
    env.index(&sid).await.unwrap_err();
    assert_eq!(
        sweep_staging(&env.index, None, T0 - 1).await.unwrap(),
        0,
        "a stage younger than the cutoff is kept"
    );
    assert_eq!(
        sweep_staging(&env.index, Some(&sid), T0 + 1).await.unwrap(),
        1
    );
    assert_eq!(env.n("SELECT count(*) FROM snapshots").await, 0);
    assert_eq!(
        env.n("SELECT count(*) FROM records").await,
        0,
        "cascade leaves no staging rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_previous_snapshot_is_kept_leased_without_chunks() {
    let env = Env::small_chunks().await;
    let mut lines = linear(10);
    let (sid, path) = env.add_omp("a.jsonl", &lines).await;
    env.index(&sid).await.unwrap();
    let old = env.active(&sid).await.unwrap();
    let leases = LeaseManager::new(
        env.index.clone(),
        env.cfg.reader.clone(),
        env.shared.clone(),
    );
    leases
        .pin(&[PinTarget {
            session_id: sid.clone(),
            snapshot_id: old.clone(),
        }])
        .await
        .unwrap();

    lines.push(omp_msg("m010", Some("m009"), "user", &text(10)));
    std::fs::write(&path, join(&lines)).unwrap();
    assert!(matches!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    let new = env.active(&sid).await.unwrap();
    assert_ne!(new, old);
    let state: String = sqlx::query_scalar("SELECT state FROM snapshots WHERE id = ?")
        .bind(&old)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(state, "retired_leased");
    assert_eq!(
        env.n(&format!(
            "SELECT count(*) FROM chunks WHERE snapshot_id = '{old}'"
        ))
        .await,
        0,
        "retired chunks are never searchable"
    );
    assert!(
        env.n(&format!(
            "SELECT count(*) FROM messages WHERE snapshot_id = '{old}'"
        ))
        .await
            > 0,
        "pinned reads keep their messages"
    );
    assert!(env.fts("qwz10").await >= 1);
    let published: i64 = env
        .n("SELECT count(*) FROM snapshots WHERE state = 'published'")
        .await;
    assert_eq!(published, 1);
}
