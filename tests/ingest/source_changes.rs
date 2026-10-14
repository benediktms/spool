//! Source observation edge cases (spec 8.2, 8.3, 11.7): pending tails, malformed records, limits,
//! growth, in-place rewrites, missing and disabled sources.

use crate::common::*;
use spool::ingest::engine::{DeferReason, IndexOutcome, IndexRequest, IngestErrorKind};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_trailing_record_is_pending_data() {
    let env = Env::new().await;
    let lines = linear(4);
    let complete = join(&lines);
    let tail = omp_msg("m004", Some("m003"), "user", &text(4));
    let (sid, path) = env
        .add(
            "omp",
            "a.jsonl",
            &format!("{complete}{}", &tail[..tail.len() / 2]),
        )
        .await;
    let out = env.index(&sid).await.unwrap();
    assert!(
        matches!(
            out,
            IndexOutcome::Published {
                follow_up_needed: false,
                ..
            }
        ),
        "an incomplete tail alone requests no follow-up: {out:?}"
    );
    assert_eq!(env.fts("qwz3").await, 1);
    assert_eq!(env.fts("qwz4").await, 0);
    assert_eq!(
        env.n("SELECT count(*) FROM parse_warnings WHERE code = 'pending_trailing_bytes'")
            .await,
        1
    );
    let cov: String = sqlx::query_scalar("SELECT coverage_status FROM sessions WHERE id=?")
        .bind(&sid)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(cov, "partial");
    // Stale 'pending' bookkeeping (set by the job layer) must not cause a reparse/republish loop.
    env.n("UPDATE sessions SET index_state = 'pending' RETURNING 1")
        .await;
    let generation = env.n("SELECT generation FROM sessions").await;
    for _ in 0..3 {
        assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp);
    }
    assert_eq!(env.n("SELECT generation FROM sessions").await, generation);
    assert_eq!(
        env.n("SELECT index_state = 'indexed' FROM sessions").await,
        1
    );
    assert_eq!(
        env.n("SELECT prefix_end FROM snapshots").await as usize,
        complete.len(),
        "checkpoint never passes incomplete bytes"
    );
    // Unchanged file: the pending tail is not reparsed again.
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp);
    // The writer finishes the record: next change picks it up.
    std::fs::write(&path, format!("{complete}{tail}\n")).unwrap();
    let out = env.index(&sid).await.unwrap();
    assert!(
        matches!(
            out,
            IndexOutcome::Published {
                follow_up_needed: false,
                ..
            }
        ),
        "{out:?}"
    );
    assert_eq!(env.fts("qwz4").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_complete_record_is_a_warning_and_marks_partial() {
    let env = Env::new().await;
    let mut lines = linear(6);
    lines.insert(3, "this line is not json".into());
    let (sid, _) = env.add_omp("a.jsonl", &lines).await;
    assert!(matches!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    let cov: String = sqlx::query_scalar("SELECT coverage_status FROM sessions WHERE id=?")
        .bind(&sid)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(cov, "partial");
    assert!(
        env.n("SELECT count(*) FROM parse_warnings WHERE code = 'invalid_record'")
            .await
            >= 1
    );
    assert_eq!(env.fts("qwz5").await, 1, "valid records are retained");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn record_limit_is_visible_and_not_retried() {
    let env = Env::with(|c| c.ingestion.max_record_bytes = 300).await;
    let mut lines = linear(3);
    lines.push(omp_msg("m003", Some("m002"), "user", &"x ".repeat(400)));
    let (sid, _) = env.add_omp("a.jsonl", &lines).await;
    assert_eq!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::LimitExceeded("max_record_bytes"))
    );
    let (state, err): (String, String) =
        sqlx::query_as("SELECT index_state, index_error FROM sessions WHERE id=?")
            .bind(&sid)
            .fetch_one(&env.index.writer)
            .await
            .unwrap();
    assert_eq!(state, "failed");
    assert!(err.starts_with("limit_exceeded:max_record_bytes"), "{err}");
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots").await,
        0,
        "staging deleted, nothing published"
    );
    assert_eq!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::LimitExceeded("max_record_bytes"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_file_limit_is_visible() {
    let env = Env::with(|c| c.ingestion.max_source_file_bytes = 100).await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(6)).await;
    assert_eq!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::LimitExceeded("max_source_file_bytes"))
    );
    assert_eq!(
        env.n("SELECT index_state = 'failed' FROM sessions").await,
        1
    );
    assert_eq!(env.n("SELECT count(*) FROM snapshots").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_that_grows_while_parsing_publishes_the_prefix_and_requests_follow_up() {
    let env = Env::new().await;
    let (sid, path) = env.add_omp("a.jsonl", &linear(4)).await;
    let size = std::fs::metadata(&path).unwrap().len();
    let p2 = path.clone();
    let once = std::sync::atomic::AtomicBool::new(true);
    *env.engine.fail.after_parse.lock() = Some(Arc::new(move || {
        if once.swap(false, Ordering::SeqCst) {
            let mut f = std::fs::OpenOptions::new().append(true).open(&p2).unwrap();
            writeln!(f, "{}", omp_msg("m004", Some("m003"), "user", &text(4))).unwrap();
        }
    }));
    let out = env.index(&sid).await.unwrap();
    assert!(
        matches!(
            out,
            IndexOutcome::Published {
                follow_up_needed: true,
                ..
            }
        ),
        "{out:?}"
    );
    assert_eq!(env.n("SELECT prefix_end FROM snapshots").await as u64, size);
    assert_eq!(env.fts("qwz3").await, 1);
    assert_eq!(
        env.fts("qwz4").await,
        0,
        "bytes appended after the observation are not claimed"
    );
    // The follow-up pass (no hook, no chasing) completes it.
    let out = env.index(&sid).await.unwrap();
    assert!(
        matches!(
            out,
            IndexOutcome::Published {
                follow_up_needed: false,
                ..
            }
        ),
        "{out:?}"
    );
    assert_eq!(env.fts("qwz4").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rewrite_in_place_discards_staging() {
    let env = Env::new().await;
    let (sid, path) = env.add_omp("a.jsonl", &linear(4)).await;
    let p2 = path.clone();
    *env.engine.fail.after_parse.lock() = Some(Arc::new(move || {
        let old = std::fs::read(&p2).unwrap();
        let rewritten: Vec<u8> = String::from_utf8(old)
            .unwrap()
            .replace("qwz2", "qwz9")
            .into_bytes();
        std::fs::write(&p2, rewritten).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&p2)
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(60))
            .unwrap();
    }));
    assert_eq!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::SourceChanged)
    );
    assert_eq!(env.staging_count().await, 0);
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(env.n("SELECT count(*) FROM chunks").await, 0);
    assert_eq!(
        env.n("SELECT index_state = 'deferred' FROM sessions").await,
        1
    );
    // Rediscovery: a clean run publishes the new content.
    *env.engine.fail.after_parse.lock() = None;
    assert!(matches!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    assert_eq!(env.fts("qwz9").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replaced_file_identity_is_detected() {
    let env = Env::new().await;
    let (sid, path) = env.add_omp("a.jsonl", &linear(4)).await;
    let p2 = path.clone();
    *env.engine.fail.after_parse.lock() = Some(Arc::new(move || {
        let tmp = p2.with_extension("new");
        std::fs::write(&tmp, join(&linear(6))).unwrap();
        std::fs::rename(&tmp, &p2).unwrap();
    }));
    assert_eq!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::SourceChanged)
    );
    assert_eq!(env.staging_count().await, 0);
    assert_eq!(env.n("SELECT count(*) FROM chunks").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_disabled_and_cancelled_sources() {
    let env = Env::new().await;
    let (sid, path) = env.add_omp("a.jsonl", &linear(4)).await;
    // Cancelled before start: nothing staged.
    let err = env
        .engine
        .index_session(
            IndexRequest {
                session_id: sid.clone(),
                observed_source_revision: None,
                fingerprint: None,
            },
            Env::arc_flag(true),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, IngestErrorKind::Cancelled);
    assert_eq!(env.n("SELECT count(*) FROM snapshots").await, 0);
    std::fs::remove_file(&path).unwrap();
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::SourceMissing);
    assert_eq!(
        env.index("ss_unknown").await.unwrap_err().kind,
        IngestErrorKind::Permanent
    );

    let off = Env::with(|c| c.sources[0].enabled = false).await;
    let (sid, _) = off.add_omp("a.jsonl", &linear(4)).await;
    assert_eq!(
        off.index(&sid).await.unwrap(),
        IndexOutcome::Deferred(DeferReason::SourceDisabled)
    );
    assert_eq!(off.n("SELECT count(*) FROM snapshots").await, 0);
}

/// Regression: every committed OMP fixture (some end in an incomplete line) settles after one pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omp_fixtures_settle_without_follow_up_loops() {
    let env = Env::new().await;
    let dir = format!("{}/fixtures/omp", env!("CARGO_MANIFEST_DIR"));
    let mut n = 0;
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.extension().and_then(|x| x.to_str()) != Some("jsonl") {
            continue;
        }
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        let (sid, _) = env
            .add("omp", &name, &std::fs::read_to_string(&p).unwrap())
            .await;
        let first = env.index(&sid).await.unwrap();
        assert!(!first.follow_up_needed(), "{name}: {first:?}");
        env.n(&format!(
            "UPDATE sessions SET index_state = 'pending' WHERE id = '{sid}' RETURNING 1"
        ))
        .await;
        for _ in 0..2 {
            assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp, "{name}");
        }
        assert_eq!(
            env.n(&format!(
                "SELECT index_state = 'indexed' FROM sessions WHERE id = '{sid}'"
            ))
            .await,
            1,
            "{name}"
        );
        n += 1;
    }
    assert!(n >= 1);
}
