//! Core pipeline behaviour: exact text in FTS, exclusion of non-conversational payloads, publication
//! atomicity, duplicate delivery, supersession, exclusion, incremental reuse, branches.

use crate::common::*;
use spool::domain::digest;
use spool::ingest::chunk::{ChunkPlan, TokenCounter};
use spool::ingest::engine::{IndexOutcome, IndexRequest, IngestEngine, IngestErrorKind};
use spool::ingest::publish::{PublishOutcome, publish};
use spool::ingest::stage::{SnapshotKind, stage_snapshot};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn fixture(rel: &str) -> String {
    std::fs::read_to_string(format!("{}/fixtures/{rel}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

const SENTINELS: &[&str] = &[
    "MUST_NOT_INDEX_TOOL_SENTINEL",
    "MUST_NOT_INDEX_THINKING_SENTINEL",
    "MUST_NOT_INDEX_SUMMARY_SENTINEL",
    "MUST_NOT_INDEX_RESULT_SENTINEL",
    "MUST_NOT_INDEX_HOOK_SENTINEL",
    "MUST_NOT_INDEX_SUBAGENT_SENTINEL",
    "MUST_NOT_INDEX_REASONING_SENTINEL",
    "MUST_NOT_INDEX_AGENT_MESSAGE_SENTINEL",
];

async fn assert_no_sentinel(env: &Env) {
    for s in SENTINELS {
        let needle = format!("%{s}%");
        assert_eq!(
            env.n(&format!(
                "SELECT count(*) FROM chunks WHERE body LIKE '{needle}'"
            ))
            .await,
            0,
            "{s} in a chunk"
        );
        assert_eq!(
            env.n(&format!(
                "SELECT count(*) FROM blocks WHERE text LIKE '{needle}'"
            ))
            .await,
            0,
            "{s} in a block"
        );
        assert!(
            env.provider.seen_inputs().iter().all(|i| !i.contains(s)),
            "{s} sent to the provider"
        );
        assert_eq!(env.fts_any(&s.replace('_', " ")).await, 0, "{s} in FTS");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn omp_exact_text_in_fts_and_no_excluded_content() {
    let env = Env::new().await;
    let (sid, _) = env.add("omp", "v3.jsonl", &fixture("omp/v3.jsonl")).await;
    let out = env.index(&sid).await.unwrap();
    let IndexOutcome::Published {
        chunks,
        embeddings_pending,
        follow_up_needed,
        ..
    } = out
    else {
        panic!("{out:?}")
    };
    assert_eq!(
        (chunks, embeddings_pending, follow_up_needed),
        (1, 0, false)
    );
    let c = env.active_chunks(&sid).await;
    assert_eq!(c[0].2, "USER: hello fixture\n\nASSISTANT: done");
    assert_eq!(env.fts("fixture").await, 1);
    assert_eq!(env.fts("done").await, 1);
    assert_no_sentinel(&env).await;
    assert_eq!(
        env.provider.seen_inputs(),
        vec!["USER: hello fixture\n\nASSISTANT: done".to_string()]
    );
    // The catalog row is filled from the manifest by the publish transaction.
    let (title, cwd, kind, state, cov): (String, String, String, String, String) =
        sqlx::query_as("SELECT native_title, cwd, session_kind, index_state, coverage_status FROM sessions WHERE id=?").bind(&sid).fetch_one(&env.index.writer).await.unwrap();
    assert_eq!(
        (
            title.as_str(),
            cwd.as_str(),
            kind.as_str(),
            state.as_str(),
            cov.as_str()
        ),
        (
            "Fixture OMP",
            "/tmp/project",
            "interactive",
            "indexed",
            "complete"
        )
    );
    assert_eq!(env.n("SELECT generation FROM sessions").await, 1);
    // Nearest conversational ancestor skips the excluded tool-result record; the tool row has no payload.
    let parent: Option<String> =
        sqlx::query_scalar("SELECT parent_message_key FROM messages WHERE record_key = '00000003'")
            .fetch_one(&env.index.writer)
            .await
            .unwrap();
    assert_eq!(parent.as_deref(), Some("00000001"));
    assert_eq!(env.n("SELECT count(*) FROM records").await, 5);
    assert_eq!(env.n("SELECT count(*) FROM messages").await, 2);
    // snapshot bookkeeping
    let (rev, fp, prefix, retained, state): (String, String, i64, i64, String) = sqlx::query_as(
        "SELECT source_revision, fingerprint, prefix_end, retained_bytes, state FROM snapshots",
    )
    .fetch_one(&env.index.writer)
    .await
    .unwrap();
    assert_eq!(state, "published");
    assert!(
        rev.ends_with(&format!(":{prefix}")) && rev.split(':').count() == 5,
        "{rev}"
    );
    assert_eq!(fp, env.engine.pipeline_fingerprint());
    assert!(retained > 0);
    assert!(env.n("SELECT file_ino FROM snapshots").await > 0);
}
struct CancelDuringChunking {
    cancelled: Arc<AtomicBool>,
    prefix_calls: AtomicUsize,
}

impl TokenCounter for CancelDuringChunking {
    fn identity(&self) -> &str {
        "cancel-during-chunking-test"
    }

    fn count(&self, text: &str) -> usize {
        text.chars().count()
    }

    fn prefix_within(&self, text: &str, max_tokens: usize) -> usize {
        let bytes = text
            .char_indices()
            .nth(max_tokens)
            .map(|(index, _)| index)
            .unwrap_or(text.len());
        if self.prefix_calls.fetch_add(1, Ordering::Relaxed) == 1 {
            self.cancelled.store(true, Ordering::Relaxed);
        }
        bytes
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_during_chunking_discards_staging_without_publication() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("cancel.jsonl", &linear(30)).await;
    let cancelled = Env::arc_flag(false);
    let tokenizer: Arc<dyn TokenCounter> = Arc::new(CancelDuringChunking {
        cancelled: cancelled.clone(),
        prefix_calls: AtomicUsize::new(0),
    });
    let engine = IngestEngine::with_tokenizer(
        env.index.clone(),
        env.cfg.clone(),
        env.sources.clone(),
        env.emb.clone(),
        env.shared.clone(),
        tokenizer,
    );
    let error = engine
        .index_session(
            IndexRequest {
                session_id: sid.clone(),
                observed_source_revision: None,
                fingerprint: None,
            },
            cancelled.clone(),
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind, IngestErrorKind::Cancelled);
    assert!(error.retryable);
    assert!(cancelled.load(Ordering::Relaxed));
    assert_eq!(env.staging_count().await, 0);
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state='published'")
            .await,
        0
    );
    assert_eq!(env.n("SELECT count(*) FROM chunks").await, 0);
    assert_eq!(env.fts_any("qwz3").await, 0);
    assert_eq!(env.provider.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_and_codex_fixtures_index_without_sentinels() {
    let env = Env::new().await;
    let (claude, _) = env
        .add(
            "claude",
            "saved.jsonl",
            &fixture("claude/saved-messages.jsonl"),
        )
        .await;
    let (codex, _) = env
        .add(
            "codex",
            "canonical.jsonl",
            &fixture("codex/canonical.jsonl"),
        )
        .await;
    assert!(matches!(
        env.index(&claude).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    assert!(matches!(
        env.index(&codex).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    assert_eq!(env.fts("authoritative").await, 1, "later saved update wins");
    assert_eq!(
        env.fts("initial").await,
        0,
        "superseded representation is not searchable"
    );
    // Superseded message rows are gone, their record rows and tool provenance stay.
    assert_eq!(
        env.n("SELECT count(*) FROM messages WHERE record_key = 'claude-a1'")
            .await,
        0
    );
    assert_eq!(
        env.n("SELECT count(*) FROM records WHERE key = 'claude-a1'")
            .await,
        1
    );
    assert_eq!(env.n("SELECT count(*) FROM tool_calls WHERE native_call_id = 'call-1' AND record_key = 'claude-a1'").await, 1);
    assert_no_sentinel(&env).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accumulated_update_upserts_tool_rows_only_inside_its_group() {
    let env = Env::new().await;
    let (sid, _) = env
        .add(
            "claude",
            "acc.jsonl",
            &fixture("ingest/claude_accumulated.jsonl"),
        )
        .await;
    assert!(matches!(
        env.index(&sid).await.unwrap(),
        IndexOutcome::Published { .. }
    ));
    assert_eq!(env.fts("accumulated").await, 1);
    assert_eq!(env.fts("sentinelzzold").await, 0);
    // call-A was not re-emitted: its row stays on the superseded record. call-B was: one row, latest record.
    assert_eq!(
        env.n(
            "SELECT count(*) FROM tool_calls WHERE native_call_id='call-A' AND record_key='acc-a1'"
        )
        .await,
        1
    );
    assert_eq!(
        env.n("SELECT count(*) FROM tool_calls WHERE native_call_id='call-B'")
            .await,
        1
    );
    assert_eq!(
        env.n(
            "SELECT count(*) FROM tool_calls WHERE native_call_id='call-B' AND record_key='acc-a2'"
        )
        .await,
        1
    );
    assert_no_sentinel(&env).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_is_invisible_and_publish_is_atomic() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(60)).await;
    let fp = env.engine.pipeline_fingerprint();
    let staged = stage_snapshot(
        &env.index,
        &env.sources,
        &env.cfg,
        &sid,
        SnapshotKind::Indexed,
        Env::arc_flag(false),
        &env.shared,
        &fp,
    )
    .await
    .unwrap();
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state='staging'")
            .await,
        1
    );
    assert!(
        env.n("SELECT count(*) FROM messages").await == 60
            && env.n("SELECT count(*) FROM chunks").await == 0
    );
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(env.fts("qwz3").await, 0, "readers never observe staging");
    assert_eq!(env.fts_any("qwz3").await, 0);
    drop(staged);
    sqlx::query("DELETE FROM snapshots")
        .execute(&env.index.writer)
        .await
        .unwrap();

    // While the engine publishes, a concurrent reader sees either nothing or everything, never a partial set.
    let index = env.index.clone();
    let sid2 = sid.clone();
    let stop = Env::arc_flag(false);
    let stop2 = stop.clone();
    let watcher = tokio::spawn(async move {
        let mut seen = HashSet::new();
        while !stop2.load(Ordering::SeqCst) {
            let (visible, leaked): (i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id WHERE s.id = ?), \
                        (SELECT count(*) FROM chunks WHERE snapshot_id IN (SELECT id FROM snapshots WHERE state = 'staging'))",
            )
            .bind(&sid2)
            .fetch_one(&index.reader)
            .await
            .unwrap();
            assert_eq!(leaked, 0);
            seen.insert(visible);
            tokio::task::yield_now().await;
        }
        seen
    });
    let IndexOutcome::Published { chunks, .. } = env.index(&sid).await.unwrap() else {
        panic!()
    };
    stop.store(true, Ordering::SeqCst);
    let seen = watcher.await.unwrap();
    assert!(chunks > 5, "{chunks}");
    assert!(
        seen.iter().all(|v| *v == 0 || *v == chunks as i64),
        "partial publication observed: {seen:?}"
    );
    assert_eq!(env.staging_count().await, 0);
    assert_eq!(
        env.n("SELECT count(*) FROM chunks_fts").await,
        chunks as i64
    );
    assert_eq!(env.n("SELECT index_epoch FROM (SELECT CAST(value AS INTEGER) AS index_epoch FROM app_meta WHERE key='index_epoch')").await > 0, true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_delivery_publishes_once_and_costs_nothing() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(30)).await;
    let (a, b) = tokio::join!(env.index(&sid), env.index(&sid));
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, IndexOutcome::Published { .. }))
            .count(),
        1,
        "{outcomes:?}"
    );
    assert!(outcomes.contains(&IndexOutcome::NoOp), "{outcomes:?}");
    let chunks = env.active_chunks(&sid).await;
    let total = env.n("SELECT count(*) FROM chunks").await as usize;
    assert_eq!(total, chunks.len(), "no duplicate chunks");
    assert_eq!(env.n("SELECT generation FROM sessions").await, 1);
    // Lost ack later: still a no-op with ZERO provider calls.
    let calls = env.provider.calls();
    assert!(calls > 0);
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp);
    assert_eq!(env.provider.calls(), calls);
    assert_eq!(
        env.embedded_count().await as usize,
        chunks
            .iter()
            .map(|c| c.1.clone())
            .collect::<HashSet<_>>()
            .len()
    );
}

fn one_chunk_plan(text: &str) -> Vec<ChunkPlan> {
    vec![ChunkPlan {
        run_key: "r".into(),
        ordinal: 0,
        rendered: text.into(),
        input_hash: digest(text.as_bytes()),
        rendered_tokens: 3,
        time_anchor: None,
        spans: vec![],
        is_tail: true,
    }]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_job_cannot_overwrite_newer_content() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(4)).await;
    let fp = env.engine.pipeline_fingerprint();
    let stage = || {
        stage_snapshot(
            &env.index,
            &env.sources,
            &env.cfg,
            &sid,
            SnapshotKind::Indexed,
            Env::arc_flag(false),
            &env.shared,
            &fp,
        )
    };
    let old = stage().await.unwrap();
    let new = stage().await.unwrap();
    assert_eq!(old.generation, new.generation);
    let r = publish(
        &env.index,
        &env.shared,
        &sid,
        &new.snapshot_id,
        new.generation,
        &fp,
        &one_chunk_plan("USER: newer"),
    )
    .await
    .unwrap();
    assert!(matches!(r, PublishOutcome::Published { generation: 1, .. }));
    // The older job arrives late: guarded no-op, its staging is deleted, content untouched.
    let r = publish(
        &env.index,
        &env.shared,
        &sid,
        &old.snapshot_id,
        old.generation,
        &fp,
        &one_chunk_plan("USER: older"),
    )
    .await
    .unwrap();
    assert_eq!(r, PublishOutcome::Superseded);
    assert_eq!(
        env.active(&sid).await.as_deref(),
        Some(new.snapshot_id.as_str())
    );
    assert_eq!(env.fts("newer").await, 1);
    assert_eq!(env.fts_any("older").await, 0);
    assert_eq!(env.staging_count().await, 0);
    // Fingerprint mismatch is also superseded.
    let third = stage().await.unwrap();
    let r = publish(
        &env.index,
        &env.shared,
        &sid,
        &third.snapshot_id,
        third.generation,
        "other-fingerprint",
        &one_chunk_plan("USER: third"),
    )
    .await
    .unwrap();
    assert_eq!(r, PublishOutcome::Superseded);
    assert_eq!(env.fts_any("third").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exclusion_is_never_resurrected() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(4)).await;
    let fp = env.engine.pipeline_fingerprint();
    let staged = stage_snapshot(
        &env.index,
        &env.sources,
        &env.cfg,
        &sid,
        SnapshotKind::Indexed,
        Env::arc_flag(false),
        &env.shared,
        &fp,
    )
    .await
    .unwrap();
    // Operator excludes the session while the job is in flight.
    env.n("UPDATE sessions SET excluded = 1 WHERE id = 'ss_a.jsonl' RETURNING 1")
        .await;
    let r = publish(
        &env.index,
        &env.shared,
        &sid,
        &staged.snapshot_id,
        staged.generation,
        &fp,
        &one_chunk_plan("USER: zombie"),
    )
    .await
    .unwrap();
    assert_eq!(r, PublishOutcome::Excluded);
    assert_eq!(env.active(&sid).await, None);
    assert_eq!(env.fts_any("zombie").await, 0);
    assert_eq!(env.staging_count().await, 0);
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::Excluded);
    assert_eq!(env.provider.calls(), 0);
    // Exclusion table entries (session or source level) count too.
    env.n("UPDATE sessions SET excluded = 0 RETURNING 1").await;
    sqlx::query("INSERT INTO exclusions(kind, target_id, created_at) VALUES ('source','omp',0)")
        .execute(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::Excluded);
    assert_eq!(env.n("SELECT count(*) FROM chunks").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_session_makes_zero_embedding_calls() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(20)).await;
    env.index(&sid).await.unwrap();
    let calls = env.provider.calls();
    let epoch = env.index.epoch().await.unwrap();
    for _ in 0..3 {
        assert_eq!(env.index(&sid).await.unwrap(), IndexOutcome::NoOp);
    }
    assert_eq!(env.provider.calls(), calls);
    assert_eq!(
        env.index.epoch().await.unwrap(),
        epoch,
        "a no-op must not touch the index"
    );
    // Touching the file without changing content reparses locally but re-embeds nothing.
    let (_, path) = (0, env.root.join("omp/a.jsonl"));
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(30))
        .unwrap();
    let out = env.index(&sid).await.unwrap();
    let IndexOutcome::Published {
        reused_chunks,
        chunks,
        embeddings_pending,
        ..
    } = out
    else {
        panic!("{out:?}")
    };
    assert_eq!((reused_chunks, embeddings_pending), (chunks, 0));
    assert_eq!(
        env.provider.calls(),
        calls,
        "no re-embedding of unchanged inputs"
    );
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots").await,
        1,
        "previous snapshot retired"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_reuses_sealed_chunks_and_replaces_only_the_tail() {
    let env = Env::small_chunks().await;
    let mut lines = linear(24);
    let (sid, path) = env.add_omp("a.jsonl", &lines).await;
    env.index(&sid).await.unwrap();
    let before = env.active_chunks(&sid).await;
    assert!(before.len() >= 5);
    let (old_tail, sealed) = before.split_last().unwrap();
    assert_eq!(old_tail.3, 1, "last chunk is the replaceable tail");
    let seen_before: HashSet<String> = env.provider.seen_inputs().into_iter().collect();
    assert_eq!(seen_before.len(), before.len());

    for i in 24..28 {
        lines.push(omp_msg(
            &format!("m{i:03}"),
            Some(&format!("m{:03}", i - 1)),
            if i % 2 == 0 { "user" } else { "assistant" },
            &text(i),
        ));
    }
    std::fs::write(&path, join(&lines)).unwrap();
    let IndexOutcome::Published {
        reused_chunks,
        chunks,
        ..
    } = env.index(&sid).await.unwrap()
    else {
        panic!()
    };
    let after = env.active_chunks(&sid).await;
    assert_eq!(chunks, after.len());
    // Sealed chunks keep their logical id and their embedding.
    for (i, s) in sealed.iter().enumerate() {
        assert_eq!(after[i].0, s.0, "sealed chunk {i} changed");
        assert_eq!(after[i].1, s.1);
    }
    assert!(reused_chunks >= sealed.len());
    // Only inputs that did not exist before were sent; sealed inputs are never sent again.
    let sent: Vec<String> = env.provider.seen_inputs();
    let fresh: Vec<&String> = sent.iter().filter(|i| !seen_before.contains(*i)).collect();
    assert_eq!(
        sent.len() - seen_before.len(),
        fresh.len(),
        "an old input was embedded twice"
    );
    // A tail whose rendered text did not change (no appended message fits it) keeps its input hash, so it is not
    // embedded again; it simply stops being the replaceable tail. Only input hashes new to the corpus are embedded.
    let before_hashes: HashSet<&Vec<u8>> = before.iter().map(|c| &c.1).collect();
    let new_in_corpus: HashSet<&Vec<u8>> = after
        .iter()
        .map(|c| &c.1)
        .filter(|h| !before_hashes.contains(*h))
        .collect();
    let sent_hashes: Vec<Vec<u8>> = fresh
        .iter()
        .map(|s| digest(s.as_bytes()).to_vec())
        .collect();
    assert_eq!(
        sent_hashes.iter().collect::<HashSet<_>>().len(),
        sent_hashes.len(),
        "no input was sent twice"
    );
    assert_eq!(
        sent_hashes.iter().collect::<HashSet<_>>(),
        new_in_corpus,
        "exactly the new unique inputs were embedded"
    );
    assert!(
        sealed.iter().all(|s| after.iter().any(|c| c.1 == s.1)),
        "sealed inputs stay active"
    );
    assert_eq!(
        after.iter().filter(|c| c.3 == 1).count(),
        1,
        "exactly one replaceable tail remains"
    );
    assert!(
        after.iter().filter(|c| c.0 == old_tail.0).all(|c| c.3 == 0),
        "the old tail is sealed or gone, never still the tail"
    );
    assert!(env.fts("qwz27").await >= 1);
    assert_eq!(
        env.n("SELECT count(*) FROM chunks").await as usize,
        after.len()
    );
    assert_eq!(
        env.n("SELECT count(*) FROM chunks_fts").await as usize,
        after.len()
    );
}

/// Messages of each non-context span of a chunk must be on one root->leaf path.
async fn assert_no_sibling_mixing(env: &Env, sid: &str) {
    let parent: std::collections::HashMap<String, Option<String>> = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT m.record_key, m.parent_message_key FROM messages m JOIN sessions s ON s.active_snapshot_id = m.snapshot_id WHERE s.id = ?",
    )
    .bind(sid)
    .fetch_all(&env.index.writer)
    .await
    .unwrap()
    .into_iter()
    .collect();
    let ancestors = |mut k: String| {
        let mut v = HashSet::from([k.clone()]);
        while let Some(Some(p)) = parent.get(&k) {
            v.insert(p.clone());
            k = p.clone();
        }
        v
    };
    let chunks: Vec<(i64, String)> = sqlx::query_as("SELECT c.rowid, c.run_key FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id WHERE s.id = ?")
        .bind(sid)
        .fetch_all(&env.index.writer)
        .await
        .unwrap();
    for (rowid, _) in chunks {
        let spans: Vec<(String, i64)> = sqlx::query_as(
            "SELECT record_key, is_context FROM chunk_spans WHERE chunk_rowid = ? ORDER BY seq",
        )
        .bind(rowid)
        .fetch_all(&env.index.writer)
        .await
        .unwrap();
        let new: Vec<&String> = spans.iter().filter(|s| s.1 == 0).map(|s| &s.0).collect();
        // Every new message is an ancestor-or-self of the deepest new message.
        let deepest = new
            .iter()
            .max_by_key(|k| ancestors((**k).clone()).len())
            .unwrap();
        let path = ancestors((*deepest).clone());
        assert!(
            new.iter().all(|k| path.contains(*k)),
            "chunk {rowid} mixes sibling branches"
        );
        // Context comes from verified ancestors of the first new message only.
        let first = new
            .iter()
            .min_by_key(|k| ancestors((**k).clone()).len())
            .unwrap();
        let first_anc = ancestors((*first).clone());
        for c in spans.iter().filter(|s| s.1 == 1) {
            assert!(
                first_anc.contains(&c.0),
                "chunk {rowid} context {} is not an ancestor",
                c.0
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_rechunks_locally_and_never_mixes_branches() {
    let env = Env::small_chunks().await;
    let mut lines = linear(16);
    let (sid, path) = env.add_omp("a.jsonl", &lines).await;
    env.index(&sid).await.unwrap();
    let before = env.active_chunks(&sid).await;
    let seen_before: HashSet<String> = env.provider.seen_inputs().into_iter().collect();
    // Fork after m005: two new children (a sibling branch) of m005 next to m006.
    lines.push(omp_msg("f100", Some("m005"), "user", &text(100)));
    lines.push(omp_msg("f101", Some("f100"), "assistant", &text(101)));
    lines.push(omp_msg("f102", Some("f101"), "user", &text(102)));
    std::fs::write(&path, join(&lines)).unwrap();
    let out = env.index(&sid).await.unwrap();
    assert!(matches!(out, IndexOutcome::Published { .. }), "{out:?}");
    let after = env.active_chunks(&sid).await;
    assert_no_sibling_mixing(&env, &sid).await;
    // Chunks entirely before the fork point are untouched and not re-embedded.
    let stable: Vec<_> = before
        .iter()
        .take_while(|c| !c.2.contains("note 5 ") && !c.2.contains("note 6 "))
        .collect();
    assert!(!stable.is_empty());
    let after_hashes: HashSet<&Vec<u8>> = after.iter().map(|c| &c.1).collect();
    for c in &stable {
        assert!(
            after_hashes.contains(&c.1),
            "a chunk before the fork point changed"
        );
    }
    let fresh = env
        .provider
        .seen_inputs()
        .into_iter()
        .filter(|i| !seen_before.contains(i))
        .count();
    assert!(
        fresh < after.len(),
        "localized: {fresh} of {} re-embedded",
        after.len()
    );
    // Both branches are searchable; their text never shares a chunk's new content.
    assert!(env.fts("qwz15").await >= 1);
    assert!(env.fts("qwz101").await >= 1);
    let sib: i64 = env
        .n("SELECT count(*) FROM chunks WHERE body LIKE '%qwz15%' AND body LIKE '%qwz101%'")
        .await;
    assert_eq!(sib, 0, "sibling branches share a chunk");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_tool_payload_changes_neither_chunks_nor_embedding_tokens() {
    let plain = Env::small_chunks().await;
    let heavy = Env::small_chunks().await;
    let base = linear(12);
    let mut with_tool = base.clone();
    // A 2 MiB tool result between m005 and m006 (m006 re-parented onto it).
    with_tool.insert(
        7,
        omp_tool_result("t001", "m005", &"payload ".repeat(262_144)),
    );
    with_tool[8] = omp_msg("m006", Some("t001"), "user", &text(6));
    let (a, _) = plain.add_omp("a.jsonl", &base).await;
    let (b, _) = heavy.add_omp("a.jsonl", &with_tool).await;
    plain.index(&a).await.unwrap();
    heavy.index(&b).await.unwrap();
    let pa = plain.active_chunks(&a).await;
    let pb = heavy.active_chunks(&b).await;
    assert_eq!(
        pa.iter().map(|c| &c.2).collect::<Vec<_>>(),
        pb.iter().map(|c| &c.2).collect::<Vec<_>>()
    );
    assert_eq!(
        plain.n("SELECT sum(rendered_tokens) FROM chunks").await,
        heavy.n("SELECT sum(rendered_tokens) FROM chunks").await,
    );
    assert_eq!(plain.provider.seen_inputs(), heavy.provider.seen_inputs());
    assert!(
        heavy.n("SELECT max(length(text)) FROM blocks").await < 1000,
        "tool payload must not be stored"
    );
    assert_eq!(heavy.fts_any("payload").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn estimate_is_local_and_matches_the_plan() {
    let env = Env::small_chunks().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(20)).await;
    let est = env.engine.estimate(&sid).await.unwrap();
    assert_eq!(est.messages, 20);
    assert!(est.chunks > 1 && est.tokens_total > 0);
    assert_eq!(est.tokens_uncached, est.tokens_total);
    assert_eq!(env.provider.calls(), 0);
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots").await,
        0,
        "estimate writes no snapshots"
    );
    env.index(&sid).await.unwrap();
    let after = env.engine.estimate(&sid).await.unwrap();
    assert_eq!(
        (after.chunks, after.tokens_total),
        (est.chunks, est.tokens_total)
    );
    assert_eq!(after.tokens_uncached, 0);
    assert_eq!(
        env.n("SELECT sum(rendered_tokens) FROM chunks").await as usize,
        est.tokens_total
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_read_snapshot_is_read_only_without_chunks() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("a.jsonl", &linear(6)).await;
    let fp = env.engine.pipeline_fingerprint();
    let s = stage_snapshot(
        &env.index,
        &env.sources,
        &env.cfg,
        &sid,
        SnapshotKind::SourceRead,
        Env::arc_flag(false),
        &env.shared,
        &fp,
    )
    .await
    .unwrap();
    let (kind, state): (String, String) =
        sqlx::query_as("SELECT kind, state FROM snapshots WHERE id = ?")
            .bind(&s.snapshot_id)
            .fetch_one(&env.index.writer)
            .await
            .unwrap();
    assert_eq!(
        (kind.as_str(), state.as_str()),
        ("source_read", "read_only")
    );
    assert_eq!(env.n("SELECT count(*) FROM chunks").await, 0);
    assert_eq!(env.active(&sid).await, None);
    // An indexing run neither deletes nor publishes it.
    env.index(&sid).await.unwrap();
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state = 'read_only'")
            .await,
        1
    );
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state = 'published'")
            .await,
        1
    );
}
