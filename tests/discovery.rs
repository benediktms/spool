//! Spec 19.5 discovery tests (catalog level: no workers, no provider).

mod jobs_common;

use jobs_common::*;
use spool::config::SourceConfig;
use spool::discovery::{DiscoveryService, Mode, Report, Selection};
use spool::domain::HarnessKind;
use spool::storage::catalog;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn svc(env: &Env) -> DiscoveryService {
    DiscoveryService::new(
        env.index.clone(),
        env.sources.clone(),
        env.cfg.clone(),
        env.shared.clone(),
    )
}

fn flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

async fn auto(d: &DiscoveryService) -> Report {
    d.run_auto("omp", flag()).await.unwrap()
}

async fn source_row(env: &Env) -> catalog::SourceRow {
    catalog::source(&env.index, "omp").await.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_covers_seven_days_and_persists_cutoff() {
    let env = Env::new().await;
    env.omp("recent.jsonl", "recent", &["hello recent"], T0 - DAY);
    env.omp("edge.jsonl", "edge", &["hello edge"], T0 - 6 * DAY);
    env.omp("old.jsonl", "old", &["hello old"], T0 - 30 * DAY);
    let d = svc(&env);
    let rep = auto(&d).await;
    assert!(rep.complete, "{rep:?}");
    assert_eq!(rep.skipped_old, 1);
    let src = source_row(&env).await;
    assert_eq!(src.bootstrap_started_at, Some(T0));
    assert_eq!(src.bootstrap_cutoff, Some(T0 - 7 * DAY));
    assert_eq!(
        src.discovery_watermark,
        Some(T0),
        "complete scan advances the watermark"
    );
    assert_eq!(src.last_scan_ok, Some(true));
    for n in ["recent", "edge"] {
        let s = env.session_by_native(n).await.expect(n);
        assert_eq!(s.eligibility, "bootstrap");
        assert_eq!(s.index_state, "pending");
    }
    assert!(
        env.session_by_native("old").await.is_none(),
        "older unseen files are not cataloged or backfilled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_cutoff_is_persisted_not_recomputed() {
    let env = Env::new().await;
    env.omp("a.jsonl", "a", &["hello"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    env.clock.advance_ms(3 * DAY);
    auto(&d).await;
    let src = source_row(&env).await;
    assert_eq!(src.bootstrap_started_at, Some(T0));
    assert_eq!(src.bootstrap_cutoff, Some(T0 - 7 * DAY));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_started_earlier_but_recently_active_is_eligible_as_whole_conversation() {
    let env = Env::new().await;
    // The header says September 1st; the file was appended to yesterday.
    env.omp(
        "long.jsonl",
        "long",
        &["started a month ago", "and continued yesterday"],
        T0 - DAY,
    );
    let d = svc(&env);
    auto(&d).await;
    let s = env.session_by_native("long").await.unwrap();
    assert_eq!(s.eligibility, "bootstrap");
    assert_eq!(s.index_state, "pending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unseen_older_files_are_never_backfilled_automatically() {
    let env = Env::new().await;
    let d = svc(&env);
    auto(&d).await;
    env.omp("ancient.jsonl", "ancient", &["old stuff"], T0 - 90 * DAY);
    for _ in 0..3 {
        env.clock.advance_ms(60_000);
        let rep = auto(&d).await;
        assert!(rep.complete);
        assert_eq!(rep.skipped_old, 1);
    }
    assert!(env.session_by_native("ancient").await.is_none());
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_explicit_backfill_is_whole_corpus_only_on_request() {
    let env = Env::new().await;
    env.omp("recent.jsonl", "recent", &["recent"], T0 - DAY);
    env.omp("mid.jsonl", "mid", &["mid"], T0 - 20 * DAY);
    env.omp("ancient.jsonl", "ancient", &["ancient"], T0 - 200 * DAY);
    let d = svc(&env);
    auto(&d).await;
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 1);

    // --since 30d reaches `mid` but not `ancient`
    let rep = d
        .run_explicit(
            "omp",
            Selection::Since(T0 - 30 * DAY),
            Mode::Enqueue("op_1".into()),
            None,
            100,
            flag(),
        )
        .await
        .unwrap();
    assert!(rep.finished);
    let mid = env.session_by_native("mid").await.unwrap();
    assert_eq!(
        (
            mid.eligibility.as_str(),
            mid.index_state.as_str(),
            mid.operation_id.as_deref()
        ),
        ("manual", "pending", Some("op_1"))
    );
    assert!(env.session_by_native("ancient").await.is_none());

    // an ordinary later discovery does not widen anything
    env.clock.advance_ms(60_000);
    auto(&d).await;
    assert!(env.session_by_native("ancient").await.is_none());

    // --all takes everything
    let rep = d
        .run_explicit(
            "omp",
            Selection::All,
            Mode::Enqueue("op_2".into()),
            None,
            100,
            flag(),
        )
        .await
        .unwrap();
    assert!(rep.finished);
    assert_eq!(
        env.session_by_native("ancient").await.unwrap().eligibility,
        "manual"
    );
    assert_eq!(rep.already_current, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_enumeration_pages_with_a_durable_cursor_and_never_loads_everything() {
    let env = Env::new().await;
    for i in 0..10 {
        env.omp(
            &format!("s{i:02}.jsonl"),
            &format!("n{i:02}"),
            &["hello"],
            T0 - 60 * DAY,
        );
    }
    let mut d = svc(&env);
    d.page_size = 3; // page of 3 files; budget of 4 sessions per call
    let (mut after, mut calls, mut total) = (None::<PathBuf>, 0, 0);
    loop {
        let rep = d
            .run_explicit(
                "omp",
                Selection::All,
                Mode::Enqueue("op_p".into()),
                after.clone(),
                4,
                flag(),
            )
            .await
            .unwrap();
        calls += 1;
        total += rep.queued;
        assert!(rep.queued <= 4, "budget respected: {}", rep.queued);
        match rep.next_after {
            Some(p) => {
                // the cursor strictly advances
                assert!(
                    after
                        .as_ref()
                        .is_none_or(|a| a.to_string_lossy().as_ref() < p.as_str()),
                    "cursor must advance: {after:?} -> {p}"
                );
                after = Some(PathBuf::from(p));
            }
            None => {
                assert!(rep.finished);
                break;
            }
        }
        assert!(calls < 20);
    }
    assert_eq!(total, 10, "every session queued exactly once across pages");
    assert_eq!(
        env.count(
            "SELECT count(*) FROM sessions WHERE operation_id = 'op_p' AND index_state = 'pending'"
        )
        .await,
        10
    );
    assert!(calls >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_indexed_sessions_are_change_checked_regardless_of_age() {
    let env = Env::new().await;
    let p = env.omp("aging.jsonl", "aging", &["first"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let s = env.session_by_native("aging").await.unwrap();
    // pretend it was indexed long ago
    sqlx::query(
        "UPDATE sessions SET index_state='indexed', active_snapshot_id='snap_x' WHERE id = ?",
    )
    .bind(&s.id)
    .execute(&env.index.writer)
    .await
    .unwrap();
    env.clock.advance_ms(60 * DAY);
    auto(&d).await;
    assert_eq!(
        env.state_of("aging").await,
        "indexed",
        "unchanged known session is left alone"
    );
    // an append, 60 days later, is still noticed even though the file is far older than the retention window
    std::fs::write(&p, spool_body("aging", &["first", "second"])).unwrap();
    set_mtime(&p, T0 + 59 * DAY);
    auto(&d).await;
    assert_eq!(env.state_of("aging").await, "pending");
    assert!(
        env.session_by_native("aging")
            .await
            .unwrap()
            .active_snapshot_id
            .is_some(),
        "published data stays until replaced"
    );
}

fn spool_body(id: &str, texts: &[&str]) -> String {
    omp_body(id, texts)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_after_more_than_seven_days_resumes_from_the_prior_watermark() {
    let env = Env::new().await;
    env.omp("pre.jsonl", "pre", &["before"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    assert_eq!(source_row(&env).await.discovery_watermark, Some(T0));
    // daemon is down for 10 days; activity happened on day 1 and day 9; a copied ancient file appears too
    env.omp(
        "day1.jsonl",
        "day1",
        &["activity during downtime"],
        T0 + DAY,
    );
    env.omp("day9.jsonl", "day9", &["late activity"], T0 + 9 * DAY);
    env.omp("copied.jsonl", "copied", &["ancient copy"], T0 - 300 * DAY);
    env.clock.set_ms(T0 + 10 * DAY);
    // a brand-new service instance = process restart
    let d2 = svc(&env);
    let rep = auto(&d2).await;
    assert!(rep.complete);
    assert!(
        env.session_by_native("day1").await.is_some(),
        "now-minus-7d would have lost day 1; the watermark must not"
    );
    assert!(env.session_by_native("day9").await.is_some());
    assert!(env.session_by_native("copied").await.is_none());
    assert_eq!(
        source_row(&env).await.discovery_watermark,
        Some(T0 + 10 * DAY)
    );
    assert_eq!(
        source_row(&env).await.bootstrap_started_at,
        Some(T0),
        "no new bootstrap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_scan_never_advances_the_watermark_or_tombstones() {
    let env = Env::new().await;
    let a = env.omp("a/one.jsonl", "one", &["hello one"], T0 - DAY);
    env.omp("b/two.jsonl", "two", &["hello two"], T0 - DAY);
    let d = svc(&env);
    assert!(auto(&d).await.complete);
    let wm = source_row(&env).await.discovery_watermark;
    assert_eq!(wm, Some(T0));

    // permission failure on one directory => partial scan
    let sub = a.parent().unwrap().to_path_buf();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    env.clock.advance_ms(120_000);
    let rep = auto(&d).await;
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!rep.complete, "{rep:?}");
    assert!(rep.errors > 0);
    let src = source_row(&env).await;
    assert_eq!(
        src.discovery_watermark, wm,
        "incomplete scan must not advance the watermark"
    );
    assert_eq!(src.last_scan_ok, Some(false));
    assert!(src.last_scan_error.is_some());
    assert_eq!(
        env.count("SELECT count(*) FROM sessions WHERE source_status != 'present'")
            .await,
        0,
        "nothing tombstoned"
    );
    // repeat to prove the second pass does not confirm a deletion either
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    env.clock.advance_ms(120_000);
    auto(&d).await;
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(env.count("SELECT count(*) FROM sessions WHERE source_status != 'present' OR missing_confirmed_at IS NOT NULL").await, 0);
    // recovery: a complete scan advances again
    env.clock.advance_ms(120_000);
    let rep = auto(&d).await;
    assert!(rep.complete);
    assert_eq!(
        source_row(&env).await.discovery_watermark,
        Some(T0 + 360_000)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_root_is_an_incomplete_scan_not_mass_deletion() {
    let env = Env::new().await;
    env.omp("a.jsonl", "a", &["x"], T0 - DAY);
    env.omp("b.jsonl", "b", &["y"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let moved = env.root.join("omp-away");
    std::fs::rename(&env.dir, &moved).unwrap();
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    assert!(!rep.complete);
    env.clock.advance_ms(60_000);
    auto(&d).await;
    std::fs::rename(&moved, &env.dir).unwrap();
    assert_eq!(env.count("SELECT count(*) FROM sessions WHERE source_status = 'present' AND tombstoned_at IS NULL").await, 2);
    assert_eq!(source_row(&env).await.discovery_watermark, Some(T0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paged_scan_with_error_in_an_early_page_stays_incomplete() {
    let env = Env::new().await;
    env.omp("a/x.jsonl", "ax", &["x"], T0 - DAY);
    env.omp("b/y.jsonl", "by", &["y"], T0 - DAY);
    env.omp("c/z.jsonl", "cz", &["z"], T0 - DAY);
    let mut d = svc(&env);
    d.page_size = 1;
    let sub = env.dir.join("a");
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    let rep = auto(&d).await;
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        !rep.complete,
        "the last page was clean but an earlier page had errors: {rep:?}"
    );
    assert_eq!(source_row(&env).await.discovery_watermark, None);
    assert_eq!(rep.scanned, 2, "later pages were still processed");
    // fixed, full paged scan completes
    assert!(auto(&d).await.complete);
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newly_enabled_source_gets_its_own_bootstrap() {
    let env = Env::with(|c| {
        c.sources[0].enabled = false;
    })
    .await;
    env.omp("a.jsonl", "a", &["hello"], T0 - 2 * DAY);
    let d = svc(&env);
    let rep = auto(&d).await;
    assert!(rep.error.is_some(), "disabled source is not scanned");
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 0);

    env.clock.set_ms(T0 + 30 * DAY);
    env.omp("b.jsonl", "b", &["hello"], T0 + 29 * DAY);
    env.omp("c.jsonl", "c", &["hello"], T0 + 10 * DAY); // older than 7 days at enable time
    let mut cfg = (*env.cfg).clone();
    cfg.sources[0].enabled = true;
    env.sources.reload(&cfg).unwrap();
    catalog::sync_sources(&env.index, &env.sources.entries())
        .await
        .unwrap();
    let rep = auto(&d).await;
    assert!(rep.complete);
    let src = source_row(&env).await;
    assert_eq!(src.bootstrap_started_at, Some(T0 + 30 * DAY));
    assert_eq!(src.bootstrap_cutoff, Some(T0 + 23 * DAY));
    assert!(env.session_by_native("b").await.is_some());
    assert!(env.session_by_native("c").await.is_none());
    assert!(env.session_by_native("a").await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rename_updates_the_path_instead_of_recreating_the_session() {
    let env = Env::new().await;
    let p = env.omp("before.jsonl", "ren", &["hello"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let before = env.session_by_native("ren").await.unwrap();
    sqlx::query(
        "UPDATE sessions SET index_state='indexed', active_snapshot_id='snap_r' WHERE id = ?",
    )
    .bind(&before.id)
    .execute(&env.index.writer)
    .await
    .unwrap();
    let q = env.path("renamed/after.jsonl");
    std::fs::create_dir_all(q.parent().unwrap()).unwrap();
    std::fs::rename(&p, &q).unwrap();
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    assert_eq!(rep.moved, 1, "{rep:?}");
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 1);
    let after = env.session_by_native("ren").await.unwrap();
    assert_eq!(after.id, before.id, "same internal session id");
    assert_eq!(after.path.as_deref(), Some(q.to_str().unwrap()));
    assert_eq!(
        after.index_state, "indexed",
        "an unchanged moved file is not re-indexed"
    );
    assert_eq!(after.source_status, "present");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_deletion_tombstones_then_removes_results_on_the_next_completed_scan() {
    let env = Env::new().await;
    let p = env.omp("gone.jsonl", "gone", &["hello gone"], T0 - DAY);
    env.omp("stays.jsonl", "stays", &["hello stays"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let id = env.session_by_native("gone").await.unwrap().id;
    // give it published derived data
    sqlx::query("INSERT INTO snapshots(id, session_id, kind, state, source_revision, fingerprint, generation, prefix_end, created_at) VALUES ('snap_g', ?, 'indexed','published','1:1:1:1:1','fp',1,1,1)").bind(&id).execute(&env.index.writer).await.unwrap();
    sqlx::query("INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, rendered_tokens) VALUES ('c1','snap_g',?,'r',0,'hello gone',zeroblob(32),3)").bind(&id).execute(&env.index.writer).await.unwrap();
    sqlx::query(
        "UPDATE sessions SET index_state='indexed', active_snapshot_id='snap_g' WHERE id = ?",
    )
    .bind(&id)
    .execute(&env.index.writer)
    .await
    .unwrap();

    std::fs::remove_file(&p).unwrap();
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    assert_eq!(rep.newly_missing, 1);
    let s = catalog::session(&env.index, &id).await.unwrap().unwrap();
    assert_eq!(s.source_status, "missing");
    assert!(
        s.active_snapshot_id.is_some(),
        "results stay until the next completed reconciliation"
    );
    assert_eq!(env.count("SELECT count(*) FROM chunks").await, 1);
    assert_eq!(env.state_of("stays").await, "pending");

    // an incomplete scan in between confirms nothing
    let sub = env.dir.join("blocked");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).unwrap();
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(!rep.complete);
    assert_eq!(env.count("SELECT count(*) FROM chunks").await, 1);

    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    assert_eq!(rep.purged, 1);
    let s = catalog::session(&env.index, &id).await.unwrap().unwrap();
    assert!(s.active_snapshot_id.is_none());
    assert_eq!(
        env.count("SELECT count(*) FROM chunks").await,
        0,
        "removed from search"
    );
    assert_eq!(env.count("SELECT count(*) FROM sessions WHERE tombstoned_at IS NOT NULL AND missing_confirmed_at IS NOT NULL").await, 1, "content-free tombstone row kept");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reappearing_file_revives_a_tombstoned_session() {
    let env = Env::new().await;
    let p = env.omp("flaky.jsonl", "flaky", &["hello"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let bytes = std::fs::read(&p).unwrap();
    std::fs::remove_file(&p).unwrap();
    env.clock.advance_ms(60_000);
    auto(&d).await;
    assert_eq!(
        env.session_by_native("flaky").await.unwrap().source_status,
        "missing"
    );
    std::fs::write(&p, bytes).unwrap();
    set_mtime(&p, T0 - DAY);
    env.clock.advance_ms(60_000);
    auto(&d).await;
    let s = env.session_by_native("flaky").await.unwrap();
    assert_eq!(s.source_status, "present");
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn copied_files_stay_separate_sessions_and_old_copies_are_not_backfilled() {
    let env = Env::new().await;
    let orig = env.omp("orig.jsonl", "dup", &["same text"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let first = env.session_by_native("dup").await.unwrap();
    // a recent copy while the original still exists: a separate session, original untouched
    std::fs::copy(&orig, env.path("copy.jsonl")).unwrap();
    set_mtime(&env.path("copy.jsonl"), T0 + 30_000); // a real copy is fresh (cp without -p), after the watermark
    // an ancient copy is not eligible
    std::fs::copy(&orig, env.path("ancient-copy.jsonl")).unwrap();
    set_mtime(&env.path("ancient-copy.jsonl"), T0 - 100 * DAY);
    env.clock.advance_ms(60_000);
    auto(&d).await;
    assert_eq!(
        env.count("SELECT count(*) FROM sessions WHERE native_id = 'dup'")
            .await,
        2,
        "original + one copy"
    );
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM sessions WHERE native_id = 'dup' ORDER BY id")
            .fetch_all(&env.index.reader)
            .await
            .unwrap();
    assert_ne!(ids[0], ids[1]);
    let again = catalog::session(&env.index, &first.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        again.path.as_deref(),
        Some(orig.to_str().unwrap()),
        "the copy did not steal the original's path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exclusion_persists_across_rediscovery_restart_and_row_recreation() {
    let env = Env::new().await;
    let p = env.omp("x.jsonl", "excl", &["hello"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let id = env.session_by_native("excl").await.unwrap().id;
    assert!(
        catalog::exclude_session(&env.index, &id, "test", T0)
            .await
            .unwrap()
    );
    assert_eq!(
        env.count("SELECT count(*) FROM exclusions WHERE kind='session'")
            .await,
        1
    );
    assert_eq!(env.session_by_native("excl").await.unwrap().excluded, true);

    // the file changes and is rediscovered: still excluded, never pending
    std::fs::write(&p, omp_body("excl", &["hello", "more"])).unwrap();
    set_mtime(&p, T0 - DAY / 2);
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    let s = env.session_by_native("excl").await.unwrap();
    assert!(s.excluded);
    assert_eq!(
        rep.skipped_excluded, 1,
        "the changed excluded file is skipped, not queued"
    );
    assert!(s.task_id.is_none());

    // restart: fresh service, fresh registry
    let d2 = svc(&env);
    env.clock.advance_ms(60_000);
    auto(&d2).await;
    assert!(env.session_by_native("excl").await.unwrap().excluded);

    // the sessions row is lost entirely (e.g. rebuilt catalog): the stable id + exclusions table win
    sqlx::query("DELETE FROM sessions WHERE id = ?")
        .bind(&id)
        .execute(&env.index.writer)
        .await
        .unwrap();
    set_mtime(&p, T0 + 100_000);
    env.clock.advance_ms(60_000);
    auto(&d2).await;
    let s = env.session_by_native("excl").await.unwrap();
    assert_eq!(s.id, id, "session ids are stable");
    assert!(s.excluded, "exclusion survives recreation of the row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_requests_do_not_override_exclusions() {
    let env = Env::new().await;
    env.omp("x.jsonl", "excl2", &["hello"], T0 - 50 * DAY);
    let d = svc(&env);
    d.run_explicit(
        "omp",
        Selection::All,
        Mode::Enqueue("op".into()),
        None,
        10,
        flag(),
    )
    .await
    .unwrap();
    let id = env.session_by_native("excl2").await.unwrap().id;
    catalog::exclude_session(&env.index, &id, "test", T0)
        .await
        .unwrap();
    sqlx::query("UPDATE sessions SET index_state='failed' WHERE id=?")
        .bind(&id)
        .execute(&env.index.writer)
        .await
        .unwrap();
    let rep = d
        .run_explicit(
            "omp",
            Selection::All,
            Mode::Enqueue("op2".into()),
            None,
            10,
            flag(),
        )
        .await
        .unwrap();
    assert_eq!(rep.skipped_excluded, 1);
    assert_eq!(env.state_of("excl2").await, "failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subagent_sessions_are_excluded_unless_the_source_includes_them() {
    let build = |include: bool| async move {
        let tmp_env = Env::with(move |c| {
            let dir = c.sources[0].roots[0].clone();
            c.sources = vec![SourceConfig {
                id: "omp".into(),
                kind: HarnessKind::Claude,
                enabled: true,
                roots: vec![dir],
                include_subagents: include,
            }];
        })
        .await;
        write_claude(
            &tmp_env.dir,
            "main.jsonl",
            "main-1",
            None,
            "main conversation",
            T0 - DAY,
        );
        write_claude(
            &tmp_env.dir,
            "sub.jsonl",
            "sub-1",
            Some("main-1"),
            "delegated work",
            T0 - DAY,
        );
        tmp_env
    };
    let env = build(false).await;
    let d = svc(&env);
    let rep = auto(&d).await;
    assert!(rep.complete);
    let sub = env
        .session_by_native("sub-1")
        .await
        .expect("kind is preserved in the catalog");
    assert_eq!(sub.session_kind, "subagent");
    assert_eq!(
        env.session_by_native("main-1").await.unwrap().session_kind,
        "interactive"
    );
    assert_eq!(rep.skipped_subagent, 1);

    let env = build(true).await;
    let d = svc(&env);
    let rep = auto(&d).await;
    assert_eq!(rep.skipped_subagent, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_is_separated_across_sources() {
    let tmp = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(tmp.path()).unwrap();
    let (d1, d2) = (root.join("one"), root.join("two"));
    std::fs::create_dir_all(&d1).unwrap();
    std::fs::create_dir_all(&d2).unwrap();
    write_omp(&d1, "s.jsonl", "shared-native-id", &["hello"], T0 - DAY);
    write_omp(&d2, "s.jsonl", "shared-native-id", &["hello"], T0 - DAY);
    let (d1c, d2c) = (
        d1.to_string_lossy().into_owned(),
        d2.to_string_lossy().into_owned(),
    );
    let env = Env::with(move |c| {
        c.sources = vec![
            SourceConfig {
                id: "omp".into(),
                kind: HarnessKind::Omp,
                enabled: true,
                roots: vec![d1c],
                include_subagents: false,
            },
            SourceConfig {
                id: "omp2".into(),
                kind: HarnessKind::Omp,
                enabled: true,
                roots: vec![d2c],
                include_subagents: false,
            },
        ];
    })
    .await;
    let d = svc(&env);
    d.run_auto("omp", flag()).await.unwrap();
    d.run_auto("omp2", flag()).await.unwrap();
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM sessions WHERE native_id = 'shared-native-id' ORDER BY source_id",
    )
    .fetch_all(&env.index.reader)
    .await
    .unwrap();
    assert_eq!(
        ids.len(),
        2,
        "same native id in different sources is two sessions"
    );
    assert_ne!(ids[0], ids[1]);
    drop(tmp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recently_modified_files_are_debounced_and_hold_the_watermark_back() {
    let env = Env::new().await;
    env.omp("hot.jsonl", "hot", &["still writing"], T0 - 2_000); // 2 s ago, debounce is 5 s
    env.omp("calm.jsonl", "calm", &["done"], T0 - DAY);
    let d = svc(&env);
    let rep = auto(&d).await;
    assert_eq!(rep.debounced, 1);
    assert!(env.session_by_native("hot").await.is_none());
    assert!(env.session_by_native("calm").await.is_some());
    let wm = source_row(&env).await.discovery_watermark.unwrap();
    assert!(
        wm <= T0 - 2_000,
        "watermark {wm} must not pass the debounced file's mtime"
    );
    env.clock.advance_ms(60_000);
    let rep = auto(&d).await;
    assert_eq!(rep.debounced, 0);
    assert!(
        env.session_by_native("hot").await.is_some(),
        "picked up once it settled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unchanged_rescan_changes_nothing() {
    let env = Env::new().await;
    env.omp("a.jsonl", "a", &["hello"], T0 - DAY);
    let d = svc(&env);
    auto(&d).await;
    let id = env.session_by_native("a").await.unwrap().id;
    sqlx::query("UPDATE sessions SET index_state='indexed', active_snapshot_id='s', index_attempts=0 WHERE id=?").bind(&id).execute(&env.index.writer).await.unwrap();
    for _ in 0..3 {
        env.clock.advance_ms(60_000);
        let rep = auto(&d).await;
        assert_eq!((rep.new_sessions, rep.changed, rep.moved), (0, 0, 0));
    }
    assert_eq!(env.state_of("a").await, "indexed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_discovery_of_one_source_is_coalesced() {
    let env = Env::new().await;
    for i in 0..30 {
        env.omp(
            &format!("f{i}.jsonl"),
            &format!("n{i}"),
            &["hello"],
            T0 - DAY,
        );
    }
    let d = Arc::new(svc(&env));
    let (a, b) = (d.clone(), d.clone());
    let (x, y) = tokio::join!(
        tokio::spawn(async move { a.run_auto("omp", flag()).await.unwrap() }),
        tokio::spawn(async move { b.run_auto("omp", flag()).await.unwrap() })
    );
    let (x, y) = (x.unwrap(), y.unwrap());
    assert!(
        x.coalesced != y.coalesced || (!x.coalesced && !y.coalesced),
        "at most one runs at a time: {x:?} {y:?}"
    );
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 30);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_corpora_are_scanned_in_pages() {
    let env = Env::new().await;
    for i in 0..1200 {
        env.omp(
            &format!("d{}/f{i:04}.jsonl", i % 7),
            &format!("n{i:04}"),
            &["hello"],
            T0 - DAY,
        );
    }
    let d = svc(&env); // default page = 500 files => three pages
    let rep = auto(&d).await;
    assert!(rep.complete, "{rep:?}");
    assert_eq!(rep.scanned, 1200);
    assert_eq!(env.count("SELECT count(*) FROM sessions").await, 1200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_discovery_is_incomplete() {
    let env = Env::new().await;
    env.omp("a.jsonl", "a", &["hello"], T0 - DAY);
    let d = svc(&env);
    let c = Arc::new(AtomicBool::new(true));
    let rep = d.run_auto("omp", c).await.unwrap();
    assert!(!rep.complete);
    assert_eq!(source_row(&env).await.discovery_watermark, None);
}
