use spool::domain::SessionKind;
use spool::storage::WAL_SIZE_LIMIT_BYTES;
use spool::storage::catalog;
use spool::storage::index::{Index, retire_snapshot};
use spool::transcript::selection::Snap;

#[tokio::test]
async fn failed_native_scan_does_not_publish_or_mark_sessions_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let ix = Index::open_for_test(tmp.path()).await.unwrap();
    sqlx::query("INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('oc','opencode','[]',1)")
        .execute(&ix.writer)
        .await
        .unwrap();
    let session = |native_id| catalog::NewSession {
        source_id: "oc",
        native_id,
        discriminator: "",
        kind: SessionKind::Interactive,
        cwd: None,
        created_at: None,
        last_activity_at: None,
        eligibility: "active",
        operation_id: None,
    };
    let observations = [
        catalog::NativeSessionObservation {
            session: session("native-a"),
            event_sequence: Some(1),
        },
        catalog::NativeSessionObservation {
            session: session("native-b"),
            event_sequence: Some(2),
        },
    ];
    let ids =
        catalog::apply_native_scan(&ix, "oc", catalog::NativeScan::Complete(&observations), 10)
            .await
            .unwrap();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    let failed = catalog::apply_native_scan(&ix, "oc", catalog::NativeScan::Failed, 20)
        .await
        .unwrap();
    assert!(failed.is_empty());
    let rows: Vec<(String, String, Option<i64>, i64)> = sqlx::query_as(
        "SELECT s.native_id,s.source_status,n.event_sequence,n.last_checked_at \
         FROM sessions s JOIN native_session_observations n ON n.session_id=s.id \
         WHERE s.source_id='oc' ORDER BY s.native_id",
    )
    .fetch_all(&ix.reader)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("native-a".into(), "present".into(), Some(1), 10),
            ("native-b".into(), "present".into(), Some(2), 10),
        ]
    );
    let bad = [
        catalog::NativeSessionObservation {
            session: session("native-a"),
            event_sequence: Some(5),
        },
        catalog::NativeSessionObservation {
            session: session("native-c"),
            event_sequence: Some(-1),
        },
    ];
    assert!(
        catalog::apply_native_scan(&ix, "oc", catalog::NativeScan::Complete(&bad), 30)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sessions WHERE source_id='oc'")
        .fetch_one(&ix.reader)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let marker: i64 = sqlx::query_scalar(
        "SELECT event_sequence FROM native_session_observations WHERE session_id=?",
    )
    .bind(&ids[0])
    .fetch_one(&ix.reader)
    .await
    .unwrap();
    assert_eq!(marker, 1);
}

#[tokio::test]
async fn native_sessions_share_a_source_without_file_catalog_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let ix = Index::open_for_test(tmp.path()).await.unwrap();
    sqlx::query("INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('oc','opencode','[]',1)")
        .execute(&ix.writer)
        .await
        .unwrap();
    let entry = |native_id| catalog::NewSession {
        source_id: "oc",
        native_id,
        discriminator: "",
        kind: SessionKind::Interactive,
        cwd: None,
        created_at: None,
        last_activity_at: None,
        eligibility: "active",
        operation_id: None,
    };
    let first = [
        catalog::NativeSessionObservation {
            session: entry("session-a"),
            event_sequence: Some(7),
        },
        catalog::NativeSessionObservation {
            session: entry("session-b"),
            event_sequence: Some(7),
        },
    ];
    let ids = catalog::apply_native_scan(&ix, "oc", catalog::NativeScan::Complete(&first), 10)
        .await
        .unwrap();
    assert_ne!(ids[0], ids[1]);
    let second = [catalog::NativeSessionObservation {
        session: entry("session-a"),
        event_sequence: None,
    }];
    catalog::apply_native_scan(&ix, "oc", catalog::NativeScan::Complete(&second), 20)
        .await
        .unwrap();
    let rows: Vec<(String, Option<i64>, i64)> =
        sqlx::query_as("SELECT session_id,event_sequence,last_checked_at FROM native_session_observations ORDER BY session_id")
            .fetch_all(&ix.reader).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|(id, seq, checked)| id == &ids[0] && seq.is_none() && *checked == 20)
    );
    assert!(
        rows.iter()
            .any(|(id, seq, checked)| id == &ids[1] && *seq == Some(7) && *checked == 10)
    );
    let files: i64 = sqlx::query_scalar("SELECT count(*) FROM source_files")
        .fetch_one(&ix.reader)
        .await
        .unwrap();
    assert_eq!(files, 0);
}

#[tokio::test]
async fn record_locator_migration_preserves_legacy_and_rejects_invalid_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let ix = Index::open_for_test(tmp.path()).await.unwrap();
    seed(&ix).await;
    sqlx::query("INSERT INTO records(snapshot_id,key,ord,kind,offset,len,digest) VALUES ('sn_a','r',0,'message',12,34,zeroblob(32))")
        .execute(&ix.writer).await.unwrap();
    let snap = Snap::load(&ix.reader, "sn_a").await.unwrap();
    assert_eq!((snap.recs[0].offset, snap.recs[0].len), (12, 34));

    sqlx::query("UPDATE records SET locator_payload=? WHERE snapshot_id='sn_a'")
        .bind(r#"{"kind":"opencode_sqlite_v1","session_id":"s","message_id":"m"}"#)
        .execute(&ix.writer)
        .await
        .unwrap();
    assert!(Snap::load(&ix.reader, "sn_a").await.is_ok());
    for payload in [
        "not json",
        r#"{"kind":"unknown"}"#,
        r#"{"kind":"opencode_sqlite_v1","session_id":"","message_id":"m"}"#,
        r#"{"kind":"opencode_sqlite_v1","session_id":"s"}"#,
    ] {
        sqlx::query("UPDATE records SET locator_payload=? WHERE snapshot_id='sn_a'")
            .bind(payload)
            .execute(&ix.writer)
            .await
            .unwrap();
        assert!(Snap::load(&ix.reader, "sn_a").await.is_err());
    }
}

async fn seed(ix: &Index) {
    let w = &ix.writer;
    for q in [
        "INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('omp','omp','[]',1)",
        "INSERT INTO sessions(id,source_id,native_id,discovered_at) VALUES ('ss_a','omp','n1',0),('ss_b','omp','n2',0)",
        "INSERT INTO snapshots(id,session_id,kind,state,source_revision,fingerprint,generation,prefix_end,created_at) VALUES ('sn_a','ss_a','indexed','published','r','f',1,10,0)",
        "INSERT INTO chunks(logical_id,snapshot_id,session_id,run_key,ordinal,body,input_hash,rendered_tokens) VALUES ('c1','sn_a','ss_a','run',0,'USER: webhook receiver',zeroblob(32),4)",
    ] {
        sqlx::query(q).execute(w).await.unwrap();
    }
}

#[tokio::test]
async fn migration_applies_fts_triggers_and_guards_hold() {
    let tmp = tempfile::tempdir().unwrap();
    let ix = Index::open_for_test(tmp.path()).await.unwrap();
    seed(&ix).await;
    let hits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH 'webhook'")
            .fetch_one(&ix.reader)
            .await
            .unwrap();
    assert_eq!(hits, 1);
    sqlx::query("INSERT INTO chunks_fts(chunks_fts, rank) VALUES('integrity-check', 1)")
        .execute(&ix.writer)
        .await
        .unwrap();

    // cross-session chunk reference is rejected by the composite FK
    let bad = sqlx::query("INSERT INTO chunks(logical_id,snapshot_id,session_id,run_key,ordinal,body,input_hash,rendered_tokens) VALUES ('c2','sn_a','ss_b','run',0,'x',zeroblob(32),1)")
        .execute(&ix.writer)
        .await;
    assert!(bad.is_err(), "cross-session chunk must be rejected");

    // vectors require length(vec) = dimensions * 4
    sqlx::query("INSERT INTO embedding_profiles(id,provider,endpoint_identity,model,dimensions,rendering_version,normalization,generation,created_at) VALUES ('p','openai','e','m',2,1,'unit',1,0)").execute(&ix.writer).await.unwrap();
    let bad = sqlx::query("INSERT INTO embeddings(profile_id,input_hash,dimensions,vec,created_at) VALUES ('p',zeroblob(32),2,zeroblob(7),0)").execute(&ix.writer).await;
    assert!(bad.is_err());
    sqlx::query("INSERT INTO embeddings(profile_id,input_hash,dimensions,vec,created_at) VALUES ('p',zeroblob(32),2,zeroblob(8),0)").execute(&ix.writer).await.unwrap();

    // retire: no lease -> snapshot deleted, chunks and FTS rows gone
    let mut tx = ix.writer.begin().await.unwrap();
    retire_snapshot(&mut tx, "sn_a", 0).await.unwrap();
    tx.commit().await.unwrap();
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH 'webhook'")
            .fetch_one(&ix.reader)
            .await
            .unwrap();
    assert_eq!(n, 0);
    sqlx::query("INSERT INTO chunks_fts(chunks_fts, rank) VALUES('integrity-check', 1)")
        .execute(&ix.writer)
        .await
        .unwrap();
    // readers are query_only
    assert!(
        sqlx::query("DELETE FROM app_meta")
            .execute(&ix.reader)
            .await
            .is_err()
    );
}

fn wal(dir: &std::path::Path) -> u64 {
    std::fs::metadata(dir.join("index.sqlite-wal"))
        .unwrap()
        .len()
}

async fn ckpt(ix: &Index) -> (i64, i64) {
    let (_busy, log, done): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(&ix.writer)
        .await
        .unwrap();
    (log, done)
}

async fn small_write(ix: &Index) {
    sqlx::query("INSERT INTO scratch VALUES (x'00')")
        .execute(&ix.writer)
        .await
        .unwrap();
}

/// One transaction of limit + 8 MiB, so the WAL file must grow past the limit.
async fn big_write(ix: &Index) {
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?) \
         INSERT INTO scratch SELECT zeroblob(1048576) FROM n",
    )
    .bind((WAL_SIZE_LIMIT_BYTES >> 20) as i64 + 8)
    .execute(&ix.writer)
    .await
    .unwrap();
}

#[tokio::test]
async fn wal_resets_to_size_limit_unless_a_reader_pins_it() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let ix = Index::open_for_test(dir).await.unwrap();
    for pool in [&ix.writer, &ix.reader] {
        let n: i64 = sqlx::query_scalar("PRAGMA journal_size_limit")
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(n as u64, WAL_SIZE_LIMIT_BYTES);
    }
    sqlx::query("CREATE TABLE scratch(b BLOB)")
        .execute(&ix.writer)
        .await
        .unwrap();

    big_write(&ix).await;
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM scratch")
        .fetch_one(&ix.reader)
        .await
        .unwrap();
    assert!(wal(dir) > WAL_SIZE_LIMIT_BYTES);
    let (log, done) = ckpt(&ix).await;
    assert_eq!(
        log, done,
        "PASSIVE copies every frame when no reader is pinned"
    );
    small_write(&ix).await;
    assert!(
        wal(dir) <= WAL_SIZE_LIMIT_BYTES,
        "first commit after reset trims the WAL"
    );

    small_write(&ix).await;
    let mut pin = ix.reader.begin().await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM scratch")
        .fetch_one(&mut *pin)
        .await
        .unwrap();
    big_write(&ix).await;
    let (log, done) = ckpt(&ix).await;
    assert!(done < log, "PASSIVE stops at the pinned reader");
    small_write(&ix).await;
    assert!(
        wal(dir) > WAL_SIZE_LIMIT_BYTES,
        "a pinned WAL is not reset or trimmed"
    );

    pin.rollback().await.unwrap();
    let (log, done) = ckpt(&ix).await;
    assert_eq!(log, done);
    small_write(&ix).await;
    assert!(wal(dir) <= WAL_SIZE_LIMIT_BYTES);
}
