//! Milestone 0: SQLx opens the search database and FTS5 (external-content, spec 10.3) works.

use spool::storage;
use std::time::Duration;

#[tokio::test]
async fn fts5_external_content_table_works_through_sqlx() {
    let tmp = tempfile::tempdir().unwrap();
    let pool = storage::connect(&tmp.path().join("index.sqlite"), 2, Duration::from_secs(5))
        .await
        .unwrap();
    let ver = storage::check_fts5(&pool).await.unwrap();
    eprintln!("SQLITE_VERSION {ver}");

    for sql in [
        "CREATE TABLE chunks(rowid INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        "CREATE VIRTUAL TABLE chunks_fts USING fts5(body, content='chunks', content_rowid='rowid', tokenize='unicode61 remove_diacritics 2')",
        "INSERT INTO chunks(rowid, body) VALUES (1, 'USER: could the webhook receiver run locally?')",
        "INSERT INTO chunks_fts(rowid, body) VALUES (1, 'USER: could the webhook receiver run locally?')",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    let hit: i64 = sqlx::query_scalar(
        "SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH 'webhook' ORDER BY bm25(chunks_fts)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(hit, 1);
    sqlx::query("INSERT INTO chunks_fts(chunks_fts, rank) VALUES('integrity-check', 1)")
        .execute(&pool)
        .await
        .unwrap();

    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&pool)
        .await
        .unwrap();
    let jm: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((sync, jm.as_str(), fk), (2, "wal", 1)); // 2 = FULL
}
