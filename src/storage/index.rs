//! `index.sqlite` handle: one writer connection, bounded readers (spec 4.1, 10.1).

use super::connect_with;
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::path::Path;
use std::time::Duration;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/index");

#[derive(Clone)]
pub struct Index {
    /// Max 1 connection: serializes writers. Never hold a transaction across provider calls.
    pub writer: SqlitePool,
    /// Max 4 connections, `query_only`.
    pub reader: SqlitePool,
}

impl Index {
    pub async fn open(path: &Path, busy_timeout: Duration) -> anyhow::Result<Self> {
        let writer = connect_with(path, 1, busy_timeout, false).await?;
        MIGRATOR.run(&writer).await?;
        let reader = connect_with(path, 4, busy_timeout, true).await?;
        Ok(Self { writer, reader })
    }

    /// In-memory-free test helper: a temp-file index.
    pub async fn open_for_test(dir: &Path) -> anyhow::Result<Self> {
        Self::open(&dir.join("index.sqlite"), Duration::from_secs(5)).await
    }

    pub async fn epoch(&self) -> Result<i64, sqlx::Error> {
        let v: String = sqlx::query_scalar("SELECT value FROM app_meta WHERE key='index_epoch'")
            .fetch_one(&self.reader)
            .await?;
        Ok(v.parse().unwrap_or(0))
    }
}

/// Increment `index_epoch` inside the caller's write transaction.
pub async fn bump_epoch(tx: &mut Transaction<'_, Sqlite>) -> Result<i64, sqlx::Error> {
    sqlx::query("UPDATE app_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key='index_epoch'").execute(&mut **tx).await?;
    let v: String = sqlx::query_scalar("SELECT value FROM app_meta WHERE key='index_epoch'")
        .fetch_one(&mut **tx)
        .await?;
    Ok(v.parse().unwrap_or(0))
}

/// Retire a snapshot inside the caller's write transaction. Chunks (and FTS rows via trigger) are
/// always removed; if a live lease pins the snapshot's manifest the conversational data is kept as
/// `retired_leased` until the lease expires (spec 10.4 exception), else the snapshot is deleted.
pub async fn retire_snapshot(
    tx: &mut Transaction<'_, Sqlite>,
    snapshot_id: &str,
    now_ms: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM chunks WHERE snapshot_id = ?")
        .bind(snapshot_id)
        .execute(&mut **tx)
        .await?;
    let pinned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lease_manifests lm JOIN read_manifests m ON m.id = lm.manifest_id \
         JOIN read_leases l ON l.id = lm.lease_id WHERE m.snapshot_id = ? AND l.state='live' AND l.expires_at > ?",
    )
    .bind(snapshot_id)
    .bind(now_ms)
    .fetch_one(&mut **tx)
    .await?;
    if pinned > 0 {
        sqlx::query("UPDATE snapshots SET state='retired_leased' WHERE id = ?")
            .bind(snapshot_id)
            .execute(&mut **tx)
            .await?;
    } else {
        sqlx::query("DELETE FROM snapshots WHERE id = ?")
            .bind(snapshot_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}
