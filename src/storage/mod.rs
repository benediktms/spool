//! SQLite access shared by `index.sqlite` and `jobs.sqlite` (spec 10.1).

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::path::Path;
use std::time::Duration;

pub mod catalog;
pub mod index;

/// WAL bytes SQLite keeps on disk after a WAL reset (`PRAGMA journal_size_limit`): about 16x the
/// default auto-checkpoint WAL (1000 pages of 4 KiB), so normal checkpoint cycles never truncate
/// and grow again. Not a cap: a large transaction or a reader that pins the WAL still grows it.
pub const WAL_SIZE_LIMIT_BYTES: u64 = 64 << 20;

/// Open a pool with WAL, a WAL size limit, foreign keys, FULL synchronous and a bounded busy timeout on every
/// connection. `create` is only for the daemon/maintenance owner.
pub async fn connect(
    path: &Path,
    max_connections: u32,
    busy_timeout: Duration,
) -> Result<SqlitePool, sqlx::Error> {
    connect_with(path, max_connections, busy_timeout, false).await
}

pub async fn connect_with(
    path: &Path,
    max_connections: u32,
    busy_timeout: Duration,
    query_only: bool,
) -> Result<SqlitePool, sqlx::Error> {
    let mut opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .foreign_keys(true)
        .busy_timeout(busy_timeout)
        .pragma("journal_size_limit", WAL_SIZE_LIMIT_BYTES.to_string());
    if query_only {
        opts = opts.pragma("query_only", "ON");
    }
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await
}

/// Startup capability test (spec 10.3): FTS5 must exist in the linked SQLite build.
pub async fn check_fts5(pool: &SqlitePool) -> Result<String, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    sqlx::query("CREATE VIRTUAL TABLE temp.spool_fts_probe USING fts5(body, tokenize='unicode61 remove_diacritics 2')")
        .execute(&mut *conn)
        .await?;
    sqlx::query("INSERT INTO temp.spool_fts_probe(body) VALUES ('hello probe')")
        .execute(&mut *conn)
        .await?;
    let hits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM temp.spool_fts_probe WHERE spool_fts_probe MATCH 'probe'",
    )
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query("DROP TABLE temp.spool_fts_probe")
        .execute(&mut *conn)
        .await?;
    if hits != 1 {
        return Err(sqlx::Error::Protocol(
            "fts5 probe returned unexpected hit count".into(),
        ));
    }
    sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&mut *conn)
        .await
}
