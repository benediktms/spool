//! Source/session/source-file catalog in `index.sqlite` (spec 10.2). Every write is an explicit
//! upsert (never `INSERT OR REPLACE`, which would cascade through provenance rows). Functions take a
//! connection so callers choose the transaction; none of them holds one across I/O.

use crate::domain::SessionKind;
use crate::ids;
use crate::source::SourceFileInfo;
use crate::sources::SourceEntry;
use crate::storage::index::Index;
use sqlx::{Row, SqliteConnection};

pub type Result<T, E = sqlx::Error> = std::result::Result<T, E>;

pub const DAY_MS: i64 = 86_400_000;

/// Sessions this source will actually index: a subagent only when its source has include_subagents.
/// Requires the `sessions` table aliased as `s`.
pub const INDEXABLE_KIND: &str = "(s.session_kind != 'subagent' OR EXISTS (SELECT 1 FROM sources src_k WHERE src_k.id = s.source_id AND src_k.include_subagents = 1))";

/// `size:mtime_ms:dev:ino` — the revision string the ingest engine compares (first four parts).
pub fn revision(len: u64, mtime_ms: i64, dev: u64, ino: u64) -> String {
    format!("{len}:{mtime_ms}:{dev}:{ino}")
}

#[derive(Debug, Clone)]
pub struct SourceRow {
    pub id: String,
    pub kind: String,
    pub enabled: bool,
    pub include_subagents: bool,
    pub config_revision: i64,
    pub bootstrap_started_at: Option<i64>,
    pub bootstrap_cutoff: Option<i64>,
    pub discovery_watermark: Option<i64>,
    pub last_scan_at: Option<i64>,
    pub last_scan_ok: Option<bool>,
    pub last_scan_error: Option<String>,
}

fn source_row(r: &sqlx::sqlite::SqliteRow) -> SourceRow {
    SourceRow {
        id: r.get("id"),
        kind: r.get("kind"),
        enabled: r.get::<i64, _>("enabled") != 0,
        include_subagents: r.get::<i64, _>("include_subagents") != 0,
        config_revision: r.get("config_revision"),
        bootstrap_started_at: r.get("bootstrap_started_at"),
        bootstrap_cutoff: r.get("bootstrap_cutoff"),
        discovery_watermark: r.get("discovery_watermark"),
        last_scan_at: r.get("last_scan_at"),
        last_scan_ok: r.get::<Option<i64>, _>("last_scan_ok").map(|v| v != 0),
        last_scan_error: r.get("last_scan_error"),
    }
}

/// Mirror the configured sources into the `sources` table. A source that flips from disabled to
/// enabled is "newly enabled" (spec 7.1): its bootstrap and watermark are reset so the next
/// discovery persists a fresh `bootstrap_started_at`/`bootstrap_cutoff`. Known sessions are kept.
pub async fn sync_sources(index: &Index, entries: &[SourceEntry]) -> Result<()> {
    let mut tx = index.writer.begin().await?;
    for e in entries {
        let roots = serde_json::to_string(
            &e.roots
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
        let prev: Option<(i64, String, i64)> = sqlx::query_as(
            "SELECT enabled, roots_json, include_subagents FROM sources WHERE id = ?",
        )
        .bind(&e.id)
        .fetch_optional(&mut *tx)
        .await?;
        let newly_enabled = e.enabled && prev.as_ref().is_none_or(|p| p.0 == 0);
        let changed = prev.as_ref().is_none_or(|p| {
            p.0 != e.enabled as i64 || p.1 != roots || p.2 != e.include_subagents as i64
        });
        sqlx::query(
            "INSERT INTO sources(id, kind, roots_json, enabled, include_subagents) VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(id) DO UPDATE SET kind = excluded.kind, roots_json = excluded.roots_json, enabled = excluded.enabled, \
             include_subagents = excluded.include_subagents, config_revision = sources.config_revision + ?6",
        )
        .bind(&e.id)
        .bind(e.kind.as_str())
        .bind(&roots)
        .bind(e.enabled as i64)
        .bind(e.include_subagents as i64)
        .bind(changed as i64)
        .execute(&mut *tx)
        .await?;
        if newly_enabled {
            sqlx::query("UPDATE sources SET bootstrap_started_at = NULL, bootstrap_cutoff = NULL, discovery_watermark = NULL WHERE id = ?")
                .bind(&e.id)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await
}

pub async fn source(index: &Index, id: &str) -> Result<Option<SourceRow>> {
    Ok(sqlx::query("SELECT * FROM sources WHERE id = ?")
        .bind(id)
        .fetch_optional(&index.reader)
        .await?
        .map(|r| source_row(&r)))
}

pub async fn sources(index: &Index) -> Result<Vec<SourceRow>> {
    Ok(sqlx::query("SELECT * FROM sources ORDER BY id")
        .fetch_all(&index.reader)
        .await?
        .iter()
        .map(source_row)
        .collect())
}

/// Persist `bootstrap_started_at` and `bootstrap_cutoff = started_at - days` once per enablement.
pub async fn begin_bootstrap(index: &Index, id: &str, now_ms: i64, days: u32) -> Result<SourceRow> {
    sqlx::query(
        "UPDATE sources SET bootstrap_started_at = ?2, bootstrap_cutoff = ?2 - ?3 WHERE id = ?1 AND bootstrap_started_at IS NULL",
    )
    .bind(id)
    .bind(now_ms)
    .bind(days as i64 * DAY_MS)
    .execute(&index.writer)
    .await?;
    let r = sqlx::query("SELECT * FROM sources WHERE id = ?")
        .bind(id)
        .fetch_one(&index.writer)
        .await?;
    Ok(source_row(&r))
}

/// Record a scan result. `watermark` is `Some` ONLY for a complete scan; an incomplete one leaves
/// the previous watermark untouched (spec 7.3).
pub async fn record_scan(
    index: &Index,
    id: &str,
    now_ms: i64,
    error: Option<&str>,
    watermark: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE sources SET last_scan_at = ?2, last_scan_ok = ?3, last_scan_error = ?4, \
         discovery_watermark = COALESCE(?5, discovery_watermark) WHERE id = ?1",
    )
    .bind(id)
    .bind(now_ms)
    .bind(error.is_none() as i64)
    .bind(error)
    .bind(watermark)
    .execute(&index.writer)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// sessions
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct NewSession<'a> {
    pub source_id: &'a str,
    pub native_id: &'a str,
    pub discriminator: &'a str,
    pub kind: SessionKind,
    pub cwd: Option<&'a str>,
    pub created_at: Option<i64>,
    pub last_activity_at: Option<i64>,
    pub eligibility: &'a str,
    pub operation_id: Option<&'a str>,
}

pub fn kind_str(k: SessionKind) -> &'static str {
    match k {
        SessionKind::Interactive => "interactive",
        SessionKind::Subagent => "subagent",
        SessionKind::Unknown => "unknown",
    }
}

/// Insert or refresh a session. Exclusions (per session or per source) are applied from the
/// `exclusions` table, so a rediscovered session can never come back unexcluded. On conflict only
/// presence and activity are refreshed: eligibility, index state and everything the ingest engine
/// owns are left alone. Returns `(session_id, created)`.
pub async fn upsert_session(
    conn: &mut SqliteConnection,
    s: &NewSession<'_>,
    now_ms: i64,
) -> Result<(String, bool)> {
    let id = ids::session_id(s.source_id, s.native_id, s.discriminator);
    let existed: Option<i64> = sqlx::query_scalar("SELECT 1 FROM sessions WHERE id = ?")
        .bind(&id)
        .fetch_optional(&mut *conn)
        .await?;
    let excl: Option<String> = sqlx::query_scalar(
        "SELECT COALESCE(reason, 'excluded') FROM exclusions WHERE (kind = 'session' AND target_id = ?1) OR (kind = 'source' AND target_id = ?2) LIMIT 1",
    )
    .bind(&id)
    .bind(s.source_id)
    .fetch_optional(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO sessions(id, source_id, native_id, discriminator, session_kind, cwd, created_at, last_activity_at, eligibility, \
         index_state, discovered_at, excluded, exclusion_reason, operation_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, ?11, ?12, ?13) \
         ON CONFLICT(id) DO UPDATE SET source_status = 'present', tombstoned_at = NULL, missing_confirmed_at = NULL, \
         last_activity_at = MAX(COALESCE(sessions.last_activity_at, 0), COALESCE(excluded.last_activity_at, 0)), \
         session_kind = CASE WHEN sessions.session_kind = 'unknown' THEN excluded.session_kind ELSE sessions.session_kind END, \
         excluded = MAX(sessions.excluded, excluded.excluded), \
         exclusion_reason = COALESCE(sessions.exclusion_reason, excluded.exclusion_reason)",
    )
    .bind(&id)
    .bind(s.source_id)
    .bind(s.native_id)
    .bind(s.discriminator)
    .bind(kind_str(s.kind))
    .bind(s.cwd)
    .bind(s.created_at)
    .bind(s.last_activity_at)
    .bind(s.eligibility)
    .bind(now_ms)
    .bind(excl.is_some() as i64)
    .bind(&excl)
    .bind(s.operation_id)
    .execute(&mut *conn)
    .await?;
    Ok((id, existed.is_none()))
}

pub async fn upsert_source_file(
    conn: &mut SqliteConnection,
    session_id: &str,
    file: &SourceFileInfo,
    now_ms: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO source_files(session_id, path, dev, ino, size, mtime_ms, exists_status, last_checked_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'present', ?7) \
         ON CONFLICT(session_id) DO UPDATE SET path = excluded.path, dev = excluded.dev, ino = excluded.ino, size = excluded.size, \
         mtime_ms = excluded.mtime_ms, exists_status = 'present', last_checked_at = excluded.last_checked_at",
    )
    .bind(session_id)
    .bind(file.path.to_string_lossy().as_ref())
    .bind(file.dev as i64)
    .bind(file.ino as i64)
    .bind(file.len as i64)
    .bind(file.mtime_ms)
    .bind(now_ms)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Mark paths as seen by the current scan without touching their stat.
pub async fn touch_seen(
    conn: &mut SqliteConnection,
    paths: &[String],
    scan_started_ms: i64,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE source_files SET last_checked_at = ?1 WHERE path IN (SELECT value FROM json_each(?2)) AND last_checked_at < ?1")
        .bind(scan_started_ms)
        .bind(serde_json::to_string(paths).unwrap_or_default())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct KnownFile {
    pub session_id: String,
    pub path: String,
    pub dev: Option<i64>,
    pub ino: Option<i64>,
    pub size: Option<i64>,
    pub mtime_ms: Option<i64>,
    pub exists_status: String,
    pub eligibility: String,
    pub index_state: String,
    pub has_snapshot: bool,
    pub excluded: bool,
    pub session_kind: String,
    pub native_id: String,
}

fn known(r: &sqlx::sqlite::SqliteRow) -> KnownFile {
    KnownFile {
        session_id: r.get("session_id"),
        path: r.get("path"),
        dev: r.get("dev"),
        ino: r.get("ino"),
        size: r.get("size"),
        mtime_ms: r.get("mtime_ms"),
        exists_status: r.get("exists_status"),
        eligibility: r.get("eligibility"),
        index_state: r.get("index_state"),
        has_snapshot: r.get::<Option<String>, _>("active_snapshot_id").is_some(),
        excluded: r.get::<i64, _>("excluded") != 0,
        session_kind: r.get("session_kind"),
        native_id: r.get("native_id"),
    }
}

const KNOWN_SELECT: &str = "SELECT f.session_id, f.path, f.dev, f.ino, f.size, f.mtime_ms, f.exists_status, s.eligibility, s.index_state, \
    s.active_snapshot_id, s.excluded, s.session_kind, s.native_id FROM source_files f JOIN sessions s ON s.id = f.session_id";

/// Known files of `source_id` among `paths` (one query per scan page).
pub async fn known_by_paths(
    conn: &mut SqliteConnection,
    source_id: &str,
    paths: &[String],
) -> Result<Vec<KnownFile>> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "{KNOWN_SELECT} WHERE s.source_id = ?1 AND f.path IN (SELECT value FROM json_each(?2))"
    )))
    .bind(source_id)
    .bind(serde_json::to_string(paths).unwrap_or_default())
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .map(known)
    .collect())
}

pub async fn known_by_identity(
    conn: &mut SqliteConnection,
    source_id: &str,
    dev: u64,
    ino: u64,
) -> Result<Option<KnownFile>> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "{KNOWN_SELECT} WHERE s.source_id = ?1 AND f.dev = ?2 AND f.ino = ?3 LIMIT 1"
    )))
    .bind(source_id)
    .bind(dev as i64)
    .bind(ino as i64)
    .fetch_optional(&mut *conn)
    .await?
    .map(|r| known(&r)))
}

/// Sessions of `source_id` with this native id (any discriminator).
pub async fn known_by_native(
    conn: &mut SqliteConnection,
    source_id: &str,
    native_id: &str,
) -> Result<Vec<KnownFile>> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "{KNOWN_SELECT} WHERE s.source_id = ?1 AND s.native_id = ?2 ORDER BY s.discriminator"
    )))
    .bind(source_id)
    .bind(native_id)
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .map(known)
    .collect())
}

/// Reset to `pending` after a change or an explicit request. Attempts and any defer/failure state
/// are cleared: new source content or explicit consent revives failed sessions.
pub async fn mark_pending(
    conn: &mut SqliteConnection,
    session_id: &str,
    operation_id: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "UPDATE sessions SET index_state = 'pending', index_attempts = 0, defer_until = NULL, index_error = NULL, \
         operation_id = COALESCE(?2, operation_id) WHERE id = ?1 AND excluded = 0",
    )
    .bind(session_id)
    .bind(operation_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn set_eligibility(
    conn: &mut SqliteConnection,
    session_id: &str,
    eligibility: &str,
) -> Result<()> {
    sqlx::query("UPDATE sessions SET eligibility = ?2 WHERE id = ?1")
        .bind(session_id)
        .bind(eligibility)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// missing files / tombstones (spec 7.3)
// ---------------------------------------------------------------------------------------------

/// Called ONLY after a complete, error-free scan of the whole source. Present files not seen by
/// this scan become `missing` with a content-free tombstone; files already `missing` and still not
/// seen (second completed reconciliation) lose their derived search data. Returns
/// `(newly_missing, purged)`.
pub async fn reconcile_missing(
    index: &Index,
    source_id: &str,
    scan_started_ms: i64,
    now_ms: i64,
) -> Result<(u64, u64)> {
    let mut tx = index.writer.begin().await?;
    let marked = sqlx::query(
        "UPDATE sessions SET source_status = 'missing', tombstoned_at = ?3 WHERE source_id = ?1 AND source_status = 'present' AND id IN \
         (SELECT session_id FROM source_files WHERE last_checked_at < ?2)",
    )
    .bind(source_id)
    .bind(scan_started_ms)
    .bind(now_ms)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query("UPDATE source_files SET exists_status = 'missing' WHERE last_checked_at < ?2 AND session_id IN (SELECT id FROM sessions WHERE source_id = ?1)")
        .bind(source_id)
        .bind(scan_started_ms)
        .execute(&mut *tx)
        .await?;
    // Second confirmation: previously tombstoned (tombstoned_at < this scan) and still unseen.
    let confirmed: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT s.id, s.active_snapshot_id FROM sessions s JOIN source_files f ON f.session_id = s.id \
         WHERE s.source_id = ?1 AND s.source_status = 'missing' AND s.missing_confirmed_at IS NULL AND s.tombstoned_at < ?2 AND f.last_checked_at < ?2",
    )
    .bind(source_id)
    .bind(scan_started_ms)
    .fetch_all(&mut *tx)
    .await?;
    for (sid, snap) in &confirmed {
        if let Some(snap) = snap {
            crate::storage::index::retire_snapshot(&mut tx, snap, now_ms).await?;
        }
        sqlx::query(
            "UPDATE sessions SET active_snapshot_id = NULL, generation = generation + 1, missing_confirmed_at = ?2, task_id = NULL, \
             index_error = 'source_removed' WHERE id = ?1",
        )
        .bind(sid)
        .bind(now_ms)
        .execute(&mut *tx)
        .await?;
    }
    if !confirmed.is_empty() {
        crate::storage::index::bump_epoch(&mut tx).await?;
    }
    tx.commit().await?;
    Ok((marked, confirmed.len() as u64))
}

// ---------------------------------------------------------------------------------------------
// exclusions (spec 7.3: stronger than rediscovery; removing one is an explicit admin action)
// ---------------------------------------------------------------------------------------------

/// Persist an exclusion and mirror it onto the session. Derived data removal is the caller's job
/// (`remove_derived`), done in the same call path so an old job cannot resurrect the session.
pub async fn exclude_session(
    index: &Index,
    session_id: &str,
    reason: &str,
    now_ms: i64,
) -> Result<bool> {
    let mut tx = index.writer.begin().await?;
    let exists: Option<i64> = sqlx::query_scalar("SELECT 1 FROM sessions WHERE id = ?")
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await?;
    if exists.is_none() {
        return Ok(false);
    }
    sqlx::query(
        "INSERT INTO exclusions(kind, target_id, reason, created_at) VALUES ('session', ?1, ?2, ?3) \
         ON CONFLICT(kind, target_id) DO UPDATE SET reason = excluded.reason",
    )
    .bind(session_id)
    .bind(reason)
    .bind(now_ms)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE sessions SET excluded = 1, exclusion_reason = ?2, task_id = NULL WHERE id = ?1",
    )
    .bind(session_id)
    .bind(reason)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn include_session(index: &Index, session_id: &str) -> Result<()> {
    let mut tx = index.writer.begin().await?;
    sqlx::query("DELETE FROM exclusions WHERE kind = 'session' AND target_id = ?")
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE sessions SET excluded = 0, exclusion_reason = NULL, index_state = 'pending', index_attempts = 0 WHERE id = ?").bind(session_id).execute(&mut *tx).await?;
    tx.commit().await
}

/// Delete derived data (active snapshot, chunks + FTS rows via trigger) of a session and bump the
/// generation so any in-flight job for the old generation cannot publish. Source files untouched.
pub async fn remove_derived(index: &Index, session_id: &str, now_ms: i64) -> Result<()> {
    let mut tx = index.writer.begin().await?;
    let snaps: Vec<String> = sqlx::query_scalar("SELECT id FROM snapshots WHERE session_id = ? AND kind = 'indexed' AND state IN ('published','staging')")
        .bind(session_id)
        .fetch_all(&mut *tx)
        .await?;
    for s in &snaps {
        crate::storage::index::retire_snapshot(&mut tx, s, now_ms).await?;
    }
    sqlx::query("UPDATE sessions SET active_snapshot_id = NULL, generation = generation + 1, coverage_status = 'unknown' WHERE id = ?").bind(session_id).execute(&mut *tx).await?;
    crate::storage::index::bump_epoch(&mut tx).await?;
    tx.commit().await
}

// ---------------------------------------------------------------------------------------------
// session reads
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SessionRow {
    pub id: String,
    pub source_id: String,
    pub native_id: String,
    pub session_kind: String,
    pub eligibility: String,
    pub index_state: String,
    pub index_error: Option<String>,
    pub excluded: bool,
    pub source_status: String,
    pub active_snapshot_id: Option<String>,
    pub generation: i64,
    pub index_attempts: i64,
    pub defer_until: Option<i64>,
    pub task_id: Option<String>,
    pub operation_id: Option<String>,
    pub last_activity_at: Option<i64>,
    pub coverage_status: String,
    pub path: Option<String>,
    pub len: Option<i64>,
    pub mtime_ms: Option<i64>,
    pub dev: Option<i64>,
    pub ino: Option<i64>,
}

impl SessionRow {
    /// `size:mtime_ms:dev:ino` of the catalogued file, if one is known.
    pub fn revision(&self) -> Option<String> {
        Some(revision(
            self.len? as u64,
            self.mtime_ms?,
            self.dev? as u64,
            self.ino? as u64,
        ))
    }
}

const SESSION_SELECT: &str = "SELECT s.id, s.source_id, s.native_id, s.session_kind, s.eligibility, s.index_state, s.index_error, s.excluded, s.source_status, \
    s.active_snapshot_id, s.generation, s.index_attempts, s.defer_until, s.task_id, s.operation_id, s.last_activity_at, s.coverage_status, \
    f.path AS path, f.size AS len, f.mtime_ms AS mtime_ms, f.dev AS dev, f.ino AS ino FROM sessions s LEFT JOIN source_files f ON f.session_id = s.id";

pub fn session_row(r: &sqlx::sqlite::SqliteRow) -> SessionRow {
    SessionRow {
        id: r.get("id"),
        source_id: r.get("source_id"),
        native_id: r.get("native_id"),
        session_kind: r.get("session_kind"),
        eligibility: r.get("eligibility"),
        index_state: r.get("index_state"),
        index_error: r.get("index_error"),
        excluded: r.get::<i64, _>("excluded") != 0,
        source_status: r.get("source_status"),
        active_snapshot_id: r.get("active_snapshot_id"),
        generation: r.get("generation"),
        index_attempts: r.get("index_attempts"),
        defer_until: r.get("defer_until"),
        task_id: r.get("task_id"),
        operation_id: r.get("operation_id"),
        last_activity_at: r.get("last_activity_at"),
        coverage_status: r.get("coverage_status"),
        path: r.get("path"),
        len: r.get("len"),
        mtime_ms: r.get("mtime_ms"),
        dev: r.get("dev"),
        ino: r.get("ino"),
    }
}

pub async fn session(index: &Index, id: &str) -> Result<Option<SessionRow>> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "{SESSION_SELECT} WHERE s.id = ?"
    )))
    .bind(id)
    .fetch_optional(&index.reader)
    .await?
    .map(|r| session_row(&r)))
}

/// Dev inspection list, keyset-paged by session id.
pub async fn sessions_page(
    index: &Index,
    source: Option<&str>,
    after: Option<&str>,
    limit: i64,
) -> Result<Vec<SessionRow>> {
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!("{SESSION_SELECT} WHERE (?1 IS NULL OR s.source_id = ?1) AND (?2 IS NULL OR s.id > ?2) ORDER BY s.id LIMIT ?3")))
        .bind(source)
        .bind(after)
        .bind(limit)
        .fetch_all(&index.reader)
        .await?
        .iter()
        .map(session_row)
        .collect())
}

/// Bookkeeping writers used by the job handlers. All guard on `excluded = 0` where they could
/// otherwise resurrect work.
pub async fn set_task(index: &Index, session_id: &str, task_id: Option<&str>) -> Result<()> {
    sqlx::query("UPDATE sessions SET task_id = ?2 WHERE id = ?1")
        .bind(session_id)
        .bind(task_id)
        .execute(&index.writer)
        .await?;
    Ok(())
}

/// Allocate the next idempotency sequence number for a session and return it.
pub async fn next_submit_seq(index: &Index, session_id: &str) -> Result<i64> {
    sqlx::query_scalar(
        "UPDATE sessions SET submit_seq = submit_seq + 1 WHERE id = ? RETURNING submit_seq",
    )
    .bind(session_id)
    .fetch_one(&index.writer)
    .await
}

pub async fn set_state(
    index: &Index,
    session_id: &str,
    state: &str,
    error: Option<&str>,
    defer_until: Option<i64>,
    attempts: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE sessions SET index_state = ?2, index_error = ?3, defer_until = ?4, index_attempts = COALESCE(?5, index_attempts) \
         WHERE id = ?1 AND excluded = 0",
    )
    .bind(session_id)
    .bind(state)
    .bind(error)
    .bind(defer_until)
    .bind(attempts)
    .execute(&index.writer)
    .await?;
    Ok(())
}

pub async fn app_meta_get(index: &Index, key: &str) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT value FROM app_meta WHERE key = ?")
        .bind(key)
        .fetch_optional(&index.reader)
        .await
}

pub async fn app_meta_set(index: &Index, key: &str, value: &str) -> Result<()> {
    sqlx::query("INSERT INTO app_meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value").bind(key).bind(value).execute(&index.writer).await?;
    Ok(())
}
