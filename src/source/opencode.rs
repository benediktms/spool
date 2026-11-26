//! OpenCode V2 SQLite discovery and bounded record reads.

use crate::adapters::opencode::{MessageProjection, ProjectedMessage, ProjectionError};
use crate::storage::catalog::SourceRow;
use crate::storage::index::Index;
use rustix::fs::{AtFlags, FileType, statat};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenCodeError {
    #[error("unsupported OpenCode storage schema")]
    Unsupported,
    #[error("OpenCode storage unavailable")]
    Unavailable(#[from] sqlx::Error),
    #[error("OpenCode storage is unsafe or changed")]
    UnsafeSource,
    #[error("OpenCode database identity changed")]
    Moved,
    #[error("OpenCode storage no longer matches the pinned observation")]
    Changed,
    #[error("OpenCode query exceeds the local limit")]
    Limit,
    #[error("OpenCode discovery was cancelled or timed out")]
    Cancelled,
}

impl From<ProjectionError> for OpenCodeError {
    fn from(error: ProjectionError) -> Self {
        match error {
            ProjectionError::Unsupported => Self::Unsupported,
            ProjectionError::Limit => Self::Limit,
        }
    }
}

pub(crate) struct ScanControl<'a> {
    pub(crate) cancel: &'a AtomicBool,
    pub(crate) deadline: Option<Instant>,
}

impl ScanControl<'_> {
    pub(crate) fn check(&self) -> Result<(), OpenCodeError> {
        if self.cancel.load(Ordering::Relaxed)
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(OpenCodeError::Cancelled);
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Session {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) directory: String,
    pub(crate) title: Option<String>,
    pub(crate) updated_at: i64,
    pub(crate) event_sequence: Option<i64>,
}

pub(crate) struct GuardedDb {
    conn: SqliteConnection,
    dir: File,
    configured_parent: PathBuf,
    parent_identity: (u64, u64),
    path: PathBuf,
    main_identity: (u64, u64),
    sidecars: [(PathBuf, Option<(u64, u64)>); 3],
}

pub(crate) struct NativeScanStage {
    pub(crate) id: String,
    source_id: String,
    identity: [i64; 4],
}

pub(crate) const SCAN_PAGE: usize = 500;

impl GuardedDb {
    fn identity(&self) -> Result<[i64; 4], OpenCodeError> {
        let values = [
            self.parent_identity.0,
            self.parent_identity.1,
            self.main_identity.0,
            self.main_identity.1,
        ];
        let mut result = [0; 4];
        for (out, value) in result.iter_mut().zip(values) {
            *out = i64::try_from(value).map_err(|_| OpenCodeError::UnsafeSource)?;
        }
        Ok(result)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<(), OpenCodeError> {
        let parent =
            std::fs::metadata(&self.configured_parent).map_err(|_| OpenCodeError::UnsafeSource)?;
        if (parent.dev(), parent.ino()) != self.parent_identity {
            return Err(OpenCodeError::Moved);
        }
        if !parent.is_dir()
            || parent.uid() != rustix::process::geteuid().as_raw()
            || parent.mode() & 0o022 != 0
        {
            return Err(OpenCodeError::UnsafeSource);
        }
        if checked_file(&self.dir, &self.path, false)? != Some(self.main_identity) {
            return Err(OpenCodeError::Moved);
        }
        for (path, prior) in &mut self.sidecars {
            let current = checked_file(&self.dir, path, true)?;
            if prior.is_some() && *prior != current {
                return Err(OpenCodeError::Changed);
            }
            *prior = current;
        }
        Ok(())
    }
}

impl NativeScanStage {
    pub(crate) async fn begin(
        index: &Index,
        db: &mut GuardedDb,
        source: &SourceRow,
        configured_path: &Path,
        after: Option<&str>,
    ) -> Result<Self, OpenCodeError> {
        let source_id = &source.id;
        if let Err(error) = db.checkpoint() {
            persist_movement(index, source_id, &error).await?;
            return Err(error);
        }
        let identity = db.identity()?;
        let id = crate::ids::new_id("nscan");
        let mut tx = index.writer.begin().await?;
        Self::check_configuration(&mut tx, source, configured_path).await?;
        sqlx::query(
            "INSERT INTO native_source_identities(source_id,parent_dev,parent_ino,main_dev,main_ino) \
             SELECT id,?2,?3,?4,?5 FROM sources WHERE id=?1 AND kind='opencode' AND enabled=1 \
             ON CONFLICT(source_id) DO NOTHING",
        ).bind(source_id).bind(identity[0]).bind(identity[1]).bind(identity[2]).bind(identity[3])
            .execute(&mut *tx).await?;
        let accepted: Option<(i64,i64,i64,i64,Option<String>)> = sqlx::query_as(
            "SELECT parent_dev,parent_ino,main_dev,main_ino,diagnostic FROM native_source_identities WHERE source_id=?",
        ).bind(source_id).fetch_optional(&mut *tx).await?;
        let Some((pdev, pino, mdev, mino, diagnostic)) = accepted else {
            return Err(OpenCodeError::UnsafeSource);
        };
        if [pdev, pino, mdev, mino] != identity || diagnostic.is_some() {
            sqlx::query(
                "UPDATE native_source_identities SET diagnostic='SOURCE_MOVED' WHERE source_id=?",
            )
            .bind(source_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Err(OpenCodeError::Moved);
        }
        let result = sqlx::query(
            "INSERT INTO native_scan_runs(id,source_id,parent_dev,parent_ino,main_dev,main_ino,cursor) \
             SELECT ?1,id,?3,?4,?5,?6,?7 FROM sources WHERE id=?2 AND kind='opencode' AND enabled=1",
        )
        .bind(&id)
        .bind(source_id)
        .bind(identity[0])
        .bind(identity[1])
        .bind(identity[2])
        .bind(identity[3])
        .bind(after)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(OpenCodeError::UnsafeSource);
        }
        if let Err(error) = db.checkpoint() {
            tx.rollback().await?;
            persist_movement(index, source_id, &error).await?;
            return Err(error);
        }
        tx.commit().await?;
        Ok(Self {
            id,
            source_id: source_id.clone(),
            identity,
        })
    }

    pub(crate) async fn check_configuration(
        tx: &mut SqliteConnection,
        source: &SourceRow,
        path: &Path,
    ) -> Result<(), OpenCodeError> {
        let configured: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE id=? AND kind='opencode' AND enabled=1 \
             AND config_revision=? AND json_array_length(roots_json)=1 \
             AND json_extract(roots_json,'$[0]')=?)",
        )
        .bind(&source.id)
        .bind(source.config_revision)
        .bind(path.to_string_lossy().as_ref())
        .fetch_one(tx)
        .await?;
        if !configured {
            return Err(OpenCodeError::UnsafeSource);
        }
        Ok(())
    }

    pub(crate) fn verify(&self, db: &mut GuardedDb) -> Result<(), OpenCodeError> {
        db.checkpoint()?;
        if db.identity()? != self.identity {
            return Err(OpenCodeError::Moved);
        }
        Ok(())
    }

    pub(crate) async fn record_movement(
        &self,
        index: &Index,
        error: &OpenCodeError,
    ) -> Result<(), OpenCodeError> {
        persist_movement(index, &self.source_id, error).await
    }

    pub(crate) async fn next_page(
        &self,
        index: &Index,
        db: &mut GuardedDb,
        limit: usize,
    ) -> Result<(usize, bool), OpenCodeError> {
        if let Err(error) = self.verify(db) {
            self.record_movement(index, &error).await?;
            return Err(error);
        }
        let (cursor, exhausted): (Option<String>, i64) = sqlx::query_as(
            "SELECT cursor,exhausted FROM native_scan_runs WHERE id=? AND source_id=? AND parent_dev=? AND parent_ino=? AND main_dev=? AND main_ino=?",
        ).bind(&self.id).bind(&self.source_id).bind(self.identity[0]).bind(self.identity[1]).bind(self.identity[2]).bind(self.identity[3])
            .fetch_one(&index.reader).await?;
        if exhausted != 0 {
            return Ok((0, true));
        }
        let limit = limit.clamp(1, SCAN_PAGE);
        let page = match sessions(db, cursor.as_deref(), limit)
            .await
            .map_err(classify_lookup_error)
        {
            Ok(page) => page,
            Err(error) => {
                self.record_movement(index, &error).await?;
                return Err(error);
            }
        };
        if page
            .iter()
            .any(|item| item.event_sequence.is_none_or(|seq| seq < 0))
            || page.windows(2).any(|pair| pair[0].id == pair[1].id)
        {
            return Err(OpenCodeError::Unsupported);
        }
        let exhausted = page.len() < limit;
        let mut tx = index.writer.begin().await?;
        for chunk in page.chunks(500 / 7) {
            let mut insert = sqlx::QueryBuilder::new(
                "INSERT INTO native_scan_entries(scan_id,native_id,parent_id,directory,title,updated_at,event_sequence) ",
            );
            insert.push_values(chunk, |mut b, item| {
                b.push_bind(&self.id)
                    .push_bind(&item.id)
                    .push_bind(&item.parent_id)
                    .push_bind(&item.directory)
                    .push_bind(&item.title)
                    .push_bind(item.updated_at)
                    .push_bind(item.event_sequence);
            });
            insert.build().execute(&mut *tx).await?;
        }
        let next = page
            .last()
            .map(|item| item.id.as_str())
            .or(cursor.as_deref());
        sqlx::query("UPDATE native_scan_runs SET cursor=?,exhausted=? WHERE id=?")
            .bind(next)
            .bind(i64::from(exhausted))
            .bind(&self.id)
            .execute(&mut *tx)
            .await?;
        if let Err(error) = self.verify(db) {
            tx.rollback().await?;
            self.record_movement(index, &error).await?;
            return Err(error);
        }
        tx.commit().await?;
        Ok((page.len(), exhausted))
    }

    pub(crate) async fn finish(&self, tx: &mut SqliteConnection) -> Result<(), OpenCodeError> {
        let deleted = sqlx::query("DELETE FROM native_scan_runs WHERE id=?")
            .bind(&self.id)
            .execute(tx)
            .await?;
        if deleted.rows_affected() != 1 {
            return Err(OpenCodeError::UnsafeSource);
        }
        Ok(())
    }

    pub(crate) async fn abort(self, index: &Index) -> Result<(), OpenCodeError> {
        sqlx::query("DELETE FROM native_scan_runs WHERE id=?")
            .bind(&self.id)
            .execute(&index.writer)
            .await?;
        Ok(())
    }
}

pub(crate) async fn open_for_discovery(
    index: &Index,
    source_id: &str,
    path: &Path,
) -> Result<GuardedDb, OpenCodeError> {
    match open(path).await.map_err(classify_lookup_error) {
        Ok(db) => Ok(db),
        Err(error) => {
            let error = if matches!(error, OpenCodeError::Moved)
                || accepted_identity_changed(index, source_id, path).await?
            {
                OpenCodeError::Moved
            } else {
                error
            };
            persist_movement(index, source_id, &error).await?;
            Err(error)
        }
    }
}

async fn accepted_identity_changed(
    index: &Index,
    source_id: &str,
    path: &Path,
) -> Result<bool, OpenCodeError> {
    let accepted: Option<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT parent_dev,parent_ino,main_dev,main_ino FROM native_source_identities WHERE source_id=?",
    )
    .bind(source_id)
    .fetch_optional(&index.reader)
    .await?;
    let Some(accepted) = accepted else {
        return Ok(false);
    };
    let Some(parent) = path.parent() else {
        return Ok(false);
    };
    let Ok(dir) = File::open(parent) else {
        return Ok(false);
    };
    let Ok(info) = dir.metadata() else {
        return Ok(false);
    };
    if !info.is_dir()
        || info.uid() != rustix::process::geteuid().as_raw()
        || info.mode() & 0o022 != 0
    {
        return Ok(false);
    }
    let Ok(Some(main)) = checked_file(&dir, path, false) else {
        return Ok(false);
    };
    let identity = [info.dev(), info.ino(), main.0, main.1];
    let expected = [accepted.0, accepted.1, accepted.2, accepted.3];
    Ok(identity
        .iter()
        .zip(expected)
        .any(|(actual, expected)| i64::try_from(*actual).ok() != Some(expected)))
}

pub(crate) async fn persist_movement(
    index: &Index,
    source_id: &str,
    error: &OpenCodeError,
) -> Result<(), OpenCodeError> {
    if matches!(error, OpenCodeError::Moved) {
        sqlx::query(
            "UPDATE native_source_identities SET diagnostic='SOURCE_MOVED' WHERE source_id=?",
        )
        .bind(source_id)
        .execute(&index.writer)
        .await?;
    }
    Ok(())
}

fn checked_file(
    dir: &File,
    path: &Path,
    optional: bool,
) -> Result<Option<(u64, u64)>, OpenCodeError> {
    let name = path.file_name().ok_or(OpenCodeError::UnsafeSource)?;
    match statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat)
            if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile
                && stat.st_uid == rustix::process::geteuid().as_raw() =>
        {
            let dev =
                u64::try_from(i128::from(stat.st_dev)).map_err(|_| OpenCodeError::UnsafeSource)?;
            let ino =
                u64::try_from(i128::from(stat.st_ino)).map_err(|_| OpenCodeError::UnsafeSource)?;
            Ok(Some((dev, ino)))
        }
        Err(error) if optional && error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        _ => Err(OpenCodeError::UnsafeSource),
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

pub(crate) async fn open(path: &Path) -> Result<GuardedDb, OpenCodeError> {
    let parent = path.parent().ok_or(OpenCodeError::UnsafeSource)?;
    let filename = path.file_name().ok_or(OpenCodeError::UnsafeSource)?;
    let configured_parent = parent.to_path_buf();
    let parent = std::fs::canonicalize(parent).map_err(|_| OpenCodeError::UnsafeSource)?;
    let path = parent.join(filename);
    let dir = File::open(&parent).map_err(|_| OpenCodeError::UnsafeSource)?;
    let parent_info = dir.metadata().map_err(|_| OpenCodeError::UnsafeSource)?;
    if !parent_info.is_dir()
        || parent_info.uid() != rustix::process::geteuid().as_raw()
        || parent_info.mode() & 0o022 != 0
    {
        return Err(OpenCodeError::UnsafeSource);
    }
    let main = checked_file(&dir, &path, false)?.ok_or(OpenCodeError::UnsafeSource)?;
    let sidecars = [
        sidecar(&path, "-wal"),
        sidecar(&path, "-shm"),
        sidecar(&path, "-journal"),
    ];
    let before: Vec<_> = sidecars
        .iter()
        .map(|sidecar| checked_file(&dir, sidecar, true))
        .collect::<Result<_, _>>()?;
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(1))
        .pragma("query_only", "ON");
    let mut conn = SqliteConnection::connect_with(&options).await?;
    let current_parent =
        std::fs::metadata(&configured_parent).map_err(|_| OpenCodeError::UnsafeSource)?;
    let current = checked_file(&dir, &path, false)?.ok_or(OpenCodeError::UnsafeSource)?;
    if main != current
        || current_parent.dev() != parent_info.dev()
        || current_parent.ino() != parent_info.ino()
    {
        return Err(OpenCodeError::Moved);
    }
    for (path, prior) in sidecars.iter().zip(&before) {
        let after = checked_file(&dir, path, true)?;
        if let Some(prior) = prior
            && after.as_ref() != Some(prior)
        {
            return Err(OpenCodeError::Changed);
        }
    }
    for (table, columns) in [
        (
            "session_v2",
            &["id", "parent_id", "directory", "title", "time_updated"][..],
        ),
        (
            "session_message",
            &["id", "session_id", "type", "seq", "data"][..],
        ),
        (
            "session_part",
            &["message_id", "session_id", "ordinal", "type", "data"][..],
        ),
        ("event_sequence", &["aggregate_id", "seq"][..]),
    ] {
        let found: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(table)
            .fetch_all(&mut conn)
            .await?;
        if columns
            .iter()
            .any(|column| !found.iter().any(|name| name == column))
        {
            return Err(OpenCodeError::Unsupported);
        }
    }
    let mut guarded = GuardedDb {
        conn,
        dir,
        configured_parent,
        parent_identity: (parent_info.dev(), parent_info.ino()),
        path,
        main_identity: main,
        sidecars: std::array::from_fn(|i| (sidecars[i].clone(), before[i])),
    };
    guarded.checkpoint()?;
    Ok(guarded)
}

pub(crate) async fn sessions(
    db: &mut GuardedDb,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<Session>, OpenCodeError> {
    db.checkpoint()?;
    let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded page size");
    let rows = sqlx::query(
        "SELECT s.id,s.parent_id,s.directory,s.title,s.time_updated, \
         (SELECT CASE WHEN COUNT(*) = 1 THEN MIN(e.seq) END \
          FROM event_sequence e WHERE e.aggregate_id=s.id) AS seq FROM session_v2 s \
         WHERE s.id >= COALESCE(?1, '') AND (?1 IS NULL OR s.id <> ?1) \
         ORDER BY s.id LIMIT ?2",
    )
    .bind(after)
    .bind(limit)
    .fetch_all(&mut db.conn)
    .await?;
    let sessions = rows
        .iter()
        .map(|row| {
            Ok(Session {
                id: row.try_get("id")?,
                parent_id: row.try_get("parent_id")?,
                directory: row.try_get("directory")?,
                title: row.try_get("title")?,
                updated_at: row.try_get("time_updated")?,
                event_sequence: row.try_get("seq")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    if sessions.iter().any(|session| session.id.is_empty()) {
        return Err(OpenCodeError::Unsupported);
    }
    db.checkpoint()?;
    Ok(sessions)
}

pub(crate) async fn missing_sessions(
    db: &mut GuardedDb,
    candidates: &[String],
) -> Result<Vec<String>, OpenCodeError> {
    if candidates.len() > 500 {
        return Err(OpenCodeError::Limit);
    }
    db.checkpoint()?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut query =
        sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT id FROM session_v2 WHERE id IN (");
    let mut separated = query.separated(",");
    for id in candidates {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    let present: std::collections::HashSet<String> = query
        .build_query_scalar()
        .fetch_all(&mut db.conn)
        .await?
        .into_iter()
        .collect();
    let missing = candidates
        .iter()
        .filter(|id| !present.contains(*id))
        .cloned()
        .collect();
    db.checkpoint()?;
    Ok(missing)
}

pub(crate) async fn project_session(
    db: &mut GuardedDb,
    session_id: &str,
    max_bytes: usize,
    control: &ScanControl<'_>,
) -> Result<Vec<ProjectedMessage>, OpenCodeError> {
    project_filtered(db, session_id, None, max_bytes, control).await
}

async fn project_filtered(
    db: &mut GuardedDb,
    session_id: &str,
    message_id: Option<&str>,
    max_bytes: usize,
    control: &ScanControl<'_>,
) -> Result<Vec<ProjectedMessage>, OpenCodeError> {
    control.check()?;
    db.checkpoint()?;
    let mut tx = db.conn.begin().await?;
    let mut used = 0usize;
    let mut output = Vec::new();
    let mut after: Option<i64> = None;
    loop {
        control.check()?;
        let message_start = used;
        let remaining = i64::try_from(max_bytes - used).map_err(|_| OpenCodeError::Limit)?;
        let row = sqlx::query(
            "SELECT CASE WHEN length(CAST(id AS BLOB))+length(CAST(type AS BLOB))+length(CAST(data AS BLOB))<=?3 THEN id END AS id, \
             CASE WHEN length(CAST(id AS BLOB))+length(CAST(type AS BLOB))+length(CAST(data AS BLOB))<=?3 THEN type END AS type, \
             seq, CASE WHEN length(CAST(id AS BLOB))+length(CAST(type AS BLOB))+length(CAST(data AS BLOB))<=?3 THEN data END AS data \
             FROM session_message WHERE session_id=?1 AND (?2 IS NULL OR seq>?2) \
             AND (?4 IS NULL OR (id=?4 AND EXISTS (SELECT 1 FROM session_v2 WHERE id=?1))) ORDER BY seq LIMIT 1",
        ).bind(session_id).bind(after).bind(remaining).bind(message_id).fetch_optional(&mut *tx).await?;
        let Some(row) = row else { break };
        let id: Option<String> = row.try_get("id")?;
        let role: Option<String> = row.try_get("type")?;
        let raw: Option<String> = row.try_get("data")?;
        let (id, role, raw) = match (id, role, raw) {
            (Some(id), Some(role), Some(raw)) => (id, role, raw),
            _ => return Err(OpenCodeError::Limit),
        };
        let seq: i64 = row.try_get("seq")?;
        after = Some(seq);
        used += id.len() + role.len() + raw.len();
        let mut message = MessageProjection::new(id, seq, role, &raw)?;
        let mut ordinal: Option<i64> = None;
        loop {
            control.check()?;
            let remaining = i64::try_from(max_bytes - used).map_err(|_| OpenCodeError::Limit)?;
            let part = sqlx::query(
                "SELECT ordinal,CASE WHEN length(CAST(type AS BLOB))+length(CAST(data AS BLOB))<=?4 THEN type END AS type, \
                 CASE WHEN length(CAST(type AS BLOB))+length(CAST(data AS BLOB))<=?4 THEN data END AS data \
                 FROM session_part WHERE session_id=?1 AND message_id=?2 AND (?3 IS NULL OR ordinal>?3) ORDER BY ordinal LIMIT 1",
            ).bind(session_id).bind(message.id()).bind(ordinal).bind(remaining).fetch_optional(&mut *tx).await?;
            let Some(part) = part else { break };
            let kind: Option<String> = part.try_get("type")?;
            let data: Option<String> = part.try_get("data")?;
            let (kind, data) = match (kind, data) {
                (Some(kind), Some(data)) => (kind, data),
                _ => return Err(OpenCodeError::Limit),
            };
            used += kind.len() + data.len();
            let n: i64 = part.try_get("ordinal")?;
            ordinal = Some(n);
            message.push_part(n, kind, &data)?;
        }
        output.extend(message.finish(session_id, used - message_start)?);
    }
    control.check()?;
    tx.commit().await?;
    db.checkpoint()?;
    Ok(output)
}

pub(crate) async fn open_session(
    index: &Index,
    source_id: &str,
    path: &Path,
    session_id: &str,
    control: &ScanControl<'_>,
) -> Result<GuardedDb, OpenCodeError> {
    control.check()?;
    let configured: Option<(i64, i64, i64, i64, Option<String>)> = sqlx::query_as(
        "SELECT i.parent_dev,i.parent_ino,i.main_dev,i.main_ino,i.diagnostic \
         FROM native_source_identities i JOIN sources s ON s.id=i.source_id \
         WHERE s.id=? AND s.kind='opencode' AND s.enabled=1 \
         AND EXISTS (SELECT 1 FROM json_each(s.roots_json) WHERE value=?) \
         AND EXISTS (SELECT 1 FROM sessions WHERE source_id=s.id AND native_id=?)",
    )
    .bind(source_id)
    .bind(path.to_string_lossy().as_ref())
    .bind(session_id)
    .fetch_optional(&index.reader)
    .await?;
    let Some((pdev, pino, mdev, mino, diagnostic)) = configured else {
        return Err(OpenCodeError::UnsafeSource);
    };
    if diagnostic.is_some() {
        return Err(OpenCodeError::Moved);
    }
    let db = match open(path).await {
        Ok(db) => db,
        Err(error) => {
            let error = classify_lookup_error(error);
            let error = if matches!(error, OpenCodeError::Moved)
                || accepted_identity_changed(index, source_id, path).await?
            {
                OpenCodeError::Moved
            } else {
                error
            };
            note_read_movement(index, source_id, &error).await;
            return Err(error);
        }
    };
    if db.identity()? != [pdev, pino, mdev, mino] {
        note_read_movement(index, source_id, &OpenCodeError::Moved).await;
        return Err(OpenCodeError::Moved);
    }
    control.check()?;
    Ok(db)
}

pub(crate) async fn observe_session(
    index: &Index,
    source_id: &str,
    db: &mut GuardedDb,
    native_id: &str,
    control: &ScanControl<'_>,
) -> Result<Option<(Session, i64)>, OpenCodeError> {
    let result = async {
        control.check()?;
        db.checkpoint()?;
        let row = sqlx::query(
            "SELECT s.id,s.parent_id,s.directory,s.title,s.time_updated,e.seq \
             FROM session_v2 s LEFT JOIN event_sequence e ON e.aggregate_id=s.id WHERE s.id=?",
        )
        .bind(native_id)
        .fetch_optional(&mut db.conn)
        .await?;
        db.checkpoint()?;
        control.check()?;
        row.map(|row| {
            let seq: Option<i64> = row.try_get("seq")?;
            let seq = seq
                .filter(|seq| *seq >= 0)
                .ok_or(OpenCodeError::Unsupported)?;
            Ok((
                Session {
                    id: row.try_get("id")?,
                    parent_id: row.try_get("parent_id")?,
                    directory: row.try_get("directory")?,
                    title: row.try_get("title")?,
                    updated_at: row.try_get("time_updated")?,
                    event_sequence: Some(seq),
                },
                seq,
            ))
        })
        .transpose()
    }
    .await
    .map_err(|error| checked_lookup_error(db, error));
    if let Err(error) = &result {
        note_read_movement(index, source_id, error).await;
    }
    result
}

pub(crate) async fn lookup_message(
    index: &Index,
    source_id: &str,
    path: &Path,
    session_id: &str,
    message_id: &str,
    max_bytes: usize,
    control: &ScanControl<'_>,
) -> Result<Option<ProjectedMessage>, OpenCodeError> {
    let mut db = open_session(index, source_id, path, session_id, control).await?;
    let result = project_filtered(&mut db, session_id, Some(message_id), max_bytes, control)
        .await
        .map_err(|error| checked_lookup_error(&mut db, error));
    if let Err(error) = &result {
        note_read_movement(index, source_id, error).await;
    }
    Ok(result?.into_iter().next())
}

async fn note_read_movement(index: &Index, source_id: &str, error: &OpenCodeError) {
    if persist_movement(index, source_id, error).await.is_err() {
        tracing::warn!("could not persist detected OpenCode source movement");
    }
}

pub(crate) fn classify_lookup_error(error: OpenCodeError) -> OpenCodeError {
    const SQLITE_READONLY_DBMOVED: &str = "1032";
    if let OpenCodeError::Unavailable(sqlx::Error::Database(error)) = &error
        && error.code().as_deref() == Some(SQLITE_READONLY_DBMOVED)
    {
        return OpenCodeError::Moved;
    }
    error
}

fn checked_lookup_error(db: &mut GuardedDb, error: OpenCodeError) -> OpenCodeError {
    let error = classify_lookup_error(error);
    match db.checkpoint() {
        Err(OpenCodeError::Moved) => OpenCodeError::Moved,
        Err(OpenCodeError::Changed) if !matches!(error, OpenCodeError::Moved) => {
            OpenCodeError::Changed
        }
        _ => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[tokio::test]
    async fn opencode_projects_inline_and_split_text_in_sequence() {
        let cancel = AtomicBool::new(false);
        let control = ScanControl {
            cancel: &cancel,
            deadline: None,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("projection.db");
        let mut writer = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
        writer.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT,title TEXT,time_updated INTEGER); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            INSERT INTO session_v2 VALUES ('s',NULL,'/synthetic',NULL,1); \
            INSERT INTO session_message VALUES \
            ('u','s','user',1,'{\"text\":\"findable user\"}'), \
            ('inline','s','assistant',2,'{\"content\":[{\"type\":\"reasoning\",\"text\":\"secret\"},{\"type\":\"text\",\"text\":\"inline answer\"},{\"type\":\"tool\",\"text\":\"hidden\"}]}'), \
            ('split','s','assistant',3,'{\"content\":[]}'), \
            ('synthetic','s','synthetic',4,'{\"text\":\"hidden input\"}'); \
            INSERT INTO session_part VALUES \
            ('split','s',2,'text','{\"type\":\"text\",\"text\":\"second\"}'), \
            ('split','s',1,'text','{\"type\":\"text\",\"text\":\"first\"}'), \
            ('split','s',0,'reasoning','{\"type\":\"reasoning\",\"text\":\"hidden\"}');").await.unwrap();
        let mut db = open(&path).await.unwrap();
        let projected = project_session(&mut db, "s", 4096, &control).await.unwrap();
        assert_eq!(
            projected
                .iter()
                .flat_map(|m| m.blocks.iter().map(|b| (b.index, b.text.as_str())))
                .collect::<Vec<_>>(),
            [
                (0, "findable user"),
                (1, "inline answer"),
                (1, "first"),
                (2, "second")
            ]
        );
        assert!(matches!(
            project_session(&mut db, "s", 5, &control).await,
            Err(OpenCodeError::Limit)
        ));
        let before = projected[2].digest;
        writer.execute("UPDATE session_part SET data='{\"type\":\"text\",\"text\":\"changed\"}' WHERE message_id='split' AND ordinal=2").await.unwrap();
        let changed = project_session(&mut db, "s", 4096, &control).await.unwrap();
        assert_ne!(before, changed[2].digest);
        assert_eq!(projected[0].digest, changed[0].digest);
        let index = Index::open_for_test(dir.path()).await.unwrap();
        sqlx::query("INSERT INTO sources(id,kind,roots_json,enabled) VALUES ('oc','opencode',?,1)")
            .bind(serde_json::to_string(&vec![path.to_string_lossy().to_string()]).unwrap())
            .execute(&index.writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO sessions(id,source_id,native_id,discriminator,session_kind,eligibility,index_state,discovered_at) VALUES ('s','oc','s','','interactive','active','pending',1)")
            .execute(&index.writer).await.unwrap();
        let source = crate::storage::catalog::source(&index, "oc")
            .await
            .unwrap()
            .unwrap();
        let stage = NativeScanStage::begin(&index, &mut db, &source, &path, None)
            .await
            .unwrap();
        stage.abort(&index).await.unwrap();
        let found = lookup_message(&index, "oc", &path, "s", "split", 4096, &control)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.digest, changed[2].digest);
        assert!(found.verify(&before).is_none());
        let found = lookup_message(&index, "oc", &path, "s", "split", 4096, &control)
            .await
            .unwrap();
        let resolved = found.unwrap().verify(&changed[2].digest).unwrap();
        assert_eq!(
            resolved
                .blocks
                .iter()
                .map(|block| (block.index, block.bytes.as_slice()))
                .collect::<Vec<_>>(),
            [(1, b"first".as_slice()), (2, b"changed".as_slice())]
        );
        let absent = lookup_message(&index, "oc", &path, "s", "absent", 4096, &control)
            .await
            .unwrap();
        assert!(absent.is_none());
        assert!(matches!(
            lookup_message(&index, "oc", &path, "wrong", "split", 4096, &control).await,
            Err(OpenCodeError::UnsafeSource)
        ));
        writer.execute("WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<601) \
            INSERT INTO session_message SELECT printf('bulk%04d',n),'bulk','user',n,'{\"text\":\"x\"}' FROM nums; \
            INSERT INTO session_message VALUES ('many','parts','assistant',1,'{\"content\":[]}'); \
            WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<601) \
            INSERT INTO session_part SELECT 'many','parts',n,'text','{\"type\":\"text\",\"text\":\"x\"}' FROM nums;").await.unwrap();
        assert_eq!(
            project_session(&mut db, "bulk", 100_000, &control)
                .await
                .unwrap()
                .len(),
            601
        );
        assert_eq!(
            project_session(&mut db, "parts", 100_000, &control)
                .await
                .unwrap()[0]
                .blocks
                .len(),
            601
        );
        assert!(matches!(
            project_session(&mut db, "bulk", 100, &control).await,
            Err(OpenCodeError::Limit)
        ));
        cancel.store(true, Ordering::Relaxed);
        assert!(matches!(
            project_session(&mut db, "bulk", 100_000, &control).await,
            Err(OpenCodeError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn opencode_read_only_schema_gate_and_keyset_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("synthetic.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
        writer.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            INSERT INTO session_v2 VALUES ('a',NULL,'/synthetic','A',1),('b','a','/synthetic','B',2),('c',NULL,'/synthetic','C',3);")
            .await.unwrap();
        let mut reader = open(&path).await.unwrap();
        let first = sessions(&mut reader, None, 2).await.unwrap();
        assert_eq!(
            first.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        let second = sessions(&mut reader, Some("b"), 2).await.unwrap();
        assert_eq!(second[0].id, "c");
        assert!(reader.conn.execute("DELETE FROM session_v2").await.is_err());
        let linked = dir.path().join("link.db");
        symlink(&path, &linked).unwrap();
        assert!(matches!(
            open(&linked).await,
            Err(OpenCodeError::UnsafeSource)
        ));
        let wal = sidecar(&path, "-wal");
        symlink(&path, &wal).unwrap();
        assert!(matches!(
            open(&path).await,
            Err(OpenCodeError::UnsafeSource)
        ));
        std::fs::remove_file(&wal).unwrap();
        let journal = sidecar(&path, "-journal");
        symlink(&path, &journal).unwrap();
        assert!(matches!(
            open(&path).await,
            Err(OpenCodeError::UnsafeSource)
        ));
        std::fs::remove_file(&journal).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(matches!(
            open(&path).await,
            Err(OpenCodeError::UnsafeSource)
        ));
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        writer.execute("DROP TABLE session_part").await.unwrap();
        assert!(matches!(open(&path).await, Err(OpenCodeError::Unsupported)));
        assert!(open(&dir.path().join("missing.db")).await.is_err());
        assert!(!dir.path().join("missing.db").exists());
    }

    #[tokio::test]
    async fn opencode_reads_committed_wal_while_writer_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
        writer.execute("PRAGMA journal_mode=WAL; \
            CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            INSERT INTO session_v2 VALUES ('wal-session',NULL,'/synthetic',NULL,1);")
            .await.unwrap();
        assert!(sidecar(&path, "-wal").exists());
        let mut reader = open(&path).await.unwrap();
        let page = sessions(&mut reader, None, 5).await.unwrap();
        assert_eq!(page[0].id, "wal-session");
        let candidates = vec!["wal-session".to_string(), "absent".to_string()];
        assert_eq!(
            missing_sessions(&mut reader, &candidates).await.unwrap(),
            ["absent"]
        );
        writer
            .execute("DELETE FROM session_v2 WHERE id='wal-session'")
            .await
            .unwrap();
        assert_eq!(
            missing_sessions(&mut reader, &candidates).await.unwrap(),
            candidates
        );
        let shm = sidecar(&path, "-shm");
        std::fs::rename(&shm, dir.path().join("old.shm")).unwrap();
        assert!(matches!(reader.checkpoint(), Err(OpenCodeError::Changed)));
        std::fs::write(&shm, b"replacement").unwrap();
        assert!(matches!(reader.checkpoint(), Err(OpenCodeError::Changed)));
        std::fs::rename(&path, dir.path().join("old.db")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(matches!(
            missing_sessions(&mut reader, &candidates).await,
            Err(OpenCodeError::Moved)
        ));
    }

    #[tokio::test]
    async fn opencode_pages_more_than_five_hundred_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pages.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
        writer.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 601) \
            INSERT INTO session_v2 SELECT printf('%04d',n),NULL,'/synthetic',NULL,n FROM nums;").await.unwrap();
        let mut reader = open(&path).await.unwrap();
        let first = sessions(&mut reader, None, 500).await.unwrap();
        assert_eq!(first.len(), 500);
        let next = sessions(&mut reader, Some(&first[499].id), 500)
            .await
            .unwrap();
        assert_eq!(next.len(), 101);
        assert_eq!(next[0].id, "0501");
        assert_eq!(next[100].id, "0601");
        std::fs::rename(&path, dir.path().join("moved.db")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(matches!(
            sessions(&mut reader, Some(&first[499].id), 500).await,
            Err(OpenCodeError::Moved)
        ));
    }

    #[tokio::test]
    async fn opencode_stages_multiple_pages_and_aborts_after_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staging-source.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
        writer.execute("CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER); \
            WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 601) \
            INSERT INTO session_v2 SELECT printf('%04d',n),NULL,'/synthetic',NULL,n FROM nums; \
            INSERT INTO event_sequence SELECT id,0 FROM session_v2;").await.unwrap();
        let index = Index::open_for_test(dir.path()).await.unwrap();
        let source = set_up_scan_source(&index, "oc", &path).await;
        let mut db = open(&path).await.unwrap();
        let scan = NativeScanStage::begin(&index, &mut db, &source, &path, None)
            .await
            .unwrap();
        assert_eq!(
            scan.next_page(&index, &mut db, 500).await.unwrap(),
            (500, false)
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sessions")
            .fetch_one(&index.reader)
            .await
            .unwrap();
        assert_eq!(count, 0);
        std::fs::rename(&path, dir.path().join("moved-source.db")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(matches!(
            scan.next_page(&index, &mut db, 500).await,
            Err(OpenCodeError::Moved)
        ));
        scan.abort(&index).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sessions")
            .fetch_one(&index.reader)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM native_scan_entries")
            .fetch_one(&index.reader)
            .await
            .unwrap();
        assert_eq!(count, 0);
        std::fs::rename(dir.path().join("moved-source.db"), &path).unwrap();
        let mut db = open(&path).await.unwrap();
        assert!(matches!(
            NativeScanStage::begin(&index, &mut db, &source, &path, None).await,
            Err(OpenCodeError::Moved)
        ));
        let diagnostic: String = sqlx::query_scalar(
            "SELECT diagnostic FROM native_source_identities WHERE source_id='oc'",
        )
        .fetch_one(&index.reader)
        .await
        .unwrap();
        assert_eq!(diagnostic, "SOURCE_MOVED");
        let source = set_up_scan_source(&index, "oc2", &path).await;
        let scan = NativeScanStage::begin(&index, &mut db, &source, &path, None)
            .await
            .unwrap();
        assert_eq!(
            scan.next_page(&index, &mut db, 500).await.unwrap(),
            (500, false)
        );
        assert_eq!(
            scan.next_page(&index, &mut db, 500).await.unwrap(),
            (101, true)
        );
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM sessions),(SELECT count(*) FROM native_scan_entries)",
        )
        .fetch_one(&index.reader)
        .await
        .unwrap();
        assert_eq!(counts, (0, 601));
        scan.abort(&index).await.unwrap();
    }

    #[tokio::test]
    async fn opencode_replacement_between_scans_keeps_identity_and_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.db");
        let schema = "CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL); \
            CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT); \
            CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT); \
            CREATE TABLE event_sequence(aggregate_id TEXT,seq INTEGER);";
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
        writer.execute(schema).await.unwrap();
        let index = Index::open_for_test(dir.path()).await.unwrap();
        let source = set_up_scan_source(&index, "oc", &path).await;
        let mut db = open(&path).await.unwrap();
        let scan = NativeScanStage::begin(&index, &mut db, &source, &path, None)
            .await
            .unwrap();
        scan.abort(&index).await.unwrap();
        let before: (i64, i64) = sqlx::query_as(
            "SELECT main_dev,main_ino FROM native_source_identities WHERE source_id='oc'",
        )
        .fetch_one(&index.reader)
        .await
        .unwrap();
        std::fs::rename(&path, dir.path().join("original.db")).unwrap();
        let mut replacement = SqliteConnection::connect_with(&options).await.unwrap();
        replacement.execute(schema).await.unwrap();
        let mut changed = open(&path).await.unwrap();
        assert!(matches!(
            NativeScanStage::begin(&index, &mut changed, &source, &path, None).await,
            Err(OpenCodeError::Moved)
        ));
        let after: (i64, i64, String) = sqlx::query_as("SELECT main_dev,main_ino,diagnostic FROM native_source_identities WHERE source_id='oc'")
            .fetch_one(&index.reader).await.unwrap();
        assert_eq!((after.0, after.1), before);
        assert_eq!(after.2, "SOURCE_MOVED");
        index.reader.close().await;
        index.writer.close().await;
        let reopened = Index::open_for_test(dir.path()).await.unwrap();
        let stored: String = sqlx::query_scalar(
            "SELECT diagnostic FROM native_source_identities WHERE source_id='oc'",
        )
        .fetch_one(&reopened.reader)
        .await
        .unwrap();
        assert_eq!(stored, "SOURCE_MOVED");
    }

    async fn set_up_scan_source(index: &Index, id: &str, path: &Path) -> SourceRow {
        sqlx::query("INSERT INTO sources(id,kind,roots_json,enabled) VALUES (?,'opencode',?,1)")
            .bind(id)
            .bind(serde_json::json!([path]).to_string())
            .execute(&index.writer)
            .await
            .unwrap();
        crate::storage::catalog::source(index, id)
            .await
            .unwrap()
            .unwrap()
    }
}
