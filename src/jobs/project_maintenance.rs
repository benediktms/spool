use crate::discovery::DiscoveryService;
use crate::domain::HarnessKind;
use crate::storage::catalog;
use crate::storage::index::Index;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};

/// Distinct paths one maintenance run resolves or probes.
pub const PROJECT_PATHS_PER_RUN: usize = 500;

/// Keep the project model current: assign projects to sessions that have none, move sessions of
/// `path` projects to the git project that ingestion has since attached to their cwd (SQL only,
/// no git discovery here), and mark vanished paths dead with a plain existence check. Idempotent.
/// A stored remote wins over the working directory; Codex sessions without one are re-probed from
/// their source header first.
pub async fn correlate_projects(
    idx: &Index,
    discovery: &DiscoveryService,
    now: i64,
    limit: usize,
    stop: &AtomicBool,
) -> Result<Value, sqlx::Error> {
    let mut report = json!({
        "remotes_backfilled": 0,
        "cwds_backfilled": 0,
        "sessions_assigned": 0,
        "sessions_merged": 0,
        "path_projects_deleted": 0,
        "paths_dead": 0,
    });
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let unprobed: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.id, s.source_id, sf.path FROM sessions s \
         JOIN sources src ON src.id = s.source_id AND src.kind = 'codex' \
         JOIN source_files sf ON sf.session_id = s.id AND sf.exists_status = 'present' \
         WHERE s.project_id IS NULL AND s.remote IS NULL AND s.cwd IS NOT NULL ORDER BY s.id LIMIT ?",
    )
    .bind(limit as i64)
    .fetch_all(&idx.reader)
    .await?;
    if !unprobed.is_empty() {
        let files = unprobed
            .iter()
            .map(|(_, source, path)| (source.clone(), std::path::PathBuf::from(path)))
            .collect();
        let remotes = discovery
            .probe_headers(HarnessKind::Codex, files)
            .await
            .map_err(|e| sqlx::Error::Io(std::io::Error::other(e)))?;
        let mut tx = idx.writer.begin().await?;
        let mut found = 0;
        for ((sid, _, _), manifest) in unprobed.iter().zip(remotes) {
            if let Some(remote) = manifest.and_then(|m| m.remote) {
                catalog::set_session_remote(&mut tx, sid, &remote).await?;
                found += 1;
            }
        }
        tx.commit().await?;
        report["remotes_backfilled"] = json!(found);
    }
    report["cwds_backfilled"] = json!(backfill_claude_cwds(idx, discovery, limit).await?);
    if stop.load(Ordering::Relaxed) {
        return Ok(report);
    }

    let orphans: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT DISTINCT cwd, remote FROM sessions WHERE project_id IS NULL AND cwd IS NOT NULL ORDER BY cwd, remote LIMIT ?",
    )
    .bind(limit as i64)
    .fetch_all(&idx.reader)
    .await?;
    let resolved = tokio::task::spawn_blocking(move || {
        orphans
            .into_iter()
            .map(|(cwd, remote)| {
                let key = crate::projects::resolve(&cwd, remote.as_deref());
                (cwd, remote, key)
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|e| sqlx::Error::Io(std::io::Error::other(e)))?;
    if !resolved.is_empty() {
        let mut tx = idx.writer.begin().await?;
        let mut assigned = 0;
        for (cwd, remote, key) in &resolved {
            assigned +=
                catalog::assign_project_to_cwd(&mut tx, cwd, remote.as_deref(), key, now).await?;
        }
        tx.commit().await?;
        report["sessions_assigned"] = json!(assigned);
    }
    seen.extend(resolved.into_iter().map(|(cwd, _, _)| cwd));
    {
        let mut tx = idx.writer.begin().await?;
        let (moved, deleted) = catalog::merge_path_projects(&mut tx, limit).await?;
        tx.commit().await?;
        report["sessions_merged"] = json!(moved);
        report["path_projects_deleted"] = json!(deleted);
    }
    if stop.load(Ordering::Relaxed) || seen.len() >= limit {
        return Ok(report);
    }

    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT path FROM project_paths WHERE dead_at IS NULL ORDER BY resolved_at, path LIMIT ?",
    )
    .bind((limit + seen.len()) as i64)
    .fetch_all(&idx.reader)
    .await?;
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|path| !seen.contains(path))
        .take(limit - seen.len())
        .collect();
    let probed = tokio::task::spawn_blocking(move || {
        rows.into_iter()
            .map(|path| {
                let alive = std::fs::metadata(&path).is_ok();
                (path, alive)
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|e| sqlx::Error::Io(std::io::Error::other(e)))?;
    if probed.is_empty() {
        return Ok(report);
    }
    let mut tx = idx.writer.begin().await?;
    let mut dead = 0;
    for (path, alive) in &probed {
        if *alive {
            sqlx::query("UPDATE project_paths SET resolved_at = ?1 WHERE path = ?2")
                .bind(now)
                .bind(path)
                .execute(&mut *tx)
                .await?;
        } else {
            dead += sqlx::query(
                "UPDATE project_paths SET dead_at = ?1 WHERE path = ?2 AND dead_at IS NULL",
            )
            .bind(now)
            .bind(path)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }
    }
    tx.commit().await?;
    report["paths_dead"] = json!(dead);
    Ok(report)
}

const CLAUDE_CWD_CURSOR: &str = "maintenance.claude_cwd_after";

/// Set the missing `cwd` of indexed Claude sessions from their source header. A cursor in
/// `app_meta` advances past every probed session, so one whose header has no cwd is not re-read;
/// sessions parsed or re-parsed later take their cwd from the parser itself.
async fn backfill_claude_cwds(
    idx: &Index,
    discovery: &DiscoveryService,
    limit: usize,
) -> Result<u64, sqlx::Error> {
    let after = catalog::app_meta_get(idx, CLAUDE_CWD_CURSOR)
        .await?
        .unwrap_or_default();
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.id, s.source_id, sf.path FROM sessions s \
         JOIN sources src ON src.id = s.source_id AND src.kind = 'claude' \
         JOIN source_files sf ON sf.session_id = s.id AND sf.exists_status = 'present' \
         WHERE s.cwd IS NULL AND s.index_state = 'indexed' AND s.id > ? ORDER BY s.id LIMIT ?",
    )
    .bind(&after)
    .bind(limit as i64)
    .fetch_all(&idx.reader)
    .await?;
    let Some((last, _, _)) = rows.last() else {
        return Ok(0);
    };
    let last = last.clone();
    let files = rows
        .iter()
        .map(|(_, source, path)| (source.clone(), std::path::PathBuf::from(path)))
        .collect();
    let manifests = discovery
        .probe_headers(HarnessKind::Claude, files)
        .await
        .map_err(|e| sqlx::Error::Io(std::io::Error::other(e)))?;

    let mut tx = idx.writer.begin().await?;
    let mut found = 0;
    for ((sid, _, _), manifest) in rows.iter().zip(manifests) {
        if let Some(cwd) = manifest.and_then(|m| m.cwd) {
            found += sqlx::query("UPDATE sessions SET cwd = ?1 WHERE id = ?2 AND cwd IS NULL")
                .bind(cwd)
                .bind(sid)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        }
    }
    tx.commit().await?;
    catalog::app_meta_set(idx, CLAUDE_CWD_CURSOR, &last).await?;
    Ok(found)
}
