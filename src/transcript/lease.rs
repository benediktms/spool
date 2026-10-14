//! Bounded read leases (spec 14.2.8). A lease pins manifests (and, through them, retired
//! conversational snapshots) until it expires. Limits: active lease count and charged bytes.

use crate::clock::SharedClock;
use crate::config::ReaderConfig;
use crate::ids::new_id;
use crate::ipc::AppError;
use crate::storage::index::Index;
use sqlx::Row;

#[derive(Clone)]
pub struct LeaseManager {
    index: Index,
    cfg: ReaderConfig,
    clock: SharedClock,
}

#[derive(Debug, Clone)]
pub struct PinTarget {
    pub session_id: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone)]
pub struct Lease {
    pub lease_id: String,
    pub expires_at_ms: i64,
    /// manifest id per requested target, same order.
    pub manifest_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseState {
    Live { expires_at_ms: i64 },
    Expired,
    Revoked,
    Unknown,
}

fn storage_err(_: sqlx::Error) -> AppError {
    AppError::new(
        "STORAGE_ERROR",
        "required local state cannot be accessed safely",
    )
}

impl LeaseManager {
    pub fn new(index: Index, cfg: ReaderConfig, clock: SharedClock) -> Self {
        Self { index, cfg, clock }
    }

    pub fn ttl_ms(&self) -> i64 {
        (self.cfg.lease_ttl_seconds as i64).saturating_mul(1000)
    }

    /// Create a lease over the given snapshots (creating manifests if absent). Rejects with
    /// `RESOURCE_LIMIT` when the configured lease count or retained-byte bound would be exceeded;
    /// never evicts a live lease early.
    pub async fn pin(&self, targets: &[PinTarget]) -> Result<Lease, AppError> {
        let now = self.clock.now_ms();
        let expires = now + self.ttl_ms();
        let mut tx = self.index.writer.begin().await.map_err(storage_err)?;
        let (live, live_bytes): (i64, i64) = sqlx::query_as(
            "SELECT count(*), COALESCE(sum(bytes_charged),0) FROM read_leases WHERE state='live' AND expires_at > ?",
        )
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_err)?;
        let mut manifests = Vec::with_capacity(targets.len());
        let mut bytes = 0i64;
        for t in targets {
            let row = sqlx::query(
                "SELECT s.kind, s.state, s.prefix_end, s.file_dev, s.file_ino, s.serializer_version, s.retained_bytes, ss.source_id, ss.excluded \
                 FROM snapshots s JOIN sessions ss ON ss.id = s.session_id WHERE s.id = ? AND s.session_id = ?",
            )
            .bind(&t.snapshot_id)
            .bind(&t.session_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_err)?;
            let Some(row) = row else {
                return Err(AppError::new(
                    "STALE_REFERENCE",
                    "snapshot is no longer available",
                ));
            };
            if row.get::<i64, _>("excluded") != 0 || row.get::<String, _>("state") == "staging" {
                return Err(AppError::new("SESSION_NOT_FOUND", "no accessible session"));
            }
            let retained: i64 = row.get("retained_bytes");
            bytes += retained;
            let existing: Option<String> =
                sqlx::query_scalar("SELECT id FROM read_manifests WHERE snapshot_id = ?")
                    .bind(&t.snapshot_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(storage_err)?;
            let id = match existing {
                Some(id) => id,
                None => {
                    let id = new_id("mf");
                    sqlx::query(
                        "INSERT INTO read_manifests(id, session_id, snapshot_id, kind, source_id, file_dev, file_ino, prefix_end, serializer_version, created_at, retained_bytes) \
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    )
                    .bind(&id)
                    .bind(&t.session_id)
                    .bind(&t.snapshot_id)
                    .bind(row.get::<String, _>("kind"))
                    .bind(row.get::<String, _>("source_id"))
                    .bind(row.get::<Option<i64>, _>("file_dev"))
                    .bind(row.get::<Option<i64>, _>("file_ino"))
                    .bind(row.get::<i64, _>("prefix_end"))
                    .bind(row.get::<i64, _>("serializer_version"))
                    .bind(now)
                    .bind(retained)
                    .execute(&mut *tx)
                    .await
                    .map_err(storage_err)?;
                    id
                }
            };
            manifests.push(id);
        }
        if live as usize >= self.cfg.max_active_leases
            || (live_bytes + bytes) as u64 > self.cfg.max_retained_lease_bytes
        {
            let mut e = AppError::new(
                "RESOURCE_LIMIT",
                "the read-lease budget is exhausted; retry after leases expire",
            );
            e.retryable = true;
            e.recovery = Some("retry_later".into());
            return Err(e);
        }
        let lease_id = new_id("ls");
        sqlx::query("INSERT INTO read_leases(id, expires_at, state, bytes_charged, created_at) VALUES (?,?,?,?,?)")
            .bind(&lease_id)
            .bind(expires)
            .bind("live")
            .bind(bytes)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_err)?;
        for m in &manifests {
            sqlx::query(
                "INSERT OR IGNORE INTO lease_manifests(lease_id, manifest_id) VALUES (?,?)",
            )
            .bind(&lease_id)
            .bind(m)
            .execute(&mut *tx)
            .await
            .map_err(storage_err)?;
        }
        tx.commit().await.map_err(storage_err)?;
        Ok(Lease {
            lease_id,
            expires_at_ms: expires,
            manifest_ids: manifests,
        })
    }

    pub async fn state(&self, lease_id: &str) -> Result<LeaseState, AppError> {
        let now = self.clock.now_ms();
        let row = sqlx::query("SELECT state, expires_at FROM read_leases WHERE id = ?")
            .bind(lease_id)
            .fetch_optional(&self.index.reader)
            .await
            .map_err(storage_err)?;
        Ok(match row {
            None => LeaseState::Unknown,
            Some(r) if r.get::<String, _>("state") == "revoked" => LeaseState::Revoked,
            Some(r) if r.get::<i64, _>("expires_at") <= now => LeaseState::Expired,
            Some(r) => LeaseState::Live {
                expires_at_ms: r.get("expires_at"),
            },
        })
    }

    /// Is `manifest_id` pinned by live lease `lease_id`?
    pub async fn covers(&self, lease_id: &str, manifest_id: &str) -> Result<bool, AppError> {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM lease_manifests WHERE lease_id = ? AND manifest_id = ?",
        )
        .bind(lease_id)
        .bind(manifest_id)
        .fetch_one(&self.index.reader)
        .await
        .map_err(storage_err)?;
        Ok(n > 0)
    }

    /// Revoke every lease that pins a manifest of `session_id` (exclusion/purge).
    pub async fn revoke_session(&self, session_id: &str) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE read_leases SET state='revoked' WHERE id IN (SELECT lm.lease_id FROM lease_manifests lm JOIN read_manifests m ON m.id = lm.manifest_id WHERE m.session_id = ?)",
        )
        .bind(session_id)
        .execute(&self.index.writer)
        .await
        .map_err(storage_err)?;
        Ok(())
    }

    /// Release expired/revoked leases, unreferenced manifests, and retired/read-only snapshots no
    /// live lease pins. Bounded; safe to call repeatedly. Returns (leases, snapshots) removed.
    pub async fn sweep(&self) -> Result<(u64, u64), AppError> {
        let now = self.clock.now_ms();
        let mut tx = self.index.writer.begin().await.map_err(storage_err)?;
        let leases =
            sqlx::query("DELETE FROM read_leases WHERE state='revoked' OR expires_at <= ?")
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage_err)?
                .rows_affected();
        sqlx::query(
            "DELETE FROM read_manifests WHERE id NOT IN (SELECT manifest_id FROM lease_manifests)",
        )
        .execute(&mut *tx)
        .await
        .map_err(storage_err)?;
        let snaps = sqlx::query(
            "DELETE FROM snapshots WHERE state IN ('retired_leased','read_only') AND id NOT IN (SELECT snapshot_id FROM read_manifests) \
             AND created_at < ?",
        )
        // A freshly staged read_only snapshot is briefly unpinned between staging and `pin`: grace period.
        .bind(now - 60_000)
        .execute(&mut *tx)
        .await
        .map_err(storage_err)?
        .rows_affected();
        tx.commit().await.map_err(storage_err)?;
        Ok((leases, snaps))
    }
}
