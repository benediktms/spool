//! Guarded publication (spec 10.4, 10.5): ONE short transaction inserts the chunks of a staged
//! snapshot, switches the session's active snapshot and bumps the index epoch.

use super::chunk::ChunkPlan;
use super::stage::{StageError, StageMeta, kind_str};
use crate::clock::SharedClock;
use crate::ids::hex;
use crate::storage::index::{Index, bump_epoch, retire_snapshot};
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::HashMap;

/// Binds per INSERT, matching the ~500 used by the other batched queries (SQLite's limit is far higher).
const MAX_BINDS: usize = 500;
const CHUNK_COLS: usize = 10;
const SPAN_COLS: usize = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    Published {
        generation: i64,
        retired_snapshot: Option<String>,
    },
    /// The session moved on (generation, fingerprint or the staging row is gone). Staging deleted.
    Superseded,
    /// The session is excluded/tombstoned. Staging deleted; nothing was published.
    Excluded,
}

/// hash(session id, chunker version, ordered spans, context flags, input hash). Independent of the
/// snapshot, so an unchanged sealed chunk keeps its logical id across re-parses.
pub fn chunk_logical_id(session_id: &str, chunker_version: u32, c: &ChunkPlan) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"spool-chunk-v1");
    h.update(&(session_id.len() as u64).to_le_bytes());
    h.update(session_id.as_bytes());
    h.update(&chunker_version.to_le_bytes());
    for s in &c.spans {
        h.update(&(s.message_key.len() as u64).to_le_bytes());
        h.update(s.message_key.as_bytes());
        h.update(&s.block_index.to_le_bytes());
        h.update(&(s.start as u64).to_le_bytes());
        h.update(&(s.end as u64).to_le_bytes());
        h.update(&[s.is_context as u8]);
    }
    h.update(&c.input_hash);
    format!("ck_{}", &hex(h.finalize().as_bytes())[..32])
}

pub async fn publish(
    index: &Index,
    clock: &SharedClock,
    session_id: &str,
    snapshot_id: &str,
    expected_generation: i64,
    fingerprint: &str,
    plan: &[ChunkPlan],
) -> Result<PublishOutcome, StageError> {
    let now = clock.now_ms();
    let mut tx = index.writer.begin().await?;

    let sess = sqlx::query(
        "SELECT s.generation, s.active_snapshot_id, s.tombstoned_at IS NOT NULL AS tomb, \
                (s.excluded = 1 OR EXISTS (SELECT 1 FROM exclusions e WHERE (e.kind='session' AND e.target_id = s.id) \
                                           OR (e.kind='source' AND e.target_id = s.source_id))) AS excl \
         FROM sessions s WHERE s.id = ?",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(sess) = sess else {
        tx.rollback().await?;
        return Err(StageError::Source("unknown session".into()));
    };
    let generation: i64 = sess.get("generation");
    let active: Option<&str> = sess.get("active_snapshot_id");
    let excluded = sess.get::<i64, _>("excl") != 0 || sess.get::<i64, _>("tomb") != 0;

    let snap = sqlx::query("SELECT state, fingerprint, counts_json, coverage, generation FROM snapshots WHERE id = ? AND session_id = ? AND kind = 'indexed'")
        .bind(snapshot_id)
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await?;

    let reject = if excluded {
        Some(PublishOutcome::Excluded)
    } else if generation != expected_generation {
        Some(PublishOutcome::Superseded)
    } else {
        match &snap {
            Some(s)
                if s.get::<&str, _>("state") == "staging"
                    && s.get::<&str, _>("fingerprint") == fingerprint =>
            {
                None
            }
            _ => Some(PublishOutcome::Superseded),
        }
    };
    if let Some(outcome) = reject {
        sqlx::query("DELETE FROM snapshots WHERE id = ? AND session_id = ? AND state = 'staging'")
            .bind(snapshot_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(outcome);
    }
    let snap = snap.expect("checked above");
    let meta: StageMeta = serde_json::from_str(snap.get::<&str, _>("counts_json"))
        .map_err(|_| StageError::Storage("snapshot meta".into()))?;
    let coverage: &str = snap.get("coverage");

    for batch in plan.chunks(MAX_BINDS / CHUNK_COLS) {
        let ids: Vec<String> = batch
            .iter()
            .map(|c| chunk_logical_id(session_id, meta.chunker_version, c))
            .collect();
        let mut qb = QueryBuilder::<Sqlite>::new(
            "INSERT INTO chunks(logical_id,snapshot_id,session_id,run_key,ordinal,body,input_hash,time_anchor,rendered_tokens,is_tail) ",
        );
        qb.push_values(batch.iter().zip(&ids), |mut b, (c, id)| {
            b.push_bind(id)
                .push_bind(snapshot_id)
                .push_bind(session_id)
                .push_bind(&c.run_key)
                .push_bind(c.ordinal as i64)
                .push_bind(&c.rendered)
                .push_bind(&c.input_hash[..])
                .push_bind(c.time_anchor)
                .push_bind(c.rendered_tokens as i64)
                .push_bind(c.is_tail as i64);
        });
        qb.push(" RETURNING rowid, logical_id");
        let rows = qb.build().fetch_all(&mut *tx).await?;
        let rowids: HashMap<&str, i64> = rows
            .iter()
            .map(|r| (r.get::<&str, _>("logical_id"), r.get::<i64, _>("rowid")))
            .collect();

        let mut spans = Vec::new();
        for (c, id) in batch.iter().zip(&ids) {
            let rowid = *rowids
                .get(id.as_str())
                .ok_or_else(|| StageError::Storage("chunk rowid missing".into()))?;
            spans.extend(
                c.spans
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (rowid, i as i64, s)),
            );
        }
        for part in spans.chunks(MAX_BINDS / SPAN_COLS) {
            let mut qb = QueryBuilder::<Sqlite>::new(
                "INSERT INTO chunk_spans(chunk_rowid,seq,record_key,block_index,start_byte,end_byte,is_context) ",
            );
            qb.push_values(part, |mut b, &(rowid, seq, s)| {
                b.push_bind(rowid)
                    .push_bind(seq)
                    .push_bind(&s.message_key)
                    .push_bind(s.block_index as i64)
                    .push_bind(s.start as i64)
                    .push_bind(s.end as i64)
                    .push_bind(s.is_context as i64);
            });
            qb.build().execute(&mut *tx).await?;
        }
    }

    let new_generation = generation + 1;
    sqlx::query("UPDATE snapshots SET state='published', published_at=?, generation=? WHERE id=?")
        .bind(now)
        .bind(new_generation)
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE sessions SET active_snapshot_id=?, generation=?, native_title=?, cwd=?, session_kind=?, lineage_native_id=?, lineage_verified=?, \
         created_at=?, last_activity_at=?, coverage_status=?, index_state='indexed', index_error=NULL, observed_fingerprint=? WHERE id=?",
    )
    .bind(snapshot_id)
    .bind(new_generation)
    .bind(&meta.title)
    .bind(&meta.cwd)
    .bind(kind_str(meta.session_kind))
    .bind(&meta.lineage)
    .bind(meta.lineage_verified as i64)
    .bind(meta.started_at)
    .bind(meta.last_activity_at)
    .bind(coverage)
    .bind(fingerprint)
    .bind(session_id)
    .execute(&mut *tx)
    .await?;

    let retired = match active {
        Some(prev) if prev != snapshot_id => {
            retire_snapshot(&mut tx, prev, now).await?;
            Some(prev.to_owned())
        }
        _ => None,
    };
    // Obsolete concurrent stagings of this session can never publish now.
    sqlx::query(
        "DELETE FROM snapshots WHERE session_id=? AND kind='indexed' AND state='staging' AND id<>?",
    )
    .bind(session_id)
    .bind(snapshot_id)
    .execute(&mut *tx)
    .await?;
    bump_epoch(&mut tx).await?;
    tx.commit().await?;
    Ok(PublishOutcome::Published {
        generation: new_generation,
        retired_snapshot: retired,
    })
}
