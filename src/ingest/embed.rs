//! Embedding step (spec 10.4 steps 4-5, 11.5): runs AFTER publication, so text is lexically
//! searchable even when the provider fails. Only active chunks without a cached vector for the
//! active profile are sent; with nothing missing there is no provider call at all.

use super::engine::{IngestError, IngestErrorKind};
use crate::clock::SharedClock;
use crate::domain::Digest;
use crate::embeddings::{EmbedError, EmbedErrorKind, EmbeddingService};
use crate::storage::index::Index;
use sqlx::Row;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbedStats {
    /// Distinct inputs that lacked a vector when the step started.
    pub missing_before: usize,
    /// Distinct inputs still lacking a vector afterwards.
    pub missing_after: usize,
    /// Embedding is not available by configuration (not approved / no credential / disabled).
    pub skipped: bool,
    /// Vectors newly committed by this call.
    pub embedded: usize,
}

/// Map a content-free provider error to the job-level classification (spec 11.7). `None` means the
/// provider is unavailable by configuration, which is not a failure (lexical-only operation).
pub fn map_embed_error(e: &EmbedError) -> Option<IngestError> {
    use EmbedErrorKind::*;
    let (kind, retryable) = match e.kind {
        NotApproved | NoCredential | Disabled => return None,
        Auth => (IngestErrorKind::AuthBlocked, false),
        BudgetExhausted => (IngestErrorKind::BudgetDeferred, false),
        RateLimited | Server | Network | Timeout | Unavailable => {
            (IngestErrorKind::Transient, true)
        }
        BadResponse => (IngestErrorKind::Transient, e.retryable),
        UnsupportedModel => (IngestErrorKind::Permanent, false),
        Cancelled => (IngestErrorKind::Cancelled, true),
    };
    Some(IngestError {
        retryable,
        kind,
        retry_after: e.retry_after,
        message: format!("embedding provider: {}", e.kind.as_str()),
    })
}

const MISSING_SQL: &str = "SELECT c.input_hash AS h, c.body AS body FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id AND s.id = c.session_id \
     WHERE c.session_id = ? AND NOT EXISTS (SELECT 1 FROM embeddings e WHERE e.profile_id = ? AND e.input_hash = c.input_hash) \
     GROUP BY c.input_hash ORDER BY min(c.rowid)";

async fn missing(
    index: &Index,
    session_id: &str,
    profile_id: &str,
) -> Result<Vec<(Digest, String)>, IngestError> {
    let rows = sqlx::query(MISSING_SQL)
        .bind(session_id)
        .bind(profile_id)
        .fetch_all(&index.writer)
        .await
        .map_err(IngestError::storage)?;
    rows.into_iter()
        .map(|r| {
            let h: Vec<u8> = r.get("h");
            let h: Digest = h
                .try_into()
                .map_err(|_| IngestError::permanent("stored input hash has the wrong length"))?;
            Ok((h, r.get::<String, _>("body")))
        })
        .collect()
}

/// Number of distinct active chunk inputs lacking a vector for `profile_id`.
pub async fn count_missing(
    index: &Index,
    session_id: &str,
    profile_id: &str,
) -> Result<usize, IngestError> {
    Ok(missing(index, session_id, profile_id).await?.len())
}

pub async fn embed_missing(
    index: &Index,
    embeddings: &EmbeddingService,
    clock: &SharedClock,
    enabled: bool,
    session_id: &str,
    cancel: &AtomicBool,
) -> Result<EmbedStats, IngestError> {
    let profile_id = embeddings.profile().id();
    let inputs = missing(index, session_id, &profile_id).await?;
    let mut stats = EmbedStats {
        missing_before: inputs.len(),
        missing_after: inputs.len(),
        skipped: false,
        embedded: 0,
    };
    if inputs.is_empty() {
        // Everything this session needs is cached: a profile migration may have just become complete.
        let _ = embeddings.activate_if_covered().await;
        return Ok(stats);
    }
    if !enabled {
        stats.skipped = true;
        return Ok(stats);
    }
    let prog = match embeddings.embed_documents(inputs, cancel).await {
        Ok(p) => p,
        Err(e) => match map_embed_error(&e) {
            Some(err) => return Err(err),
            None => {
                stats.skipped = true;
                return Ok(stats);
            }
        },
    };
    stats.embedded = prog.embedded;
    if prog.budget_exhausted {
        let wait = prog
            .resets_at_ms
            .map(|t| Duration::from_millis((t - clock.now_ms()).max(0) as u64));
        return Err(IngestError {
            retryable: false,
            kind: IngestErrorKind::BudgetDeferred,
            retry_after: wait,
            message: "embedding budget exhausted".into(),
        });
    }
    if prog.cancelled {
        return Err(IngestError::new(
            IngestErrorKind::Cancelled,
            true,
            "cancelled",
        ));
    }
    // Inputs the provider can never embed (empty / over the per-input limit) are not "pending".
    stats.missing_after = count_missing(index, session_id, &profile_id)
        .await?
        .saturating_sub(prog.rejected);
    Ok(stats)
}
