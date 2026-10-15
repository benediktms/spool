//! `operations` table bookkeeping: durable CLI operations with idempotency keys (spec 11.3, 14.5).
//! The Apalis task is the execution record; this table is what the operator asks about.

use crate::ids;
use crate::ipc::AppError;
use crate::storage::index::Index;
use serde_json::{Value, json};
use sqlx::Row;

#[derive(Debug, Clone)]
pub struct OpRow {
    pub id: String,
    pub kind: String,
    pub params: Value,
    pub idempotency_key: Option<String>,
    pub task_id: Option<String>,
    pub state: String,
    pub progress: Value,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

pub fn storage_err(_: sqlx::Error) -> AppError {
    AppError {
        retryable: true,
        ..AppError::new(
            "STORAGE_ERROR",
            "required local state cannot be accessed safely",
        )
    }
}

fn op(r: &sqlx::sqlite::SqliteRow) -> OpRow {
    OpRow {
        id: r.get("id"),
        kind: r.get("kind"),
        params: serde_json::from_str(&r.get::<String, _>("params_json")).unwrap_or(Value::Null),
        idempotency_key: r.get("idempotency_key"),
        task_id: r.get("task_id"),
        state: r.get("state"),
        progress: serde_json::from_str(&r.get::<String, _>("progress_json")).unwrap_or(Value::Null),
        error: r.get("error"),
        cancel_requested: r.get::<i64, _>("cancel_requested") != 0,
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

/// Create an operation, or return the existing one for the same idempotency key and parameters.
/// Same key with different parameters is `IDEMPOTENCY_CONFLICT`. Returns `(row, created)`.
pub async fn begin(
    index: &Index,
    kind: &str,
    params: &Value,
    key: Option<&str>,
    now: i64,
) -> Result<(OpRow, bool), AppError> {
    let params_json = serde_json::to_string(params).unwrap_or_default();
    let mut tx = index.writer.begin().await.map_err(storage_err)?;
    if let Some(k) = key
        && let Some(r) = sqlx::query("SELECT * FROM operations WHERE idempotency_key = ?")
            .bind(k)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_err)?
    {
        let existing = op(&r);
        if existing.kind != kind
            || serde_json::to_string(&existing.params).unwrap_or_default() != params_json
        {
            return Err(AppError::new(
                "IDEMPOTENCY_CONFLICT",
                "this idempotency key was already used with different parameters",
            ));
        }
        return Ok((existing, false));
    }
    let id = ids::new_id("op");
    sqlx::query(
        "INSERT INTO operations(id, kind, params_json, idempotency_key, state, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, 'accepted', ?5, ?5)",
    )
    .bind(&id)
    .bind(kind)
    .bind(&params_json)
    .bind(key)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(storage_err)?;
    tx.commit().await.map_err(storage_err)?;
    Ok((
        get(index, &id)
            .await?
            .ok_or_else(|| AppError::new("STORAGE_ERROR", "operation vanished"))?,
        true,
    ))
}

pub async fn get(index: &Index, id: &str) -> Result<Option<OpRow>, AppError> {
    Ok(sqlx::query("SELECT * FROM operations WHERE id = ?")
        .bind(id)
        .fetch_optional(&index.reader)
        .await
        .map_err(storage_err)?
        .map(|r| op(&r)))
}

pub async fn set_task(index: &Index, id: &str, task_id: &str, now: i64) -> Result<(), AppError> {
    sqlx::query("UPDATE operations SET task_id = ?2, updated_at = ?3 WHERE id = ?1")
        .bind(id)
        .bind(task_id)
        .bind(now)
        .execute(&index.writer)
        .await
        .map_err(storage_err)?;
    Ok(())
}

/// Terminal states are sticky except through `jobs retry`.
pub async fn set_state(
    index: &Index,
    id: &str,
    state: &str,
    error: Option<&str>,
    now: i64,
) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE operations SET state = ?2, error = ?3, updated_at = ?4 WHERE id = ?1 \
         AND (state IN ('accepted','running') OR ?2 = 'accepted')",
    )
    .bind(id)
    .bind(state)
    .bind(error)
    .bind(now)
    .execute(&index.writer)
    .await
    .map_err(storage_err)?;
    Ok(())
}

/// RFC 7396 merge into `progress_json` (null removes a key).
pub async fn merge_progress(
    index: &Index,
    id: &str,
    patch: &Value,
    now: i64,
) -> Result<(), AppError> {
    sqlx::query("UPDATE operations SET progress_json = json_patch(progress_json, ?2), updated_at = ?3 WHERE id = ?1")
        .bind(id)
        .bind(serde_json::to_string(patch).unwrap_or_default())
        .bind(now)
        .execute(&index.writer)
        .await
        .map_err(storage_err)?;
    Ok(())
}

pub async fn request_cancel(index: &Index, id: &str, now: i64) -> Result<(), AppError> {
    sqlx::query("UPDATE operations SET cancel_requested = 1, updated_at = ?2 WHERE id = ?1")
        .bind(id)
        .bind(now)
        .execute(&index.writer)
        .await
        .map_err(storage_err)?;
    Ok(())
}

pub async fn cancel_requested(index: &Index, id: &str) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT cancel_requested FROM operations WHERE id = ?")
        .bind(id)
        .fetch_optional(&index.reader)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
        != 0
}

/// Re-arm a failed/cancelled operation for `jobs retry`.
pub async fn rearm(index: &Index, id: &str, now: i64) -> Result<(), AppError> {
    sqlx::query("UPDATE operations SET state = 'accepted', error = NULL, cancel_requested = 0, task_id = NULL, updated_at = ?2 WHERE id = ?1")
        .bind(id)
        .bind(now)
        .execute(&index.writer)
        .await
        .map_err(storage_err)?;
    Ok(())
}

pub async fn list(index: &Index, after: Option<&str>, limit: i64) -> Result<Vec<OpRow>, AppError> {
    Ok(
        sqlx::query("SELECT * FROM operations WHERE (?1 IS NULL OR id > ?1) ORDER BY id LIMIT ?2")
            .bind(after)
            .bind(limit)
            .fetch_all(&index.reader)
            .await
            .map_err(storage_err)?
            .iter()
            .map(op)
            .collect(),
    )
}

impl OpRow {
    pub fn brief(&self) -> Value {
        json!({ "operation_id": self.id, "kind": self.kind, "state": self.state, "task_id": self.task_id, "created_at_ms": self.created_at, "updated_at_ms": self.updated_at })
    }
}
