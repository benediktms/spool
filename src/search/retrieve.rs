//! SQL retrieval for `search_sessions`: every function runs on the caller's single read transaction so lexical
//! candidates, vectors and metadata all belong to one `index_epoch`. Filters are part of every candidate query
//! (applied before `LIMIT`); only the session's `active_snapshot_id` with state `published` is eligible.

use crate::ipc::AppError;
use serde::Serialize;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Row, SqliteConnection};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtOrd};
use std::time::Instant;

pub fn storage_err(_: sqlx::Error) -> AppError {
    AppError::new("STORAGE_ERROR", "the local index could not be read")
}

#[derive(Debug, Clone)]
pub enum Bind {
    Text(String),
    Int(i64),
    Blob(Vec<u8>),
}

fn build(
    sql: String,
    binds: &[Bind],
) -> sqlx::query::Query<'static, sqlx::Sqlite, SqliteArguments> {
    // The SQL text is assembled only from fixed fragments and `?` placeholders; every user value is bound.
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    for b in binds {
        q = match b {
            Bind::Text(s) => q.bind(s.clone()),
            Bind::Int(i) => q.bind(*i),
            Bind::Blob(b) => q.bind(b.clone()),
        };
    }
    q
}

fn ph(n: usize) -> String {
    vec!["?"; n].join(",")
}

#[derive(Debug, Clone, Default)]
pub struct Filters {
    pub sources: Vec<String>,
    pub session_ids: Vec<String>,
    pub kinds: Vec<String>,
    pub cwds: Vec<String>,
    pub after: Option<i64>,
    pub before: Option<i64>,
    pub include_unknown_time: bool,
}

impl Filters {
    pub fn has_time(&self) -> bool {
        self.after.is_some() || self.before.is_some()
    }

    /// WHERE fragment over aliases `s` (sessions) and `time_col`, plus its binds in textual order.
    fn clause(&self, time_col: &str) -> (String, Vec<Bind>) {
        let (mut sql, mut binds) = self.base_clause();
        if let Some((t, tb)) = self.time_clause(time_col) {
            sql.push_str(" AND ");
            sql.push_str(&t);
            binds.extend(tb);
        }
        (sql, binds)
    }

    /// Everything except the time window: exclusion and the source/session/kind/cwd lists.
    fn base_clause(&self) -> (String, Vec<Bind>) {
        let mut sql = String::from("s.excluded = 0");
        let mut binds = Vec::new();
        for (col, vals) in [
            ("s.session_kind", &self.kinds),
            ("s.source_id", &self.sources),
            ("s.id", &self.session_ids),
            ("s.cwd", &self.cwds),
        ] {
            if !vals.is_empty() {
                sql.push_str(&format!(" AND {col} IN ({})", ph(vals.len())));
                binds.extend(vals.iter().cloned().map(Bind::Text));
            }
        }
        (sql, binds)
    }

    /// The `[after, before)` window over `time_col` (spec 13.2: the chunk time anchor), `None` without a time filter.
    fn time_clause(&self, time_col: &str) -> Option<(String, Vec<Bind>)> {
        if !self.has_time() {
            return None;
        }
        let mut binds = Vec::new();
        let mut cond = format!("{time_col} IS NOT NULL");
        if let Some(a) = self.after {
            cond.push_str(&format!(" AND {time_col} >= ?"));
            binds.push(Bind::Int(a));
        }
        if let Some(b) = self.before {
            cond.push_str(&format!(" AND {time_col} < ?"));
            binds.push(Bind::Int(b));
        }
        Some((
            if self.include_unknown_time {
                format!("(({cond}) OR {time_col} IS NULL)")
            } else {
                format!("({cond})")
            },
            binds,
        ))
    }
}

const CHUNK_FROM: &str = "FROM chunks c \
    JOIN sessions s ON s.id = c.session_id AND s.active_snapshot_id = c.snapshot_id \
    JOIN snapshots sn ON sn.id = c.snapshot_id AND sn.state = 'published'";

const COVERAGE_FROM: &str = "FROM sessions s \
    JOIN snapshots sn ON sn.id = s.active_snapshot_id AND sn.state = 'published' \
    JOIN chunks c INDEXED BY chunks_coverage ON c.snapshot_id = s.active_snapshot_id AND c.session_id = s.id";

// ---------------------------------------------------------------- lexical

#[derive(Debug, Clone)]
pub struct Scored {
    pub rowid: i64,
    pub logical_id: String,
    /// bm25 (lower is better) for lexical candidates, cosine for semantic ones.
    pub score: f64,
}

/// Top-`limit` eligible chunks by bm25. FTS5 produces the ranking on its own (no joins, so SQLite only has to
/// rank, not probe four tables per match); eligibility + filters are then applied to each ranked page, and paging
/// continues until `limit` eligible chunks are found or the matches are exhausted. Filters therefore still apply
/// before the limit.
pub async fn lexical(
    conn: &mut SqliteConnection,
    f: &Filters,
    match_expr: &str,
    limit: usize,
) -> Result<Vec<Scored>, AppError> {
    let (w, filter_binds) = f.clause("c.time_anchor");
    let mut out: Vec<Scored> = Vec::new();
    let mut offset = 0usize;
    let mut page = limit.max(64) + limit.max(64) / 4;
    loop {
        let ranked = sqlx::query("SELECT rowid, bm25(chunks_fts) AS score FROM chunks_fts WHERE chunks_fts MATCH ? ORDER BY score ASC, rowid ASC LIMIT ? OFFSET ?")
            .bind(match_expr)
            .bind(page as i64)
            .bind(offset as i64)
            .fetch_all(&mut *conn)
            .await
            .map_err(storage_err)?;
        let n = ranked.len();
        let scores: Vec<(i64, f64)> = ranked.iter().map(|r| (r.get(0), r.get(1))).collect();
        drop(ranked);
        let mut ok: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
        for part in scores.chunks(400) {
            let sql = format!(
                "SELECT c.rowid, c.logical_id {CHUNK_FROM} WHERE c.rowid IN ({}) AND {w}",
                ph(part.len())
            );
            let mut binds: Vec<Bind> = part.iter().map(|(r, _)| Bind::Int(*r)).collect();
            binds.extend(filter_binds.iter().cloned());
            for r in build(sql, &binds)
                .fetch_all(&mut *conn)
                .await
                .map_err(storage_err)?
            {
                ok.insert(r.get(0), r.get(1));
            }
        }
        for (rowid, score) in scores {
            if let Some(id) = ok.remove(&rowid) {
                out.push(Scored {
                    rowid,
                    logical_id: id,
                    score,
                });
            }
        }
        if out.len() >= limit || n < page {
            break;
        }
        offset += n;
        page = (page * 2).min(8192);
    }
    // Deterministic order: bm25, then stable chunk id.
    out.sort_by(|a, b| {
        a.score
            .total_cmp(&b.score)
            .then_with(|| a.logical_id.cmp(&b.logical_id))
            .then_with(|| a.rowid.cmp(&b.rowid))
    });
    out.truncate(limit);
    Ok(out)
}

// ---------------------------------------------------------------- semantic

struct Entry {
    score: f32,
    logical_id: String,
    rowid: i64,
}

impl PartialEq for Entry {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}
impl Eq for Entry {}

impl Ord for Entry {
    /// "Greater" is better: higher score, then smaller logical id, then smaller rowid.
    fn cmp(&self, o: &Self) -> Ordering {
        self.score
            .total_cmp(&o.score)
            .then_with(|| o.logical_id.cmp(&self.logical_id))
            .then_with(|| o.rowid.cmp(&self.rowid))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

type Heap = BinaryHeap<Reverse<Entry>>;

struct Batch {
    rows: Vec<(i64, String, Vec<u8>)>,
}

/// Score one decoded-on-the-fly batch into the bounded top-K heap. Returns `false` when cancelled / past deadline.
fn score_batch(
    heap: &mut Heap,
    batch: Batch,
    q: &[f32],
    k: usize,
    cancel: &AtomicBool,
    deadline: Instant,
) -> bool {
    for (i, (rowid, logical_id, blob)) in batch.rows.into_iter().enumerate() {
        if i % 512 == 0 && (cancel.load(AtOrd::Relaxed) || Instant::now() >= deadline) {
            return false;
        }
        if blob.len() != q.len() * 4 {
            continue;
        }
        let score: f32 = blob
            .as_chunks::<4>()
            .0
            .iter()
            .zip(q)
            .map(|(b, x)| f32::from_le_bytes(*b) * x)
            .sum();
        if !score.is_finite() {
            continue;
        }
        if heap.len() >= k && heap.peek().is_some_and(|w| score < w.0.score) {
            continue;
        }
        heap.push(Reverse(Entry {
            score,
            logical_id,
            rowid,
        }));
        if heap.len() > k {
            heap.pop();
        }
    }
    true
}

pub struct ScanOutcome {
    pub top: Vec<Scored>,
    pub complete: bool,
}

/// Limits of one semantic scan: top-`k` kept, vector bytes per batch, cooperative cancel and local deadline.
pub struct ScanLimits {
    pub k: usize,
    pub batch_bytes: usize,
    pub cancel: Arc<AtomicBool>,
    pub deadline: Instant,
}

/// Exact scan of every eligible chunk with a vector for `profile_id` (dimension `q.len()`), in batches of at most
/// `batch_bytes` of vector data; scoring runs in `spawn_blocking` (the caller holds the concurrency permit).
pub async fn semantic_scan(
    conn: &mut SqliteConnection,
    f: &Filters,
    profile_id: &str,
    q: Arc<Vec<f32>>,
    limits: ScanLimits,
) -> Result<ScanOutcome, AppError> {
    let ScanLimits {
        k,
        batch_bytes,
        cancel,
        deadline,
    } = limits;
    let (w, filter_binds) = f.clause("c.time_anchor");
    let rows_per_batch = (batch_bytes / (q.len() * 4).max(1)).max(1);
    // Driven by the embeddings primary key (profile, input_hash): a sequential range scan of the vectors, with
    // chunk eligibility probed per vector. Keyset paging on (input_hash, chunk rowid) keeps duplicate texts exact.
    let sql = format!(
        "SELECT c.rowid AS rowid, c.logical_id AS logical_id, e.vec AS vec, e.input_hash AS h \
         FROM embeddings e JOIN chunks c ON c.input_hash = e.input_hash \
         JOIN sessions s ON s.id = c.session_id AND s.active_snapshot_id = c.snapshot_id \
         JOIN snapshots sn ON sn.id = c.snapshot_id AND sn.state = 'published' \
         WHERE e.profile_id = ? AND e.dimensions = ? AND e.input_hash >= ? AND (e.input_hash > ? OR c.rowid > ?) AND {w} \
         ORDER BY e.input_hash ASC, c.rowid ASC LIMIT ?"
    );
    let mut heap: Heap = BinaryHeap::new();
    let mut after: (Vec<u8>, i64) = (Vec::new(), i64::MIN);
    loop {
        let mut binds = vec![
            Bind::Text(profile_id.to_string()),
            Bind::Int(q.len() as i64),
            Bind::Blob(after.0.clone()),
            Bind::Blob(after.0.clone()),
            Bind::Int(after.1),
        ];
        binds.extend(filter_binds.iter().cloned());
        binds.push(Bind::Int(rows_per_batch as i64));
        let rows = build(sql.clone(), &binds)
            .fetch_all(&mut *conn)
            .await
            .map_err(storage_err)?;
        let n = rows.len();
        let mut batch = Batch {
            rows: Vec::with_capacity(n),
        };
        for r in &rows {
            batch.rows.push((
                r.get::<i64, _>(0),
                r.get::<String, _>(1),
                r.get::<Vec<u8>, _>(2),
            ));
        }
        if let Some(r) = rows.last() {
            after = (r.get::<Vec<u8>, _>(3), r.get::<i64, _>(0));
        }
        drop(rows);
        let (q2, c2) = (q.clone(), cancel.clone());
        let (h, ok) = tokio::task::spawn_blocking(move || {
            let ok = score_batch(&mut heap, batch, &q2, k, &c2, deadline);
            (heap, ok)
        })
        .await
        .map_err(|_| AppError::new("RESOURCE_LIMIT", "semantic scoring was interrupted"))?;
        heap = h;
        if !ok {
            return Ok(ScanOutcome {
                top: into_sorted(heap),
                complete: false,
            });
        }
        if n < rows_per_batch {
            return Ok(ScanOutcome {
                top: into_sorted(heap),
                complete: true,
            });
        }
    }
}

fn into_sorted(heap: Heap) -> Vec<Scored> {
    let mut v: Vec<Entry> = heap.into_iter().map(|r| r.0).collect();
    v.sort_by(|a, b| b.cmp(a));
    v.into_iter()
        .map(|e| Scored {
            rowid: e.rowid,
            logical_id: e.logical_id,
            score: f64::from(e.score),
        })
        .collect()
}

// ---------------------------------------------------------------- metadata

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub rowid: i64,
    pub session_id: String,
    pub snapshot_id: String,
    pub run_key: String,
    pub ordinal: i64,
    pub time_anchor: Option<i64>,
    pub last_activity_at: Option<i64>,
}

pub async fn load_chunks(
    conn: &mut SqliteConnection,
    rowids: &[i64],
) -> Result<Vec<ChunkMeta>, AppError> {
    let mut out = Vec::with_capacity(rowids.len());
    for part in rowids.chunks(500) {
        let sql = format!(
            "SELECT c.rowid AS rowid, c.logical_id, c.session_id, c.snapshot_id, c.run_key, c.ordinal, c.time_anchor, s.last_activity_at AS la \
             FROM chunks c JOIN sessions s ON s.id = c.session_id WHERE c.rowid IN ({})",
            ph(part.len())
        );
        let binds: Vec<Bind> = part.iter().map(|r| Bind::Int(*r)).collect();
        for r in build(sql, &binds)
            .fetch_all(&mut *conn)
            .await
            .map_err(storage_err)?
        {
            out.push(ChunkMeta {
                rowid: r.get("rowid"),
                session_id: r.get("session_id"),
                snapshot_id: r.get("snapshot_id"),
                run_key: r.get("run_key"),
                ordinal: r.get("ordinal"),
                time_anchor: r.get("time_anchor"),
                last_activity_at: r.get("la"),
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct SpanRow {
    pub record_key: String,
    pub block_index: i64,
    pub start: i64,
    pub end: i64,
    pub is_context: bool,
    /// Timestamp of the message that owns the span (context-vs-interval labelling).
    pub msg_ts: Option<i64>,
}

/// Spans of each chunk in `rowids`, in `seq` order. Every requested chunk gets an entry (possibly empty).
pub async fn load_spans(
    conn: &mut SqliteConnection,
    rowids: &[i64],
) -> Result<HashMap<i64, Vec<SpanRow>>, AppError> {
    let mut out: HashMap<i64, Vec<SpanRow>> = rowids.iter().map(|r| (*r, Vec::new())).collect();
    for part in rowids.chunks(500) {
        let sql = format!(
            "SELECT sp.chunk_rowid, sp.record_key, sp.block_index, sp.start_byte, sp.end_byte, sp.is_context, m.ts \
             FROM chunk_spans sp JOIN chunks c ON c.rowid = sp.chunk_rowid \
             LEFT JOIN messages m ON m.snapshot_id = c.snapshot_id AND m.record_key = sp.record_key \
             WHERE sp.chunk_rowid IN ({}) ORDER BY sp.chunk_rowid, sp.seq",
            ph(part.len())
        );
        let binds: Vec<Bind> = part.iter().map(|r| Bind::Int(*r)).collect();
        for r in build(sql, &binds)
            .fetch_all(&mut *conn)
            .await
            .map_err(storage_err)?
        {
            out.entry(r.get(0)).or_default().push(SpanRow {
                record_key: r.get(1),
                block_index: r.get(2),
                start: r.get(3),
                end: r.get(4),
                is_context: r.get::<i64, _>(5) != 0,
                msg_ts: r.get(6),
            });
        }
    }
    Ok(out)
}

pub async fn chunk_body(conn: &mut SqliteConnection, rowid: i64) -> Result<String, AppError> {
    sqlx::query_scalar("SELECT body FROM chunks WHERE rowid = ?")
        .bind(rowid)
        .fetch_one(&mut *conn)
        .await
        .map_err(storage_err)
}

#[derive(Debug, Clone)]
pub struct SessionRow {
    pub id: String,
    pub source_id: String,
    pub kind: String,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub created_at: Option<i64>,
    pub last_activity_at: Option<i64>,
    pub source_status: String,
}

pub async fn session_row(
    conn: &mut SqliteConnection,
    id: &str,
) -> Result<Option<SessionRow>, AppError> {
    let r = sqlx::query(
        "SELECT id, source_id, session_kind, native_title, cwd, created_at, last_activity_at, source_status FROM sessions WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut *conn)
    .await
    .map_err(storage_err)?;
    Ok(r.map(|r| SessionRow {
        id: r.get(0),
        source_id: r.get(1),
        kind: r.get(2),
        title: r.get(3),
        cwd: r.get(4),
        created_at: r.get(5),
        last_activity_at: r.get(6),
        source_status: r.get(7),
    }))
}

/// Bytes `[start, end)` of a block's text (original span content for the match digest).
pub async fn block_bytes(
    conn: &mut SqliteConnection,
    snapshot: &str,
    record_key: &str,
    idx: i64,
    start: i64,
    end: i64,
) -> Result<Vec<u8>, AppError> {
    let v: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT substr(CAST(text AS BLOB), ?, ?) FROM blocks WHERE snapshot_id = ? AND record_key = ? AND idx = ?",
    )
    .bind(start + 1)
    .bind((end - start).max(0))
    .bind(snapshot)
    .bind(record_key)
    .bind(idx)
    .fetch_optional(&mut *conn)
    .await
    .map_err(storage_err)?;
    v.ok_or_else(|| AppError::new("STORAGE_ERROR", "the local index could not be read"))
}

/// Leaf message keys of the branch tree under `from_key` (inclusive), at most two (callers only need 0/1/many).
pub async fn leaves_under(
    conn: &mut SqliteConnection,
    snapshot: &str,
    from_key: Option<&str>,
) -> Result<Vec<String>, AppError> {
    let rows = match from_key {
        Some(k) => sqlx::query_scalar(
            "WITH RECURSIVE d(k) AS (SELECT ? UNION ALL SELECT m.record_key FROM messages m JOIN d ON m.parent_message_key = d.k WHERE m.snapshot_id = ?) \
             SELECT k FROM d WHERE NOT EXISTS (SELECT 1 FROM messages c WHERE c.snapshot_id = ? AND c.parent_message_key = d.k) ORDER BY k LIMIT 2",
        )
        .bind(k)
        .bind(snapshot)
        .bind(snapshot)
        .fetch_all(&mut *conn)
        .await,
        None => sqlx::query_scalar(
            "SELECT m.record_key FROM messages m WHERE m.snapshot_id = ? \
             AND NOT EXISTS (SELECT 1 FROM messages c WHERE c.snapshot_id = m.snapshot_id AND c.parent_message_key = m.record_key) ORDER BY m.record_key LIMIT 2",
        )
        .bind(snapshot)
        .fetch_all(&mut *conn)
        .await,
    };
    rows.map_err(storage_err)
}

// ---------------------------------------------------------------- literal

#[derive(Debug, Clone)]
pub struct LiteralHit {
    pub session_id: String,
    pub snapshot_id: String,
    pub role: String,
    /// Timestamp of the message holding the hit.
    pub ts: Option<i64>,
    /// Time anchor of the chunk that holds the hit (what filters and `matched_at` use); `None` if unknown.
    pub anchor: Option<i64>,
    pub record_key: String,
    pub block_index: i64,
    pub byte_start: i64,
    pub hit_text: Vec<u8>,
    pub window: String,
    pub window_covers_all: bool,
}

/// Time anchor of the active chunk whose new-content (non-context) span holds byte `{a}.bpos` of the hit's block
/// (spec 13.2: time filters and `matched_at` use the chunk anchor in every mode). `NULL` when no chunk covers it.
fn covering_anchor(a: &str) -> String {
    format!(
        "(SELECT c.time_anchor FROM chunk_spans sp JOIN chunks c ON c.rowid = sp.chunk_rowid \
          WHERE c.snapshot_id = {a}.snap AND sp.record_key = {a}.rk AND sp.block_index = {a}.bi AND sp.is_context = 0 \
            AND sp.start_byte <= {a}.bpos AND {a}.bpos < sp.end_byte ORDER BY c.ordinal LIMIT 1)"
    )
}

/// Exact, parameterized substring search over retained block text of active published snapshots (no FTS
/// prefilter, so punctuation and spans across chunk boundaries match). Each hit is associated with the chunk that
/// holds its first byte; the time filter and the returned `ts` use that chunk's anchor. At most `per_session`
/// first-hit blocks per session, taken after the filters; sessions ordered by recency (opt-in) or id.
pub async fn literal(
    conn: &mut SqliteConnection,
    f: &Filters,
    needle: &str,
    case_sensitive: bool,
    per_session: usize,
    recent_first: bool,
    limit: usize,
) -> Result<Vec<LiteralHit>, AppError> {
    let (w, base_binds) = f.base_clause();
    let (x, needle) = if case_sensitive {
        ("b.text", needle.to_string())
    } else {
        ("lower(b.text)", needle.to_ascii_lowercase())
    };
    let order = if recent_first {
        "h2.la DESC, h2.sid, h2.seq, h2.bi"
    } else {
        "h2.sid, h2.seq, h2.bi"
    };
    // The chunk lookup runs per candidate hit only when a time window has to be applied; otherwise only for the
    // returned rows (final select).
    let time = f.time_clause("anchor");
    let (anchor_all, time_where, time_binds) = match time {
        Some((c, tb)) => (covering_anchor("hits"), format!(" WHERE {c}"), tb),
        None => ("NULL".to_string(), String::new(), Vec::new()),
    };
    let final_anchor = covering_anchor("h2");
    let sql = format!(
        "WITH hits AS (\
           SELECT s.id AS sid, s.last_activity_at AS la, sn.id AS snap, m.seq AS seq, m.ts AS mts, m.role AS role, b.record_key AS rk, b.idx AS bi, \
                  instr({x}, ?) AS pos, length(CAST(substr(b.text, 1, instr({x}, ?) - 1) AS BLOB)) AS bpos \
           FROM blocks b \
           JOIN messages m ON m.snapshot_id = b.snapshot_id AND m.record_key = b.record_key \
           JOIN snapshots sn ON sn.id = b.snapshot_id AND sn.state = 'published' \
           JOIN sessions s ON s.id = sn.session_id AND s.active_snapshot_id = sn.id \
           WHERE {w} AND instr({x}, ?) > 0), \
         anch AS (SELECT hits.*, {anchor_all} AS anchor FROM hits), \
         ranked AS (SELECT anch.*, row_number() OVER (PARTITION BY sid ORDER BY seq, bi) AS rn FROM anch{time_where}), \
         h2 AS (SELECT * FROM ranked WHERE rn <= ?) \
         SELECT h2.sid, h2.snap, h2.role, h2.mts, h2.rk, h2.bi, h2.bpos AS byte_start, \
                CAST(substr(b.text, h2.pos, ?) AS BLOB) AS hit_text, \
                substr(b.text, max(1, h2.pos - 150), 600) AS win, \
                (max(1, h2.pos - 150) = 1 AND length(b.text) <= 600) AS whole, \
                {final_anchor} AS chunk_anchor \
         FROM h2 JOIN blocks b ON b.snapshot_id = h2.snap AND b.record_key = h2.rk AND b.idx = h2.bi \
         ORDER BY {order} LIMIT ?"
    );
    let mut binds = vec![Bind::Text(needle.clone()), Bind::Text(needle.clone())];
    binds.extend(base_binds);
    binds.push(Bind::Text(needle.clone()));
    binds.extend(time_binds);
    binds.push(Bind::Int(per_session as i64));
    binds.push(Bind::Int(needle.chars().count() as i64));
    binds.push(Bind::Int(limit as i64));
    let rows = build(sql, &binds)
        .fetch_all(&mut *conn)
        .await
        .map_err(storage_err)?;
    Ok(rows
        .iter()
        .map(|r| LiteralHit {
            session_id: r.get("sid"),
            snapshot_id: r.get("snap"),
            role: r.get("role"),
            ts: r.get("mts"),
            anchor: r.get("chunk_anchor"),
            record_key: r.get("rk"),
            block_index: r.get("bi"),
            byte_start: r.get("byte_start"),
            hit_text: r.get("hit_text"),
            window: r.get("win"),
            window_covers_all: r.get::<i64, _>("whole") != 0,
        })
        .collect())
}

// ---------------------------------------------------------------- coverage

#[derive(Debug, Clone, Serialize)]
pub struct CoverageRow {
    pub indexed_sessions: i64,
    pub active_chunks: i64,
    pub embedded_chunks: i64,
    pub pending_sessions: i64,
    pub last_scan_ms: Option<i64>,
    pub epoch: i64,
}

/// Chunk counts are expensive (they join the whole active corpus), so they are cached per
/// (index_epoch, profile, indexed session count); every publication and committed embedding batch bumps the epoch.
pub type CoverageCache = std::sync::Mutex<Option<((i64, String, i64), (i64, i64))>>;

pub async fn coverage(
    conn: &mut SqliteConnection,
    profile_id: &str,
    cache: &CoverageCache,
) -> Result<CoverageRow, AppError> {
    let one = |sql: &'static str| sqlx::query_scalar::<_, i64>(sql);
    let indexed_sessions = one("SELECT count(*) FROM sessions s JOIN snapshots sn ON sn.id = s.active_snapshot_id AND sn.state = 'published' WHERE s.excluded = 0")
        .fetch_one(&mut *conn)
        .await
        .map_err(storage_err)?;
    let epoch: Option<String> =
        sqlx::query_scalar("SELECT value FROM app_meta WHERE key = 'index_epoch'")
            .fetch_optional(&mut *conn)
            .await
            .map_err(storage_err)?;
    let epoch = epoch.and_then(|v| v.parse().ok()).unwrap_or(0);
    let key = (epoch, profile_id.to_string(), indexed_sessions);
    let hit = cache
        .lock()
        .ok()
        .and_then(|c| c.as_ref().filter(|(k, _)| *k == key).map(|(_, v)| *v));
    let (active_chunks, embedded_chunks) = match hit {
        Some(v) => v,
        None => {
            let active: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) {COVERAGE_FROM} WHERE s.excluded = 0"
            )))
            .fetch_one(&mut *conn)
            .await
            .map_err(storage_err)?;
            let embedded: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) {COVERAGE_FROM} JOIN embeddings e INDEXED BY embeddings_key ON e.profile_id = ? AND e.input_hash = c.input_hash WHERE s.excluded = 0"
            )))
            .bind(profile_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(storage_err)?;
            if let Ok(mut c) = cache.lock() {
                *c = Some((key, (active, embedded)));
            }
            (active, embedded)
        }
    };
    let pending_sessions = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM sessions s WHERE s.excluded = 0 AND s.index_state IN ('pending','deferred') AND {}",
        crate::storage::catalog::INDEXABLE_KIND
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(storage_err)?;
    let last_scan_ms: Option<i64> =
        sqlx::query_scalar("SELECT max(last_scan_at) FROM sources WHERE last_scan_ok = 1")
            .fetch_one(&mut *conn)
            .await
            .map_err(storage_err)?;
    Ok(CoverageRow {
        indexed_sessions,
        active_chunks,
        embedded_chunks,
        pending_sessions,
        last_scan_ms,
        epoch,
    })
}
