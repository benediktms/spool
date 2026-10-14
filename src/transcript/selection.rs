//! Snapshot graph loading, selection resolution (session/match/message/range/item, spec 14.2.2-3)
//! and the per-view item plan (conversation / with_tools turn closure / raw_records, spec 14.2.4).
//!
//! Everything here is pure over rows already loaded from one frozen snapshot; nothing reads source
//! files. Ceiling: the record/message graph of one snapshot is loaded into memory per call
//! (metadata only, no text); fine for the 2 GiB / 16 MiB-per-record ingest limits, revisit with a
//! cached graph if profiles show it matters.

use super::refs::ItemKind;
use super::types::*;
use crate::domain::Role;
use crate::ipc::AppError;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::{HashMap, HashSet};

pub fn app_err(
    code: &str,
    message: impl Into<String>,
    retryable: bool,
    recovery: &str,
) -> AppError {
    let mut e = AppError::new(code, message);
    e.retryable = retryable;
    e.recovery = Some(recovery.into());
    e
}

fn storage_err(_: sqlx::Error) -> AppError {
    AppError::new(
        "STORAGE_ERROR",
        "required local state cannot be accessed safely",
    )
}

fn stale(msg: &str) -> AppError {
    app_err("STALE_REFERENCE", msg, false, "new_read")
}

// ---------------------------------------------------------------------------------------------
// Snapshot graph
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Rec {
    pub key: String,
    pub native_id: Option<String>,
    pub parent: Option<String>,
    pub kind: String,
    pub ts: Option<i64>,
    pub offset: u64,
    pub len: u64,
    pub digest: [u8; 32],
    pub turn: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Msg {
    pub key: String,
    pub role: Role,
    pub phase: Option<String>,
    pub ts: Option<i64>,
    pub parent: Option<String>,
}

#[derive(Debug)]
pub struct Snap {
    pub id: String,
    pub session_id: String,
    pub kind: SnapshotKind,
    pub state: String,
    pub created_at: i64,
    pub coverage: String,
    pub file_dev: Option<i64>,
    pub file_ino: Option<i64>,
    pub prefix_end: i64,
    pub revision: String,
    pub active_leaf: Option<String>,
    pub recs: Vec<Rec>,
    pub rec_ix: HashMap<String, usize>,
    rec_children: HashMap<String, Vec<usize>>,
    pub msgs: Vec<Msg>,
    pub msg_ix: HashMap<String, usize>,
    msg_children: HashMap<String, Vec<usize>>,
}

impl Snap {
    /// Load one snapshot's graph. `STALE_REFERENCE` when it no longer exists.
    pub async fn load(pool: &SqlitePool, snapshot_id: &str) -> Result<Snap, AppError> {
        let row = sqlx::query(
            "SELECT session_id, kind, state, created_at, coverage, file_dev, file_ino, prefix_end, source_revision, active_leaf_key FROM snapshots WHERE id = ?",
        )
        .bind(snapshot_id)
        .fetch_optional(pool)
        .await
        .map_err(storage_err)?
        .ok_or_else(|| stale("snapshot is no longer available"))?;
        let state: String = row.get("state");
        if state == "staging" {
            return Err(stale("snapshot is not readable"));
        }
        let kind = if row.get::<String, _>("kind") == "indexed" {
            SnapshotKind::Indexed
        } else {
            SnapshotKind::SourceRead
        };
        let mut snap = Snap {
            id: snapshot_id.to_string(),
            session_id: row.get("session_id"),
            kind,
            state,
            created_at: row.get("created_at"),
            coverage: row.get("coverage"),
            file_dev: row.get("file_dev"),
            file_ino: row.get("file_ino"),
            prefix_end: row.get("prefix_end"),
            revision: row.get("source_revision"),
            active_leaf: row.get("active_leaf_key"),
            recs: Vec::new(),
            rec_ix: HashMap::new(),
            rec_children: HashMap::new(),
            msgs: Vec::new(),
            msg_ix: HashMap::new(),
            msg_children: HashMap::new(),
        };
        let rows = sqlx::query(
            "SELECT key, native_id, parent_key, kind, ts, \"offset\" AS off, len, digest, turn_id FROM records WHERE snapshot_id = ? ORDER BY ord",
        )
        .bind(snapshot_id)
        .fetch_all(pool)
        .await
        .map_err(storage_err)?;
        for r in rows {
            let d: Vec<u8> = r.get("digest");
            let digest: [u8; 32] = d
                .try_into()
                .map_err(|_| AppError::new("STORAGE_ERROR", "record digest is malformed"))?;
            snap.recs.push(Rec {
                key: r.get("key"),
                native_id: r.get("native_id"),
                parent: r.get("parent_key"),
                kind: r.get("kind"),
                ts: r.get("ts"),
                offset: r.get::<i64, _>("off").max(0) as u64,
                len: r.get::<i64, _>("len").max(0) as u64,
                digest,
                turn: r.get("turn_id"),
            });
        }
        let rows = sqlx::query("SELECT record_key, role, phase, ts, parent_message_key FROM messages WHERE snapshot_id = ? ORDER BY seq")
            .bind(snapshot_id)
            .fetch_all(pool)
            .await
            .map_err(storage_err)?;
        for r in rows {
            let role = if r.get::<String, _>("role") == "user" {
                Role::User
            } else {
                Role::Assistant
            };
            snap.msgs.push(Msg {
                key: r.get("record_key"),
                role,
                phase: r.get("phase"),
                ts: r.get("ts"),
                parent: r.get("parent_message_key"),
            });
        }
        snap.reindex();
        Ok(snap)
    }

    fn reindex(&mut self) {
        for (i, r) in self.recs.iter().enumerate() {
            self.rec_ix.insert(r.key.clone(), i);
            if let Some(p) = &r.parent {
                self.rec_children.entry(p.clone()).or_default().push(i);
            }
        }
        for (i, m) in self.msgs.iter().enumerate() {
            self.msg_ix.insert(m.key.clone(), i);
            if let Some(p) = &m.parent {
                self.msg_children.entry(p.clone()).or_default().push(i);
            }
        }
    }

    /// Test constructor over in-memory rows (same indexing as `load`).
    #[cfg(test)]
    pub fn from_parts(recs: Vec<Rec>, msgs: Vec<Msg>) -> Snap {
        let mut s = Snap {
            id: "sn".into(),
            session_id: "ss".into(),
            kind: SnapshotKind::Indexed,
            state: "published".into(),
            created_at: 0,
            coverage: "complete".into(),
            file_dev: None,
            file_ino: None,
            prefix_end: 0,
            revision: String::new(),
            active_leaf: None,
            recs,
            rec_ix: HashMap::new(),
            rec_children: HashMap::new(),
            msgs,
            msg_ix: HashMap::new(),
            msg_children: HashMap::new(),
        };
        s.reindex();
        s
    }

    pub fn rec(&self, key: &str) -> Option<&Rec> {
        self.rec_ix.get(key).map(|i| &self.recs[*i])
    }

    /// Messages root -> `leaf` along parent links (indexes into `msgs`). `None` on unknown leaf/cycle.
    pub fn msg_path(&self, leaf: &str) -> Option<Vec<usize>> {
        let mut out = Vec::new();
        let mut cur = *self.msg_ix.get(leaf)?;
        loop {
            out.push(cur);
            if out.len() > self.msgs.len() {
                return None;
            }
            match self.msgs[cur]
                .parent
                .as_ref()
                .and_then(|p| self.msg_ix.get(p))
            {
                Some(p) => cur = *p,
                None => break,
            }
        }
        out.reverse();
        Some(out)
    }

    pub fn msg_children_of(&self, key: &str) -> &[usize] {
        self.msg_children
            .get(key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Message leaves (no message descendants), in `seq` order.
    pub fn leaves(&self) -> Vec<usize> {
        (0..self.msgs.len())
            .filter(|i| self.msg_children_of(&self.msgs[*i].key).is_empty())
            .collect()
    }

    /// Message leaves in the subtree of `key` (inclusive).
    pub fn leaves_under(&self, key: &str) -> Vec<usize> {
        let mut out = Vec::new();
        let mut stack = vec![key.to_string()];
        while let Some(k) = stack.pop() {
            let ch = self.msg_children_of(&k);
            if ch.is_empty() {
                if let Some(i) = self.msg_ix.get(&k) {
                    out.push(*i);
                }
            }
            for c in ch {
                stack.push(self.msgs[*c].key.clone());
            }
        }
        out.sort_unstable();
        out
    }

    /// Records root -> `end` by parent links, then the unique chain of non-message records below it
    /// (trailing tool/control records of a branch). A fork among record children stops the chain.
    pub fn rec_path_ext(&self, end: &str) -> Option<Vec<usize>> {
        let mut out = Vec::new();
        let mut cur = *self.rec_ix.get(end)?;
        loop {
            out.push(cur);
            if out.len() > self.recs.len() {
                return None;
            }
            match self.recs[cur]
                .parent
                .as_ref()
                .and_then(|p| self.rec_ix.get(p))
            {
                Some(p) => cur = *p,
                None => break,
            }
        }
        out.reverse();
        let mut seen = out.len();
        loop {
            let last = *out.last()?;
            let ch = self
                .rec_children
                .get(&self.recs[last].key)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            if ch.len() != 1
                || self.recs[ch[0]].kind == "message"
                || self.msg_ix.contains_key(&self.recs[ch[0]].key)
            {
                break;
            }
            out.push(ch[0]);
            seen += 1;
            if seen > self.recs.len() + 1 {
                break;
            }
        }
        Some(out)
    }
}

/// Tool metadata rows of one snapshot (loaded only for `with_tools` and tool item selections).
#[derive(Debug, Default)]
pub struct Tools {
    pub calls: Vec<TcRow>,
    pub results: Vec<TrRow>,
    /// Smallest retained text block index per message record (for within-record ordering).
    pub first_block: HashMap<String, u32>,
}

#[derive(Debug, Clone)]
pub struct TcRow {
    pub rec: String,
    pub block: u32,
    pub native: Option<String>,
    pub name: String,
    pub turn: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TrRow {
    pub rec: String,
    pub block: u32,
    pub native: Option<String>,
    pub call: Option<(String, u32)>,
    pub is_error: Option<bool>,
    pub turn: Option<String>,
}

impl Tools {
    pub async fn load(pool: &SqlitePool, snapshot_id: &str) -> Result<Tools, AppError> {
        let mut t = Tools::default();
        for r in sqlx::query("SELECT record_key, block_index, native_call_id, tool_name, turn_id FROM tool_calls WHERE snapshot_id = ?")
            .bind(snapshot_id)
            .fetch_all(pool)
            .await
            .map_err(storage_err)?
        {
            t.calls.push(TcRow { rec: r.get("record_key"), block: r.get::<i64, _>("block_index") as u32, native: r.get("native_call_id"), name: r.get("tool_name"), turn: r.get("turn_id") });
        }
        for r in sqlx::query(
            "SELECT record_key, block_index, native_call_id, call_record_key, call_block_index, is_error, turn_id FROM tool_results WHERE snapshot_id = ?",
        )
        .bind(snapshot_id)
        .fetch_all(pool)
        .await
        .map_err(storage_err)?
        {
            let ck: Option<String> = r.get("call_record_key");
            let cb: Option<i64> = r.get("call_block_index");
            t.results.push(TrRow {
                rec: r.get("record_key"),
                block: r.get::<i64, _>("block_index") as u32,
                native: r.get("native_call_id"),
                call: ck.zip(cb).map(|(k, b)| (k, b as u32)),
                is_error: r.get::<Option<i64>, _>("is_error").map(|v| v != 0),
                turn: r.get("turn_id"),
            });
        }
        for r in sqlx::query("SELECT record_key, min(idx) AS m FROM blocks WHERE snapshot_id = ? GROUP BY record_key")
            .bind(snapshot_id)
            .fetch_all(pool)
            .await
            .map_err(storage_err)?
        {
            t.first_block.insert(r.get("record_key"), r.get::<i64, _>("m") as u32);
        }
        Ok(t)
    }
}

// ---------------------------------------------------------------------------------------------
// Selection resolution
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ItemSel {
    pub k: ItemKind,
    pub r: String,
    pub b: Option<u32>,
}

/// A selection frozen against one snapshot. Bound into cursors so continuation never re-resolves
/// anything ambiguous.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedSel {
    pub kind: SelKind,
    /// Branch leaf message record key (the persisted path is root -> leaf). `None`: no messages.
    pub leaf: Option<String>,
    pub first: Option<String>,
    pub last: Option<String>,
    /// Record keys of the anchoring messages (match/message/range/item).
    pub anchors: Vec<String>,
    pub item: Option<ItemSel>,
    pub before: Option<(u32, u32)>,
    pub after: Option<(u32, u32)>,
}

#[derive(Debug)]
pub enum SelFail {
    App(AppError),
    /// Needs a path choice; the message indexes are the candidate leaves.
    Branch(Vec<usize>),
}

impl From<AppError> for SelFail {
    fn from(e: AppError) -> Self {
        SelFail::App(e)
    }
}

fn range_err(msg: &str) -> SelFail {
    SelFail::App(app_err("INVALID_RANGE", msg, false, "change_request"))
}

/// Extend a message path downward through unique children; returns the new end index.
fn extend_unique(snap: &Snap, path: &mut Vec<usize>) {
    loop {
        let last = *path.last().expect("non-empty path");
        let ch = snap.msg_children_of(&snap.msgs[last].key);
        if ch.len() != 1 || path.len() > snap.msgs.len() {
            break;
        }
        path.push(ch[0]);
    }
}

pub struct WindowInput<'a> {
    /// Anchor message record keys (match spans' messages / the message / range endpoints).
    pub anchors: &'a [String],
    pub before: u32,
    pub after: u32,
    pub branch_leaf: Option<&'a str>,
}

fn msg_idx(snap: &Snap, key: &str) -> Result<usize, SelFail> {
    snap.msg_ix
        .get(key)
        .copied()
        .ok_or_else(|| SelFail::App(stale("referenced message is not part of this snapshot")))
}

pub fn resolve_session(snap: &Snap, branch_leaf: Option<&str>) -> Result<ResolvedSel, SelFail> {
    let leaf = match branch_leaf {
        Some(l) => {
            msg_idx(snap, l)?;
            Some(l.to_string())
        }
        None => {
            let leaves = snap.leaves();
            match leaves.len() {
                0 => None,
                1 => Some(snap.msgs[leaves[0]].key.clone()),
                _ => return Err(SelFail::Branch(leaves)),
            }
        }
    };
    let (first, last) = match &leaf {
        Some(l) => {
            let p = snap
                .msg_path(l)
                .ok_or_else(|| SelFail::App(stale("branch path cannot be resolved")))?;
            (
                Some(snap.msgs[p[0]].key.clone()),
                Some(snap.msgs[*p.last().unwrap()].key.clone()),
            )
        }
        None => (None, None),
    };
    Ok(ResolvedSel {
        kind: SelKind::Session,
        leaf,
        first,
        last,
        anchors: vec![],
        item: None,
        before: None,
        after: None,
    })
}

/// match / message: anchors on one ancestry path plus `before`/`after` context messages.
pub fn resolve_window(
    snap: &Snap,
    kind: SelKind,
    w: WindowInput<'_>,
) -> Result<ResolvedSel, SelFail> {
    let mut idxs = Vec::new();
    for a in w.anchors {
        idxs.push(msg_idx(snap, a)?);
    }
    let deepest = idxs
        .iter()
        .copied()
        .max_by_key(|i| {
            snap.msg_path(&snap.msgs[*i].key)
                .map(|p| p.len())
                .unwrap_or(0)
        })
        .ok_or_else(|| SelFail::App(stale("reference has no message anchor")))?;
    let mut path = snap
        .msg_path(&snap.msgs[deepest].key)
        .ok_or_else(|| SelFail::App(stale("anchor path cannot be resolved")))?;
    let mut pos = Vec::new();
    for i in &idxs {
        match path.iter().position(|p| p == i) {
            Some(p) => pos.push(p),
            None => {
                return Err(SelFail::App(stale(
                    "anchored messages do not lie on one ancestry path",
                )));
            }
        }
    }
    let (a_lo, a_hi) = (*pos.iter().min().unwrap(), *pos.iter().max().unwrap());
    let leaf_key;
    if let Some(bl) = w.branch_leaf {
        let bi = msg_idx(snap, bl)?;
        let bp = snap
            .msg_path(bl)
            .ok_or_else(|| SelFail::App(stale("branch path cannot be resolved")))?;
        if bp.len() <= a_hi || bp[a_hi] != deepest {
            return Err(range_err("branch does not contain the anchored history"));
        }
        path = bp;
        leaf_key = snap.msgs[bi].key.clone();
    } else {
        extend_unique(snap, &mut path);
        let end = *path.last().unwrap();
        let avail_after = path.len() - 1 - a_hi;
        if (avail_after as u32) < w.after && snap.msg_children_of(&snap.msgs[end].key).len() > 1 {
            return Err(SelFail::Branch(snap.leaves_under(&snap.msgs[end].key)));
        }
        leaf_key = snap.msgs[end].key.clone();
    }
    let lo = a_lo.saturating_sub(w.before as usize);
    let hi = (a_hi + w.after as usize).min(path.len() - 1);
    Ok(ResolvedSel {
        kind,
        leaf: Some(leaf_key),
        first: Some(snap.msgs[path[lo]].key.clone()),
        last: Some(snap.msgs[path[hi]].key.clone()),
        anchors: w.anchors.to_vec(),
        item: None,
        before: Some((w.before, (a_lo - lo) as u32)),
        after: Some((w.after, (hi - a_hi) as u32)),
    })
}

pub fn resolve_range(
    snap: &Snap,
    from: &str,
    through: &str,
    branch_leaf: Option<&str>,
) -> Result<ResolvedSel, SelFail> {
    let (f, t) = (msg_idx(snap, from)?, msg_idx(snap, through)?);
    let mut path = snap
        .msg_path(through)
        .ok_or_else(|| SelFail::App(stale("range path cannot be resolved")))?;
    if !path.contains(&f) {
        let rev = snap.msg_path(from).map(|p| p.contains(&t)).unwrap_or(false);
        return Err(range_err(if rev {
            "range endpoints are reversed"
        } else {
            "range endpoints are on sibling branches"
        }));
    }
    let leaf_key;
    if let Some(bl) = branch_leaf {
        let bp = snap
            .msg_path(bl)
            .ok_or_else(|| SelFail::App(stale("branch path cannot be resolved")))?;
        if !bp.contains(&t) || !bp.contains(&f) {
            return Err(range_err("branch does not contain the range"));
        }
        leaf_key = bl.to_string();
    } else {
        extend_unique(snap, &mut path);
        leaf_key = snap.msgs[*path.last().unwrap()].key.clone();
    }
    Ok(ResolvedSel {
        kind: SelKind::Range,
        leaf: Some(leaf_key),
        first: Some(from.to_string()),
        last: Some(through.to_string()),
        anchors: vec![from.to_string(), through.to_string()],
        item: None,
        before: None,
        after: None,
    })
}

/// Item selections. A message item follows the unique chain below it (or `branch_leaf`). A tool
/// call's linked results are only ever taken along ONE path: the unique one, else `BRANCH_REQUIRED`
/// when linked results exist on more than one branch below the call (unless `branch_leaf` picks one).
/// `tools` is `Some` only for the with_tools view; other views reject tool items later.
pub fn resolve_item(
    snap: &Snap,
    tools: Option<&Tools>,
    item: ItemSel,
    branch_leaf: Option<&str>,
) -> Result<ResolvedSel, SelFail> {
    let mut sel = ResolvedSel {
        kind: SelKind::Item,
        leaf: None,
        first: None,
        last: None,
        anchors: vec![],
        item: Some(item.clone()),
        before: None,
        after: None,
    };
    let ri = *snap
        .rec_ix
        .get(&item.r)
        .ok_or_else(|| SelFail::App(stale("referenced record is not part of this snapshot")))?;
    // a supplied branch must contain the item's record
    let branch_path = match branch_leaf {
        Some(bl) => {
            msg_idx(snap, bl)?;
            let p = snap
                .rec_path_ext(bl)
                .ok_or_else(|| SelFail::App(stale("branch path cannot be resolved")))?;
            if !p.contains(&ri) {
                return Err(range_err("branch does not contain the referenced item"));
            }
            Some(p)
        }
        None => None,
    };
    match item.k {
        ItemKind::Message => {
            let i = msg_idx(snap, &item.r)?;
            let leaf = match branch_leaf {
                Some(bl) => bl.to_string(),
                None => {
                    let mut path = snap
                        .msg_path(&snap.msgs[i].key)
                        .ok_or_else(|| SelFail::App(stale("message path cannot be resolved")))?;
                    extend_unique(snap, &mut path);
                    snap.msgs[*path.last().unwrap()].key.clone()
                }
            };
            sel.leaf = Some(leaf);
            sel.first = Some(item.r.clone());
            sel.last = Some(item.r.clone());
            sel.anchors = vec![item.r];
        }
        ItemKind::ToolCall => {
            let (Some(tools), Some(b)) = (tools, item.b) else {
                return Ok(sel);
            };
            if let Some(bl) = branch_leaf {
                sel.leaf = Some(bl.to_string());
                return Ok(sel);
            }
            let linked: Vec<&TrRow> = tools
                .results
                .iter()
                .filter(|r| r.call.as_ref() == Some(&(item.r.clone(), b)))
                .collect();
            let leaves: Vec<usize> = snap
                .leaves()
                .into_iter()
                .filter(|l| {
                    snap.rec_path_ext(&snap.msgs[*l].key)
                        .is_some_and(|p| p.contains(&ri))
                })
                .collect();
            // results of each candidate branch (empty when the call is unanswered on that branch)
            let per_leaf: Vec<Vec<&str>> = leaves
                .iter()
                .map(|l| {
                    let p: HashSet<&str> = snap
                        .rec_path_ext(&snap.msgs[*l].key)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|i| snap.recs[i].key.as_str())
                        .collect();
                    linked
                        .iter()
                        .filter(|r| p.contains(r.rec.as_str()))
                        .map(|r| r.rec.as_str())
                        .collect()
                })
                .collect();
            let mut distinct: Vec<&Vec<&str>> = per_leaf.iter().filter(|s| !s.is_empty()).collect();
            distinct.sort();
            distinct.dedup();
            if distinct.len() > 1 {
                return Err(SelFail::Branch(leaves));
            }
            let pick = per_leaf
                .iter()
                .position(|s| !s.is_empty())
                .or(if leaves.is_empty() { None } else { Some(0) });
            sel.leaf = pick.map(|p| snap.msgs[leaves[p]].key.clone());
        }
        ItemKind::ToolResult | ItemKind::RawRecord => {
            if let (Some(bl), Some(_)) = (branch_leaf, branch_path) {
                sel.leaf = Some(bl.to_string());
            }
        }
    }
    Ok(sel)
}

// ---------------------------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Message,
    ToolCall,
    ToolResult,
    Raw,
}

#[derive(Debug, Clone)]
pub enum Extra {
    None,
    Call {
        native: Option<String>,
        name: String,
        results: Vec<(String, u32)>,
    },
    Result {
        native: Option<String>,
        call: Option<(String, u32)>,
        is_error: Option<bool>,
    },
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    pub class: Class,
    pub rec: usize,
    pub block: Option<u32>,
    pub outside: bool,
    pub extra: Extra,
}

impl PlanItem {
    /// Stable per-position key; bound into cursors so a divergent plan is detected.
    pub fn key(&self, snap: &Snap) -> String {
        format!(
            "{:?}:{}:{}",
            self.class,
            snap.recs[self.rec].key,
            self.block.map(|b| b as i64).unwrap_or(-1)
        )
    }
}

#[derive(Debug, Default)]
pub struct Plan {
    pub items: Vec<PlanItem>,
    pub warnings: Vec<Warning>,
    pub partial: Vec<String>,
    /// Message `msgs` indexes immediately outside the selected window on the resolved path.
    pub nav_before: Option<usize>,
    pub nav_after: Option<usize>,
}

fn warn(code: &str, message: &str) -> Warning {
    Warning {
        code: code.into(),
        message: message.into(),
    }
}

pub fn build_plan(
    snap: &Snap,
    tools: Option<&Tools>,
    sel: &ResolvedSel,
    view: View,
) -> Result<Plan, AppError> {
    let mut plan = Plan::default();
    if snap.coverage != "complete" {
        plan.partial.push("snapshot_partial".into());
    }
    if let Some(b) = sel.before {
        if b.1 < b.0 {
            plan.warnings.push(warn(
                "INCOMPLETE_CONTEXT",
                "fewer preceding messages exist on this path than requested",
            ));
        }
    }
    if let Some(a) = sel.after {
        if a.1 < a.0 {
            plan.warnings.push(warn(
                "INCOMPLETE_CONTEXT",
                "fewer following messages exist on this path than requested",
            ));
        }
    }
    if let Some(item) = &sel.item {
        build_item_plan(snap, tools, item, sel.leaf.as_deref(), view, &mut plan)?;
        // a message item's navigation is its neighbours on the resolved path
        if let (Some(leaf), Some(first)) = (&sel.leaf, &sel.first) {
            window_nav(snap, leaf, first, first, &mut plan);
        }
        return Ok(plan);
    }
    let (Some(leaf), Some(first), Some(last)) = (&sel.leaf, &sel.first, &sel.last) else {
        if view == View::RawRecords {
            for (i, r) in snap.recs.iter().enumerate() {
                if r.kind == "header" {
                    plan.items.push(raw_item(i, false));
                }
            }
        }
        return Ok(plan);
    };
    let path = snap
        .msg_path(leaf)
        .ok_or_else(|| stale("branch path cannot be resolved"))?;
    let lo = path
        .iter()
        .position(|i| snap.msgs[*i].key == *first)
        .ok_or_else(|| stale("window start is not on the branch"))?;
    let hi = path
        .iter()
        .position(|i| snap.msgs[*i].key == *last)
        .ok_or_else(|| stale("window end is not on the branch"))?;
    if hi < lo {
        return Err(stale("window endpoints are reversed"));
    }
    plan.nav_before = if lo > 0 { Some(path[lo - 1]) } else { None };
    plan.nav_after = path.get(hi + 1).copied();
    let window: Vec<usize> = path[lo..=hi].to_vec();
    match view {
        View::Conversation => {
            for mi in window {
                let ri = *snap
                    .rec_ix
                    .get(&snap.msgs[mi].key)
                    .ok_or_else(|| stale("message record is missing"))?;
                plan.items.push(PlanItem {
                    class: Class::Message,
                    rec: ri,
                    block: None,
                    outside: false,
                    extra: Extra::None,
                });
            }
        }
        View::RawRecords => {
            let rpath = snap
                .rec_path_ext(leaf)
                .ok_or_else(|| stale("record path cannot be resolved"))?;
            let whole_session = sel.kind == SelKind::Session;
            if whole_session {
                for (i, r) in snap.recs.iter().enumerate() {
                    if r.kind == "header" && !rpath.contains(&i) {
                        plan.items.push(raw_item(i, false));
                    }
                }
            }
            let (p0, p1) = if whole_session {
                (0, rpath.len().saturating_sub(1))
            } else {
                let find = |k: &str| {
                    rpath
                        .iter()
                        .position(|i| snap.recs[*i].key == k)
                        .ok_or_else(|| stale("message is not on the record path"))
                };
                (find(first)?, find(last)?)
            };
            for ri in &rpath[p0..=p1] {
                plan.items.push(raw_item(*ri, false));
            }
        }
        View::WithTools => {
            let tools = tools
                .ok_or_else(|| AppError::new("STORAGE_ERROR", "tool metadata was not loaded"))?;
            with_tools_plan(snap, tools, leaf, first, last, &window, &mut plan)?;
        }
    }
    Ok(plan)
}

fn window_nav(snap: &Snap, leaf: &str, first: &str, last: &str, plan: &mut Plan) {
    if let Some(path) = snap.msg_path(leaf) {
        let lo = path.iter().position(|i| snap.msgs[*i].key == first);
        let hi = path.iter().position(|i| snap.msgs[*i].key == last);
        if let (Some(lo), Some(hi)) = (lo, hi) {
            plan.nav_before = if lo > 0 { Some(path[lo - 1]) } else { None };
            plan.nav_after = path.get(hi + 1).copied();
        }
    }
}

fn raw_item(rec: usize, outside: bool) -> PlanItem {
    PlanItem {
        class: Class::Raw,
        rec,
        block: None,
        outside,
        extra: Extra::None,
    }
}

fn build_item_plan(
    snap: &Snap,
    tools: Option<&Tools>,
    item: &ItemSel,
    leaf: Option<&str>,
    view: View,
    plan: &mut Plan,
) -> Result<(), AppError> {
    let ri = *snap
        .rec_ix
        .get(&item.r)
        .ok_or_else(|| stale("referenced record is not part of this snapshot"))?;
    let unsupported = || {
        app_err(
            "VIEW_NOT_SUPPORTED",
            "the chosen view does not permit this item class",
            false,
            "change_request",
        )
    };
    if view == View::RawRecords {
        plan.items.push(raw_item(ri, false));
        return Ok(());
    }
    match item.k {
        ItemKind::RawRecord => Err(unsupported()),
        ItemKind::Message => {
            if !snap.msg_ix.contains_key(&item.r) {
                return Err(stale("referenced message is not part of this snapshot"));
            }
            plan.items.push(PlanItem {
                class: Class::Message,
                rec: ri,
                block: None,
                outside: false,
                extra: Extra::None,
            });
            Ok(())
        }
        ItemKind::ToolCall | ItemKind::ToolResult => {
            if view != View::WithTools {
                return Err(unsupported());
            }
            let tools = tools
                .ok_or_else(|| AppError::new("STORAGE_ERROR", "tool metadata was not loaded"))?;
            let b = item
                .b
                .ok_or_else(|| stale("tool item reference has no block"))?;
            if item.k == ItemKind::ToolCall {
                let c = tools
                    .calls
                    .iter()
                    .find(|c| c.rec == item.r && c.block == b)
                    .ok_or_else(|| stale("tool call is not part of this snapshot"))?;
                // results only along the resolved path (never sibling branches)
                let path_recs: Vec<usize> = match &leaf {
                    Some(l) => snap
                        .rec_path_ext(l)
                        .ok_or_else(|| stale("record path cannot be resolved"))?,
                    None => snap
                        .rec_path_ext(&item.r)
                        .ok_or_else(|| stale("record path cannot be resolved"))?,
                };
                let on_path: HashSet<&str> = path_recs
                    .iter()
                    .map(|i| snap.recs[*i].key.as_str())
                    .collect();
                let results: Vec<(String, u32)> = tools
                    .results
                    .iter()
                    .filter(|r| {
                        r.call.as_ref() == Some(&(c.rec.clone(), c.block))
                            && on_path.contains(r.rec.as_str())
                    })
                    .map(|r| (r.rec.clone(), r.block))
                    .collect();
                plan.items.push(PlanItem {
                    class: Class::ToolCall,
                    rec: ri,
                    block: Some(b),
                    outside: false,
                    extra: Extra::Call {
                        native: c.native.clone(),
                        name: c.name.clone(),
                        results,
                    },
                });
            } else {
                let r = tools
                    .results
                    .iter()
                    .find(|r| r.rec == item.r && r.block == b)
                    .ok_or_else(|| stale("tool result is not part of this snapshot"))?;
                plan.items.push(PlanItem {
                    class: Class::ToolResult,
                    rec: ri,
                    block: Some(b),
                    outside: false,
                    extra: Extra::Result {
                        native: r.native.clone(),
                        call: r.call.clone(),
                        is_error: r.is_error,
                    },
                });
                if r.call.is_none() {
                    plan.warnings.push(warn(
                        "ORPHAN_TOOL_RESULT",
                        "tool result has no verified link to a call",
                    ));
                    plan.partial.push("orphan_tool_result".into());
                }
            }
            Ok(())
        }
    }
}

/// Turn closure (spec 14.2.4.2): tool activity owned (by verified turn id or native call link) by
/// turns the selected messages touch, never by physical proximity.
fn with_tools_plan(
    snap: &Snap,
    tools: &Tools,
    leaf: &str,
    first: &str,
    last: &str,
    window: &[usize],
    plan: &mut Plan,
) -> Result<(), AppError> {
    let rpath = snap
        .rec_path_ext(leaf)
        .ok_or_else(|| stale("record path cannot be resolved"))?;
    let pos_of = |k: &str| {
        rpath
            .iter()
            .position(|i| snap.recs[*i].key == k)
            .ok_or_else(|| stale("message is not on the record path"))
    };
    let (first_pos, last_pos) = (pos_of(first)?, pos_of(last)?);
    let window_keys: HashSet<&str> = window.iter().map(|i| snap.msgs[*i].key.as_str()).collect();
    let on_path: HashSet<&str> = rpath.iter().map(|i| snap.recs[*i].key.as_str()).collect();
    let turns: HashSet<&str> = window
        .iter()
        .filter_map(|i| snap.rec(&snap.msgs[*i].key).and_then(|r| r.turn.as_deref()))
        .collect();
    let owned = |t: &Option<String>| t.as_deref().is_some_and(|t| turns.contains(t));

    let mut calls_by_rec: HashMap<&str, Vec<&TcRow>> = HashMap::new();
    for c in tools
        .calls
        .iter()
        .filter(|c| on_path.contains(c.rec.as_str()))
    {
        calls_by_rec.entry(c.rec.as_str()).or_default().push(c);
    }
    let mut results_by_rec: HashMap<&str, Vec<&TrRow>> = HashMap::new();
    for r in tools
        .results
        .iter()
        .filter(|r| on_path.contains(r.rec.as_str()))
    {
        results_by_rec.entry(r.rec.as_str()).or_default().push(r);
    }
    let call_on_path: HashSet<(&str, u32)> = tools
        .calls
        .iter()
        .filter(|c| on_path.contains(c.rec.as_str()))
        .map(|c| (c.rec.as_str(), c.block))
        .collect();
    let included_calls: HashSet<(&str, u32)> = tools
        .calls
        .iter()
        .filter(|c| on_path.contains(c.rec.as_str()) && owned(&c.turn))
        .map(|c| (c.rec.as_str(), c.block))
        .collect();
    // results linked to each call, in path order
    let mut results_for: HashMap<(&str, u32), Vec<(String, u32)>> = HashMap::new();
    for ri in &rpath {
        for r in results_by_rec
            .get(snap.recs[*ri].key.as_str())
            .into_iter()
            .flatten()
        {
            if let Some((ck, cb)) = &r.call {
                if call_on_path.contains(&(ck.as_str(), *cb)) {
                    results_for
                        .entry((ck.as_str(), *cb))
                        .or_default()
                        .push((r.rec.clone(), r.block));
                }
            }
        }
    }

    let (mut unattributed, mut orphans) = (0usize, 0usize);
    for (pos, ri) in rpath.iter().enumerate() {
        let key = snap.recs[*ri].key.as_str();
        let outside = pos < first_pos || pos > last_pos;
        let in_interval = !outside;
        let mut here: Vec<((u32, u8), PlanItem)> = Vec::new();
        if window_keys.contains(key) {
            let fb = tools.first_block.get(key).copied().unwrap_or(0);
            here.push((
                (fb, 0),
                PlanItem {
                    class: Class::Message,
                    rec: *ri,
                    block: None,
                    outside: false,
                    extra: Extra::None,
                },
            ));
        }
        for c in calls_by_rec.get(key).into_iter().flatten() {
            if included_calls.contains(&(key, c.block)) {
                let results = results_for
                    .get(&(key, c.block))
                    .cloned()
                    .unwrap_or_default();
                here.push((
                    (c.block, 1),
                    PlanItem {
                        class: Class::ToolCall,
                        rec: *ri,
                        block: Some(c.block),
                        outside,
                        extra: Extra::Call {
                            native: c.native.clone(),
                            name: c.name.clone(),
                            results,
                        },
                    },
                ));
            } else if c.turn.is_none() && in_interval {
                unattributed += 1;
            }
        }
        for r in results_by_rec.get(key).into_iter().flatten() {
            let linked = r
                .call
                .as_ref()
                .filter(|(ck, cb)| call_on_path.contains(&(ck.as_str(), *cb)));
            let include = match linked {
                Some((ck, cb)) => included_calls.contains(&(ck.as_str(), *cb)),
                None => owned(&r.turn),
            };
            if include {
                if linked.is_none() {
                    orphans += 1;
                }
                here.push((
                    (r.block, 2),
                    PlanItem {
                        class: Class::ToolResult,
                        rec: *ri,
                        block: Some(r.block),
                        outside,
                        extra: Extra::Result {
                            native: r.native.clone(),
                            call: linked.cloned(),
                            is_error: r.is_error,
                        },
                    },
                ));
            } else if linked.is_none() && r.turn.is_none() && in_interval {
                unattributed += 1;
            }
        }
        here.sort_by_key(|(k, _)| *k);
        plan.items.extend(here.into_iter().map(|(_, p)| p));
    }
    if orphans > 0 {
        plan.warnings.push(warn(
            "ORPHAN_TOOL_RESULT",
            &format!(
                "{orphans} tool result(s) have no verified link to a call on the selected path"
            ),
        ));
        plan.partial.push("orphan_tool_result".into());
    }
    if unattributed > 0 {
        plan.warnings.push(warn("TOOL_OWNERSHIP_UNKNOWN", &format!("{unattributed} tool record(s) inside the window have no verified turn ownership and were not included")));
        plan.partial.push("tool_ownership_unknown".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &str, parent: Option<&str>, kind: &str, turn: Option<&str>) -> Rec {
        Rec {
            key: key.into(),
            native_id: None,
            parent: parent.map(Into::into),
            kind: kind.into(),
            ts: None,
            offset: 0,
            len: 1,
            digest: [0; 32],
            turn: turn.map(Into::into),
        }
    }
    fn msg(key: &str, parent: Option<&str>, role: Role) -> Msg {
        Msg {
            key: key.into(),
            role,
            phase: None,
            ts: None,
            parent: parent.map(Into::into),
        }
    }

    /// u1 <- a1(cA,cB) <- r1(res cB) <- a2 <- o1(orphan) <- r2(res cA, async) <- u2 <- a3(c3, no result); a1 <- b1 <- sr(sibling result of cA)
    fn fixture() -> (Snap, Tools) {
        let recs = vec![
            rec("u1", None, "message", Some("T1")),
            rec("a1", Some("u1"), "message", Some("T1")),
            rec("r1", Some("a1"), "tool_result", Some("T1")),
            rec("a2", Some("r1"), "message", Some("T1")),
            rec("o1", Some("a2"), "tool_result", Some("T1")),
            rec("r2", Some("o1"), "tool_result", Some("T1")),
            rec("u2", Some("r2"), "message", Some("T2")),
            rec("a3", Some("u2"), "message", Some("T2")),
            rec("b1", Some("a1"), "message", Some("T1")),
            rec("sr", Some("b1"), "tool_result", Some("T1")),
        ];
        let msgs = vec![
            msg("u1", None, Role::User),
            msg("a1", Some("u1"), Role::Assistant),
            msg("a2", Some("a1"), Role::Assistant),
            msg("u2", Some("a2"), Role::User),
            msg("a3", Some("u2"), Role::Assistant),
            msg("b1", Some("a1"), Role::Assistant),
        ];
        let call = |rec: &str, block: u32, id: &str, turn: &str| TcRow {
            rec: rec.into(),
            block,
            native: Some(id.into()),
            name: "t".into(),
            turn: Some(turn.into()),
        };
        let res = |rec: &str, id: &str, call: Option<(&str, u32)>, turn: Option<&str>| TrRow {
            rec: rec.into(),
            block: 0,
            native: Some(id.into()),
            call: call.map(|(k, b)| (k.into(), b)),
            is_error: None,
            turn: turn.map(Into::into),
        };
        let tools = Tools {
            calls: vec![
                call("a1", 1, "cA", "T1"),
                call("a1", 2, "cB", "T1"),
                call("a3", 1, "c3", "T2"),
            ],
            results: vec![
                res("r1", "cB", Some(("a1", 2)), Some("T1")),
                res("o1", "zz", None, Some("T1")),
                res("r2", "cA", Some(("a1", 1)), Some("T1")),
                res("sr", "cA", Some(("a1", 1)), Some("T1")),
            ],
            first_block: [
                ("u1", 0),
                ("a1", 0),
                ("a2", 0),
                ("u2", 0),
                ("a3", 0),
                ("b1", 0),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        };
        (Snap::from_parts(recs, msgs), tools)
    }

    fn window(snap: &Snap, key: &str, before: u32, after: u32) -> ResolvedSel {
        resolve_window(
            snap,
            SelKind::Message,
            WindowInput {
                anchors: &[key.to_string()],
                before,
                after,
                branch_leaf: None,
            },
        )
        .expect("resolves")
    }

    #[test]
    fn turn_closure_uses_turn_ownership_and_marks_outside_window() {
        let (snap, tools) = fixture();
        let sel = window(&snap, "a2", 0, 0);
        let plan = build_plan(&snap, Some(&tools), &sel, View::WithTools).unwrap();
        let seq: Vec<(String, bool)> = plan
            .items
            .iter()
            .map(|p| (format!("{:?}:{}", p.class, snap.recs[p.rec].key), p.outside))
            .collect();
        assert_eq!(
            seq,
            vec![
                ("ToolCall:a1".into(), true),
                ("ToolCall:a1".into(), true),
                ("ToolResult:r1".into(), true),
                ("Message:a2".into(), false),
                ("ToolResult:o1".into(), true),
                ("ToolResult:r2".into(), true), // async result stays at its persisted position
            ]
        );
        // other turn's call and the sibling-branch result are never imported
        assert!(
            plan.items
                .iter()
                .all(|p| snap.recs[p.rec].key != "a3" && snap.recs[p.rec].key != "sr")
        );
        match &plan.items[0].extra {
            Extra::Call {
                native, results, ..
            } => {
                assert_eq!(native.as_deref(), Some("cA"));
                assert_eq!(
                    results,
                    &vec![("r2".to_string(), 0)],
                    "only on-path linked results"
                );
            }
            e => panic!("{e:?}"),
        }
        match &plan.items[4].extra {
            Extra::Result { call, .. } => assert!(call.is_none(), "orphan keeps null tool_ref"),
            e => panic!("{e:?}"),
        }
        assert!(plan.warnings.iter().any(|w| w.code == "ORPHAN_TOOL_RESULT"));
        assert!(plan.partial.contains(&"orphan_tool_result".to_string()));
    }

    #[test]
    fn absent_result_is_reported_without_claiming_state() {
        let (snap, tools) = fixture();
        let sel = window(&snap, "a3", 0, 0);
        let plan = build_plan(&snap, Some(&tools), &sel, View::WithTools).unwrap();
        let call = plan
            .items
            .iter()
            .find(|p| p.class == Class::ToolCall)
            .expect("c3 included");
        match &call.extra {
            Extra::Call { results, .. } => assert!(results.is_empty()),
            e => panic!("{e:?}"),
        }
        assert!(!call.outside);
    }

    #[test]
    fn calls_without_verified_turn_are_not_paired_by_proximity() {
        let (snap, mut tools) = fixture();
        tools.calls[2].turn = None; // c3 loses ownership
        let sel = window(&snap, "a3", 0, 0);
        let plan = build_plan(&snap, Some(&tools), &sel, View::WithTools).unwrap();
        assert!(plan.items.iter().all(|p| p.class != Class::ToolCall));
        assert!(plan.partial.contains(&"tool_ownership_unknown".to_string()));
    }

    #[test]
    fn branch_choice_ranges_and_context_counts() {
        let (snap, _) = fixture();
        match resolve_session(&snap, None) {
            Err(SelFail::Branch(l)) => assert_eq!(l.len(), 2),
            other => panic!("{other:?}"),
        }
        let branch = resolve_session(&snap, Some("a3")).unwrap();
        assert_eq!(branch.first.as_deref(), Some("u1"));
        assert_eq!(branch.last.as_deref(), Some("a3"));
        let code = |r: Result<ResolvedSel, SelFail>| match r {
            Err(SelFail::App(e)) => e.code,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            code(resolve_range(&snap, "a2", "a1", None)),
            "INVALID_RANGE",
            "reversed"
        );
        assert_eq!(
            code(resolve_range(&snap, "b1", "a2", None)),
            "INVALID_RANGE",
            "sibling branches"
        );
        let one = resolve_range(&snap, "a2", "a2", None).unwrap();
        assert_eq!(
            (one.first, one.last),
            (Some("a2".into()), Some("a2".into()))
        );
        // fork right after the anchor is ambiguous only when `after` needs it
        assert!(matches!(
            resolve_window(
                &snap,
                SelKind::Message,
                WindowInput {
                    anchors: &["a1".into()],
                    before: 0,
                    after: 1,
                    branch_leaf: None
                }
            ),
            Err(SelFail::Branch(_))
        ));
        let w = resolve_window(
            &snap,
            SelKind::Message,
            WindowInput {
                anchors: &["a1".into()],
                before: 5,
                after: 0,
                branch_leaf: None,
            },
        )
        .unwrap();
        assert_eq!(
            w.before,
            Some((5, 1)),
            "history boundary reports resolved count"
        );
        let plan = build_plan(&snap, None, &w, View::Conversation).unwrap();
        assert!(plan.warnings.iter().any(|w| w.code == "INCOMPLETE_CONTEXT"));
        assert_eq!(plan.items.len(), 2, "never fills from sibling branches");
    }
}
