//! Page assembly (spec 14.2.7-8): turns one frozen plan position into a bounded response page.
//!
//! * The page is measured with `mcp::budget::tool_result_bytes` (structured value + escaped text
//!   copy + wrapper), so `max_bytes` caps the real serialized tool result. A worst-case cursor
//!   placeholder is part of every measurement.
//! * Fragments advance without overlap over the full decoded block; the byte limit may stop inside
//!   a block, the item limit stops at an item boundary.
//! * A page that cannot make progress is `RESOURCE_LIMIT`, never an unchanged cursor.

use super::cursor::{CURSOR_FORMAT, CursorPayload, short_hash};
use super::fragment::{BlockData, longest_fit};
use super::handle::HandleKey;
use super::locator::{ResolveFail, SourceCtx, resource_limit, source_unavailable};
use super::refs::{Anchor, BranchRef, ItemKind, ItemRef, MessageRef};
use super::selection::{Class, Extra, Plan, PlanItem, ResolvedSel, Snap, app_err};
use super::types::*;
use crate::domain::{BlockKind, Fidelity, Role};
use crate::ipc::AppError;
use crate::mcp::budget::tool_result_bytes;
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;

pub struct PageEnv<'a> {
    pub session: SessionInfo,
    pub snap: &'a Snap,
    pub anchor: Anchor,
    pub lease_expires_ms: i64,
    pub key: HandleKey,
    pub view: View,
    pub policy: SourcePolicy,
    pub sel: &'a ResolvedSel,
    pub plan: &'a Plan,
    pub max_bytes: usize,
    pub max_items: usize,
    /// (plan item, block ordinal, byte offset)
    pub pos: (u32, u32, u64),
    pub pre_warnings: Vec<Warning>,
}

/// Signs the references a response hands out; all anchored to the read's lease + manifest.
pub struct RefMaker<'a> {
    pub key: &'a HandleKey,
    pub anchor: &'a Anchor,
    pub snap: &'a Snap,
}

fn digest16(d: &[u8; 32]) -> String {
    crate::ids::hex(d)[..32].to_string()
}

impl RefMaker<'_> {
    pub fn message(&self, rec_key: &str) -> Option<String> {
        let r = self.snap.rec(rec_key)?;
        Some(
            MessageRef {
                anchor: self.anchor.clone(),
                record: rec_key.to_string(),
                digest: digest16(&r.digest),
            }
            .encode(self.key),
        )
    }

    pub fn item(&self, kind: ItemKind, rec_key: &str, block: Option<u32>) -> Option<String> {
        let r = self.snap.rec(rec_key)?;
        Some(
            ItemRef {
                anchor: self.anchor.clone(),
                kind,
                record: rec_key.to_string(),
                block,
                digest: digest16(&r.digest),
            }
            .encode(self.key),
        )
    }

    pub fn branch(&self, leaf: &str) -> String {
        BranchRef {
            anchor: self.anchor.clone(),
            leaf: leaf.to_string(),
        }
        .encode(self.key)
    }
}

pub fn record_digest_hex(d: &[u8; 32]) -> String {
    digest16(d)
}

fn map_fidelity(f: Fidelity) -> FidelityOut {
    match f {
        Fidelity::Verbatim => FidelityOut::Verbatim,
        Fidelity::NormalizedJson => FidelityOut::NormalizedJson,
        Fidelity::Metadata => FidelityOut::Metadata,
    }
}

fn block_ref(snap: &str, rec: &str, kind: &str, idx: u32) -> String {
    format!(
        "blk_{}",
        short_hash(&format!("{snap}\0{rec}\0{kind}\0{idx}"))
    )
}

fn storage_err(_: sqlx::Error) -> AppError {
    AppError::new(
        "STORAGE_ERROR",
        "required local state cannot be accessed safely",
    )
}

struct Loader<'a, 'b> {
    snap: &'a Snap,
    pool: &'a SqlitePool,
    src: &'b mut SourceCtx,
    policy: SourcePolicy,
    warnings: Vec<Warning>,
    fell_back: bool,
}

impl Loader<'_, '_> {
    fn note_fallback(&mut self) {
        if !self.fell_back {
            self.fell_back = true;
            self.warnings.push(Warning {
                code: "SOURCE_FALLBACK".into(),
                message: "the original source could not be read; affected items are served from the indexed snapshot (see per-item provenance.origin)".into(),
            });
        }
    }

    async fn resolve(
        &mut self,
        rec: &super::selection::Rec,
    ) -> Result<std::sync::Arc<crate::domain::ResolvedRecord>, ResolveFail> {
        self.src
            .resolve(
                &rec.key,
                crate::domain::SourceLocator {
                    offset: rec.offset,
                    len: rec.len,
                },
                rec.digest,
            )
            .await
    }

    async fn message_blocks(
        &mut self,
        rec: &super::selection::Rec,
    ) -> Result<(Vec<BlockData>, Origin), AppError> {
        let snap_id = self.snap.id.clone();
        let mut texts: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut origin = Origin::VerifiedSource;
        let mut from_source = false;
        if self.policy != SourcePolicy::IndexedOnly {
            match self.resolve(rec).await {
                Ok(rr) => {
                    from_source = true;
                    for b in &rr.blocks {
                        if b.kind == BlockKind::Text {
                            texts.insert(b.index, b.bytes.clone());
                        }
                    }
                }
                Err(ResolveFail::Fatal(e)) => return Err(e),
                Err(ResolveFail::Unavailable) if self.policy == SourcePolicy::PreferSource => {
                    self.note_fallback()
                }
                Err(ResolveFail::Unsupported) if self.policy == SourcePolicy::PreferSource => {
                    self.note_fallback()
                }
                Err(ResolveFail::Unavailable) => return Err(source_unavailable()),
                Err(ResolveFail::Unsupported) => return Err(unsupported_view()),
            }
        }
        // verified-source reads only need the retained block indexes; the indexed text is loaded
        // only when it is what gets served (indexed_only / fallback)
        let rows = sqlx::query(if from_source { "SELECT idx, NULL AS text FROM blocks WHERE snapshot_id = ? AND record_key = ? ORDER BY idx" } else { "SELECT idx, text FROM blocks WHERE snapshot_id = ? AND record_key = ? ORDER BY idx" })
            .bind(&snap_id)
            .bind(&rec.key)
            .fetch_all(self.pool)
            .await
            .map_err(storage_err)?;
        let mut blocks: Vec<(u32, BlockData)> = Vec::new();
        for r in rows {
            let idx = r.get::<i64, _>("idx") as u32;
            let bytes = if from_source {
                texts.remove(&idx).ok_or_else(|| {
                    app_err(
                        "STALE_REFERENCE",
                        "a retained block is missing from the verified source record",
                        false,
                        "new_read",
                    )
                })?
            } else {
                origin = Origin::IndexedSnapshot;
                r.get::<String, _>("text").into_bytes()
            };
            blocks.push((
                idx,
                BlockData::new(
                    block_ref(&snap_id, &rec.key, "text", idx),
                    BlockClass::Text,
                    FidelityOut::Verbatim,
                    bytes,
                ),
            ));
        }
        let atts = sqlx::query("SELECT idx, kind, mime, bytes FROM attachments WHERE snapshot_id = ? AND record_key = ? ORDER BY idx")
            .bind(&snap_id)
            .bind(&rec.key)
            .fetch_all(self.pool)
            .await
            .map_err(storage_err)?;
        for a in atts {
            let idx = a.get::<i64, _>("idx") as u32;
            let data = serde_json::json!({"kind": a.get::<String, _>("kind"), "mime": a.get::<Option<String>, _>("mime"), "bytes": a.get::<Option<i64>, _>("bytes")});
            blocks.push((
                idx,
                BlockData::new(
                    block_ref(&snap_id, &rec.key, "attachment", idx),
                    BlockClass::Attachment,
                    FidelityOut::Metadata,
                    data.to_string().into_bytes(),
                ),
            ));
        }
        blocks.sort_by_key(|(i, _)| *i);
        Ok((blocks.into_iter().map(|(_, b)| b).collect(), origin))
    }

    async fn source_only(
        &mut self,
        rec: &super::selection::Rec,
    ) -> Result<std::sync::Arc<crate::domain::ResolvedRecord>, AppError> {
        match self.resolve(rec).await {
            Ok(r) => Ok(r),
            Err(ResolveFail::Fatal(e)) => Err(e),
            Err(ResolveFail::Unavailable) => Err(source_unavailable()),
            Err(ResolveFail::Unsupported) => Err(unsupported_view()),
        }
    }

    async fn blocks_for(&mut self, item: &PlanItem) -> Result<(Vec<BlockData>, Origin), AppError> {
        let snap = self.snap;
        let rec = &snap.recs[item.rec];
        match item.class {
            Class::Message => self.message_blocks(rec).await,
            Class::Raw => {
                let rr = self.source_only(rec).await?;
                Ok((
                    vec![BlockData::new(
                        block_ref(&snap.id, &rec.key, "raw", 0),
                        BlockClass::RawRecord,
                        FidelityOut::Verbatim,
                        rr.raw.clone(),
                    )],
                    Origin::VerifiedSource,
                ))
            }
            Class::ToolCall | Class::ToolResult => {
                let idx = item.block.unwrap_or(0);
                let (want, class, tag) = if item.class == Class::ToolCall {
                    (BlockKind::ToolCall, BlockClass::ToolArguments, "args")
                } else {
                    (BlockKind::ToolResult, BlockClass::ToolResult, "result")
                };
                let rr = self.source_only(rec).await?;
                let b = rr
                    .blocks
                    .iter()
                    .find(|b| b.index == idx && b.kind == want)
                    .ok_or_else(|| {
                        app_err(
                            "STALE_REFERENCE",
                            "the tool block is missing from the verified source record",
                            false,
                            "new_read",
                        )
                    })?;
                Ok((
                    vec![BlockData::new(
                        block_ref(&snap.id, &rec.key, tag, idx),
                        class,
                        map_fidelity(b.fidelity),
                        b.bytes.clone(),
                    )],
                    Origin::VerifiedSource,
                ))
            }
        }
    }
}

fn unsupported_view() -> AppError {
    app_err(
        "VIEW_NOT_SUPPORTED",
        "the adapter cannot return byte-exact content for this representation",
        false,
        "change_request",
    )
}

/// Everything about the response that does not depend on which fragments fit.
struct Fixed {
    session: SessionInfo,
    snapshot: SnapshotInfo,
    view: View,
    policy: SourcePolicy,
    selection: SelectionOut,
    navigation: Navigation,
    coverage: CoverageOut,
    warnings: Vec<Warning>,
}

impl Fixed {
    fn output(&self, items: Vec<Item>, page: PageInfo, extra: &[Warning]) -> ReadSessionOutput {
        let mut warnings = self.warnings.clone();
        warnings.extend_from_slice(extra);
        if warnings.len() > WARNINGS_MAX {
            warnings.truncate(WARNINGS_MAX - 1);
            warnings.push(Warning {
                code: "WARNINGS_TRUNCATED".into(),
                message: "further notices were omitted".into(),
            });
        }
        ReadSessionOutput {
            session: self.session.clone(),
            snapshot: self.snapshot.clone(),
            view: self.view,
            source_policy: self.policy,
            selection: self.selection.clone(),
            items,
            page,
            navigation: self.navigation.clone(),
            coverage: self.coverage.clone(),
            warnings,
        }
    }
}

fn fits(
    fixed: &Fixed,
    items: &mut Vec<Item>,
    extra: &[Warning],
    placeholder: &str,
    max: usize,
) -> bool {
    let page = PageInfo {
        next_cursor: Some(placeholder.to_string()),
        selection_exhausted: false,
        stop_reason: StopReason::ByteLimit,
    };
    let out = fixed.output(std::mem::take(items), page, extra);
    let v = serde_json::to_value(&out).unwrap_or(Value::Null);
    *items = out.items;
    tool_result_bytes(&v, false) <= max
}

fn rl_too_small(max: usize) -> AppError {
    let mut e = resource_limit(
        "max_bytes",
        &format!(
            "max_bytes={max} cannot hold the next item's envelope plus one fragment; raise max_bytes"
        ),
        "increase_max_bytes",
    );
    e.retryable = false;
    e
}

/// Build one page. Errors are final (nothing partial is returned).
pub async fn assemble(
    env: &PageEnv<'_>,
    src: &mut SourceCtx,
    pool: &SqlitePool,
    source_ref: &str,
) -> Result<Value, AppError> {
    let snap = env.snap;
    let rm = RefMaker {
        key: &env.key,
        anchor: &env.anchor,
        snap,
    };
    let sel_out = selection_out(env, &rm)?;
    let ref_of = |i: Option<usize>| i.and_then(|i| rm.message(&snap.msgs[i].key));
    let mut partial = env.plan.partial.clone();
    partial.sort();
    partial.dedup();
    let fixed = Fixed {
        session: env.session.clone(),
        snapshot: SnapshotInfo {
            snapshot_id: snap.id.clone(),
            kind: snap.kind,
            observed_at: iso(snap.created_at),
            lease_expires_at: iso(env.lease_expires_ms),
        },
        view: env.view,
        policy: env.policy,
        selection: sel_out,
        navigation: Navigation {
            before_message_ref: ref_of(env.plan.nav_before),
            after_message_ref: ref_of(env.plan.nav_after),
        },
        coverage: CoverageOut {
            status: if partial.is_empty() {
                CoverageStatus::Complete
            } else {
                CoverageStatus::Partial
            },
            reasons: partial,
        },
        warnings: {
            let mut w = env.pre_warnings.clone();
            w.extend(env.plan.warnings.iter().cloned());
            w
        },
    };
    let template = CursorPayload {
        f: CURSOR_FORMAT,
        a: env.anchor.clone(),
        x: env.lease_expires_ms,
        v: env.view,
        p: env.policy,
        s: env.sel.clone(),
        i: 0,
        b: 0,
        o: 0,
        n: short_hash(""),
    };
    let placeholder = template.worst_case().encode(&env.key);

    let mut loader = Loader {
        snap,
        pool,
        src,
        policy: env.policy,
        warnings: Vec::new(),
        fell_back: false,
    };
    let mut items: Vec<Item> = Vec::new();
    let mut stop: Option<(StopReason, (u32, u32, u64))> = None;
    let plan_len = env.plan.items.len();
    let mut idx = env.pos.0 as usize;
    if idx > plan_len {
        return Err(app_err(
            "STALE_REFERENCE",
            "cursor position is outside the selection",
            false,
            "new_read",
        ));
    }
    while idx < plan_len {
        if items.len() >= env.max_items {
            stop = Some((StopReason::ItemLimit, (idx as u32, 0, 0)));
            break;
        }
        let item = &env.plan.items[idx];
        let (start_b, start_o) = if idx as u32 == env.pos.0 {
            (env.pos.1, env.pos.2)
        } else {
            (0, 0)
        };
        loader.src.check_time()?;
        let warn_mark = loader.warnings.len();
        let (blocks, origin) = loader.blocks_for(item).await?;
        if start_b as usize > blocks.len()
            || (start_b as usize == blocks.len() && !blocks.is_empty())
        {
            return Err(app_err(
                "STALE_REFERENCE",
                "cursor block position is outside the item",
                false,
                "new_read",
            ));
        }
        items.push(envelope(env, &rm, item, origin, source_ref)?);
        if !fits(
            &fixed,
            &mut items,
            &loader.warnings,
            &placeholder,
            env.max_bytes,
        ) {
            items.pop();
            loader.warnings.truncate(warn_mark);
            if items.is_empty() {
                return Err(rl_too_small(env.max_bytes));
            }
            stop = Some((StopReason::ByteLimit, (idx as u32, start_b, start_o)));
            break;
        }
        let (mut cur_b, mut cur_o) = (start_b as usize, start_o as usize);
        let mut emitted = false;
        let mut item_done = true;
        while cur_b < blocks.len() {
            let blk = &blocks[cur_b];
            let total = blk.total();
            if cur_o > total || (cur_o == total && total > 0) {
                return Err(app_err(
                    "STALE_REFERENCE",
                    "cursor byte position is outside the block",
                    false,
                    "new_read",
                ));
            }
            let end = if total == 0 {
                let f = blk.fragment(0, 0);
                items.last_mut().unwrap().common_mut().blocks.push(f);
                if fits(
                    &fixed,
                    &mut items,
                    &loader.warnings,
                    &placeholder,
                    env.max_bytes,
                ) {
                    Some(0)
                } else {
                    items.last_mut().unwrap().common_mut().blocks.pop();
                    None
                }
            } else {
                let warnings = &loader.warnings;
                let r = longest_fit(blk, cur_o, env.max_bytes, |e| {
                    items
                        .last_mut()
                        .unwrap()
                        .common_mut()
                        .blocks
                        .push(blk.fragment(cur_o, e));
                    let ok = fits(&fixed, &mut items, warnings, &placeholder, env.max_bytes);
                    items.last_mut().unwrap().common_mut().blocks.pop();
                    ok
                });
                if let Some(e) = r {
                    items
                        .last_mut()
                        .unwrap()
                        .common_mut()
                        .blocks
                        .push(blk.fragment(cur_o, e));
                }
                r
            };
            match end {
                None => {
                    item_done = false;
                    if !emitted {
                        items.pop();
                        loader.warnings.truncate(warn_mark);
                        if items.is_empty() {
                            return Err(rl_too_small(env.max_bytes));
                        }
                    }
                    stop = Some((
                        StopReason::ByteLimit,
                        (idx as u32, cur_b as u32, cur_o as u64),
                    ));
                    break;
                }
                Some(e) => {
                    emitted = true;
                    if e >= total {
                        cur_b += 1;
                        cur_o = 0;
                    } else {
                        item_done = false;
                        stop = Some((StopReason::ByteLimit, (idx as u32, cur_b as u32, e as u64)));
                        break;
                    }
                }
            }
        }
        if item_done {
            if let Some(last) = items.last_mut() {
                last.common_mut().item_end = true;
            }
            idx += 1;
        } else {
            if emitted {
                // partial item stays on the page; it is not the end of the item
                items.last_mut().unwrap().common_mut().item_end = false;
            }
            break;
        }
    }
    let page = match stop {
        None => PageInfo {
            next_cursor: None,
            selection_exhausted: true,
            stop_reason: StopReason::SelectionEnd,
        },
        Some((reason, (i, b, o))) => {
            let nk = env
                .plan
                .items
                .get(i as usize)
                .map(|p| short_hash(&p.key(snap)))
                .unwrap_or_else(|| short_hash(""));
            let c = CursorPayload {
                i,
                b,
                o,
                n: nk,
                ..template.clone()
            };
            PageInfo {
                next_cursor: Some(c.encode(&env.key)),
                selection_exhausted: false,
                stop_reason: reason,
            }
        }
    };
    let out = fixed.output(items, page, &loader.warnings);
    let v = serde_json::to_value(&out)
        .map_err(|_| AppError::new("STORAGE_ERROR", "response cannot be serialized"))?;
    if tool_result_bytes(&v, false) > env.max_bytes {
        return Err(rl_too_small(env.max_bytes));
    }
    Ok(v)
}

fn selection_out(env: &PageEnv<'_>, rm: &RefMaker<'_>) -> Result<SelectionOut, AppError> {
    let sel = env.sel;
    let msg = |k: &Option<String>| k.as_deref().and_then(|k| rm.message(k));
    let item_ref = sel.item.as_ref().and_then(|i| rm.item(i.k, &i.r, i.b));
    let count = |c: Option<(u32, u32)>| {
        c.map(|(requested, resolved)| ContextCount {
            requested,
            resolved,
        })
    };
    Ok(SelectionOut {
        kind: sel.kind,
        branch_ref: sel.leaf.as_deref().map(|l| rm.branch(l)),
        from_message_ref: msg(&sel.first),
        through_message_ref: msg(&sel.last),
        anchor_message_refs: sel.anchors.iter().filter_map(|a| rm.message(a)).collect(),
        item_ref,
        before: count(sel.before),
        after: count(sel.after),
    })
}

fn envelope(
    env: &PageEnv<'_>,
    rm: &RefMaker<'_>,
    item: &PlanItem,
    origin: Origin,
    source_ref: &str,
) -> Result<Item, AppError> {
    let snap = env.snap;
    let rec = &snap.recs[item.rec];
    let bad = || {
        app_err(
            "STALE_REFERENCE",
            "item reference cannot be constructed",
            false,
            "new_read",
        )
    };
    let kind = match item.class {
        Class::Message => ItemKind::Message,
        Class::ToolCall => ItemKind::ToolCall,
        Class::ToolResult => ItemKind::ToolResult,
        Class::Raw => ItemKind::RawRecord,
    };
    let item_ref = rm.item(kind, &rec.key, item.block).ok_or_else(bad)?;
    let msg = snap.msg_ix.get(&rec.key).map(|i| &snap.msgs[*i]);
    let ts = if item.class == Class::Message {
        msg.and_then(|m| m.ts).or(rec.ts)
    } else {
        rec.ts
    };
    let common = ItemCommon {
        item_ref: item_ref.clone(),
        timestamp: ts.map(iso),
        turn_ref: rec.turn.clone(),
        blocks: Vec::new(),
        item_end: false,
        provenance: Provenance {
            origin,
            source_ref: source_ref.to_string(),
            record_ref: format!("rec_{}", short_hash(&format!("{}\0{}", snap.id, rec.key))),
            record_digest: record_digest_hex(&rec.digest),
            native_record_id: rec.native_id.clone(),
            locator: Some(Locator {
                offset: rec.offset,
                len: rec.len,
            }),
        },
    };
    Ok(match (&item.class, &item.extra) {
        (Class::Message, _) => {
            let m = msg.ok_or_else(bad)?;
            Item::Message(MessageItem {
                message_ref: rm.message(&rec.key).ok_or_else(bad)?,
                role: if m.role == Role::User {
                    RoleOut::User
                } else {
                    RoleOut::Assistant
                },
                phase: m.phase.clone(),
                common,
            })
        }
        (
            Class::ToolCall,
            Extra::Call {
                native,
                name,
                results,
            },
        ) => Item::ToolCall(ToolCallItem {
            tool_ref: item_ref,
            native_call_id: native.clone(),
            tool_name: name.clone(),
            result_refs: results
                .iter()
                .filter_map(|(k, b)| rm.item(ItemKind::ToolResult, k, Some(*b)))
                .collect(),
            result_state: if results.is_empty() {
                ResultState::NoResultInSnapshot
            } else {
                ResultState::ResultAvailable
            },
            outside_message_window: item.outside,
            common,
        }),
        (
            Class::ToolResult,
            Extra::Result {
                native,
                call,
                is_error,
            },
        ) => Item::ToolResult(ToolResultItem {
            tool_ref: call
                .as_ref()
                .and_then(|(k, b)| rm.item(ItemKind::ToolCall, k, Some(*b))),
            native_call_id: native.clone(),
            is_error: *is_error,
            outside_message_window: item.outside,
            common,
        }),
        (Class::Raw, _) => Item::RawRecord(RawRecordItem {
            record_kind: rec.kind.clone(),
            common,
        }),
        _ => return Err(bad()),
    })
}
