use super::{Content, Line, Payload};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{Emission, LineParser, Occurrences, ParseCx, RawRecord, SessionFacts};
use crate::domain::{
    BlockOrigin, Coverage, Digest, MessageEntry, RecordKind, RecordMeta, Role, SessionKind,
    TextBlock, ToolCallMeta, ToolResultMeta, parse_timestamp_ms,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn canonical_response_item(ty: &str) -> bool {
    matches!(
        ty,
        "additional_tools"
            | "message"
            | "agent_message"
            | "reasoning"
            | "local_shell_call"
            | "function_call"
            | "tool_search_call"
            | "function_call_output"
            | "custom_tool_call"
            | "custom_tool_call_output"
            | "tool_search_output"
            | "web_search_call"
            | "image_generation_call"
            | "compaction"
            | "configuration_update"
            | "compaction_trigger"
            | "context_compaction"
            | "other"
    )
}

fn session_facts(p: Payload) -> SessionFacts {
    let kind = match p.source.as_ref() {
        Some(Value::Object(source)) if source.contains_key("subagent") => SessionKind::Subagent,
        Some(Value::String(source))
            if matches!(source.as_str(), "cli" | "vscode" | "exec" | "mcp") =>
        {
            SessionKind::Interactive
        }
        _ => SessionKind::Unknown,
    };
    let parent_native_session = p.parent_thread_id.or(p.forked_from_id).or_else(|| {
        p.source
            .as_ref()
            .and_then(|source| source.pointer("/subagent/thread_spawn/parent_thread_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    });

    SessionFacts {
        native_id: p.thread_id.or(p.conversation_id).or(p.id),
        title: None,
        cwd: p.cwd,
        parent_native_session,
        kind,
    }
}

struct Pending {
    meta: RecordMeta,
    out: Emission,
}

#[derive(Default)]
pub(crate) struct ParseState {
    // Codex's pinned rollout item schema has no per-item parent field; persisted record order is linear.
    previous_record_key: Option<String>,
    occurrences: Occurrences,
    /// Message identities already retained, shared by `response_item` and `event_msg` aliases.
    seen: HashSet<String>,
    calls_by_turn: HashMap<(String, String), Option<(String, u32)>>,
}

impl LineParser for ParseState {
    type Line = Line;

    fn record(
        &mut self,
        cx: &mut ParseCx<'_>,
        line: Line,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError> {
        let Line {
            timestamp,
            ordinal,
            ty,
            turn_id,
            payload,
            ..
        } = line;
        let timestamp = timestamp.as_deref().and_then(parse_timestamp_ms);
        observe_timestamp(cx, timestamp, ty.as_deref());

        let payload = payload.unwrap_or_default();
        let native_id = payload.id.clone();
        let key = self.record_key(cx, native_id.as_deref(), ordinal, &raw.digest);
        let mut rec = Pending {
            meta: RecordMeta {
                key,
                native_id,
                parent_key: self.previous_record_key.take(),
                kind: RecordKind::Control,
                timestamp,
                turn_id: record_turn(ty.as_deref(), turn_id, &payload),
                locator: raw.locator,
                digest: raw.digest,
            },
            out: Emission::default(),
        };

        match ty.as_deref() {
            Some("session_meta") => apply_session_meta(cx, payload),
            Some("response_item") => self.classify_response_item(cx, &mut rec, payload),
            Some("event_msg") => self.classify_event_msg(cx, &mut rec, payload),
            _ => mark_unknown(cx, &mut rec.meta),
        }

        self.publish(cx, rec)
    }

    fn malformed_parent(&mut self, key: &str) -> Option<String> {
        let parent = self.previous_record_key.take();
        self.previous_record_key = Some(key.to_owned());
        parent
    }
}

fn observe_timestamp(cx: &mut ParseCx<'_>, timestamp: Option<i64>, ty: Option<&str>) {
    cx.observe_activity(timestamp);
    if ty == Some("session_meta")
        && let Some(timestamp) = timestamp
    {
        cx.started_at.get_or_insert(timestamp);
    }
}

fn apply_session_meta(cx: &mut ParseCx<'_>, p: Payload) {
    if p.history_mode
        .as_deref()
        .is_some_and(|mode| !matches!(mode, "legacy" | "paginated"))
    {
        cx.coverage = Coverage::Unsupported;
    }
    cx.session = session_facts(p);
}

fn mark_unknown(cx: &mut ParseCx<'_>, meta: &mut RecordMeta) {
    meta.kind = RecordKind::Unknown;
    cx.counts.unknown_records += 1;
    cx.coverage = Coverage::Partial;
}

/// Canonical item types are expected Control records, so they are not counted as unknown;
/// anything outside the allowlist degrades coverage.
fn reconcile_canonical(cx: &mut ParseCx<'_>, meta: &mut RecordMeta, ty: &str) {
    if !canonical_response_item(ty) {
        meta.kind = RecordKind::Unknown;
        cx.coverage = Coverage::Partial;
    } else if meta.kind == RecordKind::Control {
        cx.counts.unknown_records = cx.counts.unknown_records.saturating_sub(1);
    }
}

fn text_blocks(cx: &mut ParseCx<'_>, content: Option<Vec<Content>>) -> Vec<TextBlock> {
    let mut blocks = Vec::new();
    for (i, c) in content.unwrap_or_default().into_iter().enumerate() {
        if !matches!(c.ty.as_deref(), Some("input_text" | "output_text" | "text")) {
            cx.counts.unknown_blocks += 1;
        } else if let Some(text) = c.text {
            blocks.push(TextBlock {
                index: i as u32,
                origin: BlockOrigin::Authored,
                text,
            });
        }
    }
    blocks
}

fn retain_message(
    cx: &mut ParseCx<'_>,
    rec: &mut Pending,
    role: Role,
    phase: Option<String>,
    blocks: Vec<TextBlock>,
) {
    cx.count_message(blocks.len());
    rec.out.message = Some(MessageEntry {
        record_key: rec.meta.key.clone(),
        role,
        phase,
        blocks,
        attachments: Vec::new(),
        supersedes: vec![],
    });
}

fn tool_call(rec: &mut Pending, call_id: Option<String>, name: Option<String>) {
    rec.meta.kind = RecordKind::ToolCall;
    rec.out.calls.push(ToolCallMeta {
        record_key: rec.meta.key.clone(),
        block_index: 0,
        native_call_id: call_id,
        tool_name: name.unwrap_or_else(|| "unknown".into()),
        turn_id: rec.meta.turn_id.clone(),
    });
}

impl ParseState {
    /// Native identity (or line ordinal) with a `~n` suffix for repeats; digest fallback when neither exists.
    fn record_key(
        &mut self,
        cx: &ParseCx<'_>,
        native_id: Option<&str>,
        ordinal: Option<u64>,
        d: &Digest,
    ) -> String {
        let identity = native_id
            .map(str::to_owned)
            .or_else(|| ordinal.map(|ordinal| ordinal.to_string()));
        match identity {
            Some(identity) => self.occurrences.key(&identity),
            None => cx.fallback_key(d, cx.counts.records),
        }
    }

    fn classify_response_item(&mut self, cx: &mut ParseCx<'_>, rec: &mut Pending, p: Payload) {
        let Payload {
            ty,
            role,
            phase,
            content,
            call_id,
            name,
            is_error,
            ..
        } = p;

        match ty.as_deref() {
            Some("message") => self.response_message(cx, rec, role.as_deref(), phase, content),
            Some("compaction" | "context_compaction") => rec.meta.kind = RecordKind::Summary,
            Some("function_call") => tool_call(rec, call_id, name),
            Some("function_call_output") => self.tool_result(rec, call_id, is_error),
            Some("local_shell_call" | "custom_tool_call") => rec.meta.kind = RecordKind::ToolCall,
            Some("custom_tool_call_output") => rec.meta.kind = RecordKind::ToolResult,
            Some(_) => {
                rec.meta.kind = RecordKind::Control;
                cx.counts.unknown_records += 1;
            }
            None => mark_unknown(cx, &mut rec.meta),
        }

        if let Some(ty) = ty.as_deref() {
            reconcile_canonical(cx, &mut rec.meta, ty);
        }
    }

    fn response_message(
        &mut self,
        cx: &mut ParseCx<'_>,
        rec: &mut Pending,
        role: Option<&str>,
        phase: Option<String>,
        content: Option<Vec<Content>>,
    ) {
        let role = match role {
            Some("user") => Role::User,
            Some("assistant") => Role::Assistant,
            _ => return,
        };
        let blocks = text_blocks(cx, content);
        if blocks.is_empty() {
            return;
        }

        rec.meta.kind = RecordKind::Message;
        if self.claim_message(&rec.meta) {
            retain_message(cx, rec, role, phase, blocks);
        }
    }

    /// First claimant of a native identity owns the message; `event_msg` aliases of a retained
    /// `response_item` (or the reverse) publish only their record.
    fn claim_message(&mut self, meta: &RecordMeta) -> bool {
        let identity = meta.native_id.clone().unwrap_or_else(|| meta.key.clone());
        self.seen.insert(identity)
    }

    fn tool_result(&self, rec: &mut Pending, call_id: Option<String>, is_error: Option<bool>) {
        rec.meta.kind = RecordKind::ToolResult;
        let call = rec
            .meta
            .turn_id
            .as_ref()
            .zip(call_id.as_ref())
            .and_then(|(turn, call_id)| {
                self.calls_by_turn
                    .get(&(turn.clone(), call_id.clone()))
                    .and_then(Clone::clone)
            });
        rec.out.results.push(ToolResultMeta {
            record_key: rec.meta.key.clone(),
            block_index: 0,
            native_call_id: call_id,
            call,
            is_error,
            turn_id: rec.meta.turn_id.clone(),
        });
    }

    fn classify_event_msg(&mut self, cx: &mut ParseCx<'_>, rec: &mut Pending, p: Payload) {
        let Payload {
            ty, message, text, ..
        } = p;
        let role = match ty.as_deref() {
            Some("UserMessage") => Role::User,
            Some("AgentMessage") => Role::Assistant,
            _ => return,
        };
        let Some(text) = message.or(text).filter(|t| !t.is_empty()) else {
            mark_unknown(cx, &mut rec.meta);
            return;
        };
        if !self.claim_message(&rec.meta) {
            return;
        }

        rec.meta.kind = RecordKind::Message;
        let blocks = vec![TextBlock {
            index: 0,
            origin: BlockOrigin::Authored,
            text,
        }];
        retain_message(cx, rec, role, None, blocks);
    }

    /// Links a tool call to its turn and native call id; a repeated pair from another record is ambiguous.
    fn link_calls(&mut self, calls: &[ToolCallMeta]) {
        for call in calls {
            let (Some(turn_id), Some(call_id)) =
                (call.turn_id.as_ref(), call.native_call_id.as_ref())
            else {
                continue;
            };
            let link = self
                .calls_by_turn
                .entry((turn_id.clone(), call_id.clone()))
                .or_insert_with(|| Some((call.record_key.clone(), call.block_index)));
            if link
                .as_ref()
                .is_some_and(|(key, _)| key != &call.record_key)
            {
                *link = None;
            }
        }
    }

    fn publish(&mut self, cx: &mut ParseCx<'_>, rec: Pending) -> Result<(), AdapterError> {
        self.link_calls(&rec.out.calls);
        self.previous_record_key = Some(rec.meta.key.clone());

        cx.emit(rec.meta, rec.out)
    }
}

fn record_turn(ty: Option<&str>, line_turn: Option<String>, p: &Payload) -> Option<String> {
    let scoped = matches!(
        p.ty.as_deref(),
        Some("message" | "function_call" | "function_call_output")
    ) || ty == Some("event_msg");
    if !scoped {
        return None;
    }

    line_turn.or_else(|| p.turn_id.clone()).or_else(|| {
        p.turn_metadata
            .as_ref()
            .and_then(|metadata| metadata.turn_id.clone())
    })
}
