use super::{Block, Content, Line};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{Emission, LineParser, Occurrences, ParseCx, RawRecord};
use crate::adapters::lineage::RecordLineage;
use crate::domain::{
    BlockOrigin, Coverage, Digest, MessageEntry, RecordKind, RecordMeta, Role, SessionKind,
    TextBlock, ToolCallMeta, ToolResultMeta, parse_timestamp_ms,
};
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct ParseState {
    occurrences: Occurrences,
    message_keys: HashMap<String, Vec<String>>,
    latest_blocks: HashMap<String, usize>,
    latest_message_parent: HashMap<String, Option<String>>,
    lineage: RecordLineage,
}

/// What one record projects to, plus the native message identity used for supersession.
struct Projection {
    identity: Option<String>,
    emission: Emission,
}

impl LineParser for ParseState {
    type Line = Line;

    fn record(
        &mut self,
        cx: &mut ParseCx<'_>,
        mut x: Line,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError> {
        let timestamp = x.timestamp.as_deref().and_then(parse_timestamp_ms);
        observe_timestamp(cx, timestamp);
        let key = self.record_key(cx, x.uuid.as_deref(), &raw.digest);
        observe_session(cx, &x);

        let mut meta = RecordMeta {
            key,
            native_id: x.uuid.clone(),
            parent_key: x.parent.clone(),
            kind: RecordKind::Control,
            timestamp,
            turn_id: None,
            locator: raw.locator,
            digest: raw.digest,
        };
        meta.turn_id = self.record_turn(cx, &x, &meta.key);

        let projection = self.classify(cx, &mut x, &mut meta);
        // Indexing must see the physical parent, so it precedes supersession's parent inheritance.
        self.lineage.index(&meta, &projection.emission.calls);

        self.publish(cx, meta, projection)
    }
}

fn observe_timestamp(cx: &mut ParseCx<'_>, timestamp: Option<i64>) {
    cx.observe_activity(timestamp);
    if let Some(timestamp) = timestamp {
        cx.started_at.get_or_insert(timestamp);
    }
}

fn observe_session(cx: &mut ParseCx<'_>, x: &Line) {
    if cx.session.native_id.is_none() {
        cx.session.native_id = x.session_id.clone();
        cx.session.cwd = x.cwd.clone();
        cx.session.parent_native_session = x.parent_session.clone();
    }
    if x.parent_session.is_some()
        || (cx.session.native_id.is_none() && x.is_sidechain == Some(true))
    {
        cx.session.kind = SessionKind::Subagent;
    } else if x.session_id.is_some() && cx.session.kind == SessionKind::Unknown {
        cx.session.kind = SessionKind::Interactive;
    }
}

fn infer_result_turn(meta: &mut RecordMeta, results: &[ToolResultMeta]) {
    let mut turns = results
        .iter()
        .filter_map(|result| result.turn_id.as_deref());
    if let Some(first) = turns.next()
        && turns.all(|turn| turn == first)
    {
        meta.turn_id = Some(first.to_owned());
    }
}

impl ParseState {
    fn record_key(&mut self, cx: &ParseCx<'_>, native_id: Option<&str>, d: &Digest) -> String {
        match native_id {
            Some(id) => self.occurrences.key(id),
            None => cx.fallback_key(d, cx.counts.records),
        }
    }

    fn record_turn(&self, cx: &ParseCx<'_>, x: &Line, key: &str) -> Option<String> {
        let role = x
            .message
            .as_ref()
            .and_then(|message| message.role.as_deref())
            .or(x.ty.as_deref());
        if cx.session.kind == SessionKind::Subagent {
            None
        } else if role == Some("user") {
            Some(key.to_owned())
        } else if role == Some("assistant") {
            self.lineage.ancestor_turn(x.parent.as_deref())
        } else {
            None
        }
    }

    fn classify(
        &mut self,
        cx: &mut ParseCx<'_>,
        x: &mut Line,
        meta: &mut RecordMeta,
    ) -> Projection {
        match x.ty.as_deref() {
            Some("user" | "assistant")
                if x.is_meta != Some(true)
                    && x.is_sidechain != Some(true)
                    && (cx.session.kind != SessionKind::Subagent
                        || cx.options.include_subagents) =>
            {
                return self.project_message(cx, x, meta);
            }
            Some(
                "user"
                | "assistant"
                | "system"
                | "progress"
                | "file-history-snapshot"
                | "last-prompt"
                | "queue-operation",
            ) => {}
            Some("summary" | "compact_boundary") => meta.kind = RecordKind::Summary,
            Some(_) | None => {
                meta.kind = RecordKind::Unknown;
                cx.counts.unknown_records += 1;
                cx.coverage = Coverage::Partial;
            }
        }

        Projection {
            identity: None,
            emission: Emission::default(),
        }
    }

    fn project_message(
        &mut self,
        cx: &mut ParseCx<'_>,
        x: &mut Line,
        meta: &mut RecordMeta,
    ) -> Projection {
        let mut out = Projection {
            identity: None,
            emission: Emission::default(),
        };
        let Some(m) = x.message.take() else {
            return out;
        };
        let role = m.role.as_deref().or(x.ty.as_deref()).unwrap_or("");
        let role = match role {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            _ => return out,
        };

        out.identity =
            m.id.as_deref()
                .map(|id| format!("message:{id}"))
                .or_else(|| x.uuid.as_deref().map(|id| format!("uuid:{id}")));
        let blocks = match m.content {
            Some(content) => self.content_blocks(cx, content, meta, &mut out.emission),
            None => Vec::new(),
        };

        let has_text = !blocks.is_empty();
        if !has_text {
            infer_result_turn(meta, &out.emission.results);
        }

        if has_text
            || out
                .identity
                .as_ref()
                .is_some_and(|id| self.message_keys.contains_key(id))
        {
            out.emission.message = Some(MessageEntry {
                record_key: meta.key.clone(),
                role,
                phase: None,
                blocks,
                attachments: Vec::new(),
                supersedes: Vec::new(),
            });
        }

        if has_text {
            meta.kind = RecordKind::Message;
        } else if !out.emission.calls.is_empty() {
            meta.kind = RecordKind::ToolCall;
        } else if !out.emission.results.is_empty() {
            meta.kind = RecordKind::ToolResult;
        }

        out
    }

    fn content_blocks(
        &mut self,
        cx: &mut ParseCx<'_>,
        content: Content,
        meta: &RecordMeta,
        emission: &mut Emission,
    ) -> Vec<TextBlock> {
        let mut blocks = Vec::new();
        match content {
            Content::Text(text) => blocks.push(TextBlock {
                index: 0,
                origin: BlockOrigin::Authored,
                text,
            }),
            Content::Blocks(items) => {
                for (i, block) in items.into_iter().enumerate() {
                    let index = i as u32;
                    match block.ty.as_deref() {
                        Some("text") => {
                            if let Some(text) = block.text {
                                blocks.push(TextBlock {
                                    index,
                                    origin: BlockOrigin::Authored,
                                    text,
                                });
                            }
                        }
                        Some("tool_use") => emission.calls.push(ToolCallMeta {
                            record_key: meta.key.clone(),
                            block_index: index,
                            native_call_id: block.id,
                            tool_name: block.name.unwrap_or_else(|| "unknown".into()),
                            turn_id: meta.turn_id.clone(),
                        }),
                        Some("tool_result") => {
                            emission.results.push(self.tool_result(block, index, meta))
                        }
                        Some("image" | "document" | "thinking" | "redacted_thinking") => {}
                        Some(_) | None => cx.counts.unknown_blocks += 1,
                    }
                }
            }
        }
        blocks
    }

    fn tool_result(&self, block: Block, index: u32, meta: &RecordMeta) -> ToolResultMeta {
        let call_id = block.call_id;
        let call = call_id
            .as_deref()
            .and_then(|id| self.lineage.ancestor_call(meta.parent_key.as_deref(), id));
        let turn_id = call
            .as_ref()
            .and_then(|(record, _)| self.lineage.turn_of(record).map(str::to_owned))
            .or_else(|| meta.turn_id.clone());
        ToolResultMeta {
            record_key: meta.key.clone(),
            block_index: index,
            native_call_id: call_id,
            call,
            is_error: block.is_error,
            turn_id,
        }
    }

    fn supersede(
        &mut self,
        cx: &mut ParseCx<'_>,
        id: String,
        blocks: usize,
        meta: &mut RecordMeta,
    ) -> Vec<String> {
        let supersedes = self.message_keys.get(&id).cloned().unwrap_or_default();
        if let Some(previous_blocks) = self.latest_blocks.insert(id.clone(), blocks) {
            cx.counts.blocks_retained = cx
                .counts
                .blocks_retained
                .saturating_sub(previous_blocks as u64);
        } else {
            cx.counts.messages += 1;
        }
        cx.counts.blocks_retained += blocks as u64;
        if meta.parent_key.is_none()
            && let Some(inherited) = self.latest_message_parent.get(&id)
        {
            meta.parent_key = inherited.clone();
        }
        self.latest_message_parent
            .insert(id.clone(), meta.parent_key.clone());
        self.message_keys
            .entry(id)
            .or_default()
            .push(meta.key.clone());
        supersedes
    }

    fn publish(
        &mut self,
        cx: &mut ParseCx<'_>,
        mut meta: RecordMeta,
        projection: Projection,
    ) -> Result<(), AdapterError> {
        let Projection {
            identity,
            mut emission,
        } = projection;
        match (emission.message.as_mut(), identity) {
            (Some(msg), Some(id)) => {
                msg.supersedes = self.supersede(cx, id, msg.blocks.len(), &mut meta);
            }
            (Some(msg), None) => cx.count_message(msg.blocks.len()),
            (None, _) => {}
        }

        cx.emit(meta, emission)
    }
}
