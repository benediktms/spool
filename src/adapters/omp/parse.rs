use super::{Block, Content, Entry, Message, TextEntry};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{Emission, LineParser, ParseCx, RawRecord, SessionFacts};
use crate::adapters::lineage::RecordLineage;
use crate::domain::{
    BlockOrigin, Coverage, MessageEntry, RecordKind, RecordMeta, Role, SessionKind, TextBlock,
    ToolCallMeta, ToolResultMeta, parse_timestamp_ms,
};

fn record_meta(
    cx: &ParseCx<'_>,
    entry: &Entry,
    timestamp: Option<i64>,
    raw: RawRecord<'_>,
) -> RecordMeta {
    let key = entry
        .id
        .clone()
        .unwrap_or_else(|| cx.fallback_key(&raw.digest, raw.locator.offset));
    RecordMeta {
        key,
        native_id: entry.id.clone(),
        parent_key: entry.parent.clone(),
        kind: RecordKind::Control,
        timestamp,
        turn_id: None,
        locator: raw.locator,
        digest: raw.digest,
    }
}

#[derive(Default)]
pub(crate) struct ParseState {
    lineage: RecordLineage,
}

impl LineParser for ParseState {
    type Line = Entry;

    fn record(
        &mut self,
        cx: &mut ParseCx<'_>,
        e: Entry,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError> {
        let timestamp = e.timestamp.as_deref().and_then(parse_timestamp_ms);
        let is_session = e.ty.as_deref() == Some("session");
        observe_time(cx, timestamp, is_session);

        if raw.locator.offset == 0 && e.ty.as_deref() == Some("title") {
            if e.slot_version != Some(1) {
                cx.coverage = Coverage::Unsupported;
            }
            Ok(())
        } else if is_session {
            handle_session(cx, e, timestamp, raw)
        } else {
            self.handle_conversation(cx, e, timestamp, raw)
        }
    }
}

fn observe_time(cx: &mut ParseCx<'_>, timestamp: Option<i64>, is_session: bool) {
    cx.observe_activity(timestamp);
    if let Some(timestamp) = timestamp {
        cx.started_at.get_or_insert(timestamp);
    }
    if is_session {
        cx.started_at = timestamp.or(cx.started_at);
    }
}

fn handle_session(
    cx: &mut ParseCx<'_>,
    e: Entry,
    timestamp: Option<i64>,
    raw: RawRecord<'_>,
) -> Result<(), AdapterError> {
    if e.version != Some(3) {
        cx.coverage = Coverage::Unsupported;
    }
    let mut meta = record_meta(cx, &e, timestamp, raw);
    meta.kind = RecordKind::Header;

    cx.session = SessionFacts {
        kind: if e.parent_session.is_some() {
            SessionKind::Subagent
        } else {
            SessionKind::Interactive
        },
        native_id: e.id,
        title: e.title,
        cwd: e.cwd,
        parent_native_session: e.parent_session,
    };

    cx.emit(meta, Emission::default())
}

impl ParseState {
    fn handle_conversation(
        &mut self,
        cx: &mut ParseCx<'_>,
        e: Entry,
        timestamp: Option<i64>,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError> {
        let mut meta = record_meta(cx, &e, timestamp, raw);
        meta.turn_id = self.turn_for(&e, &meta);
        let emission = self.project(cx, e, &mut meta, raw.bytes);

        self.lineage.index(&meta, &emission.calls);
        publish(cx, meta, emission)
    }

    /// User records start a turn; assistant/tool-result records inherit the nearest ancestor's.
    fn turn_for(&self, e: &Entry, meta: &RecordMeta) -> Option<String> {
        let role = e
            .message
            .as_ref()
            .and_then(|message| message.role.as_deref());
        if role == Some("user") {
            return Some(meta.key.clone());
        }

        let joins_turn = matches!(role, Some("assistant" | "toolResult"))
            || e.ty.as_deref() == Some("toolResult");
        if joins_turn {
            self.lineage.ancestor_turn(meta.parent_key.as_deref())
        } else {
            None
        }
    }

    fn project(
        &mut self,
        cx: &mut ParseCx<'_>,
        e: Entry,
        meta: &mut RecordMeta,
        line: &[u8],
    ) -> Emission {
        let mut emission = Emission::default();
        match e.ty.as_deref() {
            Some("message") => self.project_message(cx, e.message, meta, line, &mut emission),
            Some("compaction" | "branch_summary") => meta.kind = RecordKind::Summary,
            Some("toolResult") => {
                meta.kind = RecordKind::ToolResult;
                emission.results.push(self.tool_result(e.message, meta));
            }
            Some("title") => meta.kind = RecordKind::Header,
            Some(_) => {
                meta.kind = RecordKind::Control;
                cx.counts.unknown_records += 1;
            }
            None => {
                meta.kind = RecordKind::Unknown;
                cx.counts.unknown_records += 1;
            }
        }
        emission
    }

    fn project_message(
        &mut self,
        cx: &mut ParseCx<'_>,
        message: Option<Message>,
        meta: &mut RecordMeta,
        line: &[u8],
        emission: &mut Emission,
    ) {
        let Some(message) = message else {
            return;
        };
        match message.role.as_deref() {
            Some("toolResult") => {
                meta.kind = RecordKind::ToolResult;
                emission.results.push(self.tool_result(Some(message), meta));
            }
            Some("user") => project_authored(cx, Role::User, meta, line, emission),
            Some("assistant") => project_authored(cx, Role::Assistant, meta, line, emission),
            _ => {}
        }
    }

    /// A call link exists only when exactly one ancestor call carries the native id.
    fn tool_result(&self, message: Option<Message>, meta: &RecordMeta) -> ToolResultMeta {
        let (native_call_id, is_error) =
            message.map_or((None, None), |message| (message.call_id, message.is_error));
        let call = native_call_id
            .as_deref()
            .and_then(|id| self.lineage.ancestor_call(meta.parent_key.as_deref(), id));

        ToolResultMeta {
            record_key: meta.key.clone(),
            block_index: 0,
            native_call_id,
            call,
            is_error,
            turn_id: meta.turn_id.clone(),
        }
    }
}

/// Decodes only the text content again so excluded payloads are never buffered.
fn project_authored(
    cx: &mut ParseCx<'_>,
    role: Role,
    meta: &mut RecordMeta,
    line: &[u8],
    emission: &mut Emission,
) {
    let content = serde_json::from_slice::<TextEntry>(line)
        .ok()
        .and_then(|entry| entry.message)
        .and_then(|message| message.content);
    let phase = content.as_ref().and_then(signature_phase);
    let blocks = match content {
        Some(Content::Text(text)) => vec![TextBlock {
            index: 0,
            origin: BlockOrigin::Authored,
            text,
        }],
        Some(Content::Blocks(blocks)) => content_blocks(cx, blocks, meta, &mut emission.calls),
        None => Vec::new(),
    };

    if !blocks.is_empty() {
        meta.kind = RecordKind::Message;
        emission.message = Some(MessageEntry {
            record_key: meta.key.clone(),
            role,
            phase,
            blocks,
            attachments: Vec::new(),
            supersedes: vec![],
        });
    } else if !emission.calls.is_empty() {
        meta.kind = RecordKind::ToolCall;
    }
}

fn content_blocks(
    cx: &mut ParseCx<'_>,
    blocks: Vec<Block>,
    meta: &RecordMeta,
    calls: &mut Vec<ToolCallMeta>,
) -> Vec<TextBlock> {
    let mut text_blocks = Vec::new();
    for (i, block) in blocks.into_iter().enumerate() {
        match block.ty.as_deref() {
            Some("text") => {
                if let Some(text) = block.text {
                    text_blocks.push(TextBlock {
                        index: i as u32,
                        origin: BlockOrigin::Authored,
                        text,
                    });
                }
            }
            Some("toolCall") => calls.push(ToolCallMeta {
                record_key: meta.key.clone(),
                block_index: i as u32,
                native_call_id: block.id.or(block.call_id),
                tool_name: block.name.unwrap_or_else(|| "unknown".into()),
                turn_id: meta.turn_id.clone(),
            }),
            Some("image") => {}
            Some(_) | None => cx.counts.unknown_blocks += 1,
        }
    }
    text_blocks
}

fn publish(cx: &mut ParseCx<'_>, meta: RecordMeta, emission: Emission) -> Result<(), AdapterError> {
    if let Some(message) = &emission.message {
        cx.count_message(message.blocks.len());
    }
    cx.emit(meta, emission)
}

fn signature_phase(content: &Content) -> Option<String> {
    match content {
        Content::Blocks(blocks) => blocks
            .iter()
            .find_map(|block| block.signature.as_ref()?.get("phase")?.as_str())
            .map(str::to_owned),
        Content::Text(_) => None,
    }
}
