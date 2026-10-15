use super::{Content, Line, Payload, VERSION};
use crate::adapters::{AdapterError, ParseOptions, RecordSink};
use crate::domain::{
    BlockOrigin, Coverage, Digest, MessageEntry, ParseCounts, ParseWarning, ParsedSessionManifest,
    RecordKind, RecordMeta, Role, SessionKind, SourceLocator, TextBlock, ToolCallMeta,
    ToolResultMeta, digest, parse_timestamp_ms,
};
use crate::ids::hex;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read},
};

fn ms(timestamp: Option<&str>) -> Option<i64> {
    timestamp.and_then(parse_timestamp_ms)
}

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

/// `None` ends the readable prefix: EOF, an unterminated line, or a line crossing a byte limit.
fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    offset: u64,
    o: &ParseOptions,
) -> Result<Option<usize>, AdapterError> {
    if offset >= o.observe_len || offset >= o.max_source_bytes {
        return Ok(None);
    }

    line.clear();
    let n = reader.read_until(b'\n', line)?;
    let end = offset + n as u64;
    if n == 0 || line.last() != Some(&b'\n') || end > o.observe_len || end > o.max_source_bytes {
        return Ok(None);
    }
    if n as u64 > o.max_record_bytes {
        return Err(AdapterError::LimitExceeded);
    }

    Ok(Some(n))
}

pub(super) fn parse_snapshot(
    source: &mut dyn Read,
    o: &ParseOptions,
    sink: &mut dyn RecordSink,
) -> Result<ParsedSessionManifest, AdapterError> {
    let mut reader = BufReader::new(source);
    let mut line = Vec::new();
    let mut state = ParseState::new();
    let mut offset = 0u64;

    loop {
        if sink.cancelled().load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AdapterError::Cancelled);
        }
        let Some(n) = read_bounded_line(&mut reader, &mut line, offset, o)? else {
            break;
        };

        let locator = SourceLocator {
            offset,
            len: n as u64,
        };
        state.handle_line(&line, locator, sink)?;
        offset += n as u64;
    }

    Ok(state.finish(offset))
}

struct SessionMeta {
    native_id: Option<String>,
    cwd: Option<String>,
    parent_native_session: Option<String>,
    kind: SessionKind,
}

impl SessionMeta {
    fn unknown() -> Self {
        Self {
            native_id: None,
            cwd: None,
            parent_native_session: None,
            kind: SessionKind::Unknown,
        }
    }

    fn from_payload(p: Payload) -> Self {
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

        Self {
            native_id: p.thread_id.or(p.conversation_id).or(p.id),
            cwd: p.cwd,
            parent_native_session,
            kind,
        }
    }
}

struct Pending {
    meta: RecordMeta,
    message: Option<MessageEntry>,
    calls: Vec<ToolCallMeta>,
    results: Vec<ToolResultMeta>,
}

struct ParseState {
    counts: ParseCounts,
    coverage: Coverage,
    session: SessionMeta,
    started_at: Option<i64>,
    last_activity_at: Option<i64>,
    // Codex's pinned rollout item schema has no per-item parent field; persisted record order is linear.
    previous_record_key: Option<String>,
    occurrences: HashMap<String, usize>,
    /// Message identities already retained, shared by `response_item` and `event_msg` aliases.
    seen: HashSet<String>,
    calls_by_turn: HashMap<(String, String), Option<(String, u32)>>,
}

impl ParseState {
    fn new() -> Self {
        Self {
            counts: ParseCounts::default(),
            coverage: Coverage::Complete,
            session: SessionMeta::unknown(),
            started_at: None,
            last_activity_at: None,
            previous_record_key: None,
            occurrences: HashMap::new(),
            seen: HashSet::new(),
            calls_by_turn: HashMap::new(),
        }
    }

    fn finish(self, prefix_end: u64) -> ParsedSessionManifest {
        ParsedSessionManifest {
            native_session_id: self.session.native_id.unwrap_or_else(|| "unknown".into()),
            native_title: None,
            cwd: self.session.cwd,
            session_kind: self.session.kind,
            parent_native_session: self.session.parent_native_session,
            lineage_verified: false,
            started_at: self.started_at,
            last_activity_at: self.last_activity_at,
            prefix_end,
            active_leaf_key: None,
            coverage: self.coverage,
            counts: self.counts,
        }
    }

    fn handle_line(
        &mut self,
        line: &[u8],
        locator: SourceLocator,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        self.counts.bytes_read += locator.len;
        self.counts.records += 1;

        let d = digest(line);
        match serde_json::from_slice::<Line>(line) {
            Ok(record) => self.handle_record(record, locator, d, sink),
            Err(_) => self.handle_malformed(locator, d, sink),
        }
    }

    fn handle_malformed(
        &mut self,
        locator: SourceLocator,
        d: Digest,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let key = format!(
            "fallback:{VERSION}:{}:{}",
            hex(&d),
            self.counts.malformed_records
        );
        let meta = RecordMeta {
            key: key.clone(),
            native_id: None,
            parent_key: self.previous_record_key.take(),
            kind: RecordKind::Unknown,
            timestamp: None,
            turn_id: None,
            locator,
            digest: d,
        };
        self.previous_record_key = Some(key.clone());

        sink.record(meta)?;
        sink.warning(ParseWarning {
            code: "invalid_record".into(),
            record_key: Some(key),
            locator: Some(locator),
            detail: "invalid complete JSONL rollout line".into(),
        })?;

        self.counts.malformed_records += 1;
        self.counts.unknown_records += 1;
        self.coverage = Coverage::Partial;
        Ok(())
    }

    fn handle_record(
        &mut self,
        line: Line,
        locator: SourceLocator,
        d: Digest,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let Line {
            timestamp,
            ordinal,
            ty,
            turn_id,
            payload,
            ..
        } = line;
        let timestamp = ms(timestamp.as_deref());
        self.observe_timestamp(timestamp, ty.as_deref());

        let payload = payload.unwrap_or_default();
        let native_id = payload.id.clone();
        let key = self.record_key(native_id.as_deref(), ordinal, &d);
        let mut rec = Pending {
            meta: RecordMeta {
                key,
                native_id,
                parent_key: self.previous_record_key.take(),
                kind: RecordKind::Control,
                timestamp,
                turn_id: record_turn(ty.as_deref(), turn_id, &payload),
                locator,
                digest: d,
            },
            message: None,
            calls: Vec::new(),
            results: Vec::new(),
        };

        match ty.as_deref() {
            Some("session_meta") => self.apply_session_meta(payload),
            Some("response_item") => self.classify_response_item(&mut rec, payload),
            Some("event_msg") => self.classify_event_msg(&mut rec, payload),
            _ => self.mark_unknown(&mut rec.meta),
        }

        self.publish(rec, sink)
    }

    fn observe_timestamp(&mut self, timestamp: Option<i64>, ty: Option<&str>) {
        let Some(timestamp) = timestamp else {
            return;
        };
        if ty == Some("session_meta") {
            self.started_at.get_or_insert(timestamp);
        }
        self.last_activity_at = Some(
            self.last_activity_at
                .map_or(timestamp, |last| last.max(timestamp)),
        );
    }

    /// Native identity (or line ordinal) with a `~n` suffix for repeats; digest fallback when neither exists.
    fn record_key(&mut self, native_id: Option<&str>, ordinal: Option<u64>, d: &Digest) -> String {
        let identity = native_id
            .map(str::to_owned)
            .or_else(|| ordinal.map(|ordinal| ordinal.to_string()));
        let Some(identity) = identity else {
            return format!("fallback:{VERSION}:{}:{}", hex(d), self.counts.records);
        };

        let occurrence = self.occurrences.entry(identity.clone()).or_default();
        let key = if *occurrence == 0 {
            identity
        } else {
            format!("{identity}~{occurrence}")
        };
        *occurrence += 1;
        key
    }

    fn apply_session_meta(&mut self, p: Payload) {
        if p.history_mode
            .as_deref()
            .is_some_and(|mode| !matches!(mode, "legacy" | "paginated"))
        {
            self.coverage = Coverage::Unsupported;
        }
        self.session = SessionMeta::from_payload(p);
    }

    fn mark_unknown(&mut self, meta: &mut RecordMeta) {
        meta.kind = RecordKind::Unknown;
        self.counts.unknown_records += 1;
        self.coverage = Coverage::Partial;
    }

    fn classify_response_item(&mut self, rec: &mut Pending, p: Payload) {
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
            Some("message") => self.response_message(rec, role.as_deref(), phase, content),
            Some("compaction" | "context_compaction") => rec.meta.kind = RecordKind::Summary,
            Some("function_call") => Self::tool_call(rec, call_id, name),
            Some("function_call_output") => self.tool_result(rec, call_id, is_error),
            Some("local_shell_call" | "custom_tool_call") => rec.meta.kind = RecordKind::ToolCall,
            Some("custom_tool_call_output") => rec.meta.kind = RecordKind::ToolResult,
            Some(_) => {
                rec.meta.kind = RecordKind::Control;
                self.counts.unknown_records += 1;
            }
            None => self.mark_unknown(&mut rec.meta),
        }

        if let Some(ty) = ty.as_deref() {
            self.reconcile_canonical(&mut rec.meta, ty);
        }
    }

    /// Canonical item types are expected Control records, so they are not counted as unknown;
    /// anything outside the allowlist degrades coverage.
    fn reconcile_canonical(&mut self, meta: &mut RecordMeta, ty: &str) {
        if !canonical_response_item(ty) {
            meta.kind = RecordKind::Unknown;
            self.coverage = Coverage::Partial;
        } else if meta.kind == RecordKind::Control {
            self.counts.unknown_records = self.counts.unknown_records.saturating_sub(1);
        }
    }

    fn response_message(
        &mut self,
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
        let blocks = self.text_blocks(content);
        if blocks.is_empty() {
            return;
        }

        rec.meta.kind = RecordKind::Message;
        if self.claim_message(&rec.meta) {
            self.retain_message(rec, role, phase, blocks);
        }
    }

    fn text_blocks(&mut self, content: Option<Vec<Content>>) -> Vec<TextBlock> {
        let mut blocks = Vec::new();
        for (i, c) in content.unwrap_or_default().into_iter().enumerate() {
            if !matches!(c.ty.as_deref(), Some("input_text" | "output_text" | "text")) {
                self.counts.unknown_blocks += 1;
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

    /// First claimant of a native identity owns the message; `event_msg` aliases of a retained
    /// `response_item` (or the reverse) publish only their record.
    fn claim_message(&mut self, meta: &RecordMeta) -> bool {
        let identity = meta.native_id.clone().unwrap_or_else(|| meta.key.clone());
        self.seen.insert(identity)
    }

    fn retain_message(
        &mut self,
        rec: &mut Pending,
        role: Role,
        phase: Option<String>,
        blocks: Vec<TextBlock>,
    ) {
        self.counts.messages += 1;
        self.counts.blocks_retained += blocks.len() as u64;
        rec.message = Some(MessageEntry {
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
        rec.calls.push(ToolCallMeta {
            record_key: rec.meta.key.clone(),
            block_index: 0,
            native_call_id: call_id,
            tool_name: name.unwrap_or_else(|| "unknown".into()),
            turn_id: rec.meta.turn_id.clone(),
        });
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
        rec.results.push(ToolResultMeta {
            record_key: rec.meta.key.clone(),
            block_index: 0,
            native_call_id: call_id,
            call,
            is_error,
            turn_id: rec.meta.turn_id.clone(),
        });
    }

    fn classify_event_msg(&mut self, rec: &mut Pending, p: Payload) {
        let Payload {
            ty, message, text, ..
        } = p;
        let role = match ty.as_deref() {
            Some("UserMessage") => Role::User,
            Some("AgentMessage") => Role::Assistant,
            _ => return,
        };
        let Some(text) = message.or(text).filter(|t| !t.is_empty()) else {
            self.mark_unknown(&mut rec.meta);
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
        self.retain_message(rec, role, None, blocks);
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

    fn publish(&mut self, rec: Pending, sink: &mut dyn RecordSink) -> Result<(), AdapterError> {
        self.link_calls(&rec.calls);
        self.previous_record_key = Some(rec.meta.key.clone());

        sink.record(rec.meta)?;
        if let Some(message) = rec.message {
            sink.message(message)?;
        }
        for call in rec.calls {
            sink.tool_call(call)?;
        }
        for result in rec.results {
            sink.tool_result(result)?;
        }
        Ok(())
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
