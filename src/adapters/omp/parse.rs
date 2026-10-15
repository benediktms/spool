use super::{Block, Content, Entry, Message, TextEntry, VERSION};
use crate::adapters::{AdapterError, ParseOptions, RecordSink};
use crate::domain::{
    BlockOrigin, Coverage, MessageEntry, ParseCounts, ParseWarning, ParsedSessionManifest,
    RecordKind, RecordMeta, Role, SessionKind, SourceLocator, TextBlock, ToolCallMeta,
    ToolResultMeta, digest, parse_timestamp_ms,
};
use crate::ids::hex;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read},
};

fn bad_detail() -> String {
    "record is malformed or unsupported".into()
}
fn millis(timestamp: Option<&str>) -> Option<i64> {
    timestamp.and_then(parse_timestamp_ms)
}
fn record(entry: &Entry, timestamp: Option<i64>, loc: SourceLocator, bytes: &[u8]) -> RecordMeta {
    let d = digest(bytes);
    let key = entry
        .id
        .clone()
        .unwrap_or_else(|| format!("fallback:{VERSION}:{}:{}", hex(&d), loc.offset));
    RecordMeta {
        key,
        native_id: entry.id.clone(),
        parent_key: entry.parent.clone(),
        kind: RecordKind::Control,
        timestamp,
        turn_id: None,
        locator: loc,
        digest: d,
    }
}

fn ancestor_turn(
    parent: Option<&str>,
    parents: &HashMap<String, Option<String>>,
    turns: &HashMap<String, String>,
) -> Option<String> {
    let mut current = parent;
    let mut seen = HashSet::new();
    while let Some(key) = current {
        if !seen.insert(key) {
            break;
        }
        if let Some(turn) = turns.get(key) {
            return Some(turn.clone());
        }
        current = parents.get(key).and_then(Option::as_deref);
    }
    None
}

fn ancestor_call(
    parent: Option<&str>,
    native_call_id: &str,
    parents: &HashMap<String, Option<String>>,
    calls: &HashMap<String, Vec<(String, u32)>>,
) -> Option<(String, u32)> {
    let mut current = parent;
    let mut seen = HashSet::new();
    while let Some(key) = current {
        if !seen.insert(key) {
            break;
        }
        if let Some(matches) = calls
            .get(key)
            .filter(|calls| calls.iter().any(|(id, _)| id == native_call_id))
        {
            let mut matches = matches.iter().filter(|(id, _)| id == native_call_id);
            let call = matches.next()?;
            return matches.next().is_none().then(|| (key.to_owned(), call.1));
        }
        current = parents.get(key).and_then(Option::as_deref);
    }
    None
}

struct ParseState {
    id: Option<String>,
    title: Option<String>,
    cwd: Option<String>,
    parent_session: Option<String>,
    session_kind: SessionKind,
    started_at: Option<i64>,
    last_activity_at: Option<i64>,
    counts: ParseCounts,
    coverage: Coverage,
    parent_by_key: HashMap<String, Option<String>>,
    turn_by_key: HashMap<String, String>,
    calls_by_key: HashMap<String, Vec<(String, u32)>>,
}

#[derive(Default)]
struct Events {
    message: Option<MessageEntry>,
    calls: Vec<ToolCallMeta>,
    results: Vec<ToolResultMeta>,
}

pub(super) fn parse_snapshot(
    source: &mut dyn Read,
    o: &ParseOptions,
    sink: &mut dyn RecordSink,
) -> Result<ParsedSessionManifest, AdapterError> {
    let mut reader = BufReader::new(source);
    let mut line = Vec::new();
    let mut offset = 0u64;
    let mut state = ParseState::new();

    loop {
        if sink.cancelled().load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AdapterError::Cancelled);
        }
        if offset >= o.observe_len || offset >= o.max_source_bytes {
            break;
        }

        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        let end = offset + n as u64;
        if n == 0 || end > o.observe_len || end > o.max_source_bytes {
            break;
        }
        if line.len() as u64 > o.max_record_bytes {
            return Err(AdapterError::LimitExceeded);
        }
        if line.last() != Some(&b'\n') {
            break;
        }

        state.counts.bytes_read += n as u64;
        state.counts.records += 1;
        let loc = SourceLocator {
            offset,
            len: n as u64,
        };
        match serde_json::from_slice::<Entry>(&line) {
            Ok(entry) => state.handle_entry(entry, loc, &line, sink)?,
            Err(_) => state.handle_malformed(loc, &line, sink)?,
        }
        offset = end;
    }

    Ok(state.finish(offset))
}

impl ParseState {
    fn new() -> Self {
        Self {
            id: None,
            title: None,
            cwd: None,
            parent_session: None,
            session_kind: SessionKind::Unknown,
            started_at: None,
            last_activity_at: None,
            counts: ParseCounts::default(),
            coverage: Coverage::Complete,
            parent_by_key: HashMap::new(),
            turn_by_key: HashMap::new(),
            calls_by_key: HashMap::new(),
        }
    }

    /// `offset` is also the committed prefix: every consumed record advances both.
    fn finish(self, offset: u64) -> ParsedSessionManifest {
        ParsedSessionManifest {
            native_session_id: self.id.unwrap_or_else(|| "unknown".into()),
            native_title: self.title,
            cwd: self.cwd,
            session_kind: self.session_kind,
            parent_native_session: self.parent_session,
            lineage_verified: false,
            started_at: self.started_at,
            last_activity_at: self.last_activity_at,
            prefix_end: offset,
            active_leaf_key: None,
            coverage: self.coverage,
            counts: self.counts,
        }
    }

    fn handle_entry(
        &mut self,
        e: Entry,
        loc: SourceLocator,
        line: &[u8],
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let timestamp = millis(e.timestamp.as_deref());
        let is_session = e.ty.as_deref() == Some("session");
        self.observe_time(timestamp, is_session);

        if loc.offset == 0 && e.ty.as_deref() == Some("title") {
            if e.slot_version != Some(1) {
                self.coverage = Coverage::Unsupported;
            }
            Ok(())
        } else if is_session {
            self.handle_session(e, timestamp, loc, line, sink)
        } else {
            self.handle_conversation(e, timestamp, loc, line, sink)
        }
    }

    fn observe_time(&mut self, timestamp: Option<i64>, is_session: bool) {
        if let Some(timestamp) = timestamp {
            self.started_at.get_or_insert(timestamp);
            self.last_activity_at = Some(
                self.last_activity_at
                    .map_or(timestamp, |last| last.max(timestamp)),
            );
        }
        if is_session {
            self.started_at = timestamp.or(self.started_at);
        }
    }

    fn handle_session(
        &mut self,
        e: Entry,
        timestamp: Option<i64>,
        loc: SourceLocator,
        line: &[u8],
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        if e.version != Some(3) {
            self.coverage = Coverage::Unsupported;
        }
        let mut meta = record(&e, timestamp, loc, line);
        meta.kind = RecordKind::Header;

        self.session_kind = if e.parent_session.is_some() {
            SessionKind::Subagent
        } else {
            SessionKind::Interactive
        };
        self.id = e.id;
        self.title = e.title;
        self.cwd = e.cwd;
        self.parent_session = e.parent_session;

        sink.record(meta)?;
        Ok(())
    }

    fn handle_conversation(
        &mut self,
        e: Entry,
        timestamp: Option<i64>,
        loc: SourceLocator,
        line: &[u8],
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let mut meta = record(&e, timestamp, loc, line);
        meta.turn_id = self.turn_for(&e, &meta);
        let events = self.project(e, &mut meta, line);

        self.update_indexes(&meta, &events.calls);
        self.publish(meta, events, sink)
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
            ancestor_turn(
                meta.parent_key.as_deref(),
                &self.parent_by_key,
                &self.turn_by_key,
            )
        } else {
            None
        }
    }

    fn project(&mut self, e: Entry, meta: &mut RecordMeta, line: &[u8]) -> Events {
        let mut events = Events::default();
        match e.ty.as_deref() {
            Some("message") => self.project_message(e.message, meta, line, &mut events),
            Some("compaction" | "branch_summary") => meta.kind = RecordKind::Summary,
            Some("toolResult") => {
                meta.kind = RecordKind::ToolResult;
                events.results.push(self.tool_result(e.message, meta));
            }
            Some("title") => meta.kind = RecordKind::Header,
            Some(_) => {
                meta.kind = RecordKind::Control;
                self.counts.unknown_records += 1;
            }
            None => {
                meta.kind = RecordKind::Unknown;
                self.counts.unknown_records += 1;
            }
        }
        events
    }

    fn project_message(
        &mut self,
        message: Option<Message>,
        meta: &mut RecordMeta,
        line: &[u8],
        events: &mut Events,
    ) {
        let Some(message) = message else {
            return;
        };
        match message.role.as_deref() {
            Some("toolResult") => {
                meta.kind = RecordKind::ToolResult;
                events.results.push(self.tool_result(Some(message), meta));
            }
            Some("user") => self.project_authored(Role::User, meta, line, events),
            Some("assistant") => self.project_authored(Role::Assistant, meta, line, events),
            _ => {}
        }
    }

    /// Decodes only the text content again so excluded payloads are never buffered.
    fn project_authored(
        &mut self,
        role: Role,
        meta: &mut RecordMeta,
        line: &[u8],
        events: &mut Events,
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
            Some(Content::Blocks(blocks)) => self.content_blocks(blocks, meta, &mut events.calls),
            None => Vec::new(),
        };

        if !blocks.is_empty() {
            meta.kind = RecordKind::Message;
            events.message = Some(MessageEntry {
                record_key: meta.key.clone(),
                role,
                phase,
                blocks,
                attachments: Vec::new(),
                supersedes: vec![],
            });
        } else if !events.calls.is_empty() {
            meta.kind = RecordKind::ToolCall;
        }
    }

    fn content_blocks(
        &mut self,
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
                Some(_) | None => self.counts.unknown_blocks += 1,
            }
        }
        text_blocks
    }

    /// A call link exists only when exactly one ancestor call carries the native id.
    fn tool_result(&self, message: Option<Message>, meta: &RecordMeta) -> ToolResultMeta {
        let (native_call_id, is_error) =
            message.map_or((None, None), |message| (message.call_id, message.is_error));
        let call = native_call_id.as_deref().and_then(|id| {
            ancestor_call(
                meta.parent_key.as_deref(),
                id,
                &self.parent_by_key,
                &self.calls_by_key,
            )
        });

        ToolResultMeta {
            record_key: meta.key.clone(),
            block_index: 0,
            native_call_id,
            call,
            is_error,
            turn_id: meta.turn_id.clone(),
        }
    }

    fn update_indexes(&mut self, meta: &RecordMeta, calls: &[ToolCallMeta]) {
        if let Some(turn_id) = &meta.turn_id {
            self.turn_by_key.insert(meta.key.clone(), turn_id.clone());
        }
        self.parent_by_key
            .insert(meta.key.clone(), meta.parent_key.clone());
        if !calls.is_empty() {
            let ids = calls
                .iter()
                .filter_map(|call| {
                    call.native_call_id
                        .as_ref()
                        .map(|id| (id.clone(), call.block_index))
                })
                .collect();
            self.calls_by_key.insert(meta.key.clone(), ids);
        }
    }

    /// Sink order is part of the contract: record, message, calls, results.
    fn publish(
        &mut self,
        meta: RecordMeta,
        events: Events,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        sink.record(meta)?;
        if let Some(message) = events.message {
            self.counts.messages += 1;
            self.counts.blocks_retained += message.blocks.len() as u64;
            sink.message(message)?;
        }
        for call in events.calls {
            sink.tool_call(call)?;
        }
        for result in events.results {
            sink.tool_result(result)?;
        }
        Ok(())
    }

    fn handle_malformed(
        &mut self,
        loc: SourceLocator,
        line: &[u8],
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let d = digest(line);
        let key = format!(
            "fallback:{VERSION}:{}:{}",
            hex(&d),
            self.counts.malformed_records
        );

        sink.record(RecordMeta {
            key: key.clone(),
            native_id: None,
            parent_key: None,
            kind: RecordKind::Unknown,
            timestamp: None,
            turn_id: None,
            locator: loc,
            digest: d,
        })?;
        sink.warning(ParseWarning {
            code: "invalid_record".into(),
            record_key: Some(key),
            locator: Some(loc),
            detail: bad_detail(),
        })?;

        self.counts.malformed_records += 1;
        self.counts.unknown_records += 1;
        self.coverage = Coverage::Partial;
        Ok(())
    }
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
