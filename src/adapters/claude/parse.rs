use super::{AdapterError, Block, Content, Line, ParseOptions, RecordSink, VERSION};
use crate::domain::{
    BlockOrigin, Coverage, Digest, MessageEntry, ParseCounts, ParseWarning, ParsedSessionManifest,
    RecordKind, RecordMeta, Role, SessionKind, SourceLocator, TextBlock, ToolCallMeta,
    ToolResultMeta, digest, parse_timestamp_ms,
};
use crate::ids::hex;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read},
};

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
    offset: u64,
    prefix: u64,
    counts: ParseCounts,
    sid: Option<String>,
    cwd: Option<String>,
    parent: Option<String>,
    session_kind: SessionKind,
    coverage: Coverage,
    started_at: Option<i64>,
    last_activity_at: Option<i64>,
    occurrences: HashMap<String, usize>,
    message_keys: HashMap<String, Vec<String>>,
    latest_blocks: HashMap<String, usize>,
    parent_by_key: HashMap<String, Option<String>>,
    latest_message_parent: HashMap<String, Option<String>>,
    turn_by_key: HashMap<String, String>,
    calls_by_key: HashMap<String, Vec<(String, u32)>>,
}

#[derive(Default)]
struct Emission {
    message: Option<MessageEntry>,
    identity: Option<String>,
    calls: Vec<ToolCallMeta>,
    results: Vec<ToolResultMeta>,
}

impl ParseState {
    fn new() -> Self {
        Self {
            offset: 0,
            prefix: 0,
            counts: ParseCounts::default(),
            sid: None,
            cwd: None,
            parent: None,
            session_kind: SessionKind::Unknown,
            coverage: Coverage::Complete,
            started_at: None,
            last_activity_at: None,
            occurrences: HashMap::new(),
            message_keys: HashMap::new(),
            latest_blocks: HashMap::new(),
            parent_by_key: HashMap::new(),
            latest_message_parent: HashMap::new(),
            turn_by_key: HashMap::new(),
            calls_by_key: HashMap::new(),
        }
    }

    fn handle_line(
        &mut self,
        line: &[u8],
        o: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let n = line.len() as u64;
        self.counts.bytes_read += n;
        self.counts.records += 1;
        let locator = SourceLocator {
            offset: self.offset,
            len: n,
        };

        match serde_json::from_slice::<Line>(line) {
            Ok(x) => self.decoded_record(x, line, locator, o, sink)?,
            Err(_) => self.malformed_record(line, locator, sink)?,
        }

        self.offset += n;
        self.prefix = self.offset;

        Ok(())
    }

    fn decoded_record(
        &mut self,
        mut x: Line,
        line: &[u8],
        locator: SourceLocator,
        o: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let d = digest(line);
        let timestamp = x.timestamp.as_deref().and_then(parse_timestamp_ms);
        self.observe_timestamp(timestamp);
        let key = self.record_key(x.uuid.as_deref(), &d);
        self.observe_session(&x);

        let mut meta = RecordMeta {
            key,
            native_id: x.uuid.clone(),
            parent_key: x.parent.clone(),
            kind: RecordKind::Control,
            timestamp,
            turn_id: None,
            locator,
            digest: d,
        };
        meta.turn_id = self.record_turn(&x, &meta.key);

        let emission = self.classify(&mut x, &mut meta, o);
        // Indexing must see the physical parent, so it precedes supersession's parent inheritance.
        self.index_record(&meta, &emission.calls);

        self.publish(meta, emission, sink)
    }

    fn observe_timestamp(&mut self, timestamp: Option<i64>) {
        if let Some(timestamp) = timestamp {
            self.started_at.get_or_insert(timestamp);
            self.last_activity_at = Some(
                self.last_activity_at
                    .map_or(timestamp, |last| last.max(timestamp)),
            );
        }
    }

    fn record_key(&mut self, native_id: Option<&str>, d: &Digest) -> String {
        let Some(id) = native_id else {
            return format!("fallback:{VERSION}:{}:{}", hex(d), self.counts.records);
        };
        let occurrence = self.occurrences.entry(id.to_owned()).or_default();
        let key = if *occurrence == 0 {
            id.to_owned()
        } else {
            format!("{id}~{occurrence}")
        };
        *occurrence += 1;
        key
    }

    fn observe_session(&mut self, x: &Line) {
        if self.sid.is_none() {
            self.sid = x.session_id.clone();
            self.cwd = x.cwd.clone();
            self.parent = x.parent_session.clone();
        }
        if x.parent_session.is_some() || (self.sid.is_none() && x.is_sidechain == Some(true)) {
            self.session_kind = SessionKind::Subagent;
        } else if x.session_id.is_some() && self.session_kind == SessionKind::Unknown {
            self.session_kind = SessionKind::Interactive;
        }
    }

    fn record_turn(&self, x: &Line, key: &str) -> Option<String> {
        let role = x
            .message
            .as_ref()
            .and_then(|message| message.role.as_deref())
            .or(x.ty.as_deref());
        if self.session_kind == SessionKind::Subagent {
            None
        } else if role == Some("user") {
            Some(key.to_owned())
        } else if role == Some("assistant") {
            ancestor_turn(x.parent.as_deref(), &self.parent_by_key, &self.turn_by_key)
        } else {
            None
        }
    }

    fn classify(&mut self, x: &mut Line, meta: &mut RecordMeta, o: &ParseOptions) -> Emission {
        match x.ty.as_deref() {
            Some("user" | "assistant")
                if x.is_meta != Some(true)
                    && x.is_sidechain != Some(true)
                    && (self.session_kind != SessionKind::Subagent || o.include_subagents) =>
            {
                return self.project_message(x, meta);
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
                self.counts.unknown_records += 1;
                self.coverage = Coverage::Partial;
            }
        }

        Emission::default()
    }

    fn project_message(&mut self, x: &mut Line, meta: &mut RecordMeta) -> Emission {
        let mut out = Emission::default();
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
            Some(content) => self.content_blocks(content, meta, &mut out),
            None => Vec::new(),
        };

        let has_text = !blocks.is_empty();
        if !has_text {
            infer_result_turn(meta, &out.results);
        }

        if has_text
            || out
                .identity
                .as_ref()
                .is_some_and(|id| self.message_keys.contains_key(id))
        {
            out.message = Some(MessageEntry {
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
        } else if !out.calls.is_empty() {
            meta.kind = RecordKind::ToolCall;
        } else if !out.results.is_empty() {
            meta.kind = RecordKind::ToolResult;
        }

        out
    }

    fn content_blocks(
        &mut self,
        content: Content,
        meta: &RecordMeta,
        out: &mut Emission,
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
                        Some("tool_use") => out.calls.push(ToolCallMeta {
                            record_key: meta.key.clone(),
                            block_index: index,
                            native_call_id: block.id,
                            tool_name: block.name.unwrap_or_else(|| "unknown".into()),
                            turn_id: meta.turn_id.clone(),
                        }),
                        Some("tool_result") => {
                            out.results.push(self.tool_result(block, index, meta))
                        }
                        Some("image" | "document" | "thinking" | "redacted_thinking") => {}
                        Some(_) | None => self.counts.unknown_blocks += 1,
                    }
                }
            }
        }
        blocks
    }

    fn tool_result(&self, block: Block, index: u32, meta: &RecordMeta) -> ToolResultMeta {
        let call_id = block.call_id;
        let call = call_id.as_deref().and_then(|id| {
            ancestor_call(
                meta.parent_key.as_deref(),
                id,
                &self.parent_by_key,
                &self.calls_by_key,
            )
        });
        let turn_id = call
            .as_ref()
            .and_then(|(record, _)| self.turn_by_key.get(record).cloned())
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

    fn index_record(&mut self, meta: &RecordMeta, calls: &[ToolCallMeta]) {
        self.parent_by_key
            .insert(meta.key.clone(), meta.parent_key.clone());
        if let Some(turn_id) = &meta.turn_id {
            self.turn_by_key.insert(meta.key.clone(), turn_id.clone());
        }
        if !calls.is_empty() {
            self.calls_by_key.insert(
                meta.key.clone(),
                calls
                    .iter()
                    .filter_map(|call| {
                        call.native_call_id
                            .as_ref()
                            .map(|id| (id.clone(), call.block_index))
                    })
                    .collect(),
            );
        }
    }

    fn supersede(&mut self, id: String, blocks: usize, meta: &mut RecordMeta) -> Vec<String> {
        let supersedes = self.message_keys.get(&id).cloned().unwrap_or_default();
        if let Some(previous_blocks) = self.latest_blocks.insert(id.clone(), blocks) {
            self.counts.blocks_retained = self
                .counts
                .blocks_retained
                .saturating_sub(previous_blocks as u64);
        } else {
            self.counts.messages += 1;
        }
        self.counts.blocks_retained += blocks as u64;
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
        mut meta: RecordMeta,
        out: Emission,
        sink: &mut dyn RecordSink,
    ) -> Result<(), AdapterError> {
        let Emission {
            message,
            identity,
            calls,
            results,
        } = out;
        match (message, identity) {
            (Some(mut msg), Some(id)) => {
                msg.supersedes = self.supersede(id, msg.blocks.len(), &mut meta);
                sink.record(meta)?;
                sink.message(msg)?;
            }
            (Some(msg), None) => {
                self.counts.messages += 1;
                self.counts.blocks_retained += msg.blocks.len() as u64;
                sink.record(meta)?;
                sink.message(msg)?;
            }
            (None, _) => sink.record(meta)?,
        }

        for call in calls {
            sink.tool_call(call)?;
        }
        for result in results {
            sink.tool_result(result)?;
        }

        Ok(())
    }

    fn malformed_record(
        &mut self,
        line: &[u8],
        locator: SourceLocator,
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
            locator,
            digest: d,
        })?;
        sink.warning(ParseWarning {
            code: "invalid_record".into(),
            record_key: Some(key),
            locator: Some(locator),
            detail: "invalid complete JSONL record".into(),
        })?;

        self.counts.malformed_records += 1;
        self.counts.unknown_records += 1;
        self.coverage = Coverage::Partial;
        Ok(())
    }

    fn finish(self) -> ParsedSessionManifest {
        ParsedSessionManifest {
            native_session_id: self.sid.unwrap_or_else(|| "unknown".into()),
            native_title: None,
            cwd: self.cwd,
            session_kind: self.session_kind,
            parent_native_session: self.parent,
            lineage_verified: false,
            started_at: self.started_at,
            last_activity_at: self.last_activity_at,
            prefix_end: self.prefix,
            active_leaf_key: None,
            coverage: self.coverage,
            counts: self.counts,
        }
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

pub(super) fn parse_snapshot(
    source: &mut dyn Read,
    o: &ParseOptions,
    sink: &mut dyn RecordSink,
) -> Result<ParsedSessionManifest, AdapterError> {
    let mut reader = BufReader::new(source);
    let mut line = Vec::new();
    let mut state = ParseState::new();

    loop {
        if sink.cancelled().load(std::sync::atomic::Ordering::Relaxed) {
            return Err(AdapterError::Cancelled);
        }
        if state.offset >= o.observe_len || state.offset >= o.max_source_bytes {
            break;
        }

        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        let end = state.offset + n as u64;
        let complete = n > 0 && line.last() == Some(&b'\n');
        if !complete || end > o.observe_len || end > o.max_source_bytes {
            break;
        }
        if n as u64 > o.max_record_bytes {
            return Err(AdapterError::LimitExceeded);
        }

        state.handle_line(&line, o, sink)?;
    }

    Ok(state.finish())
}
