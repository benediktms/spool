use super::{AdapterError, ParseOptions, ProbeResult, ReadSeek, RecordSink, SessionAdapter};
use crate::domain::*;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read, SeekFrom},
};

pub struct ClaudeAdapter;
const VERSION: &str = "claude-jsonl-session-browser-2026-10.1";
#[derive(Deserialize, Default)]
struct Line {
    #[serde(rename = "type")]
    ty: Option<String>,
    uuid: Option<String>,
    timestamp: Option<String>,
    #[serde(rename = "parentUuid")]
    parent: Option<String>,
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    cwd: Option<String>,
    #[serde(rename = "parentSessionId")]
    parent_session: Option<String>,
    #[serde(rename = "isSidechain")]
    is_sidechain: Option<bool>,
    #[serde(rename = "isMeta")]
    is_meta: Option<bool>,
    message: Option<Message>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct Message {
    role: Option<String>,
    content: Option<Content>,
    id: Option<String>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Blocks(Vec<Block>),
}
#[derive(Deserialize, Default)]
struct Block {
    #[serde(rename = "type")]
    ty: Option<String>,
    text: Option<String>,
    id: Option<String>,
    name: Option<String>,
    #[serde(rename = "tool_use_id")]
    call_id: Option<String>,
    is_error: Option<bool>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
fn hex(d: &Digest) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
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

impl SessionAdapter for ClaudeAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Claude
    }
    fn parser_version(&self) -> &str {
        VERSION
    }
    fn probe(&self, bytes: &[u8]) -> ProbeResult {
        let first = bytes.split(|b| *b == b'\n').next().unwrap_or_default();
        match serde_json::from_slice::<Line>(first) {
            Ok(x)
                if matches!(
                    x.ty.as_deref(),
                    Some("user" | "assistant" | "summary" | "system" | "progress")
                ) =>
            {
                ProbeResult::Match {
                    format_version: Some("jsonl-session-browser-shape".into()),
                }
            }
            _ => ProbeResult::NoMatch,
        }
    }
    fn parse_snapshot(
        &self,
        source: &mut dyn Read,
        o: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError> {
        let mut reader = BufReader::new(source);
        let mut line = Vec::new();
        let (mut offset, mut prefix, mut counts) = (0u64, 0u64, ParseCounts::default());
        let (mut sid, mut cwd, mut parent) = (None, None, None);
        let mut occurrences = HashMap::<String, usize>::new();
        let mut message_keys = HashMap::<String, Vec<String>>::new();
        let mut latest_blocks = HashMap::<String, usize>::new();
        let mut parent_by_key = HashMap::<String, Option<String>>::new();
        let mut latest_message_parent = HashMap::<String, Option<String>>::new();
        let mut turn_by_key = HashMap::<String, String>::new();
        let mut calls_by_key = HashMap::<String, Vec<(String, u32)>>::new();
        let mut session_kind = SessionKind::Unknown;
        let mut coverage = Coverage::Complete;
        let (mut started_at, mut last_activity_at): (Option<i64>, Option<i64>) = (None, None);
        loop {
            if sink.cancelled().load(std::sync::atomic::Ordering::Relaxed) {
                return Err(AdapterError::Cancelled);
            }
            if offset >= o.observe_len || offset >= o.max_source_bytes {
                break;
            }
            line.clear();
            let n = reader.read_until(b'\n', &mut line)?;
            if n == 0
                || line.last() != Some(&b'\n')
                || offset + n as u64 > o.observe_len
                || offset + n as u64 > o.max_source_bytes
            {
                break;
            }
            if n as u64 > o.max_record_bytes {
                return Err(AdapterError::LimitExceeded);
            }
            counts.bytes_read += n as u64;
            counts.records += 1;
            let locator = SourceLocator {
                offset,
                len: n as u64,
            };
            let decoded = serde_json::from_slice::<Line>(&line);
            if let Ok(x) = decoded {
                let d = digest(&line);
                let timestamp = x.timestamp.as_deref().and_then(parse_timestamp_ms);
                if let Some(timestamp) = timestamp {
                    started_at.get_or_insert(timestamp);
                    last_activity_at =
                        Some(last_activity_at.map_or(timestamp, |last| last.max(timestamp)));
                }
                let native_id = x.uuid.clone();
                let key = native_id
                    .as_ref()
                    .map(|id| {
                        let occurrence = occurrences.entry(id.clone()).or_default();
                        let key = if *occurrence == 0 {
                            id.clone()
                        } else {
                            format!("{id}~{occurrence}")
                        };
                        *occurrence += 1;
                        key
                    })
                    .unwrap_or_else(|| {
                        format!("fallback:{VERSION}:{}:{}", hex(&d), counts.records)
                    });
                let mut meta = RecordMeta {
                    key: key.clone(),
                    native_id: native_id.clone(),
                    parent_key: x.parent.clone(),
                    kind: RecordKind::Control,
                    timestamp,
                    turn_id: None,
                    locator,
                    digest: d,
                };
                if sid.is_none() {
                    sid = x.session_id.clone();
                    cwd = x.cwd.clone();
                    parent = x.parent_session.clone();
                }
                if x.parent_session.is_some() || (sid.is_none() && x.is_sidechain == Some(true)) {
                    session_kind = SessionKind::Subagent;
                } else if x.session_id.is_some() && session_kind == SessionKind::Unknown {
                    session_kind = SessionKind::Interactive;
                }
                let role = x
                    .message
                    .as_ref()
                    .and_then(|message| message.role.as_deref())
                    .or(x.ty.as_deref());
                let turn_id = if session_kind == SessionKind::Subagent {
                    None
                } else if role == Some("user") {
                    Some(key.clone())
                } else if role == Some("assistant") {
                    ancestor_turn(x.parent.as_deref(), &parent_by_key, &turn_by_key)
                } else {
                    None
                };
                meta.turn_id = turn_id.clone();
                let mut message = None;
                let mut message_identity = None;
                let mut calls = Vec::new();
                let mut results = Vec::new();
                match x.ty.as_deref() {
                    Some("user" | "assistant")
                        if x.is_meta != Some(true)
                            && x.is_sidechain != Some(true)
                            && (session_kind != SessionKind::Subagent || o.include_subagents) =>
                    {
                        if let Some(m) = x.message {
                            let role = m.role.as_deref().or(x.ty.as_deref()).unwrap_or("");
                            message_identity = m
                                .id
                                .as_deref()
                                .map(|id| format!("message:{id}"))
                                .or_else(|| native_id.as_deref().map(|id| format!("uuid:{id}")));
                            if matches!(role, "user" | "assistant") {
                                let mut blocks = Vec::new();
                                if let Some(content) = m.content {
                                    match content {
                                        Content::Text(text) => blocks.push(TextBlock {
                                            index: 0,
                                            origin: BlockOrigin::Authored,
                                            text,
                                        }),
                                        Content::Blocks(blocks_in) => {
                                            for (i, block) in blocks_in.into_iter().enumerate() {
                                                match block.ty.as_deref() {
                                                    Some("text") => {
                                                        if let Some(text) = block.text {
                                                            blocks.push(TextBlock {
                                                                index: i as u32,
                                                                origin: BlockOrigin::Authored,
                                                                text,
                                                            });
                                                        }
                                                    }
                                                    Some("tool_use") => calls.push(ToolCallMeta {
                                                        record_key: key.clone(),
                                                        block_index: i as u32,
                                                        native_call_id: block.id,
                                                        tool_name: block
                                                            .name
                                                            .unwrap_or_else(|| "unknown".into()),
                                                        turn_id: turn_id.clone(),
                                                    }),
                                                    Some("tool_result") => {
                                                        let call_id = block.call_id;
                                                        let call =
                                                            call_id.as_deref().and_then(|id| {
                                                                ancestor_call(
                                                                    x.parent.as_deref(),
                                                                    id,
                                                                    &parent_by_key,
                                                                    &calls_by_key,
                                                                )
                                                            });
                                                        let result_turn_id = call
                                                            .as_ref()
                                                            .and_then(|(record, _)| {
                                                                turn_by_key.get(record).cloned()
                                                            })
                                                            .or_else(|| turn_id.clone());
                                                        results.push(ToolResultMeta {
                                                            record_key: key.clone(),
                                                            block_index: i as u32,
                                                            native_call_id: call_id,
                                                            call,
                                                            is_error: block.is_error,
                                                            turn_id: result_turn_id,
                                                        })
                                                    }
                                                    Some(
                                                        "image" | "document" | "thinking"
                                                        | "redacted_thinking",
                                                    ) => {}
                                                    Some(_) | None => counts.unknown_blocks += 1,
                                                }
                                            }
                                        }
                                    }
                                }
                                let has_text = !blocks.is_empty();
                                if !has_text && !results.is_empty() {
                                    let result_turns: HashSet<_> = results
                                        .iter()
                                        .filter_map(|result| result.turn_id.as_deref())
                                        .collect();
                                    if result_turns.len() == 1
                                        && let Some(turn_id) = result_turns.iter().next()
                                    {
                                        meta.turn_id = Some((*turn_id).to_owned());
                                    }
                                }
                                if has_text
                                    || message_identity
                                        .as_ref()
                                        .is_some_and(|id| message_keys.contains_key(id))
                                {
                                    message = Some(MessageEntry {
                                        record_key: key.clone(),
                                        role: if role == "user" {
                                            Role::User
                                        } else {
                                            Role::Assistant
                                        },
                                        phase: None,
                                        blocks,
                                        attachments: Vec::new(),
                                        supersedes: Vec::new(),
                                    });
                                }
                                if has_text {
                                    meta.kind = RecordKind::Message;
                                } else if !calls.is_empty() {
                                    meta.kind = RecordKind::ToolCall;
                                } else if !results.is_empty() {
                                    meta.kind = RecordKind::ToolResult;
                                }
                            }
                        }
                    }
                    Some("user" | "assistant") => meta.kind = RecordKind::Control,
                    Some("summary" | "compact_boundary") => meta.kind = RecordKind::Summary,
                    Some(
                        "system"
                        | "progress"
                        | "file-history-snapshot"
                        | "last-prompt"
                        | "queue-operation",
                    ) => meta.kind = RecordKind::Control,
                    Some(_) => {
                        meta.kind = RecordKind::Unknown;
                        counts.unknown_records += 1;
                        coverage = Coverage::Partial;
                    }
                    None => {
                        meta.kind = RecordKind::Unknown;
                        counts.unknown_records += 1;
                        coverage = Coverage::Partial;
                    }
                }
                parent_by_key.insert(key.clone(), x.parent.clone());
                if let Some(turn_id) = meta.turn_id.clone() {
                    turn_by_key.insert(key.clone(), turn_id);
                }
                if !calls.is_empty() {
                    calls_by_key.insert(
                        key.clone(),
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
                if let Some(mut msg) = message {
                    if let Some(id) = message_identity {
                        let supersedes = message_keys.get(&id).cloned().unwrap_or_default();
                        if let Some(previous_blocks) =
                            latest_blocks.insert(id.clone(), msg.blocks.len())
                        {
                            counts.blocks_retained = counts
                                .blocks_retained
                                .saturating_sub(previous_blocks as u64);
                        } else {
                            counts.messages += 1;
                        }
                        counts.blocks_retained += msg.blocks.len() as u64;
                        if meta.parent_key.is_none()
                            && let Some(inherited) = latest_message_parent.get(&id)
                        {
                            meta.parent_key = inherited.clone();
                        }
                        latest_message_parent.insert(id.clone(), meta.parent_key.clone());
                        // physical parents stay as written: superseded versions and the records between them remain on the record path
                        message_keys.entry(id).or_default().push(key.clone());
                        msg.supersedes = supersedes;
                        sink.record(meta.clone())?;
                        sink.message(msg)?;
                    } else {
                        counts.messages += 1;
                        counts.blocks_retained += msg.blocks.len() as u64;
                        sink.record(meta.clone())?;
                        sink.message(msg)?;
                    }
                } else {
                    sink.record(meta.clone())?;
                }
                for call in calls {
                    sink.tool_call(call)?;
                }
                for result in results {
                    sink.tool_result(result)?;
                }
            } else {
                let d = digest(&line);
                let key = format!(
                    "fallback:{VERSION}:{}:{}",
                    hex(&d),
                    counts.malformed_records
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
                counts.malformed_records += 1;
                counts.unknown_records += 1;
                coverage = Coverage::Partial;
            }
            offset += n as u64;
            prefix = offset;
        }
        Ok(ParsedSessionManifest {
            native_session_id: sid.unwrap_or_else(|| "unknown".into()),
            native_title: None,
            cwd,
            session_kind,
            parent_native_session: parent.clone(),
            lineage_verified: false,
            started_at,
            last_activity_at,
            prefix_end: prefix,
            active_leaf_key: None,
            coverage,
            counts,
        })
    }
    fn resolve_locator(
        &self,
        source: &mut dyn ReadSeek,
        locator: &SourceLocator,
        expected: &Digest,
        max: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        if locator.len > max {
            return Err(AdapterError::LimitExceeded);
        }
        source.seek(SeekFrom::Start(locator.offset))?;
        let mut raw = vec![0; locator.len as usize];
        source.read_exact(&mut raw)?;
        let d = digest(&raw);
        if &d != expected {
            return Err(AdapterError::LocatorMismatch("digest changed".into()));
        }
        let line = raw.strip_suffix(b"\n").unwrap_or(&raw);
        let x = serde_json::from_slice::<Line>(line).ok();
        let json = serde_json::from_slice::<Value>(line).ok();
        let native_id = x.as_ref().and_then(|record| record.uuid.clone());
        let mut blocks = Vec::new();
        if let (Some(x), Some(json)) = (x.as_ref(), json.as_ref())
            && let Some(message) = x.message.as_ref()
        {
            let role = message.role.as_deref().or(x.ty.as_deref());
            match message.content.as_ref() {
                Some(Content::Text(text)) if matches!(role, Some("user" | "assistant")) => {
                    blocks.push(ResolvedBlock {
                        index: 0,
                        kind: BlockKind::Text,
                        fidelity: Fidelity::Verbatim,
                        bytes: text.as_bytes().to_vec(),
                    });
                }
                Some(Content::Blocks(content)) => {
                    for (i, block) in content.iter().enumerate() {
                        match block.ty.as_deref() {
                            Some("text") if matches!(role, Some("user" | "assistant")) => {
                                if let Some(text) = &block.text {
                                    blocks.push(ResolvedBlock {
                                        index: i as u32,
                                        kind: BlockKind::Text,
                                        fidelity: Fidelity::Verbatim,
                                        bytes: text.as_bytes().to_vec(),
                                    });
                                }
                            }
                            Some("tool_use") => {
                                if let Some(input) =
                                    json.pointer(&format!("/message/content/{i}/input"))
                                {
                                    let (bytes, fidelity) = if let Some(text) = input.as_str() {
                                        (text.as_bytes().to_vec(), Fidelity::Verbatim)
                                    } else {
                                        (
                                            serde_json::to_vec(input).map_err(|_| {
                                                AdapterError::Unsupported(
                                                    "invalid Claude tool input".into(),
                                                )
                                            })?,
                                            Fidelity::NormalizedJson,
                                        )
                                    };
                                    blocks.push(ResolvedBlock {
                                        index: i as u32,
                                        kind: BlockKind::ToolCall,
                                        fidelity,
                                        bytes,
                                    });
                                }
                            }
                            Some("tool_result") => {
                                if let Some(result) =
                                    json.pointer(&format!("/message/content/{i}/content"))
                                {
                                    let (bytes, fidelity) = if let Some(text) = result.as_str() {
                                        (text.as_bytes().to_vec(), Fidelity::Verbatim)
                                    } else if let Some(text) = result
                                        .as_array()
                                        .and_then(|items| (items.len() == 1).then(|| &items[0]))
                                        .and_then(|item| item.get("text"))
                                        .and_then(Value::as_str)
                                    {
                                        (text.as_bytes().to_vec(), Fidelity::Verbatim)
                                    } else {
                                        (
                                            serde_json::to_vec(result).map_err(|_| {
                                                AdapterError::Unsupported(
                                                    "invalid Claude tool result".into(),
                                                )
                                            })?,
                                            Fidelity::NormalizedJson,
                                        )
                                    };
                                    blocks.push(ResolvedBlock {
                                        index: i as u32,
                                        kind: BlockKind::ToolResult,
                                        fidelity,
                                        bytes,
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(ResolvedRecord {
            raw,
            digest: d,
            native_id,
            blocks,
        })
    }
}
