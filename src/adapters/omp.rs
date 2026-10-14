use super::{AdapterError, ParseOptions, ProbeResult, ReadSeek, RecordSink, SessionAdapter};
use crate::domain::*;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read, SeekFrom},
};

pub struct OmpAdapter;
const VERSION: &str = "omp-v3-title-slot-v1";
#[derive(Deserialize, Default)]
struct Entry {
    #[serde(rename = "type")]
    ty: Option<String>,
    id: Option<String>,
    #[serde(rename = "parentId")]
    parent: Option<String>,
    #[serde(rename = "v")]
    slot_version: Option<u64>,
    version: Option<u64>,
    timestamp: Option<String>,
    cwd: Option<String>,
    title: Option<String>,
    #[serde(rename = "parentSession")]
    parent_session: Option<String>,
    message: Option<Message>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct Message {
    role: Option<String>,
    #[serde(rename = "toolCallId")]
    call_id: Option<String>,
    #[serde(rename = "isError")]
    is_error: Option<bool>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct TextEntry {
    message: Option<TextMessage>,
}
#[derive(Deserialize, Default)]
struct TextMessage {
    content: Option<Content>,
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
    #[serde(rename = "textSignature")]
    signature: Option<Value>,
    name: Option<String>,
    id: Option<String>,
    #[serde(rename = "toolCallId")]
    call_id: Option<String>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
fn bad_detail() -> String {
    "record is malformed or unsupported".into()
}
fn millis(timestamp: Option<&str>) -> Option<i64> {
    timestamp.and_then(parse_timestamp_ms)
}
fn record(entry: &Entry, timestamp: Option<i64>, idx: u64, bytes: &[u8]) -> RecordMeta {
    let d = digest(bytes);
    let key = entry
        .id
        .clone()
        .unwrap_or_else(|| format!("fallback:{VERSION}:{}:{idx}", hex(&d)));
    RecordMeta {
        key,
        native_id: entry.id.clone(),
        parent_key: entry.parent.clone(),
        kind: RecordKind::Control,
        timestamp,
        turn_id: None,
        locator: SourceLocator {
            offset: idx,
            len: bytes.len() as u64,
        },
        digest: d,
    }
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
impl SessionAdapter for OmpAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Omp
    }
    fn parser_version(&self) -> &str {
        VERSION
    }
    fn probe(&self, b: &[u8]) -> ProbeResult {
        let first = b.split(|x| *x == b'\n').next().unwrap_or_default();
        let parsed = serde_json::from_slice::<Entry>(first);
        if let Ok(e) = parsed {
            if e.ty.as_deref() == Some("title") {
                return ProbeResult::Match {
                    format_version: Some("title-slot-v1".into()),
                };
            }
            if e.ty.as_deref() == Some("session") {
                return ProbeResult::Match {
                    format_version: e.version.map(|v| v.to_string()),
                };
            }
        }
        if b.windows(18).any(|w| w == b"\"type\":\"session\"") {
            ProbeResult::Match {
                format_version: None,
            }
        } else {
            ProbeResult::NoMatch
        }
    }
    fn parse_snapshot(
        &self,
        source: &mut dyn Read,
        o: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError> {
        let mut r = BufReader::new(source);
        let mut line = Vec::new();
        let (mut offset, mut prefix, mut counts) = (0u64, 0u64, ParseCounts::default());
        let (mut id, mut title, mut cwd, mut parent) = (None, None, None, None);
        let mut session_kind = SessionKind::Unknown;
        let (mut started_at, mut last_activity_at) = (None, None);
        let mut title_slot = false;
        let mut parent_by_key = HashMap::<String, Option<String>>::new();
        let mut turn_by_key = HashMap::<String, String>::new();
        let mut calls_by_key = HashMap::<String, Vec<(String, u32)>>::new();
        let mut coverage = Coverage::Complete;
        loop {
            if sink.cancelled().load(std::sync::atomic::Ordering::Relaxed) {
                return Err(AdapterError::Cancelled);
            }
            if offset >= o.observe_len || offset >= o.max_source_bytes {
                break;
            }
            line.clear();
            let n = r.read_until(b'\n', &mut line)?;
            if n == 0 {
                break;
            }
            if offset + n as u64 > o.observe_len || offset + n as u64 > o.max_source_bytes {
                break;
            }
            if line.len() as u64 > o.max_record_bytes {
                return Err(AdapterError::LimitExceeded);
            }
            if line.last() != Some(&b'\n') {
                break;
            }
            counts.bytes_read += n as u64;
            counts.records += 1;
            let loc = SourceLocator {
                offset,
                len: n as u64,
            };
            let parsed = serde_json::from_slice::<Entry>(&line);
            if let Ok(e) = parsed {
                let timestamp = millis(e.timestamp.as_deref());
                if let Some(timestamp) = timestamp {
                    started_at.get_or_insert(timestamp);
                    last_activity_at =
                        Some(last_activity_at.map_or(timestamp, |last: i64| last.max(timestamp)));
                }
                if e.ty.as_deref() == Some("session") {
                    started_at = timestamp.or(started_at);
                }
                if offset == 0 && e.ty.as_deref() == Some("title") {
                    title_slot = true;
                    if e.slot_version != Some(1) {
                        coverage = Coverage::Unsupported;
                    }
                    offset += n as u64;
                    prefix = offset;
                    continue;
                }
                if e.ty.as_deref() == Some("session") {
                    if e.version != Some(3) {
                        coverage = Coverage::Unsupported;
                    }
                    id = e.id.clone();
                    title = e.title.clone();
                    cwd = e.cwd.clone();
                    parent = e.parent_session.clone();
                    session_kind = if parent.is_some() {
                        SessionKind::Subagent
                    } else {
                        SessionKind::Interactive
                    };
                    let mut m = record(&e, timestamp, offset, &line);
                    m.kind = RecordKind::Header;
                    sink.record(m)?;
                    offset += n as u64;
                    prefix = offset;
                    continue;
                }
                let mut meta = record(&e, timestamp, offset, &line);
                let role = e
                    .message
                    .as_ref()
                    .and_then(|message| message.role.as_deref());
                let turn_id = if role == Some("user") {
                    Some(meta.key.clone())
                } else if matches!(role, Some("assistant" | "toolResult"))
                    || e.ty.as_deref() == Some("toolResult")
                {
                    ancestor_turn(meta.parent_key.as_deref(), &parent_by_key, &turn_by_key)
                } else {
                    None
                };
                if matches!(role, Some("user" | "assistant" | "toolResult"))
                    || e.ty.as_deref() == Some("toolResult")
                {
                    meta.turn_id = turn_id.clone();
                }
                let text_content = if matches!(role, Some("user" | "assistant")) {
                    serde_json::from_slice::<TextEntry>(&line)
                        .ok()
                        .and_then(|entry| entry.message)
                        .and_then(|message| message.content)
                } else {
                    None
                };
                let mut msg = None;
                let mut calls = Vec::new();
                let mut results = Vec::new();
                let result_message = |message: &Message, meta: &RecordMeta| {
                    let native_call_id = message.call_id.clone();
                    let call = native_call_id.as_deref().and_then(|id| {
                        ancestor_call(
                            meta.parent_key.as_deref(),
                            id,
                            &parent_by_key,
                            &calls_by_key,
                        )
                    });
                    ToolResultMeta {
                        record_key: meta.key.clone(),
                        block_index: 0,
                        native_call_id,
                        call,
                        is_error: message.is_error,
                        turn_id: meta.turn_id.clone(),
                    }
                };
                match e.ty.as_deref() {
                    Some("message") => {
                        if let Some(m) = e.message {
                            let role = m.role.as_deref();
                            let phase = text_content.as_ref().and_then(|content| match content {
                                Content::Blocks(blocks) => blocks
                                    .iter()
                                    .find_map(|block| {
                                        block.signature.as_ref()?.get("phase")?.as_str()
                                    })
                                    .map(str::to_owned),
                                Content::Text(_) => None,
                            });
                            if role == Some("toolResult") {
                                meta.kind = RecordKind::ToolResult;
                                results.push(result_message(&m, &meta));
                            } else if matches!(role, Some("user" | "assistant")) {
                                let mut blocks = Vec::new();
                                if let Some(content) = text_content {
                                    match content {
                                        Content::Text(text) => blocks.push(TextBlock {
                                            index: 0,
                                            origin: BlockOrigin::Authored,
                                            text,
                                        }),
                                        Content::Blocks(content_blocks) => {
                                            for (i, block) in content_blocks.into_iter().enumerate()
                                            {
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
                                                    Some("toolCall") => {
                                                        calls.push(ToolCallMeta {
                                                            record_key: meta.key.clone(),
                                                            block_index: i as u32,
                                                            native_call_id: block
                                                                .id
                                                                .clone()
                                                                .or(block.call_id),
                                                            tool_name: block.name.unwrap_or_else(
                                                                || "unknown".into(),
                                                            ),
                                                            turn_id: turn_id.clone(),
                                                        });
                                                    }
                                                    Some("image") => {}
                                                    Some(_) | None => counts.unknown_blocks += 1,
                                                }
                                            }
                                        }
                                    }
                                }
                                if !blocks.is_empty() {
                                    meta.kind = RecordKind::Message;
                                    msg = Some(MessageEntry {
                                        record_key: meta.key.clone(),
                                        role: if role == Some("user") {
                                            Role::User
                                        } else {
                                            Role::Assistant
                                        },
                                        phase,
                                        blocks,
                                        attachments: Vec::new(),
                                        supersedes: vec![],
                                    });
                                } else if !calls.is_empty() {
                                    meta.kind = RecordKind::ToolCall;
                                } else {
                                    meta.kind = RecordKind::Control;
                                }
                            }
                        }
                    }
                    Some("compaction" | "branch_summary") => meta.kind = RecordKind::Summary,
                    Some("toolResult") => {
                        meta.kind = RecordKind::ToolResult;
                        if let Some(message) = e.message.as_ref() {
                            results.push(result_message(message, &meta));
                        } else {
                            results.push(ToolResultMeta {
                                record_key: meta.key.clone(),
                                block_index: 0,
                                native_call_id: None,
                                call: None,
                                is_error: None,
                                turn_id: meta.turn_id.clone(),
                            });
                        }
                    }
                    Some("title") => meta.kind = RecordKind::Header,
                    Some(_) => {
                        meta.kind = RecordKind::Control;
                        counts.unknown_records += 1
                    }
                    None => {
                        meta.kind = RecordKind::Unknown;
                        counts.unknown_records += 1
                    }
                }
                sink.record(meta.clone())?;
                if let Some(turn_id) = turn_id {
                    turn_by_key.insert(meta.key.clone(), turn_id);
                }
                parent_by_key.insert(meta.key.clone(), meta.parent_key.clone());
                if let Some(m) = msg {
                    counts.messages += 1;
                    counts.blocks_retained += m.blocks.len() as u64;
                    sink.message(m)?;
                }
                if !calls.is_empty() {
                    calls_by_key.insert(
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
                for c in calls {
                    sink.tool_call(c)?;
                }
                for t in results {
                    sink.tool_result(t)?;
                }
                offset += n as u64;
                prefix = offset;
            } else {
                let d = digest(&line);
                let key = format!(
                    "fallback:{VERSION}:{}:{}",
                    hex(&d),
                    counts.malformed_records
                );
                let m = RecordMeta {
                    key: key.clone(),
                    native_id: None,
                    parent_key: None,
                    kind: RecordKind::Unknown,
                    timestamp: None,
                    turn_id: None,
                    locator: loc,
                    digest: d,
                };
                sink.record(m.clone())?;
                sink.warning(ParseWarning {
                    code: "invalid_record".into(),
                    record_key: Some(key),
                    locator: Some(loc),
                    detail: bad_detail(),
                })?;
                counts.malformed_records += 1;
                counts.unknown_records += 1;
                coverage = Coverage::Partial;
                offset += n as u64;
                prefix = offset;
            }
        }
        let _ = title_slot;
        Ok(ParsedSessionManifest {
            native_session_id: id.unwrap_or_else(|| "unknown".into()),
            native_title: title,
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
        s: &mut dyn ReadSeek,
        l: &SourceLocator,
        e: &Digest,
        max: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        if l.len > max {
            return Err(AdapterError::LimitExceeded);
        }
        s.seek(SeekFrom::Start(l.offset))?;
        let mut raw = vec![0; l.len as usize];
        s.read_exact(&mut raw)?;
        let d = digest(&raw);
        if &d != e {
            return Err(AdapterError::LocatorMismatch("digest changed".into()));
        }
        let line = raw.strip_suffix(b"\n").unwrap_or(&raw);
        let ent = serde_json::from_slice::<Entry>(line).ok();
        let json = serde_json::from_slice::<Value>(line).ok();
        let text_entry = serde_json::from_slice::<TextEntry>(line).ok();
        let native_id = ent.as_ref().and_then(|entry| entry.id.clone());
        let mut blocks = Vec::new();
        if let (Some(ent), Some(json)) = (ent.as_ref(), json.as_ref()) {
            if let Some(message) = ent.message.as_ref() {
                let role = message.role.as_deref();
                if role == Some("toolResult") || ent.ty.as_deref() == Some("toolResult") {
                    if let Some(value) = json
                        .pointer("/message/content")
                        .filter(|value| !value.is_null())
                    {
                        let (bytes, fidelity) = if let Some(text) = value.as_str() {
                            (text.as_bytes().to_vec(), Fidelity::Verbatim)
                        } else if let Some(text) = value
                            .as_array()
                            .and_then(|items| (items.len() == 1).then(|| &items[0]))
                            .and_then(|item| item.get("text"))
                            .and_then(Value::as_str)
                        {
                            (text.as_bytes().to_vec(), Fidelity::Verbatim)
                        } else {
                            (
                                serde_json::to_vec(value).map_err(|_| {
                                    AdapterError::Unsupported("invalid tool result".into())
                                })?,
                                Fidelity::NormalizedJson,
                            )
                        };
                        blocks.push(ResolvedBlock {
                            index: 0,
                            kind: BlockKind::ToolResult,
                            fidelity,
                            bytes,
                        });
                    }
                } else if matches!(role, Some("user" | "assistant")) {
                    match text_entry
                        .as_ref()
                        .and_then(|t| t.message.as_ref())
                        .and_then(|m| m.content.as_ref())
                    {
                        Some(Content::Text(text)) => blocks.push(ResolvedBlock {
                            index: 0,
                            kind: BlockKind::Text,
                            fidelity: Fidelity::Verbatim,
                            bytes: text.as_bytes().to_vec(),
                        }),
                        Some(Content::Blocks(content_blocks)) => {
                            for (i, block) in content_blocks.iter().enumerate() {
                                match block.ty.as_deref() {
                                    Some("text") => {
                                        if let Some(text) = &block.text {
                                            blocks.push(ResolvedBlock {
                                                index: i as u32,
                                                kind: BlockKind::Text,
                                                fidelity: Fidelity::Verbatim,
                                                bytes: text.as_bytes().to_vec(),
                                            });
                                        }
                                    }
                                    Some("toolCall") => {
                                        if let Some(arguments) =
                                            json.pointer(&format!("/message/content/{i}/arguments"))
                                        {
                                            let (bytes, fidelity) =
                                                if let Some(text) = arguments.as_str() {
                                                    (text.as_bytes().to_vec(), Fidelity::Verbatim)
                                                } else {
                                                    (
                                                        serde_json::to_vec(arguments).map_err(
                                                            |_| {
                                                                AdapterError::Unsupported(
                                                                    "invalid tool arguments".into(),
                                                                )
                                                            },
                                                        )?,
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
                                    _ => {}
                                }
                            }
                        }
                        None => {}
                    }
                }
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
