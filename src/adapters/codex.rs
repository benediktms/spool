use super::{AdapterError, ParseOptions, ProbeResult, ReadSeek, RecordSink, SessionAdapter};
use crate::domain::*;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Read, SeekFrom},
};
pub struct CodexAdapter;
const VERSION: &str = "codex-rollout-cab328f2";
#[derive(Deserialize, Default)]
struct Line {
    timestamp: Option<String>,
    ordinal: Option<u64>,
    #[serde(rename = "type")]
    ty: Option<String>,
    turn_id: Option<String>,
    payload: Option<Payload>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct TurnMetadata {
    turn_id: Option<String>,
}
#[derive(Deserialize, Default)]
struct Payload {
    id: Option<String>,
    thread_id: Option<String>,
    conversation_id: Option<String>,
    forked_from_id: Option<String>,
    parent_thread_id: Option<String>,
    source: Option<Value>,
    cwd: Option<String>,
    #[serde(rename = "type")]
    ty: Option<String>,
    role: Option<String>,
    phase: Option<String>,
    content: Option<Vec<Content>>,
    message: Option<String>,
    text: Option<String>,
    call_id: Option<String>,
    name: Option<String>,
    #[serde(rename = "is_error")]
    is_error: Option<bool>,
    turn_id: Option<String>,
    #[serde(rename = "internal_chat_message_metadata_passthrough")]
    turn_metadata: Option<TurnMetadata>,
    history_mode: Option<String>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct Content {
    #[serde(rename = "type")]
    ty: Option<String>,
    text: Option<String>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
fn hex(d: &Digest) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
}
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
impl SessionAdapter for CodexAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Codex
    }
    fn parser_version(&self) -> &str {
        VERSION
    }
    fn probe(&self, b: &[u8]) -> ProbeResult {
        let first = b.split(|x| *x == b'\n').next().unwrap_or_default();
        match serde_json::from_slice::<Line>(first) {
            Ok(x)
                if matches!(
                    x.ty.as_deref(),
                    Some("session_meta" | "response_item" | "event_msg")
                ) =>
            {
                ProbeResult::Match {
                    format_version: Some("rollout-line".into()),
                }
            }
            _ => {
                if b.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
                    || b.starts_with(&[0x1f, 0x8b])
                    || b.starts_with(b"PK\x03\x04")
                {
                    ProbeResult::Unsupported {
                        reason: "compressed or archived rollout is not supported".into(),
                    }
                } else {
                    ProbeResult::NoMatch
                }
            }
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
        let (mut id, mut cwd): (Option<String>, Option<String>) = (None, None);
        let (mut parent_native_session, mut session_kind) = (None, SessionKind::Unknown);
        let mut calls_by_turn = HashMap::<(String, String), Option<(String, u32)>>::new();
        let mut occurrences = HashMap::<String, usize>::new();
        let mut seen = HashSet::new();
        // Codex's pinned rollout item schema has no per-item parent field; persisted record order is linear.
        let mut previous_record_key: Option<String> = None;
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
            let n = r.read_until(b'\n', &mut line)?;
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
            let loc = SourceLocator {
                offset,
                len: n as u64,
            };
            let decoded = serde_json::from_slice::<Line>(&line);
            if let Ok(x) = decoded {
                let d = digest(&line);
                let timestamp = ms(x.timestamp.as_deref());
                if let Some(timestamp) = timestamp {
                    if x.ty.as_deref() == Some("session_meta") {
                        started_at.get_or_insert(timestamp);
                    }
                    last_activity_at =
                        Some(last_activity_at.map_or(timestamp, |last| last.max(timestamp)));
                }
                let native_id = x.payload.as_ref().and_then(|payload| payload.id.clone());
                let identity = native_id
                    .clone()
                    .or_else(|| x.ordinal.map(|ordinal| ordinal.to_string()));
                let key = identity.map_or_else(
                    || format!("fallback:{VERSION}:{}:{}", hex(&d), counts.records),
                    |identity| {
                        let occurrence = occurrences.entry(identity.clone()).or_default();
                        let key = if *occurrence == 0 {
                            identity
                        } else {
                            format!("{identity}~{occurrence}")
                        };
                        *occurrence += 1;
                        key
                    },
                );
                let mut meta = RecordMeta {
                    key: key.clone(),
                    native_id: native_id.clone(),
                    parent_key: previous_record_key.clone(),
                    kind: RecordKind::Control,
                    timestamp,
                    turn_id: None,
                    locator: loc,
                    digest: d,
                };
                let p = x.payload.unwrap_or_default();
                let turn_id = x.turn_id.clone().or_else(|| p.turn_id.clone()).or_else(|| {
                    p.turn_metadata
                        .as_ref()
                        .and_then(|metadata| metadata.turn_id.clone())
                });
                if matches!(
                    p.ty.as_deref(),
                    Some("message" | "function_call" | "function_call_output")
                ) || x.ty.as_deref() == Some("event_msg")
                {
                    meta.turn_id = turn_id.clone();
                }
                let mut calls = Vec::new();
                let mut results = Vec::new();
                match x.ty.as_deref() {
                    Some("session_meta") => {
                        id = p
                            .thread_id
                            .clone()
                            .or(p.conversation_id.clone())
                            .or(p.id.clone());
                        cwd = p.cwd.clone();
                        parent_native_session = p.parent_thread_id.clone().or_else(|| {
                            p.forked_from_id.clone().or_else(|| {
                                p.source
                                    .as_ref()
                                    .and_then(|source| {
                                        source.pointer("/subagent/thread_spawn/parent_thread_id")
                                    })
                                    .and_then(Value::as_str)
                                    .map(str::to_owned)
                            })
                        });
                        session_kind = match p.source.as_ref() {
                            Some(Value::Object(source)) if source.contains_key("subagent") => {
                                SessionKind::Subagent
                            }
                            Some(Value::String(source))
                                if matches!(source.as_str(), "cli" | "vscode" | "exec" | "mcp") =>
                            {
                                SessionKind::Interactive
                            }
                            _ => SessionKind::Unknown,
                        };
                        if p.history_mode
                            .as_deref()
                            .is_some_and(|mode| !matches!(mode, "legacy" | "paginated"))
                        {
                            coverage = Coverage::Unsupported;
                        }
                    }
                    Some("response_item") => {
                        if p.ty.as_deref() == Some("message") {
                            if matches!(p.role.as_deref(), Some("user" | "assistant")) {
                                let mut blocks = Vec::new();
                                for (i, c) in p.content.unwrap_or_default().into_iter().enumerate()
                                {
                                    if matches!(
                                        c.ty.as_deref(),
                                        Some("input_text" | "output_text" | "text")
                                    ) {
                                        if let Some(t) = c.text {
                                            blocks.push(TextBlock {
                                                index: i as u32,
                                                origin: BlockOrigin::Authored,
                                                text: t,
                                            });
                                        }
                                    } else {
                                        counts.unknown_blocks += 1;
                                    }
                                }
                                if !blocks.is_empty() {
                                    meta.kind = RecordKind::Message;
                                    let rid = meta.key.clone();
                                    let phase = p.phase.clone();
                                    let msg = MessageEntry {
                                        record_key: rid,
                                        role: if p.role.as_deref() == Some("user") {
                                            Role::User
                                        } else {
                                            Role::Assistant
                                        },
                                        phase,
                                        blocks,
                                        attachments: Vec::new(),
                                        supersedes: vec![],
                                    };
                                    let identity =
                                        native_id.clone().unwrap_or_else(|| msg.record_key.clone());
                                    if seen.insert(identity) {
                                        counts.messages += 1;
                                        counts.blocks_retained += msg.blocks.len() as u64;
                                        previous_record_key = Some(meta.key.clone());
                                        sink.record(meta)?;
                                        sink.message(msg)?;
                                        offset += n as u64;
                                        prefix = offset;
                                        continue;
                                    }
                                }
                            }
                        } else if matches!(
                            p.ty.as_deref(),
                            Some("compaction" | "context_compaction")
                        ) {
                            meta.kind = RecordKind::Summary;
                        } else if p.ty.as_deref() == Some("function_call") {
                            meta.kind = RecordKind::ToolCall;
                            calls.push(ToolCallMeta {
                                record_key: meta.key.clone(),
                                block_index: 0,
                                native_call_id: p.call_id.clone(),
                                tool_name: p.name.clone().unwrap_or_else(|| "unknown".into()),
                                turn_id: turn_id.clone(),
                            });
                        } else if p.ty.as_deref() == Some("function_call_output") {
                            meta.kind = RecordKind::ToolResult;
                            let native_call_id = p.call_id.clone();
                            let call = turn_id.as_ref().and_then(|turn| {
                                native_call_id.as_ref().and_then(|call_id| {
                                    calls_by_turn
                                        .get(&(turn.clone(), call_id.clone()))
                                        .and_then(Clone::clone)
                                })
                            });
                            results.push(ToolResultMeta {
                                record_key: meta.key.clone(),
                                block_index: 0,
                                native_call_id,
                                call,
                                is_error: p.is_error,
                                turn_id: turn_id.clone(),
                            });
                        } else if matches!(
                            p.ty.as_deref(),
                            Some(
                                "local_shell_call" | "custom_tool_call" | "custom_tool_call_output"
                            )
                        ) {
                            meta.kind = if p.ty.as_deref() == Some("custom_tool_call_output") {
                                RecordKind::ToolResult
                            } else {
                                RecordKind::ToolCall
                            };
                        } else if p.ty.is_some() {
                            meta.kind = RecordKind::Control;
                            counts.unknown_records += 1;
                        } else {
                            meta.kind = RecordKind::Unknown;
                            counts.unknown_records += 1;
                            coverage = Coverage::Partial;
                        }
                    }
                    Some("event_msg") => {
                        let event = p.ty.as_deref().unwrap_or("");
                        if matches!(event, "UserMessage" | "AgentMessage") {
                            let content = p.message.or(p.text);
                            if let Some(text) = content.filter(|t| !t.is_empty()) {
                                let identity = p.id.unwrap_or_else(|| key.clone());
                                if seen.insert(identity) {
                                    meta.kind = RecordKind::Message;
                                    let role = if event == "UserMessage" {
                                        Role::User
                                    } else {
                                        Role::Assistant
                                    };
                                    let msg = MessageEntry {
                                        record_key: meta.key.clone(),
                                        role,
                                        phase: None,
                                        blocks: vec![TextBlock {
                                            index: 0,
                                            origin: BlockOrigin::Authored,
                                            text,
                                        }],
                                        attachments: Vec::new(),
                                        supersedes: vec![],
                                    };
                                    counts.messages += 1;
                                    counts.blocks_retained += 1;
                                    previous_record_key = Some(meta.key.clone());
                                    sink.record(meta)?;
                                    sink.message(msg)?;
                                    offset += n as u64;
                                    prefix = offset;
                                    continue;
                                }
                            } else {
                                meta.kind = RecordKind::Unknown;
                                counts.unknown_records += 1;
                                coverage = Coverage::Partial;
                            }
                        } else {
                            meta.kind = RecordKind::Control;
                        }
                    }
                    _ => {
                        meta.kind = RecordKind::Unknown;
                        counts.unknown_records += 1;
                        coverage = Coverage::Partial;
                    }
                }
                if x.ty.as_deref() == Some("response_item")
                    && let Some(ty) = p.ty.as_deref()
                {
                    if canonical_response_item(ty) {
                        if meta.kind == RecordKind::Control {
                            counts.unknown_records = counts.unknown_records.saturating_sub(1);
                        }
                    } else {
                        meta.kind = RecordKind::Unknown;
                        coverage = Coverage::Partial;
                    }
                }
                for call in &calls {
                    if let (Some(turn_id), Some(call_id)) =
                        (call.turn_id.as_ref(), call.native_call_id.as_ref())
                    {
                        let link = calls_by_turn
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
                previous_record_key = Some(meta.key.clone());
                sink.record(meta)?;
                for call in calls {
                    sink.tool_call(call)?;
                }
                for result in results {
                    sink.tool_result(result)?;
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
                    parent_key: previous_record_key.clone(),
                    kind: RecordKind::Unknown,
                    timestamp: None,
                    turn_id: None,
                    locator: loc,
                    digest: d,
                };
                previous_record_key = Some(key.clone());
                sink.record(m)?;
                sink.warning(ParseWarning {
                    code: "invalid_record".into(),
                    record_key: Some(key),
                    locator: Some(loc),
                    detail: "invalid complete JSONL rollout line".into(),
                })?;
                counts.malformed_records += 1;
                counts.unknown_records += 1;
                coverage = Coverage::Partial;
                offset += n as u64;
                prefix = offset;
            }
        }
        Ok(ParsedSessionManifest {
            native_session_id: id.unwrap_or_else(|| "unknown".into()),
            native_title: None,
            cwd,
            session_kind,
            parent_native_session,
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
        let x = serde_json::from_slice::<Line>(line).ok();
        let json = serde_json::from_slice::<Value>(line).ok();
        let native_id = x
            .as_ref()
            .and_then(|record| record.payload.as_ref())
            .and_then(|payload| payload.id.clone());
        let mut blocks = Vec::new();
        if let Some(json) = json.as_ref() {
            let item_type = json.pointer("/payload/type").and_then(Value::as_str);
            match (json.get("type").and_then(Value::as_str), item_type) {
                (Some("response_item"), Some("message")) => {
                    let role = json.pointer("/payload/role").and_then(Value::as_str);
                    if matches!(role, Some("user" | "assistant"))
                        && let Some(content) =
                            json.pointer("/payload/content").and_then(Value::as_array)
                    {
                        for (i, item) in content.iter().enumerate() {
                            if matches!(
                                item.get("type").and_then(Value::as_str),
                                Some("input_text" | "output_text" | "text")
                            ) && let Some(text) = item.get("text").and_then(Value::as_str)
                            {
                                blocks.push(ResolvedBlock {
                                    index: i as u32,
                                    kind: BlockKind::Text,
                                    fidelity: Fidelity::Verbatim,
                                    bytes: text.as_bytes().to_vec(),
                                });
                            }
                        }
                    }
                }
                (Some("response_item"), Some("function_call")) => {
                    if let Some(arguments) = json.pointer("/payload/arguments") {
                        let (bytes, fidelity) = if let Some(text) = arguments.as_str() {
                            (text.as_bytes().to_vec(), Fidelity::Verbatim)
                        } else {
                            (
                                serde_json::to_vec(arguments).map_err(|_| {
                                    AdapterError::Unsupported("invalid Codex tool arguments".into())
                                })?,
                                Fidelity::NormalizedJson,
                            )
                        };
                        blocks.push(ResolvedBlock {
                            index: 0,
                            kind: BlockKind::ToolCall,
                            fidelity,
                            bytes,
                        });
                    }
                }
                (Some("response_item"), Some("function_call_output")) => {
                    if let Some(output) = json.pointer("/payload/output").filter(|v| !v.is_null()) {
                        let (bytes, fidelity) = if let Some(text) = output.as_str() {
                            (text.as_bytes().to_vec(), Fidelity::Verbatim)
                        } else {
                            (
                                serde_json::to_vec(output).map_err(|_| {
                                    AdapterError::Unsupported("invalid Codex tool result".into())
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
                }
                (Some("event_msg"), Some("UserMessage" | "AgentMessage")) => {
                    if let Some(text) = json
                        .pointer("/payload/message")
                        .or_else(|| json.pointer("/payload/text"))
                        .and_then(Value::as_str)
                    {
                        blocks.push(ResolvedBlock {
                            index: 0,
                            kind: BlockKind::Text,
                            fidelity: Fidelity::Verbatim,
                            bytes: text.as_bytes().to_vec(),
                        });
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
