//! Experimental, conversation-only adapter for Factory Droid session JSONL v2.

use super::jsonl::{
    Emission, JsonlHarness, LineParser, ParseCx, RawRecord, first_line, verbatim_block,
};
use super::{AdapterError, ProbeResult};
use crate::domain::{
    BlockKind, BlockOrigin, Coverage, HarnessKind, MessageEntry, ParseWarning, RecordKind,
    RecordMeta, ResolvedBlock, Role, SessionKind, TextBlock, parse_timestamp_ms,
};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::value::RawValue;
use std::collections::HashSet;

pub struct FactoryAdapter;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Header {
    #[serde(rename = "type")]
    kind: String,
    version: u64,
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Line {
    #[serde(rename = "type")]
    kind: String,
    id: Option<String>,
    parent_id: Option<String>,
    timestamp: Option<String>,
    cwd: Option<String>,
    title: Option<String>,
    calling_session_id: Option<String>,
    calling_tool_use_id: Option<IgnoredAny>,
    message: Option<Message>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Message {
    role: String,
    content: Vec<Block>,
    visibility: Option<String>,
    system_notice: Option<IgnoredAny>,
    openai_phase: Option<String>,
}

#[derive(Deserialize)]
struct Block {
    #[serde(rename = "type")]
    kind: Option<String>,
    text: Option<Box<RawValue>>,
}

impl Message {
    fn conversation_role(&self) -> Option<Role> {
        if self.visibility.is_some() || self.system_notice.is_some() {
            return None;
        }
        match self.role.as_str() {
            "user" => Some(Role::User),
            "assistant" => Some(Role::Assistant),
            _ => None,
        }
    }

    fn project(&self, role: Role) -> (Vec<TextBlock>, u64) {
        let mut blocks = Vec::new();
        let mut unknown = 0;
        for (i, block) in self.content.iter().enumerate() {
            match block.kind.as_deref() {
                Some("text") => {
                    let text = block
                        .text
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<String>(raw.get()).ok());
                    match text {
                        Some(text) if !text.is_empty() => blocks.push(TextBlock {
                            index: u32::try_from(i).expect("record limit bounds block count"),
                            origin: match role {
                                Role::User => BlockOrigin::Unknown,
                                Role::Assistant => BlockOrigin::Authored,
                            },
                            text,
                        }),
                        Some(_) => {}
                        None => unknown += 1,
                    }
                }
                Some(
                    "tool_use" | "tool_result" | "thinking" | "redacted_thinking" | "image"
                    | "document",
                ) => {}
                _ => unknown += 1,
            }
        }
        (blocks, unknown)
    }
}

impl JsonlHarness for FactoryAdapter {
    const KIND: HarnessKind = HarnessKind::Factory;
    const PARSER_VERSION: &'static str = "factory-droid-jsonl-v2";
    const MALFORMED_DETAIL: &'static str = "invalid complete Factory JSONL record";
    const REQUIRE_HEADER: bool = true;
    type Parser = ParseState;

    fn probe_header(&self, bytes: &[u8]) -> ProbeResult {
        match serde_json::from_slice::<Header>(first_line(bytes)) {
            Ok(h) if h.kind == "session_start" && !h.id.trim().is_empty() => {
                if h.version == 2 {
                    ProbeResult::Match {
                        format_version: Some("2".into()),
                    }
                } else {
                    ProbeResult::Unsupported {
                        reason: "unsupported Factory transcript version".into(),
                    }
                }
            }
            _ => ProbeResult::NoMatch,
        }
    }

    fn resolve_line(
        &self,
        line: &[u8],
    ) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
        let line: Line = serde_json::from_slice(line)
            .map_err(|_| AdapterError::Unsupported(Self::MALFORMED_DETAIL.into()))?;
        let resolved = line
            .message
            .as_ref()
            .filter(|_| line.kind == "message")
            .and_then(|m| Some(m.project(m.conversation_role()?).0))
            .unwrap_or_default()
            .into_iter()
            .map(|b| verbatim_block(b.index, BlockKind::Text, &b.text))
            .collect();
        Ok((line.id, resolved))
    }
}

#[derive(Default)]
pub(crate) struct ParseState {
    ids: HashSet<String>,
}

impl LineParser for ParseState {
    type Line = Line;

    fn record(
        &mut self,
        cx: &mut ParseCx<'_>,
        mut line: Line,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError> {
        if raw.locator.offset == 0 {
            let id = line
                .id
                .clone()
                .ok_or_else(|| unsupported("missing Factory session id"))?;
            cx.session.native_id = Some(id);
            cx.session.title = line.title.take().filter(|s| !s.trim().is_empty());
            cx.session.cwd = line.cwd.take().filter(|s| !s.is_empty());
            cx.session.parent_native_session =
                line.calling_session_id.take().filter(|s| !s.is_empty());
            cx.session.kind = if cx.session.parent_native_session.is_some()
                || line.calling_tool_use_id.is_some()
            {
                SessionKind::Subagent
            } else {
                SessionKind::Interactive
            };
        } else if cx.session.native_id.is_none() || line.kind == "session_start" {
            return Err(unsupported("invalid Factory session header"));
        }
        let native_id = line.id.filter(|s| !s.trim().is_empty());
        if (line.kind == "message" && native_id.is_none())
            || native_id
                .as_ref()
                .is_some_and(|id| !self.ids.insert(id.clone()))
        {
            return Err(unsupported("missing or duplicate Factory record id"));
        }
        let timestamp = line.timestamp.as_deref().and_then(parse_timestamp_ms);
        cx.observe_activity(timestamp);
        if let Some(ts) = timestamp {
            cx.started_at.get_or_insert(ts);
        }
        let mut meta = RecordMeta {
            key: native_id
                .clone()
                .unwrap_or_else(|| cx.fallback_key(&raw.digest, cx.counts.records)),
            native_id,
            parent_key: line.parent_id,
            kind: RecordKind::Control,
            timestamp,
            turn_id: None,
            locator: raw.locator,
            digest: raw.digest,
        };
        let mut emission = Emission::default();
        match line.kind.as_str() {
            "session_start" => meta.kind = RecordKind::Header,
            "message" => {
                if cx.session.kind != SessionKind::Subagent || cx.options.include_subagents {
                    emission.message = project_message(cx, &mut meta, line.message)?;
                }
            }
            "todo_state" | "agent_turn_outcome" => {}
            _ => mark_unknown(cx, &mut meta)?,
        }
        cx.emit(meta, emission)
    }
}

fn unsupported(detail: &'static str) -> AdapterError {
    AdapterError::Unsupported(detail.into())
}

fn mark_unknown(cx: &mut ParseCx<'_>, meta: &mut RecordMeta) -> Result<(), AdapterError> {
    meta.kind = RecordKind::Unknown;
    cx.counts.unknown_records += 1;
    cx.coverage = Coverage::Partial;
    warn(
        cx,
        meta,
        "unknown_record",
        "unrecognized Factory record shape",
    )
}

fn warn(
    cx: &mut ParseCx<'_>,
    meta: &RecordMeta,
    code: &'static str,
    detail: &'static str,
) -> Result<(), AdapterError> {
    cx.warn(ParseWarning {
        code: code.into(),
        record_key: Some(meta.key.clone()),
        locator: Some(meta.locator),
        detail: detail.into(),
    })
}

fn project_message(
    cx: &mut ParseCx<'_>,
    meta: &mut RecordMeta,
    message: Option<Message>,
) -> Result<Option<MessageEntry>, AdapterError> {
    let Some(message) = message else {
        mark_unknown(cx, meta)?;
        return Ok(None);
    };
    let Some(role) = message.conversation_role() else {
        return Ok(None);
    };
    let (blocks, unknown) = message.project(role);
    if unknown != 0 {
        cx.counts.unknown_blocks += unknown;
        cx.coverage = Coverage::Partial;
        warn(
            cx,
            meta,
            "unknown_block",
            "unrecognized Factory message block",
        )?;
    }
    if blocks.is_empty() {
        return Ok(None);
    }
    meta.kind = RecordKind::Message;
    cx.count_message(blocks.len());
    if role == Role::User {
        cx.counts.ambiguous_origin_blocks += blocks.len() as u64;
    }
    Ok(Some(MessageEntry {
        record_key: meta.key.clone(),
        role,
        phase: message
            .openai_phase
            .filter(|p| matches!(p.as_str(), "commentary" | "final_answer")),
        blocks,
        attachments: Vec::new(),
        supersedes: Vec::new(),
    }))
}
