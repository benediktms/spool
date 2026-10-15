mod parse;
mod resolve;

use super::jsonl::{JsonlHarness, first_line};
use super::{AdapterError, ProbeResult};
use crate::domain::{HarnessKind, ResolvedBlock};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::collections::HashMap;
pub struct CodexAdapter;
#[derive(Deserialize, Default)]
pub(crate) struct Line {
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
impl JsonlHarness for CodexAdapter {
    const KIND: HarnessKind = HarnessKind::Codex;
    const PARSER_VERSION: &'static str = "codex-rollout-cab328f2";
    const MALFORMED_DETAIL: &'static str = "invalid complete JSONL rollout line";
    type Parser = parse::ParseState;

    fn probe_header(&self, b: &[u8]) -> ProbeResult {
        match serde_json::from_slice::<Line>(first_line(b)) {
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

    fn resolve_line(
        &self,
        line: &[u8],
    ) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
        resolve::resolve_line(line)
    }
}
