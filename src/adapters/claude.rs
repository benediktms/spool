use super::jsonl::{JsonlHarness, first_line};
use super::{AdapterError, ProbeResult};
use crate::domain::{HarnessKind, ResolvedBlock};
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::collections::HashMap;

mod parse;
mod resolve;

pub struct ClaudeAdapter;
#[derive(Deserialize, Default)]
pub(crate) struct Line {
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

impl JsonlHarness for ClaudeAdapter {
    const KIND: HarnessKind = HarnessKind::Claude;
    const PARSER_VERSION: &'static str = "claude-jsonl-session-browser-2026-10.1";
    const MALFORMED_DETAIL: &'static str = "invalid complete JSONL record";
    type Parser = parse::ParseState;

    fn probe_header(&self, bytes: &[u8]) -> ProbeResult {
        match serde_json::from_slice::<Line>(first_line(bytes)) {
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

    fn resolve_line(
        &self,
        line: &[u8],
    ) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
        resolve::resolve_line(line)
    }
}
