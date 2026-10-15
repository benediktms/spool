use super::{AdapterError, ParseOptions, ProbeResult, ReadSeek, RecordSink, SessionAdapter};
use crate::domain::{Digest, HarnessKind, ParsedSessionManifest, ResolvedRecord, SourceLocator};
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::{collections::HashMap, io::Read};

mod parse;
mod resolve;

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
        parse::parse_snapshot(source, o, sink)
    }
    fn resolve_locator(
        &self,
        source: &mut dyn ReadSeek,
        locator: &SourceLocator,
        expected: &Digest,
        max: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        resolve::resolve_locator(source, locator, expected, max)
    }
}
