use super::jsonl::{JsonlHarness, first_line};
use super::{AdapterError, ProbeResult};
use crate::domain::{HarnessKind, ResolvedBlock};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
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
    #[serde(rename = "logicalParentUuid")]
    logical_parent: Option<String>,
    subtype: Option<String>,
    #[serde(rename = "isCompactSummary")]
    is_compact_summary: Option<bool>,
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
    #[serde(rename = "toolUseResult")]
    tool_use_result: Option<Value>,
    attachment: Option<Attachment>,
    #[serde(rename = "aiTitle")]
    ai_title: Option<String>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize)]
struct Attachment {
    #[serde(rename = "type")]
    ty: Option<String>,
    prompt: Option<Value>,
    origin: Option<Origin>,
}
#[derive(Deserialize)]
struct Origin {
    kind: Option<String>,
}

impl Line {
    /// A prompt the person typed while a turn was running. Claude stores it only as an attachment.
    fn queued_prompt(&self) -> Option<&str> {
        let a = self.attachment.as_ref()?;
        (self.ty.as_deref() == Some("attachment")
            && a.ty.as_deref() == Some("queued_command")
            && a.origin.as_ref()?.kind.as_deref() == Some("human"))
        .then(|| a.prompt.as_ref()?.as_str())
        .flatten()
    }
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
    input: Option<Value>,
    #[serde(rename = "tool_use_id")]
    call_id: Option<String>,
    is_error: Option<bool>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}

impl JsonlHarness for ClaudeAdapter {
    const KIND: HarnessKind = HarnessKind::Claude;
    const PARSER_VERSION: &'static str =
        "claude-jsonl-session-browser-2026-10.1+late-cwd+compaction+user-input+record-types";
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

/// The only Claude tool whose question and answer text is conversational.
const USER_INPUT_TOOL: &str = "AskUserQuestion";
const MAX_EXCHANGE_TEXTS: usize = 0xFFFF;

/// Block `n` of an exchange inside content block `content`. The first block keeps the content
/// index so it matches ordinary text numbering.
fn exchange_index(content: u32, n: usize) -> u32 {
    content.saturating_add(u32::try_from(n).unwrap_or(0) << 16)
}

fn strings(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::String(text)) => vec![text],
        Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn non_empty(mut texts: Vec<&str>) -> Vec<&str> {
    texts.retain(|text| !text.is_empty());
    texts.truncate(MAX_EXCHANGE_TEXTS);
    texts
}

/// Question strings of an `AskUserQuestion` input, in order. Options and headers are ignored.
fn question_texts(input: &Value) -> Vec<&str> {
    let questions = input.get("questions").and_then(Value::as_array);
    non_empty(
        questions
            .into_iter()
            .flatten()
            .filter_map(|question| question.get("question")?.as_str())
            .collect(),
    )
}

/// Selected answers and user notes of a `toolUseResult`, in question order.
/// Annotation notes are read as `annotations[question].notes` (string); other shapes are ignored.
fn answer_texts(result: &Value) -> Vec<&str> {
    let Some(answers) = result.get("answers").and_then(Value::as_object) else {
        return Vec::new();
    };
    let annotations = result.get("annotations");
    let keys: Vec<&str> = match result.get("questions").and_then(Value::as_array) {
        Some(questions) => questions
            .iter()
            .filter_map(|question| question.get("question")?.as_str())
            .collect(),
        None => answers.keys().map(String::as_str).collect(),
    };
    let mut texts = Vec::new();
    for key in keys {
        texts.extend(strings(answers.get(key)));
        let notes = annotations
            .and_then(|all| all.get(key))
            .and_then(|annotation| annotation.get("notes"));
        texts.extend(strings(notes));
    }
    non_empty(texts)
}
