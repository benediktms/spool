use super::jsonl::{JsonlHarness, first_line, non_empty, strings};
use super::{AdapterError, ProbeResult};
use crate::domain::{HarnessKind, ResolvedBlock};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::collections::HashMap;

mod parse;
mod resolve;

pub struct OmpAdapter;
#[derive(Deserialize, Default)]
pub(crate) struct Entry {
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
    #[serde(rename = "toolName")]
    tool_name: Option<String>,
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
impl JsonlHarness for OmpAdapter {
    const KIND: HarnessKind = HarnessKind::Omp;
    const PARSER_VERSION: &'static str = "omp-v3-title-slot-v3";
    const MALFORMED_DETAIL: &'static str = "record is malformed or unsupported";
    type Parser = parse::ParseState;

    fn probe_header(&self, b: &[u8]) -> ProbeResult {
        let parsed = serde_json::from_slice::<Entry>(first_line(b));
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

    fn resolve_line(
        &self,
        line: &[u8],
    ) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
        resolve::resolve_line(line)
    }
}

/// The only OMP tool whose question and answer text is conversational.
const ASK_TOOL: &str = "ask";

/// Question strings of `ask` arguments, in order. Options, headers and recommendations are ignored.
fn question_texts(arguments: &Value) -> Vec<&str> {
    let questions = arguments.get("questions").and_then(Value::as_array);
    non_empty(
        questions
            .into_iter()
            .flatten()
            .filter_map(|question| question.get("question")?.as_str())
            .collect(),
    )
}

/// Selected options, typed input and notes of `ask` result details, in question order.
fn answer_texts(details: &Value) -> Vec<&str> {
    let answers = match details.get("results").and_then(Value::as_array) {
        Some(results) => results.iter().collect(),
        None => vec![details],
    };
    let mut texts = Vec::new();
    for answer in answers {
        for field in ["selectedOptions", "customInput", "note"] {
            texts.extend(strings(answer.get(field)));
        }
    }
    non_empty(texts)
}
