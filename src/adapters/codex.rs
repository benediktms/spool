mod parse;
mod resolve;

use super::jsonl::{JsonlHarness, first_line};
use super::{AdapterError, ProbeResult};
use crate::domain::{HarnessKind, ResolvedBlock};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
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
    arguments: Option<Value>,
    output: Option<Value>,
    #[serde(rename = "is_error")]
    is_error: Option<bool>,
    turn_id: Option<String>,
    #[serde(rename = "internal_chat_message_metadata_passthrough")]
    turn_metadata: Option<Value>,
    history_mode: Option<String>,
    git: Option<CodexGit>,
    #[serde(flatten)]
    _rest: HashMap<String, IgnoredAny>,
}
#[derive(Deserialize, Default)]
struct CodexGit {
    repository_url: Option<String>,
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
    const PARSER_VERSION: &'static str =
        "codex-rollout-cab328f2+git-remote+content-kinds+compacted+user-input";
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

/// The only Codex tool whose question and answer text is conversational.
const USER_INPUT_TOOL: &str = "request_user_input";

#[derive(Deserialize, Default)]
struct UserInputArguments {
    #[serde(default)]
    questions: Vec<UserInputQuestion>,
}
#[derive(Deserialize)]
struct UserInputQuestion {
    question: Option<String>,
}

#[derive(Deserialize)]
struct UserInputOutput {
    answers: OrderedAnswers,
}
#[derive(Deserialize)]
struct AnswerEntry {
    #[serde(default)]
    answers: Vec<String>,
}

/// Answer strings flattened in the output's own key order, which `serde_json::Map` would not keep.
struct OrderedAnswers(Vec<String>);

impl<'de> Deserialize<'de> for OrderedAnswers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AnswersVisitor;
        impl<'de> Visitor<'de> for AnswersVisitor {
            type Value = OrderedAnswers;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of question ids to answers")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some((_, entry)) = map.next_entry::<IgnoredAny, AnswerEntry>()? {
                    out.extend(entry.answers);
                }
                Ok(OrderedAnswers(out))
            }
        }
        deserializer.deserialize_map(AnswersVisitor)
    }
}

fn non_empty(mut texts: Vec<String>) -> Vec<String> {
    texts.retain(|text| !text.is_empty());
    texts
}

/// Question strings from `request_user_input` arguments (a JSON string). Options are ignored.
fn question_texts(arguments: &Value) -> Vec<String> {
    let parsed = arguments
        .as_str()
        .and_then(|raw| serde_json::from_str::<UserInputArguments>(raw).ok())
        .unwrap_or_default();
    non_empty(
        parsed
            .questions
            .into_iter()
            .filter_map(|question| question.question)
            .collect(),
    )
}

/// Selected answers and `user_note:` strings from a `request_user_input` output (a JSON string).
/// A non-JSON output (an error) yields nothing.
fn answer_texts(output: &Value) -> Vec<String> {
    let parsed = output
        .as_str()
        .and_then(|raw| serde_json::from_str::<UserInputOutput>(raw).ok());
    non_empty(parsed.map(|parsed| parsed.answers.0).unwrap_or_default())
}
