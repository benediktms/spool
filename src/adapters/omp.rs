use super::{AdapterError, HarnessAdapter, ParseOptions, ProbeResult, ReadSeek, RecordSink};
use crate::domain::{Digest, HarnessKind, ParsedSessionManifest, ResolvedRecord, SourceLocator};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use std::{collections::HashMap, io::Read};

mod parse;
mod resolve;

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
impl HarnessAdapter for OmpAdapter {
    fn kind(&self) -> HarnessKind {
        HarnessKind::Omp
    }
    fn parser_version(&self) -> &'static str {
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
        parse::parse_snapshot(source, o, sink)
    }
    fn resolve_locator(
        &self,
        s: &mut dyn ReadSeek,
        l: &SourceLocator,
        e: &Digest,
        max: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        resolve::resolve_locator(s, l, e, max)
    }
}
