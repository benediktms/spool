use super::{Content, Entry, TextEntry};
use crate::adapters::{AdapterError, ReadSeek, read_verified};
use crate::domain::{BlockKind, Digest, Fidelity, ResolvedBlock, ResolvedRecord, SourceLocator};
use serde_json::Value;

pub(super) fn resolve_locator(
    s: &mut dyn ReadSeek,
    l: &SourceLocator,
    e: &Digest,
    max: u64,
) -> Result<ResolvedRecord, AdapterError> {
    let (raw, d) = read_verified(s, l, e, max)?;
    let line = raw.strip_suffix(b"\n").unwrap_or(&raw);
    let ent = serde_json::from_slice::<Entry>(line).ok();
    let json = serde_json::from_slice::<Value>(line).ok();

    let blocks = match (ent.as_ref(), json.as_ref()) {
        (Some(ent), Some(json)) => project_blocks(line, ent, json)?,
        _ => Vec::new(),
    };

    Ok(ResolvedRecord {
        native_id: ent.and_then(|entry| entry.id),
        raw,
        digest: d,
        blocks,
    })
}

fn project_blocks(
    line: &[u8],
    ent: &Entry,
    json: &Value,
) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let Some(message) = ent.message.as_ref() else {
        return Ok(Vec::new());
    };
    let role = message.role.as_deref();

    if role == Some("toolResult") || ent.ty.as_deref() == Some("toolResult") {
        tool_result_blocks(json)
    } else if matches!(role, Some("user" | "assistant")) {
        authored_blocks(line, json)
    } else {
        Ok(Vec::new())
    }
}

fn tool_result_blocks(json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let Some(value) = json
        .pointer("/message/content")
        .filter(|value| !value.is_null())
    else {
        return Ok(Vec::new());
    };

    let single_text = value
        .as_array()
        .and_then(|items| (items.len() == 1).then(|| &items[0]))
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str);
    let block = if let Some(text) = value.as_str().or(single_text) {
        verbatim(0, BlockKind::ToolResult, text)
    } else {
        ResolvedBlock {
            index: 0,
            kind: BlockKind::ToolResult,
            fidelity: Fidelity::NormalizedJson,
            bytes: serde_json::to_vec(value)
                .map_err(|_| AdapterError::Unsupported("invalid tool result".into()))?,
        }
    };
    Ok(vec![block])
}

/// Re-decodes only the text content; tool-call arguments come from the full JSON value.
fn authored_blocks(line: &[u8], json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let content = serde_json::from_slice::<TextEntry>(line)
        .ok()
        .and_then(|entry| entry.message)
        .and_then(|message| message.content);

    let mut blocks = Vec::new();
    match content {
        Some(Content::Text(text)) => blocks.push(verbatim(0, BlockKind::Text, &text)),
        Some(Content::Blocks(content_blocks)) => {
            for (i, block) in content_blocks.iter().enumerate() {
                match block.ty.as_deref() {
                    Some("text") => {
                        if let Some(text) = &block.text {
                            blocks.push(verbatim(i as u32, BlockKind::Text, text));
                        }
                    }
                    Some("toolCall") => blocks.extend(tool_call_block(json, i)?),
                    _ => {}
                }
            }
        }
        None => {}
    }
    Ok(blocks)
}

fn tool_call_block(json: &Value, i: usize) -> Result<Option<ResolvedBlock>, AdapterError> {
    let Some(arguments) = json.pointer(&format!("/message/content/{i}/arguments")) else {
        return Ok(None);
    };

    if let Some(text) = arguments.as_str() {
        return Ok(Some(verbatim(i as u32, BlockKind::ToolCall, text)));
    }
    let bytes = serde_json::to_vec(arguments)
        .map_err(|_| AdapterError::Unsupported("invalid tool arguments".into()))?;
    Ok(Some(ResolvedBlock {
        index: i as u32,
        kind: BlockKind::ToolCall,
        fidelity: Fidelity::NormalizedJson,
        bytes,
    }))
}

fn verbatim(index: u32, kind: BlockKind, text: &str) -> ResolvedBlock {
    ResolvedBlock {
        index,
        kind,
        fidelity: Fidelity::Verbatim,
        bytes: text.as_bytes().to_vec(),
    }
}
