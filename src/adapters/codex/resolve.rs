use super::Line;
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
    let native_id = serde_json::from_slice::<Line>(line)
        .ok()
        .and_then(|record| record.payload)
        .and_then(|payload| payload.id);
    let blocks = match serde_json::from_slice::<Value>(line) {
        Ok(json) => project_blocks(&json)?,
        Err(_) => Vec::new(),
    };

    Ok(ResolvedRecord {
        raw,
        digest: d,
        native_id,
        blocks,
    })
}

fn project_blocks(json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let item_type = json.pointer("/payload/type").and_then(Value::as_str);
    let block = match (json.get("type").and_then(Value::as_str), item_type) {
        (Some("response_item"), Some("message")) => return Ok(message_blocks(json)),
        (Some("response_item"), Some("function_call")) => tool_block(
            json.pointer("/payload/arguments"),
            BlockKind::ToolCall,
            "invalid Codex tool arguments",
        )?,
        (Some("response_item"), Some("function_call_output")) => tool_block(
            json.pointer("/payload/output").filter(|v| !v.is_null()),
            BlockKind::ToolResult,
            "invalid Codex tool result",
        )?,
        (Some("event_msg"), Some("UserMessage" | "AgentMessage")) => event_text_block(json),
        _ => None,
    };
    Ok(block.into_iter().collect())
}

fn text_block(index: u32, text: &str) -> ResolvedBlock {
    ResolvedBlock {
        index,
        kind: BlockKind::Text,
        fidelity: Fidelity::Verbatim,
        bytes: text.as_bytes().to_vec(),
    }
}

fn message_blocks(json: &Value) -> Vec<ResolvedBlock> {
    let role = json.pointer("/payload/role").and_then(Value::as_str);
    let content = json.pointer("/payload/content").and_then(Value::as_array);
    let (Some("user" | "assistant"), Some(content)) = (role, content) else {
        return Vec::new();
    };

    content
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            matches!(
                item.get("type").and_then(Value::as_str),
                Some("input_text" | "output_text" | "text")
            )
        })
        .filter_map(|(i, item)| {
            let text = item.get("text").and_then(Value::as_str)?;
            Some(text_block(i as u32, text))
        })
        .collect()
}

fn tool_block(
    value: Option<&Value>,
    kind: BlockKind,
    error: &str,
) -> Result<Option<ResolvedBlock>, AdapterError> {
    let Some(value) = value else {
        return Ok(None);
    };

    let (bytes, fidelity) = if let Some(text) = value.as_str() {
        (text.as_bytes().to_vec(), Fidelity::Verbatim)
    } else {
        let bytes =
            serde_json::to_vec(value).map_err(|_| AdapterError::Unsupported(error.into()))?;
        (bytes, Fidelity::NormalizedJson)
    };
    Ok(Some(ResolvedBlock {
        index: 0,
        kind,
        fidelity,
        bytes,
    }))
}

fn event_text_block(json: &Value) -> Option<ResolvedBlock> {
    let text = json
        .pointer("/payload/message")
        .or_else(|| json.pointer("/payload/text"))
        .and_then(Value::as_str)?;
    Some(text_block(0, text))
}
