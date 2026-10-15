use super::{Content, Line};
use crate::adapters::{AdapterError, ReadSeek, read_verified};
use crate::domain::{BlockKind, Digest, Fidelity, ResolvedBlock, ResolvedRecord, SourceLocator};
use serde_json::Value;

pub(super) fn resolve_locator(
    source: &mut dyn ReadSeek,
    locator: &SourceLocator,
    expected: &Digest,
    max: u64,
) -> Result<ResolvedRecord, AdapterError> {
    let (raw, d) = read_verified(source, locator, expected, max)?;
    let line = raw.strip_suffix(b"\n").unwrap_or(&raw);
    let x = serde_json::from_slice::<Line>(line).ok();
    let json = serde_json::from_slice::<Value>(line).ok();
    let native_id = x.as_ref().and_then(|record| record.uuid.clone());
    let blocks = match (x.as_ref(), json.as_ref()) {
        (Some(x), Some(json)) => message_blocks(x, json)?,
        _ => Vec::new(),
    };

    Ok(ResolvedRecord {
        raw,
        digest: d,
        native_id,
        blocks,
    })
}

fn message_blocks(x: &Line, json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let Some(message) = x.message.as_ref() else {
        return Ok(Vec::new());
    };
    let authored = matches!(
        message.role.as_deref().or(x.ty.as_deref()),
        Some("user" | "assistant")
    );
    let mut blocks = Vec::new();
    match message.content.as_ref() {
        Some(Content::Text(text)) if authored => blocks.push(text_block(0, text)),
        Some(Content::Blocks(content)) => {
            for (i, block) in content.iter().enumerate() {
                let index = i as u32;
                match block.ty.as_deref() {
                    Some("text") if authored => {
                        if let Some(text) = &block.text {
                            blocks.push(text_block(index, text));
                        }
                    }
                    Some("tool_use") => blocks.extend(tool_call_block(json, index)?),
                    Some("tool_result") => blocks.extend(tool_result_block(json, index)?),
                    _ => {}
                }
            }
        }
        _ => {}
    }

    Ok(blocks)
}

fn text_block(index: u32, text: &str) -> ResolvedBlock {
    ResolvedBlock {
        index,
        kind: BlockKind::Text,
        fidelity: Fidelity::Verbatim,
        bytes: text.as_bytes().to_vec(),
    }
}

fn tool_call_block(json: &Value, index: u32) -> Result<Option<ResolvedBlock>, AdapterError> {
    let Some(input) = json.pointer(&format!("/message/content/{index}/input")) else {
        return Ok(None);
    };
    let (bytes, fidelity) = if let Some(text) = input.as_str() {
        (text.as_bytes().to_vec(), Fidelity::Verbatim)
    } else {
        let bytes = serde_json::to_vec(input)
            .map_err(|_| AdapterError::Unsupported("invalid Claude tool input".into()))?;
        (bytes, Fidelity::NormalizedJson)
    };

    Ok(Some(ResolvedBlock {
        index,
        kind: BlockKind::ToolCall,
        fidelity,
        bytes,
    }))
}

fn tool_result_block(json: &Value, index: u32) -> Result<Option<ResolvedBlock>, AdapterError> {
    let Some(result) = json.pointer(&format!("/message/content/{index}/content")) else {
        return Ok(None);
    };
    let (bytes, fidelity) = if let Some(text) = result.as_str() {
        (text.as_bytes().to_vec(), Fidelity::Verbatim)
    } else if let Some(text) = result
        .as_array()
        .and_then(|items| (items.len() == 1).then(|| &items[0]))
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
    {
        (text.as_bytes().to_vec(), Fidelity::Verbatim)
    } else {
        let bytes = serde_json::to_vec(result)
            .map_err(|_| AdapterError::Unsupported("invalid Claude tool result".into()))?;
        (bytes, Fidelity::NormalizedJson)
    };

    Ok(Some(ResolvedBlock {
        index,
        kind: BlockKind::ToolResult,
        fidelity,
        bytes,
    }))
}
