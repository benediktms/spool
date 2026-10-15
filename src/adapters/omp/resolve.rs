use super::{Content, Entry, TextEntry};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{json_block, tool_result_block, verbatim_block};
use crate::domain::{BlockKind, ResolvedBlock};
use serde_json::Value;

pub(super) fn resolve_line(
    line: &[u8],
) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
    let ent = serde_json::from_slice::<Entry>(line).ok();
    let json = serde_json::from_slice::<Value>(line).ok();

    let blocks = match (ent.as_ref(), json.as_ref()) {
        (Some(ent), Some(json)) => project_blocks(line, ent, json)?,
        _ => Vec::new(),
    };

    Ok((ent.and_then(|entry| entry.id), blocks))
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

    Ok(vec![tool_result_block(0, value, "invalid tool result")?])
}

/// Re-decodes only the text content; tool-call arguments come from the full JSON value.
fn authored_blocks(line: &[u8], json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let content = serde_json::from_slice::<TextEntry>(line)
        .ok()
        .and_then(|entry| entry.message)
        .and_then(|message| message.content);

    let mut blocks = Vec::new();
    match content {
        Some(Content::Text(text)) => blocks.push(verbatim_block(0, BlockKind::Text, &text)),
        Some(Content::Blocks(content_blocks)) => {
            for (i, block) in content_blocks.iter().enumerate() {
                match block.ty.as_deref() {
                    Some("text") => {
                        if let Some(text) = &block.text {
                            blocks.push(verbatim_block(i as u32, BlockKind::Text, text));
                        }
                    }
                    Some("toolCall") => {
                        if let Some(arguments) =
                            json.pointer(&format!("/message/content/{i}/arguments"))
                        {
                            blocks.push(json_block(
                                i as u32,
                                BlockKind::ToolCall,
                                arguments,
                                "invalid tool arguments",
                            )?);
                        }
                    }
                    _ => {}
                }
            }
        }
        None => {}
    }
    Ok(blocks)
}
