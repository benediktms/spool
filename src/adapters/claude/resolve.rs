use super::{Content, Line};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{json_block, tool_result_block, verbatim_block};
use crate::domain::{BlockKind, ResolvedBlock};
use serde_json::Value;

pub(super) fn resolve_line(
    line: &[u8],
) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
    let x = serde_json::from_slice::<Line>(line).ok();
    let json = serde_json::from_slice::<Value>(line).ok();
    let native_id = x.as_ref().and_then(|record| record.uuid.clone());
    let blocks = match (x.as_ref(), json.as_ref()) {
        (Some(x), Some(json)) => message_blocks(x, json)?,
        _ => Vec::new(),
    };

    Ok((native_id, blocks))
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
        Some(Content::Text(text)) if authored => {
            blocks.push(verbatim_block(0, BlockKind::Text, text))
        }
        Some(Content::Blocks(content)) => {
            for (i, block) in content.iter().enumerate() {
                let index = i as u32;
                match block.ty.as_deref() {
                    Some("text") if authored => {
                        if let Some(text) = &block.text {
                            blocks.push(verbatim_block(index, BlockKind::Text, text));
                        }
                    }
                    Some("tool_use") => {
                        if let Some(input) =
                            json.pointer(&format!("/message/content/{index}/input"))
                        {
                            blocks.push(json_block(
                                index,
                                BlockKind::ToolCall,
                                input,
                                "invalid Claude tool input",
                            )?);
                        }
                    }
                    Some("tool_result") => {
                        if let Some(result) =
                            json.pointer(&format!("/message/content/{index}/content"))
                        {
                            blocks.push(tool_result_block(
                                index,
                                result,
                                "invalid Claude tool result",
                            )?);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }

    Ok(blocks)
}
