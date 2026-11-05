use super::{Content, Line, USER_INPUT_TOOL, answer_texts, exchange_index, question_texts};
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
        (Some(x), _) if let Some(prompt) = x.queued_prompt() => {
            vec![verbatim_block(0, BlockKind::Text, prompt)]
        }
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
                        if authored
                            && block.name.as_deref() == Some(USER_INPUT_TOOL)
                            && let Some(input) = &block.input
                        {
                            exchange_blocks(&mut blocks, index, question_texts(input));
                        }
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
                        if authored && let Some(result) = &x.tool_use_result {
                            exchange_blocks(&mut blocks, index, answer_texts(result));
                        }
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

fn exchange_blocks(blocks: &mut Vec<ResolvedBlock>, content_index: u32, texts: Vec<&str>) {
    for (n, text) in texts.into_iter().enumerate() {
        blocks.push(verbatim_block(
            exchange_index(content_index, n),
            BlockKind::Text,
            text,
        ));
    }
}
