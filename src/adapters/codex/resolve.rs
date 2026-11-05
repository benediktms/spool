use super::{Line, USER_INPUT_TOOL, answer_texts, question_texts};
use crate::adapters::AdapterError;
use crate::adapters::jsonl::{json_block, verbatim_block};
use crate::domain::{BlockKind, ResolvedBlock};
use serde_json::Value;

pub(super) fn resolve_line(
    line: &[u8],
) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError> {
    let native_id = serde_json::from_slice::<Line>(line)
        .ok()
        .and_then(|record| record.payload)
        .and_then(|payload| payload.id);
    let blocks = match serde_json::from_slice::<Value>(line) {
        Ok(json) => project_blocks(&json)?,
        Err(_) => Vec::new(),
    };

    Ok((native_id, blocks))
}

fn project_blocks(json: &Value) -> Result<Vec<ResolvedBlock>, AdapterError> {
    let item_type = json.pointer("/payload/type").and_then(Value::as_str);
    let block = match (json.get("type").and_then(Value::as_str), item_type) {
        (Some("response_item"), Some("message")) => return Ok(message_blocks(json)),
        (Some("response_item"), Some("function_call")) => {
            let mut blocks = Vec::new();
            let arguments = json.pointer("/payload/arguments");
            if json.pointer("/payload/name").and_then(Value::as_str) == Some(USER_INPUT_TOOL) {
                exchange_blocks(&mut blocks, arguments.map(question_texts));
            }
            if let Some(v) = arguments {
                blocks.push(json_block(
                    0,
                    BlockKind::ToolCall,
                    v,
                    "invalid Codex tool arguments",
                )?);
            }
            return Ok(blocks);
        }
        (Some("response_item"), Some("function_call_output")) => {
            let mut blocks = Vec::new();
            let output = json.pointer("/payload/output").filter(|v| !v.is_null());
            exchange_blocks(&mut blocks, output.map(answer_texts));
            if let Some(v) = output {
                blocks.push(json_block(
                    0,
                    BlockKind::ToolResult,
                    v,
                    "invalid Codex tool result",
                )?);
            }
            return Ok(blocks);
        }
        (Some("event_msg"), Some("UserMessage" | "AgentMessage")) => event_text_block(json),
        _ => None,
    };
    Ok(block.into_iter().collect())
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
            Some(verbatim_block(i as u32, BlockKind::Text, text))
        })
        .collect()
}

fn event_text_block(json: &Value) -> Option<ResolvedBlock> {
    let text = json
        .pointer("/payload/message")
        .or_else(|| json.pointer("/payload/text"))
        .and_then(Value::as_str)?;
    Some(verbatim_block(0, BlockKind::Text, text))
}

fn exchange_blocks(blocks: &mut Vec<ResolvedBlock>, texts: Option<Vec<String>>) {
    for (text, index) in texts.unwrap_or_default().iter().zip(0..) {
        blocks.push(verbatim_block(index, BlockKind::Text, text));
    }
}
