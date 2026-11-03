//! One budget calculation shared by the reader, search, the IPC layer and the stdio bridge
//! (spec 14.2.7.2): `max_bytes` caps the *final serialized MCP tool result*, which carries one
//! compact JSON copy of the value as JSON-escaped text, plus the JSON-RPC wrapper.

use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;

/// Worst-case bytes of the JSON-RPC envelope around a tool result:
/// `{"jsonrpc":"2.0","id":<up to 20 digits or a 64-char string>,"result":{...,"resultType":"complete","ttlMs":0,"cacheScope":"private"}}`.
pub const JSONRPC_ENVELOPE_MARGIN: usize = 256;

/// The one result shape every tool returns: a single compact JSON text block, no structured copy.
pub fn tool_result(value: &Value, is_error: bool) -> CallToolResult {
    let content = vec![ContentBlock::text(value.to_string())];
    if is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    }
}

/// Exact byte size the bridge will write for `value` as a tool result (plus the envelope margin).
pub fn tool_result_bytes(value: &Value, is_error: bool) -> usize {
    serde_json::to_vec(&tool_result(value, is_error))
        .map(|v| v.len())
        .unwrap_or(usize::MAX / 2)
        + JSONRPC_ENVELOPE_MARGIN
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn counts_one_escaped_text_copy() {
        let v = json!({"data": "quote\" back\\slash\nnewline"});
        let plain = serde_json::to_string(&v).unwrap().len();
        let n = tool_result_bytes(&v, false);
        assert!(
            n > plain + JSONRPC_ENVELOPE_MARGIN,
            "text is escaped and counted"
        );
        assert!(
            n < 2 * plain + JSONRPC_ENVELOPE_MARGIN + 80,
            "only one copy is counted"
        );
        assert!(tool_result_bytes(&v, true) > plain);
        let r = tool_result(&v, false);
        assert!(r.structured_content.is_none());
    }
}
