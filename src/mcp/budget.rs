//! One budget calculation shared by the reader, search, the IPC layer and the stdio bridge
//! (spec 14.2.7.2): `max_bytes` caps the *final serialized MCP tool result*, which carries the
//! structured value AND rmcp's compatibility text copy of it, JSON-escaped, plus the JSON-RPC wrapper.

use rmcp::model::CallToolResult;
use serde_json::Value;

/// Worst-case bytes of the JSON-RPC envelope around a tool result:
/// `{"jsonrpc":"2.0","id":<up to 20 digits or a 64-char string>,"result":{...,"resultType":"complete","ttlMs":0,"cacheScope":"private"}}`.
pub const JSONRPC_ENVELOPE_MARGIN: usize = 256;

/// Exact byte size the bridge will write for `value` as a tool result (plus the envelope margin).
pub fn tool_result_bytes(value: &Value, is_error: bool) -> usize {
    let r = if is_error {
        CallToolResult::structured_error(value.clone())
    } else {
        CallToolResult::structured(value.clone())
    };
    serde_json::to_vec(&r)
        .map(|v| v.len())
        .unwrap_or(usize::MAX / 2)
        + JSONRPC_ENVELOPE_MARGIN
}

/// Cheaper estimate for building pages incrementally: size of `value` as structured content plus
/// its JSON-escaped text copy. Always `<= tool_result_bytes(value) - JSONRPC_ENVELOPE_MARGIN`
/// plus a few dozen bytes of fixed wrapper; callers keep a `JSONRPC_ENVELOPE_MARGIN` reserve and
/// MUST confirm the final page with `tool_result_bytes`.
pub fn doubled_json_len(value: &Value) -> usize {
    let s = serde_json::to_string(value).unwrap_or_default();
    s.len() + serde_json::to_string(&s).map(|e| e.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn counts_text_copy_and_escaping() {
        let v = json!({"data": "quote\" back\\slash\nnewline"});
        let plain = serde_json::to_string(&v).unwrap().len();
        assert!(
            tool_result_bytes(&v, false) > 2 * plain,
            "text copy is escaped and counted"
        );
        assert!(tool_result_bytes(&v, true) > 2 * plain);
    }
}
