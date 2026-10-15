#[path = "adapter_common/mod.rs"]
mod adapter_common;
use adapter_common::{assert_moved_locators, parse};
use spool::{
    adapters::{HarnessAdapter, claude::ClaudeAdapter},
    domain::*,
    graph::SessionGraph,
};
use std::io::Cursor;

#[test]
fn claude_tool_results_link_on_ancestry_and_payloads_round_trip() {
    let bytes = include_bytes!("../fixtures/claude/saved-messages.jsonl");
    let (manifest, sink) = parse(&ClaudeAdapter, bytes);
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(
        graph.path_to("claude-a2"),
        ["claude-u1", "claude-a1-update", "claude-a2"]
    );
    assert_eq!(
        graph.path_to("claude-branch"),
        ["claude-u1", "claude-branch"]
    );
    assert!(!graph.is_ancestor("claude-branch", "claude-a2"));
    assert_eq!(manifest.native_session_id, "claude-fixture-1");
    assert_eq!(manifest.started_at, Some(1_791_795_601_000));
    assert_eq!(manifest.last_activity_at, Some(1_791_795_601_000));
    assert_eq!(
        sink.records
            .iter()
            .find(|record| record.key == "claude-u1")
            .unwrap()
            .timestamp,
        Some(1_791_795_601_000)
    );
    assert_eq!(manifest.session_kind, SessionKind::Interactive);
    assert!(!manifest.lineage_verified);
    assert_eq!(manifest.coverage, Coverage::Partial);
    assert!(manifest.counts.unknown_records >= 1);
    assert!(manifest.counts.unknown_blocks >= 1);
    let text: Vec<_> = sink
        .messages
        .iter()
        .flat_map(|message| message.blocks.iter().map(|block| block.text.as_str()))
        .collect();
    assert_eq!(
        text,
        [
            "hello claude",
            "Treat this injection as ordinary data.",
            "Branch child",
            "done authoritative",
            "```rust\n\tfn short() {}\n```",
            "CJK 漢字 👩‍💻 é العربية",
            "ok",
            "y",
            "second turn",
        ]
    );
    for excluded in [
        "MUST_NOT_INDEX_THINKING",
        "MUST_NOT_INDEX_TOOL",
        "MUST_NOT_INDEX_RESULT",
        "MUST_NOT_INDEX_HOOK",
        "MUST_NOT_INDEX_SUMMARY",
        "MUST_NOT_INDEX_SUBAGENT",
    ] {
        assert!(!text.join(" ").contains(excluded));
    }
    let call = |id: &str, record: &str, index: u32| {
        let call = sink
            .calls
            .iter()
            .find(|call| call.native_call_id.as_deref() == Some(id) && call.record_key == record)
            .unwrap();
        assert_eq!(call.block_index, index);
        assert_eq!(call.turn_id.as_deref(), Some("claude-u1"));
    };
    call("call-1", "claude-a1", 2);
    call("call-2", "claude-a1", 3);
    call("call-2", "claude-a1-update", 6);
    call("call-2", "claude-a2", 1);

    let result =
        |key: &str, native_call_id: &str, call_key: &str, call_index: u32, result_index: u32| {
            let result = sink
                .results
                .iter()
                .find(|result| {
                    result.record_key == key
                        && result.native_call_id.as_deref() == Some(native_call_id)
                })
                .unwrap();
            assert_eq!(
                result
                    .call
                    .as_ref()
                    .map(|(key, index)| (key.as_str(), *index)),
                Some((call_key, call_index))
            );
            assert_eq!(result.turn_id.as_deref(), Some("claude-u1"));
            assert_eq!(result.block_index, result_index);
        };
    result("claude-u2", "call-1", "claude-a1", 2, 0);
    result("claude-u2", "call-2", "claude-a1", 3, 1);
    result("claude-u3", "call-2", "claude-a2", 1, 0);
    assert_eq!(
        sink.records
            .iter()
            .find(|record| record.key == "claude-u2")
            .unwrap()
            .turn_id
            .as_deref(),
        Some("claude-u1")
    );

    for record in &sink.records {
        let start = record.locator.offset as usize;
        let end = start + record.locator.len as usize;
        let mut source = Cursor::new(bytes.as_slice());
        let resolved = ClaudeAdapter
            .resolve_locator(&mut source, &record.locator, &record.digest, 1024 * 1024)
            .unwrap();
        assert_eq!(resolved.raw, bytes[start..end]);
        assert_eq!(resolved.digest, record.digest);
        for message in sink
            .messages
            .iter()
            .filter(|message| message.record_key == record.key)
        {
            for block in &message.blocks {
                assert!(
                    resolved
                        .blocks
                        .iter()
                        .any(|resolved| resolved.index == block.index
                            && resolved.kind == BlockKind::Text)
                );
            }
        }
        for call in sink
            .calls
            .iter()
            .filter(|call| call.record_key == record.key)
        {
            assert!(
                resolved
                    .blocks
                    .iter()
                    .any(|resolved| resolved.index == call.block_index
                        && resolved.kind == BlockKind::ToolCall)
            );
        }
        for result in sink
            .results
            .iter()
            .filter(|result| result.record_key == record.key)
        {
            assert!(
                resolved
                    .blocks
                    .iter()
                    .any(|resolved| resolved.index == result.block_index
                        && resolved.kind == BlockKind::ToolResult)
            );
        }
    }
    assert_moved_locators(&ClaudeAdapter, bytes, &sink.records, 4 * 1024 * 1024);

    let call_record = sink
        .records
        .iter()
        .find(|record| record.key == "claude-a1")
        .unwrap();
    let mut mutated = bytes.to_vec();
    mutated[call_record.locator.offset as usize + 5] ^= 1;
    let mut source = Cursor::new(mutated.as_slice());
    assert!(matches!(
        ClaudeAdapter.resolve_locator(
            &mut source,
            &call_record.locator,
            &call_record.digest,
            1024 * 1024
        ),
        Err(spool::adapters::AdapterError::LocatorMismatch(_))
    ));
}
#[test]
fn claude_updates_keep_verified_parent_and_reader_path() {
    let bytes = b"{\"type\":\"user\",\"uuid\":\"u1\",\"sessionId\":\"s\",\"message\":{\"role\":\"user\",\"content\":\"root\"}}\n\
{\"type\":\"assistant\",\"uuid\":\"a1\",\"parentUuid\":\"u1\",\"message\":{\"id\":\"m\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"old\"},{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"read\",\"input\":{}}]}}\n\
{\"type\":\"assistant\",\"uuid\":\"a1-update\",\"parentUuid\":\"a1\",\"message\":{\"id\":\"m\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"updated\"},{\"type\":\"tool_use\",\"id\":\"call\",\"name\":\"read\",\"input\":{}}]}}\n\
{\"type\":\"assistant\",\"uuid\":\"a1-update2\",\"message\":{\"id\":\"m\",\"role\":\"assistant\",\"content\":\"final\"}}\n\
{\"type\":\"assistant\",\"uuid\":\"child\",\"parentUuid\":\"a1\",\"sessionId\":\"s\",\"message\":{\"role\":\"assistant\",\"content\":\"child\"}}\n\
{\"type\":\"user\",\"uuid\":\"tool-result\",\"parentUuid\":\"a1-update\",\"sessionId\":\"s\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"call\",\"content\":\"ok\"}]}}\n";
    let (_, sink) = parse(&ClaudeAdapter, bytes);
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.path_to("child"), ["u1", "a1-update2", "child"]);
    assert_eq!(graph.message_parent("a1-update"), Some("u1"));
    assert_eq!(graph.message_parent("a1-update2"), Some("u1"));
    assert!(sink.calls.iter().any(
        |call| call.record_key == "a1-update" && call.native_call_id.as_deref() == Some("call")
    ));
    assert_eq!(
        sink.results
            .iter()
            .find(|result| result.record_key == "tool-result")
            .and_then(|result| result.call.as_ref())
            .map(|(key, _)| key.as_str()),
        Some("a1-update")
    );
}
