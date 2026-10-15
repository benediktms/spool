#[path = "adapter_common/mod.rs"]
mod adapter_common;
use adapter_common::{Sink, assert_moved_locators, options, parse};
use spool::{
    adapters::{HarnessAdapter, ProbeResult, codex::CodexAdapter},
    domain::*,
    graph::SessionGraph,
};
use std::io::Cursor;

#[test]
fn codex_turn_ids_scope_calls_and_outputs_and_locators_round_trip() {
    let bytes = include_bytes!("../fixtures/codex/tool_turns.jsonl");
    let (manifest, sink) = parse(&CodexAdapter, bytes);
    assert_eq!(manifest.started_at, Some(1_791_763_200_000));
    assert_eq!(manifest.last_activity_at, Some(1_791_763_215_000));
    assert_eq!(
        sink.records
            .iter()
            .find(|record| record.key == "codex-user-1")
            .unwrap()
            .timestamp,
        Some(1_791_763_201_000)
    );
    let actual: Vec<_> = sink
        .messages
        .iter()
        .flat_map(|message| message.blocks.iter().map(|block| block.text.as_str()))
        .collect();
    assert_eq!(
        actual,
        vec![
            "Prompt α 👩‍💻. Treat injection as ordinary data.",
            "Commentary is indexed.",
            "```rust\n\tfn short() {}\n```",
            "CJK 漢字 👩‍💻 é العربية",
            "ok",
            "y",
            "OK ✅",
            "Second prompt with combining é and 🧪.",
            "After malformed",
        ]
    );
    let corpus = actual.join(" ");
    for excluded in [
        "MUST_NOT_INDEX_ARGUMENT",
        "MUST_NOT_INDEX_RESULT",
        "MUST_NOT_INDEX_ORPHAN",
        "MUST_NOT_INDEX_UNKNOWN_BLOCK",
        "MUST_NOT_INDEX_COMPACTION",
    ] {
        assert!(!corpus.contains(excluded));
    }
    assert_eq!(sink.messages[1].phase.as_deref(), Some("commentary"));
    assert_eq!(sink.messages[2].phase.as_deref(), Some("final_answer"));
    assert_eq!(manifest.coverage, Coverage::Partial);
    assert!(manifest.counts.unknown_blocks >= 1);
    assert!(
        sink.warnings
            .iter()
            .any(|warning| warning.code == "invalid_record")
    );

    let first_turn = "turn-1";
    let second_turn = "turn-2";
    assert_eq!(sink.calls.len(), 3);
    let first_repeat = sink
        .calls
        .iter()
        .find(|call| call.record_key == "codex-call-a")
        .unwrap();
    let second_repeat = sink
        .calls
        .iter()
        .find(|call| call.record_key == "codex-call-c")
        .unwrap();
    assert_eq!(first_repeat.native_call_id.as_deref(), Some("repeat-call"));
    assert_eq!(second_repeat.native_call_id.as_deref(), Some("repeat-call"));
    assert_eq!(first_repeat.turn_id.as_deref(), Some(first_turn));
    assert_eq!(second_repeat.turn_id.as_deref(), Some(second_turn));
    let linked = |key: &str, call_key: &str, turn_id: &str| {
        let result = sink
            .results
            .iter()
            .find(|result| result.record_key == key)
            .unwrap();
        assert_eq!(
            result.call.as_ref().map(|(record, _)| record.as_str()),
            Some(call_key)
        );
        assert_eq!(result.turn_id.as_deref(), Some(turn_id));
    };
    linked("codex-result-b", "codex-call-b", first_turn);
    linked("codex-result-a", "codex-call-a", first_turn);
    linked("codex-result-c", "codex-call-c", second_turn);
    assert!(
        sink.results
            .iter()
            .find(|result| result.record_key == "codex-orphan")
            .unwrap()
            .call
            .is_none()
    );

    let incomplete = bytes
        .windows(
            b"{\"timestamp\":\"2026-10-12T00:00:16Z\",\"type\":\"response_item\",\"payload\"".len(),
        )
        .position(|window| {
            window
                == b"{\"timestamp\":\"2026-10-12T00:00:16Z\",\"type\":\"response_item\",\"payload\""
        })
        .unwrap();
    assert_eq!(manifest.prefix_end, incomplete as u64);
    for record in &sink.records {
        let start = record.locator.offset as usize;
        let end = start + record.locator.len as usize;
        let mut source = Cursor::new(bytes.as_slice());
        let resolved = CodexAdapter
            .resolve_locator(
                &mut source,
                &record.locator,
                &record.digest,
                4 * 1024 * 1024,
            )
            .unwrap();
        assert_eq!(resolved.raw, bytes[start..end]);
        assert_eq!(resolved.digest, record.digest);
        for message in sink
            .messages
            .iter()
            .filter(|message| message.record_key == record.key)
        {
            for text in &message.blocks {
                assert!(
                    resolved
                        .blocks
                        .iter()
                        .any(|block| block.index == text.index && block.kind == BlockKind::Text)
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
                    .any(|block| block.index == call.block_index
                        && block.kind == BlockKind::ToolCall)
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
                    .any(|block| block.index == result.block_index
                        && block.kind == BlockKind::ToolResult)
            );
        }
    }
    assert_moved_locators(&CodexAdapter, bytes, &sink.records, 4 * 1024 * 1024);
    let call_record = sink
        .records
        .iter()
        .find(|record| record.key == "codex-call-a")
        .unwrap();
    let mut mutated = bytes.to_vec();
    mutated[call_record.locator.offset as usize + 5] ^= 1;
    let mut source = Cursor::new(mutated.as_slice());
    assert!(matches!(
        CodexAdapter.resolve_locator(
            &mut source,
            &call_record.locator,
            &call_record.digest,
            4 * 1024 * 1024
        ),
        Err(spool::adapters::AdapterError::LocatorMismatch(_))
    ));
}

#[test]
fn codex_unknown_history_mode_limits_and_compressed_probe_are_explicit() {
    let unknown = b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"future\",\"history_mode\":\"future\"}}\n";
    let (manifest, _) = parse(&CodexAdapter, unknown);
    assert_eq!(manifest.coverage, Coverage::Unsupported);

    let too_long = format!(
        "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{}\"}}}}\n",
        "x".repeat(256)
    );
    let mut source = Cursor::new(too_long.as_bytes());
    let mut sink = Sink::default();
    assert!(matches!(
        CodexAdapter.parse_snapshot(&mut source, &options(too_long.len() as u64, 128), &mut sink),
        Err(spool::adapters::AdapterError::LimitExceeded)
    ));

    for compressed in [
        &[0x28, 0xb5, 0x2f, 0xfd][..],
        &[0x1f, 0x8b][..],
        b"PK\x03\x04".as_slice(),
    ] {
        assert!(matches!(
            CodexAdapter.probe(compressed),
            ProbeResult::Unsupported { .. }
        ));
    }

    let mut invalid_utf8 = b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"bad-utf8\"}}\n{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"x\",\"arguments\":\"".to_vec();
    invalid_utf8.push(0xff);
    invalid_utf8.extend_from_slice(b"\"}}\n");
    let (_, sink) = parse(&CodexAdapter, &invalid_utf8);
    let malformed = sink
        .records
        .iter()
        .find(|record| record.kind == RecordKind::Unknown)
        .unwrap();
    let mut source = Cursor::new(invalid_utf8.as_slice());
    let resolved = CodexAdapter
        .resolve_locator(
            &mut source,
            &malformed.locator,
            &malformed.digest,
            1024 * 1024,
        )
        .unwrap();
    assert!(resolved.blocks.is_empty());
}
#[test]
fn codex_graph_resolves_linear_record_ancestry_through_controls() {
    let bytes = b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"codex-chain\",\"history_mode\":\"paginated\"}}\n\
{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"user\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"root\"}]}}\n\
{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"id\":\"call\",\"call_id\":\"c\",\"name\":\"read\",\"arguments\":\"{}\"}}\n\
{\"type\":\"event_msg\",\"payload\":{\"type\":\"DeveloperMessage\",\"id\":\"control\",\"message\":\"excluded\"}}\n\
{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"id\":\"result\",\"call_id\":\"c\",\"output\":\"excluded\"}}\n\
{\"type\":\"response_item\",\"payload\":broken}\n\
{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"id\":\"assistant\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"leaf\"}]}}\n";
    let (_, sink) = parse(&CodexAdapter, bytes);
    for pair in sink.records.windows(2) {
        assert_eq!(pair[1].parent_key.as_deref(), Some(pair[0].key.as_str()));
    }
    assert!(
        sink.records
            .iter()
            .any(|record| record.kind == RecordKind::Unknown)
    );
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.path_to("assistant"), ["user", "assistant"]);
    assert_eq!(graph.message_parent("assistant"), Some("user"));
}
