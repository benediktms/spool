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

#[test]
fn claude_cwd_comes_from_the_first_line_that_has_one() {
    let bytes = concat!(
        "{\"type\":\"last-prompt\",\"sessionId\":\"s1\"}\n",
        "{\"type\":\"mode\",\"sessionId\":\"s1\"}\n",
        "{\"type\":\"attachment\",\"sessionId\":\"s1\",\"cwd\":\"/work/a\"}\n",
        "{\"type\":\"user\",\"uuid\":\"u1\",\"sessionId\":\"s1\",\"cwd\":\"/work/b\",\"message\":{\"role\":\"user\",\"content\":\"hi\"}}\n",
    );
    let (manifest, _) = parse(&ClaudeAdapter, bytes.as_bytes());
    assert_eq!(manifest.native_session_id, "s1");
    assert_eq!(manifest.cwd.as_deref(), Some("/work/a"));
}

#[test]
fn claude_compaction_is_summary_and_keeps_one_chain() {
    let bytes = include_bytes!("../fixtures/claude/compaction.jsonl");
    let (_, sink) = parse(&ClaudeAdapter, bytes);
    let text: Vec<_> = sink
        .messages
        .iter()
        .flat_map(|message| message.blocks.iter().map(|block| block.text.as_str()))
        .collect();
    assert_eq!(
        text,
        [
            "before compaction question",
            "before compaction answer",
            "after compaction question",
            "after compaction answer",
        ]
    );
    let kind = |key: &str| sink.records.iter().find(|r| r.key == key).unwrap().kind;
    assert_eq!(kind("cc-boundary"), RecordKind::Summary);
    assert_eq!(kind("cc-summary"), RecordKind::Summary);
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.path_to("cc-a2"), ["cc-u1", "cc-a1", "cc-u2", "cc-a2"]);
}

#[test]
fn claude_user_input_exchange_is_conversational_and_resolves_identically() {
    use serde_json::json;
    let q1 = "Where should the \"files\" live?\nPick one.";
    let q2 = "Which format?";
    let lines = [
        json!({"type":"user","uuid":"u1","sessionId":"s","timestamp":"2026-10-10T10:00:00Z","message":{"role":"user","content":"start"}}),
        json!({"type":"assistant","uuid":"a1","parentUuid":"u1","message":{"id":"m1","role":"assistant","content":[
            {"type":"tool_use","id":"ask-1","name":"AskUserQuestion","input":{"questions":[
                {"question":q1,"header":"MUST_NOT_INDEX_HEADER","multiSelect":false,"options":[
                    {"label":"MUST_NOT_INDEX_OPTION_LABEL","description":"MUST_NOT_INDEX_OPTION_DESCRIPTION"}]},
                {"question":q2,"header":"Fmt","options":[]}]}}]}}),
        json!({"type":"user","uuid":"u2","parentUuid":"a1","message":{"role":"user","content":[
            {"type":"tool_result","tool_use_id":"ask-1","content":"MUST_NOT_INDEX_RESULT_STRING"}]},
            "toolUseResult":{"questions":[{"question":q1},{"question":q2}],
                "answers":{q1:"~/.agents/people/",q2:"toml"},
                "annotations":{q2:{"notes":"my own note"}}}}),
        json!({"type":"assistant","uuid":"a2","parentUuid":"u2","message":{"id":"m2","role":"assistant","content":[
            {"type":"tool_use","id":"other-1","name":"Bash","input":{"questions":[{"question":"MUST_NOT_INDEX_OTHER_TOOL"}]}}]}}),
        json!({"type":"user","uuid":"u3","parentUuid":"a2","message":{"role":"user","content":[
            {"type":"tool_result","tool_use_id":"other-1","content":"MUST_NOT_INDEX_OTHER_RESULT"}]},
            "toolUseResult":{"questions":[{"question":q1}],"answers":{q1:"MUST_NOT_INDEX_OTHER_ANSWER"}}}),
    ];
    let bytes = lines
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>()
        .into_bytes();
    let (_, sink) = parse(&ClaudeAdapter, &bytes);

    let exchange: Vec<_> = sink
        .messages
        .iter()
        .filter(|message| message.record_key != "u1")
        .map(|message| {
            (
                message.record_key.as_str(),
                message.role,
                message
                    .blocks
                    .iter()
                    .map(|block| block.text.as_str())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    assert_eq!(
        exchange,
        [
            ("a1", Role::Assistant, vec![q1, q2]),
            (
                "u2",
                Role::User,
                vec!["~/.agents/people/", "toml", "my own note"]
            ),
        ]
    );
    let kind = |key: &str| sink.records.iter().find(|r| r.key == key).unwrap().kind;
    assert_eq!(kind("a1"), RecordKind::Message);
    assert_eq!(kind("u2"), RecordKind::Message);
    assert_eq!(kind("a2"), RecordKind::ToolCall);
    assert_eq!(kind("u3"), RecordKind::ToolResult);
    assert_eq!(sink.calls.len(), 2);
    assert_eq!(sink.results.len(), 2);
    let answer = sink.results.iter().find(|r| r.record_key == "u2").unwrap();
    assert_eq!(answer.turn_id.as_deref(), Some("u1"));
    assert_eq!(
        answer.call.as_ref().map(|(k, i)| (k.as_str(), *i)),
        Some(("a1", 0))
    );

    let indexed = sink
        .messages
        .iter()
        .flat_map(|m| m.blocks.iter().map(|b| b.text.as_str()))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!indexed.contains("MUST_NOT_INDEX"));

    for record in &sink.records {
        let resolved = ClaudeAdapter
            .resolve_locator(
                &mut Cursor::new(bytes.as_slice()),
                &record.locator,
                &record.digest,
                1024 * 1024,
            )
            .unwrap();
        for message in sink.messages.iter().filter(|m| m.record_key == record.key) {
            for block in &message.blocks {
                let found = resolved
                    .blocks
                    .iter()
                    .find(|b| b.index == block.index && b.kind == BlockKind::Text)
                    .unwrap();
                assert_eq!(found.bytes, block.text.as_bytes());
                assert_eq!(found.fidelity, Fidelity::Verbatim);
            }
        }
    }
}

#[test]
fn claude_record_types_classify_and_index_queued_human_prompts() {
    use serde_json::json;
    let queued = |uuid: &str, parent: &str, kind: &str, prompt: &str| {
        json!({"type":"attachment","uuid":uuid,"parentUuid":parent,"isSidechain":false,
            "attachment":{"type":"queued_command","prompt":prompt,"commandMode":"prompt","origin":{"kind":kind}}})
    };
    let lines = [
        json!({"type":"user","uuid":"u1","sessionId":"s","timestamp":"2026-10-10T10:00:00Z","message":{"role":"user","content":"start"}}),
        json!({"type":"ai-title","aiTitle":"Old title","sessionId":"s"}),
        json!({"type":"api-request-blob","messages":[{"role":"user","content":"MUST_NOT_INDEX_BLOB"}]}),
        json!({"type":"attachment","uuid":"at1","parentUuid":"u1","attachment":{"type":"file","content":"MUST_NOT_INDEX_FILE"}}),
        queued("q1", "at1", "human", "also update the docs"),
        queued("q2", "q1", "peer", "MUST_NOT_INDEX_PEER"),
        json!({"type":"assistant","uuid":"a1","parentUuid":"q2","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"done"}]}}),
        json!({"type":"permission-mode","permissionMode":"auto","sessionId":"s"}),
        json!({"type":"ai-title","aiTitle":"New title","sessionId":"s"}),
    ];
    let bytes = lines
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>()
        .into_bytes();
    let (manifest, sink) = parse(&ClaudeAdapter, &bytes);

    assert_eq!(manifest.coverage, Coverage::Complete);
    assert_eq!(manifest.counts.unknown_records, 0);
    assert_eq!(manifest.native_title.as_deref(), Some("New title"));
    let text: Vec<_> = sink
        .messages
        .iter()
        .map(|m| (m.record_key.as_str(), m.role, m.blocks[0].text.as_str()))
        .collect();
    assert_eq!(
        text,
        [
            ("u1", Role::User, "start"),
            ("q1", Role::User, "also update the docs"),
            ("a1", Role::Assistant, "done"),
        ]
    );
    let record = |key: &str| sink.records.iter().find(|r| r.key == key).unwrap();
    assert_eq!(record("q1").kind, RecordKind::Message);
    assert_eq!(record("q1").turn_id.as_deref(), Some("q1"));
    assert_eq!(record("q2").kind, RecordKind::Control);
    assert_eq!(record("a1").turn_id.as_deref(), Some("q1"));
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.path_to("a1"), ["u1", "q1", "a1"]);

    let q1 = record("q1");
    let resolved = ClaudeAdapter
        .resolve_locator(
            &mut Cursor::new(bytes.as_slice()),
            &q1.locator,
            &q1.digest,
            1024 * 1024,
        )
        .unwrap();
    assert_eq!(resolved.blocks.len(), 1);
    assert_eq!(resolved.blocks[0].index, 0);
    assert_eq!(resolved.blocks[0].kind, BlockKind::Text);
    assert_eq!(resolved.blocks[0].bytes, b"also update the docs");
}
