#[path = "adapter_common/mod.rs"]
mod adapter_common;
use adapter_common::{Sink, assert_moved_locators, options, parse};
use spool::{
    adapters::{AdapterError, HarnessAdapter, ProbeResult, RecordSink, factory::FactoryAdapter},
    domain::*,
    graph::SessionGraph,
};
use std::io::Cursor;
use std::sync::atomic::Ordering;

const FIXTURE: &[u8] = include_bytes!("../fixtures/factory/session-v2.jsonl");
const CHILD: &[u8] = include_bytes!("../fixtures/factory/subagent-v2.jsonl");

#[test]
fn factory_projects_conversation_and_reopens_the_same_blocks() {
    let (manifest, sink) = parse(&FactoryAdapter, FIXTURE);
    assert_eq!(
        manifest.native_session_id,
        "5ffdaa21-fac0-4000-8000-000000000001"
    );
    assert_eq!(
        manifest.native_title.as_deref(),
        Some("Factory mineral notes")
    );
    assert_eq!(manifest.cwd.as_deref(), Some("/tmp/fixture-project"));
    assert_eq!(manifest.session_kind, SessionKind::Interactive);
    assert_eq!(manifest.coverage, Coverage::Complete);
    assert_eq!(manifest.counts.ambiguous_origin_blocks, 2);
    assert_eq!(manifest.active_leaf_key, None);
    assert!(sink.calls.is_empty());
    assert!(sink.results.is_empty());
    assert!(sink.warnings.is_empty());
    let (graph, warnings) = SessionGraph::build(&sink.records, &sink.messages);
    assert!(warnings.is_empty());
    assert_eq!(graph.path_to("f6"), ["f1", "f3", "f4", "f6"]);
    let texts: Vec<_> = sink
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .map(|b| (b.index, b.origin, b.text.as_str()))
        .collect();
    assert_eq!(
        texts,
        [
            (
                0,
                BlockOrigin::Unknown,
                "Please catalogue the beryllattice sample."
            ),
            (1, BlockOrigin::Authored, "I will check the samples."),
            (1, BlockOrigin::Unknown, "ok"),
            (
                0,
                BlockOrigin::Authored,
                "The beryllattice sample is catalogued."
            ),
            (
                1,
                BlockOrigin::Authored,
                "```rust\n\tfn done() {}\n```\n漢字"
            ),
        ]
    );
    assert_eq!(sink.messages[1].phase.as_deref(), Some("commentary"));
    assert_eq!(sink.messages[3].phase.as_deref(), Some("final_answer"));
    assert_eq!(
        sink.records[1]
            .timestamp
            .and_then(|ts| ts.checked_rem(1_000)),
        Some(123)
    );
    assert_moved_locators(&FactoryAdapter, FIXTURE, &sink.records, 1 << 20);
    for message in &sink.messages {
        let record = sink
            .records
            .iter()
            .find(|r| r.key == message.record_key)
            .unwrap();
        let resolved = FactoryAdapter
            .resolve_locator(
                &mut Cursor::new(FIXTURE),
                &record.locator,
                &record.digest,
                1 << 20,
            )
            .unwrap();
        assert_eq!(resolved.native_id.as_ref(), record.native_id.as_ref());
        assert_eq!(resolved.blocks.len(), message.blocks.len());
        for (block, indexed) in resolved.blocks.iter().zip(&message.blocks) {
            assert_eq!(block.index, indexed.index);
            assert_eq!(block.kind, BlockKind::Text);
            assert_eq!(block.bytes, indexed.text.as_bytes());
        }
    }
}

#[test]
fn factory_admits_only_complete_v2_headers() {
    assert_eq!(
        FactoryAdapter.probe(FIXTURE),
        ProbeResult::Match {
            format_version: Some("2".into())
        }
    );
    let v3 = r#"{"type":"session_start","id":"future","version":3}"#;
    assert!(matches!(
        FactoryAdapter.probe(v3.as_bytes()),
        ProbeResult::Unsupported { .. }
    ));
    for bytes in [
        format!("{v3}\n").into_bytes(),
        b"{\"type\":\"session_start\",\"id\":\"x\",\"version\":2}".to_vec(),
        b"{\"type\":\"session_start\",\"id\":\"x\",\"version\":2,\"title\":17}\n".to_vec(),
        b"{\"type\":\"session\",\"id\":\"not-factory\",\"version\":2}\n".to_vec(),
        b"{\"type\":\"session_start\",\"id\":\"\",\"version\":2}\n".to_vec(),
        vec![],
    ] {
        let mut sink = Sink::default();
        let result = FactoryAdapter.parse_snapshot(
            &mut Cursor::new(&bytes),
            &options(bytes.len() as u64, 1 << 20),
            &mut sink,
        );
        assert!(
            matches!(result, Err(AdapterError::Unsupported(_))),
            "{result:?}"
        );
        assert!(sink.records.is_empty());
    }
}

#[test]
fn factory_handles_partial_lines_unknown_shapes_and_subagent_opt_in() {
    let (child, excluded) = parse(&FactoryAdapter, CHILD);
    assert_eq!(child.session_kind, SessionKind::Subagent);
    assert_eq!(
        child.parent_native_session.as_deref(),
        Some("5ffdaa21-fac0-4000-8000-000000000001")
    );
    assert!(!child.lineage_verified);
    assert!(excluded.messages.is_empty());
    let mut opts = options(CHILD.len() as u64, 1 << 20);
    opts.include_subagents = true;
    let mut included = Sink::default();
    FactoryAdapter
        .parse_snapshot(&mut Cursor::new(CHILD), &opts, &mut included)
        .unwrap();
    assert_eq!(included.messages[0].blocks[0].text, "childamethyst");
    included.cancelled().store(true, Ordering::Relaxed);
    assert!(matches!(
        FactoryAdapter.parse_snapshot(&mut Cursor::new(CHILD), &opts, &mut included),
        Err(AdapterError::Cancelled)
    ));

    let mut bytes = FIXTURE.to_vec();
    bytes.extend_from_slice(br#"{"type":"future_record","id":"f7","parentId":"f6"}
{"type":"message","id":"f8","parentId":"f7","message":{"role":"user","content":[{"type":"future_block","text":{"private":"must not be retained"}},{"type":"text","text":"y"}]}}
{invalid}
{"type":"message","id":"incomplete""#);
    let (partial, output) = parse(&FactoryAdapter, &bytes);
    assert_eq!(partial.coverage, Coverage::Partial);
    assert_eq!(partial.counts.unknown_records, 2);
    assert_eq!(partial.counts.unknown_blocks, 1);
    assert_eq!(
        output
            .warnings
            .iter()
            .map(|w| w.code.as_str())
            .collect::<Vec<_>>(),
        ["unknown_record", "unknown_block", "invalid_record"]
    );
    assert_eq!(output.messages.last().unwrap().blocks[0].text, "y");
    assert_eq!(
        partial.prefix_end,
        bytes.iter().rposition(|b| *b == b'\n').unwrap() as u64 + 1
    );

    let duplicate = FIXTURE.split_inclusive(|b| *b == b'\n').nth(1).unwrap();
    bytes.truncate(FIXTURE.len());
    bytes.extend_from_slice(duplicate);
    let opts = options(bytes.len() as u64, 1 << 20);
    assert!(matches!(
        FactoryAdapter.parse_snapshot(&mut Cursor::new(&bytes), &opts, &mut Sink::default()),
        Err(AdapterError::Unsupported(_))
    ));
}
