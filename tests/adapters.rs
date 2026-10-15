use spool::{
    adapters::{
        HarnessAdapter, ParseOptions, RecordSink, SinkError, claude::ClaudeAdapter,
        codex::CodexAdapter, omp::OmpAdapter,
    },
    domain::*,
};
use std::{collections::HashMap, io::Cursor, sync::atomic::AtomicBool};
#[derive(Default)]
struct Sink {
    records: Vec<RecordMeta>,
    messages: Vec<MessageEntry>,
    calls: Vec<ToolCallMeta>,
    results: Vec<ToolResultMeta>,
    warnings: Vec<ParseWarning>,
    message_groups: HashMap<String, String>,
    cancel: AtomicBool,
}
impl RecordSink for Sink {
    fn record(&mut self, x: RecordMeta) -> Result<(), SinkError> {
        self.records.push(x);
        Ok(())
    }
    fn message(&mut self, x: MessageEntry) -> Result<(), SinkError> {
        let group = x
            .supersedes
            .iter()
            .find_map(|key| self.message_groups.get(key).cloned())
            .unwrap_or_else(|| x.record_key.clone());
        self.message_groups
            .insert(x.record_key.clone(), group.clone());
        for key in &x.supersedes {
            self.message_groups.insert(key.clone(), group.clone());
            self.messages.retain(|old| old.record_key != *key);
        }
        self.messages.push(x);
        Ok(())
    }
    fn tool_call(&mut self, x: ToolCallMeta) -> Result<(), SinkError> {
        let key = x.record_key.clone();
        let group = self.message_groups.get(&key).cloned().unwrap_or(key);
        if let Some(id) = x.native_call_id.as_deref() {
            self.calls.retain(|old| {
                let old_group = self
                    .message_groups
                    .get(&old.record_key)
                    .map(String::as_str)
                    .unwrap_or(old.record_key.as_str());
                old_group != group.as_str() || old.native_call_id.as_deref() != Some(id)
            });
        }
        self.calls.push(x);
        Ok(())
    }
    fn tool_result(&mut self, x: ToolResultMeta) -> Result<(), SinkError> {
        self.results.push(x);
        Ok(())
    }
    fn warning(&mut self, x: ParseWarning) -> Result<(), SinkError> {
        self.warnings.push(x);
        Ok(())
    }
    fn cancelled(&self) -> &AtomicBool {
        &self.cancel
    }
}
fn options(len: u64) -> ParseOptions {
    ParseOptions {
        max_record_bytes: 1024 * 1024,
        max_source_bytes: 2 * 1024 * 1024,
        observe_len: len,
        include_subagents: false,
        blob_roots: vec![],
    }
}
fn run(a: &dyn HarnessAdapter, bytes: &[u8]) -> (ParsedSessionManifest, Sink) {
    let mut source = Cursor::new(bytes);
    let mut sink = Sink::default();
    let manifest = a
        .parse_snapshot(&mut source, &options(bytes.len() as u64), &mut sink)
        .unwrap();
    (manifest, sink)
}
fn corpus(s: &Sink) -> String {
    s.messages
        .iter()
        .flat_map(|m| m.blocks.iter().map(|b| b.text.as_str()))
        .collect::<Vec<_>>()
        .join(" ")
}
#[test]
fn omp_title_slot_v3_filters_blocks_and_keeps_control_ancestry() {
    let b = include_bytes!("../fixtures/omp/v3.jsonl");
    let (m, s) = run(&OmpAdapter, b);
    assert_eq!(m.native_session_id, "omp-fixture-1");
    assert_eq!(m.native_title.as_deref(), Some("Fixture OMP"));
    assert_eq!(s.messages.len(), 2);
    let all = corpus(&s);
    assert!(all.contains("hello fixture") && all.contains("done"));
    assert!(!all.contains("SENTINEL"));
    assert_eq!(s.records[2].parent_key.as_deref(), Some("00000001"));
    assert_eq!(s.records.len(), 5);
}
#[test]
fn codex_canonical_rollout_keeps_phases_and_excludes_payloads() {
    let b = include_bytes!("../fixtures/codex/canonical.jsonl");
    let (m, s) = run(&CodexAdapter, b);
    assert_eq!(m.native_session_id, "codex-fixture-1");
    assert_eq!(s.messages.len(), 2);
    assert_eq!(s.messages[1].phase.as_deref(), Some("final_answer"));
    assert_eq!(m.coverage, Coverage::Complete);
    assert_eq!(m.counts.unknown_records, 0);
    assert!(!corpus(&s).contains("SENTINEL"));
}
#[test]
fn claude_saved_updates_replace_text_and_scope_repeated_tool_ids() {
    let b = include_bytes!("../fixtures/claude/saved-messages.jsonl");
    let (m, s) = run(&ClaudeAdapter, b);
    assert_eq!(m.native_session_id, "claude-fixture-1");
    assert_eq!(m.coverage, Coverage::Partial);
    assert_eq!(s.messages.len(), 4);
    assert!(corpus(&s).contains("done authoritative"));
    assert!(corpus(&s).contains("second turn"));
    assert!(!corpus(&s).contains("done initial"));
    assert!(!corpus(&s).contains("SENTINEL"));
    assert_eq!(s.records[1].parent_key.as_deref(), Some("claude-u1"));
    assert!(s.records.iter().any(|r| r.key == "claude-a1-update"));
    let latest = s
        .messages
        .iter()
        .find(|message| message.record_key == "claude-a1-update")
        .unwrap();
    assert_eq!(latest.supersedes, vec!["claude-a1"]);
    assert_eq!(s.calls.len(), 3);
    assert_eq!(
        s.calls
            .iter()
            .find(|call| call.native_call_id.as_deref() == Some("call-1"))
            .unwrap()
            .record_key,
        "claude-a1"
    );
    let mut call2_records: Vec<_> = s
        .calls
        .iter()
        .filter(|call| call.native_call_id.as_deref() == Some("call-2"))
        .map(|call| call.record_key.as_str())
        .collect();
    call2_records.sort_unstable();
    assert_eq!(call2_records, ["claude-a1-update", "claude-a2"]);
    assert_eq!(s.results.len(), 3);
    assert_eq!(
        s.results
            .iter()
            .filter(|result| result.native_call_id.as_deref() == Some("call-2"))
            .count(),
        2
    );
}
#[test]
fn trailing_partial_jsonl_is_not_emitted() {
    let mut b = include_bytes!("../fixtures/omp/v3.jsonl").to_vec();
    let valid = b.len();
    b.extend_from_slice(b"{\"type\":\"message\"");
    let (m, s) = run(&OmpAdapter, &b);
    assert_eq!(m.prefix_end, valid as u64);
    assert_eq!(s.records.len(), 5);
}
#[test]
fn probe_rejects_non_matching_format() {
    assert_eq!(
        OmpAdapter.probe(b"{\"type\":\"response_item\"}"),
        spool::adapters::ProbeResult::NoMatch
    );
}
