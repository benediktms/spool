use spool::{
    adapters::{ParseOptions, RecordSink, SessionAdapter, SinkError},
    domain::*,
};
use std::{io::Cursor, sync::atomic::AtomicBool};

#[derive(Default)]
pub struct Sink {
    pub records: Vec<RecordMeta>,
    pub messages: Vec<MessageEntry>,
    pub calls: Vec<ToolCallMeta>,
    pub results: Vec<ToolResultMeta>,
    pub warnings: Vec<ParseWarning>,
    cancelled: AtomicBool,
}
impl RecordSink for Sink {
    fn record(&mut self, record: RecordMeta) -> Result<(), SinkError> {
        self.records.push(record);
        Ok(())
    }
    fn message(&mut self, message: MessageEntry) -> Result<(), SinkError> {
        self.messages.retain(|previous| {
            !message
                .supersedes
                .iter()
                .any(|record_key| record_key == &previous.record_key)
        });
        self.messages.push(message);
        Ok(())
    }
    fn tool_call(&mut self, call: ToolCallMeta) -> Result<(), SinkError> {
        self.calls.push(call);
        Ok(())
    }
    fn tool_result(&mut self, result: ToolResultMeta) -> Result<(), SinkError> {
        self.results.push(result);
        Ok(())
    }
    fn warning(&mut self, warning: ParseWarning) -> Result<(), SinkError> {
        self.warnings.push(warning);
        Ok(())
    }
    fn cancelled(&self) -> &AtomicBool {
        &self.cancelled
    }
}

pub fn options(len: u64, max_record_bytes: u64) -> ParseOptions {
    ParseOptions {
        max_record_bytes,
        max_source_bytes: len.max(4 * 1024 * 1024),
        observe_len: len,
        include_subagents: false,
        blob_roots: vec![],
    }
}

pub fn assert_moved_locators(
    adapter: &dyn SessionAdapter,
    bytes: &[u8],
    records: &[RecordMeta],
    max_record_bytes: u64,
) {
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("original.jsonl");
    let moved = directory.path().join("moved.jsonl");
    std::fs::write(&original, bytes).unwrap();
    std::fs::rename(&original, &moved).unwrap();
    let mut source = std::fs::File::open(moved).unwrap();
    for record in records {
        let start = record.locator.offset as usize;
        let end = start + record.locator.len as usize;
        let resolved = adapter
            .resolve_locator(
                &mut source,
                &record.locator,
                &record.digest,
                max_record_bytes,
            )
            .unwrap();
        assert_eq!(resolved.raw, bytes[start..end]);
        assert_eq!(resolved.digest, record.digest);
    }
}
pub fn parse(adapter: &dyn SessionAdapter, bytes: &[u8]) -> (ParsedSessionManifest, Sink) {
    let mut source = Cursor::new(bytes);
    let mut sink = Sink::default();
    let manifest = adapter
        .parse_snapshot(
            &mut source,
            &options(bytes.len() as u64, 4 * 1024 * 1024),
            &mut sink,
        )
        .unwrap();
    (manifest, sink)
}
