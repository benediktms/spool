//! Shared JSONL pipeline. A harness supplies format-specific hooks; the blanket
//! `HarnessAdapter` impl owns the read loop, cancellation, byte limits, locators and digests,
//! malformed-line warnings, manifest assembly, and digest-verified resolution.

use super::{AdapterError, HarnessAdapter, ParseOptions, ProbeResult, ReadSeek, RecordSink};
use crate::domain::{
    BlockKind, Coverage, Digest, Fidelity, HarnessKind, MessageEntry, ParseCounts, ParseWarning,
    ParsedSessionManifest, RecordKind, RecordMeta, ResolvedBlock, ResolvedRecord, SessionKind,
    SourceLocator, ToolCallMeta, ToolResultMeta, digest,
};
use crate::ids;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, SeekFrom};
use std::sync::atomic::Ordering;

pub(crate) trait JsonlHarness: Send + Sync {
    const KIND: HarnessKind;
    /// Part of the pipeline fingerprint and of every fallback record key.
    const PARSER_VERSION: &'static str;
    /// `ParseWarning::detail` for a complete line that does not decode as `Parser::Line`.
    const MALFORMED_DETAIL: &'static str;
    type Parser: LineParser;

    fn probe_header(&self, header: &[u8]) -> ProbeResult;
    /// `line` is the digest-verified record without its trailing `\n`.
    fn resolve_line(
        &self,
        line: &[u8],
    ) -> Result<(Option<String>, Vec<ResolvedBlock>), AdapterError>;
}

pub(crate) trait LineParser: Default {
    type Line: serde::de::DeserializeOwned;
    /// Called once per complete, decodable line in file order; emits zero or one record via
    /// `cx.emit`.
    fn record(
        &mut self,
        cx: &mut ParseCx<'_>,
        line: Self::Line,
        raw: RawRecord<'_>,
    ) -> Result<(), AdapterError>;
    /// Parent key for the malformed record `key`.
    fn malformed_parent(&mut self, _key: &str) -> Option<String> {
        None
    }
}

/// One complete physical line. `bytes` includes the trailing `\n`.
#[derive(Clone, Copy)]
pub(crate) struct RawRecord<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) locator: SourceLocator,
    pub(crate) digest: Digest,
}

pub(crate) struct SessionFacts {
    pub(crate) native_id: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) cwd: Option<String>,
    pub(crate) parent_native_session: Option<String>,
    pub(crate) kind: SessionKind,
}

impl Default for SessionFacts {
    fn default() -> Self {
        Self {
            native_id: None,
            title: None,
            cwd: None,
            parent_native_session: None,
            kind: SessionKind::Unknown,
        }
    }
}

#[derive(Default)]
pub(crate) struct Emission {
    pub(crate) message: Option<MessageEntry>,
    pub(crate) calls: Vec<ToolCallMeta>,
    pub(crate) results: Vec<ToolResultMeta>,
}

pub(crate) struct ParseCx<'a> {
    pub(crate) options: &'a ParseOptions,
    pub(crate) counts: ParseCounts,
    pub(crate) coverage: Coverage,
    pub(crate) session: SessionFacts,
    pub(crate) started_at: Option<i64>,
    pub(crate) last_activity_at: Option<i64>,
    version: &'static str,
    sink: &'a mut dyn RecordSink,
}

impl<'a> ParseCx<'a> {
    fn new(options: &'a ParseOptions, version: &'static str, sink: &'a mut dyn RecordSink) -> Self {
        Self {
            options,
            counts: ParseCounts::default(),
            coverage: Coverage::Complete,
            session: SessionFacts::default(),
            started_at: None,
            last_activity_at: None,
            version,
            sink,
        }
    }

    pub(crate) fn observe_activity(&mut self, timestamp: Option<i64>) {
        if let Some(timestamp) = timestamp {
            self.last_activity_at = Some(
                self.last_activity_at
                    .map_or(timestamp, |last| last.max(timestamp)),
            );
        }
    }

    pub(crate) fn fallback_key(&self, digest: &Digest, occurrence: u64) -> String {
        format!(
            "fallback:{}:{}:{occurrence}",
            self.version,
            ids::hex(digest)
        )
    }

    pub(crate) fn count_message(&mut self, blocks: usize) {
        self.counts.messages += 1;
        self.counts.blocks_retained += blocks as u64;
    }

    /// Sink order is part of the contract: record, message, calls, results.
    pub(crate) fn emit(
        &mut self,
        meta: RecordMeta,
        emission: Emission,
    ) -> Result<(), AdapterError> {
        self.sink.record(meta)?;
        if let Some(message) = emission.message {
            self.sink.message(message)?;
        }
        for call in emission.calls {
            self.sink.tool_call(call)?;
        }
        for result in emission.results {
            self.sink.tool_result(result)?;
        }
        Ok(())
    }

    /// `prefix_end` is the end of the last complete record consumed.
    fn finish(self, prefix_end: u64) -> ParsedSessionManifest {
        ParsedSessionManifest {
            native_session_id: self.session.native_id.unwrap_or_else(|| "unknown".into()),
            native_title: self.session.title,
            cwd: self.session.cwd,
            session_kind: self.session.kind,
            parent_native_session: self.session.parent_native_session,
            lineage_verified: false,
            started_at: self.started_at,
            last_activity_at: self.last_activity_at,
            prefix_end,
            active_leaf_key: None,
            coverage: self.coverage,
            counts: self.counts,
        }
    }
}

/// Native identity with a `~n` suffix for repeats.
#[derive(Default)]
pub(crate) struct Occurrences(HashMap<String, usize>);

impl Occurrences {
    pub(crate) fn key(&mut self, identity: &str) -> String {
        let occurrence = self.0.entry(identity.to_owned()).or_default();
        let key = if *occurrence == 0 {
            identity.to_owned()
        } else {
            format!("{identity}~{occurrence}")
        };
        *occurrence += 1;
        key
    }
}

pub(crate) fn first_line(header: &[u8]) -> &[u8] {
    header.split(|b| *b == b'\n').next().unwrap_or_default()
}

pub(crate) fn verbatim_block(index: u32, kind: BlockKind, text: &str) -> ResolvedBlock {
    ResolvedBlock {
        index,
        kind,
        fidelity: Fidelity::Verbatim,
        bytes: text.as_bytes().to_vec(),
    }
}

/// A string is kept verbatim; any other value is serialized as normalized JSON.
pub(crate) fn json_block(
    index: u32,
    kind: BlockKind,
    value: &Value,
    error: &'static str,
) -> Result<ResolvedBlock, AdapterError> {
    if let Some(text) = value.as_str() {
        return Ok(verbatim_block(index, kind, text));
    }
    Ok(ResolvedBlock {
        index,
        kind,
        fidelity: Fidelity::NormalizedJson,
        bytes: serde_json::to_vec(value).map_err(|_| AdapterError::Unsupported(error.into()))?,
    })
}

/// A string, or a one-item array whose item has a string `text`, is kept verbatim.
pub(crate) fn tool_result_block(
    index: u32,
    value: &Value,
    error: &'static str,
) -> Result<ResolvedBlock, AdapterError> {
    let single_text = value
        .as_array()
        .and_then(|items| (items.len() == 1).then(|| &items[0]))
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str);
    match value.as_str().or(single_text) {
        Some(text) => Ok(verbatim_block(index, BlockKind::ToolResult, text)),
        None => json_block(index, BlockKind::ToolResult, value, error),
    }
}

/// `None` ends the readable prefix: EOF, an unterminated line, or a line crossing a byte limit.
fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    offset: u64,
    o: &ParseOptions,
) -> Result<Option<usize>, AdapterError> {
    if offset >= o.observe_len || offset >= o.max_source_bytes {
        return Ok(None);
    }

    line.clear();
    let n = reader.read_until(b'\n', line)?;
    let end = offset + n as u64;
    if n == 0 || line.last() != Some(&b'\n') || end > o.observe_len || end > o.max_source_bytes {
        return Ok(None);
    }
    if n as u64 > o.max_record_bytes {
        return Err(AdapterError::LimitExceeded);
    }

    Ok(Some(n))
}

fn parse_snapshot<H: JsonlHarness>(
    source: &mut dyn Read,
    options: &ParseOptions,
    sink: &mut dyn RecordSink,
) -> Result<ParsedSessionManifest, AdapterError> {
    let mut reader = BufReader::new(source);
    let mut line = Vec::new();
    let mut offset = 0u64;
    let mut parser = H::Parser::default();
    let mut cx = ParseCx::new(options, H::PARSER_VERSION, sink);

    loop {
        if cx.sink.cancelled().load(Ordering::Relaxed) {
            return Err(AdapterError::Cancelled);
        }
        let Some(n) = read_bounded_line(&mut reader, &mut line, offset, options)? else {
            break;
        };

        let raw = RawRecord {
            bytes: &line,
            locator: SourceLocator {
                offset,
                len: n as u64,
            },
            digest: digest(&line),
        };
        cx.counts.bytes_read += n as u64;
        cx.counts.records += 1;
        match serde_json::from_slice::<<H::Parser as LineParser>::Line>(&line) {
            Ok(decoded) => parser.record(&mut cx, decoded, raw)?,
            Err(_) => malformed::<H>(&mut cx, &mut parser, raw)?,
        }
        offset += n as u64;
    }

    Ok(cx.finish(offset))
}

fn malformed<H: JsonlHarness>(
    cx: &mut ParseCx<'_>,
    parser: &mut H::Parser,
    raw: RawRecord<'_>,
) -> Result<(), AdapterError> {
    let key = cx.fallback_key(&raw.digest, cx.counts.malformed_records);
    let meta = RecordMeta {
        key: key.clone(),
        native_id: None,
        parent_key: parser.malformed_parent(&key),
        kind: RecordKind::Unknown,
        timestamp: None,
        turn_id: None,
        locator: raw.locator,
        digest: raw.digest,
    };
    cx.emit(meta, Emission::default())?;
    cx.sink.warning(ParseWarning {
        code: "invalid_record".into(),
        record_key: Some(key),
        locator: Some(raw.locator),
        detail: H::MALFORMED_DETAIL.into(),
    })?;

    cx.counts.malformed_records += 1;
    cx.counts.unknown_records += 1;
    cx.coverage = Coverage::Partial;
    Ok(())
}

impl<H: JsonlHarness> HarnessAdapter for H {
    fn kind(&self) -> HarnessKind {
        H::KIND
    }

    fn parser_version(&self) -> &'static str {
        H::PARSER_VERSION
    }

    fn probe(&self, header: &[u8]) -> ProbeResult {
        self.probe_header(header)
    }

    fn parse_snapshot(
        &self,
        source: &mut dyn Read,
        options: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError> {
        parse_snapshot::<H>(source, options, sink)
    }

    fn resolve_locator(
        &self,
        source: &mut dyn ReadSeek,
        locator: &SourceLocator,
        expected: &Digest,
        max_record_bytes: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        let (raw, digest) = read_verified(source, locator, expected, max_record_bytes)?;
        let (native_id, blocks) = self.resolve_line(raw.strip_suffix(b"\n").unwrap_or(&raw))?;
        Ok(ResolvedRecord {
            raw,
            digest,
            native_id,
            blocks,
        })
    }
}

impl<H: JsonlHarness> super::sealed::Sealed for H {}

/// Reads the located source slice and verifies it against the expected digest.
fn read_verified(
    source: &mut dyn ReadSeek,
    locator: &SourceLocator,
    expected: &Digest,
    max: u64,
) -> Result<(Vec<u8>, Digest), AdapterError> {
    if locator.len > max {
        return Err(AdapterError::LimitExceeded);
    }

    source.seek(SeekFrom::Start(locator.offset))?;
    let mut raw = vec![0; locator.len as usize];
    source.read_exact(&mut raw)?;

    let d = digest(&raw);
    if &d != expected {
        return Err(AdapterError::LocatorMismatch("digest changed".into()));
    }
    Ok((raw, d))
}
