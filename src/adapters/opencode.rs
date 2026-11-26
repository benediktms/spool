//! OpenCode V2 record interpretation, independent of the SQLite source.

use super::{RecordSink, SinkError};
use crate::domain::{
    BlockKind, BlockOrigin, Digest, Fidelity, MessageEntry, RecordKind, RecordMeta, ResolvedBlock,
    ResolvedRecord, Role, SourceLocator, TextBlock, digest,
};
use serde_json::Value;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub(crate) enum ProjectionError {
    Unsupported,
    Limit,
}

pub(crate) struct MessageProjection {
    id: String,
    seq: i64,
    role: String,
    value: Value,
    parts: Vec<(i64, String, Value)>,
}

impl MessageProjection {
    pub(crate) fn new(
        id: String,
        seq: i64,
        role: String,
        raw: &str,
    ) -> Result<Self, ProjectionError> {
        Ok(Self {
            id,
            seq,
            role,
            value: serde_json::from_str(raw).map_err(|_| ProjectionError::Unsupported)?,
            parts: Vec::new(),
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// Parts must arrive in ascending native ordinal order.
    pub(crate) fn push_part(
        &mut self,
        ordinal: i64,
        kind: String,
        raw: &str,
    ) -> Result<(), ProjectionError> {
        let value = serde_json::from_str(raw).map_err(|_| ProjectionError::Unsupported)?;
        self.parts.push((ordinal, kind, value));
        Ok(())
    }

    pub(crate) fn finish(
        self,
        session_id: &str,
        source_bytes: usize,
    ) -> Result<Option<ProjectedMessage>, ProjectionError> {
        let blocks = match self.role.as_str() {
            "user" => text_block(0, &self.value)?.into_iter().collect(),
            "assistant" => self.assistant_blocks()?,
            _ => return Ok(None),
        };
        let canonical_v1 = serde_json::to_vec(&(
            1,
            session_id,
            &self.id,
            self.seq,
            &self.role,
            &self.value,
            &self.parts,
        ))
        .map_err(|_| ProjectionError::Unsupported)?;
        Ok(Some(ProjectedMessage {
            id: self.id,
            seq: self.seq,
            role: self.role,
            blocks,
            digest: digest(&canonical_v1),
            source_bytes,
        }))
    }

    fn assistant_blocks(&self) -> Result<Vec<TextBlock>, ProjectionError> {
        let inline = self
            .value
            .get("content")
            .and_then(Value::as_array)
            .ok_or(ProjectionError::Unsupported)?;
        if !inline.is_empty() && !self.parts.is_empty() {
            return Err(ProjectionError::Unsupported);
        }
        let mut blocks = Vec::new();
        if self.parts.is_empty() {
            for (index, block) in inline.iter().enumerate() {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    let index = u32::try_from(index).map_err(|_| ProjectionError::Limit)?;
                    blocks.extend(text_block(index, block)?);
                }
            }
        } else {
            for (ordinal, kind, block) in &self.parts {
                if kind == "text" {
                    let index =
                        u32::try_from(*ordinal).map_err(|_| ProjectionError::Unsupported)?;
                    blocks.extend(text_block(index, block)?);
                }
            }
        }
        Ok(blocks)
    }
}

fn text_block(index: u32, value: &Value) -> Result<Option<TextBlock>, ProjectionError> {
    let text = value
        .get("text")
        .and_then(Value::as_str)
        .ok_or(ProjectionError::Unsupported)?;
    Ok((!text.is_empty()).then(|| TextBlock {
        index,
        origin: BlockOrigin::Authored,
        text: text.to_owned(),
    }))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProjectedMessage {
    pub(crate) id: String,
    pub(crate) seq: i64,
    pub(crate) role: String,
    pub(crate) blocks: Vec<TextBlock>,
    pub(crate) digest: Digest,
    pub(crate) source_bytes: usize,
}

impl ProjectedMessage {
    pub(crate) fn verify(self, expected: &Digest) -> Option<ResolvedRecord> {
        if &self.digest != expected {
            return None;
        }
        Some(ResolvedRecord {
            raw: Vec::new(),
            digest: self.digest,
            native_id: Some(self.id),
            blocks: self
                .blocks
                .into_iter()
                .map(|block| ResolvedBlock {
                    index: block.index,
                    kind: BlockKind::Text,
                    fidelity: Fidelity::Verbatim,
                    bytes: block.text.into_bytes(),
                })
                .collect(),
        })
    }
}

pub(crate) fn emit_projection(
    session_id: &str,
    projection: Vec<ProjectedMessage>,
    sink: &mut dyn RecordSink,
) -> Result<(), SinkError> {
    let mut parent = None;
    for item in projection {
        if sink.cancelled().load(Ordering::Relaxed) {
            return Err(SinkError);
        }
        let role = match item.role.as_str() {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            _ => return Err(SinkError),
        };
        let key = item.id;
        sink.opencode_record(
            RecordMeta {
                key: key.clone(),
                native_id: Some(key.clone()),
                parent_key: parent,
                kind: RecordKind::Message,
                timestamp: None,
                turn_id: None,
                locator: SourceLocator { offset: 0, len: 0 },
                digest: item.digest,
            },
            session_id,
            &key,
        )?;
        if !item.blocks.is_empty() {
            sink.message(MessageEntry {
                record_key: key.clone(),
                role,
                phase: None,
                blocks: item.blocks,
                attachments: Vec::new(),
                supersedes: Vec::new(),
            })?;
        }
        parent = Some(key);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(reasoning: &str) -> ProjectedMessage {
        let mut message =
            MessageProjection::new("split".into(), 3, "assistant".into(), r#"{"content":[]}"#)
                .unwrap();
        for (ordinal, kind, raw) in [
            (0, "reasoning", reasoning),
            (1, "text", r#"{"type":"text","text":"first"}"#),
            (2, "text", r#"{"type":"text","text":"second"}"#),
        ] {
            message.push_part(ordinal, kind.into(), raw).unwrap();
        }
        message.finish("s", 100).unwrap().unwrap()
    }

    #[test]
    fn opencode_projection_preserves_version_one_digests_and_text() {
        let user =
            MessageProjection::new("u".into(), 1, "user".into(), r#"{"text":"findable user"}"#)
                .unwrap()
                .finish("s", 24)
                .unwrap()
                .unwrap();
        let inline = MessageProjection::new(
            "inline".into(),
            2,
            "assistant".into(),
            r#"{"content":[{"type":"reasoning","text":"secret"},{"type":"text","text":"inline answer"},{"type":"tool","text":"hidden"}]}"#,
        )
        .unwrap()
        .finish("s", 100)
        .unwrap()
        .unwrap();
        let split = split(r#"{"type":"reasoning","text":"hidden"}"#);

        for (message, expected_blocks, expected_digest) in [
            (
                user,
                &[(0, "findable user")][..],
                "d446b780d0bfa832d61a5b83042e2141b04c5025ffecfb6ae0346b970a7570bb",
            ),
            (
                inline,
                &[(1, "inline answer")][..],
                "08f6a0d9e04d2bf8646bd4042ad0640bf8d9712d7cd16575cb520044944f30be",
            ),
            (
                split,
                &[(1, "first"), (2, "second")][..],
                "394abf7801f688176be9f48786e3655fa88591f57681f18c4f9e3eefd89187e4",
            ),
        ] {
            let expected = *blake3::Hash::from_hex(expected_digest).unwrap().as_bytes();
            assert_eq!(message.digest, expected);
            assert_eq!(
                message
                    .blocks
                    .iter()
                    .map(|b| (b.index, b.text.as_str()))
                    .collect::<Vec<_>>(),
                expected_blocks
            );
            let resolved = message.verify(&expected).unwrap();
            assert!(resolved.raw.is_empty());
            assert_eq!(
                resolved
                    .blocks
                    .iter()
                    .map(|b| (b.index, b.bytes.as_slice(), b.fidelity))
                    .collect::<Vec<_>>(),
                expected_blocks
                    .iter()
                    .map(|(i, text)| (*i, text.as_bytes(), Fidelity::Verbatim))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn opencode_projection_hashes_parts_that_are_not_indexed() {
        let before = split(r#"{"type":"reasoning","text":"hidden"}"#);
        let after = split(r#"{"type":"reasoning","text":"changed"}"#);
        assert_eq!(before.blocks, after.blocks);
        assert!(after.verify(&before.digest).is_none());
    }

    #[test]
    fn opencode_projection_filters_records_and_rejects_invalid_data() {
        let message = |role: &str, raw| MessageProjection::new("m".into(), 1, role.into(), raw);
        assert!(
            message("synthetic", "{}")
                .unwrap()
                .finish("s", 0)
                .unwrap()
                .is_none()
        );
        let empty = message("user", r#"{"text":""}"#)
            .unwrap()
            .finish("s", 0)
            .unwrap()
            .unwrap();
        assert!(empty.blocks.is_empty());
        assert!(matches!(
            message("synthetic", "{"),
            Err(ProjectionError::Unsupported)
        ));
        assert!(matches!(
            message("user", r#"{"text":7}"#).unwrap().finish("s", 0),
            Err(ProjectionError::Unsupported)
        ));
        let mut mixed = message(
            "assistant",
            r#"{"content":[{"type":"text","text":"inline"}]}"#,
        )
        .unwrap();
        mixed.push_part(0, "reasoning".into(), "{}").unwrap();
        assert!(matches!(
            mixed.finish("s", 0),
            Err(ProjectionError::Unsupported)
        ));
        let mut split = message("assistant", r#"{"content":[]}"#).unwrap();
        assert!(matches!(
            split.push_part(0, "tool".into(), "{"),
            Err(ProjectionError::Unsupported)
        ));
        split
            .push_part(-1, "text".into(), r#"{"text":"invalid ordinal"}"#)
            .unwrap();
        assert!(matches!(
            split.finish("s", 0),
            Err(ProjectionError::Unsupported)
        ));
    }
}
