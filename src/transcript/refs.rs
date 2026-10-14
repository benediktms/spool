//! Payloads of the signed opaque references (spec 14.2.3). Shared by search (issues match and
//! branch refs) and the reader (resolves every ref kind). All refs name a lease + manifest so a
//! read can validate retention, and a concrete snapshot so rewrites cannot silently rebase them.
//! Serialization is compact JSON; signing/verification is `HandleKey`.

use super::handle::{HandleError, HandleKey, HandleKind};
use serde::{Deserialize, Serialize};

/// An original conversational span a match or message ref anchors to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanRef {
    /// Record key of the message (`messages.record_key`).
    pub r: String,
    /// Block index inside the record.
    pub b: u32,
    /// UTF-8 byte range `[s, e)` inside the block text.
    pub s: u32,
    pub e: u32,
}

/// Common anchor fields: which session snapshot (and lease) the ref belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    pub lease: String,
    pub manifest: String,
    pub session: String,
    pub snapshot: String,
}

/// View-independent: binds source identity, anchored original spans and expected content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    pub spans: Vec<SpanRef>,
    /// blake3 (hex, first 32 chars) of the concatenated anchored span bytes.
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    pub record: String,
    /// Record digest (hex, first 32 chars) at observation time.
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    Message,
    ToolCall,
    ToolResult,
    RawRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    pub kind: ItemKind,
    pub record: String,
    pub block: Option<u32>,
    pub digest: String,
}

/// A persisted path through the frozen record graph, named by its leaf message record key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    pub leaf: String,
}

macro_rules! ref_codec {
    ($t:ty, $kind:expr) => {
        impl $t {
            pub fn encode(&self, key: &HandleKey) -> String {
                key.sign($kind, &serde_json::to_vec(self).expect("ref serializes"))
            }
            pub fn decode(key: &HandleKey, token: &str) -> Result<Self, HandleError> {
                let payload = key.verify($kind, token)?;
                serde_json::from_slice(&payload).map_err(|_| HandleError::Malformed)
            }
        }
    };
}
ref_codec!(MatchRef, HandleKind::Match);
ref_codec!(MessageRef, HandleKind::Message);
ref_codec!(ItemRef, HandleKind::Item);
ref_codec!(BranchRef, HandleKind::Branch);

/// Short content/record digest used inside refs.
pub fn short_digest(bytes: &[u8]) -> String {
    crate::ids::hex(&crate::domain::digest(bytes))[..32].to_string()
}
