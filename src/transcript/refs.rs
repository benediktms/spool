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

/// Common anchor fields: the lease and manifest a ref belongs to; session and snapshot derive from the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anchor {
    #[serde(rename = "l")]
    pub lease: String,
    #[serde(rename = "m")]
    pub manifest: String,
}

/// View-independent payload stored with the lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchRef {
    #[serde(rename = "m")]
    pub manifest: String,
    pub version: u32,
    pub records: Vec<MatchRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchRecord {
    pub key: String,
    pub digest: String,
}

/// Signed handle to a `lease_refs` row: `[lease, ref_id]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefHandle {
    pub lease: String,
    pub ref_id: i64,
}

impl RefHandle {
    pub fn encode(&self, key: &HandleKey, kind: HandleKind) -> String {
        key.sign(
            kind,
            &serde_json::to_vec(&(&self.lease, self.ref_id)).expect("ref serializes"),
        )
    }

    pub fn decode(key: &HandleKey, kind: HandleKind, token: &str) -> Result<Self, HandleError> {
        let payload = key.verify(kind, token)?;
        let (lease, ref_id): (String, i64) =
            serde_json::from_slice(&payload).map_err(|_| HandleError::Malformed)?;
        Ok(Self { lease, ref_id })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    #[serde(rename = "r")]
    pub record: String,
    /// Record digest (hex, first 32 chars) at observation time.
    #[serde(rename = "d")]
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
    #[serde(rename = "k")]
    pub kind: ItemKind,
    #[serde(rename = "r")]
    pub record: String,
    #[serde(rename = "b")]
    pub block: Option<u32>,
    #[serde(rename = "d")]
    pub digest: String,
}

/// A persisted path through the frozen record graph, named by its leaf message record key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    #[serde(flatten)]
    pub anchor: Anchor,
    #[serde(rename = "f")]
    pub leaf: String,
}

macro_rules! ref_codec {
    ($t:ident, $kind:expr, ($($field:ident : $ty:ty),+), $v:ident => $encode:expr, $build:expr) => {
        impl $t {
            pub fn encode(&self, key: &HandleKey) -> String {
                let $v = self;
                key.sign_ref($kind, &serde_json::to_vec(&$encode).expect("ref serializes"))
            }

            pub fn decode(key: &HandleKey, token: &str) -> Result<Self, HandleError> {
                let (version, payload) = key.verify_ref($kind, token)?;
                if version == 1 {
                    serde_json::from_slice(&payload).map_err(|_| HandleError::Malformed)
                } else {
                    let ($($field,)+): ($($ty,)+) =
                        serde_json::from_slice(&payload).map_err(|_| HandleError::Malformed)?;
                    Ok($build($($field),+))
                }
            }
        }
    };
}
ref_codec!(MessageRef, HandleKind::Message,
    (lease: String, manifest: String, record: String, digest: String),
    v => (&v.anchor.lease, &v.anchor.manifest, &v.record, &v.digest),
    |lease, manifest, record, digest| MessageRef { anchor: Anchor { lease, manifest }, record, digest }
);
ref_codec!(ItemRef, HandleKind::Item,
    (lease: String, manifest: String, kind: ItemKind, record: String, block: Option<u32>, digest: String),
    v => (&v.anchor.lease, &v.anchor.manifest, v.kind, &v.record, v.block, &v.digest),
    |lease, manifest, kind, record, block, digest| ItemRef { anchor: Anchor { lease, manifest }, kind, record, block, digest }
);
ref_codec!(BranchRef, HandleKind::Branch,
    (lease: String, manifest: String, leaf: String),
    v => (&v.anchor.lease, &v.anchor.manifest, &v.leaf),
    |lease, manifest, leaf| BranchRef { anchor: Anchor { lease, manifest }, leaf }
);

/// Short content/record digest used inside refs.
pub fn short_digest(bytes: &[u8]) -> String {
    crate::ids::hex(&crate::domain::digest(bytes))[..32].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_and_legacy_refs_preserve_values_and_verify_first() {
        let key = HandleKey::for_test(1);
        let anchor = Anchor {
            lease: "lease".into(),
            manifest: "manifest".into(),
        };
        let message = MessageRef {
            anchor: anchor.clone(),
            record: "record".into(),
            digest: "digest".into(),
        };
        let item = ItemRef {
            anchor: anchor.clone(),
            kind: ItemKind::ToolResult,
            record: "record".into(),
            block: None,
            digest: "digest".into(),
        };
        let branch = BranchRef {
            anchor,
            leaf: "leaf".into(),
        };
        macro_rules! check {
            ($value:expr, $type:ty, $kind:expr, $expected:expr) => {{
                let token = $value.encode(&key);
                assert_eq!(
                    key.verify_ref($kind, &token).unwrap().1,
                    $expected.as_bytes()
                );
                assert_eq!(<$type>::decode(&key, &token).unwrap(), $value);
                assert_eq!(
                    key.verify($kind, &token),
                    Err(HandleError::UnsupportedVersion)
                );
                let legacy = key.sign($kind, &serde_json::to_vec(&$value).unwrap());
                assert_eq!(<$type>::decode(&key, &legacy).unwrap(), $value);
                assert_eq!(
                    <$type>::decode(&HandleKey::for_test(2), &token),
                    Err(HandleError::KeyRotated)
                );
                let invalid = key.sign_ref($kind, b"invalid");
                let mut tampered = invalid.into_bytes();
                let last = tampered.len() - 1;
                tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
                assert!(matches!(
                    <$type>::decode(&key, std::str::from_utf8(&tampered).unwrap()),
                    Err(HandleError::BadSignature) | Err(HandleError::Malformed)
                ));
            }};
        }
        check!(
            message,
            MessageRef,
            HandleKind::Message,
            r#"["lease","manifest","record","digest"]"#
        );
        check!(
            item,
            ItemRef,
            HandleKind::Item,
            r#"["lease","manifest","tool_result","record",null,"digest"]"#
        );
        check!(
            branch,
            BranchRef,
            HandleKind::Branch,
            r#"["lease","manifest","leaf"]"#
        );
        assert_eq!(
            MessageRef::decode(&key, &branch.encode(&key)),
            Err(HandleError::WrongKind)
        );
        for payload in [
            r#"["l","m","r"]"#,
            r#"["l","m","r","d",0]"#,
            r#"["l","m",4,"d"]"#,
        ] {
            assert_eq!(
                MessageRef::decode(&key, &key.sign_ref(HandleKind::Message, payload.as_bytes())),
                Err(HandleError::Malformed)
            );
        }
        for payload in [
            r#"["l","m","tool_result","r",4294967296,"d"]"#,
            r#"["l","m","unknown","r",null,"d"]"#,
            r#"["l","m","tool_result","r","d"]"#,
        ] {
            assert_eq!(
                ItemRef::decode(&key, &key.sign_ref(HandleKind::Item, payload.as_bytes())),
                Err(HandleError::Malformed)
            );
        }
        assert_eq!(
            BranchRef::decode(&key, &key.sign_ref(HandleKind::Branch, br#"["l","m"]"#)),
            Err(HandleError::Malformed)
        );
    }
}
