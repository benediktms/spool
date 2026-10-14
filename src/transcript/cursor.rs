//! Signed, reusable continuation cursors (spec 14.2.2.4 / 14.2.8). A cursor binds the resolved
//! selection, frozen snapshot, branch, view, source policy, next item/block/byte offset, the token
//! format version and the lease expiry. It is never consumed: replay yields the same position.

use super::handle::{HandleError, HandleKey, HandleKind};
use super::refs::Anchor;
use super::selection::ResolvedSel;
use super::types::{SourcePolicy, View};
use serde::{Deserialize, Serialize};

/// Token payload format; bump on any incompatible change (old cursors then fail explicitly).
pub const CURSOR_FORMAT: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CursorPayload {
    pub f: u8,
    pub a: Anchor,
    /// Lease expiry (unix ms) at issue time.
    pub x: i64,
    pub v: View,
    pub p: SourcePolicy,
    pub s: ResolvedSel,
    /// Next plan item index, block ordinal within the item, byte offset within that block.
    pub i: u32,
    pub b: u32,
    pub o: u64,
    /// Short hash of the next plan item key (detects a divergent plan).
    pub n: String,
}

impl CursorPayload {
    pub fn encode(&self, key: &HandleKey) -> String {
        key.sign(
            HandleKind::Cursor,
            &serde_json::to_vec(self).expect("cursor serializes"),
        )
    }

    pub fn decode(key: &HandleKey, token: &str) -> Result<Self, HandleError> {
        let payload = key.verify(HandleKind::Cursor, token)?;
        let c: Self = serde_json::from_slice(&payload).map_err(|_| HandleError::Malformed)?;
        if c.f != CURSOR_FORMAT {
            return Err(HandleError::UnsupportedVersion);
        }
        Ok(c)
    }

    /// Same payload with worst-case numeric widths; its token length bounds any real cursor of
    /// this read, so budgeting with it can only be conservative.
    pub fn worst_case(&self) -> Self {
        Self {
            i: u32::MAX,
            b: u32::MAX,
            o: u64::MAX,
            ..self.clone()
        }
    }
}

pub fn short_hash(s: &str) -> String {
    super::refs::short_digest(s.as_bytes())[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper() {
        let key = HandleKey::for_test(1);
        let c = CursorPayload {
            f: CURSOR_FORMAT,
            a: Anchor {
                lease: "l".into(),
                manifest: "m".into(),
                session: "s".into(),
                snapshot: "n".into(),
            },
            x: 5,
            v: View::Conversation,
            p: SourcePolicy::PreferSource,
            s: ResolvedSel {
                kind: super::super::types::SelKind::Session,
                leaf: None,
                first: None,
                last: None,
                anchors: vec![],
                item: None,
                before: None,
                after: None,
            },
            i: 1,
            b: 2,
            o: 3,
            n: short_hash("k"),
        };
        let t = c.encode(&key);
        assert_eq!(CursorPayload::decode(&key, &t).unwrap(), c);
        assert!(c.worst_case().encode(&key).len() >= t.len());
        let mut bad = t.clone();
        bad.pop();
        bad.push('A');
        assert!(CursorPayload::decode(&key, &bad).is_err());
    }
}
