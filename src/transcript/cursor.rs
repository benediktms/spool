//! Signed, reusable continuation cursors (spec 14.2.2.4 / 14.2.8). A cursor binds the resolved
//! selection, frozen snapshot, branch, view, source policy, next item/block/byte offset, the token
//! format version and the lease expiry. It is never consumed: replay yields the same position.

use super::handle::{HandleError, HandleKey, HandleKind};
use super::selection::ResolvedSel;
use super::types::{SourcePolicy, View};
use serde::{Deserialize, Serialize};

/// Token payload format; bump on any incompatible change (old cursors then fail explicitly).
pub const CURSOR_FORMAT: u8 = 3;

/// Stored once per read in `lease_refs` (kind `cursor`); every cursor of the read names it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CursorSel {
    pub m: String,
    pub v: View,
    pub p: SourcePolicy,
    pub s: ResolvedSel,
}

/// Signed cursor body, serialized as the array `[format, lease, ref_id, verbose, i, b, o, n]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorPayload {
    pub lease: String,
    pub ref_id: i64,
    /// `verbose` output of the initial read.
    pub d: bool,
    /// Next plan item index, block ordinal within the item, byte offset within that block.
    pub i: u32,
    pub b: u32,
    pub o: u64,
    /// Short hash of the next plan item key (detects a divergent plan).
    pub n: String,
}

type Wire = (u8, String, i64, bool, u32, u32, u64, String);

impl CursorPayload {
    pub fn encode(&self, key: &HandleKey) -> String {
        let wire = (
            CURSOR_FORMAT,
            &self.lease,
            self.ref_id,
            self.d,
            self.i,
            self.b,
            self.o,
            &self.n,
        );
        key.sign(
            HandleKind::Cursor,
            &serde_json::to_vec(&wire).expect("cursor serializes"),
        )
    }

    pub fn decode(key: &HandleKey, token: &str) -> Result<Self, HandleError> {
        let payload = key.verify(HandleKind::Cursor, token)?;
        let Ok((f, lease, ref_id, d, i, b, o, n)) = serde_json::from_slice::<Wire>(&payload) else {
            return Err(
                match serde_json::from_slice::<serde_json::Value>(&payload) {
                    Ok(v) if v.is_object() => HandleError::UnsupportedVersion,
                    _ => HandleError::Malformed,
                },
            );
        };
        if f != CURSOR_FORMAT {
            return Err(HandleError::UnsupportedVersion);
        }
        Ok(Self {
            lease,
            ref_id,
            d,
            i,
            b,
            o,
            n,
        })
    }

    /// Same payload with worst-case numeric widths; its token length bounds any real cursor of
    /// this read, so budgeting with it can only be conservative.
    pub fn worst_case(&self) -> Self {
        Self {
            ref_id: i64::MAX,
            d: true,
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
            lease: "l".into(),
            ref_id: 1,
            d: false,
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
