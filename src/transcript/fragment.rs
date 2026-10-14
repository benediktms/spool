//! Block fragmentation (spec 14.2.7.1): half-open byte ranges over the full decoded block, never
//! splitting a UTF-8 code point, base64 for blocks that are not valid UTF-8.

use super::types::{BlockClass, Encoding, FidelityOut, Fragment};
use base64::Engine;

/// One complete decoded block ready to be fragmented.
#[derive(Debug, Clone)]
pub struct BlockData {
    pub block_ref: String,
    pub kind: BlockClass,
    pub fidelity: FidelityOut,
    pub bytes: Vec<u8>,
    pub encoding: Encoding,
}

impl BlockData {
    /// Encoding is `utf8` only when the *whole* block is valid UTF-8, so every fragment of one block
    /// shares one encoding.
    pub fn new(block_ref: String, kind: BlockClass, fidelity: FidelityOut, bytes: Vec<u8>) -> Self {
        let encoding = if std::str::from_utf8(&bytes).is_ok() {
            Encoding::Utf8
        } else {
            Encoding::Base64
        };
        Self {
            block_ref,
            kind,
            fidelity,
            bytes,
            encoding,
        }
    }

    pub fn total(&self) -> usize {
        self.bytes.len()
    }

    /// Smallest valid fragment length starting at `start` (one whole code point for UTF-8).
    pub fn min_step(&self, start: usize) -> usize {
        match self.encoding {
            Encoding::Base64 => 1,
            Encoding::Utf8 => {
                let mut n = 1;
                while start + n < self.bytes.len() && (self.bytes[start + n] & 0xC0) == 0x80 {
                    n += 1;
                }
                n
            }
        }
    }

    /// Largest valid end `<= want` for a fragment that starts at `start` (`want > start`).
    /// Returns `start` when not even one code point fits.
    pub fn cut(&self, start: usize, want: usize) -> usize {
        let want = want.min(self.bytes.len());
        match self.encoding {
            Encoding::Base64 => want,
            Encoding::Utf8 => {
                let mut e = want;
                while e > start && e < self.bytes.len() && (self.bytes[e] & 0xC0) == 0x80 {
                    e -= 1;
                }
                e
            }
        }
    }

    pub fn fragment(&self, start: usize, end: usize) -> Fragment {
        let slice = &self.bytes[start..end];
        let data = match self.encoding {
            // `cut` guarantees code-point boundaries and the whole block was validated as UTF-8.
            Encoding::Utf8 => String::from_utf8_lossy(slice).into_owned(),
            Encoding::Base64 => base64::engine::general_purpose::STANDARD.encode(slice),
        };
        Fragment {
            block_ref: self.block_ref.clone(),
            kind: self.kind,
            encoding: self.encoding,
            fidelity: self.fidelity,
            data,
            start_byte: start as u64,
            end_byte: end as u64,
            total_bytes: self.bytes.len() as u64,
            is_last: end == self.bytes.len(),
        }
    }
}

/// Binary search the longest valid fragment end in `(start, start + max_len]` for which `fits(end)`
/// holds. `fits` must be monotone (true for short, false for long); `max_len` is any known upper
/// bound on the fragment length (every decoded byte costs at least one serialized byte, so the
/// response cap is one) and keeps probes cheap. `None` when even the minimal fragment does not fit.
pub fn longest_fit(
    block: &BlockData,
    start: usize,
    max_len: usize,
    mut fits: impl FnMut(usize) -> bool,
) -> Option<usize> {
    let total = block.total();
    debug_assert!(start < total);
    let first = block.cut(start, start + block.min_step(start));
    if first == start || !fits(first) {
        return None;
    }
    let (mut lo, mut hi) = (first, total.min(start.saturating_add(max_len)).max(first));
    // invariant: `lo` fits; answer in [lo, hi]
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let cand = block.cut(start, mid);
        if cand <= lo {
            // only a few bytes left to probe (inside one code point): scan linearly
            for e in lo + 1..=hi {
                let c = block.cut(start, e);
                if c > lo {
                    if fits(c) {
                        lo = c;
                    } else {
                        break;
                    }
                }
            }
            break;
        }
        if fits(cand) {
            lo = cand;
        } else {
            hi = mid - 1;
        }
    }
    Some(lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blk(b: &[u8]) -> BlockData {
        BlockData::new(
            "b".into(),
            BlockClass::Text,
            FidelityOut::Verbatim,
            b.to_vec(),
        )
    }

    #[test]
    fn never_splits_code_points_and_reassembles() {
        let text = "aé€😀z".repeat(50);
        let b = blk(text.as_bytes());
        let mut pos = 0;
        let mut out = Vec::new();
        while pos < b.total() {
            let end = longest_fit(&b, pos, usize::MAX, |e| e - pos <= 5).expect("progress");
            let f = b.fragment(pos, end);
            assert_eq!(f.start_byte as usize, pos);
            out.extend_from_slice(f.data.as_bytes());
            pos = end;
        }
        assert_eq!(out, text.as_bytes());
    }

    #[test]
    fn invalid_utf8_is_base64_and_unaligned() {
        let b = blk(&[0xff, 0xfe, 0x00, 0x41]);
        assert_eq!(b.encoding, Encoding::Base64);
        assert_eq!(b.fragment(1, 3).data, "/gA=");
        assert!(longest_fit(&b, 0, usize::MAX, |_| false).is_none());
    }
}
