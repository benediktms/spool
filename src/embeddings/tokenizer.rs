//! cl100k_base token counting. The BPE ranks are embedded in the `tiktoken-rs` crate (`include_str!`),
//! so nothing is downloaded at runtime.

use crate::ingest::chunk::TokenCounter;
use std::sync::Arc;
use tiktoken_rs::CoreBPE;

pub const IDENTITY: &str = "tiktoken-rs-0.12.1/cl100k_base";

struct Cl100k {
    bpe: &'static CoreBPE,
}

impl TokenCounter for Cl100k {
    fn identity(&self) -> &str {
        IDENTITY
    }

    /// Special-token strings in conversation text are counted as ordinary text.
    fn count(&self, text: &str) -> usize {
        self.bpe.encode_ordinary(text).len()
    }

    /// Byte length of the longest prefix (on a char boundary) that tokenizes to at most `max_tokens`.
    fn prefix_within(&self, text: &str, max_tokens: usize) -> usize {
        if max_tokens == 0 {
            return 0;
        }
        // Tokenize only a window: encoding the whole remainder per call is quadratic when a caller
        // chunks a multi-megabyte message. Start at ~16 bytes/token and double until the window itself
        // exceeds the budget (then the first `max_tokens` tokens are stable) or covers the text.
        let mut win = max_tokens.saturating_mul(16).saturating_add(64);
        let toks = loop {
            let mut w = win.min(text.len());
            while w < text.len() && !text.is_char_boundary(w) {
                w += 1;
            }
            let toks = self.bpe.encode_ordinary(&text[..w]);
            if w == text.len() {
                if toks.len() <= max_tokens {
                    return text.len();
                }
                break toks;
            }
            if toks.len() > max_tokens {
                break toks;
            }
            win = win.saturating_mul(2);
        };
        let bytes = self
            .bpe
            .decode_bytes(&toks[..max_tokens])
            .unwrap_or_default();
        let mut end = bytes.len().min(text.len());
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        // Re-tokenizing a cut prefix can merge differently; shrink until the bound really holds.
        while end > 0 && self.count(&text[..end]) > max_tokens {
            end -= 1;
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
        }
        end
    }
}

pub fn tokenizer() -> Arc<dyn TokenCounter> {
    static T: std::sync::LazyLock<Arc<dyn TokenCounter>> = std::sync::LazyLock::new(|| {
        Arc::new(Cl100k {
            bpe: tiktoken_rs::cl100k_base_singleton(),
        })
    });
    T.clone()
}
