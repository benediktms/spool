//! cl100k_base counting works offline (data is embedded in the crate) and prefix_within honours its bound.

use spool::embeddings::tokenizer;

#[test]
fn counts_and_prefixes() {
    let t = tokenizer();
    assert_eq!(t.identity(), "tiktoken-rs-0.12.1/cl100k_base");
    assert_eq!(t.count("hello world"), 2);
    assert_eq!(t.count(""), 0);
    assert!(
        t.count("<|endoftext|>") > 1,
        "special-token text is counted as ordinary text"
    );
    let text = "héllo wörld — ünïcode 日本語 text ".repeat(40);
    for max in [0usize, 1, 7, 50, 10_000] {
        let end = t.prefix_within(&text, max);
        assert!(text.is_char_boundary(end));
        assert!(t.count(&text[..end]) <= max, "max {max}");
        if end < text.len() {
            assert!(max == 0 || end > 0);
        }
    }
    assert_eq!(t.prefix_within(&text, 10_000), text.len());
}
