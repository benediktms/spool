//! Internal identifiers. They are not secrets (handles are signed separately).

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Process-unique, time-ordered-ish random id: `prefix_` + 24 hex chars.
pub fn new_id(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut h = blake3::Hasher::new();
    h.update(&nanos.to_le_bytes());
    h.update(&std::process::id().to_le_bytes());
    h.update(&COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let mut rnd = [0u8; 16];
    let _ = getrandom::fill(&mut rnd);
    h.update(&rnd);
    format!("{prefix}_{}", &h.finalize().to_hex()[..24])
}

/// Stable internal session id for `(source, native id, discriminator)`; independent of file name.
pub fn session_id(source_id: &str, native_id: &str, discriminator: &str) -> String {
    let mut h = blake3::Hasher::new();
    for part in [source_id, native_id, discriminator] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part.as_bytes());
    }
    format!("ss_{}", &h.finalize().to_hex()[..24])
}

pub fn hex(bytes: &[u8]) -> String {
    const T: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(T[(b >> 4) as usize] as char);
        s.push(T[(b & 15) as usize] as char);
    }
    s
}
