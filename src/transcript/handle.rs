//! Versioned, signed, opaque handles (spec 14.2.8). The signature protects integrity only; it
//! does not authenticate a client. Key file: private (0600 on Unix).

use crate::ids::hex;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use std::io::Write;
use std::path::Path;

pub const HANDLE_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleKind {
    Match,
    Message,
    Item,
    Branch,
    Cursor,
}

impl HandleKind {
    fn tag(self) -> &'static str {
        match self {
            Self::Match => "m",
            Self::Message => "g",
            Self::Item => "i",
            Self::Branch => "b",
            Self::Cursor => "c",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HandleError {
    #[error("handle is malformed")]
    Malformed,
    #[error("handle signature does not verify")]
    BadSignature,
    #[error("handle version is not supported")]
    UnsupportedVersion,
    #[error("handle was signed with a revoked key (key rotated)")]
    KeyRotated,
    #[error("handle has the wrong kind")]
    WrongKind,
}

#[derive(Clone)]
pub struct HandleKey {
    key: [u8; 32],
    epoch: u32,
}

impl std::fmt::Debug for HandleKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HandleKey(epoch {}, redacted)", self.epoch)
    }
}

fn random32() -> std::io::Result<[u8; 32]> {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b).map_err(std::io::Error::other)?;
    Ok(b)
}

impl HandleKey {
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// Create the key file (explicit initialization only). Fails if it exists.
    pub fn create(path: &Path) -> std::io::Result<Self> {
        Self::write(path, 1, true)
    }

    /// Replace the key with a fresh one and increment the epoch (revokes all issued handles).
    pub fn rotate(path: &Path) -> std::io::Result<Self> {
        let old = Self::load(path)?;
        Self::write(path, old.epoch + 1, false)
    }

    fn write(path: &Path, epoch: u32, create_new: bool) -> std::io::Result<Self> {
        let key = random32()?;
        if create_new {
            let mut file = crate::perms::create_private_file(path)?;
            writeln!(file, "v1 {epoch} {}", hex(&key))?;
            file.sync_all()?;
            return Ok(Self { key, epoch });
        }

        let tmp = path.with_extension("tmp");
        let _ = std::fs::remove_file(&tmp);
        {
            let mut file = crate::perms::create_private_file(&tmp)?;
            writeln!(file, "v1 {epoch} {}", hex(&key))?;
            file.sync_all()?;
        }
        crate::perms::replace_private(&tmp, path)?;
        Ok(Self { key, epoch })
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        crate::perms::check_private(path, 0o600)?;
        let text = std::fs::read_to_string(path)?;
        let bad = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "signing key file is malformed",
            )
        };
        let mut it = text.split_whitespace();
        if it.next() != Some("v1") {
            return Err(bad());
        }
        let epoch: u32 = it.next().and_then(|e| e.parse().ok()).ok_or_else(bad)?;
        let h = it.next().ok_or_else(bad)?;
        if h.len() != 64 {
            return Err(bad());
        }
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = u8::from_str_radix(&h[2 * i..2 * i + 2], 16).map_err(|_| bad())?;
        }
        Ok(Self { key, epoch })
    }

    #[cfg(test)]
    pub fn for_test(epoch: u32) -> Self {
        Self {
            key: [7u8; 32],
            epoch,
        }
    }

    fn mac(&self, kind: HandleKind, epoch: u32, payload: &[u8]) -> [u8; 16] {
        let mut h = blake3::Hasher::new_keyed(&self.key);
        h.update(&[HANDLE_VERSION]);
        h.update(kind.tag().as_bytes());
        h.update(&epoch.to_le_bytes());
        h.update(payload);
        let mut out = [0u8; 16];
        out.copy_from_slice(&h.finalize().as_bytes()[..16]);
        out
    }

    /// `v1.<kind>.<epoch>.<b64 payload>.<b64 mac>`
    pub fn sign(&self, kind: HandleKind, payload: &[u8]) -> String {
        let mac = self.mac(kind, self.epoch, payload);
        format!(
            "v{HANDLE_VERSION}.{}.{}.{}.{}",
            kind.tag(),
            self.epoch,
            B64.encode(payload),
            B64.encode(mac)
        )
    }

    pub fn verify(&self, kind: HandleKind, token: &str) -> Result<Vec<u8>, HandleError> {
        let mut p = token.split('.');
        let (v, k, e, pl, m) = (p.next(), p.next(), p.next(), p.next(), p.next());
        if p.next().is_some() {
            return Err(HandleError::Malformed);
        }
        let (Some(v), Some(k), Some(e), Some(pl), Some(m)) = (v, k, e, pl, m) else {
            return Err(HandleError::Malformed);
        };
        if v != format!("v{HANDLE_VERSION}") {
            return if v.starts_with('v') && v[1..].parse::<u32>().is_ok() {
                Err(HandleError::UnsupportedVersion)
            } else {
                Err(HandleError::Malformed)
            };
        }
        if k != kind.tag() {
            return Err(HandleError::WrongKind);
        }
        let epoch: u32 = e.parse().map_err(|_| HandleError::Malformed)?;
        // The epoch is cleartext: a different epoch means the issuing key was rotated away.
        if epoch != self.epoch {
            return Err(HandleError::KeyRotated);
        }
        let payload = B64.decode(pl).map_err(|_| HandleError::Malformed)?;
        let mac = B64.decode(m).map_err(|_| HandleError::Malformed)?;
        let expect = self.mac(kind, epoch, &payload);
        // constant-time compare
        let mut diff = (mac.len() != expect.len()) as u8;
        for (a, b) in mac.iter().zip(expect.iter()) {
            diff |= a ^ b;
        }
        if diff != 0 {
            return Err(HandleError::BadSignature);
        }
        Ok(payload)
    }
}

/// Shared, reloadable signing key. `spool handles rotate-key` rewrites the key file; the daemon
/// calls `reload()` so already-issued handles fail with `KeyRotated` without a restart.
#[derive(Clone)]
pub struct KeyStore {
    path: std::path::PathBuf,
    inner: std::sync::Arc<std::sync::RwLock<HandleKey>>,
}

impl KeyStore {
    pub fn load(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            inner: std::sync::Arc::new(std::sync::RwLock::new(HandleKey::load(path)?)),
        })
    }
    pub fn current(&self) -> HandleKey {
        self.inner.read().expect("key lock").clone()
    }
    pub fn reload(&self) -> std::io::Result<()> {
        *self.inner.write().expect("key lock") = HandleKey::load(&self.path)?;
        Ok(())
    }
    /// Rotate the key file and switch to the new key atomically.
    pub fn rotate(&self) -> std::io::Result<u32> {
        let k = HandleKey::rotate(&self.path)?;
        let e = k.epoch();
        *self.inner.write().expect("key lock") = k;
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper() {
        let k = HandleKey::for_test(1);
        let t = k.sign(HandleKind::Match, b"hello");
        assert_eq!(k.verify(HandleKind::Match, &t).unwrap(), b"hello");
        assert_eq!(k.verify(HandleKind::Item, &t), Err(HandleError::WrongKind));
        let mut parts: Vec<String> = t.split('.').map(String::from).collect();
        parts[3] = B64.encode(b"hellp");
        assert_eq!(
            k.verify(HandleKind::Match, &parts.join(".")),
            Err(HandleError::BadSignature)
        );
        assert_eq!(
            k.verify(HandleKind::Match, "garbage"),
            Err(HandleError::Malformed)
        );
        let rotated = HandleKey {
            key: [8u8; 32],
            epoch: 2,
        };
        assert_eq!(
            rotated.verify(HandleKind::Match, &t),
            Err(HandleError::KeyRotated)
        );
    }

    #[test]
    fn rotation_with_same_key_material_epoch_is_explicit() {
        let a = HandleKey {
            key: [7u8; 32],
            epoch: 1,
        };
        let b = HandleKey {
            key: [7u8; 32],
            epoch: 2,
        };
        let t = a.sign(HandleKind::Cursor, b"x");
        assert_eq!(
            b.verify(HandleKind::Cursor, &t),
            Err(HandleError::KeyRotated)
        );
    }
}
