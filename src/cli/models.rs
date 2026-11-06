//! `spool models install`: fetch or copy the pinned local embedding model files, verify every size and SHA-256,
//! then publish them atomically. The daemon never downloads weights.

use crate::config::Paths;
use crate::embeddings::local::{self, ModelFile, NOMIC_V15, Verifier};
use anyhow::{Context, Result, bail};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::time::Duration;

const STAGING: &str = ".staging";

pub async fn install(paths: &Paths, from_dir: Option<&Path>) -> Result<()> {
    let target = local::model_dir(paths);
    if target.exists() {
        local::verify(&target)
            .context("an installation exists but fails verification; remove it and retry")?;
        println!("already installed: {}", target.display());
        return Ok(());
    }

    let parent = target.parent().context("model directory has no parent")?;
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .context("cannot create the models directory")?;
    let staging = parent.join(STAGING);
    if staging.exists() {
        std::fs::remove_dir_all(&staging).context("cannot remove a leftover staging directory")?;
    }
    DirBuilder::new()
        .mode(0o700)
        .create(&staging)
        .context("cannot create the staging directory")?;

    let staged = match from_dir {
        Some(dir) => stage_from_dir(&NOMIC_V15.files, dir, &staging),
        None => stage_from_https(&NOMIC_V15.files, &staging).await,
    };
    if let Err(e) = staged.and_then(|()| {
        std::fs::rename(&staging, &target).context("cannot publish the staged model")
    }) {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(e);
    }

    println!("installed: {}", target.display());
    Ok(())
}

fn create_staged(staging: &Path, f: &ModelFile) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(staging.join(f.name))
        .with_context(|| format!("{} cannot be created", f.name))
}

fn finish_file(out: File, v: Verifier<'_>, f: &ModelFile) -> Result<()> {
    v.finish()?;
    out.sync_all()
        .with_context(|| format!("{} cannot be flushed", f.name))?;
    println!("verified {} ({} bytes)", f.name, f.size);
    Ok(())
}

/// Copies each pinned file from `dir` into `staging`, verifying while writing. Nothing is cleaned up on error.
fn stage_from_dir(files: &[ModelFile], dir: &Path, staging: &Path) -> Result<()> {
    for f in files {
        let mut src =
            File::open(dir.join(f.name)).with_context(|| format!("{} cannot be opened", f.name))?;
        let mut out = create_staged(staging, f)?;
        let mut v = Verifier::new(f);
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = src
                .read(&mut buf)
                .with_context(|| format!("{} cannot be read", f.name))?;
            if n == 0 {
                break;
            }
            v.update(&buf[..n])?;
            out.write_all(&buf[..n])
                .with_context(|| format!("{} cannot be written", f.name))?;
        }
        finish_file(out, v, f)?;
    }
    Ok(())
}

/// Downloads each pinned file at the pinned revision. Errors never carry a URL: redirect targets are signed.
async fn stage_from_https(files: &[ModelFile], staging: &Path) -> Result<()> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .no_proxy()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .user_agent(concat!("spool/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("cannot build the HTTPS client")?;
    for f in files {
        let url = format!(
            "https://huggingface.co/{}/resolve/{}/{}",
            NOMIC_V15.name, NOMIC_V15.revision, f.name
        );
        let mut resp = client
            .get(url)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("{} download failed: network error", f.name))?;
        if !resp.status().is_success() {
            bail!(
                "{} download failed: HTTP {}",
                f.name,
                resp.status().as_u16()
            );
        }
        let mut out = create_staged(staging, f)?;
        let mut v = Verifier::new(f);
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("{} download failed: network error", f.name))?
        {
            v.update(&chunk)?;
            out.write_all(&chunk)
                .with_context(|| format!("{} cannot be written", f.name))?;
        }
        finish_file(out, v, f)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABC_SHA: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    fn stage(content: &[u8], file: ModelFile) -> Result<()> {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join(file.name), content).unwrap();
        stage_from_dir(&[file], src.path(), dst.path())
    }

    fn manifest(size: u64, sha256: &'static str) -> ModelFile {
        ModelFile {
            name: "m.bin",
            size,
            sha256,
        }
    }

    #[test]
    fn matching_data_is_staged_with_owner_only_mode() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("m.bin"), b"abc").unwrap();
        stage_from_dir(&[manifest(3, ABC_SHA)], src.path(), dst.path()).unwrap();
        let staged = dst.path().join("m.bin");
        assert_eq!(std::fs::read(&staged).unwrap(), b"abc");
        assert_eq!(crate::config::mode_of(&staged).unwrap(), 0o600);
    }

    #[test]
    fn oversized_and_corrupt_data_are_rejected() {
        let e = stage(b"abcd", manifest(3, ABC_SHA))
            .unwrap_err()
            .to_string();
        assert!(e.contains("size mismatch"), "{e}");
        let e = stage(b"abd", manifest(3, ABC_SHA)).unwrap_err().to_string();
        assert!(e.contains("checksum mismatch"), "{e}");
        let e = stage(b"ab", manifest(3, ABC_SHA)).unwrap_err().to_string();
        assert!(e.contains("size mismatch"), "{e}");
    }
}
