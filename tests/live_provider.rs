//! Opt-in LIVE provider contract test (spec 19.3/22): one request with a fixed harmless string, so it
//! spends a few tokens and sends no conversation data. Skipped unless `SPOOL_LIVE_PROVIDER_KEY` is set;
//! run with `cargo test --test live_provider -- --ignored`. Never part of default CI.

use spool::config::EmbeddingConfig;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[tokio::test]
#[ignore = "live provider: needs SPOOL_LIVE_PROVIDER_KEY and spends a few tokens"]
async fn live_openai_probe_returns_the_configured_dimensions() {
    let Ok(key) = std::env::var("SPOOL_LIVE_PROVIDER_KEY") else {
        eprintln!("SPOOL_LIVE_PROVIDER_KEY not set; skipping");
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let f = tmp.path().join("provider.key");
    std::fs::write(&f, key).unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
    let cfg = EmbeddingConfig {
        hosted_processing_approved: true,
        ..Default::default()
    };
    let out = spool::embeddings::probe_provider(&cfg, &f)
        .await
        .expect("live probe");
    assert_eq!(out.dimensions, cfg.dimensions);
}
