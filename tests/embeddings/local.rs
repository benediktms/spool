//! Local provider wiring: config validation, profile identity, and the model-missing state.

use super::*;
use spool::clock::{ManualClock, SharedClock};
use spool::config::{Config, Paths};
use spool::embeddings::local::{self, LOCAL_MODEL_NAME};
use spool::embeddings::{
    EmbedErrorKind as K, EmbeddingProvider, EmbeddingService, EmbeddingState, LocalProvider,
    Purpose,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

fn local_config() -> Config {
    let mut cfg = Config::default();
    cfg.embedding.provider = "local".into();
    cfg.embedding.model = LOCAL_MODEL_NAME.into();
    cfg.embedding.dimensions = 768;
    cfg
}

#[test]
fn config_validation_for_local() {
    if let Err(e) = local_config().validate() {
        assert!(!e.to_string().contains("embedding"), "{e}");
    }

    let mut wrong_model = local_config();
    wrong_model.embedding.model = "text-embedding-3-small".into();
    let e = wrong_model.validate().unwrap_err().to_string();
    assert!(e.contains("embedding.model"), "{e}");

    let mut wrong_dims = local_config();
    wrong_dims.embedding.dimensions = 512;
    let e = wrong_dims.validate().unwrap_err().to_string();
    assert!(e.contains("embedding.dimensions"), "{e}");

    let mut unknown = local_config();
    unknown.embedding.provider = "other".into();
    assert!(unknown.validate().is_err());
}

#[test]
fn profile_identity_is_pinned_and_distinct_from_hosted() {
    let cfg = local_config();
    let spec = local::profile_spec(&cfg.embedding);
    assert_eq!(spec.provider, "local");
    assert!(spec.model.contains(local::NOMIC_V15.revision));
    assert_eq!(spec.dimensions, 768);

    let hosted = spool::embeddings::provider::ProfileSpec {
        provider: "openai".into(),
        endpoint_identity: "api.openai.com".into(),
        model: "text-embedding-3-small".into(),
        ..spec.clone()
    };
    assert_ne!(spec.id(), hosted.id());

    let mut next = local_config();
    next.embedding.profile_generation += 1;
    assert_ne!(spec.id(), local::profile_spec(&next.embedding).id());
    assert_eq!(spec.id(), local::profile_spec(&cfg.embedding).id());
}

#[tokio::test]
async fn missing_model_closes_semantic_search_without_failing_ingestion() {
    let home = tempfile::tempdir().unwrap();
    let paths = Paths::from_root(home.path().to_path_buf()).unwrap();
    let (_dir, index) = open_index().await;
    let clock: SharedClock = Arc::new(ManualClock::new(NOON));
    let svc = EmbeddingService::from_config(index, &local_config(), &paths, clock).unwrap();

    let status = svc.status();
    assert_eq!(status.state, EmbeddingState::ModelMissing);
    assert!(status.detail.unwrap().contains("spool models install"));

    let docs = vec![(digest_of(1), "some document".to_string())];
    let e = svc
        .embed_documents(docs, &AtomicBool::new(false))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::ModelMissing);

    let e = svc
        .embed_query("question", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::ModelMissing);
    assert_eq!(e.to_app_error().code, "QUERY_EMBEDDING_FAILED");
    assert!(
        e.to_app_error()
            .recovery
            .unwrap()
            .contains("spool models install")
    );
}

#[tokio::test]
async fn corrupt_install_is_reported_as_unsupported_model() {
    let dir = tempfile::tempdir().unwrap();
    let provider = LocalProvider::new(dir.path().to_path_buf(), &local_config().embedding);
    let e = provider
        .embed(Purpose::Query, &["question".to_string()])
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::UnsupportedModel);
    assert!(!e.message.contains("question"));
}

/// Needs the real model: `SPOOL_LOCAL_MODEL_HOME=<SPOOL_HOME with models installed>`.
#[tokio::test]
#[ignore = "needs the installed 547 MB model; set SPOOL_LOCAL_MODEL_HOME"]
async fn local_model_embeds_with_task_prefixes() {
    let home = std::env::var("SPOOL_LOCAL_MODEL_HOME").expect("SPOOL_LOCAL_MODEL_HOME");
    let paths = Paths::from_root(home.into()).unwrap();
    let provider = LocalProvider::new(local::model_dir(&paths), &local_config().embedding);

    let docs = [
        "Rust uses ownership and borrowing to guarantee memory safety without a garbage collector.",
        "Sourdough bread needs a long fermentation and a hot oven for a crisp crust.",
    ];
    let mut vecs = Vec::new();
    for d in docs {
        let r = provider
            .embed(Purpose::Ingest, &[d.to_string()])
            .await
            .unwrap();
        vecs.push(r.vectors.into_iter().next().unwrap());
    }
    let q = provider
        .embed(
            Purpose::Query,
            &["how does Rust manage memory safely?".to_string()],
        )
        .await
        .unwrap()
        .vectors
        .remove(0);

    for v in vecs.iter().chain([&q]) {
        assert_eq!(v.len(), 768);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-4, "norm {norm}");
    }
    let related = spool::embeddings::vectors::dot(&q, &vecs[0]);
    let unrelated = spool::embeddings::vectors::dot(&q, &vecs[1]);
    assert!(
        related > unrelated,
        "related {related} unrelated {unrelated}"
    );
}
