//! Local CPU provider for `nomic-ai/nomic-embed-text-v1.5` (spec 12.1): pinned files, task prefixes, mean pooling.

use super::provider::*;
use crate::config::{EmbeddingConfig, Paths};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::nomic_bert::{self, NomicBertModel};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokenizers::{Tokenizer, TruncationDirection, TruncationParams, TruncationStrategy};
use tokio::sync::Notify;
use tokio::time::Instant;

/// Operator-facing `embedding.model` value for the local provider.
pub const LOCAL_MODEL_NAME: &str = "nomic-embed-text-v1.5";
/// The only dimension count the local provider serves.
pub const LOCAL_DIMENSIONS: usize = 768;
/// Raise when prefixes, pooling, truncation or dtype change: it is part of the profile identity.
const RECIPE: &str = "local/recipe-1";
/// Positions the model was trained on; the candle implementation has no RoPE scaling beyond them.
const MAX_MODEL_TOKENS: usize = 2048;
const MODEL_IDLE: Duration = Duration::from_secs(60);

pub struct ModelFile {
    pub name: &'static str,
    pub size: u64,
    pub sha256: &'static str,
}

pub struct Manifest {
    pub name: &'static str,
    pub revision: &'static str,
    pub files: [ModelFile; 3],
}

pub const NOMIC_V15: Manifest = Manifest {
    name: "nomic-ai/nomic-embed-text-v1.5",
    revision: "e9b6763023c676ca8431644204f50c2b100d9aab",
    files: [
        ModelFile {
            name: "model.safetensors",
            size: 546_938_168,
            sha256: "9e7d262b1fe5ea350782829496efa831901b77486bbde1cea54a4c822d010d5c",
        },
        ModelFile {
            name: "tokenizer.json",
            size: 711_396,
            sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
        },
        ModelFile {
            name: "config.json",
            size: 2538,
            sha256: "9ab00bd92cee80a569f708140b7b6c1661a65891ff3765b1519e181ba2f2c92b",
        },
    ],
};

/// `<root>/models/nomic-embed-text-v1.5/<revision>`.
pub fn model_dir(paths: &Paths) -> PathBuf {
    paths
        .models_dir
        .join(LOCAL_MODEL_NAME)
        .join(NOMIC_V15.revision)
}

pub fn profile_spec(cfg: &EmbeddingConfig) -> ProfileSpec {
    ProfileSpec {
        provider: "local".into(),
        endpoint_identity: RECIPE.into(),
        model: format!(
            "{}@{}+sha256:{}",
            NOMIC_V15.name,
            NOMIC_V15.revision,
            &NOMIC_V15.files[0].sha256[..16]
        ),
        dimensions: LOCAL_DIMENSIONS,
        rendering_version: RENDERING_VERSION,
        generation: cfg.profile_generation,
    }
}

/// Cheap presence check: every pinned file is a regular file of the exact size.
pub fn installed(dir: &Path) -> Result<(), &'static str> {
    for f in &NOMIC_V15.files {
        match std::fs::metadata(dir.join(f.name)) {
            Ok(m) if m.is_file() && m.len() == f.size => {}
            Ok(m) if m.is_file() => return Err("model file has an unexpected size"),
            _ => return Err("model file is missing"),
        }
    }
    Ok(())
}

/// Streams a full SHA-256 check of every pinned file. The error names the file and the problem, never content.
pub fn verify(dir: &Path) -> anyhow::Result<()> {
    for f in &NOMIC_V15.files {
        let mut file = std::fs::File::open(dir.join(f.name))
            .map_err(|_| anyhow::anyhow!("{} cannot be opened", f.name))?;
        let mut v = Verifier::new(f);
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = file
                .read(&mut buf)
                .map_err(|_| anyhow::anyhow!("{} cannot be read", f.name))?;
            if n == 0 {
                break;
            }
            v.update(&buf[..n])?;
        }
        v.finish()?;
    }
    Ok(())
}

/// Incremental size and SHA-256 check of one pinned file; errors name the file and the problem only.
pub struct Verifier<'a> {
    file: &'a ModelFile,
    hasher: Sha256,
    total: u64,
}

impl<'a> Verifier<'a> {
    pub fn new(file: &'a ModelFile) -> Self {
        Self {
            file,
            hasher: Sha256::new(),
            total: 0,
        }
    }

    /// Fails as soon as the stream exceeds the pinned size.
    pub fn update(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.total = self.total.saturating_add(data.len() as u64);
        anyhow::ensure!(
            self.total <= self.file.size,
            "{} size mismatch: larger than expected",
            self.file.name
        );
        self.hasher.update(data);
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<()> {
        let f = self.file;
        anyhow::ensure!(
            self.total == f.size,
            "{} size mismatch: expected {} bytes, got {}",
            f.name,
            f.size,
            self.total
        );
        anyhow::ensure!(
            hex(&self.hasher.finalize()) == f.sha256,
            "{} checksum mismatch",
            f.name
        );
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

struct Loaded {
    tokenizer: Tokenizer,
    model: NomicBertModel,
    device: Device,
}

#[derive(Default)]
struct ModelState {
    loaded: Option<Loaded>,
    last_used: Option<Instant>,
}

/// Loads the model lazily on the first embed call; inference is serialized by one lock.
pub struct LocalProvider {
    spec: ProfileSpec,
    dir: PathBuf,
    loaded: Arc<Mutex<ModelState>>,
    activity: Arc<Notify>,
    reaper: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl LocalProvider {
    pub fn new(dir: PathBuf, cfg: &EmbeddingConfig) -> Self {
        Self {
            spec: profile_spec(cfg),
            dir,
            loaded: Arc::default(),
            activity: Arc::default(),
            reaper: Mutex::new(None),
        }
    }
}

impl Drop for LocalProvider {
    fn drop(&mut self) {
        if let Some(reaper) = self.reaper.get_mut().take() {
            reaper.abort();
        }
    }
}

async fn reap_idle(
    loaded: std::sync::Weak<Mutex<ModelState>>,
    activity: Arc<Notify>,
    idle: Duration,
) {
    loop {
        activity.notified().await;
        loop {
            let Some(loaded) = loaded.upgrade() else {
                return;
            };
            let deadline = match loaded.try_lock() {
                Some(mut state) => match state.last_used {
                    Some(last_used) if last_used + idle <= Instant::now() => {
                        state.loaded = None;
                        state.last_used = None;
                        break;
                    }
                    Some(last_used) => last_used + idle,
                    None => break,
                },
                None => break,
            };
            drop(loaded);
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => {},
                () = activity.notified() => {},
            }
        }
    }
}

fn prefix(purpose: Purpose) -> &'static str {
    match purpose {
        Purpose::Query => "search_query: ",
        Purpose::Ingest => "search_document: ",
    }
}

fn load_tokenizer(path: &Path, max_length: usize) -> anyhow::Result<Tokenizer> {
    let mut tokenizer = Tokenizer::from_file(path)
        .map_err(|_| anyhow::anyhow!("tokenizer file cannot be loaded"))?;
    tokenizer
        .with_truncation(Some(TruncationParams {
            max_length,
            strategy: TruncationStrategy::LongestFirst,
            direction: TruncationDirection::Right,
            stride: 0,
        }))
        .map_err(|_| anyhow::anyhow!("tokenizer truncation cannot be configured"))?;
    tokenizer.with_padding(None);
    Ok(tokenizer)
}

fn load(dir: &Path) -> anyhow::Result<Loaded> {
    let tokenizer = load_tokenizer(&dir.join("tokenizer.json"), MAX_MODEL_TOKENS)?;
    let cfg: nomic_bert::Config = serde_json::from_slice(&std::fs::read(dir.join("config.json"))?)?;
    let device = Device::Cpu;
    let weights = std::fs::read(dir.join("model.safetensors"))?;
    let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &device)?;
    let model = NomicBertModel::load(vb, &cfg)?;
    Ok(Loaded {
        tokenizer,
        model,
        device,
    })
}

fn embed_one(l: &Loaded, purpose: Purpose, text: &str) -> candle_core::Result<(Vec<f32>, u64)> {
    let encoding = l
        .tokenizer
        .encode(format!("{}{text}", prefix(purpose)), true)
        .map_err(|e| candle_core::Error::Msg(e.to_string()))?;
    let ids = encoding.get_ids();
    let len = ids.len();
    let input_ids = Tensor::new(ids, &l.device)?.unsqueeze(0)?;
    let type_ids = input_ids.zeros_like()?;
    let mask = input_ids.ones_like()?;
    let hidden = l.model.forward(&input_ids, Some(&type_ids), Some(&mask))?;
    let pooled = nomic_bert::mean_pooling(&hidden, &mask)?;
    Ok((pooled.squeeze(0)?.to_vec1::<f32>()?, len as u64))
}

fn infer(
    loaded: &Mutex<ModelState>,
    activity: &Notify,
    dir: &Path,
    purpose: Purpose,
    inputs: &[String],
) -> Result<EmbedResponse, EmbedError> {
    let fail = |m| EmbedError::new(EmbedErrorKind::UnsupportedModel, m);
    let mut guard = loaded.lock();
    if guard.loaded.is_none() {
        guard.loaded = Some(load(dir).map_err(|_| fail("local embedding model cannot be loaded"))?);
    }
    let l = guard.loaded.as_ref().expect("loaded above");

    let mut items = Vec::with_capacity(inputs.len());
    let mut tokens = 0u64;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for (i, text) in inputs.iter().enumerate() {
            let (v, n) = embed_one(l, purpose, text)
                .map_err(|_| fail("local embedding inference failed"))?;
            tokens += n;
            items.push((i, v));
        }
        Ok(EmbedResponse {
            vectors: validate_items(items, inputs.len(), LOCAL_DIMENSIONS)?,
            usage_tokens: Some(tokens),
        })
    }));
    guard.last_used = Some(Instant::now());
    drop(guard);
    activity.notify_one();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

impl EmbeddingProvider for LocalProvider {
    fn profile(&self) -> &ProfileSpec {
        &self.spec
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits::LOCAL
    }
    fn is_hosted(&self) -> bool {
        false
    }
    fn metered(&self) -> bool {
        false
    }
    fn embed<'a>(
        &'a self,
        purpose: Purpose,
        inputs: &'a [String],
    ) -> BoxFuture<'a, Result<EmbedResponse, EmbedError>> {
        Box::pin(async move {
            check_request(&self.limits(), inputs)?;
            {
                let mut reaper = self.reaper.lock();
                if reaper.is_none() {
                    *reaper = Some(tokio::spawn(reap_idle(
                        Arc::downgrade(&self.loaded),
                        self.activity.clone(),
                        MODEL_IDLE,
                    )));
                }
            }
            let loaded = self.loaded.clone();
            let activity = self.activity.clone();
            let dir = self.dir.clone();
            let inputs = inputs.to_vec();
            tokio::task::spawn_blocking(move || infer(&loaded, &activity, &dir, purpose, &inputs))
                .await
                .map_err(|_| {
                    EmbedError::new(
                        EmbedErrorKind::UnsupportedModel,
                        "local embedding worker panicked",
                    )
                })?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_timer_refreshes_after_activity_and_waits_for_inference() {
        let state = Arc::new(Mutex::new(ModelState::default()));
        let activity = Arc::new(Notify::new());
        let idle = Duration::from_millis(100);
        let reaper = tokio::spawn(reap_idle(Arc::downgrade(&state), activity.clone(), idle));

        state.lock().last_used = Some(Instant::now());
        activity.notify_one();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let refreshed = Instant::now();
        state.lock().last_used = Some(refreshed);
        activity.notify_one();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(state.lock().last_used, Some(refreshed));

        {
            let guard = state.lock();
            std::thread::sleep(Duration::from_millis(60));
            assert_eq!(guard.last_used, Some(refreshed));
        }
        state.lock().last_used = Some(Instant::now());
        activity.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.lock().last_used.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        reaper.abort();
    }

    #[tokio::test]
    #[ignore = "requires the installed local model"]
    async fn installed_model_unloads_and_reloads() {
        let home = std::env::var_os("SPOOL_LOCAL_MODEL_HOME").unwrap();
        let dir = model_dir(&Paths::from_root(home.into()).unwrap());
        let started = std::time::Instant::now();
        let model = load(&dir).unwrap();
        eprintln!("initial load: {:?}", started.elapsed());
        let state = Arc::new(Mutex::new(ModelState {
            loaded: Some(model),
            last_used: Some(Instant::now()),
        }));
        let activity = Arc::new(Notify::new());
        let reaper = tokio::spawn(reap_idle(
            Arc::downgrade(&state),
            activity.clone(),
            Duration::from_millis(50),
        ));
        activity.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.lock().loaded.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let _model = load(&dir).unwrap();
        eprintln!("reload: {:?}", started.elapsed());
        reaper.abort();
    }

    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/embeddings/tiny-wordpiece-tokenizer.json"
    );

    fn tokens(t: &Tokenizer, text: &str) -> Vec<String> {
        t.encode(text, true).unwrap().get_tokens().to_vec()
    }

    #[test]
    fn prefix_follows_purpose() {
        assert_eq!(prefix(Purpose::Query), "search_query: ");
        assert_eq!(prefix(Purpose::Ingest), "search_document: ");
        let t = load_tokenizer(Path::new(FIXTURE), 64).unwrap();
        let q = tokens(&t, &format!("{}alpha", prefix(Purpose::Query)));
        let d = tokens(&t, &format!("{}alpha", prefix(Purpose::Ingest)));
        assert_eq!(q[1..3], ["search", "_"]);
        assert_ne!(q, d);
        assert!(d.contains(&"document".to_string()));
    }

    #[test]
    fn long_input_truncates_and_keeps_special_tokens() {
        let t = load_tokenizer(Path::new(FIXTURE), 8).unwrap();
        let long = "alpha ".repeat(50);
        let toks = tokens(&t, &long);
        assert_eq!(toks.len(), 8);
        assert_eq!(toks.first().unwrap(), "[CLS]");
        assert_eq!(toks.last().unwrap(), "[SEP]");
    }

    #[test]
    fn profile_identity_is_pinned_and_generation_sensitive() {
        let mut cfg = EmbeddingConfig::default();
        let a = profile_spec(&cfg);
        assert_eq!(a.provider, "local");
        assert!(a.model.contains(NOMIC_V15.revision));
        cfg.profile_generation += 1;
        assert_ne!(a.id(), profile_spec(&cfg).id());
    }

    #[test]
    fn verifier_accepts_match_and_rejects_size_and_digest() {
        let good = ModelFile {
            name: "f",
            size: 3,
            sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        };
        let run = |f: &ModelFile, data: &[u8]| {
            let mut v = Verifier::new(f);
            v.update(data)?;
            v.finish()
        };
        assert!(run(&good, b"abc").is_ok());
        assert!(run(&ModelFile { size: 2, ..good }, b"abc").is_err());
        assert!(run(&good, b"ab").is_err());
        let wrong = ModelFile {
            sha256: "00",
            ..good
        };
        assert!(
            run(&wrong, b"abc")
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
    }
}
