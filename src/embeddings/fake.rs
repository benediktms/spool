//! Deterministic fake provider for tests (spec 19.3/19.6): concept-table vectors plus fault injection.
//! Every fault goes through the same `validate_items` as the real client, so classification is identical.

use super::provider::*;
use super::tokenizer::tokenizer;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum Fault {
    WrongDimensions,
    /// Add (positive) or drop (negative) items from the response.
    WrongCount(i32),
    /// Duplicate the first item's index onto the last item.
    DuplicateIndex,
    Nan,
    ZeroNorm,
    /// Items returned in reverse order with correct `index` labels: a valid response.
    Reorder,
    Auth,
    RateLimited(Duration),
    Server,
    Network,
    UnsupportedModel,
    /// Never answer (the caller's deadline must save it).
    Hang,
}

/// Paraphrase groups: words in one group share a basis dimension, so texts with no shared words but a
/// shared concept are near each other.
pub const DEFAULT_CONCEPTS: &[&[&str]] = &[
    &["car", "automobile", "vehicle", "sedan", "driving"],
    &["database", "sqlite", "storage", "persistence", "sql"],
    &["error", "failure", "crash", "bug", "exception"],
    &["deploy", "release", "ship", "rollout", "publish"],
    &["test", "verify", "check", "validate", "assert"],
    &["network", "socket", "connection", "http", "wire"],
    &["auth", "login", "credential", "password", "token"],
    &["recipe", "cooking", "kitchen", "bake", "dinner"],
    &["music", "song", "melody", "tune", "guitar"],
    &["money", "budget", "cost", "price", "billing"],
    &["cache", "memoize", "lru", "remember", "memo"],
    &["search", "find", "lookup", "query", "retrieve"],
];

#[derive(Default)]
struct Stats {
    calls: usize,
    by_purpose: [usize; 2],
    batches: Vec<usize>,
    seen: Vec<String>,
    in_flight: [usize; 2],
    max_in_flight: [usize; 2],
}

#[derive(Default)]
struct Faults {
    sticky: Option<Fault>,
    queue: VecDeque<Fault>,
}

pub struct FakeProvider {
    spec: ProfileSpec,
    limits: ProviderLimits,
    hosted: bool,
    n_concepts: usize,
    words: HashMap<String, usize>,
    faults: Mutex<Faults>,
    delay: Mutex<Duration>,
    stats: Mutex<Stats>,
}

fn idx(p: Purpose) -> usize {
    p as usize
}

impl FakeProvider {
    /// `dimensions` must exceed the concept count so concepts get their own basis dimensions.
    pub fn new(dimensions: usize) -> Self {
        Self::with_concepts(dimensions, DEFAULT_CONCEPTS)
    }

    pub fn with_concepts(dimensions: usize, groups: &[&[&str]]) -> Self {
        assert!(
            dimensions > groups.len(),
            "fake provider needs more dimensions than concept groups"
        );
        let mut words = HashMap::new();
        for (i, g) in groups.iter().enumerate() {
            for w in *g {
                words.insert(w.to_ascii_lowercase(), i);
            }
        }
        Self {
            spec: ProfileSpec {
                provider: "fake".into(),
                endpoint_identity: "fake.local".into(),
                model: "fake-concepts".into(),
                dimensions,
                rendering_version: RENDERING_VERSION,
                generation: 1,
            },
            limits: ProviderLimits::OPENAI,
            hosted: false,
            n_concepts: groups.len(),
            words,
            faults: Mutex::default(),
            delay: Mutex::new(Duration::ZERO),
            stats: Mutex::default(),
        }
    }

    pub fn with_model(mut self, model: &str, generation: u32) -> Self {
        self.spec.model = model.into();
        self.spec.generation = generation;
        self
    }
    /// Treat the fake as hosted so consent gating applies to it.
    pub fn hosted(mut self, hosted: bool) -> Self {
        self.hosted = hosted;
        self
    }
    pub fn with_limits(mut self, limits: ProviderLimits) -> Self {
        self.limits = limits;
        self
    }

    /// One call fails/misbehaves with `f`, then behaviour returns to normal.
    pub fn fail_next(&self, f: Fault) {
        self.faults.lock().queue.push_back(f);
    }
    /// Every call misbehaves until cleared with `None`.
    pub fn set_fault(&self, f: Option<Fault>) {
        self.faults.lock().sticky = f;
    }
    /// Latency before each answer (tests of concurrency).
    pub fn set_delay(&self, d: Duration) {
        *self.delay.lock() = d;
    }

    pub fn calls(&self) -> usize {
        self.stats.lock().calls
    }
    pub fn calls_for(&self, p: Purpose) -> usize {
        self.stats.lock().by_purpose[idx(p)]
    }
    pub fn batch_sizes(&self) -> Vec<usize> {
        self.stats.lock().batches.clone()
    }
    /// Every input text ever received (synthetic test data only).
    pub fn seen_inputs(&self) -> Vec<String> {
        self.stats.lock().seen.clone()
    }
    pub fn max_in_flight(&self, p: Purpose) -> usize {
        self.stats.lock().max_in_flight[idx(p)]
    }

    /// The unit vector this provider produces for `text`.
    pub fn vector_for(&self, text: &str) -> Vec<f32> {
        let dims = self.spec.dimensions;
        let mut v = vec![0f32; dims];
        let spare = dims - self.n_concepts;
        let mut any = false;
        for w in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
        {
            any = true;
            let w = w.to_lowercase();
            let w = w
                .strip_suffix('s')
                .filter(|s| s.len() > 2 && !self.words.contains_key(&w))
                .unwrap_or(&w);
            match self.words.get(w) {
                Some(&c) => v[c] += 1.0,
                None => v[self.n_concepts + hash_mod(w, spare)] += 0.15,
            }
        }
        if !any {
            v[self.n_concepts + hash_mod(text, spare)] = 1.0;
        }
        super::vectors::normalize(&mut v).expect("fake vector is non-zero");
        v
    }

    fn next_fault(&self) -> Option<Fault> {
        let mut f = self.faults.lock();
        f.queue.pop_front().or_else(|| f.sticky.clone())
    }

    async fn run(&self, purpose: Purpose, inputs: &[String]) -> Result<EmbedResponse, EmbedError> {
        check_request(&self.limits, inputs)?;
        {
            let mut s = self.stats.lock();
            s.calls += 1;
            s.by_purpose[idx(purpose)] += 1;
            s.batches.push(inputs.len());
            s.seen.extend(inputs.iter().cloned());
            let i = idx(purpose);
            s.in_flight[i] += 1;
            s.max_in_flight[i] = s.max_in_flight[i].max(s.in_flight[i]);
        }
        struct Leave<'a>(&'a Mutex<Stats>, usize);
        impl Drop for Leave<'_> {
            fn drop(&mut self) {
                self.0.lock().in_flight[self.1] -= 1;
            }
        }
        let _leave = Leave(&self.stats, idx(purpose));
        let delay = *self.delay.lock();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let fault = self.next_fault();
        use EmbedErrorKind as K;
        match &fault {
            Some(Fault::Hang) => futures::future::pending::<()>().await,
            Some(Fault::Auth) => {
                return Err(EmbedError::new(K::Auth, "provider rejected the credential"));
            }
            Some(Fault::RateLimited(d)) => {
                return Err(EmbedError::new(K::RateLimited, "provider rate limit")
                    .retryable(true)
                    .retry_after(Some(*d)));
            }
            Some(Fault::Server) => {
                return Err(EmbedError::new(K::Server, "provider server error").retryable(true));
            }
            Some(Fault::Network) => {
                return Err(EmbedError::new(K::Network, "connection failed after send")
                    .retryable(true)
                    .ambiguous(true));
            }
            Some(Fault::UnsupportedModel) => {
                return Err(EmbedError::new(
                    K::UnsupportedModel,
                    "provider rejected the model or dimensions",
                ));
            }
            _ => {}
        }
        let mut items: Vec<(usize, Vec<f32>)> = inputs
            .iter()
            .enumerate()
            .map(|(i, t)| (i, self.vector_for(t)))
            .collect();
        match fault {
            Some(Fault::WrongDimensions) => items.iter_mut().for_each(|(_, v)| {
                v.pop();
            }),
            Some(Fault::WrongCount(d)) if d < 0 => {
                items.truncate(items.len().saturating_sub(d.unsigned_abs() as usize))
            }
            Some(Fault::WrongCount(d)) => {
                for k in 0..d as usize {
                    items.push((inputs.len() + k, items[0].1.clone()));
                }
            }
            Some(Fault::DuplicateIndex) if items.len() > 1 => {
                let last = items.len() - 1;
                items[last].0 = 0;
            }
            Some(Fault::Nan) => items[0].1[0] = f32::NAN,
            Some(Fault::ZeroNorm) => items[0].1.iter_mut().for_each(|x| *x = 0.0),
            Some(Fault::Reorder) => items.reverse(),
            _ => {}
        }
        let vectors = validate_items(items, inputs.len(), self.spec.dimensions)?;
        let tok = tokenizer();
        Ok(EmbedResponse {
            vectors,
            usage_tokens: Some(inputs.iter().map(|t| tok.count(t) as u64).sum()),
        })
    }
}

fn hash_mod(s: &str, m: usize) -> usize {
    (u64::from_le_bytes(
        blake3::hash(s.as_bytes()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    ) % m as u64) as usize
}

impl EmbeddingProvider for FakeProvider {
    fn profile(&self) -> &ProfileSpec {
        &self.spec
    }
    fn limits(&self) -> ProviderLimits {
        self.limits
    }
    fn is_hosted(&self) -> bool {
        self.hosted
    }
    fn embed<'a>(
        &'a self,
        purpose: Purpose,
        inputs: &'a [String],
    ) -> BoxFuture<'a, Result<EmbedResponse, EmbedError>> {
        Box::pin(self.run(purpose, inputs))
    }
}
