//! Provider seam (spec 12): types, content-free errors, and the one response validator.

use super::vectors;
use futures::future::BoxFuture;
use std::time::Duration;

/// Version of the exact-input rendering (speaker labels/separators, spec 9.1) that feeds the profile id.
pub const RENDERING_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Purpose {
    Ingest,
    Query,
}

impl Purpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Query => "query",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileSpec {
    pub provider: String,
    pub endpoint_identity: String,
    pub model: String,
    pub dimensions: usize,
    pub rendering_version: u32,
    pub generation: u32,
}

impl ProfileSpec {
    /// Deterministic: length-prefixed fields hashed with blake3. Any field change is a new profile.
    pub fn id(&self) -> String {
        let mut h = blake3::Hasher::new();
        h.update(b"spool-embedding-profile-v1");
        for s in [&self.provider, &self.endpoint_identity, &self.model] {
            h.update(&(s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        }
        h.update(&(self.dimensions as u64).to_le_bytes());
        h.update(&self.rendering_version.to_le_bytes());
        h.update(&self.generation.to_le_bytes());
        format!("ep_{}", &h.finalize().to_hex()[..24])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderLimits {
    pub max_inputs_per_request: usize,
    pub max_tokens_per_input: usize,
    pub max_tokens_per_request: usize,
}

impl ProviderLimits {
    /// Documented OpenAI embeddings limits (2048 inputs, 8191 tokens/input, 300k tokens/request).
    pub const OPENAI: Self = Self {
        max_inputs_per_request: 2048,
        max_tokens_per_input: crate::config::PROVIDER_MAX_INPUT_TOKENS,
        max_tokens_per_request: 300_000,
    };
}

#[derive(Debug, Clone)]
pub struct EmbedResponse {
    /// Unit-normalized, in input order, validated.
    pub vectors: Vec<Vec<f32>>,
    pub usage_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbedErrorKind {
    NotApproved,
    NoCredential,
    Disabled,
    Auth,
    RateLimited,
    Server,
    Network,
    Timeout,
    BadResponse,
    UnsupportedModel,
    BudgetExhausted,
    /// Circuit open or provider otherwise known to be unavailable; no request was sent.
    Unavailable,
    Cancelled,
}

impl EmbedErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotApproved => "not_approved",
            Self::NoCredential => "no_credential",
            Self::Disabled => "disabled",
            Self::Auth => "auth",
            Self::RateLimited => "rate_limited",
            Self::Server => "server",
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::BadResponse => "bad_response",
            Self::UnsupportedModel => "unsupported_model",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Unavailable => "unavailable",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Content-free: `message` is always a static description, never provider body text, input text or a credential.
#[derive(Debug, Clone, thiserror::Error)]
#[error("embedding {}: {message}", kind.as_str())]
pub struct EmbedError {
    pub kind: EmbedErrorKind,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
    /// The provider may have processed (and billed) the request.
    pub ambiguous: bool,
    pub message: &'static str,
}

impl EmbedError {
    pub fn new(kind: EmbedErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            retryable: false,
            retry_after: None,
            ambiguous: false,
            message,
        }
    }
    pub fn retryable(mut self, v: bool) -> Self {
        self.retryable = v;
        self
    }
    pub fn ambiguous(mut self, v: bool) -> Self {
        self.ambiguous = v;
        self
    }
    pub fn retry_after(mut self, d: Option<Duration>) -> Self {
        self.retry_after = d;
        self
    }
}

pub trait EmbeddingProvider: Send + Sync {
    fn profile(&self) -> &ProfileSpec;
    fn limits(&self) -> ProviderLimits;
    /// True when input text leaves the machine (consent gating applies).
    fn is_hosted(&self) -> bool {
        true
    }
    fn embed<'a>(
        &'a self,
        purpose: Purpose,
        inputs: &'a [String],
    ) -> BoxFuture<'a, Result<EmbedResponse, EmbedError>>;
}

/// Shared pre-flight checks so every provider refuses to send empty or oversized batches.
pub fn check_request(limits: &ProviderLimits, inputs: &[String]) -> Result<(), EmbedError> {
    let bad = |m| Err(EmbedError::new(EmbedErrorKind::BadResponse, m));
    if inputs.is_empty() {
        return bad("empty request");
    }
    if inputs.len() > limits.max_inputs_per_request {
        return bad("too many inputs in one request");
    }
    if inputs.iter().any(|s| s.trim().is_empty()) {
        return bad("empty input refused");
    }
    Ok(())
}

/// Validate and normalize raw `(index, vector)` items (spec 12.2): item count, index range/uniqueness, every
/// dimension, finite values, non-zero norm. Output is ordered by returned index, not response order.
pub fn validate_items(
    items: Vec<(usize, Vec<f32>)>,
    expected: usize,
    dims: usize,
) -> Result<Vec<Vec<f32>>, EmbedError> {
    // The provider answered, so the request was processed and likely billed: ambiguous for budget purposes.
    let bad = |m: &'static str| EmbedError::new(EmbedErrorKind::BadResponse, m).ambiguous(true);
    if items.len() != expected {
        return Err(bad("response item count differs from request").retryable(true));
    }
    let mut slots: Vec<Option<Vec<f32>>> = vec![None; expected];
    for (idx, v) in items {
        if idx >= expected || slots[idx].is_some() {
            return Err(bad("response index out of range or duplicated"));
        }
        if v.len() != dims {
            return Err(EmbedError::new(
                EmbedErrorKind::UnsupportedModel,
                "response vector dimension differs from profile",
            )
            .ambiguous(true));
        }
        slots[idx] = Some(v);
    }
    let mut out = Vec::with_capacity(expected);
    for slot in slots {
        let mut v = slot.ok_or_else(|| bad("response is missing an index"))?;
        vectors::normalize(&mut v).map_err(|e| match e {
            vectors::VectorError::ZeroNorm => bad("response contains a zero-norm vector"),
            _ => bad("response contains non-finite values"),
        })?;
        out.push(v);
    }
    Ok(out)
}
