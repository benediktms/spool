//! OpenAI-compatible embeddings client (spec 12.1, 12.4, 11.7 classification).

use super::provider::*;
use crate::config::EmbeddingConfig;
use futures::future::BoxFuture;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

const ERROR_BODY_CAP: usize = 64 * 1024;
const MAX_RETRY_AFTER: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialProblem {
    Missing,
    /// The file is not private (0600 on Unix, current-user ACL on Windows).
    TooPermissive,
    Empty,
}

impl CredentialProblem {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Missing => "credential file is missing or unreadable",
            Self::TooPermissive => "credential file permissions are too open (must be private)",
            Self::Empty => "credential file is empty or malformed",
        }
    }
}

/// Reads the API key. Mode is checked on the opened handle (no stat/open race); the key is never logged.
pub fn read_credential(path: &Path) -> Result<String, CredentialProblem> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).map_err(|_| CredentialProblem::Missing)?;
    let md = f.metadata().map_err(|_| CredentialProblem::Missing)?;
    if !md.is_file()
        || crate::perms::check_private(path, 0o600).is_err()
        || !crate::perms::is_current_user(&f).map_err(|_| CredentialProblem::Missing)?
    {
        return Err(CredentialProblem::TooPermissive);
    }
    let mut s = String::new();
    f.by_ref()
        .take(16 * 1024)
        .read_to_string(&mut s)
        .map_err(|_| CredentialProblem::Empty)?;
    let key = s.trim();
    if key.is_empty() || key.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(CredentialProblem::Empty);
    }
    Ok(key.to_string())
}

/// `host[:port]` of the endpoint. Cleartext http is only accepted for loopback (local fake servers).
pub fn endpoint_identity(endpoint: &str) -> anyhow::Result<String> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| anyhow::anyhow!("embedding.endpoint is not a valid URL"))?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("embedding.endpoint has no host"))?
        .to_string();
    let loopback = matches!(host.as_str(), "127.0.0.1" | "localhost" | "[::1]" | "::1");
    match url.scheme() {
        "https" => {}
        "http" if loopback => {}
        _ => anyhow::bail!(
            "embedding.endpoint must be https (http is only allowed for loopback hosts)"
        ),
    }
    Ok(match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host,
    })
}

pub struct OpenAiProvider {
    spec: ProfileSpec,
    client: reqwest::Client,
    url: reqwest::Url,
    auth: HeaderValue,
    max_body: usize,
}

impl std::fmt::Debug for OpenAiProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiProvider")
            .field("profile", &self.spec)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct Item {
    index: usize,
    embedding: Vec<f32>,
}
#[derive(Deserialize)]
struct Usage {
    total_tokens: Option<u64>,
}
#[derive(Deserialize)]
struct Body {
    data: Vec<Item>,
    usage: Option<Usage>,
}
#[derive(Deserialize)]
struct ErrBody {
    error: Option<ErrDetail>,
}
#[derive(Deserialize)]
struct ErrDetail {
    message: Option<String>,
    code: Option<serde_json::Value>,
}

pub fn profile_spec(cfg: &EmbeddingConfig) -> anyhow::Result<ProfileSpec> {
    Ok(ProfileSpec {
        provider: cfg.provider.clone(),
        endpoint_identity: endpoint_identity(&cfg.endpoint)?,
        model: cfg.model.clone(),
        dimensions: cfg.dimensions,
        rendering_version: RENDERING_VERSION,
        generation: cfg.profile_generation,
    })
}

impl OpenAiProvider {
    pub fn new(cfg: &EmbeddingConfig, api_key: &str) -> anyhow::Result<Self> {
        let spec = profile_spec(cfg)?;
        let url = reqwest::Url::parse(&cfg.endpoint)?;
        let mut auth = HeaderValue::from_str(&format!("Bearer {api_key}"))
            .map_err(|_| anyhow::anyhow!("credential is not a valid header value"))?;
        auth.set_sensitive(true);
        let total = Duration::from_secs(cfg.request_timeout_seconds.max(1));
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(total.min(Duration::from_secs(10)))
            .timeout(total)
            .no_proxy()
            .user_agent(concat!("spool/", env!("CARGO_PKG_VERSION")))
            .build()?;
        // Generous bound for the documented response shape: ~40 bytes per float plus envelope.
        let max_body = 1 << 20;
        Ok(Self {
            max_body: max_body + cfg.batch_max_inputs.max(1) * cfg.dimensions * 40,
            spec,
            client,
            url,
            auth,
        })
    }

    async fn call(&self, inputs: &[String]) -> Result<EmbedResponse, EmbedError> {
        check_request(&self.limits(), inputs)?;
        let payload = serde_json::json!({
            "model": self.spec.model,
            "input": inputs,
            "dimensions": self.spec.dimensions,
            "encoding_format": "float",
        });
        let body = serde_json::to_vec(&payload)
            .map_err(|_| EmbedError::new(EmbedErrorKind::BadResponse, "request encoding failed"))?;
        let resp = self
            .client
            .post(self.url.clone())
            .header(AUTHORIZATION, self.auth.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(classify_send)?;
        let status = resp.status();
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_retry_after);
        if let Some(len) = resp.content_length()
            && len as usize > self.max_body
            && status.is_success()
        {
            return Err(EmbedError::new(
                EmbedErrorKind::BadResponse,
                "response exceeds size limit",
            )
            .ambiguous(true));
        }
        if !status.is_success() {
            let raw = read_capped(resp, ERROR_BODY_CAP, true)
                .await
                .unwrap_or_default();
            return Err(classify_status(status.as_u16(), retry_after, &raw));
        }
        let raw = read_capped(resp, self.max_body, false).await?;
        let parsed: Body = serde_json::from_slice(&raw).map_err(|_| {
            EmbedError::new(
                EmbedErrorKind::BadResponse,
                "response is not valid embeddings JSON",
            )
            .retryable(true)
            .ambiguous(true)
        })?;
        let usage_tokens = parsed.usage.and_then(|u| u.total_tokens);
        let vectors = validate_items(
            parsed
                .data
                .into_iter()
                .map(|i| (i.index, i.embedding))
                .collect(),
            inputs.len(),
            self.spec.dimensions,
        )?;
        Ok(EmbedResponse {
            vectors,
            usage_tokens,
        })
    }
}

impl EmbeddingProvider for OpenAiProvider {
    fn profile(&self) -> &ProfileSpec {
        &self.spec
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits::OPENAI
    }
    fn embed<'a>(
        &'a self,
        _purpose: Purpose,
        inputs: &'a [String],
    ) -> BoxFuture<'a, Result<EmbedResponse, EmbedError>> {
        Box::pin(self.call(inputs))
    }
}

fn parse_retry_after(v: &str) -> Option<Duration> {
    // Integer/decimal seconds only; HTTP-date form is ignored (callers fall back to their own backoff).
    let secs: f64 = v.trim().parse().ok()?;
    if !secs.is_finite() || secs < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(secs).min(MAX_RETRY_AFTER))
}

fn classify_send(e: reqwest::Error) -> EmbedError {
    use EmbedErrorKind::*;
    if e.is_builder() {
        return EmbedError::new(Network, "request could not be built");
    }
    if e.is_connect() {
        // Connection never established: nothing was sent.
        let kind = if e.is_timeout() { Timeout } else { Network };
        return EmbedError::new(kind, "could not connect to provider").retryable(true);
    }
    if e.is_timeout() {
        return EmbedError::new(Timeout, "request timed out after send")
            .retryable(true)
            .ambiguous(true);
    }
    // Reset/closed/protocol failure once the request may have been written.
    EmbedError::new(Network, "connection failed after send")
        .retryable(true)
        .ambiguous(true)
}

async fn read_capped(
    mut resp: reqwest::Response,
    cap: usize,
    truncate_ok: bool,
) -> Result<Vec<u8>, EmbedError> {
    let mut buf = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(c)) => {
                if buf.len() + c.len() > cap {
                    if truncate_ok {
                        buf.extend_from_slice(&c[..cap - buf.len()]);
                        return Ok(buf);
                    }
                    return Err(EmbedError::new(
                        EmbedErrorKind::BadResponse,
                        "response exceeds size limit",
                    )
                    .ambiguous(true));
                }
                buf.extend_from_slice(&c);
            }
            Ok(None) => return Ok(buf),
            Err(_) if truncate_ok => return Ok(buf),
            Err(e) if e.is_timeout() => {
                return Err(
                    EmbedError::new(EmbedErrorKind::Timeout, "response body timed out")
                        .retryable(true)
                        .ambiguous(true),
                );
            }
            // Truncated body / reset while reading: the provider processed the request.
            Err(_) => {
                return Err(EmbedError::new(
                    EmbedErrorKind::Network,
                    "response body was truncated",
                )
                .retryable(true)
                .ambiguous(true));
            }
        }
    }
}

fn classify_status(status: u16, retry_after: Option<Duration>, raw: &[u8]) -> EmbedError {
    use EmbedErrorKind::*;
    let detail = serde_json::from_slice::<ErrBody>(raw)
        .ok()
        .and_then(|b| b.error);
    let code = detail
        .as_ref()
        .and_then(|d| d.code.as_ref())
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let msg = detail
        .as_ref()
        .and_then(|d| d.message.as_deref())
        .unwrap_or("")
        .to_ascii_lowercase();
    // Inspected only to classify; never copied into the error.
    let model_problem =
        code.contains("model") || msg.contains("model") || msg.contains("dimension");
    match status {
        401 | 403 => EmbedError::new(Auth, "provider rejected the credential"),
        404 => EmbedError::new(UnsupportedModel, "provider endpoint or model not found"),
        400 if model_problem => EmbedError::new(
            UnsupportedModel,
            "provider rejected the model or dimensions",
        ),
        408 => EmbedError::new(Timeout, "provider request timeout")
            .retryable(true)
            .retry_after(retry_after),
        429 => {
            let quota = code.contains("insufficient_quota");
            EmbedError::new(
                RateLimited,
                if quota {
                    "provider quota exhausted"
                } else {
                    "provider rate limit"
                },
            )
            .retryable(!quota)
            .retry_after(retry_after)
        }
        500..=599 => EmbedError::new(Server, "provider server error")
            .retryable(true)
            .retry_after(retry_after),
        300..=399 => EmbedError::new(BadResponse, "unexpected redirect refused"),
        _ => EmbedError::new(BadResponse, "provider rejected the request"),
    }
}
