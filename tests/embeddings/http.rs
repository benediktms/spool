//! OpenAiProvider against a hand-rolled HTTP/1.1 server on 127.0.0.1 (spec 19.6).

use super::*;
use parking_lot::Mutex;
use spool::clock::ManualClock;
use spool::config::{Config, EmbeddingConfig, Paths};
use spool::embeddings::openai::{CredentialProblem, endpoint_identity, read_credential};
use spool::embeddings::vectors::dot;
use spool::embeddings::{
    EmbedErrorKind as K, EmbeddingProvider, EmbeddingService, EmbeddingState, OpenAiProvider,
    Purpose, probe_provider,
};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DIMS: usize = 4;

#[derive(Clone)]
enum Action {
    Json(u16, Vec<(&'static str, String)>, String),
    /// Read the whole request, then never answer.
    ReadThenHang,
    /// Read the whole request, then close the socket without any response.
    ReadThenClose,
    /// Promise more bytes than are sent, then close.
    Truncated(String),
}

#[derive(Clone, Debug)]
struct Seen {
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

struct Mock {
    addr: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    hits: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    async fn start(handler: impl Fn(usize, &Seen) -> Action + Send + Sync + 'static) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen, hits) = (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
        );
        let handler = Arc::new(handler);
        let (s2, h2) = (seen.clone(), hits.clone());
        let task = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                let (handler, seen, hits) = (handler.clone(), s2.clone(), h2.clone());
                tokio::spawn(async move { serve(sock, handler, seen, hits).await });
            }
        });
        Mock {
            addr,
            seen,
            hits,
            task,
        }
    }

    /// Accepts connections and never reads or answers (a hung peer).
    async fn hung() -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let h2 = hits.clone();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                h2.fetch_add(1, Ordering::SeqCst);
                held.push(sock);
            }
        });
        Mock {
            addr,
            seen: Arc::new(Mutex::new(Vec::new())),
            hits,
            task,
        }
    }

    fn ok(vectors: Vec<(usize, Vec<f32>)>, tokens: u64) -> Action {
        Action::Json(200, vec![], embeddings_json(&vectors, tokens))
    }

    fn url(&self) -> String {
        format!("http://{}/v1/embeddings", self.addr)
    }
    fn hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }
    fn cfg(&self) -> EmbeddingConfig {
        EmbeddingConfig {
            endpoint: self.url(),
            dimensions: DIMS,
            request_timeout_seconds: 1,
            ..emb_cfg()
        }
    }
    fn provider(&self) -> OpenAiProvider {
        OpenAiProvider::new(&self.cfg(), "sk-test-key").unwrap()
    }
}

fn embeddings_json(vectors: &[(usize, Vec<f32>)], tokens: u64) -> String {
    let data: Vec<_> = vectors
        .iter()
        .map(|(i, v)| serde_json::json!({"object": "embedding", "index": i, "embedding": v}))
        .collect();
    serde_json::json!({"object": "list", "data": data, "model": "m", "usage": {"prompt_tokens": tokens, "total_tokens": tokens}}).to_string()
}

fn err_json(status: u16, code: &str, message: &str) -> Action {
    Action::Json(
        status,
        vec![],
        serde_json::json!({"error": {"message": message, "type": "x", "code": code}}).to_string(),
    )
}

async fn serve(
    mut sock: TcpStream,
    handler: Arc<dyn Fn(usize, &Seen) -> Action + Send + Sync>,
    seen: Arc<Mutex<Vec<Seen>>>,
    hits: Arc<AtomicUsize>,
) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        let k = sock.read(&mut tmp).await.unwrap_or(0);
        if k == 0 {
            return;
        }
        buf.extend_from_slice(&tmp[..k]);
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let path = lines
        .next()
        .unwrap_or("")
        .split(' ')
        .nth(1)
        .unwrap_or("")
        .to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < head_end + len {
        let k = sock.read(&mut tmp).await.unwrap_or(0);
        if k == 0 {
            return;
        }
        buf.extend_from_slice(&tmp[..k]);
    }
    let req = Seen {
        path,
        headers,
        body: buf[head_end..head_end + len].to_vec(),
    };
    let idx = hits.fetch_add(1, Ordering::SeqCst);
    seen.lock().push(req.clone());
    match handler(idx, &req) {
        Action::Json(status, extra, body) => {
            let mut out = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (k, v) in extra {
                out.push_str(&format!("{k}: {v}\r\n"));
            }
            out.push_str("\r\n");
            let _ = sock.write_all(out.as_bytes()).await;
            let _ = sock.write_all(body.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
        Action::ReadThenHang => tokio::time::sleep(Duration::from_secs(60)).await,
        Action::ReadThenClose => drop(sock),
        Action::Truncated(body) => {
            let out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len() + 500
            );
            let _ = sock.write_all(out.as_bytes()).await;
            let _ = sock.write_all(body.as_bytes()).await;
            drop(sock);
        }
    }
}

fn inputs(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("input {i}")).collect()
}

fn v(a: f32, b: f32, c: f32, d: f32) -> Vec<f32> {
    vec![a, b, c, d]
}

#[tokio::test]
async fn request_shape_auth_and_reordered_results() {
    let m = Mock::start(|_, req| {
        let n = serde_json::from_slice::<serde_json::Value>(&req.body).unwrap()["input"]
            .as_array()
            .unwrap()
            .len();
        // answer in reverse order; vector i has its mass on coordinate i (scaled so normalization is visible)
        Mock::ok(
            (0..n)
                .rev()
                .map(|i| {
                    (
                        i,
                        (0..DIMS)
                            .map(|d| if d == i { i as f32 + 2.0 } else { 0.0 })
                            .collect(),
                    )
                })
                .collect(),
            17,
        )
    })
    .await;
    let r = m
        .provider()
        .embed(Purpose::Ingest, &inputs(3))
        .await
        .unwrap();
    assert_eq!(r.usage_tokens, Some(17));
    assert_eq!(r.vectors.len(), 3);
    for (i, vec) in r.vectors.iter().enumerate() {
        assert!((dot(vec, vec) - 1.0).abs() < 1e-5, "unit length");
        // mapped by returned index, not response order
        assert!(
            (vec[i] - 1.0).abs() < 1e-5,
            "input {i} got someone else's vector"
        );
    }

    let seen = m.seen.lock()[0].clone();
    assert_eq!(seen.path, "/v1/embeddings");
    let auth = seen
        .headers
        .iter()
        .find(|(k, _)| k == "authorization")
        .unwrap();
    assert_eq!(auth.1, "Bearer sk-test-key");
    let body: serde_json::Value = serde_json::from_slice(&seen.body).unwrap();
    assert_eq!(body["model"], "text-embedding-3-small");
    assert_eq!(body["dimensions"], DIMS);
    assert_eq!(body["encoding_format"], "float");
    assert_eq!(
        body["input"],
        serde_json::json!(["input 0", "input 1", "input 2"])
    );
    assert!(
        !format!("{:?}", m.provider()).contains("sk-test-key"),
        "Debug never prints the credential"
    );
}

#[tokio::test]
async fn empty_and_oversized_requests_never_reach_the_network() {
    let m = Mock::start(|_, _| Mock::ok(vec![(0, v(1.0, 0.0, 0.0, 0.0))], 1)).await;
    let p = m.provider();
    assert!(p.embed(Purpose::Ingest, &[]).await.is_err());
    assert!(
        p.embed(Purpose::Ingest, &["ok".into(), "".into()])
            .await
            .is_err()
    );
    assert!(p.embed(Purpose::Ingest, &["  \n".into()]).await.is_err());
    assert!(p.embed(Purpose::Ingest, &inputs(2049)).await.is_err());
    assert_eq!(m.hits(), 0);
}

#[tokio::test]
async fn malformed_successful_responses_are_classified() {
    let u = |vs: Vec<(usize, Vec<f32>)>| Mock::ok(vs, 5);
    let cases: Vec<(&str, Action, EmbedErrorKindExpect)> = vec![
        ("too few", u(vec![(0, v(1.0, 0.0, 0.0, 0.0))]), (K::BadResponse, true, true)),
        ("too many", u(vec![(0, v(1.0, 0.0, 0.0, 0.0)), (1, v(1.0, 0.0, 0.0, 0.0)), (2, v(1.0, 0.0, 0.0, 0.0))]), (K::BadResponse, true, true)),
        ("wrong dims", u(vec![(0, vec![1.0, 0.0, 0.0]), (1, vec![1.0, 0.0, 0.0])]), (K::UnsupportedModel, false, true)),
        ("duplicate index", u(vec![(0, v(1.0, 0.0, 0.0, 0.0)), (0, v(0.0, 1.0, 0.0, 0.0))]), (K::BadResponse, false, true)),
        ("index out of range", u(vec![(0, v(1.0, 0.0, 0.0, 0.0)), (5, v(0.0, 1.0, 0.0, 0.0))]), (K::BadResponse, false, true)),
        ("zero norm", u(vec![(0, v(1.0, 0.0, 0.0, 0.0)), (1, v(0.0, 0.0, 0.0, 0.0))]), (K::BadResponse, false, true)),
        ("f32 overflow", Action::Json(200, vec![], r#"{"data":[{"index":0,"embedding":[1e39,0,0,0]},{"index":1,"embedding":[1,0,0,0]}],"usage":{"total_tokens":3}}"#.into()), (K::BadResponse, true, true)),
        ("string float", Action::Json(200, vec![], r#"{"data":[{"index":0,"embedding":["NaN",0,0,0]},{"index":1,"embedding":[1,0,0,0]}]}"#.into()), (K::BadResponse, true, true)),
        ("out of range float literal", Action::Json(200, vec![], r#"{"data":[{"index":0,"embedding":[1e999,0,0,0]},{"index":1,"embedding":[1,0,0,0]}]}"#.into()), (K::BadResponse, true, true)),
        ("truncated JSON", Action::Json(200, vec![], r#"{"data":[{"index":0,"embedding":[1,0,0"#.into()), (K::BadResponse, true, true)),
        ("not json", Action::Json(200, vec![], "<html>gateway</html>".into()), (K::BadResponse, true, true)),
    ];
    for (name, action, (kind, retryable, ambiguous)) in cases {
        let m = Mock::start(move |_, _| action.clone()).await;
        let e = m
            .provider()
            .embed(Purpose::Ingest, &inputs(2))
            .await
            .unwrap_err();
        assert_eq!(
            (e.kind, e.retryable, e.ambiguous),
            (kind, retryable, ambiguous),
            "{name}"
        );
        // content-free
        assert!(
            !e.to_string().contains("input 0") && !e.to_string().contains("sk-test"),
            "{name}"
        );
    }
}

type EmbedErrorKindExpect = (K, bool, bool);

#[tokio::test]
async fn http_status_classification() {
    // (status, extra headers, body, kind, retryable, retry_after)
    let cases: Vec<(&str, Action, K, bool, Option<u64>)> = vec![
        (
            "400 generic",
            err_json(400, "invalid_request", "bad input"),
            K::BadResponse,
            false,
            None,
        ),
        (
            "400 model",
            err_json(400, "x", "The model `foo` does not support dimensions"),
            K::UnsupportedModel,
            false,
            None,
        ),
        (
            "401",
            err_json(401, "invalid_api_key", "nope"),
            K::Auth,
            false,
            None,
        ),
        (
            "403",
            err_json(403, "forbidden", "nope"),
            K::Auth,
            false,
            None,
        ),
        (
            "404",
            err_json(404, "model_not_found", "no model"),
            K::UnsupportedModel,
            false,
            None,
        ),
        (
            "429 retry-after",
            Action::Json(429, vec![("Retry-After", "7".into())], "{}".into()),
            K::RateLimited,
            true,
            Some(7),
        ),
        (
            "429 huge retry-after is capped",
            Action::Json(429, vec![("Retry-After", "999999".into())], "{}".into()),
            K::RateLimited,
            true,
            Some(3600),
        ),
        (
            "429 quota",
            err_json(429, "insufficient_quota", "billing"),
            K::RateLimited,
            false,
            None,
        ),
        (
            "500",
            Action::Json(500, vec![], "oops".into()),
            K::Server,
            true,
            None,
        ),
        (
            "503 retry-after",
            Action::Json(503, vec![("Retry-After", "2".into())], "".into()),
            K::Server,
            true,
            Some(2),
        ),
        (
            "302 redirect",
            Action::Json(
                302,
                vec![("Location", "http://127.0.0.1:1/elsewhere".into())],
                "".into(),
            ),
            K::BadResponse,
            false,
            None,
        ),
    ];
    for (name, action, kind, retryable, retry_after) in cases {
        let m = Mock::start(move |_, _| action.clone()).await;
        let e = m
            .provider()
            .embed(Purpose::Ingest, &inputs(1))
            .await
            .unwrap_err();
        assert_eq!(
            (e.kind, e.retryable, e.retry_after.map(|d| d.as_secs())),
            (kind, retryable, retry_after),
            "{name}"
        );
        assert!(
            !e.ambiguous,
            "{name}: an HTTP error status means the request was refused"
        );
        assert_eq!(m.hits(), 1, "{name}: no automatic retry");
        let text = e.to_string();
        assert!(
            !text.contains("bad input") && !text.contains("billing") && !text.contains("foo"),
            "{name}: provider text leaked into {text}"
        );
    }
}

#[tokio::test]
async fn redirects_are_never_followed_so_credentials_stay_on_host() {
    let elsewhere = Mock::start(|_, _| Mock::ok(vec![(0, v(1.0, 0.0, 0.0, 0.0))], 1)).await;
    let target = elsewhere.url();
    let m =
        Mock::start(move |_, _| Action::Json(307, vec![("Location", target.clone())], "".into()))
            .await;
    let e = m
        .provider()
        .embed(Purpose::Ingest, &inputs(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::BadResponse);
    assert_eq!(
        elsewhere.hits(),
        0,
        "the redirect target must never receive the request or the bearer token"
    );
}

#[tokio::test]
async fn hung_connection_and_timeout_after_receipt_are_bounded_and_ambiguous() {
    for m in [
        Mock::hung().await,
        Mock::start(|_, _| Action::ReadThenHang).await,
    ] {
        let t = Instant::now();
        let e = m
            .provider()
            .embed(Purpose::Ingest, &inputs(1))
            .await
            .unwrap_err();
        let took = t.elapsed();
        assert_eq!((e.kind, e.retryable, e.ambiguous), (K::Timeout, true, true));
        assert!(
            took >= Duration::from_millis(900) && took < Duration::from_secs(4),
            "{took:?}"
        );
        assert_eq!(m.hits(), 1, "no hidden retry");
    }
}

#[tokio::test]
async fn connection_failures_and_truncated_bodies() {
    // reset after the request was fully received: outcome unknown
    let m = Mock::start(|_, _| Action::ReadThenClose).await;
    let e = m
        .provider()
        .embed(Purpose::Ingest, &inputs(1))
        .await
        .unwrap_err();
    assert_eq!((e.kind, e.retryable, e.ambiguous), (K::Network, true, true));

    // body shorter than Content-Length
    let m = Mock::start(|_, _| Action::Truncated(r#"{"data":[{"index":0,"embe"#.into())).await;
    let e = m
        .provider()
        .embed(Purpose::Ingest, &inputs(1))
        .await
        .unwrap_err();
    assert_eq!((e.kind, e.retryable, e.ambiguous), (K::Network, true, true));

    // nothing listening: nothing was sent, so not ambiguous
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let cfg = EmbeddingConfig {
        endpoint: format!("http://127.0.0.1:{port}/v1/embeddings"),
        dimensions: DIMS,
        request_timeout_seconds: 1,
        ..emb_cfg()
    };
    let e = OpenAiProvider::new(&cfg, "k")
        .unwrap()
        .embed(Purpose::Ingest, &inputs(1))
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind, e.retryable, e.ambiguous),
        (K::Network, true, false)
    );
}

#[tokio::test]
async fn response_size_is_capped() {
    let big = format!("{{\"data\":[],\"pad\":\"{}\"}}", "x".repeat(3 << 20));
    let m = Mock::start(move |_, _| Action::Json(200, vec![], big.clone())).await;
    let e = m
        .provider()
        .embed(Purpose::Ingest, &inputs(1))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::BadResponse);
    assert!(e.to_string().contains("size limit"));
}

#[test]
fn credential_file_must_be_private() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("provider.key");
    assert_eq!(read_credential(&p).unwrap_err(), CredentialProblem::Missing);
    std::fs::write(&p, "sk-abc\n").unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        read_credential(&p).unwrap_err(),
        CredentialProblem::TooPermissive
    );
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(read_credential(&p).unwrap(), "sk-abc");
    std::fs::write(&p, "  \n").unwrap();
    assert_eq!(read_credential(&p).unwrap_err(), CredentialProblem::Empty);
}

#[test]
fn endpoint_must_be_https_unless_loopback() {
    assert_eq!(
        endpoint_identity("https://api.openai.com/v1/embeddings").unwrap(),
        "api.openai.com"
    );
    assert_eq!(
        endpoint_identity("http://127.0.0.1:8080/v1/embeddings").unwrap(),
        "127.0.0.1:8080"
    );
    assert!(endpoint_identity("http://api.openai.com/v1/embeddings").is_err());
    assert!(endpoint_identity("not a url").is_err());
}

// ---- consent gating: no socket is ever opened -------------------------------------------------------------

struct Lair {
    listener: std::net::TcpListener,
    cfg: Config,
    paths: Paths,
    _dir: tempfile::TempDir,
}

impl Lair {
    fn new(approved: bool, enabled: bool) -> Lair {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths::from_root(dir.path().join("h")).unwrap();
        std::fs::create_dir_all(&paths.secrets_dir).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut cfg = Config::default();
        cfg.embedding = EmbeddingConfig {
            endpoint: format!("http://{}/v1/embeddings", listener.local_addr().unwrap()),
            hosted_processing_approved: approved,
            enabled,
            dimensions: DIMS,
            request_timeout_seconds: 1,
            ..Default::default()
        };
        Lair {
            listener,
            cfg,
            paths,
            _dir: dir,
        }
    }
    fn write_credential(&self, mode: u32) {
        let p = self.cfg.credential_path(&self.paths.root).unwrap();
        std::fs::write(&p, "sk-test-key\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    fn connections(&self) -> usize {
        let mut n = 0;
        while self.listener.accept().is_ok() {
            n += 1;
        }
        n
    }
}

#[tokio::test]
async fn closed_consent_gate_never_opens_a_socket() {
    use EmbeddingState as S;
    // (approved, enabled, write credential with mode, expected state, expected error kind)
    let cases = [
        (false, true, Some(0o600), S::NotApproved, K::NotApproved),
        (true, false, Some(0o600), S::Disabled, K::Disabled),
        (true, true, None, S::NoCredential, K::NoCredential),
        (true, true, Some(0o644), S::NoCredential, K::NoCredential),
    ];
    for (approved, enabled, mode, state, kind) in cases {
        let lair = Lair::new(approved, enabled);
        if let Some(m) = mode {
            lair.write_credential(m);
        }
        let (_d, index) = open_index().await;
        let svc = EmbeddingService::from_config(
            index,
            &lair.cfg,
            &lair.paths,
            Arc::new(ManualClock::new(NOON)),
        )
        .unwrap();
        assert_eq!(svc.status().state, state);
        assert_eq!(
            svc.embed_query("private words", Duration::from_secs(1))
                .await
                .unwrap_err()
                .kind,
            kind
        );
        assert_eq!(
            svc.embed_documents(
                vec![(digest_of(1), "private words".into())],
                &AtomicBool::new(false)
            )
            .await
            .unwrap_err()
            .kind,
            kind
        );
        assert_eq!(svc.probe().await.unwrap_err().kind, kind);
        let cred = lair.cfg.credential_path(&lair.paths.root).unwrap();
        assert_eq!(
            probe_provider(&lair.cfg.embedding, &cred)
                .await
                .unwrap_err()
                .kind,
            kind
        );
        assert_eq!(
            lair.connections(),
            0,
            "{state:?}: a connection was opened despite the closed gate"
        );
        if mode == Some(0o644) {
            assert!(svc.status().detail.unwrap().contains("0600"));
        }
    }
}

#[tokio::test]
async fn from_config_with_open_gate_embeds_through_the_listener_and_status_is_ready() {
    let m = Mock::start(|_, req| {
        let n = serde_json::from_slice::<serde_json::Value>(&req.body).unwrap()["input"]
            .as_array()
            .unwrap()
            .len();
        Mock::ok((0..n).map(|i| (i, v(1.0, i as f32, 0.0, 1.0))).collect(), 9)
    })
    .await;
    let lair = Lair::new(true, true);
    let mut cfg = lair.cfg.clone();
    cfg.embedding.endpoint = m.url();
    lair.write_credential(0o600);
    let (_d, index) = open_index().await;
    let svc =
        EmbeddingService::from_config(index, &cfg, &lair.paths, Arc::new(ManualClock::new(NOON)))
            .unwrap();
    assert_eq!(svc.status().state, EmbeddingState::Ready);
    assert_eq!(svc.profile().endpoint_identity, m.addr.to_string());
    let q = svc
        .embed_query("a question", Duration::from_secs(2))
        .await
        .unwrap();
    assert!((dot(&q, &q) - 1.0).abs() < 1e-5);
    let cred = cfg.credential_path(&lair.paths.root).unwrap();
    let out = probe_provider(&cfg.embedding, &cred).await.unwrap();
    assert_eq!(out.dimensions, DIMS);
    assert_eq!(lair.connections(), 0);
}

#[tokio::test]
async fn auth_failure_does_not_resend_text_on_later_calls() {
    let m = Mock::start(|_, _| err_json(401, "invalid_api_key", "nope")).await;
    let (_d, index) = open_index().await;
    let svc = EmbeddingService::new(
        index,
        m.cfg(),
        search_cfg(),
        Arc::new(ManualClock::new(NOON)),
        Arc::new(m.provider()),
    );
    assert_eq!(
        svc.embed_query("private query text", Duration::from_secs(2))
            .await
            .unwrap_err()
            .kind,
        K::Auth
    );
    assert_eq!(svc.status().state, EmbeddingState::AuthBlocked);
    for i in 0..5 {
        assert_eq!(
            svc.embed_query(
                &format!("another private query {i}"),
                Duration::from_secs(2)
            )
            .await
            .unwrap_err()
            .kind,
            K::Auth
        );
        assert_eq!(
            svc.embed_documents(
                vec![(digest_of(i), format!("private doc {i}"))],
                &AtomicBool::new(false)
            )
            .await
            .unwrap_err()
            .kind,
            K::Auth
        );
    }
    assert_eq!(
        m.hits(),
        1,
        "only the first request ever reached the provider"
    );
    let body = String::from_utf8(m.seen.lock()[0].body.clone()).unwrap();
    assert!(body.contains("private query text"));
}

#[tokio::test]
async fn search_waits_at_most_its_deadline_on_a_hung_provider_then_fails_fast() {
    let m = Mock::start(|_, _| Action::ReadThenHang).await;
    let (_d, index) = open_index().await;
    let mut cfg = m.cfg();
    cfg.request_timeout_seconds = 30;
    let svc = EmbeddingService::new(
        index,
        cfg.clone(),
        search_cfg(),
        Arc::new(ManualClock::new(NOON)),
        Arc::new(OpenAiProvider::new(&cfg, "k").unwrap()),
    );
    for i in 0..3 {
        let t = Instant::now();
        let e = svc
            .embed_query(&format!("slow {i}"), Duration::from_millis(300))
            .await
            .unwrap_err();
        assert_eq!((e.kind, e.ambiguous), (K::Timeout, true));
        assert!(t.elapsed() < Duration::from_secs(2));
    }
    // outage is now known: the next search does not wait at all and sends nothing
    let t = Instant::now();
    let e = svc
        .embed_query("fast fail", Duration::from_secs(2))
        .await
        .unwrap_err();
    assert_eq!(e.kind, K::Unavailable);
    assert!(t.elapsed() < Duration::from_millis(200));
    assert_eq!(m.hits(), 3);
}
