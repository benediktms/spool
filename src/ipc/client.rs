use super::{AppError, MAX_RESPONSE_FRAME, Outcome, Request, Response, framing};
use serde_json::Value;
use std::path::Path;
use tokio::net::UnixStream;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error(
        "cannot reach the spool daemon at {path}: {source}. Start it with `spool service start` or `spool daemon run`"
    )]
    Unavailable {
        path: String,
        source: std::io::Error,
    },
    #[error("ipc: {0}")]
    Io(#[from] std::io::Error),
    #[error("ipc protocol: {0}")]
    Protocol(String),
    #[error(transparent)]
    App(#[from] AppError),
}

/// One short-lived request per connection keeps bridges disposable and cancellation trivial.
pub async fn call(socket: &Path, req: Request, max_request: usize) -> Result<Value, ClientError> {
    let mut s = UnixStream::connect(socket)
        .await
        .map_err(|source| ClientError::Unavailable {
            path: socket.display().to_string(),
            source,
        })?;
    let id = req.id();
    let body = serde_json::to_vec(&req).map_err(|e| ClientError::Protocol(e.to_string()))?;
    framing::write_frame(&mut s, &body, max_request).await?;
    let frame = framing::read_frame(&mut s, MAX_RESPONSE_FRAME)
        .await?
        .ok_or_else(|| {
            ClientError::Protocol("daemon closed the connection without a response".into())
        })?;
    let resp: Response =
        serde_json::from_slice(&frame).map_err(|e| ClientError::Protocol(e.to_string()))?;
    if resp.id != id {
        return Err(ClientError::Protocol("response id mismatch".into()));
    }
    match resp.body {
        Outcome::Ok(v) => Ok(v),
        Outcome::Err(e) => Err(ClientError::App(e)),
    }
}
