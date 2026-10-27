use super::{AppError, MAX_RESPONSE_FRAME, Outcome, Request, Response, framing, transport};
use futures::future::BoxFuture;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub type Handler =
    Arc<dyn Fn(Request) -> BoxFuture<'static, Result<Value, AppError>> + Send + Sync>;

/// Accept loop. `concurrency` bounds simultaneously served requests; connections beyond that wait.
/// Unix peers are checked against our uid; Windows enforces the same boundary in the pipe DACL.
pub async fn serve(
    mut listener: transport::Listener,
    handler: Handler,
    concurrency: usize,
    max_request: usize,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let sem = Arc::new(Semaphore::new(concurrency));
    #[cfg(unix)]
    let my_uid = rustix::process::geteuid().as_raw();
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            acc = transport::accept(&mut listener) => {
                let Ok(stream) = acc else { continue };
                #[cfg(unix)]
                match stream.peer_cred() {
                    Ok(c) if c.uid() == my_uid => {}
                    _ => { tracing::warn!("rejected ipc peer with different or unknown uid"); continue }
                }
                let Ok(permit) = sem.clone().acquire_owned().await else { return };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let _p = permit;
                    if let Err(e) = handle(stream, handler, max_request).await {
                        tracing::debug!(error = %e, "ipc connection ended with error");
                    }
                });
            }
        }
    }
}

async fn handle(
    mut s: transport::Stream,
    handler: Handler,
    max_request: usize,
) -> std::io::Result<()> {
    let Some(frame) = framing::read_frame(&mut s, max_request).await? else {
        return Ok(());
    };
    let resp = match serde_json::from_slice::<Request>(&frame) {
        Ok(req) => {
            let id = req.id();
            let body = match handler(req).await {
                Ok(v) => Outcome::Ok(v),
                Err(e) => Outcome::Err(e),
            };
            Response { id, body }
        }
        Err(_) => Response {
            id: 0,
            body: Outcome::Err(AppError::new("INVALID_ARGUMENT", "malformed IPC request")),
        },
    };
    let mut bytes = serde_json::to_vec(&resp).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_RESPONSE_FRAME {
        bytes = serde_json::to_vec(&Response {
            id: resp.id,
            body: Outcome::Err(AppError::new(
                "RESOURCE_LIMIT",
                "response exceeds the IPC frame limit",
            )),
        })
        .map_err(std::io::Error::other)?;
    }
    framing::write_frame(&mut s, &bytes, MAX_RESPONSE_FRAME).await
}
