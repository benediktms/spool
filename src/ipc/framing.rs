//! Length-prefixed frames: u32 big-endian length, then that many bytes of JSON.
//! Both directions are bounded; oversized or truncated frames are errors.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Read one frame. `Ok(None)` is a clean EOF before any length byte.
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match r.read(&mut len[..1]).await? {
        0 => return Ok(None),
        _ => r.read_exact(&mut len[1..]).await?,
    };
    let n = u32::from_be_bytes(len) as usize;
    if n > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {n} bytes exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    body: &[u8],
    max: usize,
) -> io::Result<()> {
    if body.len() > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {} bytes exceeds limit {max}", body.len()),
        ));
    }
    w.write_all(&(body.len() as u32).to_be_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}
