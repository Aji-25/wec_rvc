use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use serde::{Deserialize, Serialize};
use std::io;

use crate::sync::{SyncRequest, SyncResponse};

pub const RVC_PROTOCOL: &str = "/rvc/1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RvcRequest {
    Handshake { name: String, head_hash: Option<String> },
    Sync(SyncRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RvcResponse {
    HandshakeAck { name: String, head_hash: Option<String> },
    Sync(SyncResponse),
}

/// Length-prefixed JSON codec (4-byte big-endian length + JSON body).
#[derive(Clone, Default)]
pub struct RvcCodec;

#[async_trait]
impl request_response::Codec for RvcCodec {
    type Protocol = &'static str;
    type Request = RvcRequest;
    type Response = RvcResponse;

    async fn read_request<T>(
        &mut self,
        _: &&'static str,
        io: &mut T,
    ) -> io::Result<RvcRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io, 16 * 1024 * 1024).await
    }

    async fn read_response<T>(
        &mut self,
        _: &&'static str,
        io: &mut T,
    ) -> io::Result<RvcResponse>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io, 64 * 1024 * 1024).await
    }

    async fn write_request<T>(
        &mut self,
        _: &&'static str,
        io: &mut T,
        req: RvcRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_message(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &&'static str,
        io: &mut T,
        res: RvcResponse,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_message(io, &res).await
    }
}

async fn read_message<T, M>(io: &mut T, max_len: usize) -> io::Result<M>
where
    T: AsyncRead + Unpin + Send,
    M: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "msg too large"));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

async fn write_message<T, M>(io: &mut T, message: &M) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
    M: Serialize,
{
    let bytes = serde_json::to_vec(message)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    io.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    io.write_all(&bytes).await?;
    io.flush().await
}
