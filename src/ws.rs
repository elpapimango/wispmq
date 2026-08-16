//! MQTT over WebSockets (Section 6).
//!
//! MQTT Control Packets are carried in WebSocket **binary** frames, with no
//! alignment between packet and frame boundaries: one frame may hold several
//! packets, or part of one. This module adapts a `WebSocketStream` into a
//! byte-oriented [`AsyncRead`] + [`AsyncWrite`] stream (`WsStream`) so the same
//! `server::handle_connection` used for raw TCP/TLS works unchanged — the byte
//! stream naturally reassembles packets that span frames.
//!
//! The WebSocket handshake negotiates the `mqtt` subprotocol
//! ([MQTT-6.0.0-3/-4]).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use futures_util::sink::Sink;
use futures_util::stream::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

/// Perform the server-side WebSocket handshake (selecting the `mqtt`
/// subprotocol) over an already-accepted (optionally TLS) stream, and return a
/// byte-stream adapter over it.
pub async fn accept<S>(stream: S) -> io::Result<WsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ws = tokio_tungstenite::accept_hdr_async(stream, SelectMqttSubprotocol)
        .await
        .map_err(ws_to_io)?;
    Ok(WsStream::new(ws))
}

/// Perform the *client*-side WebSocket handshake to `url` (offering the `mqtt`
/// subprotocol) over an already-connected (optionally TLS) stream, and return a
/// byte-stream adapter. Used by the bridge to reach a `ws://`/`wss://` remote.
pub async fn client<S>(stream: S, url: &str) -> io::Result<WsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url
        .into_client_request()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
    let (ws, _resp) = tokio_tungstenite::client_async(request, stream)
        .await
        .map_err(ws_to_io)?;
    Ok(WsStream::new(ws))
}

/// Handshake callback that echoes the `mqtt` subprotocol when the client offers
/// it, as required by [MQTT-6.0.0-4].
struct SelectMqttSubprotocol;

impl Callback for SelectMqttSubprotocol {
    fn on_request(
        self,
        request: &Request,
        mut response: Response,
    ) -> Result<Response, ErrorResponse> {
        let offers_mqtt = request
            .headers()
            .get_all("Sec-WebSocket-Protocol")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|p| p.trim().eq_ignore_ascii_case("mqtt"));
        if offers_mqtt {
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
        }
        Ok(response)
    }
}

/// Byte-stream view over a WebSocket carrying MQTT binary frames.
pub struct WsStream<S> {
    inner: WebSocketStream<S>,
    /// Bytes received but not yet consumed by the reader.
    read_buf: BytesMut,
}

impl<S> WsStream<S> {
    fn new(inner: WebSocketStream<S>) -> Self {
        WsStream {
            inner,
            read_buf: BytesMut::new(),
        }
    }
}

fn ws_to_io(e: WsError) -> io::Error {
    match e {
        WsError::Io(io) => io,
        other => io::Error::other(other),
    }
}

impl<S> AsyncRead for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if !this.read_buf.is_empty() {
                let n = std::cmp::min(this.read_buf.len(), buf.remaining());
                buf.put_slice(&this.read_buf[..n]);
                this.read_buf.advance(n);
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => {
                    if msg.is_binary() {
                        let data = msg.into_data();
                        if !data.is_empty() {
                            this.read_buf.extend_from_slice(data.as_ref());
                        }
                        // Loop to drain into `buf`.
                    } else if msg.is_close() {
                        return Poll::Ready(Ok(())); // clean EOF
                    } else if msg.is_text() {
                        // MQTT MUST NOT use text frames [MQTT-6.0.0-1].
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unexpected WebSocket text frame on MQTT connection",
                        )));
                    }
                    // Ping/Pong (auto-answered by tungstenite) and any other
                    // control frames: keep polling.
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(ws_to_io(e))),
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                // Each write becomes one binary frame; MQTT permits multiple or
                // partial packets per frame, so a per-write frame is valid.
                let msg = Message::binary(buf.to_vec());
                match Pin::new(&mut this.inner).start_send(msg) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(e) => Poll::Ready(Err(ws_to_io(e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(ws_to_io(e))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(r) => Poll::Ready(r.map_err(ws_to_io)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_close(cx) {
            Poll::Ready(r) => Poll::Ready(r.map_err(ws_to_io)),
            Poll::Pending => Poll::Pending,
        }
    }
}
