//! TCP server and per-connection task.
//!
//! Each accepted connection runs one async task that owns the socket. The read
//! half is driven by `framing::read_packet`; outgoing packets (routed
//! publishes, acknowledgements, control) arrive on a bounded channel
//! (`broker::OUTBOUND_CHANNEL_CAPACITY`) that the broker pushes into with a
//! non-blocking `try_send`. A `tokio::select!` multiplexes the two.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::broker::{Action, Broker, Outgoing};
use crate::error::{MqttError, Result};
use crate::framing::{read_packet, write_packet, ReadOutcome};
use crate::metrics::Metrics;
use crate::packet::{Disconnect, Packet};
use crate::types::{ProtocolVersion, ReasonCode};

/// Write a packet (in the connection's protocol version) and record it in the
/// metrics counters.
async fn send<W: AsyncWrite + Unpin>(
    wr: &mut W,
    packet: &Packet,
    version: ProtocolVersion,
    metrics: &Metrics,
) -> Result<()> {
    let n = write_packet(wr, packet, version).await?;
    metrics.record_sent(packet.packet_type(), n);
    if let Packet::Publish(p) = packet {
        Metrics::add(&metrics.publish_bytes_sent, p.payload.len() as u64);
    }
    Ok(())
}

/// Time allowed to receive the initial CONNECT before dropping the socket.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Bind a raw-TCP MQTT listener — plain if `acceptor` is `None`, TLS (or
/// mutual TLS, per what `acceptor` was built with) otherwise — and serve it
/// until the process is stopped. `mode` is purely a log-line suffix.
///
/// Shared by [`run`] (always `None`) and [`run_tls`] (always `Some`), which
/// are otherwise identical, so a plain and a TLS MQTT listener can run at
/// the same time on independent addresses.
async fn serve_mqtt(
    broker: Broker,
    addr: SocketAddr,
    acceptor: Option<TlsAcceptor>,
    mode: &'static str,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("MQTT v5.0 broker listening on {addr}{mode}");

    loop {
        let (stream, peer) = listener.accept().await?;
        Metrics::inc(broker.metrics().socket_connections_ref());
        if !broker.rate_limiter().check(peer.ip()) {
            Metrics::inc(&broker.metrics().connections_rate_limited);
            tracing::debug!("rate-limited connection from {peer}");
            continue;
        }
        let broker = broker.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            stream.set_nodelay(true).ok();
            // Wrap in TLS if configured; the handshake runs inside the task so
            // it never blocks the accept loop. For mutual TLS, the client
            // certificate CN becomes the authenticated identity.
            let result = match acceptor {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls) => {
                        let identity = crate::tls::peer_cn(tls.get_ref().1.peer_certificates());
                        handle_connection(tls, peer, broker, identity).await
                    }
                    Err(e) => {
                        tracing::debug!("TLS handshake with {peer} failed: {e}");
                        return;
                    }
                },
                None => handle_connection(stream, peer, broker, None).await,
            };
            if let Err(e) = result {
                tracing::debug!("connection {peer} ended: {e}");
            }
        });
    }
}

/// Bind and serve the plain MQTT listener (`config.listen_addr`, always set)
/// until the process is stopped.
pub async fn run(broker: Broker) -> Result<()> {
    let addr = broker.config().listen_addr;
    serve_mqtt(broker, addr, None, "").await
}

/// Bind and serve the dedicated TLS MQTT listener, independent of and
/// simultaneous with [`run`]. Requires `config.tls_listen_addr` to be set;
/// a no-op (`Ok(())`) otherwise, same as [`run_ws`] when unconfigured.
pub async fn run_tls(broker: Broker) -> Result<()> {
    let cfg = broker.config();
    let Some(addr) = cfg.tls_listen_addr else {
        return Ok(());
    };
    let acceptor =
        crate::tls::maybe_acceptor(&cfg.tls_cert, &cfg.tls_key, &cfg.tls_client_ca, "mqtt-tls")?
            .ok_or_else(|| {
                MqttError::Config(
                    "tls_listen_addr is set but tls_cert/tls_key are not — this should have \
                     been rejected at startup"
                        .to_string(),
                )
            })?;
    let mode = if cfg.tls_client_ca.is_some() {
        " (TLS, mutual: client certificate required)"
    } else {
        " (TLS)"
    };
    serve_mqtt(broker, addr, Some(acceptor), mode).await
}

/// Bind a WebSocket MQTT listener — plain if `acceptor` is `None`, TLS (wss)
/// otherwise — and serve it until the process is stopped.
///
/// Shared by [`run_ws`] (always `None`) and [`run_ws_tls`] (always `Some`),
/// which are otherwise identical, so a plain and a TLS WebSocket listener
/// can run at the same time on independent addresses.
async fn serve_ws(
    broker: Broker,
    addr: SocketAddr,
    acceptor: Option<TlsAcceptor>,
    mode: &'static str,
    max_packet_size: u32,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("MQTT-over-WebSocket listening on {addr}{mode}");

    loop {
        let (stream, peer) = listener.accept().await?;
        Metrics::inc(broker.metrics().socket_connections_ref());
        if !broker.rate_limiter().check(peer.ip()) {
            Metrics::inc(&broker.metrics().connections_rate_limited);
            tracing::debug!("rate-limited connection from {peer}");
            continue;
        }
        let broker = broker.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            stream.set_nodelay(true).ok();
            // Terminate TLS (if configured), then perform the WebSocket
            // handshake and wrap the connection as an MQTT byte stream.
            let result = match acceptor {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls) => {
                        let identity = crate::tls::peer_cn(tls.get_ref().1.peer_certificates());
                        match crate::ws::accept(tls, max_packet_size).await {
                            Ok(ws) => handle_connection(ws, peer, broker, identity).await,
                            Err(e) => {
                                tracing::debug!("WebSocket handshake with {peer} failed: {e}");
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("TLS handshake with {peer} failed: {e}");
                        return;
                    }
                },
                None => match crate::ws::accept(stream, max_packet_size).await {
                    Ok(ws) => handle_connection(ws, peer, broker, None).await,
                    Err(e) => {
                        tracing::debug!("WebSocket handshake with {peer} failed: {e}");
                        return;
                    }
                },
            };
            if let Err(e) = result {
                tracing::debug!("WebSocket connection {peer} ended: {e}");
            }
        });
    }
}

/// Bind and serve the plain MQTT-over-WebSocket listener until the process is
/// stopped. Requires `config.ws_listen_addr` to be set; a no-op (`Ok(())`)
/// otherwise.
pub async fn run_ws(broker: Broker) -> Result<()> {
    let cfg = broker.config();
    let Some(addr) = cfg.ws_listen_addr else {
        return Ok(());
    };
    let max_packet_size = cfg.max_packet_size;
    serve_ws(broker, addr, None, "", max_packet_size).await
}

/// Bind and serve the dedicated TLS (wss) MQTT-over-WebSocket listener,
/// independent of and simultaneous with [`run_ws`]. Requires
/// `config.ws_tls_listen_addr` to be set; a no-op (`Ok(())`) otherwise.
pub async fn run_ws_tls(broker: Broker) -> Result<()> {
    let cfg = broker.config();
    let Some(addr) = cfg.ws_tls_listen_addr else {
        return Ok(());
    };
    let acceptor = crate::tls::maybe_acceptor(
        &cfg.ws_tls_cert,
        &cfg.ws_tls_key,
        &cfg.ws_tls_client_ca,
        "websocket-tls",
    )?
    .ok_or_else(|| {
        MqttError::Config(
            "ws_tls_listen_addr is set but ws_tls_cert/ws_tls_key are not — this should have \
             been rejected at startup"
                .to_string(),
        )
    })?;
    let mode = if cfg.ws_tls_client_ca.is_some() {
        " (TLS, mutual: client certificate required)"
    } else {
        " (TLS)"
    };
    let max_packet_size = cfg.max_packet_size;
    serve_ws(broker, addr, Some(acceptor), mode, max_packet_size).await
}

async fn handle_connection<S>(
    stream: S,
    peer: SocketAddr,
    broker: Broker,
    identity: Option<String>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let max_packet = broker.config().max_packet_size;
    let metrics = broker.metrics();

    // --- Handshake: the first packet MUST be CONNECT (3.1.0-1). CONNECT is
    // self-describing, so it is decoded with a placeholder version. ---
    let first = match timeout(
        CONNECT_TIMEOUT,
        read_packet(&mut rd, max_packet, ProtocolVersion::V5),
    )
    .await
    {
        Ok(Ok(ReadOutcome::Packet(p, n))) => {
            metrics.record_received(p.packet_type(), n);
            p
        }
        Ok(Ok(ReadOutcome::Eof)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(MqttError::Protocol("CONNECT timeout".into())),
    };

    let connect = match first {
        Packet::Connect(c) => c,
        other => {
            return Err(MqttError::Protocol(format!(
                "first packet was {} not CONNECT",
                other.name()
            )));
        }
    };

    // Negotiated protocol version drives all subsequent framing. An unknown
    // level falls back to v3.1.1 solely to encode the rejection CONNACK.
    let version =
        ProtocolVersion::from_level(connect.protocol_version).unwrap_or(ProtocolVersion::V3_1_1);

    let (tx, mut rx) = mpsc::channel::<Outgoing>(crate::broker::OUTBOUND_CHANNEL_CAPACITY);

    let accepted = match broker.handle_connect(connect, tx, identity) {
        Ok(a) => a,
        Err(connack) => {
            // Send the rejection CONNACK, then close.
            send(&mut wr, &Packet::Connack(connack), version, metrics).await?;
            return Ok(());
        }
    };

    let client_id = accepted.client_id.clone();
    let epoch = accepted.epoch;
    let keep_alive = accepted.keep_alive;
    let identity_log = accepted.identity.as_deref().unwrap_or("<none>");
    tracing::info!(
        "client {client_id:?} connected from {peer} (v{}, identity={identity_log}, keep_alive={keep_alive}s)",
        version.level()
    );

    send(
        &mut wr,
        &Packet::Connack(accepted.connack),
        version,
        metrics,
    )
    .await?;

    // Keep Alive: disconnect if no packet arrives within 1.5x the interval
    // (3.1.2.10). Zero disables the timeout.
    let read_timeout = if keep_alive == 0 {
        Duration::from_secs(3600 * 24 * 365)
    } else {
        Duration::from_secs((keep_alive as u64 * 3) / 2 + 1)
    };

    // Reason to publish the Will: true unless the client sends a normal
    // DISCONNECT or is cleanly taken over.
    let mut publish_will = true;

    loop {
        tokio::select! {
            // Outgoing packets from the broker.
            maybe_out = rx.recv() => {
                match maybe_out {
                    Some(Outgoing::Send(publish)) => {
                        send(&mut wr, &Packet::Publish(*publish), version, metrics).await?;
                    }
                    Some(Outgoing::Control(pkt)) => {
                        send(&mut wr, &pkt, version, metrics).await?;
                    }
                    Some(Outgoing::Shutdown(rc)) => {
                        // Server-initiated close: session takeover (0x8E) or an
                        // administrative disconnect such as ACL revocation (0x87).
                        // v5 gets a DISCONNECT with the reason; v3.x has no server
                        // DISCONNECT, so we just close. The Will is suppressed; on
                        // takeover our epoch is already stale so cleanup is a no-op,
                        // while for an administrative close the epoch still matches
                        // and the session is torn down / expired normally.
                        if version.is_v5() {
                            let _ = send(&mut wr, &Packet::Disconnect(Disconnect::new(rc)), version, metrics).await;
                        }
                        publish_will = false;
                        break;
                    }
                    None => break,
                }
            }

            // Inbound packets from the client.
            read = timeout(read_timeout, read_packet(&mut rd, max_packet, version)) => {
                match read {
                    Ok(Ok(ReadOutcome::Packet(packet, n))) => {
                        metrics.record_received(packet.packet_type(), n);
                        if let Packet::Publish(p) = &packet {
                            Metrics::add(&metrics.publish_bytes_received, p.payload.len() as u64);
                        }
                        tracing::trace!("{client_id:?} -> {}", packet.name());
                        match broker.handle_packet(&client_id, epoch, packet) {
                            Action::Continue => {}
                            Action::ServerDisconnect(rc) => {
                                if version.is_v5() {
                                    let _ = send(
                                        &mut wr,
                                        &Packet::Disconnect(Disconnect::new(rc)),
                                        version,
                                        metrics,
                                    ).await;
                                }
                                break;
                            }
                            Action::ClientDisconnect { publish_will: pw } => {
                                publish_will = pw;
                                break;
                            }
                        }
                    }
                    Ok(Ok(ReadOutcome::Eof)) => {
                        // Client closed the socket without DISCONNECT: abnormal.
                        break;
                    }
                    Ok(Err(e)) => {
                        // Malformed / protocol error: inform the peer if we can.
                        if version.is_v5() {
                            let rc = e.reason_code();
                            let _ = send(
                                &mut wr,
                                &Packet::Disconnect(Disconnect::new(rc)),
                                version,
                                metrics,
                            ).await;
                        }
                        tracing::debug!("{client_id:?} protocol error: {e}");
                        break;
                    }
                    Err(_) => {
                        // Keep Alive expired (3.1.2.10-1).
                        if version.is_v5() {
                            let _ = send(
                                &mut wr,
                                &Packet::Disconnect(Disconnect::new(ReasonCode::KeepAliveTimeout)),
                                version,
                                metrics,
                            ).await;
                        }
                        tracing::debug!("{client_id:?} keep-alive timeout");
                        break;
                    }
                }
            }
        }
    }

    let _ = wr.shutdown().await;
    broker.handle_connection_closed(&client_id, epoch, publish_will);
    tracing::info!("client {client_id:?} disconnected");
    Ok(())
}
