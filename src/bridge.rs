//! Broker-to-broker **forwarding** (bridge).
//!
//! Each configured bridge runs an outbound MQTT client to a remote broker in
//! its own Tokio task, with automatic reconnect and exponential backoff. It
//! reuses the crate's own wire code (`codec`/`packet`/`framing`) over any
//! transport (`tcp`/`tls`/`ws`/`wss`, composed the same way the server side is).
//!
//! - **out** topics: the bridge registers a clean local subscriber
//!   (`broker::register_bridge`), subscribes at QoS 0 to the configured
//!   patterns, and republishes matching local messages to the remote at the
//!   mapping's QoS.
//! - **in** topics: the bridge SUBSCRIBEs on the remote and injects received
//!   messages back into local routing (`broker::inject_publish`).
//!
//! Loop prevention: injected messages are attributed to the bridge's own client
//! id and the local subscription is `no_local`, so a message never flows
//! straight back out the bridge it came in on.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;

use crate::broker::{Broker, Outgoing};
use crate::codec::Properties;
use crate::error::{MqttError, Result};
use crate::framing::{read_packet, write_packet, ReadOutcome};
use crate::message::Message;
use crate::metrics::Metrics;
use crate::packet::{Connect, Packet, PubAck, Publish, RetainHandling, Subscribe, TopicFilter};
use crate::topic;
use crate::types::{ProtocolVersion, QoS};

/// Which way a topic mapping forwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// local → remote
    Out,
    /// remote → local
    In,
    /// both directions
    Both,
}

impl Direction {
    fn forwards_out(self) -> bool {
        matches!(self, Direction::Out | Direction::Both)
    }
    fn forwards_in(self) -> bool {
        matches!(self, Direction::In | Direction::Both)
    }
}

/// One topic mapping in a bridge.
#[derive(Debug, Clone)]
pub struct BridgeTopic {
    pub pattern: String,
    pub direction: Direction,
    pub qos: QoS,
}

/// Configuration for one bridge to a remote broker.
///
/// `Debug` is implemented by hand to redact `password`; the derived version
/// would print the remote broker's credentials into any log line or error that
/// formats a config.
#[derive(Clone)]
pub struct BridgeConfig {
    pub name: String,
    /// `scheme://host[:port][/path]`; scheme is tcp|tls|ws|wss (mqtt/mqtts alias
    /// tcp/tls).
    pub address: String,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keepalive: u16,
    pub protocol_version: ProtocolVersion,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub tls_insecure: bool,
    pub topics: Vec<BridgeTopic>,
}

impl std::fmt::Debug for BridgeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeConfig")
            .field("name", &self.name)
            .field("address", &self.address)
            .field("client_id", &self.client_id)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("keepalive", &self.keepalive)
            .field("protocol_version", &self.protocol_version)
            .field("tls_ca", &self.tls_ca)
            .field("tls_cert", &self.tls_cert)
            .field("tls_key", &self.tls_key)
            .field("tls_insecure", &self.tls_insecure)
            .field("topics", &self.topics)
            .finish()
    }
}

// -------------------------------------------------------------------------
// Config parsing (`bridges` array)
// -------------------------------------------------------------------------

/// Parse the optional `bridges` array from the config document. Called with
/// the config file's contents pivoted to `serde_json::Value` — see
/// `config::Config::apply_toml_str`.
pub fn parse_bridges(doc: &Value, source: &str) -> Result<Vec<BridgeConfig>> {
    let node = &doc["bridges"];
    if node.is_null() {
        return Ok(Vec::new());
    }
    let list = node
        .as_array()
        .ok_or_else(|| cfg(format!("{source}: 'bridges' must be an array")))?;
    let mut out = Vec::with_capacity(list.len());
    for (i, b) in list.iter().enumerate() {
        let at = format!("{source}: bridges[{i}]");
        let name = req_str(b, "name", &at)?;
        let address = req_str(b, "address", &at)?;
        let client_id = opt_str(b, "client_id").unwrap_or_else(|| format!("pulsemq-bridge-{name}"));
        let keepalive = b["keepalive"].as_i64().unwrap_or(60);
        let keepalive = u16::try_from(keepalive)
            .map_err(|_| cfg(format!("{at}: keepalive out of range (0-65535)")))?;
        let protocol_version = match b["protocol_version"].as_i64() {
            None | Some(5) => ProtocolVersion::V5,
            Some(4) => ProtocolVersion::V3_1_1,
            Some(3) => ProtocolVersion::V3_1,
            Some(v) => {
                return Err(cfg(format!(
                    "{at}: protocol_version must be 3, 4 or 5 (got {v})"
                )))
            }
        };
        let topics = parse_topics(&b["topics"], &at)?;
        if topics.is_empty() {
            return Err(cfg(format!("{at}: at least one topic mapping is required")));
        }
        out.push(BridgeConfig {
            name,
            address,
            client_id,
            username: opt_str(b, "username"),
            password: opt_str(b, "password"),
            keepalive,
            protocol_version,
            tls_ca: opt_str(b, "tls_ca"),
            tls_cert: opt_str(b, "tls_cert"),
            tls_key: opt_str(b, "tls_key"),
            tls_insecure: b["tls_insecure"].as_bool().unwrap_or(false),
            topics,
        });
    }
    Ok(out)
}

fn parse_topics(node: &Value, at: &str) -> Result<Vec<BridgeTopic>> {
    let list = node
        .as_array()
        .ok_or_else(|| cfg(format!("{at}: 'topics' must be an array")))?;
    let mut out = Vec::with_capacity(list.len());
    for (j, t) in list.iter().enumerate() {
        let tat = format!("{at}.topics[{j}]");
        let pattern = req_str(t, "pattern", &tat)?;
        if !topic::valid_topic_filter(&pattern) {
            return Err(cfg(format!("{tat}: invalid topic filter {pattern:?}")));
        }
        let direction = match t["direction"].as_str() {
            Some("out") => Direction::Out,
            Some("in") => Direction::In,
            Some("both") => Direction::Both,
            _ => return Err(cfg(format!("{tat}: direction must be in|out|both"))),
        };
        let qos = match t["qos"].as_i64() {
            None | Some(0) => QoS::AtMostOnce,
            Some(1) => QoS::AtLeastOnce,
            Some(2) => QoS::ExactlyOnce,
            Some(v) => return Err(cfg(format!("{tat}: qos must be 0, 1 or 2 (got {v})"))),
        };
        out.push(BridgeTopic {
            pattern,
            direction,
            qos,
        });
    }
    Ok(out)
}

fn cfg(msg: String) -> MqttError {
    MqttError::Config(msg)
}

fn req_str(node: &Value, key: &str, at: &str) -> Result<String> {
    node[key]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| cfg(format!("{at}: missing string '{key}'")))
}

fn opt_str(node: &Value, key: &str) -> Option<String> {
    node[key].as_str().map(|s| s.to_string())
}

// -------------------------------------------------------------------------
// Runtime
// -------------------------------------------------------------------------

/// Run a bridge forever: register its local subscriber, then connect/serve with
/// reconnect + exponential backoff. Never returns.
pub async fn run(broker: Broker, cfg: BridgeConfig) {
    let bridge_client = format!("$bridge/{}", cfg.name);
    let (out_tx, mut out_rx) =
        tokio::sync::mpsc::channel::<Outgoing>(crate::broker::OUTBOUND_CHANNEL_CAPACITY);
    broker.register_bridge(bridge_client.clone(), out_tx);
    for t in &cfg.topics {
        if t.direction.forwards_out() {
            broker.bridge_add_subscription(&bridge_client, &t.pattern, true);
        }
    }

    let endpoint = match Endpoint::parse(&cfg.address) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("bridge {:?}: {e}", cfg.name);
            return;
        }
    };

    // Certificate verification off means any host able to intercept the
    // connection can impersonate the remote broker and read or inject every
    // forwarded message. It is a testing aid, so say so loudly and on every
    // start rather than letting it hide in a config file.
    if cfg.tls_insecure && endpoint.is_tls() {
        tracing::warn!(
            bridge = %cfg.name,
            address = %cfg.address,
            "tls_insecure is set: the remote broker's certificate is NOT verified. \
             This bridge is exposed to man-in-the-middle attacks and must not be \
             used in production"
        );
    }

    let base = Duration::from_secs(1);
    let max = Duration::from_secs(60);
    let mut delay = base;
    loop {
        match connect_and_serve(&broker, &cfg, &endpoint, &bridge_client, &mut out_rx).await {
            Ok(true) => {
                // Was connected and then disconnected — retry promptly.
                tracing::info!("bridge {:?} disconnected", cfg.name);
                delay = base;
            }
            Ok(false) => {
                tracing::warn!("bridge {:?} was rejected by the remote", cfg.name);
                delay = (delay * 2).min(max);
            }
            Err(e) => {
                tracing::warn!("bridge {:?}: {e}", cfg.name);
                delay = (delay * 2).min(max);
            }
        }
        tokio::time::sleep(delay).await;
    }
}

/// Parsed remote endpoint.
struct Endpoint {
    transport: Transport,
    host: String,
    port: u16,
    /// Full URL for the WebSocket handshake (ws/wss only).
    ws_url: Option<String>,
}

#[derive(Clone, Copy)]
enum Transport {
    Tcp,
    Tls,
    Ws,
    Wss,
}

impl Endpoint {
    /// Whether this endpoint negotiates TLS (and so is affected by
    /// `tls_insecure`).
    fn is_tls(&self) -> bool {
        matches!(self.transport, Transport::Tls | Transport::Wss)
    }

    fn parse(address: &str) -> Result<Endpoint> {
        let (scheme, rest) = address.split_once("://").ok_or_else(|| {
            cfg(format!(
                "bridge address {address:?} must be scheme://host[:port]"
            ))
        })?;
        let (transport, default_port) = match scheme.to_ascii_lowercase().as_str() {
            "tcp" | "mqtt" => (Transport::Tcp, 1883),
            "tls" | "mqtts" | "ssl" => (Transport::Tls, 8883),
            "ws" => (Transport::Ws, 8080),
            "wss" => (Transport::Wss, 8443),
            other => return Err(cfg(format!("bridge address: unknown scheme {other:?}"))),
        };
        let (hostport, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse::<u16>()
                    .map_err(|_| cfg(format!("bridge address: invalid port {p:?}")))?,
            ),
            None => (hostport.to_string(), default_port),
        };
        if host.is_empty() {
            return Err(cfg("bridge address: empty host".into()));
        }
        let ws_url = matches!(transport, Transport::Ws | Transport::Wss).then(|| {
            let path = if path.is_empty() { "/mqtt" } else { path };
            format!("{scheme}://{host}:{port}{path}")
        });
        Ok(Endpoint {
            transport,
            host,
            port,
            ws_url,
        })
    }
}

/// Establish the transport and serve one connection. Returns `Ok(true)` once it
/// was connected (CONNACK ok) and later disconnected, `Ok(false)` if rejected,
/// or `Err` on a connect/IO error.
async fn connect_and_serve(
    broker: &Broker,
    cfg: &BridgeConfig,
    ep: &Endpoint,
    bridge_client: &str,
    out_rx: &mut Receiver<Outgoing>,
) -> Result<bool> {
    let tcp = TcpStream::connect((ep.host.as_str(), ep.port)).await?;
    tcp.set_nodelay(true).ok();
    match ep.transport {
        Transport::Tcp => serve(tcp, broker, cfg, bridge_client, out_rx).await,
        Transport::Tls => {
            let tls = tls_connect(cfg, ep, tcp).await?;
            serve(tls, broker, cfg, bridge_client, out_rx).await
        }
        Transport::Ws => {
            let ws = crate::ws::client(
                tcp,
                ep.ws_url.as_deref().unwrap(),
                broker.config().max_packet_size,
            )
            .await?;
            serve(ws, broker, cfg, bridge_client, out_rx).await
        }
        Transport::Wss => {
            let tls = tls_connect(cfg, ep, tcp).await?;
            let ws = crate::ws::client(
                tls,
                ep.ws_url.as_deref().unwrap(),
                broker.config().max_packet_size,
            )
            .await?;
            serve(ws, broker, cfg, bridge_client, out_rx).await
        }
    }
}

async fn tls_connect<IO>(
    cfg: &BridgeConfig,
    ep: &Endpoint,
    io: IO,
) -> Result<tokio_rustls::client::TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let config = crate::tls::client_config(
        cfg.tls_ca.as_deref(),
        cfg.tls_cert.as_deref(),
        cfg.tls_key.as_deref(),
        cfg.tls_insecure,
    )?;
    let connector = TlsConnector::from(config);
    let server_name = ServerName::try_from(ep.host.clone())
        .map_err(|_| self::cfg(format!("bridge: invalid TLS server name {:?}", ep.host)))?;
    Ok(connector.connect(server_name, io).await?)
}

/// CONNECT + CONNACK + SUBSCRIBE, then run the forwarding loop.
async fn serve<S>(
    stream: S,
    broker: &Broker,
    cfg: &BridgeConfig,
    bridge_client: &str,
    out_rx: &mut Receiver<Outgoing>,
) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let version = cfg.protocol_version;
    let max = broker.config().max_packet_size;

    let metrics = broker.metrics();
    send(
        &mut wr,
        &Packet::Connect(build_connect(cfg)),
        version,
        metrics,
    )
    .await?;
    match read_packet(&mut rd, max, version).await? {
        ReadOutcome::Packet(pkt, n) => {
            record_received(metrics, &pkt, n);
            match pkt {
                Packet::Connack(c) => {
                    if c.reason_code.is_error() {
                        tracing::warn!("bridge {:?}: remote CONNACK {:?}", cfg.name, c.reason_code);
                        return Ok(false);
                    }
                }
                other => {
                    return Err(MqttError::Protocol(format!(
                        "bridge {:?}: expected CONNACK, got {}",
                        cfg.name,
                        other.name()
                    )));
                }
            }
        }
        ReadOutcome::Eof => return Ok(false),
    }
    tracing::info!("bridge {:?} connected to {}", cfg.name, cfg.address);

    // SUBSCRIBE to inbound topics (SUBACK is consumed and ignored by the loop).
    let in_filters: Vec<TopicFilter> = cfg
        .topics
        .iter()
        .filter(|t| t.direction.forwards_in())
        .map(|t| TopicFilter {
            filter: t.pattern.clone(),
            qos: t.qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        })
        .collect();
    if !in_filters.is_empty() {
        let sub = Subscribe {
            packet_id: 1,
            properties: Properties::new(),
            filters: in_filters,
        };
        send(&mut wr, &Packet::Subscribe(sub), version, metrics).await?;
    }

    Metrics::inc(&broker.metrics().bridges_connected);
    let outcome = run_loop(
        &mut rd,
        &mut wr,
        broker,
        cfg,
        bridge_client,
        out_rx,
        version,
        max,
    )
    .await;
    Metrics::dec(&broker.metrics().bridges_connected);
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn run_loop<R, W>(
    rd: &mut R,
    wr: &mut W,
    broker: &Broker,
    cfg: &BridgeConfig,
    bridge_client: &str,
    out_rx: &mut Receiver<Outgoing>,
    version: ProtocolVersion,
    max: u32,
) -> Result<bool>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut inbound_qos2: HashSet<u16> = HashSet::new();
    let mut next_id: u16 = 2; // 1 was used by SUBSCRIBE
    let ka = cfg.keepalive as u64;
    let mut ping = tokio::time::interval(Duration::from_secs(if ka > 0 { ka } else { 3600 }));
    let mut last_rx = Instant::now();

    loop {
        tokio::select! {
            maybe = out_rx.recv() => {
                let Some(out) = maybe else { return Ok(true); };
                match out {
                    Outgoing::Send(publish) => {
                        let qos = out_qos(cfg, &publish.topic);
                        let packet_id = if qos == QoS::AtMostOnce {
                            None
                        } else {
                            let id = next_id;
                            next_id = if id == u16::MAX { 1 } else { id + 1 };
                            Some(id)
                        };
                        let fwd = Publish {
                            dup: false,
                            qos,
                            retain: publish.retain,
                            topic: publish.topic.clone(),
                            packet_id,
                            properties: publish.properties.clone(),
                            payload: publish.payload.clone(),
                        };
                        send(wr, &Packet::Publish(fwd), version, broker.metrics()).await?;
                        Metrics::inc(&broker.metrics().bridge_forwarded_out);
                    }
                    // Acks/control targeted at the local session are irrelevant
                    // to the bridge; a Shutdown means the session was taken over.
                    Outgoing::Control(_) => {}
                    Outgoing::Shutdown(_) => return Ok(true),
                }
            }

            res = read_packet(rd, max, version) => {
                last_rx = Instant::now();
                match res? {
                    ReadOutcome::Eof => return Ok(true),
                    ReadOutcome::Packet(pkt, n) => {
                    record_received(broker.metrics(), &pkt, n);
                    match pkt {
                        Packet::Publish(p) => {
                            handle_inbound(broker, bridge_client, p, wr, version, &mut inbound_qos2).await?;
                        }
                        // Our outbound QoS 2 forward: remote acknowledged receipt.
                        Packet::Pubrec(a) => {
                            send(wr, &Packet::Pubrel(PubAck::new(a.packet_id, crate::types::ReasonCode::Success)), version, broker.metrics()).await?;
                        }
                        // Inbound QoS 2 completion from the remote.
                        Packet::Pubrel(a) => {
                            inbound_qos2.remove(&a.packet_id);
                            send(wr, &Packet::Pubcomp(PubAck::new(a.packet_id, crate::types::ReasonCode::Success)), version, broker.metrics()).await?;
                        }
                        Packet::Disconnect(_) => return Ok(true),
                        // PUBACK / PUBCOMP / SUBACK / PINGRESP: nothing to do.
                        _ => {}
                    }
                    }
                }
            }

            _ = ping.tick() => {
                if ka > 0 {
                    if last_rx.elapsed() > Duration::from_secs(ka * 2) {
                        return Err(MqttError::Protocol("bridge remote keep-alive timeout".into()));
                    }
                    send(wr, &Packet::Pingreq, version, broker.metrics()).await?;
                }
            }
        }
    }
}

/// Write a packet to the remote and record it in the shared counters.
///
/// Mirrors `server::send`. The bridge used to call `write_packet` directly, so
/// everything it exchanged with its remote was invisible to `packets_sent`,
/// `bytes_sent` and the `mqtt_packet_*_sent_total` series — an edge broker whose
/// only job is forwarding reported almost no traffic. Route every write through
/// here so the remote link is counted like any other connection.
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

/// Record a packet received from the remote, mirroring the server's read arm.
/// The counterpart to [`send`]: inbound bridge traffic was equally uncounted.
fn record_received(metrics: &Metrics, packet: &Packet, frame_bytes: usize) {
    metrics.record_received(packet.packet_type(), frame_bytes);
    if let Packet::Publish(p) = packet {
        Metrics::add(&metrics.publish_bytes_received, p.payload.len() as u64);
    }
}

/// Handle a PUBLISH received from the remote: inject once into local routing and
/// acknowledge per QoS.
async fn handle_inbound<W>(
    broker: &Broker,
    from: &str,
    p: Publish,
    wr: &mut W,
    version: ProtocolVersion,
    inbound_qos2: &mut HashSet<u16>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let id = p.packet_id.unwrap_or(0);
    let duplicate = p.qos == QoS::ExactlyOnce && inbound_qos2.contains(&id);
    if !duplicate {
        if p.qos == QoS::ExactlyOnce {
            inbound_qos2.insert(id);
        }
        broker.inject_publish(from, Message::from_publish(&p));
        Metrics::inc(&broker.metrics().bridge_forwarded_in);
    }
    match p.qos {
        QoS::AtMostOnce => {}
        QoS::AtLeastOnce => {
            send(
                wr,
                &Packet::Puback(PubAck::new(id, crate::types::ReasonCode::Success)),
                version,
                broker.metrics(),
            )
            .await?;
        }
        QoS::ExactlyOnce => {
            send(
                wr,
                &Packet::Pubrec(PubAck::new(id, crate::types::ReasonCode::Success)),
                version,
                broker.metrics(),
            )
            .await?;
        }
    }
    Ok(())
}

fn build_connect(cfg: &BridgeConfig) -> Connect {
    let protocol_name = if cfg.protocol_version == ProtocolVersion::V3_1 {
        "MQIsdp"
    } else {
        "MQTT"
    };
    Connect {
        protocol_name: protocol_name.to_string(),
        protocol_version: cfg.protocol_version.level(),
        clean_start: true,
        keep_alive: cfg.keepalive,
        properties: Properties::new(),
        client_id: cfg.client_id.clone(),
        will: None,
        username: cfg.username.clone(),
        password: cfg.password.as_ref().map(|p| p.as_bytes().to_vec()),
    }
}

/// The QoS to forward a local message out at: the first matching out-mapping.
fn out_qos(cfg: &BridgeConfig, topic: &str) -> QoS {
    cfg.topics
        .iter()
        .find(|t| t.direction.forwards_out() && topic::matches(&t.pattern, topic))
        .map(|t| t.qos)
        .unwrap_or(QoS::AtMostOnce)
}
