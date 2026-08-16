//! End-to-end tests for MQTT over WebSockets (plain `ws://`) and over
//! WebSockets with TLS (`wss://`). The client speaks the WebSocket protocol via
//! tokio-tungstenite and MQTT via the crate's own codec, exchanging one MQTT
//! packet per binary frame.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use mqtt_server::broker::Broker;
use mqtt_server::codec::Properties;
use mqtt_server::config::Config;
use mqtt_server::packet::{Connect, Packet, Publish, RetainHandling, Subscribe, TopicFilter};
use mqtt_server::storage::Storage;
use mqtt_server::types::{ProtocolVersion, QoS, ReasonCode};

/// Reserve an ephemeral loopback port and return its address.
fn free_addr() -> std::net::SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

fn connect_packet(client_id: &str, version: ProtocolVersion) -> Packet {
    let protocol_name = if version == ProtocolVersion::V3_1 {
        "MQIsdp"
    } else {
        "MQTT"
    };
    Packet::Connect(Connect {
        protocol_name: protocol_name.into(),
        protocol_version: version.level(),
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: client_id.into(),
        will: None,
        username: None,
        password: None,
    })
}

fn subscribe_packet(filter: &str) -> Packet {
    Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: filter.into(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        }],
    })
}

fn publish_packet(topic: &str, payload: &[u8]) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: topic.into(),
        packet_id: Some(7),
        properties: Properties::new(),
        payload: payload.to_vec(),
    })
}

/// Send one MQTT packet as a single WebSocket binary frame.
async fn send<S>(ws: &mut WebSocketStream<S>, packet: &Packet, version: ProtocolVersion)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(Message::binary(packet.encode(version).unwrap()))
        .await
        .unwrap();
}

/// Receive the next MQTT packet, decoding from a binary frame.
async fn recv<S>(ws: &mut WebSocketStream<S>, version: ProtocolVersion) -> Packet
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("timed out awaiting frame")
            .expect("stream ended")
            .expect("websocket error");
        if msg.is_binary() {
            return Packet::decode(msg.into_data().as_ref(), version).unwrap();
        }
    }
}

/// Build a client WebSocket upgrade request offering the `mqtt` subprotocol.
fn mqtt_request(url: &str) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
    req
}

async fn start_ws_broker(config: Config) -> Broker {
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        mqtt_server::acl::Acl::permit_all(),
        None,
    );
    let b = broker.clone();
    tokio::spawn(async move {
        let _ = mqtt_server::server::run_ws(b).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    broker
}

/// Run a full subscribe → publish → deliver round trip over an established
/// WebSocket MQTT connection pair.
async fn round_trip<S>(
    sub: &mut WebSocketStream<S>,
    publisher: &mut WebSocketStream<S>,
    version: ProtocolVersion,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Subscriber.
    send(sub, &connect_packet("ws-sub", version), version).await;
    match recv(sub, version).await {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }
    send(sub, &subscribe_packet("ws/#"), version).await;
    match recv(sub, version).await {
        Packet::Suback(s) => assert_eq!(s.reason_codes, vec![ReasonCode::GrantedQoS1]),
        other => panic!("expected SUBACK, got {}", other.name()),
    }

    // Publisher.
    send(publisher, &connect_packet("ws-pub", version), version).await;
    match recv(publisher, version).await {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }
    send(
        publisher,
        &publish_packet("ws/hello", b"over-websockets"),
        version,
    )
    .await;
    match recv(publisher, version).await {
        Packet::Puback(a) => assert_eq!(a.packet_id, 7),
        other => panic!("expected PUBACK, got {}", other.name()),
    }

    // Delivery to the subscriber.
    match recv(sub, version).await {
        Packet::Publish(p) => {
            assert_eq!(p.topic, "ws/hello");
            assert_eq!(p.payload, b"over-websockets");
        }
        other => panic!("expected forwarded PUBLISH, got {}", other.name()),
    }
}

#[tokio::test]
async fn mqtt_over_plain_websocket() {
    let addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(addr),
        ..Config::default()
    };
    let _broker = start_ws_broker(config).await;

    let url = format!("ws://{addr}/mqtt");
    let (mut sub, resp) = tokio_tungstenite::connect_async(mqtt_request(&url))
        .await
        .unwrap();
    // Server must select the `mqtt` subprotocol [MQTT-6.0.0-4].
    assert_eq!(
        resp.headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok()),
        Some("mqtt")
    );
    let (mut publisher, _) = tokio_tungstenite::connect_async(mqtt_request(&url))
        .await
        .unwrap();

    round_trip(&mut sub, &mut publisher, ProtocolVersion::V5).await;
}

#[tokio::test]
async fn mqtt_v311_over_plain_websocket() {
    // MQTT v3.1.1 (protocol level 4) over WebSockets: no properties on the wire.
    let addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(addr),
        ..Config::default()
    };
    let _broker = start_ws_broker(config).await;

    let url = format!("ws://{addr}/mqtt");
    let (mut sub, _) = tokio_tungstenite::connect_async(mqtt_request(&url))
        .await
        .unwrap();
    let (mut publisher, _) = tokio_tungstenite::connect_async(mqtt_request(&url))
        .await
        .unwrap();

    round_trip(&mut sub, &mut publisher, ProtocolVersion::V3_1_1).await;
}

#[tokio::test]
async fn mqtt_over_websocket_with_tls() {
    // Generate a self-signed cert for `localhost` and write it to temp files
    // for the broker to load.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = std::env::temp_dir().join(format!("mqtt-ws-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();

    let addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(addr),
        ws_tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        ws_tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let _broker = start_ws_broker(config).await;

    // Client TLS trusting exactly the generated certificate.
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let tls_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();

    // Two independent TLS+WS connections.
    let tcp1 = TcpStream::connect(addr).await.unwrap();
    let tls1 = connector.connect(server_name.clone(), tcp1).await.unwrap();
    let (mut sub, _) = tokio_tungstenite::client_async(mqtt_request("wss://localhost/mqtt"), tls1)
        .await
        .unwrap();

    let tcp2 = TcpStream::connect(addr).await.unwrap();
    let tls2 = connector.connect(server_name, tcp2).await.unwrap();
    let (mut publisher, _) =
        tokio_tungstenite::client_async(mqtt_request("wss://localhost/mqtt"), tls2)
            .await
            .unwrap();

    round_trip(&mut sub, &mut publisher, ProtocolVersion::V5).await;

    let _ = std::fs::remove_dir_all(&dir);
}
