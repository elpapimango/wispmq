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

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::codec::Properties;
use pulsemq::config::Config;
use pulsemq::packet::{Connect, Packet, Publish, RetainHandling, Subscribe, TopicFilter};
use pulsemq::storage::Storage;
use pulsemq::types::{ProtocolVersion, QoS, ReasonCode};

mod common;
use common::free_addr;

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
        payload: payload.into(),
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
    start_ws_broker_with_acl(config, Acl::permit_all()).await
}

async fn start_ws_broker_with_acl(config: Config, acl: Acl) -> Broker {
    let broker = Broker::new(config, Storage::null(), Default::default(), acl, None);
    let b = broker.clone();
    tokio::spawn(async move {
        let _ = pulsemq::server::run_ws(b).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    broker
}

/// Generate a CA plus a server certificate (SAN `localhost`) and a client
/// certificate (CN = `client_cn`) signed by it, for mutual-TLS tests. Returns
/// PEM file paths: `(ca, server_cert, server_key, client_cert, client_key)`.
fn write_mtls_certs(
    dir: &std::path::Path,
    client_cn: &str,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "pulsemq test CA");
    ca_params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);

    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params
        .distinguished_name
        .push(DnType::CommonName, client_cn);
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

    let ca_path = dir.join("ca.pem");
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server.key");
    let client_cert_path = dir.join("client.pem");
    let client_key_path = dir.join("client.key");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();

    (
        ca_path,
        server_cert_path,
        server_key_path,
        client_cert_path,
        client_key_path,
    )
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
            assert_eq!(&p.payload[..], b"over-websockets");
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

#[tokio::test]
async fn mqtt_over_websocket_with_mutual_tls_enforces_acl_by_cert_cn() {
    let dir = std::env::temp_dir().join(format!("mqtt-ws-mtls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca_path, server_cert_path, server_key_path, client_cert_path, client_key_path) =
        write_mtls_certs(&dir, "ws-mtls-client");

    let addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(addr),
        ws_tls_cert: Some(server_cert_path.to_string_lossy().into_owned()),
        ws_tls_key: Some(server_key_path.to_string_lossy().into_owned()),
        ws_tls_client_ca: Some(ca_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    // Only the "ws-mtls-client" identity (the client cert's CN) may use
    // `mtls/allowed`; everything else is denied by default.
    let acl = Acl::from_value(&serde_json::json!({
        "default": "deny",
        "rules": [
            { "identity": "ws-mtls-client", "publish": ["mtls/allowed"], "subscribe": ["mtls/allowed"] }
        ]
    }))
    .unwrap();
    let _broker = start_ws_broker_with_acl(config, acl).await;

    let ca_path = ca_path.to_string_lossy().into_owned();
    let client_cert_path = client_cert_path.to_string_lossy().into_owned();
    let client_key_path = client_key_path.to_string_lossy().into_owned();
    let tls_config = pulsemq::tls::client_config(
        Some(&ca_path),
        Some(&client_cert_path),
        Some(&client_key_path),
        false,
    )
    .unwrap();
    let connector = tokio_rustls::TlsConnector::from(tls_config);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();

    // Two connections, both authenticating as the same cert CN
    // ("ws-mtls-client") — kept separate so publisher/subscriber ordering
    // doesn't depend on a single connection racing its own self-delivery.
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

    send(
        &mut sub,
        &connect_packet("mtls-sub", ProtocolVersion::V5),
        ProtocolVersion::V5,
    )
    .await;
    match recv(&mut sub, ProtocolVersion::V5).await {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }
    send(
        &mut publisher,
        &connect_packet("mtls-pub", ProtocolVersion::V5),
        ProtocolVersion::V5,
    )
    .await;
    match recv(&mut publisher, ProtocolVersion::V5).await {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }

    // Granted: subscribe + publish on the ACL-allowed topic round-trips.
    send(
        &mut sub,
        &subscribe_packet("mtls/allowed"),
        ProtocolVersion::V5,
    )
    .await;
    match recv(&mut sub, ProtocolVersion::V5).await {
        Packet::Suback(s) => assert_eq!(s.reason_codes, vec![ReasonCode::GrantedQoS1]),
        other => panic!("expected SUBACK, got {}", other.name()),
    }
    send(
        &mut publisher,
        &publish_packet("mtls/allowed", b"granted"),
        ProtocolVersion::V5,
    )
    .await;
    match recv(&mut publisher, ProtocolVersion::V5).await {
        Packet::Puback(a) => assert_eq!(a.reason_code, ReasonCode::Success),
        other => panic!("expected PUBACK, got {}", other.name()),
    }
    match recv(&mut sub, ProtocolVersion::V5).await {
        Packet::Publish(p) => assert_eq!(&p.payload[..], b"granted"),
        other => panic!("expected forwarded PUBLISH, got {}", other.name()),
    }

    // Denied: the same identity, a topic the ACL doesn't grant — rejected,
    // not routed.
    send(
        &mut publisher,
        &publish_packet("mtls/denied", b"nope"),
        ProtocolVersion::V5,
    )
    .await;
    match recv(&mut publisher, ProtocolVersion::V5).await {
        Packet::Puback(a) => assert_eq!(a.reason_code, ReasonCode::NotAuthorized),
        other => panic!("expected PUBACK, got {}", other.name()),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mqtt_over_websocket_without_client_cert_is_rejected_by_mutual_tls() {
    let dir = std::env::temp_dir().join(format!("mqtt-ws-mtls-reject-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca_path, server_cert_path, server_key_path, _client_cert_path, _client_key_path) =
        write_mtls_certs(&dir, "irrelevant");

    let addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(addr),
        ws_tls_cert: Some(server_cert_path.to_string_lossy().into_owned()),
        ws_tls_key: Some(server_key_path.to_string_lossy().into_owned()),
        ws_tls_client_ca: Some(ca_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let _broker = start_ws_broker(config).await;

    // Trusts the CA (so the server cert verifies) but presents no client
    // certificate — the server requires one. TLS 1.3's client-side `connect`
    // completes as soon as the client sends its own (empty) Finished, before
    // it learns the server rejected the handshake for lacking a client cert
    // — so the rejection is only observable once the connection is actually
    // used (here, the WebSocket upgrade), not at `connect` itself.
    let ca_path = ca_path.to_string_lossy().into_owned();
    let tls_config = pulsemq::tls::client_config(Some(&ca_path), None, None, false).unwrap();
    let connector = tokio_rustls::TlsConnector::from(tls_config);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    let ws_result = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_tungstenite::client_async(mqtt_request("wss://localhost/mqtt"), tls),
    )
    .await
    .expect("no response after the server should have rejected the missing client certificate");
    assert!(
        ws_result.is_err(),
        "WebSocket handshake should fail: mutual TLS requires a client certificate"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
