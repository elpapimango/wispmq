//! Proves that a plain listener and its dedicated TLS listener run at the
//! same time, independently, for both the MQTT port (`listen_addr` +
//! `tls_listen_addr`) and the WebSocket port (`ws_listen_addr` +
//! `ws_tls_listen_addr`). This is the point of the split: a single broker
//! process serving Normal MQTT, MQTT over WebSocket, Normal MQTT over TLS,
//! and MQTT over WebSocket over TLS all at once, matching how the Home
//! Assistant Mosquitto add-on presents four independent ports.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;

use wispmq::broker::Broker;
use wispmq::codec::Properties;
use wispmq::config::Config;
use wispmq::framing::{read_packet, write_packet, ReadOutcome};
use wispmq::packet::{Connect, Packet};
use wispmq::storage::Storage;
use wispmq::types::{ProtocolVersion::V5, ReasonCode};

mod common;
use common::free_addr;

fn connect_packet(client_id: &str) -> Packet {
    Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: client_id.into(),
        will: None,
        username: None,
        password: None,
    })
}

/// Send CONNECT and expect a successful CONNACK over an already-established
/// transport, proving the broker is actually alive and accepting sessions on
/// it (not just that the TCP/TLS handshake succeeded).
async fn assert_connects<S>(mut stream: S, client_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    write_packet(&mut stream, &connect_packet(client_id), V5)
        .await
        .unwrap();
    match read_packet(&mut stream, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Connack(c), _) => {
            assert_eq!(c.reason_code, ReasonCode::Success);
        }
        _ => panic!("expected CONNACK"),
    }
}

fn generate_cert(
    dir: &std::path::Path,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    rcgen::CertifiedKey<rcgen::KeyPair>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    (cert_path, key_path, cert)
}

fn tls_connector(cert: &rcgen::CertifiedKey<rcgen::KeyPair>) -> tokio_rustls::TlsConnector {
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let tls_config = tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(tls_config))
}

#[tokio::test]
async fn plain_and_tls_mqtt_listeners_serve_simultaneously() {
    let dir = std::env::temp_dir().join(format!("mqtt-dual-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_path, key_path, cert) = generate_cert(&dir);

    let plain_addr = free_addr();
    let tls_addr = free_addr();
    let config = Config {
        listen_addr: plain_addr,
        tls_listen_addr: Some(tls_addr),
        tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        wispmq::acl::Acl::permit_all(),
        None,
    );
    let b1 = broker.clone();
    tokio::spawn(async move {
        let _ = wispmq::server::run(b1).await;
    });
    let b2 = broker.clone();
    tokio::spawn(async move {
        let _ = wispmq::server::run_tls(b2).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Plain client on listen_addr — no TLS involved at all.
    let plain = TcpStream::connect(plain_addr).await.unwrap();
    assert_connects(plain, "plain-client").await;

    // TLS client on the independent tls_listen_addr, at the same time.
    let connector = tls_connector(&cert);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tcp = TcpStream::connect(tls_addr).await.unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    assert_connects(tls, "tls-client").await;

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn plain_and_tls_websocket_listeners_serve_simultaneously() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    fn mqtt_request(url: &str) -> tokio_tungstenite::tungstenite::handshake::client::Request {
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("Sec-WebSocket-Protocol", HeaderValue::from_static("mqtt"));
        req
    }

    let dir = std::env::temp_dir().join(format!("ws-dual-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_path, key_path, cert) = generate_cert(&dir);

    let plain_addr = free_addr();
    let tls_addr = free_addr();
    let config = Config {
        ws_listen_addr: Some(plain_addr),
        ws_tls_listen_addr: Some(tls_addr),
        ws_tls_cert: Some(cert_path.to_string_lossy().into_owned()),
        ws_tls_key: Some(key_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        wispmq::acl::Acl::permit_all(),
        None,
    );
    let b1 = broker.clone();
    tokio::spawn(async move {
        let _ = wispmq::server::run_ws(b1).await;
    });
    let b2 = broker.clone();
    tokio::spawn(async move {
        let _ = wispmq::server::run_ws_tls(b2).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Plain ws:// client.
    let (plain_ws, _) =
        tokio_tungstenite::connect_async(mqtt_request(&format!("ws://{plain_addr}/mqtt")))
            .await
            .unwrap();
    let (mut plain_ws_write, mut plain_ws_read) = futures_util::StreamExt::split(plain_ws);
    futures_util::SinkExt::send(
        &mut plain_ws_write,
        tokio_tungstenite::tungstenite::Message::binary(
            connect_packet("plain-ws-client").encode(V5).unwrap(),
        ),
    )
    .await
    .unwrap();
    let msg = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::StreamExt::next(&mut plain_ws_read),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    match Packet::decode(msg.into_data().as_ref(), V5).unwrap() {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }

    // wss:// client on the independent ws_tls_listen_addr, at the same time.
    let connector = tls_connector(&cert);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let tcp = TcpStream::connect(tls_addr).await.unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    let (tls_ws, _) = tokio_tungstenite::client_async(mqtt_request("wss://localhost/mqtt"), tls)
        .await
        .unwrap();
    let (mut tls_ws_write, mut tls_ws_read) = futures_util::StreamExt::split(tls_ws);
    futures_util::SinkExt::send(
        &mut tls_ws_write,
        tokio_tungstenite::tungstenite::Message::binary(
            connect_packet("tls-ws-client").encode(V5).unwrap(),
        ),
    )
    .await
    .unwrap();
    let msg = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::StreamExt::next(&mut tls_ws_read),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    match Packet::decode(msg.into_data().as_ref(), V5).unwrap() {
        Packet::Connack(c) => assert_eq!(c.reason_code, ReasonCode::Success),
        other => panic!("expected CONNACK, got {}", other.name()),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
