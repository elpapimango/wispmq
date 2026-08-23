//! Mutual-TLS coverage for the admin HTTP server (`/health`, `/metrics`,
//! `/mcp`): a client presenting a certificate signed by the configured CA is
//! served normally, and one presenting none is rejected.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::config::Config;
use pulsemq::storage::Storage;

/// Reserve an ephemeral loopback port and return its address.
fn free_addr() -> std::net::SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

/// Generate a CA plus a server certificate (SAN `localhost`) and a client
/// certificate signed by it, for mutual-TLS tests. Returns PEM file paths:
/// `(ca, server_cert, server_key, client_cert, client_key)`.
fn write_mtls_certs(
    dir: &std::path::Path,
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
        .push(DnType::CommonName, "admin-mtls-client");
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

async fn start_admin(config: Config) -> Broker {
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    );
    let b = broker.clone();
    tokio::spawn(async move {
        let _ = pulsemq::admin::run(b).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    broker
}

#[tokio::test]
async fn admin_mutual_tls_serves_a_client_with_a_valid_certificate() {
    let dir = std::env::temp_dir().join(format!("mqtt-admin-mtls-ok-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca_path, server_cert_path, server_key_path, client_cert_path, client_key_path) =
        write_mtls_certs(&dir);

    let addr = free_addr();
    let config = Config {
        admin_addr: addr,
        admin_tls_cert: Some(server_cert_path.to_string_lossy().into_owned()),
        admin_tls_key: Some(server_key_path.to_string_lossy().into_owned()),
        admin_tls_client_ca: Some(ca_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let _broker = start_admin(config).await;

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

    let tcp = TcpStream::connect(addr).await.unwrap();
    let mut tls = connector.connect(server_name, tcp).await.unwrap();
    tls.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    // The admin server closes the raw TCP stream after `Connection: close`
    // rather than a graceful TLS close_notify, so read until EOF *or* that
    // (expected) unclean shutdown, not just EOF.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tls.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => panic!("unexpected read error: {e}"),
        }
    }
    let response = String::from_utf8_lossy(&buf);
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "expected 200 OK, got: {response}"
    );
    assert!(response.ends_with("{\"status\":\"ok\"}"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn admin_mutual_tls_rejects_a_client_without_a_certificate() {
    let dir = std::env::temp_dir().join(format!("mqtt-admin-mtls-reject-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca_path, server_cert_path, server_key_path, _client_cert_path, _client_key_path) =
        write_mtls_certs(&dir);

    let addr = free_addr();
    let config = Config {
        admin_addr: addr,
        admin_tls_cert: Some(server_cert_path.to_string_lossy().into_owned()),
        admin_tls_key: Some(server_key_path.to_string_lossy().into_owned()),
        admin_tls_client_ca: Some(ca_path.to_string_lossy().into_owned()),
        ..Config::default()
    };
    let _broker = start_admin(config).await;

    // Trusts the CA (so the server cert verifies) but presents no client
    // certificate. TLS 1.3's client-side `connect` can still complete (it
    // finishes as soon as the client sends its own empty Finished, before
    // learning the server rejected the missing certificate) — the rejection
    // is only observable once the connection is actually used.
    let ca_path = ca_path.to_string_lossy().into_owned();
    let tls_config = pulsemq::tls::client_config(Some(&ca_path), None, None, false).unwrap();
    let connector = tokio_rustls::TlsConnector::from(tls_config);
    let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let tcp = TcpStream::connect(addr).await.unwrap();
    let mut tls = connector.connect(server_name, tcp).await.unwrap();
    let write_result = tls
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await;
    let mut buf = Vec::new();
    let read_result = tls.read_to_end(&mut buf).await;
    let rejected = write_result.is_err() || read_result.is_err() || buf.is_empty();
    assert!(
        rejected,
        "admin endpoint should reject a connection with no client certificate; got: {:?}",
        String::from_utf8_lossy(&buf)
    );

    let _ = std::fs::remove_dir_all(&dir);
}
