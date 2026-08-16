//! Native TLS support via rustls (pure Rust, no OpenSSL at runtime).
//!
//! Builds a `TlsAcceptor` from PEM-encoded certificate and private-key files.
//! Used to optionally wrap both the MQTT listener and the admin HTTP listener.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::error::{MqttError, Result};

fn cfg_err(msg: impl Into<String>) -> MqttError {
    MqttError::Config(msg.into())
}

/// Build a `TlsAcceptor` for the given PEM certificate chain and private key.
///
/// When `client_ca` is `Some`, mutual TLS is enforced: clients MUST present a
/// certificate that chains to a CA in that trust store, or the handshake fails.
pub fn build_acceptor(
    cert_path: &str,
    key_path: &str,
    client_ca: Option<&str>,
) -> Result<TlsAcceptor> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    // Pin the ring crypto provider explicitly so we never depend on a
    // process-wide default being installed.
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| cfg_err(format!("tls provider: {e}")))?;

    // Either require & verify client certs (mTLS) or accept anonymous clients.
    let builder = match client_ca {
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            for cert in load_certs(ca_path)? {
                roots
                    .add(cert)
                    .map_err(|e| cfg_err(format!("client CA {ca_path}: {e}")))?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|e| cfg_err(format!("client verifier: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    let config = builder
        .with_single_cert(certs, key)
        .map_err(|e| cfg_err(format!("tls certificate/key mismatch: {e}")))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build an acceptor only when both cert and key are configured. Returns
/// `Ok(None)` when neither is set (plaintext), or an error when exactly one is
/// (or when a client CA is given without an enabling server certificate).
pub fn maybe_acceptor(
    cert: &Option<String>,
    key: &Option<String>,
    client_ca: &Option<String>,
    label: &str,
) -> Result<Option<TlsAcceptor>> {
    match (cert, key) {
        (Some(c), Some(k)) => Ok(Some(build_acceptor(c, k, client_ca.as_deref())?)),
        (None, None) => {
            if client_ca.is_some() {
                return Err(cfg_err(format!(
                    "{label}: a client CA was set but TLS is not enabled (needs a certificate and key)"
                )));
            }
            Ok(None)
        }
        _ => Err(cfg_err(format!(
            "{label}: both a certificate and a private key are required to enable TLS"
        ))),
    }
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| cfg_err(format!("open cert {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    let certs: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs.map_err(|e| cfg_err(format!("parse cert {path}: {e}")))?;
    if certs.is_empty() {
        return Err(cfg_err(format!("no certificates found in {path}")));
    }
    Ok(certs)
}

/// Extract the subject Common Name from the peer's leaf certificate, if any.
/// Used to derive an authenticated identity from a mutual-TLS client cert.
pub fn peer_cn(peer_certificates: Option<&[CertificateDer<'_>]>) -> Option<String> {
    let leaf = peer_certificates?.first()?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref()).ok()?;
    // Bind to an owned value in a statement so the borrowing iterator's
    // temporary is dropped before `cert` at the end of the function.
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string());
    cn
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|e| cfg_err(format!("open key {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| cfg_err(format!("parse key {path}: {e}")))?
        .ok_or_else(|| cfg_err(format!("no private key found in {path}")))
}
