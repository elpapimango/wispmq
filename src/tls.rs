//! Native TLS support via rustls (pure Rust, no OpenSSL at runtime).
//!
//! Builds a `TlsAcceptor` from PEM-encoded certificate and private-key files.
//! Used to optionally wrap both the MQTT listener and the admin HTTP listener.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::error::{MqttError, Result};

fn cfg_err(msg: impl Into<String>) -> MqttError {
    MqttError::Config(msg.into())
}

/// Build a rustls `ClientConfig` for connecting *out* to a TLS server (used by
/// the bridge). Server certificates are verified against `ca_path` (a PEM CA
/// bundle). When `client_cert`/`client_key` are given, they are presented for
/// mutual TLS. `insecure` disables server-certificate verification (danger —
/// for testing only).
pub fn client_config(
    ca_path: Option<&str>,
    client_cert: Option<&str>,
    client_key: Option<&str>,
    insecure: bool,
) -> Result<Arc<ClientConfig>> {
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| cfg_err(format!("tls provider: {e}")))?;

    // Server trust: a CA bundle, or (danger) accept anything.
    let builder = if insecure {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(danger::NoVerify(provider)))
    } else {
        let ca =
            ca_path.ok_or_else(|| cfg_err("a TLS bridge requires tls_ca (or tls_insecure)"))?;
        let mut roots = RootCertStore::empty();
        for cert in load_certs(ca)? {
            roots
                .add(cert)
                .map_err(|e| cfg_err(format!("bridge CA {ca}: {e}")))?;
        }
        builder.with_root_certificates(roots)
    };

    let config = match (client_cert, client_key) {
        (Some(c), Some(k)) => builder
            .with_client_auth_cert(load_certs(c)?, load_key(k)?)
            .map_err(|e| cfg_err(format!("bridge client cert/key: {e}")))?,
        (None, None) => builder.with_no_client_auth(),
        _ => return Err(cfg_err("bridge tls_cert and tls_key must be set together")),
    };
    Ok(Arc::new(config))
}

mod danger {
    use std::sync::Arc;

    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::crypto::{
        verify_tls12_signature, verify_tls13_signature, CryptoProvider,
    };
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{DigitallySignedStruct, Error, SignatureScheme};

    /// A `ServerCertVerifier` that accepts any certificate. Insecure — testing
    /// only. Signature verification still uses the crypto provider.
    #[derive(Debug)]
    pub struct NoVerify(pub Arc<CryptoProvider>);

    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
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
    // Prevent path traversal attacks by rejecting paths containing '..'.
    let p = Path::new(path);
    if p.components().any(|c| c == std::path::Component::ParentDir) {
        return Err(cfg_err(format!("Invalid input: {}", p.display())));
    }
    let file = File::open(p).map_err(|e| cfg_err(format!("open cert {path}: {e}")))?;
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
    // Prevent path traversal attacks by rejecting paths containing '..'.
    let p = Path::new(path);
    if p.components().any(|c| c == std::path::Component::ParentDir) {
        return Err(cfg_err(format!("Invalid input: {}", p.display())));
    }
    let file = File::open(p).map_err(|e| cfg_err(format!("open key {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| cfg_err(format!("parse key {path}: {e}")))?
        .ok_or_else(|| cfg_err(format!("no private key found in {path}")))
}
