//! Broker configuration, populated from environment variables with sensible
//! defaults so the server runs out of the box.

use std::net::SocketAddr;

use crate::error::{MqttError, Result};
use crate::types::QoS;

/// Crate version, surfaced by `--version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
pub struct Config {
    /// Address the MQTT TCP listener binds to.
    pub listen_addr: SocketAddr,
    /// Address the admin HTTP server (health, Prometheus metrics, MCP) binds
    /// to. Kept separate from the MQTT port.
    pub admin_addr: SocketAddr,
    /// Optional bearer token required on the protected admin endpoints
    /// (`/metrics`, `/mcp`). When `None`, those endpoints are unauthenticated.
    pub admin_token: Option<String>,
    /// PEM certificate chain / private key for TLS on the MQTT port. When both
    /// are set the MQTT listener speaks TLS (MQTT-over-TLS, typically port 8883).
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// PEM CA bundle used to verify client certificates on the MQTT port. When
    /// set, mutual TLS is enforced (clients must present a trusted certificate).
    pub tls_client_ca: Option<String>,
    /// Address for the MQTT-over-WebSocket listener. When `None`, WebSocket
    /// support is disabled. Carries MQTT in binary frames (subprotocol `mqtt`).
    pub ws_listen_addr: Option<SocketAddr>,
    /// PEM certificate chain / private key for TLS on the WebSocket port. When
    /// both are set the WebSocket listener speaks TLS (wss://).
    pub ws_tls_cert: Option<String>,
    pub ws_tls_key: Option<String>,
    /// PEM CA bundle used to verify client certificates on the WebSocket port.
    /// When set, mutual TLS is enforced.
    pub ws_tls_client_ca: Option<String>,
    /// PEM certificate chain / private key for TLS on the admin port. When both
    /// are set the admin server speaks HTTPS.
    pub admin_tls_cert: Option<String>,
    pub admin_tls_key: Option<String>,
    /// PEM CA bundle used to verify client certificates on the admin port.
    /// When set, mutual TLS is enforced on the admin server.
    pub admin_tls_client_ca: Option<String>,
    /// Path to a JSON ACL policy authorizing publish/subscribe per identity.
    /// When unset, all operations are permitted.
    pub acl_path: Option<String>,
    /// Path to the SQLite database file.
    pub db_path: String,
    /// Maximum packet size the server will accept (3.2.2.3.6).
    pub max_packet_size: u32,
    /// Server Receive Maximum: concurrent unacknowledged QoS>0 publications the
    /// server is willing to accept from a client (3.2.2.3.3).
    pub receive_maximum: u16,
    /// Highest QoS the server supports (3.2.2.3.4).
    pub maximum_qos: QoS,
    /// Whether retained messages are supported (3.2.2.3.5).
    pub retain_available: bool,
    /// Topic Alias Maximum the server grants to clients (3.2.2.3.8).
    pub topic_alias_maximum: u16,
    /// Server Keep Alive override in seconds; `None` honours the client value.
    pub server_keep_alive: Option<u16>,
    /// Maximum Session Expiry Interval the server will retain state for.
    pub max_session_expiry: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen_addr: "0.0.0.0:1883".parse().unwrap(),
            admin_addr: "127.0.0.1:9001".parse().unwrap(),
            admin_token: None,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            ws_listen_addr: None,
            ws_tls_cert: None,
            ws_tls_key: None,
            ws_tls_client_ca: None,
            admin_tls_cert: None,
            admin_tls_key: None,
            admin_tls_client_ca: None,
            acl_path: None,
            db_path: "mqtt_broker.db".to_string(),
            max_packet_size: 1024 * 1024, // 1 MiB
            receive_maximum: 64,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            topic_alias_maximum: 16,
            server_keep_alive: None,
            max_session_expiry: 3600,
        }
    }
}

impl Config {
    /// Load configuration from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();
        if let Ok(v) = std::env::var("MQTT_LISTEN_ADDR") {
            if let Ok(addr) = v.parse() {
                cfg.listen_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("MQTT_ADMIN_ADDR") {
            if let Ok(addr) = v.parse() {
                cfg.admin_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("MQTT_ADMIN_TOKEN") {
            let v = v.trim().to_string();
            if !v.is_empty() {
                cfg.admin_token = Some(v);
            }
        }
        cfg.tls_cert = non_empty_env("MQTT_TLS_CERT");
        cfg.tls_key = non_empty_env("MQTT_TLS_KEY");
        cfg.tls_client_ca = non_empty_env("MQTT_TLS_CLIENT_CA");
        if let Some(v) = non_empty_env("MQTT_WS_LISTEN_ADDR") {
            if let Ok(addr) = v.parse() {
                cfg.ws_listen_addr = Some(addr);
            }
        }
        cfg.ws_tls_cert = non_empty_env("MQTT_WS_TLS_CERT");
        cfg.ws_tls_key = non_empty_env("MQTT_WS_TLS_KEY");
        cfg.ws_tls_client_ca = non_empty_env("MQTT_WS_TLS_CLIENT_CA");
        cfg.admin_tls_cert = non_empty_env("MQTT_ADMIN_TLS_CERT");
        cfg.admin_tls_key = non_empty_env("MQTT_ADMIN_TLS_KEY");
        cfg.admin_tls_client_ca = non_empty_env("MQTT_ADMIN_TLS_CLIENT_CA");
        cfg.acl_path = non_empty_env("MQTT_ACL_FILE");
        if let Ok(v) = std::env::var("MQTT_DB_PATH") {
            cfg.db_path = v;
        }
        if let Ok(v) = std::env::var("MQTT_MAX_PACKET_SIZE") {
            if let Ok(n) = v.parse() {
                cfg.max_packet_size = n;
            }
        }
        if let Ok(v) = std::env::var("MQTT_RECEIVE_MAXIMUM") {
            if let Ok(n) = v.parse() {
                cfg.receive_maximum = n;
            }
        }
        if let Ok(v) = std::env::var("MQTT_MAX_SESSION_EXPIRY") {
            if let Ok(n) = v.parse() {
                cfg.max_session_expiry = n;
            }
        }
        cfg
    }
}

/// Read an environment variable, returning `None` when unset or blank.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Outcome of parsing the command line.
pub enum Startup {
    /// Run the broker with this configuration.
    Run(Box<Config>),
    /// `--help` or `--version` was handled; the process should exit 0.
    Exit,
}

/// `--help` text. Kept in sync with the option table below.
pub const HELP: &str = "\
mqtt_server — an MQTT v5.0 broker (Tokio + SQLite)

USAGE:
    mqtt_server [OPTIONS]

Every option can also be set via the environment variable shown in brackets.
Command-line flags take precedence over environment variables.

NETWORK:
    --listen-addr <ADDR>          MQTT listener bind address [MQTT_LISTEN_ADDR]
                                  (default 0.0.0.0:1883)
    --admin-addr <ADDR>           Admin/metrics/MCP HTTP bind address [MQTT_ADMIN_ADDR]
                                  (default 127.0.0.1:9001)

MQTT TLS:
    --tls-cert <FILE>             PEM certificate chain for the MQTT port [MQTT_TLS_CERT]
    --tls-key <FILE>              PEM private key for the MQTT port [MQTT_TLS_KEY]
    --tls-client-ca <FILE>        PEM CA bundle; enables mutual TLS on the MQTT
                                  port (clients must present a trusted cert)
                                  [MQTT_TLS_CLIENT_CA]

MQTT OVER WEBSOCKETS:
    --ws-listen-addr <ADDR>       Enable the WebSocket listener on this address
                                  (subprotocol \"mqtt\") [MQTT_WS_LISTEN_ADDR]
    --ws-tls-cert <FILE>          PEM certificate chain for the WS port (wss://)
                                  [MQTT_WS_TLS_CERT]
    --ws-tls-key <FILE>           PEM private key for the WS port [MQTT_WS_TLS_KEY]
    --ws-tls-client-ca <FILE>     PEM CA bundle; enables mutual TLS on the WS
                                  port [MQTT_WS_TLS_CLIENT_CA]

ADMIN TLS & AUTH:
    --admin-tls-cert <FILE>       PEM certificate chain for the admin port [MQTT_ADMIN_TLS_CERT]
    --admin-tls-key <FILE>        PEM private key for the admin port [MQTT_ADMIN_TLS_KEY]
    --admin-tls-client-ca <FILE>  PEM CA bundle; enables mutual TLS on the admin
                                  port [MQTT_ADMIN_TLS_CLIENT_CA]
    --admin-token <TOKEN>         Bearer token required for /metrics and /mcp
                                  (unset = open) [MQTT_ADMIN_TOKEN]

AUTHORIZATION:
    --acl-file <FILE>             JSON ACL policy authorizing publish/subscribe
                                  per certificate identity (unset = allow all)
                                  [MQTT_ACL_FILE]

STORAGE & LIMITS:
    --db-path <FILE>              SQLite database file [MQTT_DB_PATH]
                                  (default mqtt_broker.db)
    --max-packet-size <BYTES>     Maximum accepted packet size [MQTT_MAX_PACKET_SIZE]
                                  (default 1048576)
    --receive-maximum <N>         Server Receive Maximum [MQTT_RECEIVE_MAXIMUM]
                                  (default 64)
    --max-session-expiry <SECS>   Cap on Session Expiry Interval [MQTT_MAX_SESSION_EXPIRY]
                                  (default 3600)

OTHER:
    -h, --help                    Print this help and exit
    -V, --version                 Print version and exit

Logging verbosity is controlled by RUST_LOG (default: info).
";

impl Config {
    /// Build the configuration from environment variables, then apply any
    /// command-line overrides (flags win over the environment). Returns
    /// `Startup::Exit` when `--help`/`--version` was requested.
    pub fn from_env_and_args() -> Result<Startup> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        Config::from_env().apply_args(&args)
    }

    /// Apply CLI overrides onto an existing config. Split out for testing.
    pub fn apply_args(mut self, args: &[String]) -> Result<Startup> {
        let mut i = 0;
        while i < args.len() {
            // Support both `--flag value` and `--flag=value`.
            let (name, inline) = match args[i].split_once('=') {
                Some((n, v)) => (n.to_string(), Some(v.to_string())),
                None => (args[i].clone(), None),
            };

            // Fetch this flag's value from `=value` or the following argument.
            let value = |i: &mut usize| -> Result<String> {
                if let Some(v) = inline.clone() {
                    return Ok(v);
                }
                *i += 1;
                args.get(*i)
                    .cloned()
                    .ok_or_else(|| MqttError::Config(format!("option {name} requires a value")))
            };

            match name.as_str() {
                "-h" | "--help" => {
                    print!("{HELP}");
                    return Ok(Startup::Exit);
                }
                "-V" | "--version" => {
                    println!("mqtt_server {VERSION}");
                    return Ok(Startup::Exit);
                }
                "--listen-addr" => self.listen_addr = parse_addr(&value(&mut i)?, "--listen-addr")?,
                "--admin-addr" => self.admin_addr = parse_addr(&value(&mut i)?, "--admin-addr")?,
                "--tls-cert" => self.tls_cert = Some(value(&mut i)?),
                "--tls-key" => self.tls_key = Some(value(&mut i)?),
                "--tls-client-ca" => self.tls_client_ca = Some(value(&mut i)?),
                "--ws-listen-addr" => {
                    self.ws_listen_addr = Some(parse_addr(&value(&mut i)?, "--ws-listen-addr")?)
                }
                "--ws-tls-cert" => self.ws_tls_cert = Some(value(&mut i)?),
                "--ws-tls-key" => self.ws_tls_key = Some(value(&mut i)?),
                "--ws-tls-client-ca" => self.ws_tls_client_ca = Some(value(&mut i)?),
                "--admin-tls-cert" => self.admin_tls_cert = Some(value(&mut i)?),
                "--admin-tls-key" => self.admin_tls_key = Some(value(&mut i)?),
                "--admin-tls-client-ca" => self.admin_tls_client_ca = Some(value(&mut i)?),
                "--admin-token" => self.admin_token = Some(value(&mut i)?),
                "--acl-file" => self.acl_path = Some(value(&mut i)?),
                "--db-path" => self.db_path = value(&mut i)?,
                "--max-packet-size" => {
                    self.max_packet_size = parse_num(&value(&mut i)?, "--max-packet-size")?
                }
                "--receive-maximum" => {
                    self.receive_maximum = parse_num(&value(&mut i)?, "--receive-maximum")?
                }
                "--max-session-expiry" => {
                    self.max_session_expiry = parse_num(&value(&mut i)?, "--max-session-expiry")?
                }
                other => {
                    return Err(MqttError::Config(format!(
                        "unknown option: {other}\n\nRun with --help to list available options."
                    )));
                }
            }
            i += 1;
        }
        Ok(Startup::Run(Box::new(self)))
    }
}

fn parse_addr(v: &str, flag: &str) -> Result<SocketAddr> {
    v.parse()
        .map_err(|_| MqttError::Config(format!("{flag}: invalid socket address {v:?}")))
}

fn parse_num<T: std::str::FromStr>(v: &str, flag: &str) -> Result<T> {
    v.parse()
        .map_err(|_| MqttError::Config(format!("{flag}: invalid number {v:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_overrides_and_parses() {
        let args: Vec<String> = [
            "--listen-addr",
            "127.0.0.1:1",
            "--receive-maximum",
            "10",
            "--tls-cert",
            "c.pem",
            "--tls-key",
            "k.pem",
            "--tls-client-ca",
            "ca.pem",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let cfg = match Config::default().apply_args(&args).unwrap() {
            Startup::Run(c) => *c,
            Startup::Exit => panic!("unexpected exit"),
        };
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1");
        assert_eq!(cfg.receive_maximum, 10);
        assert_eq!(cfg.tls_client_ca.as_deref(), Some("ca.pem"));
    }

    #[test]
    fn equals_form_and_unknown_flag() {
        let ok = Config::default().apply_args(&["--db-path=/tmp/x.db".to_string()]);
        assert!(matches!(ok, Ok(Startup::Run(_))));
        let bad = Config::default().apply_args(&["--nope".to_string()]);
        assert!(bad.is_err());
    }
}
