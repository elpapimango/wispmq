//! Broker configuration, layered from four sources — defaults, a JSON config
//! file, environment variables, then command-line flags — each overriding the
//! previous, so the server runs out of the box with nothing configured.
//!
//! [`Config::load`] drives the whole pipeline. The command-line layer lives in
//! [`crate::cli`]; this module owns the defaults, the env layer, and the JSON
//! file layer.

use std::net::SocketAddr;

use clap::Parser;
use serde_json::Value;

use crate::cli::Cli;
use crate::error::{MqttError, Result};
use crate::types::QoS;

/// Config-file keys recognised in the JSON config (mirrors the env/CLI options).
const KNOWN_KEYS: &[&str] = &[
    "listen_addr",
    "admin_addr",
    "admin_token",
    "tls_cert",
    "tls_key",
    "tls_client_ca",
    "ws_listen_addr",
    "ws_tls_cert",
    "ws_tls_key",
    "ws_tls_client_ca",
    "admin_tls_cert",
    "admin_tls_key",
    "admin_tls_client_ca",
    "acl_path",
    "password_file",
    "allow_anonymous",
    "db_path",
    "max_packet_size",
    "receive_maximum",
    "max_session_expiry",
    "max_queued_messages",
    "sys_interval",
    "maximum_qos",
    "retain_available",
    "topic_alias_maximum",
    "server_keep_alive",
    "bridges",
];

/// Default config-file names looked for in the working directory.
const DEFAULT_CONFIG_FILES: &[&str] = &["pulsemq.json"];

/// Crate version, surfaced by `--version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A configuration value that must never reach a log line or error message.
///
/// `Config` derives `Debug` and is passed around freely, so any plaintext
/// secret stored in it is one `{config:?}` away from being written to disk or
/// shipped to a log aggregator. Wrapping it means the redaction cannot be
/// forgotten when a new field is added — the derive stays correct by
/// construction. Read the value with [`Secret::expose`] at the point of use.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    /// Borrow the underlying value. Named to make each use site conspicuous.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Address the MQTT TCP listener binds to.
    pub listen_addr: SocketAddr,
    /// Address the admin HTTP server (health, Prometheus metrics, MCP) binds
    /// to. Kept separate from the MQTT port.
    pub admin_addr: SocketAddr,
    /// Optional bearer token required on the protected admin endpoints
    /// (`/metrics`, `/mcp`). When `None`, those endpoints are unauthenticated.
    pub admin_token: Option<Secret>,
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
    /// Path to a username/password credential file. When set, clients must
    /// authenticate (unless `allow_anonymous`). See `auth`.
    pub password_file: Option<String>,
    /// When a password file is set, permit clients that present no credentials
    /// to connect as `anonymous`. A client that does present a username must
    /// still authenticate. Ignored when no password file is configured.
    pub allow_anonymous: bool,
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
    /// Cap on messages held for an offline persistent session. Without a cap a
    /// durable subscriber to a busy topic accumulates messages in memory for
    /// the whole session-expiry window, which is a memory-exhaustion DoS on a
    /// small host. `0` means unlimited (the pre-1.0 behaviour).
    pub max_queued_messages: u32,
    /// How often (seconds) to refresh the `$SYS/broker/...` status topics.
    /// `0` disables $SYS publishing entirely.
    pub sys_interval: u32,
    /// Broker-to-broker forwarding bridges (config-file only). See `bridge`.
    pub bridges: Vec<crate::bridge::BridgeConfig>,
    /// Path of the JSON config file that was loaded, if any (informational).
    pub config_file: Option<String>,
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
            password_file: None,
            allow_anonymous: false,
            db_path: "mqtt_broker.db".to_string(),
            max_packet_size: 1024 * 1024, // 1 MiB
            receive_maximum: 64,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            topic_alias_maximum: 16,
            server_keep_alive: None,
            max_session_expiry: 3600,
            max_queued_messages: 1000,
            sys_interval: 10,
            bridges: Vec::new(),
            config_file: None,
        }
    }
}

impl Config {
    /// Build a config from defaults and environment variables.
    pub fn from_env() -> Self {
        let mut cfg = Config::default();
        cfg.apply_env();
        cfg
    }

    /// Overlay environment variables onto this config (env wins over whatever
    /// is already set, e.g. defaults or a config file).
    pub fn apply_env(&mut self) {
        if let Some(v) = non_empty_env("MQTT_LISTEN_ADDR") {
            if let Ok(addr) = v.parse() {
                self.listen_addr = addr;
            }
        }
        if let Some(v) = non_empty_env("MQTT_ADMIN_ADDR") {
            if let Ok(addr) = v.parse() {
                self.admin_addr = addr;
            }
        }
        if let Some(v) = non_empty_env("MQTT_ADMIN_TOKEN") {
            self.admin_token = Some(Secret::new(v));
        }
        overlay_opt(&mut self.tls_cert, non_empty_env("MQTT_TLS_CERT"));
        overlay_opt(&mut self.tls_key, non_empty_env("MQTT_TLS_KEY"));
        overlay_opt(&mut self.tls_client_ca, non_empty_env("MQTT_TLS_CLIENT_CA"));
        if let Some(v) = non_empty_env("MQTT_WS_LISTEN_ADDR") {
            if let Ok(addr) = v.parse() {
                self.ws_listen_addr = Some(addr);
            }
        }
        overlay_opt(&mut self.ws_tls_cert, non_empty_env("MQTT_WS_TLS_CERT"));
        overlay_opt(&mut self.ws_tls_key, non_empty_env("MQTT_WS_TLS_KEY"));
        overlay_opt(
            &mut self.ws_tls_client_ca,
            non_empty_env("MQTT_WS_TLS_CLIENT_CA"),
        );
        overlay_opt(
            &mut self.admin_tls_cert,
            non_empty_env("MQTT_ADMIN_TLS_CERT"),
        );
        overlay_opt(&mut self.admin_tls_key, non_empty_env("MQTT_ADMIN_TLS_KEY"));
        overlay_opt(
            &mut self.admin_tls_client_ca,
            non_empty_env("MQTT_ADMIN_TLS_CLIENT_CA"),
        );
        overlay_opt(&mut self.acl_path, non_empty_env("MQTT_ACL_FILE"));
        overlay_opt(&mut self.password_file, non_empty_env("MQTT_PASSWORD_FILE"));
        if let Some(b) = non_empty_env("MQTT_ALLOW_ANONYMOUS")
            .as_deref()
            .and_then(parse_bool_value)
        {
            self.allow_anonymous = b;
        }
        if let Some(v) = non_empty_env("MQTT_DB_PATH") {
            self.db_path = v;
        }
        if let Some(v) = non_empty_env("MQTT_MAX_PACKET_SIZE") {
            if let Ok(n) = v.parse() {
                self.max_packet_size = n;
            }
        }
        if let Some(v) = non_empty_env("MQTT_RECEIVE_MAXIMUM") {
            if let Ok(n) = v.parse() {
                self.receive_maximum = n;
            }
        }
        if let Some(v) = non_empty_env("MQTT_MAX_SESSION_EXPIRY") {
            if let Ok(n) = v.parse() {
                self.max_session_expiry = n;
            }
        }
        if let Some(v) = non_empty_env("MQTT_MAX_QUEUED_MESSAGES") {
            if let Ok(n) = v.parse() {
                self.max_queued_messages = n;
            }
        }
        if let Some(v) = non_empty_env("MQTT_SYS_INTERVAL") {
            if let Ok(n) = v.parse() {
                self.sys_interval = n;
            }
        }
        if let Some(q) = non_empty_env("MQTT_MAXIMUM_QOS")
            .as_deref()
            .and_then(parse_qos_value)
        {
            self.maximum_qos = q;
        }
        if let Some(b) = non_empty_env("MQTT_RETAIN_AVAILABLE")
            .as_deref()
            .and_then(parse_bool_value)
        {
            self.retain_available = b;
        }
        if let Some(v) = non_empty_env("MQTT_TOPIC_ALIAS_MAXIMUM") {
            if let Ok(n) = v.parse() {
                self.topic_alias_maximum = n;
            }
        }
        if let Some(v) = non_empty_env("MQTT_SERVER_KEEP_ALIVE") {
            if let Ok(n) = v.parse() {
                self.server_keep_alive = Some(n);
            }
        }
    }
}

/// Overwrite `slot` with `value` when `value` is `Some`; leave it otherwise.
fn overlay_opt(slot: &mut Option<String>, value: Option<String>) {
    if value.is_some() {
        *slot = value;
    }
}

impl Config {
    /// Full configuration pipeline: defaults, then a JSON config file (if any),
    /// then environment variables, then command-line flags — each layer
    /// overriding the previous.
    pub fn load() -> Result<Startup> {
        // clap renders `--help`/`--version` and usage errors itself and exits
        // with 0 and 2 respectively, so nothing below has to special-case them
        // and they work even when the config file is missing or malformed.
        Config::from_cli(Cli::parse())
    }

    /// The layering itself, with the command line already parsed. Split out
    /// from [`Config::load`] so it does not depend on the process's argv.
    fn from_cli(cli: Cli) -> Result<Startup> {
        // `--hash-password` is a one-shot helper: answer it before reading the
        // config file, so a broken pulsemq.json cannot block hashing a password.
        if let Some(user) = cli.hash_password {
            return Ok(Startup::HashPassword(user));
        }

        let mut cfg = Config::default();

        // Config file: an explicit --config / MQTT_CONFIG_FILE path must exist;
        // otherwise fall back to a default file in the working directory.
        let explicit = cli
            .config
            .clone()
            .or_else(|| non_empty_env("MQTT_CONFIG_FILE"));
        match explicit {
            Some(path) => cfg.apply_json_file(&path)?,
            None => {
                if let Some(path) = default_config_file() {
                    cfg.apply_json_file(&path)?;
                }
            }
        }

        cfg.apply_env();
        cli.apply(&mut cfg);
        Ok(Startup::Run(Box::new(cfg)))
    }

    /// Read and apply a JSON config file, recording its path.
    pub fn apply_json_file(&mut self, path: &str) -> Result<()> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| MqttError::Config(format!("read config file {path}: {e}")))?;
        self.apply_json_str(&text, path)?;
        self.config_file = Some(path.to_string());
        Ok(())
    }

    /// Overlay JSON config text onto this config. `source` names the file for
    /// error messages. Unknown keys and wrong value types are rejected.
    pub fn apply_json_str(&mut self, text: &str, source: &str) -> Result<()> {
        // An entirely blank file is treated as "no overrides" rather than a
        // parse error, so touching the config file is harmless.
        if text.trim().is_empty() {
            return Ok(());
        }
        let doc: Value = serde_json::from_str(text)
            .map_err(|e| MqttError::Config(format!("{source}: invalid JSON: {e}")))?;
        if doc.is_null() {
            return Ok(());
        }
        let Some(map) = doc.as_object() else {
            return Err(MqttError::Config(format!(
                "{source}: top level must be a JSON object of \"option\": value pairs"
            )));
        };
        for key in map.keys() {
            if !KNOWN_KEYS.contains(&key.as_str()) {
                return Err(MqttError::Config(format!("{source}: unknown key '{key}'")));
            }
        }
        let doc = &doc;

        // Socket addresses.
        if let Some(v) = j_str(doc, "listen_addr", source)? {
            self.listen_addr = parse_addr(&v, "listen_addr")?;
        }
        if let Some(v) = j_str(doc, "admin_addr", source)? {
            self.admin_addr = parse_addr(&v, "admin_addr")?;
        }
        if let Some(v) = j_str(doc, "ws_listen_addr", source)? {
            self.ws_listen_addr = Some(parse_addr(&v, "ws_listen_addr")?);
        }

        // String / path options.
        if let Some(v) = j_str(doc, "admin_token", source)? {
            self.admin_token = Some(Secret::new(v));
        }
        if let Some(v) = j_str(doc, "tls_cert", source)? {
            self.tls_cert = Some(v);
        }
        if let Some(v) = j_str(doc, "tls_key", source)? {
            self.tls_key = Some(v);
        }
        if let Some(v) = j_str(doc, "tls_client_ca", source)? {
            self.tls_client_ca = Some(v);
        }
        if let Some(v) = j_str(doc, "ws_tls_cert", source)? {
            self.ws_tls_cert = Some(v);
        }
        if let Some(v) = j_str(doc, "ws_tls_key", source)? {
            self.ws_tls_key = Some(v);
        }
        if let Some(v) = j_str(doc, "ws_tls_client_ca", source)? {
            self.ws_tls_client_ca = Some(v);
        }
        if let Some(v) = j_str(doc, "admin_tls_cert", source)? {
            self.admin_tls_cert = Some(v);
        }
        if let Some(v) = j_str(doc, "admin_tls_key", source)? {
            self.admin_tls_key = Some(v);
        }
        if let Some(v) = j_str(doc, "admin_tls_client_ca", source)? {
            self.admin_tls_client_ca = Some(v);
        }
        if let Some(v) = j_str(doc, "acl_path", source)? {
            self.acl_path = Some(v);
        }
        if let Some(v) = j_str(doc, "password_file", source)? {
            self.password_file = Some(v);
        }
        if let Some(b) = j_bool(doc, "allow_anonymous", source)? {
            self.allow_anonymous = b;
        }
        if let Some(v) = j_str(doc, "db_path", source)? {
            self.db_path = v;
        }

        // Integers.
        if let Some(n) = j_u32(doc, "max_packet_size", source)? {
            self.max_packet_size = n;
        }
        if let Some(n) = j_u32(doc, "max_session_expiry", source)? {
            self.max_session_expiry = n;
        }
        if let Some(n) = j_u32(doc, "max_queued_messages", source)? {
            self.max_queued_messages = n;
        }
        if let Some(n) = j_u32(doc, "sys_interval", source)? {
            self.sys_interval = n;
        }
        if let Some(n) = j_i64(doc, "receive_maximum", source)? {
            self.receive_maximum = u16::try_from(n).map_err(|_| {
                MqttError::Config(format!("{source}: receive_maximum out of range (0-65535)"))
            })?;
        }
        if let Some(n) = j_i64(doc, "topic_alias_maximum", source)? {
            self.topic_alias_maximum = u16::try_from(n).map_err(|_| {
                MqttError::Config(format!(
                    "{source}: topic_alias_maximum out of range (0-65535)"
                ))
            })?;
        }
        if let Some(n) = j_i64(doc, "server_keep_alive", source)? {
            self.server_keep_alive = Some(u16::try_from(n).map_err(|_| {
                MqttError::Config(format!(
                    "{source}: server_keep_alive out of range (0-65535)"
                ))
            })?);
        }
        if let Some(n) = j_i64(doc, "maximum_qos", source)? {
            self.maximum_qos = u8::try_from(n)
                .ok()
                .and_then(|b| QoS::from_u8(b).ok())
                .ok_or_else(|| {
                    MqttError::Config(format!("{source}: maximum_qos must be 0, 1, or 2"))
                })?;
        }
        if let Some(b) = j_bool(doc, "retain_available", source)? {
            self.retain_available = b;
        }

        // Bridges are a structured array; parsed by the bridge module. A config
        // file that omits `bridges` leaves any already-set value untouched.
        let bridges = crate::bridge::parse_bridges(doc, source)?;
        if !bridges.is_empty() {
            self.bridges = bridges;
        }

        Ok(())
    }
}

/// A default config file present in the working directory, if any.
fn default_config_file() -> Option<String> {
    DEFAULT_CONFIG_FILES
        .iter()
        .find(|name| std::path::Path::new(name).is_file())
        .map(|name| name.to_string())
}

/// Read a string value from the config object, erroring on a wrong type.
/// A missing key and an explicit `null` both mean "not set".
fn j_str(doc: &Value, key: &str, source: &str) -> Result<Option<String>> {
    let v = &doc[key];
    if v.is_null() {
        return Ok(None);
    }
    match v.as_str() {
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => Ok(Some(s.trim().to_string())),
        None => Err(MqttError::Config(format!(
            "{source}: key '{key}' must be a string"
        ))),
    }
}

/// Read an integer value from the config object, erroring on a wrong type.
/// A JSON float (`5.5`) is rejected rather than silently truncated.
fn j_i64(doc: &Value, key: &str, source: &str) -> Result<Option<i64>> {
    let v = &doc[key];
    if v.is_null() {
        return Ok(None);
    }
    match v.as_i64() {
        Some(n) => Ok(Some(n)),
        None => Err(MqttError::Config(format!(
            "{source}: key '{key}' must be an integer"
        ))),
    }
}

/// Read a boolean value from the config object, erroring on a wrong type.
fn j_bool(doc: &Value, key: &str, source: &str) -> Result<Option<bool>> {
    let v = &doc[key];
    if v.is_null() {
        return Ok(None);
    }
    match v.as_bool() {
        Some(b) => Ok(Some(b)),
        None => Err(MqttError::Config(format!(
            "{source}: key '{key}' must be a boolean (true/false)"
        ))),
    }
}

/// Read a `u32` value from the config object.
fn j_u32(doc: &Value, key: &str, source: &str) -> Result<Option<u32>> {
    match j_i64(doc, key, source)? {
        Some(n) => Ok(Some(u32::try_from(n).map_err(|_| {
            MqttError::Config(format!("{source}: key '{key}' out of range (0-4294967295)"))
        })?)),
        None => Ok(None),
    }
}

/// Read an environment variable, returning `None` when unset or blank.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Outcome of resolving the configuration.
pub enum Startup {
    /// Run the broker with this configuration.
    Run(Box<Config>),
    /// `--hash-password [USERNAME]` was passed: print a credential line for
    /// that user (or just the hash, when no username was given) and exit.
    HashPassword(Option<String>),
}

fn parse_addr(v: &str, flag: &str) -> Result<SocketAddr> {
    v.parse()
        .map_err(|_| MqttError::Config(format!("{flag}: invalid socket address {v:?}")))
}

/// Parse a Maximum QoS value (0, 1, or 2). Returns `None` on garbage; callers
/// decide whether that is an error (the CLI) or an ignored value (the env).
pub(crate) fn parse_qos_value(v: &str) -> Option<QoS> {
    v.trim()
        .parse::<u8>()
        .ok()
        .and_then(|n| QoS::from_u8(n).ok())
}

/// Parse a boolean from common spellings. Lenient in the same way as
/// [`parse_qos_value`].
pub(crate) fn parse_bool_value(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The command-line layer is tested in `crate::cli`, including its
    // precedence over the env and file layers exercised here.

    #[test]
    fn json_config_applies_all_field_kinds() {
        let json = r#"{
  "listen_addr": "127.0.0.1:1884",
  "ws_listen_addr": "0.0.0.0:8080",
  "tls_cert": "server.pem",
  "admin_token": "sekret",
  "acl_path": "acl.json",
  "db_path": "/data/broker.db",
  "max_packet_size": 2097152,
  "receive_maximum": 100,
  "max_session_expiry": 600,
  "max_queued_messages": 42
}"#;
        let mut cfg = Config::default();
        cfg.apply_json_str(json, "test.json").unwrap();
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1884");
        assert_eq!(
            cfg.ws_listen_addr.map(|a| a.to_string()).as_deref(),
            Some("0.0.0.0:8080")
        );
        assert_eq!(cfg.tls_cert.as_deref(), Some("server.pem"));
        assert_eq!(cfg.admin_token.as_ref().map(Secret::expose), Some("sekret"));
        assert_eq!(cfg.db_path, "/data/broker.db");
        assert_eq!(cfg.max_packet_size, 2_097_152);
        assert_eq!(cfg.receive_maximum, 100);
        assert_eq!(cfg.max_session_expiry, 600);
        assert_eq!(cfg.max_queued_messages, 42);
    }

    #[test]
    fn queue_bound_defaults_on() {
        // The cap must be on by default: 1.0.0 shipped with an unbounded
        // offline queue, which is the memory-exhaustion path this closes.
        assert_eq!(Config::default().max_queued_messages, 1000);

        let mut cfg = Config::default();
        cfg.apply_json_str(r#"{"max_queued_messages": 5}"#, "t.json")
            .unwrap();
        assert_eq!(cfg.max_queued_messages, 5);

        // 0 is a legal opt-out, not a parse error.
        let mut cfg = Config::default();
        cfg.apply_json_str(r#"{"max_queued_messages": 0}"#, "t.json")
            .unwrap();
        assert_eq!(cfg.max_queued_messages, 0);
    }

    #[test]
    fn json_rejects_unknown_key_and_bad_type() {
        let mut cfg = Config::default();
        assert!(cfg
            .apply_json_str(r#"{"listen_port": 1883}"#, "t.json")
            .is_err());
        let mut cfg = Config::default();
        assert!(cfg
            .apply_json_str(r#"{"receive_maximum": "lots"}"#, "t.json")
            .is_err());
        let mut cfg = Config::default();
        // Top level must be an object, not an array.
        assert!(cfg.apply_json_str("[1, 2]", "t.json").is_err());
        // Malformed JSON is a clear error, not a silent no-op.
        assert!(cfg.apply_json_str("{not json", "t.json").is_err());
        // A float where an integer is required must not be truncated.
        assert!(cfg
            .apply_json_str(r#"{"receive_maximum": 5.5}"#, "t.json")
            .is_err());
    }

    #[test]
    fn json_protocol_capabilities() {
        let json = r#"{"maximum_qos": 1, "retain_available": false,
                       "topic_alias_maximum": 5, "server_keep_alive": 30}"#;
        let mut cfg = Config::default();
        cfg.apply_json_str(json, "t.json").unwrap();
        assert_eq!(cfg.maximum_qos, QoS::AtLeastOnce);
        assert!(!cfg.retain_available);
        assert_eq!(cfg.topic_alias_maximum, 5);
        assert_eq!(cfg.server_keep_alive, Some(30));

        // Out-of-range QoS is rejected.
        let mut bad = Config::default();
        assert!(bad
            .apply_json_str(r#"{"maximum_qos": 3}"#, "t.json")
            .is_err());
        // Wrong type for a boolean is rejected.
        let mut bad = Config::default();
        assert!(bad
            .apply_json_str(r#"{"retain_available": "yes"}"#, "t.json")
            .is_err());
    }

    #[test]
    fn empty_config_file_is_ok() {
        let mut cfg = Config::default();
        // A blank or whitespace-only file means "no overrides", so creating the
        // file before filling it in is not a startup failure.
        assert!(cfg.apply_json_str("", "t.json").is_ok());
        assert!(cfg.apply_json_str("   \n\t\n", "t.json").is_ok());
        // An empty object and an explicit null are also no-ops.
        assert!(cfg.apply_json_str("{}", "t.json").is_ok());
        assert!(cfg.apply_json_str("null", "t.json").is_ok());
    }

    #[test]
    fn comments_are_rejected_as_invalid_json() {
        // Documents a deliberate trade-off of the move from YAML: the config is
        // strict JSON, so the annotated example that YAML allowed is gone and
        // per-option documentation lives in the README instead. A `#` line is a
        // parse error rather than being silently ignored, so anyone porting a
        // commented pulsemq.yaml is told plainly instead of having options
        // quietly dropped.
        let mut cfg = Config::default();
        assert!(cfg.apply_json_str("# just a comment\n", "t.json").is_err());
        assert!(cfg
            .apply_json_str(
                "{\n  // listener\n  \"listen_addr\": \"0.0.0.0:1\"\n}",
                "t.json"
            )
            .is_err());
    }
}
