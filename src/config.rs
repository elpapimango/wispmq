//! Broker configuration, populated from environment variables with sensible
//! defaults so the server runs out of the box.

use std::net::SocketAddr;

use yaml_rust2::{Yaml, YamlLoader};

use crate::error::{MqttError, Result};
use crate::types::QoS;

/// Config-file keys recognised in YAML (mirrors the env/CLI options).
const KNOWN_YAML_KEYS: &[&str] = &[
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
    "maximum_qos",
    "retain_available",
    "topic_alias_maximum",
    "server_keep_alive",
];

/// Default config-file names looked for in the working directory.
const DEFAULT_CONFIG_FILES: &[&str] = &["pulsemq.yaml", "pulsemq.yml"];

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
    /// Path of the YAML config file that was loaded, if any (informational).
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
            self.admin_token = Some(v);
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
    /// Full configuration pipeline: defaults, then a YAML config file (if any),
    /// then environment variables, then command-line flags — each layer
    /// overriding the previous. Returns `Startup::Exit` for `--help`/`--version`.
    pub fn load() -> Result<Startup> {
        let args: Vec<String> = std::env::args().skip(1).collect();

        // Handle --help/--version up front so they work even if a config file
        // is missing or malformed.
        for a in &args {
            match a.as_str() {
                "-h" | "--help" => {
                    print!("{HELP}");
                    return Ok(Startup::Exit);
                }
                "-V" | "--version" => {
                    println!("pulsemq {VERSION}");
                    return Ok(Startup::Exit);
                }
                _ => {}
            }
        }

        let mut cfg = Config::default();

        // Config file: an explicit --config / MQTT_CONFIG_FILE path must exist;
        // otherwise fall back to a default file in the working directory.
        let explicit = cli_config_path(&args).or_else(|| non_empty_env("MQTT_CONFIG_FILE"));
        match explicit {
            Some(path) => cfg.apply_yaml_file(&path)?,
            None => {
                if let Some(path) = default_config_file() {
                    cfg.apply_yaml_file(&path)?;
                }
            }
        }

        cfg.apply_env();
        cfg.apply_args(&args)
    }

    /// Read and apply a YAML config file, recording its path.
    pub fn apply_yaml_file(&mut self, path: &str) -> Result<()> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| MqttError::Config(format!("read config file {path}: {e}")))?;
        self.apply_yaml_str(&text, path)?;
        self.config_file = Some(path.to_string());
        Ok(())
    }

    /// Overlay YAML config text onto this config. `source` names the file for
    /// error messages. Unknown keys and wrong value types are rejected.
    pub fn apply_yaml_str(&mut self, text: &str, source: &str) -> Result<()> {
        let docs = YamlLoader::load_from_str(text)
            .map_err(|e| MqttError::Config(format!("{source}: invalid YAML: {e}")))?;
        let Some(doc) = docs.first() else {
            return Ok(()); // empty file
        };
        if doc.is_null() {
            return Ok(());
        }
        let Yaml::Hash(map) = doc else {
            return Err(MqttError::Config(format!(
                "{source}: top level must be a mapping of option: value"
            )));
        };
        for k in map.keys() {
            if let Some(key) = k.as_str() {
                if !KNOWN_YAML_KEYS.contains(&key) {
                    return Err(MqttError::Config(format!("{source}: unknown key '{key}'")));
                }
            }
        }

        // Socket addresses.
        if let Some(v) = y_str(doc, "listen_addr", source)? {
            self.listen_addr = parse_addr(&v, "listen_addr")?;
        }
        if let Some(v) = y_str(doc, "admin_addr", source)? {
            self.admin_addr = parse_addr(&v, "admin_addr")?;
        }
        if let Some(v) = y_str(doc, "ws_listen_addr", source)? {
            self.ws_listen_addr = Some(parse_addr(&v, "ws_listen_addr")?);
        }

        // String / path options.
        if let Some(v) = y_str(doc, "admin_token", source)? {
            self.admin_token = Some(v);
        }
        if let Some(v) = y_str(doc, "tls_cert", source)? {
            self.tls_cert = Some(v);
        }
        if let Some(v) = y_str(doc, "tls_key", source)? {
            self.tls_key = Some(v);
        }
        if let Some(v) = y_str(doc, "tls_client_ca", source)? {
            self.tls_client_ca = Some(v);
        }
        if let Some(v) = y_str(doc, "ws_tls_cert", source)? {
            self.ws_tls_cert = Some(v);
        }
        if let Some(v) = y_str(doc, "ws_tls_key", source)? {
            self.ws_tls_key = Some(v);
        }
        if let Some(v) = y_str(doc, "ws_tls_client_ca", source)? {
            self.ws_tls_client_ca = Some(v);
        }
        if let Some(v) = y_str(doc, "admin_tls_cert", source)? {
            self.admin_tls_cert = Some(v);
        }
        if let Some(v) = y_str(doc, "admin_tls_key", source)? {
            self.admin_tls_key = Some(v);
        }
        if let Some(v) = y_str(doc, "admin_tls_client_ca", source)? {
            self.admin_tls_client_ca = Some(v);
        }
        if let Some(v) = y_str(doc, "acl_path", source)? {
            self.acl_path = Some(v);
        }
        if let Some(v) = y_str(doc, "password_file", source)? {
            self.password_file = Some(v);
        }
        if let Some(b) = y_bool(doc, "allow_anonymous", source)? {
            self.allow_anonymous = b;
        }
        if let Some(v) = y_str(doc, "db_path", source)? {
            self.db_path = v;
        }

        // Integers.
        if let Some(n) = y_u32(doc, "max_packet_size", source)? {
            self.max_packet_size = n;
        }
        if let Some(n) = y_u32(doc, "max_session_expiry", source)? {
            self.max_session_expiry = n;
        }
        if let Some(n) = y_i64(doc, "receive_maximum", source)? {
            self.receive_maximum = u16::try_from(n).map_err(|_| {
                MqttError::Config(format!("{source}: receive_maximum out of range (0-65535)"))
            })?;
        }
        if let Some(n) = y_i64(doc, "topic_alias_maximum", source)? {
            self.topic_alias_maximum = u16::try_from(n).map_err(|_| {
                MqttError::Config(format!(
                    "{source}: topic_alias_maximum out of range (0-65535)"
                ))
            })?;
        }
        if let Some(n) = y_i64(doc, "server_keep_alive", source)? {
            self.server_keep_alive = Some(u16::try_from(n).map_err(|_| {
                MqttError::Config(format!(
                    "{source}: server_keep_alive out of range (0-65535)"
                ))
            })?);
        }
        if let Some(n) = y_i64(doc, "maximum_qos", source)? {
            self.maximum_qos = u8::try_from(n)
                .ok()
                .and_then(|b| QoS::from_u8(b).ok())
                .ok_or_else(|| {
                    MqttError::Config(format!("{source}: maximum_qos must be 0, 1, or 2"))
                })?;
        }
        if let Some(b) = y_bool(doc, "retain_available", source)? {
            self.retain_available = b;
        }

        Ok(())
    }
}

/// The first `--config`/`--config=PATH` value found in the arguments.
fn cli_config_path(args: &[String]) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        if let Some(v) = args[i].strip_prefix("--config=") {
            return Some(v.to_string());
        }
        if args[i] == "--config" {
            return args.get(i + 1).cloned();
        }
        i += 1;
    }
    None
}

/// A default config file present in the working directory, if any.
fn default_config_file() -> Option<String> {
    DEFAULT_CONFIG_FILES
        .iter()
        .find(|name| std::path::Path::new(name).is_file())
        .map(|name| name.to_string())
}

/// Read a string value from a YAML mapping, erroring on a wrong type.
fn y_str(doc: &Yaml, key: &str, source: &str) -> Result<Option<String>> {
    let v = &doc[key];
    if v.is_badvalue() || v.is_null() {
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

/// Read an integer value from a YAML mapping, erroring on a wrong type.
fn y_i64(doc: &Yaml, key: &str, source: &str) -> Result<Option<i64>> {
    let v = &doc[key];
    if v.is_badvalue() || v.is_null() {
        return Ok(None);
    }
    match v.as_i64() {
        Some(n) => Ok(Some(n)),
        None => Err(MqttError::Config(format!(
            "{source}: key '{key}' must be an integer"
        ))),
    }
}

/// Read a boolean value from a YAML mapping, erroring on a wrong type.
fn y_bool(doc: &Yaml, key: &str, source: &str) -> Result<Option<bool>> {
    let v = &doc[key];
    if v.is_badvalue() || v.is_null() {
        return Ok(None);
    }
    match v.as_bool() {
        Some(b) => Ok(Some(b)),
        None => Err(MqttError::Config(format!(
            "{source}: key '{key}' must be a boolean (true/false)"
        ))),
    }
}

/// Read a `u32` value from a YAML mapping.
fn y_u32(doc: &Yaml, key: &str, source: &str) -> Result<Option<u32>> {
    match y_i64(doc, key, source)? {
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

/// Outcome of parsing the command line.
pub enum Startup {
    /// Run the broker with this configuration.
    Run(Box<Config>),
    /// `--help` or `--version` was handled; the process should exit 0.
    Exit,
}

/// `--help` text. Kept in sync with the option table below.
pub const HELP: &str = "\
PulseMQ — an MQTT v5.0 / v3.1.1 / v3.1 broker (Tokio + SQLite)

USAGE:
    pulsemq [OPTIONS]

Every option can also be set via the environment variable shown in brackets, or
in a YAML config file (key = the option name with underscores, e.g. listen_addr).
Precedence, lowest to highest: config file < environment < command-line flags.

CONFIG FILE:
    --config <FILE>               Load this YAML config file [MQTT_CONFIG_FILE].
                                  If omitted, pulsemq.yaml (or .yml) in the
                                  working directory is used when present.

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

AUTHENTICATION & AUTHORIZATION:
    --password-file <FILE>        Username/password credential file; when set,
                                  clients must authenticate [MQTT_PASSWORD_FILE]
    --allow-anonymous <BOOL>      Allow clients with no credentials when a
                                  password file is set [MQTT_ALLOW_ANONYMOUS]
                                  (default false)
    --acl-file <FILE>             JSON ACL policy authorizing publish/subscribe
                                  per identity (unset = allow all) [MQTT_ACL_FILE]
    --hash-password [USERNAME]    Read a password from stdin, print a credential
                                  line, and exit (helper for --password-file)

STORAGE & LIMITS:
    --db-path <FILE>              SQLite database file [MQTT_DB_PATH]
                                  (default mqtt_broker.db)
    --max-packet-size <BYTES>     Maximum accepted packet size [MQTT_MAX_PACKET_SIZE]
                                  (default 1048576)
    --receive-maximum <N>         Server Receive Maximum [MQTT_RECEIVE_MAXIMUM]
                                  (default 64)
    --max-session-expiry <SECS>   Cap on Session Expiry Interval [MQTT_MAX_SESSION_EXPIRY]
                                  (default 3600)

PROTOCOL CAPABILITIES (advertised in CONNACK):
    --maximum-qos <0|1|2>         Highest QoS the server supports [MQTT_MAXIMUM_QOS]
                                  (default 2)
    --retain-available <BOOL>     Whether retained messages are supported
                                  [MQTT_RETAIN_AVAILABLE] (default true)
    --topic-alias-maximum <N>     Topic Alias Maximum granted to clients
                                  [MQTT_TOPIC_ALIAS_MAXIMUM] (default 16)
    --server-keep-alive <SECS>    Override the client's Keep Alive
                                  [MQTT_SERVER_KEEP_ALIVE] (default: honour client)

OTHER:
    -h, --help                    Print this help and exit
    -V, --version                 Print version and exit

Logging verbosity is controlled by RUST_LOG (default: info).
";

impl Config {
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
                    println!("pulsemq {VERSION}");
                    return Ok(Startup::Exit);
                }
                // Already resolved before env/args were applied; consume value.
                "--config" => {
                    let _ = value(&mut i)?;
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
                "--password-file" => self.password_file = Some(value(&mut i)?),
                "--allow-anonymous" => {
                    self.allow_anonymous = parse_bool_arg(&value(&mut i)?, "--allow-anonymous")?
                }
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
                "--maximum-qos" => {
                    self.maximum_qos = parse_qos_arg(&value(&mut i)?, "--maximum-qos")?
                }
                "--retain-available" => {
                    self.retain_available = parse_bool_arg(&value(&mut i)?, "--retain-available")?
                }
                "--topic-alias-maximum" => {
                    self.topic_alias_maximum = parse_num(&value(&mut i)?, "--topic-alias-maximum")?
                }
                "--server-keep-alive" => {
                    self.server_keep_alive =
                        Some(parse_num(&value(&mut i)?, "--server-keep-alive")?)
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

/// Parse a Maximum QoS value (0, 1, or 2). Lenient: returns `None` on garbage.
fn parse_qos_value(v: &str) -> Option<QoS> {
    v.trim()
        .parse::<u8>()
        .ok()
        .and_then(|n| QoS::from_u8(n).ok())
}

/// Parse a boolean from common spellings. Lenient: returns `None` on garbage.
fn parse_bool_value(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_qos_arg(v: &str, flag: &str) -> Result<QoS> {
    parse_qos_value(v).ok_or_else(|| MqttError::Config(format!("{flag}: expected 0, 1, or 2")))
}

fn parse_bool_arg(v: &str, flag: &str) -> Result<bool> {
    parse_bool_value(v).ok_or_else(|| MqttError::Config(format!("{flag}: expected true or false")))
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

    #[test]
    fn yaml_config_applies_all_field_kinds() {
        let yaml = r#"
listen_addr: "127.0.0.1:1884"
ws_listen_addr: "0.0.0.0:8080"
tls_cert: "server.pem"
admin_token: "sekret"
acl_path: "acl.json"
db_path: "/data/broker.db"
max_packet_size: 2097152
receive_maximum: 100
max_session_expiry: 600
"#;
        let mut cfg = Config::default();
        cfg.apply_yaml_str(yaml, "test.yaml").unwrap();
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1884");
        assert_eq!(
            cfg.ws_listen_addr.map(|a| a.to_string()).as_deref(),
            Some("0.0.0.0:8080")
        );
        assert_eq!(cfg.tls_cert.as_deref(), Some("server.pem"));
        assert_eq!(cfg.admin_token.as_deref(), Some("sekret"));
        assert_eq!(cfg.db_path, "/data/broker.db");
        assert_eq!(cfg.max_packet_size, 2_097_152);
        assert_eq!(cfg.receive_maximum, 100);
        assert_eq!(cfg.max_session_expiry, 600);
    }

    #[test]
    fn cli_flags_override_yaml() {
        let mut cfg = Config::default();
        cfg.apply_yaml_str(
            "listen_addr: \"127.0.0.1:1\"\nreceive_maximum: 5\n",
            "t.yaml",
        )
        .unwrap();
        // CLI applied on top of the file wins.
        let cfg = match cfg
            .apply_args(&["--receive-maximum".into(), "42".into()])
            .unwrap()
        {
            Startup::Run(c) => *c,
            Startup::Exit => panic!("unexpected exit"),
        };
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1"); // from file
        assert_eq!(cfg.receive_maximum, 42); // CLI override
    }

    #[test]
    fn yaml_rejects_unknown_key_and_bad_type() {
        let mut cfg = Config::default();
        assert!(cfg.apply_yaml_str("listen_port: 1883\n", "t.yaml").is_err());
        let mut cfg = Config::default();
        assert!(cfg
            .apply_yaml_str("receive_maximum: \"lots\"\n", "t.yaml")
            .is_err());
        let mut cfg = Config::default();
        assert!(cfg.apply_yaml_str("- a\n- b\n", "t.yaml").is_err());
    }

    #[test]
    fn yaml_protocol_capabilities() {
        let yaml = "maximum_qos: 1\nretain_available: false\ntopic_alias_maximum: 5\nserver_keep_alive: 30\n";
        let mut cfg = Config::default();
        cfg.apply_yaml_str(yaml, "t.yaml").unwrap();
        assert_eq!(cfg.maximum_qos, QoS::AtLeastOnce);
        assert!(!cfg.retain_available);
        assert_eq!(cfg.topic_alias_maximum, 5);
        assert_eq!(cfg.server_keep_alive, Some(30));

        // Out-of-range QoS is rejected.
        let mut bad = Config::default();
        assert!(bad.apply_yaml_str("maximum_qos: 3\n", "t.yaml").is_err());
        // Wrong type for a boolean is rejected.
        let mut bad = Config::default();
        assert!(bad
            .apply_yaml_str("retain_available: \"yes\"\n", "t.yaml")
            .is_err());
    }

    #[test]
    fn cli_protocol_capabilities() {
        let args: Vec<String> = [
            "--maximum-qos",
            "0",
            "--retain-available",
            "false",
            "--server-keep-alive",
            "45",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let cfg = match Config::default().apply_args(&args).unwrap() {
            Startup::Run(c) => *c,
            Startup::Exit => panic!("unexpected exit"),
        };
        assert_eq!(cfg.maximum_qos, QoS::AtMostOnce);
        assert!(!cfg.retain_available);
        assert_eq!(cfg.server_keep_alive, Some(45));
        // Invalid QoS on the CLI is a hard error.
        assert!(Config::default()
            .apply_args(&["--maximum-qos".into(), "9".into()])
            .is_err());
    }

    #[test]
    fn empty_yaml_is_ok() {
        let mut cfg = Config::default();
        assert!(cfg.apply_yaml_str("", "t.yaml").is_ok());
        assert!(cfg.apply_yaml_str("# just a comment\n", "t.yaml").is_ok());
    }
}
