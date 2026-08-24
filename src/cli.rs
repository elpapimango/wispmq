//! Command-line interface, defined with `clap`'s derive API.
//!
//! Every field is an `Option` so that "the user did not pass this flag" stays
//! distinguishable from "the user passed a value that happens to equal the
//! default". The layered configuration (defaults < JSON file < env < CLI, see
//! [`crate::config`]) depends on that: [`Cli::apply`] is the last layer and
//! only a `Some` overrides what the layers below decided.
//!
//! Environment variables are deliberately **not** wired through clap's `env`
//! attribute. Doing so would fold them into the CLI layer and collapse two
//! levels of precedence into one, and it would turn today's lenient handling of
//! a malformed env value (ignore it, keep the lower layer) into a hard startup
//! error. They stay in `Config::apply_env`; the env var name is quoted in each
//! flag's help text so `--help` still documents the pair.

use std::net::SocketAddr;

use clap::Parser;

use crate::config::{parse_bool_value, parse_qos_value, Config, OtlpHeaders, Secret};
use crate::types::QoS;

/// Command-line options. Flag names are derived from the field names, so
/// `listen_addr` is `--listen-addr`.
#[derive(Debug, Parser)]
#[command(
    name = "pulsemq",
    version,
    about = "PulseMQ — an MQTT v5.0 / v3.1.1 / v3.1 broker (Tokio + SQLite)",
    after_help = "Every option can also be set via the environment variable shown in brackets, or \
                  in a JSON config file (key = the option name with underscores, e.g. \
                  listen_addr). Precedence, lowest to highest: config file < environment < \
                  command-line flags.\n\nFlags accept either `--flag value` or `--flag=value`. \
                  Logging verbosity is controlled by RUST_LOG (default: info)."
)]
pub(crate) struct Cli {
    /// Load this JSON config file [MQTT_CONFIG_FILE]. If omitted, pulsemq.json
    /// in the working directory is used when present.
    #[arg(long, value_name = "FILE", help_heading = "Config file")]
    pub config: Option<String>,

    /// MQTT listener bind address [MQTT_LISTEN_ADDR] (default 0.0.0.0:1883)
    #[arg(long, value_name = "ADDR", help_heading = "Network")]
    pub listen_addr: Option<SocketAddr>,

    /// Admin/metrics/MCP HTTP bind address [MQTT_ADMIN_ADDR] (default 127.0.0.1:9001)
    #[arg(long, value_name = "ADDR", help_heading = "Network")]
    pub admin_addr: Option<SocketAddr>,

    /// Max new connections per source IP per rate window, 0=unlimited
    /// [MQTT_MAX_CONNECTIONS_PER_IP] (default 0)
    #[arg(long, value_name = "N", help_heading = "Network")]
    pub max_connections_per_ip: Option<u32>,

    /// Window (seconds) max-connections-per-ip is measured over
    /// [MQTT_CONNECTION_RATE_WINDOW_SECS] (default 10)
    #[arg(long, value_name = "SECS", help_heading = "Network")]
    pub connection_rate_window_secs: Option<u32>,

    /// PEM certificate chain for the MQTT port [MQTT_TLS_CERT]
    #[arg(long, value_name = "FILE", help_heading = "MQTT TLS")]
    pub tls_cert: Option<String>,

    /// PEM private key for the MQTT port [MQTT_TLS_KEY]
    #[arg(long, value_name = "FILE", help_heading = "MQTT TLS")]
    pub tls_key: Option<String>,

    /// PEM CA bundle; enables mutual TLS on the MQTT port (clients must present
    /// a trusted cert) [MQTT_TLS_CLIENT_CA]
    #[arg(long, value_name = "FILE", help_heading = "MQTT TLS")]
    pub tls_client_ca: Option<String>,

    /// Enable the WebSocket listener on this address (subprotocol "mqtt")
    /// [MQTT_WS_LISTEN_ADDR]
    #[arg(long, value_name = "ADDR", help_heading = "MQTT over WebSockets")]
    pub ws_listen_addr: Option<SocketAddr>,

    /// PEM certificate chain for the WS port (wss://) [MQTT_WS_TLS_CERT]
    #[arg(long, value_name = "FILE", help_heading = "MQTT over WebSockets")]
    pub ws_tls_cert: Option<String>,

    /// PEM private key for the WS port [MQTT_WS_TLS_KEY]
    #[arg(long, value_name = "FILE", help_heading = "MQTT over WebSockets")]
    pub ws_tls_key: Option<String>,

    /// PEM CA bundle; enables mutual TLS on the WS port [MQTT_WS_TLS_CLIENT_CA]
    #[arg(long, value_name = "FILE", help_heading = "MQTT over WebSockets")]
    pub ws_tls_client_ca: Option<String>,

    /// PEM certificate chain for the admin port [MQTT_ADMIN_TLS_CERT]
    #[arg(long, value_name = "FILE", help_heading = "Admin TLS & auth")]
    pub admin_tls_cert: Option<String>,

    /// PEM private key for the admin port [MQTT_ADMIN_TLS_KEY]
    #[arg(long, value_name = "FILE", help_heading = "Admin TLS & auth")]
    pub admin_tls_key: Option<String>,

    /// PEM CA bundle; enables mutual TLS on the admin port
    /// [MQTT_ADMIN_TLS_CLIENT_CA]
    #[arg(long, value_name = "FILE", help_heading = "Admin TLS & auth")]
    pub admin_tls_client_ca: Option<String>,

    /// Bearer token required for /metrics and /mcp (unset = open)
    /// [MQTT_ADMIN_TOKEN]
    #[arg(long, value_name = "TOKEN", help_heading = "Admin TLS & auth")]
    pub admin_token: Option<String>,

    /// Username/password credential file; when set, clients must authenticate
    /// [MQTT_PASSWORD_FILE]
    #[arg(
        long,
        value_name = "FILE",
        help_heading = "Authentication & authorization"
    )]
    pub password_file: Option<String>,

    /// Allow clients with no credentials when a password file is set
    /// [MQTT_ALLOW_ANONYMOUS] (default false)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = bool_flag,
        help_heading = "Authentication & authorization"
    )]
    pub allow_anonymous: Option<bool>,

    /// JSON ACL policy authorizing publish/subscribe per identity
    /// (unset = allow all) [MQTT_ACL_FILE]
    #[arg(
        long,
        value_name = "FILE",
        help_heading = "Authentication & authorization"
    )]
    pub acl_file: Option<String>,

    /// Read a password from stdin, print a credential line, and exit (helper
    /// for --password-file). With no USERNAME only the hash is printed.
    #[arg(
        long,
        value_name = "USERNAME",
        num_args = 0..=1,
        help_heading = "Authentication & authorization"
    )]
    pub hash_password: Option<Option<String>>,

    /// SQLite database file [MQTT_DB_PATH] (default mqtt_broker.db)
    #[arg(long, value_name = "FILE", help_heading = "Storage & limits")]
    pub db_path: Option<String>,

    /// Maximum accepted packet size [MQTT_MAX_PACKET_SIZE] (default 1048576)
    #[arg(long, value_name = "BYTES", help_heading = "Storage & limits")]
    pub max_packet_size: Option<u32>,

    /// Server Receive Maximum [MQTT_RECEIVE_MAXIMUM] (default 64)
    #[arg(long, value_name = "N", help_heading = "Storage & limits")]
    pub receive_maximum: Option<u16>,

    /// Cap on Session Expiry Interval [MQTT_MAX_SESSION_EXPIRY] (default 3600)
    #[arg(long, value_name = "SECS", help_heading = "Storage & limits")]
    pub max_session_expiry: Option<u32>,

    /// Max queued messages per offline session, 0=unlimited
    /// [MQTT_MAX_QUEUED_MESSAGES] (default 1000)
    #[arg(long, value_name = "N", help_heading = "Storage & limits")]
    pub max_queued_messages: Option<u32>,

    /// $SYS/broker status refresh interval, 0=disable [MQTT_SYS_INTERVAL]
    /// (default 10)
    #[arg(long, value_name = "SECS", help_heading = "Storage & limits")]
    pub sys_interval: Option<u32>,

    /// Highest QoS the server supports [MQTT_MAXIMUM_QOS] (default 2)
    #[arg(
        long,
        value_name = "0|1|2",
        value_parser = qos_flag,
        help_heading = "Protocol capabilities (advertised in CONNACK)"
    )]
    pub maximum_qos: Option<QoS>,

    /// Whether retained messages are supported [MQTT_RETAIN_AVAILABLE]
    /// (default true)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = bool_flag,
        help_heading = "Protocol capabilities (advertised in CONNACK)"
    )]
    pub retain_available: Option<bool>,

    /// Topic Alias Maximum granted to clients [MQTT_TOPIC_ALIAS_MAXIMUM]
    /// (default 16)
    #[arg(
        long,
        value_name = "N",
        help_heading = "Protocol capabilities (advertised in CONNACK)"
    )]
    pub topic_alias_maximum: Option<u16>,

    /// Override the client's Keep Alive [MQTT_SERVER_KEEP_ALIVE]
    /// (default: honour client)
    #[arg(
        long,
        value_name = "SECS",
        help_heading = "Protocol capabilities (advertised in CONNACK)"
    )]
    pub server_keep_alive: Option<u16>,

    /// OTLP collector base URL, e.g. http://127.0.0.1:4318; unset disables
    /// telemetry export [MQTT_OTLP_ENDPOINT]
    #[arg(
        long,
        value_name = "URL",
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_endpoint: Option<String>,

    /// OTLP transport [MQTT_OTLP_PROTOCOL] (default http; this build is
    /// HTTP/protobuf only)
    #[arg(
        long,
        value_name = "http",
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_protocol: Option<String>,

    /// Header sent with every export request, e.g. --otlp-header
    /// "DD-API-KEY=..."; repeatable [MQTT_OTLP_HEADERS]
    #[arg(
        long,
        value_name = "NAME=VALUE",
        value_parser = header_flag,
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_header: Vec<(String, String)>,

    /// Metric export interval [MQTT_OTLP_INTERVAL] (default 60)
    #[arg(
        long,
        value_name = "SECS",
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_interval: Option<u32>,

    /// Export metrics [MQTT_OTLP_METRICS] (default true)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = bool_flag,
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_metrics: Option<bool>,

    /// Export logs [MQTT_OTLP_LOGS] (default true)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = bool_flag,
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub otlp_logs: Option<bool>,

    /// service.name on the exported OTLP resource [MQTT_SERVICE_NAME]
    /// (default pulsemq)
    #[arg(
        long,
        value_name = "NAME",
        help_heading = "Telemetry export (OTLP, requires --features otel)"
    )]
    pub service_name: Option<String>,

    /// Publish Home Assistant MQTT Discovery config + state topics for every
    /// broker statistic [MQTT_HA_DISCOVERY] (default false)
    #[arg(
        long,
        value_name = "BOOL",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = bool_flag,
        help_heading = "Home Assistant MQTT Discovery"
    )]
    pub ha_discovery: Option<bool>,

    /// Home Assistant's discovery topic prefix [MQTT_HA_DISCOVERY_PREFIX]
    /// (default homeassistant)
    #[arg(
        long,
        value_name = "PREFIX",
        help_heading = "Home Assistant MQTT Discovery"
    )]
    pub ha_discovery_prefix: Option<String>,
}

impl Cli {
    /// Overlay the flags the user actually passed onto `cfg`. This is the
    /// highest-precedence layer, so every `Some` wins over the config file and
    /// the environment.
    ///
    /// `--config` and `--hash-password` are absent here on purpose: both are
    /// acted on by `Config::load` before the other layers are read.
    pub(crate) fn apply(&self, cfg: &mut Config) {
        if let Some(v) = self.listen_addr {
            cfg.listen_addr = v;
        }
        if let Some(v) = self.admin_addr {
            cfg.admin_addr = v;
        }
        if let Some(v) = &self.admin_token {
            cfg.admin_token = Some(Secret::new(v.clone()));
        }
        overlay(&mut cfg.tls_cert, &self.tls_cert);
        overlay(&mut cfg.tls_key, &self.tls_key);
        overlay(&mut cfg.tls_client_ca, &self.tls_client_ca);
        if let Some(v) = self.ws_listen_addr {
            cfg.ws_listen_addr = Some(v);
        }
        overlay(&mut cfg.ws_tls_cert, &self.ws_tls_cert);
        overlay(&mut cfg.ws_tls_key, &self.ws_tls_key);
        overlay(&mut cfg.ws_tls_client_ca, &self.ws_tls_client_ca);
        overlay(&mut cfg.admin_tls_cert, &self.admin_tls_cert);
        overlay(&mut cfg.admin_tls_key, &self.admin_tls_key);
        overlay(&mut cfg.admin_tls_client_ca, &self.admin_tls_client_ca);
        overlay(&mut cfg.acl_path, &self.acl_file);
        overlay(&mut cfg.password_file, &self.password_file);
        if let Some(v) = self.allow_anonymous {
            cfg.allow_anonymous = v;
        }
        if let Some(v) = &self.db_path {
            cfg.db_path = v.clone();
        }
        if let Some(v) = self.max_packet_size {
            cfg.max_packet_size = v;
        }
        if let Some(v) = self.receive_maximum {
            cfg.receive_maximum = v;
        }
        if let Some(v) = self.max_session_expiry {
            cfg.max_session_expiry = v;
        }
        if let Some(v) = self.max_queued_messages {
            cfg.max_queued_messages = v;
        }
        if let Some(v) = self.max_connections_per_ip {
            cfg.max_connections_per_ip = v;
        }
        if let Some(v) = self.connection_rate_window_secs {
            cfg.connection_rate_window_secs = v;
        }
        if let Some(v) = self.sys_interval {
            cfg.sys_interval = v;
        }
        if let Some(v) = self.maximum_qos {
            cfg.maximum_qos = v;
        }
        if let Some(v) = self.retain_available {
            cfg.retain_available = v;
        }
        if let Some(v) = self.topic_alias_maximum {
            cfg.topic_alias_maximum = v;
        }
        if let Some(v) = self.server_keep_alive {
            cfg.server_keep_alive = Some(v);
        }
        overlay(&mut cfg.otlp_endpoint, &self.otlp_endpoint);
        if let Some(v) = &self.otlp_protocol {
            cfg.otlp_protocol = v.clone();
        }
        // Repeated flags accumulate, so "not passed" is an empty vec — the same
        // convention the `bridges` config list uses.
        if !self.otlp_header.is_empty() {
            cfg.otlp_headers = OtlpHeaders::from_pairs(self.otlp_header.iter().cloned());
        }
        if let Some(v) = self.otlp_interval {
            cfg.otlp_interval = v;
        }
        if let Some(v) = self.otlp_metrics {
            cfg.otlp_metrics = v;
        }
        if let Some(v) = self.otlp_logs {
            cfg.otlp_logs = v;
        }
        if let Some(v) = &self.service_name {
            cfg.service_name = v.clone();
        }
        if let Some(v) = self.ha_discovery {
            cfg.ha_discovery = v;
        }
        if let Some(v) = &self.ha_discovery_prefix {
            cfg.ha_discovery_prefix = v.clone();
        }
    }
}

/// Overwrite `slot` with `value` when the flag was passed; leave it otherwise.
fn overlay(slot: &mut Option<String>, value: &Option<String>) {
    if let Some(v) = value {
        *slot = Some(v.clone());
    }
}

/// Parse a Maximum QoS flag value. Strict — clap turns the `Err` into a usage
/// error, unlike the env layer which ignores garbage.
fn qos_flag(v: &str) -> Result<QoS, String> {
    parse_qos_value(v).ok_or_else(|| "expected 0, 1, or 2".to_string())
}

/// Parse a boolean flag value, accepting the same spellings as the env and
/// config-file layers so the three agree.
fn bool_flag(v: &str) -> Result<bool, String> {
    parse_bool_value(v).ok_or_else(|| "expected true or false".to_string())
}

/// Parse an `--otlp-header NAME=VALUE` flag. Validating here rather than in
/// [`Cli::apply`] keeps `apply` infallible and turns a malformed header into a
/// clap usage error, like every other bad flag value.
fn header_flag(v: &str) -> Result<(String, String), String> {
    // Split once: a header value may contain '=' (base64 padding, for one).
    let (name, value) = v
        .split_once('=')
        .ok_or_else(|| "expected NAME=VALUE".to_string())?;
    let name = name.trim();
    if name.is_empty() {
        return Err("the header name is empty".to_string());
    }
    Ok((name.to_string(), value.trim().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Startup;
    use clap::CommandFactory;

    /// Parse flags as if they were passed on the command line.
    fn parse(args: &[&str]) -> Cli {
        let mut argv = vec!["pulsemq"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv).expect("flags should parse")
    }

    /// Apply flags on top of the default config.
    fn config_from(args: &[&str]) -> Config {
        let mut cfg = Config::default();
        parse(args).apply(&mut cfg);
        cfg
    }

    #[test]
    fn command_definition_is_valid() {
        // clap's own consistency check: catches duplicate flags, a
        // `default_missing_value` that its parser rejects, and similar
        // mistakes that would otherwise only surface at runtime.
        Cli::command().debug_assert();
    }

    #[test]
    fn cli_overrides_and_parses() {
        let cfg = config_from(&[
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
        ]);
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1");
        assert_eq!(cfg.receive_maximum, 10);
        assert_eq!(cfg.tls_cert.as_deref(), Some("c.pem"));
        assert_eq!(cfg.tls_key.as_deref(), Some("k.pem"));
        assert_eq!(cfg.tls_client_ca.as_deref(), Some("ca.pem"));
    }

    #[test]
    fn equals_form_and_unknown_flag() {
        assert_eq!(config_from(&["--db-path=/tmp/x.db"]).db_path, "/tmp/x.db");
        assert!(Cli::try_parse_from(["pulsemq", "--nope"]).is_err());
        // A flag that needs a value must not silently swallow the next flag.
        assert!(Cli::try_parse_from(["pulsemq", "--db-path"]).is_err());
    }

    #[test]
    fn unset_flags_leave_lower_layers_alone() {
        // The whole point of the `Option` fields: an absent flag must not
        // overwrite a config-file or env value with a CLI default.
        let mut cfg = Config {
            db_path: "from-file.db".to_string(),
            retain_available: false,
            server_keep_alive: Some(30),
            ..Config::default()
        };
        parse(&["--listen-addr", "127.0.0.1:1"]).apply(&mut cfg);
        assert_eq!(cfg.db_path, "from-file.db");
        assert!(!cfg.retain_available);
        assert_eq!(cfg.server_keep_alive, Some(30));
    }

    #[test]
    fn cli_flags_override_config_file() {
        let mut cfg = Config::default();
        cfg.apply_json_str(
            r#"{"listen_addr": "127.0.0.1:1", "receive_maximum": 5,
                "max_queued_messages": 5}"#,
            "t.json",
        )
        .unwrap();
        parse(&[
            "--receive-maximum",
            "42",
            "--max-queued-messages",
            "7",
            "--max-connections-per-ip",
            "20",
        ])
        .apply(&mut cfg);
        assert_eq!(cfg.listen_addr.to_string(), "127.0.0.1:1"); // from file
        assert_eq!(cfg.receive_maximum, 42); // CLI override
        assert_eq!(cfg.max_queued_messages, 7); // CLI override
        assert_eq!(cfg.max_connections_per_ip, 20); // CLI override
    }

    #[test]
    fn cli_beats_env_beats_file() {
        // Proves the full precedence chain in one go. This is the only test
        // that touches the MQTT_* environment, so it cannot race the others.
        std::env::set_var("MQTT_RECEIVE_MAXIMUM", "20");
        std::env::set_var("MQTT_DB_PATH", "from-env.db");

        let mut cfg = Config::default();
        cfg.apply_json_str(
            r#"{"receive_maximum": 5, "db_path": "from-file.db", "sys_interval": 5}"#,
            "t.json",
        )
        .unwrap();
        cfg.apply_env();
        parse(&["--receive-maximum", "42"]).apply(&mut cfg);

        assert_eq!(cfg.receive_maximum, 42); // CLI > env > file
        assert_eq!(cfg.db_path, "from-env.db"); // env > file
        assert_eq!(cfg.sys_interval, 5); // file > default

        std::env::remove_var("MQTT_RECEIVE_MAXIMUM");
        std::env::remove_var("MQTT_DB_PATH");
    }

    #[test]
    fn protocol_capabilities() {
        let cfg = config_from(&[
            "--maximum-qos",
            "0",
            "--retain-available",
            "false",
            "--topic-alias-maximum",
            "5",
            "--server-keep-alive",
            "45",
        ]);
        assert_eq!(cfg.maximum_qos, QoS::AtMostOnce);
        assert!(!cfg.retain_available);
        assert_eq!(cfg.topic_alias_maximum, 5);
        assert_eq!(cfg.server_keep_alive, Some(45));

        // Out-of-range and non-numeric QoS are usage errors, not silent defaults.
        assert!(Cli::try_parse_from(["pulsemq", "--maximum-qos", "9"]).is_err());
        assert!(Cli::try_parse_from(["pulsemq", "--maximum-qos", "high"]).is_err());
        // clap's typed parsing rejects out-of-range integers too.
        assert!(Cli::try_parse_from(["pulsemq", "--receive-maximum", "70000"]).is_err());
        assert!(Cli::try_parse_from(["pulsemq", "--listen-addr", "not-an-addr"]).is_err());
    }

    #[test]
    fn bool_flags_accept_spellings_and_a_bare_form() {
        assert!(config_from(&["--allow-anonymous", "yes"]).allow_anonymous);
        assert!(!config_from(&["--allow-anonymous=off"]).allow_anonymous);
        // Bare `--allow-anonymous` means true, which the hand-rolled parser
        // rejected as a missing value. Passing a value still works, so every
        // pre-clap invocation keeps its meaning.
        assert!(config_from(&["--allow-anonymous"]).allow_anonymous);
        assert!(!config_from(&["--retain-available", "0"]).retain_available);
        assert!(Cli::try_parse_from(["pulsemq", "--allow-anonymous", "maybe"]).is_err());
    }

    #[test]
    fn hash_password_takes_an_optional_username() {
        assert_eq!(parse(&["--hash-password"]).hash_password, Some(None));
        assert_eq!(
            parse(&["--hash-password", "alice"]).hash_password,
            Some(Some("alice".to_string()))
        );
        assert_eq!(parse(&[]).hash_password, None);
        // A following flag is not mistaken for the username.
        let cli = parse(&["--hash-password", "--db-path", "x.db"]);
        assert_eq!(cli.hash_password, Some(None));
        assert_eq!(cli.db_path.as_deref(), Some("x.db"));
    }

    #[test]
    fn otlp_flags_apply() {
        let cfg = config_from(&[
            "--otlp-endpoint",
            "http://127.0.0.1:4318",
            "--otlp-interval",
            "5",
            "--otlp-logs",
            "false",
            "--service-name",
            "edge-1",
            "--otlp-header",
            "DD-API-KEY=abc",
            "--otlp-header",
            "X-Extra=1",
        ]);
        assert_eq!(cfg.otlp_endpoint.as_deref(), Some("http://127.0.0.1:4318"));
        assert_eq!(cfg.otlp_interval, 5);
        assert!(!cfg.otlp_logs);
        assert!(cfg.otlp_metrics); // untouched flag keeps its default
        assert_eq!(cfg.service_name, "edge-1");
        assert_eq!(
            cfg.otlp_headers.iter().collect::<Vec<_>>(),
            vec![("DD-API-KEY", "abc"), ("X-Extra", "1")]
        );
    }

    #[test]
    fn ha_discovery_flags_apply() {
        let cfg = config_from(&["--ha-discovery", "--ha-discovery-prefix", "hass"]);
        assert!(cfg.ha_discovery);
        assert_eq!(cfg.ha_discovery_prefix, "hass");

        // Untouched flags keep the layer below's default.
        let cfg = config_from(&[]);
        assert!(!cfg.ha_discovery);
        assert_eq!(cfg.ha_discovery_prefix, "homeassistant");
    }

    #[test]
    fn malformed_otlp_header_is_a_usage_error() {
        // Validated by clap's value parser, so a typo fails at startup with a
        // usage message instead of quietly exporting without the API key.
        assert!(Cli::try_parse_from(["pulsemq", "--otlp-header", "no-equals"]).is_err());
        assert!(Cli::try_parse_from(["pulsemq", "--otlp-header", "=novalue"]).is_err());
    }

    #[test]
    fn otlp_headers_do_not_stomp_lower_layers_when_unset() {
        // Repeated flags accumulate, so "absent" is an empty vec rather than a
        // `None` — the emptiness check is what preserves the config file.
        let mut cfg = Config {
            otlp_headers: OtlpHeaders::from_pairs([(
                "DD-API-KEY".to_string(),
                "from-file".to_string(),
            )]),
            ..Config::default()
        };
        parse(&["--otlp-endpoint", "http://127.0.0.1:4318"]).apply(&mut cfg);
        assert_eq!(
            cfg.otlp_headers.iter().collect::<Vec<_>>(),
            vec![("DD-API-KEY", "from-file")]
        );
    }

    #[test]
    fn help_and_version_are_generated_by_clap() {
        use clap::error::ErrorKind;
        for (flag, kind) in [
            ("--help", ErrorKind::DisplayHelp),
            ("-h", ErrorKind::DisplayHelp),
            ("--version", ErrorKind::DisplayVersion),
            ("-V", ErrorKind::DisplayVersion),
        ] {
            let err = Cli::try_parse_from(["pulsemq", flag]).expect_err("should not run");
            assert_eq!(err.kind(), kind, "{flag}");
        }
        // The version string is the crate version, not a hand-maintained one.
        let rendered = Cli::try_parse_from(["pulsemq", "--version"])
            .expect_err("should not run")
            .to_string();
        assert!(rendered.contains(crate::config::VERSION), "{rendered}");
    }

    #[test]
    fn startup_carries_the_hash_password_request() {
        // `Config::load` reads the real argv, so the routing from flag to
        // `Startup` is asserted on the pieces it composes.
        let cli = parse(&["--hash-password", "bob"]);
        let startup = match &cli.hash_password {
            Some(user) => Startup::HashPassword(user.clone()),
            None => Startup::Run(Box::default()),
        };
        match startup {
            Startup::HashPassword(user) => assert_eq!(user.as_deref(), Some("bob")),
            Startup::Run(_) => panic!("expected the hash-password path"),
        }
    }
}
