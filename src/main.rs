//! MQTT v5.0 broker executable.
//!
//! Configuration is layered: a YAML config file, then environment variables,
//! then command-line flags (see `config::Config`). State is persisted to
//! SQLite and reloaded on startup.

use pulsemq::acl::Acl;
use pulsemq::admin;
use pulsemq::auth::{self, Credentials};
use pulsemq::broker::Broker;
use pulsemq::config::{Config, Startup};
use pulsemq::error::Result;
use pulsemq::server;
use pulsemq::storage::Storage;

#[tokio::main]
async fn main() -> Result<()> {
    // `--hash-password [username]`: print a credential-file line and exit.
    let raw_args: Vec<String> = std::env::args().collect();
    if let Some(pos) = raw_args.iter().position(|a| a == "--hash-password") {
        let username = raw_args.get(pos + 1).filter(|s| !s.starts_with('-'));
        return hash_password_cmd(username.map(String::as_str));
    }

    // Resolve configuration (file < env < CLI) before anything else so
    // --help/--version print cleanly and bad input fails fast without log noise.
    let config = match Config::load() {
        Ok(Startup::Run(cfg)) => *cfg,
        Ok(Startup::Exit) => return Ok(()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    // Logging: honour RUST_LOG, default to `info`.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Some(path) = &config.config_file {
        tracing::info!("loaded config file {path}");
    }

    tracing::info!(
        "starting broker: listen={} db={} max_packet={}B receive_max={} max_qos={:?}",
        config.listen_addr,
        config.db_path,
        config.max_packet_size,
        config.receive_maximum,
        config.maximum_qos,
    );

    // Open persistence and reload durable state.
    let (storage, loaded) = Storage::open(&config.db_path)?;
    tracing::info!(
        "loaded {} retained message(s), {} persistent session(s) from {}",
        loaded.retained.len(),
        loaded.sessions.len(),
        config.db_path,
    );

    // Load the authorization policy (ACL). Absent => allow all.
    let acl = match &config.acl_path {
        Some(path) => {
            let acl = Acl::load(path)?;
            tracing::info!("loaded ACL policy from {path}");
            acl
        }
        None => {
            tracing::warn!(
                "no ACL file configured; all clients may publish/subscribe to any topic"
            );
            Acl::permit_all()
        }
    };

    // Load username/password credentials. Absent => no password auth.
    let credentials = match &config.password_file {
        Some(path) => {
            let creds = Credentials::load(path)?;
            tracing::info!(
                "loaded {} credential(s) from {path}; password authentication required{}",
                creds.user_count(),
                if config.allow_anonymous {
                    " (anonymous allowed)"
                } else {
                    ""
                }
            );
            Some(creds)
        }
        None => None,
    };

    let broker = Broker::new(config, storage, loaded, acl, credentials);

    // Admin/metrics/MCP HTTP server on its own port.
    let admin_broker = broker.clone();
    tokio::spawn(async move {
        if let Err(e) = admin::run(admin_broker).await {
            tracing::error!("admin server stopped: {e}");
        }
    });

    // MQTT-over-WebSocket listener, if configured.
    if broker.config().ws_listen_addr.is_some() {
        let ws_broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = server::run_ws(ws_broker).await {
                tracing::error!("WebSocket listener stopped: {e}");
            }
        });
    }

    // Broker-to-broker forwarding bridges, if any are configured.
    for bridge_cfg in broker.config().bridges.clone() {
        tracing::info!(
            "starting bridge {:?} -> {}",
            bridge_cfg.name,
            bridge_cfg.address
        );
        let bridge_broker = broker.clone();
        tokio::spawn(pulsemq::bridge::run(bridge_broker, bridge_cfg));
    }

    // Reload the ACL policy on SIGHUP (Unix). The rest of the broker keeps
    // running; a bad policy file is reported and the previous one is kept.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let hup_broker = broker.clone();
        tokio::spawn(async move {
            let mut hup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("cannot install SIGHUP handler: {e}");
                    return;
                }
            };
            while hup.recv().await.is_some() {
                match hup_broker.reload_acl() {
                    Ok(true) => tracing::info!("SIGHUP: ACL policy reloaded"),
                    Ok(false) => {
                        tracing::info!("SIGHUP: no ACL file configured, nothing to reload")
                    }
                    Err(e) => tracing::error!("SIGHUP: ACL reload failed, keeping previous: {e}"),
                }
            }
        });
    }

    // Serve MQTT until Ctrl-C.
    tokio::select! {
        res = server::run(broker) => res,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal received, exiting");
            Ok(())
        }
    }
}

/// Implements `--hash-password [username]`: hash a password (from the
/// `MQTT_HASH_PASSWORD` env var, else read from stdin) and print a line ready to
/// append to a `--password-file`.
fn hash_password_cmd(username: Option<&str>) -> Result<()> {
    use std::io::Read;
    let password = match std::env::var("MQTT_HASH_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprint!("Password: ");
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(pulsemq::error::MqttError::Io)?;
            buf.trim_end_matches(['\r', '\n']).to_string()
        }
    };
    match username {
        Some(user) => println!("{}", auth::format_entry(user, password.as_bytes())),
        None => println!("{}", auth::hash_password(password.as_bytes())),
    }
    Ok(())
}
