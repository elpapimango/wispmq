//! MQTT v5.0 broker executable.
//!
//! Configuration is layered: a TOML config file, then environment variables,
//! then command-line flags (see `config::Config`). State is persisted to
//! SQLite and reloaded on startup.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use wispmq::acl::Acl;
use wispmq::admin;
use wispmq::auth::{self, Credentials};
use wispmq::broker::Broker;
use wispmq::config::{Config, Startup};
use wispmq::error::Result;
use wispmq::otel;
use wispmq::server;
use wispmq::storage::Storage;

#[tokio::main]
async fn main() -> Result<()> {
    // Resolve configuration (file < env < CLI) before anything else so
    // --help/--version print cleanly and bad input fails fast without log
    // noise. `--hash-password` is answered here too, before any state is opened.
    let config = match Config::load() {
        Ok(Startup::Run(cfg)) => *cfg,
        Ok(Startup::HashPassword(user)) => return hash_password_cmd(user.as_deref()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    // Telemetry export, if configured. Installed before the subscriber because
    // it contributes a layer to it; it reports itself afterwards, once there is
    // somewhere for a log line to go.
    let (otlp_layer, mut telemetry) = match otel::install(&config) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    // Logging: honour RUST_LOG, default to `info`. The OTLP layer goes on
    // first: it is boxed against a bare `Registry`, and `EnvFilter` is a global
    // filter, so it applies to console and exported records alike wherever it
    // sits in the stack.
    tracing_subscriber::registry()
        .with(otlp_layer)
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Route panics through `tracing` (same sink as everything else — stdout in
    // Docker) instead of relying solely on the default hook's raw stderr
    // write, which is easy to miss in a rotated/truncated container log.
    std::panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map(|l| l.to_string())
            .unwrap_or_else(|| "unknown location".into());
        tracing::error!(
            "panic at {location}: {info}\n{}",
            std::backtrace::Backtrace::capture()
        );
    }));

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

    // Metric export needs the broker to snapshot, so it starts here rather than
    // alongside the log pipeline above. `report` waits until both halves exist,
    // or it would claim metrics were off while they were still being set up.
    otel::install_metrics(&broker, &mut telemetry, broker.config())?;
    telemetry.report();

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
        tokio::spawn(wispmq::bridge::run(bridge_broker, bridge_cfg));
    }

    // Periodic $SYS/broker status topics (no-op when sys_interval is 0).
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));

    // Periodic connection-rate-limiter cleanup (no-op when disabled).
    tokio::spawn(wispmq::ratelimit::run(broker.clone()));

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

    // Serve MQTT until Ctrl-C or, on Unix, SIGTERM — the signal `docker stop`
    // and most orchestrators send. Without an explicit handler SIGTERM's
    // default disposition kills the process before any log line is written,
    // which is why a stopped container can otherwise show no reason at all.
    #[cfg(unix)]
    let outcome = {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            res = server::run(broker) => {
                if let Err(e) = &res {
                    tracing::error!("MQTT listener stopped with error: {e}");
                }
                res
            },
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT (Ctrl-C) received, shutting down");
                Ok(())
            },
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, shutting down");
                Ok(())
            },
        }
    };
    #[cfg(not(unix))]
    let outcome = tokio::select! {
        res = server::run(broker) => {
            if let Err(e) = &res {
                tracing::error!("MQTT listener stopped with error: {e}");
            }
            res
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT (Ctrl-C) received, shutting down");
            Ok(())
        },
    };
    if let Err(e) = &outcome {
        tracing::error!("broker exiting with error: {e}");
    } else {
        tracing::info!("broker shutdown complete");
    }
    // Flush the last telemetry batch — the one covering the shutdown itself.
    telemetry.shutdown();
    outcome
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
                .map_err(wispmq::error::MqttError::Io)?;
            buf.trim_end_matches(['\r', '\n']).to_string()
        }
    };
    match username {
        Some(user) => println!("{}", auth::format_entry(user, password.as_bytes())),
        None => println!("{}", auth::hash_password(password.as_bytes())),
    }
    Ok(())
}
