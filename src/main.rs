//! MQTT v5.0 broker executable.
//!
//! Configuration is read from environment variables (see `config::Config`).
//! State is persisted to SQLite and reloaded on startup.

use mqtt_server::acl::Acl;
use mqtt_server::admin;
use mqtt_server::broker::Broker;
use mqtt_server::config::{Config, Startup};
use mqtt_server::error::Result;
use mqtt_server::server;
use mqtt_server::storage::Storage;

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI (over env) before anything else so --help/--version print
    // cleanly and bad arguments fail fast without log noise.
    let config = match Config::from_env_and_args() {
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
            tracing::warn!("no ACL file configured; all clients may publish/subscribe to any topic");
            Acl::permit_all()
        }
    };

    let broker = Broker::new(config, storage, loaded, acl);

    // Admin/metrics/MCP HTTP server on its own port.
    let admin_broker = broker.clone();
    tokio::spawn(async move {
        if let Err(e) = admin::run(admin_broker).await {
            tracing::error!("admin server stopped: {e}");
        }
    });

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
