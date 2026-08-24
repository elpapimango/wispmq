//! The `$SYS/broker/...` broker-status publisher (mosquitto-compatible).
//!
//! Broker statistics are exposed two ways from one source of truth: scraped as
//! Prometheus text on the admin port, and published into the MQTT topic space
//! here so a plain MQTT client can read them with
//! `mosquitto_sub -t '$SYS/#' -v`. Both render the same
//! [`Snapshot`](crate::metrics::Snapshot); see `metrics.rs`.
//!
//! Values are published **retained**, so a client subscribing between ticks
//! gets the current value immediately instead of waiting up to `sys_interval`
//! seconds. They are not persisted — see `Broker::publish_sys`.
//!
//! Two spec details make this safe:
//! - A filter of `#` or `+` does not match a topic beginning with `$`
//!   (§4.7.2), so `$SYS` reaches only clients that ask for it explicitly and
//!   ordinary wildcard subscribers are unaffected.
//! - Clients are refused permission to publish under `$SYS` (see
//!   `broker::handle_publish`), so these values cannot be forged.
//!
//! When `ha_discovery` is set, this loop also publishes Home Assistant MQTT
//! Discovery config (once, at startup) and state topics (every tick) built
//! from the same [`crate::metrics::Snapshot`] — see
//! [`crate::metrics::Snapshot::to_ha_discovery`] and `to_ha_states`. It rides
//! this timer rather than a separate task because it needs the same data on
//! the same cadence; `sys_interval: 0` disables both.

use std::time::Duration;

use crate::broker::Broker;

/// Run the periodic `$SYS` (and, if enabled, Home Assistant discovery)
/// publisher until the process exits.
///
/// Returns immediately when `sys_interval` is 0, which disables both.
pub async fn run(broker: Broker) {
    let interval = broker.config().sys_interval;
    if interval == 0 {
        tracing::debug!("sys_interval is 0; $SYS/broker status topics are disabled");
        return;
    }

    let period = Duration::from_secs(interval as u64);
    tracing::info!(
        "publishing $SYS/broker status topics every {interval}s (subscribe to '$SYS/#')"
    );

    let ha_discovery = broker.config().ha_discovery;
    if ha_discovery {
        publish_ha_discovery(&broker);
    }

    // Publish once immediately so the topics exist without waiting a full
    // interval, then settle into the timer.
    publish_once(&broker, ha_discovery);

    let mut ticker = tokio::time::interval(period);
    // The immediate publish above already covered this tick.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        publish_once(&broker, ha_discovery);
    }
}

fn publish_once(broker: &Broker, ha_discovery: bool) {
    let snapshot = broker.snapshot();
    let mut entries = snapshot.to_sys_topics();
    if ha_discovery {
        entries.extend(snapshot.to_ha_states(&broker.config().service_name));
    }
    broker.publish_sys(&entries);
}

/// Publish Home Assistant discovery config once. Not part of the periodic
/// tick: the config is static (it only changes across a version upgrade,
/// already covered by `sw_version`), so republishing it every `sys_interval`
/// would just be wasted retained-message churn.
fn publish_ha_discovery(broker: &Broker) {
    let cfg = broker.config();
    let entries = broker
        .snapshot()
        .to_ha_discovery(&cfg.ha_discovery_prefix, &cfg.service_name);
    tracing::info!(
        "published {} Home Assistant discovery configs under '{}/sensor/{}/...'",
        entries.len(),
        cfg.ha_discovery_prefix,
        cfg.service_name
    );
    broker.publish_sys(&entries);
}
