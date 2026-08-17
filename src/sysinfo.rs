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

use std::time::Duration;

use crate::broker::Broker;

/// Run the periodic `$SYS` publisher until the process exits.
///
/// Returns immediately when `sys_interval` is 0, which disables the feature.
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

    // Publish once immediately so the topics exist without waiting a full
    // interval, then settle into the timer.
    publish_once(&broker);

    let mut ticker = tokio::time::interval(period);
    // The immediate publish above already covered this tick.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        publish_once(&broker);
    }
}

fn publish_once(broker: &Broker) {
    let snapshot = broker.snapshot();
    let entries = snapshot.to_sys_topics();
    broker.publish_sys(&entries);
}
