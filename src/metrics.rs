//! Broker metrics: cumulative counters exposed to Prometheus and the MCP
//! server. Gauges (current clients, sessions, retained messages, …) are
//! computed on demand from broker state — see `Broker::snapshot`.

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide counters. Cheap, lock-free, incremented on the hot path.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Accepted connections since start (successful CONNECT).
    pub connections_total: AtomicU64,
    /// Control packets received from clients.
    pub packets_received: AtomicU64,
    /// Control packets sent to clients.
    pub packets_sent: AtomicU64,
    /// Bytes read off client sockets (whole frames).
    pub bytes_received: AtomicU64,
    /// Bytes written to client sockets (whole frames).
    pub bytes_sent: AtomicU64,
    /// PUBLISH packets received from clients.
    pub publish_received: AtomicU64,
    /// PUBLISH deliveries fanned out to subscribers.
    pub publish_delivered: AtomicU64,
}

impl Metrics {
    #[inline]
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

/// A point-in-time view combining counters and computed gauges.
#[derive(Debug, Clone)]
pub struct Snapshot {
    // Counters.
    pub connections_total: u64,
    pub packets_received: u64,
    pub packets_sent: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub publish_received: u64,
    pub publish_delivered: u64,
    // Gauges.
    pub clients_connected: u64,
    pub sessions_total: u64,
    pub retained_messages: u64,
    pub subscriptions_total: u64,
}

impl Snapshot {
    /// Render the snapshot in the Prometheus text exposition format (v0.0.4).
    pub fn to_prometheus(&self) -> String {
        let mut o = String::with_capacity(2048);
        let mut counter = |name: &str, help: &str, val: u64| {
            o.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {val}\n"
            ));
        };
        counter(
            "mqtt_connections_total",
            "Total accepted client connections.",
            self.connections_total,
        );
        counter(
            "mqtt_packets_received_total",
            "Total MQTT control packets received.",
            self.packets_received,
        );
        counter(
            "mqtt_packets_sent_total",
            "Total MQTT control packets sent.",
            self.packets_sent,
        );
        counter(
            "mqtt_bytes_received_total",
            "Total bytes received from clients.",
            self.bytes_received,
        );
        counter(
            "mqtt_bytes_sent_total",
            "Total bytes sent to clients.",
            self.bytes_sent,
        );
        counter(
            "mqtt_publish_received_total",
            "Total PUBLISH packets received from clients.",
            self.publish_received,
        );
        counter(
            "mqtt_publish_delivered_total",
            "Total PUBLISH deliveries to subscribers.",
            self.publish_delivered,
        );

        let mut gauge = |name: &str, help: &str, val: u64| {
            o.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {val}\n"
            ));
        };
        gauge(
            "mqtt_clients_connected",
            "Currently connected clients.",
            self.clients_connected,
        );
        gauge(
            "mqtt_sessions_total",
            "Sessions currently held (online and offline).",
            self.sessions_total,
        );
        gauge(
            "mqtt_retained_messages",
            "Retained messages currently stored.",
            self.retained_messages,
        );
        gauge(
            "mqtt_subscriptions_total",
            "Active subscriptions across all sessions.",
            self.subscriptions_total,
        );
        o
    }
}
