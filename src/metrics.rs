//! Broker metrics: cumulative counters exposed to Prometheus and the MCP
//! server. Gauges (current clients, sessions, retained messages, …) are
//! computed on demand from broker state — see `Broker::snapshot`.
//!
//! Statistics are collected once into a [`Snapshot`] and rendered twice: as
//! Prometheus text ([`Snapshot::to_prometheus`]) and as mosquitto-style
//! `$SYS/broker/...` MQTT topics ([`Snapshot::to_sys_topics`]). Both surfaces
//! read the same struct, so they cannot report different numbers for the same
//! instant.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::PacketType;

/// Number of slots in the per-control-packet arrays. Packet types are 1..=15
/// (§2.1.2); index 0 is unused so a `PacketType` can index directly.
pub const PACKET_KINDS: usize = 16;

/// Lower-case names for the per-packet series/topics, indexed by packet type.
pub const PACKET_NAMES: [&str; PACKET_KINDS] = [
    "reserved",
    "connect",
    "connack",
    "publish",
    "puback",
    "pubrec",
    "pubrel",
    "pubcomp",
    "subscribe",
    "suback",
    "unsubscribe",
    "unsuback",
    "pingreq",
    "pingresp",
    "disconnect",
    "auth",
];

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
    /// Messages dropped instead of queued because an offline session's queue
    /// was at `max_queued_messages`.
    pub publish_dropped: AtomicU64,
    /// Messages forwarded out to a bridged remote broker.
    pub bridge_forwarded_out: AtomicU64,
    /// Messages received from a bridged remote and injected locally.
    pub bridge_forwarded_in: AtomicU64,
    /// Currently-connected bridges (gauge: inc on connect, dec on disconnect).
    pub bridges_connected: AtomicU64,
    /// PUBLISH payload bytes received (distinct from whole-frame `bytes_*`).
    pub publish_bytes_received: AtomicU64,
    /// PUBLISH payload bytes sent to subscribers.
    pub publish_bytes_sent: AtomicU64,
    /// TCP/TLS/WebSocket sockets accepted, whether or not a CONNECT followed.
    /// Higher than `connections_total` when clients disconnect during the
    /// handshake or fail authentication.
    pub socket_connections: AtomicU64,
    /// High-water mark of concurrently connected clients.
    pub clients_maximum: AtomicU64,
    /// Persistent sessions discarded because their Session Expiry elapsed.
    pub clients_expired: AtomicU64,
    /// Per-control-packet counters, indexed by packet type (1..=15).
    pub packet_received: [AtomicU64; PACKET_KINDS],
    pub packet_sent: [AtomicU64; PACKET_KINDS],
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

    #[inline]
    pub fn dec(counter: &AtomicU64) {
        counter.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a received control packet: the per-type counter plus the totals.
    #[inline]
    pub fn record_received(&self, kind: PacketType, frame_bytes: usize) {
        Self::inc(&self.packets_received);
        Self::add(&self.bytes_received, frame_bytes as u64);
        Self::inc(&self.packet_received[kind as usize]);
    }

    /// Record a sent control packet: the per-type counter plus the totals.
    #[inline]
    pub fn record_sent(&self, kind: PacketType, frame_bytes: usize) {
        Self::inc(&self.packets_sent);
        Self::add(&self.bytes_sent, frame_bytes as u64);
        Self::inc(&self.packet_sent[kind as usize]);
    }

    /// Raise the connected-clients high-water mark to at least `current`.
    #[inline]
    pub fn observe_clients_connected(&self, current: u64) {
        // Compare-and-swap loop rather than a plain store: several connection
        // tasks can observe different counts concurrently and the maximum must
        // not be walked backwards by a slower one.
        let mut seen = Self::get(&self.clients_maximum);
        while current > seen {
            match self.clients_maximum.compare_exchange_weak(
                seen,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => seen = actual,
            }
        }
    }

    /// Borrow the accepted-sockets counter (used from the accept loops).
    pub fn socket_connections_ref(&self) -> &AtomicU64 {
        &self.socket_connections
    }

    /// Read the per-packet array into a plain array for a `Snapshot`.
    pub fn read_array_pub(src: &[AtomicU64; PACKET_KINDS]) -> [u64; PACKET_KINDS] {
        let mut out = [0u64; PACKET_KINDS];
        for (slot, counter) in out.iter_mut().zip(src.iter()) {
            *slot = Self::get(counter);
        }
        out
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
    pub publish_dropped: u64,
    pub bridge_forwarded_out: u64,
    pub bridge_forwarded_in: u64,
    pub publish_bytes_received: u64,
    pub publish_bytes_sent: u64,
    pub socket_connections: u64,
    pub clients_expired: u64,
    /// Per-control-packet counters indexed by packet type; see [`PACKET_NAMES`].
    pub packet_received: [u64; PACKET_KINDS],
    pub packet_sent: [u64; PACKET_KINDS],
    // Gauges.
    pub clients_connected: u64,
    pub clients_disconnected: u64,
    pub clients_total: u64,
    pub clients_maximum: u64,
    pub sessions_total: u64,
    pub retained_messages: u64,
    pub retained_bytes: u64,
    pub subscriptions_total: u64,
    pub shared_subscriptions_count: u64,
    pub bridges_connected: u64,
    /// Messages held in memory (queued for offline sessions plus retained).
    pub store_messages_count: u64,
    pub store_messages_bytes: u64,
    /// Messages queued for delivery across all sessions (backpressure signal).
    pub packet_out_count: u64,
    pub packet_out_bytes: u64,
    pub uptime_seconds: u64,
    /// Crate version, rendered as a static label / `$SYS/broker/version`.
    pub version: &'static str,
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
        counter(
            "mqtt_publish_dropped_total",
            "Total messages dropped because an offline session's queue was full.",
            self.publish_dropped,
        );
        counter(
            "mqtt_bridge_forwarded_out_total",
            "Total messages forwarded out to bridged remote brokers.",
            self.bridge_forwarded_out,
        );
        counter(
            "mqtt_bridge_forwarded_in_total",
            "Total messages received from bridged remotes and injected locally.",
            self.bridge_forwarded_in,
        );
        // mosquitto reports "messages" as all MQTT control packets and
        // "publish/messages" as PUBLISH only. The former is the same quantity
        // as packets_*, so it is aliased rather than counted twice.
        counter(
            "mqtt_messages_received_total",
            "Total MQTT messages (all control packets) received.",
            self.packets_received,
        );
        counter(
            "mqtt_messages_sent_total",
            "Total MQTT messages (all control packets) sent.",
            self.packets_sent,
        );
        counter(
            "mqtt_publish_sent_total",
            "Total PUBLISH packets sent to clients.",
            self.packet_sent[PacketType::Publish as usize],
        );
        counter(
            "mqtt_publish_bytes_received_total",
            "Total PUBLISH payload bytes received.",
            self.publish_bytes_received,
        );
        counter(
            "mqtt_publish_bytes_sent_total",
            "Total PUBLISH payload bytes sent.",
            self.publish_bytes_sent,
        );
        counter(
            "mqtt_socket_connections_total",
            "Total sockets accepted (including those that never sent CONNECT).",
            self.socket_connections,
        );
        counter(
            "mqtt_clients_expired_total",
            "Total persistent sessions discarded after Session Expiry elapsed.",
            self.clients_expired,
        );

        // Per-control-packet counters. Index 0 is the reserved type and is
        // never a real packet, so it is skipped.
        for (kind, name) in PACKET_NAMES.iter().enumerate().skip(1) {
            counter(
                &format!("mqtt_{name}_received_total"),
                &format!("Total {} packets received.", name.to_uppercase()),
                self.packet_received[kind],
            );
            counter(
                &format!("mqtt_{name}_sent_total"),
                &format!("Total {} packets sent.", name.to_uppercase()),
                self.packet_sent[kind],
            );
        }

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
        gauge(
            "mqtt_bridges_connected",
            "Currently-connected broker-to-broker bridges.",
            self.bridges_connected,
        );
        gauge(
            "mqtt_clients_disconnected",
            "Persistent sessions currently offline.",
            self.clients_disconnected,
        );
        gauge(
            "mqtt_clients_total",
            "Clients known to the broker (connected plus offline persistent).",
            self.clients_total,
        );
        gauge(
            "mqtt_clients_maximum",
            "High-water mark of concurrently connected clients.",
            self.clients_maximum,
        );
        gauge(
            "mqtt_retained_bytes",
            "Payload bytes held in retained messages.",
            self.retained_bytes,
        );
        gauge(
            "mqtt_shared_subscriptions_count",
            "Active shared subscriptions ($share/...) across all sessions.",
            self.shared_subscriptions_count,
        );
        gauge(
            "mqtt_store_messages_count",
            "Messages held in memory (queued for offline sessions plus retained).",
            self.store_messages_count,
        );
        gauge(
            "mqtt_store_messages_bytes",
            "Payload bytes of messages held in memory.",
            self.store_messages_bytes,
        );
        gauge(
            "mqtt_packet_out_count",
            "Messages queued for delivery across all sessions.",
            self.packet_out_count,
        );
        gauge(
            "mqtt_packet_out_bytes",
            "Payload bytes queued for delivery across all sessions.",
            self.packet_out_bytes,
        );
        gauge(
            "mqtt_uptime_seconds",
            "Seconds since the broker started.",
            self.uptime_seconds,
        );
        // Version as a static label on a constant-1 gauge, the conventional way
        // to expose build info to Prometheus.
        o.push_str(
            "# HELP mqtt_build_info Broker build information.\n\
             # TYPE mqtt_build_info gauge\n",
        );
        o.push_str(&format!(
            "mqtt_build_info{{version=\"{}\"}} 1\n",
            self.version
        ));
        o
    }

    /// Render the snapshot as mosquitto-compatible `$SYS/broker/...` topic /
    /// payload pairs.
    ///
    /// Topic names follow mosquitto's hierarchy so existing dashboards and
    /// `mosquitto_sub -t '$SYS/#'` habits carry over. The values are the same
    /// fields [`Snapshot::to_prometheus`] renders, so the two surfaces agree by
    /// construction.
    pub fn to_sys_topics(&self) -> Vec<(String, String)> {
        let mut t: Vec<(String, String)> = Vec::with_capacity(64);
        let mut add = |topic: &str, value: String| t.push((format!("$SYS/broker/{topic}"), value));

        add("version", format!("PulseMQ {}", self.version));
        add("uptime", format!("{} seconds", self.uptime_seconds));

        add("clients/connected", self.clients_connected.to_string());
        add(
            "clients/disconnected",
            self.clients_disconnected.to_string(),
        );
        add("clients/total", self.clients_total.to_string());
        add("clients/maximum", self.clients_maximum.to_string());
        add("clients/expired", self.clients_expired.to_string());

        add("bytes/received", self.bytes_received.to_string());
        add("bytes/sent", self.bytes_sent.to_string());
        add("messages/received", self.packets_received.to_string());
        add("messages/sent", self.packets_sent.to_string());

        add(
            "publish/messages/received",
            self.publish_received.to_string(),
        );
        add(
            "publish/messages/sent",
            self.packet_sent[PacketType::Publish as usize].to_string(),
        );
        add("publish/messages/dropped", self.publish_dropped.to_string());
        add(
            "publish/bytes/received",
            self.publish_bytes_received.to_string(),
        );
        add("publish/bytes/sent", self.publish_bytes_sent.to_string());

        add("subscriptions/count", self.subscriptions_total.to_string());
        add(
            "shared_subscriptions/count",
            self.shared_subscriptions_count.to_string(),
        );
        add(
            "retained messages/count",
            self.retained_messages.to_string(),
        );
        add(
            "store/messages/count",
            self.store_messages_count.to_string(),
        );
        add(
            "store/messages/bytes",
            self.store_messages_bytes.to_string(),
        );

        add(
            "connections/socket/count",
            self.socket_connections.to_string(),
        );
        add("bridges/connected", self.bridges_connected.to_string());

        for (kind, name) in PACKET_NAMES.iter().enumerate().skip(1) {
            add(
                &format!("mqtt/{name}/received"),
                self.packet_received[kind].to_string(),
            );
            add(
                &format!("mqtt/{name}/sent"),
                self.packet_sent[kind].to_string(),
            );
        }
        t
    }
}
