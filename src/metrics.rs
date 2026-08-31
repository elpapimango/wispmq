//! Broker metrics: cumulative counters exposed to Prometheus and the MCP
//! server. Gauges (current clients, sessions, retained messages, …) are
//! computed on demand from broker state — see `Broker::snapshot`.
//!
//! Statistics are collected once into a [`Snapshot`] and rendered several
//! ways: as Prometheus text ([`Snapshot::to_prometheus`]), as mosquitto-style
//! `$SYS/broker/...` MQTT topics ([`Snapshot::to_sys_topics`]), and as Home
//! Assistant MQTT Discovery config + state topics
//! ([`Snapshot::to_ha_discovery`], [`Snapshot::to_ha_states`]). All read the
//! same struct, so they cannot report different numbers for the same instant.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

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
    /// Control packets received from clients and from bridge remotes.
    pub packets_received: AtomicU64,
    /// Control packets sent to clients and to bridge remotes.
    pub packets_sent: AtomicU64,
    /// Bytes read off client sockets (whole frames).
    pub bytes_received: AtomicU64,
    /// Bytes written to client sockets (whole frames).
    pub bytes_sent: AtomicU64,
    /// PUBLISH packets received from clients and from bridge remotes.
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
    /// Connections refused by the per-source-IP rate limiter
    /// (`max_connections_per_ip`), before any packet was read.
    pub connections_rate_limited: AtomicU64,
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
    pub(crate) fn socket_connections_ref(&self) -> &AtomicU64 {
        &self.socket_connections
    }

    /// Read the per-packet array into a plain array for a `Snapshot`.
    pub(crate) fn read_array(src: &[AtomicU64; PACKET_KINDS]) -> [u64; PACKET_KINDS] {
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
    pub connections_rate_limited: u64,
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

/// Whether a [`Series`] accumulates monotonically or reports a current value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesKind {
    Counter,
    Gauge,
}

/// One statistic, named as Prometheus exposes it.
///
/// [`Snapshot::series`] is the single enumeration of the broker's statistics.
/// Both `/metrics` and the OTLP exporter render *this* list, so a statistic
/// added in one place cannot go missing from the other. (`$SYS` is the
/// exception — it uses mosquitto's own topic hierarchy, which is a different
/// naming scheme rather than a rendering of these names; see
/// [`Snapshot::to_sys_topics`].)
#[derive(Debug, Clone)]
pub struct Series {
    /// The Prometheus name, `_total` suffix included where one applies.
    pub name: Cow<'static, str>,
    pub kind: SeriesKind,
    pub help: Cow<'static, str>,
    pub value: u64,
}

impl Series {
    /// The name to use for an OpenTelemetry instrument.
    ///
    /// OTel counters carry no `_total` suffix — a Collector's Prometheus
    /// exporter appends one, and exporting `mqtt_packets_received_total` would
    /// come out the far end as `mqtt_packets_received_total_total`. The strip
    /// is deliberately restricted to counters: `mqtt_sessions_total` and
    /// `mqtt_subscriptions_total` are *gauges* that happen to end in `_total`,
    /// and renaming those would silently change what an existing dashboard
    /// reads.
    pub fn otel_name(&self) -> &str {
        match self.kind {
            SeriesKind::Counter => self.name.strip_suffix("_total").unwrap_or(&self.name),
            SeriesKind::Gauge => &self.name,
        }
    }
}

/// Precomputed `mqtt_packet_<name>_{received,sent}_total` names and HELP text
/// for the per-packet counters in [`Snapshot::series`], in the same
/// (packet, suffix) order it emits them — `(kind - 1) * 2 + suffix_index`.
/// `PACKET_NAMES` is fixed at compile time, so there is nothing to
/// reformat on every `/metrics` scrape or OTLP export refresh; this table is
/// built once on first use instead.
static PACKET_SERIES_TEXT: LazyLock<Vec<(String, String)>> = LazyLock::new(|| {
    let mut v = Vec::with_capacity((PACKET_NAMES.len() - 1) * 2);
    for name in PACKET_NAMES.iter().skip(1) {
        for verb in ["received", "sent"] {
            v.push((
                format!("mqtt_packet_{name}_{verb}_total"),
                format!("Total {} packets {verb}.", name.to_uppercase()),
            ));
        }
    }
    v
});

impl Snapshot {
    /// Every statistic in the snapshot, in a stable order: counters first
    /// (including the per-control-packet array), then gauges.
    ///
    /// `mqtt_build_info` is not here. It is a constant-1 gauge whose payload is
    /// a *label*, not a value, so it has no `u64` to report; `to_prometheus`
    /// writes it directly and the OTLP exporter puts the version on the
    /// Resource as `service.version` instead.
    pub fn series(&self) -> Vec<Series> {
        let mut v: Vec<Series> = Vec::with_capacity(64);
        let mut counter = |name: &'static str, help: &'static str, value: u64| {
            v.push(Series {
                name: Cow::Borrowed(name),
                kind: SeriesKind::Counter,
                help: Cow::Borrowed(help),
                value,
            });
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
            "Total bytes received from clients and bridge remotes.",
            self.bytes_received,
        );
        counter(
            "mqtt_bytes_sent_total",
            "Total bytes sent to clients and bridge remotes.",
            self.bytes_sent,
        );
        counter(
            "mqtt_publish_received_total",
            "Total PUBLISH packets received from clients and bridge remotes.",
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
            "Total PUBLISH packets sent to clients and bridge remotes.",
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
        counter(
            "mqtt_connections_rate_limited_total",
            "Total connections refused by the per-source-IP rate limiter.",
            self.connections_rate_limited,
        );

        // Per-control-packet counters. Index 0 is the reserved type and is
        // never a real packet, so it is skipped.
        //
        // The `mqtt_packet_` prefix is load-bearing: these were originally
        // `mqtt_{name}_{received,sent}_total`, which for PUBLISH collided with
        // the `mqtt_publish_received_total` / `mqtt_publish_sent_total` counters
        // above. Prometheus exposition must not repeat a metric name with a
        // second HELP/TYPE block, so a scrape saw a duplicate and dropped or
        // errored on it. `$SYS` never had the clash — it keeps these under a
        // separate `mqtt/` sub-hierarchy — so only the flattened Prometheus
        // names needed the prefix. `no_duplicate_metric_names` guards it now.
        for (kind, _) in PACKET_NAMES.iter().enumerate().skip(1) {
            for (i, value) in [self.packet_received[kind], self.packet_sent[kind]]
                .into_iter()
                .enumerate()
            {
                let (name, help) = &PACKET_SERIES_TEXT[(kind - 1) * 2 + i];
                v.push(Series {
                    name: Cow::Borrowed(name.as_str()),
                    kind: SeriesKind::Counter,
                    help: Cow::Borrowed(help.as_str()),
                    value,
                });
            }
        }

        let mut gauge = |name: &'static str, help: &'static str, value: u64| {
            v.push(Series {
                name: Cow::Borrowed(name),
                kind: SeriesKind::Gauge,
                help: Cow::Borrowed(help),
                value,
            });
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
        v
    }

    /// Render the snapshot in the Prometheus text exposition format (v0.0.4).
    pub fn to_prometheus(&self) -> String {
        use std::fmt::Write as _;

        let mut o = String::with_capacity(2048);
        for s in self.series() {
            let (name, value) = (&s.name, s.value);
            let ty = match s.kind {
                SeriesKind::Counter => "counter",
                SeriesKind::Gauge => "gauge",
            };
            // `write!` into `o` directly instead of formatting into a
            // throwaway `String` first — one fewer allocation per series per
            // scrape/export. `write!` into a `String` never fails.
            let _ = write!(
                o,
                "# HELP {name} {}\n# TYPE {name} {ty}\n{name} {value}\n",
                s.help
            );
        }
        // Version as a static label on a constant-1 gauge, the conventional way
        // to expose build info to Prometheus.
        o.push_str(
            "# HELP mqtt_build_info Broker build information.\n\
             # TYPE mqtt_build_info gauge\n",
        );
        let _ = writeln!(o, "mqtt_build_info{{version=\"{}\"}} 1", self.version);
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

        add("version", format!("WispMQ {}", self.version));
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
        add(
            "connections/rate_limited",
            self.connections_rate_limited.to_string(),
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

    /// Home Assistant [MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery)
    /// config for every statistic in [`Snapshot::series`], one retained
    /// message per series under `{prefix}/sensor/{node_id}/{name}/config`.
    /// Retained for the same reason `$SYS` is: a Home Assistant restart must
    /// see every sensor immediately rather than waiting for the next
    /// `sys_interval` tick.
    ///
    /// `node_id` scopes both the topic and `unique_id`, so multiple WispMQ
    /// instances publishing into the same broker with distinct
    /// `service_name`s do not collide or overwrite each other's entities.
    /// Each `state_topic` points at [`Snapshot::to_ha_states`]'s output for
    /// the same series name, and both are built from the same `series()`
    /// call, so they cannot disagree.
    pub fn to_ha_discovery(&self, prefix: &str, node_id: &str) -> Vec<(String, String)> {
        let device_name = if node_id == "wispmq" {
            "WispMQ".to_string()
        } else {
            format!("WispMQ ({node_id})")
        };
        self.series()
            .into_iter()
            .map(|s| {
                let object_id = s.name.as_ref();
                let topic = format!("{prefix}/sensor/{node_id}/{object_id}/config");
                let payload = serde_json::json!({
                    "name": friendly_series_name(object_id),
                    "unique_id": format!("wispmq_{node_id}_{object_id}"),
                    "state_topic": ha_state_topic(node_id, object_id),
                    "state_class": match s.kind {
                        SeriesKind::Counter => "total_increasing",
                        SeriesKind::Gauge => "measurement",
                    },
                    "device": {
                        "identifiers": [format!("wispmq_{node_id}")],
                        "name": device_name,
                        "manufacturer": "elpapimango",
                        "model": "wispmq",
                        "sw_version": self.version,
                    },
                })
                .to_string();
                (topic, payload)
            })
            .collect()
    }

    /// Current values for every statistic in [`Snapshot::series`], published
    /// under the same state topics [`Snapshot::to_ha_discovery`] points at.
    pub fn to_ha_states(&self, node_id: &str) -> Vec<(String, String)> {
        self.series()
            .into_iter()
            .map(|s| (ha_state_topic(node_id, &s.name), s.value.to_string()))
            .collect()
    }
}

/// The MQTT state topic one series' Home Assistant sensor reads from.
fn ha_state_topic(node_id: &str, object_id: &str) -> String {
    format!("wispmq/{node_id}/{object_id}")
}

/// A human-readable Home Assistant entity name from a Prometheus series name,
/// e.g. `mqtt_clients_connected` -> `clients connected`.
fn friendly_series_name(object_id: &str) -> String {
    object_id
        .strip_prefix("mqtt_")
        .unwrap_or(object_id)
        .replace('_', " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot whose every field holds a distinct value, so a series wired
    /// to the wrong field shows up as a wrong number rather than a matching
    /// zero.
    fn distinct() -> Snapshot {
        let mut n = 0u64;
        let mut next = || {
            n += 1;
            n
        };
        let mut packet_received = [0u64; PACKET_KINDS];
        let mut packet_sent = [0u64; PACKET_KINDS];
        for slot in packet_received.iter_mut().chain(packet_sent.iter_mut()) {
            *slot = next();
        }
        Snapshot {
            connections_total: next(),
            packets_received: next(),
            packets_sent: next(),
            bytes_received: next(),
            bytes_sent: next(),
            publish_received: next(),
            publish_delivered: next(),
            publish_dropped: next(),
            bridge_forwarded_out: next(),
            bridge_forwarded_in: next(),
            publish_bytes_received: next(),
            publish_bytes_sent: next(),
            socket_connections: next(),
            clients_expired: next(),
            connections_rate_limited: next(),
            packet_received,
            packet_sent,
            clients_connected: next(),
            clients_disconnected: next(),
            clients_total: next(),
            clients_maximum: next(),
            sessions_total: next(),
            retained_messages: next(),
            retained_bytes: next(),
            subscriptions_total: next(),
            shared_subscriptions_count: next(),
            bridges_connected: next(),
            store_messages_count: next(),
            store_messages_bytes: next(),
            packet_out_count: next(),
            packet_out_bytes: next(),
            uptime_seconds: next(),
            version: "test",
        }
    }

    #[test]
    fn series_names_are_unique() {
        // A repeated name is invalid Prometheus exposition and would make two
        // OTLP instruments collide. `tests/sysinfo.rs::no_duplicate_metric_names`
        // checks the rendered text; this checks the source list.
        let series = distinct().series();
        let mut names: Vec<&str> = series.iter().map(|s| s.name.as_ref()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "duplicate series name");
    }

    #[test]
    fn every_series_reaches_the_prometheus_text() {
        // The renderer must not silently skip an entry: `to_prometheus` is a
        // rendering of `series()`, not a second, drifting list.
        let snap = distinct();
        let text = snap.to_prometheus();
        for s in snap.series() {
            let ty = match s.kind {
                SeriesKind::Counter => "counter",
                SeriesKind::Gauge => "gauge",
            };
            assert!(
                text.contains(&format!("# TYPE {} {ty}\n{} {}\n", s.name, s.name, s.value)),
                "missing or mistyped series {}",
                s.name
            );
        }
    }

    #[test]
    fn otel_names_strip_total_from_counters_only() {
        let series = distinct().series();
        let find = |name: &str| {
            series
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("no series {name}"))
        };

        // Counters lose the suffix so a Collector's Prometheus exporter does
        // not emit `..._total_total`.
        assert_eq!(
            find("mqtt_packets_received_total").otel_name(),
            "mqtt_packets_received"
        );
        assert_eq!(
            find("mqtt_packet_publish_sent_total").otel_name(),
            "mqtt_packet_publish_sent"
        );

        // These two are gauges that happen to end in `_total`. Stripping them
        // would rename a series every existing dashboard already reads.
        for name in ["mqtt_sessions_total", "mqtt_subscriptions_total"] {
            let s = find(name);
            assert_eq!(s.kind, SeriesKind::Gauge, "{name}");
            assert_eq!(s.otel_name(), name);
        }

        // A gauge with no suffix is unchanged either way.
        assert_eq!(
            find("mqtt_clients_connected").otel_name(),
            "mqtt_clients_connected"
        );
    }

    #[test]
    fn ha_discovery_state_topics_match_ha_states_topics() {
        // The two lists must agree by construction (both come from `series()`),
        // so an HA discovery config's `state_topic` always resolves to a topic
        // `to_ha_states` actually publishes.
        let snap = distinct();
        let discovery = snap.to_ha_discovery("homeassistant", "wispmq");
        let states = snap.to_ha_states("wispmq");
        assert_eq!(discovery.len(), states.len());
        assert_eq!(discovery.len(), snap.series().len());

        let state_topics: std::collections::HashSet<&str> =
            states.iter().map(|(t, _)| t.as_str()).collect();
        for (config_topic, payload) in &discovery {
            assert!(
                config_topic.starts_with("homeassistant/sensor/wispmq/"),
                "{config_topic}"
            );
            assert!(config_topic.ends_with("/config"), "{config_topic}");
            let v: serde_json::Value = serde_json::from_str(payload).unwrap();
            let state_topic = v["state_topic"].as_str().unwrap();
            assert!(
                state_topics.contains(state_topic),
                "discovery config points at a topic to_ha_states never publishes: {state_topic}"
            );
            assert!(v["unique_id"].as_str().unwrap().starts_with("wispmq_"));
            assert_eq!(v["device"]["identifiers"][0], "wispmq_wispmq");
        }
    }

    #[test]
    fn ha_discovery_state_class_matches_series_kind() {
        let snap = distinct();
        let series = snap.series();
        let discovery = snap.to_ha_discovery("homeassistant", "wispmq");
        for (s, (_, payload)) in series.iter().zip(discovery.iter()) {
            let v: serde_json::Value = serde_json::from_str(payload).unwrap();
            let expected = match s.kind {
                SeriesKind::Counter => "total_increasing",
                SeriesKind::Gauge => "measurement",
            };
            assert_eq!(v["state_class"], expected, "{}", s.name);
        }
    }

    #[test]
    fn ha_discovery_node_id_scopes_device_and_avoids_default_name_suffix() {
        // The default service_name ("wispmq") gets a plain device name;
        // anything else gets it appended so multiple instances are
        // distinguishable on the same Home Assistant dashboard.
        let snap = distinct();
        let default_node = snap.to_ha_discovery("homeassistant", "wispmq");
        let v: serde_json::Value = serde_json::from_str(&default_node[0].1).unwrap();
        assert_eq!(v["device"]["name"], "WispMQ");

        let scoped = snap.to_ha_discovery("homeassistant", "edge-1");
        let v: serde_json::Value = serde_json::from_str(&scoped[0].1).unwrap();
        assert_eq!(v["device"]["name"], "WispMQ (edge-1)");
        assert_eq!(v["device"]["identifiers"][0], "wispmq_edge-1");
        assert!(scoped[0].0.starts_with("homeassistant/sensor/edge-1/"));
    }
}
