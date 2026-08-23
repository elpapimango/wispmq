//! Introspection for the admin, Prometheus and MCP surfaces: the metrics
//! snapshot plus the client and retained-message listings.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::*;

impl Broker {
    /// Combine cumulative counters with live gauges computed under the lock.
    ///
    /// Everything derived from `State` is gathered in one pass so the lock is
    /// held once and briefly: this runs on every Prometheus scrape and on every
    /// `$SYS` publish tick, both of which are on a timer rather than the
    /// network path, but it walks all sessions and must not become a stall.
    pub fn snapshot(&self) -> Snapshot {
        let m = &self.inner.metrics;
        let mut st = self.lock();

        let mut clients_connected = 0u64;
        let mut clients_disconnected = 0u64;
        let mut subscriptions_total = 0u64;
        let mut shared_subscriptions_count = 0u64;
        let mut packet_out_count = 0u64;
        let mut packet_out_bytes = 0u64;

        for s in st.sessions.values() {
            if s.is_online() {
                clients_connected += 1;
            } else {
                clients_disconnected += 1;
            }
            subscriptions_total += s.subscriptions.len() as u64;
            shared_subscriptions_count += s
                .subscriptions
                .iter()
                .filter(|x| x.share_name.is_some())
                .count() as u64;
            // Queued plus inflight is what the broker still owes this client.
            packet_out_count += (s.queue.len() + s.inflight_count()) as u64;
            packet_out_bytes += s
                .queue
                .iter()
                .map(|p| p.message.payload.len() as u64)
                .sum::<u64>();
        }

        // `$SYS` status values live in the same retained map (that is how they
        // reach late subscribers), but they are broker-owned bookkeeping, not
        // user data. Counting them would make `retained_messages` jump by ~50
        // the moment $SYS is enabled and would silently change the meaning of
        // an existing gauge, so they are excluded here.
        // Also the moment to drop entries whose `message_expiry_interval`
        // has passed: nothing else in the broker purges them (they normally
        // just get overwritten by the next retained PUBLISH on that topic),
        // so an expired entry that's never republished would otherwise leak
        // in memory forever, in addition to inflating these gauges.
        st.retained.retain(|_, msg| !msg.is_expired());
        let mut retained_messages = 0u64;
        let mut retained_bytes = 0u64;
        for (topic, msg) in st.retained.iter() {
            if topic.starts_with(SYS_PREFIX) {
                continue;
            }
            retained_messages += 1;
            retained_bytes += msg.payload.len() as u64;
        }

        Snapshot {
            connections_total: Metrics::get(&m.connections_total),
            packets_received: Metrics::get(&m.packets_received),
            packets_sent: Metrics::get(&m.packets_sent),
            bytes_received: Metrics::get(&m.bytes_received),
            bytes_sent: Metrics::get(&m.bytes_sent),
            publish_received: Metrics::get(&m.publish_received),
            publish_delivered: Metrics::get(&m.publish_delivered),
            publish_dropped: Metrics::get(&m.publish_dropped),
            bridge_forwarded_out: Metrics::get(&m.bridge_forwarded_out),
            bridge_forwarded_in: Metrics::get(&m.bridge_forwarded_in),
            publish_bytes_received: Metrics::get(&m.publish_bytes_received),
            publish_bytes_sent: Metrics::get(&m.publish_bytes_sent),
            socket_connections: Metrics::get(&m.socket_connections),
            clients_expired: Metrics::get(&m.clients_expired),
            connections_rate_limited: Metrics::get(&m.connections_rate_limited),
            packet_received: Metrics::read_array(&m.packet_received),
            packet_sent: Metrics::read_array(&m.packet_sent),
            clients_connected,
            clients_disconnected,
            clients_total: st.sessions.len() as u64,
            clients_maximum: Metrics::get(&m.clients_maximum),
            sessions_total: st.sessions.len() as u64,
            retained_messages,
            retained_bytes,
            subscriptions_total,
            shared_subscriptions_count,
            bridges_connected: Metrics::get(&m.bridges_connected),
            // "Store" is retained plus everything still queued for a client.
            store_messages_count: retained_messages + packet_out_count,
            store_messages_bytes: retained_bytes + packet_out_bytes,
            packet_out_count,
            packet_out_bytes,
            uptime_seconds: self.inner.started_at.elapsed().as_secs(),
            version: crate::config::VERSION,
        }
    }

    /// List all sessions (online and offline).
    pub fn clients(&self) -> Vec<ClientInfo> {
        let st = self.lock();
        let mut out: Vec<ClientInfo> = st
            .sessions
            .values()
            .map(|s| ClientInfo {
                client_id: s.client_id.clone(),
                online: s.is_online(),
                subscriptions: s.subscriptions.len(),
                inflight: s.inflight_count(),
                queued: s.queue.len(),
                session_expiry: s.session_expiry_interval,
                persistent: s.persistent,
            })
            .collect();
        out.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        out
    }

    /// List all retained messages (topic, size, QoS).
    pub fn retained(&self) -> Vec<RetainedInfo> {
        let st = self.lock();
        let mut out: Vec<RetainedInfo> = st
            .retained
            .iter()
            // Skip `$SYS` for the same reason the gauges do: it is broker
            // status, and ~50 of them would bury the user's own retained data.
            .filter(|(topic, m)| !m.is_expired() && !topic.starts_with(SYS_PREFIX))
            .map(|(topic, m)| RetainedInfo {
                topic: topic.clone(),
                payload_size: m.payload.len(),
                qos: m.qos.as_u8(),
            })
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }
}
