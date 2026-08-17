//! Internal API used by broker-to-broker bridges, and the `$SYS` publisher
//! that shares its injection path.
//!
//! A bridge is a trusted in-process participant: it registers a clean local
//! session bound to an outgoing channel, subscribes (at QoS 0) to the local
//! topics it forwards outward, and injects messages it receives from the remote
//! back into local routing. It bypasses auth/ACL.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::*;

impl Broker {
    /// Register a clean, non-persistent bridge session bound to `out`. Returns
    /// its epoch (used to detect takeover on deregistration).
    pub fn register_bridge(&self, client_id: String, out: OutTx) -> u64 {
        let mut st = self.lock();
        let epoch = st.next_epoch;
        st.next_epoch += 1;
        let mut session = Session::new(client_id.clone(), 0);
        session.epoch = epoch;
        session.identity = Some("$bridge".to_string());
        session.out = Some(out);
        st.sessions.insert(client_id, session);
        epoch
    }

    /// Add a local subscription for a bridge's outbound topics. The in-process
    /// hop is QoS 0 (fire-and-forget; the configured QoS applies on the
    /// bridge↔remote link), `no_local` prevents echoing injected messages back
    /// out, and Retain As Published preserves the RETAIN flag so retained
    /// state can be forwarded.
    pub fn bridge_add_subscription(&self, client_id: &str, filter: &str, no_local: bool) {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            s.subscriptions.push(Subscription {
                filter: filter.to_string(),
                share_name: None,
                qos: QoS::AtMostOnce,
                no_local,
                retain_as_published: true,
                subscription_identifier: None,
            });
        }
    }

    /// Inject a message received from a remote into local routing, as if
    /// published by `from` (so `no_local` on the bridge's own subscription
    /// prevents a loop). Handles retained storage. Bypasses publish ACL.
    pub fn inject_publish(&self, from: &str, message: Message) {
        let mut st = self.lock();
        if message.retain {
            if message.payload.is_empty() {
                st.retained.remove(&message.topic);
                self.inner.storage.delete_retained(message.topic.clone());
            } else {
                st.retained.insert(message.topic.clone(), message.clone());
                self.inner
                    .storage
                    .upsert_retained(message.topic.clone(), message.clone());
            }
        }
        self.route(&mut st, Some(from), &message);
    }

    /// Publish a batch of `$SYS/broker/...` broker-status messages.
    ///
    /// Retained **in memory only**: subscribers that arrive between ticks get
    /// the last value immediately, but these are recomputed every
    /// `sys_interval` seconds, so persisting them would mean a SQLite write
    /// storm on a timer and stale statistics resurrected at the next startup.
    ///
    /// The whole batch is published under one lock acquisition — there are
    /// ~60 topics per tick and taking the broker lock once each would be
    /// needless contention with the network path.
    pub fn publish_sys(&self, entries: &[(String, String)]) {
        let mut st = self.lock();
        for (topic, payload) in entries {
            let message = Message {
                topic: topic.clone(),
                payload: std::sync::Arc::from(payload.as_bytes()),
                qos: QoS::AtMostOnce,
                retain: true,
                payload_format_indicator: Some(1), // UTF-8 text
                content_type: None,
                response_topic: None,
                correlation_data: None,
                user_properties: Vec::new(),
                expires_at: None,
            };
            st.retained.insert(topic.clone(), message.clone());
            self.route(&mut st, None, &message);
        }
    }

    /// Remove a bridge session on shutdown, if the epoch still matches.
    pub fn deregister_bridge(&self, client_id: &str, epoch: u64) {
        let mut st = self.lock();
        if st.sessions.get(client_id).map(|s| s.epoch) == Some(epoch) {
            st.sessions.remove(client_id);
        }
    }
}
