//! Session lifecycle: client-initiated DISCONNECT, connection teardown, Will
//! publication and its delay, and session expiry.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::*;

impl Broker {
    pub(super) fn handle_client_disconnect(
        &self,
        client_id: &str,
        _epoch: u64,
        d: Disconnect,
    ) -> Action {
        // A DISCONNECT with reason 0x04 requests Will publication; a normal
        // (0x00) DISCONNECT means the Will is discarded (3.1.2.5 / 3.14.4).
        let publish_will = d.reason_code == ReasonCode::DisconnectWithWillMessage;

        // The client may update its Session Expiry Interval on disconnect.
        if let Some(new_expiry) = d.properties.session_expiry_interval {
            let mut st = self.lock();
            if let Some(s) = st.sessions.get_mut(client_id) {
                // 0 -> non-zero after a 0 CONNECT is a protocol error, but we
                // simply clamp and accept for robustness.
                s.session_expiry_interval = new_expiry.min(self.inner.config.max_session_expiry);
                if s.persistent {
                    self.inner.storage.upsert_session(
                        client_id.to_string(),
                        s.session_expiry_interval,
                        s.identity.clone(),
                    );
                }
            }
        }

        Action::ClientDisconnect { publish_will }
    }

    /// Called by the connection task once the socket is done. Publishes the
    /// Will if appropriate, then either drops or schedules expiry of the
    /// session. `epoch` guards against acting on a session already taken over.
    pub fn handle_connection_closed(&self, client_id: &str, epoch: u64, publish_will: bool) {
        let mut will_to_fire: Option<(Message, u32)> = None;
        let mut will_owner = String::new();
        let expiry: u32;

        {
            let mut st = self.lock();
            let Some(session) = st.sessions.get_mut(client_id) else {
                return;
            };
            if session.epoch != epoch {
                // Superseded by a newer connection (takeover) — do nothing.
                return;
            }
            session.go_offline();
            session.inbound_aliases.clear();

            if publish_will {
                if let Some(will) = session.will.take() {
                    will_owner = session
                        .identity
                        .clone()
                        .unwrap_or_else(|| ANONYMOUS.to_string());
                    will_to_fire = Some(will);
                }
            } else {
                // Normal disconnect discards the Will.
                session.will = None;
            }
            expiry = session.session_expiry_interval;
        }

        // Publish the Will (respecting Will Delay Interval, 3.1.3.2.2).
        if let Some((message, delay)) = will_to_fire {
            self.schedule_will(client_id.to_string(), epoch, message, delay, will_owner);
        }

        // Handle session expiry.
        if expiry == 0 {
            let mut st = self.lock();
            if let Some(s) = st.sessions.get(client_id) {
                if s.epoch == epoch && !s.is_online() {
                    let persistent = s.persistent;
                    st.sessions.remove(client_id);
                    self.inner.storage.delete_session(client_id.to_string());
                    // Only a durable session "expires"; a clean-start session
                    // ending is the normal case, not an expiry event.
                    if persistent {
                        Metrics::inc(&self.inner.metrics.clients_expired);
                    }
                }
            }
        } else {
            self.schedule_expiry(client_id.to_string(), epoch, expiry);
        }
    }

    fn schedule_will(
        &self,
        client_id: String,
        epoch: u64,
        message: Message,
        delay: u32,
        owner: String,
    ) {
        let broker = self.clone();
        tokio::spawn(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_secs(delay as u64)).await;
                // If the client reconnected in the meantime, cancel the Will.
                let st = broker.lock();
                if let Some(s) = st.sessions.get(&client_id) {
                    if s.epoch != epoch || s.is_online() {
                        return;
                    }
                }
            }

            // Re-check authorization at fire time. The Will was authorized at
            // CONNECT, but an arbitrary amount of time (plus the Will Delay)
            // may have passed, during which a SIGHUP reload can have revoked
            // the identity's right to publish here. That reload also
            // disconnects the affected client — which is precisely what fires
            // the Will — so without this check, revoking a permission would
            // *cause* the publish it was meant to prevent.
            if !broker.acl().can_publish(&owner, &message.topic) {
                tracing::info!(
                    identity = %owner,
                    topic = %message.topic,
                    "dropping Will message: identity is no longer authorized to publish there"
                );
                return;
            }

            let mut st = broker.lock();
            // Store retained Will if flagged.
            if message.retain {
                if message.payload.is_empty() {
                    st.retained.remove(&message.topic);
                    broker.inner.storage.delete_retained(message.topic.clone());
                } else {
                    st.retained.insert(message.topic.clone(), message.clone());
                    broker
                        .inner
                        .storage
                        .upsert_retained(message.topic.clone(), message.clone());
                }
            }
            broker.route(&mut st, None, &message);
        });
    }

    fn schedule_expiry(&self, client_id: String, epoch: u64, secs: u32) {
        let broker = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(secs as u64)).await;
            let mut st = broker.lock();
            if let Some(s) = st.sessions.get(&client_id) {
                if s.epoch == epoch && !s.is_online() {
                    let persistent = s.persistent;
                    st.sessions.remove(&client_id);
                    broker.inner.storage.delete_session(client_id.clone());
                    if persistent {
                        Metrics::inc(&broker.inner.metrics.clients_expired);
                    }
                }
            }
        });
    }
}

/// Build a routable Will Message from a CONNECT Will (3.1.3.2).
pub(super) fn message_from_will(w: &crate::packet::Will) -> Message {
    let expires_at = w
        .properties
        .message_expiry_interval
        .map(|s| crate::message::now_unix().saturating_add(s as u64));
    Message {
        topic: w.topic.clone(),
        // The Will arrives in CONNECT as a `Vec`, so this is the one place the
        // payload is copied into its `Arc` — once per will, not per delivery.
        payload: std::sync::Arc::from(w.payload.as_slice()),
        qos: w.qos,
        retain: w.retain,
        payload_format_indicator: w.properties.payload_format_indicator,
        content_type: w.properties.content_type.clone(),
        response_topic: w.properties.response_topic.clone(),
        correlation_data: w.properties.correlation_data.clone(),
        user_properties: w.properties.user_properties.clone(),
        expires_at,
    }
}
