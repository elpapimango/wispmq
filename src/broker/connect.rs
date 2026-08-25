//! CONNECT (3.1): authentication, session takeover and creation, and building
//! the CONNACK that advertises the server's capabilities.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::lifecycle::message_from_will;
use super::routing::resume_delivery;
use super::*;

impl Broker {
    /// Process a CONNECT and bind the new outgoing channel to a session.
    /// Returns `Ok(Accepted)` with the CONNACK to send, or `Err(Connack)`
    /// carrying a rejection CONNACK the caller should send before closing.
    pub fn handle_connect(
        &self,
        connect: Connect,
        out: OutTx,
        mut identity: Option<String>,
    ) -> Result<Accepted, Connack> {
        let version = match connect.validate_protocol() {
            Ok(v) => v,
            Err(e) => return Err(Connack::new(false, e.reason_code())),
        };
        let cfg = &self.inner.config;

        // Authentication (3.1.3.5 / 3.2.2). When a credential store is
        // configured, verify the username/password; the authenticated username
        // becomes the ACL identity (overriding any client-certificate CN).
        if let Some(creds) = &self.inner.auth {
            match (&connect.username, &connect.password) {
                (Some(user), Some(pass)) => {
                    if creds.verify(user, pass) {
                        identity = Some(user.clone());
                    } else {
                        tracing::debug!("CONNECT authentication failed for user {user:?}");
                        return Err(Connack::new(false, ReasonCode::BadUserNameOrPassword));
                    }
                }
                (Some(_), None) => {
                    return Err(Connack::new(false, ReasonCode::BadUserNameOrPassword));
                }
                (None, _) => {
                    if !cfg.allow_anonymous {
                        return Err(Connack::new(false, ReasonCode::NotAuthorized));
                    }
                    // Anonymous permitted; identity stays as the cert CN (or none).
                }
            }
        }

        // Authorization: if a Will is present, the identity must be allowed to
        // publish to the Will Topic, otherwise reject the connection (3.2.2.2 /
        // Reason Code 0x87 Not authorized).
        if let Some(will) = &connect.will {
            let who = identity.as_deref().unwrap_or(ANONYMOUS);
            if !self.acl().can_publish(who, &will.topic) {
                tracing::debug!(
                    "rejecting CONNECT from identity {who:?}: not authorized for Will topic {:?}",
                    will.topic
                );
                return Err(Connack::new(false, ReasonCode::NotAuthorized));
            }
        }

        // Resolve / assign a Client Identifier (3.1.3.1).
        let (client_id, assigned) = if connect.client_id.is_empty() {
            let mut st = self.lock();
            let n = st.auto_id_counter;
            st.auto_id_counter += 1;
            (format!("auto-{n}-{}", crate::message::now_unix()), true)
        } else {
            (connect.client_id.clone(), false)
        };

        // Reserved namespace: reject client IDs that collide with internal
        // identities. `$bridge/<name>` is assigned by `register_bridge` to
        // privileged in-process bridge sessions; `$SYS/` is the system-topic
        // prefix. Allowing a network client to claim either would let it
        // inherit bridge subscriptions (bypassing ACL) or interfere with
        // broker telemetry.
        if client_id.starts_with("$bridge/") || client_id.starts_with("$SYS/") {
            tracing::debug!("rejecting CONNECT with reserved client_id {:?}", client_id);
            return Err(Connack::new(false, ReasonCode::ClientIdentifierNotValid));
        }

        // Session Expiry Interval, clamped to the server maximum. v3.x has no
        // expiry property: a non-clean session persists (up to the server cap),
        // a clean session does not persist.
        let requested_expiry = if version.is_v5() {
            connect
                .properties
                .session_expiry_interval
                .unwrap_or(0)
                .min(cfg.max_session_expiry)
        } else if connect.clean_start {
            0
        } else {
            cfg.max_session_expiry
        };

        // Keep Alive: in v5 the server may override via CONNACK (3.2.2.3.9);
        // v3.x has no such property, so the client's value stands.
        let effective_keep_alive = if version.is_v5() {
            cfg.server_keep_alive.unwrap_or(connect.keep_alive)
        } else {
            connect.keep_alive
        };

        let mut st = self.lock();
        let epoch = st.next_epoch;
        st.next_epoch += 1;

        // Takeover: an existing online session with the same id is disconnected
        // with reason 0x8E (3.1.4-3). The outbound channel is bounded and may be
        // full, so `try_send` can fail; we close the channel itself to ensure the
        // old connection task exits even if it never drains the Shutdown signal.
        let mut session_present = false;
        if connect.clean_start {
            if let Some(old) = st.sessions.get_mut(&client_id) {
                if let Some(tx) = old.out.take() {
                    let _ = tx.try_send(Outgoing::Shutdown(ReasonCode::SessionTakenOver));
                    // Drop `tx` here to close the channel, guaranteeing the old task
                    // sees `None` from `rx.recv()` and exits (server.rs line 270).
                }
            }
            st.sessions.remove(&client_id);
            self.inner.storage.delete_session(client_id.clone());
        } else if let Some(old) = st.sessions.get_mut(&client_id) {
            if let Some(tx) = old.out.take() {
                let _ = tx.try_send(Outgoing::Shutdown(ReasonCode::SessionTakenOver));
                // Drop `tx` here to close the channel, guaranteeing the old task
                // sees `None` from `rx.recv()` and exits (server.rs line 270).
            }
            session_present = true;
        }

        // Create the session if needed.
        let session = st
            .sessions
            .entry(client_id.clone())
            .or_insert_with(|| Session::new(client_id.clone(), requested_expiry));
        session.epoch = epoch;
        session.identity = identity.clone();
        session.session_expiry_interval = requested_expiry;
        session.persistent = requested_expiry > 0 && !assigned;
        session.out = Some(out);
        session.client_receive_maximum = connect.properties.receive_maximum.unwrap_or(65_535);
        session.client_max_packet_size = connect.properties.maximum_packet_size;

        // Store the Will for later (abnormal-disconnect) publication.
        session.will = connect.will.as_ref().map(|w| {
            let delay = w.properties.will_delay_interval.unwrap_or(0);
            (message_from_will(w), delay)
        });

        // Persist the session if it is meant to outlive the connection.
        if requested_expiry > 0 && !assigned {
            self.inner
                .storage
                .upsert_session(client_id.clone(), requested_expiry);
        }

        // Build the CONNACK advertising server capabilities (3.2.2.3).
        let mut connack = Connack::new(session_present, ReasonCode::Success);
        let p = &mut connack.properties;
        p.receive_maximum = Some(cfg.receive_maximum);
        if cfg.maximum_qos != QoS::ExactlyOnce {
            p.maximum_qos = Some(cfg.maximum_qos.as_u8());
        }
        p.retain_available = Some(cfg.retain_available as u8);
        p.maximum_packet_size = Some(cfg.max_packet_size);
        p.topic_alias_maximum = Some(cfg.topic_alias_maximum);
        p.wildcard_subscription_available = Some(1);
        p.subscription_identifier_available = Some(1);
        p.shared_subscription_available = Some(1);
        if assigned {
            p.assigned_client_identifier = Some(client_id.clone());
        }
        if let Some(k) = cfg.server_keep_alive {
            p.server_keep_alive = Some(k);
        }
        if requested_expiry != connect.properties.session_expiry_interval.unwrap_or(0) {
            p.session_expiry_interval = Some(requested_expiry);
        }

        // Resume delivery of any inflight / queued messages for a resumed
        // session (4.4). Fresh sessions have nothing to resume.
        if session_present {
            let dropped = resume_delivery(session);
            if dropped > 0 {
                Metrics::add(&self.inner.metrics.publish_dropped, dropped);
            }
        }

        Metrics::inc(&self.inner.metrics.connections_total);
        let connected_now = st.sessions.values().filter(|s| s.is_online()).count() as u64;
        self.inner.metrics.observe_clients_connected(connected_now);

        Ok(Accepted {
            client_id,
            epoch,
            keep_alive: effective_keep_alive,
            identity,
            connack,
        })
    }
}
