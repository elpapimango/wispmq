//! The broker: shared session registry, message routing, retained-message
//! store, and the handlers the per-connection task drives.
//!
//! All shared state lives behind a single `Mutex`. Handlers never await while
//! holding it — outbound delivery is a non-blocking push into each client's
//! unbounded channel — so a plain `std::sync::Mutex` is used.

mod session;

pub use session::{OutTx, Outgoing, Session};

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use session::{Pending, Subscription};

use crate::acl::Acl;
use crate::auth::Credentials;
use crate::config::Config;
use crate::message::Message;
use crate::metrics::{Metrics, Snapshot};
use crate::packet::Packet;
use crate::packet::{
    Connack, Connect, Disconnect, PubAck, Publish, RetainHandling, SubAck, Subscribe, Unsubscribe,
};
use crate::storage::{LoadedState, Storage, SubRecord};
use crate::topic;
use crate::types::{QoS, ReasonCode};

/// Identity used for ACL decisions when a connection presents no client
/// certificate (no mutual TLS).
const ANONYMOUS: &str = "anonymous";

/// Result of handling an inbound packet, telling the connection task what to do.
pub enum Action {
    /// Keep serving the connection.
    Continue,
    /// Server-initiated protocol failure: send DISCONNECT with this code, then
    /// close. The Will is published (abnormal disconnection).
    ServerDisconnect(ReasonCode),
    /// Client asked to disconnect. Close without a DISCONNECT reply.
    ClientDisconnect { publish_will: bool },
}

/// Details returned when a CONNECT is accepted.
pub struct Accepted {
    pub client_id: String,
    pub epoch: u64,
    /// Effective Keep Alive in seconds (server override or client value).
    pub keep_alive: u16,
    /// Resolved authenticated identity (username or cert CN), for logging.
    pub identity: Option<String>,
    pub connack: Connack,
}

#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    config: Config,
    storage: Storage,
    state: Mutex<State>,
    metrics: Metrics,
    /// Authorization policy, swappable at runtime (reloaded on SIGHUP). Readers
    /// take a cheap snapshot of the `Arc`; a reload replaces it atomically.
    acl: RwLock<Arc<Acl>>,
    /// Username/password credentials. `None` disables password authentication.
    auth: Option<Credentials>,
}

/// Summary of one session, for the admin/MCP surface.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub online: bool,
    pub subscriptions: usize,
    pub inflight: usize,
    pub queued: usize,
    pub session_expiry: u32,
    pub persistent: bool,
}

/// Summary of one retained message, for the admin/MCP surface.
#[derive(Debug, Clone)]
pub struct RetainedInfo {
    pub topic: String,
    pub payload_size: usize,
    pub qos: u8,
}

struct State {
    sessions: HashMap<String, Session>,
    retained: HashMap<String, Message>,
    /// Round-robin cursor per shared-subscription group.
    shared_rr: HashMap<(String, String), usize>,
    next_epoch: u64,
    auto_id_counter: u64,
}

impl Broker {
    pub fn new(
        config: Config,
        storage: Storage,
        loaded: LoadedState,
        acl: Acl,
        auth: Option<Credentials>,
    ) -> Broker {
        let mut sessions = HashMap::new();
        for rec in loaded.sessions {
            let mut s = Session::new(rec.client_id.clone(), rec.session_expiry_interval);
            // A session that was on disk is by definition persistent.
            s.persistent = true;
            for sub in rec.subscriptions {
                let parsed = topic::parse_filter(&sub.filter);
                let (share_name, filter) = match parsed {
                    Some(p) => (p.share_name.map(|s| s.to_string()), p.filter.to_string()),
                    None => (None, sub.filter.clone()),
                };
                s.subscriptions.push(Subscription {
                    filter,
                    share_name,
                    qos: sub.qos,
                    no_local: sub.no_local,
                    retain_as_published: sub.retain_as_published,
                    retain_handling: sub.retain_handling,
                    subscription_identifier: sub.subscription_identifier,
                });
            }
            sessions.insert(rec.client_id, s);
        }
        let retained = loaded.retained.into_iter().collect();

        Broker {
            inner: Arc::new(Inner {
                config,
                storage,
                state: Mutex::new(State {
                    sessions,
                    retained,
                    shared_rr: HashMap::new(),
                    next_epoch: 1,
                    auto_id_counter: 1,
                }),
                metrics: Metrics::default(),
                acl: RwLock::new(Arc::new(acl)),
                auth,
            }),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// Take a cheap snapshot of the current authorization policy.
    fn acl(&self) -> Arc<Acl> {
        self.inner.acl.read().expect("acl lock poisoned").clone()
    }

    /// Reload the ACL policy from the configured file and swap it in
    /// atomically. Returns `Ok(true)` when a reload happened, `Ok(false)` when
    /// no ACL file is configured. On a parse/read error the existing policy is
    /// kept and the error is returned.
    ///
    /// After swapping, any live subscription that the new policy no longer
    /// authorizes is revoked (removed from the session and persistence).
    pub fn reload_acl(&self) -> crate::error::Result<bool> {
        let Some(path) = &self.inner.config.acl_path else {
            return Ok(false);
        };
        let fresh = Arc::new(Acl::load(path)?);
        *self.inner.acl.write().expect("acl lock poisoned") = fresh.clone();
        self.revoke_unauthorized_subscriptions(&fresh);
        Ok(true)
    }

    /// Remove subscriptions that are no longer authorized under `acl`, and
    /// disconnect any online client that had one revoked with a DISCONNECT
    /// carrying Reason Code 0x87 (Not authorized).
    ///
    /// Subscriptions are pruned even for offline persistent sessions so a
    /// later resume does not silently re-enable a revoked filter (delivery is
    /// not re-checked against the ACL).
    fn revoke_unauthorized_subscriptions(&self, acl: &Acl) {
        if !acl.is_enforced() {
            return;
        }
        let mut st = self.lock();
        for session in st.sessions.values_mut() {
            let who = session
                .identity
                .clone()
                .unwrap_or_else(|| ANONYMOUS.to_string());
            let client_id = session.client_id.clone();
            let persistent = session.persistent;

            let mut revoked: Vec<(String, Option<String>)> = Vec::new();
            session.subscriptions.retain(|sub| {
                if acl.can_subscribe(&who, &sub.filter) {
                    true
                } else {
                    revoked.push((sub.filter.clone(), sub.share_name.clone()));
                    false
                }
            });

            if revoked.is_empty() {
                continue;
            }
            for (filter, share) in &revoked {
                if persistent {
                    // Reconstruct the stored key (shared subs keep their prefix).
                    let key = match share {
                        Some(s) => format!("$share/{s}/{filter}"),
                        None => filter.clone(),
                    };
                    self.inner
                        .storage
                        .delete_subscription(client_id.clone(), key);
                }
            }

            // Administratively disconnect the client if it is online. The
            // connection task sends DISCONNECT (0x87) and closes; its Will is
            // not published for this server-initiated close.
            let online = session.out.is_some();
            if let Some(tx) = &session.out {
                let _ = tx.send(Outgoing::Shutdown(ReasonCode::NotAuthorized));
            }
            tracing::info!(
                "ACL reload revoked {} now-unauthorized subscription(s) for {client_id:?} ({who}){}",
                revoked.len(),
                if online { "; disconnecting (0x87 Not authorized)" } else { "" }
            );
        }
    }

    // ---------------------------------------------------------------------
    // Introspection for the admin / metrics / MCP surface
    // ---------------------------------------------------------------------

    /// Combine cumulative counters with live gauges computed under the lock.
    pub fn snapshot(&self) -> Snapshot {
        let m = &self.inner.metrics;
        let st = self.lock();
        let clients_connected = st.sessions.values().filter(|s| s.is_online()).count() as u64;
        let subscriptions_total = st
            .sessions
            .values()
            .map(|s| s.subscriptions.len())
            .sum::<usize>() as u64;
        Snapshot {
            connections_total: Metrics::get(&m.connections_total),
            packets_received: Metrics::get(&m.packets_received),
            packets_sent: Metrics::get(&m.packets_sent),
            bytes_received: Metrics::get(&m.bytes_received),
            bytes_sent: Metrics::get(&m.bytes_sent),
            publish_received: Metrics::get(&m.publish_received),
            publish_delivered: Metrics::get(&m.publish_delivered),
            clients_connected,
            sessions_total: st.sessions.len() as u64,
            retained_messages: st.retained.len() as u64,
            subscriptions_total,
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
            .filter(|(_, m)| !m.is_expired())
            .map(|(topic, m)| RetainedInfo {
                topic: topic.clone(),
                payload_size: m.payload.len(),
                qos: m.qos.as_u8(),
            })
            .collect();
        out.sort_by(|a, b| a.topic.cmp(&b.topic));
        out
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner.state.lock().expect("broker state poisoned")
    }

    // ---------------------------------------------------------------------
    // CONNECT
    // ---------------------------------------------------------------------

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
        // with reason 0x8E (3.1.4-3).
        let mut session_present = false;
        if connect.clean_start {
            if let Some(old) = st.sessions.get_mut(&client_id) {
                if let Some(tx) = old.out.take() {
                    let _ = tx.send(Outgoing::Shutdown(ReasonCode::SessionTakenOver));
                }
            }
            st.sessions.remove(&client_id);
            self.inner.storage.delete_session(client_id.clone());
        } else if let Some(old) = st.sessions.get_mut(&client_id) {
            if let Some(tx) = old.out.take() {
                let _ = tx.send(Outgoing::Shutdown(ReasonCode::SessionTakenOver));
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
            resume_delivery(session);
        }

        Metrics::inc(&self.inner.metrics.connections_total);

        Ok(Accepted {
            client_id,
            epoch,
            keep_alive: effective_keep_alive,
            identity,
            connack,
        })
    }

    // ---------------------------------------------------------------------
    // Packet dispatch
    // ---------------------------------------------------------------------

    pub fn handle_packet(&self, client_id: &str, epoch: u64, packet: Packet) -> Action {
        match packet {
            Packet::Publish(p) => self.handle_publish(client_id, epoch, p),
            Packet::Puback(a) => self.handle_puback(client_id, epoch, a),
            Packet::Pubrec(a) => self.handle_pubrec(client_id, epoch, a),
            Packet::Pubrel(a) => self.handle_pubrel(client_id, epoch, a),
            Packet::Pubcomp(a) => self.handle_pubcomp(client_id, epoch, a),
            Packet::Subscribe(s) => self.handle_subscribe(client_id, epoch, s),
            Packet::Unsubscribe(u) => self.handle_unsubscribe(client_id, epoch, u),
            Packet::Pingreq => {
                self.push_control(client_id, Packet::Pingresp);
                Action::Continue
            }
            Packet::Disconnect(d) => self.handle_client_disconnect(client_id, epoch, d),
            // A second CONNECT on an established connection is a protocol error.
            Packet::Connect(_) => Action::ServerDisconnect(ReasonCode::ProtocolError),
            Packet::Auth(_) => {
                // Extended authentication is not implemented; accept as no-op.
                Action::Continue
            }
            // Packets only ever sent Server->Client are illegal from a client.
            Packet::Connack(_) | Packet::Suback(_) | Packet::Unsuback(_) | Packet::Pingresp => {
                Action::ServerDisconnect(ReasonCode::ProtocolError)
            }
        }
    }

    fn push_control(&self, client_id: &str, packet: Packet) {
        let st = self.lock();
        if let Some(s) = st.sessions.get(client_id) {
            if let Some(tx) = &s.out {
                let _ = tx.send(Outgoing::Control(Box::new(packet)));
            }
        }
    }

    // ---------------------------------------------------------------------
    // PUBLISH (inbound from a client)
    // ---------------------------------------------------------------------

    fn handle_publish(&self, client_id: &str, _epoch: u64, mut publish: Publish) -> Action {
        Metrics::inc(&self.inner.metrics.publish_received);
        let cfg = &self.inner.config;

        // QoS ceiling (3.2.2.3.4 / 3.2.2-11).
        if publish.qos.as_u8() > cfg.maximum_qos.as_u8() {
            return Action::ServerDisconnect(ReasonCode::QoSNotSupported);
        }
        // RETAIN support (3.2.2.3.5).
        if publish.retain && !cfg.retain_available {
            return Action::ServerDisconnect(ReasonCode::RetainNotSupported);
        }

        let mut st = self.lock();

        // Topic Alias resolution (3.3.2.3.4).
        if let Some(alias) = publish.properties.topic_alias {
            if alias > cfg.topic_alias_maximum {
                return Action::ServerDisconnect(ReasonCode::TopicAliasInvalid);
            }
            let Some(session) = st.sessions.get_mut(client_id) else {
                return Action::ServerDisconnect(ReasonCode::UnspecifiedError);
            };
            if publish.topic.is_empty() {
                match session.inbound_aliases.get(&alias) {
                    Some(t) => publish.topic = t.clone(),
                    None => return Action::ServerDisconnect(ReasonCode::TopicAliasInvalid),
                }
            } else {
                session.inbound_aliases.insert(alias, publish.topic.clone());
            }
        }

        if !topic::valid_topic_name(&publish.topic) {
            return Action::ServerDisconnect(ReasonCode::TopicNameInvalid);
        }

        // Authorization: may this identity publish to the topic (Reason Code
        // 0x87)? An unauthorized QoS 0 message is dropped silently; QoS 1/2 are
        // acknowledged with Not authorized and neither routed nor retained.
        let acl = self.acl();
        if acl.is_enforced() {
            let who = st.sessions.get(client_id).and_then(|s| s.identity.clone());
            let who = who.as_deref().unwrap_or(ANONYMOUS);
            if !acl.can_publish(who, &publish.topic) {
                tracing::debug!(
                    "{client_id:?} ({who}) not authorized to publish to {:?}",
                    publish.topic
                );
                let id = publish.packet_id.unwrap_or(0);
                match publish.qos {
                    QoS::AtMostOnce => {}
                    QoS::AtLeastOnce => send_to(
                        &st,
                        client_id,
                        Outgoing::Control(Box::new(Packet::Puback(PubAck::new(
                            id,
                            ReasonCode::NotAuthorized,
                        )))),
                    ),
                    QoS::ExactlyOnce => send_to(
                        &st,
                        client_id,
                        Outgoing::Control(Box::new(Packet::Pubrec(PubAck::new(
                            id,
                            ReasonCode::NotAuthorized,
                        )))),
                    ),
                }
                return Action::Continue;
            }
        }

        let message = Message::from_publish(&publish);

        // Retained-message maintenance (3.3.1.3). A zero-length payload with
        // RETAIN clears the retained message for that topic.
        if publish.retain {
            if message.payload.is_empty() {
                st.retained.remove(&publish.topic);
                self.inner.storage.delete_retained(publish.topic.clone());
            } else {
                st.retained.insert(publish.topic.clone(), message.clone());
                self.inner
                    .storage
                    .upsert_retained(publish.topic.clone(), message.clone());
            }
        }

        match publish.qos {
            QoS::AtMostOnce => {
                self.route(&mut st, Some(client_id), &message);
                Action::Continue
            }
            QoS::AtLeastOnce => {
                let matched = self.route(&mut st, Some(client_id), &message);
                let rc = if matched {
                    ReasonCode::Success
                } else {
                    ReasonCode::NoMatchingSubscribers
                };
                let id = publish.packet_id.unwrap_or(0);
                send_to(
                    &st,
                    client_id,
                    Outgoing::Control(Box::new(Packet::Puback(PubAck::new(id, rc)))),
                );
                Action::Continue
            }
            QoS::ExactlyOnce => {
                let id = publish.packet_id.unwrap_or(0);
                let Some(session) = st.sessions.get_mut(client_id) else {
                    return Action::ServerDisconnect(ReasonCode::UnspecifiedError);
                };
                let duplicate = session.inbound_qos2.contains(&id);
                if !duplicate {
                    session.inbound_qos2.insert(id);
                }
                let rc = if duplicate {
                    ReasonCode::Success
                } else {
                    let matched = self.route(&mut st, Some(client_id), &message);
                    if matched {
                        ReasonCode::Success
                    } else {
                        ReasonCode::NoMatchingSubscribers
                    }
                };
                send_to(
                    &st,
                    client_id,
                    Outgoing::Control(Box::new(Packet::Pubrec(PubAck::new(id, rc)))),
                );
                Action::Continue
            }
        }
    }

    // ---------------------------------------------------------------------
    // Acknowledgements for our outbound messages
    // ---------------------------------------------------------------------

    fn handle_puback(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            s.awaiting_puback.remove(&ack.packet_id);
            flush_queue(s);
        }
        Action::Continue
    }

    fn handle_pubrec(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            let known = s.awaiting_pubrec.remove(&ack.packet_id).is_some();
            let rc = if known {
                s.awaiting_pubcomp.insert(ack.packet_id);
                ReasonCode::Success
            } else if ack.reason_code.is_error() {
                // Client rejected; nothing more to do.
                return Action::Continue;
            } else {
                ReasonCode::PacketIdentifierNotFound
            };
            let pubrel = PubAck::new(ack.packet_id, rc);
            if let Some(tx) = &s.out {
                let _ = tx.send(Outgoing::Control(Box::new(Packet::Pubrel(pubrel))));
            }
        }
        Action::Continue
    }

    fn handle_pubrel(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            let existed = s.inbound_qos2.remove(&ack.packet_id);
            let rc = if existed {
                ReasonCode::Success
            } else {
                ReasonCode::PacketIdentifierNotFound
            };
            if let Some(tx) = &s.out {
                let _ = tx.send(Outgoing::Control(Box::new(Packet::Pubcomp(PubAck::new(
                    ack.packet_id,
                    rc,
                )))));
            }
        }
        Action::Continue
    }

    fn handle_pubcomp(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            s.awaiting_pubcomp.remove(&ack.packet_id);
            flush_queue(s);
        }
        Action::Continue
    }

    // ---------------------------------------------------------------------
    // SUBSCRIBE / UNSUBSCRIBE
    // ---------------------------------------------------------------------

    fn handle_subscribe(&self, client_id: &str, _epoch: u64, sub: Subscribe) -> Action {
        let sub_id = sub.properties.subscription_identifiers.first().copied();
        let mut reason_codes = Vec::with_capacity(sub.filters.len());
        // Retained messages to send after the SUBACK, collected under the lock.
        let mut retained_to_send: Vec<(Message, QoS, Vec<u32>)> = Vec::new();

        let mut st = self.lock();
        let max_qos = self.inner.config.maximum_qos;

        // Authenticated identity for ACL decisions (constant for the session).
        let acl = self.acl();
        let enforce_acl = acl.is_enforced();
        let who: String = st
            .sessions
            .get(client_id)
            .and_then(|s| s.identity.clone())
            .unwrap_or_else(|| ANONYMOUS.to_string());

        for f in &sub.filters {
            if !topic::valid_topic_filter(strip_share_for_validation(&f.filter)) {
                reason_codes.push(ReasonCode::TopicFilterInvalid);
                continue;
            }
            let Some(parsed) = topic::parse_filter(&f.filter) else {
                reason_codes.push(ReasonCode::TopicFilterInvalid);
                continue;
            };
            let share_name = parsed.share_name.map(|s| s.to_string());
            let match_filter = parsed.filter.to_string();

            // Authorization: may this identity subscribe to the filter (0x87)?
            if enforce_acl && !acl.can_subscribe(&who, &match_filter) {
                tracing::debug!(
                    "{client_id:?} ({who}) not authorized to subscribe to {:?}",
                    f.filter
                );
                reason_codes.push(ReasonCode::NotAuthorized);
                continue;
            }

            let granted = f.qos.min(max_qos);

            let subscription = Subscription {
                filter: match_filter.clone(),
                share_name: share_name.clone(),
                qos: granted,
                no_local: f.no_local,
                retain_as_published: f.retain_as_published,
                retain_handling: f.retain_handling,
                subscription_identifier: sub_id,
            };

            let Some(session) = st.sessions.get_mut(client_id) else {
                return Action::ServerDisconnect(ReasonCode::UnspecifiedError);
            };
            let existed = session.upsert_subscription(subscription);

            // Persist subscriptions only for durable sessions (a session row
            // must exist for the foreign key).
            if session.persistent {
                self.inner.storage.upsert_subscription(
                    client_id.to_string(),
                    SubRecord {
                        filter: f.filter.clone(),
                        qos: granted,
                        no_local: f.no_local,
                        retain_as_published: f.retain_as_published,
                        retain_handling: f.retain_handling,
                        subscription_identifier: sub_id,
                    },
                );
            }

            // Decide whether to send retained messages now (3.3.1.3 / 3.8.4).
            let send_retained = share_name.is_none()
                && match f.retain_handling {
                    RetainHandling::SendAtSubscribe => true,
                    RetainHandling::SendIfNewSubscription => !existed,
                    RetainHandling::DoNotSend => false,
                };
            if send_retained {
                for (topic_name, msg) in st.retained.iter() {
                    if msg.is_expired() {
                        continue;
                    }
                    if topic::matches(&match_filter, topic_name) {
                        let eff = msg.qos.min(granted);
                        let ids = sub_id.map(|i| vec![i]).unwrap_or_default();
                        retained_to_send.push((msg.clone(), eff, ids));
                    }
                }
            }

            reason_codes.push(match granted {
                QoS::AtMostOnce => ReasonCode::Success, // Granted QoS 0
                QoS::AtLeastOnce => ReasonCode::GrantedQoS1,
                QoS::ExactlyOnce => ReasonCode::GrantedQoS2,
            });
        }

        // SUBACK first (3.8.4), then retained deliveries.
        let suback = SubAck::new(sub.packet_id, reason_codes);
        send_to(
            &st,
            client_id,
            Outgoing::Control(Box::new(Packet::Suback(suback))),
        );

        for (msg, eff_qos, ids) in retained_to_send {
            if let Some(session) = st.sessions.get_mut(client_id) {
                deliver_to_session(session, &msg, eff_qos, true, &ids);
            }
        }

        Action::Continue
    }

    fn handle_unsubscribe(&self, client_id: &str, _epoch: u64, unsub: Unsubscribe) -> Action {
        let mut reason_codes = Vec::with_capacity(unsub.filters.len());
        let mut st = self.lock();
        for filter in &unsub.filters {
            let (share_name, match_filter) = match topic::parse_filter(filter) {
                Some(p) => (p.share_name.map(|s| s.to_string()), p.filter.to_string()),
                None => (None, filter.clone()),
            };
            let (removed, persistent) = if let Some(session) = st.sessions.get_mut(client_id) {
                (
                    session.remove_subscription(&match_filter, share_name.as_deref()),
                    session.persistent,
                )
            } else {
                (false, false)
            };
            if removed && persistent {
                self.inner
                    .storage
                    .delete_subscription(client_id.to_string(), filter.clone());
            }
            reason_codes.push(if removed {
                ReasonCode::Success
            } else {
                ReasonCode::NoSubscriptionExisted
            });
        }
        let unsuback = SubAck::new(unsub.packet_id, reason_codes);
        send_to(
            &st,
            client_id,
            Outgoing::Control(Box::new(Packet::Unsuback(unsuback))),
        );
        Action::Continue
    }

    // ---------------------------------------------------------------------
    // DISCONNECT (client-initiated) and connection teardown
    // ---------------------------------------------------------------------

    fn handle_client_disconnect(&self, client_id: &str, _epoch: u64, d: Disconnect) -> Action {
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
                    self.inner
                        .storage
                        .upsert_session(client_id.to_string(), s.session_expiry_interval);
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
            self.schedule_will(client_id.to_string(), epoch, message, delay);
        }

        // Handle session expiry.
        if expiry == 0 {
            let mut st = self.lock();
            if let Some(s) = st.sessions.get(client_id) {
                if s.epoch == epoch && !s.is_online() {
                    st.sessions.remove(client_id);
                    self.inner.storage.delete_session(client_id.to_string());
                }
            }
        } else {
            self.schedule_expiry(client_id.to_string(), epoch, expiry);
        }
    }

    fn schedule_will(&self, client_id: String, epoch: u64, message: Message, delay: u32) {
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
                    st.sessions.remove(&client_id);
                    broker.inner.storage.delete_session(client_id.clone());
                }
            }
        });
    }

    // ---------------------------------------------------------------------
    // Routing
    // ---------------------------------------------------------------------

    /// Deliver `message` to all matching subscribers. Returns true if at least
    /// one subscriber matched (used for the No-Matching-Subscribers reason).
    fn route(&self, st: &mut State, publisher: Option<&str>, message: &Message) -> bool {
        if message.is_expired() {
            return false;
        }

        struct Plan {
            client_id: String,
            qos: QoS,
            retain: bool,
            sub_ids: Vec<u32>,
        }
        let mut plans: Vec<Plan> = Vec::new();
        // Shared groups: (share, filter) -> candidate deliveries.
        let mut shared: HashMap<(String, String), Vec<Plan>> = HashMap::new();

        for (cid, session) in st.sessions.iter() {
            let mut best: Option<QoS> = None;
            let mut sub_ids: Vec<u32> = Vec::new();
            let mut retain_flag = false;

            for sub in &session.subscriptions {
                if !topic::matches(&sub.filter, &message.topic) {
                    continue;
                }
                if let Some(share) = &sub.share_name {
                    // Shared subscription candidate (one member chosen later).
                    if sub.no_local && publisher == Some(cid.as_str()) {
                        continue;
                    }
                    let eff = message.qos.min(sub.qos);
                    let ids = sub
                        .subscription_identifier
                        .map(|i| vec![i])
                        .unwrap_or_default();
                    shared
                        .entry((share.clone(), sub.filter.clone()))
                        .or_default()
                        .push(Plan {
                            client_id: cid.clone(),
                            qos: eff,
                            retain: if sub.retain_as_published {
                                message.retain
                            } else {
                                false
                            },
                            sub_ids: ids,
                        });
                    continue;
                }
                // No Local: do not echo back to the publishing client (3.8.3.1).
                if sub.no_local && publisher == Some(cid.as_str()) {
                    continue;
                }
                let eff = message.qos.min(sub.qos);
                best = Some(match best {
                    Some(b) if b.as_u8() >= eff.as_u8() => b,
                    _ => eff,
                });
                if let Some(id) = sub.subscription_identifier {
                    sub_ids.push(id);
                }
                if sub.retain_as_published {
                    retain_flag = true;
                }
            }

            if let Some(qos) = best {
                plans.push(Plan {
                    client_id: cid.clone(),
                    qos,
                    retain: if retain_flag { message.retain } else { false },
                    sub_ids,
                });
            }
        }

        // Choose one member per shared group (round-robin).
        for ((share, filter), mut candidates) in shared {
            if candidates.is_empty() {
                continue;
            }
            let cursor = st.shared_rr.entry((share, filter)).or_insert(0);
            let idx = *cursor % candidates.len();
            *cursor = cursor.wrapping_add(1);
            plans.push(candidates.swap_remove(idx));
        }

        let matched = !plans.is_empty();
        Metrics::add(&self.inner.metrics.publish_delivered, plans.len() as u64);
        for plan in plans {
            if let Some(session) = st.sessions.get_mut(&plan.client_id) {
                deliver_to_session(session, message, plan.qos, plan.retain, &plan.sub_ids);
            }
        }
        matched
    }
}

/// Build a routable Will Message from a CONNECT Will (3.1.3.2).
fn message_from_will(w: &crate::packet::Will) -> Message {
    let expires_at = w
        .properties
        .message_expiry_interval
        .map(|s| crate::message::now_unix().saturating_add(s as u64));
    Message {
        topic: w.topic.clone(),
        payload: w.payload.clone(),
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

/// Deliver a single message to one session, applying its QoS state machine.
fn deliver_to_session(
    session: &mut Session,
    message: &Message,
    qos: QoS,
    retain: bool,
    sub_ids: &[u32],
) {
    if message.is_expired() {
        return;
    }
    match qos {
        QoS::AtMostOnce => {
            if let Some(tx) = &session.out {
                let publish = message.to_publish(QoS::AtMostOnce, None, retain, sub_ids);
                let _ = tx.send(Outgoing::Send(Box::new(publish)));
            }
            // QoS 0 is never queued for offline sessions.
        }
        _ => {
            if session.is_online() && session.window_open() {
                if let Some(id) = session.next_id() {
                    let publish = message.to_publish(qos, Some(id), retain, sub_ids);
                    match qos {
                        QoS::AtLeastOnce => {
                            session.awaiting_puback.insert(id, publish.clone());
                        }
                        QoS::ExactlyOnce => {
                            session.awaiting_pubrec.insert(id, publish.clone());
                        }
                        QoS::AtMostOnce => unreachable!(),
                    }
                    if let Some(tx) = &session.out {
                        let _ = tx.send(Outgoing::Send(Box::new(publish)));
                    }
                    return;
                }
            }
            // Offline or window full: queue for later.
            session.queue.push_back(Pending {
                message: message.clone(),
                qos,
                retain,
                subscription_ids: sub_ids.to_vec(),
            });
        }
    }
}

/// Send queued messages while the inflight window has room and the client is
/// online. Called after a PUBACK/PUBCOMP frees a slot, or on resume.
fn flush_queue(session: &mut Session) {
    while session.is_online() && session.window_open() {
        let Some(pending) = session.queue.pop_front() else {
            break;
        };
        if pending.message.is_expired() {
            continue;
        }
        let Some(id) = session.next_id() else {
            session.queue.push_front(pending);
            break;
        };
        let publish = pending.message.to_publish(
            pending.qos,
            Some(id),
            pending.retain,
            &pending.subscription_ids,
        );
        match pending.qos {
            QoS::AtLeastOnce => {
                session.awaiting_puback.insert(id, publish.clone());
            }
            QoS::ExactlyOnce => {
                session.awaiting_pubrec.insert(id, publish.clone());
            }
            QoS::AtMostOnce => {}
        }
        if let Some(tx) = &session.out {
            let _ = tx.send(Outgoing::Send(Box::new(publish)));
        }
    }
}

/// On session resume, redeliver unacknowledged messages (4.4) then flush the
/// offline queue.
fn resume_delivery(session: &mut Session) {
    let Some(tx) = session.out.clone() else {
        return;
    };
    // Re-send QoS 1 & 2 PUBLISH packets with DUP=1, preserving packet ids.
    for publish in session.awaiting_puback.values() {
        let mut p = publish.clone();
        p.dup = true;
        let _ = tx.send(Outgoing::Send(Box::new(p)));
    }
    for publish in session.awaiting_pubrec.values() {
        let mut p = publish.clone();
        p.dup = true;
        let _ = tx.send(Outgoing::Send(Box::new(p)));
    }
    // Re-send PUBREL for QoS 2 messages awaiting PUBCOMP.
    let pubcomp_ids: Vec<u16> = session.awaiting_pubcomp.iter().copied().collect();
    for id in pubcomp_ids {
        let _ = tx.send(Outgoing::Control(Box::new(Packet::Pubrel(PubAck::new(
            id,
            ReasonCode::Success,
        )))));
    }
    flush_queue(session);
}

/// Push an outgoing item to a client if it is online.
fn send_to(st: &State, client_id: &str, item: Outgoing) {
    if let Some(s) = st.sessions.get(client_id) {
        if let Some(tx) = &s.out {
            let _ = tx.send(item);
        }
    }
}

/// For validation we need the filter with any `$share/{name}/` prefix removed;
/// `parse_filter` handles the real split, this is just for the validity check.
fn strip_share_for_validation(filter: &str) -> &str {
    if let Some(rest) = filter.strip_prefix("$share/") {
        if let Some(slash) = rest.find('/') {
            return &rest[slash + 1..];
        }
    }
    filter
}
