//! Inbound PUBLISH (3.3) and the acknowledgement state machines for our own
//! outbound QoS 1 and QoS 2 deliveries (3.4 - 3.7).
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::routing::{flush_queue, send_to};
use super::*;

impl Broker {
    pub(super) fn handle_publish(
        &self,
        client_id: &str,
        _epoch: u64,
        mut publish: Publish,
    ) -> Action {
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

        // `$SYS` is broker-owned status (see `publish_sys`). A client writing
        // there could forge broker statistics for every $SYS subscriber, so it
        // is refused regardless of ACL. Checked after alias resolution so an
        // alias cannot be used to smuggle the topic in.
        if publish.topic.starts_with(SYS_PREFIX) {
            tracing::debug!(
                client_id,
                topic = %publish.topic,
                "refusing client PUBLISH to the broker-owned $SYS hierarchy"
            );
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

    pub(super) fn handle_puback(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            s.awaiting_puback.remove(&ack.packet_id);
            flush_queue(s);
        }
        Action::Continue
    }

    pub(super) fn handle_pubrec(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            let known = s.awaiting_pubrec.remove(&ack.packet_id).is_some();
            if known && ack.reason_code.is_error() {
                // Client rejected the message (MQTT-4.3.3-4): no PUBREL, and
                // the id is already free since we removed it above.
                return Action::Continue;
            }
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

    pub(super) fn handle_pubrel(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
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

    pub(super) fn handle_pubcomp(&self, client_id: &str, _epoch: u64, ack: PubAck) -> Action {
        let mut st = self.lock();
        if let Some(s) = st.sessions.get_mut(client_id) {
            s.awaiting_pubcomp.remove(&ack.packet_id);
            flush_queue(s);
        }
        Action::Continue
    }
}
