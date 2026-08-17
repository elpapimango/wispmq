//! SUBSCRIBE and UNSUBSCRIBE (3.8 / 3.10), including retained-message delivery
//! on subscribe and shared-subscription filter handling.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::routing::{deliver_to_session, send_to};
use super::*;

impl Broker {
    pub(super) fn handle_subscribe(&self, client_id: &str, _epoch: u64, sub: Subscribe) -> Action {
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

        let max_queued = self.inner.config.max_queued_messages as usize;
        let mut dropped = 0u64;
        for (msg, eff_qos, ids) in retained_to_send {
            if let Some(session) = st.sessions.get_mut(client_id) {
                if deliver_to_session(session, &msg, eff_qos, true, &ids, max_queued) {
                    dropped += 1;
                }
            }
        }
        if dropped > 0 {
            Metrics::add(&self.inner.metrics.publish_dropped, dropped);
        }

        Action::Continue
    }

    pub(super) fn handle_unsubscribe(
        &self,
        client_id: &str,
        _epoch: u64,
        unsub: Unsubscribe,
    ) -> Action {
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
