//! Message routing: matching a publication against every subscription, choosing
//! one member per shared-subscription group, and the per-session delivery and
//! queue-flushing that follows.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::*;

impl Broker {
    /// Deliver `message` to all matching subscribers. Returns true if at least
    /// one subscriber matched (used for the No-Matching-Subscribers reason).
    pub(super) fn route(&self, st: &mut State, publisher: Option<&str>, message: &Message) -> bool {
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
        let max_queued = self.inner.config.max_queued_messages as usize;
        let mut dropped = 0u64;
        for plan in plans {
            if let Some(session) = st.sessions.get_mut(&plan.client_id) {
                if deliver_to_session(
                    session,
                    message,
                    plan.qos,
                    plan.retain,
                    &plan.sub_ids,
                    max_queued,
                ) {
                    dropped += 1;
                }
            }
        }
        if dropped > 0 {
            Metrics::add(&self.inner.metrics.publish_dropped, dropped);
        }
        matched
    }
}

/// Deliver a single message to one session, applying its QoS state machine.
///
/// `max_queued` bounds the offline queue (0 = unlimited). Returns `true` when a
/// message had to be dropped to stay within that bound, so the caller can count
/// it.
#[must_use]
pub(super) fn deliver_to_session(
    session: &mut Session,
    message: &Message,
    qos: QoS,
    retain: bool,
    sub_ids: &[u32],
    max_queued: usize,
) -> bool {
    let mut dropped = false;
    if message.is_expired() {
        return dropped;
    }
    match qos {
        QoS::AtMostOnce => {
            if let Some(tx) = &session.out {
                let publish = message.to_publish(QoS::AtMostOnce, None, retain, sub_ids);
                let _ = tx.send(Outgoing::Send(Box::new(publish)));
            }
            // QoS 0 is never queued for offline sessions.
        }
        QoS::AtLeastOnce | QoS::ExactlyOnce => {
            if session.is_online() && session.window_open() {
                if let Some(id) = session.next_id() {
                    let publish = message.to_publish(qos, Some(id), retain, sub_ids);
                    // Matching on the two-variant set the outer arm admits keeps
                    // this exhaustive without an `unreachable!()` that a future
                    // refactor of the outer match could turn into a live panic.
                    if qos == QoS::AtLeastOnce {
                        session.awaiting_puback.insert(id, publish.clone());
                    } else {
                        session.awaiting_pubrec.insert(id, publish.clone());
                    }
                    if let Some(tx) = &session.out {
                        let _ = tx.send(Outgoing::Send(Box::new(publish)));
                    }
                    return dropped;
                }
            }
            // Offline or window full: queue for later, up to the configured
            // bound. An unbounded queue lets a durable subscriber to a busy
            // topic consume memory for the whole session-expiry window while
            // disconnected, which exhausts a small host. Dropping the oldest
            // keeps the most recent state, which is what a reconnecting client
            // usually wants.
            if max_queued > 0 && session.queue.len() >= max_queued {
                session.queue.pop_front();
                dropped = true;
            }
            session.queue.push_back(Pending {
                message: message.clone(),
                qos,
                retain,
                subscription_ids: sub_ids.to_vec(),
            });
        }
    }
    dropped
}

/// Send queued messages while the inflight window has room and the client is
/// online. Called after a PUBACK/PUBCOMP frees a slot, or on resume.
pub(super) fn flush_queue(session: &mut Session) {
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
pub(super) fn resume_delivery(session: &mut Session) {
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
pub(super) fn send_to(st: &State, client_id: &str, item: Outgoing) {
    if let Some(s) = st.sessions.get(client_id) {
        if let Some(tx) = &s.out {
            let _ = tx.send(item);
        }
    }
}
