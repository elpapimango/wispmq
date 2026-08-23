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

        /// A shared-subscription candidate, held until the whole group is known.
        struct Candidate {
            client_id: String,
            qos: QoS,
            retain: bool,
            sub_ids: Vec<u32>,
        }
        // Shared groups: (share, filter) -> candidates. `HashMap::new` does not
        // allocate, so this costs nothing until a shared subscription matches.
        let mut shared: HashMap<(String, String), Vec<Candidate>> = HashMap::new();

        let max_queued = self.inner.config.max_queued_messages as usize;
        let mut delivered = 0u64;
        let mut dropped = 0u64;

        // Ordinary subscriptions are delivered in this single pass: `iter_mut`
        // hands over the session mutably, so nothing needs to be cloned into a
        // plan list and re-looked-up afterwards. Only shared subscriptions
        // still need two phases, because the round-robin choice depends on the
        // whole group and the cursor lives in `State` next to the sessions.
        for (cid, session) in st.sessions.iter_mut() {
            let mut best: Option<QoS> = None;
            let mut sub_ids: Vec<u32> = Vec::new();
            let mut retain_flag = false;

            for sub in &session.subscriptions {
                if !topic::matches(&sub.filter, &message.topic) {
                    continue;
                }
                // No Local: do not echo back to the publishing client (3.8.3.1).
                // Applies to shared and ordinary subscriptions alike.
                if sub.no_local && publisher == Some(cid.as_str()) {
                    continue;
                }
                let eff = message.qos.min(sub.qos);
                if let Some(share) = &sub.share_name {
                    // Shared subscription candidate (one member chosen below).
                    shared
                        .entry((share.clone(), sub.filter.clone()))
                        .or_default()
                        .push(Candidate {
                            client_id: cid.clone(),
                            qos: eff,
                            retain: if sub.retain_as_published {
                                message.retain
                            } else {
                                false
                            },
                            sub_ids: sub
                                .subscription_identifier
                                .map(|i| vec![i])
                                .unwrap_or_default(),
                        });
                    continue;
                }
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
                delivered += 1;
                let retain = if retain_flag { message.retain } else { false };
                if deliver_to_session(session, message, qos, retain, &sub_ids, max_queued) {
                    dropped += 1;
                }
            }
        }

        // Choose one member per shared group (round-robin), then deliver.
        if !shared.is_empty() {
            let mut chosen: Vec<Candidate> = Vec::with_capacity(shared.len());
            for ((share, filter), mut candidates) in shared {
                if candidates.is_empty() {
                    continue;
                }
                let cursor = st.shared_rr.entry((share, filter)).or_insert(0);
                let idx = *cursor % candidates.len();
                *cursor = cursor.wrapping_add(1);
                chosen.push(candidates.swap_remove(idx));
            }
            delivered += chosen.len() as u64;
            for c in chosen {
                if let Some(session) = st.sessions.get_mut(&c.client_id) {
                    if deliver_to_session(session, message, c.qos, c.retain, &c.sub_ids, max_queued)
                    {
                        dropped += 1;
                    }
                }
            }
        }

        Metrics::add(&self.inner.metrics.publish_delivered, delivered);
        if dropped > 0 {
            Metrics::add(&self.inner.metrics.publish_dropped, dropped);
        }
        delivered > 0
    }
}

/// Whether `publish`, as it would appear on the wire, exceeds the client's
/// Maximum Packet Size (`cap`, 3.1.2.11.4, MQTT-3.1.2-24). Only v5 clients can
/// advertise this property (v3.x CONNECTs carry no Properties), so
/// `ProtocolVersion::V5` is always the correct encoding to measure against
/// wherever this is called. A placeholder packet id is fine for sizing before
/// the real one is assigned — a packet identifier always encodes as exactly 2
/// bytes regardless of value, so the measurement is exact, not approximate.
fn exceeds_wire_size(publish: &Publish, cap: u32) -> bool {
    match Packet::Publish(publish.clone()).encode(ProtocolVersion::V5) {
        Ok(bytes) => bytes.len() as u32 > cap,
        Err(_) => false,
    }
}

/// Deliver a single message to one session, applying its QoS state machine.
///
/// `max_queued` bounds the offline queue (0 = unlimited). Returns `true` when a
/// message had to be dropped — to stay within that bound, because the
/// client's outbound channel was full (see `session::OUTBOUND_CHANNEL_
/// CAPACITY`), or because it exceeds the client's advertised Maximum Packet
/// Size — so the caller can count it.
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
                if session
                    .client_max_packet_size
                    .is_some_and(|cap| exceeds_wire_size(&publish, cap))
                {
                    dropped = true;
                } else if tx.try_send(Outgoing::Send(Box::new(publish))).is_err() {
                    // The channel is bounded, so a client that stops draining
                    // its socket eventually fills it. QoS 0 has no offline
                    // queue to fall back to (spec: never queued while
                    // offline), so a full channel just drops the message —
                    // that's what "at most once" means; the alternative would
                    // be unbounded memory growth.
                    dropped = true;
                }
            }
        }
        QoS::AtLeastOnce | QoS::ExactlyOnce => {
            // A message that will never fit under the client's current cap
            // must not be sent *or* queued — queueing would just wait forever
            // for a cap that only changes on the next CONNECT. Checked before
            // the online/offline branch below so both paths are covered by
            // one early return; `next_id()` is deliberately not called here,
            // since the probe never needs a real id (see `exceeds_wire_size`).
            if let Some(cap) = session.client_max_packet_size {
                let probe = message.to_publish(qos, Some(0), retain, sub_ids);
                if exceeds_wire_size(&probe, cap) {
                    dropped = true;
                    return dropped;
                }
            }
            if session.is_online() && session.window_open() {
                if let Some(id) = session.next_id() {
                    let publish = message.to_publish(qos, Some(id), retain, sub_ids);
                    // Try the send *before* recording the id as in-flight: if
                    // the channel is full, nothing was actually sent, so
                    // marking it awaiting-ack would leak the id forever (the
                    // client can never ack a PUBLISH it never received) — the
                    // same class of bug the QoS 2 PUBREC fix (item 7) closed.
                    // Fall through to the queue path instead, exactly as if
                    // the client were offline or its window were full.
                    if let Some(tx) = &session.out {
                        if tx
                            .try_send(Outgoing::Send(Box::new(publish.clone())))
                            .is_ok()
                        {
                            // Matching on the two-variant set the outer arm
                            // admits keeps this exhaustive without an
                            // `unreachable!()` that a future refactor of the
                            // outer match could turn into a live panic.
                            if qos == QoS::AtLeastOnce {
                                session.awaiting_puback.insert(id, publish);
                            } else {
                                session.awaiting_pubrec.insert(id, publish);
                            }
                            return dropped;
                        }
                    }
                }
            }
            // Offline, window full, or the outbound channel was full: queue
            // for later, up to the configured bound. An unbounded queue lets
            // a durable subscriber to a busy topic consume memory for the
            // whole session-expiry window while disconnected, which exhausts
            // a small host. Dropping the oldest keeps the most recent state,
            // which is what a reconnecting client usually wants.
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
/// online. Called after a PUBACK/PUBCOMP frees a slot, or on resume. Returns
/// how many queued messages were dropped for exceeding the client's Maximum
/// Packet Size, so the caller can add it to `mqtt_publish_dropped_total`.
#[must_use]
pub(super) fn flush_queue(session: &mut Session) -> u64 {
    let mut dropped = 0u64;
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
        // A message that no longer fits under the client's current cap (it
        // may have been queued under a looser or absent earlier one) is
        // dropped outright rather than requeued — waiting won't help, the
        // cap only changes on the next CONNECT. `id` was never recorded into
        // `awaiting_puback`/`awaiting_pubrec`, so it isn't "in use" (see
        // `Session::id_in_use`) and is simply available again — no leak.
        if session
            .client_max_packet_size
            .is_some_and(|cap| exceeds_wire_size(&publish, cap))
        {
            dropped += 1;
            continue;
        }
        // Same ordering rule as `deliver_to_session`: only record the id as
        // in-flight once the send actually succeeded. If the channel is
        // full, put the message back at the front and stop — it'll be
        // retried the next time the window opens or the session resumes.
        let sent = match &session.out {
            Some(tx) => tx
                .try_send(Outgoing::Send(Box::new(publish.clone())))
                .is_ok(),
            None => false,
        };
        if !sent {
            session.queue.push_front(pending);
            break;
        }
        match pending.qos {
            QoS::AtLeastOnce => {
                session.awaiting_puback.insert(id, publish);
            }
            QoS::ExactlyOnce => {
                session.awaiting_pubrec.insert(id, publish);
            }
            QoS::AtMostOnce => {}
        }
    }
    dropped
}

/// On session resume, redeliver unacknowledged messages (4.4) then flush the
/// offline queue. Returns how many queued messages `flush_queue` dropped for
/// exceeding the client's Maximum Packet Size.
#[must_use]
pub(super) fn resume_delivery(session: &mut Session) -> u64 {
    let Some(tx) = session.out.clone() else {
        return 0;
    };
    // Re-send QoS 1 & 2 PUBLISH packets with DUP=1, preserving packet ids.
    for publish in session.awaiting_puback.values() {
        let mut p = publish.clone();
        p.dup = true;
        let _ = tx.try_send(Outgoing::Send(Box::new(p)));
    }
    for publish in session.awaiting_pubrec.values() {
        let mut p = publish.clone();
        p.dup = true;
        let _ = tx.try_send(Outgoing::Send(Box::new(p)));
    }
    // Re-send PUBREL for QoS 2 messages awaiting PUBCOMP.
    let pubcomp_ids: Vec<u16> = session.awaiting_pubcomp.iter().copied().collect();
    for id in pubcomp_ids {
        let _ = tx.try_send(Outgoing::Control(Box::new(Packet::Pubrel(PubAck::new(
            id,
            ReasonCode::Success,
        )))));
    }
    flush_queue(session)
}

/// Push an outgoing item to a client if it is online.
pub(super) fn send_to(st: &State, client_id: &str, item: Outgoing) {
    if let Some(s) = st.sessions.get(client_id) {
        if let Some(tx) = &s.out {
            let _ = tx.try_send(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_message(topic: &str) -> Message {
        Message {
            topic: topic.to_string(),
            payload: Arc::from(vec![0u8; 4]),
            qos: QoS::AtMostOnce,
            retain: false,
            payload_format_indicator: None,
            content_type: None,
            response_topic: None,
            correlation_data: None,
            user_properties: Vec::new(),
            expires_at: None,
        }
    }

    /// A full outbound channel is the online counterpart of a full offline
    /// queue: a QoS 0 delivery must be dropped (spec-compliant "at most
    /// once"), never block or grow the channel past its bound.
    #[test]
    fn qos0_drops_when_channel_is_full() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut session = Session::new("c1".into(), 0);
        session.out = Some(tx);
        let msg = test_message("t");

        // First delivery fills the channel (capacity 1); it must succeed.
        assert!(!deliver_to_session(
            &mut session,
            &msg,
            QoS::AtMostOnce,
            false,
            &[],
            0
        ));
        // Second delivery finds the channel full and is dropped.
        assert!(deliver_to_session(
            &mut session,
            &msg,
            QoS::AtMostOnce,
            false,
            &[],
            0
        ));
    }

    /// A QoS 1/2 delivery that finds the channel full must not be recorded
    /// as in-flight — the client never actually received the PUBLISH, so it
    /// can never ack it, and the id would leak forever (the class of bug the
    /// QoS 2 PUBREC fix in item 7 closed for a different code path). It must
    /// fall through to the offline-style queue instead.
    #[test]
    fn qos1_falls_through_to_queue_when_channel_is_full() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut session = Session::new("c1".into(), 0);
        session.out = Some(tx);
        let msg = test_message("t");

        // Fill the channel with an unrelated QoS 0 send first.
        assert!(!deliver_to_session(
            &mut session,
            &msg,
            QoS::AtMostOnce,
            false,
            &[],
            0
        ));

        let dropped = deliver_to_session(&mut session, &msg, QoS::AtLeastOnce, false, &[], 0);
        assert!(
            !dropped,
            "max_queued=0 means unlimited, so nothing should be evicted"
        );
        assert!(
            session.awaiting_puback.is_empty(),
            "id must not be marked in-flight for a message that was never sent"
        );
        assert_eq!(
            session.queue.len(),
            1,
            "message should have been queued instead"
        );
    }

    /// A QoS 0 message that exceeds the client's advertised Maximum Packet
    /// Size (MQTT-3.1.2-24) must be dropped, not sent — the cap is
    /// per-client, so this is the "at most once" drop path, same as a full
    /// channel.
    #[test]
    fn qos0_oversized_publish_is_dropped_not_sent() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut session = Session::new("c1".into(), 0);
        session.out = Some(tx);
        session.client_max_packet_size = Some(3); // smaller than any real PUBLISH
        let msg = test_message("t");

        assert!(deliver_to_session(
            &mut session,
            &msg,
            QoS::AtMostOnce,
            false,
            &[],
            0
        ));
        assert!(
            rx.try_recv().is_err(),
            "oversized message must not reach the channel"
        );
    }

    /// A QoS 1/2 message that exceeds the client's cap must be dropped
    /// outright — not sent, and not queued for a cap that only changes on
    /// the next CONNECT (which would otherwise wait forever), and not
    /// recorded as in-flight (nothing was actually sent to ack).
    #[test]
    fn qos1_oversized_publish_is_dropped_not_queued() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut session = Session::new("c1".into(), 0);
        session.out = Some(tx);
        session.client_max_packet_size = Some(3);
        let msg = test_message("t");

        assert!(deliver_to_session(
            &mut session,
            &msg,
            QoS::AtLeastOnce,
            false,
            &[],
            0
        ));
        assert!(
            rx.try_recv().is_err(),
            "oversized message must not reach the channel"
        );
        assert!(session.queue.is_empty(), "must not be queued either");
        assert!(
            session.awaiting_puback.is_empty(),
            "must not be marked in-flight for a message that was never sent"
        );
    }

    /// A message already sitting in the offline queue (queued under a looser
    /// or absent earlier cap) that no longer fits the client's current cap on
    /// resume must be dropped by `flush_queue`, not resent or requeued.
    #[test]
    fn flush_queue_drops_oversized_pending_without_requeueing() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let mut session = Session::new("c1".into(), 0);
        session.out = Some(tx);
        session.client_max_packet_size = Some(3);
        session.queue.push_back(Pending {
            message: test_message("t"),
            qos: QoS::AtLeastOnce,
            retain: false,
            subscription_ids: Vec::new(),
        });

        let dropped = flush_queue(&mut session);

        assert_eq!(dropped, 1);
        assert!(session.queue.is_empty(), "must not be pushed back");
        assert!(
            rx.try_recv().is_err(),
            "oversized message must not reach the channel"
        );
    }
}
