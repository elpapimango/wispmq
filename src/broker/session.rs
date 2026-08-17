//! Per-client Session State (Section 4.1): subscriptions, the QoS 1/2 delivery
//! state machines, offline message queue, and packet-identifier allocation.

use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::mpsc::UnboundedSender;

use crate::message::Message;
use crate::packet::Publish;
use crate::types::QoS;

/// A message waiting in a session's offline queue (or to be sent on resume).
#[derive(Debug, Clone)]
pub struct Pending {
    pub message: Message,
    pub qos: QoS,
    pub retain: bool,
    pub subscription_ids: Vec<u32>,
}

/// A stored subscription belonging to a session.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Matching filter, with any `$share/{name}/` prefix stripped off.
    pub filter: String,
    /// Share name for a shared subscription, else `None`.
    pub share_name: Option<String>,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub subscription_identifier: Option<u32>,
}

/// The message-to-send channel to a client's writer task, plus a control
/// signal to disconnect it (used for takeover / keep-alive).
#[derive(Debug)]
pub enum Outgoing {
    Send(Box<Publish>),
    /// Any already-encoded control packet (acks etc.).
    Control(Box<crate::packet::Packet>),
    /// Ask the connection to send a DISCONNECT with this reason and close.
    Shutdown(crate::types::ReasonCode),
}

pub type OutTx = UnboundedSender<Outgoing>;

pub struct Session {
    pub client_id: String,
    /// Authenticated identity (mutual-TLS certificate CN), or `None` when the
    /// connection presented no client certificate. Drives ACL decisions.
    pub identity: Option<String>,
    /// Increments on every new network connection bound to this session; used
    /// to detect and ignore stale timers after takeover.
    pub epoch: u64,
    /// Online link to the current connection, or `None` when disconnected.
    pub out: Option<OutTx>,
    pub subscriptions: Vec<Subscription>,
    /// Whether this session's state is persisted to disk. True only for named
    /// clients with a non-zero Session Expiry Interval.
    pub persistent: bool,
    /// Session Expiry Interval in seconds (0 = expire on disconnect).
    pub session_expiry_interval: u32,
    /// Will message to publish on abnormal disconnection, plus its delay.
    pub will: Option<(Message, u32)>,
    /// The client's Receive Maximum (bounds our outbound inflight window).
    pub client_receive_maximum: u16,
    /// The client's Maximum Packet Size, if advertised.
    pub client_max_packet_size: Option<u32>,

    // ---- Outbound QoS state ----
    next_packet_id: u16,
    /// QoS 1 publishes awaiting PUBACK (kept for redelivery on resume).
    pub awaiting_puback: HashMap<u16, Publish>,
    /// QoS 2 publishes awaiting PUBREC (kept for redelivery on resume).
    pub awaiting_pubrec: HashMap<u16, Publish>,
    /// QoS 2 packet ids for which we sent PUBREL and await PUBCOMP.
    pub awaiting_pubcomp: HashSet<u16>,
    /// Offline / flow-controlled queue of outgoing messages.
    pub queue: VecDeque<Pending>,

    // ---- Inbound QoS 2 state ----
    /// Packet ids received via QoS 2 PUBLISH, awaiting PUBREL (dedup set).
    pub inbound_qos2: HashSet<u16>,
    /// Topic Alias -> Topic Name mappings received from the client (3.3.2.3.4).
    /// Reset on each new network connection.
    pub inbound_aliases: HashMap<u16, String>,
}

impl Session {
    pub fn new(client_id: String, session_expiry_interval: u32) -> Self {
        Session {
            client_id,
            identity: None,
            epoch: 0,
            out: None,
            subscriptions: Vec::new(),
            persistent: false,
            session_expiry_interval,
            will: None,
            client_receive_maximum: 65_535,
            client_max_packet_size: None,
            next_packet_id: 1,
            awaiting_puback: HashMap::new(),
            awaiting_pubrec: HashMap::new(),
            awaiting_pubcomp: HashSet::new(),
            queue: VecDeque::new(),
            inbound_qos2: HashSet::new(),
            inbound_aliases: HashMap::new(),
        }
    }

    pub fn is_online(&self) -> bool {
        self.out.is_some()
    }

    /// Number of outstanding outbound QoS>0 messages (the inflight window).
    pub fn inflight_count(&self) -> usize {
        self.awaiting_puback.len() + self.awaiting_pubrec.len() + self.awaiting_pubcomp.len()
    }

    /// Whether the outbound inflight window has room, honouring the client's
    /// Receive Maximum (3.3.4 / 4.9).
    pub fn window_open(&self) -> bool {
        self.inflight_count() < self.client_receive_maximum as usize
    }

    /// Allocate the next unused packet identifier, or `None` if exhausted.
    pub fn next_id(&mut self) -> Option<u16> {
        for _ in 0..=u16::MAX {
            let id = self.next_packet_id;
            self.next_packet_id = if id == u16::MAX { 1 } else { id + 1 };
            if !self.id_in_use(id) {
                return Some(id);
            }
        }
        None
    }

    fn id_in_use(&self, id: u16) -> bool {
        self.awaiting_puback.contains_key(&id)
            || self.awaiting_pubrec.contains_key(&id)
            || self.awaiting_pubcomp.contains(&id)
    }

    /// Replace an existing subscription with the same filter, or add a new one.
    /// Returns true if a subscription with this filter already existed.
    pub fn upsert_subscription(&mut self, sub: Subscription) -> bool {
        if let Some(existing) = self
            .subscriptions
            .iter_mut()
            .find(|s| s.filter == sub.filter && s.share_name == sub.share_name)
        {
            *existing = sub;
            true
        } else {
            self.subscriptions.push(sub);
            false
        }
    }

    /// Remove a subscription by its full filter (share prefix already stripped
    /// for matching). Returns true if one was removed.
    pub fn remove_subscription(&mut self, filter: &str, share_name: Option<&str>) -> bool {
        let before = self.subscriptions.len();
        self.subscriptions
            .retain(|s| !(s.filter == filter && s.share_name.as_deref() == share_name));
        self.subscriptions.len() != before
    }

    /// Reset all volatile connection state, keeping durable session state
    /// (subscriptions, inflight). Called when the client disconnects.
    pub fn go_offline(&mut self) {
        self.out = None;
    }
}
