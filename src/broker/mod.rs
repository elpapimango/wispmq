//! The broker: shared session registry, message routing, retained-message
//! store, and the handlers the per-connection task drives.
//!
//! All shared state lives behind a single `Mutex`. Handlers never await while
//! holding it — outbound delivery is a non-blocking `try_send` into each
//! client's bounded channel (`session::OUTBOUND_CHANNEL_CAPACITY`) — so a
//! plain `std::sync::Mutex` is used.
//!
//! ## Module layout
//!
//! `impl Broker` is split across sibling files, one per spec area, because a
//! single 1.5k-line block made the seams hard to see. This file holds the
//! shared types (`Broker`, `Inner`, `State`), construction, the lock, and the
//! packet dispatch that fans out to the rest:
//!
//! - `authz`     — ACL snapshot, SIGHUP reload, revoking subscriptions
//! - `stats`     — admin / Prometheus / MCP introspection
//! - `bridges`   — the internal API bridges and `$SYS` publish through
//! - `connect`   — CONNECT: auth, takeover, CONNACK
//! - `publish`   — inbound PUBLISH and the outbound QoS ack machines
//! - `subscribe` — SUBSCRIBE / UNSUBSCRIBE and retained delivery
//! - `lifecycle` — DISCONNECT, teardown, wills, session expiry
//! - `routing`   — matching, shared-group selection, per-session delivery
//! - `session`   — the `Session` type itself
//!
//! The slices use `use super::*` to share this file's imports: they are one
//! type's methods grouped by concern, not independent abstractions. Methods
//! called from another slice are `pub(super)`, which reads as "internal to the
//! broker" — nothing here is part of the crate's public surface unless it is
//! `pub`.

mod authz;
mod bridges;
mod connect;
mod lifecycle;
mod publish;
mod routing;
mod session;
mod stats;
mod subscribe;

// `OutTx`/`Outgoing` are part of the bridge-registration signature, so they
// are public. `Session` is an internal type: re-exported only inside the crate.
pub use session::{OutTx, Outgoing};
pub(crate) use session::{Session, OUTBOUND_CHANNEL_CAPACITY};

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
use crate::ratelimit::RateLimiter;
use crate::storage::{LoadedState, Storage, SubRecord};
use crate::topic;
use crate::types::{ProtocolVersion, QoS, ReasonCode};

/// Identity used for ACL decisions when a connection presents no client
/// certificate (no mutual TLS).
const ANONYMOUS: &str = "anonymous";

/// Root of the broker-owned status hierarchy. Clients may subscribe here but
/// never publish; the broker is the only writer (`Broker::publish_sys`).
pub const SYS_PREFIX: &str = "$SYS/";

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
    /// Process start, for `$SYS/broker/uptime` and `mqtt_uptime_seconds`.
    started_at: std::time::Instant,
    /// Authorization policy, swappable at runtime (reloaded on SIGHUP). Readers
    /// take a cheap snapshot of the `Arc`; a reload replaces it atomically.
    acl: RwLock<Arc<Acl>>,
    /// Username/password credentials. `None` disables password authentication.
    auth: Option<Credentials>,
    /// Per-source-IP connection-rate limiter for the MQTT and WebSocket
    /// listeners; disabled by default. See `crate::ratelimit`.
    rate_limiter: RateLimiter,
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
                    subscription_identifier: sub.subscription_identifier,
                });
            }
            sessions.insert(rec.client_id, s);
        }
        let retained = loaded.retained.into_iter().collect();
        let rate_limiter = RateLimiter::new(
            config.max_connections_per_ip,
            config.connection_rate_window_secs,
        );

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
                started_at: std::time::Instant::now(),
                acl: RwLock::new(Arc::new(acl)),
                auth,
                rate_limiter,
            }),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.inner.rate_limiter
    }

    /// Acquire the single lock guarding all shared broker state.
    ///
    /// **Poisoning policy: fail fast, deliberately.** A poisoned mutex means a
    /// previous holder panicked mid-mutation, so `State` may be torn — a session
    /// inserted but not indexed, a retained message half-replaced. Recovering
    /// with `into_inner()` would let the broker keep routing on structurally
    /// inconsistent state and silently misdeliver messages, which for a message
    /// broker is worse than stopping. So we propagate the panic instead.
    ///
    /// This is only defensible because reaching a poisoned lock should be
    /// impossible: handlers run synchronously under the lock without awaiting,
    /// and `tests/malformed.rs` establishes that decoding hostile input returns
    /// an error rather than panicking. If this ever fires in production it is a
    /// genuine bug, and the panic message is the signal.
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .expect("broker state mutex poisoned: a previous holder panicked mid-mutation")
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
                let _ = tx.try_send(Outgoing::Control(Box::new(packet)));
            }
        }
    }
}
