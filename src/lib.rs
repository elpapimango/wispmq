//! An MQTT v5.0 broker library.
//!
//! Two clippy lints are allowed crate-wide by design: the control-packet and
//! frame enums intentionally have variants of very different sizes
//! (`large_enum_variant`), and the CONNECT handshake returns a rejection
//! CONNACK by value on the error path (`result_large_err`).
#![allow(clippy::large_enum_variant, clippy::result_large_err)]
//!
//! Modules mirror the layered design: `codec` (wire primitives + properties),
//! `packet` (the 15 control packets) and `topic` (filter matching) come from
//! the `wispmq-protocol` crate (shared with `wispmq-cli`) and are re-exported
//! here unchanged; `message` (the routable application message), `broker`
//! (sessions + routing), `storage` (SQLite persistence) and `server` (tokio
//! networking) are broker-specific and stay in this crate.

pub mod acl;
pub mod admin;
pub mod auth;
pub mod bridge;
pub mod broker;
pub(crate) mod cli;
pub mod config;
pub mod error;
pub mod message;
pub mod metrics;
pub mod otel;
pub mod ratelimit;
pub mod server;
pub mod storage;
pub mod sysinfo;
pub mod tls;
pub mod ws;

pub use wispmq_protocol::{codec, framing, packet, topic, types};
