//! An MQTT v5.0 broker library.
//!
//! Two clippy lints are allowed crate-wide by design: the control-packet and
//! frame enums intentionally have variants of very different sizes
//! (`large_enum_variant`), and the CONNECT handshake returns a rejection
//! CONNACK by value on the error path (`result_large_err`).
#![allow(clippy::large_enum_variant, clippy::result_large_err)]
//!
//! Modules mirror the layered design: `codec` (wire primitives + properties),
//! `packet` (the 15 control packets), `topic` (filter matching), `message`
//! (the routable application message), `broker` (sessions + routing),
//! `storage` (SQLite persistence) and `server` (tokio networking).

pub mod acl;
pub mod admin;
pub mod broker;
pub mod codec;
pub mod config;
pub mod error;
pub mod framing;
pub mod message;
pub mod metrics;
pub mod packet;
pub mod server;
pub mod storage;
pub mod tls;
pub mod topic;
pub mod types;
