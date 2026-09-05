//! Error types for the broker.
//!
//! `MqttError` distinguishes protocol-level failures (which map to an MQTT
//! Reason Code and typically cause a DISCONNECT / connection close per the
//! spec) from lower-level I/O and storage failures.

use std::fmt;

use crate::types::ReasonCode;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, MqttError>;

#[derive(Debug)]
pub enum MqttError {
    /// A Malformed Packet: the bytes on the wire could not be decoded per
    /// the MQTT v5.0 grammar. Section 1.2 / 4.13.
    Malformed(String),
    /// A Protocol Error: the packet decoded but violates a MUST rule.
    Protocol(String),
    /// A protocol violation carrying a specific Reason Code to return to the
    /// peer (e.g. in CONNACK or DISCONNECT) before closing the connection.
    Reason(ReasonCode, String),
    /// Underlying transport error.
    Io(std::io::Error),
    /// Storage / persistence error.
    Storage(String),
    /// Configuration / startup error (e.g. invalid TLS material).
    Config(String),
}

impl MqttError {
    /// The Reason Code that best describes this error, for inclusion in a
    /// CONNACK or DISCONNECT packet.
    pub fn reason_code(&self) -> ReasonCode {
        match self {
            MqttError::Malformed(_) => ReasonCode::MalformedPacket,
            MqttError::Protocol(_) => ReasonCode::ProtocolError,
            MqttError::Reason(rc, _) => *rc,
            MqttError::Io(_) => ReasonCode::UnspecifiedError,
            MqttError::Storage(_) => ReasonCode::ImplementationSpecificError,
            MqttError::Config(_) => ReasonCode::UnspecifiedError,
        }
    }
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::Malformed(m) => write!(f, "malformed packet: {m}"),
            MqttError::Protocol(m) => write!(f, "protocol error: {m}"),
            MqttError::Reason(rc, m) => write!(f, "protocol error ({rc:?}): {m}"),
            MqttError::Io(e) => write!(f, "io error: {e}"),
            MqttError::Storage(m) => write!(f, "storage error: {m}"),
            MqttError::Config(m) => write!(f, "config error: {m}"),
        }
    }
}

impl std::error::Error for MqttError {}

impl From<std::io::Error> for MqttError {
    fn from(e: std::io::Error) -> Self {
        MqttError::Io(e)
    }
}

impl From<rusqlite::Error> for MqttError {
    fn from(e: rusqlite::Error) -> Self {
        MqttError::Storage(e.to_string())
    }
}

/// Protocol-layer errors from `wispmq-protocol` (codec/packet/framing/topic)
/// map onto this crate's broader error type one-for-one; `Storage`/`Config`
/// have no protocol-layer counterpart.
impl From<wispmq_protocol::error::MqttError> for MqttError {
    fn from(e: wispmq_protocol::error::MqttError) -> Self {
        use wispmq_protocol::error::MqttError as P;
        match e {
            P::Malformed(m) => MqttError::Malformed(m),
            P::Protocol(m) => MqttError::Protocol(m),
            P::Reason(rc, m) => MqttError::Reason(rc, m),
            P::Io(e) => MqttError::Io(e),
        }
    }
}

/// Convenience constructors.
pub fn malformed(msg: impl Into<String>) -> MqttError {
    MqttError::Malformed(msg.into())
}

pub fn protocol(msg: impl Into<String>) -> MqttError {
    MqttError::Protocol(msg.into())
}
