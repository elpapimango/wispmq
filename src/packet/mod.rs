//! MQTT v5.0 Control Packets (Section 3).
//!
//! Each packet type is decoded from / encoded to its full byte form. The
//! fixed header (type nibble + flags + Remaining Length) is handled here; the
//! variable header and payload live in the per-packet modules.

mod connect;
mod other;
mod publish;
mod subscribe;

pub use connect::{Connack, Connect, Will};
pub use other::{Auth, Disconnect};
pub use publish::{PubAck, Publish};
pub use subscribe::{RetainHandling, SubAck, Subscribe, TopicFilter, Unsubscribe};

use crate::codec::{varint_len, Reader, Writer};
use crate::error::{malformed, Result};
use crate::types::PacketType;

/// A fully decoded MQTT control packet.
#[derive(Debug, Clone)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback(PubAck),
    Pubrec(PubAck),
    Pubrel(PubAck),
    Pubcomp(PubAck),
    Subscribe(Subscribe),
    Suback(SubAck),
    Unsubscribe(Unsubscribe),
    Unsuback(SubAck),
    Pingreq,
    Pingresp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::Connack(_) => PacketType::Connack,
            Packet::Publish(_) => PacketType::Publish,
            Packet::Puback(_) => PacketType::Puback,
            Packet::Pubrec(_) => PacketType::Pubrec,
            Packet::Pubrel(_) => PacketType::Pubrel,
            Packet::Pubcomp(_) => PacketType::Pubcomp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::Suback(_) => PacketType::Suback,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::Unsuback(_) => PacketType::Unsuback,
            Packet::Pingreq => PacketType::Pingreq,
            Packet::Pingresp => PacketType::Pingresp,
            Packet::Disconnect(_) => PacketType::Disconnect,
            Packet::Auth(_) => PacketType::Auth,
        }
    }

    /// A short human-readable name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            Packet::Connect(_) => "CONNECT",
            Packet::Connack(_) => "CONNACK",
            Packet::Publish(_) => "PUBLISH",
            Packet::Puback(_) => "PUBACK",
            Packet::Pubrec(_) => "PUBREC",
            Packet::Pubrel(_) => "PUBREL",
            Packet::Pubcomp(_) => "PUBCOMP",
            Packet::Subscribe(_) => "SUBSCRIBE",
            Packet::Suback(_) => "SUBACK",
            Packet::Unsubscribe(_) => "UNSUBSCRIBE",
            Packet::Unsuback(_) => "UNSUBACK",
            Packet::Pingreq => "PINGREQ",
            Packet::Pingresp => "PINGRESP",
            Packet::Disconnect(_) => "DISCONNECT",
            Packet::Auth(_) => "AUTH",
        }
    }

    /// Decode a complete packet from its raw bytes (fixed header included).
    pub fn decode(buf: &[u8]) -> Result<Packet> {
        let mut r = Reader::new(buf);
        let first = r.u8()?;
        let ptype = PacketType::from_u8(first >> 4)?;
        let flags = first & 0x0F;
        let remaining_len = r.varint()? as usize;
        if r.remaining() != remaining_len {
            return Err(malformed(format!(
                "remaining length {remaining_len} does not match buffer ({} bytes left)",
                r.remaining()
            )));
        }
        let mut body = r.sub(remaining_len)?;

        match ptype {
            PacketType::Connect => {
                check_flags(flags, 0, "CONNECT")?;
                Ok(Packet::Connect(Connect::decode(&mut body)?))
            }
            PacketType::Connack => {
                check_flags(flags, 0, "CONNACK")?;
                Ok(Packet::Connack(Connack::decode(&mut body)?))
            }
            PacketType::Publish => Ok(Packet::Publish(Publish::decode(flags, &mut body)?)),
            PacketType::Puback => {
                check_flags(flags, 0, "PUBACK")?;
                Ok(Packet::Puback(PubAck::decode(&mut body, remaining_len)?))
            }
            PacketType::Pubrec => {
                check_flags(flags, 0, "PUBREC")?;
                Ok(Packet::Pubrec(PubAck::decode(&mut body, remaining_len)?))
            }
            PacketType::Pubrel => {
                check_flags(flags, 0b0010, "PUBREL")?;
                Ok(Packet::Pubrel(PubAck::decode(&mut body, remaining_len)?))
            }
            PacketType::Pubcomp => {
                check_flags(flags, 0, "PUBCOMP")?;
                Ok(Packet::Pubcomp(PubAck::decode(&mut body, remaining_len)?))
            }
            PacketType::Subscribe => {
                check_flags(flags, 0b0010, "SUBSCRIBE")?;
                Ok(Packet::Subscribe(Subscribe::decode(&mut body)?))
            }
            PacketType::Suback => {
                check_flags(flags, 0, "SUBACK")?;
                Ok(Packet::Suback(SubAck::decode(&mut body)?))
            }
            PacketType::Unsubscribe => {
                check_flags(flags, 0b0010, "UNSUBSCRIBE")?;
                Ok(Packet::Unsubscribe(Unsubscribe::decode(&mut body)?))
            }
            PacketType::Unsuback => {
                check_flags(flags, 0, "UNSUBACK")?;
                Ok(Packet::Unsuback(SubAck::decode(&mut body)?))
            }
            PacketType::Pingreq => {
                check_flags(flags, 0, "PINGREQ")?;
                Ok(Packet::Pingreq)
            }
            PacketType::Pingresp => {
                check_flags(flags, 0, "PINGRESP")?;
                Ok(Packet::Pingresp)
            }
            PacketType::Disconnect => {
                check_flags(flags, 0, "DISCONNECT")?;
                Ok(Packet::Disconnect(Disconnect::decode(
                    &mut body,
                    remaining_len,
                )?))
            }
            PacketType::Auth => {
                check_flags(flags, 0, "AUTH")?;
                Ok(Packet::Auth(Auth::decode(&mut body, remaining_len)?))
            }
        }
    }

    /// Encode this packet to a fresh byte vector, fixed header included.
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Each encoder produces (flags, body). We then prepend the fixed header.
        let (flags, body) = match self {
            Packet::Connect(p) => (0u8, p.encode_body()?),
            Packet::Connack(p) => (0, p.encode_body()?),
            Packet::Publish(p) => (p.fixed_flags(), p.encode_body()?),
            Packet::Puback(p) => (0, p.encode_body()?),
            Packet::Pubrec(p) => (0, p.encode_body()?),
            Packet::Pubrel(p) => (0b0010, p.encode_body()?),
            Packet::Pubcomp(p) => (0, p.encode_body()?),
            Packet::Subscribe(p) => (0b0010, p.encode_body()?),
            Packet::Suback(p) => (0, p.encode_body()?),
            Packet::Unsubscribe(p) => (0b0010, p.encode_body()?),
            Packet::Unsuback(p) => (0, p.encode_body()?),
            Packet::Pingreq => (0, Vec::new()),
            Packet::Pingresp => (0, Vec::new()),
            Packet::Disconnect(p) => (0, p.encode_body()?),
            Packet::Auth(p) => (0, p.encode_body()?),
        };

        let mut out = Writer::with_capacity(body.len() + 5);
        out.put_u8((self.packet_type() as u8) << 4 | flags);
        out.put_varint(body.len() as u32)?;
        out.put_bytes(&body);
        Ok(out.into_vec())
    }

    /// Total encoded size in bytes, used to enforce Maximum Packet Size before
    /// actually serializing (3.1.2.11.4).
    pub fn encoded_size(&self) -> Result<usize> {
        // Cheap path: re-use encode() length. Bodies here are small for control
        // packets; for PUBLISH the caller usually knows this already.
        Ok(self.encode()?.len())
    }
}

/// Convenience for computing a fixed header + body length without a second
/// allocation when only the size is needed.
pub fn total_len(body_len: usize) -> usize {
    1 + varint_len(body_len as u32) + body_len
}

fn check_flags(flags: u8, expected: u8, name: &str) -> Result<()> {
    if flags != expected {
        return Err(malformed(format!(
            "invalid fixed header flags 0x{flags:X} for {name}"
        )));
    }
    Ok(())
}
