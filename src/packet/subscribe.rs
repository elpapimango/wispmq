//! SUBSCRIBE (3.8), SUBACK (3.9), UNSUBSCRIBE (3.10), UNSUBACK (3.11).

use crate::codec::{Properties, Reader, Writer};
use crate::error::{malformed, Result};
use crate::types::{QoS, ReasonCode};

/// Retain Handling option in a Subscription Options byte (3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// Send retained messages at the time of the subscribe.
    SendAtSubscribe = 0,
    /// Send retained messages only if the subscription did not already exist.
    SendIfNewSubscription = 1,
    /// Do not send retained messages at subscribe time.
    DoNotSend = 2,
}

impl RetainHandling {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => RetainHandling::SendAtSubscribe,
            1 => RetainHandling::SendIfNewSubscription,
            2 => RetainHandling::DoNotSend,
            _ => return Err(malformed("invalid Retain Handling value 3")),
        })
    }
}

/// One entry in a SUBSCRIBE payload: a Topic Filter plus its options.
#[derive(Debug, Clone)]
pub struct TopicFilter {
    pub filter: String,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    pub properties: Properties,
    pub filters: Vec<TopicFilter>,
}

impl Subscribe {
    pub fn decode(r: &mut Reader) -> Result<Subscribe> {
        let packet_id = r.u16()?;
        if packet_id == 0 {
            return Err(malformed("SUBSCRIBE with Packet Identifier 0"));
        }
        let properties = Properties::decode(r)?;
        let mut filters = Vec::new();
        while r.has_remaining() {
            let filter = r.utf8()?;
            let opts = r.u8()?;
            // Bits 6-7 reserved, MUST be 0 [MQTT-3.8.3-5].
            if opts & 0xC0 != 0 {
                return Err(malformed("reserved bits set in Subscription Options"));
            }
            let qos = QoS::from_u8(opts & 0x03)?;
            let no_local = opts & 0x04 != 0;
            let retain_as_published = opts & 0x08 != 0;
            let retain_handling = RetainHandling::from_u8((opts & 0x30) >> 4)?;
            filters.push(TopicFilter {
                filter,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }
        // [MQTT-3.8.3-2] MUST contain at least one Topic Filter.
        if filters.is_empty() {
            return Err(malformed("SUBSCRIBE with no Topic Filters"));
        }
        Ok(Subscribe {
            packet_id,
            properties,
            filters,
        })
    }

    pub fn encode_body(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        self.properties.encode(&mut w)?;
        for f in &self.filters {
            w.put_utf8(&f.filter);
            let mut opts = f.qos.as_u8();
            if f.no_local {
                opts |= 0x04;
            }
            if f.retain_as_published {
                opts |= 0x08;
            }
            opts |= (f.retain_handling as u8) << 4;
            w.put_u8(opts);
        }
        Ok(w.into_vec())
    }
}

/// SUBACK and UNSUBACK share this shape: a packet id, properties, and a list
/// of Reason Codes (one per requested filter).
#[derive(Debug, Clone)]
pub struct SubAck {
    pub packet_id: u16,
    pub properties: Properties,
    pub reason_codes: Vec<ReasonCode>,
}

impl SubAck {
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        SubAck {
            packet_id,
            properties: Properties::new(),
            reason_codes,
        }
    }

    pub fn decode(r: &mut Reader) -> Result<SubAck> {
        let packet_id = r.u16()?;
        let properties = Properties::decode(r)?;
        let mut reason_codes = Vec::new();
        while r.has_remaining() {
            reason_codes.push(ReasonCode::from_u8(r.u8()?)?);
        }
        Ok(SubAck {
            packet_id,
            properties,
            reason_codes,
        })
    }

    pub fn encode_body(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        self.properties.encode(&mut w)?;
        for rc in &self.reason_codes {
            w.put_u8(rc.as_u8());
        }
        Ok(w.into_vec())
    }
}

#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub properties: Properties,
    pub filters: Vec<String>,
}

impl Unsubscribe {
    pub fn decode(r: &mut Reader) -> Result<Unsubscribe> {
        let packet_id = r.u16()?;
        if packet_id == 0 {
            return Err(malformed("UNSUBSCRIBE with Packet Identifier 0"));
        }
        let properties = Properties::decode(r)?;
        let mut filters = Vec::new();
        while r.has_remaining() {
            filters.push(r.utf8()?);
        }
        if filters.is_empty() {
            return Err(malformed("UNSUBSCRIBE with no Topic Filters"));
        }
        Ok(Unsubscribe {
            packet_id,
            properties,
            filters,
        })
    }

    pub fn encode_body(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        self.properties.encode(&mut w)?;
        for f in &self.filters {
            w.put_utf8(f);
        }
        Ok(w.into_vec())
    }
}
