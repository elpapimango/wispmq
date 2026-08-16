//! The application message that flows through the broker, decoupled from the
//! wire-level PUBLISH packet so it can be queued, retained and persisted.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::codec::Properties;
use crate::packet::Publish;
use crate::types::QoS;

/// An application message ready to be routed to subscribers.
#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    /// Publication properties that are forwarded to subscribers: payload
    /// format indicator, content type, response topic, correlation data, user
    /// properties (3.3.2.3). Subscription identifiers are added per-subscriber.
    pub payload_format_indicator: Option<u8>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub user_properties: Vec<(String, String)>,
    /// Absolute Unix time (seconds) at which the message expires, derived from
    /// the Message Expiry Interval (3.3.2.3.3). `None` means no expiry.
    pub expires_at: Option<u64>,
}

impl Message {
    /// Build a routable message from an inbound PUBLISH.
    pub fn from_publish(p: &Publish) -> Self {
        let expires_at = p
            .properties
            .message_expiry_interval
            .map(|secs| now_unix().saturating_add(secs as u64));
        Message {
            topic: p.topic.clone(),
            payload: p.payload.clone(),
            qos: p.qos,
            retain: p.retain,
            payload_format_indicator: p.properties.payload_format_indicator,
            content_type: p.properties.content_type.clone(),
            response_topic: p.properties.response_topic.clone(),
            correlation_data: p.properties.correlation_data.clone(),
            user_properties: p.properties.user_properties.clone(),
            expires_at,
        }
    }

    /// Remaining Message Expiry Interval in seconds, or `None` if unset.
    /// Returns `Some(0)` when already expired.
    pub fn remaining_expiry(&self) -> Option<u32> {
        self.expires_at.map(|deadline| {
            let now = now_unix();
            if deadline <= now {
                0
            } else {
                (deadline - now).min(u32::MAX as u64) as u32
            }
        })
    }

    pub fn is_expired(&self) -> bool {
        matches!(self.remaining_expiry(), Some(0))
    }

    /// Render this message into an outbound PUBLISH for a specific subscriber.
    ///
    /// `qos` is the effective delivery QoS (min of publish and subscription),
    /// `packet_id` is required for QoS > 0, `retain` reflects the RETAIN flag
    /// this subscriber should see, and `subscription_ids` carries any matching
    /// Subscription Identifiers (3.3.4).
    pub fn to_publish(
        &self,
        qos: QoS,
        packet_id: Option<u16>,
        retain: bool,
        subscription_ids: &[u32],
    ) -> Publish {
        let mut properties = Properties::new();
        properties.payload_format_indicator = self.payload_format_indicator;
        properties.content_type = self.content_type.clone();
        properties.response_topic = self.response_topic.clone();
        properties.correlation_data = self.correlation_data.clone();
        properties.user_properties = self.user_properties.clone();
        properties.message_expiry_interval = self.remaining_expiry();
        properties.subscription_identifiers = subscription_ids.to_vec();

        Publish {
            dup: false,
            qos,
            retain,
            topic: self.topic.clone(),
            packet_id: if qos == QoS::AtMostOnce {
                None
            } else {
                packet_id
            },
            properties,
            payload: self.payload.clone(),
        }
    }
}

/// Current Unix time in whole seconds.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
