//! End-to-end tests for broker-to-broker forwarding (bridge): two in-process
//! brokers, bridged, with delivery asserted in both directions and across a
//! reconnect.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use wispmq::acl::Acl;
use wispmq::bridge::{BridgeConfig, BridgeTopic, Direction};
use wispmq::broker::Broker;
use wispmq::codec::Properties;
use wispmq::config::Config;
use wispmq::framing::{read_packet, write_packet, ReadOutcome};
use wispmq::message::Message;
use wispmq::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use wispmq::storage::Storage;
use wispmq::types::{PacketType, ProtocolVersion::V5, QoS, ReasonCode};

mod common;
use common::free_addr;

/// Build a broker serving MQTT on `addr` and return the handle.
fn make_broker(addr: SocketAddr) -> Broker {
    let config = Config {
        listen_addr: addr,
        ..Config::default()
    };
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    );
    let b = broker.clone();
    tokio::spawn(async move {
        let _ = wispmq::server::run(b).await;
    });
    broker
}

fn bridge_config(name: &str, remote: SocketAddr) -> BridgeConfig {
    BridgeConfig {
        name: name.into(),
        address: format!("tcp://{remote}"),
        client_id: format!("br-{name}"),
        username: None,
        password: None,
        keepalive: 30,
        protocol_version: V5,
        tls_ca: None,
        tls_cert: None,
        tls_key: None,
        tls_insecure: false,
        topics: vec![
            BridgeTopic {
                pattern: "a2b/#".into(),
                direction: Direction::Out,
                qos: QoS::AtLeastOnce,
            },
            BridgeTopic {
                pattern: "b2a/#".into(),
                direction: Direction::In,
                qos: QoS::AtLeastOnce,
            },
        ],
    }
}

// ---- minimal MQTT client helpers (raw codec over TCP) ----

async fn connect(addr: SocketAddr, client_id: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let c = Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: client_id.into(),
        will: None,
        username: None,
        password: None,
    });
    write_packet(&mut s, &c, V5).await.unwrap();
    match read_packet(&mut s, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Connack(a), _) => {
            assert_eq!(a.reason_code, ReasonCode::Success)
        }
        _ => panic!("expected CONNACK"),
    }
    s
}

async fn subscribe(s: &mut TcpStream, filter: &str) {
    let sub = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: filter.into(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: wispmq::packet::RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(s, &sub, V5).await.unwrap();
    match read_packet(s, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Suback(_), _) => {}
        _ => panic!("expected SUBACK"),
    }
}

async fn publish(s: &mut TcpStream, topic: &str, payload: &[u8], qos: QoS) {
    let packet_id = (qos != QoS::AtMostOnce).then_some(9);
    let p = Packet::Publish(Publish {
        dup: false,
        qos,
        retain: false,
        topic: topic.into(),
        packet_id,
        properties: Properties::new(),
        payload: payload.into(),
    });
    write_packet(s, &p, V5).await.unwrap();
    if qos == QoS::AtLeastOnce {
        // Read the PUBACK.
        match read_packet(s, 1 << 20, V5).await.unwrap() {
            ReadOutcome::Packet(Packet::Puback(_), _) => {}
            _ => panic!("expected PUBACK"),
        }
    }
}

/// Wait for the next PUBLISH and return (topic, payload), or panic on timeout.
async fn expect_publish(s: &mut TcpStream) -> (String, Vec<u8>) {
    loop {
        let out = timeout(Duration::from_secs(5), read_packet(s, 1 << 20, V5))
            .await
            .expect("timed out waiting for forwarded PUBLISH")
            .unwrap();
        if let ReadOutcome::Packet(Packet::Publish(p), _) = out {
            return (p.topic, p.payload.to_vec());
        }
    }
}

async fn wait_for_bridge(broker: &Broker) {
    for _ in 0..100 {
        if broker.snapshot().bridges_connected >= 1 {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("bridge did not connect");
}

#[tokio::test]
async fn bridge_forwards_both_directions() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let broker_a = make_broker(addr_a);
    let _broker_b = make_broker(addr_b);
    sleep(Duration::from_millis(100)).await;

    // Bridge lives on A and connects to B.
    tokio::spawn(wispmq::bridge::run(
        broker_a.clone(),
        bridge_config("t", addr_b),
    ));
    wait_for_bridge(&broker_a).await;
    // Give the remote SUBSCRIBE (for the "in" topic) a moment to register.
    sleep(Duration::from_millis(200)).await;

    // A -> B (out): subscriber on B receives a message published on A.
    let mut sub_b = connect(addr_b, "sub-b").await;
    subscribe(&mut sub_b, "a2b/#").await;
    let mut pub_a = connect(addr_a, "pub-a").await;
    publish(&mut pub_a, "a2b/hello", b"from-a", QoS::AtMostOnce).await;
    let (topic, payload) = expect_publish(&mut sub_b).await;
    assert_eq!(topic, "a2b/hello");
    assert_eq!(payload, b"from-a");

    // B -> A (in): subscriber on A receives a message published on B (QoS 1).
    let mut sub_a = connect(addr_a, "sub-a").await;
    subscribe(&mut sub_a, "b2a/#").await;
    let mut pub_b = connect(addr_b, "pub-b").await;
    publish(&mut pub_b, "b2a/hi", b"from-b", QoS::AtLeastOnce).await;
    let (topic, payload) = expect_publish(&mut sub_a).await;
    assert_eq!(topic, "b2a/hi");
    assert_eq!(payload, b"from-b");
}

#[tokio::test]
async fn bridge_reconnects_when_remote_starts_late() {
    let addr_a = free_addr();
    let addr_b = free_addr(); // not listening yet
    let broker_a = make_broker(addr_a);

    // Start the bridge before B exists: its first connect fails and it backs off.
    tokio::spawn(wispmq::bridge::run(
        broker_a.clone(),
        bridge_config("late", addr_b),
    ));
    sleep(Duration::from_millis(300)).await;

    // Now bring B up; the bridge should reconnect within a couple of seconds.
    let _broker_b = make_broker(addr_b);
    wait_for_bridge(&broker_a).await;
    sleep(Duration::from_millis(200)).await;

    let mut sub_b = connect(addr_b, "sub-b").await;
    subscribe(&mut sub_b, "a2b/#").await;
    let mut pub_a = connect(addr_a, "pub-a").await;
    publish(&mut pub_a, "a2b/after-reconnect", b"ok", QoS::AtMostOnce).await;
    let (topic, payload) = expect_publish(&mut sub_b).await;
    assert_eq!(topic, "a2b/after-reconnect");
    assert_eq!(payload, b"ok");
}

/// Traffic the bridge exchanges with its remote must land in the shared packet
/// counters.
///
/// CONNECT-sent and CONNACK-received are the giveaways: a broker never sends a
/// CONNECT or receives a CONNACK on a *client* connection, so a non-zero count
/// for either can only have come from a bridge's own link. Broker A here has no
/// clients at all — the bridge owns the only socket it holds — so every byte
/// counted below is bridge traffic and nothing else.
#[tokio::test]
async fn bridge_traffic_is_counted_in_the_packet_metrics() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let broker_a = make_broker(addr_a);
    let _broker_b = make_broker(addr_b);
    sleep(Duration::from_millis(100)).await;

    // Nothing has happened yet, so the giveaway counters start at zero.
    let before = broker_a.snapshot();
    assert_eq!(before.packet_sent[PacketType::Connect as usize], 0);
    assert_eq!(before.packet_received[PacketType::Connack as usize], 0);

    tokio::spawn(wispmq::bridge::run(
        broker_a.clone(),
        bridge_config("metrics", addr_b),
    ));
    wait_for_bridge(&broker_a).await;
    sleep(Duration::from_millis(200)).await;

    // Originate a message on A with no client socket involved, so the forwarded
    // PUBLISH is the only PUBLISH A writes. `$bridge/metrics` is the bridge's own
    // session id; publishing under a different origin keeps its `no_local`
    // subscription from suppressing the forward.
    broker_a.inject_publish(
        "test-origin",
        Message {
            topic: "a2b/counted".into(),
            payload: b"bridge-payload"[..].into(),
            qos: QoS::AtMostOnce,
            retain: false,
            payload_format_indicator: None,
            content_type: None,
            response_topic: None,
            correlation_data: None,
            user_properties: Vec::new(),
            expires_at: None,
        },
    );
    sleep(Duration::from_millis(300)).await;

    let after = broker_a.snapshot();
    assert!(
        after.packet_sent[PacketType::Connect as usize] >= 1,
        "the bridge's CONNECT to the remote was not counted"
    );
    assert!(
        after.packet_received[PacketType::Connack as usize] >= 1,
        "the remote's CONNACK was not counted"
    );
    assert!(
        after.packet_sent[PacketType::Subscribe as usize] >= 1,
        "the bridge's SUBSCRIBE to the remote was not counted"
    );
    assert!(
        after.packet_sent[PacketType::Publish as usize] >= 1,
        "the forwarded PUBLISH was not counted"
    );
    assert!(
        after.packets_sent >= 3 && after.bytes_sent > 0,
        "aggregate sent counters did not move: {} packets, {} bytes",
        after.packets_sent,
        after.bytes_sent
    );
    assert!(
        after.packets_received >= 1 && after.bytes_received > 0,
        "aggregate received counters did not move"
    );
    assert_eq!(
        after.publish_bytes_sent,
        b"bridge-payload".len() as u64,
        "forwarded payload bytes should be counted exactly once"
    );
    // And the bridge-specific counter still works alongside the shared ones.
    assert!(after.bridge_forwarded_out >= 1);
}
