//! Resource-limit tests: an offline durable session must not be able to grow
//! the broker's memory without bound.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::sleep;

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::codec::Properties;
use pulsemq::config::Config;
use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
use pulsemq::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use pulsemq::storage::Storage;
use pulsemq::types::{ProtocolVersion::V5, QoS, ReasonCode};

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

fn make_broker(addr: SocketAddr, max_queued: u32) -> Broker {
    let config = Config {
        listen_addr: addr,
        max_queued_messages: max_queued,
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
        let _ = pulsemq::server::run(b).await;
    });
    broker
}

/// CONNECT with a persistent session (clean_start = false, expiry > 0).
async fn connect_persistent(addr: SocketAddr, client_id: &str) -> TcpStream {
    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut props = Properties::new();
    props.session_expiry_interval = Some(300);
    let c = Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: false,
        keep_alive: 0,
        properties: props,
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

async fn subscribe_qos1(s: &mut TcpStream, filter: &str) {
    let sub = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: filter.into(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: pulsemq::packet::RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(s, &sub, V5).await.unwrap();
    match read_packet(s, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Suback(_), _) => {}
        _ => panic!("expected SUBACK"),
    }
}

#[tokio::test]
async fn offline_session_queue_is_bounded() {
    const CAP: u32 = 10;
    let addr = free_addr();
    let broker = make_broker(addr, CAP);
    sleep(Duration::from_millis(100)).await;

    // A durable subscriber that then goes away.
    let mut sub = connect_persistent(addr, "durable").await;
    subscribe_qos1(&mut sub, "load/#").await;
    drop(sub);
    sleep(Duration::from_millis(150)).await;

    // Flood the topic while it is offline.
    let mut pubr = TcpStream::connect(addr).await.unwrap();
    let c = Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: "flooder".into(),
        will: None,
        username: None,
        password: None,
    });
    write_packet(&mut pubr, &c, V5).await.unwrap();
    let _ = read_packet(&mut pubr, 1 << 20, V5).await.unwrap();

    // QoS 1: QoS 0 is deliberately never queued for offline sessions, so only
    // QoS>0 traffic can grow the queue at all.
    for i in 0..(CAP * 10) {
        let p = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "load/x".into(),
            packet_id: Some((i % 65535 + 1) as u16),
            properties: Properties::new(),
            payload: format!("msg-{i}").into_bytes(),
        });
        write_packet(&mut pubr, &p, V5).await.unwrap();
        // Drain the PUBACK so the socket buffer cannot stall the flood.
        let _ = read_packet(&mut pubr, 1 << 20, V5).await.unwrap();
    }
    sleep(Duration::from_millis(300)).await;

    // The queue must be capped, and the overflow counted.
    let snap = broker.snapshot();
    assert!(
        snap.publish_dropped > 0,
        "expected drops once the offline queue hit its cap, got {}",
        snap.publish_dropped
    );
}

#[tokio::test]
async fn zero_means_unlimited() {
    // Explicitly configuring 0 restores the unbounded behaviour, so the
    // setting can be opted out of.
    let addr = free_addr();
    let broker = make_broker(addr, 0);
    sleep(Duration::from_millis(100)).await;

    let mut sub = connect_persistent(addr, "durable-unbounded").await;
    subscribe_qos1(&mut sub, "load/#").await;
    drop(sub);
    sleep(Duration::from_millis(150)).await;

    let mut pubr = TcpStream::connect(addr).await.unwrap();
    let c = Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: "flooder2".into(),
        will: None,
        username: None,
        password: None,
    });
    write_packet(&mut pubr, &c, V5).await.unwrap();
    let _ = read_packet(&mut pubr, 1 << 20, V5).await.unwrap();

    for i in 0..50u32 {
        let p = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "load/x".into(),
            packet_id: Some((i % 65535 + 1) as u16),
            properties: Properties::new(),
            payload: format!("m{i}").into_bytes(),
        });
        write_packet(&mut pubr, &p, V5).await.unwrap();
        let _ = read_packet(&mut pubr, 1 << 20, V5).await.unwrap();
    }
    sleep(Duration::from_millis(300)).await;

    assert_eq!(
        broker.snapshot().publish_dropped,
        0,
        "max_queued_messages=0 must not drop anything"
    );
}
