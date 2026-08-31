//! Resource-limit tests: an offline durable session must not be able to grow
//! the broker's memory without bound.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::sleep;

use wispmq::acl::Acl;
use wispmq::broker::Broker;
use wispmq::codec::Properties;
use wispmq::config::Config;
use wispmq::framing::{read_packet, write_packet, ReadOutcome};
use wispmq::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use wispmq::storage::Storage;
use wispmq::types::{ProtocolVersion::V5, QoS, ReasonCode};

mod common;
use common::free_addr;

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
        let _ = wispmq::server::run(b).await;
    });
    broker
}

fn make_rate_limited_broker(addr: SocketAddr, max_per_ip: u32, window_secs: u32) -> Broker {
    let config = Config {
        listen_addr: addr,
        max_connections_per_ip: max_per_ip,
        connection_rate_window_secs: window_secs,
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

/// Plain CONNECT (clean session), asserting a successful CONNACK.
async fn connect_clean(addr: SocketAddr, client_id: &str) -> TcpStream {
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
            retain_handling: wispmq::packet::RetainHandling::SendAtSubscribe,
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
            payload: format!("msg-{i}").into_bytes().into(),
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
            payload: format!("m{i}").into_bytes().into(),
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

#[tokio::test]
async fn connection_rate_limit_rejects_beyond_the_cap() {
    const LIMIT: u32 = 2;
    let addr = free_addr();
    let broker = make_rate_limited_broker(addr, LIMIT, 60);
    sleep(Duration::from_millis(100)).await;

    // The first LIMIT connections from this source IP must succeed normally.
    let _a = connect_clean(addr, "rl-a").await;
    let _b = connect_clean(addr, "rl-b").await;

    // The next one is rejected: the server closes the socket immediately,
    // before TLS/framing or any packet is read — so the client sees a clean
    // EOF without ever getting a CONNACK.
    let mut rejected = TcpStream::connect(addr).await.unwrap();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        read_packet(&mut rejected, 1 << 20, V5),
    )
    .await
    .expect("rejected connection should close promptly, not hang")
    .unwrap();
    assert!(
        matches!(outcome, ReadOutcome::Eof),
        "rate-limited connection must be closed before any packet is sent"
    );

    assert_eq!(broker.snapshot().connections_rate_limited, 1);
}

#[tokio::test]
async fn connection_rate_limit_zero_means_unlimited() {
    // The default: unlimited connections per source IP.
    let addr = free_addr();
    let broker = make_rate_limited_broker(addr, 0, 60);
    sleep(Duration::from_millis(100)).await;

    for i in 0..10 {
        let _c = connect_clean(addr, &format!("rl-unlimited-{i}")).await;
    }

    assert_eq!(
        broker.snapshot().connections_rate_limited,
        0,
        "max_connections_per_ip=0 must not reject anything"
    );
}
