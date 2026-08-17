//! `$SYS/broker/...` status topics and their Prometheus counterparts.
//!
//! The central property is that both surfaces render the same `Snapshot`, so a
//! value read over MQTT and the same value scraped by Prometheus must agree.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::codec::Properties;
use pulsemq::config::Config;
use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
use pulsemq::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use pulsemq::storage::Storage;
use pulsemq::types::{PacketType, ProtocolVersion::V5, QoS, ReasonCode};

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

fn make_broker(addr: SocketAddr, sys_interval: u32) -> Broker {
    let config = Config {
        listen_addr: addr,
        sys_interval,
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
            retain_handling: pulsemq::packet::RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(s, &sub, V5).await.unwrap();
    match read_packet(s, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Suback(_), _) => {}
        _ => panic!("expected SUBACK"),
    }
}

/// Collect PUBLISHes for `window`, returning topic -> payload.
async fn collect(s: &mut TcpStream, window: Duration) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        match timeout(remaining, read_packet(s, 1 << 20, V5)).await {
            Ok(Ok(ReadOutcome::Packet(Packet::Publish(p), _))) => {
                out.insert(p.topic, String::from_utf8_lossy(&p.payload).to_string());
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => break,
        }
    }
    out
}

#[tokio::test]
async fn sys_topics_are_published_and_retained() {
    let addr = free_addr();
    let broker = make_broker(addr, 1);
    tokio::spawn(pulsemq::sysinfo::run(broker.clone()));
    sleep(Duration::from_millis(200)).await;

    let mut c = connect(addr, "sys-reader").await;
    subscribe(&mut c, "$SYS/#").await;

    // Retained delivery means subscribing alone hands over the last published
    // values immediately.
    let retained_now = collect(&mut c, Duration::from_millis(300)).await;
    assert!(
        retained_now.contains_key("$SYS/broker/version"),
        "retained $SYS values should arrive on subscribe, got {:?}",
        retained_now.keys().collect::<Vec<_>>()
    );

    // Those first values predate this connection (they were published before it
    // existed), so collect across the next tick as well to see them refresh.
    let topics = collect(&mut c, Duration::from_millis(1500)).await;

    assert!(
        topics.contains_key("$SYS/broker/version"),
        "expected $SYS/broker/version, got {:?}",
        topics.keys().collect::<Vec<_>>()
    );
    assert!(topics["$SYS/broker/version"].starts_with("PulseMQ "));
    assert!(topics.contains_key("$SYS/broker/clients/connected"));
    assert!(topics.contains_key("$SYS/broker/messages/received"));
    assert!(topics.contains_key("$SYS/broker/mqtt/publish/received"));
    assert!(topics.contains_key("$SYS/broker/uptime"));
    assert!(topics["$SYS/broker/uptime"].ends_with(" seconds"));

    // Our own connection is counted.
    let connected: u64 = topics["$SYS/broker/clients/connected"].parse().unwrap();
    assert!(connected >= 1, "expected at least our own client connected");
}

#[tokio::test]
async fn sys_interval_zero_disables_publishing() {
    let addr = free_addr();
    let broker = make_broker(addr, 0);
    tokio::spawn(pulsemq::sysinfo::run(broker.clone()));
    sleep(Duration::from_millis(300)).await;

    let mut c = connect(addr, "sys-off").await;
    subscribe(&mut c, "$SYS/#").await;
    let topics = collect(&mut c, Duration::from_millis(400)).await;
    assert!(
        topics.is_empty(),
        "sys_interval=0 must publish nothing, got {:?}",
        topics.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn wildcard_subscribers_do_not_receive_sys_topics() {
    // §4.7.2: '#' must not match a topic starting with '$'. A plain '#'
    // subscriber must not be flooded with broker statistics.
    let addr = free_addr();
    let broker = make_broker(addr, 1);
    tokio::spawn(pulsemq::sysinfo::run(broker.clone()));
    sleep(Duration::from_millis(200)).await;

    let mut c = connect(addr, "hash-sub").await;
    subscribe(&mut c, "#").await;
    let topics = collect(&mut c, Duration::from_millis(500)).await;
    assert!(
        topics.keys().all(|t| !t.starts_with("$SYS")),
        "'#' must not match $SYS topics, got {:?}",
        topics.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn clients_cannot_publish_to_sys() {
    let addr = free_addr();
    let _broker = make_broker(addr, 0);
    sleep(Duration::from_millis(150)).await;

    let mut c = connect(addr, "forger").await;
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "$SYS/broker/clients/connected".into(),
        packet_id: None,
        properties: Properties::new(),
        payload: b"99999".to_vec(),
    });
    write_packet(&mut c, &p, V5).await.unwrap();

    // The broker must reject this rather than accept forged statistics: it
    // sends DISCONNECT (0x90 Topic Name invalid) and closes.
    match timeout(Duration::from_secs(2), read_packet(&mut c, 1 << 20, V5)).await {
        Ok(Ok(ReadOutcome::Packet(Packet::Disconnect(d), _))) => {
            assert_eq!(d.reason_code, ReasonCode::TopicNameInvalid);
        }
        Ok(Ok(ReadOutcome::Eof)) => {} // Closed without DISCONNECT is also a refusal.
        Ok(Ok(ReadOutcome::Packet(p, _))) => {
            panic!("expected refusal of a $SYS publish, got {}", p.name())
        }
        Ok(Err(_)) => {} // A transport error after the refusal is fine.
        Err(_) => panic!("broker neither refused nor closed after a $SYS publish"),
    }
}

#[tokio::test]
async fn sys_topics_do_not_inflate_the_retained_gauge() {
    // $SYS values are retained in the same map as user data so that late
    // subscribers get them, but they must not be counted as user retained
    // messages -- otherwise enabling $SYS silently adds ~50 to an existing
    // gauge and breaks whatever was alerting on it.
    let addr = free_addr();
    let broker = make_broker(addr, 1);
    tokio::spawn(pulsemq::sysinfo::run(broker.clone()));
    sleep(Duration::from_millis(400)).await;

    // No user retained messages yet, despite many $SYS topics being live.
    let snap = broker.snapshot();
    assert!(
        !snap.to_sys_topics().is_empty(),
        "expected $SYS topics to exist for this test to mean anything"
    );
    assert_eq!(
        snap.retained_messages, 0,
        "$SYS topics must not count as user retained messages"
    );
    assert_eq!(snap.retained_bytes, 0);
    assert!(
        broker.retained().is_empty(),
        "$SYS topics must not appear in the retained listing"
    );

    // One real retained publish shows up as exactly one.
    let mut c = connect(addr, "retainer").await;
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "user/keep".into(),
        packet_id: None,
        properties: Properties::new(),
        payload: b"hello".to_vec(),
    });
    write_packet(&mut c, &p, V5).await.unwrap();
    sleep(Duration::from_millis(250)).await;

    let snap = broker.snapshot();
    assert_eq!(
        snap.retained_messages, 1,
        "expected exactly one user retained message"
    );
    assert_eq!(snap.retained_bytes, 5);
    assert_eq!(broker.retained().len(), 1);
}

#[tokio::test]
async fn sys_and_prometheus_report_the_same_values() {
    let addr = free_addr();
    let broker = make_broker(addr, 0);
    sleep(Duration::from_millis(150)).await;

    // Generate some traffic so the counters are non-zero.
    let mut c = connect(addr, "traffic").await;
    subscribe(&mut c, "load/#").await;
    for i in 0..5 {
        let p = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "load/x".into(),
            packet_id: None,
            properties: Properties::new(),
            payload: format!("payload-{i}").into_bytes(),
        });
        write_packet(&mut c, &p, V5).await.unwrap();
    }
    sleep(Duration::from_millis(250)).await;

    // One snapshot, two renderings: they must not disagree.
    let snap = broker.snapshot();
    let prom = snap.to_prometheus();
    let sys: HashMap<String, String> = snap.to_sys_topics().into_iter().collect();

    let prom_value = |name: &str| -> u64 {
        for line in prom.lines() {
            if let Some(rest) = line.strip_prefix(name) {
                if let Some(v) = rest.strip_prefix(' ') {
                    return v.trim().parse().unwrap_or_else(|_| {
                        panic!("could not parse {name} from {line:?}");
                    });
                }
            }
        }
        panic!("{name} missing from /metrics output");
    };

    for (prom_name, sys_topic) in [
        ("mqtt_bytes_received_total", "$SYS/broker/bytes/received"),
        (
            "mqtt_messages_received_total",
            "$SYS/broker/messages/received",
        ),
        (
            "mqtt_publish_received_total",
            "$SYS/broker/publish/messages/received",
        ),
        (
            "mqtt_publish_bytes_received_total",
            "$SYS/broker/publish/bytes/received",
        ),
        ("mqtt_clients_connected", "$SYS/broker/clients/connected"),
        (
            "mqtt_subscriptions_total",
            "$SYS/broker/subscriptions/count",
        ),
        (
            "mqtt_socket_connections_total",
            "$SYS/broker/connections/socket/count",
        ),
        (
            "mqtt_publish_received_total",
            "$SYS/broker/mqtt/publish/received",
        ),
    ] {
        let from_sys: u64 = sys[sys_topic]
            .parse()
            .unwrap_or_else(|_| panic!("{sys_topic} was not numeric: {:?}", sys[sys_topic]));
        assert_eq!(
            prom_value(prom_name),
            from_sys,
            "{prom_name} and {sys_topic} disagree"
        );
    }

    // Sanity: the traffic we generated actually registered.
    assert!(snap.publish_received >= 5, "publishes were not counted");
    assert!(
        snap.publish_bytes_received >= 5 * "payload-0".len() as u64,
        "publish payload bytes were not counted"
    );
    assert_eq!(
        snap.packet_received[PacketType::Publish as usize],
        snap.publish_received,
        "per-packet PUBLISH counter should match publish_received"
    );
    assert!(
        snap.packet_received[PacketType::Connect as usize] >= 1,
        "CONNECT should be counted per-packet"
    );
    assert!(snap.socket_connections >= 1, "sockets were not counted");
    assert!(snap.clients_maximum >= 1, "clients_maximum was not tracked");
}
