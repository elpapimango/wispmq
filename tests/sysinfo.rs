//! `$SYS/broker/...` status topics and their Prometheus counterparts.
//!
//! The central property is that both surfaces render the same `Snapshot`, so a
//! value read over MQTT and the same value scraped by Prometheus must agree.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use wispmq::acl::Acl;
use wispmq::broker::Broker;
use wispmq::codec::Properties;
use wispmq::config::Config;
use wispmq::framing::{read_packet, write_packet, ReadOutcome};
use wispmq::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use wispmq::storage::Storage;
use wispmq::types::{PacketType, ProtocolVersion::V5, QoS, ReasonCode};

mod common;
use common::free_addr;

fn make_broker(addr: SocketAddr, sys_interval: u32) -> Broker {
    let config = Config {
        listen_addr: Some(addr),
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
        let _ = wispmq::server::run(b).await;
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
            retain_handling: wispmq::packet::RetainHandling::SendAtSubscribe,
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
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));
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
    assert!(topics["$SYS/broker/version"].starts_with("WispMQ "));
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
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));
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
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));
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
        payload: b"99999"[..].into(),
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
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));
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
        payload: b"hello"[..].into(),
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
async fn ha_discovery_topics_do_not_inflate_the_retained_gauge() {
    // Same property as $SYS: enabling ha_discovery publishes ~2x
    // series().len() retained messages (discovery config + state, per
    // statistic) of the broker's own making, and none of them may count as
    // user retained data.
    let addr = free_addr();
    let config = Config {
        listen_addr: Some(addr),
        sys_interval: 1,
        ha_discovery: true,
        service_name: "edge-1".to_string(),
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
    tokio::spawn(wispmq::sysinfo::run(broker.clone()));
    sleep(Duration::from_millis(400)).await;

    let snap = broker.snapshot();
    assert!(
        !snap.to_ha_discovery("homeassistant", "edge-1").is_empty(),
        "expected HA discovery topics to exist for this test to mean anything"
    );
    assert_eq!(
        snap.retained_messages, 0,
        "HA discovery/state topics must not count as user retained messages"
    );
    assert_eq!(snap.retained_bytes, 0);
    assert!(
        broker.retained().is_empty(),
        "HA discovery/state topics must not appear in the retained listing"
    );

    // A real retained publish under an unrelated topic still shows up.
    let mut c = connect(addr, "retainer").await;
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "user/keep".into(),
        packet_id: None,
        properties: Properties::new(),
        payload: b"hello"[..].into(),
    });
    write_packet(&mut c, &p, V5).await.unwrap();
    sleep(Duration::from_millis(250)).await;

    let snap = broker.snapshot();
    assert_eq!(snap.retained_messages, 1);
    assert_eq!(broker.retained().len(), 1);
}

#[tokio::test]
async fn expired_retained_messages_are_excluded_and_purged() {
    // A retained message with a short `message_expiry_interval` must stop
    // counting toward the gauges once it expires, and the entry must not
    // linger in memory forever just because nothing republished that topic.
    let addr = free_addr();
    let broker = make_broker(addr, 0);
    sleep(Duration::from_millis(150)).await;

    let mut c = connect(addr, "expiring-retainer").await;
    let mut props = Properties::new();
    // 3s, not 1s: connect()+write_packet()+broker processing must land the
    // "before expiry" check below comfortably inside this window even on a
    // loaded CI runner. A 1s interval against ~350ms of intentional sleep
    // left almost no slack, and flaked for real under GitHub Actions'
    // shared runners (message already expired by the time the assertion
    // ran, wrongly reading as a broker bug).
    props.message_expiry_interval = Some(3);
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "user/soon-gone".into(),
        packet_id: None,
        properties: props,
        payload: b"bye"[..].into(),
    });
    write_packet(&mut c, &p, V5).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // Before expiry: counted and listed normally.
    let snap = broker.snapshot();
    assert_eq!(snap.retained_messages, 1);
    assert_eq!(snap.retained_bytes, 3);
    assert_eq!(broker.retained().len(), 1);

    // After expiry: excluded from the gauges, and purged from the map (not
    // just hidden) -- a later `retained()` call reflects the same removal
    // whether or not `snapshot()` was called in between.
    sleep(Duration::from_millis(3200)).await;
    let snap = broker.snapshot();
    assert_eq!(
        snap.retained_messages, 0,
        "expired retained message must not be counted"
    );
    assert_eq!(snap.retained_bytes, 0);
    assert!(
        broker.retained().is_empty(),
        "expired retained message must be purged, not just hidden"
    );
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
            payload: format!("payload-{i}").into_bytes().into(),
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
        // The per-control-packet series, distinct from the aggregate
        // publish counter above. These two pairs used to share the name
        // `mqtt_publish_received_total`, and this table asserted the same
        // Prometheus name against both $SYS topics without noticing.
        (
            "mqtt_packet_publish_received_total",
            "$SYS/broker/mqtt/publish/received",
        ),
        (
            "mqtt_packet_connect_received_total",
            "$SYS/broker/mqtt/connect/received",
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

/// The Prometheus exposition must never repeat a metric name.
///
/// A name appearing twice with its own HELP/TYPE block is invalid exposition:
/// the scrape either errors or silently keeps one series and drops the other, so
/// a dashboard reads a plausible-looking number that is not what it claims to
/// be. This actually shipped — the per-control-packet counters were named
/// `mqtt_{packet}_{received,sent}_total`, which for PUBLISH collided with the
/// aggregate `mqtt_publish_received_total` / `mqtt_publish_sent_total`. Nothing
/// caught it: `sys_and_prometheus_report_the_same_values` compares *values*, and
/// the two colliding series happened to hold the same number.
///
/// Checks structure only, so it needs no traffic — just a broker to snapshot.
#[test]
fn no_duplicate_metric_names() {
    let broker = Broker::new(
        Config::default(),
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    );
    let prom = broker.snapshot().to_prometheus();

    /// Metric name from a `# HELP <name> ...` line.
    fn help_name(line: &str) -> Option<String> {
        line.strip_prefix("# HELP ")
            .and_then(|r| r.split_whitespace().next())
            .map(str::to_string)
    }
    /// Metric name from a `<name> <value>` or `<name>{labels} <value>` line.
    fn sample_name(line: &str) -> Option<String> {
        if line.starts_with('#') || line.trim().is_empty() {
            return None;
        }
        line.split_whitespace()
            .next()
            .map(|n| n.split('{').next().unwrap_or(n).to_string())
    }

    let mut declared: Vec<String> = prom.lines().filter_map(help_name).collect();
    let mut emitted: Vec<String> = prom.lines().filter_map(sample_name).collect();
    assert!(declared.len() > 40, "suspiciously few metrics parsed");

    for (what, names) in [("HELP", &mut declared), ("sample", &mut emitted)] {
        names.sort();
        let dupes: Vec<&String> = names
            .windows(2)
            .filter(|w| w[0] == w[1])
            .map(|w| &w[0])
            .collect();
        assert!(
            dupes.is_empty(),
            "duplicate {what} lines in /metrics for: {dupes:?}"
        );
    }

    // Every declared metric must also emit a sample and vice versa, so a rename
    // cannot leave a HELP block orphaned.
    assert_eq!(declared, emitted, "HELP blocks and samples disagree");
}
