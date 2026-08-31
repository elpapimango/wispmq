//! Shared subscriptions (4.8.2): `$share/{group}/{filter}`.
//!
//! A message matching a shared subscription goes to exactly **one** member of
//! each group, chosen round-robin, while every distinct group and every ordinary
//! subscription still gets its own copy.
//!
//! These paths had no coverage before, and `route()` treats them as its one
//! genuinely two-phase case (the choice depends on the whole group), so they are
//! the easiest thing to break while refactoring routing.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use wispmq::acl::Acl;
use wispmq::broker::Broker;
use wispmq::codec::Properties;
use wispmq::config::Config;
use wispmq::framing::{read_packet, write_packet, ReadOutcome};
use wispmq::packet::{Connect, Packet, Publish, RetainHandling, Subscribe, TopicFilter};
use wispmq::storage::Storage;
use wispmq::types::{ProtocolVersion::V5, QoS, ReasonCode};

mod common;
use common::free_addr;

async fn start_broker() -> SocketAddr {
    let addr = free_addr();
    let config = Config {
        listen_addr: addr,
        sys_interval: 0, // keep $SYS traffic out of the way
        ..Config::default()
    };
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    );
    tokio::spawn(async move {
        let _ = wispmq::server::run(broker).await;
    });
    sleep(Duration::from_millis(100)).await;
    addr
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

async fn subscribe(s: &mut TcpStream, filter: &str, no_local: bool) {
    let sub = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: filter.into(),
            qos: QoS::AtMostOnce,
            no_local,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(s, &sub, V5).await.unwrap();
    match read_packet(s, 1 << 20, V5).await.unwrap() {
        ReadOutcome::Packet(Packet::Suback(a), _) => {
            // Granted QoS 0 is reason code 0x00, i.e. Success.
            assert!(
                a.reason_codes.iter().all(|rc| *rc == ReasonCode::Success),
                "subscribe to {filter} was refused: {:?}",
                a.reason_codes
            );
        }
        _ => panic!("expected SUBACK for {filter}"),
    }
}

async fn publish(s: &mut TcpStream, topic: &str, payload: &[u8]) {
    let p = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.into(),
        packet_id: None,
        properties: Properties::new(),
        payload: payload.into(),
    });
    write_packet(s, &p, V5).await.unwrap();
}

/// Drain whatever PUBLISHes have arrived within a short grace period.
async fn drain(s: &mut TcpStream) -> Vec<String> {
    let mut got = Vec::new();
    loop {
        match timeout(Duration::from_millis(250), read_packet(s, 1 << 20, V5)).await {
            Ok(Ok(ReadOutcome::Packet(Packet::Publish(p), _))) => {
                got.push(String::from_utf8_lossy(&p.payload).into_owned());
            }
            Ok(Ok(_)) => continue, // ignore anything else
            _ => break,            // timeout or closed: nothing more pending
        }
    }
    got
}

/// The core guarantee: one member per group per message, spread round-robin.
#[tokio::test]
async fn shared_group_delivers_each_message_to_exactly_one_member() {
    let addr = start_broker().await;

    let mut a = connect(addr, "member-a").await;
    let mut b = connect(addr, "member-b").await;
    subscribe(&mut a, "$share/g1/sensors/temp", false).await;
    subscribe(&mut b, "$share/g1/sensors/temp", false).await;

    let mut pubr = connect(addr, "publisher").await;
    const N: usize = 10;
    for i in 0..N {
        publish(&mut pubr, "sensors/temp", format!("m{i}").as_bytes()).await;
    }
    sleep(Duration::from_millis(200)).await;

    let got_a = drain(&mut a).await;
    let got_b = drain(&mut b).await;

    // Every message delivered exactly once across the group — no duplicates,
    // none lost.
    let mut all: Vec<String> = got_a.iter().chain(got_b.iter()).cloned().collect();
    all.sort();
    let mut expected: Vec<String> = (0..N).map(|i| format!("m{i}")).collect();
    expected.sort();
    assert_eq!(all, expected, "a={got_a:?} b={got_b:?}");

    // And the load was actually shared rather than all landing on one member.
    assert!(
        !got_a.is_empty() && !got_b.is_empty(),
        "round-robin should reach both members: a={got_a:?} b={got_b:?}"
    );
}

/// Distinct groups are independent, and an ordinary subscription is unaffected
/// by a shared one on the same filter.
#[tokio::test]
async fn each_group_and_ordinary_subscribers_get_their_own_copy() {
    let addr = start_broker().await;

    let mut g1 = connect(addr, "g1-only").await;
    let mut g2 = connect(addr, "g2-only").await;
    let mut plain = connect(addr, "plain").await;
    subscribe(&mut g1, "$share/groupone/sensors/temp", false).await;
    subscribe(&mut g2, "$share/grouptwo/sensors/temp", false).await;
    subscribe(&mut plain, "sensors/temp", false).await;

    let mut pubr = connect(addr, "publisher").await;
    publish(&mut pubr, "sensors/temp", b"only-once").await;
    sleep(Duration::from_millis(200)).await;

    // Sole member of its group, so each gets it; the ordinary subscriber does
    // too. Three copies of one publication, one per subscription "channel".
    assert_eq!(drain(&mut g1).await, vec!["only-once".to_string()]);
    assert_eq!(drain(&mut g2).await, vec!["only-once".to_string()]);
    assert_eq!(drain(&mut plain).await, vec!["only-once".to_string()]);
}

/// No Local (3.8.3.1) applies to shared subscriptions too: the publisher must
/// not receive its own message through its own shared subscription. With the
/// publisher excluded, the other member must still get it.
#[tokio::test]
async fn no_local_excludes_the_publisher_from_its_shared_group() {
    let addr = start_broker().await;

    let mut selfsub = connect(addr, "self-publisher").await;
    let mut other = connect(addr, "other-member").await;
    subscribe(&mut selfsub, "$share/g1/loop/test", true).await;
    subscribe(&mut other, "$share/g1/loop/test", false).await;

    publish(&mut selfsub, "loop/test", b"mine").await;
    sleep(Duration::from_millis(200)).await;

    assert!(
        drain(&mut selfsub).await.is_empty(),
        "no_local must suppress the echo to the publisher"
    );
    assert_eq!(
        drain(&mut other).await,
        vec!["mine".to_string()],
        "the message should still reach the other group member"
    );
}
