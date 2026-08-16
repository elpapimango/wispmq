//! End-to-end tests that drive the running broker over a real TCP socket
//! using the crate's own codec.

use std::time::Duration;

use tokio::net::TcpStream;

use mqtt_server::broker::Broker;
use mqtt_server::codec::Properties;
use mqtt_server::config::Config;
use mqtt_server::framing::{read_packet, write_packet, ReadOutcome};
use mqtt_server::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use mqtt_server::storage::Storage;
use mqtt_server::types::{QoS, ReasonCode};

/// Start a broker on an ephemeral loopback port and return its address.
async fn start_broker() -> String {
    // Bind first to grab a free port, then hand the address to the broker.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let config = Config {
        listen_addr: addr,
        ..Config::default()
    };
    let broker = Broker::new(
        config,
        Storage::null(),
        Default::default(),
        mqtt_server::acl::Acl::permit_all(),
        None,
    );
    tokio::spawn(async move {
        let _ = mqtt_server::server::run(broker).await;
    });
    // Give the listener a moment to bind.
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr.to_string()
}

fn connect_packet(client_id: &str) -> Packet {
    Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 0,
        properties: Properties::new(),
        client_id: client_id.into(),
        will: None,
        username: None,
        password: None,
    })
}

async fn connect(addr: &str, client_id: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    write_packet(&mut stream, &connect_packet(client_id))
        .await
        .unwrap();
    match read_packet(&mut stream, 1 << 20).await.unwrap() {
        ReadOutcome::Packet(Packet::Connack(c), _) => {
            assert_eq!(c.reason_code, ReasonCode::Success);
        }
        other => panic!("expected CONNACK, got something else: {:?}", other.is_eof()),
    }
    stream
}

// Small helper so the panic branch above can name the outcome.
trait OutcomeExt {
    fn is_eof(&self) -> bool;
}
impl OutcomeExt for ReadOutcome {
    fn is_eof(&self) -> bool {
        matches!(self, ReadOutcome::Eof)
    }
}

#[tokio::test]
async fn qos1_publish_is_routed_to_subscriber() {
    let addr = start_broker().await;

    // Subscriber connects and subscribes to a wildcard filter.
    let mut sub = connect(&addr, "sub").await;
    let subscribe = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: "sensors/#".into(),
            qos: QoS::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: mqtt_server::packet::RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(&mut sub, &subscribe).await.unwrap();
    match read_packet(&mut sub, 1 << 20).await.unwrap() {
        ReadOutcome::Packet(Packet::Suback(s), _) => {
            assert_eq!(s.reason_codes, vec![ReasonCode::GrantedQoS1]);
        }
        _ => panic!("expected SUBACK"),
    }

    // Publisher connects and publishes a QoS 1 message.
    let mut pubr = connect(&addr, "pub").await;
    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "sensors/temp".into(),
        packet_id: Some(10),
        properties: Properties::new(),
        payload: b"22.4C".to_vec(),
    });
    write_packet(&mut pubr, &publish).await.unwrap();
    // Publisher gets a PUBACK.
    match read_packet(&mut pubr, 1 << 20).await.unwrap() {
        ReadOutcome::Packet(Packet::Puback(a), _) => assert_eq!(a.packet_id, 10),
        _ => panic!("expected PUBACK"),
    }

    // Subscriber receives the forwarded PUBLISH.
    let received = tokio::time::timeout(Duration::from_secs(2), read_packet(&mut sub, 1 << 20))
        .await
        .expect("timed out waiting for delivery")
        .unwrap();
    match received {
        ReadOutcome::Packet(Packet::Publish(p), _) => {
            assert_eq!(p.topic, "sensors/temp");
            assert_eq!(p.payload, b"22.4C");
            assert_eq!(p.qos, QoS::AtLeastOnce);
        }
        _ => panic!("expected forwarded PUBLISH"),
    }
}

#[tokio::test]
async fn retained_message_delivered_on_subscribe() {
    let addr = start_broker().await;

    // Publish a retained message with no subscribers.
    let mut pubr = connect(&addr, "retpub").await;
    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: "state/lamp".into(),
        packet_id: None,
        properties: Properties::new(),
        payload: b"on".to_vec(),
    });
    write_packet(&mut pubr, &publish).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A later subscriber should immediately receive the retained message.
    let mut sub = connect(&addr, "retsub").await;
    let subscribe = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: "state/lamp".into(),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: mqtt_server::packet::RetainHandling::SendAtSubscribe,
        }],
    });
    write_packet(&mut sub, &subscribe).await.unwrap();

    // SUBACK, then the retained PUBLISH.
    let mut saw_publish = false;
    for _ in 0..2 {
        let outcome = tokio::time::timeout(Duration::from_secs(2), read_packet(&mut sub, 1 << 20))
            .await
            .expect("timeout")
            .unwrap();
        if let ReadOutcome::Packet(Packet::Publish(p), _) = outcome {
            assert_eq!(p.topic, "state/lamp");
            assert_eq!(p.payload, b"on");
            assert!(p.retain, "retained delivery must set the RETAIN flag");
            saw_publish = true;
        }
    }
    assert!(saw_publish, "did not receive retained message");
}
