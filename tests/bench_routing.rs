//! Routing-throughput benchmarks, kept because TODO item 5C asks for a
//! measurement before anyone reaches for a topic trie or reworks fan-out.
//!
//! These are `#[ignore]`d — they are measurements, not assertions about
//! correctness. Run them deliberately:
//!
//! ```text
//! cargo test --release --test bench_routing -- --ignored --nocapture
//! ```
//!
//! They drive `Broker::inject_publish`, which is the same `route()` the network
//! path uses, but deliver into in-process channels instead of sockets, so what
//! is timed is matching plus fan-out rather than TCP.

use std::time::Instant;

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::config::Config;
use pulsemq::message::Message;
use pulsemq::storage::Storage;
use pulsemq::types::QoS;

/// A broker with no listener and no database — only the routing core.
fn bare_broker() -> Broker {
    Broker::new(
        Config::default(),
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    )
}

fn message(topic: &str, payload_len: usize) -> Message {
    Message {
        topic: topic.to_string(),
        payload: vec![0xab; payload_len].into(),
        qos: QoS::AtMostOnce,
        retain: false,
        payload_format_indicator: None,
        content_type: None,
        response_topic: None,
        correlation_data: None,
        user_properties: Vec::new(),
        expires_at: None,
    }
}

/// Register `sessions` subscribers, each holding `subs_each` subscriptions, and
/// return the receivers so the delivery channels stay open (dropping them would
/// make every send fail and measure the wrong thing).
#[allow(clippy::type_complexity)]
fn populate(
    broker: &Broker,
    sessions: usize,
    subs_each: usize,
    filter: impl Fn(usize, usize) -> String,
) -> Vec<tokio::sync::mpsc::Receiver<pulsemq::broker::Outgoing>> {
    let mut receivers = Vec::with_capacity(sessions);
    for s in 0..sessions {
        // Capacity comfortably exceeds every `iters` used in this file (max
        // 20_000) so a full channel never kicks in and skews what's being
        // timed; the real per-session bound lives in
        // `broker::OUTBOUND_CHANNEL_CAPACITY`.
        let (tx, rx) = tokio::sync::mpsc::channel(100_000);
        broker.register_bridge(format!("sub-{s}"), tx);
        for f in 0..subs_each {
            broker.bridge_add_subscription(&format!("sub-{s}"), &filter(s, f), false);
        }
        receivers.push(rx);
    }
    receivers
}

/// Time `iters` publishes and report per-publish cost.
fn time_publishes(broker: &Broker, topic: &str, payload_len: usize, iters: usize, label: &str) {
    let msg = message(topic, payload_len);
    let start = Instant::now();
    for _ in 0..iters {
        broker.inject_publish("publisher", msg.clone());
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / iters as f64;
    println!("{label}: {iters} publishes in {elapsed:?} => {per:.0} ns/publish");
}

/// How routing scales with the number of subscriptions that must be tested.
/// This is the number that decides whether a topic trie is worth building: the
/// current implementation is linear over every subscription of every session.
#[test]
#[ignore = "benchmark, not a correctness test"]
fn routing_scales_with_subscription_count() {
    println!("\n-- fan-out to matching subscribers (every subscriber matches) --");
    for sessions in [1usize, 10, 100, 1000] {
        let broker = bare_broker();
        let _rx = populate(&broker, sessions, 1, |_, _| "sensors/+/temp".to_string());
        time_publishes(
            &broker,
            "sensors/kitchen/temp",
            64,
            20_000,
            &format!("  {sessions:>5} matching subscribers"),
        );
    }

    println!("\n-- non-matching subscriptions (pure matching cost, no delivery) --");
    for subs in [10usize, 100, 1000, 10_000] {
        let broker = bare_broker();
        // One session holding many subscriptions, none of which match, so this
        // isolates match cost from delivery cost.
        let _rx = populate(&broker, 1, subs, |_, f| format!("other/{f}/thing"));
        time_publishes(
            &broker,
            "sensors/kitchen/temp",
            64,
            20_000,
            &format!("  {subs:>5} non-matching subscriptions"),
        );
    }
}

/// How fan-out scales with payload size. `Message.payload` is a `Vec<u8>` that
/// is cloned once per recipient (twice for QoS>0, which also buffers the
/// PUBLISH for retransmission), so this is the cost an `Arc<[u8]>` payload
/// would remove.
#[test]
#[ignore = "benchmark, not a correctness test"]
fn fanout_scales_with_payload_size() {
    println!("\n-- 100 matching subscribers, varying payload --");
    for len in [8usize, 256, 4096, 65536] {
        let broker = bare_broker();
        let _rx = populate(&broker, 100, 1, |_, _| "sensors/#".to_string());
        time_publishes(
            &broker,
            "sensors/kitchen/temp",
            len,
            2_000,
            &format!("  payload {len:>6} B"),
        );
    }
}
