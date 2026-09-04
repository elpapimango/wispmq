//! End-to-end load/throughput benchmarks, kept because TODO item 9 asks for a
//! measurement of the broker under real connection load — distinct from
//! `tests/bench_routing.rs`, which drives `Broker::inject_publish` directly
//! and so measures matching/delivery cost in isolation, not socket/framing/
//! task overhead. These go over real TCP loopback sockets using the crate's
//! own codec, the same way `tests/interop.rs`/`tests/limits.rs` do.
//!
//! `#[ignore]`d — they are measurements, not assertions about correctness.
//! Run them deliberately, with enough worker threads that the test harness
//! itself isn't the bottleneck:
//!
//! ```text
//! cargo test --release --test bench_load -- --ignored --nocapture --test-threads=1
//! ```

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

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

fn start_broker(addr: SocketAddr) -> Broker {
    let config = Config {
        listen_addr: Some(addr),
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

/// The p-th percentile (0.0..=1.0) of an already-sorted slice.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn report_percentiles(label: &str, mut samples: Vec<u64>, unit: &str) {
    samples.sort_unstable();
    println!(
        "  {label}: n={} min={}{unit} p50={}{unit} p99={}{unit} max={}{unit}",
        samples.len(),
        samples.first().copied().unwrap_or(0),
        percentile(&samples, 0.50),
        percentile(&samples, 0.99),
        samples.last().copied().unwrap_or(0),
    );
}

/// How CONNECT-to-CONNACK latency holds up as the number of concurrent
/// connections grows. Each connection is opened concurrently and kept open
/// until the whole batch has connected, so the broker genuinely has that many
/// simultaneous connections at once rather than measuring them serially.
#[test]
#[ignore = "benchmark, not a correctness test"]
fn connection_scale() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        println!("\n-- CONNECT-to-CONNACK latency vs. concurrent connections --");
        for n in [10usize, 100, 500, 1000] {
            let addr = free_addr();
            let _broker = start_broker(addr);
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut tasks = Vec::with_capacity(n);
            for i in 0..n {
                tasks.push(tokio::spawn(async move {
                    let start = Instant::now();
                    let mut s = TcpStream::connect(addr).await.unwrap();
                    write_packet(&mut s, &connect_packet(&format!("scale-{i}")), V5)
                        .await
                        .unwrap();
                    match read_packet(&mut s, 1 << 20, V5).await.unwrap() {
                        ReadOutcome::Packet(Packet::Connack(a), _) => {
                            assert_eq!(a.reason_code, ReasonCode::Success)
                        }
                        _ => panic!("expected CONNACK"),
                    }
                    (start.elapsed().as_micros() as u64, s)
                }));
            }

            let mut latencies = Vec::with_capacity(n);
            let mut streams = Vec::with_capacity(n); // held open until the batch is done
            for t in tasks {
                let (us, s) = t.await.unwrap();
                latencies.push(us);
                streams.push(s);
            }
            report_percentiles(&format!("{n:>5} concurrent connections"), latencies, "us");
            drop(streams);
        }
    });
}

/// Sustained publish throughput and end-to-end delivery latency, fanning one
/// publisher out to many subscribers on the same topic. QoS 0 throughout, so
/// nothing here waits on acks — this isolates routing + socket throughput,
/// not the QoS 1/2 ack round trip (that's a different, narrower question).
///
/// Latency is measured by stamping each payload with nanoseconds elapsed
/// since a shared `Instant` (valid because publisher and subscribers run in
/// the same process) and comparing against the receive time.
#[test]
#[ignore = "benchmark, not a correctness test"]
fn fanout_throughput() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        println!("\n-- fan-out publish throughput and delivery latency --");
        for (subscribers, messages) in [(1usize, 20_000usize), (10, 5_000), (100, 1_000)] {
            let addr = free_addr();
            let _broker = start_broker(addr);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let epoch = Instant::now();

            // Subscribers first, so none of the publisher's messages arrive
            // before every subscriber is listening.
            let mut sub_tasks = Vec::with_capacity(subscribers);
            for i in 0..subscribers {
                sub_tasks.push(tokio::spawn(async move {
                    let mut s = TcpStream::connect(addr).await.unwrap();
                    write_packet(&mut s, &connect_packet(&format!("fanout-sub-{i}")), V5)
                        .await
                        .unwrap();
                    read_packet(&mut s, 1 << 20, V5).await.unwrap();

                    let sub = Packet::Subscribe(Subscribe {
                        packet_id: 1,
                        properties: Properties::new(),
                        filters: vec![TopicFilter {
                            filter: "load/topic".into(),
                            qos: QoS::AtMostOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: wispmq::packet::RetainHandling::SendAtSubscribe,
                        }],
                    });
                    write_packet(&mut s, &sub, V5).await.unwrap();
                    read_packet(&mut s, 1 << 20, V5).await.unwrap();

                    // Read until the stream goes quiet for a while, rather than
                    // expecting exactly `messages` packets: QoS 0 deliberately
                    // drops on a full outbound channel (`OUTBOUND_CHANNEL_
                    // CAPACITY`) rather than blocking, so a subscriber that
                    // can't keep up with a fire-hosed publisher legitimately
                    // receives fewer than `messages` — that gap is itself part
                    // of what this benchmark is measuring.
                    let mut latencies = Vec::with_capacity(messages);
                    loop {
                        match tokio::time::timeout(
                            Duration::from_millis(500),
                            read_packet(&mut s, 1 << 20, V5),
                        )
                        .await
                        {
                            Ok(Ok(ReadOutcome::Packet(Packet::Publish(p), _))) => {
                                let sent_ns =
                                    u64::from_be_bytes(p.payload[..8].try_into().unwrap());
                                let recv_ns = epoch.elapsed().as_nanos() as u64;
                                latencies.push(recv_ns.saturating_sub(sent_ns));
                            }
                            Ok(Ok(_)) => panic!("expected PUBLISH"),
                            Ok(Err(e)) => panic!("read error: {e}"),
                            Err(_) => break, // idle: no more messages coming
                        }
                    }
                    latencies
                }));
            }
            tokio::time::sleep(Duration::from_millis(100)).await; // let SUBACKs land

            let mut pubr = TcpStream::connect(addr).await.unwrap();
            write_packet(&mut pubr, &connect_packet("fanout-pub"), V5)
                .await
                .unwrap();
            read_packet(&mut pubr, 1 << 20, V5).await.unwrap();

            let send_start = Instant::now();
            for _ in 0..messages {
                let mut payload = epoch.elapsed().as_nanos().to_be_bytes()[8..16].to_vec();
                payload.resize(64, 0xab);
                let p = Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: "load/topic".into(),
                    packet_id: None,
                    properties: Properties::new(),
                    payload: payload.into(),
                });
                write_packet(&mut pubr, &p, V5).await.unwrap();
            }
            let send_elapsed = send_start.elapsed();
            let rate = messages as f64 / send_elapsed.as_secs_f64();

            let mut all_latencies = Vec::new();
            let mut total_received = 0usize;
            for t in sub_tasks {
                let latencies = tokio::time::timeout(Duration::from_secs(30), t)
                    .await
                    .expect("subscriber timed out waiting for its messages")
                    .unwrap();
                total_received += latencies.len();
                all_latencies.extend(latencies);
            }
            let expected = subscribers * messages;

            println!(
                "  {subscribers:>3} subscribers x {messages:>5} msgs: publish rate {rate:.0} msg/s, \
                 delivered {total_received}/{expected} ({:.1}%, rest dropped on a full outbound channel)",
                100.0 * total_received as f64 / expected as f64,
            );
            report_percentiles("    end-to-end delivery latency", all_latencies, "ns");
        }
    });
}
