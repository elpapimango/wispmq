//! OTLP telemetry export.
//!
//! The mapping from `Snapshot` to instruments is checkable in-process, but the
//! part that actually breaks in the field is the *wire*: the wrong URL, a
//! dropped header, a body that never leaves. So these tests stand up a fake
//! collector — a TCP listener that speaks just enough HTTP to accept an OTLP
//! POST — and assert on what the exporter really sent.
//!
//! Only built with `--features otel`; without it there is nothing to export.
#![cfg(feature = "otel")]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};

use pulsemq::acl::Acl;
use pulsemq::broker::Broker;
use pulsemq::config::Config;
use pulsemq::otel;
use pulsemq::storage::Storage;

/// One request the fake collector received.
#[derive(Debug, Clone)]
struct Received {
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Received {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A collector that accepts OTLP/HTTP posts and records them.
///
/// Deliberately hand-rolled rather than pulled from a crate: the point is to
/// observe exactly what went over the socket, and the exporter only needs a
/// 200 with an empty protobuf body (a zero-length message is a valid empty
/// `ExportMetricsServiceResponse`).
async fn fake_collector() -> (SocketAddr, Arc<Mutex<Vec<Received>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Arc<Mutex<Vec<Received>>> = Arc::new(Mutex::new(Vec::new()));

    let sink = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                // The exporter reuses connections, so serve requests in a loop
                // until the peer goes away.
                let mut buf = Vec::new();
                loop {
                    let mut chunk = [0u8; 8192];
                    let n = match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&chunk[..n]);

                    // Parse as many complete request head + body pairs as the
                    // buffer holds. Content-Length is always present on an OTLP
                    // post, so no chunked decoding is needed.
                    while let Some(head_end) = find_headers_end(&buf) {
                        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                        let (path, headers) = parse_head(&head);
                        let want: usize = headers
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                            .and_then(|(_, v)| v.parse().ok())
                            .unwrap_or(0);
                        if buf.len() < head_end + want {
                            break; // body still arriving
                        }
                        sink.lock().unwrap().push(Received {
                            path,
                            headers,
                            body: buf[head_end..head_end + want].to_vec(),
                        });
                        buf.drain(..head_end + want);
                        let _ = stream
                            .write_all(
                                b"HTTP/1.1 200 OK\r\n\
                                  Content-Type: application/x-protobuf\r\n\
                                  Content-Length: 0\r\n\r\n",
                            )
                            .await;
                    }
                }
            });
        }
    });
    (addr, seen)
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn parse_head(head: &str) -> (String, Vec<(String, String)>) {
    let mut lines = head.lines();
    let path = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or("")
        .to_string();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    (path, headers)
}

fn make_broker(config: Config) -> Broker {
    Broker::new(
        config,
        Storage::null(),
        Default::default(),
        Acl::permit_all(),
        None,
    )
}

/// Wait for the fake collector to see a request matching `pred`.
async fn wait_for(
    seen: &Arc<Mutex<Vec<Received>>>,
    pred: impl Fn(&Received) -> bool,
) -> Option<Received> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(r) = seen.lock().unwrap().iter().find(|r| pred(r)).cloned() {
                return r;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_are_exported_to_the_configured_endpoint() {
    let (addr, seen) = fake_collector().await;
    let config = Config {
        otlp_endpoint: Some(format!("http://{addr}")),
        otlp_interval: 1,
        otlp_logs: false, // logs install a subscriber layer; metrics stand alone
        otlp_headers: pulsemq::config::OtlpHeaders::from_pairs([(
            "DD-API-KEY".to_string(),
            "s3cret".to_string(),
        )]),
        ..Config::default()
    };
    let broker = make_broker(config.clone());
    let (_layer, mut tel) = otel::install(&config).unwrap();
    otel::install_metrics(&broker, &mut tel, &config).unwrap();

    let req = wait_for(&seen, |r| r.path == "/v1/metrics")
        .await
        .expect("no metric export reached the collector");

    // The configured value is a *base* URL; the exporter's own `with_endpoint`
    // appends nothing, so `otel::signal_url` must add the signal path. Getting
    // this wrong posts to `/` and every export 404s.
    assert_eq!(req.path, "/v1/metrics");
    assert_eq!(req.header("content-type"), Some("application/x-protobuf"));
    // A dropped header is an authentication failure at the far end that looks
    // like a broker problem.
    assert_eq!(req.header("dd-api-key"), Some("s3cret"));
    assert!(!req.body.is_empty(), "exported an empty payload");

    // Instrument names travel as plain UTF-8 inside the protobuf, so a
    // substring search over the raw body checks the real payload without
    // decoding it. This is what proves the export carries the same series
    // `/metrics` renders — and that the `_total` rule was applied correctly.
    let body = req.body.clone();
    let has = |needle: &str| find(&body, needle.as_bytes());

    assert!(has("mqtt_packets_received"), "no packet counter exported");
    assert!(has("mqtt_clients_connected"), "no client gauge exported");
    assert!(has("mqtt_packet_publish_sent"), "no per-packet counter");
    // Counters shed `_total`: a Collector's Prometheus exporter appends it, and
    // exporting it here would produce `..._total_total` at the far end.
    assert!(
        !has("mqtt_packets_received_total"),
        "counter kept its _total suffix"
    );
    // ...but these two are gauges that merely end in `_total`, and renaming
    // them would break every dashboard already reading them.
    assert!(has("mqtt_sessions_total"), "gauge lost its name");
    assert!(has("mqtt_subscriptions_total"), "gauge lost its name");
    // Resource attributes identify the sender.
    assert!(has("pulsemq"), "no service.name on the resource");
    assert!(has(pulsemq::config::VERSION), "no service.version");

    tel.shutdown();
}

/// Naive substring search over the raw export body.
fn find(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[tokio::test(flavor = "multi_thread")]
async fn logs_are_exported_to_the_configured_endpoint() {
    use tracing_subscriber::layer::SubscriberExt;

    let (addr, seen) = fake_collector().await;
    let config = Config {
        otlp_endpoint: Some(format!("http://{addr}")),
        otlp_metrics: false,
        service_name: "edge-under-test".to_string(),
        ..Config::default()
    };
    let (layer, tel) = otel::install(&config).unwrap();
    let layer = layer.expect("no log layer installed");

    // A *scoped* subscriber, not `init()`: the global one can only be set once
    // per process and every other test in this binary would inherit it.
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(client_id = "probe", "broker log line under test");
    });

    let req = wait_for(&seen, |r| r.path == "/v1/logs")
        .await
        .expect("no log export reached the collector");
    let body = req.body.clone();
    assert!(
        find(&body, b"broker log line under test"),
        "the log message did not make it into the export"
    );
    // Structured fields must survive, or the exported logs are not queryable.
    assert!(find(&body, b"client_id"), "structured field was dropped");
    assert!(find(&body, b"edge-under-test"), "no service.name");

    tel.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn export_is_off_unless_an_endpoint_is_configured() {
    // The default config must not talk to the network at all: an existing
    // deployment that upgrades and changes nothing keeps its behaviour.
    let config = Config::default();
    let broker = make_broker(config.clone());
    let (layer, mut tel) = otel::install(&config).unwrap();
    assert!(layer.is_none(), "installed a log layer with no endpoint");
    otel::install_metrics(&broker, &mut tel, &config).unwrap();
    tel.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_collector_does_not_stop_the_broker() {
    // The failure mode this rules out: export errors propagating into startup,
    // or a wedged exporter stalling the runtime. Nothing is listening on this
    // port, so every export attempt fails.
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        drop(l);
        a
    };
    let config = Config {
        otlp_endpoint: Some(format!("http://{addr}")),
        otlp_interval: 1,
        otlp_logs: false,
        ..Config::default()
    };
    let broker = make_broker(config.clone());
    let (_layer, mut tel) = otel::install(&config).unwrap();
    otel::install_metrics(&broker, &mut tel, &config).unwrap();

    // Let several export cycles fail, then confirm the broker still answers.
    sleep(Duration::from_secs(3)).await;
    let snapshot = timeout(Duration::from_secs(1), async { broker.snapshot() })
        .await
        .expect("broker.snapshot() blocked while exports were failing");
    assert_eq!(snapshot.version, pulsemq::config::VERSION);

    tel.shutdown();
}

#[test]
fn grpc_is_rejected_with_a_message_naming_the_reason() {
    // This build has only the HTTP exporter compiled in, and `grpc` is the
    // first thing an operator will try. Failing at startup beats posting
    // protobuf at a port that speaks gRPC.
    let config = Config {
        otlp_endpoint: Some("http://127.0.0.1:4317".to_string()),
        otlp_protocol: "grpc".to_string(),
        ..Config::default()
    };
    // `Ok` carries a boxed trait object, so match rather than `unwrap_err`.
    let err = match otel::install(&config) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("grpc should have been rejected"),
    };
    assert!(err.contains("HTTP/protobuf only"), "{err}");
}
