//! OTLP telemetry export: broker metrics and logs pushed to an OpenTelemetry
//! endpoint, for deployments where nothing comes to scrape `/metrics`.
//!
//! Off unless `otlp_endpoint` is configured, and only compiled when the
//! **`otel` Cargo feature** is on — the OpenTelemetry dependency tree is large
//! and the default binary and Raspberry Pi image must not carry it. The config
//! keys parse in *every* build so one config file is portable between them;
//! a build without the feature that is asked to export says so loudly rather
//! than starting up silently doing nothing.
//!
//! Both shapes of this module expose the same items, so `main.rs` contains no
//! `#[cfg]`: the feature-off half is a set of no-ops.
//!
//! ## What is exported
//!
//! - **Metrics** — every [`Series`](crate::metrics::Series) from
//!   [`Snapshot::series`](crate::metrics::Snapshot::series), the same list
//!   `/metrics` renders. Adding a statistic to `Snapshot` exports it here too;
//!   there is no second list to forget.
//! - **Logs** — every `tracing` event, via a second subscriber layer.
//!
//! Traces/spans are deliberately not exported: no part of the broker is
//! instrumented with spans today, so there would be nothing to send.
//!
//! ## Why the exporter cannot hurt the broker
//!
//! - The exporter is the **blocking** reqwest client, driven by the SDK's own
//!   dedicated threads. A collector that is slow, wedged, or gone cannot
//!   occupy a tokio worker, so it cannot slow a publish.
//! - The log processor has a **bounded** queue and drops records when it is
//!   full, rather than applying backpressure or growing without limit.
//! - Export failures are reported through `tracing`, and the log layer
//!   **excludes the exporter's own targets** — otherwise a failing export logs
//!   an error, which is queued for export, which fails, forever.

use crate::broker::Broker;
use crate::config::Config;
use crate::error::Result;

/// The `tracing` layer that ships log records, when one was installed.
///
/// Boxed against `Registry` rather than the composed subscriber type, which is
/// why `main` adds it as the *first* layer: at that point the subscriber is
/// still a bare `Registry`. `EnvFilter` is a global filter, so it still applies
/// to these records wherever it sits in the stack.
pub type LogLayer =
    Option<Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>>;

/// Append an OTLP signal path to the configured base URL.
///
/// `otlp_endpoint` is documented as a base (`http://host:4318`), matching
/// `OTEL_EXPORTER_OTLP_ENDPOINT`, but the exporter's programmatic
/// `with_endpoint` is used verbatim with no path appended — so the join happens
/// here.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
fn signal_url(base: &str, path: &str) -> String {
    format!("{}/{path}", base.trim_end_matches('/'))
}

// ---------------------------------------------------------------------------
// Feature enabled
// ---------------------------------------------------------------------------

#[cfg(feature = "otel")]
mod imp {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use opentelemetry::metrics::{AsyncInstrument, MeterProvider as _};
    use opentelemetry::KeyValue;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider};
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::Resource;
    use tracing_subscriber::layer::Layer as _;

    use super::{signal_url, Broker, Config, LogLayer, Result};
    use crate::config::check_otlp_protocol;
    use crate::error::MqttError;
    use crate::metrics::SeriesKind;

    /// Records queued for export before the processor starts dropping. At
    /// WispMQ's log volume this is minutes of buffer, and dropping beats
    /// letting an unreachable collector grow the heap on a 512 MB box.
    const LOG_QUEUE_SIZE: usize = 2048;

    /// How long the exporter waits for the collector before giving up on a
    /// batch. Short on purpose: the next interval brings fresh values anyway.
    const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

    /// `tracing` targets whose records must never be exported.
    ///
    /// The exporter reports its own failures through `tracing`. Without this,
    /// one unreachable collector produces an error, which is queued for export,
    /// which fails, which produces an error — a loop that saturates the queue
    /// and hides everything else. These records still reach the console layer,
    /// which is how an operator sees that export is broken.
    const EXPORTER_TARGETS: &[&str] = &[
        "opentelemetry",
        "opentelemetry_sdk",
        "opentelemetry_otlp",
        "reqwest",
        "hyper",
        "hyper_util",
        "h2",
        "rustls",
        "tower",
    ];

    fn is_exporter_target(target: &str) -> bool {
        // Allocation-free equivalent of `target == t || target.starts_with(&
        // format!("{t}::"))`: strip the candidate prefix, then check what's
        // left is either nothing (exact match) or starts with the `::`
        // module separator (a submodule) — not just any string that happens
        // to start with the same letters (e.g. `wispmq::broker` must not
        // match a hypothetical `wispmq` candidate the way `wispmqx` would
        // with a bare `starts_with(t)`).
        EXPORTER_TARGETS.iter().any(|t| {
            target
                .strip_prefix(t)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
        })
    }

    /// Live telemetry export. Held by `main` for the lifetime of the process so
    /// the providers (and the observable instruments' callbacks) stay alive,
    /// and so the last batch can be flushed on shutdown.
    #[derive(Default)]
    pub struct Telemetry {
        endpoint: Option<String>,
        headers: Vec<(String, String)>,
        interval: Duration,
        want_metrics: bool,
        logs: Option<SdkLoggerProvider>,
        meters: Option<SdkMeterProvider>,
        /// Observable instruments, kept alive only so their callbacks are.
        instruments: Vec<Box<dyn std::any::Any + Send + Sync>>,
    }

    /// One snapshot per collection cycle, shared by every instrument callback.
    ///
    /// The SDK invokes each observable instrument's callback separately, so a
    /// naive implementation would take ~60 `Broker::snapshot()`s — and worse,
    /// sixty *different* ones, which would let a single export report
    /// `packets_received` and `bytes_received` from different instants and
    /// break the collect-once invariant the whole metrics design rests on.
    ///
    /// The callbacks of one cycle run back to back, so a short freshness window
    /// makes them all read the same values. It is a time window rather than a
    /// "have all instruments read yet?" count so that a partial collection
    /// cannot wedge the cache permanently.
    struct SnapshotCache {
        broker: Broker,
        max_age: Duration,
        inner: Mutex<Cached>,
    }

    struct Cached {
        values: Vec<u64>,
        taken: Instant,
    }

    impl SnapshotCache {
        fn new(broker: Broker, interval: Duration) -> Self {
            let values = Self::read(&broker);
            SnapshotCache {
                broker,
                // Half the export interval: long enough that one cycle's
                // callbacks all hit the cache, short enough that the next
                // cycle always refreshes.
                max_age: interval / 2,
                inner: Mutex::new(Cached {
                    values,
                    taken: Instant::now(),
                }),
            }
        }

        fn read(broker: &Broker) -> Vec<u64> {
            broker.snapshot().series().iter().map(|s| s.value).collect()
        }

        fn value(&self, index: usize) -> u64 {
            let mut cached = self.inner.lock().expect("snapshot cache poisoned");
            if cached.taken.elapsed() >= self.max_age {
                cached.values = Self::read(&self.broker);
                cached.taken = Instant::now();
            }
            cached.values.get(index).copied().unwrap_or(0)
        }
    }

    /// Validate the telemetry configuration and, when export is on, install the
    /// log pipeline. Returns the subscriber layer for `main` to compose.
    ///
    /// Called before the `tracing` subscriber exists, so it reports nothing
    /// itself — see [`Telemetry::report`].
    pub fn install(cfg: &Config) -> Result<(LogLayer, Telemetry)> {
        let Some(endpoint) = cfg.otlp_endpoint.clone() else {
            return Ok((None, Telemetry::default()));
        };
        check_otlp_protocol(&cfg.otlp_protocol)?;

        let mut tel = Telemetry {
            endpoint: Some(endpoint.clone()),
            headers: cfg
                .otlp_headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            interval: Duration::from_secs(cfg.otlp_interval.max(1) as u64),
            want_metrics: cfg.otlp_metrics,
            ..Telemetry::default()
        };

        if !cfg.otlp_logs {
            return Ok((None, tel));
        }

        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(signal_url(&endpoint, "v1/logs"))
            .with_headers(tel.headers.iter().cloned().collect())
            .with_timeout(EXPORT_TIMEOUT)
            .build()
            .map_err(|e| MqttError::Config(format!("otlp log exporter: {e}")))?;

        let processor = BatchLogProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(LOG_QUEUE_SIZE)
                    .build(),
            )
            .build();
        let provider = SdkLoggerProvider::builder()
            .with_log_processor(processor)
            .with_resource(resource(cfg))
            .build();

        let layer = OpenTelemetryTracingBridge::new(&provider).with_filter(
            tracing_subscriber::filter::filter_fn(|meta| !is_exporter_target(meta.target())),
        );
        tel.logs = Some(provider);
        Ok((Some(Box::new(layer)), tel))
    }

    /// Start exporting metrics, if export is on and `otlp_metrics` is set.
    ///
    /// Separate from [`install`] because it needs the `Broker`, which does not
    /// exist until persistence and the ACL have loaded.
    pub fn install_metrics(broker: &Broker, tel: &mut Telemetry, cfg: &Config) -> Result<()> {
        let (Some(endpoint), true) = (tel.endpoint.clone(), tel.want_metrics) else {
            return Ok(());
        };

        let exporter = MetricExporter::builder()
            .with_http()
            .with_endpoint(signal_url(&endpoint, "v1/metrics"))
            .with_headers(tel.headers.iter().cloned().collect())
            .with_timeout(EXPORT_TIMEOUT)
            .build()
            .map_err(|e| MqttError::Config(format!("otlp metric exporter: {e}")))?;

        let reader = PeriodicReader::builder(exporter)
            .with_interval(tel.interval)
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource(cfg))
            .build();
        let meter = provider.meter("wispmq");

        // One instrument per series, each reading its own slot of the shared
        // per-cycle snapshot. The index is the position in `series()`, which is
        // deterministic, so instrument and field cannot drift apart.
        let cache = Arc::new(SnapshotCache::new(broker.clone(), tel.interval));
        for (index, series) in broker.snapshot().series().into_iter().enumerate() {
            let cache = Arc::clone(&cache);
            let name = series.otel_name().to_string();
            let help = series.help.into_owned();
            let observe =
                move |inst: &dyn AsyncInstrument<u64>| inst.observe(cache.value(index), &[]);
            let instrument: Box<dyn std::any::Any + Send + Sync> = match series.kind {
                SeriesKind::Counter => Box::new(
                    meter
                        .u64_observable_counter(name)
                        .with_description(help)
                        .with_callback(observe)
                        .build(),
                ),
                SeriesKind::Gauge => Box::new(
                    meter
                        .u64_observable_gauge(name)
                        .with_description(help)
                        .with_callback(observe)
                        .build(),
                ),
            };
            tel.instruments.push(instrument);
        }

        tel.meters = Some(provider);
        Ok(())
    }

    /// The OTLP resource: who is reporting, and which build.
    ///
    /// `mqtt_build_info` has no OTLP equivalent — a constant-1 gauge carrying a
    /// label is a Prometheus idiom — so the version lands here instead.
    fn resource(cfg: &Config) -> Resource {
        Resource::builder()
            .with_service_name(cfg.service_name.clone())
            .with_attribute(KeyValue::new("service.version", crate::config::VERSION))
            .build()
    }

    impl Telemetry {
        /// Log what was (or was not) set up. Called by `main` once the
        /// subscriber is installed, since `install` runs before it exists.
        pub fn report(&self) {
            let Some(endpoint) = &self.endpoint else {
                return;
            };
            tracing::info!(
                "exporting telemetry to {endpoint} over OTLP/HTTP every {}s (metrics: {}, logs: {})",
                self.interval.as_secs(),
                self.meters.is_some(),
                self.logs.is_some(),
            );
        }

        /// Flush and stop both pipelines. Without this the final batch — the
        /// one covering the shutdown itself — is lost.
        pub fn shutdown(self) {
            if let Some(p) = &self.meters {
                if let Err(e) = p.shutdown() {
                    tracing::warn!("otlp metric pipeline shutdown: {e}");
                }
            }
            if let Some(p) = &self.logs {
                if let Err(e) = p.shutdown() {
                    tracing::warn!("otlp log pipeline shutdown: {e}");
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn exporter_targets_are_excluded_from_export() {
            // The feedback loop this prevents: an export failure logs an error,
            // the error is queued for export, that export fails...
            assert!(is_exporter_target("opentelemetry_sdk"));
            assert!(is_exporter_target("opentelemetry_sdk::logs::internal"));
            assert!(is_exporter_target("hyper_util::client::pool"));
            // Broker logs must still be exported — including modules whose name
            // merely starts with the same letters.
            assert!(!is_exporter_target("wispmq::broker"));
            assert!(!is_exporter_target("h2o"));
            assert!(!is_exporter_target("rustls_pemfile"));
        }
    }
}

// ---------------------------------------------------------------------------
// Feature disabled — same signatures, no dependencies, no work.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "otel"))]
mod imp {
    use super::{Broker, Config, LogLayer, Result};

    /// Placeholder telemetry handle. Remembers only whether export was asked
    /// for, so [`Telemetry::report`] can say why nothing is being exported.
    #[derive(Default)]
    pub struct Telemetry {
        configured: bool,
    }

    pub fn install(cfg: &Config) -> Result<(LogLayer, Telemetry)> {
        // The config is still validated in this build, so a file that is wrong
        // is wrong in both — otherwise a value would be accepted here and
        // rejected only after someone rebuilt with the feature on. Nothing is
        // installed either way.
        if cfg.otlp_endpoint.is_some() {
            crate::config::check_otlp_protocol(&cfg.otlp_protocol)?;
        }
        Ok((
            None,
            Telemetry {
                configured: cfg.otlp_endpoint.is_some(),
            },
        ))
    }

    pub fn install_metrics(_broker: &Broker, _tel: &mut Telemetry, _cfg: &Config) -> Result<()> {
        Ok(())
    }

    impl Telemetry {
        pub fn report(&self) {
            if self.configured {
                // Loud, because the alternative is an operator watching an
                // empty dashboard with a config that looks correct.
                tracing::warn!(
                    "otlp_endpoint is set but this binary was built without the 'otel' \
                     feature, so no telemetry is exported; rebuild with \
                     `cargo build --features otel`"
                );
            }
        }

        pub fn shutdown(self) {}
    }
}

pub use imp::{install, install_metrics, Telemetry};

#[cfg(test)]
mod tests {
    use super::signal_url;

    #[test]
    fn signal_paths_are_appended_to_the_base_url() {
        // `otlp_endpoint` is a base, like OTEL_EXPORTER_OTLP_ENDPOINT. The
        // exporter's own `with_endpoint` appends nothing, so getting this wrong
        // posts everything to `/` and every export 404s.
        assert_eq!(
            signal_url("http://127.0.0.1:4318", "v1/metrics"),
            "http://127.0.0.1:4318/v1/metrics"
        );
        // A trailing slash must not double up.
        assert_eq!(
            signal_url("http://collector:4318/", "v1/logs"),
            "http://collector:4318/v1/logs"
        );
        // A base path (a gateway routing OTLP under a prefix) is preserved.
        assert_eq!(
            signal_url("https://otlp.example.com/ingest", "v1/metrics"),
            "https://otlp.example.com/ingest/v1/metrics"
        );
    }
}
