# TODO / Roadmap

Work items for PulseMQ, most important first. This file is meant to be picked up
by a fresh Claude session — see `CLAUDE.md` for architecture, conventions, and
the "wire a config option everywhere" checklist. Keep every change green under
`cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D
warnings`, and `cargo test`, and add tests for new behavior.

When you finish an item, tick its boxes, move it to a "Done" note, and commit.

---

## 1. Message forwarding (broker-to-broker bridge) — ✅ DONE

Implemented in `src/bridge.rs` (+ `broker` internal API, `tls::client_config`,
`ws::client`, config `bridges:` list, `tests/bridge.rs`, README/example). All
acceptance criteria below are met: bidirectional forwarding over tcp/tls/ws/wss,
reconnect+backoff, loop prevention via `no_local`, QoS 0/1/2, bridge metrics.
Verified with two in-process brokers (integration test) and two live PulseMQ
instances via mosquitto. The original design notes are kept below for reference.

Let PulseMQ **forward** messages to/from one or more remote MQTT brokers, the
way a mosquitto "bridge" does — so PulseMQ can aggregate edge brokers up to a
central one, or fan a central broker's topics down to the edge.

### Scope / design
- Add a `bridge` module that runs an **outbound MQTT client** to each configured
  remote. Reuse the existing wire code: `codec`, `packet`, `framing`
  (`read_packet`/`write_packet` already work over any `AsyncRead + AsyncWrite`,
  and `tls`/`ws` streams compose the same way the server side does).
- Each bridge is its own Tokio task with **automatic reconnect + backoff**. On
  connect it sends CONNECT (default v5; make the version configurable), then
  SUBSCRIBEs to the remote for "in" topics and, for "out" topics, subscribes to
  the local broker and republishes matching messages to the remote.
- Hook into local routing so "out" topics are delivered to the bridge like any
  other subscriber. Options: give the bridge an internal session/`OutTx` in the
  broker, or add a broker-internal subscriber hook. Prefer reusing the existing
  session/delivery path to avoid a parallel code path.
- **Loop prevention**: don't forward a message straight back out the bridge it
  came in on (track origin, or use the v5 No-Local subscription option on the
  bridge's own subscriptions). Verify no infinite echo between two bridged
  brokers.
- **Topic mapping** (optional, nice-to-have): a local/remote topic prefix
  translation per mapping, like mosquitto's bridge `topic <pat> <dir> <local>
  <remote>`.

### Config (wire it through Config + Default, env, CLI, YAML, README, example)
Bridges are a list, so this is the first *structured* config — a YAML list under
a `bridges:` key is the natural home (env/CLI can stay minimal or be skipped for
the list; document that bridges are config-file-only if that's simpler):
```yaml
bridges:
  - name: central
    address: "tls://central.example:8883"   # tcp:// | tls:// | ws:// | wss://
    client_id: "pulsemq-edge-1"
    username: "..."            # optional
    password: "..."            # optional
    tls_ca: "certs/ca.pem"     # optional (for tls/wss)
    tls_cert: "..."            # optional (mutual TLS)
    tls_key: "..."
    keepalive: 30
    topics:
      - { pattern: "sensors/#", direction: out, qos: 1 }   # local -> remote
      - { pattern: "cmd/#",     direction: in,  qos: 1 }   # remote -> local
```

### Acceptance criteria
- [ ] Two PulseMQ instances bridged together: publishing to a bridged "out"
      topic on A is received by a subscriber on B, and vice-versa for "in".
- [ ] Survives the remote going away and coming back (reconnect with backoff).
- [ ] No message loops between mutually bridged brokers.
- [ ] Works over tcp/tls/mtls and ws/wss remotes.
- [ ] Integration test in `tests/` (start two in-process brokers on ephemeral
      ports, bridge them, assert delivery) + reconnect test.
- [ ] Add bridge metrics (see item 2): messages forwarded in/out, bridge
      connection state/up, reconnect count.
- [ ] Docs: README "Forwarding / bridging" section + `pulsemq.example.yaml`.

---

## 2. More metrics

Expand `metrics.rs` (counters/gauges) and the Prometheus/MCP snapshot. Current
set: connections_total, packets_received/sent_total, bytes_received/sent_total,
publish_received/delivered_total; gauges clients_connected, sessions_total,
retained_messages, subscriptions_total. Keep the `mqtt_*` prefix.

### Candidate metrics
- [ ] **Per-QoS publish counters**: `mqtt_publish_received_total{qos="0|1|2"}`
      (or separate counters if avoiding labels in the hand-rolled exposition).
- [ ] **Dropped / rejected**: publishes rejected by ACL (`0x87`), by QoS/retain
      not-supported, malformed packets, packets-too-large, keepalive timeouts,
      auth failures — each a counter.
- [ ] **Queue / inflight gauges**: total queued (offline) messages and total
      inflight QoS>0 across sessions; max/among sessions.
- [ ] **Subscription cardinality**: distinct topic filters; shared-subscription
      groups.
- [ ] **Connection churn**: disconnections_total, takeovers_total,
      current connections by transport (tcp/tls/ws) if cheap.
- [ ] **Uptime**: process start time / `mqtt_uptime_seconds`.
- [ ] **Message size**: total payload bytes published; consider a small manual
      histogram (bucketed counters) for payload size — only if it stays simple.

### Notes / where to touch
- Counters live on `Metrics` (atomic) and are incremented on the hot paths in
  `broker/mod.rs` (publish/subscribe/connect handlers) and `server.rs`
  (packet/byte counting, keepalive timeout, disconnect). Gauges are computed at
  scrape time in `Broker::snapshot()` under the state lock — add fields to
  `metrics::Snapshot`, render in `to_prometheus()`, and surface in the MCP
  `broker_stats` tool (`admin.rs`).
- Keep the exposition format valid (HELP/TYPE lines); if adding labels, make
  sure the text output is well-formed. There's a metrics smoke test path via
  `/metrics` — extend coverage.

### Acceptance criteria
- [ ] New series appear in `/metrics` with correct HELP/TYPE and move under load.
- [ ] `broker_stats` MCP tool reflects the new gauges/counters.
- [ ] README metrics list updated.
