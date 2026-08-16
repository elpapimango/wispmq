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

## 2. More metrics — mosquitto-parity broker status ($SYS + Prometheus)

Bring broker statistics up to **mosquitto parity**, exposed **two ways**:

1. **`$SYS/broker/...` MQTT topics** — clients subscribe (e.g. `$SYS/#`) and read
   broker status directly over MQTT, updated every `sys_interval` seconds.
2. **Prometheus `/metrics`** — the same values as `mqtt_*` series (and reflected
   in the MCP `broker_stats` tool).

Same underlying counters/gauges drive both surfaces — collect once, render twice.

### $SYS topic publisher
- New `sysinfo` (or extend `metrics`) module + a periodic task started from
  `main`. Every `sys_interval` seconds it publishes the current values to
  `$SYS/broker/...` as **retained** messages (so late subscribers get the last
  value immediately) via a broker method (reuse `inject_publish`, source
  `"$sys"`, or a dedicated internal publish). `$SYS/broker/version` is **static**
  (publish once, retained).
- Config: `sys_interval` (seconds; default 10; **0 disables** $SYS updates), wired
  through YAML/env/CLI/README/example + the config checklist.
- Routing: `$`-topics are already excluded from `#`/`+` at level 0
  (`topic::matches`), so `$SYS` only reaches clients that subscribe to it
  explicitly — good. Make sure clients are *not* allowed to publish to `$SYS/#`
  (reject inbound publishes to `$SYS/...`).
- ACL note: consider gating `$SYS` subscribe behind the ACL like any topic.

### Counters to add (increment on the hot paths)
Currently: connections_total, packets_received/sent_total, bytes_received/sent_
total, publish_received/delivered_total, bridge_forwarded_{out,in}_total; gauges
clients_connected, sessions_total, retained_messages, subscriptions_total,
bridges_connected. Add:
- **Per-control-packet counters, received & sent**: connect, connack, publish,
  puback, pubrec, pubrel, pubcomp, subscribe, suback, unsubscribe, unsuback,
  pingreq, pingresp, disconnect, auth. Increment in `server.rs` (the `send`
  helper for sent; the read arm for received) and bridge. A `[AtomicU64; N]`
  indexed by packet type keeps it cheap; render each as its own series/topic.
- **messages_received / messages_sent**: total of all packet types (sum, or
  separate counters).
- **publish payload bytes** received / sent (distinct from total bytes).
- **publish_dropped**: messages dropped for offline/over-quota durable clients
  (when the offline queue is bounded — see below) + rejected (ACL/qos/retain).
- **connections/socket count**: total socket connections accepted.
- **clients_maximum**: high-water mark of concurrent connected clients.
- **clients_expired**: persistent sessions removed by expiry.
- **uptime_seconds** / process start time; `$SYS/broker/version` (static).

### Gauges to add (compute in `Broker::snapshot()` under the lock)
- **clients_total** (connected + disconnected persistent sessions),
  **clients_disconnected** (offline persistent), **clients_connected** (have).
- **store/messages count + bytes**: retained + queued messages, and their
  payload bytes. **retained bytes** too.
- **shared_subscriptions_count**; keep **subscriptions_count**.
- **packet/out count + bytes**: total queued-for-delivery across sessions
  (sum of session queues / inflight) — useful backpressure signal.

### Load moving averages (1min / 5min / 15min) — second phase
mosquitto's `$SYS/broker/load/...` are per-minute counts averaged over 1/5/15
min for: connections, bytes received/sent, messages received/sent, publish
dropped/received/sent, sockets. Implement with a 1 Hz sampler that snapshots the
relevant counters, keeps ~15 min of per-second deltas (ring buffer) or an EWMA,
and exposes the three windows. This is more involved — do it after the plain
counters/gauges land. Prometheus users can also derive rates with `rate()`, so
the `load/*` topics are mainly for $SYS parity.

### `$SYS` ↔ Prometheus name map (keep the `mqtt_` prefix)
| `$SYS/broker/…` | Prometheus |
|---|---|
| `bytes/received` · `bytes/sent` | `mqtt_bytes_received_total` · `mqtt_bytes_sent_total` (exist) |
| `messages/received` · `messages/sent` | `mqtt_messages_received_total` · `mqtt_messages_sent_total` |
| `publish/messages/received` · `/sent` · `/dropped` | `mqtt_publish_received_total` (exist) · `mqtt_publish_sent_total` · `mqtt_publish_dropped_total` |
| `publish/bytes/received` · `/sent` | `mqtt_publish_bytes_received_total` · `mqtt_publish_bytes_sent_total` |
| `mqtt/<packet>/{received,sent}` | `mqtt_<packet>_{received,sent}_total` |
| `clients/connected` · `/disconnected` · `/total` · `/maximum` · `/expired` | `mqtt_clients_connected` (exist) · `mqtt_clients_disconnected` · `mqtt_clients_total` · `mqtt_clients_maximum` · `mqtt_clients_expired_total` |
| `subscriptions/count` · `shared_subscriptions/count` | `mqtt_subscriptions_total` (exist) · `mqtt_shared_subscriptions_count` |
| `retained messages/count` | `mqtt_retained_messages` (exist) |
| `store/messages/count` · `/bytes` | `mqtt_store_messages_count` · `mqtt_store_messages_bytes` |
| `connections/socket/count` | `mqtt_socket_connections_total` |
| `connection/<name>` (bridge up/down) | `mqtt_bridges_connected` (exist) + per-bridge label/topic |
| `version` | `mqtt_build_info{version="…"}` (static) |
| `load/**` | derive via `rate()`; $SYS-only for parity |

### Where to touch
- `metrics.rs`: add atomics (incl. the per-packet array) + `Snapshot` fields +
  `to_prometheus()`. `broker/mod.rs`: gauge computation in `snapshot()`; a
  `$SYS` publish path. `server.rs`: per-packet + byte + clients_maximum counting.
  `admin.rs`: extend `broker_stats`. `config.rs`: `sys_interval`. `main.rs`:
  spawn the $SYS publisher.
- Bounding the offline queue would make `publish_dropped` meaningful (currently
  the queue is unbounded) — optional companion change.

### Acceptance criteria
- [ ] Subscribing to `$SYS/#` returns the broker-status topics; values refresh
      every `sys_interval` and `sys_interval: 0` disables updates.
- [ ] `$SYS/broker/version` is retained/static; clients cannot publish to `$SYS`.
- [ ] Equivalent `mqtt_*` series appear in `/metrics` (valid HELP/TYPE) and move
      under load; `broker_stats` MCP tool reflects them.
- [ ] Per-control-packet received/sent counters are correct (unit/integration
      test), and the `$SYS`↔Prometheus values agree.
- [ ] README metrics + $SYS sections and `pulsemq.example.yaml` updated.
