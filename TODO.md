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

## 2. More metrics — mosquitto-parity broker status ($SYS + Prometheus) — ✅ DONE

Implemented. `metrics::Snapshot` is collected once and rendered twice —
`to_prometheus()` and `to_sys_topics()` — with `tests/sysinfo.rs` asserting the
two agree, so the surfaces cannot drift. Added per-control-packet
received/sent counters (a `[AtomicU64; 16]` indexed by packet type), publish
payload bytes, socket count, `clients_maximum`/`clients_expired`, uptime and
build version, plus the client/store/subscription gauges computed in
`Broker::snapshot()`. `sysinfo::run` publishes `$SYS/broker/...` retained every
`sys_interval` seconds (default 10, `0` disables), and client PUBLISH under
`$SYS` is refused with `0x90`.

Two decisions worth recording:
- **`$SYS` values are retained in memory only, never persisted.** They are
  recomputed every interval, so writing ~60 rows to SQLite on a timer would be
  a write storm and would resurrect stale statistics after a restart.
- **`$SYS` is excluded from `retained_messages`/`retained_bytes` and from
  `list_retained`.** They share the retained map (that is how late subscribers
  get them), but counting them would have added ~50 to an existing gauge the
  moment $SYS was switched on, silently changing what it measured.

The `load/*` moving averages were deliberately **not** implemented — see the
second-phase note below; `rate()` covers it for Prometheus users. Original
notes follow.

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

---

## 3. Switch the config file format from YAML to JSON — ✅ DONE

Implemented: `config.rs` parses with `serde_json` (`apply_json_str`/
`apply_json_file`, `KNOWN_KEYS`), `bridge::parse_bridges` takes a
`&serde_json::Value`, the default file is `pulsemq.json`, `yaml-rust2` is gone,
and the example is `pulsemq.example.json` (a minimal, copy-paste-valid file).
Per the caveat below we kept **strict JSON** and moved per-option docs to the
README; a `#`/`//` comment is now a startup error rather than being silently
ignored, which is covered by a test so anyone porting a commented
`pulsemq.yaml` is told plainly. Original notes follow.

Replace the YAML config file with **JSON**. Rationale: one config format across
the project (the ACL policy is already JSON), `serde_json` is already a
dependency, and it lets us drop `yaml-rust2`.

### Scope
- **Parsing** (`config.rs`): swap `apply_yaml_str` for `apply_json_str` using
  `serde_json::Value` (mirror the manual, validated extraction already used in
  `acl.rs`). Keep the strict unknown-key check (rename `KNOWN_YAML_KEYS` →
  `KNOWN_KEYS`) and the type/range validation. Precedence is unchanged:
  **defaults < config file < env < CLI**.
- **Bridges** (`bridge.rs`): change `parse_bridges(&Yaml, …)` to take a
  `&serde_json::Value`; update the `bridges` array + `topics` parsing.
- **Discovery / naming**: default files become `pulsemq.json` (drop
  `pulsemq.yaml`/`.yml`); `--config` / `MQTT_CONFIG_FILE` still point anywhere.
  Update `DEFAULT_CONFIG_FILES` and the `--help`/README wording.
- **Example + ignore**: rename `pulsemq.example.yaml` → `pulsemq.example.json`;
  update `.gitignore` (`pulsemq.yaml`/`.yml` → `pulsemq.json`) and `.dockerignore`.
- **Dependency**: remove `yaml-rust2` from `Cargo.toml` once nothing imports it.
- **Docs**: README "Configuration" section + the config table, `CLAUDE.md`
  (default file name, the "wire an option everywhere" checklist), and any
  YAML snippets → JSON.
- **Tests**: the `config` unit tests use YAML strings — convert to JSON, keeping
  the same cases (field application, CLI-over-file precedence, unknown-key and
  wrong-type rejection, empty/comment file, protocol caps, bridges).

### Caveat — comments
JSON has no comments, so the current heavily-commented example can't carry inline
docs. Options (pick one, note it in the PR):
- Ship a plain `pulsemq.example.json` and move the field docs to the README
  (recommended — the README already documents every option), **or**
- Accept JSONC/JSON5 (a `//`-comment-tolerant parser) so the example can stay
  annotated — heavier, adds a dep; probably not worth it.

Keep it strict JSON unless there's a strong reason otherwise.

### Acceptance criteria
- [ ] `pulsemq.json` (auto-discovered) and `--config file.json` load correctly;
      precedence file < env < CLI holds (live check + unit tests).
- [ ] Unknown keys and wrong types are rejected with a clear message.
- [ ] Bridges parse from JSON; the bridge integration test still passes.
- [ ] `yaml-rust2` removed; `cargo fmt`/`clippy -D warnings`/`test` all green.
- [ ] README, CLAUDE.md, example file, and ignore files updated; no lingering
      references to `pulsemq.yaml` / YAML config.

---

## 4. Parse command-line options with `clap`

Replace the hand-rolled argument parser (`apply_args` + the `HELP` string
constant in `config.rs`) with **`clap`** (derive API). Motivation: generated
`--help`/`--version`, automatic validation and error messages, subcommand
support, and less bespoke string-matching to maintain.

### Scope
- Add `clap` (with the `derive` feature) to `Cargo.toml`. Keep the surface small
  — no extra plugins.
- Define a `#[derive(Parser)]` `Cli` struct whose fields mirror every current
  flag (`--listen-addr`, `--ws-listen-addr`, `--admin-addr`, all TLS paths,
  `--password-file`, `--allow-anonymous`, `--acl-file`, `--db-path`,
  `--max-packet-size`, `--receive-maximum`, `--max-session-expiry`,
  `--maximum-qos`, `--retain-available`, `--topic-alias-maximum`,
  `--server-keep-alive`, `--config`, …). Preserve the **exact flag names** so
  existing invocations and docs keep working.
- Make CLI options `Option<T>` so "unset" is distinguishable — the layering
  (`defaults < file < env < CLI`) must be preserved: only apply a CLI value when
  the user actually passed it. `clap`'s env integration is tempting but would
  collapse our env layer into CLI; **keep env handling in `apply_env`** so the
  precedence order stays intact (or verify clap's `.env()` yields identical
  ordering before adopting it).
- Fold the `--hash-password <USER>` and any other one-shot commands into a clap
  **subcommand** (e.g. `pulsemq hash-password <user>`) or an `Option<String>`
  flag — match current behavior (reads password from stdin, prints the line).
- Drop the `HELP` constant; let clap generate help/usage. Wire `--version` from
  `CARGO_PKG_VERSION`.
- `Config::load()` keeps its shape: build `Cli` via `Cli::parse()`, then apply
  onto the config after defaults/file/env.

### Docs & tests
- Update the README (any literal `--help` output block) and `CLAUDE.md`
  (the "wire an option everywhere" checklist — `apply_args` + HELP becomes "add a
  field to the clap `Cli` struct").
- The `config` unit tests build args as `Vec<String>`; adapt them to
  `Cli::try_parse_from([...])`. Keep coverage of precedence (CLI overrides file
  and env) and of parse errors (bad value → nonzero exit / `Err`).

### Acceptance criteria
- [ ] Every existing flag works with the same name and semantics; `--help` and
      `--version` are generated by clap.
- [ ] Precedence defaults < file < env < CLI is unchanged (unit tests prove CLI
      wins over env which wins over file).
- [ ] `hash-password` still works.
- [ ] `cargo fmt`/`clippy -D warnings`/`test` green; README + CLAUDE.md updated.

---

## 5. Full code audit — refactor, optimize, harden error handling

A sweep over the whole crate (~6.1k lines, 19 modules) now that the feature set
is settled. Three passes, ideally three commits, so a regression is easy to
bisect. **Behavior must not change** except where a genuine bug is fixed — every
fix gets a test that fails before it.

### Pass A — correctness & error handling (highest value)
- **No panics on the network path.** Audit every `unwrap()`/`expect()`/slice
  index/`as` cast reachable from untrusted input (currently ~20 unwrap/expect
  sites and ~79 numeric casts across `src/`). A malformed packet must yield a
  protocol error and a DISCONNECT with the right reason code — never a panic
  that kills the connection task (or worse, poisons the shared `Mutex`).
  - Start at `codec/`, `packet/`, `framing`, `topic`, then `broker`.
  - Prefer `checked_add`/`try_from`/`get()` over `as` and `[i]` on
    attacker-controlled lengths. VBI/length fields are the classic overflow spot.
- **Mutex poisoning**: `broker::lock()` — decide and document the policy. A
  panic while holding `State` currently poisons it for the whole process. Either
  keep `unwrap()` (fail fast, but then Pass A must guarantee no panics under the
  lock) or recover with `into_inner()`. Make it deliberate, not accidental.
- **Error taxonomy** (`error.rs`): make sure each `MqttError` maps to the right
  MQTT reason code per version, and that v3.x paths (no reason codes, no server
  DISCONNECT) degrade correctly instead of silently doing nothing.
- **Every `Result` is handled**: grep for ignored results (`let _ =`) and confirm
  each is intentional (best-effort sends on a closing socket are fine; a swallowed
  storage error is not). Add a comment where it's deliberate.
- Confirm all three protocol versions still behave identically after refactors —
  the version-aware integration tests are the safety net.

### Pass B — security review
- **Resource limits / DoS**: verify `max_packet_size` is enforced *before*
  allocating, that the VBI decoder can't be made to spin, and that a client
  can't exhaust memory via: huge/many subscriptions, topic-alias table growth,
  unbounded offline queues for durable sessions (should be capped — ties into
  `publish_dropped` in item 2), retained-message count/size, or many half-open
  connections (consider a CONNECT timeout + max-connections cap).
- **Authn/authz**: re-verify the identity pipeline (password → cert CN →
  anonymous) can't be skipped; that ACL checks cover publish, subscribe, *and*
  retained delivery and will messages; that a SIGHUP reload can't transiently
  permit a revoked topic. Confirm auth failures are constant-time where it
  matters (`auth.rs` PBKDF2 compare) and that timing doesn't leak user existence.
- **Secrets hygiene**: no passwords/tokens in logs or error messages (bridge
  credentials, `admin_token`, password-file contents). Check `Debug` impls.
- **TLS**: confirm `tls_insecure` is loudly warned about at startup and is never
  the default; check that mTLS actually *requires* a cert on both ports.
- **Admin surface**: bearer-token comparison should be constant-time; `/metrics`
  and `/mcp` must stay guarded; MCP tool inputs validated.
- **Input validation**: UTF-8 rules (§1.5.4 — no U+0000, no surrogates), topic
  name/filter validation on every path, client-id constraints.
- Run `cargo clippy -W clippy::pedantic` once and triage (don't adopt wholesale);
  consider `cargo audit`/`cargo deny` in CI for advisories.

### Pass C — refactor & optimize
- **`broker/mod.rs` is 1343 lines** — the largest module by far. Split along
  seams that already exist: routing, QoS/inflight state machine, retained store,
  will handling, session lifecycle. `config.rs` (880) similarly splits into
  parse/apply layers (coordinate with items 3 & 4 — both touch it).
- **Allocation on the hot path**: `topic::matches` allocates two `Vec`s per call
  and is invoked per-subscription per-publish — switch to `split('/')` iterators
  (zero-alloc) and benchmark. Look for needless `to_string()`/`clone()` in
  routing and delivery; consider `Arc<[u8]>` payloads so fan-out doesn't copy.
- **Routing cost** is currently linear over subscriptions; if benchmarks justify
  it, consider a topic tree. Measure first — don't add a trie on a hunch.
- Reduce duplication between the server connection task and the bridge client
  (both do CONNECT/keepalive/packet-loop work).
- Tighten module visibility (`pub` → `pub(crate)` where nothing external needs it).

### Acceptance criteria
- [ ] No `unwrap`/`expect`/indexing reachable from untrusted input; fuzz-ish
      test that feeds malformed/truncated packets and asserts no panic.
- [ ] Each security item above is checked off with a note on what was found.
- [ ] Any bug fixed has a regression test; all 27+ tests still pass.
- [ ] `cargo fmt`/`clippy -D warnings`/`test` green; no behavior change otherwise.
- [ ] Notable findings summarized in the commit message / a short SECURITY note.

---

## 6. Ship telemetry & logs to Datadog / Splunk / OTLP

Let PulseMQ push its **logs** and **metrics** to an external observability
backend, instead of only being scraped on `/metrics`. Useful for edge/Pi
deployments where nothing is scraping the box.

`tracing` + `tracing-subscriber` are already dependencies and all logging goes
through them, so this is mostly a matter of adding an exporter layer — not
re-instrumenting the code.

### Recommended approach — OTLP first
Prefer **OpenTelemetry (OTLP)** as the single native export path rather than one
integration per vendor: Datadog, Splunk (Observability Cloud), Grafana,
Honeycomb, and the OTel Collector all ingest OTLP, and the Collector can fan out
to anything else. One exporter, every backend.
- Add `opentelemetry`, `opentelemetry-otlp`, `tracing-opentelemetry` (weigh the
  dependency cost — it is not small; consider putting the whole thing behind a
  **non-default Cargo feature** like `otel` so the lean default build and the
  Raspberry Pi image stay small).
- Export **metrics** (reuse the item-2 counters/gauges — same collect-once,
  render-many source) and **logs** (a `tracing` layer). Traces/spans are optional;
  per-connection spans would be a nice third phase.
- Config: `otlp_endpoint`, `otlp_protocol` (grpc|http), `otlp_headers` (for API
  keys), `otlp_interval`, `service_name` — wire through the config checklist.

### Direct vendor paths (only if OTLP isn't enough)
- **Splunk HEC**: POST JSON events to `/services/collector/event` with an
  `Authorization: Splunk <token>` header. Simple enough to implement by hand
  over the existing HTTP-ish code; a good fallback for log-only setups.
- **Datadog**: the Agent accepts DogStatsD (UDP) for metrics and the HTTP logs
  intake for logs. Direct HTTP intake needs an API key. Only worth it if the
  user specifically wants agent-less Datadog.
- Whatever is added, it must be **optional and off by default**, and must never
  block or slow the MQTT path — batch on a background task with a bounded queue
  and drop (with a counter) rather than applying backpressure.

### Cross-cutting requirements
- **Structured logs**: audit existing log sites for consistent fields
  (`client_id`, `identity`, `topic`, `reason_code`) so the exported JSON is
  actually queryable. Redact secrets (ties into item 5's secrets hygiene).
- **Failure isolation**: an unreachable collector must only log a throttled
  warning; never crash, never stall the broker, never grow memory without bound.
- Document the whole thing in a README "Observability" section alongside the
  existing Prometheus/`$SYS` docs.

### Acceptance criteria
- [ ] With export enabled, logs and metrics arrive at an OTLP collector
      (verify against a local `otel/opentelemetry-collector` container).
- [ ] Disabled by default; the default build/image size is unaffected (feature-gated).
- [ ] A dead/slow collector degrades gracefully — bounded queue, drop counter,
      throttled warning, no impact on publish latency.
- [ ] No secrets in exported logs.
- [ ] Config options wired everywhere (config checklist) + README section.
