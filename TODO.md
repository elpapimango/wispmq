# TODO / Roadmap

Work items for PulseMQ, most important first. This file is meant to be picked up
by a fresh Claude session — see `CLAUDE.md` for architecture, conventions, and
the "wire a config option everywhere" checklist. Keep every change green under
`cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D
warnings`, and `cargo test`, and add tests for new behavior.

When you finish an item, tick its boxes, move it to a "Done" note, and commit.

## Status at a glance

| # | Item | State |
|---|------|-------|
| 1 | Forwarding (broker-to-broker bridge) | ✅ done |
| 2 | Metrics — `$SYS` + Prometheus | ✅ done (`load/*` averages skipped) |
| 3 | Config file YAML → JSON | ✅ done |
| 4 | `clap` for CLI parsing | ✅ done |
| 5A | No-panic / error-handling audit | ✅ done |
| 5B | Security review | ✅ done |
| 5C | Refactor & optimize | ✅ done (1 bullet declined) |
| — | [Bridge send metrics](#resolved-bridge-traffic-is-counted) | ✅ resolved — counted |
| 6 | Telemetry/log export (OTLP) | ✅ done (feature-gated `otel`) |
| — | Mutual-TLS test coverage (admin port, WS) | ✅ done |
| 7 | Post-0.9.2 audit — fix pass (QoS2/retained/timing/panic bugs, dep trim, test dedup) | ✅ done |
| 8 | Post-0.9.2 audit — backlog (unbounded outbound channel, admin timeout, WS frame cap, ...) | ✅ done |
| 9 | New topic ideas (not yet scoped) | 💡 ideas |

**Nothing is left on the roadmap.** All of item 8's backlog is resolved —
either fixed in code, resolved as intentional/accepted with a documentation
note, or (for two low-priority perf bullets) fixed and confirmed
output-identical. Item 9 is a set of unscoped ideas, not a committed item —
pick one and scope it before starting if wanted.

**Mutual-TLS test coverage** (optional follow-up, now done): admin-port mTLS
and WS+mTLS both worked already but had no automated tests. Added
`tests/admin_tls.rs` (valid client cert served, no cert rejected) and two
tests in `tests/websocket.rs` (client cert CN drives ACL identity; no cert is
rejected). Note for anyone extending these: a TLS 1.3 client's `connect()`
can return `Ok` even when the server is about to reject the handshake for a
missing client certificate — completion only requires the client to send its
own (possibly empty) Finished, not to hear back from the server. The
rejection is only observable by then actually using the connection (a read,
or the next protocol handshake), which is why both "no cert" tests assert on
that instead of on `connect()`'s result.

`main` is at crate version **0.9.2** (item 6), tagged `v0.9.2` and released
(marked latest — see the release note at the bottom of this file). This
project was originally released as 1.0.0 → 1.1.1 → 1.2.0, then renumbered to
start at 0.9.0 before those tags/Releases/images existed for long; 0.9.0 and
0.9.1 are historical waypoints referenced in commit messages and release
notes, not separate tags or Releases — **v0.9.2 is the only one that actually
exists.**

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
| `mqtt/<packet>/{received,sent}` | `mqtt_packet_<packet>_{received,sent}_total` (renamed in 0.9.1; the original `mqtt_<packet>_...` collided with the aggregate publish counters) |
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

## 4. Parse command-line options with `clap` — ✅ DONE

Implemented. `clap` (derive) now owns the command line: flags live in a
`#[derive(Parser)] Cli` struct in the new `src/cli.rs`, and `config.rs` lost
`apply_args`, the `HELP` constant, `cli_config_path` and the per-flag `parse_*`
helpers (1011 lines down to 725). Every flag kept its name; help, usage errors and
`--version` (from `CARGO_PKG_VERSION`) are generated, with `help_heading`
preserving the old grouping. Exit codes are unchanged: 0 for `--help`/
`--version`, 2 for a bad flag.

Decisions worth recording:
- **Env handling stayed in `apply_env`**, not clap's `env` attribute. clap would
  have folded env into the CLI layer, and it would have turned a malformed env
  value from "ignored, keep the lower layer" into a hard startup error. The env
  var name is quoted in each flag's help text so `--help` still documents the
  pair.
- **Every `Cli` field is an `Option`** so "flag absent" stays distinguishable
  from "flag set to the default value" — otherwise the CLI layer would stomp the
  config file with clap's defaults. `unset_flags_leave_lower_layers_alone`
  guards this.
- **`--hash-password` stayed a flag** (`Option<Option<String>>`) rather than
  becoming a subcommand, so existing invocations keep working. `Startup` gained
  a `HashPassword` variant and lost `Exit` (clap exits by itself for help and
  version), which moved the argv scan out of `main.rs`.
- **Boolean flags gained a bare form**: `--allow-anonymous` alone now means
  `true`, via `num_args(0..=1)` + `default_missing_value`. Passing a value still
  works and the lenient spellings (`1`/`yes`/`on`/…) are shared with the env and
  file layers, so no pre-clap invocation changed meaning.
- Also fixed a pre-existing `clippy::nonminimal_bool` in
  `packet/subscribe.rs` that a newer clippy than the last CI run flags.

Original notes follow.

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
- [x] Every existing flag works with the same name and semantics; `--help` and
      `--version` are generated by clap.
- [x] Precedence defaults < file < env < CLI is unchanged (unit tests prove CLI
      wins over env which wins over file — `cli_beats_env_beats_file`).
- [x] `hash-password` still works (verified live, including with a malformed
      `pulsemq.json` present, which must not block it).
- [x] `cargo fmt`/`clippy -D warnings`/`test` green (54 tests at the time; the
      suite has grown since); README + CLAUDE.md updated.

---

## 5. Full code audit — Pass A ✅, Pass B ✅, Pass C ✅ — DONE

A sweep over the whole crate, in three passes so a regression is easy to bisect.
**Behavior must not change** except where a genuine bug is fixed — every fix gets
a test that fails before it.

**Pass A (error handling) — ✅ DONE** (commit `f7ee7d3`). The decoder held up:
`Reader::ensure` fronts every buffer access, `varint` cannot overflow, and
framing checks `max_packet_size` *before* allocating. `tests/malformed.rs` now
proves the no-panic property empirically rather than by inspection. Fixed: a
silent persistence-loss path (a dead storage thread discarded every write
unreported), a `len as u16` truncation in `Writer` that would have desynced the
peer's framing, and an `unreachable!()` that a refactor could have made live.
The mutex-poisoning policy is now documented as deliberate fail-fast.

**Pass B (security) — ✅ DONE** (commit `0bc7bb4`). Four findings, each with a
regression test: a PBKDF2 **username-enumeration timing leak** (unknown users
returned instantly, known users cost 200k iterations); a **Will authorization
bypass** — the Will was authorized at CONNECT but published unchecked at
disconnect, so an ACL reload revoking a permission *fired* the publish it meant
to block; an **unbounded offline queue** (now `max_queued_messages`, default
1000, with `mqtt_publish_dropped_total`); and **secrets in `Debug` output**
(`admin_token` is now `config::Secret`, `BridgeConfig` has a redacting `Debug`,
and `tls_insecure` warns loudly at startup). Verified sound and left alone:
pre-allocation size enforcement, CONNECT/keep-alive timeouts, and the
already-constant-time admin token compare.

**Pass C is done** — see below. Item 4 had moved the command line out to
`cli.rs`, taking `config.rs` from 1011 to 725 lines, so C was re-scoped to
`broker/mod.rs` and the routing hot path; the `config.rs` split in the original
notes was dropped as no longer worth doing.

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

### Pass C — refactor & optimize — ✅ DONE

Delivered in five commits so the diff stays bisectable:

1. **`topic::matches` is allocation-free** — it collected two `Vec<&str>` per
   call on the routing hot path; now walks `split('/')` iterators. 102 -> 61
   ns/call. Guarded by an equivalence test that keeps the old implementation as a
   reference and compares ~1.2M filter/topic pairs.
2. **Measured before optimizing** (`tests/bench_routing.rs`), which redirected
   the rest of the item — see "the trie question" below.
3. **`Arc<[u8]>` payloads** — `Message`/`Publish` held `Vec<u8>`, so fan-out
   copied the payload per recipient plus again into the QoS>0 retransmit buffer.
   Fan-out to 100 subscribers of a 64 KiB message: 5.1 ms -> 32 us per publish
   (162x). Cost is now independent of payload size.
4. **`broker/mod.rs` split** 1552 lines -> 9 files, largest 263, along the
   section banners the file already had. Verified as a pure move: all 36
   functions present, bodies byte-identical.
5. **One-pass delivery + visibility tightening**, the latter of which exposed
   two pieces of long-dead code (`Session::clear`, `Subscription::retain_handling`).

**The trie question is answered: don't.** Matching costs ~19 ns per subscription
and is perfectly linear; each *delivery* costs ~430 ns. Delivery is 20-25x the
cost of matching, so a trie would optimize the cheap half. Revisit only if a
deployment genuinely runs 10k+ subscriptions on one broker, and re-measure first.

**Declined: deduplicating the server connection task and the bridge client.**
They share the *shape* of a `tokio::select!` over "outgoing channel" and "inbound
socket read" — about six lines of structure — and nothing else. The server
accepts a CONNECT and drives the broker's QoS state machine, enforces keep-alive
as a read timeout, and reports outcomes as `Action` plus Will suppression. The
bridge initiates a CONNECT, owns *client-side* QoS state (its own packet ids and
inbound QoS 2 set), pings on an interval, rewrites each publish's QoS per topic,
and returns `Result<bool>` to drive reconnect. A shared helper would need to be
parameterized on the outgoing transform, the inbound handler, the timeout
behavior, the shutdown behavior and the return type — five injection points for
six shared lines, which costs more than it saves and would couple the server hot
path to the bridge. The parts that *are* genuinely shareable are already shared:
`framing::read_packet`/`write_packet`, the packet/codec layer, `tls::client_config`
and `ws::client`.

Two things noticed here but left alone:
- `SubRecord::to_topic_filter` has no callers, but it is public API on a public
  struct, so removing it is a breaking change rather than a drive-by cleanup.
- Bridge-to-remote writes were not counted by `record_sent`. Fixed in 0.9.1 —
  see "Resolved: bridge traffic is counted" below.

Original notes follow.

### Pass C — original notes
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

## 6. Ship telemetry & logs to Datadog / Splunk / OTLP — ✅ DONE

Implemented in `src/otel.rs` behind the non-default **`otel`** Cargo feature,
exporting **metrics and logs** over **OTLP/HTTP-protobuf**. Off unless
`otlp_endpoint` is set. `tests/otel.rs` stands up a fake collector — a TCP
listener speaking just enough HTTP — and asserts on what actually went over the
socket, so the wire path is covered rather than just the type-checking.

### Decisions worth recording

- **Metrics reuse `Snapshot`, and the reuse is now enforced.** `to_prometheus`
  used to hold its own copy of the field list; it was refactored onto a new
  `Snapshot::series() -> Vec<Series>` that the OTLP exporter also consumes, so
  a statistic cannot reach one surface and miss the other. The Prometheus
  output is **byte-identical** to before the refactor (verified by diffing a
  fully-populated snapshot's rendering against the pre-change binary).
- **`Series.name` is the full Prometheus name; `otel_name()` strips `_total`
  for counters only.** The original plan was to store base names and append
  `_total` when rendering, on the belief that every counter ends in `_total`
  and no gauge does. False: `mqtt_sessions_total` and
  `mqtt_subscriptions_total` are gauges. Renaming those would have silently
  broken every dashboard reading them.
- **HTTP/protobuf only, no gRPC.** One transport, one dependency tree. `grpc`
  is a startup error naming the missing build option, in *both* builds — a
  value must not be accepted by the lean binary and rejected by the otel one.
- **Observable instruments with a shared per-cycle snapshot cache.** The 0.32
  SDK invokes each instrument's callback separately, so the naive version takes
  ~60 snapshots per export — and sixty *different* ones, which would let a
  single export report `packets_received` and `bytes_received` from different
  instants. The cache is time-windowed (half the export interval) rather than
  counting instrument reads, so a partial collection cannot wedge it.
- **`otlp_endpoint` is a base URL.** The exporter's programmatic
  `with_endpoint` appends nothing, so `otel::signal_url` adds `/v1/metrics` and
  `/v1/logs`. Getting this wrong posts everything to `/`.
- **The exporter's own targets are excluded from the exported logs.** An
  export failure logs an error, which would be queued for export, which fails —
  a loop that saturates the queue and hides everything else. Those lines still
  reach the console, which is how the failure is seen.
- **`otlp_headers` values are `config::Secret`** with a redacting `Debug` on
  the collection: names show (an operator needs them), values never do.
- **The config keys parse and validate in every build**, so one file is
  portable; a lean binary asked to export warns loudly instead of doing nothing.
- `OTEL_EXPORTER_OTLP_*` env vars are deliberately **not** read — one source of
  truth.

### Acceptance criteria — verified

- [x] Metrics and logs arrive at a collector: `tests/otel.rs` asserts the
      request path, the API-key header, and that the protobuf body carries
      `mqtt_packets_received` (counter, `_total` stripped),
      `mqtt_sessions_total` (gauge, kept), `service.name` and `service.version`
      — plus the log message and its structured fields.
- [x] Disabled by default; `cargo tree` shows **zero** OpenTelemetry/reqwest
      crates in the default build.
- [x] A dead collector degrades gracefully. Measured live: 25 s with nothing
      listening and a 2 s interval gave **one** error line (not a loop), RSS
      flat at 27192 kB start to finish, and `/health` still answering.
- [x] No secrets in exported logs (`Secret` + the redacting `Debug`, with a
      test asserting the value never appears in `{cfg:?}`).
- [x] Config wired through every layer with tests, README "OTLP telemetry
      export" section, `pulsemq.example.json`, a CI job for the feature build,
      and a `FEATURES` build-arg on the Dockerfile.

### Not done, on purpose

- **Traces/spans.** Nothing in the broker is instrumented with spans, so there
  would be nothing to send. Per-connection spans remain a possible later phase.
- **Direct Splunk HEC and Datadog intakes.** Both ingest OTLP, directly or via
  the Collector, so a vendor-specific path would be a second thing to maintain
  for no new reach.

Original notes follow.

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

---

## 7. Post-0.9.2 audit — fix pass — ✅ DONE

A three-way parallel audit (security-sensitive modules; broker/codec core
logic; dependency surface, config wiring, and test cleanliness) turned up six
self-contained, verifiable bugs and cleanups, each fixed with a regression
test:

- **QoS 2 PUBREC error handling leaked packet identifiers**
  (`src/broker/publish.rs`, `handle_pubrec`). It checked whether the packet id
  was known *before* checking `ack.reason_code.is_error()`, so a client that
  rejected a QoS 2 message with an error PUBREC still got a `PUBREL(Success)`
  reply and had the id parked in `awaiting_pubcomp` forever (violates
  MQTT-4.3.3-4) — `resume_delivery` would keep resending the bogus PUBREL on
  every reconnect, and repeated rejections could exhaust the 65535-id space.
  Fixed by checking `is_error()` first. `tests/interop.rs::
  qos2_pubrec_error_gets_no_pubrel` pins it.
- **Retained-message gauges never excluded or purged expired entries**
  (`src/broker/stats.rs::snapshot`). Unlike `retained()` (the admin/MCP
  listing), the gauge loop counted every non-`$SYS` retained entry regardless
  of `is_expired()`, and nothing anywhere purged an expired entry — it only
  disappeared when overwritten. Result: `mqtt_retained_messages`/
  `mqtt_retained_bytes`/the `$SYS` counters drifted from what `retained()` and
  actual delivery showed, and an expired-but-never-republished topic leaked in
  memory indefinitely. Fixed by having `snapshot()` purge expired entries from
  `st.retained` in the same pass it already walks under the lock — one change
  fixes both the drift and the leak. `tests/sysinfo.rs::
  expired_retained_messages_are_excluded_and_purged` pins it.
- **Admin bearer-token compare leaked the token's length via timing**
  (`src/admin.rs::tokens_match`). Its doc comment claimed constant-time, but
  an early `if a.len() != b.len() { return false }` short-circuited before
  the constant-time loop. Fixed to run over `max(a.len(), b.len())`
  unconditionally, folding the length mismatch into the same diff
  accumulator. `admin::tests::tokens_match_correctness` covers the
  behavior (the timing property itself isn't practically assertable in CI).
- **`auth::from_hex` panicked on malformed multi-byte input** — it sliced the
  password-file salt/hash field by byte index (`&s[i..i+2]`) after only
  checking the byte length was even, so a field containing a UTF-8 character
  whose boundary didn't land on an even offset panicked the whole process at
  startup instead of failing cleanly. Fixed to operate on raw bytes via
  `chunks(2)` with no `str` index slicing. `auth::tests::
  from_hex_rejects_non_ascii_without_panicking` pins it.
- **`tokio`'s `full` feature pulled in unused surface** (`Cargo.toml`) —
  grepped the crate for `tokio::fs`/`tokio::process`/stdio helpers (none
  found; file I/O is `std::fs`, stdin is `std::io::stdin`) and narrowed to
  the features actually used: `rt-multi-thread`, `macros`, `net`, `sync`,
  `time`, `signal`, `io-util`. Drops the `parking_lot` subtree (7 transitive
  packages) from the default build; `cargo tree` diffed before/after to
  confirm, no behavior change.
- **`free_addr()` was copy-pasted across 6 integration-test files** — moved
  to `tests/common/mod.rs` (the standard shared-code pattern for Rust
  integration tests) and imported via `mod common; use common::free_addr;`.

All findings were verified by reading the actual code, not taken on an
agent's say-so. Verification: `cargo fmt --all -- --check`, `cargo clippy
--all-targets --all-features -- -D warnings`, `cargo test --locked` (48 lib
unit tests, up from 46; all integration suites green), and `cargo test
--locked --features otel` — all green.

---

## 8. Post-0.9.2 audit — backlog — ✅ done

Real findings from the same audit that need a design decision (a backpressure
policy, a new config knob, or changes spanning several send paths) rather
than a same-pass fix. Pick any item; each is independent.

- [x] **Unbounded per-connection outbound channel — memory-exhaustion DoS**
      (High). ✅ done — bounded to `OUTBOUND_CHANNEL_CAPACITY` (1024) via
      `try_send`; QoS 0 drops on a full channel, QoS 1/2 falls through to the
      offline queue instead of marking an id in-flight it can never be acked
      for.
- [x] **Admin HTTP server has no read/idle timeout** (Medium/High). ✅ done —
      fixed 15s `ADMIN_REQUEST_TIMEOUT` constant on request reads, mirroring
      `CONNECT_TIMEOUT`'s precedent.
- [x] **WebSocket transport doesn't cap frame/message size to
      `max_packet_size`** (Medium). ✅ done — `src/ws.rs` now builds a
      `WebSocketConfig` from `max_packet_size` and passes it to
      `accept_hdr_async_with_config`/`client_async_with_config`, so
      tungstenite refuses to buffer a frame/message past that bound instead
      of allowing its 64 MiB/16 MiB defaults ahead of
      `framing::read_packet`'s own check.
- [x] **`client_max_packet_size` (CONNECT's Maximum Packet Size property) is
      parsed but never enforced on send** (Medium) — MQTT-3.1.2-24. ✅ done —
      `src/broker/routing.rs` now checks the wire size of every QoS 0/1/2
      PUBLISH (fresh delivery in `deliver_to_session`, and re-delivery from
      the offline queue in `flush_queue`) against `client_max_packet_size`
      and drops (not sends, not queues) whatever exceeds it, counted in
      `mqtt_publish_dropped_total`.
- [x] **Auto-assigned Client Identifier sessions can never persist to
      SQLite** (Low/informational). ✅ resolved as intentional — confirmed
      deliberate (avoids unbounded SQLite rows keyed by throwaway auto-ids
      from anonymous CONNECTs) and moved to CLAUDE.md's "expected behaviour
      that reads like a bug" list. No code change.
- [x] **`--admin-token` is visible to any local user via `ps aux`/
      `/proc/<pid>/cmdline`** (Low, advisory, not a code bug). ✅ done — added
      a README note (Admin server → Authentication) steering operators
      toward `MQTT_ADMIN_TOKEN` or the config file for this secret.
- [x] **`Snapshot::series()` reformats ~30 `Cow::Owned` strings on every
      call** (Low, perf). ✅ done — `src/metrics.rs` now builds the
      per-packet names/HELP text once into a `static LazyLock` table
      (`PACKET_SERIES_TEXT`) and borrows from it (`Cow::Borrowed`) instead of
      `format!`-ing them on every call. `to_prometheus()` output confirmed
      byte-identical before/after via the scratchpad-dump-and-diff method.
- [x] **`Snapshot::to_prometheus()` builds an intermediate `String` per
      series** (Low, perf). ✅ done — `write!` into the output buffer
      directly instead of `format!` + `push_str`, dropping one allocation per
      series per scrape/export.
- [x] **`otel::is_exporter_target` allocates a `format!` per candidate per
      log line** (Low, perf, `otel`-feature-only). ✅ done — rewritten
      allocation-free with `target.strip_prefix(t)` + an empty-or-`::`
      boundary check; `exporter_targets_are_excluded_from_export` still
      passes.
- [x] **`x509-parser` dependency weight for one call site**
      (informational — no action recommended unless revisited). ✅ resolved
      as accepted — re-reviewed, no lighter alternative surfaced;
      `src/tls.rs`'s CN-extraction call site is unchanged. Revisit only if
      that changes.

---

## 9. New topic ideas — 💡 not yet scoped

Surfaced 2026-08-23, not from the audit — candidates for future items. None
have acceptance criteria yet; scope one out before starting it.

- [ ] **Continuous fuzzing** (`cargo-fuzz`/AFL) on the codec. `tests/malformed.rs`
      is a fixed corpus; a fuzz harness finds cases nobody thought to write.
- [ ] **Load/throughput benchmark harness** — many concurrent clients, sustained
      publish rate, latency/memory under real load. Distinct from the routing
      microbenchmarks in `tests/bench_routing.rs` (those measure matching/delivery
      cost in isolation, not end-to-end connection load).
- [ ] **Connection-rate / per-IP limiting** — nothing today caps connection
      attempts per source. Overlaps with item 8's admin-timeout and outbound-
      channel entries as a DoS-hardening cluster.
- [ ] **SQLite backup/restore + WAL tuning** — no documented backup story for
      `/data/*.db`; WAL checkpoint behavior under high write rate untested.
- [ ] **v5 Enhanced Authentication (AUTH packet, SCRAM)** — spec supports it,
      PulseMQ only does username/password. Confirm a real need before building.
- [ ] **Structured audit log for auth/ACL events** — separate from general
      `tracing` output: connect/disconnect, ACL denials, admin actions, for
      compliance/forensics.
- [ ] **Helm chart / k8s manifests** — Docker image exists; no k8s-native
      deployment path if that becomes a target environment.

---

## Resolved: bridge traffic is counted

**Decision: count it.** Found while smoke-testing 0.9.1 and fixed in 0.9.1.

`src/bridge.rs` wrote to its remote with `framing::write_packet` and read with
`framing::read_packet` directly, while the server's connection task went through
a `send` helper that also calls `metrics.record_sent(...)` and a read arm that
calls `record_received(...)`. So **both** directions of the bridge's remote link
were invisible to `packets_sent`/`packets_received`, `bytes_sent`/`bytes_received`
and every `mqtt_packet_*` series. Messages the bridge delivered *locally* through
`route` were counted, which made the gap easy to miss.

The rejected alternative was to document the exclusion — arguing those packets go
to another broker rather than to a subscribing client. Counting won because an
operator watching `bytes_sent` on an edge broker whose only job is forwarding
would otherwise see almost nothing, and because mosquitto counts bridge traffic in
its totals.

What changed:
- `bridge::send` and `bridge::record_received` now wrap every write and read,
  mirroring `server::send`. Only one raw `write_packet` call remains in the file,
  inside `send` itself.
- The affected HELP strings say "clients and bridge remotes" rather than
  "clients", since they no longer mean only client traffic.
- `tests/bridge.rs::bridge_traffic_is_counted_in_the_packet_metrics` pins it. It
  uses CONNECT-sent and CONNACK-received as the giveaways — a broker never sends a
  CONNECT or receives a CONNACK on a client connection, so a non-zero count can
  only have come from a bridge — and drives a broker with **no clients at all**,
  so every counted byte is bridge traffic. Verified to fail with the old
  behaviour ("the bridge's CONNECT to the remote was not counted") and confirmed
  live on two bridged release binaries.

**This inflates existing series for anyone running bridges**, which is why it went
out in 0.9.1 with a note rather than silently.

---

## Historical note: the 0.9.1 waypoint

This body of work was the release before the current one. It predates the
0.9.x renumbering (see CLAUDE.md's versioning note) — no `v0.9.1` tag, image,
or GitHub Release exists on its own; it's folded into the v0.9.2 release
notes' delta-from-0.9.0 section instead. Kept here for the breaking-changes
list it originally shipped with, hand-written rather than
`--generate-notes` because auto-generated notes are a commit list that buries
the part that actually matters to someone upgrading:

- the config file is JSON, not YAML (item 3) — a `pulsemq.yaml` no longer loads;
- `config::Startup` lost `Exit` and gained `HashPassword`, and `config::HELP` is
  gone (item 4) — library-only, but a source break;
- the per-control-packet metrics are `mqtt_packet_<packet>_*` (item 2's note) —
  dashboards scraping the old names need updating;
- traffic counters now include bridge-to-remote packets (see above) — existing
  series read higher on any broker running a bridge.
