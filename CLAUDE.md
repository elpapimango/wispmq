# CLAUDE.md

Guidance for working in this repository.

## Project

**WispMQ** — an **MQTT broker** in Rust (protocols **v5.0**, **v3.1.1**,
**v3.1**), built from the OASIS specs (in `spec/`). Async networking via
**Tokio**; durable state in **SQLite** (bundled via `rusqlite`). Single binary
`wispmq` (crate `wispmq`) plus a library crate. Transports: TCP, TLS, mutual
TLS, WebSockets, WebSockets-over-TLS. Repo:
https://github.com/elpapimango/wispmq

Display name is **WispMQ**; the identifier everywhere (crate, binary, image,
repo, default `wispmq.toml`) is `wispmq`. The `MQTT_*` env vars and `mqtt_*`
metric names are intentionally kept — they describe the protocol, not the
project.

## Project history & status

**Versioning note**: this project briefly shipped as 1.0.0 → 1.1.1 → 1.2.0
before that was judged premature and renumbered to start at 0.9.0. All three
old tags, their GitHub Releases, and the GHCR package were deleted (commit
history was rewritten to match: `git log` messages say 0.9.x, not 1.x).
**v0.9.2** and **v0.9.3** are tagged/released artifacts — 0.9.0 and 0.9.1 are
historical waypoints named in commit messages and the milestone list below,
not separate tags, Releases, or images.

**v0.9.2** added OTLP telemetry export (item 6) on top of the 0.9.1
waypoint's work — forwarding, the 5A/5B audit, JSON config, `$SYS` metrics,
the clap CLI and the 5C refactor (see "Done since 0.9.0" below). A **minor**
bump: it is additive — the feature is off by default, the default build
carries none of its dependencies, and the Prometheus output is byte-identical
to the pre-OTLP build despite the `Snapshot::series()` refactor underneath it.
Its release notes describe the delta from **0.9.0**, leading with the four
breaking changes carried over from the 0.9.1 waypoint (JSON config move, the
`mqtt_packet_*` rename, bridge traffic in the counters, and the `Startup`/
`HELP` library-API change), then the 0.9.2 OTLP addition.

**v0.9.3**, tagged and released (marked latest), is **another breaking
change** — the config file format switched from JSON to TOML
(`config::Config::apply_toml_str`, replacing `apply_json_str`;
`wispmq.toml` replaces `wispmq.json`; `wispmq.example.toml` replaces
`wispmq.example.json`). At the time, the ACL policy file (`--acl-file`) was
unaffected — still JSON — but see the 0.9.5 waypoint below: it later moved
too.

The **0.9.4 waypoint** (real, in commit history, not yet a tag/Release/image
when it was current) was an Aikido security-fix pass — eight fixes closing
session-takeover paths (epoch validation at the packet-dispatch boundary,
sessions bound to authenticated identity with resubscription re-authorized on
resume, reserved `$bridge/`/`$SYS/` client-id prefixes rejected), a
bearer-token comparison timing leak, and path-traversal rejection (a `..`
path component is refused) on `--acl-file`, `--password-file`, `--cert-file`,
and `--key-file`. No breaking change — a **patch** bump.

The **0.9.5 waypoint** — **another breaking change**: the ACL policy file
(`--acl-file`) switched from JSON to TOML (`acl::Acl::from_toml_str`,
`[[rules]]` array-of-tables replacing the JSON `"rules"` array; `from_value`
itself is unchanged — it already operated on a generic `serde_json::Value`,
the same pivot-through-JSON technique `Config::apply_toml_str` uses), closing
the inconsistency the 0.9.3 note above flagged.

Current status: `Cargo.toml` on `main` has since moved to a **0.9.6
waypoint** — **another breaking change**: `listen_addr` and `ws_listen_addr`
are now **always plain**. TLS on the MQTT port used to be "whichever of
`tls_cert`/`tls_key` happen to be set wraps `listen_addr`"; that coupling is
gone. TLS now lives on two new, independent, optional listener addresses —
`tls_listen_addr` (MQTT) and `ws_tls_listen_addr` (WebSocket) — each
requiring its own cert+key (checked once at startup, alongside
`otlp_protocol`, in the new `Config::validate`) and each able to run at the
same time as its plain sibling on a different address. `tls_client_ca`/
`ws_tls_client_ca` (mutual TLS) now gate the dedicated TLS addresses, not the
plain ones. Driven by a downstream need in the `wispmq-addon` repo: Home
Assistant's Mosquitto add-on shows four *always-present* ports (Normal MQTT,
MQTT over WebSocket, and TLS variants of both) because mosquitto always runs
all four listeners — this waypoint gives WispMQ the same real capability
instead of faking a 4-way choice with a single listener toggling modes.
**Migration**: a deployment that set `tls_cert`/`tls_key` (or the `ws_*`
equivalents) without the new `*_listen_addr` loses TLS on that port after
upgrading — add the matching `tls_listen_addr`/`ws_tls_listen_addr` to keep
it (same address as before is fine; the port just needs naming explicitly
now). `server.rs`'s `run`/`run_ws` (always plain) gained siblings
`run_tls`/`run_ws_tls` (always TLS, no-op if their address is unset), both
built around new private `serve_mqtt`/`serve_ws` helpers so the accept-loop
body isn't duplicated four times. CI (`ci.yml`) and image builds
(`docker.yml`) are green on `main`. `git log` has the detail — this is the
map. See "Done since 0.9.0" below for what else has landed since 0.9.2.

Milestones, in order:
1. Core MQTT **v5.0** broker: codec (all 15 packets + properties/reason codes),
   sessions + routing + QoS 0/1/2, retained messages, wills, SQLite persistence,
   Tokio TCP server. Verified against `mosquitto` v5 clients.
2. **WebSocket** transport (`ws://` + `wss://`), spec §6, via a byte-stream
   adapter reusing the generic connection task.
3. **Dual license** (MIT OR Apache-2.0) + CI workflow.
4. **Admin server** on a separate port: `/health`, Prometheus `/metrics`, and an
   **MCP** (JSON-RPC) server; then a **bearer-token** guard for it.
5. **Native TLS** (rustls) on both the MQTT and admin ports; then **mutual TLS**
   with a client-CA trust store.
6. **Cert-CN identity + per-identity ACLs**; ACL **hot-reload on SIGHUP** that
   revokes live subscriptions and disconnects affected clients (`0x87`).
7. **CLI** (`--help`) with a flag for every env option; then a **config
   file** (precedence: file < env < CLI); then exposed the remaining
   capability knobs (`maximum_qos`, `retain_available`, `topic_alias_maximum`,
   `server_keep_alive`).
8. **Username/password auth** (PBKDF2-HMAC-SHA256, `--password-file`,
   `--allow-anonymous`, `--hash-password`).
9. **Docker** image + `docker-compose.yml` + GHCR publish workflow.
10. **MQTT v3.1 & v3.1.1** support (version-aware codec; v5 stays the default).
11. First stable milestone reached (the 0.9.0 waypoint — see versioning note
    above).
12. **Renamed** `mqtt_server` → **PulseMQ** (`pulsemq`): crate, binary, image,
    GitHub repo (old URLs redirect). GHCR package was renamed with the repo.
13. Docker workflow set to **`provenance: false`** and the GHCR package pruned
    of orphaned untagged versions.
14. **Renamed again**, `pulsemq` → **WispMQ** (`wispmq`) — same reason as
    milestone 12, another name collision. Crate, binary, image, default config
    filename, GitHub repo (`elpapimango/pulsemq` → `elpapimango/wispmq`, old
    URLs redirect), plus the companion `pulsemq-cli` → `wispmq-cli` and
    `pulsemq-addon` → `wispmq-addon` repos. Unlike milestone 12, the old
    `ghcr.io/elpapimango/pulsemq` GHCR package was **not** renamed (GHCR has
    no rename operation) — it's left orphaned/public; delete by hand via the
    web UI if desired.

No known open bugs. Optional follow-ups if wanted: WebSocket server-initiated
SSE is not implemented (`GET /mcp` → 405); `storage::SubRecord::to_topic_filter`
has no callers but is public API, so removing it is a breaking change rather
than a cleanup.

**Expected behaviour that reads like a bug** (don't "fix" these):
- `$SYS` deliveries count as real traffic, so `packets_sent`/`bytes_sent`/
  `mqtt_publish_sent_total` climb steadily whenever something subscribes to
  `$SYS/#` (~55 topics per `sys_interval`). mosquitto behaves the same way. Set
  `sys_interval: 0` if that noise matters.
- `/metrics` carries **two** publish counter families that look duplicative:
  `mqtt_publish_{received,sent}_total` (aggregate application messages) and
  `mqtt_packet_publish_{received,sent}_total` (the PUBLISH entry in the
  per-control-packet array). They are deliberately distinct series; the
  `mqtt_packet_` prefix exists precisely because the per-packet names used to
  collide with the aggregates and produced invalid exposition.
- `Writer::put_utf8`/`put_binary` clamp at 65535 bytes instead of returning an
  error. Unreachable today (every such field arrives u16-length-prefixed), and
  clamping beats a wrapping length prefix, which would desync the peer's framing
  for every subsequent packet.
- A CONNECT with an empty Client Identifier (server-assigned, `connect.rs`)
  never persists to SQLite even with a non-zero requested Session Expiry
  Interval — `session.persistent` is forced `false` whenever the id was
  auto-assigned. In-memory survival across a clean reconnect within the same
  process still works; only surviving a broker *restart* is unavailable.
  Deliberate: a client can't predict what id it'll be assigned, so it's not
  going to reconnect with it after a restart anyway, and persisting rows keyed
  by throwaway `auto-<counter>-<unix time>` ids for every anonymous CONNECT
  would let one open a SQLite row per connection with no bound — the same
  class of unbounded-resource issue `max_queued_messages` and
  `OUTBOUND_CHANNEL_CAPACITY` guard against elsewhere.

**Planned work is in [`TODO.md`](TODO.md)** — pick the top item.

Done since 0.9.0:
- (1) **Forwarding** — broker-to-broker bridge (`bridge.rs`).
- (5A) **No-panic audit** of the untrusted-input path. `tests/malformed.rs`
  proves `Packet::decode` never panics on hostile bytes across all three
  versions; keep it passing when touching the codec.
- (5B) **Security review** — fixed a PBKDF2 username-enumeration timing leak,
  a Will authorization bypass across ACL reload, and the unbounded offline
  queue (`max_queued_messages`). Secrets are wrapped in `config::Secret`.
- (3) **Config file is JSON** — `serde_json`, `wispmq.json`, no `yaml-rust2`.
- (4) **`clap` parses the CLI** — the hand-rolled `apply_args`/`HELP` are gone;
  flags live in a `#[derive(Parser)]` struct in `cli.rs`. Env stays in
  `apply_env` so the precedence layers do not collapse.
- (5C) **Refactor/optimize** — `topic::matches` is allocation-free (102 -> 61
  ns/call); payloads are `Arc<[u8]>` so fan-out no longer copies them (64 KiB to
  100 subscribers: 5.1 ms -> 32 us); `broker/mod.rs` split 1552 lines -> 9 files
  along its existing section banners; routing delivers ordinary subscriptions in
  one pass. **A topic trie was measured and rejected** — matching is ~19 ns per
  subscription, delivery ~430 ns, so a trie optimizes the cheap half.
  Deduplicating the server task and bridge client was declined with reasons in
  `TODO.md`. Benchmarks live in `tests/bench_routing.rs` and `topic::bench_matches`
  (both `#[ignore]`d).
- **Bridge traffic is counted** (post-5C). The bridge wrote to its remote with
  `write_packet` directly and read with `read_packet` directly, so *both*
  directions of its remote link were invisible to `packets_sent`/`packets_received`,
  `bytes_*` and every `mqtt_packet_*` series — an edge broker whose only job is
  forwarding reported almost no traffic. All bridge I/O now goes through
  `bridge::send`/`bridge::record_received`, mirroring `server::send`.
- **Per-packet metric rename** (post-5C): `mqtt_packet_<packet>_*_total`, because
  the old `mqtt_<packet>_*_total` collided with the aggregate publish counters and
  made `/metrics` emit a duplicate name.
- (2) **Metrics** — mosquitto-parity broker status on both surfaces:
  `$SYS/broker/...` retained topics (`sysinfo.rs`, `sys_interval`) and
  Prometheus series, incl. per-control-packet counters. `load/*` moving
  averages were deliberately skipped — use `rate()`.
- (6) **OTLP telemetry export** (`otel.rs`, non-default `otel` feature) — pushes
  metrics and logs over OTLP/HTTP-protobuf; off unless `otlp_endpoint` is set.
  Landed with a refactor that makes the `Snapshot` reuse structural: see
  "Metrics and `$SYS`" below.
- **Home Assistant MQTT Discovery** (`sysinfo.rs`, `ha_discovery`/
  `ha_discovery_prefix`, off by default) — publishes retained HA discovery
  config + state topics for every `Snapshot::series()` statistic, so Home
  Assistant's own MQTT integration auto-creates sensor entities. Rides
  `sys_interval` for its cadence. A companion Supervisor add-on repo,
  [`wispmq-addon`](https://github.com/elpapimango/wispmq-addon), wraps the
  published image for one-click install — no separate Rust build there.
- **Config file switched from JSON to TOML** (0.9.3 waypoint, breaking) —
  `apply_toml_str` replaces `apply_json_str`, parsing with the `toml` crate
  and pivoting through `serde_json::Value` so the validation logic
  (`KNOWN_KEYS`, `j_str`/`j_bool`/`j_i64`/`j_u32`, `bridge::parse_bridges`,
  `otlp_headers`) stayed unchanged underneath it. Picked over JSON for the
  ecosystem fit (Cargo's own format) and over YAML for `serde_yaml` being
  archived/deprecated upstream; unlike the earlier JSON move, this one is not
  a strictness-for-comments trade — TOML keeps the strict unknown-key/wrong-type
  rejection *and* supports `#` comments. `bridges` reads more naturally too,
  as `[[bridges]]`/`[[bridges.topics]]` array-of-tables instead of a JSON
  array of objects.
- **Post-0.9.3 Aikido security-fix pass** (0.9.4 waypoint, item 10, no
  breaking change) — session takeover closed from three angles: sessions are
  now bound to the authenticated identity that created them (re-authorizing
  every subscription on resume), state-changing packet handlers validate the
  session's epoch so a superseded connection's inbound packets can't act on a
  session it no longer owns, and CONNECT/storage-restore reject reserved
  `$bridge/`/`$SYS/` client-id prefixes. `admin::tokens_match` no longer
  short-circuits its iteration count, closing a bearer-token-length timing
  leak. `acl::Acl::load`, `auth::Credentials::load`, and `tls`'s cert/key
  loaders now reject any path containing a `..` component.
- **Optional TOML `[section]` grouping** (0.9.4 waypoint, no breaking change,
  additive) — every config-file option can now be written inside a
  `[section]` table matching the `--help` headings, purely for readability;
  bare keys still work identically and can't be mixed with the same key
  inside a section. See "Configuration" below (`flatten_sections`).
- **ACL file switched from JSON to TOML** (0.9.5 waypoint, breaking) —
  `acl::Acl::from_toml_str` replaces the JSON parse in `Acl::load`;
  `Acl::from_value` (the shared validation, unchanged) still consumes a
  generic `serde_json::Value`, so only the parse step at the top of `load`
  needed to change. `[[rules]]` array-of-tables replaces the JSON `"rules"`
  array. Matches the config file's own JSON→TOML move from 0.9.3.

**Nothing is left on the roadmap** — every numbered item in `TODO.md` is done.
Item 6 shipped as the **v0.9.2** release.

## Commands

```bash
cargo build                                             # debug build
cargo test                                              # all tests (unit + integration)
cargo test --features otel                              # + the OTLP export suite
cargo run -- --help                                     # list every config option
cargo fmt --all -- --check                              # formatting (CI-enforced)
cargo clippy --all-targets --all-features -- -D warnings # lints (CI-enforced)
```

After bumping the crate version, run plain `cargo build` once — `--locked` fails
until `Cargo.lock` records the new version.

CI (`.github/workflows/ci.yml`) runs fmt, clippy `-D warnings`, `build --locked`,
and `test --locked` on push/PR to `main`, then repeats build and test with
`--features otel`. **Keep all four green** — run them
locally before committing. Clippy is strict: two lints are allowed crate-wide in
`src/lib.rs` (`large_enum_variant`, `result_large_err`) because the packet/frame
enums intentionally vary in size; prefer a crate-level `allow` with a comment
over restructuring protocol types.

## Architecture

Layered; each module maps to a spec area. Read a module's top-of-file doc comment
before changing it.

- `codec` — wire primitives (VBI, UTF-8, binary) + property codec (§1.5, §2.2.2)
- `types` — packet types, QoS, ReasonCode, PropertyId enums
- `packet` — encode/decode for all 15 control packets (§3)
- `framing` — async read/write of whole packets over any `AsyncRead`/`AsyncWrite`
- `topic` — topic-name/filter validation + wildcard matching (§4.7/§4.8)
- `message` — the routable application message (queue/retain/persist form)
- `broker` — session registry, routing, QoS state machines, retained store,
  wills, ACL enforcement. One `impl Broker` split across `broker/*.rs` by spec
  area (`routing`, `publish`, `subscribe`, `connect`, `lifecycle`, `authz`,
  `stats`, `bridges`, `session`); `mod.rs` holds the types, the lock and packet
  dispatch, and documents the layout. Slices share imports via `use super::*`
  and mark cross-slice items `pub(super)`.
- `storage` — SQLite persistence actor + startup loader
- `server` — TCP/TLS listeners and the generic per-connection task
- `ws` — WebSocket transport: `mqtt`-subprotocol handshake + byte-stream adapter (§6)
- `tls` — rustls `TlsAcceptor` from PEM cert/key; client-cert CN extraction
- `auth` — username/password credentials (PBKDF2-HMAC-SHA256 via `ring`)
- `acl` — per-identity publish/subscribe authorization (TOML policy)
- `bridge` — broker-to-broker forwarding: outbound MQTT client per remote,
  reconnect+backoff, QoS 0/1/2 both ways, loop prevention via `no_local`
- `metrics` — atomic counters + gauges; one `Snapshot` rendered two ways
  (`to_prometheus`, `to_sys_topics`) so the surfaces cannot disagree
- `sysinfo` — periodic `$SYS/broker/...` publisher (retained, `sys_interval`)
- `otel` — OTLP export of metrics + logs, behind the `otel` Cargo feature. The
  file holds **two** modules with identical signatures (feature on / off), so
  `main.rs` has no `#[cfg]`; the off half is no-ops that still validate config
- `admin` — HTTP server: `/health`, Prometheus `/metrics`, MCP `/mcp`
- `cli` — the `clap` `Cli` struct: one `Option` field per flag, applied last
- `config` — layered configuration (see below)

### Concurrency model (important)

- All shared broker state lives behind **one `std::sync::Mutex`** (`broker::State`).
  **Never `await` while holding it.** Outbound delivery is a non-blocking push
  into each client's unbounded channel, so handlers stay synchronous.
- Each connection is one Tokio task that `tokio::select!`s over the socket read
  half and its outgoing channel. `handle_connection<S>` is **generic** over the
  stream, so TCP, TLS, and WebSocket (`ws::WsStream`) all reuse it — add
  transports by adapting to `AsyncRead + AsyncWrite`, not by duplicating the loop.
- Persistence runs on a dedicated OS thread owning the `rusqlite::Connection`;
  the broker sends it commands over a channel (never blocks the network path).
- The ACL is `RwLock<Arc<Acl>>`, hot-reloaded on SIGHUP.

### Transports (four independent listeners)

Two protocol families (MQTT, WebSocket), each split into an always-plain
address and a dedicated always-TLS address that can run at the same time:
`--listen-addr` (plain) + `--tls-listen-addr` (TLS/mutual TLS, needs
`--tls-cert`/`--tls-key`), and `--ws-listen-addr` (plain) +
`--ws-tls-listen-addr` (TLS/mutual TLS, needs `--ws-tls-cert`/
`--ws-tls-key`). All four addresses are independent — any subset can be
configured, all at once if wanted (see `server::run`/`run_tls`/`run_ws`/
`run_ws_tls`, built on the shared `serve_mqtt`/`serve_ws` accept loops).
Client-cert CN becomes the authenticated identity for ACLs on either TLS
transport.

### Metrics and `$SYS`

Statistics are collected **once** into `metrics::Snapshot` and rendered on
**three** surfaces. Two of them share one list:

- `Snapshot::series()` is the canonical enumeration — `Vec<Series { name, kind,
  help, value }>`. `to_prometheus()` renders it, and `otel.rs` builds one
  observable instrument per entry from it. **Adding a statistic to `series()`
  puts it on both surfaces**; there is no second list to forget.
- `to_sys_topics()` is separate on purpose: `$SYS` uses mosquitto's own topic
  hierarchy, which is a different naming scheme rather than a rendering of
  these names. Add the statistic there too — `tests/sysinfo.rs` asserts the
  `$SYS` and Prometheus values agree.

`Series.name` is the **full Prometheus name**, `_total` included.
`Series::otel_name()` strips that suffix **for counters only**, because
`mqtt_sessions_total` and `mqtt_subscriptions_total` are *gauges* that happen to
end in `_total` — stripping those would rename series that dashboards read.
`mqtt_build_info` is outside `series()` (a constant-1 gauge carrying a label has
no `u64` value); its version reaches OTLP as `service.version` on the resource.

Counters live in `metrics::Metrics` (atomics, incremented on the hot path via
`record_received`/`record_sent`); gauges are computed under the lock in
`Broker::snapshot()`.

The traffic counters (`packets_*`, `bytes_*`, `publish_bytes_*`, `mqtt_packet_*`)
cover **both** client connections and the bridge's link to its remote — the
bridge routes its I/O through `bridge::send`/`bridge::record_received` for exactly
that reason. `tests/bridge.rs::bridge_traffic_is_counted_in_the_packet_metrics`
pins it, using CONNECT-sent and CONNACK-received as the giveaways: a broker never
sends a CONNECT or receives a CONNACK on a client connection, so a non-zero count
can only come from a bridge.

The per-control-packet Prometheus series are `mqtt_packet_<packet>_...`, **not**
`mqtt_<packet>_...`: the latter collided with the aggregate
`mqtt_publish_{received,sent}_total` and made `/metrics` repeat a metric name,
which is invalid exposition. `tests/sysinfo.rs::no_duplicate_metric_names` now
fails on any repeated name or orphaned HELP block — keep it passing when adding a
statistic, because the value-agreement test cannot see a name clash.

`$SYS` invariants, all covered by tests: `#`/`+` never match `$SYS` (§4.7.2, so
ordinary subscribers are unaffected); clients cannot publish under `$SYS`
(refused `0x90`); values are retained **in memory only**, never persisted; and
`$SYS` is excluded from `retained_messages`/`retained_bytes`/`list_retained` so
enabling it does not silently inflate pre-existing gauges.

### Protocol versions

The version is negotiated per-connection in CONNECT and threaded as a
`ProtocolVersion` (`types.rs`) through `Packet::encode/decode`, `framing`, and
the connection task. v3.x has no Properties, uses 1-byte CONNACK return codes,
omits reason codes in (un)subscribe acks, and has no server DISCONNECT — all
handled by branching in the per-packet `encode_body`/`decode`. **When changing
packet codecs, keep all three versions correct** and update the version-aware
integration tests. CONNECT is self-describing (decoded with a placeholder
version); everything after uses the negotiated one.

### Authn / authz pipeline

Identity = authenticated username (`--password-file`, PBKDF2 in `auth`) if
present, else mutual-TLS cert CN, else `anonymous`. That identity drives the
ACL (`--acl-file`), which is `RwLock<Arc<Acl>>` and hot-reloaded on SIGHUP
(revoking live subscriptions + disconnecting affected clients with 0x87).

## Configuration

Precedence, lowest to highest: **defaults < TOML config file < env vars < CLI
flags**. Entry point is `Config::load()`. `wispmq.toml` in the cwd is
auto-loaded; `--config` / `MQTT_CONFIG_FILE` overrides. When adding a new option,
wire it in **all** places: the `Config` struct + `Default`, `apply_env`,
the `cli::Cli` struct (+ its `apply`), `apply_toml_str` (+ `KNOWN_KEYS`), the
README tables, and `wispmq.example.toml`. There are unit tests in `cli` and
`config` covering each layer — extend them.

Two options are **structured** and so config-file-first: `bridges` (an array of
tables, `[[bridges]]`, config-file only) and `otlp_headers` (a table, `[otlp_headers]`;
also `MQTT_OTLP_HEADERS="K=V,K=V"` and repeated `--otlp-header K=V`). A credential
value goes in `config::Secret` and its container gets a hand-written redacting
`Debug` — `Config` derives `Debug` and is one `{cfg:?}` away from a log line.

Every other option can additionally be written inside a `[section]` table
(`network`, `mqtt_tls`, `websockets`, `admin`, `auth`, `storage`, `protocol`,
`otlp`, `home_assistant` — `config::SECTION_NAMES`, mirroring the `--help`
headings) purely for readability: `apply_toml_str` calls `flatten_sections`
first, which folds a recognised section's keys up to the document's top level
before the existing flat `j_str`/`j_bool`/... lookups ever run, so a key
reaches `Config` the same way whether it was written bare or inside its
section. Setting the same key both ways (or in two sections) is a config
error, not a silent pick. `otlp_headers` can nest as `[otlp.otlp_headers]`
since flattening `[otlp]` carries it up unchanged. When adding a new option,
put it in the README's per-section reference too.

An option whose *value* needs validating beyond its type (`otlp_protocol`) is
checked **once**, at startup, rather than per layer: the env layer cannot return
an error, so a per-layer check would silently ignore a bad env value while
rejecting the same value from the config file.

The whole config pipeline runs **before** the `tracing` subscriber exists, so a
`warn!` in `apply_env`/`apply_toml_str` goes nowhere — use `eprintln!`, as `main`
does for config errors.

`apply_toml_str` parses with the `toml` crate into a `toml::Value`, then
pivots through `serde_json::to_value` into a `serde_json::Value` — every
validation/assignment helper (`j_str`/`j_bool`/`j_i64`/`j_u32`,
`bridge::parse_bridges`, the `otlp_headers` walk) still operates on that
shared `Value` tree unchanged, so TOML support did not duplicate the
validation logic. `KNOWN_KEYS` and per-option types are unaffected by the
format.

## Tests

114 tests on default features (120 with `--features otel`), plus five
`#[ignore]`d benchmarks. Unit tests live in-module (`topic`, `acl`, `cli`,
`config`, `auth`, `storage`, `metrics`, `otel`); integration suites are in
`tests/`:

- `tests/interop.rs` — TCP round trips using the crate's own codec, per version.
- `tests/websocket.rs` — `ws://` and `wss://` round trips via `tokio-tungstenite`;
  the wss cert is generated in-test with `rcgen` (dev-dep) so tests are
  self-contained/CI-safe. Also WS + mutual TLS: a client cert's CN drives ACL
  identity (granted vs. denied topics over the same connection pair), and a
  connection presenting no client cert is rejected — note that a TLS 1.3
  client's `connect()` can still return `Ok` in that case (it completes as
  soon as the client sends its own empty Finished, before learning the server
  rejected the handshake), so the rejection assertion has to exercise the
  connection (the WS upgrade), not just check `connect()`'s result.
- `tests/admin_tls.rs` — mutual TLS on the admin HTTP server: a client with a
  valid cert gets served normally (`/health` returns 200), one with no client
  cert is rejected (same TLS 1.3 caveat as above — asserted by attempting an
  actual request, not by checking `connect()`).
- `tests/bridge.rs` — two in-process brokers bridged; delivery both ways,
  reconnect when the remote starts late, and that the bridge's remote traffic
  lands in the shared packet counters.
- `tests/malformed.rs` — **the no-panic guard.** Truncations, byte mutations,
  every type/flag nibble, lying Remaining Lengths, pathological VBIs, all
  decoded under v3.1/v3.1.1/v5. Keep it passing when touching the codec; if a
  change here fails, a remote peer can crash the broker.
- `tests/limits.rs` — the offline-queue bound (`max_queued_messages`), including
  that `0` still means unlimited.
- `tests/shared.rs` — shared subscriptions (`$share/...`): one member per group
  per message, distinct groups and ordinary subscribers each getting their own
  copy, and No Local excluding the publisher from its own group.
- `tests/bench_routing.rs` — **benchmarks, `#[ignore]`d.** Run with
  `cargo test --release --test bench_routing -- --ignored --nocapture
  --test-threads=1` (they contaminate each other in parallel). Together with
  `topic::bench_matches` these are the measurements behind the 5C decisions —
  re-run them before claiming a routing optimization.
- `tests/bench_load.rs` — **benchmarks, `#[ignore]`d.** Run with
  `cargo test --release --test bench_load -- --ignored --nocapture
  --test-threads=1`. Distinct from `bench_routing.rs`: these go over real TCP
  loopback sockets (CONNECT-to-CONNACK latency as concurrent connections
  scale; sustained fan-out publish throughput and end-to-end delivery
  latency), so what's timed includes socket/framing/task overhead, not just
  matching/delivery in isolation. The fan-out case also surfaces real
  backpressure: QoS 0 deliberately drops on a full outbound channel
  (`OUTBOUND_CHANNEL_CAPACITY`) rather than blocking, so a subscriber slower
  than a fire-hosed publisher legitimately receives fewer messages than were
  sent — the harness reports that delivered/sent ratio rather than asserting
  an exact count.
- `tests/otel.rs` — **`--features otel` only.** OTLP export against a fake
  collector (a TCP listener speaking just enough HTTP), asserting on what
  actually went over the socket: the `/v1/metrics` and `/v1/logs` paths, the
  API-key header, and the metric/log names inside the protobuf body. Also that
  export is off by default, that `grpc` is refused, and that an unreachable
  collector does not block the broker.
- `tests/sysinfo.rs` — `$SYS` publishing/retention, `sys_interval: 0`, that `#`
  does not match `$SYS`, that clients cannot publish there, and that the
  Prometheus and `$SYS` renderings of the same `Snapshot` agree.

Pattern for a broker-backed test: bind an ephemeral port with
`TcpListener::bind("127.0.0.1:0")`, read the addr, drop the listener, then hand
the addr to the broker. Use `Storage::null()` to avoid touching a real DB.

To prove a renderer refactor changed nothing, dump its output for a
fully-populated `Snapshot` to the scratchpad before and after and `diff` them —
far cheaper than a golden string in the repo, and it covers every field.

## Environment gotchas

- **zsh does not word-split unquoted variables.** For `mosquitto_pub/sub` flags
  use an array: `CERT=(--cafile ca.pem --cert c.pem --key k.pem -V 5)` then
  `mosquitto_sub "${CERT[@]}" ...`.
- **Do not `pkill -f release/wispmq`** — the pattern matches the shell
  running pkill and kills it (seen as exit 144). Use `killall wispmq`.
- `mosquitto_pub/sub` cannot speak WebSockets — use the Rust WS integration tests.
- Put temporary files in the session scratchpad, not the repo.
- TLS pins the rustls **ring** provider explicitly (`tls.rs`); don't rely on a
  process-default crypto provider.
- `cargo info <crate>` **truncates** the feature list. For the full table read
  `~/.cargo/registry/src/*/<crate>-<version>/Cargo.toml`.

## Docker

Multi-stage `Dockerfile`: the builder is pinned to `$BUILDPLATFORM` and
**cross-compiles** the Rust binary to `$TARGETARCH` (installs
`gcc-aarch64-linux-gnu` + `libc6-dev-arm64-cross`, builds with `--target`), so
the arm64 image isn't built under slow QEMU emulation — only the tiny target-arch
runtime stage (apt/useradd) is emulated. Runtime is `debian-slim`, non-root uid
10001, state on `/data`, HEALTHCHECK on `/health`. `docker-compose.yml` is an
example. `.github/workflows/docker.yml` builds a **multi-arch**
(`linux/amd64,linux/arm64`) image and pushes to `ghcr.io/elpapimango/wispmq`
on pushes to `main` and `v*` tags, with `provenance: false` (no attestation
children; the multi-arch index still has one image manifest per platform, which
is expected). Two CI workflows: `ci.yml` (fmt/clippy/build/test) and `docker.yml`
(image). Verify a multi-arch build locally with `docker buildx build --platform
linux/amd64,linux/arm64 .` (needs `docker buildx` + QEMU binfmt).

## Continuing on another machine

Everything needed is in the repo; a fresh `git clone` + `cargo build` works.
Notes for picking up in a new session/machine:

- **Toolchain**: Rust ≥ 1.87 (uses `is_multiple_of`, `io::Error::other`). A C
  compiler (`cc`) is required — `rusqlite` (bundled SQLite) and `ring` build it.
- **Live testing** needs `mosquitto-clients` (`apt install mosquitto-clients`,
  installed on this machine as of 2026-08-23); `mosquitto_pub/sub` can't do
  WebSockets, so WS/WSS are covered by the Rust integration tests instead.
  The MQTT-spec PDFs in `spec/` need `poppler-utils` to extract text if you
  want to consult them. The published `ghcr.io/elpapimango/pulsemq:0.9.2`
  image (pre-rename name — see milestone 14) was verified end-to-end this
  way: `docker run` it, `mosquitto_pub`/`mosquitto_sub` round-tripped a
  message through it, on top of the `/health`+`/metrics` checks.
- **Not in the repo** (were session-scratch only): the TLS test PKI
  (`ca.pem`, `server.pem/.key`, `client.pem/.key` with CN `test-client`), any
  `passwd` credential file, and `*.db` state — all regenerable. Cert-gen
  commands are in the README (TLS section) and `tests/websocket.rs` generates a
  wss cert in-test via `rcgen`, so the automated suite is self-contained.
- **The GitHub repo is PUBLIC.** It was briefly private on 2026-08-17 and made
  public again the same day, so anonymous clones work and the README badges
  render. Nothing sensitive was ever committed — no cert, key, `*.db` or
  credential file appears anywhere in history (verified before going public; the
  TLS test PKI and `passwd` files have always been session-scratch only). The
  GHCR **package is public** too, but note its visibility is managed
  *separately* from the repo: flipping the repo does not flip the package.
  There is **no REST endpoint for package visibility** — `gh api -X PATCH
  /user/packages/container/<name> --field visibility=...` 404s regardless of
  scopes (verified 2026-08-23 against the pre-rename `pulsemq` package; same
  GHCR API limitation applies to `wispmq`); change it via the web UI instead:
  <https://github.com/users/elpapimango/packages/container/wispmq/settings>
  → Danger Zone → Change visibility. The repo's own visibility *does* have a
  CLI path: `gh repo edit elpapimango/wispmq --visibility private
  --accept-visibility-change-consequences`.
- **GitHub/GHCR**: repo `elpapimango/wispmq`; image
  `ghcr.io/elpapimango/wispmq`. Image pushes happen only via `docker.yml`
  (workflow `GITHUB_TOKEN` has `packages: write`). Managing GHCR packages from
  the CLI needs a `gh` token with `read:packages`/`delete:packages` (the default
  `repo,workflow,...` scopes can't list or delete packages).
- **Verify a checkout**: `cargo fmt --all -- --check && cargo clippy
  --all-targets --all-features -- -D warnings && cargo test` (114 tests), then
  `cargo run -- --help`. The OTLP suite needs its feature:
  `cargo test --features otel` (120).

## Conventions

- Keep the dependency surface small and justified; prefer std + the existing
  crates. A dependency the default deployment does not need goes behind a
  **non-default Cargo feature** (`otel` is the precedent — `cargo tree` must
  show none of it in the default build).
- When adding a config option, wire it through Config + Default, `apply_env`,
  the `cli::Cli` struct (+ its `apply`), `apply_toml_str` (+ `KNOWN_KEYS`),
  README, and `wispmq.example.toml` — with tests. (See the Configuration
  checklist.)
- Comments cite the spec section they implement; match the surrounding density.
- Commit/push only when asked. Branch is `main`. End commit messages with the
  `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` trailer.
- Real `wispmq.toml` and `*.db` files are gitignored (may hold secrets/state);
  the tracked template is `wispmq.example.toml`.
