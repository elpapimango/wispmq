# CLAUDE.md

Guidance for working in this repository.

## Project

**PulseMQ** — an **MQTT broker** in Rust (protocols **v5.0**, **v3.1.1**,
**v3.1**), built from the OASIS specs (in `spec/`). Async networking via
**Tokio**; durable state in **SQLite** (bundled via `rusqlite`). Single binary
`pulsemq` (crate `pulsemq`) plus a library crate. Transports: TCP, TLS, mutual
TLS, WebSockets, WebSockets-over-TLS. Repo:
https://github.com/elpapimango/pulsemq

Display name is **PulseMQ**; the identifier everywhere (crate, binary, image,
repo, default `pulsemq.json`) is `pulsemq`. The `MQTT_*` env vars and `mqtt_*`
metric names are intentionally kept — they describe the protocol, not the
project.

## Project history & status

Current status: **v1.0.0 released** (tag `v1.0.0`, GitHub Release, and
`ghcr.io/elpapimango/pulsemq:1.0.0`). Everything below is done and on `main`;
CI (`ci.yml`) and image builds (`docker.yml`) are green. `git log` has the
detail — this is the map.

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
11. **v1.0.0** released.
12. **Renamed** `mqtt_server` → **PulseMQ** (`pulsemq`): crate, binary, image,
    GitHub repo (old URLs redirect). GHCR package was renamed with the repo.
13. Docker workflow set to **`provenance: false`** and the GHCR package pruned
    of orphaned untagged versions.

No known open bugs. Optional follow-ups if wanted: the `1.0.0` image predates
`provenance: false` so its index still has two attestation child manifests
(re-tag/rebuild to make it single-manifest); mutual-TLS on the *admin* port and
WS+mTLS work but aren't covered by automated tests; WebSocket server-initiated
SSE is not implemented (`GET /mcp` → 405).

**Planned work is in [`TODO.md`](TODO.md)** — pick the top item.

Done since 1.0.0:
- (1) **Forwarding** — broker-to-broker bridge (`bridge.rs`).
- (5A) **No-panic audit** of the untrusted-input path. `tests/malformed.rs`
  proves `Packet::decode` never panics on hostile bytes across all three
  versions; keep it passing when touching the codec.
- (5B) **Security review** — fixed a PBKDF2 username-enumeration timing leak,
  a Will authorization bypass across ACL reload, and the unbounded offline
  queue (`max_queued_messages`). Secrets are wrapped in `config::Secret`.
- (3) **Config file is JSON** — `serde_json`, `pulsemq.json`, no `yaml-rust2`.
- (2) **Metrics** — mosquitto-parity broker status on both surfaces:
  `$SYS/broker/...` retained topics (`sysinfo.rs`, `sys_interval`) and
  Prometheus series, incl. per-control-packet counters. `load/*` moving
  averages were deliberately skipped — use `rate()`.

Remaining, in order:

4. **`clap` for CLI parsing** (replaces the hand-rolled `apply_args` + `HELP`).
5C. **Refactor/optimize** — split `broker/mod.rs` (1.3k lines); `topic::matches`
   allocates two `Vec`s per call on the routing hot path. Measure before adding
   a topic trie. `config.rs` shrank with the JSON move, so it is less urgent.
6. **Telemetry/log export** to Datadog/Splunk/OTLP — OTLP first, feature-gated.
   Reuse the existing `Snapshot` rather than building a parallel counter set.

## Commands

```bash
cargo build                                             # debug build
cargo test                                              # all tests (unit + integration)
cargo run -- --help                                     # list every config option
cargo fmt --all -- --check                              # formatting (CI-enforced)
cargo clippy --all-targets --all-features -- -D warnings # lints (CI-enforced)
```

CI (`.github/workflows/ci.yml`) runs fmt, clippy `-D warnings`, `build --locked`,
and `test --locked` on push/PR to `main`. **Keep all four green** — run them
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
- `broker` — session registry, routing, QoS state machines, retained store, wills, ACL enforcement
- `storage` — SQLite persistence actor + startup loader
- `server` — TCP/TLS listeners and the generic per-connection task
- `ws` — WebSocket transport: `mqtt`-subprotocol handshake + byte-stream adapter (§6)
- `tls` — rustls `TlsAcceptor` from PEM cert/key; client-cert CN extraction
- `auth` — username/password credentials (PBKDF2-HMAC-SHA256 via `ring`)
- `acl` — per-identity publish/subscribe authorization (JSON policy)
- `bridge` — broker-to-broker forwarding: outbound MQTT client per remote,
  reconnect+backoff, QoS 0/1/2 both ways, loop prevention via `no_local`
- `metrics` — atomic counters + gauges; one `Snapshot` rendered two ways
  (`to_prometheus`, `to_sys_topics`) so the surfaces cannot disagree
- `sysinfo` — periodic `$SYS/broker/...` publisher (retained, `sys_interval`)
- `admin` — HTTP server: `/health`, Prometheus `/metrics`, MCP `/mcp`
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

### Transports (five modes)

Plain TCP, TLS, mutual TLS (all on `--listen-addr`), plus WebSockets and
WebSockets-over-TLS (on `--ws-listen-addr`). Both ports can run at once and share
the session/routing core. Client-cert CN becomes the authenticated identity for
ACLs on any TLS transport.

### Metrics and `$SYS`

Statistics are collected **once** into `metrics::Snapshot` and rendered
**twice**: `to_prometheus()` for `/metrics` and `to_sys_topics()` for the
`$SYS/broker/...` MQTT topics published by `sysinfo::run`. When adding a
statistic, add it to `Snapshot` and to *both* renderers — `tests/sysinfo.rs`
asserts the two agree. Counters live in `metrics::Metrics` (atomics, incremented
on the hot path via `record_received`/`record_sent`); gauges are computed under
the lock in `Broker::snapshot()`.

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

Precedence, lowest to highest: **defaults < JSON config file < env vars < CLI
flags**. Entry point is `Config::load()`. `pulsemq.json` in the cwd is
auto-loaded; `--config` / `MQTT_CONFIG_FILE` overrides. When adding a new option,
wire it in **all** places: the `Config` struct + `Default`, `apply_env`,
`apply_args` (+ `HELP` text), `apply_json_str` (+ `KNOWN_KEYS`), the README
tables, and `pulsemq.example.json`. There are unit tests in `config` covering
each layer — extend them.

## Tests

- Unit tests live in-module (`topic`, `acl`, `config`).
- `tests/interop.rs` — TCP round trips using the crate's own codec.
- `tests/websocket.rs` — `ws://` and `wss://` round trips via `tokio-tungstenite`;
  the wss cert is generated in-test with `rcgen` (dev-dep) so tests are
  self-contained/CI-safe. Bind an ephemeral port with `TcpListener::bind(":0")`,
  read the addr, drop, then hand it to the broker.

## Environment gotchas

- **zsh does not word-split unquoted variables.** For `mosquitto_pub/sub` flags
  use an array: `CERT=(--cafile ca.pem --cert c.pem --key k.pem -V 5)` then
  `mosquitto_sub "${CERT[@]}" ...`.
- **Do not `pkill -f release/pulsemq`** — the pattern matches the shell
  running pkill and kills it (seen as exit 144). Use `killall pulsemq`.
- `mosquitto_pub/sub` cannot speak WebSockets — use the Rust WS integration tests.
- Put temporary files in the session scratchpad, not the repo.
- TLS pins the rustls **ring** provider explicitly (`tls.rs`); don't rely on a
  process-default crypto provider.

## Docker

Multi-stage `Dockerfile`: the builder is pinned to `$BUILDPLATFORM` and
**cross-compiles** the Rust binary to `$TARGETARCH` (installs
`gcc-aarch64-linux-gnu` + `libc6-dev-arm64-cross`, builds with `--target`), so
the arm64 image isn't built under slow QEMU emulation — only the tiny target-arch
runtime stage (apt/useradd) is emulated. Runtime is `debian-slim`, non-root uid
10001, state on `/data`, HEALTHCHECK on `/health`. `docker-compose.yml` is an
example. `.github/workflows/docker.yml` builds a **multi-arch**
(`linux/amd64,linux/arm64`) image and pushes to `ghcr.io/elpapimango/pulsemq`
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
- **Live testing** needs `mosquitto-clients` (`apt install mosquitto-clients`);
  `mosquitto_pub/sub` can't do WebSockets, so WS/WSS are covered by the Rust
  integration tests instead. The MQTT-spec PDFs in `spec/` need `poppler-utils`
  to extract text if you want to consult them.
- **Not in the repo** (were session-scratch only): the TLS test PKI
  (`ca.pem`, `server.pem/.key`, `client.pem/.key` with CN `test-client`), any
  `passwd` credential file, and `*.db` state — all regenerable. Cert-gen
  commands are in the README (TLS section) and `tests/websocket.rs` generates a
  wss cert in-test via `rcgen`, so the automated suite is self-contained.
- **GitHub/GHCR**: repo `elpapimango/pulsemq`; image
  `ghcr.io/elpapimango/pulsemq`. Image pushes happen only via `docker.yml`
  (workflow `GITHUB_TOKEN` has `packages: write`). Managing GHCR packages from
  the CLI needs a `gh` token with `read:packages`/`delete:packages` (the default
  `repo,workflow,...` scopes can't list or delete packages).
- **Verify a checkout**: `cargo fmt --all -- --check && cargo clippy
  --all-targets --all-features -- -D warnings && cargo test` (25 tests), then
  `cargo run -- --help`.

## Conventions

- Keep the dependency surface small and justified; prefer std + the existing crates.
- When adding a config option, wire it through Config + Default, `apply_env`,
  `apply_args` (+ HELP), `apply_json_str` (+ `KNOWN_KEYS`), README, and
  `pulsemq.example.json` — with tests. (See the Configuration checklist.)
- Comments cite the spec section they implement; match the surrounding density.
- Commit/push only when asked. Branch is `main`. End commit messages with the
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer.
- Real `pulsemq.json` and `*.db` files are gitignored (may hold secrets/state);
  the tracked template is `pulsemq.example.json`.
