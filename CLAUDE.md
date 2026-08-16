# CLAUDE.md

Guidance for working in this repository.

## Project

An **MQTT v5.0 broker** in Rust, built from the OASIS spec (in `spec/`). Async
networking via **Tokio**; durable state in **SQLite** (bundled via `rusqlite`).
Single binary `mqtt_server` plus a library crate. Repo:
https://github.com/elpapimango/mqtt_server

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
- `acl` — per-identity publish/subscribe authorization (JSON policy)
- `metrics` — atomic counters + Prometheus/JSON snapshot
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

## Configuration

Precedence, lowest to highest: **defaults < YAML config file < env vars < CLI
flags**. Entry point is `Config::load()`. `mqtt_server.yaml`/`.yml` in the cwd is
auto-loaded; `--config` / `MQTT_CONFIG_FILE` overrides. When adding a new option,
wire it in **all** places: the `Config` struct + `Default`, `apply_env`,
`apply_args` (+ `HELP` text), `apply_yaml_str` (+ `KNOWN_YAML_KEYS`), the README
tables, and `mqtt_server.example.yaml`. There are unit tests in `config` covering
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
- **Do not `pkill -f release/mqtt_server`** — the pattern matches the shell
  running pkill and kills it (seen as exit 144). Use `killall mqtt_server`.
- `mosquitto_pub/sub` cannot speak WebSockets — use the Rust WS integration tests.
- Put temporary files in the session scratchpad, not the repo.
- TLS pins the rustls **ring** provider explicitly (`tls.rs`); don't rely on a
  process-default crypto provider.

## Conventions

- Keep the dependency surface small and justified; prefer std + the existing crates.
- Comments cite the spec section they implement; match the surrounding density.
- Commit/push only when asked. Branch is `main`. End commit messages with the
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer.
- Real `mqtt_server.yaml` and `*.db` files are gitignored (may hold secrets/state);
  the tracked template is `mqtt_server.example.yaml`.
