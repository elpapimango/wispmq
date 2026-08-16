# mqtt_server

[![CI](https://github.com/elpapimango/mqtt_server/actions/workflows/ci.yml/badge.svg)](https://github.com/elpapimango/mqtt_server/actions/workflows/ci.yml)
[![Docker](https://github.com/elpapimango/mqtt_server/actions/workflows/docker.yml/badge.svg)](https://github.com/elpapimango/mqtt_server/actions/workflows/docker.yml)
[![Container image](https://img.shields.io/badge/ghcr.io-mqtt__server-2496ed?logo=docker&logoColor=white)](https://github.com/elpapimango/mqtt_server/pkgs/container/mqtt_server)

An **MQTT broker** written in Rust, built directly from the
[OASIS MQTT Version 5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
(07 March 2019). It also speaks **MQTT v3.1.1** and **v3.1**, negotiated
per-connection (v5 is the primary/default protocol). It uses **Tokio** for
asynchronous networking and **SQLite** (bundled, latest amalgamation, via
`rusqlite`) for durable state.

## Features

Implements the full MQTT v5.0 control-packet set and the core broker behaviour:

- **Protocol versions**: MQTT **v5.0**, **v3.1.1** and **v3.1**, negotiated from
  each client's CONNECT. The codec is version-aware (v3.x has no properties,
  different CONNACK return codes, no reason codes in (un)subscribe acks, no
  server-side DISCONNECT); v5 features degrade gracefully for older clients.
- **All 15 control packets** — CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC,
  PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ,
  PINGRESP, DISCONNECT, AUTH — with full MQTT v5 **Properties** and
  **Reason Codes**.
- **QoS 0, 1 and 2** delivery with the complete acknowledgement state machines
  and message redelivery (DUP) on session resume.
- **Sessions** with Clean Start, Session Expiry Interval, session takeover
  (Reason Code `0x8E`), and offline message queueing for persistent sessions.
- **Topic matching** with `+` / `#` wildcards, `$`-topic exclusion,
  **shared subscriptions** (`$share/{group}/filter`, round-robin), **No Local**,
  **Retain As Published**, **Retain Handling**, and **Subscription Identifiers**.
- **Retained messages**, including zero-length-payload clearing.
- **Will messages** with **Will Delay Interval** and cancellation on reconnect.
- **Topic Aliases** (inbound), **Keep Alive** enforcement, **Receive Maximum**
  flow control, **Maximum Packet Size**, and **Maximum QoS** advertisement.
- **SQLite persistence** of retained messages, persistent sessions and their
  subscriptions, reloaded on startup so state survives restarts.
- **Admin HTTP server** on a separate port with a **health check**, a
  **Prometheus** metrics endpoint, and a **Model Context Protocol (MCP)** server
  exposing read-only broker-introspection tools.
- **Transports**: plain TCP, TLS, mutual TLS, and **WebSockets** (plain and
  over TLS), all sharing the same MQTT session/routing core.

## Architecture

The code is layered so each concern maps to the spec section it implements:

| Module | Responsibility | Spec |
|--------|----------------|------|
| `codec` | Wire primitives (VBI, UTF-8, Binary) and the Property codec | §1.5, §2.2.2 |
| `types` | Packet types, QoS, Reason Codes, Property IDs | §2.1, §2.4 |
| `packet` | Encode/decode for all 15 control packets | §3 |
| `framing` | Async read/write of whole packets over a stream | §2.1.4 |
| `topic` | Topic-name/filter validation and wildcard matching | §4.7, §4.8 |
| `message` | The routable application message (queue/retain/persist) | §4.1 |
| `broker` | Session registry, routing, QoS flows, retained store, wills | §4 |
| `storage` | SQLite persistence actor + startup loader | — |
| `server` | Tokio TCP listener and per-connection task | — |
| `metrics` | Atomic counters + Prometheus/JSON snapshot | — |
| `admin` | HTTP server: health, Prometheus metrics, MCP | — |
| `tls` | rustls `TlsAcceptor` from PEM cert/key + client-cert CN | — |
| `ws` | WebSocket transport: `mqtt`-subprotocol handshake + byte-stream adapter | §6 |
| `acl` | Per-identity publish/subscribe authorization | — |

**Concurrency.** All shared state lives behind a single `std::sync::Mutex`.
Handlers never `await` while holding it — outbound delivery is a non-blocking
push into each client's unbounded channel — so the lock is never held across a
suspension point. Each connection is one Tokio task that `select!`s between the
socket read half and its outgoing channel. Persistence runs on a dedicated
thread that owns the SQLite connection, keeping disk latency off the network
path.

## Build & run

```bash
cargo build --release
./target/release/mqtt_server
```

The broker listens for MQTT on `0.0.0.0:1883` by default, serves the admin
HTTP endpoints on `127.0.0.1:9001`, and writes state to `mqtt_broker.db` in the
working directory.

## Docker

A published image is available from the GitHub Container Registry:

```bash
docker run -d --name mqtt \
  -p 1883:1883 -p 9001:9001 \
  -v mqtt-data:/data \
  ghcr.io/elpapimango/mqtt_server:latest
```

Or with Compose (see [`docker-compose.yml`](docker-compose.yml)):

```bash
docker compose up -d
```

The image runs as a non-root user, stores its SQLite state in the `/data`
volume, exposes `1883` (MQTT), `8883` (TLS), `8080` (WebSockets) and `9001`
(admin), and has a `HEALTHCHECK` against `/health`. Configure it with the same
environment variables described below (mount certs / password / ACL / YAML
files into the container, e.g. under `/config`). Build locally with
`docker build -t mqtt_server .`.

## Configuration

Every setting can be provided three ways — a **YAML config file**, an
**environment variable**, or a **command-line flag** — layered in this order of
increasing precedence:

```
defaults  <  config file  <  environment variables  <  command-line flags
```

The config file is discovered automatically: if `mqtt_server.yaml` (or
`mqtt_server.yml`) exists in the working directory it is loaded. A different
path can be given with `--config <FILE>` or `MQTT_CONFIG_FILE`; an explicitly
named file that is missing or invalid is a startup error. Unknown keys and
wrong value types are rejected.

YAML keys are the option names with underscores (e.g. `listen_addr`). Example
[`mqtt_server.example.yaml`](mqtt_server.example.yaml):

```yaml
listen_addr: "0.0.0.0:1883"
tls_cert: "certs/server.pem"
tls_key: "certs/server.key"
tls_client_ca: "certs/ca.pem"        # enables mutual TLS

ws_listen_addr: "0.0.0.0:8080"       # MQTT over WebSockets
# ws_tls_cert / ws_tls_key           # ... over TLS (wss)

admin_addr: "127.0.0.1:9001"
admin_token: "change-me"             # bearer token for /metrics and /mcp
acl_path: "acl.json"

db_path: "mqtt_broker.db"
max_packet_size: 1048576
receive_maximum: 64
max_session_expiry: 3600

# advertised in CONNACK
maximum_qos: 2                       # 0, 1, or 2
retain_available: true
topic_alias_maximum: 16
# server_keep_alive: 60              # override the client's Keep Alive
```

## Command-line options

Every setting is also available as a command-line flag and an environment
variable. Run `--help` for the full list:

```bash
./target/release/mqtt_server --help
```

```
CONFIG FILE:
    --config <FILE>               Load a YAML config file [MQTT_CONFIG_FILE]
NETWORK:
    --listen-addr <ADDR>          MQTT listener bind address [MQTT_LISTEN_ADDR]
    --admin-addr <ADDR>           Admin/metrics/MCP HTTP bind address [MQTT_ADMIN_ADDR]
MQTT TLS:
    --tls-cert <FILE>             PEM certificate chain for the MQTT port [MQTT_TLS_CERT]
    --tls-key <FILE>              PEM private key for the MQTT port [MQTT_TLS_KEY]
    --tls-client-ca <FILE>        PEM CA bundle; enables mutual TLS [MQTT_TLS_CLIENT_CA]
MQTT OVER WEBSOCKETS:
    --ws-listen-addr <ADDR>       Enable the WebSocket listener [MQTT_WS_LISTEN_ADDR]
    --ws-tls-cert <FILE>          PEM certificate chain for the WS port [MQTT_WS_TLS_CERT]
    --ws-tls-key <FILE>           PEM private key for the WS port [MQTT_WS_TLS_KEY]
    --ws-tls-client-ca <FILE>     PEM CA bundle; enables mutual TLS [MQTT_WS_TLS_CLIENT_CA]
ADMIN TLS & AUTH:
    --admin-tls-cert <FILE>       PEM certificate chain for the admin port [MQTT_ADMIN_TLS_CERT]
    --admin-tls-key <FILE>        PEM private key for the admin port [MQTT_ADMIN_TLS_KEY]
    --admin-tls-client-ca <FILE>  PEM CA bundle; enables mutual TLS [MQTT_ADMIN_TLS_CLIENT_CA]
    --admin-token <TOKEN>         Bearer token for /metrics and /mcp [MQTT_ADMIN_TOKEN]
AUTHENTICATION & AUTHORIZATION:
    --password-file <FILE>        Username/password credentials [MQTT_PASSWORD_FILE]
    --allow-anonymous <BOOL>      Allow credential-less clients [MQTT_ALLOW_ANONYMOUS]
    --acl-file <FILE>             JSON ACL policy per identity [MQTT_ACL_FILE]
    --hash-password [USERNAME]    Print a credential line (password from stdin) and exit
STORAGE & LIMITS:
    --db-path <FILE>              SQLite database file [MQTT_DB_PATH]
    --max-packet-size <BYTES>     Maximum accepted packet size [MQTT_MAX_PACKET_SIZE]
    --receive-maximum <N>         Server Receive Maximum [MQTT_RECEIVE_MAXIMUM]
    --max-session-expiry <SECS>   Cap on Session Expiry Interval [MQTT_MAX_SESSION_EXPIRY]
PROTOCOL CAPABILITIES:
    --maximum-qos <0|1|2>         Highest QoS supported [MQTT_MAXIMUM_QOS]
    --retain-available <BOOL>     Retained messages supported [MQTT_RETAIN_AVAILABLE]
    --topic-alias-maximum <N>     Topic Alias Maximum [MQTT_TOPIC_ALIAS_MAXIMUM]
    --server-keep-alive <SECS>    Override client Keep Alive [MQTT_SERVER_KEEP_ALIVE]
OTHER:
    -h, --help                    Print help and exit
    -V, --version                 Print version and exit
```

Flags accept either `--flag value` or `--flag=value`.

## Admin, metrics & MCP (separate HTTP port)

A lightweight HTTP/1.1 server runs on `MQTT_ADMIN_ADDR` (default
`127.0.0.1:9001`), independent of the MQTT listener:

| Route | Method | Auth | Purpose |
|-------|--------|------|---------|
| `/health` | GET | open | Liveness probe → `{"status":"ok"}` |
| `/metrics` | GET | guarded | Prometheus text exposition of broker metrics |
| `/mcp` | POST | guarded | MCP server (JSON-RPC 2.0 over Streamable HTTP) |

### Authentication

Set `MQTT_ADMIN_TOKEN` to require a bearer token on the guarded endpoints
(`/metrics` and `/mcp`). `/health` and CORS preflight stay open so liveness
probes work without credentials. When the variable is unset the endpoints are
unauthenticated and the broker logs a warning at startup.

```bash
MQTT_ADMIN_TOKEN=s3cr3t ./target/release/mqtt_server
# then:
curl -H 'Authorization: Bearer s3cr3t' http://127.0.0.1:9001/metrics
```

Requests without a valid token receive `401 Unauthorized` with a
`WWW-Authenticate: Bearer` challenge. The token is compared in constant time.
Since the token travels in plaintext, terminate TLS in front of the admin port
(or keep it bound to loopback) when using it across a network.

### Health

```bash
curl http://127.0.0.1:9001/health
```

### Prometheus

Point a scraper at `http://127.0.0.1:9001/metrics`. Exposed series:

- Counters: `mqtt_connections_total`, `mqtt_packets_received_total`,
  `mqtt_packets_sent_total`, `mqtt_bytes_received_total`,
  `mqtt_bytes_sent_total`, `mqtt_publish_received_total`,
  `mqtt_publish_delivered_total`.
- Gauges: `mqtt_clients_connected`, `mqtt_sessions_total`,
  `mqtt_retained_messages`, `mqtt_subscriptions_total`.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: mqtt-broker
    static_configs:
      - targets: ['127.0.0.1:9001']
```

### MCP server

`POST /mcp` speaks the Model Context Protocol as a JSON-RPC 2.0 endpoint
(request/response mode of the Streamable-HTTP transport). It implements
`initialize`, `ping`, `tools/list` and `tools/call`, and provides three
read-only tools for inspecting the running broker:

| Tool | Returns |
|------|---------|
| `broker_stats` | All counters and gauges |
| `list_clients` | Every session: online status, subscription/inflight/queued counts, expiry |
| `list_retained` | Retained topics with payload size and QoS |

```bash
# List available tools
curl -s -X POST http://127.0.0.1:9001/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# Call a tool
curl -s -X POST http://127.0.0.1:9001/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"broker_stats","arguments":{}}}'
```

To use it from an MCP client that supports HTTP transport, point it at
`http://127.0.0.1:9001/mcp`. (Server-initiated SSE streaming is not
implemented; a `GET /mcp` returns 405.)

### Configuration (environment variables)

| Variable | Default | Meaning |
|----------|---------|---------|
| `MQTT_CONFIG_FILE` | _(auto)_ | YAML config file path (else `mqtt_server.yaml` in cwd) |
| `MQTT_LISTEN_ADDR` | `0.0.0.0:1883` | MQTT listener bind address |
| `MQTT_TLS_CERT` / `MQTT_TLS_KEY` | _(unset)_ | PEM cert + key; both set = TLS on the MQTT port |
| `MQTT_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the MQTT port |
| `MQTT_WS_LISTEN_ADDR` | _(unset)_ | Enables the MQTT-over-WebSocket listener |
| `MQTT_WS_TLS_CERT` / `MQTT_WS_TLS_KEY` | _(unset)_ | PEM cert + key; both set = wss on the WS port |
| `MQTT_WS_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the WS port |
| `MQTT_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the MQTT port |
| `MQTT_ADMIN_ADDR` | `127.0.0.1:9001` | Admin/metrics/MCP HTTP bind address |
| `MQTT_ADMIN_TLS_CERT` / `MQTT_ADMIN_TLS_KEY` | _(unset)_ | PEM cert + key; both set = HTTPS on the admin port |
| `MQTT_ADMIN_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the admin port |
| `MQTT_ADMIN_TOKEN` | _(unset)_ | Bearer token for `/metrics` and `/mcp`; unset = open |
| `MQTT_PASSWORD_FILE` | _(unset)_ | Username/password credential file; set = auth required |
| `MQTT_ALLOW_ANONYMOUS` | `false` | Allow credential-less clients when a password file is set |
| `MQTT_ACL_FILE` | _(unset)_ | JSON ACL policy per identity; unset = allow all |
| `MQTT_DB_PATH` | `mqtt_broker.db` | SQLite database file |
| `MQTT_MAX_PACKET_SIZE` | `1048576` | Max accepted packet size (bytes) |
| `MQTT_RECEIVE_MAXIMUM` | `64` | Server Receive Maximum |
| `MQTT_MAX_SESSION_EXPIRY` | `3600` | Cap on Session Expiry Interval (s) |
| `MQTT_MAXIMUM_QOS` | `2` | Highest QoS the server supports (0/1/2) |
| `MQTT_RETAIN_AVAILABLE` | `true` | Whether retained messages are supported |
| `MQTT_TOPIC_ALIAS_MAXIMUM` | `16` | Topic Alias Maximum granted to clients |
| `MQTT_SERVER_KEEP_ALIVE` | _(unset)_ | Override the client's Keep Alive (s) |
| `RUST_LOG` | `info` | Log level (`tracing` filter) |

### Transports

The broker accepts MQTT over several transports, all feeding the same session
and routing core:

| Mode | How to enable |
|------|---------------|
| Plain MQTT (TCP) | default (`--listen-addr`) |
| MQTT over TLS | `--listen-addr` + `--tls-cert` + `--tls-key` |
| MQTT over TLS with client certificate (mTLS) | add `--tls-client-ca` |
| MQTT over WebSockets | `--ws-listen-addr` |
| MQTT over WebSockets with TLS (wss) | `--ws-listen-addr` + `--ws-tls-cert` + `--ws-tls-key` (add `--ws-tls-client-ca` for mTLS) |

The raw-MQTT port and the WebSocket port are independent and can run at the same
time. WebSocket connections carry MQTT Control Packets in binary frames and
negotiate the `mqtt` subprotocol (spec §6); packet boundaries need not align
with frame boundaries. TLS termination and client-certificate identity (CN)
work identically on both transports. Each transport accepts MQTT **v3.1, v3.1.1
and v5** clients interchangeably (the version is negotiated per connection).

```bash
# Plain MQTT on 1883 and MQTT-over-WebSockets on 8080, together:
./target/release/mqtt_server --listen-addr 0.0.0.0:1883 --ws-listen-addr 0.0.0.0:8080

# WebSockets over TLS (wss):
./target/release/mqtt_server \
  --ws-listen-addr 0.0.0.0:8443 --ws-tls-cert cert.pem --ws-tls-key key.pem
```

### TLS

Both MQTT transports (and the admin server) support native TLS (rustls; no
OpenSSL needed at runtime). TLS is enabled per-listener by pointing at a PEM
certificate chain and private key — the listeners are independent and may use
the same or different certificates.

```bash
# self-signed cert for local testing
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 365 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

MQTT_LISTEN_ADDR=0.0.0.0:8883 \
MQTT_TLS_CERT=cert.pem  MQTT_TLS_KEY=key.pem \
MQTT_ADMIN_ADDR=0.0.0.0:9443 \
MQTT_ADMIN_TLS_CERT=cert.pem  MQTT_ADMIN_TLS_KEY=key.pem \
./target/release/mqtt_server
```

```bash
# MQTT over TLS
mosquitto_pub -h localhost -p 8883 --cafile cert.pem -V 5 -t 'secure/x' -m 'hi'
# HTTPS admin
curl --cacert cert.pem https://localhost:9443/health
```

When a port's cert/key pair is unset it serves plaintext as before. Setting only
one of the pair is a startup error. RSA and PKCS#8/SEC1 EC keys are supported;
TLS 1.2 and 1.3 are enabled.

#### Mutual TLS (client certificates)

Point `--tls-client-ca` (MQTT) or `--admin-tls-client-ca` (admin) at a PEM CA
bundle to require **client** certificates: a connecting client must present a
certificate that chains to a CA in that store, or the TLS handshake is refused.

```bash
# PKI: one CA signs the server cert and each client cert
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 365 -subj "/CN=My-CA"
# ... sign server.pem (with SANs) and client.pem against ca.pem ...

./target/release/mqtt_server \
  --listen-addr 0.0.0.0:8883 \
  --tls-cert server.pem --tls-key server.key --tls-client-ca ca.pem \
  --admin-addr 0.0.0.0:9443 \
  --admin-tls-cert server.pem --admin-tls-key server.key --admin-tls-client-ca ca.pem \
  --admin-token secret
```

```bash
# client must present its certificate
mosquitto_pub -h localhost -p 8883 --cafile ca.pem --cert client.pem --key client.key \
  -V 5 -t 'secure/x' -m 'hi'
curl --cacert ca.pem --cert client.pem --key client.key \
  -H 'Authorization: Bearer secret' https://localhost:9443/metrics
```

Setting a client CA without also enabling the port's server certificate is a
startup error. The verified certificate's Common Name becomes the client's
authenticated **identity**, which the ACL (below) authorizes.

## Authentication & authorization

### Username / password

Point `--password-file` / `MQTT_PASSWORD_FILE` at a credential file to require
username/password authentication on CONNECT. Passwords are stored as
**PBKDF2-HMAC-SHA256** with a per-user random salt; verification is
constant-time. Generate an entry (password read from stdin):

```bash
mqtt_server --hash-password alice >> passwd     # prompts for the password
```

Each line is `username:pbkdf2_sha256$iterations$salt$hash`. When a password file
is configured, clients must present valid credentials — a bad username/password
gets CONNACK `0x86`, and a client with no credentials gets `0x87` unless
`--allow-anonymous` / `MQTT_ALLOW_ANONYMOUS=true` is set (in which case
credential-less clients connect as `anonymous`, but any client that *does* send
a username must still authenticate). The authenticated username becomes the
identity used for ACLs (below), overriding any client-certificate CN.

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -V 5 -u alice -P s3cr3t -t t -m hi
```

### Identity & ACLs

The connection's **identity** is the authenticated username if password auth is
used, otherwise the **Common Name (CN)** of the mutual-TLS client certificate,
otherwise `anonymous`. It is logged on connect and drives per-client access
control.

Point `--acl-file` / `MQTT_ACL_FILE` at a JSON policy to authorize publish and
subscribe per identity. Without an ACL file every client may publish/subscribe
anywhere (a warning is logged at startup).

```json
{
  "default": "deny",
  "rules": [
    { "identity": "sensor-01", "publish": ["sensors/01/#"], "subscribe": ["cmd/01/#"] },
    { "identity": "gateway",   "publish": ["#"],            "subscribe": ["#"] },
    { "identity": "*",         "subscribe": ["public/#"] }
  ]
}
```

- `identity` — matches the certificate CN exactly, or `*` for any identity.
- `publish` / `subscribe` — MQTT topic filters (wildcards allowed) the identity
  is permitted to publish to / subscribe to. Multiple matching rules union.
- `default` — action when no rule grants the operation: `deny` (default) or
  `allow`.

Enforcement:

- **PUBLISH** to an unauthorized topic: QoS 0 is dropped silently; QoS 1 → PUBACK
  and QoS 2 → PUBREC with Reason Code `0x87` (Not authorized). The message is
  neither routed nor retained.
- **SUBSCRIBE** to an unauthorized filter: that filter's SUBACK entry is `0x87`
  and the subscription is not created (other filters in the same packet are
  handled independently).
- **CONNECT** with a Will whose topic is not publish-authorized: rejected with
  CONNACK `0x87`.

Subscribe coverage is checked by matching the requested filter against each
allowed pattern, so a request broader than any grant is denied (fail-closed).

### Reloading the ACL (SIGHUP)

Send `SIGHUP` to reload the ACL file without restarting the broker; the new
policy is swapped in atomically and applies to subsequent PUBLISH/SUBSCRIBE
operations. Any **live subscription the new policy no longer authorizes is
revoked** — removed from the session (and persistence) so the broker stops
delivering to it — and any **online client that had a subscription revoked is
disconnected** with a `DISCONNECT` carrying Reason Code `0x87` (Not authorized),
forcing it to reconnect and re-subscribe under the new policy. Its Will is not
published for this administrative close. If the file is missing or invalid, the
error is logged and the **previous policy is kept**.

```bash
# edit acl.json, then:
kill -HUP "$(pgrep -f target/release/mqtt_server)"
```

Reloading is a no-op (logged) when no `--acl-file` was configured.

### Try it with mosquitto clients

```bash
# terminal 1 — subscribe to a wildcard at QoS 1
mosquitto_sub -h 127.0.0.1 -p 1883 -V 5 -t 'sensors/#' -q 1 -v

# terminal 2 — publish
mosquitto_pub -h 127.0.0.1 -p 1883 -V 5 -t 'sensors/temp' -q 1 -m '21.5C'
```

## Tests

```bash
cargo test
```

Covers topic-matching unit tests and end-to-end integration tests that drive
the running broker over a real socket (QoS 1 routing, retained delivery).
Interoperability has also been verified against the `mosquitto_pub` /
`mosquitto_sub` v5 clients for QoS 0/1/2, wildcards, retained messages,
persistent sessions with offline queueing, shared subscriptions, and state
survival across a broker restart.

## Persistence schema

```
retained_messages(topic PK, payload, qos, payload_format, content_type,
                  response_topic, correlation_data, user_properties, expires_at)
sessions(client_id PK, session_expiry)
subscriptions(client_id, filter, qos, no_local, retain_as_published,
              retain_handling, sub_id, PRIMARY KEY(client_id, filter),
              FOREIGN KEY(client_id) -> sessions ON DELETE CASCADE)
```

Only durable sessions (a named client with a non-zero Session Expiry Interval)
are written to disk; clean sessions live purely in memory.

## Limitations

Intentionally out of scope for this implementation:
extended AUTH (SCRAM/challenge-response — AUTH is accepted as a no-op),
username/password authentication (identity comes from the mutual-TLS client
certificate CN), `$SYS` topics, and outbound topic-alias assignment (the server
always sends full topic names).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
