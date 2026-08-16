# mqtt_server

An **MQTT v5.0 broker** written in Rust, built directly from the
[OASIS MQTT Version 5.0 specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
(07 March 2019). It uses **Tokio** for asynchronous networking and **SQLite**
(bundled, latest amalgamation, via `rusqlite`) for durable state.

## Features

Implements the full MQTT v5.0 control-packet set and the core broker behaviour:

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

## Command-line options

Every setting is available as both a command-line flag and an environment
variable; **flags take precedence over the environment**. Run `--help` for the
full list:

```bash
./target/release/mqtt_server --help
```

```
NETWORK:
    --listen-addr <ADDR>          MQTT listener bind address [MQTT_LISTEN_ADDR]
    --admin-addr <ADDR>           Admin/metrics/MCP HTTP bind address [MQTT_ADMIN_ADDR]
MQTT TLS:
    --tls-cert <FILE>             PEM certificate chain for the MQTT port [MQTT_TLS_CERT]
    --tls-key <FILE>              PEM private key for the MQTT port [MQTT_TLS_KEY]
    --tls-client-ca <FILE>        PEM CA bundle; enables mutual TLS [MQTT_TLS_CLIENT_CA]
ADMIN TLS & AUTH:
    --admin-tls-cert <FILE>       PEM certificate chain for the admin port [MQTT_ADMIN_TLS_CERT]
    --admin-tls-key <FILE>        PEM private key for the admin port [MQTT_ADMIN_TLS_KEY]
    --admin-tls-client-ca <FILE>  PEM CA bundle; enables mutual TLS [MQTT_ADMIN_TLS_CLIENT_CA]
    --admin-token <TOKEN>         Bearer token for /metrics and /mcp [MQTT_ADMIN_TOKEN]
AUTHORIZATION:
    --acl-file <FILE>             JSON ACL policy per certificate identity [MQTT_ACL_FILE]
STORAGE & LIMITS:
    --db-path <FILE>              SQLite database file [MQTT_DB_PATH]
    --max-packet-size <BYTES>     Maximum accepted packet size [MQTT_MAX_PACKET_SIZE]
    --receive-maximum <N>         Server Receive Maximum [MQTT_RECEIVE_MAXIMUM]
    --max-session-expiry <SECS>   Cap on Session Expiry Interval [MQTT_MAX_SESSION_EXPIRY]
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
| `MQTT_LISTEN_ADDR` | `0.0.0.0:1883` | MQTT listener bind address |
| `MQTT_TLS_CERT` / `MQTT_TLS_KEY` | _(unset)_ | PEM cert + key; both set = TLS on the MQTT port |
| `MQTT_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the MQTT port |
| `MQTT_ADMIN_ADDR` | `127.0.0.1:9001` | Admin/metrics/MCP HTTP bind address |
| `MQTT_ADMIN_TLS_CERT` / `MQTT_ADMIN_TLS_KEY` | _(unset)_ | PEM cert + key; both set = HTTPS on the admin port |
| `MQTT_ADMIN_TLS_CLIENT_CA` | _(unset)_ | PEM CA bundle; enables mutual TLS on the admin port |
| `MQTT_ADMIN_TOKEN` | _(unset)_ | Bearer token for `/metrics` and `/mcp`; unset = open |
| `MQTT_ACL_FILE` | _(unset)_ | JSON ACL policy per certificate identity; unset = allow all |
| `MQTT_DB_PATH` | `mqtt_broker.db` | SQLite database file |
| `MQTT_MAX_PACKET_SIZE` | `1048576` | Max accepted packet size (bytes) |
| `MQTT_RECEIVE_MAXIMUM` | `64` | Server Receive Maximum |
| `MQTT_MAX_SESSION_EXPIRY` | `3600` | Cap on Session Expiry Interval (s) |
| `RUST_LOG` | `info` | Log level (`tracing` filter) |

### TLS

Both listeners support native TLS (rustls; no OpenSSL needed at runtime). TLS
is enabled per-port by pointing at a PEM certificate chain and private key —
the two ports are independent and may use the same or different certificates.

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

## Authentication & authorization (ACLs)

With mutual TLS, the **Common Name (CN)** of the client certificate is taken as
the connection's authenticated identity (a connection with no client
certificate has the identity `anonymous`). That identity is logged on connect
and drives per-client access control.

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

Intentionally out of scope for this implementation: WebSocket transport,
extended AUTH (SCRAM/challenge-response — AUTH is accepted as a no-op),
username/password authentication (identity comes from the mutual-TLS client
certificate CN), `$SYS` topics, and outbound topic-alias assignment (the server
always sends full topic names).
