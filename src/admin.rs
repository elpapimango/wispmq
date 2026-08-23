//! Admin surface served on a dedicated port (default `127.0.0.1:9001`):
//!
//! - `GET  /health`  — liveness probe, returns `{"status":"ok"}`.
//! - `GET  /metrics` — Prometheus text exposition of broker metrics.
//! - `POST /mcp`     — a Model Context Protocol server (JSON-RPC 2.0 over the
//!   Streamable-HTTP transport, request/response mode) exposing read-only
//!   tools to inspect broker state.
//!
//! A small hand-rolled HTTP/1.1 handler keeps the dependency surface minimal;
//! each request is served with `Connection: close`.

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::broker::Broker;
use crate::error::Result;

/// MCP protocol revision we implement (fallback when the client omits one).
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "mqtt-broker";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum bytes accepted for an HTTP request (headers + body).
const MAX_REQUEST: usize = 1 << 20;

/// Time allowed to read a full request (headers + body) before dropping the
/// socket — mirrors `server.rs::CONNECT_TIMEOUT`. `/health` is
/// unauthenticated, so without this a client can hold the connection open
/// indefinitely (a slowloris against liveness probes/metrics/MCP) by never
/// finishing its request; `MAX_REQUEST` alone only bounds bytes, not time.
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Bind the admin listener and serve until the process stops.
pub async fn run(broker: Broker) -> Result<()> {
    let cfg = broker.config();
    let addr = cfg.admin_addr;
    let acceptor = crate::tls::maybe_acceptor(
        &cfg.admin_tls_cert,
        &cfg.admin_tls_key,
        &cfg.admin_tls_client_ca,
        "admin",
    )?;
    let scheme = if acceptor.is_some() { "https" } else { "http" };
    let mtls = if cfg.admin_tls_client_ca.is_some() {
        " (mutual TLS: client certificate required)"
    } else {
        ""
    };
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "admin/metrics/MCP server listening on {scheme}://{addr}{mtls} (/health /metrics /mcp)"
    );
    if broker.config().admin_token.is_some() {
        tracing::info!("admin endpoints /metrics and /mcp require a bearer token");
    } else {
        tracing::warn!(
            "admin endpoints /metrics and /mcp are UNAUTHENTICATED; set MQTT_ADMIN_TOKEN to require a bearer token"
        );
    }

    loop {
        let (stream, _peer) = listener.accept().await?;
        let broker = broker.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let result = match acceptor {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls) => serve_conn(tls, broker).await,
                    Err(e) => {
                        tracing::debug!("admin TLS handshake failed: {e}");
                        return;
                    }
                },
                None => serve_conn(stream, broker).await,
            };
            if let Err(e) = result {
                tracing::debug!("admin connection error: {e}");
            }
        });
    }
}

/// A parsed HTTP request (only the parts we need).
struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
    /// Bearer token extracted from the `Authorization` header, if present.
    bearer: Option<String>,
}

async fn serve_conn<S>(mut stream: S, broker: Broker) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let req = match timeout(ADMIN_REQUEST_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(Some(req))) => req,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(()), // read timed out: drop the connection
    };

    let (status, content_type, body) = route(&broker, &req);

    // Offer the bearer challenge when we reject an unauthenticated request.
    let extra = if status.starts_with("401") {
        "WWW-Authenticate: Bearer\r\n"
    } else {
        ""
    };

    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Authorization, Content-Type\r\n\
         {extra}\
         Connection: close\r\n\
         \r\n",
        len = body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

/// Constant-time equality to avoid leaking the token via timing. Runs over
/// `max(a.len(), b.len())` unconditionally so a length mismatch doesn't
/// short-circuit and leak the true token's length.
fn tokens_match(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = (a.len() != b.len()) as u8;
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

/// Whether `req` satisfies the configured bearer token. Always true when no
/// token is configured.
fn authorized(broker: &Broker, req: &Request) -> bool {
    match &broker.config().admin_token {
        None => true,
        Some(expected) => req
            .bearer
            .as_deref()
            .map(|got| tokens_match(got, expected.expose()))
            .unwrap_or(false),
    }
}

/// Dispatch a request to a handler, returning (status line, content type, body).
fn route(broker: &Broker, req: &Request) -> (&'static str, &'static str, Vec<u8>) {
    // `/health` and CORS preflight are always open so liveness probes and
    // browsers work without credentials; everything else is guarded.
    let protected = !matches!(
        (req.method.as_str(), req.path.as_str()),
        ("OPTIONS", _) | ("GET", "/health") | ("GET", "/healthz") | ("GET", "/")
    );
    if protected && !authorized(broker, req) {
        return (
            "401 Unauthorized",
            "application/json",
            br#"{"error":"missing or invalid bearer token"}"#.to_vec(),
        );
    }

    match (req.method.as_str(), req.path.as_str()) {
        ("OPTIONS", _) => ("204 No Content", "text/plain", Vec::new()),
        ("GET", "/health") | ("GET", "/healthz") => {
            ("200 OK", "application/json", br#"{"status":"ok"}"#.to_vec())
        }
        ("GET", "/metrics") => (
            "200 OK",
            "text/plain; version=0.0.4",
            broker.snapshot().to_prometheus().into_bytes(),
        ),
        ("POST", "/mcp") => {
            let body = handle_mcp_http(broker, &req.body);
            match body {
                Some(v) => (
                    "200 OK",
                    "application/json",
                    serde_json::to_vec(&v).unwrap_or_default(),
                ),
                // Notifications produce no response body (Accepted).
                None => ("202 Accepted", "application/json", Vec::new()),
            }
        }
        ("GET", "/mcp") => (
            "405 Method Not Allowed",
            "application/json",
            br#"{"error":"use POST for JSON-RPC; SSE streaming is not supported"}"#.to_vec(),
        ),
        ("GET", "/") => (
            "200 OK",
            "text/plain",
            b"mqtt-broker admin: GET /health, GET /metrics, POST /mcp\n".to_vec(),
        ),
        _ => (
            "404 Not Found",
            "application/json",
            br#"{"error":"not found"}"#.to_vec(),
        ),
    }
}

/// Read one HTTP request: headers up to CRLFCRLF, then `Content-Length` bytes.
async fn read_request<S>(stream: &mut S) -> Result<Option<Request>>
where
    S: AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];

    // Read until we have the full header block.
    let header_end = loop {
        if let Some(pos) = find_crlf_crlf(&buf) {
            break pos;
        }
        if buf.len() > MAX_REQUEST {
            return Ok(None);
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None); // connection closed before a full request
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    // Headers we care about (case-insensitive names).
    let mut content_length = 0usize;
    let mut bearer: Option<String> = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            } else if name.eq_ignore_ascii_case("authorization") {
                // Expect "Bearer <token>" (scheme is case-insensitive).
                if let Some((scheme, token)) = value.split_once(' ') {
                    if scheme.eq_ignore_ascii_case("bearer") {
                        bearer = Some(token.trim().to_string());
                    }
                }
            }
        }
    }

    // Body: some may already be buffered after the header terminator.
    let body_start = header_end + 4;
    let mut body = buf[body_start..].to_vec();
    while body.len() < content_length {
        if body.len() > MAX_REQUEST {
            return Ok(None);
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);

    Ok(Some(Request {
        method,
        path,
        body,
        bearer,
    }))
}

fn find_crlf_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

// -------------------------------------------------------------------------
// MCP (Model Context Protocol) — JSON-RPC 2.0
// -------------------------------------------------------------------------

/// Handle a raw MCP HTTP body, which may be a single JSON-RPC message or a
/// batch array. Returns the JSON response, or `None` when the message(s) were
/// purely notifications requiring no reply.
fn handle_mcp_http(broker: &Broker, body: &[u8]) -> Option<Value> {
    let parsed: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": Value::Null,
                "error": { "code": -32700, "message": format!("parse error: {e}") }
            }));
        }
    };

    match parsed {
        Value::Array(items) => {
            let responses: Vec<Value> = items
                .iter()
                .filter_map(|item| handle_rpc(broker, item))
                .collect();
            if responses.is_empty() {
                None
            } else {
                Some(Value::Array(responses))
            }
        }
        other => handle_rpc(broker, &other),
    }
}

/// Handle a single JSON-RPC request object. `None` for notifications.
fn handle_rpc(broker: &Broker, req: &Value) -> Option<Value> {
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let id = req.get("id").cloned();
    // A request without an `id` is a notification: never produce a response.
    let is_notification = id.is_none();

    // Notifications (e.g. notifications/initialized) are simply acknowledged.
    if is_notification {
        return None;
    }
    let id = id.unwrap_or(Value::Null);

    let result: std::result::Result<Value, (i64, String)> = match method {
        "initialize" => {
            let version = req
                .get("params")
                .and_then(|p| p.get("protocolVersion"))
                .and_then(Value::as_str)
                .unwrap_or(MCP_PROTOCOL_VERSION)
                .to_string();
            Ok(json!({
                "protocolVersion": version,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION }
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => call_tool(broker, req.get("params")),
        other => Err((-32601, format!("method not found: {other}"))),
    };

    Some(match result {
        Ok(value) => json!({ "jsonrpc": "2.0", "id": id, "result": value }),
        Err((code, message)) => json!({
            "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message }
        }),
    })
}

/// The MCP tool catalogue advertised via `tools/list`.
fn tool_definitions() -> Value {
    let empty_schema = json!({ "type": "object", "properties": {}, "additionalProperties": false });
    json!([
        {
            "name": "broker_stats",
            "description": "Return broker counters and gauges (connections, packets, bytes, publishes, current clients/sessions/retained/subscriptions).",
            "inputSchema": empty_schema,
        },
        {
            "name": "list_clients",
            "description": "List all sessions with online status, subscription/inflight/queued counts and session expiry.",
            "inputSchema": empty_schema,
        },
        {
            "name": "list_retained",
            "description": "List retained messages with their topic, payload size in bytes and QoS.",
            "inputSchema": empty_schema,
        }
    ])
}

/// Execute a `tools/call`.
fn call_tool(broker: &Broker, params: Option<&Value>) -> std::result::Result<Value, (i64, String)> {
    let name = params
        .and_then(|p| p.get("name"))
        .and_then(Value::as_str)
        .ok_or((-32602, "missing tool name".to_string()))?;

    let payload = match name {
        "broker_stats" => {
            let s = broker.snapshot();
            json!({
                "connections_total": s.connections_total,
                "packets_received": s.packets_received,
                "packets_sent": s.packets_sent,
                "bytes_received": s.bytes_received,
                "bytes_sent": s.bytes_sent,
                "publish_received": s.publish_received,
                "publish_delivered": s.publish_delivered,
                "publish_dropped": s.publish_dropped,
                "bridge_forwarded_out": s.bridge_forwarded_out,
                "bridge_forwarded_in": s.bridge_forwarded_in,
                "clients_connected": s.clients_connected,
                "sessions_total": s.sessions_total,
                "retained_messages": s.retained_messages,
                "subscriptions_total": s.subscriptions_total,
                "bridges_connected": s.bridges_connected,
                "publish_bytes_received": s.publish_bytes_received,
                "publish_bytes_sent": s.publish_bytes_sent,
                "socket_connections": s.socket_connections,
                "clients_disconnected": s.clients_disconnected,
                "clients_total": s.clients_total,
                "clients_maximum": s.clients_maximum,
                "clients_expired": s.clients_expired,
                "retained_bytes": s.retained_bytes,
                "shared_subscriptions_count": s.shared_subscriptions_count,
                "store_messages_count": s.store_messages_count,
                "store_messages_bytes": s.store_messages_bytes,
                "packet_out_count": s.packet_out_count,
                "packet_out_bytes": s.packet_out_bytes,
                "uptime_seconds": s.uptime_seconds,
                "version": s.version,
            })
        }
        "list_clients" => {
            let clients: Vec<Value> = broker
                .clients()
                .into_iter()
                .map(|c| {
                    json!({
                        "client_id": c.client_id,
                        "online": c.online,
                        "subscriptions": c.subscriptions,
                        "inflight": c.inflight,
                        "queued": c.queued,
                        "session_expiry": c.session_expiry,
                        "persistent": c.persistent,
                    })
                })
                .collect();
            json!({ "clients": clients, "count": clients.len() })
        }
        "list_retained" => {
            let retained: Vec<Value> = broker
                .retained()
                .into_iter()
                .map(|r| json!({ "topic": r.topic, "payload_size": r.payload_size, "qos": r.qos }))
                .collect();
            json!({ "retained": retained, "count": retained.len() })
        }
        other => return Err((-32602, format!("unknown tool: {other}"))),
    };

    // MCP tool results wrap content items; we return the JSON as pretty text.
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
    Ok(json!({
        "content": [ { "type": "text", "text": text } ],
        "isError": false
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_match_correctness() {
        assert!(tokens_match("secret", "secret"));
        assert!(!tokens_match("secret", "different"));
        // Different lengths must still compare false, not short-circuit in a
        // way that would make the two calls take a different number of
        // rounds (the actual timing effect isn't testable here, but the
        // result must stay correct for every length combination).
        assert!(!tokens_match("short", "much-longer-token"));
        assert!(!tokens_match("much-longer-token", "short"));
        assert!(!tokens_match("", "nonempty"));
        assert!(tokens_match("", ""));
    }
}
