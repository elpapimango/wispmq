//! Shared helpers for integration tests. Not a test binary itself — `tests/`
//! only treats top-level files as separate binaries, so a `mod common;`
//! subdirectory is the standard way to share code between them.

/// Bind an ephemeral port, read its address, then release it immediately so
/// a broker under test can bind the same address a moment later.
pub fn free_addr() -> std::net::SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}
