//! Per-source-IP connection-rate limiting for the client-facing listeners
//! (MQTT TCP/TLS and WebSocket).
//!
//! Fixed-window counter: each source IP gets `limit` new connections per
//! `window`, shared across both listeners so switching transport doesn't
//! reset a source's budget. Disabled by default (`limit == 0`) — unlike the
//! always-on bounds elsewhere in the broker (outbound channel capacity, WS
//! frame size), a connection-rate cap has a real false-positive risk: many
//! devices behind one NAT gateway can look identical to an attacker from one
//! source IP. Operators who know their deployment opt in via
//! `max_connections_per_ip`.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::broker::Broker;

pub struct RateLimiter {
    limit: u32,
    window: Duration,
    state: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl RateLimiter {
    pub fn new(limit: u32, window_secs: u32) -> Self {
        Self::with_window(limit, Duration::from_secs(window_secs as u64))
    }

    fn with_window(limit: u32, window: Duration) -> Self {
        RateLimiter {
            limit,
            window,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Records one connection attempt from `ip` and returns whether it may
    /// proceed. `limit == 0` always returns `true` without touching the lock.
    pub fn check(&self, ip: IpAddr) -> bool {
        if self.limit == 0 {
            return true;
        }
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let entry = state.entry(ip).or_insert((now, 0));
        if now.duration_since(entry.0) >= self.window {
            *entry = (now, 1);
            return true;
        }
        entry.1 += 1;
        entry.1 <= self.limit
    }

    /// Drop entries whose window has aged out, so the tracking map doesn't
    /// grow forever from one-off or attacker source IPs. Called periodically
    /// by [`run`].
    fn sweep(&self) {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        state.retain(|_, (start, _)| now.duration_since(*start) < self.window * 2);
    }
}

/// Periodic cleanup of stale per-IP entries. Mirrors `sysinfo::run`'s shape:
/// returns immediately when the limiter is disabled, so `main` can always
/// spawn this unconditionally.
pub async fn run(broker: Broker) {
    if broker.config().max_connections_per_ip == 0 {
        tracing::debug!("max_connections_per_ip is 0; connection-rate limiting is disabled");
        return;
    }
    let period = Duration::from_secs(broker.config().connection_rate_window_secs as u64) * 2;
    loop {
        tokio::time::sleep(period).await;
        broker.rate_limiter().sweep();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_exactly_limit_attempts_per_window() {
        let rl = RateLimiter::with_window(2, Duration::from_secs(60));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(rl.check(ip));
        assert!(rl.check(ip));
        assert!(
            !rl.check(ip),
            "third attempt within the window must be rejected"
        );
    }

    #[test]
    fn a_new_window_resets_the_count() {
        let rl = RateLimiter::with_window(1, Duration::from_millis(20));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(rl.check(ip));
        assert!(!rl.check(ip));
        std::thread::sleep(Duration::from_millis(30));
        assert!(rl.check(ip), "a new window should allow another attempt");
    }

    #[test]
    fn zero_limit_never_rejects() {
        let rl = RateLimiter::with_window(0, Duration::from_secs(60));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        for _ in 0..100 {
            assert!(rl.check(ip));
        }
    }

    #[test]
    fn distinct_ips_have_independent_budgets() {
        let rl = RateLimiter::with_window(1, Duration::from_secs(60));
        let a: IpAddr = "127.0.0.1".parse().unwrap();
        let b: IpAddr = "127.0.0.2".parse().unwrap();
        assert!(rl.check(a));
        assert!(!rl.check(a));
        assert!(
            rl.check(b),
            "a different source IP must have its own budget"
        );
    }

    #[test]
    fn sweep_evicts_aged_out_entries() {
        let rl = RateLimiter::with_window(1, Duration::from_millis(10));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(rl.check(ip));
        assert_eq!(rl.state.lock().unwrap().len(), 1);
        std::thread::sleep(Duration::from_millis(25)); // > window * 2
        rl.sweep();
        assert!(
            rl.state.lock().unwrap().is_empty(),
            "an entry older than window * 2 should be swept"
        );
    }
}
