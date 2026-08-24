//! Username / password authentication.
//!
//! Credentials live in a text file, one entry per line:
//!
//! ```text
//! # comments and blank lines are ignored
//! alice:pbkdf2_sha256$200000$<salt_hex>$<hash_hex>
//! ```
//!
//! Passwords are stored as PBKDF2-HMAC-SHA256 with a per-user random salt.
//! Generate an entry with `pulsemq --hash-password <username>` (the password
//! is read from stdin). Verification is constant-time (via `ring::pbkdf2`).

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::OnceLock;

use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};

use crate::error::{MqttError, Result};

/// PBKDF2 iterations used when hashing a new password.
const DEFAULT_ITERATIONS: u32 = 200_000;
/// Derived key / salt lengths in bytes.
const HASH_LEN: usize = 32;
const SALT_LEN: usize = 16;
const SCHEME: &str = "pbkdf2_sha256";

static PBKDF2_ALG: pbkdf2::Algorithm = pbkdf2::PBKDF2_HMAC_SHA256;

#[derive(Debug, Clone)]
struct Stored {
    iterations: NonZeroU32,
    salt: Vec<u8>,
    hash: Vec<u8>,
}

/// An in-memory credential store loaded from a password file.
#[derive(Debug, Clone)]
pub struct Credentials {
    users: HashMap<String, Stored>,
}

impl Credentials {
    /// Load and validate a password file.
    pub fn load(path: &str) -> Result<Credentials> {
        // Prevent path traversal attacks by rejecting paths containing '..'.
        let path_obj = std::path::Path::new(path);
        if path_obj
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        {
            return Err(MqttError::Config(format!(
                "Invalid input: {}",
                path_obj.display()
            )));
        }
        let text = std::fs::read_to_string(path_obj)
            .map_err(|e| MqttError::Config(format!("read password file {path}: {e}")))?;
        let mut users = HashMap::new();
        for (i, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (user, stored) = parse_entry(line)
                .map_err(|e| MqttError::Config(format!("{path}:{}: {e}", i + 1)))?;
            users.insert(user, stored);
        }
        Ok(Credentials { users })
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Verify a username / password pair.
    ///
    /// A PBKDF2 derivation is always performed, even when the username is not
    /// in the store. Returning early on an unknown user would make a bad
    /// username resolve in nanoseconds while a bad password costs the full
    /// iteration count (~200k iterations, tens of milliseconds) — a difference
    /// an attacker can measure remotely to enumerate valid usernames. The
    /// decoy keeps both paths on the same order of magnitude.
    pub fn verify(&self, username: &str, password: &[u8]) -> bool {
        let (stored, known) = match self.users.get(username) {
            Some(s) => (s, true),
            None => (decoy(), false),
        };
        let matched = pbkdf2::verify(
            PBKDF2_ALG,
            stored.iterations,
            &stored.salt,
            password,
            &stored.hash,
        )
        .is_ok();
        // Non-short-circuiting `&`: never branch away before the work is done.
        matched & known
    }
}

/// A fixed credential used only to spend the same work as a real verification
/// when the username is unknown. The derived hash is all-zero, so verification
/// against it fails; only its cost matters, and the result is discarded.
fn decoy() -> &'static Stored {
    static DECOY: OnceLock<Stored> = OnceLock::new();
    DECOY.get_or_init(|| Stored {
        iterations: NonZeroU32::new(DEFAULT_ITERATIONS).expect("DEFAULT_ITERATIONS is nonzero"),
        salt: vec![0u8; SALT_LEN],
        hash: vec![0u8; HASH_LEN],
    })
}

/// Hash a password into a storable `pbkdf2_sha256$iters$salt$hash` string.
pub fn hash_password(password: &[u8]) -> String {
    let rng = SystemRandom::new();
    let mut salt = [0u8; SALT_LEN];
    rng.fill(&mut salt).expect("system RNG failure");
    let iterations = NonZeroU32::new(DEFAULT_ITERATIONS).unwrap();
    let mut hash = [0u8; HASH_LEN];
    pbkdf2::derive(PBKDF2_ALG, iterations, &salt, password, &mut hash);
    format!(
        "{SCHEME}${DEFAULT_ITERATIONS}${}${}",
        to_hex(&salt),
        to_hex(&hash)
    )
}

/// Format a full `username:hash` credential-file line.
pub fn format_entry(username: &str, password: &[u8]) -> String {
    format!("{username}:{}", hash_password(password))
}

fn parse_entry(line: &str) -> std::result::Result<(String, Stored), String> {
    let (user, rest) = line
        .split_once(':')
        .ok_or_else(|| "expected 'username:hash'".to_string())?;
    if user.is_empty() {
        return Err("empty username".to_string());
    }
    let parts: Vec<&str> = rest.split('$').collect();
    if parts.len() != 4 || parts[0] != SCHEME {
        return Err(format!("unsupported password hash (expected {SCHEME}$…)"));
    }
    let iterations = parts[1]
        .parse::<u32>()
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| "iterations must be a positive integer".to_string())?;
    let salt = from_hex(parts[2]).ok_or_else(|| "invalid salt hex".to_string())?;
    let hash = from_hex(parts[3]).ok_or_else(|| "invalid hash hex".to_string())?;
    Ok((
        user.to_string(),
        Stored {
            iterations,
            salt,
            hash,
        },
    ))
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes.len().is_multiple_of(2) {
        return None;
    }
    bytes
        .chunks(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16)?;
            let lo = (pair[1] as char).to_digit(16)?;
            Some(((hi << 4) | lo) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_hash_and_verify() {
        let entry = format_entry("alice", b"s3cr3t");
        // Persist via the parser and verify.
        let creds = {
            let mut users = HashMap::new();
            let (u, s) = parse_entry(&entry).unwrap();
            users.insert(u, s);
            Credentials { users }
        };
        assert!(creds.verify("alice", b"s3cr3t"));
        assert!(!creds.verify("alice", b"wrong"));
        assert!(!creds.verify("bob", b"s3cr3t"));
    }

    #[test]
    fn rejects_malformed_entries() {
        assert!(parse_entry("nouserline").is_err());
        assert!(parse_entry("u:md5$1$aa$bb").is_err());
        assert!(parse_entry("u:pbkdf2_sha256$0$aa$bb").is_err());
        assert!(parse_entry("u:pbkdf2_sha256$1$zz$bb").is_err());
    }

    #[test]
    fn unknown_user_costs_the_same_as_a_wrong_password() {
        // Guards the username-enumeration fix: an unknown username must not
        // resolve dramatically faster than a known one with a bad password.
        use std::time::Instant;
        let entry = format_entry("alice", b"s3cr3t");
        let creds = {
            let mut users = HashMap::new();
            let (u, s) = parse_entry(&entry).unwrap();
            users.insert(u, s);
            Credentials { users }
        };

        let t0 = Instant::now();
        assert!(!creds.verify("alice", b"wrong-password"));
        let known = t0.elapsed();

        let t1 = Instant::now();
        assert!(!creds.verify("nonexistent-user", b"wrong-password"));
        let unknown = t1.elapsed();

        // Both run the same iteration count, so allow generous scheduling
        // noise but reject the old behaviour (which was orders of magnitude
        // apart -- a hashmap miss versus 200k PBKDF2 iterations).
        assert!(
            unknown * 10 > known,
            "unknown-user verify ({unknown:?}) was far faster than \
             known-user verify ({known:?}): username enumeration is possible"
        );
    }

    #[test]
    fn distinct_salts_per_hash() {
        // Two hashes of the same password differ (random salt).
        assert_ne!(hash_password(b"same"), hash_password(b"same"));
    }

    #[test]
    fn from_hex_rejects_non_ascii_without_panicking() {
        // "a€" has an even byte length (1-byte 'a' + 3-byte '€' = 4), so the
        // old byte-index str-slicing implementation passed the length check
        // and then panicked slicing mid-codepoint instead of returning None.
        assert_eq!(from_hex("a€"), None);
        assert_eq!(from_hex("zz"), None); // not hex digits, but ASCII
        assert_eq!(from_hex("2a"), Some(vec![0x2a]));
    }
}
