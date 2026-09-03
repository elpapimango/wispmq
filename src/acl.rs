//! Per-identity access control (authorization).
//!
//! An ACL grants an authenticated identity (the Common Name of its mutual-TLS
//! client certificate, or `"anonymous"` when there is no client certificate)
//! the right to publish to and/or subscribe to sets of topic filters.
//!
//! The policy is loaded from a TOML file:
//!
//! ```toml
//! default = "deny"
//!
//! [[rules]]
//! identity = "sensor-01"
//! publish = ["sensors/01/#"]
//! subscribe = ["cmd/01/#"]
//!
//! [[rules]]
//! identity = "*"
//! subscribe = ["public/#"]
//! ```
//!
//! - `identity` matches the certificate CN exactly, or `"*"` for any identity.
//! - `publish` / `subscribe` are MQTT topic filters (wildcards allowed).
//! - `default` is the action when no rule grants access: `"deny"` (default) or
//!   `"allow"`.
//!
//! When no ACL file is configured the broker authorizes everything, preserving
//! the previous behaviour.

use serde_json::Value;

use crate::error::{MqttError, Result};
use crate::topic;

/// The wildcard identity that matches every principal.
const ANY: &str = "*";

#[derive(Debug, Clone)]
struct Rule {
    identity: String,
    publish: Vec<String>,
    subscribe: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Acl {
    /// When false, everything is permitted (no ACL file configured).
    enforced: bool,
    /// Action when no rule grants the operation.
    default_allow: bool,
    rules: Vec<Rule>,
}

impl Acl {
    /// A permissive ACL that authorizes every operation.
    pub fn permit_all() -> Acl {
        Acl {
            enforced: false,
            default_allow: true,
            rules: Vec::new(),
        }
    }

    pub fn is_enforced(&self) -> bool {
        self.enforced
    }

    /// Load and validate an ACL policy from a TOML file.
    pub fn load(path: &str) -> Result<Acl> {
        // Prevent path traversal attacks by rejecting paths containing '..'.
        let p = std::path::Path::new(path);
        if p.components().any(|c| c == std::path::Component::ParentDir) {
            return Err(MqttError::Config(format!("Invalid input: {}", p.display())));
        }
        let text = std::fs::read_to_string(p)
            .map_err(|e| MqttError::Config(format!("read ACL file {path}: {e}")))?;
        Acl::from_toml_str(&text, path)
    }

    /// Parse and validate ACL policy TOML text. `source` names the file for
    /// error messages (also used directly by tests, without touching the
    /// filesystem — mirrors `Config::apply_toml_str`).
    pub fn from_toml_str(text: &str, source: &str) -> Result<Acl> {
        let toml_doc: toml::Value = toml::from_str(text)
            .map_err(|e| MqttError::Config(format!("{source}: invalid TOML: {e}")))?;
        // Pivot through JSON, same technique `Config::apply_toml_str` uses:
        // `from_value` below only ever looked at a generic `serde_json::Value`,
        // so it needs no changes to accept a TOML-sourced document.
        let value: Value = serde_json::to_value(toml_doc)
            .map_err(|e| MqttError::Config(format!("{source}: {e}")))?;
        Acl::from_value(&value).map_err(|e| MqttError::Config(format!("{source}: {e}")))
    }

    /// Parse an ACL from a `serde_json::Value` — the shared representation
    /// `from_toml_str` pivots the TOML source through (also used directly by
    /// tests, format-agnostic either way).
    pub fn from_value(value: &Value) -> std::result::Result<Acl, String> {
        let default_allow = match value.get("default").and_then(Value::as_str) {
            None | Some("deny") => false,
            Some("allow") => true,
            Some(other) => return Err(format!("invalid \"default\": {other:?} (use allow|deny)")),
        };

        let mut rules = Vec::new();
        if let Some(arr) = value.get("rules") {
            let arr = arr
                .as_array()
                .ok_or_else(|| "\"rules\" must be an array".to_string())?;
            for (i, r) in arr.iter().enumerate() {
                let identity = r
                    .get("identity")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("rule {i}: missing string \"identity\""))?
                    .to_string();
                let publish = parse_filters(r.get("publish"), i, "publish")?;
                let subscribe = parse_filters(r.get("subscribe"), i, "subscribe")?;
                rules.push(Rule {
                    identity,
                    publish,
                    subscribe,
                });
            }
        }

        Ok(Acl {
            enforced: true,
            default_allow,
            rules,
        })
    }

    /// May `identity` publish to the concrete Topic Name `topic`?
    pub fn can_publish(&self, identity: &str, topic: &str) -> bool {
        if !self.enforced {
            return true;
        }
        for rule in self.applicable(identity) {
            if rule.publish.iter().any(|f| topic::matches(f, topic)) {
                return true;
            }
        }
        self.default_allow
    }

    /// May `identity` subscribe to the Topic Filter `filter`?
    ///
    /// A subscription is granted when the requested filter is covered by an
    /// allowed filter. Coverage is tested by matching the requested filter
    /// against each allowed pattern as if it were a topic; a request broader
    /// than any grant is therefore denied (fail-closed).
    pub fn can_subscribe(&self, identity: &str, filter: &str) -> bool {
        if !self.enforced {
            return true;
        }
        for rule in self.applicable(identity) {
            if rule.subscribe.iter().any(|f| topic::matches(f, filter)) {
                return true;
            }
        }
        self.default_allow
    }

    fn applicable<'a>(&'a self, identity: &'a str) -> impl Iterator<Item = &'a Rule> + 'a {
        self.rules
            .iter()
            .filter(move |r| r.identity == identity || r.identity == ANY)
    }
}

fn parse_filters(
    value: Option<&Value>,
    rule_index: usize,
    field: &str,
) -> std::result::Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let arr = value
        .as_array()
        .ok_or_else(|| format!("rule {rule_index}: \"{field}\" must be an array of strings"))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let f = item
            .as_str()
            .ok_or_else(|| format!("rule {rule_index}: \"{field}\" entries must be strings"))?;
        if !topic::valid_topic_filter(f) {
            return Err(format!(
                "rule {rule_index}: invalid topic filter {f:?} in \"{field}\""
            ));
        }
        out.push(f.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn acl() -> Acl {
        Acl::from_value(&json!({
            "default": "deny",
            "rules": [
                { "identity": "sensor-01", "publish": ["sensors/01/#"], "subscribe": ["cmd/01/#"] },
                { "identity": "*", "subscribe": ["public/#"] }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn publish_authorization() {
        let a = acl();
        assert!(a.can_publish("sensor-01", "sensors/01/temp"));
        assert!(!a.can_publish("sensor-01", "sensors/02/temp"));
        assert!(!a.can_publish("other", "sensors/01/temp"));
    }

    #[test]
    fn subscribe_authorization_and_wildcard_identity() {
        let a = acl();
        assert!(a.can_subscribe("sensor-01", "cmd/01/reboot"));
        assert!(a.can_subscribe("sensor-01", "public/news")); // via "*"
        assert!(a.can_subscribe("anyone", "public/news")); // via "*"
        assert!(!a.can_subscribe("anyone", "cmd/01/reboot"));
    }

    #[test]
    fn permit_all_allows_everything() {
        let a = Acl::permit_all();
        assert!(a.can_publish("x", "anything/at/all"));
        assert!(a.can_subscribe("x", "#"));
    }

    #[test]
    fn default_allow() {
        let a = Acl::from_value(&json!({ "default": "allow", "rules": [] })).unwrap();
        assert!(a.can_publish("x", "y"));
    }

    #[test]
    fn toml_parses_multiple_rules() {
        let toml = r#"
default = "deny"

[[rules]]
identity = "sensor-01"
publish = ["sensors/01/#"]
subscribe = ["cmd/01/#"]

[[rules]]
identity = "*"
subscribe = ["public/#"]
"#;
        let a = Acl::from_toml_str(toml, "acl.toml").unwrap();
        assert!(a.can_publish("sensor-01", "sensors/01/temp"));
        assert!(!a.can_publish("sensor-01", "sensors/02/temp"));
        assert!(a.can_subscribe("anyone", "public/news"));
    }

    #[test]
    fn toml_default_allow() {
        let a = Acl::from_toml_str("default = \"allow\"", "acl.toml").unwrap();
        assert!(a.can_publish("x", "y"));
    }

    #[test]
    fn toml_rejects_bad_default_and_malformed_syntax() {
        assert!(Acl::from_toml_str("default = \"maybe\"", "acl.toml").is_err());
        assert!(Acl::from_toml_str("key = ", "acl.toml").is_err());
        // A rule missing its required "identity" is rejected, not defaulted.
        let toml = "[[rules]]\npublish = [\"a/#\"]\n";
        assert!(Acl::from_toml_str(toml, "acl.toml").is_err());
    }
}
