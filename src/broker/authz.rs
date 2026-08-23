//! Authorization policy: the live ACL snapshot, SIGHUP reload, and revoking
//! subscriptions a reload no longer permits.
//!
//! Part of the `broker` module: this file continues `impl Broker` from
//! `mod.rs`. The slices share the parent's imports via `use super::*` because
//! they are not independent abstractions — they are one type's methods grouped
//! by the spec area they implement, so moving a method between slices should
//! not require import surgery.

use super::*;

impl Broker {
    /// Take a cheap snapshot of the current authorization policy.
    pub(super) fn acl(&self) -> Arc<Acl> {
        self.inner.acl.read().expect("acl lock poisoned").clone()
    }

    /// Reload the ACL policy from the configured file and swap it in
    /// atomically. Returns `Ok(true)` when a reload happened, `Ok(false)` when
    /// no ACL file is configured. On a parse/read error the existing policy is
    /// kept and the error is returned.
    ///
    /// After swapping, any live subscription that the new policy no longer
    /// authorizes is revoked (removed from the session and persistence).
    pub fn reload_acl(&self) -> crate::error::Result<bool> {
        let Some(path) = &self.inner.config.acl_path else {
            return Ok(false);
        };
        let fresh = Arc::new(Acl::load(path)?);
        *self.inner.acl.write().expect("acl lock poisoned") = fresh.clone();
        self.revoke_unauthorized_subscriptions(&fresh);
        Ok(true)
    }

    /// Remove subscriptions that are no longer authorized under `acl`, and
    /// disconnect any online client that had one revoked with a DISCONNECT
    /// carrying Reason Code 0x87 (Not authorized).
    ///
    /// Subscriptions are pruned even for offline persistent sessions so a
    /// later resume does not silently re-enable a revoked filter (delivery is
    /// not re-checked against the ACL).
    fn revoke_unauthorized_subscriptions(&self, acl: &Acl) {
        if !acl.is_enforced() {
            return;
        }
        let mut st = self.lock();
        for session in st.sessions.values_mut() {
            let who = session
                .identity
                .clone()
                .unwrap_or_else(|| ANONYMOUS.to_string());
            let client_id = session.client_id.clone();
            let persistent = session.persistent;

            let mut revoked: Vec<(String, Option<String>)> = Vec::new();
            session.subscriptions.retain(|sub| {
                if acl.can_subscribe(&who, &sub.filter) {
                    true
                } else {
                    revoked.push((sub.filter.clone(), sub.share_name.clone()));
                    false
                }
            });

            if revoked.is_empty() {
                continue;
            }
            for (filter, share) in &revoked {
                if persistent {
                    // Reconstruct the stored key (shared subs keep their prefix).
                    let key = match share {
                        Some(s) => format!("$share/{s}/{filter}"),
                        None => filter.clone(),
                    };
                    self.inner
                        .storage
                        .delete_subscription(client_id.clone(), key);
                }
            }

            // Administratively disconnect the client if it is online. The
            // connection task sends DISCONNECT (0x87) and closes; its Will is
            // not published for this server-initiated close.
            let online = session.out.is_some();
            if let Some(tx) = &session.out {
                let _ = tx.try_send(Outgoing::Shutdown(ReasonCode::NotAuthorized));
            }
            tracing::info!(
                "ACL reload revoked {} now-unauthorized subscription(s) for {client_id:?} ({who}){}",
                revoked.len(),
                if online { "; disconnecting (0x87 Not authorized)" } else { "" }
            );
        }
    }
}
