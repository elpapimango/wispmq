//! SQLite-backed persistence.
//!
//! Durable broker state — retained messages, persistent Sessions and their
//! Subscriptions — is stored in SQLite (bundled, latest amalgamation). Startup
//! loads happen synchronously; runtime writes are sent to a background thread
//! that owns the connection, keeping the network path off the disk latency.
//!
//! The connection uses WAL mode for concurrent durability.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread;

use rusqlite::Connection;

use crate::codec::{Reader, Writer};
use crate::error::{MqttError, Result};
use crate::message::Message;
use crate::packet::{RetainHandling, TopicFilter};
use crate::types::QoS;

/// A persisted subscription for a session.
#[derive(Debug, Clone)]
pub struct SubRecord {
    pub filter: String,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
    pub subscription_identifier: Option<u32>,
}

impl SubRecord {
    pub fn to_topic_filter(&self) -> TopicFilter {
        TopicFilter {
            filter: self.filter.clone(),
            qos: self.qos,
            no_local: self.no_local,
            retain_as_published: self.retain_as_published,
            retain_handling: self.retain_handling,
        }
    }
}

/// A persisted session with its subscriptions.
#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub client_id: String,
    pub session_expiry_interval: u32,
    /// The authenticated identity that owns this session. `None` for anonymous
    /// sessions or sessions created before the identity column was added.
    pub owner_identity: Option<String>,
    pub subscriptions: Vec<SubRecord>,
}

/// Everything loaded from disk when the broker starts.
#[derive(Debug, Default)]
pub struct LoadedState {
    pub retained: Vec<(String, Message)>,
    pub sessions: Vec<SessionRecord>,
}

/// Runtime persistence commands executed on the background thread.
enum Command {
    UpsertRetained {
        topic: String,
        message: Message,
    },
    DeleteRetained {
        topic: String,
    },
    UpsertSession {
        client_id: String,
        session_expiry: u32,
        owner_identity: Option<String>,
    },
    DeleteSession {
        client_id: String,
    },
    UpsertSubscription {
        client_id: String,
        sub: SubRecord,
    },
    DeleteSubscription {
        client_id: String,
        filter: String,
    },
    ClearSubscriptions {
        client_id: String,
    },
}

/// Handle used by the broker to persist state. Cloneable and cheap.
#[derive(Clone)]
pub struct Storage {
    tx: Sender<Command>,
    /// Set once the writer thread has been observed gone, so the (serious)
    /// "persistence has stopped" warning is logged once rather than per write.
    writer_lost: Arc<AtomicBool>,
}

impl Storage {
    /// Open the database, run migrations, load existing state, and start the
    /// background writer thread. Returns the handle and the loaded state.
    pub fn open(path: &str) -> Result<(Storage, LoadedState)> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&conn)?;
        let loaded = load_all(&conn)?;

        let (tx, rx) = mpsc::channel::<Command>();
        thread::Builder::new()
            .name("mqtt-storage".into())
            .spawn(move || writer_loop(conn, rx))
            .map_err(|e| MqttError::Storage(format!("spawn storage thread: {e}")))?;

        Ok((
            Storage {
                tx,
                writer_lost: Arc::new(AtomicBool::new(false)),
            },
            loaded,
        ))
    }

    /// A no-op storage handle for tests / when persistence is disabled.
    pub fn null() -> Storage {
        let (tx, rx) = mpsc::channel::<Command>();
        thread::spawn(move || {
            // Drain and discard.
            while rx.recv().is_ok() {}
        });
        Storage {
            tx,
            writer_lost: Arc::new(AtomicBool::new(false)),
        }
    }

    fn send(&self, cmd: Command) {
        // The network path must never block on persistence, so a failed send is
        // not propagated. It is still reported: `send` only fails once the
        // writer thread is gone, after which *nothing* is being persisted even
        // though the broker keeps advertising durable sessions and retained
        // messages. Silently continuing would turn that into data loss the
        // operator discovers at the next restart. Log once, not per write.
        if self.tx.send(cmd).is_err() && !self.writer_lost.swap(true, Ordering::Relaxed) {
            tracing::error!(
                "storage writer thread is gone; persistence has stopped. \
                 Sessions and retained messages will not survive a restart"
            );
        }
    }

    pub fn upsert_retained(&self, topic: String, message: Message) {
        self.send(Command::UpsertRetained { topic, message });
    }

    pub fn delete_retained(&self, topic: String) {
        self.send(Command::DeleteRetained { topic });
    }

    pub fn upsert_session(
        &self,
        client_id: String,
        session_expiry: u32,
        owner_identity: Option<String>,
    ) {
        self.send(Command::UpsertSession {
            client_id,
            session_expiry,
            owner_identity,
        });
    }

    pub fn delete_session(&self, client_id: String) {
        self.send(Command::DeleteSession { client_id });
    }

    pub fn upsert_subscription(&self, client_id: String, sub: SubRecord) {
        self.send(Command::UpsertSubscription { client_id, sub });
    }

    pub fn delete_subscription(&self, client_id: String, filter: String) {
        self.send(Command::DeleteSubscription { client_id, filter });
    }

    pub fn clear_subscriptions(&self, client_id: String) {
        self.send(Command::ClearSubscriptions { client_id });
    }
}

fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS retained_messages (
            topic               TEXT PRIMARY KEY,
            payload             BLOB NOT NULL,
            qos                 INTEGER NOT NULL,
            payload_format      INTEGER,
            content_type        TEXT,
            response_topic      TEXT,
            correlation_data    BLOB,
            user_properties     BLOB,
            expires_at          INTEGER
        );

        CREATE TABLE IF NOT EXISTS sessions (
            client_id           TEXT PRIMARY KEY,
            session_expiry      INTEGER NOT NULL,
            owner_identity      TEXT
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            client_id           TEXT NOT NULL,
            filter              TEXT NOT NULL,
            qos                 INTEGER NOT NULL,
            no_local            INTEGER NOT NULL,
            retain_as_published INTEGER NOT NULL,
            retain_handling     INTEGER NOT NULL,
            sub_id              INTEGER,
            PRIMARY KEY (client_id, filter),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );

        PRAGMA foreign_keys = ON;
        "#,
    )?;

    // Migration: add owner_identity column if it doesn't exist (for existing databases).
    // This is safe because the column is nullable and defaults to NULL.
    let has_owner_identity: bool = conn
        .prepare("SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name='owner_identity'")?
        .query_row([], |row| row.get::<_, i64>(0))
        .map(|count| count > 0)?;

    if !has_owner_identity {
        conn.execute("ALTER TABLE sessions ADD COLUMN owner_identity TEXT", [])?;
        tracing::info!("migrated sessions table: added owner_identity column");
    }

    Ok(())
}

fn load_all(conn: &Connection) -> Result<LoadedState> {
    let mut state = LoadedState::default();

    // Retained messages.
    let mut stmt = conn.prepare(
        "SELECT topic, payload, qos, payload_format, content_type, response_topic, \
         correlation_data, user_properties, expires_at FROM retained_messages",
    )?;
    let rows = stmt.query_map([], |row| {
        let topic: String = row.get(0)?;
        let payload: Vec<u8> = row.get(1)?;
        let qos: i64 = row.get(2)?;
        let payload_format: Option<i64> = row.get(3)?;
        let content_type: Option<String> = row.get(4)?;
        let response_topic: Option<String> = row.get(5)?;
        let correlation_data: Option<Vec<u8>> = row.get(6)?;
        let user_props_blob: Option<Vec<u8>> = row.get(7)?;
        let expires_at: Option<i64> = row.get(8)?;
        Ok((
            topic,
            payload,
            qos,
            payload_format,
            content_type,
            response_topic,
            correlation_data,
            user_props_blob,
            expires_at,
        ))
    })?;

    for row in rows {
        let (topic, payload, qos, pf, ct, rt, cd, up, exp) = row?;
        let message = Message {
            topic: topic.clone(),
            payload: payload.into(),
            qos: QoS::from_u8(qos as u8).unwrap_or(QoS::AtMostOnce),
            retain: true,
            payload_format_indicator: pf.map(|v| v as u8),
            content_type: ct,
            response_topic: rt,
            correlation_data: cd,
            user_properties: up.map(|b| decode_user_props(&b)).unwrap_or_default(),
            expires_at: exp.map(|v| v as u64),
        };
        // Skip already-expired retained messages.
        if !message.is_expired() {
            state.retained.push((topic, message));
        }
    }
    drop(stmt);

    // Sessions with their subscriptions.
    let mut sess_stmt =
        conn.prepare("SELECT client_id, session_expiry, owner_identity FROM sessions")?;
    let sessions: Vec<(String, i64, Option<String>)> = sess_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    drop(sess_stmt);

    for (client_id, expiry, owner_identity) in sessions {
        let mut sub_stmt = conn.prepare(
            "SELECT filter, qos, no_local, retain_as_published, retain_handling, sub_id \
             FROM subscriptions WHERE client_id = ?1",
        )?;
        let subs = sub_stmt
            .query_map([&client_id], |row| {
                Ok(SubRecord {
                    filter: row.get(0)?,
                    qos: QoS::from_u8(row.get::<_, i64>(1)? as u8).unwrap_or(QoS::AtMostOnce),
                    no_local: row.get::<_, i64>(2)? != 0,
                    retain_as_published: row.get::<_, i64>(3)? != 0,
                    retain_handling: match row.get::<_, i64>(4)? {
                        1 => RetainHandling::SendIfNewSubscription,
                        2 => RetainHandling::DoNotSend,
                        _ => RetainHandling::SendAtSubscribe,
                    },
                    subscription_identifier: row.get::<_, Option<i64>>(5)?.map(|v| v as u32),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        state.sessions.push(SessionRecord {
            client_id,
            session_expiry_interval: expiry as u32,
            owner_identity,
            subscriptions: subs,
        });
    }

    Ok(state)
}

fn writer_loop(conn: Connection, rx: mpsc::Receiver<Command>) {
    while let Ok(cmd) = rx.recv() {
        if let Err(e) = apply(&conn, cmd) {
            tracing::warn!("storage write failed: {e}");
        }
    }
}

fn apply(conn: &Connection, cmd: Command) -> rusqlite::Result<()> {
    match cmd {
        Command::UpsertRetained { topic, message } => {
            conn.execute(
                "INSERT INTO retained_messages \
                 (topic, payload, qos, payload_format, content_type, response_topic, \
                  correlation_data, user_properties, expires_at) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
                 ON CONFLICT(topic) DO UPDATE SET \
                  payload=excluded.payload, qos=excluded.qos, \
                  payload_format=excluded.payload_format, content_type=excluded.content_type, \
                  response_topic=excluded.response_topic, correlation_data=excluded.correlation_data, \
                  user_properties=excluded.user_properties, expires_at=excluded.expires_at",
                rusqlite::params![
                    topic,
                    message.payload,
                    message.qos.as_u8() as i64,
                    message.payload_format_indicator.map(|v| v as i64),
                    message.content_type,
                    message.response_topic,
                    message.correlation_data,
                    encode_user_props(&message.user_properties),
                    message.expires_at.map(|v| v as i64),
                ],
            )?;
        }
        Command::DeleteRetained { topic } => {
            conn.execute("DELETE FROM retained_messages WHERE topic = ?1", [topic])?;
        }
        Command::UpsertSession {
            client_id,
            session_expiry,
            owner_identity,
        } => {
            conn.execute(
                "INSERT INTO sessions (client_id, session_expiry, owner_identity) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(client_id) DO UPDATE SET session_expiry=excluded.session_expiry, owner_identity=excluded.owner_identity",
                rusqlite::params![client_id, session_expiry as i64, owner_identity],
            )?;
        }
        Command::DeleteSession { client_id } => {
            conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1",
                [&client_id],
            )?;
            conn.execute("DELETE FROM sessions WHERE client_id = ?1", [client_id])?;
        }
        Command::UpsertSubscription { client_id, sub } => {
            conn.execute(
                "INSERT INTO subscriptions \
                 (client_id, filter, qos, no_local, retain_as_published, retain_handling, sub_id) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7) \
                 ON CONFLICT(client_id, filter) DO UPDATE SET \
                  qos=excluded.qos, no_local=excluded.no_local, \
                  retain_as_published=excluded.retain_as_published, \
                  retain_handling=excluded.retain_handling, sub_id=excluded.sub_id",
                rusqlite::params![
                    client_id,
                    sub.filter,
                    sub.qos.as_u8() as i64,
                    sub.no_local as i64,
                    sub.retain_as_published as i64,
                    sub.retain_handling as i64,
                    sub.subscription_identifier.map(|v| v as i64),
                ],
            )?;
        }
        Command::DeleteSubscription { client_id, filter } => {
            conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1 AND filter = ?2",
                rusqlite::params![client_id, filter],
            )?;
        }
        Command::ClearSubscriptions { client_id } => {
            conn.execute(
                "DELETE FROM subscriptions WHERE client_id = ?1",
                [client_id],
            )?;
        }
    }
    Ok(())
}

/// Serialize user properties as a sequence of UTF-8 string pairs, reusing the
/// MQTT string encoding so the blob is self-describing.
fn encode_user_props(props: &[(String, String)]) -> Vec<u8> {
    let mut w = Writer::new();
    for (k, v) in props {
        w.put_utf8(k);
        w.put_utf8(v);
    }
    w.into_vec()
}

fn decode_user_props(blob: &[u8]) -> Vec<(String, String)> {
    let mut r = Reader::new(blob);
    let mut out = Vec::new();
    while r.has_remaining() {
        match r.utf8_pair() {
            Ok(pair) => out.push(pair),
            Err(_) => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Losing the writer thread must degrade to "not persisted" rather than
    /// panicking or blocking the caller, and must be reported exactly once.
    #[test]
    fn send_after_writer_thread_is_gone_does_not_panic() {
        let (tx, rx) = mpsc::channel::<Command>();
        drop(rx); // Simulate the writer thread having died.
        let storage = Storage {
            tx,
            writer_lost: Arc::new(AtomicBool::new(false)),
        };

        // Each of these would previously vanish without a trace.
        storage.delete_retained("a/b".into());
        assert!(
            storage.writer_lost.load(Ordering::Relaxed),
            "writer loss should be recorded on the first failed send"
        );

        // Subsequent sends still must not panic (and must not re-log).
        storage.delete_session("client".into());
        storage.delete_retained("c/d".into());
    }

    /// The null handle drains commands, so sends succeed and nothing is flagged.
    #[test]
    fn null_storage_accepts_writes() {
        let storage = Storage::null();
        storage.delete_retained("x".into());
        assert!(!storage.writer_lost.load(Ordering::Relaxed));
    }

    /// A retained message must survive a write/reopen cycle byte for byte.
    ///
    /// The payload crosses three representations on this path — `Arc<[u8]>` in
    /// `Message`, a SQLite BLOB on disk, and a `Vec<u8>` on the way back — so
    /// this pins the conversion at both ends. It also covers a payload with
    /// embedded NULs and high bytes, which would survive a text column but not
    /// a mangled one.
    #[test]
    fn retained_message_survives_a_reopen() {
        let dir = std::env::temp_dir().join(format!("wispmq-storage-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("retained.db");
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        let payload: Vec<u8> = vec![0x00, 0xff, b'h', b'i', 0x00, 0x80, 0x7f];
        let message = Message {
            topic: "sensors/kitchen".to_string(),
            payload: payload.clone().into(),
            qos: QoS::ExactlyOnce,
            retain: true,
            payload_format_indicator: Some(0),
            content_type: Some("application/octet-stream".to_string()),
            response_topic: Some("reply/here".to_string()),
            correlation_data: Some(vec![1, 2, 3]),
            user_properties: vec![("k".to_string(), "v".to_string())],
            expires_at: None,
        };

        {
            let (storage, loaded) = Storage::open(&path_str).unwrap();
            assert!(loaded.retained.is_empty(), "fresh db should be empty");
            storage.upsert_retained(message.topic.clone(), message.clone());
            drop(storage);
        }

        // Writes are asynchronous: dropping the handle closes the command
        // channel, but nothing joins the writer thread, so the commit can still
        // be in flight. Poll rather than assuming it has landed — assuming it
        // made this test pass locally and fail under load.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let restored = loop {
            let (_storage, loaded) = Storage::open(&path_str).unwrap();
            if let Some((_, m)) = loaded
                .retained
                .into_iter()
                .find(|(topic, _)| topic == "sensors/kitchen")
            {
                break m;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "retained message was never persisted"
            );
            thread::sleep(std::time::Duration::from_millis(25));
        };
        assert_eq!(&restored.payload[..], &payload[..]);
        assert_eq!(restored.qos, QoS::ExactlyOnce);
        assert_eq!(
            restored.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(restored.response_topic.as_deref(), Some("reply/here"));
        assert_eq!(restored.correlation_data.as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(
            restored.user_properties,
            vec![("k".to_string(), "v".to_string())]
        );

        let _ = std::fs::remove_file(&path);
    }
}
