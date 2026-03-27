//! Session Clustering Module — RFC 003 §5
//!
//! Implements social-topology-first session clustering with a three-layer
//! LRU memory hierarchy (L0 active → L1 inactive → L2 archived to SQLite).
//!
//! Core insight (RFC 003 §5.1): reply chains are hard bindings;
//! semantic similarity is only a fallback when topology is absent.

use std::collections::HashMap;

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;
use tokio::sync::Mutex;

/// A reference to a message within a session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MsgRef {
    pub msg_id: i64,
    pub chat_id: String,
    pub reply_to: Option<i64>,
    pub text: String,
    pub speaker: String,
    pub ts: String,
}

/// A session — a cluster of topologically linked messages.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Session {
    pub session_id: String,
    pub chat_id: String,
    pub messages: Vec<MsgRef>,
    pub last_active: chrono::DateTime<chrono::Utc>,
}

impl Session {
    fn new(chat_id: &str, first_msg: MsgRef) -> Self {
        let session_id = format!("s-{}-{}", chat_id, first_msg.msg_id);
        Self {
            session_id,
            chat_id: chat_id.to_string(),
            messages: vec![first_msg],
            last_active: chrono::Utc::now(),
        }
    }

    fn touch(&mut self) {
        self.last_active = chrono::Utc::now();
    }

    fn add_message(&mut self, msg: MsgRef) {
        self.messages.push(msg);
        self.touch();
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::json!(null))
    }
}

struct Inner {
    msg_index: HashMap<i64, String>, // msg_id → session_id
    l0: HashMap<String, Session>,
    l1: HashMap<String, Session>,
    l0_capacity: usize,
    l1_capacity: usize,
}

impl Inner {
    fn find_session_mut(&mut self, sid: &str) -> Option<(&mut Session, u8)> {
        if let Some(s) = self.l0.get_mut(sid) {
            return Some((s, 0));
        }
        if let Some(s) = self.l1.get_mut(sid) {
            return Some((s, 1));
        }
        None
    }

    fn promote_to_l0(&mut self, sid: &str) {
        if let Some(s) = self.l1.remove(sid) {
            self.l0.insert(sid.to_string(), s);
        }
    }
}

/// Session store with three-layer LRU: L0 (active) → L1 (inactive) → L2 (SQLite).
pub struct SessionStore {
    inner: Mutex<Inner>,
    pool: Option<SqlitePool>,
}

impl SessionStore {
    /// Create an in-memory-only session store (no L2 persistence).
    pub fn new(l0_capacity: usize, l1_capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                l0: HashMap::new(),
                l1: HashMap::new(),
                l0_capacity,
                l1_capacity,
            }),
            pool: None,
        }
    }

    /// Create a session store with SQLite L2 persistence.
    pub async fn with_sqlite(
        db_path: impl AsRef<std::path::Path>,
        l0_capacity: usize,
        l1_capacity: usize,
    ) -> Result<Self, logos_vfs::VfsError> {
        if let Some(parent) = db_path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| logos_vfs::VfsError::Io(format!("create session dir: {e}")))?;
        }
        let url = format!("sqlite:{}?mode=rwc", db_path.as_ref().display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .map_err(|e| logos_vfs::VfsError::Sqlite(format!("open session db: {e}")))?;

        init_schema(&pool).await?;

        Ok(Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                l0: HashMap::new(),
                l1: HashMap::new(),
                l0_capacity,
                l1_capacity,
            }),
            pool: Some(pool),
        })
    }

    /// Observe a new message and update session topology.
    pub async fn observe(&self, msg: MsgRef) {
        let msg_id = msg.msg_id;
        let reply_to = msg.reply_to;

        // Page fault: load from L2 outside the lock
        let page_fault_session = if let Some(reply_to) = reply_to {
            let need_fault = {
                let inner = self.inner.lock().await;
                inner.msg_index.get(&reply_to).is_none()
            };
            if need_fault {
                if let Some(ref pool) = self.pool {
                    l2_load_by_msg(pool, reply_to).await.ok().flatten()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Delete from L2 outside the lock (if we loaded a session)
        if let Some(ref session) = page_fault_session {
            if let Some(ref pool) = self.pool {
                let _ = l2_delete(pool, &session.session_id).await;
            }
        }

        // Now take the lock for in-memory operations only
        let evicted = {
            let mut inner = self.inner.lock().await;

            if let Some(mut session) = page_fault_session {
                for m in &session.messages {
                    inner.msg_index.insert(m.msg_id, session.session_id.clone());
                }
                session.add_message(msg.clone());
                inner.msg_index.insert(msg_id, session.session_id.clone());
                let sid = session.session_id.clone();
                inner.l0.insert(sid, session);
                Self::evict_collect(&mut inner)
            } else if let Some(reply_to) = reply_to {
                if let Some(sid) = inner.msg_index.get(&reply_to).cloned() {
                    let needs_promote = match inner.find_session_mut(&sid) {
                        Some((session, layer)) => {
                            session.add_message(msg);
                            layer > 0
                        }
                        None => false,
                    };
                    if needs_promote {
                        inner.promote_to_l0(&sid);
                    }
                    inner.msg_index.insert(msg_id, sid);
                    Self::evict_collect(&mut inner)
                } else {
                    let chat_id = msg.chat_id.clone();
                    let session = Session::new(&chat_id, msg);
                    let sid = session.session_id.clone();
                    inner.msg_index.insert(msg_id, sid.clone());
                    inner.l0.insert(sid, session);
                    Self::evict_collect(&mut inner)
                }
            } else {
                let chat_id = msg.chat_id.clone();
                let session = Session::new(&chat_id, msg);
                let sid = session.session_id.clone();
                inner.msg_index.insert(msg_id, sid.clone());
                inner.l0.insert(sid, session);
                Self::evict_collect(&mut inner)
            }
        };

        // Persist evicted sessions outside the lock
        if let Some(ref pool) = self.pool {
            for session in evicted {
                let _ = l2_persist(pool, &session).await;
            }
        }
    }

    fn evict_collect(inner: &mut Inner) -> Vec<Session> {
        while inner.l0.len() > inner.l0_capacity {
            let oldest = inner.l0.iter()
                .min_by_key(|(_, s)| s.last_active)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                if let Some(s) = inner.l0.remove(&key) {
                    inner.l1.insert(key, s);
                }
            } else {
                break;
            }
        }
        let mut evicted = Vec::new();
        while inner.l1.len() > inner.l1_capacity {
            let oldest = inner.l1.iter()
                .min_by_key(|(_, s)| s.last_active)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                if let Some(s) = inner.l1.remove(&key) {
                    inner.msg_index.retain(|_, v| *v != key);
                    evicted.push(s);
                }
            } else {
                break;
            }
        }
        evicted
    }

    pub async fn get_session_for_msg(&self, msg_id: i64) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(sid) = inner.msg_index.get(&msg_id) {
            if let Some(s) = inner.l0.get(sid) { return Some(s.clone()); }
            if let Some(s) = inner.l1.get(sid) { return Some(s.clone()); }
        }
        drop(inner);
        // Try L2
        if let Some(ref pool) = self.pool {
            return l2_load_by_msg(pool, msg_id).await.ok().flatten();
        }
        None
    }

    pub async fn get_session(&self, session_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(s) = inner.l0.get(session_id) { return Some(s.clone()); }
        if let Some(s) = inner.l1.get(session_id) { return Some(s.clone()); }
        drop(inner);
        if let Some(ref pool) = self.pool {
            return l2_load_by_id(pool, session_id).await.ok().flatten();
        }
        None
    }

    pub async fn get_active_session(&self, chat_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        inner.l0.values()
            .filter(|s| s.chat_id == chat_id)
            .max_by_key(|s| s.last_active)
            .cloned()
    }
}

// =============================================================================
// L2 SQLite operations
// =============================================================================

async fn init_schema(pool: &SqlitePool) -> Result<(), logos_vfs::VfsError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id  TEXT PRIMARY KEY,
            chat_id     TEXT NOT NULL,
            data        TEXT NOT NULL,
            last_active TEXT NOT NULL
        )"
    )
    .execute(pool)
    .await
    .map_err(|e| logos_vfs::VfsError::Sqlite(format!("init sessions schema: {e}")))?;

    // msg_id → session_id index for fast page fault lookups
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS msg_index (
            msg_id      INTEGER PRIMARY KEY,
            session_id  TEXT NOT NULL
        )"
    )
    .execute(pool)
    .await
    .map_err(|e| logos_vfs::VfsError::Sqlite(format!("init msg_index schema: {e}")))?;

    Ok(())
}

async fn l2_persist(pool: &SqlitePool, session: &Session) -> Result<(), sqlx::Error> {
    let data = serde_json::to_string(session).unwrap_or_default();
    let last_active = session.last_active.to_rfc3339();

    sqlx::query(
        "INSERT OR REPLACE INTO sessions (session_id, chat_id, data, last_active)
         VALUES (?1, ?2, ?3, ?4)"
    )
    .bind(&session.session_id)
    .bind(&session.chat_id)
    .bind(&data)
    .bind(&last_active)
    .execute(pool)
    .await?;

    // Update msg_id index
    for msg in &session.messages {
        sqlx::query(
            "INSERT OR REPLACE INTO msg_index (msg_id, session_id) VALUES (?1, ?2)"
        )
        .bind(msg.msg_id)
        .bind(&session.session_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}

async fn l2_delete(pool: &SqlitePool, session_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM msg_index WHERE session_id = ?1")
        .bind(session_id)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM sessions WHERE session_id = ?1")
        .bind(session_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn l2_load_by_id(pool: &SqlitePool, session_id: &str) -> Result<Option<Session>, sqlx::Error> {
    let row = sqlx::query("SELECT data FROM sessions WHERE session_id = ?1")
        .bind(session_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        let data: String = sqlx::Row::get(&row, 0);
        return Ok(serde_json::from_str(&data).ok());
    }
    Ok(None)
}

async fn l2_load_by_msg(pool: &SqlitePool, msg_id: i64) -> Result<Option<Session>, sqlx::Error> {
    // O(1) lookup via msg_index table — no more LIKE full-table scan
    let row = sqlx::query("SELECT session_id FROM msg_index WHERE msg_id = ?1")
        .bind(msg_id)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = row {
        let session_id: String = sqlx::Row::get(&row, 0);
        return l2_load_by_id(pool, &session_id).await;
    }
    Ok(None)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(msg_id: i64, chat_id: &str, reply_to: Option<i64>) -> MsgRef {
        MsgRef {
            msg_id,
            chat_id: chat_id.to_string(),
            reply_to,
            text: format!("msg-{msg_id}"),
            speaker: "test".to_string(),
            ts: "2026-03-20T10:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn new_message_creates_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 1);
    }

    #[tokio::test]
    async fn reply_joins_existing_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", Some(1))).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2);
        assert_eq!(s.session_id, store.get_session_for_msg(2).await.unwrap().session_id);
    }

    #[tokio::test]
    async fn no_reply_creates_separate_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        let s1 = store.get_session_for_msg(1).await.unwrap();
        let s2 = store.get_session_for_msg(2).await.unwrap();
        assert_ne!(s1.session_id, s2.session_id);
    }

    #[tokio::test]
    async fn deep_reply_chain() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", Some(1))).await;
        store.observe(make_msg(3, "c1", Some(2))).await;
        let s = store.get_session_for_msg(3).await.unwrap();
        assert_eq!(s.messages.len(), 3);
    }

    #[tokio::test]
    async fn lru_eviction_l0_to_l1() {
        let store = SessionStore::new(2, 256); // L0 capacity = 2
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await; // evicts oldest from L0

        // All should still be findable (in L0 or L1)
        assert!(store.get_session_for_msg(1).await.is_some());
        assert!(store.get_session_for_msg(2).await.is_some());
        assert!(store.get_session_for_msg(3).await.is_some());
    }

    #[tokio::test]
    async fn reply_promotes_from_l1() {
        let store = SessionStore::new(2, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await; // msg1's session evicted to L1

        store.observe(make_msg(4, "c1", Some(1))).await; // reply promotes back to L0
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2); // original msg1 + reply msg4
    }

    #[tokio::test]
    async fn l2_persistence_and_page_fault() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::with_sqlite(
            dir.path().join("sessions.db"), 2, 2
        ).await.unwrap();

        // Fill L0 and L1, force eviction to L2
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await;
        store.observe(make_msg(4, "c1", None)).await;
        store.observe(make_msg(5, "c1", None)).await; // should push oldest to L2

        // Reply to msg1 (should page fault from L2)
        store.observe(make_msg(6, "c1", Some(1))).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2);
    }

    #[tokio::test]
    async fn l2_get_session_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::with_sqlite(
            dir.path().join("sessions.db"), 2, 2
        ).await.unwrap();

        store.observe(make_msg(1, "c1", None)).await;
        let sid = store.get_session_for_msg(1).await.unwrap().session_id;

        // Force to L2
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await;
        store.observe(make_msg(4, "c1", None)).await;
        store.observe(make_msg(5, "c1", None)).await;

        // Should find via L2
        let s = store.get_session(&sid).await;
        assert!(s.is_some());
    }
}
