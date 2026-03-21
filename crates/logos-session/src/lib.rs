//! Session Clustering Module — RFC 003 §5
//!
//! Implements social-topology-first session clustering with a three-layer
//! LRU memory hierarchy (L0 active → L1 inactive → L2 archived to LanceDB).
//!
//! Core insight (RFC 003 §5.1): reply chains are hard bindings;
//! semantic similarity is only a fallback when topology is absent.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::Connection as LanceConnection;
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
        let messages: Vec<serde_json::Value> = self
            .messages
            .iter()
            .map(|m| {
                serde_json::json!({
                    "msg_id": m.msg_id,
                    "chat_id": m.chat_id,
                    "speaker": m.speaker,
                    "text": m.text,
                    "ts": m.ts,
                    "reply_to": m.reply_to,
                })
            })
            .collect();
        serde_json::json!({
            "session_id": self.session_id,
            "chat_id": self.chat_id,
            "message_count": self.messages.len(),
            "last_active": self.last_active.to_rfc3339(),
            "messages": messages,
        })
    }
}

/// Internal state protected by a single mutex to avoid deadlocks.
struct Inner {
    /// msg_id → session_id
    msg_index: HashMap<i64, String>,
    l0: HashMap<String, Session>,
    l1: HashMap<String, Session>,
    l0_capacity: usize,
    l1_capacity: usize,
}

impl Inner {
    fn find_session_mut(&mut self, session_id: &str) -> Option<(&mut Session, u8)> {
        if let Some(s) = self.l0.get_mut(session_id) {
            return Some((s, 0));
        }
        if let Some(s) = self.l1.get_mut(session_id) {
            return Some((s, 1));
        }
        None
    }

    fn promote_to_l0(&mut self, session_id: &str) {
        if let Some(s) = self.l1.remove(session_id) {
            self.l0.insert(session_id.to_string(), s);
        }
    }

    fn get_session(&self, session_id: &str) -> Option<Session> {
        self.l0
            .get(session_id)
            .or_else(|| self.l1.get(session_id))
            .cloned()
    }
}

/// Three-layer LRU session store (RFC 003 §5.2).
///
/// - L0: Active sessions (recently replied to) — in memory
/// - L1: Inactive sessions (demoted from L0 by LRU) — in memory
/// - L2: Archived sessions — persisted to LanceDB
pub struct SessionStore {
    inner: Mutex<Inner>,
    lance: Option<LanceConnection>,
}

const SESSIONS_TABLE: &str = "sessions";

impl SessionStore {
    pub fn new(l0_capacity: usize, l1_capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                l0: HashMap::new(),
                l1: HashMap::new(),
                l0_capacity,
                l1_capacity,
            }),
            lance: None,
        }
    }

    /// Create a session store with LanceDB L2 backend.
    pub async fn with_lance(
        l0_capacity: usize,
        l1_capacity: usize,
        db_path: &str,
    ) -> Result<Self, logos_vfs::VfsError> {
        let conn = lancedb::connect(db_path)
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb connect: {e}")))?;

        // Create sessions table if it doesn't exist
        let tables = conn
            .table_names()
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb list tables: {e}")))?;

        if !tables.contains(&SESSIONS_TABLE.to_string()) {
            let schema = sessions_schema();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["__init__"])),
                    Arc::new(StringArray::from(vec![""])),
                    Arc::new(StringArray::from(vec!["{}"])),
                    Arc::new(StringArray::from(vec![""])),
                ],
            )
            .map_err(|e| logos_vfs::VfsError::Io(format!("create init batch: {e}")))?;
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
            let table = conn
                .create_table(SESSIONS_TABLE, reader)
                .execute()
                .await
                .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb create table: {e}")))?;
            table
                .delete("session_id = '__init__'")
                .await
                .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb cleanup init: {e}")))?;
        }

        Ok(Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                l0: HashMap::new(),
                l1: HashMap::new(),
                l0_capacity,
                l1_capacity,
            }),
            lance: Some(conn),
        })
    }

    /// Observe a new message and update session topology.
    pub async fn observe(&self, msg: MsgRef) {
        let mut inner = self.inner.lock().await;
        let msg_id = msg.msg_id;

        if let Some(reply_to) = msg.reply_to {
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
                self.evict_if_needed(&mut inner).await;
                return;
            }

            // Page fault: check L2 (LanceDB)
            if let Some(ref lance) = self.lance {
                if let Ok(Some(mut session)) = lance_load_by_msg(lance, reply_to).await {
                    for m in &session.messages {
                        inner.msg_index.insert(m.msg_id, session.session_id.clone());
                    }
                    session.add_message(msg);
                    inner.msg_index.insert(msg_id, session.session_id.clone());
                    let sid = session.session_id.clone();
                    let _ = lance_delete(lance, &sid).await;
                    inner.l0.insert(sid, session);
                    self.evict_if_needed(&mut inner).await;
                    return;
                }
            }
        }

        let chat_id = msg.chat_id.clone();
        let session = Session::new(&chat_id, msg);
        let sid = session.session_id.clone();
        inner.msg_index.insert(msg_id, sid.clone());
        inner.l0.insert(sid, session);
        self.evict_if_needed(&mut inner).await;
    }

    async fn evict_if_needed(&self, inner: &mut Inner) {
        while inner.l0.len() > inner.l0_capacity {
            let oldest = inner
                .l0
                .iter()
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
        while inner.l1.len() > inner.l1_capacity {
            let oldest = inner
                .l1
                .iter()
                .min_by_key(|(_, s)| s.last_active)
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest {
                if let Some(s) = inner.l1.remove(&key) {
                    if let Some(ref lance) = self.lance {
                        let _ = lance_persist(lance, &s).await;
                    }
                    inner.msg_index.retain(|_, v| *v != key);
                }
            } else {
                break;
            }
        }
    }

    pub async fn get_session_for_msg(&self, msg_id: i64) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(sid) = inner.msg_index.get(&msg_id) {
            if let Some(s) = inner.get_session(sid) {
                return Some(s);
            }
        }
        if let Some(ref lance) = self.lance {
            if let Ok(Some(s)) = lance_load_by_msg(lance, msg_id).await {
                return Some(s);
            }
        }
        None
    }

    pub async fn get_session(&self, session_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(s) = inner.get_session(session_id) {
            return Some(s);
        }
        if let Some(ref lance) = self.lance {
            if let Ok(Some(s)) = lance_load_by_id(lance, session_id).await {
                return Some(s);
            }
        }
        None
    }

    pub async fn get_active_session(&self, chat_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        inner
            .l0
            .values()
            .filter(|s| s.chat_id == chat_id)
            .max_by_key(|s| s.last_active)
            .cloned()
    }
}

// --- L2 LanceDB persistence ---

fn sessions_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("session_id", DataType::Utf8, false),
        Field::new("chat_id", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("last_active", DataType::Utf8, false),
    ]))
}

async fn lance_persist(
    conn: &LanceConnection,
    session: &Session,
) -> Result<(), lancedb::Error> {
    let data = serde_json::to_string(session).unwrap_or_default();
    let last_active = session.last_active.to_rfc3339();

    // Delete existing row first (upsert)
    let _ = lance_delete(conn, &session.session_id).await;

    let schema = sessions_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![session.session_id.as_str()])),
            Arc::new(StringArray::from(vec![session.chat_id.as_str()])),
            Arc::new(StringArray::from(vec![data.as_str()])),
            Arc::new(StringArray::from(vec![last_active.as_str()])),
        ],
    )
    .map_err(|e| lancedb::Error::Arrow {
        source: arrow_schema::ArrowError::InvalidArgumentError(e.to_string()),
    })?;

    let table = conn.open_table(SESSIONS_TABLE).execute().await?;
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    table.add(reader).execute().await?;
    Ok(())
}

async fn lance_delete(
    conn: &LanceConnection,
    session_id: &str,
) -> Result<(), lancedb::Error> {
    let table = conn.open_table(SESSIONS_TABLE).execute().await?;
    table
        .delete(&format!("session_id = '{session_id}'"))
        .await?;
    Ok(())
}

async fn lance_load_by_id(
    conn: &LanceConnection,
    session_id: &str,
) -> Result<Option<Session>, lancedb::Error> {
    let table = conn.open_table(SESSIONS_TABLE).execute().await?;
    let results = table
        .query()
        .only_if(format!("session_id = '{session_id}'"))
        .limit(1)
        .execute()
        .await?;

    use futures_util::TryStreamExt;
    let batches: Vec<RecordBatch> = results.try_collect().await?;

    for batch in &batches {
        if batch.num_rows() > 0 {
            let data_col = batch
                .column_by_name("data")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            if let Some(arr) = data_col {
                if let Some(data) = arr.iter().next().flatten() {
                    return Ok(serde_json::from_str(data).ok());
                }
            }
        }
    }
    Ok(None)
}

async fn lance_load_by_msg(
    conn: &LanceConnection,
    msg_id: i64,
) -> Result<Option<Session>, lancedb::Error> {
    // Scan all sessions and check for msg_id in data.
    // LanceDB SQL filter on JSON substring:
    let table = conn.open_table(SESSIONS_TABLE).execute().await?;
    let results = table
        .query()
        .only_if(format!("data LIKE '%\"msg_id\":{msg_id}%'"))
        .execute()
        .await?;

    use futures_util::TryStreamExt;
    let batches: Vec<RecordBatch> = results.try_collect().await?;

    for batch in &batches {
        let data_col = batch
            .column_by_name("data")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        if let Some(arr) = data_col {
            for data in arr.iter().flatten() {
                if let Ok(session) = serde_json::from_str::<Session>(data) {
                    if session.messages.iter().any(|m| m.msg_id == msg_id) {
                        return Ok(Some(session));
                    }
                }
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(msg_id: i64, chat_id: &str, reply_to: Option<i64>) -> MsgRef {
        MsgRef {
            msg_id,
            chat_id: chat_id.to_string(),
            reply_to,
            text: format!("msg-{msg_id}"),
            speaker: "user".to_string(),
            ts: "2026-03-20T10:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn new_message_creates_session() {
        let store = SessionStore::new(10, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        let session = store.get_session_for_msg(1).await;
        assert!(session.is_some());
        assert_eq!(session.unwrap().messages.len(), 1);
    }

    #[tokio::test]
    async fn reply_joins_existing_session() {
        let store = SessionStore::new(10, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", Some(1))).await;

        let s1 = store.get_session_for_msg(1).await.unwrap();
        let s2 = store.get_session_for_msg(2).await.unwrap();
        assert_eq!(s1.session_id, s2.session_id);
        assert_eq!(s1.messages.len(), 2);
    }

    #[tokio::test]
    async fn no_reply_creates_separate_session() {
        let store = SessionStore::new(10, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", None)).await;

        let s1 = store.get_session_for_msg(1).await.unwrap();
        let s2 = store.get_session_for_msg(2).await.unwrap();
        assert_ne!(s1.session_id, s2.session_id);
    }

    #[tokio::test]
    async fn lru_eviction_l0_to_l1() {
        let store = SessionStore::new(2, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", None)).await;
        store.observe(make_msg(3, "chat-1", None)).await;

        assert!(store.get_session_for_msg(1).await.is_some());
        assert!(store.get_session_for_msg(2).await.is_some());
        assert!(store.get_session_for_msg(3).await.is_some());
    }

    #[tokio::test]
    async fn reply_promotes_from_l1() {
        let store = SessionStore::new(1, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", None)).await;

        store.observe(make_msg(3, "chat-1", Some(1))).await;

        let session = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(session.messages.len(), 2);
    }

    #[tokio::test]
    async fn deep_reply_chain() {
        let store = SessionStore::new(10, 10);
        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", Some(1))).await;
        store.observe(make_msg(3, "chat-1", Some(2))).await;

        let session = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(session.messages.len(), 3);
    }

    #[tokio::test]
    async fn l2_persistence_and_page_fault() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test-sessions");
        let store = SessionStore::with_lance(1, 1, db_path.to_str().unwrap())
            .await
            .unwrap();

        store.observe(make_msg(1, "chat-1", None)).await;
        store.observe(make_msg(2, "chat-1", None)).await;
        store.observe(make_msg(3, "chat-1", None)).await;

        store.observe(make_msg(4, "chat-1", Some(1))).await;
        let session = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(session.messages.len(), 2);
    }

    #[tokio::test]
    async fn l2_get_session_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test-sessions-2");
        let store = SessionStore::with_lance(1, 1, db_path.to_str().unwrap())
            .await
            .unwrap();

        store.observe(make_msg(1, "chat-1", None)).await;
        let sid = store.get_session_for_msg(1).await.unwrap().session_id;

        store.observe(make_msg(2, "chat-1", None)).await;
        store.observe(make_msg(3, "chat-1", None)).await;

        let session = store.get_session(&sid).await;
        assert!(session.is_some());
        assert_eq!(session.unwrap().session_id, sid);
    }
}
