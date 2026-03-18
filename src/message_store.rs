use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::service::VfsError;

const RANGE_FETCH_DEFAULT_LIMIT: i32 = 50;
const RANGE_FETCH_MAX_LIMIT: i32 = 200;
const FTS_DEFAULT_LIMIT: i32 = 10;
const FTS_MAX_LIMIT: i32 = 50;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub msg_id: i64,
    pub external_id: String,
    pub ts: String,
    pub chat_id: String,
    pub speaker: String,
    pub reply_to: String,
    pub text: String,
    pub mentions: Vec<String>,
    pub session_id: String,
}

pub struct MessageStore {
    db_root: PathBuf,
    connections: Mutex<HashMap<String, Arc<std::sync::Mutex<Connection>>>>,
}

impl MessageStore {
    pub fn new(db_root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&db_root)?;
        Ok(Self {
            db_root,
            connections: Mutex::new(HashMap::new()),
        })
    }

    pub async fn insert_messages(
        &self,
        chat_id: &str,
        session_id: &str,
        messages: &[InsertMessage],
    ) -> Result<(), VfsError> {
        if messages.is_empty() {
            return Ok(());
        }
        let conn = self.get_connection(chat_id).await?;
        let session_id = session_id.to_string();
        let messages: Vec<InsertMessage> = messages.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;
            let tx = conn.unchecked_transaction().map_err(|e| {
                VfsError::Sqlite(format!("begin transaction failed: {e}"))
            })?;

            {
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO messages (external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare insert failed: {e}")))?;

                for msg in &messages {
                    let mentions_json = serde_json::to_string(&msg.mentions)
                        .unwrap_or_else(|_| "[]".to_string());
                    let reply_to: Option<&str> = if msg.reply_to.is_empty() {
                        None
                    } else {
                        Some(&msg.reply_to)
                    };
                    stmt.execute(rusqlite::params![
                        msg.external_id,
                        msg.ts,
                        msg.chat_id,
                        msg.speaker,
                        reply_to,
                        msg.text,
                        mentions_json,
                        session_id,
                    ])
                    .map_err(|e| VfsError::Sqlite(format!("insert message failed: {e}")))?;
                }
            }

            sync_fts_for_session(&tx, &session_id)?;

            tx.commit()
                .map_err(|e| VfsError::Sqlite(format!("commit failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_messages_by_session(
        &self,
        chat_id: &str,
        session_id: &str,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let session_id = session_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                     FROM messages WHERE session_id = ?1 ORDER BY msg_id ASC",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![session_id], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn range_fetch(
        &self,
        chat_id: &str,
        ranges: &[(i64, i64)],
        limit: i32,
        offset: i32,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        if chat_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "chat_id cannot be empty".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let limit = clamp(limit, 1, RANGE_FETCH_DEFAULT_LIMIT, RANGE_FETCH_MAX_LIMIT);
        let offset = offset.max(0);

        let conn = self.get_connection(chat_id).await?;
        let ranges = ranges.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;

            let mut conditions = Vec::with_capacity(ranges.len());
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            for (start, end) in &ranges {
                let base = params.len();
                conditions.push(format!("(msg_id >= ?{} AND msg_id <= ?{})", base + 1, base + 2));
                params.push(Box::new(*start));
                params.push(Box::new(*end));
            }
            let where_clause = conditions.join(" OR ");
            let sql = format!(
                "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                 FROM messages WHERE ({}) ORDER BY msg_id ASC LIMIT ?{} OFFSET ?{}",
                where_clause,
                params.len() + 1,
                params.len() + 2,
            );
            params.push(Box::new(limit));
            params.push(Box::new(offset));

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| VfsError::Sqlite(format!("prepare range_fetch failed: {e}")))?;
            let rows = stmt
                .query_map(param_refs.as_slice(), row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("range_fetch query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn search_fts(
        &self,
        chat_id: &str,
        query: &str,
        limit: i32,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        if chat_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "chat_id cannot be empty for FTS search".to_string(),
            ));
        }
        if query.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "query cannot be empty for FTS search".to_string(),
            ));
        }

        let limit = clamp(limit, 1, FTS_DEFAULT_LIMIT, FTS_MAX_LIMIT);
        let conn = self.get_connection(chat_id).await?;
        let query = query.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;

            let mut stmt = conn
                .prepare_cached(
                    "SELECT m.msg_id, m.external_id, m.ts, m.chat_id, m.speaker, m.reply_to, m.text, m.mentions, m.session_id
                     FROM messages_fts fts
                     JOIN messages m ON fts.rowid = m.msg_id
                     WHERE messages_fts MATCH ?1
                     ORDER BY rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare FTS query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![query, limit], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("FTS query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    async fn get_connection(
        &self,
        chat_id: &str,
    ) -> Result<Arc<std::sync::Mutex<Connection>>, VfsError> {
        let mut map = self.connections.lock().await;
        if let Some(conn) = map.get(chat_id) {
            return Ok(Arc::clone(conn));
        }

        let db_path = self.db_root.join(format!("{chat_id}.db"));
        let conn = Connection::open(&db_path).map_err(|e| {
            VfsError::Sqlite(format!(
                "open sqlite {}: {e}",
                db_path.display()
            ))
        })?;
        init_schema(&conn)?;
        let conn = Arc::new(std::sync::Mutex::new(conn));
        map.insert(chat_id.to_string(), Arc::clone(&conn));
        Ok(conn)
    }
}

#[derive(Debug, Clone)]
pub struct InsertMessage {
    pub external_id: String,
    pub ts: String,
    pub chat_id: String,
    pub speaker: String,
    pub reply_to: String,
    pub text: String,
    pub mentions: Vec<String>,
}

fn init_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;

         CREATE TABLE IF NOT EXISTS messages (
           msg_id       INTEGER PRIMARY KEY AUTOINCREMENT,
           external_id  TEXT NOT NULL,
           ts           TEXT NOT NULL,
           chat_id      TEXT NOT NULL,
           speaker      TEXT NOT NULL,
           reply_to     TEXT,
           text         TEXT NOT NULL,
           mentions     TEXT,
           session_id   TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts);
         CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
         CREATE INDEX IF NOT EXISTS idx_messages_external ON messages(chat_id, external_id);

         CREATE TABLE IF NOT EXISTS summaries (
           id            INTEGER PRIMARY KEY AUTOINCREMENT,
           chat_id       TEXT NOT NULL,
           layer         TEXT NOT NULL,
           period_start  TEXT NOT NULL,
           period_end    TEXT NOT NULL,
           source_refs   TEXT NOT NULL,
           content       TEXT NOT NULL,
           generated_at  TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_summaries_chat_layer ON summaries(chat_id, layer, period_start);

         CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(text, content=messages, content_rowid=msg_id);",
    )
    .map_err(|e| VfsError::Sqlite(format!("init schema failed: {e}")))
}

fn sync_fts_for_session(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
) -> Result<(), VfsError> {
    tx.execute(
        "INSERT INTO messages_fts(messages_fts) VALUES('rebuild')",
        [],
    )
    .map_err(|e| {
        VfsError::Sqlite(format!(
            "FTS rebuild after session {session_id} failed: {e}"
        ))
    })?;
    Ok(())
}

fn row_to_stored_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    let mentions_raw: String = row.get(7)?;
    let mentions: Vec<String> = serde_json::from_str(&mentions_raw).unwrap_or_default();
    Ok(StoredMessage {
        msg_id: row.get(0)?,
        external_id: row.get(1)?,
        ts: row.get(2)?,
        chat_id: row.get(3)?,
        speaker: row.get(4)?,
        reply_to: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
        text: row.get(6)?,
        mentions,
        session_id: row.get(8)?,
    })
}

fn collect_rows(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage>>,
) -> Result<Vec<StoredMessage>, VfsError> {
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
}

fn clamp(value: i32, min: i32, default: i32, max: i32) -> i32 {
    if value <= 0 {
        return default;
    }
    value.max(min).min(max)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn make_test_store(dir: &Path) -> MessageStore {
        MessageStore::new(dir.to_path_buf()).unwrap()
    }

    fn make_messages(n: usize, chat_id: &str) -> Vec<InsertMessage> {
        (0..n)
            .map(|i| InsertMessage {
                external_id: format!("ext-{i}"),
                ts: format!("2026-03-18T{:02}:00:00Z", i % 24),
                chat_id: chat_id.to_string(),
                speaker: format!("user-{}", i % 3),
                reply_to: String::new(),
                text: format!("message number {i} about rust and databases"),
                mentions: vec![],
            })
            .collect()
    }

    #[tokio::test]
    async fn insert_and_get_by_session() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(5, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .get_messages_by_session("chat-1", "session-a")
            .await
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].external_id, "ext-0");
        assert_eq!(result[4].external_id, "ext-4");
        assert_eq!(result[0].session_id, "session-a");
    }

    #[tokio::test]
    async fn range_fetch_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 5)], 50, 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].msg_id, 1);
        assert_eq!(result[4].msg_id, 5);
    }

    #[tokio::test]
    async fn range_fetch_multiple_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 3), (7, 9)], 50, 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 6);
    }

    #[tokio::test]
    async fn range_fetch_with_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 10)], 3, 2)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].msg_id, 3);
    }

    #[tokio::test]
    async fn fts_search() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(5, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .search_fts("chat-1", "rust databases", 10)
            .await
            .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn per_group_isolation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store
            .insert_messages("chat-1", "s1", &make_messages(3, "chat-1"))
            .await
            .unwrap();
        store
            .insert_messages("chat-2", "s2", &make_messages(5, "chat-2"))
            .await
            .unwrap();

        let r1 = store
            .get_messages_by_session("chat-1", "s1")
            .await
            .unwrap();
        let r2 = store
            .get_messages_by_session("chat-2", "s2")
            .await
            .unwrap();
        assert_eq!(r1.len(), 3);
        assert_eq!(r2.len(), 5);

        let r_cross = store
            .get_messages_by_session("chat-1", "s2")
            .await
            .unwrap();
        assert_eq!(r_cross.len(), 0);
    }

    #[tokio::test]
    async fn empty_ranges_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let result = store.range_fetch("chat-1", &[], 50, 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fts_empty_query_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let err = store.search_fts("chat-1", "  ", 10).await;
        assert!(err.is_err());
    }
}
