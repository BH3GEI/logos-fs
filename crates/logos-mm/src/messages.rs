use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use logos_vfs::VfsError;

const RANGE_FETCH_MAX_LIMIT: i64 = 200;
const FTS_MAX_LIMIT: i64 = 50;

/// Per-group SQLite message store.
///
/// Each group gets its own database file under `db_root/{chat_id}.db`.
/// Messages are append-only (the lossless raw archive).
pub struct MessageDb {
    db_root: PathBuf,
    connections: Mutex<HashMap<String, Arc<std::sync::Mutex<Connection>>>>,
}

impl MessageDb {
    pub fn new(db_root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&db_root)
            .map_err(|e| VfsError::Io(format!("create memory dir: {e}")))?;
        Ok(Self {
            db_root,
            connections: Mutex::new(HashMap::new()),
        })
    }

    /// Get or create a pooled connection for a chat_id.
    /// Also used by SummaryDb to share the same per-group connection.
    pub(crate) async fn conn(&self, chat_id: &str) -> Result<Arc<std::sync::Mutex<Connection>>, VfsError> {
        let mut map = self.connections.lock().await;
        if let Some(c) = map.get(chat_id) {
            return Ok(Arc::clone(c));
        }
        let db_path = self.db_root.join(format!("{chat_id}.db"));
        let conn = Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open {}: {e}", db_path.display())))?;
        init_schema(&conn)?;
        let arc = Arc::new(std::sync::Mutex::new(conn));
        map.insert(chat_id.to_string(), Arc::clone(&arc));
        Ok(arc)
    }

    /// Insert a message. `content` is a JSON object with fields:
    /// `external_id`, `ts`, `chat_id`, `speaker`, `reply_to_external_id`, `text`, `mentions`, `session_id`
    pub async fn insert(&self, chat_id: &str, content: &str) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(content).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let conn = self.conn(chat_id).await?;
        let chat_id = chat_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let now = now_iso8601();
            let external_id = val["external_id"].as_str().unwrap_or_default();
            let ts = val["ts"].as_str().unwrap_or(&now);
            let speaker = val["speaker"].as_str().unwrap_or_default();
            let text = val["text"].as_str().unwrap_or_default();
            let mentions = val["mentions"].as_array()
                .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "[]".to_string()))
                .unwrap_or_else(|| "[]".to_string());
            let session_id = val["session_id"].as_str().unwrap_or_default();

            conn.execute(
                "INSERT INTO messages (external_id, ts, chat_id, speaker, text, mentions, session_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![external_id, ts, &chat_id, speaker, text, mentions, session_id],
            ).map_err(|e| VfsError::Sqlite(format!("insert: {e}")))?;

            let msg_id: i64 = conn.last_insert_rowid();

            // Resolve reply_to external_id → internal msg_id
            let reply_ext = val["reply_to_external_id"].as_str().unwrap_or_default();
            if !reply_ext.is_empty() {
                let resolved: Option<i64> = conn
                    .query_row(
                        "SELECT msg_id FROM messages WHERE chat_id = ?1 AND external_id = ?2 LIMIT 1",
                        rusqlite::params![chat_id, reply_ext],
                        |row| row.get(0),
                    )
                    .ok();
                if let Some(ref_id) = resolved {
                    conn.execute(
                        "UPDATE messages SET reply_to = ?1 WHERE msg_id = ?2",
                        rusqlite::params![ref_id, msg_id],
                    ).map_err(|e| VfsError::Sqlite(format!("update reply_to: {e}")))?;
                }
            }

            // Sync FTS
            conn.execute(
                "INSERT INTO messages_fts(rowid, text) VALUES (?1, ?2)",
                rusqlite::params![msg_id, text],
            ).map_err(|e| VfsError::Sqlite(format!("fts sync: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    /// Get a single message by msg_id.
    pub async fn get_by_id(&self, chat_id: &str, msg_id: i64) -> Result<Option<String>, VfsError> {
        let conn = self.conn(chat_id).await?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                     FROM messages WHERE msg_id = ?1",
                )
                .map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let result = stmt
                .query_row(rusqlite::params![msg_id], |row| {
                    Ok(serde_json::json!({
                        "msg_id": row.get::<_, i64>(0)?,
                        "external_id": row.get::<_, String>(1)?,
                        "ts": row.get::<_, String>(2)?,
                        "chat_id": row.get::<_, String>(3)?,
                        "speaker": row.get::<_, String>(4)?,
                        "reply_to": row.get::<_, Option<i64>>(5)?,
                        "text": row.get::<_, String>(6)?,
                        "mentions": row.get::<_, String>(7)?,
                        "session_id": row.get::<_, String>(8)?,
                    }))
                })
                .ok();
            Ok(result.map(|v| v.to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    /// FTS search. Returns JSON array of matching messages.
    pub async fn search_fts(
        &self,
        chat_id: &str,
        query: &str,
        limit: i64,
    ) -> Result<String, VfsError> {
        let conn = self.conn(chat_id).await?;
        let query = query.to_string();
        let limit = limit.clamp(1, FTS_MAX_LIMIT);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT m.msg_id, m.external_id, m.ts, m.chat_id, m.speaker, m.reply_to, m.text, m.mentions, m.session_id
                     FROM messages_fts f
                     JOIN messages m ON m.msg_id = f.rowid
                     WHERE messages_fts MATCH ?1
                     ORDER BY f.rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let rows: Vec<serde_json::Value> = stmt
                .query_map(rusqlite::params![query, limit], |row| {
                    Ok(serde_json::json!({
                        "msg_id": row.get::<_, i64>(0)?,
                        "external_id": row.get::<_, String>(1)?,
                        "ts": row.get::<_, String>(2)?,
                        "chat_id": row.get::<_, String>(3)?,
                        "speaker": row.get::<_, String>(4)?,
                        "reply_to": row.get::<_, Option<i64>>(5)?,
                        "text": row.get::<_, String>(6)?,
                        "mentions": row.get::<_, String>(7)?,
                        "session_id": row.get::<_, String>(8)?,
                    }))
                })
                .map_err(|e| VfsError::Sqlite(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    /// Range fetch: return messages within msg_id ranges. `params` is JSON:
    /// `{ "ranges": [[start, end], ...], "limit": N, "offset": N }`
    pub async fn range_fetch(&self, chat_id: &str, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let ranges: Vec<(i64, i64)> = val["ranges"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|r| {
                let arr = r.as_array()?;
                Some((arr.first()?.as_i64()?, arr.get(1)?.as_i64()?))
            })
            .collect();
        if ranges.is_empty() {
            return Ok("[]".to_string());
        }
        let limit = val["limit"].as_i64().unwrap_or(50).clamp(1, RANGE_FETCH_MAX_LIMIT);
        let offset = val["offset"].as_i64().unwrap_or(0).max(0);
        let conn = self.conn(chat_id).await?;

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let where_clauses: Vec<String> = ranges
                .iter()
                .map(|(s, e)| format!("(msg_id >= {s} AND msg_id <= {e})"))
                .collect();
            let sql = format!(
                "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                 FROM messages WHERE ({}) ORDER BY msg_id ASC LIMIT {} OFFSET {}",
                where_clauses.join(" OR "),
                limit,
                offset
            );
            let mut stmt = conn.prepare(&sql).map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let rows: Vec<serde_json::Value> = stmt
                .query_map([], |row| {
                    Ok(serde_json::json!({
                        "msg_id": row.get::<_, i64>(0)?,
                        "external_id": row.get::<_, String>(1)?,
                        "ts": row.get::<_, String>(2)?,
                        "chat_id": row.get::<_, String>(3)?,
                        "speaker": row.get::<_, String>(4)?,
                        "reply_to": row.get::<_, Option<i64>>(5)?,
                        "text": row.get::<_, String>(6)?,
                        "mentions": row.get::<_, String>(7)?,
                        "session_id": row.get::<_, String>(8)?,
                    }))
                })
                .map_err(|e| VfsError::Sqlite(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }
}

fn init_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS messages (
            msg_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id TEXT NOT NULL DEFAULT '',
            ts          TEXT NOT NULL,
            chat_id     TEXT NOT NULL,
            speaker     TEXT NOT NULL,
            reply_to    INTEGER REFERENCES messages(msg_id),
            text        TEXT NOT NULL,
            mentions    TEXT NOT NULL DEFAULT '[]',
            session_id  TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts);
        CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to);
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(text, content='messages', content_rowid='msg_id');",
    )
    .map_err(|e| VfsError::Sqlite(format!("init messages schema: {e}")))?;
    Ok(())
}

pub(crate) fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
