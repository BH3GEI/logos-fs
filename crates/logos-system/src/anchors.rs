use std::sync::Arc;

use rusqlite::Connection;

use logos_vfs::VfsError;

pub struct AnchorDb {
    pub(crate) conn: Arc<std::sync::Mutex<Connection>>,
}

impl AnchorDb {
    pub fn new(conn: Arc<std::sync::Mutex<Connection>>) -> Result<Self, VfsError> {
        {
            let c = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            init_schema(&c)?;
        }
        Ok(Self { conn })
    }

    pub async fn list(&self, task_id: &str) -> Result<String, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, task_id, summary, facts, created_at
                     FROM anchors WHERE task_id = ?1 ORDER BY created_at ASC",
                )
                .map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let rows: Vec<serde_json::Value> = stmt
                .query_map(rusqlite::params![task_id], anchor_row_to_json)
                .map_err(|e| VfsError::Sqlite(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub async fn get(&self, task_id: &str, anchor_id: &str) -> Result<Option<String>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let anchor_id = anchor_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let result = conn
                .query_row(
                    "SELECT id, task_id, summary, facts, created_at
                     FROM anchors WHERE task_id = ?1 AND id = ?2",
                    rusqlite::params![task_id, anchor_id],
                    anchor_row_to_json,
                )
                .ok();
            Ok(result.map(|v| v.to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub(crate) fn create_sync(
        conn: &Connection,
        task_id: &str,
        summary: &str,
        facts: &str,
    ) -> Result<String, VfsError> {
        let anchor_id = chrono::Utc::now().format("%Y-%m-%dT%H:%M").to_string();
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        conn.execute(
            "INSERT INTO anchors (id, task_id, summary, facts, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![anchor_id, task_id, summary, facts, now],
        )
        .map_err(|e| VfsError::Sqlite(format!("insert anchor: {e}")))?;

        // Sync FTS index for L1 experience retrieval (RFC 003 §6.2)
        conn.execute(
            "INSERT INTO anchors_fts(rowid, facts)
             SELECT rowid, facts FROM anchors WHERE id = ?1 AND task_id = ?2",
            rusqlite::params![anchor_id, task_id],
        )
        .map_err(|e| VfsError::Sqlite(format!("sync anchor FTS: {e}")))?;

        Ok(anchor_id)
    }
}

fn anchor_row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<serde_json::Value> {
    Ok(serde_json::json!({
        "id": row.get::<_, String>(0)?,
        "task_id": row.get::<_, String>(1)?,
        "summary": row.get::<_, String>(2)?,
        "facts": row.get::<_, String>(3)?,
        "created_at": row.get::<_, String>(4)?,
    }))
}

fn init_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS anchors (
            id         TEXT NOT NULL,
            task_id    TEXT NOT NULL,
            summary    TEXT NOT NULL,
            facts      TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, id)
        );
        CREATE INDEX IF NOT EXISTS idx_anchors_task ON anchors(task_id, created_at);
        CREATE VIRTUAL TABLE IF NOT EXISTS anchors_fts USING fts5(facts, content='anchors', content_rowid='rowid');",
    )
    .map_err(|e| VfsError::Sqlite(format!("init anchors schema: {e}")))?;
    Ok(())
}
