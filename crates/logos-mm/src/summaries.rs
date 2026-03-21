use std::sync::Arc;

use rusqlite::Connection;

use logos_vfs::VfsError;

/// Summary tree storage.
///
/// Shares the per-group SQLite connection with MessageDb.
/// Summaries are written by the consolidator (userspace cron) via `logos_write`,
/// and read by agents via `logos_read`. The kernel never generates summaries itself.
pub struct SummaryDb;

impl SummaryDb {
    pub fn ensure_schema(conn: &Connection) -> Result<(), VfsError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS summaries (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id       TEXT NOT NULL,
                layer         TEXT NOT NULL,
                period_start  TEXT NOT NULL,
                period_end    TEXT NOT NULL,
                source_refs   TEXT NOT NULL DEFAULT '[]',
                content       TEXT NOT NULL,
                generated_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_summaries_chat_layer
                ON summaries(chat_id, layer, period_start);",
        )
        .map_err(|e| VfsError::Sqlite(format!("init summaries schema: {e}")))?;
        Ok(())
    }

    /// Write a summary. `content` is JSON with fields:
    /// `layer`, `period_start`, `period_end`, `source_refs`, `content`
    pub async fn write(
        conn: Arc<std::sync::Mutex<Connection>>,
        chat_id: &str,
        content: &str,
    ) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(content).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let chat_id = chat_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let now = super::messages::now_iso8601();
            let layer = val["layer"].as_str().unwrap_or_default();
            let period_start = val["period_start"].as_str().unwrap_or_default();
            let period_end = val["period_end"].as_str().unwrap_or(period_start);
            let source_refs = if val["source_refs"].is_string() {
                val["source_refs"].as_str().unwrap_or("[]").to_string()
            } else {
                serde_json::to_string(&val["source_refs"]).unwrap_or_else(|_| "[]".to_string())
            };
            let text = val["content"].as_str().unwrap_or_default();

            conn.execute(
                "INSERT INTO summaries (chat_id, layer, period_start, period_end, source_refs, content, generated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![chat_id, layer, period_start, period_end, source_refs, text, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert summary: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    /// Read a summary by layer + period key. If `period_key` is "latest", returns the most recent.
    pub async fn read(
        conn: Arc<std::sync::Mutex<Connection>>,
        chat_id: &str,
        layer: &str,
        period_key: &str,
    ) -> Result<Option<String>, VfsError> {
        let chat_id = chat_id.to_string();
        let layer = layer.to_string();
        let period_key = period_key.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let sql = if period_key == "latest" {
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2
                 ORDER BY period_start DESC LIMIT 1"
            } else if layer == "long" {
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2 AND period_start LIKE ?3 || '%'
                 ORDER BY period_start DESC LIMIT 1"
            } else {
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2 AND period_start = ?3
                 LIMIT 1"
            };

            let mut stmt = conn.prepare_cached(sql).map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let params: Vec<Box<dyn rusqlite::types::ToSql>> = if period_key == "latest" {
                vec![Box::new(chat_id), Box::new(layer)]
            } else {
                vec![Box::new(chat_id), Box::new(layer), Box::new(period_key)]
            };
            let result = stmt
                .query_row(rusqlite::params_from_iter(params.iter()), |row| {
                    Ok(serde_json::json!({
                        "id": row.get::<_, i64>(0)?,
                        "chat_id": row.get::<_, String>(1)?,
                        "layer": row.get::<_, String>(2)?,
                        "period_start": row.get::<_, String>(3)?,
                        "period_end": row.get::<_, String>(4)?,
                        "source_refs": row.get::<_, String>(5)?,
                        "content": row.get::<_, String>(6)?,
                        "generated_at": row.get::<_, String>(7)?,
                    }))
                })
                .ok();
            Ok(result.map(|v| v.to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }
}
