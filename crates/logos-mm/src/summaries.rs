use async_trait::async_trait;
use sqlx::SqlitePool;

use logos_vfs::VfsError;
use crate::plugin::MemoryPlugin;

/// Summary tree storage.
///
/// Shares the per-group SQLite pool with MessageDb.
/// Summaries are written by the consolidator (userspace cron) via `logos_write`,
/// and read by agents via `logos_read`. The kernel never generates summaries itself.
pub struct SummaryDb;

impl SummaryDb {
    pub async fn ensure_schema(pool: &SqlitePool) -> Result<(), VfsError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS summaries (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id       TEXT NOT NULL,
                layer         TEXT NOT NULL,
                period_start  TEXT NOT NULL,
                period_end    TEXT NOT NULL,
                source_refs   TEXT NOT NULL DEFAULT '[]',
                content       TEXT NOT NULL,
                generated_at  TEXT NOT NULL
            )",
        )
        .execute(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("init summaries schema: {e}")))?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_summaries_chat_layer
                ON summaries(chat_id, layer, period_start)",
        )
        .execute(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("init summaries index: {e}")))?;

        Ok(())
    }

    /// Write a summary. `content` is JSON with fields:
    /// `layer`, `period_start`, `period_end`, `source_refs`, `content`
    pub async fn write(
        pool: SqlitePool,
        chat_id: &str,
        content: &str,
    ) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(content).map_err(|e| VfsError::InvalidJson(e.to_string()))?;

        let now = super::messages::now_iso8601();
        let layer = val["layer"].as_str().unwrap_or_default().to_string();
        let period_start = val["period_start"].as_str().unwrap_or_default().to_string();
        let period_end = val["period_end"]
            .as_str()
            .unwrap_or(val["period_start"].as_str().unwrap_or_default())
            .to_string();
        let source_refs = if val["source_refs"].is_string() {
            val["source_refs"].as_str().unwrap_or("[]").to_string()
        } else {
            serde_json::to_string(&val["source_refs"]).unwrap_or_else(|_| "[]".to_string())
        };
        let text = val["content"].as_str().unwrap_or_default().to_string();

        sqlx::query(
            "INSERT INTO summaries (chat_id, layer, period_start, period_end, source_refs, content, generated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(chat_id)
        .bind(&layer)
        .bind(&period_start)
        .bind(&period_end)
        .bind(&source_refs)
        .bind(&text)
        .bind(&now)
        .execute(&pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("insert summary: {e}")))?;

        Ok(())
    }

    /// Read a summary by layer + period key. If `period_key` is "latest", returns the most recent.
    pub async fn read(
        pool: SqlitePool,
        chat_id: &str,
        layer: &str,
        period_key: &str,
    ) -> Result<Option<String>, VfsError> {
        let row = if period_key == "latest" {
            sqlx::query(
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2
                 ORDER BY period_start DESC LIMIT 1",
            )
            .bind(chat_id)
            .bind(layer)
            .fetch_optional(&pool)
            .await
        } else if layer == "long" {
            let pattern = format!("{period_key}%");
            sqlx::query(
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2 AND period_start LIKE ?3
                 ORDER BY period_start DESC LIMIT 1",
            )
            .bind(chat_id)
            .bind(layer)
            .bind(&pattern)
            .fetch_optional(&pool)
            .await
        } else {
            sqlx::query(
                "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                 FROM summaries WHERE chat_id = ?1 AND layer = ?2 AND period_start = ?3
                 LIMIT 1",
            )
            .bind(chat_id)
            .bind(layer)
            .bind(period_key)
            .fetch_optional(&pool)
            .await
        };

        let row = row.map_err(|e| VfsError::Sqlite(e.to_string()))?;
        Ok(row.as_ref().map(|r| summary_row_to_json(r).to_string()))
    }
}

/// Hierarchical summary tree as a MemoryPlugin (RFC 003 §4).
pub struct HierarchicalPlugin;

#[async_trait]
impl MemoryPlugin for HierarchicalPlugin {
    fn name(&self) -> &str { "summary" }

    fn docs(&self) -> &str {
        "summary/ : Hierarchical summary tree. Navigate by time.\n\
         - summary/short/latest : most recent hourly summary\n\
         - summary/short/{date}T{hour} : specific hour\n\
         - summary/mid/{date} : daily summary\n\
         - summary/long/{year-month} : monthly summary\n\
         Each node has source_refs pointing to the layer below for drill-down."
    }

    async fn init_schema(&self, pool: &SqlitePool) -> Result<(), VfsError> {
        SummaryDb::ensure_schema(pool).await
    }

    async fn read(&self, pool: &SqlitePool, chat_id: &str, path: &[&str]) -> Result<String, VfsError> {
        // path: [layer, period_key] e.g. ["short", "latest"] or ["mid", "2026-03-20"]
        let layer = path.first().ok_or_else(|| VfsError::InvalidPath("missing layer".to_string()))?;
        let period_key = path.get(1).unwrap_or(&"latest");
        SummaryDb::read(pool.clone(), chat_id, layer, period_key)
            .await
            .map(|opt| opt.unwrap_or_else(|| "null".to_string()))
    }

    async fn write(&self, pool: &SqlitePool, chat_id: &str, _path: &[&str], content: &str) -> Result<(), VfsError> {
        SummaryDb::write(pool.clone(), chat_id, content).await
    }

    async fn patch(&self, pool: &SqlitePool, chat_id: &str, path: &[&str], partial: &str) -> Result<(), VfsError> {
        // Read existing, deep merge, write back
        let existing = self.read(pool, chat_id, path).await?;
        if let (Ok(mut base), Ok(patch_val)) = (
            serde_json::from_str::<serde_json::Value>(&existing),
            serde_json::from_str::<serde_json::Value>(partial),
        ) {
            logos_vfs::json_deep_merge(&mut base, &patch_val);
            let merged = serde_json::to_string(&base).unwrap_or_else(|_| partial.to_string());
            return self.write(pool, chat_id, path, &merged).await;
        }
        self.write(pool, chat_id, path, partial).await
    }
}

fn summary_row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    use sqlx::Row;
    serde_json::json!({
        "id": row.get::<i64, _>("id"),
        "chat_id": row.get::<String, _>("chat_id"),
        "layer": row.get::<String, _>("layer"),
        "period_start": row.get::<String, _>("period_start"),
        "period_end": row.get::<String, _>("period_end"),
        "source_refs": row.get::<String, _>("source_refs"),
        "content": row.get::<String, _>("content"),
        "generated_at": row.get::<String, _>("generated_at"),
    })
}
