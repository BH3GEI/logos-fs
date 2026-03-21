use async_trait::async_trait;
use sqlx::SqlitePool;

use logos_vfs::VfsError;

/// A pluggable memory view — custom query over chat data.
///
/// RFC 003 §11: view plugins provide filtered/aggregated views
/// without agents needing to construct complex queries.
#[async_trait]
pub trait MemoryView: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    async fn query(
        &self,
        pool: &SqlitePool,
        chat_id: &str,
        params: &str,
    ) -> Result<String, VfsError>;
}

/// View: filter messages by speaker.
///
/// Params: `{ "speaker": "alice", "limit": 20 }`
pub struct BySpeakerView;

#[async_trait]
impl MemoryView for BySpeakerView {
    fn name(&self) -> &str {
        "by_speaker"
    }
    fn description(&self) -> &str {
        "Filter messages by speaker"
    }
    async fn query(
        &self,
        pool: &SqlitePool,
        chat_id: &str,
        params: &str,
    ) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let speaker = val["speaker"].as_str().unwrap_or_default();
        let limit = val["limit"].as_i64().unwrap_or(20).clamp(1, 200);

        let rows = sqlx::query(
            "SELECT msg_id, ts, chat_id, speaker, reply_to, text, mentions
             FROM messages WHERE chat_id = ?1 AND speaker = ?2
             ORDER BY msg_id DESC LIMIT ?3",
        )
        .bind(chat_id)
        .bind(speaker)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                use sqlx::Row;
                serde_json::json!({
                    "msg_id": row.get::<i64, _>("msg_id"),
                    "ts": row.get::<String, _>("ts"),
                    "speaker": row.get::<String, _>("speaker"),
                    "text": row.get::<String, _>("text"),
                    "reply_to": row.get::<Option<i64>, _>("reply_to"),
                })
            })
            .collect();

        Ok(serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string()))
    }
}

/// View: most recent N messages (no need to know msg_id ranges).
///
/// Params: `{ "limit": 20 }`
pub struct RecentView;

#[async_trait]
impl MemoryView for RecentView {
    fn name(&self) -> &str {
        "recent"
    }
    fn description(&self) -> &str {
        "Most recent messages"
    }
    async fn query(
        &self,
        pool: &SqlitePool,
        chat_id: &str,
        params: &str,
    ) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).unwrap_or(serde_json::json!({}));
        let limit = val["limit"].as_i64().unwrap_or(20).clamp(1, 200);

        let rows = sqlx::query(
            "SELECT msg_id, ts, chat_id, speaker, reply_to, text, mentions
             FROM messages WHERE chat_id = ?1
             ORDER BY msg_id DESC LIMIT ?2",
        )
        .bind(chat_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                use sqlx::Row;
                serde_json::json!({
                    "msg_id": row.get::<i64, _>("msg_id"),
                    "ts": row.get::<String, _>("ts"),
                    "speaker": row.get::<String, _>("speaker"),
                    "text": row.get::<String, _>("text"),
                    "reply_to": row.get::<Option<i64>, _>("reply_to"),
                })
            })
            .collect();

        Ok(serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string()))
    }
}
