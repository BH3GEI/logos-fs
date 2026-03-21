use sqlx::SqlitePool;

use logos_vfs::VfsError;

/// Multi-level experience retrieval — RFC 003 §6.2.
///
/// L1: Anchor facts BM25 search via FTS5
/// L2: Task metadata keyword search
///
/// Returns `{ "l1_hits": [...], "l2_hits": [...] }`.
pub async fn search_tasks(
    pool: &SqlitePool,
    query: &str,
    limit: i64,
) -> Result<String, VfsError> {
    let limit = limit.clamp(1, 50);

    // L1 — Anchor facts BM25 match via FTS5 (RFC 003 §6.2)
    // Escape query for FTS5 safety
    let escaped_query = format!("\"{}\"", query.replace('"', "\"\""));
    let l1_rows = sqlx::query(
        "SELECT a.task_id, a.id, a.summary, a.facts, a.created_at
         FROM anchors_fts f
         JOIN anchors a ON a.rowid = f.rowid
         WHERE anchors_fts MATCH ?1
         ORDER BY f.rank
         LIMIT ?2",
    )
    .bind(&escaped_query)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| VfsError::Sqlite(format!("search anchors fts: {e}")))?;

    let l1_hits: Vec<serde_json::Value> = l1_rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            serde_json::json!({
                "task_id": row.get::<String, _>("task_id"),
                "anchor_id": row.get::<String, _>("id"),
                "summary": row.get::<String, _>("summary"),
                "facts": row.get::<String, _>("facts"),
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect();

    // L2 — Task description keyword match (LIKE fallback)
    let pattern = format!("%{query}%");
    let l2_rows = sqlx::query(
        "SELECT task_id, description, status, created_at FROM tasks
         WHERE (description LIKE ?1 OR task_id LIKE ?1) AND status != 'pending'
         ORDER BY created_at DESC LIMIT ?2",
    )
    .bind(&pattern)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| VfsError::Sqlite(format!("search tasks: {e}")))?;

    let l2_hits: Vec<serde_json::Value> = l2_rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            serde_json::json!({
                "task_id": row.get::<String, _>("task_id"),
                "description": row.get::<String, _>("description"),
                "status": row.get::<String, _>("status"),
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect();

    let result = serde_json::json!({ "l1_hits": l1_hits, "l2_hits": l2_hits });
    Ok(result.to_string())
}
