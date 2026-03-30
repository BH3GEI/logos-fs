use sqlx::SqlitePool;

use logos_vfs::VfsError;

pub struct AnchorDb {
    pub(crate) pool: SqlitePool,
}

impl AnchorDb {
    pub async fn new(pool: SqlitePool) -> Result<Self, VfsError> {
        init_schema(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn list(&self, task_id: &str) -> Result<String, VfsError> {
        let rows = sqlx::query(
            "SELECT id, task_id, summary, facts, created_at
             FROM anchors WHERE task_id = ?1 ORDER BY created_at ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        let anchors: Vec<serde_json::Value> = rows.iter().map(anchor_row_to_json).collect();
        Ok(serde_json::to_string(&anchors).unwrap_or_else(|_| "[]".to_string()))
    }

    pub async fn get(
        &self,
        task_id: &str,
        anchor_id: &str,
    ) -> Result<Option<String>, VfsError> {
        let row = sqlx::query(
            "SELECT id, task_id, summary, facts, created_at
             FROM anchors WHERE task_id = ?1 AND id = ?2",
        )
        .bind(task_id)
        .bind(anchor_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        Ok(row.as_ref().map(|r| anchor_row_to_json(r).to_string()))
    }

    pub(crate) async fn create(
        pool: &SqlitePool,
        task_id: &str,
        summary: &str,
        facts: &str,
    ) -> Result<String, VfsError> {
        let anchor_id = chrono::Utc::now().format("%Y-%m-%dT%H:%M").to_string();
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let new_anchor_uri = format!("logos://system/anchors/{task_id}/{anchor_id}");

        // Process version chain: if any fact contains "supersedes", mark the
        // referenced anchors as superseded (RFC 003 §6.1).
        if let Ok(facts_arr) = serde_json::from_str::<Vec<serde_json::Value>>(facts) {
            for fact in &facts_arr {
                if let Some(supersedes) = fact.get("supersedes") {
                    let uris: Vec<&str> = if let Some(arr) = supersedes.as_array() {
                        arr.iter().filter_map(|v| v.as_str()).collect()
                    } else if let Some(s) = supersedes.as_str() {
                        vec![s]
                    } else {
                        vec![]
                    };
                    for uri in uris {
                        let _ = Self::mark_superseded(pool, uri, &new_anchor_uri).await;
                    }
                }
            }
        }

        sqlx::query(
            "INSERT INTO anchors (id, task_id, summary, facts, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&anchor_id)
        .bind(task_id)
        .bind(summary)
        .bind(facts)
        .bind(&now)
        .execute(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("insert anchor: {e}")))?;

        // Sync FTS5 index for BM25 search
        let rowid = sqlx::query("SELECT rowid FROM anchors WHERE task_id = ?1 AND id = ?2")
            .bind(task_id)
            .bind(&anchor_id)
            .fetch_one(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("get anchor rowid: {e}")))?;
        let rowid: i64 = sqlx::Row::get(&rowid, 0);

        sqlx::query("INSERT INTO anchors_fts(rowid, facts, summary) VALUES (?1, ?2, ?3)")
            .bind(rowid)
            .bind(facts)
            .bind(summary)
            .execute(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("sync anchors fts: {e}")))?;

        Ok(anchor_id)
    }

    /// Mark facts in a target anchor as superseded.
    ///
    /// `target_uri` is `logos://system/anchors/{task_id}/{anchor_id}`.
    /// Each fact in the target gets `"status": "superseded"` and
    /// `"superseded_by": "{superseded_by_uri}"`.
    pub(crate) async fn mark_superseded(
        pool: &SqlitePool,
        target_uri: &str,
        superseded_by_uri: &str,
    ) -> Result<(), VfsError> {
        // Parse URI: logos://system/anchors/{task_id}/{anchor_id}
        let parts: Vec<&str> = target_uri.split('/').collect();
        // ["logos:", "", "system", "anchors", task_id, anchor_id]
        if parts.len() != 6 || parts[3] != "anchors" {
            return Err(VfsError::InvalidPath(format!(
                "invalid anchor URI: {target_uri}"
            )));
        }
        let target_task_id = parts[4];
        let target_anchor_id = parts[5];

        let row = sqlx::query(
            "SELECT facts FROM anchors WHERE task_id = ?1 AND id = ?2",
        )
        .bind(target_task_id)
        .bind(target_anchor_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("read anchor for supersede: {e}")))?;

        let Some(row) = row else {
            return Ok(()); // target not found, skip silently
        };

        let facts_str: String = sqlx::Row::get(&row, "facts");
        let mut facts: Vec<serde_json::Value> = serde_json::from_str(&facts_str)
            .unwrap_or_default();

        for fact in &mut facts {
            if let Some(obj) = fact.as_object_mut() {
                obj.insert("status".to_string(), serde_json::json!("superseded"));
                obj.insert(
                    "superseded_by".to_string(),
                    serde_json::json!(superseded_by_uri),
                );
            }
        }

        let updated = serde_json::to_string(&facts).unwrap_or_else(|_| "[]".to_string());
        sqlx::query("UPDATE anchors SET facts = ?1 WHERE task_id = ?2 AND id = ?3")
            .bind(&updated)
            .bind(target_task_id)
            .bind(target_anchor_id)
            .execute(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("update superseded: {e}")))?;

        Ok(())
    }
}

fn anchor_row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    use sqlx::Row;
    serde_json::json!({
        "id": row.get::<String, _>("id"),
        "task_id": row.get::<String, _>("task_id"),
        "summary": row.get::<String, _>("summary"),
        "facts": row.get::<String, _>("facts"),
        "created_at": row.get::<String, _>("created_at"),
    })
}

async fn init_schema(pool: &SqlitePool) -> Result<(), VfsError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS anchors (
            id         TEXT NOT NULL,
            task_id    TEXT NOT NULL,
            summary    TEXT NOT NULL,
            facts      TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            PRIMARY KEY (task_id, id)
        )",
    )
    .execute(pool)
    .await
    .map_err(|e| VfsError::Sqlite(format!("init anchors schema: {e}")))?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_anchors_task ON anchors(task_id, created_at)")
        .execute(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("init anchors index: {e}")))?;

    // FTS5 for BM25 search over anchor facts and summaries (RFC 003 §6.2)
    sqlx::query(
        "CREATE VIRTUAL TABLE IF NOT EXISTS anchors_fts USING fts5(facts, summary, content=anchors, content_rowid=rowid)",
    )
    .execute(pool)
    .await
    .map_err(|e| VfsError::Sqlite(format!("init anchors fts: {e}")))?;

    Ok(())
}
