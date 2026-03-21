use std::sync::Arc;

use rusqlite::Connection;

use logos_vfs::VfsError;

/// RFC 002 Section 4.3: pending → active → finished, active ↔ sleep
const VALID_TRANSITIONS: &[(&str, &str)] = &[
    ("pending", "active"),
    ("active", "finished"),
    ("active", "sleep"),
    ("sleep", "active"),
];

pub struct TaskDb {
    pub(crate) conn: Arc<std::sync::Mutex<Connection>>,
}

impl TaskDb {
    pub fn new(conn: Arc<std::sync::Mutex<Connection>>) -> Result<Self, VfsError> {
        {
            let c = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            init_schema(&c)?;
        }
        Ok(Self { conn })
    }

    pub async fn list(&self, chat_id: Option<&str>, status: Option<&str>) -> Result<String, VfsError> {
        let conn = Arc::clone(&self.conn);
        let chat_id = chat_id.map(|s| s.to_string());
        let status = status.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            // RFC 002 §4.3: agents never observe pending tasks
            let mut sql = "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at FROM tasks WHERE status != 'pending'".to_string();
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            if let Some(ref cid) = chat_id {
                sql.push_str(&format!(" AND chat_id = ?{}", params.len() + 1));
                params.push(Box::new(cid.clone()));
            }
            if let Some(ref st) = status {
                sql.push_str(&format!(" AND status = ?{}", params.len() + 1));
                params.push(Box::new(st.clone()));
            }
            sql.push_str(" ORDER BY created_at DESC");

            let mut stmt = conn.prepare(&sql).map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let rows: Vec<serde_json::Value> = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), task_row_to_json)
                .map_err(|e| VfsError::Sqlite(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(serde_json::json!({ "tasks": rows }).to_string())
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub async fn get(&self, task_id: &str) -> Result<Option<String>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let result = conn
                .query_row(
                    "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at
                     FROM tasks WHERE task_id = ?1",
                    rusqlite::params![task_id],
                    task_row_to_json,
                )
                .ok();
            Ok(result.map(|v| v.to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub async fn create(&self, content: &str) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(content).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let conn = Arc::clone(&self.conn);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let now = now();
            conn.execute(
                "INSERT INTO tasks (task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7, ?7)",
                rusqlite::params![
                    val["task_id"].as_str().unwrap_or_default(),
                    val["description"].as_str().unwrap_or_default(),
                    val["workspace"].as_str().unwrap_or_default(),
                    val["resource"].as_str().unwrap_or_default(),
                    val["chat_id"].as_str().unwrap_or_default(),
                    val["trigger"].as_str().unwrap_or("user_message"),
                    now,
                ],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert task: {e}")))?;

            // Sync FTS for L2 experience retrieval (RFC 003 §6.2)
            conn.execute(
                "INSERT INTO tasks_fts(rowid, description) SELECT rowid, description FROM tasks WHERE task_id = ?1",
                rusqlite::params![val["task_id"].as_str().unwrap_or_default()],
            )
            .map_err(|e| VfsError::Sqlite(format!("sync task FTS: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub async fn update_description(&self, task_id: &str, description: &str) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let description = description.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            conn.execute(
                "UPDATE tasks SET description = ?1, updated_at = ?2 WHERE task_id = ?3",
                rusqlite::params![description, now(), task_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("update description: {e}")))?;

            // Sync FTS
            conn.execute(
                "DELETE FROM tasks_fts WHERE rowid = (SELECT rowid FROM tasks WHERE task_id = ?1)",
                rusqlite::params![task_id],
            ).ok(); // ignore if no previous entry
            conn.execute(
                "INSERT INTO tasks_fts(rowid, description) SELECT rowid, description FROM tasks WHERE task_id = ?1",
                rusqlite::params![task_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("sync task FTS on update: {e}")))?;

            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    /// FTS search over task descriptions (L2 experience retrieval).
    pub async fn search_fts(&self, query: &str, limit: i64) -> Result<String, VfsError> {
        let conn = Arc::clone(&self.conn);
        let query = query.to_string();
        let limit = limit.clamp(1, 50);

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let mut stmt = conn
                .prepare(
                    "SELECT t.task_id, t.description, t.workspace, t.resource, t.status, t.chat_id, t.trigger, t.created_at, t.updated_at
                     FROM tasks_fts f
                     JOIN tasks t ON t.rowid = f.rowid
                     WHERE tasks_fts MATCH ?1
                     ORDER BY f.rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(e.to_string()))?;
            let rows: Vec<serde_json::Value> = stmt
                .query_map(rusqlite::params![query, limit], task_row_to_json)
                .map_err(|e| VfsError::Sqlite(e.to_string()))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        })
        .await
        .map_err(|e| VfsError::Io(e.to_string()))?
    }

    pub(crate) fn transition_status_sync(
        conn: &Connection,
        task_id: &str,
        new_status: &str,
    ) -> Result<(), VfsError> {
        let current: String = conn
            .query_row(
                "SELECT status FROM tasks WHERE task_id = ?1",
                rusqlite::params![task_id],
                |row| row.get(0),
            )
            .map_err(|e| VfsError::Sqlite(format!("get task status: {e}")))?;

        let valid = VALID_TRANSITIONS
            .iter()
            .any(|(from, to)| *from == current && *to == new_status);
        if !valid {
            return Err(VfsError::InvalidPath(format!(
                "invalid transition: {current} → {new_status}"
            )));
        }

        conn.execute(
            "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE task_id = ?3",
            rusqlite::params![new_status, now(), task_id],
        )
        .map_err(|e| VfsError::Sqlite(format!("transition status: {e}")))?;
        Ok(())
    }
}

fn task_row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<serde_json::Value> {
    Ok(serde_json::json!({
        "task_id": row.get::<_, String>(0)?,
        "description": row.get::<_, String>(1)?,
        "workspace": row.get::<_, String>(2)?,
        "resource": row.get::<_, String>(3)?,
        "status": row.get::<_, String>(4)?,
        "chat_id": row.get::<_, String>(5)?,
        "trigger": row.get::<_, String>(6)?,
        "created_at": row.get::<_, String>(7)?,
        "updated_at": row.get::<_, String>(8)?,
    }))
}

fn init_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS tasks (
            task_id     TEXT PRIMARY KEY,
            description TEXT NOT NULL DEFAULT '',
            workspace   TEXT NOT NULL DEFAULT '',
            resource    TEXT NOT NULL DEFAULT '',
            status      TEXT NOT NULL DEFAULT 'pending',
            chat_id     TEXT NOT NULL DEFAULT '',
            trigger     TEXT NOT NULL DEFAULT 'user_message',
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(description, content='tasks', content_rowid='rowid');",
    )
    .map_err(|e| VfsError::Sqlite(format!("init tasks schema: {e}")))?;
    Ok(())
}

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
