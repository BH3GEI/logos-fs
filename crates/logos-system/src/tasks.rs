use sqlx::SqlitePool;

use logos_vfs::VfsError;

/// RFC 002 Section 4.3: pending → active → finished, active ↔ sleep
const VALID_TRANSITIONS: &[(&str, &str)] = &[
    ("pending", "active"),
    ("active", "finished"),
    ("active", "sleep"),
    ("sleep", "active"),
];

pub struct TaskDb {
    pub(crate) pool: SqlitePool,
}

impl TaskDb {
    pub async fn new(pool: SqlitePool) -> Result<Self, VfsError> {
        init_schema(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn list(
        &self,
        chat_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<String, VfsError> {
        // RFC 002 §4.3: agents never observe pending tasks
        let mut sql = "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at FROM tasks WHERE status != 'pending'".to_string();
        let mut args: Vec<String> = Vec::new();
        if let Some(cid) = chat_id {
            args.push(cid.to_string());
            sql.push_str(&format!(" AND chat_id = ?{}", args.len()));
        }
        if let Some(st) = status {
            args.push(st.to_string());
            sql.push_str(&format!(" AND status = ?{}", args.len()));
        }
        sql.push_str(" ORDER BY created_at DESC");

        let mut query = sqlx::query(&sql);
        for arg in &args {
            query = query.bind(arg);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        let tasks: Vec<serde_json::Value> = rows.iter().map(task_row_to_json).collect();
        Ok(serde_json::json!({ "tasks": tasks }).to_string())
    }

    pub async fn get(&self, task_id: &str) -> Result<Option<String>, VfsError> {
        let row = sqlx::query(
            "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at
             FROM tasks WHERE task_id = ?1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| VfsError::Sqlite(e.to_string()))?;

        Ok(row.as_ref().map(|r| task_row_to_json(r).to_string()))
    }

    pub async fn create(&self, content: &str) -> Result<(), VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(content).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let now = now();
        sqlx::query(
            "INSERT INTO tasks (task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7, ?7)",
        )
        .bind(val["task_id"].as_str().unwrap_or_default())
        .bind(val["description"].as_str().unwrap_or_default())
        .bind(val["workspace"].as_str().unwrap_or_default())
        .bind(val["resource"].as_str().unwrap_or_default())
        .bind(val["chat_id"].as_str().unwrap_or_default())
        .bind(val["trigger"].as_str().unwrap_or("user_message"))
        .bind(&now)
        .execute(&self.pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("insert task: {e}")))?;

        Ok(())
    }

    pub async fn update_description(
        &self,
        task_id: &str,
        description: &str,
    ) -> Result<(), VfsError> {
        sqlx::query("UPDATE tasks SET description = ?1, updated_at = ?2 WHERE task_id = ?3")
            .bind(description)
            .bind(now())
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("update description: {e}")))?;
        Ok(())
    }

    pub(crate) async fn transition_status(
        pool: &SqlitePool,
        task_id: &str,
        new_status: &str,
    ) -> Result<(), VfsError> {
        let row = sqlx::query("SELECT status FROM tasks WHERE task_id = ?1")
            .bind(task_id)
            .fetch_one(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("get task status: {e}")))?;

        let current: String = sqlx::Row::get(&row, 0);

        let valid = VALID_TRANSITIONS
            .iter()
            .any(|(from, to)| *from == current && *to == new_status);
        if !valid {
            return Err(VfsError::InvalidPath(format!(
                "invalid transition: {current} → {new_status}"
            )));
        }

        sqlx::query("UPDATE tasks SET status = ?1, updated_at = ?2 WHERE task_id = ?3")
            .bind(new_status)
            .bind(now())
            .bind(task_id)
            .execute(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("transition status: {e}")))?;
        Ok(())
    }
}

fn task_row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    use sqlx::Row;
    serde_json::json!({
        "task_id": row.get::<String, _>("task_id"),
        "description": row.get::<String, _>("description"),
        "workspace": row.get::<String, _>("workspace"),
        "resource": row.get::<String, _>("resource"),
        "status": row.get::<String, _>("status"),
        "chat_id": row.get::<String, _>("chat_id"),
        "trigger": row.get::<String, _>("trigger"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    })
}

async fn init_schema(pool: &SqlitePool) -> Result<(), VfsError> {
    sqlx::query(
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
        )",
    )
    .execute(pool)
    .await
    .map_err(|e| VfsError::Sqlite(format!("init tasks schema: {e}")))?;
    Ok(())
}

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
