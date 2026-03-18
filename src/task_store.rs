use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;

use crate::service::VfsError;

pub struct TaskStore {
    conn: Arc<std::sync::Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: String,
    pub description: String,
    pub workspace: String,
    pub resource: String,
    pub status: String,
    pub chat_id: String,
    pub trigger: String,
    pub created_at: String,
    pub updated_at: String,
}

pub struct NewTask {
    pub task_id: String,
    pub description: String,
    pub workspace: String,
    pub resource: String,
    pub chat_id: String,
    pub trigger: String,
}

// RFC 002 Section 4.3: pending → active → finished, active ↔ sleep
// There is no 'cancelled' state — task abandonment is implicit in logos_complete({ resume })
const VALID_TRANSITIONS: &[(&str, &str)] = &[
    ("pending", "active"),
    ("active", "finished"),
    ("active", "sleep"),
    ("sleep", "active"),
];

impl TaskStore {
    pub fn new(db_path: PathBuf) -> Result<Self, VfsError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VfsError::Io(format!("create system dir failed: {e}")))?;
        }
        let conn = Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open system db failed: {e}")))?;
        init_task_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    pub async fn create_task(&self, task: &NewTask) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task.task_id.clone();
        let description = task.description.clone();
        let workspace = task.workspace.clone();
        let resource = task.resource.clone();
        let chat_id = task.chat_id.clone();
        let trigger = task.trigger.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "INSERT INTO tasks (task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6, ?7, ?7)",
                rusqlite::params![task_id, description, workspace, resource, chat_id, trigger, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert task failed: {e}")))?;
            conn.execute(
                "INSERT INTO tasks_fts(rowid, description) SELECT rowid, description FROM tasks WHERE task_id = ?1",
                rusqlite::params![task_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("sync task FTS failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<Task>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at
                     FROM tasks WHERE task_id = ?1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![task_id], row_to_task)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(t)) => Ok(Some(t)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn list_tasks(
        &self,
        chat_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<Task>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let chat_id = chat_id.map(|s| s.to_string());
        let status = status.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;

            let mut conditions = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

            if let Some(ref cid) = chat_id {
                params.push(Box::new(cid.clone()));
                conditions.push(format!("chat_id = ?{}", params.len()));
            }
            if let Some(ref s) = status {
                params.push(Box::new(s.clone()));
                conditions.push(format!("status = ?{}", params.len()));
            }

            let where_clause = if conditions.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", conditions.join(" AND "))
            };

            let sql = format!(
                "SELECT task_id, description, workspace, resource, status, chat_id, trigger, created_at, updated_at
                 FROM tasks {where_clause} ORDER BY created_at DESC"
            );

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let rows = stmt
                .query_map(param_refs.as_slice(), row_to_task)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn update_status(
        &self,
        task_id: &str,
        new_status: &str,
    ) -> Result<(), VfsError> {
        let current = self
            .get_task(task_id)
            .await?
            .ok_or_else(|| VfsError::NotFound(format!("task not found: {task_id}")))?;

        if !is_valid_transition(&current.status, new_status) {
            return Err(VfsError::InvalidRequest(format!(
                "invalid status transition: {} -> {new_status}",
                current.status
            )));
        }

        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let new_status = new_status.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE task_id = ?3",
                rusqlite::params![new_status, now, task_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("update status failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    // RFC 003 Section 6.2 L2: text search on task description
    pub async fn search_tasks_fts(
        &self,
        query: &str,
        limit: i32,
    ) -> Result<Vec<Task>, VfsError> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let conn = Arc::clone(&self.conn);
        let query = query.to_string();
        let limit = limit.max(1).min(50);

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT t.task_id, t.description, t.workspace, t.resource, t.status,
                            t.chat_id, t.trigger, t.created_at, t.updated_at
                     FROM tasks_fts fts
                     JOIN tasks t ON fts.rowid = t.rowid
                     WHERE tasks_fts MATCH ?1
                     ORDER BY rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare FTS query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![query, limit], row_to_task)
                .map_err(|e| VfsError::Sqlite(format!("FTS query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn update_description(
        &self,
        task_id: &str,
        description: &str,
    ) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let description = description.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            let changed = conn
                .execute(
                    "UPDATE tasks SET description = ?1, updated_at = ?2 WHERE task_id = ?3",
                    rusqlite::params![description, now, task_id],
                )
                .map_err(|e| VfsError::Sqlite(format!("update description failed: {e}")))?;
            if changed == 0 {
                return Err(VfsError::NotFound(format!("task not found: {task_id}")));
            }
            // Rebuild FTS entry for updated description
            conn.execute(
                "DELETE FROM tasks_fts WHERE rowid = (SELECT rowid FROM tasks WHERE task_id = ?1)",
                rusqlite::params![task_id],
            ).ok();
            conn.execute(
                "INSERT INTO tasks_fts(rowid, description) SELECT rowid, description FROM tasks WHERE task_id = ?1",
                rusqlite::params![task_id],
            ).ok();
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }
}

fn init_task_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;

         CREATE TABLE IF NOT EXISTS tasks (
           task_id      TEXT PRIMARY KEY,
           description  TEXT NOT NULL DEFAULT '',
           workspace    TEXT NOT NULL,
           resource     TEXT NOT NULL DEFAULT '',
           status       TEXT NOT NULL DEFAULT 'pending',
           chat_id      TEXT NOT NULL,
           trigger      TEXT NOT NULL DEFAULT 'user_message',
           created_at   TEXT NOT NULL,
           updated_at   TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
         CREATE INDEX IF NOT EXISTS idx_tasks_chat ON tasks(chat_id);

         CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(
           description, content=tasks, content_rowid=rowid
         );",
    )
    .map_err(|e| VfsError::Sqlite(format!("init task schema failed: {e}")))
}

fn row_to_task(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
        task_id: row.get(0)?,
        description: row.get(1)?,
        workspace: row.get(2)?,
        resource: row.get(3)?,
        status: row.get(4)?,
        chat_id: row.get(5)?,
        trigger: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn is_valid_transition(from: &str, to: &str) -> bool {
    VALID_TRANSITIONS
        .iter()
        .any(|(f, t)| *f == from && *t == to)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_store(dir: &std::path::Path) -> TaskStore {
        TaskStore::new(dir.join("system.db")).unwrap()
    }

    fn sample_task(id: &str) -> NewTask {
        NewTask {
            task_id: id.to_string(),
            description: "test task".to_string(),
            workspace: "ws-1".to_string(),
            resource: "".to_string(),
            chat_id: "chat-1".to_string(),
            trigger: "user_message".to_string(),
        }
    }

    #[tokio::test]
    async fn create_and_get_task() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        let task = store.get_task("t1").await.unwrap().unwrap();
        assert_eq!(task.status, "pending");
        assert_eq!(task.description, "test task");
    }

    #[tokio::test]
    async fn status_machine_valid_transitions() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        store.update_status("t1", "active").await.unwrap();
        store.update_status("t1", "sleep").await.unwrap();
        store.update_status("t1", "active").await.unwrap();
        store.update_status("t1", "finished").await.unwrap();

        let task = store.get_task("t1").await.unwrap().unwrap();
        assert_eq!(task.status, "finished");
    }

    #[tokio::test]
    async fn status_machine_rejects_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        let err = store.update_status("t1", "finished").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn list_tasks_with_filters() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        store
            .create_task(&NewTask {
                chat_id: "chat-2".to_string(),
                ..sample_task("t2")
            })
            .await
            .unwrap();
        store.update_status("t1", "active").await.unwrap();

        let all = store.list_tasks(None, None).await.unwrap();
        assert_eq!(all.len(), 2);

        let active = store.list_tasks(None, Some("active")).await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].task_id, "t1");

        let chat2 = store.list_tasks(Some("chat-2"), None).await.unwrap();
        assert_eq!(chat2.len(), 1);
        assert_eq!(chat2[0].task_id, "t2");
    }

    #[tokio::test]
    async fn update_description_works() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        store.update_description("t1", "updated desc").await.unwrap();

        let task = store.get_task("t1").await.unwrap().unwrap();
        assert_eq!(task.description, "updated desc");
    }

    #[tokio::test]
    async fn update_nonexistent_task_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let err = store.update_description("no-such", "desc").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn search_tasks_fts_works() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store
            .create_task(&NewTask {
                description: "Deploy TTS service for voice synthesis".to_string(),
                ..sample_task("t1")
            })
            .await
            .unwrap();
        store
            .create_task(&NewTask {
                description: "Fix database migration for PostgreSQL".to_string(),
                ..sample_task("t2")
            })
            .await
            .unwrap();

        let results = store.search_tasks_fts("TTS voice", 10).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].task_id, "t1");

        let results = store.search_tasks_fts("PostgreSQL", 10).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].task_id, "t2");

        let empty = store.search_tasks_fts("", 10).await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn cancelled_state_does_not_exist() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_task(&sample_task("t1")).await.unwrap();
        assert!(store.update_status("t1", "cancelled").await.is_err());

        store.update_status("t1", "active").await.unwrap();
        assert!(store.update_status("t1", "cancelled").await.is_err());

        store.update_status("t1", "sleep").await.unwrap();
        assert!(store.update_status("t1", "cancelled").await.is_err());
    }
}
