pub mod anchors;
pub mod complete;
pub mod tasks;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

use anchors::AnchorDb;
use tasks::TaskDb;

/// The System Module — handles `logos://system/`.
///
/// URI routing:
///   logos://system/tasks                          → read: task list
///   logos://system/tasks                          → write: create task (JSON)
///   logos://system/tasks/{task_id}                → read: single task
///   logos://system/tasks/{task_id}/description    → write: update description
///   logos://system/anchors/{task_id}              → read: anchor list for task
///   logos://system/anchors/{task_id}/{anchor_id}  → read: single anchor
pub struct SystemModule {
    pub(crate) tasks: TaskDb,
    pub(crate) anchors: AnchorDb,
}

impl SystemModule {
    pub fn init(db_path: PathBuf) -> Result<Self, VfsError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VfsError::Io(format!("create system dir: {e}")))?;
        }
        let conn = rusqlite::Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open system db: {e}")))?;
        let conn = Arc::new(std::sync::Mutex::new(conn));
        let tasks = TaskDb::new(Arc::clone(&conn))?;
        let anchors = AnchorDb::new(conn)?;
        Ok(Self { tasks, anchors })
    }

    /// Multi-level experience retrieval (RFC 003 §6.2).
    /// L1: BM25 on anchor facts. L2: FTS on task description.
    /// Called by logos_call("system.search_tasks", params).
    pub async fn search_tasks(&self, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let query = val["query"].as_str().unwrap_or_default();
        let limit = val["limit"].as_i64().unwrap_or(5);

        if query.is_empty() {
            return Ok(r#"{"anchors":[],"tasks":[]}"#.to_string());
        }

        let anchor_results = self.anchors.search_fts(query, limit).await?;
        let task_results = self.tasks.search_fts(query, limit).await?;

        Ok(serde_json::json!({
            "anchors": serde_json::from_str::<serde_json::Value>(&anchor_results).unwrap_or_default(),
            "tasks": serde_json::from_str::<serde_json::Value>(&task_results).unwrap_or_default(),
        })
        .to_string())
    }

    /// Execute logos_complete. Called by the gRPC layer directly, not through Namespace trait.
    pub async fn complete(
        &self,
        params: complete::CompleteParams,
    ) -> Result<complete::CompleteResult, VfsError> {
        complete::execute(&self.tasks, params).await
    }
}

#[async_trait]
impl Namespace for SystemModule {
    fn name(&self) -> &str {
        "system"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        let resource = path
            .first()
            .ok_or_else(|| VfsError::InvalidPath("empty system path".to_string()))?;

        match *resource {
            "tasks" => {
                if let Some(task_id) = path.get(1) {
                    return self
                        .tasks
                        .get(task_id)
                        .await
                        .map(|opt| opt.unwrap_or_else(|| "null".to_string()));
                }
                self.tasks.list(None, None).await
            }
            "anchors" => {
                let task_id = path
                    .get(1)
                    .ok_or_else(|| VfsError::InvalidPath("missing task_id".to_string()))?;
                if let Some(anchor_id) = path.get(2) {
                    return self
                        .anchors
                        .get(task_id, anchor_id)
                        .await
                        .map(|opt| opt.unwrap_or_else(|| "null".to_string()));
                }
                self.anchors.list(task_id).await
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unknown system resource: {resource}"
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        let resource = path
            .first()
            .ok_or_else(|| VfsError::InvalidPath("empty system path".to_string()))?;

        match *resource {
            "tasks" => {
                // logos://system/tasks/{task_id}/description
                if path.len() >= 3 && path[2] == "description" {
                    return self.tasks.update_description(path[1], content).await;
                }
                // logos://system/tasks → create new task
                self.tasks.create(content).await
            }
            _ => Err(VfsError::InvalidPath(format!(
                "system write not supported for: {resource}"
            ))),
        }
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        let resource = path.first().map(|s| *s).unwrap_or("");

        // For tasks/{task_id}: read-merge-write
        if resource == "tasks" && path.len() >= 2 && path.len() < 3 {
            let existing = self.read(path).await?;
            if let (Ok(mut base), Ok(patch)) = (
                serde_json::from_str::<serde_json::Value>(&existing),
                serde_json::from_str::<serde_json::Value>(partial),
            ) {
                logos_vfs::json_deep_merge(&mut base, &patch);
                let merged = serde_json::to_string(&base).unwrap_or_else(|_| partial.to_string());
                return self.write(path, &merged).await;
            }
        }

        self.write(path, partial).await
    }
}
