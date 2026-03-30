pub mod anchors;
pub mod complete;
pub mod search;
pub mod tasks;

pub use complete::{CompleteParams, CompleteResult};

use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};
use sqlx::sqlite::SqlitePoolOptions;

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
    pub async fn init(db_path: PathBuf) -> Result<Self, VfsError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VfsError::Io(format!("create system dir: {e}")))?;
        }
        let url = format!("sqlite:{}?mode=rwc", db_path.display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .map_err(|e| VfsError::Sqlite(format!("open system db: {e}")))?;

        let tasks = TaskDb::new(pool.clone()).await?;
        let anchors = AnchorDb::new(pool).await?;
        Ok(Self { tasks, anchors })
    }

    /// Get a single task by ID.
    pub async fn get_task(&self, task_id: &str) -> Result<Option<String>, VfsError> {
        self.tasks.get(task_id).await
    }

    /// Create a task programmatically (used by cron scheduler).
    pub async fn create_task(&self, content: &str) -> Result<(), VfsError> {
        self.tasks.create(content).await
    }

    /// Transition a task's status (used by runtime for task lifecycle).
    pub async fn transition_task(&self, task_id: &str, new_status: &str) -> Result<(), VfsError> {
        tasks::TaskDb::transition_status(&self.tasks.pool, task_id, new_status).await
    }

    /// Search tasks and anchors — RFC 003 §6.2 multi-level experience retrieval.
    pub async fn search_tasks(&self, query: &str, limit: i64) -> Result<String, VfsError> {
        search::search_tasks(&self.tasks.pool, query, limit).await
    }

    /// Get sleeping tasks that have a non-empty plan_todo.
    pub async fn list_plan_pending(&self) -> Result<Vec<(String, String)>, VfsError> {
        TaskDb::list_plan_pending(&self.tasks.pool).await
    }

    /// Pop the first item from a task's plan_todo.
    pub async fn pop_plan_head(&self, task_id: &str) -> Result<Option<String>, VfsError> {
        TaskDb::pop_plan_head(&self.tasks.pool, task_id).await
    }

    /// Check if a planner has active executor tasks.
    pub async fn has_active_executor(&self, planner_id: &str) -> Result<bool, VfsError> {
        let row = sqlx::query(
            "SELECT COUNT(*) as cnt FROM tasks WHERE plan_parent = ?1 AND status = 'active'"
        )
        .bind(planner_id)
        .fetch_one(&self.tasks.pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("check active executor: {e}")))?;
        use sqlx::Row;
        let count: i32 = row.get("cnt");
        Ok(count > 0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use logos_vfs::Namespace;

    async fn test_system() -> (SystemModule, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let sys = SystemModule::init(dir.path().join("test.db")).await.unwrap();
        (sys, dir)
    }

    #[tokio::test]
    async fn task_create_and_get() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-1","description":"test task","chat_id":"c-1"}"#)
            .await.unwrap();

        let result = sys.read(&["tasks", "t-1"]).await.unwrap();
        let val: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(val["task_id"], "t-1");
        assert_eq!(val["status"], "pending");
    }

    #[tokio::test]
    async fn task_lifecycle() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-2","description":"lifecycle","chat_id":"c-1"}"#)
            .await.unwrap();

        // pending → active
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-2", "active").await.unwrap();
        let result = sys.read(&["tasks", "t-2"]).await.unwrap();
        assert!(result.contains("\"active\""));

        // active → sleep
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-2", "sleep").await.unwrap();
        let result = sys.read(&["tasks", "t-2"]).await.unwrap();
        assert!(result.contains("\"sleep\""));

        // sleep → active
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-2", "active").await.unwrap();

        // active → finished
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-2", "finished").await.unwrap();
        let result = sys.read(&["tasks", "t-2"]).await.unwrap();
        assert!(result.contains("\"finished\""));
    }

    #[tokio::test]
    async fn task_list_hides_pending() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-3","description":"pending","chat_id":"c-1"}"#)
            .await.unwrap();
        sys.write(&["tasks"], r#"{"task_id":"t-4","description":"active","chat_id":"c-1"}"#)
            .await.unwrap();
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-4", "active").await.unwrap();

        let result = sys.read(&["tasks"]).await.unwrap();
        assert!(!result.contains("t-3")); // pending hidden
        assert!(result.contains("t-4")); // active visible
    }

    #[tokio::test]
    async fn anchor_create_and_list() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-5","description":"anchor test","chat_id":"c-1"}"#)
            .await.unwrap();
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-5", "active").await.unwrap();

        // Create anchor via complete
        let params = complete::CompleteParams {
            task_id: "t-5".to_string(),
            summary: "test anchor".to_string(),
            reply: String::new(),
            anchor: true,
            anchor_facts: r#"[{"type":"decision","topic":"test","value":"yes"}]"#.to_string(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![],
        };
        let result = sys.complete(params).await.unwrap();
        assert!(!result.anchor_id.is_empty());

        // List anchors
        let anchors = sys.read(&["anchors", "t-5"]).await.unwrap();
        assert!(anchors.contains("test anchor"));
    }

    #[tokio::test]
    async fn anchor_search_bm25() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-6","description":"search test","chat_id":"c-1"}"#)
            .await.unwrap();
        tasks::TaskDb::transition_status(&sys.tasks.pool, "t-6", "active").await.unwrap();

        let params = complete::CompleteParams {
            task_id: "t-6".to_string(),
            summary: "npm dependency resolution".to_string(),
            reply: String::new(),
            anchor: true,
            anchor_facts: r#"[{"type":"troubleshooting","symptom_snippet":"ERESOLVE unable to resolve dependency tree","solution":"npm install --legacy-peer-deps"}]"#.to_string(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![],
        };
        sys.complete(params).await.unwrap();

        let result = sys.search_tasks("ERESOLVE", 10).await.unwrap();
        assert!(result.contains("ERESOLVE"));
    }

    #[tokio::test]
    async fn invalid_transition_rejected() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"t-7","description":"invalid","chat_id":"c-1"}"#)
            .await.unwrap();

        // pending → finished is not a valid transition
        let result = tasks::TaskDb::transition_status(&sys.tasks.pool, "t-7", "finished").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn plan_complete_stores_todo_and_sleeps() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"planner-1","description":"deploy service","chat_id":"c-1"}"#)
            .await.unwrap();
        tasks::TaskDb::transition_status(&sys.tasks.pool, "planner-1", "active").await.unwrap();

        // Complete with plan
        let params = complete::CompleteParams {
            task_id: "planner-1".to_string(),
            summary: "planned 3 steps".to_string(),
            reply: String::new(),
            anchor: false,
            anchor_facts: String::new(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![
                "install dependencies".to_string(),
                "build project".to_string(),
                "run tests".to_string(),
            ],
        };
        let result = sys.complete(params).await.unwrap();
        assert_eq!(result.task_id, "planner-1");

        // Task should be sleeping
        let task_json = sys.get_task("planner-1").await.unwrap().unwrap();
        let task: serde_json::Value = serde_json::from_str(&task_json).unwrap();
        assert_eq!(task["status"], "sleep");
        assert_eq!(task["plan_todo"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn plan_pop_head() {
        let (sys, _dir) = test_system().await;
        sys.write(&["tasks"], r#"{"task_id":"planner-2","description":"multi-step","chat_id":"c-1"}"#)
            .await.unwrap();
        tasks::TaskDb::transition_status(&sys.tasks.pool, "planner-2", "active").await.unwrap();

        let params = complete::CompleteParams {
            task_id: "planner-2".to_string(),
            summary: "planned".to_string(),
            reply: String::new(),
            anchor: false,
            anchor_facts: String::new(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec!["step A".to_string(), "step B".to_string()],
        };
        sys.complete(params).await.unwrap();

        // Pop first item
        let head = sys.pop_plan_head("planner-2").await.unwrap();
        assert_eq!(head, Some("step A".to_string()));

        // Remaining should be ["step B"]
        let task_json = sys.get_task("planner-2").await.unwrap().unwrap();
        let task: serde_json::Value = serde_json::from_str(&task_json).unwrap();
        assert_eq!(task["plan_todo"].as_array().unwrap().len(), 1);

        // Pop second
        let head = sys.pop_plan_head("planner-2").await.unwrap();
        assert_eq!(head, Some("step B".to_string()));

        // Now empty
        let head = sys.pop_plan_head("planner-2").await.unwrap();
        assert_eq!(head, None);
    }
}
