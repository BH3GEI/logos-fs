use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use logos_vfs::VfsError;
use tokio::sync::Mutex;

/// A cron job definition — creates a task on schedule.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CronJob {
    pub name: String,
    pub cron_expr: String,
    pub task_template: serde_json::Value,
    pub enabled: bool,
}

/// Cron scheduler for the Logos kernel (RFC 002 §12.5).
///
/// Evaluates cron expressions every minute and fires matching jobs
/// by creating pending tasks for agent execution.
pub struct CronScheduler {
    jobs: Mutex<HashMap<String, CronJob>>,
    system: Arc<logos_system::SystemModule>,
}

impl CronScheduler {
    pub fn new(system: Arc<logos_system::SystemModule>) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            system,
        }
    }

    /// Register a cron job.
    pub async fn register(&self, job: CronJob) {
        self.jobs.lock().await.insert(job.name.clone(), job);
    }

    /// Remove a cron job.
    pub async fn remove(&self, name: &str) {
        self.jobs.lock().await.remove(name);
    }

    /// List all registered cron jobs.
    pub async fn list(&self) -> Vec<CronJob> {
        self.jobs.lock().await.values().cloned().collect()
    }

    /// Start the background tick loop. Checks every 60 seconds.
    pub fn start(self: &Arc<Self>) {
        let scheduler = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                scheduler.tick().await;
            }
        });
    }

    /// Check all jobs and fire those whose cron expression matches now.
    /// Also checks for plan tasks that need scheduling.
    async fn tick(&self) {
        let now = chrono::Utc::now();
        let job_names: Vec<String> = {
            let jobs = self.jobs.lock().await;
            jobs.values()
                .filter(|j| j.enabled)
                .filter(|j| {
                    cron::Schedule::from_str(&j.cron_expr)
                        .ok()
                        .and_then(|s| s.upcoming(chrono::Utc).next())
                        .map(|next| (next - now).num_seconds().abs() < 60)
                        .unwrap_or(false)
                })
                .map(|j| j.name.clone())
                .collect()
        };

        for name in job_names {
            if let Err(e) = self.fire(&name).await {
                eprintln!("[logos-cron] ERROR: failed to fire {name}: {e}");
            }
        }

        // Plan scheduling: pick up sleeping tasks with non-empty plan_todo
        if let Err(e) = self.tick_plans().await {
            eprintln!("[logos-plan] ERROR: plan tick failed: {e}");
        }
    }

    /// Process plan tasks: for each sleeping task with plan_todo, pop the head
    /// and create an executor task.
    async fn tick_plans(&self) -> Result<(), VfsError> {
        let pending = self.system.list_plan_pending().await?;
        for (planner_id, _todo_json) in pending {
            // Skip if executor already running for this planner
            if self.system.has_active_executor(&planner_id).await.unwrap_or(false) {
                continue;
            }

            // Pop head from todo
            let Some(description) = self.system.pop_plan_head(&planner_id).await? else {
                continue;
            };

            // Create executor task
            let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
            let executor_id = format!("plan-exec-{planner_id}-{ts}");
            let task_content = serde_json::json!({
                "task_id": executor_id,
                "description": description,
                "chat_id": "",
                "trigger": "plan",
                "plan_parent": planner_id,
            });
            self.system.create_task(&task_content.to_string()).await?;
            println!("[logos-plan] created executor task {executor_id} for planner {planner_id}");
        }
        Ok(())
    }

    /// Create a task from a cron job template.
    pub async fn fire(&self, job_name: &str) -> Result<(), VfsError> {
        let jobs = self.jobs.lock().await;
        let job = jobs.get(job_name).ok_or_else(|| {
            VfsError::NotFound(format!("cron job not found: {job_name}"))
        })?;

        if !job.enabled {
            return Ok(());
        }

        let mut task = job.task_template.clone();
        if let Some(obj) = task.as_object_mut() {
            obj.insert("trigger".to_string(), serde_json::json!("cron"));
            if !obj.contains_key("task_id") || obj["task_id"].as_str().unwrap_or("").is_empty() {
                let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S").to_string();
                obj.insert(
                    "task_id".to_string(),
                    serde_json::json!(format!("cron-{}-{ts}", job.name)),
                );
            }
        }

        let content = serde_json::to_string(&task)
            .map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        drop(jobs); // release lock before async call
        self.system.create_task(&content).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let system = Arc::new(
            logos_system::SystemModule::init(dir.path().join("test.db"))
                .await
                .unwrap(),
        );
        let scheduler = CronScheduler::new(system);

        scheduler
            .register(CronJob {
                name: "daily-summary".to_string(),
                cron_expr: "0 9 * * * *".to_string(),
                task_template: serde_json::json!({
                    "description": "Generate daily summary",
                    "chat_id": "group-1",
                }),
                enabled: true,
            })
            .await;

        let jobs = scheduler.list().await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].name, "daily-summary");
    }

    #[tokio::test]
    async fn fire_creates_task() {
        let dir = tempfile::tempdir().unwrap();
        let system = Arc::new(
            logos_system::SystemModule::init(dir.path().join("test.db"))
                .await
                .unwrap(),
        );
        let scheduler = CronScheduler::new(Arc::clone(&system));

        scheduler
            .register(CronJob {
                name: "test-cron".to_string(),
                cron_expr: "* * * * * *".to_string(),
                task_template: serde_json::json!({
                    "description": "Cron test task",
                    "chat_id": "group-1",
                }),
                enabled: true,
            })
            .await;

        scheduler.fire("test-cron").await.unwrap();
    }

    #[tokio::test]
    async fn cron_expr_parsing() {
        // Verify cron expressions parse correctly
        assert!(cron::Schedule::from_str("0 * * * * *").is_ok()); // every hour
        assert!(cron::Schedule::from_str("0 0 3 * * *").is_ok()); // daily at 3am
        assert!(cron::Schedule::from_str("0 5 * * * *").is_ok()); // hourly at :05
        assert!(cron::Schedule::from_str("0 30 3 * * *").is_ok()); // daily at 3:30
    }
}
