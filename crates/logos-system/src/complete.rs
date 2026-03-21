use std::sync::Arc;

use logos_vfs::VfsError;

use crate::tasks::TaskDb;

/// Parameters for logos_complete — the turn terminator.
pub struct CompleteParams {
    pub task_id: String,
    pub summary: String,
    pub reply: String,
    pub anchor: bool,
    pub anchor_facts: String,
    pub task_log: String,
    pub sleep_reason: String,
    pub sleep_retry: bool,
    pub resume_task_id: String,
}

/// Result of logos_complete.
pub struct CompleteResult {
    pub reply: String,
    pub anchor_id: String,
}

/// Execute logos_complete atomically:
///   1. Handle resume (discard current task, activate target) — OR:
///   2. Update task status (finished / sleep)
///   3. Create anchor (if anchor: true)
pub async fn execute(
    tasks: &TaskDb,
    params: CompleteParams,
) -> Result<CompleteResult, VfsError> {
    let conn = Arc::clone(&tasks.conn);
    let reply = params.reply.clone();

    let anchor_id = tokio::task::spawn_blocking(move || {
        // Tasks and anchors share the same SQLite database.
        let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;

        // Resume: discard current task, activate target
        if !params.resume_task_id.is_empty() {
            // Discard current empty task
            if !params.task_id.is_empty() {
                conn.execute(
                    "DELETE FROM tasks WHERE task_id = ?1",
                    rusqlite::params![params.task_id],
                )
                .map_err(|e| VfsError::Sqlite(format!("discard task: {e}")))?;
            }
            // Activate target
            TaskDb::transition_status_sync(&conn, &params.resume_task_id, "active")?;
            return Ok(String::new());
        }

        // Determine target status
        let new_status = if !params.sleep_reason.is_empty() {
            "sleep"
        } else {
            "finished"
        };

        // 1. Transition task status
        if !params.task_id.is_empty() {
            TaskDb::transition_status_sync(&conn, &params.task_id, new_status)?;
        }

        // 2. Create anchor if requested
        let mut anchor_id = String::new();
        if params.anchor && !params.task_id.is_empty() {
            anchor_id = crate::anchors::AnchorDb::create_sync(
                &conn,
                &params.task_id,
                &params.summary,
                &params.anchor_facts,
            )?;
        }

        Ok(anchor_id)
    })
    .await
    .map_err(|e| VfsError::Io(e.to_string()))??;

    Ok(CompleteResult { reply, anchor_id })
}
