use logos_vfs::VfsError;

use crate::anchors::AnchorDb;
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
    // Resume: discard current task, activate target
    if !params.resume_task_id.is_empty() {
        if !params.task_id.is_empty() {
            TaskDb::delete(&tasks.pool, &params.task_id).await?;
        }
        TaskDb::transition_status(&tasks.pool, &params.resume_task_id, "active").await?;
        return Ok(CompleteResult {
            reply: params.reply,
            anchor_id: String::new(),
        });
    }

    // Determine target status
    let new_status = if !params.sleep_reason.is_empty() {
        "sleep"
    } else {
        "finished"
    };

    // 1. Transition task status
    if !params.task_id.is_empty() {
        TaskDb::transition_status(&tasks.pool, &params.task_id, new_status).await?;
    }

    // 2. Create anchor if requested
    let mut anchor_id = String::new();
    if params.anchor && !params.task_id.is_empty() {
        anchor_id = AnchorDb::create(
            &tasks.pool,
            &params.task_id,
            &params.summary,
            &params.anchor_facts,
        )
        .await?;

        // RFC 003 §7.1: anchor creation triggers short summary regeneration
        let now = chrono::Utc::now();
        let regen_task_id = format!("regen-summary-{}", now.format("%Y%m%dT%H%M%S"));
        let period = now.format("%Y-%m-%dT%H").to_string();
        let regen_content = serde_json::json!({
            "task_id": regen_task_id,
            "description": format!(
                "Regenerate short summary for period {period} (triggered by anchor {anchor_id})"
            ),
            "resource": "logos://memory/*/summary/short",
            "chat_id": "",
            "trigger": "anchor",
        });
        let _ = tasks.create(&regen_content.to_string()).await;
    }

    Ok(CompleteResult {
        reply: params.reply,
        anchor_id,
    })
}
