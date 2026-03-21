//! Consolidator — RFC 003 §4 Memory Consolidation.
//!
//! Registers 4 cron jobs that create tasks for agent execution:
//! - consolidate-short-summary (hourly): raw messages → short summary
//! - consolidate-short-persona (hourly): raw messages → persona observations
//! - consolidate-mid-summary (daily): short summaries → mid summary
//! - consolidate-mid-persona (daily): short persona → rewrite mid persona

use crate::cron::{CronJob, CronScheduler};

/// Register all 4 consolidator cron jobs.
///
/// `chat_ids` should be scanned from the memory db_root directory.
pub async fn register_consolidator_jobs(scheduler: &CronScheduler) {
    // 1. Short summary — every hour at :00
    scheduler
        .register(CronJob {
            name: "consolidate-short-summary".to_string(),
            cron_expr: "0 0 * * * *".to_string(), // sec=0, min=0, every hour
            task_template: serde_json::json!({
                "description": concat!(
                    "Consolidator: generate hourly short summary.\n",
                    "1. Read messages from the past hour using memory.range_fetch or memory.search\n",
                    "2. Compress into a concise summary\n",
                    "3. Write to logos://memory/groups/{chat_id}/summary/short/{period}\n",
                    "   where {period} = current YYYY-MM-DDTHH\n",
                    "   JSON: {\"layer\":\"short\",\"period_start\":\"...\",\"period_end\":\"...\",",
                    "\"source_refs\":[[msg_start,msg_end]],\"content\":\"summary text\"}"
                ),
                "resource": "logos://memory/*/summary/short",
            }),
            enabled: true,
        })
        .await;

    // 2. Short persona — every hour at :05 (staggered)
    scheduler
        .register(CronJob {
            name: "consolidate-short-persona".to_string(),
            cron_expr: "0 5 * * * *".to_string(), // sec=0, min=5, every hour
            task_template: serde_json::json!({
                "description": concat!(
                    "Consolidator: extract hourly persona observations.\n",
                    "1. Read messages from the past hour\n",
                    "2. For each active speaker, extract minimal new observations\n",
                    "3. Append to logos://users/{uid}/persona/short/{period}\n",
                    "   (append-only JSON array, one entry per observation)"
                ),
                "resource": "logos://users/*/persona/short",
            }),
            enabled: true,
        })
        .await;

    // 3. Mid summary — daily at 03:00
    scheduler
        .register(CronJob {
            name: "consolidate-mid-summary".to_string(),
            cron_expr: "0 0 3 * * *".to_string(), // sec=0, min=0, hour=3, daily
            task_template: serde_json::json!({
                "description": concat!(
                    "Consolidator: generate daily mid summary.\n",
                    "1. Read all short summaries from the past 24 hours\n",
                    "2. Compress into a daily mid summary\n",
                    "3. Write to logos://memory/groups/{chat_id}/summary/mid/{date}\n",
                    "   where {date} = yesterday's YYYY-MM-DD\n",
                    "4. If month boundary crossed, also update summary/long/{month}"
                ),
                "resource": "logos://memory/*/summary/mid",
            }),
            enabled: true,
        })
        .await;

    // 4. Mid persona — daily at 03:30 (staggered)
    scheduler
        .register(CronJob {
            name: "consolidate-mid-persona".to_string(),
            cron_expr: "0 30 3 * * *".to_string(), // sec=0, min=30, hour=3, daily
            task_template: serde_json::json!({
                "description": concat!(
                    "Consolidator: rewrite daily mid persona.\n",
                    "1. Read today's persona/short entries for each active user\n",
                    "2. Read existing persona/mid content\n",
                    "3. Deduplicate, synthesize, and rewrite persona/mid/\n",
                    "4. If significant changes, also update persona/long.md"
                ),
                "resource": "logos://users/*/persona/mid",
            }),
            enabled: true,
        })
        .await;

    println!("[logos] registered 4 consolidator cron jobs");
}
