//! End-to-end integration test simulating a kairos-runtime session.
//!
//! This test boots the kernel in-process and exercises every primitive
//! through the Namespace and gRPC-equivalent interfaces, verifying the
//! full lifecycle that a real runtime would perform.

use std::sync::Arc;

use logos_vfs::{Namespace, RoutingTable};

// --- Namespace wrappers for Arc sharing ---

struct MmRef(Arc<logos_mm::MemoryModule>);
#[async_trait::async_trait]
impl Namespace for MmRef {
    fn name(&self) -> &str { "memory" }
    async fn read(&self, p: &[&str]) -> Result<String, logos_vfs::VfsError> { self.0.read(p).await }
    async fn write(&self, p: &[&str], c: &str) -> Result<(), logos_vfs::VfsError> { self.0.write(p, c).await }
    async fn patch(&self, p: &[&str], c: &str) -> Result<(), logos_vfs::VfsError> { self.0.patch(p, c).await }
}

struct SysRef(Arc<logos_system::SystemModule>);
#[async_trait::async_trait]
impl Namespace for SysRef {
    fn name(&self) -> &str { "system" }
    async fn read(&self, p: &[&str]) -> Result<String, logos_vfs::VfsError> { self.0.read(p).await }
    async fn write(&self, p: &[&str], c: &str) -> Result<(), logos_vfs::VfsError> { self.0.write(p, c).await }
    async fn patch(&self, p: &[&str], c: &str) -> Result<(), logos_vfs::VfsError> { self.0.patch(p, c).await }
}

/// Boot a minimal kernel with all namespaces, return shared references.
async fn boot_kernel() -> (
    Arc<RoutingTable>,
    Arc<logos_system::SystemModule>,
    Arc<logos_mm::MemoryModule>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    let mut table = RoutingTable::new();

    // Middleware
    table.add_middleware(Box::new(logos_vfs::JsonValidator));

    // Memory
    let sessions = Arc::new(logos_mm::SessionStore::new(64, 256));
    let mm = logos_mm::MemoryModule::init(root.join("memory"), sessions).unwrap();
    let mm_arc = Arc::new(mm);

    // System
    let system = logos_system::SystemModule::init(root.join("system.db"))
        .await
        .unwrap();
    let system_arc = Arc::new(system);

    table.mount(Box::new(MmRef(Arc::clone(&mm_arc))));
    table.mount(Box::new(SysRef(Arc::clone(&system_arc))));
    table.open();

    (Arc::new(table), system_arc, mm_arc, dir)
}

/// Simulate: a user sends messages in a group chat.
#[tokio::test]
async fn simulate_message_flow() {
    let (table, _system, _mm, _dir) = boot_kernel().await;

    // --- 1. User sends messages ---
    println!("=== Phase 1: Message ingestion ===");

    let messages = vec![
        r#"{"ts":"2026-03-20T10:00:00Z","speaker":"alice","text":"Hey everyone, let's discuss the PPT font scheme","reply_to":null}"#,
        r#"{"ts":"2026-03-20T10:01:00Z","speaker":"bob","text":"I think Helvetica looks great","reply_to":null}"#,
        r#"{"ts":"2026-03-20T10:02:00Z","speaker":"alice","text":"Helvetica is too bold, how about Georgia?","reply_to":null}"#,
        r#"{"ts":"2026-03-20T10:03:00Z","speaker":"charlie","text":"I agree, Georgia is more formal","reply_to":null}"#,
        r#"{"ts":"2026-03-20T10:04:00Z","speaker":"bob","text":"Alright, let's go with Georgia then","reply_to":null}"#,
    ];

    for msg in &messages {
        table
            .write("logos://memory/groups/chat-design/messages", msg)
            .await
            .unwrap();
    }
    println!("  ✓ Wrote {} messages", messages.len());

    // --- 2. Read back a message ---
    let msg1 = table
        .read("logos://memory/groups/chat-design/messages/1")
        .await
        .unwrap();
    let msg1_val: serde_json::Value = serde_json::from_str(&msg1).unwrap();
    assert_eq!(msg1_val["speaker"], "alice");
    assert!(msg1_val["text"].as_str().unwrap().contains("PPT font"));
    println!("  ✓ Read message 1 back correctly");

    // --- 3. FTS search ---
    println!("\n=== Phase 2: Memory search ===");

    let search_result = _mm
        .handle_call(
            "memory.search",
            r#"{"chat_id":"chat-design","query":"Georgia","limit":10}"#,
        )
        .await
        .unwrap();
    assert!(search_result.contains("Georgia"));
    println!("  ✓ FTS search for 'Georgia' returned results");

    // --- 4. Range fetch ---
    let range_result = _mm
        .handle_call(
            "memory.range_fetch",
            r#"{"chat_id":"chat-design","ranges":[[1,3]],"limit":10}"#,
        )
        .await
        .unwrap();
    let range_arr: Vec<serde_json::Value> = serde_json::from_str(&range_result).unwrap();
    assert_eq!(range_arr.len(), 3);
    println!("  ✓ Range fetch [1,3] returned 3 messages");
}

/// Simulate: runtime creates a task, agent executes it, completes with anchor.
#[tokio::test]
async fn simulate_task_lifecycle() {
    let (table, system, _mm, _dir) = boot_kernel().await;

    println!("=== Phase 3: Task lifecycle ===");

    // --- 1. Runtime creates a task ---
    table
        .write(
            "logos://system/tasks",
            r#"{"task_id":"task-001","description":"Deploy TTS service","workspace":"logos://sandbox/task-001","resource":"logos://services/tts","chat_id":"chat-ops"}"#,
        )
        .await
        .unwrap();
    println!("  ✓ Created task-001");

    // --- 2. Task should be pending (invisible to agents) ---
    let tasks = table.read("logos://system/tasks").await.unwrap();
    assert!(!tasks.contains("task-001")); // pending → hidden
    println!("  ✓ task-001 is pending (hidden from agent list)");

    // --- 3. Runtime activates the task ---
    system.transition_task("task-001", "active").await.unwrap();
    let tasks = table.read("logos://system/tasks").await.unwrap();
    assert!(tasks.contains("task-001"));
    println!("  ✓ task-001 activated and visible");

    // --- 4. Agent completes the task with anchor ---
    let result = system
        .complete(logos_system::complete::CompleteParams {
            task_id: "task-001".to_string(),
            summary: "TTS service deployed successfully using kokoro model".to_string(),
            reply: "TTS 服务已部署完成".to_string(),
            anchor: true,
            anchor_facts: r#"[{"type":"decision","topic":"TTS模型","value":"kokoro","status":"active"},{"type":"troubleshooting","symptom_snippet":"pip install failed","solution":"use --break-system-packages flag"}]"#.to_string(),
            task_log: "pip install kokoro-tts\n...\nService started on port 8080".to_string(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![],
        })
        .await
        .unwrap();
    assert!(!result.anchor_id.is_empty());
    assert_eq!(result.reply, "TTS 服务已部署完成");
    println!("  ✓ task-001 completed with anchor {}", result.anchor_id);

    // --- 5. Task should now be finished ---
    let task = table.read("logos://system/tasks/task-001").await.unwrap();
    assert!(task.contains("finished"));
    println!("  ✓ task-001 status = finished");

    // --- 6. Anchor should be readable ---
    let anchors = table.read("logos://system/anchors/task-001").await.unwrap();
    assert!(anchors.contains("kokoro"));
    println!("  ✓ Anchor readable with facts");

    // --- 7. Experience retrieval: search for past troubleshooting ---
    println!("\n=== Phase 4: Experience retrieval ===");
    let search = system.search_tasks("pip install failed", 10).await.unwrap();
    assert!(search.contains("pip install failed"));
    println!("  ✓ BM25 search found pip troubleshooting in L1 anchors");
}

/// Simulate: summary write + persona write + context assembly.
#[tokio::test]
async fn simulate_consolidator_and_context() {
    let (table, _system, mm, _dir) = boot_kernel().await;

    println!("=== Phase 5: Consolidator output simulation ===");

    // --- 1. Write messages first ---
    for i in 0..3 {
        table
            .write(
                "logos://memory/groups/chat-daily/messages",
                &format!(
                    r#"{{"ts":"2026-03-20T{:02}:00:00Z","speaker":"yao","text":"message {}"}}"#,
                    10 + i,
                    i
                ),
            )
            .await
            .unwrap();
    }

    // --- 2. Simulate consolidator writing a short summary ---
    let summary = r#"{"layer":"short","period_start":"2026-03-20T10","period_end":"2026-03-20T12","source_refs":"[[1,3]]","content":"yao 发了3条日常消息，讨论了今天的安排"}"#;
    table
        .write(
            "logos://memory/groups/chat-daily/summary/short/2026-03-20T10",
            summary,
        )
        .await
        .unwrap();
    println!("  ✓ Consolidator wrote short summary");

    // --- 3. Read latest summary ---
    let latest = table
        .read("logos://memory/groups/chat-daily/summary/short/latest")
        .await
        .unwrap();
    assert!(latest.contains("3条日常消息"));
    println!("  ✓ Latest short summary readable");

    // --- 4. Simulate context assembly (system.get_context) ---
    println!("\n=== Phase 6: Context injection ===");

    let context_result = mm
        .read(&["groups", "chat-daily", "summary", "short", "latest"])
        .await
        .unwrap();
    assert!(context_result.contains("3条日常消息"));
    println!("  ✓ Context injection can read latest summary");

    // --- 5. Memory view: recent messages ---
    let recent = mm
        .handle_call(
            "memory.view.recent",
            r#"{"chat_id":"chat-daily","limit":2}"#,
        )
        .await
        .unwrap();
    let recent_arr: Vec<serde_json::Value> = serde_json::from_str(&recent).unwrap();
    assert_eq!(recent_arr.len(), 2);
    println!("  ✓ memory.view.recent returned 2 messages");
}

/// Simulate: anchor version chain (decision superseding).
#[tokio::test]
async fn simulate_decision_evolution() {
    let (_table, system, _mm, _dir) = boot_kernel().await;

    println!("=== Phase 7: Decision evolution ===");

    // --- 1. First decision: use 黑体 ---
    system
        .create_task(
            r#"{"task_id":"task-font-1","description":"Choose PPT font","chat_id":"chat-design"}"#,
        )
        .await
        .unwrap();
    system.transition_task("task-font-1", "active")
        .await
        .unwrap();
    let r1 = system
        .complete(logos_system::complete::CompleteParams {
            task_id: "task-font-1".to_string(),
            summary: "Decided on 黑体 for PPT".to_string(),
            reply: String::new(),
            anchor: true,
            anchor_facts: r#"[{"type":"decision","topic":"PPT字体","value":"黑体","status":"active"}]"#
                .to_string(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![],
        })
        .await
        .unwrap();
    let anchor1_uri = format!("logos://system/anchors/task-font-1/{}", r1.anchor_id);
    println!("  ✓ First decision: 黑体 (anchor: {})", r1.anchor_id);

    // --- 2. Second decision supersedes first: change to 宋体 ---
    system
        .create_task(
            r#"{"task_id":"task-font-2","description":"Revise PPT font","chat_id":"chat-design"}"#,
        )
        .await
        .unwrap();
    system.transition_task("task-font-2", "active")
        .await
        .unwrap();
    let r2 = system
        .complete(logos_system::complete::CompleteParams {
            task_id: "task-font-2".to_string(),
            summary: "Changed PPT font from 黑体 to 宋体".to_string(),
            reply: String::new(),
            anchor: true,
            anchor_facts: format!(
                r#"[{{"type":"decision","topic":"PPT字体","value":"宋体","status":"active","supersedes":["{anchor1_uri}"]}}]"#
            ),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: String::new(),
            plan_todo: vec![],
        })
        .await
        .unwrap();
    println!("  ✓ Second decision: 宋体 supersedes 黑体");

    // --- 3. Verify first anchor is now superseded ---
    let anchor1 = _table
        .read(&format!(
            "logos://system/anchors/task-font-1/{}",
            r1.anchor_id
        ))
        .await
        .unwrap();
    assert!(anchor1.contains("superseded"));
    println!("  ✓ First anchor marked as superseded");

    // --- 4. Search should find the latest decision ---
    let search = system.search_tasks("PPT字体", 10).await.unwrap();
    assert!(search.contains("宋体"));
    println!("  ✓ Search finds current decision (宋体)");
    let _ = r2; // use r2
}

/// Simulate: full middleware validation.
#[tokio::test]
async fn simulate_middleware_validation() {
    let (table, _system, _mm, _dir) = boot_kernel().await;

    println!("=== Phase 8: Middleware validation ===");

    // --- 1. Valid JSON write to system should succeed ---
    table
        .write(
            "logos://system/tasks",
            r#"{"task_id":"t-valid","description":"test","chat_id":"c-1"}"#,
        )
        .await
        .unwrap();
    println!("  ✓ Valid JSON write to system/ accepted");

    // --- 2. Invalid JSON write to system should be rejected ---
    let result = table
        .write("logos://system/tasks", "this is not json{{{")
        .await;
    assert!(result.is_err());
    println!("  ✓ Invalid JSON write to system/ rejected by middleware");

    // --- 3. Invalid JSON write to memory should be rejected ---
    let result = table
        .write("logos://memory/groups/test/messages", "not json!!!")
        .await;
    assert!(result.is_err());
    println!("  ✓ Invalid JSON write to memory/ rejected by middleware");
}

// ===================================================================
// Phase 9–14: Previously untested functionality
// ===================================================================

/// Resume flow: logos_complete with resume discards current task and activates target.

/// Resume flow: logos_complete with resume discards current task and activates target.
#[tokio::test]
async fn simulate_resume_flow() {
    let (_table, system, _mm, _dir) = boot_kernel().await;

    println!("=== Phase 11: Resume flow ===");

    // Create two tasks
    system
        .create_task(r#"{"task_id":"t-current","description":"current empty task","chat_id":"c-1"}"#)
        .await
        .unwrap();
    system.transition_task("t-current", "active").await.unwrap();

    system
        .create_task(r#"{"task_id":"t-sleeping","description":"sleeping task","chat_id":"c-1"}"#)
        .await
        .unwrap();
    system.transition_task("t-sleeping", "active").await.unwrap();
    system.transition_task("t-sleeping", "sleep").await.unwrap();

    // Resume: discard t-current, activate t-sleeping
    let result = system
        .complete(logos_system::complete::CompleteParams {
            task_id: "t-current".to_string(),
            summary: "Routing to sleeping task".to_string(),
            reply: String::new(),
            anchor: false,
            anchor_facts: String::new(),
            task_log: String::new(),
            sleep_reason: String::new(),
            sleep_retry: false,
            resume_task_id: "t-sleeping".to_string(),
            plan_todo: vec![],
        })
        .await
        .unwrap();
    assert!(result.anchor_id.is_empty());
    println!("  ✓ Resume completed");

    // t-current should be deleted
    let current = system.get_task("t-current").await.unwrap();
    assert!(current.is_none());
    println!("  ✓ Current task deleted");

    // t-sleeping should be active again
    let sleeping = system.get_task("t-sleeping").await.unwrap().unwrap();
    assert!(sleeping.contains("\"active\""));
    println!("  ✓ Target task reactivated");
}

/// Summary tree drill-down: long → mid → short → range_fetch.
#[tokio::test]
async fn simulate_summary_drill_down() {
    let (table, _system, mm, _dir) = boot_kernel().await;

    println!("=== Phase 12: Summary tree drill-down ===");

    // Write some messages
    for i in 0..5 {
        table
            .write(
                "logos://memory/groups/chat-drill/messages",
                &format!(
                    r#"{{"ts":"2026-03-20T10:{:02}:00Z","speaker":"yao","text":"drill message {}"}}"#,
                    i, i
                ),
            )
            .await
            .unwrap();
    }

    // Write summaries at all 3 layers
    table
        .write(
            "logos://memory/groups/chat-drill/summary/short/2026-03-20T10",
            r#"{"layer":"short","period_start":"2026-03-20T10","period_end":"2026-03-20T10","source_refs":"[[1,5]]","content":"yao 发了5条 drill 测试消息"}"#,
        )
        .await
        .unwrap();

    table
        .write(
            "logos://memory/groups/chat-drill/summary/mid/2026-03-20",
            r#"{"layer":"mid","period_start":"2026-03-20","period_end":"2026-03-20","source_refs":"[\"2026-03-20T10\"]","content":"3月20日：yao 做了 drill 测试"}"#,
        )
        .await
        .unwrap();

    table
        .write(
            "logos://memory/groups/chat-drill/summary/long/2026-03",
            r#"{"layer":"long","period_start":"2026-03","period_end":"2026-03","source_refs":"[\"2026-03-20\"]","content":"2026年3月：drill 测试月"}"#,
        )
        .await
        .unwrap();

    // Drill down: long → mid → short → messages
    let long = table
        .read("logos://memory/groups/chat-drill/summary/long/2026-03")
        .await
        .unwrap();
    assert!(long.contains("drill 测试月"));
    println!("  ✓ Read long summary");

    let mid = table
        .read("logos://memory/groups/chat-drill/summary/mid/2026-03-20")
        .await
        .unwrap();
    assert!(mid.contains("drill 测试"));
    println!("  ✓ Read mid summary");

    let short = table
        .read("logos://memory/groups/chat-drill/summary/short/2026-03-20T10")
        .await
        .unwrap();
    assert!(short.contains("5条 drill"));
    println!("  ✓ Read short summary");

    // Range fetch raw messages from source_refs
    let messages = mm
        .handle_call(
            "memory.range_fetch",
            r#"{"chat_id":"chat-drill","ranges":[[1,5]],"limit":10}"#,
        )
        .await
        .unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_str(&messages).unwrap();
    assert_eq!(arr.len(), 5);
    println!("  ✓ Range fetch returned 5 raw messages");
}

/// Session clustering: LIKE false match prevention (#10).
#[tokio::test]
async fn session_like_no_false_match() {
    // Test that msg_id=1 doesn't falsely match msg_id=10 or msg_id=100
    let sessions = logos_mm::SessionStore::new(64, 256);

    // Insert messages with msg_ids 1, 10, 100 into separate sessions
    sessions.observe(logos_session::MsgRef {
        msg_id: 1,
        chat_id: "c-1".to_string(),
        reply_to: None,
        text: "msg one".to_string(),
        speaker: "alice".to_string(),
        ts: "2026-03-20T10:00:00Z".to_string(),
    }).await;
    sessions.observe(logos_session::MsgRef {
        msg_id: 10,
        chat_id: "c-1".to_string(),
        reply_to: None,
        text: "msg ten".to_string(),
        speaker: "bob".to_string(),
        ts: "2026-03-20T10:01:00Z".to_string(),
    }).await;
    sessions.observe(logos_session::MsgRef {
        msg_id: 100,
        chat_id: "c-1".to_string(),
        reply_to: None,
        text: "msg hundred".to_string(),
        speaker: "charlie".to_string(),
        ts: "2026-03-20T10:02:00Z".to_string(),
    }).await;

    // Each should be in its own session
    let s1 = sessions.get_session_for_msg(1).await.unwrap();
    let s10 = sessions.get_session_for_msg(10).await.unwrap();
    let s100 = sessions.get_session_for_msg(100).await.unwrap();

    assert_ne!(s1.session_id, s10.session_id);
    assert_ne!(s1.session_id, s100.session_id);
    assert_ne!(s10.session_id, s100.session_id);

    // msg_id=1 session should only have 1 message
    assert_eq!(s1.messages.len(), 1);
    assert_eq!(s1.messages[0].msg_id, 1);
    println!("  ✓ msg_id 1, 10, 100 are in separate sessions (no LIKE false match)");
}

/// FTS5 special character escaping.
#[tokio::test]
async fn fts5_special_characters() {
    let (table, _system, mm, _dir) = boot_kernel().await;

    println!("=== Phase 14: FTS5 special characters ===");

    // Write message with special characters
    table
        .write(
            "logos://memory/groups/chat-fts/messages",
            r#"{"ts":"2026-03-20T10:00:00Z","speaker":"yao","text":"npm ERR! code ERESOLVE\nnpm ERR! ERESOLVE unable to resolve dependency tree"}"#,
        )
        .await
        .unwrap();

    table
        .write(
            "logos://memory/groups/chat-fts/messages",
            r#"{"ts":"2026-03-20T10:01:00Z","speaker":"yao","text":"connection_pool.max_size = 10"}"#,
        )
        .await
        .unwrap();

    // Search for text with special chars
    let result = mm
        .handle_call(
            "memory.search",
            r#"{"chat_id":"chat-fts","query":"ERESOLVE","limit":10}"#,
        )
        .await
        .unwrap();
    assert!(result.contains("ERESOLVE"));
    println!("  ✓ FTS5 search with special chars (ERESOLVE)");

    let result = mm
        .handle_call(
            "memory.search",
            r#"{"chat_id":"chat-fts","query":"connection_pool","limit":10}"#,
        )
        .await
        .unwrap();
    assert!(result.contains("connection_pool"));
    println!("  ✓ FTS5 search with underscores");
}

/// VFS routing: unknown namespace rejected (table not open already tested in logos_vfs unit tests).
#[tokio::test]
async fn vfs_unknown_namespace_rejected() {
    let (table, _system, _mm, _dir) = boot_kernel().await;

    let result = table.read("logos://nonexistent/path").await;
    assert!(result.is_err());
    println!("  ✓ Unknown namespace rejected");
}

/// Multiple groups: messages in different groups don't leak.
#[tokio::test]
async fn multi_group_isolation() {
    let (table, _system, mm, _dir) = boot_kernel().await;

    println!("=== Phase 16: Multi-group isolation ===");

    table
        .write(
            "logos://memory/groups/group-A/messages",
            r#"{"ts":"2026-03-20T10:00:00Z","speaker":"alice","text":"secret message in A"}"#,
        )
        .await
        .unwrap();

    table
        .write(
            "logos://memory/groups/group-B/messages",
            r#"{"ts":"2026-03-20T10:00:00Z","speaker":"bob","text":"public message in B"}"#,
        )
        .await
        .unwrap();

    // Search in group-A should not find group-B content
    let result = mm
        .handle_call(
            "memory.search",
            r#"{"chat_id":"group-A","query":"public","limit":10}"#,
        )
        .await
        .unwrap();
    assert!(!result.contains("public message in B"));
    println!("  ✓ group-A search doesn't leak group-B content");

    // Search in group-B should not find group-A content
    let result = mm
        .handle_call(
            "memory.search",
            r#"{"chat_id":"group-B","query":"secret","limit":10}"#,
        )
        .await
        .unwrap();
    assert!(!result.contains("secret message in A"));
    println!("  ✓ group-B search doesn't leak group-A content");
}
