//! Built-in proc tools that bridge kernel modules to the ProcTool trait.

use std::sync::Arc;

use async_trait::async_trait;
use logos_vfs::VfsError;

use crate::proc::ProcTool;

/// memory.search — FTS full-text search (RFC 003 §3.2)
pub struct MemorySearchTool {
    pub mm: Arc<logos_mm::MemoryModule>,
}

#[async_trait]
impl ProcTool for MemorySearchTool {
    fn name(&self) -> &str {
        "memory.search"
    }
    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "memory.search",
            "description": "Full-text search over chat messages.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": { "type": "string", "description": "Group chat ID" },
                    "query": { "type": "string", "description": "Search query" },
                    "limit": { "type": "integer", "default": 10 }
                },
                "required": ["chat_id", "query"]
            }
        })
    }
    async fn call(&self, params: &str) -> Result<String, VfsError> {
        self.mm.handle_call("memory.search", params).await
    }
}

/// memory.range_fetch — paginated message range query (RFC 003 §3.2)
pub struct MemoryRangeFetchTool {
    pub mm: Arc<logos_mm::MemoryModule>,
}

#[async_trait]
impl ProcTool for MemoryRangeFetchTool {
    fn name(&self) -> &str {
        "memory.range_fetch"
    }
    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "memory.range_fetch",
            "description": "Fetch messages within msg_id ranges with pagination.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": { "type": "string", "description": "Group chat ID" },
                    "ranges": { "type": "array", "items": { "type": "array", "items": { "type": "integer" } }, "description": "Array of [start, end] msg_id pairs" },
                    "limit": { "type": "integer", "default": 50 },
                    "offset": { "type": "integer", "default": 0 }
                },
                "required": ["chat_id", "ranges"]
            }
        })
    }
    async fn call(&self, params: &str) -> Result<String, VfsError> {
        self.mm.handle_call("memory.range_fetch", params).await
    }
}

/// system.search_tasks — multi-level experience retrieval (RFC 003 §6.2)
pub struct SystemSearchTasksTool {
    pub system: Arc<logos_system::SystemModule>,
}

#[async_trait]
impl ProcTool for SystemSearchTasksTool {
    fn name(&self) -> &str {
        "system.search_tasks"
    }
    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "system.search_tasks",
            "description": "Search tasks and anchors for relevant experience. Returns L1 (anchor facts) and L2 (task metadata) hits.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query (keyword or error snippet)" },
                    "limit": { "type": "integer", "default": 10 }
                },
                "required": ["query"]
            }
        })
    }
    async fn call(&self, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let query = val["query"].as_str().unwrap_or_default();
        let limit = val["limit"].as_i64().unwrap_or(10);
        self.system.search_tasks(query, limit).await
    }
}

/// system.get_context — auto-inject context for agent turn (RFC 003 §5.5)
pub struct SystemGetContextTool {
    pub mm: Arc<logos_mm::MemoryModule>,
    pub sessions: Arc<logos_mm::SessionStore>,
}

#[async_trait]
impl ProcTool for SystemGetContextTool {
    fn name(&self) -> &str {
        "system.get_context"
    }
    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "system.get_context",
            "description": "Assemble context for an agent turn: session + recent summary + sender persona.",
            "parameters": {
                "type": "object",
                "properties": {
                    "chat_id": { "type": "string", "description": "Group chat ID" },
                    "sender_uid": { "type": "string", "description": "Message sender's user ID" },
                    "msg_id": { "type": "integer", "description": "Trigger message ID (optional)" }
                },
                "required": ["chat_id", "sender_uid"]
            }
        })
    }
    async fn call(&self, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let chat_id = val["chat_id"].as_str().unwrap_or_default();
        let sender_uid = val["sender_uid"].as_str().unwrap_or_default();
        let msg_id = val["msg_id"].as_i64();
        use logos_vfs::Namespace;

        // 1. Session from clustering module
        let session = if let Some(mid) = msg_id {
            self.sessions.get_session_for_msg(mid).await.map(|s| s.to_json())
        } else {
            self.sessions.get_active_session(chat_id).await.map(|s| s.to_json())
        }
        .unwrap_or(serde_json::json!(null));

        // 2. Latest short summary
        let summary = self.mm
            .read(&["groups", chat_id, "summary", "short", "latest"])
            .await
            .unwrap_or_else(|_| "null".to_string());

        // 3+4. Sender persona (read through users namespace would need table ref,
        // so we return instructions for the runtime to inject these)
        let context = serde_json::json!({
            "session": session,
            "recent_summary": serde_json::from_str::<serde_json::Value>(&summary)
                .unwrap_or(serde_json::json!(null)),
            "sender_uid": sender_uid,
            "persona_paths": {
                "long": format!("logos://users/{sender_uid}/persona/long.md"),
                "mid": format!("logos://users/{sender_uid}/persona/mid/"),
            }
        });
        Ok(context.to_string())
    }
}
