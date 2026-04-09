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
