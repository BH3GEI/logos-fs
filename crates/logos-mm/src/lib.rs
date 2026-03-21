mod messages;
mod summaries;
pub mod views;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

use messages::MessageDb;
use summaries::SummaryDb;
use views::MemoryView;

pub use logos_session::SessionStore;

/// The Memory Module — handles `logos://memory/`.
///
/// Path-mapped reads (RFC 003 §3.1):
///   logos://memory/groups/{gid}/messages/{msg_id}           → single message
///   logos://memory/groups/{gid}/summary/{layer}/latest      → latest summary
///   logos://memory/groups/{gid}/summary/{layer}/{period}    → summary by period
///
/// Path-mapped writes:
///   logos://memory/groups/{gid}/messages                    → insert message (JSON)
///   logos://memory/groups/{gid}/summary/{layer}/{period}    → insert summary (JSON)
///
/// Complex queries go through logos_call (proc tools) — RFC 003 §3.2:
///   call("memory.search",      { chat_id, query, limit })
///   call("memory.range_fetch", { chat_id, ranges, limit, offset })
pub struct MemoryModule {
    messages: MessageDb,
    sessions: Arc<SessionStore>,
    views: HashMap<String, Box<dyn views::MemoryView>>,
}

impl MemoryModule {
    pub fn init(db_root: PathBuf, sessions: Arc<SessionStore>) -> Result<Self, VfsError> {
        let messages = MessageDb::new(db_root)?;
        let mut view_map: HashMap<String, Box<dyn views::MemoryView>> = HashMap::new();
        let by_speaker = views::BySpeakerView;
        view_map.insert(by_speaker.name().to_string(), Box::new(by_speaker));
        let recent = views::RecentView;
        view_map.insert(recent.name().to_string(), Box::new(recent));
        Ok(Self {
            messages,
            sessions,
            views: view_map,
        })
    }

    /// Access the session store (for runtime context injection — RFC 003 §5.5).
    pub fn sessions(&self) -> &Arc<SessionStore> {
        &self.sessions
    }

    // --- call handlers (proc tools) ---

    /// Dispatch a logos_call to the appropriate handler.
    pub async fn handle_call(&self, tool: &str, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let chat_id = val["chat_id"].as_str().unwrap_or_default();

        match tool {
            "memory.search" => {
                let query = val["query"].as_str().unwrap_or_default();
                let limit = val["limit"].as_i64().unwrap_or(10);
                self.messages.search_fts(chat_id, query, limit).await
            }
            "memory.range_fetch" => {
                self.messages.range_fetch(chat_id, params).await
            }
            _ if tool.starts_with("memory.view.") => {
                let view_name = tool.strip_prefix("memory.view.").unwrap_or("");
                let view = self.views.get(view_name).ok_or_else(|| {
                    VfsError::NotFound(format!("unknown view: {view_name}"))
                })?;
                let pool = self.messages.pool(chat_id).await?;
                view.query(&pool, chat_id, params).await
            }
            _ => Err(VfsError::NotFound(format!("unknown memory tool: {tool}"))),
        }
    }
}

#[async_trait]
impl Namespace for MemoryModule {
    fn name(&self) -> &str {
        "memory"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        if path.len() < 3 || path[0] != "groups" {
            return Err(VfsError::InvalidPath(
                "expected logos://memory/groups/{gid}/...".to_string(),
            ));
        }
        let gid = path[1];
        let resource = path[2];

        match resource {
            "messages" => {
                let msg_id_str = path.get(3).ok_or_else(|| {
                    VfsError::InvalidPath("expected logos://memory/groups/{gid}/messages/{msg_id}".to_string())
                })?;
                let msg_id: i64 = msg_id_str
                    .parse()
                    .map_err(|_| VfsError::InvalidPath(format!("invalid msg_id: {msg_id_str}")))?;
                self.messages
                    .get_by_id(gid, msg_id)
                    .await
                    .map(|opt| opt.unwrap_or_else(|| "null".to_string()))
            }
            "summary" => {
                let layer = path.get(3).ok_or_else(|| {
                    VfsError::InvalidPath("missing summary layer".to_string())
                })?;
                let period = path.get(4).copied().unwrap_or("latest");
                let pool = self.messages.pool(gid).await?;
                SummaryDb::ensure_schema(&pool).await?;
                SummaryDb::read(pool, gid, layer, period)
                    .await
                    .map(|opt| opt.unwrap_or_else(|| "null".to_string()))
            }
            // logos://memory/groups/{gid}/views/{view_name}
            "views" => {
                let view_name = path.get(3).ok_or_else(|| {
                    VfsError::InvalidPath("missing view name".to_string())
                })?;
                let view = self.views.get(*view_name).ok_or_else(|| {
                    VfsError::NotFound(format!("unknown view: {view_name}"))
                })?;
                // Remaining path segments joined as params JSON (or empty)
                let params = path.get(4).copied().unwrap_or("{}");
                let pool = self.messages.pool(gid).await?;
                view.query(&pool, gid, params).await
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unknown memory resource: {resource}"
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.len() < 3 || path[0] != "groups" {
            return Err(VfsError::InvalidPath(
                "expected logos://memory/groups/{gid}/...".to_string(),
            ));
        }
        let gid = path[1];
        let resource = path[2];

        match resource {
            "messages" => {
                let msg_id = self.messages.insert(gid, content).await?;

                // Update session topology (RFC 003 §5.1)
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(content) {
                    let msg_ref = logos_session::MsgRef {
                        msg_id,
                        chat_id: gid.to_string(),
                        reply_to: val["reply_to"].as_i64(),
                        text: val["text"].as_str().unwrap_or_default().to_string(),
                        speaker: val["speaker"].as_str().unwrap_or_default().to_string(),
                        ts: val["ts"].as_str().unwrap_or_default().to_string(),
                    };
                    self.sessions.observe(msg_ref).await;
                }

                Ok(())
            }
            "summary" => {
                if path.len() < 5 {
                    return Err(VfsError::InvalidPath(
                        "expected logos://memory/groups/{gid}/summary/{layer}/{period}".to_string(),
                    ));
                }
                let pool = self.messages.pool(gid).await?;
                SummaryDb::ensure_schema(&pool).await?;
                SummaryDb::write(pool, gid, content).await
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unknown memory resource for write: {resource}"
            ))),
        }
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        // For summary resources: read-merge-write with deep merge.
        // For messages: patch is not meaningful (append-only), fall through to write.
        if path.len() >= 3 && path[2] == "summary" {
            let existing = self.read(path).await.unwrap_or_else(|_| "null".to_string());
            if let (Ok(mut base), Ok(patch_val)) = (
                serde_json::from_str::<serde_json::Value>(&existing),
                serde_json::from_str::<serde_json::Value>(partial),
            ) {
                if !base.is_null() {
                    logos_vfs::json_deep_merge(&mut base, &patch_val);
                    let merged = serde_json::to_string(&base)
                        .unwrap_or_else(|_| partial.to_string());
                    return self.write(path, &merged).await;
                }
            }
        }
        self.write(path, partial).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logos_vfs::Namespace;

    async fn test_mm() -> (MemoryModule, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let sessions = Arc::new(SessionStore::new(10, 10));
        let mm = MemoryModule::init(dir.path().to_path_buf(), sessions).unwrap();
        (mm, dir)
    }

    #[tokio::test]
    async fn insert_and_get_message() {
        let (mm, _dir) = test_mm().await;
        let msg = r#"{"ts":"2026-03-20T10:00:00Z","speaker":"alice","text":"hello","reply_to":null}"#;
        mm.write(&["groups", "chat-1", "messages"], msg).await.unwrap();

        let result = mm.read(&["groups", "chat-1", "messages", "1"]).await.unwrap();
        let val: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(val["speaker"], "alice");
        assert_eq!(val["text"], "hello");
    }

    #[tokio::test]
    async fn search_fts() {
        let (mm, _dir) = test_mm().await;
        mm.write(&["groups", "chat-1", "messages"],
            r#"{"ts":"2026-03-20T10:00:00Z","speaker":"bob","text":"rust is great"}"#
        ).await.unwrap();
        mm.write(&["groups", "chat-1", "messages"],
            r#"{"ts":"2026-03-20T10:01:00Z","speaker":"alice","text":"python is nice"}"#
        ).await.unwrap();

        let result = mm.handle_call("memory.search",
            r#"{"chat_id":"chat-1","query":"rust","limit":10}"#
        ).await.unwrap();
        assert!(result.contains("rust is great"));
        assert!(!result.contains("python"));
    }

    #[tokio::test]
    async fn range_fetch() {
        let (mm, _dir) = test_mm().await;
        for i in 0..5 {
            mm.write(&["groups", "chat-1", "messages"],
                &format!(r#"{{"ts":"2026-03-20T10:{i:02}:00Z","speaker":"user","text":"msg {i}"}}"#)
            ).await.unwrap();
        }

        let result = mm.handle_call("memory.range_fetch",
            r#"{"chat_id":"chat-1","ranges":[[2,4]],"limit":10}"#
        ).await.unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(arr.len(), 3); // msg_id 2,3,4
    }

    #[tokio::test]
    async fn summary_write_and_read() {
        let (mm, _dir) = test_mm().await;
        let summary = r#"{"layer":"short","period_start":"2026-03-20T10","period_end":"2026-03-20T10","source_refs":"[[1,5]]","content":"test summary"}"#;
        mm.write(&["groups", "chat-1", "summary", "short", "2026-03-20T10"], summary)
            .await.unwrap();

        let result = mm.read(&["groups", "chat-1", "summary", "short", "latest"]).await.unwrap();
        assert!(result.contains("test summary"));
    }

    #[tokio::test]
    async fn view_recent() {
        let (mm, _dir) = test_mm().await;
        for i in 0..3 {
            mm.write(&["groups", "chat-1", "messages"],
                &format!(r#"{{"ts":"2026-03-20T10:{i:02}:00Z","speaker":"user","text":"msg {i}"}}"#)
            ).await.unwrap();
        }

        let result = mm.handle_call("memory.view.recent",
            r#"{"chat_id":"chat-1","limit":2}"#
        ).await.unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[tokio::test]
    async fn view_by_speaker() {
        let (mm, _dir) = test_mm().await;
        mm.write(&["groups", "chat-1", "messages"],
            r#"{"ts":"2026-03-20T10:00:00Z","speaker":"alice","text":"hello from alice"}"#
        ).await.unwrap();
        mm.write(&["groups", "chat-1", "messages"],
            r#"{"ts":"2026-03-20T10:01:00Z","speaker":"bob","text":"hello from bob"}"#
        ).await.unwrap();

        let result = mm.handle_call("memory.view.by_speaker",
            r#"{"chat_id":"chat-1","speaker":"alice","limit":10}"#
        ).await.unwrap();
        assert!(result.contains("alice"));
        assert!(!result.contains("bob"));
    }
}
