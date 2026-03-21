use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

const SESSION_TTL: Duration = Duration::from_secs(24 * 3600); // 24 hours
const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour

/// One-time token registry for task binding.
///
/// Flow (RFC 002 §11.2):
///   1. Runtime calls RegisterToken(token, task_id)
///   2. Runtime launches agent with token in env
///   3. Agent connects and calls Handshake(token)
///   4. Kernel consumes token → binds connection to task_id
///   5. All subsequent calls on this connection are scoped to that task
pub struct TokenRegistry {
    pending: RwLock<HashMap<String, String>>,
    sessions: RwLock<HashMap<String, SessionEntry>>,
    counter: std::sync::atomic::AtomicU64,
}

struct SessionEntry {
    task_id: String,
    created_at: Instant,
}

impl TokenRegistry {
    pub fn new() -> Arc<Self> {
        let registry = Arc::new(Self {
            pending: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            counter: std::sync::atomic::AtomicU64::new(1),
        });

        // Background TTL cleanup
        let weak = Arc::clone(&registry);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            loop {
                interval.tick().await;
                weak.cleanup_expired().await;
            }
        });

        registry
    }

    pub async fn register(&self, token: String, task_id: String) {
        self.pending.write().await.insert(token, task_id);
    }

    /// Revoke a pending token (RFC 002 §8.2).
    pub async fn revoke(&self, token: &str) {
        self.pending.write().await.remove(token);
    }

    pub async fn consume(&self, token: &str) -> Option<(String, String)> {
        let task_id = self.pending.write().await.remove(token)?;
        let session_key = format!(
            "s-{}",
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        self.sessions.write().await.insert(
            session_key.clone(),
            SessionEntry {
                task_id: task_id.clone(),
                created_at: Instant::now(),
            },
        );
        Some((session_key, task_id))
    }

    pub async fn resolve(&self, session_key: &str) -> Option<String> {
        self.sessions
            .read()
            .await
            .get(session_key)
            .map(|e| e.task_id.clone())
    }

    async fn cleanup_expired(&self) {
        let now = Instant::now();
        self.sessions
            .write()
            .await
            .retain(|_, entry| now.duration_since(entry.created_at) < SESSION_TTL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_consume() {
        let reg = TokenRegistry::new();
        reg.register("tok-1".to_string(), "task-001".to_string())
            .await;
        let (key, task_id) = reg.consume("tok-1").await.unwrap();
        assert_eq!(task_id, "task-001");
        assert!(key.starts_with("s-"));
    }

    #[tokio::test]
    async fn consume_is_one_time() {
        let reg = TokenRegistry::new();
        reg.register("tok-2".to_string(), "task-002".to_string())
            .await;
        assert!(reg.consume("tok-2").await.is_some());
        assert!(reg.consume("tok-2").await.is_none());
    }

    #[tokio::test]
    async fn unknown_token_returns_none() {
        let reg = TokenRegistry::new();
        assert!(reg.consume("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn resolve_after_handshake() {
        let reg = TokenRegistry::new();
        reg.register("tok-3".to_string(), "task-003".to_string())
            .await;
        let (key, _) = reg.consume("tok-3").await.unwrap();
        assert_eq!(reg.resolve(&key).await, Some("task-003".to_string()));
    }

    #[tokio::test]
    async fn revoke_pending_token() {
        let reg = TokenRegistry::new();
        reg.register("tok-4".to_string(), "task-004".to_string())
            .await;
        reg.revoke("tok-4").await;
        assert!(reg.consume("tok-4").await.is_none());
    }
}
