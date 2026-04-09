use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

const SESSION_TTL: Duration = Duration::from_secs(24 * 3600);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);

/// Agent role — determines namespace permissions (RFC 002 §12.3).
#[derive(Debug, Clone, PartialEq)]
pub enum AgentRole {
    User,
    Admin,
}

impl AgentRole {
    pub fn from_str(s: &str) -> Self {
        match s {
            "admin" => Self::Admin,
            _ => Self::User,
        }
    }

    pub fn is_admin(&self) -> bool {
        matches!(self, Self::Admin)
    }
}

/// Session info resolved from a connection header.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub task_id: String,
    pub agent_config_id: String,
    pub role: AgentRole,
}

/// One-time token registry for task binding.
///
/// Flow (RFC 002 §11.2):
///   1. Runtime calls RegisterToken(token, task_id, role)
///   2. Runtime launches agent with token in env
///   3. Agent connects and calls Handshake(token)
///   4. Kernel consumes token → binds connection to task_id + role
///   5. All subsequent calls on this connection are scoped to that task
pub struct TokenRegistry {
    pending: RwLock<HashMap<String, PendingEntry>>,
    sessions: RwLock<HashMap<String, SessionEntry>>,
    counter: std::sync::atomic::AtomicU64,
}

struct PendingEntry {
    task_id: String,
    agent_config_id: String,
    role: AgentRole,
}

struct SessionEntry {
    task_id: String,
    agent_config_id: String,
    role: AgentRole,
    created_at: Instant,
}

impl TokenRegistry {
    pub fn new() -> Arc<Self> {
        let registry = Arc::new(Self {
            pending: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            counter: std::sync::atomic::AtomicU64::new(1),
        });

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

    pub async fn register(&self, token: String, task_id: String, agent_config_id: String, role: AgentRole) {
        self.pending
            .write()
            .await
            .insert(token, PendingEntry { task_id, agent_config_id, role });
    }

    pub async fn revoke(&self, token: &str) {
        self.pending.write().await.remove(token);
    }

    /// Consume a one-time token. Returns (session_key, task_id, agent_config_id).
    pub async fn consume(&self, token: &str) -> Option<(String, String, String)> {
        let entry = self.pending.write().await.remove(token)?;
        let session_key = format!(
            "s-{}",
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let agent_config_id = entry.agent_config_id.clone();
        self.sessions.write().await.insert(
            session_key.clone(),
            SessionEntry {
                task_id: entry.task_id.clone(),
                agent_config_id: entry.agent_config_id.clone(),
                role: entry.role,
                created_at: Instant::now(),
            },
        );
        Some((session_key, entry.task_id, agent_config_id))
    }

    /// Resolve a session key to task_id (backward compat).
    pub async fn resolve(&self, session_key: &str) -> Option<String> {
        self.sessions
            .read()
            .await
            .get(session_key)
            .map(|e| e.task_id.clone())
    }

    /// Resolve a session key to full session info (task_id + role).
    pub async fn resolve_info(&self, session_key: &str) -> Option<SessionInfo> {
        self.sessions.read().await.get(session_key).map(|e| SessionInfo {
            task_id: e.task_id.clone(),
            agent_config_id: e.agent_config_id.clone(),
            role: e.role.clone(),
        })
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
        reg.register("tok-1".to_string(), "task-001".to_string(), String::new(), AgentRole::User)
            .await;
        let (key, task_id, _) = reg.consume("tok-1").await.unwrap();
        assert_eq!(task_id, "task-001");
        assert!(key.starts_with("s-"));
    }

    #[tokio::test]
    async fn consume_is_one_time() {
        let reg = TokenRegistry::new();
        reg.register("tok-2".to_string(), "task-002".to_string(), String::new(), AgentRole::User)
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
        reg.register("tok-3".to_string(), "task-003".to_string(), "agent-A".to_string(), AgentRole::Admin)
            .await;
        let (key, _, _) = reg.consume("tok-3").await.unwrap();
        let info = reg.resolve_info(&key).await.unwrap();
        assert_eq!(info.task_id, "task-003");
        assert!(info.role.is_admin());
    }

    #[tokio::test]
    async fn revoke_pending_token() {
        let reg = TokenRegistry::new();
        reg.register("tok-4".to_string(), "task-004".to_string(), String::new(), AgentRole::User)
            .await;
        reg.revoke("tok-4").await;
        assert!(reg.consume("tok-4").await.is_none());
    }

    #[tokio::test]
    async fn user_role_default() {
        let reg = TokenRegistry::new();
        reg.register("tok-5".to_string(), "task-005".to_string(), String::new(), AgentRole::User)
            .await;
        let (key, _, _) = reg.consume("tok-5").await.unwrap();
        let info = reg.resolve_info(&key).await.unwrap();
        assert!(!info.role.is_admin());
    }
}
