//! Memory View Plugin trait — RFC 003 §10.6, §11.
//!
//! Memory views are derived indexes over the immutable message SSOT.
//! Each plugin exposes its own natural URI structure under
//! `logos://memory/groups/{gid}/{plugin_name}/`.

use async_trait::async_trait;
use logos_vfs::VfsError;
use sqlx::SqlitePool;

/// A pluggable memory view module (RFC 003 §11.2).
#[async_trait]
pub trait MemoryPlugin: Send + Sync {
    /// Plugin name used in URI path (e.g. "summary", "graph").
    fn name(&self) -> &str;

    /// Documentation injected into system prompt (RFC 003 §11.3).
    fn docs(&self) -> &str;

    /// Initialize plugin-specific tables.
    async fn init_schema(&self, pool: &SqlitePool) -> Result<(), VfsError>;

    /// Read at `logos://memory/groups/{gid}/{name}/{...path}`.
    async fn read(&self, pool: &SqlitePool, chat_id: &str, path: &[&str]) -> Result<String, VfsError>;

    /// Write at `logos://memory/groups/{gid}/{name}/{...path}`.
    async fn write(&self, pool: &SqlitePool, chat_id: &str, path: &[&str], content: &str) -> Result<(), VfsError>;

    /// Patch at `logos://memory/groups/{gid}/{name}/{...path}`.
    async fn patch(&self, pool: &SqlitePool, chat_id: &str, path: &[&str], partial: &str) -> Result<(), VfsError>;
}
