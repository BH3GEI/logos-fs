use async_trait::async_trait;

use crate::VfsError;

/// VFS middleware — intercepts operations before/after routing.
///
/// RFC 002 §12.6: verification and inspection layer.
#[async_trait]
pub trait VfsMiddleware: Send + Sync {
    /// Called before a read operation. Return Err to reject.
    async fn before_read(&self, _uri: &str) -> Result<(), VfsError> {
        Ok(())
    }

    /// Called before a write operation. Can validate content.
    async fn before_write(&self, _uri: &str, _content: &str) -> Result<(), VfsError> {
        Ok(())
    }

    /// Called before a patch operation.
    async fn before_patch(&self, _uri: &str, _partial: &str) -> Result<(), VfsError> {
        Ok(())
    }

    /// Called after any operation completes (for audit logging, metrics, etc).
    async fn after_op(&self, _op: &str, _uri: &str, _success: bool) {}
}

/// Built-in middleware: validates that JSON content is well-formed
/// when writing to `system/` or `memory/` namespaces.
pub struct JsonValidator;

#[async_trait]
impl VfsMiddleware for JsonValidator {
    async fn before_write(&self, uri: &str, content: &str) -> Result<(), VfsError> {
        if needs_json_validation(uri) && !content.is_empty() {
            serde_json::from_str::<serde_json::Value>(content)
                .map_err(|e| VfsError::InvalidJson(format!("write to {uri}: {e}")))?;
        }
        Ok(())
    }

    async fn before_patch(&self, uri: &str, partial: &str) -> Result<(), VfsError> {
        if needs_json_validation(uri) && !partial.is_empty() {
            serde_json::from_str::<serde_json::Value>(partial)
                .map_err(|e| VfsError::InvalidJson(format!("patch to {uri}: {e}")))?;
        }
        Ok(())
    }
}

fn needs_json_validation(uri: &str) -> bool {
    // system/ and memory/ writes must be JSON, except for plain-text fields like description
    if uri.ends_with("/description") {
        return false;
    }
    uri.starts_with("logos://system/") || uri.starts_with("logos://memory/")
}
