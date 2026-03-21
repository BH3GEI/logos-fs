pub mod middleware;
pub mod namespace;
pub mod table;
pub mod uri;

pub use middleware::{JsonValidator, VfsMiddleware};
pub use namespace::Namespace;
pub use table::RoutingTable;

/// Deep-merge two JSON values. Objects are recursively merged; other types are replaced.
pub fn json_deep_merge(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (Some(base_obj), Some(patch_obj)) = (base.as_object_mut(), patch.as_object()) {
        for (key, value) in patch_obj {
            let entry = base_obj
                .entry(key.clone())
                .or_insert(serde_json::Value::Null);
            json_deep_merge(entry, value);
        }
    } else {
        *base = patch.clone();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VfsError {
    #[error("invalid URI: {0}")]
    InvalidUri(String),

    #[error("namespace not mounted: {0}")]
    NamespaceNotMounted(String),

    #[error("kernel not ready — boot sequence incomplete")]
    NotReady,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("invalid json: {0}")]
    InvalidJson(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("sqlite error: {0}")]
    Sqlite(String),
}
