use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

/// Users namespace — `logos://users/`.
///
/// Backed by the filesystem. Each user gets a directory under `root/{uid}/`.
/// Files are read/written as-is. Directories are listed as JSON arrays.
///
/// Special behavior for persona paths (RFC 003 §8.1):
///   - `persona/short`: append-only (each write appends to a JSON array)
///   - `persona/mid`, `persona/long.md`: overwrite (daily/weekly rewrite)
pub struct UsersNs {
    root: PathBuf,
}

impl UsersNs {
    pub fn init(root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&root)
            .map_err(|e| VfsError::Io(format!("create users dir: {e}")))?;
        Ok(Self { root })
    }

    /// Write a file, creating parent directories as needed.
    async fn write_file(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        let file_path = self.root.join(path.join("/"));
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| VfsError::Io(format!("mkdir {}: {e}", parent.display())))?;
        }
        tokio::fs::write(&file_path, content)
            .await
            .map_err(|e| VfsError::Io(format!("write {}: {e}", file_path.display())))
    }

    /// Append a new entry to persona/short/{period} file.
    ///
    /// RFC 003 §8.1: short is append-only, one entry per hour.
    /// Path: logos://users/{uid}/persona/short/{period}
    /// If no period in path, appends to a default file.
    /// Each file is a JSON array; each write appends one entry.
    async fn append_persona_short(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        // path: [uid, "persona", "short", period?]
        let file_path = if path.len() >= 4 {
            // logos://users/{uid}/persona/short/{period}
            self.root.join(path.join("/"))
        } else {
            // logos://users/{uid}/persona/short → use "latest" as default
            self.root.join(format!("{}/persona/short/latest", path[0]))
        };

        let existing = tokio::fs::read_to_string(&file_path)
            .await
            .unwrap_or_else(|_| "[]".to_string());
        let mut entries: Vec<serde_json::Value> =
            serde_json::from_str(&existing).unwrap_or_default();
        let new_entry: serde_json::Value = serde_json::from_str(content)
            .unwrap_or(serde_json::Value::String(content.to_string()));
        entries.push(new_entry);
        let merged = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string());

        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| VfsError::Io(format!("mkdir {}: {e}", parent.display())))?;
        }
        tokio::fs::write(&file_path, merged)
            .await
            .map_err(|e| VfsError::Io(format!("write {}: {e}", file_path.display())))
    }
}

#[async_trait]
impl Namespace for UsersNs {
    fn name(&self) -> &str {
        "users"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }
        let file_path = self.root.join(path.join("/"));

        if file_path.is_dir() {
            let mut entries = Vec::new();
            let dir = std::fs::read_dir(&file_path)
                .map_err(|e| VfsError::Io(format!("read dir {}: {e}", file_path.display())))?;
            for entry in dir.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
            return Ok(serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string()));
        }

        tokio::fs::read_to_string(&file_path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    VfsError::NotFound(format!("logos://users/{}", path.join("/")))
                }
                _ => VfsError::Io(format!("read {}: {e}", file_path.display())),
            })
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }

        // RFC 003 §8.1: persona/short is append-only
        if path.len() >= 3 && path[1] == "persona" && path[2] == "short" {
            return self.append_persona_short(path, content).await;
        }

        self.write_file(path, content).await
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }
        let file_path = self.root.join(path.join("/"));

        let existing = tokio::fs::read_to_string(&file_path)
            .await
            .unwrap_or_default();

        if let (Ok(mut base), Ok(patch)) = (
            serde_json::from_str::<serde_json::Value>(&existing),
            serde_json::from_str::<serde_json::Value>(partial),
        ) {
            logos_vfs::json_deep_merge(&mut base, &patch);
            let merged =
                serde_json::to_string_pretty(&base).unwrap_or_else(|_| partial.to_string());
            return self.write_file(path, &merged).await;
        }

        self.write_file(path, partial).await
    }
}
