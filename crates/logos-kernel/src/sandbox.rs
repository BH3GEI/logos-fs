use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

/// Sandbox namespace — `logos://sandbox/`.
///
/// Each task gets a directory: `{root}/{task_id}/`.
/// Provides file read/write via the Namespace trait,
/// and shell execution via the `exec` method.
///
/// No container isolation — just a local directory.
/// Swap in containerd/overlayfs later without changing the agent interface.
pub struct SandboxNs {
    root: PathBuf,
}

pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

impl SandboxNs {
    pub fn init(root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&root)
            .map_err(|e| VfsError::Io(format!("create sandbox dir: {e}")))?;
        Ok(Self { root })
    }

    /// Execute a shell command with logos://sandbox/ URI translation.
    ///
    /// Scans the command for `logos://sandbox/` URIs and replaces them
    /// with real filesystem paths before executing.
    pub async fn exec(&self, command: &str) -> Result<ExecResult, VfsError> {
        let root_str = self.root.to_string_lossy().to_string();
        let translated = command.replace("logos://sandbox/", &format!("{root_str}/"));

        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&translated)
            .output()
            .await
            .map_err(|e| VfsError::Io(format!("exec failed: {e}")))?;

        Ok(ExecResult {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code().unwrap_or(-1),
        })
    }
}

#[async_trait]
impl Namespace for SandboxNs {
    fn name(&self) -> &str {
        "sandbox"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty sandbox path".to_string()));
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
                    VfsError::NotFound(format!("logos://sandbox/{}", path.join("/")))
                }
                _ => VfsError::Io(format!("read {}: {e}", file_path.display())),
            })
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty sandbox path".to_string()));
        }
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

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        // Sandbox files: append for log, overwrite for everything else
        if path.last().map(|s| *s) == Some("log") {
            // Append to log file
            let file_path = self.root.join(path.join("/"));
            if let Some(parent) = file_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| VfsError::Io(format!("mkdir {}: {e}", parent.display())))?;
            }
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .await
                .map_err(|e| VfsError::Io(format!("open log {}: {e}", file_path.display())))?;
            file.write_all(partial.as_bytes())
                .await
                .map_err(|e| VfsError::Io(format!("append log: {e}")))?;
            file.write_all(b"\n")
                .await
                .map_err(|e| VfsError::Io(format!("append newline: {e}")))?;
            return Ok(());
        }
        self.write(path, partial).await
    }
}
