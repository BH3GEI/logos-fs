use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};
use tokio::sync::Mutex;

/// Runtime service entry — in-memory registry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ServiceEntry {
    name: String,
    source: String,     // "builtin" | "agent"
    svc_type: String,   // "oneshot" | "daemon"
    endpoint: String,   // daemon network address (empty for oneshot)
    status: String,     // "running" | "stopped"
}

/// Services namespace — `logos://services/`.
///
/// Two-layer architecture (RFC 002 §6):
///
/// **svc-store** (persistent, filesystem-backed):
///   logos://svc-store/{name}/compose.yaml   → service declaration
///   logos://svc-store/{name}/artifacts/...   → build artifacts
///
/// **services registry** (runtime, in-memory):
///   logos://services/                        → list running services
///   logos://services/{name}                  → service details
///   logos://services/{name}                  → write: register/update
///
/// The kernel only maintains the registry. Actual service start/stop
/// is the runtime's responsibility.
pub struct ServicesNs {
    store_root: PathBuf,
    registry: Mutex<HashMap<String, ServiceEntry>>,
}

impl ServicesNs {
    pub fn init(store_root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&store_root)
            .map_err(|e| VfsError::Io(format!("create svc-store dir: {e}")))?;
        Ok(Self {
            store_root,
            registry: Mutex::new(HashMap::new()),
        })
    }

    /// Restore services from svc-store on boot.
    ///
    /// Scans `store_root` for directories containing `compose.yaml`,
    /// parses them, and registers as stopped services.
    pub async fn restore_from_store(&self) -> Result<usize, VfsError> {
        let dir = std::fs::read_dir(&self.store_root)
            .map_err(|e| VfsError::Io(format!("read svc-store: {e}")))?;

        let mut count = 0;
        let mut reg = self.registry.lock().await;

        for entry in dir.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let compose_path = entry.path().join("compose.yaml");

            if !compose_path.exists() {
                continue;
            }

            let content = match std::fs::read_to_string(&compose_path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Parse compose.yaml — extract name, image, command
            let yaml: serde_json::Value = match serde_yaml::from_str(&content) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let svc_name = yaml["name"].as_str().unwrap_or(&name).to_string();
            let entry = ServiceEntry {
                name: svc_name.clone(),
                source: "agent".to_string(),
                svc_type: if yaml["command"].is_string() {
                    "daemon".to_string()
                } else {
                    "oneshot".to_string()
                },
                endpoint: String::new(),
                status: "stopped".to_string(),
            };

            reg.insert(svc_name, entry);
            count += 1;
        }

        Ok(count)
    }
}

#[async_trait]
impl Namespace for ServicesNs {
    fn name(&self) -> &str {
        "services"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        match path.len() {
            // logos://services/ → list
            0 => {
                let reg = self.registry.lock().await;
                let entries: Vec<&ServiceEntry> = reg.values().collect();
                Ok(serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string()))
            }
            // logos://services/{name} → details
            1 => {
                let reg = self.registry.lock().await;
                match reg.get(path[0]) {
                    Some(entry) => Ok(serde_json::to_string(entry)
                        .unwrap_or_else(|_| "null".to_string())),
                    None => Err(VfsError::NotFound(format!(
                        "service not registered: {}",
                        path[0]
                    ))),
                }
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unexpected services path: {}",
                path.join("/")
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath(
                "expected logos://services/{name}".to_string(),
            ));
        }
        let name = path[0];

        // Register or update a service entry
        let entry: ServiceEntry = serde_json::from_str(content)
            .map_err(|e| VfsError::InvalidJson(format!("invalid service entry: {e}")))?;

        let mut reg = self.registry.lock().await;
        reg.insert(name.to_string(), entry);
        Ok(())
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath(
                "expected logos://services/{name}".to_string(),
            ));
        }
        let name = path[0];

        let mut reg = self.registry.lock().await;
        let existing = reg.get(name).ok_or_else(|| {
            VfsError::NotFound(format!("service not registered: {name}"))
        })?;

        let mut base = serde_json::to_value(existing)
            .map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        let patch_val: serde_json::Value = serde_json::from_str(partial)
            .map_err(|e| VfsError::InvalidJson(e.to_string()))?;
        logos_vfs::json_deep_merge(&mut base, &patch_val);

        let updated: ServiceEntry = serde_json::from_value(base)
            .map_err(|e| VfsError::InvalidJson(format!("merge result invalid: {e}")))?;
        reg.insert(name.to_string(), updated);
        Ok(())
    }
}

/// SvcStore namespace — `logos://svc-store/`.
///
/// Filesystem-backed persistent storage for service declarations.
/// Similar to UsersNs but for service artifacts.
pub struct SvcStoreNs {
    root: PathBuf,
}

impl SvcStoreNs {
    pub fn init(root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&root)
            .map_err(|e| VfsError::Io(format!("create svc-store dir: {e}")))?;
        Ok(Self { root })
    }
}

#[async_trait]
impl Namespace for SvcStoreNs {
    fn name(&self) -> &str {
        "svc-store"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        if path.is_empty() {
            // List all services in store
            let mut entries = Vec::new();
            let dir = std::fs::read_dir(&self.root)
                .map_err(|e| VfsError::Io(format!("read svc-store: {e}")))?;
            for entry in dir.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
            return Ok(serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string()));
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
                    VfsError::NotFound(format!("logos://svc-store/{}", path.join("/")))
                }
                _ => VfsError::Io(format!("read {}: {e}", file_path.display())),
            })
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty svc-store path".to_string()));
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
        self.write(path, partial).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_read_service() {
        let dir = tempfile::tempdir().unwrap();
        let ns = ServicesNs::init(dir.path().to_path_buf()).unwrap();

        let entry = r#"{"name":"tts","source":"builtin","svc_type":"daemon","endpoint":"localhost:8080","status":"running"}"#;
        ns.write(&["tts"], entry).await.unwrap();

        let list = ns.read(&[]).await.unwrap();
        assert!(list.contains("tts"));

        let detail = ns.read(&["tts"]).await.unwrap();
        assert!(detail.contains("localhost:8080"));
    }

    #[tokio::test]
    async fn patch_service() {
        let dir = tempfile::tempdir().unwrap();
        let ns = ServicesNs::init(dir.path().to_path_buf()).unwrap();

        let entry = r#"{"name":"tts","source":"builtin","svc_type":"daemon","endpoint":"localhost:8080","status":"running"}"#;
        ns.write(&["tts"], entry).await.unwrap();

        ns.patch(&["tts"], r#"{"status":"stopped"}"#).await.unwrap();
        let detail = ns.read(&["tts"]).await.unwrap();
        assert!(detail.contains("stopped"));
    }

    #[tokio::test]
    async fn unknown_service_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let ns = ServicesNs::init(dir.path().to_path_buf()).unwrap();
        assert!(ns.read(&["nope"]).await.is_err());
    }
}
