use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use tokio::fs;
use tokio::sync::Mutex;

use crate::service::VfsError;

const MEM_SCHEME_PREFIX: &str = "mem://";
const ROOT_USERS: &str = "users";
const ROOT_CHATS: &str = "chats";

pub struct UsersStore {
    users_root: PathBuf,
    file_locks: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>,
}

#[derive(Debug, Clone)]
struct UsersFilePath {
    user_id: String,
    file_name: String,
}

impl UsersStore {
    pub fn new(users_root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&users_root)?;
        Ok(Self {
            users_root,
            file_locks: Mutex::new(HashMap::new()),
        })
    }

    pub fn users_root(&self) -> &Path {
        &self.users_root
    }

    pub async fn read(&self, raw_path: &str) -> Result<String, VfsError> {
        let path = parse_users_file_path(raw_path)?;
        let file_path = self.physical_user_file_path(&path);
        let content = fs::read_to_string(&file_path).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                VfsError::NotFound(format!("file not found: {}", file_path.display()))
            }
            _ => VfsError::Io(format!("failed to read {}: {e}", file_path.display())),
        })?;

        let value: Value = serde_json::from_str(&content).map_err(|e| {
            VfsError::InvalidJson(format!(
                "invalid json in {}: {e}",
                file_path.display()
            ))
        })?;
        serde_json::to_string(&value)
            .map_err(|e| VfsError::InvalidJson(format!("failed to serialize json: {e}")))
    }

    pub async fn write(&self, raw_path: &str, content: &str) -> Result<(), VfsError> {
        let path = parse_users_file_path(raw_path)?;
        let parsed = parse_json_object(content)?;
        let file_path = self.physical_user_file_path(&path);
        self.ensure_user_dir_initialized(&path.user_id).await?;

        let file_lock = self.get_or_create_file_lock(&file_path).await;
        let _guard = file_lock.lock().await;
        atomic_write_json(&file_path, &parsed).await?;
        Ok(())
    }

    pub async fn patch(&self, raw_path: &str, partial_content: &str) -> Result<(), VfsError> {
        let path = parse_users_file_path(raw_path)?;
        let patch = parse_json_object(partial_content)?;
        let file_path = self.physical_user_file_path(&path);
        self.ensure_user_dir_initialized(&path.user_id).await?;

        let file_lock = self.get_or_create_file_lock(&file_path).await;
        let _guard = file_lock.lock().await;

        let current_content = fs::read_to_string(&file_path).await.map_err(|e| {
            VfsError::Io(format!(
                "failed to read current json for patch {}: {e}",
                file_path.display()
            ))
        })?;
        let mut current = parse_json_object(&current_content)?;
        merge_json_object(&mut current, &patch);
        atomic_write_json(&file_path, &current).await?;
        Ok(())
    }

    fn physical_user_file_path(&self, path: &UsersFilePath) -> PathBuf {
        self.users_root.join(&path.user_id).join(&path.file_name)
    }

    async fn get_or_create_file_lock(&self, file_path: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.file_locks.lock().await;
        locks
            .entry(file_path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn ensure_user_dir_initialized(&self, user_id: &str) -> Result<(), VfsError> {
        let user_dir = self.users_root.join(user_id);
        fs::create_dir_all(&user_dir).await.map_err(|e| {
            VfsError::Io(format!(
                "failed to create user directory {}: {e}",
                user_dir.display()
            ))
        })?;

        for file_name in valid_user_file_names() {
            let file_path = user_dir.join(file_name);
            if fs::metadata(&file_path).await.is_err() {
                atomic_write_json(&file_path, &Value::Object(Map::new())).await?;
            }
        }
        Ok(())
    }
}

fn parse_users_file_path(raw_path: &str) -> Result<UsersFilePath, VfsError> {
    if !raw_path.starts_with(MEM_SCHEME_PREFIX) {
        return Err(VfsError::InvalidPath(format!(
            "path must start with {MEM_SCHEME_PREFIX}"
        )));
    }

    let path = &raw_path[MEM_SCHEME_PREFIX.len()..];
    let segments: Vec<&str> = path.split('/').collect();
    if segments.len() != 3 {
        return Err(VfsError::InvalidPath(
            "path must match mem://users/{user_id}/{file_name}".to_string(),
        ));
    }

    match segments[0] {
        ROOT_USERS => {}
        ROOT_CHATS => {
            return Err(VfsError::InvalidPath(
                "chats root is not supported by read/write/patch".to_string(),
            ))
        }
        _ => {
            return Err(VfsError::InvalidPath(
                "only users root is currently supported".to_string(),
            ))
        }
    }

    if segments[1].is_empty() {
        return Err(VfsError::InvalidPath("user_id cannot be empty".to_string()));
    }

    let file_name = segments[2];
    if !valid_user_file_names().contains(&file_name) {
        return Err(VfsError::InvalidPath(format!(
            "unsupported users file: {file_name}"
        )));
    }

    Ok(UsersFilePath {
        user_id: segments[1].to_string(),
        file_name: file_name.to_string(),
    })
}

fn parse_json_object(content: &str) -> Result<Value, VfsError> {
    let value: Value = serde_json::from_str(content)
        .map_err(|e| VfsError::InvalidJson(format!("invalid json content: {e}")))?;
    if !value.is_object() {
        return Err(VfsError::InvalidJson(
            "json content must be an object".to_string(),
        ));
    }
    Ok(value)
}

fn merge_json_object(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            for (key, patch_value) in patch_map {
                match target_map.get_mut(key) {
                    Some(existing_value) if existing_value.is_object() && patch_value.is_object() => {
                        merge_json_object(existing_value, patch_value);
                    }
                    _ => {
                        target_map.insert(key.clone(), patch_value.clone());
                    }
                }
            }
        }
        (target_value, patch_value) => {
            *target_value = patch_value.clone();
        }
    }
}

async fn atomic_write_json(target: &Path, value: &Value) -> Result<(), VfsError> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await.map_err(|e| {
            VfsError::Io(format!(
                "failed to create parent directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    let content = serde_json::to_string_pretty(value)
        .map_err(|e| VfsError::InvalidJson(format!("failed to serialize json: {e}")))?;

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| VfsError::Io(format!("system clock error: {e}")))?
        .as_nanos();
    let temp_path = target.with_extension(format!("tmp.{nonce}"));

    fs::write(&temp_path, content).await.map_err(|e| {
        VfsError::Io(format!(
            "failed to write temp file {}: {e}",
            temp_path.display()
        ))
    })?;

    fs::rename(&temp_path, target).await.map_err(|e| {
        VfsError::Io(format!(
            "failed to replace file {} with temp {}: {e}",
            target.display(),
            temp_path.display()
        ))
    })?;
    Ok(())
}

fn valid_user_file_names() -> [&'static str; 3] {
    ["preferences.json", "tech_projects.json", "relations.json"]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store(dir: &Path) -> UsersStore {
        UsersStore::new(dir.to_path_buf()).unwrap()
    }

    #[tokio::test]
    async fn read_write_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        store
            .write(
                "mem://users/alice/preferences.json",
                r#"{"theme": "dark"}"#,
            )
            .await
            .unwrap();

        let content = store.read("mem://users/alice/preferences.json").await.unwrap();
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(value["theme"], "dark");
    }

    #[tokio::test]
    async fn patch_merges_deeply() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        store
            .write(
                "mem://users/bob/preferences.json",
                r#"{"ui": {"font": "mono", "size": 14}, "lang": "en"}"#,
            )
            .await
            .unwrap();

        store
            .patch(
                "mem://users/bob/preferences.json",
                r#"{"ui": {"size": 16}, "color": "blue"}"#,
            )
            .await
            .unwrap();

        let content = store.read("mem://users/bob/preferences.json").await.unwrap();
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(value["ui"]["font"], "mono");
        assert_eq!(value["ui"]["size"], 16);
        assert_eq!(value["lang"], "en");
        assert_eq!(value["color"], "blue");
    }

    #[tokio::test]
    async fn invalid_path_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        assert!(store.read("invalid://path").await.is_err());
        assert!(store.read("mem://users/alice/unknown.json").await.is_err());
        assert!(store.read("mem://chats/123/preferences.json").await.is_err());
    }

    #[tokio::test]
    async fn read_nonexistent_initializes_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        store
            .write(
                "mem://users/carol/preferences.json",
                r#"{"x": 1}"#,
            )
            .await
            .unwrap();

        let content = store
            .read("mem://users/carol/tech_projects.json")
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(value.as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn concurrent_writes_dont_corrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(make_store(tmp.path()));

        let mut handles = Vec::new();
        for i in 0..10 {
            let s = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                s.write(
                    "mem://users/dave/preferences.json",
                    &format!(r#"{{"iter": {i}}}"#),
                )
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let content = store
            .read("mem://users/dave/preferences.json")
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(value["iter"].is_number());
    }
}
