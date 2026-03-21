use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
#[allow(deprecated)]
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
use bollard::exec::{CreateExecOptions, StartExecResults};
#[allow(deprecated)]
use bollard::image::CreateImageOptions;
use bollard::Docker;
use futures_util::StreamExt;
use logos_vfs::{Namespace, VfsError};
use tokio::sync::Mutex;

const CONTAINER_WORKDIR: &str = "/workspace";

pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Sandbox namespace — `logos://sandbox/`.
///
/// Each task gets a Docker container with `host_root/{task_id}/` bind-mounted
/// to `/workspace` inside the container.
///
/// - **read/write/patch**: operate on the host filesystem directly (via bind mount)
/// - **exec**: runs commands inside the container via `docker exec`
pub struct SandboxNs {
    docker: Docker,
    host_root: PathBuf,
    image: String,
    containers: Mutex<HashMap<String, String>>, // task_id → container_id
}

impl SandboxNs {
    /// Connect to Docker and prepare the sandbox root directory.
    pub async fn init(host_root: PathBuf, image: Option<String>) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&host_root)
            .map_err(|e| VfsError::Io(format!("create sandbox dir: {e}")))?;

        let docker = connect_docker().await?;

        // Verify connection
        docker
            .ping()
            .await
            .map_err(|e| VfsError::Io(format!("docker ping failed: {e}")))?;

        let image = image.unwrap_or_else(|| "ubuntu:24.04".to_string());

        // Pull image if not present
        ensure_image(&docker, &image).await?;

        Ok(Self {
            docker,
            host_root,
            image,
            containers: Mutex::new(HashMap::new()),
        })
    }

    /// Ensure a container exists and is running for the given task_id.
    async fn ensure_container(&self, task_id: &str) -> Result<String, VfsError> {
        {
            let containers = self.containers.lock().await;
            if let Some(id) = containers.get(task_id) {
                return Ok(id.clone());
            }
        }

        // Create host directory for this task
        let task_dir = self.host_root.join(task_id);
        tokio::fs::create_dir_all(&task_dir)
            .await
            .map_err(|e| VfsError::Io(format!("create task dir: {e}")))?;

        let container_name = format!("logos-sandbox-{task_id}");

        // Check if container already exists (e.g. from previous run)
        #[allow(deprecated)]
        let container_id = match self
            .docker
            .inspect_container(&container_name, None::<bollard::container::InspectContainerOptions>)
            .await
        {
            Ok(info) => {
                let id = info.id.unwrap_or_default();
                if let Some(state) = &info.state {
                    if state.running != Some(true) {
                        self.docker
                            .start_container(&id, None::<StartContainerOptions<String>>)
                            .await
                            .map_err(|e| VfsError::Io(format!("start container: {e}")))?;
                    }
                }
                id
            }
            Err(_) => {
                let host_path = task_dir
                    .canonicalize()
                    .unwrap_or(task_dir.clone())
                    .to_string_lossy()
                    .to_string();

                let config = Config {
                    image: Some(self.image.clone()),
                    cmd: Some(vec!["tail".to_string(), "-f".to_string(), "/dev/null".to_string()]),
                    working_dir: Some(CONTAINER_WORKDIR.to_string()),
                    host_config: Some(bollard::models::HostConfig {
                        binds: Some({
                            let mut binds = vec![format!("{host_path}:{CONTAINER_WORKDIR}")];
                            // RFC 002 §11.1: bind mount logos.sock so agents can connect
                            let sock_path = self.host_root.join("logos.sock");
                            if sock_path.exists() {
                                let sock_host = sock_path.canonicalize()
                                    .unwrap_or(sock_path)
                                    .to_string_lossy()
                                    .to_string();
                                binds.push(format!("{sock_host}:/logos.sock"));
                            }
                            binds
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                let resp = self
                    .docker
                    .create_container(
                        Some(CreateContainerOptions {
                            name: &container_name,
                            platform: None,
                        }),
                        config,
                    )
                    .await
                    .map_err(|e| VfsError::Io(format!("create container: {e}")))?;

                self.docker
                    .start_container(&resp.id, None::<StartContainerOptions<String>>)
                    .await
                    .map_err(|e| VfsError::Io(format!("start container: {e}")))?;

                resp.id
            }
        };

        let mut containers = self.containers.lock().await;
        containers.insert(task_id.to_string(), container_id.clone());
        Ok(container_id)
    }

    /// Translate `logos://services/{name}/...` URIs in a command.
    ///
    /// RFC 002 §3.1: oneshot services are invoked as CLI commands via filesystem paths.
    /// Daemon services are invoked via their registered endpoint (agent reads endpoint
    /// from logos://services/{name} and calls it directly — not through exec).
    fn translate_service_uris(&self, command: &str) -> String {
        let host_root_str = self.host_root.to_string_lossy();
        command.replace(
            "logos://services/",
            &format!("{host_root_str}/__system__/svc/"),
        )
    }

    /// Extract task_id from a command by looking for `logos://sandbox/{task_id}/`.
    fn extract_task_id(command: &str) -> Option<String> {
        let prefix = "logos://sandbox/";
        if let Some(start) = command.find(prefix) {
            let rest = &command[start + prefix.len()..];
            let task_id = rest.split('/').next().unwrap_or("");
            if !task_id.is_empty() {
                return Some(task_id.to_string());
            }
        }
        None
    }

    /// Execute a shell command inside the task's container.
    ///
    /// Translates `logos://sandbox/{task_id}/...` → `/workspace/...` in the command.
    pub async fn exec(&self, command: &str) -> Result<ExecResult, VfsError> {
        // Determine task_id from the command or use a default
        let task_id = Self::extract_task_id(command)
            .unwrap_or_else(|| "__default__".to_string());

        let container_id = self.ensure_container(&task_id).await?;

        // Translate URIs: logos://sandbox/{task_id}/... → /workspace/...
        let translated = command.replace(
            &format!("logos://sandbox/{task_id}/"),
            &format!("{CONTAINER_WORKDIR}/"),
        );
        // Also handle logos://sandbox/{task_id} without trailing slash
        let translated = translated.replace(
            &format!("logos://sandbox/{task_id}"),
            CONTAINER_WORKDIR,
        );

        // RFC 002 §3.1: translate logos://services/ URIs.
        // Daemon services → endpoint URL; oneshot → filesystem path.
        let translated = self.translate_service_uris(&translated);

        let exec_config = CreateExecOptions {
            cmd: Some(vec!["sh", "-c", &translated]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            working_dir: Some(CONTAINER_WORKDIR),
            ..Default::default()
        };

        let exec = self
            .docker
            .create_exec(&container_id, exec_config)
            .await
            .map_err(|e| VfsError::Io(format!("create exec: {e}")))?;

        let output = self
            .docker
            .start_exec(&exec.id, None)
            .await
            .map_err(|e| VfsError::Io(format!("start exec: {e}")))?;

        let mut stdout = String::new();
        let mut stderr = String::new();

        if let StartExecResults::Attached { mut output, .. } = output {
            while let Some(Ok(msg)) = output.next().await {
                match msg {
                    bollard::container::LogOutput::StdOut { message } => {
                        stdout.push_str(&String::from_utf8_lossy(&message));
                    }
                    bollard::container::LogOutput::StdErr { message } => {
                        stderr.push_str(&String::from_utf8_lossy(&message));
                    }
                    _ => {}
                }
            }
        }

        // Get exit code
        let inspect = self
            .docker
            .inspect_exec(&exec.id)
            .await
            .map_err(|e| VfsError::Io(format!("inspect exec: {e}")))?;
        let exit_code = inspect.exit_code.unwrap_or(-1) as i32;

        Ok(ExecResult {
            stdout,
            stderr,
            exit_code,
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
        let file_path = self.host_root.join(path.join("/"));

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
        let file_path = self.host_root.join(path.join("/"));

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
            let file_path = self.host_root.join(path.join("/"));
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

/// Connect to Docker daemon, trying multiple socket paths.
async fn connect_docker() -> Result<Docker, VfsError> {
    // 1. DOCKER_HOST env var
    if let Ok(host) = std::env::var("DOCKER_HOST") {
        if let Some(path) = host.strip_prefix("unix://") {
            if std::path::Path::new(path).exists() {
                return Docker::connect_with_unix(path, 120, bollard::API_DEFAULT_VERSION)
                    .map_err(|e| VfsError::Io(format!("docker connect {host}: {e}")));
            }
        }
    }

    // 2. Colima default
    let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
    let colima_sock = format!("{home}/.colima/default/docker.sock");
    if std::path::Path::new(&colima_sock).exists() {
        return Docker::connect_with_unix(&colima_sock, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| VfsError::Io(format!("docker connect colima: {e}")));
    }

    // 3. Standard path
    let std_sock = "/var/run/docker.sock";
    if std::path::Path::new(std_sock).exists() {
        return Docker::connect_with_unix(std_sock, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| VfsError::Io(format!("docker connect standard: {e}")));
    }

    Err(VfsError::Io(
        "no docker socket found (tried DOCKER_HOST, ~/.colima/default/docker.sock, /var/run/docker.sock)".to_string(),
    ))
}

/// Ensure the image is available locally, pull if not.
async fn ensure_image(docker: &Docker, image: &str) -> Result<(), VfsError> {
    // Check if image exists
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }

    println!("[logos] pulling image {image}...");
    let mut stream = docker.create_image(
        Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        }),
        None,
        None,
    );

    while let Some(result) = stream.next().await {
        result.map_err(|e| VfsError::Io(format!("pull image {image}: {e}")))?;
    }
    println!("[logos] image {image} ready");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test sandbox under $HOME (required for Colima bind mounts —
    /// Colima only exposes $HOME to the VM by default).
    async fn test_sandbox(name: &str) -> (SandboxNs, PathBuf) {
        let home = std::env::var("HOME").expect("HOME not set");
        let dir = PathBuf::from(home).join(".logos-test").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let sandbox = SandboxNs::init(dir.clone(), None).await.unwrap();
        (sandbox, dir)
    }

    async fn cleanup(sandbox: &SandboxNs, dir: &PathBuf) {
        let containers = sandbox.containers.lock().await;
        for (_, id) in containers.iter() {
            let _ = sandbox.docker.stop_container(id, None::<bollard::query_parameters::StopContainerOptions>).await;
            let _ = sandbox.docker.remove_container(id, None::<bollard::query_parameters::RemoveContainerOptions>).await;
        }
        drop(containers);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Integration test — requires Docker running.
    /// Run with: cargo test -p logos-kernel -- --ignored docker
    #[tokio::test]
    #[ignore]
    async fn docker_exec_basic() {
        let (sandbox, dir) = test_sandbox("exec-basic").await;

        let container_id = sandbox.ensure_container("test-exec").await.unwrap();
        assert!(!container_id.is_empty());

        let result = sandbox.exec("echo 'hello from sandbox'").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("hello from sandbox"));

        cleanup(&sandbox, &dir).await;
    }

    #[tokio::test]
    #[ignore]
    async fn docker_file_visibility() {
        let (sandbox, dir) = test_sandbox("file-vis").await;

        // Write file on host side
        sandbox.write(&["test-vis", "hello.txt"], "world").await.unwrap();

        // Read it back from host
        let content = sandbox.read(&["test-vis", "hello.txt"]).await.unwrap();
        assert_eq!(content, "world");

        // Exec cat inside container — should see the file via bind mount
        let result = sandbox
            .exec("cat logos://sandbox/test-vis/hello.txt")
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        assert_eq!(result.stdout.trim(), "world");

        cleanup(&sandbox, &dir).await;
    }

    #[tokio::test]
    #[ignore]
    async fn docker_uri_translation() {
        let (sandbox, dir) = test_sandbox("uri-trans").await;

        sandbox.write(&["test-uri", "data.txt"], "translated!").await.unwrap();

        let result = sandbox
            .exec("cat logos://sandbox/test-uri/data.txt")
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        assert_eq!(result.stdout.trim(), "translated!");

        cleanup(&sandbox, &dir).await;
    }
}
