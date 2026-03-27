use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};
use tokio::sync::Mutex;

const CONTAINER_WORKDIR: &str = "/workspace";

pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// Sandbox executor trait — pluggable container backend.
/// Result of ensure_container: container ID + host path for filesystem operations.
#[derive(Clone)]
pub struct ContainerInfo {
    pub container_id: String,
    /// Host path where files can be read/written directly.
    /// For containerd: overlayfs upperdir. For Docker: bind-mount host dir.
    pub host_path: PathBuf,
}

#[async_trait]
pub trait SandboxExecutor: Send + Sync {
    /// Ensure a container is running for the given agent_config_id.
    async fn ensure_container(&self, agent_config_id: &str, sock_path: Option<&PathBuf>) -> Result<ContainerInfo, VfsError>;

    /// Execute a command inside the container.
    async fn exec_in_container(&self, container_id: &str, command: &str) -> Result<ExecResult, VfsError>;

    /// Stop and remove a container.
    async fn remove_container(&self, container_id: &str) -> Result<(), VfsError>;
}

/// Sandbox namespace — `logos://sandbox/`.
///
/// RFC 002 §5: sandbox paths are mapped via overlayfs. The container's rootfs
/// IS the workspace. Files written to the overlayfs upperdir are visible inside
/// the container. Multiple tasks from the same agent share one container.
///
/// - **read/write/patch**: operate on the host filesystem directly (via bind mount)
/// - **exec**: runs commands inside the container
pub struct SandboxNs {
    executor: Box<dyn SandboxExecutor>,
    host_root: PathBuf,
    containers: Mutex<HashMap<String, ContainerInfo>>, // agent_config_id → info
}

impl SandboxNs {
    pub async fn init(host_root: PathBuf, image: Option<String>) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&host_root)
            .map_err(|e| VfsError::Io(format!("create sandbox dir: {e}")))?;

        // Try containerd first, fall back to Docker
        let executor: Box<dyn SandboxExecutor> = match ContainerdExecutor::try_init(image.clone()).await {
            Ok(e) => {
                println!("[logos] sandbox: using containerd");
                Box::new(e)
            }
            Err(e) => {
                println!("[logos] sandbox: containerd unavailable ({e}), trying docker...");
                let docker = DockerExecutor::try_init(image).await?;
                println!("[logos] sandbox: using docker");
                Box::new(docker)
            }
        };

        Ok(Self {
            executor,
            host_root,
            containers: Mutex::new(HashMap::new()),
        })
    }

    /// Ensure a container exists and is running for the given agent_config_id.
    /// Returns (container_id, host_path for filesystem ops).
    async fn ensure_container(&self, agent_config_id: &str) -> Result<ContainerInfo, VfsError> {
        {
            let containers = self.containers.lock().await;
            if let Some(info) = containers.get(agent_config_id) {
                return Ok(info.clone());
            }
        }

        let sock_path = self.host_root.join("logos.sock");
        let sock = if sock_path.exists() { Some(&sock_path) } else { None };

        let info = self.executor.ensure_container(agent_config_id, sock).await?;

        let mut containers = self.containers.lock().await;
        containers.insert(agent_config_id.to_string(), info.clone());
        Ok(info)
    }

    fn translate_service_uris(&self, command: &str) -> String {
        let host_root_str = self.host_root.to_string_lossy();
        command.replace(
            "logos://services/",
            &format!("{host_root_str}/__system__/svc/"),
        )
    }

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

    pub async fn exec(&self, command: &str, agent_config_id: &str) -> Result<ExecResult, VfsError> {
        let info = self.ensure_container(agent_config_id).await?;

        let task_id = Self::extract_task_id(command)
            .unwrap_or_else(|| agent_config_id.to_string());

        let translated = command.replace(
            &format!("logos://sandbox/{task_id}/"),
            &format!("{CONTAINER_WORKDIR}/"),
        );
        let translated = translated.replace(
            &format!("logos://sandbox/{task_id}"),
            CONTAINER_WORKDIR,
        );
        let translated = self.translate_service_uris(&translated);

        self.executor.exec_in_container(&info.container_id, &translated).await
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

// =============================================================================
// Containerd executor (default)
// =============================================================================

use containerd_client as ctrd;
use ctrd::services::v1::{
    containers_client::ContainersClient,
    tasks_client::TasksClient,
    container::Runtime,
    Container, CreateContainerRequest, CreateTaskRequest,
    StartRequest, ExecProcessRequest, WaitRequest,
};
use ctrd::with_namespace;
// with_namespace! macro requires tonic::Request in scope — use containerd-client's re-export
use ctrd::tonic::Request;
use ctrd::tonic::transport::Channel as CtrdChannel;
use prost_types::Any;

const CONTAINERD_NS: &str = "logos";

struct ContainerdExecutor {
    channel: CtrdChannel,
    image: String,
}

impl ContainerdExecutor {
    async fn try_init(image: Option<String>) -> Result<Self, VfsError> {
        // Try multiple socket paths
        let paths = [
            std::env::var("CONTAINERD_SOCKET").unwrap_or_default(),
            format!("{}/.colima/default/containerd.sock",
                std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())),
            "/run/containerd/containerd.sock".to_string(),
        ];

        let mut last_err = String::new();
        for path in &paths {
            if path.is_empty() || !std::path::Path::new(path).exists() {
                continue;
            }
            match ctrd::connect(path).await {
                Ok(channel) => {
                    // Verify connection with version check
                    let mut vc = ctrd::services::v1::version_client::VersionClient::new(channel.clone());
                    match vc.version(()).await {
                        Ok(_) => {
                            let image = image.unwrap_or_else(|| "docker.io/library/alpine:latest".to_string());
                            return Ok(Self { channel, image });
                        }
                        Err(e) => last_err = format!("version check failed on {path}: {e}"),
                    }
                }
                Err(e) => last_err = format!("connect {path}: {e}"),
            }
        }
        Err(VfsError::Io(format!("no containerd socket found: {last_err}")))
    }

    /// Generate OCI runtime spec.
    ///
    /// RFC 002 §5: sandbox paths are mapped via overlayfs, not bind mounts.
    /// The container's rootfs IS the workspace — files written to the overlayfs
    /// upperdir are visible inside the container at /workspace.
    fn make_spec(sock_path: Option<&PathBuf>) -> String {
        let mut mounts = vec![
            r#"{"destination":"/proc","type":"proc","source":"proc"}"#.to_string(),
            r#"{"destination":"/dev","type":"tmpfs","source":"tmpfs","options":["nosuid","strictatime","mode=755","size=65536k"]}"#.to_string(),
        ];

        if let Some(sock) = sock_path {
            let sock_str = sock.canonicalize().unwrap_or(sock.clone()).to_string_lossy().to_string();
            mounts.push(format!(
                r#"{{"destination":"/logos.sock","type":"bind","source":"{}","options":["rbind","rw"]}}"#,
                sock_str
            ));
        }

        let mounts_json = mounts.join(",");

        format!(r#"{{
            "ociVersion": "1.0.2",
            "process": {{
                "terminal": false,
                "user": {{"uid": 0, "gid": 0}},
                "args": ["sleep", "infinity"],
                "env": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
                "cwd": "/workspace"
            }},
            "root": {{"path": "rootfs", "readonly": false}},
            "mounts": [{mounts_json}],
            "linux": {{
                "namespaces": [
                    {{"type": "pid"}},
                    {{"type": "ipc"}},
                    {{"type": "uts"}},
                    {{"type": "mount"}}
                ]
            }}
        }}"#)
    }

    /// Pull and unpack image if not already present.
    async fn ensure_image(&self) -> Result<(), VfsError> {
        let mut images = ctrd::services::v1::images_client::ImagesClient::new(self.channel.clone());
        let req = ctrd::services::v1::GetImageRequest { name: self.image.clone() };
        let req = with_namespace!(req, CONTAINERD_NS);

        // Check if image exists and is already unpacked
        if let Ok(resp) = images.get(req).await {
            let image = resp.into_inner().image.unwrap_or_default();
            // Check if snapshot exists for this image (means it's unpacked)
            let mut snapshots = ctrd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(self.channel.clone());
            let identity = self.get_image_identity(&image).await;
            if let Some(ref id) = identity {
                let req = ctrd::services::v1::snapshots::StatSnapshotRequest {
                    snapshotter: "overlayfs".to_string(),
                    key: id.clone(),
                };
                let req = with_namespace!(req, CONTAINERD_NS);
                if snapshots.stat(req).await.is_ok() {
                    return Ok(()); // already pulled and unpacked
                }
            }
        }

        println!("[logos] pulling image {} via containerd...", self.image);

        let mut transfer = ctrd::services::v1::transfer_client::TransferClient::new(self.channel.clone());

        let source = ctrd::types::transfer::OciRegistry {
            reference: self.image.clone(),
            ..Default::default()
        };
        let dest = ctrd::types::transfer::ImageStore {
            name: self.image.clone(),
            unpacks: vec![ctrd::types::transfer::UnpackConfiguration {
                platform: Some(ctrd::types::Platform {
                    os: "linux".to_string(),
                    architecture: std::env::consts::ARCH.replace("aarch64", "arm64").replace("x86_64", "amd64"),
                    ..Default::default()
                }),
                snapshotter: "overlayfs".to_string(),
            }],
            ..Default::default()
        };

        let req = ctrd::services::v1::TransferRequest {
            source: Some(ctrd::to_any(&source)),
            destination: Some(ctrd::to_any(&dest)),
            options: None,
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        transfer.transfer(req).await
            .map_err(|e| VfsError::Io(format!("pull image {}: {e}", self.image)))?;

        println!("[logos] image {} ready", self.image);
        Ok(())
    }

    /// Get the identity (chain ID) of an image's top layer for use as snapshot parent.
    async fn get_image_identity(&self, image: &ctrd::services::v1::Image) -> Option<String> {
        // The image's target digest is the manifest descriptor
        let target = image.target.as_ref()?;
        let digest = &target.digest;
        // containerd uses the image digest to create an identity snapshot
        // The convention is: the image name itself is used as the snapshot key after unpack
        Some(self.image.clone())
    }

    /// Find the snapshot parent key for the unpacked image.
    ///
    /// After image unpack, containerd creates committed snapshots for each layer.
    /// The top layer's chain ID is the snapshot key we need as parent.
    /// For multi-arch images, we find the snapshot matching current platform by
    /// looking for the most recently created committed snapshot with a sha256: prefix.
    async fn find_image_snapshot_parent(&self) -> Result<String, VfsError> {
        let mut snapshots = ctrd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(self.channel.clone());

        let req = ctrd::services::v1::snapshots::ListSnapshotsRequest {
            snapshotter: "overlayfs".to_string(),
            filters: vec![],
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let resp = snapshots.list(req).await
            .map_err(|e| VfsError::Io(format!("list snapshots: {e}")))?;

        use futures_util::StreamExt;
        let mut stream = resp.into_inner();
        let mut best: Option<(String, Option<prost_types::Timestamp>)> = None;

        while let Some(Ok(resp)) = stream.next().await {
            for info in resp.info {
                // Committed snapshots (kind == 3) with sha256: prefix are image layers
                if info.kind == 3 && info.name.starts_with("sha256:") {
                    let is_newer = match (&best, &info.created_at) {
                        (None, _) => true,
                        (Some((_, Some(prev))), Some(curr)) => {
                            (curr.seconds, curr.nanos) > (prev.seconds, prev.nanos)
                        }
                        (Some((_, None)), Some(_)) => true,
                        _ => false,
                    };
                    if is_newer {
                        best = Some((info.name, info.created_at));
                    }
                }
            }
        }

        best.map(|(name, _)| name).ok_or_else(|| VfsError::Io(format!(
            "no committed snapshot found for image {}. Is the image unpacked?", self.image
        )))
    }

    /// Get the overlayfs upperdir/workspace for an existing snapshot.
    async fn get_upperdir(&self, snapshot_key: &str) -> Result<PathBuf, VfsError> {
        let mut snapshots = ctrd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(self.channel.clone());
        let req = ctrd::services::v1::snapshots::MountsRequest {
            snapshotter: "overlayfs".to_string(),
            key: snapshot_key.to_string(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let resp = snapshots.mounts(req).await
            .map_err(|e| VfsError::Io(format!("get snapshot mounts: {e}")))?;

        for mount in resp.into_inner().mounts {
            for opt in &mount.options {
                if let Some(upper) = opt.strip_prefix("upperdir=") {
                    return Ok(PathBuf::from(upper).join("workspace"));
                }
            }
        }
        Err(VfsError::Io("no upperdir found in snapshot".to_string()))
    }
}

#[async_trait]
impl SandboxExecutor for ContainerdExecutor {
    async fn ensure_container(&self, agent_config_id: &str, sock_path: Option<&PathBuf>) -> Result<ContainerInfo, VfsError> {
        let container_id = format!("logos-sandbox-{agent_config_id}");

        // Check if container already exists
        let mut containers = ContainersClient::new(self.channel.clone());
        let req = ctrd::services::v1::GetContainerRequest { id: container_id.clone() };
        let req = with_namespace!(req, CONTAINERD_NS);
        if containers.get(req).await.is_ok() {
            let mut tasks = TasksClient::new(self.channel.clone());
            let req = ctrd::services::v1::GetRequest { container_id: container_id.clone(), exec_id: String::new() };
            let req = with_namespace!(req, CONTAINERD_NS);
            if tasks.get(req).await.is_ok() {
                // Find upperdir from existing snapshot
                let snapshot_key = format!("logos-snap-{agent_config_id}");
                let host_path = self.get_upperdir(&snapshot_key).await
                    .unwrap_or_else(|_| PathBuf::from("/tmp"));
                return Ok(ContainerInfo { container_id, host_path });
            }
            let req = ctrd::services::v1::DeleteContainerRequest { id: container_id.clone() };
            let req = with_namespace!(req, CONTAINERD_NS);
            let _ = containers.delete(req).await;
        }

        // Pull and unpack image if needed
        self.ensure_image().await?;

        // Find the image's top committed snapshot (from unpack)
        let parent_key = self.find_image_snapshot_parent().await?;

        // Prepare active snapshot for this agent (layered on top of image)
        let snapshot_key = format!("logos-snap-{agent_config_id}");
        let mut snapshots = ctrd::services::v1::snapshots::snapshots_client::SnapshotsClient::new(self.channel.clone());

        // Remove old snapshot if exists
        let req = ctrd::services::v1::snapshots::RemoveSnapshotRequest {
            snapshotter: "overlayfs".to_string(),
            key: snapshot_key.clone(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let _ = snapshots.remove(req).await;

        // Prepare new snapshot with image as parent
        let req = ctrd::services::v1::snapshots::PrepareSnapshotRequest {
            snapshotter: "overlayfs".to_string(),
            key: snapshot_key.clone(),
            parent: parent_key,
            ..Default::default()
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let mounts_resp = snapshots.prepare(req).await
            .map_err(|e| VfsError::Io(format!("prepare snapshot: {e}")))?;
        let mounts = mounts_resp.into_inner().mounts;

        // Find the overlayfs upperdir — this is where host-side file ops go
        let mut upperdir = PathBuf::new();
        for mount in &mounts {
            for opt in &mount.options {
                if let Some(upper) = opt.strip_prefix("upperdir=") {
                    upperdir = PathBuf::from(upper);
                }
            }
        }
        // Pre-create /workspace in upperdir so runc can use it as cwd
        let workspace_dir = upperdir.join("workspace");
        std::fs::create_dir_all(&workspace_dir)
            .map_err(|e| VfsError::Io(format!("create workspace in upperdir: {e}")))?;

        // Create OCI spec (no bind mounts — overlayfs IS the filesystem)
        let spec_json = Self::make_spec(sock_path);
        let spec = Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: spec_json.into_bytes(),
        };

        // Create container
        let container = Container {
            id: container_id.clone(),
            image: self.image.clone(),
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
            spec: Some(spec),
            snapshotter: "overlayfs".to_string(),
            snapshot_key: snapshot_key.clone(),
            ..Default::default()
        };

        let req = CreateContainerRequest { container: Some(container) };
        let req = with_namespace!(req, CONTAINERD_NS);
        containers.create(req).await
            .map_err(|e| VfsError::Io(format!("create container: {e}")))?;

        // Create task (the running process) with FIFOs
        let tmp_dir = std::env::temp_dir().join(format!("logos-{agent_config_id}"));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir)
            .map_err(|e| VfsError::Io(format!("create tmp dir: {e}")))?;

        let stdin_path = tmp_dir.join("stdin");
        let stdout_path = tmp_dir.join("stdout");
        let stderr_path = tmp_dir.join("stderr");
        create_fifo(&stdin_path)?;
        create_fifo(&stdout_path)?;
        create_fifo(&stderr_path)?;

        // Spawn FIFO readers BEFORE creating task (prevents FIFO deadlock)
        let so = stdout_path.clone();
        let se = stderr_path.clone();
        let si = stdin_path.clone();
        let _stdout_reader = tokio::spawn(async move { let _ = tokio::fs::read_to_string(so).await; });
        let _stderr_reader = tokio::spawn(async move { let _ = tokio::fs::read_to_string(se).await; });
        let _stdin_writer = tokio::spawn(async move {
            let _ = tokio::fs::OpenOptions::new().write(true).open(si).await;
        });

        let mut tasks = TasksClient::new(self.channel.clone());
        let req = CreateTaskRequest {
            container_id: container_id.clone(),
            rootfs: mounts,
            stdin: stdin_path.to_string_lossy().to_string(),
            stdout: stdout_path.to_string_lossy().to_string(),
            stderr: stderr_path.to_string_lossy().to_string(),
            ..Default::default()
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        tasks.create(req).await
            .map_err(|e| VfsError::Io(format!("create task: {e}")))?;

        // Start task
        let req = StartRequest {
            container_id: container_id.clone(),
            ..Default::default()
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        tasks.start(req).await
            .map_err(|e| VfsError::Io(format!("start task: {e}")))?;

        Ok(ContainerInfo {
            container_id,
            host_path: workspace_dir,
        })
    }

    async fn exec_in_container(&self, container_id: &str, command: &str) -> Result<ExecResult, VfsError> {
        let exec_id = format!("exec-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));

        let tmp_dir = std::env::temp_dir().join(format!("logos-exec-{exec_id}"));
        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir)
            .map_err(|e| VfsError::Io(format!("create exec tmp: {e}")))?;

        let stdin_path = tmp_dir.join("stdin");
        let stdout_path = tmp_dir.join("stdout");
        let stderr_path = tmp_dir.join("stderr");
        create_fifo(&stdin_path)?;
        create_fifo(&stdout_path)?;
        create_fifo(&stderr_path)?;

        // Spawn FIFO readers BEFORE exec (prevents FIFO deadlock)
        let so = stdout_path.clone();
        let se = stderr_path.clone();
        let si = stdin_path.clone();
        let stdout_reader = tokio::spawn(async move {
            tokio::fs::read_to_string(so).await.unwrap_or_default()
        });
        let stderr_reader = tokio::spawn(async move {
            tokio::fs::read_to_string(se).await.unwrap_or_default()
        });
        let _stdin_writer = tokio::spawn(async move {
            let _ = tokio::fs::OpenOptions::new().write(true).open(si).await;
        });

        // Build exec process spec
        // runc exec checks cwd before bind mounts are visible, so use / as cwd
        // and prepend cd /workspace to the command
        let full_cmd = format!("cd /workspace && {command}");
        let process_spec = serde_json::json!({
            "terminal": false,
            "user": {"uid": 0, "gid": 0},
            "args": ["sh", "-c", full_cmd],
            "env": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],
            "cwd": "/"
        });
        let spec = Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Process".to_string(),
            value: serde_json::to_vec(&process_spec).unwrap_or_default(),
        };

        let mut tasks = TasksClient::new(self.channel.clone());

        // Exec
        let req = ExecProcessRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
            stdin: stdin_path.to_string_lossy().to_string(),
            stdout: stdout_path.to_string_lossy().to_string(),
            stderr: stderr_path.to_string_lossy().to_string(),
            terminal: false,
            spec: Some(spec),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        tasks.exec(req).await
            .map_err(|e| VfsError::Io(format!("exec: {e}")))?;

        // Start the exec process
        let req = StartRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        tasks.start(req).await
            .map_err(|e| VfsError::Io(format!("start exec: {e}")))?;

        // Wait for completion
        let req = WaitRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let wait_resp = tasks.wait(req).await
            .map_err(|e| VfsError::Io(format!("wait exec: {e}")))?;
        let exit_code = wait_resp.into_inner().exit_status as i32;

        // Read stdout/stderr from FIFO readers
        let stdout = stdout_reader.await.unwrap_or_default();
        let stderr = stderr_reader.await.unwrap_or_default();

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);

        Ok(ExecResult {
            stdout,
            stderr,
            exit_code,
        })
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), VfsError> {
        let mut tasks = TasksClient::new(self.channel.clone());

        // Kill and delete task
        let req = ctrd::services::v1::KillRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
            signal: 9, // SIGKILL
            ..Default::default()
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let _ = tasks.kill(req).await;

        // Wait for exit
        let req = WaitRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let _ = tasks.wait(req).await;

        // Delete task
        let req = ctrd::services::v1::DeleteTaskRequest {
            container_id: container_id.to_string(),
        };
        let req = with_namespace!(req, CONTAINERD_NS);
        let _ = tasks.delete(req).await;

        // Delete container
        let mut containers = ContainersClient::new(self.channel.clone());
        let req = ctrd::services::v1::DeleteContainerRequest { id: container_id.to_string() };
        let req = with_namespace!(req, CONTAINERD_NS);
        let _ = containers.delete(req).await;

        Ok(())
    }
}

// =============================================================================
// Docker executor (fallback)
// =============================================================================

use bollard::Docker;
use bollard::exec::{CreateExecOptions, StartExecResults};
#[allow(deprecated)]
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
#[allow(deprecated)]
use bollard::image::CreateImageOptions;
use futures_util::StreamExt;

struct DockerExecutor {
    docker: Docker,
    image: String,
}

impl DockerExecutor {
    async fn try_init(image: Option<String>) -> Result<Self, VfsError> {
        let docker = connect_docker().await?;
        docker.ping().await
            .map_err(|e| VfsError::Io(format!("docker ping: {e}")))?;

        let image = image.unwrap_or_else(|| "ubuntu:24.04".to_string());
        ensure_docker_image(&docker, &image).await?;

        Ok(Self { docker, image })
    }
}

#[async_trait]
impl SandboxExecutor for DockerExecutor {
    async fn ensure_container(&self, agent_config_id: &str, sock_path: Option<&PathBuf>) -> Result<ContainerInfo, VfsError> {
        let container_name = format!("logos-sandbox-{agent_config_id}");
        // Docker uses bind-mount, so host_dir is under host_root
        let host_dir = PathBuf::from(
            std::env::var("VFS_SANDBOX_ROOT").unwrap_or_else(|_| "/tmp/logos-sandbox".to_string())
        ).join(agent_config_id);
        std::fs::create_dir_all(&host_dir)
            .map_err(|e| VfsError::Io(format!("create docker agent dir: {e}")))?;

        #[allow(deprecated)]
        let container_id = match self.docker
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
                let host_path = host_dir.canonicalize()
                    .unwrap_or(host_dir.clone())
                    .to_string_lossy()
                    .to_string();

                let mut binds = vec![format!("{host_path}:{CONTAINER_WORKDIR}")];
                if let Some(sock) = sock_path {
                    let sock_host = sock.canonicalize()
                        .unwrap_or(sock.clone())
                        .to_string_lossy()
                        .to_string();
                    binds.push(format!("{sock_host}:/logos.sock"));
                }

                let config = Config {
                    image: Some(self.image.clone()),
                    cmd: Some(vec!["tail".to_string(), "-f".to_string(), "/dev/null".to_string()]),
                    working_dir: Some(CONTAINER_WORKDIR.to_string()),
                    host_config: Some(bollard::models::HostConfig {
                        binds: Some(binds),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                let resp = self.docker
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

        Ok(ContainerInfo { container_id, host_path: host_dir })
    }

    async fn exec_in_container(&self, container_id: &str, command: &str) -> Result<ExecResult, VfsError> {
        let exec_config = CreateExecOptions {
            cmd: Some(vec!["sh", "-c", command]),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            working_dir: Some(CONTAINER_WORKDIR),
            ..Default::default()
        };

        let exec = self.docker.create_exec(container_id, exec_config).await
            .map_err(|e| VfsError::Io(format!("create exec: {e}")))?;

        let output = self.docker.start_exec(&exec.id, None).await
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

        let inspect = self.docker.inspect_exec(&exec.id).await
            .map_err(|e| VfsError::Io(format!("inspect exec: {e}")))?;
        let exit_code = inspect.exit_code.unwrap_or(-1) as i32;

        Ok(ExecResult { stdout, stderr, exit_code })
    }

    async fn remove_container(&self, container_id: &str) -> Result<(), VfsError> {
        let _ = self.docker.stop_container(container_id, None::<bollard::query_parameters::StopContainerOptions>).await;
        let _ = self.docker.remove_container(container_id, None::<bollard::query_parameters::RemoveContainerOptions>).await;
        Ok(())
    }
}

/// Create a FIFO (named pipe) at the given path. Required by containerd for stdio.
#[cfg(unix)]
fn create_fifo(path: &std::path::Path) -> Result<(), VfsError> {
    let _ = std::fs::remove_file(path);
    let c_path = std::ffi::CString::new(path.to_string_lossy().as_bytes())
        .map_err(|e| VfsError::Io(format!("invalid fifo path: {e}")))?;
    let ret = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
    if ret != 0 {
        return Err(VfsError::Io(format!(
            "mkfifo {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn create_fifo(path: &std::path::Path) -> Result<(), VfsError> {
    // FIFOs not supported on non-Unix; create regular file as fallback (Docker mode)
    std::fs::write(path, "").map_err(|e| VfsError::Io(format!("create file {}: {e}", path.display())))
}

async fn connect_docker() -> Result<Docker, VfsError> {
    if let Ok(host) = std::env::var("DOCKER_HOST") {
        if let Some(path) = host.strip_prefix("unix://") {
            if std::path::Path::new(path).exists() {
                return Docker::connect_with_unix(path, 120, bollard::API_DEFAULT_VERSION)
                    .map_err(|e| VfsError::Io(format!("docker connect {host}: {e}")));
            }
        }
    }

    let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
    let colima_sock = format!("{home}/.colima/default/docker.sock");
    if std::path::Path::new(&colima_sock).exists() {
        return Docker::connect_with_unix(&colima_sock, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| VfsError::Io(format!("docker connect colima: {e}")));
    }

    let std_sock = "/var/run/docker.sock";
    if std::path::Path::new(std_sock).exists() {
        return Docker::connect_with_unix(std_sock, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|e| VfsError::Io(format!("docker connect standard: {e}")));
    }

    Err(VfsError::Io("no docker socket found".to_string()))
}

async fn ensure_docker_image(docker: &Docker, image: &str) -> Result<(), VfsError> {
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }

    println!("[logos] pulling image {image}...");
    let mut stream = docker.create_image(
        Some(CreateImageOptions { from_image: image, ..Default::default() }),
        None, None,
    );
    while let Some(result) = stream.next().await {
        result.map_err(|e| VfsError::Io(format!("pull image {image}: {e}")))?;
    }
    println!("[logos] image {image} ready");
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_sandbox(name: &str) -> (SandboxNs, PathBuf) {
        let home = std::env::var("HOME").expect("HOME not set");
        let dir = PathBuf::from(home).join(".logos-test").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let sandbox = SandboxNs::init(dir.clone(), None).await.unwrap();
        (sandbox, dir)
    }

    async fn cleanup(sandbox: &SandboxNs, dir: &PathBuf) {
        let containers = sandbox.containers.lock().await;
        for (_, info) in containers.iter() {
            let _ = sandbox.executor.remove_container(&info.container_id).await;
        }
        drop(containers);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    #[ignore]
    async fn exec_basic() {
        let (sandbox, dir) = test_sandbox("exec-basic").await;

        let info = sandbox.ensure_container("test-exec").await.unwrap();
        assert!(!info.container_id.is_empty());

        let result = sandbox.exec("echo 'hello from sandbox'", "test-exec").await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout.contains("hello from sandbox"));

        cleanup(&sandbox, &dir).await;
    }

    #[tokio::test]
    #[ignore]
    async fn file_visibility() {
        let (sandbox, dir) = test_sandbox("file-vis").await;

        // Create container first to get the overlayfs host path
        let info = sandbox.ensure_container("test-vis").await.unwrap();

        // Write file to the overlayfs upperdir (visible inside container as /workspace/hello.txt)
        let file_path = info.host_path.join("hello.txt");
        std::fs::create_dir_all(info.host_path.parent().unwrap_or(&info.host_path)).unwrap();
        std::fs::write(&file_path, "world").unwrap();

        // Exec inside container should see the file
        let result = sandbox
            .exec("cat /workspace/hello.txt", "test-vis")
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        assert_eq!(result.stdout.trim(), "world");

        cleanup(&sandbox, &dir).await;
    }

    #[tokio::test]
    #[ignore]
    async fn uri_translation() {
        let (sandbox, dir) = test_sandbox("uri-trans").await;

        // Create container first
        let info = sandbox.ensure_container("test-uri").await.unwrap();

        // Write to overlayfs upperdir
        std::fs::write(info.host_path.join("data.txt"), "translated!").unwrap();

        // URI translation: logos://sandbox/test-uri/data.txt → /workspace/data.txt
        let result = sandbox
            .exec("cat logos://sandbox/test-uri/data.txt", "test-uri")
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        assert_eq!(result.stdout.trim(), "translated!");

        cleanup(&sandbox, &dir).await;
    }
}
