mod grpc;
mod sandbox;
mod tmp;
mod token;
mod users;

use std::path::PathBuf;
use std::sync::Arc;
use std::{io, net::SocketAddr};

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use logos_vfs::RoutingTable;

pub mod pb {
    tonic::include_proto!("logos.kernel.v1");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_env();

    // --- Boot sequence ---
    println!("[logos] booting...");

    let mut table = RoutingTable::new();

    // 1. users/
    let users_root = env_path("VFS_USERS_ROOT", "../../data/state/entities");
    let users_ns = users::UsersNs::init(users_root)?;
    table.mount(Box::new(users_ns));
    println!("[logos] mounted logos://users/");

    // 2. memory/
    let memory_root = env_path("VFS_MEMORY_ROOT", "../../data/state/memory");
    let memory_ns = logos_mm::MemoryModule::init(memory_root)?;
    let mm_arc = Arc::new(memory_ns);
    table.mount(Box::new(MemoryNsRef(Arc::clone(&mm_arc))));
    println!("[logos] mounted logos://memory/");

    // 3. system/
    let system_db = env_path("VFS_SYSTEM_DB", "../../data/state/system.db");
    let system_ns = logos_system::SystemModule::init(system_db)?;
    let system_arc = Arc::new(system_ns);
    // Mount system as Namespace via a wrapper that shares the Arc
    table.mount(Box::new(SystemNsRef(Arc::clone(&system_arc))));
    println!("[logos] mounted logos://system/");

    // 4. sandbox/
    let sandbox_root = env_path("VFS_SANDBOX_ROOT", "../../data/state/sandbox");
    let sandbox_ns = sandbox::SandboxNs::init(sandbox_root)?;
    let sandbox_arc = Arc::new(sandbox_ns);
    table.mount(Box::new(SandboxNsRef(Arc::clone(&sandbox_arc))));
    println!("[logos] mounted logos://sandbox/");

    // 5. tmp/
    table.mount(Box::new(tmp::TmpNs::new()));
    println!("[logos] mounted logos://tmp/");

    // --- Open ---
    table.open();
    println!(
        "[logos] kernel ready — {} namespace(s) mounted",
        table.mounted().len()
    );

    // --- Serve ---
    let table = Arc::new(table);
    let tokens = token::TokenRegistry::new();
    let service = grpc::LogosService::new(
        Arc::clone(&table),
        system_arc,
        mm_arc,
        sandbox_arc,
        tokens,
    );
    let grpc_service = pb::logos_server::LogosServer::new(service);

    let listen = env_str("VFS_LISTEN", "unix:///run/logos/logos.sock");
    if let Some(socket_path) = parse_uds_path(&listen) {
        prepare_unix_socket(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)?;
        println!("[logos] listening on unix://{}", socket_path.display());
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await?;
    } else {
        let addr: SocketAddr = listen.parse()?;
        println!("[logos] listening on {addr}");
        Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await?;
    }

    Ok(())
}

// --- SystemModule Namespace wrapper ---
// SystemModule implements Namespace, but we need to mount it as Box<dyn Namespace>
// while also keeping an Arc for the Complete handler. This wrapper delegates.
struct MemoryNsRef(Arc<logos_mm::MemoryModule>);

#[async_trait::async_trait]
impl logos_vfs::Namespace for MemoryNsRef {
    fn name(&self) -> &str { self.0.name() }
    async fn read(&self, path: &[&str]) -> Result<String, logos_vfs::VfsError> { self.0.read(path).await }
    async fn write(&self, path: &[&str], content: &str) -> Result<(), logos_vfs::VfsError> { self.0.write(path, content).await }
    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), logos_vfs::VfsError> { self.0.patch(path, partial).await }
}

struct SandboxNsRef(Arc<sandbox::SandboxNs>);

#[async_trait::async_trait]
impl logos_vfs::Namespace for SandboxNsRef {
    fn name(&self) -> &str { self.0.name() }
    async fn read(&self, path: &[&str]) -> Result<String, logos_vfs::VfsError> { self.0.read(path).await }
    async fn write(&self, path: &[&str], content: &str) -> Result<(), logos_vfs::VfsError> { self.0.write(path, content).await }
    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), logos_vfs::VfsError> { self.0.patch(path, partial).await }
}

struct SystemNsRef(Arc<logos_system::SystemModule>);

#[async_trait::async_trait]
impl logos_vfs::Namespace for SystemNsRef {
    fn name(&self) -> &str {
        self.0.name()
    }
    async fn read(&self, path: &[&str]) -> Result<String, logos_vfs::VfsError> {
        self.0.read(path).await
    }
    async fn write(&self, path: &[&str], content: &str) -> Result<(), logos_vfs::VfsError> {
        self.0.write(path, content).await
    }
    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), logos_vfs::VfsError> {
        self.0.patch(path, partial).await
    }
}

// --- Helpers ---

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_path(key: &str, default: &str) -> PathBuf {
    if let Ok(val) = std::env::var(key) {
        return PathBuf::from(val);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(default)
}

fn parse_uds_path(listen: &str) -> Option<PathBuf> {
    let path = listen
        .strip_prefix("unix://")
        .or_else(|| listen.strip_prefix("unix:"))?;
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(path))
}

fn prepare_unix_socket(socket_path: &PathBuf) -> io::Result<()> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(socket_path) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

fn load_env() {
    let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".env");
    let _ = dotenvy::from_path(env_path);
}
