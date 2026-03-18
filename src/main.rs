mod message_store;
mod service;
mod sessions_store;
mod users_store;

use std::path::PathBuf;
use std::sync::Arc;
use std::{io, net::SocketAddr};

use message_store::MessageStore;
use service::{EmbeddingConfig, MemoryVfsService};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

pub mod pb {
    tonic::include_proto!("kairos.vfs.v1");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_repo_env_file();
    let listen = resolve_listen_target();
    let users_root = resolve_users_root();
    let embedding = resolve_embedding_config();

    let state_root = users_root
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let memory_db_root = state_root.join("memory");
    let message_store = Arc::new(MessageStore::new(memory_db_root)?);

    let service = MemoryVfsService::new(users_root.clone(), embedding, message_store)?;
    let grpc_service = pb::memory_vfs_server::MemoryVfsServer::new(service);

    if let Some(socket_path) = parse_uds_path(&listen) {
        prepare_unix_socket(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)?;
        println!(
            "memory-vfs listening on unix://{}, users root: {}",
            socket_path.display(),
            users_root.display()
        );
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await?;
    } else {
        let addr: SocketAddr = listen.parse()?;
        println!(
            "memory-vfs listening on {addr}, users root: {}",
            users_root.display()
        );
        Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await?;
    }

    Ok(())
}

fn resolve_listen_target() -> String {
    std::env::var("VFS_LISTEN")
        .or_else(|_| std::env::var("KAIROS_VFS_SOCKET"))
        .unwrap_or_else(|_| "unix:///run/kairos-runtime/sockets/kairos-runtime-vfs.sock".to_string())
}

fn parse_uds_path(listen: &str) -> Option<PathBuf> {
    if let Some(path) = listen.strip_prefix("unix://") {
        if path.is_empty() {
            return None;
        }
        return Some(PathBuf::from(path));
    }
    if let Some(path) = listen.strip_prefix("unix:") {
        if path.is_empty() {
            return None;
        }
        return Some(PathBuf::from(path));
    }
    None
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

fn resolve_users_root() -> PathBuf {
    if let Ok(path) = std::env::var("VFS_USERS_ROOT") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/state/entities")
}

fn resolve_embedding_config() -> EmbeddingConfig {
    EmbeddingConfig {
        base_url: std::env::var("OLLAMA_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:11434".to_string()),
        model: std::env::var("OLLAMA_EMBED_MODEL")
            .unwrap_or_else(|_| "qwen3-embedding:0.6b".to_string()),
        dimension: std::env::var("OLLAMA_EMBED_DIM")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1024),
    }
}

fn load_repo_env_file() {
    let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(".env");
    let _ = dotenvy::from_path(env_path);
}
