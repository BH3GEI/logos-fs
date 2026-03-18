use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};

use crate::message_store::MessageStore;
use crate::pb::{
    memory_vfs_server::MemoryVfs, ArchiveRequest, ArchiveResponse, PatchRequest, PatchResponse,
    RangeFetchRequest, RangeFetchResponse, ReadRequest, ReadResponse, SearchMode, SearchRequest,
    SearchResponse, WriteRequest, WriteResponse,
};
use crate::sessions_store::SessionsStore;
use crate::users_store::UsersStore;

pub use crate::sessions_store::EmbeddingConfig;

pub struct MemoryVfsService {
    users: UsersStore,
    sessions: SessionsStore,
    messages: Arc<MessageStore>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VfsError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid json: {0}")]
    InvalidJson(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("lancedb error: {0}")]
    Lance(String),
    #[error("sqlite error: {0}")]
    Sqlite(String),
}

impl From<VfsError> for Status {
    fn from(e: VfsError) -> Status {
        match &e {
            VfsError::InvalidPath(_) | VfsError::InvalidRequest(_) | VfsError::InvalidJson(_) => {
                Status::invalid_argument(e.to_string())
            }
            VfsError::NotFound(_) => Status::not_found(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl MemoryVfsService {
    pub fn new(
        users_root: PathBuf,
        embedding: EmbeddingConfig,
        messages: Arc<MessageStore>,
    ) -> std::io::Result<Self> {
        let users = UsersStore::new(users_root)?;
        let state_root = users
            .users_root()
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let lancedb_dir = state_root.join("lancedb");
        std::fs::create_dir_all(&lancedb_dir)?;

        Ok(Self {
            users,
            sessions: SessionsStore::new(
                lancedb_dir.to_string_lossy().to_string(),
                embedding,
                Arc::clone(&messages),
            ),
            messages,
        })
    }
}

fn log_ok(op: &str, detail: &str, started_at: Instant) {
    println!(
        "[vfs] op={} status=ok elapsed_ms={} {}",
        op,
        started_at.elapsed().as_millis(),
        detail
    );
}

fn log_err(op: &str, detail: &str, err: &VfsError, started_at: Instant) {
    eprintln!(
        "[vfs] op={} status=error elapsed_ms={} {} err=\"{}\"",
        op,
        started_at.elapsed().as_millis(),
        detail,
        err
    );
}

fn resolve_search_mode(raw: i32) -> SearchMode {
    SearchMode::try_from(raw).unwrap_or(SearchMode::Unspecified)
}

#[tonic::async_trait]
impl MemoryVfs for MemoryVfsService {
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("path={}", req.path);
        match self.users.read(&req.path).await {
            Ok(content) => {
                log_ok("read", &detail, started_at);
                Ok(Response::new(ReadResponse { content }))
            }
            Err(err) => {
                log_err("read", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("path={} content_len={}", req.path, req.content.len());
        match self.users.write(&req.path, &req.content).await {
            Ok(_) => {
                log_ok("write", &detail, started_at);
                Ok(Response::new(WriteResponse {}))
            }
            Err(err) => {
                log_err("write", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn patch(
        &self,
        request: Request<PatchRequest>,
    ) -> Result<Response<PatchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "path={} partial_content_len={}",
            req.path,
            req.partial_content.len()
        );
        match self.users.patch(&req.path, &req.partial_content).await {
            Ok(_) => {
                log_ok("patch", &detail, started_at);
                Ok(Response::new(PatchResponse {}))
            }
            Err(err) => {
                log_err("patch", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "scope={} limit={} query_len={} mode={:?}",
            req.scope,
            req.limit,
            req.query.len(),
            resolve_search_mode(req.mode),
        );

        match resolve_search_mode(req.mode) {
            SearchMode::Fts => {
                match self
                    .messages
                    .search_fts(&req.scope, &req.query, req.limit)
                    .await
                {
                    Ok(stored_messages) => {
                        log_ok(
                            "search",
                            &format!("{} result_count={}", detail, stored_messages.len()),
                            started_at,
                        );
                        Ok(Response::new(SearchResponse {
                            results: vec![crate::pb::SearchResult {
                                session_id: String::new(),
                                center_vector: Vec::new(),
                                abstract_summary: String::new(),
                                messages: stored_messages
                                    .into_iter()
                                    .map(|m| crate::pb::ChatMessage {
                                        user_id: m.speaker.clone(),
                                        message_id: m.external_id,
                                        chat_id: m.chat_id,
                                        conversation_type: String::new(),
                                        context: m.text,
                                        timestamp: m.msg_id,
                                        metadata: None,
                                        vector: Vec::new(),
                                    })
                                    .collect(),
                                score: 0.0,
                            }],
                        }))
                    }
                    Err(err) => {
                        log_err("search", &detail, &err, started_at);
                        Err(err.into())
                    }
                }
            }
            _ => match self.sessions.search(req).await {
                Ok(results) => {
                    log_ok(
                        "search",
                        &format!("{} result_count={}", detail, results.len()),
                        started_at,
                    );
                    Ok(Response::new(SearchResponse { results }))
                }
                Err(err) => {
                    log_err("search", &detail, &err, started_at);
                    Err(err.into())
                }
            },
        }
    }

    async fn archive(
        &self,
        request: Request<ArchiveRequest>,
    ) -> Result<Response<ArchiveResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "session_id={} chat_id={} messages_count={}",
            req.session_id,
            req.chat_id,
            req.messages.len()
        );
        match self.sessions.archive(req).await {
            Ok(_) => {
                log_ok("archive", &detail, started_at);
                Ok(Response::new(ArchiveResponse {}))
            }
            Err(err) => {
                log_err("archive", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn range_fetch(
        &self,
        request: Request<RangeFetchRequest>,
    ) -> Result<Response<RangeFetchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "chat_id={} ranges_count={} limit={} offset={}",
            req.chat_id,
            req.ranges.len(),
            req.limit,
            req.offset,
        );

        let ranges: Vec<(i64, i64)> = req.ranges.iter().map(|r| (r.start, r.end)).collect();
        match self
            .messages
            .range_fetch(&req.chat_id, &ranges, req.limit, req.offset)
            .await
        {
            Ok(messages) => {
                log_ok(
                    "range_fetch",
                    &format!("{} result_count={}", detail, messages.len()),
                    started_at,
                );
                Ok(Response::new(RangeFetchResponse {
                    messages: messages
                        .into_iter()
                        .map(|m| crate::pb::StoredMessage {
                            msg_id: m.msg_id,
                            external_id: m.external_id,
                            ts: m.ts,
                            chat_id: m.chat_id,
                            speaker: m.speaker,
                            reply_to: m.reply_to,
                            text: m.text,
                            mentions: m.mentions,
                            session_id: m.session_id,
                        })
                        .collect(),
                }))
            }
            Err(err) => {
                log_err("range_fetch", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }
}
