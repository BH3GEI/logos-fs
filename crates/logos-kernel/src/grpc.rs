use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::pb::logos_server::Logos;
use crate::pb::*;
use crate::token::TokenRegistry;
use logos_vfs::{RoutingTable, VfsError};

/// The metadata key used to pass the session key after Handshake.
/// Client sends this in every request after a successful handshake.
const SESSION_KEY_HEADER: &str = "x-logos-session";

pub struct LogosService {
    table: Arc<RoutingTable>,
    system: Arc<logos_system::SystemModule>,
    tokens: Arc<TokenRegistry>,
}

impl LogosService {
    pub fn new(
        table: Arc<RoutingTable>,
        system: Arc<logos_system::SystemModule>,
        tokens: Arc<TokenRegistry>,
    ) -> Self {
        Self {
            table,
            system,
            tokens,
        }
    }
}

/// Extract task_id from request metadata via session key.
async fn extract_task_id(tokens: &TokenRegistry, request: &Request<impl std::any::Any>) -> Option<String> {
    let session_key = request
        .metadata()
        .get(SESSION_KEY_HEADER)?
        .to_str()
        .ok()?;
    tokens.resolve(session_key).await
}

fn vfs_to_status(e: VfsError) -> Status {
    match &e {
        VfsError::InvalidUri(_) | VfsError::InvalidPath(_) | VfsError::InvalidJson(_) => {
            Status::invalid_argument(e.to_string())
        }
        VfsError::NotFound(_) | VfsError::NamespaceNotMounted(_) => {
            Status::not_found(e.to_string())
        }
        VfsError::NotReady => Status::unavailable(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[tonic::async_trait]
impl Logos for LogosService {
    async fn read(&self, request: Request<ReadReq>) -> Result<Response<ReadRes>, Status> {
        let uri = request.into_inner().uri;
        let content = self.table.read(&uri).await.map_err(vfs_to_status)?;
        Ok(Response::new(ReadRes { content }))
    }

    async fn write(&self, request: Request<WriteReq>) -> Result<Response<WriteRes>, Status> {
        let req = request.into_inner();
        self.table
            .write(&req.uri, &req.content)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(WriteRes {}))
    }

    async fn patch(&self, request: Request<PatchReq>) -> Result<Response<PatchRes>, Status> {
        let req = request.into_inner();
        self.table
            .patch(&req.uri, &req.partial)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(PatchRes {}))
    }

    async fn exec(&self, _request: Request<ExecReq>) -> Result<Response<ExecRes>, Status> {
        Err(Status::unimplemented("sandbox not mounted"))
    }

    async fn call(&self, _request: Request<CallReq>) -> Result<Response<CallRes>, Status> {
        Err(Status::unimplemented("proc not mounted"))
    }

    async fn complete(
        &self,
        request: Request<CompleteReq>,
    ) -> Result<Response<CompleteRes>, Status> {
        let task_id = extract_task_id(&self.tokens, &request)
            .await
            .unwrap_or_default();
        let req = request.into_inner();
        let params = logos_system::complete::CompleteParams {
            task_id,
            summary: req.summary,
            reply: req.reply,
            anchor: req.anchor,
            anchor_facts: req.anchor_facts,
            task_log: req.task_log,
            sleep_reason: req.sleep_reason,
            sleep_retry: req.sleep_retry,
            resume_task_id: req.resume_task_id,
        };
        let result = self.system.complete(params).await.map_err(vfs_to_status)?;
        Ok(Response::new(CompleteRes {
            reply: result.reply,
            anchor_id: result.anchor_id,
        }))
    }

    // --- Kernel management interface ---

    async fn handshake(
        &self,
        request: Request<HandshakeReq>,
    ) -> Result<Response<HandshakeRes>, Status> {
        let token = request.into_inner().token;
        match self.tokens.consume(&token).await {
            Some((session_key, _task_id)) => {
                let mut res = Response::new(HandshakeRes {
                    ok: true,
                    error: String::new(),
                });
                // Return session key in response metadata — client must send it back
                // in subsequent requests as x-logos-session header.
                res.metadata_mut().insert(
                    SESSION_KEY_HEADER,
                    session_key.parse().unwrap(),
                );
                Ok(res)
            }
            None => Ok(Response::new(HandshakeRes {
                ok: false,
                error: "invalid or already consumed token".to_string(),
            })),
        }
    }

    async fn register_token(
        &self,
        request: Request<RegisterTokenReq>,
    ) -> Result<Response<RegisterTokenRes>, Status> {
        let req = request.into_inner();
        if req.token.is_empty() || req.task_id.is_empty() {
            return Err(Status::invalid_argument("token and task_id required"));
        }
        self.tokens.register(req.token, req.task_id).await;
        Ok(Response::new(RegisterTokenRes {}))
    }

    async fn revoke_token(
        &self,
        request: Request<RevokeTokenReq>,
    ) -> Result<Response<RevokeTokenRes>, Status> {
        let token = request.into_inner().token;
        self.tokens.revoke(&token).await;
        Ok(Response::new(RevokeTokenRes {}))
    }
}
