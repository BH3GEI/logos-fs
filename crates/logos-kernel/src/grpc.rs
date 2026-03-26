use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::pb::logos_server::Logos;
use crate::pb::*;
use crate::sandbox::SandboxNs;
use crate::token::TokenRegistry;
use logos_vfs::{Namespace, RoutingTable, VfsError};

const SESSION_KEY_HEADER: &str = "x-logos-session";

pub struct LogosService {
    table: Arc<RoutingTable>,
    system: Arc<logos_system::SystemModule>,
    #[allow(dead_code)]
    mm: Arc<logos_mm::MemoryModule>,
    sandbox: Arc<SandboxNs>,
    proc_ns: Arc<crate::proc::ProcNs>,
    tokens: Arc<TokenRegistry>,
}

impl LogosService {
    pub fn new(
        table: Arc<RoutingTable>,
        system: Arc<logos_system::SystemModule>,
        mm: Arc<logos_mm::MemoryModule>,
        sandbox: Arc<SandboxNs>,
        proc_ns: Arc<crate::proc::ProcNs>,
        tokens: Arc<TokenRegistry>,
    ) -> Self {
        Self {
            table,
            system,
            mm,
            sandbox,
            proc_ns,
            tokens,
        }
    }
}

use crate::token::{AgentRole, SessionInfo};

async fn extract_session_info(
    tokens: &TokenRegistry,
    request: &Request<impl std::any::Any>,
) -> Option<SessionInfo> {
    let session_key = request.metadata().get(SESSION_KEY_HEADER)?.to_str().ok()?;
    tokens.resolve_info(session_key).await
}

/// RFC 002 §12.3: enforce sandbox access + workspace exclusivity + namespace permissions.
fn check_access(info: Option<&SessionInfo>, uri: &str, is_write: bool) -> Result<(), Status> {
    // Sandbox isolation: task can only access its own sandbox
    if let Some(si) = info {
        if let Some(rest) = uri.strip_prefix("logos://sandbox/") {
            let uri_task = rest.split('/').next().unwrap_or("");
            if !uri_task.is_empty() && uri_task != "__system__" && uri_task != si.task_id {
                return Err(Status::permission_denied(format!(
                    "task {} cannot access sandbox of {uri_task}",
                    si.task_id
                )));
            }
        }
    }

    // Namespace permission enforcement (RFC 002 §12.3)
    if is_write {
        if let Some(si) = info {
            if !si.role.is_admin() {
                // User agents: services, system, devices are read-only
                if uri.starts_with("logos://services/")
                    || uri.starts_with("logos://system/")
                    || uri.starts_with("logos://devices/")
                {
                    return Err(Status::permission_denied(format!(
                        "user agent cannot write to {}",
                        uri.split('/').take(4).collect::<Vec<_>>().join("/")
                    )));
                }
            }
        }
        // No session header = management interface = admin (allowed)
    }

    Ok(())
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
        let info = extract_session_info(&self.tokens, &request).await;
        let uri = request.into_inner().uri;
        check_access(info.as_ref(), &uri, false)?;
        let content = self.table.read(&uri).await.map_err(vfs_to_status)?;
        Ok(Response::new(ReadRes { content }))
    }

    async fn write(&self, request: Request<WriteReq>) -> Result<Response<WriteRes>, Status> {
        let info = extract_session_info(&self.tokens, &request).await;
        let req = request.into_inner();
        check_access(info.as_ref(), &req.uri, true)?;
        self.table
            .write(&req.uri, &req.content)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(WriteRes {}))
    }

    async fn patch(&self, request: Request<PatchReq>) -> Result<Response<PatchRes>, Status> {
        let info = extract_session_info(&self.tokens, &request).await;
        let req = request.into_inner();
        check_access(info.as_ref(), &req.uri, true)?;
        self.table
            .patch(&req.uri, &req.partial)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(PatchRes {}))
    }

    async fn exec(&self, request: Request<ExecReq>) -> Result<Response<ExecRes>, Status> {
        let info = extract_session_info(&self.tokens, &request).await;
        let agent_id = info.map(|i| i.agent_id).unwrap_or_else(|| "__default__".to_string());
        let command = request.into_inner().command;
        let result = self.sandbox.exec(&command, &agent_id).await.map_err(vfs_to_status)?;
        Ok(Response::new(ExecRes {
            stdout: result.stdout,
            stderr: result.stderr,
            exit_code: result.exit_code,
        }))
    }

    async fn call(&self, request: Request<CallReq>) -> Result<Response<CallRes>, Status> {
        let req = request.into_inner();
        let result_json = self.proc_ns.call(&req.tool, &req.params_json)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(CallRes { result_json }))
    }

    async fn complete(
        &self,
        request: Request<CompleteReq>,
    ) -> Result<Response<CompleteRes>, Status> {
        let info = extract_session_info(&self.tokens, &request).await;
        let task_id = info.map(|i| i.task_id).unwrap_or_default();
        let req = request.into_inner();
        let task_log = req.task_log.clone();
        let tid = task_id.clone();
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

        // RFC 002 §9.1 step 3: write task_log to logos://sandbox/{task_id}/log
        if !task_log.is_empty() && !tid.is_empty() {
            self.sandbox
                .patch(&[&tid, "log"], &task_log)
                .await
                .map_err(vfs_to_status)?;
        }

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
                res.metadata_mut()
                    .insert(SESSION_KEY_HEADER, session_key.parse().unwrap());
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
        let role = AgentRole::from_str(&req.role);
        self.tokens.register(req.token, req.task_id, req.agent_id, role).await;
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
