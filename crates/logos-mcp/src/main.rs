//! Logos MCP Server Adapter — RFC 002 §11.4.
//!
//! A standalone process that bridges MCP (JSON-RPC over stdio) to the
//! Logos kernel (gRPC over Unix socket).
//!
//! Exposes 5 Logos primitives as MCP tools:
//! - logos_read, logos_write, logos_exec, logos_call, logos_complete
//!
//! Usage:
//!   LOGOS_TOKEN=<token> LOGOS_SOCKET=<path> logos-mcp

mod pb {
    tonic::include_proto!("logos.kernel.v1");
}

use pb::logos_client::LogosClient;
use pb::*;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

// --- JSON-RPC 2.0 types ---

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

// --- MCP tool definitions ---

fn tool_definitions() -> serde_json::Value {
    serde_json::json!({
        "tools": [
            {
                "name": "logos_read",
                "description": "Read data from a Logos URI",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "uri": { "type": "string", "description": "Logos URI (e.g. logos://memory/groups/chat-1/messages/1)" }
                    },
                    "required": ["uri"]
                }
            },
            {
                "name": "logos_write",
                "description": "Write data to a Logos URI",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "uri": { "type": "string", "description": "Logos URI" },
                        "content": { "type": "string", "description": "Content to write" }
                    },
                    "required": ["uri", "content"]
                }
            },
            {
                "name": "logos_exec",
                "description": "Execute a shell command in the sandbox container",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "command": { "type": "string", "description": "Shell command (logos:// URIs are auto-translated)" }
                    },
                    "required": ["command"]
                }
            },
            {
                "name": "logos_call",
                "description": "Call a proc tool by name",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tool": { "type": "string", "description": "Tool name (e.g. memory.search)" },
                        "params": { "type": "object", "description": "Tool parameters" }
                    },
                    "required": ["tool", "params"]
                }
            },
            {
                "name": "logos_complete",
                "description": "End the current turn (mandatory final call)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "summary": { "type": "string", "description": "What happened this turn" },
                        "reply": { "type": "string", "description": "Message to deliver to the user" },
                        "anchor": { "type": "boolean", "description": "Create a task completion anchor?" },
                        "anchor_facts": { "type": "string", "description": "JSON array of fact objects for the anchor" }
                    },
                    "required": ["summary"]
                }
            }
        ]
    })
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = std::env::var("LOGOS_SOCKET")
        .unwrap_or_else(|_| "/tmp/logos-sandbox/logos.sock".to_string());
    let token = std::env::var("LOGOS_TOKEN").unwrap_or_default();

    // Connect to kernel gRPC via UDS
    let channel = connect_uds(&socket_path).await?;
    let mut client = LogosClient::new(channel);

    // Handshake with token
    let session_key = if !token.is_empty() {
        let resp = client
            .handshake(HandshakeReq {
                token: token.clone(),
            })
            .await?
            .into_inner();
        if !resp.ok {
            eprintln!("handshake failed: {}", resp.error);
            std::process::exit(1);
        }
        // Extract session key from response metadata (would need interceptor)
        // For now, we'll pass token as session header
        Some(token)
    } else {
        None
    };

    eprintln!("[logos-mcp] connected to {socket_path}");

    // stdio JSON-RPC loop
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: serde_json::Value::Null,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32700,
                        message: format!("parse error: {e}"),
                    }),
                };
                let out = serde_json::to_string(&resp)? + "\n";
                stdout.write_all(out.as_bytes()).await?;
                stdout.flush().await?;
                continue;
            }
        };

        let response = handle_request(&mut client, &session_key, request).await;
        let out = serde_json::to_string(&response)? + "\n";
        stdout.write_all(out.as_bytes()).await?;
        stdout.flush().await?;
    }

    Ok(())
}

async fn handle_request(
    client: &mut LogosClient<Channel>,
    _session_key: &Option<String>,
    req: JsonRpcRequest,
) -> JsonRpcResponse {
    let id = req.id.clone();

    match req.method.as_str() {
        // MCP protocol: list tools
        "tools/list" => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(tool_definitions()),
            error: None,
        },

        // MCP protocol: call tool
        "tools/call" => {
            let tool_name = req.params["name"].as_str().unwrap_or("");
            let arguments = &req.params["arguments"];
            match call_tool(client, tool_name, arguments).await {
                Ok(result) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: Some(serde_json::json!({
                        "content": [{ "type": "text", "text": result }]
                    })),
                    error: None,
                },
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32000,
                        message: e,
                    }),
                },
            }
        }

        // MCP protocol: initialize
        "initialize" => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "logos-mcp", "version": "0.1.0" }
            })),
            error: None,
        },

        // MCP protocol: initialized notification (no response needed, but we respond anyway)
        "notifications/initialized" => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: Some(serde_json::json!({})),
            error: None,
        },

        _ => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: format!("method not found: {}", req.method),
            }),
        },
    }
}

async fn call_tool(
    client: &mut LogosClient<Channel>,
    tool_name: &str,
    args: &serde_json::Value,
) -> Result<String, String> {
    match tool_name {
        "logos_read" => {
            let uri = args["uri"].as_str().unwrap_or("").to_string();
            let resp = client
                .read(ReadReq { uri })
                .await
                .map_err(|e| e.message().to_string())?;
            Ok(resp.into_inner().content)
        }
        "logos_write" => {
            let uri = args["uri"].as_str().unwrap_or("").to_string();
            let content = args["content"].as_str().unwrap_or("").to_string();
            client
                .write(WriteReq { uri, content })
                .await
                .map_err(|e| e.message().to_string())?;
            Ok("ok".to_string())
        }
        "logos_exec" => {
            let command = args["command"].as_str().unwrap_or("").to_string();
            let resp = client
                .exec(ExecReq { command })
                .await
                .map_err(|e| e.message().to_string())?;
            let inner = resp.into_inner();
            Ok(serde_json::json!({
                "stdout": inner.stdout,
                "stderr": inner.stderr,
                "exit_code": inner.exit_code,
            })
            .to_string())
        }
        "logos_call" => {
            let tool = args["tool"].as_str().unwrap_or("").to_string();
            let params = serde_json::to_string(&args["params"]).unwrap_or_else(|_| "{}".to_string());
            let resp = client
                .call(CallReq {
                    tool,
                    params_json: params,
                })
                .await
                .map_err(|e| e.message().to_string())?;
            Ok(resp.into_inner().result_json)
        }
        "logos_complete" => {
            let summary = args["summary"].as_str().unwrap_or("").to_string();
            let reply = args["reply"].as_str().unwrap_or("").to_string();
            let anchor = args["anchor"].as_bool().unwrap_or(false);
            let anchor_facts = args["anchor_facts"].as_str().unwrap_or("").to_string();
            let resp = client
                .complete(CompleteReq {
                    summary,
                    reply,
                    anchor,
                    anchor_facts,
                    task_log: String::new(),
                    sleep_reason: String::new(),
                    sleep_retry: false,
                    resume_task_id: String::new(),
                })
                .await
                .map_err(|e| e.message().to_string())?;
            let inner = resp.into_inner();
            Ok(serde_json::json!({
                "reply": inner.reply,
                "anchor_id": inner.anchor_id,
            })
            .to_string())
        }
        _ => Err(format!("unknown tool: {tool_name}")),
    }
}

/// Connect to a Unix Domain Socket gRPC endpoint.
async fn connect_uds(path: &str) -> Result<Channel, Box<dyn std::error::Error>> {
    let path = path.to_string();
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await?;
    Ok(channel)
}
