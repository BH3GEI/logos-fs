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
                "name": "logos_patch",
                "description": "Partially update data at a Logos URI (JSON deep merge)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "uri": { "type": "string", "description": "Logos URI" },
                        "partial": { "type": "string", "description": "Partial content to merge" }
                    },
                    "required": ["uri", "partial"]
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
            }
        ]
    })
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = std::env::var("LOGOS_SOCKET")
        // Default must match kernel's VFS_LISTEN default (sandbox_root + /logos.sock)
        .unwrap_or_else(|_| "../../data/state/sandbox/logos.sock".to_string());
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
            .await?;
        // Extract session key from response metadata before consuming
        let key = resp.metadata()
            .get("x-logos-session")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let inner = resp.into_inner();
        if !inner.ok {
            eprintln!("handshake failed: {}", inner.error);
            std::process::exit(1);
        }
        key
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
    session_key: &Option<String>,
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
            match call_tool(client, session_key, tool_name, arguments).await {
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

/// Inject x-logos-session header into a tonic request.
fn with_session<T>(req: T, session_key: &Option<String>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(req);
    if let Some(key) = session_key {
        if let Ok(val) = key.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>() {
            request.metadata_mut().insert("x-logos-session", val);
        }
    }
    request
}

async fn call_tool(
    client: &mut LogosClient<Channel>,
    session_key: &Option<String>,
    tool_name: &str,
    args: &serde_json::Value,
) -> Result<String, String> {
    match tool_name {
        "logos_read" => {
            let uri = args["uri"].as_str().unwrap_or("").to_string();
            let resp = client
                .read(with_session(ReadReq { uri }, session_key))
                .await
                .map_err(|e| e.message().to_string())?;
            Ok(resp.into_inner().content)
        }
        "logos_write" => {
            let uri = args["uri"].as_str().unwrap_or("").to_string();
            let content = args["content"].as_str().unwrap_or("").to_string();
            client
                .write(with_session(WriteReq { uri, content }, session_key))
                .await
                .map_err(|e| e.message().to_string())?;
            Ok("ok".to_string())
        }
        "logos_patch" => {
            let uri = args["uri"].as_str().unwrap_or("").to_string();
            let partial = args["partial"].as_str().unwrap_or("").to_string();
            client
                .patch(with_session(PatchReq { uri, partial }, session_key))
                .await
                .map_err(|e| e.message().to_string())?;
            Ok("ok".to_string())
        }
        "logos_exec" => {
            let command = args["command"].as_str().unwrap_or("").to_string();
            let resp = client
                .exec(with_session(ExecReq { command }, session_key))
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
                .call(with_session(CallReq {
                    tool,
                    params_json: params,
                }, session_key))
                .await
                .map_err(|e| e.message().to_string())?;
            Ok(resp.into_inner().result_json)
        }
        _ => Err(format!("unknown tool: {tool_name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definitions_has_five_tools() {
        let defs = tool_definitions();
        let tools = defs["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 5);
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"logos_read"));
        assert!(names.contains(&"logos_write"));
        assert!(names.contains(&"logos_patch"));
        assert!(names.contains(&"logos_exec"));
        assert!(names.contains(&"logos_call"));
    }

    #[test]
    fn tool_schemas_have_required_fields() {
        let defs = tool_definitions();
        let tools = defs["tools"].as_array().unwrap();
        for tool in tools {
            assert!(tool["name"].is_string());
            assert!(tool["description"].is_string());
            assert!(tool["inputSchema"].is_object());
            assert!(tool["inputSchema"]["required"].is_array());
        }
    }

    #[test]
    fn parse_valid_jsonrpc_request() {
        let input = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
        let req: JsonRpcRequest = serde_json::from_str(input).unwrap();
        assert_eq!(req.method, "tools/list");
        assert_eq!(req.id, serde_json::json!(1));
    }

    #[test]
    fn parse_invalid_jsonrpc_fails() {
        let input = "not json{{{";
        let result: Result<JsonRpcRequest, _> = serde_json::from_str(input);
        assert!(result.is_err());
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
