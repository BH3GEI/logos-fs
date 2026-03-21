use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};
use tokio::sync::Mutex;

/// A proc tool — stateless, callable via `logos_call`.
///
/// Built-in tools implement this trait directly in Rust.
/// External tools delegate to a git project process in the sandbox.
#[async_trait]
pub trait ProcTool: Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> serde_json::Value;
    async fn call(&self, params: &str) -> Result<String, VfsError>;
}

/// Per-call session slot (RFC 002 §3.2 call_id model).
struct CallSlot {
    tool_name: String,
    input: String,
    output: Option<String>,
    error: Option<String>,
    #[allow(dead_code)]
    created_at: Instant,
}

/// The Proc namespace — `logos://proc/`.
///
/// Provides tool discovery, call dispatch, and per-call session state.
///
/// URI routing:
///   logos://proc/                                → read: tool name list
///   logos://proc/{tool_name}                     → read: tool schema
///   logos://proc/{tool_name}/.schema             → read: tool schema
///   logos://proc/{tool_name}/{call_id}/input     → write: submit params and trigger execution
///   logos://proc/{tool_name}/{call_id}/output    → read: call result
///   logos://proc/{tool_name}/{call_id}/error     → read: call error (if any)
pub struct ProcNs {
    tools: HashMap<String, Arc<dyn ProcTool>>,
    call_slots: Mutex<HashMap<String, CallSlot>>,
}

impl ProcNs {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
            call_slots: Mutex::new(HashMap::new()),
        }
    }

    /// Register a tool. Overwrites if name already exists.
    pub fn register(&mut self, tool: Arc<dyn ProcTool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Dispatch a `logos_call` to the named tool (synchronous sugar).
    ///
    /// Internally generates a call_id, writes input, executes, returns output.
    pub async fn call(&self, tool_name: &str, params: &str) -> Result<String, VfsError> {
        let tool = self.tools.get(tool_name).ok_or_else(|| {
            VfsError::NotFound(format!("unknown proc tool: {tool_name}"))
        })?;
        tool.call(params).await
    }

    /// Execute a call via the call_id session model.
    async fn execute_call(&self, tool_name: &str, call_id: &str, input: &str) -> Result<(), VfsError> {
        let tool = self.tools.get(tool_name).ok_or_else(|| {
            VfsError::NotFound(format!("unknown proc tool: {tool_name}"))
        })?;

        let result = tool.call(input).await;

        let mut slots = self.call_slots.lock().await;
        let slot = slots.entry(format!("{tool_name}/{call_id}")).or_insert(CallSlot {
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            output: None,
            error: None,
            created_at: Instant::now(),
        });
        slot.input = input.to_string();

        match result {
            Ok(output) => {
                slot.output = Some(output);
                slot.error = None;
            }
            Err(e) => {
                slot.output = None;
                slot.error = Some(e.to_string());
            }
        }

        // Lazy cleanup: remove slots older than 5 minutes
        let cutoff = Instant::now() - std::time::Duration::from_secs(300);
        slots.retain(|_, s| s.created_at > cutoff);

        Ok(())
    }
}

#[async_trait]
impl Namespace for ProcNs {
    fn name(&self) -> &str {
        "proc"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        match path.len() {
            // logos://proc/ → list all tools
            0 => {
                let names: Vec<&str> = self.tools.keys().map(|s| s.as_str()).collect();
                Ok(serde_json::to_string(&names).unwrap_or_else(|_| "[]".to_string()))
            }
            // logos://proc/{tool_name} → schema
            1 => {
                let tool = self.tools.get(path[0]).ok_or_else(|| {
                    VfsError::NotFound(format!("unknown tool: {}", path[0]))
                })?;
                Ok(tool.schema().to_string())
            }
            // logos://proc/{tool_name}/.schema → schema
            2 if path[1] == ".schema" => {
                let tool = self.tools.get(path[0]).ok_or_else(|| {
                    VfsError::NotFound(format!("unknown tool: {}", path[0]))
                })?;
                Ok(tool.schema().to_string())
            }
            // logos://proc/{tool_name}/{call_id}/output → read result, then cleanup
            3 if path[2] == "output" => {
                let key = format!("{}/{}", path[0], path[1]);
                let mut slots = self.call_slots.lock().await;
                let slot = slots.get(&key).ok_or_else(|| {
                    VfsError::NotFound(format!("call not found: {key}"))
                })?;
                let result = match &slot.output {
                    Some(o) => Ok(o.clone()),
                    None => Err(VfsError::NotFound(format!("call {key}: no output yet"))),
                };
                // RFC: cleanup slot after output is read
                if result.is_ok() {
                    slots.remove(&key);
                }
                result
            }
            // logos://proc/{tool_name}/{call_id}/error → read error
            3 if path[2] == "error" => {
                let key = format!("{}/{}", path[0], path[1]);
                let slots = self.call_slots.lock().await;
                let slot = slots.get(&key).ok_or_else(|| {
                    VfsError::NotFound(format!("call not found: {key}"))
                })?;
                match &slot.error {
                    Some(e) => Ok(e.clone()),
                    None => Ok("null".to_string()),
                }
            }
            // logos://proc/{tool_name}/{call_id}/input → read back input
            3 if path[2] == "input" => {
                let key = format!("{}/{}", path[0], path[1]);
                let slots = self.call_slots.lock().await;
                let slot = slots.get(&key).ok_or_else(|| {
                    VfsError::NotFound(format!("call not found: {key}"))
                })?;
                Ok(slot.input.clone())
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unexpected proc path: {}",
                path.join("/")
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        // logos://proc/{tool_name}/{call_id}/input → submit and execute
        if path.len() == 3 && path[2] == "input" {
            return self.execute_call(path[0], path[1], content).await;
        }
        Err(VfsError::InvalidPath(format!(
            "proc write only supports .../{{call_id}}/input: {}",
            path.join("/")
        )))
    }

    async fn patch(&self, path: &[&str], _partial: &str) -> Result<(), VfsError> {
        Err(VfsError::InvalidPath(format!(
            "proc does not support patch: {}",
            path.join("/")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyTool;

    #[async_trait]
    impl ProcTool for DummyTool {
        fn name(&self) -> &str { "test.echo" }
        fn schema(&self) -> serde_json::Value {
            serde_json::json!({
                "name": "test.echo",
                "description": "Echo input back",
                "parameters": { "type": "object", "properties": { "msg": { "type": "string" } } }
            })
        }
        async fn call(&self, params: &str) -> Result<String, VfsError> {
            Ok(params.to_string())
        }
    }

    #[tokio::test]
    async fn list_tools() {
        let mut ns = ProcNs::new();
        ns.register(Arc::new(DummyTool));
        let list = ns.read(&[]).await.unwrap();
        assert!(list.contains("test.echo"));
    }

    #[tokio::test]
    async fn read_schema() {
        let mut ns = ProcNs::new();
        ns.register(Arc::new(DummyTool));
        let schema = ns.read(&["test.echo", ".schema"]).await.unwrap();
        assert!(schema.contains("Echo input back"));
    }

    #[tokio::test]
    async fn call_tool() {
        let mut ns = ProcNs::new();
        ns.register(Arc::new(DummyTool));
        let result = ns.call("test.echo", r#"{"msg":"hello"}"#).await.unwrap();
        assert_eq!(result, r#"{"msg":"hello"}"#);
    }

    #[tokio::test]
    async fn unknown_tool() {
        let ns = ProcNs::new();
        assert!(ns.call("nope", "{}").await.is_err());
    }

    #[tokio::test]
    async fn call_id_session() {
        let mut ns = ProcNs::new();
        ns.register(Arc::new(DummyTool));

        // Write input triggers execution
        ns.write(&["test.echo", "call-001", "input"], r#"{"msg":"hi"}"#)
            .await
            .unwrap();

        // Read error before output (output read triggers cleanup)
        let error = ns.read(&["test.echo", "call-001", "error"]).await.unwrap();
        assert_eq!(error, "null");

        // Read input back
        let input = ns.read(&["test.echo", "call-001", "input"]).await.unwrap();
        assert_eq!(input, r#"{"msg":"hi"}"#);

        // Read output (triggers slot cleanup per RFC)
        let output = ns.read(&["test.echo", "call-001", "output"]).await.unwrap();
        assert_eq!(output, r#"{"msg":"hi"}"#);

        // Slot should be cleaned up — reading again should fail
        assert!(ns.read(&["test.echo", "call-001", "output"]).await.is_err());
    }
}
