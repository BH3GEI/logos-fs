//! Context auto-injection — RFC 003 §5.5.
//!
//! Assembles context for an agent turn: session + summary + persona.
//! Injected before the agent's first token so it has immediate context
//! without needing to make any logos_read calls.

use std::sync::Arc;

use logos_mm::SessionStore;
use logos_vfs::{RoutingTable, VfsError};

/// Assemble context for an agent turn.
///
/// Returns a JSON object with:
/// - `session`: current reply-chain session (if any)
/// - `recent_summary`: latest short summary for the chat
/// - `sender_persona.long`: sender's stable core traits
/// - `sender_persona.mid`: sender's recent behavioral patterns
pub async fn assemble_context(
    table: &RoutingTable,
    sessions: &Arc<SessionStore>,
    chat_id: &str,
    sender_uid: &str,
    trigger_msg_id: Option<i64>,
) -> Result<String, VfsError> {
    // 1. Current session from clustering module
    let session = if let Some(msg_id) = trigger_msg_id {
        sessions
            .get_session_for_msg(msg_id)
            .await
            .map(|s| s.to_json())
    } else {
        sessions
            .get_active_session(chat_id)
            .await
            .map(|s| s.to_json())
    }
    .unwrap_or(serde_json::json!(null));

    // 2. Latest short summary
    let summary_uri = format!("logos://memory/groups/{chat_id}/summary/short/latest");
    let recent_summary = table.read(&summary_uri).await.unwrap_or_else(|_| "null".to_string());

    // 3. Sender persona/long.md
    let persona_long_uri = format!("logos://users/{sender_uid}/persona/long.md");
    let persona_long = table.read(&persona_long_uri).await.unwrap_or_else(|_| "".to_string());

    // 4. Sender persona/mid/latest
    let persona_mid_uri = format!("logos://users/{sender_uid}/persona/mid/");
    let persona_mid = table.read(&persona_mid_uri).await.unwrap_or_else(|_| "".to_string());

    let context = serde_json::json!({
        "session": session,
        "recent_summary": serde_json::from_str::<serde_json::Value>(&recent_summary)
            .unwrap_or(serde_json::json!(null)),
        "sender_persona": {
            "long": persona_long,
            "mid": persona_mid,
        }
    });

    Ok(context.to_string())
}
