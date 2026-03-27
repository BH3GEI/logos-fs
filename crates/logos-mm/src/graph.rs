//! Knowledge Graph Plugin — RFC 003 §10.6.
//!
//! Derived index over messages stored as (subject, predicate, object) triples.
//! Navigate by subject — "what do we know about X" — rather than by time.

use async_trait::async_trait;
use sqlx::SqlitePool;

use logos_vfs::VfsError;
use crate::plugin::MemoryPlugin;

pub struct GraphPlugin;

#[async_trait]
impl MemoryPlugin for GraphPlugin {
    fn name(&self) -> &str { "graph" }

    fn docs(&self) -> &str {
        "graph/ : Knowledge graph. Navigate by subject.\n\
         - graph/ : list all unique subjects\n\
         - graph/{subject} : all triples where subject matches\n\
         Triples are (subject, predicate, object) — no fixed schema.\n\
         Write: logos_write(\"graph/{subject}\", [{\"predicate\":\"...\",\"object\":\"...\"}])"
    }

    async fn init_schema(&self, pool: &SqlitePool) -> Result<(), VfsError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS triples (
                subject     TEXT NOT NULL,
                predicate   TEXT NOT NULL,
                object      TEXT NOT NULL,
                source_refs TEXT NOT NULL DEFAULT '[]',
                updated_at  TEXT NOT NULL,
                PRIMARY KEY (subject, predicate, object)
            )"
        )
        .execute(pool)
        .await
        .map_err(|e| VfsError::Sqlite(format!("init triples schema: {e}")))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_triples_subject ON triples(subject)")
            .execute(pool).await
            .map_err(|e| VfsError::Sqlite(format!("init triples index: {e}")))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_triples_object ON triples(object)")
            .execute(pool).await
            .map_err(|e| VfsError::Sqlite(format!("init triples index: {e}")))?;

        Ok(())
    }

    async fn read(&self, pool: &SqlitePool, _chat_id: &str, path: &[&str]) -> Result<String, VfsError> {
        match path.len() {
            // graph/ → list all unique subjects
            0 => {
                let rows = sqlx::query("SELECT DISTINCT subject FROM triples ORDER BY subject")
                    .fetch_all(pool)
                    .await
                    .map_err(|e| VfsError::Sqlite(e.to_string()))?;

                let subjects: Vec<String> = rows.iter().map(|r| {
                    use sqlx::Row;
                    r.get::<String, _>("subject")
                }).collect();
                Ok(serde_json::to_string(&subjects).unwrap_or_else(|_| "[]".to_string()))
            }
            // graph/{subject} → all triples with this subject
            _ => {
                let subject = path[0];
                let rows = sqlx::query(
                    "SELECT subject, predicate, object, source_refs, updated_at
                     FROM triples WHERE subject = ?1 ORDER BY predicate"
                )
                .bind(subject)
                .fetch_all(pool)
                .await
                .map_err(|e| VfsError::Sqlite(e.to_string()))?;

                if rows.is_empty() {
                    return Err(VfsError::NotFound(format!("no triples for subject: {subject}")));
                }

                let triples: Vec<serde_json::Value> = rows.iter().map(triple_row_to_json).collect();
                Ok(serde_json::to_string(&triples).unwrap_or_else(|_| "[]".to_string()))
            }
        }
    }

    async fn write(&self, pool: &SqlitePool, _chat_id: &str, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("graph write requires subject".to_string()));
        }
        let subject = path[0];
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Accept either a single triple or an array of triples
        let val: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| VfsError::InvalidJson(e.to_string()))?;

        let triples = if val.is_array() {
            val.as_array().cloned().unwrap_or_default()
        } else {
            vec![val]
        };

        for triple in &triples {
            let predicate = triple["predicate"].as_str().unwrap_or_default();
            let object = triple["object"].as_str().unwrap_or_default();
            if predicate.is_empty() || object.is_empty() { continue; }

            let source_refs = serde_json::to_string(&triple["source_refs"])
                .unwrap_or_else(|_| "[]".to_string());

            sqlx::query(
                "INSERT OR REPLACE INTO triples (subject, predicate, object, source_refs, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )
            .bind(subject)
            .bind(predicate)
            .bind(object)
            .bind(&source_refs)
            .bind(&now)
            .execute(pool)
            .await
            .map_err(|e| VfsError::Sqlite(format!("write triple: {e}")))?;
        }

        Ok(())
    }

    async fn patch(&self, pool: &SqlitePool, chat_id: &str, path: &[&str], partial: &str) -> Result<(), VfsError> {
        self.write(pool, chat_id, path, partial).await
    }
}

fn triple_row_to_json(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    use sqlx::Row;
    serde_json::json!({
        "subject": row.get::<String, _>("subject"),
        "predicate": row.get::<String, _>("predicate"),
        "object": row.get::<String, _>("object"),
        "source_refs": row.get::<String, _>("source_refs"),
        "updated_at": row.get::<String, _>("updated_at"),
    })
}
