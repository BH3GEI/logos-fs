use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::pb::{ArchiveRequest, ChatMessage, MessageMetadata, SearchMode, SearchRequest, SearchResult};
use crate::service::VfsError;

const DEFAULT_LIMIT: usize = 2;
const MAX_LIMIT: usize = 5;
const MIN_ROWS_FOR_PQ_TRAINING: usize = 256;
const LANCEDB_SESSIONS_TABLE: &str = "sessions";
const LANCEDB_MSG_ID_TO_SESSION_ID_TABLE: &str = "msg_id_to_session_id";

#[derive(Clone, Debug)]
pub struct EmbeddingConfig {
    pub base_url: String,
    pub model: String,
    pub dimension: usize,
}

pub struct SessionsStore {
    lancedb_uri: String,
    embedding: EmbeddingConfig,
    http_client: Client,
    lock: Mutex<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredChatMessage {
    user_id: String,
    message_id: String,
    chat_id: String,
    conversation_type: String,
    context: String,
    timestamp: i64,
    metadata: Option<StoredMessageMetadata>,
    vector: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMessageMetadata {
    is_bot: bool,
    username: String,
    reply_to_message_id: String,
    reply_to_user_id: String,
    is_reply_to_me: bool,
    is_mention_me: bool,
    mentions: Vec<String>,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    session_id: String,
    chat_id: String,
    center_vector: Vec<f32>,
    abstract_summary: String,
    messages: Vec<StoredChatMessage>,
    updated_at_unix: i64,
}

#[derive(Debug, Clone)]
struct MsgIdMappingRecord {
    msg_id: String,
    session_id: String,
    chat_id: String,
    updated_at_unix: i64,
}

#[derive(Debug, Deserialize)]
struct OllamaEmbedResponse {
    embedding: Option<Vec<f32>>,
}

impl SessionsStore {
    pub fn new(lancedb_uri: String, embedding: EmbeddingConfig) -> Self {
        Self {
            lancedb_uri,
            embedding,
            http_client: Client::new(),
            lock: Mutex::new(()),
        }
    }

    pub async fn archive(&self, req: ArchiveRequest) -> Result<(), VfsError> {
        if req.session_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "session_id cannot be empty".to_string(),
            ));
        }
        if req.chat_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "chat_id cannot be empty".to_string(),
            ));
        }
        if req.centroid_vector.len() != self.embedding.dimension {
            return Err(VfsError::InvalidRequest(format!(
                "centroid_vector dimension mismatch: expected {}, got {}",
                self.embedding.dimension,
                req.centroid_vector.len()
            )));
        }

        let _guard = self.lock.lock().await;
        let table = self.open_or_create_sessions_table().await?;
        let record = SessionRecord {
            session_id: req.session_id,
            chat_id: req.chat_id,
            center_vector: req.centroid_vector,
            abstract_summary: req.abstract_summary,
            messages: req.messages.into_iter().map(stored_message_from_pb).collect(),
            updated_at_unix: current_unix_timestamp()?,
        };

        let delete_predicate = format!(
            "session_id = '{}'",
            escape_sql_literal(&record.session_id)
        );
        table
            .delete(&delete_predicate)
            .await
            .map_err(|e| VfsError::Lance(format!("delete old session failed: {e}")))?;

        let batch = self.build_session_batch(&record)?;
        let schema = batch.schema();
        let source = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        table
            .add(Box::new(source))
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("insert session failed: {e}")))?;
        self.sync_msg_id_mappings(&record).await?;
        Ok(())
    }

    pub async fn search(&self, req: SearchRequest) -> Result<Vec<SearchResult>, VfsError> {
        match resolve_search_mode(req.mode) {
            SearchMode::Exact => self.search_exact(req).await,
            SearchMode::Semantic | SearchMode::Unspecified => self.search_semantic(req).await,
        }
    }

    async fn search_semantic(&self, req: SearchRequest) -> Result<Vec<SearchResult>, VfsError> {
        if req.scope.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "scope(chat_id) cannot be empty".to_string(),
            ));
        }
        if req.query.trim().is_empty() {
            return Err(VfsError::InvalidRequest("query cannot be empty".to_string()));
        }

        let query_vec = self.embed_query(&req.query).await?;
        if query_vec.len() != self.embedding.dimension {
            return Err(VfsError::InvalidRequest(format!(
                "query embedding dimension mismatch: expected {}, got {}",
                self.embedding.dimension,
                query_vec.len()
            )));
        }

        let limit = clamp_limit(req.limit);
        let _guard = self.lock.lock().await;
        let table = self.open_or_create_sessions_table().await?;
        let filter = format!("chat_id = '{}'", escape_sql_literal(&req.scope));

        let stream = table
            .query()
            .only_if(filter)
            .nearest_to(query_vec.as_slice())
            .map_err(|e| VfsError::Lance(format!("build vector query failed: {e}")))?
            .limit(limit)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("execute vector query failed: {e}")))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| VfsError::Lance(format!("collect search results failed: {e}")))?;

        let mut results = Vec::new();
        for batch in batches {
            results.extend(search_results_from_batch(&batch, &query_vec)?);
        }
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
        results.truncate(limit);
        Ok(results)
    }

    async fn search_exact(&self, req: SearchRequest) -> Result<Vec<SearchResult>, VfsError> {
        let msg_id = req.query.trim();
        if msg_id.is_empty() {
            return Err(VfsError::InvalidRequest(
                "query(msg_id) cannot be empty in exact mode".to_string(),
            ));
        }

        let _guard = self.lock.lock().await;
        let mapping_table = self.open_or_create_msg_id_to_session_id_table().await?;
        let Some(session_id) = self
            .find_session_id_by_msg_id(&mapping_table, msg_id)
            .await?
        else {
            return Ok(Vec::new());
        };

        let sessions_table = self.open_or_create_sessions_table().await?;
        let mut filter = format!("session_id = '{}'", escape_sql_literal(&session_id));
        if !req.scope.trim().is_empty() {
            filter = format!(
                "{} AND chat_id = '{}'",
                filter,
                escape_sql_literal(req.scope.trim())
            );
        }
        let stream = sessions_table
            .query()
            .only_if(filter)
            .limit(1)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("execute exact query failed: {e}")))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| VfsError::Lance(format!("collect exact query results failed: {e}")))?;

        let mut results = Vec::new();
        for batch in batches {
            results.extend(exact_results_from_batch(&batch)?);
        }
        results.truncate(1);
        Ok(results)
    }

    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, VfsError> {
        let base_url = self.embedding.base_url.trim_end_matches('/');
        let input = if query.trim().is_empty() {
            "(empty)"
        } else {
            query.trim()
        };
        let response = self
            .http_client
            .post(format!("{base_url}/api/embeddings"))
            .json(&serde_json::json!({
                "model": self.embedding.model,
                "prompt": input
            }))
            .send()
            .await
            .map_err(|e| VfsError::Http(format!("ollama request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read response body".to_string());
            return Err(VfsError::Http(format!(
                "ollama embed failed ({status}): {body}"
            )));
        }

        let payload: OllamaEmbedResponse = response
            .json()
            .await
            .map_err(|e| VfsError::Http(format!("invalid ollama response: {e}")))?;
        payload.embedding.ok_or_else(|| {
            VfsError::Http("invalid ollama response: embedding field missing".to_string())
        })
    }

    async fn open_or_create_sessions_table(&self) -> Result<lancedb::Table, VfsError> {
        let db = lancedb::connect(&self.lancedb_uri)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("connect lancedb failed: {e}")))?;
        if let Ok(table) = db.open_table(LANCEDB_SESSIONS_TABLE).execute().await {
            self.ensure_vector_index(&table).await?;
            return Ok(table);
        }

        let empty_batch = self.build_empty_sessions_batch()?;
        let schema = empty_batch.schema();
        let source = RecordBatchIterator::new(vec![Ok(empty_batch)].into_iter(), schema);
        let table = db
            .create_table(LANCEDB_SESSIONS_TABLE, Box::new(source))
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("create sessions table failed: {e}")))?;
        self.ensure_vector_index(&table).await?;
        Ok(table)
    }

    async fn open_or_create_msg_id_to_session_id_table(&self) -> Result<lancedb::Table, VfsError> {
        let db = lancedb::connect(&self.lancedb_uri)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("connect lancedb failed: {e}")))?;
        if let Ok(table) = db
            .open_table(LANCEDB_MSG_ID_TO_SESSION_ID_TABLE)
            .execute()
            .await
        {
            return Ok(table);
        }

        let empty_batch = self.build_empty_msg_id_mapping_batch()?;
        let schema = empty_batch.schema();
        let source = RecordBatchIterator::new(vec![Ok(empty_batch)].into_iter(), schema);
        db.create_table(LANCEDB_MSG_ID_TO_SESSION_ID_TABLE, Box::new(source))
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("create msg_id_to_session_id table failed: {e}")))
    }

    async fn ensure_vector_index(&self, table: &lancedb::Table) -> Result<(), VfsError> {
        let indices = table
            .list_indices()
            .await
            .map_err(|e| VfsError::Lance(format!("list indices failed: {e}")))?;
        let has_center_vector_index = indices.iter().any(|idx| {
            idx.columns.len() == 1 && idx.columns[0].as_str() == "center_vector"
        });
        if has_center_vector_index {
            return Ok(());
        }

        let existing_rows = table
            .query()
            .limit(MIN_ROWS_FOR_PQ_TRAINING)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("count rows for index failed: {e}")))?
            .try_fold(0usize, |acc, batch| async move { Ok(acc + batch.num_rows()) })
            .await
            .map_err(|e| VfsError::Lance(format!("count rows for index failed: {e}")))?;
        if existing_rows < MIN_ROWS_FOR_PQ_TRAINING {
            return Ok(());
        }

        if let Err(e) = table
            .create_index(&["center_vector"], Index::Auto)
            .execute()
            .await
        {
            let msg = e.to_string();
            // Lance currently cannot build an untrained vector index on empty tables.
            // This is safe to ignore; index creation will be retried on subsequent opens.
            if !msg.contains("Creating empty vector indices with train=False is not yet implemented")
            {
                return Err(VfsError::Lance(format!("create vector index failed: {e}")));
            }
        }
        Ok(())
    }

    fn build_empty_sessions_batch(&self) -> Result<RecordBatch, VfsError> {
        let schema = self.sessions_schema()?;
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            std::iter::empty::<Option<Vec<Option<f32>>>>(),
            self.embedding.dimension as i32,
        );
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(vectors),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .map_err(|e| VfsError::Lance(format!("build empty record batch failed: {e}")))
    }

    fn build_empty_msg_id_mapping_batch(&self) -> Result<RecordBatch, VfsError> {
        let schema = self.msg_id_mapping_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        )
        .map_err(|e| VfsError::Lance(format!("build empty msg_id mapping batch failed: {e}")))
    }

    fn build_session_batch(&self, record: &SessionRecord) -> Result<RecordBatch, VfsError> {
        let schema = self.sessions_schema()?;
        let messages_json = serde_json::to_string(&record.messages)
            .map_err(|e| VfsError::InvalidJson(format!("serialize messages failed: {e}")))?;
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            std::iter::once(Some(
                record
                    .center_vector
                    .iter()
                    .copied()
                    .map(Some)
                    .collect::<Vec<Option<f32>>>(),
            )),
            self.embedding.dimension as i32,
        );
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![record.session_id.as_str()])),
                Arc::new(StringArray::from(vec![record.chat_id.as_str()])),
                Arc::new(vectors),
                Arc::new(StringArray::from(vec![record.abstract_summary.as_str()])),
                Arc::new(StringArray::from(vec![messages_json.as_str()])),
                Arc::new(Int64Array::from(vec![record.updated_at_unix])),
            ],
        )
        .map_err(|e| VfsError::Lance(format!("build session record batch failed: {e}")))
    }

    fn build_msg_id_mapping_batch(
        &self,
        records: &[MsgIdMappingRecord],
    ) -> Result<RecordBatch, VfsError> {
        let schema = self.msg_id_mapping_schema();
        let msg_ids = records.iter().map(|r| r.msg_id.as_str()).collect::<Vec<_>>();
        let session_ids = records
            .iter()
            .map(|r| r.session_id.as_str())
            .collect::<Vec<_>>();
        let chat_ids = records.iter().map(|r| r.chat_id.as_str()).collect::<Vec<_>>();
        let updated_at = records.iter().map(|r| r.updated_at_unix).collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(msg_ids)),
                Arc::new(StringArray::from(session_ids)),
                Arc::new(StringArray::from(chat_ids)),
                Arc::new(Int64Array::from(updated_at)),
            ],
        )
        .map_err(|e| VfsError::Lance(format!("build msg_id mapping batch failed: {e}")))
    }

    fn sessions_schema(&self) -> Result<Arc<Schema>, VfsError> {
        let dim = i32::try_from(self.embedding.dimension)
            .map_err(|e| VfsError::InvalidRequest(format!("invalid embedding dimension: {e}")))?;
        Ok(Arc::new(Schema::new(vec![
            Field::new("session_id", DataType::Utf8, false),
            Field::new("chat_id", DataType::Utf8, false),
            Field::new(
                "center_vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim,
                ),
                false,
            ),
            Field::new("abstract_summary", DataType::Utf8, false),
            Field::new("messages_json", DataType::Utf8, false),
            Field::new("updated_at_unix", DataType::Int64, false),
        ])))
    }

    fn msg_id_mapping_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("msg_id", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
            Field::new("chat_id", DataType::Utf8, false),
            Field::new("updated_at_unix", DataType::Int64, false),
        ]))
    }

    async fn sync_msg_id_mappings(&self, session: &SessionRecord) -> Result<(), VfsError> {
        let mapping_table = self.open_or_create_msg_id_to_session_id_table().await?;
        let delete_predicate = format!("session_id = '{}'", escape_sql_literal(&session.session_id));
        mapping_table
            .delete(&delete_predicate)
            .await
            .map_err(|e| VfsError::Lance(format!("delete old msg_id mappings failed: {e}")))?;

        let mappings = build_msg_id_mapping_records(session)?;
        if mappings.is_empty() {
            return Ok(());
        }

        let batch = self.build_msg_id_mapping_batch(&mappings)?;
        let schema = batch.schema();
        let source = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        mapping_table
            .add(Box::new(source))
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("insert msg_id mappings failed: {e}")))?;
        Ok(())
    }

    async fn find_session_id_by_msg_id(
        &self,
        mapping_table: &lancedb::Table,
        msg_id: &str,
    ) -> Result<Option<String>, VfsError> {
        let filter = format!("msg_id = '{}'", escape_sql_literal(msg_id));
        let stream = mapping_table
            .query()
            .only_if(filter)
            .limit(1)
            .execute()
            .await
            .map_err(|e| VfsError::Lance(format!("execute msg_id lookup failed: {e}")))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| VfsError::Lance(format!("collect msg_id lookup failed: {e}")))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let session_ids = downcast_utf8_column(&batch, "session_id")?;
            return Ok(Some(session_ids.value(0).to_string()));
        }
        Ok(None)
    }
}

fn resolve_search_mode(raw_mode: i32) -> SearchMode {
    SearchMode::try_from(raw_mode).unwrap_or(SearchMode::Unspecified)
}

fn search_results_from_batch(batch: &RecordBatch, query_vec: &[f32]) -> Result<Vec<SearchResult>, VfsError> {
    let session_ids = downcast_utf8_column(batch, "session_id")?;
    let vectors = downcast_fsl_column(batch, "center_vector")?;
    let summaries = downcast_utf8_column(batch, "abstract_summary")?;
    let messages_json = downcast_utf8_column(batch, "messages_json")?;

    let mut results = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let center_vector = extract_vector(vectors, row)?;
        let stored_messages: Vec<StoredChatMessage> = serde_json::from_str(messages_json.value(row))
            .map_err(|e| VfsError::InvalidJson(format!("deserialize messages_json failed: {e}")))?;
        let score = cosine_similarity(query_vec, &center_vector);
        results.push(SearchResult {
            session_id: session_ids.value(row).to_string(),
            center_vector,
            abstract_summary: summaries.value(row).to_string(),
            messages: stored_messages.into_iter().map(pb_message_from_stored).collect(),
            score,
        });
    }
    Ok(results)
}

fn exact_results_from_batch(batch: &RecordBatch) -> Result<Vec<SearchResult>, VfsError> {
    let session_ids = downcast_utf8_column(batch, "session_id")?;
    let vectors = downcast_fsl_column(batch, "center_vector")?;
    let summaries = downcast_utf8_column(batch, "abstract_summary")?;
    let messages_json = downcast_utf8_column(batch, "messages_json")?;

    let mut results = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let center_vector = extract_vector(vectors, row)?;
        let stored_messages: Vec<StoredChatMessage> = serde_json::from_str(messages_json.value(row))
            .map_err(|e| VfsError::InvalidJson(format!("deserialize messages_json failed: {e}")))?;
        results.push(SearchResult {
            session_id: session_ids.value(row).to_string(),
            center_vector,
            abstract_summary: summaries.value(row).to_string(),
            messages: stored_messages.into_iter().map(pb_message_from_stored).collect(),
            score: 1.0,
        });
    }
    Ok(results)
}

fn downcast_utf8_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, VfsError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| VfsError::Lance(format!("column {name} missing: {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| VfsError::Lance(format!("column {name} type mismatch")))
}

fn downcast_fsl_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeListArray, VfsError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|e| VfsError::Lance(format!("column {name} missing: {e}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| VfsError::Lance(format!("column {name} type mismatch")))
}

fn extract_vector(array: &FixedSizeListArray, row: usize) -> Result<Vec<f32>, VfsError> {
    let values = array.value(row);
    let floats = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| VfsError::Lance("center_vector inner type mismatch".to_string()))?;
    let mut result = Vec::with_capacity(floats.len());
    for i in 0..floats.len() {
        result.push(floats.value(i));
    }
    Ok(result)
}

fn clamp_limit(raw: i32) -> usize {
    if raw <= 0 {
        return DEFAULT_LIMIT;
    }
    usize::try_from(raw).unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
}

fn current_unix_timestamp() -> Result<i64, VfsError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| VfsError::Io(format!("system clock error: {e}")))?;
    i64::try_from(duration.as_secs())
        .map_err(|e| VfsError::Io(format!("failed to convert timestamp: {e}")))
}

fn escape_sql_literal(raw: &str) -> String {
    raw.replace('\'', "''")
}

fn build_msg_id_mapping_records(session: &SessionRecord) -> Result<Vec<MsgIdMappingRecord>, VfsError> {
    let mut seen_msg_ids = HashSet::new();
    let mut mappings = Vec::new();
    for message in &session.messages {
        let chat_id = if message.chat_id.trim().is_empty() {
            session.chat_id.as_str()
        } else {
            message.chat_id.as_str()
        };
        let message_id = message.message_id.trim();
        if chat_id.trim().is_empty() || message_id.is_empty() {
            continue;
        }
        let msg_id = format!("{chat_id}:{message_id}");
        if seen_msg_ids.insert(msg_id.clone()) {
            mappings.push(MsgIdMappingRecord {
                msg_id,
                session_id: session.session_id.clone(),
                chat_id: session.chat_id.clone(),
                updated_at_unix: session.updated_at_unix,
            });
        }
    }
    Ok(mappings)
}

fn stored_message_from_pb(msg: ChatMessage) -> StoredChatMessage {
    StoredChatMessage {
        user_id: msg.user_id,
        message_id: msg.message_id,
        chat_id: msg.chat_id,
        conversation_type: msg.conversation_type,
        context: msg.context,
        timestamp: msg.timestamp,
        metadata: msg.metadata.map(stored_metadata_from_pb),
        vector: msg.vector,
    }
}

fn pb_message_from_stored(msg: StoredChatMessage) -> ChatMessage {
    ChatMessage {
        user_id: msg.user_id,
        message_id: msg.message_id,
        chat_id: msg.chat_id,
        conversation_type: msg.conversation_type,
        context: msg.context,
        timestamp: msg.timestamp,
        metadata: msg.metadata.map(pb_metadata_from_stored),
        vector: msg.vector,
    }
}

fn stored_metadata_from_pb(meta: MessageMetadata) -> StoredMessageMetadata {
    StoredMessageMetadata {
        is_bot: meta.is_bot,
        username: meta.username,
        reply_to_message_id: meta.reply_to_message_id,
        reply_to_user_id: meta.reply_to_user_id,
        is_reply_to_me: meta.is_reply_to_me,
        is_mention_me: meta.is_mention_me,
        mentions: meta.mentions,
    }
}

fn pb_metadata_from_stored(meta: StoredMessageMetadata) -> MessageMetadata {
    MessageMetadata {
        is_bot: meta.is_bot,
        username: meta.username,
        reply_to_message_id: meta.reply_to_message_id,
        reply_to_user_id: meta.reply_to_user_id,
        is_reply_to_me: meta.is_reply_to_me,
        is_mention_me: meta.is_mention_me,
        mentions: meta.mentions,
    }
}
