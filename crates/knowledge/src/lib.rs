use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::search::BM25;
use rustliza_core::traits::{DatabaseAdapter, ModelProvider, Provider, ProviderResult, Runtime};
use rustliza_core::types::*;

// ---------------------------------------------------------------------------
// Document chunking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ChunkOptions {
    pub chunk_size: usize,
    pub chunk_overlap: usize,
}

impl Default for ChunkOptions {
    fn default() -> Self {
        Self {
            chunk_size: 512,
            chunk_overlap: 64,
        }
    }
}

pub fn chunk_text(text: &str, options: &ChunkOptions) -> Vec<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() <= options.chunk_size {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < words.len() {
        let end = (start + options.chunk_size).min(words.len());
        let chunk = words[start..end].join(" ");
        chunks.push(chunk);

        if end >= words.len() {
            break;
        }
        start = end - options.chunk_overlap;
    }

    chunks
}

// ---------------------------------------------------------------------------
// Knowledge ingestion pipeline
// ---------------------------------------------------------------------------

pub struct KnowledgePipeline {
    database: Arc<dyn DatabaseAdapter>,
    model_provider: Arc<dyn ModelProvider>,
    chunk_options: ChunkOptions,
}

impl KnowledgePipeline {
    pub fn new(
        database: Arc<dyn DatabaseAdapter>,
        model_provider: Arc<dyn ModelProvider>,
    ) -> Self {
        Self {
            database,
            model_provider,
            chunk_options: ChunkOptions::default(),
        }
    }

    pub fn with_chunk_options(mut self, options: ChunkOptions) -> Self {
        self.chunk_options = options;
        self
    }

    pub async fn ingest_text(
        &self,
        text: &str,
        title: &str,
        agent_id: AgentId,
        room_id: RoomId,
    ) -> Result<Vec<MemoryId>> {
        info!(title = %title, text_len = text.len(), "ingesting document");

        // Create the document memory
        let doc_id = Uuid::new_v4();
        let doc_memory = Memory {
            id: doc_id,
            content: Content {
                text: Some(title.to_string()),
                ..Default::default()
            },
            entity_id: agent_id,
            agent_id,
            room_id,
            world_id: None,
            unique: true,
            created_at: Some(Utc::now()),
            embedding: None,
            metadata: Some(serde_json::json!({
                "type": "document",
                "title": title,
                "total_length": text.len(),
            })),
            memory_type: MemoryType::Document,
        };
        self.database.create_memory(&doc_memory).await?;

        // Chunk the text
        let chunks = chunk_text(text, &self.chunk_options);
        info!(chunks = chunks.len(), "split into chunks");

        let mut fragment_ids = Vec::new();

        for (i, chunk) in chunks.iter().enumerate() {
            let fragment_id = Uuid::new_v4();

            // Generate embedding
            let embedding = self
                .model_provider
                .generate_embedding(chunk)
                .await
                .map_err(|e| RustlizaError::Other(format!("embedding generation failed: {}", e)))?;

            let fragment = Memory {
                id: fragment_id,
                content: Content::text(chunk),
                entity_id: agent_id,
                agent_id,
                room_id,
                world_id: None,
                unique: true,
                created_at: Some(Utc::now()),
                embedding: Some(embedding),
                metadata: Some(serde_json::json!({
                    "type": "fragment",
                    "document_id": doc_id.to_string(),
                    "chunk_index": i,
                    "total_chunks": chunks.len(),
                })),
                memory_type: MemoryType::Fragment,
            };

            self.database.create_memory(&fragment).await?;
            fragment_ids.push(fragment_id);

            debug!(chunk = i, fragment_id = %fragment_id, "stored fragment");
        }

        info!(fragments = fragment_ids.len(), "document ingestion complete");
        Ok(fragment_ids)
    }

    pub async fn ingest_file(
        &self,
        path: &std::path::Path,
        agent_id: AgentId,
        room_id: RoomId,
    ) -> Result<Vec<MemoryId>> {
        let text = std::fs::read_to_string(path)?;
        let title = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("untitled");
        self.ingest_text(&text, title, agent_id, room_id).await
    }
}

// ---------------------------------------------------------------------------
// Knowledge retrieval
// ---------------------------------------------------------------------------

pub struct KnowledgeRetriever {
    database: Arc<dyn DatabaseAdapter>,
    model_provider: Arc<dyn ModelProvider>,
}

impl KnowledgeRetriever {
    pub fn new(
        database: Arc<dyn DatabaseAdapter>,
        model_provider: Arc<dyn ModelProvider>,
    ) -> Self {
        Self {
            database,
            model_provider,
        }
    }

    pub async fn search(
        &self,
        query: &str,
        agent_id: AgentId,
        top_k: usize,
    ) -> Result<Vec<Memory>> {
        // Vector search
        let query_embedding = self.model_provider.generate_embedding(query).await?;

        let vector_results = self
            .database
            .search_memories(&SearchMemoriesParams {
                embedding: query_embedding,
                room_id: None,
                agent_id,
                memory_type: Some(MemoryType::Fragment),
                match_threshold: 0.3,
                match_count: top_k * 2,
                unique: None,
            })
            .await
            .unwrap_or_default();

        // BM25 keyword search over the same fragments
        let all_fragments = self
            .database
            .get_memories(&GetMemoriesParams {
                agent_id: Some(agent_id),
                memory_type: Some(MemoryType::Fragment),
                count: Some(500),
                ..Default::default()
            })
            .await
            .unwrap_or_default();

        let mut bm25 = BM25::new();
        let mut fragment_map: Vec<&Memory> = Vec::new();

        for fragment in &all_fragments {
            let text = fragment.content.text.as_deref().unwrap_or("");
            bm25.add_document(text);
            fragment_map.push(fragment);
        }

        let keyword_results = bm25.search(query, top_k * 2);

        // Merge and deduplicate results
        let mut seen = std::collections::HashSet::new();
        let mut merged = Vec::new();

        // Add vector results first (usually more relevant)
        for mem in vector_results {
            if seen.insert(mem.id) {
                merged.push(mem);
            }
        }

        // Add keyword results
        for (idx, _score) in keyword_results {
            if idx < fragment_map.len() {
                let mem = fragment_map[idx];
                if seen.insert(mem.id) {
                    merged.push(mem.clone());
                }
            }
        }

        merged.truncate(top_k);
        Ok(merged)
    }
}

// ---------------------------------------------------------------------------
// Knowledge Provider — injects retrieved knowledge into agent state
// ---------------------------------------------------------------------------

pub struct KnowledgeProvider {
    database: Arc<dyn DatabaseAdapter>,
    model_provider: Arc<dyn ModelProvider>,
    top_k: usize,
}

impl KnowledgeProvider {
    pub fn new(
        database: Arc<dyn DatabaseAdapter>,
        model_provider: Arc<dyn ModelProvider>,
    ) -> Self {
        Self {
            database,
            model_provider,
            top_k: 5,
        }
    }

    pub fn with_top_k(mut self, k: usize) -> Self {
        self.top_k = k;
        self
    }
}

#[async_trait]
impl Provider for KnowledgeProvider {
    fn name(&self) -> &str {
        "knowledge"
    }
    fn description(&self) -> &str {
        "Retrieves relevant knowledge from ingested documents"
    }
    fn position(&self) -> i32 {
        5
    }

    async fn get(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let query = message.content.text.as_deref().unwrap_or("");
        if query.is_empty() {
            return Ok(ProviderResult::default());
        }

        let retriever = KnowledgeRetriever::new(
            self.database.clone(),
            self.model_provider.clone(),
        );

        let results = retriever.search(query, runtime.agent_id(), self.top_k).await?;

        if results.is_empty() {
            return Ok(ProviderResult::default());
        }

        let text = results
            .iter()
            .filter_map(|m| m.content.text.as_deref())
            .enumerate()
            .map(|(i, t)| format!("[{}] {}", i + 1, t))
            .collect::<Vec<_>>()
            .join("\n\n");

        Ok(ProviderResult {
            text: Some(format!("Relevant knowledge:\n{}", text)),
            ..Default::default()
        })
    }
}
