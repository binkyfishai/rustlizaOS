use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::traits::ModelProvider;
use rustliza_core::types::{GenerateTextParams, ModelType, StreamEvent, TextStream};

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

pub struct OpenAIProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
    small_model: String,
    large_model: String,
    embedding_model: String,
}

impl OpenAIProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.into(),
            client: reqwest::Client::new(),
            small_model: "gpt-4o-mini".into(),
            large_model: "gpt-4o".into(),
            embedding_model: "text-embedding-3-small".into(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into().trim_end_matches('/').to_string();
        self
    }

    pub fn with_models(
        mut self,
        small_model: impl Into<String>,
        large_model: impl Into<String>,
    ) -> Self {
        self.small_model = small_model.into();
        self.large_model = large_model.into();
        self
    }

    pub fn with_embedding_model(mut self, model: impl Into<String>) -> Self {
        self.embedding_model = model.into();
        self
    }

    fn model_for_type(&self, model_type: ModelType) -> &str {
        match model_type {
            ModelType::TextSmall => &self.small_model,
            ModelType::TextLarge => &self.large_model,
            ModelType::TextEmbedding => &self.small_model,
            ModelType::ImageDescription => &self.large_model,
        }
    }
}

// ---------------------------------------------------------------------------
// OpenAI API types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    stop: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatStreamChunk {
    choices: Vec<StreamChoice>,
}

#[derive(Deserialize)]
struct StreamChoice {
    delta: StreamDelta,
}

#[derive(Deserialize)]
struct StreamDelta {
    #[serde(default)]
    content: Option<String>,
}

#[derive(Serialize)]
struct EmbeddingRequest {
    model: String,
    input: String,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct ApiError {
    error: ApiErrorDetail,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    message: String,
}

impl OpenAIProvider {
    async fn generate_text_once(&self, params: &GenerateTextParams) -> Result<String> {
        let model = self.model_for_type(params.model_type).to_string();
        debug!(model = %model, "generating text (OpenAI-compat)");

        let mut messages = Vec::new();
        if !params.system_prompt.is_empty() {
            messages.push(ChatMessage {
                role: "system".into(),
                content: params.system_prompt.clone(),
            });
        }
        messages.push(ChatMessage {
            role: "user".into(),
            content: params.prompt.clone(),
        });

        let request = ChatRequest {
            model,
            messages,
            max_tokens: params.max_tokens,
            temperature: params.temperature,
            stop: params.stop_sequences.clone(),
            stream: None,
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| RustlizaError::ModelProvider(format!("request failed: {}", e)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| RustlizaError::ModelProvider(format!("failed to read response: {}", e)))?;

        if !status.is_success() {
            if let Ok(err) = serde_json::from_str::<ApiError>(&body) {
                return Err(RustlizaError::ModelProvider(format!(
                    "API error ({}): {}",
                    status, err.error.message
                )));
            }
            return Err(RustlizaError::ModelProvider(format!(
                "API error ({}): {}",
                status, body
            )));
        }

        let chat_response: ChatResponse = serde_json::from_str(&body)
            .map_err(|e| RustlizaError::ModelProvider(format!("failed to parse response: {}", e)))?;

        let text = chat_response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(text)
    }
}

// ---------------------------------------------------------------------------
// ModelProvider implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ModelProvider for OpenAIProvider {
    async fn generate_text(&self, params: &GenerateTextParams) -> Result<String> {
        rustliza_core::retry::with_retry(3, || self.generate_text_once(params)).await
    }

    async fn generate_text_stream(&self, params: &GenerateTextParams) -> Result<TextStream> {
        let model = self.model_for_type(params.model_type).to_string();
        debug!(model = %model, "generating text stream (OpenAI-compat)");

        let mut messages = Vec::new();
        if !params.system_prompt.is_empty() {
            messages.push(ChatMessage {
                role: "system".into(),
                content: params.system_prompt.clone(),
            });
        }
        messages.push(ChatMessage {
            role: "user".into(),
            content: params.prompt.clone(),
        });

        let request = ChatRequest {
            model,
            messages,
            max_tokens: params.max_tokens,
            temperature: params.temperature,
            stop: params.stop_sequences.clone(),
            stream: Some(true),
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| RustlizaError::ModelProvider(format!("stream request failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<ApiError>(&body) {
                return Err(RustlizaError::ModelProvider(format!(
                    "API error ({}): {}",
                    status, err.error.message
                )));
            }
            return Err(RustlizaError::ModelProvider(format!(
                "API error ({}): {}",
                status, body
            )));
        }

        let (tx, rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);

        tokio::spawn(async move {
            use futures::StreamExt;

            let mut full_text = String::new();
            let mut bytes_stream = response.bytes_stream();
            let mut buffer = String::new();

            while let Some(chunk_result) = bytes_stream.next().await {
                let chunk = match chunk_result {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(StreamEvent::Error(e.to_string())).await;
                        return;
                    }
                };

                let text = match std::str::from_utf8(&chunk) {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                buffer.push_str(text);

                while let Some(newline_pos) = buffer.find('\n') {
                    let line = buffer[..newline_pos].trim_end_matches('\r').to_string();
                    buffer = buffer[newline_pos + 1..].to_string();

                    if line.is_empty() || line.starts_with(':') {
                        continue;
                    }

                    if let Some(data) = line.strip_prefix("data: ") {
                        if data == "[DONE]" {
                            let _ = tx.send(StreamEvent::Done { full_text }).await;
                            return;
                        }

                        if let Ok(chunk) = serde_json::from_str::<ChatStreamChunk>(data) {
                            if let Some(choice) = chunk.choices.first() {
                                if let Some(content) = &choice.delta.content {
                                    if !content.is_empty() {
                                        full_text.push_str(content);
                                        let _ = tx.send(StreamEvent::Token(content.clone())).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !full_text.is_empty() {
                let _ = tx.send(StreamEvent::Done { full_text }).await;
            }
        });

        Ok(rx)
    }

    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>> {
        let request = EmbeddingRequest {
            model: self.embedding_model.clone(),
            input: text.to_string(),
        };

        let response = self
            .client
            .post(format!("{}/embeddings", self.base_url))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| RustlizaError::ModelProvider(format!("embedding request failed: {}", e)))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| RustlizaError::ModelProvider(format!("failed to read response: {}", e)))?;

        if !status.is_success() {
            if let Ok(err) = serde_json::from_str::<ApiError>(&body) {
                return Err(RustlizaError::ModelProvider(format!(
                    "embedding error ({}): {}",
                    status, err.error.message
                )));
            }
            return Err(RustlizaError::ModelProvider(format!(
                "embedding error ({}): {}",
                status, body
            )));
        }

        let emb_response: EmbeddingResponse = serde_json::from_str(&body)
            .map_err(|e| RustlizaError::ModelProvider(format!("failed to parse embedding: {}", e)))?;

        emb_response
            .data
            .first()
            .map(|d| d.embedding.clone())
            .ok_or_else(|| RustlizaError::ModelProvider("no embedding in response".into()))
    }

    fn embedding_dimensions(&self) -> usize {
        1536
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}
