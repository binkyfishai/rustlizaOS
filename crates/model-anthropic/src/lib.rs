use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::debug;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::traits::ModelProvider;
use rustliza_core::types::{GenerateTextParams, ModelType, StreamEvent, TextStream};

const MESSAGES_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicProvider {
    api_key: String,
    client: reqwest::Client,
    small_model: String,
    large_model: String,
}

impl AnthropicProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            client: reqwest::Client::new(),
            small_model: "claude-haiku-4-5-20251001".to_string(),
            large_model: "claude-sonnet-4-6-20250514".to_string(),
        }
    }

    pub fn with_models(
        api_key: impl Into<String>,
        small_model: impl Into<String>,
        large_model: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            client: reqwest::Client::new(),
            small_model: small_model.into(),
            large_model: large_model.into(),
        }
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
// Anthropic API types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct SystemContent {
    #[serde(rename = "type")]
    content_type: String,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<CacheControl>,
}

#[derive(Serialize)]
struct CacheControl {
    #[serde(rename = "type")]
    cache_type: String,
}

#[derive(Serialize)]
struct MessagesRequest {
    model: String,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<Vec<SystemContent>>,
    messages: Vec<Message>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    stop_sequences: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
    #[allow(dead_code)]
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct Usage {
    #[allow(dead_code)]
    input_tokens: u32,
    #[allow(dead_code)]
    output_tokens: u32,
    #[serde(default)]
    #[allow(dead_code)]
    cache_creation_input_tokens: u32,
    #[serde(default)]
    #[allow(dead_code)]
    cache_read_input_tokens: u32,
}

#[derive(Deserialize)]
struct ApiError {
    #[allow(dead_code)]
    error: ApiErrorDetail,
}

#[derive(Deserialize)]
struct ApiErrorDetail {
    message: String,
}

#[derive(Deserialize)]
struct SseContentBlockDelta {
    delta: SseDelta,
}

#[derive(Deserialize)]
struct SseDelta {
    #[serde(default)]
    text: String,
}

// ---------------------------------------------------------------------------
// Build request helper
// ---------------------------------------------------------------------------

fn build_request(provider: &AnthropicProvider, params: &GenerateTextParams, stream: bool) -> (reqwest::RequestBuilder, String) {
    let model = provider.model_for_type(params.model_type).to_string();
    let max_tokens = params.max_tokens.unwrap_or(2048);

    let system = if params.system_prompt.is_empty() {
        None
    } else {
        Some(vec![SystemContent {
            content_type: "text".into(),
            text: params.system_prompt.clone(),
            cache_control: Some(CacheControl {
                cache_type: "ephemeral".into(),
            }),
        }])
    };

    let request = MessagesRequest {
        model: model.clone(),
        max_tokens,
        temperature: params.temperature,
        system,
        messages: vec![Message {
            role: "user".to_string(),
            content: params.prompt.clone(),
        }],
        stop_sequences: params.stop_sequences.clone(),
        stream: if stream { Some(true) } else { None },
    };

    let req_builder = provider
        .client
        .post(MESSAGES_API_URL)
        .header("x-api-key", &provider.api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .header("anthropic-beta", "prompt-caching-2024-07-31")
        .header("content-type", "application/json")
        .json(&request);

    (req_builder, model)
}

impl AnthropicProvider {
    async fn generate_text_once(&self, params: &GenerateTextParams) -> Result<String> {
        let (req_builder, model) = build_request(self, params, false);
        debug!(model = %model, "generating text (non-streaming)");

        let response = req_builder
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

        let messages_response: MessagesResponse = serde_json::from_str(&body)
            .map_err(|e| RustlizaError::ModelProvider(format!("failed to parse response: {}", e)))?;

        if let Some(usage) = &messages_response.usage {
            debug!(
                cache_created = usage.cache_creation_input_tokens,
                cache_read = usage.cache_read_input_tokens,
                "prompt cache stats"
            );
        }

        let text = messages_response
            .content
            .iter()
            .filter(|b| b.block_type == "text")
            .map(|b| b.text.as_str())
            .collect::<Vec<_>>()
            .join("");

        Ok(text)
    }
}

// ---------------------------------------------------------------------------
// ModelProvider implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ModelProvider for AnthropicProvider {
    async fn generate_text(&self, params: &GenerateTextParams) -> Result<String> {
        rustliza_core::retry::with_retry(3, || self.generate_text_once(params)).await
    }

    async fn generate_text_stream(&self, params: &GenerateTextParams) -> Result<TextStream> {
        let (req_builder, model) = build_request(self, params, true);
        debug!(model = %model, "generating text (streaming)");

        let response = req_builder
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
            let mut full_text = String::new();
            let mut bytes_stream = response.bytes_stream();

            use futures::StreamExt;

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

                // Parse SSE lines from buffer
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

                        // Try parsing as content_block_delta (most common)
                        if let Ok(delta) = serde_json::from_str::<SseContentBlockDelta>(data) {
                            if !delta.delta.text.is_empty() {
                                full_text.push_str(&delta.delta.text);
                                let _ = tx.send(StreamEvent::Token(delta.delta.text)).await;
                            }
                        }
                        // message_start and message_stop are handled implicitly
                    }
                }
            }

            // Stream ended without [DONE] — still send what we have
            if !full_text.is_empty() {
                let _ = tx.send(StreamEvent::Done { full_text }).await;
            }
        });

        Ok(rx)
    }

    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>> {
        Ok(simple_hash_embedding(text, self.embedding_dimensions()))
    }

    fn embedding_dimensions(&self) -> usize {
        384
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

/// Deterministic pseudo-embedding based on text hashing.
/// Not semantically meaningful — placeholder for a real embedding model.
fn simple_hash_embedding(text: &str, dims: usize) -> Vec<f32> {
    let mut embedding = vec![0.0f32; dims];
    let bytes = text.as_bytes();

    for (i, &byte) in bytes.iter().enumerate() {
        let idx = i % dims;
        embedding[idx] += (byte as f32 - 128.0) / 128.0;
    }

    // Normalise
    let magnitude: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    if magnitude > 0.0 {
        for val in &mut embedding {
            *val /= magnitude;
        }
    }

    embedding
}
