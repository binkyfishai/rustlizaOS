use std::sync::Arc;

use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::Runtime;
use rustliza_core::types::*;

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    pub runtime: Arc<dyn Runtime>,
    pub metrics: Option<Arc<rustliza_core::PipelineMetrics>>,
}

impl ApiState {
    pub fn metrics_snapshot(&self) -> rustliza_core::MetricsSnapshot {
        self.metrics
            .as_ref()
            .map(|m| m.snapshot())
            .unwrap_or(rustliza_core::MetricsSnapshot {
                messages_processed: 0,
                messages_skipped: 0,
                avg_total_ms: 0,
                avg_compose_ms: 0,
                avg_should_respond_ms: 0,
                avg_generate_ms: 0,
                api_errors: 0,
                api_retries: 0,
            })
    }
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SendMessageRequest {
    pub text: String,
    pub room_id: String,
    pub entity_id: String,
}

#[derive(Serialize)]
pub struct SendMessageResponse {
    pub responses: Vec<MessageResponse>,
}

#[derive(Serialize)]
pub struct MessageResponse {
    pub id: String,
    pub text: Option<String>,
    pub actions: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct AgentInfoResponse {
    pub agent_id: String,
    pub name: String,
    pub bio: String,
    pub topics: Vec<String>,
}

#[derive(Serialize)]
pub struct RoomResponse {
    pub id: String,
    pub name: Option<String>,
    pub source: String,
    pub channel_type: String,
}

#[derive(Serialize)]
pub struct MemoryResponse {
    pub id: String,
    pub text: Option<String>,
    pub entity_id: String,
    pub memory_type: String,
    pub created_at: Option<String>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub agent: String,
    pub version: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn health(AxumState(state): AxumState<ApiState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".into(),
        agent: state.runtime.character().name.clone(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}

async fn get_agent(AxumState(state): AxumState<ApiState>) -> impl IntoResponse {
    let character = state.runtime.character();
    Json(AgentInfoResponse {
        agent_id: state.runtime.agent_id().to_string(),
        name: character.name.clone(),
        bio: character.bio_text(),
        topics: character.topics.clone(),
    })
}

async fn send_message(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<SendMessageRequest>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let room_id = Uuid::parse_str(&req.room_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid room_id".into(),
            }),
        )
    })?;

    let entity_id = Uuid::parse_str(&req.entity_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid entity_id".into(),
            }),
        )
    })?;

    let message = Memory::new_message(
        state.runtime.agent_id(),
        entity_id,
        room_id,
        Content::text(&req.text),
    );

    let responses = state.runtime.process_message(&message).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    let response_messages: Vec<MessageResponse> = responses
        .iter()
        .map(|m| MessageResponse {
            id: m.id.to_string(),
            text: m.content.text.clone(),
            actions: m.content.actions.clone(),
        })
        .collect();

    Ok(Json(SendMessageResponse {
        responses: response_messages,
    }))
}

async fn send_message_stream(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<SendMessageRequest>,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, std::convert::Infallible>>>,
    (StatusCode, Json<ErrorResponse>),
> {
    let room_id = Uuid::parse_str(&req.room_id).map_err(|_| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid room_id".into() }))
    })?;
    let entity_id = Uuid::parse_str(&req.entity_id).map_err(|_| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid entity_id".into() }))
    })?;

    let message = Memory::new_message(
        state.runtime.agent_id(),
        entity_id,
        room_id,
        Content::text(&req.text),
    );

    let stream = state.runtime.process_message_stream(&message).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    let sse_stream = stream.map(|event| {
        let sse_event = match event {
            StreamEvent::Token(token) => {
                Event::default().event("token").data(token)
            }
            StreamEvent::Done { full_text } => {
                Event::default().event("done").data(full_text)
            }
            StreamEvent::Error(err) => {
                Event::default().event("error").data(err)
            }
        };
        Ok(sse_event)
    });

    Ok(Sse::new(sse_stream).keep_alive(KeepAlive::default()))
}

async fn get_room(
    AxumState(state): AxumState<ApiState>,
    Path(room_id): Path<String>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::parse_str(&room_id).map_err(|_| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid room_id".into() }))
    })?;

    let room = state.runtime.database().get_room(id).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    match room {
        Some(r) => Ok(Json(RoomResponse {
            id: r.id.to_string(),
            name: r.name,
            source: r.source,
            channel_type: format!("{:?}", r.channel_type),
        })),
        None => Err((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "room not found".into() }))),
    }
}

async fn get_memories(
    AxumState(state): AxumState<ApiState>,
    Path(room_id): Path<String>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::parse_str(&room_id).map_err(|_| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid room_id".into() }))
    })?;

    let memories = state
        .runtime
        .database()
        .get_memories(&GetMemoriesParams {
            room_id: Some(id),
            agent_id: Some(state.runtime.agent_id()),
            memory_type: Some(MemoryType::Message),
            count: Some(50),
            ..Default::default()
        })
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?;

    let response: Vec<MemoryResponse> = memories
        .iter()
        .map(|m| MemoryResponse {
            id: m.id.to_string(),
            text: m.content.text.clone(),
            entity_id: m.entity_id.to_string(),
            memory_type: format!("{:?}", m.memory_type),
            created_at: m.created_at.map(|dt| dt.to_rfc3339()),
        })
        .collect();

    Ok(Json(response))
}

#[derive(Deserialize)]
pub struct CreateRoomRequest {
    pub name: Option<String>,
    pub source: Option<String>,
}

async fn create_room(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<CreateRoomRequest>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let room = Room {
        id: Uuid::new_v4(),
        agent_id: state.runtime.agent_id(),
        source: req.source.unwrap_or_else(|| "api".into()),
        channel_type: ChannelType::Api,
        name: req.name,
        channel_id: None,
        world_id: None,
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };

    let id = state.runtime.database().create_room(&room).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    Ok((
        StatusCode::CREATED,
        Json(RoomResponse {
            id: id.to_string(),
            name: room.name,
            source: room.source,
            channel_type: format!("{:?}", room.channel_type),
        }),
    ))
}

async fn get_metrics(
    AxumState(state): AxumState<ApiState>,
) -> impl IntoResponse {
    // Downcast to AgentRuntime to access metrics
    // The runtime trait doesn't expose metrics directly — we store it in ApiState
    Json(state.metrics_snapshot())
}

#[derive(Deserialize)]
pub struct WebhookRequest {
    pub text: String,
    pub source: String,
    #[serde(default)]
    pub entity_name: Option<String>,
    #[serde(default)]
    pub room_name: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

async fn webhook(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<WebhookRequest>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let agent_id = state.runtime.agent_id();
    let entity_id = Uuid::new_v4();
    let room_id = Uuid::new_v4();

    // Create entity
    let entity = Entity {
        id: entity_id,
        agent_id,
        names: vec![req.entity_name.unwrap_or_else(|| "webhook".into())],
        metadata: req.metadata,
        created_at: Some(chrono::Utc::now()),
    };
    let _ = state.runtime.database().create_entity(&entity).await;

    // Create room
    let room = Room {
        id: room_id,
        agent_id,
        source: req.source.clone(),
        channel_type: ChannelType::Api,
        name: req.room_name.or(Some(format!("webhook-{}", req.source))),
        channel_id: None,
        world_id: None,
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };
    let _ = state.runtime.database().create_room(&room).await;

    let message = Memory::new_message(
        agent_id,
        entity_id,
        room_id,
        Content {
            text: Some(req.text),
            source: Some(req.source),
            ..Default::default()
        },
    );

    let responses = state.runtime.process_message(&message).await.map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    let response_messages: Vec<MessageResponse> = responses
        .iter()
        .map(|m| MessageResponse {
            id: m.id.to_string(),
            text: m.content.text.clone(),
            actions: m.content.actions.clone(),
        })
        .collect();

    Ok(Json(SendMessageResponse { responses: response_messages }))
}

// ---------------------------------------------------------------------------
// Router construction
// ---------------------------------------------------------------------------

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/agent", get(get_agent))
        .route("/metrics", get(get_metrics))
        .route("/message", post(send_message))
        .route("/message/stream", post(send_message_stream))
        .route("/webhook", post(webhook))
        .route("/rooms", post(create_room))
        .route("/rooms/{room_id}", get(get_room))
        .route("/rooms/{room_id}/memories", get(get_memories))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Server startup
// ---------------------------------------------------------------------------

pub async fn start_server(runtime: Arc<dyn Runtime>, bind: &str) -> Result<()> {
    let state = ApiState { runtime, metrics: None };
    let app = create_router(state);

    let listener = TcpListener::bind(bind)
        .await
        .map_err(|e| rustliza_core::RustlizaError::Other(format!("bind failed: {}", e)))?;

    info!(bind = %bind, "API server starting");

    axum::serve(listener, app)
        .await
        .map_err(|e| rustliza_core::RustlizaError::Other(format!("server error: {}", e)))?;

    Ok(())
}
