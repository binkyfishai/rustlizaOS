use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::Runtime;
use rustliza_core::types::*;
use rustliza_plugin_coding::CodingEvent;
use rustliza_plugin_vamp::VampState;

mod vamp;

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    pub runtime: Arc<dyn Runtime>,
    pub metrics: Option<Arc<rustliza_core::PipelineMetrics>>,
    pub project_dir: Arc<RwLock<Option<PathBuf>>>,
    pub vamp: Option<Arc<VampState>>,
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

#[derive(Serialize, Clone)]
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
// Task endpoint — autonomous coding loop with SSE progress
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TaskRequest {
    pub task: String,
}

async fn run_task(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<TaskRequest>,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, std::convert::Infallible>>>,
    (StatusCode, Json<ErrorResponse>),
> {
    let project_dir = state.project_dir.read().await.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "no project directory set — use /project <path> first".into(),
            }),
        )
    })?;

    let (tx, rx) = tokio::sync::mpsc::channel::<CodingEvent>(64);
    let runtime = state.runtime.clone();
    let task = req.task.clone();

    tokio::spawn(async move {
        let _ = rustliza_plugin_coding::run_coding_loop(
            runtime.as_ref(),
            &project_dir,
            &task,
            50,
            Some(tx),
        )
        .await;
    });

    let stream = ReceiverStream::new(rx).map(|event| {
        let (etype, data) = match &event {
            CodingEvent::Status(s) => ("status", s.clone()),
            CodingEvent::Thinking(s) => ("thinking", s.clone()),
            CodingEvent::Tool(s) => ("tool", s.clone()),
            CodingEvent::Output(s) => ("output", s.clone()),
            CodingEvent::Done(s) => ("done", s.clone()),
            CodingEvent::Error(s) => ("error", s.clone()),
            CodingEvent::Workspace { branch, dir } => (
                "workspace",
                serde_json::json!({"branch": branch, "dir": dir}).to_string(),
            ),
            CodingEvent::FileChanged { path, action } => (
                "file-changed",
                serde_json::json!({"path": path, "action": action}).to_string(),
            ),
            CodingEvent::Iteration(n) => ("iteration", n.to_string()),
        };
        Ok(Event::default().event(etype).data(data))
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

// ---------------------------------------------------------------------------
// Project directory management
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetProjectRequest {
    pub path: String,
}

#[derive(Serialize)]
pub struct ProjectResponse {
    pub path: Option<String>,
}

async fn set_project(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<SetProjectRequest>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let expanded = if req.path.starts_with('~') {
        if let Some(home) = std::env::var("HOME").ok() {
            PathBuf::from(req.path.replacen('~', &home, 1))
        } else {
            PathBuf::from(&req.path)
        }
    } else {
        PathBuf::from(&req.path)
    };

    let canonical = expanded.canonicalize().map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("invalid path: {}", e),
            }),
        )
    })?;

    if !canonical.is_dir() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "path is not a directory".into(),
            }),
        ));
    }

    let path_str = canonical.display().to_string();
    *state.project_dir.write().await = Some(canonical);

    Ok(Json(ProjectResponse {
        path: Some(path_str),
    }))
}

async fn get_project(
    AxumState(state): AxumState<ApiState>,
) -> impl IntoResponse {
    let dir = state.project_dir.read().await;
    Json(ProjectResponse {
        path: dir.as_ref().map(|p| p.display().to_string()),
    })
}

#[derive(Serialize)]
pub struct FileEntry {
    pub path: String,
    pub status: Option<String>,
}

async fn list_files(
    AxumState(state): AxumState<ApiState>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let dir = state.project_dir.read().await.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "no project set".into(),
            }),
        )
    })?;

    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg("find . -maxdepth 4 -type f -not -path '*/target/*' -not -path '*/.git/*' -not -path '*/node_modules/*' 2>/dev/null | sed 's|^\\./||' | sort | head -300")
        .current_dir(&dir)
        .output()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e.to_string() }),
            )
        })?;

    let status_out = tokio::process::Command::new("sh")
        .arg("-c")
        .arg("git status --porcelain 2>/dev/null")
        .current_dir(&dir)
        .output()
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .unwrap_or_default();

    let mut status_map = std::collections::HashMap::new();
    for line in status_out.lines() {
        if line.len() >= 4 {
            let st = line[..2].trim().to_string();
            let path = line[3..].to_string();
            let action = match st.as_str() {
                "M" | "MM" | " M" => "modified",
                "A" | "??" => "added",
                "D" => "deleted",
                _ => "changed",
            };
            status_map.insert(path, action.to_string());
        }
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let entries: Vec<FileEntry> = stdout
        .lines()
        .map(|p| FileEntry {
            path: p.to_string(),
            status: status_map.get(p).cloned(),
        })
        .collect();

    Ok(Json(entries))
}

#[derive(Deserialize)]
pub struct ReadFileQuery {
    pub path: String,
}

async fn read_file(
    AxumState(state): AxumState<ApiState>,
    axum::extract::Query(q): axum::extract::Query<ReadFileQuery>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let dir = state.project_dir.read().await.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "no project set".into(),
            }),
        )
    })?;

    let full = dir.join(&q.path);
    let canonical = full.canonicalize().map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;
    if !canonical.starts_with(&dir) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "outside project".into(),
            }),
        ));
    }
    let content = tokio::fs::read_to_string(&canonical).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;

    Ok(content)
}

async fn get_diff(
    AxumState(state): AxumState<ApiState>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let dir = state.project_dir.read().await.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "no project set".into(),
            }),
        )
    })?;
    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg("git diff HEAD 2>/dev/null; git ls-files --others --exclude-standard 2>/dev/null | xargs -I {} sh -c 'echo \"--- /dev/null\"; echo \"+++ b/{}\"; cat {} | head -200'")
        .current_dir(&dir)
        .output()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: e.to_string() }),
            )
        })?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

// ---------------------------------------------------------------------------
// Chat UI
// ---------------------------------------------------------------------------

async fn chat_ui() -> impl IntoResponse {
    axum::response::Html(CHAT_HTML)
}

const CHAT_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>botdick</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0a0f;--panel:#101018;--panel-2:#15151f;--border:#1f1f2e;
  --text:#e4e4e7;--dim:#71717a;--dim2:#a1a1aa;
  --accent:#ec4899;--accent-2:#a78bfa;
  --green:#10b981;--yellow:#eab308;--red:#ef4444;--blue:#3b82f6;
  --added:#10b98122;--modified:#eab30822;--deleted:#ef444422;
  --added-fg:#34d399;--modified-fg:#fbbf24;--deleted-fg:#f87171;
}
html,body{height:100%;overflow:hidden}
body{font-family:'SF Pro Text','Inter',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);font-size:13px;line-height:1.5}
.mono{font-family:'SF Mono','JetBrains Mono','Fira Code',Monaco,monospace}

/* Layout */
#app{display:grid;grid-template-rows:42px 1fr;height:100vh}
#topbar{display:flex;align-items:center;padding:0 14px;gap:14px;background:var(--panel);border-bottom:1px solid var(--border);font-size:12px}
#topbar .logo{font-weight:700;color:var(--accent);font-size:14px;letter-spacing:-0.02em}
#topbar .sep{width:1px;height:18px;background:var(--border)}
#topbar .pill{padding:3px 8px;border-radius:4px;background:var(--panel-2);color:var(--dim2);font-family:'SF Mono',monospace;font-size:11px;display:flex;align-items:center;gap:5px;cursor:pointer}
#topbar .pill:hover{color:var(--text);background:var(--border)}
#topbar .pill.active{background:#ec489922;color:var(--accent)}
#topbar .dot{width:6px;height:6px;border-radius:50%;background:var(--green)}
#topbar .spacer{flex:1}

#main{display:grid;grid-template-columns:240px 1fr 380px;height:calc(100vh - 42px);overflow:hidden}

/* Sidebar — file tree */
#sidebar{background:var(--panel);border-right:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column}
#sidebar .header{padding:10px 14px;font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:var(--dim);font-weight:600;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
#sidebar .header button{background:none;border:none;color:var(--dim);font-size:13px;cursor:pointer}
#sidebar .header button:hover{color:var(--text)}
#tree{flex:1;overflow-y:auto;padding:6px 0;font-family:'SF Mono',monospace;font-size:12px}
.tree-item{padding:2px 14px;cursor:pointer;color:var(--dim2);display:flex;align-items:center;gap:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tree-item:hover{background:var(--panel-2);color:var(--text)}
.tree-item.active{background:#ec489922;color:var(--accent)}
.tree-item .badge{margin-left:auto;font-size:9px;padding:1px 5px;border-radius:3px;font-weight:700}
.tree-item .badge.modified{background:var(--modified);color:var(--modified-fg)}
.tree-item .badge.added{background:var(--added);color:var(--added-fg)}
.tree-item .badge.deleted{background:var(--deleted);color:var(--deleted-fg)}

/* Center — activity feed */
#center{display:flex;flex-direction:column;background:var(--bg);overflow:hidden}
#feed{flex:1;overflow-y:auto;padding:18px 22px;display:flex;flex-direction:column;gap:10px}
#feed::-webkit-scrollbar,#tree::-webkit-scrollbar,#viewer::-webkit-scrollbar,#sidebar::-webkit-scrollbar{width:8px;height:8px}
#feed::-webkit-scrollbar-thumb,#tree::-webkit-scrollbar-thumb,#viewer::-webkit-scrollbar-thumb,#sidebar::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}

.entry{display:flex;gap:10px;align-items:flex-start;animation:slidein .18s ease}
@keyframes slidein{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.entry .icon{width:22px;height:22px;border-radius:5px;display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:11px;font-weight:700}
.entry .body{flex:1;min-width:0}
.entry .head{font-size:11px;color:var(--dim);margin-bottom:3px;display:flex;align-items:center;gap:6px;font-weight:600}
.entry .content{font-size:13px;color:var(--text);word-break:break-word}

.entry.user .icon{background:#ec489922;color:var(--accent)}
.entry.user .content{background:var(--panel);border:1px solid var(--border);padding:10px 12px;border-radius:8px}

.entry.agent .icon{background:#a78bfa22;color:var(--accent-2)}
.entry.agent .content{background:var(--panel);border:1px solid var(--border);padding:10px 12px;border-radius:8px;white-space:pre-wrap}

.entry.system .icon{background:var(--panel-2);color:var(--dim)}
.entry.system .content{font-size:12px;color:var(--dim2)}

.entry.tool .icon{background:#3b82f622;color:var(--blue)}
.entry.tool .content{padding:8px 12px;background:var(--panel);border:1px solid var(--border);border-radius:6px;font-family:'SF Mono',monospace;font-size:12px;color:var(--dim2)}
.entry.tool .content .name{color:var(--blue);font-weight:700;margin-right:8px}

.entry.output{margin-left:32px}
.entry.output .icon{display:none}
.entry.output .body{padding-left:0}
.entry.output .content{background:var(--panel-2);border:1px solid var(--border);border-radius:6px;font-family:'SF Mono',monospace;font-size:11.5px;color:var(--dim2);padding:8px 12px;white-space:pre;overflow-x:auto;max-height:300px;overflow-y:auto}

.entry.workspace .icon{background:#10b98122;color:var(--green)}
.entry.workspace .content{background:#10b98111;border:1px solid #10b98144;color:var(--text);padding:8px 12px;border-radius:6px;font-family:'SF Mono',monospace;font-size:12px}

.entry.file-changed .icon{display:none}
.entry.file-changed{margin-left:32px}
.entry.file-changed .content{font-size:12px;color:var(--dim2);font-family:'SF Mono',monospace}
.entry.file-changed .action{font-weight:700;margin-right:6px}
.entry.file-changed .action.modified{color:var(--modified-fg)}
.entry.file-changed .action.added{color:var(--added-fg)}
.entry.file-changed .action.deleted{color:var(--deleted-fg)}

.entry.done .icon{background:#10b98122;color:var(--green)}
.entry.done .content{background:#10b98111;border:1px solid #10b98144;padding:10px 12px;border-radius:6px;color:var(--text)}

.entry.error .icon{background:#ef444422;color:var(--red)}
.entry.error .content{background:#ef444411;border:1px solid #ef444444;padding:8px 12px;border-radius:6px;color:var(--red);font-family:'SF Mono',monospace;font-size:12px}

.iter-divider{display:flex;align-items:center;gap:10px;margin:6px 0;color:var(--dim);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.1em}
.iter-divider::before,.iter-divider::after{content:'';flex:1;height:1px;background:var(--border)}

#composer{padding:14px 22px;border-top:1px solid var(--border);background:var(--panel)}
#composer-row{display:flex;gap:10px;align-items:flex-end}
#input{flex:1;background:var(--panel-2);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:8px;font-size:13px;font-family:inherit;outline:none;resize:none;min-height:38px;max-height:150px;line-height:1.5}
#input:focus{border-color:var(--accent)}
#input::placeholder{color:var(--dim)}
#send{background:var(--accent);color:#fff;border:none;padding:10px 18px;border-radius:8px;font-size:13px;cursor:pointer;font-family:inherit;font-weight:600;height:38px}
#send:hover{opacity:.9}
#send:disabled{opacity:.4;cursor:default}
#hint{margin-top:6px;font-size:11px;color:var(--dim);font-family:'SF Mono',monospace}
.hint-cmd{color:var(--accent-2);font-weight:600}

/* Right panel — viewer */
#right{background:var(--panel);border-left:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
#right .tabs{display:flex;border-bottom:1px solid var(--border);background:var(--panel-2)}
#right .tab{padding:10px 14px;font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:var(--dim);font-weight:600;cursor:pointer;border-bottom:2px solid transparent}
#right .tab.active{color:var(--text);border-bottom-color:var(--accent)}
#right .tab:hover{color:var(--text)}
#viewer{flex:1;overflow:auto;padding:14px;font-family:'SF Mono',monospace;font-size:11.5px;line-height:1.55;color:var(--dim2);white-space:pre;tab-size:4}
#viewer .empty{color:var(--dim);font-style:italic;text-align:center;padding:40px 0;white-space:normal}
.diff-line.add{background:#10b98122;color:var(--added-fg)}
.diff-line.del{background:#ef444422;color:var(--deleted-fg)}
.diff-line.hunk{color:var(--blue);font-weight:600}
.diff-line.file{color:var(--accent);font-weight:700;margin-top:8px}
.line-num{display:inline-block;width:42px;color:var(--dim);text-align:right;padding-right:12px;user-select:none;opacity:.6}
</style>
</head>
<body>
<div id="app">
  <div id="topbar">
    <div class="logo">▮ botdick</div>
    <div class="sep"></div>
    <div class="pill" id="agent-status"><span class="dot"></span><span id="agent-name">connecting</span></div>
    <div class="pill" id="project-pill" title="click to change project">no project</div>
    <div class="pill" id="branch-pill" style="display:none">main</div>
    <div class="spacer"></div>
    <div class="pill" id="iter-pill" style="display:none">idle</div>
  </div>

  <div id="main">
    <div id="sidebar">
      <div class="header">
        Files
        <button id="refresh-tree" title="refresh">↻</button>
      </div>
      <div id="tree"><div style="padding:14px;color:var(--dim);font-size:12px">no project set</div></div>
    </div>

    <div id="center">
      <div id="feed"></div>
      <div id="composer">
        <div id="composer-row">
          <textarea id="input" rows="1" placeholder="ask anything · /code <task> · /project <path>" disabled></textarea>
          <button id="send" disabled>send</button>
        </div>
        <div id="hint"><span class="hint-cmd">/code</span> &lt;task&gt; — autonomous build · <span class="hint-cmd">/project</span> &lt;path&gt; — set workspace · <span class="hint-cmd">/diff</span> — show changes</div>
      </div>
    </div>

    <div id="right">
      <div class="tabs">
        <div class="tab active" data-view="diff">Diff</div>
        <div class="tab" data-view="file">File</div>
      </div>
      <div id="viewer"><div class="empty">select a file or run /diff</div></div>
    </div>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);

let roomId = null, entityId = null, agentName = 'bot', streaming = false;
let currentView = 'diff', currentFile = null;

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random()*16|0;
    return (c==='x'?r:(r&0x3|0x8)).toString(16);
  });
}

function el(tag, attrs={}, ...children) {
  const e = document.createElement(tag);
  for (const [k,v] of Object.entries(attrs)) {
    if (k === 'class') e.className = v;
    else if (k === 'style') e.style.cssText = v;
    else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of children) {
    if (c == null) continue;
    e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return e;
}

const feed = () => $('#feed');
const scrollFeed = () => { const f = feed(); f.scrollTop = f.scrollHeight; };

function addEntry(type, body, opts={}) {
  const icons = {user:'You', agent:'A', system:'·', tool:'⚙', output:'', workspace:'⎇', done:'✓', error:'!', 'file-changed':''};
  const head = opts.head || '';
  const e = el('div', {class:'entry '+type});
  if (icons[type]) e.appendChild(el('div', {class:'icon'}, icons[type]));
  else e.appendChild(el('div', {class:'icon'}));
  const inner = el('div', {class:'body'});
  if (head) inner.appendChild(el('div', {class:'head'}, head));
  inner.appendChild(typeof body === 'string' ? el('div', {class:'content'}, body) : body);
  e.appendChild(inner);
  feed().appendChild(e);
  scrollFeed();
  return e;
}

function addIterDivider(n) {
  const d = el('div', {class:'iter-divider'}, 'iteration ' + n);
  feed().appendChild(d);
  scrollFeed();
}

function addToolEntry(toolText) {
  const colon = toolText.indexOf(':');
  const name = colon > 0 ? toolText.slice(0, colon) : toolText;
  const arg = colon > 0 ? toolText.slice(colon+1).trim() : '';
  const content = el('div', {class:'content'},
    el('span', {class:'name'}, name),
    el('span', {}, arg)
  );
  return addEntry('tool', content);
}

function addFileChanged(path, action) {
  const content = el('div', {class:'content'},
    el('span', {class:'action '+action}, action),
    el('span', {}, path)
  );
  return addEntry('file-changed', content);
}

async function init() {
  try {
    const agent = await (await fetch('/agent')).json();
    agentName = agent.name;
    $('#agent-name').textContent = agent.name;
    entityId = uuid();
    const room = await (await fetch('/rooms', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name:'web-' + Date.now(), source:'web'})
    })).json();
    roomId = room.id;

    // Check if project is already set
    const proj = await (await fetch('/project')).json();
    if (proj.path) {
      $('#project-pill').textContent = proj.path.split('/').pop();
      $('#project-pill').title = proj.path;
      await refreshTree();
    }

    $('#input').disabled = false;
    $('#send').disabled = false;
    $('#input').focus();

    addEntry('system', `connected as ${agent.name}. ${proj.path ? 'project: ' + proj.path : 'set a project with /project <path>'}`);
  } catch(e) {
    $('#agent-name').textContent = 'offline';
    $('#agent-status .dot').style.background = 'var(--red)';
    addEntry('error', 'failed to connect: ' + e.message);
  }
}

async function refreshTree() {
  try {
    const files = await (await fetch('/files')).json();
    const tree = $('#tree');
    tree.innerHTML = '';
    if (!files.length) {
      tree.appendChild(el('div', {style:'padding:14px;color:var(--dim);font-size:12px'}, 'empty'));
      return;
    }
    for (const f of files) {
      const item = el('div', {class:'tree-item', title:f.path, onclick:() => openFile(f.path)},
        el('span', {}, f.path)
      );
      if (f.status) {
        item.appendChild(el('span', {class:'badge '+f.status}, f.status[0].toUpperCase()));
      }
      tree.appendChild(item);
    }
  } catch(e) { /* ignore */ }
}

async function openFile(path) {
  currentFile = path;
  switchView('file');
  $$('.tree-item').forEach(i => i.classList.toggle('active', i.title === path));
  try {
    const r = await fetch('/file?path=' + encodeURIComponent(path));
    if (!r.ok) {
      $('#viewer').innerHTML = '';
      $('#viewer').appendChild(el('div', {class:'empty'}, 'cannot read: ' + r.statusText));
      return;
    }
    const text = await r.text();
    renderFile(text);
  } catch(e) {
    $('#viewer').textContent = 'error: ' + e.message;
  }
}

function renderFile(text) {
  const v = $('#viewer');
  v.innerHTML = '';
  const lines = text.split('\n');
  const frag = document.createDocumentFragment();
  lines.forEach((line, i) => {
    const ln = el('div', {},
      el('span', {class:'line-num'}, String(i+1)),
      document.createTextNode(line)
    );
    frag.appendChild(ln);
  });
  v.appendChild(frag);
}

async function showDiff() {
  switchView('diff');
  try {
    const r = await fetch('/diff');
    const text = await r.text();
    const v = $('#viewer');
    v.innerHTML = '';
    if (!text.trim()) {
      v.appendChild(el('div', {class:'empty'}, 'no changes'));
      return;
    }
    const frag = document.createDocumentFragment();
    text.split('\n').forEach(line => {
      let cls = '';
      if (line.startsWith('+++') || line.startsWith('---') || line.startsWith('diff ')) cls = 'file';
      else if (line.startsWith('@@')) cls = 'hunk';
      else if (line.startsWith('+')) cls = 'add';
      else if (line.startsWith('-')) cls = 'del';
      frag.appendChild(el('div', {class:'diff-line '+cls}, line));
    });
    v.appendChild(frag);
  } catch(e) {
    $('#viewer').textContent = 'error: ' + e.message;
  }
}

function switchView(view) {
  currentView = view;
  $$('#right .tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
}

function parseSSE(buf, handler) {
  const lines = buf.split('\n');
  const rest = lines.pop();
  let etype = '', dataLines = [];
  for (const line of lines) {
    if (line.startsWith('event: ')) {
      if (dataLines.length && etype) handler(etype, dataLines.join('\n'));
      etype = line.slice(7).trim();
      dataLines = [];
    } else if (line.startsWith('data: ')) {
      dataLines.push(line.slice(6));
    } else if (line.trim() === '' && dataLines.length) {
      handler(etype, dataLines.join('\n'));
      etype = '';
      dataLines = [];
    }
  }
  if (dataLines.length && etype) handler(etype, dataLines.join('\n'));
  return rest;
}

async function sendChat(text) {
  const e = addEntry('agent', '...', {head:agentName});
  const span = e.querySelector('.content');
  span.textContent = '';
  let full = '';
  try {
    const res = await fetch('/message/stream', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({text, room_id: roomId, entity_id: entityId})
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += decoder.decode(value, {stream:true});
      buf = parseSSE(buf, (etype, data) => {
        if (etype === 'token') { full += data; span.textContent = full; scrollFeed(); }
        else if (etype === 'done') span.textContent = data || full || '[no response]';
        else if (etype === 'error') span.textContent = full + '\n[error: ' + data + ']';
      });
    }
  } catch(err) { span.textContent = full + '\n[error: ' + err.message + ']'; }
  if (!span.textContent) span.textContent = '[no response]';
}

async function sendTask(task) {
  addEntry('system', 'starting: ' + task);
  $('#iter-pill').style.display = 'inline-flex';
  $('#iter-pill').textContent = 'starting...';
  let lastIter = 0;
  try {
    const res = await fetch('/task', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({task})
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({error:res.statusText}));
      addEntry('error', err.error || res.statusText);
      $('#iter-pill').style.display = 'none';
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += decoder.decode(value, {stream:true});
      buf = parseSSE(buf, (etype, data) => {
        if (etype === 'status') { $('#iter-pill').textContent = data; }
        else if (etype === 'iteration') { lastIter = parseInt(data); $('#iter-pill').textContent = 'iter ' + data; addIterDivider(data); }
        else if (etype === 'workspace') {
          const ws = JSON.parse(data);
          $('#branch-pill').style.display = 'inline-flex';
          $('#branch-pill').textContent = '⎇ ' + ws.branch;
          addEntry('workspace', 'branch: ' + ws.branch + ' · dir: ' + ws.dir);
        }
        else if (etype === 'thinking') {
          if (data.trim()) addEntry('agent', data, {head: agentName});
        }
        else if (etype === 'tool') addToolEntry(data);
        else if (etype === 'output') {
          addEntry('output', el('div', {class:'content'}, data));
        }
        else if (etype === 'file-changed') {
          const fc = JSON.parse(data);
          addFileChanged(fc.path, fc.action);
          refreshTree();
        }
        else if (etype === 'done') {
          addEntry('done', data || 'task complete');
          $('#iter-pill').textContent = 'done';
          setTimeout(() => $('#iter-pill').style.display = 'none', 3000);
          refreshTree();
          if (currentView === 'diff') showDiff();
        }
        else if (etype === 'error') addEntry('error', data);
      });
    }
  } catch(e) {
    addEntry('error', 'stream error: ' + e.message);
    $('#iter-pill').style.display = 'none';
  }
  refreshTree();
}

async function setProject(path) {
  try {
    const r = await fetch('/project', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({path})
    });
    const j = await r.json();
    if (r.ok) {
      addEntry('system', 'project set: ' + j.path);
      $('#project-pill').textContent = j.path.split('/').pop();
      $('#project-pill').title = j.path;
      await refreshTree();
    } else {
      addEntry('error', j.error || 'failed');
    }
  } catch(e) { addEntry('error', e.message); }
}

async function send() {
  const text = $('#input').value.trim();
  if (!text || streaming) return;
  $('#input').value = '';
  autoResize();
  addEntry('user', text, {head:'you'});
  streaming = true;
  $('#send').disabled = true;

  if (text.startsWith('/code ')) await sendTask(text.slice(6));
  else if (text.startsWith('/project ')) await setProject(text.slice(9).trim());
  else if (text === '/project') {
    const r = await fetch('/project');
    const j = await r.json();
    addEntry('system', j.path ? 'project: ' + j.path : 'no project set');
  }
  else if (text === '/diff') showDiff();
  else if (text === '/refresh') await refreshTree();
  else await sendChat(text);

  streaming = false;
  $('#send').disabled = false;
  $('#input').focus();
}

function autoResize() {
  const el = $('#input');
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 150) + 'px';
}

$('#send').addEventListener('click', send);
$('#input').addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
});
$('#input').addEventListener('input', autoResize);
$('#refresh-tree').addEventListener('click', refreshTree);
$$('#right .tab').forEach(tab => tab.addEventListener('click', () => {
  if (tab.dataset.view === 'diff') showDiff();
  else if (currentFile) openFile(currentFile);
  else { switchView('file'); $('#viewer').innerHTML = ''; $('#viewer').appendChild(el('div', {class:'empty'}, 'select a file from the sidebar')); }
}));
$('#project-pill').addEventListener('click', async () => {
  const path = prompt('project path:', $('#project-pill').title || '~/');
  if (path) await setProject(path);
});

init();
</script>
</body>
</html>"##;

// ---------------------------------------------------------------------------
// Router construction
// ---------------------------------------------------------------------------

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/", get(chat_ui))
        .route("/health", get(health))
        .route("/agent", get(get_agent))
        .route("/metrics", get(get_metrics))
        .route("/message", post(send_message))
        .route("/message/stream", post(send_message_stream))
        .route("/webhook", post(webhook))
        .route("/task", post(run_task))
        .route("/project", get(get_project).post(set_project))
        .route("/files", get(list_files))
        .route("/file", get(read_file))
        .route("/diff", get(get_diff))
        .route("/rooms", post(create_room))
        .route("/rooms/{room_id}", get(get_room))
        .route("/rooms/{room_id}/memories", get(get_memories))
        .merge(vamp::router())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Server startup
// ---------------------------------------------------------------------------

pub async fn start_server(runtime: Arc<dyn Runtime>, bind: &str) -> Result<()> {
    start_server_full(runtime, bind, None, None).await
}

pub async fn start_server_with_options(
    runtime: Arc<dyn Runtime>,
    bind: &str,
    project_dir: Option<PathBuf>,
) -> Result<()> {
    start_server_full(runtime, bind, project_dir, None).await
}

pub async fn start_server_full(
    runtime: Arc<dyn Runtime>,
    bind: &str,
    project_dir: Option<PathBuf>,
    vamp: Option<Arc<VampState>>,
) -> Result<()> {
    let state = ApiState {
        runtime,
        metrics: None,
        project_dir: Arc::new(RwLock::new(project_dir)),
        vamp,
    };
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
