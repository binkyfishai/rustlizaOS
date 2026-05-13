use std::path::PathBuf;
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
use tokio_stream::wrappers::ReceiverStream;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::Runtime;
use rustliza_core::types::*;
use rustliza_plugin_coding::CodingEvent;

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ApiState {
    pub runtime: Arc<dyn Runtime>,
    pub metrics: Option<Arc<rustliza_core::PipelineMetrics>>,
    pub project_dir: Option<PathBuf>,
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
    let project_dir = state.project_dir.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "no project directory configured — start with --project-dir".into(),
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
        };
        Ok(Event::default().event(etype).data(data))
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
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
<title>rustliza</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#1a1a2e;--surface:#16213e;--input-bg:#0f3460;--accent:#e94560;--text:#eee;--dim:#888;--user-bg:#0f3460;--bot-bg:#1a1a2e;--border:#2a2a4a;--tool:#264653;--tool-border:#2a9d8f}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,monospace;background:var(--bg);color:var(--text);height:100vh;display:flex;flex-direction:column}
header{padding:12px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px;background:var(--surface)}
header .dot{width:10px;height:10px;border-radius:50%;background:#4ade80;flex-shrink:0}
header h1{font-size:16px;font-weight:600}
header .bio{font-size:12px;color:var(--dim);margin-left:auto;max-width:50%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#messages{flex:1;overflow-y:auto;padding:16px 20px;display:flex;flex-direction:column;gap:8px}
.msg{max-width:80%;padding:10px 14px;border-radius:12px;font-size:14px;line-height:1.5;word-wrap:break-word;white-space:pre-wrap}
.msg.user{align-self:flex-end;background:var(--user-bg);border:1px solid var(--border);border-bottom-right-radius:4px}
.msg.bot{align-self:flex-start;background:var(--bot-bg);border:1px solid var(--border);border-bottom-left-radius:4px}
.msg.bot .name{font-size:11px;color:var(--accent);margin-bottom:4px;font-weight:600}
.msg.tool{align-self:flex-start;background:var(--tool);border:1px solid var(--tool-border);font-size:12px;max-width:90%;border-radius:6px}
.msg.tool .label{font-size:10px;color:var(--tool-border);font-weight:700;text-transform:uppercase;margin-bottom:2px}
.msg.system{align-self:center;color:var(--dim);font-size:12px;background:none;padding:4px}
@keyframes pulse{0%,100%{opacity:.3}50%{opacity:1}}
#input-area{padding:12px 20px;border-top:1px solid var(--border);background:var(--surface);display:flex;gap:8px}
#input{flex:1;background:var(--input-bg);border:1px solid var(--border);color:var(--text);padding:10px 14px;border-radius:8px;font-size:14px;font-family:inherit;outline:none;resize:none;max-height:120px}
#input:focus{border-color:var(--accent)}
#input::placeholder{color:var(--dim)}
#send{background:var(--accent);color:#fff;border:none;padding:10px 20px;border-radius:8px;font-size:14px;cursor:pointer;font-family:inherit;font-weight:600;transition:opacity .15s}
#send:hover{opacity:.85}
#send:disabled{opacity:.4;cursor:default}
</style>
</head>
<body>
<header>
  <div class="dot" id="status-dot"></div>
  <h1 id="agent-name">connecting...</h1>
  <div class="bio" id="agent-bio"></div>
</header>
<div id="messages"></div>
<div id="input-area">
  <textarea id="input" rows="1" placeholder="chat or /code &lt;task&gt; for autonomous coding..." disabled></textarea>
  <button id="send" disabled>send</button>
</div>
<script>
const $ = s => document.querySelector(s);
const msgs = $('#messages');
let roomId = null, entityId = null, agentName = 'bot', streaming = false;

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random()*16|0;
    return (c==='x'?r:(r&0x3|0x8)).toString(16);
  });
}

function addMsg(text, cls, name) {
  const d = document.createElement('div');
  d.className = 'msg ' + cls;
  if (name) {
    const n = document.createElement('div');
    n.className = 'name';
    n.textContent = name;
    d.appendChild(n);
  }
  const t = document.createElement('span');
  t.textContent = text;
  d.appendChild(t);
  msgs.appendChild(d);
  msgs.scrollTop = msgs.scrollHeight;
  return t;
}

function addTool(label, text) {
  const d = document.createElement('div');
  d.className = 'msg tool';
  const l = document.createElement('div');
  l.className = 'label';
  l.textContent = label;
  d.appendChild(l);
  const t = document.createElement('span');
  t.textContent = text.length > 500 ? text.slice(0, 500) + '...' : text;
  d.appendChild(t);
  msgs.appendChild(d);
  msgs.scrollTop = msgs.scrollHeight;
}

function addSystem(text) { addMsg(text, 'system'); }

async function init() {
  try {
    const agent = await (await fetch('/agent')).json();
    agentName = agent.name;
    $('#agent-name').textContent = agent.name;
    $('#agent-bio').textContent = agent.bio;
    entityId = uuid();
    const room = await (await fetch('/rooms', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({name:'chat-' + Date.now(), source:'web'})
    })).json();
    roomId = room.id;
    $('#input').disabled = false;
    $('#send').disabled = false;
    $('#input').focus();
    addSystem('connected. type to chat, or /code <task> for autonomous coding.');
  } catch(e) {
    $('#agent-name').textContent = 'offline';
    $('#status-dot').style.background = '#ef4444';
    addSystem('failed to connect: ' + e.message);
  }
}

function parseSSE(buf, handler) {
  const lines = buf.split('\n');
  const rest = lines.pop();
  let etype = '';
  for (const line of lines) {
    if (line.startsWith('event: ')) etype = line.slice(7).trim();
    else if (line.startsWith('data: ')) handler(etype, line.slice(6));
  }
  return rest;
}

async function sendChat(text) {
  const span = addMsg('', 'bot', agentName);
  let full = '';
  try {
    const res = await fetch('/message/stream', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({text, room_id: roomId, entity_id: entityId})
    });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += decoder.decode(value, {stream: true});
      buf = parseSSE(buf, (etype, data) => {
        if (etype === 'token') { full += data; span.textContent = full; msgs.scrollTop = msgs.scrollHeight; }
        else if (etype === 'done') span.textContent = data || full;
        else if (etype === 'error') span.textContent = full + '\n[error: ' + data + ']';
      });
    }
  } catch(e) { span.textContent = full + '\n[stream error: ' + e.message + ']'; }
  if (!span.textContent) span.textContent = '[no response]';
}

async function sendTask(task) {
  addSystem('starting coding task...');
  let thinkSpan = null;
  try {
    const res = await fetch('/task', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({task})
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({error: res.statusText}));
      addSystem('error: ' + (err.error || res.statusText));
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += decoder.decode(value, {stream: true});
      buf = parseSSE(buf, (etype, data) => {
        if (etype === 'status') addSystem(data);
        else if (etype === 'thinking') { thinkSpan = addMsg('', 'bot', agentName + ' [thinking]'); thinkSpan.textContent = data.length > 1000 ? data.slice(0,1000)+'...' : data; }
        else if (etype === 'tool') addTool('tool', data);
        else if (etype === 'output') addTool('output', data);
        else if (etype === 'done') { addMsg(data, 'bot', agentName); addSystem('task complete.'); }
        else if (etype === 'error') addSystem('error: ' + data);
      });
    }
  } catch(e) { addSystem('task stream error: ' + e.message); }
}

async function send() {
  const text = $('#input').value.trim();
  if (!text || streaming) return;
  $('#input').value = '';
  autoResize();
  addMsg(text, 'user');
  streaming = true;
  $('#send').disabled = true;

  if (text.startsWith('/code ')) {
    await sendTask(text.slice(6));
  } else {
    await sendChat(text);
  }

  streaming = false;
  $('#send').disabled = false;
  $('#input').focus();
}

function autoResize() {
  const el = $('#input');
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

$('#send').addEventListener('click', send);
$('#input').addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
});
$('#input').addEventListener('input', autoResize);
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
    start_server_with_options(runtime, bind, None).await
}

pub async fn start_server_with_options(
    runtime: Arc<dyn Runtime>,
    bind: &str,
    project_dir: Option<PathBuf>,
) -> Result<()> {
    let state = ApiState {
        runtime,
        metrics: None,
        project_dir,
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
