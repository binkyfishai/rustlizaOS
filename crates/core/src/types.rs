use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub type AgentId = Uuid;
pub type EntityId = Uuid;
pub type RoomId = Uuid;
pub type WorldId = Uuid;
pub type MemoryId = Uuid;

// ---------------------------------------------------------------------------
// Channel & content types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChannelType {
    #[serde(rename = "SELF")]
    Itself,
    #[serde(rename = "DM")]
    Dm,
    #[serde(rename = "GROUP")]
    Group,
    #[serde(rename = "VOICE_DM")]
    VoiceDm,
    #[serde(rename = "VOICE_GROUP")]
    VoiceGroup,
    #[serde(rename = "FEED")]
    Feed,
    #[serde(rename = "THREAD")]
    Thread,
    #[serde(rename = "WORLD")]
    World,
    #[serde(rename = "FORUM")]
    Forum,
    #[serde(rename = "API")]
    Api,
}

impl Default for ChannelType {
    fn default() -> Self {
        Self::Dm
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Media {
    pub id: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

// ---------------------------------------------------------------------------
// Content — the payload of every message / memory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Content {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thought: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachments: Option<Vec<Media>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_type: Option<ChannelType>,

    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Content {
    pub fn text(s: impl Into<String>) -> Self {
        Self {
            text: Some(s.into()),
            ..Default::default()
        }
    }

    pub fn text_with_action(s: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            text: Some(s.into()),
            actions: Some(vec![action.into()]),
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Message,
    Document,
    Fragment,
    Description,
    Custom,
}

impl Default for MemoryType {
    fn default() -> Self {
        Self::Message
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    pub id: MemoryId,
    pub content: Content,
    pub entity_id: EntityId,
    pub agent_id: AgentId,
    pub room_id: RoomId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_id: Option<WorldId>,

    #[serde(default)]
    pub unique: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,

    #[serde(default, rename = "type")]
    pub memory_type: MemoryType,
}

impl Memory {
    pub fn new_message(
        agent_id: AgentId,
        entity_id: EntityId,
        room_id: RoomId,
        content: Content,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            content,
            entity_id,
            agent_id,
            room_id,
            world_id: None,
            unique: true,
            created_at: Some(Utc::now()),
            embedding: None,
            metadata: None,
            memory_type: MemoryType::Message,
        }
    }
}

// ---------------------------------------------------------------------------
// Entity (replaces old "Actor" concept)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub agent_id: AgentId,
    pub names: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

impl Entity {
    pub fn display_name(&self) -> &str {
        self.names.first().map(|s| s.as_str()).unwrap_or("Unknown")
    }
}

// ---------------------------------------------------------------------------
// Room
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Room {
    pub id: RoomId,
    pub agent_id: AgentId,
    pub source: String,
    #[serde(default)]
    pub channel_type: ChannelType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_id: Option<WorldId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// World
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct World {
    pub id: WorldId,
    pub agent_id: AgentId,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Relationship
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    pub id: Uuid,
    pub source_entity_id: EntityId,
    pub target_entity_id: EntityId,
    pub agent_id: AgentId,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Component (ECS-style)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Component {
    pub id: Uuid,
    pub entity_id: EntityId,
    pub agent_id: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_id: Option<RoomId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_id: Option<WorldId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_entity_id: Option<EntityId>,
    pub component_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Participant
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Participant {
    pub id: Uuid,
    pub entity_id: EntityId,
    pub room_id: RoomId,
    pub agent_id: AgentId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Failed,
    Cancelled,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room_id: Option<RoomId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_id: Option<WorldId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<EntityId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// State — assembled context for prompt generation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub values: HashMap<String, String>,
    pub data: StateData,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StateData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<Room>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world: Option<World>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<Entity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_messages: Option<Vec<Memory>>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Action-related types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionExample {
    pub user: String,
    pub content: Content,
}

#[derive(Debug, Clone)]
pub struct ActionResult {
    pub success: bool,
    pub text: Option<String>,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
    pub continue_chain: bool,
}

impl ActionResult {
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            success: true,
            text: Some(text.into()),
            data: None,
            error: None,
            continue_chain: false,
        }
    }

    pub fn err(error: impl Into<String>) -> Self {
        Self {
            success: false,
            text: None,
            data: None,
            error: Some(error.into()),
            continue_chain: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluator example
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluatorExample {
    pub prompt: String,
    pub messages: Vec<ActionExample>,
    pub outcome: String,
}

// ---------------------------------------------------------------------------
// Model types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModelType {
    TextSmall,
    TextLarge,
    TextEmbedding,
    ImageDescription,
}

#[derive(Debug, Clone)]
pub struct GenerateTextParams {
    pub model_type: ModelType,
    pub system_prompt: String,
    pub prompt: String,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub stop_sequences: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Token(String),
    Done { full_text: String },
    Error(String),
}

pub type TextStream = tokio::sync::mpsc::Receiver<StreamEvent>;

// ---------------------------------------------------------------------------
// Query parameter types for database operations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct GetMemoriesParams {
    pub room_id: Option<RoomId>,
    pub agent_id: Option<AgentId>,
    pub entity_id: Option<EntityId>,
    pub memory_type: Option<MemoryType>,
    pub unique: Option<bool>,
    pub count: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct SearchMemoriesParams {
    pub embedding: Vec<f32>,
    pub room_id: Option<RoomId>,
    pub agent_id: AgentId,
    pub memory_type: Option<MemoryType>,
    pub match_threshold: f32,
    pub match_count: usize,
    pub unique: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct GetParticipantsParams {
    pub room_id: RoomId,
}
