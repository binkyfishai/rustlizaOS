use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Action — executable behaviour the agent can perform
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Action: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn similes(&self) -> Vec<String> {
        vec![]
    }
    fn examples(&self) -> Vec<Vec<ActionExample>> {
        vec![]
    }
    fn priority(&self) -> i32 {
        0
    }
    async fn validate(&self, runtime: &dyn Runtime, message: &Memory, state: &State)
        -> Result<bool>;
    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<ActionResult>;
}

// ---------------------------------------------------------------------------
// Evaluator — analyses conversations and triggers side-effects
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Evaluator: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn always_run(&self) -> bool {
        false
    }
    fn similes(&self) -> Vec<String> {
        vec![]
    }
    fn examples(&self) -> Vec<EvaluatorExample> {
        vec![]
    }
    async fn validate(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<bool>;
    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Provider — injects context into prompt state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ProviderResult {
    pub text: Option<String>,
    pub values: std::collections::HashMap<String, String>,
    pub data: Option<serde_json::Value>,
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str {
        ""
    }
    fn position(&self) -> i32 {
        0
    }
    fn is_private(&self) -> bool {
        false
    }
    async fn get(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<ProviderResult>;
}

// ---------------------------------------------------------------------------
// Service — long-running client (Discord, Telegram, etc.)
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Service: Send + Sync {
    fn name(&self) -> &str;
    async fn start(&self, runtime: Arc<dyn Runtime>) -> Result<()>;
    async fn stop(&self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// DatabaseAdapter — persistence layer
// ---------------------------------------------------------------------------

#[async_trait]
pub trait DatabaseAdapter: Send + Sync {
    async fn init(&self) -> Result<()>;

    // Entity operations
    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>>;
    async fn create_entity(&self, entity: &Entity) -> Result<()>;
    async fn update_entity(&self, entity: &Entity) -> Result<()>;

    // Memory operations
    async fn create_memory(&self, memory: &Memory) -> Result<()>;
    async fn get_memories(&self, params: &GetMemoriesParams) -> Result<Vec<Memory>>;
    async fn get_memory_by_id(&self, id: MemoryId) -> Result<Option<Memory>>;
    async fn search_memories(&self, params: &SearchMemoriesParams) -> Result<Vec<Memory>>;
    async fn delete_memory(&self, id: MemoryId) -> Result<()>;
    async fn count_memories(
        &self,
        room_id: RoomId,
        unique: bool,
        memory_type: MemoryType,
    ) -> Result<usize>;

    // Room operations
    async fn get_room(&self, id: RoomId) -> Result<Option<Room>>;
    async fn create_room(&self, room: &Room) -> Result<RoomId>;
    async fn get_rooms_for_participant(&self, entity_id: EntityId) -> Result<Vec<RoomId>>;
    async fn get_participants_for_room(&self, room_id: RoomId) -> Result<Vec<EntityId>>;
    async fn add_participant(&self, entity_id: EntityId, room_id: RoomId, agent_id: AgentId) -> Result<()>;
    async fn remove_participant(&self, entity_id: EntityId, room_id: RoomId) -> Result<()>;

    // Relationship operations
    async fn get_relationship(
        &self,
        source: EntityId,
        target: EntityId,
        agent_id: AgentId,
    ) -> Result<Option<Relationship>>;
    async fn get_relationships(&self, entity_id: EntityId) -> Result<Vec<Relationship>>;
    async fn create_relationship(&self, relationship: &Relationship) -> Result<()>;

    // Component operations
    async fn get_components(
        &self,
        entity_id: EntityId,
        component_type: Option<&str>,
    ) -> Result<Vec<Component>>;
    async fn create_component(&self, component: &Component) -> Result<()>;
    async fn update_component(&self, component: &Component) -> Result<()>;
    async fn delete_component(&self, id: uuid::Uuid) -> Result<()>;

    // World operations
    async fn get_world(&self, id: WorldId) -> Result<Option<World>>;
    async fn create_world(&self, world: &World) -> Result<WorldId>;

    // Cache
    async fn get_cache(&self, key: &str, agent_id: AgentId) -> Result<Option<serde_json::Value>>;
    async fn set_cache(
        &self,
        key: &str,
        agent_id: AgentId,
        value: &serde_json::Value,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<()>;
    async fn delete_cache(&self, key: &str, agent_id: AgentId) -> Result<()>;
}

// ---------------------------------------------------------------------------
// ModelProvider — LLM / embedding backend
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ModelProvider: Send + Sync {
    async fn generate_text(&self, params: &GenerateTextParams) -> Result<String>;

    async fn generate_text_stream(&self, params: &GenerateTextParams) -> Result<TextStream> {
        let text = self.generate_text(params).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let _ = tx.send(StreamEvent::Token(text.clone())).await;
        let _ = tx.send(StreamEvent::Done { full_text: text }).await;
        Ok(rx)
    }

    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>>;
    fn embedding_dimensions(&self) -> usize;
    fn supports_streaming(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Plugin — bundle of actions, evaluators, providers, services
// ---------------------------------------------------------------------------

pub struct Plugin {
    pub name: String,
    pub description: String,
    pub actions: Vec<Arc<dyn Action>>,
    pub evaluators: Vec<Arc<dyn Evaluator>>,
    pub providers: Vec<Arc<dyn Provider>>,
    pub services: Vec<Arc<dyn Service>>,
}

impl Plugin {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            actions: vec![],
            evaluators: vec![],
            providers: vec![],
            services: vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime — the core trait that actions/evaluators/providers see
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Runtime: Send + Sync {
    fn agent_id(&self) -> AgentId;
    fn character(&self) -> &crate::character::Character;
    fn database(&self) -> &dyn DatabaseAdapter;
    fn model_provider(&self) -> &dyn ModelProvider;

    async fn compose_state(&self, message: &Memory) -> Result<State>;
    async fn generate_text(&self, params: &GenerateTextParams) -> Result<String>;
    async fn generate_text_stream(&self, params: &GenerateTextParams) -> Result<TextStream>;
    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>>;
    async fn process_message(&self, message: &Memory) -> Result<Vec<Memory>>;
    async fn process_message_stream(
        &self,
        message: &Memory,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = StreamEvent> + Send>>>;

    fn actions(&self) -> &[Arc<dyn Action>];
    fn evaluators(&self) -> &[Arc<dyn Evaluator>];
    fn providers(&self) -> &[Arc<dyn Provider>];
}
