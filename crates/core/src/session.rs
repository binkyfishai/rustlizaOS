use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{AgentId, EntityId, RoomId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: Uuid,
    pub agent_id: AgentId,
    pub entity_id: EntityId,
    pub room_id: RoomId,
    pub session_key: String,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl Session {
    pub fn new(
        agent_id: AgentId,
        entity_id: EntityId,
        room_id: RoomId,
        source: &str,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            agent_id,
            entity_id,
            room_id,
            session_key: format!("agent:{}:{}:{}", agent_id, source, entity_id),
            started_at: Utc::now(),
            ended_at: None,
            summary: None,
            metadata: None,
        }
    }

    pub fn is_active(&self) -> bool {
        self.ended_at.is_none()
    }

    pub fn end(&mut self) {
        self.ended_at = Some(Utc::now());
    }

    pub fn duration_secs(&self) -> i64 {
        let end = self.ended_at.unwrap_or_else(Utc::now);
        (end - self.started_at).num_seconds()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub summary: String,
    pub topics: Vec<String>,
    pub message_count: usize,
    pub created_at: DateTime<Utc>,
}

impl SessionSummary {
    pub fn new(session_id: Uuid, summary: String, topics: Vec<String>, message_count: usize) -> Self {
        Self {
            session_id,
            summary,
            topics,
            message_count,
            created_at: Utc::now(),
        }
    }
}
