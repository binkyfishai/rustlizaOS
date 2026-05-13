use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tracing::debug;
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::{Action, Evaluator, Provider, ProviderResult, Runtime};
use rustliza_core::types::*;
use rustliza_core::Plugin;

// ---------------------------------------------------------------------------
// Bootstrap plugin — bundles all built-in actions, evaluators, providers
// ---------------------------------------------------------------------------

pub fn bootstrap_plugin() -> Plugin {
    let mut plugin = Plugin::new("bootstrap", "Core actions, evaluators, and providers");

    // Actions
    plugin.actions.push(Arc::new(ContinueAction));
    plugin.actions.push(Arc::new(IgnoreAction));
    plugin.actions.push(Arc::new(FollowRoomAction));
    plugin.actions.push(Arc::new(UnfollowRoomAction));
    plugin.actions.push(Arc::new(MuteRoomAction));
    plugin.actions.push(Arc::new(UnmuteRoomAction));
    plugin.actions.push(Arc::new(NoneAction));

    // Evaluators
    plugin.evaluators.push(Arc::new(FactExtractionEvaluator));
    plugin.evaluators.push(Arc::new(ReflectionEvaluator));

    // Providers
    plugin.providers.push(Arc::new(TimeProvider));
    plugin.providers.push(Arc::new(FactsProvider));
    plugin.providers.push(Arc::new(RelationshipsProvider));
    plugin.providers.push(Arc::new(BoredomProvider));

    plugin
}

// ===========================================================================
// Actions
// ===========================================================================

// -- CONTINUE -----------------------------------------------------------------

struct ContinueAction;

#[async_trait]
impl Action for ContinueAction {
    fn name(&self) -> &str {
        "CONTINUE"
    }
    fn description(&self) -> &str {
        "Continue the conversation naturally, responding to the most recent message"
    }
    fn similes(&self) -> Vec<String> {
        vec!["ELABORATE".into(), "GO_ON".into(), "KEEP_TALKING".into()]
    }
    fn priority(&self) -> i32 {
        -1
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, _state: &State) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<ActionResult> {
        let prompt = format!(
            "Continue the conversation. Recent context:\n{}\n\nContinue naturally as {}.",
            state.values.get("recentMessages").cloned().unwrap_or_default(),
            runtime.character().name
        );

        let continuation = runtime
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextLarge,
                system_prompt: format!("You are {}. Continue the conversation naturally.", runtime.character().name),
                prompt,
                max_tokens: Some(1024),
                temperature: Some(0.7),
                stop_sequences: vec![],
            })
            .await?;

        let memory = Memory::new_message(
            runtime.agent_id(),
            runtime.agent_id(),
            message.room_id,
            Content::text(&continuation),
        );
        runtime.database().create_memory(&memory).await?;

        Ok(ActionResult {
            success: true,
            text: Some(continuation),
            data: None,
            error: None,
            continue_chain: false,
        })
    }
}

// -- IGNORE -------------------------------------------------------------------

struct IgnoreAction;

#[async_trait]
impl Action for IgnoreAction {
    fn name(&self) -> &str {
        "IGNORE"
    }
    fn description(&self) -> &str {
        "Ignore the message and do not respond"
    }
    fn similes(&self) -> Vec<String> {
        vec!["SKIP".into(), "PASS".into()]
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, _state: &State) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        debug!("ignoring message");
        Ok(ActionResult::ok("Message ignored"))
    }
}

// -- FOLLOW_ROOM --------------------------------------------------------------

struct FollowRoomAction;

#[async_trait]
impl Action for FollowRoomAction {
    fn name(&self) -> &str {
        "FOLLOW_ROOM"
    }
    fn description(&self) -> &str {
        "Start actively following a room, responding to messages more frequently"
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, state: &State) -> Result<bool> {
        Ok(state.data.room.is_some())
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        runtime
            .database()
            .set_cache(
                &format!("room_follow:{}", message.room_id),
                runtime.agent_id(),
                &serde_json::json!({"following": true, "since": Utc::now().to_rfc3339()}),
                None,
            )
            .await?;
        Ok(ActionResult::ok("Now following this room"))
    }
}

// -- UNFOLLOW_ROOM ------------------------------------------------------------

struct UnfollowRoomAction;

#[async_trait]
impl Action for UnfollowRoomAction {
    fn name(&self) -> &str {
        "UNFOLLOW_ROOM"
    }
    fn description(&self) -> &str {
        "Stop actively following a room"
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, state: &State) -> Result<bool> {
        Ok(state.data.room.is_some())
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        runtime
            .database()
            .delete_cache(
                &format!("room_follow:{}", message.room_id),
                runtime.agent_id(),
            )
            .await?;
        Ok(ActionResult::ok("Stopped following this room"))
    }
}

// -- MUTE_ROOM ----------------------------------------------------------------

struct MuteRoomAction;

#[async_trait]
impl Action for MuteRoomAction {
    fn name(&self) -> &str {
        "MUTE_ROOM"
    }
    fn description(&self) -> &str {
        "Mute a room to stop receiving or responding to messages"
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, state: &State) -> Result<bool> {
        Ok(state.data.room.is_some())
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        runtime
            .database()
            .set_cache(
                &format!("room_muted:{}", message.room_id),
                runtime.agent_id(),
                &serde_json::json!({"muted": true, "since": Utc::now().to_rfc3339()}),
                None,
            )
            .await?;
        Ok(ActionResult::ok("Room muted"))
    }
}

// -- UNMUTE_ROOM --------------------------------------------------------------

struct UnmuteRoomAction;

#[async_trait]
impl Action for UnmuteRoomAction {
    fn name(&self) -> &str {
        "UNMUTE_ROOM"
    }
    fn description(&self) -> &str {
        "Unmute a room to resume responding to messages"
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, state: &State) -> Result<bool> {
        Ok(state.data.room.is_some())
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        runtime
            .database()
            .delete_cache(
                &format!("room_muted:{}", message.room_id),
                runtime.agent_id(),
            )
            .await?;
        Ok(ActionResult::ok("Room unmuted"))
    }
}

// -- NONE ---------------------------------------------------------------------

struct NoneAction;

#[async_trait]
impl Action for NoneAction {
    fn name(&self) -> &str {
        "NONE"
    }
    fn description(&self) -> &str {
        "No action needed — respond with just a message"
    }
    fn priority(&self) -> i32 {
        -100
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, _state: &State) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        Ok(ActionResult::ok("No action taken"))
    }
}

// ===========================================================================
// Evaluators
// ===========================================================================

// -- Fact Extraction ----------------------------------------------------------

struct FactExtractionEvaluator;

#[async_trait]
impl Evaluator for FactExtractionEvaluator {
    fn name(&self) -> &str {
        "fact_extraction"
    }
    fn description(&self) -> &str {
        "Extract factual information from conversations and store as memories"
    }
    fn always_run(&self) -> bool {
        true
    }

    async fn validate(&self, _runtime: &dyn Runtime, _message: &Memory, _state: &State) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<()> {
        let text = message.content.text.as_deref().unwrap_or("");
        if text.len() < 20 {
            return Ok(());
        }

        let prompt = format!(
            "Extract any factual statements or important information from this message. \
             Only extract clear, definitive facts — not opinions or questions.\n\n\
             Message from {}: {}\n\n\
             Respond with a JSON array of fact strings. If no facts, respond with [].",
            state.values.get("senderName").cloned().unwrap_or("User".into()),
            text
        );

        let response = runtime
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextSmall,
                system_prompt: "You are a fact extraction engine. Respond ONLY with a JSON array of strings.".into(),
                prompt,
                max_tokens: Some(256),
                temperature: Some(0.1),
                stop_sequences: vec![],
            })
            .await?;

        let trimmed = response.trim();
        let json_str = if let (Some(start), Some(end)) = (trimmed.find('['), trimmed.rfind(']')) {
            &trimmed[start..=end]
        } else {
            return Ok(());
        };

        let facts: Vec<String> = serde_json::from_str(json_str).unwrap_or_default();

        for fact in facts {
            if fact.len() < 5 {
                continue;
            }
            let fact_memory = Memory {
                id: Uuid::new_v4(),
                content: Content::text(&fact),
                entity_id: message.entity_id,
                agent_id: runtime.agent_id(),
                room_id: message.room_id,
                world_id: message.world_id,
                unique: true,
                created_at: Some(Utc::now()),
                embedding: None,
                metadata: Some(serde_json::json!({"type": "fact", "source_message": message.id.to_string()})),
                memory_type: MemoryType::Description,
            };

            let _ = runtime.database().create_memory(&fact_memory).await;

            // Generate and store embedding
            if let Ok(embedding) = runtime.generate_embedding(&fact).await {
                let mut mem_with_emb = fact_memory;
                mem_with_emb.embedding = Some(embedding);
                let _ = runtime.database().create_memory(&mem_with_emb).await;
            }
        }

        Ok(())
    }
}

// -- Reflection ---------------------------------------------------------------

struct ReflectionEvaluator;

#[async_trait]
impl Evaluator for ReflectionEvaluator {
    fn name(&self) -> &str {
        "reflection"
    }
    fn description(&self) -> &str {
        "Periodically reflect on conversations to generate insights"
    }

    async fn validate(&self, runtime: &dyn Runtime, message: &Memory, _state: &State) -> Result<bool> {
        let count = runtime
            .database()
            .count_memories(message.room_id, true, MemoryType::Message)
            .await
            .unwrap_or(0);
        // Reflect every 10 messages
        Ok(count > 0 && count % 10 == 0)
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        state: &State,
    ) -> Result<()> {
        let recent = state.values.get("recentMessages").cloned().unwrap_or_default();

        let prompt = format!(
            "Reflect on the recent conversation as {}. \
             What patterns do you notice? What's important to remember? \
             Write a brief (2-3 sentence) reflection.\n\nRecent conversation:\n{}",
            runtime.character().name,
            recent
        );

        let reflection = runtime
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextSmall,
                system_prompt: format!("You are {}. Write a brief internal reflection.", runtime.character().name),
                prompt,
                max_tokens: Some(256),
                temperature: Some(0.5),
                stop_sequences: vec![],
            })
            .await?;

        let reflection_memory = Memory {
            id: Uuid::new_v4(),
            content: Content::text(&reflection),
            entity_id: runtime.agent_id(),
            agent_id: runtime.agent_id(),
            room_id: message.room_id,
            world_id: message.world_id,
            unique: true,
            created_at: Some(Utc::now()),
            embedding: None,
            metadata: Some(serde_json::json!({"type": "reflection"})),
            memory_type: MemoryType::Description,
        };
        runtime.database().create_memory(&reflection_memory).await?;

        debug!(reflection = %reflection, "agent reflection");
        Ok(())
    }
}

// ===========================================================================
// Providers
// ===========================================================================

// -- Time Provider ------------------------------------------------------------

struct TimeProvider;

#[async_trait]
impl Provider for TimeProvider {
    fn name(&self) -> &str {
        "time"
    }
    fn description(&self) -> &str {
        "Provides the current date and time"
    }
    fn position(&self) -> i32 {
        -100
    }

    async fn get(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let now = Utc::now();
        Ok(ProviderResult {
            text: Some(format!(
                "Current time: {} (UTC)",
                now.format("%Y-%m-%d %H:%M:%S")
            )),
            values: [("currentTime".into(), now.to_rfc3339())].into(),
            data: None,
        })
    }
}

// -- Facts Provider -----------------------------------------------------------

struct FactsProvider;

#[async_trait]
impl Provider for FactsProvider {
    fn name(&self) -> &str {
        "facts"
    }
    fn description(&self) -> &str {
        "Provides known facts about the conversation participants"
    }
    fn position(&self) -> i32 {
        10
    }

    async fn get(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let facts = runtime
            .database()
            .get_memories(&GetMemoriesParams {
                entity_id: Some(message.entity_id),
                agent_id: Some(runtime.agent_id()),
                memory_type: Some(MemoryType::Description),
                count: Some(10),
                ..Default::default()
            })
            .await
            .unwrap_or_default();

        if facts.is_empty() {
            return Ok(ProviderResult::default());
        }

        let text = facts
            .iter()
            .filter_map(|f| f.content.text.as_deref())
            .collect::<Vec<_>>()
            .join("\n- ");

        Ok(ProviderResult {
            text: Some(format!("Known facts:\n- {}", text)),
            ..Default::default()
        })
    }
}

// -- Relationships Provider ---------------------------------------------------

struct RelationshipsProvider;

#[async_trait]
impl Provider for RelationshipsProvider {
    fn name(&self) -> &str {
        "relationships"
    }
    fn description(&self) -> &str {
        "Provides relationship context between entities"
    }
    fn position(&self) -> i32 {
        20
    }

    async fn get(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let relationships = runtime
            .database()
            .get_relationships(message.entity_id)
            .await
            .unwrap_or_default();

        if relationships.is_empty() {
            return Ok(ProviderResult::default());
        }

        let lines: Vec<String> = relationships
            .iter()
            .map(|r| {
                let tags = if r.tags.is_empty() {
                    "none".to_string()
                } else {
                    r.tags.join(", ")
                };
                format!("- Relationship with {}: tags=[{}]", r.target_entity_id, tags)
            })
            .collect();

        Ok(ProviderResult {
            text: Some(format!("Relationships:\n{}", lines.join("\n"))),
            ..Default::default()
        })
    }
}

// -- Boredom Provider ---------------------------------------------------------

struct BoredomProvider;

#[async_trait]
impl Provider for BoredomProvider {
    fn name(&self) -> &str {
        "boredom"
    }
    fn description(&self) -> &str {
        "Tracks conversation engagement level"
    }
    fn position(&self) -> i32 {
        50
    }

    async fn get(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let count = runtime
            .database()
            .count_memories(message.room_id, true, MemoryType::Message)
            .await
            .unwrap_or(0);

        let engagement = match count {
            0..=5 => "high",
            6..=20 => "moderate",
            21..=50 => "settling",
            _ => "low",
        };

        Ok(ProviderResult {
            text: None,
            values: [
                ("engagementLevel".into(), engagement.into()),
                ("messageCount".into(), count.to_string()),
            ]
            .into(),
            data: None,
        })
    }
}
