use std::sync::Arc;

use async_trait::async_trait;
use serenity::all::{
    Context, CreateMessage, EventHandler, GatewayIntents, Message as DiscordMessage, Ready,
};
use serenity::Client;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::{Runtime, Service};
use rustliza_core::types::*;

// ---------------------------------------------------------------------------
// Discord service
// ---------------------------------------------------------------------------

pub struct DiscordService {
    token: String,
    client: RwLock<Option<()>>,
}

impl DiscordService {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            client: RwLock::new(None),
        }
    }
}

#[async_trait]
impl Service for DiscordService {
    fn name(&self) -> &str {
        "discord"
    }

    async fn start(&self, runtime: Arc<dyn Runtime>) -> Result<()> {
        let token = self.token.clone();
        let handler = DiscordHandler::new(runtime);

        tokio::spawn(async move {
            let intents = GatewayIntents::GUILD_MESSAGES
                | GatewayIntents::DIRECT_MESSAGES
                | GatewayIntents::MESSAGE_CONTENT;

            let mut client = Client::builder(&token, intents)
                .event_handler(handler)
                .await
                .expect("failed to create discord client");

            if let Err(e) = client.start().await {
                error!(error = %e, "discord client error");
            }
        });

        *self.client.write().await = Some(());
        info!("discord service started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        *self.client.write().await = None;
        info!("discord service stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Event handler
// ---------------------------------------------------------------------------

struct DiscordHandler {
    runtime: Arc<dyn Runtime>,
}

impl DiscordHandler {
    fn new(runtime: Arc<dyn Runtime>) -> Self {
        Self { runtime }
    }

    fn discord_id_to_uuid(id: u64) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&id.to_le_bytes());
        // Set version 4 and variant bits
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[async_trait]
impl EventHandler for DiscordHandler {
    async fn message(&self, ctx: Context, msg: DiscordMessage) {
        // Ignore bot messages
        if msg.author.bot {
            return;
        }

        let agent_id = self.runtime.agent_id();
        let entity_id = Self::discord_id_to_uuid(msg.author.id.get());
        let room_id = Self::discord_id_to_uuid(msg.channel_id.get());

        // Ensure entity exists
        let entity = Entity {
            id: entity_id,
            agent_id,
            names: vec![msg.author.name.clone()],
            metadata: Some(serde_json::json!({
                "discord_id": msg.author.id.get().to_string(),
                "discriminator": msg.author.discriminator,
            })),
            created_at: Some(chrono::Utc::now()),
        };
        let _ = self.runtime.database().create_entity(&entity).await;

        // Ensure room exists
        let is_dm = msg.guild_id.is_none();
        let room = Room {
            id: room_id,
            agent_id,
            source: "discord".to_string(),
            channel_type: if is_dm {
                ChannelType::Dm
            } else {
                ChannelType::Group
            },
            name: Some(format!("discord-{}", msg.channel_id.get())),
            channel_id: Some(msg.channel_id.get().to_string()),
            world_id: msg.guild_id.map(|g| Self::discord_id_to_uuid(g.get())),
            metadata: None,
            created_at: Some(chrono::Utc::now()),
        };
        let _ = self.runtime.database().create_room(&room).await;
        let _ = self
            .runtime
            .database()
            .add_participant(entity_id, room_id, agent_id)
            .await;

        // Create memory and process
        let memory = Memory::new_message(
            agent_id,
            entity_id,
            room_id,
            Content {
                text: Some(msg.content.clone()),
                source: Some("discord".into()),
                ..Default::default()
            },
        );

        match self.runtime.process_message(&memory).await {
            Ok(responses) => {
                for response in &responses {
                    if let Some(text) = &response.content.text {
                        if !text.is_empty() {
                            let builder = CreateMessage::new().content(text);
                            if let Err(e) = msg.channel_id.send_message(&ctx.http, builder).await {
                                error!(error = %e, "failed to send discord message");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "error processing discord message");
            }
        }
    }

    async fn ready(&self, _ctx: Context, ready: Ready) {
        info!(
            user = %ready.user.name,
            guilds = ready.guilds.len(),
            "discord bot connected"
        );
    }
}
