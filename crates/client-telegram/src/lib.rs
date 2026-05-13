use std::sync::Arc;

use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::Message as TgMessage;
use tokio::sync::RwLock;
use tracing::{error, info};
use uuid::Uuid;

use rustliza_core::error::Result;
use rustliza_core::traits::{Runtime, Service};
use rustliza_core::types::*;

// ---------------------------------------------------------------------------
// Telegram service
// ---------------------------------------------------------------------------

pub struct TelegramService {
    token: String,
    shutdown: RwLock<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl TelegramService {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            shutdown: RwLock::new(None),
        }
    }

    fn telegram_id_to_uuid(chat_id: i64) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&chat_id.to_le_bytes());
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[async_trait]
impl Service for TelegramService {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn start(&self, runtime: Arc<dyn Runtime>) -> Result<()> {
        let bot = Bot::new(&self.token);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        *self.shutdown.write().await = Some(tx);

        tokio::spawn(async move {
            let handler = Update::filter_message().endpoint(handle_message);

            let mut dispatcher = Dispatcher::builder(bot, handler)
                .dependencies(dptree::deps![runtime])
                .enable_ctrlc_handler()
                .build();

            tokio::select! {
                _ = dispatcher.dispatch() => {},
                _ = async { rx.await.ok() } => {
                    info!("telegram service shutting down");
                },
            }
        });

        info!("telegram service started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(tx) = self.shutdown.write().await.take() {
            let _ = tx.send(());
        }
        info!("telegram service stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

async fn handle_message(
    bot: Bot,
    msg: TgMessage,
    runtime: Arc<dyn Runtime>,
) -> ResponseResult<()> {
    let text = match msg.text() {
        Some(t) => t.to_string(),
        None => return Ok(()),
    };

    let agent_id = runtime.agent_id();
    let user = msg.from.as_ref();
    let user_id = user.map(|u| u.id.0 as i64).unwrap_or(0);
    let entity_id = TelegramService::telegram_id_to_uuid(user_id);
    let room_id = TelegramService::telegram_id_to_uuid(msg.chat.id.0);

    // Ensure entity
    let username = user
        .and_then(|u| u.username.clone())
        .unwrap_or_else(|| {
            user.map(|u| u.first_name.clone()).unwrap_or_else(|| "Unknown".into())
        });

    let entity = Entity {
        id: entity_id,
        agent_id,
        names: vec![username],
        metadata: Some(serde_json::json!({
            "telegram_id": user_id,
            "chat_id": msg.chat.id.0,
        })),
        created_at: Some(chrono::Utc::now()),
    };
    let _ = runtime.database().create_entity(&entity).await;

    // Ensure room
    let is_private = msg.chat.is_private();
    let room = Room {
        id: room_id,
        agent_id,
        source: "telegram".to_string(),
        channel_type: if is_private {
            ChannelType::Dm
        } else {
            ChannelType::Group
        },
        name: msg.chat.title().map(|t| t.to_string()),
        channel_id: Some(msg.chat.id.0.to_string()),
        world_id: None,
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };
    let _ = runtime.database().create_room(&room).await;
    let _ = runtime
        .database()
        .add_participant(entity_id, room_id, agent_id)
        .await;

    // Process message
    let memory = Memory::new_message(
        agent_id,
        entity_id,
        room_id,
        Content {
            text: Some(text),
            source: Some("telegram".into()),
            ..Default::default()
        },
    );

    match runtime.process_message(&memory).await {
        Ok(responses) => {
            for response in &responses {
                if let Some(reply_text) = &response.content.text {
                    if !reply_text.is_empty() {
                        if let Err(e) = bot.send_message(msg.chat.id, reply_text).await {
                            error!(error = %e, "failed to send telegram message");
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!(error = %e, "error processing telegram message");
        }
    }

    Ok(())
}
