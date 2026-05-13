use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::traits::{Runtime, Service};
use rustliza_core::types::*;

// ---------------------------------------------------------------------------
// Twitter API types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct TweetData {
    id: String,
    text: String,
    author_id: Option<String>,
    conversation_id: Option<String>,
    #[allow(dead_code)]
    created_at: Option<String>,
}

#[derive(Deserialize)]
struct UserData {
    id: String,
    name: String,
    username: String,
}

#[derive(Deserialize)]
struct TweetsResponse {
    #[serde(default)]
    data: Vec<TweetData>,
    #[serde(default)]
    includes: Option<TweetIncludes>,
    meta: Option<TweetMeta>,
}

#[derive(Deserialize)]
struct TweetIncludes {
    #[serde(default)]
    users: Vec<UserData>,
}

#[derive(Deserialize)]
struct TweetMeta {
    newest_id: Option<String>,
}

#[derive(Serialize)]
struct CreateTweetBody {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply: Option<TweetReply>,
}

#[derive(Serialize)]
struct TweetReply {
    in_reply_to_tweet_id: String,
}

#[derive(Deserialize)]
struct MeResponse {
    data: UserData,
}

// ---------------------------------------------------------------------------
// Twitter service
// ---------------------------------------------------------------------------

pub struct TwitterService {
    bearer_token: String,
    poll_interval: Duration,
    shutdown: RwLock<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl TwitterService {
    pub fn new(bearer_token: impl Into<String>) -> Self {
        Self {
            bearer_token: bearer_token.into(),
            poll_interval: Duration::from_secs(30),
            shutdown: RwLock::new(None),
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    fn twitter_id_to_uuid(id: &str) -> Uuid {
        let hash = id.bytes().fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&hash.to_le_bytes());
        bytes[8..16].copy_from_slice(&hash.to_be_bytes());
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[async_trait]
impl Service for TwitterService {
    fn name(&self) -> &str {
        "twitter"
    }

    async fn start(&self, runtime: Arc<dyn Runtime>) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        *self.shutdown.write().await = Some(tx);

        let bearer_token = self.bearer_token.clone();
        let poll_interval = self.poll_interval;
        let client = reqwest::Client::new();

        tokio::spawn(async move {
            // Get our own user ID
            let me = match get_me(&client, &bearer_token).await {
                Ok(me) => me,
                Err(e) => {
                    error!(error = %e, "failed to get twitter user info");
                    return;
                }
            };
            info!(username = %me.username, id = %me.id, "twitter bot authenticated");

            let agent_id = runtime.agent_id();
            let mut since_id: Option<String> = None;
            let mut shutdown = rx;

            loop {
                tokio::select! {
                    _ = &mut shutdown => {
                        info!("twitter service shutting down");
                        return;
                    }
                    _ = tokio::time::sleep(poll_interval) => {
                        match poll_mentions(&client, &bearer_token, &me.id, since_id.as_deref()).await {
                            Ok(response) => {
                                if let Some(meta) = &response.meta {
                                    if let Some(newest) = &meta.newest_id {
                                        since_id = Some(newest.clone());
                                    }
                                }

                                let users = response.includes
                                    .as_ref()
                                    .map(|i| &i.users[..])
                                    .unwrap_or(&[]);

                                for tweet in &response.data {
                                    let author = tweet.author_id.as_deref().unwrap_or("unknown");
                                    let user = users.iter().find(|u| u.id == author);
                                    let username = user.map(|u| u.username.as_str()).unwrap_or("unknown");
                                    let display_name = user.map(|u| u.name.as_str()).unwrap_or("Unknown");

                                    let entity_id = Self::twitter_id_to_uuid(author);
                                    let room_id = tweet.conversation_id
                                        .as_deref()
                                        .map(|c| Self::twitter_id_to_uuid(c))
                                        .unwrap_or_else(|| Self::twitter_id_to_uuid(&tweet.id));

                                    // Ensure entity
                                    let entity = Entity {
                                        id: entity_id,
                                        agent_id,
                                        names: vec![display_name.to_string()],
                                        metadata: Some(serde_json::json!({
                                            "twitter_id": author,
                                            "username": username,
                                        })),
                                        created_at: Some(chrono::Utc::now()),
                                    };
                                    let _ = runtime.database().create_entity(&entity).await;

                                    // Ensure room
                                    let room = Room {
                                        id: room_id,
                                        agent_id,
                                        source: "twitter".to_string(),
                                        channel_type: ChannelType::Feed,
                                        name: Some(format!("twitter-{}", &tweet.id[..8.min(tweet.id.len())])),
                                        channel_id: tweet.conversation_id.clone(),
                                        world_id: None,
                                        metadata: None,
                                        created_at: Some(chrono::Utc::now()),
                                    };
                                    let _ = runtime.database().create_room(&room).await;

                                    // Process message
                                    let memory = Memory::new_message(
                                        agent_id,
                                        entity_id,
                                        room_id,
                                        Content {
                                            text: Some(tweet.text.clone()),
                                            source: Some("twitter".into()),
                                            ..Default::default()
                                        },
                                    );

                                    match runtime.process_message(&memory).await {
                                        Ok(responses) => {
                                            for response in &responses {
                                                if let Some(reply_text) = &response.content.text {
                                                    if !reply_text.is_empty() {
                                                        // Truncate to 280 chars
                                                        let truncated = if reply_text.len() > 280 {
                                                            format!("{}…", &reply_text[..279])
                                                        } else {
                                                            reply_text.clone()
                                                        };
                                                        if let Err(e) = post_reply(
                                                            &client,
                                                            &bearer_token,
                                                            &truncated,
                                                            &tweet.id,
                                                        ).await {
                                                            error!(error = %e, "failed to reply to tweet");
                                                        } else {
                                                            debug!(tweet_id = %tweet.id, "replied to tweet");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(error = %e, "error processing twitter mention");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to poll twitter mentions");
                            }
                        }
                    }
                }
            }
        });

        info!("twitter service started");
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(tx) = self.shutdown.write().await.take() {
            let _ = tx.send(());
        }
        info!("twitter service stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Twitter API helpers
// ---------------------------------------------------------------------------

async fn get_me(
    client: &reqwest::Client,
    bearer_token: &str,
) -> std::result::Result<UserData, RustlizaError> {
    let resp = client
        .get("https://api.twitter.com/2/users/me")
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .map_err(|e| RustlizaError::Other(format!("twitter request failed: {}", e)))?;

    let body = resp
        .text()
        .await
        .map_err(|e| RustlizaError::Other(format!("failed to read twitter response: {}", e)))?;

    let me: MeResponse = serde_json::from_str(&body)
        .map_err(|e| RustlizaError::Other(format!("failed to parse twitter user: {}", e)))?;

    Ok(me.data)
}

async fn poll_mentions(
    client: &reqwest::Client,
    bearer_token: &str,
    user_id: &str,
    since_id: Option<&str>,
) -> std::result::Result<TweetsResponse, RustlizaError> {
    let mut url = format!(
        "https://api.twitter.com/2/users/{}/mentions?tweet.fields=author_id,conversation_id,created_at&expansions=author_id",
        user_id
    );

    if let Some(since) = since_id {
        url.push_str(&format!("&since_id={}", since));
    }

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .map_err(|e| RustlizaError::Other(format!("twitter poll failed: {}", e)))?;

    let body = resp
        .text()
        .await
        .map_err(|e| RustlizaError::Other(format!("twitter response read failed: {}", e)))?;

    serde_json::from_str(&body)
        .map_err(|e| RustlizaError::Other(format!("twitter parse failed: {}", e)))
}

async fn post_reply(
    client: &reqwest::Client,
    bearer_token: &str,
    text: &str,
    in_reply_to: &str,
) -> std::result::Result<(), RustlizaError> {
    let body = CreateTweetBody {
        text: text.to_string(),
        reply: Some(TweetReply {
            in_reply_to_tweet_id: in_reply_to.to_string(),
        }),
    };

    let resp = client
        .post("https://api.twitter.com/2/tweets")
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| RustlizaError::Other(format!("tweet post failed: {}", e)))?;

    if !resp.status().is_success() {
        let err_body = resp.text().await.unwrap_or_default();
        return Err(RustlizaError::Other(format!("tweet failed: {}", err_body)));
    }

    Ok(())
}
