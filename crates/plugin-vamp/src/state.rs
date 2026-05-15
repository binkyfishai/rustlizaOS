//! Shared mutable state for the vamp plugin.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::coins::TrendingCoin;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VampConfig {
    pub auto_enabled: bool,
    pub interval_secs: u64,
    pub scrape_enabled: bool,
    pub scrape_interval_secs: u64,
    pub coin_chain: String,
    pub listings_dir: PathBuf,
    pub max_trends: usize,
}

impl Default for VampConfig {
    fn default() -> Self {
        Self {
            auto_enabled: false,
            interval_secs: 3600,
            scrape_enabled: true,
            scrape_interval_secs: 1800,
            coin_chain: "solana".to_string(),
            listings_dir: PathBuf::from("out/listings"),
            max_trends: 12,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapedTweet {
    pub url: String,
    pub author: String,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapedTrend {
    pub name: String,
    pub category: Option<String>,
    pub post_count: Option<String>,
    pub top_tweet: Option<ScrapedTweet>,
    pub scraped_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneratedCoin {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub name: String,
    pub ticker: String,
    pub blurb: String,
    pub website: String,
    pub source_coin: TrendingCoin,
    pub source_trend: Option<ScrapedTrend>,
    pub listing_path: PathBuf,
}

/// Process-wide shared state for the vamp plugin. All fields are behind
/// `RwLock` so the long-running services and the API handlers can both read
/// and update them without dancing through the rustlizaOS database for state
/// that is fundamentally ephemeral.
pub struct VampState {
    config: RwLock<VampConfig>,
    trends: RwLock<Vec<ScrapedTrend>>,
    coins: RwLock<Vec<GeneratedCoin>>,
    last_scrape: RwLock<Option<DateTime<Utc>>>,
    last_generate: RwLock<Option<DateTime<Utc>>>,
}

impl VampState {
    pub fn new(config: VampConfig) -> Arc<Self> {
        Arc::new(Self {
            config: RwLock::new(config),
            trends: RwLock::new(Vec::new()),
            coins: RwLock::new(Vec::new()),
            last_scrape: RwLock::new(None),
            last_generate: RwLock::new(None),
        })
    }

    pub async fn config(&self) -> VampConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config<F>(&self, f: F) -> VampConfig
    where
        F: FnOnce(&mut VampConfig),
    {
        let mut cfg = self.config.write().await;
        f(&mut cfg);
        cfg.clone()
    }

    pub async fn replace_trends(&self, trends: Vec<ScrapedTrend>) {
        *self.trends.write().await = trends;
        *self.last_scrape.write().await = Some(Utc::now());
    }

    pub async fn trends(&self) -> Vec<ScrapedTrend> {
        self.trends.read().await.clone()
    }

    pub async fn push_coin(&self, coin: GeneratedCoin) {
        let mut coins = self.coins.write().await;
        coins.push(coin);
        if coins.len() > 200 {
            let drop = coins.len() - 200;
            coins.drain(0..drop);
        }
        *self.last_generate.write().await = Some(Utc::now());
    }

    pub async fn coins(&self) -> Vec<GeneratedCoin> {
        self.coins.read().await.clone()
    }

    pub async fn last_scrape(&self) -> Option<DateTime<Utc>> {
        *self.last_scrape.read().await
    }

    pub async fn last_generate(&self) -> Option<DateTime<Utc>> {
        *self.last_generate.read().await
    }
}
