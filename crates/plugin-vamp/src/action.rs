//! Pseudo-vamp coin generation action.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use tracing::{info, warn};
use uuid::Uuid;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::traits::{Action, Runtime};
use rustliza_core::types::*;

use crate::coins::{CoinFetcher, TrendingCoin};
use crate::listing;
use crate::state::{GeneratedCoin, ScrapedTrend, VampState};

pub struct GenerateVampCoinAction {
    state: Arc<VampState>,
}

impl GenerateVampCoinAction {
    pub fn new(state: Arc<VampState>) -> Self {
        Self { state }
    }

    /// Run the full pipeline once and return the generated coin. Used both
    /// by the dispatched [`Action`] handler and by the schedule service.
    pub async fn run_once(&self, runtime: &dyn Runtime) -> Result<GeneratedCoin> {
        let cfg = self.state.config().await;

        let fetcher = CoinFetcher::new(&cfg.coin_chain);
        let coin = fetcher
            .pick_random(20)
            .await
            .map_err(|e| RustlizaError::Other(format!("coin fetch failed: {e}")))?
            .ok_or_else(|| RustlizaError::Other("no trending coins available".into()))?;

        let trends = self.state.trends().await;
        let chosen_trend = pick_relevant_trend(&trends, &coin);

        let generated = generate_parody(runtime, &coin, chosen_trend.as_ref()).await?;

        let coin_record = GeneratedCoin {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            name: generated.name,
            ticker: generated.ticker,
            blurb: generated.blurb,
            website: generated.website,
            source_coin: coin,
            source_trend: chosen_trend,
            listing_path: cfg.listings_dir.clone(),
        };

        let path = listing::write_to(&cfg.listings_dir, &coin_record)
            .map_err(|e| RustlizaError::Other(format!("write listing: {e}")))?;
        let mut coin_record = coin_record;
        coin_record.listing_path = path;

        info!(
            ticker = %coin_record.ticker,
            source = %coin_record.source_coin.symbol,
            listing = %coin_record.listing_path.display(),
            "vamp: minted parody coin"
        );

        self.state.push_coin(coin_record.clone()).await;
        Ok(coin_record)
    }
}

#[async_trait]
impl Action for GenerateVampCoinAction {
    fn name(&self) -> &str {
        "GENERATE_VAMP_COIN"
    }

    fn description(&self) -> &str {
        "Generate a parody (pseudo-vamp) coin from a currently trending memecoin, \
         paired with a current Twitter trend and a fitting funny website."
    }

    fn similes(&self) -> Vec<String> {
        vec![
            "VAMP_COIN".into(),
            "PARODY_COIN".into(),
            "MINT_PARODY".into(),
        ]
    }

    fn priority(&self) -> i32 {
        50
    }

    async fn validate(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        match self.run_once(runtime).await {
            Ok(coin) => Ok(ActionResult {
                success: true,
                text: Some(format!(
                    "Minted parody coin ${}: {} — paired website {}",
                    coin.ticker, coin.name, coin.website
                )),
                data: Some(serde_json::to_value(&coin).unwrap_or(serde_json::Value::Null)),
                error: None,
                continue_chain: false,
            }),
            Err(e) => {
                warn!(error = %e, "vamp: coin generation failed");
                Ok(ActionResult::err(format!("{}", e)))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ParodyResponse {
    name: String,
    ticker: String,
    blurb: String,
    website: String,
}

/// Build the LLM prompt, ask, and parse the JSON. Anything outside the JSON
/// braces is tolerated since model outputs commonly include leading prose.
async fn generate_parody(
    runtime: &dyn Runtime,
    coin: &TrendingCoin,
    trend: Option<&ScrapedTrend>,
) -> Result<ParodyResponse> {
    let trend_block = match trend {
        Some(t) => {
            let tweet_block = t
                .top_tweet
                .as_ref()
                .map(|tw| {
                    format!(
                        "Paired tweet (you may riff on it): @{author}: \"{text}\"\nTweet URL: {url}",
                        author = tw.author,
                        text = tw.text.chars().take(280).collect::<String>(),
                        url = tw.url,
                    )
                })
                .unwrap_or_default();
            format!(
                "Current X trend to riff on: \"{name}\"{category}{posts}\n{tweet_block}",
                name = t.name,
                category = t
                    .category
                    .as_ref()
                    .map(|c| format!(" (category: {c})"))
                    .unwrap_or_default(),
                posts = t
                    .post_count
                    .as_ref()
                    .map(|p| format!(" — {p}"))
                    .unwrap_or_default(),
                tweet_block = tweet_block,
            )
        }
        None => "No current trend available — invent something topical and unhinged.".to_string(),
    };

    let prompt = format!(
        r#"You are a memecoin parody factory. Given a currently-pumping coin and a current X trend, invent a parody coin that puns on the original's name in the spirit of the trend.

Rules:
- The new name MUST be a pun on the original coin's name — not a copy.
- The ticker is 3-7 uppercase letters or digits, no leading $.
- The blurb is one or two sentences, in-character for crypto Twitter (irreverent, knowing, no buzzwords like "revolutionary").
- The website is a single funny but plausible domain (e.g. "wifegavemecancer.fail") with NO scheme, NO path, NO http://. Use a non-malicious TLD (.com, .lol, .fail, .biz, .wtf, .gay, .xyz, .meme).
- Output VALID JSON ONLY, no markdown fences, no commentary, matching exactly:
  {{ "name": "...", "ticker": "...", "blurb": "...", "website": "..." }}

The original (vamp target):
- Name: {coin_name}
- Symbol: {coin_symbol}
- Chain: {chain}

{trend_block}

JSON response:"#,
        coin_name = coin.name,
        coin_symbol = coin.symbol,
        chain = coin.chain,
        trend_block = trend_block,
    );

    let response = runtime
        .generate_text(&GenerateTextParams {
            model_type: ModelType::TextLarge,
            system_prompt:
                "You are a memecoin parody copywriter. You respond with valid JSON only — no \
                 markdown, no prose."
                    .into(),
            prompt,
            max_tokens: Some(400),
            temperature: Some(0.95),
            stop_sequences: vec![],
        })
        .await?;

    let parsed = extract_json::<ParodyResponse>(&response).ok_or_else(|| {
        RustlizaError::Other(format!("model did not return valid JSON: {response}"))
    })?;

    // Sanitise the ticker: uppercase, strip leading $, drop anything weird.
    let mut ticker: String = parsed
        .ticker
        .trim_start_matches('$')
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_uppercase();
    if ticker.is_empty() {
        ticker = "VAMP".into();
    }
    ticker.truncate(8);

    let website = parsed.website.trim().trim_start_matches("https://").trim_start_matches("http://").trim_end_matches('/').to_string();

    Ok(ParodyResponse {
        name: parsed.name,
        ticker,
        blurb: parsed.blurb,
        website,
    })
}

fn extract_json<T: serde::de::DeserializeOwned>(s: &str) -> Option<T> {
    let trimmed = s.trim();
    if let Ok(v) = serde_json::from_str::<T>(trimmed) {
        return Some(v);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<T>(&trimmed[start..=end]).ok()
}

/// Cheap heuristic: prefer a trend whose name overlaps token-wise with the
/// coin's name or symbol; otherwise fall back to a random pick.
fn pick_relevant_trend(trends: &[ScrapedTrend], coin: &TrendingCoin) -> Option<ScrapedTrend> {
    if trends.is_empty() {
        return None;
    }
    let needle = format!("{} {}", coin.name, coin.symbol).to_lowercase();
    let scored: Option<&ScrapedTrend> = trends
        .iter()
        .filter(|t| {
            let lname = t.name.to_lowercase();
            needle.split_whitespace().any(|w| w.len() > 3 && lname.contains(w))
        })
        .next();
    if let Some(t) = scored {
        return Some(t.clone());
    }
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    trends.choose(&mut rng).cloned()
}
