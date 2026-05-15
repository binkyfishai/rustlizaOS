//! DexScreener trending-coin fetcher.
//!
//! DexScreener's `latest/dex/search` endpoint is documented and free; we use
//! it as a deterministic lookup over recent high-volume Solana pairs. If
//! that's empty (e.g. rate-limited) we fall back to the token-boosts feed,
//! which is the closest thing DexScreener exposes to a "what's hot right
//! now" list.

use anyhow::{Context as _, Result};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendingCoin {
    pub symbol: String,
    pub name: String,
    pub chain: String,
    pub address: String,
    pub price_usd: Option<String>,
    pub volume_24h_usd: Option<f64>,
    pub fdv_usd: Option<f64>,
    pub url: String,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    pairs: Vec<SearchPair>,
}

#[derive(Debug, Deserialize)]
struct SearchPair {
    #[serde(default)]
    chain_id: String,
    #[serde(default, rename = "chainId")]
    chain_id_camel: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    base_token: Option<TokenInfo>,
    #[serde(default, rename = "baseToken")]
    base_token_camel: Option<TokenInfo>,
    #[serde(default, rename = "priceUsd")]
    price_usd: Option<String>,
    #[serde(default)]
    fdv: Option<f64>,
    #[serde(default)]
    volume: Option<Volume>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    #[serde(default)]
    address: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    symbol: String,
}

#[derive(Debug, Deserialize)]
struct Volume {
    #[serde(default)]
    h24: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct BoostEntry {
    #[serde(default, rename = "chainId")]
    chain_id: String,
    #[serde(default, rename = "tokenAddress")]
    token_address: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

pub struct CoinFetcher {
    client: reqwest::Client,
    chain: String,
}

impl CoinFetcher {
    pub fn new(chain: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("rustliza-vamp/0.1 (+https://example.invalid)")
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            chain: chain.into(),
        }
    }

    /// Return a list of currently-trending tokens on the configured chain.
    /// Ordered roughly by 24h volume, which is a reasonable proxy for "hot."
    pub async fn trending(&self, limit: usize) -> Result<Vec<TrendingCoin>> {
        // "Search" with an empty-ish query returns popular pairs sorted by
        // volume — undocumented but stable enough to be the canonical
        // memecoin discovery path on DexScreener.
        let search_url =
            format!("https://api.dexscreener.com/latest/dex/search?q={}", self.chain);
        let resp = self
            .client
            .get(&search_url)
            .send()
            .await
            .context("dexscreener search request failed")?;

        let body: SearchResponse = resp
            .json()
            .await
            .context("dexscreener search response parse failed")?;

        let mut coins: Vec<TrendingCoin> = body
            .pairs
            .into_iter()
            .filter_map(|p| {
                let chain = if p.chain_id.is_empty() {
                    p.chain_id_camel
                } else {
                    p.chain_id
                };
                if !chain.eq_ignore_ascii_case(&self.chain) {
                    return None;
                }
                let token = p.base_token.or(p.base_token_camel)?;
                if token.symbol.is_empty() {
                    return None;
                }
                Some(TrendingCoin {
                    symbol: token.symbol,
                    name: if token.name.is_empty() {
                        "Unknown".to_string()
                    } else {
                        token.name
                    },
                    chain,
                    address: token.address,
                    price_usd: p.price_usd,
                    volume_24h_usd: p.volume.and_then(|v| v.h24),
                    fdv_usd: p.fdv,
                    url: p.url,
                })
            })
            .collect();

        coins.sort_by(|a, b| {
            b.volume_24h_usd
                .unwrap_or(0.0)
                .partial_cmp(&a.volume_24h_usd.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Deduplicate by symbol — DexScreener returns multiple pairs per
        // token across DEXes.
        let mut seen = std::collections::HashSet::new();
        coins.retain(|c| seen.insert(c.symbol.to_lowercase()));
        coins.truncate(limit);

        if !coins.is_empty() {
            return Ok(coins);
        }

        // Fallback: token boosts feed.
        let boost_url = "https://api.dexscreener.com/token-boosts/latest/v1";
        let boosts: Vec<BoostEntry> = self
            .client
            .get(boost_url)
            .send()
            .await
            .context("dexscreener boosts request failed")?
            .json()
            .await
            .context("dexscreener boosts parse failed")?;

        let coins = boosts
            .into_iter()
            .filter(|b| b.chain_id.eq_ignore_ascii_case(&self.chain))
            .take(limit)
            .map(|b| TrendingCoin {
                symbol: derive_symbol_from_description(b.description.as_deref())
                    .unwrap_or_else(|| short_address(&b.token_address)),
                name: b
                    .description
                    .clone()
                    .unwrap_or_else(|| "Boosted Token".to_string()),
                chain: b.chain_id,
                address: b.token_address,
                price_usd: None,
                volume_24h_usd: None,
                fdv_usd: None,
                url: b.url.unwrap_or_default(),
            })
            .collect();

        Ok(coins)
    }

    /// Convenience: pick one coin at random from the top `limit`.
    pub async fn pick_random(&self, limit: usize) -> Result<Option<TrendingCoin>> {
        let coins = self.trending(limit).await?;
        let mut rng = rand::thread_rng();
        Ok(coins.choose(&mut rng).cloned())
    }
}

fn derive_symbol_from_description(desc: Option<&str>) -> Option<String> {
    let d = desc?;
    // descriptions sometimes look like "$FOO" — pull the ticker out.
    let re = regex::Regex::new(r"\$([A-Z0-9]{2,12})").ok()?;
    re.captures(d)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
}

fn short_address(addr: &str) -> String {
    if addr.len() < 8 {
        addr.to_string()
    } else {
        format!("{}…{}", &addr[..4], &addr[addr.len() - 4..])
    }
}
