//! Trend curator: drives a headless Chrome session against x.com/explore to
//! pull the current trending list plus one top tweet per trend, and stores
//! the result in the shared [`VampState`].
//!
//! The selectors used here are X/Twitter's current ones at time of writing
//! and **will break** when the site is changed. Treat this as a personal toy
//! — when it stops working, either replace the selectors or feed `VampState`
//! from a different source.
//!
//! Authentication: x.com renders very little useful content while signed
//! out. To improve results, point Chrome at a logged-in profile by setting
//! `VAMP_CHROME_USER_DATA_DIR` to the path of an existing Chrome profile
//! that you've already logged into x.com with. Without it the service still
//! runs but trend coverage degrades.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use headless_chrome::{Browser, LaunchOptions};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use rustliza_core::error::Result;
use rustliza_core::traits::{Runtime, Service};

use crate::state::{ScrapedTrend, ScrapedTweet, VampState};

pub struct TrendScraperService {
    state: Arc<VampState>,
}

impl TrendScraperService {
    pub fn new(state: Arc<VampState>) -> Self {
        Self { state }
    }

    fn scrape_once(max_trends: usize) -> anyhow::Result<Vec<ScrapedTrend>> {
        let mut options = LaunchOptions::default_builder();
        options.headless(std::env::var("VAMP_BROWSER_HEAD").ok().is_none());
        options.sandbox(false);
        if let Ok(dir) = std::env::var("VAMP_CHROME_USER_DATA_DIR") {
            options.user_data_dir(Some(std::path::PathBuf::from(dir)));
        }
        let opts = options
            .build()
            .map_err(|e| anyhow::anyhow!("chrome options: {e}"))?;

        let browser = Browser::new(opts)?;
        let tab = browser.new_tab()?;
        tab.set_default_timeout(Duration::from_secs(20));

        tab.navigate_to("https://x.com/explore/tabs/trending")?;
        tab.wait_until_navigated()?;
        // Trend cells are not present immediately; the explore page hydrates
        // client-side. Wait for at least one trend element.
        let _ = tab.wait_for_element_with_custom_timeout(
            "div[data-testid='trend']",
            Duration::from_secs(15),
        );

        // Tiny additional settle.
        std::thread::sleep(Duration::from_millis(800));

        let trend_elems = tab
            .find_elements("div[data-testid='trend']")
            .unwrap_or_default();

        let mut trends: Vec<ScrapedTrend> = Vec::new();
        for el in trend_elems.into_iter().take(max_trends) {
            let text = el.get_inner_text().unwrap_or_default();
            if text.is_empty() {
                continue;
            }
            // Trend cells render as three stacked lines: category, name,
            // "<n> posts". The order is usually [category, name, count] but
            // tweets-style trends collapse to two lines. We take the longest
            // line as the name and pull a "K posts" line if present.
            let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
            let name = lines
                .iter()
                .max_by_key(|l| l.len())
                .map(|s| s.trim().to_string())
                .unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            let category = lines.first().map(|s| s.trim().to_string()).filter(|s| s != &name);
            let post_count = lines
                .iter()
                .find(|l| l.to_lowercase().contains("post"))
                .map(|s| s.trim().to_string());

            trends.push(ScrapedTrend {
                name,
                category,
                post_count,
                top_tweet: None,
                scraped_at: Utc::now(),
            });
        }

        // For each trend, hop to the live-search page and grab the first
        // visible tweet. This is the slowest part of the scrape — gated to
        // the first 5 trends.
        let trend_fetch_limit = trends.len().min(5);
        for i in 0..trend_fetch_limit {
            let q = urlencoding_encode(&trends[i].name);
            let url = format!("https://x.com/search?q={q}&src=trend_click&f=live");
            if tab.navigate_to(&url).is_err() {
                continue;
            }
            if tab.wait_until_navigated().is_err() {
                continue;
            }
            let _ = tab.wait_for_element_with_custom_timeout(
                "article[role='article']",
                Duration::from_secs(8),
            );
            std::thread::sleep(Duration::from_millis(400));
            let Ok(article) = tab.find_element("article[role='article']") else {
                continue;
            };

            let tweet_text = article.get_inner_text().unwrap_or_default();
            let tweet_text = tweet_text.lines().take(8).collect::<Vec<_>>().join(" ");

            let url_attr = article
                .find_element("a[href*='/status/']")
                .ok()
                .and_then(|a| a.get_attribute_value("href").ok().flatten())
                .map(|h| {
                    if h.starts_with("http") {
                        h
                    } else {
                        format!("https://x.com{h}")
                    }
                });

            let author = article
                .find_element("div[data-testid='User-Name']")
                .ok()
                .and_then(|n| n.get_inner_text().ok())
                .and_then(|t| t.lines().next().map(|s| s.to_string()))
                .unwrap_or_else(|| "anon".to_string());

            if let Some(u) = url_attr {
                trends[i].top_tweet = Some(ScrapedTweet {
                    url: u,
                    author,
                    text: tweet_text,
                });
            }
        }

        Ok(trends)
    }
}

#[async_trait]
impl Service for TrendScraperService {
    fn name(&self) -> &str {
        "vamp.trend_scraper"
    }

    async fn start(&self, _runtime: Arc<dyn Runtime>) -> Result<()> {
        let state = self.state.clone();
        tokio::spawn(async move {
            loop {
                let cfg = state.config().await;
                if !cfg.scrape_enabled {
                    sleep(Duration::from_secs(15)).await;
                    continue;
                }

                let max = cfg.max_trends;
                info!(max_trends = max, "vamp: scraping x trends");
                let trends = tokio::task::spawn_blocking(move || {
                    TrendScraperService::scrape_once(max)
                })
                .await;

                match trends {
                    Ok(Ok(t)) if !t.is_empty() => {
                        info!(count = t.len(), "vamp: scraped trends");
                        state.replace_trends(t).await;
                    }
                    Ok(Ok(_)) => {
                        warn!("vamp: scraper returned zero trends");
                    }
                    Ok(Err(e)) => {
                        warn!(error = %e, "vamp: scraper error (will retry)");
                    }
                    Err(e) => {
                        warn!(error = %e, "vamp: scraper task join error");
                    }
                }

                let interval = state.config().await.scrape_interval_secs.max(60);
                debug!(sleep_secs = interval, "vamp: scraper sleeping");
                sleep(Duration::from_secs(interval)).await;
            }
        });
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // The spawned task is best-effort; toggling scrape_enabled to false
        // via the API is the documented way to pause it.
        Ok(())
    }
}

fn urlencoding_encode(s: &str) -> String {
    // tiny encoder — we only encode the trend name as a query param. No
    // dependency on the `urlencoding` crate.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}
