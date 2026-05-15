//! Pseudo-vamp coin generator plugin.
//!
//! Two operational pieces wrapped in one rustlizaOS plugin:
//!
//! * [`TrendScraperService`] — drives a headless Chrome against x.com to pull
//!   trending topics and one top tweet per trend, written into the shared
//!   [`VampState`].
//! * [`GenerateVampCoinAction`] — fetches currently-trending memecoins from
//!   DexScreener, pairs one with a curated trend/tweet, asks the configured
//!   model for a parody name/ticker/blurb + funny website domain, writes a
//!   standalone listing HTML to `out/listings/<ticker>.html`, persists the
//!   coin to the agent database, and returns the coin as JSON.
//!
//! A separate [`VampScheduleService`] drives the action on an interval when
//! auto-mode is enabled via the UI.

use std::sync::Arc;

use rustliza_core::Plugin;

pub mod action;
pub mod coins;
pub mod listing;
pub mod schedule;
pub mod scraper;
pub mod state;

pub use action::GenerateVampCoinAction;
pub use coins::{CoinFetcher, TrendingCoin};
pub use schedule::VampScheduleService;
pub use scraper::TrendScraperService;
pub use state::{GeneratedCoin, ScrapedTrend, VampConfig, VampState};

/// Run the full vamp pipeline once. Convenience wrapper used by the HTTP
/// `/vamp/run` endpoint so the server crate doesn't need to know about the
/// action type.
pub async fn generate_now(
    state: Arc<VampState>,
    runtime: &dyn rustliza_core::traits::Runtime,
) -> rustliza_core::error::Result<GeneratedCoin> {
    let action = GenerateVampCoinAction::new(state);
    action.run_once(runtime).await
}

/// Build a `Plugin` bundling the vamp action and the two services. The caller
/// keeps the returned [`VampState`] handle so the server-api layer can read
/// generated coins and flip auto-mode.
pub fn vamp_plugin(state: Arc<VampState>) -> Plugin {
    let mut plugin = Plugin::new("vamp", "Pseudo-vamp parody coin generator");

    plugin
        .actions
        .push(Arc::new(GenerateVampCoinAction::new(state.clone())));

    plugin
        .services
        .push(Arc::new(TrendScraperService::new(state.clone())));

    plugin
        .services
        .push(Arc::new(VampScheduleService::new(state)));

    plugin
}
