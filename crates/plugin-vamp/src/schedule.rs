//! Auto-mode driver: when `VampConfig::auto_enabled` is true, repeatedly
//! invoke [`GenerateVampCoinAction::run_once`] on the configured interval.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;
use tracing::{info, warn};

use rustliza_core::error::Result;
use rustliza_core::traits::{Runtime, Service};

use crate::action::GenerateVampCoinAction;
use crate::state::VampState;

pub struct VampScheduleService {
    state: Arc<VampState>,
}

impl VampScheduleService {
    pub fn new(state: Arc<VampState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl Service for VampScheduleService {
    fn name(&self) -> &str {
        "vamp.scheduler"
    }

    async fn start(&self, runtime: Arc<dyn Runtime>) -> Result<()> {
        let state = self.state.clone();
        tokio::spawn(async move {
            let action = GenerateVampCoinAction::new(state.clone());
            loop {
                let cfg = state.config().await;
                if !cfg.auto_enabled {
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }

                info!(interval = cfg.interval_secs, "vamp: auto-generating coin");
                match action.run_once(runtime.as_ref()).await {
                    Ok(coin) => info!(ticker = %coin.ticker, "vamp: auto-mint ok"),
                    Err(e) => warn!(error = %e, "vamp: auto-mint failed"),
                }

                let wait = state.config().await.interval_secs.max(5);
                sleep(Duration::from_secs(wait)).await;
            }
        });
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // Flip auto_enabled=false via the API to actually pause.
        Ok(())
    }
}
