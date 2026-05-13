use std::future::Future;
use std::time::Duration;

use crate::error::{Result, RustlizaError};

pub async fn with_retry<F, Fut, T>(max_attempts: u32, f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_err = RustlizaError::Other("no attempts made".into());

    for attempt in 0..max_attempts {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                let is_retryable = matches!(&e,
                    RustlizaError::ModelProvider(msg) if
                        msg.contains("529") ||
                        msg.contains("500") ||
                        msg.contains("502") ||
                        msg.contains("503") ||
                        msg.contains("429") ||
                        msg.contains("request failed") ||
                        msg.contains("stream request failed")
                );

                if !is_retryable || attempt + 1 >= max_attempts {
                    return Err(e);
                }

                let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                tracing::debug!(
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis(),
                    "retrying after error"
                );
                tokio::time::sleep(delay).await;
                last_err = e;
            }
        }
    }

    Err(last_err)
}
