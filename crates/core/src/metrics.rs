use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct PipelineMetrics {
    pub messages_processed: AtomicU64,
    pub messages_skipped: AtomicU64,
    pub total_latency_us: AtomicU64,
    pub compose_latency_us: AtomicU64,
    pub should_respond_latency_us: AtomicU64,
    pub generate_latency_us: AtomicU64,
    pub store_latency_us: AtomicU64,
    pub provider_latency_us: AtomicU64,
    pub api_errors: AtomicU64,
    pub api_retries: AtomicU64,
}

impl PipelineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_message(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_skip(&self) {
        self.messages_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compose(&self, duration_us: u64) {
        self.compose_latency_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_should_respond(&self, duration_us: u64) {
        self.should_respond_latency_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_generate(&self, duration_us: u64) {
        self.generate_latency_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_total(&self, duration_us: u64) {
        self.total_latency_us.fetch_add(duration_us, Ordering::Relaxed);
    }

    pub fn record_api_error(&self) {
        self.api_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.api_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        MetricsSnapshot {
            messages_processed: processed,
            messages_skipped: self.messages_skipped.load(Ordering::Relaxed),
            avg_total_ms: if processed > 0 {
                self.total_latency_us.load(Ordering::Relaxed) / processed / 1000
            } else {
                0
            },
            avg_compose_ms: if processed > 0 {
                self.compose_latency_us.load(Ordering::Relaxed) / processed / 1000
            } else {
                0
            },
            avg_should_respond_ms: if processed > 0 {
                self.should_respond_latency_us.load(Ordering::Relaxed) / processed / 1000
            } else {
                0
            },
            avg_generate_ms: if processed > 0 {
                self.generate_latency_us.load(Ordering::Relaxed) / processed / 1000
            } else {
                0
            },
            api_errors: self.api_errors.load(Ordering::Relaxed),
            api_retries: self.api_retries.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub messages_processed: u64,
    pub messages_skipped: u64,
    pub avg_total_ms: u64,
    pub avg_compose_ms: u64,
    pub avg_should_respond_ms: u64,
    pub avg_generate_ms: u64,
    pub api_errors: u64,
    pub api_retries: u64,
}

pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self { start: Instant::now() }
    }

    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}
