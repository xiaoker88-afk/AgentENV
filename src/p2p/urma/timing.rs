//! Structured diagnostic timings for migration logs, including failed/cancelled stages.
use std::time::Instant;

pub(super) struct StageTimer {
    key: String,
    stage: &'static str,
    started: Instant,
    status: &'static str,
}

impl StageTimer {
    pub(super) fn start(key: &str, stage: &'static str) -> Self {
        Self {
            key: key.to_owned(),
            stage,
            started: Instant::now(),
            status: "error_or_cancelled",
        }
    }

    pub(super) fn finish(mut self) {
        self.status = "ok";
    }
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        tracing::info!(
            target: "agentenv::p2p::urma::timing",
            key = %self.key,
            stage = self.stage,
            status = self.status,
            duration_ms = self.started.elapsed().as_secs_f64() * 1000.0,
            "urma stage timing"
        );
    }
}
