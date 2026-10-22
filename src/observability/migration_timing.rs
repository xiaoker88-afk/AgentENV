use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::info;

pub(crate) const MARKER: &str = "AENV_MIGRATION_STAGE";

#[derive(Serialize)]
struct MigrationStageEvent<'a> {
    schema: &'static str,
    timestamp_unix_ms: u128,
    component: &'static str,
    stage: &'a str,
    duration_ms: u128,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    sandbox_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
}

/// Emit one stable, machine-readable timing record into the normal tracing sink.
///
/// The marker lets the offline report tool extract these records from plain
/// systemd journal exports without depending on the selected tracing formatter.
pub(crate) fn log_stage(
    stage: &str,
    duration: Duration,
    status: &str,
    sandbox_id: Option<&str>,
    snapshot_id: Option<&str>,
    backend: Option<&str>,
    source: Option<&str>,
    size_bytes: Option<u64>,
) {
    let event = MigrationStageEvent {
        schema: "agentenv.migration-stage.v1",
        timestamp_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        component: "runtime",
        stage,
        duration_ms: duration.as_millis(),
        status,
        sandbox_id,
        snapshot_id,
        backend,
        source,
        size_bytes,
    };
    match serde_json::to_string(&event) {
        Ok(json) => info!(target: "agentenv::migration_timing", "{MARKER} {json}"),
        Err(error) => {
            info!(target: "agentenv::migration_timing", %error, "failed to serialize migration timing event")
        }
    }
}
