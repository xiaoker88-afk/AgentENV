//! P2P transport operation metrics (M4).
//!
//! Both the iroh and urma backends record every `lookup`/`fetch*` call as a
//! histogram sample (`agentenv_p2p_operation_duration_seconds`, labeled by
//! backend) and completed transfers as byte counters
//! (`agentenv_p2p_fetch_bytes_total`). The p95 comparison between the
//! urma and iroh data paths is a simple histogram_quantile(0.95) over the
//! backend label.
//!
//! Note: for `fetch_byte_range` the sample covers call setup plus the
//! stream's *first* poll window only where the transport is synchronous;
//! streaming time is attributed via the byte counters.
//!
//! Data-plane granularity (urma one-sided READ path): every bounce-window
//! `read_range` call is recorded as a latency sample plus bytes
//! (`agentenv_p2p_read_window_seconds` / `..._read_window_bytes_total`),
//! and every remote chunk stream records its end-to-end duration, bytes and
//! terminal status (`agentenv_p2p_stream_duration_seconds` /
//! `..._stream_bytes_total`, `status` = ok|error|dropped) — the streamed
//! `fetch_byte_range` path is otherwise invisible to the op timer, which
//! only covers setup.
//!
//! p95 comparison (Prometheus):
//!
//! ```text
//! histogram_quantile(0.95,
//!   sum by (backend, le) (
//!     rate(agentenv_p2p_operation_duration_seconds_bucket[5m])
//!   )
//! )
//! ```
//! filtered to `op="fetch_byte_range"` for the M3 acceptance comparison
//! between `backend="urma"` and `backend="iroh"`.
use std::cell::Cell;
use std::future::Future;
use std::time::Instant;

pub(crate) const OP_LOOKUP: &str = "lookup";
pub(crate) const OP_FETCH: &str = "fetch";
pub(crate) const OP_FETCH_BYTES: &str = "fetch_bytes";
pub(crate) const OP_FETCH_RANGE: &str = "fetch_byte_range";

/// Drop-guard timing one P2P operation. Records the elapsed time under
/// `agentenv_p2p_operation_duration_seconds` when dropped; call
/// [`OpTimer::succeed`] before returning `Ok` to label the sample `ok`
/// (samples dropped without `succeed` are labeled `error`).
pub(crate) struct OpTimer {
    backend: &'static str,
    op: &'static str,
    start: Instant,
    status: Cell<&'static str>,
}

impl OpTimer {
    pub(crate) fn start(backend: &'static str, op: &'static str) -> Self {
        Self {
            backend,
            op,
            start: Instant::now(),
            status: Cell::new("error"),
        }
    }

    pub(crate) fn succeed(&self) {
        self.status.set("ok");
    }
}

impl Drop for OpTimer {
    fn drop(&mut self) {
        metrics::histogram!(
            "agentenv_p2p_operation_duration_seconds",
            "backend" => self.backend,
            "op" => self.op,
            "status" => self.status.get(),
        )
        .record(self.start.elapsed().as_secs_f64());
    }
}

/// Count transferred bytes for a completed fetch.
pub(crate) fn record_fetch_bytes(backend: &'static str, bytes: u64) {
    metrics::counter!(
        "agentenv_p2p_fetch_bytes_total",
        "backend" => backend,
    )
    .increment(bytes);
}

/// Record one one-sided READ window: a single `read_range` call pulling at
/// most one bounce buffer of bytes (internally chunked into serial READ
/// WRs). This is the innermost data-plane primitive; throughput is
/// `read_window_bytes_total / read_window_seconds` (sum over windows).
pub(crate) fn record_read_window(backend: &'static str, bytes: u64, duration_secs: f64) {
    metrics::histogram!(
        "agentenv_p2p_read_window_seconds",
        "backend" => backend,
    )
    .record(duration_secs);
    metrics::counter!(
        "agentenv_p2p_read_window_bytes_total",
        "backend" => backend,
    )
    .increment(bytes);
}

/// Record one remote chunk stream lifecycle (setup through the last
/// delivered chunk): end-to-end duration, delivered bytes and terminal
/// status. Covers the streamed `fetch_byte_range` path whose op timer only
/// captures call setup.
pub(crate) fn record_stream(
    backend: &'static str,
    status: &'static str,
    bytes: u64,
    duration_secs: f64,
) {
    metrics::histogram!(
        "agentenv_p2p_stream_duration_seconds",
        "backend" => backend,
        "status" => status,
    )
    .record(duration_secs);
    metrics::counter!(
        "agentenv_p2p_stream_bytes_total",
        "backend" => backend,
        "status" => status,
    )
    .increment(bytes);
}

/// Decorator recording operation metrics for any [`P2pTransport`] leg
/// (used for the iroh backend; the urma backend instruments itself).
pub(crate) struct InstrumentedP2pTransport {
    backend: &'static str,
    inner: std::sync::Arc<dyn super::transport::P2pTransport>,
}

impl InstrumentedP2pTransport {
    pub(crate) fn new(
        backend: &'static str,
        inner: std::sync::Arc<dyn super::transport::P2pTransport>,
    ) -> Self {
        Self { backend, inner }
    }

    async fn time<T, E>(
        &self,
        op: &'static str,
        future: impl Future<Output = std::result::Result<T, E>>,
    ) -> std::result::Result<T, E> {
        let timer = OpTimer::start(self.backend, op);
        let result = future.await;
        if result.is_ok() {
            timer.succeed();
        }
        result
    }
}

#[async_trait::async_trait]
impl super::transport::P2pTransport for InstrumentedP2pTransport {
    async fn lookup_with_hints(
        &self,
        key: &super::types::P2pArtifactKey,
        hints: &[super::types::P2pArtifactProviderHint],
    ) -> super::error::Result<Option<super::types::P2pArtifactDescriptor>> {
        self.time(OP_LOOKUP, self.inner.lookup_with_hints(key, hints))
            .await
    }

    async fn fetch_with_options(
        &self,
        descriptor: &super::types::P2pArtifactDescriptor,
        destination: &std::path::Path,
        _options: super::types::P2pFetchOptions,
    ) -> super::error::Result<u64> {
        let result = self.time(OP_FETCH, self.inner.fetch(descriptor, destination)).await;
        if let Ok(size) = result.as_ref() {
            record_fetch_bytes(self.backend, *size);
        }
        result
    }

    async fn fetch_bytes_with_options(
        &self,
        descriptor: &super::types::P2pArtifactDescriptor,
        _options: super::types::P2pFetchOptions,
    ) -> super::error::Result<bytes::Bytes> {
        let result = self.time(OP_FETCH_BYTES, self.inner.fetch_bytes(descriptor)).await;
        if let Ok(bytes) = result.as_ref() {
            record_fetch_bytes(self.backend, bytes.len() as u64);
        }
        result
    }

    async fn fetch_byte_range(
        &self,
        descriptor: &super::types::P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> super::error::Result<super::transport::P2pByteStream> {
        self.time(
            OP_FETCH_RANGE,
            self.inner.fetch_byte_range(descriptor, offset, len),
        )
        .await
    }

    async fn prepare_publish(
        &self,
        request: &super::types::P2pPublishRequest,
    ) -> super::error::Result<Option<Box<dyn super::transport::PreparedP2pPublication>>> {
        self.inner.prepare_publish(request).await
    }

    async fn publish(&self, request: &super::types::P2pPublishRequest) -> super::error::Result<()> {
        self.inner.publish(request).await
    }

    async fn unpublish(&self, key: &super::types::P2pArtifactKey) -> super::error::Result<bool> {
        self.inner.unpublish(key).await
    }

    fn local_endpoint(&self) -> Option<super::types::P2pEndpoint> {
        self.inner.local_endpoint()
    }

    async fn shutdown(&self) -> super::error::Result<()> {
        self.inner.shutdown().await
    }
}
