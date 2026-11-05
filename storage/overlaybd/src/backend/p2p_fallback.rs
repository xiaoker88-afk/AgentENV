use crate::io::virtual_file::VirtualFile;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::header::RANGE;
use std::sync::Arc;
use std::time::Instant;

pub struct P2pRangeFile {
    client: reqwest::Client,
    url: String,
    size: u64,
}

impl P2pRangeFile {
    pub fn new(url: String, size: u64) -> Self {
        Self {
            client: reqwest::Client::new(),
            url,
            size,
        }
    }
}

#[async_trait]
impl VirtualFile for P2pRangeFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 || offset >= self.size {
            return Ok(Bytes::new());
        }
        let count = len.min((self.size - offset) as usize);
        let end = offset + count as u64 - 1;
        let response = self
            .client
            .get(&self.url)
            .header(RANGE, format!("bytes={offset}-{end}"))
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            bail!("P2P range request returned status {}", response.status());
        }
        let bytes = response.bytes().await?;
        if bytes.len() != count {
            bail!(
                "P2P range request returned {} bytes, expected {count}",
                bytes.len()
            );
        }
        Ok(bytes)
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("P2P range file is read-only")
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

/// A read-only source that prefers P2P and retries the complete range from the
/// durable backend when P2P is unavailable. The caller may wrap this source in
/// the regular file cache, so bytes from either source share one cache entry.
pub struct P2pFallbackFile {
    p2p: Arc<dyn VirtualFile>,
    fallback: Arc<dyn VirtualFile>,
    digest: String,
    read_path: &'static str,
    size: u64,
}

impl P2pFallbackFile {
    pub fn new(
        p2p: Arc<dyn VirtualFile>,
        fallback: Arc<dyn VirtualFile>,
        digest: String,
        read_path: &'static str,
        size: u64,
    ) -> Self {
        Self {
            p2p,
            fallback,
            digest,
            read_path,
            size,
        }
    }
}

#[async_trait]
impl VirtualFile for P2pFallbackFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let started = Instant::now();
        match self.p2p.read_at(offset, len).await {
            Ok(bytes) => {
                tracing::debug!(
                    digest = %self.digest,
                    offset,
                    bytes = bytes.len(),
                    duration_ms = started.elapsed().as_secs_f64() * 1000.0,
                    source = "p2p",
                    read_path = self.read_path,
                    "overlaybd layer range read"
                );
                Ok(bytes)
            }
            Err(error) => {
                tracing::debug!(
                    digest = %self.digest,
                    offset,
                    bytes = len,
                    error = %error,
                    read_path = self.read_path,
                    "overlaybd P2P layer range failed; falling back to OSS"
                );
                let fallback_started = Instant::now();
                let result = self.fallback.read_at(offset, len).await;
                tracing::debug!(
                    digest = %self.digest,
                    offset,
                    bytes = result.as_ref().map_or(0, Bytes::len),
                    duration_ms = fallback_started.elapsed().as_secs_f64() * 1000.0,
                    source = "oss",
                    read_path = self.read_path,
                    status = if result.is_ok() { "ok" } else { "error" },
                    "overlaybd layer range read"
                );
                result
            }
        }
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("P2P fallback file is read-only")
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestFile {
        data: Bytes,
        fail: bool,
        reads: AtomicUsize,
    }

    #[async_trait]
    impl VirtualFile for TestFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                return Err(anyhow!("injected read failure"));
            }
            let start = usize::try_from(offset).unwrap_or(usize::MAX);
            if start >= self.data.len() {
                return Ok(Bytes::new());
            }
            Ok(self.data.slice(start..(start + len).min(self.data.len())))
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("test file is read-only")
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    #[tokio::test]
    async fn successful_p2p_read_does_not_touch_fallback() {
        let p2p = Arc::new(TestFile {
            data: Bytes::from_static(b"p2p-data"),
            fail: false,
            reads: AtomicUsize::new(0),
        });
        let fallback = Arc::new(TestFile {
            data: Bytes::from_static(b"oss-data"),
            fail: false,
            reads: AtomicUsize::new(0),
        });
        let file = P2pFallbackFile::new(
            p2p.clone(),
            fallback.clone(),
            "sha256:test".to_string(),
            "on_demand",
            8,
        );

        assert_eq!(file.read_at(0, 8).await.unwrap().as_ref(), b"p2p-data");
        assert_eq!(p2p.reads.load(Ordering::Relaxed), 1);
        assert_eq!(fallback.reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn p2p_error_retries_complete_range_from_fallback() {
        let p2p = Arc::new(TestFile {
            data: Bytes::new(),
            fail: true,
            reads: AtomicUsize::new(0),
        });
        let fallback = Arc::new(TestFile {
            data: Bytes::from_static(b"oss-data"),
            fail: false,
            reads: AtomicUsize::new(0),
        });
        let file = P2pFallbackFile::new(
            p2p.clone(),
            fallback.clone(),
            "sha256:test".to_string(),
            "background_download",
            8,
        );

        assert_eq!(file.read_at(0, 8).await.unwrap().as_ref(), b"oss-data");
        assert_eq!(p2p.reads.load(Ordering::Relaxed), 1);
        assert_eq!(fallback.reads.load(Ordering::Relaxed), 1);
    }
}
