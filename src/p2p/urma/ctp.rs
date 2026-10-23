//! CTP (connection management): dynamic transport-channel establishment.
//!
//! Note: Despite the module name, all transports now use the CTP plane
//! (`tp_type = URMA_CTP`, priority 0) on UB hardware, which supports both
//! one-sided READ/WRITE and two-sided SEND/RECV. The module name is kept
//! for stability; the manager imports remote RM jetties and owns their
//! local submission jetties.
//!
//! URMA RM/CTP transports are set up dynamically at runtime by importing the
//! remote jetty:
//!   1. `urma_import_jetty` — import the remote jetty id (obtained out of
//!      band, e.g. from the scheduler peer discovery / catalog).
//!
//! `urma_bind_jetty` is RC-only. The AgentENV backend uses RM, so it must not
//! bind. The manager keeps one local jetty and imported target jetty per
//! remote EID, caches them, and tears them down after an idle TTL.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::context::UrmaContext;
use super::ops::{JettyHandle, JettyId, TargetJettyHandle};
use crate::p2p::error::{Error as P2pError, Result as P2pResult};

/// An established (or being-established) connection to one remote peer,
/// keyed by the remote EID.
#[derive(Clone)]
pub struct PeerConnection {
    /// Remote EID hex string (cache key).
    pub remote_eid: String,
    /// Local RM jetty used to submit work requests to the imported target.
    pub jetty: JettyHandle,
    /// Imported remote jetty handle.
    pub tjetty: TargetJettyHandle,
    established_at: Instant,
    last_used: Arc<Mutex<Instant>>,
}

impl PeerConnection {
    pub fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_used.lock().unwrap().elapsed()
    }
}

/// Manages per-peer local and imported RM jetties.
pub struct PeerConnectionManager {
    context: Arc<UrmaContext>,
    /// Remote EID hex -> connection.
    connections: Mutex<HashMap<String, PeerConnection>>,
    idle_ttl: Duration,
    connect_timeout: Duration,
}

impl PeerConnectionManager {
    pub fn new(context: Arc<UrmaContext>, connect_timeout: Duration, idle_ttl: Duration) -> Self {
        Self {
            context,
            connections: Mutex::new(HashMap::new()),
            idle_ttl,
            connect_timeout,
        }
    }

    /// Import one remote RM/CTP jetty. This is blocking FFI work and must run
    /// on a blocking thread when called from async code.
    fn import_sync(&self, remote: &JettyId) -> P2pResult<PeerConnection> {
        let key = remote.eid.to_hex();
        let ops = self.context.ops();
        let jetty = self
            .context
            .create_connection_jetty()
            .map_err(|e| P2pError::internal_message("urma connect: create_jetty", e))?;
        let tjetty = match ops.import_jetty(self.context.handle(), remote) {
            Ok(tjetty) => tjetty,
            Err(err) => {
                let _ = ops.delete_jetty(jetty);
                return Err(P2pError::internal_message(
                    "urma connect: import_jetty",
                    err,
                ));
            }
        };

        let conn = PeerConnection {
            remote_eid: key.clone(),
            jetty,
            tjetty,
            established_at: Instant::now(),
            last_used: Arc::new(Mutex::new(Instant::now())),
        };
        Ok(conn)
    }

    fn close_connection(&self, conn: PeerConnection) {
        let ops = self.context.ops();
        let _ = ops.unimport_jetty(conn.tjetty);
        let _ = ops.delete_jetty(conn.jetty);
    }

    /// Import a peer on a blocking thread, bounded by `connect_timeout`.
    ///
    /// A timed-out C FFI call cannot be force-cancelled safely. The blocking
    /// task therefore owns cleanup when its receiver has timed out or the
    /// caller has otherwise been cancelled.
    pub async fn ensure_connection(
        self: &Arc<Self>,
        remote: &JettyId,
    ) -> P2pResult<PeerConnection> {
        let key = remote.eid.to_hex();
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(&key) {
                conn.touch();
                return Ok(conn.clone());
            }
        }
        let remote = *remote;
        let blocking_self = Arc::clone(self);
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let result = blocking_self.import_sync(&remote);
            if let Err(Ok(conn)) = tx.send(result) {
                blocking_self.close_connection(conn);
            }
        });
        let conn = match tokio::time::timeout(self.connect_timeout, rx).await {
            Ok(Ok(result)) => result?,
            Ok(Err(err)) => {
                return Err(P2pError::internal_message("urma connect task join", err));
            }
            Err(_) => {
                return Err(P2pError::Internal(anyhow::anyhow!(
                    "urma connect: import_jetty timed out after {:?}",
                    self.connect_timeout
                )));
            }
        };
        let existing = {
            let mut connections = self.connections.lock().unwrap();
            match connections.get(&key) {
                Some(existing) => Some(existing.clone()),
                None => {
                    connections.insert(key, conn.clone());
                    None
                }
            }
        };
        if let Some(existing) = existing {
            existing.touch();
            self.close_connection(conn);
            Ok(existing)
        } else {
            Ok(conn)
        }
    }

    /// Drop the cached connection for a peer (e.g. after a transport-level
    /// failure) so the next access re-establishes it.
    pub fn invalidate(&self, remote_eid: &str) {
        if let Some(conn) = self.connections.lock().unwrap().remove(remote_eid) {
            self.close_connection(conn);
        }
    }

    /// Tear down connections idle beyond the TTL. Called periodically.
    pub fn reap_idle(&self) {
        let mut connections = self.connections.lock().unwrap();
        let expired: Vec<String> = connections
            .iter()
            .filter(|(_, conn)| conn.idle_for() > self.idle_ttl)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            if let Some(conn) = connections.remove(&key) {
                tracing::debug!("urma: reaping idle connection to {key}");
                self.close_connection(conn);
            }
        }
    }

    /// Shut down every connection.
    pub fn shutdown(&self) {
        let mut connections = self.connections.lock().unwrap();
        for (_, conn) in connections.drain() {
            self.close_connection(conn);
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::super::ops::{Eid, MockUrmaOps};
    use super::*;

    fn remote_id() -> JettyId {
        JettyId {
            eid: Eid([0xaa; 16]),
            uasid: 0x1000,
            id: 7,
        }
    }

    #[tokio::test]
    async fn establishes_and_caches_connection() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let conn = manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
        // Second call hits the cache: no new import.
        let conn2 = manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
        assert_eq!(conn.jetty, conn2.jetty);

        let status = ops.status.lock().unwrap();
        assert_eq!(status.jetties_imported.len(), 1);
        assert_ne!(conn.jetty.0, 0);
    }

    #[tokio::test]
    async fn import_timeout_returns_and_late_handles_are_cleaned_up() {
        let ops = MockUrmaOps::new();
        ops.status.lock().unwrap().import_delay = Duration::from_millis(80);
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_millis(10),
            Duration::from_secs(3600),
        ));

        let err = match manager.ensure_connection(&remote_id()).await {
            Ok(_) => panic!("delayed import unexpectedly succeeded before timeout"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("timed out"));
        assert_eq!(manager.connection_count(), 0);

        tokio::time::sleep(Duration::from_millis(120)).await;
        let status = ops.status.lock().unwrap();
        assert_eq!(status.jetties_unimported.len(), 1);
        assert_eq!(status.deleted_jetties, 1);
    }

    #[tokio::test]
    async fn invalidate_forces_reconnect() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let conn = manager.ensure_connection(&remote_id()).await.unwrap();
        manager.invalidate(&conn.remote_eid);
        assert_eq!(manager.connection_count(), 0);

        manager.ensure_connection(&remote_id()).await.unwrap();
        let status = ops.status.lock().unwrap();
        assert_eq!(status.jetties_imported.len(), 2);
    }

    #[tokio::test]
    async fn reap_idle_tears_down_idle_connections() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        // Very short idle TTL so the connection expires immediately.
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_millis(1),
        ));

        manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);

        // Sleep briefly so idle_for exceeds the 1ms TTL.
        std::thread::sleep(Duration::from_millis(10));
        manager.reap_idle();
        assert_eq!(manager.connection_count(), 0, "idle connection must be reaped");

        // Reconnect after reap: should establish fresh.
        manager.ensure_connection(&remote_id()).await.unwrap();
        assert_eq!(manager.connection_count(), 1);
    }

    #[tokio::test]
    async fn multiple_peers_get_independent_jetties() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let peer_a = JettyId { eid: Eid([0xaa; 16]), uasid: 0x1000, id: 1 };
        let peer_b = JettyId { eid: Eid([0xbb; 16]), uasid: 0x1000, id: 2 };

        let conn_a = manager.ensure_connection(&peer_a).await.unwrap();
        let conn_b = manager.ensure_connection(&peer_b).await.unwrap();
        // Keep one local submission jetty per imported peer.
        assert_ne!(conn_a.jetty, conn_b.jetty);
        assert_eq!(manager.connection_count(), 2);

        // Invalidation of one peer does not affect the other.
        manager.invalidate(&conn_a.remote_eid);
        assert_eq!(manager.connection_count(), 1);
        // conn_b is still usable (touch doesn't panic).
        conn_b.touch();
    }

    #[tokio::test]
    async fn shutdown_closes_all_connections() {
        let ops = MockUrmaOps::new();
        let context = Arc::new(UrmaContext::mock(ops.clone()).unwrap());
        let manager = Arc::new(PeerConnectionManager::new(
            context,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        let peer_a = JettyId { eid: Eid([0xaa; 16]), uasid: 0x1000, id: 1 };
        let peer_b = JettyId { eid: Eid([0xbb; 16]), uasid: 0x1000, id: 2 };
        manager.ensure_connection(&peer_a).await.unwrap();
        manager.ensure_connection(&peer_b).await.unwrap();
        assert_eq!(manager.connection_count(), 2);

        manager.shutdown();
        assert_eq!(manager.connection_count(), 0);

        let status = ops.status.lock().unwrap();
        // RM cleanup unimports and deletes its local submission jetty.
        assert_eq!(status.jetties_unimported.len(), 2);
        assert_eq!(status.deleted_jetties, 2);
    }
}
