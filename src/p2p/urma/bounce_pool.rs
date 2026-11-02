//! Pre-registered local buffers used as one-sided READ destinations.

use std::alloc::{alloc, dealloc, Layout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::context::UrmaContext;
use super::ops::RegisteredSeg;
use crate::p2p::error::{Error, Result};

struct BounceBuffer {
    addr: usize,
    layout: Layout,
    registered: RegisteredSeg,
}

/// Fixed-size pool whose buffers remain registered for the transport lifetime.
pub struct BounceBufferPool {
    context: Arc<UrmaContext>,
    buffers: Vec<BounceBuffer>,
    available: Mutex<Vec<usize>>,
    permits: Arc<Semaphore>,
    buffer_len: u64,
    closed: AtomicBool,
}

impl BounceBufferPool {
    pub fn new(context: Arc<UrmaContext>, size: usize, buffer_len: u64) -> Result<Arc<Self>> {
        let size = size.max(1);
        let buffer_len = buffer_len.max(4096);
        let alloc_len = usize::try_from(buffer_len).map_err(|_| {
            Error::Internal(anyhow::anyhow!(
                "urma: bounce buffer size {buffer_len} does not fit usize"
            ))
        })?;
        let layout = Layout::from_size_align(alloc_len, 4096).map_err(|err| {
            Error::internal_message("urma bounce buffer layout", err)
        })?;
        let mut buffers = Vec::with_capacity(size);

        for _ in 0..size {
            let addr = unsafe { alloc(layout) as usize };
            if addr == 0 {
                Self::release_buffers(&context, &mut buffers);
                return Err(Error::Internal(anyhow::anyhow!(
                    "urma: bounce buffer allocation failed"
                )));
            }
            unsafe { std::ptr::write_bytes(addr as *mut u8, 0, alloc_len) };
            let registered = match context
                .ops()
                .register_local_seg(context.handle(), addr, buffer_len)
            {
                Ok(registered) => registered,
                Err(err) => {
                    unsafe { dealloc(addr as *mut u8, layout) };
                    Self::release_buffers(&context, &mut buffers);
                    return Err(Error::internal_message("urma register bounce pool", err));
                }
            };
            buffers.push(BounceBuffer {
                addr,
                layout,
                registered,
            });
        }

        Ok(Arc::new(Self {
            context,
            buffers,
            available: Mutex::new((0..size).rev().collect()),
            permits: Arc::new(Semaphore::new(size)),
            buffer_len,
            closed: AtomicBool::new(false),
        }))
    }

    pub fn buffer_len(&self) -> u64 {
        self.buffer_len
    }

    pub async fn lease(self: &Arc<Self>) -> Result<BounceBufferLease> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal(anyhow::anyhow!("urma: bounce buffer pool closed")))?;
        let index = self
            .available
            .lock()
            .unwrap()
            .pop()
            .expect("bounce pool permit must correspond to an available buffer");
        Ok(BounceBufferLease {
            pool: self.clone(),
            index,
            _permit: permit,
        })
    }

    pub fn shutdown(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.permits.close();
        for buffer in &self.buffers {
            let _ = self.context.ops().unregister_seg(buffer.registered.tseg);
        }
    }

    fn release_buffers(context: &UrmaContext, buffers: &mut Vec<BounceBuffer>) {
        for buffer in buffers.drain(..) {
            let _ = context.ops().unregister_seg(buffer.registered.tseg);
            unsafe { dealloc(buffer.addr as *mut u8, buffer.layout) };
        }
    }
}

impl Drop for BounceBufferPool {
    fn drop(&mut self) {
        self.shutdown();
        for buffer in &self.buffers {
            unsafe { dealloc(buffer.addr as *mut u8, buffer.layout) };
        }
    }
}

pub struct BounceBufferLease {
    pool: Arc<BounceBufferPool>,
    index: usize,
    _permit: OwnedSemaphorePermit,
}

impl BounceBufferLease {
    pub fn addr(&self) -> usize {
        self.pool.buffers[self.index].addr
    }

    pub fn registered(&self) -> &RegisteredSeg {
        &self.pool.buffers[self.index].registered
    }
}

impl Drop for BounceBufferLease {
    fn drop(&mut self) {
        self.pool.available.lock().unwrap().push(self.index);
    }
}
