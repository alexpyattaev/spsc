//! Consumer side of the SPSC channel.

use crate::inner::Inner;
use crate::sync::{Arc, Ordering};
use crate::Closed;
use std::future::poll_fn;
use std::task::{Context, Poll};
use std::time::Duration;

/// Single-consumer half of the channel.
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
    /// Consumer's view of its own monotonic read counter. Always equal to
    /// the value last Release-stored to `inner.read`.
    local_read: usize,
    /// Cached lower-bound on `inner.write`. Refreshed by Acquire load when
    /// the cache claims the buffer is empty.
    cached_write: usize,
}

/// Reason a non-blocking pop failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// Nothing available right now; producer still alive.
    Empty,
    /// Producer dropped and buffer empty.
    Closed,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "channel empty",
            TryRecvError::Closed => "channel closed",
        })
    }
}

impl std::error::Error for TryRecvError {}

impl<T> Consumer<T> {
    pub(crate) fn new(inner: Arc<Inner<T>>) -> Self {
        Self {
            inner,
            local_read: 0,
            cached_write: 0,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Snapshot of available element count (Acquire-loads the producer counter).
    pub fn len(&self) -> usize {
        self.inner
            .write
            .0
            .load(Ordering::Acquire)
            .wrapping_sub(self.local_read)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_producer_alive(&self) -> bool {
        self.inner.producer_alive.load(Ordering::Acquire)
    }

    #[inline]
    fn cached_len(&self) -> usize {
        self.cached_write.wrapping_sub(self.local_read)
    }

    #[inline]
    fn refresh_cached_write(&mut self) {
        self.cached_write = self.inner.write.0.load(Ordering::Acquire);
    }

    /// Try to pop one element.
    pub fn try_pop(&mut self) -> Result<T, TryRecvError> {
        if self.cached_len() == 0 {
            // Order matters: load `producer_alive` BEFORE the write
            // counter. If we observe alive=false, the Acquire load
            // synchronizes-with the producer's `Drop` SC store, and
            // therefore the subsequent `write` load sees the final
            // value. If we did it the other way we could observe a
            // stale write counter together with a fresh alive=false
            // and miss elements that the producer pushed before
            // dropping.
            let alive = self.is_producer_alive();
            self.refresh_cached_write();
            if self.cached_len() == 0 {
                return if alive {
                    Err(TryRecvError::Empty)
                } else {
                    Err(TryRecvError::Closed)
                };
            }
        }
        let idx = self.local_read & self.inner.mask;
        // SAFETY: slot was initialised by the producer (it's in `[read, cached_write)`)
        // and the consumer has exclusive read access here.
        let value = unsafe { (*self.inner.slot_ptr(idx)).assume_init_read() };
        let new_read = self.local_read.wrapping_add(1);
        self.local_read = new_read;
        self.publish_read(new_read);
        Ok(value)
    }

    /// Acquire a read handle viewing all currently-available elements.
    /// Returns `None` if the buffer is empty (regardless of producer liveness;
    /// the consumer should distinguish via `is_producer_alive`).
    pub fn try_bulk_read(&mut self) -> Option<ReadHandle<'_, T>> {
        self.refresh_cached_write();
        let avail = self.cached_len();
        if avail == 0 {
            return None;
        }
        Some(ReadHandle {
            consumer: self,
            available: avail,
            committed: 0,
        })
    }

    /// Park-and-poll until at least `min` elements are available, sleeping
    /// 1 ms between polls. If the producer drops while elements remain, the
    /// handle is returned with whatever's there (possibly fewer than `min`).
    /// Returns `Closed` only if the producer drops and the buffer is empty.
    pub fn bulk_read_blocking(
        &mut self,
        min: usize,
    ) -> Result<ReadHandle<'_, T>, Closed> {
        self.bulk_read_blocking_with(min, Duration::from_millis(1))
    }

    pub fn bulk_read_blocking_with(
        &mut self,
        min: usize,
        poll: Duration,
    ) -> Result<ReadHandle<'_, T>, Closed> {
        assert!(
            min > 0 && min <= self.inner.capacity,
            "min must be in 1..=capacity, got {min} (capacity {})",
            self.inner.capacity
        );
        loop {
            // Load alive first so that a fresh alive=false ⇒ subsequent
            // write load sees the final value (see `try_pop` for the
            // ordering rationale).
            let alive = self.is_producer_alive();
            self.refresh_cached_write();
            let avail = self.cached_len();
            if avail >= min || (avail > 0 && !alive) {
                return Ok(ReadHandle {
                    consumer: self,
                    available: avail,
                    committed: 0,
                });
            }
            if !alive {
                return Err(Closed);
            }
            std::thread::sleep(poll);
        }
    }

    /// Async wait for at least `min` elements. Producer-drop semantics match
    /// `bulk_read_blocking`.
    pub async fn bulk_read_async(
        &mut self,
        min: usize,
    ) -> Result<ReadHandle<'_, T>, Closed> {
        assert!(
            min > 0 && min <= self.inner.capacity,
            "min must be in 1..=capacity, got {min} (capacity {})",
            self.inner.capacity
        );
        poll_fn(|cx| self.poll_readable(cx, min)).await?;
        let avail = self.cached_len();
        Ok(ReadHandle {
            consumer: self,
            available: avail,
            committed: 0,
        })
    }

    fn poll_readable(
        &mut self,
        cx: &mut Context<'_>,
        min: usize,
    ) -> Poll<Result<(), Closed>> {
        // Load alive first (see `try_pop` ordering note).
        let alive = self.is_producer_alive();
        self.refresh_cached_write();
        let avail = self.cached_len();
        if avail >= min {
            return Poll::Ready(Ok(()));
        }
        if !alive {
            return Poll::Ready(if avail > 0 { Ok(()) } else { Err(Closed) });
        }

        // Park: register waker, set wake_pending with SeqCst, then SC
        // recheck the write counter. By the four-way SC pattern (see
        // Producer::publish_write), if we go Pending the producer's
        // next commit must observe our flag and call wake.
        self.inner.consumer_waker.register(cx.waker());
        self.inner
            .consumer_wake_pending
            .store(true, Ordering::SeqCst);

        let alive = self.is_producer_alive();
        self.cached_write = self.inner.write.0.load(Ordering::SeqCst);
        let avail = self.cached_len();
        if avail >= min {
            self.inner
                .consumer_wake_pending
                .store(false, Ordering::Release);
            return Poll::Ready(Ok(()));
        }
        if !alive {
            self.inner
                .consumer_wake_pending
                .store(false, Ordering::Release);
            return Poll::Ready(if avail > 0 { Ok(()) } else { Err(Closed) });
        }
        Poll::Pending
    }

    /// SeqCst-publish the read counter, then SeqCst-check the
    /// producer's wake-pending flag and wake only if set.  See the
    /// matching comment on `Producer::publish_write` for the four-way
    /// SC argument that makes this sound.
    fn publish_read(&self, new_read: usize) {
        self.inner.read.0.store(new_read, Ordering::SeqCst);
        if self.inner.producer_wake_pending.load(Ordering::SeqCst) {
            self.inner
                .producer_wake_pending
                .store(false, Ordering::Release);
            self.inner.producer_waker.wake();
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.inner.consumer_alive.store(false, Ordering::SeqCst);
        self.inner.producer_waker.wake();
    }
}

/// Bulk read handle. Holds an exclusive borrow of the `Consumer`.
///
/// The handle does **not** copy elements — `as_slices` returns shared
/// references into the ringbuffer. `commit(n)` drops the first `n`
/// elements in place and frees their slots for the producer.
///
/// Dropping a handle with uncommitted elements leaves them in the queue,
/// available on the next read.
#[must_use = "ReadHandle does nothing unless commit() is called"]
pub struct ReadHandle<'a, T> {
    consumer: &'a mut Consumer<T>,
    /// Total elements available at handle creation.
    available: usize,
    /// How many have been committed via `commit`.
    committed: usize,
}

impl<'a, T> ReadHandle<'a, T> {
    /// Uncommitted element count.
    #[inline]
    pub fn len(&self) -> usize {
        self.available - self.committed
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Two slice views over the uncommitted region. Second slice is
    /// non-empty only when the region wraps. Logical order: first then
    /// second.
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let cap = self.consumer.inner.capacity;
        let mask = self.consumer.inner.mask;
        let start = self.consumer.local_read & mask;
        let len = self.len();
        let first_len = (cap - start).min(len);
        let second_len = len - first_len;
        let base = self.consumer.inner.buffer_base().cast::<T>();
        // SAFETY: slots `[start..start+first_len) ∪ [0..second_len)` are all
        // within the consumer's exclusive `[read, read+available)` range
        // and contain initialised `T` values. The producer never writes
        // into this range. No `&mut T` is being formed elsewhere.
        unsafe {
            let first = std::slice::from_raw_parts(base.add(start), first_len);
            let second = std::slice::from_raw_parts(base, second_len);
            (first, second)
        }
    }

    /// Drop the first `n` uncommitted elements in place and publish the new
    /// read counter so the producer can reuse those slots. Wakes a parked
    /// producer.
    ///
    /// # Panics
    /// Panics if `n > self.len()`.
    pub fn commit(&mut self, n: usize) {
        let remaining = self.len();
        assert!(
            n <= remaining,
            "commit({n}) exceeds available {remaining}"
        );
        if n == 0 {
            return;
        }
        let mask = self.consumer.inner.mask;
        let start = self.consumer.local_read;
        for i in 0..n {
            let idx = start.wrapping_add(i) & mask;
            // SAFETY: each slot is initialised (in our exclusive range) and
            // we are about to mark it as freed; nobody else can observe it
            // before the publish below.
            unsafe { (*self.consumer.inner.slot_ptr(idx)).assume_init_drop() };
        }
        self.consumer.local_read = start.wrapping_add(n);
        self.committed += n;
        self.consumer.publish_read(self.consumer.local_read);
    }
}
