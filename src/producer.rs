//! Producer side of the SPSC channel.

use crate::Closed;
use crate::inner::Inner;
use crate::sync::{Arc, Ordering};
use std::future::poll_fn;
use std::mem::MaybeUninit;
use std::task::{Context, Poll};
use std::time::Duration;

/// Producer half of the channel.
///
/// `try_push` and the bulk write APIs all take `&mut self`, statically
/// guaranteeing the SPSC invariant on the channel.
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
    /// Producer's view of its own monotonic write counter. Always equal to
    /// the value last Release-stored to `inner.write`, which we never
    /// Acquire-load from our own thread.
    local_write: usize,
    /// Cached upper-bound on `inner.read`. Refreshed by Acquire load when
    /// the cache claims the buffer is full.
    cached_read: usize,
}

/// Reason a non-blocking push failed. The unsent value is returned in the
/// error variant so the caller can recover it.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// Buffer full; consumer has not freed enough space.
    #[error("channel full")]
    Full(T),
    /// Consumer has been dropped.
    #[error("channel closed")]
    Closed(T),
}

impl<T> Producer<T> {
    pub(crate) fn new(inner: Arc<Inner<T>>) -> Self {
        Self {
            inner,
            local_write: 0,
            cached_read: 0,
        }
    }

    /// Power-of-two slot count. May exceed the value passed to `channel`.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Snapshot of free slots after refreshing from the consumer's counter.
    pub fn free(&mut self) -> usize {
        self.refresh_cached_read();
        self.cached_free()
    }

    pub fn is_consumer_alive(&self) -> bool {
        self.inner.consumer_alive.load(Ordering::Acquire)
    }

    #[inline]
    fn cached_free(&self) -> usize {
        self.inner.capacity - self.local_write.wrapping_sub(self.cached_read)
    }

    #[inline]
    fn refresh_cached_read(&mut self) {
        self.cached_read = self.inner.read.0.load(Ordering::Acquire);
    }

    /// Try to push one element. Returns the value back on failure.
    ///
    /// Returns `Closed` immediately if the consumer has dropped, regardless
    /// of whether buffer space is available — there's no point storing data
    /// nobody will read. (Race: a consumer drop concurrent with this call
    /// may not be visible; the value then goes into the buffer and is
    /// dropped when the channel itself is destroyed.)
    pub fn try_push(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if !self.is_consumer_alive() {
            return Err(TrySendError::Closed(value));
        }
        if self.cached_free() == 0 {
            self.refresh_cached_read();
            if self.cached_free() == 0 {
                if !self.is_consumer_alive() {
                    return Err(TrySendError::Closed(value));
                }
                return Err(TrySendError::Full(value));
            }
        }
        let idx = self.local_write & self.inner.mask;
        // SAFETY: slot `idx` is in the producer's exclusive [write, write+free)
        // range and is currently uninitialised.
        unsafe { (*self.inner.slot_ptr(idx)).write(value) };
        let new_write = self.local_write.wrapping_add(1);
        self.local_write = new_write;
        self.publish_write(new_write);
        Ok(())
    }

    /// Acquire a write handle exposing all currently-free slots. Returns
    /// `None` if the buffer is full and the consumer is alive, also `None`
    /// if the consumer has dropped (use `is_consumer_alive` to disambiguate).
    pub fn try_bulk_write(&mut self) -> Option<WriteHandle<'_, T>> {
        self.refresh_cached_read();
        let free = self.cached_free();
        if free == 0 {
            return None;
        }
        Some(WriteHandle {
            producer: self,
            free,
        })
    }

    /// Park-and-poll until at least `min` slots are free, sleeping 1 ms
    /// between polls. Returns `Closed` if the consumer drops.
    pub fn bulk_write_blocking(&mut self, min: usize) -> Result<WriteHandle<'_, T>, Closed> {
        self.bulk_write_blocking_with_interval(min, Duration::from_millis(1))
    }

    /// Like [`bulk_write_blocking`] but with a caller-supplied sleep
    /// duration between polls.
    ///
    /// [`bulk_write_blocking`]: Producer::bulk_write_blocking
    pub fn bulk_write_blocking_with_interval(
        &mut self,
        min: usize,
        interval: Duration,
    ) -> Result<WriteHandle<'_, T>, Closed> {
        assert!(
            min > 0 && min <= self.inner.capacity,
            "min must be in 1..=capacity, got {min} (capacity {})",
            self.inner.capacity
        );
        loop {
            self.refresh_cached_read();
            let free = self.cached_free();
            if free >= min {
                return Ok(WriteHandle {
                    producer: self,
                    free,
                });
            }
            if !self.is_consumer_alive() {
                return Err(Closed);
            }
            std::thread::sleep(interval);
        }
    }

    /// Async wait for at least `min` free slots.
    pub async fn bulk_write_async(&mut self, min: usize) -> Result<WriteHandle<'_, T>, Closed> {
        assert!(
            min > 0 && min <= self.inner.capacity,
            "min must be in 1..=capacity, got {min} (capacity {})",
            self.inner.capacity
        );
        poll_fn(|cx| self.poll_writable(cx, min)).await?;
        let free = self.cached_free();
        Ok(WriteHandle {
            producer: self,
            free,
        })
    }

    fn poll_writable(&mut self, cx: &mut Context<'_>, min: usize) -> Poll<Result<(), Closed>> {
        self.refresh_cached_read();
        if self.cached_free() >= min {
            return Poll::Ready(Ok(()));
        }
        if !self.is_consumer_alive() {
            return Poll::Ready(Err(Closed));
        }

        // Park: register waker, set wake_pending with SeqCst, then
        // recheck the read counter with SeqCst. By the four-way SC
        // pattern (consumer's commit does SC-store of `read` + SC-load
        // of producer_wake_pending), if we end up returning Pending,
        // the consumer's next commit must observe our wake_pending
        // flag and call wake.
        self.inner.producer_waker.register(cx.waker());
        self.inner
            .producer_wake_pending
            .store(true, Ordering::SeqCst);

        // SC recheck.
        self.cached_read = self.inner.read.0.load(Ordering::SeqCst);
        if self.cached_free() >= min {
            self.inner
                .producer_wake_pending
                .store(false, Ordering::Release);
            return Poll::Ready(Ok(()));
        }
        if !self.is_consumer_alive() {
            self.inner
                .producer_wake_pending
                .store(false, Ordering::Release);
            return Poll::Ready(Err(Closed));
        }
        Poll::Pending
    }

    /// SeqCst-publish a new write counter, then SeqCst-load the
    /// consumer's wake-pending flag and call wake() only if set.
    ///
    /// Correctness rests on the four-way SC pattern (this store, the
    /// consumer's flag store, this flag load, the consumer's data
    /// recheck load all SeqCst).
    fn publish_write(&self, new_write: usize) {
        self.inner.write.0.store(new_write, Ordering::SeqCst);
        if self.inner.consumer_wake_pending.load(Ordering::SeqCst) {
            // Clear the flag before waking. A racing consumer that
            // re-parks after this clear will set the flag again with
            // SeqCst, so the next commit will see it.
            self.inner
                .consumer_wake_pending
                .store(false, Ordering::Release);
            self.inner.consumer_waker.wake();
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // SeqCst so a concurrent consumer's `is_producer_alive` Acquire
        // load observes us, and `Consumer::try_pop` (which loads alive
        // *before* the write counter) drains correctly.
        self.inner.producer_alive.store(false, Ordering::SeqCst);
        // Always wake the consumer on close — bypasses `consumer_wake_pending`
        // because the closing thread can't reliably observe its state.
        self.inner.consumer_waker.wake();
    }
}

/// Bulk write handle. Holds an exclusive borrow of the `Producer` so at most
/// one is alive at a time. Drop without calling [`commit`] is harmless: no
/// counter advance, no published data, slots remain "free" from the queue's
/// perspective (any `MaybeUninit` writes the user did into the slices are
/// silently discarded).
///
/// [`commit`]: WriteHandle::commit
#[must_use = "WriteHandle does nothing unless commit() is called"]
pub struct WriteHandle<'a, T> {
    producer: &'a mut Producer<T>,
    free: usize,
}

impl<'a, T> WriteHandle<'a, T> {
    /// Number of writable slots in this handle (= sum of the two slice
    /// lengths returned by `as_uninit_slices_mut`).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.free
    }

    /// Two `&mut [MaybeUninit<T>]` slices covering the writable region.
    /// The second slice is non-empty only when the writable region wraps
    /// around the buffer end. Logical write order: first slice, then second.
    pub fn as_uninit_slices_mut(&mut self) -> (&mut [MaybeUninit<T>], &mut [MaybeUninit<T>]) {
        let cap = self.producer.inner.capacity;
        let mask = self.producer.inner.mask;
        let start = self.producer.local_write & mask;
        let first_len = (cap - start).min(self.free);
        let second_len = self.free - first_len;
        let base = self.producer.inner.buffer_base_ptr();
        // SAFETY: `[start..start+first_len) ∪ [0..second_len)` is a subset of
        // the producer's exclusive `[write, write+free)` range. UnsafeCell
        // gives us write provenance through `base`. No shared/overlapping
        // borrows exist on this region (consumer never reaches it).
        unsafe {
            let first = std::slice::from_raw_parts_mut(base.add(start), first_len);
            let second = std::slice::from_raw_parts_mut(base, second_len);
            (first, second)
        }
    }

    /// Inform the queue that the first `n` slots (logical order across both
    /// slices) have been initialised. Publishes a new write counter and
    /// wakes the consumer if parked. Consumes the handle.
    ///
    /// # Panics
    /// Panics if `n > self.capacity()`.
    pub fn commit(self, n: usize) {
        assert!(
            n <= self.free,
            "commit({n}) exceeds writable capacity {}",
            self.free
        );
        if n == 0 {
            return;
        }
        let new_write = self.producer.local_write.wrapping_add(n);
        self.producer.local_write = new_write;
        self.producer.publish_write(new_write);
    }
}
