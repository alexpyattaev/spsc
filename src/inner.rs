//! Shared state behind the SPSC channel.
//!
//! `Inner<T>` lives inside an `Arc<Inner<T>>` shared by `Producer` and
//! `Consumer`. The buffer is a `Box<[UnsafeCell<MaybeUninit<T>>]>` of fixed,
//! power-of-two length. Indices are computed based on monothonic counters,
//! wrapped around the channel length via bitmasking. This way ringbuffer
//! math becomes trivial.

use crate::atomic_waker::AtomicWaker;
use crate::sync::{AtomicBool, AtomicUsize};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

/// Crude cache-line padding — 64 covers x86_64, other arch may need more/less.
#[repr(align(64))]
pub(crate) struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

pub(crate) struct Inner<T> {
    /// Total capacity of the ringbuffer
    pub capacity: usize,
    /// Mask applied to counters to get actual indices (function of capacity)
    pub mask: usize,
    /// Actual ringbuffer
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,

    /// Producer's monotonic write counter. Index = `write & mask`.
    pub write: CachePadded<AtomicUsize>,
    /// Consumer's monotonic read counter. Index = `read & mask`.
    pub read: CachePadded<AtomicUsize>,

    /// Registered by the consumer when it parks in `poll_readable`;
    /// woken by the producer in `publish_write` whenever
    /// `consumer_wake_pending` is observed `true`. Cache-line padded
    /// because every `register`/`take` is an AcqRel RMW on the waker
    /// state, and we don't want that to invalidate lines holding
    /// `mask` / `capacity` / the other waker.
    pub consumer_waker: CachePadded<AtomicWaker>,
    /// Registered by the producer when it parks in `poll_writable`;
    /// woken by the consumer in `publish_read` whenever
    /// `producer_wake_pending` is observed `true`. Cache-line padded
    /// for the same reason as `consumer_waker`.
    pub producer_waker: CachePadded<AtomicWaker>,

    /// Set by the consumer just before it parks (in `poll_readable`),
    /// cleared by the producer when it consumes the wake event in
    /// `publish_write`. Both accesses are SeqCst - relaxing them is
    /// not justified here.
    pub consumer_wake_pending: AtomicBool,
    /// Set by the producer just before it parks (in `poll_writable`),
    /// cleared by the consumer when it consumes the wake event in
    /// `publish_read`. Same SeqCst pattern as
    /// `consumer_wake_pending` but with the roles flipped: when the
    /// producer parks (flag set), the consumer's next commit must
    /// observe the flag and call wake.
    pub producer_wake_pending: AtomicBool,

    /// Cleared when the producer is dropped.
    pub producer_alive: AtomicBool,
    /// Cleared when the consumer is dropped.
    pub consumer_alive: AtomicBool,
}

impl<T> Inner<T> {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        debug_assert!(
            cap.is_power_of_two() && cap > 0,
            "capacity must be a power of two and > 0"
        );
        let mut v = Vec::with_capacity(cap);
        v.resize_with(cap, || UnsafeCell::new(MaybeUninit::<T>::uninit()));
        Self {
            capacity: cap,
            mask: cap - 1, // ones for bits we use as indices
            buffer: v.into_boxed_slice(),
            write: CachePadded(AtomicUsize::new(0)),
            read: CachePadded(AtomicUsize::new(0)),
            consumer_waker: CachePadded(AtomicWaker::new()),
            producer_waker: CachePadded(AtomicWaker::new()),
            consumer_wake_pending: AtomicBool::new(false),
            producer_wake_pending: AtomicBool::new(false),
            producer_alive: AtomicBool::new(true),
            consumer_alive: AtomicBool::new(true),
        }
    }

    /// Pointer to slot `idx` (caller must keep `idx < capacity`).
    ///
    /// SAFETY: caller must respect SPSC ordering — only the producer may
    /// write to a slot in `[write, write + free)` and only the consumer may
    /// read from a slot in `[read, read + len)`; these ranges never overlap.
    #[inline]
    pub(crate) unsafe fn slot_ptr(&self, idx: usize) -> *mut MaybeUninit<T> {
        debug_assert!(idx < self.capacity, "slot index out of bounds");
        // SAFETY: `idx < capacity == buffer.len()`.
        unsafe { self.buffer.get_unchecked(idx) }.get()
    }

    /// Base pointer of the buffer as `*mut MaybeUninit<T>`. Valid because
    /// `UnsafeCell<X>` has the same layout as `X`.
    #[inline]
    pub(crate) fn buffer_base_ptr(&self) -> *mut MaybeUninit<T> {
        self.buffer.as_ptr() as *mut UnsafeCell<MaybeUninit<T>> as *mut MaybeUninit<T>
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Last Arc reference: both ends are gone. Drop any element still
        // sitting in `[read, write)`.
        let read = *self.read.0.get_mut();
        let write = *self.write.0.get_mut();
        let mut idx = read;
        while idx != write {
            let slot = idx & self.mask;
            // SAFETY: producer wrote this slot and consumer never advanced
            // past it; nobody else can touch it now.
            unsafe {
                self.buffer
                    .get_unchecked_mut(slot)
                    .get_mut()
                    .assume_init_drop();
            }
            idx = idx.wrapping_add(1);
        }
    }
}

// SAFETY: SPSC discipline + UnsafeCell prevents racing access. We hand out
// `&mut [MaybeUninit<T>]` / `&[T]` only to disjoint slot ranges.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

#[cfg(all(test, not(feature = "shuttle-test")))]
mod tests {
    use super::*;

    #[test]
    fn cache_padded_alignment() {
        assert_eq!(std::mem::align_of::<CachePadded<u8>>(), 64);
    }

    #[test]
    fn buffer_base_offset_eq_slot_ptr() {
        let inner = Inner::<u32>::with_capacity(8);
        let base = inner.buffer_base_ptr();
        for i in 0..8 {
            // SAFETY: i < capacity.
            let p = unsafe { inner.slot_ptr(i) };
            assert_eq!(p, unsafe { base.add(i) });
        }
    }
}
