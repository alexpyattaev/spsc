//! Shared state behind the SPSC channel.
//!
//! `Inner<T>` lives inside an `Arc<Inner<T>>` shared by `Producer` and
//! `Consumer`. The buffer is a `Box<[UnsafeCell<MaybeUninit<T>>]>` of fixed,
//! power-of-two length. Indices are computed as `monotonic_counter & mask`.

use crate::atomic_waker::AtomicWaker;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize};

/// Crude cache-line padding — 64 covers x86_64; AArch64 prefers 128 but 64
/// is a safe lower bound that still avoids false sharing in typical hardware.
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
    pub capacity: usize,
    pub mask: usize,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,

    /// Producer's monotonic write counter. Index = `write & mask`.
    pub write: CachePadded<AtomicUsize>,
    /// Consumer's monotonic read counter. Index = `read & mask`.
    pub read: CachePadded<AtomicUsize>,

    /// Set by the consumer before parking; checked by the producer after
    /// each commit so it can skip waker.wake() when nobody waits.
    pub consumer_needs_wake: AtomicBool,
    /// Symmetric flag for the producer side.
    pub producer_needs_wake: AtomicBool,

    /// Woken when the producer commits or drops.
    pub consumer_waker: AtomicWaker,
    /// Woken when the consumer commits or drops.
    pub producer_waker: AtomicWaker,

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
            mask: cap - 1,
            buffer: v.into_boxed_slice(),
            write: CachePadded(AtomicUsize::new(0)),
            read: CachePadded(AtomicUsize::new(0)),
            consumer_needs_wake: AtomicBool::new(false),
            producer_needs_wake: AtomicBool::new(false),
            consumer_waker: AtomicWaker::new(),
            producer_waker: AtomicWaker::new(),
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
    pub(crate) fn buffer_base(&self) -> *mut MaybeUninit<T> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_padded_alignment() {
        assert_eq!(std::mem::align_of::<CachePadded<u8>>(), 64);
    }

    #[test]
    fn buffer_base_offset_eq_slot_ptr() {
        let inner = Inner::<u32>::with_capacity(8);
        let base = inner.buffer_base();
        for i in 0..8 {
            // SAFETY: i < capacity.
            let p = unsafe { inner.slot_ptr(i) };
            assert_eq!(p, unsafe { base.add(i) });
        }
    }
}
