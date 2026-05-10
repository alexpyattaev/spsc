//! Fixed-capacity async SPSC ringbuffer with bulk read/write handles.
//!
//! ```no_run
//! use spsc::{channel, TryRecvError};
//!
//! let (mut tx, mut rx) = channel::<u32>(16);
//! tx.try_push(42).unwrap();
//! assert_eq!(rx.try_pop(), Ok(42));
//! ```
//!
//! ## Bulk read (zero-copy view)
//!
//! ```no_run
//! # use spsc::channel;
//! # let (mut tx, mut rx) = channel::<u32>(16);
//! # for i in 0..4 { tx.try_push(i).unwrap(); }
//! if let Some(mut handle) = rx.try_bulk_read() {
//!     let (a, b) = handle.as_slices();
//!     let sum: u32 = a.iter().chain(b.iter()).sum();
//!     handle.commit(handle.len());            // drop all
//! }
//! ```
//!
//! ## Bulk write (uninit slot view)
//!
//! ```no_run
//! # use spsc::channel;
//! # let (mut tx, _rx) = channel::<u32>(16);
//! if let Some(mut handle) = tx.try_bulk_write() {
//!     let (a, _b) = handle.as_uninit_slices_mut();
//!     for (i, slot) in a.iter_mut().enumerate() {
//!         slot.write(i as u32);
//!     }
//!     let n = a.len();
//!     handle.commit(n);
//! }
//! ```

#![forbid(unsafe_op_in_unsafe_fn)]

mod atomic_waker;
mod consumer;
mod inner;
mod producer;

use std::sync::Arc;

pub use consumer::{Consumer, ReadHandle, TryRecvError};
pub use producer::{Producer, TrySendError, WriteHandle};

use inner::Inner;

/// Returned when the channel's peer half has been dropped and the operation
/// cannot make progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("channel peer dropped")
    }
}

impl std::error::Error for Closed {}

/// Allocate a bounded SPSC channel.
///
/// `capacity` is rounded up to the next power of two; query the actual
/// capacity via [`Producer::capacity`] / [`Consumer::capacity`].
///
/// # Panics
/// Panics if `capacity == 0`.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "capacity must be > 0");
    let cap = capacity.next_power_of_two();
    let inner = Arc::new(Inner::<T>::with_capacity(cap));
    (Producer::new(inner.clone()), Consumer::new(inner))
}
