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
//!     handle.consume(handle.len());           // drop all
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
//!
//! ## Assumption: `T::drop` does not panic
//!
//! The queue runs `T`'s destructor in three places:
//!
//! - `Consumer::try_pop` (move-out via `assume_init_read`, then drop on
//!   the popped value).
//! - `ReadHandle::consume(n)` (`drop_in_place` on each of the `n` consumed
//!   slots, in a loop).
//! - `Inner::drop` (drop every element still in `[read, write)` when both
//!   halves are gone).
//!
//! All three paths assume `<T as Drop>::drop` does not unwind. A panic in
//! a `T` destructor inside `ReadHandle::consume` or `Inner::drop` would
//! leak the rest of the in-flight elements (we don't `catch_unwind` the
//! drop loop) and, if it occurs while `Inner::drop` is already running,
//! abort the process (double-panic). This matches the convention that
//! `Drop` impls are infallible — keep payload destructors panic-free.
//! `T`s with potentially-panicking drops should be wrapped in a guard
//! type (`std::mem::ManuallyDrop`, a custom `PanicGuard`, etc.) before
//! being sent through the queue.

#![forbid(unsafe_op_in_unsafe_fn)]

mod atomic_waker;
mod consumer;
mod inner;
mod producer;
mod sync;

use crate::sync::Arc;

pub use consumer::{Consumer, ReadHandle, TryRecvError};
pub use producer::{Producer, TrySendError, WriteHandle};

use inner::Inner;

/// Returned when the channel's peer half has been dropped and the operation
/// cannot make progress.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("channel peer dropped")]
pub struct Closed;

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
