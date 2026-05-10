//! Single-registrar `AtomicWaker` vendored inline.
//!
//! Behaviour mirrors the `atomic-waker` crate. Exactly one party calls
//! `register` (the parking side); `wake` may run concurrently. The protocol
//! resolves register/wake races by having `register` deliver the wake itself
//! when it observes that a concurrent `take` ran during its critical section.

use crate::sync::AtomicUsize;
use std::cell::UnsafeCell;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::task::Waker;

const WAITING: usize = 0;
const REGISTERING: usize = 0b01;
const WAKING: usize = 0b10;

pub(crate) struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

impl AtomicWaker {
    pub const fn new() -> Self {
        Self {
            state: AtomicUsize::new(WAITING),
            waker: UnsafeCell::new(None),
        }
    }

    /// Stash `waker`. The single-registrar invariant is required: it is UB to
    /// call `register` from multiple threads concurrently.
    pub fn register(&self, waker: &Waker) {
        match self
            .state
            .compare_exchange(WAITING, REGISTERING, AcqRel, Acquire)
            .unwrap_or_else(|s| s)
        {
            WAITING => {
                // SAFETY: the REGISTERING bit acts as an exclusive lock on `waker`.
                unsafe {
                    let prev = (*self.waker.get()).take();
                    *self.waker.get() = Some(waker.clone());

                    match self.state.compare_exchange(
                        REGISTERING,
                        WAITING,
                        AcqRel,
                        Acquire,
                    ) {
                        Ok(_) => drop(prev),
                        Err(actual) => {
                            // wake() ran between our two CASes; deliver the
                            // wake ourselves.
                            debug_assert_eq!(actual, REGISTERING | WAKING);
                            let now = (*self.waker.get()).take();
                            self.state.swap(WAITING, AcqRel);
                            drop(prev);
                            if let Some(w) = now {
                                w.wake();
                            }
                        }
                    }
                }
            }
            WAKING => {
                // wake() in flight; reschedule the new task immediately.
                waker.wake_by_ref();
            }
            other => {
                debug_assert!(
                    other == REGISTERING || other == REGISTERING | WAKING,
                    "concurrent AtomicWaker::register is UB"
                );
            }
        }
    }

    pub fn wake(&self) {
        if let Some(w) = self.take() {
            w.wake();
        }
    }

    fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKING, AcqRel) {
            WAITING => {
                // SAFETY: we hold the WAKING bit; `waker` is exclusive.
                let w = unsafe { (*self.waker.get()).take() };
                self.state.fetch_and(!WAKING, Release);
                w
            }
            _ => None,
        }
    }
}

// SAFETY: state machine serialises all access to `waker`.
unsafe impl Send for AtomicWaker {}
unsafe impl Sync for AtomicWaker {}
