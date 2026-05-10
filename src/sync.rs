//! Synchronization primitives used by the queue.
//!
//! Under the `shuttle-test` feature these come from the [`shuttle`] crate so
//! that randomized scheduling can intercept every load/store/RMW. Otherwise
//! they're just `std::sync` re-exports.
//!
//! `Ordering` is the same `std::sync::atomic::Ordering` enum either way —
//! shuttle uses the std type directly.

#[cfg(feature = "shuttle-test")]
pub(crate) use shuttle::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(feature = "shuttle-test")]
pub(crate) use shuttle::sync::Arc;

#[cfg(not(feature = "shuttle-test"))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(feature = "shuttle-test"))]
pub(crate) use std::sync::Arc;

pub(crate) use std::sync::atomic::Ordering;
