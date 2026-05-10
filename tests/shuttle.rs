//! Randomized concurrency exploration via [`shuttle`].
//!
//! Build / run with:
//!
//! ```sh
//! cargo test --features shuttle-test --release --test shuttle
//! ```
//!
//! The `shuttle-test` feature swaps `shuttle::sync` atomics + `Arc` into
//! the queue, so every load/store/RMW becomes a scheduling point. Each
//! `check_random` call runs the closure across many random schedules,
//! looking for orderings that violate the SPSC invariants.
//!
//! The default `cargo test` (without the feature) excludes this file.

#![cfg(feature = "shuttle-test")]

use shuttle::sync::Arc;
use shuttle::{check_random, thread};
use spsc::{channel, TryRecvError, TrySendError};
use std::sync::atomic::{AtomicBool, Ordering};

/// Number of randomized schedules per `check_random` invocation.
/// Keep modest — each iteration creates+drops a channel and runs both
/// halves to completion. Bump locally if you suspect a rare race.
const SCHEDULES: usize = 2000;

// ---- Single-element try_push / try_pop ----

#[test]
fn try_push_pop_preserves_order() {
    check_random(
        || {
            const N: u32 = 6;
            let (mut tx, mut rx) = channel::<u32>(4);

            let prod = thread::spawn(move || {
                for i in 0..N {
                    loop {
                        match tx.try_push(i) {
                            Ok(()) => break,
                            Err(TrySendError::Full(_)) => thread::yield_now(),
                            Err(TrySendError::Closed(_)) => panic!("closed"),
                        }
                    }
                }
            });

            let mut got = Vec::with_capacity(N as usize);
            while got.len() < N as usize {
                match rx.try_pop() {
                    Ok(v) => got.push(v),
                    Err(TryRecvError::Empty) => thread::yield_now(),
                    Err(TryRecvError::Closed) => break,
                }
            }
            prod.join().unwrap();

            let expected: Vec<u32> = (0..N).collect();
            assert_eq!(got, expected, "FIFO order violated");
        },
        SCHEDULES,
    );
}

#[test]
fn closed_consumer_observed_by_producer() {
    check_random(
        || {
            let (mut tx, rx) = channel::<u32>(2);
            let saw_closed = Arc::new(AtomicBool::new(false));
            let saw_closed_p = saw_closed.clone();

            let prod = thread::spawn(move || {
                for i in 0..10u32 {
                    match tx.try_push(i) {
                        Ok(()) => {}
                        Err(TrySendError::Closed(_)) => {
                            saw_closed_p.store(true, Ordering::Relaxed);
                            break;
                        }
                        Err(TrySendError::Full(_)) => thread::yield_now(),
                    }
                }
            });

            // Drop receiver first thing.
            drop(rx);
            prod.join().unwrap();
            assert!(
                saw_closed.load(Ordering::Relaxed),
                "producer never observed closed receiver"
            );
        },
        SCHEDULES,
    );
}

#[test]
fn closed_producer_drains_then_closes() {
    check_random(
        || {
            const N: u32 = 4;
            let (mut tx, mut rx) = channel::<u32>(8);

            let prod = thread::spawn(move || {
                for i in 0..N {
                    while tx.try_push(i).is_err() {
                        thread::yield_now();
                    }
                }
                // tx dropped here
            });

            let mut got = Vec::new();
            loop {
                match rx.try_pop() {
                    Ok(v) => got.push(v),
                    Err(TryRecvError::Empty) => thread::yield_now(),
                    Err(TryRecvError::Closed) => break,
                }
            }
            prod.join().unwrap();
            let expected: Vec<u32> = (0..N).collect();
            assert_eq!(got, expected, "elements lost on producer close");
        },
        SCHEDULES,
    );
}

// ---- Bulk handles ----

#[test]
fn bulk_write_read_preserves_order() {
    check_random(
        || {
            const N: u32 = 8;
            let (mut tx, mut rx) = channel::<u32>(4);

            let prod = thread::spawn(move || {
                let mut sent = 0u32;
                while sent < N {
                    if let Some(mut h) = tx.try_bulk_write() {
                        let cap = h.capacity().min((N - sent) as usize);
                        let (a, b) = h.as_uninit_slices_mut();
                        let mut written = 0usize;
                        for slot in a.iter_mut().chain(b.iter_mut()).take(cap) {
                            slot.write(sent + written as u32);
                            written += 1;
                        }
                        h.commit(written);
                        sent += written as u32;
                    } else {
                        thread::yield_now();
                    }
                }
            });

            let mut got: Vec<u32> = Vec::with_capacity(N as usize);
            while got.len() < N as usize {
                if let Some(mut h) = rx.try_bulk_read() {
                    let (a, b) = h.as_slices();
                    got.extend(a.iter().chain(b.iter()).copied());
                    let n = h.len();
                    h.consume(n);
                } else {
                    thread::yield_now();
                }
            }
            prod.join().unwrap();

            let expected: Vec<u32> = (0..N).collect();
            assert_eq!(got, expected, "bulk FIFO order violated");
        },
        SCHEDULES,
    );
}

#[test]
fn capacity_invariant_holds_during_streaming() {
    // The queue must never report negative free or > capacity in flight,
    // i.e. the producer/consumer counters never get out of sync.
    check_random(
        || {
            const N: u32 = 6;
            const CAP: usize = 4;
            let (mut tx, mut rx) = channel::<u32>(CAP);
            assert_eq!(tx.capacity(), CAP);

            let prod = thread::spawn(move || {
                for i in 0..N {
                    while tx.try_push(i).is_err() {
                        thread::yield_now();
                    }
                }
            });

            let mut got = Vec::with_capacity(N as usize);
            while got.len() < N as usize {
                // len() does an Acquire load of write; must always be in
                // [0, CAP] from the consumer's perspective.
                let len = rx.len();
                assert!(len <= CAP, "rx.len() = {len} exceeds capacity {CAP}");
                match rx.try_pop() {
                    Ok(v) => got.push(v),
                    Err(_) => thread::yield_now(),
                }
            }
            prod.join().unwrap();
            assert_eq!(got, (0..N).collect::<Vec<_>>());
        },
        SCHEDULES,
    );
}
