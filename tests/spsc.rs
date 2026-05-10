// These tests use the std-atomic build of the queue. Under
// `--features shuttle-test` the queue's atomics come from `shuttle::sync`
// and panic outside `shuttle::check_*` — so this whole file is excluded
// in that mode. See `tests/shuttle.rs` for the shuttle-only test suite.
#![cfg(not(feature = "shuttle-test"))]

use rand::SeedableRng;
use spsc::{channel, Closed, TryRecvError, TrySendError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

#[test]
fn smoke_single_thread() {
    let (mut tx, mut rx) = channel::<u32>(4);
    assert_eq!(tx.capacity(), 4);
    assert_eq!(rx.capacity(), 4);
    assert_eq!(rx.len(), 0);

    tx.try_push(1).unwrap();
    tx.try_push(2).unwrap();
    tx.try_push(3).unwrap();
    tx.try_push(4).unwrap();
    assert!(matches!(tx.try_push(5), Err(TrySendError::Full(5))));

    assert_eq!(rx.try_pop(), Ok(1));
    assert_eq!(rx.try_pop(), Ok(2));
    tx.try_push(5).unwrap();
    tx.try_push(6).unwrap();
    assert!(matches!(tx.try_push(7), Err(TrySendError::Full(7))));

    assert_eq!(rx.try_pop(), Ok(3));
    assert_eq!(rx.try_pop(), Ok(4));
    assert_eq!(rx.try_pop(), Ok(5));
    assert_eq!(rx.try_pop(), Ok(6));
    assert_eq!(rx.try_pop(), Err(TryRecvError::Empty));
}

#[test]
fn capacity_rounds_up() {
    let (tx, _rx) = channel::<u8>(5);
    assert_eq!(tx.capacity(), 8);
}

#[test]
fn cross_thread_streaming() {
    const N: usize = 100_000;
    let (mut tx, mut rx) = channel::<usize>(64);

    let prod = thread::spawn(move || {
        for i in 0..N {
            loop {
                match tx.try_push(i) {
                    Ok(()) => break,
                    Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                    Err(TrySendError::Closed(_)) => panic!("consumer dropped"),
                }
            }
        }
    });

    let mut next = 0usize;
    while next < N {
        match rx.try_pop() {
            Ok(v) => {
                assert_eq!(v, next, "out-of-order delivery");
                next += 1;
            }
            Err(TryRecvError::Empty) => std::hint::spin_loop(),
            Err(TryRecvError::Closed) => panic!("producer dropped early"),
        }
    }
    prod.join().unwrap();
}

#[test]
fn bulk_write_read_wraps_around() {
    let (mut tx, mut rx) = channel::<u32>(8);

    // Fill 5 elements.
    {
        let mut h = tx.try_bulk_write().unwrap();
        let (a, b) = h.as_uninit_slices_mut();
        assert_eq!(a.len() + b.len(), 8);
        for (i, slot) in a.iter_mut().take(5).enumerate() {
            slot.write(100 + i as u32);
        }
        h.commit(5);
    }
    // Drain 4 → read pointer at 4.
    {
        let mut h = rx.try_bulk_read().unwrap();
        assert_eq!(h.len(), 5);
        let (a, b) = h.as_slices();
        assert_eq!([a, b].concat(), vec![100, 101, 102, 103, 104]);
        h.consume(4);
    }
    // Push 6 more → wraps. write was at 5, now at 11; read at 4. len = 7.
    {
        let mut h = tx.try_bulk_write().unwrap();
        assert_eq!(h.capacity(), 7); // 8 - (5 - 4)
        let (a, b) = h.as_uninit_slices_mut();
        // First slice covers slots 5,6,7 (3 elems); second covers 0,1,2,3 (4 elems).
        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 4);
        for (i, s) in a.iter_mut().enumerate() {
            s.write(200 + i as u32);
        }
        for (i, s) in b.iter_mut().enumerate() {
            s.write(300 + i as u32);
        }
        h.commit(7);
    }
    // Read everything.
    let mut got = Vec::new();
    while let Some(mut h) = rx.try_bulk_read() {
        let (a, b) = h.as_slices();
        got.extend(a.iter().copied());
        got.extend(b.iter().copied());
        let n = h.len();
        h.consume(n);
    }
    assert_eq!(
        got,
        vec![104, 200, 201, 202, 300, 301, 302, 303]
    );
}

#[test]
fn read_handle_slices_split_at_wrap() {
    // Force the consumer's live range to cross the buffer-end boundary,
    // then check that `as_slices()` returns the correct (non-empty,
    // ordered) first/second halves.
    //
    // Capacity 4. Push 3, drain 3 → write=3, read=3. Push 4 more (20..24)
    // → write=7, read=3. Live range [3, 7) maps to slot indices 3, 0, 1, 2.
    let (mut tx, mut rx) = channel::<u32>(4);
    for i in 0..3 {
        tx.try_push(10 + i).unwrap();
    }
    for _ in 0..3 {
        rx.try_pop().unwrap();
    }
    for i in 0..4 {
        tx.try_push(20 + i).unwrap();
    }
    let mut h = rx.try_bulk_read().unwrap();
    assert_eq!(h.len(), 4);
    let (a, b) = h.as_slices();
    assert_eq!(a.len(), 1, "expected 1-elem first slice covering slot 3");
    assert_eq!(b.len(), 3, "expected 3-elem second slice covering slots 0..3");
    assert_eq!(a, &[20], "first slice contents in slot 3 = first pushed value");
    assert_eq!(b, &[21, 22, 23], "second slice contents in logical order");
    h.consume(4);
    assert!(rx.try_bulk_read().is_none());
}

#[test]
fn read_handle_slices_no_wrap() {
    // Live range fully within the buffer end ⇒ second slice is empty.
    let (mut tx, mut rx) = channel::<u32>(8);
    for i in 0..5 {
        tx.try_push(i).unwrap();
    }
    let h = rx.try_bulk_read().unwrap();
    let (a, b) = h.as_slices();
    assert_eq!(a, &[0, 1, 2, 3, 4]);
    assert!(
        b.is_empty(),
        "expected empty second slice with no wrap, got {b:?}"
    );
}

#[test]
fn write_handle_uninit_slices_split_at_wrap() {
    // Mirror of `read_handle_slices_split_at_wrap` for the write side.
    // Capacity 4. Push 3, drain 3 → write=3, read=3. Now the producer's
    // 4 free slots are [3, 7) which maps to slot indices 3, 0, 1, 2 —
    // wrap on the first slot.
    let (mut tx, mut rx) = channel::<u32>(4);
    for i in 0..3 {
        tx.try_push(i).unwrap();
    }
    for _ in 0..3 {
        rx.try_pop().unwrap();
    }

    let mut h = tx.try_bulk_write().unwrap();
    assert_eq!(h.capacity(), 4);
    let (a, b) = h.as_uninit_slices_mut();
    assert_eq!(a.len(), 1, "expected 1-elem first slice covering slot 3");
    assert_eq!(b.len(), 3, "expected 3-elem second slice covering slots 0..3");
    a[0].write(100);
    b[0].write(200);
    b[1].write(201);
    b[2].write(202);
    h.commit(4);

    // Pop in logical write order — first slice first, then second.
    assert_eq!(rx.try_pop(), Ok(100));
    assert_eq!(rx.try_pop(), Ok(200));
    assert_eq!(rx.try_pop(), Ok(201));
    assert_eq!(rx.try_pop(), Ok(202));
}

#[test]
fn write_handle_uninit_slices_no_wrap() {
    // Fresh channel: writable region [0, capacity) doesn't wrap.
    let (mut tx, _rx) = channel::<u32>(8);
    let mut h = tx.try_bulk_write().unwrap();
    assert_eq!(h.capacity(), 8);
    let (a, b) = h.as_uninit_slices_mut();
    assert_eq!(a.len(), 8);
    assert!(
        b.is_empty(),
        "expected empty second slice with no wrap, got len {}",
        b.len()
    );
    // No commit — handle drops, slots remain free.
}

#[test]
fn handle_drop_without_commit_is_noop() {
    let (mut tx, mut rx) = channel::<u32>(4);
    tx.try_push(1).unwrap();
    tx.try_push(2).unwrap();
    {
        let h = rx.try_bulk_read().unwrap();
        // Drop without commit.
        drop(h);
    }
    // Elements still there.
    assert_eq!(rx.try_pop(), Ok(1));
    assert_eq!(rx.try_pop(), Ok(2));
}

#[test]
fn closed_consumer_send_fails() {
    let (mut tx, rx) = channel::<u32>(4);
    drop(rx);
    match tx.try_push(1) {
        Err(TrySendError::Closed(1)) => {}
        other => panic!("expected Closed, got {other:?}"),
    }
}

#[test]
fn closed_producer_drains_then_closes() {
    let (mut tx, mut rx) = channel::<u32>(4);
    tx.try_push(10).unwrap();
    tx.try_push(20).unwrap();
    drop(tx);
    assert_eq!(rx.try_pop(), Ok(10));
    assert_eq!(rx.try_pop(), Ok(20));
    assert_eq!(rx.try_pop(), Err(TryRecvError::Closed));
}

#[test]
fn drop_counts_in_flight_and_committed() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct DropCounter(#[allow(dead_code)] u32);
    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    {
        let (mut tx, mut rx) = channel::<DropCounter>(8);
        for i in 0..5 {
            tx.try_push(DropCounter(i)).unwrap();
        }
        // Pop two via try_pop (these drop normally on the consumer side).
        let _ = rx.try_pop().unwrap();
        let _ = rx.try_pop().unwrap();
        assert_eq!(DROPS.load(Ordering::Relaxed), 2);

        // Consume one via bulk handle (drops in place).
        {
            let mut h = rx.try_bulk_read().unwrap();
            assert_eq!(h.len(), 3);
            h.consume(1);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);

        // Leave 2 elements in the queue; channel drop must drop them.
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), 5);
}

// ---- Order preservation under contention ----

/// On average one sleep per ~4096 iterations, of jittered amount up to ~1 ms.
/// With 10M iterations that's ~2400 sleep events per side at ~500 µs mean,
/// well above the per-iter cost of try_push/try_pop, so each sleep lets the
/// other side fully fill or drain the buffer — naturally exercising the
/// "producer faster", "consumer faster", and balanced regimes.
#[inline]
fn maybe_jitter(rng: &mut rand::rngs::SmallRng) {
    use rand::Rng;
    if rng.random_ratio(1, 4096) {
        let us = rng.random_range(0..1024);
        std::thread::sleep(Duration::from_micros(us));
    }
}

#[test]
fn fifo_order_single_element_under_contention() {
    const N: u64 = 10_000_000;
    const CAP: usize = 1024;
    let (mut tx, mut rx) = spsc::channel::<u64>(CAP);

    let prod = thread::spawn(move || {
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xCAFE_BABE_DEAD_BEEF);
        for i in 0..N {
            maybe_jitter(&mut rng);
            while tx.try_push(i).is_err() {
                std::hint::spin_loop();
            }
        }
    });

    let mut rng = rand::rngs::SmallRng::seed_from_u64(0x0123_4567_89AB_CDEF);
    let mut expected = 0u64;
    while expected < N {
        match rx.try_pop() {
            Ok(v) => {
                // assert_eq! formatting on every successful pop would
                // dominate the test runtime; check cheaply, format only
                // on failure.
                if v != expected {
                    panic!("FIFO violation: expected {expected}, got {v}");
                }
                expected += 1;
            }
            Err(_) => std::hint::spin_loop(),
        }
        maybe_jitter(&mut rng);
    }
    prod.join().unwrap();
    assert_eq!(expected, N);
}

#[test]
fn fifo_order_bulk_under_contention() {
    const N: u64 = 10_000_000;
    const CAP: usize = 1024;
    let (mut tx, mut rx) = spsc::channel::<u64>(CAP);

    let prod = thread::spawn(move || {
        use rand::Rng;
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0x1357_9BDF_2468_ACE0);
        let mut next = 0u64;
        while next < N {
            maybe_jitter(&mut rng);
            // Random batch size 1..=16, clamped to remaining.
            let want = (rng.random_range(1..=16) as u64).min(N - next) as usize;

            // Spin until we have a handle with `want` slots free.
            let mut h = loop {
                if let Some(h) = tx.try_bulk_write()
                    && h.capacity() >= want
                {
                    break h;
                }
                std::hint::spin_loop();
            };
            let (a, b) = h.as_uninit_slices_mut();
            for (i, slot) in a.iter_mut().chain(b.iter_mut()).take(want).enumerate() {
                slot.write(next + i as u64);
            }
            h.commit(want);
            next += want as u64;
        }
    });

    use rand::Rng;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(0x2468_ACE0_1357_9BDF);
    let mut expected = 0u64;
    while expected < N {
        maybe_jitter(&mut rng);
        // Random max-read 1..=16 per handle; consumer reads at most that.
        let max_take = rng.random_range(1..=16);
        if let Some(mut h) = rx.try_bulk_read() {
            let (a, b) = h.as_slices();
            let mut consumed = 0usize;
            for &v in a.iter().chain(b.iter()).take(max_take) {
                if v != expected {
                    panic!("FIFO violation: expected {expected}, got {v}");
                }
                expected += 1;
                consumed += 1;
            }
            h.consume(consumed);
        } else {
            std::hint::spin_loop();
        }
    }
    prod.join().unwrap();
    assert_eq!(expected, N);
}

#[test]
fn drop_in_flight_with_counter_wraparound() {
    // Force the monotonic counters past `capacity` so the live range
    // `[read, write)` wraps the buffer end. If `Inner::drop` iterated
    // raw slot indices instead of monotonic counters it would either
    // double-drop or skip elements; if it dropped the wrong slots it
    // would call `assume_init_drop` on uninitialized memory.
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct D(#[allow(dead_code)] u32);
    impl Drop for D {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    {
        let (mut tx, mut rx) = spsc::channel::<D>(4);
        // Push 4, drain 4 → write=4, read=4. No wrap yet but cursors at end.
        for i in 0..4 {
            tx.try_push(D(i)).unwrap();
        }
        for _ in 0..4 {
            let _ = rx.try_pop().unwrap();
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 4);

        // Push 6 with intermediate drains so the live range wraps:
        // write 4→5, drain → read=5; then push 5,6,7,8,9 in batches.
        for i in 4..10 {
            while tx.try_push(D(i)).is_err() {
                // shouldn't happen with this drain pattern, but yield
                std::hint::spin_loop();
            }
            if i < 7 {
                let _ = rx.try_pop().unwrap();
            }
        }
        // State: 6 pushed, 3 drained → 3 remaining (values 7,8,9).
        // local_write = 10, local_read = 7. Live range [7, 10) wraps:
        // slot indices 7&3=3, 8&3=0, 9&3=1.
        assert_eq!(rx.len(), 3);

        // After this point we have 6 + 4 - 1 = 9 drops accumulated
        // (the original 4 + 5 from the in-loop pops).
        assert_eq!(DROPS.load(Ordering::Relaxed), 4 + 3);

        // Drop the channel. Inner::drop must drop the 3 remaining,
        // visiting slots 3, 0, 1 in monotonic order.
    }
    assert_eq!(
        DROPS.load(Ordering::Relaxed),
        10,
        "Inner::drop missed elements across the wrap boundary"
    );
}

#[test]
fn drop_does_not_touch_uncommitted_bulk_write_slots() {
    // The user may write into MaybeUninit slots from a WriteHandle and
    // then drop the handle without committing. Those slots are
    // OUTSIDE [read, write) and the queue must not run Drop on them
    // (it can't tell whether the user actually initialised them).
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct CountOnDrop;
    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    DROPS.store(0, Ordering::Relaxed);
    {
        let (mut tx, _rx) = spsc::channel::<CountOnDrop>(4);
        let mut h = tx.try_bulk_write().unwrap();
        let (a, _b) = h.as_uninit_slices_mut();
        // "Initialise" two slots, then drop the handle without commit.
        // The values written here MUST be considered leaked from the
        // queue's perspective — Inner::drop must not touch slots
        // outside [read, write).
        a[0].write(CountOnDrop);
        a[1].write(CountOnDrop);
        // Handle drops here without commit; local_write unchanged.
    }
    // We deliberately leak two CountOnDrop instances by writing
    // without committing. The queue must NOT have dropped them.
    assert_eq!(
        DROPS.load(Ordering::Relaxed),
        0,
        "Inner::drop touched uncommitted slots — would be UB on partially-initialised data"
    );
}

#[test]
fn drop_either_end_first_works() {
    // Inner::drop runs on the last Arc reference, regardless of which
    // half went away first. Verify both orderings drop the same set.
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug)]
    struct D;
    impl Drop for D {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Producer dropped first.
    DROPS.store(0, Ordering::Relaxed);
    {
        let (mut tx, rx) = spsc::channel::<D>(4);
        tx.try_push(D).unwrap();
        tx.try_push(D).unwrap();
        drop(tx);
        // rx still alive; Inner not yet dropped.
        assert_eq!(DROPS.load(Ordering::Relaxed), 0);
        drop(rx);
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);

    // Consumer dropped first.
    DROPS.store(0, Ordering::Relaxed);
    {
        let (mut tx, rx) = spsc::channel::<D>(4);
        tx.try_push(D).unwrap();
        tx.try_push(D).unwrap();
        drop(rx);
        assert_eq!(DROPS.load(Ordering::Relaxed), 0);
        drop(tx);
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), 2);
}

#[test]
fn blocking_send_unblocks_when_consumer_drains() {
    let (mut tx, mut rx) = channel::<u32>(2);
    tx.try_push(1).unwrap();
    tx.try_push(2).unwrap();

    let consumer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        assert_eq!(rx.try_pop(), Ok(1));
        assert_eq!(rx.try_pop(), Ok(2));
        // Hold rx alive until producer finishes.
        thread::sleep(Duration::from_millis(20));
        rx
    });

    let h = tx.bulk_write_blocking(2).unwrap();
    h.commit(0); // just prove we acquired it
    drop(consumer.join().unwrap());
}

#[test]
fn blocking_send_returns_closed_when_consumer_drops() {
    let (mut tx, rx) = channel::<u32>(1);
    tx.try_push(99).unwrap(); // fill
    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        drop(rx);
    });
    let res = tx.bulk_write_blocking(1);
    assert!(matches!(res, Err(Closed)));
    t.join().unwrap();
}

#[test]
fn blocking_recv_returns_partial_when_producer_drops() {
    let (mut tx, mut rx) = channel::<u32>(8);
    tx.try_push(7).unwrap();
    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        drop(tx);
    });
    let mut h = rx.bulk_read_blocking(4).unwrap();
    assert_eq!(h.len(), 1);
    let (a, _b) = h.as_slices();
    assert_eq!(a, &[7]);
    h.consume(1);
    t.join().unwrap();
    let res = rx.bulk_read_blocking(1);
    assert!(matches!(res, Err(Closed)));
}

// --- async tests ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_roundtrip() {
    const N: usize = 10_000;
    let (mut tx, mut rx) = channel::<usize>(64);

    let prod = tokio::spawn(async move {
        for i in 0..N {
            let mut h = tx.bulk_write_async(1).await.unwrap();
            let (a, _) = h.as_uninit_slices_mut();
            a[0].write(i);
            h.commit(1);
        }
    });

    let cons = tokio::spawn(async move {
        let mut got = 0usize;
        while got < N {
            let mut h = rx.bulk_read_async(1).await.unwrap();
            let (a, b) = h.as_slices();
            for &v in a.iter().chain(b.iter()) {
                assert_eq!(v, got);
                got += 1;
            }
            let n = h.len();
            h.consume(n);
        }
    });

    prod.await.unwrap();
    cons.await.unwrap();
}

#[tokio::test]
async fn async_recv_unblocks_on_close() {
    let (tx, mut rx) = channel::<u32>(4);
    let t = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(tx);
    });
    let res = rx.bulk_read_async(1).await;
    assert!(matches!(res, Err(Closed)));
    t.await.unwrap();
}

#[tokio::test]
async fn async_send_unblocks_on_close() {
    let (mut tx, rx) = channel::<u32>(1);
    tx.try_push(1).unwrap();
    let t = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(rx);
    });
    let res = tx.bulk_write_async(1).await;
    assert!(matches!(res, Err(Closed)));
    t.await.unwrap();
}
