// These tests use the std-atomic build of the queue. Under
// `--features shuttle-test` the queue's atomics come from `shuttle::sync`
// and panic outside `shuttle::check_*` — so this whole file is excluded
// in that mode. See `tests/shuttle.rs` for the shuttle-only test suite.
#![cfg(not(feature = "shuttle-test"))]

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
        h.commit(4);
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
        h.commit(n);
    }
    assert_eq!(
        got,
        vec![104, 200, 201, 202, 300, 301, 302, 303]
    );
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

        // Commit one via bulk handle (drops in place).
        {
            let mut h = rx.try_bulk_read().unwrap();
            assert_eq!(h.len(), 3);
            h.commit(1);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 3);

        // Leave 2 elements in the queue; channel drop must drop them.
    }
    assert_eq!(DROPS.load(Ordering::Relaxed), 5);
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
    h.commit(1);
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
            h.commit(n);
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
