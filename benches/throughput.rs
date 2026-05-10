//! Throughput benchmarks for the SPSC channel.
//!
//! Each scenario streams `ITEMS_PER_ITER` `u64` values through a channel
//! whose allocation is performed in criterion's **setup** phase via
//! `iter_batched`, so the `Arc<Inner<_>>` allocation and counter init are
//! *not* part of the measured interval. The thread (or tokio task) spawn
//! and join are still inside the measured region — that overhead is small
//! relative to 50 000 elements and is comparable across the variants
//! being compared.
//!
//! ## Groups
//!
//! - `one_at_a_time` — element-by-element commit, exercising per-commit
//!   overhead (one SeqCst store + one SeqCst flag-load + at most one
//!   `AtomicWaker::wake` per element).
//!
//! - `bulk_{4,8,16}` — N elements per commit via the bulk handles, which
//!   amortises the per-commit overhead across the batch.
//!
//! - `pure_async` — current-thread tokio runtime, so producer and
//!   consumer tasks alternate on a single OS thread cooperatively via
//!   `.await`. This isolates the cost of the async wake/poll machinery
//!   (waker registration + `consumer_wake_pending` SC pair + Tokio task
//!   bookkeeping) from cross-thread cache contention — handy when
//!   diagnosing regressions to the protocol itself.
//!
//! ## Variants per scenario
//!
//! - `spsc_sync` — `std::thread` producer / consumer using
//!   `try_push` / `try_pop` (or `try_bulk_*`) with `spin_loop` on
//!   Full / Empty. Never parks, so the waker side is essentially free.
//!
//! - `spsc_async` — multi-thread tokio runtime (2 workers), one task per
//!   side, using `bulk_*_async` with `min = 1` (or `min = batch` for
//!   bulk groups). Both halves run in parallel on distinct workers.
//!
//! - `tokio_mpsc` — reference: `tokio::sync::mpsc` with the same runtime.
//!   Producer awaits `send`, consumer drains via `recv` (single) or
//!   `recv_many(buf, batch)` (bulk). Async-only by construction.

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use spsc::{channel, Consumer, Producer};
use std::hint::black_box;
use std::thread;
use tokio::runtime::{Builder, Runtime};

/// Number of u64s pumped through the channel per measured iteration.
const ITEMS_PER_ITER: usize = 50_000;
/// Channel capacity (rounded up to a power of two by `channel`).
const CAPACITY: usize = 1024;

// =====================================================================
// spsc, sync via std::thread
// =====================================================================

/// Producer thread `try_push`-es `n` consecutive `u64`s into `tx`, the
/// consumer thread `try_pop`-s them back. Both spin on Full / Empty.
/// Measures per-element commit + (no-op) wake overhead in the
/// uncontended steady state where neither side ever parks.
fn run_spsc_sync_single(mut tx: Producer<u64>, mut rx: Consumer<u64>, n: usize) {
    let prod = thread::spawn(move || {
        let mut sent = 0u64;
        while (sent as usize) < n {
            if tx.try_push(sent).is_ok() {
                sent += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });
    let mut got = 0usize;
    while got < n {
        match rx.try_pop() {
            Ok(v) => {
                black_box(v);
                got += 1;
            }
            Err(_) => std::hint::spin_loop(),
        }
    }
    prod.join().unwrap();
}

/// Same as `run_spsc_sync_single` but uses `try_bulk_write`/`try_bulk_read`
/// with a fixed `batch` size per handle. Producer fills `batch` slots
/// per commit; consumer drains `batch` per read. Amortises commit
/// overhead across the batch.
fn run_spsc_sync_bulk(mut tx: Producer<u64>, mut rx: Consumer<u64>, n: usize, batch: usize) {
    debug_assert_eq!(n % batch, 0);
    let prod = thread::spawn(move || {
        let mut sent = 0u64;
        while (sent as usize) < n {
            let mut h = loop {
                if let Some(h) = tx.try_bulk_write()
                    && h.capacity() >= batch
                {
                    break h;
                }
                std::hint::spin_loop();
            };
            let (a, b) = h.as_uninit_slices_mut();
            for (i, slot) in a.iter_mut().chain(b.iter_mut()).take(batch).enumerate() {
                slot.write(sent + i as u64);
            }
            h.commit(batch);
            sent += batch as u64;
        }
    });
    let mut got = 0usize;
    while got < n {
        let mut h = loop {
            if let Some(h) = rx.try_bulk_read()
                && h.len() >= batch
            {
                break h;
            }
            std::hint::spin_loop();
        };
        let (a, b) = h.as_slices();
        let mut sum = 0u64;
        for v in a.iter().chain(b.iter()).take(batch) {
            sum = sum.wrapping_add(*v);
        }
        black_box(sum);
        h.consume(batch);
        got += batch;
    }
    prod.join().unwrap();
}

// =====================================================================
// spsc, async — multi-thread runtime
// =====================================================================

/// Tokio-spawned producer task pushes `n` elements via
/// `bulk_write_async(1)`; the consumer drains them via
/// `bulk_read_async(1)`. The minimum-wait-of-1 forces the slow path
/// (waker registration + flag SeqCst pair) on every empty/full
/// transition.
async fn run_spsc_async_single(mut tx: Producer<u64>, mut rx: Consumer<u64>, n: usize) {
    let prod = tokio::spawn(async move {
        for i in 0..(n as u64) {
            let mut h = tx.bulk_write_async(1).await.unwrap();
            let (a, _) = h.as_uninit_slices_mut();
            a[0].write(i);
            h.commit(1);
        }
    });
    let mut got = 0usize;
    while got < n {
        let mut h = rx.bulk_read_async(1).await.unwrap();
        let (a, b) = h.as_slices();
        let mut sum = 0u64;
        for v in a.iter().chain(b.iter()) {
            sum = sum.wrapping_add(*v);
        }
        black_box(sum);
        let len = h.len();
        h.consume(len);
        got += len;
    }
    prod.await.unwrap();
}

/// Bulk async variant: producer writes `batch` elements per
/// `bulk_write_async(batch)`, consumer reads `batch` per
/// `bulk_read_async(batch)`.
async fn run_spsc_async_bulk(
    mut tx: Producer<u64>,
    mut rx: Consumer<u64>,
    n: usize,
    batch: usize,
) {
    debug_assert_eq!(n % batch, 0);
    let prod = tokio::spawn(async move {
        let mut sent = 0u64;
        while (sent as usize) < n {
            let mut h = tx.bulk_write_async(batch).await.unwrap();
            let (a, b) = h.as_uninit_slices_mut();
            for (i, slot) in a.iter_mut().chain(b.iter_mut()).take(batch).enumerate() {
                slot.write(sent + i as u64);
            }
            h.commit(batch);
            sent += batch as u64;
        }
    });
    let mut got = 0usize;
    while got < n {
        let mut h = rx.bulk_read_async(batch).await.unwrap();
        let (a, b) = h.as_slices();
        let mut sum = 0u64;
        for v in a.iter().chain(b.iter()).take(batch) {
            sum = sum.wrapping_add(*v);
        }
        black_box(sum);
        h.consume(batch);
        got += batch;
    }
    prod.await.unwrap();
}

// =====================================================================
// tokio mpsc reference
// =====================================================================

/// Reference: tokio's bounded mpsc channel with one sender + one
/// receiver. Producer awaits `send` per element; consumer awaits
/// `recv`. Shows the cost of a general-purpose async channel.
async fn run_tokio_mpsc_single(
    tx: tokio::sync::mpsc::Sender<u64>,
    mut rx: tokio::sync::mpsc::Receiver<u64>,
    n: usize,
) {
    let prod = tokio::spawn(async move {
        for i in 0..(n as u64) {
            tx.send(i).await.unwrap();
        }
    });
    let mut got = 0usize;
    while got < n {
        let v = rx.recv().await.unwrap();
        black_box(v);
        got += 1;
    }
    prod.await.unwrap();
}

/// Reference: tokio mpsc, single send + bulk receive via `recv_many`.
/// Tokio mpsc has no bulk-send API, so the producer side stays
/// element-at-a-time.
async fn run_tokio_mpsc_bulk(
    tx: tokio::sync::mpsc::Sender<u64>,
    mut rx: tokio::sync::mpsc::Receiver<u64>,
    n: usize,
    batch: usize,
) {
    let prod = tokio::spawn(async move {
        for i in 0..(n as u64) {
            tx.send(i).await.unwrap();
        }
    });
    let mut buf = Vec::with_capacity(batch);
    let mut got = 0usize;
    while got < n {
        buf.clear();
        let received = rx.recv_many(&mut buf, batch).await;
        if received == 0 {
            break;
        }
        let mut sum = 0u64;
        for v in &buf {
            sum = sum.wrapping_add(*v);
        }
        black_box(sum);
        got += received;
    }
    prod.await.unwrap();
}

// =====================================================================
// criterion wiring
// =====================================================================

fn multi_thread_rt() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn current_thread_rt() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

/// One-element-at-a-time across {spsc_sync, spsc_async, tokio_mpsc}.
fn bench_one_at_a_time(c: &mut Criterion) {
    let rt = multi_thread_rt();
    let mut group = c.benchmark_group("one_at_a_time");
    group.throughput(Throughput::Elements(ITEMS_PER_ITER as u64));

    group.bench_function("spsc_sync", |b| {
        b.iter_batched(
            || channel::<u64>(CAPACITY),
            |(tx, rx)| run_spsc_sync_single(tx, rx, ITEMS_PER_ITER),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("spsc_async", |b| {
        b.iter_batched(
            || channel::<u64>(CAPACITY),
            |(tx, rx)| rt.block_on(run_spsc_async_single(tx, rx, ITEMS_PER_ITER)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("tokio_mpsc", |b| {
        b.iter_batched(
            || tokio::sync::mpsc::channel::<u64>(CAPACITY),
            |(tx, rx)| rt.block_on(run_tokio_mpsc_single(tx, rx, ITEMS_PER_ITER)),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Bulk handles for batch sizes 4, 8, 16 across all three variants.
fn bench_bulk(c: &mut Criterion) {
    let rt = multi_thread_rt();
    let mut group = c.benchmark_group("bulk");
    group.throughput(Throughput::Elements(ITEMS_PER_ITER as u64));

    for &batch in &[4usize, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("spsc_sync", batch),
            &batch,
            |b, &batch| {
                b.iter_batched(
                    || channel::<u64>(CAPACITY),
                    |(tx, rx)| run_spsc_sync_bulk(tx, rx, ITEMS_PER_ITER, batch),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("spsc_async", batch),
            &batch,
            |b, &batch| {
                b.iter_batched(
                    || channel::<u64>(CAPACITY),
                    |(tx, rx)| {
                        rt.block_on(run_spsc_async_bulk(tx, rx, ITEMS_PER_ITER, batch))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("tokio_mpsc", batch),
            &batch,
            |b, &batch| {
                b.iter_batched(
                    || tokio::sync::mpsc::channel::<u64>(CAPACITY),
                    |(tx, rx)| {
                        rt.block_on(run_tokio_mpsc_bulk(tx, rx, ITEMS_PER_ITER, batch))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Pure-async cost: producer and consumer tasks run on a
/// **current-thread** runtime, so there is no second OS thread and no
/// cross-thread cache contention. All cost is async machinery (waker
/// register, SC flag pair, tokio yield) plus the SC store on the data
/// counter. Useful for diagnosing regressions to the async protocol
/// itself.
fn bench_pure_async(c: &mut Criterion) {
    let rt = current_thread_rt();
    let mut group = c.benchmark_group("pure_async");
    group.throughput(Throughput::Elements(ITEMS_PER_ITER as u64));

    group.bench_function("spsc_async/1", |b| {
        b.iter_batched(
            || channel::<u64>(CAPACITY),
            |(tx, rx)| rt.block_on(run_spsc_async_single(tx, rx, ITEMS_PER_ITER)),
            BatchSize::SmallInput,
        );
    });
    for &batch in &[4usize, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("spsc_async", batch),
            &batch,
            |b, &batch| {
                b.iter_batched(
                    || channel::<u64>(CAPACITY),
                    |(tx, rx)| {
                        rt.block_on(run_spsc_async_bulk(tx, rx, ITEMS_PER_ITER, batch))
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    // Reference: tokio_mpsc on the same single-threaded runtime.
    group.bench_function("tokio_mpsc/1", |b| {
        b.iter_batched(
            || tokio::sync::mpsc::channel::<u64>(CAPACITY),
            |(tx, rx)| rt.block_on(run_tokio_mpsc_single(tx, rx, ITEMS_PER_ITER)),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// =====================================================================
// 1 GiB payload bandwidth
// =====================================================================

/// 32-byte payload used by the 1 GiB bandwidth bench. Four `f64`s
/// gives a realistic "small struct of doubles" shape and makes the
/// per-element copy a memcpy (no Drop, no inner allocations).
#[derive(Clone, Copy)]
struct Payload([f64; 4]);

const ONE_GIB: u64 = 1 << 30;
const PAYLOAD_SIZE: u64 = std::mem::size_of::<Payload>() as u64;
/// Number of `Payload`s to stream per measured iteration: 33_554_432.
/// 1 GiB / 32 B = exactly `1 << 25`, also evenly divisible by `BATCH`
/// below so the producer/consumer loops never deal with a partial batch.
const PAYLOAD_COUNT: usize = (ONE_GIB / PAYLOAD_SIZE) as usize;
/// Bulk batch size for the SPSC variant. 64 ⇒ ~524 288 commits per iter.
const PAYLOAD_BATCH: usize = 64;
/// Channel slot count for the bandwidth bench. Bigger than the 1024
/// used elsewhere — for the large-payload + bulk-handle case a deeper
/// buffer cushions scheduling jitter without disturbing the L1 hit
/// rate on the counters (those are still cache-padded into their own
/// lines).
const PAYLOAD_CAPACITY: usize = 8192;

/// SPSC: bulk-write and bulk-read 1 GiB worth of `Payload` between
/// two tokio tasks on the multi-thread runtime. Producer writes
/// `PAYLOAD_BATCH` payloads per `bulk_write_async`, consumer reads
/// the same per `bulk_read_async`.
async fn run_spsc_payload_1gib(mut tx: Producer<Payload>, mut rx: Consumer<Payload>) {
    let n = PAYLOAD_COUNT;
    debug_assert_eq!(n % PAYLOAD_BATCH, 0);

    let prod = tokio::spawn(async move {
        let mut sent = 0usize;
        while sent < n {
            let mut h = tx.bulk_write_async(PAYLOAD_BATCH).await.unwrap();
            let (a, b) = h.as_uninit_slices_mut();
            for (i, slot) in a
                .iter_mut()
                .chain(b.iter_mut())
                .take(PAYLOAD_BATCH)
                .enumerate()
            {
                slot.write(Payload([(sent + i) as f64; 4]));
            }
            h.commit(PAYLOAD_BATCH);
            sent += PAYLOAD_BATCH;
        }
    });
    let mut got = 0usize;
    while got < n {
        let mut h = rx.bulk_read_async(PAYLOAD_BATCH).await.unwrap();
        let (a, b) = h.as_slices();
        // Touch one f64 from each payload so the read isn't elided.
        let mut acc = 0.0f64;
        for v in a.iter().chain(b.iter()).take(PAYLOAD_BATCH) {
            acc += v.0[0];
        }
        black_box(acc);
        h.consume(PAYLOAD_BATCH);
        got += PAYLOAD_BATCH;
    }
    prod.await.unwrap();
}

/// Reference: same 1 GiB transfer via `tokio::sync::mpsc`. Producer
/// `send`s one at a time (mpsc has no bulk-send API); consumer drains
/// via `recv_many(buf, PAYLOAD_BATCH)`.
async fn run_tokio_mpsc_payload_1gib(
    tx: tokio::sync::mpsc::Sender<Payload>,
    mut rx: tokio::sync::mpsc::Receiver<Payload>,
) {
    let n = PAYLOAD_COUNT;
    let prod = tokio::spawn(async move {
        for i in 0..n {
            tx.send(Payload([i as f64; 4])).await.unwrap();
        }
    });
    let mut buf = Vec::with_capacity(PAYLOAD_BATCH);
    let mut got = 0usize;
    while got < n {
        buf.clear();
        let r = rx.recv_many(&mut buf, PAYLOAD_BATCH).await;
        if r == 0 {
            break;
        }
        let mut acc = 0.0f64;
        for v in &buf {
            acc += v.0[0];
        }
        black_box(acc);
        got += r;
    }
    prod.await.unwrap();
}

/// One iteration moves exactly 1 GiB of `Payload([f64; 4])` between
/// two tokio tasks. `Throughput::Bytes` reports the result in
/// MiB/s / GiB/s. Sample size lowered (per-iter is ~1 s in release)
/// to keep total bench time bounded.
fn bench_payload_1gib(c: &mut Criterion) {
    let rt = multi_thread_rt();
    let mut group = c.benchmark_group("payload_1gib");
    group.throughput(Throughput::Bytes(ONE_GIB));
    group.sample_size(10);

    group.bench_function("spsc_async", |b| {
        b.iter_batched(
            || channel::<Payload>(PAYLOAD_CAPACITY),
            |(tx, rx)| rt.block_on(run_spsc_payload_1gib(tx, rx)),
            BatchSize::PerIteration,
        );
    });
    group.bench_function("tokio_mpsc", |b| {
        b.iter_batched(
            || tokio::sync::mpsc::channel::<Payload>(PAYLOAD_CAPACITY),
            |(tx, rx)| rt.block_on(run_tokio_mpsc_payload_1gib(tx, rx)),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_one_at_a_time,
    bench_bulk,
    bench_pure_async,
    bench_payload_1gib
);
criterion_main!(benches);
