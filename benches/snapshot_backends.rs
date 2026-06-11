//! Comparative benchmarks for the on-disk [`SnapshotStore`] backends:
//! `FjallSnapshot` vs `RocksDbSnapshot`, on the route-fold workload shape the
//! backends are tuned for (clustered `route.svc-NNNNNN.NNNNNNNN` keys, ~200 B
//! values, batched applies, point gets for existing keys, per-service prefix
//! scans).
//!
//! Run with both backends enabled:
//!
//! ```text
//! cargo bench --bench snapshot_backends --features fjall,rocksdb
//! SLIPSTREAM_BENCH_ENTRIES=4000000 cargo bench --bench snapshot_backends --features fjall,rocksdb
//! ```
//!
//! Env knobs: `SLIPSTREAM_BENCH_ENTRIES` (default 1_000_000),
//! `SLIPSTREAM_BENCH_VALUE_BYTES` (default 200), and
//! `SLIPSTREAM_BENCH_CACHE_BYTES` (default: each backend's default cache) for
//! measuring cache-size sensitivity, e.g. 32 MiB vs 2 GiB.
//!
//! Caveats for honest numbers: `TempDir` honors `TMPDIR` — point it at real
//! NVMe, not tmpfs, when disk behavior matters. Criterion's repeated iterations
//! measure *warm-cache* reads, which is fair for cross-backend comparison;
//! cold-cache latency needs a manual run against a freshly opened store.

use std::hint::black_box;
use std::path::Path;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use slipstream::snapshot::SnapshotStore;
use slipstream::{
    FjallConfig, FjallSnapshot, KvEntry, KvUpdate, RocksDbConfig, RocksDbSnapshot, VersionToken,
    WatchCursor,
};
use tempfile::TempDir;

/// Routes per service: keys cluster as `route.svc-{service:06}.{route:08}`.
const ROUTES_PER_SERVICE: usize = 1000;
/// Updates per `apply` batch — the `watch_applied` flush-batch shape.
const APPLY_BATCH: usize = 1024;
/// Pseudo-random pool the entry values are sliced from (so compression sees
/// realistic entropy instead of a constant byte).
const VALUE_POOL_BYTES: usize = 1 << 20;

fn entries() -> usize {
    std::env::var("SLIPSTREAM_BENCH_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

fn value_bytes() -> usize {
    std::env::var("SLIPSTREAM_BENCH_VALUE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

/// Deterministic xorshift64* step — repeatable key/value choice across runs.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn key(i: usize) -> String {
    format!(
        "route.svc-{:06}.{:08}",
        i / ROUTES_PER_SERVICE,
        i % ROUTES_PER_SERVICE
    )
}

fn value_pool() -> Vec<u8> {
    let mut pool = vec![0u8; VALUE_POOL_BYTES];
    let mut state = 0x5EED_5EED_5EED_5EEDu64;
    for chunk in pool.chunks_mut(8) {
        let bytes = next_rand(&mut state).to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    pool
}

fn value_for(pool: &[u8], i: usize, len: usize) -> &[u8] {
    let off = (i * 7919) % (pool.len() - len);
    &pool[off..off + len]
}

/// Fold `n` entries into `store` in `APPLY_BATCH`-sized applies, the way
/// `watch_applied` would during hydration.
fn hydrate<S: SnapshotStore>(store: &mut S, n: usize, pool: &[u8], vlen: usize) {
    let mut batch = Vec::with_capacity(APPLY_BATCH);
    let mut i = 0usize;
    while i < n {
        batch.clear();
        let end = (i + APPLY_BATCH).min(n);
        for j in i..end {
            batch.push(KvUpdate::Put(KvEntry {
                key: key(j),
                value: value_for(pool, j, vlen).to_vec(),
                version: VersionToken::from_u64(j as u64 + 1),
            }));
        }
        store
            .apply(&batch, &WatchCursor::from_u64(end as u64))
            .expect("apply");
        i = end;
    }
}

/// A key shaped like the fold's keys but in a service range that is never
/// hydrated — the absent-key (unknown service) lookup.
fn miss_key(i: usize) -> String {
    format!("route.svc-9{:06}.{:08}", i % 999_999, i % 1000)
}

/// 10k uniform random single-key probes, reported as a latency distribution.
/// Criterion's mean-of-batches hides exactly the tail this exists to surface
/// (a 200 µs p50 with a 100 ms p999 and "everything is 10 ms" have the same
/// mean and opposite fixes).
fn probe_percentiles(label: &str, mut op: impl FnMut(usize)) {
    const PROBES: usize = 10_000;
    let n = entries();
    let mut state = 0x0123_4567_89AB_CDEFu64;
    let mut lat = Vec::with_capacity(PROBES);
    for _ in 0..PROBES {
        let i = (next_rand(&mut state) % n as u64) as usize;
        let t = std::time::Instant::now();
        op(i);
        lat.push(t.elapsed());
    }
    lat.sort();
    let p = |q: f64| lat[(((lat.len() as f64) * q) as usize).min(lat.len() - 1)];
    eprintln!(
        "{label}: p50 {:.1?} / p90 {:.1?} / p99 {:.1?} / p999 {:.1?} / max {:.1?}",
        p(0.50),
        p(0.90),
        p(0.99),
        p(0.999),
        lat[lat.len() - 1],
    );
}

fn cache_bytes() -> Option<u64> {
    std::env::var("SLIPSTREAM_BENCH_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
}

/// Recursive on-disk size of a store directory, for the post-hydration space
/// report (WAL/journal and not-yet-compacted overhead included — this is what
/// the disk actually holds after a hydration).
fn dir_size_bytes(path: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

fn open_fjall(path: &Path) -> FjallSnapshot {
    let mut config = FjallConfig::default();
    if let Some(bytes) = cache_bytes() {
        config.cache_size_bytes = bytes;
    }
    FjallSnapshot::open(path, config).expect("open fjall").1
}

fn open_rocksdb(path: &Path) -> RocksDbSnapshot {
    let mut config = RocksDbConfig::default();
    if let Some(bytes) = cache_bytes() {
        config.cache_size_bytes = bytes;
    }
    RocksDbSnapshot::open(path, config).expect("open rocksdb").1
}

fn bench_apply_hydrate(c: &mut Criterion) {
    let n = entries();
    let vlen = value_bytes();
    let pool = value_pool();

    // Criterion runs at least 10 samples; 10 full hydrations beyond a few
    // million entries is hours. At large scale, skip this group — the read
    // benchmarks' setup (`bench_reads`) times its one-shot hydration of each
    // backend and prints the throughput instead.
    if n > 8_000_000 {
        eprintln!(
            "apply_hydrate: skipped at {n} entries (>8M); see the one-shot \
             hydration timings printed by the read benchmarks' setup"
        );
        return;
    }

    let mut g = c.benchmark_group("apply_hydrate");
    g.sample_size(10);
    g.throughput(Throughput::Elements(n as u64));

    g.bench_function(BenchmarkId::new("fjall", n), |b| {
        b.iter_batched(
            || TempDir::new().unwrap(),
            |dir| {
                let mut s = open_fjall(&dir.path().join("store"));
                hydrate(&mut s, n, &pool, vlen);
                dir
            },
            BatchSize::PerIteration,
        );
    });
    g.bench_function(BenchmarkId::new("rocksdb", n), |b| {
        b.iter_batched(
            || TempDir::new().unwrap(),
            |dir| {
                let mut s = open_rocksdb(&dir.path().join("store"));
                hydrate(&mut s, n, &pool, vlen);
                dir
            },
            BatchSize::PerIteration,
        );
    });
    g.finish();
}

fn bench_reads(c: &mut Criterion) {
    let n = entries();
    let vlen = value_bytes();
    let pool = value_pool();

    // One hydrated store per backend, built outside the timers and shared by
    // every read benchmark below. The hydrations are timed and printed — at
    // large scale this is the apply-throughput measurement (see
    // `bench_apply_hydrate`).
    let fjall_dir = TempDir::new().unwrap();
    let mut fjall = open_fjall(&fjall_dir.path().join("store"));
    let started = std::time::Instant::now();
    hydrate(&mut fjall, n, &pool, vlen);
    let secs = started.elapsed().as_secs_f64();
    let bytes = dir_size_bytes(fjall_dir.path());
    eprintln!(
        "hydrate/fjall: {n} entries in {secs:.1}s ({:.2}M entries/s); on disk \
         {:.2} GiB ({:.0} B/entry)",
        n as f64 / secs / 1e6,
        bytes as f64 / (1u64 << 30) as f64,
        bytes as f64 / n as f64,
    );
    // Settle before reading: a fresh hydration leaves compaction debt that
    // inflates cold reads (the unsettled state is reported separately by the
    // settle duration itself).
    let started = std::time::Instant::now();
    fjall.settle().expect("settle fjall");
    let settled_bytes = dir_size_bytes(fjall_dir.path());
    eprintln!(
        "settle/fjall: {:.1}s; on disk after {:.2} GiB",
        started.elapsed().as_secs_f64(),
        settled_bytes as f64 / (1u64 << 30) as f64,
    );

    let rocks_dir = TempDir::new().unwrap();
    let mut rocks = open_rocksdb(&rocks_dir.path().join("store"));
    let started = std::time::Instant::now();
    hydrate(&mut rocks, n, &pool, vlen);
    let secs = started.elapsed().as_secs_f64();
    let bytes = dir_size_bytes(rocks_dir.path());
    eprintln!(
        "hydrate/rocksdb: {n} entries in {secs:.1}s ({:.2}M entries/s); on disk \
         {:.2} GiB ({:.0} B/entry)",
        n as f64 / secs / 1e6,
        bytes as f64 / (1u64 << 30) as f64,
        bytes as f64 / n as f64,
    );
    let started = std::time::Instant::now();
    rocks.settle().expect("settle rocksdb");
    let settled_bytes = dir_size_bytes(rocks_dir.path());
    eprintln!(
        "settle/rocksdb: {:.1}s; on disk after {:.2} GiB",
        started.elapsed().as_secs_f64(),
        settled_bytes as f64 / (1u64 << 30) as f64,
    );
    let rocks_reader = rocks.reader();

    // --- Cold-read latency distributions (percentiles, not criterion means).
    // 10k uniform random probes each; "miss" keys share the key shape but name
    // services that were never written. Run-order caveat: rocksdb hydrated
    // last, so a slice of its store is page-cache-resident that fjall's isn't.
    probe_percentiles("probe_hit/fjall", |i| {
        let _ = black_box(fjall.get(&key(i)).expect("get"));
    });
    probe_percentiles("probe_miss/fjall", |i| {
        let _ = black_box(fjall.get(&miss_key(i)).expect("get"));
    });
    probe_percentiles("probe_hit/rocksdb", |i| {
        let _ = black_box(rocks.get(&key(i)).expect("get"));
    });
    probe_percentiles("probe_miss/rocksdb", |i| {
        let _ = black_box(rocks.get(&miss_key(i)).expect("get"));
    });

    // --- Point gets for existing keys, uniform over the whole fold. ---
    // CRITICAL: the RNG state must live OUTSIDE the benchmark closure.
    // Criterion re-invokes that closure once per sample (and per warmup pass);
    // state declared inside the body resets to the seed every time, replaying
    // the same key prefix — which the page cache then serves warm. At 250M
    // that artifact reported 1.5 µs "gets" while criterion's own calibration
    // (estimated time / iterations) showed ~860 µs during warmup. Hoisted
    // state keeps every draw fresh across samples, so the measured miss rate
    // is the true one for uniform access over a fold larger than RAM.
    let mut g = c.benchmark_group("get_hit");
    g.throughput(Throughput::Elements(1));
    let mut fjall_state = 0xDEAD_BEEFu64;
    g.bench_function("fjall", |b| {
        b.iter(|| {
            let i = (next_rand(&mut fjall_state) % n as u64) as usize;
            black_box(fjall.get(&key(i)).expect("get"))
        });
    });
    let mut rocks_state = 0xDEAD_BEEFu64;
    g.bench_function("rocksdb", |b| {
        b.iter(|| {
            let i = (next_rand(&mut rocks_state) % n as u64) as usize;
            black_box(rocks.get(&key(i)).expect("get"))
        });
    });
    g.finish();

    // --- One service's routes: the working-set hydration scan. ---
    let mid_service = (n / ROUTES_PER_SERVICE) / 2;
    let prefix = format!("route.svc-{mid_service:06}.");
    let mut g = c.benchmark_group("prefix_scan");
    g.throughput(Throughput::Elements(ROUTES_PER_SERVICE as u64));
    g.bench_function("fjall", |b| {
        b.iter(|| {
            let mut count = 0usize;
            fjall
                .for_each_in_range(&prefix, |e| {
                    count += black_box(e.value.len());
                    Ok(())
                })
                .expect("scan");
            black_box(count)
        });
    });
    g.bench_function("rocksdb", |b| {
        b.iter(|| {
            let mut count = 0usize;
            rocks
                .for_each_in_range(&prefix, |e| {
                    count += black_box(e.value.len());
                    Ok(())
                })
                .expect("scan");
            black_box(count)
        });
    });
    g.finish();

    // --- Batched lookups (rocksdb-only API): one MultiGet vs 100 gets. ---
    // Fresh random keys EVERY iteration, generated in `iter_batched` setup so
    // key construction is excluded from the timing. A fixed key set would be
    // cache-hot after criterion's warmup and measure only per-call overhead —
    // MultiGet exists to coalesce filter/index probes and block reads on
    // *misses*, so the batches must keep missing. Each arm uses a different
    // seed: identical sequences would hand the second arm blocks the first
    // arm just pulled into cache. (Page cache still warms globally as the
    // group runs — both arms drift faster over time, in run order.)
    let make_keys = |state: &mut u64| -> Vec<String> {
        (0..100)
            .map(|_| key((next_rand(state) % n as u64) as usize))
            .collect()
    };
    // RNG state hoisted for the same reason as `get_hit` above: criterion
    // re-invokes the closure per sample, and a body-local seed would replay
    // the same batches into a warmed page cache.
    let mut g = c.benchmark_group("lookup_100");
    g.throughput(Throughput::Elements(100));
    let mut loop_state = 0xFACE_FEEDu64;
    g.bench_function("rocksdb_get_loop", |b| {
        b.iter_batched(
            || make_keys(&mut loop_state),
            |keys| {
                for k in &keys {
                    black_box(rocks_reader.get(k).expect("get"));
                }
            },
            BatchSize::SmallInput,
        );
    });
    let mut mg_state = 0xBADC_0FFEu64;
    g.bench_function("rocksdb_multi_get", |b| {
        b.iter_batched(
            || make_keys(&mut mg_state),
            |keys| {
                black_box(
                    rocks_reader
                        .multi_get(keys.iter().map(String::as_str))
                        .expect("multi_get"),
                )
            },
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

criterion_group!(benches, bench_apply_hydrate, bench_reads);
criterion_main!(benches);
