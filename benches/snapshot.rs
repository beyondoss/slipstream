use criterion::{Criterion, criterion_group, criterion_main};
use slipstream::snapshot::{self, SnapshotWriter};
use slipstream::{KvEntry, KvUpdate, VersionToken, WatchCursor};
use tempfile::TempDir;

fn entry(key: &str, value: &[u8], rev: u64) -> KvEntry {
    KvEntry {
        key: key.to_string(),
        value: value.to_vec(),
        version: VersionToken::from_u64(rev),
    }
}

fn cursor(rev: u64) -> WatchCursor {
    WatchCursor::from_u64(rev)
}

fn bench_append(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.snap");

    // Pre-build updates
    let updates: Vec<KvUpdate> = (0..100)
        .map(|i| {
            KvUpdate::Put(entry(
                &format!("node.region-{i}"),
                &vec![0x42; 512],
                i as u64,
            ))
        })
        .collect();

    c.bench_function("append_100_records", |b| {
        b.iter(|| {
            let _ = std::fs::remove_file(&path);
            let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
            for u in &updates {
                w.write_update(u).unwrap();
            }
            w.checkpoint(&cursor(100)).unwrap();
        });
    });
}

fn bench_load(c: &mut Criterion) {
    use criterion::BatchSize;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.snap");

    // 500 unique keys, each rewritten 3× → 1500 records that dedup to 500.
    // This exercises the replay + compaction-on-load path. `load()` rewrites the
    // file in place, so a plain `b.iter` would compact on the first iteration and
    // measure the already-compact fast path forever after (that path is covered
    // separately by `bench_load_compacted`). `iter_batched` restores the bloated
    // log before each timed call, and the restore is excluded from the timing.
    let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
    let mut rev = 0u64;
    for _ in 0..3 {
        for i in 0..500 {
            rev += 1;
            w.write_update(&KvUpdate::Put(entry(
                &format!("node.region-{i}"),
                &vec![0x42; 512],
                rev,
            )))
            .unwrap();
        }
    }
    w.checkpoint(&cursor(rev)).unwrap();
    drop(w);
    let bloated = std::fs::read(&path).unwrap();

    c.bench_function("load_500_entries", |b| {
        b.iter_batched(
            || std::fs::write(&path, &bloated).unwrap(),
            |_| {
                snapshot::load(&path).unwrap().unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_load_compacted(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.snap");

    // Write 500 entries, load once to compact, then bench subsequent loads
    let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
    for i in 0..500 {
        w.write_update(&KvUpdate::Put(entry(
            &format!("node.region-{i}"),
            &vec![0x42; 512],
            i as u64,
        )))
        .unwrap();
    }
    w.checkpoint(&cursor(500)).unwrap();
    drop(w);
    snapshot::load(&path).unwrap(); // compact once

    c.bench_function("load_500_entries_compacted", |b| {
        b.iter(|| {
            snapshot::load(&path).unwrap().unwrap();
        });
    });
}

fn bench_compact(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.snap");

    // Write 500 entries with 10 updates each (5000 records total)
    let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
    let mut rev = 0u64;
    for round in 0..10 {
        for i in 0..500 {
            rev += 1;
            w.write_update(&KvUpdate::Put(entry(
                &format!("node.region-{i}"),
                format!("value-round-{round}").as_bytes(),
                rev,
            )))
            .unwrap();
        }
        w.checkpoint(&cursor(rev)).unwrap();
    }
    drop(w);

    // Benchmark: load (which compacts) from the bloated log
    let bloated = std::fs::read(&path).unwrap();

    c.bench_function("compact_5000_to_500", |b| {
        b.iter(|| {
            std::fs::write(&path, &bloated).unwrap();
            snapshot::load(&path).unwrap().unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_append,
    bench_load,
    bench_load_compacted,
    bench_compact
);
criterion_main!(benches);
