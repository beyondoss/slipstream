# Snapshot Backends

Four [`SnapshotStore`] backends ship with slipstream. Pick one based on fold size
and read pattern.

## Quick reference

| | `AppendLogSnapshot` | `FjallSnapshot` | `RocksDbSnapshot` | `PedraDbSnapshot` |
|---|---|---|---|---|
| Feature flag | (default) | `fjall` | `rocksdb` | `pedradb` |
| Build deps | none | none | C++ toolchain, libclang | none (pure Rust; git dep today) |
| Fold size | fits in RAM | any | any | any |
| Cold get p50 (500M routes) | n/a | 542 µs | 292 µs | *run `snapshot_backends`* |
| Cold get p999 (500M routes) | n/a | 3.7 ms | 898 µs | *run `snapshot_backends`* |
| Hydrate + settle (500M) | n/a | 40 min | 43 min | *run `snapshot_backends`* |
| Disk per entry (500M) | n/a | 226 B | 245 B | *run `snapshot_backends`* |
| `settle()` cost (500M) | n/a | ~19 min, 2x disk | ~40 s | flush + full compact |

## Choosing

**`AppendLogSnapshot`**: fold fits in RAM. No configuration, no dependencies.

**`FjallSnapshot`**: fold is too large for RAM; build environment is pure Rust;
write throughput matters more than cold-read tail latency.

**`RocksDbSnapshot`**: fold is too large for RAM; cold-read tail latency or settle
time matters; operational tooling (`ldb`, `sst_dump`) is useful.

**`PedraDbSnapshot`**: fold is too large for RAM; want a pure-Rust LSM with a
RocksDB-shaped API ([PedraDB](https://github.com/paulocsanz/pedradb) via
`rocksdb-compat`). Pre-1.0; compare with `cargo bench --bench snapshot_backends
--features fjall,rocksdb,pedradb`.

Total time-to-serving-ready at 500M routes is a wash between fjall and rocksdb
(2602 s fjall, 2593 s rocksdb). The divergence is tail latency and settle cost.
Pedra numbers land in the comparative bench output once you run it.

## Benchmark data

All numbers: 500M routes, ~60 B keys, ~200 B incompressible values, 1 GiB block
cache, NVMe ext4, settled trees. Source: `benches/snapshot_backends.rs`
(`--features fjall,rocksdb,pedradb`).

### Hydration (`apply` path, 1024-update batches)

| | fjall | rocksdb | pedradb |
|---|---|---|---|
| 50M routes | 47 s (1.06 M/s) | 121 s (0.41 M/s) | *bench* |
| 100M routes | 110 s (0.91 M/s) | 344 s (0.29 M/s) | *bench* |
| 250M routes | 480 s (0.52 M/s) | 1165 s (0.21 M/s) | *bench* |
| 500M routes | 1475 s (0.34 M/s) | 2552 s (0.20 M/s) | *bench* |

fjall throughput decays with scale as compaction debt accumulates during
hydration; rocksdb drains that debt concurrently.

### `settle()` after 500M hydration

| | fjall | rocksdb |
|---|---|---|
| Duration | 1127 s | 41 s |
| Peak disk | 203 GiB (2x: old + new generations) | 105 GiB (shrinks: zstd reaches bottom) |
| Mechanism | Full tree rewrite (major compaction) | Drain queued compactions |

**Call `settle()` before serving.** Both engines accumulate compaction debt during
bulk hydration. Without it, cold point-gets are 8-10x slower (measured: rocksdb
~0.9 ms mean unsettled vs 248 µs mean settled at 500M).

### Cold point-gets (settled, uniform random, 10k probes)

| | fjall | rocksdb |
|---|---|---|
| p50 | 542 µs | 292 µs |
| p90 | 757 µs | 485 µs |
| p99 | 1.9 ms | 686 µs |
| p999 | 3.7 ms | 898 µs |
| max | 39.9 ms | 2.5 ms |

### Absent-key lookups (settled, filters reject in RAM)

| | fjall | rocksdb |
|---|---|---|
| p50 | 421 ns | 321 ns |

Both engines build bottom-level filters. `expect_point_read_hits` (fjall) and
`optimize_filters_for_hits` (rocksdb) are disabled in both backends: without
bottom-level filters, absent-key lookups become guaranteed disk probes, and
cold-get latency on unsettled trees spikes to double digits of milliseconds.

### Prefix scans (hot prefix, 1000 entries)

| | fjall | rocksdb |
|---|---|---|
| 50M routes | 189 µs | 122 µs |
| 500M routes | 176 µs | 129 µs |

Scan latency is flat with scale for both engines. This measures a single
service's routes, resident in cache.

### `RocksDbReader::multi_get` vs get-loop (100 cold keys)

| | settled | unsettled |
|---|---|---|
| get-loop | 23.7 ms | 103 ms |
| multi_get | 19.5 ms | 18.5 ms |

`multi_get` overlaps cold block reads the loop pays sequentially. Its edge
grows with compaction debt and cache-miss rate. Against a hot working set,
the loop is faster (marshaling overhead, nothing to coalesce).

## Rerun, Pedra main `60d88f48` (2026-09-24)

Same harness (`benches/snapshot_backends.rs`), this host: 4 vCPU, 16 GiB RAM,
254 GiB ext4, backend-default 1 GiB block cache, 200 B values, 1024-entry
batches. One run per cell. Pedra is `rocksdb-compat` @ `60d88f48`. Fjall and
RocksDB do not link Pedra, so their rows are from the same host and harness.

### 100M routes, settled

| | fjall | rocksdb | pedradb |
|---|---:|---:|---:|
| hydrate | 106 s (0.94 M/s) | 108 s (0.93 M/s) | **98 s (1.02 M/s)** |
| on disk after hydrate | 21.4 GiB (230 B) | 22.7 GiB (243 B) | 23.8 GiB (255 B) |
| settle | 82.5 s → 41.1 GiB | 5.0 s → 21.0 GiB | **2.0 s → 24.0 GiB** |
| cold hit p50 / p99 / p999 | 78 / 129 / 831 µs | 52 / 74 / 399 µs | **25 / 52 / 580 µs** |
| cold miss p50 | 419 ns | **339 ns** | 528 ns |
| warm get_hit | 37.3 µs | 37.3 µs | **19.9 µs** |
| prefix scan (1k keys) | 194 µs | **167 µs** | 205 µs |
| 100-key get-loop | — | 3.79 ms | **2.05 ms** |
| 100-key multi_get | — | 3.87 ms | **2.17 ms** |

### 500M routes, unsettled

`settle()` was skipped: a fjall rewrite peaks near 2× the hydrated size, and
this volume did not have that much free space (105 GiB store, 139 GiB free).

| | fjall | rocksdb | pedradb |
|---|---:|---:|---:|
| hydrate | 553 s (0.90 M/s) | **512 s (0.98 M/s)** | did not finish |
| on disk | 105.3 GiB (226 B) | 106.7 GiB (229 B) | — |
| cold hit p50 / p99 | **46 µs / 136 µs** | 78 µs / 2.3 ms | — |
| cold miss p50 | **425 ns** | 604 ns | — |
| warm get_hit | **39 µs** | 146 µs | — |
| prefix scan (1k keys) | 194 µs | **170 µs** | — |
| 100-key get-loop | — | 24.1 ms | — |
| 100-key multi_get | — | 23.6 ms | — |

Pedra's 500M hydrate does not fit in 16 GiB RAM. Without a stage clamp the
kernel OOM-killed it at 48% (240M keys) with 15.4 GiB anonymous RSS. With
`PEDRA_STAGE_MAX_BYTES=67108864` it was still at 11.8 GiB anonymous RSS by
32% (160M keys, 0.91 M/s) and was stopped before the killer. Anonymous RSS
tracked key count at roughly 64 B/key.

## Usage

```rust
use slipstream::{RocksDbConfig, RocksDbSnapshot, SnapshotStore};

// open or resume
let (cursor, mut store) = RocksDbSnapshot::open(path, RocksDbConfig::default())?;

// after bulk hydration, before serving
store.settle()?;

// concurrent read handle (safe to clone across threads)
let reader = store.reader();
```

```rust
use slipstream::{FjallConfig, FjallSnapshot, SnapshotStore};

let (cursor, mut store) = FjallSnapshot::open(path, FjallConfig::default())?;
store.settle()?; // ~19 min at 500M routes; budget 2x disk headroom
let reader = store.reader();
```

```rust
use slipstream::{PedraDbConfig, PedraDbSnapshot, SnapshotStore};

let (cursor, mut store) = PedraDbSnapshot::open(path, PedraDbConfig::default())?;
store.settle()?; // flush + full compact (Pedra's wait_for_compact is a no-op)
let reader = store.reader();
```

All three on-disk backends implement [`SnapshotStore`], so the `watch_applied`
integration is identical regardless of which you choose.
