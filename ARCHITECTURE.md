# Slipstream Architecture

Trait-based KV abstraction over NATS JetStream: read, write, and watch distributed config with a resumable cursor so services replay only the delta after any restart.

## Data Flow

### Happy path: startup with snapshot

```
Disk ──► load(path) ──► replay_log() ──► HashMap<key, KvEntry> + WatchCursor
                                                    │
                                    watcher.watch_all_from(cursor, tx)
                                                    │
                                              CursorExpired?
                                             /              \
                                           Yes               No
                                            │                 │
                              watch_all(tx) + stale_keys()   delta stream
                                            └───────┬─────────┘
                                                    │
                                   KvUpdate → cache.apply() + snap.write_update()
                                                    │
                                     snap.checkpoint(cursor) ──► compact() if due
```

### Read path

```
reader.get("key")    ──► NATS kv.entry() ──► filter tombstones ──► KvEntry | None
reader.entry("key")  ──► NATS kv.entry() ──► raw (includes tombstones, for CAS)
reader.scan("pfx.")  ──► ephemeral push consumer (DeliverPolicy::LastPerSubject) ──► Vec<KvEntry>
reader.keys("pfx.")  ──► same consumer, headers_only ──► Vec<String>
```

### CAS write path

```
writer.create("lock", val)           ──► kv.create()        ──► AlreadyExists | VersionToken
writer.update("node", val, ver)      ──► kv.update()        ──► RevisionMismatch | VersionToken
writer.delete_with_version("k", ver) ──► kv.update(key, []) ──► RevisionMismatch | bool
```

CAS tombstone (empty-value Put) is how `delete_with_version` works — it writes an empty value via a CAS operation so concurrent writers see a conflict. `get()` and `scan()` filter these out; `entry()` exposes them for CAS callers that need the version.

### Watch resumption

```
watch_all_from(cursor, tx)
  cursor.is_none() ─────────────────────────► watch_all(tx)        (full replay)
  cursor has rev ──► kv.watch_all_from_revision(rev+1)
                       │                       │
                    Success                CursorExpired (NATS compacted past cursor)
                       │                       │
                   delta stream          caller falls back to watch_all(tx)
```

## Concepts & Terminology

| Term                     | Definition                                                           | NOT                                              |
| ------------------------ | -------------------------------------------------------------------- | ------------------------------------------------ |
| `Connection`             | Socket lifecycle manager + store factory                             | Not a store; not the NATS client itself          |
| `KvStore`                | Named bucket; vends reader, watcher, writer                          | Not the connection; holds no socket              |
| `KvReader`               | Point-in-time reads: `get`, `entry`, `keys`, `scan`                  | Not a live stream; returns a snapshot moment     |
| `KvWatcher`              | Live update stream pushed via mpsc channel                           | Not a polling loop; push from NATS               |
| `KvWriter`               | Write, soft-delete, CAS (`create`, `update`, `delete_with_version`)  | Not multi-key transactions                       |
| `WatchCursor`            | Opaque resume position in a watch stream (NATS: u64 revision)        | Not a per-key version; only for watch resumption |
| `VersionToken`           | Opaque per-key version (NATS: 8-byte u64; FDB: 10-byte versionstamp) | Not a wall-clock timestamp; not globally ordered |
| `KvEntry`                | One key + value + version from a read                                | Not a watch event; immutable once returned       |
| `KvUpdate`               | One watch event: `Put`, `Delete`, or `Purge`                         | Not a read result; carries deletes too           |
| `Snapshot`               | Deduplicated KV state + cursor persisted to disk                     | Not the source of truth; a cache of NATS         |
| `SnapshotWriter`         | Append-only log of `KvUpdate`s; no in-memory state beyond a counter  | Not the in-memory cache itself                   |
| `SnapshotStore`          | Trait: the durable-fold contract — atomic `apply(batch, cursor)`, `load`, `get`, `range` | Not a serving index; stops at fold + cursor + query |
| `AppendLogSnapshot`      | Default `SnapshotStore`: append-only log + in-RAM fold (pure-Rust)   | Not for folds larger than RAM                     |
| `FjallSnapshot`          | On-disk `SnapshotStore` (fjall LSM, `feature = "fjall"`) for large folds | Not in the pure-Rust core; opt-in feature        |
| `RocksDbSnapshot`        | On-disk `SnapshotStore` (RocksDB, `feature = "rocksdb"`) for large folds | Not pure-Rust; opt-in feature with a C++ build dep |
| `watch_applied`          | Combinator: batch → apply → *then* advance cursor / fold into `SnapshotStore` | Not a raw watch; the cursor follows `apply`, not receipt |
| `ConnectionCapabilities` | Feature flags for runtime branching (CAS, streaming watch, …)        | Not enforced; purely advisory                    |

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│        KvReader │ KvWatcher │ KvWriter │ KvTtl              │
│         (async_trait, object-safe, Arc<dyn Trait>)          │
├─────────────────────────────────────────────────────────────┤
│                        KvStore                              │
│          (named bucket — vends the three roles above)       │
├─────────────────────────────────────────────────────────────┤
│                       Connection                            │
│         (connect/shutdown/is_healthy + store factory)       │
├─────────────────────────────────────────────────────────────┤
│                    NatsConnection                           │
│   NatsKvStore │ NatsKvReader │ NatsKvWatcher │ NatsKvWriterImpl
│              (concrete NATS JetStream impl)                 │
└─────────────────────────────────────────────────────────────┘
                  snapshot.rs (orthogonal, optional)
┌─────────────────────────────────────────────────────────────┐
│   SnapshotStore trait: apply(batch, cursor) │ load │ get │ range
│   AppendLogSnapshot (default, in-RAM)                       │
│   FjallSnapshot │ RocksDbSnapshot (feature-gated, on-disk)  │
│          (append-only CRC log, tempfile+rename compact)     │
└─────────────────────────────────────────────────────────────┘
                  applied.rs (combinator over KvWatcher + snapshot)
┌──────────────────────────────────────────────────────────────┐
│   watch_applied(): batch → apply → advance cursor/checkpoint │
│   (cursor-after-apply; the safe default for resumable watch) │
└──────────────────────────────────────────────────────────────┘
```

## Core Mechanism

### Resumable Watch

The cursor is the NATS stream sequence number at the last checkpoint. On restart, pass it to `watch_all_from()` to subscribe at `cursor+1` — only the delta arrives, not the full history.

When the cursor expires (NATS retention window evicted those records), `CursorExpired` is returned. The caller falls back to `watch_all()` and should call `Snapshot::stale_keys()` to emit synthetic `Delete` events for keys that disappeared during the gap:

```rust
match watcher.watch_all_from(&snap.cursor, tx).await {
    Ok(()) => {}
    Err(KvError::CursorExpired) => {
        let live = reader.keys("").await?;
        for key in snap.stale_keys(live.iter().map(|s| s.as_str())) {
            cache.remove(key);
        }
        watcher.watch_all(tx).await?;
    }
    Err(e) => return Err(e.into()),
}
```

### Applied-Cursor Watch (`watch_applied`)

The resumable watch above hands the caller raw machinery — a channel of `KvUpdate`s, a cursor, a snapshot writer — and trusts each one to hand-roll the loop that batches updates, applies them, and advances the cursor. Every hand-rolled instance got the same step wrong: it advanced the cursor on **receipt** of an update (`high_water = rev` at `rx.recv()`) and applied the batch afterward. The combinator `watch_applied` exists to encode the correct discipline once.

**The resume guarantee.** This library's contract is "resume from a sequence number after any restart." `watch_applied` sharpens that into a single invariant:

> A persisted/reported cursor `C` ⟹ every update with revision ≤ `C` has been **applied** — the caller's `apply()` has returned for it.

The cursor is written from `apply()`'s completion, never from the channel's delivery. Concretely, on each flush the combinator runs `apply(batch)` to completion, *then* sets `cursor = batch_high`, *then* checkpoints the snapshot at that cursor, *then* fires `on_applied`. Nothing advances the cursor before `apply` returns.

**Why receipt is the wrong signal.** Bumping the cursor at `rx.recv()` and applying later opens a crash window: the persisted cursor claims "caught up to rev N" while rev N still sits in an unapplied batch buffer (or in flight to a separate apply task). On crash+resume the watch re-arms at `cursor+1`, *past* the unapplied rev N, and silently skips it. The data is gone with no error — a hole in the exact guarantee the crate advertises.

This is the lesson of Saltzer, Reed & Clark, *End-to-End Arguments in System Design* (1984): a checkpoint placed below the endpoint — here, at the transport's delivery rather than at the application of the update — can only ever be a performance hint, never a correctness guarantee. The "it happened" property can only be established at the endpoint that actually performs the work, so the cursor is sourced from `apply`, not from `recv`. The cursor-as-monotonic-index shape itself is the HashiCorp Consul anti-entropy / blocking-query lineage: a client re-arms its watch from the last index it *reconciled*, never from the index it merely *saw*.

**Cursor authority covers rejected entries.** `batch_high` tracks the highest revision *received* since the last flush, including updates that `parse` rejected (corrupt bytes, irrelevant keys). A rejected entry is still "nothing to apply," so it is covered by the cursor — and because NATS delivers in revision order, advancing to the max revision after one atomic `apply` is sound: having seen the max means every revision below it has been seen too. Without this, a run of irrelevant keys would pin the cursor in place and force redundant replay on every restart.

**Snapshot consistency.** Raw `KvUpdate`s stream to the snapshot log as they arrive, but the *checkpoint* cursor is the post-apply cursor. A crash after a raw record is written but before its `apply`/checkpoint leaves the log holding data *ahead* of its cursor — which is safe: the cursor never names a revision whose `apply` had not returned, so resume re-delivers and re-applies that tail rather than skipping it. Compaction runs off the hot path via `spawn_blocking`, as everywhere else in the snapshot subsystem.

**Flush triggers.** A batch flushes when any of these fires: the `window` elapses, `batch.len()` reaches `config.max`, a shutdown is signalled, or the channel closes with a pending batch (the remainder is flushed before returning). On `CursorExpired` from the resume path the combinator logs and falls back to the full-scope watch (`watch_all` / `watch_prefix`); v1 replays the full re-list as a stream of puts (a deeper "resync" signal that diffs against prior state is a documented TODO).

This is the layer the tunnel router (swap route table) and edge origin watcher (rebuild hashrings) both collapse onto: `parse` extracts the domain registration, `apply` swaps the live state, `on_applied` persists the cursor.

### scan() and keys() via Ephemeral Push Consumer

Both use `DeliverPolicy::LastPerSubject` — one ephemeral push consumer delivers the latest value per key in a single streaming operation, rather than N sequential `get()` calls. `keys()` adds `headers_only: true` so no value bytes cross the wire.

The consumer is always `AckPolicy::None`. The default `AckPolicy::Explicit` stops delivery after `max_ack_pending` (1000) un-acked messages, silently truncating any bucket with >1000 keys.

The consumer is created with **subscribe-before-create** ordering: the inbox subscription is registered before the consumer exists, closing a race in async-nats ≤0.46 where early messages arrive before the subscription is ready.

### ACK Subject Format Parsing in scan()

Each message delivered by the `scan()` push consumer carries the KV revision in its JetStream ACK subject (the message's reply subject). The revision is the stream sequence number, and it sits at a field offset that varies by NATS server version:

```
Legacy (9 tokens):  $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>
Modern (11–12 tok): $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>[.<token>]
```

The stream sequence sits at index 5 (legacy) or index 7 (modern). The final token is always `num_pending` (typically 0), which looks like a sequence but is not. The previous implementation took the last token and produced a wrong version on every scanned entry; the current parser reads from the front and branches on token count.

The implementation uses a fixed 8-element stack array for the first 8 tokens (no heap allocation per message). An A/B against the previous `Vec`-collecting approach measured **~3.1× speedup** — 1.59 ms → 0.51 ms per 10k ACK parses. See `benches/ack.rs`.

### 30-Second Operation Timeout

Every NATS operation is wrapped in `timed()` (30 s). Without it, a CLOSE_WAIT connection (half-dead TCP) parks `await`s forever — async-nats does not fail in-flight requests when the TCP layer goes dead. 30 s is generous for legitimate slow ops (JetStream stream sync, leader election) while still being debuggable.

### VersionToken: Inline Multi-Backend Versioning

`VersionToken` is a 10-byte inline buffer — no heap allocation. It covers all current backends without widening:

| Backend  | Encoding                 | `as_u64()` |
| -------- | ------------------------ | ---------- |
| NATS     | 8-byte big-endian u64    | `Some(rev)` |
| FDB      | 10-byte versionstamp     | `None`     |
| Unknown  | len=0                    | `None`     |

## Snapshot Subsystem

### The durable-fold trait

The durable fold is a trait, `SnapshotStore`, so the backend is pluggable while the contract is fixed:

```rust
fn load(path) -> (WatchCursor, Self);          // resume position + store
fn apply(&mut self, batch, cursor);            // fold data AND advance cursor, atomically
fn get(key) -> Option<KvEntry>;                // point query
fn range(prefix) -> Vec<KvEntry>;              // ordered prefix scan
```

Three invariants bind every implementation:

- **Pure function of the log.** Delete the store, replay every update with revision `> cursor`, and the state is identical. The store caches the fold; NATS is the source of truth.
- **Cursor-after-apply.** `apply` makes data and cursor durable together, so the cursor never names a revision whose data is absent — one transaction on a transactional backend, data-then-cursor on the append log (a torn write leaves data *ahead* of the cursor, which replay re-folds, never skips).
- **Snapshot is a cache.** A tail lost to power loss (under a no-sync durability mode) is rebuilt by resuming the watch from the recovered cursor.

`watch_applied` is generic over `SnapshotStore`: on each flush, after `apply` returns, it hands the raw batch + post-apply cursor to `store.apply(...)` on a blocking task. The trait stops at fold + cursor + query; serving structures built from the fold (routing rings, hashrings) live in the consumer, which reads them out via `get`/`range`.

| Backend | Module | State | Durability |
| ------- | ------ | ----- | ---------- |
| `AppendLogSnapshot` (default) | `snapshot.rs` | Append-only CRC log + in-RAM `HashMap` fold | `checkpoint` flush (page cache); `fsync` only at `compact` |
| `FjallSnapshot` (`feature = "fjall"`) | `snapshot_fjall.rs` | On-disk fjall LSM (`data` + `meta` partitions) | One atomic batch per `apply` (data + cursor); per-commit `fsync` configurable (NO_SYNC default) |
| `RocksDbSnapshot` (`feature = "rocksdb"`) | `snapshot_rocksdb.rs` | On-disk RocksDB (`data` + `meta` column families), tuned for billion-key folds (hit-optimized ribbon filters, partitioned index, zstd bottommost — see the module's Tuning docs) | One atomic `WriteBatch` per `apply` (data + cursor); WAL always on, per-commit `fsync` configurable (NO_SYNC default) |

The two LSM backends are interchangeable in contract and share the value-record codec (`snapshot_record.rs`); fjall keeps the crate pure-Rust, RocksDB trades a C++ build dependency for the battle-tested engine and its operational tooling (`ldb`, `sst_dump`). Both keep the cursor in the same atomic batch as the data it names, so under NO_SYNC a crash can lose the un-synced tail but never desynchronize cursor from data — on reopen the recovered cursor is consistent and the watch re-folds the tail. The rest of this section describes the **append-log backend** (the default), whose on-disk format is below.

### File Format

```
Header:  b"PGSS" ++ version:u16le

Record:  crc32:u32le ++ type:u8 ++ payload

Put:     key_len:u16le ++ key ++ value_len:u32le ++ value ++ ver_len:u8 ++ version_bytes
Delete:  key_len:u16le ++ key ++ ver_len:u8 ++ version_bytes
Cursor:  cur_len:u8 ++ cursor_bytes
```

Version bytes are stored as length-prefixed raw bytes, not a fixed `u64`. A 10-byte FDB versionstamp round-trips intact; a `u64`-only field would flatten it to 0 and break every subsequent CAS on a restored entry.

CRC covers from the type byte through the end of the record. A truncated final record (crash mid-write) is silently discarded. A CRC mismatch in the middle of the file returns `SnapshotError::Corrupted`.

### State Machine

```
APPENDING ──► checkpoint() returns true ──► NEEDS_COMPACT
    │                                              │
write_update()                           compact() [blocking: replay → dedup → tempfile → rename]
    │                                              │
    └──────────────────────────────────────────────┘
                bytes_since_compact = 0
```

| From          | Event                            | To            | Guard / Side-effect                              |
| ------------- | -------------------------------- | ------------- | ------------------------------------------------ |
| APPENDING     | `write_update()`                 | APPENDING     | Buffered; bytes_since_compact += n               |
| APPENDING     | `checkpoint()` → true            | NEEDS_COMPACT | Cursor + cursor record flushed to page cache     |
| APPENDING     | `checkpoint()` → false           | APPENDING     | Same flush; below threshold                      |
| NEEDS_COMPACT | `compact()` succeeds             | APPENDING     | Tempfile → sync_all → rename; counter reset to 0 |
| NEEDS_COMPACT | `compact()` fails on reopen      | POISONED      | `writer = None`; subsequent writes return `Io`   |
| POISONED      | any `write_update`/`checkpoint`  | POISONED      | `Err(Io("snapshot writer poisoned"))` returned   |

### Load + Compaction

`load()` replays the full log into a `HashMap` (last write wins per key, deletes remove entries), then rewrites to a compact file (no duplicates) via tempfile + `sync_all` + rename. It skips the rewrite when the log is already compact (no duplicate keys, no delete records, clean EOF).

`compact()` flushes the BufWriter first so un-checkpointed records survive. It reads the current file, replays it, writes to a same-directory tempfile (same filesystem = atomic rename, no `EXDEV`), `sync_all`s, then renames.

`checkpoint()` writes only a cursor record and calls `BufWriter::flush()` — a `write(2)` into the page cache. This survives a process crash but NOT a power loss. The only `fsync` is in `compact()`. The snapshot is a cache; a lost tail is rebuilt from a NATS scan + watch replay.

## Connection Lifecycle

```
NEW (healthy=false, handle=None)
    │
    │ .connect()
    ▼
CONNECTED (healthy=true, handle=Some(NatsHandle))
    │                  │
    │ .shutdown()      │ .store() → NatsKvStore
    ▼                  │ .is_healthy() → AtomicBool::load (O(1), no lock)
SHUTDOWN (healthy=false, handle=None)
    │
    └─► .connect() can reconnect
```

`is_healthy()` for the `new()` + `connect()` path reads an `AtomicBool` driven by an installed NATS event callback (`Connected`/`Disconnected`). For the `from_client()` path (pre-connected client, no event callback), it reads the client's live `connection_state()` instead.

The double-check pattern in `connect()` guards a concurrent connect race: a second caller that wins the dial drops its handle (leaving `installed=false` on the event callback) so the teardown event does not clobber the winner's `healthy` flag.

## Design Decisions

### Why a `watch_applied` combinator instead of leaving the loop to callers?

The raw `KvWatcher` + `WatchCursor` + `SnapshotWriter` pieces let callers hand-roll the batch/apply/advance loop — and every known caller advanced the cursor on *receipt* rather than after *apply*, silently skipping un-applied updates on crash+resume. That is a footgun in the library's core guarantee, not a caller bug to be fixed N times. Encoding cursor-after-apply once, behind a combinator that callers can't get wrong, is cheaper and safer than documentation. `apply` stays the only domain logic; the cursor/snapshot/`on_applied` bookkeeping is the library's. See [Applied-Cursor Watch](#applied-cursor-watch-watch_applied).

### Why KvError: Clone instead of Box<dyn Error>?

A failed connect future may be observed by multiple concurrent callers waiting on a shared result. `Clone` lets the error fan out to N waiters without `Arc`. The cost: `std::io::Error` and `async-nats` error types are not `Clone`, so their structured cause chain is flattened into a pre-rendered `String` at the boundary. The trade-off is explicit: no `#[source]` chain, but the message carries context instead.

### Why object-safe async traits (Arc<dyn Trait>) instead of generics?

`KvStore` vends `Arc<dyn KvReader>` / `Arc<dyn KvWatcher>` / `Arc<dyn KvWriter>` so callers hold narrowed capabilities without knowing the backend type at compile time. This lets services swap NATS for an in-memory stub in tests, and lets the edge proxy hold only `Arc<dyn KvReader>` without dragging in write types. The `async_trait` macro desugars to `Pin<Box<dyn Future>>` to satisfy object safety.

### Why optional watcher() and writer()?

Not all backends support streaming watch or writes. Optional returns (`Option<Arc<dyn …>>`) let the read path — the hot path for config consumers — be free of watch/write complexity. The edge proxy, for example, only calls `reader()`. Callers check `ConnectionCapabilities` to branch on feature availability before attempting optional paths.

### Why a raw JetStream API fallback for bucket creation?

Synadia Cloud returns `$JS.API.STREAM.CREATE` response shapes that `async-nats`'s `create_key_value()` cannot parse. The `create_kv_bucket_raw()` fallback sends the JSON config as a plain request/reply and classifies responses by error code:

- `10058` in `code` or `err_code` → stream already exists (non-fatal)
- `400` + "maximum number of streams" → Synadia at-limit but bucket may exist (non-fatal)
- Anything else → hard failure

The standard async-nats path is tried first; raw is a fallback.

### Why subscribe-before-create in scan/keys?

async-nats ≤0.46 has a race: the server can deliver the first batch of push-consumer messages before the client's subscribe call completes. Creating the consumer first loses those early messages. Subscribing to the inbox first closes the race — the subscription is ready before the server can deliver.

### Why checkpoint() does not fsync?

Checkpoints are frequent (every N watch events). An fsync per checkpoint would add milliseconds of disk-sync latency to the hot watch path. Since the snapshot is a cache backed by NATS, a tail lost to power loss is rebuilt from a NATS replay — not a correctness failure. The only `fsync` is in `compact_to_file()`, where it guarantees the new compact file is durable before the atomic rename replaces the old one.

### Why write in sorted key order during compaction?

`HashMap` iteration order is random per process. Sorting produces a deterministic byte layout for a given logical state, enabling byte-level snapshot comparison (integrity checksums, test assertions) and making file diffs readable. The O(n log n) sort is negligible relative to the I/O it precedes.

## NATS Mapping

| Concept                  | NATS primitive                                                                  |
| ------------------------ | ------------------------------------------------------------------------------- |
| `KvStore`                | JetStream KV bucket (`KV_{name}` stream, `$KV.{name}.>` subjects)              |
| `VersionToken`           | Per-key stream sequence number (u64, stored big-endian in the 8-byte token)    |
| `WatchCursor`            | Stream sequence number at last checkpoint                                       |
| `delete()`               | `kv.delete()` — writes `KV-Operation: DEL` marker; always returns `Ok(true)`   |
| `delete_with_version()`  | `kv.update(key, [], rev)` — CAS write of empty value as tombstone               |
| `KvUpdate::Purge`        | `KV-Operation: PURGE` — all history removed; treated same as Delete in snapshot |
| `scan()` / `keys()`      | Ephemeral push consumer with `DeliverPolicy::LastPerSubject`                    |
| `watch_prefix()`         | `kv.watch("{prefix}>")` — server-side subject-filter wildcard                  |
| `watch_all_from(cursor)` | `kv.watch_all_from_revision(cursor+1)` — server-side delta delivery            |

## Trust Model

**What the store layer verifies:**
- NATS credentials are valid (at `connect()`)
- Bucket exists or can be created (at `store()`)
- Snapshot CRC per record (at `load()` and `compact()`)
- Snapshot magic bytes and format version (at `load()`)

**What passes through unchecked:**
- Key names (no validation; NATS accepts any key)
- Value content (raw bytes; deserialization is the caller's responsibility)
- Bucket permissions (NATS auth rules govern access; the store layer does not re-check)
- Channel capacity (caller sets it; a full channel backpressures the watcher task)
- Snapshot cursor validity (stale cursors surface as `CursorExpired` from NATS, not from the snapshot layer)

**Why this is acceptable:** Applications own value encoding (JSON, proto, etc.). NATS owns authorization. The store layer is a transport adapter.

## Failure Modes

| Failure                         | Recovery                                                       |
| ------------------------------- | -------------------------------------------------------------- |
| `CursorExpired`                 | Fall back to `watch_all()`; use `stale_keys()` for deletes     |
| `WatchError`                    | Re-subscribe; watch stream dropped (NATS restart, reconnect)   |
| `Timeout` on any op             | CLOSE_WAIT connection; call `shutdown()` + `connect()`         |
| `RevisionMismatch` on CAS       | Re-read with `entry()`, resolve conflict, retry                |
| `AlreadyExists` on `create()`   | Read the live value, decide whether to proceed                 |
| Snapshot truncated tail         | `load()` discards partial record; earlier state preserved      |
| Snapshot mid-file CRC mismatch  | `SnapshotError::Corrupted`; delete file, do full NATS replay   |
| Snapshot wrong format version   | `SnapshotError::InvalidFormat`; delete file, full NATS replay  |
| `compact()` I/O error           | Retry; if persistent, delete file and rebuild from NATS        |
| Synadia Cloud stream limit      | Raw API path treats as non-fatal; verifies with `get_key_value` |

## Package Structure

| File              | Purpose                                                                              |
| ----------------- | ------------------------------------------------------------------------------------ |
| `src/kv.rs`       | Core traits (`KvReader`, `KvWriter`, `KvWatcher`, `KvTtl`) and types (`KvEntry`, `KvUpdate`, `VersionToken`, `WatchCursor`, `KvError`) |
| `src/stores.rs`   | `Connection`, `KvStore`, `StoreConfig`, `StorageType`, `ConnectionCapabilities`      |
| `src/nats.rs`     | NATS JetStream implementation; bucket creation, scan consumer lifecycle, timeout wrapping, Synadia Cloud workarounds |
| `src/snapshot.rs` | `SnapshotStore` trait; append-only log + `AppendLogSnapshot` (default backend): `SnapshotWriter`, `load()`, `replay_log()`, `compact_to_file()` |
| `src/snapshot_fjall.rs` | `FjallSnapshot`: on-disk `SnapshotStore` backed by fjall (`feature = "fjall"`)  |
| `src/snapshot_rocksdb.rs` | `RocksDbSnapshot`: on-disk `SnapshotStore` backed by RocksDB (`feature = "rocksdb"`) |
| `src/snapshot_record.rs` | Shared `[ver_len][version][value]` value-record codec for the LSM backends |
| `src/applied.rs`  | `watch_applied` cursor-after-apply combinator, generic over `SnapshotStore`: `WatchScope`, `BatchConfig` |
| `src/lib.rs`      | Re-exports all public types; no logic                                                |
| `benches/`        | Criterion benchmarks for snapshot write/checkpoint/load throughput and batch throughput |
| `tests/`          | Integration tests (require live NATS)                                                |

## Configuration

### StoreConfig (bucket creation only)

Config applies only at creation. If the bucket already exists, the existing one is returned as-is — `max_bytes`, `num_replicas`, `max_history`, `max_age` are ignored. To change settings on a live bucket, alter the underlying JetStream stream out-of-band.

| Field          | Default    | Rationale                                                          |
| -------------- | ---------- | ------------------------------------------------------------------ |
| `max_bytes`    | 10 MiB     | Required by Synadia Cloud; omit only for self-hosted NATS          |
| `max_history`  | 1          | Config stores rarely need change history                           |
| `num_replicas` | 1          | Set to 3 for production HA clusters                                |
| `max_age`      | None       | Set to gate retention window (also determines when cursors expire) |

### NatsConnectionConfig

| Field        | Notes                                                               |
| ------------ | ------------------------------------------------------------------- |
| `url`        | `nats://` or `tls://`; may embed `user:pass@` for legacy auth       |
| `creds`      | Base64-encoded `.creds` content (containers, ECS — no file mount)  |
| `creds_file` | Path to `.creds` on disk (bare-metal, local dev)                    |

Credentials priority: `creds` > `creds_file` > URL-embedded > no auth.

### Snapshot Tuning

| Parameter           | Effect                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------- |
| `compact_threshold` | Bytes appended since last compaction before `checkpoint()` returns `true`. Typical: 1–10 MB |
