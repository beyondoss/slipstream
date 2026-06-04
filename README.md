# Slipstream

Trait-based KV abstraction over NATS JetStream. Read, write, and watch distributed config. A cursor tracks position in the watch stream so services resume from the delta after any restart.

```toml
[dependencies]
beyond-slipstream = "0.2"
```

## Concepts

| Term                 | What It Is                                                                       |
| -------------------- | -------------------------------------------------------------------------------- |
| `Connection`         | NATS connection lifecycle + store factory                                        |
| `KvStore`            | Named bucket. Vends reader, watcher, writer                                      |
| `KvReader`           | Point-in-time reads: `get`, `entry`, `keys`, `scan`                              |
| `KvWatcher`          | Live update stream via channel                                                   |
| `KvWriter`           | Write, soft-delete, CAS (`create`, `update`, `delete_with_version`)              |
| `WatchCursor`        | Opaque position in a watch stream. Save it; pass it back on reconnect            |
| `VersionToken`       | Opaque version — NATS: u64 revision; FDB: 10-byte versionstamp                   |
| `KvEntry`            | One key + value + version from a read                                            |
| `KvUpdate`           | One watch event: `Put`, `Delete`, or `Purge`                                     |
| `Snapshot`           | Deduplicated KV state + cursor at a point in time. Disk cache, not source of truth |
| `SnapshotWriter`     | Append-only log of `KvUpdate`s; survives restarts without a full NATS scan       |
| `watch_applied`      | Watch loop that advances the cursor only after your `apply` returns               |
| `ConnectionCapabilities` | Feature flags for runtime branching (CAS, streaming watch, global ordering) |

## Usage

### Connect

```rust
use slipstream::{Connection, NatsConnection, NatsConnectionConfig};

let conn = NatsConnection::new(NatsConnectionConfig {
    url: "nats://localhost:4222".into(),
    creds: None,
    creds_file: None,
});
conn.connect().await?;
```

### Open a store

```rust
use slipstream::{StoreConfig, StorageType};
use std::time::Duration;

let store = conn.store_with_config(StoreConfig {
    name: "nodes".into(),
    storage: StorageType::Persistent,
    max_bytes: Some(512 * 1024 * 1024), // required by Synadia Cloud
    max_history: Some(1),
    max_age: Some(Duration::from_secs(30 * 24 * 3600)),
    num_replicas: Some(3), // HA clusters
    ..Default::default()
}).await?;
```

`max_bytes` is required on Synadia Cloud. Omit only for self-hosted NATS.

### Read

```rust
use slipstream::KvReader;

let reader = store.reader();

// Single key — filters tombstones; use entry() to include them for CAS
if let Some(entry) = reader.get("node.us-east-1").await? {
    println!("{}: {:?}", entry.key, entry.version);
}

// All entries under prefix
// Uses DeliverPolicy::LastPerSubject: one NATS consumer, not N round-trips.
let entries = reader.scan("node.").await?;

// Key names only (no value transfer)
let keys = reader.keys("node.").await?;
```

### Write

```rust
use slipstream::KvWriter;

let writer = store.writer().expect("store is writable");

// Unconditional write
let version = writer.put("node.us-east-1", &payload).await?;

// Create only. Returns AlreadyExists if key has a live value.
let version = writer.create("lock.migration", &payload).await?;

// CAS update. Returns RevisionMismatch if version doesn't match.
let new_version = writer.update("node.us-east-1", &payload, &version).await?;

// CAS delete. Returns RevisionMismatch on conflict.
writer.delete_with_version("node.us-east-1", &version).await?;

// Best-effort delete — returns Ok(true) even if key didn't exist.
writer.delete("node.us-east-1").await?;
```

### Watch

```rust
use slipstream::{KvUpdate, KvWatcher};

let watcher = store.watcher().expect("store supports streaming");
let (tx, mut rx) = tokio::sync::mpsc::channel(128);

// watch_all blocks until the stream ends — run it in a separate task
tokio::spawn(async move {
    watcher.watch_all(tx).await.unwrap();
});

while let Some(update) = rx.recv().await {
    match update {
        KvUpdate::Put(entry) => { /* ... */ }
        KvUpdate::Delete { key, version } => { /* ... */ }
        KvUpdate::Purge { key, version } => { /* ... */ }
    }
}
```

Dropping `rx` cancels the watch. The watcher task exits and unsubscribes automatically.

### Resumable watch

The cursor is a sequence number. Persist it; pass it back on reconnect. NATS delivers only the delta since that position.

```rust
let cursor = load_cursor().unwrap_or(WatchCursor::none());

match watcher.watch_all_from(&cursor, tx.clone()).await {
    Ok(()) => {}
    Err(KvError::CursorExpired) => {
        // NATS compacted past the cursor. Full replay required.
        watcher.watch_all(tx).await?;
    }
    Err(e) => return Err(e.into()),
}
```

`watch_prefix_from()` works the same way for prefix-filtered streams.

## Snapshot

For services that cache KV state locally, the snapshot persists both state and cursor to disk. On restart, load the snapshot and resume the watch from its cursor — only the delta since the last checkpoint arrives from NATS.

### Startup

```rust
use slipstream::snapshot;

if let Some(snap) = snapshot::load(Path::new("/var/lib/svc/state.snap"))? {
    for (key, entry) in snap.entries {
        cache.insert(key, entry.value);
    }
    watcher.watch_all_from(&snap.cursor, tx).await?;
} else {
    watcher.watch_all(tx).await?;
}
```

### Runtime

```rust
use slipstream::snapshot::SnapshotWriter;

let mut snap = SnapshotWriter::open(
    Path::new("/var/lib/svc/state.snap"),
    10 * 1024 * 1024, // compact after 10MB of appended records
)?;

while let Some(update) = rx.recv().await {
    cache.apply(&update);
    snap.write_update(&update); // buffered, no I/O

    // checkpoint() flushes + syncs to disk; returns true when compaction is due
    if snap.checkpoint(&current_cursor)? {
        // compact() is blocking I/O; run via spawn_blocking in async contexts
        tokio::task::spawn_blocking(move || snap.compact()).await??;
    }
}
```

This loop has a trap: `current_cursor` must track what `cache.apply()` has consumed, not what `rx.recv()` delivered. Get it wrong and a crash skips updates on resume. [`watch_applied`](#applied-watch) runs this loop for you with that invariant enforced.

The snapshot is a cache. Delete it and the service falls back to full replay on next start.

### File format

```
Header:  b"PGSS" ++ version:u16le
Record:  crc32:u32le ++ type:u8 ++ payload
```

| Record type  | Byte | Payload                                                                              |
| ------------ | ---- | ------------------------------------------------------------------------------------ |
| `REC_PUT`    | 0x01 | key_len:u16le ++ key ++ value_len:u32le ++ value ++ ver_len:u8 ++ version_bytes      |
| `REC_DELETE` | 0x02 | key_len:u16le ++ key ++ ver_len:u8 ++ version_bytes                                  |
| `REC_CURSOR` | 0x03 | cursor_len:u8 ++ cursor bytes                                                        |

`version_bytes` is the raw [`VersionToken`] bytes (≤10), not a fixed u64, so NATS revisions (8 bytes) and FDB versionstamps (10 bytes) both round-trip intact.

A truncated final record (crash mid-write) is discarded; earlier records are intact. A CRC failure mid-file returns `SnapshotError::Corrupted`.

## Applied watch

`watch_applied` drives the watch-batch-apply-checkpoint loop and enforces one rule the hand-rolled version can't: the cursor advances only after your `apply` returns, never on receipt.

```rust
use slipstream::{watch_applied, BatchConfig, KvUpdate, WatchCursor, WatchScope};

let final_cursor = watch_applied(
    watcher,
    WatchScope::All,                  // or WatchScope::Prefix("node.".into())
    load_cursor(),                    // Option<WatchCursor> — resume here, or None
    Some(snapshot_writer),            // checkpoints at the applied cursor, or None
    BatchConfig::default(),           // 10ms window, 100 updates per batch
    |update: &KvUpdate| parse(update),        // KvUpdate -> Option<U>; None just drops it
    |batch: Vec<U>| cache.apply_batch(batch), // your only domain logic
    |cursor: WatchCursor| persist(cursor),    // fires after apply returns
    shutdown,                                 // tokio::sync::watch::Receiver<bool>
).await?;
```

A batch closes when `window` elapses or it hits `max` updates, whichever comes first. Then, in order: `apply(batch)` runs to completion, the cursor advances to the batch's highest revision, the snapshot checkpoints at that cursor, and `on_applied` fires.

Persist the cursor on receipt instead and a crash between receive and apply loses data: the cursor reads "caught up to rev N" while rev N sits in an unapplied buffer, and the next resume starts past it. `watch_applied` checkpoints at the applied cursor, so a persisted cursor always means every update up to it has been applied.

- `parse` returning `None` (corrupt bytes, irrelevant key) still advances the cursor — nothing to apply means nothing to skip.
- On `CursorExpired`, it falls back to a full watch automatically.
- It returns the final applied cursor on shutdown or stream close.

`apply` runs inline. If it panics, the panic aborts the watch.

## NATS mapping

| Concept           | NATS primitive                                                   |
| ----------------- | ---------------------------------------------------------------- |
| Store             | JetStream KV bucket (`KV_{name}` stream)                         |
| `VersionToken`    | Per-key revision (u64, big-endian)                               |
| `WatchCursor`     | NATS revision at last checkpoint                                 |
| `delete()`        | Writes empty value (soft delete). Always returns `Ok(true)`      |
| `KvUpdate::Purge` | Hard delete: all history removed from stream                     |
| `scan()`          | `DeliverPolicy::LastPerSubject`: one entry per key, one consumer |
| `watch_prefix()`  | Native NATS subject filter (`{prefix}>` wildcard)                |

## Feature detection

```rust
let caps = conn.capabilities();

if caps.cas             { /* safe to call create/update/delete_with_version */ }
if caps.streaming_watch { /* watcher() is Some */ }
if caps.prefix_watch    { /* watch_prefix() uses a server-side filter */ }
if caps.global_ordering { /* VersionToken is globally ordered across keys */ }
```

## Errors

| Error              | Cause                                                | Recovery                   |
| ------------------ | ---------------------------------------------------- | -------------------------- |
| `NotConnected`     | Operation before `connect()`                         | Call `connect()`           |
| `AlreadyExists`    | `create()` on a live key                             | Read current state, decide |
| `RevisionMismatch` | CAS conflict on `update()` / `delete_with_version()` | Re-read, retry             |
| `CursorExpired`    | `watch_*_from()` cursor compacted by NATS            | Fall back to `watch_all()` |
| `WatchError`       | NATS stream dropped                                  | Re-subscribe               |

## Credentials

Priority order, first match wins:

1. `creds`: base64-encoded `.creds` content (containers, ECS)
2. `creds_file`: path to `.creds` on disk (bare-metal, local dev)
3. URL-embedded `user:pass@host`
4. No auth
