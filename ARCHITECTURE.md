# Slipstream Architecture

Takes NATS JetStream KV buckets as input and produces a local durable fold — a `SnapshotStore` whose cursor is guaranteed to name only revisions whose `apply` has returned — so edge nodes survive restarts without full replay, and can bootstrap from a content-addressed artifact when the NATS log has compacted past their cursor.

## Data Flow

### Happy path: startup with snapshot — no export

```
Disk ──► load(path) ──► replay_log() ──► HashMap<key, KvEntry> + WatchCursor
                                                    │
                                    watcher.watch_all_from(cursor, tx)
                                                    │
                                              CursorExpired?   (at resume, or mid-watch
                                             /              \    from the floor guard)
                                           Yes               No
                                            │                 │
                              bucket evicts current values?   delta stream
                               /                      \           │
                             No                       Yes         │
                              │                        │          │
                key-listing diff (synthetic   artifact restore:   │
                deletes) + watch_all(tx)      fold := artifact,   │
                (state-sync re-list)          resume from its     │
                              │               cursor              │
                              └──────────┬─────────┘              │
                                         └──────────┬─────────────┘
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

### Bootstrap via artifact (bounded log, cursor evicted)

```
ObjectStore (<prefix>/<key>.manifest.json)
  ↓ download pointer
ExportManifest (cursor, backend, file hashes)
  ↓ derive payload key from the cursor and blake3(manifest)[..8]
ObjectStore (<prefix>/<key>.payloads/<origin>-<rank>-<hash>.tar)
  ↓ stream to temp dir (verified: embedded manifest == pointer bytes, every file hash re-checked)
Staged artifact dir
  ↓ rename into place (atomic; bad artifact never becomes the fold)
SnapshotStore (recovered cursor = manifest.cursor)
  ↓ watch_applied(resume=cursor)
NATS JetStream (tail delta from cursor+1)
  ↓ apply tail
Converged fold
```

### Export round (one winner per fleet, TTL-gated)

```
ExportLease::try_acquire(ttl)
  ↓ create-only CAS wins; expired/corrupt lease CAS-stolen
watch_applied ExportRequest channel
  ↓ pending batch flushed first
store.export_to(artifact_dir)
  ↓ backend snapshot + verify-by-reopen (cursor equality gate)
ExportManifest (cursor, backend, file BLAKE3 hashes)
  ↓
ObjectStoreTransport::upload(key, artifact_dir)
  ├─ tar data/ → multipart upload (8 MiB chunks, 8 concurrent)
  │    key = <prefix>/<key>.payloads/<origin hex>-<rank hex>-<blake3(manifest)[..8]>.tar
  └─ swap_pointer() → conditional put at <prefix>/<key>.manifest.json
       ├─ pointer_publish_allowed(current, candidate Pos)?  (incarnation, then rank)
       │    true  → CAS put (max 8 retries on concurrent swap)
       │    false → SupersededByNewer (slow exporter gives up cleanly)
       └─ PublishOutcome::Published | SupersededByNewer
  ↓ Published
LeaseGuard::complete(cursor)
  ↓
ObjectStoreTransport::prune(key, grace=4×ttl)
  └─ delete payloads: payload_prunable(payload Pos, pointer Pos, is_target, aged_out)?
  ↓
delete local artifact dir (transience enforced — not hoped for)
```

### Watch resumption

```
watch_all_from(cursor, tx)
  cursor.is_none() ─────────────────────────► watch_all(tx)        (full replay)
  cursor has rev ──► kv.watch_all_from_revision(rev+1)
                       │                       │
                    Success                CursorExpired (NATS compacted past cursor)
                       │                       │
                   delta stream          caller falls back to watch_all(tx)

  cursor from another stream incarnation (bucket deleted + recreated) ──► ForeignCursor
```

Before attaching, `check_resume_window` reads the stream once and applies two kernels: `same_incarnation` (is this cursor this stream's?) and `resume_window_ok` (is the position still retained?). The incarnation is the stream's creation time (Unix ns, from stream info). The watcher reads it before a watch attaches and stamps every update's `VersionToken` with it (`with_origin`), so every cursor a watch produces records where it was taken, and the cursor's bytes and every artifact manifest carry it (`WatchCursor::to_bytes`, flag `0b100`, no manifest schema change). A recreated bucket restarts its revisions at 1, which the sequence window alone cannot see: a cursor above the new head made the watch deliver nothing until the stream outgrew it (silently skipping every new revision up to it), and one inside the new range spliced the old stream's keys under the new stream's tail. A cursor with no recorded incarnation (written by 0.9.x) is admitted only when its revision is at most the stream's `last_sequence`, and records the incarnation from its next batch on: it keeps an in-place upgrade from forcing every fold to reseed, at the cost of one hole (an unrecorded cursor from an older incarnation that falls inside the new range), which `tests/model_incarnation.rs` derives and which closes as folds restart on this version.

Every non-`_from` watch is a **state-sync** stream (NATS `DeliverPolicy::LastPerSubject`): the latest revision of every matching key the bucket *still holds* is delivered first — the re-list — then live updates. On a bucket that never evicts current values that is the full current state, with no separate scan and no scan-to-watch race window. On an evicting bucket (`discard: old` with a limit, `max_age`) it misses current keys whose last write was evicted: the current set lives in a fold plus artifacts (`watch_applied`, `Preset::node_slice` / `Preset::full_replica`), never in the raw watch. The NATS watcher warns once per bucket per process on a raw watch of an evicting bucket; slipstream's own re-lists (`repair::watch_scope`, the `_from` fallbacks) are exempt via the `INTERNAL_WATCH` task-local. The `_from` variants skip the re-list and deliver only the delta.

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
| `Snapshot`               | Deduplicated KV state + cursor persisted to disk; on a bounded log, a replica of record | Not a cache: NATS no longer holds what it evicted |
| `SnapshotWriter`         | Append-only log of `KvUpdate`s; no in-memory state beyond a counter  | Not the in-memory cache itself                   |
| `SnapshotStore`          | Trait: the durable-fold contract — atomic `apply(batch, cursor)`, `load`, `get`, `range` | Not a serving index; stops at fold + cursor + query |
| `AppendLogSnapshot`      | Default `SnapshotStore`: append-only log + in-RAM fold (pure-Rust)   | Not for folds larger than RAM                     |
| `FjallSnapshot`          | On-disk `SnapshotStore` (fjall LSM, `feature = "fjall"`) for large folds | Not in the pure-Rust core; opt-in feature        |
| `RocksDbSnapshot`        | On-disk `SnapshotStore` (RocksDB, `feature = "rocksdb"`) for large folds | Not pure-Rust; opt-in feature with a C++ build dep |
| `watch_applied`          | Combinator: batch → apply → *then* advance cursor / fold into `SnapshotStore`; repairs the fold when its cursor expires | Not a raw watch; the cursor follows `apply`, not receipt |
| `ExpiryRepair`           | How `watch_applied` repairs an expired cursor: `Relist` (key-listing diff), `Restore { reader, restore }` (artifact; the reader's live listing keeps it from deleting live keys), `Auto` (by bucket retention), `None` | Not a retry policy; it decides what the fold may trust |
| `Retention`              | What a watcher's log can evict: `evicts_current_values` + `first_revision`, read live from the stream (`KvWatcher::retention`) | Not config; read at the moment of expiry |
| `RestoreSource` / `ArtifactRestore` | Supplies the newest published artifact, opened as a temporary fold of the consumer's backend | Not a bootstrap tool for new nodes' paths; feeds `ExpiryRepair::Restore` |
| `WatchScope`             | What `watch_applied` watches: `All`, `Prefix`, or `Prefixes` (multi-filter union) | Not N consumers; `Prefixes` costs one consumer    |
| `ConnectionCapabilities` | Feature flags for runtime branching (CAS, streaming watch, …)        | Not enforced; purely advisory                    |
| `ExportRequest`          | One-shot channel message that asks `watch_applied` to flush + export the current fold | Not a store operation; handled between flushes   |
| `ExportLease`            | Fleet-wide at-most-one coordinator: CAS key that prevents N nodes exporting the same round | Not a correctness gate; only dedup. Pointer monotonicity is the gate |
| `LeaseGuard`             | RAII guard for a won export round: `complete(cursor)` stamps success, `abandon()` frees it early via CAS delete | Not held across restarts; process crash → expiry → next node steals |
| `LeaseRecord`            | Value stored in the lease key: holder, acquired/expires timestamps (wall-clock, embedded — no server TTL), optional completed cursor | Expiry is compared by callers, not the store; requires NTP-sane clocks |
| `ExportManifest`         | Artifact metadata committed to disk and object store: backend identity, format generation, cursor, per-file BLAKE3 hashes, and (schema 2) the exporting watch's key scope | The manifest bytes are their own content address (blake3[..8]) |
| `ArtifactTransport`      | Trait: `upload(key, dir) → ManifestAndOutcome`, `download(key) → ManifestAndDir`, `pointer(key)`, `prune(key, grace)` | Not an object store; adapts `ObjectStore` for the pointer-swap protocol |
| `ObjectStoreTransport`   | Concrete `ArtifactTransport` over any `object_store` backend (S3, GCS, Azure, local) | `file://` lacks CAS and FAILS CLOSED unless `with_non_atomic_pointer_fallback()` (dev only) |
| `PublishOutcome`         | `Published` (pointer advanced) or `SupersededByNewer` (refused — a newer pointer exists) | Not an error; a slow exporter's normal exit under concurrent rounds |
| `PointerState`           | What `swap_pointer` observes before deciding: `Absent`, `Present { at: Some(Pos) }`, or `Present { at: None }` (corrupt) | Used only by `pointer_publish_allowed`; never stored independently |

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

When the cursor expires (NATS retention window evicted those records), `CursorExpired` is returned. The fallback `watch_all()` re-list re-delivers the current value of every live key, but it cannot cover keys **deleted during the gap whose delete markers were also evicted** — those need synthetic `Delete` events diffed from prior state. On a bucket whose retention never evicts current values, `watch_applied` does this automatically (see below), and a raw-API caller hand-rolls the same diff with `Snapshot::stale_keys()`, which returns `None` when the bucket's retention says the listing isn't the truth (the same planner as `ExpiryRepair::Relist`):

```rust
match watcher.watch_all_from(&snap.cursor, tx).await {
    Ok(()) => {}
    Err(KvError::CursorExpired) => {
        let live = reader.keys("").await?;
        let Some(stale) = snap.stale_keys(live.iter().map(|s| s.as_str()), watcher.retention().await?)
        else {
            // Retention evicts current values: a missing key may have aged
            // out. Restore from an artifact instead (ExpiryRepair::Restore).
            return Err(KvError::CursorExpired.into());
        };
        for key in stale {
            cache.remove(key);
        }
        watcher.watch_all(tx).await?;
    }
    Err(e) => return Err(e.into()),
}
```

That diff is only sound where "not in the bucket" means "deleted". On a bucket with `max_age`, per-message TTLs, or `discard: old` under a limit, retention evicts **current** values too: a key can age out with nobody deleting it, and a write made during the gap can age out before the consumer returns. NATS cannot tell "deleted, marker evicted" from "aged out", so no listing diff can be made correct there — `watch_applied` restores from an artifact instead (see [Cursor-expiry repair](#cursor-expiry-repair)).

### Applied-Cursor Watch (`watch_applied`)

The resumable watch above hands the caller raw machinery — a channel of `KvUpdate`s, a cursor, a snapshot writer — and trusts each one to hand-roll the loop that batches updates, applies them, and advances the cursor. Every hand-rolled instance got the same step wrong: it advanced the cursor on **receipt** of an update (`high_water = rev` at `rx.recv()`) and applied the batch afterward. The combinator `watch_applied` exists to encode the correct discipline once.

**The resume guarantee.** This library's contract is "resume from a sequence number after any restart." `watch_applied` sharpens that into a single invariant:

> A persisted/reported cursor `C` ⟹ every update with revision ≤ `C` has been **applied** — the caller's `apply()` has returned for it.

The cursor is written from `apply()`'s completion, never from the channel's delivery. Concretely, on each flush the combinator runs `apply(batch)` to completion, *then* sets `cursor = batch_high`, *then* checkpoints the snapshot at that cursor, *then* fires `on_applied`. Nothing advances the cursor before `apply` returns.

**Why receipt is the wrong signal.** Bumping the cursor at `rx.recv()` and applying later opens a crash window: the persisted cursor claims "caught up to rev N" while rev N still sits in an unapplied batch buffer (or in flight to a separate apply task). On crash+resume the watch re-arms at `cursor+1`, *past* the unapplied rev N, and silently skips it. The data is gone with no error — a hole in the exact guarantee the crate advertises.

This is the lesson of Saltzer, Reed & Clark, *End-to-End Arguments in System Design* (1984): a checkpoint placed below the endpoint — here, at the transport's delivery rather than at the application of the update — can only ever be a performance hint, never a correctness guarantee. The "it happened" property can only be established at the endpoint that actually performs the work, so the cursor is sourced from `apply`, not from `recv`. The cursor-as-monotonic-index shape itself is the HashiCorp Consul anti-entropy / blocking-query lineage: a client re-arms its watch from the last index it *reconciled*, never from the index it merely *saw*.

**Cursor authority covers rejected entries.** `batch_high` tracks the highest revision *received* since the last flush, including updates that `parse` rejected (corrupt bytes, irrelevant keys). A rejected entry is still "nothing to apply," so it is covered by the cursor — and because NATS delivers in revision order, advancing to the max revision after one atomic `apply` is sound: having seen the max means every revision below it has been seen too. Without this, a run of irrelevant keys would pin the cursor in place and force redundant replay on every restart.

The one exception: an update carrying the **unknown** version (an unparseable ACK subject on the hand-built multi-prefix consumer path) never touches `batch_high` — it can neither mint a cursor position nor clobber the real high from earlier in the batch. The update is still applied; skipping it under-advances at worst, and re-delivery on resume is idempotent. (The pre-guard code fabricated revision 0 for these, which the loop adopted as a real position — regressing the persisted cursor to 0 and forcing a full replay on the next restart.)

**Persist the store's cursor, not `on_applied`'s, when there is a store.** Across a transient store failure the batch is re-queued, but `apply` has run and `on_applied` fires with the advanced cursor — ahead of what the store made durable. Resume from the cursor the store reports (`load`/`open`), never from one persisted separately in `on_applied`: that one can skip the re-queued batch. Without a store, `on_applied`'s cursor is the one to persist.

**Snapshot consistency.** Raw `KvUpdate`s stream to the snapshot log as they arrive, but the *checkpoint* cursor is the post-apply cursor. A crash after a raw record is written but before its `apply`/checkpoint leaves the log holding data *ahead* of its cursor — which is safe: the cursor never names a revision whose `apply` had not returned, so resume re-delivers and re-applies that tail rather than skipping it. Compaction runs off the hot path via `spawn_blocking`, as everywhere else in the snapshot subsystem.

**Flush triggers.** A batch flushes when any of these fires: the `window` elapses, `batch.len()` reaches `config.max`, a shutdown is signalled, or the channel closes with a pending batch (the remainder is flushed before returning).

**Transient store failures re-queue, not drop.** If `store.apply()` returns an error, the raw batch is prepended to the next flush's accumulation and the watch continues. The streak counter increments; at 16 consecutive failures the watch fail-stops with `KvError::WatchError`. Dropping the failed batch and continuing was the shipped behavior until `transient_store_failure_never_leaves_a_cursor_gap` reproduced the bug: a transient failure followed by a successful flush advanced the cursor over a hole that survived every restart, because the restart re-folds from the advanced cursor, skipping the missing range. Cursor authority requires the store's cursor and contents to advance together, always (`Fold::flush` in `applied.rs`, `tests/model_applied.rs`).

**The store closes off the runtime.** However the watch ends — shutdown, stream close, or any error — `watch_applied` hands its store to a dedicated thread to drop, and waits up to 30 s for it (`close_off_runtime`). A store's destructor may block: fjall 3.1.x's `Database::drop` can deadlock joining its worker pool. Dropped on the runtime, that parked the only tokio worker of a `CPUQuota=100%` service and froze the whole process before it could act on the watch's error. A normal close finishes inside the wait, so the caller can reopen the same path; a hung close leaves its thread behind and the watch returns anyway. A std thread, not `spawn_blocking`, because runtime shutdown waits on blocking tasks (`a_store_that_hangs_on_close_cannot_freeze_the_runtime`, `the_store_is_closed_before_watch_applied_returns`).

**Resuming in-process (`watch_applied_owned`).** Closing on every exit makes a watch error fatal to a caller whose readers hold the store (a fold's `FjallReader` keeps its database alive). Such a caller can't re-open the path while those handles live, so its only recovery was a process restart, which a NATS heartbeat miss forced. `watch_applied_owned` is the same loop without the close. Its `WatchEnd` carries the result, the resume cursor, and the store itself, intact on every exit. With a store the resume cursor (`WatchEnd::applied`, and the `Ok` of `result` and of `watch_applied`) is the store's own persisted cursor, read after the last flush, never the in-memory one: a transient store failure leaves the in-memory cursor ahead of the store and drops the re-queued batch with the run, so resuming from the in-memory cursor skipped that batch for good (`in_process_resume_after_failed_final_flush_leaves_no_hole`; the `mut_resume_from_memory` kernel mutation in `tests/model_applied.rs`). Without a store it is the in-memory applied cursor. `on_applied` is a notification, not a resume source with a store. The caller resumes by calling again with the same store and `resume = applied`: nothing is re-applied out of order and nothing is skipped, and cursor expiry and its repair take the same path as a fresh start. A store that comes back `None` was lost (a flush task panicked), and only a fresh open recovers it. `watch_applied` is `watch_applied_owned` followed by `close_off_runtime` (`owned_watch_hands_the_store_back_and_resumes_in_process`).

**Foreign cursor.** A resume that fails with `KvError::ForeignCursor` (or a live All-scope watch the floor guard ends with it, below) is repaired as a fold that holds data but has no cursor — the unanchored start below — against the stream that exists now: the key-listing diff on a bucket that keeps current values, an artifact restore otherwise, whose artifact must itself be this stream's (`repair::check_restore`). The repair first forgets the old cursor with everything it carried (`Fold::forget_cursor`): its position (so an artifact below the old rank is no longer "not ahead"), its incarnation, and the eviction memo (a fact about the stream that was evicting: the new stream's retention is read afresh, so an exact listing serves a recreated bucket that keeps its values). A restore over that fold replaces it with the artifact's, key for key (`restore_key`'s `local_known`), because the old stream's revision numbers decide nothing in the new one (fleet findings F2, F4). With `ExpiryRepair::None` or no store the error stands: a re-list alone would leave the old stream's keys in the fold. Nothing of the new stream is folded under the old cursor.

**Cursor-expiry repair.** <a id="cursor-expiry-repair"></a> The cursor expires at resume (`CursorExpired` from the `*_from` watch) or mid-watch (the live floor guard ends an All-scope watch with `CursorExpired` when retention overruns it). Both take one path. Correct means **every write minus every real delete** — not "whatever NATS still lists"; the two agree only when retention never evicts current values. The repair (`ExpiryRepair`, `src/repair.rs`) is chosen accordingly:

- **Key-listing diff** (`Relist`; `Auto` on a bucket that keeps current values). The watch task hands the main loop the reader and waits for an ack; the main loop lists the bucket's live keys, sorting them externally (`KeySorter`: 64 MiB runs spilled to the restore source's scratch dir under `Auto`, else `TMPDIR`), merge-joins that against the fold's own key-ordered scan — so neither the listing nor the fold is held in memory, only the stale keys — and runs a synthetic `KvUpdate::Delete` (unknown version — never advances the cursor) through `parse`/`apply`/store for each key that vanished; only then does the full-scope re-list (`watch_all` / `watch_prefix` / `watch_prefixes`) start. That ack ordering is the invariant: a synthetic delete always precedes the re-list put for the same key, so delete-then-recreate during the gap converges. (With the listing taken by the main loop after its drain, `tests/model_repair.rs` shows axiom 6 alone already keeps it correct; the barrier is defense in depth.) **Refused** — the watch fails — when the watcher reports retention that evicts current values (`KvWatcher::retention`, read live from the stream config), or the fold remembers seeing it do so (below): there it deletes valid keys. `tests/eviction.rs` reproduced exactly that against a live server before the fix.
- **Artifact restore** (`Restore`; `Auto` on a bucket that evicts current values or can't say). The watch task peeks the newest artifact's manifest and checks it before downloading: it must cover the watch scope (recorded in the manifest by the exporting `watch_applied`), and pass `protocol::restore_allowed` — strictly ahead of the local fold, cursor still inside retention. It then downloads and imports it into scratch as a temporary fold of the consumer's backend, re-checks what actually arrived, and hands it to the main loop. The main loop re-checks "ahead" against its own applied cursor (a mid-watch expiry delivered past the resume cursor) and diffs the in-scope fold against the artifact in one key-ordered pass: keys the artifact's entry should replace are spilled to a scratch file next to the imported artifact (`RestoredFold::scratch_in`), and local keys the artifact lacks are collected as delete candidates. Only if there are candidates does it stream the bucket's live keys past them (`KvReader::for_each_key`) and then re-read retention: the listing vouches for writes after the artifact's cursor only while the log still holds all of them, so if retention passed that cursor meanwhile the restore is refused before anything is folded (`tests/model_repair.rs`, `NoListingRecheck`). It then folds the difference through `parse`/`apply`/store in `max`-sized chunks, read back from the spill, then advances the cursor to the artifact's in one final commit and replies. Memory is the delete candidates — the keys deleted between the two cursors — never the fold, the artifact, or the bucket's listing, so a billion-key fold (or a fresh node seeding from an artifact) restores in bounded memory. The diff takes an artifact entry only when it is newer than the local one, and deletes a key the artifact lacks only when the bucket does not list it live; over a fold of another incarnation (a foreign cursor: the repair forgets that cursor, its eviction memo and its rank first, so the ahead check is against nothing) it takes the artifact's entries and deletes every other key. An artifact is complete *together with the log after its cursor* — a NATS cursor C means every retained message ≤ C is applied, not "the truth at C": a key whose latest write is after C may be missing or older in an artifact exported mid catch-up — and in both skipped cases the key's latest write is after C, so the resume delivers it. The consumer therefore never sees a key move backward or a live key deleted, not even transiently. The watch resumes from that cursor (which can itself expire later and go round again, each time only for a strictly newer artifact). A failed check fails the watch and logs the reason at `error`: a stale artifact has no safe recovery.
- **None** (`None`, or no store): the re-list alone, with a warning that deleted keys may persist.

Whichever runs, the main loop first **drains** the watch channel: a mid-watch expiry leaves delivered updates buffered there, and the repair must fold them first — one applied after the repair would resurrect pre-repair state and could move the cursor backward. The watch task is parked on the ack/reply, so the drain takes exactly that backlog.

A cursor-less start is repaired before it watches in two cases. A fold that holds **data but no cursor** (a torn first checkpoint, or a populated store started without one) runs the planned repair like an expiry at revision 0: a re-list alone never removes what it doesn't deliver. An **empty** fold with `Restore`/`Auto` on a bucket that has already evicted current values seeds from the artifact, since a re-list would be incomplete.

The listing counts as the truth — "not listed" means "deleted" — when retention never evicts current values, or when it can but has never evicted anything yet (first retained revision ≤ 1). Otherwise the key-listing diff is refused.

**Retention can be edited, so the fold remembers what it saw.** A key that aged out while `max_age` or `discard: old` was in force is missing from the listing forever after, even once the operator turns eviction off or recreates the bucket — and the fold, the only place it still exists, would have it deleted by a diff against that listing. So the first time the watch task reads retention and finds it has evicted (eviction on and the log past its first revision), it remembers: the main loop commits a memo on the fold's cursor (`WatchCursor::to_bytes`: a 12- to 28-byte encoding, longer than any bare version; a cursor with neither the memo nor an incarnation keeps the old encoding byte for byte), every later cursor carries it, and so does every artifact exported from the fold, so a node restored from one inherits it. A fold with the memo never treats the listing as the truth again (`listing_is_truth`'s `seen_evicting`; `Snapshot::stale_keys` reads it too). Retention is read at every watch start, at every expiry, and every 60 s during a live watch while a key-listing repair could still be planned (`Relist`/`Auto`) and the memo is unset. The axiom: a fold reads retention while eviction is in force. Eviction that starts, drops a key, and is turned off again entirely between two reads (within a minute, or while the node is down) is not seen. `tests/model_repair.rs` (`eviction_turned_off_repair_steps_are_correct`, mutation `NoEvictionMemo`) and `tests/eviction.rs::eviction_turned_off_never_trusts_the_listing_again` (live) pin it.

**A supersession-only gap resumes** (`NatsKvWatcher::resume_point`). The resume check sees only the stream's first retained revision, so when every message after a node's cursor was superseded while it was down — a small hot bucket on a short restart — the cursor looks expired though nothing aged out, and on an evicting bucket the restore needs an artifact newer than the node. A cursor with the memo also keeps the server time its message was written. When the window check fails, the resume reads the stream's config and the server's clock (`ts` in the raw stream-info reply, NATS 2.10+); if age is the bucket's only way to evict a current value (`max_age`, no per-message TTLs, no `discard: old` limit) and the cursor's message is at most half of `max_age` old (`protocol::gap_holds_no_eviction`: half is headroom for clock steps and leader changes), every later message is younger still, none has aged out, and each missing one was superseded by a later write the resume delivers — so it resumes from the first retained revision instead. Assumes the config didn't switch from a `discard: old` limit to age-only while the node was down.

Before diffing, both repairs flush until the store holds every delivered update: a batch re-queued by a transient store failure is invisible to a diff of the store, and a re-queued put of a key deleted during the gap would otherwise commit after the diff and resurrect it.

**The key-listing repair's one axiom.** Between taking the listing and the full re-list delivering what the bucket holds, retention must not evict a delete marker (axiom 6 of `tests/model.rs`, narrowed to this window). On a bucket that keeps current values, the only such eviction is an admin purge of markers, and NATS's `purge_deletes` keeps markers younger than 30 minutes by default. `tests/model_repair.rs` drops the axiom and the checker reaches the purge-inside-the-window trace; the artifact restore resumes on a floor-guarded watch and holds without it.

This is the layer the tunnel router (swap route table) and edge origin watcher (rebuild hashrings) both collapse onto: `parse` extracts the domain registration, `apply` swaps the live state, `on_applied` persists the cursor.

### Health snapshot (`health.rs`)

A watch that is stale, stuck, or refused a repair looks like a healthy one from outside, so `BatchConfig::health` takes a `WatchHealth`: one `Arc` of atomics plus a small mutex-guarded detail block. **Observation only**: the watch writes it and no cursor, repair or restore decision reads it (the tests in `tests/health.rs` and every protocol model run unchanged).

| Recorded | Where | Cost |
|---|---|---|
| applied revision | `Fold::flush`, after the cursor advances | one relaxed store per flush |
| last commit time | `Fold::flush`, when the store apply returns `Ok` (without a store, when the batch is applied) | one clock read + store per flush |
| apply in flight | `Fold::flush`, around the blocking `store.apply` (a drop guard clears it on return, panic, or a task that never starts) | one clock read + two stores per flush |
| repair state, cursor expiries | `run_watch`, at each repair step | rare |
| newest artifact | `Fold::export` (this watch's own export) and `restore_from_artifact` (the source's `latest()`) | rare |
| run start, end, last error | `watch_applied_owned`, once per run | rare |
| stream head and first revision | a sampler task, one `KvWatcher::log_range` request per 30 s (`NatsKvWatcher`: one stream-info call), spawned only when a handle is set and aborted when the run ends | one request per interval, never per update |

No allocation per update; the detail mutex is held for a few stores and never across an await, and `snapshot()` copies under it. A handle outlives the watch's runs, so `restarts` and `last_error` accumulate across the caller's restart loop.

**Repair state is sticky where it must be.** A refused or waiting repair ends the watch, and the caller's restart retries the same expired cursor, so `Refused` and `WaitingForArtifact` persist across runs and clear only when a repair succeeds (`Relisting` and `Restoring` never overwrite them). `WaitingForArtifact` means a restore found no usable artifact: none published, none readable, or the newest is not ahead of the fold while covering the scope. `Reseeding` marks a foreign cursor (recreated bucket) with a repair armed, replaced by the repair's own state; `ForeignCursorRefused` is sticky and marks the same event with no repair armed, where the error stands. `Refused` is every other refusal: an artifact outside retention or scope, a key-listing diff on an evicting bucket, or the main loop's authoritative not-ahead check. The split is made at the call site from the artifact's scope and cursor; `check_restore` itself is unchanged.

**Lag is `head - applied` at the last sample.** It is exact only for `All` scope. A `Prefix` or `Prefixes` watch's cursor advances only on matching keys, so it reads nonzero lag while idle; read it as a trend. `head_sample_age` says how stale it is.

**Failure modes.** A failed head sample leaves the previous one (its age grows). A backend with no `log_range` (the trait default) reports no head and no lag. `None` always means unknown, never healthy.

### Presets (`preset.rs`)

A `Preset` is data: a scope, an optional `(reader, restore)` pair, and a `BatchConfig`. `Preset::run` builds the `ExpiryRepair` and calls `watch_applied_owned`; it adds no mechanism and no decision. `full_replica` is `All` + `ExpiryRepair::Auto`; `node_slice` is `Prefix` + `ExpiryRepair::None`. Both require a store and an export receiver by type. What each does on cursor expiry is the repair's own behavior (see "Cursor-expiry repair"): the replica's fold ends exact or the watch fails, the slice's fold may keep keys deleted during the gap. A use case that needs a prefix scope **and** a restore (artifacts that cover the prefix) uses the low-level API with `ExpiryRepair::Restore`.

### Live retention floor guard (`stream_watch_floor_guarded`, `nats.rs:1042`)

NATS does **not** error when a live consumer's position is overrun by retention — it silently skips evicted messages (the same silent-clamp behaviour that `resume_window_ok` closes at resume time). For an All-scope watch, a skipped delete marker permanently diverges the fold, with zero log lines. The floor guard closes this mid-stream:

- **In-band (primary):** when a delivered revision jumps the frontier by more than 1, fetch `first_sequence` and call `resume_window_ok(frontier, first_seq)`. A benign interior gap — per-subject overwrites below the floor threshold — passes. Head eviction past the frontier ends the watch with `CursorExpired`, which `watch_applied` routes in process into the same [cursor-expiry repair](#cursor-expiry-repair) a resume takes — the artifact restore on a bucket that evicts current values, never the key-listing diff there. The check runs *before* the entry is processed: the fold never advances past unexamined evidence of loss.
- **Backstop (30 s):** when no deliveries arrive, the periodic probe catches the no-traffic case where there is no in-band evidence to act on.
- **Incarnation:** both probes read the stream's creation time next to `first_sequence`. A bucket recreated under a live watch makes the ordered consumer reattach and carry on from the old position in the new stream; a creation time other than the one the watch attached to ends it with `KvError::ForeignCursor` (`recreated_stream_trips_the_live_guard`), which `watch_applied` repairs as above.

A periodic-only design was rejected by the model checker: deliveries catching the frontier up past a gap between probes erase the evidence, leaving permanent silent divergence with the guard running. The in-band check is what makes the design correct (`tests/model_live_watch.rs`).

Scope: this guard is sound only for the All-scope unfiltered resume watch, where every stream message is deliverable and a delivery gap implies a missed message. Prefix-scoped watches deliver sparse revisions — gaps from non-matching subjects are indistinguishable from eviction gaps client-side — and retain the retention-outlives-lag operating axiom.

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

A token read off a watch also carries, outside its bytes and ignored by equality, the server time its message was written and the stream incarnation it was read from. The cursor built from it records both (see Watch resumption).

### Machine-Checked Protocol Kernels (`src/kernels/protocol.rs`)

Every decision the protocol's correctness rests on is a pure function in `src/kernels/protocol.rs`. Production code and the Stateright models (`tests/model*.rs`) call the **same functions** — not hand-synchronized copies. A change to any of them is re-verified against the full bounded state space on the next model run. A mutation is a cargo feature that breaks the kernel itself (e.g. `mut_relist_on_evicting`: the listing is "the truth" on an evicting bucket), never a re-implementation of the decision in the model; each mutation must produce a counterexample.

| Kernel | Production | Models |
| --- | --- | --- |
| `pointer_publish_allowed` | `transport` pointer swap | `model.rs`, `model_fleet.rs` |
| `payload_prunable` | `transport` prune | `model.rs` |
| `resume_window_ok` | `nats` resume paths (`create_resume`), floor guard | every model |
| `same_incarnation` | `nats` resume paths (`check_resume_window`), floor guard, `repair::check_restore` | `model_incarnation.rs` |
| `restore_allowed` / `restore_ahead` | `repair::check_restore`, the main loop's ahead re-check | `model.rs`, `model_live_watch.rs`, `model_repair.rs`, `model_fleet.rs` |
| `listing_is_truth` | `repair` (plan, cursor-less start), `Snapshot::stale_keys` | every model's view of retention |
| `plan_repair` | `repair::plan`, `Snapshot::stale_keys` | every model's expiry dispatch |
| `cursorless_start_needs_repair` | `repair::run_watch` | `model_repair.rs`, `model_fleet.rs` |
| `restore_key` | `repair::restore_diff` | `model_repair.rs`, `fleet_proofs.rs` |
| `cursorless_fold_is_this_stream` | `repair::run_watch` (a fold with data and no cursor) | `tests/recreated_fleet.rs` (live), Verus |

**The cursor-authority kernel** (`src/kernels/applied.rs`) is the one stateful kernel: `step(state, event) -> (state, actions)`, no I/O, async or clock. `applied::Fold` turns what it observes (a received update, a flush request, a store result, an export or repair request) into an `Event`, calls `step`, and runs the returned `Action`s through its interpreter; it decides nothing itself. The kernel owns what has no other home: whether a batch is open, whether a stream position is pending, the unsynced eviction memo, the store-failure streak, and where an in-flight flush or resync is. It decides which flush steps run and in which order (domain `apply` before the cursor moves), that a failed store fold re-queues its raw batch at the front (and the streak bound that fail-stops), that an export flushes first, that the resync's ack follows the folded deletes, and the artifact restore's order: ahead check, diff, listing with its window re-check (only when there are delete candidates), chunks folded under the old cursor, the cursor commit, then the reply; a refusal (not ahead, window aged) replies with an error before anything is folded. The restore's phase is kernel state, so an event out of phase is ignored and a restart (a fresh state) re-runs the whole repair. The data (updates, cursors, the store) stay in the shell. The three applied models call it too (`tests/model_applied.rs`, `tests/model_resync_order.rs`, `tests/model_repair.rs`, through `slipstream::cursor_kernel`): they keep only environment state (deliveries, store contents, crashes, the bucket) and feed `step` events. Their mutations are cargo features that break the kernel itself (`mut_drop_failed_batch`, `mut_resume_from_memory`, `mut_no_ack_barrier`, and for the repair steps `mut_no_drain`, `mut_no_settle_retry`, `mut_cursor_before_diff`, `mut_no_restore_guard`, `mut_no_listing_recheck`; the protocol-kernel ones `mut_relist_on_evicting`, `mut_unanchored_relist`, `mut_restore_ignores_versions`, `mut_restore_ignores_listing`, `mut_restore_trusts_foreign`, `mut_pointer_ignores_incarnation`, `mut_no_eviction_memo` break `protocol.rs` (and `mut_foreign_keeps_memo` the repair's forgetting of a foreign cursor)); each turns its model red, and they are for model tests only, never production builds.

**`pointer_publish_allowed(current: &PointerState, candidate: Pos) → bool`**

The monotonic pointer guard. `Pos` is `(origin, rank)`: the stream incarnation a cursor was taken from (its creation time; 0 for a cursor that records none) and its revision. Ranks are ordered only within an incarnation; nothing orders two incarnations, and no clock is assumed. Returns `true` for an open slot, a corrupt pointer, a pointer of another incarnation than the candidate's, or a candidate at or above the existing cursor in its own incarnation — and `false` exactly when the existing pointer is parseable, of the candidate's incarnation, and higher.

```
Absent                      → true   (open slot)
Present { at: None }        → true   (corrupt pointer is replaceable, not a wedge)
Present { at: Some(p) }
  other origin              → true   (a dead incarnation's pointer is replaceable)
  same origin, rank >= p    → true
  same origin, rank <  p    → false  (stale publish refused)
```

A pointer of a dead incarnation never refuses a publish of the incarnation that replaced it, whatever its rank, so a recreated bucket's first export replaces it; the guard compared ranks only before, and a dead pointer ranked above the new head refused every publish of the new stream (fleet finding F1). Only one incarnation is live at a time: a slow exporter of the dead stream can replace the live pointer once, the next live publish replaces it, and restorers refuse it meanwhile (`same_incarnation`). The cost (a stale live exporter publishing a payload a prune deleted) is finding F1b below.

Machine-checked as: _"published cursor never regresses"_ (`tests/model.rs`, one incarnation) and, within an incarnation `transport_proofs::the_pointer_never_regresses`, and `protocol::a_pointer_of_another_incarnation_never_refuses_a_publish` (liveness, no time axiom; Verus).

**`payload_prunable(payload, pointer, is_pointer_target, aged_out) → bool`**

The prune guard. A payload is deletable only when all hold: it is not the pointer's target, its position (`Pos`, parsed from the file name: `<origin>-<rank>-<digest>.tar`, or `<rank>-<digest>.tar` for an unrecorded incarnation) is parseable AND strictly below the pointer's, and its age has cleared the grace period.

```
!is_pointer_target && aged_out && payload.is_some_and(|p| p < pointer)
```

Strictly-below (not `<=`) is the structural fix that makes dangling pointers impossible: `pointer_publish_allowed` refuses any candidate strictly below the pointer, and the pointer is monotone — so anything this guard deletes can never be successfully published afterward. The model checker found a dangling-pointer counterexample under an earlier age-only rule; this guard is the fix.

Machine-checked as: _"pointer target always fetchable"_ under zero-grace pruning.

**`resume_window_ok(revision: u64, first_sequence: u64) → bool`**

The cursor-expiry guard. Resume reads `revision + 1` onward; it is sound iff `first_sequence ≤ revision + 1`. NATS does not error on a below-head start — it silently clamps to `first_sequence`, skipping evicted delete markers with no fallback. This check is ours to make.

```
first_sequence ≤ revision.saturating_add(1)  → sound
first_sequence >  revision + 1               → CursorExpired
```

Machine-checked as: _"bootstrap never silently diverges."_ Empirically pinned by `tests/resync.rs::nats_silently_clamps_resume_below_first_seq`.

**`same_incarnation(cursor_origin, revision, stream_origin, last_sequence) → bool`**

The incarnation guard. A cursor is this stream's when its recorded origin equals the stream's creation time. A cursor with no recorded origin (written before cursors recorded one) is admitted when `revision ≤ last_sequence`, so a fleet upgraded in place keeps restarting.

```
cursor_origin = Some(o) → o == stream_origin
cursor_origin = None    → revision <= last_sequence
```

Machine-checked as: _"no fold resumes into a different incarnation"_ (`tests/model_incarnation.rs`): under the shipped policy for every recorded cursor, with the one unrecorded-cursor hole derived as a `sometimes` witness; under the alternative that refuses unrecorded cursors, for every cursor. The pre-fix sequence-only guard violates it in three steps (recreate, churn, resume).

**`restore_allowed(artifact_revision, local_revision, first_sequence) → bool`**

The restore guard. An expired fold may be replaced by an artifact only if the artifact is strictly ahead of it (a restore never moves the fold, or the consumer's domain state, backward) and its cursor is still inside retention (`resume_window_ok`), so the resume from it is gap-free.

```
artifact_revision > local_revision && resume_window_ok(artifact_revision, first_sequence)
```

Machine-checked as: _"bootstrap never silently diverges from every write minus every real delete"_ on an evicting bucket (`tests/model.rs`), and _"a repair never moves the fold backward"_ (`tests/model_live_watch.rs`, where removing the guard is a mutation the checker catches).

**`listing_is_truth(evicts_current_values, first_revision, seen_evicting) → bool`**, **`listing_truth(retention, seen_evicting)`** and **`plan_repair(mode, listing_truth) → RepairPlan`**

Whether "not listed" means "deleted": yes when retention never evicts current values, or when it can but has evicted nothing yet (first retained revision ≤ 1) — and never once the fold has seen the answer be no (`seen_evicting`, the cursor's memo). The planner turns that (or `None`, a backend that can't say) and the armed `ExpiryRepair` into the repair: `Relist` where the listing isn't the truth is `RefuseRelist`; `Auto` re-lists only on `Some(true)` and restores otherwise.

**`cursorless_start_needs_repair(mode, fold_has_data, listing_truth) → bool`**

A start with no cursor repairs first when the fold holds data anyway (a re-list never removes what it doesn't deliver), or, with a restore armed, when the listing isn't the truth (a re-list would be incomplete).

**`restore_key(local, artifact, identical, listed_live, local_known) → KeyRestore`**

The per-key restore decision: take the artifact's entry unless the local one is newer (never a move backward), and delete a key the artifact lacks unless the bucket lists it live (never a phantom delete). Both hold for a fold of this stream (`local_known`). A fold whose cursor was another incarnation's (`KvError::ForeignCursor`, repaired as a cursor-less fold) is not: its revision numbers are the dead stream's, so the restore makes it the artifact's key for key (`mut_restore_trusts_foreign` is the pre-fix decision). `tests/model_repair.rs` catches both pre-fix variants as kernel mutation features (`mut_restore_ignores_versions`, `mut_restore_ignores_listing`).

### What is proven (Verus, `mise run verify`)

The kernels are checked twice: Stateright finds counterexamples in a bounded world, and Verus proves the kernel's own decisions for every input, with no bound. The proofs live inside `verus!` blocks in `src/kernels/`; a normal `cargo build` (and a published crate) erases them, so nothing but the `vstd` macro crate is added. `mise run verify` (CI job `Verify (Verus)`) runs `cargo verus verify` with the Verus release pinned in `dev/verus/VERSION.env`, in a fresh target dir, and must report 0 errors. No `assume`, `admit` or `external_body` stands in for kernel logic. Every `mut_*` feature turns at least one obligation red (`mise run verify -- -- --features mut_x`).

`src/kernels/protocol.rs`: each function has an `ensures` giving exactly what it decides (`*_spec` is the readable table; Verus checks the code equals it), and these theorems:

| Theorem | Where | Says |
| --- | --- | --- |
| `a_publish_never_regresses_the_pointer` | `protocol.rs:91` | an admitted publish never lowers a parseable pointer: its rank is monotone |
| `a_publish_at_or_above_the_pointer_is_admitted` | `protocol.rs:104` | liveness: a publish at or above the pointer (or over an open or corrupt slot) is always admitted |
| `a_pruned_payload_is_never_published_after` | `protocol.rs:162` | what the prune guard deletes can never be published later: no dangling pointer, at any timing |
| `the_target_and_unknown_objects_are_never_pruned` | `protocol.rs:178` | the pointer's target and unparseable objects are never pruned |
| `a_fresh_artifact_is_always_restorable` | `protocol.rs:315` | liveness: an artifact ahead of the fold and inside the window is always admitted |
| `only_an_explicit_relist_can_be_refused` | `protocol.rs:451` | liveness: `Auto` and `Restore` are never refused; a refusal exists only for an explicit `Relist` on a listing known not to be the truth |
| `restore_key` ensures | `protocol.rs:526` | a newer local entry is kept (never backward); a key listed live is never deleted; a delete needs a local entry the artifact lacks |
| `resume_window_ok` / `resume_start_sequence` ensures | `protocol.rs:215`, `:625` | the window and the next sequence are exact in unbounded integers; no wrap at `u64::MAX` |

Proving `gap_holds_no_eviction` found a real overflow: with `max_age = cursor_age = u64::MAX` the saturating multiply made the check pass for a cursor older than the retention. It now compares `age <= max / 2`.

`src/kernels/applied.rs` (`step` is proven equal to `step_spec`, its whole transition table; no path emits more than the action list holds) and `src/kernels/applied_proofs.rs`. The cursor-authority invariant is stated over the kernel state plus ghost positions the shell keeps (`World`: delivered, applied, store cursor, raw queue start, store contents, the running fold, the restore's artifact), with `w_step` giving what the shell does with each action. It is inductive over every event and unbounded in positions and queue lengths:

| Theorem | Where | Says |
| --- | --- | --- |
| `a_fresh_run_satisfies_the_invariant` | `applied_proofs.rs:169` | `inv(init)` |
| `every_event_preserves_the_invariant` | `applied_proofs.rs:179` | `inv(w) && pre(w, e) ==> inv(w_step(w, e))` for ALL events |
| `a_restart_preserves_the_invariant` | `applied_proofs.rs:190` | a crash or fail-stop at any point resumes from a cursor the store holds |
| `the_store_cursor_never_names_data_the_store_lacks` | `applied_proofs.rs:199` | `inv ==> store cursor <= the contents the store holds` |
| `only_a_landed_fold_moves_the_store_cursor` | `applied_proofs.rs:209` | only `StoreCommitted` moves what a restart resumes from |
| `the_cursor_commits_only_after_the_chunks_fold` | `applied_proofs.rs:225` | `CommitCursor` only from the `Fold` phase, reply next; the restore's phase chain and guards |
| `a_refusal_changes_nothing` | `applied_proofs.rs:255` | a refused restore emits only the error and leaves the state but for the phase |
| `the_resync_acks_only_after_its_deletes_are_folded` | `applied_proofs.rs:266` | the ack barrier |
| `a_repair_drains_then_settles_and_an_export_flushes_first` | `applied_proofs.rs:276` | repair and export order |
| `a_flush_applies_then_advances_then_folds` | `applied_proofs.rs:288` | flush order |
| `a_failed_fold_requeues_its_batch` | `applied_proofs.rs:303` | a non-fatal failure re-queues |
| `an_event_out_of_phase_is_ignored` | `applied_proofs.rs:317` | out-of-phase answers do nothing; an unpositioned update never records a cursor |
| `a_failed_fold_is_retried_until_it_lands_or_the_run_fail_stops` | `applied_proofs.rs:345` | liveness, rank `16 - fail_streak`: each failure lowers it or fail-stops |
| `a_flush_with_anything_owed_always_folds` | `applied_proofs.rs:366` | liveness: the retry always starts a fold |
| `a_landed_fold_brings_the_store_to_the_head` | `applied_proofs.rs:380` | liveness: one landed fold after any failures brings the store to the head |
| `a_failed_fold_leaves_the_store_behind_and_retryable` | `applied_proofs.rs:401` | liveness: after a failure the batch is still owed, the invariant holds, the rank fell |
| `every_restore_answer_lowers_the_rank` | `applied_proofs.rs:435` | liveness: restore phases rank 4..0; every answer lowers it |
| `a_restore_with_a_fresh_artifact_always_completes` | `applied_proofs.rs:446` | liveness: after a refusal the same request restores a fresh artifact to its reply |
| `a_resync_is_always_released` | `applied_proofs.rs:468` | liveness: the resync's ack is always released |

What the shell must supply (explicit preconditions, `pre` in `applied_proofs.rs`, not assumptions about the kernel): its loop is sequential (only `StoreDone` while a fold runs); no update is received while a restore runs; `ahead` is honest and the pre-repair settle has run; the run has a store; `AdvanceTo` follows only `CommitCursor`; restore chunks carry no stream position.

**Environment axioms** (the progress theorems rest on these and on nothing in slipstream's own code): *A1*, every store fold is answered (the disk and the blocking task return; whether the answer is `Ok` is not assumed, a fold that always fails ends the run through the failure streak); *A2*, the environment answers each repair action the restore asks of it (NATS answers the listing and retention reads; the disk returns the chunk folds).

Proving `step` equal to its spec found that a failure streak above the bound (unreachable: the run has fail-stopped) was left as it was; `step` is now total, the streak saturates at `MAX_STORE_APPLY_FAILURES`.

Each `mut_*` feature turns an obligation red (`mise run verify -- --features mut_x`): `mut_drop_failed_batch`, `mut_no_ack_barrier`, `mut_no_drain`, `mut_no_settle_retry`, `mut_cursor_before_diff`, `mut_no_listing_recheck` (and `mut_no_restore_guard`) fail `step`'s proof of equality with `step_spec`, whose theorems above are the properties those mutations break; `mut_resume_from_memory` fails `resume_from`'s postcondition; the protocol ones fail their `ensures`.

### What is proven about the fleet and the transport (Verus, no bound)

`tests/model_fleet.rs`, `tests/model.rs` check three nodes and one key (Stateright). `src/kernels/fleet_proofs.rs` and `src/kernels/transport_proofs.rs` are their unbounded twins: any number of nodes, keys, writes, evictions, restarts and stream recreations. The log is a spec (`Log`: writes, deletes, head eviction, incarnation); the nodes run the production cursor kernel (`applied::step_spec`, through `applied_proofs::w_step`, one event at a time) and the production repair decisions through their `*_spec` tables (`restore_allowed_spec`, `plan_repair_spec`, `listing_truth_spec`, `cursorless_spec`, `restore_key_spec`, `same_incarnation`, `publish_allowed_spec`, `prunable_spec`). Nothing restates a decision. A mutation forges a decision's input exactly as the `mut_*` feature forges the code.

| Theorem | Where | Says |
| --- | --- | --- |
| `a_fresh_fleet_satisfies_the_invariant` | `fleet_proofs.rs:1279` | base case: any number of fresh nodes over an empty log |
| `every_fleet_action_preserves_the_invariant` | `fleet_proofs.rs:1302` | `finv` is inductive over every action of every node: deliver, flush, crash at any point (even mid-restore), floor guard on a foreign cursor, key-listing repair, restore (begin, chunks, commit), publish; and write, delete, head eviction, stream recreation |
| `every_live_nodes_fold_is_complete_at_its_cursor` | `fleet_proofs.rs:1502` | a node of the current incarnation holds only real writes, and the truth at its cursor on every key not written since ("complete", not exact: F3) |
| `every_published_artifact_is_complete_at_its_cursor` | `fleet_proofs.rs:1517` | the same of the pointer's artifact |
| `a_converged_fold_is_the_truth` / `an_artifact_at_the_head_is_the_truth` | `fleet_proofs.rs:1532`, `:1545` | at the head every key is "not written since": exactly the truth |
| `a_restore_leaves_the_fold_complete_at_both_cursors` | `fleet_proofs.rs:896` | `restore_key` over every key leaves a fold complete at the old cursor (a restore cut short is safe) and at the artifact's |
| `a_restore_over_a_foreign_fold_is_the_artifact` | `fleet_proofs.rs:976` | a restore over a fold of another incarnation (any content) is the artifact, key for key: no premise on the old fold (F2) |
| `a_restore_never_moves_a_key_backward_or_deletes_a_live_one` | `fleet_proofs.rs:956` | the kernel's per-key guarantees over the whole key space |
| `the_trusted_listing_is_the_truth` | `fleet_proofs.rs:270` | a listing the planner trusts is the truth |
| `folding_the_log_forward_keeps_a_fold_ok` | `fleet_proofs.rs:335` | delivery from `a` to `b` keeps a complete fold complete |
| `a_node_is_converged_or_a_fair_step_lowers_its_rank_or_it_is_blocked` | `fleet_proofs.rs:1758` | liveness: a ranking function (`nrank`) every fair step lowers; a node is converged or blocked |
| `a_converged_node_unblocks_every_blocked_one` | `fleet_proofs.rs:1958` | liveness: a converged node's publish is admitted over any pointer (a dead incarnation's included: no hypothesis) and gives every blocked node an artifact |
| `finding_f1_*`, `a_newer_incarnation_replaces_any_pointer` | `fleet_proofs.rs:1633`, `protocol.rs` | a dead incarnation's pointer, whatever its rank, never refuses a publish of the current one (F1) |
| `finding_f4_*` | `fleet_proofs.rs:1651` | a node that sees a foreign cursor forgets the eviction memo and plans the exact listing wherever the listing is exact (F4) |
| `a_blocked_node_names_the_write_that_aged_out` | `fleet_proofs.rs:2011` | the one fail-stop is loud and exact: revision `first - 1` was evicted after the node's cursor and no artifact of this stream holds it |
| `every_step_preserves_the_invariant` | `transport_proofs.rs:183` | transport invariant over every interleaving of N exporters, pruners and fetchers with stale reads |
| `the_pointer_never_regresses` | `transport_proofs.rs:270` | the monotone-pointer theorem lifted to the whole protocol |
| `prune_never_deletes_the_pointers_target` | `transport_proofs.rs:296` | whatever a pruner read, what it may delete is not the pointer's current target |
| `a_fetch_gets_its_payload_or_a_detected_miss` | `transport_proofs.rs:314` | a fetch racing a prune gets the object its read named or a miss, never another payload; a miss means the pointer moved strictly past it |
| `a_fresh_read_never_misses_without_a_publish`, `a_lost_cas_means_a_publish_happened` | `transport_proofs.rs:334`, `:346` | liveness: a refused fetch retries; a lost CAS is paid for by a publish |

**The statement.** `finv`: every node of the current incarnation holds a real fold (every entry a write of the log) that is *complete* at its cursor `c`: equal to the truth at `c` on every key not written after `c`; every published artifact of the current incarnation likewise. Exact equality at `c` is not what the code keeps and is not an invariant (F3): an artifact at `C` is complete together with the log after `C`, and a restore keeps the stale entry of a key the bucket lists live (`restore_key`) because the resume delivers its later write. At the head every key is "not written since", so a converged node holds exactly the truth. The Stateright model (`tests/model_fleet.rs`: one key, atomic repair) states the stronger "equals the truth at its cursor" and passes only because of those bounds; the statement the code keeps is this one.

**Environment axioms (the liveness rests on these and on nothing in slipstream).** *F-a* NATS delivers a received revision while its window holds; *F-b* the disk works (a flush lands, chunks fold, the commit flushes); *F-c* the object store answers (publish, the artifact read); *F-d* quiescence: from some point the log takes no more writes, evictions or recreations and nodes no more crashes. For the transport: *O1* a payload path names its content for ever; *O2* the pointer is written only by a publish; *O3* a compare-and-swap succeeds exactly when the object is unchanged since the read. Also: **O4** (scope of the transport proofs) the pointer stays in one incarnation: nothing orders two incarnations and no clock is assumed (a pointer of another incarnation is replaceable; F1b is the cost); the retention config is constant inside an incarnation (the `seen_evicting` memo guards a toggle; a foreign cursor forgets it); positions are `u64`; the relist (key-listing repair) is one atomic step (its steps are `tests/model_repair.rs`, `tests/resync.rs`); a cursor with no recorded origin is `tests/model_incarnation.rs`'s.

**Findings the proofs surfaced** (each a Verus-checked concrete state of the shipped decisions, in `fleet_proofs.rs`; F1, F2 and F4 were real bugs, reached against a live nats-server in `tests/recreated_fleet.rs`, and are fixed; the proof no longer carries a premise that excluded them):

- **F1** (`finding_f1_*`) the pointer guard compared ranks only, never the incarnation. A pointer whose artifact was the old stream's, ranked above the new head, refused every publish of the new stream until the head passed it, while `same_incarnation` refused to restore from it: a node that must restore had no artifact though other nodes held the head. Reachable: `swap_pointer` read only `cursor.rank()`, and the exporter's candidate was whatever its fold held. Fixed: ranks are ordered only within an incarnation (`protocol::Pos`) and a pointer of another incarnation is replaceable, like a corrupt one, so the new stream's first publish replaces a dead incarnation's pointer; no clock and no order between incarnations is assumed. The payload name carries the incarnation so `prune` only judges payloads of the pointer's own incarnation (`payload_prunable`). Test `a_dead_incarnations_pointer_does_not_refuse_the_new_streams_publish`. The liveness theorem lost its hypothesis.
- **F1b** (`finding_f1b_*`) the price of F1's fix, reported rather than assumed away: with no order between incarnations a slow exporter of a dead incarnation can move the live pointer out of its incarnation, and a stale live exporter can then publish a payload the prune guard had already deleted (it was below the live pointer). The pointer then names a missing payload until the next live publish replaces it; restorers see a loud miss, never wrong data. `transport_proofs` proves the protocol within one incarnation (monotone rank, no dangling pointer, fetch and prune races). Closing F1b needs the swap to check its payload still exists, or an order between incarnations.
- **F2** (`finding_f2_*`) a restore over the fold of a recreated stream kept old entries that carried a higher revision number than the artifact's (`restore_key` decided by revision number), and refused an artifact below the old cursor's rank as "not ahead" (`restore_request` compared against the foreign cursor the fold still held). Reachable: `ForeignCursor` with a repair armed ran the restore over the old fold (`repair::run_watch`). Fixed: the repair forgets the foreign cursor (`Fold::forget_cursor`) and the restore of a fold of another incarnation takes the artifact key for key (`restore_key`'s `local_known`). Tests `a_restore_over_a_foreign_fold_replaces_entries_the_old_stream_numbered_higher`, `a_restore_over_a_foreign_fold_is_not_barred_by_the_old_cursors_rank`. A fold that holds data and no cursor (a torn first checkpoint) is judged by its own record: a fold persists the incarnation of its stream before its first data record (`AppendLogSnapshot::apply` writes an origin-only cursor record first, and `compact_to_file` keeps it ahead of the data; fjall and RocksDB commit data and cursor in one atomic batch, so the cursor's origin is the record). On start, `cursorless_fold_is_this_stream` compares it with the stream's incarnation: a match is this stream's (trusted, so "never backward" holds), another incarnation is foreign (the F2 path), and no record is foreign too (fail closed). The commits a repair makes under a cursor-less fold of this stream keep recording its incarnation (`Fold::stamp_incarnation`); a restore over a foreign fold commits under no cursor, so a crash part-way leaves a fold that names no stream and the whole repair runs again. The last case is only files written before folds recorded an incarnation: a one-time re-seed on upgrade. Test `a_torn_first_checkpoint_of_the_old_stream_is_foreign_to_the_recreated_one`.
- **F3** (`finding_f3_*`) the statement above: complete, not exact.
- **F4** (`finding_f4_*`) the eviction memo (`seen_evicting`) rode the cursor and survived a recreation (the task's copy, and the fold's, which carried it over every cursor it advanced). On the new, un-evicted bucket the exact listing was distrusted and a restore planned; a fleet whose nodes are all foreign has no node to publish an artifact of the new stream, so none could start. Fixed with F2's `forget_cursor`: the memo goes with the cursor, and the new stream's retention is read afresh. Test `the_old_streams_eviction_memo_does_not_distrust_the_new_streams_listing`.

`mise run verify` runs both files. Mutations (`--features mut_x`) turn these red: `mut_no_restore_guard` fails `lemma_begin` (the artifact is ahead of the cursor) and `prog_repair` (the artifact is inside the window); `mut_relist_on_evicting` and `mut_unanchored_relist` fail `lemma_relist` (the re-listed fold is the truth); `mut_restore_ignores_versions` and `mut_restore_ignores_listing` fail `a_restore_never_moves_a_key_backward_or_deletes_a_live_one`; `mut_restore_trusts_foreign` fails `restore_key`'s contract, `a_restore_over_a_foreign_fold_is_the_artifact` and `finding_f2_*`; `mut_pointer_ignores_incarnation` fails `pointer_publish_allowed`'s contract and `finding_f1_*`; `mut_foreign_keeps_memo` fails `finding_f4_*` (and the live test); `mut_cursorless_fold_trusted` fails `cursorless_fold_is_this_stream`'s contract; `mut_resume_from_memory` fails `lemma_crash` and `a_restart_preserves_the_invariant`. `mut_no_eviction_memo` guards a retention toggle, which the closed world excludes; it fails `listing_is_truth`'s contract only. The `applied` ones (`mut_drop_failed_batch`, `mut_no_ack_barrier`, `mut_no_drain`, `mut_no_settle_retry`, `mut_cursor_before_diff`, `mut_no_listing_recheck`) fail `step`'s equality with `step_spec`, which every fleet node runs.

### Export Round State Machine

```
[Idle] ──ExportRequest──► [Acquiring]
                              │
                    try_acquire(ttl)
                         /         \
                      Won          Lost (another holder)
                       │               │
                [Exporting]        [Idle]
                       │
             flush pending batch
             store.export_to(dir)
             verify-by-reopen (cursor eq)
                       │
                 [Uploading]
                       │
              tar + multipart put
              pointer_publish_allowed?
              ├─ true  → CAS swap (≤8 retries)
              └─ false → SupersededByNewer
                       │
              Published    SupersededByNewer
                 │                │
          [Completing]      [Abandoning]
                 │                │
         complete(cursor)   abandon() → CAS delete
         prune(grace=4×ttl)
         delete local dir
                 │
            [Idle]
```

| State        | Entry Condition                          | On Failure                             |
| ------------ | ---------------------------------------- | -------------------------------------- |
| Acquiring    | `ExportRequest` received by watch task   | —                                      |
| Exporting    | Lease won (CAS create or takeover)       | Abandon lease; delete local dir        |
| Uploading    | Export + verify-by-reopen succeeded      | Abandon lease; delete local dir        |
| Completing   | `Published`                              | Prune/complete failures are non-fatal  |
| Abandoning   | `SupersededByNewer` or upload failed     | CAS delete failure is non-fatal        |

### watch_applied loop state transitions

| From | Trigger | Guard | What Actually Happens |
|---|---|---|---|
| Watching | `rx.recv()` delivers a `KvUpdate` | version not unknown | `batch_high` updated; raw update pushed to `raw_batch`; `parse()` called; window timer armed on first update |
| Watching | `rx.recv()` delivers an unknown-version `KvUpdate` | — | Same, except `batch_high` untouched — an unparseable revision neither mints nor clobbers a cursor position |
| Batching | Window elapses | — | `apply(batch)`; cursor = `batch_high`; `store.apply(raw_batch, cursor)` on blocking task; `on_applied` fired |
| Batching | `batch.len() == config.max` | — | Same as window flush |
| Flush | `store.apply()` succeeds | streak < 16 | Streak reset; `on_applied` fired; continue watching |
| Flush | `store.apply()` transient error | streak < 16 | Raw batch prepended to next `raw_batch`; streak++; warn; continue |
| Flush | `store.apply()` error | streak ≥ 16 | `KvError::WatchError` returned; watch fail-stops |
| Watching | Watch task gets `CursorExpired` (resume, or floor-guard trip mid-watch) | bucket keeps current values (or `Relist` chosen on unknown retention) | Drain channel + flush; key-listing diff: list keys → synthetic deletes → ack; then full re-list |
| Watching | Same | bucket evicts current values, `Restore`/`Auto` | Peek + check artifact (scope, ahead, fresh) → download → re-check → drain + flush → diff → chunked fold → cursor := artifact's → reply; resume `*_from(artifact cursor)` |
| Watching | Same | bucket evicts current values, `Relist` | Fatal: `KvError::WatchError` ("evicts current values"); fold untouched |
| Repair | listing / fold scan / download / check fails | — | Fatal: `KvError::WatchError`; restart re-runs the full resume → expiry → repair path (a part-done restore committed under the old cursor, so it re-runs idempotently) |
| Watching | `GuardTrip` (in-band gap or 30s backstop) | `!resume_window_ok(frontier, first_seq)` | Floor-guarded watch ends with `CursorExpired` → the repair rows above, in process |
| Watching | `ExportRequest` | — | Flush pending batch; `store.export_to()` on blocking task; reply on oneshot; continue |
| Any | Shutdown signal | — | Flush pending batch; abort watch task; return applied cursor |
| Any | Channel closes cleanly | — | Flush remaining batch; return applied cursor |

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

- **Fold + tail = truth.** The fold at cursor C, plus a resume from C, is every write minus every real delete. A NATS cursor means every *retained* message at or below it is applied — not "the truth at C": a key whose latest write is after C arrives with the resume.
- **Cursor-after-apply.** `apply` makes data and cursor durable together, so the cursor never names a revision whose data is absent — one transaction on a transactional backend, data-then-cursor on the append log (a torn write leaves data *ahead* of the cursor, which replay re-folds, never skips).
- **Replica of record.** NATS is a bounded log and keeps only the retained tail, so a fold can't be rebuilt from NATS alone. A tail lost to power loss (under a no-sync durability mode) leaves a consistent fold at an earlier cursor, rebuilt by resuming from the recovered cursor while it is inside NATS's retention, and by the cursor-expiry repair (an artifact restore) once it isn't. A lost or corrupt fold is rebuilt by importing an artifact, never by deleting it and re-listing NATS (complete only on a bucket that has never evicted a current value).

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

CRC covers from the type byte through the end of the record. A truncated final record (crash mid-write, or a short write when the disk ran out) is silently discarded: `load` rewrites the log without it, and `SnapshotWriter::open` truncates it away before appending (a truncate needs no free space; appending behind a torn record would corrupt the log mid-file for good). A CRC mismatch in the middle of the file returns `SnapshotError::Corrupted`, from `load` and from `SnapshotWriter::open`. Lengths are read before the CRC can be checked, so a corrupted length that points past EOF reads as a truncated tail and drops the records after it: the fold lands at an earlier cursor, the same shape as a tail lost to power loss. A value is at most 64 MiB (NATS's payload ceiling), enforced by the writer and the reader alike; a larger length is treated as corruption.

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

`checkpoint()` writes only a cursor record and calls `BufWriter::flush()` — a `write(2)` into the page cache. This survives a process crash but NOT a power loss. The only `fsync` is in `compact()`. A tail lost to power loss leaves a consistent fold at an earlier cursor, rebuilt by resuming from the recovered cursor while it is inside NATS's retention, and by the cursor-expiry repair (an artifact restore) once it isn't.

### Export Lease (`export_lease.rs`)

`ExportLease` coordinates fleet-wide at-most-one export rounds using a NATS KV key as a mutex, with expiry embedded in the value (no server TTL machinery required).

**Acquisition:**
- `create(key, value)` → wins if key absent; exactly one caller fleet-wide succeeds
- If key exists: parse `LeaseRecord` → compare `expires_at` to wall clock
  - Expired or unparseable → CAS `update(key, new_record, observed_version)` to steal
  - Live → return `None` (another holder is active)

**`LeaseRecord` wire format (JSON):**
```json
{ "holder_id": "node-42", "acquired_at_unix": 1718000000, "expires_at_unix": 1718000300,
  "completed_cursor_hex": null, "completed_at_unix": null }
```
After upload: `completed_cursor_hex` and `completed_at_unix` are filled in by `complete(cursor)`.

**`LeaseGuard` RAII discipline:**
- `complete(cursor)` → CAS-rewrite lease with cursor + timestamp (fleet-visible last-export record)
- `abandon()` → CAS-delete the key to free the round early (next trigger elects immediately)
- Drop without completing → lease expires naturally; next node steals after TTL

**Why embedded TTL over server TTL:** Portable to any `KvWriter`; no version/bucket-flag requirements. The cost is wall-clock comparison — acceptable because a premature steal merely produces a duplicate artifact (last-write-wins on the same key), never corruption. The lease is work-dedup, not a correctness gate.

**Why corrupt-lease stealing:** One unparseable value would otherwise wedge the fleet at `expires_at` forever. The same rule as `pointer_publish_allowed`'s treatment of corrupt pointers: unknown/corrupt state is replaceable, not a hard stop.

### Export / Import (replica bootstrap)

When the watched bucket is a **bounded** log (size-capped, history evicted), a fold is no longer rebuildable from NATS alone — the folds become the only full replicas. Export/import makes them transferable, which is what lets a new node, a node with a lost/corrupt fold, or a node whose cursor aged out of the log bootstrap at all.

**Artifact anatomy.** A directory: the backend's files under `data/`, plus `MANIFEST.json` carrying the artifact schema version, the backend identity and its on-disk format generation, per-file sizes + BLAKE3 digests, and — the load-bearing field — the **watch cursor the payload is exactly consistent with**. Import resumes the watch from that cursor and replays only the log tail. The manifest is written last and the whole stage is atomically renamed, so an artifact that exists is complete; a crash mid-export leaves only a hidden temp dir. Artifacts exported through `watch_applied` also record the exporting watch's key **scope** (schema 2; the manifest is rewritten atomically after the seal), which the expiry restore checks covers the restoring watcher. Schema 1 (no scope) is still read; builds older than schema 2 refuse schema-2 manifests, so upgrade importers before exporters.

**The cursor-consistency invariant.** `export_to(&mut self)` cannot run concurrently with `apply` (exclusive borrow), and inside `watch_applied` exports run between flushes via the `ExportRequest` channel (pending batch flushed first) — so the embedded cursor equals the applied cursor, exactly. Every backend re-proves it at export time by **reopening the copy** and checking cursor equality: because every `apply` commits the cursor in the same atomic batch as its data, a recovered cursor that matches the live one is a complete tail-loss detector.

**Per-backend export mechanics.**

- **append-log**: write a compacted log from the in-RAM fold (`compact_to_file`), verify by `load()`.
- **RocksDB**: the engine's native `Checkpoint` (memtable flush + SST hardlinks) — consistent by construction; verify-open anyway for the uniform guarantee.
- **fjall** (no checkpoint API): `persist(SyncAll)` (journal complete), best-effort quiesce (rotate memtables, drain flushes/compactions, bounded), copy the DB dir — hardlink immutable `tables/`/`blobs/`, byte-copy journal + metadata + `lock` — with a bounded retry against background GC, then verify-by-reopen. Correctness rests on the exclusive borrow + the verify gate, never on the quiesce.

**Import** stages a verified copy beside the destination (every hash re-checked — the transport that delivered the artifact is untrusted), opens the staged copy and gates on manifest-cursor equality, then atomically renames. A bad artifact never becomes a fold; a crash mid-import leaves nothing at the destination.

**Storage accounting.** Exports are hardlink-dominant (extra disk ≈ journal + small metadata, not 2×), but a lingering artifact pins files the source later compacts away — artifacts are **transient**: upload, then delete (`run_export_round` enforces this). Stage and destination must be on the fold's filesystem; the EXDEV fallback silently degrades hardlinks to full copies.

**Fleet coordination.** `ExportLease` makes exactly one replica perform a given export round: create-only CAS to win, expiry embedded in the value (no server TTL machinery), CAS takeover of expired/corrupt leases, `complete()` publishes the exported cursor on the key (the fleet-visible last-export record), `abandon()` frees a failed round early. Clock skew at worst causes a duplicate export — safe; the lease is dedup, not a correctness gate.

**Transport** (feature `transport`): `ObjectStoreTransport` adapts any `object_store` backend (S3, GCS, Azure, local). Object layout:

```
<prefix>/<key>.payloads/<origin hex>-<rank hex>-<blake3(manifest)[0..8] hex>.tar   ← content-addressed payload (write-once; a cursor with no recorded incarnation omits the origin)
<prefix>/<key>.manifest.json                               ← monotonic pointer (published last)
```

The payload key is derived from the manifest bytes, making collisions require hash breakage. The manifest is the single trusted object; the payload key is derived from it at download time — readers never trust a separately-stored pointer to the payload.

`run_export_round` composes the full at-most-once round: lease → export (via `ExportRequest`) → upload → complete → prune → delete-local. Per-backend `import_remote` composes: download pointer → derive payload key → download + verify → import.

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

### Why do watches deliver current state first (state-sync semantics)?

async-nats's bare `watch`/`watch_all`/`watch_many` ride `DeliverPolicy::New` — live updates only. A consumer built on that needs a separate `scan()` to seed, and any write landing between the scan and the watch attach is in neither — silently lost until the next reseed (the seed-then-watch race, demonstrated in `watch_prefix_relist_covers_seed_then_watch_gap`). Mapping every non-`_from` watch to `_with_history` (`LastPerSubject`) makes the watch itself deliver the seed, in revision order, with no race window: one primitive, correct by construction. The cost is a full re-list on every no-cursor watch start — which is what a no-cursor start *means*; consumers that have state resume with a `_from` variant and skip it.

### Why does the cursor-expired resync list keys instead of re-scanning values?

The fallback watch's re-list already carries every live key's value, so the resync only needs to learn which keys *no longer exist* — `reader.keys()` (headers only, no value bytes) is sufficient and cheap. The fold side of the diff is equally key-only: it streams via `for_each_in_range` rather than `range()`, so an All-scope resync against an on-disk backend never materializes the whole fold (values included) on the repair path. The synthetic deletes carry an unknown version and never advance the cursor: the fold's persisted cursor stays at its (expired) position until real re-list revisions move it, so a crash mid-resync just re-runs the same idempotent diff on the next start. Ordering, not versioning, provides correctness: the resync acks before the fallback watch is established, so a synthetic delete can never land after the re-list put that resurrects the same key.

### Why can't the key-listing resync be fixed for buckets that evict current values?

Because the information isn't there. With `max_age` or `discard: old`, a key missing from the bucket is either "deleted, and its delete marker was evicted" or "aged out, never deleted". Both leave nothing in the stream. A listing diff can only guess, and either guess loses data: delete and you drop valid keys (the bug `tests/eviction.rs` reproduced live); keep and you resurrect real deletes. Writes made during the gap that also aged out aren't in the stream either. The only place that still has every write minus every real delete is a fold that was watching at the time, so the repair restores from one: the newest published artifact. Choosing between the two repairs reads the stream's live config at the moment of expiry (`KvWatcher::retention`) rather than trusting `StoreConfig`, which only applies at bucket creation and can be edited out-of-band.

The same reasoning removed the old definition of convergence from the model. `tests/model.rs` used to judge a fold against what the bucket lists and never evicted a current value, so it certified the key-listing resync on evicting buckets as correct. It now judges against every write minus every real delete, has an `AgeOut` step, and keeps the old resync as a mutation the checker must catch.

### Why apply a restore as a diff instead of swapping in the artifact's files?

The result is a wholesale replacement within the watch scope, not a merge: every in-scope key ends up exactly as the artifact has it, and keys it lacks are deleted. But the mechanism is a diff folded through the normal path, for three reasons:

- **Crash safety without a new on-disk protocol.** Swapping a live LSM directory isn't atomic (rename can't replace a non-empty directory), and a half-swapped fold with a cursor is worse than a stale one. The diff commits each chunk under the old, expired cursor and moves the cursor to the artifact's only in the final commit, so a crash mid-restore leaves a fold whose cursor is still expired: the restart runs the restore again, idempotently. That's the same argument the key-listing resync already relies on.
- **The consumer's domain state.** `apply` built routing tables (or hashrings) from the old fold. A file swap would leave them stale with no signal; the diff delivers exactly the changes through `parse`/`apply`, the same way the synthetic deletes always have.
- **Bounded memory.** The diff holds keys only; values are read back from the artifact a chunk at a time.

The cost is a scan of both folds over the scope. Restores are rare, and the alternative costs correctness.

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

The standard async-nats path is tried first; raw is a fallback — taken on a parse-shaped error **or a timeout**. A timeout must not skip the fallback: Synadia Cloud, the deployment the raw path exists for, is also the most likely to be slow or distant, and the raw path's own per-op timeouts bound a genuinely dead connection anyway.

### Why subscribe-before-create in scan/keys?

async-nats ≤0.46 has a race: the server can deliver the first batch of push-consumer messages before the client's subscribe call completes. Creating the consumer first loses those early messages. Subscribing to the inbox first closes the race — the subscription is ready before the server can deliver.

### Why checkpoint() does not fsync?

Checkpoints are frequent (every N watch events). An fsync per checkpoint would add milliseconds of disk-sync latency to the hot watch path. A tail lost to power loss leaves a consistent fold at an earlier cursor (data and cursor commit together), rebuilt by resuming from the recovered cursor while it is inside NATS's retention, and by the cursor-expiry repair (an artifact restore) once it isn't — not a correctness failure. The only `fsync` is in `compact_to_file()`, where it guarantees the new compact file is durable before the atomic rename replaces the old one.

### Why raw backend-dir artifacts instead of a logical re-encode?

Export copies the backend's own files rather than re-encoding entries into a portable format. The engine's native integrity machinery (fjall journal CRCs + version checksums, RocksDB MANIFEST validation) then verifies the artifact on open — the same code path that validates the fold after a crash — and hardlinks make export cost ~O(metadata) instead of O(data). The trade is per-backend artifact formats; the manifest's backend identity + format generation make that explicit, and the engines' own format markers travel inside the payload as defense in depth.

### Why does export verify by reopening the copy?

fjall has no checkpoint API, so its copy is assembled from parts; rather than trusting that assembly, every backend opens the staged copy and requires the recovered cursor to equal the live fold's. Cursor-in-every-apply-batch makes this a complete tail-loss detector, and it reuses the engine's recovery as the oracle instead of reimplementing consistency checks. The cost — one extra open per export — is noise for a periodic exporter.

### Why is the export lease's expiry in the value instead of a server TTL?

Per-message TTLs need a new-enough NATS server, a bucket flag, and a backend that supports them; an `expires_at` inside the value works on any `KvWriter` and keeps acquisition/takeover as two plain CAS operations. The cost is wall-clock comparison across nodes — acceptable because a premature steal merely produces a duplicate artifact (last-write-wins on the same key), never corruption: the lease is work-dedup, not a correctness gate.

### Why a content-addressed, write-once payload + monotonic pointer instead of two fixed-key objects?

The legacy layout wrote the payload at `<key>.payload.tar` and the manifest at `<key>.manifest.json` as independent last-write-wins objects. The model checker proved two failure modes under that layout: (1) a cursor regression when a slow exporter's stale manifest overwrites a faster one's, and (2) a dangling pointer when the manifest advances past a payload that an older concurrent publisher still holds. The shipped layout fixes both structurally:

- **Content-addressed payload** — the key is `<origin>-<rank>-blake3(manifest)[..8].tar` (the cursor's incarnation and revision in the name let `prune` order a payload against the pointer; the digest is the content address). Two different artifacts can never share a key. Re-uploading the same artifact is an idempotent overwrite. A slow exporter's payload is valid until pruned; it just can't become the pointer.
- **Monotonic pointer via CAS** — `pointer_publish_allowed` refuses any candidate strictly below the current pointer in its own incarnation: a pointer of a bucket's dead incarnation, whatever its rank, is replaced by the recreated stream's first publish (it used to refuse every publish of the new stream until its head passed the old rank); no order between incarnations is assumed. Combined with CAS (no torn reads between observe and write), the pointer's rank is monotone non-decreasing within an incarnation. Slow exporters always get `SupersededByNewer`, never succeed at regression.
- **Strictly-below prune rule** — `payload_prunable` only deletes payloads of the pointer's incarnation strictly below it. Because the pointer is monotone and `pointer_publish_allowed` refuses `< pointer`, a pruned payload can never become reachable again. The model checker found the dangling counterexample under an earlier age-only rule; this is the structural fix.

Machine-checked as three theorems in `tests/model.rs`: published cursor never regresses, pointer target always fetchable (zero-grace prune), bootstrap never silently diverges.

### Why extract protocol guards into `src/kernels/protocol.rs` and share them with the model?

Production code and an exhaustive model checker running the same logic closes the drift gap between proof and implementation. Previously the models had their own inline copies; a change to the production decision would not update them, leaving the proof covering the old variant. Extracting every decision into `src/kernels/protocol.rs` and importing it from both call sites means every model run verifies the current production code, not a snapshot. The cost is a hard dependency: the models import `pub mod protocol` directly, so the module cannot go private.

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
| `watch_all()`            | `kv.watch_with_history(">")` — `LastPerSubject`: state-sync re-list, then live |
| `watch_prefix()`         | `kv.watch_with_history("{prefix}>")` — server-side subject filter, same re-list |
| `watch_prefixes()`       | `kv.watch_many_with_history([..])` — ONE multi-filter consumer (NATS 2.10)      |
| `watch_all_from(cursor)` | `kv.watch_all_from_revision(cursor+1)` — server-side delta delivery            |
| `watch_prefixes_from()`  | Hand-built ordered push consumer: `filter_subjects` + `ByStartSequence(cursor+1)` (async-nats has no `watch_many_from_revision`) |

## Trust Model

**What the store layer verifies:**
- NATS credentials are valid (at `connect()`)
- Bucket exists or can be created (at `store()`)
- Snapshot CRC per record (at `load()` and `compact()`)
- Snapshot magic bytes and format version (at `load()`)
- Resume window soundness — `resume_window_ok(revision, first_sequence)` before attaching (NATS silently clamps; we error explicitly)
- Stream incarnation — `same_incarnation` on every resume, restore and live probe (a recreated bucket restarts its revisions; its old cursors and artifacts are refused)

**What passes through unchecked:**
- Key names (no validation; NATS accepts any key)
- Value content (raw bytes; deserialization is the caller's responsibility)
- Bucket permissions (NATS auth rules govern access; the store layer does not re-check)
- Channel capacity (caller sets it; a full channel backpressures the watcher task)
- Snapshot cursor validity (stale cursors surface as `CursorExpired` from NATS, not from the snapshot layer)

**Why this is acceptable:** Applications own value encoding (JSON, proto, etc.). NATS owns authorization. The store layer is a transport adapter.

**What the transport layer verifies (import path):**
- Embedded manifest in the tar matches the pointer bytes downloaded from object store (content-address cross-check)
- Every payload file's BLAKE3 hash matches the manifest (per-file integrity)
- Recovered cursor of the staged copy equals the manifest cursor (tail-loss detector)
- Backend identity + format generation match the local node's backend

**What the transport layer does NOT verify:**
- Object store authorization (delegated to `object_store` credentials)
- Pointer freshness for manual bootstrap (callers decide whether the embedded cursor is recent enough). The automatic expiry restore does check it: `restore_allowed` refuses an artifact outside retention
- Whether the exporting node's fold was correct (garbage in, garbage out — the integrity guarantees cover transport, not source correctness)

## Failure Modes

| Failure                              | What Actually Happens                                                       | Recovery                                          |
| ------------------------------------ | --------------------------------------------------------------------------- | ------------------------------------------------- |
| Transient `store.apply()` failure    | Raw batch re-queued at front; next flush commits cumulatively; streak counter incremented | Automatic up to 16 consecutive failures |
| 16 consecutive `store.apply()` failures | Watch fail-stops: `KvError::WatchError` returned | Restart re-folds from last good cursor; investigate store (disk, permissions) |
| `store.apply()` never returns       | The watch stops advancing, with no error and no timeout, because the blocking call cannot be cancelled | Poll `WatchHealth::in_flight()` (or `snapshot().apply_in_flight`), alert on its age, and restart the process past your limit |
| Live watch retention overrun (All scope) | Floor guard detects in-band (gapped delivery) or via 30 s backstop; the watch ends with `CursorExpired` | `watch_applied` repairs in process (key-listing diff, or artifact restore on an evicting bucket) |
| Live watch retention overrun (Prefix scope) | **Undetected until the next restart** — prefix watches deliver sparse revisions, so eviction gaps are indistinguishable from non-matching subjects client-side; the floor guard is sound for All scope only (a periodic probe here would false-positive into spurious resyncs, a design the model checker rejected) | Next restart's resume-window check hits `CursorExpired` → resync repairs; operate prefix-scoped watches with retention window comfortably above the restart/redeploy interval |
| Bucket deleted and recreated, warm fold | The fold's cursor records the old incarnation: resume refuses with `KvError::ForeignCursor` (also when the new stream has grown past the old cursor) | `watch_applied` repairs as an unanchored fold against the new stream (re-list diff, or restore of an artifact from the new stream); with `ExpiryRepair::None` or no store the error stands: discard the fold |
| Bucket recreated, fresh node bootstraps from an artifact of the old stream | Same refusal at the resume, and the restore refuses the artifact up front (`check_restore`) | Publish a new export round from a node folding the new stream |
| Bucket recreated under a live All-scope watch | The floor guard's next probe (in-band gap or 30 s) reads another creation time and ends the watch with `KvError::ForeignCursor` | As above, in process |
| Cursor written before incarnations were recorded, from an older incarnation, inside the new stream's range | **Not detectable** (nothing records where it came from); admitted. Above the new head it is refused | Closes as folds restart on this version (every cursor a watch commits records its incarnation); to be sure after recreating a bucket, discard folds and artifacts written by 0.9.x |
| Resync `reader.keys()` failure       | Watch fail-stops (fail-stop, not degrade — degrade semantics proven to leave stale keys permanently) | Restart re-runs full resume → expiry → resync |
| `CursorExpired`, bucket keeps current values | `watch_applied` lists live keys, diffs fold, applies synthetic deletes, then falls back to state-sync re-list | Automatic; raw callers use `stale_keys()` manually |
| Cursor-expiry restore refused | The watch fails; the fold stays stale; the caller's restart retries the same expired cursor forever | `health.snapshot().repair` is `Refused(reason)` or `WaitingForArtifact(reason)`; publish a fresh artifact (inside retention, covering the scope) |
| `CursorExpired`, bucket evicts current values | `watch_applied` restores the in-scope fold from the newest artifact and resumes from its cursor | Automatic with `ExpiryRepair::Restore`/`Auto` |
| Fold holds data but no cursor (torn first checkpoint) | Repaired like an expiry at revision 0 before watching | Automatic with a repair armed |
| Every message after a node's cursor SUPERSEDED (every key rewritten while it was down), evicting bucket | On an age-only (`max_age`) bucket within half of `max_age` of the cursor's message: resumes from the first retained revision (`tests/eviction.rs::supersession_only_restart_resumes_on_a_max_age_bucket`). Otherwise (a `discard: old` limit or per-message TTLs, an older cursor, or one without the memo) the cursor reads as expired, the restore needs an artifact ahead of the node, and without one the watch fail-stops (safe, unavailable) | Restarts once the next export round publishes; export more often on small hot buckets |
| Eviction turned off (or the bucket recreated) after it dropped keys | The fold remembers it saw eviction (cursor memo) and never trusts the key listing again: `Auto` restores, `Relist` is refused | Automatic with `ExpiryRepair::Auto`; an artifact source is needed for good |
| A write ages out before ANY node folds it | Every node that needed it fail-stops (no fold anywhere holds it; `tests/model_fleet.rs` proves this is the only way the fleet fail-stops) | Data loss by retention; widen retention or export more often |
| Key-listing resync on a bucket that evicts current values | Refused: `KvError::WatchError` ("evicts current values"); fold untouched | Wire `ExpiryRepair::Restore`/`Auto` with an artifact source |
| Restore: no artifact ahead, stale artifact, or one that doesn't cover the scope | `KvError::WatchError`, reason logged at `error`; fold untouched | Fix the export pipeline (run exports well inside `max_age` / `discard: old` turnover); restart |
| `WatchError`                         | Watch stream dropped (NATS restart, reconnect)                              | Re-subscribe                                      |
| `Timeout` on any NATS op             | CLOSE_WAIT half-dead TCP parks the `await` without this guard              | Call `shutdown()` + `connect()`                   |
| `RevisionMismatch` on CAS            | Concurrent writer won the race                                              | Re-read with `entry()`, resolve, retry            |
| `AlreadyExists` on `create()`        | Key already present; caller's create was not exclusive                      | Read live value, decide whether to proceed        |
| Value over 64 MiB written to the append log | `apply` returns `InvalidFormat` before writing anything; under `watch_applied` the batch re-queues until the watch fail-stops | Unreachable from a NATS watch (its payload ceiling is the same); a direct `apply` caller must keep values under 64 MiB or use an LSM backend |
| Snapshot truncated tail              | `load()` discards partial final record; earlier records intact              | Resume from recovered cursor; tail re-folded      |
| Snapshot mid-file CRC mismatch       | `SnapshotError::Corrupted`                                                  | Import the latest artifact (a NATS re-list loses everything evicted; complete only on a bucket that never evicted a current value) |
| Snapshot wrong format version        | `SnapshotError::InvalidFormat`                                              | Same: import the latest artifact                  |
| `compact()` I/O error                | Writer poisoned; subsequent writes return `Io`                              | Reopen (the old file is intact until the atomic rename); if unreadable, import the latest artifact |
| Synadia Cloud stream limit           | Raw API path treats as non-fatal; verifies bucket with `get_key_value`      | Non-fatal if bucket exists                        |
| Multipart upload fails mid-stream    | The upload is aborted, so S3/GCS drop its parts; if the abort itself fails, a warning names the key | Next round re-uploads; a bucket lifecycle rule for incomplete uploads is the backstop (a crashed process can't abort) |
| Crash between payload upload and pointer swap | Old pointer remains fully consistent; payload orphaned | Next export round publishes new pointer; stale payload pruned after grace |
| Slow exporter after newer round published | `pointer_publish_allowed` returns false → `SupersededByNewer`           | Lease abandoned, local artifact deleted; payload orphaned until prune |
| Tampered / torn artifact             | `ArtifactInvalid` at import (hash + cursor gate); nothing written to destination | Fetch another artifact                        |
| Artifact backend/format mismatch     | `ArtifactInvalid` before any open; engine format markers re-checked internally | Fetch another artifact                        |
| Export under churn — fjall copy      | File GC'd mid-copy; retry ×3; verify-by-reopen catches anything torn       | Abandon lease on persistent failure               |
| Export/upload fails mid-round        | Lease abandoned (CAS delete); local artifact deleted                       | Next trigger elects a new node                    |
| Artifact cursor older than NATS log  | `CursorExpired` on resume → the cursor-expiry repair; on an evicting bucket a stale artifact fails the restore | Export more often than the log's retention window |
| Corrupt lease value                  | Treated as expired: CAS-stolen by next acquirer                             | Non-fatal; one bad write cannot wedge the fleet   |
| `prune()` I/O error                  | Stale payloads linger; warning logged                                       | Retried next export round; correctness unaffected |

## Package Structure

| File                       | What It Does                                                                         |
| -------------------------- | ------------------------------------------------------------------------------------ |
| `src/kv.rs`                | Core traits (`KvReader`, `KvWriter`, `KvWatcher`, `KvTtl`) and types (`KvEntry`, `KvUpdate`, `VersionToken`, `WatchCursor`, `KvError`) |
| `src/stores.rs`            | `Connection`, `KvStore`, `StoreConfig`, `StorageType`, `ConnectionCapabilities`      |
| `src/nats.rs`              | NATS JetStream implementation; bucket creation, scan consumer lifecycle, timeout wrapping, Synadia Cloud workarounds, resume-window checks (`create_resume`), the live floor guard |
| `src/protocol.rs`          | The protocol's decisions as pure functions (see "Machine-Checked Protocol Kernels"), called by both production code and the Stateright models |
| `src/snapshot.rs`          | `SnapshotStore` trait; append-only log + `AppendLogSnapshot` (default backend): `SnapshotWriter`, `load()`, `replay_log()`, `compact_to_file()` |
| `src/snapshot_fjall.rs`    | `FjallSnapshot`: on-disk `SnapshotStore` backed by fjall (`feature = "fjall"`)       |
| `src/snapshot_rocksdb.rs`  | `RocksDbSnapshot`: on-disk `SnapshotStore` backed by RocksDB (`feature = "rocksdb"`) |
| `src/snapshot_record.rs`   | Shared `[ver_len][version][value]` value-record codec for the LSM backends           |
| `src/artifact.rs`          | `ExportManifest`, `ArtifactFile`, BLAKE3 integrity; stage-then-rename discipline; backend `export_to` + `import` (append-log, fjall, RocksDB) |
| `src/export_lease.rs`      | `ExportLease`, `LeaseGuard`, `LeaseRecord`: fleet-wide at-most-one via embedded-expiry CAS |
| `src/transport.rs`         | `ObjectStoreTransport`, `ArtifactTransport` trait, `run_export_round`: monotonic pointer swap, multipart upload, prune, content-addressed keys (`feature = "transport"`) |
| `src/applied.rs`           | `watch_applied` cursor-after-apply combinator, generic over `SnapshotStore`: `WatchScope`, `BatchConfig`, the select loop, and `Fold` — the batch, applied cursor, and store every change goes through (ingest, repair corrections, flush, `ExportRequest` handling, which stamps the scope) |
| `src/health.rs`            | `WatchHealth`, `HealthSnapshot`, `RepairState`: the observation handle and the head sampler. Writes only; no decision reads it |
| `src/preset.rs`            | `Preset::full_replica` / `node_slice`: fixes scope and repair per use case, then calls `watch_applied_owned` |
| `src/repair.rs`            | Cursor-expiry repair, both halves: `ExpiryRepair`, `RestoreSource`, `RestoredFold`; the watch task (`run_watch`: plan, key listing, artifact checks and fetch) and the main loop's side (`fold_in`: runs the kernel's repair actions: drain, settle, key-listing diff, artifact restore into the `Fold`); scope coverage, restore checks, the in-scope restore diff |
| `src/lib.rs`               | Re-exports all public types; no logic                                                |
| `benches/`                 | Criterion benchmarks: snapshot write/checkpoint/load throughput, batch throughput, ACK subject parsing |
| `tests/integration.rs`     | NATS JetStream backend integration suite: each test boots its own `nats-server` on a free port; covers bucket create, CAS, watch semantics, cursor resume, and delete reconciliation |
| `tests/snapshot_store.rs`  | Backend-agnostic `SnapshotStore` conformance suite: every check is generic over the backend, instantiated for `AppendLogSnapshot`, `FjallSnapshot`, and `RocksDbSnapshot`; new backends get the whole suite for free |
| `tests/transport.rs`       | Integration: upload/download/manifest round-trip, pointer swap, prune, non-CAS fail-closed |
| `tests/transport_s3.rs`    | Live MinIO: CAS semantics (create, If-Match update, refusal, crash-window availability, prune) verified against a real object store |
| `tests/bootstrap.rs`       | Tier-2 bootstrap proofs on live NATS + on-disk backends: exports from a live `watch_applied` loop under churn, imports as a second node, and asserts **delta-only resume** via delivery count (not just convergence — a full replay would produce the same end state) |
| `tests/multi_export.rs`    | Prevention proofs on live NATS + real fjall/RocksDB: slow-exporter clobber refused, crash window keeps bootstrap available, multi-SST post-compaction fidelity |
| `tests/resync.rs`          | Live-NATS conformance: NATS silent-clamp pinned; full expiry → resync chain e2e; no-reader divergence pinned |
| `tests/repair_dst.rs`      | Deterministic fault injection over the REAL `watch_applied`, on every backend: a simulated NATS log and a fault store (transient failure, crash before/after/torn) at every store-apply call, across 17 scenarios (expiry repairs and the normal path; All, Prefix, multi-prefix; complete and mid-catch-up artifacts) × both delivery orders × batch sizes — ~32k schedules on the append log, every single fault on fjall and RocksDB (deep tier for the full sweep). Asserts convergence to every write minus every real delete, domain state == fold, cursor monotone, no revision regression or phantom delete seen by `apply`, and every mid-run export resumes to the truth. Reverting any of nine repair steps fails it |
| `tests/model_repair.rs`    | Stateright model of the repair AS STEPS (drain, pre-repair flush retry, diff, chunk, cursor commit, ack/reply) interleaved with deliveries, writes, eviction, publishes, transient failures, and crashes; checks that a resumable cursor reaches the truth, cursors are monotone, the domain never regresses or loses a current value, and every run converges; calls the production repair kernel (`cursor_kernel::step`); eleven mutations as `mut_*` kernel features, each caught |
| `tests/model_fleet.rs`     | Stateright fleet model where the exporters themselves expire, restore, start cursor-less, and publish their actual folds: every published artifact is the truth at its cursor (discharges the "exporters are correct" axiom); fleet-poisoning mutations caught |
| `tests/health.rs`          | Live NATS: the snapshot reports lag, apply in flight, a refused restore, a wait for an artifact, a cursor expiry with no repair, restarts and the last error; both presets build and run |
| `tests/eviction.rs`        | Live NATS + local object store, on `discard: old` and `max_age` buckets: expiry restores from the artifact (aged-out key kept, aged-out gap write recovered, real delete applied, identical to the exporter); key-listing resync refused; stale artifact fails the watch; a stalled, floor-guarded watcher overrun by a 20 MB burst trips mid-stream and is restored in process, identical to the exporter |
| `tests/model.rs`           | Stateright exhaustive model (fleet size 2–3, unbounded rounds): pointer swap theorems (no regression, no torn pair, no dangling pointer, terminal liveness) and convergence to every write minus every real delete, on buckets that keep and that evict current values (`AgeOut`, artifact restore); mutation tests prove each protocol guard load-bearing, and keep the key-listing resync on an evicting bucket as a mutation the checker must catch |
| `tests/model_applied.rs`   | Stateright cursor-authority model: delivery → flush → transient failure → crash/restart; calls the production `cursor_kernel::step`; `mut_drop_failed_batch` and `mut_resume_from_memory` kernel features pin both pre-fix bug classes |
| `tests/model_resync_order.rs` | Stateright resync ack-barrier model: calls the production kernel; proves synthetic deletes strictly precede re-list puts; `mut_no_ack_barrier` kernel feature reaches lost-recreate divergence |
| `tests/model_live_watch.rs` | Stateright floor-guard model: guarded variant proves the fold always converges to the truth, repaired by the key-listing diff or (evicting bucket) the artifact restore; unguarded variant pins permanent silent divergence; mutations: relist on an evicting bucket, restore without `restore_allowed` |
| `tests/common/`            | Shared test helpers: `nats.rs` (ephemeral NATS server, also the crate's own unit tests' via `#[path]`), `minio.rs` (MinIO harness), `crash.rs` (`ManifestPutCrash` transport injection, `feature = "transport"`) |

## Configuration

### StoreConfig (bucket creation only)

Config applies only at creation. If the bucket already exists, the existing one is returned as-is — `max_bytes`, `num_replicas`, `max_history`, `max_age` are ignored. To change settings on a live bucket, alter the underlying JetStream stream out-of-band.

| Field          | Default    | Rationale                                                          |
| -------------- | ---------- | ------------------------------------------------------------------ |
| `max_bytes`    | 10 MiB     | Required by Synadia Cloud; omit only for self-hosted NATS          |
| `max_history`  | 1          | Config stores rarely need change history                           |
| `num_replicas` | 1          | Set to 3 for production HA clusters                                |
| `max_age`      | None       | Set to gate retention window (also determines when cursors expire). Evicts current values: expired consumers must restore from artifacts, exported well inside `max_age` |

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

### BatchConfig (watch_applied)

| Field    | Default | Effect                                                                     |
| -------- | ------- | -------------------------------------------------------------------------- |
| `window` | 10 ms   | Max time a batch stays open before flush                                   |
| `max`    | 100     | Max updates per batch before early flush                                   |
| `channel_capacity` | 256 | Depth of the watch-task → loop channel. A full channel backpressures the watch task (by design); during state-sync hydration of a large bucket it can become the effective batch boundary, so tune it together with `max` |
| `health` | `None` | A `WatchHealth` the watch records into (see "Health snapshot"). Its `apply_in_flight` is the age of the current store apply; `apply` has no timeout, so this is how a consumer detects a store that stopped returning. |

| Constant | Value | Effect |
|---|---|---|
| `MAX_STORE_APPLY_FAILURES` | 16 | Consecutive transient store-apply failures before the watch fail-stops. Prevents the re-queued batch backlog from growing unboundedly under a persistently failing store. |
| `FLOOR_GUARD_INTERVAL` | 30 s | Period of the no-traffic backstop probe in the live floor guard. Primary detection is in-band (gapped delivery); this only fires when no deliveries arrive. |

### ObjectStoreTransport / ExportLease

| Setting                              | Default | Effect                                                                              |
| ------------------------------------ | ------- | ----------------------------------------------------------------------------------- |
| `prefix`                             | —       | Object-store key namespace; all payloads + pointer live under this prefix           |
| `allow_non_atomic_pointer`           | false   | Opt-in for `file://` stores that lack CAS (dev/test only); FAILS CLOSED by default  |
| `ExportLease::ttl`                   | —       | Round period; embedded in `expires_at`; determines prune grace (`4 × ttl`)          |

**Prune grace is `4 × lease_ttl`:** protects payloads a concurrent publisher may still reference (one TTL) plus in-flight bootstrap readers (three more TTLs of headroom). Increase if bootstrap download latency approaches `ttl`.

### Per-backend durability

| Backend          | Config     | Default    | Effect                                                                 |
| ---------------- | ---------- | ---------- | ---------------------------------------------------------------------- |
| `AppendLogSnapshot` | —       | page-cache | `checkpoint()` flushes to OS page cache; `compact()` fsyncs the new file |
| `FjallSnapshot`  | `sync`     | false      | `true` → fsync on every `apply` batch; `false` → OS page cache         |
| `RocksDbSnapshot` | `sync`    | false      | `true` → fsync on every `apply` WriteBatch; WAL always on              |
