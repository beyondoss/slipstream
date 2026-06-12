//! Cursor-after-apply watch combinator.
//!
//! [`watch_applied`] drives a [`KvWatcher`], batches incoming [`KvUpdate`]s over
//! a short window (or a max count), hands each batch to a caller-supplied
//! `apply` closure, and **only then** advances the resume cursor, checkpoints
//! the snapshot, and fires `on_applied`. It encodes one discipline that every
//! hand-rolled watch loop in the wider system gets subtly wrong:
//!
//! > **INVARIANT.** A persisted/reported cursor `C` implies every update with
//! > revision ≤ `C` has been *applied* — the caller's `apply()` has returned for
//! > it. The cursor never advances on *receipt* of an update, only after it has
//! > durably taken effect.
//!
//! ## Why receipt is the wrong signal
//!
//! The tempting shortcut is to bump the cursor as each update arrives off the
//! channel (`high_water = rev` on `rx.recv()`), then apply the batch later. On a
//! crash between those two steps the persisted cursor claims "caught up to rev
//! N" while rev N is still sitting in an unapplied buffer. On resume the watch
//! starts *past* rev N and silently skips it — a correctness hole in the exact
//! "resume after any restart" guarantee this crate advertises.
//!
//! Saltzer, Reed & Clark's *End-to-End Arguments in System Design* (1984) names
//! the fix: a function placed below the endpoints (here, the channel receive)
//! can only be a performance hint; the *endpoint* — the application of the
//! update — is the only place the "it happened" guarantee can actually be
//! established. So the cursor is written from `apply()`'s completion, not from
//! the transport's delivery.
//!
//! The cursor-as-monotonic-index-into-a-log shape itself follows HashiCorp
//! Consul's anti-entropy / blocking-query lineage: a client holds the last index
//! it has *reconciled* and re-arms the watch from there, never from the index it
//! merely *saw*.
//!
//! ## What the caller supplies
//!
//! - `parse`: maps a raw [`KvUpdate`] to an optional domain value `U`. Returning
//!   `None` (corrupt bytes, irrelevant key) is fine — the update is still
//!   *received*, so it still counts toward the cursor; there is simply nothing to
//!   apply for it.
//! - `apply`: consumes a `Vec<U>` in revision order. This is the only domain
//!   logic; for the tunnel router it swaps the route table, for the edge origin
//!   watcher it rebuilds the hashrings.
//! - `on_applied`: fires once per flush, *after* `apply` returns, with the new
//!   applied cursor. Callers use it to persist the cursor for the next restart.
//!
//! ## Panics
//!
//! `apply` runs inline on the watch task. If it panics, the panic propagates out
//! of [`watch_applied`] and aborts the watch — that is the caller's contract,
//! the same as a panic in any other supplied closure.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::{oneshot, watch};
use tracing::warn;

use crate::artifact::ExportManifest;
use crate::kv::{KvError, KvReader, KvUpdate, KvWatcher, WatchCursor};
use crate::snapshot::{SnapshotError, SnapshotStore};

/// A request, sent into a running [`watch_applied`] loop, to export the fold it
/// owns (see [`SnapshotStore::export_to`]).
///
/// `watch_applied` takes its snapshot store **by value**, so a consumer that
/// wants periodic artifacts of a live fold cannot call `export_to` itself. It
/// instead passes an `mpsc::Receiver<ExportRequest>` to [`watch_applied`] and
/// sends requests through the paired sender. The loop handles a request
/// between batch flushes — after flushing any pending batch — so the artifact's
/// embedded cursor is exactly the applied cursor at the moment of export.
///
/// The export result (or error) comes back on `reply`; an export failure is
/// reported there and the watch keeps running (the snapshot is a cache — a
/// failed artifact is the requester's problem, not the fold's).
pub struct ExportRequest {
    /// Where the artifact directory will be created. Must not exist (or be an
    /// empty directory); same filesystem as the fold for cheap hardlinks.
    pub dest_dir: PathBuf,
    /// Receives the sealed manifest on success. A dropped receiver is ignored.
    pub reply: oneshot::Sender<Result<ExportManifest, SnapshotError>>,
}

/// What to watch: every key, every key under a prefix, or the union of several
/// prefixes.
///
/// Mirrors the [`KvWatcher`] surface — `All` maps to `watch_all` /
/// `watch_all_from`, `Prefix` to `watch_prefix` / `watch_prefix_from`,
/// `Prefixes` to `watch_prefixes` / `watch_prefixes_from` (one multi-filter
/// consumer for the whole union, never one consumer per prefix).
#[derive(Debug, Clone)]
pub enum WatchScope {
    /// Watch all keys in the bucket.
    All,
    /// Watch only keys beginning with this prefix.
    Prefix(String),
    /// Watch keys beginning with ANY of these prefixes, on a single consumer.
    Prefixes(Vec<String>),
}

impl WatchScope {
    /// The scope as a list of key prefixes (`All` = the empty prefix), for
    /// callers that enumerate scope contents (live listings, fold ranges).
    fn prefixes(&self) -> Vec<String> {
        match self {
            WatchScope::All => vec![String::new()],
            WatchScope::Prefix(p) => vec![p.clone()],
            WatchScope::Prefixes(ps) => ps.clone(),
        }
    }
}

/// Internal: a cursor-expired resync handoff from the watch task to the main
/// loop. Carries the bucket's live key listing for the watch scope; the main
/// loop diffs it against the fold and applies synthetic deletes for keys that
/// vanished during the gap, then acks so the watch task can start the fallback
/// watch — the ack ordering guarantees every synthetic delete is applied before
/// the first re-list put arrives.
struct ResyncRequest {
    live_keys: Vec<String>,
    ack: oneshot::Sender<()>,
}

/// Internal: what the watch task needs to initiate a resync — the reader that
/// lists live keys and the channel into the main loop that owns the fold.
type ResyncHandle = (Arc<dyn KvReader>, mpsc::Sender<ResyncRequest>);

/// Batching policy for [`watch_applied`].
///
/// A flush fires when **either** bound is hit, whichever comes first: `window`
/// time has elapsed since the batch opened, or `max` updates have accumulated.
/// The window amortizes the cost of `apply` (e.g. one route-table clone per
/// flush instead of one per update); `max` caps memory and latency when updates
/// arrive faster than the window.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum time a batch stays open before being flushed.
    pub window: Duration,
    /// Maximum number of parsed updates in a batch before forcing a flush.
    pub max: usize,
    /// Capacity of the internal watch-task → main-loop channel. When the main
    /// loop falls behind (slow `apply`, blocking store flush), a full channel
    /// backpressures the watch task — that is the design — but during initial
    /// state-sync hydration of a large bucket the channel can fill faster than
    /// the window flushes, making *this* the effective batch boundary rather
    /// than [`max`](Self::max). Tune it together with `max` for high-fanout
    /// hydration; clamped to a minimum of 1.
    pub channel_capacity: usize,
}

impl Default for BatchConfig {
    /// 10 ms / 100 updates — the de-facto default every hand-rolled caller
    /// already used, lifted into one place — and the 256-deep channel the
    /// loop always allocated, now tunable.
    fn default() -> Self {
        Self {
            window: Duration::from_millis(10),
            max: 100,
            channel_capacity: 256,
        }
    }
}

/// Drive a watch with cursor-after-apply semantics.
///
/// Subscribes per `scope` (resuming from `resume` when it carries a position),
/// batches updates per `config`, applies each batch via `apply`, and only then
/// advances the cursor / folds the batch into `store` / calls `on_applied`.
/// Returns the final applied cursor when the watch ends (shutdown signalled, or
/// the underlying stream closed).
///
/// `store` is any [`SnapshotStore`] backend the consumer chose (the in-RAM
/// [`AppendLogSnapshot`](crate::AppendLogSnapshot) default, an on-disk backend, or
/// its own impl) — or `None` to run without persistence. On each flush, *after*
/// `apply` returns, the whole batch of raw [`KvUpdate`]s is handed to
/// `store.apply(batch, applied_cursor)` on a blocking task, so the store's
/// persisted cursor is always the post-apply cursor and never names a revision
/// whose `apply` had not returned. The store fold is atomic (data + cursor), so a
/// crash leaves the store consistent and resume re-folds only the tail.
///
/// # Cursor expiry and stale-key resync
///
/// On [`KvError::CursorExpired`] from the `*_from` resume path, this logs and
/// falls back to a full-scope watch (`watch_all` / `watch_prefix` /
/// `watch_prefixes`), whose state-sync re-list re-delivers the current value of
/// every in-scope key as puts. The re-list alone cannot cover keys that were
/// **deleted during the gap** and whose delete markers the backend has since
/// evicted — they simply don't appear, leaving the fold (and the caller's
/// domain state) holding them forever.
///
/// When both `reader` and `store` are provided, the expiry path closes that
/// hole: before the fallback watch starts, the bucket's live keys are listed
/// via `reader`, diffed against the fold's in-scope keys, and a synthetic
/// [`KvUpdate::Delete`] (with an unknown [`VersionToken`](crate::VersionToken)) is run through
/// `parse`/`apply`/`store` for each key that vanished. The synthetic deletes
/// are strictly ordered before the first re-list put, so a key deleted and
/// re-created during the gap converges correctly. Without a `reader` (or
/// without a `store` to diff against) the fallback is re-list-only and a
/// warning marks the possible stale keys.
///
/// A resync that was armed but FAILS (live-key listing or fold diff error) is
/// fatal to the watch — degrading to re-list-only would silently leave
/// deleted keys in the fold (`tests/model.rs` proves that divergence
/// reachable), so the error surfaces and the caller's restart retries the
/// resume → expiry → resync path from scratch.
///
/// See `ARCHITECTURE.md` ("Applied-Cursor Watch") for the invariant and its
/// rationale.
///
/// # Type parameters
/// - `U`: the caller's domain update type, produced by `parse` and consumed by
///   `apply`.
// This combinator takes each of its dependencies as a parameter so every
// caller-supplied closure (`parse`/`apply`/`on_applied`) keeps its own distinct
// type and is monomorphized at the call site. Folding them into a builder struct
// would either box the closures or force a single generic bundle, losing that.
#[allow(clippy::too_many_arguments)]
// The flush macro resets `batch_high`/`batch_deadline` for the next loop
// iteration. At the two flush sites that return immediately afterward (shutdown,
// channel-close) those resets are dead stores — correct, but flagged. The allow
// must sit on the function: a statement-scoped `#[allow]` inside the macro body
// trips the experimental attributes-on-expressions gate (E0658) on stable.
#[allow(unused_assignments)]
pub async fn watch_applied<U, S, P, A, O>(
    watcher: Arc<dyn KvWatcher>,
    scope: WatchScope,
    resume: Option<WatchCursor>,
    // `Some(reader)` arms the cursor-expired stale-key resync (see the function
    // docs); `None` keeps the re-list-only fallback. Only consulted on expiry —
    // the hot path never touches it.
    reader: Option<Arc<dyn KvReader>>,
    mut store: Option<S>,
    // `Some(rx)` arms an export-request arm in the select loop: each
    // [`ExportRequest`] is handled between flushes (pending batch flushed
    // first), so the exported artifact's cursor is the applied cursor (or,
    // across a transiently failed store flush, the store's own lagging but
    // self-consistent cursor — never a cursor past unfolded data).
    // `None` (or dropping the paired sender) leaves the loop's behavior
    // unchanged.
    mut exports: Option<mpsc::Receiver<ExportRequest>>,
    config: BatchConfig,
    mut parse: P,
    mut apply: A,
    mut on_applied: O,
    mut shutdown: watch::Receiver<bool>,
) -> Result<WatchCursor, KvError>
where
    U: Send,
    // `Send + 'static`: each flush moves `store` onto a blocking task to run its
    // (potentially blocking) `apply`, then takes it back — the same offload the
    // append log's compaction always used.
    S: SnapshotStore + Send + 'static,
    P: FnMut(&KvUpdate) -> Option<U> + Send,
    A: FnMut(Vec<U>) + Send,
    O: FnMut(WatchCursor) + Send,
{
    // The cursor we'll return. Initialized from the resume position so that a
    // watch which receives nothing new still reports the position it resumed
    // from as "applied" (it is — everything up to it was applied before the last
    // run persisted it).
    let mut applied = match &resume {
        Some(c) => c.clone(),
        None => WatchCursor::none(),
    };

    // The scope's prefixes, for the resync diff against the fold. Cloned out
    // before `scope` moves into the watch task.
    let scope_prefixes = scope.prefixes();

    // Cursor-expired resync channel, armed only when there is a reader to list
    // live keys AND a store to diff them against. The watch task sends the live
    // listing here and waits for the ack before starting the fallback watch, so
    // synthetic deletes always precede the re-list.
    let (resync_pair, mut resyncs): (Option<ResyncHandle>, Option<mpsc::Receiver<ResyncRequest>>) =
        match reader {
            Some(reader) if store.is_some() => {
                let (rs_tx, rs_rx) = mpsc::channel(1);
                (Some((reader, rs_tx)), Some(rs_rx))
            }
            _ => (None, None),
        };

    // Spawn the watch task. It owns the cursor-expired fallback so the main loop
    // only ever sees a clean ordered stream of updates on `rx`.
    let (tx, mut rx) = mpsc::channel::<KvUpdate>(config.channel_capacity.max(1));
    let handle = {
        let watcher = Arc::clone(&watcher);
        tokio::spawn(
            async move { run_watch(watcher.as_ref(), &scope, resume, resync_pair, tx).await },
        )
    };

    // Batch state.
    //
    // `batch_high` tracks the version of the most recently *received* update
    // since the last flush — including updates `parse` rejected. NATS delivers
    // in revision order, so the last received is the highest, and advancing the
    // cursor to it after a single atomic `apply` is correct: having seen the max
    // means we've seen everything below it, and a rejected entry is still
    // "nothing to apply", hence covered. Reset to `none()` after every flush.
    // Pre-size to the flush bound so no batch ever re-climbs the reallocation
    // ladder; `max(1)` only guards a nonsensical `max = 0` config.
    let batch_cap = config.max.max(1);
    let mut batch: Vec<U> = Vec::with_capacity(batch_cap);
    // Raw received updates for the durable `store`, in revision order. Only
    // populated when a `store` is present; the store folds the *raw* updates
    // (including ones `parse` rejected — they are still part of the bucket's
    // state), whereas the parsed `batch` above is the consumer's domain view.
    let mut raw_batch: Vec<KvUpdate> = Vec::new();
    let mut batch_high = WatchCursor::none();
    // Consecutive store-apply failures. A transient failure re-queues its raw
    // batch (cursor authority: the store's cursor and contents advance
    // together, always); a persistent streak fail-stops before the re-queued
    // backlog grows without bound.
    const MAX_STORE_APPLY_FAILURES: u32 = 16;
    let mut store_fail_streak: u32 = 0;
    // `Some` once a batch has opened and the window timer is armed; `None`
    // between flushes. Only the armed/idle distinction is read in the loop —
    // the absolute instant lives in the pinned `sleep` future below.
    let mut batch_deadline: Option<tokio::time::Instant> = None;

    // Flush the current batch, in order: run the domain `apply` (if non-empty) to
    // completion, advance the cursor, fold the raw batch + cursor durably into
    // `store`, then fire `on_applied`. The store fold runs on a blocking task
    // (its `apply` may block on I/O), moving the store in and taking it back — the
    // same offload the append log's compaction always used. A TRANSIENT store
    // error re-queues the raw batch for cumulative commit on the next flush
    // (the watch continues; the store's cursor never advances past data it
    // dropped) and a persistent failure streak is fatal; a panicked
    // blocking task drops the store irrecoverably, which breaks the
    // resume-after-restart guarantee, so it is surfaced as fatal.
    macro_rules! flush {
        () => {{
            // Nothing received since the last flush → nothing to do at all.
            // (`raw_batch` can be non-empty with no cursor advance only via the
            // resync path's synthetic deletes, which carry no revision.)
            if !batch.is_empty() || !raw_batch.is_empty() || !batch_high.is_none() {
                if !batch.is_empty() {
                    // INVARIANT: apply() runs and RETURNS before any cursor
                    // advance below. Move the batch out so a panicking apply
                    // can't leave half-consumed state behind.
                    //
                    // `replace` (not `take`) leaves a pre-sized Vec behind so each
                    // batch after the first doesn't re-climb the reallocation
                    // ladder (4→8→…→cap).
                    apply(std::mem::replace(&mut batch, Vec::with_capacity(batch_cap)));
                }
                let advanced = !batch_high.is_none();
                if advanced {
                    applied = batch_high.clone();
                }
                if !raw_batch.is_empty()
                    && let Some(mut st) = store.take()
                {
                    let raw = std::mem::take(&mut raw_batch);
                    // Fold at the post-advance cursor. A synthetic-deletes-only
                    // batch leaves the cursor where it was (the deletes are a
                    // state correction, not log entries), which is safe: an
                    // unchanged — possibly expired — cursor only ever re-runs
                    // this same resync on the next restart.
                    let cur = applied.clone();
                    // Hand the store AND the raw batch back on a clean return:
                    // a *failed* apply (Ok(Err)) RE-QUEUES the batch so the
                    // next flush commits it cumulatively — the store's cursor
                    // and contents always advance together. Dropping the
                    // failed batch instead lets the NEXT successful flush
                    // advance the cursor over a hole that survives every
                    // restart (reproduced by
                    // `transient_store_failure_never_leaves_a_cursor_gap`).
                    // Only a *panicked* task (Err) loses the store: fatal.
                    match tokio::task::spawn_blocking(move || {
                        let res = st.apply(&raw, &cur);
                        (st, raw, res)
                    })
                    .await
                    {
                        Ok((st, _raw, Ok(()))) => {
                            store = Some(st);
                            store_fail_streak = 0;
                        }
                        Ok((st, raw, Err(e))) => {
                            store_fail_streak += 1;
                            if store_fail_streak >= MAX_STORE_APPLY_FAILURES {
                                // A persistently failing store would otherwise
                                // grow the re-queued batch without bound while
                                // the fold silently stales. Fail-stop: the
                                // restart refolds the tail from the store's
                                // last good cursor.
                                warn!(error = %e, streak = store_fail_streak,
                                    "snapshot store apply failing persistently; aborting watch");
                                handle.abort();
                                return Err(KvError::WatchError(format!(
                                    "snapshot store apply failed {store_fail_streak} consecutive times: {e}"
                                )));
                            }
                            warn!(error = %e, streak = store_fail_streak,
                                "snapshot store apply failed; batch re-queued for the next flush");
                            store = Some(st);
                            // Prepend: the failed range precedes anything
                            // received since (stream order is preserved for
                            // the eventual cumulative commit).
                            let newer = std::mem::replace(&mut raw_batch, raw);
                            raw_batch.extend(newer);
                        }
                        Err(e) => {
                            warn!(error = %e, "snapshot store task panicked; aborting watch");
                            handle.abort();
                            return Err(KvError::WatchError(format!(
                                "snapshot store task panicked: {e}"
                            )));
                        }
                    }
                }
                if advanced {
                    on_applied(applied.clone());
                    batch_high = WatchCursor::none();
                }
            }
            batch_deadline = None;
        }};
    }

    // A single timer future, reset in place each time a batch opens. The old
    // `tokio::time::sleep(timeout)` lived inside the select arm, so it was
    // re-created on every loop iteration — one Arc-backed timer-wheel entry
    // allocated, registered, and immediately dropped per received update.
    // Pinning one future and `reset`-ing it reuses that single allocation; the
    // `if batch_deadline.is_some()` guard keeps it from firing while idle, so
    // its initial already-elapsed deadline is never observed.
    let sleep = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            biased;

            // Shutdown wins: flush whatever is batched (so the cursor reflects
            // it), abandon any updates still in flight on the channel — they
            // weren't applied, the cursor doesn't claim them, and they'll be
            // re-delivered on the next resume — and return the applied cursor.
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    flush!();
                    handle.abort();
                    // Observe the task's terminal state. An abort surfaces as a
                    // cancelled JoinError, which we ignore; a genuine panic that
                    // raced ahead of the abort is logged rather than silently lost.
                    if let Err(join) = handle.await
                        && !join.is_cancelled()
                    {
                        warn!(error = %join, "watch task panicked at shutdown");
                    }
                    return Ok(applied);
                }
            }

            // Batch window elapsed.
            () = &mut sleep, if batch_deadline.is_some() => {
                flush!();
            }

            // Cursor-expired resync. Placed before `rx.recv()` (biased) so the
            // synthetic deletes are folded before any update the fallback watch
            // delivers — though the ack protocol already guarantees the fallback
            // hasn't started while this arm runs. Diff the fold's in-scope keys
            // against the bucket's live listing; anything the fold holds that
            // the bucket no longer does vanished during the gap (its delete
            // marker evicted with the cursor), so synthesize the delete the
            // re-list can't deliver.
            req = async { resyncs.as_mut().expect("arm guarded by is_some").recv().await },
                if resyncs.is_some() => {
                match req {
                    Some(ResyncRequest { live_keys, ack }) => {
                        // Flush first so the diff runs against a fold that
                        // reflects everything received so far.
                        flush!();
                        let live: std::collections::HashSet<&str> =
                            live_keys.iter().map(String::as_str).collect();
                        let mut stale: Vec<String> = Vec::new();
                        if let Some(st) = &store {
                            for prefix in &scope_prefixes {
                                // Stream the fold's keys rather than `range()`,
                                // which buffers every in-scope entry — values
                                // included — into one Vec. On an on-disk backend
                                // holding a fold larger than RAM (the case those
                                // backends exist for), an All-scope resync would
                                // materialize the entire fold on the repair
                                // path. Only the keys matter for the diff.
                                if let Err(e) = st.for_each_in_range(prefix, |entry| {
                                    if !live.contains(entry.key.as_str()) {
                                        stale.push(entry.key);
                                    }
                                    Ok(())
                                }) {
                                    // FATAL, not a degrade: an incomplete
                                    // diff silently leaves deleted keys in
                                    // the fold forever (tests/model.rs
                                    // proves the divergence reachable
                                    // under degrade semantics). Fail the
                                    // watch; the restart re-runs the
                                    // resume → expiry → resync from
                                    // scratch.
                                    warn!(error = %e, prefix = %prefix,
                                        "resync fold scan failed; aborting watch rather than diverging");
                                    handle.abort();
                                    return Err(KvError::WatchError(format!(
                                        "cursor-expired resync failed listing fold prefix {prefix:?}: {e}"
                                    )));
                                }
                            }
                        }
                        // Overlapping prefixes can list a key twice.
                        stale.sort_unstable();
                        stale.dedup();
                        if !stale.is_empty() {
                            warn!(stale = stale.len(), "cursor-expired resync: deleting keys that vanished during the gap");
                        }
                        for key in stale {
                            // Synthetic: carries no revision (unknown version)
                            // and so never advances the cursor.
                            let u = KvUpdate::Delete {
                                key,
                                version: crate::kv::VersionToken::unknown(),
                            };
                            if store.is_some() {
                                raw_batch.push(u.clone());
                            }
                            if let Some(parsed) = parse(&u) {
                                batch.push(parsed);
                            }
                        }
                        flush!();
                        // Ack AFTER the deletes are applied: the watch task is
                        // holding the fallback watch until it hears back, which
                        // is what orders deletes before the re-list
                        // (tests/model_resync_order.rs proves the barrier
                        // load-bearing). If the flush's STORE apply failed
                        // transiently, the deletes sit re-queued at the FRONT
                        // of the raw batch — still strictly before any
                        // re-list put in the eventual cumulative commit, and
                        // the domain apply saw them before this ack either
                        // way.
                        let _ = ack.send(());
                    }
                    None => resyncs = None,
                }
            }

            // Export request. Placed after shutdown/window (they stay prompt)
            // and before `rx.recv()` so a firehose of updates cannot starve an
            // export indefinitely. The pending batch is flushed first, so the
            // exported cursor is exactly the applied cursor — except when
            // that flush's store apply transiently failed (batch re-queued):
            // the export then captures the store's OWN lagging cursor, which
            // is still self-consistent with its contents (cursor authority,
            // tests/model_applied.rs); the artifact never includes unfolded
            // data, and a bootstrap from it simply replays the short gap.
            // The export itself runs on a blocking task with the store moved
            // in and taken back — the same offload the flush path uses.
            req = async { exports.as_mut().expect("arm guarded by is_some").recv().await },
                if exports.is_some() => {
                match req {
                    Some(ExportRequest { dest_dir, reply }) => {
                        flush!();
                        match store.take() {
                            Some(mut st) => {
                                match tokio::task::spawn_blocking(move || {
                                    let res = st.export_to(&dest_dir);
                                    (st, res)
                                })
                                .await
                                {
                                    // Hand the store back on any clean return; an
                                    // export failure goes to the requester only —
                                    // the watch keeps running (the snapshot is a
                                    // cache). A panicked task lost the store,
                                    // which breaks the resume guarantee: fatal,
                                    // same as the flush path's apply panic.
                                    Ok((st, res)) => {
                                        store = Some(st);
                                        let _ = reply.send(res);
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "snapshot export task panicked; aborting watch");
                                        handle.abort();
                                        return Err(KvError::WatchError(format!(
                                            "snapshot export task panicked: {e}"
                                        )));
                                    }
                                }
                            }
                            None => {
                                let _ = reply.send(Err(SnapshotError::Backend(
                                    "watch_applied runs without a snapshot store; nothing to export"
                                        .into(),
                                )));
                            }
                        }
                    }
                    // Sender dropped: disarm the arm for the rest of the run.
                    None => exports = None,
                }
            }

            update = rx.recv() => {
                match update {
                    Some(u) => {
                        // Cursor authority: every received update bumps the
                        // pending high-water, regardless of whether `parse`
                        // keeps it — but only when it carries a real position.
                        // An unknown version (e.g. an unparseable ACK subject
                        // on the hand-built multi-prefix consumer path) must
                        // neither mint a fake cursor nor clobber the real high
                        // from earlier in the batch; skipping it under-advances
                        // at worst, and re-delivery on resume is idempotent.
                        if !u.version().is_unknown() {
                            batch_high = WatchCursor::from_version(u.version().clone());
                        }

                        // Buffer the raw update for the durable store fold (which
                        // commits the whole batch + cursor atomically on flush).
                        // Done before `parse` consumes `u` by reference, and only
                        // when a store is present so the no-persistence path keeps
                        // its zero-copy cost.
                        if store.is_some() {
                            raw_batch.push(u.clone());
                        }

                        if let Some(parsed) = parse(&u) {
                            batch.push(parsed);
                        }

                        // Arm the window on the first received update of a batch
                        // — even a parse-rejected one, so the cursor advances
                        // within `window` even through a run of irrelevant keys.
                        // Reset the pinned timer to the new deadline rather than
                        // allocating a fresh `Sleep`.
                        if batch_deadline.is_none() {
                            let deadline = tokio::time::Instant::now() + config.window;
                            sleep.as_mut().reset(deadline);
                            batch_deadline = Some(deadline);
                        }

                        // Flush on a full parsed batch, or — when persisting — a
                        // full raw batch, so a window packed with parse-rejected
                        // updates can't grow `raw_batch` without bound before the
                        // window elapses.
                        if batch.len() >= config.max || raw_batch.len() >= config.max {
                            flush!();
                        }
                    }
                    None => {
                        // Stream closed. Flush the remainder, then surface the
                        // watch task's terminal result: a clean end returns the
                        // applied cursor, an error propagates.
                        flush!();
                        return match handle.await {
                            Ok(Ok(())) => Ok(applied),
                            Ok(Err(e)) => Err(e),
                            Err(join) => Err(KvError::WatchError(format!(
                                "watch task panicked: {join}"
                            ))),
                        };
                    }
                }
            }
        }
    }
}

/// Run the underlying watch for `scope`, resuming from `resume` when it carries
/// a position, with the [`KvError::CursorExpired`] → resync + full-watch
/// fallback.
async fn run_watch(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    resume: Option<WatchCursor>,
    resync: Option<ResyncHandle>,
    tx: mpsc::Sender<KvUpdate>,
) -> Result<(), KvError> {
    // Resume only when the cursor carries a real position; an absent or `none()`
    // cursor falls through to a full watch. Binding `cursor` here makes "we have a
    // resume position" structural — there is no separate bool whose truth a later
    // edit could let drift from the `Some`.
    let resume_cursor = resume.filter(|c| !c.is_none());

    match scope {
        WatchScope::All => {
            if let Some(cursor) = resume_cursor {
                match watcher.watch_all_from(&cursor, tx.clone()).await {
                    Err(KvError::CursorExpired) => {
                        warn!(
                            "watch cursor expired; resyncing, then falling back to full watch_all"
                        );
                        resync_stale_keys(scope, &resync).await?;
                        watcher.watch_all(tx).await
                    }
                    other => other,
                }
            } else {
                watcher.watch_all(tx).await
            }
        }
        WatchScope::Prefix(prefix) => {
            if let Some(cursor) = resume_cursor {
                match watcher.watch_prefix_from(prefix, &cursor, tx.clone()).await {
                    Err(KvError::CursorExpired) => {
                        warn!(
                            "watch cursor expired; resyncing, then falling back to full watch_prefix"
                        );
                        resync_stale_keys(scope, &resync).await?;
                        watcher.watch_prefix(prefix, tx).await
                    }
                    other => other,
                }
            } else {
                watcher.watch_prefix(prefix, tx).await
            }
        }
        WatchScope::Prefixes(prefixes) => {
            let refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
            if let Some(cursor) = resume_cursor {
                match watcher
                    .watch_prefixes_from(&refs, &cursor, tx.clone())
                    .await
                {
                    Err(KvError::CursorExpired) => {
                        warn!(
                            "watch cursor expired; resyncing, then falling back to full watch_prefixes"
                        );
                        resync_stale_keys(scope, &resync).await?;
                        watcher.watch_prefixes(&refs, tx).await
                    }
                    other => other,
                }
            } else {
                watcher.watch_prefixes(&refs, tx).await
            }
        }
    }
}

/// Cursor-expired stale-key resync, run BEFORE the fallback watch is
/// established: list the scope's live keys, hand them to the main loop (which
/// diffs them against the fold and applies synthetic deletes), and wait for the
/// ack. That ordering — deletes applied, then fallback watch armed — is what
/// makes a delete-then-recreate during the gap converge: the synthetic delete
/// always lands before the re-list put.
///
/// With no reader/store wired (`resync` is `None`) the caller explicitly opted
/// out: warn and fall back re-list-only (keys deleted during the gap stay in
/// the fold — `tests/model.rs` pins this divergence as reachable).
///
/// A FAILED listing, by contrast, is **fatal** — it fails the watch rather
/// than degrading. The resync is load-bearing for the "stale, never corrupt"
/// convergence guarantee: a silently degraded resync leaves the fold holding
/// keys the bucket deleted, with one warn line as the only witness
/// (`tests/model.rs` proves this divergence reachable under degrade
/// semantics). Failing the watch turns the violated guarantee into a visible
/// error; the caller's restart re-resumes, hits `CursorExpired` again, and
/// retries the resync from scratch.
async fn resync_stale_keys(
    scope: &WatchScope,
    resync: &Option<ResyncHandle>,
) -> Result<(), KvError> {
    let Some((reader, resync_tx)) = resync else {
        warn!(
            "no reader wired for cursor-expired resync; keys deleted during the gap may persist in the fold"
        );
        return Ok(());
    };
    let mut live_keys = Vec::new();
    for prefix in scope.prefixes() {
        match reader.keys(&prefix).await {
            Ok(keys) => live_keys.extend(keys),
            Err(e) => {
                return Err(KvError::WatchError(format!(
                    "cursor-expired resync failed listing live keys under {prefix:?}: {e}; \
                     failing the watch rather than silently keeping stale keys"
                )));
            }
        }
    }
    let (ack_tx, ack_rx) = oneshot::channel();
    if resync_tx
        .send(ResyncRequest {
            live_keys,
            ack: ack_tx,
        })
        .await
        .is_ok()
    {
        // A dropped ack (main loop shutting down) just means the fallback watch
        // is about to die with it; nothing to recover.
        let _ = ack_rx.await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{KvEntry, VersionToken};
    use crate::snapshot::AppendLogSnapshot;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::mpsc::Sender;

    fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
        KvUpdate::Put(KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            version: VersionToken::from_u64(rev),
        })
    }

    /// A scripted watcher. Delivers a pre-set list of updates through the
    /// channel, then either holds the channel open (so window/max/shutdown
    /// flushes can be exercised without the stream ending) or returns cleanly
    /// (so channel-close flushing can be exercised).
    struct MockWatcher {
        full: Mutex<Option<Vec<KvUpdate>>>,
        from: Mutex<Option<Vec<KvUpdate>>>,
        from_expires: bool,
        hold: bool,
    }

    impl MockWatcher {
        fn new(updates: Vec<KvUpdate>, hold: bool) -> Self {
            Self {
                full: Mutex::new(Some(updates)),
                from: Mutex::new(None),
                from_expires: false,
                hold,
            }
        }

        async fn deliver(&self, which: &Mutex<Option<Vec<KvUpdate>>>, tx: Sender<KvUpdate>) {
            let updates = which.lock().unwrap().take().unwrap_or_default();
            for u in updates {
                if tx.send(u).await.is_err() {
                    return;
                }
            }
            if self.hold {
                // Keep `tx` alive (channel open) until this task is aborted.
                std::future::pending::<()>().await;
            }
        }
    }

    #[async_trait]
    impl KvWatcher for MockWatcher {
        async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.deliver(&self.full, tx).await;
            Ok(())
        }

        async fn watch_prefix(&self, _prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.deliver(&self.full, tx).await;
            Ok(())
        }

        async fn watch_prefixes(
            &self,
            _prefixes: &[&str],
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            // This mock scripts the applied-watch resumption tests, not prefix
            // filtering; it delivers the same `full` script as `watch_prefix`.
            // The real multi-filter scoping is proved in the NATS integration test.
            self.deliver(&self.full, tx).await;
            Ok(())
        }

        async fn watch_all_from(
            &self,
            _cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            if self.from_expires {
                return Err(KvError::CursorExpired);
            }
            self.deliver(&self.from, tx).await;
            Ok(())
        }

        // Mirror watch_all_from so the prefix resume / expiry arms of run_watch
        // are exercised against the same `from` script. Without this the trait's
        // default impl would delegate to watch_prefix and silently deliver the
        // full set instead of the delta.
        async fn watch_prefix_from(
            &self,
            _prefix: &str,
            _cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            if self.from_expires {
                return Err(KvError::CursorExpired);
            }
            self.deliver(&self.from, tx).await;
            Ok(())
        }

        // Same mirroring for the multi-prefix resume arm.
        async fn watch_prefixes_from(
            &self,
            _prefixes: &[&str],
            _cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            if self.from_expires {
                return Err(KvError::CursorExpired);
            }
            self.deliver(&self.from, tx).await;
            Ok(())
        }
    }

    /// A reader whose `keys()` serves a scripted live listing — the only call
    /// the cursor-expired resync makes. Filters by prefix like a real backend
    /// so prefix-scoped resyncs are exercised faithfully.
    struct MockReader {
        live: Vec<String>,
    }

    #[async_trait]
    impl KvReader for MockReader {
        async fn get(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }

        async fn entry(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }

        async fn keys(&self, prefix: &str) -> Result<Vec<String>, KvError> {
            Ok(self
                .live
                .iter()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect())
        }

        async fn scan(&self, _prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }
    }

    /// A watcher whose entry points all fail. Used to prove the watch task's
    /// terminal error is surfaced out of `watch_applied` rather than swallowed
    /// as a clean `Ok(applied)` when the channel closes.
    struct ErrorWatcher;

    #[async_trait]
    impl KvWatcher for ErrorWatcher {
        async fn watch_all(&self, _tx: Sender<KvUpdate>) -> Result<(), KvError> {
            Err(KvError::WatchError("injected watch failure".into()))
        }

        async fn watch_prefix(&self, _prefix: &str, _tx: Sender<KvUpdate>) -> Result<(), KvError> {
            Err(KvError::WatchError("injected watch failure".into()))
        }

        async fn watch_prefixes(
            &self,
            _prefixes: &[&str],
            _tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            Err(KvError::WatchError("injected watch failure".into()))
        }
    }

    // A no-op parse that keeps every Put as the value bytes; drops deletes.
    fn parse_put(u: &KvUpdate) -> Option<Vec<u8>> {
        match u {
            KvUpdate::Put(e) => Some(e.value.clone()),
            _ => None,
        }
    }

    /// The stream closes (hold = false) with a pending batch; the remainder is
    /// flushed before returning, the returned cursor is the last revision, and
    /// `on_applied` ran exactly once after `apply`.
    #[tokio::test]
    async fn flush_on_channel_close() {
        let updates = vec![put("a", b"1", 1), put("b", b"2", 2), put("c", b"3", 3)];
        let watcher = Arc::new(MockWatcher::new(updates, false));

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let on_applied_cursors = Arc::new(Mutex::new(Vec::<u64>::new()));

        let ab = Arc::clone(&applied_batches);
        let oc = Arc::clone(&on_applied_cursors);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch| ab.lock().unwrap().push(batch),
            move |c| oc.lock().unwrap().push(c.as_u64().unwrap()),
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(3));
        let batches = applied_batches.lock().unwrap();
        let flat: Vec<Vec<u8>> = batches.iter().flatten().cloned().collect();
        assert_eq!(flat, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
        assert_eq!(*on_applied_cursors.lock().unwrap().last().unwrap(), 3);
    }

    /// Fewer than `max` updates, then the channel idles: the window timer must
    /// flush them and advance the cursor.
    #[tokio::test(start_paused = true)]
    async fn flush_on_window() {
        let updates = vec![put("a", b"1", 1), put("b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open

        let applied = Arc::new(AtomicU64::new(0));
        let count = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);
        let c = Arc::clone(&count);
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| {
                c.fetch_add(batch.len() as u64, Ordering::SeqCst);
            },
            move |cur| a.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        ));

        // Let the window (10ms) elapse under virtual time.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "window should have flushed"
        );
        assert_eq!(applied.load(Ordering::SeqCst), 2);

        sd_tx.send(true).unwrap();
        let cursor = task.await.unwrap().unwrap();
        assert_eq!(cursor.as_u64(), Some(2));
    }

    /// Exactly `max` updates fills a batch and flushes immediately — before the
    /// window would have elapsed.
    #[tokio::test(start_paused = true)]
    async fn flush_on_max() {
        let max = 4;
        let updates: Vec<_> = (1..=max as u64)
            .map(|i| put(&format!("k{i}"), b"v", i))
            .collect();
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open

        let flushes = Arc::new(Mutex::new(Vec::<usize>::new()));
        let f = Arc::clone(&flushes);
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig {
                window: Duration::from_secs(3600), // effectively never
                max,
                ..BatchConfig::default()
            },
            parse_put,
            move |batch: Vec<Vec<u8>>| f.lock().unwrap().push(batch.len()),
            move |_| {},
            sd_rx,
        ));

        // Yield enough for the mock to push all `max` updates; the window is an
        // hour, so any flush is purely the max trigger.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            *flushes.lock().unwrap(),
            vec![max],
            "a full batch should flush on max, not wait for the window"
        );

        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    /// A pending batch plus a shutdown signal: the batch is flushed and the
    /// applied cursor returned.
    #[tokio::test(start_paused = true)]
    async fn flush_on_shutdown() {
        let updates = vec![put("a", b"1", 1), put("b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open

        let applied = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig {
                window: Duration::from_secs(3600), // window won't fire
                max: 100,
                ..BatchConfig::default()
            },
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |cur| a.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        ));

        // Give the mock time to deliver both updates into the pending batch.
        tokio::time::sleep(Duration::from_millis(1)).await;
        sd_tx.send(true).unwrap();

        let cursor = task.await.unwrap().unwrap();
        assert_eq!(
            cursor.as_u64(),
            Some(2),
            "shutdown flushes the pending batch"
        );
        assert_eq!(applied.load(Ordering::SeqCst), 2);
    }

    /// The cursor must not advance until `apply` has returned. We prove it by
    /// having `apply` read the cursor that `on_applied` last published: when the
    /// second batch is applied, the visible cursor must still be the *first*
    /// batch's — never the second's, which only becomes visible after this
    /// `apply` returns.
    #[tokio::test(start_paused = true)]
    async fn cursor_advances_only_after_apply() {
        // Two batches of `max` updates each.
        let max = 2usize;
        let updates: Vec<_> = (1..=4u64).map(|i| put(&format!("k{i}"), b"v", i)).collect();
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open

        // Cursor as last published by on_applied; starts at 0 (nothing applied).
        let published = Arc::new(AtomicU64::new(0));
        // What `apply` observed as the published cursor at the moment it ran.
        let seen_at_apply = Arc::new(Mutex::new(Vec::<u64>::new()));

        let pub_for_apply = Arc::clone(&published);
        let seen = Arc::clone(&seen_at_apply);
        let pub_for_on = Arc::clone(&published);
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig {
                window: Duration::from_secs(3600),
                max,
                ..BatchConfig::default()
            },
            parse_put,
            move |_batch: Vec<Vec<u8>>| {
                // The cursor visible here is whatever the PREVIOUS flush
                // published — never this batch's, because we haven't returned.
                seen.lock()
                    .unwrap()
                    .push(pub_for_apply.load(Ordering::SeqCst));
            },
            move |cur| pub_for_on.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        ));

        tokio::time::sleep(Duration::from_millis(1)).await;
        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();

        // First apply saw 0 (nothing applied yet); second apply saw 2 (first
        // batch's cursor), NOT 4. The cursor only reached 4 after the second
        // apply returned.
        assert_eq!(*seen_at_apply.lock().unwrap(), vec![0, 2]);
        assert_eq!(published.load(Ordering::SeqCst), 4);
    }

    /// Updates whose `parse` returns `None` (corrupt / irrelevant) carry no
    /// domain work, but they were still received — so the cursor must advance
    /// over them.
    #[tokio::test]
    async fn corrupt_parse_entries_advance_cursor() {
        let updates = vec![put("a", b"1", 5), put("b", b"2", 6), put("c", b"3", 7)];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after

        let apply_calls = Arc::new(AtomicU64::new(0));
        let on_applied_max = Arc::new(AtomicU64::new(0));
        let ac = Arc::clone(&apply_calls);
        let om = Arc::clone(&on_applied_max);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            // Reject everything — simulates corrupt/irrelevant entries.
            |_u: &KvUpdate| -> Option<Vec<u8>> { None },
            move |batch: Vec<Vec<u8>>| {
                ac.fetch_add(1, Ordering::SeqCst);
                assert!(batch.is_empty());
            },
            move |cur| om.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(7), "cursor covers rejected updates");
        assert_eq!(
            apply_calls.load(Ordering::SeqCst),
            0,
            "an all-rejected batch applies nothing"
        );
        assert_eq!(on_applied_max.load(Ordering::SeqCst), 7);
    }

    /// An update carrying the UNKNOWN version (an unparseable ACK subject on
    /// the hand-built multi-prefix consumer path) must neither mint a cursor
    /// position nor clobber the real high-water from earlier in the batch.
    /// Pre-guard behavior: `kv_message_to_update` fabricated revision 0 for
    /// such updates and the unconditional `batch_high = ...` adopted it,
    /// regressing the persisted cursor to 0. The update itself is still
    /// applied — only the cursor ignores it.
    #[tokio::test]
    async fn unknown_version_update_does_not_move_or_clobber_cursor() {
        let unknown_put = KvUpdate::Put(KvEntry {
            key: "u".to_string(),
            value: b"x".to_vec(),
            version: VersionToken::unknown(),
        });
        let updates = vec![put("a", b"1", 5), unknown_put];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(
            cursor.as_u64(),
            Some(5),
            "the unknown-version update must not clobber the real batch high"
        );
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"x".to_vec()],
            "the unknown-version update is still applied"
        );
    }

    /// A resume whose cursor has expired falls back to the full watch and still
    /// applies the delivered updates.
    #[tokio::test]
    async fn cursor_expired_falls_back_to_full_watch() {
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![put("a", b"1", 10), put("b", b"2", 11)])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let watcher = Arc::new(mock);

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            Some(WatchCursor::from_u64(5)), // resume position that "expired"
            None,                           // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(11));
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"2".to_vec()],
            "fallback full watch's updates were applied"
        );
    }

    /// Cursor-expired resync: with a reader + store wired, a key the fold holds
    /// that the live listing no longer does gets a synthetic delete — applied
    /// strictly BEFORE the fallback re-list — and the persisted fold converges
    /// to the live state. The synthetic delete (unknown version) must not move
    /// the cursor; the re-list put must.
    #[tokio::test]
    async fn cursor_expired_resync_deletes_stale_keys() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("resync.snap");
        let (_r, mut store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        // The fold from the previous run: node.a and node.b at cursor 2.
        store
            .apply(
                &[put("node.a", b"1", 1), put("node.b", b"2", 2)],
                &WatchCursor::from_u64(2),
            )
            .unwrap();

        // During the gap node.b was deleted (marker since evicted) and node.a
        // updated; the resume cursor (2) has expired. The fallback re-list
        // therefore carries only the surviving key.
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![put("node.a", b"1b", 10)])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let reader = MockReader {
            live: vec!["node.a".to_string()],
        };

        // Record everything `parse` sees, in order, deletes included.
        let seen = Arc::new(Mutex::new(Vec::<(String, bool)>::new()));
        let s = Arc::clone(&seen);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            Arc::new(mock),
            WatchScope::All,
            Some(WatchCursor::from_u64(2)),
            Some(Arc::new(reader) as Arc<dyn KvReader>),
            Some(store),
            None,
            BatchConfig::default(),
            move |u: &KvUpdate| {
                s.lock()
                    .unwrap()
                    .push((u.key().to_string(), matches!(u, KvUpdate::Delete { .. })));
                Some(())
            },
            |_batch: Vec<()>| {},
            |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        // The re-list put advanced the cursor; the synthetic delete did not.
        assert_eq!(cursor.as_u64(), Some(10));
        // The synthetic delete strictly precedes the re-list put.
        assert_eq!(
            *seen.lock().unwrap(),
            vec![("node.b".to_string(), true), ("node.a".to_string(), false)],
            "synthetic delete must be applied before the fallback re-list"
        );

        // The persisted fold converged: stale key gone, live key updated.
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.cursor.as_u64(), Some(10));
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries["node.a"].value, b"1b");
    }

    /// A prefix-scoped resync diffs only in-scope keys: an out-of-scope key the
    /// fold holds survives, the in-scope stale key is deleted, and a flush
    /// containing only synthetic deletes leaves the cursor untouched.
    #[tokio::test]
    async fn cursor_expired_resync_respects_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("resync-scope.snap");
        let (_r, mut store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        store
            .apply(
                &[put("node.b", b"2", 1), put("other.z", b"9", 2)],
                &WatchCursor::from_u64(2),
            )
            .unwrap();

        // Expired resume; the bucket no longer has ANY node.* keys; the
        // fallback re-list is empty.
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let reader = MockReader { live: vec![] };
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            Arc::new(mock),
            WatchScope::Prefix("node.".to_string()),
            Some(WatchCursor::from_u64(2)),
            Some(Arc::new(reader) as Arc<dyn KvReader>),
            Some(store),
            None,
            BatchConfig::default(),
            |_u: &KvUpdate| Some(()),
            |_batch: Vec<()>| {},
            |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        // Deletes-only flush: cursor stays at the resume position.
        assert_eq!(cursor.as_u64(), Some(2));

        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.cursor.as_u64(), Some(2));
        assert!(
            !snap.entries.contains_key("node.b"),
            "in-scope stale key must be resync-deleted"
        );
        assert_eq!(
            snap.entries["other.z"].value, b"9",
            "out-of-scope key must survive a prefix-scoped resync"
        );
    }

    /// `WatchScope::Prefixes` dispatches to `watch_prefixes` (no resume) and to
    /// `watch_prefixes_from` with the expiry → full-watch fallback (resume).
    #[tokio::test]
    async fn prefixes_scope_dispatches_full_watch() {
        let updates = vec![put("a.x", b"1", 1), put("b.y", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, false));
        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::Prefixes(vec!["a.".to_string(), "b.".to_string()]),
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(2));
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"2".to_vec()]
        );
    }

    /// `WatchScope::Prefixes` resume whose cursor has expired falls back to the
    /// full multi-prefix watch and applies its updates.
    #[tokio::test]
    async fn prefixes_scope_expired_resume_falls_back() {
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![put("a.x", b"1", 7)])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            Arc::new(mock),
            WatchScope::Prefixes(vec!["a.".to_string()]),
            Some(WatchCursor::from_u64(3)),
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(7));
        assert_eq!(*applied_batches.lock().unwrap(), vec![b"1".to_vec()]);
    }

    /// End-to-end with a real snapshot file: after the run, the persisted
    /// snapshot's cursor equals the applied cursor and its entries match the
    /// applied state — proving the checkpoint is written at the post-apply
    /// cursor, never ahead of it.
    #[tokio::test]
    async fn snapshot_checkpoint_matches_applied_cursor() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("applied.snap");
        let (_resume, store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();

        let updates = vec![put("node.a", b"1", 1), put("node.b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            Some(store),
            None,
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(2));

        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(
            snap.cursor.as_u64(),
            cursor.as_u64(),
            "snapshot checkpoint cursor must equal the applied cursor"
        );
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.entries["node.a"].value, b"1");
        assert_eq!(snap.entries["node.b"].value, b"2");
    }

    /// Happy-path resume: a non-expired cursor takes the `*_from` path and the
    /// delta (the `from` script, NOT the full set) is applied. Proves the
    /// resume branch delivers only post-cursor updates and advances to their
    /// max revision.
    #[tokio::test]
    async fn resume_from_cursor_delivers_only_delta() {
        let mock = MockWatcher {
            // `full` would be delivered only if the resume path were (wrongly)
            // bypassed; a non-empty distinguishing value makes that visible.
            full: Mutex::new(Some(vec![put("full.x", b"FULL", 1)])),
            from: Mutex::new(Some(vec![put("node.c", b"3", 10), put("node.d", b"4", 11)])),
            from_expires: false,
            hold: false,
        };
        let watcher = Arc::new(mock);

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            Some(WatchCursor::from_u64(9)), // resume past rev 9 — not expired
            None,                           // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(
            cursor.as_u64(),
            Some(11),
            "cursor advances to the delta max"
        );
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"3".to_vec(), b"4".to_vec()],
            "only the post-cursor delta is applied, never the full set"
        );
    }

    /// `WatchScope::Prefix` with no resume dispatches to `watch_prefix` and
    /// applies the delivered updates. Every other test uses `WatchScope::All`;
    /// this covers the prefix dispatch arm.
    #[tokio::test]
    async fn prefix_scope_applies_delivered_updates() {
        let updates = vec![put("node.a", b"1", 1), put("node.b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::Prefix("node.".to_string()),
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(2));
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"2".to_vec()]
        );
    }

    /// `WatchScope::Prefix` happy-path resume: a non-expired cursor takes the
    /// `watch_prefix_from` path and only the delta is applied — the prefix
    /// twin of `resume_from_cursor_delivers_only_delta`.
    #[tokio::test]
    async fn prefix_resume_from_cursor_delivers_only_delta() {
        let mock = MockWatcher {
            // `full` would be delivered only if the resume path were (wrongly)
            // bypassed; a distinguishing value makes that visible.
            full: Mutex::new(Some(vec![put("node.x", b"FULL", 1)])),
            from: Mutex::new(Some(vec![put("node.c", b"3", 10), put("node.d", b"4", 11)])),
            from_expires: false,
            hold: false,
        };
        let watcher = Arc::new(mock);

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::Prefix("node.".to_string()),
            Some(WatchCursor::from_u64(9)), // resume past rev 9 — not expired
            None,                           // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(
            cursor.as_u64(),
            Some(11),
            "cursor advances to the delta max"
        );
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"3".to_vec(), b"4".to_vec()],
            "only the post-cursor delta is applied via watch_prefix_from"
        );
    }

    /// `WatchScope::Prefix` resume whose cursor has expired falls back to the
    /// full `watch_prefix` and still applies the delivered updates — the prefix
    /// twin of `cursor_expired_falls_back_to_full_watch`.
    #[tokio::test]
    async fn prefix_cursor_expired_falls_back_to_full_prefix_watch() {
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![put("node.a", b"1", 10), put("node.b", b"2", 11)])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let watcher = Arc::new(mock);

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let ab = Arc::clone(&applied_batches);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::Prefix("node.".to_string()),
            Some(WatchCursor::from_u64(5)), // resume position that "expired"
            None,                           // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(11));
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"2".to_vec()],
            "prefix fallback full watch's updates were applied"
        );
    }

    /// The watch task's terminal error must propagate out of `watch_applied`
    /// rather than being swallowed as `Ok(applied)` when the channel closes.
    #[tokio::test]
    async fn watch_task_error_propagates() {
        let watcher = Arc::new(ErrorWatcher);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let result = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        )
        .await;

        match result {
            Err(KvError::WatchError(msg)) => {
                assert!(msg.contains("injected"), "error carries the cause: {msg}");
            }
            other => panic!("expected WatchError, got {other:?}"),
        }
    }

    /// A batch where `parse` accepts some updates and rejects others: the cursor
    /// must still advance to the highest *received* revision (covering the
    /// rejected entry in the middle), while `apply` sees only the accepted ones.
    #[tokio::test]
    async fn mixed_parse_advances_cursor_over_rejected_entries() {
        let updates = vec![
            put("keep.a", b"1", 5),
            put("skip.b", b"2", 6), // rejected by parse
            put("keep.c", b"3", 7),
        ];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after

        let applied_batches = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let on_applied_max = Arc::new(AtomicU64::new(0));
        let ab = Arc::clone(&applied_batches);
        let om = Arc::clone(&on_applied_max);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            // Keep only keys under "keep."; reject everything else.
            |u: &KvUpdate| -> Option<Vec<u8>> {
                match u {
                    KvUpdate::Put(e) if e.key.starts_with("keep.") => Some(e.value.clone()),
                    _ => None,
                }
            },
            move |batch: Vec<Vec<u8>>| ab.lock().unwrap().extend(batch),
            move |cur| om.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(
            cursor.as_u64(),
            Some(7),
            "cursor covers the rejected middle entry (rev 6)"
        );
        assert_eq!(
            *applied_batches.lock().unwrap(),
            vec![b"1".to_vec(), b"3".to_vec()],
            "apply sees only the accepted entries"
        );
        assert_eq!(on_applied_max.load(Ordering::SeqCst), 7);
    }

    /// Shutdown before any update arrives: nothing was received, so the cursor
    /// stays at the resume position (here `none()`), `apply` never runs, and
    /// `on_applied` never fires.
    #[tokio::test(start_paused = true)]
    async fn shutdown_with_no_pending_batch() {
        let watcher = Arc::new(MockWatcher::new(vec![], true)); // deliver nothing, hold open

        let apply_calls = Arc::new(AtomicU64::new(0));
        let on_applied_calls = Arc::new(AtomicU64::new(0));
        let ac = Arc::clone(&apply_calls);
        let oc = Arc::clone(&on_applied_calls);
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            None,
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {
                ac.fetch_add(1, Ordering::SeqCst);
            },
            move |_| {
                oc.fetch_add(1, Ordering::SeqCst);
            },
            sd_rx,
        ));

        // Let the watcher attach and idle (it has nothing to deliver), then shut down.
        tokio::time::sleep(Duration::from_millis(1)).await;
        sd_tx.send(true).unwrap();

        let cursor = task.await.unwrap().unwrap();
        assert_eq!(
            cursor.as_u64(),
            None,
            "no updates received → cursor unmoved"
        );
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0, "apply never runs");
        assert_eq!(
            on_applied_calls.load(Ordering::SeqCst),
            0,
            "on_applied never fires"
        );
    }

    /// An [`ExportRequest`] flushes the pending batch first, so the artifact's
    /// cursor is exactly the applied cursor — and the artifact is importable
    /// with the batched entries in it.
    #[tokio::test(start_paused = true)]
    async fn export_request_flushes_pending_batch_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let store_path = dir.path().join("fold.snap");
        let artifact = dir.path().join("artifact");
        let (_r, store) = AppendLogSnapshot::open(&store_path, u64::MAX).unwrap();

        let updates = vec![put("a", b"1", 1), put("b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel(1);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            Some(store),
            Some(ex_rx),
            BatchConfig {
                window: Duration::from_secs(3600), // window never fires
                max: 100,
                ..BatchConfig::default()
            },
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        ));

        // Let both updates land in the (unflushed) pending batch, then export.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: artifact.clone(),
                reply: reply_tx,
            })
            .await
            .unwrap();

        let manifest = reply_rx.await.unwrap().expect("export succeeds");
        assert_eq!(
            manifest.cursor.as_u64(),
            Some(2),
            "pending batch flushed before export: artifact cursor is the applied cursor"
        );

        // The artifact is importable and holds both batched entries.
        let (cursor, imported) =
            AppendLogSnapshot::import(&artifact, &dir.path().join("imported.snap"), u64::MAX)
                .unwrap();
        assert_eq!(cursor.as_u64(), Some(2));
        assert_eq!(imported.get("a").unwrap().unwrap().value, b"1");
        assert_eq!(imported.get("b").unwrap().unwrap().value, b"2");

        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    /// An [`ExportRequest`] that arrives with NOTHING pending (the window
    /// already flushed everything) still produces a valid artifact whose
    /// cursor is the applied cursor. The flush-before-export step must be a
    /// clean no-op, not an error or a cursor regression.
    #[tokio::test(start_paused = true)]
    async fn export_with_empty_pending_batch_succeeds() {
        let dir = tempfile::TempDir::new().unwrap();
        let store_path = dir.path().join("fold.snap");
        let artifact = dir.path().join("artifact");
        let (_r, store) = AppendLogSnapshot::open(&store_path, u64::MAX).unwrap();

        let updates = vec![put("a", b"1", 1), put("b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, true)); // hold open
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel(1);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            Some(store),
            Some(ex_rx),
            BatchConfig::default(), // 10 ms window
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        ));

        // Let the window flush both updates, so the export request finds an
        // EMPTY pending batch.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: artifact.clone(),
                reply: reply_tx,
            })
            .await
            .unwrap();
        let manifest = reply_rx
            .await
            .unwrap()
            .expect("export succeeds with nothing pending");
        assert_eq!(
            manifest.cursor.as_u64(),
            Some(2),
            "artifact cursor is the applied cursor, unchanged by the no-op flush"
        );

        // The artifact is importable and holds the already-flushed entries.
        let (cursor, imported) =
            AppendLogSnapshot::import(&artifact, &dir.path().join("imported.snap"), u64::MAX)
                .unwrap();
        assert_eq!(cursor.as_u64(), Some(2));
        assert_eq!(imported.get("a").unwrap().unwrap().value, b"1");
        assert_eq!(imported.get("b").unwrap().unwrap().value, b"2");

        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    /// An export request against a store-less watch replies with an error and
    /// the watch keeps running.
    #[tokio::test(start_paused = true)]
    async fn export_without_store_replies_error() {
        let watcher = Arc::new(MockWatcher::new(vec![put("a", b"1", 1)], true));
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel(1);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            Some(ex_rx),
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        ));

        tokio::time::sleep(Duration::from_millis(1)).await;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: std::env::temp_dir().join("never-created"),
                reply: reply_tx,
            })
            .await
            .unwrap();
        assert!(
            reply_rx.await.unwrap().is_err(),
            "no store → export errors via the reply"
        );

        // The watch is still alive and returns its applied cursor on shutdown.
        sd_tx.send(true).unwrap();
        let cursor = task.await.unwrap().unwrap();
        assert_eq!(cursor.as_u64(), Some(1));
    }

    /// An export failure (unavailable destination) is reported on the reply and
    /// the watch keeps applying later updates.
    #[tokio::test(start_paused = true)]
    async fn export_error_does_not_kill_watch() {
        let dir = tempfile::TempDir::new().unwrap();
        let store_path = dir.path().join("fold.snap");
        let (_r, store) = AppendLogSnapshot::open(&store_path, u64::MAX).unwrap();

        // Occupied destination → export fails.
        let occupied = dir.path().join("occupied");
        std::fs::create_dir(&occupied).unwrap();
        std::fs::write(occupied.join("stray"), b"x").unwrap();

        let watcher = Arc::new(MockWatcher::new(vec![put("a", b"1", 1)], true));
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel(1);

        let applied = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            Some(store),
            Some(ex_rx),
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |cur| a.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        ));

        tokio::time::sleep(Duration::from_millis(1)).await;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: occupied,
                reply: reply_tx,
            })
            .await
            .unwrap();
        match reply_rx.await.unwrap() {
            Err(crate::snapshot::SnapshotError::ArtifactInvalid(_)) => {}
            other => panic!("expected ArtifactInvalid, got {other:?}"),
        }

        // Watch still folds: a clean shutdown returns the applied cursor.
        sd_tx.send(true).unwrap();
        let cursor = task.await.unwrap().unwrap();
        assert_eq!(cursor.as_u64(), Some(1), "watch survived the failed export");
        assert_eq!(applied.load(Ordering::SeqCst), 1);
    }

    /// Dropping the export sender disarms the arm; the loop keeps batching and
    /// flushing normally.
    #[tokio::test(start_paused = true)]
    async fn export_sender_dropped_disarms_channel() {
        let watcher = Arc::new(MockWatcher::new(vec![put("a", b"1", 1)], true));
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel::<ExportRequest>(1);

        let applied = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            None::<AppendLogSnapshot>,
            Some(ex_rx),
            BatchConfig::default(),
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |cur| a.store(cur.as_u64().unwrap(), Ordering::SeqCst),
            sd_rx,
        ));

        drop(ex_tx); // disarm
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            applied.load(Ordering::SeqCst),
            1,
            "loop keeps flushing after the export sender is gone"
        );

        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    /// With a low `compact_threshold`, the flush path's `spawn_blocking`
    /// compaction actually fires (every other snapshot test pins the threshold
    /// at `u64::MAX`, leaving that branch dead). After a compacting run the
    /// snapshot must still load cleanly with the right cursor and entries.
    #[tokio::test]
    async fn snapshot_compaction_fires_and_stays_consistent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("applied.snap");
        // threshold 0 → every checkpoint reports "needs compact", forcing the
        // store's inline-compaction branch on each flush (run off the hot path via
        // spawn_blocking inside watch_applied).
        let (_resume, store) = AppendLogSnapshot::open(&path, 0).unwrap();

        // Re-put the same key across flushes so compaction has duplicates to
        // dedup; small max forces multiple flushes (hence multiple compactions).
        let updates = vec![
            put("node.a", b"1", 1),
            put("node.a", b"2", 2),
            put("node.b", b"3", 3),
            put("node.a", b"4", 4),
        ];
        let watcher = Arc::new(MockWatcher::new(updates, false)); // close after
        let (_sd_tx, sd_rx) = watch::channel(false);

        let cursor = watch_applied(
            watcher,
            WatchScope::All,
            None,
            None, // reader (no resync in this test)
            Some(store),
            None,
            BatchConfig {
                window: Duration::from_secs(3600),
                max: 1, // one update per flush → a compaction per update
                ..BatchConfig::default()
            },
            parse_put,
            move |_batch: Vec<Vec<u8>>| {},
            move |_| {},
            sd_rx,
        )
        .await
        .unwrap();

        assert_eq!(cursor.as_u64(), Some(4));

        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(
            snap.cursor.as_u64(),
            cursor.as_u64(),
            "compacted snapshot's cursor still equals the applied cursor"
        );
        assert_eq!(snap.entries.len(), 2, "duplicates of node.a deduped");
        assert_eq!(
            snap.entries["node.a"].value, b"4",
            "last write per key survives compaction"
        );
        assert_eq!(snap.entries["node.b"].value, b"3");
    }
    /// A SnapshotStore whose FIRST apply fails (transient store error: disk
    /// pressure, lock timeout), then behaves normally — the trigger for the
    /// lost-raw-batch hazard in the flush path.
    struct FailOnceStore {
        inner: AppendLogSnapshot,
        failed: std::sync::atomic::AtomicBool,
    }

    impl crate::snapshot::SnapshotStore for FailOnceStore {
        fn load(
            _path: &std::path::Path,
        ) -> Result<(WatchCursor, Self), crate::snapshot::SnapshotError> {
            unreachable!("test store is constructed directly")
        }
        fn apply(
            &mut self,
            batch: &[KvUpdate],
            cursor: &WatchCursor,
        ) -> Result<(), crate::snapshot::SnapshotError> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                return Err(crate::snapshot::SnapshotError::Backend(
                    "injected transient store failure".into(),
                ));
            }
            self.inner.apply(batch, cursor)
        }
        fn get(&self, key: &str) -> Result<Option<KvEntry>, crate::snapshot::SnapshotError> {
            self.inner.get(key)
        }
        fn range(&self, prefix: &str) -> Result<Vec<KvEntry>, crate::snapshot::SnapshotError> {
            self.inner.range(prefix)
        }
        fn cursor(&self) -> WatchCursor {
            self.inner.cursor()
        }
        fn export_to(
            &mut self,
            dest_dir: &std::path::Path,
        ) -> Result<crate::artifact::ExportManifest, crate::snapshot::SnapshotError> {
            self.inner.export_to(dest_dir)
        }
    }

    /// CURSOR AUTHORITY under a transient store failure: a failed store apply
    /// must NOT cause later successful applies to advance the persisted
    /// cursor past data that never landed. The failed batch is re-queued and
    /// committed cumulatively with the next flush, so the store's cursor
    /// never lies about its contents — a restart resuming from it sees
    /// exactly the missing tail, not a silent hole.
    ///
    /// (Pre-fix behavior, found while writing the watch_applied model: the
    /// failed batch's raw updates were dropped on the warn-and-continue
    /// path, and the NEXT successful flush committed only newer updates
    /// under the newest cursor — a permanent, restart-surviving gap in the
    /// fold.)
    #[tokio::test(start_paused = true)]
    async fn transient_store_failure_never_leaves_a_cursor_gap() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("fold.snap");
        let (_r, inner) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        let store = FailOnceStore {
            inner,
            failed: std::sync::atomic::AtomicBool::new(false),
        };

        // max: 1 -> one flush per update: flush #1 (a@1) hits the injected
        // failure, flush #2 (b@2) succeeds.
        let updates = vec![put("node.a", b"1", 1), put("node.b", b"2", 2)];
        let watcher = Arc::new(MockWatcher::new(updates, true));
        let (sd_tx, sd_rx) = watch::channel(false);

        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::All,
            None,
            None,
            Some(store),
            None,
            BatchConfig {
                window: Duration::from_millis(1),
                max: 1,
                ..BatchConfig::default()
            },
            parse_put,
            |_batch: Vec<Vec<u8>>| {},
            |_| {},
            sd_rx,
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        sd_tx.send(true).unwrap();
        let cursor = task.await.unwrap().unwrap();
        assert_eq!(cursor.as_u64(), Some(2));

        // The store on disk must be SELF-CONSISTENT: whatever its cursor
        // claims, the data at or below it is present. With the re-queue fix
        // the cumulative commit lands both keys at cursor 2.
        let (persisted, reopened) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        assert_eq!(persisted.as_u64(), Some(2), "cursor reached the head");
        assert_eq!(
            reopened.get("node.a").unwrap().map(|e| e.value),
            Some(b"1".to_vec()),
            "the transiently-failed batch was re-queued, not silently dropped \
             behind an advancing cursor"
        );
        assert_eq!(
            reopened.get("node.b").unwrap().map(|e| e.value),
            Some(b"2".to_vec())
        );
    }
    /// A reader whose live-key listing always fails — the resync's I/O
    /// failure mode.
    struct FailingReader;

    #[async_trait]
    impl KvReader for FailingReader {
        async fn get(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }
        async fn entry(&self, _key: &str) -> Result<Option<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }
        async fn keys(&self, _prefix: &str) -> Result<Vec<String>, KvError> {
            Err(KvError::OperationFailed("injected listing failure".into()))
        }
        async fn scan(&self, _prefix: &str) -> Result<Vec<KvEntry>, KvError> {
            unreachable!("resync only lists keys")
        }
    }

    /// REGRESSION PIN (code-level twin of tests/model.rs's Degrade
    /// configuration): a resync whose live-key listing fails must FAIL THE
    /// WATCH, not degrade to re-list-only with a warning — the degrade
    /// semantics provably break the convergence theorem (silent stale keys).
    /// Reverting `resync_stale_keys` to warn-and-continue fails this test.
    #[tokio::test]
    async fn resync_listing_failure_is_fatal_not_degraded() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("resync-fatal.snap");
        let (_r, mut store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        store
            .apply(&[put("node.a", b"1", 1)], &WatchCursor::from_u64(1))
            .unwrap();

        // Resume cursor expired -> resync path -> reader listing fails.
        let mock = MockWatcher {
            full: Mutex::new(Some(vec![])),
            from: Mutex::new(Some(vec![])),
            from_expires: true,
            hold: false,
        };
        let (_sd_tx, sd_rx) = watch::channel(false);
        let err = watch_applied(
            Arc::new(mock),
            WatchScope::All,
            Some(WatchCursor::from_u64(1)),
            Some(Arc::new(FailingReader) as Arc<dyn KvReader>),
            Some(store),
            None,
            BatchConfig::default(),
            parse_put,
            |_batch: Vec<Vec<u8>>| {},
            |_| {},
            sd_rx,
        )
        .await
        .expect_err("a failed resync listing must fail the watch");
        assert!(
            err.to_string().contains("resync failed listing live keys"),
            "{err}"
        );
    }
}
