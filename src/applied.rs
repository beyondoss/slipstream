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
use tracing::{error, info, warn};

use crate::artifact::ExportManifest;
use crate::kv::{KvError, KvReader, KvUpdate, KvWatcher, Retention, WatchCursor};
use crate::protocol::resume_window_ok;
use crate::repair::{
    ExpiryRepair, RestoreSource, RestoredFold, check_restore, cursor_rank, materialize,
    restore_diff,
};
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

/// Internal: a cursor-expired repair handoff from the watch task to the main
/// loop, which owns the fold. The watch task stays parked on the ack/reply
/// until the main loop has folded the repair, so the repair is strictly
/// ordered between everything delivered before the expiry and everything the
/// next watch delivers.
enum RepairRequest<S> {
    /// The key-listing diff: the bucket's live keys for the watch scope. The
    /// main loop applies synthetic deletes for in-scope keys missing from it,
    /// then acks so the watch task can start the fallback re-list.
    Relist {
        live_keys: Vec<String>,
        ack: oneshot::Sender<()>,
    },
    /// The artifact restore: a verified artifact fold. The main loop folds
    /// the in-scope difference, advances to the artifact's cursor, and replies
    /// with it so the watch task resumes from there.
    Restore {
        restored: RestoredFold<S>,
        reply: oneshot::Sender<Result<WatchCursor, KvError>>,
    },
}

/// Internal: what the watch task needs to repair an expiry — the caller's
/// chosen repair, and the channel into the main loop that owns the fold.
struct RepairHandle<S> {
    mode: ExpiryRepair<S>,
    tx: mpsc::Sender<RepairRequest<S>>,
}

/// Internal: the repair the watch task settled on for one expiry.
enum Plan<'a, S> {
    /// Fall back to the re-list alone (nothing armed).
    ReListOnly,
    /// The key-listing diff, then the re-list.
    Relist(&'a Arc<dyn KvReader>, &'a mpsc::Sender<RepairRequest<S>>),
    /// The artifact restore, then a resume from the artifact's cursor.
    Restore(
        &'a Arc<dyn RestoreSource<S>>,
        &'a mpsc::Sender<RepairRequest<S>>,
    ),
}

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
/// # Cursor expiry
///
/// The cursor expires when the log no longer holds the revision after it: at
/// resume ([`KvError::CursorExpired`] from the `*_from` watch), or mid-watch,
/// when the live floor guard finds retention overran the consumer (All scope).
/// Both take the same path. Everything delivered before the expiry is folded
/// first; then `repair` (an [`ExpiryRepair`], or the pre-0.8
/// `Option<Arc<dyn KvReader>>`, which converts to it) decides what the fold
/// can trust:
///
/// - [`ExpiryRepair::Relist`]: the key-listing diff. The bucket's live keys
///   are listed via the reader and diffed against the fold's in-scope keys; a
///   synthetic [`KvUpdate::Delete`] (unknown
///   [`VersionToken`](crate::VersionToken), never moves the cursor) runs
///   through `parse`/`apply`/store for each key that vanished, strictly before
///   the full-scope re-list (`watch_all` / `watch_prefix` / `watch_prefixes`)
///   starts. Sound only when "not in the bucket" means "deleted", so it is
///   REFUSED — the watch fails — when the watcher reports that retention
///   evicts current values (`max_age`, `discard: old`).
/// - [`ExpiryRepair::Restore`]: the artifact restore. The newest published
///   artifact is checked (ahead of the fold, covers this scope, cursor still
///   inside retention), downloaded, and the in-scope fold replaced with its
///   contents: changed keys as puts carrying their real revisions, keys it
///   lacks as synthetic deletes, all through `parse`/`apply`/store, then the
///   cursor advanced to the artifact's. The watch resumes from there. Each
///   chunk commits under the old (expired) cursor until the last, so a crash
///   mid-restore re-runs it on the next start. A failed check fails the watch
///   with the reason logged at `error`: there is no safe recovery from a
///   stale artifact.
/// - [`ExpiryRepair::Auto`]: Relist when the bucket never evicts current
///   values, Restore otherwise.
/// - [`ExpiryRepair::None`] (or no `store`): the re-list alone, with a warning
///   that keys deleted during the gap may persist.
///
/// A repair that was armed but FAILS (listing, fold scan, download, checks) is
/// fatal to the watch — degrading would silently leave the fold wrong
/// (`tests/model.rs` proves that divergence reachable) — and the caller's
/// restart retries from scratch.
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
pub async fn watch_applied<U, S, P, A, O, R>(
    watcher: Arc<dyn KvWatcher>,
    scope: WatchScope,
    resume: Option<WatchCursor>,
    // How an expired cursor is repaired (see the function docs). Only
    // consulted on expiry (and, for a restore, at a cursor-less start on a
    // bucket that has already evicted current values) — the hot path never
    // touches it.
    repair: R,
    mut store: Option<S>,
    // `Some(rx)` arms an export-request arm in the select loop: each
    // [`ExportRequest`] is handled between flushes (pending batch flushed
    // first), so the exported artifact's cursor is the applied cursor (or,
    // across a transiently failed store flush, the store's own lagging but
    // self-consistent cursor — never a cursor past unfolded data). The
    // artifact's manifest records this watch's scope, which a restore checks.
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
    R: Into<ExpiryRepair<S>>,
{
    // The cursor we'll return. Initialized from the resume position so that a
    // watch which receives nothing new still reports the position it resumed
    // from as "applied" (it is — everything up to it was applied before the last
    // run persisted it).
    let mut applied = match &resume {
        Some(c) => c.clone(),
        None => WatchCursor::none(),
    };

    // The scope's prefixes, for the repair diffs against the fold and the
    // scope recorded in exported artifacts. Cloned out before `scope` moves
    // into the watch task.
    let scope_prefixes = scope.prefixes();

    // Repair channel, armed only when there is a repair to run AND a store to
    // run it against (both repairs edit the fold). The watch task sends the
    // repair here and waits for the ack/reply before starting its next watch.
    let (repair_handle, mut repairs): (
        Option<RepairHandle<S>>,
        Option<mpsc::Receiver<RepairRequest<S>>>,
    ) = match repair.into() {
        ExpiryRepair::None => (None, None),
        mode if store.is_some() => {
            let (tx, rx) = mpsc::channel(1);
            (Some(RepairHandle { mode, tx }), Some(rx))
        }
        _ => (None, None),
    };

    // A fold with data but no cursor can't be re-listed blind (see
    // `run_watch`). Only the first entry is read.
    let unanchored = match &store {
        Some(st) if resume.as_ref().is_none_or(WatchCursor::is_none) => fold_has_entries(st)
            .map_err(|e| KvError::WatchError(format!("reading the fold at start failed: {e}")))?,
        _ => false,
    };

    // Spawn the watch task. It owns the cursor-expiry handling so the main loop
    // only ever sees a clean ordered stream of updates on `rx`, plus repairs.
    let (tx, mut rx) = mpsc::channel::<KvUpdate>(config.channel_capacity.max(1));
    let handle = {
        let watcher = Arc::clone(&watcher);
        tokio::spawn(async move {
            run_watch(
                watcher.as_ref(),
                &scope,
                resume,
                unanchored,
                repair_handle,
                tx,
            )
            .await
        })
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

    // A single timer future, reset in place each time a batch opens. The old
    // `tokio::time::sleep(timeout)` lived inside the select arm, so it was
    // re-created on every loop iteration — one Arc-backed timer-wheel entry
    // allocated, registered, and immediately dropped per received update.
    // Pinning one future and `reset`-ing it reuses that single allocation; the
    // `if batch_deadline.is_some()` guard keeps it from firing while idle, so
    // its initial already-elapsed deadline is never observed.
    let sleep = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(sleep);

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
            // (`raw_batch` can be non-empty with no cursor advance only via a
            // repair's synthetic updates, which carry no stream position.)
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
                // A cursor-only advance (nothing raw to fold) still commits
                // to the store: a restore's final step moves the cursor to the
                // artifact's with no updates left. In the steady state every
                // advance carries raw updates, so this changes nothing there.
                if (!raw_batch.is_empty() || advanced)
                    && let Some(mut st) = store.take()
                {
                    let raw = std::mem::take(&mut raw_batch);
                    // Fold at the post-advance cursor. A repair-only batch
                    // (synthetic updates) leaves the cursor where it was (they
                    // are a state correction, not log entries), which is safe:
                    // an unchanged — possibly expired — cursor only ever re-runs
                    // the same repair on the next restart.
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

    // Take one update off the watch channel into the pending batch.
    macro_rules! ingest {
        ($u:expr) => {{
            let u: KvUpdate = $u;
            // Cursor authority: every received update bumps the pending
            // high-water, regardless of whether `parse` keeps it — but only
            // when it carries a real position. An unknown version (e.g. an
            // unparseable ACK subject on the hand-built multi-prefix consumer
            // path) must neither mint a fake cursor nor clobber the real high
            // from earlier in the batch; skipping it under-advances at worst,
            // and re-delivery on resume is idempotent.
            if !u.version().is_unknown() {
                batch_high = WatchCursor::from_version(u.version().clone());
            }

            // Buffer the raw update for the durable store fold (which commits
            // the whole batch + cursor atomically on flush). Done before
            // `parse` consumes `u` by reference, and only when a store is
            // present so the no-persistence path keeps its zero-copy cost.
            if store.is_some() {
                raw_batch.push(u.clone());
            }

            if let Some(parsed) = parse(&u) {
                batch.push(parsed);
            }

            // Arm the window on the first received update of a batch — even a
            // parse-rejected one, so the cursor advances within `window` even
            // through a run of irrelevant keys. Reset the pinned timer to the
            // new deadline rather than allocating a fresh `Sleep`.
            if batch_deadline.is_none() {
                let deadline = tokio::time::Instant::now() + config.window;
                sleep.as_mut().reset(deadline);
                batch_deadline = Some(deadline);
            }

            // Flush on a full parsed batch, or — when persisting — a full raw
            // batch, so a window packed with parse-rejected updates can't grow
            // `raw_batch` without bound before the window elapses.
            if batch.len() >= config.max || raw_batch.len() >= config.max {
                flush!();
            }
        }};
    }

    // Fail the watch from inside a repair: the fold is either unchanged or
    // part-way through a repair that commits under its old (expired) cursor,
    // so the caller's restart re-runs the whole expiry path.
    macro_rules! repair_fatal {
        ($($msg:tt)*) => {{
            let msg = format!($($msg)*);
            error!(%msg, "cursor-expiry repair failed; aborting watch");
            handle.abort();
            return Err(KvError::WatchError(msg));
        }};
    }

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

            // Cursor-expiry repair. Placed before `rx.recv()` (biased), but
            // ordering against the update stream comes from the drain below
            // and the ack/reply protocol: the watch task is parked until this
            // arm finishes, so nothing from its next watch can interleave.
            req = async { repairs.as_mut().expect("arm guarded by is_some").recv().await },
                if repairs.is_some() => {
                match req {
                    Some(req) => {
                        // Fold everything the watch task delivered before it
                        // asked for this repair. A floor-guard trip ends a
                        // live watch mid-stream with updates still buffered in
                        // the channel: the repair must see them (it diffs the
                        // fold) and supersede them (one applied after the
                        // repair would resurrect pre-repair state, and could
                        // even move the cursor backward). The watch task is
                        // parked on the ack/reply, so this drains exactly that
                        // backlog.
                        while let Ok(u) = rx.try_recv() {
                            ingest!(u);
                        }
                        flush!();
                        // Both repairs diff the STORE, so it must hold
                        // everything delivered. A transient store failure
                        // re-queues the batch in `raw_batch`, which the diff
                        // can't see: it would miss a re-queued put that a
                        // deletion during the gap has since removed, and the
                        // put would then commit and resurrect the key. Retry
                        // until the store has it all; a persistent failure
                        // hits the flush's own fail-stop bound.
                        while !raw_batch.is_empty() {
                            flush!();
                        }
                        match req {
                            RepairRequest::Relist { live_keys, ack } => {
                                // Diff the fold's in-scope keys against the
                                // bucket's live listing; anything the fold
                                // holds that the bucket no longer does vanished
                                // during the gap (its delete marker evicted with
                                // the cursor), so synthesize the delete the
                                // re-list can't deliver.
                                let live: std::collections::HashSet<&str> =
                                    live_keys.iter().map(String::as_str).collect();
                                let mut stale: Vec<String> = Vec::new();
                                if let Some(st) = &store {
                                    for prefix in &scope_prefixes {
                                        // Stream the fold's keys rather than
                                        // `range()`, which buffers every in-scope
                                        // entry — values included — into one Vec.
                                        // On an on-disk backend holding a fold
                                        // larger than RAM (the case those backends
                                        // exist for), an All-scope resync would
                                        // materialize the entire fold on the
                                        // repair path. Only the keys matter.
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
                                    // Synthetic: carries no revision (unknown
                                    // version) and so never advances the cursor.
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
                                // Ack AFTER the deletes are applied: the watch
                                // task is holding the fallback watch until it
                                // hears back, which is what orders deletes
                                // before the re-list
                                // (tests/model_resync_order.rs proves the
                                // barrier load-bearing). If the flush's STORE
                                // apply failed transiently, the deletes sit
                                // re-queued at the FRONT of the raw batch —
                                // still strictly before any re-list put in the
                                // eventual cumulative commit, and the domain
                                // apply saw them before this ack either way.
                                let _ = ack.send(());
                            }
                            RepairRequest::Restore { restored, reply } => {
                                let target = restored.manifest().cursor.clone();
                                // The authoritative ahead check (the watch
                                // task's ran against its resume cursor; a
                                // mid-watch expiry delivered past that). A
                                // restore never moves the fold backward.
                                if cursor_rank(&target) <= cursor_rank(&applied) {
                                    let msg = format!(
                                        "cursor-expired restore: the latest artifact (cursor \
                                         {target:?}) is not ahead of the fold (cursor {applied:?}); \
                                         nothing newer to restore from"
                                    );
                                    error!(%msg, "cursor-expiry repair refused");
                                    // The watch task fails with this; its
                                    // channel closing ends the loop.
                                    let _ = reply.send(Err(KvError::WatchError(msg)));
                                } else {
                                    let Some(st) = store.take() else {
                                        unreachable!("repairs are armed only with a store")
                                    };
                                    let prefixes = scope_prefixes.clone();
                                    let (st, mut restored, diff) =
                                        match tokio::task::spawn_blocking(move || {
                                            let diff = restore_diff(&st, restored.fold(), &prefixes);
                                            (st, restored, diff)
                                        })
                                        .await
                                        {
                                            Ok(v) => v,
                                            Err(e) => repair_fatal!(
                                                "cursor-expired restore: diff task panicked: {e}"
                                            ),
                                        };
                                    store = Some(st);
                                    let diff = match diff {
                                        Ok(d) => d,
                                        Err(e) => repair_fatal!(
                                            "cursor-expired restore: diffing the fold against the \
                                             artifact failed: {e}"
                                        ),
                                    };
                                    info!(
                                        from = ?applied,
                                        to = ?target,
                                        changed = diff.len(),
                                        "cursor-expired restore: replacing the in-scope fold with the artifact's"
                                    );
                                    // Fold the difference through parse/apply/
                                    // store in `max`-sized chunks, values read
                                    // back per chunk. These carry no stream
                                    // position: every chunk commits under the
                                    // old cursor, so a crash part-way re-runs
                                    // the (idempotent) restore on restart.
                                    let mut ops = diff.into_iter();
                                    loop {
                                        let chunk: Vec<_> = ops.by_ref().take(batch_cap).collect();
                                        if chunk.is_empty() {
                                            break;
                                        }
                                        let (r, updates) = match tokio::task::spawn_blocking(move || {
                                            let updates = materialize(restored.fold(), chunk);
                                            (restored, updates)
                                        })
                                        .await
                                        {
                                            Ok(v) => v,
                                            Err(e) => repair_fatal!(
                                                "cursor-expired restore: read task panicked: {e}"
                                            ),
                                        };
                                        restored = r;
                                        let updates = match updates {
                                            Ok(u) => u,
                                            Err(e) => repair_fatal!(
                                                "cursor-expired restore: reading the artifact failed: {e}"
                                            ),
                                        };
                                        for u in updates {
                                            if store.is_some() {
                                                raw_batch.push(u.clone());
                                            }
                                            if let Some(parsed) = parse(&u) {
                                                batch.push(parsed);
                                            }
                                        }
                                        flush!();
                                    }
                                    // Now the fold IS the artifact's (in
                                    // scope): take its cursor, committed to
                                    // the store with the final chunk's
                                    // re-queued remainder, if any.
                                    batch_high = target.clone();
                                    flush!();
                                    // Closing an LSM can block; drop the
                                    // temporary fold and its scratch dir off
                                    // the async thread.
                                    let _ = tokio::task::spawn_blocking(move || drop(restored)).await;
                                    let _ = reply.send(Ok(target));
                                }
                            }
                        }
                    }
                    None => repairs = None,
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
            // in and taken back — the same offload the flush path uses. The
            // artifact's manifest records this watch's scope, so a restore can
            // tell whether it covers the restoring watcher.
            req = async { exports.as_mut().expect("arm guarded by is_some").recv().await },
                if exports.is_some() => {
                match req {
                    Some(ExportRequest { dest_dir, reply }) => {
                        flush!();
                        match store.take() {
                            Some(mut st) => {
                                let scope = scope_prefixes.clone();
                                match tokio::task::spawn_blocking(move || {
                                    let res = st.export_to(&dest_dir).and_then(|_| {
                                        crate::artifact::stamp_scope(&dest_dir, &scope)
                                    });
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
                    Some(u) => ingest!(u),
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

/// Does the fold hold any entry? Stops at the first.
fn fold_has_entries<S: SnapshotStore>(store: &S) -> Result<bool, SnapshotError> {
    let mut found = false;
    match store.for_each_in_range("", |_| {
        found = true;
        // Stop the scan; `found` carries the answer.
        Err(SnapshotError::Backend(String::new()))
    }) {
        Err(e) if !found => Err(e),
        _ => Ok(found),
    }
}

/// Run the underlying watch for `scope`, resuming from `resume` when it carries
/// a position, with cursor-expiry repair: an expiry (at resume, or mid-watch
/// from the floor guard) runs the planned repair and then either falls back to
/// the full-scope re-list (Relist / nothing armed) or resumes from the restored
/// artifact's cursor (Restore) — which can itself expire later and go round
/// again, each time only for a strictly newer artifact.
///
/// `unanchored`: the fold holds data but no cursor (a torn first checkpoint,
/// or a populated store started without one). A re-list alone never removes
/// what it doesn't deliver, so that start is repaired like an expiry at
/// revision 0 before watching.
async fn run_watch<S: Send + 'static>(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    resume: Option<WatchCursor>,
    unanchored: bool,
    repair: Option<RepairHandle<S>>,
    tx: mpsc::Sender<KvUpdate>,
) -> Result<(), KvError> {
    // Resume only when the cursor carries a real position; an absent or `none()`
    // cursor falls through to a full watch. Binding `cursor` here makes "we have a
    // resume position" structural — there is no separate bool whose truth a later
    // edit could let drift from the `Some`.
    let mut resume = resume.filter(|c| !c.is_none());

    // A cursor-less start that must be repaired before it watches: a fold
    // with data and no cursor (any repair), or an empty fold on a bucket that
    // already evicted current values with a restore armed (its re-list would
    // seed an incomplete fold).
    let mut repair_first = resume.is_none()
        && match &repair {
            // (A handle exists only for a real repair mode.)
            Some(_) if unanchored => {
                warn!(
                    "the fold holds data but no cursor; repairing it like an expired cursor \
                     before watching (a re-list alone never removes what it doesn't deliver)"
                );
                true
            }
            Some(h) => {
                let seed = fresh_start_needs_restore(watcher, h).await?;
                if seed {
                    warn!(
                        "no resume cursor, and the bucket has already evicted current values: a \
                         re-list would seed an incomplete fold; restoring from the latest artifact"
                    );
                }
                seed
            }
            None => false,
        };

    loop {
        let cursor = if std::mem::take(&mut repair_first) {
            WatchCursor::none()
        } else {
            let Some(cursor) = resume.take() else {
                return watch_scope(watcher, scope, tx).await;
            };
            match watch_scope_from(watcher, scope, &cursor, tx.clone()).await {
                Err(KvError::CursorExpired) => cursor,
                other => return other,
            }
        };
        match plan_repair(watcher, repair.as_ref()).await? {
            Plan::ReListOnly => {
                warn!(
                    "watch cursor expired with no repair armed (needs a store and a reader or \
                     restore source); falling back to the re-list alone — keys deleted during \
                     the gap may persist in the fold"
                );
                return watch_scope(watcher, scope, tx).await;
            }
            Plan::Relist(reader, repairs) => {
                warn!(
                    "watch cursor expired; resyncing stale keys, then falling back to the full re-list"
                );
                resync_stale_keys(scope, reader, repairs).await?;
                return watch_scope(watcher, scope, tx).await;
            }
            Plan::Restore(source, repairs) => {
                warn!("watch cursor expired; restoring the fold from the latest artifact");
                let restored = restore_from_artifact(watcher, scope, source, repairs, &cursor)
                    .await
                    .map_err(|e| match e {
                        KvError::WatchError(msg) if cursor.is_none() => {
                            KvError::WatchError(format!(
                                "{msg}. This node has no cursor, so it can only seed from an \
                             artifact. If none exists yet (first deploy onto a bucket that \
                             already evicted values), start one node with ExpiryRepair::None \
                             — accepting that values which already aged out are gone — and \
                             publish an export from it"
                            ))
                        }
                        other => other,
                    })?;
                resume = Some(restored);
            }
        }
    }
}

/// The full-scope (state-sync re-list) watch for `scope`.
async fn watch_scope(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    tx: mpsc::Sender<KvUpdate>,
) -> Result<(), KvError> {
    match scope {
        WatchScope::All => watcher.watch_all(tx).await,
        WatchScope::Prefix(prefix) => watcher.watch_prefix(prefix, tx).await,
        WatchScope::Prefixes(prefixes) => {
            let refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
            watcher.watch_prefixes(&refs, tx).await
        }
    }
}

/// The delta watch for `scope`, resuming after `cursor`.
async fn watch_scope_from(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    cursor: &WatchCursor,
    tx: mpsc::Sender<KvUpdate>,
) -> Result<(), KvError> {
    match scope {
        WatchScope::All => watcher.watch_all_from(cursor, tx).await,
        WatchScope::Prefix(prefix) => watcher.watch_prefix_from(prefix, cursor, tx).await,
        WatchScope::Prefixes(prefixes) => {
            let refs: Vec<&str> = prefixes.iter().map(String::as_str).collect();
            watcher.watch_prefixes_from(&refs, cursor, tx).await
        }
    }
}

/// Is the bucket's key listing the truth — does "not listed" mean "deleted"?
/// Yes when retention never evicts current values, and also when it can but
/// has never evicted anything (the first retained revision is still 1). Read
/// live; `None` when the backend can't say.
fn listing_is_truth(r: &Retention) -> bool {
    !r.evicts_current_values || resume_window_ok(0, r.first_revision)
}

/// Decide how to repair one expiry, reading the bucket's retention live when
/// the choice depends on it.
async fn plan_repair<'a, S>(
    watcher: &dyn KvWatcher,
    repair: Option<&'a RepairHandle<S>>,
) -> Result<Plan<'a, S>, KvError> {
    let Some(h) = repair else {
        return Ok(Plan::ReListOnly);
    };
    Ok(match &h.mode {
        ExpiryRepair::None => Plan::ReListOnly,
        ExpiryRepair::Relist(reader) => {
            if watcher
                .retention()
                .await?
                .is_some_and(|r| !listing_is_truth(&r))
            {
                let msg = "watch cursor expired on a bucket whose retention evicts current \
                           values (max_age, per-message TTL, or discard:old): its key listing \
                           can't tell a deleted key from an aged-out one, so the key-listing \
                           resync would delete valid keys. Refusing. Repair from artifacts with \
                           ExpiryRepair::Restore or ExpiryRepair::Auto (or accept a re-list-only \
                           fallback with ExpiryRepair::None)";
                error!(msg);
                return Err(KvError::WatchError(msg.into()));
            }
            Plan::Relist(reader, &h.tx)
        }
        ExpiryRepair::Restore(source) => Plan::Restore(source, &h.tx),
        ExpiryRepair::Auto { reader, restore } => match watcher.retention().await? {
            Some(r) if listing_is_truth(&r) => Plan::Relist(reader, &h.tx),
            _ => Plan::Restore(restore, &h.tx),
        },
    })
}

/// Must an EMPTY cursor-less start seed from an artifact? Only with a restore
/// armed, on a bucket whose re-list is no longer complete (it evicted current
/// values). A bucket whose listing is the truth re-lists completely.
async fn fresh_start_needs_restore<S>(
    watcher: &dyn KvWatcher,
    h: &RepairHandle<S>,
) -> Result<bool, KvError> {
    if !matches!(h.mode, ExpiryRepair::Restore(_) | ExpiryRepair::Auto { .. }) {
        return Ok(false);
    }
    Ok(watcher
        .retention()
        .await?
        .is_some_and(|r| !listing_is_truth(&r)))
}

/// The artifact restore, watch-task half: check the newest artifact before
/// downloading it (ahead of `local`, covers the scope, still inside
/// retention), download it, check what actually arrived, then hand it to the
/// main loop and wait for the cursor it restored to.
async fn restore_from_artifact<S: Send + 'static>(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    source: &Arc<dyn RestoreSource<S>>,
    repairs: &mpsc::Sender<RepairRequest<S>>,
    local: &WatchCursor,
) -> Result<WatchCursor, KvError> {
    let prefixes = scope.prefixes();
    let fail = |msg: String| {
        error!(%msg, "cursor-expiry restore refused; no safe recovery");
        KvError::WatchError(format!("cursor-expired restore: {msg}"))
    };

    let first = watcher.retention().await?.map(|r| r.first_revision);
    let latest = source
        .latest()
        .await
        .map_err(|e| fail(format!("reading the latest artifact failed: {e}")))?;
    check_restore(&latest, local, first, &prefixes).map_err(fail)?;

    let restored = source
        .fetch()
        .await
        .map_err(|e| fail(format!("fetching the latest artifact failed: {e}")))?;
    // The pointer may have advanced between the peek and the download, and
    // retention may have moved: check what actually arrived, against now.
    let first = watcher.retention().await?.map(|r| r.first_revision);
    check_restore(restored.manifest(), local, first, &prefixes).map_err(fail)?;
    let age_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|now| {
            now.as_secs()
                .saturating_sub(restored.manifest().created_at_unix)
        })
        .unwrap_or(0);
    info!(
        cursor = ?restored.manifest().cursor,
        age_secs,
        first_retained = ?first,
        "restoring the fold from the latest artifact"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    repairs
        .send(RepairRequest::Restore {
            restored,
            reply: reply_tx,
        })
        .await
        .map_err(|_| KvError::WatchError("watch loop ended during a restore".into()))?;
    reply_rx
        .await
        .map_err(|_| KvError::WatchError("watch loop dropped a restore reply".into()))?
}

/// Cursor-expired stale-key resync (the key-listing diff), run BEFORE the
/// fallback watch is established: list the scope's live keys, hand them to the
/// main loop (which diffs them against the fold and applies synthetic
/// deletes), and wait for the ack. That ordering — deletes applied, then
/// fallback watch armed — is what makes a delete-then-recreate during the gap
/// converge: the synthetic delete always lands before the re-list put.
///
/// Sound only on a bucket that never evicts current values; `plan_repair`
/// refuses it otherwise.
///
/// A FAILED listing is **fatal** — it fails the watch rather than degrading.
/// The resync is load-bearing for the "stale, never corrupt" convergence
/// guarantee: a silently degraded resync leaves the fold holding keys the
/// bucket deleted, with one warn line as the only witness (`tests/model.rs`
/// proves this divergence reachable under degrade semantics). Failing the
/// watch turns the violated guarantee into a visible error; the caller's
/// restart re-resumes, hits `CursorExpired` again, and retries the resync from
/// scratch.
async fn resync_stale_keys<S>(
    scope: &WatchScope,
    reader: &Arc<dyn KvReader>,
    repairs: &mpsc::Sender<RepairRequest<S>>,
) -> Result<(), KvError> {
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
    if repairs
        .send(RepairRequest::Relist {
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

    // --- Cursor-expiry repair: evicting buckets, artifact restore -----------

    use crate::kv::Retention;
    use crate::repair::{ExpiryRepair, RestoreSource, RestoredFold};
    use std::collections::VecDeque;

    /// A watcher scripted per `*_from` call, reporting configurable retention.
    /// Each step delivers its updates and then either ends in `CursorExpired`
    /// (a resume-time expiry when the step is empty, a mid-watch floor-guard
    /// trip when it isn't) or returns cleanly, closing the stream. Records the
    /// cursor every `*_from` call resumed from.
    struct ScriptedWatcher {
        from_steps: Mutex<VecDeque<(Vec<KvUpdate>, bool)>>,
        full: Mutex<Option<Vec<KvUpdate>>>,
        retention: Option<Retention>,
        resumed_from: Mutex<Vec<u64>>,
    }

    impl ScriptedWatcher {
        fn new(retention: Option<Retention>, steps: Vec<(Vec<KvUpdate>, bool)>) -> Self {
            Self {
                from_steps: Mutex::new(steps.into()),
                full: Mutex::new(Some(vec![])),
                retention,
                resumed_from: Mutex::new(vec![]),
            }
        }

        async fn step(&self, cursor: &WatchCursor, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.resumed_from
                .lock()
                .unwrap()
                .push(cursor.as_u64().unwrap_or(0));
            let step = self.from_steps.lock().unwrap().pop_front();
            let Some((updates, expire)) = step else {
                return Ok(());
            };
            for u in updates {
                if tx.send(u).await.is_err() {
                    return Ok(());
                }
            }
            if expire {
                Err(KvError::CursorExpired)
            } else {
                Ok(())
            }
        }

        async fn full(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            let updates = self.full.lock().unwrap().take().unwrap_or_default();
            for u in updates {
                let _ = tx.send(u).await;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl KvWatcher for ScriptedWatcher {
        async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.full(tx).await
        }
        async fn watch_prefix(&self, _p: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.full(tx).await
        }
        async fn watch_prefixes(&self, _p: &[&str], tx: Sender<KvUpdate>) -> Result<(), KvError> {
            self.full(tx).await
        }
        async fn watch_all_from(
            &self,
            cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            self.step(cursor, tx).await
        }
        async fn watch_prefix_from(
            &self,
            _p: &str,
            cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            self.step(cursor, tx).await
        }
        async fn watch_prefixes_from(
            &self,
            _p: &[&str],
            cursor: &WatchCursor,
            tx: Sender<KvUpdate>,
        ) -> Result<(), KvError> {
            self.step(cursor, tx).await
        }
        async fn retention(&self) -> Result<Option<Retention>, KvError> {
            Ok(self.retention)
        }
    }

    fn evicting(first_revision: u64) -> Option<Retention> {
        Some(Retention {
            evicts_current_values: true,
            first_revision,
        })
    }

    /// A restore source serving one artifact: a fold built from `updates` at
    /// `cursor`, recorded under `scope`. Counts downloads.
    struct MockRestore {
        dir: tempfile::TempDir,
        updates: Vec<KvUpdate>,
        cursor: u64,
        scope: Option<Vec<String>>,
        fetches: AtomicU64,
    }

    impl MockRestore {
        fn new(updates: Vec<KvUpdate>, cursor: u64, scope: Option<&[&str]>) -> Arc<Self> {
            Arc::new(Self {
                dir: tempfile::TempDir::new().unwrap(),
                updates,
                cursor,
                scope: scope.map(|s| s.iter().map(|p| p.to_string()).collect()),
                fetches: AtomicU64::new(0),
            })
        }

        fn manifest(&self) -> ExportManifest {
            ExportManifest {
                schema_version: crate::ARTIFACT_SCHEMA_VERSION,
                backend: "append-log".into(),
                backend_version: "2".into(),
                cursor: WatchCursor::from_u64(self.cursor),
                created_at_unix: 0,
                files: vec![],
                scope: self.scope.clone(),
            }
        }
    }

    #[async_trait]
    impl RestoreSource<AppendLogSnapshot> for MockRestore {
        async fn latest(&self) -> Result<ExportManifest, SnapshotError> {
            Ok(self.manifest())
        }
        async fn fetch(&self) -> Result<RestoredFold<AppendLogSnapshot>, SnapshotError> {
            let n = self.fetches.fetch_add(1, Ordering::SeqCst);
            let path = self.dir.path().join(format!("artifact-{n}.snap"));
            let (_c, mut fold) = AppendLogSnapshot::open(&path, u64::MAX)?;
            fold.apply(&self.updates, &WatchCursor::from_u64(self.cursor))?;
            Ok(RestoredFold::new(self.manifest(), fold))
        }
    }

    /// The local fold every restore test starts from: three keys at cursor 4.
    fn expired_fold(dir: &tempfile::TempDir) -> (std::path::PathBuf, AppendLogSnapshot) {
        let path = dir.path().join("fold.snap");
        let (_r, mut store) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
        store
            .apply(
                &[
                    put("node.keep", b"v1", 1),
                    put("node.changed", b"old", 2),
                    put("node.gone", b"x", 3),
                    put("node.a", b"a", 4),
                ],
                &WatchCursor::from_u64(4),
            )
            .unwrap();
        (path, store)
    }

    /// What one watch run did: every update `parse` saw, as (key, is_delete),
    /// every cursor `on_applied` reported, and the watch's result.
    struct Run {
        seen: Vec<(String, bool)>,
        cursors: Vec<u64>,
        result: Result<WatchCursor, KvError>,
    }

    async fn run_repair(
        watcher: Arc<ScriptedWatcher>,
        scope: WatchScope,
        resume: Option<u64>,
        repair: ExpiryRepair<AppendLogSnapshot>,
        store: AppendLogSnapshot,
        max: usize,
    ) -> Run {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let cursors = Arc::new(Mutex::new(Vec::new()));
        let (s, c) = (Arc::clone(&seen), Arc::clone(&cursors));
        let (_sd_tx, sd_rx) = watch::channel(false);
        let result = watch_applied(
            watcher,
            scope,
            resume.map(WatchCursor::from_u64),
            repair,
            Some(store),
            None,
            BatchConfig {
                max,
                ..BatchConfig::default()
            },
            move |u: &KvUpdate| {
                s.lock()
                    .unwrap()
                    .push((u.key().to_string(), matches!(u, KvUpdate::Delete { .. })));
                Some(())
            },
            |_batch: Vec<()>| {},
            move |cur: WatchCursor| c.lock().unwrap().push(cur.as_u64().unwrap()),
            sd_rx,
        )
        .await;
        let seen = seen.lock().unwrap().clone();
        let cursors = cursors.lock().unwrap().clone();
        Run {
            seen,
            cursors,
            result,
        }
    }

    /// THE FIX, end to end against the store: on a bucket whose retention
    /// evicts current values, an expired cursor is repaired from the artifact,
    /// not from the bucket's key listing. `Auto` picks the restore; the reader
    /// it also carries lists NOTHING (every key aged out), so a relist would
    /// have deleted the whole fold. The in-scope fold becomes the artifact's
    /// (changed value with its real revision, the gap write, the real delete),
    /// identical entries are not re-applied, chunked across `max`, the cursor
    /// moves to the artifact's only after the last chunk, and the watch
    /// resumes from there.
    #[tokio::test]
    async fn auto_restores_from_artifact_on_evicting_bucket() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let artifact = MockRestore::new(
            vec![
                put("node.keep", b"v1", 1),
                put("node.changed", b"new", 7),
                put("node.a", b"a", 4),
                put("node.late", b"gap-write", 8),
            ],
            10,
            Some(&[""]),
        );
        let watcher = Arc::new(ScriptedWatcher::new(
            evicting(9),
            vec![(vec![], true), (vec![put("node.tail", b"t", 11)], false)],
        ));
        let run = run_repair(
            Arc::clone(&watcher),
            WatchScope::All,
            Some(4),
            ExpiryRepair::Auto {
                reader: Arc::new(MockReader { live: vec![] }),
                restore: artifact.clone(),
            },
            store,
            2,
        )
        .await;

        assert_eq!(run.result.unwrap().as_u64(), Some(11));
        assert_eq!(
            run.seen,
            vec![
                ("node.changed".into(), false),
                ("node.gone".into(), true),
                ("node.late".into(), false),
                ("node.tail".into(), false),
            ],
            "the diff (key order), then the delta from the artifact's cursor"
        );
        assert_eq!(
            run.cursors,
            vec![10, 11],
            "cursor reaches the artifact's after the diff"
        );
        assert_eq!(*watcher.resumed_from.lock().unwrap(), vec![4, 10]);
        assert_eq!(artifact.fetches.load(Ordering::SeqCst), 1);

        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.cursor.as_u64(), Some(11));
        let keys: std::collections::BTreeSet<&str> =
            snap.entries.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "node.a",
                "node.changed",
                "node.keep",
                "node.late",
                "node.tail"
            ]
            .into(),
            "aged-out keys kept, gap write recovered, real delete applied"
        );
        let changed = &snap.entries["node.changed"];
        assert_eq!(changed.value, b"new");
        assert_eq!(
            changed.version.as_u64(),
            Some(7),
            "restored entries keep their revision"
        );
    }

    /// REGRESSION PIN for the reported bug: the key-listing resync on a
    /// bucket that evicts current values. The listing is empty because every
    /// key aged out; pre-fix, the resync deleted the whole fold. Now the
    /// legacy `Some(reader)` argument (which means Relist) is refused and the
    /// fold is left exactly as it was.
    #[tokio::test]
    async fn relist_refused_on_evicting_bucket() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let watcher = Arc::new(ScriptedWatcher::new(evicting(9), vec![(vec![], true)]));
        let legacy: Option<Arc<dyn KvReader>> = Some(Arc::new(MockReader { live: vec![] }));
        let run = run_repair(watcher, WatchScope::All, Some(4), legacy.into(), store, 100).await;

        let err = run
            .result
            .expect_err("relist on an evicting bucket must be refused");
        assert!(err.to_string().contains("evicts current values"), "{err}");
        assert!(run.seen.is_empty(), "no synthetic deletes reached apply");
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 4, "the fold is untouched");
        assert_eq!(snap.cursor.as_u64(), Some(4));
    }

    /// `Auto` on a bucket that never evicts current values keeps the
    /// key-listing diff (absence there does mean deleted) and never touches
    /// the artifact store.
    #[tokio::test]
    async fn auto_relists_when_bucket_never_evicts() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let artifact = MockRestore::new(vec![], 10, Some(&[""]));
        let watcher = Arc::new(ScriptedWatcher::new(
            Some(Retention {
                evicts_current_values: false,
                first_revision: 9,
            }),
            vec![(vec![], true)],
        ));
        let live = ["node.keep", "node.changed", "node.a"]
            .map(String::from)
            .to_vec();
        let run = run_repair(
            watcher,
            WatchScope::All,
            Some(4),
            ExpiryRepair::Auto {
                reader: Arc::new(MockReader { live }),
                restore: artifact.clone(),
            },
            store,
            100,
        )
        .await;

        run.result.unwrap();
        assert_eq!(run.seen, vec![("node.gone".into(), true)]);
        assert_eq!(
            artifact.fetches.load(Ordering::SeqCst),
            0,
            "no artifact needed"
        );
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert!(!snap.entries.contains_key("node.gone"));
        assert_eq!(snap.entries.len(), 3);
    }

    /// No safe recovery from a stale artifact: its cursor is outside the
    /// log's retention window, so the evictions between them exist nowhere.
    /// Refused from the manifest alone (never downloaded); the fold is
    /// untouched and the watch fails loudly.
    #[tokio::test]
    async fn restore_refuses_stale_artifact() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let artifact = MockRestore::new(vec![put("node.keep", b"v1", 1)], 6, Some(&[""]));
        let watcher = Arc::new(ScriptedWatcher::new(evicting(9), vec![(vec![], true)]));
        let run = run_repair(
            watcher,
            WatchScope::All,
            Some(4),
            ExpiryRepair::Restore(artifact.clone()),
            store,
            100,
        )
        .await;

        let err = run
            .result
            .expect_err("a stale artifact must fail the watch");
        assert!(err.to_string().contains("retention window"), "{err}");
        assert_eq!(
            artifact.fetches.load(Ordering::SeqCst),
            0,
            "refused before download"
        );
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!((snap.entries.len(), snap.cursor.as_u64()), (4, Some(4)));
    }

    /// An artifact must cover the restoring watcher's scope — and an
    /// unscoped (pre-scope / direct `export_to`) artifact can't be shown to.
    #[tokio::test]
    async fn restore_refuses_artifact_not_covering_scope() {
        for (scope, why) in [
            (Some(&["edge."][..]), "does not cover"),
            (None, "no key scope"),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let (_path, store) = expired_fold(&dir);
            let artifact = MockRestore::new(vec![], 10, scope);
            let watcher = Arc::new(ScriptedWatcher::new(evicting(9), vec![(vec![], true)]));
            let run = run_repair(
                watcher,
                WatchScope::Prefix("node.".into()),
                Some(4),
                ExpiryRepair::Restore(artifact),
                store,
                100,
            )
            .await;
            let err = run
                .result
                .expect_err("an uncovering artifact must be refused");
            assert!(err.to_string().contains(why), "{err}");
        }
    }

    /// A prefix-scoped restore replaces only the in-scope fold: an
    /// out-of-scope key in the local fold survives, and an out-of-scope key in
    /// a wider (All-scope) artifact is not imported.
    #[tokio::test]
    async fn restore_respects_watch_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, mut store) = expired_fold(&dir);
        store
            .apply(&[put("other.local", b"mine", 5)], &WatchCursor::from_u64(5))
            .unwrap();
        let artifact = MockRestore::new(
            vec![
                put("node.keep", b"v1", 1),
                put("other.remote", b"theirs", 6),
            ],
            10,
            Some(&[""]),
        );
        let watcher = Arc::new(ScriptedWatcher::new(evicting(9), vec![(vec![], true)]));
        let run = run_repair(
            watcher,
            WatchScope::Prefix("node.".into()),
            Some(5),
            ExpiryRepair::Restore(artifact),
            store,
            100,
        )
        .await;
        run.result.unwrap();
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        let keys: std::collections::BTreeSet<&str> =
            snap.entries.keys().map(String::as_str).collect();
        assert_eq!(keys, ["node.keep", "other.local"].into());
    }

    /// THE FLOOR-GUARD ROUTE: a live watch delivers past its resume cursor,
    /// then retention overruns it (the NATS floor guard ends the stream with
    /// `CursorExpired`). The delivered backlog is folded BEFORE the restore —
    /// never after it, where a stale put would resurrect old state and drag
    /// the cursor backward — and the restore diffs against that backlog.
    #[tokio::test]
    async fn floor_guard_trip_folds_backlog_then_restores() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let artifact = MockRestore::new(
            vec![
                put("node.keep", b"v1", 1),
                put("node.changed", b"old", 2),
                put("node.a", b"a2", 5),
                put("node.b", b"b-newer", 9),
            ],
            10,
            Some(&[""]),
        );
        let watcher = Arc::new(ScriptedWatcher::new(
            evicting(9),
            vec![
                (
                    vec![put("node.a", b"a2", 5), put("node.b", b"b-older", 6)],
                    true,
                ),
                (vec![put("node.c", b"c", 12)], false),
            ],
        ));
        let run = run_repair(
            Arc::clone(&watcher),
            WatchScope::All,
            Some(4),
            ExpiryRepair::Restore(artifact),
            store,
            100,
        )
        .await;

        assert_eq!(run.result.unwrap().as_u64(), Some(12));
        assert_eq!(
            run.seen,
            vec![
                ("node.a".into(), false),
                ("node.b".into(), false),
                ("node.b".into(), false),
                ("node.gone".into(), true),
                ("node.c".into(), false),
            ],
            "backlog (a, b-older), then the diff (b-newer, gone), then the delta"
        );
        assert!(
            run.cursors.windows(2).all(|w| w[0] <= w[1]),
            "the cursor never moves backward: {:?}",
            run.cursors
        );
        assert_eq!(*watcher.resumed_from.lock().unwrap(), vec![4, 10]);
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.entries["node.b"].value, b"b-newer");
        assert_eq!(snap.cursor.as_u64(), Some(12));
    }

    /// The main loop's ahead check is the authoritative one: a mid-watch
    /// expiry can deliver past the artifact the watch task accepted against
    /// its (older) resume cursor. Restoring it would move the fold backward,
    /// so it's refused and the delivered fold kept.
    #[tokio::test]
    async fn restore_behind_the_delivered_frontier_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = expired_fold(&dir);
        let artifact = MockRestore::new(vec![put("node.keep", b"v1", 1)], 10, Some(&[""]));
        let watcher = Arc::new(ScriptedWatcher::new(
            evicting(9),
            vec![(vec![put("node.z", b"z", 12)], true)],
        ));
        let run = run_repair(
            watcher,
            WatchScope::All,
            Some(4),
            ExpiryRepair::Restore(artifact),
            store,
            100,
        )
        .await;
        let err = run
            .result
            .expect_err("an artifact behind the fold must be refused");
        assert!(err.to_string().contains("not ahead"), "{err}");
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.cursor.as_u64(), Some(12));
        assert!(
            snap.entries.contains_key("node.gone"),
            "fold not rolled back"
        );
    }

    /// A cursor-less start on a bucket that has already evicted current
    /// values can't seed a complete fold from the re-list; with a restore
    /// armed it seeds from the artifact and resumes from its cursor instead
    /// of re-listing.
    #[tokio::test]
    async fn fresh_start_seeds_from_artifact_when_bucket_already_evicted() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_r, store) =
            AppendLogSnapshot::open(&dir.path().join("fresh.snap"), u64::MAX).unwrap();
        let artifact = MockRestore::new(vec![put("node.old", b"aged-out", 2)], 7, Some(&[""]));
        let watcher =
            ScriptedWatcher::new(evicting(5), vec![(vec![put("node.new", b"n", 8)], false)]);
        *watcher.full.lock().unwrap() = Some(vec![put("RELIST", b"must not run", 99)]);
        let watcher = Arc::new(watcher);
        let run = run_repair(
            Arc::clone(&watcher),
            WatchScope::All,
            None,
            ExpiryRepair::Restore(artifact),
            store,
            100,
        )
        .await;
        assert_eq!(run.result.unwrap().as_u64(), Some(8));
        assert_eq!(
            run.seen,
            vec![("node.old".into(), false), ("node.new".into(), false)]
        );
        assert_eq!(*watcher.resumed_from.lock().unwrap(), vec![7]);
    }

    /// Every artifact a watch exports records its scope (schema 2), so a
    /// restore can check coverage.
    #[tokio::test(start_paused = true)]
    async fn export_records_watch_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_r, store) = AppendLogSnapshot::open(&dir.path().join("fold.snap"), u64::MAX).unwrap();
        let watcher = Arc::new(MockWatcher::new(vec![put("node.a", b"1", 1)], true));
        let (sd_tx, sd_rx) = watch::channel(false);
        let (ex_tx, ex_rx) = mpsc::channel(1);
        let task = tokio::spawn(watch_applied(
            watcher,
            WatchScope::Prefixes(vec!["node.".into(), "edge.".into()]),
            None,
            None,
            Some(store),
            Some(ex_rx),
            BatchConfig::default(),
            parse_put,
            |_b: Vec<Vec<u8>>| {},
            |_| {},
            sd_rx,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let artifact = dir.path().join("artifact");
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: artifact.clone(),
                reply: reply_tx,
            })
            .await
            .unwrap();
        let manifest = reply_rx.await.unwrap().unwrap();
        let want = Some(vec!["node.".to_string(), "edge.".to_string()]);
        assert_eq!(manifest.scope, want);
        assert_eq!(manifest.schema_version, crate::ARTIFACT_SCHEMA_VERSION);
        assert_eq!(
            crate::artifact::read_manifest(&artifact).unwrap().scope,
            want
        );
        sd_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }

    #[async_trait]
    impl RestoreSource<FailOnceStore> for MockRestore {
        async fn latest(&self) -> Result<ExportManifest, SnapshotError> {
            Ok(self.manifest())
        }
        async fn fetch(&self) -> Result<RestoredFold<FailOnceStore>, SnapshotError> {
            let restored = RestoreSource::<AppendLogSnapshot>::fetch(self).await?;
            let manifest = restored.manifest().clone();
            let path = self.dir.path().join(format!(
                "wrapped-{}.snap",
                self.fetches.load(Ordering::SeqCst)
            ));
            drop(restored);
            let (_c, mut inner) = AppendLogSnapshot::open(&path, u64::MAX)?;
            inner.apply(&self.updates, &WatchCursor::from_u64(self.cursor))?;
            Ok(RestoredFold::new(
                manifest,
                FailOnceStore {
                    inner,
                    failed: std::sync::atomic::AtomicBool::new(true),
                },
            ))
        }
    }

    /// A fold at cursor 4 whose FIRST store apply will fail transiently.
    fn fail_once_fold(dir: &tempfile::TempDir) -> (std::path::PathBuf, FailOnceStore) {
        let (path, inner) = expired_fold(dir);
        (
            path,
            FailOnceStore {
                inner,
                failed: std::sync::atomic::AtomicBool::new(false),
            },
        )
    }

    async fn run_fail_once(
        watcher: Arc<ScriptedWatcher>,
        repair: ExpiryRepair<FailOnceStore>,
        store: FailOnceStore,
    ) -> Result<WatchCursor, KvError> {
        let (_sd_tx, sd_rx) = watch::channel(false);
        watch_applied(
            watcher,
            WatchScope::All,
            Some(WatchCursor::from_u64(4)),
            repair,
            Some(store),
            None,
            BatchConfig {
                window: Duration::from_secs(3600),
                ..BatchConfig::default()
            },
            |u: &KvUpdate| Some(u.key().to_string()),
            |_b: Vec<String>| {},
            |_| {},
            sd_rx,
        )
        .await
    }

    /// A repair diffs the STORE, so the store must hold everything delivered
    /// before it. Here the pre-repair flush's store apply fails transiently
    /// and re-queues `node.new@5`; a diff against the store would not see it,
    /// no synthetic delete would be made, and the re-queued put would then
    /// resurrect a key that was deleted during the gap (marker evicted, so the
    /// listing lacks it).
    #[tokio::test]
    async fn relist_diff_sees_updates_requeued_by_a_failed_flush() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = fail_once_fold(&dir);
        let watcher = Arc::new(ScriptedWatcher::new(
            Some(Retention {
                evicts_current_values: false,
                first_revision: 9,
            }),
            vec![(vec![put("node.new", b"recreated-then-deleted", 5)], true)],
        ));
        let live = ["node.keep", "node.changed", "node.gone", "node.a"]
            .map(String::from)
            .to_vec();
        let repair: Option<Arc<dyn KvReader>> = Some(Arc::new(MockReader { live }));
        run_fail_once(watcher, repair.into(), store).await.unwrap();
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert!(
            !snap.entries.contains_key("node.new"),
            "a key gone from the bucket must not be resurrected by a re-queued put"
        );
    }

    /// The restore twin: the artifact (cursor 10) lacks `node.new` (deleted at
    /// 8), so the restore must delete the `node.new@5` the failed flush
    /// re-queued — which it can only do if the diff sees it.
    #[tokio::test]
    async fn restore_diff_sees_updates_requeued_by_a_failed_flush() {
        let dir = tempfile::TempDir::new().unwrap();
        let (path, store) = fail_once_fold(&dir);
        let artifact = MockRestore::new(
            vec![
                put("node.keep", b"v1", 1),
                put("node.changed", b"old", 2),
                put("node.gone", b"x", 3),
                put("node.a", b"a", 4),
            ],
            10,
            Some(&[""]),
        );
        let watcher = Arc::new(ScriptedWatcher::new(
            evicting(9),
            vec![(vec![put("node.new", b"deleted-at-8", 5)], true)],
        ));
        run_fail_once(watcher, ExpiryRepair::Restore(artifact), store)
            .await
            .unwrap();
        let snap = crate::snapshot::load(&path).unwrap().unwrap();
        assert_eq!(snap.cursor.as_u64(), Some(10));
        assert!(
            !snap.entries.contains_key("node.new"),
            "the restored fold must equal the artifact's"
        );
    }
}
