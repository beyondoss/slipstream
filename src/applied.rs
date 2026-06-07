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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::warn;

use crate::kv::{KvError, KvUpdate, KvWatcher, WatchCursor};
use crate::snapshot::SnapshotStore;

/// What to watch: every key, or every key under a prefix.
///
/// Mirrors the [`KvWatcher`] surface — `All` maps to `watch_all` /
/// `watch_all_from`, `Prefix` to `watch_prefix` / `watch_prefix_from`.
#[derive(Debug, Clone)]
pub enum WatchScope {
    /// Watch all keys in the bucket.
    All,
    /// Watch only keys beginning with this prefix.
    Prefix(String),
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
}

impl Default for BatchConfig {
    /// 10 ms / 100 updates — the de-facto default every hand-rolled caller
    /// already used, lifted into one place.
    fn default() -> Self {
        Self {
            window: Duration::from_millis(10),
            max: 100,
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
/// On [`KvError::CursorExpired`] from the `*_from` resume path, this logs and
/// falls back to a full-scope watch (`watch_all` / `watch_prefix`). Callers see
/// the full re-list as a stream of puts, exactly as the hand-rolled loops did.
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
// channel-close) those resets are dead stores — correct, but flagged.
#[allow(unused_assignments)]
pub async fn watch_applied<U, S, P, A, O>(
    watcher: Arc<dyn KvWatcher>,
    scope: WatchScope,
    resume: Option<WatchCursor>,
    mut store: Option<S>,
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

    // Spawn the watch task. It owns the cursor-expired fallback so the main loop
    // only ever sees a clean ordered stream of updates on `rx`.
    let (tx, mut rx) = mpsc::channel::<KvUpdate>(256);
    let handle = {
        let watcher = Arc::clone(&watcher);
        tokio::spawn(async move { run_watch(watcher.as_ref(), &scope, resume, tx).await })
    };

    // Batch state.
    //
    // `batch_high` tracks the version of the most recently *received* update
    // since the last flush — including updates `parse` rejected. NATS delivers
    // in revision order, so the last received is the highest, and advancing the
    // cursor to it after a single atomic `apply` is correct: having seen the max
    // means we've seen everything below it, and a rejected entry is still
    // "nothing to apply", hence covered. Reset to `none()` after every flush.
    let batch_cap = config.max.clamp(1, 64);
    let mut batch: Vec<U> = Vec::with_capacity(batch_cap);
    // Raw received updates for the durable `store`, in revision order. Only
    // populated when a `store` is present; the store folds the *raw* updates
    // (including ones `parse` rejected — they are still part of the bucket's
    // state), whereas the parsed `batch` above is the consumer's domain view.
    let mut raw_batch: Vec<KvUpdate> = Vec::new();
    let mut batch_high = WatchCursor::none();
    // `Some` once a batch has opened and the window timer is armed; `None`
    // between flushes. Only the armed/idle distinction is read in the loop —
    // the absolute instant lives in the pinned `sleep` future below.
    let mut batch_deadline: Option<tokio::time::Instant> = None;

    // Flush the current batch, in order: run the domain `apply` (if non-empty) to
    // completion, advance the cursor, fold the raw batch + cursor durably into
    // `store`, then fire `on_applied`. The store fold runs on a blocking task
    // (its `apply` may block on I/O), moving the store in and taking it back — the
    // same offload the append log's compaction always used. A store error is
    // logged and the watch continues (the snapshot is a cache); a panicked
    // blocking task drops the store irrecoverably, which breaks the
    // resume-after-restart guarantee, so it is surfaced as fatal.
    macro_rules! flush {
        () => {{
            // Nothing received since the last flush → nothing to do at all.
            if !batch.is_empty() || !batch_high.is_none() {
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
                if !batch_high.is_none() {
                    applied = batch_high.clone();
                    if let Some(mut st) = store.take() {
                        let raw = std::mem::take(&mut raw_batch);
                        let cur = applied.clone();
                        // Hand the store back unconditionally on a clean return so
                        // a *failed* apply (Ok(Err)) keeps the watch running; only
                        // a *panicked* task (Err) loses the store and is fatal.
                        match tokio::task::spawn_blocking(move || {
                            let res = st.apply(&raw, &cur);
                            (st, res)
                        })
                        .await
                        {
                            Ok((st, Ok(()))) => store = Some(st),
                            Ok((st, Err(e))) => {
                                warn!(error = %e, "snapshot store apply failed; continuing");
                                store = Some(st);
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

            update = rx.recv() => {
                match update {
                    Some(u) => {
                        // Cursor authority: every received update bumps the
                        // pending high-water, regardless of whether `parse`
                        // keeps it.
                        batch_high = WatchCursor::from_version(u.version().clone());

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
/// a position, with the [`KvError::CursorExpired`] → full-watch fallback.
async fn run_watch(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    resume: Option<WatchCursor>,
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
                        // TODO(v2): signal a "resync" to the caller so it can
                        // diff the full re-list against prior state and emit
                        // synthetic deletes for keys that vanished during the
                        // gap (see Snapshot::stale_keys). For v1 the full
                        // re-list is replayed as a stream of puts, matching the
                        // hand-rolled loops this combinator replaces.
                        warn!("watch cursor expired, falling back to full watch_all");
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
                        // TODO(v2): see the watch_all arm above.
                        warn!("watch cursor expired, falling back to full watch_prefix");
                        watcher.watch_prefix(prefix, tx).await
                    }
                    other => other,
                }
            } else {
                watcher.watch_prefix(prefix, tx).await
            }
        }
    }
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
            BatchConfig {
                window: Duration::from_secs(3600), // effectively never
                max,
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
            None::<AppendLogSnapshot>,
            BatchConfig {
                window: Duration::from_secs(3600), // window won't fire
                max: 100,
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
            None::<AppendLogSnapshot>,
            BatchConfig {
                window: Duration::from_secs(3600),
                max,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            Some(store),
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            None::<AppendLogSnapshot>,
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
            Some(store),
            BatchConfig {
                window: Duration::from_secs(3600),
                max: 1, // one update per flush → a compaction per update
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
}
