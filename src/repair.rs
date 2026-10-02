//! Cursor-expiry repair: what [`watch_applied`](crate::watch_applied) does
//! when its resume cursor, or a live watch's frontier, has fallen out of the
//! log.
//!
//! The fallback full watch re-lists every key the bucket still holds. That
//! re-list can't deliver what retention already dropped, and what retention
//! drops depends on the bucket:
//!
//! - **Never evicts current values** (`discard: new`, no `max_age`): only
//!   superseded history and delete markers go. A key missing from the bucket
//!   was deleted, so the listing is the truth. The repair is the
//!   **key-listing diff** ([`ExpiryRepair::Relist`]): synthetic deletes for
//!   in-scope keys the bucket no longer lists, then the re-list.
//! - **Evicts current values** (`max_age`, per-message TTLs, `discard: old`
//!   under a limit): a key missing from the bucket may just have aged out, and
//!   writes made during the gap may have aged out too. NATS can't tell
//!   "deleted, marker evicted" from "aged out", so no listing diff can be
//!   repaired into a correct one. The repair is an **artifact restore**
//!   ([`ExpiryRepair::Restore`]): replace the in-scope fold with the newest
//!   published artifact — which carries every real delete, because its
//!   exporter folded them — and resume from its cursor.
//!
//! Correct means "every write minus every real delete", not "whatever NATS
//! still lists". On a bucket that evicts current values those differ, and only
//! a fold (here or in an artifact) holds the former.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::applied::{Fold, WatchScope};
use crate::artifact::ExportManifest;
use crate::kv::{KvEntry, KvError, KvReader, KvUpdate, KvWatcher, VersionToken, WatchCursor};
use crate::protocol::{
    KeyRestore, KeyState, RepairMode, RepairPlan, cursorless_start_needs_repair, listing_is_truth,
    plan_repair, restore_ahead, restore_allowed, restore_key, resume_window_ok,
};
use crate::snapshot::{SnapshotError, SnapshotStore};

/// How [`watch_applied`](crate::watch_applied) repairs the fold when its
/// cursor expires — at resume, or mid-watch when the live floor guard finds
/// retention overran the consumer. Both cases take the same path.
///
/// `Option<Arc<dyn KvReader>>` converts into this, so the pre-0.8 `reader`
/// argument keeps compiling: `None` is [`None`](Self::None), `Some(reader)` is
/// [`Relist`](Self::Relist).
pub enum ExpiryRepair<S> {
    /// No repair: fall back to the re-list alone. Keys deleted during the gap
    /// whose delete markers were evicted stay in the fold (a warning names the
    /// hazard), and on a bucket that evicts current values, gap writes that
    /// aged out are missed.
    None,
    /// The key-listing diff: synthetic deletes for in-scope keys the bucket
    /// no longer lists, then the re-list. Sound only when absence from the
    /// bucket means deleted, so it is REFUSED (the watch fails) when the
    /// watcher reports retention that evicts current values. On a backend that
    /// can't report retention, choosing this vouches that it doesn't.
    Relist(Arc<dyn KvReader>),
    /// Replace the in-scope fold with the newest published artifact and resume
    /// from its cursor. Sound for any bucket, as long as the artifact is ahead
    /// of the fold, covers the watch scope, and its cursor is still inside the
    /// log's retention window. When one of those fails there is no safe
    /// recovery, and the watch fails with an error (logged at `error`) rather
    /// than guess. Keep the export interval well under the bucket's `max_age`
    /// (or its `discard: old` turnover).
    ///
    /// Also used at a cursor-less start when the watcher reports that
    /// retention has already evicted current values: the bucket alone can no
    /// longer seed a complete fold.
    ///
    /// The `reader` lists the bucket's live keys at restore time. A restore
    /// never deletes a key that is listed live, nor moves a key to an older
    /// revision: an artifact exported while its exporter was catching up
    /// lacks (or holds older values for) keys whose latest write is after
    /// its cursor, and the resume delivers those — so the consumer never
    /// sees a phantom delete or a regression, even transiently.
    Restore {
        /// Lists live keys, so the restore never deletes a live one.
        reader: Arc<dyn KvReader>,
        /// Supplies the artifact.
        restore: Arc<dyn RestoreSource<S>>,
    },
    /// Decide by the bucket's live retention, read at the moment of expiry:
    /// [`Relist`](Self::Relist) when it never evicts current values,
    /// [`Restore`](Self::Restore) when it does or can't say.
    Auto {
        /// Lists live keys (for either repair).
        reader: Arc<dyn KvReader>,
        /// Supplies the artifact for a restore.
        restore: Arc<dyn RestoreSource<S>>,
    },
}

impl<S> ExpiryRepair<S> {
    /// The repair without its payloads, as the protocol kernels take it.
    pub(crate) fn mode(&self) -> RepairMode {
        match self {
            ExpiryRepair::None => RepairMode::None,
            ExpiryRepair::Relist(_) => RepairMode::Relist,
            ExpiryRepair::Restore { .. } => RepairMode::Restore,
            ExpiryRepair::Auto { .. } => RepairMode::Auto,
        }
    }
}

impl<S> From<Option<Arc<dyn KvReader>>> for ExpiryRepair<S> {
    fn from(reader: Option<Arc<dyn KvReader>>) -> Self {
        match reader {
            Some(reader) => ExpiryRepair::Relist(reader),
            None => ExpiryRepair::None,
        }
    }
}

/// Where an [`ExpiryRepair::Restore`] gets its artifact: the newest one
/// published for this fleet, opened as a temporary fold of the consumer's own
/// backend. [`ArtifactRestore`](crate::ArtifactRestore) (feature `transport`)
/// implements it over an [`ArtifactTransport`](crate::ArtifactTransport).
#[async_trait]
pub trait RestoreSource<S: Send>: Send + Sync {
    /// The newest published artifact's manifest, without downloading the
    /// payload, so a stale, out-of-scope, or not-newer artifact is refused
    /// before its download.
    async fn latest(&self) -> Result<ExportManifest, SnapshotError>;

    /// Download, verify, and open the newest published artifact as a
    /// temporary fold. It may be newer than what [`latest`](Self::latest)
    /// returned; the caller re-checks it.
    async fn fetch(&self) -> Result<RestoredFold<S>, SnapshotError>;
}

/// A verified artifact opened as a temporary, read-only fold, plus anything
/// that must outlive it (its scratch directory). Dropping it drops the fold
/// first, then the guard.
pub struct RestoredFold<S> {
    manifest: ExportManifest,
    // Declaration order is drop order, and it is load-bearing: the fold (an
    // open LSM, possibly) must close before its guard deletes its directory.
    fold: S,
    guard: Option<Box<dyn Any + Send>>,
}

impl<S> RestoredFold<S> {
    /// Wrap a fold opened from the artifact described by `manifest`.
    pub fn new(manifest: ExportManifest, fold: S) -> Self {
        Self {
            manifest,
            fold,
            guard: None,
        }
    }

    /// Keep `guard` (e.g. the `TempDir` holding the fold's files) alive until
    /// after the fold is dropped.
    pub fn holding(mut self, guard: impl Any + Send) -> Self {
        self.guard = Some(Box::new(guard));
        self
    }

    /// The artifact's manifest: its cursor, backend, and scope.
    pub fn manifest(&self) -> &ExportManifest {
        &self.manifest
    }

    /// The artifact's fold.
    pub fn fold(&self) -> &S {
        &self.fold
    }
}

/// Does an artifact exported under the `exporter` scope cover every key in
/// the `watcher` scope? Both are key-prefix lists (`[""]` is every key). A
/// watcher prefix is covered when some exporter prefix is a prefix of it.
/// Sound but conservative: a watcher prefix covered only by a union of
/// narrower exporter prefixes counts as uncovered.
pub(crate) fn scope_covers(exporter: &[String], watcher: &[String]) -> bool {
    watcher
        .iter()
        .all(|w| exporter.iter().any(|e| w.starts_with(e.as_str())))
}

/// Can `artifact` repair a fold at `local` whose cursor expired? Checks scope
/// coverage, then [`restore_allowed`] (ahead + fresh) when the log's first
/// retained revision is known, the ahead half alone when it isn't (the resume
/// that follows re-checks the window). `Err` is the operator-facing reason.
pub(crate) fn check_restore(
    artifact: &ExportManifest,
    local: &WatchCursor,
    first_revision: Option<u64>,
    watch_scope: &[String],
) -> Result<(), String> {
    match &artifact.scope {
        None => {
            return Err(format!(
                "the latest artifact (cursor {:?}) records no key scope — it was exported \
                 outside watch_applied or by a pre-scope build — so it can't be shown to cover \
                 watch scope {watch_scope:?}; publish a new export round",
                artifact.cursor
            ));
        }
        Some(exporter) if !scope_covers(exporter, watch_scope) => {
            return Err(format!(
                "the latest artifact covers scope {exporter:?}, which does not cover watch \
                 scope {watch_scope:?}"
            ));
        }
        Some(_) => {}
    }
    let (a, l) = (artifact.cursor.rank(), local.rank());
    if !restore_ahead(a, l) {
        return Err(format!(
            "the latest artifact (cursor {a}) is not ahead of the local fold (cursor {l}): \
             nothing newer to restore from; the export pipeline is behind this node"
        ));
    }
    if let Some(first) = first_revision
        && !restore_allowed(a, l, first)
    {
        debug_assert!(!resume_window_ok(a, first));
        return Err(format!(
            "the latest artifact (cursor {a}) is outside the log's retention window (first \
             retained revision {first}): what was evicted between them exists nowhere, so there \
             is no safe recovery. Exports must run well inside the bucket's max_age / discard \
             turnover"
        ));
    }
    Ok(())
}

/// One key of a restore: take the artifact's entry, or delete the key.
pub(crate) enum RestoreOp {
    Put(String),
    Delete(String),
}

impl RestoreOp {
    fn key(&self) -> &str {
        match self {
            RestoreOp::Put(k) | RestoreOp::Delete(k) => k,
        }
    }
}

/// The in-scope ops that bring the `local` fold to the `artifact` fold's
/// state at its cursor `C`, without ever moving a key backward or deleting a
/// live one. Keys only, in key order — values are read back in bounded
/// chunks ([`materialize`]), so a restore never holds the whole changed set's
/// values at once (the same discipline as the key-listing diff).
///
/// Each in-scope key is decided by [`restore_key`], which the repair model
/// (`tests/model_repair.rs`) runs too. Out-of-scope keys are untouched on
/// both sides.
pub(crate) fn restore_diff<S: SnapshotStore>(
    local: &S,
    artifact: &S,
    prefixes: &[String],
    live: &HashSet<String>,
) -> Result<Vec<RestoreOp>, SnapshotError> {
    let mut ops = Vec::new();
    for prefix in prefixes {
        // Every key the artifact holds.
        artifact.for_each_in_range(prefix, |entry| {
            let at = KeyState::At(entry.version.as_u64());
            let (state, identical) = match local.get(&entry.key)? {
                Some(l) => (
                    KeyState::At(l.version.as_u64()),
                    l.version == entry.version && l.value == entry.value,
                ),
                None => (KeyState::Absent, false),
            };
            let listed = live.contains(&entry.key);
            if restore_key(state, at, identical, listed) == KeyRestore::TakeArtifact {
                ops.push(RestoreOp::Put(entry.key));
            }
            Ok(())
        })?;
        // Local keys the artifact lacks. The kernel is asked first, as if the
        // artifact lacked the key, so the artifact is read only for keys that
        // would be deleted.
        local.for_each_in_range(prefix, |entry| {
            let state = KeyState::At(entry.version.as_u64());
            let listed = live.contains(&entry.key);
            if restore_key(state, KeyState::Absent, false, listed) == KeyRestore::Delete
                && artifact.get(&entry.key)?.is_none()
            {
                ops.push(RestoreOp::Delete(entry.key));
            }
            Ok(())
        })?;
    }
    // Overlapping prefixes visit a key more than once; a key is a Put or a
    // Delete, never both (Put iff the artifact has it).
    ops.sort_unstable_by(|a, b| a.key().cmp(b.key()));
    ops.dedup_by(|a, b| a.key() == b.key());
    Ok(ops)
}

/// Read one chunk of a restore's values back from the artifact fold. Deletes
/// carry the unknown version: like the key-listing diff's synthetic deletes,
/// they are a state correction, not a log entry, and never move a cursor.
pub(crate) fn materialize<S: SnapshotStore>(
    artifact: &S,
    ops: Vec<RestoreOp>,
) -> Result<Vec<KvUpdate>, SnapshotError> {
    ops.into_iter()
        .map(|op| match op {
            RestoreOp::Put(key) => match artifact.get(&key)? {
                Some(entry @ KvEntry { .. }) => Ok(KvUpdate::Put(entry)),
                None => Err(SnapshotError::Backend(format!(
                    "restored artifact fold lost key {key:?} mid-restore"
                ))),
            },
            RestoreOp::Delete(key) => Ok(KvUpdate::Delete {
                key,
                version: VersionToken::unknown(),
            }),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The repair flow. Two tasks share it: the watch task, which owns the watch
// and decides on a repair when the cursor expires, and `watch_applied`'s main
// loop, which owns the fold and applies the repair to it. The watch task
// hands the repair over (`RepairRequest`) and stays parked on the ack/reply
// until the fold has it, so the repair is strictly ordered between everything
// delivered before the expiry and everything the next watch delivers.
// ---------------------------------------------------------------------------

/// A cursor-expired repair handoff from the watch task to the main loop.
pub(crate) enum RepairRequest<S> {
    /// The key-listing diff: the bucket's live keys for the watch scope. The
    /// main loop applies synthetic deletes for in-scope keys missing from it,
    /// then acks so the watch task can start the fallback re-list.
    Relist {
        live_keys: Vec<String>,
        ack: oneshot::Sender<()>,
    },
    /// The artifact restore: a verified artifact fold, and the bucket's live
    /// keys for the scope (listed after the fetch). The main loop folds the
    /// in-scope difference, advances to the artifact's cursor, and replies
    /// with it so the watch task resumes from there.
    Restore {
        restored: RestoredFold<S>,
        live: HashSet<String>,
        reply: oneshot::Sender<Result<WatchCursor, KvError>>,
    },
}

/// What the watch task needs to repair an expiry: the caller's chosen
/// repair, and the channel into the main loop that owns the fold.
pub(crate) struct RepairHandle<S> {
    mode: ExpiryRepair<S>,
    tx: mpsc::Sender<RepairRequest<S>>,
}

/// The repair the watch task settled on for one expiry.
enum Plan<'a, S> {
    /// Fall back to the re-list alone (nothing armed).
    ReListOnly,
    /// The key-listing diff, then the re-list.
    Relist(&'a Arc<dyn KvReader>, &'a mpsc::Sender<RepairRequest<S>>),
    /// The artifact restore, then a resume from the artifact's cursor.
    Restore(
        &'a Arc<dyn KvReader>,
        &'a Arc<dyn RestoreSource<S>>,
        &'a mpsc::Sender<RepairRequest<S>>,
    ),
}

/// Arm `mode` for a watch: the watch task's handle and the main loop's
/// receiving end, both `None` when there is nothing to run (no repair, or no
/// store to run it against — both repairs edit the fold).
///
/// A restore replaces the fold, so asking for one without a fold is a
/// contradiction: on a bounded log such a consumer would silently hold
/// NATS's retained view (missing everything evicted) while believing it
/// repairs. That is refused up front; `ExpiryRepair::None` is the explicit
/// way to accept the retained view.
#[allow(clippy::type_complexity)]
pub(crate) fn arm<S>(
    mode: ExpiryRepair<S>,
    has_store: bool,
) -> Result<
    (
        Option<RepairHandle<S>>,
        Option<mpsc::Receiver<RepairRequest<S>>>,
    ),
    KvError,
> {
    match mode.mode() {
        RepairMode::None => Ok((None, None)),
        _ if has_store => {
            let (tx, rx) = mpsc::channel(1);
            Ok((Some(RepairHandle { mode, tx }), Some(rx)))
        }
        RepairMode::Restore | RepairMode::Auto => Err(KvError::WatchError(
            "ExpiryRepair::Restore / ::Auto repair the fold, so they need a store: a consumer \
             without one cannot repair a bounded log. Pass a SnapshotStore, or \
             ExpiryRepair::None to accept NATS's retained view"
                .into(),
        )),
        RepairMode::Relist => Ok((None, None)),
    }
}

/// [`listing_is_truth`] for the watcher's live retention; `None` when the
/// backend can't say.
async fn listing_truth(watcher: &dyn KvWatcher) -> Result<Option<bool>, KvError> {
    Ok(watcher
        .retention()
        .await?
        .map(|r| listing_is_truth(r.evicts_current_values, r.first_revision)))
}

/// The watch task: run the underlying watch for `scope`, resuming from
/// `resume` when it carries a position, with cursor-expiry repair: an expiry
/// (at resume, or mid-watch from the floor guard) runs the planned repair and
/// then either falls back to the full-scope re-list (Relist / nothing armed)
/// or resumes from the restored artifact's cursor (Restore) — which can
/// itself expire later and go round again, each time only for a strictly
/// newer artifact.
///
/// `unanchored`: the fold holds data but no cursor (a torn first checkpoint,
/// or a populated store started without one). A re-list alone never removes
/// what it doesn't deliver, so that start is repaired like an expiry at
/// revision 0 before watching.
pub(crate) async fn run_watch<S: Send + 'static>(
    watcher: Arc<dyn KvWatcher>,
    scope: WatchScope,
    resume: Option<WatchCursor>,
    unanchored: bool,
    repair: Option<RepairHandle<S>>,
    tx: mpsc::Sender<KvUpdate>,
) -> Result<(), KvError> {
    let (watcher, scope) = (watcher.as_ref(), &scope);
    // Resume only when the cursor carries a real position; an absent or `none()`
    // cursor falls through to a full watch. Binding `cursor` here makes "we have a
    // resume position" structural — there is no separate bool whose truth a later
    // edit could let drift from the `Some`.
    let mut resume = resume.filter(|c| !c.is_none());

    // A cursor-less start that must be repaired before it watches
    // ([`cursorless_start_needs_repair`]). Retention is read only when the
    // decision, or the warning, depends on it.
    let mode = repair.as_ref().map_or(RepairMode::None, |h| h.mode.mode());
    let mut repair_first = false;
    if resume.is_none() {
        let truth = match mode {
            RepairMode::Relist => None,
            RepairMode::Restore | RepairMode::Auto if unanchored => None,
            _ => listing_truth(watcher).await?,
        };
        repair_first = cursorless_start_needs_repair(mode, unanchored, truth);
        if repair_first && unanchored {
            warn!(
                "the fold holds data but no cursor; repairing it like an expired cursor \
                 before watching (a re-list alone never removes what it doesn't deliver)"
            );
        } else if repair_first {
            warn!(
                "no resume cursor, and the bucket has already evicted current values: a \
                 re-list would seed an incomplete fold; restoring from the latest artifact"
            );
        } else if mode == RepairMode::None && truth == Some(false) {
            // Nothing armed: the re-list stands as the fold, and lacks
            // whatever aged out. Nothing will repair it.
            warn!(
                "no resume cursor and no repair armed, on a bucket that has already \
                 evicted current values: the re-list lacks whatever aged out. Wire a \
                 store and ExpiryRepair::Auto to seed from an artifact"
            );
        }
    }

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
        match plan(watcher, repair.as_ref()).await? {
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
            Plan::Restore(reader, source, repairs) => {
                warn!("watch cursor expired; restoring the fold from the latest artifact");
                let restored =
                    restore_from_artifact(watcher, scope, reader, source, repairs, &cursor)
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

/// Decide how to repair one expiry ([`plan_repair`]), reading the bucket's
/// retention live when the choice depends on it.
async fn plan<'a, S>(
    watcher: &dyn KvWatcher,
    repair: Option<&'a RepairHandle<S>>,
) -> Result<Plan<'a, S>, KvError> {
    let Some(h) = repair else {
        return Ok(Plan::ReListOnly);
    };
    let mode = h.mode.mode();
    let truth = match mode {
        RepairMode::Relist | RepairMode::Auto => listing_truth(watcher).await?,
        RepairMode::None | RepairMode::Restore => None,
    };
    Ok(match (plan_repair(mode, truth), &h.mode) {
        (RepairPlan::ReListOnly, _) => Plan::ReListOnly,
        (RepairPlan::RefuseRelist, _) => {
            let msg = "watch cursor expired on a bucket whose retention evicts current \
                       values (max_age, per-message TTL, or discard:old): its key listing \
                       can't tell a deleted key from an aged-out one, so the key-listing \
                       resync would delete valid keys. Refusing. Repair from artifacts with \
                       ExpiryRepair::Restore or ExpiryRepair::Auto (or accept a re-list-only \
                       fallback with ExpiryRepair::None)";
            error!(msg);
            return Err(KvError::WatchError(msg.into()));
        }
        (RepairPlan::Relist, ExpiryRepair::Relist(reader) | ExpiryRepair::Auto { reader, .. }) => {
            Plan::Relist(reader, &h.tx)
        }
        (
            RepairPlan::Restore,
            ExpiryRepair::Restore { reader, restore } | ExpiryRepair::Auto { reader, restore },
        ) => Plan::Restore(reader, restore, &h.tx),
        (plan, _) => unreachable!("plan_repair chose {plan:?}, which mode {mode:?} can't run"),
    })
}

/// The artifact restore, watch-task half: check the newest artifact before
/// downloading it (ahead of `local`, covers the scope, still inside
/// retention), download it, check what actually arrived, then hand it to the
/// main loop and wait for the cursor it restored to.
async fn restore_from_artifact<S: Send + 'static>(
    watcher: &dyn KvWatcher,
    scope: &WatchScope,
    reader: &Arc<dyn KvReader>,
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

    // The bucket's live keys, listed after the artifact's cursor is fixed:
    // the restore never deletes one of these (see `restore_diff`). A failed
    // listing fails the watch, like the key-listing repair's.
    let mut live = HashSet::new();
    for prefix in &prefixes {
        let keys = reader
            .keys(prefix)
            .await
            .map_err(|e| fail(format!("listing live keys under {prefix:?} failed: {e}")))?;
        live.extend(keys);
    }

    let (reply_tx, reply_rx) = oneshot::channel();
    repairs
        .send(RepairRequest::Restore {
            restored,
            live,
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
/// Sound only on a bucket that never evicts current values; `plan` refuses it
/// otherwise.
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

/// Fail the watch from inside a repair: the fold is either unchanged or
/// part-way through a repair that commits under its old (expired) cursor, so
/// the caller's restart re-runs the whole expiry path.
fn repair_fatal(msg: String) -> KvError {
    error!(%msg, "cursor-expiry repair failed; aborting watch");
    KvError::WatchError(msg)
}

/// The main-loop half: apply one repair to the fold. `backlog` is the watch
/// task's update channel and `prefixes` the watch scope. An `Err` is fatal to
/// the watch.
pub(crate) async fn fold_in<U, S, P, A, O>(
    fold: &mut Fold<U, S, P, A, O>,
    req: RepairRequest<S>,
    backlog: &mut mpsc::Receiver<KvUpdate>,
    prefixes: &[String],
) -> Result<(), KvError>
where
    U: Send,
    S: SnapshotStore + Send + 'static,
    P: FnMut(&KvUpdate) -> Option<U> + Send,
    A: FnMut(Vec<U>) + Send,
    O: FnMut(WatchCursor) + Send,
{
    // Fold everything the watch task delivered before it asked for this
    // repair. A floor-guard trip ends a live watch mid-stream with updates
    // still buffered in the channel: the repair must see them (it diffs the
    // fold) and supersede them (one applied after the repair would resurrect
    // pre-repair state, and could even move the cursor backward). The watch
    // task is parked on the ack/reply, so this drains exactly that backlog.
    while let Ok(u) = backlog.try_recv() {
        if fold.ingest(u) {
            fold.flush().await?;
        }
    }
    // Both repairs diff the STORE, so it must hold everything delivered. A
    // transient store failure re-queues the batch, which the diff can't see:
    // it would miss a re-queued put that a deletion during the gap has since
    // removed, and the put would then commit and resurrect the key.
    fold.settle().await?;
    match req {
        RepairRequest::Relist { live_keys, ack } => {
            relist(fold, &live_keys, prefixes).await?;
            // Ack AFTER the deletes are applied: the watch task is holding
            // the fallback watch until it hears back, which is what orders
            // deletes before the re-list (tests/model_resync_order.rs proves
            // the barrier load-bearing). If the flush's STORE apply failed
            // transiently, the deletes sit re-queued at the FRONT of the raw
            // batch — still strictly before any re-list put in the eventual
            // cumulative commit, and the domain apply saw them before this ack
            // either way.
            let _ = ack.send(());
            Ok(())
        }
        RepairRequest::Restore {
            restored,
            live,
            reply,
        } => {
            let target = restored.manifest().cursor.clone();
            // The authoritative ahead check (the watch task's ran against its
            // resume cursor; a mid-watch expiry delivered past that). A
            // restore never moves the fold backward.
            if !restore_ahead(target.rank(), fold.applied().rank()) {
                let msg = format!(
                    "cursor-expired restore: the latest artifact (cursor {target:?}) is not \
                     ahead of the fold (cursor {:?}); nothing newer to restore from",
                    fold.applied()
                );
                error!(%msg, "cursor-expiry repair refused");
                // The watch task fails with this; its channel closing ends the
                // loop.
                let _ = reply.send(Err(KvError::WatchError(msg)));
                return Ok(());
            }
            restore(fold, restored, live, prefixes).await?;
            let _ = reply.send(Ok(target));
            Ok(())
        }
    }
}

/// The key-listing diff, main-loop half: synthetic deletes for in-scope keys
/// the fold holds and the bucket no longer lists — they vanished during the
/// gap (their delete markers evicted with the cursor), and the re-list can't
/// deliver a delete.
async fn relist<U, S, P, A, O>(
    fold: &mut Fold<U, S, P, A, O>,
    live_keys: &[String],
    prefixes: &[String],
) -> Result<(), KvError>
where
    U: Send,
    S: SnapshotStore + Send + 'static,
    P: FnMut(&KvUpdate) -> Option<U> + Send,
    A: FnMut(Vec<U>) + Send,
    O: FnMut(WatchCursor) + Send,
{
    let live: HashSet<&str> = live_keys.iter().map(String::as_str).collect();
    let mut stale: Vec<String> = Vec::new();
    if let Some(st) = fold.store() {
        for prefix in prefixes {
            // Stream the fold's keys rather than `range()`, which buffers every
            // in-scope entry — values included — into one Vec. On an on-disk
            // backend holding a fold larger than RAM (the case those backends
            // exist for), an All-scope resync would materialize the entire
            // fold on the repair path. Only the keys matter.
            if let Err(e) = st.for_each_in_range(prefix, |entry| {
                if !live.contains(entry.key.as_str()) {
                    stale.push(entry.key);
                }
                Ok(())
            }) {
                // FATAL, not a degrade: an incomplete diff silently leaves
                // deleted keys in the fold forever (tests/model.rs proves the
                // divergence reachable under degrade semantics). Fail the
                // watch; the restart re-runs the resume → expiry → resync from
                // scratch.
                warn!(error = %e, prefix = %prefix,
                    "resync fold scan failed; aborting watch rather than diverging");
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
        warn!(
            stale = stale.len(),
            "cursor-expired resync: deleting keys that vanished during the gap"
        );
    }
    for key in stale {
        // Synthetic: carries no revision (unknown version) and so never
        // advances the cursor.
        fold.correct(KvUpdate::Delete {
            key,
            version: VersionToken::unknown(),
        });
    }
    fold.flush().await
}

/// The artifact restore, main-loop half: replace the in-scope fold with the
/// artifact's ([`restore_diff`]), then take its cursor.
async fn restore<U, S, P, A, O>(
    fold: &mut Fold<U, S, P, A, O>,
    mut restored: RestoredFold<S>,
    live: HashSet<String>,
    prefixes: &[String],
) -> Result<(), KvError>
where
    U: Send,
    S: SnapshotStore + Send + 'static,
    P: FnMut(&KvUpdate) -> Option<U> + Send,
    A: FnMut(Vec<U>) + Send,
    O: FnMut(WatchCursor) + Send,
{
    let target = restored.manifest().cursor.clone();
    let Some(st) = fold.take_store() else {
        unreachable!("repairs are armed only with a store")
    };
    let (prefixes, max) = (prefixes.to_vec(), fold.batch_cap());
    let (st, r, diff) = tokio::task::spawn_blocking(move || {
        let diff = restore_diff(&st, restored.fold(), &prefixes, &live);
        (st, restored, diff)
    })
    .await
    .map_err(|e| repair_fatal(format!("cursor-expired restore: diff task panicked: {e}")))?;
    fold.put_store(st);
    restored = r;
    let diff = diff.map_err(|e| {
        repair_fatal(format!(
            "cursor-expired restore: diffing the fold against the artifact failed: {e}"
        ))
    })?;
    info!(
        from = ?fold.applied(),
        to = ?target,
        changed = diff.len(),
        "cursor-expired restore: replacing the in-scope fold with the artifact's"
    );
    // Fold the difference through parse/apply/store in `max`-sized chunks,
    // values read back per chunk. These carry no stream position: every chunk
    // commits under the old cursor, so a crash part-way re-runs the
    // (idempotent) restore on restart.
    let mut ops = diff.into_iter();
    loop {
        let chunk: Vec<_> = ops.by_ref().take(max).collect();
        if chunk.is_empty() {
            break;
        }
        let (r, updates) = tokio::task::spawn_blocking(move || {
            let updates = materialize(restored.fold(), chunk);
            (restored, updates)
        })
        .await
        .map_err(|e| repair_fatal(format!("cursor-expired restore: read task panicked: {e}")))?;
        restored = r;
        let updates = updates.map_err(|e| {
            repair_fatal(format!(
                "cursor-expired restore: reading the artifact failed: {e}"
            ))
        })?;
        for u in updates {
            fold.correct(u);
        }
        fold.flush().await?;
    }
    // Now the fold IS the artifact's (in scope): take its cursor, committed to
    // the store with the final chunk's re-queued remainder, if any.
    fold.advance_to(target);
    fold.flush().await?;
    // Closing an LSM can block; drop the temporary fold and its scratch dir
    // off the async thread.
    let _ = tokio::task::spawn_blocking(move || drop(restored)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::AppendLogSnapshot;

    fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
        KvUpdate::Put(KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            version: VersionToken::from_u64(rev),
        })
    }

    fn fold(
        dir: &tempfile::TempDir,
        name: &str,
        updates: &[KvUpdate],
        at: u64,
    ) -> AppendLogSnapshot {
        let (_c, mut s) = AppendLogSnapshot::open(&dir.path().join(name), u64::MAX).unwrap();
        s.apply(updates, &WatchCursor::from_u64(at)).unwrap();
        s
    }

    fn manifest(cursor: u64, scope: Option<&[&str]>) -> ExportManifest {
        ExportManifest {
            schema_version: crate::artifact::ARTIFACT_SCHEMA_VERSION,
            backend: "append-log".into(),
            backend_version: "2".into(),
            cursor: WatchCursor::from_u64(cursor),
            created_at_unix: 0,
            files: vec![],
            scope: scope.map(|s| s.iter().map(|p| p.to_string()).collect()),
        }
    }

    #[test]
    fn scope_coverage() {
        let s = |v: &[&str]| v.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        assert!(
            scope_covers(&s(&[""]), &s(&["node.", "edge."])),
            "All covers anything"
        );
        assert!(
            scope_covers(&s(&["node."]), &s(&["node.us."])),
            "wider prefix covers narrower"
        );
        assert!(
            !scope_covers(&s(&["node.us."]), &s(&["node."])),
            "narrower never covers wider"
        );
        assert!(
            !scope_covers(&s(&["node."]), &s(&[""])),
            "a prefix never covers All"
        );
        assert!(
            !scope_covers(&s(&["node."]), &s(&["node.", "edge."])),
            "every watcher prefix must be covered"
        );
        assert!(
            scope_covers(&s(&["a."]), &s(&[])),
            "an empty watch scope is vacuously covered"
        );
    }

    #[test]
    fn check_restore_refusals() {
        let all = vec![String::new()];
        let local = WatchCursor::from_u64(3);
        // Ahead + fresh + covering: accepted.
        check_restore(&manifest(9, Some(&[""])), &local, Some(8), &all).unwrap();
        // Unknown retention: ahead alone suffices here (the resume re-checks).
        check_restore(&manifest(9, Some(&[""])), &local, None, &all).unwrap();

        let err = check_restore(&manifest(9, None), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("no key scope"), "{err}");
        let err = check_restore(&manifest(9, Some(&["node."])), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("does not cover"), "{err}");
        let err = check_restore(&manifest(3, Some(&[""])), &local, None, &all).unwrap_err();
        assert!(err.contains("not ahead"), "{err}");
        let err = check_restore(&manifest(6, Some(&[""])), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("outside the log's retention window"), "{err}");
    }

    /// The diff turns the local in-scope fold into the artifact's: changed and
    /// new keys are Puts, keys the artifact lacks are Deletes, identical
    /// entries and out-of-scope keys are left alone.
    #[test]
    fn diff_is_wholesale_replacement_within_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = fold(
            &dir,
            "local",
            &[
                put("node.same", b"1", 1),
                put("node.changed", b"old", 2),
                put("node.gone", b"x", 3),
                put("other.local", b"keep", 4),
            ],
            4,
        );
        let artifact = fold(
            &dir,
            "artifact",
            &[
                put("node.same", b"1", 1),
                put("node.changed", b"new", 7),
                put("node.new", b"gap-write", 8),
                put("other.remote", b"ignored", 9),
            ],
            9,
        );
        let ops = restore_diff(
            &local,
            &artifact,
            &["node.".to_string()],
            &Default::default(),
        )
        .unwrap();
        let got: Vec<(String, bool)> = ops
            .iter()
            .map(|o| (o.key().to_string(), matches!(o, RestoreOp::Put(_))))
            .collect();
        assert_eq!(
            got,
            vec![
                ("node.changed".into(), true),
                ("node.gone".into(), false),
                ("node.new".into(), true),
            ]
        );

        let updates = materialize(&artifact, ops).unwrap();
        match &updates[0] {
            KvUpdate::Put(e) => {
                assert_eq!(e.value, b"new");
                assert_eq!(
                    e.version.as_u64(),
                    Some(7),
                    "restored entries keep their revision"
                );
            }
            other => panic!("expected put, got {other:?}"),
        }
        assert!(
            matches!(&updates[1], KvUpdate::Delete { version, .. } if version.is_unknown()),
            "restore deletes never carry a revision"
        );
    }

    #[test]
    fn diff_dedups_overlapping_prefixes() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = fold(&dir, "local", &[put("node.a", b"1", 1)], 1);
        let artifact = fold(&dir, "artifact", &[put("node.b", b"2", 2)], 2);
        let ops = restore_diff(
            &local,
            &artifact,
            &["node.".into(), "node".into()],
            &Default::default(),
        )
        .unwrap();
        assert_eq!(ops.len(), 2, "each key once despite two covering prefixes");
    }

    // --- Exhaustive small-domain checks of the real functions ---------------

    /// Every string over `alphabet` of length 0..=max_len.
    fn strings(alphabet: &[char], max_len: usize) -> Vec<String> {
        let mut out = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..max_len {
            let mut next = Vec::new();
            for s in &frontier {
                for c in alphabet {
                    let mut t = s.clone();
                    t.push(*c);
                    next.push(t);
                }
            }
            out.extend(next.iter().cloned());
            frontier = next;
        }
        out
    }

    /// Every subset of `items` with at most `max` elements.
    fn subsets(items: &[String], max: usize) -> Vec<Vec<String>> {
        let mut out: Vec<Vec<String>> = vec![vec![]];
        for item in items {
            let grown: Vec<Vec<String>> = out
                .iter()
                .filter(|s| s.len() < max)
                .map(|s| {
                    let mut t = s.clone();
                    t.push(item.clone());
                    t
                })
                .collect();
            out.extend(grown);
        }
        out
    }

    /// SOUNDNESS of `scope_covers`, exhaustively over every pair of prefix
    /// sets (≤ 2 prefixes of length ≤ 2 over {a, b, .}) and every key of
    /// length ≤ 3: whenever it says the exporter covers the watcher, every
    /// key the watcher can see is one the exporter folded. (It is
    /// deliberately not complete — a union of narrower exporter prefixes is
    /// refused — which only ever refuses a restore, never accepts a bad one.)
    #[test]
    fn exhaustive_scope_covers_is_sound() {
        let alphabet = ['a', 'b', '.'];
        let prefixes = strings(&alphabet, 2);
        let keys = strings(&alphabet, 3);
        let sets = subsets(&prefixes, 2);
        let matches =
            |scope: &[String], key: &str| scope.iter().any(|p| key.starts_with(p.as_str()));
        let mut checked = 0usize;
        for exporter in &sets {
            for watcher in &sets {
                if !scope_covers(exporter, watcher) {
                    continue;
                }
                for key in &keys {
                    assert!(
                        !matches(watcher, key) || matches(exporter, key),
                        "covers({exporter:?}, {watcher:?}) but {key:?} is visible to the \
                         watcher and not in the export"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 10_000, "the domain is not vacuous ({checked})");
    }

    /// `check_restore` against its specification, exhaustively over cursors
    /// 0..=6, first retained revisions (unknown, 0..=8), and three scope
    /// shapes: accept iff the artifact has a scope covering the watcher's, is
    /// strictly ahead, and (when retention is known) resumes gap-free.
    #[test]
    fn exhaustive_check_restore_matches_spec() {
        let all = vec![String::new()];
        let node = vec!["node.".to_string()];
        let scopes: [Option<&[&str]>; 3] = [None, Some(&[""]), Some(&["edge."])];
        for watcher in [&all, &node] {
            for scope in scopes {
                for a in 0..=6u64 {
                    for l in 0..=6u64 {
                        for first in std::iter::once(None).chain((0..=8u64).map(Some)) {
                            let got = check_restore(
                                &manifest(a, scope),
                                &WatchCursor::from_u64(l),
                                first,
                                watcher,
                            )
                            .is_ok();
                            let covered = scope.is_some_and(|s| {
                                let s: Vec<String> = s.iter().map(|p| p.to_string()).collect();
                                watcher
                                    .iter()
                                    .all(|w| s.iter().any(|e| w.starts_with(e.as_str())))
                            });
                            let want = covered && a > l && first.is_none_or(|f| f <= a + 1);
                            assert_eq!(
                                got, want,
                                "artifact {a} scope {scope:?}, local {l}, first {first:?}, \
                                 watcher {watcher:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// One key's state in the exhaustive restore check: absent, or a value
    /// at a revision.
    type KeyState = Option<(&'static [u8], u64)>;
    const STATES: [KeyState; 3] = [None, Some((b"v1", 1)), Some((b"v2", 2))];
    const KEYS: [&str; 3] = ["a.1", "a.2", "b.1"];

    fn fold_of(
        dir: &tempfile::TempDir,
        name: &str,
        states: &[KeyState],
        cursor: u64,
    ) -> AppendLogSnapshot {
        let updates: Vec<KvUpdate> = KEYS
            .iter()
            .zip(states)
            .filter_map(|(k, st)| st.map(|(v, rev)| put(k, v, rev)))
            .collect();
        fold(dir, name, &updates, cursor)
    }

    /// THE RESTORE, exhaustively over every pair of 3-key folds (each key
    /// absent or at one of two revisions: 729 pairs), four scopes including
    /// overlapping prefixes, and every live-key listing (8), using the real
    /// `restore_diff` and `materialize` and a real store. Per in-scope key,
    /// the restored fold holds:
    ///
    /// - the artifact's entry, unless the local one is NEWER (or the same) —
    ///   never a move backward;
    /// - nothing where the artifact has nothing, unless the key is listed
    ///   live — never a phantom delete;
    ///
    /// and out-of-scope keys are untouched. A crash after any prefix of the
    /// ops (committed under the old cursor, as `watch_applied` does) followed
    /// by a fresh diff converges to the same result — the restore is
    /// idempotent, which is what makes re-running it on restart safe — and a
    /// converged fold diffs empty.
    #[test]
    fn exhaustive_restore_diff_never_regresses_never_drops_live_and_survives_partial_application() {
        let scopes: [&[&str]; 4] = [&[""], &["a."], &["b."], &["a.", "a.1"]];
        let in_scope = |scope: &[&str], key: &str| scope.iter().any(|p| key.starts_with(p));
        let lives: Vec<std::collections::HashSet<String>> = (0..8u8)
            .map(|mask| {
                KEYS.iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1 << i) != 0)
                    .map(|(_, k)| k.to_string())
                    .collect()
            })
            .collect();
        let mut cases = 0usize;
        for local_states in itertools_product(&STATES) {
            for artifact_states in itertools_product(&STATES) {
                let dir = tempfile::TempDir::new().unwrap();
                let artifact = fold_of(&dir, "artifact", &artifact_states, 9);
                for scope in scopes {
                    let prefixes: Vec<String> = scope.iter().map(|p| p.to_string()).collect();
                    for (li, live) in lives.iter().enumerate() {
                        let full = restore_diff(
                            &fold_of(
                                &dir,
                                &format!("probe-{li}-{}", scope.join("|")),
                                &local_states,
                                3,
                            ),
                            &artifact,
                            &prefixes,
                            live,
                        )
                        .unwrap();
                        // Crash after `cut` ops: apply them, then re-diff.
                        for cut in 0..=full.len() {
                            let name = format!("local-{li}-{cut}-{}", scope.join("|"));
                            let mut local = fold_of(&dir, &name, &local_states, 3);
                            let first = restore_diff(&local, &artifact, &prefixes, live).unwrap();
                            let partial: Vec<RestoreOp> = first.into_iter().take(cut).collect();
                            let updates = materialize(&artifact, partial).unwrap();
                            local.apply(&updates, &WatchCursor::from_u64(3)).unwrap();
                            let rest = restore_diff(&local, &artifact, &prefixes, live).unwrap();
                            let updates = materialize(&artifact, rest).unwrap();
                            local.apply(&updates, &WatchCursor::from_u64(9)).unwrap();

                            for (i, key) in KEYS.iter().enumerate() {
                                let (l, a) = (local_states[i], artifact_states[i]);
                                let want = if !in_scope(scope, key) {
                                    l
                                } else {
                                    match (l, a) {
                                        (Some(l), Some(a)) if l.1 >= a.1 => Some(l),
                                        (_, Some(a)) => Some(a),
                                        (Some(l), None) if live.contains(*key) => Some(l),
                                        (_, None) => None,
                                    }
                                };
                                let got = local
                                    .get(key)
                                    .unwrap()
                                    .map(|e| (e.value, e.version.as_u64()));
                                assert_eq!(
                                    got,
                                    want.map(|(v, r)| (v.to_vec(), Some(r))),
                                    "key {key} scope {scope:?} live {live:?} local {local_states:?} \
                                     artifact {artifact_states:?} cut {cut}"
                                );
                            }
                            assert!(
                                restore_diff(&local, &artifact, &prefixes, live)
                                    .unwrap()
                                    .is_empty(),
                                "a converged fold must diff empty"
                            );
                            cases += 1;
                        }
                    }
                }
            }
        }
        assert!(cases > 20_000, "the domain is not vacuous ({cases})");
    }

    /// Every assignment of `states` to the three keys.
    fn itertools_product(states: &[KeyState; 3]) -> Vec<[KeyState; 3]> {
        let mut out = Vec::new();
        for a in states {
            for b in states {
                for c in states {
                    out.push([*a, *b, *c]);
                }
            }
        }
        out
    }
}
