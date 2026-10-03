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
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::applied::{Fold, WatchScope};
use crate::artifact::ExportManifest;
use crate::kv::{KvError, KvReader, KvUpdate, KvWatcher, Retention, VersionToken, WatchCursor};
use crate::protocol::{
    KeyRestore, KeyState, RepairMode, RepairPlan, cursorless_start_needs_repair, listing_truth,
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
    /// [`Restore`](Self::Restore) when it does or can't say — and always
    /// Restore once the fold has seen it evict, even after eviction is
    /// turned off (a key that aged out stays missing from the listing).
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

    /// A directory on the fold's filesystem for a repair's scratch files.
    /// [`ExpiryRepair::Auto`]'s key-listing diff sorts the bucket's listing
    /// there; without one (and always under [`ExpiryRepair::Relist`]) it uses
    /// the system temp dir (`TMPDIR`), which may be RAM-backed.
    fn scratch_dir(&self) -> Option<PathBuf> {
        None
    }
}

/// A verified artifact opened as a temporary, read-only fold, plus anything
/// that must outlive it (its scratch directory). Dropping it drops the fold
/// first, then the guard.
pub struct RestoredFold<S> {
    manifest: ExportManifest,
    scratch: Option<PathBuf>,
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
            scratch: None,
            fold,
            guard: None,
        }
    }

    /// Where the restore may spill its working set (the keys it will
    /// rewrite) instead of holding it in memory. Put it on the fold's
    /// filesystem, not a RAM-backed temp dir; without it the restore spills
    /// to the system temp dir.
    pub fn scratch_in(mut self, dir: impl Into<PathBuf>) -> Self {
        self.scratch = Some(dir.into());
        self
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

/// `prefixes` with every prefix that another one covers dropped, so no key
/// matches two of them: a scan per prefix then visits each key once.
pub(crate) fn disjoint(prefixes: &[String]) -> Vec<String> {
    let mut sorted: Vec<&String> = prefixes.iter().collect();
    sorted.sort_by_key(|p| p.len());
    let mut kept: Vec<String> = Vec::new();
    for p in sorted {
        if !kept.iter().any(|k| p.starts_with(k.as_str())) {
            kept.push(p.clone());
        }
    }
    kept
}

/// One pass over a restore's in-scope keys, in key order, deciding each with
/// [`restore_key`] (which the repair model, `tests/model_repair.rs`, runs too).
/// Keys the artifact's entry should replace go to `take`, one at a time, so the
/// caller can spill them rather than hold them. Local keys the artifact lacks
/// are returned: the restore deletes them unless the bucket lists them live,
/// and only these need checking against the listing. Out-of-scope keys are
/// untouched on both sides.
///
/// Memory is the returned set, which holds the keys deleted between the local
/// fold's cursor and the artifact's: never the fold, the artifact, or the
/// bucket's listing.
pub(crate) fn restore_diff<S: SnapshotStore>(
    local: &S,
    artifact: &S,
    prefixes: &[String],
    mut take: impl FnMut(String) -> Result<(), SnapshotError>,
) -> Result<HashSet<String>, SnapshotError> {
    let mut candidates = HashSet::new();
    for prefix in disjoint(prefixes) {
        // Every key the artifact holds. Whether the bucket lists it doesn't
        // matter when the artifact has it.
        artifact.for_each_in_range(&prefix, |entry| {
            let at = KeyState::At(entry.version.as_u64());
            let (state, identical) = match local.get(&entry.key)? {
                Some(l) => (
                    KeyState::At(l.version.as_u64()),
                    l.version == entry.version && l.value == entry.value,
                ),
                None => (KeyState::Absent, false),
            };
            if restore_key(state, at, identical, false) == KeyRestore::TakeArtifact {
                take(entry.key)?;
            }
            Ok(())
        })?;
        // Local keys the artifact lacks. The kernel is asked first, as if the
        // artifact lacked the key and the bucket didn't list it, so the
        // artifact is read only for keys that could be deleted.
        local.for_each_in_range(&prefix, |entry| {
            let state = KeyState::At(entry.version.as_u64());
            if restore_key(state, KeyState::Absent, false, false) == KeyRestore::Delete
                && artifact.get(&entry.key)?.is_none()
            {
                candidates.insert(entry.key);
            }
            Ok(())
        })?;
    }
    Ok(candidates)
}

/// Keys written to a scratch file and read back in chunks: a restore's
/// rewrite set can be the whole fold (a fresh node seeding from an artifact),
/// so it never sits in memory. Each key is `len: u32 LE ++ bytes`.
pub(crate) struct KeySpill {
    w: BufWriter<File>,
    len: u64,
}

impl KeySpill {
    /// A spill in `dir`, or the system temp dir. The file is unlinked from the
    /// start, so nothing is left behind.
    pub(crate) fn new(dir: Option<&Path>) -> std::io::Result<Self> {
        let file = match dir {
            Some(dir) => tempfile::tempfile_in(dir)?,
            None => tempfile::tempfile()?,
        };
        Ok(Self {
            w: BufWriter::new(file),
            len: 0,
        })
    }

    pub(crate) fn push(&mut self, key: &str) -> std::io::Result<()> {
        let n = u32::try_from(key.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "key longer than 4 GiB")
        })?;
        self.w.write_all(&n.to_le_bytes())?;
        self.w.write_all(key.as_bytes())?;
        self.len += 1;
        Ok(())
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Rewind for reading.
    pub(crate) fn into_reader(self) -> std::io::Result<SpillReader> {
        let mut file = self.w.into_inner().map_err(|e| e.into_error())?;
        file.rewind()?;
        Ok(SpillReader {
            r: BufReader::new(file),
            left: self.len,
        })
    }
}

/// The read side of a [`KeySpill`].
pub(crate) struct SpillReader {
    r: BufReader<File>,
    left: u64,
}

impl SpillReader {
    /// The next key, in the order they were pushed.
    pub(crate) fn next_key(&mut self) -> std::io::Result<Option<String>> {
        if self.left == 0 {
            return Ok(None);
        }
        let mut n = [0u8; 4];
        self.r.read_exact(&mut n)?;
        let mut key = vec![0u8; u32::from_le_bytes(n) as usize];
        self.r.read_exact(&mut key)?;
        self.left -= 1;
        String::from_utf8(key)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Up to `max` keys, in the order they were pushed; empty when done.
    pub(crate) fn take(&mut self, max: usize) -> std::io::Result<Vec<String>> {
        let mut keys = Vec::new();
        while keys.len() < max {
            match self.next_key()? {
                Some(key) => keys.push(key),
                None => break,
            }
        }
        Ok(keys)
    }
}

/// How many bytes of keys a [`KeySorter`] holds before spilling a sorted run.
const SORT_RUN_BYTES: usize = 64 << 20;

/// An external sort of key names: keys accumulate up to a run's worth, each
/// full run is sorted and spilled ([`KeySpill`]), and [`sorted`](Self::sorted)
/// merges the runs. Memory is one run, however many keys pass through; a
/// listing that fits in one run never touches disk.
pub(crate) struct KeySorter {
    dir: Option<PathBuf>,
    run_bytes: usize,
    buf: Vec<String>,
    held: usize,
    runs: Vec<SpillReader>,
}

impl KeySorter {
    pub(crate) fn new(dir: Option<PathBuf>) -> Self {
        Self::with_run_bytes(dir, SORT_RUN_BYTES)
    }

    pub(crate) fn with_run_bytes(dir: Option<PathBuf>, run_bytes: usize) -> Self {
        Self {
            dir,
            run_bytes,
            buf: Vec::new(),
            held: 0,
            runs: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, key: String) -> std::io::Result<()> {
        self.held += key.len() + std::mem::size_of::<String>();
        self.buf.push(key);
        if self.held >= self.run_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    fn spill_run(&mut self) -> std::io::Result<()> {
        self.buf.sort_unstable();
        self.buf.dedup();
        let mut spill = KeySpill::new(self.dir.as_deref())?;
        for key in self.buf.drain(..) {
            spill.push(&key)?;
        }
        self.runs.push(spill.into_reader()?);
        self.held = 0;
        Ok(())
    }

    /// Every key pushed, ascending, each once.
    pub(crate) fn sorted(mut self) -> std::io::Result<SortedKeys> {
        if self.runs.is_empty() {
            self.buf.sort_unstable();
            self.buf.dedup();
            return Ok(SortedKeys::Held(self.buf.into_iter()));
        }
        if !self.buf.is_empty() {
            self.spill_run()?;
        }
        let mut heads = std::collections::BinaryHeap::new();
        for (i, run) in self.runs.iter_mut().enumerate() {
            if let Some(key) = run.next_key()? {
                heads.push(std::cmp::Reverse((key, i)));
            }
        }
        Ok(SortedKeys::Merged {
            runs: self.runs,
            heads,
            last: None,
        })
    }
}

/// [`KeySorter`]'s output: ascending, without duplicates.
pub(crate) enum SortedKeys {
    Held(std::vec::IntoIter<String>),
    Merged {
        runs: Vec<SpillReader>,
        heads: std::collections::BinaryHeap<std::cmp::Reverse<(String, usize)>>,
        last: Option<String>,
    },
}

impl SortedKeys {
    pub(crate) fn next_key(&mut self) -> std::io::Result<Option<String>> {
        match self {
            SortedKeys::Held(keys) => Ok(keys.next()),
            SortedKeys::Merged { runs, heads, last } => loop {
                let Some(std::cmp::Reverse((key, i))) = heads.pop() else {
                    return Ok(None);
                };
                if let Some(next) = runs[i].next_key()? {
                    heads.push(std::cmp::Reverse((next, i)));
                }
                // Runs are deduplicated within, not across.
                if last.as_deref() != Some(key.as_str()) {
                    *last = Some(key.clone());
                    return Ok(Some(key));
                }
            },
        }
    }
}

/// Read one chunk of a restore's rewritten entries back from the artifact
/// fold.
pub(crate) fn materialize<S: SnapshotStore>(
    artifact: &S,
    keys: Vec<String>,
) -> Result<Vec<KvUpdate>, SnapshotError> {
    keys.into_iter()
        .map(|key| match artifact.get(&key)? {
            Some(entry) => Ok(KvUpdate::Put(entry)),
            None => Err(SnapshotError::Backend(format!(
                "restored artifact fold lost key {key:?} mid-restore"
            ))),
        })
        .collect()
}

/// A restore's delete: like the key-listing diff's synthetic deletes, a state
/// correction rather than a log entry, so it carries the unknown version and
/// never moves a cursor.
fn restore_delete(key: String) -> KvUpdate {
    KvUpdate::Delete {
        key,
        version: VersionToken::unknown(),
    }
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
    /// The bucket's retention was seen evicting current values: the fold
    /// commits the memo on its cursor. No reply; the watch keeps running.
    Evicting,
    /// The key-listing diff: the reader that lists the bucket's live keys,
    /// and where to sort that listing. The main loop applies synthetic deletes
    /// for in-scope keys missing from it, then acks so the watch task can
    /// start the fallback re-list.
    Relist {
        reader: Arc<dyn KvReader>,
        scratch: Option<PathBuf>,
        ack: oneshot::Sender<()>,
    },
    /// The artifact restore: a verified artifact fold, the reader that lists
    /// the bucket's live keys, and the watcher whose retention vouches for
    /// that listing. The main loop folds the in-scope difference, advances to
    /// the artifact's cursor, and replies with it so the watch task resumes
    /// from there.
    Restore {
        restored: RestoredFold<S>,
        reader: Arc<dyn KvReader>,
        watcher: Arc<dyn KvWatcher>,
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
    Relist(
        &'a Arc<dyn KvReader>,
        Option<PathBuf>,
        &'a mpsc::Sender<RepairRequest<S>>,
    ),
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

/// How often a live watch re-reads the bucket's retention, until it first
/// sees current-value eviction (only while a key-listing repair could still
/// be planned: `Relist` or `Auto`).
const RETENTION_POLL: Duration = Duration::from_secs(60);

/// The watch task's copy of the eviction memo ([`WatchCursor`]'s): has this
/// fold ever seen the bucket's retention evict current values? The fold
/// keeps it durably; this copy decides the task's repairs. Retention can be
/// edited, and a key that aged out while eviction was in force stays missing
/// from the bucket's listing after it is turned off, so the listing is
/// trusted only by a fold that never saw eviction ([`listing_truth`]).
struct EvictionMemo<'a, S> {
    seen: bool,
    repair: Option<&'a RepairHandle<S>>,
}

impl<S> EvictionMemo<'_, S> {
    /// Read the bucket's retention. The first time it shows current values
    /// may have been evicted (eviction on, and the log already past its first
    /// revision), remember it here and have the fold commit it. Eviction that
    /// is configured but hasn't dropped anything yet isn't remembered: the
    /// listing is still complete, and a fresh node on a fresh bucket must not
    /// be sent to an artifact that doesn't exist yet.
    async fn observe(&mut self, watcher: &dyn KvWatcher) -> Result<Option<Retention>, KvError> {
        let retention = watcher.retention().await?;
        if !self.seen && listing_truth(retention, false) == Some(false) {
            self.seen = true;
            if let Some(h) = self.repair {
                info!(
                    "the bucket's retention has evicted current values; this fold will not \
                     trust the bucket's key listing again, even if eviction is turned off"
                );
                h.tx.send(RepairRequest::Evicting).await.map_err(|_| {
                    KvError::WatchError("watch loop ended while recording eviction".into())
                })?;
            }
        }
        Ok(retention)
    }

    /// [`listing_truth`] for the retention read now and the memo.
    async fn listing_truth(&mut self, watcher: &dyn KvWatcher) -> Result<Option<bool>, KvError> {
        let retention = self.observe(watcher).await?;
        Ok(listing_truth(retention, self.seen))
    }

    /// Could a key-listing repair still be planned, so retention is worth
    /// watching?
    fn polls(&self) -> bool {
        !self.seen
            && matches!(
                self.repair.map(|h| h.mode.mode()),
                Some(RepairMode::Relist | RepairMode::Auto)
            )
    }

    /// Read retention without failing the watch on an error: the read only
    /// feeds the memo, and the next one retries.
    async fn poll(&mut self, watcher: &dyn KvWatcher) {
        if let Err(e) = self.observe(watcher).await {
            warn!(error = %e, "reading the bucket's retention failed; retrying later");
        }
    }

    /// Run a live watch, re-reading retention every [`RETENTION_POLL`]
    /// while [`polls`](Self::polls): eviction turned on and off again while
    /// the watch runs must still be seen.
    async fn watching(
        &mut self,
        watcher: &dyn KvWatcher,
        watch: impl std::future::Future<Output = Result<(), KvError>>,
    ) -> Result<(), KvError> {
        if !self.polls() {
            return watch.await;
        }
        tokio::pin!(watch);
        let mut tick = tokio::time::interval(RETENTION_POLL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                res = &mut watch => return res,
                _ = tick.tick() => {
                    self.poll(watcher).await;
                    if !self.polls() {
                        return watch.await;
                    }
                }
            }
        }
    }
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
    let shared = Arc::clone(&watcher);
    let (watcher, scope) = (watcher.as_ref(), &scope);
    let mut memo = EvictionMemo {
        seen: resume.as_ref().is_some_and(WatchCursor::seen_evicting),
        repair: repair.as_ref(),
    };
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
            _ => memo.listing_truth(watcher).await?,
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

    if memo.polls() {
        memo.poll(watcher).await;
    }

    loop {
        let cursor = if std::mem::take(&mut repair_first) {
            WatchCursor::none()
        } else {
            let Some(cursor) = resume.take() else {
                return memo
                    .watching(watcher, watch_scope(watcher, scope, tx))
                    .await;
            };
            let watch = watch_scope_from(watcher, scope, &cursor, tx.clone());
            match memo.watching(watcher, watch).await {
                Err(KvError::CursorExpired) => cursor,
                other => return other,
            }
        };
        match plan(watcher, repair.as_ref(), &mut memo).await? {
            Plan::ReListOnly => {
                warn!(
                    "watch cursor expired with no repair armed (needs a store and a reader or \
                     restore source); falling back to the re-list alone — keys deleted during \
                     the gap may persist in the fold"
                );
                return memo
                    .watching(watcher, watch_scope(watcher, scope, tx))
                    .await;
            }
            Plan::Relist(reader, scratch, repairs) => {
                warn!(
                    "watch cursor expired; resyncing stale keys, then falling back to the full re-list"
                );
                resync_stale_keys(reader, scratch, repairs).await?;
                return memo
                    .watching(watcher, watch_scope(watcher, scope, tx))
                    .await;
            }
            Plan::Restore(reader, source, repairs) => {
                warn!("watch cursor expired; restoring the fold from the latest artifact");
                let restored =
                    restore_from_artifact(&shared, scope, reader, source, repairs, &cursor)
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
                // An artifact from a fold that saw eviction carries the memo
                // (the fold took it with the artifact's cursor).
                memo.seen |= restored.seen_evicting();
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
async fn plan<'a, S: Send>(
    watcher: &dyn KvWatcher,
    repair: Option<&'a RepairHandle<S>>,
    memo: &mut EvictionMemo<'_, S>,
) -> Result<Plan<'a, S>, KvError> {
    let Some(h) = repair else {
        return Ok(Plan::ReListOnly);
    };
    let mode = h.mode.mode();
    let truth = match mode {
        RepairMode::Relist | RepairMode::Auto => memo.listing_truth(watcher).await?,
        RepairMode::None | RepairMode::Restore => None,
    };
    Ok(match (plan_repair(mode, truth), &h.mode) {
        (RepairPlan::ReListOnly, _) => Plan::ReListOnly,
        (RepairPlan::RefuseRelist, _) => {
            let msg = "watch cursor expired on a bucket whose retention evicts current \
                       values (max_age, per-message TTL, or discard:old), or did while this \
                       fold tracked it: its key listing can't tell a deleted key from an \
                       aged-out one, so the key-listing resync would delete valid keys. \
                       Refusing. Repair from artifacts with \
                       ExpiryRepair::Restore or ExpiryRepair::Auto (or accept a re-list-only \
                       fallback with ExpiryRepair::None)";
            error!(msg);
            return Err(KvError::WatchError(msg.into()));
        }
        (RepairPlan::Relist, ExpiryRepair::Relist(reader)) => Plan::Relist(reader, None, &h.tx),
        (RepairPlan::Relist, ExpiryRepair::Auto { reader, restore }) => {
            Plan::Relist(reader, restore.scratch_dir(), &h.tx)
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
    watcher: &Arc<dyn KvWatcher>,
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

    let (reply_tx, reply_rx) = oneshot::channel();
    repairs
        .send(RepairRequest::Restore {
            restored,
            reader: Arc::clone(reader),
            watcher: Arc::clone(watcher),
            reply: reply_tx,
        })
        .await
        .map_err(|_| KvError::WatchError("watch loop ended during a restore".into()))?;
    reply_rx
        .await
        .map_err(|_| KvError::WatchError("watch loop dropped a restore reply".into()))?
}

/// Cursor-expired stale-key resync (the key-listing diff), run BEFORE the
/// fallback watch is established: hand the main loop the reader (it lists the
/// scope's live keys, diffs them against the fold, and applies synthetic
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
    reader: &Arc<dyn KvReader>,
    scratch: Option<PathBuf>,
    repairs: &mpsc::Sender<RepairRequest<S>>,
) -> Result<(), KvError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    if repairs
        .send(RepairRequest::Relist {
            reader: Arc::clone(reader),
            scratch,
            ack: ack_tx,
        })
        .await
        .is_ok()
    {
        // A dropped ack (main loop shut down, or the resync failed and ended
        // it) means the fallback watch is about to die with it.
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
    let req = match req {
        RepairRequest::Evicting => return fold.remember_evicting().await,
        req => req,
    };
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
        RepairRequest::Evicting => unreachable!("handled above"),
        RepairRequest::Relist {
            reader,
            scratch,
            ack,
        } => {
            relist(fold, &*reader, scratch, prefixes).await?;
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
            reader,
            watcher,
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
            match restore(fold, restored, &*reader, &*watcher, prefixes).await? {
                Ok(()) => {
                    let _ = reply.send(Ok(target));
                }
                Err(msg) => {
                    error!(%msg, "cursor-expiry repair refused");
                    let _ = reply.send(Err(KvError::WatchError(msg)));
                }
            }
            Ok(())
        }
    }
}

/// The key-listing diff, main-loop half: synthetic deletes for in-scope keys
/// the fold holds and the bucket no longer lists — they vanished during the
/// gap (their delete markers evicted with the cursor), and the re-list can't
/// deliver a delete.
///
/// Memory stays bounded at any fold size: the listing is sorted externally
/// ([`KeySorter`], runs spilled to `scratch`) and merge-joined against the
/// fold's own key-ordered scan, so neither the listing nor the fold is ever
/// held. Only the stale keys are — the keys deleted during the gap.
async fn relist<U, S, P, A, O>(
    fold: &mut Fold<U, S, P, A, O>,
    reader: &dyn KvReader,
    scratch: Option<PathBuf>,
    prefixes: &[String],
) -> Result<(), KvError>
where
    U: Send,
    S: SnapshotStore + Send + 'static,
    P: FnMut(&KvUpdate) -> Option<U> + Send,
    A: FnMut(Vec<U>) + Send,
    O: FnMut(WatchCursor) + Send,
{
    // Disjoint and in order, so the fold's per-prefix scans run in one
    // ascending sequence the sorted listing can be walked alongside.
    let mut prefixes = disjoint(prefixes);
    prefixes.sort_unstable();

    // The listing streams into a sorter on a blocking thread. The send blocks
    // only if sorting falls behind the network.
    let (tx, rx) = std::sync::mpsc::sync_channel::<String>(8192);
    let sorter = tokio::task::spawn_blocking(move || {
        let mut sorter = KeySorter::new(scratch);
        for key in rx {
            sorter.push(key)?;
        }
        sorter.sorted()
    });
    for prefix in &prefixes {
        reader
            .for_each_key(prefix, &mut |key| {
                let _ = tx.send(key);
            })
            .await
            .map_err(|e| {
                repair_fatal(format!(
                    "cursor-expired resync failed listing live keys under {prefix:?}: {e}; \
                     failing the watch rather than silently keeping stale keys"
                ))
            })?;
    }
    drop(tx);
    let mut live = sorter
        .await
        .map_err(|e| repair_fatal(format!("cursor-expired resync: sort task panicked: {e}")))?
        .map_err(|e| repair_fatal(format!("cursor-expired resync: sorting the listing: {e}")))?;

    let Some(st) = fold.take_store() else {
        unreachable!("repairs are armed only with a store")
    };
    let (st, stale) = tokio::task::spawn_blocking(move || {
        let mut stale: Vec<String> = Vec::new();
        let res = (|| {
            let mut next = live.next_key()?;
            for prefix in &prefixes {
                // Stream the fold's keys rather than `range()`, which buffers
                // every in-scope entry — values included.
                st.for_each_in_range(prefix, |entry| {
                    while next.as_ref().is_some_and(|k| *k < entry.key) {
                        next = live.next_key()?;
                    }
                    if next.as_deref() != Some(entry.key.as_str()) {
                        stale.push(entry.key);
                    }
                    Ok(())
                })?;
            }
            Ok::<_, SnapshotError>(())
        })();
        (st, res.map(|()| stale))
    })
    .await
    .map_err(|e| repair_fatal(format!("cursor-expired resync: diff task panicked: {e}")))?;
    fold.put_store(st);
    // FATAL, not a degrade: an incomplete diff silently leaves deleted keys in
    // the fold forever (tests/model.rs proves the divergence reachable under
    // degrade semantics). Fail the watch; the restart re-runs the resume →
    // expiry → resync from scratch.
    let stale = stale.map_err(|e| {
        repair_fatal(format!(
            "cursor-expired resync failed diffing the fold against the listing: {e}"
        ))
    })?;
    if !stale.is_empty() {
        warn!(
            stale = stale.len(),
            "cursor-expired resync: deleting keys that vanished during the gap"
        );
    }
    // Synthetic deletes: no revision (unknown version), so they never advance
    // the cursor. Flushed in batch-sized chunks, all before the ack.
    let max = fold.batch_cap();
    let mut stale = stale.into_iter();
    loop {
        let chunk: Vec<String> = stale.by_ref().take(max).collect();
        if chunk.is_empty() {
            break;
        }
        for key in chunk {
            fold.correct(restore_delete(key));
        }
        fold.flush().await?;
    }
    fold.flush().await
}

/// The artifact restore, main-loop half: replace the in-scope fold with the
/// artifact's ([`restore_diff`]), then take its cursor.
///
/// Memory stays bounded at any fold size. The keys to rewrite are spilled to
/// scratch and read back a chunk at a time; only the keys the restore would
/// delete are held, and the bucket's listing is streamed past them rather
/// than collected. With no delete candidates the listing is skipped.
async fn restore<U, S, P, A, O>(
    fold: &mut Fold<U, S, P, A, O>,
    mut restored: RestoredFold<S>,
    reader: &dyn KvReader,
    watcher: &dyn KvWatcher,
    prefixes: &[String],
) -> Result<Result<(), String>, KvError>
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
    let prefixes = disjoint(prefixes);
    let max = fold.batch_cap();
    let scope = prefixes.clone();
    let (st, r, diff) = tokio::task::spawn_blocking(move || {
        let diff = (|| {
            let mut spill = KeySpill::new(restored.scratch.as_deref())?;
            let candidates = restore_diff(&st, restored.fold(), &scope, |key| {
                spill.push(&key).map_err(SnapshotError::from)
            })?;
            Ok::<_, SnapshotError>((spill, candidates))
        })();
        (st, restored, diff)
    })
    .await
    .map_err(|e| repair_fatal(format!("cursor-expired restore: diff task panicked: {e}")))?;
    fold.put_store(st);
    restored = r;
    let (spill, mut deletes) = diff.map_err(|e| {
        repair_fatal(format!(
            "cursor-expired restore: diffing the fold against the artifact failed: {e}"
        ))
    })?;

    // A delete candidate the bucket lists live is kept: its current value is a
    // write after the artifact's cursor, which the resume delivers
    // (`restore_key`'s `listed_live`). A failed listing fails the watch, like
    // the key-listing repair's.
    if !deletes.is_empty() {
        for prefix in &prefixes {
            reader
                .for_each_key(prefix, &mut |key| {
                    deletes.remove(&key);
                })
                .await
                .map_err(|e| {
                    repair_fatal(format!(
                        "cursor-expired restore: listing live keys under {prefix:?} failed: {e}"
                    ))
                })?;
        }
        // The listing vouches for every write after the artifact's cursor only
        // while the log still holds all of them: one that aged out before the
        // listing is unlisted, and would be deleted. Retention only moves
        // forward, so a check after the listing covers it
        // (`tests/model_repair.rs`, `NoListingRecheck`). Nothing is folded yet.
        if let Some(first) = watcher.retention().await?.map(|r| r.first_revision)
            && !resume_window_ok(target.rank(), first)
        {
            let _ = tokio::task::spawn_blocking(move || drop(restored)).await;
            return Ok(Err(format!(
                "cursor-expired restore: the log evicted writes after the artifact's cursor \
                 ({:?}, first retained revision {first}) while the restore was listing live \
                 keys, so the listing can't vouch for them; nothing was changed. Exports must \
                 run well inside the bucket's max_age / discard turnover",
                target
            )));
        }
    }
    info!(
        from = ?fold.applied(),
        to = ?target,
        rewritten = spill.len(),
        deleted = deletes.len(),
        "cursor-expired restore: replacing the in-scope fold with the artifact's"
    );

    // Fold the difference through parse/apply/store in `max`-sized chunks,
    // values read back per chunk. These carry no stream position: every chunk
    // commits under the old cursor, so a crash part-way re-runs the
    // (idempotent) restore on restart.
    let mut spill = spill
        .into_reader()
        .map_err(|e| repair_fatal(format!("cursor-expired restore: reading scratch: {e}")))?;
    loop {
        let (r, sp, updates) = tokio::task::spawn_blocking(move || {
            let updates = spill
                .take(max)
                .map_err(SnapshotError::from)
                .and_then(|keys| materialize(restored.fold(), keys));
            (restored, spill, updates)
        })
        .await
        .map_err(|e| repair_fatal(format!("cursor-expired restore: read task panicked: {e}")))?;
        restored = r;
        spill = sp;
        let updates = updates.map_err(|e| {
            repair_fatal(format!(
                "cursor-expired restore: reading the artifact failed: {e}"
            ))
        })?;
        if updates.is_empty() {
            break;
        }
        for u in updates {
            fold.correct(u);
        }
        fold.flush().await?;
    }
    let mut deletes = deletes.into_iter();
    loop {
        let chunk: Vec<_> = deletes.by_ref().take(max).collect();
        if chunk.is_empty() {
            break;
        }
        for key in chunk {
            fold.correct(restore_delete(key));
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
    Ok(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::KvEntry;
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

    /// The restore's updates in production order — rewritten entries from
    /// the spill, then deletes for candidates the listing lacks — through the
    /// real `restore_diff`, `KeySpill`, and `materialize`.
    fn restore_ops(
        local: &AppendLogSnapshot,
        artifact: &AppendLogSnapshot,
        prefixes: &[String],
        live: &HashSet<String>,
    ) -> Vec<KvUpdate> {
        let mut spill = KeySpill::new(None).unwrap();
        let candidates = restore_diff(local, artifact, prefixes, |k| {
            spill.push(&k).map_err(SnapshotError::from)
        })
        .unwrap();
        let mut reader = spill.into_reader().unwrap();
        let mut updates = Vec::new();
        loop {
            let chunk = reader.take(2).unwrap();
            if chunk.is_empty() {
                break;
            }
            updates.extend(materialize(artifact, chunk).unwrap());
        }
        let mut deletes: Vec<String> = candidates
            .into_iter()
            .filter(|k| !live.contains(k))
            .collect();
        deletes.sort();
        updates.extend(deletes.into_iter().map(restore_delete));
        updates
    }

    /// The diff turns the local in-scope fold into the artifact's: changed and
    /// new keys are rewritten, keys the artifact lacks are delete candidates,
    /// identical entries and out-of-scope keys are left alone.
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
        let mut rewritten = Vec::new();
        let candidates = restore_diff(&local, &artifact, &["node.".to_string()], |k| {
            rewritten.push(k);
            Ok(())
        })
        .unwrap();
        assert_eq!(rewritten, vec!["node.changed", "node.new"]);
        assert_eq!(candidates, HashSet::from(["node.gone".to_string()]));

        let updates = restore_ops(&local, &artifact, &["node.".to_string()], &HashSet::new());
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
            matches!(&updates[2], KvUpdate::Delete { version, .. } if version.is_unknown()),
            "restore deletes never carry a revision"
        );
    }

    #[test]
    fn diff_visits_each_key_once_under_overlapping_prefixes() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = fold(&dir, "local", &[put("node.a", b"1", 1)], 1);
        let artifact = fold(&dir, "artifact", &[put("node.b", b"2", 2)], 2);
        let updates = restore_ops(
            &local,
            &artifact,
            &["node.".into(), "node".into()],
            &HashSet::new(),
        );
        assert_eq!(
            updates.len(),
            2,
            "each key once despite two covering prefixes"
        );
    }

    #[test]
    fn disjoint_drops_covered_prefixes() {
        let s = |v: &[&str]| v.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        assert_eq!(
            disjoint(&s(&["node.", "node", "edge."])),
            s(&["node", "edge."])
        );
        assert_eq!(disjoint(&s(&["a.", ""])), s(&[""]));
        assert_eq!(disjoint(&s(&["a.", "a."])), s(&["a."]));
    }

    /// The external sort against an in-memory sort, across run sizes that
    /// force one run, a few, and one per key, with duplicates within and
    /// across runs.
    #[test]
    fn key_sorter_matches_sort_dedup_at_every_run_size() {
        let mut keys: Vec<String> = (0..300u32)
            .map(|i| format!("k.{}", (i * 7919) % 211))
            .collect();
        keys.push(String::new());
        keys.push("k.".into());
        let mut want = keys.clone();
        want.sort();
        want.dedup();
        for run_bytes in [usize::MAX, 4096, 512, 1] {
            let mut sorter = KeySorter::with_run_bytes(None, run_bytes);
            for k in &keys {
                sorter.push(k.clone()).unwrap();
            }
            let mut sorted = sorter.sorted().unwrap();
            let mut got = Vec::new();
            while let Some(k) = sorted.next_key().unwrap() {
                got.push(k);
            }
            assert_eq!(got, want, "run_bytes {run_bytes}");
        }
    }

    #[test]
    fn spill_round_trips_in_order_and_chunks() {
        let mut spill = KeySpill::new(None).unwrap();
        let keys: Vec<String> = (0..10).map(|i| format!("k.{i}")).collect();
        for k in &keys {
            spill.push(k).unwrap();
        }
        spill.push("").unwrap();
        assert_eq!(spill.len(), 11);
        let mut r = spill.into_reader().unwrap();
        let mut got = Vec::new();
        loop {
            let chunk = r.take(3).unwrap();
            if chunk.is_empty() {
                break;
            }
            assert!(chunk.len() <= 3);
            got.extend(chunk);
        }
        let mut want = keys;
        want.push(String::new());
        assert_eq!(got, want);
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
    /// `restore_diff`, `KeySpill` and `materialize` and a real store. Per in-scope key,
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
                        let full = restore_ops(
                            &fold_of(
                                &dir,
                                &format!("probe-{li}-{}", scope.join("|")),
                                &local_states,
                                3,
                            ),
                            &artifact,
                            &prefixes,
                            live,
                        );
                        // Crash after `cut` ops: apply them, then re-diff.
                        for cut in 0..=full.len() {
                            let name = format!("local-{li}-{cut}-{}", scope.join("|"));
                            let mut local = fold_of(&dir, &name, &local_states, 3);
                            let first = restore_ops(&local, &artifact, &prefixes, live);
                            let partial: Vec<KvUpdate> = first.into_iter().take(cut).collect();
                            local.apply(&partial, &WatchCursor::from_u64(3)).unwrap();
                            let rest = restore_ops(&local, &artifact, &prefixes, live);
                            local.apply(&rest, &WatchCursor::from_u64(9)).unwrap();

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
                                restore_ops(&local, &artifact, &prefixes, live).is_empty(),
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
