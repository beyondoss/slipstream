//! Health of a running watch: one handle, one cheap snapshot.
//!
//! [`watch_applied`](crate::watch_applied) is correct without anyone looking
//! at it, but a fold that is stale, stuck, or refused a repair looks exactly
//! like a healthy one from the outside: the process is up and the reads
//! succeed. A [`WatchHealth`] is the outside view. Clone one into
//! [`BatchConfig::health`](crate::BatchConfig::health), keep a clone, and call
//! [`snapshot`](WatchHealth::snapshot) from a metrics or health task.
//!
//! Observation only. The watch writes these fields; nothing it decides reads
//! them. Every field is an atomic or sits behind a lock held for a few stores,
//! never across an await, and the per-flush cost is a handful of relaxed
//! stores (no allocation, no I/O). One handle serves one watch, and it
//! survives the watch's restarts: reuse the same handle across calls, and
//! [`restarts`](HealthSnapshot::restarts) and
//! [`last_error`](HealthSnapshot::last_error) will tell you the story a log
//! line would have.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::artifact::ExportManifest;
use crate::kv::{KvError, KvWatcher, WatchCursor};

/// How often the watch asks the backend for the log's head, by default. One
/// small request per interval per watch, never one per update.
pub const DEFAULT_HEAD_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);

/// The shared, cheap-to-clone health handle of one watch. See the
/// [module docs](self).
#[derive(Debug, Clone)]
pub struct WatchHealth {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    epoch: Instant,
    head_sample_interval: Duration,
    // Microseconds from `epoch`, plus one. Zero is "never" / "not in flight".
    apply_started: AtomicU64,
    committed_at: AtomicU64,
    head_sampled_at: AtomicU64,
    // Revisions. Zero is "unknown" (no real revision is 0).
    applied: AtomicU64,
    head: AtomicU64,
    first: AtomicU64,
    starts: AtomicU64,
    expiries: AtomicU64,
    running: AtomicBool,
    detail: Mutex<Detail>,
}

// Written on rare events (a repair step, an export, an error), read by
// `snapshot`. Never touched per update.
#[derive(Debug)]
struct Detail {
    repair: RepairState,
    artifact: Option<ArtifactSeen>,
    last_error: Option<(String, Instant)>,
}

#[derive(Debug, Clone, Copy)]
struct ArtifactSeen {
    cursor: Option<u64>,
    created_at_unix: u64,
}

/// What the watch's cursor-expiry repair is doing, or last concluded.
///
/// `Refused` and `WaitingForArtifact` are sticky: they stay until a repair
/// succeeds, because the watch ends on them and the caller's restart retries
/// the repair from the same expired cursor. A fold in either state is stale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairState {
    /// No repair is running and none is outstanding.
    Idle,
    /// The key-listing diff is running (synthetic deletes for vanished keys).
    Relisting,
    /// An artifact restore is running: checking, downloading, or folding it.
    Restoring,
    /// A repair was refused and there is no safe recovery from this artifact
    /// or bucket: an artifact outside retention or scope, or a key-listing
    /// diff on a bucket that evicts current values. The reason is the one
    /// logged at `error`. Needs an operator or a newer, valid artifact.
    Refused(String),
    /// A restore is needed, but no usable artifact exists yet: none is
    /// published, the newest is not ahead of the fold, or it could not be
    /// read. A newer export round clears this on its own; if it persists the
    /// exporter is down or behind.
    WaitingForArtifact(String),
}

/// The newest artifact this watch knows of, from its own exports or from the
/// restore source it last consulted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactHealth {
    /// The artifact's cursor revision, when it has one this build can read.
    pub cursor: Option<u64>,
    /// Time since the artifact was created, by the wall clock. Clock skew
    /// between exporter and reader shifts it; saturates at zero.
    pub age: Duration,
}

/// The last error that ended a watch run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastError {
    /// The error, as displayed.
    pub message: String,
    /// Time since the run ended with it.
    pub age: Duration,
}

/// A point-in-time copy of a watch's health. Plain data: build alerts on it,
/// export it as metrics, serialize it into a health endpoint.
///
/// `None` and zero mean "unknown", never "healthy". Alert on what a field
/// says, and separately on a field that stays unknown for longer than the
/// watch has been running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// A `watch_applied` run is in progress on this handle. False before the
    /// first run, between restarts, and after a run ends. **Alert** when it
    /// is false for longer than your restart backoff.
    pub running: bool,
    /// The applied cursor's revision: every update at or below it has been
    /// applied (and, with a store, committed with it). `None` until the
    /// watch has a position (a fresh start with nothing delivered yet), or
    /// when the backend's cursor is not a plain revision. **Alert** when it
    /// stops moving while [`lag`](Self::lag) is nonzero.
    pub applied: Option<u64>,
    /// Time since the last successful commit: the store committed the batch
    /// and its cursor, or, without a store, the batch was applied. `None`
    /// until the first one in this process. It grows on an idle bucket, so
    /// read it together with [`lag`](Self::lag): **alert** on a large age
    /// with nonzero lag.
    pub last_commit_age: Option<Duration>,
    /// How long the current store apply has been in flight, from when it was
    /// handed to the blocking pool. `None` when none is. `apply` has no
    /// timeout and cannot be cancelled, so this is the only sign of a store
    /// that stopped returning. **Alert** when it passes your limit and
    /// restart the process.
    pub apply_in_flight: Option<Duration>,
    /// The log's newest revision at the last sample. `None` until the first
    /// sample lands, or on a backend that cannot report it.
    pub stream_head: Option<u64>,
    /// The log's oldest retained revision at the last sample.
    pub stream_first: Option<u64>,
    /// Time since [`stream_head`](Self::stream_head) was sampled. Sampled
    /// every [`DEFAULT_HEAD_SAMPLE_INTERVAL`] unless the handle was built
    /// with [`WatchHealth::with_head_sample_interval`]. **Alert** when it is
    /// several intervals old: the lag below is then stale too.
    pub head_sample_age: Option<Duration>,
    /// Revisions between the log's head and the applied cursor, as of the
    /// last head sample; `head - applied`, floored at zero. It is the whole
    /// bucket's head, so under a [`WatchScope::Prefix`](crate::WatchScope)
    /// or `Prefixes` scope it also counts revisions this watch will never
    /// see: such a watch reads nonzero lag while idle, and lag is a trend
    /// there, not a level. Under `All` scope it is the true backlog.
    /// **Alert** when it grows without bound or stays high.
    pub lag: Option<u64>,
    /// The repair state. **Alert** on anything but `Idle` and `Relisting`
    /// and `Restoring` that persists: `Refused` and `WaitingForArtifact` mean
    /// the fold is stale and is retrying forever.
    pub repair: RepairState,
    /// Cursor expiries this handle has handled: the cursor fell out of the
    /// log, at resume or mid-watch. Each one runs the configured repair, or
    /// under [`ExpiryRepair::None`](crate::ExpiryRepair::None) only a re-list.
    /// **Alert** on any increase for a watch configured with no repair, and
    /// on a rising rate for the others.
    pub cursor_expiries: u64,
    /// The newest artifact this watch exported or read, when it has seen one.
    /// **Alert** when its age passes your export interval by a margin, and
    /// always when it nears the bucket's retention: a restore from an artifact
    /// older than retention is refused.
    pub newest_artifact: Option<ArtifactHealth>,
    /// How many times a run on this handle started after the first.
    /// **Alert** on a rising rate.
    pub restarts: u64,
    /// The last error that ended a run. Cleared by nothing: a healthy run
    /// after it shows as `running` with a growing age.
    pub last_error: Option<LastError>,
}

impl WatchHealth {
    /// A handle with nothing recorded, sampling the log head every
    /// [`DEFAULT_HEAD_SAMPLE_INTERVAL`].
    pub fn new() -> Self {
        Self::with_head_sample_interval(DEFAULT_HEAD_SAMPLE_INTERVAL)
    }

    /// A handle that samples the log head every `interval` (at least 1 ms).
    /// Each sample is one stream-info request.
    pub fn with_head_sample_interval(interval: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                epoch: Instant::now(),
                head_sample_interval: interval.max(Duration::from_millis(1)),
                apply_started: AtomicU64::new(0),
                committed_at: AtomicU64::new(0),
                head_sampled_at: AtomicU64::new(0),
                applied: AtomicU64::new(0),
                head: AtomicU64::new(0),
                first: AtomicU64::new(0),
                starts: AtomicU64::new(0),
                expiries: AtomicU64::new(0),
                running: AtomicBool::new(false),
                detail: Mutex::new(Detail {
                    repair: RepairState::Idle,
                    artifact: None,
                    last_error: None,
                }),
            }),
        }
    }

    /// How long the current store apply has been in flight; the same value as
    /// [`HealthSnapshot::apply_in_flight`], without the rest of the snapshot.
    pub fn in_flight(&self) -> Option<Duration> {
        self.age_of(&self.inner.apply_started)
    }

    /// Read every field. Lock-free except for a brief lock to copy the repair
    /// state, artifact and last error. Never blocks the watch for longer than
    /// a few stores.
    pub fn snapshot(&self) -> HealthSnapshot {
        let i = &*self.inner;
        let nonzero = |a: &AtomicU64| Some(a.load(Ordering::Relaxed)).filter(|v| *v != 0);
        let applied = nonzero(&i.applied);
        let head = nonzero(&i.head);
        let d = self.detail();
        let now_unix = unix_now();
        HealthSnapshot {
            running: i.running.load(Ordering::Relaxed),
            applied,
            last_commit_age: self.age_of(&i.committed_at),
            apply_in_flight: self.age_of(&i.apply_started),
            stream_head: head,
            stream_first: nonzero(&i.first),
            head_sample_age: self.age_of(&i.head_sampled_at),
            lag: head.map(|h| h.saturating_sub(applied.unwrap_or(0))),
            repair: d.repair.clone(),
            cursor_expiries: i.expiries.load(Ordering::Relaxed),
            newest_artifact: d.artifact.map(|a| ArtifactHealth {
                cursor: a.cursor,
                age: Duration::from_secs(now_unix.saturating_sub(a.created_at_unix)),
            }),
            restarts: i.starts.load(Ordering::Relaxed).saturating_sub(1),
            last_error: d.last_error.as_ref().map(|(message, at)| LastError {
                message: message.clone(),
                age: at.elapsed(),
            }),
        }
    }

    // ---- watch-side recording -------------------------------------------

    fn detail(&self) -> MutexGuard<'_, Detail> {
        self.inner
            .detail
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn micros(&self) -> u64 {
        u64::try_from(self.inner.epoch.elapsed().as_micros()).unwrap_or(u64::MAX - 1) + 1
    }

    fn age_of(&self, at: &AtomicU64) -> Option<Duration> {
        let at = at.load(Ordering::Relaxed).checked_sub(1)?;
        Some(
            self.inner
                .epoch
                .elapsed()
                .saturating_sub(Duration::from_micros(at)),
        )
    }

    /// A run starts (or restarts).
    pub(crate) fn run_started(&self, applied: &WatchCursor) {
        self.inner.starts.fetch_add(1, Ordering::Relaxed);
        self.inner.running.store(true, Ordering::Relaxed);
        self.set_applied(applied);
    }

    /// A run ends. An error is kept as the last error.
    pub(crate) fn run_ended(&self, result: &Result<WatchCursor, KvError>) {
        self.inner.running.store(false, Ordering::Relaxed);
        if let Err(e) = result {
            self.detail().last_error = Some((e.to_string(), Instant::now()));
        }
    }

    pub(crate) fn set_applied(&self, cursor: &WatchCursor) {
        self.inner.applied.store(cursor.rank(), Ordering::Relaxed);
    }

    /// The hot path: a store apply is about to run on the blocking pool. The
    /// returned guard clears the mark when the apply really ends: on return,
    /// on panic, or if the task never starts.
    pub(crate) fn begin_apply(&self) -> ApplyInFlight {
        self.inner
            .apply_started
            .store(self.micros(), Ordering::Relaxed);
        ApplyInFlight(self.clone())
    }

    pub(crate) fn committed(&self) {
        self.inner
            .committed_at
            .store(self.micros(), Ordering::Relaxed);
    }

    pub(crate) fn expired(&self) {
        self.inner.expiries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a repair step. A refusal or a wait outlives the watch that hit
    /// it, so progress states never overwrite one; only
    /// [`repair_done`](Self::repair_done) or another blocking state does.
    pub(crate) fn repair_step(&self, state: RepairState) {
        let mut d = self.detail();
        let blocked = matches!(
            d.repair,
            RepairState::Refused(_) | RepairState::WaitingForArtifact(_)
        );
        if !blocked
            || matches!(
                state,
                RepairState::Refused(_) | RepairState::WaitingForArtifact(_)
            )
        {
            d.repair = state;
        }
    }

    /// A repair finished: clears any refusal or wait.
    pub(crate) fn repair_done(&self) {
        self.detail().repair = RepairState::Idle;
    }

    /// Note the newest artifact seen. Keeps whichever is newer.
    pub(crate) fn artifact_seen(&self, manifest: &ExportManifest) {
        let seen = ArtifactSeen {
            cursor: manifest.cursor.as_u64(),
            created_at_unix: manifest.created_at_unix,
        };
        let mut d = self.detail();
        if d.artifact
            .is_none_or(|a| a.created_at_unix <= seen.created_at_unix)
        {
            d.artifact = Some(seen);
        }
    }

    fn head_sampled(&self, first: u64, head: u64) {
        let i = &*self.inner;
        i.first.store(first, Ordering::Relaxed);
        i.head.store(head, Ordering::Relaxed);
        i.head_sampled_at.store(self.micros(), Ordering::Relaxed);
    }
}

impl Default for WatchHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Held by the blocking task: see [`WatchHealth::begin_apply`].
pub(crate) struct ApplyInFlight(WatchHealth);

impl Drop for ApplyInFlight {
    fn drop(&mut self) {
        self.0.inner.apply_started.store(0, Ordering::Relaxed);
    }
}

/// The oldest and newest revisions a log holds, as a backend reports them
/// ([`KvWatcher::log_range`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRange {
    /// The oldest retained revision.
    pub first: u64,
    /// The newest revision: the log's head.
    pub last: u64,
}

/// Sample the log's range into `health` every interval until aborted. A
/// failed sample leaves the last one in place (its age shows the staleness).
pub(crate) async fn sample_log_range(watcher: Arc<dyn KvWatcher>, health: WatchHealth) {
    let mut tick = tokio::time::interval(health.inner.head_sample_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if let Ok(Some(range)) = watcher.log_range().await {
            health.head_sampled(range.first, range.last);
        }
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
