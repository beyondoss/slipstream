//! Deterministic fault injection over the REAL `watch_applied`: every
//! scenario × every fault schedule, against the shipped code, on every
//! snapshot backend.
//!
//! The Stateright models check protocols over abstractions; this checks the
//! implementation. A simulated NATS log (latest message per key, head
//! eviction on an evicting bucket, marker purges on one that keeps current
//! values, an optional mid-watch floor-guard trip) feeds the real
//! `watch_applied` with a real snapshot backend wrapped in a fault store. A
//! schedule injects, at chosen store-apply calls of chosen runs:
//!
//! - a TRANSIENT failure (the batch is re-queued; the watch continues), or
//! - a CRASH — process death — before the apply, after it, or TORN (the
//!   batch's data durable, its cursor not: the append log's torn-write shape,
//!   and a superset state for the atomic-batch LSM backends). The run dies
//!   there; the harness reopens the fold from disk and restarts, as a
//!   supervisor would.
//!
//! Runs repeat until one completes with no fault left to inject. Then:
//! - the fold equals every write minus every real delete in scope, at the
//!   head's cursor, and every out-of-scope key exactly as it started;
//! - the consumer's domain state — rebuilt from the fold at each start, then
//!   driven only by `apply` — equals the fold;
//! - the fold's cursor never moved backward across any crash, and
//!   `on_applied` never reported a backward step within a run;
//! - `apply` never saw a key's revision go backward, and no repair deleted a
//!   key that is live in the log (no regressions, no phantom deletes — not
//!   even transiently);
//! - every artifact exported mid-run (an `ExportRequest` sent from
//!   `on_applied`) records the watch's scope and, with a resume from its
//!   cursor, reaches the truth. This is the real-code half of
//!   `tests/model_fleet.rs`'s induction. Restores also run from artifacts
//!   exported mid-catch-up — what a real exporter publishes when an export
//!   round lands during its own resume or initial re-list.
//!
//! Scenarios cover both the expiry repairs (resume-time expiry, floor-guard
//! trip mid-watch, cursor-less starts with and without data; key-listing
//! diff and artifact restore; All, Prefix and multi-prefix scopes) and the
//! normal data path (resume inside the window with deletes and re-creates,
//! a fresh full re-list). Each runs in both delivery orders and with batches
//! of 1 and 100. The append log gets the full schedule space: no fault;
//! every set of ≤ 2 transient failures; every single crash (each call × each
//! kind); every pair of crashes in consecutive runs; every transient failure
//! followed by a crash; a persistent store failure. The LSM backends get
//! every single fault and the persistent failure (their per-run cost is
//! dominated by opening a database).
//!
//! Reverting any of seven repair steps (pre-repair flush retry, drain,
//! cursor-after-diff, `Auto`'s retention check, the diff's deletes, the ack
//! barrier, the data-without-cursor repair) fails this file.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use slipstream::protocol::resume_window_ok;
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    ARTIFACT_SCHEMA_VERSION, AppendLogSnapshot, BatchConfig, ExpiryRepair, ExportManifest,
    ExportRequest, KvEntry, KvError, KvReader, KvUpdate, KvWatcher, RestoreSource, RestoredFold,
    Retention, VersionToken, WatchCursor, WatchScope, watch_applied,
};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};

// --- Backends -------------------------------------------------------------

trait Backend: Send + Sync + 'static {
    type S: SnapshotStore + Send + 'static;
    const NAME: &'static str;
    fn open(path: &Path) -> (WatchCursor, Self::S);
    fn import(artifact: &Path, dest: &Path) -> (WatchCursor, Self::S);
}

struct AppendLog;
impl Backend for AppendLog {
    type S = AppendLogSnapshot;
    const NAME: &'static str = "append-log";
    fn open(path: &Path) -> (WatchCursor, AppendLogSnapshot) {
        AppendLogSnapshot::open(path, u64::MAX).unwrap()
    }
    fn import(artifact: &Path, dest: &Path) -> (WatchCursor, AppendLogSnapshot) {
        AppendLogSnapshot::import(artifact, dest, u64::MAX).unwrap()
    }
}

#[cfg(feature = "fjall")]
struct Fjall;
#[cfg(feature = "fjall")]
impl Backend for Fjall {
    type S = slipstream::FjallSnapshot;
    const NAME: &'static str = "fjall";
    fn open(path: &Path) -> (WatchCursor, Self::S) {
        slipstream::FjallSnapshot::open(path, Self::cfg()).unwrap()
    }
    fn import(artifact: &Path, dest: &Path) -> (WatchCursor, Self::S) {
        slipstream::FjallSnapshot::import(artifact, dest, Self::cfg()).unwrap()
    }
}
#[cfg(feature = "fjall")]
impl Fjall {
    fn cfg() -> slipstream::FjallConfig {
        slipstream::FjallConfig {
            sync: false,
            cache_size_bytes: 4 << 20,
        }
    }
}

#[cfg(feature = "rocksdb")]
struct Rocks;
#[cfg(feature = "rocksdb")]
impl Backend for Rocks {
    type S = slipstream::RocksDbSnapshot;
    const NAME: &'static str = "rocksdb";
    fn open(path: &Path) -> (WatchCursor, Self::S) {
        slipstream::RocksDbSnapshot::open(path, Self::cfg()).unwrap()
    }
    fn import(artifact: &Path, dest: &Path) -> (WatchCursor, Self::S) {
        slipstream::RocksDbSnapshot::import(artifact, dest, Self::cfg()).unwrap()
    }
}
#[cfg(feature = "rocksdb")]
impl Rocks {
    fn cfg() -> slipstream::RocksDbConfig {
        slipstream::RocksDbConfig {
            sync: false,
            cache_size_bytes: 4 << 20,
        }
    }
}

// --- The simulated log ----------------------------------------------------

#[derive(Clone, Debug)]
struct Event {
    rev: u64,
    key: &'static str,
    value: Option<&'static str>,
}

/// A mid-watch floor-guard trip: the first `*_from` call delivers `deliver`
/// messages; then, while the watch is behind, the bucket moves on (`append`)
/// and retention overruns the consumer (`evict_to` on an evicting bucket, a
/// marker purge on one that keeps current values); the watch ends with
/// `CursorExpired`.
#[derive(Clone, Debug)]
struct Trip {
    deliver: usize,
    append: Vec<Event>,
    evict_to: Option<u64>,
    purge_markers: bool,
}

#[derive(Debug)]
struct LogState {
    events: Vec<Event>,
    evicts_current: bool,
    /// Revisions <= floor are head-evicted (evicting buckets only).
    floor: u64,
    /// Delete markers removed by an admin purge.
    purged: BTreeSet<u64>,
    trip: Option<Trip>,
    /// Yield between messages, so the main loop ingests them before a repair
    /// request arrives (the other delivery order).
    yield_between: bool,
}

impl LogState {
    fn head(&self) -> u64 {
        self.events.last().map_or(0, |e| e.rev)
    }

    /// The stream's retained messages: the latest per key (history 1), minus
    /// head eviction and purged markers, in revision order.
    fn retained(&self) -> Vec<Event> {
        let mut latest: BTreeMap<&str, &Event> = BTreeMap::new();
        for e in &self.events {
            latest.insert(e.key, e);
        }
        let mut out: Vec<Event> = latest
            .into_values()
            .filter(|e| !(self.evicts_current && e.rev <= self.floor))
            .filter(|e| !(e.value.is_none() && self.purged.contains(&e.rev)))
            .cloned()
            .collect();
        out.sort_by_key(|e| e.rev);
        out
    }

    fn first_revision(&self) -> u64 {
        self.retained().first().map_or(self.head() + 1, |e| e.rev)
    }

    fn apply_trip(&mut self, trip: Trip) {
        self.events.extend(trip.append);
        if let Some(rev) = trip.evict_to {
            self.floor = self.floor.max(rev);
        }
        if trip.purge_markers {
            for e in &self.events {
                if e.value.is_none() {
                    self.purged.insert(e.rev);
                }
            }
        }
    }
}

fn in_scope(prefixes: &[String], key: &str) -> bool {
    prefixes.iter().any(|p| key.starts_with(p.as_str()))
}

fn to_update(e: &Event) -> KvUpdate {
    let version = VersionToken::from_u64(e.rev);
    match e.value {
        Some(v) => KvUpdate::Put(KvEntry {
            key: e.key.to_string(),
            value: v.as_bytes().to_vec(),
            version,
        }),
        None => KvUpdate::Delete {
            key: e.key.to_string(),
            version,
        },
    }
}

/// THE TRUTH at `cursor`: every write minus every real delete.
fn truth(events: &[Event], cursor: u64) -> BTreeMap<String, (Vec<u8>, u64)> {
    let mut out = BTreeMap::new();
    for e in events.iter().filter(|e| e.rev <= cursor) {
        match e.value {
            Some(v) => {
                out.insert(e.key.to_string(), (v.as_bytes().to_vec(), e.rev));
            }
            None => {
                out.remove(e.key);
            }
        }
    }
    out
}

struct SimLog(Mutex<LogState>);

impl SimLog {
    async fn deliver(&self, msgs: Vec<Event>, tx: &Sender<KvUpdate>) {
        let yield_between = self.0.lock().unwrap().yield_between;
        for m in msgs {
            if tx.send(to_update(&m)).await.is_err() {
                return;
            }
            if yield_between {
                tokio::task::yield_now().await;
            }
        }
    }

    async fn from(
        &self,
        prefixes: &[String],
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let c = cursor.as_u64().unwrap_or(0);
        let (msgs, trip) = {
            let mut st = self.0.lock().unwrap();
            if !resume_window_ok(c, st.first_revision()) {
                return Err(KvError::CursorExpired);
            }
            let msgs: Vec<Event> = st
                .retained()
                .into_iter()
                .filter(|e| e.rev > c && in_scope(prefixes, e.key))
                .collect();
            (msgs, st.trip.take())
        };
        match trip {
            Some(trip) => {
                self.deliver(msgs.into_iter().take(trip.deliver).collect(), &tx)
                    .await;
                self.0.lock().unwrap().apply_trip(trip);
                Err(KvError::CursorExpired)
            }
            None => {
                self.deliver(msgs, &tx).await;
                Ok(())
            }
        }
    }

    async fn full(&self, prefixes: &[String], tx: Sender<KvUpdate>) -> Result<(), KvError> {
        let msgs: Vec<Event> = {
            let st = self.0.lock().unwrap();
            st.retained()
                .into_iter()
                .filter(|e| in_scope(prefixes, e.key))
                .collect()
        };
        self.deliver(msgs, &tx).await;
        Ok(())
    }
}

fn owned(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| s.to_string()).collect()
}

#[async_trait]
impl KvWatcher for SimLog {
    async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.full(&owned(&[""]), tx).await
    }
    async fn watch_prefix(&self, prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.full(&owned(&[prefix]), tx).await
    }
    async fn watch_prefixes(&self, prefixes: &[&str], tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.full(&owned(prefixes), tx).await
    }
    async fn watch_all_from(&self, c: &WatchCursor, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.from(&owned(&[""]), c, tx).await
    }
    async fn watch_prefix_from(
        &self,
        prefix: &str,
        c: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        self.from(&owned(&[prefix]), c, tx).await
    }
    async fn watch_prefixes_from(
        &self,
        prefixes: &[&str],
        c: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        self.from(&owned(prefixes), c, tx).await
    }
    async fn retention(&self) -> Result<Option<Retention>, KvError> {
        let st = self.0.lock().unwrap();
        Ok(Some(Retention {
            evicts_current_values: st.evicts_current,
            first_revision: st.first_revision(),
        }))
    }
}

#[async_trait]
impl KvReader for SimLog {
    async fn get(&self, _k: &str) -> Result<Option<KvEntry>, KvError> {
        unreachable!("repairs only list keys")
    }
    async fn entry(&self, _k: &str) -> Result<Option<KvEntry>, KvError> {
        unreachable!("repairs only list keys")
    }
    async fn keys(&self, prefix: &str) -> Result<Vec<String>, KvError> {
        let st = self.0.lock().unwrap();
        Ok(st
            .retained()
            .into_iter()
            .filter(|e| e.value.is_some() && e.key.starts_with(prefix))
            .map(|e| e.key.to_string())
            .collect())
    }
    async fn scan(&self, _p: &str) -> Result<Vec<KvEntry>, KvError> {
        unreachable!("repairs only list keys")
    }
}

// --- The fault store ------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Crash {
    /// Dies before anything of this apply is durable.
    Before,
    /// Dies after the apply is fully durable, before returning.
    After,
    /// The batch's data is durable, its cursor is not.
    Torn,
}

/// Faults for one run: store-apply call indices to fail transiently, and an
/// optional crash.
#[derive(Clone, Debug, Default)]
struct RunFaults {
    transient: Vec<usize>,
    crash: Option<(usize, Crash)>,
}

impl RunFaults {
    fn is_empty(&self) -> bool {
        self.transient.is_empty() && self.crash.is_none()
    }
}

const CRASH_MSG: &str = "simulated crash (repair_dst)";

struct FaultStore<S> {
    inner: S,
    faults: RunFaults,
    calls: Arc<AtomicUsize>,
}

impl<S: SnapshotStore> SnapshotStore for FaultStore<S> {
    fn load(_path: &Path) -> Result<(WatchCursor, Self), SnapshotError> {
        unreachable!("constructed directly")
    }
    fn apply(&mut self, batch: &[KvUpdate], cursor: &WatchCursor) -> Result<(), SnapshotError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some((at, kind)) = self.faults.crash
            && at == call
        {
            match kind {
                Crash::Before => {}
                Crash::After => self.inner.apply(batch, cursor)?,
                Crash::Torn => {
                    let old = self.inner.cursor();
                    self.inner.apply(batch, &old)?;
                }
            }
            panic!("{CRASH_MSG}");
        }
        if self.faults.transient.contains(&call) {
            return Err(SnapshotError::Backend("injected transient failure".into()));
        }
        self.inner.apply(batch, cursor)
    }
    fn get(&self, key: &str) -> Result<Option<KvEntry>, SnapshotError> {
        self.inner.get(key)
    }
    fn range(&self, prefix: &str) -> Result<Vec<KvEntry>, SnapshotError> {
        self.inner.range(prefix)
    }
    fn for_each_in_range(
        &self,
        prefix: &str,
        f: impl FnMut(KvEntry) -> Result<(), SnapshotError>,
    ) -> Result<(), SnapshotError> {
        self.inner.for_each_in_range(prefix, f)
    }
    fn cursor(&self) -> WatchCursor {
        self.inner.cursor()
    }
    fn export_to(&mut self, dest: &Path) -> Result<ExportManifest, SnapshotError> {
        self.inner.export_to(dest)
    }
}

// --- The artifact source --------------------------------------------------

/// The newest published artifact: a correct exporter's fold at `cursor` (an
/// exporter that saw the whole log, so it reads the log's events). Built on
/// the backend under test, like a real `ArtifactRestore` import.
struct SimRestore<B> {
    log: Arc<SimLog>,
    cursor: u64,
    /// How the exporter came to its fold: `None` — complete (the truth at
    /// `cursor`); `Some(e)` — it held the truth at `e`, then caught up
    /// through `cursor` by a resume (latest message per key, superseded
    /// revisions skipped); `Some(0)` is a fresh full re-list. Mid-catch-up
    /// artifacts lack keys whose latest write is after `cursor`, and hold
    /// older values for keys whose intermediate revisions were superseded.
    exporter_from: Option<u64>,
    dir: PathBuf,
    fetches: AtomicUsize,
    _b: std::marker::PhantomData<fn() -> B>,
}

impl<B> SimRestore<B> {
    fn manifest(&self) -> ExportManifest {
        ExportManifest {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            backend: "sim".into(),
            backend_version: "0".into(),
            cursor: WatchCursor::from_u64(self.cursor),
            created_at_unix: 0,
            files: vec![],
            scope: Some(vec![String::new()]),
        }
    }
}

#[async_trait]
impl<B: Backend> RestoreSource<FaultStore<B::S>> for SimRestore<B> {
    async fn latest(&self) -> Result<ExportManifest, SnapshotError> {
        Ok(self.manifest())
    }
    async fn fetch(&self) -> Result<RestoredFold<FaultStore<B::S>>, SnapshotError> {
        let n = self.fetches.fetch_add(1, Ordering::SeqCst);
        let (_c, mut fold) = B::open(&self.dir.join(format!("artifact-{n}")));
        let events = self.log.0.lock().unwrap().events.clone();
        let state = match self.exporter_from {
            None => truth(&events, self.cursor),
            Some(e) => {
                let mut state = truth(&events, e);
                let mut latest: BTreeMap<&str, &Event> = BTreeMap::new();
                for ev in &events {
                    latest.insert(ev.key, ev);
                }
                for ev in latest
                    .into_values()
                    .filter(|ev| ev.rev > e && ev.rev <= self.cursor)
                {
                    match ev.value {
                        Some(v) => {
                            state.insert(ev.key.to_string(), (v.as_bytes().to_vec(), ev.rev));
                        }
                        None => {
                            state.remove(ev.key);
                        }
                    }
                }
                state
            }
        };
        let updates: Vec<KvUpdate> = state
            .into_iter()
            .map(|(key, (value, rev))| {
                KvUpdate::Put(KvEntry {
                    key,
                    value,
                    version: VersionToken::from_u64(rev),
                })
            })
            .collect();
        fold.apply(&updates, &WatchCursor::from_u64(self.cursor))?;
        Ok(RestoredFold::new(
            self.manifest(),
            FaultStore {
                inner: fold,
                faults: RunFaults::default(),
                calls: Arc::new(AtomicUsize::new(0)),
            },
        ))
    }
}

// --- Scenarios ------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Mode {
    Auto,
    Relist,
}

#[derive(Clone, Debug)]
struct Scenario {
    name: &'static str,
    events: Vec<Event>,
    /// The fold's starting state: every event up to here (`None`: a fresh
    /// node with no cursor and an empty fold).
    local_at: Option<u64>,
    /// Seed `local_at`'s data WITHOUT its cursor: a torn first checkpoint.
    unanchored: bool,
    evicts_current: bool,
    floor: u64,
    purged: Vec<u64>,
    trip: Option<Trip>,
    artifact_at: u64,
    /// See `SimRestore::exporter_from`.
    exporter_from: Option<u64>,
    scope: WatchScope,
    mode: Mode,
}

impl Scenario {
    fn prefixes(&self) -> Vec<String> {
        match &self.scope {
            WatchScope::All => vec![String::new()],
            WatchScope::Prefix(p) => vec![p.clone()],
            WatchScope::Prefixes(ps) => ps.clone(),
        }
    }
}

fn ev(rev: u64, key: &'static str, value: Option<&'static str>) -> Event {
    Event { rev, key, value }
}

/// k1..k3 written at 1..3; the local fold has them at cursor 3. Then, while
/// the node is away: an update, a real delete, two new keys, a tail write.
fn evicting_log() -> Vec<Event> {
    vec![
        ev(1, "n.k1", Some("a")),
        ev(2, "n.k2", Some("b")),
        ev(3, "n.k3", Some("c")),
        ev(4, "n.k2", Some("b2")),
        ev(5, "n.k1", None),
        ev(6, "n.k4", Some("d")),
        ev(7, "n.k5", Some("e")),
        ev(8, "n.k6", Some("f")),
    ]
}

/// Three key families: two in a multi-prefix scope (n., m.), one outside
/// (x.), interleaved.
fn multi_log() -> Vec<Event> {
    vec![
        ev(1, "n.k1", Some("a")),
        ev(2, "m.k1", Some("b")),
        ev(3, "x.k1", Some("out")),
        ev(4, "m.k1", None),
        ev(5, "x.k1", Some("out2")),
        ev(6, "n.k2", Some("c")),
        ev(7, "m.k2", Some("d")),
        ev(8, "n.k1", Some("a2")),
    ]
}

/// The normal path: a resume inside the window with an update, a delete, a
/// re-create, a create-then-delete, and a tail.
fn churn_log() -> Vec<Event> {
    vec![
        ev(1, "n.k1", Some("a")),
        ev(2, "n.k2", Some("b")),
        ev(3, "n.k3", Some("c")),
        ev(4, "n.k1", Some("a2")),
        ev(5, "n.k2", None),
        ev(6, "n.k2", Some("b3")),
        ev(7, "n.tmp", Some("t")),
        ev(8, "n.tmp", None),
        ev(9, "n.k3", Some("c2")),
    ]
}

/// Keys created or updated just before the artifact's cursor (7) and
/// written again after it: a mid-catch-up exporter at 7 never saw their
/// pre-7 revisions, while the local fold (at 3, or with n.k2's 5 via a
/// torn tail) did.
fn straddle_log() -> Vec<Event> {
    vec![
        ev(1, "n.k1", Some("a")),
        ev(2, "n.k2", Some("b")),
        ev(3, "n.k3", Some("c")),
        ev(4, "n.k3", Some("c2")),
        ev(5, "n.k4", Some("d")),
        ev(6, "n.k5", Some("e")),
        ev(7, "n.k6", Some("f")),
        ev(8, "n.k3", Some("c3")),
        ev(9, "n.k4", Some("d2")),
        ev(10, "n.k1", None),
    ]
}

fn relist_log() -> Vec<Event> {
    vec![
        ev(1, "n.k1", Some("a")),
        ev(2, "n.k2", Some("b")),
        ev(3, "n.k3", Some("c")),
        ev(4, "n.new", Some("recreated")),
        ev(5, "n.new", None),
        ev(6, "n.k1", Some("a2")),
        ev(7, "n.k2", Some("b2")),
        ev(8, "n.k3", Some("c2")),
        ev(9, "n.k4", Some("d")),
    ]
}

fn relist_trip() -> Trip {
    Trip {
        deliver: 1,
        append: relist_log()[4..].to_vec(),
        evict_to: None,
        purge_markers: true,
    }
}

fn evict_trip(deliver: usize, to: u64) -> Trip {
    Trip {
        deliver,
        append: vec![],
        evict_to: Some(to),
        purge_markers: false,
    }
}

fn base(name: &'static str, events: Vec<Event>) -> Scenario {
    Scenario {
        name,
        events,
        local_at: Some(3),
        unanchored: false,
        evicts_current: false,
        floor: 0,
        purged: vec![],
        trip: None,
        artifact_at: 0,
        exporter_from: None,
        scope: WatchScope::All,
        mode: Mode::Auto,
    }
}

fn scenarios() -> Vec<Scenario> {
    let evicting = |name, trip, floor, scope| Scenario {
        evicts_current: true,
        floor,
        trip,
        artifact_at: 7,
        scope,
        ..base(name, evicting_log())
    };
    vec![
        // --- Expiry repairs ---
        // Resume finds the cursor expired: k3, the k2 update, k4 all aged out
        // (never deleted); the k1 delete marker too. Restore from 7.
        evicting(
            "evicting: resume expired → restore",
            None,
            6,
            WatchScope::All,
        ),
        // A live watch delivers 4 and 5, then retention overruns it.
        evicting(
            "evicting: floor-guard trip mid-watch → restore",
            Some(evict_trip(2, 6)),
            3,
            WatchScope::All,
        ),
        evicting(
            "evicting: prefix scope, trip → restore",
            Some(evict_trip(1, 6)),
            3,
            WatchScope::Prefix("n.".into()),
        ),
        Scenario {
            local_at: None,
            ..evicting("evicting: fresh start → restore", None, 6, WatchScope::All)
        },
        Scenario {
            unanchored: true,
            ..evicting(
                "evicting: data but no cursor → restore",
                None,
                6,
                WatchScope::All,
            )
        },
        // Two prefixes on one consumer, x.* out of scope; restore after a
        // trip. The local fold also holds x.k1 (out of scope, untouched).
        Scenario {
            name: "evicting: multi-prefix, trip → restore",
            events: multi_log(),
            evicts_current: true,
            floor: 3,
            trip: Some(evict_trip(1, 6)),
            artifact_at: 7,
            scope: WatchScope::Prefixes(vec!["n.".into(), "m.".into()]),
            ..base("", vec![])
        },
        // The artifact was exported while its exporter was still catching
        // up: a fresh full re-list (from 0), or a resume from 2. It lacks
        // keys whose latest write is after its cursor and holds superseded
        // values for others; the resume from its cursor completes them.
        Scenario {
            exporter_from: Some(0),
            ..evicting(
                "evicting: trip → restore from a re-list-hydrating artifact",
                Some(evict_trip(2, 6)),
                3,
                WatchScope::All,
            )
        },
        Scenario {
            name: "evicting: expired → restore from a mid-catch-up artifact",
            events: straddle_log(),
            evicts_current: true,
            floor: 6,
            artifact_at: 7,
            exporter_from: Some(2),
            ..base("", vec![])
        },
        Scenario {
            name: "keeps-current: resume expired → relist",
            purged: vec![5],
            mode: Mode::Relist,
            ..base("", relist_log())
        },
        // THE RE-QUEUE BUG's shape: the live watch delivers the re-create at
        // 4; then the key is deleted again, everything else superseded, and
        // the marker purged, overrunning the watch.
        Scenario {
            name: "keeps-current: trip mid-watch → relist",
            trip: Some(relist_trip()),
            mode: Mode::Relist,
            ..base("", relist_log()[..4].to_vec())
        },
        Scenario {
            name: "keeps-current: trip mid-watch → auto picks relist",
            trip: Some(relist_trip()),
            artifact_at: 9,
            ..base("", relist_log()[..4].to_vec())
        },
        Scenario {
            name: "keeps-current: data but no cursor → relist",
            local_at: Some(4),
            unanchored: true,
            purged: vec![5],
            ..base("", relist_log())
        },
        // Overlapping prefixes (a key matched twice) on a key-listing repair.
        Scenario {
            name: "keeps-current: overlapping prefixes, expired → relist",
            purged: vec![5],
            scope: WatchScope::Prefixes(vec!["n.".into(), "n.k".into()]),
            mode: Mode::Relist,
            ..base("", relist_log())
        },
        // --- The normal data path (no expiry) ---
        Scenario {
            name: "keeps-current: resume inside the window, churn",
            ..base("", churn_log())
        },
        // Evicting bucket, resume inside the window; k3 (at 3) has aged out
        // of NATS but the live fold keeps it — never deleted.
        Scenario {
            name: "evicting: resume inside the window keeps an aged-out key",
            evicts_current: true,
            floor: 3,
            artifact_at: 7,
            ..base("", evicting_log())
        },
        Scenario {
            name: "keeps-current: fresh full re-list",
            local_at: None,
            ..base("", churn_log())
        },
        Scenario {
            name: "keeps-current: multi-prefix resume inside the window",
            scope: WatchScope::Prefixes(vec!["n.".into(), "m.".into()]),
            ..base("", multi_log())
        },
    ]
}

// --- The harness ----------------------------------------------------------

struct World<B> {
    log: Arc<SimLog>,
    restore: Arc<SimRestore<B>>,
    path: PathBuf,
    dir: tempfile::TempDir,
    /// The out-of-scope part of the fold at the start (must never change).
    out_of_scope: BTreeMap<String, Vec<u8>>,
}

fn world<B: Backend>(sc: &Scenario, yield_between: bool) -> World<B> {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fold");
    let (_c, mut fold) = B::open(&path);
    if let Some(at) = sc.local_at {
        let seed: Vec<KvUpdate> = sc
            .events
            .iter()
            .filter(|e| e.rev <= at)
            .map(to_update)
            .collect();
        let cursor = if sc.unanchored {
            WatchCursor::none()
        } else {
            WatchCursor::from_u64(at)
        };
        fold.apply(&seed, &cursor).unwrap();
    }
    let prefixes = sc.prefixes();
    let out_of_scope = fold
        .range("")
        .unwrap()
        .into_iter()
        .filter(|e| !in_scope(&prefixes, &e.key))
        .map(|e| (e.key, e.value))
        .collect();
    drop(fold);
    let log = Arc::new(SimLog(Mutex::new(LogState {
        events: sc.events.clone(),
        evicts_current: sc.evicts_current,
        floor: sc.floor,
        purged: sc.purged.iter().copied().collect(),
        trip: sc.trip.clone(),
        yield_between,
    })));
    let restore = Arc::new(SimRestore {
        log: Arc::clone(&log),
        cursor: sc.artifact_at,
        exporter_from: sc.exporter_from,
        dir: dir.path().to_path_buf(),
        fetches: AtomicUsize::new(0),
        _b: std::marker::PhantomData,
    });
    World {
        log,
        restore,
        path,
        dir,
        out_of_scope,
    }
}

fn repair<B: Backend>(sc: &Scenario, w: &World<B>) -> ExpiryRepair<FaultStore<B::S>> {
    match sc.mode {
        Mode::Relist => ExpiryRepair::Relist(Arc::clone(&w.log) as Arc<dyn KvReader>),
        Mode::Auto => ExpiryRepair::Auto {
            reader: Arc::clone(&w.log) as Arc<dyn KvReader>,
            restore: Arc::clone(&w.restore) as Arc<dyn RestoreSource<FaultStore<B::S>>>,
        },
    }
}

enum RunEnd {
    Clean,
    Crashed,
}

type Exports = Arc<
    Mutex<
        Vec<(
            oneshot::Receiver<Result<ExportManifest, SnapshotError>>,
            PathBuf,
        )>,
    >,
>;

struct RunResult {
    end: RunEnd,
    reported: Vec<u64>,
    domain: HashMap<String, Vec<u8>>,
    /// Keys whose revision `apply` saw go backward.
    regressions: Vec<String>,
    /// Keys `apply` saw deleted by a repair (revisionless delete).
    synthetic_deletes: Vec<String>,
    /// Store-apply calls this run made.
    calls: usize,
}

/// One process lifetime: open the fold, rebuild domain state from it, run
/// `watch_applied` to the end of the (simulated) stream or a crash. With
/// `export`, the first `on_applied` of the run requests an export.
async fn run_once<B: Backend>(
    sc: &Scenario,
    w: &World<B>,
    faults: RunFaults,
    max: usize,
    export: bool,
    exports: &Exports,
    run: usize,
) -> RunResult {
    let (cursor, inner) = B::open(&w.path);
    let start = inner.range("").unwrap();
    let domain: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(
        start
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect(),
    ));
    // The highest revision `apply` has seen per key (seeded from the fold).
    let seen_rev: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(
        start
            .iter()
            .filter_map(|e| Some((e.key.clone(), e.version.as_u64()?)))
            .collect(),
    ));
    let regressions = Arc::new(Mutex::new(Vec::new()));
    let synthetic = Arc::new(Mutex::new(Vec::new()));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let store = FaultStore {
        inner,
        faults,
        calls: Arc::clone(&calls),
    };
    let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let (ex_tx, ex_rx) = mpsc::channel::<ExportRequest>(4);
    let (d, r) = (Arc::clone(&domain), Arc::clone(&reported));
    let (seen, regr, synth) = (
        Arc::clone(&seen_rev),
        Arc::clone(&regressions),
        Arc::clone(&synthetic),
    );
    let (exports_c, export_dir) = (
        Arc::clone(exports),
        w.dir.path().join(format!("export-{run}")),
    );
    let mut requested = false;
    let res = watch_applied(
        Arc::clone(&w.log) as Arc<dyn KvWatcher>,
        sc.scope.clone(),
        (!cursor.is_none()).then_some(cursor),
        repair(sc, w),
        Some(store),
        Some(ex_rx),
        BatchConfig {
            window: std::time::Duration::from_secs(3600),
            max,
            ..BatchConfig::default()
        },
        |u: &KvUpdate| {
            Some(match u {
                KvUpdate::Put(e) => (e.key.clone(), Some(e.value.clone()), e.version.as_u64()),
                KvUpdate::Delete { key, version } | KvUpdate::Purge { key, version } => {
                    (key.clone(), None, version.as_u64())
                }
            })
        },
        move |batch: Vec<(String, Option<Vec<u8>>, Option<u64>)>| {
            let mut d = d.lock().unwrap();
            let mut seen = seen.lock().unwrap();
            for (k, v, rev) in batch {
                match rev {
                    Some(rev) => {
                        let max = seen.entry(k.clone()).or_insert(0);
                        if rev < *max {
                            regr.lock().unwrap().push(format!("{k}: {max} → {rev}"));
                        }
                        *max = (*max).max(rev);
                    }
                    None if v.is_none() => synth.lock().unwrap().push(k.clone()),
                    None => {}
                }
                match v {
                    Some(v) => {
                        d.insert(k, v);
                    }
                    None => {
                        d.remove(&k);
                    }
                }
            }
        },
        move |c: WatchCursor| {
            r.lock().unwrap().push(c.as_u64().unwrap());
            if export && !requested {
                requested = true;
                let (reply, rx) = oneshot::channel();
                let dest = export_dir.join("artifact");
                std::fs::create_dir_all(&export_dir).unwrap();
                if ex_tx
                    .try_send(ExportRequest {
                        dest_dir: dest.clone(),
                        reply,
                    })
                    .is_ok()
                {
                    exports_c.lock().unwrap().push((rx, dest));
                }
            }
        },
        sd_rx,
    )
    .await;
    let reported = reported.lock().unwrap().clone();
    let domain = domain.lock().unwrap().clone();
    let end = match res {
        Ok(_) => RunEnd::Clean,
        // A crash, or the fail-stop after a persistent store failure: either
        // way the process restarts.
        Err(KvError::WatchError(msg))
            if msg.contains("panicked") || msg.contains("consecutive times") =>
        {
            RunEnd::Crashed
        }
        Err(e) => panic!("[{}] unexpected watch error: {e}", sc.name),
    };
    RunResult {
        end,
        reported,
        domain,
        regressions: regressions.lock().unwrap().clone(),
        synthetic_deletes: synthetic.lock().unwrap().clone(),
        calls: calls.load(Ordering::SeqCst),
    }
}

/// The artifact plus a resume from its cursor reaches the truth: applying,
/// on top of `fold`, the latest message per in-scope key written after
/// `cursor` yields every write minus every real delete. This is what a
/// NATS cursor means (every RETAINED message at or below it is applied —
/// not "the truth at the cursor": a key whose latest write is after it
/// hasn't been delivered yet); the restore's freshness check guarantees the
/// tail is retained.
fn replay_complete(
    events: &[Event],
    prefixes: &[String],
    fold: &BTreeMap<String, Vec<u8>>,
    cursor: u64,
    head: u64,
) -> Result<(), String> {
    let mut state: BTreeMap<String, Vec<u8>> = fold
        .iter()
        .filter(|(k, _)| in_scope(prefixes, k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut latest: BTreeMap<&str, &Event> = BTreeMap::new();
    for e in events.iter().filter(|e| e.rev <= head) {
        latest.insert(e.key, e);
    }
    for e in latest
        .into_values()
        .filter(|e| e.rev > cursor && in_scope(prefixes, e.key))
    {
        match e.value {
            Some(v) => {
                state.insert(e.key.to_string(), v.as_bytes().to_vec());
            }
            None => {
                state.remove(e.key);
            }
        }
    }
    let want: BTreeMap<String, Vec<u8>> = truth(events, head)
        .into_iter()
        .filter(|(k, _)| in_scope(prefixes, k))
        .map(|(k, (v, _))| (k, v))
        .collect();
    if state == want {
        Ok(())
    } else {
        Err(format!(
            "artifact + tail after {cursor} = {state:?}, truth = {want:?}"
        ))
    }
}

#[derive(Clone, Copy)]
struct Cfg {
    yield_between: bool,
    max: usize,
    export: bool,
}

/// Drive a schedule (faults per run) to convergence and check the outcome.
/// Returns the number of store-apply calls the first run made.
async fn check_schedule<B: Backend>(sc: &Scenario, cfg: Cfg, schedule: &[RunFaults]) -> usize {
    let w = world::<B>(sc, cfg.yield_between);
    let ctx = format!(
        "[{} / {}] yield={} max={} schedule={schedule:?}",
        B::NAME,
        sc.name,
        cfg.yield_between,
        cfg.max
    );
    let prefixes = sc.prefixes();
    let exports: Exports = Arc::new(Mutex::new(Vec::new()));
    let mut last_cursor = B::open(&w.path).0.as_u64().unwrap_or(0);
    let mut first_calls = None;
    let mut run = 0usize;
    loop {
        let faults = schedule.get(run).cloned().unwrap_or_default();
        let faultless = faults.is_empty() && run >= schedule.len();
        let res = run_once::<B>(sc, &w, faults, cfg.max, cfg.export, &exports, run).await;
        first_calls.get_or_insert(res.calls);
        assert!(
            res.reported.windows(2).all(|p| p[0] <= p[1]),
            "{ctx}: on_applied went backward within a run: {:?}",
            res.reported
        );
        // The consumer never sees a key's revision go backward, and a repair
        // never deletes a key that is live in the log (the log is static
        // once a repair can run).
        assert!(
            res.regressions.is_empty(),
            "{ctx}: apply saw revisions go backward: {:?}",
            res.regressions
        );
        {
            let st = w.log.0.lock().unwrap();
            let live = truth(&st.events, st.head());
            let phantom: Vec<&String> = res
                .synthetic_deletes
                .iter()
                .filter(|k| live.contains_key(k.as_str()))
                .collect();
            assert!(
                phantom.is_empty(),
                "{ctx}: a repair deleted keys that are live: {phantom:?}"
            );
        }
        let (cursor, fold) = B::open(&w.path);
        let cursor = cursor.as_u64().unwrap_or(0);
        assert!(
            cursor >= last_cursor,
            "{ctx}: the fold's cursor went backward across run {run}: {last_cursor} → {cursor}"
        );
        last_cursor = cursor;
        run += 1;
        assert!(run < 20, "{ctx}: no convergence after {run} runs");
        if !(faultless && matches!(res.end, RunEnd::Clean)) {
            drop(fold);
            continue;
        }

        // Converged run: judge it.
        let (head, events) = {
            let st = w.log.0.lock().unwrap();
            (st.head(), st.events.clone())
        };
        let want: BTreeMap<String, (Vec<u8>, u64)> = truth(&events, head)
            .into_iter()
            .filter(|(k, _)| in_scope(&prefixes, k))
            .collect();
        let all: Vec<KvEntry> = fold.range("").unwrap();
        let got: BTreeMap<String, (Vec<u8>, u64)> = all
            .iter()
            .filter(|e| in_scope(&prefixes, &e.key))
            .map(|e| {
                (
                    e.key.clone(),
                    (e.value.clone(), e.version.as_u64().unwrap()),
                )
            })
            .collect();
        assert_eq!(
            got, want,
            "{ctx}: the fold is not every write minus every real delete"
        );
        let in_scope_head = events
            .iter()
            .filter(|e| in_scope(&prefixes, e.key))
            .map(|e| e.rev)
            .max()
            .unwrap_or(0);
        assert!(
            cursor >= in_scope_head && cursor <= head,
            "{ctx}: cursor {cursor} short of the scope's head {in_scope_head}"
        );
        let out: BTreeMap<String, Vec<u8>> = all
            .iter()
            .filter(|e| !in_scope(&prefixes, &e.key))
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect();
        assert_eq!(out, w.out_of_scope, "{ctx}: out-of-scope keys changed");
        let fold_map: HashMap<String, Vec<u8>> = all
            .iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect();
        assert_eq!(
            res.domain, fold_map,
            "{ctx}: the domain state (apply) diverged from the fold"
        );
        drop(fold);

        // Every export that completed: scope recorded, never behind its cursor.
        let pending = std::mem::take(&mut *exports.lock().unwrap());
        for (mut rx, artifact) in pending {
            let Ok(Ok(manifest)) = rx.try_recv() else {
                continue;
            };
            assert_eq!(
                manifest.scope.as_ref(),
                Some(&prefixes),
                "{ctx}: export scope"
            );
            let dest = artifact.with_file_name("imported");
            let (c, imported) = B::import(&artifact, &dest);
            assert_eq!(c, manifest.cursor, "{ctx}: imported cursor");
            let contents: BTreeMap<String, Vec<u8>> = imported
                .range("")
                .unwrap()
                .into_iter()
                .map(|e| (e.key, e.value))
                .collect();
            replay_complete(&events, &prefixes, &contents, c.as_u64().unwrap_or(0), head)
                .unwrap_or_else(|e| {
                    panic!("{ctx}: exported artifact can't resume to the truth: {e}")
                });
        }
        return first_calls.unwrap();
    }
}

fn quiet_simulated_crashes() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let msg = info
                .payload()
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| info.payload().downcast_ref::<&str>().copied())
                .unwrap_or("");
            if !msg.contains(CRASH_MSG) {
                default(info);
            }
        }));
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Depth {
    /// Every ≤2-transient set, single and paired crashes, transient + crash.
    Full,
    /// Every single fault.
    Single,
}

/// Every schedule for one scenario configuration. Returns how many ran.
async fn exhaust<B: Backend>(sc: &Scenario, cfg: Cfg, depth: Depth) -> usize {
    let k = check_schedule::<B>(sc, cfg, &[]).await;
    // A crash index can also land past the first run's calls (a later run —
    // e.g. a restore re-run after a crash — makes more): probe beyond.
    let calls: Vec<usize> = (0..=k).collect();
    let later_calls: Vec<usize> = (0..=k + 4).collect();
    let kinds = [Crash::Before, Crash::After, Crash::Torn];
    let mut schedules: Vec<Vec<RunFaults>> = Vec::new();
    for a in &calls {
        schedules.push(vec![RunFaults {
            transient: vec![*a],
            crash: None,
        }]);
        for kind in kinds {
            schedules.push(vec![RunFaults {
                transient: vec![],
                crash: Some((*a, kind)),
            }]);
        }
        if depth == Depth::Single {
            continue;
        }
        for b in calls.iter().filter(|b| *b > a) {
            schedules.push(vec![RunFaults {
                transient: vec![*a, *b],
                crash: None,
            }]);
        }
        for kind in kinds {
            let crash = RunFaults {
                transient: vec![],
                crash: Some((*a, kind)),
            };
            for b in &later_calls {
                for kind2 in kinds {
                    schedules.push(vec![
                        crash.clone(),
                        RunFaults {
                            transient: vec![],
                            crash: Some((*b, kind2)),
                        },
                    ]);
                }
            }
            for t in calls.iter().filter(|t| *t < a) {
                schedules.push(vec![RunFaults {
                    transient: vec![*t],
                    crash: Some((*a, kind)),
                }]);
            }
        }
    }
    // A persistent store failure: every apply fails until the watch
    // fail-stops; the restart must still converge.
    schedules.push(vec![RunFaults {
        transient: (0..64).collect(),
        crash: None,
    }]);
    for s in &schedules {
        check_schedule::<B>(sc, cfg, s).await;
    }
    schedules.len() + 1
}

async fn exhaust_backend<B: Backend>(depth: Depth, maxes: &[usize], yields: &[bool]) -> usize {
    quiet_simulated_crashes();
    let mut total = 0usize;
    for sc in scenarios() {
        for &yield_between in yields {
            for &max in maxes {
                let cfg = Cfg {
                    yield_between,
                    max,
                    export: true,
                };
                let n = exhaust::<B>(&sc, cfg, depth).await;
                println!(
                    "{} / {} yield={yield_between} max={max}: {n} schedules",
                    B::NAME,
                    sc.name
                );
                total += n;
            }
        }
    }
    println!("{}: {total} schedules", B::NAME);
    total
}

#[tokio::test(flavor = "current_thread")]
async fn append_log_every_fault_schedule_converges() {
    let total = exhaust_backend::<AppendLog>(Depth::Full, &[1, 100], &[false, true]).await;
    assert!(
        total > 20_000,
        "the schedule space is not vacuous ({total})"
    );
}

// The LSM backends: every single fault, every scenario, batches of one (the
// most store applies, so the most crash points) in one delivery order — a
// CI-sized slice. The deep tier adds batches of 100 and the other delivery
// order (~11 min for both backends, dominated by filesystem syncs).

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
async fn fjall_every_single_fault_converges() {
    let total = exhaust_backend::<Fjall>(Depth::Single, &[1], &[false]).await;
    assert!(total > 400, "the schedule space is not vacuous ({total})");
}

#[cfg(feature = "rocksdb")]
#[tokio::test(flavor = "current_thread")]
async fn rocksdb_every_single_fault_converges() {
    let total = exhaust_backend::<Rocks>(Depth::Single, &[1], &[false]).await;
    assert!(total > 400, "the schedule space is not vacuous ({total})");
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "deep tier: ~6 min"]
async fn deep_fjall_every_single_fault_both_orders_both_batch_sizes() {
    exhaust_backend::<Fjall>(Depth::Single, &[1, 100], &[false, true]).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "deep tier: ~6 min"]
async fn deep_rocksdb_every_single_fault_both_orders_both_batch_sizes() {
    exhaust_backend::<Rocks>(Depth::Single, &[1, 100], &[false, true]).await;
}
