//! Deterministic fault injection over the REAL `watch_applied` cursor-expiry
//! repair: every scenario × every fault schedule, against the shipped code.
//!
//! The Stateright models check the repair protocol over abstractions; this
//! checks the implementation. A simulated NATS log (latest message per key,
//! head eviction on an evicting bucket, marker purges on one that keeps
//! current values, an optional mid-watch floor-guard trip) feeds the real
//! `watch_applied` with a real `AppendLogSnapshot` wrapped in a fault store.
//! A schedule injects, at chosen store-apply calls of chosen runs:
//!
//! - a TRANSIENT failure (the batch is re-queued; the watch continues), or
//! - a CRASH — process death — before the apply, after it, or TORN (the
//!   batch's data durable, its cursor not: the append log's torn-write
//!   shape). The run dies there; the harness reopens the fold from disk and
//!   restarts, as a supervisor would.
//!
//! Runs repeat until one completes with no fault left to inject. Then:
//! - the fold equals every write minus every real delete (in scope; the
//!   out-of-scope keys exactly as they started), at the head's cursor;
//! - the consumer's domain state — rebuilt from the fold at each start, then
//!   driven only by `apply` — equals the fold;
//! - the fold's cursor never moved backward across any crash, and
//!   `on_applied` never reported a backward step within a run.
//!
//! Enumerated exhaustively per scenario: no fault; every set of ≤ 2
//! transient failures; every single crash (each call × each crash kind);
//! every pair of crashes in consecutive runs; every transient failure
//! followed by a crash. Each scenario runs in both delivery orders (the
//! watch task's updates reaching the main loop before, or after, its repair
//! request) and with batches of 1 and 100.
//!
//! The bug fixed alongside this harness — a repair diffing a store that
//! hadn't yet absorbed a re-queued batch — is a transient failure on the
//! pre-repair flush here; reverting that fix fails this file.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use slipstream::protocol::resume_window_ok;
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    ARTIFACT_SCHEMA_VERSION, AppendLogSnapshot, BatchConfig, ExpiryRepair, ExportManifest, KvEntry,
    KvError, KvReader, KvUpdate, KvWatcher, RestoreSource, RestoredFold, Retention, VersionToken,
    WatchCursor, WatchScope, watch_applied,
};
use tokio::sync::mpsc::Sender;

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
    /// The first `*_from` call delivers this many messages, applies the trip,
    /// and ends with `CursorExpired` — a floor-guard trip.
    trip: Option<Trip>,
    /// Yield between messages, so the main loop ingests them before the
    /// repair request arrives (the other delivery order).
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
    async fn deliver(&self, msgs: Vec<Event>, tx: &Sender<KvUpdate>) -> bool {
        let yield_between = self.0.lock().unwrap().yield_between;
        for m in msgs {
            if tx.send(to_update(&m)).await.is_err() {
                return false;
            }
            if yield_between {
                tokio::task::yield_now().await;
            }
        }
        true
    }

    async fn from(
        &self,
        prefix: &str,
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
                .filter(|e| e.rev > c && e.key.starts_with(prefix))
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

    async fn full(&self, prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        let msgs: Vec<Event> = {
            let st = self.0.lock().unwrap();
            st.retained()
                .into_iter()
                .filter(|e| e.key.starts_with(prefix))
                .collect()
        };
        self.deliver(msgs, &tx).await;
        Ok(())
    }
}

#[async_trait]
impl KvWatcher for SimLog {
    async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.full("", tx).await
    }
    async fn watch_prefix(&self, prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.full(prefix, tx).await
    }
    async fn watch_prefixes(&self, _p: &[&str], _tx: Sender<KvUpdate>) -> Result<(), KvError> {
        unreachable!("scenarios use All and Prefix")
    }
    async fn watch_all_from(&self, c: &WatchCursor, tx: Sender<KvUpdate>) -> Result<(), KvError> {
        self.from("", c, tx).await
    }
    async fn watch_prefix_from(
        &self,
        prefix: &str,
        c: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        self.from(prefix, c, tx).await
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

struct FaultStore {
    inner: AppendLogSnapshot,
    faults: RunFaults,
    calls: Arc<AtomicUsize>,
}

impl SnapshotStore for FaultStore {
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
    fn cursor(&self) -> WatchCursor {
        self.inner.cursor()
    }
    fn export_to(&mut self, dest: &Path) -> Result<ExportManifest, SnapshotError> {
        self.inner.export_to(dest)
    }
}

// --- The artifact source --------------------------------------------------

/// The newest published artifact: a correct exporter's fold at `cursor` (an
/// exporter that saw the whole log, so it reads the log's events).
struct SimRestore {
    log: Arc<SimLog>,
    cursor: u64,
    dir: PathBuf,
    fetches: AtomicUsize,
}

impl SimRestore {
    fn manifest(&self) -> ExportManifest {
        ExportManifest {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            backend: "append-log".into(),
            backend_version: "2".into(),
            cursor: WatchCursor::from_u64(self.cursor),
            created_at_unix: 0,
            files: vec![],
            scope: Some(vec![String::new()]),
        }
    }
}

#[async_trait]
impl RestoreSource<FaultStore> for SimRestore {
    async fn latest(&self) -> Result<ExportManifest, SnapshotError> {
        Ok(self.manifest())
    }
    async fn fetch(&self) -> Result<RestoredFold<FaultStore>, SnapshotError> {
        let n = self.fetches.fetch_add(1, Ordering::SeqCst);
        let (_c, mut fold) =
            AppendLogSnapshot::open(&self.dir.join(format!("artifact-{n}.snap")), u64::MAX)?;
        let events = self.log.0.lock().unwrap().events.clone();
        let updates: Vec<KvUpdate> = truth(&events, self.cursor)
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
    /// Out-of-scope keys the local fold holds (must survive untouched).
    out_of_scope: Vec<(&'static str, &'static str)>,
    evicts_current: bool,
    floor: u64,
    purged: Vec<u64>,
    trip: Option<Trip>,
    artifact_at: u64,
    scope: WatchScope,
    mode: Mode,
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

fn scenarios() -> Vec<Scenario> {
    vec![
        // Resume finds the cursor expired: k3, the k2 update, k4 all aged out
        // (never deleted); the k1 delete marker too. Restore from 7.
        Scenario {
            name: "evicting: resume expired → restore",
            events: evicting_log(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: true,
            floor: 6,
            purged: vec![],
            trip: None,
            artifact_at: 7,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
        // A live watch delivers 4 and 5, then retention overruns it (head
        // eviction to 6): the floor guard trips mid-watch. Restore from 7.
        Scenario {
            name: "evicting: floor-guard trip mid-watch → restore",
            events: evicting_log(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: true,
            floor: 3,
            purged: vec![],
            trip: Some(Trip {
                deliver: 2,
                append: vec![],
                evict_to: Some(6),
                purge_markers: false,
            }),
            artifact_at: 7,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
        // Same, prefix-scoped: the local fold also holds out-of-scope keys
        // that a restore from an All-scope artifact must not touch.
        Scenario {
            name: "evicting: prefix scope, trip → restore",
            events: evicting_log(),
            local_at: Some(3),
            out_of_scope: vec![("x.mine", "keep"), ("x.other", "keep2")],
            unanchored: false,
            evicts_current: true,
            floor: 3,
            purged: vec![],
            trip: Some(Trip {
                deliver: 1,
                append: vec![],
                evict_to: Some(6),
                purge_markers: false,
            }),
            artifact_at: 7,
            scope: WatchScope::Prefix("n.".into()),
            mode: Mode::Auto,
        },
        // A fresh node (no cursor) on a bucket that already evicted current
        // values seeds from the artifact.
        Scenario {
            name: "evicting: fresh start → restore",
            events: evicting_log(),
            local_at: None,
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: true,
            floor: 6,
            purged: vec![],
            trip: None,
            artifact_at: 7,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
        // Keeps-current bucket: a key re-created at 4 and deleted again at 5,
        // its marker purged; the rest superseded, pushing the head past the
        // cursor. Resume finds it expired → key-listing diff.
        Scenario {
            name: "keeps-current: resume expired → relist",
            events: relist_log(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: false,
            floor: 0,
            purged: vec![5],
            trip: None,
            artifact_at: 0,
            scope: WatchScope::All,
            mode: Mode::Relist,
        },
        // THE RE-QUEUE BUG's shape: the live watch delivers the re-create at
        // 4; then the key is deleted again, everything else superseded, and
        // the marker purged, overrunning the watch. The put at 4 is pending
        // (or re-queued) when the key-listing diff runs, and the key is gone.
        Scenario {
            name: "keeps-current: trip mid-watch → relist",
            events: relist_log()[..4].to_vec(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: false,
            floor: 0,
            purged: vec![],
            trip: Some(relist_trip()),
            artifact_at: 0,
            scope: WatchScope::All,
            mode: Mode::Relist,
        },
        // Auto on a keeps-current bucket chooses the key-listing diff.
        Scenario {
            name: "keeps-current: trip mid-watch → auto picks relist",
            events: relist_log()[..4].to_vec(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: false,
            evicts_current: false,
            floor: 0,
            purged: vec![],
            trip: Some(relist_trip()),
            artifact_at: 9,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
    ]
}

fn unanchored_scenarios() -> Vec<Scenario> {
    vec![
        // A torn first checkpoint left n.new's put on disk with no cursor;
        // the key was then deleted and its marker purged. A blind re-list
        // would keep n.new forever.
        Scenario {
            name: "keeps-current: data but no cursor → relist",
            events: relist_log(),
            local_at: Some(4),
            out_of_scope: vec![],
            unanchored: true,
            evicts_current: false,
            floor: 0,
            purged: vec![5],
            trip: None,
            artifact_at: 0,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
        // Same on an evicting bucket that already evicted: restore.
        Scenario {
            name: "evicting: data but no cursor → restore",
            events: evicting_log(),
            local_at: Some(3),
            out_of_scope: vec![],
            unanchored: true,
            evicts_current: true,
            floor: 6,
            purged: vec![],
            trip: None,
            artifact_at: 7,
            scope: WatchScope::All,
            mode: Mode::Auto,
        },
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

// --- The harness ----------------------------------------------------------

struct World {
    log: Arc<SimLog>,
    restore: Arc<SimRestore>,
    path: PathBuf,
    _dir: tempfile::TempDir,
}

fn world(sc: &Scenario, yield_between: bool) -> World {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("fold.snap");
    let (_c, mut fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let mut seed: Vec<KvUpdate> = sc
        .out_of_scope
        .iter()
        .map(|(k, v)| {
            KvUpdate::Put(KvEntry {
                key: k.to_string(),
                value: v.as_bytes().to_vec(),
                version: VersionToken::from_u64(0),
            })
        })
        .collect();
    if let Some(at) = sc.local_at {
        seed.extend(sc.events.iter().filter(|e| e.rev <= at).map(to_update));
        let cursor = if sc.unanchored {
            WatchCursor::none()
        } else {
            WatchCursor::from_u64(at)
        };
        fold.apply(&seed, &cursor).unwrap();
    }
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
        dir: dir.path().to_path_buf(),
        fetches: AtomicUsize::new(0),
    });
    World {
        log,
        restore,
        path,
        _dir: dir,
    }
}

fn repair(sc: &Scenario, w: &World) -> ExpiryRepair<FaultStore> {
    match sc.mode {
        Mode::Relist => ExpiryRepair::Relist(Arc::clone(&w.log) as Arc<dyn KvReader>),
        Mode::Auto => ExpiryRepair::Auto {
            reader: Arc::clone(&w.log) as Arc<dyn KvReader>,
            restore: Arc::clone(&w.restore) as Arc<dyn RestoreSource<FaultStore>>,
        },
    }
}

enum RunEnd {
    Clean,
    Crashed,
}

/// One process lifetime: open the fold, rebuild domain state from it, run
/// `watch_applied` to the end of the (simulated) stream or a crash.
async fn run_once(
    sc: &Scenario,
    w: &World,
    faults: RunFaults,
    max: usize,
    calls: Arc<AtomicUsize>,
) -> (RunEnd, Vec<u64>, HashMap<String, Vec<u8>>) {
    let (cursor, inner) = AppendLogSnapshot::open(&w.path, u64::MAX).unwrap();
    let domain: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(
        inner
            .range("")
            .unwrap()
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect(),
    ));
    let reported = Arc::new(Mutex::new(Vec::new()));
    let store = FaultStore {
        inner,
        faults,
        calls,
    };
    let (_sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    let (d, r) = (Arc::clone(&domain), Arc::clone(&reported));
    let res = watch_applied(
        Arc::clone(&w.log) as Arc<dyn KvWatcher>,
        sc.scope.clone(),
        (!cursor.is_none()).then_some(cursor),
        repair(sc, w),
        Some(store),
        None,
        BatchConfig {
            window: std::time::Duration::from_secs(3600),
            max,
            ..BatchConfig::default()
        },
        |u: &KvUpdate| {
            Some(match u {
                KvUpdate::Put(e) => (e.key.clone(), Some(e.value.clone())),
                KvUpdate::Delete { key, .. } | KvUpdate::Purge { key, .. } => (key.clone(), None),
            })
        },
        move |batch: Vec<(String, Option<Vec<u8>>)>| {
            let mut d = d.lock().unwrap();
            for (k, v) in batch {
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
        move |c: WatchCursor| r.lock().unwrap().push(c.as_u64().unwrap()),
        sd_rx,
    )
    .await;
    let reported = reported.lock().unwrap().clone();
    let domain = domain.lock().unwrap().clone();
    match res {
        Ok(_) => (RunEnd::Clean, reported, domain),
        // A crash, or the fail-stop after a persistent store failure: either
        // way the process restarts.
        Err(KvError::WatchError(msg))
            if msg.contains("panicked") || msg.contains("consecutive times") =>
        {
            (RunEnd::Crashed, reported, domain)
        }
        Err(e) => panic!("[{}] unexpected watch error: {e}", sc.name),
    }
}

/// Drive a schedule (faults per run) to convergence and check the outcome.
/// Returns the number of store-apply calls the first run made.
async fn check_schedule(
    sc: &Scenario,
    yield_between: bool,
    max: usize,
    schedule: &[RunFaults],
) -> usize {
    let w = world(sc, yield_between);
    let ctx = format!(
        "[{}] yield={yield_between} max={max} schedule={schedule:?}",
        sc.name
    );
    let mut last_cursor = AppendLogSnapshot::open(&w.path, u64::MAX)
        .unwrap()
        .0
        .as_u64()
        .unwrap_or(0);
    let mut first_calls = None;
    let mut run = 0usize;
    loop {
        let faults = schedule.get(run).cloned().unwrap_or_default();
        let faultless = faults.is_empty() && run >= schedule.len();
        let calls = Arc::new(AtomicUsize::new(0));
        let (end, reported, domain) = run_once(sc, &w, faults, max, Arc::clone(&calls)).await;
        first_calls.get_or_insert(calls.load(Ordering::SeqCst));
        assert!(
            reported.windows(2).all(|p| p[0] <= p[1]),
            "{ctx}: on_applied went backward within a run: {reported:?}"
        );
        let (cursor, fold) = AppendLogSnapshot::open(&w.path, u64::MAX).unwrap();
        let cursor = cursor.as_u64().unwrap_or(0);
        assert!(
            cursor >= last_cursor,
            "{ctx}: the fold's cursor went backward across run {run}: {last_cursor} → {cursor}"
        );
        last_cursor = cursor;
        run += 1;
        assert!(run < 20, "{ctx}: no convergence after {run} runs");
        if !(faultless && matches!(end, RunEnd::Clean)) {
            continue;
        }

        // Converged run: judge it.
        let (head, events) = {
            let st = w.log.0.lock().unwrap();
            (st.head(), st.events.clone())
        };
        let prefix = match &sc.scope {
            WatchScope::Prefix(p) => p.clone(),
            _ => String::new(),
        };
        let want: BTreeMap<String, (Vec<u8>, u64)> = truth(&events, head)
            .into_iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .collect();
        let got: BTreeMap<String, (Vec<u8>, u64)> = fold
            .range(&prefix)
            .unwrap()
            .into_iter()
            .map(|e| (e.key, (e.value, e.version.as_u64().unwrap())))
            .collect();
        assert_eq!(
            got, want,
            "{ctx}: the fold is not every write minus every real delete"
        );
        assert_eq!(
            cursor, head,
            "{ctx}: the fold's cursor did not reach the head"
        );
        for (k, v) in &sc.out_of_scope {
            assert_eq!(
                fold.get(k).unwrap().map(|e| e.value),
                Some(v.as_bytes().to_vec()),
                "{ctx}: out-of-scope {k} was touched"
            );
        }
        let all: HashMap<String, Vec<u8>> = fold
            .range("")
            .unwrap()
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        assert_eq!(
            domain, all,
            "{ctx}: the domain state (apply) diverged from the fold"
        );
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

/// Every schedule for one scenario configuration. Returns how many ran.
async fn exhaust(sc: &Scenario, yield_between: bool, max: usize) -> usize {
    let k = check_schedule(sc, yield_between, max, &[]).await;
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
            schedules.push(vec![crash.clone()]);
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
        check_schedule(sc, yield_between, max, s).await;
    }
    schedules.len() + 1
}

#[tokio::test(flavor = "current_thread")]
async fn every_fault_schedule_converges() {
    quiet_simulated_crashes();
    let mut total = 0usize;
    for sc in scenarios().into_iter().chain(unanchored_scenarios()) {
        for yield_between in [false, true] {
            for max in [1, 100] {
                let n = exhaust(&sc, yield_between, max).await;
                println!("{} yield={yield_between} max={max}: {n} schedules", sc.name);
                total += n;
            }
        }
    }
    println!("total: {total} schedules");
    assert!(
        total > 10_000,
        "the schedule space is not vacuous ({total})"
    );
}
