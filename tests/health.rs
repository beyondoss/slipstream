//! The health snapshot and the presets, against a live nats-server.
//!
//! Every state the snapshot claims to report is produced for real and read
//! back through [`WatchHealth::snapshot`]: lag behind the log's head, a store
//! apply in flight, a refused restore, a wait for an artifact, a cursor
//! expiry with no repair armed, restarts and the last error. Each assertion
//! would fail with the recording removed: the states exist nowhere else.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::snapshot::SnapshotError;
use slipstream::{
    AppendLogSnapshot, BatchConfig, Connection, DiscardPolicy, ExportManifest, ExportRequest,
    KvError, KvStore, KvUpdate, NatsConnection, NatsConnectionConfig, Preset, RepairState,
    RestoreSource, RestoredFold, StoreConfig, WatchCursor, WatchEnd, WatchHealth, WatchScope,
    watch_applied_owned,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

const SAMPLE: Duration = Duration::from_millis(50);

struct Harness {
    _nats: TestNats,
    _conn: NatsConnection,
    bucket: Arc<dyn KvStore>,
    dir: TempDir,
}

impl Harness {
    async fn new(config: StoreConfig) -> Harness {
        let nats = TestNats::start().await;
        let conn = NatsConnection::new(NatsConnectionConfig {
            url: nats.url.clone(),
            creds: None,
            creds_file: None,
        });
        conn.connect().await.expect("connect");
        let bucket = conn.store_with_config(config).await.expect("bucket");
        Harness {
            _nats: nats,
            _conn: conn,
            bucket,
            dir: TempDir::new().unwrap(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn open(&self, name: &str) -> (WatchCursor, AppendLogSnapshot) {
        AppendLogSnapshot::open(&self.path(name), u64::MAX).expect("open fold")
    }

    async fn put(&self, key: &str, value: &[u8]) -> u64 {
        self.bucket
            .writer()
            .unwrap()
            .put(key, value)
            .await
            .unwrap()
            .as_u64()
            .unwrap()
    }
}

fn evicting() -> StoreConfig {
    StoreConfig {
        name: "routes".into(),
        max_bytes: Some(4096),
        discard: DiscardPolicy::Old,
        ..Default::default()
    }
}

async fn eventually(what: &str, mut ready: impl FnMut() -> bool) {
    timeout(Duration::from_secs(15), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never reached: {what}"));
}

/// A running watch: `apply` blocks while `gate` is up, `applied` follows
/// `on_applied`, and exports go through `exports`.
struct Node {
    exports: mpsc::Sender<ExportRequest>,
    gate: Gate,
    applied: Arc<AtomicU64>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<WatchEnd<AppendLogSnapshot>>,
}

/// Holds `apply` while up. Opens on drop, so a failed assertion cannot leave
/// `apply` parked and hang the runtime's shutdown.
struct Gate(Arc<AtomicBool>);

impl Drop for Gate {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Node {
    fn spawn(
        bucket: &Arc<dyn KvStore>,
        scope: WatchScope,
        fold: AppendLogSnapshot,
        resume: Option<WatchCursor>,
        repair: slipstream::ExpiryRepair<AppendLogSnapshot>,
        health: &WatchHealth,
        gated: bool,
    ) -> Node {
        let gate = Arc::new(AtomicBool::new(gated));
        let g = Arc::clone(&gate);
        let (ex_tx, ex_rx) = mpsc::channel(1);
        let (sd_tx, sd_rx) = watch::channel(false);
        let applied = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);
        let watcher = bucket.watcher().expect("watcher");
        let health = health.clone();
        let task = tokio::spawn(async move {
            watch_applied_owned(
                watcher,
                scope,
                resume,
                repair,
                Some(fold),
                Some(ex_rx),
                BatchConfig {
                    health: Some(health),
                    ..BatchConfig::default()
                },
                |u: &KvUpdate| Some(u.key().to_string()),
                move |_batch: Vec<String>| {
                    // Tell the runtime this worker is about to block, so its
                    // timers and other tasks keep running.
                    tokio::task::block_in_place(|| {
                        while g.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    });
                },
                move |c: WatchCursor| a.store(c.as_u64().unwrap_or(0), Ordering::SeqCst),
                sd_rx,
            )
            .await
        });
        Node {
            exports: ex_tx,
            gate: Gate(gate),
            applied,
            shutdown: sd_tx,
            task,
        }
    }

    async fn wait_applied(&self, at_least: u64) {
        eventually("applied", || {
            assert!(!self.task.is_finished(), "watch ended early");
            self.applied.load(Ordering::SeqCst) >= at_least
        })
        .await;
    }

    async fn stop(self) -> WatchEnd<AppendLogSnapshot> {
        let _ = self.shutdown.send(true);
        self.task.await.expect("watch task")
    }

    async fn export(&self, into: &std::path::Path) -> ExportManifest {
        let (reply, rx) = oneshot::channel();
        self.exports
            .send(ExportRequest {
                dest_dir: into.to_path_buf(),
                reply,
            })
            .await
            .expect("export request");
        rx.await.expect("reply").expect("export")
    }
}

fn health() -> WatchHealth {
    WatchHealth::with_head_sample_interval(SAMPLE)
}

fn none() -> slipstream::ExpiryRepair<AppendLogSnapshot> {
    slipstream::ExpiryRepair::None
}

/// Lag and apply-in-flight, read while a watch is stuck behind a slow apply,
/// then read again once it catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lag_is_reported_and_clears() {
    let h = Harness::new(StoreConfig {
        name: "routes".into(),
        ..Default::default()
    })
    .await;
    let health = health();
    let before = health.snapshot();
    assert!(!before.running);
    assert_eq!(before.applied, None);
    assert_eq!(before.stream_head, None);
    assert_eq!(before.lag, None, "unknown, not zero");

    let (_c, fold) = h.open("fold");
    let node = Node::spawn(
        &h.bucket,
        WatchScope::All,
        fold,
        None,
        none(),
        &health,
        true, // apply blocks: the watch is behind
    );
    // The watch attaches asynchronously; write until it has a batch stuck.
    let mut head = 0;
    for i in 0..10 {
        head = h.put(&format!("k.{i}"), b"v").await;
    }
    eventually("a sample sees the head", || {
        health.snapshot().stream_head == Some(head)
    })
    .await;
    let stuck = health.snapshot();
    assert!(stuck.running);
    assert_eq!(stuck.applied, None, "nothing applied while apply blocks");
    assert_eq!(stuck.stream_first, Some(1));
    assert_eq!(stuck.lag, Some(head), "the whole backlog");
    assert!(stuck.head_sample_age.unwrap() < Duration::from_secs(5));
    assert_eq!(stuck.last_commit_age, None);
    assert_eq!(stuck.repair, RepairState::Idle);
    assert_eq!(stuck.restarts, 0);

    node.gate.0.store(false, Ordering::SeqCst);
    node.wait_applied(head).await;
    eventually("lag clears", || {
        let s = health.snapshot();
        s.applied == Some(head) && s.lag == Some(0)
    })
    .await;
    let caught_up = health.snapshot();
    assert!(caught_up.last_commit_age.is_some(), "the store committed");
    assert_eq!(caught_up.apply_in_flight, None);

    let end = node.stop().await;
    end.result.expect("clean end");
    let ended = health.snapshot();
    assert!(!ended.running);
    assert_eq!(ended.last_error, None);
}

/// Both presets build and run against a live server: they watch the scope
/// they promise, export through the channel they require, and report health.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presets_build_and_run() {
    struct NoArtifact;
    #[async_trait::async_trait]
    impl RestoreSource<AppendLogSnapshot> for NoArtifact {
        async fn latest(&self) -> Result<ExportManifest, SnapshotError> {
            Err(SnapshotError::Backend("no artifact".into()))
        }
        async fn fetch(&self) -> Result<RestoredFold<AppendLogSnapshot>, SnapshotError> {
            Err(SnapshotError::Backend("no artifact".into()))
        }
    }

    for slice in [false, true] {
        let h = Harness::new(StoreConfig {
            name: "routes".into(),
            ..Default::default()
        })
        .await;
        let health = health();
        let preset = if slice {
            Preset::node_slice("node1.")
        } else {
            Preset::full_replica(h.bucket.reader(), Arc::new(NoArtifact))
        }
        .health(health.clone());
        assert!(
            matches!(preset.scope(), WatchScope::Prefix(p) if slice && p == "node1.")
                || matches!(preset.scope(), WatchScope::All if !slice)
        );

        let (_c, fold) = h.open("fold");
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let sink = Arc::clone(&seen);
        let (ex_tx, ex_rx) = mpsc::channel(1);
        let (sd_tx, sd_rx) = watch::channel(false);
        let applied = Arc::new(AtomicU64::new(0));
        let a = Arc::clone(&applied);
        let watcher = h.bucket.watcher().unwrap();
        let task = tokio::spawn(async move {
            preset
                .run(
                    watcher,
                    None,
                    fold,
                    ex_rx,
                    |u: &KvUpdate| Some(u.key().to_string()),
                    move |keys: Vec<String>| sink.lock().unwrap().extend(keys),
                    move |c: WatchCursor| a.store(c.as_u64().unwrap_or(0), Ordering::SeqCst),
                    sd_rx,
                )
                .await
        });

        h.put("node1.a", b"1").await;
        h.put("node2.b", b"2").await;
        let last = h.put("node1.c", b"3").await;
        eventually("applied", || applied.load(Ordering::SeqCst) >= last).await;

        let dest = h.path("export");
        let (reply, rx) = oneshot::channel();
        ex_tx
            .send(ExportRequest {
                dest_dir: dest,
                reply,
            })
            .await
            .unwrap();
        let manifest = rx.await.unwrap().expect("export");

        let snap = health.snapshot();
        assert!(snap.running);
        assert_eq!(snap.applied, Some(last));
        assert_eq!(
            snap.newest_artifact.as_ref().and_then(|a| a.cursor),
            manifest.cursor.as_u64(),
            "an export is the newest artifact"
        );

        let _ = sd_tx.send(true);
        let end = task.await.unwrap();
        end.result.expect("clean end");
        assert!(end.store.is_some(), "the store comes back");

        let mut keys = seen.lock().unwrap().clone();
        keys.sort();
        if slice {
            assert_eq!(keys, ["node1.a", "node1.c"], "a slice sees only its prefix");
        } else {
            assert_eq!(keys, ["node1.a", "node1.c", "node2.b"]);
        }
    }
}

/// A slice has no repair, so an expired cursor is a re-list only: the snapshot
/// must say the cursor expired, because the fold may now hold a ghost key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slice_counts_a_cursor_expiry_it_cannot_repair() {
    let h = Harness::new(evicting()).await;
    let health = health();
    let (_c, fold) = h.open("fold");
    let node = Node::spawn(
        &h.bucket,
        WatchScope::Prefix("node1.".into()),
        fold,
        None,
        none(),
        &health,
        false,
    );
    let mut rev = 0;
    for _ in 0..5 {
        rev = h.put("node1.a", b"v").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    node.wait_applied(rev).await;
    let end = node.stop().await;
    let resume = end.applied.clone();

    for i in 0..200 {
        h.put(&format!("filler.{i}"), &[b'x'; 64]).await;
    }
    assert_eq!(health.snapshot().cursor_expiries, 0);
    let (_c, fold) = h.open("fold");
    let node = Node::spawn(
        &h.bucket,
        WatchScope::Prefix("node1.".into()),
        fold,
        Some(resume),
        none(),
        &health,
        false,
    );
    eventually("the expiry is counted", || {
        health.snapshot().cursor_expiries == 1
    })
    .await;
    let snap = health.snapshot();
    assert_eq!(snap.restarts, 1, "the second run on this handle");
    assert_eq!(snap.repair, RepairState::Idle, "nothing was armed to run");
    node.stop().await.result.expect("re-list only");
}

/// A recreated bucket makes the fold's cursor foreign. With no repair armed
/// the watch fails, and the snapshot says why; with a repair armed it reseeds
/// and the refusal does not appear.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_cursor_is_refused_without_a_repair() {
    let cfg = || StoreConfig {
        name: "routes".into(),
        ..Default::default()
    };
    let h = Harness::new(cfg()).await;
    let (_c, fold) = h.open("fold");
    let node = Node::spawn(
        &h.bucket,
        WatchScope::All,
        fold,
        None,
        none(),
        &health(),
        false,
    );
    let mut rev = 0;
    for i in 0..20 {
        rev = h.put(&format!("old.{i}"), b"v").await;
    }
    node.wait_applied(rev).await;
    let resume = node.stop().await.applied;

    let client = async_nats::connect(&h._nats.url).await.unwrap();
    async_nats::jetstream::new(client)
        .delete_key_value("routes")
        .await
        .unwrap();
    let new = h._conn.store_with_config(cfg()).await.unwrap();
    let w = new.writer().unwrap();
    let mut head = 0;
    for i in 0..5 {
        head = w
            .put(&format!("new.{i}"), b"v")
            .await
            .unwrap()
            .as_u64()
            .unwrap();
    }

    let refused = health();
    let (c, fold) = h.open("fold");
    assert_eq!(c.as_u64(), resume.as_u64());
    let node = Node::spawn(
        &new,
        WatchScope::All,
        fold,
        Some(c),
        none(),
        &refused,
        false,
    );
    let end = timeout(Duration::from_secs(15), node.task)
        .await
        .expect("fails, not hangs")
        .unwrap();
    assert!(matches!(end.result, Err(KvError::ForeignCursor)));
    let snap = refused.snapshot();
    assert_eq!(snap.repair, RepairState::ForeignCursorRefused);
    assert!(snap.last_error.is_some());
    assert!(!snap.running);

    let repaired = health();
    let (c, fold) = h.open("fold");
    let relist = slipstream::ExpiryRepair::Relist(new.reader());
    let node = Node::spawn(
        &new,
        WatchScope::All,
        fold,
        Some(c),
        relist,
        &repaired,
        false,
    );
    node.wait_applied(head).await;
    assert_eq!(repaired.snapshot().repair, RepairState::Idle);
    node.stop().await.result.expect("clean end");
}

#[cfg(feature = "transport")]
mod restore {
    use super::*;
    use slipstream::{ArtifactRestore, ArtifactTransport, ObjectStoreTransport};

    const KEY: &str = "routes/latest";

    struct Fleet {
        h: Harness,
        transport: Arc<dyn ArtifactTransport>,
    }

    impl Fleet {
        async fn new() -> Fleet {
            let h = Harness::new(evicting()).await;
            std::fs::create_dir(h.path("objects")).unwrap();
            let store = object_store::local::LocalFileSystem::new_with_prefix(h.path("objects"))
                .expect("local object store");
            let transport: Arc<dyn ArtifactTransport> = Arc::new(
                ObjectStoreTransport::new(Arc::new(store), "artifacts")
                    .with_non_atomic_pointer_fallback(),
            );
            Fleet { h, transport }
        }

        fn auto(&self) -> slipstream::ExpiryRepair<AppendLogSnapshot> {
            slipstream::ExpiryRepair::Auto {
                reader: self.h.bucket.reader(),
                restore: Arc::new(ArtifactRestore::new(
                    Arc::clone(&self.transport),
                    KEY,
                    self.h.dir.path(),
                    |artifact: &std::path::Path, dest: &std::path::Path| {
                        AppendLogSnapshot::import(artifact, dest, u64::MAX)
                    },
                )),
            }
        }

        async fn publish(&self, exporter: &Node, name: &str) -> u64 {
            let dest = self.h.path(name);
            let manifest = exporter.export(&dest).await;
            self.transport.upload(KEY, &dest).await.expect("upload");
            manifest.cursor.as_u64().unwrap()
        }

        /// A live exporter and a watcher that has folded `route.keep` and
        /// gone offline; the bucket then evicts everything, so the watcher's
        /// cursor is expired. Returns the exporter, and the watcher's cursor.
        async fn gap(&self, exporter_health: &WatchHealth) -> (Node, WatchCursor) {
            let h = &self.h;
            let (_c, fold_x) = h.open("exporter.snap");
            let exporter = Node::spawn(
                &h.bucket,
                WatchScope::All,
                fold_x,
                None,
                self.auto(),
                exporter_health,
                false,
            );
            let wh = health();
            let (_c, fold_w) = h.open("watcher.snap");
            let watcher = Node::spawn(
                &h.bucket,
                WatchScope::All,
                fold_w,
                None,
                self.auto(),
                &wh,
                false,
            );
            let mut rev = 0;
            for _ in 0..5 {
                rev = h.put("route.keep", b"v1").await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            watcher.wait_applied(rev).await;
            let resume = watcher.stop().await.applied;
            exporter.wait_applied(rev).await;
            (exporter, resume)
        }

        async fn evict(&self, exporter: &Node) {
            let mut last = 0;
            for i in 0..200 {
                last = self.h.put(&format!("filler.{i}"), &[b'x'; 64]).await;
            }
            exporter.wait_applied(last).await;
        }

        fn restart(&self, resume: &WatchCursor, health: &WatchHealth) -> Node {
            let (c, fold) = self.h.open("watcher.snap");
            assert_eq!(c.as_u64(), resume.as_u64());
            Node::spawn(
                &self.h.bucket,
                WatchScope::All,
                fold,
                Some(c),
                self.auto(),
                health,
                false,
            )
        }
    }

    /// No artifact exists: the expired watcher can only wait for one. The
    /// watch fails and the snapshot says what it is waiting for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn waiting_for_an_artifact() {
        let f = Fleet::new().await;
        let (exporter, resume) = f.gap(&health()).await;
        f.evict(&exporter).await;

        let wh = health();
        let node = f.restart(&resume, &wh);
        let end = timeout(Duration::from_secs(15), node.task)
            .await
            .expect("the watch must fail, not hang")
            .unwrap();
        end.result.expect_err("no artifact: no safe recovery");

        let snap = wh.snapshot();
        assert!(!snap.running);
        assert_eq!(snap.cursor_expiries, 1);
        assert!(
            matches!(&snap.repair, RepairState::WaitingForArtifact(m) if m.contains("latest artifact")),
            "{:?}",
            snap.repair
        );
        assert!(snap.last_error.is_some());
        assert_eq!(snap.newest_artifact, None);
        assert_eq!(snap.applied, resume.as_u64(), "the fold did not move");
    }

    /// An artifact outside retention is refused; the refusal outlives the
    /// failed run and a restart, and a fresh artifact clears it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restore_refused_then_cleared_by_a_fresh_artifact() {
        let f = Fleet::new().await;
        let xh = health();
        let (exporter, resume) = f.gap(&xh).await;
        // Written while the watcher is offline: the artifact is ahead of it.
        let gap_write = f.h.put("route.late", b"gap").await;
        exporter.wait_applied(gap_write).await;
        let stale = f.publish(&exporter, "stale").await;
        assert!(stale > resume.as_u64().unwrap());
        f.evict(&exporter).await;

        let wh = health();
        let node = f.restart(&resume, &wh);
        let end = timeout(Duration::from_secs(15), node.task)
            .await
            .expect("the watch must fail, not hang")
            .unwrap();
        end.result.expect_err("a stale artifact must be refused");

        let snap = wh.snapshot();
        assert!(!snap.running);
        assert_eq!(snap.cursor_expiries, 1);
        assert!(
            matches!(&snap.repair, RepairState::Refused(m) if m.contains("retention window")),
            "{:?}",
            snap.repair
        );
        assert_eq!(snap.newest_artifact.as_ref().unwrap().cursor, Some(stale));
        assert!(
            snap.last_error
                .as_ref()
                .unwrap()
                .message
                .contains("retention window")
        );
        assert_eq!(snap.restarts, 0);
        // The exporter's own export is the newest artifact it knows of too.
        assert_eq!(
            xh.snapshot().newest_artifact.as_ref().unwrap().cursor,
            Some(stale)
        );

        // The caller's restart retries; still refused, still visible.
        let node = f.restart(&resume, &wh);
        timeout(Duration::from_secs(15), node.task)
            .await
            .expect("fails again")
            .unwrap()
            .result
            .expect_err("still stale");
        let snap = wh.snapshot();
        assert_eq!(snap.restarts, 1);
        assert_eq!(snap.cursor_expiries, 2);
        assert!(matches!(snap.repair, RepairState::Refused(_)));

        // The exporter publishes a fresh artifact; the next retry restores.
        let fresh = f.publish(&exporter, "fresh").await;
        assert!(fresh > stale);
        let node = f.restart(&resume, &wh);
        node.wait_applied(fresh).await;
        let snap = wh.snapshot();
        assert!(snap.running);
        assert_eq!(snap.repair, RepairState::Idle, "success clears the refusal");
        assert_eq!(snap.newest_artifact.as_ref().unwrap().cursor, Some(fresh));
        node.stop().await.result.expect("clean end");
        exporter.stop().await.result.expect("clean end");
    }
}
