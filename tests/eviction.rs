//! Cursor-expiry repair on buckets whose retention evicts CURRENT values
//! (`discard: old` under `max_bytes`, `max_age`), against a live nats-server
//! and a real artifact transport (local filesystem object store).
//!
//! On such a bucket "not in NATS" no longer means "deleted": a key can age out
//! without anyone deleting it, and a write made while a watcher was offline
//! can age out before the watcher returns. The pre-fix key-listing resync
//! treated every key NATS no longer listed as deleted — reproduced here
//! against a live server for both eviction kinds: a restarted watcher deleted
//! a valid key it had already folded and never saw the gap write. These tests
//! pin the fix:
//!
//! - **Restore**: the expired watcher repairs from the newest artifact (the
//!   live exporter folded every write and every real delete), keeps the
//!   aged-out key, recovers the gap write, applies the real delete, and ends
//!   identical to the exporter.
//! - **Relist refused**: the legacy `Some(reader)` repair fails the watch on
//!   such a bucket instead of deleting valid keys.
//! - **Stale artifact**: an artifact whose cursor is outside retention fails
//!   the watch — there is no safe recovery — and leaves the fold untouched.
#![cfg(feature = "transport")]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    AppendLogSnapshot, ArtifactRestore, ArtifactTransport, BatchConfig, Connection, DiscardPolicy,
    ExpiryRepair, ExportRequest, KvError, KvReader, KvStore, KvUpdate, NatsConnection,
    NatsConnectionConfig, ObjectStoreTransport, StoreConfig, WatchCursor, WatchScope,
    watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

const KEY: &str = "routes/latest";

struct Node {
    exports: mpsc::Sender<ExportRequest>,
    applied: Arc<AtomicU64>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<WatchCursor, KvError>>,
}

impl Node {
    fn spawn(
        bucket: &Arc<dyn KvStore>,
        fold: AppendLogSnapshot,
        resume: Option<WatchCursor>,
        repair: ExpiryRepair<AppendLogSnapshot>,
    ) -> Node {
        let (ex_tx, ex_rx) = mpsc::channel(1);
        let (sd_tx, sd_rx) = watch::channel(false);
        let applied = Arc::new(AtomicU64::new(
            resume.as_ref().and_then(WatchCursor::as_u64).unwrap_or(0),
        ));
        let a = Arc::clone(&applied);
        let task = tokio::spawn(watch_applied(
            bucket.watcher().expect("watcher"),
            WatchScope::All,
            resume,
            repair,
            Some(fold),
            Some(ex_rx),
            BatchConfig::default(),
            |u: &KvUpdate| Some(u.key().to_string()),
            |_batch: Vec<String>| {},
            move |c: WatchCursor| a.store(c.as_u64().unwrap_or(0), Ordering::SeqCst),
            sd_rx,
        ));
        Node {
            exports: ex_tx,
            applied,
            shutdown: sd_tx,
            task,
        }
    }

    async fn wait_applied(&self, at_least: u64) {
        timeout(Duration::from_secs(15), async {
            while self.applied.load(Ordering::SeqCst) < at_least {
                assert!(
                    !self.task.is_finished(),
                    "watch ended before applying {at_least}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "never applied {at_least} (at {})",
                self.applied.load(Ordering::SeqCst)
            )
        });
    }

    async fn stop(self) -> Result<WatchCursor, KvError> {
        let _ = self.shutdown.send(true);
        self.task.await.expect("watch task")
    }

    /// Export through the live loop and publish it.
    async fn publish(&self, transport: &dyn ArtifactTransport, scratch: &Path) -> u64 {
        let dest = tempfile::Builder::new()
            .prefix("export-")
            .tempdir_in(scratch)
            .unwrap();
        let artifact = dest.path().join("artifact");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.exports
            .send(ExportRequest {
                dest_dir: artifact.clone(),
                reply: reply_tx,
            })
            .await
            .expect("export request");
        let manifest = reply_rx.await.expect("reply").expect("export");
        transport.upload(KEY, &artifact).await.expect("upload");
        manifest.cursor.as_u64().expect("cursor")
    }
}

struct Harness {
    _nats: TestNats,
    _conn: NatsConnection,
    bucket: Arc<dyn KvStore>,
    dir: TempDir,
    transport: Arc<dyn ArtifactTransport>,
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
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("objects")).unwrap();
        let store =
            object_store::local::LocalFileSystem::new_with_prefix(dir.path().join("objects"))
                .expect("local object store");
        // file:// has no conditional puts; the explicit dev opt-in is fine for
        // a single-exporter test (the pointer protocol is proved elsewhere).
        let transport: Arc<dyn ArtifactTransport> = Arc::new(
            ObjectStoreTransport::new(Arc::new(store), "artifacts")
                .with_non_atomic_pointer_fallback(),
        );
        Harness {
            _nats: nats,
            _conn: conn,
            bucket,
            dir,
            transport,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn open(&self, name: &str) -> (WatchCursor, AppendLogSnapshot) {
        AppendLogSnapshot::open(&self.path(name), u64::MAX).expect("open fold")
    }

    fn auto(&self) -> ExpiryRepair<AppendLogSnapshot> {
        ExpiryRepair::Auto {
            reader: self.bucket.reader(),
            restore: Arc::new(ArtifactRestore::new(
                Arc::clone(&self.transport),
                KEY,
                self.dir.path(),
                |artifact: &Path, dest: &Path| -> Result<_, SnapshotError> {
                    AppendLogSnapshot::import(artifact, dest, u64::MAX)
                },
            )),
        }
    }

    async fn first_sequence(&self) -> u64 {
        self.bucket
            .watcher()
            .unwrap()
            .retention()
            .await
            .unwrap()
            .expect("NATS reports retention")
            .first_revision
    }
}

/// How the test makes the bucket evict current values while the watcher is
/// offline.
#[derive(Clone, Copy)]
enum Eviction {
    /// `discard: old` under a small `max_bytes`: filler writes push the oldest
    /// messages out. Deterministic.
    DiscardOld,
    /// `max_age`: everything older than one second ages out.
    MaxAge,
}

impl Eviction {
    fn config(self) -> StoreConfig {
        match self {
            Eviction::DiscardOld => StoreConfig {
                name: "routes".into(),
                max_bytes: Some(4096),
                discard: DiscardPolicy::Old,
                ..Default::default()
            },
            Eviction::MaxAge => StoreConfig {
                name: "routes".into(),
                max_age: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        }
    }

    /// Evict everything written so far; returns the bucket's final revision.
    async fn evict(self, bucket: &Arc<dyn KvStore>) -> u64 {
        let w = bucket.writer().unwrap();
        let mut last = 0;
        match self {
            Eviction::DiscardOld => {
                for i in 0..200 {
                    last = w
                        .put(&format!("filler.{i}"), &[b'x'; 64])
                        .await
                        .unwrap()
                        .as_u64()
                        .unwrap();
                }
            }
            Eviction::MaxAge => {
                tokio::time::sleep(Duration::from_millis(2500)).await;
                last = w.put("filler.tail", b"x").await.unwrap().as_u64().unwrap();
            }
        }
        last
    }
}

/// The shared setup: an exporter that stays live throughout, and a watcher
/// that folds `route.keep` and `route.gone`, goes offline, and comes back to
/// a bucket that — meanwhile — got a gap write (`route.late`), a real delete
/// (`route.gone`), and evicted the current values of `route.keep` and
/// `route.late`. The watcher's resume cursor is expired.
struct Gap {
    h: Harness,
    exporter: Node,
    resume: WatchCursor,
    final_rev: u64,
}

async fn build_gap(eviction: Eviction, export_before_eviction: bool) -> (Gap, Option<u64>) {
    let h = Harness::new(eviction.config()).await;
    let w = h.bucket.writer().unwrap();

    let (_c, fold_x) = h.open("exporter.snap");
    let exporter = Node::spawn(&h.bucket, fold_x, None, h.auto());
    let (_c, fold_w) = h.open("watcher.snap");
    let watcher = Node::spawn(&h.bucket, fold_w, None, h.auto());

    // KV watches attach asynchronously; write until both have folded a seed.
    timeout(Duration::from_secs(10), async {
        loop {
            w.put("route.seed", b"seed").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            if exporter.applied.load(Ordering::SeqCst) > 0
                && watcher.applied.load(Ordering::SeqCst) > 0
            {
                return;
            }
        }
    })
    .await
    .expect("watches never attached");

    w.put("route.keep", b"v1").await.unwrap();
    let rev = w.put("route.gone", b"x").await.unwrap().as_u64().unwrap();
    watcher.wait_applied(rev).await;
    let resume = watcher.stop().await.expect("watcher ran clean");

    // --- Offline ------------------------------------------------------------
    w.put("route.late", b"gap-write").await.unwrap();
    w.delete("route.gone").await.unwrap();
    let stale_export = if export_before_eviction {
        let head = w.put("route.marker", b"m").await.unwrap().as_u64().unwrap();
        exporter.wait_applied(head).await;
        Some(exporter.publish(&*h.transport, h.dir.path()).await)
    } else {
        None
    };
    let final_rev = eviction.evict(&h.bucket).await;

    // The premise, asserted: NATS lost the current values (not deleted —
    // aged out), and the watcher's cursor is outside retention.
    let reader = h.bucket.reader();
    assert!(
        reader.get("route.keep").await.unwrap().is_none(),
        "route.keep must have aged out"
    );
    assert!(
        reader.get("route.late").await.unwrap().is_none(),
        "route.late must have aged out"
    );
    let first = h.first_sequence().await;
    let resume_rev = resume.as_u64().unwrap();
    assert!(
        first > resume_rev + 1,
        "first_seq {first} must pass the resume cursor {resume_rev}"
    );

    exporter.wait_applied(final_rev).await;
    (
        Gap {
            h,
            exporter,
            resume,
            final_rev,
        },
        stale_export,
    )
}

fn assert_converged(fold: &AppendLogSnapshot) {
    assert_eq!(
        fold.get("route.keep").unwrap().map(|e| e.value),
        Some(b"v1".to_vec()),
        "a key that aged out of NATS was never deleted: it stays"
    );
    assert_eq!(
        fold.get("route.late").unwrap().map(|e| e.value),
        Some(b"gap-write".to_vec()),
        "the write made while the watcher was offline is recovered"
    );
    assert!(
        fold.get("route.gone").unwrap().is_none(),
        "the real delete is applied"
    );
}

async fn restore_recovers(eviction: Eviction) {
    let (gap, _) = build_gap(eviction, false).await;
    let h = &gap.h;
    let artifact_rev = gap.exporter.publish(&*h.transport, h.dir.path()).await;
    assert!(artifact_rev >= gap.final_rev);

    // The live exporter never expired: it folded everything, the aged-out
    // keys included (NATS sends no marker when a value ages out).
    let exporter_fold = gap.exporter.stop().await.expect("exporter ran clean");
    assert_eq!(exporter_fold.as_u64(), Some(gap.final_rev));
    let (_c, exporter) = h.open("exporter.snap");
    assert_converged(&exporter);

    // The watcher comes back: expiry → (bucket evicts) → restore.
    let (cursor, fold) = h.open("watcher.snap");
    assert_eq!(cursor, gap.resume);
    let watcher = Node::spawn(&h.bucket, fold, Some(cursor), h.auto());
    watcher.wait_applied(artifact_rev).await;
    let end = watcher.stop().await.expect("restore succeeded");
    assert_eq!(end.as_u64(), Some(artifact_rev));

    let (cursor, watcher) = h.open("watcher.snap");
    assert_eq!(cursor.as_u64(), Some(artifact_rev));
    assert_converged(&watcher);
    assert_eq!(
        watcher
            .range("")
            .unwrap()
            .iter()
            .map(|e| (&e.key, &e.value))
            .collect::<Vec<_>>(),
        exporter
            .range("")
            .unwrap()
            .iter()
            .map(|e| (&e.key, &e.value))
            .collect::<Vec<_>>(),
        "the restored watcher is identical to the exporter"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn discard_old_expiry_restores_from_artifact() {
    restore_recovers(Eviction::DiscardOld).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn max_age_expiry_restores_from_artifact() {
    restore_recovers(Eviction::MaxAge).await;
}

/// The reported bug, pinned against a live server: the key-listing resync on
/// an evicting bucket would delete `route.keep` (NATS no longer lists it).
/// The legacy `Some(reader)` repair is now refused, and the fold is left
/// exactly as the watcher last persisted it.
#[tokio::test(flavor = "multi_thread")]
async fn relist_on_evicting_bucket_is_refused() {
    let (gap, _) = build_gap(Eviction::DiscardOld, false).await;
    let h = &gap.h;
    let (cursor, fold) = h.open("watcher.snap");
    let legacy: Option<Arc<dyn KvReader>> = Some(h.bucket.reader());
    let watcher = Node::spawn(&h.bucket, fold, Some(cursor.clone()), legacy.into());
    let err = timeout(Duration::from_secs(15), watcher.task)
        .await
        .expect("the watch must fail, not hang")
        .unwrap()
        .expect_err("relist on an evicting bucket must be refused");
    assert!(err.to_string().contains("evicts current values"), "{err}");

    let (after, fold) = h.open("watcher.snap");
    assert_eq!(after, cursor, "cursor untouched");
    assert_eq!(
        fold.get("route.keep").unwrap().map(|e| e.value),
        Some(b"v1".to_vec()),
        "the valid key the pre-fix resync deleted is still there"
    );
}

/// An artifact exported before the eviction is ahead of the watcher but its
/// cursor fell out of retention with everything else: whatever was evicted
/// between it and the log's head exists nowhere. No safe recovery — the
/// watch fails and the fold stays as it was.
#[tokio::test(flavor = "multi_thread")]
async fn stale_artifact_fails_the_watch() {
    let (gap, stale) = build_gap(Eviction::DiscardOld, true).await;
    let h = &gap.h;
    let stale = stale.expect("published before eviction");
    assert!(
        stale > gap.resume.as_u64().unwrap(),
        "ahead of the watcher, but stale"
    );

    let (cursor, fold) = h.open("watcher.snap");
    let watcher = Node::spawn(&h.bucket, fold, Some(cursor.clone()), h.auto());
    let err = timeout(Duration::from_secs(15), watcher.task)
        .await
        .expect("the watch must fail, not hang")
        .unwrap()
        .expect_err("a stale artifact must fail the watch");
    assert!(err.to_string().contains("retention window"), "{err}");
    let (after, fold) = h.open("watcher.snap");
    assert_eq!(after, cursor);
    assert!(fold.get("route.keep").unwrap().is_some());
}
