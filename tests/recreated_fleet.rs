//! A bucket deleted and recreated under the same name, with artifacts in play:
//! the fleet findings F1, F2 and F4 of the Verus fleet proof, each reached
//! against a live nats-server and a real transport (local filesystem object
//! store). `tests/recreated_stream.rs` covers the key-listing repair; these
//! cover what an artifact adds.
//!
//! - **F1**: a dead incarnation's pointer, ranked above the new stream's head,
//!   must not refuse the new stream's publishes.
//! - **F2**: a restore over a fold of the old stream must replace it with the
//!   artifact's, key for key: the old stream's revision numbers decide nothing
//!   in the new one, and the old cursor's rank is no bar to the artifact.
//! - **F4**: the eviction memo is a fact about the stream that was evicting;
//!   a recreated stream that keeps its values is served by the exact listing.
#![cfg(feature = "transport")]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::snapshot::SnapshotStore;
use slipstream::{
    AppendLogSnapshot, ArtifactRestore, ArtifactTransport, BatchConfig, Connection, DiscardPolicy,
    ExpiryRepair, ExportRequest, KvStore, KvUpdate, NatsConnection, NatsConnectionConfig,
    ObjectStoreTransport, PublishOutcome, StoreConfig, WatchCursor, WatchScope, watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

const BUCKET: &str = "routes";
const KEY: &str = "routes";

fn plain() -> StoreConfig {
    StoreConfig {
        name: BUCKET.into(),
        max_bytes: Some(8 * 1024 * 1024),
        ..Default::default()
    }
}

/// `discard: old` under a small `max_bytes`: filler writes evict current values.
fn evicting() -> StoreConfig {
    StoreConfig {
        name: BUCKET.into(),
        max_bytes: Some(4096),
        discard: DiscardPolicy::Old,
        ..Default::default()
    }
}

async fn open_bucket(nats: &TestNats, config: StoreConfig) -> (NatsConnection, Arc<dyn KvStore>) {
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.expect("connect");
    let store = conn.store_with_config(config).await.expect("open bucket");
    (conn, store)
}

/// Delete the bucket and create it again under the same name.
async fn recreate(nats: &TestNats, config: StoreConfig) -> (NatsConnection, Arc<dyn KvStore>) {
    let client = async_nats::connect(&nats.url).await.expect("connect");
    async_nats::jetstream::new(client)
        .delete_key_value(BUCKET)
        .await
        .expect("delete the bucket");
    open_bucket(nats, config).await
}

async fn put(bucket: &Arc<dyn KvStore>, key: &str, value: &str) -> u64 {
    bucket
        .writer()
        .expect("writer")
        .put(key, value.as_bytes())
        .await
        .expect("put")
        .as_u64()
        .expect("revision")
}

struct Node {
    applied: Arc<AtomicU64>,
    exports: mpsc::Sender<ExportRequest>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<WatchCursor, slipstream::KvError>>,
}

fn spawn_node(
    bucket: &Arc<dyn KvStore>,
    fold: AppendLogSnapshot,
    resume: Option<WatchCursor>,
    repair: ExpiryRepair<AppendLogSnapshot>,
) -> Node {
    let (ex_tx, ex_rx) = mpsc::channel(1);
    let (sd_tx, sd_rx) = watch::channel(false);
    let applied = Arc::new(AtomicU64::new(0));
    let applied_w = Arc::clone(&applied);
    let task = tokio::spawn(watch_applied(
        bucket.watcher().expect("watcher"),
        WatchScope::All,
        resume,
        repair,
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        |_: &KvUpdate| Some(()),
        |_: Vec<()>| {},
        move |cur: WatchCursor| applied_w.store(cur.as_u64().unwrap_or(0), Ordering::SeqCst),
        sd_rx,
    ));
    Node {
        applied,
        exports: ex_tx,
        shutdown: sd_tx,
        task,
    }
}

async fn wait_applied(node: &Node, at_least: u64) {
    let outcome = timeout(Duration::from_secs(15), async {
        while node.applied.load(Ordering::SeqCst) < at_least {
            if node.task.is_finished() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        true
    })
    .await;
    assert!(
        outcome == Ok(true),
        "the fold never applied revision {at_least} (at {}); the watch {}",
        node.applied.load(Ordering::SeqCst),
        if node.task.is_finished() {
            "ended"
        } else {
            "ran on"
        }
    );
}

async fn stop(node: Node) -> Result<WatchCursor, slipstream::KvError> {
    node.shutdown.send(true).unwrap();
    node.task.await.unwrap()
}

/// Export a live node's fold to `dest`.
async fn export(node: &Node, dest: &Path) {
    let (reply_tx, reply_rx) = oneshot::channel();
    node.exports
        .send(ExportRequest {
            dest_dir: dest.to_path_buf(),
            reply: reply_tx,
        })
        .await
        .expect("export request");
    reply_rx.await.expect("reply").expect("export");
}

struct Fleet {
    nats: TestNats,
    dir: TempDir,
    transport: Arc<dyn ArtifactTransport>,
}

impl Fleet {
    async fn new() -> Fleet {
        let nats = TestNats::start().await;
        let dir = TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("objects")).unwrap();
        let store =
            object_store::local::LocalFileSystem::new_with_prefix(dir.path().join("objects"))
                .expect("local object store");
        let transport: Arc<dyn ArtifactTransport> = Arc::new(
            ObjectStoreTransport::new(Arc::new(store), "artifacts")
                .with_non_atomic_pointer_fallback(),
        );
        Fleet {
            nats,
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

    fn auto(&self, bucket: &Arc<dyn KvStore>) -> ExpiryRepair<AppendLogSnapshot> {
        ExpiryRepair::Auto {
            reader: bucket.reader(),
            restore: self.restore(),
        }
    }

    fn restore_only(&self, bucket: &Arc<dyn KvStore>) -> ExpiryRepair<AppendLogSnapshot> {
        ExpiryRepair::Restore {
            reader: bucket.reader(),
            restore: self.restore(),
        }
    }

    fn restore(&self) -> Arc<ArtifactRestore<AppendLogSnapshot>> {
        Arc::new(ArtifactRestore::new(
            Arc::clone(&self.transport),
            KEY,
            self.dir.path(),
            |artifact: &Path, dest: &Path| AppendLogSnapshot::import(artifact, dest, u64::MAX),
        ))
    }

    /// Export a live node and publish the artifact.
    async fn publish(&self, node: &Node, name: &str) -> PublishOutcome {
        let dest = self.path(name);
        export(node, &dest).await;
        self.transport.upload(KEY, &dest).await.expect("upload")
    }
}

/// The fold's keys and values, and the bucket's: the same set.
async fn assert_fold_is_bucket(fold_path: &Path, bucket: &Arc<dyn KvStore>) {
    let (_, fold) = AppendLogSnapshot::open(fold_path, u64::MAX).expect("reopen the fold");
    let mut got: Vec<(String, Vec<u8>)> = fold
        .range("")
        .expect("range")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    got.sort();
    let mut want: Vec<(String, Vec<u8>)> = bucket
        .reader()
        .scan("")
        .await
        .expect("scan")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    want.sort();
    assert_eq!(got, want, "the fold is exactly the stream that exists now");
}

/// F1. The old stream's fleet published an artifact at cursor 20. The bucket
/// is recreated; its head is 5. The new stream's first export must replace the
/// dead incarnation's pointer, or no node of the new stream ever publishes
/// again (and `same_incarnation` refuses to restore from the dead one).
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_incarnations_pointer_does_not_refuse_the_new_streams_publish() {
    let fleet = Fleet::new().await;
    let (_c, old) = open_bucket(&fleet.nats, plain()).await;
    let a = spawn_node(&old, fleet.open("a").1, None, ExpiryRepair::None);
    for i in 0..20 {
        put(&old, &format!("old.{i}"), "o").await;
    }
    wait_applied(&a, 20).await;
    assert_eq!(fleet.publish(&a, "art-a").await, PublishOutcome::Published);
    stop(a).await.expect("clean stop");
    assert_eq!(
        fleet.transport.manifest(KEY).await.unwrap().cursor.as_u64(),
        Some(20)
    );

    let (_c, new) = recreate(&fleet.nats, plain()).await;
    let mut head = 0;
    for i in 0..5 {
        head = put(&new, &format!("new.{i}"), "n").await;
    }
    let b = spawn_node(&new, fleet.open("b").1, None, ExpiryRepair::None);
    wait_applied(&b, head).await;
    assert_eq!(
        fleet.publish(&b, "art-b").await,
        PublishOutcome::Published,
        "the new stream's artifact replaces the dead incarnation's"
    );
    assert_eq!(
        fleet.transport.manifest(KEY).await.unwrap().cursor.as_u64(),
        Some(head)
    );
    stop(b).await.expect("clean stop");
}

/// F2. A fold of the old stream, repaired by a restore from an artifact of the
/// new one. The old fold holds `k` at revision 18 ("old"); the new stream wrote
/// `k` at revision 1 ("new"). By revision number the old entry looks newer.
/// `new_head` below the old cursor (20) also checks the old cursor's rank is no
/// bar to the artifact.
async fn restore_over_a_foreign_fold(new_head: u64) {
    let fleet = Fleet::new().await;
    let (_c, old) = open_bucket(&fleet.nats, plain()).await;
    let a = spawn_node(&old, fleet.open("a").1, None, ExpiryRepair::None);
    for i in 0..17 {
        put(&old, &format!("o.{i}"), "o").await;
    }
    assert_eq!(put(&old, "k", "old").await, 18);
    put(&old, "o.17", "o").await;
    put(&old, "o.18", "o").await;
    wait_applied(&a, 20).await;
    stop(a).await.expect("clean stop");
    let (cursor, _) = fleet.open("a");
    assert_eq!(cursor.as_u64(), Some(20));

    let (_c, new) = recreate(&fleet.nats, plain()).await;
    put(&new, "k", "new").await;
    let mut head = 1;
    for i in 1..new_head {
        head = put(&new, &format!("n.{i}"), "n").await;
    }
    let exporter = spawn_node(&new, fleet.open("x").1, None, ExpiryRepair::None);
    wait_applied(&exporter, head).await;
    assert_eq!(
        fleet.publish(&exporter, "art").await,
        PublishOutcome::Published
    );
    stop(exporter).await.expect("clean stop");

    let (cursor, fold) = fleet.open("a");
    let node = spawn_node(&new, fold, Some(cursor), fleet.restore_only(&new));
    wait_applied(&node, head).await;
    stop(node).await.expect("clean stop");
    assert_fold_is_bucket(&fleet.path("a"), &new).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_over_a_foreign_fold_replaces_entries_the_old_stream_numbered_higher() {
    restore_over_a_foreign_fold(25).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_over_a_foreign_fold_is_not_barred_by_the_old_cursors_rank() {
    restore_over_a_foreign_fold(5).await;
}

/// F4. The fold saw its bucket evict current values (the memo rides its
/// cursor). The bucket is recreated without eviction: the exact listing serves
/// the repair, with no artifact anywhere. The memo is the old stream's.
#[tokio::test(flavor = "multi_thread")]
async fn the_old_streams_eviction_memo_does_not_distrust_the_new_streams_listing() {
    let fleet = Fleet::new().await;
    let (_c, old) = open_bucket(&fleet.nats, evicting()).await;
    put(&old, "route.keep", "v1").await;
    let node = spawn_node(&old, fleet.open("n").1, None, fleet.auto(&old));
    let mut last = 0;
    for i in 0..200 {
        last = put(&old, &format!("filler.{i}"), &"x".repeat(64)).await;
    }
    wait_applied(&node, last).await;
    stop(node).await.expect("clean stop");
    // A restart on the evicting bucket reads retention and commits the memo.
    let (c, fold) = fleet.open("n");
    let node = spawn_node(&old, fold, Some(c), fleet.auto(&old));
    let marker = put(&old, "route.marker", "m").await;
    wait_applied(&node, marker).await;
    stop(node).await.expect("clean stop");
    let (c, _) = fleet.open("n");
    assert!(
        format!("{c:?}").contains("seen evicting"),
        "the fold remembers the eviction: {c:?}"
    );

    let (_c, new) = recreate(&fleet.nats, plain()).await;
    let mut head = 0;
    for i in 0..30 {
        head = put(&new, &format!("new.{i}"), "n").await;
    }
    let (c, fold) = fleet.open("n");
    let node = spawn_node(&new, fold, Some(c), fleet.auto(&new));
    wait_applied(&node, head).await;
    stop(node).await.expect("clean stop");
    assert_fold_is_bucket(&fleet.path("n"), &new).await;
    let (c, _) = fleet.open("n");
    assert!(
        !format!("{c:?}").contains("seen evicting"),
        "the new stream never evicted: {c:?}"
    );
}
