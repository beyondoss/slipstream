//! A bucket deleted and recreated under the same name is a NEW stream whose
//! revisions start again at 1. A fold's persisted cursor, and the cursor inside
//! an artifact a fresh node bootstraps from, are the OLD stream's positions.
//! Resuming from them must never happen: before this was guarded, a cursor above
//! the new head made the watch deliver nothing until the stream outgrew it
//! (silently skipping every new revision up to it; an AWS recovery test saw
//! cursor 49,980 against a new stream at 7,373 and 0 events for ~230 s), and a
//! cursor inside the new range spliced the old stream's keys under the new
//! stream's tail.
//!
//! Each test drives a real `nats-server` (`tests/common`). The model
//! (`tests/model_incarnation.rs`) proves the guard; these prove the code
//! executes it, on both paths and in both shapes (cursor above the new head,
//! and the harder one — the new stream already grown past the old cursor,
//! where a sequence-only check cannot tell).
//!
//! Every test asserts the end state of the fold against the bucket that exists
//! now, so each fails without the guard (old keys left in the fold, or new
//! keys never delivered) and passes with it.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::{
    AppendLogSnapshot, BatchConfig, Connection, ExpiryRepair, ExportRequest, KvStore, KvUpdate,
    NatsConnection, NatsConnectionConfig, SnapshotStore, StoreConfig, WatchCursor, WatchScope,
    watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

const BUCKET: &str = "routes";

async fn open_bucket(nats: &TestNats) -> (NatsConnection, Arc<dyn KvStore>) {
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.expect("connect");
    let store = conn
        .store_with_config(StoreConfig {
            name: BUCKET.into(),
            max_bytes: Some(8 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open bucket");
    (conn, store)
}

/// Delete the bucket and create it again under the same name.
async fn recreate(nats: &TestNats) -> (NatsConnection, Arc<dyn KvStore>) {
    let client = async_nats::connect(&nats.url).await.expect("connect");
    async_nats::jetstream::new(client)
        .delete_key_value(BUCKET)
        .await
        .expect("delete the bucket");
    open_bucket(nats).await
}

async fn put_all(bucket: &Arc<dyn KvStore>, prefix: &str, n: u64) -> u64 {
    let writer = bucket.writer().expect("writer");
    let mut last = 0;
    for i in 0..n {
        last = writer
            .put(&format!("{prefix}.{i}"), format!("{prefix}-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("revision");
    }
    last
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
    timeout(Duration::from_secs(10), async {
        while node.applied.load(Ordering::SeqCst) < at_least {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the fold never applied revision {at_least} (at {}): the watch skipped the new \
             stream's revisions",
            node.applied.load(Ordering::SeqCst)
        )
    });
}

async fn stop(node: Node) -> Result<WatchCursor, slipstream::KvError> {
    node.shutdown.send(true).unwrap();
    node.task.await.unwrap()
}

fn relist(bucket: &Arc<dyn KvStore>) -> ExpiryRepair<AppendLogSnapshot> {
    ExpiryRepair::Relist(bucket.reader())
}

/// The fold's keys and values, and the bucket's: they must be the same set.
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
    assert_eq!(
        got.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        want.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        "the fold holds exactly the keys of the stream that exists now"
    );
    assert_eq!(got, want, "and their values");
}

/// A fold built on the old stream: `old.0..old.{n-1}` at revisions 1..=n,
/// stopped cleanly. Returns the fold path and its persisted cursor.
async fn fold_the_old_stream(
    bucket: &Arc<dyn KvStore>,
    dir: &Path,
    n: u64,
) -> (std::path::PathBuf, WatchCursor) {
    let path = dir.join("fold");
    let (_, fold) = AppendLogSnapshot::open(&path, u64::MAX).expect("open");
    put_all(bucket, "old", n).await;
    let node = spawn_node(bucket, fold, None, relist(bucket));
    wait_applied(&node, n).await;
    stop(node).await.expect("clean stop");
    let (cursor, _) = AppendLogSnapshot::open(&path, u64::MAX).expect("reopen");
    assert_eq!(cursor.as_u64(), Some(n));
    (path, cursor)
}

/// WARM FOLD, cursor above the new head (the AWS recovery case): the new
/// stream has fewer revisions than the old cursor.
#[tokio::test(flavor = "multi_thread")]
async fn warm_fold_with_a_cursor_beyond_the_new_head_reseeds() {
    let nats = TestNats::start().await;
    let (_c, old) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, cursor) = fold_the_old_stream(&old, dir.path(), 20).await;

    let (_c, new) = recreate(&nats).await;
    let head = put_all(&new, "new", 5).await;
    assert!(
        head < cursor.as_u64().unwrap(),
        "the cursor is beyond the head"
    );

    let (cursor, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&new, fold, Some(cursor), relist(&new));
    wait_applied(&node, head).await;
    stop(node).await.expect("clean stop");
    assert_fold_is_bucket(&path, &new).await;
}

/// WARM FOLD, the harder case: the new stream has already grown PAST the old
/// cursor, so the cursor is a perfectly possible position in it and a
/// sequence-only check admits it. The fold must not splice the old stream's
/// keys under the new stream's tail.
#[tokio::test(flavor = "multi_thread")]
async fn warm_fold_with_a_cursor_inside_the_new_range_reseeds() {
    let nats = TestNats::start().await;
    let (_c, old) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, cursor) = fold_the_old_stream(&old, dir.path(), 20).await;

    let (_c, new) = recreate(&nats).await;
    let head = put_all(&new, "new", 25).await;
    assert!(
        head > cursor.as_u64().unwrap(),
        "the new stream outgrew the cursor"
    );

    let (cursor, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&new, fold, Some(cursor), relist(&new));
    wait_applied(&node, head).await;
    stop(node).await.expect("clean stop");
    assert_fold_is_bucket(&path, &new).await;
}

/// With no repair armed the same fold is REFUSED, loudly, not resumed.
#[tokio::test(flavor = "multi_thread")]
async fn warm_fold_with_no_repair_armed_is_refused() {
    let nats = TestNats::start().await;
    let (_c, old) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, _) = fold_the_old_stream(&old, dir.path(), 20).await;

    let (_c, new) = recreate(&nats).await;
    put_all(&new, "new", 25).await;

    let (cursor, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&new, fold, Some(cursor), ExpiryRepair::None);
    let result = timeout(Duration::from_secs(10), node.task)
        .await
        .expect("the watch must end with an error, not run on")
        .unwrap();
    let err = result.expect_err("a cursor from another incarnation must not resume");
    assert!(
        err.to_string().contains("different incarnation"),
        "the error names the cause: {err}"
    );
    // Nothing of the new stream was folded under the old cursor.
    let (cursor, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    assert_eq!(cursor.as_u64(), Some(20), "the fold is untouched");
    assert!(fold.get("new.0").unwrap().is_none());
}

/// FRESH BOOTSTRAP from an artifact exported on the old incarnation, in both
/// shapes: the new stream behind the artifact's cursor, and past it.
async fn bootstrap_from_an_old_artifact(new_puts: u64) {
    let nats = TestNats::start().await;
    let (_c, old) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();

    // Node A folds the old stream and exports.
    let (_, fold_a) = AppendLogSnapshot::open(&dir.path().join("a"), u64::MAX).unwrap();
    let node_a = spawn_node(&old, fold_a, None, relist(&old));
    let n = 20;
    put_all(&old, "old", n).await;
    wait_applied(&node_a, n).await;
    let artifact = dir.path().join("artifact");
    let (reply_tx, reply_rx) = oneshot::channel();
    node_a
        .exports
        .send(ExportRequest {
            dest_dir: artifact.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
    let manifest = reply_rx.await.unwrap().expect("export");
    assert_eq!(manifest.cursor.as_u64(), Some(n));
    stop(node_a).await.expect("clean stop");

    // The bucket is recreated; node B bootstraps from the old artifact.
    let (_c, new) = recreate(&nats).await;
    let head = put_all(&new, "new", new_puts).await;
    let dest = dir.path().join("b");
    let (cursor, fold_b) = AppendLogSnapshot::import(&artifact, &dest, u64::MAX).unwrap();
    assert_eq!(cursor.as_u64(), Some(n));

    let node_b = spawn_node(&new, fold_b, Some(cursor), relist(&new));
    wait_applied(&node_b, head).await;
    stop(node_b).await.expect("clean stop");
    assert_fold_is_bucket(&dest, &new).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_from_an_old_artifact_beyond_the_new_head_reseeds() {
    bootstrap_from_an_old_artifact(5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_from_an_old_artifact_inside_the_new_range_reseeds() {
    bootstrap_from_an_old_artifact(25).await;
}

/// THE NORMAL CASE IS NOT BLOCKED: same stream, restart, resume — delta-only,
/// no repair, no error — and an artifact of the same stream bootstraps a node
/// the same way.
#[tokio::test(flavor = "multi_thread")]
async fn same_stream_restart_and_bootstrap_resume_without_repair() {
    let nats = TestNats::start().await;
    let (_c, bucket) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, cursor) = fold_the_old_stream(&bucket, dir.path(), 20).await;
    let head = put_all(&bucket, "more", 5).await;

    // No repair armed: any refusal would surface as an error.
    let (_, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&bucket, fold, Some(cursor), ExpiryRepair::None);
    wait_applied(&node, head).await;

    // Export the running fold and bootstrap a second node from it.
    let artifact = dir.path().join("artifact");
    let (reply_tx, reply_rx) = oneshot::channel();
    node.exports
        .send(ExportRequest {
            dest_dir: artifact.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
    let manifest = reply_rx.await.unwrap().expect("export");
    let tail = put_all(&bucket, "tail", 3).await;
    let dest = dir.path().join("b");
    let (cursor_b, fold_b) = AppendLogSnapshot::import(&artifact, &dest, u64::MAX).unwrap();
    assert_eq!(cursor_b, manifest.cursor);
    let node_b = spawn_node(&bucket, fold_b, Some(cursor_b), ExpiryRepair::None);
    wait_applied(&node_b, tail).await;

    stop(node).await.expect("restart resumed without error");
    stop(node_b).await.expect("bootstrap resumed without error");
    assert_fold_is_bucket(&path, &bucket).await;
    assert_fold_is_bucket(&dest, &bucket).await;
}

/// A cursor written before cursors recorded an incarnation (every fold a
/// 0.9.x build left behind) keeps resuming on its own stream: it carries no
/// evidence, so it is admitted while its position is possible in the stream.
#[tokio::test(flavor = "multi_thread")]
async fn unrecorded_cursor_on_the_same_stream_still_resumes() {
    let nats = TestNats::start().await;
    let (_c, bucket) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, cursor) = fold_the_old_stream(&bucket, dir.path(), 20).await;
    let head = put_all(&bucket, "more", 5).await;

    // Strip what this build recorded: the bare revision, as 0.9.x wrote it.
    let legacy = WatchCursor::from_u64(cursor.as_u64().unwrap());
    assert_eq!(legacy.to_bytes().len(), 8);
    let (_, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&bucket, fold, Some(legacy), ExpiryRepair::None);
    wait_applied(&node, head).await;
    stop(node).await.expect("resumed without error");
    assert_fold_is_bucket(&path, &bucket).await;
}

/// ... and a cursor with no recorded incarnation that is BEYOND the new
/// stream's head can only be the old stream's: refused.
#[tokio::test(flavor = "multi_thread")]
async fn unrecorded_cursor_beyond_the_head_is_refused() {
    let nats = TestNats::start().await;
    let (_c, old) = open_bucket(&nats).await;
    let dir = TempDir::new().unwrap();
    let (path, cursor) = fold_the_old_stream(&old, dir.path(), 20).await;
    let (_c, new) = recreate(&nats).await;
    put_all(&new, "new", 5).await;

    let legacy = WatchCursor::from_u64(cursor.as_u64().unwrap());
    let (_, fold) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    let node = spawn_node(&new, fold, Some(legacy), ExpiryRepair::None);
    let err = timeout(Duration::from_secs(10), node.task)
        .await
        .expect("must end with an error")
        .unwrap()
        .expect_err("refused");
    assert!(err.to_string().contains("different incarnation"), "{err}");
}
