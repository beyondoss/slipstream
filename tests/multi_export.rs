//! Empirical multi-exporter consistency proofs (feature `transport` + an LSM
//! backend), the live twins of the `pointer_swap` model in `tests/model.rs`:
//! real fjall/RocksDB folds driven by live `watch_applied` loops over a
//! throwaway NATS server, genuinely diverged across nodes, driving the exact
//! interleavings the legacy two-register layout failed under — and asserting
//! the shipped content-addressed + monotonic-pointer protocol PREVENTS them.
//!
//! 1. **Slow-exporter clobber → REFUSED.** Node A wins a round and stalls in
//!    upload past its lease ttl; node B takes over and publishes a NEWER
//!    artifact; A's stale upload then lands LAST. A's payload goes to its own
//!    content address (clobbering nothing) and its pointer swap is refused:
//!    the published "latest" never regresses, the lease record and the
//!    pointer agree, and a bootstrapper gets B's newest state. (Also proven:
//!    concurrent exporters' artifacts genuinely differ — each replica
//!    exports at its own applied cursor — which is exactly why the pointer
//!    must be monotonic rather than last-write-wins.)
//!
//! 2. **Crash between payload and pointer → NOTHING tears.** The crash
//!    window that tore the legacy layout (payload landed, manifest didn't)
//!    now leaves the old pointer fully consistent: peek and download serve
//!    the old artifact throughout; the next round publishes the new one.
//!
//! 3. **Post-compaction, multi-data-file fidelity.** A fold with real LSM
//!    structure — flush cycles, overwrites, tombstones, a `settle()`
//!    compaction, a fresh tail — exports a multi-SST/multi-table artifact;
//!    the source then churns and compacts AGAIN (rewriting/unlinking the
//!    files the RocksDB checkpoint hardlinked) before the slow upload ships
//!    it. Every hash verifies; the import is byte-exactly the at-export
//!    state.
#![cfg(all(feature = "transport", any(feature = "fjall", feature = "rocksdb")))]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common::{ManifestPutCrash, TestNats};
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    ArtifactTransport, BatchConfig, Connection, ExportLease, ExportManifest, ExportRequest,
    KvStore, KvUpdate, NatsConnection, NatsConnectionConfig, ObjectStoreTransport, PublishOutcome,
    StoreConfig, WatchCursor, WatchScope, run_export_round, watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

// --- Shared live-fleet harness (same shape as tests/bootstrap.rs) -------------

async fn open_buckets(nats: &TestNats) -> (NatsConnection, Arc<dyn KvStore>, Arc<dyn KvStore>) {
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.expect("connect");
    let routes = conn
        .store_with_config(StoreConfig {
            name: "routes".into(),
            max_bytes: Some(8 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open routes bucket");
    let leases = conn
        .store_with_config(StoreConfig {
            name: "leases".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open lease bucket");
    (conn, routes, leases)
}

struct Node {
    exports: mpsc::Sender<ExportRequest>,
    applied: Arc<AtomicU64>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<WatchCursor, slipstream::KvError>>,
}

fn spawn_node<S: SnapshotStore + Send + 'static>(
    bucket: &Arc<dyn KvStore>,
    fold: S,
    resume: Option<WatchCursor>,
) -> Node {
    let watcher = bucket.watcher().expect("bucket watcher");
    let (ex_tx, ex_rx) = mpsc::channel(1);
    let (sd_tx, sd_rx) = watch::channel(false);
    let applied = Arc::new(AtomicU64::new(0));
    let applied_w = Arc::clone(&applied);

    let task = tokio::spawn(watch_applied(
        watcher,
        WatchScope::All,
        resume,
        None,
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        |u: &KvUpdate| match u {
            KvUpdate::Put(e) => Some(e.key.clone()),
            _ => None,
        },
        |_batch: Vec<String>| {},
        move |cur: WatchCursor| {
            applied_w.store(cur.as_u64().unwrap_or(0), Ordering::SeqCst);
        },
        sd_rx,
    ));

    Node {
        exports: ex_tx,
        applied,
        shutdown: sd_tx,
        task,
    }
}

async fn wait_applied(node: &Node, at_least: u64) {
    timeout(Duration::from_secs(10), async {
        loop {
            if node.applied.load(Ordering::SeqCst) >= at_least {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "node never applied rev {at_least} (at {})",
            node.applied.load(Ordering::SeqCst)
        )
    });
}

/// Export through a live node's request channel into `dest`.
async fn export_via(node: &Node, dest: &Path) -> ExportManifest {
    let (reply_tx, reply_rx) = oneshot::channel();
    node.exports
        .send(ExportRequest {
            dest_dir: dest.to_path_buf(),
            reply: reply_tx,
        })
        .await
        .expect("send export request");
    reply_rx.await.expect("reply").expect("export succeeds")
}

/// The lease's `completed_cursor_hex` rendering of a u64-revision cursor:
/// 8 bytes big-endian, lowercase hex (see `integration.rs`'s
/// `export_lease_complete_publishes_outcome`).
fn cursor_hex(c: &WatchCursor) -> String {
    format!("{:016x}", c.as_u64().expect("u64 cursor"))
}

type ImportFn<S> = fn(&Path, &Path) -> Result<(WatchCursor, S), SnapshotError>;

// --- Scenario 1: slow-exporter clobber → refused by the monotonic pointer ----

/// An [`ArtifactTransport`] whose `upload` parks on a gate — the stand-in for
/// a slow tar/multipart on a stalled node. Signals `reached` when the round
/// arrives at upload (its export is already done by then), then waits for a
/// permit before delegating to the real transport.
struct GatedTransport {
    inner: ObjectStoreTransport,
    gate: tokio::sync::Semaphore,
    reached: std::sync::Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl ArtifactTransport for GatedTransport {
    async fn upload(
        &self,
        key: &str,
        artifact_dir: &Path,
    ) -> Result<PublishOutcome, SnapshotError> {
        if let Some(tx) = self.reached.lock().unwrap().take() {
            let _ = tx.send(());
        }
        let _permit = self.gate.acquire().await.expect("gate never closed");
        self.inner.upload(key, artifact_dir).await
    }
    async fn manifest(&self, key: &str) -> Result<ExportManifest, SnapshotError> {
        self.inner.manifest(key).await
    }
    async fn download(&self, key: &str, dest_dir: &Path) -> Result<ExportManifest, SnapshotError> {
        self.inner.download(key, dest_dir).await
    }
}

/// Two replicas of one fold, genuinely diverged (A's artifact is frozen at the
/// pre-churn cursor while B keeps applying), racing `run_export_round` to the
/// same remote key. A stalls in upload past its ttl; B takes the round over
/// and publishes newer; A's stale upload lands last — and the monotonic
/// pointer swap REFUSES it. The published "latest" never regresses.
async fn slow_exporter_cannot_clobber_newer_artifact<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) where
    S: SnapshotStore + Send + 'static,
{
    let nats = TestNats::start().await;
    let (_conn, bucket, leases) = open_buckets(&nats).await;
    let writer = bucket.writer().expect("writer");
    let dir = TempDir::new().unwrap();

    let bucket_dir = TempDir::new().unwrap();
    let fs =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap());
    let transport = ObjectStoreTransport::new(fs.clone(), "artifacts");

    // Two live replicas of the same fold.
    let (_ra, fold_a) = open(&dir.path().join("node-a"));
    let (_rb, fold_b) = open(&dir.path().join("node-b"));
    let node_a = spawn_node(&bucket, fold_a, None);
    let node_b = spawn_node(&bucket, fold_b, None);

    // Deterministic attach for BOTH consumers (KV watches deliver new updates
    // only), then the pre-churn history.
    let attach_rev = timeout(Duration::from_secs(10), async {
        loop {
            let v = writer.put("route.seed", b"seed").await.expect("seed");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let rev = v.as_u64().expect("nats rev");
            if node_a.applied.load(Ordering::SeqCst) > 0
                && node_b.applied.load(Ordering::SeqCst) > 0
            {
                return rev;
            }
        }
    })
    .await
    .expect("watches never attached");

    let mut pre_rev = attach_rev;
    for i in 0..10u64 {
        pre_rev = writer
            .put(&format!("route.pre.{i}"), format!("pre-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_a, pre_rev).await;
    wait_applied(&node_b, pre_rev).await;

    // Node A wins a round with a short ttl and stalls in upload. The round
    // runs the REAL composed path — lease, live export, transport — only the
    // upload is parked on the gate.
    let (reached_tx, reached_rx) = oneshot::channel();
    let gated = Arc::new(GatedTransport {
        inner: ObjectStoreTransport::new(fs.clone(), "artifacts"),
        gate: tokio::sync::Semaphore::new(0),
        reached: std::sync::Mutex::new(Some(reached_tx)),
    });
    let scratch_a = dir.path().join("scratch-a");
    std::fs::create_dir(&scratch_a).unwrap();
    let task_a = {
        let leases = Arc::clone(&leases);
        let exports = node_a.exports.clone();
        let gated = Arc::clone(&gated);
        let scratch = scratch_a.clone();
        tokio::spawn(async move {
            let lease = ExportLease::new(leases.as_ref(), "round", "node-a").unwrap();
            run_export_round(
                &lease,
                Duration::from_secs(2),
                &exports,
                gated.as_ref(),
                "edge/latest",
                &scratch,
            )
            .await
        })
    };
    reached_rx.await.expect("node A reached upload");

    // Post-export churn: node B's fold moves past the cursor frozen in A's
    // artifact — the replicas have genuinely diverged.
    let mut final_rev = pre_rev;
    for i in 0..8u64 {
        final_rev = writer
            .put(&format!("route.post.{i}"), format!("post-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_b, final_rev).await;

    // Node B takes the round over once A's ttl lapses (polled, not slept).
    let lease_b = ExportLease::new(leases.as_ref(), "round", "node-b").unwrap();
    let scratch_b = dir.path().join("scratch-b");
    std::fs::create_dir(&scratch_b).unwrap();
    let manifest_b = timeout(Duration::from_secs(15), async {
        loop {
            if let Some(m) = run_export_round(
                &lease_b,
                Duration::from_secs(60),
                &node_b.exports,
                &transport,
                "edge/latest",
                &scratch_b,
            )
            .await
            .expect("node B round")
            {
                return m;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("node B never won the round after A's ttl");
    assert_eq!(
        manifest_b.cursor.as_u64(),
        Some(final_rev),
        "B exported the full post-churn state"
    );
    assert_eq!(
        transport.manifest("edge/latest").await.unwrap().cursor,
        manifest_b.cursor,
        "remote holds B's newer artifact before A's stale upload lands"
    );

    // Release A: its stale upload lands LAST — the payload uploads to its
    // own content address (clobbering nothing) and the pointer swap REFUSES
    // the regression. The round still completes cleanly.
    gated.gate.add_permits(1);
    let manifest_a = task_a
        .await
        .unwrap()
        .expect("node A round returned an error")
        .expect("node A round completes despite overrunning its lease");

    // Divergence, empirically: concurrent exporters do NOT produce identical
    // artifacts — each replica exports at its own applied cursor. This is
    // exactly why the pointer must be monotonic rather than last-write-wins.
    let (rev_a, rev_b) = (
        manifest_a.cursor.as_u64().expect("rev a"),
        manifest_b.cursor.as_u64().expect("rev b"),
    );
    assert!(
        rev_a < rev_b,
        "concurrent exporters produced different artifacts (a={rev_a}, b={rev_b})"
    );

    // PREVENTION (model: `published cursor never regresses`): the remote
    // "latest" still holds B's newer artifact — A's stale publish was
    // superseded, not last-write-wins.
    let remote = transport.manifest("edge/latest").await.unwrap();
    assert_eq!(
        remote.cursor, manifest_b.cursor,
        "the pointer never regressed: B's newer artifact survived A's stale upload landing last"
    );

    // The fleet-visible lease record and the remote object AGREE (both B) —
    // under the legacy two-register layout they disagreed here.
    let record = lease_b.current().await.unwrap().expect("lease record");
    assert_eq!(record.holder_id, "node-b", "B owns the round record");
    assert_eq!(
        record.completed_cursor_hex.as_deref(),
        Some(cursor_hex(&manifest_b.cursor).as_str()),
        "the completion record and the published pointer agree on B's cursor"
    );

    // A bootstrapping node C gets B's artifact — the NEWEST published state,
    // post-churn keys included — and converges on the live tail.
    let dl = dir.path().join("downloaded");
    let got = transport.download("edge/latest", &dl).await.unwrap();
    assert_eq!(got.cursor, manifest_b.cursor);
    let dest_c = dir.path().join("node-c");
    let (cursor_c, fold_c) = import(&dl, &dest_c).expect("import published artifact");
    assert_eq!(cursor_c.as_u64(), Some(rev_b));
    assert_eq!(
        fold_c.range("route.post.").expect("range").len(),
        8,
        "the bootstrapped fold carries the full post-churn state"
    );

    // C is already AT the bucket head (it imported the newest artifact), so
    // prove live convergence with a fresh tail written after it resumes.
    let node_c = spawn_node(&bucket, fold_c, Some(cursor_c));
    let mut tail_rev = final_rev;
    for i in 0..3u64 {
        tail_rev = writer
            .put(&format!("route.tail.{i}"), format!("tail-{i}").as_bytes())
            .await
            .expect("put tail")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_c, tail_rev).await;
    node_c.shutdown.send(true).unwrap();
    node_c.task.await.unwrap().unwrap();

    let (final_cursor, fold_c) = open(&dest_c);
    assert_eq!(final_cursor.as_u64(), Some(tail_rev));
    let mut fold_state: Vec<(String, Vec<u8>)> = fold_c
        .range("route.")
        .expect("range")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    fold_state.sort();
    let mut bucket_state: Vec<(String, Vec<u8>)> = bucket
        .reader()
        .scan("route.")
        .await
        .expect("scan")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    bucket_state.sort();
    assert_eq!(
        fold_state, bucket_state,
        "the bootstrapped node converged to the bucket"
    );

    node_a.shutdown.send(true).unwrap();
    node_a.task.await.unwrap().unwrap();
    node_b.shutdown.send(true).unwrap();
    node_b.task.await.unwrap().unwrap();
}

// --- Scenario 2: crash between payload and pointer swap -----------------------
// (crash injection: common::ManifestPutCrash)

/// A healthy old artifact sits at "latest". A newer export's upload crashes
/// after its payload lands but before the pointer swap — and NOTHING tears:
/// the new payload sits at its own content address, the pointer still
/// references the old artifact, and every bootstrap keeps working throughout.
/// The next successful round publishes the new state.
async fn crash_between_payload_and_pointer_keeps_bootstrap_available<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) where
    S: SnapshotStore + Send + 'static,
{
    let nats = TestNats::start().await;
    let (_conn, bucket, _leases) = open_buckets(&nats).await;
    let writer = bucket.writer().expect("writer");
    let dir = TempDir::new().unwrap();

    let bucket_dir = TempDir::new().unwrap();
    let fs: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap());
    // Local FS lacks CAS, so the recovery publish (an Update over the old
    // pointer) takes the explicitly opted-in fallback; the crash-window
    // semantics under test are store-agnostic and re-proven on real CAS by
    // `transport_s3.rs`'s MinIO twin.
    let transport =
        ObjectStoreTransport::new(fs.clone(), "artifacts").with_non_atomic_pointer_fallback();
    let crash = Arc::new(ManifestPutCrash {
        inner: fs.clone(),
        armed: AtomicBool::new(false),
    });
    let crashing =
        ObjectStoreTransport::new(crash.clone(), "artifacts").with_non_atomic_pointer_fallback();

    // One live node; two real exports at genuinely different cursors.
    let (_r, fold) = open(&dir.path().join("node-a"));
    let node = spawn_node(&bucket, fold, None);
    let attach_rev = timeout(Duration::from_secs(10), async {
        loop {
            let v = writer.put("route.seed", b"seed").await.expect("seed");
            tokio::time::sleep(Duration::from_millis(50)).await;
            if node.applied.load(Ordering::SeqCst) > 0 {
                return v.as_u64().expect("rev");
            }
        }
    })
    .await
    .expect("watch never attached");

    let mut pre_rev = attach_rev;
    for i in 0..6u64 {
        pre_rev = writer
            .put(&format!("route.pre.{i}"), format!("pre-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node, pre_rev).await;
    let artifact_old = dir.path().join("artifact-old");
    let manifest_old = export_via(&node, &artifact_old).await;

    let mut final_rev = pre_rev;
    for i in 0..5u64 {
        final_rev = writer
            .put(&format!("route.post.{i}"), format!("post-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node, final_rev).await;
    let artifact_new = dir.path().join("artifact-new");
    let manifest_new = export_via(&node, &artifact_new).await;
    assert!(manifest_old.cursor.as_u64() < manifest_new.cursor.as_u64());

    // Healthy baseline: the old round completed fully.
    assert_eq!(
        transport.upload("latest", &artifact_old).await.unwrap(),
        PublishOutcome::Published
    );
    assert_eq!(
        transport.manifest("latest").await.unwrap().cursor,
        manifest_old.cursor
    );

    // The new round's upload "crashes" after its payload multipart completes,
    // before the pointer swap.
    crash.armed.store(true, Ordering::SeqCst);
    let err = crashing
        .upload("latest", &artifact_new)
        .await
        .expect_err("injected crash fails the upload");
    assert!(
        err.to_string().contains("injected crash"),
        "failed for the injected reason: {err}"
    );

    // PREVENTION (model: `cross-check never fires`): no torn state exists.
    // The pointer still references the OLD artifact in full — peek and
    // download both serve it; bootstrap stayed available straight through
    // the crash. (Under the legacy two-register layout this exact crash
    // broke every download until the next round.)
    assert_eq!(
        transport.manifest("latest").await.unwrap().cursor,
        manifest_old.cursor,
        "the pointer still advertises the old round, consistently"
    );
    let dl_during = dir.path().join("dl-during-crash-window");
    let got = transport
        .download("latest", &dl_during)
        .await
        .expect("bootstrap keeps working through the crash window");
    assert_eq!(got.cursor, manifest_old.cursor);
    let (cursor_during, fold_during) =
        import(&dl_during, &dir.path().join("node-during")).expect("import old artifact");
    assert_eq!(cursor_during, manifest_old.cursor);
    assert!(
        fold_during.range("route.post.").expect("range").is_empty(),
        "the crash window serves the old (consistent) state, not a torn one"
    );

    // The next successful round publishes the new state end-to-end.
    assert_eq!(
        transport.upload("latest", &artifact_new).await.unwrap(),
        PublishOutcome::Published
    );
    let dl = dir.path().join("dl-published");
    let got = transport.download("latest", &dl).await.unwrap();
    assert_eq!(got.cursor, manifest_new.cursor);
    let (cursor, fold_b) = import(&dl, &dir.path().join("node-b")).expect("import published");
    assert_eq!(cursor, manifest_new.cursor);
    assert_eq!(
        fold_b.range("route.post.").expect("range").len(),
        5,
        "the published artifact carries the post-churn keys"
    );

    node.shutdown.send(true).unwrap();
    node.task.await.unwrap().unwrap();
}

// --- Scenario 3: post-compaction, multi-file artifacts ------------------------

fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
    KvUpdate::Put(slipstream::KvEntry {
        key: key.to_string(),
        value: value.to_vec(),
        version: slipstream::VersionToken::from_u64(rev),
    })
}

fn del(key: &str, rev: u64) -> KvUpdate {
    KvUpdate::Delete {
        key: key.to_string(),
        version: slipstream::VersionToken::from_u64(rev),
    }
}

/// A ~2 KiB value unique to (generation, batch, key) so overwrites really
/// change bytes and post-export churn provably diverges the source.
fn big_value(generation: u32, batch: u64, i: u64) -> Vec<u8> {
    format!("g{generation}-b{batch}-i{i}-")
        .into_bytes()
        .into_iter()
        .cycle()
        .take(2048)
        .collect()
}

fn full_state<S: SnapshotStore>(fold: &S) -> Vec<(String, Vec<u8>)> {
    let mut state: Vec<(String, Vec<u8>)> = fold
        .range("route.")
        .expect("range")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    state.sort();
    state
}

/// The realistic-scale fidelity proof: a fold driven through real LSM
/// lifecycle — multiple flush cycles, overwrites, tombstones, an explicit
/// `settle()` compaction, plus a fresh post-compaction tail — exports a
/// MULTI-DATA-FILE artifact. The source then keeps churning and compacts
/// again, which rewrites/unlinks the very files the export staged (RocksDB
/// checkpoints HARDLINK live SSTs into the artifact; this is the stalled
/// node's artifact sitting in scratch while its fold moves on). Only then
/// does the "slow" upload ship it. Proves: every payload hash verifies after
/// the source churn, the import opens, and the imported state is exactly the
/// at-export state — multiple SSTs, compaction, and tombstones included.
async fn compacted_multi_file_artifact_survives_source_churn<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
    settle: fn(&S) -> Result<(), SnapshotError>,
    is_data_file: fn(&str) -> bool,
) where
    S: SnapshotStore + Send + 'static,
{
    let dir = TempDir::new().unwrap();
    let bucket_dir = TempDir::new().unwrap();
    let fs =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(bucket_dir.path()).unwrap());
    let transport = ObjectStoreTransport::new(fs, "artifacts");

    let (_r, mut fold) = open(&dir.path().join("source"));
    let mut rev = 0u64;

    // Real LSM history: 12 batches × 50 keys over a 100-key space (heavy
    // overwrites), a slug of deletes every 4th batch (tombstones), and a
    // mid-history settle so part of the history is flushed + compacted.
    for b in 0..12u64 {
        let mut batch = Vec::new();
        for i in 0..50u64 {
            rev += 1;
            let key = format!("route.{:03}", (b * 37 + i) % 100);
            batch.push(put(&key, &big_value(1, b, i), rev));
        }
        if b % 4 == 3 {
            for d in 0..10u64 {
                rev += 1;
                batch.push(del(&format!("route.{:03}", (b + d * 7) % 100), rev));
            }
        }
        fold.apply(&batch, &WatchCursor::from_u64(rev))
            .expect("apply history batch");
        if b == 5 {
            settle(&fold).expect("mid-history settle");
        }
    }
    // Compact the whole history, then lay a fresh un-compacted tail on top so
    // the export's own flush produces a new data file ALONGSIDE the compacted
    // one(s) — a genuinely multi-file payload.
    settle(&fold).expect("settle: flush + compact the history");
    let mut tail = Vec::new();
    for i in 0..30u64 {
        rev += 1;
        tail.push(put(
            &format!("route.{:03}", i * 3 % 100),
            &big_value(2, 99, i),
            rev,
        ));
    }
    fold.apply(&tail, &WatchCursor::from_u64(rev))
        .expect("apply post-compaction tail");

    let artifact = dir.path().join("artifact");
    let manifest = fold.export_to(&artifact).expect("export");
    assert_eq!(manifest.cursor.as_u64(), Some(rev));
    let data_files: Vec<&str> = manifest
        .files
        .iter()
        .filter(|f| is_data_file(&f.path))
        .map(|f| f.path.as_str())
        .collect();
    assert!(
        data_files.len() >= 2,
        "artifact must carry MULTIPLE data files (compacted history + fresh \
         tail); got {data_files:?} out of {:?}",
        manifest.files.iter().map(|f| &f.path).collect::<Vec<_>>()
    );
    let expected = full_state(&fold);

    // Post-export source churn: overwrite, delete, and compact AGAIN — on
    // RocksDB this rewrites/unlinks the very SSTs the artifact hardlinked.
    // The artifact must be immune (its links pin the immutable inodes).
    for b in 0..6u64 {
        let mut batch = Vec::new();
        for i in 0..50u64 {
            rev += 1;
            batch.push(put(
                &format!("route.{:03}", (b * 13 + i) % 100),
                &big_value(3, b, i),
                rev,
            ));
        }
        rev += 1;
        batch.push(del(&format!("route.{:03}", b * 11 % 100), rev));
        fold.apply(&batch, &WatchCursor::from_u64(rev))
            .expect("apply churn batch");
    }
    settle(&fold).expect("settle: compact the post-export churn");
    assert_ne!(
        full_state(&fold),
        expected,
        "the churn really diverged the source from the artifact"
    );

    // The "slow" upload ships the artifact only now — after its source files
    // were compacted away — then a bootstrap round-trips it.
    assert_eq!(
        transport
            .upload("compacted/latest", &artifact)
            .await
            .unwrap(),
        PublishOutcome::Published
    );
    let dl = dir.path().join("downloaded");
    let got = transport.download("compacted/latest", &dl).await.unwrap();
    assert_eq!(got.cursor, manifest.cursor);
    let (cursor, imported) = import(&dl, &dir.path().join("imported"))
        .expect("every payload hash verifies and the staged copy opens");
    assert_eq!(cursor, manifest.cursor);
    assert_eq!(
        full_state(&imported),
        expected,
        "imported state is exactly the at-export state — multiple data files, \
         compaction, and tombstones survived the round trip and source churn"
    );
}

// --- Backend instantiations ----------------------------------------------------

#[cfg(feature = "fjall")]
mod fjall_multi {
    use super::*;
    use slipstream::{FjallConfig, FjallSnapshot};

    fn cfg() -> FjallConfig {
        FjallConfig {
            sync: false,
            cache_size_bytes: 64 << 20,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_slow_exporter_cannot_clobber_newer_artifact() {
        slow_exporter_cannot_clobber_newer_artifact(
            |path| FjallSnapshot::open(path, cfg()).expect("open fjall"),
            |artifact, dest| FjallSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_crash_between_payload_and_pointer_keeps_bootstrap_available() {
        crash_between_payload_and_pointer_keeps_bootstrap_available(
            |path| FjallSnapshot::open(path, cfg()).expect("open fjall"),
            |artifact, dest| FjallSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_compacted_multi_file_artifact_survives_source_churn() {
        compacted_multi_file_artifact_survives_source_churn(
            |path| FjallSnapshot::open(path, cfg()).expect("open fjall"),
            |artifact, dest| FjallSnapshot::import(artifact, dest, cfg()),
            |fold| fold.settle(),
            // fjall/lsm-tree on-disk tables (sorted runs) live at
            // `keyspaces/<n>/tables/<id>`.
            |path| path.contains("/tables/"),
        )
        .await;
    }
}

#[cfg(feature = "rocksdb")]
mod rocksdb_multi {
    use super::*;
    use slipstream::{RocksDbConfig, RocksDbSnapshot};

    fn cfg() -> RocksDbConfig {
        RocksDbConfig {
            sync: false,
            cache_size_bytes: 64 << 20,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_slow_exporter_cannot_clobber_newer_artifact() {
        slow_exporter_cannot_clobber_newer_artifact(
            |path| RocksDbSnapshot::open(path, cfg()).expect("open rocksdb"),
            |artifact, dest| RocksDbSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_crash_between_payload_and_pointer_keeps_bootstrap_available() {
        crash_between_payload_and_pointer_keeps_bootstrap_available(
            |path| RocksDbSnapshot::open(path, cfg()).expect("open rocksdb"),
            |artifact, dest| RocksDbSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_compacted_multi_file_artifact_survives_source_churn() {
        compacted_multi_file_artifact_survives_source_churn(
            |path| RocksDbSnapshot::open(path, cfg()).expect("open rocksdb"),
            |artifact, dest| RocksDbSnapshot::import(artifact, dest, cfg()),
            |fold| fold.settle(),
            |path| path.ends_with(".sst"),
        )
        .await;
    }
}
