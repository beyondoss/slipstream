//! End-to-end conformance tests for the cursor-expired resync — the leg of
//! the convergence proof (`tests/model.rs`) that rests on live-server
//! behavior nowhere else verified against a real NATS:
//!
//! **The expiry axiom.** The model's `Resume` transition assumes a resume
//! below the retention floor is DETECTED (`KvError::CursorExpired`). In the
//! code that detection is `nats.rs`'s error-string matching on consumer
//! creation — which presumes NATS *errors* on a too-old `ByStartSequence`
//! rather than silently clamping to the first retained sequence. If NATS
//! clamped, the gap (deletes included) would be skipped with no error, no
//! fallback, no resync: silent divergence through a path the model marks
//! Synced. These tests force real per-subject eviction on a throwaway
//! `nats-server` and prove the whole chain end-to-end: eviction → expiry
//! detected → full-watch fallback → stale-key resync → reconciled fold.
//!
//! The proof is self-validating by construction: the deleted key's marker is
//! purged from the stream, so NOTHING in the replay or the re-list can ever
//! deliver its delete — the key disappears from the bootstrapped fold if and
//! only if the expiry was detected and the resync ran. A silent clamp, a
//! missed error string, or a skipped resync each leave the key in the fold
//! and fail the test.
//!
//! The evil twin (`reader: None`) pins the divergence the model proves
//! reachable without the resync: same gap, same fallback, and the deleted
//! key persists — silently — in an otherwise fully converged fold.
//!
//! Generic over the on-disk backends; instantiated for fjall and RocksDB.
#![cfg(any(feature = "fjall", feature = "rocksdb"))]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::TestNats;
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    BatchConfig, Connection, ExportRequest, KvReader, KvStore, KvUpdate, NatsConnection,
    NatsConnectionConfig, StoreConfig, WatchCursor, WatchScope, watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::timeout;

async fn open_bucket(nats: &TestNats) -> (NatsConnection, Arc<dyn KvStore>) {
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.expect("connect");
    let store = conn
        .store_with_config(StoreConfig {
            name: "routes".into(),
            // History 1 (the default) is load-bearing: every overwrite of a
            // subject evicts its older message, which is how the test
            // advances the stream's first sequence past the export cursor.
            max_bytes: Some(8 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open bucket");
    (conn, store)
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
    reader: Option<Arc<dyn KvReader>>,
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
        reader,
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

type ImportFn<S> = fn(&Path, &Path) -> Result<(WatchCursor, S), SnapshotError>;

/// Everything both scenarios share: drive a live fold, export at cursor C
/// with `route.gone` present, then — offline — delete `route.gone`, purge its
/// subject so even the delete marker is gone, and overwrite every other
/// subject so the stream's first sequence moves past C. Returns the artifact,
/// its cursor, the final bucket revision, and the bucket handle.
struct ExpiredGap {
    _nats: TestNats,
    _conn: NatsConnection,
    bucket: Arc<dyn KvStore>,
    dir: TempDir,
    artifact: std::path::PathBuf,
    export_rev: u64,
    final_rev: u64,
}

async fn build_expired_gap<S>(open: impl Fn(&Path) -> (WatchCursor, S)) -> ExpiredGap
where
    S: SnapshotStore + Send + 'static,
{
    let nats = TestNats::start().await;
    let (conn, bucket) = open_bucket(&nats).await;
    let writer = bucket.writer().expect("writer");
    let dir = TempDir::new().unwrap();

    // Node A, live.
    let (_r, fold_a) = open(&dir.path().join("node-a"));
    let node_a = spawn_node(&bucket, fold_a, None, None);

    // Deterministic attach (KV watches deliver new updates only).
    timeout(Duration::from_secs(10), async {
        loop {
            writer.put("route.seed", b"seed").await.expect("seed");
            tokio::time::sleep(Duration::from_millis(50)).await;
            if node_a.applied.load(Ordering::SeqCst) > 0 {
                return;
            }
        }
    })
    .await
    .expect("node A watch never attached");

    // The fold state the artifact will carry: route.gone + five keepers.
    writer
        .put("route.gone", b"present-at-export")
        .await
        .expect("put gone");
    let mut export_rev = 0;
    for i in 0..5u64 {
        export_rev = writer
            .put(&format!("route.keep.{i}"), format!("v1-{i}").as_bytes())
            .await
            .expect("put keep")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_a, export_rev).await;

    // Export at cursor C through the live loop; A is done after this.
    let artifact = dir.path().join("artifact");
    let (reply_tx, reply_rx) = oneshot::channel();
    node_a
        .exports
        .send(ExportRequest {
            dest_dir: artifact.clone(),
            reply: reply_tx,
        })
        .await
        .expect("send export request");
    let manifest = reply_rx.await.expect("reply").expect("export succeeds");
    let export_rev = manifest.cursor.as_u64().expect("cursor rev");
    node_a.shutdown.send(true).unwrap();
    node_a.task.await.unwrap().unwrap();

    // --- The offline gap -------------------------------------------------
    // 1. Delete route.gone (leaves a delete marker)...
    assert!(writer.delete("route.gone").await.expect("delete gone"));
    // 2. ...then PURGE its subject so the marker itself is gone: after this,
    //    no replay and no re-list can ever deliver the delete. Only the
    //    resync can reconcile it.
    let raw = async_nats::connect(&nats.url).await.expect("raw connect");
    let js = async_nats::jetstream::new(raw);
    let mut stream = js.get_stream("KV_routes").await.expect("stream");
    stream
        .purge()
        .filter("$KV.routes.route.gone")
        .await
        .expect("purge route.gone subject");
    // 3. Overwrite every subject with messages at or below C (history=1
    //    evicts each subject's older revision), pushing first_seq past C.
    writer.put("route.seed", b"seed-v2").await.expect("put");
    let mut final_rev = 0;
    for i in 0..5u64 {
        final_rev = writer
            .put(&format!("route.keep.{i}"), format!("v2-{i}").as_bytes())
            .await
            .expect("put keep v2")
            .as_u64()
            .expect("rev");
    }

    // The premise the whole test rests on, asserted explicitly: the stream
    // has compacted past the export cursor, so a resume from C MUST hit the
    // expiry path (and if NATS silently clamped instead of erroring, the
    // missing route.gone delete below would catch it).
    let info = stream.info().await.expect("stream info");
    assert!(
        info.state.first_sequence > export_rev + 1,
        "stream first_seq {} must exceed export cursor {} + 1 — eviction premise",
        info.state.first_sequence,
        export_rev
    );

    ExpiredGap {
        _nats: nats,
        _conn: conn,
        bucket,
        dir,
        artifact,
        export_rev,
        final_rev,
    }
}

/// Import the gap's artifact as node B and run the watch to convergence with
/// the given reader wiring; return B's reopened fold for inspection.
async fn bootstrap_through_gap<S>(
    gap: &ExpiredGap,
    open: &impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
    reader: Option<Arc<dyn KvReader>>,
) -> S
where
    S: SnapshotStore + Send + 'static,
{
    let dest = gap.dir.path().join(if reader.is_some() {
        "node-b-resync"
    } else {
        "node-b-bare"
    });
    let (cursor_b, fold_b) = import(&gap.artifact, &dest).expect("import artifact");
    assert_eq!(cursor_b.as_u64(), Some(gap.export_rev));
    assert!(
        fold_b.get("route.gone").expect("get").is_some(),
        "the artifact carries route.gone — it was live at export time"
    );

    let node_b = spawn_node(&gap.bucket, fold_b, Some(cursor_b), reader);
    wait_applied(&node_b, gap.final_rev).await;
    node_b.shutdown.send(true).unwrap();
    node_b.task.await.unwrap().unwrap();

    open(&dest).1
}

/// The keepers must equal the bucket in BOTH scenarios — divergence, when it
/// happens, is confined to the unreconciled delete, which is exactly what
/// makes it silent.
fn assert_keepers_converged<S: SnapshotStore>(fold: &S) {
    for i in 0..5u64 {
        let e = fold
            .get(&format!("route.keep.{i}"))
            .expect("get")
            .unwrap_or_else(|| panic!("route.keep.{i} missing after bootstrap"));
        assert_eq!(e.value, format!("v2-{i}").as_bytes(), "route.keep.{i}");
    }
}

/// Reader wired (the shipped configuration): the expiry is detected against a
/// REAL nats-server, the fallback runs, and the resync deletes the key whose
/// delete marker no longer exists anywhere in the stream. This is the live
/// verification of the model's `Resume → Synced` transition for the expired
/// path — and of the axiom that NATS errors (rather than silently clamping)
/// on a too-old start sequence.
async fn resync_reconciles_offline_delete<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) where
    S: SnapshotStore + Send + 'static,
{
    let gap = build_expired_gap(&open).await;
    let reader = Some(gap.bucket.reader());
    let fold = bootstrap_through_gap(&gap, &open, import, reader).await;

    assert!(
        fold.get("route.gone").expect("get").is_none(),
        "route.gone must be reconciled away: its delete marker was purged, so \
         only the expiry-detected resync path can have removed it"
    );
    assert_keepers_converged(&fold);
}

/// Reader NOT wired: same gap, same fallback — and the deleted key persists
/// in an otherwise fully converged fold. The live twin of the model's
/// "HAZARD reachable: silent stale-key divergence without resync".
async fn without_reader_stale_key_persists<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) where
    S: SnapshotStore + Send + 'static,
{
    let gap = build_expired_gap(&open).await;
    let fold = bootstrap_through_gap(&gap, &open, import, None).await;

    assert!(
        fold.get("route.gone").expect("get").is_some(),
        "without the resync reader the deleted key persists — the divergence \
         the model proves reachable, pinned against a live server"
    );
    assert_keepers_converged(&fold);
}

/// Pins the live-server behavior that makes proactive expiry detection
/// necessary: an ordered consumer whose `ByStartSequence` falls below the
/// stream's first retained sequence gets NO error — NATS silently delivers
/// from the first available message. This is why `nats.rs` compares the
/// stream's `first_sequence` against the resume point
/// (`check_resume_window`) instead of relying on a consumer-create error,
/// and why the resume below first_seq here MUST surface as
/// `KvError::CursorExpired` from slipstream's own check.
///
/// If a future nats-server/async-nats starts erroring on the raw seek, the
/// raw-probe half of this test fails and the error-string fallback can be
/// re-evaluated; the slipstream-level half must keep returning
/// `CursorExpired` either way.
#[tokio::test(flavor = "multi_thread")]
async fn nats_silently_clamps_resume_below_first_seq() {
    let nats = TestNats::start().await;
    let (_conn, bucket) = open_bucket(&nats).await;
    let writer = bucket.writer().expect("writer");

    // Four rounds over two subjects: revs 1..=8, history=1 keeps only the
    // last round → first_seq advances to 7.
    for round in 0..4u64 {
        for k in ["route.a", "route.b"] {
            writer
                .put(k, format!("{round}").as_bytes())
                .await
                .expect("put");
        }
    }
    let raw = async_nats::connect(&nats.url).await.expect("raw connect");
    let js = async_nats::jetstream::new(raw);
    let mut stream = js.get_stream("KV_routes").await.expect("stream");
    let info = stream.info().await.expect("info");
    assert!(
        info.state.first_sequence > 3,
        "eviction premise: first_seq {} > 3",
        info.state.first_sequence
    );

    // RAW async-nats seek below first_seq: no error, delivery starts at the
    // first retained message — the silent clamp.
    let kv = js.get_key_value("routes").await.expect("kv handle");
    let mut watch = kv
        .watch_all_from_revision(3)
        .await
        .expect("NATS accepts a below-head start sequence without error — the clamp");
    use futures::StreamExt;
    let first = timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("clamped watch delivers")
        .expect("entry")
        .expect("entry ok");
    assert!(
        first.revision >= info.state.first_sequence,
        "delivery starts at the clamped head (rev {}), silently skipping the gap",
        first.revision
    );

    // Slipstream's watcher refuses the same resume: proactive expiry.
    let watcher = bucket.watcher().expect("watcher");
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let err = watcher
        .watch_all_from(&WatchCursor::from_u64(2), tx)
        .await
        .expect_err("resume below first_seq must be detected");
    assert!(
        matches!(err, slipstream::KvError::CursorExpired),
        "expected CursorExpired, got {err:?}"
    );
}

#[cfg(feature = "fjall")]
mod fjall_resync {
    use super::*;
    use slipstream::{FjallConfig, FjallSnapshot};

    fn cfg() -> FjallConfig {
        FjallConfig {
            sync: false,
            cache_size_bytes: 64 << 20,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_cursor_expired_resync_reconciles_offline_delete() {
        resync_reconciles_offline_delete(
            |path| FjallSnapshot::open(path, cfg()).expect("open fjall"),
            |artifact, dest| FjallSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_cursor_expired_without_reader_keeps_stale_key() {
        without_reader_stale_key_persists(
            |path| FjallSnapshot::open(path, cfg()).expect("open fjall"),
            |artifact, dest| FjallSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }
}

#[cfg(feature = "rocksdb")]
mod rocksdb_resync {
    use super::*;
    use slipstream::{RocksDbConfig, RocksDbSnapshot};

    fn cfg() -> RocksDbConfig {
        RocksDbConfig {
            sync: false,
            cache_size_bytes: 64 << 20,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_cursor_expired_resync_reconciles_offline_delete() {
        resync_reconciles_offline_delete(
            |path| RocksDbSnapshot::open(path, cfg()).expect("open rocksdb"),
            |artifact, dest| RocksDbSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_cursor_expired_without_reader_keeps_stale_key() {
        without_reader_stale_key_persists(
            |path| RocksDbSnapshot::open(path, cfg()).expect("open rocksdb"),
            |artifact, dest| RocksDbSnapshot::import(artifact, dest, cfg()),
        )
        .await;
    }
}
