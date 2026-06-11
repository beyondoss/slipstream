//! Tier-2 bootstrap tests: export a fold from a LIVE `watch_applied` loop
//! under churn, import it as a second node, resume the watch from the
//! embedded cursor — and prove **delta-only resume**: the bootstrapped node
//! receives exactly the post-export tail, never a replay of the full history.
//!
//! Convergence alone would mask a full replay (the end state is identical
//! either way); the delivery COUNT is the assertion that carries the scaling
//! property — bootstrap cost = artifact + tail, not a rescan.
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
    BatchConfig, Connection, ExportRequest, KvStore, KvUpdate, NatsConnection,
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
            max_bytes: Some(8 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open bucket");
    (conn, store)
}

/// Spawn a `watch_applied` over `bucket` folding into `fold`, with an export
/// channel and a watch on the applied cursor.
struct Node {
    exports: mpsc::Sender<ExportRequest>,
    applied: Arc<AtomicU64>,
    delivered: Arc<AtomicU64>,
    min_rev: Arc<AtomicU64>,
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
    let delivered = Arc::new(AtomicU64::new(0));
    let min_rev = Arc::new(AtomicU64::new(u64::MAX));

    let applied_w = Arc::clone(&applied);
    let delivered_w = Arc::clone(&delivered);
    let min_rev_w = Arc::clone(&min_rev);

    let task = tokio::spawn(watch_applied(
        watcher,
        WatchScope::All,
        resume,
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        move |u: &KvUpdate| {
            // Count every DELIVERED update and track the lowest revision —
            // the delta-only assertions read these.
            delivered_w.fetch_add(1, Ordering::SeqCst);
            if let Some(rev) = u.version().as_u64() {
                min_rev_w.fetch_min(rev, Ordering::SeqCst);
            }
            Some(())
        },
        move |_batch: Vec<()>| {},
        move |cur: WatchCursor| {
            applied_w.store(cur.as_u64().unwrap_or(0), Ordering::SeqCst);
        },
        sd_rx,
    ));

    Node {
        exports: ex_tx,
        applied,
        delivered,
        min_rev,
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

/// The full bootstrap story for one backend:
///
/// 1. Node A folds the bucket live; N updates land.
/// 2. Export through A's request channel → artifact cursor == applied cursor.
/// 3. M more updates land (churn after the export).
/// 4. Node B imports the artifact and resumes from the embedded cursor.
/// 5. **Delta-only**: B was delivered exactly M updates, the first at
///    cursor+1 — no overlap, no gap, no full replay.
/// 6. B's fold state equals the bucket (the truth).
async fn live_export_bootstrap_delta_only<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: fn(&Path, &Path) -> Result<(WatchCursor, S), SnapshotError>,
) where
    S: SnapshotStore + Send + 'static,
{
    let nats = TestNats::start().await;
    let (_conn, bucket) = open_bucket(&nats).await;
    let writer = bucket.writer().expect("writer");
    let dir = TempDir::new().unwrap();

    // Node A, live.
    let (_r, fold_a) = open(&dir.path().join("node-a"));
    let node_a = spawn_node(&bucket, fold_a, None);

    // Deterministic attach: write until A applies something (KV watches
    // deliver new updates only; writes before the consumer attaches are
    // missed, so we probe rather than sleep).
    let attach_rev = timeout(Duration::from_secs(10), async {
        loop {
            let v = writer.put("route.seed", b"seed").await.expect("seed");
            tokio::time::sleep(Duration::from_millis(50)).await;
            if node_a.applied.load(Ordering::SeqCst) > 0 {
                return v.as_u64().expect("nats rev");
            }
        }
    })
    .await
    .expect("node A watch never attached");

    // Pre-export history: N real updates.
    let n = 12u64;
    let mut last_rev = attach_rev;
    for i in 0..n {
        last_rev = writer
            .put(&format!("route.pre.{i}"), format!("pre-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_a, last_rev).await;

    // Export from the live loop.
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
    assert!(export_rev >= last_rev, "artifact covers the pre-export history");

    // Post-export churn: exactly M updates (including a delete — tombstones
    // must ride the tail too).
    let m = 7u64;
    let mut final_rev = export_rev;
    for i in 0..m - 1 {
        final_rev = writer
            .put(&format!("route.post.{i}"), format!("post-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    assert!(writer.delete("route.pre.0").await.expect("delete"));
    final_rev += 1; // the delete's revision

    wait_applied(&node_a, final_rev).await;

    // Node B: import the artifact, resume from its cursor.
    let dest_b = dir.path().join("node-b");
    let (cursor_b, fold_b) = import(&artifact, &dest_b).expect("import");
    assert_eq!(
        cursor_b, manifest.cursor,
        "imported cursor is the manifest cursor"
    );
    let node_b = spawn_node(&bucket, fold_b, Some(cursor_b.clone()));
    wait_applied(&node_b, final_rev).await;

    // THE delta-only assertions.
    assert_eq!(
        node_b.delivered.load(Ordering::SeqCst),
        m,
        "bootstrapped node was delivered exactly the post-export tail, not a replay"
    );
    assert_eq!(
        node_b.min_rev.load(Ordering::SeqCst),
        export_rev + 1,
        "the tail starts at cursor+1 — no overlap, no gap"
    );

    // Shut both down; B's fold must equal the bucket.
    node_a.shutdown.send(true).unwrap();
    node_a.task.await.unwrap().unwrap();
    node_b.shutdown.send(true).unwrap();
    node_b.task.await.unwrap().unwrap();

    let (final_cursor, fold_b) = open(&dest_b);
    assert_eq!(final_cursor.as_u64(), Some(final_rev));
    let mut fold_state: Vec<(String, Vec<u8>)> = fold_b
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
        "bootstrapped fold equals the bucket"
    );
    assert!(
        !fold_state.iter().any(|(k, _)| k == "route.pre.0"),
        "the tail's delete reached the bootstrapped fold"
    );
}

#[cfg(feature = "fjall")]
mod fjall_bootstrap {
    use super::*;
    use slipstream::{FjallConfig, FjallSnapshot};

    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_live_export_bootstrap_delta_only() {
        live_export_bootstrap_delta_only(
            |path| {
                FjallSnapshot::open(
                    path,
                    FjallConfig {
                        sync: false,
                        cache_size_bytes: 64 << 20,
                    },
                )
                .expect("open fjall")
            },
            |artifact, dest| {
                FjallSnapshot::import(
                    artifact,
                    dest,
                    FjallConfig {
                        sync: false,
                        cache_size_bytes: 64 << 20,
                    },
                )
            },
        )
        .await;
    }
}

#[cfg(feature = "rocksdb")]
mod rocksdb_bootstrap {
    use super::*;
    use slipstream::{RocksDbConfig, RocksDbSnapshot};

    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_live_export_bootstrap_delta_only() {
        live_export_bootstrap_delta_only(
            |path| {
                RocksDbSnapshot::open(
                    path,
                    RocksDbConfig {
                        sync: false,
                        cache_size_bytes: 64 << 20,
                    },
                )
                .expect("open rocksdb")
            },
            |artifact, dest| {
                RocksDbSnapshot::import(
                    artifact,
                    dest,
                    RocksDbConfig {
                        sync: false,
                        cache_size_bytes: 64 << 20,
                    },
                )
            },
        )
        .await;
    }
}
