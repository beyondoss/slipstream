//! Tier-4 tests: the full bootstrap loop against a REAL S3 API — a throwaway
//! MinIO per test (mise-installed binary, no Docker). Node A exports through a
//! live `watch_applied` via `run_export_round` (lease + upload + completion);
//! node B downloads, imports, and resumes — with the delta-only assertion.
//! Plus the multipart upload path (artifact >> part size).
#![cfg(all(feature = "transport", any(feature = "fjall", feature = "rocksdb")))]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::{MINIO_BUCKET, TestMinio, TestNats};
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    ArtifactTransport, BatchConfig, Connection, ExportLease, ExportRequest, KvStore, KvUpdate,
    NatsConnection, NatsConnectionConfig, ObjectStoreTransport, StoreConfig, WatchCursor,
    WatchScope, run_export_round, watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

fn minio_transport(minio: &TestMinio) -> ObjectStoreTransport {
    ObjectStoreTransport::from_url_opts(
        &format!("s3://{MINIO_BUCKET}/slipstream"),
        minio.s3_options(),
    )
    .expect("build s3 transport against minio")
}

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
        .expect("routes bucket");
    let leases = conn
        .store_with_config(StoreConfig {
            name: "leases".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("leases bucket");
    (conn, routes, leases)
}

struct Node {
    exports: mpsc::Sender<ExportRequest>,
    applied: Arc<AtomicU64>,
    delivered: Arc<AtomicU64>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<WatchCursor, slipstream::KvError>>,
}

fn spawn_node<S: SnapshotStore + Send + 'static>(
    bucket: &Arc<dyn KvStore>,
    fold: S,
    resume: Option<WatchCursor>,
) -> Node {
    let watcher = bucket.watcher().expect("watcher");
    let (ex_tx, ex_rx) = mpsc::channel(1);
    let (sd_tx, sd_rx) = watch::channel(false);
    let applied = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let applied_w = Arc::clone(&applied);
    let delivered_w = Arc::clone(&delivered);

    let task = tokio::spawn(watch_applied(
        watcher,
        WatchScope::All,
        resume,
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        move |_u: &KvUpdate| {
            delivered_w.fetch_add(1, Ordering::SeqCst);
            Some(())
        },
        move |_batch: Vec<()>| {},
        move |cur: WatchCursor| applied_w.store(cur.as_u64().unwrap_or(0), Ordering::SeqCst),
        sd_rx,
    ));

    Node {
        exports: ex_tx,
        applied,
        delivered,
        shutdown: sd_tx,
        task,
    }
}

async fn wait_applied(node: &Node, at_least: u64) {
    timeout(Duration::from_secs(15), async {
        while node.applied.load(Ordering::SeqCst) < at_least {
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

/// Full loop, generic over the on-disk backend: live node A → run_export_round
/// (lease won, exported through the watch loop, uploaded to MinIO, completion
/// published, artifact deleted) → node B downloads + imports + resumes →
/// delta-only delivery → state equals the bucket.
async fn full_bootstrap_loop<S>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: fn(&Path, &Path) -> Result<(WatchCursor, S), SnapshotError>,
) where
    S: SnapshotStore + Send + 'static,
{
    let (nats, minio) = tokio::join!(TestNats::start(), TestMinio::start());
    let (_conn, routes, leases) = open_buckets(&nats).await;
    let writer = routes.writer().expect("writer");
    let transport = minio_transport(&minio);
    let dir = TempDir::new().unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();

    // Node A live; deterministic watch attach.
    let (_r, fold_a) = open(&dir.path().join("node-a"));
    let node_a = spawn_node(&routes, fold_a, None);
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

    // History, then the round.
    let mut last_rev = 0;
    for i in 0..10u64 {
        last_rev = writer
            .put(&format!("route.pre.{i}"), format!("pre-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_a, last_rev).await;

    let lease = ExportLease::new(leases.as_ref(), "round.routes", "node-a").expect("lease");
    let manifest = run_export_round(
        &lease,
        Duration::from_secs(120),
        &node_a.exports,
        &transport,
        "edge/us-east/latest",
        &scratch,
    )
    .await
    .expect("round")
    .expect("this node wins the only round");
    let export_rev = manifest.cursor.as_u64().expect("rev");
    assert!(export_rev >= last_rev);
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "artifact transience enforced"
    );

    // Post-export churn.
    let m = 5u64;
    let mut final_rev = export_rev;
    for i in 0..m {
        final_rev = writer
            .put(&format!("route.post.{i}"), format!("post-{i}").as_bytes())
            .await
            .expect("put")
            .as_u64()
            .expect("rev");
    }
    wait_applied(&node_a, final_rev).await;

    // Node B: manifest peek, download, import, resume.
    let peeked = transport.manifest("edge/us-east/latest").await.expect("peek");
    assert_eq!(peeked.cursor, manifest.cursor);

    let downloaded = dir.path().join("downloaded-artifact");
    transport
        .download("edge/us-east/latest", &downloaded)
        .await
        .expect("download");
    let dest_b = dir.path().join("node-b");
    let (cursor_b, fold_b) = import(&downloaded, &dest_b).expect("import");
    assert_eq!(cursor_b, manifest.cursor);

    let node_b = spawn_node(&routes, fold_b, Some(cursor_b));
    wait_applied(&node_b, final_rev).await;
    assert_eq!(
        node_b.delivered.load(Ordering::SeqCst),
        m,
        "bootstrap cost = artifact download + tail replay, never a rescan"
    );

    // Converged with the truth.
    node_a.shutdown.send(true).unwrap();
    node_a.task.await.unwrap().unwrap();
    node_b.shutdown.send(true).unwrap();
    node_b.task.await.unwrap().unwrap();

    let (_c, fold_b) = open(&dest_b);
    let mut fold_state: Vec<(String, Vec<u8>)> = fold_b
        .range("route.")
        .expect("range")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    fold_state.sort();
    let mut bucket_state: Vec<(String, Vec<u8>)> = routes
        .reader()
        .scan("route.")
        .await
        .expect("scan")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect();
    bucket_state.sort();
    assert_eq!(fold_state, bucket_state, "bootstrapped fold equals the bucket");
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread")]
async fn minio_full_bootstrap_loop_fjall() {
    use slipstream::{FjallConfig, FjallSnapshot};
    let cfg = FjallConfig {
        sync: false,
        cache_size_bytes: 64 << 20,
    };
    full_bootstrap_loop(
        move |path| FjallSnapshot::open(path, cfg).expect("open fjall"),
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

#[cfg(feature = "rocksdb")]
#[tokio::test(flavor = "multi_thread")]
async fn minio_full_bootstrap_loop_rocksdb() {
    use slipstream::{RocksDbConfig, RocksDbSnapshot};
    let cfg = RocksDbConfig {
        sync: false,
        cache_size_bytes: 64 << 20,
    };
    full_bootstrap_loop(
        move |path| RocksDbSnapshot::open(path, cfg).expect("open rocksdb"),
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

/// Multipart path: an artifact several times the 8 MiB part size streams up as
/// real multipart against the S3 API and round-trips intact. (Values are
/// random-ish so tar size ≈ data size; ~2,500 × 10 KiB ≈ 25 MiB ≈ 4 parts.)
#[tokio::test(flavor = "multi_thread")]
async fn minio_multipart_upload_round_trips() {
    use slipstream::AppendLogSnapshot;

    let minio = TestMinio::start().await;
    let transport = minio_transport(&minio);
    let dir = TempDir::new().unwrap();

    let (_r, mut fold) = AppendLogSnapshot::open(&dir.path().join("big.snap"), u64::MAX).unwrap();
    let mut batch = Vec::with_capacity(2500);
    for i in 0..2500u64 {
        // Pseudo-random bytes (LCG) so neither tar nor S3 sees compressible runs.
        let mut v = vec![0u8; 10 * 1024];
        let mut x = i.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        for b in &mut v {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            *b = (x >> 33) as u8;
        }
        batch.push(KvUpdate::Put(slipstream::KvEntry {
            key: format!("route.{i:06}"),
            value: v,
            version: slipstream::VersionToken::from_u64(i + 1),
        }));
    }
    fold.apply(&batch, &WatchCursor::from_u64(2500)).unwrap();

    let artifact = dir.path().join("artifact");
    let manifest = fold.export_to(&artifact).unwrap();
    let payload_bytes: u64 = manifest.files.iter().map(|f| f.size).sum();
    assert!(
        payload_bytes > 20 * 1024 * 1024,
        "artifact must exceed several part sizes (got {payload_bytes} bytes)"
    );

    transport.upload("big/latest", &artifact).await.expect("multipart upload");

    let downloaded = dir.path().join("downloaded");
    transport
        .download("big/latest", &downloaded)
        .await
        .expect("download");
    let (cursor, imported) =
        AppendLogSnapshot::import(&downloaded, &dir.path().join("imported.snap"), u64::MAX)
            .expect("import verifies every hash");
    assert_eq!(cursor.as_u64(), Some(2500));
    assert_eq!(
        imported.range("route.").unwrap().len(),
        2500,
        "all entries survive the multipart round trip"
    );
}
