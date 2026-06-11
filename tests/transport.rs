//! Transport-layer tests (feature `transport`), Tier 3: the
//! [`ObjectStoreTransport`] wire format and [`run_export_round`] against
//! `object_store`'s local filesystem backend — no cloud, no servers, except a
//! throwaway `nats-server` where a real lease/watch loop is part of the claim.
#![cfg(feature = "transport")]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::TestNats;

use async_trait::async_trait;
use slipstream::snapshot::{SnapshotError, SnapshotStore};
use slipstream::{
    AppendLogSnapshot, ArtifactTransport, BatchConfig, Connection, ExportLease, ExportManifest,
    KvEntry, KvUpdate, KvWriter, NatsConnection, NatsConnectionConfig, ObjectStoreTransport,
    StoreConfig, VersionToken, WatchCursor, WatchScope, run_export_round, watch_applied,
};
use tempfile::TempDir;
use tokio::sync::{mpsc, watch};

// --- Fixtures ----------------------------------------------------------------

fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
    KvUpdate::Put(KvEntry {
        key: key.to_string(),
        value: value.to_vec(),
        version: VersionToken::from_u64(rev),
    })
}

/// A local-filesystem transport rooted in a tempdir, plus the dir handle (the
/// "bucket" is inspectable for tamper tests).
fn local_transport() -> (ObjectStoreTransport, TempDir) {
    let bucket = TempDir::new().unwrap();
    let store = object_store::local::LocalFileSystem::new_with_prefix(bucket.path()).unwrap();
    (
        ObjectStoreTransport::new(Arc::new(store), "slipstream-artifacts"),
        bucket,
    )
}

/// Export a 3-entry append-log fold and return `(artifact_dir, manifest, dir)`.
fn exported_artifact() -> (std::path::PathBuf, ExportManifest, TempDir) {
    let dir = TempDir::new().unwrap();
    let (_r, mut s) = AppendLogSnapshot::open(&dir.path().join("fold.snap"), u64::MAX).unwrap();
    s.apply(
        &[put("a", b"1", 1), put("b", b"2", 2), put("c", b"3", 3)],
        &WatchCursor::from_u64(3),
    )
    .unwrap();
    let artifact = dir.path().join("artifact");
    let manifest = s.export_to(&artifact).unwrap();
    (artifact, manifest, dir)
}

// --- Wire-format tests ---------------------------------------------------------

/// upload → manifest-peek → download → import: the full remote round-trip, with
/// the imported fold byte-identical to the source.
#[tokio::test(flavor = "multi_thread")]
async fn upload_manifest_download_import_round_trip() {
    let (transport, _bucket) = local_transport();
    let (artifact, manifest, dir) = exported_artifact();

    transport
        .upload("edge/us-east/latest", &artifact)
        .await
        .unwrap();

    // Manifest peek without a payload download.
    let peeked = transport.manifest("edge/us-east/latest").await.unwrap();
    assert_eq!(peeked.cursor, manifest.cursor);
    assert_eq!(peeked.backend, "append-log");

    // Download to a fresh "node" and import.
    let downloaded = dir.path().join("downloaded");
    let got = transport
        .download("edge/us-east/latest", &downloaded)
        .await
        .unwrap();
    assert_eq!(got.cursor, manifest.cursor);

    let (cursor, imported) =
        AppendLogSnapshot::import(&downloaded, &dir.path().join("imported.snap"), u64::MAX)
            .unwrap();
    assert_eq!(cursor.as_u64(), Some(3));
    assert_eq!(imported.get("b").unwrap().unwrap().value, b"2");
}

/// `import_remote` composes download + import + cleanup in one call.
#[tokio::test(flavor = "multi_thread")]
async fn import_remote_bootstraps_a_fold() {
    let (transport, _bucket) = local_transport();
    let (artifact, _m, dir) = exported_artifact();
    transport.upload("latest", &artifact).await.unwrap();

    let scratch = dir.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let (cursor, imported) = AppendLogSnapshot::import_remote(
        &transport,
        "latest",
        &scratch,
        &dir.path().join("bootstrapped.snap"),
        u64::MAX,
    )
    .await
    .unwrap();
    assert_eq!(cursor.as_u64(), Some(3));
    assert_eq!(imported.get("a").unwrap().unwrap().value, b"1");
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "downloaded artifact cleaned from scratch"
    );
}

/// A sibling manifest that disagrees with the tar's embedded manifest is
/// rejected at download — the transport is untrusted.
#[tokio::test(flavor = "multi_thread")]
async fn download_rejects_manifest_disagreement() {
    let (transport, bucket) = local_transport();
    let (artifact, _m, dir) = exported_artifact();
    transport.upload("latest", &artifact).await.unwrap();

    // Doctor the sibling manifest object in the "bucket" (cursor 999).
    let sibling = bucket
        .path()
        .join("slipstream-artifacts")
        .join("latest.manifest.json");
    let raw = std::fs::read(&sibling).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    json["cursor_hex"] = "00000000000003e7".into();
    std::fs::write(&sibling, serde_json::to_vec(&json).unwrap()).unwrap();

    let dest = dir.path().join("downloaded");
    match transport.download("latest", &dest).await {
        Err(SnapshotError::ArtifactInvalid(msg)) => {
            assert!(msg.contains("disagrees"), "{msg}");
        }
        other => panic!("expected ArtifactInvalid, got {other:?}"),
    }
    assert!(!dest.exists(), "nothing lands at the destination");
}

/// A missing remote object is an ArtifactInvalid, not an opaque backend error.
#[tokio::test(flavor = "multi_thread")]
async fn missing_remote_artifact_is_artifact_invalid() {
    let (transport, _bucket) = local_transport();
    match transport.manifest("never-uploaded").await {
        Err(SnapshotError::ArtifactInvalid(msg)) => assert!(msg.contains("not found"), "{msg}"),
        other => panic!("expected ArtifactInvalid, got {other:?}"),
    }
}

/// A sibling-manifest object larger than the 1 MiB cap is rejected before
/// being buffered whole — the OOM guard against a hostile or corrupted object
/// at the manifest key.
#[tokio::test(flavor = "multi_thread")]
async fn oversized_remote_manifest_is_rejected() {
    let (transport, bucket) = local_transport();
    // Plant the oversized object directly in the "bucket" at the sibling key.
    let sibling_dir = bucket.path().join("slipstream-artifacts");
    std::fs::create_dir_all(&sibling_dir).unwrap();
    std::fs::write(
        sibling_dir.join("oversized.manifest.json"),
        vec![b'x'; (1 << 20) + 1],
    )
    .unwrap();

    match transport.manifest("oversized").await {
        Err(SnapshotError::ArtifactInvalid(msg)) => assert!(msg.contains("exceeds"), "{msg}"),
        other => panic!("expected ArtifactInvalid, got {other:?}"),
    }
}

/// Both URL-parse failure modes surface as transport errors, not panics: a
/// string that is not a URL at all, and a URL whose scheme `object_store`
/// doesn't recognize.
#[test]
fn from_url_opts_rejects_bad_urls() {
    // Garbage that fails URL parsing, and a well-formed URL whose scheme
    // object_store doesn't recognize.
    for bad in ["not a url", "bogus://bucket/prefix"] {
        match ObjectStoreTransport::from_url_opts(bad, std::iter::empty::<(&str, &str)>()) {
            Err(SnapshotError::Backend(_)) => {}
            Err(other) => panic!("url {bad:?}: expected Backend, got {other:?}"),
            Ok(_) => panic!("url {bad:?} must not build a transport"),
        }
    }
}

/// A destination with no parent directory (a bare relative path) is refused
/// before any remote I/O — there is nowhere to stage the download beside it.
#[tokio::test(flavor = "multi_thread")]
async fn download_rejects_dest_without_parent() {
    let (transport, _bucket) = local_transport();
    match transport
        .download("anything", Path::new("slipstream-bare-dest"))
        .await
    {
        Err(SnapshotError::ArtifactInvalid(msg)) => {
            assert!(msg.contains("no parent"), "{msg}");
        }
        other => panic!("expected ArtifactInvalid, got {other:?}"),
    }
}

// --- run_export_round ------------------------------------------------------------
// These need a real lease (NATS KV) and a live watch_applied loop.

/// Everything a live export round needs: a fold being driven by watch_applied
/// over a real NATS bucket, an export channel into it, and a lease store.
struct LiveRound {
    _nats: TestNats,
    _conn: NatsConnection,
    writer: Arc<dyn KvWriter>,
    lease_store: Arc<dyn slipstream::KvStore>,
    exports: mpsc::Sender<slipstream::ExportRequest>,
    applied: Arc<std::sync::atomic::AtomicU64>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<WatchCursor, slipstream::KvError>>,
    dir: TempDir,
}

async fn live_round() -> LiveRound {
    let nats = TestNats::start().await;
    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    conn.connect().await.unwrap();
    let bucket = conn
        .store_with_config(StoreConfig {
            name: "routes".into(),
            max_bytes: Some(8 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .unwrap();
    let lease_store = conn
        .store_with_config(StoreConfig {
            name: "leases".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .unwrap();

    let writer = bucket.writer().unwrap();
    let watcher = bucket.watcher().unwrap();

    let dir = TempDir::new().unwrap();
    let (_r, fold) = AppendLogSnapshot::open(&dir.path().join("fold.snap"), u64::MAX).unwrap();
    let (ex_tx, ex_rx) = mpsc::channel(1);
    let (sd_tx, sd_rx) = watch::channel(false);
    let applied = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let applied_w = Arc::clone(&applied);

    let task = tokio::spawn(watch_applied(
        watcher,
        WatchScope::All,
        None,
        None, // reader: cursor-expired resync not exercised here
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        |u: &KvUpdate| match u {
            KvUpdate::Put(e) => Some(e.key.clone()),
            _ => None,
        },
        |_batch: Vec<String>| {},
        move |cur: WatchCursor| {
            applied_w.store(
                cur.as_u64().unwrap_or(0),
                std::sync::atomic::Ordering::SeqCst,
            );
        },
        sd_rx,
    ));

    LiveRound {
        _nats: nats,
        _conn: conn,
        writer,
        lease_store,
        exports: ex_tx,
        applied,
        shutdown: sd_tx,
        task,
        dir,
    }
}

/// Deterministically wait until the fold has applied an update: KV watches
/// deliver new updates only, so writes that land before the consumer attaches
/// are missed — probe with repeated puts until the applied cursor moves.
async fn settle_watch(round: &LiveRound) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            round.writer.put("seed.key", b"seed").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            if round.applied.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                return;
            }
        }
    })
    .await
    .expect("watch never attached");
}

/// The composed happy path: lease won → export through the live loop → upload
/// → completion published → local artifact gone. And a concurrent second
/// caller loses the round cleanly.
#[tokio::test(flavor = "multi_thread")]
async fn run_export_round_uploads_once_and_cleans_up() {
    let round = live_round().await;
    settle_watch(&round).await;

    let (transport, _bucket) = local_transport();
    let scratch = round.dir.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();

    let lease_a = ExportLease::new(round.lease_store.as_ref(), "round", "node-a").unwrap();
    let lease_b = ExportLease::new(round.lease_store.as_ref(), "round", "node-b").unwrap();

    let (a, b) = tokio::join!(
        run_export_round(
            &lease_a,
            Duration::from_secs(60),
            &round.exports,
            &transport,
            "latest",
            &scratch,
        ),
        run_export_round(
            &lease_b,
            Duration::from_secs(60),
            &round.exports,
            &transport,
            "latest",
            &scratch,
        ),
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    let winners: Vec<_> = outcomes.iter().filter(|o| o.is_some()).collect();
    assert_eq!(winners.len(), 1, "exactly one round runs");
    let manifest = winners[0].as_ref().unwrap();
    assert!(!manifest.cursor.is_none(), "exported a live cursor");

    // Uploaded and fetchable; completion published on the lease key.
    let peeked = transport.manifest("latest").await.unwrap();
    assert_eq!(peeked.cursor, manifest.cursor);
    let record = lease_a.current().await.unwrap().unwrap();
    assert!(
        record.completed_cursor_hex.is_some(),
        "completion is fleet-visible"
    );

    // Transience: nothing left in scratch.
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "local artifact deleted after upload"
    );

    round.shutdown.send(true).unwrap();
    round.task.await.unwrap().unwrap();
}

/// A transport that always fails its upload.
struct FailingTransport;

#[async_trait]
impl ArtifactTransport for FailingTransport {
    async fn upload(&self, _key: &str, _dir: &Path) -> Result<(), SnapshotError> {
        Err(SnapshotError::Backend("injected upload failure".into()))
    }
    async fn manifest(&self, _key: &str) -> Result<ExportManifest, SnapshotError> {
        Err(SnapshotError::Backend("unused".into()))
    }
    async fn download(&self, _key: &str, _dest: &Path) -> Result<ExportManifest, SnapshotError> {
        Err(SnapshotError::Backend("unused".into()))
    }
}

/// Upload failure: the error propagates, no completion is published, the local
/// artifact is cleaned up, and — because the lease is abandoned — the next
/// round can be won immediately instead of waiting out the ttl.
#[tokio::test(flavor = "multi_thread")]
async fn run_export_round_upload_failure_abandons_lease() {
    let round = live_round().await;
    settle_watch(&round).await;

    let scratch = round.dir.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let lease = ExportLease::new(round.lease_store.as_ref(), "round", "node-a").unwrap();

    let err = run_export_round(
        &lease,
        Duration::from_secs(600), // long ttl: only abandon makes retry possible
        &round.exports,
        &FailingTransport,
        "latest",
        &scratch,
    )
    .await
    .expect_err("upload failure propagates");
    assert!(matches!(err, SnapshotError::Backend(_)));

    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "artifact cleaned up after the failed round"
    );

    // The lease was abandoned: a fresh round wins immediately with a working
    // transport, long before the 600s ttl.
    let (transport, _bucket) = local_transport();
    let retry = run_export_round(
        &lease,
        Duration::from_secs(60),
        &round.exports,
        &transport,
        "latest",
        &scratch,
    )
    .await
    .unwrap();
    assert!(retry.is_some(), "abandoned lease frees the next round");

    round.shutdown.send(true).unwrap();
    round.task.await.unwrap().unwrap();
}

/// The watch loop is gone before the round begins (panicked / shut down): the
/// round fails with an error naming the loop, nothing is left in scratch, and
/// the lease is abandoned so a replacement node can win the round immediately
/// instead of waiting out the ttl.
#[tokio::test(flavor = "multi_thread")]
async fn run_export_round_dead_loop_abandons_lease() {
    let round = live_round().await;
    settle_watch(&round).await;

    // Kill the loop definitively; its export receiver drops with it.
    round.shutdown.send(true).unwrap();
    round.task.await.unwrap().unwrap();

    let (transport, _bucket) = local_transport();
    let scratch = round.dir.path().join("scratch");
    std::fs::create_dir(&scratch).unwrap();
    let lease = ExportLease::new(round.lease_store.as_ref(), "round", "node-a").unwrap();

    let err = run_export_round(
        &lease,
        Duration::from_secs(600), // long ttl: only abandon makes re-acquire possible
        &round.exports,
        &transport,
        "latest",
        &scratch,
    )
    .await
    .expect_err("a dead watch loop must fail the round");
    match err {
        SnapshotError::Backend(msg) => assert!(msg.contains("watch loop"), "{msg}"),
        other => panic!("expected Backend, got {other:?}"),
    }
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "scratch cleaned up after the failed round"
    );

    // The lease was abandoned: a replacement node wins immediately, long
    // before the 600 s ttl.
    let replacement = ExportLease::new(round.lease_store.as_ref(), "round", "node-b").unwrap();
    assert!(
        replacement
            .try_acquire(Duration::from_secs(60))
            .await
            .unwrap()
            .is_some(),
        "abandoned lease frees the round for a replacement node"
    );
}

// --- import_remote error paths for the LSM backends (local transport) --------
// The MinIO tier only exercises these backends with a GOOD artifact; these
// prove the verify gate fires for a tampered one without needing MinIO.

/// Export a fold, flip a byte in its largest payload file, upload it, and
/// assert the remote import rejects it with nothing at the destination and a
/// clean scratch dir. Generic over the backend's export + import_remote.
#[cfg(any(feature = "fjall", feature = "rocksdb"))]
async fn assert_import_remote_rejects_tampered(
    artifact: &std::path::Path,
    manifest: &ExportManifest,
    transport: &ObjectStoreTransport,
) {
    // Tamper with the largest payload file (most likely to hold real data).
    let victim = manifest
        .files
        .iter()
        .max_by_key(|f| f.size)
        .expect("at least one payload file");
    let victim_path = artifact.join(&victim.path);
    let mut bytes = std::fs::read(&victim_path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&victim_path, &bytes).unwrap();

    // upload() validates only the manifest, so the tampered payload ships.
    transport.upload("tampered", artifact).await.unwrap();
}

#[cfg(feature = "fjall")]
mod fjall_remote {
    use super::*;
    use slipstream::snapshot::SnapshotStore;
    use slipstream::{FjallConfig, FjallSnapshot};

    /// `import_remote` re-verifies every payload hash: a tampered artifact
    /// shipped through the (untrusted) transport is rejected, nothing lands at
    /// the destination, and the downloaded copy is cleaned from scratch.
    #[tokio::test(flavor = "multi_thread")]
    async fn fjall_import_remote_rejects_tampered_artifact() {
        let (transport, _bucket) = local_transport();
        let dir = TempDir::new().unwrap();
        let cfg = FjallConfig {
            sync: false,
            ..Default::default()
        };

        let (_r, mut fold) = FjallSnapshot::open(&dir.path().join("fold"), cfg).unwrap();
        fold.apply(
            &[put("a", b"1", 1), put("b", b"2", 2), put("c", b"3", 3)],
            &WatchCursor::from_u64(3),
        )
        .unwrap();
        let artifact = dir.path().join("artifact");
        let manifest = fold.export_to(&artifact).unwrap();
        assert_import_remote_rejects_tampered(&artifact, &manifest, &transport).await;

        let scratch = dir.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let dest = dir.path().join("imported");
        match FjallSnapshot::import_remote(&transport, "tampered", &scratch, &dest, cfg).await {
            Err(SnapshotError::ArtifactInvalid(_)) => {}
            Err(other) => panic!("expected ArtifactInvalid, got {other:?}"),
            Ok(_) => panic!("tampered artifact must not import"),
        }
        assert!(!dest.exists(), "nothing lands at the destination");
        assert_eq!(
            std::fs::read_dir(&scratch).unwrap().count(),
            0,
            "downloaded artifact cleaned from scratch"
        );
    }
}

#[cfg(feature = "rocksdb")]
mod rocksdb_remote {
    use super::*;
    use slipstream::snapshot::SnapshotStore;
    use slipstream::{RocksDbConfig, RocksDbSnapshot};

    /// RocksDB twin of the fjall test above.
    #[tokio::test(flavor = "multi_thread")]
    async fn rocksdb_import_remote_rejects_tampered_artifact() {
        let (transport, _bucket) = local_transport();
        let dir = TempDir::new().unwrap();
        let cfg = RocksDbConfig {
            sync: false,
            ..Default::default()
        };

        let (_r, mut fold) = RocksDbSnapshot::open(&dir.path().join("fold"), cfg).unwrap();
        fold.apply(
            &[put("a", b"1", 1), put("b", b"2", 2), put("c", b"3", 3)],
            &WatchCursor::from_u64(3),
        )
        .unwrap();
        let artifact = dir.path().join("artifact");
        let manifest = fold.export_to(&artifact).unwrap();
        assert_import_remote_rejects_tampered(&artifact, &manifest, &transport).await;

        let scratch = dir.path().join("scratch");
        std::fs::create_dir(&scratch).unwrap();
        let dest = dir.path().join("imported");
        match RocksDbSnapshot::import_remote(&transport, "tampered", &scratch, &dest, cfg).await {
            Err(SnapshotError::ArtifactInvalid(_)) => {}
            Err(other) => panic!("expected ArtifactInvalid, got {other:?}"),
            Ok(_) => panic!("tampered artifact must not import"),
        }
        assert!(!dest.exists(), "nothing lands at the destination");
        assert_eq!(
            std::fs::read_dir(&scratch).unwrap().count(),
            0,
            "downloaded artifact cleaned from scratch"
        );
    }
}
