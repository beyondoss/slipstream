//! Transport-layer tests (feature `transport`), Tier 3: the
//! [`ObjectStoreTransport`] wire format and [`run_export_round`] against
//! `object_store`'s local filesystem backend — no cloud, no servers, except a
//! throwaway `nats-server` where a real lease/watch loop is part of the claim.
#![cfg(feature = "transport")]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

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

    transport.upload("edge/us-east/latest", &artifact).await.unwrap();

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

// --- run_export_round ------------------------------------------------------------
// These need a real lease (NATS KV) and a live watch_applied loop.

struct TestNats {
    child: Child,
    url: String,
    _store_dir: TempDir,
}

impl TestNats {
    async fn start() -> TestNats {
        let bin = std::env::var("NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".to_string());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let store_dir = tempfile::tempdir().unwrap();
        let child = Command::new(&bin)
            .args([
                "--jetstream",
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--store_dir",
                store_dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn `{bin}`: {e}. Run `mise install`."));
        let url = format!("nats://127.0.0.1:{port}");
        for _ in 0..100 {
            if async_nats::connect(&url).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        TestNats {
            child,
            url,
            _store_dir: store_dir,
        }
    }
}

impl Drop for TestNats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Everything a live export round needs: a fold being driven by watch_applied
/// over a real NATS bucket, an export channel into it, and a lease store.
struct LiveRound {
    _nats: TestNats,
    _conn: NatsConnection,
    writer: Arc<dyn KvWriter>,
    lease_store: Arc<dyn slipstream::KvStore>,
    exports: mpsc::Sender<slipstream::ExportRequest>,
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

    let task = tokio::spawn(watch_applied(
        watcher,
        WatchScope::All,
        None,
        Some(fold),
        Some(ex_rx),
        BatchConfig::default(),
        |u: &KvUpdate| match u {
            KvUpdate::Put(e) => Some(e.key.clone()),
            _ => None,
        },
        |_batch: Vec<String>| {},
        |_| {},
        sd_rx,
    ));

    LiveRound {
        _nats: nats,
        _conn: conn,
        writer,
        lease_store,
        exports: ex_tx,
        shutdown: sd_tx,
        task,
        dir,
    }
}

/// Wait until the fold has applied at least one update (watch attached).
async fn settle_watch(round: &LiveRound) {
    round.writer.put("seed.key", b"seed").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
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
