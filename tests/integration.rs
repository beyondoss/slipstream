//! Integration tests for the NATS JetStream backend.
//!
//! Each test boots its own `nats-server` (JetStream enabled) on a free port with
//! a throwaway store directory, then talks to it through the public `slipstream`
//! API. The server is killed when the [`TestNats`] guard drops, so tests are
//! fully isolated and leave nothing running.
//!
//! `nats-server` comes from mise (`ubi:nats-io/nats-server`). When mise is
//! activated it's on `PATH`; otherwise set `NATS_SERVER_BIN` to an explicit path.

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use slipstream::{
    Connection, KvError, KvStore, KvUpdate, KvWriter, NatsConnection, NatsConnectionConfig,
    StoreConfig, VersionToken, WatchCursor,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

// --- Test harness ------------------------------------------------------------

/// A running `nats-server` with JetStream enabled. Killed on drop.
struct TestNats {
    child: Child,
    url: String,
    // Kept alive so the JetStream store directory survives for the server's
    // lifetime; removed when the guard drops.
    _store_dir: tempfile::TempDir,
}

impl TestNats {
    /// Boot a fresh server and block until it accepts connections.
    async fn start() -> TestNats {
        let bin = std::env::var("NATS_SERVER_BIN").unwrap_or_else(|_| "nats-server".to_string());
        let port = free_port();
        let store_dir = tempfile::tempdir().expect("create jetstream store dir");

        let child = Command::new(&bin)
            .args([
                "--jetstream",
                "--addr",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--store_dir",
                store_dir.path().to_str().expect("utf-8 store path"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "failed to spawn `{bin}`: {e}. Is nats-server installed? \
                     Run `mise install` or set NATS_SERVER_BIN."
                )
            });

        let url = format!("nats://127.0.0.1:{port}");
        wait_until_ready(&url).await;

        TestNats {
            child,
            url,
            _store_dir: store_dir,
        }
    }

    /// Connect through the public API and return a ready connection.
    async fn connect(&self) -> NatsConnection {
        let conn = NatsConnection::new(NatsConnectionConfig {
            url: self.url.clone(),
            creds: None,
            creds_file: None,
        });
        conn.connect().await.expect("connect to test nats");
        conn
    }

    /// Connect and open a store named `bucket`. The connection is returned too
    /// because it owns the underlying NATS client; keep it in scope.
    async fn store(&self, bucket: &str) -> (NatsConnection, Arc<dyn KvStore>) {
        let conn = self.connect().await;
        let store = conn
            .store_with_config(StoreConfig {
                name: bucket.to_string(),
                max_bytes: Some(8 * 1024 * 1024),
                ..Default::default()
            })
            .await
            .expect("open store");
        (conn, store)
    }
}

impl Drop for TestNats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Grab a free TCP port by binding to :0 and reading the assigned port back.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read local addr")
        .port()
}

/// Poll the server until a client connects or we give up.
async fn wait_until_ready(url: &str) {
    for _ in 0..100 {
        if async_nats::connect(url).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("nats-server at {url} never became ready");
}

/// Deterministically wait until a freshly spawned watch is live.
///
/// NATS KV watches deliver updates only (no initial-state replay), and the
/// ephemeral consumer takes a moment to attach — so any write issued before the
/// subscription is established is silently missed. We close that race by writing
/// `sentinel` (which must fall within the watch's filter) on a retry loop until
/// the watch echoes it back, then drain any duplicate echoes.
async fn establish_watch(writer: &dyn KvWriter, rx: &mut mpsc::Receiver<KvUpdate>, sentinel: &str) {
    loop {
        writer.put(sentinel, b"ready").await.expect("put sentinel");
        match timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(u)) if u.key() == sentinel => break,
            Ok(Some(_)) => {} // no real writes yet; ignore anything unexpected
            Ok(None) => panic!("watch channel closed during handshake"),
            Err(_) => {} // not attached yet — write the sentinel again
        }
    }
    // Drain buffered sentinel echoes so they don't leak into the real assertions.
    while rx.try_recv().is_ok() {}
}

/// Receive exactly `n` updates from a watch channel, failing on timeout so a
/// missing update can't hang the suite.
async fn collect_updates(rx: &mut mpsc::Receiver<KvUpdate>, n: usize) -> Vec<KvUpdate> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let update = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for watch update")
            .expect("watch channel closed early");
        out.push(update);
    }
    out
}

// --- Lifecycle & configuration ----------------------------------------------

#[tokio::test]
async fn health_follows_lifecycle() {
    let nats = TestNats::start().await;

    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });
    assert!(!conn.is_healthy(), "fresh connection is not healthy");

    conn.connect().await.expect("connect");
    assert!(conn.is_healthy(), "healthy after connect");

    conn.shutdown().await.expect("shutdown");
    assert!(!conn.is_healthy(), "not healthy after shutdown");
}

#[tokio::test]
async fn store_before_connect_is_not_connected() {
    let nats = TestNats::start().await;

    let conn = NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    });

    match conn.store("anything").await {
        Err(KvError::NotConnected) => {}
        Ok(_) => panic!("store before connect should fail"),
        Err(other) => panic!("expected NotConnected, got {other:?}"),
    }
}

#[tokio::test]
async fn capabilities_report_nats_features() {
    let nats = TestNats::start().await;
    let conn = nats.connect().await;

    let caps = conn.capabilities();
    assert!(caps.streaming_watch);
    assert!(caps.prefix_watch);
    assert!(caps.cas);
    // KvTtl is not implemented for the NATS backend yet, so the capability must
    // report false — advertising true would send callers down a dead path.
    assert!(!caps.ttl, "TTL capability must be false until KvTtl ships");
    assert!(!caps.transactions);
    assert!(!caps.global_ordering);
}

#[tokio::test]
async fn from_client_reuses_existing_connection() {
    let nats = TestNats::start().await;

    let client = async_nats::connect(&nats.url).await.expect("raw connect");
    let conn = NatsConnection::from_client(client);
    assert!(conn.is_healthy(), "pre-connected client is healthy");

    // No explicit connect() call — the store should open directly.
    let store = conn
        .store_with_config(StoreConfig {
            name: "reused".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open store on reused client");
    let writer = store.writer().expect("writer");
    writer.put("k", b"v").await.expect("put");
}

#[tokio::test]
async fn from_client_connection_refuses_to_reconnect() {
    // A `from_client` connection borrows a caller-owned client and retains no URL
    // or credentials, so it genuinely cannot redial. After shutdown(), connect()
    // must fail fast with an actionable error rather than dialing the empty config
    // URL (an opaque connect failure) or — worse — silently flipping healthy=true
    // while leaving the stale `state_probe` to pin is_healthy() false forever.
    let nats = TestNats::start().await;

    let client = async_nats::connect(&nats.url).await.expect("raw connect");
    let conn = NatsConnection::from_client(client);
    assert!(conn.is_healthy(), "pre-connected client is healthy");

    // While still live, connect() is a harmless no-op (fast-path on healthy).
    conn.connect()
        .await
        .expect("connect() on a live borrowed client is a no-op");

    conn.shutdown().await.expect("shutdown");
    assert!(!conn.is_healthy(), "not healthy after shutdown");

    // Now connect() must refuse with a clear, non-reconnectable error.
    let err = conn
        .connect()
        .await
        .expect_err("from_client connection must not reconnect");
    match err {
        KvError::ConnectionFailed(msg) => {
            assert!(
                msg.contains("from_client"),
                "error must name the cause: {msg}"
            );
        }
        other => panic!("expected ConnectionFailed, got {other:?}"),
    }
    assert!(
        !conn.is_healthy(),
        "still unhealthy after a refused reconnect"
    );
}

// --- Read / write / CAS ------------------------------------------------------

#[tokio::test]
async fn put_then_get_roundtrips() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    let version = writer.put("node.a", b"hello").await.expect("put");
    assert!(version.as_u64().is_some(), "NATS version is a u64 revision");

    let entry = reader.get("node.a").await.expect("get").expect("present");
    assert_eq!(entry.key, "node.a");
    assert_eq!(entry.value, b"hello");
    assert_eq!(entry.version.as_u64(), version.as_u64());
}

#[tokio::test]
async fn get_missing_key_returns_none() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;

    let got = store.reader().get("absent").await.expect("get");
    assert!(got.is_none());
}

#[tokio::test]
async fn create_conflicts_on_live_key() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");

    writer.create("lock.x", b"1").await.expect("first create");
    let err = writer
        .create("lock.x", b"2")
        .await
        .expect_err("second create must conflict");
    assert!(matches!(err, KvError::AlreadyExists), "got {err:?}");
}

#[tokio::test]
async fn update_cas_succeeds_then_detects_conflict() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");

    let v1 = writer.put("node.a", b"one").await.expect("put");
    let v2 = writer
        .update("node.a", b"two", &v1)
        .await
        .expect("cas update with current version");
    assert_ne!(v1.as_u64(), v2.as_u64());

    // Re-using the stale version must fail.
    let err = writer
        .update("node.a", b"three", &v1)
        .await
        .expect_err("stale CAS must fail");
    assert!(matches!(err, KvError::RevisionMismatch), "got {err:?}");
}

#[tokio::test]
async fn delete_removes_key() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"v").await.expect("put");
    assert!(writer.delete("node.a").await.expect("delete"));
    assert!(reader.get("node.a").await.expect("get").is_none());
}

#[tokio::test]
async fn delete_with_version_is_cas_gated() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    let v1 = writer.put("node.a", b"v").await.expect("put");

    // Stale version (a revision that doesn't match the live key) is rejected.
    let stale = writer
        .delete_with_version("node.a", &VersionToken::from_u64(999_999))
        .await;
    assert!(
        matches!(stale, Err(KvError::RevisionMismatch)),
        "got {stale:?}"
    );

    // Current version succeeds and logically deletes (get() filters tombstone).
    assert!(
        writer
            .delete_with_version("node.a", &v1)
            .await
            .expect("cas delete")
    );
    assert!(reader.get("node.a").await.expect("get").is_none());

    // entry() still exposes the empty-value tombstone for conflict detection.
    let tombstone = reader.entry("node.a").await.expect("entry");
    let tombstone = tombstone.expect("tombstone present");
    assert!(tombstone.value.is_empty(), "tombstone has empty value");
}

#[tokio::test]
async fn update_with_invalid_version_errors() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");

    writer.put("node.a", b"v").await.expect("put");
    // An unknown/empty version token has no u64 revision for NATS.
    let err = writer
        .update("node.a", b"v2", &VersionToken::unknown())
        .await
        .expect_err("invalid version must error");
    assert!(matches!(err, KvError::OperationFailed(_)), "got {err:?}");
}

#[tokio::test]
async fn scan_returns_last_value_per_key_and_skips_deletes() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"1").await.expect("put a");
    writer.put("node.a", b"2").await.expect("update a");
    writer.put("node.b", b"3").await.expect("put b");
    writer.put("other.c", b"4").await.expect("put c");
    writer.delete("node.b").await.expect("delete b");

    let mut entries = reader.scan("node.").await.expect("scan");
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    assert_eq!(entries.len(), 1, "deleted b excluded, c out of prefix");
    assert_eq!(entries[0].key, "node.a");
    assert_eq!(entries[0].value, b"2", "scan returns latest value");
}

#[tokio::test]
async fn scan_version_matches_live_revision() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    // Build a few unrelated revisions so the stream sequence is well past 1 and
    // any "wrong field" parse (e.g. num_pending) would produce a different value.
    writer.put("node.a", b"1").await.expect("put a");
    writer.put("node.a", b"2").await.expect("update a");
    let live = writer.put("node.b", b"3").await.expect("put b");

    // Cross-check against the per-key read, which is known to carry the real
    // revision (it comes from entry.revision, not the ack subject).
    let via_get = reader
        .get("node.b")
        .await
        .expect("get")
        .expect("present")
        .version;
    assert_eq!(
        via_get.as_u64(),
        live.as_u64(),
        "sanity: get == put revision"
    );

    // The version a scan reports MUST equal the key's actual revision, because
    // CAS callers (update / delete_with_version) feed it straight back to NATS.
    let entries = reader.scan("node.").await.expect("scan");
    let b = entries
        .iter()
        .find(|e| e.key == "node.b")
        .expect("node.b in scan");
    assert_eq!(
        b.version.as_u64(),
        live.as_u64(),
        "scan version must equal the live revision returned by put()"
    );

    // And it must round-trip through a real CAS update.
    writer
        .update("node.b", b"4", &b.version)
        .await
        .expect("CAS update using the scan-reported version must succeed");
}

#[tokio::test]
async fn keys_returns_names_under_prefix() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"1").await.expect("put a");
    writer.put("node.b", b"2").await.expect("put b");
    writer.put("other.c", b"3").await.expect("put c");
    writer.delete("node.b").await.expect("delete b");

    let mut keys = reader.keys("node.").await.expect("keys");
    keys.sort();

    assert_eq!(
        keys,
        vec!["node.a".to_string()],
        "deleted/out-of-prefix excluded"
    );
}

#[tokio::test]
async fn keys_excludes_cas_tombstones_like_get_and_scan() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"1").await.expect("put a");
    let v = writer.put("node.b", b"2").await.expect("put b");
    // CAS-delete writes an empty-value Put tombstone (not a KV Delete marker).
    writer
        .delete_with_version("node.b", &v)
        .await
        .expect("cas delete b");

    // get() and scan() both treat the tombstone as absent…
    assert!(reader.get("node.b").await.expect("get").is_none());
    let scanned: Vec<String> = reader
        .scan("node.")
        .await
        .expect("scan")
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert!(
        !scanned.contains(&"node.b".to_string()),
        "scan hides tombstone"
    );

    // …so keys() must agree, or callers that list-then-get see phantom keys.
    let keys = reader.keys("node.").await.expect("keys");
    assert_eq!(
        keys,
        vec!["node.a".to_string()],
        "keys() must exclude CAS tombstones for consistency with get()/scan()"
    );
}

// --- Watch -------------------------------------------------------------------

#[tokio::test]
async fn watch_all_streams_live_updates() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("watch").await;
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = watcher.watch_all(tx).await;
    });
    establish_watch(writer.as_ref(), &mut rx, "__ready__").await;

    // A single writer issues these sequentially, so order is deterministic.
    writer.put("node.a", b"1").await.expect("put a");
    writer.put("node.b", b"2").await.expect("put b");
    writer.delete("node.a").await.expect("delete a");

    let updates = collect_updates(&mut rx, 3).await;
    assert!(matches!(&updates[0], KvUpdate::Put(e) if e.key == "node.a" && e.value == b"1"));
    assert!(matches!(&updates[1], KvUpdate::Put(e) if e.key == "node.b" && e.value == b"2"));
    assert!(matches!(&updates[2], KvUpdate::Delete { key, .. } if key == "node.a"));
}

#[tokio::test]
async fn watch_prefix_filters_by_subject() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("watch").await;
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = watcher.watch_prefix("node.", tx).await;
    });
    // Sentinel must live within the watched prefix to be echoed back.
    establish_watch(writer.as_ref(), &mut rx, "node.__ready__").await;

    writer.put("other.x", b"skip").await.expect("put other"); // filtered out
    writer.put("node.a", b"keep").await.expect("put node");

    // Only the in-prefix update should arrive.
    let updates = collect_updates(&mut rx, 1).await;
    assert!(matches!(&updates[0], KvUpdate::Put(e) if e.key == "node.a"));
}

#[tokio::test]
async fn watch_all_from_replays_only_the_delta() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("watch").await;
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    // Phase 1: establish a baseline and capture the cursor.
    writer.put("node.a", b"1").await.expect("put a");
    let cursor_rev = writer.put("node.b", b"2").await.expect("put b");
    let cursor = WatchCursor::from_u64(cursor_rev.as_u64().expect("u64 rev"));

    // Phase 2: more changes after the cursor.
    writer.put("node.c", b"3").await.expect("put c");
    writer.put("node.a", b"1b").await.expect("update a");

    // Resuming from the cursor replays exactly the two post-cursor writes.
    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = watcher.watch_all_from(&cursor, tx).await;
    });

    let updates = collect_updates(&mut rx, 2).await;
    let keys: Vec<&str> = updates.iter().map(|u| u.key()).collect();
    assert!(keys.contains(&"node.c"), "delta includes node.c: {keys:?}");
    assert!(
        keys.contains(&"node.a"),
        "delta includes updated node.a: {keys:?}"
    );
}

#[tokio::test]
async fn watch_prefix_from_replays_only_the_delta() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("watch").await;
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    writer.put("node.a", b"1").await.expect("put a");
    let cursor_rev = writer.put("other.z", b"z").await.expect("put z");
    let cursor = WatchCursor::from_u64(cursor_rev.as_u64().expect("u64 rev"));

    writer.put("node.b", b"2").await.expect("put b");
    writer.put("other.y", b"y").await.expect("put y");

    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = watcher.watch_prefix_from("node.", &cursor, tx).await;
    });

    // Only node.b is both after the cursor and within the prefix.
    let updates = collect_updates(&mut rx, 1).await;
    assert!(matches!(&updates[0], KvUpdate::Put(e) if e.key == "node.b"));
}

#[tokio::test]
async fn watch_from_compacted_cursor_does_not_spuriously_fail() {
    let nats = TestNats::start().await;
    let conn = nats.connect().await;
    // history=1 means each re-put of the same key purges the prior revision,
    // advancing the stream's first sequence past old cursors.
    let store = conn
        .store_with_config(StoreConfig {
            name: "compacted".into(),
            max_history: Some(1),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open store");
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    for i in 0..6u8 {
        writer.put("k", &[i]).await.expect("put k");
    }

    // Cursor points at revision 1, long since compacted away.
    let cursor = WatchCursor::from_u64(1);
    let (tx, _rx) = mpsc::channel(64);

    // Either the backend reports CursorExpired (caller should full-replay) or it
    // transparently resumes from the earliest available revision and streams —
    // in which case the call blocks and we hit the timeout. Both are acceptable;
    // a returned WatchError would be the bug we're guarding against.
    let res = timeout(Duration::from_secs(2), watcher.watch_all_from(&cursor, tx)).await;
    match res {
        Ok(Ok(())) => {} // stream ended cleanly
        Ok(Err(KvError::CursorExpired)) => {}
        Err(_elapsed) => {} // resumed and is streaming — fine
        Ok(Err(other)) => panic!("unexpected watch error from compacted cursor: {other:?}"),
    }
}

// --- Lifecycle: reconnect & concurrency -------------------------------------

#[tokio::test]
async fn reconnect_after_shutdown() {
    let nats = TestNats::start().await;
    let conn = nats.connect().await;
    assert!(conn.is_healthy(), "healthy after first connect");

    conn.shutdown().await.expect("shutdown");
    assert!(!conn.is_healthy(), "not healthy after shutdown");

    // The state machine documents SHUTDOWN → connect() can reconnect. The
    // fast-path `healthy` check must not strand a shut-down connection.
    conn.connect().await.expect("reconnect after shutdown");
    assert!(conn.is_healthy(), "healthy after reconnect");

    // A reconnected connection must serve real work, not just flip the flag.
    let store = conn
        .store_with_config(StoreConfig {
            name: "reconnect".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open store after reconnect");
    let writer = store.writer().expect("writer");
    writer.put("k", b"v").await.expect("put after reconnect");
    let entry = store
        .reader()
        .get("k")
        .await
        .expect("get after reconnect")
        .expect("present");
    assert_eq!(entry.value, b"v");
}

#[tokio::test]
async fn concurrent_connect_is_safe() {
    let nats = TestNats::start().await;
    let conn = Arc::new(NatsConnection::new(NatsConnectionConfig {
        url: nats.url.clone(),
        creds: None,
        creds_file: None,
    }));

    // Many callers race into connect() at once. The double-checked lock in
    // connect() must install exactly one handle and drop the losers' freshly
    // built connections — every call still returns Ok and the connection works.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let c = Arc::clone(&conn);
        handles.push(tokio::spawn(async move { c.connect().await }));
    }
    for h in handles {
        h.await
            .expect("connect task panicked")
            .expect("concurrent connect failed");
    }

    assert!(conn.is_healthy(), "healthy after concurrent connect");

    // The surviving handle is functional — a store opens and round-trips.
    let store = conn
        .store_with_config(StoreConfig {
            name: "concurrent".into(),
            max_bytes: Some(1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open store after concurrent connect");
    store
        .writer()
        .expect("writer")
        .put("k", b"v")
        .await
        .expect("put after concurrent connect");
}

// --- create() vs the two delete variants ------------------------------------

#[tokio::test]
async fn create_succeeds_after_plain_delete() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");

    writer.put("lock.x", b"1").await.expect("put");
    assert!(writer.delete("lock.x").await.expect("delete"));

    // A plain delete() writes a Delete marker, so the key is logically absent
    // and a fresh create() must succeed — the lock can be re-acquired.
    let v = writer
        .create("lock.x", b"2")
        .await
        .expect("create after plain delete should succeed");
    assert!(v.as_u64().is_some());
    let entry = store
        .reader()
        .get("lock.x")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(entry.value, b"2");
}

#[tokio::test]
async fn create_conflicts_after_delete_with_version() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    let v1 = writer.put("lock.y", b"1").await.expect("put");
    assert!(
        writer
            .delete_with_version("lock.y", &v1)
            .await
            .expect("cas delete")
    );

    // delete_with_version() writes an empty-value *Put* tombstone (so concurrent
    // CAS writers still conflict). NATS therefore still sees a live Put on the
    // key, and create() — which requires the last op to be Delete/Purge or
    // absent — conflicts. This is the load-bearing difference from delete():
    // a versioned delete does NOT free the key for create().
    let err = writer
        .create("lock.y", b"2")
        .await
        .expect_err("create after versioned delete must conflict");
    assert!(matches!(err, KvError::AlreadyExists), "got {err:?}");

    // get() still hides the tombstone from ordinary readers.
    assert!(reader.get("lock.y").await.expect("get").is_none());
}

// --- Empty-prefix scan / keys (full-bucket path) ----------------------------

#[tokio::test]
async fn scan_empty_prefix_returns_all_live_entries() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"1").await.expect("put a");
    writer.put("other.b", b"2").await.expect("put b");
    writer.put("third.c", b"3").await.expect("put c");
    writer.delete("other.b").await.expect("delete b");

    // Empty prefix exercises the `$KV.{bucket}.>` branch of the consumer filter,
    // distinct from the `{prefix}>` branch every other test uses.
    let mut entries = reader.scan("").await.expect("scan all");
    entries.sort_by(|a, b| a.key.cmp(&b.key));

    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["node.a", "third.c"], "deleted b excluded");
}

#[tokio::test]
async fn keys_empty_prefix_returns_all_live_keys() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("node.a", b"1").await.expect("put a");
    writer.put("other.b", b"2").await.expect("put b");
    writer.delete("other.b").await.expect("delete b");

    let mut keys = reader.keys("").await.expect("keys all");
    keys.sort();
    assert_eq!(keys, vec!["node.a".to_string()], "deleted b excluded");
}

// --- Watch task teardown -----------------------------------------------------

#[tokio::test]
async fn dropping_receiver_stops_watch_task() {
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("watch").await;
    let writer = store.writer().expect("writer");
    let watcher = store.watcher().expect("watcher");

    let (tx, mut rx) = mpsc::channel(64);
    let task = tokio::spawn(async move { watcher.watch_all(tx).await });
    establish_watch(writer.as_ref(), &mut rx, "__ready__").await;

    // Drop the receiver: the watcher task should notice on its next send and
    // exit cleanly (Ok(())), letting JetStream tear the subscription down.
    drop(rx);

    // The task only observes the closed channel when it has something to send,
    // so push updates until it terminates. Without the fix this loop would run
    // until the timeout below fires.
    let exited = timeout(Duration::from_secs(5), async {
        loop {
            // A live key in the (unfiltered) watch guarantees a delivery attempt.
            writer.put("node.poke", b"x").await.expect("poke put");
            if task.is_finished() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(
        exited.is_ok(),
        "watch task did not exit after the receiver was dropped"
    );
    let result = task.await.expect("watch task panicked");
    assert!(
        matches!(result, Ok(())),
        "watch task should exit cleanly on receiver drop, got {result:?}"
    );
}

// --- Large-bucket scan/keys (max_ack_pending) --------------------------------

#[tokio::test]
async fn scan_and_keys_cover_buckets_larger_than_max_ack_pending() {
    let nats = TestNats::start().await;
    // Generous max_bytes: 1500 small entries plus per-message overhead.
    let conn = nats.connect().await;
    let store = conn
        .store_with_config(StoreConfig {
            name: "big".into(),
            max_bytes: Some(64 * 1024 * 1024),
            ..Default::default()
        })
        .await
        .expect("open store");
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    // Cross the JetStream default `max_ack_pending` (1000). The scan/keys
    // consumer never acks, so under the default Explicit ack policy delivery
    // stalls at 1000 — silently truncating the result (or hanging). With
    // `AckPolicy::None` every key must come through. The outer timeouts turn a
    // regression into a fast failure instead of a hung suite.
    const N: usize = 1500;
    for i in 0..N {
        writer
            .put(&format!("node.{i:05}"), b"v")
            .await
            .expect("put");
    }

    let entries = timeout(Duration::from_secs(30), reader.scan("node."))
        .await
        .expect("scan must not hang past max_ack_pending")
        .expect("scan");
    assert_eq!(
        entries.len(),
        N,
        "scan must return every key past max_ack_pending, got {}",
        entries.len()
    );

    let keys = timeout(Duration::from_secs(30), reader.keys("node."))
        .await
        .expect("keys must not hang past max_ack_pending")
        .expect("keys");
    assert_eq!(
        keys.len(),
        N,
        "keys must return every key past max_ack_pending, got {}",
        keys.len()
    );
}

// --- Edge cases in read semantics --------------------------------------------

#[tokio::test]
async fn get_treats_empty_value_as_absent() {
    // get() filters any entry with an empty value to present a uniform
    // "absent = None" contract — the same check that hides delete_with_version
    // tombstones. A caller who stores b"" (e.g. a presence flag) gets None back
    // from get(), which is data-loss if unexpected. This test documents that
    // callers needing zero-length semantics must use entry() instead.
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");
    let reader = store.reader();

    writer.put("flag.x", b"").await.expect("put empty value");

    // get() hides the empty-value entry — same behaviour as delete_with_version tombstone.
    assert!(reader.get("flag.x").await.expect("get").is_none());

    // entry() exposes the raw Put record so callers with empty-value semantics
    // can still access both the version and the fact that the key exists.
    let raw = reader
        .entry("flag.x")
        .await
        .expect("entry")
        .expect("present via entry()");
    assert!(raw.value.is_empty());
}

#[tokio::test]
async fn delete_with_version_on_missing_key_returns_revision_mismatch() {
    // NATS returns WrongLastRevision when there is no entry at the provided
    // revision — which covers both "someone else updated it" and "key does not
    // exist". Callers cannot distinguish the two via the error alone.
    let nats = TestNats::start().await;
    let (_conn, store) = nats.store("rw").await;
    let writer = store.writer().expect("writer");

    let err = writer
        .delete_with_version("never.existed", &VersionToken::from_u64(1))
        .await
        .expect_err("delete_with_version on absent key must fail");
    assert!(
        matches!(err, KvError::RevisionMismatch),
        "got {err:?} — NATS does not distinguish 'wrong version' from 'key absent'"
    );
}

#[tokio::test]
async fn store_with_config_is_idempotent() {
    // Creating the same bucket twice on the same connection must succeed both
    // times and the second call must return a functional store.
    let nats = TestNats::start().await;
    let conn = nats.connect().await;
    let cfg = StoreConfig {
        name: "idempotent".to_string(),
        max_bytes: Some(1024 * 1024),
        ..Default::default()
    };

    let store1 = conn
        .store_with_config(cfg.clone())
        .await
        .expect("first store_with_config");
    let store2 = conn
        .store_with_config(cfg)
        .await
        .expect("second store_with_config must not fail");

    // Both handles must be functional.
    store1
        .writer()
        .expect("writer")
        .put("k", b"v")
        .await
        .expect("put via first handle");
    let entry = store2
        .reader()
        .get("k")
        .await
        .expect("get via second handle")
        .expect("present");
    assert_eq!(entry.value, b"v");
}

// --- Health tracks real connection state -------------------------------------

#[tokio::test]
async fn health_reflects_server_death() {
    let nats = TestNats::start().await;
    let conn = nats.connect().await;
    assert!(conn.is_healthy(), "healthy immediately after connect");

    // Kill the server out from under the live connection. async-nats sees the
    // socket close and fires `Disconnected`; the health flag must follow reality
    // rather than staying pinned at its connect-time `true`. Before the
    // event-callback fix this loop never exits and the test times out.
    drop(nats);

    let flipped = timeout(Duration::from_secs(15), async {
        while conn.is_healthy() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        flipped.is_ok(),
        "is_healthy() must report false after the NATS server dies"
    );
}
