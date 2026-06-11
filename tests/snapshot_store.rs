//! Backend-agnostic conformance suite for [`SnapshotStore`].
//!
//! Every check is written generically over a backend and an `open` closure, then
//! instantiated for each shipped backend: [`AppendLogSnapshot`] (always) and
//! `FjallSnapshot` (behind `--features fjall`), and `RocksDbSnapshot` (behind
//! `--features rocksdb`). New backends get the whole suite by adding two
//! wrapper lines.
//!
//! Run the full matrix with:
//! ```text
//! cargo test --test snapshot_store
//! cargo test --test snapshot_store --features fjall
//! cargo test --test snapshot_store --features rocksdb
//! ```

use std::path::Path;

use slipstream::snapshot::SnapshotStore;
use slipstream::{KvEntry, KvUpdate, VersionToken, WatchCursor};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
    KvUpdate::Put(KvEntry {
        key: key.to_string(),
        value: value.to_vec(),
        version: VersionToken::from_u64(rev),
    })
}

fn del(key: &str, rev: u64) -> KvUpdate {
    KvUpdate::Delete {
        key: key.to_string(),
        version: VersionToken::from_u64(rev),
    }
}

fn purge(key: &str, rev: u64) -> KvUpdate {
    KvUpdate::Purge {
        key: key.to_string(),
        version: VersionToken::from_u64(rev),
    }
}

/// A deterministic stream exercising puts, an overwrite, and a delete. Final live
/// state: `node.a=v1b`, `node.c=v3`, `svc.x=sx`, `svc.y=sy` (`node.b` deleted),
/// resume cursor `7`.
fn stream() -> Vec<KvUpdate> {
    vec![
        put("node.a", b"v1", 1),
        put("node.b", b"v2", 2),
        put("svc.x", b"sx", 3),
        put("node.a", b"v1b", 4), // overwrite
        del("node.b", 5),         // delete
        put("node.c", b"v3", 6),
        put("svc.y", b"sy", 7),
    ]
}

/// Live state of [`stream`] as `(key, value)` pairs in key order — what every
/// backend must converge to.
fn expected_state() -> Vec<(String, Vec<u8>)> {
    vec![
        ("node.a".into(), b"v1b".to_vec()),
        ("node.c".into(), b"v3".to_vec()),
        ("svc.x".into(), b"sx".to_vec()),
        ("svc.y".into(), b"sy".to_vec()),
    ]
}

/// Fold `updates` into `store` as a series of batches (3 updates each), advancing
/// the cursor to each batch's last revision — mirroring how `watch_applied` flushes.
fn fold<S: SnapshotStore>(store: &mut S, updates: &[KvUpdate]) {
    for chunk in updates.chunks(3) {
        let cursor = WatchCursor::from_version(chunk.last().unwrap().version().clone());
        store.apply(chunk, &cursor).expect("apply batch");
    }
}

/// The full live state as `(key, value)` pairs in key order, via `range("")`.
fn dump<S: SnapshotStore>(store: &S) -> Vec<(String, Vec<u8>)> {
    store
        .range("")
        .expect("range")
        .into_iter()
        .map(|e| (e.key, e.value))
        .collect()
}

// ---------------------------------------------------------------------------
// Generic checks — each runs against any backend via its `open` closure
// ---------------------------------------------------------------------------

/// Round-trip: fold, drop, reopen — state and cursor survive the restart.
fn check_round_trip<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");

    {
        let (_resume, mut s) = open(&path);
        fold(&mut s, &stream());
    } // drop closes the store

    let (cursor, s) = open(&path);
    assert_eq!(cursor.as_u64(), Some(7), "resume cursor survives reopen");
    assert_eq!(dump(&s), expected_state(), "state survives reopen");
}

/// get/range correctness: point lookups, deleted keys, prefix scans, ordering.
fn check_get_range<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let (_resume, mut s) = open(&path);
    fold(&mut s, &stream());

    // Point lookups: live key, overwritten key (latest value), deleted key, absent key.
    assert_eq!(s.get("svc.x").unwrap().unwrap().value, b"sx");
    assert_eq!(s.get("node.a").unwrap().unwrap().value, b"v1b");
    assert!(
        s.get("node.b").unwrap().is_none(),
        "deleted key reads as None"
    );
    assert!(s.get("absent").unwrap().is_none());

    // The matched entry carries its version.
    assert_eq!(s.get("svc.x").unwrap().unwrap().version.as_u64(), Some(3));

    // Prefix scan: only matching, deleted excluded, ascending key order.
    let nodes: Vec<String> = s
        .range("node.")
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert_eq!(nodes, vec!["node.a".to_string(), "node.c".to_string()]);

    let svcs: Vec<String> = s
        .range("svc.")
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert_eq!(svcs, vec!["svc.x".to_string(), "svc.y".to_string()]);

    // Empty prefix returns everything; a non-matching prefix returns nothing.
    assert_eq!(s.range("").unwrap().len(), 4);
    assert!(s.range("zzz").unwrap().is_empty());
}

/// `for_each_in_range` streams the same entries `range` buffers — same matches,
/// same ascending order, deletes excluded — and an early `Err` from the callback
/// stops the scan and propagates.
fn check_for_each_in_range<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let (_resume, mut s) = open(&path);
    fold(&mut s, &stream());

    // Streamed scan equals the buffered scan, for a prefix and for everything.
    for prefix in ["", "node.", "svc.", "zzz"] {
        let mut streamed = Vec::new();
        s.for_each_in_range(prefix, |e| {
            streamed.push((e.key, e.value));
            Ok(())
        })
        .expect("for_each_in_range");
        let buffered: Vec<_> = s
            .range(prefix)
            .unwrap()
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        assert_eq!(
            streamed, buffered,
            "streamed scan matches range for {prefix:?}"
        );
    }

    // An `Err` from the callback halts the scan early and propagates.
    let mut seen = 0;
    let result = s.for_each_in_range("", |_| {
        seen += 1;
        if seen == 2 {
            return Err(slipstream::snapshot::SnapshotError::Backend("stop".into()));
        }
        Ok(())
    });
    assert!(result.is_err(), "callback error propagates");
    assert_eq!(seen, 2, "scan stops at the first callback error");
}

/// Cursor-resume after a reconnect: fold a first segment, reopen (cursor reflects
/// it), fold the post-cursor delta, reopen again (cursor advanced). Models a
/// service restarting and resuming the watch from the persisted position.
fn check_cursor_resume<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    let all = stream();
    let (first, rest) = all.split_at(3); // revs 1..=3, then 4..=7

    {
        let (_r, mut s) = open(&path);
        fold(&mut s, first);
    }

    // Scope the handle: a "restart" means the prior store is gone before the next
    // open. An on-disk backend may hold a single-writer lock on the path (fjall
    // does), so leaving this handle alive would block the reopen below.
    {
        let (resume, _s) = open(&path);
        assert_eq!(
            resume.as_u64(),
            Some(3),
            "cursor reflects the first segment"
        );
    }

    {
        let (r, mut s) = open(&path);
        assert_eq!(r.as_u64(), Some(3));
        fold(&mut s, rest); // only the post-cursor delta
    }

    let (resume2, s) = open(&path);
    assert_eq!(resume2.as_u64(), Some(7), "cursor advanced over the delta");
    assert_eq!(dump(&s), expected_state());
}

/// PROPERTY — pure function of the log. Lose the store mid-stream, replay from the
/// cursor, and the fold is byte-identical to one that never lost the store.
///
/// "rm the store" wipes its files (and therefore its cursor), so reopen resumes
/// from `none()` and the entire stream is replayed — exactly what a consumer does
/// when its cache is gone: full re-fold from NATS. The result must match a
/// continuous, never-interrupted fold.
fn check_property_pure_fold<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let updates = stream();

    // Reference: one continuous store, never lost.
    let ref_dir = TempDir::new().unwrap();
    let ref_path = ref_dir.path().join("store");
    let (_r, mut reference) = open(&ref_path);
    fold(&mut reference, &updates);
    let reference_dump = dump(&reference);
    drop(reference);

    // Victim: fold the first half, then rm the store mid-stream, reopen fresh,
    // and replay the WHOLE stream from the start (cursor is gone with the files).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");
    {
        let (_r, mut s) = open(&path);
        fold(&mut s, &updates[..4]);
    }
    wipe(&path);
    let (resume, mut s) = open(&path);
    assert!(resume.is_none(), "a wiped store reopens with no cursor");
    fold(&mut s, &updates);

    assert_eq!(
        dump(&s),
        reference_dump,
        "replay after losing the store is byte-identical to the continuous fold"
    );
    // And identical to the independently-computed expected state.
    assert_eq!(dump(&s), expected_state());
}

/// `Purge` is folded the same as `Delete` — the key disappears, untouched keys
/// survive, and the removal persists across a reopen. The shared `stream()` only
/// exercises `Put`/`Delete`, so without this the `Purge` match arm is dead in tests
/// and a refactor that diverges purge from delete would ship unnoticed.
fn check_purge<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");

    {
        let (_r, mut s) = open(&path);
        s.apply(
            &[put("a", b"1", 1), put("b", b"2", 2)],
            &WatchCursor::from_u64(2),
        )
        .expect("seed");
        s.apply(&[purge("a", 3)], &WatchCursor::from_u64(3))
            .expect("purge");

        assert!(s.get("a").unwrap().is_none(), "purged key is gone");
        assert_eq!(
            s.get("b").unwrap().unwrap().value,
            b"2",
            "untouched key survives a purge of its neighbor"
        );
    }

    // The purge persists across a restart, just like a delete.
    let (cursor, s) = open(&path);
    assert_eq!(cursor.as_u64(), Some(3), "cursor advanced over the purge");
    assert!(
        s.get("a").unwrap().is_none(),
        "purge persists across reopen"
    );
    assert_eq!(s.get("b").unwrap().unwrap().value, b"2");
}

/// An empty batch carries no data but still advances (and persists) the cursor.
/// `watch_applied` can flush a batch with zero updates (e.g. a heartbeat that only
/// moves the resume position); the cursor must follow it, or a restart re-folds
/// already-seen revisions.
fn check_empty_batch_advances_cursor<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");

    {
        let (_r, mut s) = open(&path);
        fold(&mut s, &stream()[..3]); // real data up to rev 3
        s.apply(&[], &WatchCursor::from_u64(9))
            .expect("empty batch applies");
    }

    let (cursor, s) = open(&path);
    assert_eq!(
        cursor.as_u64(),
        Some(9),
        "empty batch still advances and persists the cursor"
    );
    assert_eq!(dump(&s).len(), 3, "an empty batch mutates no data");
}

/// A stored empty value round-trips as a *present* entry, not as a deletion. This
/// is the CAS-tombstone shape (`delete_with_version` writes an empty-value `Put` so
/// concurrent writers still conflict) — the snapshot layer must preserve it
/// verbatim, including across a reopen, so the version stays available for CAS.
fn check_empty_value_round_trip<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store");

    {
        let (_r, mut s) = open(&path);
        s.apply(&[put("lock", b"", 1)], &WatchCursor::from_u64(1))
            .expect("apply empty-value put");
        let e = s
            .get("lock")
            .unwrap()
            .expect("empty-value entry is present, not absent");
        assert!(e.value.is_empty());
        assert_eq!(
            e.version.as_u64(),
            Some(1),
            "version survives an empty value"
        );
    }

    let (_cursor, s) = open(&path);
    let e = s
        .get("lock")
        .unwrap()
        .expect("empty-value entry survives reopen, not confused with a delete");
    assert!(e.value.is_empty());
}

/// Remove a store at `path`, whether it is a single file (append log) or a
/// directory (fjall keyspace, RocksDB database).
fn wipe(path: &Path) {
    if path.is_dir() {
        std::fs::remove_dir_all(path).expect("rm store dir");
    } else if path.exists() {
        std::fs::remove_file(path).expect("rm store file");
    }
}

// ---------------------------------------------------------------------------
// Export / import — generic checks
// ---------------------------------------------------------------------------

use slipstream::snapshot::SnapshotError;
use slipstream::{ExportManifest, MANIFEST_FILE};

type ImportFn<S> = fn(&Path, &Path) -> Result<(WatchCursor, S), SnapshotError>;

/// Export a fold of [`stream`] and return `(artifact_dir, manifest, tempdir)`.
fn exported_stream_artifact<S: SnapshotStore>(
    open: &impl Fn(&Path) -> (WatchCursor, S),
) -> (PathBuf, ExportManifest, TempDir) {
    let dir = TempDir::new().unwrap();
    let store_path = dir.path().join("store");
    let artifact = dir.path().join("artifact");
    let (_r, mut s) = open(&store_path);
    fold(&mut s, &stream());
    let manifest = s.export_to(&artifact).expect("export");
    assert_eq!(
        manifest.cursor,
        s.cursor(),
        "manifest cursor equals the live fold's cursor"
    );
    (artifact, manifest, dir)
}

use std::path::PathBuf;

/// Round-trip: export a fold, import it elsewhere, and the imported store has
/// the same cursor and byte-identical state.
fn check_export_import_round_trip<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, manifest, dir) = exported_stream_artifact(&open);
    assert_eq!(manifest.cursor.as_u64(), Some(7));
    assert!(!manifest.files.is_empty(), "manifest lists payload files");
    assert!(artifact.join(MANIFEST_FILE).is_file());

    let dest = dir.path().join("imported");
    let (cursor, s) = import(&artifact, &dest).expect("import");
    assert_eq!(
        cursor.as_u64(),
        Some(7),
        "imported cursor is the manifest's"
    );
    assert_eq!(cursor, manifest.cursor);
    assert_eq!(dump(&s), expected_state(), "imported state is identical");
}

/// The scaling property behind import: fold the post-cursor delta into the
/// imported store and it converges to the continuous-fold reference — import +
/// tail replay, never a full re-fold.
fn check_import_resume_continues_fold<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let updates = stream();
    let dir = TempDir::new().unwrap();

    // Export mid-stream, after revs 1..=4.
    let store_path = dir.path().join("store");
    let artifact = dir.path().join("artifact");
    let (_r, mut s) = open(&store_path);
    fold(&mut s, &updates[..4]);
    let manifest = s.export_to(&artifact).expect("export");
    assert_eq!(manifest.cursor.as_u64(), Some(4));

    // Import on "another node" and fold ONLY the tail (revs 5..=7).
    let dest = dir.path().join("imported");
    let (cursor, mut imported) = import(&artifact, &dest).expect("import");
    assert_eq!(cursor.as_u64(), Some(4));
    fold(&mut imported, &updates[4..]);

    assert_eq!(
        dump(&imported),
        expected_state(),
        "import + tail replay equals the continuous fold"
    );
}

/// A flipped byte in the payload fails checksum verification, and nothing is
/// ever created at the destination (stage-then-rename crash safety).
fn check_import_rejects_tampered_payload<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, manifest, dir) = exported_stream_artifact(&open);

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

    let dest = dir.path().join("imported");
    match import(&artifact, &dest) {
        Err(SnapshotError::ArtifactInvalid(msg)) => {
            assert!(
                msg.contains("checksum") || msg.contains("recover") || msg.contains("cursor"),
                "rejection names the failure: {msg}"
            );
        }
        Err(other) => panic!("expected ArtifactInvalid, got {other:?}"),
        Ok(_) => panic!("tampered artifact must not import"),
    }
    assert!(!dest.exists(), "nothing lands at the destination");
}

/// A payload file missing from the artifact is rejected.
fn check_import_rejects_missing_payload_file<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, manifest, dir) = exported_stream_artifact(&open);
    std::fs::remove_file(artifact.join(&manifest.files[0].path)).unwrap();

    let dest = dir.path().join("imported");
    assert!(
        matches!(
            import(&artifact, &dest),
            Err(SnapshotError::ArtifactInvalid(_))
        ),
        "missing payload file must be rejected"
    );
    assert!(!dest.exists());
}

/// A payload file the manifest never declared is rejected (it was never hashed
/// at export, so it cannot be trusted).
fn check_import_rejects_undeclared_extra_file<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _manifest, dir) = exported_stream_artifact(&open);
    std::fs::write(artifact.join("data").join("smuggled"), b"x").unwrap();

    let dest = dir.path().join("imported");
    assert!(
        matches!(
            import(&artifact, &dest),
            Err(SnapshotError::ArtifactInvalid(_))
        ),
        "undeclared extra payload file must be rejected"
    );
    assert!(!dest.exists());
}

/// Rewrite one top-level manifest field (checksums untouched) and re-import.
fn with_doctored_manifest(artifact: &Path, field: &str, value: serde_json::Value) {
    let raw = std::fs::read(artifact.join(MANIFEST_FILE)).unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    json[field] = value;
    std::fs::write(
        artifact.join(MANIFEST_FILE),
        serde_json::to_vec(&json).unwrap(),
    )
    .unwrap();
}

/// Wrong backend identity in the manifest is rejected.
fn check_import_rejects_wrong_backend<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _m, dir) = exported_stream_artifact(&open);
    with_doctored_manifest(&artifact, "backend", "bogus-backend".into());
    let dest = dir.path().join("imported");
    assert!(matches!(
        import(&artifact, &dest),
        Err(SnapshotError::ArtifactInvalid(_))
    ));
    assert!(!dest.exists());
}

/// Unsupported artifact schema version is rejected.
fn check_import_rejects_schema_version<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _m, dir) = exported_stream_artifact(&open);
    with_doctored_manifest(&artifact, "schema_version", 999.into());
    let dest = dir.path().join("imported");
    assert!(matches!(
        import(&artifact, &dest),
        Err(SnapshotError::ArtifactInvalid(_))
    ));
    assert!(!dest.exists());
}

/// Mismatched on-disk format generation is rejected (strict backends only:
/// append-log and fjall gate on it; RocksDB deliberately does not).
fn check_import_rejects_backend_version<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _m, dir) = exported_stream_artifact(&open);
    with_doctored_manifest(&artifact, "backend_version", "999".into());
    let dest = dir.path().join("imported");
    assert!(matches!(
        import(&artifact, &dest),
        Err(SnapshotError::ArtifactInvalid(_))
    ));
    assert!(!dest.exists());
}

/// A doctored cursor (valid hex, valid checksums) is caught by the post-open
/// cursor-equality gate — the payload's recovered cursor is the authority.
fn check_import_rejects_cursor_mismatch<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _m, dir) = exported_stream_artifact(&open);
    // rev 999 as 8-byte big-endian hex — well-formed, just wrong.
    with_doctored_manifest(&artifact, "cursor_hex", "00000000000003e7".into());
    let dest = dir.path().join("imported");
    match import(&artifact, &dest) {
        Err(SnapshotError::ArtifactInvalid(msg)) => {
            assert!(msg.contains("cursor"), "rejection names the cursor: {msg}");
        }
        Err(other) => panic!("expected ArtifactInvalid, got {other:?}"),
        Ok(_) => panic!("cursor mismatch must not import"),
    }
    assert!(!dest.exists());
}

/// Import refuses a non-empty destination, and the artifact survives untouched
/// (a retry against a fresh destination succeeds).
fn check_import_rejects_nonempty_dest<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let (artifact, _m, dir) = exported_stream_artifact(&open);

    let dest = dir.path().join("occupied");
    std::fs::create_dir(&dest).unwrap();
    std::fs::write(dest.join("stray"), b"x").unwrap();
    assert!(matches!(
        import(&artifact, &dest),
        Err(SnapshotError::ArtifactInvalid(_))
    ));
    assert!(dest.join("stray").exists(), "occupied dest untouched");

    let fresh = dir.path().join("fresh");
    let (cursor, s) = import(&artifact, &fresh).expect("artifact survives a refused import");
    assert_eq!(cursor.as_u64(), Some(7));
    assert_eq!(dump(&s), expected_state());
}

/// Export refuses a non-empty destination.
fn check_export_rejects_nonempty_dest<S: SnapshotStore>(open: impl Fn(&Path) -> (WatchCursor, S)) {
    let dir = TempDir::new().unwrap();
    let (_r, mut s) = open(&dir.path().join("store"));
    fold(&mut s, &stream());

    let dest = dir.path().join("occupied");
    std::fs::create_dir(&dest).unwrap();
    std::fs::write(dest.join("stray"), b"x").unwrap();
    assert!(matches!(
        s.export_to(&dest),
        Err(SnapshotError::ArtifactInvalid(_))
    ));
    assert!(dest.join("stray").exists(), "occupied dest untouched");
}

/// An empty fold (nothing applied, no cursor) exports and imports cleanly.
fn check_export_empty_store<S: SnapshotStore>(
    open: impl Fn(&Path) -> (WatchCursor, S),
    import: ImportFn<S>,
) {
    let dir = TempDir::new().unwrap();
    let (_r, mut s) = open(&dir.path().join("store"));
    let artifact = dir.path().join("artifact");
    let manifest = s.export_to(&artifact).expect("export empty fold");
    assert!(manifest.cursor.is_none(), "empty fold has no cursor");

    let dest = dir.path().join("imported");
    let (cursor, imported) = import(&artifact, &dest).expect("import empty fold");
    assert!(cursor.is_none());
    assert!(dump(&imported).is_empty());
}

// ---------------------------------------------------------------------------
// AppendLogSnapshot — the default backend
// ---------------------------------------------------------------------------

use slipstream::AppendLogSnapshot;

fn open_append_log(path: &Path) -> (WatchCursor, AppendLogSnapshot) {
    AppendLogSnapshot::open(path, u64::MAX).expect("open append log")
}

#[test]
fn append_log_round_trip() {
    check_round_trip(open_append_log);
}

#[test]
fn append_log_get_range() {
    check_get_range(open_append_log);
}

#[test]
fn append_log_for_each_in_range() {
    check_for_each_in_range(open_append_log);
}

#[test]
fn append_log_cursor_resume() {
    check_cursor_resume(open_append_log);
}

#[test]
fn append_log_pure_fold_property() {
    check_property_pure_fold(open_append_log);
}

#[test]
fn append_log_purge() {
    check_purge(open_append_log);
}

#[test]
fn append_log_empty_batch_advances_cursor() {
    check_empty_batch_advances_cursor(open_append_log);
}

#[test]
fn append_log_empty_value_round_trip() {
    check_empty_value_round_trip(open_append_log);
}

fn import_append_log(
    artifact: &Path,
    dest: &Path,
) -> Result<(WatchCursor, AppendLogSnapshot), SnapshotError> {
    AppendLogSnapshot::import(artifact, dest, u64::MAX)
}

#[test]
fn append_log_export_import_round_trip() {
    check_export_import_round_trip(open_append_log, import_append_log);
}

#[test]
fn append_log_import_resume_continues_fold() {
    check_import_resume_continues_fold(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_tampered_payload() {
    check_import_rejects_tampered_payload(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_missing_payload_file() {
    check_import_rejects_missing_payload_file(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_undeclared_extra_file() {
    check_import_rejects_undeclared_extra_file(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_wrong_backend() {
    check_import_rejects_wrong_backend(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_schema_version() {
    check_import_rejects_schema_version(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_backend_version() {
    check_import_rejects_backend_version(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_cursor_mismatch() {
    check_import_rejects_cursor_mismatch(open_append_log, import_append_log);
}

#[test]
fn append_log_import_rejects_nonempty_dest() {
    check_import_rejects_nonempty_dest(open_append_log, import_append_log);
}

#[test]
fn append_log_export_rejects_nonempty_dest() {
    check_export_rejects_nonempty_dest(open_append_log);
}

#[test]
fn append_log_export_empty_store() {
    check_export_empty_store(open_append_log, import_append_log);
}

/// Backwards-compat: a file written by the existing [`SnapshotWriter`] API loads
/// through the new [`AppendLogSnapshot`] store (the on-disk v2 format is unchanged).
#[test]
fn append_log_loads_legacy_writer_file() {
    use slipstream::snapshot::SnapshotWriter;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.snap");

    let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
    w.write_update(&put("node.a", b"v1", 1)).unwrap();
    w.write_update(&put("node.b", b"v2", 2)).unwrap();
    w.checkpoint(&WatchCursor::from_u64(2)).unwrap();
    drop(w);

    let (cursor, s) = AppendLogSnapshot::open(&path, u64::MAX).unwrap();
    assert_eq!(cursor.as_u64(), Some(2), "legacy cursor loads");
    assert_eq!(s.get("node.a").unwrap().unwrap().value, b"v1");
    assert_eq!(s.get("node.b").unwrap().unwrap().value, b"v2");
}

// ---------------------------------------------------------------------------
// FjallSnapshot — on-disk backend (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "fjall")]
mod fjall_backend {
    use super::*;
    use slipstream::{FjallConfig, FjallSnapshot};

    fn open_no_sync(path: &Path) -> (WatchCursor, FjallSnapshot) {
        FjallSnapshot::open(
            path,
            FjallConfig {
                sync: false,
                ..Default::default()
            },
        )
        .expect("open fjall")
    }

    #[test]
    fn fjall_round_trip() {
        check_round_trip(open_no_sync);
    }

    #[test]
    fn fjall_get_range() {
        check_get_range(open_no_sync);
    }

    #[test]
    fn fjall_for_each_in_range() {
        check_for_each_in_range(open_no_sync);
    }

    #[test]
    fn fjall_cursor_resume() {
        check_cursor_resume(open_no_sync);
    }

    #[test]
    fn fjall_pure_fold_property() {
        check_property_pure_fold(open_no_sync);
    }

    #[test]
    fn fjall_purge() {
        check_purge(open_no_sync);
    }

    #[test]
    fn fjall_empty_batch_advances_cursor() {
        check_empty_batch_advances_cursor(open_no_sync);
    }

    #[test]
    fn fjall_empty_value_round_trip() {
        check_empty_value_round_trip(open_no_sync);
    }

    fn import_fjall(
        artifact: &Path,
        dest: &Path,
    ) -> Result<(WatchCursor, FjallSnapshot), SnapshotError> {
        FjallSnapshot::import(
            artifact,
            dest,
            FjallConfig {
                sync: false,
                ..Default::default()
            },
        )
    }

    #[test]
    fn fjall_export_import_round_trip() {
        check_export_import_round_trip(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_resume_continues_fold() {
        check_import_resume_continues_fold(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_tampered_payload() {
        check_import_rejects_tampered_payload(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_missing_payload_file() {
        check_import_rejects_missing_payload_file(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_undeclared_extra_file() {
        check_import_rejects_undeclared_extra_file(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_wrong_backend() {
        check_import_rejects_wrong_backend(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_schema_version() {
        check_import_rejects_schema_version(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_backend_version() {
        check_import_rejects_backend_version(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_cursor_mismatch() {
        check_import_rejects_cursor_mismatch(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_import_rejects_nonempty_dest() {
        check_import_rejects_nonempty_dest(open_no_sync, import_fjall);
    }

    #[test]
    fn fjall_export_rejects_nonempty_dest() {
        check_export_rejects_nonempty_dest(open_no_sync);
    }

    #[test]
    fn fjall_export_empty_store() {
        check_export_empty_store(open_no_sync, import_fjall);
    }

    /// `settle` (major compaction) must preserve the fold byte-for-byte.
    #[test]
    fn fjall_settle_preserves_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let (_r, mut s) = open_no_sync(&path);
        fold(&mut s, &stream());
        s.settle().expect("settle");
        assert_eq!(dump(&s), expected_state(), "state survives settle");
    }

    /// NO_SYNC crash-tail recovery. With sync off, commits are not fsync'd, but
    /// data and cursor share one atomic batch, so whatever survives is mutually
    /// consistent. We can't deterministically simulate a power-loss (a clean drop
    /// flushes fjall's journal), so this asserts the load-bearing invariants:
    /// after reopen the recovered cursor matches the recovered data, and re-folding
    /// the tail from that cursor is idempotent (safe to replay).
    #[test]
    fn fjall_no_sync_tail_is_consistent_and_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let updates = stream();

        {
            let (_r, mut s) = open_no_sync(&path);
            fold(&mut s, &updates);
        } // drop flushes the journal

        // Recovered cursor names rev 7, and the data it names is all present.
        let (cursor, s) = open_no_sync(&path);
        assert_eq!(
            cursor.as_u64(),
            Some(7),
            "cursor recovered, never ahead of data"
        );
        assert_eq!(dump(&s), expected_state());
        drop(s);

        // Re-folding the tail (the last batch, revs 6..=7) from the recovered
        // cursor is idempotent — replaying the un-synced tail never corrupts state.
        let (_r, mut s) = open_no_sync(&path);
        fold(&mut s, &updates[5..]);
        assert_eq!(
            dump(&s),
            expected_state(),
            "re-folding the tail is idempotent"
        );
    }

    /// With sync on, every commit fsyncs — round-trip must still hold.
    #[test]
    fn fjall_sync_mode_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        {
            let (_r, mut s) = FjallSnapshot::open(
                &path,
                FjallConfig {
                    sync: true,
                    ..Default::default()
                },
            )
            .expect("open fjall sync");
            fold(&mut s, &stream());
        }
        let (cursor, s) = FjallSnapshot::open(
            &path,
            FjallConfig {
                sync: true,
                ..Default::default()
            },
        )
        .expect("reopen fjall sync");
        assert_eq!(cursor.as_u64(), Some(7));
        assert_eq!(dump(&s), expected_state());
    }
}

// ---------------------------------------------------------------------------
// RocksDbSnapshot — on-disk backend (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "rocksdb")]
mod rocksdb_backend {
    use super::*;
    use slipstream::{RocksDbConfig, RocksDbSnapshot};

    fn open_no_sync(path: &Path) -> (WatchCursor, RocksDbSnapshot) {
        RocksDbSnapshot::open(
            path,
            RocksDbConfig {
                sync: false,
                ..Default::default()
            },
        )
        .expect("open rocksdb")
    }

    #[test]
    fn rocksdb_round_trip() {
        check_round_trip(open_no_sync);
    }

    #[test]
    fn rocksdb_get_range() {
        check_get_range(open_no_sync);
    }

    #[test]
    fn rocksdb_for_each_in_range() {
        check_for_each_in_range(open_no_sync);
    }

    #[test]
    fn rocksdb_cursor_resume() {
        check_cursor_resume(open_no_sync);
    }

    #[test]
    fn rocksdb_pure_fold_property() {
        check_property_pure_fold(open_no_sync);
    }

    #[test]
    fn rocksdb_purge() {
        check_purge(open_no_sync);
    }

    #[test]
    fn rocksdb_empty_batch_advances_cursor() {
        check_empty_batch_advances_cursor(open_no_sync);
    }

    #[test]
    fn rocksdb_empty_value_round_trip() {
        check_empty_value_round_trip(open_no_sync);
    }

    fn import_rocksdb(
        artifact: &Path,
        dest: &Path,
    ) -> Result<(WatchCursor, RocksDbSnapshot), SnapshotError> {
        RocksDbSnapshot::import(
            artifact,
            dest,
            RocksDbConfig {
                sync: false,
                ..Default::default()
            },
        )
    }

    #[test]
    fn rocksdb_export_import_round_trip() {
        check_export_import_round_trip(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_resume_continues_fold() {
        check_import_resume_continues_fold(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_tampered_payload() {
        check_import_rejects_tampered_payload(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_missing_payload_file() {
        check_import_rejects_missing_payload_file(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_undeclared_extra_file() {
        check_import_rejects_undeclared_extra_file(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_wrong_backend() {
        check_import_rejects_wrong_backend(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_schema_version() {
        check_import_rejects_schema_version(open_no_sync, import_rocksdb);
    }

    // No `rocksdb_import_rejects_backend_version`: deliberately — the manifest's
    // backend_version is informational for RocksDB (the engine reads older
    // formats; its own open is the arbiter). See `RocksDbSnapshot::import`.

    #[test]
    fn rocksdb_import_rejects_cursor_mismatch() {
        check_import_rejects_cursor_mismatch(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_import_rejects_nonempty_dest() {
        check_import_rejects_nonempty_dest(open_no_sync, import_rocksdb);
    }

    #[test]
    fn rocksdb_export_rejects_nonempty_dest() {
        check_export_rejects_nonempty_dest(open_no_sync);
    }

    #[test]
    fn rocksdb_export_empty_store() {
        check_export_empty_store(open_no_sync, import_rocksdb);
    }

    /// `settle` (flush + wait-for-compact) must preserve the fold byte-for-byte.
    #[test]
    fn rocksdb_settle_preserves_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let (_r, mut s) = open_no_sync(&path);
        fold(&mut s, &stream());
        s.settle().expect("settle");
        assert_eq!(dump(&s), expected_state(), "state survives settle");
    }

    /// NO_SYNC crash-tail recovery. With sync off, commits reach the WAL but are
    /// not fsync'd; data and cursor still share one atomic WriteBatch, so whatever
    /// survives is mutually consistent. We can't deterministically simulate a
    /// power-loss (a clean drop flushes the WAL), so this asserts the load-bearing
    /// invariants: after reopen the recovered cursor matches the recovered data,
    /// and re-folding the tail from that cursor is idempotent (safe to replay).
    #[test]
    fn rocksdb_no_sync_tail_is_consistent_and_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let updates = stream();

        {
            let (_r, mut s) = open_no_sync(&path);
            fold(&mut s, &updates);
        } // drop flushes the WAL

        // Recovered cursor names rev 7, and the data it names is all present.
        let (cursor, s) = open_no_sync(&path);
        assert_eq!(
            cursor.as_u64(),
            Some(7),
            "cursor recovered, never ahead of data"
        );
        assert_eq!(dump(&s), expected_state());
        drop(s);

        // Re-folding the tail (the last batch, revs 6..=7) from the recovered
        // cursor is idempotent — replaying the un-synced tail never corrupts state.
        let (_r, mut s) = open_no_sync(&path);
        fold(&mut s, &updates[5..]);
        assert_eq!(
            dump(&s),
            expected_state(),
            "re-folding the tail is idempotent"
        );
    }

    /// `RocksDbReader::multi_get` is positionally aligned with its input and
    /// agrees with `get` for every key — hits, misses, deleted keys, and the
    /// empty-value CAS-tombstone shape — including duplicates and empty input.
    #[test]
    fn rocksdb_multi_get_matches_get() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        let (_r, mut s) = open_no_sync(&path);
        fold(&mut s, &stream());
        // One more apply: an empty-valued put (the CAS-tombstone shape).
        s.apply(&[put("lock", b"", 8)], &WatchCursor::from_u64(8))
            .expect("apply lock");
        let reader = s.reader();

        let keys = [
            "node.a",  // live
            "missing", // never written
            "node.b",  // deleted by the stream
            "lock",    // present with an empty value (CAS tombstone)
            "node.a",  // duplicate of a live key
        ];
        let got = reader.multi_get(keys.iter().copied()).expect("multi_get");

        assert_eq!(got.len(), keys.len(), "positionally aligned with input");
        for (key, entry) in keys.iter().zip(&got) {
            let single = reader.get(key).expect("get");
            assert_eq!(
                entry.as_ref().map(|e| (&e.key, &e.value)),
                single.as_ref().map(|e| (&e.key, &e.value)),
                "multi_get and get disagree for {key:?}"
            );
        }
        assert!(got[0].is_some(), "live key resolves");
        assert!(got[1].is_none(), "missing key is None");
        assert!(got[2].is_none(), "deleted key is None");
        assert!(
            got[3].as_ref().is_some_and(|e| e.value.is_empty()),
            "empty value is present, not confused with a delete"
        );

        let empty = reader.multi_get(std::iter::empty()).expect("empty input");
        assert!(empty.is_empty());
    }

    /// With sync on, every commit fsyncs the WAL — round-trip must still hold.
    #[test]
    fn rocksdb_sync_mode_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");
        {
            let (_r, mut s) = RocksDbSnapshot::open(
                &path,
                RocksDbConfig {
                    sync: true,
                    ..Default::default()
                },
            )
            .expect("open rocksdb sync");
            fold(&mut s, &stream());
        }
        let (cursor, s) = RocksDbSnapshot::open(
            &path,
            RocksDbConfig {
                sync: true,
                ..Default::default()
            },
        )
        .expect("reopen rocksdb sync");
        assert_eq!(cursor.as_u64(), Some(7));
        assert_eq!(dump(&s), expected_state());
    }
}
