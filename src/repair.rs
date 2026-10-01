//! Cursor-expiry repair: what [`watch_applied`](crate::watch_applied) does
//! when its resume cursor, or a live watch's frontier, has fallen out of the
//! log.
//!
//! The fallback full watch re-lists every key the bucket still holds. That
//! re-list can't deliver what retention already dropped, and what retention
//! drops depends on the bucket:
//!
//! - **Never evicts current values** (`discard: new`, no `max_age`): only
//!   superseded history and delete markers go. A key missing from the bucket
//!   was deleted, so the listing is the truth. The repair is the
//!   **key-listing diff** ([`ExpiryRepair::Relist`]): synthetic deletes for
//!   in-scope keys the bucket no longer lists, then the re-list.
//! - **Evicts current values** (`max_age`, per-message TTLs, `discard: old`
//!   under a limit): a key missing from the bucket may just have aged out, and
//!   writes made during the gap may have aged out too. NATS can't tell
//!   "deleted, marker evicted" from "aged out", so no listing diff can be
//!   repaired into a correct one. The repair is an **artifact restore**
//!   ([`ExpiryRepair::Restore`]): replace the in-scope fold with the newest
//!   published artifact — which carries every real delete, because its
//!   exporter folded them — and resume from its cursor.
//!
//! Correct means "every write minus every real delete", not "whatever NATS
//! still lists". On a bucket that evicts current values those differ, and only
//! a fold (here or in an artifact) holds the former.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::artifact::ExportManifest;
use crate::kv::{KvEntry, KvReader, KvUpdate, VersionToken, WatchCursor};
use crate::protocol::{restore_allowed, resume_window_ok};
use crate::snapshot::{SnapshotError, SnapshotStore};

/// How [`watch_applied`](crate::watch_applied) repairs the fold when its
/// cursor expires — at resume, or mid-watch when the live floor guard finds
/// retention overran the consumer. Both cases take the same path.
///
/// `Option<Arc<dyn KvReader>>` converts into this, so the pre-0.8 `reader`
/// argument keeps compiling: `None` is [`None`](Self::None), `Some(reader)` is
/// [`Relist`](Self::Relist).
pub enum ExpiryRepair<S> {
    /// No repair: fall back to the re-list alone. Keys deleted during the gap
    /// whose delete markers were evicted stay in the fold (a warning names the
    /// hazard), and on a bucket that evicts current values, gap writes that
    /// aged out are missed.
    None,
    /// The key-listing diff: synthetic deletes for in-scope keys the bucket
    /// no longer lists, then the re-list. Sound only when absence from the
    /// bucket means deleted, so it is REFUSED (the watch fails) when the
    /// watcher reports retention that evicts current values. On a backend that
    /// can't report retention, choosing this vouches that it doesn't.
    Relist(Arc<dyn KvReader>),
    /// Replace the in-scope fold with the newest published artifact and resume
    /// from its cursor. Sound for any bucket, as long as the artifact is ahead
    /// of the fold, covers the watch scope, and its cursor is still inside the
    /// log's retention window. When one of those fails there is no safe
    /// recovery, and the watch fails with an error (logged at `error`) rather
    /// than guess. Keep the export interval well under the bucket's `max_age`
    /// (or its `discard: old` turnover).
    ///
    /// Also used at a cursor-less start when the watcher reports that
    /// retention has already evicted current values: the bucket alone can no
    /// longer seed a complete fold.
    Restore(Arc<dyn RestoreSource<S>>),
    /// Decide by the bucket's live retention, read at the moment of expiry:
    /// [`Relist`](Self::Relist) when it never evicts current values,
    /// [`Restore`](Self::Restore) when it does or can't say.
    Auto {
        /// Lists live keys for the key-listing diff.
        reader: Arc<dyn KvReader>,
        /// Supplies the artifact for a restore.
        restore: Arc<dyn RestoreSource<S>>,
    },
}

impl<S> From<Option<Arc<dyn KvReader>>> for ExpiryRepair<S> {
    fn from(reader: Option<Arc<dyn KvReader>>) -> Self {
        match reader {
            Some(reader) => ExpiryRepair::Relist(reader),
            None => ExpiryRepair::None,
        }
    }
}

/// Where an [`ExpiryRepair::Restore`] gets its artifact: the newest one
/// published for this fleet, opened as a temporary fold of the consumer's own
/// backend. [`ArtifactRestore`](crate::ArtifactRestore) (feature `transport`)
/// implements it over an [`ArtifactTransport`](crate::ArtifactTransport).
#[async_trait]
pub trait RestoreSource<S: Send>: Send + Sync {
    /// The newest published artifact's manifest, without downloading the
    /// payload, so a stale, out-of-scope, or not-newer artifact is refused
    /// before its download.
    async fn latest(&self) -> Result<ExportManifest, SnapshotError>;

    /// Download, verify, and open the newest published artifact as a
    /// temporary fold. It may be newer than what [`latest`](Self::latest)
    /// returned; the caller re-checks it.
    async fn fetch(&self) -> Result<RestoredFold<S>, SnapshotError>;
}

/// A verified artifact opened as a temporary, read-only fold, plus anything
/// that must outlive it (its scratch directory). Dropping it drops the fold
/// first, then the guard.
pub struct RestoredFold<S> {
    manifest: ExportManifest,
    // Declaration order is drop order, and it is load-bearing: the fold (an
    // open LSM, possibly) must close before its guard deletes its directory.
    fold: S,
    guard: Option<Box<dyn Any + Send>>,
}

impl<S> RestoredFold<S> {
    /// Wrap a fold opened from the artifact described by `manifest`.
    pub fn new(manifest: ExportManifest, fold: S) -> Self {
        Self {
            manifest,
            fold,
            guard: None,
        }
    }

    /// Keep `guard` (e.g. the `TempDir` holding the fold's files) alive until
    /// after the fold is dropped.
    pub fn holding(mut self, guard: impl Any + Send) -> Self {
        self.guard = Some(Box::new(guard));
        self
    }

    /// The artifact's manifest: its cursor, backend, and scope.
    pub fn manifest(&self) -> &ExportManifest {
        &self.manifest
    }

    /// The artifact's fold.
    pub fn fold(&self) -> &S {
        &self.fold
    }
}

/// Rank for the restore guard: the cursor's revision, revisionless cursors 0.
pub(crate) fn cursor_rank(c: &WatchCursor) -> u64 {
    c.as_u64().unwrap_or(0)
}

/// Does an artifact exported under the `exporter` scope cover every key in
/// the `watcher` scope? Both are key-prefix lists (`[""]` is every key). A
/// watcher prefix is covered when some exporter prefix is a prefix of it.
/// Sound but conservative: a watcher prefix covered only by a union of
/// narrower exporter prefixes counts as uncovered.
pub(crate) fn scope_covers(exporter: &[String], watcher: &[String]) -> bool {
    watcher
        .iter()
        .all(|w| exporter.iter().any(|e| w.starts_with(e.as_str())))
}

/// Can `artifact` repair a fold at `local` whose cursor expired? Checks scope
/// coverage, then [`restore_allowed`] (ahead + fresh) when the log's first
/// retained revision is known, the ahead half alone when it isn't (the resume
/// that follows re-checks the window). `Err` is the operator-facing reason.
pub(crate) fn check_restore(
    artifact: &ExportManifest,
    local: &WatchCursor,
    first_revision: Option<u64>,
    watch_scope: &[String],
) -> Result<(), String> {
    match &artifact.scope {
        None => {
            return Err(format!(
                "the latest artifact (cursor {:?}) records no key scope — it was exported \
                 outside watch_applied or by a pre-scope build — so it can't be shown to cover \
                 watch scope {watch_scope:?}; publish a new export round",
                artifact.cursor
            ));
        }
        Some(exporter) if !scope_covers(exporter, watch_scope) => {
            return Err(format!(
                "the latest artifact covers scope {exporter:?}, which does not cover watch \
                 scope {watch_scope:?}"
            ));
        }
        Some(_) => {}
    }
    let (a, l) = (cursor_rank(&artifact.cursor), cursor_rank(local));
    if a <= l {
        return Err(format!(
            "the latest artifact (cursor {a}) is not ahead of the local fold (cursor {l}): \
             nothing newer to restore from; the export pipeline is behind this node"
        ));
    }
    if let Some(first) = first_revision
        && !restore_allowed(a, l, first)
    {
        debug_assert!(!resume_window_ok(a, first));
        return Err(format!(
            "the latest artifact (cursor {a}) is outside the log's retention window (first \
             retained revision {first}): what was evicted between them exists nowhere, so there \
             is no safe recovery. Exports must run well inside the bucket's max_age / discard \
             turnover"
        ));
    }
    Ok(())
}

/// One key of a restore: take the artifact's entry, or delete the key.
pub(crate) enum RestoreOp {
    Put(String),
    Delete(String),
}

impl RestoreOp {
    fn key(&self) -> &str {
        match self {
            RestoreOp::Put(k) | RestoreOp::Delete(k) => k,
        }
    }
}

/// The in-scope keys whose state differs between the `local` fold and the
/// `artifact` fold: what turns the local fold into the artifact's, scope by
/// scope. Keys only, in key order — values are read back in bounded chunks
/// ([`materialize`]), so a restore never holds the whole changed set's values
/// at once (the same discipline as the key-listing diff).
///
/// Wholesale replacement within the scope: an artifact key whose entry
/// differs is a `Put`, a local key the artifact lacks is a `Delete` (its
/// exporter folded a real delete). Out-of-scope keys are untouched on both
/// sides.
pub(crate) fn restore_diff<S: SnapshotStore>(
    local: &S,
    artifact: &S,
    prefixes: &[String],
) -> Result<Vec<RestoreOp>, SnapshotError> {
    let mut ops = Vec::new();
    for prefix in prefixes {
        artifact.for_each_in_range(prefix, |entry| {
            let same = local
                .get(&entry.key)?
                .is_some_and(|l| l.version == entry.version && l.value == entry.value);
            if !same {
                ops.push(RestoreOp::Put(entry.key));
            }
            Ok(())
        })?;
        local.for_each_in_range(prefix, |entry| {
            if artifact.get(&entry.key)?.is_none() {
                ops.push(RestoreOp::Delete(entry.key));
            }
            Ok(())
        })?;
    }
    // Overlapping prefixes visit a key more than once; a key is a Put or a
    // Delete, never both (Put iff the artifact has it).
    ops.sort_unstable_by(|a, b| a.key().cmp(b.key()));
    ops.dedup_by(|a, b| a.key() == b.key());
    Ok(ops)
}

/// Read one chunk of a restore's values back from the artifact fold. Deletes
/// carry the unknown version: like the key-listing diff's synthetic deletes,
/// they are a state correction, not a log entry, and never move a cursor.
pub(crate) fn materialize<S: SnapshotStore>(
    artifact: &S,
    ops: Vec<RestoreOp>,
) -> Result<Vec<KvUpdate>, SnapshotError> {
    ops.into_iter()
        .map(|op| match op {
            RestoreOp::Put(key) => match artifact.get(&key)? {
                Some(entry @ KvEntry { .. }) => Ok(KvUpdate::Put(entry)),
                None => Err(SnapshotError::Backend(format!(
                    "restored artifact fold lost key {key:?} mid-restore"
                ))),
            },
            RestoreOp::Delete(key) => Ok(KvUpdate::Delete {
                key,
                version: VersionToken::unknown(),
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::AppendLogSnapshot;

    fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
        KvUpdate::Put(KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            version: VersionToken::from_u64(rev),
        })
    }

    fn fold(
        dir: &tempfile::TempDir,
        name: &str,
        updates: &[KvUpdate],
        at: u64,
    ) -> AppendLogSnapshot {
        let (_c, mut s) = AppendLogSnapshot::open(&dir.path().join(name), u64::MAX).unwrap();
        s.apply(updates, &WatchCursor::from_u64(at)).unwrap();
        s
    }

    fn manifest(cursor: u64, scope: Option<&[&str]>) -> ExportManifest {
        ExportManifest {
            schema_version: crate::artifact::ARTIFACT_SCHEMA_VERSION,
            backend: "append-log".into(),
            backend_version: "2".into(),
            cursor: WatchCursor::from_u64(cursor),
            created_at_unix: 0,
            files: vec![],
            scope: scope.map(|s| s.iter().map(|p| p.to_string()).collect()),
        }
    }

    #[test]
    fn scope_coverage() {
        let s = |v: &[&str]| v.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        assert!(
            scope_covers(&s(&[""]), &s(&["node.", "edge."])),
            "All covers anything"
        );
        assert!(
            scope_covers(&s(&["node."]), &s(&["node.us."])),
            "wider prefix covers narrower"
        );
        assert!(
            !scope_covers(&s(&["node.us."]), &s(&["node."])),
            "narrower never covers wider"
        );
        assert!(
            !scope_covers(&s(&["node."]), &s(&[""])),
            "a prefix never covers All"
        );
        assert!(
            !scope_covers(&s(&["node."]), &s(&["node.", "edge."])),
            "every watcher prefix must be covered"
        );
        assert!(
            scope_covers(&s(&["a."]), &s(&[])),
            "an empty watch scope is vacuously covered"
        );
    }

    #[test]
    fn check_restore_refusals() {
        let all = vec![String::new()];
        let local = WatchCursor::from_u64(3);
        // Ahead + fresh + covering: accepted.
        check_restore(&manifest(9, Some(&[""])), &local, Some(8), &all).unwrap();
        // Unknown retention: ahead alone suffices here (the resume re-checks).
        check_restore(&manifest(9, Some(&[""])), &local, None, &all).unwrap();

        let err = check_restore(&manifest(9, None), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("no key scope"), "{err}");
        let err = check_restore(&manifest(9, Some(&["node."])), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("does not cover"), "{err}");
        let err = check_restore(&manifest(3, Some(&[""])), &local, None, &all).unwrap_err();
        assert!(err.contains("not ahead"), "{err}");
        let err = check_restore(&manifest(6, Some(&[""])), &local, Some(8), &all).unwrap_err();
        assert!(err.contains("outside the log's retention window"), "{err}");
    }

    /// The diff turns the local in-scope fold into the artifact's: changed and
    /// new keys are Puts, keys the artifact lacks are Deletes, identical
    /// entries and out-of-scope keys are left alone.
    #[test]
    fn diff_is_wholesale_replacement_within_scope() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = fold(
            &dir,
            "local",
            &[
                put("node.same", b"1", 1),
                put("node.changed", b"old", 2),
                put("node.gone", b"x", 3),
                put("other.local", b"keep", 4),
            ],
            4,
        );
        let artifact = fold(
            &dir,
            "artifact",
            &[
                put("node.same", b"1", 1),
                put("node.changed", b"new", 7),
                put("node.new", b"gap-write", 8),
                put("other.remote", b"ignored", 9),
            ],
            9,
        );
        let ops = restore_diff(&local, &artifact, &["node.".to_string()]).unwrap();
        let got: Vec<(String, bool)> = ops
            .iter()
            .map(|o| (o.key().to_string(), matches!(o, RestoreOp::Put(_))))
            .collect();
        assert_eq!(
            got,
            vec![
                ("node.changed".into(), true),
                ("node.gone".into(), false),
                ("node.new".into(), true),
            ]
        );

        let updates = materialize(&artifact, ops).unwrap();
        match &updates[0] {
            KvUpdate::Put(e) => {
                assert_eq!(e.value, b"new");
                assert_eq!(
                    e.version.as_u64(),
                    Some(7),
                    "restored entries keep their revision"
                );
            }
            other => panic!("expected put, got {other:?}"),
        }
        assert!(
            matches!(&updates[1], KvUpdate::Delete { version, .. } if version.is_unknown()),
            "restore deletes never carry a revision"
        );
    }

    #[test]
    fn diff_dedups_overlapping_prefixes() {
        let dir = tempfile::TempDir::new().unwrap();
        let local = fold(&dir, "local", &[put("node.a", b"1", 1)], 1);
        let artifact = fold(&dir, "artifact", &[put("node.b", b"2", 2)], 2);
        let ops = restore_diff(&local, &artifact, &["node.".into(), "node".into()]).unwrap();
        assert_eq!(ops.len(), 2, "each key once despite two covering prefixes");
    }
}
