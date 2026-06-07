//! On-disk [`SnapshotStore`] backed by [fjall](https://docs.rs/fjall) — for
//! consumers whose fold is too large to hold in RAM (e.g. routing at ~1B routes).
//!
//! fjall is a pure-Rust LSM-tree (no C toolchain), so the crate core stays
//! pure-Rust and this backend is opt-in behind `feature = "fjall"`. The same
//! engine is already used by `../objects`.
//!
//! ## How it honors the [`SnapshotStore`] invariants
//!
//! - **Atomic data + cursor.** Each [`apply`](SnapshotStore::apply) is a single
//!   fjall write batch: every put/delete *and* the resume cursor land under one
//!   sequence number and commit together. There is no window where the cursor
//!   names a revision whose data is missing.
//! - **Self-sufficient under NO_SYNC.** The durability mode is configurable. With
//!   sync off (the default — same cache philosophy as the append log's
//!   no-fsync-per-checkpoint path), a commit is not fsync'd; a power-loss crash can
//!   lose the un-synced *tail*. That is safe precisely because data and cursor are
//!   one atomic batch: whatever survived has its matching cursor, so on reopen the
//!   consumer resumes the watch from the recovered cursor and re-folds the tail
//!   from NATS. Set `sync = true` to fsync every commit.
//! - **Queryable.** [`get`](SnapshotStore::get) and [`range`](SnapshotStore::range)
//!   read straight from fjall's block-cached, `Slice`-backed storage — no full-DB
//!   deserialization — so a 1B-route consumer can build its serving index from a
//!   prefix scan.
//!
//! ## Threading
//!
//! fjall is synchronous; [`watch_applied`](crate::watch_applied) already offloads
//! [`apply`](SnapshotStore::apply) to a blocking task, and async callers querying
//! [`get`](SnapshotStore::get)/[`range`](SnapshotStore::range) should use
//! `spawn_blocking` likewise.

use std::path::Path;

use fjall::{Config, Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::kv::{KvEntry, KvUpdate, VersionToken, WatchCursor};
use crate::snapshot::{SnapshotError, SnapshotStore};

/// Partition holding the folded KV state: `key` → encoded `(version, value)`.
const DATA_PARTITION: &str = "data";
/// Partition holding fold metadata (just the resume cursor today).
const META_PARTITION: &str = "meta";
/// Key under [`META_PARTITION`] storing the resume cursor's raw version bytes.
const CURSOR_KEY: &[u8] = b"cursor";

/// Durability configuration for [`FjallSnapshot`].
///
/// Defaults to NO_SYNC (`sync: false`) — same cache philosophy as the append
/// log's no-fsync-per-checkpoint path.
#[derive(Debug, Clone, Copy, Default)]
pub struct FjallConfig {
    /// `fsync` every [`apply`](SnapshotStore::apply) commit when `true`. When
    /// `false` (the default), commits are not fsync'd (NO_SYNC): faster, and a
    /// tail lost to power loss is rebuilt by resuming the watch from the recovered
    /// cursor — the snapshot is a cache.
    pub sync: bool,
}

/// On-disk durable fold backed by fjall. See the [module docs](self).
pub struct FjallSnapshot {
    // fjall 3 renamed its types: the database root is `Database` (was `Keyspace`)
    // and each named partition is a `Keyspace` (was `PartitionHandle`).
    db: Database,
    data: Keyspace,
    meta: Keyspace,
    config: FjallConfig,
    cursor: WatchCursor,
}

impl FjallSnapshot {
    /// Open or resume the store at `path` with explicit durability config.
    ///
    /// `path` is a directory (fjall keyspace), created if absent. Returns the
    /// persisted resume cursor — [`WatchCursor::none`] when fresh — and the store.
    pub fn open(path: &Path, config: FjallConfig) -> Result<(WatchCursor, Self), SnapshotError> {
        std::fs::create_dir_all(path)?;
        let db = Database::open(Config::new(path)).map_err(map_fjall)?;
        let data = db
            .keyspace(DATA_PARTITION, KeyspaceCreateOptions::default)
            .map_err(map_fjall)?;
        let meta = db
            .keyspace(META_PARTITION, KeyspaceCreateOptions::default)
            .map_err(map_fjall)?;

        let cursor = match meta.get(CURSOR_KEY).map_err(map_fjall)? {
            Some(raw) => VersionToken::from_raw(&raw)
                .map(WatchCursor::from_version)
                .ok_or_else(|| {
                    SnapshotError::InvalidFormat(format!(
                        "stored cursor is {} bytes, exceeds version token capacity",
                        raw.len()
                    ))
                })?,
            None => WatchCursor::none(),
        };

        Ok((
            cursor.clone(),
            Self {
                db,
                data,
                meta,
                config,
                cursor,
            },
        ))
    }

    /// The most recently applied resume cursor.
    pub fn cursor(&self) -> &WatchCursor {
        &self.cursor
    }
}

impl SnapshotStore for FjallSnapshot {
    fn load(path: &Path) -> Result<(WatchCursor, Self), SnapshotError> {
        Self::open(path, FjallConfig::default())
    }

    fn apply(&mut self, batch: &[KvUpdate], cursor: &WatchCursor) -> Result<(), SnapshotError> {
        // One atomic batch: every data mutation AND the cursor commit under a
        // single sequence number. Either the whole fold step is durable or none of
        // it is — the cursor never outraces its data.
        let mut wb = self.db.batch().durability(self.durability());
        // One scratch buffer reused across the whole batch. `insert` converts its
        // value into fjall's owned `Slice` eagerly — it copies the bytes before
        // returning — so the buffer is free to be refilled for the next entry. That
        // turns N per-`Put` assembly allocations into one amortized allocation.
        let mut scratch = Vec::new();
        for update in batch {
            match update {
                KvUpdate::Put(entry) => {
                    encode_value_into(&mut scratch, &entry.value, &entry.version)?;
                    wb.insert(&self.data, entry.key.as_bytes(), scratch.as_slice());
                }
                KvUpdate::Delete { key, .. } | KvUpdate::Purge { key, .. } => {
                    wb.remove(&self.data, key.as_bytes());
                }
            }
        }
        // Cursor in the SAME batch as the data it names.
        wb.insert(&self.meta, CURSOR_KEY, cursor.version().as_bytes());
        wb.commit().map_err(map_fjall)?;

        self.cursor = cursor.clone();
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<KvEntry>, SnapshotError> {
        match self.data.get(key.as_bytes()).map_err(map_fjall)? {
            Some(raw) => Ok(Some(decode_entry(key, &raw)?)),
            None => Ok(None),
        }
    }

    fn range(&self, prefix: &str) -> Result<Vec<KvEntry>, SnapshotError> {
        // Collect the streaming scan — same decode path as `for_each_in_range`,
        // just buffered. fjall yields keys in ascending byte order, so the result
        // is already sorted (unlike the HashMap-backed append log).
        let mut out = Vec::new();
        self.for_each_in_range(prefix, |entry| {
            out.push(entry);
            Ok(())
        })?;
        Ok(out)
    }

    fn for_each_in_range(
        &self,
        prefix: &str,
        mut f: impl FnMut(KvEntry) -> Result<(), SnapshotError>,
    ) -> Result<(), SnapshotError> {
        // fjall's prefix iterator is lazy — entries are decoded and handed to `f`
        // one at a time, so a 1B-route consumer building a serving index never
        // holds more than a single `KvEntry` in memory at once.
        for guard in self.data.prefix(prefix.as_bytes()) {
            // fjall 3 yields a lazy `Guard` per entry; `into_inner` resolves it to
            // the `(key, value)` pair (loading the value, which keeps the scan lazy
            // for key-only iterations elsewhere).
            let (raw_key, raw_val) = guard.into_inner().map_err(map_fjall)?;
            let key = std::str::from_utf8(&raw_key).map_err(|e| {
                SnapshotError::InvalidFormat(format!("non-UTF-8 key in fjall store: {e}"))
            })?;
            f(decode_entry(key, &raw_val)?)?;
        }
        Ok(())
    }
}

impl FjallSnapshot {
    /// Per-commit durability: `fsync` when configured, otherwise NO_SYNC.
    fn durability(&self) -> Option<PersistMode> {
        if self.config.sync {
            Some(PersistMode::SyncAll)
        } else {
            // Explicit NO_SYNC: flush to OS buffers only — survives a process crash,
            // not a power loss, which is exactly the cache semantics the module docs
            // promise. Stating `Buffer` rather than `None` keeps that guarantee
            // independent of whatever default durability the keyspace was opened
            // with, so a future change to fjall's default can't silently make
            // `sync: false` durable (or weaker).
            Some(PersistMode::Buffer)
        }
    }
}

/// Encode a stored value as `[ver_len:u8][version bytes][value bytes]` into `buf`.
///
/// `buf` is cleared and refilled (its capacity is reused across a batch). The
/// version is length-prefixed raw bytes for the same reason the append-log format
/// uses it: a backend's token (NATS u64, FDB 10-byte versionstamp) must round-trip
/// intact.
///
/// `VersionToken` caps inline storage at 10 bytes, so the `u8` length prefix never
/// truncates today. Checking with `try_from` rather than casting surfaces a format
/// error instead of silently writing a wrong length — which would frame a record
/// `decode_entry` then mis-parses — if a future token ever widens past 255 bytes.
/// This mirrors `write_put_record` in `snapshot.rs`.
fn encode_value_into(
    buf: &mut Vec<u8>,
    value: &[u8],
    version: &VersionToken,
) -> Result<(), SnapshotError> {
    let vb = version.as_bytes();
    let ver_len = u8::try_from(vb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "version too long: {} bytes (max {})",
            vb.len(),
            u8::MAX
        ))
    })?;
    buf.clear();
    buf.reserve(1 + vb.len() + value.len());
    buf.push(ver_len);
    buf.extend_from_slice(vb);
    buf.extend_from_slice(value);
    Ok(())
}

/// Decode a `[ver_len:u8][version][value]` record back into a [`KvEntry`].
fn decode_entry(key: &str, raw: &[u8]) -> Result<KvEntry, SnapshotError> {
    let ver_len = *raw.first().ok_or_else(|| {
        SnapshotError::InvalidFormat("fjall value record is empty (no version length)".into())
    })? as usize;
    let value_off = 1 + ver_len;
    if raw.len() < value_off {
        return Err(SnapshotError::InvalidFormat(format!(
            "fjall value record truncated: need {value_off} bytes for version, have {}",
            raw.len()
        )));
    }
    let version = VersionToken::from_raw(&raw[1..value_off]).ok_or_else(|| {
        SnapshotError::InvalidFormat(format!(
            "version length {ver_len} exceeds version token capacity"
        ))
    })?;
    Ok(KvEntry {
        key: key.to_string(),
        value: raw[value_off..].to_vec(),
        version,
    })
}

/// Map a [`fjall::Error`] into the backend-agnostic [`SnapshotError`].
fn map_fjall(e: fjall::Error) -> SnapshotError {
    match e {
        // Surface I/O failures (disk full, permission denied, …) as a real
        // `io::Error` so the OS errno and the `#[source]` chain survive for
        // operators, instead of being flattened into an opaque backend string.
        fjall::Error::Io(io) => SnapshotError::Io(io),
        // Everything else keeps fjall's own variant name — its `Display` renders
        // as `FjallError: {variant:?}`, so `Poisoned` (a flush/commit failure
        // that should crash the app), journal recovery, decode, etc. stay legible
        // in logs without leaking the `fjall` type into this error enum.
        other => SnapshotError::Backend(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A 10-byte FDB versionstamp has no `u64` form; the length-prefixed value
    /// format must carry it intact. A `u64`-only field would flatten it to 0 and
    /// silently break every later CAS — so this is the load-bearing reason the
    /// record stores a length-prefixed token rather than a fixed 8 bytes.
    #[test]
    fn encode_decode_round_trips_fdb_versionstamp() {
        let vs = VersionToken::from_fdb_versionstamp(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let mut enc = Vec::new();
        encode_value_into(&mut enc, b"payload", &vs).expect("encode");
        let entry = decode_entry("k", &enc).expect("decode");

        assert_eq!(entry.version.as_bytes(), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert!(
            entry.version.as_u64().is_none(),
            "a 10-byte token has no u64 form — it must not be flattened"
        );
        assert_eq!(entry.value, b"payload");
    }

    /// An empty value (the CAS-tombstone shape) encodes to just the version prefix
    /// and decodes back to a present, empty-valued entry with its version intact.
    #[test]
    fn encode_decode_round_trips_empty_value() {
        let mut enc = Vec::new();
        encode_value_into(&mut enc, b"", &VersionToken::from_u64(7)).expect("encode");
        let entry = decode_entry("k", &enc).expect("decode");

        assert!(entry.value.is_empty());
        assert_eq!(entry.version.as_u64(), Some(7));
    }

    /// A zero-byte record has no version-length byte — corruption, not a valid
    /// record. It must surface as a recoverable `InvalidFormat`, never a panic.
    #[test]
    fn decode_entry_rejects_empty_record() {
        let err = decode_entry("k", &[]).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidFormat(_)),
            "empty record must be a format error, got {err:?}"
        );
    }

    /// A record that claims a longer version than its bytes provide is truncated
    /// on-disk corruption — reject it instead of reading past the buffer.
    #[test]
    fn decode_entry_rejects_truncated_version() {
        // Claims a 5-byte version, but only 2 bytes follow the length prefix.
        let raw = [5u8, 0xAA, 0xBB];
        let err = decode_entry("k", &raw).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidFormat(_)),
            "truncated version must be a format error, got {err:?}"
        );
    }

    /// A version length beyond `VersionToken`'s 10-byte capacity can't round-trip;
    /// `from_raw` rejects it and `decode_entry` maps that to `InvalidFormat` rather
    /// than silently truncating to a wrong (CAS-breaking) version.
    #[test]
    fn decode_entry_rejects_oversized_version() {
        // ver_len = 11 with 11 trailing bytes: passes the truncation check, then
        // trips the capacity check inside `VersionToken::from_raw`.
        let mut raw = vec![11u8];
        raw.extend_from_slice(&[0u8; 11]);
        let err = decode_entry("k", &raw).unwrap_err();
        assert!(
            matches!(err, SnapshotError::InvalidFormat(_)),
            "oversized version must be a format error, got {err:?}"
        );
    }

    /// A persisted cursor blob larger than the version-token capacity must surface
    /// as a recoverable `InvalidFormat` at `open`, not a panic or a silently
    /// truncated cursor that would resume the watch from the wrong position.
    #[test]
    fn open_rejects_corrupted_cursor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("store");

        {
            let (_c, store) =
                FjallSnapshot::open(&path, FjallConfig::default()).expect("initial open");
            // Write an 11-byte blob straight into the meta partition under the
            // cursor key, bypassing the apply path's bounded encoding.
            store
                .meta
                .insert(CURSOR_KEY, [0u8; 11])
                .expect("insert oversized cursor");
            store.db.persist(PersistMode::SyncAll).expect("persist");
        }

        // `FjallSnapshot` isn't `Debug`, so match the result rather than `unwrap_err`.
        match FjallSnapshot::open(&path, FjallConfig::default()) {
            Err(SnapshotError::InvalidFormat(_)) => {}
            Err(other) => panic!("expected InvalidFormat, got {other:?}"),
            Ok(_) => panic!("expected open to reject the oversized cursor"),
        }
    }
}
