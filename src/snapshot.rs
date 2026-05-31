//! Append-only snapshot log for KV state.
//!
//! Streams [`KvUpdate`]s to disk as they arrive via [`SnapshotWriter`],
//! with no in-memory state beyond a file handle and byte counter.
//! On startup, [`load`] replays the log to reconstruct entries + cursor,
//! then compacts the file.
//!
//! ## File format
//!
//! 6-byte header followed by a sequence of CRC'd records:
//!
//! ```text
//! Header:  b"PGSS" ++ version:u16le
//! Record:  crc32:u32le ++ type:u8 ++ payload (varies by type)
//!
//! Put:     key_len:u16le ++ key ++ value_len:u32le ++ value ++ ver_len:u8 ++ version
//! Delete:  key_len:u16le ++ key ++ ver_len:u8 ++ version
//! Cursor:  cur_len:u8 ++ cursor
//! ```
//!
//! `version` is the raw [`VersionToken`] bytes (≤10), not a fixed u64 — a
//! 10-byte FDB versionstamp round-trips intact, where a `u64`-only field would
//! silently flatten it to 0 and break every later CAS.
//!
//! Used by edge/tunnel services to survive restarts without a full
//! NATS KV scan. The snapshot is a cache — delete it and the system
//! falls back to `load_all()`.
//!
//! ## Blocking I/O
//!
//! Every function in this module performs **synchronous** file I/O and is
//! deliberately runtime-agnostic — it pulls in no async runtime so a caller can
//! place it on whichever executor (or none) it wants. The flip side is that none
//! of these calls may run directly on an async executor thread: [`load`],
//! [`SnapshotWriter::compact`], and friends `read`/`write`/`rename` whole files
//! and will stall the reactor if awaited inline. Async callers must offload them
//! with `tokio::task::spawn_blocking` (or the equivalent).
//! [`SnapshotWriter::compact`] is the heaviest — it reads and rewrites the entire
//! log — but the rule is the same for all of them.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::kv::{KvEntry, KvUpdate, VersionToken, WatchCursor};

const MAGIC: &[u8; 4] = b"PGSS";
// v2: Put/Delete records store the version as length-prefixed raw bytes instead
// of a fixed 8-byte u64, so non-u64 tokens (e.g. FDB versionstamps) survive a
// round-trip. v1 files are rejected by `replay_log`; the snapshot is a cache, so
// a rejected file just triggers a fresh NATS scan + watch replay.
const FORMAT_VERSION: u16 = 2;
const HEADER_LEN: usize = 6;

const REC_PUT: u8 = 0x01;
const REC_DELETE: u8 = 0x02;
const REC_CURSOR: u8 = 0x03;

// Minimum complete record sizes (CRC + type + minimum payload)
const MIN_CURSOR_RECORD: usize = 4 + 1 + 1; // 6

/// Errors from snapshot operations.
///
/// Uses `thiserror` to match [`crate::KvError`]; unlike `KvError` (which is
/// `Clone` and so flattens its causes to strings), snapshot errors are observed
/// by a single caller, so `Io` keeps its `#[source]` chain via `#[from]`.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid snapshot format: {0}")]
    InvalidFormat(String),
    #[error("snapshot corrupted (CRC mismatch)")]
    Corrupted,
}

/// Result of loading a snapshot from disk.
#[derive(Debug)]
pub struct Snapshot {
    /// Watch cursor at the time of the last checkpoint.
    pub cursor: WatchCursor,
    /// Live KV entries keyed by name (deduplicated, deletes applied).
    pub entries: HashMap<String, KvEntry>,
}

impl Snapshot {
    /// Keys present in this snapshot but absent from a fresh scan result.
    ///
    /// After a cursor-expired fallback to full `watch_all()`, callers should
    /// compare the snapshot against the live key set and emit synthetic
    /// `Delete` events for stale keys to ensure convergence.
    pub fn stale_keys<'a, I>(&'a self, current_keys: I) -> Vec<&'a str>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let current: HashSet<&str> = current_keys.into_iter().collect();
        self.entries
            .keys()
            .filter(|k| !current.contains(k.as_str()))
            .map(|k| k.as_str())
            .collect()
    }
}

/// Append-only snapshot writer.
///
/// Streams [`KvUpdate`] records to disk via a buffered writer. No
/// in-memory state beyond the file handle and a byte counter for
/// compaction triggering.
///
/// Compacts automatically when bytes written since last compaction
/// exceeds `compact_threshold`. Compaction replays the log into a
/// transient [`HashMap`], rewrites via tempfile+rename, and reopens
/// for append.
pub struct SnapshotWriter {
    path: PathBuf,
    // `None` only after a `compact()` rewrote the file (atomic rename succeeded)
    // but failed to reopen it for append. The old handle then pointed at the
    // renamed-away inode; rather than keep writing into that orphan — silently
    // losing every later record on close — we drop it and poison the writer so
    // `write_update`/`checkpoint`/`flush` return an error until reconstructed.
    writer: Option<io::BufWriter<File>>,
    bytes_since_compact: u64,
    compact_threshold: u64,
}

impl SnapshotWriter {
    /// Open or create a snapshot log.
    ///
    /// If the file doesn't exist, writes the header. If it exists, opens
    /// for append. `compact_threshold` controls how many bytes accumulate
    /// before an automatic compaction.
    pub fn open(path: &Path, compact_threshold: u64) -> Result<Self, SnapshotError> {
        // Open first, then size the file from its own handle. The previous
        // `path.exists()` + `fs::metadata(path)` pair issued two extra `stat(2)`
        // syscalls and left a TOCTOU window (the file could be created or removed
        // between the checks). One `open(2)` plus a handle `metadata()` is both
        // fewer syscalls and race-free — the length we read is the length of the
        // file we hold open.
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let existing_len = file.metadata()?.len();

        let mut writer = io::BufWriter::new(file);

        // A file with at least a full header is an existing log: everything past
        // the 6-byte header counts toward the compaction threshold. Anything
        // shorter (brand-new, or a header torn by a crash mid-create) gets a
        // fresh header written before the first record.
        let bytes_since_compact = if existing_len >= HEADER_LEN as u64 {
            existing_len - HEADER_LEN as u64
        } else {
            writer.write_all(MAGIC)?;
            writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
            writer.flush()?;
            0
        };

        Ok(Self {
            path: path.to_path_buf(),
            writer: Some(writer),
            bytes_since_compact,
            compact_threshold,
        })
    }

    /// Borrow the underlying writer, or fail if a prior `compact()` poisoned it
    /// (see the `writer` field doc). Keeps the orphaned-fd failure mode a
    /// surfaced error rather than a silent data loss.
    fn writer(&mut self) -> Result<&mut io::BufWriter<File>, SnapshotError> {
        self.writer.as_mut().ok_or_else(|| {
            SnapshotError::Io(io::Error::other(
                "snapshot writer poisoned: a prior compact() failed to reopen the log for append",
            ))
        })
    }

    /// Write a single [`KvUpdate`] record to the log.
    ///
    /// Buffered — does not flush to disk until [`checkpoint`](Self::checkpoint).
    #[must_use = "I/O errors mean the write was lost"]
    pub fn write_update(&mut self, update: &KvUpdate) -> Result<(), SnapshotError> {
        let w = self.writer()?;
        let bytes = match update {
            KvUpdate::Put(entry) => write_put_record(w, &entry.key, &entry.value, &entry.version)?,
            KvUpdate::Delete { key, version } | KvUpdate::Purge { key, version } => {
                write_delete_record(w, key, version)?
            }
        };
        self.bytes_since_compact += bytes as u64;
        Ok(())
    }

    /// Write a cursor checkpoint and flush the buffer to the OS.
    ///
    /// The flush is a `write(2)` into the page cache — it survives a process
    /// crash, but NOT power loss: there is no `fsync` on this path. The durable
    /// `sync_all` happens in [`compact`](Self::compact). That's deliberate — the
    /// snapshot is a cache backed by NATS and checkpoints are frequent, so an
    /// fsync per checkpoint isn't worth its latency; a tail lost to power loss is
    /// rebuilt from a NATS scan + watch replay.
    ///
    /// Returns `true` when the log has grown past the compaction threshold
    /// and the caller should run [`compact`](Self::compact). Separating
    /// the check from the I/O lets async callers offload compaction to a
    /// blocking task instead of stalling the executor.
    #[must_use = "returns true when compaction is needed"]
    pub fn checkpoint(&mut self, cursor: &WatchCursor) -> Result<bool, SnapshotError> {
        let w = self.writer()?;
        let bytes = write_cursor_record(w, cursor)?;
        w.flush()?;
        self.bytes_since_compact += bytes as u64;
        Ok(self.bytes_since_compact > self.compact_threshold)
    }

    /// Flush the buffer to disk without writing a cursor record.
    #[must_use = "I/O errors mean the flush failed"]
    pub fn flush(&mut self) -> Result<(), SnapshotError> {
        self.writer()?.flush()?;
        Ok(())
    }

    /// Compact the snapshot log: replay, deduplicate, and rewrite.
    ///
    /// Performs synchronous file I/O. In async contexts, run via
    /// `spawn_blocking` to avoid stalling the executor.
    #[must_use = "compaction errors leave the log uncompacted"]
    pub fn compact(&mut self) -> Result<(), SnapshotError> {
        // Flush buffered records to the file before reading it back. Records
        // written since the last `checkpoint()` still sit in the BufWriter; if we
        // read+rewrite without flushing, they're excluded from the replay, and
        // the old BufWriter then flushes them on drop into the inode we just
        // renamed away — silently losing them. Flushing first makes `compact()`
        // safe regardless of whether a `checkpoint()` preceded it.
        self.writer()?.flush()?;
        let data = fs::read(&self.path)?;
        let (entries, cursor, _already_compact) = replay_log(&data)?;
        compact_to_file(&self.path, &entries, &cursor)?;

        // The rename in `compact_to_file` has already replaced the file, so the
        // current handle now points at the orphaned (renamed-away) inode. Drop it
        // *before* reopening: if the reopen fails, `writer` stays `None` and the
        // writer is poisoned (see field doc), turning a would-be silent loss into
        // a surfaced error on the next write.
        self.writer = None;
        let file = OpenOptions::new().append(true).open(&self.path)?;
        self.writer = Some(io::BufWriter::new(file));
        self.bytes_since_compact = 0;

        Ok(())
    }
}

/// Load a snapshot from disk.
///
/// Replays the append log, deduplicates (last write wins per key),
/// compacts the file, and returns the live entries + cursor.
///
/// Returns `Ok(None)` if the file doesn't exist or contains no entries.
pub fn load(path: &Path) -> Result<Option<Snapshot>, SnapshotError> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SnapshotError::Io(e)),
    };

    let (entries, cursor, already_compact) = replay_log(&data)?;

    if entries.is_empty() && cursor.is_none() {
        return Ok(None);
    }

    // Skip the rewrite when the log is already in compact form (no duplicate
    // keys, no deletes, no truncated tail). Avoids a full read+write on every
    // startup after the first. A truncated tail forces a rewrite even when the
    // surviving records are unique: leaving the partial record on disk would
    // let the next append land after it, corrupting the log.
    if !already_compact {
        compact_to_file(path, &entries, &cursor)?;
    }

    Ok(Some(Snapshot { cursor, entries }))
}

// ---------------------------------------------------------------------------
// Internal: log replay
// ---------------------------------------------------------------------------

/// Replay the append log into the live key set.
///
/// Records borrow directly from `data` — keys and values are not allocated
/// until the surviving set is materialized at the end, so overwritten or
/// deleted records cost nothing. Returns the entries, the latest cursor, and
/// whether the log was already compact (no duplicate keys, no deletes, no
/// truncated tail) so callers can skip a redundant rewrite.
fn replay_log(data: &[u8]) -> Result<(HashMap<String, KvEntry>, WatchCursor, bool), SnapshotError> {
    if data.len() < HEADER_LEN {
        return Err(SnapshotError::InvalidFormat("file too short".into()));
    }
    if &data[0..4] != MAGIC {
        return Err(SnapshotError::InvalidFormat("bad magic".into()));
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != FORMAT_VERSION {
        return Err(SnapshotError::InvalidFormat(format!(
            "unsupported version {version}"
        )));
    }

    // Upper-bound the working set by data size to avoid rehashing on large
    // snapshots, capped so a tiny-record file can't request a huge table.
    let estimated = (data.len() - HEADER_LEN) / 30;
    let mut live: HashMap<&str, (&[u8], VersionToken)> =
        HashMap::with_capacity(estimated.min(4096));
    let mut cursor = WatchCursor::none();
    let mut pos = HEADER_LEN;

    // The log is compact only if every record contributed a unique surviving
    // entry and we consumed the file cleanly. Any overwrite, delete, or
    // truncated tail means a rewrite would shrink or repair the file.
    let mut redundant = false;
    let mut clean_eof = true;

    while pos < data.len() {
        match parse_record(&data[pos..]) {
            Ok((record, consumed)) => {
                match record {
                    Record::Put {
                        key,
                        value,
                        version,
                    } => {
                        if live.insert(key, (value, version)).is_some() {
                            redundant = true;
                        }
                    }
                    Record::Delete { key } => {
                        live.remove(key);
                        redundant = true;
                    }
                    Record::Cursor(c) => {
                        // Each new cursor record supersedes the previous one, so
                        // any earlier cursor on disk is now dead weight. Mark the
                        // log redundant so `load()` rewrites it — otherwise a
                        // service that checkpoints frequently accumulates one
                        // cursor record per checkpoint and `already_compact` stays
                        // true (entries are unique, EOF is clean), leaving the
                        // bloat in place until the writer's own threshold fires.
                        if !cursor.is_none() {
                            redundant = true;
                        }
                        cursor = c;
                    }
                }
                pos += consumed;
            }
            Err(RecordError::Truncated) => {
                // KNOWN LIMITATION: a record's length is read from its (CRC-but-
                // not-yet-verified) length fields, so corruption that inflates a
                // key_len/value_len makes the record look like it runs past EOF —
                // indistinguishable here from a genuine torn final write. Both
                // land as `Truncated`, so we stop and silently drop everything
                // after this point. Detecting it would need a framed, separately-
                // checksummed length (a format change). Acceptable because the
                // snapshot is a cache: a short read just triggers a fuller NATS
                // scan + watch replay, never data loss of record.
                clean_eof = false;
                break;
            }
            Err(RecordError::CrcMismatch { consumed }) => {
                // Near EOF → crash recovery (partial final write).
                // Otherwise → mid-file corruption.
                if pos + consumed >= data.len() || data.len() - (pos + consumed) < MIN_CURSOR_RECORD
                {
                    clean_eof = false;
                    break;
                }
                return Err(SnapshotError::Corrupted);
            }
            Err(RecordError::Invalid(msg)) => {
                return Err(SnapshotError::InvalidFormat(msg));
            }
        }
    }

    // Materialize owned entries for the survivors only — one key + one value
    // allocation per live key, instead of per record.
    let mut entries: HashMap<String, KvEntry> = HashMap::with_capacity(live.len());
    for (key, (value, version)) in live {
        let key = key.to_string();
        entries.insert(
            key.clone(),
            KvEntry {
                key,
                value: value.to_vec(),
                version,
            },
        );
    }

    let already_compact = !redundant && clean_eof;
    Ok((entries, cursor, already_compact))
}

enum Record<'a> {
    Put {
        key: &'a str,
        value: &'a [u8],
        version: VersionToken,
    },
    Delete {
        key: &'a str,
    },
    Cursor(WatchCursor),
}

enum RecordError {
    Truncated,
    CrcMismatch { consumed: usize },
    Invalid(String),
}

fn parse_record(data: &[u8]) -> Result<(Record<'_>, usize), RecordError> {
    // Need at least CRC (4) + type (1)
    if data.len() < 5 {
        return Err(RecordError::Truncated);
    }

    let stored_crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    match data[4] {
        REC_PUT => parse_put(data, stored_crc),
        REC_DELETE => parse_delete(data, stored_crc),
        REC_CURSOR => parse_cursor(data, stored_crc),
        other => Err(RecordError::Invalid(format!(
            "unknown record type: {other:#x}"
        ))),
    }
}

fn parse_put(data: &[u8], stored_crc: u32) -> Result<(Record<'_>, usize), RecordError> {
    // CRC(4) + type(1) + key_len(2) = 7 minimum to read key_len
    if data.len() < 7 {
        return Err(RecordError::Truncated);
    }
    let key_len = u16::from_le_bytes([data[5], data[6]]) as usize;
    let vl_off = 7 + key_len;

    if data.len() < vl_off + 4 {
        return Err(RecordError::Truncated);
    }
    let value_len = u32::from_le_bytes([
        data[vl_off],
        data[vl_off + 1],
        data[vl_off + 2],
        data[vl_off + 3],
    ]) as usize;

    // ver_len byte sits right after the value.
    let ver_len_off = vl_off + 4 + value_len;
    if data.len() < ver_len_off + 1 {
        return Err(RecordError::Truncated);
    }
    let ver_len = data[ver_len_off] as usize;
    // A version token holds at most 10 bytes inline. A larger length means a
    // corrupt or incompatible record — reject it rather than letting it reach
    // `VersionToken::from_raw`, which would panic.
    if ver_len > 10 {
        return Err(RecordError::Invalid(format!(
            "version length {ver_len} exceeds max version token size (10)"
        )));
    }

    let total = ver_len_off + 1 + ver_len;
    if data.len() < total {
        return Err(RecordError::Truncated);
    }

    let computed = crc32fast::hash(&data[4..total]);
    if computed != stored_crc {
        return Err(RecordError::CrcMismatch { consumed: total });
    }

    let key = std::str::from_utf8(&data[7..7 + key_len])
        .map_err(|e| RecordError::Invalid(format!("invalid UTF-8 key: {e}")))?;
    let value = &data[vl_off + 4..vl_off + 4 + value_len];
    // The `ver_len > 10` check above bounds this to the inline capacity, so
    // `from_raw` always returns `Some` here; the guard makes that explicit.
    let version = VersionToken::from_raw(&data[ver_len_off + 1..total]).ok_or_else(|| {
        RecordError::Invalid(format!(
            "version length {ver_len} exceeds max version token size (10)"
        ))
    })?;

    Ok((
        Record::Put {
            key,
            value,
            version,
        },
        total,
    ))
}

fn parse_delete(data: &[u8], stored_crc: u32) -> Result<(Record<'_>, usize), RecordError> {
    if data.len() < 7 {
        return Err(RecordError::Truncated);
    }
    let key_len = u16::from_le_bytes([data[5], data[6]]) as usize;
    let ver_len_off = 7 + key_len;
    if data.len() < ver_len_off + 1 {
        return Err(RecordError::Truncated);
    }
    let ver_len = data[ver_len_off] as usize;
    // See `parse_put`: reject oversized version lengths before `from_raw`.
    if ver_len > 10 {
        return Err(RecordError::Invalid(format!(
            "version length {ver_len} exceeds max version token size (10)"
        )));
    }
    let total = ver_len_off + 1 + ver_len;

    if data.len() < total {
        return Err(RecordError::Truncated);
    }

    let computed = crc32fast::hash(&data[4..total]);
    if computed != stored_crc {
        return Err(RecordError::CrcMismatch { consumed: total });
    }

    let key = std::str::from_utf8(&data[7..7 + key_len])
        .map_err(|e| RecordError::Invalid(format!("invalid UTF-8 key: {e}")))?;
    // The delete record's version is written for format symmetry but unused on
    // replay (a delete just removes the key from the live set).

    Ok((Record::Delete { key }, total))
}

fn parse_cursor(data: &[u8], stored_crc: u32) -> Result<(Record<'_>, usize), RecordError> {
    if data.len() < 6 {
        return Err(RecordError::Truncated);
    }
    let cursor_len = data[5] as usize;
    // A version token holds at most 10 bytes inline. A larger length means a
    // corrupt or incompatible record — reject it rather than letting it reach
    // `VersionToken::from_raw`, which would panic.
    if cursor_len > 10 {
        return Err(RecordError::Invalid(format!(
            "cursor length {cursor_len} exceeds max version token size (10)"
        )));
    }
    let total = 6 + cursor_len;

    if data.len() < total {
        return Err(RecordError::Truncated);
    }

    let computed = crc32fast::hash(&data[4..total]);
    if computed != stored_crc {
        return Err(RecordError::CrcMismatch { consumed: total });
    }

    // The `cursor_len > 10` check above bounds this to the inline capacity, so
    // `from_raw` always returns `Some` here; the guard makes that explicit.
    let version = VersionToken::from_raw(&data[6..total]).ok_or_else(|| {
        RecordError::Invalid(format!(
            "cursor length {cursor_len} exceeds max version token size (10)"
        ))
    })?;

    Ok((Record::Cursor(WatchCursor::from_version(version)), total))
}

// ---------------------------------------------------------------------------
// Internal: record writing (incremental CRC, no allocations)
// ---------------------------------------------------------------------------

fn write_put_record(
    w: &mut impl Write,
    key: &str,
    value: &[u8],
    version: &VersionToken,
) -> Result<usize, SnapshotError> {
    let kb = key.as_bytes();
    let vb = version.as_bytes();

    // The wire format encodes key length as u16 and value length as u32.
    // Reject anything that would truncate on cast — a silent truncation here
    // produces a record whose CRC covers the full bytes but whose stored length
    // is wrong, which the reader can only interpret as mid-file corruption.
    let key_len = u16::try_from(kb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "key too long: {} bytes (max {})",
            kb.len(),
            u16::MAX
        ))
    })?;
    let value_len = u32::try_from(value.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "value too long: {} bytes (max {})",
            value.len(),
            u32::MAX
        ))
    })?;
    // The version is stored as length-prefixed raw bytes so any backend's token
    // (NATS u64, FDB 10-byte versionstamp) round-trips intact. `VersionToken`
    // caps inline storage at 10 bytes, so this `u8` length never truncates today;
    // checking surfaces a format error rather than corrupting the frame if a
    // future token widens past 255 bytes.
    let ver_len = u8::try_from(vb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "version too long: {} bytes (max {})",
            vb.len(),
            u8::MAX
        ))
    })?;

    let mut h = crc32fast::Hasher::new();
    h.update(&[REC_PUT]);
    h.update(&key_len.to_le_bytes());
    h.update(kb);
    h.update(&value_len.to_le_bytes());
    h.update(value);
    h.update(&[ver_len]);
    h.update(vb);
    let crc = h.finalize();

    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&[REC_PUT])?;
    w.write_all(&key_len.to_le_bytes())?;
    w.write_all(kb)?;
    w.write_all(&value_len.to_le_bytes())?;
    w.write_all(value)?;
    w.write_all(&[ver_len])?;
    w.write_all(vb)?;

    Ok(4 + 1 + 2 + kb.len() + 4 + value.len() + 1 + vb.len())
}

fn write_delete_record(
    w: &mut impl Write,
    key: &str,
    version: &VersionToken,
) -> Result<usize, SnapshotError> {
    let kb = key.as_bytes();
    let vb = version.as_bytes();

    let key_len = u16::try_from(kb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "key too long: {} bytes (max {})",
            kb.len(),
            u16::MAX
        ))
    })?;
    // Length-prefixed version, matching `write_put_record`. See its comment for
    // why this is bytes rather than a fixed u64.
    let ver_len = u8::try_from(vb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "version too long: {} bytes (max {})",
            vb.len(),
            u8::MAX
        ))
    })?;

    let mut h = crc32fast::Hasher::new();
    h.update(&[REC_DELETE]);
    h.update(&key_len.to_le_bytes());
    h.update(kb);
    h.update(&[ver_len]);
    h.update(vb);
    let crc = h.finalize();

    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&[REC_DELETE])?;
    w.write_all(&key_len.to_le_bytes())?;
    w.write_all(kb)?;
    w.write_all(&[ver_len])?;
    w.write_all(vb)?;

    Ok(4 + 1 + 2 + kb.len() + 1 + vb.len())
}

fn write_cursor_record(w: &mut impl Write, cursor: &WatchCursor) -> Result<usize, SnapshotError> {
    let cb = cursor.version().as_bytes();
    // The record encodes the cursor length as a single byte. `VersionToken`
    // caps inline storage at 10 bytes, so this never trips today — but checking
    // here rather than casting means a future backend that widens the token
    // surfaces a format error instead of silently truncating the length prefix
    // (which the reader would then mis-frame as corruption).
    let cb_len = u8::try_from(cb.len()).map_err(|_| {
        SnapshotError::InvalidFormat(format!(
            "cursor too long: {} bytes (max {})",
            cb.len(),
            u8::MAX
        ))
    })?;

    let mut h = crc32fast::Hasher::new();
    h.update(&[REC_CURSOR]);
    h.update(&[cb_len]);
    h.update(cb);
    let crc = h.finalize();

    w.write_all(&crc.to_le_bytes())?;
    w.write_all(&[REC_CURSOR])?;
    w.write_all(&[cb_len])?;
    w.write_all(cb)?;

    Ok(4 + 1 + 1 + cb.len())
}

// ---------------------------------------------------------------------------
// Internal: compaction
// ---------------------------------------------------------------------------

fn compact_to_file(
    path: &Path,
    entries: &HashMap<String, KvEntry>,
    cursor: &WatchCursor,
) -> Result<(), SnapshotError> {
    // The tempfile must live in the same directory as the target so the final
    // `persist` is an atomic same-filesystem rename. Falling back to "." here
    // would silently place it in the cwd, and a cross-filesystem rename then
    // fails with EXDEV — a hard error masquerading as a path quirk. A snapshot
    // path with no parent is a caller bug; surface it.
    let dir = path.parent().ok_or_else(|| {
        SnapshotError::InvalidFormat(format!("snapshot path has no parent: {}", path.display()))
    })?;
    // Collect the live entries once, then drive both the buffer-size estimate
    // and the write pass off this slice — a single walk of `entries` instead of
    // one to sum sizes and another to write.
    //
    // Write in sorted key order so a given logical state always serializes to
    // identical bytes. `HashMap` iteration order is randomized per process,
    // which would otherwise make every compaction emit a different layout —
    // defeating byte-level snapshot comparison (e.g. an integrity checksum) and
    // making file diffs noise. The sort is O(n log n) on the live key set, which
    // is trivial next to the I/O it precedes.
    let mut sorted: Vec<&KvEntry> = entries.values().collect();
    sorted.sort_unstable_by(|a, b| a.key.cmp(&b.key));

    // Buffer the writes: each record is 5–8 individual `write_all` calls, so an
    // unbuffered tempfile would issue thousands of `write(2)` syscalls per
    // compaction. Size the buffer to the exact serialized length (clamped to
    // [8 KiB, 1 MiB]) so the whole snapshot flushes in a handful of syscalls
    // instead of one per default 8 KiB page — and a pathologically large table
    // can't balloon the buffer past 1 MiB.
    let estimated: usize = HEADER_LEN
        + sorted
            .iter()
            .map(|e| 4 + 1 + 2 + e.key.len() + 4 + e.value.len() + 1 + e.version.as_bytes().len())
            .sum::<usize>()
        + if cursor.is_none() {
            0
        } else {
            4 + 1 + 1 + cursor.version().as_bytes().len()
        };
    let capacity = estimated.clamp(8 * 1024, 1024 * 1024);
    let mut buf = io::BufWriter::with_capacity(capacity, tempfile::NamedTempFile::new_in(dir)?);

    buf.write_all(MAGIC)?;
    buf.write_all(&FORMAT_VERSION.to_le_bytes())?;

    for entry in sorted {
        write_put_record(&mut buf, &entry.key, &entry.value, &entry.version)?;
    }

    if !cursor.is_none() {
        write_cursor_record(&mut buf, cursor)?;
    }

    buf.flush()?;
    let tmp = buf
        .into_inner()
        .map_err(|e| SnapshotError::Io(e.into_error()))?;

    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| SnapshotError::Io(e.error))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(key: &str, value: &[u8], rev: u64) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            version: VersionToken::from_u64(rev),
        }
    }

    fn cursor(rev: u64) -> WatchCursor {
        WatchCursor::from_u64(rev)
    }

    fn put(key: &str, value: &[u8], rev: u64) -> KvUpdate {
        KvUpdate::Put(entry(key, value, rev))
    }

    fn delete(key: &str, rev: u64) -> KvUpdate {
        KvUpdate::Delete {
            key: key.to_string(),
            version: VersionToken::from_u64(rev),
        }
    }

    #[test]
    fn round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("node.us-east-1", b"val1", 1)).unwrap();
        w.write_update(&put("node.eu-west-1", b"val2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.cursor.as_u64(), Some(2));

        assert_eq!(snap.entries["node.us-east-1"].value, b"val1");
        assert_eq!(snap.entries["node.eu-west-1"].value, b"val2");
    }

    #[test]
    fn multiple_batches() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.cursor.as_u64(), Some(2));
    }

    #[test]
    fn delete_removes_entry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        w.write_update(&delete("a", 3)).unwrap();
        w.checkpoint(&cursor(3)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 1);
        assert!(snap.entries.contains_key("b"));
    }

    #[test]
    fn purge_removes_entry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        w.write_update(&KvUpdate::Purge {
            key: "a".to_string(),
            version: VersionToken::from_u64(2),
        })
        .unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert!(!snap.entries.contains_key("a"));
    }

    #[test]
    fn missing_file_returns_none() {
        let dir = TempDir::new().unwrap();
        assert!(load(&dir.path().join("nope.snap")).unwrap().is_none());
    }

    #[test]
    fn corrupted_mid_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        // Write three entries so corruption in the middle has records on both sides
        w.write_update(&put("a", b"aaaa-long-value-here", 1))
            .unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        w.write_update(&put("b", b"bbbb-long-value-here", 2))
            .unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        w.write_update(&put("c", b"cccc-long-value-here", 3))
            .unwrap();
        w.checkpoint(&cursor(3)).unwrap();
        drop(w);

        let mut data = fs::read(&path).unwrap();
        // Flip a byte in the second record area (well past the first, well before EOF)
        let target = HEADER_LEN + 40;
        assert!(
            target < data.len() - 60,
            "need enough room after corruption for valid records"
        );
        data[target] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        match load(&path) {
            Err(SnapshotError::Corrupted) => {}
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn truncated_final_record_recovered() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        // Chop a few bytes off the end (simulates crash mid-write)
        let mut data = fs::read(&path).unwrap();
        data.truncate(data.len() - 3);
        fs::write(&path, &data).unwrap();

        let snap = load(&path).unwrap().unwrap();
        assert!(snap.entries.contains_key("a"));
    }

    #[test]
    fn truncated_tail_repaired_then_appendable() {
        // The already-compact skip optimization must NOT skip the rewrite when
        // the log has a truncated tail: leaving the partial record on disk would
        // let the next append land after it and corrupt the log.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        // Simulate a crash mid-write: chop the final bytes.
        let mut data = fs::read(&path).unwrap();
        data.truncate(data.len() - 3);
        fs::write(&path, &data).unwrap();

        // load() repairs the file by rewriting without the partial tail.
        let snap = load(&path).unwrap().unwrap();
        assert!(snap.entries.contains_key("a"));
        assert!(snap.entries.contains_key("b"));

        // Appending after the repaired load must not corrupt the log.
        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("c", b"v3", 3)).unwrap();
        w.checkpoint(&cursor(3)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 3);
        assert!(snap.entries.contains_key("c"));
        assert_eq!(snap.cursor.as_u64(), Some(3));
    }

    #[test]
    fn repeated_cursor_records_trigger_compaction() {
        // A service that checkpoints frequently writes one cursor record per
        // checkpoint. Even with unique keys and a clean EOF, the stale cursor
        // records are dead weight: load() must NOT treat the file as already
        // compact, or the bloat persists until the writer's own threshold fires
        // (and never, if the process crashes first).
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        for i in 1..=10u64 {
            w.checkpoint(&cursor(i)).unwrap();
        }
        drop(w);

        let size_before = fs::metadata(&path).unwrap().len();
        let snap = load(&path).unwrap().unwrap();
        let size_after = fs::metadata(&path).unwrap().len();

        // Single entry + only the latest cursor survive.
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.cursor.as_u64(), Some(10));
        assert!(
            size_after < size_before,
            "stale cursor records should be compacted away: {size_before} -> {size_after}"
        );

        // The rewritten file is now genuinely compact: a second load is a no-op.
        let after_first = fs::read(&path).unwrap();
        load(&path).unwrap().unwrap();
        let after_second = fs::read(&path).unwrap();
        assert_eq!(after_first, after_second);
    }

    #[test]
    fn already_compact_file_reloads_unchanged() {
        // A compact file (unique keys, no deletes, clean EOF) should reload
        // correctly even though load() skips the rewrite.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        // First load compacts. Capture the resulting bytes.
        load(&path).unwrap().unwrap();
        let after_first = fs::read(&path).unwrap();

        // Second load sees an already-compact file: skips the rewrite, so the
        // bytes are byte-for-byte identical, and the data still round-trips.
        let snap = load(&path).unwrap().unwrap();
        let after_second = fs::read(&path).unwrap();

        assert_eq!(after_first, after_second);
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.cursor.as_u64(), Some(2));
    }

    #[test]
    fn bad_magic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");
        fs::write(&path, b"XXXX\x01\x00").unwrap();

        match load(&path) {
            Err(SnapshotError::InvalidFormat(msg)) => assert!(msg.contains("magic")),
            other => panic!("expected InvalidFormat, got {other:?}"),
        }
    }

    #[test]
    fn wrong_version_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");
        // Valid magic, but a format version this build doesn't understand.
        // A future engineer who bumps FORMAT_VERSION must see old files rejected
        // rather than silently misparsed.
        let mut data = Vec::new();
        data.extend_from_slice(MAGIC);
        data.extend_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        fs::write(&path, &data).unwrap();

        match load(&path) {
            Err(SnapshotError::InvalidFormat(msg)) => {
                assert!(
                    msg.contains("version"),
                    "message should mention version: {msg}"
                )
            }
            other => panic!("expected InvalidFormat, got {other:?}"),
        }
    }

    #[test]
    fn empty_log_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut f = File::create(&path).unwrap();
        f.write_all(MAGIC).unwrap();
        f.write_all(&FORMAT_VERSION.to_le_bytes()).unwrap();
        drop(f);

        assert!(load(&path).unwrap().is_none());
    }

    #[test]
    fn compaction_on_load_shrinks_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        for i in 0..10u64 {
            w.write_update(&put("same-key", format!("v{i}").as_bytes(), i))
                .unwrap();
            w.checkpoint(&cursor(i)).unwrap();
        }
        drop(w);

        let size_before = fs::metadata(&path).unwrap().len();
        let snap = load(&path).unwrap().unwrap();
        let size_after = fs::metadata(&path).unwrap().len();

        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries["same-key"].value, b"v9");
        assert!(
            size_after < size_before,
            "compaction should shrink: {size_before} -> {size_after}"
        );
    }

    #[test]
    fn compact_when_threshold_exceeded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        // Threshold low enough to trigger after a few writes
        let mut w = SnapshotWriter::open(&path, 100).unwrap();
        for i in 0..20u64 {
            w.write_update(&put("key", format!("value-{i}").as_bytes(), i))
                .unwrap();
            if w.checkpoint(&cursor(i)).unwrap() {
                w.compact().unwrap();
            }
        }
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries["key"].value, b"value-19");
    }

    #[test]
    fn reopen_appends() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        // First writer
        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("a", b"v1", 1)).unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        drop(w);

        // Second writer appends
        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("b", b"v2", 2)).unwrap();
        w.checkpoint(&cursor(2)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 2);
        assert_eq!(snap.cursor.as_u64(), Some(2));
    }

    #[test]
    fn large_values() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let big = vec![0xABu8; 100_000];
        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("big", &big, 1)).unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries["big"].value.len(), 100_000);
        assert!(snap.entries["big"].value.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn cursor_only_snapshot_returns_some_with_empty_entries() {
        // A service that checkpoints before writing any entries produces a file
        // with only a cursor record. load() must return Some (not None) so callers
        // get the resume position even when there's nothing to preload. The guard
        // `entries.is_empty() && cursor.is_none()` must NOT fire when the cursor
        // is present.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.checkpoint(&cursor(42)).unwrap();
        drop(w);

        let snap = load(&path)
            .unwrap()
            .expect("cursor-only snapshot should return Some");
        assert!(snap.entries.is_empty(), "no entries written, none expected");
        assert_eq!(
            snap.cursor.as_u64(),
            Some(42),
            "cursor must survive round-trip"
        );
    }

    #[test]
    fn stale_keys_detects_removed_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&put("node.a", b"v1", 1)).unwrap();
        w.write_update(&put("node.b", b"v2", 2)).unwrap();
        w.write_update(&put("node.c", b"v3", 3)).unwrap();
        w.checkpoint(&cursor(3)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();

        // Simulate a fresh scan that only has "node.a" and "node.c"
        let mut stale = snap.stale_keys(["node.a", "node.c"]);
        stale.sort();
        assert_eq!(stale, vec!["node.b"]);

        // All keys present → no stale
        let stale = snap.stale_keys(["node.a", "node.b", "node.c"]);
        assert!(stale.is_empty());

        // No keys present → all stale
        let mut stale: Vec<&str> = snap.stale_keys(std::iter::empty::<&str>());
        stale.sort();
        assert_eq!(stale, vec!["node.a", "node.b", "node.c"]);
    }

    #[test]
    fn non_u64_version_token_round_trips() {
        // Regression: Put/Delete records store the version as length-prefixed raw
        // bytes, not a fixed u64. A 10-byte FDB-style versionstamp must survive a
        // round-trip intact — the old u64-only field flattened it to 0, which
        // would make every later CAS on a restored entry fail RevisionMismatch.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let stamp = [9u8, 8, 7, 6, 5, 4, 3, 2, 1, 0];
        let token = VersionToken::from_fdb_versionstamp(&stamp);
        assert!(token.as_u64().is_none(), "10-byte token is not a u64");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        w.write_update(&KvUpdate::Put(KvEntry {
            key: "fdb.key".to_string(),
            value: b"v".to_vec(),
            version: token.clone(),
        }))
        .unwrap();
        w.checkpoint(&cursor(1)).unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert_eq!(
            snap.entries["fdb.key"].version.as_bytes(),
            &stamp,
            "versionstamp must survive the snapshot round-trip byte-for-byte"
        );
    }

    /// Proves the `compact()` flush-before-read fix: records written but not yet
    /// checkpointed still sit in the BufWriter. Before the fix, `compact()` read
    /// the file (missing those records), rewrote it, and the old buffer then
    /// flushed into the renamed-away inode — silently dropping the writes. With
    /// the leading flush, an un-checkpointed write survives compaction.
    #[test]
    fn compact_preserves_uncheckpointed_writes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.snap");

        let mut w = SnapshotWriter::open(&path, u64::MAX).unwrap();
        // Buffered only — deliberately NO checkpoint() before compacting.
        w.write_update(&put("node.a", b"survives", 1)).unwrap();
        w.compact().unwrap();
        drop(w);

        let snap = load(&path).unwrap().unwrap();
        assert!(
            snap.entries.contains_key("node.a"),
            "compact() must not drop buffered-but-uncheckpointed writes"
        );
        assert_eq!(snap.entries["node.a"].value, b"survives");
    }
}
