use async_trait::async_trait;
use std::fmt;
use tokio::sync::mpsc::Sender;

/// Opaque position in a watch stream for resuming after disconnect.
///
/// Backends store whatever they need to resume (NATS: u64 revision).
/// Callers should treat this as opaque and only pass it back to
/// `watch_all_from` / `watch_prefix_from`.
///
/// A cursor that [`watch_applied`](crate::watch_applied) produced may also
/// remember that the bucket's retention was once seen evicting current
/// values. That memo outlives the retention setting: a key that aged out
/// while it was in force is missing from the bucket's listing forever after,
/// so the cursor-expiry repair must never again treat the listing as the
/// truth. A store persisting a cursor should write
/// [`to_bytes`](Self::to_bytes) and read [`from_bytes`](Self::from_bytes);
/// [`as_u64`](Self::as_u64) keeps only the position.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct WatchCursor {
    version: VersionToken,
    seen_evicting: bool,
}

impl fmt::Debug for WatchCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.seen_evicting {
            write!(f, "WatchCursor({:?}, seen evicting)", self.version)
        } else {
            write!(f, "WatchCursor({:?})", self.version)
        }
    }
}

/// The longest encoding [`WatchCursor::to_bytes`] produces.
pub(crate) const MAX_CURSOR_BYTES: usize = CURSOR_MEMO_LEN + 8;

/// A cursor with its memo: the version padded to the token's 10-byte
/// capacity, its length, a flags byte, and — when the flags say so — the
/// server time its message was written (u64 LE Unix milliseconds). Longer
/// than any bare version, so the encodings never collide, and a cursor
/// without the memo keeps the pre-memo encoding byte for byte (older builds
/// read it; they refuse this one as too long rather than misread it).
const CURSOR_MEMO_LEN: usize = 12;
const CURSOR_SEEN_EVICTING: u8 = 0b01;
const CURSOR_WRITTEN_AT: u8 = 0b10;

impl WatchCursor {
    /// No cursor — forces a full watch on next connect.
    pub fn none() -> Self {
        Self::default()
    }

    /// Returns true if this cursor has no position (will trigger full watch).
    pub fn is_none(&self) -> bool {
        self.version.is_unknown()
    }

    /// Create a cursor from a version token.
    pub fn from_version(token: VersionToken) -> Self {
        Self {
            version: token,
            seen_evicting: false,
        }
    }

    /// Create a cursor from a u64 revision (convenience for NATS).
    pub fn from_u64(rev: u64) -> Self {
        Self::from_version(VersionToken::from_u64(rev))
    }

    /// The cursor as a store should persist it, memo included. The time its
    /// message was written is kept only alongside the memo: it matters only
    /// on a bucket that evicts (`NatsKvWatcher`'s resume check).
    pub fn to_bytes(&self) -> Vec<u8> {
        let v = self.version.as_bytes();
        if !self.seen_evicting {
            return v.to_vec();
        }
        let mut out = vec![0u8; CURSOR_MEMO_LEN];
        out[..v.len()].copy_from_slice(v);
        out[10] = v.len() as u8;
        out[11] = CURSOR_SEEN_EVICTING;
        if let Some(ms) = self.version.written_ms() {
            out[11] |= CURSOR_WRITTEN_AT;
            out.extend_from_slice(&ms.to_le_bytes());
        }
        out
    }

    /// Read back [`to_bytes`](Self::to_bytes). `None` for bytes no build
    /// wrote: a length or flags this build doesn't know.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() <= 10 {
            return VersionToken::from_raw(bytes).map(Self::from_version);
        }
        let (head, tail) = bytes.split_at_checked(CURSOR_MEMO_LEN)?;
        let (len, flags) = (head[10] as usize, head[11]);
        if len > 10 || flags & !(CURSOR_SEEN_EVICTING | CURSOR_WRITTEN_AT) != 0 {
            return None;
        }
        let mut version = VersionToken::from_raw(&head[..len])?;
        match (flags & CURSOR_WRITTEN_AT != 0, tail.len()) {
            (false, 0) => {}
            (true, 8) => {
                let ms = u64::from_le_bytes(tail.try_into().ok()?);
                if ms == 0 {
                    return None;
                }
                version = version.written_at(ms);
            }
            _ => return None,
        }
        Some(Self {
            version,
            seen_evicting: flags & CURSOR_SEEN_EVICTING != 0,
        })
    }

    /// The server time this cursor's message was written, when known (Unix
    /// milliseconds).
    pub(crate) fn written_ms(&self) -> Option<u64> {
        self.version.written_ms()
    }

    /// The bucket's retention was seen evicting current values while this
    /// cursor's fold — or one it was restored from — tracked it.
    pub(crate) fn seen_evicting(&self) -> bool {
        self.seen_evicting
    }

    /// This cursor, remembering that retention evicts current values (or
    /// unchanged when `seen` is false: the memo never clears).
    pub(crate) fn remembering(mut self, seen: bool) -> Self {
        self.seen_evicting |= seen;
        self
    }

    /// Try to extract as u64 revision.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        self.version.as_u64()
    }

    /// Access the underlying version token.
    pub(crate) fn version(&self) -> &VersionToken {
        &self.version
    }

    /// The cursor's position for ordering — the artifact pointer swap, the
    /// restore guard: its u64 revision, or 0 when it has none. Those
    /// protocols need a backend with u64 positions; a revisionless cursor
    /// ranks lowest, so it never supersedes a real one and is never "ahead".
    /// The ONE place cursors are ranked.
    pub(crate) fn rank(&self) -> u64 {
        self.as_u64().unwrap_or(0)
    }
}

/// Error type for KV operations.
///
/// `KvError` is `Clone` so a single failure can fan out to multiple waiters
/// (e.g. callers blocked on a shared connect result). The underlying backend
/// errors — `std::io::Error`, the `async-nats` error types — are *not* `Clone`,
/// so their detail is flattened into the message string at this boundary rather
/// than retained as a `#[source]` cause. Keeping `KvError: Clone` across the
/// object-safe `async_trait` surface is the deliberate trade-off; the cost is a
/// structured cause chain, which is why the `String` variants carry pre-rendered
/// context instead of a nested error.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KvError {
    #[error("store not connected")]
    NotConnected,
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("key not found")]
    KeyNotFound,
    /// Key already exists (create-if-not-exists conflict).
    #[error("key already exists")]
    AlreadyExists,
    /// CAS conflict: current version doesn't match expected.
    #[error("revision mismatch")]
    RevisionMismatch,
    #[error("deserialization error: {0}")]
    DeserializationError(String),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("watch error: {0}")]
    WatchError(String),
    #[error("operation failed: {0}")]
    OperationFailed(String),
    #[error("operation timed out")]
    Timeout,
    /// The watch cursor/revision is too old — the backend has compacted past it.
    /// Callers should fall back to a full scan + watch.
    ///
    /// A `*_from` resume can also return this MID-STREAM, after delivering
    /// updates: the NATS All-scope resume watch does so when retention overruns
    /// the live consumer (the floor guard). Everything delivered before the
    /// error is valid; the position after it is expired.
    #[error("watch cursor expired (compacted)")]
    CursorExpired,
}

/// What a backend's retention can do to the log a watch resumes from. These
/// are the facts [`watch_applied`](crate::watch_applied) needs to repair an
/// expired cursor without guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// Retention can evict a key's CURRENT value, not just superseded history
    /// (NATS: `max_age`, per-message TTLs, or `discard: old` under a byte or
    /// message limit). When true, a key missing from the bucket may never have
    /// been deleted, so the bucket's key listing is not evidence of deletion.
    pub evicts_current_values: bool,
    /// The oldest revision still in the log (NATS: the stream's
    /// `first_sequence`). Resuming after cursor `C` is gap-free iff
    /// `first_revision <= C + 1`
    /// ([`resume_window_ok`](crate::protocol::resume_window_ok)).
    pub first_revision: u64,
}

/// Opaque version token that abstracts store-specific versioning.
///
/// Different stores use different versioning schemes:
/// - NATS: 8-byte u64 revision
/// - FDB: 10-byte versionstamp
/// - Redis: could be stream ID + sequence
///
/// Stored inline (no heap allocation) — fits up to 10 bytes, which covers
/// every current backend.
///
/// A token read off a watch may also carry the server time its message was
/// written (NATS: the message timestamp). That time is not part of the
/// version: equality and hashing ignore it, and it isn't in
/// [`as_bytes`](Self::as_bytes).
#[derive(Clone, Default)]
pub struct VersionToken {
    len: u8,
    buf: [u8; 10],
    // Unix milliseconds of the message's server timestamp; 0 = unknown.
    written_ms: u64,
}

impl PartialEq for VersionToken {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for VersionToken {}

impl std::hash::Hash for VersionToken {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for VersionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.as_bytes();
        if let Some(v) = self.as_u64() {
            write!(f, "VersionToken(u64: {v})")
        } else if bytes.is_empty() {
            write!(f, "VersionToken(unknown)")
        } else {
            write!(f, "VersionToken({bytes:?})")
        }
    }
}

impl VersionToken {
    /// Create an empty/unknown version (for entries without version info).
    pub fn unknown() -> Self {
        Self::default()
    }

    /// Check if this is an unknown/empty version.
    pub fn is_unknown(&self) -> bool {
        self.len == 0
    }

    /// Create from NATS u64 revision.
    pub fn from_u64(rev: u64) -> Self {
        let mut buf = [0u8; 10];
        buf[..8].copy_from_slice(&rev.to_be_bytes());
        Self {
            len: 8,
            buf,
            written_ms: 0,
        }
    }

    /// This version, carrying the server time its message was written
    /// (Unix milliseconds).
    pub(crate) fn written_at(mut self, unix_ms: u64) -> Self {
        self.written_ms = unix_ms;
        self
    }

    /// The server time this version's message was written, when the backend
    /// reported it (Unix milliseconds).
    pub(crate) fn written_ms(&self) -> Option<u64> {
        (self.written_ms != 0).then_some(self.written_ms)
    }

    /// Create from FDB versionstamp (10 bytes).
    ///
    /// `cfg(test)` until a FoundationDB backend ships and the round-trip is
    /// tested end-to-end: a 10-byte token has no `as_u64()`, so handing one to
    /// the NATS backend's CAS path yields an unactionable `OperationFailed`.
    /// Today it exists only for the snapshot length-prefixed-version tests; an
    /// FDB backend should lift the gate (and the visibility) rather than add a
    /// second constructor.
    #[cfg(test)]
    pub(crate) fn from_fdb_versionstamp(vs: &[u8; 10]) -> Self {
        Self {
            len: 10,
            buf: *vs,
            written_ms: 0,
        }
    }

    /// Try to extract as u64 (for NATS compatibility).
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        if self.len == 8 {
            Some(u64::from_be_bytes(self.buf[..8].try_into().unwrap_or_else(
                |_| unreachable!("len == 8 guarantees an 8-byte slice"),
            )))
        } else {
            None
        }
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len as usize]
    }

    /// Create from raw bytes (crate-internal, e.g. snapshot deserialization).
    ///
    /// Returns `None` if `bytes` exceeds the 10-byte inline capacity. Silently
    /// truncating instead would store a version that differs from the real
    /// revision, causing every later CAS to fail with `RevisionMismatch` and no
    /// actionable error — so an oversized token is rejected at the boundary
    /// rather than absorbed. Callers parse a length-prefixed field that is
    /// structurally bounded to 10 bytes, so `None` is unreachable in practice;
    /// returning it (instead of panicking) keeps the failure mode a recoverable
    /// format error for any future caller that lacks that guard.
    #[must_use]
    pub(crate) fn from_raw(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 10 {
            return None;
        }
        let len = bytes.len() as u8;
        let mut buf = [0u8; 10];
        buf[..len as usize].copy_from_slice(bytes);
        Some(Self {
            len,
            buf,
            written_ms: 0,
        })
    }
}

/// A single key-value entry with metadata.
#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub version: VersionToken,
}

/// Update event from a watch stream.
#[derive(Debug, Clone)]
pub enum KvUpdate {
    /// Key was created or updated.
    Put(KvEntry),
    /// Key was deleted — by a delete marker, or (NATS) by a
    /// [`delete_with_version`](KvWriter::delete_with_version) tombstone, which
    /// watches report as a delete with the tombstone's revision: the same rule
    /// `get`/`scan`/`keys` apply. Only [`entry`](KvReader::entry) exposes the raw
    /// tombstone.
    Delete { key: String, version: VersionToken },
    /// Key was purged (NATS-specific: all history removed).
    /// Stores without purge semantics should map this to Delete.
    Purge { key: String, version: VersionToken },
}

impl KvUpdate {
    /// Get the key affected by this update.
    pub fn key(&self) -> &str {
        match self {
            KvUpdate::Put(e) => &e.key,
            KvUpdate::Delete { key, .. } => key,
            KvUpdate::Purge { key, .. } => key,
        }
    }

    /// Get the version of this update.
    pub fn version(&self) -> &VersionToken {
        match self {
            KvUpdate::Put(e) => &e.version,
            KvUpdate::Delete { version, .. } => version,
            KvUpdate::Purge { version, .. } => version,
        }
    }
}

/// Core read-only KV operations - the minimal interface every store must implement.
#[async_trait]
pub trait KvReader: Send + Sync {
    /// Get a value by key. Returns `None` if the key doesn't exist.
    ///
    /// Backends that use empty-value tombstones (NATS: `delete_with_version`
    /// writes an empty-value Put so concurrent CAS writers still conflict) also
    /// return `None` for a *stored* empty value — `get()` cannot tell a real
    /// `b""` apart from a tombstone. A caller using zero-length values as a
    /// presence signal (locks, feature flags) must use [`entry`](Self::entry),
    /// which exposes the raw record including empty-value Puts.
    async fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError>;

    /// Get all keys matching a prefix. Returns keys only, not values.
    async fn keys(&self, prefix: &str) -> Result<Vec<String>, KvError>;

    /// Hand every key [`keys`](Self::keys) would list to `f`, one at a time,
    /// without collecting them: the cursor-expiry restore checks a large
    /// bucket's listing this way rather than holding every key name at once.
    ///
    /// The provided implementation collects [`keys`](Self::keys) and iterates
    /// it. A backend that can stream should override it.
    async fn for_each_key(
        &self,
        prefix: &str,
        f: &mut (dyn FnMut(String) + Send),
    ) -> Result<(), KvError> {
        for key in self.keys(prefix).await? {
            f(key);
        }
        Ok(())
    }

    /// Get multiple entries by prefix. Useful for bulk loading.
    async fn scan(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError>;

    /// Get the raw entry for a key, including tombstones (empty-value Put
    /// entries written by `delete_with_version`). Most callers should use
    /// `get()` instead, which filters tombstones for consistency with `scan()`.
    ///
    /// REQUIRED (no default) — deliberately. A default delegating to `get()`
    /// silently hid tombstones on any backend that forgot to override it,
    /// which breaks CAS callers that need the tombstone's version: e.g.
    /// [`ExportLease::try_acquire`](crate::ExportLease::try_acquire) reads an
    /// abandoned (CAS-deleted) lease's version through `entry()` for its
    /// takeover write — with a `get()` default it would see `None` and report
    /// the round as live instead of stealing it. Backends without empty-value
    /// tombstone semantics (where delete genuinely removes the key) should
    /// implement this as a delegation to `get()` — explicitly, so the choice
    /// is a reviewed decision rather than an inherited footgun.
    async fn entry(&self, key: &str) -> Result<Option<KvEntry>, KvError>;
}

/// Watch capability - optional, not all stores support real-time updates.
///
/// The non-`_from` watches are **state-sync** streams: they first deliver the
/// current value of every matching key (the "re-list", as a stream of puts plus
/// any surviving delete markers), then live updates. A consumer starting with
/// no cursor therefore converges on the full bucket state without a separate
/// scan — and without the scan-to-watch race a separate scan would open. The
/// `_from` variants skip the re-list and deliver only the delta past the cursor.
#[async_trait]
pub trait KvWatcher: Send + Sync {
    /// Watch all keys: current state first, then live changes. Sends updates
    /// through the channel. Returns when the watch ends or an error occurs.
    async fn watch_all(&self, tx: Sender<KvUpdate>) -> Result<(), KvError>;

    /// Watch keys matching a prefix: current state first, then live changes.
    async fn watch_prefix(&self, prefix: &str, tx: Sender<KvUpdate>) -> Result<(), KvError>;

    /// Watch keys matching ANY of `prefixes`, delivered through one channel.
    ///
    /// The contract is exactly the union of the prefixes — no other keys. A
    /// backend with native multi-filter consumers (NATS server 2.10+) serves all
    /// `prefixes` from a SINGLE consumer; that matters because consumers are a
    /// per-stream resource (measured at ~tens of KB of server state each, growing
    /// super-linearly past a few thousand on one stream), so a watcher scoped to N
    /// prefixes must not cost N consumers.
    async fn watch_prefixes(&self, prefixes: &[&str], tx: Sender<KvUpdate>) -> Result<(), KvError>;

    /// Resume watching all keys from a previously saved cursor position.
    ///
    /// Returns `KvError::CursorExpired` if the backend has compacted past the
    /// cursor — callers should fall back to a full `watch_all()`.
    ///
    /// Default implementation ignores the cursor and delegates to `watch_all()`.
    async fn watch_all_from(
        &self,
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let _ = cursor;
        self.watch_all(tx).await
    }

    /// Resume watching keys with a prefix from a previously saved cursor.
    ///
    /// Default implementation ignores the cursor and delegates to `watch_prefix()`.
    async fn watch_prefix_from(
        &self,
        prefix: &str,
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let _ = cursor;
        self.watch_prefix(prefix, tx).await
    }

    /// Resume watching the union of `prefixes` from a previously saved cursor.
    ///
    /// Same single-consumer contract as [`watch_prefixes`](Self::watch_prefixes),
    /// same delta semantics as the other `_from` variants: only updates past the
    /// cursor are delivered, or [`KvError::CursorExpired`] if the backend has
    /// compacted past it.
    ///
    /// Default implementation ignores the cursor and delegates to
    /// `watch_prefixes()` — correct (the state-sync re-list is a superset of any
    /// delta) but a full replay; backends that can seek a multi-filter stream
    /// should override it.
    async fn watch_prefixes_from(
        &self,
        prefixes: &[&str],
        cursor: &WatchCursor,
        tx: Sender<KvUpdate>,
    ) -> Result<(), KvError> {
        let _ = cursor;
        self.watch_prefixes(prefixes, tx).await
    }

    /// The watched log's retention, read live: whether it can evict current
    /// values, and the oldest revision it still holds.
    ///
    /// `Ok(None)` (the default) means the backend can't say. Callers must then
    /// assume current values CAN be evicted, so the cursor-expired key-listing
    /// diff is not trusted unless the caller explicitly chose it.
    async fn retention(&self) -> Result<Option<Retention>, KvError> {
        Ok(None)
    }
}

/// Write operations - optional, edge proxy is primarily read-only.
#[async_trait]
pub trait KvWriter: Send + Sync {
    /// Put a value. Returns the new version token.
    async fn put(&self, key: &str, value: &[u8]) -> Result<VersionToken, KvError>;

    /// Delete a key. Best-effort: may return `true` even if the key did not
    /// exist (NATS does not report pre-existence). Use `get()` first if you
    /// need to distinguish "deleted something" from "nothing to delete".
    async fn delete(&self, key: &str) -> Result<bool, KvError>;

    /// Create a key only if it doesn't exist.
    /// Returns `AlreadyExists` if the key has a live value.
    async fn create(&self, key: &str, value: &[u8]) -> Result<VersionToken, KvError>;

    /// Compare-and-swap: update only if current version matches `expected`.
    /// Returns `RevisionMismatch` on conflict.
    async fn update(
        &self,
        key: &str,
        value: &[u8],
        expected: &VersionToken,
    ) -> Result<VersionToken, KvError>;

    /// CAS-gated delete: delete only if current version matches `expected`.
    /// Returns `RevisionMismatch` on conflict.
    /// Writes an empty value (logical delete) so concurrent writers get a conflict.
    /// Every read path treats it as a delete — `get`/`scan`/`keys` hide it, and
    /// watches (hence folds) receive a [`KvUpdate::Delete`] — except
    /// [`entry`](KvReader::entry), which exposes the tombstone and its version for
    /// CAS callers.
    async fn delete_with_version(
        &self,
        key: &str,
        expected: &VersionToken,
    ) -> Result<bool, KvError>;
}

/// TTL support - optional, for stores that support key expiration.
#[async_trait]
pub trait KvTtl: KvWriter {
    /// Put a value with TTL. Value expires after duration.
    async fn put_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: std::time::Duration,
    ) -> Result<VersionToken, KvError>;
}

/// Purge support - optional, for stores that can reclaim a key's storage.
///
/// Unlike [`KvWriter::delete`] (which writes a delete marker) and
/// [`KvWriter::delete_with_version`] (which writes an empty-value tombstone),
/// `purge` removes a key *and reclaims its bytes*. On NATS this issues a
/// rollup (`Nats-Rollup: sub`) that drops all prior revisions of the subject,
/// so the bytes stop counting against the stream's `max_bytes`.
///
/// Use this to bound a bucket that has no `max_age`: dead keys deleted with
/// `delete`/`delete_with_version` accumulate forever, but purged keys are
/// reclaimed.
#[async_trait]
pub trait KvPurge: KvWriter {
    /// Purge a key, reclaiming its storage. Idempotent: purging an absent key
    /// is not an error.
    async fn purge(&self, key: &str) -> Result<(), KvError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cursor without the eviction memo encodes exactly as before the memo
    /// existed (older builds keep reading it); with the memo it round-trips
    /// and is longer than any bare version; unknown encodings are refused.
    #[test]
    fn cursor_bytes_round_trip_and_stay_compatible() {
        let c = WatchCursor::from_u64(42);
        assert_eq!(c.to_bytes(), VersionToken::from_u64(42).as_bytes());
        assert_eq!(WatchCursor::from_bytes(&c.to_bytes()), Some(c.clone()));
        assert!(WatchCursor::from_bytes(&[]).is_some_and(|c| c.is_none()));

        let m = c.clone().remembering(true);
        assert!(m.seen_evicting() && m.as_u64() == Some(42));
        assert!(m.to_bytes().len() > 10);
        assert_eq!(WatchCursor::from_bytes(&m.to_bytes()), Some(m.clone()));
        assert!(
            m.clone().remembering(false).seen_evicting(),
            "the memo never clears"
        );

        let fdb = WatchCursor::from_version(VersionToken::from_fdb_versionstamp(&[7; 10]))
            .remembering(true);
        assert_eq!(WatchCursor::from_bytes(&fdb.to_bytes()), Some(fdb));

        let t = WatchCursor::from_version(VersionToken::from_u64(42).written_at(1_700_000_000_123));
        assert_eq!(t.to_bytes(), c.to_bytes(), "no memo: the time isn't kept");
        let t = t.remembering(true);
        let back = WatchCursor::from_bytes(&t.to_bytes()).unwrap();
        assert_eq!(back.written_ms(), Some(1_700_000_000_123));
        assert!(back.seen_evicting() && back.as_u64() == Some(42));
        assert_eq!(back, t);

        let mut bad = m.to_bytes();
        bad[11] |= 0b100;
        assert_eq!(WatchCursor::from_bytes(&bad), None, "unknown flag");
        let mut bad = m.to_bytes();
        bad[11] |= CURSOR_WRITTEN_AT;
        assert_eq!(WatchCursor::from_bytes(&bad), None, "time flagged, missing");
        let mut bad = m.to_bytes();
        bad[10] = 11;
        assert_eq!(WatchCursor::from_bytes(&bad), None, "version too long");
        assert_eq!(WatchCursor::from_bytes(&[0; 11]), None, "no such length");
    }

    #[test]
    fn from_raw_roundtrips_within_capacity() {
        // The largest token any backend uses is a 10-byte FDB versionstamp.
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let token = VersionToken::from_raw(&bytes).expect("10 bytes is within capacity");
        assert_eq!(token.as_bytes(), &bytes);

        // An 8-byte token is still interpretable as a NATS u64 revision.
        let rev = 0x0102_0304_0506_0708u64;
        let token = VersionToken::from_raw(&rev.to_be_bytes()).expect("8 bytes is within capacity");
        assert_eq!(token.as_u64(), Some(rev));

        // Empty input is the "unknown" token.
        assert!(
            VersionToken::from_raw(&[])
                .expect("empty is within capacity")
                .is_unknown()
        );
    }

    #[test]
    fn from_raw_rejects_above_capacity() {
        // 11 bytes exceeds the 10-byte inline buffer. This guards against a
        // loosened `parse_cursor` bound ever feeding oversized data through —
        // returning `None` surfaces the format/backend mismatch at its origin
        // instead of silently truncating into a wrong revision.
        assert!(VersionToken::from_raw(&[0u8; 11]).is_none());
    }
}
