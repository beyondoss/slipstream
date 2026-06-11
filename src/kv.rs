use async_trait::async_trait;
use std::fmt;
use tokio::sync::mpsc::Sender;

/// Opaque position in a watch stream for resuming after disconnect.
///
/// Backends store whatever they need to resume (NATS: u64 revision).
/// Callers should treat this as opaque and only pass it back to
/// `watch_all_from` / `watch_prefix_from`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchCursor(VersionToken);

impl WatchCursor {
    /// No cursor — forces a full watch on next connect.
    pub fn none() -> Self {
        Self(VersionToken::unknown())
    }

    /// Returns true if this cursor has no position (will trigger full watch).
    pub fn is_none(&self) -> bool {
        self.0.is_unknown()
    }

    /// Create a cursor from a version token.
    pub fn from_version(token: VersionToken) -> Self {
        Self(token)
    }

    /// Create a cursor from a u64 revision (convenience for NATS).
    pub fn from_u64(rev: u64) -> Self {
        Self(VersionToken::from_u64(rev))
    }

    /// Try to extract as u64 revision.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }

    /// Access the underlying version token.
    pub(crate) fn version(&self) -> &VersionToken {
        &self.0
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
    #[error("watch cursor expired (compacted)")]
    CursorExpired,
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
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct VersionToken {
    len: u8,
    buf: [u8; 10],
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
        Self { len: 8, buf }
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
        Self { len: 10, buf: *vs }
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
        Some(Self { len, buf })
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
    /// Key was deleted.
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

    /// Get multiple entries by prefix. Useful for bulk loading.
    async fn scan(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError>;

    /// Get the raw entry for a key, including tombstones (empty-value Put
    /// entries written by `delete_with_version`). Most callers should use
    /// `get()` instead, which filters tombstones for consistency with `scan()`.
    ///
    /// Override in backends where tombstone version access is needed for
    /// CAS conflict detection.
    async fn entry(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.get(key).await
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
