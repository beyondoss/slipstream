//! Fleet-wide "export at most once per round" lease.
//!
//! Every replica of a fold runs the same checkpoint loop; without coordination,
//! N nodes would produce N identical artifacts per round. [`ExportLease`] makes
//! exactly one of them do the work: each candidate calls
//! [`try_acquire`](ExportLease::try_acquire) when its trigger fires; one wins,
//! the rest skip the round.
//!
//! ## Mechanism: CAS + embedded expiry — no TTL machinery
//!
//! The lease is a single KV key. Acquisition is a **create-only** write
//! (`KvWriter::create`): exactly one caller fleet-wide can create a missing
//! key, so the race has one winner by construction. The lease's lifetime is an
//! `expires_at_unix` timestamp **inside the value** — not a server-side TTL —
//! and an expired (or unparseable) lease is taken over with a CAS
//! [`update`](crate::KvWriter::update) against the observed version, so the
//! steal race also has exactly one winner.
//!
//! Embedding the expiry rather than using per-message TTL keeps the lease
//! portable to any [`KvWriter`] backend and free of server-version/bucket-flag
//! requirements. The cost is wall-clock comparison across nodes: with
//! NTP-sane clocks and round periods measured in minutes, skew is noise — and
//! a premature steal is *safe* anyway (two exporters produce two identical
//! artifacts; the upload is last-write-wins on the same key). The lease is a
//! work-deduplication optimization, never a correctness gate.
//!
//! ## Lifecycle
//!
//! A successful round leaves the key in place until it expires — that is the
//! "at most once per `ttl`" semantic: `ttl` IS the round period. The winner
//! calls [`LeaseGuard::complete`] after its upload succeeds, which (best
//! effort) rewrites the value with the exported cursor and completion time, so
//! the lease key doubles as the fleet-visible "last export" record. A crash
//! mid-round simply lets the key expire; the next trigger elects someone else.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::artifact::hex_encode;
use crate::kv::{KvError, KvReader, KvWriter, VersionToken, WatchCursor};
use crate::stores::KvStore;

/// The lease key's value: who holds (or last held) the round, until when, and
/// — after [`LeaseGuard::complete`] — what was exported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRecord {
    /// Identity of the node that won the round (caller-chosen, e.g. node id).
    pub holder_id: String,
    /// When the round was won, seconds since the Unix epoch.
    pub acquired_at_unix: u64,
    /// When the lease lapses and the next round may be won. This is the round
    /// period: the "at most once per `ttl`" bound.
    pub expires_at_unix: u64,
    /// Hex of the exported artifact's cursor, set by [`LeaseGuard::complete`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_cursor_hex: Option<String>,
    /// When the round completed (artifact uploaded), set by
    /// [`LeaseGuard::complete`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix: Option<u64>,
}

/// Coordinates "at most one export per round" across every replica of a fold.
/// See the module docs for the CAS + embedded-expiry mechanism.
pub struct ExportLease {
    reader: Arc<dyn KvReader>,
    writer: Arc<dyn KvWriter>,
    key: String,
    holder_id: String,
}

/// Proof of a won round, returned by [`ExportLease::try_acquire`].
///
/// There is nothing to release — the round runs until the lease expires (that
/// is the round period). Call [`complete`](Self::complete) after the artifact
/// is safely uploaded to publish what was exported.
pub struct LeaseGuard {
    writer: Arc<dyn KvWriter>,
    key: String,
    record: LeaseRecord,
    version: VersionToken,
    /// Set by [`complete`](Self::complete) / [`abandon`](Self::abandon). A
    /// guard dropped without either (early `?`, cancelled future) leaks the
    /// round — the fleet waits out the ttl — so [`Drop`] logs it.
    resolved: bool,
}

impl ExportLease {
    /// A lease on `key` in `store`, identifying this node as `holder_id`.
    ///
    /// Fails with [`KvError::OperationFailed`] if the store has no writer.
    pub fn new(
        store: &dyn KvStore,
        key: impl Into<String>,
        holder_id: impl Into<String>,
    ) -> Result<Self, KvError> {
        let writer = store.writer().ok_or_else(|| {
            KvError::OperationFailed(format!(
                "store {:?} has no writer; an export lease needs create/update",
                store.name()
            ))
        })?;
        Ok(Self {
            reader: store.reader(),
            writer,
            key: key.into(),
            holder_id: holder_id.into(),
        })
    }

    /// Try to win this export round. Exactly one caller fleet-wide gets
    /// `Ok(Some(guard))` per round; everyone else gets `Ok(None)` and skips.
    ///
    /// `ttl` is the round period: the winner's lease suppresses further rounds
    /// until it lapses, whether or not the winner survives. Crash mid-round →
    /// the key expires → the next trigger elects someone else.
    pub async fn try_acquire(&self, ttl: Duration) -> Result<Option<LeaseGuard>, KvError> {
        let now = unix_now();
        let record = LeaseRecord {
            holder_id: self.holder_id.clone(),
            acquired_at_unix: now,
            expires_at_unix: now.saturating_add(ttl.as_secs()),
            completed_cursor_hex: None,
            completed_at_unix: None,
        };
        let bytes =
            serde_json::to_vec(&record).map_err(|e| KvError::SerializationError(e.to_string()))?;

        // Fast path: the round is open — create-only, one winner.
        match self.writer.create(&self.key, &bytes).await {
            Ok(version) => {
                debug!(key = %self.key, holder = %self.holder_id, "export lease acquired (create)");
                return Ok(Some(self.guard(record, version)));
            }
            Err(KvError::AlreadyExists) => {}
            Err(e) => return Err(e),
        }

        // The key exists. `entry` (not `get`): a CAS-deleted lease is an
        // empty-value tombstone that `get` hides, but its version is exactly
        // what the takeover CAS needs.
        let Some(entry) = self.reader.entry(&self.key).await? else {
            // Deleted between create and read; treat as lost — the next
            // trigger retries cleanly rather than looping here.
            return Ok(None);
        };

        // A live, parseable, unexpired lease wins; anything else (expired,
        // tombstone, unparseable garbage) is taken over. Unparseable leases
        // MUST be stealable or one corrupt write wedges exports fleet-wide.
        if let Ok(existing) = serde_json::from_slice::<LeaseRecord>(&entry.value)
            && existing.expires_at_unix > now
        {
            return Ok(None);
        }

        // Takeover: CAS against the version we read — one winner.
        match self.writer.update(&self.key, &bytes, &entry.version).await {
            Ok(version) => {
                debug!(key = %self.key, holder = %self.holder_id, "export lease acquired (takeover)");
                Ok(Some(self.guard(record, version)))
            }
            // Someone else's create/update landed first: their round.
            Err(KvError::RevisionMismatch | KvError::AlreadyExists | KvError::KeyNotFound) => {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Read the current lease record, if any — the fleet-visible "last export"
    /// state. `None` when no round has ever run (or the key was tombstoned);
    /// [`KvError::SerializationError`] when the key holds unparseable bytes —
    /// distinct from `None` so an operator can see "present but corrupt" (a
    /// state [`try_acquire`](Self::try_acquire) will repair by takeover) rather
    /// than a false "never ran".
    pub async fn current(&self) -> Result<Option<LeaseRecord>, KvError> {
        match self.reader.get(&self.key).await? {
            Some(entry) => serde_json::from_slice(&entry.value).map(Some).map_err(|e| {
                KvError::SerializationError(format!(
                    "lease key {:?} holds an unparseable value: {e}",
                    self.key
                ))
            }),
            None => Ok(None),
        }
    }

    fn guard(&self, record: LeaseRecord, version: VersionToken) -> LeaseGuard {
        LeaseGuard {
            writer: Arc::clone(&self.writer),
            key: self.key.clone(),
            record,
            version,
            resolved: false,
        }
    }
}

impl LeaseGuard {
    /// The record this guard wrote when it won the round.
    pub fn record(&self) -> &LeaseRecord {
        &self.record
    }

    /// Give the round back early: a failed export/upload should not suppress
    /// the fleet for the rest of the ttl. CAS-deletes the lease (tombstone)
    /// against this guard's version, so the next trigger on any node can win a
    /// fresh round immediately.
    ///
    /// Best-effort: a CAS conflict (someone already took over) or write error
    /// is logged, not surfaced — worst case the round waits out its ttl, which
    /// is the no-abandon behavior anyway.
    pub async fn abandon(mut self) {
        self.resolved = true;
        match self
            .writer
            .delete_with_version(&self.key, &self.version)
            .await
        {
            Ok(_) => {
                debug!(key = %self.key, holder = %self.record.holder_id, "export lease abandoned");
            }
            Err(e) => {
                warn!(
                    key = %self.key,
                    holder = %self.record.holder_id,
                    error = %e,
                    "failed to abandon export lease; next round waits for expiry"
                );
            }
        }
    }

    /// Publish the round's outcome: rewrite the lease value with the exported
    /// cursor and completion time (expiry unchanged — the round still runs its
    /// full period).
    ///
    /// Best-effort observability: a CAS conflict means the lease was already
    /// taken over (this round overran its ttl) and is logged, not surfaced —
    /// the artifact is already safe wherever the caller put it.
    pub async fn complete(mut self, cursor: &WatchCursor) -> Result<(), KvError> {
        self.resolved = true;
        self.record.completed_cursor_hex = Some(hex_encode(cursor.version().as_bytes()));
        self.record.completed_at_unix = Some(unix_now());
        let bytes = serde_json::to_vec(&self.record)
            .map_err(|e| KvError::SerializationError(e.to_string()))?;
        match self.writer.update(&self.key, &bytes, &self.version).await {
            Ok(_) => Ok(()),
            Err(KvError::RevisionMismatch) => {
                warn!(
                    key = %self.key,
                    holder = %self.record.holder_id,
                    "export round overran its lease; completion record skipped"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.resolved {
            warn!(
                key = %self.key,
                holder = %self.record.holder_id,
                "LeaseGuard dropped without complete() or abandon(); the fleet waits out the lease ttl"
            );
        }
    }
}

fn unix_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => {
            // A pre-epoch clock means every lease this node writes is already
            // expired (`expires_at = 0 + ttl` is in the past), so any node can
            // steal it — duplicate exports every round, which the lease design
            // tolerates (dedup, not correctness). That direction is deliberate:
            // the alternative sentinel (`u64::MAX`) would mint a never-expiring
            // lease that wedges the fleet until manual cleanup. But it must not
            // be silent — duplicate artifacts with no log line is undebuggable.
            warn!(
                "system clock predates the Unix epoch; lease expiry math degraded (expect duplicate export rounds until the clock is fixed)"
            );
            0
        }
    }
}
