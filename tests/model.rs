//! Exhaustive model-check of the snapshot export/import protocol (Stateright).
//!
//! `tests/multi_export.rs` demonstrates specific bad interleavings; this file
//! PROVES properties over **every** interleaving of the modeled protocol,
//! within explicit bounds. The two layers are deliberately coupled: each
//! hazard demonstrated empirically appears here as a `sometimes` property the
//! checker must re-derive (so the model is faithful enough to express the
//! bugs), and each safety claim appears as an `always` property checked over
//! the full state space (so the claim is not an induction from sampled runs).
//!
//! ## What is modeled
//!
//! - N exporter replicas of one fold, each at its own applied cursor, racing
//!   uploads of a shared "latest" key. A replica may **crash between any two
//!   steps** — including between its payload upload and its manifest publish.
//! - The object store under BOTH transport layouts: the SHIPPED protocol
//!   (`pointer_swap: true` — content-addressed write-once payloads plus a
//!   single pointer object published last via monotonic conditional swap,
//!   `transport.rs` as of 0.6), and the LEGACY pre-0.6 layout
//!   (`pointer_swap: false` — payload tar and sibling manifest as two
//!   independent atomic last-write-wins registers), kept as the
//!   machine-checked record of why the protocol changed.
//! - **Prune** (shipped protocol): unreferenced payload objects can be
//!   deleted at any moment — modeled at zero grace, a superset of every real
//!   grace-period timing. The checker proves a prune racing a stale pointer
//!   read costs a DETECTED fetch miss and a retry, never wrong data, and the
//!   current pointer's target is never pruned.
//! - The source stream with **retention**: a floor that advances freely;
//!   resuming below the floor is `CursorExpired`; delete markers at or below
//!   the floor are evicted (the re-list cannot see them). Under
//!   `BucketRetention::EvictsCurrent` (`max_age`, `discard: old`) retention
//!   ALSO evicts the sentinel key's current value (`AgeOut`, FIFO-consistent:
//!   only once the floor has passed that value's revision), so the bucket
//!   stops listing a key nobody deleted.
//! - The sentinel key's full history: its initial put, an optional real
//!   delete, and an optional later write (`PutKey` — an update, or a
//!   re-create after the delete). A write made while the importer is behind
//!   that then ages out is exactly the gap write a re-list can never deliver.
//! - A bootstrapping importer whose two reads (sibling manifest, then
//!   payload) interleave with all of the above, whose cross-check compares
//!   them, and whose post-import resume either replays the tail, or falls
//!   back (expired cursor) under one of THREE resync modes: reader not wired
//!   (`None`), reader wired with the pre-fix warn-and-continue failure
//!   semantics (`Degrade`), or reader wired with fail-stop failure semantics
//!   (`FailStop` — `applied.rs` as it ships). The checker proves `Degrade`
//!   breaks the convergence theorem, which is the machine-checked
//!   justification for the fail-stop change in `resync_stale_keys`.
//!   `FailStop` repairs the way `ExpiryRepair::Auto` does: the key-listing
//!   diff on a bucket that keeps current values, an ARTIFACT RESTORE on one
//!   that evicts them (`Restoring` → `RestoreRead`, gated by the shared
//!   `protocol::restore_allowed` kernel: ahead of the local fold and still
//!   inside retention). The pre-fix behavior — the key-listing diff on an
//!   evicting bucket — is the `RelistOnEvicting` mutation, which the checker
//!   must catch.
//!
//! ## What "converged" means
//!
//! The fold is correct when it holds **every write minus every real delete**
//! (`key_at(head)`), not when it matches what NATS still lists. The two agree
//! on a bucket that keeps current values; on one that evicts them they
//! diverge, and the pre-fix resync — which defined correct as "matches
//! NATS" — deleted valid keys to match. `FoldStatus` judges against the
//! truth: `StaleKey` holds a really-deleted key, `Lost` lacks a write the
//! truth has (dropped a key that aged out, or missed an aged-out gap write).
//!
//! The empirical coupling runs both directions: the legacy configuration's
//! `sometimes` hazards are the interleavings `tests/multi_export.rs` drives
//! against the real code, where the shipped protocol's `always` theorems are
//! asserted as outcomes.
//!
//! ## What is deliberately NOT modeled, and why that is sound
//!
//! - **The export lease.** The lease only ever REMOVES interleavings (its own
//!   docs: "a work-deduplication optimization, never a correctness gate").
//!   Exporters here act with no coordination at all, which checks a strict
//!   SUPERSET of the behaviors any lease implementation (any ttl, any clock
//!   skew, any takeover policy) can produce. Every `always` property proven
//!   here therefore holds a fortiori with the lease present. This removes
//!   clock skew from the proof obligation entirely.
//! - **Artifact bytes.** An artifact is its identity `(node, cursor, key-set)`;
//!   "embedded manifest equals sibling manifest byte-for-byte" is modeled as
//!   identity equality. Axiom: manifest bytes are equal iff they describe the
//!   same artifact content (manifests embed a BLAKE3 digest per payload file;
//!   collision resistance). Under that axiom the model's cross-check and the
//!   code's byte-compare accept exactly the same pairs.
//!
//! ## Axioms (the environment obligations the proof is relative to)
//!
//! 1. Object PUTs are atomic per object, and conditional puts (create-only,
//!    compare-and-swap on the object version) have one winner per slot —
//!    S3/GCS/Azure/MinIO semantics, verified against live MinIO by
//!    `tests/transport_s3.rs`. (`object_store`'s LocalFileSystem lacks CAS;
//!    `swap_pointer` FAILS CLOSED there unless the caller explicitly opts in
//!    via `with_non_atomic_pointer_fallback()` — `file://` is a dev
//!    convenience outside the verified envelope, by signed waiver only.)
//! 2. BLAKE3 collision resistance (manifest equality ⟺ content identity).
//! 3. Cursor expiry is DETECTED: the model's `floor` is the stream's
//!    `first_sequence`, and a resume below it takes the expired path, never
//!    a silent skip. NATS does NOT provide this by erroring — it silently
//!    clamps a below-head start sequence (pinned by
//!    `tests/resync.rs::nats_silently_clamps_resume_below_first_seq`) — so
//!    the code provides it proactively via `check_resume_window`
//!    (first_sequence comparison), verified end-to-end against a live
//!    nats-server by `tests/resync.rs`.
//! 4. The fold is the KV-mirror `SnapshotStore` (last-write-wins per key);
//!    `import` verifies every declared file hash and rejects extras
//!    (empirical tier: tampered-artifact and multi-SST round-trip tests).
//!    NATS KV CAS semantics for the lease are unneeded here (see above); the
//!    lease layer is verified by `integration.rs` contention tests.
//! 5. Exporters are correct folds: an artifact holds `key_at(cursor)`.
//!    DISCHARGED by `tests/model_fleet.rs`, where the exporters are watchers
//!    that themselves expire, restore, start cursor-less, and publish their
//!    actual folds, and the checker proves every published artifact is the
//!    truth at its cursor.
//! 6. Retention outlives consumer lag — NARROWED to prefix-scoped watches,
//!    the fresh full watch's initial history scan, and the key-listing
//!    repair's window between its listing and its re-list
//!    (`tests/model_repair.rs` drops it and reaches that trace; the artifact
//!    restore does not depend on it). The ALL-scope resume
//!    watch (steady-state operation) no longer relies on it: the live floor
//!    guard (`tests/model_live_watch.rs`, `stream_watch_floor_guarded`)
//!    fail-stops on in-band evidence of retention overrunning the consumer
//!    and routes into this model's verified resume → expiry → resync repair
//!    path. Prefix scopes deliver sparse revisions by design and cannot
//!    distinguish benign from hazardous eviction client-side; for them the
//!    operating requirement stands: configure retention in hours, not
//!    seconds.
//!
//! ## Bounds and the small-scope argument
//!
//! Default: 2 exporters (a const-generic parameter), revisions ≤ 3, one
//! importer, one sentinel key (initial put, at most one delete, at most one
//! later write, and — on an evicting bucket — aging out) — and **unbounded
//! rounds**: a
//! publisher re-enters the pipeline whenever it has applied past its last
//! publish (`NextRound`), so every theorem quantifies over repeated rounds,
//! including a node racing its own previous publish. Every hazard class
//! needs at most: two distinct cursors (regression), one crash window (torn
//! pair), one delete + floor advance (stale key), one write + age-out + floor
//! advance (lost write) — and every `sometimes` witness fails loudly if a
//! bound ever clips its scenario. The evicting configuration's deep tier
//! (revisions ≤ 4, ~124M unique states) is ignored by default.
//!
//! The ignored deep tier (release mode, scheduled runs) pushes both axes:
//! revisions ≤ 5 (~154M unique states) and THREE exporters (~95M unique
//! states — three-way publish races, double-stalled rounds behind a
//! takeover, prune racing two concurrent uploads), plus the legacy layout at
//! fleet size 3 proving the hazards stay reachable at scale.
//!
//! ## Liveness
//!
//! Cycle-proof and Stateright-native: the `every maximal run ends with a
//! completed, synced bootstrap` invariant recomputes the enabled-action set
//! per state — a state with no enabled actions is the end of a maximal
//! execution and must hold a finished, converged bootstrap. Retry loops are
//! cycles, never terminal, so they cannot satisfy it vacuously; a protocol
//! change that could strand the importer (deadlock, unrecoverable failure
//! state, bootstrap that can never finish) fails this invariant.

use stateright::{Checker, Model, Property};

/// The convergence theorem's name (looked up by the mutation tests).
const DIVERGENCE_THEOREM: &str =
    "bootstrap never silently diverges from every write minus every real delete";

/// Default bucket-revision bound (1..=MAX_REV). 3 suffices for every hazard
/// class and witness (two distinct export cursors plus a delete revision —
/// each `sometimes` property fails loudly if a bound ever clips its
/// scenario). The ignored deep tests push the bound and the fleet size
/// further in release mode.
const MAX_REV: u8 = 3;

/// An export artifact's identity: who exported, at which applied cursor, and
/// the sentinel key's value at that cursor (the revision of the put it holds,
/// `None` if deleted). Two artifacts are byte-identical iff this identity is
/// equal (BLAKE3 axiom).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Artifact {
    node: u8,
    cursor: u8,
    key: Option<u8>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum ExporterPc {
    Idle,
    /// Fold exported to local scratch at this identity; nothing uploaded yet.
    Exported(Artifact),
    /// Payload object uploaded; sibling manifest / pointer not yet published.
    /// Crashing HERE is the torn-pair window of the current protocol.
    PayloadUp(Artifact),
    /// Published at this cursor. `NextRound` re-enters the pipeline once
    /// the replica has applied past it — fleets round forever.
    Done(u8),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum FoldStatus {
    /// The bootstrapped fold holds every write minus every real delete (tail
    /// replay, a repaired fallback, or a fallback the re-list happens to
    /// cover).
    Synced,
    /// Silent divergence: the fold holds a key that was really deleted, and
    /// nothing will ever remove it (expired cursor + evicted marker + no
    /// resync).
    StaleKey,
    /// Silent divergence: the fold lacks a write the truth has — a key that
    /// aged out of NATS (never deleted) was dropped, or a write made during
    /// the gap aged out before the fold could see it.
    Lost,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum ImporterPc {
    Start,
    /// Read the sibling manifest (or pointer) — first of the two reads.
    GotManifest(Artifact),
    /// Cross-check passed; fold installed at the artifact's cursor.
    Imported(Artifact),
    /// Cross-check FAILED (embedded manifest ≠ sibling): the torn pair was
    /// detected and rejected. Retry returns to Start.
    CrossCheckFailed,
    /// Pointer-swap only: the held pointer's payload was PRUNED between the
    /// pointer read and the payload fetch (in code: download's content
    /// address dereferences to NotFound → `ArtifactInvalid`). Detected,
    /// never silent; Retry re-reads the (necessarily newer) pointer.
    FetchMissed,
    /// Resume ran; final verdict on this bootstrap.
    Resumed(Artifact, FoldStatus),
    /// Resume hit an expired cursor on a bucket that evicts current values:
    /// the fold (at this artifact) must be restored from a newer published
    /// artifact still inside retention. Fail-stop until one exists — in code
    /// the watch fails and the caller's restart retries; here `RestoreRead`
    /// is simply disabled until the pointer qualifies.
    Restoring(Artifact),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St<const N: usize> {
    /// Bucket high-water revision.
    head: u8,
    /// Revision at which the sentinel key was deleted, if it was.
    delete_rev: Option<u8>,
    /// Revision of the sentinel's later write (`PutKey`), if any. Its initial
    /// put is revision 0, before every replica's first cursor.
    reput_rev: Option<u8>,
    /// `EvictsCurrent` only: retention evicted the sentinel's CURRENT value
    /// (it was never deleted; the bucket just stopped listing it).
    aged: bool,
    /// Stream retention floor: resuming from cursor < floor is CursorExpired;
    /// a delete marker at rev ≤ floor has been evicted.
    floor: u8,
    /// Each replica's applied cursor (≤ head).
    applied: [u8; N],
    exporters: [ExporterPc; N],
    /// CURRENT protocol: the payload object — an atomic LWW register.
    payload: Option<Artifact>,
    /// CURRENT: the sibling manifest LWW register. FIXED: the pointer object,
    /// published only via monotonic conditional swap.
    manifest: Option<Artifact>,
    /// FIXED protocol: content-addressed payload objects — write-once, never
    /// overwritten. (Unused in the current protocol.)
    uploaded: std::collections::BTreeSet<Artifact>,
    /// Latched when a manifest/pointer publish replaced a strictly newer one.
    regressed: bool,
    /// FIXED: latched when the monotonic swap refused an older publish —
    /// vacuity witness that the guard actually fires within the bounds.
    refused: bool,
    /// Latched when the importer's fold was restored from a newer artifact
    /// (vacuity witness for the restore theorem).
    restored: bool,
    /// `TrustRestoredCursor` only: the next resume skips its window check.
    trust_next_resume: bool,
    importer: ImporterPc,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands in the bucket.
    Churn,
    /// The sentinel key is deleted (consumes a revision).
    DeleteKey,
    /// The sentinel key is written again (consumes a revision): an update, or
    /// a re-create after the delete.
    PutKey,
    /// `EvictsCurrent` only: retention evicts the sentinel's current value.
    AgeOut,
    /// Retention floor advances by one.
    Compact,
    /// Replica n applies the next revision.
    Apply(u8),
    /// Replica n snapshots its fold at its current applied cursor.
    Export(u8),
    /// Replica n uploads its payload object.
    UploadPayload(u8),
    /// Replica n publishes its sibling manifest (current) / swaps the
    /// pointer (fixed).
    Publish(u8),
    /// Replica n crashes mid-round and restarts idle.
    Crash(u8),
    /// Replica n starts a fresh round after a successful publish (enabled
    /// once it has applied past its last published cursor).
    NextRound(u8),
    ReadManifest,
    ReadPayload,
    /// Pointer-swap only: delete every payload the current pointer does not
    /// reference — the harshest prune (zero grace, fires whenever anything
    /// is unreferenced), a SUPERSET of every real grace-period timing.
    Prune,
    Retry,
    Resume,
    /// `Restoring` only: read the current pointer and, if
    /// `protocol::restore_allowed` accepts it, import it. Collapses the
    /// payload fetch: the CURRENT pointer's target is always fetchable
    /// (`pointer target always fetchable`), and the stale-read fetch miss is
    /// already explored on the initial import.
    RestoreRead,
    /// Degrade mode only: the resume completed but its resync failed
    /// mid-flight and the code warned-and-continued re-list-only.
    ResumeResyncDegraded,
}

/// How the bootstrapping node handles the cursor-expired stale-key resync.
/// What the bucket's retention can evict.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BucketRetention {
    /// Only superseded history and delete markers (`discard: new`, no
    /// `max_age`): a key missing from the bucket was deleted.
    KeepsCurrent,
    /// Current values too (`max_age`, `discard: old`): a key missing from the
    /// bucket may simply have aged out.
    EvictsCurrent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResyncMode {
    /// No reader wired (`watch_applied(reader: None, ..)`): expiry falls back
    /// re-list-only by explicit caller choice.
    None,
    /// Reader wired, but a resync I/O failure DEGRADES to re-list-only with a
    /// warning — the code's semantics BEFORE the fail-stop fix. The checker
    /// proves this breaks the convergence theorem, which is why the code
    /// changed.
    Degrade,
    /// Reader wired, resync failure fails the watch (the caller's restart
    /// retries resume → expiry → resync) — `applied.rs` as it ships now. A
    /// failed attempt changes nothing observable, so in the model it is the
    /// `Resume` action simply remaining enabled; only a SUCCESSFUL resync
    /// completes the bootstrap.
    FailStop,
}

/// Deliberately broken guard variants. Each mutation test substitutes one
/// and asserts the checker PRODUCES A COUNTEREXAMPLE — proving every shared
/// kernel guard is load-bearing for the theorems, not incidentally safe.
/// (The unmutated configurations call the kernels themselves, so a kernel
/// regression fails the main theorems directly; these prove the properties
/// would catch it.)
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    /// Pointer publish ignores the monotonic guard (last-write-wins).
    LwwPointer,
    /// Prune ignores the strictly-below-the-pointer rule (age-only — the
    /// rule the checker originally caught dangling).
    PruneAgeOnly,
    /// Expiry detection removed: an expired resume behaves like NATS's
    /// silent clamp (gap skipped, resync never triggered) — the live bug
    /// `tests/resync.rs` pinned.
    SilentClamp,
    /// The pre-fix repair: the key-listing diff even on a bucket that evicts
    /// current values (absence from NATS taken as deletion). The bug
    /// `tests/eviction.rs` reproduced live.
    RelistOnEvicting,
    /// The restore accepts any published artifact (no
    /// `protocol::restore_allowed`) — the resume window re-check still runs.
    NoRestoreGuard,
    /// The resume after a restore skips its window check, trusting the
    /// restored cursor (the restore guard still runs).
    TrustRestoredCursor,
}

/// Model parameters: which protocol, the importer's resync mode, an optional
/// guard mutation, and the revision bound.
#[derive(Clone)]
struct SnapshotProtocol<const N: usize> {
    pointer_swap: bool,
    resync: ResyncMode,
    retention: BucketRetention,
    mutation: Mutation,
    max_rev: u8,
}

impl<const N: usize> SnapshotProtocol<N> {
    fn shipped(resync: ResyncMode) -> Self {
        Self {
            pointer_swap: true,
            resync,
            retention: BucketRetention::KeepsCurrent,
            mutation: Mutation::None,
            max_rev: MAX_REV,
        }
    }

    /// The shipped protocol on a bucket whose retention evicts current
    /// values.
    fn evicting(resync: ResyncMode) -> Self {
        Self {
            retention: BucketRetention::EvictsCurrent,
            ..Self::shipped(resync)
        }
    }

    fn legacy(resync: ResyncMode) -> Self {
        Self {
            pointer_swap: false,
            ..Self::shipped(resync)
        }
    }

    /// THE TRUTH: the sentinel's value after every write and every real
    /// delete up to `cursor` — the revision of the put it holds, `None` if
    /// deleted. Initial put at 0; the delete and the later write apply in
    /// revision order.
    fn key_at(s: &St<N>, cursor: u8) -> Option<u8> {
        let mut key = Some(0);
        let mut events = [
            s.delete_rev.map(|d| (d, None)),
            s.reput_rev.map(|r| (r, Some(r))),
        ];
        events.sort();
        for (rev, value) in events.into_iter().flatten() {
            if rev <= cursor {
                key = value;
            }
        }
        key
    }

    /// What NATS still lists for the sentinel (its live-key listing, and the
    /// current value a re-list delivers): the truth, minus a current value
    /// retention evicted.
    fn listed(s: &St<N>) -> Option<u8> {
        Self::key_at(s, s.head).filter(|_| !s.aged)
    }

    fn status(fold: Option<u8>, truth: Option<u8>) -> FoldStatus {
        match (fold, truth) {
            (f, t) if f == t => FoldStatus::Synced,
            (Some(_), None) => FoldStatus::StaleKey,
            _ => FoldStatus::Lost,
        }
    }

    /// The expiry guard — THE SHARED KERNEL (`slipstream::protocol`), the
    /// same function `nats.rs`'s resume paths execute. The model's retention
    /// floor is the stream's `first_sequence - 1`, so the first retained
    /// sequence is `floor + 1`.
    fn resume_ok(&self, s: &St<N>, a: Artifact) -> bool {
        slipstream::protocol::resume_window_ok(a.cursor as u64, s.floor as u64 + 1)
    }

    /// Outcome of an expired-cursor fallback WITHOUT a working resync: the
    /// re-list delivers the current values NATS still holds, so a key deleted
    /// during the gap is covered iff its delete marker survived retention
    /// (delete_rev > floor, and no later write superseded it — the fallback
    /// watch replays retained history, markers included); anything else the
    /// re-list can't deliver (an aged-out current value) leaves the
    /// artifact's value in place.
    fn relist_only_status(s: &St<N>, a: Artifact) -> FoldStatus {
        let truth = Self::key_at(s, s.head);
        let marker_delivered = truth.is_none() && s.delete_rev.is_some_and(|d| d > s.floor);
        let fold = if Self::listed(s).is_some() {
            truth
        } else if marker_delivered {
            None
        } else {
            a.key
        };
        Self::status(fold, truth)
    }

    /// Outcome of the key-listing resync: synthetic deletes for whatever NATS
    /// no longer lists, then the re-list. The fold becomes the LISTING —
    /// the truth only on a bucket that keeps current values.
    fn relist_status(s: &St<N>) -> FoldStatus {
        Self::status(Self::listed(s), Self::key_at(s, s.head))
    }
}

impl<const N: usize> Model for SnapshotProtocol<N> {
    type State = St<N>;
    type Action = Act;

    fn init_states(&self) -> Vec<St<N>> {
        vec![St {
            head: 0,
            delete_rev: None,
            reput_rev: None,
            aged: false,
            floor: 0,
            applied: [0; N],
            exporters: [ExporterPc::Idle; N],
            payload: None,
            manifest: None,
            uploaded: Default::default(),
            regressed: false,
            refused: false,
            restored: false,
            trust_next_resume: false,
            importer: ImporterPc::Start,
        }]
    }

    fn actions(&self, s: &St<N>, acts: &mut Vec<Act>) {
        if s.head < self.max_rev {
            acts.push(Act::Churn);
            if s.delete_rev.is_none() {
                acts.push(Act::DeleteKey);
            }
            if s.reput_rev.is_none() {
                acts.push(Act::PutKey);
            }
        }
        // FIFO eviction: a current value ages out only once everything up to
        // its revision has (the initial put, at 0, predates every message).
        if self.retention == BucketRetention::EvictsCurrent
            && !s.aged
            && Self::key_at(s, s.head).is_some_and(|w| w == 0 || s.floor >= w)
        {
            acts.push(Act::AgeOut);
        }
        if s.floor < s.head {
            acts.push(Act::Compact);
        }
        for n in 0..N as u8 {
            if s.applied[n as usize] < s.head {
                acts.push(Act::Apply(n));
            }
            match s.exporters[n as usize] {
                ExporterPc::Idle if s.applied[n as usize] >= 1 => acts.push(Act::Export(n)),
                // A new round once the replica has applied past its last
                // publish — fleets round forever, so every theorem must hold
                // across repeated rounds, including a node racing its OWN
                // previous publish.
                ExporterPc::Done(c) if s.applied[n as usize] > c => {
                    acts.push(Act::NextRound(n));
                }
                ExporterPc::Exported(_) => {
                    acts.push(Act::UploadPayload(n));
                    acts.push(Act::Crash(n));
                }
                ExporterPc::PayloadUp(_) => {
                    acts.push(Act::Publish(n));
                    acts.push(Act::Crash(n));
                }
                _ => {}
            }
        }
        if self.pointer_swap
            && let Some(m) = s.manifest
            && s.uploaded.iter().any(|a| a.cursor < m.cursor)
        {
            acts.push(Act::Prune);
        }
        match s.importer {
            ImporterPc::Start if s.manifest.is_some() => acts.push(Act::ReadManifest),
            ImporterPc::GotManifest(_) => acts.push(Act::ReadPayload),
            ImporterPc::CrossCheckFailed | ImporterPc::FetchMissed => acts.push(Act::Retry),
            ImporterPc::Imported(a) => {
                acts.push(Act::Resume);
                // Under Degrade semantics an expired-cursor resume may also
                // complete with its resync having FAILED mid-flight (I/O
                // error → warn → re-list only). Distinct action: the
                // nondeterminism is the scheduler's, not the property's.
                if self.resync == ResyncMode::Degrade && !self.resume_ok(s, a) {
                    acts.push(Act::ResumeResyncDegraded);
                }
            }
            // The restore guard — THE SHARED KERNEL
            // (`slipstream::protocol::restore_allowed`), the same function
            // `applied.rs`'s restore path executes (`check_restore`): the
            // pointer must be ahead of the local fold and still inside
            // retention. Otherwise fail-stop: no enabled action until the
            // fleet publishes one that is.
            ImporterPc::Restoring(a) => {
                if let Some(m) = s.manifest
                    && (self.mutation == Mutation::NoRestoreGuard
                        || slipstream::protocol::restore_allowed(
                            m.cursor as u64,
                            a.cursor as u64,
                            s.floor as u64 + 1,
                        ))
                {
                    acts.push(Act::RestoreRead);
                }
            }
            _ => {}
        }
    }

    fn next_state(&self, s: &St<N>, a: Act) -> Option<St<N>> {
        let mut s = s.clone();
        match a {
            Act::Churn => s.head += 1,
            Act::DeleteKey => {
                s.head += 1;
                s.delete_rev = Some(s.head);
            }
            Act::PutKey => {
                s.head += 1;
                s.reput_rev = Some(s.head);
                s.aged = false; // a fresh current value
            }
            Act::AgeOut => s.aged = true,
            Act::Compact => s.floor += 1,
            Act::Apply(n) => s.applied[n as usize] += 1,
            Act::Export(n) => {
                let cursor = s.applied[n as usize];
                s.exporters[n as usize] = ExporterPc::Exported(Artifact {
                    node: n,
                    cursor,
                    key: Self::key_at(&s, cursor),
                });
            }
            Act::UploadPayload(n) => {
                let ExporterPc::Exported(a) = s.exporters[n as usize] else {
                    return None;
                };
                if self.pointer_swap {
                    // Content-addressed: write-once, no register to clobber.
                    s.uploaded.insert(a);
                } else {
                    // Atomic LWW overwrite of the shared payload key.
                    s.payload = Some(a);
                }
                s.exporters[n as usize] = ExporterPc::PayloadUp(a);
            }
            Act::Publish(n) => {
                let ExporterPc::PayloadUp(a) = s.exporters[n as usize] else {
                    return None;
                };
                if self.pointer_swap {
                    // THE monotonic guard — the SHARED KERNEL
                    // (`slipstream::protocol::pointer_publish_allowed`), the
                    // same function `transport::swap_pointer` executes. The
                    // LwwPointer mutation bypasses it to prove the checker
                    // catches a broken guard.
                    let observed = match s.manifest {
                        None => slipstream::protocol::PointerState::Absent,
                        Some(m) => slipstream::protocol::PointerState::Present {
                            rank: Some(m.cursor as u64),
                        },
                    };
                    let allowed = self.mutation == Mutation::LwwPointer
                        || slipstream::protocol::pointer_publish_allowed(
                            &observed,
                            a.cursor as u64,
                        );
                    if allowed {
                        if let Some(m) = s.manifest
                            && m.cursor > a.cursor
                        {
                            s.regressed = true;
                        }
                        s.manifest = Some(a);
                    } else {
                        s.refused = true;
                    }
                } else {
                    // Atomic LWW overwrite — an older round's publish lands.
                    if let Some(m) = s.manifest
                        && m.cursor > a.cursor
                    {
                        s.regressed = true;
                    }
                    s.manifest = Some(a);
                }
                s.exporters[n as usize] = ExporterPc::Done(a.cursor);
            }
            Act::Crash(n) => {
                // Mid-round crash: local scratch artifact lost, whatever was
                // already uploaded stays. The node restarts idle.
                s.exporters[n as usize] = ExporterPc::Idle;
            }
            Act::NextRound(n) => {
                s.exporters[n as usize] = ExporterPc::Idle;
            }
            Act::ReadManifest => {
                let m = s.manifest?;
                s.importer = ImporterPc::GotManifest(m);
            }
            Act::ReadPayload => {
                let ImporterPc::GotManifest(m) = s.importer else {
                    return None;
                };
                if self.pointer_swap {
                    // Fetch the payload at the pointer's content address.
                    // Present unless a prune raced a STALE pointer read (the
                    // current pointer's target is never pruned —
                    // `pointer_target_always_fetchable`); a miss is a
                    // detected NotFound → retry, never wrong data.
                    if s.uploaded.contains(&m) {
                        s.importer = ImporterPc::Imported(m);
                    } else {
                        s.importer = ImporterPc::FetchMissed;
                    }
                } else {
                    // The cross-check: embedded manifest (inside the payload
                    // tar) vs the sibling object, byte equality ⟺ identity.
                    match s.payload {
                        Some(p) if p == m => s.importer = ImporterPc::Imported(p),
                        _ => s.importer = ImporterPc::CrossCheckFailed,
                    }
                }
            }
            Act::Prune => {
                // THE prune guard — the SHARED KERNEL
                // (`slipstream::protocol::payload_prunable`), the same
                // function `ObjectStoreTransport::prune` executes; the
                // strictly-below rule and its dangling-pointer impossibility
                // argument live there. The first modeling attempt used
                // "everything the pointer doesn't reference" — and the
                // checker found the dangling trace, which is how the kernel
                // got its rule. PruneAgeOnly resurrects the broken rule to
                // prove the checker still catches it. Zero grace
                // (`aged_out: true`) is the harshest timing.
                let keep = s.manifest?;
                if self.mutation == Mutation::PruneAgeOnly {
                    s.uploaded.retain(|a| *a == keep);
                } else {
                    s.uploaded.retain(|a| {
                        !slipstream::protocol::payload_prunable(
                            Some(a.cursor as u64),
                            keep.cursor as u64,
                            *a == keep,
                            true,
                        )
                    });
                }
            }
            Act::Retry => s.importer = ImporterPc::Start,
            Act::Resume => {
                let ImporterPc::Imported(a) = s.importer else {
                    return None;
                };
                let trusted = std::mem::take(&mut s.trust_next_resume);
                if trusted && !self.resume_ok(&s, a) {
                    // The unchecked resume from a restored cursor that
                    // retention overran since the restore's check: NATS
                    // silently clamps, skipping the evicted gap.
                    s.importer = ImporterPc::Resumed(a, Self::relist_only_status(&s, a));
                } else if self.resume_ok(&s, a) {
                    // Window intact (shared kernel `resume_window_ok` — the
                    // same guard `nats.rs` executes): tail replay from the
                    // embedded cursor delivers every event past it (all
                    // retained: > a.cursor >= floor), so the fold reaches
                    // the truth. A current value that aged out has revision
                    // <= floor <= a.cursor, so the artifact already holds it.
                    s.importer = ImporterPc::Resumed(a, FoldStatus::Synced);
                } else if self.mutation == Mutation::SilentClamp {
                    // Expiry detection removed: the resume silently skips
                    // the gap (NATS's native clamp behavior) — deletes whose
                    // markers were evicted are lost and the resync never
                    // triggers, regardless of the resync mode.
                    s.importer = ImporterPc::Resumed(a, Self::relist_only_status(&s, a));
                } else {
                    // CursorExpired: the shared planner decides, under the
                    // shipped `ExpiryRepair::Auto` (or `None`, no repair).
                    // `RelistOnEvicting` forges the listing as the truth.
                    use slipstream::protocol::{
                        RepairMode, RepairPlan, listing_is_truth, plan_repair,
                    };
                    let mode = match self.resync {
                        ResyncMode::None => RepairMode::None,
                        ResyncMode::FailStop | ResyncMode::Degrade => RepairMode::Auto,
                    };
                    let truth = self.mutation == Mutation::RelistOnEvicting
                        || listing_is_truth(
                            self.retention == BucketRetention::EvictsCurrent,
                            s.floor as u64 + 1,
                        );
                    s.importer = match plan_repair(mode, Some(truth)) {
                        RepairPlan::ReListOnly => {
                            ImporterPc::Resumed(a, Self::relist_only_status(&s, a))
                        }
                        // On a bucket that evicts current values, NATS's
                        // listing can't separate deleted from aged out, so
                        // the fold is restored from an artifact.
                        RepairPlan::Restore => ImporterPc::Restoring(a),
                        // Full re-list + a SUCCESSFUL key-listing resync:
                        // live keys diffed against the fold, vanished keys
                        // get synthetic deletes. (Under FailStop a failed
                        // resync fails the watch and changes nothing — this
                        // action stays enabled for the retry. Under Degrade
                        // the failed-resync outcome is ResumeResyncDegraded.)
                        // Sound exactly when the listing is the truth.
                        RepairPlan::Relist => ImporterPc::Resumed(a, Self::relist_status(&s)),
                        RepairPlan::RefuseRelist => unreachable!("Auto never refuses"),
                    };
                }
            }
            Act::RestoreRead => {
                let ImporterPc::Restoring(a) = s.importer else {
                    return None;
                };
                let m = s.manifest?;
                if self.mutation != Mutation::NoRestoreGuard
                    && !slipstream::protocol::restore_allowed(
                        m.cursor as u64,
                        a.cursor as u64,
                        s.floor as u64 + 1,
                    )
                {
                    return None;
                }
                s.trust_next_resume = self.mutation == Mutation::TrustRestoredCursor;
                // The in-scope fold becomes the artifact's; the watch then
                // resumes from its cursor (the next `Resume`, which re-checks
                // the window — retention can still overrun it, and the
                // restore runs again for a newer pointer).
                s.restored = true;
                s.importer = ImporterPc::Imported(m);
            }
            Act::ResumeResyncDegraded => {
                let ImporterPc::Imported(a) = s.importer else {
                    return None;
                };
                if self.resync != ResyncMode::Degrade || self.resume_ok(&s, a) {
                    return None;
                }
                // The pre-fix code path: resync I/O failed, one warn line,
                // fallback proceeds re-list-only.
                s.importer = ImporterPc::Resumed(a, Self::relist_only_status(&s, a));
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = Vec::new();

        if self.pointer_swap {
            // ---- The SHIPPED protocol's theorems (transport.rs as of 0.6;
            // the empirical twins in tests/multi_export.rs drive these same
            // interleavings against the real code). -------------------------
            props.push(Property::<Self>::always(
                "published cursor never regresses",
                |_, s| !s.regressed,
            ));
            props.push(Property::<Self>::always(
                "monotone pointer: importer never observes a cursor drop",
                |_, s| match (s.importer, s.manifest) {
                    // Once the pointer holds m, any importer state derived
                    // from an earlier pointer read has cursor <= m.cursor.
                    (ImporterPc::GotManifest(g), Some(m))
                    | (ImporterPc::Imported(g), Some(m))
                    | (ImporterPc::Restoring(g), Some(m))
                    | (ImporterPc::Resumed(g, _), Some(m)) => g.cursor <= m.cursor,
                    _ => true,
                },
            ));
            props.push(Property::<Self>::always(
                "cross-check never fires (torn pair structurally impossible)",
                |_, s| s.importer != ImporterPc::CrossCheckFailed,
            ));
            props.push(Property::<Self>::always(
                "pointer target always fetchable (write-once before publish)",
                |_, s| s.manifest.is_none_or(|m| s.uploaded.contains(&m)),
            ));
            // Vacuity witness: the monotonic guard is exercised, not just
            // present — the slow-exporter interleaving reaches it and is
            // refused (the model twin of the clobber hazard, now prevented).
            props.push(Property::<Self>::sometimes(
                "the swap refuses an older publish (clobber attempt occurs and is stopped)",
                |_, s| s.refused,
            ));
            // Prune racing a stale pointer read: the importer's fetch can
            // MISS (detected NotFound → retry) but never import wrong data —
            // and the miss is reachable, so the prune action is genuinely
            // exercised, not vacuously safe.
            props.push(Property::<Self>::sometimes(
                "a prune racing a stale pointer read forces a detected retry",
                |_, s| s.importer == ImporterPc::FetchMissed,
            ));
            props.push(Property::<Self>::always(
                "a fetch miss only happens on a stale pointer read, never the current one",
                |_, s| {
                    s.importer != ImporterPc::FetchMissed
                        || matches!(s.manifest, Some(m) if s.uploaded.contains(&m))
                },
            ));
        } else {
            // ---- The LEGACY two-register layout (pre-0.6): the hazards the
            // checker must re-derive. Kept as the machine-checked record of
            // WHY the protocol changed — these are the interleavings
            // tests/multi_export.rs drives against the real code, where the
            // shipped protocol now refuses them. If one becomes unreachable
            // the model has drifted from the mechanism and this fails loudly.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: published artifact regresses (slow-exporter clobber)",
                |_, s| s.regressed,
            ));
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: importer observes a torn pair (detected, bootstrap outage)",
                |_, s| s.importer == ImporterPc::CrossCheckFailed,
            ));
            props.push(Property::<Self>::sometimes(
                "regression is non-fatal: a post-regression bootstrap still converges",
                |_, s| {
                    s.regressed && matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Synced))
                },
            ));
        }

        // ---- Detection soundness, BOTH protocols: every install is exactly
        // one exporter's artifact at that exporter's exported state. With the
        // BLAKE3 axiom this is "no silent corruption" — a torn pair can only
        // park the importer in CrossCheckFailed, never in Imported. ---------
        props.push(Property::<Self>::always(
            "no mixed import: an installed fold is one exporter's exported state",
            |_, s| match s.importer {
                ImporterPc::Imported(a) | ImporterPc::Restoring(a) | ImporterPc::Resumed(a, _) => {
                    a.node < N as u8
                        && a.cursor >= 1
                        && a.cursor <= s.head
                        // The artifact's key-set is exactly the truth at its
                        // cursor — imports never Frankenstein.
                        && a.key == Self::key_at(s, a.cursor)
                }
                _ => true,
            },
        ));

        fn diverged<const N: usize>(s: &St<N>) -> bool {
            matches!(
                s.importer,
                ImporterPc::Resumed(_, FoldStatus::StaleKey | FoldStatus::Lost)
            )
        }
        match self.resync {
            ResyncMode::FailStop => {
                // ---- THE convergence claim: repair wired with fail-stop
                // error semantics (`applied.rs` as it ships, choosing like
                // `ExpiryRepair::Auto`): a bootstrap NEVER silently diverges
                // from every write minus every real delete — over every
                // interleaving of churn, writes, deletes, compaction, aging
                // out, crashes, racing exporters, and repair failures (a
                // failed repair fails the watch; only a successful one
                // completes a bootstrap). ---------------------------------
                props.push(Property::<Self>::always(DIVERGENCE_THEOREM, |_, s| {
                    !diverged(s)
                }));
            }
            ResyncMode::Degrade => {
                // ---- The PRE-FIX code semantics (resync failure → warn →
                // re-list only): the convergence theorem is FALSE — silent
                // divergence is reachable even with the reader wired. This
                // configuration is the machine-checked justification for the
                // fail-stop change; it must stay reachable so the model
                // remains an honest record of why.
                props.push(Property::<Self>::sometimes(
                    "HAZARD reachable: armed resync that degrades on error diverges silently",
                    |_, s| diverged(s),
                ));
            }
            ResyncMode::None => {
                // ---- No reader wired: divergence is REACHABLE — the resync
                // reader is a load-bearing requirement, not an optimization.
                // (Holds under the pointer-swap protocol too: the transport
                // fix does not remove the resync obligation.) ---------------
                props.push(Property::<Self>::sometimes(
                    "HAZARD reachable: silent stale-key divergence without resync",
                    |_, s| matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::StaleKey)),
                ));
                if self.retention == BucketRetention::EvictsCurrent {
                    // And the eviction hazard: the re-list can't deliver a
                    // gap write whose current value aged out.
                    props.push(Property::<Self>::sometimes(
                        "HAZARD reachable: the re-list alone misses a write that aged out",
                        |_, s| matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Lost)),
                    ));
                }
            }
        }

        if self.retention == BucketRetention::EvictsCurrent
            && self.resync == ResyncMode::FailStop
            && self.mutation == Mutation::None
        {
            // Vacuity witnesses for the theorem on an evicting bucket: it is
            // earned by restores that really happen past evicted current
            // values — an aged-out key that was never deleted, and an
            // aged-out later write — not by the hazard never arising.
            props.push(Property::<Self>::sometimes(
                "a restore resumes synced past a key that aged out but was never deleted",
                |_, s| {
                    s.restored
                        && s.aged
                        && matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Synced))
                },
            ));
            props.push(Property::<Self>::sometimes(
                "a restore recovers a later write that aged out",
                |_, s| {
                    s.restored
                        && s.aged
                        && s.reput_rev.is_some()
                        && s.delete_rev.is_none()
                        && matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Synced))
                },
            ));
        }

        // Vacuity witness for every always-property above: bootstraps really
        // complete in this configuration (Imported and Resumed are reachable,
        // so the invariants quantify over live states, not an empty set).
        props.push(Property::<Self>::sometimes(
            "a bootstrap completes and resumes synced",
            |_, s| matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Synced)),
        ));

        // Multi-round vacuity witness: a node that already published is back
        // in the pipeline (only node n publishes artifacts with node == n, so
        // pointer-by-n + n mid-flight means a SECOND round is genuinely
        // explored — every theorem above quantifies over repeated rounds).
        props.push(Property::<Self>::sometimes(
            "a publisher runs a second round against its own previous publish",
            |_, s| {
                s.manifest.is_some_and(|m| {
                    matches!(
                        s.exporters[m.node as usize],
                        ExporterPc::Exported(_) | ExporterPc::PayloadUp(_)
                    )
                })
            },
        ));

        if self.resync == ResyncMode::FailStop && self.mutation == Mutation::None {
            // ---- Terminal liveness, Stateright-native and cycle-proof:
            // recompute the enabled-action set inside the invariant — a
            // state with NO enabled actions is a maximal execution's end,
            // and every such state must hold a COMPLETED, SYNCED bootstrap.
            // No run can end with the importer stuck, failed, or diverged;
            // retry loops are cycles (never terminal), so they cannot
            // satisfy this vacuously.
            props.push(Property::<Self>::always(
                "every maximal run ends with a completed, synced bootstrap",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty()
                        || matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::Synced))
                },
            ));
        }

        props
    }
}

fn run<const N: usize>(
    model: SnapshotProtocol<N>,
    label: &str,
) -> impl Checker<SnapshotProtocol<N>> {
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count(),
    );
    checker
}

fn check<const N: usize>(model: SnapshotProtocol<N>, label: &str) {
    run(model, label).assert_properties();
}

/// THE SHIPPED CONFIGURATION (pointer-swap transport + fail-stop resync —
/// `transport.rs` and `applied.rs` as of 0.6, executing the SHARED protocol
/// kernels): regression, torn pairs, and dangling pointers are structurally
/// impossible, detection and convergence hold, over every interleaving
/// within bounds.
#[test]
fn shipped_protocol_pointer_swap_failstop_resync() {
    check(
        SnapshotProtocol::<2>::shipped(ResyncMode::FailStop),
        "shipped: pointer-swap + failstop",
    );
}

/// THE EVICTION FIX: on a bucket whose retention evicts current values, the
/// shipped repair (artifact restore, gated by `protocol::restore_allowed`)
/// keeps every write minus every real delete — aged-out keys kept, aged-out
/// gap writes recovered — and every maximal run still ends synced.
#[test]
fn evicting_bucket_restores_from_artifact() {
    check(
        SnapshotProtocol::<2>::evicting(ResyncMode::FailStop),
        "evicting: pointer-swap + restore",
    );
}

/// The pre-fix repair on an evicting bucket — the key-listing diff, which
/// takes "NATS no longer lists it" as "deleted" — MUST be caught: the checker
/// produces a fold that lost a write (a valid key deleted, or an aged-out gap
/// write never seen).
#[test]
fn mutation_relist_on_evicting_bucket_is_caught() {
    let mut model = SnapshotProtocol::<2>::evicting(ResyncMode::FailStop);
    model.mutation = Mutation::RelistOnEvicting;
    let checker = run(model, "mutation: relist on evicting bucket");
    let path = checker
        .discovery(DIVERGENCE_THEOREM)
        .expect("the checker must find the relist-on-evicting divergence");
    assert!(
        matches!(
            path.last_state().importer,
            ImporterPc::Resumed(_, FoldStatus::Lost)
        ),
        "the counterexample is a LOST write (not a stale key): {:?}",
        path.last_state().importer
    );
}

/// The restore's two defenses, separated. The window re-check on the resume
/// that follows a restore is LOAD-BEARING: retention can overrun the
/// restored cursor between the restore's check and the resume, and without
/// the re-check that gap is skipped silently. The restore guard
/// (`restore_allowed`) is FAIL-FAST: without it a stale artifact is imported
/// and the resume refuses it, so the theorem still holds (the guard turns a
/// pointless download-and-retry into an immediate, explained refusal).
#[test]
fn restore_defenses_resume_recheck_is_load_bearing_guard_is_fail_fast() {
    // One exporter suffices: the race is retention vs the importer.
    let mut model = SnapshotProtocol::<1>::evicting(ResyncMode::FailStop);
    model.mutation = Mutation::TrustRestoredCursor;
    let checker = run(model, "mutation: trust restored cursor");
    assert!(
        checker.discovery(DIVERGENCE_THEOREM).is_some(),
        "skipping the post-restore window check must be caught"
    );

    let mut model = SnapshotProtocol::<1>::evicting(ResyncMode::FailStop);
    model.mutation = Mutation::NoRestoreGuard;
    let checker = run(model, "mutation: no restore guard");
    assert!(
        checker.discovery(DIVERGENCE_THEOREM).is_none(),
        "the restore guard is fail-fast, not the safety gate: without it the resume re-check \
         still refuses a stale artifact"
    );
}

/// No repair on an evicting bucket: both divergence classes reachable.
#[test]
fn evicting_bucket_without_repair_diverges() {
    check(
        SnapshotProtocol::<2>::evicting(ResyncMode::None),
        "evicting: no repair",
    );
}

/// The shipped transport still requires the resync reader — the pointer-swap
/// fix does not absolve the convergence obligation.
#[test]
fn shipped_protocol_without_resync_still_diverges() {
    check(
        SnapshotProtocol::<2>::shipped(ResyncMode::None),
        "shipped: pointer-swap + no resync",
    );
}

/// LEGACY two-register transport (pre-0.6) with fail-stop resync: the
/// convergence and detection theorems held, but the clobber-regression and
/// torn-pair hazards are reachable — the machine-checked record of why the
/// transport moved to content-addressed payloads + a monotonic pointer.
#[test]
fn legacy_two_register_transport_has_reachable_hazards() {
    check(
        SnapshotProtocol::<2>::legacy(ResyncMode::FailStop),
        "legacy: two-register + failstop",
    );
}

/// LEGACY resync semantics (failure degrades to re-list with a warning):
/// silent divergence is reachable even with the reader wired — the
/// machine-checked reason `resync_stale_keys` now fails the watch instead.
#[test]
fn legacy_degrading_resync_diverges() {
    check(
        SnapshotProtocol::<2>::legacy(ResyncMode::Degrade),
        "legacy: degrade-on-error resync",
    );
}

/// No resync reader wired: silent stale-key divergence is reachable. This
/// pins the resync reader as a correctness requirement for the "stale, never
/// corrupt" claim, independent of the transport protocol.
#[test]
fn no_resync_reader_diverges() {
    check(
        SnapshotProtocol::<2>::legacy(ResyncMode::None),
        "legacy: no resync reader",
    );
}

// --- Mutation tests: every shared-kernel guard is load-bearing ----------------
// Each substitutes one deliberately broken guard and asserts the checker
// PRODUCES A COUNTEREXAMPLE for the theorem that guard carries. This proves
// the properties have teeth: a future regression in any kernel cannot pass
// the checker silently. (The unmutated configurations execute the kernels
// themselves, so a kernel regression also fails the main theorems directly.)

#[test]
fn mutation_lww_pointer_is_caught() {
    let mut model = SnapshotProtocol::<2>::shipped(ResyncMode::FailStop);
    model.mutation = Mutation::LwwPointer;
    let checker = run(model, "mutation: lww pointer");
    assert!(
        checker
            .discovery("published cursor never regresses")
            .is_some(),
        "the checker must produce a regression counterexample when the \
         monotonic publish guard is removed"
    );
}

#[test]
fn mutation_age_only_prune_is_caught() {
    let mut model = SnapshotProtocol::<2>::shipped(ResyncMode::FailStop);
    model.mutation = Mutation::PruneAgeOnly;
    let checker = run(model, "mutation: age-only prune");
    assert!(
        checker
            .discovery("pointer target always fetchable (write-once before publish)")
            .is_some(),
        "the checker must produce a dangling-pointer counterexample when \
         prune ignores the strictly-below rule (the original design bug)"
    );
}

#[test]
fn mutation_silent_clamp_is_caught() {
    let mut model = SnapshotProtocol::<2>::shipped(ResyncMode::FailStop);
    model.mutation = Mutation::SilentClamp;
    let checker = run(model, "mutation: silent clamp");
    assert!(
        checker.discovery(DIVERGENCE_THEOREM).is_some(),
        "the checker must produce a silent-divergence counterexample when \
         expiry detection is removed (the live NATS clamp bug class)"
    );
}

// --- Deep configurations for scheduled release runs ---------------------------
// `cargo test --release --test model -- --ignored --nocapture`

/// More revisions: each level multiplies the state space severalfold without
/// changing the mechanism set — slack against any witness being
/// bound-limited.
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_more_revisions() {
    let mut model = SnapshotProtocol::<2>::shipped(ResyncMode::FailStop);
    model.max_rev = 5;
    check(model, "deep: 2 exporters, rev <= 5");
}

/// The eviction axis at more revisions: restores racing more churn, aging,
/// and compaction.
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_evicting_more_revisions() {
    let mut model = SnapshotProtocol::<2>::evicting(ResyncMode::FailStop);
    model.max_rev = 4;
    check(model, "deep: evicting, 2 exporters, rev <= 4");
}

/// THREE exporters: the classic check that nothing in the protocol is
/// accidentally pairwise — three-way publish races, two stalled rounds
/// landing after a third's takeover, prune racing two concurrent uploads.
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_three_exporters() {
    let model = SnapshotProtocol::<3>::shipped(ResyncMode::FailStop);
    check(model, "deep: 3 exporters, rev <= 3");
}

/// Three exporters against the legacy layout: the hazards must STILL be
/// reachable at fleet size 3 (model honesty at larger scale).
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_three_exporters_legacy_hazards() {
    let model = SnapshotProtocol::<3>::legacy(ResyncMode::FailStop);
    check(model, "deep: 3 exporters, legacy two-register");
}
