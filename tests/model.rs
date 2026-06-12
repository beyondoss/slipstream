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
//!   the floor are evicted (the re-list cannot see them).
//! - A bootstrapping importer whose two reads (sibling manifest, then
//!   payload) interleave with all of the above, whose cross-check compares
//!   them, and whose post-import resume either replays the tail, or falls
//!   back (expired cursor) under one of THREE resync modes: reader not wired
//!   (`None`), reader wired with the pre-fix warn-and-continue failure
//!   semantics (`Degrade`), or reader wired with fail-stop failure semantics
//!   (`FailStop` — `applied.rs` as it ships). The checker proves `Degrade`
//!   breaks the convergence theorem, which is the machine-checked
//!   justification for the fail-stop change in `resync_stale_keys`.
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
//! 5. Retention outlives consumer lag — NARROWED to prefix-scoped watches
//!    and the fresh full watch's initial history scan. The ALL-scope resume
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
//! importer, one deletable sentinel key — and **unbounded rounds**: a
//! publisher re-enters the pipeline whenever it has applied past its last
//! publish (`NextRound`), so every theorem quantifies over repeated rounds,
//! including a node racing its own previous publish. Every hazard class
//! needs at most: two distinct cursors (regression), one crash window (torn
//! pair), one delete + floor advance (stale key) — and every `sometimes`
//! witness fails loudly if a bound ever clips its scenario.
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

/// Default bucket-revision bound (1..=MAX_REV). 3 suffices for every hazard
/// class and witness (two distinct export cursors plus a delete revision —
/// each `sometimes` property fails loudly if a bound ever clips its
/// scenario). The ignored deep tests push the bound and the fleet size
/// further in release mode.
const MAX_REV: u8 = 3;

/// An export artifact's identity: who exported, at which applied cursor, and
/// whether the sentinel key was still present at that cursor. Two artifacts
/// are byte-identical iff this identity is equal (BLAKE3 axiom).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Artifact {
    node: u8,
    cursor: u8,
    has_key: bool,
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
    /// The bootstrapped fold converges to the bucket (tail replay, or
    /// fallback with resync, or fallback where the re-list happens to cover).
    Synced,
    /// Silent divergence: the fold holds a key the bucket deleted, and
    /// nothing will ever remove it (expired cursor + evicted marker + no
    /// resync).
    StaleKey,
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
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St<const N: usize> {
    /// Bucket high-water revision.
    head: u8,
    /// Revision at which the sentinel key was deleted, if it was.
    delete_rev: Option<u8>,
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
    importer: ImporterPc,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands in the bucket.
    Churn,
    /// The sentinel key is deleted (consumes a revision).
    DeleteKey,
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
    /// Degrade mode only: the resume completed but its resync failed
    /// mid-flight and the code warned-and-continued re-list-only.
    ResumeResyncDegraded,
}

/// How the bootstrapping node handles the cursor-expired stale-key resync.
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
}

/// Model parameters: which protocol, the importer's resync mode, an optional
/// guard mutation, and the revision bound.
#[derive(Clone)]
struct SnapshotProtocol<const N: usize> {
    pointer_swap: bool,
    resync: ResyncMode,
    mutation: Mutation,
    max_rev: u8,
}

impl<const N: usize> SnapshotProtocol<N> {
    fn shipped(resync: ResyncMode) -> Self {
        Self {
            pointer_swap: true,
            resync,
            mutation: Mutation::None,
            max_rev: MAX_REV,
        }
    }

    fn legacy(resync: ResyncMode) -> Self {
        Self {
            pointer_swap: false,
            resync,
            mutation: Mutation::None,
            max_rev: MAX_REV,
        }
    }

    fn key_present_at(s: &St<N>, cursor: u8) -> bool {
        s.delete_rev.is_none_or(|d| cursor < d)
    }

    fn bucket_has_key(s: &St<N>) -> bool {
        s.delete_rev.is_none()
    }

    /// The expiry guard — THE SHARED KERNEL (`slipstream::protocol`), the
    /// same function `nats.rs`'s resume paths execute. The model's retention
    /// floor is the stream's `first_sequence - 1`, so the first retained
    /// sequence is `floor + 1`.
    fn resume_ok(&self, s: &St<N>, a: Artifact) -> bool {
        slipstream::protocol::resume_window_ok(a.cursor as u64, s.floor as u64 + 1)
    }

    /// Outcome of an expired-cursor fallback WITHOUT a working resync: the
    /// re-list delivers current values only, so a key deleted during the gap
    /// is covered iff its delete marker survived retention (delete_rev >
    /// floor — the fallback watch replays retained history, markers
    /// included).
    fn relist_only_status(s: &St<N>, a: Artifact) -> FoldStatus {
        let marker_evicted = s.delete_rev.is_some_and(|d| d <= s.floor);
        if a.has_key && !Self::bucket_has_key(s) && marker_evicted {
            FoldStatus::StaleKey
        } else {
            FoldStatus::Synced
        }
    }
}

impl<const N: usize> Model for SnapshotProtocol<N> {
    type State = St<N>;
    type Action = Act;

    fn init_states(&self) -> Vec<St<N>> {
        vec![St {
            head: 0,
            delete_rev: None,
            floor: 0,
            applied: [0; N],
            exporters: [ExporterPc::Idle; N],
            payload: None,
            manifest: None,
            uploaded: Default::default(),
            regressed: false,
            refused: false,
            importer: ImporterPc::Start,
        }]
    }

    fn actions(&self, s: &St<N>, acts: &mut Vec<Act>) {
        if s.head < self.max_rev {
            acts.push(Act::Churn);
            if s.delete_rev.is_none() {
                acts.push(Act::DeleteKey);
            }
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
            Act::Compact => s.floor += 1,
            Act::Apply(n) => s.applied[n as usize] += 1,
            Act::Export(n) => {
                let cursor = s.applied[n as usize];
                s.exporters[n as usize] = ExporterPc::Exported(Artifact {
                    node: n,
                    cursor,
                    has_key: Self::key_present_at(&s, cursor),
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
                let status = if self.resume_ok(&s, a) {
                    // Window intact (shared kernel `resume_window_ok` — the
                    // same guard `nats.rs` executes): tail replay from the
                    // embedded cursor. Any delete in the gap has its marker
                    // retained (delete_rev > a.cursor >= floor), so the
                    // replay delivers it.
                    FoldStatus::Synced
                } else if self.mutation == Mutation::SilentClamp {
                    // Expiry detection removed: the resume silently skips
                    // the gap (NATS's native clamp behavior) — deletes whose
                    // markers were evicted are lost and the resync never
                    // triggers, regardless of the resync mode.
                    Self::relist_only_status(&s, a)
                } else if self.resync != ResyncMode::None {
                    // CursorExpired -> full re-list + a SUCCESSFUL stale-key
                    // resync: live keys diffed against the fold, vanished
                    // keys get synthetic deletes. (Under FailStop a failed
                    // resync fails the watch and changes nothing — this
                    // action stays enabled for the retry. Under Degrade the
                    // failed-resync outcome is ResumeResyncDegraded.)
                    FoldStatus::Synced
                } else {
                    Self::relist_only_status(&s, a)
                };
                s.importer = ImporterPc::Resumed(a, status);
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
                ImporterPc::Imported(a) | ImporterPc::Resumed(a, _) => {
                    a.node < N as u8
                        && a.cursor >= 1
                        && a.cursor <= s.head
                        // The artifact's key-set is exactly the bucket state
                        // at its cursor — imports never Frankenstein.
                        && a.has_key == s.delete_rev.is_none_or(|d| a.cursor < d)
                }
                _ => true,
            },
        ));

        match self.resync {
            ResyncMode::FailStop => {
                // ---- THE convergence claim: resync wired with fail-stop
                // error semantics (`applied.rs` as it ships): bootstrap NEVER
                // silently diverges — over every interleaving of churn,
                // deletes, compaction, crashes, racing exporters, and resync
                // failures (a failed resync fails the watch; only a
                // successful one completes a bootstrap). -------------------
                props.push(Property::<Self>::always(
                    "bootstrap never silently diverges (stale is merely stale)",
                    |_, s| !matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::StaleKey)),
                ));
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
                    |_, s| matches!(s.importer, ImporterPc::Resumed(_, FoldStatus::StaleKey)),
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
            }
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
        checker
            .discovery("bootstrap never silently diverges (stale is merely stale)")
            .is_some(),
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
