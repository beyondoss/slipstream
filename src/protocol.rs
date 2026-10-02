//! Protocol decision kernels — every decision the snapshot export/import
//! protocol and the cursor-expiry repair rest on, extracted as pure functions
//! so the PRODUCTION code and the exhaustive model checkers (`tests/model*.rs`)
//! execute the **same logic**, not hand-synchronized copies of it.
//!
//! | kernel                     | production                          | models             |
//! |----------------------------|-------------------------------------|--------------------|
//! | [`pointer_publish_allowed`]| `transport::swap_pointer`           | `model.rs` `Act::Publish`, fleet `Publish` |
//! | [`payload_prunable`]       | `transport::ObjectStoreTransport::prune` | `model.rs` `Act::Prune` |
//! | [`resume_window_ok`]       | `nats` resume paths (`check_resume_window`), the live floor guard | every model |
//! | [`restore_allowed`]        | `repair::check_restore`             | `model.rs` `Act::RestoreRead`, live `GuardRepair`, repair-steps `WPlan`, fleet `Repair` |
//! | [`restore_ahead`]          | `repair::check_restore`, the main loop's re-check (`repair::fold_in`) | repair-steps restore reply |
//! | [`listing_is_truth`]       | `repair` (plan, cursor-less start), `Snapshot::stale_keys` | every model's view of retention |
//! | [`plan_repair`]            | `repair::plan`, `Snapshot::stale_keys` | expiry dispatch in every model |
//! | [`cursorless_start_needs_repair`] | `repair::run_watch`          | repair-steps `WStart`, fleet `Start` |
//! | [`restore_key`]            | `repair::restore_diff`              | repair-steps restore op |
//!
//! Because the model transitions call these very functions, a change to any
//! of them is re-verified against the full bounded state space on the next
//! model run — the decisions cannot drift from the proof. A model expresses
//! a mutation by forging a kernel's INPUT (the listing is "the truth" on an
//! evicting bucket, the fold looks empty, versions are invisible), never by
//! re-implementing the decision, and the mutation tests prove each one
//! load-bearing: the checker must produce a counterexample.
//!
//! Kernels operate on plain `u64` ranks (a [`WatchCursor`](crate::WatchCursor)'s
//! revision, with revisionless cursors ranked 0 by the callers) so they stay
//! free of I/O types and usable from the checker's `u8`-bounded state space.

/// What the publisher observed at the pointer key before deciding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerState {
    /// No pointer object exists — the slot is open (create-only publish).
    Absent,
    /// A pointer object exists. `rank` is its cursor's rank, or `None` when
    /// the object is unparseable — a corrupt pointer MUST be replaceable, or
    /// one bad write wedges publishing forever (the same rule as the export
    /// lease's corrupt-steal).
    Present {
        /// Rank of the existing pointer's cursor; `None` if unparseable.
        rank: Option<u64>,
    },
}

/// THE monotonic pointer guard: may `candidate_rank` be published over the
/// observed `current` pointer?
///
/// `true` for an open slot, a corrupt pointer, or a candidate at or above
/// the existing cursor; `false` exactly when the existing pointer is
/// parseable and STRICTLY newer — the refusal that makes a slow exporter's
/// stale publish a no-op instead of a regression.
///
/// Soundness of deciding on a read (before the conditional put): every
/// writer uses this guard with a compare-and-swap, so the pointer's rank is
/// monotone non-decreasing — once "strictly newer" is observed, it can never
/// become false, so refusal needs no CAS. Machine-checked as `published
/// cursor never regresses` in `tests/model.rs`.
pub fn pointer_publish_allowed(current: &PointerState, candidate_rank: u64) -> bool {
    match current {
        PointerState::Absent => true,
        PointerState::Present { rank: None } => true,
        PointerState::Present {
            rank: Some(existing),
        } => candidate_rank >= *existing,
    }
}

/// THE prune guard: may this payload object be deleted, given the current
/// pointer's rank?
///
/// A payload is prunable only when ALL hold:
/// - it is not the pointer's own target;
/// - its rank is parseable AND **strictly below** the pointer's (an
///   unparseable rank is never deleted — unknown objects are not ours);
/// - its age has cleared the grace period (`aged_out`; the model passes
///   `true`, checking the harshest zero-grace timing).
///
/// Strictly-below is what makes a dangling pointer impossible regardless of
/// timing: [`pointer_publish_allowed`] refuses any candidate below the
/// pointer, and the pointer is monotone — so anything this guard deletes
/// (rank < pointer-at-prune ≤ pointer-at-any-later-swap) can never be
/// successfully published afterward. The model checker FOUND the dangling
/// counterexample under the earlier age-only rule (a same-cursor payload
/// collected mid-publish, then published by the `>=` swap guard); this rule
/// is the structural fix, machine-checked as `pointer target always
/// fetchable` under zero-grace pruning.
pub fn payload_prunable(
    payload_rank: Option<u64>,
    pointer_rank: u64,
    is_pointer_target: bool,
    aged_out: bool,
) -> bool {
    !is_pointer_target && aged_out && payload_rank.is_some_and(|rank| rank < pointer_rank)
}

/// THE cursor-expiry guard: is resuming from `revision` sound, given the
/// stream's first retained sequence?
///
/// A resume reads `revision + 1` onward; it is sound iff nothing at or below
/// `revision + 1`'s predecessor gap has been head-evicted — i.e.
/// `first_sequence <= revision + 1`. Interior (per-subject) eviction inside
/// the gap is safe for a last-write-wins fold (an overwrite-evicted revision
/// implies a later revision of the same subject exists and will be
/// delivered); lost DELETES come from head eviction, which is exactly what
/// advances `first_sequence`. On a bucket whose retention also evicts
/// *current* values (`max_age`, `discard: old`), head eviction loses live puts
/// too. The same check detects that, but the repair then has to come from an
/// artifact ([`restore_allowed`]), because the bucket's key listing no longer
/// separates "deleted" from "aged out".
///
/// This check must be performed by US: NATS does not error on a below-head
/// start sequence — it silently clamps to the first retained message
/// (pinned live by `tests/resync.rs::nats_silently_clamps_resume_below_first_seq`),
/// which would skip the gap's evicted delete markers with no fallback and no
/// resync. Machine-checked as `bootstrap never silently diverges` in
/// `tests/model.rs` (where the model's retention floor is
/// `first_sequence - 1`).
pub fn resume_window_ok(revision: u64, first_sequence: u64) -> bool {
    first_sequence <= revision.saturating_add(1)
}

/// THE restore guard: may a fold whose cursor (`local_revision`) fell out of
/// the log be replaced by an artifact exported at `artifact_revision`?
///
/// Both must hold:
/// - **Ahead**: the artifact is strictly newer than the local fold. A restore
///   never moves a fold (or the consumer's domain state built from it)
///   backward.
/// - **Fresh**: the artifact's cursor is still inside the log's retention
///   window ([`resume_window_ok`]), so the watch can resume from it and replay
///   the tail without a gap. A stale artifact has no safe recovery: whatever
///   was evicted between its cursor and `first_sequence` is gone from both the
///   artifact and the log.
///
/// "Fresh" implies "ahead" whenever the local cursor is genuinely expired; the
/// ahead half is what still protects a restore when retention can't be read
/// (or was read before a stream was recreated). Machine-checked in
/// `tests/model.rs` (`RestoreRead`) and `tests/model_live_watch.rs`
/// (`GuardRepair`).
pub fn restore_allowed(artifact_revision: u64, local_revision: u64, first_sequence: u64) -> bool {
    restore_ahead(artifact_revision, local_revision)
        && resume_window_ok(artifact_revision, first_sequence)
}

/// The "ahead" half of [`restore_allowed`], on its own: what the main loop
/// re-checks against its own applied cursor (a mid-watch expiry delivered
/// past the resume cursor the watch task checked against).
pub fn restore_ahead(artifact_revision: u64, local_revision: u64) -> bool {
    artifact_revision > local_revision
}

/// Is the bucket's key listing the truth — does "not listed" mean
/// "deleted"? Yes when retention never evicts current values, and also when
/// it can but has evicted nothing yet (first retained revision ≤ 1).
/// Erring toward `false` is safe: it only routes a repair to an artifact.
pub fn listing_is_truth(evicts_current_values: bool, first_revision: u64) -> bool {
    !evicts_current_values || resume_window_ok(0, first_revision)
}

/// The repair a watch armed for an expired cursor, without its payloads
/// (`ExpiryRepair` in `watch_applied`; `None` also when there is no store).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairMode {
    /// Nothing armed: the re-list alone.
    None,
    /// The key-listing diff.
    Relist,
    /// The artifact restore.
    Restore,
    /// Decide by retention.
    Auto,
}

/// What [`plan_repair`] decided for one expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairPlan {
    /// Fall back to the re-list alone.
    ReListOnly,
    /// The key-listing diff, then the re-list.
    Relist,
    /// The artifact restore, then a resume from the artifact's cursor.
    Restore,
    /// Refused: the key-listing diff where the listing isn't the truth
    /// would delete keys that merely aged out. The watch fails.
    RefuseRelist,
}

/// THE repair planner. `listing_truth` is [`listing_is_truth`] when the
/// backend reports retention, `None` when it can't say — then `Relist`
/// (chosen explicitly) is trusted as the caller's word, and `Auto` restores.
pub fn plan_repair(mode: RepairMode, listing_truth: Option<bool>) -> RepairPlan {
    match mode {
        RepairMode::None => RepairPlan::ReListOnly,
        RepairMode::Relist if listing_truth == Some(false) => RepairPlan::RefuseRelist,
        RepairMode::Relist => RepairPlan::Relist,
        RepairMode::Restore => RepairPlan::Restore,
        RepairMode::Auto if listing_truth == Some(true) => RepairPlan::Relist,
        RepairMode::Auto => RepairPlan::Restore,
    }
}

/// A start with no cursor: must the fold be repaired before watching? Yes
/// when it holds data anyway (a torn first checkpoint — a re-list never
/// removes what it doesn't deliver), and, with a restore armed, when the
/// bucket's listing is no longer the truth (a re-list would be incomplete).
pub fn cursorless_start_needs_repair(
    mode: RepairMode,
    fold_has_data: bool,
    listing_truth: Option<bool>,
) -> bool {
    match mode {
        RepairMode::None => false,
        _ if fold_has_data => true,
        RepairMode::Restore | RepairMode::Auto => listing_truth == Some(false),
        RepairMode::Relist => false,
    }
}

/// One side of a key in a restore: absent, or present at a revision
/// (`At(None)`: present but revisionless).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyState {
    /// The key isn't there.
    Absent,
    /// The key is there, at this revision if it has one.
    At(Option<u64>),
}

/// What a restore does with one in-scope key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRestore {
    /// Leave the local entry as it is.
    Keep,
    /// Take the artifact's entry.
    TakeArtifact,
    /// Delete the local entry.
    Delete,
}

/// THE per-key restore decision. An artifact at cursor `C` is complete
/// together with the log after `C`: a key whose latest write is after `C`
/// may be missing or older in it, and the resume delivers that write. So:
///
/// - the artifact has the key: take it, unless the local entry is NEWER
///   (revisions decide; without both, only `identical` entries are kept) —
///   never a move backward;
/// - the artifact lacks it, local has it: delete, unless the bucket lists it
///   live (`listed_live`) — then its current value is a write after `C` —
///   never a phantom delete;
/// - neither has it: nothing to do.
pub fn restore_key(
    local: KeyState,
    artifact: KeyState,
    identical: bool,
    listed_live: bool,
) -> KeyRestore {
    match (local, artifact) {
        (KeyState::At(Some(l)), KeyState::At(Some(a))) if l != a => {
            if l > a {
                KeyRestore::Keep
            } else {
                KeyRestore::TakeArtifact
            }
        }
        (KeyState::At(_), KeyState::At(_)) if identical => KeyRestore::Keep,
        (_, KeyState::At(_)) => KeyRestore::TakeArtifact,
        (KeyState::At(_), KeyState::Absent) if listed_live => KeyRestore::Keep,
        (KeyState::At(_), KeyState::Absent) => KeyRestore::Delete,
        (KeyState::Absent, KeyState::Absent) => KeyRestore::Keep,
    }
}

/// Sequence the watch should start at after applying `revision`.
///
/// `None` at `u64::MAX`: wrapping to 0 would silently replay the bucket
/// from the beginning. Callers must fail the watch rather than wrap.
pub fn resume_start_sequence(revision: u64) -> Option<u64> {
    revision.checked_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_guard_boundaries() {
        let absent = PointerState::Absent;
        let corrupt = PointerState::Present { rank: None };
        let at = |r| PointerState::Present { rank: Some(r) };
        assert!(pointer_publish_allowed(&absent, 0));
        assert!(pointer_publish_allowed(&corrupt, 0));
        assert!(pointer_publish_allowed(&at(5), 5), "equal republishes");
        assert!(pointer_publish_allowed(&at(5), 6));
        assert!(!pointer_publish_allowed(&at(5), 4), "stale is refused");
    }

    #[test]
    fn prune_guard_boundaries() {
        // Strictly below, aged, not the target: prunable.
        assert!(payload_prunable(Some(4), 5, false, true));
        // Equal rank is NOT prunable — it is still publishable (>= guard).
        assert!(!payload_prunable(Some(5), 5, false, true));
        assert!(!payload_prunable(Some(6), 5, false, true));
        // The pointer's own target and unparseable ranks are never prunable.
        assert!(!payload_prunable(Some(4), 5, true, true));
        assert!(!payload_prunable(None, 5, false, true));
        // Grace window holds everything.
        assert!(!payload_prunable(Some(4), 5, false, false));
    }

    #[test]
    fn resume_guard_boundaries() {
        assert!(resume_window_ok(3, 4), "first retained == next read: sound");
        assert!(resume_window_ok(3, 1), "history intact");
        assert!(!resume_window_ok(3, 5), "gap head-evicted: expired");
        assert!(resume_window_ok(u64::MAX, u64::MAX), "saturating boundary");
        assert_eq!(resume_start_sequence(3), Some(4));
        assert_eq!(resume_start_sequence(u64::MAX), None);
    }

    #[test]
    fn repair_planner_is_total() {
        use RepairMode::*;
        for truth in [Option::None, Some(true), Some(false)] {
            assert_eq!(plan_repair(None, truth), RepairPlan::ReListOnly);
            assert_eq!(plan_repair(Restore, truth), RepairPlan::Restore);
        }
        assert_eq!(plan_repair(Relist, Some(false)), RepairPlan::RefuseRelist);
        assert_eq!(plan_repair(Relist, Some(true)), RepairPlan::Relist);
        assert_eq!(
            plan_repair(Relist, Option::None),
            RepairPlan::Relist,
            "caller vouches"
        );
        assert_eq!(plan_repair(Auto, Some(true)), RepairPlan::Relist);
        assert_eq!(plan_repair(Auto, Some(false)), RepairPlan::Restore);
        assert_eq!(
            plan_repair(Auto, Option::None),
            RepairPlan::Restore,
            "unknown: restore"
        );
        assert!(listing_is_truth(false, 99));
        assert!(
            listing_is_truth(true, 1),
            "evicting, but nothing evicted yet"
        );
        assert!(!listing_is_truth(true, 2));
        assert!(
            cursorless_start_needs_repair(Relist, true, Some(true)),
            "data, no cursor"
        );
        assert!(
            !cursorless_start_needs_repair(None, true, Some(false)),
            "nothing armed"
        );
        assert!(cursorless_start_needs_repair(Auto, false, Some(false)));
        assert!(!cursorless_start_needs_repair(Relist, false, Some(false)));
    }

    #[test]
    fn restore_key_never_regresses_never_drops_live() {
        use KeyRestore::*;
        use KeyState::*;
        assert_eq!(
            restore_key(At(Some(5)), At(Some(3)), false, false),
            Keep,
            "local newer"
        );
        assert_eq!(
            restore_key(At(Some(3)), At(Some(5)), false, false),
            TakeArtifact
        );
        assert_eq!(
            restore_key(At(Some(3)), At(Some(3)), true, false),
            Keep,
            "identical"
        );
        assert_eq!(restore_key(At(None), At(None), false, false), TakeArtifact);
        assert_eq!(restore_key(Absent, At(Some(1)), false, false), TakeArtifact);
        assert_eq!(
            restore_key(At(Some(1)), Absent, false, true),
            Keep,
            "listed live"
        );
        assert_eq!(restore_key(At(Some(1)), Absent, false, false), Delete);
        assert_eq!(restore_key(Absent, Absent, false, true), Keep);
    }

    #[test]
    fn restore_guard_boundaries() {
        // Local cursor 3 expired (first retained 6): an artifact at 5 resumes
        // at 6 — ahead and fresh.
        assert!(restore_allowed(5, 3, 6));
        // Artifact at 4 would resume at 5, already evicted: stale.
        assert!(!restore_allowed(4, 3, 6), "stale artifact refused");
        // Equal to local is not ahead, even inside the window.
        assert!(!restore_allowed(3, 3, 1), "not strictly ahead");
        assert!(
            !restore_allowed(2, 3, 1),
            "older artifact never regresses the fold"
        );
        assert!(
            restore_allowed(u64::MAX, 0, u64::MAX),
            "saturating boundary"
        );
    }

    #[test]
    fn resume_start_does_not_wrap_in_nats() {
        let src = include_str!("nats.rs");
        assert!(
            !src.contains("revision + 1"),
            "watch resume must not wrap u64::MAX to 0; use resume_start_sequence"
        );
    }
}
