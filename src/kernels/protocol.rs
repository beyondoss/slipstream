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
//! | [`same_incarnation`]       | `nats` resume paths (`check_resume_window`), the live floor guard, `repair::check_restore` | `model_incarnation.rs` |
//! | [`restore_allowed`]        | `repair::check_restore`             | `model.rs` `Act::RestoreRead`, live `GuardRepair`, repair-steps `WPlan`, fleet `Repair` |
//! | [`restore_ahead`]          | `repair::check_restore`, the main loop's re-check (`repair::fold_in`) | repair-steps restore reply |
//! | [`listing_is_truth`] / [`listing_truth`] | `repair` (plan, cursor-less start), `Snapshot::stale_keys` | every model's view of retention; repair-steps `ConfigForgot` |
//! | [`plan_repair`]            | `repair::plan`, `Snapshot::stale_keys` | expiry dispatch in every model |
//! | [`cursorless_start_needs_repair`] | `repair::run_watch`          | repair-steps `WStart`, fleet `Start` |
//! | [`cursorless_fold_is_this_stream`] | `repair::run_watch` (a fold with data and no cursor) | `tests/recreated_fleet.rs` (live) |
//! | [`restore_key`]            | `repair::restore_diff`              | repair-steps restore op |
//! | [`gap_holds_no_eviction`]  | `nats` resume (`NatsKvWatcher::resume_point`) | — (unit-tested; `tests/eviction.rs` live) |
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

use crate::kv::Retention;
use vstd::prelude::*;

// Every function below is proven against its spec by Verus (`mise run verify`): the specs say exactly what each
// decides, the lemmas are the safety arguments the doc comments make in prose. A normal build erases all of it.
verus! {

/// A position in the artifact order: the stream incarnation a cursor was taken from (its creation time in Unix ns; 0 for a cursor
/// that records none) and its revision. Revisions are ordered only WITHIN an incarnation: nothing orders two incarnations (no
/// clock is assumed), and only one is live at a time. A position of another incarnation is replaceable, not older or newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pos {
    /// The stream incarnation (0 when the cursor records none).
    pub origin: u64,
    /// The cursor's revision.
    pub rank: u64,
}

/// `a` is strictly below `b`: the same incarnation at a lower revision. Positions of different incarnations are incomparable.
pub open spec fn pos_lt(a: Pos, b: Pos) -> bool {
    a.origin == b.origin && a.rank < b.rank
}

/// What the publisher observed at the pointer key before deciding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerState {
    /// No pointer object exists — the slot is open (create-only publish).
    Absent,
    /// A pointer object exists. `at` is its cursor's position, or `None` when
    /// the object is unparseable — a corrupt pointer MUST be replaceable, or
    /// one bad write wedges publishing forever (the same rule as the export
    /// lease's corrupt-steal).
    Present {
        /// Position of the existing pointer's cursor; `None` if unparseable.
        at: Option<Pos>,
    },
}

/// THE monotonic pointer guard: may `candidate` be published over the
/// observed `current` pointer?
///
/// `true` for an open slot, a corrupt pointer, a pointer of ANOTHER incarnation than the candidate's, or a candidate at or above
/// the existing cursor in its own incarnation; `false` exactly when the existing pointer is parseable, of the candidate's
/// incarnation, and STRICTLY higher — the refusal that makes a slow exporter's stale publish a no-op instead of a regression.
/// A pointer of a dead incarnation, whatever its revision, never refuses a publish of the incarnation that replaced it (ranks
/// alone left such a pointer, ranked above the new head, refusing every publish of the new stream): it is foreign, like a corrupt
/// one, and replaceable. A slow exporter of the dead stream can replace the live pointer once; the next live publish replaces it
/// (restorers refuse it meanwhile: `same_incarnation`).
///
/// Soundness of deciding on a read (before the conditional put): every writer uses this guard with a compare-and-swap, so while
/// the pointer stays in one incarnation its rank is monotone non-decreasing — once "strictly higher" is observed, it stays so
/// until the pointer leaves that incarnation, so refusal needs no CAS. Machine-checked as `published cursor never regresses` in
/// `tests/model.rs` (one incarnation).
pub fn pointer_publish_allowed(current: &PointerState, candidate: Pos) -> (r: bool)
    ensures
        r == publish_allowed_spec(*current, candidate),
{
    // Mutation (model tests only): compare revisions across incarnations (the pre-fix guard).
    let candidate = if cfg!(feature = "mut_pointer_ignores_incarnation") {
        Pos { origin: 0, rank: candidate.rank }
    } else {
        candidate
    };
    match current {
        PointerState::Absent => true,
        PointerState::Present { at: None } => true,
        PointerState::Present { at: Some(existing) } => {
            let existing = if cfg!(feature = "mut_pointer_ignores_incarnation") {
                Pos { origin: 0, rank: existing.rank }
            } else {
                *existing
            };
            candidate.origin != existing.origin || candidate.rank >= existing.rank
        },
    }
}

/// [`pointer_publish_allowed`]: refused exactly when the existing pointer is parseable, of the candidate's incarnation, and higher.
pub open spec fn publish_allowed_spec(current: PointerState, candidate: Pos) -> bool {
    !(current is Present && current->Present_at is Some && pos_lt(candidate, current->Present_at->Some_0))
}

/// The pointer after a publish that [`pointer_publish_allowed`] admitted: the candidate's cursor.
pub open spec fn published(candidate: Pos) -> PointerState {
    PointerState::Present { at: Some(candidate) }
}

/// THEOREM (monotone within an incarnation): an admitted publish over a parseable pointer of its own incarnation never lowers it,
/// so while the pointer stays in an incarnation its rank is monotone non-decreasing under every interleaving of admitted
/// publishes. This is what lets a refusal be decided on a read.
pub proof fn a_publish_never_regresses_the_pointer_within_an_incarnation(current: PointerState, candidate: Pos)
    requires
        publish_allowed_spec(current, candidate),
        current is Present && current->Present_at is Some && current->Present_at->Some_0.origin == candidate.origin,
    ensures
        candidate.rank >= current->Present_at->Some_0.rank,
{
}

/// THEOREM (liveness: a dead incarnation's pointer never refuses a live exporter): whatever revision the pointer holds, a candidate
/// of another incarnation is admitted, with no assumption about time or about which incarnation is newer. The pointer of a deleted
/// and recreated bucket is replaced by the new stream's first publish, even when ranked above the new head.
pub proof fn a_pointer_of_another_incarnation_never_refuses_a_publish(current: PointerState, candidate: Pos)
    requires
        current is Present && current->Present_at is Some ==> candidate.origin != current->Present_at->Some_0.origin,
    ensures
        publish_allowed_spec(current, candidate),
{
}

/// THEOREM (liveness of the publish guard): the refusal clears. An open slot, a corrupt pointer, a pointer of another
/// incarnation, and any candidate at or above the pointer in its incarnation are always admitted, so an exporter that retries with a cursor at or above the pointer's succeeds. A stale
/// publish is refused only while the pointer is strictly newer, and the pointer never regresses, so it stays refused: the
/// exporter's exit is a fresher export, never a retry of the stale one. Rank: none needed, the guard is stateless.
pub proof fn a_publish_at_or_above_the_pointer_is_admitted(current: PointerState, candidate: Pos)
    requires
        current is Present && current->Present_at is Some ==> !pos_lt(candidate, current->Present_at->Some_0),
    ensures
        publish_allowed_spec(current, candidate),
{
}

/// THE prune guard: may this payload object be deleted, given the current
/// pointer's position?
///
/// A payload is prunable only when ALL hold:
/// - it is not the pointer's own target;
/// - its position is parseable AND **strictly below** the pointer's (an
///   unparseable name is never deleted — unknown objects are not ours);
/// - its age has cleared the grace period (`aged_out`; the model passes
///   `true`, checking the harshest zero-grace timing).
///
/// Strictly-below, in the pointer's own incarnation, is what makes a dangling
/// pointer impossible regardless of timing, WHILE THE POINTER STAYS IN THAT
/// INCARNATION: [`pointer_publish_allowed`] refuses any candidate below the
/// pointer in its incarnation, and the pointer's rank is monotone there — so
/// anything this guard deletes (rank < pointer-at-prune ≤ pointer-at-any-later
/// -swap) can never be successfully published afterward. The payload's name
/// carries its incarnation as well as its revision so that a payload of another
/// incarnation is never judged by a revision that means nothing to it (it is
/// never pruned here). The bound is real: a slow exporter of a dead
/// incarnation can move the pointer out of the live one, and a stale live
/// exporter can then publish a payload this guard already deleted (fleet
/// finding F1b, `fleet_proofs`); the next live publish replaces that pointer.
/// The model checker FOUND the dangling
/// counterexample under the earlier age-only rule (a same-cursor payload
/// collected mid-publish, then published by the `>=` swap guard); this rule
/// is the structural fix, machine-checked as `pointer target always
/// fetchable` under zero-grace pruning.
pub fn payload_prunable(
    payload: Option<Pos>,
    pointer: Pos,
    is_pointer_target: bool,
    aged_out: bool,
) -> (r: bool)
    ensures
        r == prunable_spec(payload, pointer, is_pointer_target, aged_out),
{
    if is_pointer_target || !aged_out {
        return false;
    }
    match payload {
        Some(p) => p.origin == pointer.origin && p.rank < pointer.rank,
        None => false,
    }
}

/// [`payload_prunable`]: not the target, aged out, and parseable strictly below the pointer.
pub open spec fn prunable_spec(
    payload: Option<Pos>,
    pointer: Pos,
    is_pointer_target: bool,
    aged_out: bool,
) -> bool {
    !is_pointer_target && aged_out && payload is Some && pos_lt(payload->Some_0, pointer)
}

/// THEOREM (no dangling pointer, within an incarnation): a payload the prune guard deletes can never be published afterwards while
/// the pointer stays in its incarnation. The rank is monotone there ([`a_publish_never_regresses_the_pointer_within_an_incarnation`]),
/// so at any later swap it is `later >= pointer > payload`, and the publish guard refuses that candidate. Holds for every timing,
/// including zero grace (`aged_out` is a free input). Across incarnations it does not hold: see F1b.
pub proof fn a_pruned_payload_is_never_published_after(
    payload: Pos,
    pointer: Pos,
    later_pointer: Pos,
    is_pointer_target: bool,
    aged_out: bool,
)
    requires
        prunable_spec(Some(payload), pointer, is_pointer_target, aged_out),
        later_pointer.origin == pointer.origin,
        later_pointer.rank >= pointer.rank,
    ensures
        !publish_allowed_spec(PointerState::Present { at: Some(later_pointer) }, payload),
{
}

/// THEOREM: the pointer's own target and an unparseable payload are never pruned, whatever the positions and ages.
pub proof fn the_target_and_unknown_objects_are_never_pruned(
    payload: Option<Pos>,
    pointer: Pos,
    aged_out: bool,
)
    ensures
        !prunable_spec(payload, pointer, true, aged_out),
        !prunable_spec(None, pointer, false, aged_out),
{
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
///
/// This answers only whether the window is intact; it holds at
/// `revision == u64::MAX`. The sequence to start at must come from
/// [`resume_start_sequence`], which refuses to wrap there.
pub fn resume_window_ok(revision: u64, first_sequence: u64) -> (r: bool)
    ensures
        r == resume_window_spec(revision, first_sequence),
{
    // `saturating_add(1)` in plain terms: at `u64::MAX` every first sequence is in the window.
    revision == u64::MAX || first_sequence <= revision + 1
}

/// [`resume_window_ok`], in unbounded integers: nothing below `revision + 1` has been head-evicted.
pub open spec fn resume_window_spec(revision: u64, first_sequence: u64) -> bool {
    first_sequence as int <= revision as int + 1
}

/// THE incarnation guard: is a cursor's position the CURRENT stream's?
///
/// A stream deleted and recreated under the same name numbers its revisions
/// from 1 again, so [`resume_window_ok`] — sequences alone — admits an old
/// incarnation's cursor whenever it is not below the new stream's floor: a
/// cursor above the new head makes the watch skip every new revision up to it
/// (silently, no error), and one inside the new range splices old state under
/// the new tail. A cursor therefore records which incarnation it was taken
/// from (`cursor_origin`; NATS: the stream's creation time in Unix ns, never
/// 0), and a resume or restore is admitted only against the same one.
///
/// A cursor with no recorded origin was written before cursors recorded one.
/// Nothing proves which incarnation it belongs to; it is admitted when its
/// position is possible in this stream at all (`revision <= last_sequence`),
/// so a fleet upgraded in place keeps restarting. That leaves one hole — an
/// unrecorded cursor from an older incarnation that falls inside the new
/// stream's range — which closes the moment the fold commits a recorded
/// cursor (every cursor a watch produces records one). Machine-checked in
/// `tests/model_incarnation.rs`, which also checks the alternative that
/// refuses unrecorded cursors outright.
pub fn same_incarnation(
    cursor_origin: Option<u64>,
    revision: u64,
    stream_origin: u64,
    last_sequence: u64,
) -> (r: bool)
    ensures
        r == same_incarnation_spec(cursor_origin, revision, stream_origin, last_sequence),
        // A recorded origin decides alone: the position is never consulted, so a recreated stream refuses an old cursor
        // wherever it falls.
        cursor_origin is Some ==> (r <==> cursor_origin->Some_0 == stream_origin),
{
    match cursor_origin {
        Some(origin) => origin == stream_origin,
        None => revision <= last_sequence,
    }
}

/// [`same_incarnation`]'s table.
pub open spec fn same_incarnation_spec(
    cursor_origin: Option<u64>,
    revision: u64,
    stream_origin: u64,
    last_sequence: u64,
) -> bool {
    match cursor_origin {
        Some(origin) => origin == stream_origin,
        None => revision <= last_sequence,
    }
}

/// THE provenance guard for a fold that holds data and NO cursor (a torn first checkpoint, or a restore cut short): is its data of
/// the stream that exists now? The fold persists its stream's incarnation (`recorded`) before any data; the stream's own is
/// `stream` (`Retention::incarnation`). Only an exact match is this stream's. A fold that names another incarnation is foreign, and
/// so is one that names none (a file written before folds recorded one: a one-time re-seed on upgrade) or a stream that cannot say:
/// what cannot be proven to be this stream's is treated as foreign, and the repair replaces it key for key.
pub fn cursorless_fold_is_this_stream(recorded: Option<u64>, stream: Option<u64>) -> (r: bool)
    ensures
        r == cursorless_fold_spec(recorded, stream),
        recorded is None ==> !r,
        stream is None ==> !r,
        (recorded is Some && stream is Some && recorded->Some_0 != stream->Some_0) ==> !r,
{
    // Mutation (model tests only): trust any fold with data and no cursor (the pre-provenance restore).
    if cfg!(feature = "mut_cursorless_fold_trusted") {
        return true;
    }
    match (recorded, stream) {
        (Some(r), Some(s)) => r == s,
        _ => false,
    }
}

/// [`cursorless_fold_is_this_stream`]'s table.
pub open spec fn cursorless_fold_spec(recorded: Option<u64>, stream: Option<u64>) -> bool {
    recorded is Some && stream is Some && recorded->Some_0 == stream->Some_0
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
pub fn restore_allowed(artifact_revision: u64, local_revision: u64, first_sequence: u64) -> (r: bool)
    ensures
        r == restore_allowed_spec(artifact_revision, local_revision, first_sequence),
        // Never backward, never stale: an admitted restore is strictly ahead of the fold and resumable from its cursor.
        r ==> artifact_revision > local_revision && first_sequence as int <= artifact_revision as int + 1,
{
    // Mutation (model tests only): no guard.
    if cfg!(feature = "mut_no_restore_guard") {
        return true;
    }
    restore_ahead(artifact_revision, local_revision)
        && resume_window_ok(artifact_revision, first_sequence)
}

/// [`restore_allowed`]: strictly ahead of the fold, and resumable from the artifact's cursor.
pub open spec fn restore_allowed_spec(artifact_revision: u64, local_revision: u64, first_sequence: u64) -> bool {
    artifact_revision > local_revision && resume_window_spec(artifact_revision, first_sequence)
}

/// The "ahead" half of [`restore_allowed`], on its own: what the main loop
/// re-checks against its own applied cursor (a mid-watch expiry delivered
/// past the resume cursor the watch task checked against).
pub fn restore_ahead(artifact_revision: u64, local_revision: u64) -> (r: bool)
    ensures
        r == (artifact_revision > local_revision),
{
    artifact_revision > local_revision
}

/// THEOREM (liveness of the restore guard): a restore is refused only for a cause that is the artifact's own: it is not ahead
/// of the fold, or its cursor is already below the log's first retained revision. A fresh export (a cursor above the local
/// one, inside the retention window) is always admitted, so the refusal clears when the exporter publishes one: the fairness
/// assumption is the environment's (the exporter keeps exporting; NATS keeps its window while it does).
pub proof fn a_fresh_artifact_is_always_restorable(
    artifact_revision: u64,
    local_revision: u64,
    first_sequence: u64,
)
    requires
        artifact_revision > local_revision,
        first_sequence as int <= artifact_revision as int + 1,
    ensures
        resume_window_spec(artifact_revision, first_sequence) && artifact_revision > local_revision,
{
}

/// Is the bucket's key listing the truth — does "not listed" mean
/// "deleted"? Yes when retention never evicts current values, and also when
/// it can but has evicted nothing yet (first retained revision ≤ 1) — unless
/// the fold has ever seen this answer be no (`seen_evicting`). Retention can be edited: a key that aged out while
/// `max_age` or `discard: old` was in force stays missing from the listing
/// after the setting is removed (or the bucket recreated), so the current
/// config alone can't vouch for the listing.
/// Erring toward `false` is safe: it only routes a repair to an artifact.
pub fn listing_is_truth(
    evicts_current_values: bool,
    first_revision: u64,
    seen_evicting: bool,
) -> (r: bool)
    ensures
        r == listing_is_truth_spec(evicts_current_values, first_revision, seen_evicting),
        // Once the fold has seen eviction in force, the listing is never the truth again, whatever the config says now.
        seen_evicting ==> !r,
{
    // Mutations (model tests only): trust the listing on an evicting bucket;
    // forget what the fold saw.
    if cfg!(feature = "mut_relist_on_evicting") {
        return true;
    }
    let seen_evicting = seen_evicting && !cfg!(feature = "mut_no_eviction_memo");
    !seen_evicting && (!evicts_current_values || resume_window_ok(0, first_revision))
}

/// [`listing_is_truth`]: never seen evicting, and either retention keeps current values or nothing has been evicted yet.
pub open spec fn listing_is_truth_spec(
    evicts_current_values: bool,
    first_revision: u64,
    seen_evicting: bool,
) -> bool {
    !seen_evicting && (!evicts_current_values || resume_window_spec(0, first_revision))
}

/// [`listing_truth`] on the two fields of [`Retention`] it reads: `(evicts_current_values, first_revision)`.
pub fn listing_truth_of(retention: Option<(bool, u64)>, seen_evicting: bool) -> (r: Option<bool>)
    ensures
        r == listing_truth_spec(retention, seen_evicting),
        // Never vouches for a listing the fold has seen evict, and says nothing only when it knows nothing.
        seen_evicting ==> r == Some(false),
        r is None <==> (retention is None && !seen_evicting),
{
    match retention {
        Some((evicts, first)) => Some(listing_is_truth(evicts, first, seen_evicting)),
        None => if seen_evicting { Some(false) } else { None },
    }
}

/// [`listing_truth_of`]'s decision.
pub open spec fn listing_truth_spec(retention: Option<(bool, u64)>, seen_evicting: bool) -> Option<bool> {
    match retention {
        Some((evicts, first)) => Some(listing_is_truth_spec(evicts, first, seen_evicting)),
        None => if seen_evicting { Some(false) } else { None },
    }
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
pub fn plan_repair(mode: RepairMode, listing_truth: Option<bool>) -> (r: RepairPlan)
    ensures
        r == plan_repair_spec(mode, listing_truth),
        // The only refusal: an explicit re-list on a listing that is known not to be the truth.
        r is RefuseRelist <==> (mode is Relist && listing_truth == Some(false)),
        // Nothing armed re-lists alone; an armed restore restores; `Auto` never refuses.
        mode is None ==> r is ReListOnly,
        mode is Restore ==> r is Restore,
        mode is Auto ==> (r is Relist <==> listing_truth == Some(true)),
        mode is Auto ==> (r is Relist || r is Restore),
{
    match mode {
        RepairMode::None => RepairPlan::ReListOnly,
        RepairMode::Relist if listing_truth == Some(false) => RepairPlan::RefuseRelist,
        RepairMode::Relist => RepairPlan::Relist,
        RepairMode::Restore => RepairPlan::Restore,
        RepairMode::Auto if listing_truth == Some(true) => RepairPlan::Relist,
        RepairMode::Auto => RepairPlan::Restore,
    }
}

/// [`plan_repair`]'s table.
pub open spec fn plan_repair_spec(mode: RepairMode, listing_truth: Option<bool>) -> RepairPlan {
    match mode {
        RepairMode::None => RepairPlan::ReListOnly,
        RepairMode::Relist => if listing_truth == Some(false) { RepairPlan::RefuseRelist } else { RepairPlan::Relist },
        RepairMode::Restore => RepairPlan::Restore,
        RepairMode::Auto => if listing_truth == Some(true) { RepairPlan::Relist } else { RepairPlan::Restore },
    }
}

/// THEOREM (liveness of the planner): a refusal is the caller's configuration, and only an explicit `Relist` refuses; the
/// same expiry planned as `Auto` or `Restore` is never refused. The `RefuseRelist` is also never planned over a listing
/// that is the truth, so the refusal clears with the listing (retention restored and not previously seen evicting).
pub proof fn only_an_explicit_relist_can_be_refused(mode: RepairMode, listing_truth: Option<bool>)
    ensures
        plan_repair_spec(mode, listing_truth) is RefuseRelist ==> mode is Relist && listing_truth == Some(false),
        (mode is Auto || mode is Restore || mode is None) ==> !(plan_repair_spec(mode, listing_truth) is RefuseRelist),
        listing_truth != Some(false) ==> !(plan_repair_spec(mode, listing_truth) is RefuseRelist),
{
}

/// A start with no cursor: must the fold be repaired before watching? Yes
/// when it holds data anyway (a torn first checkpoint — a re-list never
/// removes what it doesn't deliver), and, with a restore armed, when the
/// bucket's listing is no longer the truth (a re-list would be incomplete).
pub fn cursorless_start_needs_repair(
    mode: RepairMode,
    fold_has_data: bool,
    listing_truth: Option<bool>,
) -> (r: bool)
    ensures
        r == cursorless_spec(mode, fold_has_data, listing_truth),
        // Nothing armed never repairs; a fold with data always does once anything is armed.
        mode is None ==> !r,
        (!(mode is None) && fold_has_data) ==> r,
{
    // Mutation (model tests only): a fold with data is taken as empty.
    let fold_has_data = fold_has_data && !cfg!(feature = "mut_unanchored_relist");
    match mode {
        RepairMode::None => false,
        _ if fold_has_data => true,
        RepairMode::Restore | RepairMode::Auto => listing_truth == Some(false),
        RepairMode::Relist => false,
    }
}

/// [`cursorless_start_needs_repair`]'s table.
pub open spec fn cursorless_spec(mode: RepairMode, fold_has_data: bool, listing_truth: Option<bool>) -> bool {
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
/// - the local fold is not this stream's (`!local_known`: its cursor was another incarnation's, which the repair forgets): its
///   entries and revision numbers say nothing about this log, so the artifact's entry is taken whenever it has one and a
///   local entry it lacks is deleted (the bucket's listing proves nothing about an entry of another stream; the resume
///   delivers every write after the artifact's cursor). What follows is for a known fold;
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
    local_known: bool,
) -> (r: KeyRestore)
    ensures
        r == restore_key_spec(local, artifact, identical, listed_live, local_known),
        // A fold of another incarnation has entries whose revision numbers mean nothing here: the restore makes it the
        // artifact's, key for key.
        !local_known ==> (r is TakeArtifact <==> artifact is At),
        !local_known ==> (r is Delete <==> (local is At && artifact is Absent)),
        // Never a move backward: a known local entry strictly newer than the artifact's is kept.
        (local_known && local is At && artifact is At && local->At_0 is Some && artifact->At_0 is Some
            && local->At_0->Some_0 > artifact->At_0->Some_0) ==> r is Keep,
        // Never a phantom delete: a key of a known fold that the bucket lists live is not deleted.
        (local_known && listed_live) ==> !(r is Delete),
        // A delete needs a local entry the artifact lacks (and, of a known fold, the bucket not listing it live).
        r is Delete ==> (local is At && artifact is Absent && (!listed_live || !local_known)),
        // The artifact's entry is taken only if it has one.
        r is TakeArtifact ==> artifact is At,
        // Nothing on either side: nothing to do.
        (local is Absent && artifact is Absent) ==> r is Keep,
{
    // Mutations (model tests only): forge both sides revisionless (the
    // pre-fix diff: take anything not identical); forge the key unlisted.
    let (local, artifact) = if cfg!(feature = "mut_restore_ignores_versions") {
        (strip_revision(local), strip_revision(artifact))
    } else {
        (local, artifact)
    };
    let listed_live = listed_live && !cfg!(feature = "mut_restore_ignores_listing");
    // Mutation (model tests only): trust a fold of another incarnation (the pre-fix restore).
    let local_known = local_known || cfg!(feature = "mut_restore_trusts_foreign");
    if !local_known {
        return match (local, artifact) {
            (_, KeyState::At(_)) => KeyRestore::TakeArtifact,
            (KeyState::At(_), KeyState::Absent) => KeyRestore::Delete,
            (KeyState::Absent, KeyState::Absent) => KeyRestore::Keep,
        };
    }
    match (local, artifact) {
        (KeyState::At(Some(l)), KeyState::At(Some(a))) if l != a => {
            if l > a {
                KeyRestore::Keep
            } else {
                KeyRestore::TakeArtifact
            }
        },
        (KeyState::At(_), KeyState::At(_)) if identical => KeyRestore::Keep,
        (_, KeyState::At(_)) => KeyRestore::TakeArtifact,
        (KeyState::At(_), KeyState::Absent) if listed_live => KeyRestore::Keep,
        (KeyState::At(_), KeyState::Absent) => KeyRestore::Delete,
        (KeyState::Absent, KeyState::Absent) => KeyRestore::Keep,
    }
}

/// A key's side with its revision forgotten (the mutation's input forgery).
fn strip_revision(k: KeyState) -> (r: KeyState)
    ensures
        r == match k {
            KeyState::At(_) => KeyState::At(None),
            KeyState::Absent => KeyState::Absent,
        },
{
    match k {
        KeyState::At(_) => KeyState::At(None),
        KeyState::Absent => KeyState::Absent,
    }
}

/// [`restore_key`]'s table.
pub open spec fn restore_key_spec(
    local: KeyState,
    artifact: KeyState,
    identical: bool,
    listed_live: bool,
    local_known: bool,
) -> KeyRestore {
    if !local_known {
        match (local, artifact) {
            (_, KeyState::At(_)) => KeyRestore::TakeArtifact,
            (KeyState::At(_), KeyState::Absent) => KeyRestore::Delete,
            (KeyState::Absent, KeyState::Absent) => KeyRestore::Keep,
        }
    } else {
    match (local, artifact) {
        (KeyState::At(Some(l)), KeyState::At(Some(a))) if l != a => {
            if l > a { KeyRestore::Keep } else { KeyRestore::TakeArtifact }
        },
        (KeyState::At(_), KeyState::At(_)) if identical => KeyRestore::Keep,
        (_, KeyState::At(_)) => KeyRestore::TakeArtifact,
        (KeyState::At(_), KeyState::Absent) => if listed_live { KeyRestore::Keep } else { KeyRestore::Delete },
        (KeyState::Absent, KeyState::Absent) => KeyRestore::Keep,
    }
    }
}

/// Can a resume gap that looks expired (the log's first retained revision is
/// past the cursor's successor) hold no evicted message? Only when age is the
/// log's sole way to evict a current value (`age_only`: `max_age`, no
/// per-message TTLs, no `discard: old` limit) and the cursor's own message
/// is at most half of `max_age` old: every later message is younger still, so
/// none has aged out, and each one missing was superseded by a later write
/// the resume delivers. Half rather than all of `max_age` is headroom for
/// clock steps and leader changes between the server that stamped the
/// message and the one reporting the time. `cursor_age_ms` is 0 when the
/// stamp is ahead of the clock.
pub fn gap_holds_no_eviction(age_only: bool, max_age_ms: u64, cursor_age_ms: u64) -> (r: bool)
    ensures
        r == (age_only && max_age_ms > 0 && 2 * (cursor_age_ms as int) <= max_age_ms as int),
{
    // `2 * age <= max` without the saturating multiply: at `age == max == u64::MAX` the saturated product equals `max`
    // and the check passed for a cursor older than the retention.
    age_only && max_age_ms > 0 && cursor_age_ms <= max_age_ms / 2
}

/// Sequence the watch should start at after applying `revision`.
///
/// `None` at `u64::MAX`: wrapping to 0 would silently replay the bucket
/// from the beginning. Callers must fail the watch rather than wrap.
pub fn resume_start_sequence(revision: u64) -> (r: Option<u64>)
    ensures
        r is Some <==> revision < u64::MAX,
        r is Some ==> r->Some_0 as int == revision as int + 1,
{
    if revision == u64::MAX { None } else { Some(revision + 1) }
}

} // verus!

/// [`listing_is_truth`] for retention as a watcher reports it: `None` when
/// the backend can't say and the fold never saw it evict — then the caller's
/// chosen repair decides ([`plan_repair`]).
pub fn listing_truth(retention: Option<Retention>, seen_evicting: bool) -> Option<bool> {
    // The adapter only reads two fields; the decision is `listing_truth_of`.
    listing_truth_of(
        retention.map(|r| (r.evicts_current_values, r.first_revision)),
        seen_evicting,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_guard_boundaries() {
        let absent = PointerState::Absent;
        let corrupt = PointerState::Present { at: None };
        let at = |origin, rank| PointerState::Present {
            at: Some(Pos { origin, rank }),
        };
        let pos = |origin, rank| Pos { origin, rank };
        assert!(pointer_publish_allowed(&absent, pos(1, 0)));
        assert!(pointer_publish_allowed(&corrupt, pos(1, 0)));
        assert!(
            pointer_publish_allowed(&at(1, 5), pos(1, 5)),
            "equal republishes"
        );
        assert!(pointer_publish_allowed(&at(1, 5), pos(1, 6)));
        assert!(
            !pointer_publish_allowed(&at(1, 5), pos(1, 4)),
            "stale is refused"
        );
        // A dead incarnation's pointer, ranked above the new head, is replaced...
        assert!(pointer_publish_allowed(&at(1, 500), pos(2, 3)));
        // ...and nothing orders incarnations: no clock is assumed, a pointer of another incarnation is replaceable either way.
        assert!(pointer_publish_allowed(&at(2, 3), pos(1, 500)));
        // A pointer that records no incarnation is another incarnation to every recorded one.
        assert!(pointer_publish_allowed(&at(0, 500), pos(2, 3)));
        assert!(pointer_publish_allowed(&at(2, 3), pos(0, 500)));
    }

    #[test]
    fn prune_guard_boundaries() {
        let pos = |origin, rank| Some(Pos { origin, rank });
        let ptr = Pos { origin: 2, rank: 5 };
        // Strictly below, aged, not the target: prunable.
        assert!(payload_prunable(pos(2, 4), ptr, false, true));
        // Equal position is NOT prunable: it is still publishable (>= guard).
        assert!(!payload_prunable(pos(2, 5), ptr, false, true));
        assert!(!payload_prunable(pos(2, 6), ptr, false, true));
        // A payload of another incarnation is never judged by a revision that means nothing to it.
        assert!(!payload_prunable(pos(1, 4), ptr, false, true));
        assert!(!payload_prunable(pos(3, 1), ptr, false, true));
        // The pointer's own target and unparseable names are never prunable.
        assert!(!payload_prunable(pos(2, 4), ptr, true, true));
        assert!(!payload_prunable(None, ptr, false, true));
        // Grace window holds everything.
        assert!(!payload_prunable(pos(2, 4), ptr, false, false));
    }

    #[test]
    fn incarnation_guard_boundaries() {
        assert!(
            same_incarnation(Some(7), 5_000, 7, 3),
            "same stream: any cursor"
        );
        assert!(
            !same_incarnation(Some(6), 1, 7, 100),
            "recreated: even in range"
        );
        assert!(same_incarnation(None, 3, 7, 3), "unrecorded, possible here");
        assert!(
            !same_incarnation(None, 4, 7, 3),
            "unrecorded, beyond the head"
        );
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
    fn gap_holds_no_eviction_boundaries() {
        assert!(gap_holds_no_eviction(true, 60_000, 0));
        assert!(gap_holds_no_eviction(true, 60_000, 30_000));
        assert!(!gap_holds_no_eviction(true, 60_000, 30_001));
        assert!(!gap_holds_no_eviction(false, 60_000, 0), "other eviction");
        assert!(!gap_holds_no_eviction(true, 0, 0), "no max_age");
        assert!(
            !gap_holds_no_eviction(true, 60_000, u64::MAX),
            "no overflow"
        );
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
        assert!(listing_is_truth(false, 99, false));
        assert!(
            listing_is_truth(true, 1, false),
            "evicting, but nothing evicted yet"
        );
        assert!(!listing_is_truth(true, 2, false));
        assert!(
            !listing_is_truth(false, 99, true),
            "eviction removed from the config, but the fold saw it in force"
        );
        let r = |evicts| {
            Some(Retention {
                evicts_current_values: evicts,
                first_revision: 9,
                last_revision: u64::MAX,
                incarnation: Option::None,
            })
        };
        assert_eq!(listing_truth(r(false), false), Some(true));
        assert_eq!(listing_truth(r(false), true), Some(false));
        assert_eq!(listing_truth(Option::None, false), Option::None);
        assert_eq!(
            listing_truth(Option::None, true),
            Some(false),
            "the memo speaks even when the backend can't"
        );
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
            restore_key(At(Some(5)), At(Some(3)), false, false, true),
            Keep,
            "local newer"
        );
        assert_eq!(
            restore_key(At(Some(3)), At(Some(5)), false, false, true),
            TakeArtifact
        );
        assert_eq!(
            restore_key(At(Some(3)), At(Some(3)), true, false, true),
            Keep,
            "identical"
        );
        assert_eq!(
            restore_key(At(None), At(None), false, false, true),
            TakeArtifact
        );
        assert_eq!(
            restore_key(Absent, At(Some(1)), false, false, true),
            TakeArtifact
        );
        assert_eq!(
            restore_key(At(Some(1)), Absent, false, true, true),
            Keep,
            "listed live"
        );
        assert_eq!(restore_key(At(Some(1)), Absent, false, false, true), Delete);
        assert_eq!(restore_key(Absent, Absent, false, true, true), Keep);
    }

    #[test]
    fn restore_key_over_an_unknown_fold_is_the_artifacts() {
        use KeyRestore::*;
        use KeyState::*;
        // A local entry the old stream numbered above the artifact's is not newer.
        assert_eq!(
            restore_key(At(Some(500)), At(Some(3)), false, false, false),
            TakeArtifact
        );
        // Even when identical: nothing is kept on a foreign claim.
        assert_eq!(
            restore_key(At(Some(3)), At(Some(3)), true, false, false),
            TakeArtifact
        );
        // The bucket listing the key proves nothing about an entry of another stream.
        assert_eq!(
            restore_key(At(Some(500)), Absent, false, true, false),
            Delete
        );
        assert_eq!(restore_key(Absent, Absent, false, true, false), Keep);
        assert_eq!(
            restore_key(Absent, At(Some(1)), false, false, false),
            TakeArtifact
        );
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
        let src = include_str!("../nats.rs");
        assert!(
            !src.contains("revision + 1"),
            "watch resume must not wrap u64::MAX to 0; use resume_start_sequence"
        );
    }
}
