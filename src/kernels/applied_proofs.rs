//! What `applied::step` is proven to do, for every event and with no bound: Verus theorems over the kernel's own
//! `step_spec` (which `step` is proven equal to).
//!
//! # The world
//!
//! The kernel owns no data, so the cursor authority invariant ("the store's persisted cursor never names data the store does not
//! hold") is stated over [`World`]: the kernel's [`State`] together with ghost positions, all unbounded naturals, that the shell
//! keeps in its queues and its store. [`w_step`] says what the shell does with the kernel's actions (the interpreter in
//! `applied::Fold::flush` and `repair::restore_request`), one case per action; it decides nothing. The proof is inductive:
//! `inv(init)` and `inv(w) && pre(w, e) ==> inv(w_step(w, e))` for every event, then `inv ==> cursor <= contents`.
//!
//! # What the shell must supply ([`pre`])
//!
//! These are facts about the shell's own loop, not assumptions the proof makes about the kernel. Each is explicit:
//! * **Sequential loop.** While a store fold runs (`Flush::Storing`) the loop awaits it: the only event is `StoreDone`.
//! * **Repair runs to completion.** No update is received while a restore is in flight (`fold_in` is awaited by the main loop).
//! * **Honest `ahead`.** `RestoreAhead { ahead }` is `artifact cursor > applied cursor`, and the pre-repair settle has run: the
//!   raw queue is empty and no position is pending.
//! * **A run with a store.** `FlushRequested { has_store: true }` (a run without a store has no persisted cursor to protect).
//! * **`AdvanceTo` only follows `CommitCursor`** (`repair::restore_request` is its one caller).
//! * Restore chunks carry no stream position: they move the store's contents, never the cursor.
//!
//! # Environment axioms (the progress lemmas rest on these, never on slipstream's own code)
//!
//! * **A1, the store answers.** Every `FoldStore` is eventually answered with a `StoreDone` (the disk and the blocking task
//!   return). Whether the answer is `Ok` is not assumed: a fold that fails every time ends the run through the failure streak.
//! * **A2, the environment answers each repair action** (`CheckAhead`, `RunDiff`, `ListLive`, `FoldChunks`) with its event: NATS
//!   replies to the listing and retention reads; the disk returns the chunk folds.

use crate::kernels::applied::*;
use vstd::prelude::*;

verus! {

/// The kernel's state with the ghost positions of the shell's queues and store. Ranks are stream positions.
pub struct World {
    pub k: State,
    /// The last stream position received (the pending position while `k.high`).
    pub delivered: nat,
    /// The in-memory applied cursor.
    pub applied: nat,
    /// The store's PERSISTED cursor: what a restart resumes from.
    pub store_cursor: nat,
    /// The raw queue holds the updates in `(raw_from, delivered]`.
    pub raw_from: nat,
    /// The store holds complete contents up to this position (data of the log, folded).
    pub contents: nat,
    /// The cursor the running store fold commits at, while one runs.
    pub fold: Option<nat>,
    /// The cursor of the artifact a restore is replacing the fold with.
    pub art: nat,
}

pub open spec fn max_nat(a: nat, b: nat) -> nat {
    if a >= b { a } else { b }
}

/// A fresh run over a store that holds `contents` up to its cursor `c`.
pub open spec fn init_world(c: nat, contents: nat) -> World {
    World { k: init_state(), delivered: c, applied: c, store_cursor: c, raw_from: c, contents, fold: None, art: 0 }
}

/// THE invariant.
pub open spec fn inv(w: World) -> bool {
    // Cursor authority, structurally: the raw queue starts exactly at the store's cursor (nothing between the store and the
    // loop falls on the floor), and the store holds what its cursor names.
    &&& w.raw_from == w.store_cursor
    &&& w.store_cursor <= w.contents
    &&& w.store_cursor <= w.applied <= w.delivered
    // A running fold is the flush's: at the applied cursor, which is the pending position.
    &&& match w.k.flush {
        Flush::Idle => w.fold is None,
        Flush::Storing { advanced } => w.fold == Some(w.applied) && w.applied == w.delivered && advanced == w.k.high,
    }
    &&& (!w.k.high ==> w.applied == w.delivered)
    &&& w.k.fail_streak <= MAX_STORE_APPLY_FAILURES
    // A restore past its ahead check replaces a fold that is settled and strictly behind the artifact; once the chunks are
    // folded the store holds the artifact's contents.
    &&& ((w.k.restore is Diff || w.k.restore is Listing || w.k.restore is Fold)
        ==> w.art > w.delivered && w.applied == w.delivered && !w.k.high)
    &&& (w.k.restore is Fold ==> w.contents >= w.art)
}

/// The facts the shell supplies with an event (see the module docs).
pub open spec fn pre(w: World, e: Event) -> bool {
    &&& (w.k.flush is Storing ==> e is StoreDone)
    &&& (e is Received ==> w.k.restore is Idle)
    // `AdvanceTo` is never sent on its own: only as the interpretation of `CommitCursor` (`w_step`).
    &&& !(e is AdvanceTo)
    &&& match e {
        Event::FlushRequested { has_store, .. } => has_store,
        Event::RestoreAhead { ahead } => {
            ahead == (w.art > w.applied) && w.raw_from == w.delivered && !w.k.high
        },
        _ => true,
    }
}

/// Where a restart resumes from: the production decision, `resume_from`'s table. `mut_resume_from_memory` forges it exactly as that
/// feature forges the code, so a mutated build fails every proof that rests on a restart (the invariant, the fleet's crash step).
pub open spec fn resume_source(holds_store: bool) -> ResumeFrom {
    if cfg!(feature = "mut_resume_from_memory") {
        ResumeFrom::Memory
    } else {
        resume_from_spec(holds_store)
    }
}

/// After a crash or a fail-stop: the run restarts from the cursor `resume_from` names; the store keeps what it persisted.
pub open spec fn restart(w: World) -> World {
    let resume = match resume_source(true) {
        ResumeFrom::Store => w.store_cursor,
        ResumeFrom::Memory => w.applied,
    };
    World {
        k: init_state(),
        delivered: resume,
        applied: resume,
        raw_from: max_nat(resume, w.store_cursor),
        fold: None,
        ..w
    }
}

/// One event, as the shell runs it: the kernel's new state and the world its actions leave. `art_new` is the artifact cursor
/// a `RepairRequested(Restore)` starts a restore with.
pub open spec fn w_step(w: World, e: Event, art_new: nat) -> World {
    let (k1, acts) = step_spec(w.k, e);
    let delivered1 = if e is Received && e->Received_positioned { w.delivered + 1 } else { w.delivered };
    let art1 = if e == Event::RepairRequested(RepairKind::Restore) { art_new } else { w.art };
    let applied1 = if acts.contains(Action::AdvanceApplied) { delivered1 } else { w.applied };
    let contents1 = if acts.contains(Action::FoldChunks) { max_nat(w.contents, art1) } else { w.contents };
    // The store fold takes the raw queue and runs at the applied cursor.
    let fold1 = if acts.contains(Action::FoldStore) { Some(applied1) } else { w.fold };
    // The store's answer.
    let done = e is StoreDone && w.fold is Some;
    let c = w.fold->Some_0;
    let landed = done && acts.contains(Action::StoreCommitted);
    let raw_from1 = if landed {
        c
    } else if done && !acts.contains(Action::Requeue) {
        // Taken for the fold and never put back: gone from the queue.
        c
    } else {
        w.raw_from
    };
    let contents2 = if landed && w.raw_from <= contents1 && c > contents1 { c } else { contents1 };
    let store_cursor1 = if landed { c } else { w.store_cursor };
    let fold2 = if done { None } else { fold1 };
    let w1 = World {
        k: k1,
        delivered: delivered1,
        applied: applied1,
        store_cursor: store_cursor1,
        raw_from: raw_from1,
        contents: contents2,
        fold: fold2,
        art: art1,
    };
    // A restore's last step takes the artifact's cursor as the pending position (`advance_to`).
    let w2 = if acts.contains(Action::CommitCursor) {
        World {
            k: step_spec(w1.k, Event::AdvanceTo).0,
            delivered: w1.art,
            raw_from: if w1.art <= w1.contents { w1.raw_from } else { w1.art },
            ..w1
        }
    } else {
        w1
    };
    if acts.contains(Action::Fail(Fatal::StoreFailStreak)) || acts.contains(Action::Fail(Fatal::StorePanicked)) {
        restart(w2)
    } else {
        w2
    }
}

/// THEOREM: a fresh run satisfies the invariant, over any store.
pub proof fn a_fresh_run_satisfies_the_invariant(c: nat, contents: nat)
    requires
        c <= contents,
    ensures
        inv(init_world(c, contents)),
{
}

/// THEOREM (cursor authority, inductive over ALL events): from any world satisfying the invariant, whatever the kernel is
/// handed next, the invariant holds again. No bound on positions, queue lengths or the number of events.
pub proof fn every_event_preserves_the_invariant(w: World, e: Event, art_new: nat)
    requires
        inv(w),
        pre(w, e),
    ensures
        inv(w_step(w, e, art_new)),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (a crash is safe): a restart resumes from a cursor the store holds, so the invariant survives it at any point.
pub proof fn a_restart_preserves_the_invariant(w: World)
    requires
        inv(w),
    ensures
        inv(restart(w)),
{
}

/// THEOREM (cursor authority): the store's persisted cursor never names data the store does not hold.
pub proof fn the_store_cursor_never_names_data_the_store_lacks(w: World)
    requires
        inv(w),
    ensures
        w.store_cursor <= w.contents,
{
}

/// THEOREM: the store's cursor moves only when a store fold lands (`StoreCommitted`): nothing else, no repair step and no
/// flush step, can advance what a restart resumes from.
pub proof fn only_a_landed_fold_moves_the_store_cursor(w: World, e: Event, art_new: nat)
    requires
        inv(w),
        pre(w, e),
    ensures
        w_step(w, e, art_new).store_cursor != w.store_cursor ==> step_spec(w.k, e).1.contains(Action::StoreCommitted),
{
}

// ---------------------------------------------------------------------------------------------------------------------
// Ordering: what the kernel emits, and from where. Each is a fact about `step_spec` for every state and event.
// ---------------------------------------------------------------------------------------------------------------------

/// THEOREM (restore order): the artifact's cursor is committed only by the answer to the chunk fold, from the `Fold` phase, and
/// the reply to the watch task is the very next action. The restore's phases are a chain: `Ahead -> Diff -> (Listing ->) Fold ->
/// Idle`, with a refusal returning to `Idle` from `Ahead` or `Listing` only.
#[verifier::rlimit(40)]
pub proof fn the_cursor_commits_only_after_the_chunks_fold(s: State, e: Event)
    ensures
        step_spec(s, e).1.contains(Action::CommitCursor) ==> (e is RestoreFolded && s.restore is Fold
            && step_spec(s, e).1 == seq![Action::CommitCursor, Action::ReplyRestored]),
        step_spec(s, e).1.contains(Action::ReplyRestored) ==> (e is RestoreFolded && s.restore is Fold),
        // `FoldChunks` follows a passed ahead check and, when there are delete candidates, a passed window re-check.
        step_spec(s, e).1.contains(Action::FoldChunks) ==> (
            (e is RestoreDiffed && s.restore is Diff && !e->RestoreDiffed_has_deletes)
            || (e is RestoreListed && s.restore is Listing && e->RestoreListed_window_ok)),
        step_spec(s, e).1.contains(Action::ListLive) ==> (e is RestoreDiffed && s.restore is Diff && e->RestoreDiffed_has_deletes),
        step_spec(s, e).1.contains(Action::RunDiff) ==> (e is RestoreAhead && s.restore is Ahead && e->RestoreAhead_ahead),
        // The phase chain.
        (step_spec(s, e).0.restore != s.restore) ==> match (s.restore, step_spec(s, e).0.restore) {
            (Restore::Idle, Restore::Ahead) => e == Event::RepairRequested(RepairKind::Restore),
            (Restore::Ahead, Restore::Diff) => e is RestoreAhead && e->RestoreAhead_ahead,
            (Restore::Ahead, Restore::Idle) => e is RestoreAhead && !e->RestoreAhead_ahead,
            (Restore::Diff, Restore::Listing) => e is RestoreDiffed && e->RestoreDiffed_has_deletes,
            (Restore::Diff, Restore::Fold) => e is RestoreDiffed && !e->RestoreDiffed_has_deletes,
            (Restore::Listing, Restore::Fold) => e is RestoreListed && e->RestoreListed_window_ok,
            (Restore::Listing, Restore::Idle) => e is RestoreListed && !e->RestoreListed_window_ok,
            (Restore::Fold, Restore::Idle) => e is RestoreFolded,
            (_, Restore::Ahead) => e == Event::RepairRequested(RepairKind::Restore),
            _ => false,
        },
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (a refusal changes nothing): a refused restore replies with the error and emits nothing else (no diff, fold, commit
/// or flush), and leaves the kernel state as it was but for the phase, which returns to `Idle`.
pub proof fn a_refusal_changes_nothing(s: State, e: Event)
    ensures
        step_spec(s, e).1.contains(Action::Refuse(Refusal::NotAhead))
            || step_spec(s, e).1.contains(Action::Refuse(Refusal::WindowAged))
            ==> (step_spec(s, e).0 == State { restore: Restore::Idle, ..s } && step_spec(s, e).1.len() == 1),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (the resync's ack barrier): the parked watch task is released only by the answer that the deletes are folded, and only
/// once per resync. Starting the repair never releases it.
pub proof fn the_resync_acks_only_after_its_deletes_are_folded(s: State, e: Event)
    ensures
        step_spec(s, e).1.contains(Action::AckResync) ==> (e is RelistFolded && s.relisting && !step_spec(s, e).0.relisting),
        (e is RepairRequested) ==> !step_spec(s, e).1.contains(Action::AckResync),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (a repair sees everything delivered): a re-list or a restore drains the watch task's backlog, then settles, and only
/// then runs its first step; an export flushes before it exports.
pub proof fn a_repair_drains_then_settles_and_an_export_flushes_first(s: State, kind: RepairKind, has_store: bool)
    ensures
        kind is Relist ==> step_spec(s, Event::RepairRequested(kind)).1
            == seq![Action::DrainBacklog, Action::Settle, Action::RunRelist],
        kind is Restore ==> step_spec(s, Event::RepairRequested(kind)).1
            == seq![Action::DrainBacklog, Action::Settle, Action::CheckAhead],
        step_spec(s, Event::ExportRequested { has_store }).1[0] == Action::Flush,
{
}

/// THEOREM (flush order): the domain `apply` runs before the cursor advances, the cursor advances before the store fold, and the
/// consumer is told only after the store settled; a failed fold re-queues its batch (never drops it) unless it fail-stops.
pub proof fn a_flush_applies_then_advances_then_folds(s: State, q: Queued, has_store: bool)
    ensures
        ({
            let acts = step_spec(s, Event::FlushRequested { queued: q, has_store }).1;
            (acts.contains(Action::AdvanceApplied) && acts.contains(Action::ApplyDomain)
                ==> acts.index_of(Action::ApplyDomain) < acts.index_of(Action::AdvanceApplied))
            && (acts.contains(Action::FoldStore) && acts.contains(Action::AdvanceApplied)
                ==> acts.index_of(Action::AdvanceApplied) < acts.index_of(Action::FoldStore))
            && (acts.contains(Action::FoldStore) ==> !acts.contains(Action::NotifyApplied))
        }),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (a failed fold is never dropped): every non-fatal failed store answer re-queues its batch.
pub proof fn a_failed_fold_requeues_its_batch(s: State)
    requires
        s.flush is Storing,
        s.fail_streak < MAX_STORE_APPLY_FAILURES,
    ensures
        step_spec(s, Event::StoreDone(StoreOutcome::Failed)).1.contains(Action::Requeue)
            || step_spec(s, Event::StoreDone(StoreOutcome::Failed)).1.contains(Action::Fail(Fatal::StoreFailStreak)),
        step_spec(s, Event::StoreDone(StoreOutcome::Ok)).1.contains(Action::StoreCommitted),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (no event out of phase does anything): a restore or resync answer that arrives in the wrong phase, and a store answer
/// with no fold running, leave the state and the actions empty.
pub proof fn an_event_out_of_phase_is_ignored(s: State, e: Event)
    ensures
        (e is RestoreAhead && !(s.restore is Ahead)) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        (e is RestoreDiffed && !(s.restore is Diff)) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        (e is RestoreListed && !(s.restore is Listing)) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        (e is RestoreFolded && !(s.restore is Fold)) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        (e is RelistFolded && !s.relisting) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        (e is StoreDone && s.flush is Idle) ==> step_spec(s, e) == (s, Seq::<Action>::empty()),
        // An unpositioned update never records a cursor.
        (e is Received && !e->Received_positioned) ==> (step_spec(s, e).0.high == s.high
            && !step_spec(s, e).1.contains(Action::RecordHigh)),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

// ---------------------------------------------------------------------------------------------------------------------
// Progress. Every refusal and fail-stop has a ranking function that each fair step lowers until the goal.
// Fairness: A1 (every fold is answered), A2 (every repair action is answered); see the module docs.
// ---------------------------------------------------------------------------------------------------------------------

/// Failed store folds left before the run fail-stops.
pub open spec fn streak_rank(s: State) -> int {
    MAX_STORE_APPLY_FAILURES as int - s.fail_streak as int
}

/// THEOREM (progress, a failed fold): from a running fold, a failed answer either fail-stops the run (a restart refolds from the
/// store's cursor) or re-queues the batch and strictly lowers the rank, so at most `MAX_STORE_APPLY_FAILURES` consecutive
/// failures are ever retried. A landed fold is the goal: the streak resets and the commit is observed.
pub proof fn a_failed_fold_is_retried_until_it_lands_or_the_run_fail_stops(s: State)
    requires
        s.flush is Storing,
        s.fail_streak < MAX_STORE_APPLY_FAILURES,
    ensures
        ({
            let (s1, acts) = step_spec(s, Event::StoreDone(StoreOutcome::Failed));
            acts.contains(Action::Fail(Fatal::StoreFailStreak))
                || (acts.contains(Action::Requeue) && s1.flush is Idle && streak_rank(s1) < streak_rank(s)
                && streak_rank(s1) >= 1)
        }),
        ({
            let (s1, acts) = step_spec(s, Event::StoreDone(StoreOutcome::Ok));
            s1.fail_streak == 0 && s1.flush is Idle && acts.contains(Action::StoreCommitted)
        }),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (progress, the retry): once the store is behind (a failed fold re-queued its batch, or any position is pending), the
/// next flush request always starts a fold; a flush with nothing queued, pending or unsynced is the only one that does not.
pub proof fn a_flush_with_anything_owed_always_folds(s: State, q: Queued)
    requires
        q.raw > 0 || s.high || s.memo_dirty,
    ensures
        step_spec(s, Event::FlushRequested { queued: q, has_store: true }).1.contains(Action::FoldStore),
        step_spec(s, Event::FlushRequested { queued: q, has_store: true }).0.flush is Storing,
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (progress, the store catches up): from any invariant world with the store behind, whatever failures came before
/// (the streak is not an input), a flush request followed by one landed fold brings the store's persisted cursor to the head.
/// Together with the streak rank this bounds the retries: the store reaches the head after at most `MAX_STORE_APPLY_FAILURES`
/// failures and one success, or the run fail-stops and restarts at the store's cursor with the invariant intact.
pub proof fn a_landed_fold_brings_the_store_to_the_head(w: World, q: Queued)
    requires
        inv(w),
        w.k.flush is Idle,
        w.store_cursor < w.delivered,
        // The shell's queue holds what the world says it holds.
        q.raw > 0,
    ensures
        ({
            let w1 = w_step(w, Event::FlushRequested { queued: q, has_store: true }, 0);
            let w2 = w_step(w1, Event::StoreDone(StoreOutcome::Ok), 0);
            w1.k.flush is Storing && w2.store_cursor == w.delivered && w2.k.flush is Idle && w2.k.fail_streak == 0
        }),
{
    broadcast use vstd::seq_lib::group_seq_properties;
    let w1 = w_step(w, Event::FlushRequested { queued: q, has_store: true }, 0);
    every_event_preserves_the_invariant(w, Event::FlushRequested { queued: q, has_store: true }, 0);
}

/// THEOREM (progress, after a failed fold): the failed batch is still owed. The store's cursor is where it was, below the head,
/// and the kernel is back to `Idle`, so the retry above applies.
pub proof fn a_failed_fold_leaves_the_store_behind_and_retryable(w: World, q: Queued)
    requires
        inv(w),
        w.k.flush is Idle,
        w.store_cursor < w.delivered,
        q.raw > 0,
        w.k.fail_streak + 1 < MAX_STORE_APPLY_FAILURES,
    ensures
        ({
            let w1 = w_step(w, Event::FlushRequested { queued: q, has_store: true }, 0);
            let w2 = w_step(w1, Event::StoreDone(StoreOutcome::Failed), 0);
            w2.store_cursor == w.store_cursor && w2.raw_from < w2.delivered && w2.k.flush is Idle
                && streak_rank(w2.k) < streak_rank(w.k) && inv(w2)
        }),
{
    broadcast use vstd::seq_lib::group_seq_properties;
    every_event_preserves_the_invariant(w, Event::FlushRequested { queued: q, has_store: true }, 0);
}

/// The phases left before a restore ends.
pub open spec fn restore_rank(r: Restore) -> int {
    match r {
        Restore::Idle => 0,
        Restore::Fold => 1,
        Restore::Listing => 2,
        Restore::Diff => 3,
        Restore::Ahead => 4,
    }
}

/// THEOREM (progress, the restore): each answer the shell gives to the phase's action either refuses (`Idle`) or moves one phase
/// on, so the rank strictly falls; a restore ends within four answers. The refusals (`NotAhead`, `WindowAged`) end at `Idle`
/// with nothing else changed, so a fresh `RepairRequested(Restore)` starts again exactly as from a new run (the refusal has no
/// residue), and an artifact that is ahead and inside the window always completes.
pub proof fn every_restore_answer_lowers_the_rank(s: State, e: Event)
    requires
        (s.restore is Ahead && e is RestoreAhead) || (s.restore is Diff && e is RestoreDiffed)
            || (s.restore is Listing && e is RestoreListed) || (s.restore is Fold && e is RestoreFolded),
    ensures
        restore_rank(step_spec(s, e).0.restore) < restore_rank(s.restore),
{
}

/// THEOREM (progress, a refused restore retries): after a refusal the same request starts a restore again, and a fresh artifact
/// (ahead, with its window intact) runs to the reply in four answers or fewer.
pub proof fn a_restore_with_a_fresh_artifact_always_completes(s: State, has_deletes: bool)
    requires
        s.restore is Idle,
    ensures
        ({
            let (s1, a1) = step_spec(s, Event::RepairRequested(RepairKind::Restore));
            let (s2, a2) = step_spec(s1, Event::RestoreAhead { ahead: true });
            let (s3, a3) = step_spec(s2, Event::RestoreDiffed { has_deletes });
            let (s4, a4) = if has_deletes {
                step_spec(s3, Event::RestoreListed { window_ok: true })
            } else {
                (s3, Seq::<Action>::empty())
            };
            let (s5, a5) = step_spec(s4, Event::RestoreFolded);
            s5.restore is Idle && a5.contains(Action::ReplyRestored) && a5.contains(Action::CommitCursor)
        }),
{
    broadcast use vstd::seq_lib::group_seq_properties;
}

/// THEOREM (progress, the resync): a resync's ack is always released by the answer that its deletes are folded; the rank is
/// the one pending ack.
pub proof fn a_resync_is_always_released(s: State)
    requires
        s.relisting,
    ensures
        step_spec(s, Event::RelistFolded).1 == seq![Action::AckResync],
        !step_spec(s, Event::RelistFolded).0.relisting,
{
}

} // verus!
