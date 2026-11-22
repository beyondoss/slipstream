//! The fleet, proven with no bound: any number of nodes, keys, writes, evictions, restarts and stream recreations.
//! `tests/model_fleet.rs` model-checks three nodes and one key (Stateright); this is its unbounded twin.
//!
//! # What is proven
//!
//! A spec of the log ([`Log`]: writes, deletes, retention eviction, incarnation) and a spec of N nodes ([`Node`]), each running
//! the production cursor kernel (`applied::step_spec`, through `applied_proofs::w_step`) and the production repair decisions
//! (`restore_allowed`, `plan_repair`, `listing_truth`, `restore_key`, `cursorless_start_needs_repair`, `same_incarnation`,
//! `pointer_publish_allowed`), through their `*_spec` tables. Nothing here restates a decision: every guard below is one of those
//! specs (the kernel's own `ensures` says the code equals it). The invariant [`finv`] is inductive over every action of every
//! node ([`every_fleet_action_preserves_the_invariant`]) and holds of the initial fleet ([`a_fresh_fleet_satisfies_the_invariant`]):
//!
//! * every node of the current incarnation holds a fold that is the truth at its cursor, and
//! * every published artifact of the current incarnation is the truth at its cursor,
//!
//! where "the truth at its cursor `c` on key `k`" is the last write to `k` at or before `c` (a delete is `None`).
//!
//! # The statement, exactly (F3)
//!
//! The Stateright header used to say "equals the truth at its cursor". On one key with an atomic repair that is the invariant; on
//! many keys it is not what the code keeps, by design: an artifact at cursor `C` is complete *together with the log after `C`*
//! (see `restore_key`): a key whose latest write is after `C` may hold an older value in it, and the resume delivers that write.
//! So the invariant is [`complete`]: the fold equals the truth at its cursor on every key that has not been written after the
//! cursor. At the head that is every key, so a converged node's fold is exactly the truth at the head
//! ([`a_converged_fold_is_the_truth`]). [`finding_f3_the_fold_is_not_the_truth_on_a_key_rewritten_after_the_cursor`] is a concrete
//! restore where the fold at cursor `C` is not the truth at `C`, by the kernel's own `restore_key` table.
//!
//! # Findings, and what each one changed (every one is a Verus-checked concrete state of the shipped decisions)
//!
//! * **F1** the pointer guard compared ranks only: a dead incarnation's pointer above the new head refused every publish of the
//!   new stream while `same_incarnation` refused to restore from it. FIXED: a pointer of another incarnation is replaceable
//!   (`protocol::Pos`: ranks are ordered only within an incarnation, no clock is assumed), so the new stream's first publish
//!   replaces the dead pointer; the price is F1b below. [`finding_f1_*`] states the fix; the liveness
//!   theorem no longer assumes the state away.
//! * **F2** a restore over a fold of a recreated stream kept stale entries by revision number, and refused an artifact below the
//!   old cursor's rank. FIXED: a fold whose cursor is no position of this stream (none, or another incarnation's, which the repair
//!   forgets) is replaced by the artifact key for key (`restore_key`'s `local_known`). [`finding_f2_*`] states the fix; a restore
//!   needs no premise on the local entries.
//! * **F3** the invariant is `complete`, not exact equality (above).
//! * **F4** the eviction memo outlived the incarnation: a fleet of foreign nodes blocked on a restore the exact listing could
//!   serve. FIXED: the repair forgets the old cursor, memo included (`Detect`). [`finding_f4_*`] states the fix.
//!
//! # What the shell and the environment must supply
//!
//! * the restore's steps (chunks, commit) interleave with crashes (a crash mid-restore leaves a store that is complete at its old
//!   cursor: [`rst_ok`]); the relist (the key-listing repair) is one atomic step, as in `tests/model_fleet.rs` (its steps are
//!   `tests/model_repair.rs` and `tests/resync.rs`); the restore's diff and listing events run inside [`restore_begin_w`], which
//!   needs the ahead check, the window re-check and no eviction in between (a refusal is a stutter: `blocked_by_artifact`);
//! * **A1** retention config is constant inside an incarnation (a toggle is what the `seen_evicting` memo guards, so
//!   `mut_no_eviction_memo` is not observable in this world); `seen` is carried inside an incarnation and only ever narrows trust;
//!   the repair of a foreign cursor forgets it (the memo is a fact about the stream that was evicting);
//! * **A2** positions are `u64`; the stream's revision numbers fit (below `2^63`); incarnations (creation times) fit `u64` (no
//!   order between incarnations is assumed);
//! * **A3** the pointer object is written only by publishes (`transport_proofs`, O2);
//! * a cursor with no recorded origin (written before cursors recorded one) is excluded: `tests/model_incarnation.rs` owns it;
//! * a fold that holds data and NO cursor (a torn first checkpoint) names its stream: `Node::origin` is the fold's durable record
//!   of its incarnation (written before its first data record: `AppendLogSnapshot::apply`; with the data in one atomic batch in
//!   the other backends), and the shell judges it against the stream that exists now (`cursorless_fold_is_this_stream`, proven
//!   in `protocol.rs`: an exact match is this stream's; another incarnation, or no record, is foreign: one origin is
//!   `Node::origin != l.origin`, the foreign path of every theorem below);
//! * the liveness fairness axioms below (F-a to F-d).

use crate::kernels::applied::*;
use crate::kernels::applied_proofs::*;
use crate::kernels::protocol::*;
use vstd::prelude::*;

verus! {

// ---------------------------------------------------------------------------------------------------------------------
// The log and the truth.
// ---------------------------------------------------------------------------------------------------------------------

pub type Fold = spec_fn(nat) -> Option<nat>;

/// One write: revision `r` (1-based) is `ops[r - 1]`.
pub struct Op {
    pub key: nat,
    pub put: bool,
}

pub struct Log {
    /// The stream incarnation: strictly increases on a recreation.
    pub origin: nat,
    pub ops: Seq<Op>,
    /// The first retained revision.
    pub first: nat,
    /// Retention can evict current values (`max_age`, `discard: old`).
    pub evicts: bool,
}

pub open spec fn head(l: Log) -> nat {
    l.ops.len()
}

pub open spec fn umax() -> nat {
    u64::MAX as nat
}

/// The truth on `k` at cursor `c`: the last write to `k` at or before `c` (a delete is `None`); `Some(revision)`.
pub open spec fn truth(ops: Seq<Op>, c: nat, k: nat) -> Option<nat>
    decreases c,
{
    if c == 0 {
        None
    } else if c > ops.len() {
        truth(ops, ops.len(), k)
    } else if ops[(c - 1) as int].key == k {
        if ops[(c - 1) as int].put { Some(c) } else { None }
    } else {
        truth(ops, (c - 1) as nat, k)
    }
}

/// `k` is written after `c`.
pub open spec fn touched(ops: Seq<Op>, c: nat, k: nat) -> bool {
    exists|j: int| c <= j && j < ops.len() && #[trigger] ops[j].key == k
}

/// A fold entry `Some(l)` is a real write: revision `l` is a put of `k`.
pub open spec fn real_entry(ops: Seq<Op>, k: nat, l: nat) -> bool {
    1 <= l && l <= ops.len() && ops[(l - 1) as int].key == k && ops[(l - 1) as int].put
}

pub open spec fn real(g: Fold, ops: Seq<Op>) -> bool {
    forall|k: nat| (#[trigger] g(k)) is Some ==> real_entry(ops, k, g(k)->Some_0)
}

/// THE statement: the fold is the truth at `c` on every key not written after `c` ("complete together with the log after `c`").
pub open spec fn complete(g: Fold, ops: Seq<Op>, c: nat) -> bool {
    forall|k: nat| !touched(ops, c, k) ==> #[trigger] g(k) == truth(ops, c, k)
}

/// A real fold, complete at `c`, with `c` a position of the log.
pub open spec fn ok_fold(g: Fold, ops: Seq<Op>, c: nat) -> bool {
    real(g, ops) && complete(g, ops, c) && c <= ops.len()
}

/// The bucket's key listing (live keys only): what a key-listing repair reads. A value evicted from the log is not listed.
pub open spec fn listed(l: Log, k: nat) -> Option<nat> {
    match truth(l.ops, l.ops.len(), k) {
        Some(r) => if r >= l.first { Some(r) } else { None },
        None => None,
    }
}

pub open spec fn live_rev(l: Log, k: nat) -> Option<nat> {
    truth(l.ops, l.ops.len(), k)
}

pub open spec fn log_ok(l: Log) -> bool {
    &&& l.origin <= umax()
    &&& 1 <= l.first
    &&& l.first <= l.ops.len() + 1
    &&& l.ops.len() < 0x8000_0000_0000_0000
    // A bucket that keeps current values never evicts one.
    &&& (!l.evicts ==> forall|k: nat| (#[trigger] live_rev(l, k)) is Some ==> live_rev(l, k)->Some_0 >= l.first)
}

// ---------------------------------------------------------------------------------------------------------------------
// Lemmas about the truth.
// ---------------------------------------------------------------------------------------------------------------------

pub proof fn truth_clamp(ops: Seq<Op>, c: nat, k: nat)
    requires
        c >= ops.len(),
    ensures
        truth(ops, c, k) == truth(ops, ops.len(), k),
{
}

/// The truth only ever names real writes, at or before the cursor.
pub proof fn truth_is_real(ops: Seq<Op>, c: nat, k: nat)
    ensures
        truth(ops, c, k) is Some ==> real_entry(ops, k, truth(ops, c, k)->Some_0) && truth(ops, c, k)->Some_0 <= c,
    decreases c,
{
    if c == 0 {
    } else if c > ops.len() {
        truth_is_real(ops, ops.len(), k);
    } else if ops[(c - 1) as int].key == k {
    } else {
        truth_is_real(ops, (c - 1) as nat, k);
    }
}

/// A key not written after `c` has the same truth at every later cursor.
pub proof fn truth_untouched(ops: Seq<Op>, c: nat, c2: nat, k: nat)
    requires
        c <= c2,
        c2 <= ops.len(),
        !touched(ops, c, k),
    ensures
        truth(ops, c2, k) == truth(ops, c, k),
    decreases c2 - c,
{
    if c2 > c {
        assert(ops[(c2 - 1) as int].key != k) by {
            if ops[(c2 - 1) as int].key == k {
                assert(touched(ops, c, k));
            }
        }
        truth_untouched(ops, c, (c2 - 1) as nat, k);
    }
}

/// `touched` shrinks as the cursor rises.
pub proof fn touched_monotone(ops: Seq<Op>, c: nat, c2: nat, k: nat)
    requires
        c <= c2,
        touched(ops, c2, k),
    ensures
        touched(ops, c, k),
{
    let j = choose|j: int| c2 <= j && j < ops.len() && #[trigger] ops[j].key == k;
    assert(c <= j && j < ops.len() && ops[j].key == k);
}

/// A real put at or before `c` is not later than the truth at `c`, when the truth is a put: the truth is the LAST write.
pub proof fn truth_is_the_last_write(ops: Seq<Op>, c: nat, k: nat, l: nat)
    requires
        c <= ops.len(),
        real_entry(ops, k, l),
        l <= c,
        truth(ops, c, k) is Some,
    ensures
        l <= truth(ops, c, k)->Some_0,
    decreases c,
{
    if c == l {
    } else if ops[(c - 1) as int].key == k {
    } else {
        truth_is_the_last_write(ops, (c - 1) as nat, k, l);
    }
}

/// Appending a write changes nothing at or below the old head.
pub proof fn truth_append(ops: Seq<Op>, op: Op, c: nat, k: nat)
    requires
        c <= ops.len(),
    ensures
        truth(ops.push(op), c, k) == truth(ops, c, k),
    decreases c,
{
    let ops2 = ops.push(op);
    if c == 0 {
    } else if ops[(c - 1) as int].key == k {
        assert(ops2[(c - 1) as int] == ops[(c - 1) as int]);
    } else {
        assert(ops2[(c - 1) as int] == ops[(c - 1) as int]);
        truth_append(ops, op, (c - 1) as nat, k);
    }
}

/// A real fold is complete at cursor 0: a key with no write at all has no entry.
pub proof fn a_real_fold_is_complete_at_zero(g: Fold, ops: Seq<Op>)
    requires
        real(g, ops),
    ensures
        complete(g, ops, 0),
{
    assert forall|k: nat| !touched(ops, 0, k) implies #[trigger] g(k) == truth(ops, 0, k) by {
        if g(k) is Some {
            let l = g(k)->Some_0;
            assert(real_entry(ops, k, l));
            assert(0 <= (l - 1) as int && ((l - 1) as int) < ops.len() && ops[(l - 1) as int].key == k);
            assert(touched(ops, 0, k));
        }
    }
}

/// The listing is the truth when the planner trusts it: the production `listing_is_truth` (Verus-proven equal to its spec) on a
/// log whose retention keeps current values, or has evicted nothing.
pub proof fn the_trusted_listing_is_the_truth(l: Log, seen: bool, k: nat)
    requires
        log_ok(l),
        listing_is_truth_spec(l.evicts, l.first as u64, seen),
    ensures
        listed(l, k) == truth(l.ops, l.ops.len(), k),
{
    truth_is_real(l.ops, l.ops.len(), k);
    if !l.evicts {
        assert(live_rev(l, k) == truth(l.ops, l.ops.len(), k));
    } else {
        assert(l.first <= 1);
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Folding the log into a fold.
// ---------------------------------------------------------------------------------------------------------------------

/// Apply write `r` to a fold.
pub open spec fn fold_op(g: Fold, ops: Seq<Op>, r: nat) -> Fold {
    if 1 <= r && r <= ops.len() {
        |k: nat| if k == ops[(r - 1) as int].key { if ops[(r - 1) as int].put { Some(r) } else { None } } else { g(k) }
    } else {
        g
    }
}

/// Apply writes `a + 1 ..= b` in order.
pub open spec fn fold_range(g: Fold, ops: Seq<Op>, a: nat, b: nat) -> Fold
    decreases (if b > a { b - a } else { 0int }),
{
    if b > a { fold_op(fold_range(g, ops, a, (b - 1) as nat), ops, b) } else { g }
}

pub proof fn fold_op_keeps_ok(g: Fold, ops: Seq<Op>, c: nat)
    requires
        ok_fold(g, ops, c),
        c < ops.len(),
    ensures
        ok_fold(fold_op(g, ops, c + 1), ops, c + 1),
{
    let g2 = fold_op(g, ops, c + 1);
    let op = ops[c as int];
    assert forall|k: nat| (#[trigger] g2(k)) is Some implies real_entry(ops, k, g2(k)->Some_0) by {
        if k == op.key {
            assert(real_entry(ops, k, c + 1));
        } else {
        }
    }
    assert forall|k: nat| !touched(ops, c + 1, k) implies #[trigger] g2(k) == truth(ops, c + 1, k) by {
        if k != op.key {
            assert(!touched(ops, c, k)) by {
                if touched(ops, c, k) {
                    let j = choose|j: int| c <= j && j < ops.len() && #[trigger] ops[j].key == k;
                    assert(j != c as int);
                    assert(c + 1 <= j);
                    assert(touched(ops, c + 1, k));
                }
            }
        }
    }
}

/// THEOREM (delivery keeps the fold complete): folding the log from `a` to `b` takes a fold complete at `a` to one complete at `b`.
pub proof fn folding_the_log_forward_keeps_a_fold_ok(g: Fold, ops: Seq<Op>, a: nat, b: nat)
    requires
        ok_fold(g, ops, a),
        a <= b,
        b <= ops.len(),
    ensures
        ok_fold(fold_range(g, ops, a, b), ops, b),
    decreases b - a,
{
    if b > a {
        folding_the_log_forward_keeps_a_fold_ok(g, ops, a, (b - 1) as nat);
        fold_op_keeps_ok(fold_range(g, ops, a, (b - 1) as nat), ops, (b - 1) as nat);
    }
}

/// A fold stays ok when a write is appended: nothing it claims is changed.
pub proof fn ok_fold_append(g: Fold, ops: Seq<Op>, op: Op, c: nat)
    requires
        ok_fold(g, ops, c),
    ensures
        ok_fold(g, ops.push(op), c),
{
    let ops2 = ops.push(op);
    assert forall|k: nat| (#[trigger] g(k)) is Some implies real_entry(ops2, k, g(k)->Some_0) by {
        let l = g(k)->Some_0;
        assert(real_entry(ops, k, l));
        assert(ops2[(l - 1) as int] == ops[(l - 1) as int]);
    }
    assert forall|k: nat| !touched(ops2, c, k) implies #[trigger] g(k) == truth(ops2, c, k) by {
        assert(!touched(ops, c, k)) by {
            if touched(ops, c, k) {
                let j = choose|j: int| c <= j && j < ops.len() && #[trigger] ops[j].key == k;
                assert(ops2[j] == ops[j]);
                assert(touched(ops2, c, k));
            }
        }
        truth_append(ops, op, c, k);
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The production decisions, through their specs. A mutation forges the decision's INPUT exactly as the code's `mut_*` feature
// forges it, so a mutated build fails the induction below.
// ---------------------------------------------------------------------------------------------------------------------

/// `resume_window_ok` on positions.
pub open spec fn win(c: nat, first: nat) -> bool {
    resume_window_spec(c as u64, first as u64)
}

/// `restore_allowed`.
pub open spec fn restore_ok(artifact: nat, local: nat, first: nat) -> bool {
    if cfg!(feature = "mut_no_restore_guard") {
        true
    } else {
        restore_allowed_spec(artifact as u64, local as u64, first as u64)
    }
}

/// `listing_truth` on a backend that reports retention.
pub open spec fn listing_lt(evicts: bool, first: nat, seen: bool) -> Option<bool> {
    if cfg!(feature = "mut_relist_on_evicting") {
        Some(true)
    } else {
        listing_truth_spec(Some((evicts, first as u64)), seen && !cfg!(feature = "mut_no_eviction_memo"))
    }
}

/// `plan_repair` under the shipped `ExpiryRepair::Auto`.
pub open spec fn plan_of(lt: Option<bool>) -> RepairPlan {
    plan_repair_spec(RepairMode::Auto, lt)
}

/// `cursorless_start_needs_repair` under `Auto`.
pub open spec fn needs_repair_first(has_data: bool, lt: Option<bool>) -> bool {
    cursorless_spec(RepairMode::Auto, has_data && !cfg!(feature = "mut_unanchored_relist"), lt)
}

pub open spec fn key_state(o: Option<nat>) -> KeyState {
    match o {
        Some(r) => KeyState::At(if cfg!(feature = "mut_restore_ignores_versions") { None } else { Some(r as u64) }),
        None => KeyState::Absent,
    }
}

/// `restore_key` for one key: the local entry, the artifact's, whether the bucket lists the key live, and whether the local fold
/// is KNOWN to be this stream's (it has a cursor of it).
pub open spec fn key_decision(local: Option<nat>, art: Option<nat>, listed_live: bool, known: bool) -> KeyRestore {
    restore_key_spec(
        key_state(local),
        key_state(art),
        local == art,
        listed_live && !cfg!(feature = "mut_restore_ignores_listing"),
        known || cfg!(feature = "mut_restore_trusts_foreign"),
    )
}

/// What the restore leaves on each key. `known`: the local fold is of THIS stream (the repair saw no foreign cursor); a fold of
/// another incarnation is replaced by the artifact key for key.
pub open spec fn restore_result(l: Log, art: Fold, old: Fold, known: bool) -> Fold {
    |k: nat| match key_decision(old(k), art(k), listed(l, k) is Some, known) {
        KeyRestore::Keep => old(k),
        KeyRestore::TakeArtifact => art(k),
        KeyRestore::Delete => None,
    }
}

pub open spec fn has_data(g: Fold) -> bool {
    exists|k: nat| #[trigger] g(k) is Some
}

// ---------------------------------------------------------------------------------------------------------------------
// The kernel's side: what each fleet step does to a node's `World` (the production kernel, one event at a time).
// ---------------------------------------------------------------------------------------------------------------------

/// A revision of the stream is received and queued.
pub open spec fn recv_w(w: World) -> World {
    w_step(w, Event::Received { positioned: true, queued: Queued { batch: 1, raw: 1 }, max: 1000 }, 0)
}

/// A flush asks, the store fold lands.
pub open spec fn flush_w(w: World) -> World {
    let w1 = w_step(w, Event::FlushRequested { queued: Queued { batch: 0, raw: 1 }, has_store: true }, 0);
    w_step(w1, Event::StoreDone(StoreOutcome::Ok), 0)
}

/// A restore starts against an artifact at cursor `a` and runs to the chunk fold.
pub open spec fn restore_begin_w(w: World, a: nat, hd: bool) -> World {
    let w1 = w_step(w, Event::RepairRequested(RepairKind::Restore), a);
    let w2 = w_step(w1, Event::RestoreAhead { ahead: true }, 0);
    let w3 = w_step(w2, Event::RestoreDiffed { has_deletes: hd }, 0);
    if hd { w_step(w3, Event::RestoreListed { window_ok: true }, 0) } else { w3 }
}

/// The chunks are folded: the artifact's cursor is committed and flushed.
pub open spec fn restore_commit_w(w: World) -> World {
    let w1 = w_step(w, Event::RestoreFolded, 0);
    let w2 = w_step(w1, Event::FlushRequested { queued: Queued { batch: 0, raw: 1 }, has_store: true }, 0);
    w_step(w2, Event::StoreDone(StoreOutcome::Ok), 0)
}

pub proof fn a_received_revision_keeps_the_kernel_invariant(w: World)
    requires
        inv(w),
        w.k.flush is Idle,
        w.k.restore is Idle,
    ensures
        inv(recv_w(w)),
        recv_w(w).store_cursor == w.store_cursor,
        recv_w(w).delivered == w.delivered + 1,
        recv_w(w).k.flush is Idle,
        recv_w(w).k.restore is Idle,
        recv_w(w).k.high,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    every_event_preserves_the_invariant(
        w,
        Event::Received { positioned: true, queued: Queued { batch: 1, raw: 1 }, max: 1000 },
        0,
    );
}

pub proof fn a_flush_lands_the_store_at_the_delivered_position(w: World)
    requires
        inv(w),
        w.k.flush is Idle,
        w.k.restore is Idle,
        w.store_cursor < w.delivered,
    ensures
        inv(flush_w(w)),
        flush_w(w).store_cursor == w.delivered,
        flush_w(w).delivered == w.delivered,
        flush_w(w).k.flush is Idle,
        flush_w(w).k.restore is Idle,
        !flush_w(w).k.high,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    let q = Queued { batch: 0, raw: 1 };
    a_landed_fold_brings_the_store_to_the_head(w, q);
    let w1 = w_step(w, Event::FlushRequested { queued: q, has_store: true }, 0);
    every_event_preserves_the_invariant(w, Event::FlushRequested { queued: q, has_store: true }, 0);
    every_event_preserves_the_invariant(w1, Event::StoreDone(StoreOutcome::Ok), 0);
}

pub proof fn a_restore_runs_from_a_settled_node_to_the_chunk_fold(w: World, a: nat, hd: bool)
    requires
        inv(w),
        w.k.flush is Idle,
        w.k.restore is Idle,
        !w.k.high,
        w.store_cursor == w.delivered,
        a > w.store_cursor,
    ensures
        inv(restore_begin_w(w, a, hd)),
        restore_begin_w(w, a, hd).k.restore is Fold,
        restore_begin_w(w, a, hd).k.flush is Idle,
        restore_begin_w(w, a, hd).art == a,
        restore_begin_w(w, a, hd).store_cursor == w.store_cursor,
        restore_begin_w(w, a, hd).delivered == w.delivered,
        !restore_begin_w(w, a, hd).k.high,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    let w1 = w_step(w, Event::RepairRequested(RepairKind::Restore), a);
    every_event_preserves_the_invariant(w, Event::RepairRequested(RepairKind::Restore), a);
    let w2 = w_step(w1, Event::RestoreAhead { ahead: true }, 0);
    every_event_preserves_the_invariant(w1, Event::RestoreAhead { ahead: true }, 0);
    let w3 = w_step(w2, Event::RestoreDiffed { has_deletes: hd }, 0);
    every_event_preserves_the_invariant(w2, Event::RestoreDiffed { has_deletes: hd }, 0);
    if hd {
        every_event_preserves_the_invariant(w3, Event::RestoreListed { window_ok: true }, 0);
    }
}

pub proof fn a_restore_commit_lands_the_store_at_the_artifacts_cursor(w: World)
    requires
        inv(w),
        w.k.flush is Idle,
        w.k.restore is Fold,
    ensures
        inv(restore_commit_w(w)),
        restore_commit_w(w).store_cursor == w.art,
        restore_commit_w(w).delivered == w.art,
        restore_commit_w(w).k.flush is Idle,
        restore_commit_w(w).k.restore is Idle,
        !restore_commit_w(w).k.high,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    let w1 = w_step(w, Event::RestoreFolded, 0);
    every_event_preserves_the_invariant(w, Event::RestoreFolded, 0);
    let q = Queued { batch: 0, raw: 1 };
    let w2 = w_step(w1, Event::FlushRequested { queued: q, has_store: true }, 0);
    every_event_preserves_the_invariant(w1, Event::FlushRequested { queued: q, has_store: true }, 0);
    every_event_preserves_the_invariant(w2, Event::StoreDone(StoreOutcome::Ok), 0);
}

// ---------------------------------------------------------------------------------------------------------------------
// The fleet.
// ---------------------------------------------------------------------------------------------------------------------

/// A published artifact: the cursor it holds, the incarnation that cursor belongs to, its content.
pub struct Art {
    pub cursor: nat,
    pub origin: nat,
    pub fold: Fold,
}

/// A restore in flight: the artifact, the fold it started from, and the fold it ends with (what the diff decided per key).
pub struct Rst {
    pub art: Art,
    pub old: Fold,
    pub result: Fold,
    /// The old fold was of this stream (its incarnation is the current one) when the restore began: its entries were trusted.
    pub known: bool,
}

/// One node: the production kernel's world (`w`, whose `store_cursor` is the persisted cursor), the STORE's content, and
/// the incarnation that content belongs to.
pub struct Node {
    pub w: World,
    pub fold: Fold,
    pub origin: nat,
    /// A cursor is persisted (else `w.store_cursor == 0` and the start is cursor-less).
    pub anchored: bool,
    /// The fold has seen retention evict current values (the memo).
    pub seen: bool,
    pub rst: Option<Rst>,
}

pub struct F {
    pub log: Log,
    pub nodes: Seq<Node>,
    pub ptr: Option<Art>,
}

pub enum Act {
    Put(nat),
    Del(nat),
    /// Head eviction raises the first retained revision to `.0`.
    Evict(nat),
    /// The stream is deleted and recreated (new incarnation, retention `.0`).
    Recreate(bool),
    /// A revision reaches node `.0`'s kernel.
    Advance(int),
    /// A flush lands node `.0`'s store at what it received.
    Flush(int),
    /// Node `.0` crashes and restarts.
    Crash(int),
    /// The floor guard sees the cursor belongs to another incarnation: the fold is repaired as a cursor-less one.
    Detect(int),
    /// The fold sees retention evict current values.
    See(int),
    /// The key-listing repair (`ExpiryRepair::Auto` chose it), or the plain start of a cursor-less empty fold.
    Relist(int),
    /// A restore starts and folds some of its chunks (`.2` picks which keys); `.1`: there are delete candidates.
    RestoreBegin(int, bool, spec_fn(nat) -> bool),
    /// The rest of the chunks fold.
    FoldChunks(int),
    /// The restore's cursor commits and flushes.
    RestoreEnd(int),
    /// Node `.0` exports its store; the publish guard decides.
    Publish(int),
}

pub open spec fn valid(f: F, i: int) -> bool {
    0 <= i < f.nodes.len()
}

/// A position in the artifact order (`protocol::Pos`): the incarnation, then the cursor. A mutation forges the incarnation away
/// exactly as `mut_pointer_ignores_incarnation` does in the code.
pub open spec fn art_pos(origin: nat, cursor: nat) -> Pos {
    Pos { origin: if cfg!(feature = "mut_pointer_ignores_incarnation") { 0 } else { origin as u64 }, rank: cursor as u64 }
}

pub open spec fn ptr_state(p: Option<Art>) -> PointerState {
    match p {
        None => PointerState::Absent,
        Some(a) => PointerState::Present { at: Some(art_pos(a.origin, a.cursor)) },
    }
}

/// The kernel has nothing in flight: the pre-repair settle has run.
pub open spec fn settled(n: Node) -> bool {
    n.w.k.flush is Idle && n.w.k.restore is Idle && !n.w.k.high && n.w.store_cursor == n.w.delivered && n.rst is None
}

/// The node's cursor is expired (or it has none) and the fold's own stream is the current one.
pub open spec fn repairable(l: Log, n: Node) -> bool {
    !n.anchored || (n.origin == l.origin && !win(n.w.store_cursor, l.first))
}

pub open spec fn lt_of(l: Log, n: Node) -> Option<bool> {
    listing_lt(l.evicts, l.first, n.seen)
}

/// The fold after the key-listing repair: the listing replaces the fold; a plain start of a fold that needs no repair re-lists
/// OVER it (a re-list never removes what it does not deliver).
pub open spec fn relist_result(l: Log, n: Node) -> Fold {
    |k: nat| if n.anchored || needs_repair_first(has_data(n.fold), lt_of(l, n)) {
        listed(l, k)
    } else {
        match listed(l, k) {
            Some(r) => Some(r),
            None => (n.fold)(k),
        }
    }
}

pub open spec fn rst_of(n: Node) -> Rst {
    n.rst->Some_0
}

pub open spec fn node_at(f: F, i: int) -> Node {
    f.nodes[i]
}

pub open spec fn enabled(f: F, a: Act) -> bool {
    let l = f.log;
    match a {
        Act::Put(k) => l.ops.len() + 1 < 0x8000_0000_0000_0000,
        Act::Del(k) => l.ops.len() + 1 < 0x8000_0000_0000_0000,
        Act::Evict(g) => {
            &&& l.first < g
            &&& g <= l.ops.len() + 1
            &&& (l.evicts || forall|k: nat| (#[trigger] live_rev(l, k)) is Some ==> live_rev(l, k)->Some_0 >= g)
        },
        Act::Recreate(e) => l.origin + 1 <= umax(),
        Act::Advance(i) => {
            valid(f, i) && ({
                let n = node_at(f, i);
                &&& n.anchored
                &&& n.origin == l.origin
                &&& n.rst is None
                &&& n.w.k.restore is Idle
                &&& n.w.k.flush is Idle
                &&& win(n.w.delivered, l.first)
                &&& n.w.delivered < l.ops.len()
            })
        },
        Act::Flush(i) => {
            valid(f, i) && ({
                let n = node_at(f, i);
                n.w.k.flush is Idle && n.w.k.restore is Idle && n.w.store_cursor < n.w.delivered
            })
        },
        Act::Crash(i) => valid(f, i),
        Act::Detect(i) => valid(f, i) && node_at(f, i).anchored && node_at(f, i).origin != l.origin,
        Act::See(i) => valid(f, i) && l.evicts,
        Act::Relist(i) => {
            valid(f, i) && ({
                let n = node_at(f, i);
                repairable(l, n) && n.rst is None && plan_of(lt_of(l, n)) is Relist
            })
        },
        Act::RestoreBegin(i, hd, pick) => {
            valid(f, i) && ({
                let n = node_at(f, i);
                &&& repairable(l, n)
                &&& settled(n)
                &&& plan_of(lt_of(l, n)) is Restore
                &&& f.ptr is Some
                // The artifact is of THIS stream (`same_incarnation`) and `restore_allowed` (ahead, inside the window).
                &&& f.ptr->Some_0.origin == l.origin
                &&& restore_ok(f.ptr->Some_0.cursor, n.w.store_cursor, l.first)
            })
        },
        Act::FoldChunks(i) => valid(f, i) && node_at(f, i).rst is Some,
        Act::RestoreEnd(i) => {
            valid(f, i) && node_at(f, i).rst is Some
                && forall|k: nat| (#[trigger] (node_at(f, i).fold)(k)) == (rst_of(node_at(f, i)).result)(k)
        },
        Act::Publish(i) => {
            valid(f, i) && ({
                let n = node_at(f, i);
                n.anchored && n.w.store_cursor >= 1 && n.origin <= umax()
                    && publish_allowed_spec(ptr_state(f.ptr), art_pos(n.origin, n.w.store_cursor))
            })
        },
    }
}

pub open spec fn advance_node(n: Node) -> Node {
    Node { w: recv_w(n.w), ..n }
}

pub open spec fn flush_node(l: Log, n: Node) -> Node {
    // The flush folds the raw queue, which is `(raw_from, delivered]`.
    Node { w: flush_w(n.w), fold: fold_range(n.fold, l.ops, n.w.raw_from, n.w.delivered), ..n }
}

pub open spec fn crash_node(n: Node) -> Node {
    Node { w: restart(n.w), rst: None, ..n }
}

pub open spec fn detect_node(n: Node) -> Node {
    // The cursor is forgotten with its incarnation and its eviction memo: the fold holds data and no cursor.
    Node { w: init_world(0, 0), anchored: false, rst: None, seen: n.seen && cfg!(feature = "mut_foreign_keeps_memo"), ..n }
}

pub open spec fn relist_node(l: Log, n: Node) -> Node {
    Node {
        w: init_world(l.ops.len(), l.ops.len()),
        fold: relist_result(l, n),
        origin: l.origin,
        anchored: true,
        seen: n.seen,
        rst: None,
    }
}

pub open spec fn begin_node(l: Log, n: Node, a: Art, hd: bool, pick: spec_fn(nat) -> bool) -> Node {
    let result = restore_result(l, a.fold, n.fold, n.origin == l.origin);
    Node {
        w: restore_begin_w(n.w, a.cursor, hd),
        fold: |k: nat| if pick(k) { result(k) } else { (n.fold)(k) },
        rst: Some(Rst { art: a, old: n.fold, result, known: n.origin == l.origin }),
        ..n
    }
}

pub open spec fn chunks_node(n: Node) -> Node {
    Node { fold: rst_of(n).result, ..n }
}

pub open spec fn end_node(n: Node) -> Node {
    Node { w: restore_commit_w(n.w), origin: rst_of(n).art.origin, anchored: true, rst: None, ..n }
}

pub open spec fn set_node(f: F, i: int, n: Node) -> F {
    F { nodes: f.nodes.update(i, n), ..f }
}

pub open spec fn step(f: F, a: Act) -> F {
    let l = f.log;
    match a {
        Act::Put(k) => F { log: Log { ops: l.ops.push(Op { key: k, put: true }), ..l }, ..f },
        Act::Del(k) => F { log: Log { ops: l.ops.push(Op { key: k, put: false }), ..l }, ..f },
        Act::Evict(g) => F { log: Log { first: g, ..l }, ..f },
        Act::Recreate(e) => F { log: Log { origin: l.origin + 1, ops: Seq::empty(), first: 1, evicts: e }, ..f },
        Act::Advance(i) => set_node(f, i, advance_node(node_at(f, i))),
        Act::Flush(i) => set_node(f, i, flush_node(l, node_at(f, i))),
        Act::Crash(i) => set_node(f, i, crash_node(node_at(f, i))),
        Act::Detect(i) => set_node(f, i, detect_node(node_at(f, i))),
        Act::See(i) => set_node(f, i, Node { seen: true, ..node_at(f, i) }),
        Act::Relist(i) => set_node(f, i, relist_node(l, node_at(f, i))),
        Act::RestoreBegin(i, hd, pick) => set_node(f, i, begin_node(l, node_at(f, i), f.ptr->Some_0, hd, pick)),
        Act::FoldChunks(i) => set_node(f, i, chunks_node(node_at(f, i))),
        Act::RestoreEnd(i) => set_node(f, i, end_node(node_at(f, i))),
        Act::Publish(i) => {
            let n = node_at(f, i);
            F { ptr: Some(Art { cursor: n.w.store_cursor, origin: n.origin, fold: n.fold }), ..f }
        },
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The invariant.
// ---------------------------------------------------------------------------------------------------------------------

pub open spec fn rst_ok(l: Log, n: Node) -> bool {
    match n.rst {
        None => true,
        Some(r) => {
            &&& ok_fold(r.art.fold, l.ops, r.art.cursor)
            &&& r.art.cursor > n.w.store_cursor
            &&& ok_fold(r.old, l.ops, n.w.store_cursor)
            &&& ok_fold(r.result, l.ops, n.w.store_cursor)
            &&& ok_fold(r.result, l.ops, r.art.cursor)
            // The store holds some chunks of the result and the rest of the old fold.
            &&& forall|k: nat| (#[trigger] (n.fold)(k)) == (r.old)(k) || (n.fold)(k) == (r.result)(k)
        },
    }
}

/// What a node of the CURRENT incarnation claims.
pub open spec fn claims(l: Log, n: Node) -> bool {
    &&& n.w.delivered <= l.ops.len()
    &&& ok_fold(n.fold, l.ops, n.w.store_cursor)
    &&& rst_ok(l, n)
}

pub open spec fn node_ok(l: Log, n: Node) -> bool {
    // The cursor authority invariant of the production kernel (`applied_proofs::inv`), per node.
    &&& inv(n.w)
    &&& n.w.k.flush is Idle
    &&& (n.w.k.restore is Idle || n.w.k.restore is Fold)
    &&& (n.w.k.restore is Fold <==> n.rst is Some)
    &&& (n.rst is Some ==> n.w.art == rst_of(n).art.cursor && rst_of(n).art.cursor <= umax() && art_ok(l, rst_of(n).art))
    // A restore of a fold of this stream is of this stream's artifact; of a foreign fold, the restore IS the artifact.
    &&& (n.rst is Some && rst_of(n).known ==> rst_of(n).art.origin == n.origin)
    &&& (n.rst is Some && !rst_of(n).known ==> forall|k: nat| (#[trigger] (rst_of(n).result)(k)) == (rst_of(n).art.fold)(k))
    &&& (n.rst is Some && n.origin == l.origin ==> rst_of(n).art.origin == l.origin)
    &&& n.w.delivered <= umax()
    &&& (!n.anchored ==> n.w.store_cursor == 0 && n.w.delivered == 0)
    // A store that holds everything received has no pending position.
    &&& (n.w.store_cursor == n.w.delivered ==> !n.w.k.high)
    &&& n.origin <= l.origin
    &&& (n.origin == l.origin ==> claims(l, n))
}

pub open spec fn art_ok(l: Log, a: Art) -> bool {
    &&& a.cursor <= umax()
    &&& a.origin <= l.origin
    &&& (a.origin == l.origin ==> ok_fold(a.fold, l.ops, a.cursor))
}

/// THE FLEET INVARIANT: every node of the current incarnation holds a real fold that is the truth at its cursor (on every key not
/// written since), and so does every published artifact of it.
pub open spec fn finv(f: F) -> bool {
    &&& log_ok(f.log)
    &&& forall|i: int| 0 <= i < f.nodes.len() ==> node_ok(f.log, #[trigger] f.nodes[i])
    &&& (f.ptr is Some ==> art_ok(f.log, f.ptr->Some_0))
}

// ---------------------------------------------------------------------------------------------------------------------
// The restore's per-key decision (`restore_key`), over the whole key space.
// ---------------------------------------------------------------------------------------------------------------------

/// THEOREM (a restore leaves a node complete at BOTH cursors): from a real fold complete at `c0` and an artifact complete at
/// `c1 > c0`, the per-key decisions (`restore_key`: take the artifact, keep the local entry, delete it unless the bucket lists
/// the key live) produce a real fold complete at `c0` (so a restore interrupted after any number of chunks leaves the store
/// complete at its old cursor) and complete at `c1` (so the committed cursor names a fold that is complete there).
pub proof fn a_restore_leaves_the_fold_complete_at_both_cursors(l: Log, old: Fold, art: Fold, c0: nat, c1: nat)
    requires
        log_ok(l),
        ok_fold(old, l.ops, c0),
        ok_fold(art, l.ops, c1),
        c0 < c1,
    ensures
        ok_fold(restore_result(l, art, old, true), l.ops, c0),
        ok_fold(restore_result(l, art, old, true), l.ops, c1),
{
    let ops = l.ops;
    let res = restore_result(l, art, old, true);
    assert forall|k: nat| (#[trigger] res(k)) is Some implies real_entry(ops, k, res(k)->Some_0) by {
        assert(old(k) is Some ==> real_entry(ops, k, old(k)->Some_0));
        assert(art(k) is Some ==> real_entry(ops, k, art(k)->Some_0));
    }
    assert forall|k: nat| !touched(ops, c0, k) implies #[trigger] res(k) == truth(ops, c0, k) by {
        if !touched(ops, c0, k) {
            assert(!touched(ops, c1, k)) by {
                if touched(ops, c1, k) {
                    touched_monotone(ops, c0, c1, k);
                }
            }
            truth_untouched(ops, c0, c1, k);
            assert(old(k) == art(k));
            if old(k) is Some {
                assert(real_entry(ops, k, old(k)->Some_0));
            }
        }
    }
    assert forall|k: nat| !touched(ops, c1, k) implies #[trigger] res(k) == truth(ops, c1, k) by {
        if !touched(ops, c1, k) {
            truth_untouched(ops, c1, ops.len(), k);
            truth_clamp(ops, ops.len(), k);
            assert(art(k) == truth(ops, c1, k));
            // The bucket lists a key only if its current value is a write; an untouched key's current value is the truth at `c1`.
            assert(listed(l, k) is Some ==> truth(ops, c1, k) is Some);
            if old(k) is Some {
                let lo = old(k)->Some_0;
                assert(real_entry(ops, k, lo));
                // A write of `k` after `c1` would touch it: so every real write of `k` is at or before `c1`.
                assert(lo <= c1) by {
                    if lo > c1 {
                        assert(c1 <= (lo - 1) as int && ((lo - 1) as int) < ops.len() && ops[(lo - 1) as int].key == k);
                    }
                }
                if art(k) is Some {
                    truth_is_the_last_write(ops, c1, k, lo);
                    let ar = art(k)->Some_0;
                    assert(lo <= ar);
                    assert(ar <= umax() && lo <= umax());
                }
            }
        }
    }
}

/// THEOREM (a restore never moves a key backward, and never deletes a key the bucket lists live): the kernel's `restore_key`
/// guarantees for a fold with a cursor, over every key of the fleet's fold. Every key of the fold: if the local entry survives or is replaced, its
/// revision does not fall; a deleted entry was not listed live.
pub proof fn a_restore_never_moves_a_key_backward_or_deletes_a_live_one(l: Log, old: Fold, art: Fold, k: nat)
    requires
        log_ok(l),
        real(old, l.ops),
        real(art, l.ops),
    ensures
        ({
            let res = restore_result(l, art, old, true);
            &&& (old(k) is Some && res(k) is Some ==> res(k)->Some_0 >= old(k)->Some_0)
            &&& (old(k) is Some && res(k) is None ==> listed(l, k) is None)
        }),
{
    assert(old(k) is Some ==> real_entry(l.ops, k, old(k)->Some_0));
    assert(art(k) is Some ==> real_entry(l.ops, k, art(k)->Some_0));
}

/// THEOREM (a restore of a foreign fold is the artifact): the old fold may be anything, entries and revision numbers of another
/// stream included. The restore leaves exactly the artifact's fold, key for key, so it is real and complete wherever the
/// artifact is, with no premise on what the old fold held (FINDING F2: the revision-number comparison of `restore_key` is for a
/// fold of this stream).
pub proof fn a_restore_over_a_foreign_fold_is_the_artifact(l: Log, old: Fold, art: Fold, k: nat)
    ensures
        restore_result(l, art, old, false)(k) == art(k),
{
}

// ---------------------------------------------------------------------------------------------------------------------
// Node-level steps.
// ---------------------------------------------------------------------------------------------------------------------

proof fn lemma_advance(l: Log, n: Node)
    requires
        log_ok(l),
        node_ok(l, n),
        n.anchored,
        n.origin == l.origin,
        n.rst is None,
        n.w.k.restore is Idle,
        n.w.k.flush is Idle,
        n.w.delivered < l.ops.len(),
    ensures
        node_ok(l, advance_node(n)),
{
    a_received_revision_keeps_the_kernel_invariant(n.w);
}

proof fn lemma_flush(l: Log, n: Node)
    requires
        node_ok(l, n),
        n.w.k.flush is Idle && n.w.k.restore is Idle && n.w.store_cursor < n.w.delivered,
    ensures
        node_ok(l, flush_node(l, n)),
{
    a_flush_lands_the_store_at_the_delivered_position(n.w);
    let n2 = flush_node(l, n);
    // Nothing between the store and the loop falls on the floor: the queue starts at the store's cursor (the kernel invariant).
    assert(n.w.raw_from == n.w.store_cursor);
    if n.origin == l.origin {
        assert(n.rst is None);
        folding_the_log_forward_keeps_a_fold_ok(n.fold, l.ops, n.w.store_cursor, n.w.delivered);
    }
}

proof fn lemma_crash(l: Log, n: Node)
    requires
        node_ok(l, n),
    ensures
        node_ok(l, crash_node(n)),
{
    // The kernel invariant survives a restart (`a_restart_preserves_the_invariant`, re-derived here from `restart`, so a forged
    // `resume_from` breaks THIS proof and not only that theorem).
    assert(inv(restart(n.w)));
    let w2 = restart(n.w);
    assert(w2.store_cursor == n.w.store_cursor);
    assert(w2.k.flush is Idle && w2.k.restore is Idle);
}

proof fn lemma_detect(l: Log, n: Node)
    requires
        node_ok(l, n),
        n.origin != l.origin,
    ensures
        node_ok(l, detect_node(n)),
{
    a_fresh_run_satisfies_the_invariant(0, 0);
}

proof fn lemma_relist(l: Log, n: Node)
    requires
        log_ok(l),
        node_ok(l, n),
        repairable(l, n),
        n.rst is None,
        plan_of(lt_of(l, n)) is Relist,
    ensures
        node_ok(l, relist_node(l, n)),
{
    let n2 = relist_node(l, n);
    a_fresh_run_satisfies_the_invariant(l.ops.len(), l.ops.len());
    let lt = lt_of(l, n);
    assert(lt == Some(true));
    let seen2 = n.seen && !cfg!(feature = "mut_no_eviction_memo");
    assert(listing_is_truth_spec(l.evicts, l.first as u64, seen2));
    assert forall|k: nat| (#[trigger] (n2.fold)(k)) is Some implies real_entry(l.ops, k, (n2.fold)(k)->Some_0) by {
        the_trusted_listing_is_the_truth(l, seen2, k);
        truth_is_real(l.ops, l.ops.len(), k);
    }
    assert forall|k: nat| !touched(l.ops, l.ops.len(), k) implies #[trigger] (n2.fold)(k) == truth(l.ops, l.ops.len(), k) by {
        the_trusted_listing_is_the_truth(l, seen2, k);
        if !n.anchored && !needs_repair_first(has_data(n.fold), lt) {
            // A cursor-less fold that needs no repair holds no data: the re-list over it is the listing.
            assert(!has_data(n.fold));
            assert((n.fold)(k) is None);
        }
    }
}

proof fn lemma_begin(l: Log, f_ptr: Art, n: Node, hd: bool, pick: spec_fn(nat) -> bool)
    requires
        log_ok(l),
        node_ok(l, n),
        art_ok(l, f_ptr),
        repairable(l, n),
        settled(n),
        f_ptr.origin == l.origin,
        restore_ok(f_ptr.cursor, n.w.store_cursor, l.first),
    ensures
        node_ok(l, begin_node(l, n, f_ptr, hd, pick)),
{
    let n2 = begin_node(l, n, f_ptr, hd, pick);
    let c0 = n.w.store_cursor;
    let c1 = f_ptr.cursor;
    // Positions are `u64`: the casts in the guard lose nothing.
    assert(c0 <= umax() && c1 <= umax());
    assert(c1 > c0) by {
        assert(c1 as u64 as nat == c1);
        assert(c0 as u64 as nat == c0);
    }
    a_restore_runs_from_a_settled_node_to_the_chunk_fold(n.w, c1, hd);
    assert(ok_fold(f_ptr.fold, l.ops, c1));
    assert(c1 <= l.ops.len());
    let result = restore_result(l, f_ptr.fold, n.fold, n.origin == l.origin);
    assert(n.w.delivered <= l.ops.len());
    if n.origin == l.origin {
        // A fold of this stream: real and complete at its cursor by its claims (at 0, when it has none).
        assert(ok_fold(n.fold, l.ops, c0));
        a_restore_leaves_the_fold_complete_at_both_cursors(l, n.fold, f_ptr.fold, c0, c1);
        assert forall|k: nat| (#[trigger] (n2.fold)(k)) is Some implies real_entry(l.ops, k, (n2.fold)(k)->Some_0) by {
            assert(result(k) is Some ==> real_entry(l.ops, k, result(k)->Some_0));
        }
        assert forall|k: nat| !touched(l.ops, c0, k) implies #[trigger] (n2.fold)(k) == truth(l.ops, c0, k) by {
            assert(result(k) == truth(l.ops, c0, k));
            assert((n.fold)(k) == truth(l.ops, c0, k));
        }
        assert(rst_ok(l, n2));
    } else {
        // A fold of another incarnation, of any content: the result IS the artifact's fold, so nothing is claimed of the mix.
        assert forall|k: nat| (#[trigger] result(k)) == (f_ptr.fold)(k) by {
            a_restore_over_a_foreign_fold_is_the_artifact(l, n.fold, f_ptr.fold, k);
        }
    }
}

proof fn lemma_chunks(l: Log, n: Node)
    requires
        node_ok(l, n),
        n.rst is Some,
    ensures
        node_ok(l, chunks_node(n)),
{
}

proof fn lemma_end(l: Log, n: Node)
    requires
        node_ok(l, n),
        n.rst is Some,
        forall|k: nat| (#[trigger] (n.fold)(k)) == (rst_of(n).result)(k),
    ensures
        node_ok(l, end_node(n)),
{
    a_restore_commit_lands_the_store_at_the_artifacts_cursor(n.w);
    let n2 = end_node(n);
    let art = rst_of(n).art;
    assert(n2.w.store_cursor == n.w.art);
    assert(art_ok(l, art));
    if art.origin == l.origin {
        // The node now holds a fold of this stream at the artifact's cursor: complete there.
        if rst_of(n).known {
            // Its restore was of a fold of this stream, so its claims hold (the artifact is this stream's): the result is complete
            // at the artifact's cursor.
            assert(n.origin == l.origin);
            assert(ok_fold(n.fold, l.ops, art.cursor));
        } else {
            // A foreign fold's restore is the artifact.
            assert forall|k: nat| (#[trigger] (n.fold)(k)) == (art.fold)(k) by {
                assert((n.fold)(k) == (rst_of(n).result)(k));
            }
            assert(ok_fold(n.fold, l.ops, art.cursor)) by {
                assert forall|k: nat| (#[trigger] (n.fold)(k)) is Some implies real_entry(l.ops, k, (n.fold)(k)->Some_0) by {
                    assert((art.fold)(k) == (n.fold)(k));
                }
                assert forall|k: nat| !touched(l.ops, art.cursor, k) implies #[trigger] (n.fold)(k) == truth(l.ops, art.cursor, k) by {
                    assert((art.fold)(k) == (n.fold)(k));
                }
            }
        }
        assert(n2.fold == n.fold);
        assert(ok_fold(n2.fold, l.ops, art.cursor));
        assert(claims(l, n2));
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Log-level steps.
// ---------------------------------------------------------------------------------------------------------------------

proof fn lemma_append_node(l: Log, op: Op, n: Node)
    requires
        node_ok(l, n),
    ensures
        node_ok(Log { ops: l.ops.push(op), ..l }, n),
{
    let l2 = Log { ops: l.ops.push(op), ..l };
    if n.origin == l.origin {
        ok_fold_append(n.fold, l.ops, op, n.w.store_cursor);
        if n.rst is Some {
            let r = rst_of(n);
            ok_fold_append(r.art.fold, l.ops, op, r.art.cursor);
            ok_fold_append(r.old, l.ops, op, n.w.store_cursor);
            ok_fold_append(r.result, l.ops, op, n.w.store_cursor);
            ok_fold_append(r.result, l.ops, op, r.art.cursor);
        }
        assert(claims(l2, n));
    }
    if n.rst is Some {
        lemma_append_art(l, op, rst_of(n).art);
    }
}

proof fn lemma_append_art(l: Log, op: Op, a: Art)
    requires
        art_ok(l, a),
    ensures
        art_ok(Log { ops: l.ops.push(op), ..l }, a),
{
    if a.origin == l.origin {
        ok_fold_append(a.fold, l.ops, op, a.cursor);
    }
}

proof fn lemma_append_log(l: Log, op: Op)
    requires
        log_ok(l),
        l.ops.len() + 1 < 0x8000_0000_0000_0000,
    ensures
        log_ok(Log { ops: l.ops.push(op), ..l }),
{
    let l2 = Log { ops: l.ops.push(op), ..l };
    if !l.evicts {
        assert forall|k: nat| (#[trigger] live_rev(l2, k)) is Some implies live_rev(l2, k)->Some_0 >= l2.first by {
            truth_is_real(l2.ops, l2.ops.len(), k);
            assert(l2.ops[l.ops.len() as int] == op);
            if op.key == k {
                assert(live_rev(l2, k) == truth(l2.ops, l2.ops.len(), k));
                if op.put {
                    assert(live_rev(l2, k) == Some(l2.ops.len()));
                }
            } else {
                truth_append(l.ops, op, l.ops.len(), k);
                assert(live_rev(l2, k) == live_rev(l, k));
            }
        }
    }
}

proof fn lemma_evict_log(l: Log, g: nat)
    requires
        log_ok(l),
        l.first < g,
        g <= l.ops.len() + 1,
        l.evicts || forall|k: nat| (#[trigger] live_rev(l, k)) is Some ==> live_rev(l, k)->Some_0 >= g,
    ensures
        log_ok(Log { first: g, ..l }),
{
    let l2 = Log { first: g, ..l };
    if !l.evicts {
        assert forall|k: nat| (#[trigger] live_rev(l2, k)) is Some implies live_rev(l2, k)->Some_0 >= l2.first by {
            assert(live_rev(l2, k) == live_rev(l, k));
        }
    }
}

proof fn update_node_keeps(f: F, i: int, n2: Node)
    requires
        valid(f, i),
        forall|j: int| 0 <= j < f.nodes.len() ==> node_ok(f.log, #[trigger] f.nodes[j]),
        node_ok(f.log, n2),
    ensures
        forall|j: int| 0 <= j < set_node(f, i, n2).nodes.len() ==> node_ok(f.log, #[trigger] set_node(f, i, n2).nodes[j]),
{
    let f2 = set_node(f, i, n2);
    assert forall|j: int| 0 <= j < f2.nodes.len() implies node_ok(f.log, #[trigger] f2.nodes[j]) by {
        if j != i {
            assert(f2.nodes[j] == f.nodes[j]);
        }
    }
}

proof fn lemma_recreate_node(l: Log, e: bool, n: Node)
    requires
        node_ok(l, n),
        l.origin + 1 <= umax(),
    ensures
        node_ok(Log { origin: l.origin + 1, ops: Seq::empty(), first: 1, evicts: e }, n),
{
}

// ---------------------------------------------------------------------------------------------------------------------
// THE INDUCTION.
// ---------------------------------------------------------------------------------------------------------------------

/// THEOREM (base case): the initial fleet satisfies the invariant: an empty log and any number of nodes, each a fresh run over an
/// empty store (anchored at cursor 0 or cursor-less), no artifact.
pub proof fn a_fresh_fleet_satisfies_the_invariant(nodes: Seq<Node>, evicts: bool)
    requires
        forall|i: int| 0 <= i < nodes.len() ==> (
            #[trigger] nodes[i].w == init_world(0, 0) && nodes[i].origin == 1 && nodes[i].rst is None),
        forall|i: int, k: nat| 0 <= i < nodes.len() ==> #[trigger] (nodes[i].fold)(k) is None,
    ensures
        finv(F { log: Log { origin: 1, ops: Seq::empty(), first: 1, evicts }, nodes, ptr: None }),
{
    a_fresh_run_satisfies_the_invariant(0, 0);
    let l = Log { origin: 1, ops: Seq::empty(), first: 1, evicts };
    assert forall|i: int| 0 <= i < nodes.len() implies node_ok(l, #[trigger] nodes[i]) by {
        let n = nodes[i];
        assert(ok_fold(n.fold, l.ops, 0)) by {
            assert forall|k: nat| (#[trigger] (n.fold)(k)) is Some implies real_entry(l.ops, k, (n.fold)(k)->Some_0) by {}
            assert forall|k: nat| !touched(l.ops, 0, k) implies #[trigger] (n.fold)(k) == truth(l.ops, 0, k) by {}
        }
    }
}

/// THEOREM (THE INDUCTION): from any fleet satisfying the invariant, every action of every node (and of the log) that its guard
/// admits leaves a fleet that satisfies it: a delivery, a flush, a crash and restart at any point (even mid-restore), the floor
/// guard seeing a foreign cursor, a key-listing repair, a restore (its chunks, its commit), a publish; and a write, a delete,
/// head eviction, a recreation of the stream. Any number of nodes, keys and writes.
pub proof fn every_fleet_action_preserves_the_invariant(f: F, a: Act)
    requires
        finv(f),
        enabled(f, a),
    ensures
        finv(step(f, a)),
{
    match a {
        Act::Put(k) => case_write(f, Op { key: k, put: true }),
        Act::Del(k) => case_write(f, Op { key: k, put: false }),
        Act::Evict(g) => case_evict(f, g),
        Act::Recreate(e) => case_recreate(f, e),
        Act::Publish(i) => case_publish(f, i),
        Act::Advance(i) => case_advance(f, i),
        Act::Flush(i) => case_flush(f, i),
        Act::Crash(i) => case_crash(f, i),
        Act::Detect(i) => case_detect(f, i),
        Act::See(i) => case_see(f, i),
        Act::Relist(i) => case_relist(f, i),
        Act::RestoreBegin(i, hd, pick) => case_begin(f, i, hd, pick),
        Act::FoldChunks(i) => case_chunks(f, i),
        Act::RestoreEnd(i) => case_end(f, i),
    }
}

proof fn case_advance(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Advance(i)),
    ensures
        finv(step(f, Act::Advance(i))),
{
    lemma_advance(f.log, node_at(f, i));
    node_step_ok(f, i, advance_node(node_at(f, i)));
}

proof fn case_flush(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Flush(i)),
    ensures
        finv(step(f, Act::Flush(i))),
{
    lemma_flush(f.log, node_at(f, i));
    node_step_ok(f, i, flush_node(f.log, node_at(f, i)));
}

proof fn case_crash(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Crash(i)),
    ensures
        finv(step(f, Act::Crash(i))),
{
    lemma_crash(f.log, node_at(f, i));
    node_step_ok(f, i, crash_node(node_at(f, i)));
}

proof fn case_detect(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Detect(i)),
    ensures
        finv(step(f, Act::Detect(i))),
{
    lemma_detect(f.log, node_at(f, i));
    node_step_ok(f, i, detect_node(node_at(f, i)));
}

proof fn case_see(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::See(i)),
    ensures
        finv(step(f, Act::See(i))),
{
    let n = node_at(f, i);
    assert(node_ok(f.log, n));
    node_step_ok(f, i, Node { seen: true, ..n });
}

proof fn case_relist(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Relist(i)),
    ensures
        finv(step(f, Act::Relist(i))),
{
    lemma_relist(f.log, node_at(f, i));
    node_step_ok(f, i, relist_node(f.log, node_at(f, i)));
}

proof fn case_begin(f: F, i: int, hd: bool, pick: spec_fn(nat) -> bool)
    requires
        finv(f),
        enabled(f, Act::RestoreBegin(i, hd, pick)),
    ensures
        finv(step(f, Act::RestoreBegin(i, hd, pick))),
{
    lemma_begin(f.log, f.ptr->Some_0, node_at(f, i), hd, pick);
    node_step_ok(f, i, begin_node(f.log, node_at(f, i), f.ptr->Some_0, hd, pick));
}

proof fn case_chunks(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::FoldChunks(i)),
    ensures
        finv(step(f, Act::FoldChunks(i))),
{
    lemma_chunks(f.log, node_at(f, i));
    node_step_ok(f, i, chunks_node(node_at(f, i)));
}

proof fn case_end(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::RestoreEnd(i)),
    ensures
        finv(step(f, Act::RestoreEnd(i))),
{
    lemma_end(f.log, node_at(f, i));
    node_step_ok(f, i, end_node(node_at(f, i)));
}

proof fn node_step_ok(f: F, i: int, n2: Node)
    requires
        finv(f),
        valid(f, i),
        node_ok(f.log, n2),
    ensures
        finv(set_node(f, i, n2)),
{
    update_node_keeps(f, i, n2);
}

proof fn case_write(f: F, op: Op)
    requires
        finv(f),
        f.log.ops.len() + 1 < 0x8000_0000_0000_0000,
    ensures
        finv(F { log: Log { ops: f.log.ops.push(op), ..f.log }, ..f }),
{
    let l = f.log;
    let f2 = F { log: Log { ops: l.ops.push(op), ..l }, ..f };
    lemma_append_log(l, op);
    assert forall|i: int| 0 <= i < f2.nodes.len() implies node_ok(f2.log, #[trigger] f2.nodes[i]) by {
        lemma_append_node(l, op, f.nodes[i]);
    }
    if f.ptr is Some {
        lemma_append_art(l, op, f.ptr->Some_0);
    }
}

proof fn case_evict(f: F, g: nat)
    requires
        finv(f),
        enabled(f, Act::Evict(g)),
    ensures
        finv(step(f, Act::Evict(g))),
{
    let l = f.log;
    let f2 = step(f, Act::Evict(g));
    lemma_evict_log(l, g);
    assert forall|i: int| 0 <= i < f2.nodes.len() implies node_ok(f2.log, #[trigger] f2.nodes[i]) by {
        assert(node_ok(l, f.nodes[i]));
    }
}

proof fn case_recreate(f: F, e: bool)
    requires
        finv(f),
        enabled(f, Act::Recreate(e)),
    ensures
        finv(step(f, Act::Recreate(e))),
{
    let l = f.log;
    let f2 = step(f, Act::Recreate(e));
    assert forall|i: int| 0 <= i < f2.nodes.len() implies node_ok(f2.log, #[trigger] f2.nodes[i]) by {
        lemma_recreate_node(l, e, f.nodes[i]);
    }
}

proof fn case_publish(f: F, i: int)
    requires
        finv(f),
        enabled(f, Act::Publish(i)),
    ensures
        finv(step(f, Act::Publish(i))),
{
    let n = node_at(f, i);
    assert(node_ok(f.log, n));
}

// ---------------------------------------------------------------------------------------------------------------------
// What the invariant says, stated as the two safety properties.
// ---------------------------------------------------------------------------------------------------------------------

/// THEOREM (every live node's fold is the truth at its cursor): a node of the current incarnation holds, on every key not written
/// since its cursor, the truth at its cursor; and holds only writes of this log.
pub proof fn every_live_nodes_fold_is_complete_at_its_cursor(f: F, i: int, k: nat)
    requires
        finv(f),
        valid(f, i),
        node_at(f, i).origin == f.log.origin,
    ensures
        !touched(f.log.ops, node_at(f, i).w.store_cursor, k)
            ==> (node_at(f, i).fold)(k) == truth(f.log.ops, node_at(f, i).w.store_cursor, k),
        (node_at(f, i).fold)(k) is Some ==> real_entry(f.log.ops, k, (node_at(f, i).fold)(k)->Some_0),
{
    assert(node_ok(f.log, f.nodes[i]));
}

/// THEOREM (every published artifact is the truth at its cursor): the same of the pointer's artifact, when it is of the current
/// incarnation (an artifact of another incarnation is never restored: `same_incarnation`).
pub proof fn every_published_artifact_is_complete_at_its_cursor(f: F, k: nat)
    requires
        finv(f),
        f.ptr is Some,
        f.ptr->Some_0.origin == f.log.origin,
    ensures
        !touched(f.log.ops, f.ptr->Some_0.cursor, k)
            ==> (f.ptr->Some_0.fold)(k) == truth(f.log.ops, f.ptr->Some_0.cursor, k),
        (f.ptr->Some_0.fold)(k) is Some ==> real_entry(f.log.ops, k, (f.ptr->Some_0.fold)(k)->Some_0),
        f.ptr->Some_0.cursor <= f.log.ops.len(),
{
}

/// THEOREM (converged means exactly the truth): at the head every key is "not written since", so a node (or artifact) at the head
/// holds exactly the truth at the head, key for key.
pub proof fn a_converged_fold_is_the_truth(f: F, i: int, k: nat)
    requires
        finv(f),
        valid(f, i),
        node_at(f, i).origin == f.log.origin,
        node_at(f, i).w.store_cursor == f.log.ops.len(),
    ensures
        (node_at(f, i).fold)(k) == truth(f.log.ops, f.log.ops.len(), k),
{
    assert(node_ok(f.log, f.nodes[i]));
    assert(!touched(f.log.ops, f.log.ops.len(), k));
}

pub proof fn an_artifact_at_the_head_is_the_truth(f: F, k: nat)
    requires
        finv(f),
        f.ptr is Some,
        f.ptr->Some_0.origin == f.log.origin,
        f.ptr->Some_0.cursor == f.log.ops.len(),
    ensures
        (f.ptr->Some_0.fold)(k) == truth(f.log.ops, f.log.ops.len(), k),
{
    assert(!touched(f.log.ops, f.log.ops.len(), k));
}

// ---------------------------------------------------------------------------------------------------------------------
// FINDINGS, each a concrete state the shipped decisions reach.
// ---------------------------------------------------------------------------------------------------------------------

/// FINDING F3 (the statement): `fold == truth at cursor` is not what the code keeps, by design. A restore keeps the stale local
/// entry of a key the bucket lists live (`restore_key`: "never a phantom delete"; the resume delivers the later write). Here
/// key 0 is put (rev 1), deleted (rev 2) and put again (rev 3). A node at cursor 1 restores from an artifact at cursor 2 (key 0
/// absent): the bucket lists key 0 (rev 3), so the entry `Some(1)` is kept, and the committed cursor 2 names a fold whose key 0 is
/// `Some(1)`, not the truth at 2 (absent). The fold is complete at 2 on every key not written since (key 0 was), and converges
/// when revision 3 is delivered. The invariant is `complete`, and the Stateright model (one key, atomic repair) cannot see this.
pub proof fn finding_f3_the_fold_is_not_the_truth_on_a_key_rewritten_after_the_cursor()
    requires
        !cfg!(feature = "mut_restore_ignores_listing"),
        !cfg!(feature = "mut_restore_ignores_versions"),
    ensures
        ({
            let ops = seq![Op { key: 0, put: true }, Op { key: 0, put: false }, Op { key: 0, put: true }];
            let l = Log { origin: 1, ops, first: 1, evicts: true };
            let old: Fold = |k: nat| if k == 0 { Some(1nat) } else { None };
            let art: Fold = |k: nat| None;
            restore_result(l, art, old, true)(0) == Some(1nat) && truth(ops, 2, 0) is None && truth(ops, 3, 0) == Some(3nat)
                && touched(ops, 2, 0)
        }),
{
    let ops = seq![Op { key: 0, put: true }, Op { key: 0, put: false }, Op { key: 0, put: true }];
    let l = Log { origin: 1, ops, first: 1, evicts: true };
    assert(ops[2].key == 0);
    assert(touched(ops, 2, 0));
    assert(truth(ops, 3, 0) == Some(3nat));
    assert(truth(ops, 2, 0) is None);
}

/// FINDING F2 (a restore over a foreign fold), FIXED: when the stream is deleted and recreated, a node's fold holds the OLD
/// stream's entries and revisions (`Detect`: the cursor is forgotten and the fold repaired "like a cursor-less start that holds
/// data"). With the listing untrusted (an evicting bucket) that repair is a restore, and `restore_key` used to decide by REVISION
/// NUMBER: an old entry numbered above the artifact's was "newer" and kept. Here the new stream has put key 0 seven times
/// (artifact cursor 7, key 0 at rev 7) and the old entry is `Some(500)`. The restore of a fold with no cursor (`known == false`)
/// takes the artifact's entry: the committed fold at cursor 7 holds the write the log made. The same restore, with the old
/// fold's entries trusted, is the pre-fix state (`mut_restore_trusts_foreign` forges it: this proof goes red).
pub proof fn finding_f2_a_restore_over_a_foreign_fold_takes_the_artifacts_entry()
    ensures
        ({
            let ops = Seq::new(7, |i: int| Op { key: 0, put: true });
            let l = Log { origin: 2, ops, first: 1, evicts: true };
            let old: Fold = |k: nat| if k == 0 { Some(500nat) } else { None };
            let art: Fold = |k: nat| if k == 0 { Some(7nat) } else { None };
            ok_fold(art, ops, 7) && !real(old, ops) && restore_result(l, art, old, false)(0) == Some(7nat)
                && truth(ops, 7, 0) == Some(7nat) && !touched(ops, 7, 0)
        }),
{
    let ops = Seq::new(7, |i: int| Op { key: 0, put: true });
    let l = Log { origin: 2, ops, first: 1, evicts: true };
    let old: Fold = |k: nat| if k == 0 { Some(500nat) } else { None };
    let art: Fold = |k: nat| if k == 0 { Some(7nat) } else { None };
    assert(truth(ops, 7, 0) == Some(7nat));
    assert(!real_entry(ops, 0, 500));
    assert(old(0) == Some(500nat));
    assert(!real(old, ops));
    assert forall|k: nat| (#[trigger] art(k)) is Some implies real_entry(ops, k, art(k)->Some_0) by {}
    assert forall|k: nat| !touched(ops, 7, k) implies #[trigger] art(k) == truth(ops, 7, k) by {
        if k == 0 {
        } else {
            assert(truth(ops, 7, k) is None) by {
                truth_is_real(ops, 7, k);
            }
        }
    }
    assert(!touched(ops, 7, 0));
}

/// FINDING F1 (a dead incarnation's pointer wedged publishing), FIXED: the pointer guard used to compare ranks only
/// (`pointer_publish_allowed`, `transport::swap_pointer`), never the incarnation. After the stream is recreated, a pointer whose
/// artifact is the OLD stream's, ranked above the new stream's head, refused every publish of the new stream until its head
/// passed that rank, while `same_incarnation` refused to restore from it. Now a pointer of another incarnation is replaceable (ranks are ordered only within one; no clock is assumed): a publish of
/// the current incarnation is admitted over any pointer of a dead one, whatever its rank (`mut_pointer_ignores_incarnation`
/// forges the old guard: this proof goes red).
pub proof fn finding_f1_a_dead_pointer_ahead_of_the_head_does_not_refuse_a_publish(l: Log, a: Art, c: nat)
    requires
        log_ok(l),
        a.origin < l.origin,
        a.cursor > c,
        a.cursor <= umax(),
        c <= umax(),
    ensures
        publish_allowed_spec(ptr_state(Some(a)), art_pos(l.origin, c)),
{
}

/// FINDING F1b (the price of F1's fix, reported, not assumed away): with no order between incarnations, a slow exporter of a dead
/// incarnation `b` may replace the live pointer (admitted: another incarnation's), and a STALE live exporter may then replace
/// that (admitted likewise) with a payload the prune guard has already deleted (it was strictly below the live pointer `(a, R)`
/// when pruned). Every step is a guard verdict below: the pointer then names a payload that does not exist, until the next live
/// publish replaces it. `transport_proofs` proves the protocol within one incarnation, where this cannot happen. Closing it
/// needs either an order between incarnations (the creation-time axiom, refused) or the swap to check its payload still exists.
pub proof fn finding_f1b_a_dead_exporter_lets_a_pruned_payload_be_published(a: u64, b: u64, r: u64, big: u64)
    requires
        a != b,
        r < big,
    ensures
        prunable_spec(Some(Pos { origin: a, rank: r }), Pos { origin: a, rank: big }, false, true),
        publish_allowed_spec(PointerState::Present { at: Some(Pos { origin: a, rank: big }) }, Pos { origin: b, rank: 1 }),
        publish_allowed_spec(PointerState::Present { at: Some(Pos { origin: b, rank: 1 }) }, Pos { origin: a, rank: r }),
{
}

/// FINDING F4 (a stale memo blocked a start the listing could serve), FIXED: `seen_evicting` rides the cursor, and a node that
/// sees its cursor belong to another incarnation used to keep it (`Detect`). On the recreated, un-evicted bucket the listing IS the
/// truth, but the memo distrusted it, so the plan was a restore; with no artifact of the new stream (a fleet that is all foreign
/// has no node to publish one) every such node was blocked for ever, though nothing was lost. Now the detected node forgets the
/// memo with the cursor, and plans the listing wherever the listing is exact.
pub proof fn finding_f4_a_detected_node_forgets_the_old_streams_eviction_memo(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
        !f.log.evicts,
        f.log.first == 1,
        !cfg!(feature = "mut_no_eviction_memo"),
        enabled(f, Act::Detect(i)),
    ensures
        !step(f, Act::Detect(i)).nodes[i].seen,
        plan_of(lt_of(f.log, step(f, Act::Detect(i)).nodes[i])) is Relist,
        forall|k: nat| (#[trigger] listed(f.log, k)) == truth(f.log.ops, f.log.ops.len(), k),
{
    let l = f.log;
    assert(log_ok(l));
    assert forall|k: nat| (#[trigger] listed(l, k)) == truth(l.ops, l.ops.len(), k) by {
        the_trusted_listing_is_the_truth(l, false, k);
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// LIVENESS. Every node converges, or fail-stops loudly exactly where a write aged out before any fold held it.
//
// The progress argument is a ranking function (`nrank`) that every fair step lowers until the goal, under named fairness
// assumptions on the ENVIRONMENT (never on slipstream's own code):
//   * F-a NATS delivers: a received revision reaches the kernel (`Advance`) while the window holds;
//   * F-b the disk works: a flush lands, the chunks fold, the commit flushes (`Flush`, `FoldChunks`, `RestoreEnd`);
//   * F-c the object store answers (`Publish`, the artifact read in `RestoreBegin`);
//   * F-d quiescence: from some point on the log takes no more writes, evictions or recreations, and the nodes no more crashes.
// (Every other node action is a node's own decision, and a fair scheduler runs each enabled one.)
// The lemmas below are state-based: from any state satisfying the invariant, for each node.
// ---------------------------------------------------------------------------------------------------------------------

/// The node is watching the current stream inside its window.
pub open spec fn live(l: Log, n: Node) -> bool {
    n.anchored && n.origin == l.origin && win(n.w.delivered, l.first) && n.rst is None
}

pub open spec fn converged(l: Log, n: Node) -> bool {
    live(l, n) && n.w.delivered == l.ops.len() && n.w.store_cursor == n.w.delivered
}

pub open spec fn folded(n: Node) -> bool {
    forall|k: nat| (#[trigger] (n.fold)(k)) == (rst_of(n).result)(k)
}

/// THE RANK. Zero is converged. A live node's rank is what it still has to receive and flush. A node that must repair ranks
/// above every live one, and a restore's stages rank in the order they run.
pub open spec fn nrank(l: Log, n: Node) -> int {
    let len = l.ops.len() as int;
    if converged(l, n) {
        0
    } else if live(l, n) {
        2 * (len - n.w.delivered as int) + (n.w.delivered as int - n.w.store_cursor as int)
    } else if n.rst is Some {
        let fl: int = if folded(n) { 0 } else { 1 };
        if rst_of(n).art.origin != l.origin {
            2 * len + 11 + fl
        } else if win(rst_of(n).art.cursor, l.first) {
            2 * len + 4 + fl
        } else {
            2 * len + 7 + fl
        }
    } else if n.anchored && n.origin != l.origin {
        2 * len + 10
    } else {
        2 * len + 6 + (n.w.delivered as int - n.w.store_cursor as int)
    }
}

/// The artifact the pointer names can seed this node: of this stream, ahead, inside the window.
pub open spec fn admissible_art(f: F, n: Node) -> bool {
    f.ptr is Some && f.ptr->Some_0.origin == f.log.origin && restore_ok(f.ptr->Some_0.cursor, n.w.store_cursor, f.log.first)
}

/// The node must restore (expired cursor, or none, and the listing is not the truth) and NO artifact can seed it: the loud
/// fail-stop (`check_restore` returns the operator-facing error). This is the one data loss retention can cause.
pub open spec fn blocked_by_artifact(f: F, i: int) -> bool {
    let n = node_at(f, i);
    repairable(f.log, n) && settled(n) && plan_of(lt_of(f.log, n)) is Restore && !admissible_art(f, n)
}

pub open spec fn acts_on(a: Act, i: int) -> bool {
    match a {
        Act::Advance(j) => j == i,
        Act::Flush(j) => j == i,
        Act::Detect(j) => j == i,
        Act::Relist(j) => j == i,
        Act::RestoreBegin(j, hd, pick) => j == i,
        Act::FoldChunks(j) => j == i,
        Act::RestoreEnd(j) => j == i,
        _ => false,
    }
}

/// A fair step of node `i` that lowers its rank (and leaves the log as it is).
pub open spec fn lowers(f: F, i: int, a: Act) -> bool {
    &&& enabled(f, a)
    &&& acts_on(a, i)
    &&& step(f, a).log == f.log
    &&& valid(step(f, a), i)
    &&& nrank(f.log, step(f, a).nodes[i]) < nrank(f.log, f.nodes[i])
}

/// THEOREM (progress): from any state satisfying the invariant, every node is converged, or some fair step of its own lowers its
/// rank, or it is blocked: no artifact can seed it (and the listing is not the truth). The rank is a natural number, so a node
/// that is never blocked reaches the head in finitely many steps. A foreign fold needs no case of its own: a restore replaces it.
pub proof fn a_node_is_converged_or_a_fair_step_lowers_its_rank_or_it_is_blocked(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
    ensures
        converged(f.log, node_at(f, i)) || (exists|a: Act| #[trigger] lowers(f, i, a)) || blocked_by_artifact(f, i),
{
    let l = f.log;
    let n = node_at(f, i);
    assert(node_ok(l, n));
    if converged(l, n) {
    } else if live(l, n) {
        prog_live(f, i);
    } else if n.rst is Some {
        prog_restore(f, i);
    } else if n.anchored && n.origin != l.origin {
        let a = Act::Detect(i);
        lemma_detect(l, n);
        node_step_ok(f, i, detect_node(n));
        assert(lowers(f, i, a));
    } else {
        prog_repair(f, i);
    }
}

proof fn lowers_from(f: F, i: int, a: Act, n2: Node)
    requires
        valid(f, i),
        enabled(f, a),
        acts_on(a, i),
        step(f, a) == set_node(f, i, n2),
        nrank(f.log, n2) < nrank(f.log, f.nodes[i]),
    ensures
        lowers(f, i, a),
{
    assert(step(f, a).nodes[i] == n2);
}

proof fn prog_live(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
        live(f.log, node_at(f, i)),
        !converged(f.log, node_at(f, i)),
    ensures
        exists|a: Act| #[trigger] lowers(f, i, a),
{
    let l = f.log;
    let n = node_at(f, i);
    assert(node_ok(l, n));
    let len = l.ops.len();
    assert(n.w.delivered <= len);
    if n.w.delivered < len {
        let a = Act::Advance(i);
        lemma_advance(l, n);
        let n2 = advance_node(n);
        assert(win(n2.w.delivered, l.first)) by {
            assert(n.w.delivered <= umax());
            assert(n.w.delivered as u64 as nat == n.w.delivered);
            assert(l.first <= l.ops.len() + 1);
        }
        assert(live(l, n2));
        assert(!converged(l, n2));
        lowers_from(f, i, a, n2);
    } else {
        assert(n.w.store_cursor < n.w.delivered);
        let a = Act::Flush(i);
        lemma_flush(l, n);
        a_flush_lands_the_store_at_the_delivered_position(n.w);
        let n2 = flush_node(l, n);
        assert(live(l, n2));
        assert(converged(l, n2));
        lowers_from(f, i, a, n2);
    }
}

proof fn prog_restore(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
        node_at(f, i).rst is Some,
    ensures
        exists|a: Act| #[trigger] lowers(f, i, a),
{
    let l = f.log;
    let n = node_at(f, i);
    let len = l.ops.len() as int;
    assert(node_ok(l, n));
    assert(!live(l, n));
    assert(!converged(l, n));
    if folded(n) {
        let a = Act::RestoreEnd(i);
        lemma_end(l, n);
        a_restore_commit_lands_the_store_at_the_artifacts_cursor(n.w);
        let n2 = end_node(n);
        let art = rst_of(n).art;
        assert(enabled(f, a));
        assert(n2.w.delivered == art.cursor && n2.w.store_cursor == art.cursor);
        assert(n2.rst is None && n2.anchored && n2.origin == art.origin);
        assert(nrank(l, n) == (if art.origin != l.origin { 2 * len + 11 } else if win(art.cursor, l.first) {
            2 * len + 4
        } else {
            2 * len + 7
        }));
        if art.origin == l.origin {
            assert(ok_fold(n2.fold, l.ops, art.cursor));
            if win(art.cursor, l.first) {
                assert(live(l, n2));
                assert(nrank(l, n2) <= 2 * len);
            } else {
                assert(!live(l, n2));
                assert(nrank(l, n2) == 2 * len + 6);
            }
        } else {
            assert(!live(l, n2));
            assert(nrank(l, n2) == 2 * len + 10);
        }
        lowers_from(f, i, a, n2);
    } else {
        let a = Act::FoldChunks(i);
        lemma_chunks(l, n);
        let n2 = chunks_node(n);
        assert(folded(n2));
        assert(!live(l, n2) && !converged(l, n2));
        lowers_from(f, i, a, n2);
    }
}

proof fn prog_repair(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
        !live(f.log, node_at(f, i)),
        node_at(f, i).rst is None,
        !(node_at(f, i).anchored && node_at(f, i).origin != f.log.origin),
    ensures
        (exists|a: Act| #[trigger] lowers(f, i, a)) || blocked_by_artifact(f, i),
{
    let l = f.log;
    let n = node_at(f, i);
    let len = l.ops.len() as int;
    assert(node_ok(l, n));
    assert(!converged(l, n));
    assert(nrank(l, n) == 2 * len + 6 + (n.w.delivered as int - n.w.store_cursor as int));
    // Unanchored, or anchored on this stream and out of its window: the cursor is expired (or none).
    assert(repairable(l, n)) by {
        if n.anchored {
            assert(!win(n.w.delivered, l.first));
            assert(!win(n.w.store_cursor, l.first)) by {
                assert(n.w.store_cursor <= n.w.delivered);
            }
        }
    }
    if n.w.store_cursor < n.w.delivered {
        let a = Act::Flush(i);
        lemma_flush(l, n);
        a_flush_lands_the_store_at_the_delivered_position(n.w);
        let n2 = flush_node(l, n);
        assert(!live(l, n2));
        assert(!(n2.anchored && n2.origin != l.origin));
        assert(nrank(l, n2) == 2 * len + 6);
        lowers_from(f, i, a, n2);
    } else {
        assert(settled(n));
        if plan_of(lt_of(l, n)) is Relist {
            let a = Act::Relist(i);
            lemma_relist(l, n);
            let n2 = relist_node(l, n);
            assert(win(n2.w.delivered, l.first)) by {
                assert(l.ops.len() < 0x8000_0000_0000_0000);
                assert(l.first <= l.ops.len() + 1);
                assert(l.ops.len() as u64 as nat == l.ops.len());
                assert(l.first as u64 as nat == l.first);
            }
            assert(converged(l, n2));
            assert(enabled(f, a));
            lowers_from(f, i, a, n2);
        } else if admissible_art(f, n) {
            let pick = |k: nat| true;
            let a = Act::RestoreBegin(i, false, pick);
            assert(enabled(f, a));
            lemma_begin(l, f.ptr->Some_0, n, false, pick);
            let n2 = begin_node(l, n, f.ptr->Some_0, false, pick);
            assert(win(f.ptr->Some_0.cursor, l.first)) by {
                assert(f.ptr->Some_0.cursor <= umax());
                assert(f.ptr->Some_0.cursor as u64 as nat == f.ptr->Some_0.cursor);
            }
            assert(folded(n2));
            assert(!live(l, n2) && !converged(l, n2));
            assert(nrank(l, n2) == 2 * len + 4);
            lowers_from(f, i, a, n2);
        }
    }
}

/// THEOREM (a blocked node unblocks): if some node is converged, a publish by it is admitted by the pointer guard, and afterwards
/// the pointer names an artifact that seeds every blocked node behind the head. The artifact an exporter publishes is its own
/// store at its own cursor; a converged node's cursor is the head. No hypothesis on the pointer: a dead incarnation's, whatever its
/// rank, is replaced (FINDING F1, fixed: `finding_f1_a_dead_pointer_ahead_of_the_head_does_not_refuse_a_publish`), and an artifact
/// of this stream is at most the head.
pub proof fn a_converged_node_unblocks_every_blocked_one(f: F, j: int, i: int)
    requires
        finv(f),
        valid(f, j),
        valid(f, i),
        converged(f.log, node_at(f, j)),
        blocked_by_artifact(f, i),
        node_at(f, i).w.store_cursor < f.log.ops.len(),
    ensures
        enabled(f, Act::Publish(j)),
        finv(step(f, Act::Publish(j))),
        admissible_art(step(f, Act::Publish(j)), node_at(f, i)),
{
    let l = f.log;
    let nj = node_at(f, j);
    let ni = node_at(f, i);
    assert(node_ok(l, nj));
    assert(node_ok(l, ni));
    let len = l.ops.len();
    assert(len >= 1);
    assert(nj.w.store_cursor == len);
    assert(len <= umax());
    assert(len as u64 as nat == len);
    // The guard: the pointer is absent, or an artifact of this stream (at most the head), or a dead incarnation's (older).
    if f.ptr is Some {
        let a = f.ptr->Some_0;
        assert(a.origin <= l.origin);
        assert(a.cursor <= umax());
        assert(a.cursor as u64 as nat == a.cursor);
        assert(a.origin as u64 as nat == a.origin);
        assert(nj.origin == l.origin);
        assert(l.origin as u64 as nat == l.origin);
        if a.origin == l.origin {
            assert(a.cursor <= len);
        }
    }
    assert(publish_allowed_spec(ptr_state(f.ptr), art_pos(nj.origin, len)));
    case_publish(f, j);
    let f2 = step(f, Act::Publish(j));
    let a2 = f2.ptr->Some_0;
    assert(a2.origin == l.origin);
    assert(a2.cursor == len);
    assert(restore_ok(len, ni.w.store_cursor, l.first)) by {
        assert(ni.w.store_cursor as u64 as nat == ni.w.store_cursor);
        assert(l.first as u64 as nat == l.first);
        assert(l.first <= len + 1);
    }
}

/// THEOREM (the fail-stop is exactly the aged-out case, and it is loud): a node blocked by the artifact whose cursor is outside
/// the window (every anchored one, and a cursor-less one on a bucket that has evicted) names a write, revision `first - 1`, that
/// was evicted after its cursor, and no artifact of this stream reaches it either: the write was lost before any fold held it.
/// The plan is a restore only where the listing is not the truth.
pub proof fn a_blocked_node_names_the_write_that_aged_out(f: F, i: int)
    requires
        finv(f),
        valid(f, i),
        blocked_by_artifact(f, i),
        !win(node_at(f, i).w.store_cursor, f.log.first),
    ensures
        ({
            let n = node_at(f, i);
            let r = (f.log.first - 1) as nat;
            // The write `r` is evicted, and is past the node's cursor:
            &&& r < f.log.first
            &&& n.w.store_cursor < r
            // no artifact of this stream holds it;
            &&& !(f.ptr is Some && f.ptr->Some_0.origin == f.log.origin && f.ptr->Some_0.cursor >= r)
        }),
{
    let l = f.log;
    let n = node_at(f, i);
    assert(node_ok(l, n));
    assert(l.first <= l.ops.len() + 1);
    assert(n.w.store_cursor <= umax());
    assert(n.w.store_cursor as u64 as nat == n.w.store_cursor);
    assert(l.first as u64 as nat == l.first);
    if f.ptr is Some && f.ptr->Some_0.origin == l.origin {
        let a = f.ptr->Some_0;
        assert(a.cursor as u64 as nat == a.cursor);
        assert(!restore_allowed_spec(a.cursor as u64, n.w.store_cursor as u64, l.first as u64)) by {
            assert(!admissible_art(f, n));
        }
    }
}

} // verus!
