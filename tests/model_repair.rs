//! Exhaustive model-check of the cursor-expiry repair AS STEPS (Stateright).
//!
//! `tests/model.rs` and `tests/model_live_watch.rs` collapse a repair into
//! one transition. That abstraction hid a real bug: a transient store
//! failure on the flush just before a repair left updates re-queued in
//! memory, invisible to the diff — the diff then missed them and a re-queued
//! put resurrected a key deleted during the gap. This model takes the repair
//! apart into the steps `applied.rs` executes and interleaves them with
//! everything else:
//!
//! - the watch task: resume (window check), delivery, the floor-guard trip,
//!   the repair plan, the listing / artifact checks, the ack/reply wait;
//! - the main loop: receive, flush (domain `apply`, cursor advance, store
//!   apply), and each repair step — drain, pre-repair flush (retried until
//!   the store holds everything), diff, chunk flush, cursor commit, reply;
//! - the store: every apply can succeed, fail transiently (re-queue; a
//!   second consecutive failure fail-stops), or crash before / after / torn;
//!   the process can also crash between any two steps (bounded budgets);
//! - the environment, at any moment: writes (put or delete), retention
//!   eviction (head eviction on an evicting bucket; marker purge on one that
//!   keeps current values), and artifact publishes.
//!
//! One key. Retained messages are the latest per key (history 1), so the
//! floor guard here is maximally trigger-happy — every superseding write
//! gaps the frontier — which only exercises the repair more. Writes to other
//! keys don't change this key's fold; the multi-key behavior of the same
//! code is covered by `repair::tests` (exhaustive diff) and
//! `tests/repair_dst.rs` (fault injection over the real loop).
//!
//! Checked, exhaustively within bounds, for both bucket kinds:
//! - **While the store's cursor is resumable, the store plus the tail is the
//!   truth**: what a NATS cursor promises — every RETAINED message at or below
//!   it is applied, so a resume from it reaches every write minus every real
//!   delete. (An expired cursor promises nothing; the repair that must then
//!   run fixes it, which the terminal property checks.)
//! - **The store's cursor never moves backward.**
//! - **The same for the domain state at the applied cursor**, and the domain
//!   never sees a key's revision go backward or a repair delete its current
//!   value — no transient regressions or phantom deletes.
//! - **Every maximal run ends with the store and the domain state equal to
//!   every write minus every real delete.**
//!
//! THE MODEL RUNS THE PRODUCTION KERNEL. The main loop's every repair decision
//! (which steps run and in which order, the settle retry, the ahead check and
//! the listing's window re-check with their refusals, when the cursor commits,
//! when the reply and the ack go out) comes from
//! `slipstream::cursor_kernel::step`, the function `repair::fold_in` calls.
//! The model keeps environment state only (the log, the store, the channel,
//! crashes) and feeds the kernel the facts the shell would observe:
//! `RepairRequested`, `SettleCheck { raw_queued }`, `RestoreAhead`,
//! `RestoreDiffed`, `RestoreListed`, `RestoreFolded`, `RelistFolded`; it runs
//! the returned actions one for one, each its own step, with every
//! interleaving of the environment between them. The per-key decisions are
//! the protocol kernels (`plan_repair`, `restore_allowed`, `restore_ahead`,
//! `listing_is_truth`, `cursorless_start_needs_repair`, `restore_key`,
//! `resume_window_ok`).
//!
//! Fidelity: a flush inside `Settle`, `RunRelist`, `FoldChunks` and
//! `CommitCursor` is one model step with its failure outcomes (the flush
//! protocol itself is `tests/model_applied.rs`'s); `FoldChunks` is one chunk
//! (the key is one). The listing is skipped when the diff has no delete
//! candidate, as in `repair::restore_request`. Retention is read in the same
//! step as the listing (it only moves forward, so a check after the listing
//! covers it).
//!
//! Mutations are cargo features that break the KERNEL (see Cargo.toml), not
//! the model; each is one test in this file, run with `--features mut_<name>`,
//! and must turn a property red (the shipped tests step aside): `mut_no_drain`,
//! `mut_no_settle_retry` (the bug this model found), `mut_cursor_before_diff`,
//! `mut_no_restore_guard`, `mut_no_listing_recheck`, `mut_relist_on_evicting`,
//! `mut_unanchored_relist`, `mut_restore_ignores_versions`,
//! `mut_restore_ignores_listing`, `mut_no_eviction_memo`. `mut_no_ack_barrier`
//! must stay green here (axiom 6 suffices; the barrier is defense in depth,
//! load-bearing in `tests/model_resync_order.rs`). Every run terminates by
//! itself (finite budgets); `run` asserts it stays under `MAX_DEPTH`.
//!
//! Axioms: correct exporters (an artifact at `c` holds the truth at `c` —
//! discharged by `tests/model_fleet.rs`); a full re-list is not overrun by
//! retention before it delivers what it found (axiom 6 of `tests/model.rs`).

use slipstream::cursor_kernel::{Action, Event, RepairKind, State as K, step};
use slipstream::protocol::{
    KeyRestore, KeyState, RepairMode, RepairPlan, cursorless_start_needs_repair, listing_is_truth,
    plan_repair, restore_ahead, restore_allowed, restore_key, resume_window_ok,
};
use stateright::{Checker, Model, Property};

/// Any kernel mutation feature is on: the shipped-kernel tests step aside and
/// the one test for that feature asserts its counterexample.
const MUTATED: bool = cfg!(any(
    feature = "mut_no_drain",
    feature = "mut_no_settle_retry",
    feature = "mut_cursor_before_diff",
    feature = "mut_no_ack_barrier",
    feature = "mut_no_restore_guard",
    feature = "mut_no_listing_recheck",
    feature = "mut_relist_on_evicting",
    feature = "mut_unanchored_relist",
    feature = "mut_restore_ignores_versions",
    feature = "mut_restore_ignores_listing",
    feature = "mut_no_eviction_memo",
));

/// Longest run the checker may reach. Every run terminates on its own (the
/// budgets and revision cap are finite); a run that hit this would be cut
/// short, and `run` asserts none does.
const MAX_DEPTH: usize = 120;

/// Capacity of the revision array (bounds are per model: `max_rev`).
const CAP: usize = 8;
/// Consecutive transient failures before the watch fail-stops (16 in code;
/// 2 reaches the same branch).
const FAILSTOP_STREAK: u8 = 2;

/// An update on the key: its value (`Some(put revision)`, `None` deleted) and
/// the stream position it carries (`None`: a repair's synthetic update).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Upd {
    pos: Option<u8>,
    value: Option<u8>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Req {
    /// Key-listing diff. The main loop lists the live keys itself, after the
    /// pre-repair flush and before the diff (`repair::relist`).
    Relist,
    /// Artifact restore to this cursor. The main loop lists the live keys
    /// itself, after the diff and before folding it (`repair::restore`).
    Restore { target: u8 },
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Reply {
    Ack,
    Restored(u8),
    Refused,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum WPhase {
    /// Process start: resume from the store's cursor (or a fresh start).
    Start,
    /// Delivering from frontier `f`. `guarded`: the `_from` resume watch
    /// (floor guard on); otherwise the full re-list watch.
    Watch {
        f: u8,
        guarded: bool,
    },
    /// The cursor (or frontier) `local` expired.
    Expired {
        local: u8,
    },
    AwaitAck,
    AwaitReply,
    /// The watch task failed (a refused restore): the process ends.
    Failed,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    // --- the log
    head: u8,
    /// `ops[r]`: the write at revision r was a put (true) or delete.
    ops: [bool; CAP],
    /// The key's latest (only retained) message was evicted.
    evicted: bool,
    /// Newest published artifact's cursor (0: none).
    pointer: u8,
    // --- durable
    store_val: Option<u8>,
    store_cur: u8,
    // --- in memory
    chan: Vec<Upd>,
    batch: Vec<Upd>,
    raw: Vec<Upd>,
    batch_high: Option<u8>,
    applied: u8,
    domain: Option<u8>,
    streak: u8,
    req: Option<Req>,
    reply: Option<Reply>,
    w: WPhase,
    // --- the main loop: the kernel's state and the actions it still owes. The
    // head action is the one in flight; the model decides none of them.
    k: K,
    todo: Vec<Action>,
    /// The repair `todo` runs (its target).
    run: Option<Req>,
    /// A flush has completed under the head action.
    fl: bool,
    /// The restore's key operation (`None`: keep the local entry).
    op: Option<Upd>,
    // --- budgets and latches
    crashes: u8,
    transients: u8,
    regressed: bool,
    /// The domain saw the key's revision go backward.
    domain_regressed: bool,
    /// The domain saw a repair delete the key while it was live.
    phantom_delete: bool,
    /// The highest revision the domain has seen for the key.
    domain_max: u8,
    restored: bool,
    relisted: bool,
    drained_backlog: bool,
    requeued_at_repair: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum FlushOutcome {
    Ok,
    Transient,
    CrashBefore,
    CrashAfter,
    CrashTorn,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    Put,
    Delete,
    Evict,
    Publish(u8),
    WStart,
    WDeliver,
    WTrip,
    WPlan,
    WGotReply,
    Restart,
    Crash,
    MRecv,
    MStartRepair,
    Flush(FlushOutcome),
    MNext,
}

#[derive(Clone)]
struct RepairModel {
    evicting: bool,
    /// The repair armed (`ExpiryRepair`'s mode).
    mode: RepairMode,
    fresh: bool,
    /// Let retention evict during a key-listing repair (see `actions`).
    drop_axiom_6: bool,
    /// The bucket evicts (`evicting`), but its config no longer says so: the
    /// operator turned `max_age` / `discard: old` off after the fold saw it
    /// evict. The memo axiom: a fold reads retention while eviction is in
    /// force, so it has seen any eviction that happened.
    config_forgot: bool,
    /// The key's revisions run 1..=max_rev (revision 1 is its creation).
    max_rev: u8,
    /// Crash budget (process deaths, including store-apply crashes).
    max_crashes: u8,
    /// Transient store-failure budget.
    max_transient: u8,
}

impl RepairModel {
    /// THE TRUTH at `c`: every revision is a write to this key, so the
    /// latest write at or before `c` is revision `min(c, head)`.
    fn truth_at(s: &St, c: u8) -> Option<u8> {
        let r = c.min(s.head);
        (r >= 1 && s.ops[r as usize]).then_some(r)
    }

    fn truth(s: &St) -> Option<u8> {
        Self::truth_at(s, s.head)
    }

    fn first_revision(s: &St) -> u64 {
        if s.evicted {
            s.head as u64 + 1
        } else {
            s.head as u64
        }
    }

    /// The listing as the planner sees it: the shared kernel on the bucket's
    /// retention and what the fold saw.
    fn listing_truth(&self, s: &St) -> Option<bool> {
        let reported = self.evicting && !self.config_forgot;
        let seen = self.evicting && s.evicted;
        Some(listing_is_truth(reported, Self::first_revision(s), seen))
    }

    fn listed(s: &St) -> Option<u8> {
        if s.evicted { None } else { Self::truth(s) }
    }

    /// What a NATS cursor promises: every RETAINED message at or below it is
    /// applied. So a value at cursor `c` is correct when a resume from `c`
    /// reaches the truth — the key's latest message, if after `c` and still
    /// retained, is delivered; otherwise the value itself must be the truth.
    /// Vacuous once `c` is outside retention: that cursor is expired, and the
    /// repair that must then run is what makes it right (the terminal
    /// property checks that it does).
    fn resumes_to_truth(s: &St, c: u8, value: Option<u8>) -> bool {
        if c == 0 || !resume_window_ok(c as u64, Self::first_revision(s)) {
            return true;
        }
        let tail_delivers = s.head > c && !s.evicted;
        tail_delivers || value == Self::truth(s)
    }

    fn flushable(s: &St) -> bool {
        !s.batch.is_empty() || !s.raw.is_empty() || s.batch_high.is_some()
    }

    /// Process death: everything in memory is gone; the restart rebuilds the
    /// domain from the store and resumes from its cursor.
    fn restart(s: &mut St) {
        s.chan.clear();
        s.batch.clear();
        s.raw.clear();
        s.batch_high = None;
        s.applied = s.store_cur;
        s.domain = s.store_val;
        s.domain_max = s.store_val.unwrap_or(0);
        s.streak = 0;
        s.req = None;
        s.reply = None;
        s.w = WPhase::Start;
        // The fold is rebuilt: a fresh kernel state, nothing owed.
        s.k = K::default();
        s.todo.clear();
        s.run = None;
        s.fl = false;
        s.op = None;
    }

    fn ingest(s: &mut St, u: Upd) {
        if let Some(p) = u.pos {
            s.batch_high = Some(p);
        }
        s.batch.push(u);
        s.raw.push(u);
    }

    /// `flush!()`: domain apply, cursor advance, store apply with `outcome`.
    /// Returns false if the process died.
    fn flush(s: &mut St, outcome: FlushOutcome) -> bool {
        for u in std::mem::take(&mut s.batch) {
            match u.value {
                Some(rev) => {
                    if rev < s.domain_max {
                        s.domain_regressed = true;
                    }
                    s.domain_max = s.domain_max.max(rev);
                }
                // A PHANTOM delete: a repair's revisionless delete of the value
                // the domain holds while that value is still the current one.
                // (Deleting a value a newer write has replaced is just the
                // domain being behind; the newer write arrives later.)
                None if u.pos.is_none() && s.domain.is_some() && s.domain == Self::truth(s) => {
                    s.phantom_delete = true
                }
                None => {}
            }
            s.domain = u.value;
        }
        let advanced = s.batch_high.is_some();
        if let Some(h) = s.batch_high.take() {
            s.applied = h;
        }
        if s.raw.is_empty() && !advanced {
            return true;
        }
        let commit = |s: &mut St, cursor: u8| {
            if let Some(last) = s.raw.last() {
                s.store_val = last.value;
            }
            s.raw.clear();
            if cursor < s.store_cur {
                s.regressed = true;
            }
            s.store_cur = cursor;
        };
        match outcome {
            FlushOutcome::Ok => {
                commit(s, s.applied);
                s.streak = 0;
                true
            }
            FlushOutcome::Transient => {
                s.transients += 1;
                s.streak += 1;
                if s.todo.first() == Some(&Action::Settle) {
                    s.requeued_at_repair = true;
                }
                if s.streak >= FAILSTOP_STREAK {
                    Self::restart(s);
                    return false;
                }
                true
            }
            FlushOutcome::CrashBefore => {
                s.crashes += 1;
                Self::restart(s);
                false
            }
            FlushOutcome::CrashAfter => {
                s.crashes += 1;
                commit(s, s.applied);
                Self::restart(s);
                false
            }
            FlushOutcome::CrashTorn => {
                s.crashes += 1;
                let old = s.store_cur;
                commit(s, old);
                Self::restart(s);
                false
            }
        }
    }

    /// `restore_diff` on the key, through the shared kernel: the artifact at
    /// `target` holds the truth there. `listed_live`: the bucket lists the key
    /// live (taken after the diff).
    fn restore_op(s: &St, target: u8, listed_live: bool) -> KeyRestore {
        let want = Self::truth_at(s, target);
        let state = |v: Option<u8>| match v {
            Some(r) => KeyState::At(Some(r as u64)),
            None => KeyState::Absent,
        };
        let identical = s.store_val.is_some() && s.store_val == want;
        // One incarnation here: the fold is this stream's (`tests/model_incarnation.rs`, `tests/recreated_fleet.rs`).
        restore_key(
            state(s.store_val),
            state(want),
            identical,
            listed_live,
            true,
        )
    }

    fn upd(target: u8, s: &St, r: KeyRestore) -> Option<Upd> {
        match r {
            KeyRestore::Keep => None,
            KeyRestore::TakeArtifact => Some(Upd {
                pos: None,
                value: Self::truth_at(s, target),
            }),
            KeyRestore::Delete => Some(Upd {
                pos: None,
                value: None,
            }),
        }
    }

    /// Resume from cursor `c` (> 0): the window check.
    fn resume(s: &St, c: u8) -> WPhase {
        if resume_window_ok(c as u64, Self::first_revision(s)) {
            WPhase::Watch {
                f: c,
                guarded: true,
            }
        } else {
            WPhase::Expired { local: c }
        }
    }

    fn restore_ok(s: &St, target: u8, local: u8) -> bool {
        target > 0 && restore_allowed(target as u64, local as u64, Self::first_revision(s))
    }

    /// Feed the kernel one event; its actions replace the head of `todo`
    /// (`pop`: the head just finished), in order, and the new head is entered.
    fn feed(&self, s: &mut St, pop: bool, e: Event) {
        let (k, acts) = step(s.k, e);
        s.k = k;
        if pop {
            s.todo.remove(0);
        }
        for (i, a) in acts.as_slice().iter().enumerate() {
            s.todo.insert(i, *a);
        }
        s.fl = false;
        self.enter(s);
    }

    /// The head action begins: what the shell does the moment it runs it,
    /// before its flush. The listing diff's synthetic delete, the restore's
    /// key operation and the cursor take their places in the queue.
    fn enter(&self, s: &mut St) {
        match s.todo.first() {
            None => s.run = None,
            Some(Action::RunRelist) => {
                // List, then diff the STORE against the listing.
                if s.store_val.is_some() && Self::listed(s).is_none() {
                    let u = Upd {
                        pos: None,
                        value: None,
                    };
                    s.batch.push(u);
                    s.raw.push(u);
                }
                s.relisted = true;
            }
            Some(Action::FoldChunks) => {
                if let Some(u) = s.op.take() {
                    s.batch.push(u);
                    s.raw.push(u);
                }
            }
            Some(Action::CommitCursor) => {
                if let Some(Req::Restore { target }) = s.run {
                    s.batch_high = Some(target);
                }
            }
            Some(_) => {}
        }
    }

    /// The head action ran to completion: drop it, enter the next.
    fn finish(&self, s: &mut St) {
        s.todo.remove(0);
        s.fl = false;
        self.enter(s);
    }

    /// A repair arrived at the main loop.
    fn feed_request(&self, s: &mut St, kind: RepairKind) {
        let (k, acts) = step(s.k, Event::RepairRequested(kind));
        s.k = k;
        s.todo = acts.as_slice().to_vec();
        s.fl = false;
        self.enter(s);
    }

    /// The head action needs a flush to settle: the kernel's settle check
    /// says the store is still behind the queue.
    fn settle_flushes(s: &St) -> bool {
        step(
            s.k,
            Event::SettleCheck {
                raw_queued: s.raw.len(),
            },
        )
        .1
        .contains(Action::Flush)
    }
}

impl Model for RepairModel {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        let mut ops = [false; CAP];
        ops[1] = true;
        let (store_val, store_cur) = if self.fresh { (None, 0) } else { (Some(1), 1) };
        vec![St {
            head: 1,
            ops,
            evicted: false,
            pointer: 0,
            store_val,
            store_cur,
            chan: vec![],
            batch: vec![],
            raw: vec![],
            batch_high: None,
            applied: store_cur,
            domain: store_val,
            streak: 0,
            req: None,
            reply: None,
            w: WPhase::Start,
            k: K::default(),
            todo: vec![],
            run: None,
            fl: false,
            op: None,
            crashes: 0,
            transients: 0,
            regressed: false,
            domain_regressed: false,
            phantom_delete: false,
            domain_max: store_val.unwrap_or(0),
            restored: false,
            relisted: false,
            drained_backlog: false,
            requeued_at_repair: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        // Environment.
        if s.head < self.max_rev {
            acts.push(Act::Put);
            acts.push(Act::Delete);
        }
        // Axiom 6: retention doesn't overrun a key-listing repair or a full
        // re-list while it is in flight — from the moment the listing is
        // taken until the re-list has delivered what the bucket holds. (On a
        // bucket that keeps current values, the only eviction is an admin
        // purge of delete markers; NATS's `purge_deletes` keeps markers
        // younger than 30 minutes by default. Without this axiom the checker
        // finds the delete-and-purge-inside-the-window trace — kept as
        // `relist_window_needs_axiom_6`.)
        let relist_in_flight = !self.drop_axiom_6
            && (matches!(s.w, WPhase::Watch { f, guarded: false } if f < s.head)
                || matches!(s.w, WPhase::AwaitAck)
                || matches!(s.req, Some(Req::Relist))
                || matches!(s.run, Some(Req::Relist)));
        let evictable = if self.evicting {
            true
        } else {
            !s.ops[s.head as usize]
        };
        if !s.evicted && evictable && !relist_in_flight {
            acts.push(Act::Evict);
        }
        if self.evicting {
            for c in (s.pointer + 1)..=s.head {
                acts.push(Act::Publish(c));
            }
        }

        // Watch task.
        match s.w {
            WPhase::Start => acts.push(Act::WStart),
            WPhase::Watch { f, guarded } => {
                if !s.evicted && s.head > f {
                    // Guarded: the in-band check precedes delivery.
                    if !guarded || resume_window_ok(f as u64, Self::first_revision(s)) {
                        acts.push(Act::WDeliver);
                    }
                }
                if guarded && !resume_window_ok(f as u64, Self::first_revision(s)) {
                    acts.push(Act::WTrip);
                }
            }
            WPhase::Expired { .. } => acts.push(Act::WPlan),
            WPhase::AwaitAck | WPhase::AwaitReply => {
                if s.reply.is_some() {
                    acts.push(Act::WGotReply);
                }
            }
            WPhase::Failed => acts.push(Act::Restart),
        }

        // Main loop.
        let flush_outcomes = |acts: &mut Vec<Act>| {
            acts.push(Act::Flush(FlushOutcome::Ok));
            if s.transients < self.max_transient {
                acts.push(Act::Flush(FlushOutcome::Transient));
            }
            if s.crashes < self.max_crashes {
                acts.push(Act::Flush(FlushOutcome::CrashBefore));
                acts.push(Act::Flush(FlushOutcome::CrashAfter));
                acts.push(Act::Flush(FlushOutcome::CrashTorn));
            }
        };
        match s.todo.first() {
            None => {
                // Superset of the biased select: receive is allowed even with
                // a repair pending (the drain makes either order safe).
                if !s.chan.is_empty() {
                    acts.push(Act::MRecv);
                }
                if s.req.is_some() {
                    acts.push(Act::MStartRepair);
                }
                if Self::flushable(s) {
                    flush_outcomes(acts);
                }
            }
            // Settle: flush, then again while the kernel says the store is
            // still behind.
            Some(Action::Settle) => {
                if Self::flushable(s) && (!s.fl || Self::settle_flushes(s)) {
                    flush_outcomes(acts);
                }
                if (s.fl || !Self::flushable(s)) && !(s.fl && Self::settle_flushes(s)) {
                    acts.push(Act::MNext);
                }
            }
            // One flush attempt, then on (a transient failure leaves the
            // remainder re-queued for the next flush).
            Some(Action::RunRelist | Action::FoldChunks | Action::CommitCursor) => {
                if !s.fl && Self::flushable(s) {
                    flush_outcomes(acts);
                }
                if s.fl || !Self::flushable(s) {
                    acts.push(Act::MNext);
                }
            }
            Some(_) => acts.push(Act::MNext),
        }
        if s.crashes < self.max_crashes {
            acts.push(Act::Crash);
        }
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::Put | Act::Delete => {
                s.head += 1;
                s.ops[s.head as usize] = a == Act::Put;
                s.evicted = false;
            }
            Act::Evict => s.evicted = true,
            Act::Publish(c) => s.pointer = c,
            Act::WStart => {
                s.w = if s.store_cur == 0 {
                    // Data but no cursor (a torn first checkpoint), or an
                    // empty fold on a bucket that already evicted current
                    // values: repair like an expiry at 0. `UnanchoredRelist`
                    // forges the fold empty.
                    let has_data = s.store_val.is_some();
                    if cursorless_start_needs_repair(self.mode, has_data, self.listing_truth(&s)) {
                        WPhase::Expired { local: 0 }
                    } else {
                        WPhase::Watch {
                            f: 0,
                            guarded: false,
                        }
                    }
                } else {
                    Self::resume(&s, s.store_cur)
                };
            }
            Act::WDeliver => {
                let WPhase::Watch { guarded, .. } = s.w else {
                    return None;
                };
                s.chan.push(Upd {
                    pos: Some(s.head),
                    value: Self::truth(&s),
                });
                s.w = WPhase::Watch { f: s.head, guarded };
            }
            Act::WTrip => {
                let WPhase::Watch { f, .. } = s.w else {
                    return None;
                };
                s.w = WPhase::Expired { local: f };
            }
            Act::WPlan => {
                let WPhase::Expired { local } = s.w else {
                    return None;
                };
                match plan_repair(self.mode, self.listing_truth(&s)) {
                    RepairPlan::Restore => {
                        // Peek + fetch collapsed onto the CURRENT pointer
                        // (the code re-checks what it fetched against now).
                        let target = s.pointer;
                        if Self::restore_ok(&s, target, local) {
                            s.req = Some(Req::Restore { target });
                            s.w = WPhase::AwaitReply;
                        } else {
                            s.w = WPhase::Failed;
                        }
                    }
                    RepairPlan::Relist => {
                        s.req = Some(Req::Relist);
                        s.w = WPhase::AwaitAck;
                    }
                    RepairPlan::RefuseRelist => s.w = WPhase::Failed,
                    RepairPlan::ReListOnly => {
                        s.w = WPhase::Watch {
                            f: 0,
                            guarded: false,
                        }
                    }
                }
            }
            Act::WGotReply => {
                s.w = match s.reply.take()? {
                    Reply::Ack => WPhase::Watch {
                        f: 0,
                        guarded: false,
                    },
                    Reply::Restored(c) => Self::resume(&s, c),
                    Reply::Refused => WPhase::Failed,
                };
            }
            Act::Restart => Self::restart(&mut s),
            Act::Crash => {
                s.crashes += 1;
                Self::restart(&mut s);
            }
            Act::MRecv => {
                let u = s.chan.remove(0);
                Self::ingest(&mut s, u);
            }
            Act::MStartRepair => {
                let req = s.req.take()?;
                s.run = Some(req);
                let kind = match req {
                    Req::Relist => RepairKind::Relist,
                    Req::Restore { .. } => RepairKind::Restore,
                };
                s.todo.clear();
                self.feed_request(&mut s, kind);
            }
            Act::Flush(outcome) => {
                if Self::flush(&mut s, outcome) {
                    s.fl = true;
                }
            }
            Act::MNext => {
                let head = *s.todo.first()?;
                let target = match s.run {
                    Some(Req::Restore { target }) => target,
                    _ => 0,
                };
                match head {
                    Action::DrainBacklog => {
                        if !s.chan.is_empty() {
                            s.drained_backlog = true;
                        }
                        for u in std::mem::take(&mut s.chan) {
                            Self::ingest(&mut s, u);
                        }
                        self.finish(&mut s);
                    }
                    Action::Settle | Action::CommitCursor => self.finish(&mut s),
                    Action::RunRelist => self.feed(&mut s, true, Event::RelistFolded),
                    Action::FoldChunks => {
                        s.restored = true;
                        self.feed(&mut s, true, Event::RestoreFolded);
                    }
                    Action::CheckAhead => {
                        let ahead = restore_ahead(target as u64, s.applied as u64);
                        self.feed(&mut s, true, Event::RestoreAhead { ahead });
                    }
                    Action::RunDiff => {
                        // The diff, before the listing: a key the artifact
                        // lacks is a delete candidate.
                        let r = Self::restore_op(&s, target, false);
                        s.op = Self::upd(target, &s, r);
                        let has_deletes = r == KeyRestore::Delete;
                        self.feed(&mut s, true, Event::RestoreDiffed { has_deletes });
                    }
                    Action::ListLive => {
                        // The listing, then the retention re-read: the live
                        // key is kept, and the window must still hold every
                        // write past the artifact's cursor.
                        let r = Self::restore_op(&s, target, Self::listed(&s).is_some());
                        s.op = Self::upd(target, &s, r);
                        let window_ok = resume_window_ok(target as u64, Self::first_revision(&s));
                        self.feed(&mut s, true, Event::RestoreListed { window_ok });
                    }
                    Action::Refuse(_) => {
                        s.reply = Some(Reply::Refused);
                        s.todo.clear();
                        s.run = None;
                    }
                    Action::ReplyRestored => {
                        s.reply = Some(Reply::Restored(target));
                        self.finish(&mut s);
                    }
                    Action::AckResync => {
                        s.reply = Some(Reply::Ack);
                        self.finish(&mut s);
                    }
                    other => unreachable!("model: {other:?}"),
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = vec![
            Property::<Self>::always(
                "while the store's cursor is resumable, the store plus the tail is the truth",
                |_, s| RepairModel::resumes_to_truth(s, s.store_cur, s.store_val),
            ),
            Property::<Self>::always("the store's cursor never moves backward", |_, s| {
                !s.regressed
            }),
            Property::<Self>::always(
                "the domain never sees the key's revision go backward",
                |_, s| !s.domain_regressed,
            ),
            Property::<Self>::always("a repair never deletes the key's current value", |_, s| {
                !s.phantom_delete
            }),
            Property::<Self>::always(
                "while the applied cursor is resumable, the domain plus the tail is the truth",
                |_, s| RepairModel::resumes_to_truth(s, s.applied, s.domain),
            ),
            Property::<Self>::always(
                "every maximal run ends with the store and domain equal to every write minus every real delete",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty()
                        || (s.store_val == RepairModel::truth(s) && s.domain == s.store_val)
                },
            ),
        ];
        if !MUTATED {
            // The theorems are earned: the repairs run, the hazards they
            // guard against occur.
            if self.evicting {
                props.push(Property::<Self>::sometimes(
                    "a restore completes past an evicted current value",
                    |_, s| s.restored && s.evicted && RepairModel::truth(s).is_some(),
                ));
            } else {
                props.push(Property::<Self>::sometimes(
                    "a key-listing repair runs",
                    |_, s| s.relisted,
                ));
            }
            props.push(Property::<Self>::sometimes(
                "a repair drains a delivered backlog",
                |_, s| s.drained_backlog,
            ));
            props.push(Property::<Self>::sometimes(
                "a pre-repair flush fails transiently and is retried",
                |_, s| s.requeued_at_repair,
            ));
        }
        props
    }
}

fn run(model: RepairModel, label: &str) -> impl Checker<RepairModel> {
    let started = std::time::Instant::now();
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .spawn_bfs()
        .join();
    println!(
        "{label}: {} states, {} unique, depth {}, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        checker.max_depth(),
        started.elapsed(),
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "{label}: the run reached the depth bound; raise MAX_DEPTH"
    );
    checker
}

/// The shipped configuration: `ExpiryRepair::Auto` on an evicting bucket,
/// `ExpiryRepair::Relist` on one that keeps current values.
fn shipped(evicting: bool, fresh: bool) -> RepairModel {
    RepairModel {
        evicting,
        mode: if evicting {
            RepairMode::Auto
        } else {
            RepairMode::Relist
        },
        fresh,
        drop_axiom_6: false,
        config_forgot: false,
        max_rev: 4,
        max_crashes: 1,
        max_transient: 2,
    }
}

const NAMES: [&str; 6] = [
    "while the store's cursor is resumable, the store plus the tail is the truth",
    "the store's cursor never moves backward",
    "the domain never sees the key's revision go backward",
    "a repair never deletes the key's current value",
    "while the applied cursor is resumable, the domain plus the tail is the truth",
    "every maximal run ends with the store and domain equal to every write minus every real delete",
];

/// A kernel mutation must produce a counterexample to at least one safety or
/// terminal property, on each bucket kind it applies to.
fn assert_load_bearing(feature: &str, cases: &[(bool, bool)]) {
    for &(evicting, fresh) in cases {
        let label = format!(
            "{feature}: {}{}",
            if evicting {
                "evicting"
            } else {
                "keeps-current"
            },
            if fresh { ", fresh" } else { "" }
        );
        let checker = run(shipped(evicting, fresh), &label);
        let found: Vec<&str> = NAMES
            .iter()
            .copied()
            .filter(|n| checker.discovery(n).is_some())
            .collect();
        assert!(
            !found.is_empty(),
            "{label}: the checker found no counterexample"
        );
        println!("  caught by: {found:?}");
    }
}

#[test]
fn keeps_current_repair_steps_are_correct() {
    if MUTATED {
        return;
    }
    run(shipped(false, false), "repair steps: keeps-current").assert_properties();
    run(
        RepairModel {
            mode: RepairMode::Auto,
            ..shipped(false, false)
        },
        "repair steps: keeps-current, Auto",
    )
    .assert_properties();
}

#[test]
fn evicting_repair_steps_are_correct() {
    if MUTATED {
        return;
    }
    run(shipped(true, false), "repair steps: evicting").assert_properties();
}

/// Eviction turned off after it dropped a key: the planner sees a config
/// that keeps current values, but the fold remembers what it saw, so `Auto`
/// still restores rather than diffing the listing.
#[test]
fn eviction_turned_off_repair_steps_are_correct() {
    if MUTATED {
        return;
    }
    run(
        RepairModel {
            config_forgot: true,
            ..shipped(true, false)
        },
        "repair steps: eviction turned off",
    )
    .assert_properties();
}

#[test]
fn fresh_start_repair_steps_are_correct() {
    if MUTATED {
        return;
    }
    run(shipped(false, true), "repair steps: keeps-current, fresh").assert_properties();
    run(shipped(true, true), "repair steps: evicting, fresh").assert_properties();
}

/// The axiom the key-listing repair rests on, made explicit: drop it and the
/// checker finds a delete whose marker is purged between the listing and the
/// re-list. The artifact restore resumes on a floor-guarded watch and does
/// not need it — the evicting configuration holds without the axiom.
#[test]
fn relist_window_needs_axiom_6() {
    if MUTATED {
        return;
    }
    let checker = run(
        RepairModel {
            drop_axiom_6: true,
            ..shipped(false, false)
        },
        "repair steps: keeps-current, axiom 6 dropped",
    );
    assert!(
        checker.discovery(NAMES[5]).is_some(),
        "without axiom 6 the relist window must be reachable"
    );
    run(
        RepairModel {
            drop_axiom_6: true,
            ..shipped(true, false)
        },
        "repair steps: evicting, axiom 6 dropped",
    )
    .assert_properties();
}

/// Deeper bounds: one more revision, two crashes, three transient failures.
/// `cargo test --release --test model_repair -- --ignored`
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_repair_steps() {
    if MUTATED {
        return;
    }
    for (evicting, fresh) in [(false, false), (true, false), (false, true), (true, true)] {
        run(
            RepairModel {
                max_rev: 5,
                max_crashes: 2,
                max_transient: 3,
                ..shipped(evicting, fresh)
            },
            &format!("deep repair steps: evicting={evicting} fresh={fresh}"),
        )
        .assert_properties();
    }
}

// Kernel mutations: one feature per run (`--features mut_<name>`); the
// shipped-kernel tests above step aside.

/// The repair does not drain the delivered backlog first.
#[cfg(feature = "mut_no_drain")]
#[test]
fn no_drain_is_caught() {
    assert_load_bearing("mut_no_drain", &[(true, false), (false, false)]);
}

/// The pre-repair flush runs once, not until the store holds everything (the
/// bug this model found).
#[cfg(feature = "mut_no_settle_retry")]
#[test]
fn no_settle_retry_is_caught() {
    assert_load_bearing("mut_no_settle_retry", &[(false, false), (true, false)]);
}

/// The restore commits the artifact's cursor before the diff.
#[cfg(feature = "mut_cursor_before_diff")]
#[test]
fn cursor_before_diff_is_caught() {
    assert_load_bearing("mut_cursor_before_diff", &[(true, false)]);
}

/// The restore guards (watch task's `restore_allowed`, the main loop's ahead
/// and window checks) are gone.
#[cfg(feature = "mut_no_restore_guard")]
#[test]
fn no_restore_guard_is_caught() {
    assert_load_bearing("mut_no_restore_guard", &[(true, false)]);
}

/// The restore trusts a listing taken after retention evicted writes.
#[cfg(feature = "mut_no_listing_recheck")]
#[test]
fn no_listing_recheck_is_caught() {
    assert_load_bearing("mut_no_listing_recheck", &[(true, false)]);
}

/// The key-listing diff on a bucket that evicts current values.
#[cfg(feature = "mut_relist_on_evicting")]
#[test]
fn relist_on_evicting_is_caught() {
    assert_load_bearing("mut_relist_on_evicting", &[(true, false)]);
}

/// A fold with data but no cursor is re-listed blind (the pre-fix start).
#[cfg(feature = "mut_unanchored_relist")]
#[test]
fn unanchored_relist_is_caught() {
    assert_load_bearing("mut_unanchored_relist", &[(false, true)]);
}

/// The restore takes the artifact's value even when the local one is newer.
#[cfg(feature = "mut_restore_ignores_versions")]
#[test]
fn restore_ignoring_versions_is_caught() {
    assert_load_bearing("mut_restore_ignores_versions", &[(true, false)]);
}

/// The restore deletes keys the bucket lists live.
#[cfg(feature = "mut_restore_ignores_listing")]
#[test]
fn restore_ignoring_the_listing_is_caught() {
    assert_load_bearing("mut_restore_ignores_listing", &[(true, false)]);
}

/// The fold forgets that it saw retention evict current values: once the
/// operator turns eviction off, the key-listing diff deletes a key that aged
/// out.
#[cfg(feature = "mut_no_eviction_memo")]
#[test]
fn eviction_memo_is_load_bearing() {
    let checker = run(
        RepairModel {
            config_forgot: true,
            ..shipped(true, false)
        },
        "mut_no_eviction_memo: eviction turned off",
    );
    assert!(
        checker.discovery(NAMES[3]).is_some(),
        "the checker found no deleted aged-out key without the memo"
    );
}

/// The key-listing resync stays correct without the ack barrier now that the
/// main loop takes the listing after its drain (axiom 6 suffices); the
/// barrier is defense in depth (still load-bearing in
/// `tests/model_resync_order.rs`, which models a listing taken before the
/// handoff).
#[cfg(feature = "mut_no_ack_barrier")]
#[test]
fn relist_holds_without_the_ack_barrier() {
    run(shipped(false, false), "mut_no_ack_barrier: keeps-current").assert_properties();
}
