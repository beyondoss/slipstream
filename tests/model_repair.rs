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
//! Mutations the checker must catch, one per step: no drain, no pre-repair
//! flush retry (the bug), cursor committed before the restore diff, no ack
//! barrier, no restore guard, key-listing diff on an evicting bucket.
//!
//! Axioms: correct exporters (an artifact at `c` holds the truth at `c` —
//! discharged by `tests/model_fleet.rs`); a full re-list is not overrun by
//! retention before it delivers what it found (axiom 6 of `tests/model.rs`).

use slipstream::protocol::{
    KeyRestore, KeyState, RepairMode, RepairPlan, cursorless_start_needs_repair, listing_is_truth,
    plan_repair, restore_ahead, restore_allowed, restore_key, resume_window_ok,
};
use stateright::{Checker, Model, Property};

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
    /// Key-listing diff; what the listing showed for the key.
    Relist { listed: Option<u8> },
    /// Artifact restore to this cursor, with what the live-key listing (taken
    /// after the fetch) showed for the key.
    Restore { target: u8, listed: Option<u8> },
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

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum MPhase {
    Idle,
    /// Repair received; next: drain the channel.
    Drain(Req),
    /// Pre-repair flush (retried until the store holds everything).
    PreFlush(Req, bool),
    /// Key-listing diff: one flush of the synthetic delete, then ack.
    RelistFlush(bool),
    /// Restore: one flush of the diff (under the old cursor).
    RestoreChunk(u8, bool),
    /// Restore: the cursor commit, then reply.
    RestoreCommit(u8, bool),
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
    m: MPhase,
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
    MDrain,
    Flush(FlushOutcome),
    MNext,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    NoDrain,
    NoPreFlushRetry,
    CursorBeforeDiff,
    NoAckBarrier,
    NoRestoreGuard,
    RelistOnEvicting,
    /// A fold with data but no cursor re-listed blind (the pre-fix start).
    UnanchoredRelist,
    /// The restore takes the artifact's value even when the local one is
    /// newer (the pre-fix diff).
    RestoreIgnoresVersions,
    /// The restore deletes keys the bucket lists live (the pre-fix diff).
    RestoreIgnoresListing,
}

#[derive(Clone)]
struct RepairModel {
    evicting: bool,
    /// The repair armed (`ExpiryRepair`'s mode).
    mode: RepairMode,
    fresh: bool,
    mutation: Mutation,
    /// Let retention evict during a key-listing repair (see `actions`).
    drop_axiom_6: bool,
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
    /// retention. `RelistOnEvicting` forges it as the truth.
    fn listing_truth(&self, s: &St) -> Option<bool> {
        if self.mutation == Mutation::RelistOnEvicting {
            return Some(true);
        }
        Some(listing_is_truth(self.evicting, Self::first_revision(s)))
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
        s.m = MPhase::Idle;
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
                if matches!(s.m, MPhase::PreFlush(..)) {
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
    /// `target` holds the truth there. `RestoreIgnoresVersions` forges both
    /// sides revisionless (the pre-fix diff: take anything not identical);
    /// `RestoreIgnoresListing` forges the key unlisted.
    fn restore_op(&self, s: &St, target: u8, listed: Option<u8>) -> Option<Upd> {
        let want = Self::truth_at(s, target);
        let versions = self.mutation != Mutation::RestoreIgnoresVersions;
        let state = |v: Option<u8>| match v {
            Some(r) => KeyState::At(versions.then_some(r as u64)),
            None => KeyState::Absent,
        };
        let listed_live = listed.is_some() && self.mutation != Mutation::RestoreIgnoresListing;
        let identical = s.store_val.is_some() && s.store_val == want;
        match restore_key(state(s.store_val), state(want), identical, listed_live) {
            KeyRestore::Keep => None,
            KeyRestore::TakeArtifact => Some(Upd {
                pos: None,
                value: want,
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

    fn restore_ok(&self, s: &St, target: u8, local: u8) -> bool {
        self.mutation == Mutation::NoRestoreGuard
            || (target > 0 && restore_allowed(target as u64, local as u64, Self::first_revision(s)))
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
            m: MPhase::Idle,
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
                || matches!(s.req, Some(Req::Relist { .. }))
                || matches!(
                    s.m,
                    MPhase::Drain(Req::Relist { .. })
                        | MPhase::PreFlush(Req::Relist { .. }, _)
                        | MPhase::RelistFlush(_)
                ));
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
        match s.m {
            MPhase::Idle => {
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
            MPhase::Drain(_) => acts.push(Act::MDrain),
            MPhase::PreFlush(_, attempted) => {
                let retry = self.mutation != Mutation::NoPreFlushRetry || !attempted;
                if Self::flushable(s) && retry {
                    flush_outcomes(acts);
                }
                let done = if self.mutation == Mutation::NoPreFlushRetry {
                    attempted || !Self::flushable(s)
                } else {
                    !Self::flushable(s)
                };
                if done {
                    acts.push(Act::MNext);
                }
            }
            MPhase::RelistFlush(attempted)
            | MPhase::RestoreChunk(_, attempted)
            | MPhase::RestoreCommit(_, attempted) => {
                if !attempted && Self::flushable(s) {
                    flush_outcomes(acts);
                }
                if attempted || !Self::flushable(s) {
                    acts.push(Act::MNext);
                }
            }
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
                    let has_data =
                        s.store_val.is_some() && self.mutation != Mutation::UnanchoredRelist;
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
                        if self.restore_ok(&s, target, local) && target > 0 {
                            s.req = Some(Req::Restore {
                                target,
                                listed: Self::listed(&s),
                            });
                            s.w = WPhase::AwaitReply;
                        } else {
                            s.w = WPhase::Failed;
                        }
                    }
                    RepairPlan::Relist => {
                        s.req = Some(Req::Relist {
                            listed: Self::listed(&s),
                        });
                        s.w = if self.mutation == Mutation::NoAckBarrier {
                            WPhase::Watch {
                                f: 0,
                                guarded: false,
                            }
                        } else {
                            WPhase::AwaitAck
                        };
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
                s.m = MPhase::Drain(req);
            }
            Act::MDrain => {
                let MPhase::Drain(req) = s.m else { return None };
                if self.mutation != Mutation::NoDrain {
                    if !s.chan.is_empty() {
                        s.drained_backlog = true;
                    }
                    for u in std::mem::take(&mut s.chan) {
                        Self::ingest(&mut s, u);
                    }
                }
                s.m = MPhase::PreFlush(req, false);
            }
            Act::Flush(outcome) => {
                let before = s.m;
                if Self::flush(&mut s, outcome) {
                    s.m = match before {
                        MPhase::PreFlush(r, _) => MPhase::PreFlush(r, true),
                        MPhase::RelistFlush(_) => MPhase::RelistFlush(true),
                        MPhase::RestoreChunk(t, _) => MPhase::RestoreChunk(t, true),
                        MPhase::RestoreCommit(t, _) => MPhase::RestoreCommit(t, true),
                        other => other,
                    };
                }
            }
            Act::MNext => match s.m {
                MPhase::PreFlush(Req::Relist { listed }, _) => {
                    // Diff the STORE against the listing.
                    if s.store_val.is_some() && listed.is_none() {
                        let u = Upd {
                            pos: None,
                            value: None,
                        };
                        s.batch.push(u);
                        s.raw.push(u);
                    }
                    s.relisted = true;
                    s.m = MPhase::RelistFlush(false);
                }
                MPhase::PreFlush(Req::Restore { target, listed }, _) => {
                    // The authoritative ahead check.
                    if self.mutation != Mutation::NoRestoreGuard
                        && !restore_ahead(target as u64, s.applied as u64)
                    {
                        s.reply = Some(Reply::Refused);
                        s.m = MPhase::Idle;
                    } else if self.mutation == Mutation::CursorBeforeDiff {
                        s.batch_high = Some(target);
                        s.m = MPhase::RestoreCommit(target, false);
                    } else {
                        if let Some(u) = self.restore_op(&s, target, listed) {
                            s.batch.push(u);
                            s.raw.push(u);
                        }
                        s.m = MPhase::RestoreChunk(target, false);
                    }
                }
                MPhase::RelistFlush(_) => {
                    s.reply = Some(Reply::Ack);
                    s.m = MPhase::Idle;
                }
                MPhase::RestoreChunk(target, _) => {
                    s.batch_high = Some(target);
                    s.m = MPhase::RestoreCommit(target, false);
                }
                MPhase::RestoreCommit(target, _) => {
                    if self.mutation == Mutation::CursorBeforeDiff && !s.restored {
                        // The diff, after the cursor already moved.
                        s.restored = true;
                        if let Some(u) = self.restore_op(&s, target, Self::listed(&s)) {
                            s.batch.push(u);
                            s.raw.push(u);
                        }
                        s.m = MPhase::RestoreChunk(target, false);
                        return Some(s);
                    }
                    s.restored = true;
                    s.reply = Some(Reply::Restored(target));
                    s.m = MPhase::Idle;
                }
                _ => return None,
            },
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
        if self.mutation == Mutation::None {
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
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count()
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
        mutation: Mutation::None,
        drop_axiom_6: false,
        max_rev: 4,
        max_crashes: 1,
        max_transient: 2,
    }
}

#[test]
fn keeps_current_repair_steps_are_correct() {
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
    run(shipped(true, false), "repair steps: evicting").assert_properties();
}

#[test]
fn fresh_start_repair_steps_are_correct() {
    run(shipped(false, true), "repair steps: keeps-current, fresh").assert_properties();
    run(shipped(true, true), "repair steps: evicting, fresh").assert_properties();
}

/// Every mutation must produce a counterexample to at least one safety or
/// terminal property, on the bucket kind it applies to.
#[test]
fn every_repair_step_is_load_bearing() {
    let cases = [
        (Mutation::NoDrain, true, false),
        (Mutation::NoDrain, false, false),
        (Mutation::NoPreFlushRetry, false, false),
        (Mutation::NoPreFlushRetry, true, false),
        (Mutation::CursorBeforeDiff, true, false),
        (Mutation::NoAckBarrier, false, false),
        (Mutation::NoRestoreGuard, true, false),
        (Mutation::RelistOnEvicting, true, false),
        (Mutation::UnanchoredRelist, false, true),
        (Mutation::RestoreIgnoresVersions, true, false),
        (Mutation::RestoreIgnoresListing, true, false),
    ];
    let names = [
        "while the store's cursor is resumable, the store plus the tail is the truth",
        "the store's cursor never moves backward",
        "the domain never sees the key's revision go backward",
        "a repair never deletes the key's current value",
        "while the applied cursor is resumable, the domain plus the tail is the truth",
        "every maximal run ends with the store and domain equal to every write minus every real delete",
    ];
    for (mutation, evicting, fresh) in cases {
        let label = format!(
            "mutation {}: {}",
            match mutation {
                Mutation::None => "none",
                Mutation::NoDrain => "no drain",
                Mutation::NoPreFlushRetry => "no pre-repair flush retry",
                Mutation::CursorBeforeDiff => "cursor before diff",
                Mutation::NoAckBarrier => "no ack barrier",
                Mutation::NoRestoreGuard => "no restore guard",
                Mutation::RelistOnEvicting => "relist on evicting",
                Mutation::UnanchoredRelist => "unanchored fold re-listed blind",
                Mutation::RestoreIgnoresVersions => "restore ignores versions",
                Mutation::RestoreIgnoresListing => "restore ignores the live listing",
            },
            if evicting {
                "evicting"
            } else {
                "keeps-current"
            }
        );
        let checker = run(
            RepairModel {
                mutation,
                ..shipped(evicting, fresh)
            },
            &label,
        );
        let found: Vec<&str> = names
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

/// The axiom the key-listing repair rests on, made explicit: drop it and the
/// checker finds a delete whose marker is purged between the listing and the
/// re-list. The artifact restore resumes on a floor-guarded watch and does
/// not need it — the evicting configuration holds without the axiom.
#[test]
fn relist_window_needs_axiom_6() {
    let checker = run(
        RepairModel {
            drop_axiom_6: true,
            ..shipped(false, false)
        },
        "repair steps: keeps-current, axiom 6 dropped",
    );
    assert!(
        checker
            .discovery(
                "every maximal run ends with the store and domain equal to every write minus every real delete"
            )
            .is_some(),
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
