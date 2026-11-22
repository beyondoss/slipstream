//! The cursor-authority kernel behind `watch_applied`: every decision about
//! WHEN a batch commits, WHICH cursor is persisted, what happens to a batch
//! whose store apply failed, the order an export and a resync repair run in,
//! and when the resync's ack may be sent. `step(state, event) -> (state,
//! actions)`: no I/O, no async, no clock. The shell (`applied::Fold`) turns
//! what it observes into [`Event`]s, calls [`step`], and runs the [`Action`]s
//! through one interpreter; it decides nothing itself.
//!
//! The invariant every rule here serves: the store's persisted cursor never
//! advances past data the store does not hold.
//!
//! What the kernel owns (state that has no other home): whether a batch is
//! open, whether a stream position is pending, whether the in-memory cursor
//! gained the eviction memo the store has not seen, the consecutive store
//! failure streak, and where an in-flight flush or resync repair is. What the
//! shell passes in as facts (`Event` fields): the sizes of its queues, whether
//! it holds a store. The data themselves (updates, cursors, the store) stay in
//! the shell.

/// Consecutive store-apply failures that fail-stop the watch. A transient
/// failure re-queues its raw batch (cursor authority: the store's cursor and
/// contents advance together, always); a persistent streak fail-stops before
/// the re-queued backlog grows without bound.
pub const MAX_STORE_APPLY_FAILURES: u32 = 16;

/// Where a flush is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Flush {
    #[default]
    Idle,
    /// The store fold is running on its blocking task. `advanced`: the flush
    /// moved the applied cursor.
    Storing { advanced: bool },
}

/// Kernel state of one `watch_applied` fold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct State {
    /// A batch opened since the last flush (the window timer is armed).
    pub open: bool,
    /// A stream position was received since the last flush: the next flush
    /// advances the applied cursor to it.
    pub high: bool,
    /// The applied cursor gained the eviction memo and the store does not have
    /// it yet.
    pub memo_dirty: bool,
    /// Consecutive store-apply failures.
    pub fail_streak: u32,
    pub flush: Flush,
    /// A resync's synthetic deletes are being folded; the ack is owed after.
    pub relisting: bool,
    /// Where an artifact restore is.
    pub restore: Restore,
}

/// Where an artifact restore is. Every step waits on a fact the shell
/// observes, so an event that arrives out of phase is ignored (a restart
/// builds a fresh `State`: a crash part-way leaves the whole repair to re-run).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Restore {
    #[default]
    Idle,
    /// Settled; waiting for the authoritative ahead check.
    Ahead,
    /// Diffing the fold against the artifact.
    Diff,
    /// Listing live keys for the delete candidates.
    Listing,
    /// Folding the difference under the old cursor.
    Fold,
}

/// Why a restore changed nothing and replied with an error. The watch fails
/// with it; the main loop carries on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// The artifact is not ahead of the fold: a restore never moves it back.
    NotAhead,
    /// The log evicted writes after the artifact's cursor while the live keys
    /// were listed, so the listing cannot vouch for them.
    WindowAged,
}

/// What the shell's queues hold, as facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Queued {
    /// Parsed updates waiting for the domain `apply`.
    pub batch: usize,
    /// Raw updates waiting for the store (stream order).
    pub raw: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreOutcome {
    Ok,
    /// The store's apply returned an error.
    Failed,
    /// The blocking task panicked: the store is gone.
    Panicked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RepairKind {
    /// Remember durably that retention evicts current values.
    Evicting,
    /// The key-listing resync.
    Relist,
    /// The artifact restore.
    Restore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// An update was parsed and queued. `positioned`: it carries a real
    /// stream position. `max`: the flush bound.
    Received {
        positioned: bool,
        queued: Queued,
        max: usize,
    },
    /// A restore took the artifact's cursor as the pending position.
    AdvanceTo,
    /// A flush was asked for.
    FlushRequested { queued: Queued, has_store: bool },
    /// The store fold returned.
    StoreDone(StoreOutcome),
    /// The eviction memo is to be remembered durably.
    RememberEvicting { already_seen: bool, has_store: bool },
    /// After a flush while settling: is the store behind the queue?
    SettleCheck { raw_queued: usize },
    /// An export was requested.
    ExportRequested { has_store: bool },
    /// A repair arrived at the main loop.
    RepairRequested(RepairKind),
    /// The resync's synthetic deletes are folded.
    RelistFolded,
    /// The authoritative ahead check ran: `ahead` is
    /// `protocol::restore_ahead(artifact cursor, applied cursor)`.
    RestoreAhead { ahead: bool },
    /// The fold was diffed against the artifact: `has_deletes`, some key the
    /// artifact lacks is a delete candidate.
    RestoreDiffed { has_deletes: bool },
    /// The live keys were listed and retention re-read after: `window_ok` is
    /// `protocol::resume_window_ok(artifact cursor, first retained revision)`.
    RestoreListed { window_ok: bool },
    /// The difference is folded and flushed.
    RestoreFolded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fatal {
    /// `MAX_STORE_APPLY_FAILURES` consecutive store failures (the count is
    /// `State::fail_streak`).
    StoreFailStreak,
    StorePanicked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// Keep the received update's position as the pending cursor.
    RecordHigh,
    /// The queue is full: flush now.
    FlushNow,
    /// Run the domain `apply` on the parsed batch, to completion.
    ApplyDomain,
    /// Move the applied cursor to the pending position (the eviction memo
    /// rides over from the cursor it replaces).
    AdvanceApplied,
    /// Fold the raw batch into the store at the applied cursor.
    FoldStore,
    /// The fold landed: the store is back, the commit is observed.
    StoreCommitted,
    /// The fold failed: the store is back and the raw batch goes back to the
    /// FRONT of the queue for a cumulative commit.
    Requeue,
    /// Without a store, applying the batch is the commit.
    CommitWithoutStore,
    /// Tell the consumer the cursor advanced, then clear the pending position.
    NotifyApplied,
    /// Fail the watch.
    Fail(Fatal),
    /// The memo flag is set on the in-memory cursor.
    SetMemo,
    /// Flush the pending batch (the interpreter runs the flush protocol).
    Flush,
    /// Flush again until the store holds everything: a settle step.
    Settle,
    /// Export the store (after the flush before it).
    ExportStore,
    /// An export with no store to export.
    ReplyNoStore,
    /// Fold everything the watch task already delivered into the batch.
    DrainBacklog,
    /// Run the resync's listing diff.
    RunRelist,
    /// The authoritative ahead check; answer with `RestoreAhead`.
    CheckAhead,
    /// Diff the fold against the artifact; answer with `RestoreDiffed`.
    RunDiff,
    /// List the live keys, then re-read retention; answer with `RestoreListed`.
    ListLive,
    /// Fold the difference in chunks, each flushed, under the OLD cursor;
    /// answer with `RestoreFolded`.
    FoldChunks,
    /// Take the artifact's cursor and flush it, with the final chunk's
    /// re-queued remainder if any.
    CommitCursor,
    /// Reply to the watch task with the artifact's cursor.
    ReplyRestored,
    /// Reply to the watch task with an error: nothing was changed.
    Refuse(Refusal),
    /// Remember the eviction memo durably.
    RememberEvicting,
    /// Release the watch task parked on the resync: the deletes are folded.
    AckResync,
}

const MAX_ACTIONS: usize = 6;

/// A short fixed-capacity action list: the hot path allocates nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Actions {
    len: u8,
    items: [Action; MAX_ACTIONS],
}

impl Actions {
    const fn none() -> Self {
        Self {
            len: 0,
            items: [Action::FlushNow; MAX_ACTIONS],
        }
    }

    fn push(&mut self, a: Action) {
        self.items[self.len as usize] = a;
        self.len += 1;
    }

    pub fn as_slice(&self) -> &[Action] {
        &self.items[..self.len as usize]
    }

    pub fn contains(&self, a: Action) -> bool {
        self.as_slice().contains(&a)
    }
}

impl<'a> IntoIterator for &'a Actions {
    type Item = &'a Action;
    type IntoIter = std::slice::Iter<'a, Action>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

/// What follows a flush's store step (or its absence): the consumer hears of
/// an advance only after the store settled, and the pending position clears.
#[inline]
fn finish(s: &mut State, a: &mut Actions, advanced: bool, had_store: bool) {
    if advanced {
        if !had_store {
            a.push(Action::CommitWithoutStore);
        }
        a.push(Action::NotifyApplied);
        s.high = false;
    }
}

/// Which cursor a run hands its caller as the next `resume`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResumeFrom {
    /// The store's own persisted cursor: it never names data the store lacks,
    /// while the in-memory cursor runs ahead of it across a failed store
    /// apply (the re-queued batch dies with the run).
    Store,
    /// No store holds the data (none given, or lost): the in-memory applied
    /// cursor is the only position there is.
    Memory,
}

/// The resume cursor's source at the end of a run. `holds_store`: the fold
/// still holds its store.
#[inline]
pub const fn resume_from(holds_store: bool) -> ResumeFrom {
    // Mutation (model tests only): resume from the in-memory cursor even
    // with a store.
    if holds_store && !cfg!(feature = "mut_resume_from_memory") {
        ResumeFrom::Store
    } else {
        ResumeFrom::Memory
    }
}

/// The one transition function.
#[inline]
pub fn step(mut s: State, e: Event) -> (State, Actions) {
    let mut a = Actions::none();
    match e {
        Event::Received {
            positioned,
            queued,
            max,
        } => {
            // Every received update bumps the pending high-water, parsed or
            // not — but only with a real position: an unknown version neither
            // mints a fake cursor nor clobbers the real high from earlier in
            // the batch.
            if positioned {
                s.high = true;
                a.push(Action::RecordHigh);
            }
            s.open = true;
            if queued.batch >= max || queued.raw >= max {
                a.push(Action::FlushNow);
            }
        }
        Event::AdvanceTo => {
            s.high = true;
            a.push(Action::RecordHigh);
        }
        Event::FlushRequested { queued, has_store } => {
            s.open = false;
            // Nothing received since the last flush: nothing to do at all.
            if queued.batch == 0 && queued.raw == 0 && !s.high && !s.memo_dirty {
                return (s, a);
            }
            if queued.batch > 0 {
                a.push(Action::ApplyDomain);
            }
            let advanced = s.high;
            if advanced {
                a.push(Action::AdvanceApplied);
            }
            // A cursor-only advance (nothing raw to fold) still commits to
            // the store.
            if (queued.raw > 0 || advanced || s.memo_dirty) && has_store {
                a.push(Action::FoldStore);
                s.flush = Flush::Storing { advanced };
            } else {
                finish(&mut s, &mut a, advanced, has_store);
            }
        }
        Event::StoreDone(outcome) => {
            let Flush::Storing { advanced } = s.flush else {
                return (s, a);
            };
            s.flush = Flush::Idle;
            match outcome {
                StoreOutcome::Ok => {
                    s.fail_streak = 0;
                    s.memo_dirty = false;
                    a.push(Action::StoreCommitted);
                    finish(&mut s, &mut a, advanced, true);
                }
                StoreOutcome::Failed => {
                    s.fail_streak += 1;
                    if s.fail_streak >= MAX_STORE_APPLY_FAILURES {
                        a.push(Action::Fail(Fatal::StoreFailStreak));
                    } else {
                        // The failed range precedes anything received since:
                        // it goes back to the front, so the store's cursor
                        // and contents advance together.
                        #[cfg(not(feature = "mut_drop_failed_batch"))]
                        a.push(Action::Requeue);
                        finish(&mut s, &mut a, advanced, true);
                    }
                }
                StoreOutcome::Panicked => a.push(Action::Fail(Fatal::StorePanicked)),
            }
        }
        Event::RememberEvicting {
            already_seen,
            has_store,
        } => {
            if !already_seen {
                s.memo_dirty = has_store;
                a.push(Action::SetMemo);
                a.push(Action::Flush);
            }
        }
        Event::SettleCheck { raw_queued } => {
            // Mutation (model tests only): settle with one flush, not until
            // the store holds everything.
            if raw_queued > 0 && !cfg!(feature = "mut_no_settle_retry") {
                a.push(Action::Flush);
            }
        }
        Event::ExportRequested { has_store } => {
            // The pending batch is flushed first, so the exported cursor is
            // the applied cursor (or, across a transiently failed store
            // flush, the store's own lagging, self-consistent one).
            a.push(Action::Flush);
            a.push(if has_store {
                Action::ExportStore
            } else {
                Action::ReplyNoStore
            });
        }
        Event::RepairRequested(kind) => match kind {
            RepairKind::Evicting => a.push(Action::RememberEvicting),
            RepairKind::Relist | RepairKind::Restore => {
                // The repair diffs the STORE, so it sees everything delivered:
                // drain the watch task's backlog, then settle.
                // Mutation (model tests only): skip the drain.
                #[cfg(not(feature = "mut_no_drain"))]
                a.push(Action::DrainBacklog);
                a.push(Action::Settle);
                if kind == RepairKind::Relist {
                    s.relisting = true;
                    // Mutation (model tests only): ack the parked watch task
                    // before the deletes are folded.
                    #[cfg(feature = "mut_no_ack_barrier")]
                    a.push(Action::AckResync);
                    a.push(Action::RunRelist);
                } else {
                    s.restore = Restore::Ahead;
                    a.push(Action::CheckAhead);
                }
            }
        },
        Event::RestoreAhead { ahead } => {
            if s.restore == Restore::Ahead {
                // A restore never moves the fold backward.
                let ahead = ahead || cfg!(feature = "mut_no_restore_guard");
                if ahead {
                    s.restore = Restore::Diff;
                    // Mutation (model tests only): take the cursor before the
                    // diff, so a crash between leaves a cursor past data the
                    // store lacks.
                    #[cfg(feature = "mut_cursor_before_diff")]
                    a.push(Action::CommitCursor);
                    a.push(Action::RunDiff);
                } else {
                    s.restore = Restore::Idle;
                    a.push(Action::Refuse(Refusal::NotAhead));
                }
            }
        }
        Event::RestoreDiffed { has_deletes } => {
            if s.restore == Restore::Diff {
                // Nothing to delete: no listing to vouch for, so nothing to
                // re-check.
                if has_deletes {
                    s.restore = Restore::Listing;
                    a.push(Action::ListLive);
                } else {
                    s.restore = Restore::Fold;
                    a.push(Action::FoldChunks);
                }
            }
        }
        Event::RestoreListed { window_ok } => {
            if s.restore == Restore::Listing {
                // The listing vouches for every write after the artifact's
                // cursor only while the log still holds all of them.
                let window_ok = window_ok
                    || cfg!(any(
                        feature = "mut_no_listing_recheck",
                        feature = "mut_no_restore_guard"
                    ));
                if window_ok {
                    s.restore = Restore::Fold;
                    a.push(Action::FoldChunks);
                } else {
                    s.restore = Restore::Idle;
                    a.push(Action::Refuse(Refusal::WindowAged));
                }
            }
        }
        Event::RestoreFolded => {
            if s.restore == Restore::Fold {
                s.restore = Restore::Idle;
                // Now the fold IS the artifact's (in scope): its cursor
                // commits with the final chunk's re-queued remainder, and only
                // then does the watch task hear of it.
                #[cfg(not(feature = "mut_cursor_before_diff"))]
                a.push(Action::CommitCursor);
                a.push(Action::ReplyRestored);
            }
        }
        Event::RelistFolded => {
            // The ack barrier: the fallback watch starts only after the
            // synthetic deletes are folded, so a delete-then-recreate during
            // the gap converges.
            if s.relisting {
                s.relisting = false;
                #[cfg(not(feature = "mut_no_ack_barrier"))]
                a.push(Action::AckResync);
            }
        }
    }
    (s, a)
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: Queued = Queued { batch: 1, raw: 1 };

    #[cfg(not(feature = "mut_resume_from_memory"))]
    #[test]
    fn the_resume_cursor_is_the_stores_whenever_a_store_holds_the_data() {
        assert_eq!(resume_from(true), ResumeFrom::Store);
        assert_eq!(resume_from(false), ResumeFrom::Memory);
    }

    #[cfg(not(feature = "mut_drop_failed_batch"))]
    fn flush(s: State, has_store: bool) -> (State, Actions) {
        step(
            s,
            Event::FlushRequested {
                queued: Q,
                has_store,
            },
        )
    }

    fn received(s: State, positioned: bool) -> (State, Actions) {
        step(
            s,
            Event::Received {
                positioned,
                queued: Q,
                max: 8,
            },
        )
    }

    #[test]
    fn an_unpositioned_update_never_records_a_cursor() {
        let (s, a) = received(State::default(), false);
        assert!(!s.high && a.as_slice().is_empty() && s.open);
        let (s, a) = received(State::default(), true);
        assert!(s.high && a.as_slice() == [Action::RecordHigh]);
    }

    #[cfg(not(feature = "mut_drop_failed_batch"))]
    #[test]
    fn a_failed_store_fold_requeues_and_still_notifies() {
        let (s, _) = received(State::default(), true);
        let (s, a) = flush(s, true);
        assert_eq!(
            a.as_slice(),
            [
                Action::ApplyDomain,
                Action::AdvanceApplied,
                Action::FoldStore
            ]
        );
        let (s, a) = step(s, Event::StoreDone(StoreOutcome::Failed));
        assert_eq!(a.as_slice(), [Action::Requeue, Action::NotifyApplied]);
        assert!(!s.high && s.fail_streak == 1);
    }

    #[test]
    fn the_failure_streak_fail_stops_at_the_bound() {
        let mut s = State::default();
        for n in 1..=MAX_STORE_APPLY_FAILURES {
            s.flush = Flush::Storing { advanced: false };
            let (next, a) = step(s, Event::StoreDone(StoreOutcome::Failed));
            s = next;
            let fatal = a.contains(Action::Fail(Fatal::StoreFailStreak));
            assert_eq!(fatal, n == MAX_STORE_APPLY_FAILURES, "streak {n}");
        }
    }

    #[cfg(not(any(feature = "mut_no_ack_barrier", feature = "mut_no_drain")))]
    #[test]
    fn a_resync_acks_only_after_its_deletes_are_folded() {
        let (s, a) = step(State::default(), Event::RepairRequested(RepairKind::Relist));
        assert_eq!(
            a.as_slice(),
            [Action::DrainBacklog, Action::Settle, Action::RunRelist]
        );
        assert!(!a.contains(Action::AckResync));
        let (s, a) = step(s, Event::RelistFolded);
        assert_eq!(a.as_slice(), [Action::AckResync]);
        let (_, a) = step(s, Event::RelistFolded);
        assert!(a.as_slice().is_empty(), "one ack per resync");
    }

    /// Drive a restore through the kernel: the facts each step reports.
    #[cfg(not(any(
        feature = "mut_no_drain",
        feature = "mut_no_restore_guard",
        feature = "mut_no_listing_recheck",
        feature = "mut_cursor_before_diff"
    )))]
    fn restore(ahead: bool, has_deletes: bool, window_ok: bool) -> Vec<Action> {
        let mut seen = Vec::new();
        let (mut s, a) = step(
            State::default(),
            Event::RepairRequested(RepairKind::Restore),
        );
        assert_eq!(
            a.as_slice(),
            [Action::DrainBacklog, Action::Settle, Action::CheckAhead]
        );
        seen.extend(a.as_slice());
        let events = [
            Event::RestoreAhead { ahead },
            Event::RestoreDiffed { has_deletes },
            Event::RestoreListed { window_ok },
            Event::RestoreFolded,
        ];
        for e in events {
            let (next, a) = step(s, e);
            s = next;
            seen.extend(a.as_slice());
        }
        assert_eq!(s.restore, Restore::Idle, "a restore ends idle");
        seen
    }

    #[cfg(not(any(
        feature = "mut_no_drain",
        feature = "mut_no_restore_guard",
        feature = "mut_no_listing_recheck",
        feature = "mut_cursor_before_diff"
    )))]
    #[test]
    fn a_restore_commits_the_cursor_after_the_diff_and_replies_last() {
        let seen = restore(true, true, true);
        assert_eq!(
            &seen[3..],
            [
                Action::RunDiff,
                Action::ListLive,
                Action::FoldChunks,
                Action::CommitCursor,
                Action::ReplyRestored,
            ]
        );
        // No delete candidate: no listing, so no window re-check.
        let seen = restore(true, false, true);
        assert_eq!(
            &seen[3..],
            [
                Action::RunDiff,
                Action::FoldChunks,
                Action::CommitCursor,
                Action::ReplyRestored,
            ]
        );
    }

    #[cfg(not(any(
        feature = "mut_no_drain",
        feature = "mut_no_restore_guard",
        feature = "mut_no_listing_recheck",
        feature = "mut_cursor_before_diff"
    )))]
    #[test]
    fn a_restore_refuses_before_it_changes_anything() {
        assert_eq!(
            &restore(false, true, true)[3..],
            [Action::Refuse(Refusal::NotAhead)]
        );
        assert_eq!(
            &restore(true, true, false)[3..],
            [
                Action::RunDiff,
                Action::ListLive,
                Action::Refuse(Refusal::WindowAged)
            ]
        );
    }

    #[test]
    fn a_restore_event_out_of_phase_is_ignored() {
        let (s, a) = step(State::default(), Event::RestoreFolded);
        assert!(a.as_slice().is_empty());
        assert_eq!(s, State::default());
    }

    #[test]
    fn an_export_flushes_first() {
        let (_, a) = step(State::default(), Event::ExportRequested { has_store: true });
        assert_eq!(a.as_slice(), [Action::Flush, Action::ExportStore]);
    }
}
