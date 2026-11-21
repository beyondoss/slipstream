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
    /// Run the artifact restore.
    RunRestore,
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
            if raw_queued > 0 {
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
                a.push(Action::DrainBacklog);
                a.push(Action::Settle);
                if kind == RepairKind::Relist {
                    s.relisting = true;
                    a.push(Action::RunRelist);
                } else {
                    a.push(Action::RunRestore);
                }
            }
        },
        Event::RelistFolded => {
            // The ack barrier: the fallback watch starts only after the
            // synthetic deletes are folded, so a delete-then-recreate during
            // the gap converges.
            if s.relisting {
                s.relisting = false;
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

    #[test]
    fn an_export_flushes_first() {
        let (_, a) = step(State::default(), Event::ExportRequested { has_store: true });
        assert_eq!(a.as_slice(), [Action::Flush, Action::ExportStore]);
    }
}
