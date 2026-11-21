//! Exhaustive model-check of `watch_applied`'s CURSOR AUTHORITY: the
//! invariant the entire resume story sits on: the store's persisted cursor
//! never advances past data that is not in the store. A violation is a
//! restart-surviving hole in the fold: silent data loss, strictly worse
//! than anything the transport could do.
//!
//! Modeled: the delivery -> batch -> flush pipeline with TRANSIENT STORE
//! FAILURES, crashes at any point, and restarts that resume from the cursor
//! the kernel names. Flush boundaries interleave freely, which is a
//! superset of every window/max/shutdown/export trigger policy.
//!
//! THE MODEL RUNS THE PRODUCTION KERNEL. Every decision (which flush steps
//! run, whether a failed store fold re-queues its batch, the failure streak
//! that fail-stops, which cursor a restart resumes from) comes from
//! `slipstream::cursor_kernel::{step, resume_from}`, the functions
//! `applied::Fold` calls. The model keeps only environment state (what the
//! stream delivered, what the store holds, crashes) and feeds the kernel
//! events; it interprets the returned actions the way `Fold` does, one for
//! one, and restates no decision.
//!
//! Mutations are cargo features that break the KERNEL (see Cargo.toml), not
//! the model. Run with `--features <name>`; each flips this file's test to
//! assert the counterexample is found:
//! - `mut_drop_failed_batch`: the kernel stops emitting `Requeue`, so a failed
//!   store apply drops its raw batch and the next successful flush advances
//!   the cursor over the hole. THIS WAS THE SHIPPED CODE until
//!   `transient_store_failure_never_leaves_a_cursor_gap` reproduced it.
//! - `mut_resume_from_memory`: `resume_from` names the in-memory applied
//!   cursor even with a store. That cursor legitimately runs ahead of the
//!   store across failed flushes, so resuming from it skips the un-stored gap.
//!
//! Fidelity: transitions are the kernel's. Events fed: `Received` (positioned,
//! `max` unbounded: the flush point is a free model step instead, which
//! covers every `FlushNow`), `FlushRequested`, `StoreDone(Ok | Failed)`.
//! Not fed: the eviction memo (`RememberEvicting`), export and repair events
//! (repairs: `tests/model_repair.rs`, `tests/model_resync_order.rs`), unparsed
//! updates, no-store runs (there is no store cursor to protect), and
//! `StoreDone(Panicked)` (the store is gone; only a fresh open recovers).
//! `Fail(StoreFailStreak)` ends the run and restarts it, like a crash.
//! Actions are interpreted as `Fold` does: `Requeue` leaves the raw range
//! queued, its absence after a failure drops it; `AdvanceApplied` moves the
//! in-memory cursor to the pending position; `FoldStore` takes the raw batch.
//! The state space is finite (revisions are capped, the failure streak is
//! bounded by the kernel), so every run, mutated ones included, terminates
//! without a depth cut that could hide a violation.

use slipstream::cursor_kernel::{
    Action, Event, Fatal, Flush, Queued, ResumeFrom, State as K, StoreOutcome, resume_from, step,
};
use stateright::{Checker, Model, Property};

/// Stream revisions run 1..=MAX_REV.
const MAX_REV: u8 = 5;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    /// Bucket high-water revision.
    head: u8,
    /// Delivered to the loop (in-memory; lost on crash). Also the pending
    /// stream position (every modeled update is positioned).
    delivered: u8,
    /// The store's PERSISTED cursor: what a restart resumes from.
    store_cursor: u8,
    /// Lower bound of the queued raw range: the store holds complete
    /// contents up to `store_cursor`; the loop holds raw updates
    /// (`q_from`, `delivered`].
    q_from: u8,
    /// The in-memory applied cursor: advances at every flush attempt,
    /// success or failure (the domain apply is not transactional with the
    /// store).
    applied_mem: u8,
    /// The cursor the running store fold commits at, while one runs.
    inflight: Option<u8>,
    /// The production kernel's state.
    k: K,
    /// Latched when a commit left the store's contents incomplete below its
    /// cursor: the restart-surviving hole. THE invariant: never latches.
    holes: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands in the bucket.
    Churn,
    /// The watch delivers the next revision: `Event::Received`.
    Deliver,
    /// A flush boundary: `Event::FlushRequested`.
    Flush,
    /// The store fold returns: `Event::StoreDone`.
    StoreDone(StoreOutcome),
    /// Process crash: all in-memory state is lost; the restart reopens the
    /// store and resumes from the cursor `resume_from` names.
    CrashRestart,
}

#[derive(Clone)]
struct AppliedLoop;

impl AppliedLoop {
    fn queued(s: &St) -> Queued {
        Queued {
            batch: (s.delivered - s.applied_mem) as usize,
            raw: (s.delivered - s.q_from) as usize,
        }
    }

    /// The run ends (crash or fail-stop) and restarts from `resume_from`.
    fn restart(s: &mut St) {
        let resume = match resume_from(true) {
            ResumeFrom::Store => s.store_cursor,
            ResumeFrom::Memory => s.applied_mem,
        };
        // Resuming above the store's cursor never re-delivers the un-stored
        // gap: it is permanently skipped.
        if resume > s.store_cursor {
            s.holes = true;
        }
        s.delivered = resume;
        s.q_from = resume.max(s.store_cursor);
        s.applied_mem = resume;
        s.inflight = None;
        s.k = K::default();
    }
}

impl Model for AppliedLoop {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        vec![St {
            head: 0,
            delivered: 0,
            store_cursor: 0,
            q_from: 0,
            applied_mem: 0,
            inflight: None,
            k: K::default(),
            holes: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < MAX_REV {
            acts.push(Act::Churn);
        }
        if let Flush::Storing { .. } = s.k.flush {
            // The loop awaits the store fold: nothing else runs on it.
            acts.push(Act::StoreDone(StoreOutcome::Ok));
            acts.push(Act::StoreDone(StoreOutcome::Failed));
        } else {
            if s.delivered < s.head {
                acts.push(Act::Deliver);
            }
            acts.push(Act::Flush);
        }
        // A crash is possible at any moment.
        acts.push(Act::CrashRestart);
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::Churn => s.head += 1,
            Act::Deliver => {
                s.delivered += 1;
                let (k, acts) = step(
                    s.k,
                    Event::Received {
                        positioned: true,
                        queued: Self::queued(&s),
                        max: usize::MAX,
                    },
                );
                s.k = k;
                for act in &acts {
                    match act {
                        // The pending position is `delivered`.
                        Action::RecordHigh => {}
                        other => unreachable!("received: {other:?}"),
                    }
                }
            }
            Act::Flush => {
                let before = s.clone();
                let (k, acts) = step(
                    s.k,
                    Event::FlushRequested {
                        queued: Self::queued(&s),
                        has_store: true,
                    },
                );
                s.k = k;
                for act in &acts {
                    match act {
                        // The domain apply has no model-visible effect.
                        Action::ApplyDomain => {}
                        Action::AdvanceApplied => s.applied_mem = s.delivered,
                        Action::FoldStore => s.inflight = Some(s.applied_mem),
                        // Nothing observes the consumer notification.
                        Action::NotifyApplied => {}
                        other => unreachable!("flush: {other:?}"),
                    }
                }
                if s == before {
                    return None;
                }
            }
            Act::StoreDone(outcome) => {
                let (k, acts) = step(s.k, Event::StoreDone(outcome));
                s.k = k;
                let cursor = s.inflight.take().expect("a fold is in flight");
                let mut requeued = false;
                let mut failed_run = false;
                for act in &acts {
                    match act {
                        Action::StoreCommitted => {
                            // Atomic commit of (`q_from`, `cursor`] at
                            // `cursor` (backend axiom: data + cursor in one
                            // transaction). If `q_from` ran ahead of the
                            // store's cursor (a dropped batch), the committed
                            // contents are missing (`store_cursor`, `q_from`]
                            // while the cursor claims them: the hole.
                            if s.q_from > s.store_cursor {
                                s.holes = true;
                            }
                            s.store_cursor = cursor;
                            s.q_from = cursor;
                        }
                        Action::Requeue => requeued = true,
                        Action::NotifyApplied => {}
                        Action::Fail(Fatal::StoreFailStreak) => failed_run = true,
                        other => unreachable!("store done: {other:?}"),
                    }
                }
                if outcome == StoreOutcome::Failed && !requeued {
                    // The raw batch was taken for the fold and never went
                    // back: it is gone from the queue.
                    s.q_from = cursor;
                }
                if failed_run {
                    Self::restart(&mut s);
                }
            }
            Act::CrashRestart => Self::restart(&mut s),
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // THE cursor-authority invariant, over every interleaving of
            // delivery, flush boundaries, transient store failures, and
            // crashes: the store's cursor never claims data it dropped.
            Property::<Self>::always(
                "the store's contents are always complete up to its cursor",
                |_, s| !s.holes,
            ),
            // Cursor authority restated structurally: the pending raw range
            // always begins exactly at the store's cursor: nothing between
            // the store and the loop can fall on the floor.
            Property::<Self>::always(
                "the re-queued range is always anchored at the store cursor",
                |_, s| s.q_from == s.store_cursor,
            ),
            // Quiescence: whenever nothing is deliverable and nothing is
            // queued, the store is at the head.
            Property::<Self>::always("quiescence implies the store is at the head", |_, s| {
                let quiesced =
                    s.head == MAX_REV && s.delivered == s.head && s.q_from == s.delivered;
                !quiesced || s.store_cursor == s.head
            }),
            // Vacuity witnesses: failures really happen and are really
            // recovered from, and the in-memory cursor really runs ahead.
            Property::<Self>::sometimes(
                "a failed flush is followed by a cumulative successful commit",
                |_, s| s.store_cursor > 0 && s.applied_mem == s.store_cursor && s.head > 1,
            ),
            Property::<Self>::sometimes(
                "the in-memory cursor runs ahead of the store across a failure",
                |_, s| s.applied_mem > s.store_cursor,
            ),
        ]
    }
}

fn run(label: &str) -> impl Checker<AppliedLoop> {
    let started = std::time::Instant::now();
    let checker = AppliedLoop.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        started.elapsed(),
    );
    checker
}

/// The production kernel: failed store applies re-queue their raw batch;
/// restarts resume from the store's persisted cursor. Cursor authority
/// holds over every interleaving of delivery, flushing, transient failure,
/// and crash.
#[cfg(not(any(feature = "mut_drop_failed_batch", feature = "mut_resume_from_memory")))]
#[test]
fn the_kernel_preserves_cursor_authority() {
    run("applied: kernel").assert_properties();
}

/// Kernel mutation: a failed batch is dropped (the pre-fix behavior). The
/// checker reaches the restart-surviving hole.
#[cfg(feature = "mut_drop_failed_batch")]
#[test]
fn dropping_failed_batches_corrupts_the_fold() {
    let c = run("applied: mut_drop_failed_batch");
    assert!(
        c.discovery("the store's contents are always complete up to its cursor")
            .is_some(),
        "no counterexample"
    );
}

/// Kernel mutation: restart resumes from the in-memory cursor. The checker
/// reaches the skipped gap.
#[cfg(feature = "mut_resume_from_memory")]
#[test]
fn resuming_from_memory_cursor_corrupts_the_fold() {
    let c = run("applied: mut_resume_from_memory");
    assert!(
        c.discovery("the store's contents are always complete up to its cursor")
            .is_some(),
        "no counterexample"
    );
}
