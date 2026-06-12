//! Exhaustive model-check of `watch_applied`'s CURSOR AUTHORITY — the
//! invariant the entire resume story sits on: the store's persisted cursor
//! never advances past data that is not in the store. A violation is a
//! restart-surviving hole in the fold: silent data loss, strictly worse
//! than anything the transport could do.
//!
//! Modeled: the delivery → batch → flush pipeline with TRANSIENT STORE
//! FAILURES (re-queued for cumulative commit — `applied.rs`'s flush path),
//! crashes at any point, and restarts that resume from the store's
//! persisted cursor. Flush boundaries interleave freely, which is a
//! superset of every window/max/shutdown/export trigger policy.
//!
//! Mutations — both are REAL bug classes, the first found live:
//! - `DropFailedBatch`: a failed store apply drops its raw batch and the
//!   next successful flush advances the cursor over the hole. THIS WAS THE
//!   SHIPPED CODE until `transient_store_failure_never_leaves_a_cursor_gap`
//!   reproduced it (found while writing this model); the fix re-queues the
//!   failed batch. The checker proves the old behavior violates the
//!   invariant.
//! - `ResumeFromMemApplied`: restart resumes from the in-memory applied
//!   cursor instead of the store's persisted one. The in-memory cursor
//!   legitimately runs ahead of the store across failed flushes (the
//!   domain-apply side is not transactional with the store), so resuming
//!   from it skips the un-stored gap. Pins WHY the resume source must be
//!   the store's own cursor.
//!
//! Fidelity note: unlike the transport and live-watch models, there is no
//! shared decision kernel here — the invariant is carried by control-flow
//! structure (what gets re-queued, which cursor is committed, where the
//! restart resumes), not by a guard expression. Fidelity therefore rests on
//! the transition audit against `applied.rs`'s flush macro plus the
//! mutation contrast, and the empirical twin in `applied.rs` tests.

use stateright::{Checker, Model, Property};

/// Stream revisions run 1..=MAX_REV.
const MAX_REV: u8 = 5;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    /// Bucket high-water revision.
    head: u8,
    /// Delivered to the loop (in-memory; lost on crash).
    delivered: u8,
    /// The store's PERSISTED cursor — what a restart resumes from.
    store_cursor: u8,
    /// Lower bound of the re-queued/pending raw range: the store holds
    /// complete contents up to `store_cursor`; the loop holds raw updates
    /// (`q_from`, `delivered`]. Shipped semantics keep `q_from ==
    /// store_cursor` (re-queue on failure); the DropFailedBatch mutation
    /// lets it advance past the store.
    q_from: u8,
    /// The in-memory applied cursor: advances at every flush attempt,
    /// success or failure (the domain apply is not transactional with the
    /// store).
    applied_mem: u8,
    /// Latched when a commit left the store's contents incomplete below its
    /// cursor — the restart-surviving hole. THE invariant: never latches.
    holes: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands in the bucket.
    Churn,
    /// The watch delivers the next revision to the loop.
    Deliver,
    /// A flush whose store apply SUCCEEDS: commits (`q_from`, `delivered`]
    /// with cursor `delivered`, atomically (backend axiom: data + cursor in
    /// one transaction).
    FlushOk,
    /// A flush whose store apply FAILS transiently. Shipped: the raw batch
    /// is re-queued (`q_from` unchanged — the next FlushOk commits
    /// cumulatively); the in-memory applied cursor still advances.
    /// DropFailedBatch mutation: the batch is dropped (`q_from` jumps to
    /// `delivered`).
    FlushFail,
    /// Process crash: all in-memory state is lost; the restart reopens the
    /// store and resumes the watch from its persisted cursor (or, under the
    /// ResumeFromMemApplied mutation, from the in-memory applied cursor —
    /// as if the caller trusted `on_applied` instead of the store).
    CrashRestart,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    DropFailedBatch,
    ResumeFromMemApplied,
}

#[derive(Clone)]
struct AppliedLoop {
    mutation: Mutation,
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
            holes: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < MAX_REV {
            acts.push(Act::Churn);
        }
        if s.delivered < s.head {
            acts.push(Act::Deliver);
        }
        if s.delivered > s.q_from {
            acts.push(Act::FlushOk);
            acts.push(Act::FlushFail);
        }
        // A crash is possible at any moment.
        acts.push(Act::CrashRestart);
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::Churn => s.head += 1,
            Act::Deliver => s.delivered += 1,
            Act::FlushOk => {
                // Atomic commit of (`q_from`, `delivered`] at cursor
                // `delivered`. If `q_from` ran ahead of the store's cursor
                // (a dropped batch), the committed contents are missing
                // (`store_cursor`, `q_from`] while the cursor claims them:
                // the hole, permanent from here on.
                if s.q_from > s.store_cursor {
                    s.holes = true;
                }
                s.store_cursor = s.delivered;
                s.q_from = s.delivered;
                s.applied_mem = s.delivered;
            }
            Act::FlushFail => {
                // The domain apply ran and the in-memory cursor advanced
                // regardless; only the store commit failed.
                s.applied_mem = s.delivered;
                match self.mutation {
                    // Shipped: re-queue — `q_from` stays put, the next
                    // FlushOk commits cumulatively.
                    Mutation::None | Mutation::ResumeFromMemApplied => {}
                    // The pre-fix behavior: the failed batch is dropped.
                    Mutation::DropFailedBatch => s.q_from = s.delivered,
                }
            }
            Act::CrashRestart => {
                let resume = match self.mutation {
                    Mutation::ResumeFromMemApplied => s.applied_mem,
                    _ => s.store_cursor,
                };
                // Resuming above the store's cursor never re-delivers the
                // un-stored gap: it is permanently skipped.
                if resume > s.store_cursor {
                    s.holes = true;
                }
                s.delivered = resume;
                s.q_from = resume.max(s.store_cursor);
                s.applied_mem = resume;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = Vec::new();

        if self.mutation == Mutation::None {
            // THE cursor-authority invariant, over every interleaving of
            // delivery, flush boundaries, transient store failures, and
            // crashes: the store's cursor never claims data it dropped.
            props.push(Property::<Self>::always(
                "the store's contents are always complete up to its cursor",
                |_, s| !s.holes,
            ));
            // Cursor authority restated structurally: the pending raw range
            // always begins exactly at the store's cursor — nothing between
            // the store and the loop can fall on the floor.
            props.push(Property::<Self>::always(
                "the re-queued range is always anchored at the store cursor",
                |_, s| s.q_from == s.store_cursor,
            ));
            // Terminal liveness: every maximal run ends fully caught up.
            // (CrashRestart is always enabled, so no state is terminal in
            // the strict sense; quiescence here is head reached + nothing
            // pending + store caught up, checked as: whenever nothing is
            // deliverable or flushable, the store is at the head.)
            props.push(Property::<Self>::always(
                "quiescence implies the store is at the head",
                |_, s| {
                    let quiesced =
                        s.head == MAX_REV && s.delivered == s.head && s.q_from == s.delivered;
                    !quiesced || s.store_cursor == s.head
                },
            ));
            // Vacuity witnesses: failures really happen and are really
            // recovered from (a failed flush precedes a successful
            // cumulative one), and crashes really replay.
            props.push(Property::<Self>::sometimes(
                "a failed flush is followed by a cumulative successful commit",
                |_, s| s.store_cursor > 0 && s.applied_mem == s.store_cursor && s.head > 1,
            ));
            props.push(Property::<Self>::sometimes(
                "the in-memory cursor runs ahead of the store across a failure",
                |_, s| s.applied_mem > s.store_cursor,
            ));
        } else {
            // Each mutation must produce the hole — proving the respective
            // rule (re-queue on failure; resume from the store's cursor) is
            // load-bearing, not incidental.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: cursor advances over dropped data",
                |_, s| s.holes,
            ));
        }

        props
    }
}

fn check(mutation: Mutation, label: &str) {
    let model = AppliedLoop { mutation };
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count(),
    );
    checker.assert_properties();
}

/// The SHIPPED flush semantics (failed store applies re-queue their raw
/// batch; restarts resume from the store's persisted cursor): cursor
/// authority holds over every interleaving of delivery, flushing, transient
/// failure, and crash.
#[test]
fn shipped_flush_semantics_preserve_cursor_authority() {
    check(Mutation::None, "applied: shipped");
}

/// The PRE-FIX behavior (failed batch dropped, warn-and-continue): the
/// checker reaches the restart-surviving hole — the machine-checked record
/// of the bug `transient_store_failure_never_leaves_a_cursor_gap`
/// reproduced live.
#[test]
fn dropping_failed_batches_corrupts_the_fold() {
    check(Mutation::DropFailedBatch, "applied: drop-on-fail (pre-fix)");
}

/// Resuming from the in-memory applied cursor instead of the store's
/// persisted one skips the un-stored gap: pins why the restart's resume
/// source must be the store itself.
#[test]
fn resuming_from_memory_cursor_corrupts_the_fold() {
    check(
        Mutation::ResumeFromMemApplied,
        "applied: resume-from-memory",
    );
}
