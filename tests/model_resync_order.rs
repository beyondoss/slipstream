//! Exhaustive model-check of the cursor-expired resync's ORDERING claim
//! (Stateright) — until now carried by a comment in `applied.rs`:
//!
//! > "The synthetic deletes are strictly ordered before the first re-list
//! > put, so a key deleted and re-created during the gap converges
//! > correctly."
//!
//! The mechanism under test is the ACK BARRIER: the watch task lists the
//! bucket's live keys, sends them to the main loop, and AWAITS the ack —
//! which the main loop sends only after folding the synthetic deletes —
//! before starting the fallback watch. If the fallback could start earlier,
//! its re-list put for a key re-created during the gap could fold BEFORE
//! the synthetic delete, which then erases the re-creation: the fold drops
//! a key the bucket has, permanently (until that key's next update). The
//! `mut_no_ack_barrier` kernel mutation is exactly that interleaving, and the
//! checker reaches the divergence: the barrier is load-bearing.
//!
//! Setup modeled: the fold holds key K from before the gap; during the gap
//! K was deleted and its marker evicted (the resync's reason to exist). K
//! may be RE-CREATED at any point — before the listing (then it is live in
//! the listing, no synthetic delete, and the re-list delivers it) or after
//! (synthetic delete fires, and ONLY the barrier guarantees the re-list put
//! folds after it).
//!
//! THE MODEL RUNS THE PRODUCTION KERNEL. When the ack is sent comes from
//! `slipstream::cursor_kernel::step`: the main loop's `RepairRequested(Relist)`
//! and `RelistFolded` events, and the `AckResync` action the kernel emits
//! (after the folded deletes, never before). The model keeps only the
//! environment (bucket, listing, fold contents, watch phase) and gates the
//! fallback watch on having received `AckResync`, as `resync_stale_keys`'s
//! `ack_rx.await` does. It restates no decision.
//!
//! The mutation is a cargo feature that breaks the KERNEL: `mut_no_ack_barrier`
//! makes the kernel emit `AckResync` with the repair request, before the
//! deletes are folded. Run with `--features mut_no_ack_barrier`; the test then
//! asserts the checker finds the divergence.
//!
//! Fidelity: events fed are `RepairRequested(Relist)` and `RelistFolded`
//! (`fold_in`'s `RunRelist` arm). `DrainBacklog`/`Settle` have no model-visible
//! effect (no backlog is modeled; the store path is `tests/model_applied.rs`),
//! `RunRelist` is the synthetic-delete fold, `AckResync` releases the watch
//! task. `FoldSyntheticDeletes` is modeled atomic; in code, the synth-delete
//! flush's STORE apply can transiently fail. The ordering still holds by
//! composition with `tests/model_applied.rs`: the failed batch is re-queued
//! AT THE FRONT (stream order preserved), so the eventual cumulative commit
//! applies delete-then-put in order within one backend batch, and the
//! domain apply saw the deletes before the ack regardless. The listing is
//! modeled instantaneous; a key changing mid-scan is equivalent to ordering
//! its change before or after `ListLive`, both explored. Scope: one key,
//! one delete-recreate cycle; longer histories converge by per-key LWW on
//! the same machinery. The state space is tiny and finite, so every run,
//! mutated included, terminates without a depth cut.

use slipstream::cursor_kernel::{Action, Event, RepairKind, State as K, step};
use stateright::{Checker, Model, Property};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum WatchPhase {
    /// Resume failed with CursorExpired; the live listing has not run.
    Expired,
    /// Live keys listed; the `k_in_list` snapshot is fixed. The request is
    /// in flight to the main loop; the watch task awaits the ack.
    Listed,
    /// Ack received: fallback watch delivering.
    Fallback,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    phase: WatchPhase,
    /// Bucket truth: K live right now. Starts false (deleted during the
    /// gap, marker evicted).
    bucket_k: bool,
    /// What the listing captured for K (meaningful from Listed onward).
    k_in_list: bool,
    /// The fold's view of K. Starts true (stale from before the gap).
    fold_k: bool,
    /// The main loop took the request (the kernel ran `RepairRequested`).
    requested: bool,
    /// The kernel's `RunRelist` is pending: the deletes are not folded yet.
    relist_running: bool,
    /// The kernel emitted `AckResync`: the parked watch task is released.
    acked: bool,
    /// Main loop folded the synthetic deletes.
    deletes_folded: bool,
    /// The fallback's re-list put for K was folded (delivered only if K is
    /// live when the fallback runs).
    relist_put_folded: bool,
    /// The production kernel's state.
    k: K,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// The watch task lists live keys (snapshot of bucket truth, this
    /// instant) and sends the resync request.
    ListLive,
    /// K is re-created in the bucket (any time).
    RecreateK,
    /// The main loop takes the request: `Event::RepairRequested(Relist)`.
    TakeRequest,
    /// The main loop folds a synthetic delete for every fold key absent from
    /// the listing, then feeds `Event::RelistFolded`.
    FoldSyntheticDeletes,
    /// The fallback watch starts once the watch task is released.
    StartFallback,
    /// The fallback delivers K's current value as a put (re-list or live
    /// tail), folded into the fold.
    DeliverRelistPut,
}

#[derive(Clone)]
struct ResyncOrder;

impl Model for ResyncOrder {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        vec![St {
            phase: WatchPhase::Expired,
            bucket_k: false,
            k_in_list: false,
            fold_k: true,
            requested: false,
            relist_running: false,
            acked: false,
            deletes_folded: false,
            relist_put_folded: false,
            k: K::default(),
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.phase == WatchPhase::Expired {
            acts.push(Act::ListLive);
        }
        if !s.bucket_k {
            acts.push(Act::RecreateK);
        }
        if s.phase != WatchPhase::Expired && !s.requested {
            acts.push(Act::TakeRequest);
        }
        if s.relist_running {
            acts.push(Act::FoldSyntheticDeletes);
        }
        // The watch task is parked on `ack_rx` until the kernel acks.
        if s.phase == WatchPhase::Listed && s.acked {
            acts.push(Act::StartFallback);
        }
        if s.phase == WatchPhase::Fallback && s.bucket_k && !s.relist_put_folded {
            acts.push(Act::DeliverRelistPut);
        }
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::ListLive => {
                s.k_in_list = s.bucket_k;
                s.phase = WatchPhase::Listed;
            }
            Act::RecreateK => s.bucket_k = true,
            Act::TakeRequest => {
                s.requested = true;
                let (k, acts) = step(s.k, Event::RepairRequested(RepairKind::Relist));
                s.k = k;
                for act in &acts {
                    match act {
                        // No backlog is modeled.
                        Action::DrainBacklog | Action::Settle => {}
                        Action::RunRelist => s.relist_running = true,
                        Action::AckResync => s.acked = true,
                        other => unreachable!("relist request: {other:?}"),
                    }
                }
            }
            Act::FoldSyntheticDeletes => {
                // Synthetic delete for every fold key the listing lacks.
                if !s.k_in_list {
                    s.fold_k = false;
                }
                s.deletes_folded = true;
                s.relist_running = false;
                let (k, acts) = step(s.k, Event::RelistFolded);
                s.k = k;
                for act in &acts {
                    match act {
                        Action::AckResync => s.acked = true,
                        other => unreachable!("relist folded: {other:?}"),
                    }
                }
            }
            Act::StartFallback => s.phase = WatchPhase::Fallback,
            Act::DeliverRelistPut => {
                s.fold_k = true;
                s.relist_put_folded = true;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // THE ordering theorem: every maximal run converges, including
            // delete-then-recreate straddling the listing in either
            // direction. (Terminal-state invariant, cycle-proof.)
            Property::<Self>::always(
                "every maximal run ends with the fold equal to the bucket",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty() || s.fold_k == s.bucket_k
                },
            ),
            // Witnesses: both straddles genuinely occur.
            Property::<Self>::sometimes(
                "recreate-after-listing converges via delete-then-put ordering",
                |_, s| !s.k_in_list && s.relist_put_folded && s.fold_k && s.bucket_k,
            ),
            Property::<Self>::sometimes(
                "recreate-before-listing converges via the listing itself",
                |_, s| s.k_in_list && s.fold_k && s.bucket_k,
            ),
            Property::<Self>::sometimes("never-recreated K is reconciled away", |_, s| {
                !s.fold_k && !s.bucket_k && s.deletes_folded
            }),
        ]
    }
}

fn run(label: &str) -> impl Checker<ResyncOrder> {
    let started = std::time::Instant::now();
    let checker = ResyncOrder.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique, {:?}",
        checker.state_count(),
        checker.unique_state_count(),
        started.elapsed(),
    );
    checker
}

/// The production kernel (the ack follows the folded synthetic deletes):
/// delete-then-recreate converges in every interleaving. The comment in
/// `applied.rs` is a theorem.
#[cfg(not(feature = "mut_no_ack_barrier"))]
#[test]
fn ack_barrier_makes_resync_ordering_converge() {
    run("resync order: kernel").assert_properties();
}

/// Kernel mutation: the ack goes out with the request. The re-list put can
/// fold before the synthetic delete, which then erases a live key: the
/// machine-checked reason the handshake exists.
#[cfg(feature = "mut_no_ack_barrier")]
#[test]
fn without_ack_barrier_recreated_keys_are_lost() {
    let c = run("resync order: mut_no_ack_barrier");
    assert!(
        c.discovery("every maximal run ends with the fold equal to the bucket")
            .is_some(),
        "no counterexample"
    );
}
