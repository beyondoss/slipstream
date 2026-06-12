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
//! `NoAckBarrier` mutation is exactly that interleaving, and the checker
//! reaches the divergence — proving the barrier is load-bearing.
//!
//! Setup modeled: the fold holds key K from before the gap; during the gap
//! K was deleted and its marker evicted (the resync's reason to exist). K
//! may be RE-CREATED at any point — before the listing (then it is live in
//! the listing, no synthetic delete, and the re-list delivers it) or after
//! (synthetic delete fires, and ONLY the barrier guarantees the re-list put
//! folds after it).
//!
//! Fidelity: the barrier in code is `resync_stale_keys`'s `ack_rx.await`
//! between the `ResyncRequest` send and the fallback watch start, paired
//! with the main loop folding the synthetic deletes before sending the ack
//! (`applied.rs`). No shared kernel exists (it is control flow, not a
//! guard); fidelity rests on this transition audit, the mutation contrast,
//! and the mock-driven ordering tests in `applied.rs`.
//!
//! `FoldSyntheticDeletes` is modeled atomic; in code, the synth-delete
//! flush's STORE apply can transiently fail. The ordering still holds by
//! composition with `tests/model_applied.rs`: the failed batch is re-queued
//! AT THE FRONT (stream order preserved), so the eventual cumulative commit
//! applies delete-then-put in order within one backend batch, and the
//! domain apply saw the deletes before the ack regardless. The listing is
//! modeled instantaneous; a key changing mid-scan is equivalent to ordering
//! its change before or after `ListLive`, both explored. Scope: one key,
//! one delete-recreate cycle — longer histories converge by per-key LWW on
//! the same machinery.

use stateright::{Checker, Model, Property};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum WatchPhase {
    /// Resume failed with CursorExpired; the live listing has not run.
    Expired,
    /// Live keys listed; the `k_in_list` snapshot is fixed. The request is
    /// in flight to the main loop; the watch task awaits the ack.
    Listed,
    /// Ack received (or barrier skipped, under mutation): fallback watch
    /// delivering.
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
    /// Main loop folded the synthetic deletes (and acked).
    deletes_folded: bool,
    /// The fallback's re-list put for K was folded (delivered only if K is
    /// live when the fallback runs).
    relist_put_folded: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// The watch task lists live keys (snapshot of bucket truth, this
    /// instant) and sends the resync request.
    ListLive,
    /// K is re-created in the bucket (any time).
    RecreateK,
    /// The main loop processes the resync request: folds a synthetic delete
    /// for every fold key absent from the listing, then acks.
    FoldSyntheticDeletes,
    /// The fallback watch starts. SHIPPED: gated on the ack (deletes
    /// folded). NoAckBarrier mutation: gated only on the listing having
    /// happened.
    StartFallback,
    /// The fallback delivers K's current value as a put (re-list or live
    /// tail), folded into the fold.
    DeliverRelistPut,
}

#[derive(Clone)]
struct ResyncOrder {
    ack_barrier: bool,
}

impl Model for ResyncOrder {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        vec![St {
            phase: WatchPhase::Expired,
            bucket_k: false,
            k_in_list: false,
            fold_k: true,
            deletes_folded: false,
            relist_put_folded: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.phase == WatchPhase::Expired {
            acts.push(Act::ListLive);
        }
        if !s.bucket_k {
            acts.push(Act::RecreateK);
        }
        if s.phase != WatchPhase::Expired && !s.deletes_folded {
            acts.push(Act::FoldSyntheticDeletes);
        }
        if s.phase == WatchPhase::Listed {
            // SHIPPED: the watch task is parked on ack_rx until the main
            // loop folds the deletes. MUTATION: it proceeds immediately.
            if !self.ack_barrier || s.deletes_folded {
                acts.push(Act::StartFallback);
            }
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
            Act::FoldSyntheticDeletes => {
                // Synthetic delete for every fold key the listing lacks.
                if !s.k_in_list {
                    s.fold_k = false;
                }
                s.deletes_folded = true;
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
        let mut props: Vec<Property<Self>> = Vec::new();

        if self.ack_barrier {
            // THE ordering theorem: every maximal run converges — including
            // delete-then-recreate straddling the listing in either
            // direction. (Terminal-state invariant, cycle-proof.)
            props.push(Property::<Self>::always(
                "every maximal run ends with the fold equal to the bucket",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty() || s.fold_k == s.bucket_k
                },
            ));
            // Witnesses: both straddles genuinely occur.
            props.push(Property::<Self>::sometimes(
                "recreate-after-listing converges via delete-then-put ordering",
                |_, s| !s.k_in_list && s.relist_put_folded && s.fold_k && s.bucket_k,
            ));
            props.push(Property::<Self>::sometimes(
                "recreate-before-listing converges via the listing itself",
                |_, s| s.k_in_list && s.fold_k && s.bucket_k,
            ));
            props.push(Property::<Self>::sometimes(
                "never-recreated K is reconciled away",
                |_, s| !s.fold_k && !s.bucket_k && s.deletes_folded,
            ));
        } else {
            // Without the barrier the checker must reach the lost-recreate
            // divergence: re-list put folded first, synthetic delete erases
            // it, K is live but the fold dropped it — terminally.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: synthetic delete erases a re-created key",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    acts.is_empty() && s.bucket_k && !s.fold_k
                },
            ));
        }

        props
    }
}

fn check(ack_barrier: bool, label: &str) {
    let model = ResyncOrder { ack_barrier };
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count(),
    );
    checker.assert_properties();
}

/// The SHIPPED handshake (fallback gated on the ack, which follows the
/// folded synthetic deletes): delete-then-recreate converges in every
/// interleaving — the comment in `applied.rs` is now a theorem.
#[test]
fn ack_barrier_makes_resync_ordering_converge() {
    check(true, "resync order: ack barrier");
}

/// Without the barrier, the re-list put can fold before the synthetic
/// delete, which then erases a live key — the machine-checked reason the
/// handshake exists.
#[test]
fn without_ack_barrier_recreated_keys_are_lost() {
    check(false, "resync order: no barrier");
}
