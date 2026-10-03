//! Exhaustive model-check of a FLEET repairing from each other's artifacts
//! (Stateright) — discharging the "exporters are correct folds" axiom the
//! other models assume.
//!
//! `tests/model.rs`, `tests/model_live_watch.rs` and `tests/model_repair.rs`
//! take an artifact at cursor `c` to hold the truth at `c`. In a real fleet
//! the exporter is just another watcher: it can itself expire, restore from
//! an older artifact, start with no cursor, and publish whatever its fold
//! holds. If any of those paths ever produced a wrong fold, the error would
//! propagate through artifacts to every node that restores. Here the nodes
//! ARE the exporters, and the checker proves the induction:
//!
//! - **Every published artifact is the truth at its cursor.**
//! - **Every live node's fold is the truth at its cursor.**
//! - **Every maximal run ends with every live node converged** to every
//!   write minus every real delete. A node may instead be fail-stopped — and
//!   then only because a write aged out before ANY node folded it: no fold
//!   anywhere holds it, so there is nothing safe to restore. The checker
//!   reaches that state (witness): it is the one data loss retention can
//!   cause, and it is loud.
//!
//! Three nodes: two start synced, one starts with no cursor and an empty
//! fold (it seeds from an artifact once the bucket has evicted current
//! values). One key; writes, eviction (head eviction on an evicting bucket,
//! marker purge on one that keeps current values), floor-guarded delivery,
//! expiry, repair (restore via `protocol::restore_allowed`, or the
//! key-listing diff), and monotone publishes (`protocol::
//! pointer_publish_allowed`). Repairs are atomic here; their steps are
//! checked in `tests/model_repair.rs`.
//!
//! Mutations the checker must catch: the key-listing repair on an evicting
//! bucket, and a cursor-less start that re-lists an already-evicted bucket —
//! each poisons the fleet's artifacts.

use slipstream::protocol::{
    PointerState, RepairMode, RepairPlan, cursorless_start_needs_repair, listing_is_truth,
    plan_repair, pointer_publish_allowed, restore_allowed, resume_window_ok,
};
use stateright::{Checker, Model, Property};

const CAP: usize = 8;
const N: usize = 3;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Node {
    /// Not yet started (no cursor, empty fold).
    fresh: bool,
    /// The fold's value for the key (`Some(put revision)`).
    fold: Option<u8>,
    /// The watch frontier / fold cursor.
    cursor: u8,
    /// Expired and awaiting a repair (fail-stopped until one is possible).
    expired: bool,
    /// This node's fold came (at some point) from a restore.
    restored: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    head: u8,
    ops: [bool; CAP],
    evicted: bool,
    /// The published pointer: (cursor, value the artifact holds).
    pointer: Option<(u8, Option<u8>)>,
    nodes: [Node; N],
    bad_publish: bool,
    restored_exporter_published: bool,
    seeded_from_artifact: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    Put,
    Delete,
    Evict,
    Start(usize),
    Deliver(usize),
    Expire(usize),
    Repair(usize),
    Publish(usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    RelistOnEvicting,
    FreshStartRelist,
}

#[derive(Clone)]
struct Fleet {
    evicting: bool,
    mutation: Mutation,
    /// The key's revisions run 1..=max_rev.
    max_rev: u8,
}

impl Fleet {
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
    fn listed(s: &St) -> Option<u8> {
        if s.evicted { None } else { Self::truth(s) }
    }
    /// The listing as the planner sees it: the shared kernel on the bucket's
    /// retention. `RelistOnEvicting` forges it as the truth.
    fn listing_truth(&self, s: &St) -> Option<bool> {
        if self.mutation == Mutation::RelistOnEvicting {
            return Some(true);
        }
        Some(listing_is_truth(
            self.evicting,
            Self::first_revision(s),
            false,
        ))
    }
    /// Does an expired node repair by the artifact restore (else the
    /// key-listing diff)? The shared planner, under the shipped
    /// `ExpiryRepair::Auto`.
    fn restores(&self, s: &St) -> bool {
        match plan_repair(RepairMode::Auto, self.listing_truth(s)) {
            RepairPlan::Restore => true,
            RepairPlan::Relist => false,
            plan => unreachable!("Auto never plans {plan:?}"),
        }
    }
    fn restore_target(s: &St, n: &Node) -> Option<(u8, Option<u8>)> {
        let (c, v) = s.pointer?;
        restore_allowed(c as u64, n.cursor as u64, Self::first_revision(s)).then_some((c, v))
    }
}

impl Model for Fleet {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        let mut ops = [false; CAP];
        ops[1] = true;
        let synced = Node {
            fresh: false,
            fold: Some(1),
            cursor: 1,
            expired: false,
            restored: false,
        };
        let fresh = Node {
            fresh: true,
            fold: None,
            cursor: 0,
            expired: false,
            restored: false,
        };
        vec![St {
            head: 1,
            ops,
            evicted: false,
            pointer: None,
            nodes: [synced, synced, fresh],
            bad_publish: false,
            restored_exporter_published: false,
            seeded_from_artifact: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < self.max_rev {
            acts.push(Act::Put);
            acts.push(Act::Delete);
        }
        let evictable = self.evicting || !s.ops[s.head as usize];
        if !s.evicted && evictable {
            acts.push(Act::Evict);
        }
        for (i, n) in s.nodes.iter().enumerate() {
            if n.fresh {
                acts.push(Act::Start(i));
                continue;
            }
            if n.expired {
                let possible = !self.restores(s) || Self::restore_target(s, n).is_some();
                if possible {
                    acts.push(Act::Repair(i));
                }
                continue;
            }
            let window = resume_window_ok(n.cursor as u64, Self::first_revision(s));
            if window && s.head > n.cursor && !s.evicted {
                acts.push(Act::Deliver(i));
            }
            if !window {
                acts.push(Act::Expire(i));
            }
            let current = s.pointer.map(|(c, _)| PointerState::Present {
                rank: Some(c as u64),
            });
            if n.cursor >= 1
                && s.pointer.is_none_or(|(c, _)| c < n.cursor)
                && pointer_publish_allowed(
                    &current.unwrap_or(PointerState::Absent),
                    n.cursor as u64,
                )
            {
                acts.push(Act::Publish(i));
            }
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
            Act::Start(i) => {
                // The shared start kernel on an empty fold. `FreshStartRelist`
                // forges the listing as the truth.
                let truth = if self.mutation == Mutation::FreshStartRelist {
                    Some(true)
                } else {
                    self.listing_truth(&s)
                };
                let relist = !cursorless_start_needs_repair(RepairMode::Auto, false, truth);
                let (listed, head) = (Fleet::listed(&s), s.head);
                let n = &mut s.nodes[i];
                n.fresh = false;
                if relist {
                    // Full re-list: the latest retained message, then live.
                    n.fold = listed;
                    n.cursor = head;
                } else {
                    // An empty fold on an already-evicted bucket seeds from
                    // an artifact (`ExpiryRepair::Auto`).
                    n.expired = true;
                }
            }
            Act::Deliver(i) => {
                let (truth, head) = (Self::truth(&s), s.head);
                let n = &mut s.nodes[i];
                n.fold = truth;
                n.cursor = head;
            }
            Act::Expire(i) => s.nodes[i].expired = true,
            Act::Repair(i) => {
                if self.restores(&s) {
                    let (c, v) = Self::restore_target(&s, &s.nodes[i])?;
                    let n = &mut s.nodes[i];
                    if n.cursor == 0 {
                        s.seeded_from_artifact = true;
                    }
                    n.fold = v;
                    n.cursor = c;
                    n.restored = true;
                } else {
                    // Key-listing diff + re-list, positioned at the head.
                    let (listed, head) = (Self::listed(&s), s.head);
                    let n = &mut s.nodes[i];
                    n.fold = listed;
                    n.cursor = head;
                }
                s.nodes[i].expired = false;
            }
            Act::Publish(i) => {
                let n = s.nodes[i];
                if n.fold != Self::truth_at(&s, n.cursor) {
                    s.bad_publish = true;
                }
                if n.restored {
                    s.restored_exporter_published = true;
                }
                s.pointer = Some((n.cursor, n.fold));
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::<Self>::always(
                "every published artifact is the truth at its cursor",
                |_, s| !s.bad_publish && s.pointer.is_none_or(|(c, v)| v == Fleet::truth_at(s, c)),
            ),
            Property::<Self>::always(
                "every live node's fold is the truth at its cursor",
                |_, s| {
                    s.nodes
                        .iter()
                        .filter(|n| !n.fresh && !n.expired)
                        .all(|n| n.fold == Fleet::truth_at(s, n.cursor))
                },
            ),
            Property::<Self>::always(
                "every maximal run ends with every live node converged, and a node fail-stops only if no fold anywhere holds the head",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    if !acts.is_empty() {
                        return true;
                    }
                    let truth = Fleet::truth(s);
                    let live_converged = s
                        .nodes
                        .iter()
                        .filter(|n| !n.expired)
                        .all(|n| n.fold == truth && n.cursor == s.head);
                    let any_expired = s.nodes.iter().any(|n| n.expired);
                    let no_fold_has_head = s.nodes.iter().all(|n| n.cursor < s.head)
                        && s.pointer.is_none_or(|(c, _)| c < s.head);
                    live_converged && (!any_expired || no_fold_has_head)
                },
            ),
        ];
        if self.mutation == Mutation::None {
            // (A keeps-current fleet repairs by the key-listing diff and
            // never restores; its witnesses are the repairs themselves.)
            if self.evicting {
                props.push(Property::<Self>::sometimes(
                    "a node that restored from an artifact publishes one (the induction is exercised)",
                    |_, s| s.restored_exporter_published,
                ));
                props.push(Property::<Self>::sometimes(
                    "a cursor-less node seeds from an artifact",
                    |_, s| s.seeded_from_artifact,
                ));
                props.push(Property::<Self>::sometimes(
                    "a write ages out before any node folds it: the fleet fail-stops, loudly",
                    |m, s| {
                        let mut acts = Vec::new();
                        m.actions(s, &mut acts);
                        acts.is_empty() && s.nodes.iter().all(|n| n.expired)
                    },
                ));
            }
        }
        props
    }
}

fn run(model: Fleet, label: &str) -> impl Checker<Fleet> {
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count()
    );
    checker
}

#[test]
fn evicting_fleet_artifacts_stay_correct() {
    run(
        Fleet {
            evicting: true,
            mutation: Mutation::None,
            max_rev: 4,
        },
        "fleet: evicting",
    )
    .assert_properties();
}

#[test]
fn keeps_current_fleet_artifacts_stay_correct() {
    run(
        Fleet {
            evicting: false,
            mutation: Mutation::None,
            max_rev: 4,
        },
        "fleet: keeps-current",
    )
    .assert_properties();
}

#[test]
fn mutations_poison_the_fleet_and_are_caught() {
    for (mutation, label) in [
        (
            Mutation::RelistOnEvicting,
            "fleet mutation: relist on evicting",
        ),
        (
            Mutation::FreshStartRelist,
            "fleet mutation: cursor-less start re-lists",
        ),
    ] {
        let checker = run(
            Fleet {
                evicting: true,
                mutation,
                max_rev: 4,
            },
            label,
        );
        assert!(
            checker
                .discovery("every published artifact is the truth at its cursor")
                .is_some()
                || checker
                    .discovery("every live node's fold is the truth at its cursor")
                    .is_some(),
            "{label}: the checker must find a wrong fold or a poisoned artifact"
        );
        println!(
            "  poisoned artifact reachable: {}",
            checker
                .discovery("every published artifact is the truth at its cursor")
                .is_some()
        );
    }
}

/// Deeper bounds: one more revision.
/// `cargo test --release --test model_fleet -- --ignored`
#[test]
#[ignore = "deep bounds: run in release"]
fn deep_fleet() {
    for evicting in [true, false] {
        run(
            Fleet {
                evicting,
                mutation: Mutation::None,
                max_rev: 6,
            },
            &format!("deep fleet: evicting={evicting}, rev <= 6"),
        )
        .assert_properties();
    }
}
