//! Exhaustive model-check of the LIVE-WATCH retention race (Stateright) —
//! the axiom-6 boundary of `tests/model.rs`, now mechanized.
//!
//! The hazard: a live consumer's position can fall behind the stream's
//! retention floor. JetStream then silently skips evicted messages (the same
//! clamp behavior `tests/resync.rs` pins for resumes — consumers never error
//! on evicted messages, they just never see them). A skipped DELETE marker
//! leaves the fold holding a key that was deleted, permanently and silently:
//! the exact failure class the resume-time `check_resume_window` eliminates,
//! alive again mid-watch.
//!
//! The fix this model dictates: a **floor guard** during live consumption —
//! the same shared kernel guard (`protocol::resume_window_ok`) applied to the
//! delivered frontier instead of a resume cursor. A trip ends the watch with
//! `CursorExpired`, and `watch_applied` repairs it in process through the
//! same expiry path a resume takes. `GuardRepair` is that repair, collapsed to
//! one transition, in the form the bucket's retention calls for:
//!
//! - **Keeps current values** (`discard: new`, no `max_age`): the key-listing
//!   diff — fold := what the bucket lists, consumption re-entitled from the
//!   floor. Sound, because a key missing from the bucket was deleted.
//! - **Evicts current values** (`max_age`, `discard: old`): an ARTIFACT
//!   RESTORE — fold := the newest published artifact's state, consumption
//!   resumed from its cursor — gated by the shared `protocol::restore_allowed`
//!   kernel (ahead of the frontier, still inside retention). The key-listing
//!   diff here is the pre-fix behavior, kept as the `RelistOnEvicting`
//!   mutation the checker must catch: the bucket stops listing a key that
//!   merely aged out, and the diff deletes it.
//!
//! Correct means **every write minus every real delete** (`key_at(head)`),
//! not "matches what NATS lists" — the two differ exactly when retention
//! evicts current values.
//!
//! Checked, exhaustively within bounds:
//! - GUARDED (both retentions): every maximal run ends with the fold equal to
//!   the truth — divergence is at worst transient, never permanent. The guard
//!   genuinely trips (witness), restores genuinely happen past aged-out values
//!   (witness), and markers are also delivered normally (witness), so the
//!   theorems are not vacuous.
//! - UNGUARDED (the pre-guard code, kept as the machine-checked record):
//!   permanent terminal divergence is REACHABLE, in both directions (a
//!   skipped delete marker, a skipped write).
//! - GUARDED: the fold never drops a value that is still current (no
//!   phantom deletes), and a repair never moves the fold backward.
//! - MUTATIONS: `RelistOnEvicting` reaches a phantom delete;
//!   `RestoreUnguarded` (restore any published artifact) reaches a backward
//!   repair.
//!
//! Scope, honestly: this models the ALL-scope watch, where every stream
//! message is deliverable to the consumer and a frontier-vs-floor gap
//! therefore implies genuinely missed messages. Prefix-scoped watches
//! cannot distinguish benign eviction (non-matching subjects) from a missed
//! marker without server-side help; they retain the narrowed operating
//! axiom (retention >> lag) plus the resume-time check on every restart.
//!
//! Abstractions, stated:
//! - **No client buffer.** `Compact` means eviction of messages the
//!   consumer has NOT received — the hazardous kind. In reality a message
//!   pushed to the client before eviction is still processed; that is
//!   received data, not loss, and needs no guard. (The code folds that
//!   delivered backlog before the repair — `applied.rs` drains the channel —
//!   which this model's atomic repair assumes.)
//! - **`GuardRepair` is a composite** of trip → `CursorExpired` → repair →
//!   resume, each half verified separately (`tests/model.rs` for the repairs,
//!   `tests/resync.rs` and `tests/eviction.rs` live); the composition is by
//!   argument.
//! - **Exporters are correct folds**: a published artifact at cursor `c`
//!   holds `key_at(c)` (axiom 5 of `tests/model.rs`, which proves it of every
//!   bootstrapped fold). `Publish` may lag arbitrarily — a stale pointer is
//!   exactly what the restore guard must refuse.

use slipstream::protocol::{restore_allowed, resume_window_ok};
use stateright::{Checker, Model, Property};

/// Stream revisions run 1..=MAX_REV.
const MAX_REV: u8 = 5;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    /// Stream high-water revision.
    head: u8,
    /// Retention floor: revisions <= floor are evicted.
    floor: u8,
    /// Revision of the sentinel key's delete marker, if deleted.
    delete_rev: Option<u8>,
    /// Revision of the sentinel's later write (an update, or a re-create
    /// after the delete), if any. Its initial put is revision 0.
    reput_rev: Option<u8>,
    /// Evicting retention only: the sentinel's current value aged out (never
    /// deleted; the bucket just stopped listing it).
    aged: bool,
    /// The newest published artifact's cursor, if any (monotone).
    pointer: Option<u8>,
    /// The consumer's delivered frontier (max revision handed to the fold).
    frontier: u8,
    /// The fold's view of the sentinel: the revision of the put it holds,
    /// `None` if absent.
    fold_key: Option<u8>,
    /// Latched when the floor guard tripped (witness that the guarded
    /// theorem is earned by repair, not by the race never occurring).
    tripped: bool,
    /// Latched when a repair restored from an artifact.
    restored: bool,
    /// Latched when a repair moved the frontier backward.
    regressed: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands (a put to some other key).
    Churn,
    /// The sentinel key is deleted (consumes a revision: the marker).
    DeleteKey,
    /// The sentinel key is written again (consumes a revision).
    PutKey,
    /// Evicting retention only: the sentinel's current value ages out
    /// (FIFO-consistent: once the floor has passed its revision).
    AgeOut,
    /// Retention evicts the oldest retained revision.
    Compact,
    /// A (correct) exporter publishes an artifact at this cursor.
    Publish(u8),
    /// The consumer receives the next RETAINED revision after its frontier —
    /// evicted revisions are silently skipped, which is the hazard.
    Deliver,
    /// Guarded variant only: the floor guard finds the frontier behind the
    /// floor and the expiry repair runs (see the module docs). Free
    /// interleaving of this action is a superset of every guard cadence.
    GuardRepair,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BucketRetention {
    KeepsCurrent,
    EvictsCurrent,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    /// The pre-fix repair on an evicting bucket: the key-listing diff.
    RelistOnEvicting,
    /// The restore without `restore_allowed`: any published artifact.
    RestoreUnguarded,
}

#[derive(Clone)]
struct LiveWatch {
    guarded: bool,
    retention: BucketRetention,
    mutation: Mutation,
}

impl LiveWatch {
    /// THE TRUTH: the sentinel after every write and every real delete up to
    /// `cursor` (initial put at 0; the delete and later write in revision
    /// order).
    fn key_at(s: &St, cursor: u8) -> Option<u8> {
        let mut key = Some(0);
        let mut events = [
            s.delete_rev.map(|d| (d, None)),
            s.reput_rev.map(|r| (r, Some(r))),
        ];
        events.sort();
        for (rev, value) in events.into_iter().flatten() {
            if rev <= cursor {
                key = value;
            }
        }
        key
    }

    fn truth(s: &St) -> Option<u8> {
        Self::key_at(s, s.head)
    }

    /// What the bucket lists: the truth minus an evicted current value.
    fn listed(s: &St) -> Option<u8> {
        Self::truth(s).filter(|_| !s.aged)
    }

    fn restores(&self) -> bool {
        self.retention == BucketRetention::EvictsCurrent
            && self.mutation != Mutation::RelistOnEvicting
    }

    /// The restore this repair may take from the current pointer, if any.
    fn restore_target(&self, s: &St) -> Option<u8> {
        let p = s.pointer?;
        (self.mutation == Mutation::RestoreUnguarded
            || restore_allowed(p as u64, s.frontier as u64, s.floor as u64 + 1))
        .then_some(p)
    }
}

impl Model for LiveWatch {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        // The sentinel key exists and the fold is synced at frontier 0.
        vec![St {
            head: 0,
            floor: 0,
            delete_rev: None,
            reput_rev: None,
            aged: false,
            pointer: None,
            frontier: 0,
            fold_key: Some(0),
            tripped: false,
            restored: false,
            regressed: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < MAX_REV {
            acts.push(Act::Churn);
            if s.delete_rev.is_none() {
                acts.push(Act::DeleteKey);
            }
            if s.reput_rev.is_none() {
                acts.push(Act::PutKey);
            }
        }
        if self.retention == BucketRetention::EvictsCurrent
            && !s.aged
            && Self::truth(s).is_some_and(|w| w == 0 || s.floor >= w)
        {
            acts.push(Act::AgeOut);
        }
        if s.floor < s.head {
            acts.push(Act::Compact);
        }
        // Artifacts only matter where a restore can use them.
        if self.restores() {
            for c in s.pointer.map_or(1, |p| p + 1)..=s.head {
                acts.push(Act::Publish(c));
            }
        }
        // Something retained remains beyond the frontier.
        if s.frontier.max(s.floor) < s.head {
            // GUARDED: delivery never silently jumps a gap. A delivered
            // revision > frontier+1 is in-band evidence of eviction past
            // the frontier, checked AT THE DELIVERY (the kernel gate below)
            // — the checker rejected the periodic-only design with exactly
            // the catch-up-erases-the-evidence trace this gate closes.
            // UNGUARDED: the skip happens silently (JetStream's behavior).
            if !self.guarded || resume_window_ok(s.frontier as u64, s.floor as u64 + 1) {
                acts.push(Act::Deliver);
            }
        }
        // THE GUARD GATE — the shared kernel: the frontier has fallen
        // behind the first retained revision (floor + 1). Reached either by
        // the gapped-delivery check (above: Deliver disabled, this is the
        // only progress) or by the periodic backstop when no deliveries
        // arrive at all; free interleaving covers every cadence of both. A
        // restore additionally needs an artifact the guard accepts:
        // fail-stop until the fleet publishes one.
        if self.guarded
            && !resume_window_ok(s.frontier as u64, s.floor as u64 + 1)
            && (!self.restores() || self.restore_target(s).is_some())
        {
            acts.push(Act::GuardRepair);
        }
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::Churn => s.head += 1,
            Act::DeleteKey => {
                s.head += 1;
                s.delete_rev = Some(s.head);
            }
            Act::PutKey => {
                s.head += 1;
                s.reput_rev = Some(s.head);
                s.aged = false;
            }
            Act::AgeOut => s.aged = true,
            Act::Compact => s.floor += 1,
            Act::Publish(c) => s.pointer = Some(c),
            Act::Deliver => {
                // Next retained revision; anything evicted in between is
                // SKIPPED — if the skipped range held a sentinel event, the
                // fold silently misses it.
                let next = s.frontier.max(s.floor) + 1;
                if next > s.head {
                    return None;
                }
                s.frontier = next;
                if s.delete_rev == Some(next) {
                    s.fold_key = None;
                }
                if s.reput_rev == Some(next) {
                    s.fold_key = Some(next);
                }
            }
            Act::GuardRepair => {
                s.tripped = true;
                let before = s.frontier;
                if self.restores() {
                    // Artifact restore: the fold becomes the artifact's,
                    // consumption resumes from its cursor.
                    let p = self.restore_target(&s)?;
                    s.fold_key = Self::key_at(&s, p);
                    s.frontier = p;
                    s.restored = true;
                } else {
                    // Key-listing diff + re-list: the fold becomes what the
                    // bucket lists, consumption re-entitled from the floor.
                    s.fold_key = Self::listed(&s);
                    s.frontier = s.floor;
                }
                s.regressed |= s.frontier < before;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = Vec::new();

        // Vacuity witness shared by every variant.
        props.push(Property::<Self>::sometimes(
            "the marker is delivered normally and the fold drops the key",
            |_, s| s.fold_key.is_none() && LiveWatch::truth(s).is_none() && !s.tripped,
        ));

        if self.guarded {
            // Divergence is only ever stale-direction: the fold never drops
            // a value that is still current (unchanged since the fold's
            // cursor). On an evicting bucket the key-listing diff breaks
            // exactly this — the `RelistOnEvicting` mutation.
            props.push(Property::<Self>::always(
                "no phantom deletes: the fold never drops a value that is still current",
                |_, s| {
                    let truth = LiveWatch::truth(s);
                    truth.is_none()
                        || LiveWatch::key_at(s, s.frontier) != truth
                        || s.fold_key == truth
                },
            ));
            props.push(Property::<Self>::always(
                "a repair never moves the fold backward",
                |_, s| !s.regressed,
            ));
            // THE theorem: every maximal run ends with the fold equal to the
            // truth — divergence is at worst transient (a trip away), never
            // permanent. Terminal-state invariant (cycle-proof): a state
            // with no enabled actions must be converged.
            props.push(Property::<Self>::always(
                "every maximal run ends with the fold equal to every write minus every real delete",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty() || s.fold_key == LiveWatch::truth(s)
                },
            ));
            // The theorem is earned: the guard genuinely trips within the
            // bounds (the race occurs and is repaired, not avoided).
            props.push(Property::<Self>::sometimes(
                "the floor guard trips and repairs a real divergence",
                |_, s| s.tripped && s.fold_key == LiveWatch::truth(s),
            ));
            if self.restores() {
                props.push(Property::<Self>::sometimes(
                    "a trip restores from an artifact past a value that aged out",
                    |_, s| s.restored && s.aged && s.fold_key == LiveWatch::truth(s),
                ));
            }
        } else {
            // The pre-guard code, machine-checked: PERMANENT silent
            // divergence is reachable — a terminal state where the fold
            // misses an event, nothing remains to deliver, and no error
            // occurred anywhere.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: permanent silent divergence (marker evicted unseen)",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    acts.is_empty() && s.fold_key.is_some() && LiveWatch::truth(s).is_none()
                },
            ));
            // ...and in the other direction: a skipped WRITE leaves the fold
            // without a value the truth has.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: permanent silent divergence (write evicted unseen)",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    acts.is_empty()
                        && LiveWatch::truth(s).is_some()
                        && s.fold_key != LiveWatch::truth(s)
                },
            ));
        }

        props
    }
}

fn run(model: LiveWatch, label: &str) -> impl Checker<LiveWatch> {
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count(),
    );
    checker
}

fn shipped(guarded: bool, retention: BucketRetention) -> LiveWatch {
    LiveWatch {
        guarded,
        retention,
        mutation: Mutation::None,
    }
}

/// The SHIPPED behavior on a bucket that keeps current values: All-scope
/// watches carry the floor guard (`nats.rs`), gated by the same shared kernel
/// this model executes, repaired by the key-listing diff.
#[test]
fn guarded_live_watch_always_converges() {
    run(
        shipped(true, BucketRetention::KeepsCurrent),
        "live watch: guarded",
    )
    .assert_properties();
}

/// The SHIPPED behavior on a bucket that evicts current values: a trip is
/// repaired from an artifact (`restore_allowed`-gated), keeping values that
/// aged out of NATS without being deleted.
#[test]
fn guarded_live_watch_restores_on_evicting_bucket() {
    run(
        shipped(true, BucketRetention::EvictsCurrent),
        "live watch: guarded, evicting, restore",
    )
    .assert_properties();
}

/// The pre-guard behavior, kept as the machine-checked record of the hazard:
/// retention overrunning a live consumer silently and permanently diverges
/// the fold.
#[test]
fn unguarded_live_watch_diverges_permanently() {
    run(
        shipped(false, BucketRetention::KeepsCurrent),
        "live watch: unguarded",
    )
    .assert_properties();
}

/// The pre-fix repair of a floor-guard trip on an evicting bucket — the
/// key-listing diff — MUST be caught: it deletes a value that merely aged out.
#[test]
fn mutation_relist_on_evicting_trip_is_caught() {
    let checker = run(
        LiveWatch {
            mutation: Mutation::RelistOnEvicting,
            ..shipped(true, BucketRetention::EvictsCurrent)
        },
        "live watch mutation: relist on evicting",
    );
    assert!(
        checker
            .discovery("no phantom deletes: the fold never drops a value that is still current")
            .is_some(),
        "the checker must find the key-listing diff deleting an aged-out value"
    );
}

/// Without `restore_allowed`, a trip can restore an artifact older than what
/// the watch already delivered, moving the fold (and the consumer's domain
/// state) backward. The guard is load-bearing; the checker must catch it.
#[test]
fn mutation_unguarded_restore_is_caught() {
    let checker = run(
        LiveWatch {
            mutation: Mutation::RestoreUnguarded,
            ..shipped(true, BucketRetention::EvictsCurrent)
        },
        "live watch mutation: unguarded restore",
    );
    assert!(
        checker
            .discovery("a repair never moves the fold backward")
            .is_some(),
        "the checker must find a restore regressing the fold"
    );
}
