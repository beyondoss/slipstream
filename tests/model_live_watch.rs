//! Exhaustive model-check of the LIVE-WATCH retention race (Stateright) —
//! the axiom-5 boundary of `tests/model.rs`, now mechanized.
//!
//! The hazard: a live consumer's position can fall behind the stream's
//! retention floor. JetStream then silently skips evicted messages (the same
//! clamp behavior `tests/resync.rs` pins for resumes — consumers never error
//! on evicted messages, they just never see them). A skipped DELETE marker
//! leaves the fold holding a key the bucket deleted, permanently and
//! silently: the exact failure class the resume-time `check_resume_window`
//! eliminates, alive again mid-watch.
//!
//! The fix this model dictates: a periodic **floor guard** during live
//! consumption — the same shared kernel guard (`protocol::resume_window_ok`)
//! applied to the delivered frontier instead of a resume cursor. A trip
//! fails the watch; the caller's restart routes into the resume → expiry →
//! resync path whose convergence the main model already proves. The repair
//! composite here (fold := bucket truth, frontier := floor) is exactly that
//! verified subprotocol, collapsed to one transition.
//!
//! Checked, exhaustively within bounds:
//! - GUARDED: every maximal run ends with the fold equal to the bucket —
//!   divergence is at worst transient (bounded by the guard cadence), never
//!   permanent. The guard genuinely trips (witness) and markers are also
//!   delivered normally (witness), so the theorem is not vacuous.
//! - UNGUARDED (the pre-guard code, kept as the machine-checked record):
//!   permanent terminal divergence is REACHABLE — fold holds a deleted key
//!   forever with nothing left to deliver and no error anywhere.
//! - BOTH: divergence is only ever in the stale direction — the fold never
//!   drops a key the bucket still has (no phantom deletes).
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
//!   received data, not loss, and needs no guard. (The code's dense-pass
//!   fast path folds such messages without probing — sound because a
//!   delivered message was by definition retained when pushed.)
//! - **`GuardRepair` is a composite** of fail-stop → restart → resume →
//!   `CursorExpired` → resync, each verified separately (`tests/model.rs`,
//!   `tests/resync.rs`); the composition is by argument. Repair LIVENESS is
//!   conditional on the caller restarting the failed watch (standard
//!   supervision); the safety half — never folding past unexamined
//!   evidence — is unconditional in the code path itself.

use slipstream::protocol::resume_window_ok;
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
    /// The consumer's delivered frontier (max revision handed to the fold).
    frontier: u8,
    /// The fold's view of the sentinel key (bucket truth: `delete_rev` none).
    fold_has_key: bool,
    /// Latched when the floor guard tripped (witness that the guarded
    /// theorem is earned by repair, not by the race never occurring).
    tripped: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    /// A new revision lands (a put to some other key).
    Churn,
    /// The sentinel key is deleted (consumes a revision: the marker).
    DeleteKey,
    /// Retention evicts the oldest retained revision.
    Compact,
    /// The consumer receives the next RETAINED revision after its frontier —
    /// evicted revisions are silently skipped, which is the hazard.
    Deliver,
    /// Guarded variant only: the periodic floor check fires, finds the
    /// frontier behind the floor, and fail-stops; the restart's resume →
    /// expiry → resync (verified by tests/model.rs) repairs the fold. Free
    /// interleaving of this action is a superset of every periodic cadence.
    GuardRepair,
}

#[derive(Clone)]
struct LiveWatch {
    guarded: bool,
}

impl LiveWatch {
    fn bucket_has_key(s: &St) -> bool {
        s.delete_rev.is_none()
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
            frontier: 0,
            fold_has_key: true,
            tripped: false,
        }]
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < MAX_REV {
            acts.push(Act::Churn);
            if s.delete_rev.is_none() {
                acts.push(Act::DeleteKey);
            }
        }
        if s.floor < s.head {
            acts.push(Act::Compact);
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
        // arrive at all; free interleaving covers every cadence of both.
        if self.guarded && !resume_window_ok(s.frontier as u64, s.floor as u64 + 1) {
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
            Act::Compact => s.floor += 1,
            Act::Deliver => {
                // Next retained revision; anything evicted in between is
                // SKIPPED — if the skipped range held the delete marker, the
                // fold silently keeps the key.
                let next = s.frontier.max(s.floor) + 1;
                if next > s.head {
                    return None;
                }
                s.frontier = next;
                if s.delete_rev == Some(next) {
                    s.fold_has_key = false;
                }
            }
            Act::GuardRepair => {
                // Fail-stop + restart + resume(frontier) + CursorExpired +
                // resync, collapsed to its verified outcome: the fold equals
                // the bucket's live keys, and consumption is re-entitled
                // from the floor.
                s.tripped = true;
                s.fold_has_key = Self::bucket_has_key(&s);
                s.frontier = s.floor;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = Vec::new();

        // BOTH variants: divergence is only ever stale-direction — the fold
        // never drops a key the bucket still has.
        props.push(Property::<Self>::always(
            "no phantom deletes: the fold never drops a live key",
            |_, s| s.fold_has_key || !LiveWatch::bucket_has_key(s),
        ));

        // Vacuity witnesses shared by both variants.
        props.push(Property::<Self>::sometimes(
            "the marker is delivered normally and the fold drops the key",
            |_, s| !s.fold_has_key && !LiveWatch::bucket_has_key(s),
        ));

        if self.guarded {
            // THE theorem: every maximal run ends with the fold equal to the
            // bucket — divergence is at worst transient (a trip away), never
            // permanent. Terminal-state invariant (cycle-proof): a state
            // with no enabled actions must be converged.
            props.push(Property::<Self>::always(
                "every maximal run ends with the fold equal to the bucket",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    !acts.is_empty() || s.fold_has_key == LiveWatch::bucket_has_key(s)
                },
            ));
            // The theorem is earned: the guard genuinely trips within the
            // bounds (the race occurs and is repaired, not avoided).
            props.push(Property::<Self>::sometimes(
                "the floor guard trips and repairs a real divergence",
                |_, s| s.tripped && s.fold_has_key == LiveWatch::bucket_has_key(s),
            ));
        } else {
            // The pre-guard code, machine-checked: PERMANENT silent
            // divergence is reachable — a terminal state where the fold
            // holds a deleted key, nothing remains to deliver, and no error
            // occurred anywhere.
            props.push(Property::<Self>::sometimes(
                "HAZARD reachable: permanent silent divergence (marker evicted unseen)",
                |m, s| {
                    let mut acts = Vec::new();
                    m.actions(s, &mut acts);
                    acts.is_empty() && s.fold_has_key && !LiveWatch::bucket_has_key(s)
                },
            ));
        }

        props
    }
}

fn check(guarded: bool, label: &str) {
    let model = LiveWatch { guarded };
    let checker = model.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count(),
    );
    checker.assert_properties();
}

/// The SHIPPED behavior: All-scope watches carry the periodic floor guard
/// (`nats.rs`), gated by the same shared kernel this model executes.
#[test]
fn guarded_live_watch_always_converges() {
    check(true, "live watch: guarded");
}

/// The pre-guard behavior, kept as the machine-checked record of the hazard:
/// retention overrunning a live consumer silently and permanently diverges
/// the fold.
#[test]
fn unguarded_live_watch_diverges_permanently() {
    check(false, "live watch: unguarded");
}
