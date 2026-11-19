//! Exhaustive model-check of STREAM INCARNATION: a fold must never resume
//! into a different incarnation of its stream.
//!
//! A KV bucket can be deleted and recreated under the same name (disaster
//! recovery, an environment rebuilt from a template). The new stream numbers
//! its revisions from 1 again, while a fold's persisted cursor and an
//! artifact's embedded cursor still carry the OLD stream's numbers. A resume
//! guard that looks at sequences alone ([`resume_window_ok`]: cursor not
//! below `first_sequence - 1`) cannot tell the two apart:
//!
//! - a cursor ABOVE the new stream's head is "beyond the head": NATS delivers
//!   nothing until the new stream grows past it, silently skipping every new
//!   revision up to the cursor (observed in an AWS recovery test: cursor
//!   49,980, new stream 1..=7,373, zero events for 230 s);
//! - a cursor AT OR BELOW the new head passes the window check, and the fold
//!   silently splices the old incarnation's state under the new stream's tail.
//!
//! Modeled: the stream (head, floor, recreation), and the two holders of a
//! cursor that resume — a WARM fold (its store's persisted cursor) and an
//! ARTIFACT a fresh node bootstraps from. Each cursor records its stamp (the
//! incarnation that wrote it, if the writer was a build that stamps) and, as
//! hidden ground truth, the incarnation its position really belongs to. A
//! resume is ADMITTED or REFUSED by the guard under test; a refused fold
//! re-seeds (the repair paths, or a loud error) and never resumes.
//!
//! The invariant: no admitted resume has `truth != the stream's incarnation`.
//!
//! Unstamped cursors (written before stamping existed: `legacy`) carry no
//! evidence of their incarnation. [`LegacyPolicy::Admit`] is what ships: a
//! fleet upgraded in place must keep restarting, so an unstamped cursor is
//! admitted when its position is possible in the current stream (not above
//! the head). That leaves ONE reachable hole — an unstamped cursor from the
//! old incarnation, within the new stream's range — which the checker must
//! derive (`sometimes`), and which closes as soon as a fold writes a stamped
//! cursor. [`LegacyPolicy::Refuse`] closes it entirely and the same
//! invariant then holds for every cursor.

use stateright::{Checker, Model, Property};

const IN_RANGE: &str =
    "no fold resumes into a different incarnation whose head already passed the old cursor";
const INVARIANT: &str = "no fold resumes into a different incarnation";
const MAX_REV: u8 = 3;
const MAX_INC: u8 = 2;

/// A resume position: what the writer recorded, and what is really true.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Cursor {
    /// The incarnation the writing build stamped. `None`: written before
    /// stamping existed.
    stamp: Option<u8>,
    rev: u8,
    /// Ground truth, invisible to the guard: the incarnation `rev` belongs to.
    truth: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct St {
    /// The stream's current incarnation.
    inc: u8,
    /// Last sequence of the current incarnation.
    head: u8,
    /// Retention floor: first retained sequence is `floor + 1`.
    floor: u8,
    /// The warm fold's persisted cursor.
    fold: Option<Cursor>,
    /// The latest artifact's embedded cursor.
    artifact: Option<Cursor>,
    /// Latched: a resume was admitted whose cursor belonged to another
    /// incarnation, and it was STAMPED (so the guard had the evidence).
    bad_stamped: bool,
    /// Same, for an unstamped cursor.
    bad_legacy: bool,
    /// Latched with either of the above when the foreign cursor was not above
    /// the new stream's head: the case a sequence-only check can never catch.
    bad_in_range: bool,
    /// Witnesses.
    refused_warm_in_range: bool,
    refused_artifact: bool,
    resumed_same: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Act {
    Churn,
    Compact,
    /// The bucket is deleted and recreated under the same name.
    Recreate,
    /// A cursorless fold starts a full watch: stamped cursor at the head.
    Seed,
    /// The fold exports its cursor as the latest artifact.
    Publish,
    /// The warm fold restarts and resumes from its persisted cursor.
    ResumeWarm,
    /// A fresh node bootstraps from the latest artifact and resumes.
    Bootstrap,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Guard {
    /// The guard before this change: sequence window only.
    SequenceOnly,
    /// The shipped guard: window plus stream incarnation.
    Incarnation,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LegacyPolicy {
    Admit,
    Refuse,
}

#[derive(Clone)]
struct Incarnations {
    guard: Guard,
    legacy: LegacyPolicy,
}

impl Incarnations {
    /// May a resume from `c` proceed? The guard under test.
    fn admit(&self, s: &St, c: Cursor) -> bool {
        let first = s.floor as u64 + 1;
        let window = slipstream::protocol::resume_window_ok(c.rev as u64, first);
        match self.guard {
            Guard::SequenceOnly => window,
            Guard::Incarnation => {
                // The shipped guard: the kernel `nats.rs` executes. Origins
                // are never 0 in code, so the model's incarnation `i` is
                // `i + 1`.
                let origin = |i: u8| i as u64 + 1;
                let stream_origin = origin(s.inc);
                let same = match (c.stamp, self.legacy) {
                    (None, LegacyPolicy::Refuse) => false,
                    (stamp, _) => slipstream::protocol::same_incarnation(
                        stamp.map(origin),
                        c.rev as u64,
                        stream_origin,
                        s.head as u64,
                    ),
                };
                window && same
            }
        }
    }

    /// Run a resume of `c`; the resulting fold cursor, or `None` when refused
    /// (re-seed).
    fn resume(&self, s: &mut St, c: Cursor) -> Option<Cursor> {
        if self.admit(s, c) {
            if c.truth != s.inc {
                s.bad_in_range |= c.rev <= s.head;
                match c.stamp {
                    Some(_) => s.bad_stamped = true,
                    None => s.bad_legacy = true,
                }
            } else {
                s.resumed_same = true;
            }
            Some(Cursor {
                stamp: Some(s.inc),
                rev: s.head,
                truth: s.inc,
            })
        } else {
            None
        }
    }
}

impl Model for Incarnations {
    type State = St;
    type Action = Act;

    fn init_states(&self) -> Vec<St> {
        // Folds and artifacts that predate stamping: unstamped cursors of
        // incarnation 0, at any revision up to the head they were taken at.
        let legacy = |r: u8| Cursor {
            stamp: None,
            rev: r,
            truth: 0,
        };
        let mut out = Vec::new();
        for f in 0..=MAX_REV {
            for a in 0..=MAX_REV {
                out.push(St {
                    inc: 0,
                    head: f.max(a).max(1),
                    floor: 0,
                    fold: (f > 0).then(|| legacy(f)),
                    artifact: (a > 0).then(|| legacy(a)),
                    bad_stamped: false,
                    bad_legacy: false,
                    bad_in_range: false,
                    refused_warm_in_range: false,
                    refused_artifact: false,
                    resumed_same: false,
                });
            }
        }
        out
    }

    fn actions(&self, s: &St, acts: &mut Vec<Act>) {
        if s.head < MAX_REV {
            acts.push(Act::Churn);
        }
        if s.floor < s.head {
            acts.push(Act::Compact);
        }
        if s.inc < MAX_INC {
            acts.push(Act::Recreate);
        }
        if s.fold.is_none() {
            acts.push(Act::Seed);
        }
        if s.fold.is_some() {
            acts.push(Act::Publish);
            acts.push(Act::ResumeWarm);
        }
        if s.artifact.is_some() {
            acts.push(Act::Bootstrap);
        }
    }

    fn next_state(&self, s: &St, a: Act) -> Option<St> {
        let mut s = s.clone();
        match a {
            Act::Churn => s.head += 1,
            Act::Compact => s.floor += 1,
            Act::Recreate => {
                s.inc += 1;
                s.head = 0;
                s.floor = 0;
            }
            Act::Seed => {
                s.fold = Some(Cursor {
                    stamp: Some(s.inc),
                    rev: s.head,
                    truth: s.inc,
                });
            }
            Act::Publish => s.artifact = s.fold,
            Act::ResumeWarm => {
                let c = s.fold?;
                let in_range = c.rev <= s.head;
                s.fold = self.resume(&mut s, c);
                if s.fold.is_none() && c.stamp.is_some() && c.truth != s.inc && in_range {
                    s.refused_warm_in_range = true;
                }
            }
            Act::Bootstrap => {
                let c = s.artifact?;
                s.fold = self.resume(&mut s, c);
                if s.fold.is_none() && c.stamp.is_some() && c.truth != s.inc {
                    s.refused_artifact = true;
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props: Vec<Property<Self>> = Vec::new();
        match (self.guard, self.legacy) {
            (Guard::SequenceOnly, _) => {
                // The same invariant; the pre-fix guard must violate it.
                props.push(Property::<Self>::always(INVARIANT, |_, s| {
                    !s.bad_stamped && !s.bad_legacy
                }));
                props.push(Property::<Self>::always(IN_RANGE, |_, s| !s.bad_in_range));
            }
            (Guard::Incarnation, LegacyPolicy::Refuse) => {
                props.push(Property::<Self>::always(INVARIANT, |_, s| {
                    !s.bad_stamped && !s.bad_legacy
                }));
            }
            (Guard::Incarnation, LegacyPolicy::Admit) => {
                props.push(Property::<Self>::always(
                    "no stamped fold resumes into a different incarnation",
                    |_, s| !s.bad_stamped,
                ));
                props.push(Property::<Self>::sometimes(
                    "LEGACY hole reachable: an unstamped old-incarnation cursor within the new range",
                    |_, s| s.bad_legacy,
                ));
            }
        }
        // Vacuity witnesses: the scenarios the invariant quantifies over occur,
        // and the guard does not block the normal case.
        if self.guard == Guard::Incarnation {
            props.push(Property::<Self>::sometimes(
                "a stamped warm cursor from an older incarnation, inside the new range, is refused",
                |_, s| s.refused_warm_in_range,
            ));
            props.push(Property::<Self>::sometimes(
                "a stamped artifact cursor from an older incarnation is refused",
                |_, s| s.refused_artifact,
            ));
            props.push(Property::<Self>::sometimes(
                "a cursor of the current incarnation resumes",
                |_, s| s.resumed_same,
            ));
        }
        props
    }
}

fn check(guard: Guard, legacy: LegacyPolicy, label: &str) {
    let m = Incarnations { guard, legacy };
    let checker = m.checker().spawn_bfs().join();
    println!(
        "{label}: {} states, {} unique",
        checker.state_count(),
        checker.unique_state_count()
    );
    checker.assert_properties();
}

/// The pre-fix guard violates the invariant: record the counterexample.
fn counterexample(m: Incarnations, property: &'static str) -> Vec<Act> {
    let checker = m.checker().spawn_bfs().join();
    println!("violated: {property}");
    let path = checker
        .discovery(property)
        .expect("the pre-fix guard must violate the invariant");
    println!(
        "counterexample (init: {:?}):",
        path.clone().into_states()[0]
    );
    let path_actions = path.into_actions();
    for a in &path_actions {
        println!("  {a:?}");
    }
    path_actions
}

/// The shipped guard: no stamped cursor ever resumes into another incarnation,
/// and the one unstamped hole is exactly the documented one.
#[test]
fn shipped_guard_admits_only_the_same_incarnation() {
    check(
        Guard::Incarnation,
        LegacyPolicy::Admit,
        "incarnation: shipped",
    );
}

/// The alternative that refuses unstamped cursors: the full invariant holds.
#[test]
fn refusing_unstamped_cursors_closes_the_legacy_hole() {
    check(
        Guard::Incarnation,
        LegacyPolicy::Refuse,
        "incarnation: refuse unstamped",
    );
}

#[test]
fn sequence_only_guard_resumes_into_another_incarnation() {
    for property in [INVARIANT, IN_RANGE] {
        let acts = counterexample(
            Incarnations {
                guard: Guard::SequenceOnly,
                legacy: LegacyPolicy::Admit,
            },
            property,
        );
        assert!(acts.contains(&Act::Recreate));
    }
}
