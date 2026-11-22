//! The artifact transport protocol, proven for every interleaving of any number of exporters, pruners and fetchers, with no
//! bound on how many objects, actors or steps there are. `tests/model.rs` checks the same protocol in a bounded world (Stateright);
//! this is its unbounded twin. The decisions are the production kernels, not copies: [`pointer_publish_allowed`]'s spec
//! (`publish_allowed_spec`) decides every publish and [`payload_prunable`]'s spec (`prunable_spec`) decides every delete.
//!
//! # The world
//!
//! The object store holds immutable, content-addressed payload objects (`<incarnation>-<cursor>-<digest of the manifest>`: [`Payload`]) and one
//! pointer object, swapped by compare-and-swap (`transport::swap_pointer`; [`TS::ver`] counts successful swaps, and a CAS only
//! succeeds against the version the publisher read). An exporter runs `Upload`, then reads the pointer and applies the guard
//! (`Observe`), then swaps (`Swap`); a refused guard ends its round, a lost CAS re-reads. A pruner reads the pointer once and
//! then deletes any number of objects against that (possibly stale) read. A fetcher reads the pointer and later gets the object.
//! Every actor's read can be arbitrarily stale: that is what the interleaving quantifies over.
//!
//! # Environment axioms
//!
//! * **O1** a payload path names its content for ever (content addressing; a hash collision is the only way to break it);
//! * **O2** the pointer object is written only by [`Act::Swap`] (nothing else deletes or rewrites it);
//! * **O3** a compare-and-swap succeeds exactly when the object is unchanged since the read (`ver`);
//! * **O4** (scope) the pointer stays in one incarnation: every payload and the pointer are of [`TS::inc`]. Across incarnations
//!   nothing orders them (no clock is assumed): a pointer of another incarnation is replaceable (`a_pointer_of_another_incarnation_
//!   never_refuses_a_publish`, proven on the guard), and what that costs is FINDING F1b in `fleet_proofs`: a slow exporter of a dead
//!   incarnation moves the pointer out, and a stale live exporter can then publish a payload a prune already deleted. The
//!   invariants below are therefore the within-incarnation protocol, which is what every pointer swap of a live fleet runs.

use crate::kernels::protocol::*;
use vstd::prelude::*;

verus! {

/// A payload object's address: the cursor's incarnation and rank (the file name records both) and the digest of the manifest
/// (which commits to the content).
pub struct Payload {
    pub origin: u64,
    pub rank: u64,
    pub digest: nat,
}

/// The payload's place in the artifact order.
pub open spec fn pos(p: Payload) -> Pos {
    Pos { origin: p.origin, rank: p.rank }
}

/// One actor of the protocol, with what it has read so far.
pub enum Actor {
    Idle,
    /// An exporter whose payload is uploaded.
    Uploaded { p: Payload },
    /// An exporter that read the pointer (`st`, at swap-version `ver`) and the guard admitted its candidate.
    Observed { p: Payload, st: PointerState, ver: nat },
    /// A pruner that read the pointer's position `pr` (and its target `keep`).
    Pruning { pr: Pos, keep: Payload },
    /// A fetcher that read the pointer's target `t`.
    Fetching { t: Payload },
}

pub struct TS {
    /// The incarnation every payload and the pointer belong to (the scope of this proof).
    pub inc: u64,
    /// The pointer's target payload, once one is published. Its position is the target's.
    pub ptr: Option<Payload>,
    /// Successful swaps so far.
    pub ver: nat,
    /// The payload objects that exist.
    pub objs: Set<Payload>,
    pub actors: Seq<Actor>,
}

pub open spec fn pstate(ptr: Option<Payload>) -> PointerState {
    match ptr {
        None => PointerState::Absent,
        Some(p) => PointerState::Present { at: Some(pos(p)) },
    }
}

pub enum Act {
    /// Actor `i` (idle) uploads payload `p`.
    Upload(int, Payload),
    /// Actor `i` reads the pointer and applies the publish guard.
    Observe(int),
    /// Actor `i` swaps the pointer by CAS; it succeeds only if the pointer is as read.
    Swap(int),
    /// Actor `i`'s CAS lost the race: it re-reads.
    Reread(int),
    /// Actor `i` (idle) starts a prune: reads the pointer.
    PruneRead(int),
    /// Pruner `i` deletes object `o` if the prune guard allows.
    Delete(int, Payload),
    /// Actor `i` (idle) starts a fetch: reads the pointer.
    FetchRead(int),
    /// Fetcher `i` gets its object (the answer is [`fetched`]).
    FetchGet(int),
}

pub open spec fn actor(s: TS, i: int) -> Actor {
    s.actors[i]
}

pub open spec fn valid(s: TS, i: int) -> bool {
    0 <= i < s.actors.len()
}

pub open spec fn enabled(s: TS, a: Act) -> bool {
    match a {
        Act::Upload(i, p) => valid(s, i) && actor(s, i) is Idle && p.origin == s.inc,
        Act::Observe(i) => valid(s, i) && actor(s, i) is Uploaded,
        Act::Swap(i) => valid(s, i) && actor(s, i) is Observed && actor(s, i)->Observed_ver == s.ver,
        Act::Reread(i) => valid(s, i) && actor(s, i) is Observed && actor(s, i)->Observed_ver != s.ver,
        Act::PruneRead(i) => valid(s, i) && actor(s, i) is Idle && s.ptr is Some,
        Act::Delete(i, o) => {
            valid(s, i) && actor(s, i) is Pruning && s.objs.contains(o)
                && prunable_spec(Some(pos(o)), actor(s, i)->Pruning_pr, o == actor(s, i)->Pruning_keep, true)
        },
        Act::FetchRead(i) => valid(s, i) && actor(s, i) is Idle && s.ptr is Some,
        Act::FetchGet(i) => valid(s, i) && actor(s, i) is Fetching,
    }
}

pub open spec fn step(s: TS, a: Act) -> TS {
    match a {
        Act::Upload(i, p) => TS { objs: s.objs.insert(p), actors: s.actors.update(i, Actor::Uploaded { p }), ..s },
        Act::Observe(i) => {
            let p = actor(s, i)->Uploaded_p;
            let st = pstate(s.ptr);
            if publish_allowed_spec(st, pos(p)) {
                TS { actors: s.actors.update(i, Actor::Observed { p, st, ver: s.ver }), ..s }
            } else {
                // Refused (`SupersededByNewer`): the round ends, nothing changes.
                TS { actors: s.actors.update(i, Actor::Idle), ..s }
            }
        },
        Act::Swap(i) => {
            let p = actor(s, i)->Observed_p;
            TS { ptr: Some(p), ver: s.ver + 1, actors: s.actors.update(i, Actor::Idle), ..s }
        },
        Act::Reread(i) => TS { actors: s.actors.update(i, Actor::Uploaded { p: actor(s, i)->Observed_p }), ..s },
        Act::PruneRead(i) => {
            let t = s.ptr->Some_0;
            TS { actors: s.actors.update(i, Actor::Pruning { pr: pos(t), keep: t }), ..s }
        },
        Act::Delete(i, o) => TS { objs: s.objs.remove(o), ..s },
        Act::FetchRead(i) => TS { actors: s.actors.update(i, Actor::Fetching { t: s.ptr->Some_0 }), ..s },
        Act::FetchGet(i) => TS { actors: s.actors.update(i, Actor::Idle), ..s },
    }
}

/// What a fetcher's get answers: the object its read named, or a detected miss.
pub open spec fn fetched(s: TS, t: Payload) -> Option<Payload> {
    if s.objs.contains(t) { Some(t) } else { None }
}

/// An actor's reads are consistent with the store (whatever the interleaving).
pub open spec fn actor_ok(s: TS, a: Actor) -> bool {
    match a {
        Actor::Idle => true,
        // An uploaded payload exists, or was pruned because the pointer is strictly past it.
        Actor::Uploaded { p } => p.origin == s.inc && (s.objs.contains(p) || (s.ptr is Some && pos_lt(pos(p), pos(s.ptr->Some_0)))),
        Actor::Observed { p, st, ver } => {
            &&& p.origin == s.inc
            &&& publish_allowed_spec(st, pos(p))
            &&& ver <= s.ver
            // Same as `Uploaded`, whatever the version: a lost CAS goes back to `Uploaded`.
            &&& (s.objs.contains(p) || (s.ptr is Some && pos_lt(pos(p), pos(s.ptr->Some_0))))
            // The read is current while the version is: so the guard's verdict still stands.
            &&& (ver == s.ver ==> st == pstate(s.ptr))
        },
        Actor::Pruning { pr, keep } => s.ptr is Some && pr.origin == s.inc && pr.rank <= s.ptr->Some_0.rank,
        Actor::Fetching { t } => t.origin == s.inc && (s.objs.contains(t) || (s.ptr is Some && pos_lt(pos(t), pos(s.ptr->Some_0)))),
    }
}

/// THE invariant: the pointer's target exists, and every actor's reads are consistent.
pub open spec fn inv(s: TS) -> bool {
    &&& s.ptr is Some ==> s.objs.contains(s.ptr->Some_0) && s.ptr->Some_0.origin == s.inc
    &&& forall|i: int| 0 <= i < s.actors.len() ==> actor_ok(s, #[trigger] s.actors[i])
}

/// THEOREM: the empty store with `n` idle actors satisfies the invariant, for any `n`.
pub proof fn a_fresh_store_satisfies_the_invariant(inc: u64, actors: Seq<Actor>)
    requires
        forall|i: int| 0 <= i < actors.len() ==> #[trigger] actors[i] is Idle,
    ensures
        inv(TS { inc, ptr: None, ver: 0, objs: Set::empty(), actors }),
{
}

/// THEOREM (the invariant is inductive over every interleaving): any actor's any step keeps it, so it holds in every reachable
/// state, however many exporters, pruners and fetchers run and however stale their reads are.
pub proof fn every_step_preserves_the_invariant(s: TS, a: Act)
    requires
        inv(s),
        enabled(s, a),
    ensures
        inv(step(s, a)),
{
    let s1 = step(s, a);
    assert forall|j: int| 0 <= j < s1.actors.len() implies actor_ok(s1, #[trigger] s1.actors[j]) by {
        assert(s1.actors.len() == s.actors.len());
        assert(actor_ok(s, s.actors[j]));
        match a {
            Act::Upload(i, p) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
            Act::Observe(i) => {
                let pp = actor(s, i)->Uploaded_p;
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                } else if publish_allowed_spec(pstate(s.ptr), pos(pp)) {
                    assert(actor_ok(s, s.actors[i]));
                    // Admitted: the candidate is not below the pointer, so it was not pruned: it exists.
                    assert(s.objs.contains(pp));
                }
            },
            Act::Swap(i) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
            Act::Reread(i) => {
                let pp = actor(s, i)->Observed_p;
                if j == i {
                    assert(actor_ok(s, s.actors[i]));
                } else {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
            Act::PruneRead(i) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
            Act::Delete(i, o) => {
                assert(s1.actors[j] == s.actors[j]);
                let pr = actor(s, i)->Pruning_pr;
                let t = s.ptr->Some_0;
                assert(actor_ok(s, s.actors[i]));
                // `o` is strictly below the pruner's read of the pointer, which is at most the pointer's position now.
                assert(pos_lt(pos(o), pr));
                assert(pr.rank <= t.rank);
            },
            Act::FetchRead(i) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
            Act::FetchGet(i) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                }
            },
        }
    }
    if s1.ptr is Some {
        match a {
            Act::Swap(i) => {
                assert(actor_ok(s, s.actors[i]));
            },
            Act::Delete(i, o) => {
                let pr = actor(s, i)->Pruning_pr;
                assert(actor_ok(s, s.actors[i]));
                assert(pos_lt(pos(o), pr));
                assert(o != s.ptr->Some_0);
            },
            _ => {},
        }
    }
}


/// THEOREM (the pointer never regresses, for every interleaving of N exporters): every step either leaves the pointer alone or
/// moves it to a payload whose position (incarnation, then revision) is at least the old one, and a published pointer is never removed. This is
/// `a_publish_never_regresses_the_pointer` lifted from one decision to the whole protocol: the guard decided on a read, but the
/// swap succeeds only against that read (O3), so the read is the pointer being replaced.
pub proof fn the_pointer_never_regresses(s: TS, a: Act)
    requires
        inv(s),
        enabled(s, a),
    ensures
        s.ptr is Some ==> step(s, a).ptr is Some && step(s, a).ptr->Some_0.rank >= s.ptr->Some_0.rank
            && step(s, a).ptr->Some_0.origin == s.inc,
{
    match a {
        Act::Swap(i) => {
            assert(actor_ok(s, s.actors[i]));
            let p = actor(s, i)->Observed_p;
            if s.ptr is Some {
                a_publish_never_regresses_the_pointer_within_an_incarnation(pstate(s.ptr), pos(p));
            }
        },
        _ => {},
    }
}

/// THEOREM (prune never deletes the pointer's target): at any moment, whatever a pruner read earlier, the object it is allowed to
/// delete is not the pointer's current target. The pruner's read of the pointer is stale, but the pointer's position only grows, and
/// the guard deletes only strictly below the pruner's read (`the_target_and_unknown_objects_are_never_pruned` is the guard's own
/// half).
pub proof fn prune_never_deletes_the_pointers_target(s: TS, i: int, o: Payload)
    requires
        inv(s),
        enabled(s, Act::Delete(i, o)),
    ensures
        s.ptr is Some,
        s.ptr->Some_0 != o,
        inv(step(s, Act::Delete(i, o))),
        step(s, Act::Delete(i, o)).objs.contains(step(s, Act::Delete(i, o)).ptr->Some_0),
{
    assert(actor_ok(s, s.actors[i]));
    every_step_preserves_the_invariant(s, Act::Delete(i, o));
}

/// THEOREM (a fetch races a prune: the object it named, or a detected miss, never another payload): a get answers the payload
/// its read named (a path names its content, O1) or a miss; and a miss means the pointer has strictly moved on since the read, so
/// a re-read names a payload that exists and, by `prune_never_deletes_the_pointers_target`, stays until the pointer moves again.
/// (A fetch that does not race a prune never misses: the target exists.)
pub proof fn a_fetch_gets_its_payload_or_a_detected_miss(s: TS, i: int)
    requires
        inv(s),
        enabled(s, Act::FetchGet(i)),
    ensures
        ({
            let t = actor(s, i)->Fetching_t;
            match fetched(s, t) {
                Some(x) => x == t,
                None => s.ptr is Some && pos_lt(pos(t), pos(s.ptr->Some_0)) && s.objs.contains(s.ptr->Some_0),
            }
        }),
{
    assert(actor_ok(s, s.actors[i]));
}

/// THEOREM (liveness: a refused fetch retries): after a miss, a fresh read names the pointer's current target, which exists, so
/// the get answers it. The only way to miss again is for the pointer to move again, strictly upward. Rank: the pointer's position
/// (monotone), unbounded, but each miss is paid for by one publish, so a fetch retries at most once per publish. Fairness
/// assumption: the publishers do not publish for ever between a read and its get (the environment, not this code).
pub proof fn a_fresh_read_never_misses_without_a_publish(s: TS, i: int)
    requires
        inv(s),
        s.ptr is Some,
    ensures
        s.objs.contains(s.ptr->Some_0),
{
}

/// THEOREM (a publisher's refusal clears): an exporter refused by the guard (the pointer strictly newer) is refused for as long as
/// the pointer stays newer, and the pointer never regresses, so its exit is a fresher export (`a_publish_at_or_above_the_pointer_is_admitted`).
/// A lost CAS is never final: the exporter re-reads, and the version it lost to is a publish that raised the pointer.
pub proof fn a_lost_cas_means_a_publish_happened(s: TS, i: int)
    requires
        inv(s),
        enabled(s, Act::Reread(i)),
    ensures
        actor(s, i)->Observed_ver < s.ver,
{
    assert(actor_ok(s, s.actors[i]));
}

} // verus!
