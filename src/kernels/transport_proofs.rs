//! The artifact transport protocol, proven for every interleaving of any number of exporters, pruners and fetchers, over ALL stream
//! incarnations, with no bound on how many objects, actors or steps there are. `tests/model.rs` checks the same protocol in a
//! bounded world (Stateright); this is its unbounded twin. The decisions are the production kernels, not copies:
//! [`pointer_publish_allowed`]'s spec (`publish_allowed_spec`) decides every publish and [`payload_prunable`]'s spec
//! (`prunable_spec`) decides every delete.
//!
//! # The world
//!
//! The object store holds immutable, content-addressed payload objects (`<incarnation>-<cursor>-<digest of the manifest>`:
//! [`Payload`]) and one pointer object, swapped by compare-and-swap (`transport::swap_pointer`; [`TS::ver`] counts successful
//! swaps, and a CAS only succeeds against the version the publisher read). An exporter runs `Upload`, then reads the pointer and
//! applies the guard (`Observe`), then swaps (`Swap`); a refused guard ends its round, a lost CAS re-reads. A pruner reads the
//! pointer once and then deletes any number of objects against that (possibly stale) read. A fetcher reads the pointer and later
//! gets the object. Every actor's read can be arbitrarily stale: that is what the interleaving quantifies over.
//!
//! # What is proven, and what the pointer of another incarnation costs (FINDING F1b)
//!
//! No order between incarnations is assumed (no clock), so a pointer of another incarnation is replaceable: a slow exporter of a
//! dead incarnation can replace the live pointer, and a stale live exporter can then replace THAT with a payload a prune already
//! deleted. The pointer then DANGLES: it names a payload that does not exist. So "the pointer's target exists" is not an
//! invariant of this world, and is not claimed. What holds, for every incarnation:
//!
//! * **(a) safety**: a fetch gets the payload its read named or a detected miss, never another payload
//!   ([`a_fetch_gets_its_payload_or_a_detected_miss`]), also in the state where the pointer dangles
//!   ([`finding_f1b_a_dangling_pointer_is_a_detected_miss`]); the pointer never regresses within an incarnation
//!   ([`the_pointer_never_regresses`]); a restore never takes a foreign artifact (`fleet_proofs`);
//! * **(b) liveness**: the window of a dangling pointer is bounded by the live exporter's round
//!   ([`a_live_round_ends_the_dangling_window`], ranking function [`rho`]): its round replaces the pointer with a payload that
//!   exists, in at most one round plus one re-read per dead round still in flight.
//!
//! # Environment axioms (the object store, NATS, clocks: nothing of slipstream's)
//!
//! * **O1** a payload path names its content for ever (content addressing; a hash collision is the only way to break it);
//! * **O2** the pointer object is written only by [`Act::Swap`] (nothing else deletes or rewrites it);
//! * **O3** a compare-and-swap succeeds exactly when the object is unchanged since the read (`ver`);
//! * **T** a round finishes before its lease lapses (the clock, and an object store that answers within the round period).
//!
//! The liveness lemma's premises ([`round_ok`]) are claims about slipstream's own code, so each is a THEOREM, not an axiom:
//!
//! * the live round runs alone: `protocol::a_held_lease_is_never_taken` (the takeover guard `ExportLease` runs) with **T**;
//! * no dead round starts after the recreation: `protocol::export_allowed` refuses the export of a fold whose cursor is not the
//!   stream's (`Fold::export`), so a node of a dead stream gives its lease back and publishes nothing; only the dead rounds
//!   already in flight at the recreation remain, finitely many ([`credits`]);
//! * the exporter is at the live head: the window ends once a live exporter's fold REACHES the head, which the fleet liveness
//!   theorem delivers (`fleet_proofs::a_converged_exporter_is_at_the_live_head`: a converged node's cursor is at least every live
//!   artifact's);
//! * its payload exists when its round starts: `Act::Upload` inserts it.

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
    /// The live stream incarnation (what the liveness theorem calls live; no safety statement mentions it).
    pub live: u64,
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
        Act::Upload(i, p) => valid(s, i) && actor(s, i) is Idle,
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

/// An actor's reads are consistent with the store (whatever the interleaving): an exporter that read the pointer holds the guard's
/// verdict, at a version no later than the pointer's, and still current while the version is.
pub open spec fn actor_ok(s: TS, a: Actor) -> bool {
    match a {
        Actor::Observed { p, st, ver } => {
            &&& publish_allowed_spec(st, pos(p))
            &&& ver <= s.ver
            &&& (ver == s.ver ==> st == pstate(s.ptr))
        },
        _ => true,
    }
}

/// THE invariant, over every incarnation: every actor's reads are consistent. (That the pointer's target exists is NOT part of it:
/// see FINDING F1b.)
pub open spec fn inv(s: TS) -> bool {
    forall|i: int| 0 <= i < s.actors.len() ==> actor_ok(s, #[trigger] s.actors[i])
}

/// THEOREM: the empty store with `n` idle actors satisfies the invariant, for any `n`.
pub proof fn a_fresh_store_satisfies_the_invariant(live: u64, actors: Seq<Actor>)
    requires
        forall|i: int| 0 <= i < actors.len() ==> #[trigger] actors[i] is Idle,
    ensures
        inv(TS { live, ptr: None, ver: 0, objs: Set::empty(), actors }),
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
                    assert(s1.actors[j] == (Actor::Observed { p: pp, st: pstate(s.ptr), ver: s.ver }));
                } else {
                    assert(s1.actors[j] == Actor::Idle);
                }
            },
            Act::Swap(i) => {
                if j != i {
                    assert(s1.actors[j] == s.actors[j]);
                } else {
                    assert(s1.actors[j] == Actor::Idle);
                }
            },
            Act::Reread(i) => {
                if j != i {
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
}

/// THEOREM (the pointer never regresses within an incarnation, for every interleaving of N exporters): every step either leaves the
/// pointer alone or moves it to a payload that is not strictly below the old one in its incarnation ([`pos_lt`]: a pointer of
/// another incarnation is replaceable, so nothing orders it), and a published pointer is never removed. This is
/// `a_publish_never_regresses_the_pointer_within_an_incarnation` lifted from one decision to the whole protocol: the guard decided
/// on a read, but the swap succeeds only against that read (O3), so the read is the pointer being replaced.
pub proof fn the_pointer_never_regresses(s: TS, a: Act)
    requires
        inv(s),
        enabled(s, a),
    ensures
        s.ptr is Some ==> step(s, a).ptr is Some && !pos_lt(pos(step(s, a).ptr->Some_0), pos(s.ptr->Some_0)),
{
    match a {
        Act::Swap(i) => {
            assert(actor_ok(s, s.actors[i]));
        },
        _ => {},
    }
}

/// THEOREM (a prune deletes only below its own read): whatever the pruner read, and however long ago, what it may delete is
/// strictly below that read in the read pointer's incarnation. (That the object is not the pointer's CURRENT target is true within
/// an incarnation and not across them: FINDING F1b.)
pub proof fn a_prune_deletes_only_below_what_it_read(s: TS, i: int, o: Payload)
    requires
        inv(s),
        enabled(s, Act::Delete(i, o)),
    ensures
        pos_lt(pos(o), actor(s, i)->Pruning_pr),
        o != actor(s, i)->Pruning_keep,
{
}

/// THEOREM (a) (a fetch gets its payload or a detected miss, never another payload): a get answers the payload its read named (a
/// path names its content, O1) or a miss. A miss means the object is gone: it was pruned. This holds in every state, the one where
/// the pointer dangles included.
pub proof fn a_fetch_gets_its_payload_or_a_detected_miss(s: TS, i: int)
    requires
        inv(s),
        enabled(s, Act::FetchGet(i)),
    ensures
        ({
            let t = actor(s, i)->Fetching_t;
            match fetched(s, t) {
                Some(x) => x == t,
                None => !s.objs.contains(t),
            }
        }),
{
}

/// THEOREM (a publisher's refusal clears): a lost CAS is never final: the exporter re-reads, and the version it lost to is a
/// publish that moved the pointer.
pub proof fn a_lost_cas_means_a_publish_happened(s: TS, i: int)
    requires
        inv(s),
        enabled(s, Act::Reread(i)),
    ensures
        actor(s, i)->Observed_ver < s.ver,
{
    assert(actor_ok(s, s.actors[i]));
}

// ---------------------------------------------------------------------------------------------------------------------
// FINDING F1b: the pointer can dangle. (a) holds there; (b) is the theorem below.
// ---------------------------------------------------------------------------------------------------------------------

/// The pointer names a payload that does not exist.
pub open spec fn dangling(s: TS) -> bool {
    s.ptr is Some && !s.objs.contains(s.ptr->Some_0)
}

/// FINDING F1b, the state: with no order between incarnations, a slow exporter of a dead incarnation `b` may replace the live
/// pointer `(a, big)` (admitted: another incarnation's), and a STALE live exporter may then replace that (admitted likewise) with a
/// payload `(a, r)` the prune guard had already deleted (it was strictly below the live pointer when pruned). Every step is a guard
/// verdict below.
pub proof fn finding_f1b_a_dead_exporter_lets_a_pruned_payload_be_published(a: u64, b: u64, r: u64, big: u64)
    requires
        a != b,
        r < big,
    ensures
        prunable_spec(Some(Pos { origin: a, rank: r }), Pos { origin: a, rank: big }, false, true),
        publish_allowed_spec(PointerState::Present { at: Some(Pos { origin: a, rank: big }) }, Pos { origin: b, rank: 1 }),
        publish_allowed_spec(PointerState::Present { at: Some(Pos { origin: b, rank: 1 }) }, Pos { origin: a, rank: r }),
{
}

/// THEOREM (a) in the F1b state: when the pointer dangles, a fetch of its target is a DETECTED miss (the object is gone), never
/// another payload: the window is loud, and a restore of it is refused by `same_incarnation` or fails to fetch, never takes wrong data.
pub proof fn finding_f1b_a_dangling_pointer_is_a_detected_miss(s: TS)
    requires
        dangling(s),
    ensures
        fetched(s, s.ptr->Some_0) is None,
{
}

// ---------------------------------------------------------------------------------------------------------------------
// (b) LIVENESS: the window of a dangling pointer ends with the live exporter's round.
// ---------------------------------------------------------------------------------------------------------------------

/// An exporter mid-round whose payload is of another incarnation than the live one.
pub open spec fn dead_flight(live: u64, a: Actor) -> bool {
    match a {
        Actor::Uploaded { p } => p.origin != live,
        Actor::Observed { p, st, ver } => p.origin != live,
        _ => false,
    }
}

/// An exporter mid-round whose payload is of the live incarnation.
pub open spec fn live_flight(live: u64, a: Actor) -> bool {
    match a {
        Actor::Uploaded { p } => p.origin == live,
        Actor::Observed { p, st, ver } => p.origin == live,
        _ => false,
    }
}

pub open spec fn flight(live: u64, a: Actor) -> nat {
    if dead_flight(live, a) { 1 } else { 0 }
}

/// The dead rounds in flight among the first `n` actors.
pub open spec fn credits_upto(actors: Seq<Actor>, live: u64, n: nat) -> nat
    decreases n,
{
    if n == 0 {
        0
    } else if n > actors.len() {
        credits_upto(actors, live, actors.len())
    } else {
        credits_upto(actors, live, (n - 1) as nat) + flight(live, actors[(n - 1) as int])
    }
}

/// The dead rounds in flight: finitely many (no round starts after the recreation: `export_allowed`), and each ends with a swap or a refused guard.
pub open spec fn credits(s: TS) -> nat {
    credits_upto(s.actors, s.live, s.actors.len())
}

/// The live exporter `i`'s distance to the end of its round: Uploaded 2, Observed 1, Observed on a stale read (it lost its CAS) 3,
/// otherwise 0.
pub open spec fn phase(s: TS, a: Actor) -> int {
    match a {
        Actor::Uploaded { p } => 2,
        Actor::Observed { p, st, ver } => if ver == s.ver { 1 } else { 3 },
        _ => 0,
    }
}

/// THE RANK of the live exporter `i`'s round: four per dead round still in flight, plus its own phase. A dead round's swap or
/// refusal lowers it by four and can make `i`'s read stale (up by two); `i`'s own steps lower it.
pub open spec fn rho(s: TS, i: int) -> int {
    4 * credits(s) + phase(s, actor(s, i))
}

pub open spec fn payload_of(a: Actor) -> Payload {
    match a {
        Actor::Uploaded { p } => p,
        Actor::Observed { p, st, ver } => p,
        _ => Payload { origin: 0, rank: 0, digest: 0 },
    }
}

/// The live exporter `i` is mid-round and alone in a live round (the lease with **T**), its payload exists (`Act::Upload`), and it is
/// at the live head (its fold is converged): no live rank in the pointer or in a pruner's read exceeds its own (each is a
/// published artifact's cursor, at most the head). The header says why each holds.
pub open spec fn round_ok(s: TS, i: int) -> bool {
    &&& valid(s, i)
    &&& live_flight(s.live, actor(s, i))
    &&& s.objs.contains(payload_of(actor(s, i)))
    &&& (s.ptr is Some && s.ptr->Some_0.origin == s.live ==> s.ptr->Some_0.rank <= payload_of(actor(s, i)).rank)
    &&& forall|j: int| 0 <= j < s.actors.len() && j != i ==> !live_flight(s.live, #[trigger] s.actors[j])
    &&& forall|j: int|
        0 <= j < s.actors.len() && #[trigger] s.actors[j] is Pruning && s.actors[j]->Pruning_pr.origin == s.live
            ==> s.actors[j]->Pruning_pr.rank <= payload_of(actor(s, i)).rank
}

/// Updating one actor changes the credits by the difference of the two actors' flights.
proof fn credits_update(actors: Seq<Actor>, live: u64, i: int, a2: Actor, n: nat)
    requires
        0 <= i < actors.len(),
    ensures
        credits_upto(actors.update(i, a2), live, n) as int == credits_upto(actors, live, n) as int
            + (if i < n { flight(live, a2) as int - flight(live, actors[i]) as int } else { 0int }),
    decreases n,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    if n == 0 {
    } else if n > actors.len() {
        credits_update(actors, live, i, a2, actors.len());
    } else {
        credits_update(actors, live, i, a2, (n - 1) as nat);
        assert(actors.update(i, a2).len() == actors.len());
        if (n - 1) as int == i {
            assert(actors.update(i, a2)[(n - 1) as int] == a2);
        } else {
            assert(actors.update(i, a2)[(n - 1) as int] == actors[(n - 1) as int]);
        }
    }
}

/// THEOREM (b) (the dangling window ends with the live exporter's round): while the live exporter `i` runs its round (no round
/// starts meanwhile: the header), every step of the world either leaves its rank [`rho`] where it was or lowers it, and every step of `i`
/// itself lowers it: its read of the pointer is admitted (the pointer is not above its payload, so the guard cannot refuse it:
/// the refusal clears), its swap succeeds against a fresh read, a lost CAS (a dead round's swap, which lowered the credits) takes
/// it back to a re-read. Its payload exists throughout (a prune never deletes it: every live read is at most its rank), so the
/// swap that ends the round leaves a pointer whose target EXISTS: the pointer no longer dangles. The window is therefore at most
/// one round plus one re-read per dead round in flight, and in it a fetch is a detected miss (a), never wrong data.
pub proof fn a_live_round_ends_the_dangling_window(s: TS, i: int, a: Act)
    requires
        inv(s),
        round_ok(s, i),
        enabled(s, a),
        !(a is Upload),
    ensures
        inv(step(s, a)),
        // `i`'s own steps lower the rank; every other step does not raise it.
        (a is Observe && a->Observe_0 == i) ==> actor(step(s, a), i) is Observed && rho(step(s, a), i) < rho(s, i),
        (a is Reread && a->Reread_0 == i) ==> live_flight(s.live, actor(step(s, a), i)) && rho(step(s, a), i) < rho(s, i),
        (a is Swap && a->Swap_0 == i) ==> !dangling(step(s, a)) && step(s, a).ptr == Some(payload_of(actor(s, i))),
        !(a is Observe && a->Observe_0 == i) && !(a is Reread && a->Reread_0 == i) && !(a is Swap && a->Swap_0 == i)
            ==> round_ok(step(s, a), i) && rho(step(s, a), i) <= rho(s, i),
{
    broadcast use vstd::seq_lib::group_seq_properties;
    every_step_preserves_the_invariant(s, a);
    let ai = actor(s, i);
    let p = payload_of(ai);
    let s1 = step(s, a);
    assert(actor_ok(s, ai));
    match a {
        Act::Observe(j) => {
            let pj = actor(s, j)->Uploaded_p;
            if j == i {
                // The pointer is not above `p` in its own incarnation, so the guard admits the candidate.
                assert(publish_allowed_spec(pstate(s.ptr), pos(p)));
                assert(s1.actors[i] == (Actor::Observed { p, st: pstate(s.ptr), ver: s.ver }));
                credits_update(s.actors, s.live, i, s1.actors[i], s.actors.len());
            } else {
                // A dead exporter reads the pointer: it stays in flight or ends its round.
                assert(!live_flight(s.live, actor(s, j)));
                assert(s1.actors[i] == s.actors[i]);
                let a2 = s1.actors[j];
                credits_update(s.actors, s.live, j, a2, s.actors.len());
                assert(flight(s.live, a2) <= flight(s.live, actor(s, j)));
                assert(s1.ptr == s.ptr && s1.objs == s.objs && s1.ver == s.ver);
            }
        },
        Act::Swap(j) => {
            if j == i {
                assert(s1.ptr == Some(p));
                assert(s1.objs == s.objs);
            } else {
                // A dead round ends: the pointer is dead, the credits fall by one, `i`'s read goes stale.
                assert(!live_flight(s.live, actor(s, j)));
                let pj = payload_of(actor(s, j));
                assert(pj.origin != s.live);
                assert(dead_flight(s.live, actor(s, j)));
                assert(s1.actors[j] == Actor::Idle);
                credits_update(s.actors, s.live, j, Actor::Idle, s.actors.len());
                assert(s1.actors[i] == s.actors[i]);
                assert(s1.ver == s.ver + 1);
                assert(s1.objs == s.objs);
                assert(s1.ptr == Some(pj));
            }
        },
        Act::Reread(j) => {
            if j == i {
                assert(ai is Observed);
                assert(actor(s1, i) == (Actor::Uploaded { p }));
                credits_update(s.actors, s.live, i, s1.actors[i], s.actors.len());
            } else {
                assert(s1.actors[i] == s.actors[i]);
                credits_update(s.actors, s.live, j, s1.actors[j], s.actors.len());
            }
        },
        Act::PruneRead(j) => {
            assert(s1.actors[i] == s.actors[i]);
            credits_update(s.actors, s.live, j, s1.actors[j], s.actors.len());
            if s.ptr->Some_0.origin == s.live {
                assert(s.ptr->Some_0.rank <= p.rank);
            }
        },
        Act::Delete(j, o) => {
            assert(actor_ok(s, s.actors[j]));
            // `o` is strictly below the pruner's read in its incarnation; every live read is at most `p`'s rank, so `o` is not `p`.
            assert(s.actors[j] is Pruning);
            if o == p {
                assert(pos_lt(pos(o), s.actors[j]->Pruning_pr));
                assert(p.origin == s.live);
            }
            assert(s1.actors == s.actors);
        },
        Act::FetchRead(j) => {
            assert(s1.actors[i] == s.actors[i]);
            credits_update(s.actors, s.live, j, s1.actors[j], s.actors.len());
        },
        Act::FetchGet(j) => {
            assert(s1.actors[i] == s.actors[i]);
            credits_update(s.actors, s.live, j, s1.actors[j], s.actors.len());
        },
        Act::Upload(j, q) => {},
    }
}

} // verus!
