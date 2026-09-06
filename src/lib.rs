// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (c) 2026 Konstantin Osipov.

//! A pure Plumtree state machine for epidemic broadcast.
//!
//! Plumtree spreads a message to every node over a spanning tree, and repairs the tree with
//! lazy gossip when a message is lost. It gives tree-cost delivery (one full message per edge)
//! with gossip-level resilience.
//!
//! This crate is the algorithm only. It reads no clock and no socket. See [the contract](#the-
//! contract) for how you drive it, and [membership and liveness](#membership-and-liveness) for
//! what to do when the cluster changes.
//!
//! # Papers
//!
//! - The tree: João Leitão, José Pereira, Luís Rodrigues, *Epidemic Broadcast Trees*, IEEE SRDS
//!   2007 — the eager-push / lazy-pull design and the `GRAFT`/`PRUNE` repair.
//! - Membership: João Leitão, José Pereira, Luís Rodrigues, *HyParView*, IEEE/IFIP DSN 2007 —
//!   the peer-sampling service Plumtree was designed to sit on. This crate does **not**
//!   implement it; you supply membership (from Raft, from a catalog) through
//!   [`membership`](Plumtree::membership).
//! - The driving contract: the single outbound queue is modelled on `etcd/raft`'s `Ready` and on
//!   Scylla's Raft `get_output()` — inputs mutate state and enqueue outputs; the caller drains.
//!
//! # The contract
//!
//! Every input — [`broadcast`](Plumtree::broadcast), [`on_message`](Plumtree::on_message),
//! [`tick`](Plumtree::tick), [`membership`](Plumtree::membership), [`down`](Plumtree::down),
//! [`up`](Plumtree::up) — mutates state and **appends to one FIFO outbound queue**. You drain
//! the queue with [`ready`](Plumtree::ready) and run the [`Action`]s in order.
//!
//! You do **not** have to drain between inputs. Call several inputs, then drain once; their
//! outputs concatenate in call order. No input reads the queue, so nothing depends on when you
//! process it. That is the whole independence rule: inputs are order-independent of output
//! handling, and outputs are run in FIFO order.
//!
//! Draining is **edge-triggered**. The three inputs that can produce output — `broadcast`,
//! `on_message`, `tick` — return `true` only when they took the queue from empty to non-empty.
//! That is the moment to wake whoever drains. A pure state machine cannot call you; it hands
//! you the edge instead, and you turn it into a wake-up for a sender fiber. `membership`,
//! `down` and `up` only ever remove queued sends, so they cannot wake anyone. The one rule an
//! edge-triggered wake imposes: the drainer must always drain to empty — which `ready` does.
//!
//! An [`Action`] is either `Send(peer, message)` — serialize and send it — or `Deliver(payload)`
//! — hand it to the application. Pair this with `bcounter`: on `Deliver`, decode the bytes and
//! call `apply`; to originate, call `broadcast` with an encoded `delta`. Neither library knows
//! about the other.
//!
//! # Membership and liveness
//!
//! Two different things change the peer set, and they are separate calls:
//!
//! - [`membership`](Plumtree::membership) — a node **joined** the cluster or **left for good**. A
//!   joined node starts lazy and is grafted into the tree by its first message. A left node is
//!   forgotten entirely. Drive this from the cluster's membership record (from Raft).
//! - [`down`](Plumtree::down) / [`up`](Plumtree::up) — a member became **unreachable** or
//!   **reachable** again. A down node is set aside, so the tree routes around it, but it is kept
//!   and returns on `up` (or as soon as a message from it arrives, which proves it is
//!   reachable). Drive this from a failure detector.
//!
//! The two are not the same: removal expels a node; down only sets it aside while it is
//! unreachable. In both cases, any queued `Send` to an excluded node is dropped, so you never
//! send to a peer that is gone or unreachable.
//!
//! # The root, and the shape of the tree
//!
//! Plumtree has **no single root**. Each broadcast spreads from its own source over the shared
//! eager/lazy mesh. The mesh does tune itself to whoever is sending: every message carries its
//! hop count, lazy announcements carry it too, and a node that hears of a message from a lazy
//! peer [`Config::swap_threshold`] hops earlier than its eager copy arrived makes that peer
//! eager and the old link lazy. After a few messages from a new source the tree is balanced
//! around it again, at about `log N` depth. So a Raft leader change costs the leader's next few
//! messages, not a rebuild.
//!
//! Each peer has a [`Cost`], chosen by the caller: 0 for the same failure domain, more for
//! another. The tree starts with [`Config::fanout`] cheap eager peers and one eager peer per
//! other cost, grafts the cheapest announcer first, and needs a larger hop gain to swap in a
//! costlier peer. A domain is entered once and spread inside.
//!
//! A rooted, directed tree — for handing quota leases *down* from the leader — is a separate
//! layer built on top: it follows the peer that delivers the leader's messages.
//!
//! # Loss
//!
//! `GRAFT` recovers a message the eager tree dropped, as long as one lazy peer announced it. If
//! every announcer is unreachable, that one message is lost. So do not rely on any single
//! broadcast: carry CRDT state, re-broadcast it on a timer, and merge on receipt. A lost message
//! is then covered by the next broadcast and a duplicate is harmless.
//!
//! # Example
//!
//! ```
//! use plumtree_fsm::{Plumtree, Message, Action, Config};
//!
//! // Node 1 among peers 2..=5 in its own domain (cost 0) and 6 in another (cost 1). With the
//! // default fanout of 3, peers 2, 3, 4 and 6 start eager and 5 starts lazy.
//! let peers = [(2, 0), (3, 0), (4, 0), (5, 0), (6, 1)];
//! let mut n: Plumtree<u32> = Plumtree::new(1, peers, Config::default());
//! n.broadcast(b"hello".to_vec());
//! let actions = n.ready();
//! // A full push to each eager peer.
//! assert!(actions.iter().any(|a| matches!(a, Action::Send(2, Message::Gossip { .. }))));
//! assert!(actions.iter().any(|a| matches!(a, Action::Send(6, Message::Gossip { .. }))));
//! assert!(!actions.iter().any(|a| matches!(a, Action::Send(5, _))));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// A message id: the node that first broadcast it, and that node's sequence number. Unique
/// across the cluster without coordination, because a node never reuses its own sequence.
pub type MsgId<Id> = (Id, u64);

/// How expensive a peer is to talk to, in hops' worth of latency: `0` for the same failure
/// domain, and for another domain about the ratio of the latencies (10, say, for a link ten
/// times slower). A cheaper link always wins a duplicate; a costlier one needs that many
/// more hops of gain to be swapped in.
pub type Cost = u8;

/// A message between nodes. The caller serializes and sends it; on receipt it feeds it back in
/// through [`Plumtree::on_message`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<Id> {
    /// A full message on an eager link.
    Gossip {
        /// Its id.
        id: MsgId<Id>,
        /// The payload -- opaque to this crate.
        payload: Vec<u8>,
        /// How many hops from the origin. The tree is tuned on it: a node that hears of a
        /// message from a lazy peer at a much lower hop count than its eager copy arrived at
        /// swaps the two links.
        round: u16,
    },
    /// Ids a lazy peer has, with the hop count it got each at, announced so a node missing one
    /// can ask for it, and so a node with a long eager path can find a shorter one.
    Ihave(Vec<(MsgId<Id>, u16)>),
    /// Make this link eager. With an id: also send me that message, which I heard of but did
    /// not get. Without: I have everything; you are just closer to the source than my current
    /// eager peer.
    Graft(Option<MsgId<Id>>),
    /// A request to stop sending full messages on this link -- move it to lazy.
    Prune,
}

/// Something the caller must do: send a message, or deliver a received payload to the
/// application. Actions come off [`ready`](Plumtree::ready) in FIFO order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<Id> {
    /// Send `Message` to this peer.
    Send(Id, Message<Id>),
    /// Hand this payload to the application (for `bcounter`, decode it and `apply`).
    Deliver(Vec<u8>),
}

/// Tuning. Times are in whatever unit the caller's clock uses (milliseconds, say).
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// How long to wait after hearing an `Ihave` before sending a `Graft` for it. Must exceed
    /// the time a message takes to cross the tree, or lazy peers graft onto the source itself.
    pub graft_timeout: u64,
    /// The largest number of payloads to keep for serving `Graft`s. Oldest are dropped.
    pub cache_cap: usize,
    /// Eager peers to start with among the cheapest (cost 0) peers. One more is taken for each
    /// distinct higher cost, so every other domain is entered once.
    pub fanout: usize,
    /// Swap an eager link for a lazy one when the lazy peer heard the message this many hops
    /// earlier. Each unit of extra cost on the lazy peer raises the bar by one; each unit less
    /// lowers it, never below one hop.
    pub swap_threshold: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            graft_timeout: 500,
            cache_cap: 512,
            fanout: 3,
            swap_threshold: 2,
        }
    }
}

/// A message a node has heard of (via `Ihave`) but does not yet have.
struct Missing<Id> {
    /// Peers that announced it, with the hop count each got it at and the tick from which it
    /// may be asked; cheapest first, tried in turn. A costly announcer waits its cost longer:
    /// the cheap path may still deliver.
    announcers: VecDeque<(Id, u16, u64)>,
}

/// A message this node has.
struct Cached<Id> {
    payload: Vec<u8>,
    /// The hop count it arrived at.
    round: u16,
    /// The eager peer it came from; `None` if this node broadcast it.
    from: Option<Id>,
}

/// One node's Plumtree state.
///
/// Generic over the node id `Id`. Feed it events; drain [`ready`](Plumtree::ready)
/// and run the [`Action`]s. See the crate docs for the contract.
pub struct Plumtree<Id: Ord + Clone> {
    me: Id,
    eager: BTreeSet<Id>,
    lazy: BTreeSet<Id>,
    /// Members currently unreachable: kept as known peers, but excluded from the tree so it
    /// routes around them. Distinct from a removed peer, which is forgotten entirely.
    down: BTreeSet<Id>,
    /// What each known peer costs to talk to. Unknown peers cost 0.
    cost: BTreeMap<Id, Cost>,
    seq: u64,
    /// The internal tick counter, advanced only by [`tick`](Plumtree::tick).
    now: u64,
    cache: BTreeMap<MsgId<Id>, Cached<Id>>,
    cache_order: VecDeque<MsgId<Id>>,
    missing: BTreeMap<MsgId<Id>, Missing<Id>>,
    /// Ids to announce to lazy peers, collected until the next [`tick`](Plumtree::tick).
    lazy_announce: BTreeMap<MsgId<Id>, u16>,
    /// The one FIFO queue of actions the caller drains.
    outbound: Vec<Action<Id>>,
    cfg: Config,
}

impl<Id: Ord + Clone> Plumtree<Id> {
    /// A new node among `peers`, each with its [`Cost`]. The crate picks the tree links: the
    /// first [`Config::fanout`] peers of cost 0, and the first peer of each distinct higher
    /// cost, start eager; the rest start lazy. The order of `peers` breaks ties, so pass them
    /// shuffled.
    pub fn new<P>(me: Id, peers: P, cfg: Config) -> Self
    where
        P: IntoIterator<Item = (Id, Cost)>,
    {
        let peers: Vec<(Id, Cost)> = peers.into_iter().filter(|(p, _)| *p != me).collect();
        let mut eager = Vec::new();
        let mut lazy = Vec::new();
        let mut cheap = 0usize;
        let mut entered = BTreeSet::new();
        for (p, c) in &peers {
            let take = if *c == 0 {
                cheap += 1;
                cheap <= cfg.fanout
            } else {
                entered.insert(*c)
            };
            if take {
                eager.push(p.clone());
            } else {
                lazy.push(p.clone());
            }
        }
        let mut node = Self::with_split(me, eager, lazy, cfg);
        node.cost = peers.into_iter().collect();
        node.introduce();
        node
    }

    /// Tell each chosen peer about us, so links are the same from both ends: a bare `Graft`
    /// to an eager peer, an empty `Ihave` to a lazy one. A node that hears from a stranger
    /// takes it as a lazy peer.
    fn introduce(&mut self) {
        for p in self.eager.clone() {
            self.outbound.push(Action::Send(p, Message::Graft(None)));
        }
        for p in self.lazy.clone() {
            self.outbound
                .push(Action::Send(p, Message::Ihave(Vec::new())));
        }
    }

    /// A new node with an explicit split of peers into eager and lazy, all at cost 0. For
    /// tests, and for callers that build the overlay themselves.
    pub fn with_split<E, L>(me: Id, eager: E, lazy: L, cfg: Config) -> Self
    where
        E: IntoIterator<Item = Id>,
        L: IntoIterator<Item = Id>,
    {
        Self {
            me,
            eager: eager.into_iter().collect(),
            lazy: lazy.into_iter().collect(),
            down: BTreeSet::new(),
            cost: BTreeMap::new(),
            seq: 0,
            now: 0,
            cache: BTreeMap::new(),
            cache_order: VecDeque::new(),
            missing: BTreeMap::new(),
            lazy_announce: BTreeMap::new(),
            outbound: Vec::new(),
            cfg,
        }
    }

    /// Drain the outbound queue: the actions accumulated since the last drain, in FIFO order.
    /// Run each in order -- `Send` over your connection pool, `Deliver` to the application.
    #[must_use]
    pub fn ready(&mut self) -> Vec<Action<Id>> {
        std::mem::take(&mut self.outbound)
    }

    /// True while the outbound queue holds actions. A level check; the inputs that can produce
    /// output (`broadcast`, `on_message`, `tick`) also return the *edge* -- see the crate docs.
    #[must_use]
    pub fn has_ready(&self) -> bool {
        !self.outbound.is_empty()
    }

    /// The edge: did this input take the queue from empty to non-empty? That is the moment to
    /// wake whoever drains `ready()`.
    fn woke(&self, was_empty: bool) -> bool {
        was_empty && !self.outbound.is_empty()
    }

    /// This node's current eager peers (its tree links). For tests and inspection.
    pub fn eager(&self) -> impl Iterator<Item = &Id> {
        self.eager.iter()
    }

    /// This node's current lazy peers. For tests and inspection.
    pub fn lazy(&self) -> impl Iterator<Item = &Id> {
        self.lazy.iter()
    }

    fn cost_of(&self, peer: &Id) -> Cost {
        self.cost.get(peer).copied().unwrap_or(0)
    }

    fn remember(&mut self, id: MsgId<Id>, payload: Vec<u8>, round: u16, from: Option<Id>) {
        let entry = Cached {
            payload,
            round,
            from,
        };
        if self.cache.insert(id.clone(), entry).is_none() {
            self.cache_order.push_back(id);
            while self.cache_order.len() > self.cfg.cache_cap {
                if let Some(old) = self.cache_order.pop_front() {
                    self.cache.remove(&old);
                }
            }
        }
    }

    /// Start spreading `payload`. Pushes a full message to each eager peer; lazy peers are told
    /// at the next [`tick`](Plumtree::tick).
    pub fn broadcast(&mut self, payload: Vec<u8>) -> bool {
        let was_empty = self.outbound.is_empty();
        let id = (self.me.clone(), self.seq);
        self.seq += 1;
        self.remember(id.clone(), payload.clone(), 0, None);
        self.spread(&id, &payload, 0, None);
        self.woke(was_empty)
    }

    /// Push `id`/`payload` to every eager peer except `except`, and queue an `Ihave` for the
    /// lazy peers.
    fn spread(&mut self, id: &MsgId<Id>, payload: &[u8], round: u16, except: Option<&Id>) {
        let sends: Vec<Id> = self
            .eager
            .iter()
            .filter(|p| Some(*p) != except)
            .cloned()
            .collect();
        for p in sends {
            self.outbound.push(Action::Send(
                p,
                Message::Gossip {
                    id: id.clone(),
                    payload: payload.to_vec(),
                    round,
                },
            ));
        }
        if !self.lazy.is_empty() {
            self.lazy_announce.insert(id.clone(), round);
        }
    }

    /// Handle a message from `from`.
    pub fn on_message(&mut self, from: Id, msg: Message<Id>) -> bool {
        let was_empty = self.outbound.is_empty();
        let known =
            self.eager.contains(&from) || self.lazy.contains(&from) || self.down.contains(&from);
        if !known && from != self.me {
            self.lazy.insert(from.clone());
        }
        match msg {
            Message::Gossip { id, payload, round } => self.on_gossip(from, id, payload, round),
            Message::Ihave(ids) => self.on_ihave(from, ids),
            Message::Graft(id) => self.on_graft(from, id),
            Message::Prune => self.move_to_lazy(&from),
        }
        self.woke(was_empty)
    }

    fn on_gossip(&mut self, from: Id, id: MsgId<Id>, payload: Vec<u8>, round: u16) {
        if let Some(c) = self.cache.get(&id) {
            // A duplicate: one of the two eager links is redundant. Drop the costlier one, and
            // between equals the one that was late.
            let via = c.from.clone();
            let cheaper = via
                .as_ref()
                .is_some_and(|v| self.cost_of(&from) < self.cost_of(v) && self.eager.contains(v));
            if cheaper {
                let v = via.expect("checked");
                if let Some(c) = self.cache.get_mut(&id) {
                    c.from = Some(from.clone());
                    c.round = round;
                }
                self.graft_in(&from);
                self.move_to_lazy(&v);
                self.outbound.push(Action::Send(v, Message::Prune));
            } else {
                self.move_to_lazy(&from);
                self.outbound.push(Action::Send(from, Message::Prune));
            }
            return;
        }
        // New. Cache it, stop waiting for it, deliver it, and pass it on.
        self.remember(id.clone(), payload.clone(), round, Some(from.clone()));
        let heard = self.missing.remove(&id);
        self.graft_in(&from);
        self.outbound.push(Action::Deliver(payload.clone()));
        self.spread(&id, &payload, round.saturating_add(1), Some(&from));
        // A lazy peer announced it much earlier: it is closer to the source. Swap links.
        if let Some(m) = heard {
            for (q, r, _) in m.announcers {
                if self.try_swap(&from, round, &q, r) {
                    break;
                }
            }
        }
    }

    fn on_ihave(&mut self, from: Id, ids: Vec<(MsgId<Id>, u16)>) {
        for (id, r) in ids {
            if let Some(c) = self.cache.get(&id) {
                // Already have it. If it came by a much longer eager path, swap.
                if let (Some(via), round) = (c.from.clone(), c.round) {
                    self.try_swap(&via, round, &from, r);
                }
                continue;
            }
            let cost = self.cost_of(&from);
            let ready = self.now + self.cfg.graft_timeout + u64::from(cost);
            let entry = self.missing.entry(id).or_insert_with(|| Missing {
                announcers: VecDeque::new(),
            });
            // Cheapest announcer first.
            let pos = entry
                .announcers
                .iter()
                .position(|(a, _, _)| self.cost.get(a).copied().unwrap_or(0) > cost)
                .unwrap_or(entry.announcers.len());
            entry.announcers.insert(pos, (from.clone(), r, ready));
        }
    }

    /// The optimisation step of the paper: `lazy` heard the message at hop `lazy_round`, our
    /// eager copy from `via` arrived at hop `round`. If the gap clears the threshold, adjusted
    /// for cost, make `lazy` eager and `via` lazy. Cost is hops' worth: a peer cheaper by 10
    /// is taken even 8 hops farther out, and a peer costlier by 10 needs 12 hops of gain.
    /// Returns whether it did.
    fn try_swap(&mut self, via: &Id, round: u16, lazy: &Id, lazy_round: u16) -> bool {
        if lazy == via || self.eager.contains(lazy) || !self.eager.contains(via) {
            return false;
        }
        let bar = i32::from(self.cfg.swap_threshold) + i32::from(self.cost_of(lazy))
            - i32::from(self.cost_of(via));
        let gain = i32::from(round) - i32::from(lazy_round);
        if gain < bar {
            return false;
        }
        self.graft_in(lazy);
        self.outbound
            .push(Action::Send(lazy.clone(), Message::Graft(None)));
        self.move_to_lazy(via);
        self.outbound
            .push(Action::Send(via.clone(), Message::Prune));
        true
    }

    fn on_graft(&mut self, from: Id, id: Option<MsgId<Id>>) {
        self.graft_in(&from);
        let Some(id) = id else { return };
        if let Some(c) = self.cache.get(&id) {
            self.outbound.push(Action::Send(
                from,
                Message::Gossip {
                    id,
                    payload: c.payload.clone(),
                    round: c.round.saturating_add(1),
                },
            ));
        }
    }

    fn graft_in(&mut self, peer: &Id) {
        if *peer != self.me {
            // A message from a peer proves it is reachable, so it also clears any `down` mark.
            self.down.remove(peer);
            self.lazy.remove(peer);
            self.eager.insert(peer.clone());
        }
    }

    /// Drop `peers` from the tree and from pending recovery, and remove any queued Send to them.
    fn exclude(&mut self, peers: &[Id]) {
        for p in peers {
            self.eager.remove(p);
            self.lazy.remove(p);
            for m in self.missing.values_mut() {
                m.announcers.retain(|(a, _, _)| a != p);
            }
        }
        self.outbound.retain(|a| match a {
            Action::Send(peer, _) => !peers.contains(peer),
            Action::Deliver(_) => true,
        });
    }

    fn move_to_lazy(&mut self, peer: &Id) {
        if self.eager.remove(peer) {
            self.lazy.insert(peer.clone());
        }
    }

    /// Advance the internal clock by `ticks`, then act on the new time. Flushes queued `Ihave`
    /// announcements to lazy peers, and sends a `Graft` for any message still missing past its
    /// deadline (trying the next announcer, and re-arming). Call it on a timer -- once per timer
    /// fire, or with a larger `ticks` to fast-forward. This is the only input that moves time; a
    /// tick is whatever unit you choose, and [`Config::graft_timeout`] counts in the same unit.
    pub fn tick(&mut self, ticks: u64) -> bool {
        let was_empty = self.outbound.is_empty();
        self.now = self.now.saturating_add(ticks);
        let now = self.now;
        // Announce everything heard since the last tick to every lazy peer.
        if !self.lazy_announce.is_empty() && !self.lazy.is_empty() {
            let ids: Vec<(MsgId<Id>, u16)> = self
                .lazy_announce
                .iter()
                .map(|(id, r)| (id.clone(), *r))
                .collect();
            for p in &self.lazy {
                self.outbound
                    .push(Action::Send(p.clone(), Message::Ihave(ids.clone())));
            }
        }
        self.lazy_announce.clear();

        // Graft anything still missing from the first announcer whose time has come; give up
        // on a message nobody is left to ask for -- a later broadcast will heal it.
        let ids: Vec<MsgId<Id>> = self.missing.keys().cloned().collect();
        let timeout = self.cfg.graft_timeout;
        for id in ids {
            let m = self.missing.get_mut(&id).expect("id came from missing");
            let Some(pos) = m.announcers.iter().position(|(_, _, t)| *t <= now) else {
                if m.announcers.is_empty() {
                    self.missing.remove(&id);
                }
                continue;
            };
            let (next, _, _) = m.announcers.remove(pos).expect("position found");
            // Whoever is left waits a full timeout more before being asked.
            for a in m.announcers.iter_mut() {
                a.2 = a.2.max(now + timeout);
            }
            self.outbound
                .push(Action::Send(next, Message::Graft(Some(id))));
        }
        self.woke(was_empty)
    }

    /// Change the cluster's membership: `added` nodes have joined, each with its cost, and
    /// `removed` nodes have left for good. A joined node is only *known* here (its cost is
    /// recorded); it becomes a peer when it introduces itself, which a new node does to the
    /// peers it was constructed with. A removed node is **forgotten entirely** -- dropped
    /// from every set and from any pending recovery, and any queued `Send` to it is removed.
    ///
    /// This is not the same as [`down`](Plumtree::down)/[`up`](Plumtree::up): removal expels a
    /// node, while down only sets it aside while it is unreachable.
    pub fn membership(&mut self, added: &[(Id, Cost)], removed: &[Id]) {
        for (p, c) in added {
            if *p != self.me {
                self.cost.insert(p.clone(), *c);
            }
        }
        if !removed.is_empty() {
            self.exclude(removed);
            for p in removed {
                self.down.remove(p);
                self.cost.remove(p);
            }
        }
    }

    /// Mark `peers` unreachable. They stay members but are set aside: excluded from the tree so
    /// it routes around them, with any queued `Send` to them dropped. Use this when a failure
    /// detector reports a node down. A later [`up`](Plumtree::up) restores them; so does any
    /// message received from one, which proves it is reachable again.
    pub fn down(&mut self, peers: &[Id]) {
        self.exclude(peers);
        for p in peers {
            if *p != self.me {
                self.down.insert(p.clone());
            }
        }
    }

    /// Mark `peers` reachable again after a [`down`](Plumtree::down). They return as lazy peers
    /// and are pulled back into the tree by the next message. Peers that were not down are
    /// ignored.
    pub fn up(&mut self, peers: &[Id]) {
        for p in peers {
            if self.down.remove(p) {
                self.lazy.insert(p.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broadcast_pushes_to_eager_and_queues_lazy() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2, 3], [4], Config::default());
        n.broadcast(b"x".to_vec());
        let a = n.ready();
        // Full push to both eager peers, nothing to the lazy peer yet.
        assert_eq!(a.len(), 2);
        assert!(a.contains(&Action::Send(
            2,
            Message::Gossip {
                id: (1, 0),
                payload: b"x".to_vec(),
                round: 0
            }
        )));
        // The lazy peer is told on the next tick.
        n.tick(1);
        assert_eq!(
            n.ready(),
            vec![Action::Send(4, Message::Ihave(vec![((1, 0), 0)]))]
        );
    }

    #[test]
    fn outputs_of_several_inputs_concatenate_in_call_order() {
        // The contract: you need not drain between inputs.
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [], Config::default());
        n.broadcast(b"a".to_vec());
        n.broadcast(b"b".to_vec());
        let a = n.ready();
        assert_eq!(a.len(), 2);
        assert_eq!(
            a[0],
            Action::Send(
                2,
                Message::Gossip {
                    id: (1, 0),
                    payload: b"a".to_vec(),
                    round: 0
                }
            )
        );
        assert_eq!(
            a[1],
            Action::Send(
                2,
                Message::Gossip {
                    id: (1, 1),
                    payload: b"b".to_vec(),
                    round: 0
                }
            )
        );
    }

    #[test]
    fn inputs_report_the_empty_to_non_empty_edge() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [], Config::default());
        // The first broadcast takes the queue from empty to non-empty: an edge, wake the sender.
        assert!(n.broadcast(b"a".to_vec()));
        // A second lands on a non-empty queue: no edge -- the drainer will get both anyway.
        assert!(!n.broadcast(b"b".to_vec()));
        assert!(n.has_ready());
        let _ = n.ready();
        assert!(!n.has_ready());
        // Once drained, the next output is an edge again.
        assert!(n.broadcast(b"c".to_vec()));
    }

    #[test]
    fn a_new_gossip_is_delivered_and_forwarded() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [], Config::default());
        n.on_message(
            5,
            Message::Gossip {
                id: (9, 0),
                payload: b"p".to_vec(),
                round: 0,
            },
        );
        let a = n.ready();
        assert!(a.contains(&Action::Deliver(b"p".to_vec())));
        assert!(a.contains(&Action::Send(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"p".to_vec(),
                round: 1
            }
        )));
        assert!(n.eager().any(|&p| p == 5)); // sender grafted into the eager set
    }

    #[test]
    fn a_duplicate_gossip_prunes_the_link() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2, 3], [], Config::default());
        let m = Message::Gossip {
            id: (9, 0),
            payload: b"p".to_vec(),
            round: 0,
        };
        n.on_message(2, m.clone());
        let _ = n.ready();
        n.on_message(3, m); // same message again, from 3
        assert_eq!(n.ready(), vec![Action::Send(3, Message::Prune)]);
        assert!(!n.eager().any(|&p| p == 3)); // 3 moved to lazy
    }

    #[test]
    fn a_missing_message_is_grafted_after_the_timeout() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [], [2], Config::default());
        n.on_message(2, Message::Ihave(vec![((7, 0), 0)]));
        n.tick(100); // before the timeout
        assert!(n.ready().is_empty());
        n.tick(600); // after it
        assert_eq!(
            n.ready(),
            vec![Action::Send(2, Message::Graft(Some((7, 0))))]
        );
    }

    #[test]
    fn a_graft_is_answered_with_the_cached_payload() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [], Config::default());
        n.broadcast(b"p".to_vec()); // id (1,0) is now cached
        let _ = n.ready();
        n.on_message(8, Message::Graft(Some((1, 0))));
        assert_eq!(
            n.ready(),
            vec![Action::Send(
                8,
                Message::Gossip {
                    id: (1, 0),
                    payload: b"p".to_vec(),
                    round: 1
                }
            )]
        );
        assert!(n.eager().any(|&p| p == 8)); // grafted
    }

    #[test]
    fn a_departing_peer_is_dropped_and_its_queued_sends_are_scrubbed() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2, 3], [4], Config::default());
        n.broadcast(b"x".to_vec()); // queues Sends to 2 and 3
        n.membership(&[], &[3, 4]); // 3 leaves
        assert!(!n.eager().any(|&p| p == 3));
        // The queued Send to the departed peer 3 is gone; the Send to 2 stays.
        let a = n.ready();
        assert!(a.iter().all(|x| !matches!(x, Action::Send(3, _))));
        assert!(a.iter().any(|x| matches!(x, Action::Send(2, _))));
    }

    #[test]
    fn a_down_peer_is_routed_around_then_restored_by_up() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2, 3], [4], Config::default());
        n.down(&[2]); // peer 2 unreachable
        n.broadcast(b"x".to_vec());
        let a = n.ready();
        // Nothing goes to the down peer 2; the other eager peer 3 still gets it.
        assert!(a.iter().all(|x| !matches!(x, Action::Send(2, _))));
        assert!(a.iter().any(|x| matches!(x, Action::Send(3, _))));
        assert!(!n.eager().any(|&p| p == 2));

        n.up(&[2]); // reachable again -> lazy
        n.broadcast(b"y".to_vec());
        n.tick(1);
        let a = n.ready();
        // 2 hears about messages again, now as a lazy peer (via IHAVE).
        assert!(a
            .iter()
            .any(|x| matches!(x, Action::Send(2, Message::Ihave(_)))));
    }

    #[test]
    fn a_message_from_a_down_peer_restores_it() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [], [2], Config::default());
        n.down(&[2]);
        n.on_message(
            2,
            Message::Gossip {
                id: (2, 0),
                payload: b"p".to_vec(),
                round: 0,
            },
        );
        let _ = n.ready();
        assert!(n.eager().any(|&p| p == 2)); // grafted back in, no longer down
    }

    // ---------------------------------------------------------------- cost and optimisation

    #[test]
    fn new_picks_eager_peers_by_cost_and_introduces_itself() {
        let cfg = Config {
            fanout: 2,
            ..Config::default()
        };
        let peers = [(2, 0), (3, 0), (4, 0), (5, 0), (6, 1), (7, 1), (8, 2)];
        let mut n: Plumtree<u32> = Plumtree::new(1, peers, cfg);
        // Two of the cheapest, then one per other cost.
        assert_eq!(n.eager().copied().collect::<Vec<_>>(), vec![2, 3, 6, 8]);
        assert_eq!(n.lazy().copied().collect::<Vec<_>>(), vec![4, 5, 7]);
        let a = n.ready();
        assert!(a.contains(&Action::Send(2, Message::Graft(None))));
        assert!(a.contains(&Action::Send(4, Message::Ihave(vec![]))));
        // The other end takes a stranger as lazy, and a bare graft as eager.
        let mut m: Plumtree<u32> = Plumtree::with_split(4, [], [], Config::default());
        m.on_message(1, Message::Ihave(vec![]));
        assert!(m.lazy().any(|&p| p == 1));
        m.on_message(1, Message::Graft(None));
        assert!(m.eager().any(|&p| p == 1));
    }

    #[test]
    fn a_lazy_peer_much_closer_to_the_source_replaces_the_eager_link() {
        // Eager peer 2, lazy peer 3. 3 announces the message at hop 0; 2 delivers it at hop 3.
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [3], Config::default());
        n.on_message(3, Message::Ihave(vec![((9, 0), 0)]));
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"m".to_vec(),
                round: 3,
            },
        );
        let a = n.ready();
        assert!(a.contains(&Action::Send(3, Message::Graft(None))));
        assert!(a.contains(&Action::Send(2, Message::Prune)));
        assert_eq!(n.eager().copied().collect::<Vec<_>>(), vec![3]);
        assert_eq!(n.lazy().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn the_swap_also_happens_when_the_announcement_comes_second() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [3], Config::default());
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"m".to_vec(),
                round: 3,
            },
        );
        let _ = n.ready();
        n.on_message(3, Message::Ihave(vec![((9, 0), 0)]));
        let a = n.ready();
        assert!(a.contains(&Action::Send(3, Message::Graft(None))));
        assert!(a.contains(&Action::Send(2, Message::Prune)));
    }

    #[test]
    fn a_gap_under_the_threshold_leaves_the_tree_alone() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [2], [3], Config::default());
        n.on_message(3, Message::Ihave(vec![((9, 0), 2)]));
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"m".to_vec(),
                round: 3,
            },
        );
        let a = n.ready();
        assert!(!a
            .iter()
            .any(|x| matches!(x, Action::Send(_, Message::Prune))));
        assert_eq!(n.eager().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn a_costlier_announcer_needs_a_bigger_gap() {
        // 3 is in another domain (cost 1): a gap of 2 is not enough, 3 is.
        let mut n: Plumtree<u32> = Plumtree::new(1, [(2, 0), (3, 1)], Config::default());
        n.on_message(3, Message::Prune); // make 3 lazy for the test
        n.on_message(3, Message::Ihave(vec![((9, 0), 1)]));
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"m".to_vec(),
                round: 3,
            },
        );
        let _ = n.ready();
        assert!(
            n.eager().any(|&p| p == 2),
            "gap 2 must not swap to a cost-1 peer"
        );
        n.on_message(3, Message::Ihave(vec![((9, 1), 0)]));
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 1),
                payload: b"m".to_vec(),
                round: 3,
            },
        );
        let a = n.ready();
        assert!(
            a.contains(&Action::Send(3, Message::Graft(None))),
            "gap 3 swaps"
        );
    }

    #[test]
    fn a_duplicate_over_a_cheaper_link_prunes_the_costly_one_instead() {
        // 2 is across a slow link (cost 10), 3 is local. The copy from 2 arrives first.
        let mut n: Plumtree<u32> = Plumtree::new(1, [(2, 10), (3, 0)], Config::default());
        let m = |id| Message::Gossip {
            id,
            payload: b"m".to_vec(),
            round: 1,
        };
        n.on_message(2, m((9, 0)));
        let _ = n.ready();
        n.on_message(3, m((9, 0)));
        let a = n.ready();
        assert_eq!(
            a,
            vec![Action::Send(2, Message::Prune)],
            "the slow link goes"
        );
        assert!(n.eager().any(|&p| p == 3));
        assert!(n.lazy().any(|&p| p == 2));
    }

    #[test]
    fn a_cheaper_announcer_is_taken_even_on_a_longer_path() {
        // The eager copy came over a slow link (cost 10) at hop 1; a local lazy peer (cost 0)
        // announces it at hop 6. Five hops farther, ten hops cheaper: swap.
        let mut n: Plumtree<u32> = Plumtree::new(1, [(2, 10), (3, 0)], Config::default());
        n.on_message(3, Message::Prune);
        n.on_message(
            2,
            Message::Gossip {
                id: (9, 0),
                payload: b"m".to_vec(),
                round: 1,
            },
        );
        let _ = n.ready();
        n.on_message(3, Message::Ihave(vec![((9, 0), 6)]));
        let a = n.ready();
        assert!(a.contains(&Action::Send(3, Message::Graft(None))));
        assert!(a.contains(&Action::Send(2, Message::Prune)));
    }

    #[test]
    fn a_costly_announcer_is_asked_its_cost_later() {
        // 2 announces over a cost-10 link; 3 (local) has not announced yet. The graft to 2
        // waits graft_timeout + 10, so a local copy has time to arrive.
        let cfg = Config {
            graft_timeout: 5,
            ..Config::default()
        };
        let mut n: Plumtree<u32> = Plumtree::new(1, [(2, 10), (3, 0)], cfg);
        let _ = n.ready(); // the introductions
        n.on_message(2, Message::Prune);
        n.on_message(3, Message::Prune);
        n.on_message(2, Message::Ihave(vec![((9, 0), 0)]));
        n.tick(14);
        assert!(n.ready().is_empty(), "not yet");
        n.on_message(3, Message::Ihave(vec![((9, 0), 3)]));
        n.tick(1);
        // 3's own wait is 5 from its announcement; 2's has run out. 2 goes first here.
        assert_eq!(
            n.ready(),
            vec![Action::Send(2, Message::Graft(Some((9, 0))))]
        );
    }

    #[test]
    fn a_bare_graft_promotes_the_link_and_sends_nothing() {
        let mut n: Plumtree<u32> = Plumtree::with_split(1, [], [2], Config::default());
        n.on_message(2, Message::Graft(None));
        assert!(n.eager().any(|&p| p == 2));
        assert!(n.ready().is_empty());
    }

    #[test]
    fn the_cheapest_announcer_is_grafted_first() {
        let mut n: Plumtree<u32> = Plumtree::new(1, [(2, 1), (3, 0)], Config::default());
        n.on_message(2, Message::Prune);
        n.on_message(3, Message::Prune);
        n.on_message(2, Message::Ihave(vec![((9, 0), 0)]));
        n.on_message(3, Message::Ihave(vec![((9, 0), 0)]));
        let _ = n.ready();
        n.tick(Config::default().graft_timeout);
        let a = n.ready();
        assert_eq!(a, vec![Action::Send(3, Message::Graft(Some((9, 0))))]);
    }

    /// A tiny instant network: every node's actions are delivered in the same step.
    fn run_round(nodes: &mut BTreeMap<u32, Plumtree<u32>>) -> BTreeMap<u32, u16> {
        // Returns the hop count each node delivered the latest message at.
        let mut rounds = BTreeMap::new();
        let mut pending: Vec<(u32, u32, Message<u32>)> = Vec::new();
        for _ in 0..20 {
            for (&id, n) in nodes.iter_mut() {
                for a in n.ready() {
                    if let Action::Send(to, m) = a {
                        pending.push((id, to, m));
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
            for (from, to, m) in std::mem::take(&mut pending) {
                if let Message::Gossip { round, .. } = &m {
                    rounds.entry(to).or_insert(*round);
                }
                nodes.get_mut(&to).unwrap().on_message(from, m);
            }
            for n in nodes.values_mut() {
                n.tick(1);
            }
        }
        rounds
    }

    #[test]
    fn a_line_rebalances_around_a_source_at_its_far_end() {
        // Eager links form the line 1-2-3-...-8; every node is lazy to every other. Node 8
        // broadcasts. At first node 1 gets each message at hop 7; the swaps pull it in.
        let n = 8u32;
        let mut nodes: BTreeMap<u32, Plumtree<u32>> = (1..=n)
            .map(|i| {
                let eager: Vec<u32> = [i - 1, i + 1]
                    .into_iter()
                    .filter(|&j| (1..=n).contains(&j))
                    .collect();
                let lazy: Vec<u32> = (1..=n).filter(|&j| j != i && !eager.contains(&j)).collect();
                let cfg = Config {
                    graft_timeout: 30,
                    ..Config::default()
                };
                (i, Plumtree::with_split(i, eager, lazy, cfg))
            })
            .collect();
        let mut first = None;
        let mut last = None;
        for _ in 0..6 {
            nodes.get_mut(&n).unwrap().broadcast(b"m".to_vec());
            let rounds = run_round(&mut nodes);
            let at_one = rounds.get(&1).copied().unwrap_or(0);
            first.get_or_insert(at_one);
            last = Some(at_one);
        }
        assert_eq!(first, Some(6), "the line delivers to the far end at hop 6");
        assert!(
            last.unwrap() <= 2,
            "after a few messages node 1 is within 2 hops: {last:?}"
        );
    }
}
