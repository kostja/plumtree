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
//! [`tick`](Plumtree::tick), [`membership`](Plumtree::membership) — mutates state and **appends
//! to one FIFO outbound queue**. It returns nothing. You drain the queue with
//! [`ready`](Plumtree::ready) and run the [`Action`]s in order.
//!
//! You do **not** have to drain between inputs. Call several inputs, then drain once; their
//! outputs concatenate in call order. No input reads the queue, so nothing depends on when you
//! process it. That is the whole independence rule: inputs are order-independent of output
//! handling, and outputs are run in FIFO order.
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
//! # The root
//!
//! Plumtree has **no single root**. Each broadcast spreads from its own source over the shared
//! eager/lazy mesh, so a Raft leader or governor change does not touch this overlay: there is
//! nothing to re-root here, and pending actions are unaffected. A rooted, directed tree — for
//! handing quota leases *down* from the governor — is a separate layer built on top. When the
//! governor changes, that layer re-roots; the plumtree mesh does not.
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
//! // A node whose id is 1, with eager peers 2 and 3 and one lazy peer, 4.
//! let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());
//! n.broadcast(b"hello".to_vec());
//! let actions = n.ready();
//! // A full push to each eager peer.
//! assert!(actions.iter().any(|a| matches!(a, Action::Send(2, Message::Gossip { .. }))));
//! assert!(actions.iter().any(|a| matches!(a, Action::Send(3, Message::Gossip { .. }))));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// A message id: the node that first broadcast it, and that node's sequence number. Unique
/// across the cluster without coordination, because a node never reuses its own sequence.
pub type MsgId<Id> = (Id, u64);

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
        /// How many hops from the origin. Carried for tree tuning; not required for correctness.
        round: u16,
    },
    /// Ids a lazy peer has, announced so a node missing one can ask for it.
    Ihave(Vec<MsgId<Id>>),
    /// A request for a message a node heard of via `Ihave` but does not have.
    Graft(MsgId<Id>),
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
    /// How long to wait after hearing an `Ihave` before sending a `Graft` for it.
    pub graft_timeout: u64,
    /// The largest number of payloads to keep for serving `Graft`s. Oldest are dropped.
    pub cache_cap: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            graft_timeout: 500,
            cache_cap: 512,
        }
    }
}

/// A message a node has heard of (via `Ihave`) but does not yet have.
struct Missing<Id> {
    /// Peers that announced it, tried in turn.
    announcers: VecDeque<Id>,
    /// When to send the next `Graft`.
    deadline: u64,
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
    seq: u64,
    /// The internal tick counter, advanced only by [`tick`](Plumtree::tick).
    now: u64,
    cache: BTreeMap<MsgId<Id>, Vec<u8>>,
    cache_order: VecDeque<MsgId<Id>>,
    missing: BTreeMap<MsgId<Id>, Missing<Id>>,
    /// Ids to announce to lazy peers, collected until the next [`tick`](Plumtree::tick).
    lazy_announce: BTreeSet<MsgId<Id>>,
    /// The one FIFO queue of actions the caller drains.
    outbound: Vec<Action<Id>>,
    cfg: Config,
}

impl<Id: Ord + Clone> Plumtree<Id> {
    /// A new node with an initial split of peers into eager and lazy. The caller chooses the
    /// split from membership -- a common choice is `ceil(log2 N) + 1` random eager peers, the
    /// rest lazy.
    pub fn new<E, L>(me: Id, eager: E, lazy: L, cfg: Config) -> Self
    where
        E: IntoIterator<Item = Id>,
        L: IntoIterator<Item = Id>,
    {
        Self {
            me,
            eager: eager.into_iter().collect(),
            lazy: lazy.into_iter().collect(),
            down: BTreeSet::new(),
            seq: 0,
            now: 0,
            cache: BTreeMap::new(),
            cache_order: VecDeque::new(),
            missing: BTreeMap::new(),
            lazy_announce: BTreeSet::new(),
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

    /// This node's current eager peers (its tree links). For tests and inspection.
    pub fn eager(&self) -> impl Iterator<Item = &Id> {
        self.eager.iter()
    }

    fn remember(&mut self, id: MsgId<Id>, payload: Vec<u8>) {
        if self.cache.insert(id.clone(), payload).is_none() {
            self.cache_order.push_back(id);
            while self.cache_order.len() > self.cfg.cache_cap {
                if let Some(old) = self.cache_order.pop_front() {
                    self.cache.remove(&old);
                }
            }
        }
    }

    /// Start spreading `payload`. Pushes a full message to each eager peer; lazy peers are told
    /// at the next [`tick`](Plumtree::tick). `now` seeds nothing here but keeps the input API
    /// uniform.
    pub fn broadcast(&mut self, payload: Vec<u8>) {
        let id = (self.me.clone(), self.seq);
        self.seq += 1;
        self.remember(id.clone(), payload.clone());
        self.spread(&id, &payload, 0, None);
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
            self.lazy_announce.insert(id.clone());
        }
    }

    /// Handle a message from `from`.
    pub fn on_message(&mut self, from: Id, msg: Message<Id>) {
        match msg {
            Message::Gossip { id, payload, round } => self.on_gossip(from, id, payload, round),
            Message::Ihave(ids) => self.on_ihave(from, ids),
            Message::Graft(id) => self.on_graft(from, id),
            Message::Prune => self.move_to_lazy(&from),
        }
    }

    fn on_gossip(&mut self, from: Id, id: MsgId<Id>, payload: Vec<u8>, round: u16) {
        if self.cache.contains_key(&id) {
            // A duplicate: this eager link is redundant. Prune it.
            self.move_to_lazy(&from);
            self.outbound.push(Action::Send(from, Message::Prune));
            return;
        }
        // New. Cache it, stop waiting for it, deliver it, and pass it on.
        self.remember(id.clone(), payload.clone());
        self.missing.remove(&id);
        self.graft_in(&from);
        self.outbound.push(Action::Deliver(payload.clone()));
        self.spread(&id, &payload, round.saturating_add(1), Some(&from));
    }

    fn on_ihave(&mut self, from: Id, ids: Vec<MsgId<Id>>) {
        for id in ids {
            if self.cache.contains_key(&id) {
                continue;
            }
            let entry = self.missing.entry(id).or_insert_with(|| Missing {
                announcers: VecDeque::new(),
                deadline: self.now + self.cfg.graft_timeout,
            });
            entry.announcers.push_back(from.clone());
        }
    }

    fn on_graft(&mut self, from: Id, id: MsgId<Id>) {
        self.graft_in(&from);
        if let Some(payload) = self.cache.get(&id) {
            self.outbound.push(Action::Send(
                from,
                Message::Gossip {
                    id,
                    payload: payload.clone(),
                    round: 0,
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
                m.announcers.retain(|a| a != p);
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
    pub fn tick(&mut self, ticks: u64) {
        self.now = self.now.saturating_add(ticks);
        let now = self.now;
        // Announce everything heard since the last tick to every lazy peer.
        if !self.lazy_announce.is_empty() && !self.lazy.is_empty() {
            let ids: Vec<MsgId<Id>> = self.lazy_announce.iter().cloned().collect();
            for p in &self.lazy {
                self.outbound
                    .push(Action::Send(p.clone(), Message::Ihave(ids.clone())));
            }
        }
        self.lazy_announce.clear();

        // Graft anything still missing.
        let due: Vec<MsgId<Id>> = self
            .missing
            .iter()
            .filter(|(_, m)| m.deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            let m = self.missing.get_mut(&id).expect("id came from missing");
            if let Some(next) = m.announcers.pop_front() {
                m.deadline = now + self.cfg.graft_timeout;
                self.outbound.push(Action::Send(next, Message::Graft(id)));
            } else {
                // No one left to ask. Give up; a later state broadcast will heal it.
                self.missing.remove(&id);
            }
        }
    }

    /// Change the cluster's membership: `added` nodes have joined, `removed` nodes have left for
    /// good. A joined node starts lazy and is grafted into the tree by the first message it
    /// exchanges. A removed node is **forgotten entirely** -- dropped from every set and from any
    /// pending recovery, and any queued `Send` to it is removed.
    ///
    /// This is not the same as [`down`](Plumtree::down)/[`up`](Plumtree::up): removal expels a
    /// node, while down only sets it aside while it is unreachable.
    pub fn membership(&mut self, added: &[Id], removed: &[Id]) {
        for p in added {
            let known = self.eager.contains(p) || self.lazy.contains(p) || self.down.contains(p);
            if *p != self.me && !known {
                self.lazy.insert(p.clone());
            }
        }
        if !removed.is_empty() {
            self.exclude(removed);
            for p in removed {
                self.down.remove(p);
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
        let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());
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
            vec![Action::Send(4, Message::Ihave(vec![(1, 0)]))]
        );
    }

    #[test]
    fn outputs_of_several_inputs_concatenate_in_call_order() {
        // The contract: you need not drain between inputs.
        let mut n: Plumtree<u32> = Plumtree::new(1, [2], [], Config::default());
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
    fn a_new_gossip_is_delivered_and_forwarded() {
        let mut n: Plumtree<u32> = Plumtree::new(1, [2], [], Config::default());
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
        let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [], Config::default());
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
        let mut n: Plumtree<u32> = Plumtree::new(1, [], [2], Config::default());
        n.on_message(2, Message::Ihave(vec![(7, 0)]));
        n.tick(100); // before the timeout
        assert!(n.ready().is_empty());
        n.tick(600); // after it
        assert_eq!(n.ready(), vec![Action::Send(2, Message::Graft((7, 0)))]);
    }

    #[test]
    fn a_graft_is_answered_with_the_cached_payload() {
        let mut n: Plumtree<u32> = Plumtree::new(1, [2], [], Config::default());
        n.broadcast(b"p".to_vec()); // id (1,0) is now cached
        let _ = n.ready();
        n.on_message(8, Message::Graft((1, 0)));
        assert_eq!(
            n.ready(),
            vec![Action::Send(
                8,
                Message::Gossip {
                    id: (1, 0),
                    payload: b"p".to_vec(),
                    round: 0
                }
            )]
        );
        assert!(n.eager().any(|&p| p == 8)); // grafted
    }

    #[test]
    fn a_departing_peer_is_dropped_and_its_queued_sends_are_scrubbed() {
        let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());
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
        let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());
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
        let mut n: Plumtree<u32> = Plumtree::new(1, [], [2], Config::default());
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
}
