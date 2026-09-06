# plumtree-fsm

A pure [Plumtree][paper] state machine for epidemic broadcast. No IO, no clock. You feed it
events; it returns actions. A caller runs the actions and supplies the clock. This is the same
split `etcd/raft` and Scylla's Raft use.

Plumtree sends a message to every node over a spanning tree, and repairs the tree with lazy
gossip when a message is lost. It gives tree-cost delivery (one full message per edge) with
gossip-level resilience.

## At a glance

| | this crate (`plumtree-fsm`) | [`plumtree`][sile] (sile, 2018) |
|---|---|---|
| Type | `Plumtree<Id>` — one type parameter, the node id | `Node<S>` over a `System` trait with three associated types |
| Payload | opaque `Vec<u8>` | a generic `MessagePayload` type you choose |
| Getting actions out | `ready()` drains the whole FIFO queue at once (`etcd/raft` `Ready` style) | `poll_action()` returns one action at a time |
| Clock | an internal tick counter you advance with `tick(n)` | an internal clock you advance with `tick(duration)` |
| Membership vs liveness | separate: `membership(added, removed)` and `down`/`up` | one path: `handle_neighbor_up`/`down` |
| Tree tuning | the paper's swap on hop count, weighted by a per-peer cost (failure domain) | none |

There is an older crate, [`plumtree`][sile] (2018), built on the same idea; the right column notes
where this one differs.

## How it works

Each node keeps two sets of peers. **Eager** peers get the full message. **Lazy** peers get only
its id (an `IHAVE`). A node that is missing a message asks for it with a `GRAFT`, which also
pulls that peer into its eager set. A node that receives the same message twice removes the
duplicate link with a `PRUNE`. The eager links settle into a tree in a round or two; the lazy
links stay ready to repair it.

The tree also tunes itself to the sender. Every message carries its hop count, and so does
every `IHAVE`. A node that hears of a message from a lazy peer several hops earlier than its
eager copy arrived swaps the two links: that lazy peer is closer to the source. After a few
messages from a new source the tree is balanced around it again. A Raft leader change, for
example, costs the leader's next few messages, not a rebuild.

## Constructing a node

```rust
use plumtree_fsm::{Plumtree, Config};

// Plumtree::new(me, peers, config):
//   me     -- this node's id
//   peers  -- every other member, each with a cost: 0 for the same failure domain,
//             1 (or more) for another. Pass them shuffled; order breaks ties.
//   config -- tuning: fanout, swap threshold, GRAFT timeout, message-cache size
let me = 1u32;
let peers = [(2, 0), (3, 0), (4, 0), (5, 0), (6, 1), (7, 1)];
let mut node = Plumtree::new(me, peers, Config::default());
```

The crate picks the eager set: `fanout` peers of cost 0, plus one peer of each other cost, so
every other domain is entered once and the message spreads inside it. The rest start lazy.
Keep the lazy set to a handful of random peers per domain, not every member: a `GRAFT` goes to
the announcer that spoke first, and if every node is lazy-linked to the source that is the
source itself. The paper draws lazy peers from a small random view (HyParView) for this reason.

Cost shapes the tree in three places: the starting eager set, which announcer is grafted first
(the cheapest), and the swap: a costlier peer needs a bigger hop gain to replace an eager link,
a cheaper one a smaller gain. So a cross-domain link is kept only when it saves real hops, and
a domain is normally reached through one entry point.

`Plumtree::with_split(me, eager, lazy, config)` takes an explicit split instead, all at cost 0,
for tests and for callers that build the overlay themselves.

The id type is generic (`Plumtree<Id>` for any `Ord + Clone`): a `u32` raft id, a uuid, anything.

## Driving it: a worker

Every input — `broadcast`, `on_message`, `tick`, `membership`, `down`, `up` — changes state and
appends to one FIFO queue. You drain it with `ready()` and run the actions in order. You do not
have to drain between inputs; outputs of several inputs come out in call order.

Draining is edge-triggered. `broadcast`, `on_message` and `tick` return `true` only when they
take the queue from empty to non-empty. Use that to wake a sender fiber; the input handlers
themselves never drain. The sender must drain to empty each time — `ready()` does.

Here is a complete worker that pairs `plumtree-fsm` with a
[`bcounter`](https://crates.io/crates/bcounter) counter and a network. Two libraries, three
parts: `plumtree-fsm` spreads bytes, `bcounter` produces them and enforces the quota, and the
**lease root** — the Raft leader — refills each node's lease. The worker is the only part that
touches the network and the clock. (The tested version is the `gossip-sim` crate in the
`bcounter` repository.)

```rust,ignore
use plumtree_fsm::{Plumtree, Message, Action, Config, Cost};
use bcounter::{BCounter, Denied, Quota};

const LEASE_CHUNK: u64 = 1 << 20; // how much a node asks for at a time

// Every node runs this. Two cooperative fibers share it: the input handlers below, and the
// sender fiber at the end. (In a cooperative scheduler such as Picodata's, the two never run at
// the same instant, so a shared handle to `Node` is enough.)
struct Node {
    tree: Plumtree<NodeId>,
    usage: BCounter<NodeId>,   // this node's lease, and its view of everyone's usage
    governor: NodeId,          // the lease root: the current Raft leader
    net: ConnectionPool,
    sender_wake: Notify,       // wakes the sender fiber
}

impl Node {
    // Forward the edge: if an input just made the queue non-empty, wake the sender.
    fn wake_if(&self, edge: bool) {
        if edge {
            self.sender_wake.notify();
        }
    }

    // A write. Admit it against the local lease. If the lease is short, first ask the lease root
    // for a chunk -- a direct request to the governor, not gossip. The governor answers 0 when
    // the quota is exhausted, and then the write is denied.
    async fn on_local_write(&mut self, amount: u64) -> Result<(), Denied> {
        if self.usage.local_available() < amount {
            let got = self.net.request_lease(self.governor, LEASE_CHUNK).await;
            self.usage.grant(got);
        }
        self.usage.acquire(amount)?;
        // Spread the new usage. Only this node's slot changed, but the delta is what we gossip.
        let edge = self.tree.broadcast(encode_delta(&self.usage.delta()));
        self.wake_if(edge);
        Ok(())
    }

    // A plumtree message arrived from `peer`.
    fn on_network_message(&mut self, peer: NodeId, msg: Message<NodeId>) {
        let edge = self.tree.on_message(peer, msg);
        self.wake_if(edge);
    }

    // The periodic timer fired (say every 100 ms). One tick, and a re-broadcast of the current
    // state so that a message lost earlier is covered by this one.
    fn on_timer(&mut self) {
        let e1 = self.tree.tick(1);
        let e2 = self.tree.broadcast(encode_delta(&self.usage.delta()));
        self.wake_if(e1 || e2);
    }

    // The cluster's membership record changed (from Raft): nodes joined or left for good.
    // These only remove queued sends, so they never wake the sender.
    fn on_membership_change(&mut self, joined: &[(NodeId, Cost)], left: &[NodeId]) {
        self.tree.membership(joined, left);
    }

    // The failure detector's verdict changed: a member became unreachable, or reachable again.
    fn on_liveness_change(&mut self, down: &[NodeId], up: &[NodeId]) {
        self.tree.down(down);
        self.tree.up(up);
    }

    // The Raft leader changed, so the lease root moved. Plumtree needs nothing: its overlay is
    // not rooted. The lease layer re-roots by sending future requests to the new leader. Grants
    // already held stay valid: the governor's ledger lives in Raft state, so the new leader
    // inherits it and lends nothing twice.
    fn on_leader_change(&mut self, new_leader: NodeId) {
        self.governor = new_leader;
    }

    // The sender fiber. Woken on the edge; drains the queue to empty; runs every action. Sends
    // yield on the network here without blocking the input handlers.
    async fn sender(&mut self) {
        loop {
            self.sender_wake.wait().await;
            for action in self.tree.ready() {
                match action {
                    Action::Send(peer, msg) => self.net.send(peer, encode_msg(&msg)).await,
                    Action::Deliver(bytes) => self.usage.apply(&decode_delta(&bytes)),
                }
            }
        }
    }
}

// The lease root. Only the Raft leader runs this. It hands out leases from a `Quota` whose
// ledger of outstanding grants is kept in Raft state -- that is what makes a leader change safe.
struct Governor {
    quota: RaftQuota, // implements bcounter::Quota; keeps  Σ grants ≤ limit (+ Δ)
}

impl Governor {
    // A node ran short and asked for a chunk. Returns what was lent: at most `want`, and 0 when
    // the quota is exhausted.
    fn on_lease_request(&mut self, who: NodeId, want: u64) -> u64 {
        self.quota.grant(&who, want)
    }

    // A node returned rights it will not use, so they can be lent to a busier node.
    fn on_lease_return(&mut self, who: NodeId, unused: u64) {
        self.quota.reclaim(&who, unused);
    }
}
```

What refills the quota: a node draws a chunk from the governor when its lease runs short, and
returns unused rights so the governor can move them elsewhere. A node never spends more than its
lease, and the governor never lends more than the limit, so the cluster never exceeds the limit
— without a round trip on the write path, except the occasional chunk request.

## Membership and liveness

These are two different events, so they are two calls.

- `membership(added, removed)` — a node joined the cluster (with its cost), or left for good. A
  joined node starts lazy. A left node is forgotten. Drive this from the cluster's membership record.
- `down(peers)` / `up(peers)` — a member became unreachable, or reachable again. A down node is
  kept but set aside, so the tree routes around it. `up` (or any message from it) brings it back.
  Drive this from a failure detector.

Plumtree has no single root. Each broadcast spreads from its own source, so a leader change does
not change this overlay. A rooted tree on top (for example, to hand something down from a leader)
is a separate layer.

## Loss

`GRAFT` recovers a message the eager tree dropped, if one lazy peer announced it. If every
announcer is unreachable, that one message is lost. So do not depend on a single broadcast.
Carry state that merges (a CRDT), re-broadcast it on a timer, and merge on receipt. Then a lost
message is covered by the next broadcast, and a duplicate does no harm.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

[paper]: https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf "João Leitão, José Pereira, Luís Rodrigues, Epidemic Broadcast Trees, SRDS 2007"
[sile]: https://crates.io/crates/plumtree
