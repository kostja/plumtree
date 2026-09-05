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
| Tests | multi-node convergence under message loss and cluster churn | multi-node convergence, no loss, no churn |
| Maintenance | active | last release 2018 |

There is an older crate, [`plumtree`][sile] (2018), built on the same idea; the right column notes
where this one differs.

## How it works

Each node keeps two sets of peers. **Eager** peers get the full message. **Lazy** peers get only
its id (an `IHAVE`). A node that is missing a message asks for it with a `GRAFT`, which also
pulls that peer into its eager set. A node that receives the same message twice removes the
duplicate link with a `PRUNE`. The eager links settle into a tree in a round or two; the lazy
links stay ready to repair it.

## Constructing a node

```rust
use plumtree_fsm::{Plumtree, Config};

// Plumtree::new(me, eager, lazy, config):
//   me     -- this node's id
//   eager  -- the peers that start on the tree (get full messages)
//   lazy   -- the other peers (get only message ids, and repair the tree)
//   config -- tuning (GRAFT timeout, message-cache size)
let me = 1u32;
let eager = [2, 3];   // a small set; a common choice is ceil(log2 N)+1 random peers
let lazy = [4, 5, 6]; // the rest of the members
let mut node = Plumtree::new(me, eager, lazy, Config::default());
```

The id type is generic (`Plumtree<Id>` for any `Ord + Clone`): a `u32` raft id, a uuid, anything.

## Driving it: a worker fiber

Every input — `broadcast`, `on_message`, `tick`, `membership`, `down`, `up` — changes state and
appends to one FIFO queue. You drain it with `ready()` and run the actions in order. You do not
have to drain between inputs; outputs of several inputs come out in call order.

Here is a complete worker that pairs `plumtree-fsm` with a [`bcounter`](https://crates.io/crates/bcounter)
counter and a network. `plumtree-fsm` carries opaque bytes; `bcounter` produces them. The worker
is the only part that touches the network and the clock — the two libraries do neither. (The
tested version of this is the `gossip-sim` crate in the `bcounter` repository.)

```rust,ignore
use plumtree_fsm::{Plumtree, Message, Action, Config};
use bcounter::BCounter;

struct Worker {
    tree: Plumtree<NodeId>,
    usage: BCounter<NodeId>,
    net: ConnectionPool,   // your transport (msgpack over iproto, say)
}

impl Worker {
    // Run every queued action: send a message, or apply a delivered payload to the counter.
    fn flush(&mut self) {
        for action in self.tree.ready() {
            match action {
                Action::Send(peer, msg) => self.net.send(peer, encode_msg(&msg)),
                Action::Deliver(bytes) => self.usage.apply(&decode_delta(&bytes)),
            }
        }
    }

    // A quota change happened locally (a write): record it and start spreading it.
    fn on_local_write(&mut self, amount: u64) {
        self.usage.acquire(amount).ok();
        self.tree.broadcast(encode_delta(&self.usage.delta()));
        self.flush();
    }

    // A plumtree message arrived from `peer`.
    fn on_network_message(&mut self, peer: NodeId, msg: Message<NodeId>) {
        self.tree.on_message(peer, msg);
        self.flush();
    }

    // The periodic timer fired (say every 100 ms). Advance the clock by one tick, and re-broadcast
    // current state so a message lost earlier is covered by this one.
    fn on_timer(&mut self) {
        self.tree.tick(1);
        self.tree.broadcast(encode_delta(&self.usage.delta()));
        self.flush();
    }

    // The cluster's membership record changed (from Raft): nodes joined or left for good.
    fn on_membership_change(&mut self, joined: &[NodeId], left: &[NodeId]) {
        self.tree.membership(joined, left);
        self.flush();
    }

    // The failure detector's verdict changed: a member became unreachable, or reachable again.
    fn on_liveness_change(&mut self, down: &[NodeId], up: &[NodeId]) {
        self.tree.down(down);
        self.tree.up(up);
        self.flush();
    }

    // The Raft leader changed. Plumtree needs no action: the overlay is not rooted, so a leader
    // change does not touch it. (If you run a rooted lease layer on top, that layer re-roots
    // here; plumtree does not.)
    fn on_leader_change(&mut self, _new_leader: NodeId) {}
}
```

## Membership and liveness

These are two different events, so they are two calls.

- `membership(added, removed)` — a node joined the cluster, or left for good. A joined node
  starts lazy. A left node is forgotten. Drive this from the cluster's membership record.
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
