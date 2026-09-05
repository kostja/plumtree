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
| Clock | you pass a logical `now` into each call | an internal clock you advance with `tick(duration)` |
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

## Driving it

Every input — `broadcast`, `on_message`, `tick`, `membership`, `down`, `up` — changes state and
appends to one FIFO outbound queue. You drain the queue with `ready()` and run the
actions in order. You do not have to drain between inputs: call several, then drain once, and
their outputs are in call order.

```rust
use plumtree_fsm::{Plumtree, Message, Action, Config};

let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());

n.broadcast(0, b"hello".to_vec());
for a in n.ready() {
    match a {
        Action::Send(peer, msg) => { /* serialize msg, send to peer */ }
        Action::Deliver(payload) => { /* hand payload to the application */ }
    }
}
```

## Membership and liveness

These are two different events, so they are two calls.

- `membership(added, removed)` — a node joined the cluster, or left for good. A joined node
  starts lazy. A left node is forgotten. Drive this from the cluster's membership record.
- `down(peers)` / `up(peers)` — a member became unreachable, or reachable again. A down node is
  kept but set aside, so the tree routes around it. `up` (or any message from it) brings it back.
  Drive this from a failure detector.

For example, if a failure detector reports node 4 unreachable:

```rust
# use plumtree_fsm::{Plumtree, Config};
# let mut n: Plumtree<u32> = Plumtree::new(1, [2, 3], [4], Config::default());
n.down(&[4]);        // the tree now routes around node 4
// ... later, node 4 answers again ...
n.up(&[4]);          // node 4 rejoins the tree
```

Plumtree has no single root. Each broadcast spreads from its own source, so a leader change does
not change this overlay. If you build a rooted tree on top (for example, to hand something down
from a leader), that is a separate layer.

## Loss

`GRAFT` recovers a message the eager tree dropped, if one lazy peer announced it. If every
announcer is unreachable, that one message is lost. So do not depend on a single broadcast.
Carry state that merges (a CRDT), re-broadcast it on a timer, and merge on receipt. Then a lost
message is covered by the next broadcast, and a duplicate does no harm.

## Pairing with a CRDT counter

`plumtree-fsm` carries opaque bytes; it does not produce them. A counter such as
[`bcounter`](https://crates.io/crates/bcounter) produces a delta of its state; a worker joins the
two:

1. `counter.delta()` → encode to bytes → `plumtree.broadcast(now, bytes)`
2. run the `Send` actions over your connection pool
3. on receipt, pass the message to `plumtree.on_message`; for a `Deliver`, decode the bytes and
   `counter.apply(...)`

Neither library knows about the other.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

[paper]: https://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf "João Leitão, José Pereira, Luís Rodrigues, Epidemic Broadcast Trees, SRDS 2007"
[sile]: https://crates.io/crates/plumtree
