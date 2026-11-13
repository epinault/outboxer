# Choosing a setup

Outboxer has a handful of knobs that are easy to mix up. This page says what each one is for and
when to reach for it. The defaults are a good place to start: one table, one worker per outbox,
polling, every node relaying.

## The knobs at a glance

| You want | Use | Default |
| --- | --- | --- |
| Different kinds of messages handled separately | Several **outboxes** | none; you name them |
| Each outbox isolated in the database | A **`table`** per outbox | one shared table |
| One outbox delivered faster, order kept per group | More **`partitions`** | 1 |
| Events of one entity kept in sequence | A **group key** on the message | no group, no order |
| Messages picked up within milliseconds | **`notify: true`** (Postgres) | polling every second |
| Web nodes that publish, other nodes that deliver | **`relay: false`** on the web nodes | every node relays |

## Outboxes: one per destination or kind of message

An outbox is a named stream with one relay and its own settings. Split by where messages go or how
they must be treated, not by message type alone:

```elixir
outboxes: [
  orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod_client, topic: "orders"}],
  emails: [relay: {Outboxer.Relay.Oban, worker: MyApp.EmailWorker}, ordering: :none]
]
```

Several message types can share an outbox. `OrderPlaced` and `OrderShipped` belong together in
`orders`, because they must stay in order with each other.

## Tables: shared, or one per outbox

Where an outbox's rows live. It changes nothing about delivery.

- **Shared (default).** All outboxes in `outboxer_messages`. Fine for a few low-volume outboxes.
- **One per outbox.** `orders: [table: "order_outbox", ...]`. A backlog or bloat in one outbox
  does not touch the others, and you can tune, inspect or truncate each on its own.

See [Storage](storage.md#one-table-per-outbox).

## Group keys: what must stay in order

A message's group key says which other messages it must stay in sequence with. Usually it is the
entity: `"order:42"`. Messages of one group are delivered one after another; a failing message holds
back its own group only. A message without a group key is unordered.

See [Ordering and groups](ordering-and-groups.md).

## Partitions: parallel workers inside one outbox

`partitions: n` gives an outbox `n` workers. Each group is pinned to one partition by a hash of its
group key, so order within a group still holds while different groups are delivered in parallel.

- It is **not** a way to separate message types. Type plays no role in the partition; the group key
  does. Use outboxes to separate kinds of messages.
- It is **not** related to tables. A table per outbox with the default `partitions: 1` has one
  worker and a `partition` column that is always 0.
- Raise it only when the relay is the bottleneck and the outbox has many groups. Let the outbox
  drain before changing it.

## Polling and notify: how fast a message is picked up

Polling is how workers always find messages: one small indexed query per partition every
`poll_interval` (1 second by default), and immediately again after a full batch.

`notify: true` is an optional addition on Postgres. Publishing also issues `pg_notify`, Postgres
delivers it when the transaction commits, and a listener on each relaying node wakes the right
worker at once. Polling keeps running underneath, so a missed notification only costs a poll
interval, never a message. It uses one extra database connection per relaying node.

## Where the relay runs

- **Every node relays (default).** Simplest. On Postgres the nodes compete for partitions through
  the database and exactly one delivers each batch.
- **Dedicated relay nodes.** Start the same outboxes everywhere, with `relay: false` on the nodes
  that should only publish. Those nodes start no processes and need no broker settings.

See [Operations](operations.md#running-the-relay-on-its-own-nodes) for what both sides must agree on.

## Three typical setups

**Small application, one node, or a few identical nodes.** Defaults. One table, every node relays.

```elixir
{Outboxer, repo: MyApp.Repo, outboxes: [orders: [relay: ...]]}
```

**Several domains, one table each.**

```elixir
{Outboxer,
 repo: MyApp.Repo,
 outboxes: [
   orders:   [table: "order_outbox",   relay: ...],
   invoices: [table: "invoice_outbox", relay: ...]
 ]}
```

**Web pods publish, a worker pod relays, low latency, a busy outbox.**

```elixir
{Outboxer,
 repo: MyApp.Repo,
 relay: System.get_env("OUTBOXER_RELAY", "true") == "true",
 notify: true,
 outboxes: [
   orders: [table: "order_outbox", partitions: 4, relay: ...]
 ]}
```
