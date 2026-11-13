# Ordering and groups

## Three modes

Set per outbox with `:ordering`.

| Mode | Order guaranteed | A failing message blocks | Parallelism |
| --- | --- | --- | --- |
| `:group` (default) | among messages with the same group key | its own group only | `partitions` workers |
| `:total` | across the whole outbox | everything behind it | one worker |
| `:none` | none | nothing | `partitions` workers |

In `:group` mode, ordering is also opt-in **per message**: a message whose `group_key/1` returns
`nil` is unordered and can neither block nor be blocked.

```elixir
defmodule MyApp.Events.OrderPaid do
  use Outboxer.Message, outbox: :orders
  defstruct [:order_id]

  @impl true
  def group_key(%__MODULE__{order_id: id}), do: "order:#{id}"
end
```

Choose the key as the smallest thing whose events must stay in sequence: usually the aggregate
(`"order:42"`), sometimes the tenant. The narrower the key, the less one bad message can hold up.

## How order is kept

1. Every message gets a monotonically increasing id when it is inserted.
2. A group is hashed to one **partition** of its outbox (`partitions`, default 1).
3. A partition has exactly one active worker in the whole cluster. It reads pending messages by id
   and hands them to the relay in that order.
4. A message is removed only after the relay confirmed it.

So within a group, message *n+1* is never handed to the relay before message *n* was confirmed.

## When a message fails

The failed message gets its attempt counter increased and a retry time (`:backoff`, exponential
from 1 s to 60 s by default). Until then:

- the rest of **its group** is held back, because sending them would break the order;
- every other group, and all ungrouped messages, keep flowing.

What happens next depends on `:max_attempts`:

- **`:infinity` (default): strict.** The message is retried forever. Order is never broken, and
  the group stays blocked until the cause is fixed or you remove the message with
  `Outboxer.discard/3`. Choose this when a gap in the sequence is worse than a delay.
- **A number: dead-letter and continue.** After that many failures the message becomes *dead* and
  the group moves on. Order now has a gap. Dead messages stay in the table until you call
  `Outboxer.retry_dead/2` or `Outboxer.discard/3`. Choose this when one poison message must not
  stall an entity indefinitely.

```elixir
orders: [relay: ..., max_attempts: 10, backoff: [min: 500, max: 30_000]]
```

A retried dead message keeps its original id, so it goes out as soon as it is revived, by which
time later messages of its group may already have been delivered.

## The fine print

### "Order" means insertion order, which is commit order only if writers are serialised

Ids are assigned when a row is inserted, not when its transaction commits. If two transactions
write to the same group at the same time, the one with the higher id can commit first, be relayed,
and then the lower id shows up.

In practice this rarely applies: two transactions changing the same order usually lock the same
`orders` row, which already serialises them. If nothing in your transaction does that, set
`serialize_writes: true` on the outbox. On Postgres, `publish` then takes a transaction-scoped
advisory lock on the group key, so concurrent publishers of one group queue up. (SQLite has a
single writer and needs nothing.)

`publish` must then come **before** other statements that could block on the competing
transaction, or after the lock that already serialises them; otherwise it only narrows the window.

### Duplicates can appear, but never out of order within a batch

If a batch was delivered and the following database commit failed, the whole batch is delivered
again. A consumer that tracks the last id it processed per group can drop these.

### Do not change `partitions` on a live backlog

The partition is stored on the row when it is published. Changing the count sends new messages of
a group to a different partition than its pending ones, which can reorder them, and lowering it
orphans rows in partitions that no longer have a worker. Let the outbox drain first
(`Outboxer.counts/2` shows `pending: 0`), then change it.

### The relay has to play along

Order survives only if the destination keeps it:

- **Kafka**: yes, per key. The group key becomes the message key.
- **SQS**: only FIFO queues. Standard queues do not keep order.
- **Oban**: jobs are inserted in order, but run in order only with a queue concurrency of 1.

### `:total` ordering has a price

One partition, one worker, and any failing message stops the entire outbox. Use it for low-volume
streams where global order really matters (an audit log), not as a default.
