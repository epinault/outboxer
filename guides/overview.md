# Overview

## The problem

A service changes its database and needs to tell the rest of the system about it:

```elixir
Repo.insert!(order)
Kafka.produce("orders", order_placed)   # what if we crash right here?
```

There are two writes to two systems and nothing makes them atomic. Crash after the first and the
event is lost. Swap them and you can announce an order that was never saved. Wrapping both in a
database transaction does not help: the broker is not part of it.

## The pattern

Write the event to a table in the **same database, in the same transaction** as the business data.
A separate process, the relay, reads that table and forwards the rows to the broker.

```text
 your code                         Outboxer
┌──────────────────────────┐     ┌───────────────────────────────┐
│ BEGIN                    │     │ worker (one per partition)    │
│  INSERT INTO orders      │     │   fetch pending, oldest first │
│  INSERT INTO outbox  ────┼──►  │   relay.deliver(batch) ───────┼──► Kafka / SQS / Oban / ...
│ COMMIT                   │     │   delete delivered rows       │
└──────────────────────────┘     └───────────────────────────────┘
```

Now there is one atomic write. Either both rows exist or neither does. Delivery to the broker
happens afterwards and can be retried for as long as it takes.

## What you are guaranteed

- **No lost messages.** A message whose transaction committed stays in the table until a relay
  confirms delivery.
- **No phantom messages.** A message whose transaction rolled back was never visible to a worker.
- **At-least-once delivery.** If delivery succeeded but recording that fact failed (a crash, a lost
  connection), the message is delivered again.
- **Order within a group**, if you ask for it. See [Ordering and groups](ordering-and-groups.md).

## What you are not guaranteed

- **Exactly-once.** Duplicates are rare but possible, so consumers must be idempotent. Every
  message has a stable id (`Outboxer.Envelope.dedup_id/1`) that the built-in relays pass along.
  The one exception is `Outboxer.Relay.Oban` on the same Postgres repo, where the handoff is
  atomic; see its guide.
- **Bounded latency.** A message is picked up at the next poll (`poll_interval`, default one
  second), or within milliseconds with `notify: true` on Postgres. If the broker is down, it waits.
- **Global order.** Order is per group. Two messages of different groups can overtake each other.
  `ordering: :total` gives a global order at the cost of all parallelism.

## When an outbox is the right tool

- An event must be published **if and only if** a database change happened: domain events,
  integration events, change notifications, search-index updates, cache invalidation across
  services.
- Consumers rely on seeing the events of one entity in order.
- You would rather be late than wrong: a broker outage should delay events, not drop them and not
  fail user requests.

## When it is not

| You need | Use instead |
| --- | --- |
| Sustained very high volume | Log-based CDC (Debezium, WalEx): it reads the WAL and adds no writes |
| To stream every change of a table without touching application code | CDC again |
| Background jobs: scheduling, priorities, uniqueness, per-job retries | Oban. With Oban on the same repo, inserting the job in your transaction is already transactional |
| A message that is not tied to any database change | Publish to the broker directly |
| Request/response with a deadline | A direct call; an outbox adds latency by design |
| Big payloads (files, large documents) | Store the blob elsewhere and send a reference |

## What it costs

- **One extra row per message** in your primary database: an insert in your transaction and, later,
  a delete. That is write load and WAL volume on the database you care most about.
- **A database connection per busy partition** on Postgres while a batch is being delivered.
- **A polling query per partition per `poll_interval`** when idle. It hits a small partial index
  and is cheap, but it is not zero.
- **Operational attention.** A blocked group or a growing backlog needs to be noticed. See
  [Operations](operations.md).

## Vocabulary

- **Outbox**: a named stream of messages with one relay and its own settings. An instance can have
  several; they share one table.
- **Message**: your struct implementing `Outboxer.Message`.
- **Envelope**: a stored message as the relay sees it (`Outboxer.Envelope`).
- **Group**: messages sharing a group key; the unit of ordering.
- **Partition**: a lane of an outbox with exactly one active worker. A group lives in one partition.
- **Relay**: the module that delivers envelopes somewhere (`Outboxer.Relay`).
