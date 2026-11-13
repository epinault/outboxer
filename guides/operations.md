# Operations

## Sizing

**Partitions** set how many batches of an outbox are delivered at the same time. Start with 1.
Raise it when the relay, not the database, is the bottleneck and you have many groups. Messages of
one group never run in parallel, whatever the number.

**Connection pool (Postgres).** A partition that is delivering holds one connection. Add the total
number of partitions of all outboxes to the pool size your application needs anyway. With
`notify: true`, add one more connection outside the pool. This applies to nodes that relay; a
publish-only node (`relay: false`) uses no connections of its own.

**Batch size** (default 100). Larger batches mean fewer round trips and longer transactions. If the
relay sends messages one by one (function relay, group-safe SQS), a batch takes `batch_size ×
latency` and holds a connection for that long.

**Poll interval** (default 1 s) is the worst-case pickup delay for a message published inside a
transaction, and the idle query rate: one small indexed query per partition per interval. A worker
that finds a full batch polls again immediately, so a backlog drains at full speed regardless.

**Latency.** For pickup within milliseconds on Postgres, set `notify: true`: publishing calls
`pg_notify`, which Postgres delivers when the transaction commits. Polling stays on as a fallback.
`pg_notify` takes a brief global lock at commit, so very write-heavy systems should measure before
enabling it.

## Keeping the table small

Table bloat is the usual way outboxes hurt a database. Outboxer is designed around it, and a few
choices are yours.

- **Leave `retention: :delete` on** unless you need delivered messages for audit or debugging. With
  it, the table holds only the backlog.
- With `retention: {:keep, seconds}`, delivered rows are marked and pruned in batches every
  `prune_interval`. Keep the window short. Each kept message costs an extra `UPDATE` and a dead
  tuple.
- **Postgres autovacuum.** An outbox table is small and churns constantly, which the default
  thresholds handle poorly. Tune it for each outbox table:

  ```sql
  ALTER TABLE outboxer_messages SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
  );
  ```

- **Long transactions anywhere in the database** stop vacuum from removing dead rows, here as in
  every table. An idle-in-transaction session elsewhere is the most common cause of an outbox
  table that grows while its row count stays near zero.
- **Dead messages are never pruned.** They wait for a decision. Alert on them.
- Do not put large payloads in messages.

## What to watch

`Outboxer.counts/2` returns `%{pending: n, delivered: n, dead: n}` for an outbox. Export it
periodically and alert on:

| Signal | Meaning |
| --- | --- |
| `pending` growing | Relay slower than publishers, destination down, or a blocked group |
| `dead > 0` | Messages gave up after `max_attempts`; order has a gap |
| `[:outboxer, :message, :retry]` rate | Destination trouble |
| The same id retrying for a long time | A poison message blocking its group |
| `[:outboxer, :relay, :batch, :stop]` duration | Time a connection and transaction are held |

All telemetry events are listed in the `Outboxer` module documentation.

## Stuck and dead messages

**A group is blocked.** With `max_attempts: :infinity`, a message that cannot be delivered holds
its group. Find it:

```elixir
:orders
|> Outboxer.list_pending(limit: 1_000)
|> Enum.filter(&(&1.attempts > 0))
#=> [%Outboxer.Envelope{id: 4711, attempts: 37, last_error: "...", group_key: "order:42", ...}]
```

Fix the cause and it goes out at its next retry. If the message can never be delivered, remove it
and let the group continue:

```elixir
Outboxer.discard(:orders, [4711])
```

**Messages are dead.** With a numeric `max_attempts`:

```elixir
Outboxer.list_dead(:orders)
Outboxer.retry_dead(:orders)                 # all of them
Outboxer.retry_dead(:orders, ids: [4711])    # some
Outboxer.discard(:orders, [4711])            # give up for good
```

A revived message is sent as soon as it is picked up, possibly after later messages of its group.

## Deploying

- **Rolling deploys** are safe on Postgres: old and new nodes compete for partitions through the
  database, and only one wins each batch.
- **Shutdown.** A worker that is stopped mid-batch rolls its transaction back; the batch is
  delivered again by whoever picks the partition up. That is one source of duplicates.
- **Changing `partitions`**: let the outbox drain first. See
  [Ordering and groups](ordering-and-groups.md).
- **Renaming or removing an outbox** leaves its pending rows without a worker. Drain it first, or
  delete the rows.
- **Publish-only nodes.** See [Running the relay on its own nodes](#running-the-relay-on-its-own-nodes).
- **Read replicas.** Point Outboxer at the primary repo. Workers must see committed rows
  immediately and must write.

## Running the relay on its own nodes

By default every node that starts Outboxer both publishes and relays. To publish from one set of
nodes (web pods, say) and relay from another, start the same outboxes everywhere and switch
delivery off where you do not want it:

```elixir
# lib/my_app/application.ex, the same code in every pod
{Outboxer,
 repo: MyApp.Repo,
 relay: System.get_env("OUTBOXER_RELAY", "true") == "true",
 outboxes: MyApp.Outboxes.config()}
```

Set `OUTBOXER_RELAY=false` on the web deployment and leave it unset on the relay deployment.

A `relay: false` instance:

- starts no processes: no workers, no relay clients, no notifier, no pruner;
- ignores the `:relay` option of its outboxes, so that key can be left out, and the node needs
  neither broker settings nor the broker dependency (brod, ex_aws, Oban);
- still supports `publish/2`, `publish_multi/4`, `counts/2`, `list_pending/2`, `list_dead/2`,
  `retry_dead/2` and `discard/3`.

What the two sides must agree on, because it decides where a message is written and who picks it
up. Outboxer cannot check this across nodes, so build both from one function:

| Setting | Why |
| --- | --- |
| Outbox names and `table` | The relay only reads the tables and outboxes it knows |
| `partitions` | Publishers assign the partition; a relay with fewer never reads the rest |
| `ordering`, `serialize_writes` | They decide the group key and locking at publish time |
| Instance `name` and `notify` | The name is part of the `NOTIFY` payload the relay listens for |
| `prefix`, `repo` database | Same tables |

Things to know:

- **Run at least one relaying node**, or messages only pile up. Several are fine on Postgres:
  they compete for partitions through the database. Alert on `pending` growing.
- **Pickup delay.** A relay on another node learns of a new message at its next poll
  (`poll_interval`), or within milliseconds with `notify: true` on both sides.
- **Retention.** The pruner runs on relaying nodes only.
- **SQLite** is single-node, so this split applies to it only in a primary/replica setup such as
  LiteFS: relay on the primary, `relay: false` elsewhere.

`testing: :manual` is for tests. It also starts no workers, but it still initialises the relays so
that `Outboxer.Testing.drain/2` can deliver through them.

## Failure modes, in short

| What happens | Result |
| --- | --- |
| App crashes before commit | No message. Correct |
| App crashes after commit | Message is relayed later |
| Broker is down | Messages wait and retry with backoff; requests are unaffected |
| Worker or node dies mid-batch | Batch is redelivered; possible duplicates |
| Database is down | Publishing fails with your transaction; workers log and retry |
| One message can never be delivered | Its group blocks (strict) or it goes dead (`max_attempts`) |
