# Storage

Outboxer stores messages through the `Outboxer.Storage` behaviour. The adapter is picked from your
repo's Ecto adapter.

## Postgres

`Outboxer.Storage.Postgres`. Works on one node or many.

A worker delivers a batch inside a short transaction:

```sql
BEGIN;
SELECT pg_try_advisory_xact_lock(:outbox_key, :partition);  -- false: another node has it, back off
SELECT ... FROM outboxer_messages WHERE ... ORDER BY id LIMIT :batch_size;
-- relay delivers
DELETE FROM outboxer_messages WHERE id = ANY(:delivered);
COMMIT;
```

Why this shape:

- **The lock cannot be stranded.** It belongs to the transaction. If the worker, the node or the
  connection dies, Postgres releases it. There are no leases, heartbeats or timeouts to tune.
- **No claim step.** Many outbox implementations first `UPDATE` rows to mark them as taken. That
  doubles the writes and leaves a dead tuple per message. Here a message is inserted once and
  deleted once.
- **It works behind PgBouncer** in transaction pooling mode, which session-level advisory locks
  do not.

Things to keep in mind:

- A busy partition holds **one pooled connection** for the duration of a delivery. Budget for the
  total number of partitions across your outboxes.
- The transaction is open while the relay talks to the broker. Keep batches and relay timeouts
  small enough that this stays in the low seconds; long transactions hold back vacuum.
- Use the default `READ COMMITTED` isolation level.

Options that only apply to Postgres: `notify`, `serialize_writes`, `prefix`.

## SQLite

`Outboxer.Storage.SQLite`. For single-node applications.

SQLite has one writer at a time, so the design differs:

- Delivery happens **outside** any transaction. A batch is read, delivered, then deleted in one
  short statement. Holding the write lock during broker I/O would stall your whole application.
- Exclusivity comes from the supervision tree: one worker per partition on the one node.

Consequences:

- **Do not run two nodes against the same database file**, and do not start two Outboxer instances
  with the same outboxes on it. For LiteFS-style replication, relay on the primary only and start
  the other nodes with `relay: false` so they can still publish.
- Use WAL mode (the `ecto_sqlite3` default) and a `busy_timeout` of a few seconds.
- More partitions help only as far as the relay is the bottleneck; writes are serialised anyway.

## Could Redis be a storage?

It could implement the behaviour, but it would not be a transactional outbox, and Outboxer does not
ship one.

The pattern works because the message and the business data commit **atomically, in one
transaction of one database**. With Redis you are back to two writes:

```text
COMMIT to Postgres     ← crash here and the message is lost
LPUSH to Redis
```

or, the other way round, a message for a change that was rolled back. A lock on a Redis key does
not help: locks coordinate the *relays* (who delivers a partition), not the *write*. What you would
get is a durable-ish queue in front of the broker, with the very gap the outbox exists to close.

Redis as storage does make sense in one case: **when Redis itself is your system of record** for
the data in question, so that the business write and the outbox write can share a `MULTI`/Lua
script. If that is your situation, the `Outboxer.Storage` behaviour documents the contract an
adapter must meet. The important parts are that `insert/2` must be atomic with the caller's write,
and `with_partition/4` must give cluster-wide exclusive ownership that is released when the owner
dies (a lock with a TTL and fencing, in Redis terms).

## The table

By default one table, `outboxer_messages`, shared by every outbox of the instance (instance options
`:table`, and `:prefix` for a Postgres schema):

| Column | Purpose |
| --- | --- |
| `id` | Order of delivery and stable identity |
| `outbox`, `partition` | Which worker handles the row |
| `group_key` | Ordering group, or `NULL` |
| `type`, `payload`, `headers` | The message; `payload` is an opaque binary |
| `status` | `pending`, `delivered` (only with `retention: {:keep, _}`) or `dead` |
| `attempts`, `last_error`, `scheduled_at` | Retry bookkeeping |
| `inserted_at`, `delivered_at` | Timestamps |

All three indexes are partial. The two used on the hot path cover only `pending` rows, so their
size tracks the backlog rather than the history.

## One table per outbox

An outbox can have a table of its own, named as you like:

```elixir
{Outboxer,
 repo: MyApp.Repo,
 outboxes: [
   orders:   [table: "order_outbox",   relay: ...],
   invoices: [table: "invoice_outbox", relay: ...],
   audit:    [relay: ...]              # no :table, so it uses the instance table
 ]}
```

Create each table with its own migration call. They all have the same columns and indexes:

```elixir
def up do
  Outboxer.Migration.up(table: "order_outbox")
  Outboxer.Migration.up(table: "invoice_outbox")
  Outboxer.Migration.up()                          # outboxer_messages, for :audit
end

def down do
  Outboxer.Migration.down(table: "order_outbox")
  Outboxer.Migration.down(table: "invoice_outbox")
  Outboxer.Migration.down()
end
```

Publishing does not change: a message names its outbox, and the outbox knows its table. One
`publish/2` call with messages for several outboxes writes to each table in the same transaction.

When to split:

- **Isolation.** A backlog, a bloated index or a slow vacuum in one outbox does not touch the
  others. Autovacuum settings, retention and monitoring can differ per table.
- **Operations.** You can truncate, inspect or move one outbox without filtering by name.

When one shared table is fine: a handful of low-volume outboxes. The hot indexes are partial and
lead with `outbox`, so outboxes in one table do not scan each other's rows.

Two things follow from splitting:

- **Ids are unique within a table, not across tables.** That is why `Outboxer.discard/3` and
  `retry_dead/2` take the outbox. `Outboxer.Envelope.dedup_id/1` includes the outbox name and stays
  unique.
- **`partitions` is unrelated to tables.** It is the number of parallel delivery lanes inside one
  outbox, 1 by default. With one table per outbox and the default, the `partition` column is
  always 0.

Moving an existing outbox to a new table: let it drain, then change `:table`. Rows left in the old
table are not picked up.
