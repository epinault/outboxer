# Constraints and gotchas

Things the code relies on that are not obvious from reading one file.

## Delivery path

- **One insert, one delete per delivered message.** No claim or lease `UPDATE`. Rows are only
  updated when a delivery fails (attempts, retry time) or with `retention: {:keep, _}`.
- **Exclusivity is per partition, not per row.** Postgres: `pg_try_advisory_xact_lock(outbox_key,
  partition)` inside the batch transaction. SQLite: the single worker process per partition. There
  is no `FOR UPDATE SKIP LOCKED` anywhere, and adding competing consumers on a partition would
  break ordering.
- **The Postgres batch transaction must be READ COMMITTED.** A worker that just acquired the lock
  must see the deletes the previous owner committed; under REPEATABLE READ it could redeliver.
- **SQLite delivers outside a transaction** because holding SQLite's single write lock during
  broker I/O would stall the host application. Do not "unify" the two adapters.
- **The fetch query matches `partition` as a range (`>= p AND <= p`) and orders by
  `(partition, id)`. Do not simplify it to `partition = p ORDER BY id`.** With the equality,
  Postgres may scan the primary key instead of the partial pending index. That is equally fast
  while every row is pending, but with kept or dead rows at the low ids it filters them on every
  poll and holds that plan until the next `ANALYZE`: a 200,000-row drain with `{:keep, _}` fell
  from 27,000 to 3,000 msg/s. The range form leaves the pending index as the only one that
  returns rows in order. A test in `postgres_test.exs` checks the plan; `mix bench` shows the
  effect.
- **Blocked groups are excluded in SQL**, by a subquery over rows with `attempts > 0` and a future
  `scheduled_at`. `scheduled_at` is only ever in the future for failed messages; there is no
  delayed delivery, and adding it would make every delayed message block its group.
- **Only the head of a failed group is charged an attempt.** Messages behind it are left untouched
  to avoid dead tuples. Do not "fix" this by updating the whole group.
- **A relay result of `:ok` for a message after a failed one of its group is still acked.** It has
  been sent; not acking would only add a duplicate. Relays are responsible for not sending it.

## Publishing

- `partition` is computed at publish time and stored. Changing an outbox's `partitions` with
  pending rows can reorder a group or orphan rows.
- Ids are assigned at insert, not at commit. Same-group ordering assumes writers are serialised
  (row lock on the aggregate, or `serialize_writes: true`).
- Local workers are woken directly only when `publish` runs outside a transaction; inside one the
  rows are not visible yet. Postgres `notify` covers the transactional case.

## Tables

- **An outbox lives in exactly one table** (`Outbox.table`, defaulting to the instance table), and
  **ids are unique per table only**. Every storage call that takes ids (`ack`, `fail`, `kill`,
  `discard`) also takes the outbox. Never look a message up by id alone.
- `Storage.SQL.insert/2` writes one `INSERT` per table and restores the caller's order by index.
  The envelopes returned by `publish/2` must stay in input order.
- The Postgres advisory lock key hashes `{prefix, table, outbox}`. Moving an outbox to another
  table changes its key, which is safe only because the old rows are no longer read.

## Configuration

- `relay: false` makes an instance publish-only: no children, and `Outbox.relay` /
  `Outbox.relay_state` are `nil` because relays are not even initialised (the node may lack the
  broker dependency). Use `Config.workers?/1` to decide whether local workers exist; do not test
  `conf.testing` directly. Anything that calls a relay must handle the `nil`.
- Publishers and relays on different nodes must agree on outbox names, `table`, `partitions`,
  `ordering`, instance `name` and `notify`. Nothing checks this across nodes.

- Config lives in `:persistent_term` and is not erased when the supervisor stops. Relay state is
  part of it, so it must be small and immutable.
- `Outboxer.Config.registry/1` builds one atom per instance name. That is the only dynamic atom.
- NOTIFY payloads are `name|outbox|partition`; the listener matches outbox names against the
  configured atoms rather than converting strings to atoms.

## Tests

- SQLite ids must not be reused after delete-on-delivery (they are the dedup id). ecto_sqlite3
  creates the primary key with AUTOINCREMENT; a test guards this.
- `Outboxer.Testing.drain/2` skips the partition lock in `testing: :manual`. With the lock, a
  sandboxed test's outer transaction would hold it and starve concurrent tests.
- `drain/2` must stop when a pass delivers nothing; with a zero backoff it once looped forever.
- MiniStack does not understand the `MessageAttributeNames` parameter ex_aws_sqs sends on receive;
  the integration test sets `MessageAttributeName.1` by hand. Sending attributes works.

## Environment

- brod pulls in `crc32cer`, a NIF that needs `cmake` to build.
- ex_aws needs an XML parser (`sweet_xml` or `saxy`) to report per-entry SQS batch results. Without
  one the SQS relay fails every batch on purpose.
