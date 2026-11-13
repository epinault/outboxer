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

## Configuration

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
