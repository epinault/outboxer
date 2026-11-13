# Changelog

## 0.1.0 (2026-11-13)

Initial release.

- `Outboxer.Message` behaviour and `Outboxer.publish/2`, `Outboxer.publish_multi/4`.
- Postgres and SQLite storage behind `Outboxer.Storage`.
- Ordered groups, total ordering, or no ordering per outbox; partitioned workers.
- Retries with backoff, strict blocking by default, optional dead-lettering with `max_attempts`.
- Relays: `Outboxer.Relay.Kafka`, `Outboxer.Relay.SQS`, `Outboxer.Relay.Oban`, `Outboxer.Relay.Function`.
- Optional Postgres `LISTEN/NOTIFY` wake-up.
- `Outboxer.Testing` helpers and telemetry events.
- Publish-only nodes: `relay: false` on the instance stores messages without starting workers or
  initialising relays, so publishing and relaying can run in different pods.
- One table per outbox: the `table` outbox option, for layouts such as `order_outbox` and
  `invoice_outbox`. Outboxes without it share the instance table.
- `Outboxer.discard/3` takes the outbox, since ids are unique per table.
- `mix bench`: a benchmark against real Postgres or SQLite (publishing, relaying, deep backlogs,
  blocked groups, churn, pruning), with the results in the operations guide.
- Postgres: the fetch query always reads the partial pending index. Before, with
  `retention: {:keep, _}` and stale table statistics, the planner could walk the primary key and
  relay throughput fell by up to ten times until the next `ANALYZE`.
