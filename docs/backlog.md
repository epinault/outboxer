# Backlog

Known gaps and ideas, with why they are not done.

| Item | Why deferred |
| --- | --- |
| Redis storage adapter | Cannot be transactional with a SQL database; only meaningful when Redis is the system of record. The `Outboxer.Storage` behaviour is ready for it. See `guides/storage.md` |
| Changing `partitions` on a live backlog | Needs slot-based partitioning (fixed slot count, workers own slot ranges). Documented as "drain first" for now |
| Kafka-compatible partitioner (murmur2) | Outboxer hashes keys with `phash2`. Only matters when other producers write the same keys. A `:partitioner` function is the escape hatch |
| MySQL storage | No demand yet. Would use `GET_LOCK` or `SKIP LOCKED` per partition row |
| Metrics helper (periodic `counts/2` as telemetry) | Easy to do in user land with `:telemetry_poller`; worth adding once the shape is clear |
| Outbox-level pause/resume | Possible through the registry; no use case yet |
| Rename `delivered_at` to `settled_at` | It also holds the time a message died. Proposed to the owner, not decided |
| `group_key` as `text` instead of `varchar(255)` | Long group keys fail on Postgres. Proposed to the owner, not decided |
| Pruning dead messages after N days | Dead messages are never removed automatically, on purpose; an opt-in setting was offered, not requested |
| Per-outbox `prune_interval` | Instance-level today |
| Cross-node config check | A publisher and a relay with different `partitions` or `table` silently strand rows. A stored config fingerprint could detect it |
| GitHub remote, first Hex release | Waiting for the owner's go-ahead |
| elixir-scaffold GitHub path | The scaffold skill is GitLab-only; this repo's GitHub Actions file was written by hand |
