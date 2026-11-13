# Build log

## 2026-11-12: initial build

Built in one session from the design in `superpowers/specs/2026-11-12-outboxer-design.md`.

Order: core (message, storage, workers, function relay, demo) → failure semantics and retention →
Kafka/SQS/Oban relays against fakes → Postgres NOTIFY and multi-node tests → Docker integration
tests → docs.

What went wrong, and what it taught:

- **`outbox` was taken on Hex**, hence `outboxer`.
- **First design had SKIP LOCKED competing consumers** for unordered outboxes and partitions for
  ordered ones. Collapsed into "everything is a partition": one algorithm, and SQLite (no SKIP
  LOCKED, one writer) fits the same model.
- **A test read table state right after a delivery and failed on Postgres only.** The relay runs
  before the batch commits. Tests now use `eventually/2` for table state.
- **`Outboxer.Testing.drain/2` looped forever** when a message kept failing with zero backoff. It
  now stops when a pass makes no progress.
- **The notifier crashed on start**: `Postgrex.Notifications.listen/2` returns `{:ok, ref}` when
  already connected, not only `{:eventually, ref}`.
- **Racing SQLite connections on a fresh file** logged "database is locked"; the test helper now
  migrates through a single connection first.
- **MiniStack returned no message attributes**; the cause was the receive request ex_aws_sqs
  builds, not the relay. See `constraints.md`.
- Rough throughput on a laptop (Postgres in Docker): ~3,300 single-message transactions/s
  published with 8 writers; ~39,000 messages/s relayed with 4 partitions and a no-op relay.
