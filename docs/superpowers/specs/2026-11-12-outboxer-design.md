# Outboxer — transactional outbox library for Elixir

## Context

You want an OSS transactional outbox library (GitHub-hosted) to replace a design you built before and
disliked. The two pains you named were **performance / table bloat** and **tangled API and config**, so
the design optimises for: few row writes per message, small hot indexes, no global config, and relays
fully decoupled from the core.

Decisions you made:

- Name: **`outboxer`** (`outbox` is taken on Hex). New repo at `/Users/manu/Projects/outboxer`.
- Storage behaviour with **Postgres + SQLite** adapters in v1. No Redis in v1 (documented as a possible
  later best-effort adapter, since it cannot be transactional with your DB).
- Failing message in an ordered group: **configurable, strict by default** (block and retry; optional
  dead-letter after N attempts and continue).
- Tests: **fake clients in CI**, plus a **full local Docker** path with real Postgres, Kafka and
  MiniStack (SQS).

My calls (say so if you disagree): Ecto-based; Oban-style instance API (`{Outboxer, ...}` in the
supervision tree, no `application.ex`); payload stored as opaque binary; MIT licence; Elixir `~> 1.18`
(built-in `JSON`, no Jason dependency).

**Scaffolding (your instruction):** the project is initialised with your
`/elixir-scaffold:new-elixir-project` skill, Mix Library type. The skill is GitLab-only today, so
right after it runs I swap the GitLab pieces for GitHub: no `git@gitlab.com:` remote, replace
`.gitlab-ci.yml` with a GitHub Actions workflow, and drop the Dockerfile / fly.toml if it generates
them for a library. I also apply the known template fixes (`usage_rules ~> 1.2`, AGENTS.md as the
substance with a thin `CLAUDE.md`, `mix usage_rules.sync AGENTS.md --all`).

## Public API

```elixir
# 1. A message
defmodule MyApp.Events.OrderPlaced do
  use Outboxer.Message, outbox: :orders
  defstruct [:order_id, :total]

  @impl true
  def group_key(%__MODULE__{order_id: id}), do: "order:#{id}"   # optional; nil = unordered
end

# 2. Supervision tree (starts Outboxer's own supervisor)
{Outboxer,
 repo: MyApp.Repo,
 outboxes: [
   orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod, topic: "orders"}, partitions: 4],
   emails: [relay: {Outboxer.Relay.Oban, worker: MyApp.EmailWorker}, ordering: :none]
 ]}

# 3. Publish inside your transaction
Repo.transaction(fn ->
  order = Repo.insert!(changeset)
  Outboxer.publish(%OrderPlaced{order_id: order.id, total: order.total})
end)

Ecto.Multi.new() |> Ecto.Multi.insert(:order, cs) |> Outboxer.publish_multi(:event, &build_event/1)
```

- `Outboxer.Message` behaviour: `outbox/0`, `type/0`, `encode/1` (→ binary, default JSON of the struct),
  `group_key/1`, `headers/1`. `use` supplies overridable defaults.
- `Outboxer.Relay` behaviour: `init/1` (validate opts), optional `children/1` (processes to supervise,
  e.g. a brod client), `deliver([Envelope.t()], state)` → `:ok | {:error, reason} | {:partial, %{id => result}}`.
- `Outboxer.Storage` behaviour: `insert/2`, `with_partition/4`, `fetch_batch/4`, `ack/2`, `retry/3`,
  `dead/3`, `prune/2`. Adapter picked from `repo.__adapter__()`.
- All options validated with NimbleOptions at start; nothing read from `Application` env.
- Per-outbox options: `relay`, `ordering` (`:group` default | `:total` | `:none`), `partitions`,
  `batch_size`, `poll_interval`, `max_attempts` (`:infinity` default = strict block), `backoff`,
  `retention` (`:delete` default | `{:keep, seconds}`).
- Instance options: `name`, `repo`, `prefix`/`table`, `notify` (Postgres LISTEN/NOTIFY wake-up),
  `testing: :manual` (no workers; publish only), `require_transaction`.
- Admin: `Outboxer.list_dead/2`, `retry_dead/2`, `discard/2`. `Outboxer.Testing`: `assert_published/2`,
  `drain/2`.
- Telemetry events for publish, batch delivery, retry, dead, prune.

## How it works

**Table** `outboxer_messages`: `id bigserial`, `outbox`, `partition`, `group_key`, `type`,
`payload bytea`, `headers`, `status` (`pending|delivered|dead`), `attempts`, `scheduled_at`,
`last_error`, `inserted_at`, `delivered_at`. Installed by a versioned `Outboxer.Migration.up/1` called
from the user's migration. Partial indexes only on pending rows, so the hot index stays small.

**Everything is partitions.** On insert, `partition = phash2(group_key, partitions)` (random for a nil
group). One worker process per partition; a partition has exactly one active owner cluster-wide:

- Postgres: each batch runs in a short transaction holding `pg_try_advisory_xact_lock(outbox_hash, partition)`.
  Fetch by `id` order → deliver → delete → commit. The lock dies with the transaction, so a crash can
  never strand it, and it works behind PgBouncer. No claim/lease UPDATE: a message costs **one insert
  and one delete**, which is the main anti-bloat measure.
- SQLite: single-node, so the supervisor's one-worker-per-partition is the exclusivity. Delivery happens
  **outside** any transaction (SQLite has one writer; holding it during broker I/O would block the app).

**Ordering and failure.** Within a group, messages go out in `id` order. On failure the message gets
`attempts + 1` and a backoff `scheduled_at`; the fetch query excludes any group that has a waiting
failed message, so that group blocks while other groups and ungrouped messages keep flowing. After
`max_attempts` the message becomes `dead` and the group continues.

**Wake-up.** Adaptive polling (immediately re-poll after a full batch, jittered sleep when idle), plus
optional Postgres `pg_notify` in the publishing transaction, carrying outbox and partition.

**Supervision.** `Outboxer` (Supervisor) → optional notifier → one supervisor per outbox (relay
children + partition workers) → pruner (only when retention keeps rows).

**Guarantees to document plainly:** at-least-once (consumers must be idempotent; a stable dedup id is
provided); order follows `id`, which matches commit order only when same-group writers are serialised
(they normally are via the aggregate row lock; a `serialize_writes` option takes an advisory lock for
strictness); changing `partitions` needs a drained outbox.

## Relays (each compiled only when its dependency is present)

All wrapped in `if Code.ensure_loaded?(...)`, deps declared `optional: true`, each takes an injectable
`client` module so CI uses fakes.

| Relay | Dep | Notes |
| --- | --- | --- |
| `Outboxer.Relay.Kafka` | `brod` | Existing `client:` or `client_config:` + `producer_config:` started under Outboxer; `topic` static or function; group key → Kafka key/partition; batched `produce_sync` per topic-partition |
| `Outboxer.Relay.SQS` | `ex_aws_sqs` | `send_message_batch` in 10s; FIFO detected from `.fifo` (group id + dedup id); per-entry partial failures |
| `Outboxer.Relay.Oban` | `oban` | `Oban.insert_all`; on Postgres with the same repo it joins the batch transaction, so the handoff is atomic |
| `Outboxer.Relay.Function` | none | `fun:` callback; for custom sinks, demos and tests |

## Build order (each milestone ends runnable and committed)

1. **Vertical slice.** Scaffold with `/elixir-scaffold:new-elixir-project` (Mix Library), convert the
   GitLab pieces to GitHub as above, add deps and compose.yml (Postgres on a non-default port, Kafka,
   MiniStack). Save this design to
   `docs/superpowers/specs/2026-11-12-outboxer-design.md`. Message behaviour, migration, config,
   `publish`, supervisor, worker, both storage adapters, Function relay. Deliverable:
   `mix run examples/demo.exs` (SQLite, no Docker) publishes in a transaction and prints relayed messages.
2. **Semantics.** Group blocking, backoff, `max_attempts`/dead, retention + pruner, admin API,
   telemetry, `Outboxer.Testing`, `publish_multi`.
3. **Relays.** Oban, SQS, Kafka with fake clients; verify `mix compile --no-optional-deps --warnings-as-errors`.
4. **NOTIFY wake-up + Docker integration tests** (`@tag :integration`, excluded by default).
5. **Docs and release hygiene.** README, guides (overview: good at / not good at; ordering and groups;
   storage; one per relay; custom relay; operations: bloat, vacuum, pool sizing, dead letters; testing),
   ex_doc config, AGENTS.md + thin CLAUDE.md, GitHub Actions CI, Hex metadata.

I will `git init` and commit locally per milestone; no GitHub remote or push until you say so.

## Tests

- The storage and worker suites run against **both** adapters (ExUnit `parameterize` over a Postgres and
  a SQLite test repo).
- Ordering: per-group order under concurrent publishers; two Outboxer instances on one Postgres DB
  (simulated two nodes) with no loss and no reorder; blocked group does not stall other groups.
- Failure: retry/backoff, dead-letter and continue, crash mid-batch redelivers, partial relay results.
- Bloat: delete-on-delivery leaves the table empty; pruner removes kept/dead rows.
- Relays: unit tests against fake clients; integration tests against real Kafka and MiniStack.
- Supervisor: starts only what the config needs; `testing: :manual` starts no workers.

## Verification

```bash
mix run examples/demo.exs                                  # no Docker needed
docker compose up -d postgres && mix precommit             # format, credo --strict, warnings, tests
mix test --exclude postgres                                # SQLite + fakes only
docker compose up -d && mix test --include integration     # real Postgres, Kafka, MiniStack
mix compile --no-optional-deps --warnings-as-errors        # relays truly optional
mix docs
```

To confirm at implementation time: current versions of brod / ex_aws_sqs / oban and the MiniStack
image name and port.
