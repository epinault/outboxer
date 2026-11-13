# AGENTS.md

Guidance for AI agents (and people) working in this repository.

## What this is

`outboxer` is an Elixir library implementing the transactional outbox pattern on Ecto. Applications
write messages to an outbox table inside their own database transaction; Outboxer relays them to
Kafka, SQS, Oban or a custom sink, in order per group, with retries. Published to Hex, hosted on
GitHub (`epinault/outboxer`).

Project memory lives in this repo, not in tool storage:

- `docs/README.md` - index of the project's own notes
- `docs/constraints.md` - invariants and gotchas the code depends on. **Read before changing
  `lib/outboxer/dispatcher.ex`, `lib/outboxer/storage/`, or `lib/outboxer/worker.ex`.**
- `docs/backlog.md` - known gaps and deferred ideas, with the reasoning
- `docs/superpowers/specs/` - the original design
- `guides/` - user-facing documentation, shipped with the package

Add new durable knowledge to `docs/`, not to machine-local memory.

## Commands

```sh
mix setup                          # deps.get
docker compose up -d postgres      # Postgres on localhost:54329 for the tests
mix test                           # SQLite + Postgres; relays against fake clients
mix test --exclude postgres        # no Docker needed
docker compose up -d               # adds Kafka (9092) and MiniStack/SQS (4566)
mix test.integration               # includes test/integration against the real services
mix precommit                      # compile --warnings-as-errors, format check, credo --strict, test
mix compile --no-optional-deps --warnings-as-errors   # relays must stay optional
mix run examples/demo.exs          # runnable demo on SQLite
mix docs
```

Run `mix precommit` before every commit. Building brod needs `cmake`.

## Architecture

```text
Outboxer (Supervisor; no application.ex)          lib/outboxer.ex
├── Registry                                       worker lookup, named <name>.Registry
├── Outboxer.Notifier                              Postgres LISTEN, only with notify: true
├── Outboxer.OutboxSupervisor (one per outbox)     rest_for_one
│   ├── relay.children(state)                      e.g. a brod client
│   └── Outboxer.Worker (one per partition)        polls, calls the dispatcher
└── Outboxer.Pruner                                only with retention: {:keep, _}
```

- `Outboxer.Config` validates options with NimbleOptions and stores the result in
  `:persistent_term` under `{Outboxer, name}`. Nothing reads `Application` env.
- With `relay: false` (publish-only node) or `testing: :manual` the supervisor has no children.
  `Config.workers?/1` is the one check for "are there local workers".
- Each outbox has a `table` (default: the instance table). Message ids are unique per table, so
  every id-based call carries the outbox.
- `Outboxer.Dispatcher.run/4` is the single delivery path: lock partition, fetch, `relay.deliver`,
  settle. Workers and `Outboxer.Testing.drain/2` both go through it.
- `Outboxer.Storage` is the database behaviour. `Storage.SQL` holds the portable Ecto queries;
  `Storage.Postgres` and `Storage.SQLite` add locking and notification.
- `Outboxer.Relay` is the delivery behaviour. Built-in relays live in `lib/outboxer/relay/` and are
  each wrapped in `if Code.ensure_loaded?(Dep) do ... end`. Their option schemas live in separate
  `Options` modules that compile unconditionally.

## Conventions

- Public functions take the instance through a `:name` option (default `Outboxer`). Do not add
  positional name arguments.
- Every public module and function has `@moduledoc`/`@doc`; internal modules use
  `@moduledoc false` with a comment explaining why they exist.
- Module layout is enforced by Credo: `use`, `import`, `alias`, `require`, attributes, functions.
- Line length 120. No `IO.inspect`, no `IO.puts` in `lib/` or `test/`.
- Error messages say what was wrong and what to do about it.
- User-facing behaviour changes need a guide update in `guides/` and a `CHANGELOG.md` entry.

## Testing

- `test/support/case.ex` (`Outboxer.Case`) is the base for database tests. Tests are parameterised
  over the available repos with `parameterize: Outboxer.Case.repos()`, so one test covers SQLite
  and Postgres.
- Tests do not use the SQL sandbox: workers are separate processes and the Postgres adapter depends
  on real transactions and advisory locks. Each test starts from an empty table; database test
  modules are `async: false`.
- `start_outboxer/3` starts a uniquely named instance whose relay (`Outboxer.Test.Sink`) reports
  deliveries to the test process. Use `collect/2` and `eventually/2` rather than sleeping.
- On Postgres the relay runs before the batch commits. Never assert on table state right after
  receiving a delivery; wrap it in `eventually/2`.
- Relays are unit-tested with the fakes in `test/support/fakes.ex`. Anything needing a real broker
  goes in `test/integration/` with `@moduletag :integration`.

## Never

- Never add an `application.ex` or a `mod:` entry. Users start Outboxer in their own tree.
- Never make a relay dependency non-optional, and never reference `:brod`, `ExAws` or `Oban` outside
  their guarded modules.
- Never add an `UPDATE` to the happy path of a message (claiming, leasing, heartbeats). One insert
  and one delete per delivered message is a design goal.
- Never deliver inside a transaction on SQLite, and never drop the partition lock on Postgres.
- Never leave a literal to-do marker in `lib/` or `test/`: Credo fails the build on it.
- Never edit `mix.lock` by hand.
- Never push, create a remote, or publish to Hex without being asked.
