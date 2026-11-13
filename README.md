# Outboxer

A transactional outbox for Elixir and Ecto.

Write a message to the outbox **in the same database transaction** as your business data. Outboxer
relays it to Kafka, SQS, Oban or your own sink, in order, retrying until it gets through.

```elixir
Repo.transaction(fn ->
  order = Repo.insert!(changeset)
  {:ok, _} = Outboxer.publish(%OrderPlaced{order_id: order.id, total: order.total})
  order
end)
```

If the transaction commits, the message will be delivered. If it rolls back, the message never
existed. That is the guarantee "save to the database, then publish to the broker" cannot give: a
crash between the two steps either loses the message or publishes one for data that was never saved.

## Features

- **Postgres and SQLite** storage, behind a behaviour.
- **Ordered groups.** Messages sharing a group key are delivered in order. Ordering is optional per
  message and per outbox, and a whole outbox can be strictly ordered.
- **Strict by default.** A failing message blocks its own group (and nothing else) until it goes
  through. Opt into dead-lettering after N attempts when you prefer progress over order.
- **Relays for Kafka (brod), SQS (ex_aws) and Oban**, each compiled only when you have its
  dependency, plus a function relay and a small behaviour for writing your own.
- **Small footprint in your database.** A delivered message costs one `INSERT` and one `DELETE`.
  No claim/lease updates, and indexes that cover only the backlog.
- **No global configuration and no application.** Everything is passed to `{Outboxer, opts}` in your
  supervision tree, and only the processes that configuration needs are started.
- **Multi-node safe** on Postgres, with no leader election and no leases to expire.

## Installation

```elixir
def deps do
  [
    {:outboxer, "~> 0.1"},

    # the driver your Repo already uses
    {:postgrex, ">= 0.0.0"},        # or {:ecto_sqlite3, ">= 0.0.0"}

    # add only the relay you need
    {:brod, "~> 4.0"},               # Outboxer.Relay.Kafka
    {:ex_aws_sqs, "~> 3.4"},         # Outboxer.Relay.SQS
    {:oban, "~> 2.18"}               # Outboxer.Relay.Oban
  ]
end
```

A relay module exists only if its dependency is in your project when Outboxer is compiled. If you
add one later, run `mix deps.compile outboxer --force`.

Requires Elixir 1.18 or later.

## Quick start

**1. Create the table.**

```elixir
defmodule MyApp.Repo.Migrations.AddOutboxer do
  use Ecto.Migration

  def up, do: Outboxer.Migration.up()
  def down, do: Outboxer.Migration.down()
end
```

**2. Define a message.**

```elixir
defmodule MyApp.Events.OrderPlaced do
  use Outboxer.Message, outbox: :orders

  defstruct [:order_id, :total]

  # Optional. Messages with the same group key are delivered in order.
  @impl true
  def group_key(%__MODULE__{order_id: id}), do: "order:#{id}"
end
```

**3. Add Outboxer to your supervision tree**, after the repo.

```elixir
children = [
  MyApp.Repo,
  {Outboxer,
   repo: MyApp.Repo,
   outboxes: [
     orders: [
       relay: {Outboxer.Relay.Kafka, client: :my_brod_client, topic: "orders"},
       partitions: 4
     ]
   ]}
]
```

**4. Publish inside your transaction**, with `Outboxer.publish/2` as above or in an `Ecto.Multi`:

```elixir
Ecto.Multi.new()
|> Ecto.Multi.insert(:order, changeset)
|> Outboxer.publish_multi(:event, fn %{order: order} ->
  %OrderPlaced{order_id: order.id, total: order.total}
end)
|> Repo.transaction()
```

## Try it

A demo that needs nothing but this repository (SQLite, no broker):

```sh
mix deps.get
mix run examples/demo.exs
```

It publishes order events in transactions, rolls one back, and relays through a sink that fails
one time in four, so you can see ordering and retries.

## What it is good at, and what it is not

**Good at**

- Never losing a message that belongs to a committed transaction, and never emitting one for a
  transaction that rolled back.
- Keeping per-entity order (all events of one order, one account, one aggregate) end to end.
- Staying out of the way: the table stays close to empty, and the library is rarely the
  bottleneck. On a laptop with Postgres in Docker, a rough measurement gave about 3,000
  single-message transactions per second published, and tens of thousands of messages per second
  relayed through a no-op relay. Real throughput is set by your database and the destination.

**Not good at / not for**

- **Exactly-once delivery.** Delivery is at-least-once. Consumers must tolerate a duplicate; every
  message carries a stable id for that.
- **Very high throughput streaming.** Every message is a row written and deleted in your primary
  database. For sustained high volume, log-based change data capture (Debezium, WalEx) adds no
  writes and scales further.
- **Large payloads.** The payload lives in your database until it is relayed. Store a reference.
- **Low-latency request/response.** Latency is "next poll" (default up to 1 s), or a few
  milliseconds with `notify: true` on Postgres.
- **Delayed or scheduled messages**, priorities, fan-out to several relays from one outbox. It is an
  outbox, not a job queue. Use Oban for those (possibly fed by `Outboxer.Relay.Oban`).
- **Databases other than the one your business data is in.** The guarantee comes from sharing a
  transaction. See the Storage guide for why Redis does not qualify.

The guides go into each of these:

- [Overview](guides/overview.md): the pattern, the guarantees and the trade-offs
- [Ordering and groups](guides/ordering-and-groups.md)
- [Storage: Postgres, SQLite, and why not Redis](guides/storage.md)
- Relays: [Kafka](guides/relay-kafka.md), [SQS](guides/relay-sqs.md), [Oban](guides/relay-oban.md),
  [writing your own](guides/custom-relays.md)
- [Operations](guides/operations.md): sizing, table bloat, monitoring, stuck and dead messages
- [Testing](guides/testing.md)

## Development

```sh
mix setup
docker compose up -d postgres      # Postgres for the test suite
mix test                           # SQLite + Postgres, relays against fake clients
mix test --exclude postgres        # SQLite only, no Docker
docker compose up -d               # + Kafka and MiniStack (SQS)
mix test.integration               # everything, against the real services
mix precommit                      # compile (warnings as errors), format, credo --strict, test
```

Building brod's dependencies needs `cmake` (`brew install cmake`).

## License

MIT. See [LICENSE](LICENSE).
