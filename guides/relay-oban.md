# Oban relay

`Outboxer.Relay.Oban` turns outbox messages into [Oban](https://hex.pm/packages/oban) jobs. It is
compiled only when `:oban` is among your dependencies.

## First: do you need it?

If Oban uses **the same repo** as your business data, this already is a transactional outbox:

```elixir
Repo.transaction(fn ->
  order = Repo.insert!(changeset)
  Oban.insert!(MyApp.Workers.SendReceipt.new(%{order_id: order.id}))
end)
```

The job commits or rolls back with the order. Adding Outboxer in front adds a table and a hop for
no extra safety.

Use the relay when one of these holds:

- **Oban is in another database** (a dedicated jobs database, another repo), so a job insert cannot
  join your transaction. The outbox restores the guarantee.
- **Jobs must be enqueued in order** per entity, and you want a failing handoff to hold back only
  that entity.
- **Your domain code publishes messages** and should not know that some become jobs while others go
  to Kafka. You can then re-route an outbox without touching the publishing code.

## Setup

```elixir
emails: [
  relay: {Outboxer.Relay.Oban, worker: MyApp.Workers.SendEmail, job_opts: [queue: :mailers]}
]
```

```elixir
defmodule MyApp.Workers.SendEmail do
  use Oban.Worker, queue: :mailers

  @impl true
  def perform(%Oban.Job{args: %{"type" => type, "payload" => payload, "id" => id}}) do
    # payload is the decoded JSON of your message
    ...
  end
end
```

Choose the worker, args or job options per message with functions:

```elixir
worker: fn envelope -> MyApp.Workers.for_type(envelope.type) end,
args: fn envelope -> %{"event" => envelope.type, "data" => JSON.decode!(envelope.payload)} end,
job_opts: fn envelope -> [queue: :events, tags: [envelope.type]] end
```

## Guarantees

| Setup | Handoff |
| --- | --- |
| Postgres, Oban on the **same repo** as Outboxer | **Atomic.** Jobs are inserted and outbox rows deleted in one transaction: exactly one job per message |
| Oban on another repo or database | At-least-once. A job can be inserted twice after a failure |
| SQLite | At-least-once |

Where it is at-least-once, add uniqueness on the message id:

```elixir
job_opts: [unique: [keys: [:id], period: :infinity]]
```

## Ordering

Outboxer inserts jobs in message order. Oban **runs** them in that order only if the queue executes
one job at a time across the cluster (`queues: [mailers: 1]` on a single node, or a global limit /
partitioned queue in Oban Pro). With normal concurrency, jobs of one group run in parallel and can
finish in any order. Also remember that a job that fails and is retried by Oban runs after the jobs
behind it.

If strict processing order matters, let the worker check a sequence number, or keep the processing
in a consumer that reads an ordered stream instead.

## Good to know

- With the default `payload: :json`, a message whose payload is not JSON fails the handoff. Use
  `payload: :raw` (text) or `:base64` (binary), or build the args yourself with `:args`.
- Job arguments are stored as JSON by Oban. Keep them small.
- Outboxer's `max_attempts` covers the *handoff* into Oban. Retries of the *job* are Oban's
  (`max_attempts` in `:job_opts`).
