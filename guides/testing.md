# Testing

## Your application

Start Outboxer in **manual mode** in the test environment. No workers run, nothing is relayed
unless you ask, and `publish` writes through the test's own sandboxed connection, so everything
rolls back with the test and `async: true` keeps working.

```elixir
# config/test.exs
config :my_app, :outboxer, testing: :manual

# lib/my_app/application.ex
outboxer =
  [
    repo: MyApp.Repo,
    outboxes: [orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod, topic: "orders"}]]
  ] ++ Application.get_env(:my_app, :outboxer, [])

children = [MyApp.Repo, {Outboxer, outboxer}]
```

The relay's options are still validated at boot, so configuration mistakes show up in tests.

### Assert that something was published

```elixir
import Outboxer.Testing

test "placing an order publishes OrderPlaced" do
  {:ok, order} = Orders.place(@params)

  envelope = assert_published(:orders, MyApp.Events.OrderPlaced)
  assert envelope.group_key == "order:#{order.id}"
  assert JSON.decode!(envelope.payload)["total"] == 100
end

test "a rejected order publishes nothing" do
  {:error, _changeset} = Orders.place(%{})
  refute_published(:orders, MyApp.Events.OrderPlaced)
end
```

`assert_published/3` takes a message module, a type string, or a function on the envelope.
`published/2` returns all pending envelopes if you want to assert on the list.

### Exercise the relay

`Outboxer.Testing.drain/2` delivers everything pending, synchronously, in the test process, through
the configured relay:

```elixir
test "events reach the sink" do
  Orders.place(@params)
  assert %{delivered: 1, failed: 0} = drain(:orders)
end
```

For that you usually want a relay you control in tests. Swap it in configuration:

```elixir
# config/test.exs
config :my_app, :orders_relay, {Outboxer.Relay.Function, fun: {MyApp.TestSink, :deliver, []}}
```

`drain/2` stops when a pass delivers nothing, so a relay that keeps failing cannot hang your test.

### If you run real workers in tests

You rarely need to. If you do, the workers are separate processes that use real transactions, so
the SQL sandbox must be in shared mode (`async: false`) and you should stop Outboxer at the end of
the test.

## This library

```sh
docker compose up -d postgres
mix test                        # SQLite and Postgres; relays against fake clients
mix test --exclude postgres     # SQLite only, no Docker
docker compose up -d
mix test.integration            # adds real Kafka and SQS (MiniStack)
```

- Database tests are parameterised and run against both adapters.
- `test/outboxer/postgres_test.exs` covers what only Postgres does: several nodes on one outbox, a
  worker killed mid-batch, NOTIFY, write serialisation, schema prefixes.
- Relay unit tests use fake clients (`test/support/fakes.ex`) injected through an undocumented
  relay option, so CI needs no broker.
- `test/integration/` runs against the services in `compose.yml` and is excluded unless you pass
  `--include integration`.
