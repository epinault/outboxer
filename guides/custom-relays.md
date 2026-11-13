# Custom relays

A relay is a module with two required callbacks. For a one-off, you may not even need a module.

## The quick way: a function

```elixir
audit: [relay: {Outboxer.Relay.Function, fun: &MyApp.Audit.forward/1}]
```

```elixir
def forward(%Outboxer.Envelope{} = envelope) do
  case MyApp.AuditClient.post(envelope.type, envelope.payload) do
    {:ok, _response} -> :ok
    {:error, reason} -> {:error, reason}
  end
end
```

Messages are passed one at a time, in order. Returning `{:error, reason}` or raising marks the
message as failed and holds back the rest of its group.

## A relay module

```elixir
defmodule MyApp.Relay.Webhook do
  @behaviour Outboxer.Relay

  @impl true
  def init(opts) do
    case Keyword.fetch(opts, :url) do
      {:ok, url} when is_binary(url) -> {:ok, %{url: url, timeout: Keyword.get(opts, :timeout, 5_000)}}
      _missing -> {:error, "MyApp.Relay.Webhook requires a :url"}
    end
  end

  @impl true
  def deliver(envelopes, state) do
    Outboxer.Relay.sequential(envelopes, fn envelope ->
      headers = [{"idempotency-key", Outboxer.Envelope.dedup_id(envelope)}]

      case Req.post(state.url, body: envelope.payload, headers: headers, receive_timeout: state.timeout) do
        {:ok, %{status: status}} when status in 200..299 -> :ok
        {:ok, %{status: status}} -> {:error, "HTTP #{status}"}
        {:error, exception} -> {:error, exception}
      end
    end)
  end
end
```

```elixir
hooks: [relay: {MyApp.Relay.Webhook, url: "https://example.com/hooks"}]
```

### `init/1`

Runs once, when the Outboxer supervisor starts. Validate options here so a typo stops the boot
instead of failing every message later. The returned state is passed to every `deliver/2` call. It
is stored in `:persistent_term`, so keep it small and immutable: configuration, not connections.

### `children/1` (optional)

Return child specs for processes the relay needs: a connection pool, a broker client. They are
started under the outbox's supervisor before its workers and restarted with them.

```elixir
@impl true
def children(state), do: [{MyApp.BrokerConnection, name: state.connection, url: state.url}]
```

### `deliver/2`

Receives a non-empty list of envelopes in delivery order and returns one of:

| Return | Meaning |
| --- | --- |
| `:ok` | all delivered |
| `{:error, reason}` | none delivered |
| `{:partial, %{id => :ok \| {:error, reason}}}` | per message; ids not in the map were not attempted |

It is called from the partition's worker process. On Postgres it runs inside the batch transaction,
so the repo calls you make there join it, and the time you spend holds a database connection.

## Rules a relay must follow

1. **Report success only when the destination has durably accepted the message.** After `:ok` the
   row is deleted.
2. **Keep the ordering rule.** Once a message of a group fails, do not deliver later messages of
   that group in the same call. `Outboxer.Relay.sequential/2` does this for you. A relay that
   sends real batches must make the batch all-or-nothing per group, as the Kafka relay does per
   topic-partition.
3. **Bound your time.** Use timeouts. A relay that hangs holds its partition, and on Postgres a
   connection and an open transaction.
4. **Expect to be called again with the same message.** Pass `Outboxer.Envelope.dedup_id/1` to the
   destination when it supports idempotency keys.
5. **Do not swallow errors.** Return them. Retrying and backoff are Outboxer's job; sleeping and
   retrying inside `deliver/2` only blocks the partition.

## Testing a relay

Call it directly; no database is needed:

```elixir
test "posts the payload" do
  {:ok, state} = MyApp.Relay.Webhook.init(url: bypass_url)
  envelope = %Outboxer.Envelope{id: 1, outbox: :hooks, type: "t", payload: ~s({"a":1})}

  assert {:partial, %{1 => :ok}} = MyApp.Relay.Webhook.deliver([envelope], state)
end
```

For the whole path, publish and call `Outboxer.Testing.drain/2`.
