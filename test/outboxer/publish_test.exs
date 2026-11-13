defmodule Outboxer.PublishTest do
  use Outboxer.Case, async: false, parameterize: Outboxer.Case.repos()

  import Outboxer.Testing

  alias Outboxer.Envelope
  alias Outboxer.Test.Custom

  setup context do
    name =
      start_outboxer(
        context,
        [
          events: [partitions: 4],
          custom: [],
          total: [ordering: :total, partitions: 4],
          loose: [ordering: :none]
        ],
        testing: :manual
      )

    {:ok, name: name}
  end

  test "stores a message and returns its envelope", %{name: name} do
    assert {:ok, %Envelope{} = envelope} =
             Outboxer.publish(%Event{n: 1, group: "a"}, name: name, outbox: :events)

    assert envelope.id > 0
    assert envelope.outbox == :events
    assert envelope.type == "Outboxer.Test.Event"
    assert envelope.group_key == "a"
    assert JSON.decode!(envelope.payload) == %{"n" => 1, "group" => "a"}

    assert [stored] = published(:events, name: name)
    assert stored.id == envelope.id
    assert stored.payload == envelope.payload
    assert stored.attempts == 0
    assert %DateTime{} = stored.inserted_at
  end

  test "uses the message's own outbox, type, encoding and headers", %{name: name} do
    {:ok, envelope} =
      Outboxer.publish(%Custom{body: "hi", trace: "t-1"}, name: name, headers: %{extra: 1})

    assert envelope.outbox == :custom
    assert envelope.type == "custom.v1"
    assert envelope.payload == "raw:hi"
    assert envelope.headers == %{"trace" => "t-1", "extra" => "1"}
    assert [%{headers: %{"trace" => "t-1", "extra" => "1"}}] = published(:custom, name: name)
  end

  test "stores binary payloads untouched", %{name: name} do
    binary = <<0, 255, 128, 1, 2, 3>>
    {:ok, _envelope} = Outboxer.publish(%Custom{body: binary, trace: "t"}, name: name)
    assert [%{payload: "raw:" <> ^binary}] = published(:custom, name: name)
  end

  test "publishes a list in order", %{name: name} do
    messages = for n <- 1..5, do: %Event{n: n, group: "g"}
    assert {:ok, envelopes} = Outboxer.publish(messages, name: name, outbox: :events)

    assert Enum.map(envelopes, &n/1) == [1, 2, 3, 4, 5]
    assert envelopes |> Enum.map(& &1.id) |> Enum.sort() == Enum.map(envelopes, & &1.id)
    assert {:ok, []} = Outboxer.publish([], name: name)
  end

  test "commits with the surrounding transaction", %{name: name, repo: repo} do
    {:ok, _result} =
      repo.transaction(fn ->
        Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
      end)

    assert %{pending: 1} = Outboxer.counts(:events, name: name)
  end

  test "rolls back with the surrounding transaction", %{name: name, repo: repo} do
    {:error, :nope} =
      repo.transaction(fn ->
        {:ok, _envelope} = Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
        repo.rollback(:nope)
      end)

    assert %{pending: 0} = Outboxer.counts(:events, name: name)
  end

  test "publish_multi publishes with the multi and rolls back with it", %{name: name, repo: repo} do
    multi =
      Ecto.Multi.new()
      |> Ecto.Multi.run(:order, fn _repo, _changes -> {:ok, %{id: 7}} end)
      |> Outboxer.publish_multi(:event, fn %{order: order} -> %Event{n: order.id, group: "o"} end,
        name: name,
        outbox: :events
      )

    assert {:ok, %{event: %Envelope{} = envelope}} = repo.transaction(multi)
    assert n(envelope) == 7

    failing = Ecto.Multi.run(multi, :boom, fn _repo, _changes -> {:error, :boom} end)
    assert {:error, :boom, :boom, _changes} = repo.transaction(failing)
    assert %{pending: 1} = Outboxer.counts(:events, name: name)
  end

  test "a group always lands in the same partition", %{name: name} do
    {:ok, envelopes} =
      Outboxer.publish(for(n <- 1..20, do: %Event{n: n, group: "same"}),
        name: name,
        outbox: :events
      )

    assert [_one] = envelopes |> Enum.map(& &1.partition) |> Enum.uniq()

    {:ok, spread} =
      Outboxer.publish(for(n <- 1..200, do: %Event{n: n, group: "g#{n}"}),
        name: name,
        outbox: :events
      )

    assert spread |> Enum.map(& &1.partition) |> Enum.uniq() |> Enum.sort() == [0, 1, 2, 3]
  end

  test "group_key can be overridden per call", %{name: name} do
    {:ok, envelope} =
      Outboxer.publish(%Event{n: 1, group: "a"}, name: name, outbox: :events, group_key: "b")

    assert envelope.group_key == "b"
  end

  test "ordering: :total puts everything in one group and one partition", %{name: name} do
    {:ok, envelopes} =
      Outboxer.publish(for(n <- 1..10, do: %Event{n: n, group: "g#{n}"}),
        name: name,
        outbox: :total
      )

    assert Enum.all?(envelopes, &(&1.partition == 0 and &1.group_key == "*"))
  end

  test "ordering: :none drops group keys", %{name: name} do
    {:ok, envelope} = Outboxer.publish(%Event{n: 1, group: "a"}, name: name, outbox: :loose)
    assert envelope.group_key == nil
  end

  test "rejects things that are not messages", %{name: name} do
    assert_raise ArgumentError, ~r/uses Outboxer.Message/, fn ->
      Outboxer.publish(%{n: 1}, name: name)
    end

    assert_raise ArgumentError, ~r/uses Outboxer.Message/, fn ->
      Outboxer.publish(%URI{}, name: name)
    end
  end

  test "rejects unknown and missing outboxes", %{name: name} do
    assert_raise ArgumentError, ~r/unknown outbox :nope/, fn ->
      Outboxer.publish(%Event{n: 1}, name: name, outbox: :nope)
    end

    assert_raise ArgumentError, ~r/does not name an outbox/, fn ->
      Outboxer.publish(%Event{n: 1}, name: name)
    end
  end

  test "rejects a non-string group key", %{name: name} do
    assert_raise ArgumentError, ~r/group_key must be a string or nil/, fn ->
      Outboxer.publish(%Event{n: 1, group: 42}, name: name, outbox: :events)
    end
  end

  test "raises for an instance that is not running" do
    assert_raise ArgumentError, ~r/no Outboxer instance named Nope/, fn ->
      Outboxer.publish(%Event{n: 1}, name: Nope, outbox: :events)
    end
  end

  test "require_transaction raises outside a transaction", context do
    name = start_outboxer(context, [events: []], testing: :manual, require_transaction: true)

    assert_raise ArgumentError, ~r/outside a transaction/, fn ->
      Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
    end

    assert {:ok, {:ok, _envelope}} =
             context.repo.transaction(fn ->
               Outboxer.publish(%Event{n: 1}, name: name, outbox: :events)
             end)
  end

  test "emits a publish telemetry event", %{name: name} do
    ref = :telemetry_test.attach_event_handlers(self(), [[:outboxer, :publish]])
    Outboxer.publish([%Event{n: 1}, %Event{n: 2}], name: name, outbox: :events)
    assert_receive {[:outboxer, :publish], ^ref, %{count: 2}, %{name: ^name, outboxes: [:events]}}
  end

  describe "Outboxer.Testing" do
    test "assert_published and refute_published", %{name: name} do
      Outboxer.publish(%Event{n: 1, group: "a"}, name: name, outbox: :events)

      assert %Envelope{} = assert_published(:events, Event, name: name)
      assert %Envelope{} = assert_published(:events, "Outboxer.Test.Event", name: name)
      assert %Envelope{} = assert_published(:events, &(&1.group_key == "a"), name: name)
      assert :ok = refute_published(:events, "other", name: name)

      assert_raise ExUnit.AssertionError, ~r/expected a message matching "other"/, fn ->
        assert_published(:events, "other", name: name)
      end

      assert_raise ExUnit.AssertionError, ~r/expected no message matching/, fn ->
        refute_published(:events, Event, name: name)
      end
    end

    test "drain delivers through the relay in the calling process", %{name: name} do
      Outboxer.publish(for(n <- 1..5, do: %Event{n: n, group: "g#{n}"}),
        name: name,
        outbox: :events
      )

      assert %{delivered: 5, failed: 0, dead: 0} = drain(:events, name: name)
      assert 5 |> collect() |> Enum.map(&n/1) |> Enum.sort() == [1, 2, 3, 4, 5]
      assert %{pending: 0} = Outboxer.counts(:events, name: name)
      assert %{delivered: 0} = drain(:events, name: name)
    end

    test "drain returns instead of looping when a message keeps failing", context do
      failures = start_failures(fn envelope -> n(envelope) == 1 end)

      name =
        start_outboxer(context, [events: [fail: failures, backoff: [min: 0, max: 0]]],
          testing: :manual
        )

      Outboxer.publish([%Event{n: 1, group: "g"}, %Event{n: 2, group: "h"}],
        name: name,
        outbox: :events
      )

      assert %{delivered: 1, failed: failed, dead: 0} = drain(:events, name: name)
      assert failed >= 1
      assert [%{attempts: attempts}] = published(:events, name: name)
      assert attempts >= 1
    end
  end
end
