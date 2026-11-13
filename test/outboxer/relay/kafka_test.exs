defmodule Outboxer.Relay.KafkaTest do
  use Outboxer.Case, async: false, parameterize: Outboxer.Case.repos()

  alias Outboxer.Envelope
  alias Outboxer.Relay.Kafka
  alias Outboxer.Test.FakeBrod

  defp envelope(id, group, attrs \\ []) do
    struct!(
      %Envelope{
        id: id,
        outbox: :orders,
        type: "order.placed",
        payload: "p#{id}",
        group_key: group
      },
      attrs
    )
  end

  defp client(context) do
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    client = :"fake_brod_#{System.unique_integer([:positive])}"
    start_supervised!(%{id: client, start: {FakeBrod, :start_link_client, [[], client, []]}})
    FakeBrod.configure(client, pid: self(), partitions: context[:partitions] || 3)
    client
  end

  defp relay(client, opts \\ []) do
    {:ok, state} =
      Kafka.init(Keyword.merge([client: client, topic: "orders", brod: FakeBrod], opts))

    state
  end

  describe "init/1" do
    test "validates options" do
      assert {:error, %NimbleOptions.ValidationError{}} = Kafka.init(topic: "orders")
      assert {:error, %NimbleOptions.ValidationError{}} = Kafka.init(client: :c)

      assert {:ok, %{producer_config: [required_acks: -1], partitioner: :hash}} =
               Kafka.init(client: :c, topic: "t")
    end

    test "starts no client by default and one when endpoints are given" do
      {:ok, state} = Kafka.init(client: :c, topic: "t")
      assert Kafka.children(state) == []

      {:ok, state} =
        Kafka.init(
          client: :c,
          topic: "t",
          endpoints: [{"kafka", 9092}],
          client_config: [ssl: true],
          brod: FakeBrod
        )

      assert [
               %{
                 id: {:brod_client, :c},
                 start: {FakeBrod, :start_link_client, [[{"kafka", 9092}], :c, config]}
               }
             ] =
               Kafka.children(state)

      assert config[:ssl] == true
      assert config[:auto_start_producers] == true
      assert config[:default_producer_config] == [required_acks: -1]
    end
  end

  describe "deliver/2" do
    test "produces one batch per partition, in order, keyed by group", context do
      client = client(context)
      envelopes = [envelope(1, "a"), envelope(2, "b"), envelope(3, "a"), envelope(4, "a")]

      assert {:partial, results} = Kafka.deliver(envelopes, relay(client))
      assert results == %{1 => :ok, 2 => :ok, 3 => :ok, 4 => :ok}

      produced = produced_batches(4, [])
      by_key = produced |> Enum.flat_map(&elem(&1, 1)) |> Enum.group_by(& &1.key, & &1.value)
      assert by_key == %{"a" => ["p1", "p3", "p4"], "b" => ["p2"]}

      # A group never spans partitions.
      partitions_of = fn key ->
        for {partition, records} <- produced, Enum.any?(records, &(&1.key == key)), do: partition
      end

      assert [_one] = partitions_of.("a")
      assert [_one] = partitions_of.("b")
    end

    test "sends headers, including the dedup id and type", context do
      client = client(context)
      Kafka.deliver([envelope(7, "a", headers: %{"trace" => "t1"})], relay(client))

      assert_receive {:produced, "orders", _partition, [record]}

      assert record.headers == [
               {"outboxer-id", "orders-7"},
               {"outboxer-type", "order.placed"},
               {"trace", "t1"}
             ]

      assert record.value == "p7"
    end

    test "starts the topic's producer on demand with the producer config", context do
      client = client(context)
      Kafka.deliver([envelope(1, "a")], relay(client, producer_config: [required_acks: 1]))

      assert_receive {:produced, "orders", _partition, _records}
      assert [{"orders", [required_acks: 1]}] = FakeBrod.state(client).producers
    end

    test "resolves the topic per message from a function or MFA", context do
      client = client(context)
      by_type = fn envelope -> "topic-" <> envelope.group_key end

      Kafka.deliver([envelope(1, "a"), envelope(2, "b")], relay(client, topic: by_type))
      assert_receive {:produced, "topic-a", _partition, [%{value: "p1"}]}
      assert_receive {:produced, "topic-b", _partition, [%{value: "p2"}]}

      Kafka.deliver([envelope(3, "a")], relay(client, topic: {__MODULE__, :topic_for, ["x"]}))
      assert_receive {:produced, "x-orders", _partition, [%{value: "p3"}]}
    end

    @tag partitions: 1
    test "reports a failed partition for exactly its messages", context do
      client = client(context)
      FakeBrod.configure(client, fail: fn topic, _partition, _records -> topic == "bad" end)
      topic = fn envelope -> if envelope.group_key == "x", do: "bad", else: "good" end

      assert {:partial, results} =
               Kafka.deliver(
                 [envelope(1, "x"), envelope(2, "y"), envelope(3, "x")],
                 relay(client, topic: topic)
               )

      assert results == %{
               1 => {:error, :leader_not_available},
               2 => :ok,
               3 => {:error, :leader_not_available}
             }
    end

    test "supports a custom partitioner", context do
      client = client(context)

      Kafka.deliver(
        [envelope(1, "a")],
        relay(client, partitioner: fn _envelope, count -> count - 1 end)
      )

      assert_receive {:produced, "orders", 2, _records}
    end
  end

  test "end to end: an outbox relays to Kafka in group order", context do
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    client = :"fake_brod_e2e_#{System.unique_integer([:positive])}"

    relay =
      {Kafka, client: client, topic: "orders", endpoints: [{"localhost", 9092}], brod: FakeBrod}

    # The fake client is started by the relay's children/1; point it at the test.
    name = start_outboxer(context, orders: [relay: relay, poll_interval: 20, partitions: 2])
    FakeBrod.configure(client, pid: self())

    Outboxer.publish(for(n <- 1..6, do: %Event{n: n, group: "g#{rem(n, 2)}"}),
      name: name,
      outbox: :orders
    )

    values = collect_values(6, [])
    for group <- ["g0", "g1"], do: assert(Enum.sort(values[group]) == values[group])
    eventually(fn -> Outboxer.counts(:orders, name: name).pending == 0 end)
  end

  defp produced_batches(left, acc) when left <= 0, do: Enum.reverse(acc)

  defp produced_batches(left, acc) do
    assert_receive {:produced, "orders", partition, records}
    produced_batches(left - length(records), [{partition, records} | acc])
  end

  defp collect_values(0, acc), do: Enum.group_by(acc, &elem(&1, 0), &elem(&1, 1))

  defp collect_values(left, acc) do
    assert_receive {:produced, "orders", _partition, records}, 5_000
    seen = Enum.map(records, &{&1.key, JSON.decode!(&1.value)["n"]})
    collect_values(left - length(records), acc ++ seen)
  end

  def topic_for(envelope, prefix), do: "#{prefix}-#{envelope.outbox}"
end
