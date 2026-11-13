defmodule Outboxer.Integration.KafkaTest do
  # Against a real broker: `docker compose up -d kafka && mix test.integration`
  use Outboxer.Case, async: false

  alias Outboxer.Test.SqliteRepo

  @moduletag :integration
  @endpoints [{"localhost", 9092}]

  setup do
    SqliteRepo.delete_all("outboxer_messages")
    {:ok, _apps} = Application.ensure_all_started(:brod)
    topic = "outboxer-test-#{System.unique_integer([:positive])}"

    :ok =
      :brod.create_topics(
        @endpoints,
        [%{name: topic, num_partitions: 3, replication_factor: 1, assignments: [], configs: []}],
        %{timeout: 10_000}
      )

    {:ok, repo: SqliteRepo, topic: topic}
  end

  test "messages reach Kafka keyed by group, in order, with headers", %{topic: topic} = context do
    # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
    client = :"outboxer_it_#{System.unique_integer([:positive])}"
    relay = {Outboxer.Relay.Kafka, client: client, endpoints: @endpoints, topic: topic}

    name =
      start_outboxer(context,
        orders: [relay: relay, partitions: 2, poll_interval: 50, batch_size: 20]
      )

    groups = ["order:1", "order:2", "order:3", "order:4"]
    messages = for n <- 1..25, group <- groups, do: %Event{n: n, group: group}
    Outboxer.publish(messages, name: name, outbox: :orders, headers: %{"trace" => "abc"})

    eventually(fn -> Outboxer.counts(:orders, name: name).pending == 0 end, 30_000)

    records =
      for partition <- 0..2, record <- fetch_all(topic, partition), do: {partition, record}

    assert length(records) == 100

    for group <- groups do
      of_group = Enum.filter(records, fn {_partition, record} -> record.key == group end)
      assert [_one_partition] = of_group |> Enum.map(&elem(&1, 0)) |> Enum.uniq()

      assert Enum.map(of_group, fn {_partition, record} -> JSON.decode!(record.value)["n"] end) ==
               Enum.to_list(1..25)
    end

    {_partition, record} = hd(records)
    headers = Map.new(record.headers)
    assert headers["trace"] == "abc"
    assert headers["outboxer-type"] == "Outboxer.Test.Event"
    assert headers["outboxer-id"] =~ ~r/^orders-\d+$/
  end

  defp fetch_all(topic, partition, offset \\ 0, acc \\ []) do
    {:ok, {high_watermark, messages}} =
      :brod.fetch(@endpoints, topic, partition, offset, %{max_wait_time: 500})

    records =
      for {:kafka_message, _offset, key, value, _ts_type, _ts, headers} <- messages do
        %{key: key, value: value, headers: headers}
      end

    next = offset + length(messages)

    if next >= high_watermark,
      do: acc ++ records,
      else: fetch_all(topic, partition, next, acc ++ records)
  end
end
