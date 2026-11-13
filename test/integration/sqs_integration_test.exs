defmodule Outboxer.Integration.SQSTest do
  # Against MiniStack: `docker compose up -d ministack && mix test.integration`
  use Outboxer.Case, async: false

  alias Outboxer.Test.SqliteRepo

  @moduletag :integration
  @aws [scheme: "http://", host: "localhost", port: 4566, region: "us-east-1"]

  setup do
    SqliteRepo.delete_all("outboxer_messages")
    {:ok, repo: SqliteRepo}
  end

  defp create_queue(name, attributes) do
    {:ok, %{body: %{queue_url: url}}} =
      name |> ExAws.SQS.create_queue(attributes) |> ExAws.request(@aws)

    url
  end

  defp receive_all(url, expected, acc \\ [], empty_polls \\ 0)
  defp receive_all(_url, expected, acc, _empty) when length(acc) >= expected, do: acc
  defp receive_all(_url, _expected, acc, 5), do: acc

  defp receive_all(url, expected, acc, empty_polls) do
    operation =
      ExAws.SQS.receive_message(url,
        max_number_of_messages: 10,
        wait_time_seconds: 1,
        attribute_names: :all
      )

    # ex_aws_sqs asks for message attributes with a parameter name MiniStack
    # does not recognise, so the standard one is set by hand.
    operation = %{operation | params: Map.put(operation.params, "MessageAttributeName.1", "All")}
    {:ok, %{body: %{messages: messages}}} = ExAws.request(operation, @aws)

    for message <- messages do
      {:ok, _response} =
        url |> ExAws.SQS.delete_message(message.receipt_handle) |> ExAws.request(@aws)
    end

    empty_polls = if messages == [], do: empty_polls + 1, else: 0
    receive_all(url, expected, acc ++ messages, empty_polls)
  end

  test "FIFO queue: every group arrives in order, with group and dedup ids", context do
    url =
      create_queue("outboxer-#{System.unique_integer([:positive])}.fifo",
        fifo_queue: true,
        content_based_deduplication: false
      )

    relay = {Outboxer.Relay.SQS, queue_url: url, ex_aws_config: @aws}
    name = start_outboxer(context, orders: [relay: relay, poll_interval: 50])

    groups = ["order:1", "order:2", "order:3"]
    messages = for n <- 1..8, group <- groups, do: %Event{n: n, group: group}
    Outboxer.publish(messages, name: name, outbox: :orders, headers: %{"trace" => "abc"})
    eventually(fn -> Outboxer.counts(:orders, name: name).pending == 0 end, 30_000)

    received = receive_all(url, 24)
    assert length(received) == 24

    for group <- groups do
      of_group = Enum.filter(received, &(&1.attributes["message_group_id"] == group))
      assert Enum.map(of_group, &JSON.decode!(&1.body)["n"]) == Enum.to_list(1..8)
    end

    message = hd(received)
    assert message.message_attributes["trace"].value == "abc"
    assert message.message_attributes["outboxer-type"].value == "Outboxer.Test.Event"

    assert message.message_attributes["outboxer-id"].value ==
             message.attributes["message_deduplication_id"]

    assert message.attributes["message_deduplication_id"] =~ ~r/^orders-\d+$/
  end

  test "standard queue: every message arrives", context do
    url = create_queue("outboxer-#{System.unique_integer([:positive])}", [])
    relay = {Outboxer.Relay.SQS, queue_url: url, ex_aws_config: @aws}
    name = start_outboxer(context, orders: [relay: relay, poll_interval: 50, ordering: :none])

    Outboxer.publish(for(n <- 1..25, do: %Event{n: n}), name: name, outbox: :orders)
    eventually(fn -> Outboxer.counts(:orders, name: name).pending == 0 end, 30_000)

    received = receive_all(url, 25)
    assert received |> Enum.map(&JSON.decode!(&1.body)["n"]) |> Enum.sort() == Enum.to_list(1..25)
  end
end
