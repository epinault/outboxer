defmodule Outboxer.Relay.SQSTest do
  use ExUnit.Case, async: true

  alias Outboxer.Envelope
  alias Outboxer.Relay.SQS
  alias Outboxer.Test.FakeExAws

  @fifo "https://sqs.us-east-1.amazonaws.com/1/orders.fifo"
  @standard "https://sqs.us-east-1.amazonaws.com/1/orders"

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

  defp relay(url, opts \\ []) do
    {config, opts} = Keyword.split(opts, [:fail, :respond])

    {:ok, state} =
      SQS.init(
        [queue_url: url, client: FakeExAws, ex_aws_config: [test_pid: self()] ++ config] ++ opts
      )

    state
  end

  defp requests do
    receive do
      {:sqs_request, _path, entries} -> [entries | requests()]
    after
      0 -> []
    end
  end

  describe "init/1" do
    test "detects FIFO queues from the URL and lets it be overridden" do
      assert {:ok, %{fifo: true}} = SQS.init(queue_url: @fifo)
      assert {:ok, %{fifo: false}} = SQS.init(queue_url: @standard)
      assert {:ok, %{fifo: true}} = SQS.init(queue_url: @standard, fifo: true)
      assert {:error, %NimbleOptions.ValidationError{}} = SQS.init([])
    end
  end

  describe "standard queues" do
    test "sends batches of at most 10 without FIFO fields" do
      envelopes = for id <- 1..23, do: envelope(id, "g")

      assert {:partial, results} = SQS.deliver(envelopes, relay(@standard))
      assert map_size(results) == 23 and Enum.all?(results, fn {_id, result} -> result == :ok end)

      assert [first, second, third] = requests()
      assert Enum.map([first, second, third], &length/1) == [10, 10, 3]
      assert Enum.map(first, & &1["MessageBody"]) == Enum.map(1..10, &"p#{&1}")
      refute Map.has_key?(hd(first), "MessageGroupId")
    end

    test "sends headers and identity as message attributes" do
      SQS.deliver([envelope(5, nil, headers: %{"trace" => "t1"})], relay(@standard))
      assert [[entry]] = requests()

      attributes =
        for n <- 1..3, into: %{} do
          {entry["MessageAttribute.#{n}.Name"], entry["MessageAttribute.#{n}.Value.StringValue"]}
        end

      assert attributes == %{
               "outboxer-id" => "orders-5",
               "outboxer-type" => "order.placed",
               "trace" => "t1"
             }
    end
  end

  describe "FIFO queues" do
    test "sets group and deduplication ids" do
      SQS.deliver([envelope(1, "a"), envelope(2, nil)], relay(@fifo))
      assert [[grouped, ungrouped]] = requests()

      assert grouped["MessageGroupId"] == "a"
      assert grouped["MessageDeduplicationId"] == "orders-1"
      # No group: its own group, so it never queues behind another message.
      assert ungrouped["MessageGroupId"] == "orders-2"
      assert ungrouped["MessageDeduplicationId"] == "orders-2"
    end

    test "group_safe batching never puts two messages of a group in one request" do
      envelopes = [
        envelope(1, "a"),
        envelope(2, "a"),
        envelope(3, "b"),
        envelope(4, "a"),
        envelope(5, "b")
      ]

      assert {:partial, results} = SQS.deliver(envelopes, relay(@fifo))
      assert Enum.all?(results, fn {_id, result} -> result == :ok end)

      sent = Enum.map(requests(), fn entries -> Enum.map(entries, & &1["Id"]) end)
      assert sent == [["1", "3"], ["2", "5"], ["4"]]
    end

    test "fast batching fills requests regardless of group" do
      envelopes = for id <- 1..12, do: envelope(id, "a")
      SQS.deliver(envelopes, relay(@fifo, batching: :fast))
      assert Enum.map(requests(), &length/1) == [10, 2]
    end

    test "after a failure the rest of that group is not sent" do
      envelopes = [
        envelope(1, "a"),
        envelope(2, "b"),
        envelope(3, "a"),
        envelope(4, "b"),
        envelope(5, "a")
      ]

      state = relay(@fifo, fail: fn entry -> entry["Id"] == "1" end)

      assert {:partial, results} = SQS.deliver(envelopes, state)
      assert results == %{1 => {:error, "InternalError: nope"}, 2 => :ok, 4 => :ok}

      sent = requests() |> List.flatten() |> Enum.map(& &1["Id"])
      assert sent == ["1", "2", "4"]
    end
  end

  describe "payload encoding" do
    test "raw rejects payloads that are not UTF-8, without failing the others" do
      envelopes = [envelope(1, nil, payload: <<255, 254>>), envelope(2, nil)]

      assert {:partial, %{1 => {:error, message}, 2 => :ok}} =
               SQS.deliver(envelopes, relay(@standard))

      assert message =~ "encoding: :base64"
    end

    test "base64 encodes the payload" do
      SQS.deliver([envelope(1, nil, payload: <<255, 254>>)], relay(@standard, encoding: :base64))
      assert [[%{"MessageBody" => body}]] = requests()
      assert Base.decode64!(body) == <<255, 254>>
    end
  end

  describe "errors" do
    test "a failed request fails every entry in it" do
      state = relay(@standard, respond: {:error, {:http_error, 503, "unavailable"}})
      assert {:partial, results} = SQS.deliver([envelope(1, nil), envelope(2, nil)], state)

      assert results == %{
               1 => {:error, {:http_error, 503, "unavailable"}},
               2 => {:error, {:http_error, 503, "unavailable"}}
             }
    end

    test "an unparsed response is reported with a hint about the XML parser" do
      state = relay(@standard, respond: {:ok, %{body: "<SendMessageBatchResponse/>"}})
      assert {:partial, %{1 => {:error, message}}} = SQS.deliver([envelope(1, nil)], state)
      assert message =~ "sweet_xml"
    end

    test "entries missing from the response are not treated as sent" do
      state = relay(@standard, respond: {:ok, %{body: %{successes: [%{id: "1"}], failures: []}}})
      assert {:partial, results} = SQS.deliver([envelope(1, nil), envelope(2, nil)], state)
      assert results == %{1 => :ok, 2 => {:error, :missing_from_sqs_response}}
    end
  end
end
