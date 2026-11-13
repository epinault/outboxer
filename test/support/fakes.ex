defmodule Outboxer.Test.FakeBrod do
  @moduledoc false
  # Stands in for the :brod module. A "client" is an Agent registered under the
  # client id, holding where to report and how to misbehave.

  def start_link_client(endpoints, client, config) do
    Agent.start_link(
      fn ->
        %{pid: nil, partitions: 3, fail: nil, started_with: {endpoints, config}, producers: []}
      end,
      name: client
    )
  end

  def configure(client, changes), do: Agent.update(client, &Map.merge(&1, Map.new(changes)))
  def state(client), do: Agent.get(client, & &1)

  def get_partitions_count(client, _topic), do: {:ok, state(client).partitions}

  def start_producer(client, topic, config) do
    Agent.update(client, &%{&1 | producers: [{topic, config} | &1.producers]})
  end

  def produce_sync(client, topic, partition, "", records) do
    %{pid: pid, fail: fail, producers: producers} = state(client)

    cond do
      not Enum.any?(producers, &match?({^topic, _config}, &1)) ->
        {:error, {:producer_not_found, topic}}

      fail && fail.(topic, partition, records) ->
        {:error, :leader_not_available}

      true ->
        send(pid, {:produced, topic, partition, records})
        :ok
    end
  end
end

defmodule Outboxer.Test.FakeExAws do
  @moduledoc false
  # Stands in for ExAws.request/2. The test passes its pid and an optional
  # per-entry failure function through the relay's :ex_aws_config.

  def request(
        %ExAws.Operation.Query{action: :send_message_batch, params: params} = operation,
        config
      ) do
    entries = entries(params)
    send(config[:test_pid], {:sqs_request, operation.path, entries})

    case config[:respond] do
      nil ->
        fail = config[:fail] || fn _entry -> false end
        {failed, sent} = Enum.split_with(entries, fail)

        {:ok,
         %{
           body: %{
             successes: Enum.map(sent, &%{id: &1["Id"], message_id: "m-" <> &1["Id"]}),
             failures:
               Enum.map(
                 failed,
                 &%{id: &1["Id"], code: "InternalError", message: "nope", sender_fault: "false"}
               )
           }
         }}

      response ->
        response
    end
  end

  # "SendMessageBatchRequestEntry.2.MessageBody" => %{2 => %{"MessageBody" => ...}}
  defp entries(params) do
    params
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      case String.split(key, ".", parts: 3) do
        ["SendMessageBatchRequestEntry", index, field] ->
          Map.update(acc, String.to_integer(index), %{field => value}, &Map.put(&1, field, value))

        _other ->
          acc
      end
    end)
    |> Enum.sort()
    |> Enum.map(fn {_index, entry} -> entry end)
  end
end

defmodule Outboxer.Test.JobWorker do
  @moduledoc false
  use Oban.Worker, queue: :outbox

  @impl Oban.Worker
  def perform(_job), do: :ok
end
