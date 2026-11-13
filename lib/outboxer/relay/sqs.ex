if Code.ensure_loaded?(ExAws.SQS) do
  defmodule Outboxer.Relay.SQS do
    @moduledoc """
    Relays messages to Amazon SQS through
    [ex_aws_sqs](https://hex.pm/packages/ex_aws_sqs).

    This module only exists when `:ex_aws_sqs` is in your dependencies. ex_aws
    also needs an HTTP client and, to read SQS responses, an XML parser:

        {:ex_aws, "~> 2.5"},
        {:ex_aws_sqs, "~> 3.4"},
        {:sweet_xml, "~> 0.7"},   # or {:saxy, "~> 1.1"}
        {:hackney, "~> 1.20"}     # or configure ex_aws to use Req

    Without an XML parser ex_aws cannot tell Outboxer which entries of a batch
    were accepted, and the relay reports every batch as failed.

    ## Usage

        {Outboxer,
         repo: MyApp.Repo,
         outboxes: [
           orders: [
             relay: {Outboxer.Relay.SQS, queue_url: "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"}
           ]
         ]}

    Credentials and region come from your ex_aws configuration.

    ## Options

    #{NimbleOptions.docs(Outboxer.Relay.SQS.Options.schema())}

    ## How messages map to SQS

    | SQS | Outboxer |
    | --- | --- |
    | `MessageBody` | the encoded payload (see `:encoding`) |
    | `MessageGroupId` (FIFO) | the group key; messages without one get a unique id, so they do not queue behind each other |
    | `MessageDeduplicationId` (FIFO) | `Outboxer.Envelope.dedup_id/1`, stable across attempts |
    | message attributes | the message headers, plus `outboxer-id` and `outboxer-type` |

    SQS allows 10 message attributes per message, so keep headers to 8 or fewer.

    ## Ordering

    **Only FIFO queues keep order.** A standard queue will happily deliver
    messages out of order whatever Outboxer does; use one when you want the
    transactional guarantee without ordering.

    On a FIFO queue, messages of a group are sent in order. With the default
    `batching: :group_safe` a request never carries two messages of one group,
    because SQS can accept some entries of a batch and reject others. The cost
    is throughput for a single very busy group (one request per message).
    `batching: :fast` lifts that at the price of a small reordering window
    when SQS partially rejects a batch.

    ## Duplicates

    Delivery is at-least-once. On FIFO queues the deduplication id makes SQS
    drop a repeated send within its five-minute window. On standard queues the
    consumer has to deduplicate, for example on the `outboxer-id` attribute.

    ## Limits

    SQS rejects a message, or a whole batch, above its size limit. A message
    that is too large fails on every attempt, so set `max_attempts` on the
    outbox if your payloads can get near the limit.
    """

    @behaviour Outboxer.Relay

    alias Outboxer.Envelope
    alias Outboxer.Relay.SQS.Options

    @max_batch 10

    @impl Outboxer.Relay
    def init(opts) do
      with {:ok, opts} <- NimbleOptions.validate(opts, Options.schema()) do
        state = Map.new(opts)

        fifo =
          if is_nil(state.fifo), do: String.ends_with?(state.queue_url, ".fifo"), else: state.fifo

        {:ok, %{state | fifo: fifo}}
      end
    end

    @impl Outboxer.Relay
    def deliver(envelopes, state), do: {:partial, send_all(envelopes, state, %{}, MapSet.new())}

    # Sends request after request until every envelope is either sent, failed,
    # or skipped because an earlier message of its group failed.
    defp send_all([], _state, results, _failed_groups), do: results

    defp send_all(remaining, state, results, failed_groups) do
      remaining =
        Enum.reject(
          remaining,
          &(&1.group_key != nil and MapSet.member?(failed_groups, &1.group_key))
        )

      {batch, rest} = take_batch(remaining, state)

      if batch == [] do
        results
      else
        outcome = send_batch(batch, state)

        failed_groups =
          for envelope <- batch,
              envelope.group_key != nil,
              Map.fetch!(outcome, envelope.id) != :ok,
              reduce: failed_groups do
            failed -> MapSet.put(failed, envelope.group_key)
          end

        send_all(rest, state, Map.merge(results, outcome), failed_groups)
      end
    end

    # Takes up to 10 envelopes, in order. In group-safe mode an envelope is
    # left for a later request when its group already appeared in this pass,
    # whether that earlier envelope was taken or not.
    defp take_batch(envelopes, state) do
      one_per_group? = state.fifo and state.batching == :group_safe

      {batch, rest, _seen} =
        Enum.reduce(envelopes, {[], [], MapSet.new()}, fn envelope, {batch, rest, seen} ->
          key = envelope.group_key
          held_back? = one_per_group? and key != nil and MapSet.member?(seen, key)
          seen = if key, do: MapSet.put(seen, key), else: seen

          if held_back? or length(batch) >= @max_batch do
            {batch, [envelope | rest], seen}
          else
            {[envelope | batch], rest, seen}
          end
        end)

      {Enum.reverse(batch), Enum.reverse(rest)}
    end

    defp send_batch(batch, state) do
      {entries, invalid} =
        Enum.reduce(batch, {[], %{}}, fn envelope, {entries, invalid} ->
          case entry(envelope, state) do
            {:ok, entry} -> {[entry | entries], invalid}
            {:error, _reason} = error -> {entries, Map.put(invalid, envelope.id, error)}
          end
        end)

      Map.merge(request(Enum.reverse(entries), state), invalid)
    end

    defp request([], _state), do: %{}

    defp request(entries, state) do
      ids = Enum.map(entries, &(&1 |> Keyword.fetch!(:id) |> String.to_integer()))
      operation = ExAws.SQS.send_message_batch(state.queue_url, entries)

      case state.client.request(operation, state.ex_aws_config) do
        {:ok, %{body: %{successes: successes, failures: failures}}} ->
          failed =
            Map.new(failures, &{String.to_integer(&1.id), {:error, "#{&1.code}: #{&1.message}"}})

          succeeded = Map.new(successes, &{String.to_integer(&1.id), :ok})
          # Anything SQS did not mention is not known to be sent.
          unknown = Map.new(ids, &{&1, {:error, :missing_from_sqs_response}})
          unknown |> Map.merge(failed) |> Map.merge(succeeded)

        {:ok, %{body: body}} when is_binary(body) ->
          all(
            ids,
            {:error, "SQS response was not parsed; add :sweet_xml or :saxy to your dependencies"}
          )

        {:ok, other} ->
          all(ids, {:error, {:unexpected_sqs_response, other}})

        {:error, reason} ->
          all(ids, {:error, reason})
      end
    end

    defp all(ids, result), do: Map.new(ids, &{&1, result})

    defp entry(%Envelope{} = envelope, state) do
      with {:ok, body} <- body(envelope.payload, state.encoding) do
        {:ok,
         [
           id: Integer.to_string(envelope.id),
           message_body: body,
           message_attributes: attributes(envelope)
         ] ++
           fifo_fields(envelope, state)}
      end
    end

    defp body(payload, :base64), do: {:ok, Base.encode64(payload)}

    defp body(payload, :raw) do
      if String.valid?(payload) do
        {:ok, payload}
      else
        {:error, "payload is not valid UTF-8; use encoding: :base64 on the SQS relay"}
      end
    end

    defp fifo_fields(_envelope, %{fifo: false}), do: []

    defp fifo_fields(envelope, %{fifo: true}) do
      dedup_id = Envelope.dedup_id(envelope)
      [message_group_id: envelope.group_key || dedup_id, message_deduplication_id: dedup_id]
    end

    defp attributes(%Envelope{} = envelope) do
      envelope.headers
      |> Map.merge(%{
        "outboxer-id" => Envelope.dedup_id(envelope),
        "outboxer-type" => envelope.type
      })
      |> Enum.sort()
      |> Enum.map(fn {name, value} -> %{name: name, data_type: :string, value: value} end)
    end
  end
end
