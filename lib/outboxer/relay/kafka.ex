if Code.ensure_loaded?(:brod) do
  defmodule Outboxer.Relay.Kafka do
    @moduledoc """
    Relays messages to Kafka through [brod](https://hex.pm/packages/brod).

    This module only exists when `:brod` is in your dependencies:

        {:brod, "~> 4.0"}

    ## With a brod client you already run

        {Outboxer,
         repo: MyApp.Repo,
         outboxes: [
           orders: [relay: {Outboxer.Relay.Kafka, client: :my_brod_client, topic: "orders"}]
         ]}

    ## Letting Outboxer start the client

    Give `:endpoints` and Outboxer supervises a brod client under the outbox,
    registered as `:client`:

        orders: [
          relay:
            {Outboxer.Relay.Kafka,
             client: :outboxer_orders,
             endpoints: [{"localhost", 9092}],
             topic: "orders",
             producer_config: [required_acks: -1, compression: :snappy]}
        ]

    ## Options

    #{NimbleOptions.docs(Outboxer.Relay.Kafka.Options.schema())}

    ## How messages map to Kafka

    | Kafka | Outboxer |
    | --- | --- |
    | topic | the `:topic` option |
    | key | the message's group key (empty when it has none) |
    | value | the encoded payload, as is |
    | headers | the message headers, plus `outboxer-id` and `outboxer-type` |
    | partition | hash of the key; random for messages without a key |

    ## Ordering

    A group key becomes the Kafka key, so a group always goes to the same Kafka
    partition and Kafka keeps the order Outboxer delivered in. A batch is
    produced with one synchronous request per topic-partition and Outboxer
    waits for the acknowledgement before it removes the messages.

    The partition is chosen by Outboxer, not by brod's partitioner, and it
    depends on the topic's partition count. If you add partitions to a topic,
    keys move, as they would with any hash partitioner.

    Use `required_acks: -1` (the default here) unless you can afford to lose
    acknowledged messages when a broker fails.

    ## Duplicates

    Delivery is at-least-once. If the broker stored a batch but the
    acknowledgement was lost, or the database commit that follows fails, the
    batch is produced again. Consumers can deduplicate on the `outboxer-id`
    header, which is stable across attempts.
    """

    @behaviour Outboxer.Relay

    alias Outboxer.Envelope
    alias Outboxer.Relay.Kafka.Options

    @impl Outboxer.Relay
    def init(opts) do
      with {:ok, opts} <- NimbleOptions.validate(opts, Options.schema()) do
        {:ok, Map.new(opts)}
      end
    end

    @impl Outboxer.Relay
    def children(%{endpoints: nil}), do: []

    def children(%{endpoints: endpoints, client: client} = state) do
      config =
        Keyword.merge(
          [auto_start_producers: true, default_producer_config: state.producer_config],
          state.client_config
        )

      [
        %{
          id: {:brod_client, client},
          start: {state.brod, :start_link_client, [endpoints, client, config]}
        }
      ]
    end

    @impl Outboxer.Relay
    def deliver(envelopes, state) do
      results =
        envelopes
        |> Enum.map(&{&1, topic(state, &1)})
        |> Enum.group_by(fn {_envelope, topic} -> topic end, fn {envelope, _topic} -> envelope end)
        |> Enum.flat_map(fn {topic, envelopes} -> produce_topic(state, topic, envelopes) end)
        |> Map.new()

      {:partial, results}
    end

    defp produce_topic(state, topic, envelopes) do
      case partitions_count(state, topic) do
        {:ok, count} ->
          envelopes
          |> Enum.group_by(&partition(state, &1, count))
          |> Enum.flat_map(fn {partition, batch} ->
            result = produce(state, topic, partition, batch)
            Enum.map(batch, &{&1.id, result})
          end)

        {:error, reason} ->
          Enum.map(envelopes, &{&1.id, {:error, reason}})
      end
    end

    defp partitions_count(%{brod: brod, client: client} = state, topic) do
      with {:error, {:producer_not_found, _topic}} <- brod.get_partitions_count(client, topic),
           :ok <- brod.start_producer(client, topic, state.producer_config) do
        brod.get_partitions_count(client, topic)
      end
    end

    defp produce(%{brod: brod, client: client} = state, topic, partition, batch) do
      records = Enum.map(batch, &record/1)

      case brod.produce_sync(client, topic, partition, "", records) do
        :ok ->
          :ok

        {:error, {:producer_not_found, _topic}} ->
          with :ok <- brod.start_producer(client, topic, state.producer_config) do
            brod.produce_sync(client, topic, partition, "", records)
          end

        {:error, {:producer_not_found, _topic, _partition}} = error ->
          error

        {:error, _reason} = error ->
          error
      end
    end

    defp record(%Envelope{} = envelope) do
      headers =
        [{"outboxer-id", Envelope.dedup_id(envelope)}, {"outboxer-type", envelope.type}] ++
          Enum.sort(Map.to_list(envelope.headers))

      %{key: envelope.group_key || "", value: envelope.payload, headers: headers}
    end

    defp topic(%{topic: topic}, _envelope) when is_binary(topic), do: topic
    defp topic(%{topic: topic}, envelope) when is_function(topic, 1), do: topic.(envelope)

    defp topic(%{topic: {module, function, args}}, envelope),
      do: apply(module, function, [envelope | args])

    defp partition(%{partitioner: :hash}, %Envelope{group_key: nil}, count),
      do: :rand.uniform(count) - 1

    defp partition(%{partitioner: :hash}, %Envelope{group_key: key}, count),
      do: :erlang.phash2(key, count)

    defp partition(%{partitioner: fun}, envelope, count) when is_function(fun, 2),
      do: fun.(envelope, count)
  end
end
