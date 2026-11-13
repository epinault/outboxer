defmodule Outboxer.Dispatcher do
  @moduledoc false
  # Delivers one batch of one partition: fetch, hand to the relay, record the
  # outcome. Called by the partition workers and by `Outboxer.Testing.drain/3`,
  # so the same code path runs in production and in tests.

  alias Outboxer.Backoff
  alias Outboxer.Config
  alias Outboxer.Config.Outbox
  alias Outboxer.Envelope

  require Logger

  @type summary :: %{
          fetched: non_neg_integer(),
          delivered: non_neg_integer(),
          failed: non_neg_integer(),
          dead: non_neg_integer()
        }

  @empty %{fetched: 0, delivered: 0, failed: 0, dead: 0}

  @spec run(Config.t(), Outbox.t(), non_neg_integer()) :: {:ok, summary()} | :busy
  def run(%Config{} = conf, %Outbox{} = outbox, partition) do
    conf.storage.with_partition(conf, outbox.name, partition, fn ->
      now = DateTime.utc_now()

      case conf.storage.fetch(conf, outbox.name, partition, outbox.batch_size, now) do
        [] -> @empty
        envelopes -> deliver(conf, outbox, partition, envelopes)
      end
    end)
  end

  defp deliver(conf, outbox, partition, envelopes) do
    metadata = %{name: conf.name, outbox: outbox.name, partition: partition, relay: outbox.relay}

    :telemetry.span(
      [:outboxer, :relay, :batch],
      Map.put(metadata, :count, length(envelopes)),
      fn ->
        result = call_relay(outbox, envelopes)
        summary = settle(conf, outbox, envelopes, result)
        {summary, Map.merge(metadata, summary)}
      end
    )
  end

  defp call_relay(%Outbox{relay: relay, relay_state: state}, envelopes) do
    case relay.deliver(envelopes, state) do
      :ok -> :ok
      {:error, _reason} = error -> error
      {:partial, results} when is_map(results) -> {:partial, results}
      other -> {:error, {:bad_return, other}}
    end
  rescue
    exception ->
      Logger.error(Exception.format(:error, exception, __STACKTRACE__))
      {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Walks the batch in delivery order and sorts every message into delivered,
  # failed, or left alone. Within a group only the first failure is recorded:
  # it is the one that blocks the group, and touching the rows behind it would
  # only create dead tuples.
  defp settle(conf, outbox, envelopes, result) do
    now = DateTime.utc_now()

    {acked, failures, _blocked} =
      Enum.reduce(envelopes, {[], [], MapSet.new()}, &sort(outcome(result, &1), &1, &2))

    conf.storage.ack(conf, acked, outbox.retention, now)
    {dead, retried} = record_failures(conf, outbox, Enum.reverse(failures), now)

    %{fetched: length(envelopes), delivered: length(acked), failed: retried, dead: dead}
  end

  defp sort(:ok, envelope, {acked, failures, blocked}),
    do: {[envelope.id | acked], failures, blocked}

  defp sort(:skipped, _envelope, acc), do: acc

  defp sort({:error, reason}, envelope, {acked, failures, blocked} = acc) do
    if grouped?(envelope) and MapSet.member?(blocked, envelope.group_key) do
      acc
    else
      {acked, [{envelope, reason} | failures], block(blocked, envelope)}
    end
  end

  defp outcome(:ok, _envelope), do: :ok
  defp outcome({:error, reason}, _envelope), do: {:error, reason}

  defp outcome({:partial, results}, envelope) do
    case Map.get(results, envelope.id, :skipped) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      :skipped -> :skipped
      other -> {:error, {:bad_return, other}}
    end
  end

  defp grouped?(%Envelope{group_key: key}), do: key != nil

  defp block(blocked, %Envelope{group_key: nil}), do: blocked
  defp block(blocked, %Envelope{group_key: key}), do: MapSet.put(blocked, key)

  defp record_failures(_conf, _outbox, [], _now), do: {0, 0}

  defp record_failures(conf, outbox, failures, now) do
    failures = Enum.map(failures, fn {envelope, reason} -> {envelope, format_error(reason)} end)

    {dead, retry} =
      Enum.split_with(failures, fn {envelope, _error} -> last_attempt?(outbox, envelope) end)

    kill(conf, dead, now)
    schedule_retries(conf, outbox, retry, now)
    {length(dead), length(retry)}
  end

  defp kill(conf, dead, now) do
    for {error, envelopes} <- Enum.group_by(dead, &elem(&1, 1), &elem(&1, 0)) do
      conf.storage.kill(conf, ids(envelopes), error, now)
      for envelope <- envelopes, do: emit(:dead, conf, envelope, error)
    end

    :ok
  end

  # One UPDATE per distinct (attempt number, error), not one per row.
  defp schedule_retries(conf, outbox, retry, now) do
    groups =
      Enum.group_by(
        retry,
        fn {envelope, error} -> {envelope.attempts + 1, error} end,
        &elem(&1, 0)
      )

    for {{attempt, error}, envelopes} <- groups do
      retry_at = DateTime.add(now, Backoff.delay(outbox.backoff, attempt), :millisecond)
      conf.storage.fail(conf, ids(envelopes), retry_at, error)
      for envelope <- envelopes, do: emit(:retry, conf, envelope, error)
    end

    :ok
  end

  defp ids(envelopes), do: Enum.map(envelopes, & &1.id)

  defp last_attempt?(%Outbox{max_attempts: :infinity}, _envelope), do: false

  defp last_attempt?(%Outbox{max_attempts: max}, %Envelope{attempts: attempts}),
    do: attempts + 1 >= max

  defp emit(event, conf, envelope, error) do
    :telemetry.execute(
      [:outboxer, :message, event],
      %{attempt: envelope.attempts + 1},
      %{
        name: conf.name,
        outbox: envelope.outbox,
        id: envelope.id,
        type: envelope.type,
        group_key: envelope.group_key,
        error: error
      }
    )
  end

  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format_error(reason), do: inspect(reason)
end
